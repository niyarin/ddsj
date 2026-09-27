package ddsj.rtps.discovery;

import ddsj.rtps.message.Heartbeat;
import ddsj.rtps.message.RtpsMessageBuilder;
import ddsj.rtps.message.RtpsUserDataParser;
import ddsj.rtps.protocol.RtpsEntity;
import ddsj.rtps.transport.RtpsPacket;
import ddsj.rtps.transport.RtpsTransport;
import ddsj.rtps.types.GuidPrefix;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public final class SpdpDiscoveryListener implements Closeable {
    private static final long PURGE_INTERVAL_MILLIS = 1000L;

    private final RtpsTransport transport;
    private final GuidPrefix localGuidPrefix;
    private final RemoteParticipantStore remoteParticipants;
    private final RemoteEndpointStore<RemotePublication> remotePublications;
    private final RemoteEndpointStore<RemoteSubscription> remoteSubscriptions;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Closeable listener;
    private final Thread purgeWorker;
    private final AtomicInteger ackNackCount = new AtomicInteger(1);
    // Track received sequence numbers for SEDP
    private final Set<Long> receivedPubSequences = new HashSet<>();
    private final Set<Long> receivedSubSequences = new HashSet<>();

    public SpdpDiscoveryListener(
            RtpsTransport transport,
            GuidPrefix localGuidPrefix,
            RemoteParticipantStore remoteParticipants,
            RemoteEndpointStore<RemotePublication> remotePublications,
            RemoteEndpointStore<RemoteSubscription> remoteSubscriptions) throws IOException {
        this.transport = transport;
        this.localGuidPrefix = localGuidPrefix;
        this.remoteParticipants = remoteParticipants;
        this.remotePublications = remotePublications;
        this.remoteSubscriptions = remoteSubscriptions;
        this.listener = transport.listenMetatraffic(this::handlePacket);
        this.purgeWorker = new Thread(this::purgeLoop, "ddsj-rtps-discovery-purge");
        this.purgeWorker.setDaemon(true);
        this.purgeWorker.start();
    }

    private void handlePacket(RtpsPacket packet) {
        // Handle SEDP HEARTBEAT and send AckNack for missing sequences
        var pubHbs = RtpsUserDataParser.readHeartbeats(
            packet.data(), packet.length(), RtpsEntity.PUBLICATIONS_BUILTIN_TOPIC_READER);
        for (var hb : pubHbs) {
            handleSedpHeartbeat(hb, RtpsEntity.PUBLICATIONS_BUILTIN_TOPIC_READER,
                RtpsEntity.PUBLICATIONS_BUILTIN_TOPIC_WRITER, receivedPubSequences);
        }
        var subHbs = RtpsUserDataParser.readHeartbeats(
            packet.data(), packet.length(), RtpsEntity.SUBSCRIPTIONS_BUILTIN_TOPIC_READER);
        for (var hb : subHbs) {
            handleSedpHeartbeat(hb, RtpsEntity.SUBSCRIPTIONS_BUILTIN_TOPIC_READER,
                RtpsEntity.SUBSCRIPTIONS_BUILTIN_TOPIC_WRITER, receivedSubSequences);
        }
        RtpsDiscoveryReader.readRemoteParticipant(packet.data(), packet.length()).ifPresent(remote -> {
            if (!remote.guidPrefix().equals(localGuidPrefix)) {
                remoteParticipants.upsert(remote);
            }
        });
        RtpsDiscoveryReader.readRemotePublicationChange(packet.data(), packet.length()).ifPresent(change -> {
            if (!change.endpointGuid().prefix().equals(localGuidPrefix)) {
                if (change.disposedOrUnregistered()) {
                    remotePublications.remove(change.endpointGuid());
                } else {
                    change.endpoint().ifPresent(remotePublications::upsert);
                }
            }
        });
        RtpsDiscoveryReader.readRemoteSubscriptionChange(packet.data(), packet.length()).ifPresent(change -> {
            if (!change.endpointGuid().prefix().equals(localGuidPrefix)) {
                if (change.disposedOrUnregistered()) {
                    remoteSubscriptions.remove(change.endpointGuid());
                } else {
                    change.endpoint().ifPresent(remoteSubscriptions::upsert);
                }
            }
        });
        purgeExpiredParticipants();
    }

    private void handleSedpHeartbeat(Heartbeat hb,
            ddsj.rtps.types.EntityId readerId,
            ddsj.rtps.types.EntityId writerId,
            Set<Long> receivedSequences) {
        // Find missing sequences
        Set<Long> missing = new HashSet<>();
        for (long sn = hb.firstSequenceNumber(); sn <= hb.lastSequenceNumber(); sn++) {
            if (!receivedSequences.contains(sn)) {
                missing.add(sn);
            }
        }
        if (!missing.isEmpty()) {
            try {
                sendAckNack(hb, readerId, writerId, missing);
            } catch (IOException ignored) {
            }
        }
    }

    private void sendAckNack(Heartbeat hb,
            ddsj.rtps.types.EntityId readerId,
            ddsj.rtps.types.EntityId writerId,
            Set<Long> missing) throws IOException {
        RtpsMessageBuilder message = new RtpsMessageBuilder(localGuidPrefix);
        long baseSeqNum = missing.stream().min(Long::compare).orElse(1L);
        message.ackNack(readerId, writerId, baseSeqNum, missing, ackNackCount.getAndIncrement());
        // Send to metatraffic multicast
        transport.sendMetatraffic(message.bytes());
        // Also send to known participants' metatraffic unicast
        remoteParticipants.get(hb.writerGuid().prefix()).ifPresent(participant -> {
            for (var locator : participant.metatrafficUnicast()) {
                try {
                    transport.send(message.bytes(), locator);
                } catch (IOException ignored) {
                }
            }
        });
    }

    private void purgeLoop() {
        while (running.get()) {
            purgeExpiredParticipants();
            try {
                Thread.sleep(PURGE_INTERVAL_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void purgeExpiredParticipants() {
        for (RemoteParticipant participant : remoteParticipants.removeExpired()) {
            remotePublications.removeByParticipant(participant.guidPrefix());
            remoteSubscriptions.removeByParticipant(participant.guidPrefix());
        }
    }

    @Override
    public void close() throws IOException {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        listener.close();
        purgeWorker.interrupt();
    }
}
