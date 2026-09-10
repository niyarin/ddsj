package ddsjdk.rtps.discovery;

import ddsjdk.rtps.transport.RtpsPacket;
import ddsjdk.rtps.transport.RtpsTransport;
import ddsjdk.rtps.types.GuidPrefix;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public final class SpdpDiscoveryListener implements Closeable {
    private static final long PURGE_INTERVAL_MILLIS = 1000L;

    private final GuidPrefix localGuidPrefix;
    private final RemoteParticipantStore remoteParticipants;
    private final RemoteEndpointStore<RemotePublication> remotePublications;
    private final RemoteEndpointStore<RemoteSubscription> remoteSubscriptions;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Closeable listener;
    private final Thread purgeWorker;

    public SpdpDiscoveryListener(
            RtpsTransport transport,
            GuidPrefix localGuidPrefix,
            RemoteParticipantStore remoteParticipants,
            RemoteEndpointStore<RemotePublication> remotePublications,
            RemoteEndpointStore<RemoteSubscription> remoteSubscriptions) throws IOException {
        this.localGuidPrefix = localGuidPrefix;
        this.remoteParticipants = remoteParticipants;
        this.remotePublications = remotePublications;
        this.remoteSubscriptions = remoteSubscriptions;
        this.listener = transport.listenMetatraffic(this::handlePacket);
        this.purgeWorker = new Thread(this::purgeLoop, "ddsjdk-rtps-discovery-purge");
        this.purgeWorker.setDaemon(true);
        this.purgeWorker.start();
    }

    private void handlePacket(RtpsPacket packet) {
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
