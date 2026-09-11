package ddsjdk.rtps.runtime;

import ddsjdk.rtps.discovery.EndpointQos.HistoryKind;
import ddsjdk.rtps.discovery.EndpointQos.ReliabilityKind;
import ddsjdk.rtps.discovery.LocalEndpoint;
import ddsjdk.rtps.discovery.RemoteParticipant;
import ddsjdk.rtps.discovery.RemoteParticipantStore;
import ddsjdk.rtps.discovery.RemoteEndpointStore;
import ddsjdk.rtps.discovery.RemoteSubscription;
import ddsjdk.rtps.discovery.SedpPublicationAnnouncer;
import ddsjdk.rtps.discovery.SpdpAnnouncer;
import ddsjdk.rtps.discovery.SpdpDiscoveryListener;
import ddsjdk.rtps.history.WriterHistoryCache;
import ddsjdk.rtps.message.AckNack;
import ddsjdk.rtps.message.AckNackListener;
import ddsjdk.rtps.message.RtpsMessageBuilder;
import ddsjdk.rtps.protocol.RtpsEntity;
import ddsjdk.rtps.transport.RtpsParticipantConfig;
import ddsjdk.rtps.transport.RtpsTransport;
import ddsjdk.rtps.transport.UdpRtpsTransport;
import ddsjdk.rtps.types.EntityId;
import ddsjdk.rtps.types.GuidPrefix;
import ddsjdk.rtps.types.RtpsGuid;
import ddsjdk.rtps.types.RtpsTimestamp;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public final class RtpsDataWriter<T> implements Closeable {
    private static final int DEFAULT_KEEP_ALL_WRITER_HISTORY_LIMIT = 128;
    private static final long ANNOUNCE_INTERVAL_MILLIS = 1000L;

    private final LocalEndpoint endpoint;
    private final PayloadSerializer<T> serializer;
    private final RtpsTransport transport;
    private final GuidPrefix guidPrefix = RtpsGuid.newGuidPrefix();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicLong userSequence = new AtomicLong(1);
    private final AtomicLong userHeartbeatCount = new AtomicLong(1);
    private final WriterHistoryCache history;
    private final RemoteParticipantStore remoteParticipants = new RemoteParticipantStore();
    private final RemoteEndpointStore<RemoteSubscription> remoteSubscriptions = new RemoteEndpointStore<>();
    private final SpdpAnnouncer spdpAnnouncer;
    private final SedpPublicationAnnouncer sedpPublicationAnnouncer;
    private final SpdpDiscoveryListener discoveryListener;
    private final AckNackListener ackNackListener;
    private final Thread announcer;

    public RtpsDataWriter(RtpsParticipantConfig config, LocalEndpoint endpoint, PayloadSerializer<T> serializer) throws IOException {
        this(config, endpoint, serializer, new UdpRtpsTransport(config));
    }

    public RtpsDataWriter(LocalEndpoint endpoint, PayloadSerializer<T> serializer) throws IOException {
        this(new RtpsParticipantConfig(0), endpoint, serializer);
    }

    public RtpsDataWriter(RtpsParticipantConfig config, LocalEndpoint endpoint, PayloadSerializer<T> serializer, RtpsTransport transport) throws IOException {
        this.endpoint = endpoint;
        this.serializer = serializer;
        this.transport = transport;
        this.history = new WriterHistoryCache(endpoint.qos().history() == HistoryKind.KEEP_LAST
                ? endpoint.qos().depth()
                : DEFAULT_KEEP_ALL_WRITER_HISTORY_LIMIT);
        this.spdpAnnouncer = new SpdpAnnouncer(config, transport, guidPrefix);
        this.sedpPublicationAnnouncer = new SedpPublicationAnnouncer(endpoint, transport, guidPrefix, remoteParticipants);
        this.discoveryListener = new SpdpDiscoveryListener(transport, guidPrefix, remoteParticipants, new RemoteEndpointStore<>(), remoteSubscriptions);
        this.ackNackListener = new AckNackListener(
                transport,
                Set.of(RtpsEntity.USER_WRITER_NO_KEY, RtpsEntity.PUBLICATIONS_BUILTIN_TOPIC_WRITER),
                this::handleAckNack);
        this.announcer = new Thread(this::announceLoop, "ddsjdk-rtps-writer-announcer-" + endpoint.topicName());
        this.announcer.setDaemon(true);
        this.announcer.start();
    }

    public void write(T value) throws IOException {
        long sequenceNumber = userSequence.getAndIncrement();
        byte[] payload = serializer.serialize(value);
        history.put(sequenceNumber, payload);
        sendUserData(sequenceNumber, payload);
    }

    private void announceLoop() {
        while (running.get()) {
            try {
                spdpAnnouncer.announce();
                sedpPublicationAnnouncer.announce();
                if (endpoint.qos().reliability() == ReliabilityKind.RELIABLE) {
                    sendUserHeartbeat();
                }
                Thread.sleep(ANNOUNCE_INTERVAL_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (IOException e) {
                if (running.get()) {
                    throw new UncheckedIOException(e);
                }
            }
        }
    }

    private void handleAckNack(AckNack ackNack) {
        try {
            if (ackNack.writerId().equals(RtpsEntity.USER_WRITER_NO_KEY)) {
                resendRequestedSamples(ackNack);
            } else if (ackNack.writerId().equals(RtpsEntity.PUBLICATIONS_BUILTIN_TOPIC_WRITER)) {
                sedpPublicationAnnouncer.respondTo(ackNack);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void resendRequestedSamples(AckNack ackNack) throws IOException {
        if (endpoint.qos().reliability() != ReliabilityKind.RELIABLE) {
            return;
        }
        for (long sequenceNumber : ackNack.requestedSequenceNumbers()) {
            var payload = history.get(sequenceNumber);
            if (payload.isPresent()) {
                sendUserData(sequenceNumber, payload.get());
            } else {
                sendGap(ackNack.readerId(), sequenceNumber);
            }
        }
    }

    private void sendUserData(long sequenceNumber, byte[] payload) throws IOException {
        RtpsMessageBuilder message = new RtpsMessageBuilder(guidPrefix);
        message.infoTs(RtpsTimestamp.now());
        message.data(RtpsEntity.USER_READER_NO_KEY, RtpsEntity.USER_WRITER_NO_KEY, sequenceNumber, payload);
        sendToUserLocators(message.bytes());
    }

    private void sendUserHeartbeat() throws IOException {
        var firstSequenceNumber = history.firstSequenceNumber();
        var lastSequenceNumber = history.lastSequenceNumber();
        if (firstSequenceNumber.isEmpty() || lastSequenceNumber.isEmpty()) {
            return;
        }
        RtpsMessageBuilder message = new RtpsMessageBuilder(guidPrefix);
        message.heartbeat(
                RtpsEntity.USER_READER_NO_KEY,
                RtpsEntity.USER_WRITER_NO_KEY,
                firstSequenceNumber.get(),
                lastSequenceNumber.get(),
                (int) userHeartbeatCount.getAndIncrement());
        sendToUserLocators(message.bytes());
    }

    private void sendGap(EntityId readerId, long sequenceNumber) throws IOException {
        RtpsMessageBuilder message = new RtpsMessageBuilder(guidPrefix);
        message.gap(readerId, RtpsEntity.USER_WRITER_NO_KEY, sequenceNumber);
        sendToUserLocators(message.bytes());
    }

    private void sendToUserLocators(byte[] message) throws IOException {
        transport.sendUserData(message);
        for (RemoteParticipant participant : remoteParticipantsForMatchedSubscriptions()) {
            for (var locator : participant.userUnicast()) {
                transport.send(message, locator);
            }
        }
    }

    private Set<RemoteParticipant> remoteParticipantsForMatchedSubscriptions() {
        Set<GuidPrefix> matchedPrefixes = new HashSet<>();
        for (RemoteSubscription subscription : remoteSubscriptions) {
            if (subscription.topicName().equals(endpoint.topicName())
                    && subscription.typeName().equals(endpoint.typeName())
                    && endpoint.qos().isCompatibleWithRequested(subscription.qos())) {
                matchedPrefixes.add(subscription.endpointGuid().prefix());
            }
        }
        if (matchedPrefixes.isEmpty()) {
            return Set.of();
        }
        Set<RemoteParticipant> result = new HashSet<>();
        for (RemoteParticipant participant : remoteParticipants) {
            if (matchedPrefixes.contains(participant.guidPrefix())) {
                result.add(participant);
            }
        }
        return result;
    }

    @Override
    public void close() throws IOException {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        announcer.interrupt();
        IOException first = null;
        first = closeOrCapture(() -> sedpPublicationAnnouncer.disposeAndUnregister(), first);
        first = closeOrCapture(ackNackListener, first);
        first = closeOrCapture(discoveryListener, first);
        first = closeOrCapture(transport, first);
        if (first != null) {
            throw first;
        }
    }

    private static IOException closeOrCapture(Closeable closeable, IOException first) {
        try {
            closeable.close();
        } catch (IOException e) {
            if (first == null) {
                return e;
            }
            first.addSuppressed(e);
        }
        return first;
    }
}
