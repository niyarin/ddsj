package ddsjdk.rtps.runtime;

import ddsjdk.rtps.discovery.EndpointQos.ReliabilityKind;
import ddsjdk.rtps.discovery.LocalEndpoint;
import ddsjdk.rtps.discovery.RemoteParticipant;
import ddsjdk.rtps.discovery.RemoteParticipantStore;
import ddsjdk.rtps.discovery.RemoteEndpointStore;
import ddsjdk.rtps.discovery.RemotePublication;
import ddsjdk.rtps.discovery.SedpSubscriptionAnnouncer;
import ddsjdk.rtps.discovery.SpdpAnnouncer;
import ddsjdk.rtps.discovery.SpdpDiscoveryListener;
import ddsjdk.rtps.history.ReaderHistoryCache;
import ddsjdk.rtps.message.AckNackListener;
import ddsjdk.rtps.message.Heartbeat;
import ddsjdk.rtps.message.RtpsMessageBuilder;
import ddsjdk.rtps.message.RtpsUserDataReader;
import ddsjdk.rtps.message.UserDataSample;
import ddsjdk.rtps.protocol.RtpsEntity;
import ddsjdk.rtps.transport.RtpsParticipantConfig;
import ddsjdk.rtps.transport.RtpsTransport;
import ddsjdk.rtps.transport.UdpRtpsTransport;
import ddsjdk.rtps.types.Guid;
import ddsjdk.rtps.types.GuidPrefix;
import ddsjdk.rtps.types.RtpsGuid;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public final class RtpsDataReader<T> implements Closeable {
    private static final long ANNOUNCE_INTERVAL_MILLIS = 1000L;

    private final LocalEndpoint endpoint;
    private final PayloadSerializer<T> serializer;
    private final RtpsTransport transport;
    private final GuidPrefix guidPrefix = RtpsGuid.newGuidPrefix();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Queue<T> messages = new ConcurrentLinkedQueue<>();
    private final ReaderHistoryCache history = new ReaderHistoryCache();
    private final AtomicLong ackNackCount = new AtomicLong(1);
    private final RemoteParticipantStore remoteParticipants = new RemoteParticipantStore();
    private final RemoteEndpointStore<RemotePublication> remotePublications = new RemoteEndpointStore<>();
    private final SpdpAnnouncer spdpAnnouncer;
    private final SedpSubscriptionAnnouncer sedpSubscriptionAnnouncer;
    private final SpdpDiscoveryListener discoveryListener;
    private final AckNackListener ackNackListener;
    private final RtpsUserDataReader userDataReader;
    private final DeadlineMonitor deadlineMonitor;
    private final LivelinessMonitor livelinessMonitor;
    private final Thread announcer;

    public RtpsDataReader(RtpsParticipantConfig config, LocalEndpoint endpoint, PayloadSerializer<T> serializer) throws IOException {
        this(config, endpoint, serializer, new UdpRtpsTransport(config), null);
    }

    public RtpsDataReader(LocalEndpoint endpoint, PayloadSerializer<T> serializer) throws IOException {
        this(new RtpsParticipantConfig(0), endpoint, serializer);
    }

    public RtpsDataReader(
            RtpsParticipantConfig config,
            LocalEndpoint endpoint,
            PayloadSerializer<T> serializer,
            Consumer<DeadlineMonitor.DeadlineMissedStatus> onDeadlineMissed) throws IOException {
        this(config, endpoint, serializer, new UdpRtpsTransport(config), onDeadlineMissed);
    }

    public RtpsDataReader(RtpsParticipantConfig config, LocalEndpoint endpoint, PayloadSerializer<T> serializer, RtpsTransport transport) throws IOException {
        this(config, endpoint, serializer, transport, null);
    }

    public RtpsDataReader(
            RtpsParticipantConfig config,
            LocalEndpoint endpoint,
            PayloadSerializer<T> serializer,
            RtpsTransport transport,
            Consumer<DeadlineMonitor.DeadlineMissedStatus> onDeadlineMissed) throws IOException {
        this(config, endpoint, serializer, transport, onDeadlineMissed, null);
    }

    public RtpsDataReader(
            RtpsParticipantConfig config,
            LocalEndpoint endpoint,
            PayloadSerializer<T> serializer,
            Consumer<DeadlineMonitor.DeadlineMissedStatus> onDeadlineMissed,
            Consumer<LivelinessMonitor.LivelinessChangedStatus> onLivelinessChanged) throws IOException {
        this(config, endpoint, serializer, new UdpRtpsTransport(config), onDeadlineMissed, onLivelinessChanged);
    }

    public RtpsDataReader(
            RtpsParticipantConfig config,
            LocalEndpoint endpoint,
            PayloadSerializer<T> serializer,
            RtpsTransport transport,
            Consumer<DeadlineMonitor.DeadlineMissedStatus> onDeadlineMissed,
            Consumer<LivelinessMonitor.LivelinessChangedStatus> onLivelinessChanged) throws IOException {
        this.endpoint = endpoint;
        this.serializer = serializer;
        this.transport = transport;
        this.spdpAnnouncer = new SpdpAnnouncer(config, transport, guidPrefix);
        this.sedpSubscriptionAnnouncer = new SedpSubscriptionAnnouncer(endpoint, transport, guidPrefix, remoteParticipants);
        this.discoveryListener = new SpdpDiscoveryListener(transport, guidPrefix, remoteParticipants, remotePublications, new RemoteEndpointStore<>());
        this.ackNackListener = new AckNackListener(
                transport,
                Set.of(RtpsEntity.SUBSCRIPTIONS_BUILTIN_TOPIC_WRITER),
                ackNack -> {
                    try {
                        sedpSubscriptionAnnouncer.respondTo(ackNack);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
        this.userDataReader = new RtpsUserDataReader(transport, RtpsEntity.USER_READER_NO_KEY, this::onSample, this::onHeartbeat);

        // Initialize deadline monitor if deadline is finite
        if (endpoint.qos().hasFiniteDeadline() && onDeadlineMissed != null) {
            this.deadlineMonitor = new DeadlineMonitor(endpoint.qos().deadline(), onDeadlineMissed);
        } else {
            this.deadlineMonitor = null;
        }

        // Initialize liveliness monitor if lease duration is finite
        if (endpoint.qos().hasFiniteLeaseDuration() && onLivelinessChanged != null) {
            this.livelinessMonitor = new LivelinessMonitor(endpoint.qos().leaseDuration(), onLivelinessChanged);
        } else {
            this.livelinessMonitor = null;
        }

        this.announcer = new Thread(this::announceLoop, "ddsjdk-rtps-reader-announcer-" + endpoint.topicName());
        this.announcer.setDaemon(true);
        this.announcer.start();
    }

    public List<T> take() {
        List<T> result = new ArrayList<>();
        T message;
        while ((message = messages.poll()) != null) {
            result.add(message);
        }
        return result;
    }

    public T read(Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            T message = messages.poll();
            if (message != null) {
                return message;
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return messages.poll();
            }
        }
        return messages.poll();
    }

    public T read() {
        return read(Duration.ofMillis(100));
    }

    /**
     * Returns the total number of deadline misses, or 0 if no deadline is configured.
     */
    public long deadlineMissedCount() {
        return deadlineMonitor != null ? deadlineMonitor.totalMissedCount() : 0;
    }

    /**
     * Returns the number of currently alive writers, or 0 if no liveliness monitoring is configured.
     */
    public long livelinessAliveCount() {
        return livelinessMonitor != null ? livelinessMonitor.aliveCount() : 0;
    }

    /**
     * Returns the number of writers that became not alive, or 0 if no liveliness monitoring is configured.
     */
    public long livelinessNotAliveCount() {
        return livelinessMonitor != null ? livelinessMonitor.notAliveCount() : 0;
    }

    private void announceLoop() {
        while (running.get()) {
            try {
                spdpAnnouncer.announce();
                sedpSubscriptionAnnouncer.announce();
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

    private void onSample(UserDataSample sample) {
        if (!isMatchedOrUndiscoveredPublication(sample.writerGuid())) {
            return;
        }
        if (!history.record(sample)) {
            return;
        }
        try {
            messages.add(serializer.deserialize(sample.payload()));
            // Notify deadline monitor that data was received
            if (deadlineMonitor != null) {
                deadlineMonitor.notifyActivity();
            }
            // Notify liveliness monitor that writer is alive
            if (livelinessMonitor != null) {
                livelinessMonitor.assertLiveliness(sample.writerGuid());
            }
        } catch (RuntimeException ignored) {
        }
    }

    private void onHeartbeat(Heartbeat heartbeat) {
        if (endpoint.qos().reliability() != ReliabilityKind.RELIABLE) {
            return;
        }
        if (!isMatchedOrUndiscoveredPublication(heartbeat.writerGuid())) {
            return;
        }
        var missing = history.missingSequences(heartbeat);
        try {
            sendAckNack(heartbeat.writerGuid(), missing.baseSequenceNumber(), missing.missing());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void sendAckNack(Guid writerGuid, long baseSequenceNumber, Set<Long> missingSequenceNumbers) throws IOException {
        RtpsMessageBuilder message = new RtpsMessageBuilder(guidPrefix);
        message.ackNack(
                RtpsEntity.USER_READER_NO_KEY,
                writerGuid.entityId(),
                baseSequenceNumber,
                missingSequenceNumbers,
                (int) ackNackCount.getAndIncrement());
        transport.sendUserData(message.bytes());
        for (RemoteParticipant participant : remoteParticipantsForMatchedPublication(writerGuid)) {
            for (var locator : participant.userUnicast()) {
                transport.send(message.bytes(), locator);
            }
        }
    }

    private boolean isMatchedOrUndiscoveredPublication(Guid writerGuid) {
        var publication = remotePublications.get(writerGuid);
        if (publication.isEmpty()) {
            return true;
        }
        RemotePublication remote = publication.get();
        return remote.topicName().equals(endpoint.topicName())
                && remote.typeName().equals(endpoint.typeName())
                && remote.qos().isCompatibleWithRequested(endpoint.qos());
    }

    private Set<RemoteParticipant> remoteParticipantsForMatchedPublication(Guid writerGuid) {
        if (!isMatchedOrUndiscoveredPublication(writerGuid)) {
            return Set.of();
        }
        Set<RemoteParticipant> result = new HashSet<>();
        for (RemoteParticipant participant : remoteParticipants) {
            if (participant.guidPrefix().equals(writerGuid.prefix())) {
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
        if (deadlineMonitor != null) {
            deadlineMonitor.close();
        }
        if (livelinessMonitor != null) {
            livelinessMonitor.close();
        }
        first = closeOrCapture(() -> sedpSubscriptionAnnouncer.disposeAndUnregister(), first);
        first = closeOrCapture(userDataReader, first);
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
