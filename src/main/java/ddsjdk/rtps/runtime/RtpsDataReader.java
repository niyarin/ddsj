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
import ddsjdk.rtps.history.ReaderSampleQueue;
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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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
    private final ReaderSampleQueue<T> messages;
    private final AtomicLong sampleRejectedCount = new AtomicLong();
    private final AtomicLong deserializationErrorCount = new AtomicLong();
    private volatile DeserializationError lastDeserializationError;
    private volatile Consumer<DeserializationError> onDeserializationError = ignored -> { };
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
        this.messages = new ReaderSampleQueue<>(endpoint.qos().history(), endpoint.qos().depth(), endpoint.resourceLimits());
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

    /** Number of deliveries rejected because KEEP_ALL unread history was full. */
    public long sampleRejectedCount() {
        return sampleRejectedCount.get();
    }

    /** Removes up to maxSamples queued values in arrival order without waiting.
     * @throws IllegalArgumentException if maxSamples is negative
     * @throws IllegalStateException if this reader is closed
     */
    public synchronized List<T> drain(int maxSamples) {
        if (maxSamples < 0) {
            throw new IllegalArgumentException("maxSamples must not be negative");
        }
        ensureOpen();
        List<T> result = new ArrayList<>();
        T message;
        while (result.size() < maxSamples && (message = messages.poll()) != null) {
            result.add(message);
        }
        return result;
    }

    /** Removes all currently queued values without waiting.
     * @throws IllegalStateException if this reader is closed
     */
    public List<T> drain() {
        return drain(Integer.MAX_VALUE);
    }

    /** Removes one queued value without waiting; empty means no value is available.
     * @throws IllegalStateException if this reader is closed
     */
    public synchronized Optional<T> poll() {
        ensureOpen();
        return Optional.ofNullable(messages.poll());
    }

    /** Removes one value, waiting up to timeout; zero performs an immediate poll.
     * Empty means the timeout elapsed. Closing the reader wakes waiting callers.
     * @throws InterruptedException if interrupted before or during the wait
     * @throws IllegalStateException if this reader is closed
     * @throws IllegalArgumentException if timeout is negative or exceeds Long.MAX_VALUE nanoseconds
     */
    public synchronized Optional<T> poll(Duration timeout) throws InterruptedException {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must not be negative");
        }
        final long timeoutNanos;
        try {
            timeoutNanos = timeout.toNanos();
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("timeout is too large", e);
        }
        long start = System.nanoTime();
        long remaining = timeoutNanos;
        while (true) {
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
            ensureOpen();
            T message = messages.poll();
            if (message != null) {
                return Optional.of(message);
            }
            if (remaining <= 0) {
                return Optional.empty();
            }
            wait(remaining / 1_000_000, (int) (remaining % 1_000_000));
            remaining = timeoutNanos - (System.nanoTime() - start);
        }
    }

    private void ensureOpen() {
        if (!running.get()) {
            throw new IllegalStateException("reader is closed");
        }
    }

    /** @deprecated Use {@link #drain()} or {@link #drain(int)}. */
    @Deprecated
    public List<T> take() {
        return drain();
    }

    /** @deprecated Use {@link #poll(Duration)} to distinguish timeout from interruption.
     * This compatibility method returns null on timeout or interruption, restoring the interrupt flag.
     */
    @Deprecated
    public T read(Duration timeout) {
        try {
            return poll(timeout).orElse(null);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    /** @deprecated Use {@link #poll(Duration)}. This method waits up to 100 ms. */
    @Deprecated
    public T read() {
        return read(Duration.ofMillis(100));
    }

    /** Registers a non-blocking callback on the receiving thread, replacing the previous callback.
     * Callback exceptions are logged and do not stop reception. Past errors are not replayed.
     */
    public void onDeserializationError(Consumer<DeserializationError> listener) {
        onDeserializationError = Objects.requireNonNull(listener, "listener");
    }

    /** Number of failed decoding attempts, including null results and repeated deliveries. */
    public long deserializationErrorCount() {
        return deserializationErrorCount.get();
    }

    /** Latest decoding failure, retained even when no callback is registered. */
    public Optional<DeserializationError> lastDeserializationError() {
        return Optional.ofNullable(lastDeserializationError);
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

    private synchronized void onSample(UserDataSample sample) {
        if (!running.get()) {
            return;
        }
        if (!isMatchedOrUndiscoveredPublication(sample.writerGuid())) {
            return;
        }
        if (history.contains(sample)) {
            return;
        }
        final T value;
        try {
            value = Objects.requireNonNull(serializer.deserialize(sample.payload()), "deserializer returned null");
        } catch (RuntimeException cause) {
            var error = new DeserializationError(sample.writerGuid(), sample.sequenceNumber(), cause);
            lastDeserializationError = error;
            deserializationErrorCount.incrementAndGet();
            try {
                onDeserializationError.accept(error);
            } catch (RuntimeException callbackError) {
                System.getLogger(RtpsDataReader.class.getName()).log(
                        System.Logger.Level.WARNING, "Deserialization error callback failed", callbackError);
            }
            return;
        }
        if (!messages.offer(value)) {
            sampleRejectedCount.incrementAndGet();
            return;
        }
        history.record(sample);
        notifyAll();
        if (deadlineMonitor != null) {
            deadlineMonitor.notifyActivity();
        }
        if (livelinessMonitor != null) {
            livelinessMonitor.assertLiveliness(sample.writerGuid());
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
    public synchronized void close() throws IOException {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        notifyAll();
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
        messages.clear();
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
