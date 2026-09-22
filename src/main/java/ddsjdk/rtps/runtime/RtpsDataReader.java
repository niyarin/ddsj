package ddsjdk.rtps.runtime;

import ddsjdk.rtps.util.Closeables;
import ddsjdk.rtps.qos.EndpointQos.ReliabilityKind;
import ddsjdk.rtps.discovery.LocalEndpoint;
import ddsjdk.rtps.discovery.RemoteParticipant;
import ddsjdk.rtps.history.ReaderHistoryCache;
import ddsjdk.rtps.history.ReaderSampleQueue;
import ddsjdk.rtps.message.Heartbeat;
import ddsjdk.rtps.message.RtpsMessageBuilder;
import ddsjdk.rtps.message.RtpsUserDataReader;
import ddsjdk.rtps.message.UserDataSample;
import ddsjdk.rtps.transport.RtpsParticipantConfig;
import ddsjdk.rtps.transport.RtpsTransport;
import ddsjdk.rtps.types.EntityId;
import ddsjdk.rtps.types.Guid;
import ddsjdk.rtps.types.GuidPrefix;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public final class RtpsDataReader<T> implements Closeable {

    private final LocalEndpoint endpoint;
    private final PayloadSerializer<T> serializer;
    private final RtpsTransport transport;
    private final RtpsParticipant participant;
    private final boolean ownsParticipant;
    private final EntityId entityId;
    private final GuidPrefix guidPrefix;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ReaderSampleQueue<T> messages;
    private final AtomicLong sampleRejectedCount = new AtomicLong();
    private final AtomicLong deserializationErrorCount = new AtomicLong();
    private volatile DeserializationError lastDeserializationError;
    private volatile Consumer<DeserializationError> onDeserializationError = ignored -> { };
    private final ReaderHistoryCache history = new ReaderHistoryCache();
    private final AtomicLong ackNackCount = new AtomicLong(1);
    private final EndpointResolver endpointResolver;
    private final RtpsUserDataReader userDataReader;
    private final DeadlineMonitor deadlineMonitor;
    private final LivelinessMonitor livelinessMonitor;

    public RtpsDataReader(RtpsParticipantConfig config, LocalEndpoint endpoint, PayloadSerializer<T> serializer) throws IOException {
        this(config, endpoint, serializer, ReaderListeners.DEFAULT);
    }

    public RtpsDataReader(LocalEndpoint endpoint, PayloadSerializer<T> serializer) throws IOException {
        this(new RtpsParticipantConfig(0), endpoint, serializer);
    }

    /** Creates a reader with named notification listeners and an owned participant. */
    public RtpsDataReader(RtpsParticipantConfig config, LocalEndpoint endpoint, PayloadSerializer<T> serializer,
            ReaderListeners listeners) throws IOException {
        this(RtpsParticipant.owned(requireListeners(config, listeners), endpoint, serializer), endpoint, serializer, true, listeners);
    }

    public RtpsDataReader(RtpsParticipantConfig config, LocalEndpoint endpoint, PayloadSerializer<T> serializer,
            RtpsTransport transport) throws IOException {
        this(config, endpoint, serializer, transport, ReaderListeners.DEFAULT);
    }

    /** Creates a reader with named notification listeners; the participant takes ownership of transport. */
    public RtpsDataReader(RtpsParticipantConfig config, LocalEndpoint endpoint, PayloadSerializer<T> serializer,
            RtpsTransport transport, ReaderListeners listeners) throws IOException {
        this(RtpsParticipant.owned(requireListeners(config, listeners), endpoint, serializer, transport), endpoint, serializer, true, listeners);
    }

    private static RtpsParticipantConfig requireListeners(RtpsParticipantConfig config, ReaderListeners listeners) {
        Objects.requireNonNull(listeners, "listeners");
        return config;
    }

    /** @deprecated Use the constructor accepting {@link ReaderListeners}. */
    @Deprecated
    public RtpsDataReader(RtpsParticipantConfig config, LocalEndpoint endpoint, PayloadSerializer<T> serializer,
            Consumer<DeadlineMissedStatus> onDeadlineMissed) throws IOException {
        this(config, endpoint, serializer, ReaderListeners.legacy(onDeadlineMissed, null));
    }

    /** @deprecated Use the constructor accepting {@link ReaderListeners}. */
    @Deprecated
    public RtpsDataReader(RtpsParticipantConfig config, LocalEndpoint endpoint, PayloadSerializer<T> serializer,
            RtpsTransport transport, Consumer<DeadlineMissedStatus> onDeadlineMissed) throws IOException {
        this(config, endpoint, serializer, transport, ReaderListeners.legacy(onDeadlineMissed, null));
    }

    /** @deprecated Use the constructor accepting {@link ReaderListeners}. */
    @Deprecated
    public RtpsDataReader(RtpsParticipantConfig config, LocalEndpoint endpoint, PayloadSerializer<T> serializer,
            Consumer<DeadlineMissedStatus> onDeadlineMissed,
            Consumer<LivelinessChangedStatus> onLivelinessChanged) throws IOException {
        this(config, endpoint, serializer, ReaderListeners.legacy(onDeadlineMissed, onLivelinessChanged));
    }

    /** @deprecated Use the constructor accepting {@link ReaderListeners}. */
    @Deprecated
    public RtpsDataReader(RtpsParticipantConfig config, LocalEndpoint endpoint, PayloadSerializer<T> serializer,
            RtpsTransport transport, Consumer<DeadlineMissedStatus> onDeadlineMissed,
            Consumer<LivelinessChangedStatus> onLivelinessChanged) throws IOException {
        this(config, endpoint, serializer, transport, ReaderListeners.legacy(onDeadlineMissed, onLivelinessChanged));
    }

    RtpsDataReader(RtpsParticipant participant, LocalEndpoint endpoint, PayloadSerializer<T> serializer,
            boolean ownsParticipant, ReaderListeners listeners) throws IOException {
        Objects.requireNonNull(endpoint, "endpoint");
        Objects.requireNonNull(serializer, "serializer");
        Objects.requireNonNull(listeners, "listeners");
        this.onDeserializationError = listeners.deserializationListener();
        this.participant = participant;
        this.ownsParticipant = ownsParticipant;
        List<Closeable> opened = new ArrayList<>();
        if (ownsParticipant) opened.add(participant);
        try {
            this.entityId = participant.allocateEntityId(false);
            this.guidPrefix = participant.guidPrefix();
            this.endpointResolver = new EndpointResolver(endpoint, participant.remoteParticipants(),
                    participant.publications(), participant.subscriptions());
            this.endpoint = endpoint;
            this.messages = new ReaderSampleQueue<>(endpoint.qos().history(), endpoint.qos().depth(), endpoint.resourceLimits());
            this.serializer = serializer;
            this.transport = participant.transport();
            this.userDataReader = new RtpsUserDataReader(transport, entityId, this::onSample, this::onHeartbeat);
            opened.add(userDataReader);

            // Initialize deadline monitor if deadline is finite
            if (endpoint.qos().hasFiniteDeadline()) {
                this.deadlineMonitor = new DeadlineMonitor(endpoint.qos().deadline(), listeners.deadlineListener());
                opened.add(deadlineMonitor);
            } else {
                this.deadlineMonitor = null;
            }

            // Initialize liveliness monitor if lease duration is finite
            if (endpoint.qos().hasFiniteLeaseDuration()) {
                this.livelinessMonitor = new LivelinessMonitor(endpoint.qos().leaseDuration(), listeners.livelinessListener());
                opened.add(livelinessMonitor);
            } else {
                this.livelinessMonitor = null;
            }

            participant.register(guid(), endpoint, this, false);
            running.set(true);
        } catch (IOException | RuntimeException e) {
            Closeables.rollback(e, opened);
            throw e;
        }
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
     * Returns the total number of deadline misses, independently of callback registration.
     * Returns 0 if the deadline is infinite.
     */
    public long deadlineMissedCount() {
        return deadlineMonitor != null ? deadlineMonitor.totalMissedCount() : 0;
    }

    /**
     * Returns the number of currently alive writers, independently of callback registration.
     * Returns 0 if the lease duration is infinite.
     */
    public long livelinessAliveCount() {
        return livelinessMonitor != null ? livelinessMonitor.aliveCount() : 0;
    }

    /**
     * Returns the number of writers that became not alive, independently of callback registration.
     * Returns 0 if the lease duration is infinite.
     */
    public long livelinessNotAliveCount() {
        return livelinessMonitor != null ? livelinessMonitor.notAliveCount() : 0;
    }

    private synchronized void onSample(UserDataSample sample) {
        if (!running.get()) {
            return;
        }
        if (!endpointResolver.acceptsPublication(sample.writerGuid(), ownsParticipant)) {
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

    private synchronized void onHeartbeat(Heartbeat heartbeat) {
        if (!running.get()) return;
        if (endpoint.qos().reliability() != ReliabilityKind.RELIABLE) {
            return;
        }
        if (!endpointResolver.acceptsPublication(heartbeat.writerGuid(), ownsParticipant)) {
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
                entityId,
                writerGuid.entityId(),
                baseSequenceNumber,
                missingSequenceNumbers,
                (int) ackNackCount.getAndIncrement());
        transport.sendUserData(message.bytes());
        for (RemoteParticipant participant : endpointResolver.participantsForPublication(writerGuid, ownsParticipant)) {
            for (var locator : participant.userUnicast()) {
                transport.send(message.bytes(), locator);
            }
        }
    }

    /** Identity allocated by the owning participant. */
    public Guid guid() { return guidPrefix.toGuid(entityId); }

    @Override
    public synchronized void close() throws IOException {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        notifyAll();
        List<Closeable> resources = new ArrayList<>();
        if (deadlineMonitor != null) resources.add(deadlineMonitor);
        if (livelinessMonitor != null) resources.add(livelinessMonitor);
        resources.add(() -> participant.unregister(guid(), false));
        resources.add(userDataReader);
        if (ownsParticipant) resources.add(participant);
        try {
            Closeables.closeAll(resources);
        } finally {
            messages.clear();
        }
    }
}
