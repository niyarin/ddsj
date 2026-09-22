package ddsjdk.rtps.runtime;

import ddsjdk.rtps.qos.EndpointQos;
import ddsjdk.rtps.discovery.LocalEndpoint;
import ddsjdk.rtps.message.RtpsMessageBuilder;
import ddsjdk.rtps.protocol.RtpsEntity;
import ddsjdk.rtps.transport.*;
import ddsjdk.rtps.types.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.Closeable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(10)
class ReaderReceiveApiTest {
    private static final GuidPrefix PREFIX = new GuidPrefix(new byte[12]);
    private static final PayloadSerializer<Integer> CODEC = new PayloadSerializer<>() {
        public byte[] serialize(Integer value) { return new byte[]{value.byteValue()}; }
        public Integer deserialize(byte[] payload) { return (int) payload[0]; }
    };

    private RtpsDataReader<Integer> reader(FakeTransport transport, PayloadSerializer<Integer> codec) throws Exception {
        return new RtpsDataReader<>(new RtpsParticipantConfig(0),
                new LocalEndpoint("topic", "int", EndpointQos.DEFAULT), codec, transport);
    }

    @Test void pollAndBoundedDrainRemoveValuesInOrder() throws Exception {
        var transport = new FakeTransport();
        try (var reader = reader(transport, CODEC)) {
            assertEquals(Optional.empty(), reader.poll());
            transport.deliverData(1); transport.deliverData(2); transport.deliverData(3);
            assertEquals(List.of(), reader.drain(0));
            assertEquals(List.of(1, 2), reader.drain(2));
            assertEquals(Optional.of(3), reader.poll(Duration.ZERO));
            assertEquals(List.of(), reader.drain());
            transport.deliverData(4); transport.deliverData(5);
            assertEquals(List.of(4, 5), reader.drain());
        }
    }

    @Test void validatesTimeoutAndBatchSizeWithoutConsuming() throws Exception {
        try (var reader = reader(new FakeTransport(), CODEC)) {
            assertThrows(IllegalArgumentException.class, () -> reader.drain(-1));
            assertThrows(IllegalArgumentException.class, () -> reader.poll(Duration.ofNanos(-1)));
            assertThrows(IllegalArgumentException.class, () -> reader.poll(Duration.ofSeconds(Long.MAX_VALUE)));
            assertThrows(NullPointerException.class, () -> reader.poll(null));
            assertEquals(Optional.empty(), reader.poll(Duration.ZERO));
            long start = System.nanoTime();
            assertEquals(Optional.empty(), reader.poll(Duration.ofMillis(20)));
            assertTrue(System.nanoTime() - start >= Duration.ofMillis(20).toNanos());
        }
    }

    @Test void deliveryWakesWaitingPoll() throws Exception {
        var transport = new FakeTransport();
        try (var reader = reader(transport, CODEC)) {
            var result = new FutureTask<>(() -> reader.poll(Duration.ofSeconds(30)));
            Thread worker = new Thread(result);
            worker.start();
            try {
                awaitWaiting(worker);
                transport.deliverData(1);
                assertEquals(Optional.of(1), result.get(2, TimeUnit.SECONDS));
                assertEquals(Optional.empty(), reader.poll());
            } finally {
                worker.interrupt();
                worker.join(2000);
            }
        }
    }

    @Test void interruptionIsReportedAndDoesNotConsumeNextValue() throws Exception {
        var transport = new FakeTransport();
        try (var reader = reader(transport, CODEC)) {
            var result = new FutureTask<>(() -> assertThrows(InterruptedException.class,
                    () -> reader.poll(Duration.ofSeconds(30))));
            Thread worker = new Thread(result);
            worker.start();
            try {
                awaitWaiting(worker);
                worker.interrupt();
                result.get(2, TimeUnit.SECONDS);
                transport.deliverData(1);
                assertEquals(Optional.of(1), reader.poll());
            } finally {
                worker.interrupt();
                worker.join(2000);
            }
        }
    }

    @Test void preexistingInterruptDoesNotConsumeQueuedValue() throws Exception {
        var transport = new FakeTransport();
        try (var reader = reader(transport, CODEC)) {
            transport.deliverData(1);
            Thread.currentThread().interrupt();
            try {
                assertThrows(InterruptedException.class, () -> reader.poll(Duration.ZERO));
            } finally {
                Thread.interrupted();
            }
            assertEquals(Optional.of(1), reader.poll());
        }
    }

    @Test void closeWakesWaitersAndRejectsFurtherReads() throws Exception {
        try (var reader = reader(new FakeTransport(), CODEC)) {
            var result = new FutureTask<>(() -> assertThrows(IllegalStateException.class,
                    () -> reader.poll(Duration.ofSeconds(30))));
            Thread worker = new Thread(result);
            worker.start();
            try {
                awaitWaiting(worker);
                reader.close();
                result.get(2, TimeUnit.SECONDS);
                assertThrows(IllegalStateException.class, reader::poll);
                assertThrows(IllegalStateException.class, reader::drain);
                assertThrows(IllegalStateException.class, () -> reader.poll(Duration.ZERO));
            } finally {
                worker.interrupt();
                worker.join(2000);
            }
        }
    }

    @Test void decodingFailureIsObservableAndSameSequenceCanBeRetried() throws Exception {
        var transport = new FakeTransport();
        var fail = new AtomicBoolean(true);
        var cause = new IllegalArgumentException("bad payload");
        var codec = new PayloadSerializer<Integer>() {
            public byte[] serialize(Integer value) { return CODEC.serialize(value); }
            public Integer deserialize(byte[] bytes) {
                if (fail.get()) { throw cause; }
                return CODEC.deserialize(bytes);
            }
        };
        try (var reader = reader(transport, codec)) {
            assertTrue(reader.lastDeserializationError().isEmpty());
            var errors = new CopyOnWriteArrayList<DeserializationError>();
            reader.onDeserializationError(errors::add);
            transport.deliverData(1);
            assertEquals(1, reader.deserializationErrorCount());
            assertEquals(1, errors.size());
            var error = reader.lastDeserializationError().orElseThrow();
            assertSame(cause, error.cause());
            assertEquals(PREFIX.toGuid(RtpsEntity.USER_WRITER_NO_KEY), error.writerGuid());
            assertEquals(1, error.sequenceNumber());
            assertEquals(error, errors.get(0));
            assertTrue(reader.poll().isEmpty());
            assertEquals(0, reader.sampleRejectedCount());
            fail.set(false);
            transport.deliverData(1);
            transport.deliverData(1);
            assertEquals(List.of(1), reader.drain());
        }
    }

    @Test void nullDecodeIsRecordedWithoutCallbackAndCallbackFailureDoesNotStopReception() throws Exception {
        var transport = new FakeTransport();
        var codec = new PayloadSerializer<Integer>() {
            public byte[] serialize(Integer value) { return CODEC.serialize(value); }
            public Integer deserialize(byte[] bytes) { return bytes[0] == 1 ? null : (int) bytes[0]; }
        };
        try (var reader = reader(transport, codec)) {
            transport.deliverData(1);
            assertInstanceOf(NullPointerException.class, reader.lastDeserializationError().orElseThrow().cause());
            var callbackFailure = new IllegalStateException("callback failed");
            reader.onDeserializationError(error -> { throw callbackFailure; });
            // System.Logger uses JUL in this test environment. Capture only the expected
            // warning; unrelated warnings still go through the original logging setup.
            var logger = Logger.getLogger(RtpsDataReader.class.getName());
            var originalFilter = logger.getFilter();
            var originalLevel = logger.getLevel();
            var warnings = new CopyOnWriteArrayList<LogRecord>();
            try {
                logger.setLevel(Level.ALL);
                logger.setFilter(record -> {
                    if (record.getLevel().equals(Level.WARNING)
                            && record.getMessage().equals("Deserialization error callback failed")
                            && record.getThrown() == callbackFailure) {
                        warnings.add(record);
                        return false;
                    }
                    return originalFilter == null || originalFilter.isLoggable(record);
                });
                assertDoesNotThrow(() -> transport.deliverData(1));
                assertEquals(1, warnings.size(), "Callback failure must produce a warning");
                assertSame(callbackFailure, warnings.get(0).getThrown());
            } finally {
                logger.setFilter(originalFilter);
                logger.setLevel(originalLevel);
            }
            transport.deliverData(2);
            assertEquals(Optional.of(2), reader.poll());
            assertEquals(2, reader.deserializationErrorCount());
        }
    }

    @SuppressWarnings("deprecation")
    @Test void legacyApiDelegatesAndRestoresInterruptFlag() throws Exception {
        var transport = new FakeTransport();
        try (var reader = reader(transport, CODEC)) {
            transport.deliverData(1);
            assertEquals(1, reader.read());
            assertNull(reader.read(Duration.ZERO));
            transport.deliverData(2);
            Thread.currentThread().interrupt();
            try {
                assertNull(reader.read(Duration.ZERO));
                assertTrue(Thread.currentThread().isInterrupted());
            } finally {
                Thread.interrupted();
            }
            assertEquals(List.of(2), reader.take());
        }
    }

    @Test void finiteQosTracksStatusWithoutCallbacks() throws Exception {
        var transport = new FakeTransport();
        try (var reader = new RtpsDataReader<>(new RtpsParticipantConfig(0),
                monitoredEndpoint(), CODEC, transport)) {
            assertEquals(0, reader.deadlineMissedCount());
            assertEquals(0, reader.livelinessAliveCount());
            transport.deliverData(1);
            awaitCondition(() -> reader.deadlineMissedCount() > 0
                    && reader.livelinessNotAliveCount() == 1);
            assertEquals(0, reader.livelinessAliveCount());
            assertEquals(List.of(1), reader.drain());
        }
    }

    @Test void finiteQosStillNotifiesCallbacks() throws Exception {
        var transport = new FakeTransport();
        var deadlines = new CopyOnWriteArrayList<DeadlineMissedStatus>();
        var liveliness = new CopyOnWriteArrayList<LivelinessChangedStatus>();
        try (var reader = new RtpsDataReader<>(new RtpsParticipantConfig(0),
                monitoredEndpoint(), CODEC, transport, ReaderListeners.builder().onDeadlineMissed(deadlines::add)
                        .onLivelinessChanged(liveliness::add).build())) {
            transport.deliverData(1);
            awaitCondition(() -> !deadlines.isEmpty()
                    && liveliness.stream().anyMatch(status -> !status.alive()));
            assertTrue(reader.deadlineMissedCount() >= deadlines.get(0).totalCount());
            assertTrue(liveliness.get(0).alive());
            assertEquals(PREFIX.toGuid(RtpsEntity.USER_WRITER_NO_KEY), liveliness.get(0).writerGuid());
            assertEquals(1, reader.livelinessNotAliveCount());
            assertEquals(0, reader.livelinessAliveCount());
        }
    }

    @Test void infiniteQosDoesNotMonitorEvenWithCallbacks() throws Exception {
        var transport = new FakeTransport();
        var deadlines = new CopyOnWriteArrayList<DeadlineMissedStatus>();
        var liveliness = new CopyOnWriteArrayList<LivelinessChangedStatus>();
        try (var reader = new RtpsDataReader<>(new RtpsParticipantConfig(0),
                new LocalEndpoint("topic", "int", EndpointQos.DEFAULT), CODEC,
                transport, ReaderListeners.builder().onDeadlineMissed(deadlines::add)
                        .onLivelinessChanged(liveliness::add).build())) {
            transport.deliverData(1);
            assertEquals(0, reader.deadlineMissedCount());
            assertEquals(0, reader.livelinessAliveCount());
            assertEquals(0, reader.livelinessNotAliveCount());
            assertTrue(deadlines.isEmpty());
            assertTrue(liveliness.isEmpty());
        }
    }

    @Test void listenersCaptureInitialErrorListenerAndBuilderChangesDoNotAffectReader() throws Exception {
        var transport = new FakeTransport();
        var errors = new CopyOnWriteArrayList<DeserializationError>();
        var replacement = new CopyOnWriteArrayList<DeserializationError>();
        var builder = ReaderListeners.builder().onDeserializationError(errors::add);
        var listeners = builder.build();
        builder.onDeserializationError(replacement::add);
        var codec = new PayloadSerializer<Integer>() {
            public byte[] serialize(Integer value) { return CODEC.serialize(value); }
            public Integer deserialize(byte[] bytes) { throw new IllegalArgumentException("bad payload"); }
        };
        try (var reader = new RtpsDataReader<>(new RtpsParticipantConfig(0),
                monitoredEndpoint(), codec, transport, listeners)) {
            transport.deliverData(1);
            assertEquals(1, errors.size());
            assertTrue(replacement.isEmpty());
            reader.onDeserializationError(replacement::add);
            transport.deliverData(2);
            assertEquals(1, errors.size());
            assertEquals(1, replacement.size());
            assertEquals(2, reader.deserializationErrorCount());
        }
    }

    @SuppressWarnings("deprecation")
    @Test void legacyCallbacksStillDelegateToListeners() throws Exception {
        var transport = new FakeTransport();
        var events = new CopyOnWriteArrayList<LivelinessChangedStatus>();
        try (var reader = new RtpsDataReader<>(new RtpsParticipantConfig(0),
                monitoredEndpoint(), CODEC, transport, null, events::add)) {
            transport.deliverData(1);
            awaitCondition(() -> events.stream().anyMatch(status -> !status.alive()));
            assertTrue(events.get(0).alive());
            assertEquals(1, reader.livelinessNotAliveCount());
        }
    }

    private static LocalEndpoint monitoredEndpoint() {
        return new LocalEndpoint("topic", "int", new EndpointQos(
                EndpointQos.ReliabilityKind.BEST_EFFORT, EndpointQos.DurabilityKind.VOLATILE,
                EndpointQos.HistoryKind.KEEP_LAST, 10, Duration.ofMillis(50),
                EndpointQos.OwnershipKind.SHARED, 0, EndpointQos.LivelinessKind.AUTOMATIC,
                Duration.ofMillis(50)));
    }

    private static void awaitCondition(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long start = System.nanoTime();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() - start > TimeUnit.SECONDS.toNanos(3)) {
                fail("Monitoring status did not change before timeout");
            }
            Thread.sleep(5);
        }
    }

    private static void awaitWaiting(Thread thread) {
        long start = System.nanoTime();
        while (thread.getState() != Thread.State.TIMED_WAITING) {
            if (!thread.isAlive() || System.nanoTime() - start > TimeUnit.SECONDS.toNanos(2)) {
                fail("poll did not enter waiting state");
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
    }
    private static final class FakeTransport implements RtpsTransport {
        private final List<PacketHandler> handlers = new CopyOnWriteArrayList<>();
        private final List<byte[]> sent = new CopyOnWriteArrayList<>();
        public InetAddress multicastGroup() { return InetAddress.getLoopbackAddress(); }
        public void sendMetatraffic(byte[] message) {}
        public void sendUserData(byte[] message) { sent.add(message.clone()); }
        public void send(byte[] message, InetSocketAddress address) {}
        public Closeable listenMetatraffic(PacketHandler handler) { return () -> {}; }
        public Closeable listenUserData(PacketHandler handler) {
            handlers.add(handler); return () -> handlers.remove(handler);
        }
        public Locator unicastLocator(int port) { return new Locator(multicastGroup(), port); }
        public Locator multicastLocator(int port) { return unicastLocator(port); }
        public void close() { handlers.clear(); }
        void deliver(byte[] bytes) {
            var packet = new RtpsPacket(bytes, bytes.length);
            handlers.forEach(handler -> handler.handle(packet));
        }
        void deliverData(int sequence) {
            var message = new RtpsMessageBuilder(PREFIX);
            message.data(RtpsEntity.USER_READER_NO_KEY, RtpsEntity.USER_WRITER_NO_KEY, sequence, new byte[]{(byte) sequence});
            deliver(message.bytes());
        }
    }
}
