package ddsj.dds.core;

import ddsj.dds.exception.ReturnCode;
import ddsj.dds.instance.InstanceHandle;
import ddsj.dds.instance.InstanceState;
import ddsj.dds.qos.*;
import ddsj.dds.qos.policies.*;
import ddsj.rtps.transport.*;
import ddsj.rtps.types.Locator;
import org.junit.jupiter.api.Test;
import java.io.Closeable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import static org.junit.jupiter.api.Assertions.*;

class KeyedDdsTest {
    record Sensor(@CdrKey int id, String value) { }
    record Composite(@CdrKey int group, @CdrKey @CdrFixedLength(2) byte[] id, String value) { }

    @Test void recordKeysSerializeOnlyKeyFieldsAndUseValueEquality() {
        var codec = TypeSupport.forCdrRecord(Composite.class, "Sensors");
        assertTrue(codec.hasKey());
        var first = new Composite(1, new byte[]{2, 3}, "first");
        var same = new Composite(1, new byte[]{2, 3}, "different");
        assertEquals(codec.extractKey(first), codec.extractKey(same));
        assertNotEquals(codec.extractKey(first), codec.extractKey(new Composite(2, new byte[]{2, 3}, "first")));
        assertArrayEquals(new byte[]{0, 1, 0, 2, 1, 0, 0, 0, 2, 3, 0, 0}, codec.serializeKey(first));
        var holder = codec.deserializeKey(codec.serializeKey(first));
        assertEquals(1, holder.group());
        assertArrayEquals(new byte[]{2, 3}, holder.id());
        assertEquals("", holder.value());
    }

    @Test void keyedReadCacheKeepsLatestForEveryInstanceAcrossReadCalls() throws Exception {
        var transport = new FakeTransport();
        try (var participant = new DomainParticipant(0, DomainParticipantQos.DEFAULT, transport)) {
            var topic = participant.createTopic("sensors", Sensor.class);
            var writer = participant.createPublisher().createDataWriter(topic);
            var reader = participant.createSubscriber().createDataReader(topic);
            assertEquals(ReturnCode.OK, writer.write(new Sensor(1, "old")));
            transport.deliverSent();
            assertEquals(1, reader.read().size());
            transport.sent.clear();
            writer.write(new Sensor(2, "other"));
            writer.write(new Sensor(1, "new"));
            transport.deliverSent();
            var samples = reader.read();
            assertEquals(Set.of(new Sensor(1, "new"), new Sensor(2, "other")),
                    new HashSet<>(samples.stream().map(s -> s.data()).toList()));
            assertNotEquals(reader.lookupInstance(new Sensor(1, "")), reader.lookupInstance(new Sensor(2, "")));
        }
    }

    @Test void disposeUnregisterAndRevivalExposeInvalidDataAndGenerationCounts() throws Exception {
        var transport = new FakeTransport();
        try (var participant = new DomainParticipant(0, DomainParticipantQos.DEFAULT, transport)) {
            var topic = participant.createTopic("sensors", Sensor.class);
            var writer = participant.createPublisher().createDataWriter(topic);
            var reader = participant.createSubscriber().createDataReader(topic);
            Sensor data = new Sensor(1, "live");
            writer.write(data);
            transport.deliverSent();
            var original = reader.take().get(0);
            InstanceHandle handle = writer.lookupInstance(data);
            transport.sent.clear();
            assertEquals(ReturnCode.OK, writer.dispose(data, handle));
            assertEquals(handle, writer.lookupInstance(data), "dispose must retain registration");
            transport.deliverSent();
            var disposed = reader.take().get(0);
            assertFalse(disposed.hasValidData());
            assertEquals(InstanceState.NOT_ALIVE_DISPOSED, disposed.instanceState());
            assertEquals(original.instanceHandle(), disposed.instanceHandle());
            transport.sent.clear();
            writer.write(data);
            transport.deliverSent();
            var revived = reader.take().get(0);
            assertTrue(revived.hasValidData());
            assertEquals(InstanceState.ALIVE, revived.instanceState());
            assertEquals(1, revived.info().disposedGenerationCount());
            transport.sent.clear();
            assertEquals(ReturnCode.OK, writer.unregisterInstance(data, handle));
            assertTrue(writer.lookupInstance(data).isNil());
            transport.deliverSent();
            var unregistered = reader.take().get(0);
            assertFalse(unregistered.hasValidData());
            assertEquals(InstanceState.NOT_ALIVE_NO_WRITERS, unregistered.instanceState());
            transport.sent.clear();
            writer.write(data);
            transport.deliverSent();
            assertEquals(1, reader.take().get(0).info().noWritersGenerationCount());
        }
    }

    @Test void unregisterOnlyReportsNoWritersAfterTheLastWriterUnregisters() throws Exception {
        var transport = new FakeTransport();
        try (var participant = new DomainParticipant(0, DomainParticipantQos.DEFAULT, transport)) {
            var topic = participant.createTopic("sensors", Sensor.class);
            var publisher = participant.createPublisher();
            var first = publisher.createDataWriter(topic);
            var second = publisher.createDataWriter(topic);
            var reader = participant.createSubscriber().createDataReader(topic);
            Sensor data = new Sensor(1, "live");
            first.write(data);
            second.write(data);
            first.unregisterInstance(data, InstanceHandle.NIL);
            transport.deliverSent();
            assertEquals(InstanceState.ALIVE, reader.take().get(0).instanceState());
            transport.sent.clear();
            second.unregisterInstance(data, InstanceHandle.NIL);
            transport.deliverSent();
            assertEquals(InstanceState.NOT_ALIVE_NO_WRITERS, reader.take().get(0).instanceState());
        }
    }

    @Test void writerReportsInstanceAndPerInstanceResourceLimits() throws Exception {
        var transport = new FakeTransport();
        try (var participant = new DomainParticipant(0, DomainParticipantQos.DEFAULT, transport)) {
            var topic = participant.createTopic("sensors", Sensor.class);
            var qos = DataWriterQos.builder().history(HistoryQosPolicy.keepAll())
                    .resourceLimits(ResourceLimitsQosPolicy.of(4, 2, 2)).build();
            var writer = participant.createPublisher().createDataWriter(topic, qos);
            assertEquals(ReturnCode.OK, writer.write(new Sensor(1, "a")));
            assertEquals(ReturnCode.OK, writer.write(new Sensor(1, "b")));
            assertEquals(ReturnCode.OUT_OF_RESOURCES, writer.write(new Sensor(1, "c")));
            assertEquals(ReturnCode.OK, writer.write(new Sensor(2, "a")));
            assertEquals(ReturnCode.OUT_OF_RESOURCES, writer.write(new Sensor(3, "a")));
        }
    }

    @Test void readerKeepAllAccountsForSamplesRetainedByReadUntilTake() throws Exception {
        var transport = new FakeTransport();
        try (var participant = new DomainParticipant(0, DomainParticipantQos.DEFAULT, transport)) {
            var topic = participant.createTopic("sensors", Sensor.class);
            var writer = participant.createPublisher().createDataWriter(topic,
                    DataWriterQos.builder().history(HistoryQosPolicy.keepAll()).build());
            var reader = participant.createSubscriber().createDataReader(topic, DataReaderQos.builder()
                    .history(HistoryQosPolicy.keepAll()).resourceLimits(ResourceLimitsQosPolicy.of(3, 2, 2)).build());
            writer.write(new Sensor(1, "a"));
            writer.write(new Sensor(1, "b"));
            transport.deliverSent();
            assertEquals(2, reader.read().size());
            transport.sent.clear();
            writer.write(new Sensor(1, "c"));
            transport.deliverSent();
            assertEquals(2, reader.read().size());
            assertEquals(2, reader.take().size());
            // The rejected packet was not acknowledged as received, so it can be retried.
            transport.deliverSent();
            assertEquals("c", reader.take().get(0).data().value());
        }
    }

    @Test void closingAWriterUnregistersItsInstancesEvenWhenKeepAllIsFull() throws Exception {
        var transport = new FakeTransport();
        try (var participant = new DomainParticipant(0, DomainParticipantQos.DEFAULT, transport)) {
            var topic = participant.createTopic("sensors", Sensor.class);
            var writer = participant.createPublisher().createDataWriter(topic, DataWriterQos.builder()
                    .history(HistoryQosPolicy.keepAll()).resourceLimits(ResourceLimitsQosPolicy.of(1, 1, 1)).build());
            var reader = participant.createSubscriber().createDataReader(topic);
            writer.write(new Sensor(1, "live"));
            transport.deliverSent();
            reader.take();
            transport.sent.clear();
            writer.close();
            transport.deliverSent();
            var sample = reader.take().get(0);
            assertFalse(sample.hasValidData());
            assertEquals(InstanceState.NOT_ALIVE_NO_WRITERS, sample.instanceState());
        }
    }

    @Test void closingUsesTheRegisteredKeySnapshotAfterAnArrayKeyIsMutated() throws Exception {
        var transport = new FakeTransport();
        try (var participant = new DomainParticipant(0, DomainParticipantQos.DEFAULT, transport)) {
            var topic = participant.createTopic("composite", Composite.class);
            var writer = participant.createPublisher().createDataWriter(topic);
            var reader = participant.createSubscriber().createDataReader(topic);
            byte[] key = {2, 3};
            writer.write(new Composite(1, key, "live"));
            transport.deliverSent();
            var original = reader.take().get(0);
            key[0] = 9;
            transport.sent.clear();
            writer.close();
            transport.deliverSent();
            var notification = reader.take().get(0);
            assertFalse(notification.hasValidData());
            assertArrayEquals(new byte[]{2, 3}, notification.data().id());
            assertEquals(original.instanceHandle(), notification.instanceHandle());
        }
    }

    private static final class FakeTransport implements RtpsTransport {
        private final List<PacketHandler> handlers = new CopyOnWriteArrayList<>();
        private final List<byte[]> sent = new CopyOnWriteArrayList<>();
        public InetAddress multicastGroup() { return InetAddress.getLoopbackAddress(); }
        public void sendMetatraffic(byte[] message) { }
        public void sendUserData(byte[] message) { sent.add(message.clone()); }
        public void send(byte[] message, InetSocketAddress address) { }
        public Closeable listenMetatraffic(PacketHandler handler) { return () -> { }; }
        public Closeable listenUserData(PacketHandler handler) { handlers.add(handler); return () -> handlers.remove(handler); }
        public Locator userUnicastLocator() { return unicastLocator(7411); }
        public Locator unicastLocator(int port) { return new Locator(multicastGroup(), port); }
        public Locator multicastLocator(int port) { return unicastLocator(port); }
        public void close() { handlers.clear(); }
        void deliverSent() { List.copyOf(sent).forEach(b -> handlers.forEach(h -> h.handle(new RtpsPacket(b, b.length)))); }
    }
}
