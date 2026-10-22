package ddsj.rtps.runtime;

import ddsj.dds.core.CdrKey;
import ddsj.dds.core.TypeSupport;
import ddsj.rtps.discovery.LocalEndpoint;
import ddsj.rtps.history.ResourceLimits;
import ddsj.rtps.message.*;
import ddsj.rtps.protocol.RtpsEntity;
import ddsj.rtps.protocol.RtpsSubmessageKind;
import ddsj.rtps.qos.EndpointQos;
import ddsj.rtps.transport.*;
import ddsj.rtps.types.*;
import org.junit.jupiter.api.Test;
import java.io.Closeable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import static ddsj.rtps.qos.EndpointQos.DurabilityKind.*;
import static ddsj.rtps.qos.EndpointQos.ReliabilityKind.*;
import static org.junit.jupiter.api.Assertions.*;

class KeyedDurabilityRuntimeTest {
    record Sensor(@CdrKey int id, int value) { }
    record Blob(@CdrKey int id, byte[] value) { }
    private static final TypeSupport<Sensor> CODEC = TypeSupport.forCdrRecord(Sensor.class);
    private static LocalEndpoint endpoint(EndpointQos.DurabilityKind durability, EndpointQos.ReliabilityKind reliability) {
        return new LocalEndpoint("sensors", CODEC.getTypeName(), EndpointQos.builder()
                .durability(durability).reliability(reliability).keepLast(1).build());
    }

    @Test void transientLocalReplaysLatestSampleForEveryKeyIncludingSequenceGaps() throws Exception {
        for (var reliability : EndpointQos.ReliabilityKind.values()) {
            var transport = new FakeTransport();
            try (var participant = new RtpsParticipant(new RtpsParticipantConfig(0), transport)) {
                var writer = participant.createWriter(endpoint(TRANSIENT_LOCAL, reliability), CODEC);
                writer.write(new Sensor(1, 10));
                writer.write(new Sensor(2, 20));
                writer.write(new Sensor(1, 11));
                writer.write(new Sensor(3, 30));
                var reader = participant.createReader(endpoint(TRANSIENT_LOCAL, reliability), CODEC);
                var volatileReader = participant.createReader(endpoint(VOLATILE, reliability), CODEC);
                assertEquals(2, writer.guid().entityId().bytes()[3]);
                assertEquals(7, reader.guid().entityId().bytes()[3]);
                transport.sent.clear();
                writer.announceHeartbeat();
                transport.deliverSent();
                assertEquals(Set.of(new Sensor(1, 11), new Sensor(2, 20), new Sensor(3, 30)), new HashSet<>(reader.drain()));
                assertTrue(volatileReader.drain().isEmpty());
                transport.sent.clear();
                writer.write(new Sensor(1, 12));
                transport.deliverSent();
                assertEquals(new Sensor(1, 12), volatileReader.poll().orElseThrow());
            }
        }
    }

    @Test void readerKeepsLatestPerKeyAndDoesNotReplaceItWithAnOlderRepair() throws Exception {
        var transport = new FakeTransport();
        try (var participant = new RtpsParticipant(new RtpsParticipantConfig(0), transport)) {
            var reader = participant.createReader(endpoint(VOLATILE, RELIABLE), CODEC);
            GuidPrefix remote = new GuidPrefix(new byte[12]);
            EntityId writerId = new EntityId(new byte[]{0, 0, 2, 2});
            for (var item : List.of(new Sensor(1, 11), new Sensor(2, 20), new Sensor(1, 10))) {
                long sequence = item.value() == 11 ? 3 : item.value() == 20 ? 2 : 1;
                var packet = new RtpsMessageBuilder(remote);
                packet.data(reader.guid().entityId(), writerId, sequence, CODEC.serialize(item));
                transport.deliver(packet.bytes());
            }
            assertEquals(Set.of(new Sensor(1, 11), new Sensor(2, 20)), new HashSet<>(reader.drain()));
        }
    }

    @Test void lifecycleChangesAreRetainedAndReplayedPerKey() throws Exception {
        var transport = new FakeTransport();
        try (var participant = new RtpsParticipant(new RtpsParticipantConfig(0), transport)) {
            var writer = participant.createWriter(endpoint(TRANSIENT_LOCAL, RELIABLE), CODEC);
            writer.write(new Sensor(1, 10));
            writer.write(new Sensor(2, 20));
            writer.dispose(new Sensor(1, 99));
            writer.unregisterInstance(new Sensor(2, 99));
            var reader = participant.createReader(endpoint(TRANSIENT_LOCAL, RELIABLE), CODEC);
            transport.sent.clear();
            writer.announceHeartbeat();
            transport.deliverSent();
            var samples = reader.drainWithMetadata();
            assertEquals(2, samples.size());
            assertTrue(samples.stream().noneMatch(ReceivedSample::validData));
            assertEquals(1, samples.get(0).data().id());
            assertEquals(0, samples.get(0).data().value(), "key-only payload excludes the value");
            assertEquals(1, samples.get(0).statusInfo());
            assertEquals(2, samples.get(1).statusInfo());
            assertEquals(1, samples.get(0).instanceSnapshot().orElseThrow().state());
            assertEquals(2, samples.get(1).instanceSnapshot().orElseThrow().state());
        }
    }

    @Test void unregisteringOneWriterDoesNotMarkAnInstanceWithoutWriters() throws Exception {
        var transport = new FakeTransport();
        try (var participant = new RtpsParticipant(new RtpsParticipantConfig(0), transport)) {
            var first = participant.createWriter(endpoint(VOLATILE, RELIABLE), CODEC);
            var second = participant.createWriter(endpoint(VOLATILE, RELIABLE), CODEC);
            var reader = participant.createReader(endpoint(VOLATILE, RELIABLE), CODEC);
            first.write(new Sensor(1, 10));
            second.write(new Sensor(1, 20));
            first.unregisterInstance(new Sensor(1, 0));
            transport.deliverSent();
            var sample = reader.pollWithMetadata().orElseThrow();
            assertFalse(sample.validData());
            assertEquals(0, sample.instanceSnapshot().orElseThrow().state());
            assertEquals(Set.of(second.guid()), sample.instanceSnapshot().orElseThrow().writers());
        }
    }

    @Test void writerResourceRejectionPreservesOtherKeysAndSequenceNumbers() throws Exception {
        var transport = new FakeTransport();
        var local = new LocalEndpoint("sensors", CODEC.getTypeName(), endpoint(TRANSIENT_LOCAL, RELIABLE).qos(),
                new ResourceLimits(2, 2, 1));
        try (var participant = new RtpsParticipant(new RtpsParticipantConfig(0), transport)) {
            var writer = participant.createWriter(local, CODEC);
            writer.write(new Sensor(1, 10));
            writer.write(new Sensor(2, 20));
            assertThrows(ResourceLimitException.class, () -> writer.write(new Sensor(3, 30)));
            var reader = participant.createReader(local, CODEC);
            transport.sent.clear();
            writer.write(new Sensor(1, 11));
            transport.deliverSent();
            assertEquals(Set.of(new Sensor(1, 11), new Sensor(2, 20)), new HashSet<>(reader.drain()));
            var samples = transport.sent.stream().flatMap(b -> RtpsUserDataParser
                    .readUserSamples(b, b.length, reader.guid().entityId()).stream()).toList();
            assertTrue(samples.stream().anyMatch(s -> s.sequenceNumber() == 3));
            assertTrue(samples.stream().noneMatch(s -> s.sequenceNumber() == 4));
        }
    }

    @Test void fragmentedHistoryRetainsDifferentKeys() throws Exception {
        var codec = TypeSupport.forCdrRecord(Blob.class);
        var local = new LocalEndpoint("blobs", codec.getTypeName(), endpoint(TRANSIENT_LOCAL, BEST_EFFORT).qos());
        var transport = new FakeTransport();
        try (var participant = new RtpsParticipant(new RtpsParticipantConfig(0), transport)) {
            var writer = participant.createWriter(local, codec);
            writer.write(new Blob(1, new byte[65000]));
            writer.write(new Blob(2, new byte[65001]));
            var reader = participant.createReader(local, codec);
            transport.sent.clear();
            writer.announceHeartbeat();
            transport.deliverSent();
            var samples = reader.drain();
            assertEquals(2, samples.size());
            assertEquals(Set.of(1, 2), new HashSet<>(samples.stream().map(Blob::id).toList()));
            assertTrue(transport.hasKind(RtpsSubmessageKind.DATA_FRAG));
        }
    }

    @Test void reliableRepairAdvancesPastEvictedSequencesInSparseKeyedHistory() throws Exception {
        var transport = new FakeTransport();
        try (var participant = new RtpsParticipant(new RtpsParticipantConfig(0), transport)) {
            var writer = participant.createWriter(endpoint(TRANSIENT_LOCAL, RELIABLE), CODEC);
            writer.write(new Sensor(1, 10));
            for (int value = 1; value <= 600; value++) writer.write(new Sensor(2, value));
            var reader = participant.createReader(endpoint(TRANSIENT_LOCAL, RELIABLE), CODEC);
            transport.sent.clear();
            writer.announceHeartbeat();
            var initial = List.copyOf(transport.sent);
            transport.sent.clear();
            for (byte[] packet : initial) {
                // Drop the newer key's initial replay; it must be recovered through ACKNACK.
                boolean dropped = RtpsUserDataParser.readUserSamples(packet, packet.length, reader.guid().entityId())
                        .stream().anyMatch(sample -> sample.sequenceNumber() == 601);
                if (!dropped) transport.deliver(packet);
            }
            assertEquals(new Sensor(1, 10), reader.poll().orElseThrow());
            for (int round = 0; round < 5; round++) {
                while (!transport.sent.isEmpty()) {
                    var batch = List.copyOf(transport.sent);
                    transport.sent.clear();
                    batch.forEach(transport::deliver);
                }
                var repaired = reader.poll();
                if (repaired.isPresent()) {
                    assertEquals(new Sensor(2, 600), repaired.get());
                    return;
                }
                writer.announceHeartbeat();
            }
            fail("GAP processing must allow repair of the retained sample beyond the first 256 sequences");
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
        void deliver(byte[] bytes) { handlers.forEach(h -> h.handle(new RtpsPacket(bytes, bytes.length))); }
        void deliverSent() { List.copyOf(sent).forEach(this::deliver); }
        boolean hasKind(int kind) {
            return sent.stream().flatMap(b -> new RtpsMessageParser(b, b.length).submessages().stream())
                    .anyMatch(s -> s.kind() == kind);
        }
    }
}
