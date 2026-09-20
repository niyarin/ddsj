package ddsjdk.rtps.runtime;

import ddsjdk.rtps.discovery.EndpointQos;
import ddsjdk.rtps.discovery.LocalEndpoint;
import ddsjdk.rtps.history.ResourceLimits;
import ddsjdk.rtps.message.RtpsMessageBuilder;
import ddsjdk.rtps.message.RtpsMessageParser;
import ddsjdk.rtps.message.RtpsUserDataParser;
import ddsjdk.rtps.protocol.RtpsEntity;
import ddsjdk.rtps.protocol.RtpsSubmessageKind;
import ddsjdk.rtps.transport.*;
import ddsjdk.rtps.types.*;
import org.junit.jupiter.api.Test;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import static org.junit.jupiter.api.Assertions.*;
import static ddsjdk.rtps.discovery.EndpointQos.HistoryKind.*;

class HistoryRuntimeTest {
    private static final GuidPrefix PREFIX = new GuidPrefix(new byte[12]);
    private static final PayloadSerializer<byte[]> CODEC = new PayloadSerializer<>() {
        public byte[] serialize(byte[] value) { return value; }
        public byte[] deserialize(byte[] payload) { return payload; }
    };
    private LocalEndpoint endpoint(EndpointQos.HistoryKind kind, int depth, int limit) {
        return new LocalEndpoint("topic", "bytes", new EndpointQos(
                EndpointQos.ReliabilityKind.RELIABLE, EndpointQos.DurabilityKind.VOLATILE,
                kind, depth), new ResourceLimits(limit));
    }
    @Test void readerKeepsLatestDepthSamples() throws Exception {
        var transport = new FakeTransport();
        try (var reader = new RtpsDataReader<>(new RtpsParticipantConfig(0), endpoint(KEEP_LAST, 2, 3), CODEC, transport)) {
            transport.deliverData(1); transport.deliverData(2); transport.deliverData(3);
            var samples = reader.drain();
            assertEquals(2, samples.size());
            assertArrayEquals(new byte[]{2}, samples.get(0));
            assertArrayEquals(new byte[]{3}, samples.get(1));
            assertEquals(0, reader.sampleRejectedCount());
        }
    }
    @Test void readerRetriesRejectedSampleAfterTakeWithoutMarkingItReceived() throws Exception {
        var transport = new FakeTransport();
        try (var reader = new RtpsDataReader<>(new RtpsParticipantConfig(0), endpoint(KEEP_ALL, 1, 2), CODEC, transport)) {
            transport.deliverData(1); transport.deliverData(2); transport.deliverData(3);
            assertEquals(1, reader.sampleRejectedCount());
            var retained = reader.drain();
            assertEquals(2, retained.size());
            assertArrayEquals(new byte[]{1}, retained.get(0));
            assertArrayEquals(new byte[]{2}, retained.get(1));
            transport.deliverData(3); transport.deliverData(3);
            var retried = reader.drain();
            assertEquals(1, retried.size());
            assertArrayEquals(new byte[]{3}, retried.get(0));
        }
    }
    @Test void writerKeepAllRejectsWithoutSendingOrLosingOldPayload() throws Exception {
        var transport = new FakeTransport();
        try (var writer = new RtpsDataWriter<>(new RtpsParticipantConfig(0), endpoint(KEEP_ALL, 1, 2), CODEC, transport)) {
            writer.write(new byte[]{1}); writer.write(new byte[]{2});
            assertThrows(IOException.class, () -> writer.write(new byte[]{3}));
            var data = transport.sent.stream().flatMap(bytes -> RtpsUserDataParser.readUserSamples(bytes, bytes.length, RtpsEntity.USER_READER_NO_KEY).stream()).toList();
            assertEquals(2, data.size());
            var ack = new RtpsMessageBuilder(PREFIX);
            ack.ackNack(RtpsEntity.USER_READER_NO_KEY, RtpsEntity.USER_WRITER_NO_KEY, 1, Set.of(1L), 1);
            transport.deliver(ack.bytes());
            var resent = transport.sent.stream().flatMap(bytes -> RtpsUserDataParser.readUserSamples(bytes, bytes.length, RtpsEntity.USER_READER_NO_KEY).stream()).toList();
            assertEquals(3, resent.size());
            assertArrayEquals(new byte[]{1}, resent.get(2).payload());
        }
    }
    @Test void evictedFragmentCannotBeResentAndProducesGap() throws Exception {
        var transport = new FakeTransport();
        try (var writer = new RtpsDataWriter<>(new RtpsParticipantConfig(0), endpoint(KEEP_LAST, 1, 2), CODEC, transport)) {
            writer.write(new byte[65000]);
            writer.write(new byte[]{2});
            transport.sent.clear();
            var nack = new RtpsMessageBuilder(PREFIX);
            nack.nackFrag(RtpsEntity.USER_READER_NO_KEY, RtpsEntity.USER_WRITER_NO_KEY, 1, Set.of(1), 1);
            transport.deliver(nack.bytes());
            assertTrue(transport.sent.stream().flatMap(bytes -> new RtpsMessageParser(bytes, bytes.length).submessages().stream()).anyMatch(s -> s.kind() == RtpsSubmessageKind.GAP));
            assertFalse(transport.sent.stream().flatMap(bytes -> new RtpsMessageParser(bytes, bytes.length).submessages().stream()).anyMatch(s -> s.kind() == RtpsSubmessageKind.DATA_FRAG));
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
