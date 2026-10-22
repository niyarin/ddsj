package ddsj.rtps.runtime;

import ddsj.rtps.discovery.LocalEndpoint;
import ddsj.rtps.discovery.RemoteSubscription;
import ddsj.rtps.message.RtpsMessageBuilder;
import ddsj.rtps.message.RtpsMessageParser;
import ddsj.rtps.message.RtpsUserDataParser;
import ddsj.rtps.protocol.RtpsSubmessageKind;
import ddsj.rtps.qos.EndpointQos;
import ddsj.rtps.transport.*;
import ddsj.rtps.types.*;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static ddsj.rtps.qos.EndpointQos.DurabilityKind.*;
import static ddsj.rtps.qos.EndpointQos.ReliabilityKind.*;
import static org.junit.jupiter.api.Assertions.*;

class DurabilityRuntimeTest {
    private static final PayloadSerializer<byte[]> CODEC = new PayloadSerializer<>() {
        public byte[] serialize(byte[] value) { return value; }
        public byte[] deserialize(byte[] payload) { return payload; }
    };

    private static LocalEndpoint endpoint(EndpointQos.DurabilityKind durability,
            EndpointQos.ReliabilityKind reliability) {
        return new LocalEndpoint("topic", "bytes", EndpointQos.builder()
                .durability(durability).reliability(reliability).keepLast(2).build());
    }

    @Test void lateReadersReceiveHistoryOnlyWhenBothSidesRequestTransientLocal() throws Exception {
        for (var reliability : EndpointQos.ReliabilityKind.values()) {
            for (var durability : EndpointQos.DurabilityKind.values()) {
                var transport = new FakeTransport();
                try (var participant = new RtpsParticipant(new RtpsParticipantConfig(0), transport)) {
                    var writer = participant.createWriter(endpoint(durability, reliability), CODEC);
                    writer.write(new byte[]{1});
                    writer.write(new byte[]{2});
                    writer.write(new byte[]{3});
                    var volatileReader = participant.createReader(endpoint(VOLATILE, reliability), CODEC);
                    var durableReader = participant.createReader(endpoint(TRANSIENT_LOCAL, reliability), CODEC);
                    transport.sent.clear();
                    writer.announceHeartbeat();
                    transport.deliverSent();
                    assertTrue(volatileReader.drain().isEmpty());
                    var history = durableReader.drain();
                    assertEquals(durability == TRANSIENT_LOCAL ? 2 : 0, history.size());
                    if (!history.isEmpty()) {
                        assertArrayEquals(new byte[]{2}, history.get(0));
                        assertArrayEquals(new byte[]{3}, history.get(1));
                    }
                    transport.sent.clear();
                    writer.announceHeartbeat();
                    assertTrue(transport.sent.stream().flatMap(b -> RtpsUserDataParser
                            .readUserSamples(b, b.length, durableReader.guid().entityId()).stream()).toList().isEmpty(),
                            "discovery refresh must not replay the history again");
                    transport.sent.clear();
                    writer.write(new byte[]{4});
                    transport.deliverSent();
                    assertArrayEquals(new byte[]{4}, volatileReader.poll().orElseThrow());
                    if (durability == TRANSIENT_LOCAL) {
                        assertArrayEquals(new byte[]{4}, durableReader.poll().orElseThrow());
                    } else {
                        assertTrue(durableReader.poll().isEmpty(), "incompatible durability must not match");
                    }
                }
            }
        }
    }

    @Test void volatileRepairsLiveSamplesButGapsSamplesWrittenBeforeMatching() throws Exception {
        var transport = new FakeTransport();
        try (var participant = new RtpsParticipant(new RtpsParticipantConfig(0), transport)) {
            var writer = participant.createWriter(endpoint(VOLATILE, RELIABLE), CODEC);
            writer.write(new byte[]{1});
            var reader = participant.createReader(endpoint(VOLATILE, RELIABLE), CODEC);
            writer.write(new byte[]{2}); // dropped; still eligible for reliable repair
            transport.sent.clear();
            writer.announceHeartbeat();
            var heartbeats = transport.sent.stream().flatMap(b -> RtpsUserDataParser
                    .readHeartbeats(b, b.length, reader.guid().entityId()).stream()).toList();
            assertFalse(heartbeats.isEmpty());
            assertEquals(2, heartbeats.get(0).firstSequenceNumber());
            transport.sent.clear();
            var request = new RtpsMessageBuilder(reader.guid().prefix());
            request.ackNack(reader.guid().entityId(), writer.guid().entityId(), 1, Set.of(1L, 2L), 1);
            transport.deliver(request.bytes());
            var samples = transport.sent.stream().flatMap(b -> RtpsUserDataParser
                    .readUserSamples(b, b.length, reader.guid().entityId()).stream()).toList();
            assertEquals(1, samples.size());
            assertEquals(2, samples.get(0).sequenceNumber());
            assertTrue(transport.hasKind(RtpsSubmessageKind.GAP));
        }
    }

    @Test void historicalReplayIsIsolatedByParticipantPrefixAndReaderId() throws Exception {
        var transport = new FakeTransport();
        try (var participant = new RtpsParticipant(new RtpsParticipantConfig(0), transport)) {
            var writer = participant.createWriter(endpoint(TRANSIENT_LOCAL, RELIABLE), CODEC);
            writer.write(new byte[]{1});
            // Independent participants allocate identical reader entity IDs.
            var remoteTransport = new FakeTransport();
            try (var remote = new RtpsParticipant(new RtpsParticipantConfig(0), remoteTransport)) {
                var durable = participant.createReader(endpoint(TRANSIENT_LOCAL, RELIABLE), CODEC);
                var volatileReader = remote.createReader(endpoint(VOLATILE, RELIABLE), CODEC);
                assertEquals(durable.guid().entityId(), volatileReader.guid().entityId());
                participant.subscriptions().upsert(new RemoteSubscription(volatileReader.guid(),
                        "topic", "bytes", endpoint(VOLATILE, RELIABLE).qos()));
                transport.sent.clear();
                writer.announceHeartbeat();
                for (byte[] packet : List.copyOf(transport.sent)) {
                    transport.deliver(packet);
                    remoteTransport.deliver(packet);
                }
                assertArrayEquals(new byte[]{1}, durable.poll().orElseThrow());
                assertTrue(volatileReader.poll().isEmpty());
            }
        }
    }

    @Test void fragmentRequestsCannotBypassTheVolatileMatchBoundary() throws Exception {
        var transport = new FakeTransport();
        try (var participant = new RtpsParticipant(new RtpsParticipantConfig(0), transport)) {
            var writer = participant.createWriter(endpoint(TRANSIENT_LOCAL, RELIABLE), CODEC);
            writer.write(new byte[65000]);
            var reader = participant.createReader(endpoint(VOLATILE, RELIABLE), CODEC);
            writer.write(new byte[65000]);
            transport.sent.clear();
            var request = new RtpsMessageBuilder(reader.guid().prefix());
            request.nackFrag(reader.guid().entityId(), writer.guid().entityId(), 1, Set.of(1), 1);
            transport.deliver(request.bytes());
            assertTrue(transport.hasKind(RtpsSubmessageKind.GAP));
            assertFalse(transport.hasKind(RtpsSubmessageKind.DATA_FRAG));
            transport.sent.clear();
            request = new RtpsMessageBuilder(reader.guid().prefix());
            request.nackFrag(reader.guid().entityId(), writer.guid().entityId(), 2, Set.of(1), 2);
            transport.deliver(request.bytes());
            assertTrue(transport.hasKind(RtpsSubmessageKind.DATA_FRAG));
        }
    }

    @Test void bestEffortTransientLocalReplaysFragmentedHistory() throws Exception {
        var transport = new FakeTransport();
        try (var participant = new RtpsParticipant(new RtpsParticipantConfig(0), transport)) {
            var writer = participant.createWriter(endpoint(TRANSIENT_LOCAL, BEST_EFFORT), CODEC);
            byte[] payload = new byte[65000];
            payload[64999] = 42;
            writer.write(payload);
            var reader = participant.createReader(endpoint(TRANSIENT_LOCAL, BEST_EFFORT), CODEC);
            transport.sent.clear();
            writer.announceHeartbeat();
            transport.deliverSent();
            assertArrayEquals(payload, reader.poll().orElseThrow());
        }
    }

    @Test void unmatchedRequestsCannotReadRetainedHistory() throws Exception {
        var transport = new FakeTransport();
        try (var participant = new RtpsParticipant(new RtpsParticipantConfig(0), transport)) {
            var writer = participant.createWriter(endpoint(TRANSIENT_LOCAL, RELIABLE), CODEC);
            writer.write(new byte[]{1});
            transport.sent.clear();
            var request = new RtpsMessageBuilder(new GuidPrefix(new byte[12]));
            request.ackNack(new EntityId(new byte[]{0, 0, 2, 4}), writer.guid().entityId(), 1, Set.of(1L), 1);
            transport.deliver(request.bytes());
            assertTrue(transport.sent.isEmpty());
        }
    }

    @Test void historyIsReplayedBeforeTheFirstLiveSampleForANewMatch() throws Exception {
        var transport = new FakeTransport();
        try (var participant = new RtpsParticipant(new RtpsParticipantConfig(0), transport)) {
            var writer = participant.createWriter(endpoint(TRANSIENT_LOCAL, BEST_EFFORT), CODEC);
            writer.write(new byte[]{1});
            writer.write(new byte[]{2});
            var reader = participant.createReader(endpoint(TRANSIENT_LOCAL, BEST_EFFORT), CODEC);
            transport.sent.clear();
            writer.write(new byte[]{3});
            transport.deliverSent();
            var samples = reader.drain();
            assertEquals(2, samples.size());
            assertArrayEquals(new byte[]{2}, samples.get(0));
            assertArrayEquals(new byte[]{3}, samples.get(1));
        }
    }

    @Test void repairForAnEarlierReaderDoesNotLeakToALateVolatileReader() throws Exception {
        var transport = new FakeTransport();
        try (var participant = new RtpsParticipant(new RtpsParticipantConfig(0), transport)) {
            var writer = participant.createWriter(endpoint(VOLATILE, RELIABLE), CODEC);
            var early = participant.createReader(endpoint(VOLATILE, RELIABLE), CODEC);
            writer.write(new byte[]{1});
            var late = participant.createReader(endpoint(VOLATILE, RELIABLE), CODEC);
            transport.sent.clear();
            writer.announceHeartbeat();
            transport.sent.clear();
            var request = new RtpsMessageBuilder(early.guid().prefix());
            request.infoDst(writer.guid().prefix());
            request.ackNack(early.guid().entityId(), writer.guid().entityId(), 1, Set.of(1L), 1);
            transport.deliver(request.bytes());
            transport.deliverSent();
            assertArrayEquals(new byte[]{1}, early.poll().orElseThrow());
            assertTrue(late.poll().isEmpty());
        }
    }

    @Test void directedRepairRequestsDoNotReachAnotherWriterWithTheSameEntityId() throws Exception {
        var transport = new FakeTransport();
        var otherTransport = new FakeTransport();
        try (var participant = new RtpsParticipant(new RtpsParticipantConfig(0), transport);
                var other = new RtpsParticipant(new RtpsParticipantConfig(0), otherTransport)) {
            var writer = participant.createWriter(endpoint(TRANSIENT_LOCAL, RELIABLE), CODEC);
            var otherWriter = other.createWriter(endpoint(TRANSIENT_LOCAL, RELIABLE), CODEC);
            assertEquals(writer.guid().entityId(), otherWriter.guid().entityId());
            Guid reader = new GuidPrefix(new byte[12]).toGuid(new EntityId(new byte[]{0, 0, 2, 4}));
            var subscription = new RemoteSubscription(reader, "topic", "bytes", endpoint(TRANSIENT_LOCAL, RELIABLE).qos());
            participant.subscriptions().upsert(subscription);
            other.subscriptions().upsert(subscription);
            writer.write(new byte[65000]);
            otherWriter.write(new byte[65000]);
            transport.sent.clear();
            otherTransport.sent.clear();
            var request = new RtpsMessageBuilder(reader.prefix());
            request.infoDst(writer.guid().prefix());
            request.ackNack(reader.entityId(), writer.guid().entityId(), 1, Set.of(1L), 1);
            request.nackFrag(reader.entityId(), writer.guid().entityId(), 1, Set.of(1), 1);
            transport.deliver(request.bytes());
            otherTransport.deliver(request.bytes());
            assertTrue(transport.hasKind(RtpsSubmessageKind.DATA_FRAG));
            assertTrue(otherTransport.sent.isEmpty());
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
        public Closeable listenUserData(PacketHandler handler) {
            handlers.add(handler);
            return () -> handlers.remove(handler);
        }
        public Locator userUnicastLocator() { return unicastLocator(7411); }
        public Locator unicastLocator(int port) { return new Locator(multicastGroup(), port); }
        public Locator multicastLocator(int port) { return unicastLocator(port); }
        public void close() { handlers.clear(); }
        void deliver(byte[] bytes) {
            handlers.forEach(h -> h.handle(new RtpsPacket(bytes, bytes.length)));
        }
        void deliverSent() { List.copyOf(sent).forEach(this::deliver); }
        boolean hasKind(int kind) {
            return sent.stream().flatMap(b -> new RtpsMessageParser(b, b.length).submessages().stream())
                    .anyMatch(s -> s.kind() == kind);
        }
    }
}
