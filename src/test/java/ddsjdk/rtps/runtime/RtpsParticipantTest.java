package ddsjdk.rtps.runtime;

import ddsjdk.rtps.discovery.*;
import ddsjdk.rtps.message.*;
import ddsjdk.rtps.protocol.RtpsEntity;
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

class RtpsParticipantTest {
    private static final PayloadSerializer<byte[]> CODEC = new PayloadSerializer<>() {
        public byte[] serialize(byte[] value) { return value; }
        public byte[] deserialize(byte[] value) { return value; }
    };
    private static LocalEndpoint endpoint(String topic) {
        return new LocalEndpoint(topic, "bytes", EndpointQos.DEFAULT);
    }

    @Test void sharedIdentityDistinctEndpointsAndOnlyOneReceiverPerChannel() throws Exception {
        var transport = new FakeTransport();
        try (var participant = new RtpsParticipant(new RtpsParticipantConfig(0), transport)) {
            var writer1 = participant.createWriter(endpoint("one"), CODEC);
            var writer2 = participant.createWriter(endpoint("two"), CODEC);
            var reader1 = participant.createReader(endpoint("one"), CODEC);
            var reader2 = participant.createReader(endpoint("two"), CODEC);
            assertEquals(4, Set.of(writer1.guid(), writer2.guid(), reader1.guid(), reader2.guid()).size());
            for (Guid guid : List.of(writer1.guid(), writer2.guid(), reader1.guid(), reader2.guid())) {
                assertEquals(participant.guidPrefix(), guid.prefix());
            }
            assertEquals(1, transport.metaSubscriptions);
            assertEquals(1, transport.userSubscriptions);
            writer1.write(new byte[]{1});
            transport.deliverLastUserPacket();
            assertArrayEquals(new byte[]{1}, reader1.poll().orElseThrow());
            assertTrue(reader2.poll().isEmpty());
            writer2.write(new byte[]{2});
            transport.deliverLastUserPacket();
            assertArrayEquals(new byte[]{2}, reader2.poll().orElseThrow());
            assertTrue(reader1.poll().isEmpty());
        }
        assertEquals(1, transport.closeCount);
        assertEquals(2, transport.receiverCloseCount);
    }

    @Test void closingEndpointDoesNotCloseParticipantOrSiblings() throws Exception {
        var transport = new FakeTransport();
        try (var participant = new RtpsParticipant(new RtpsParticipantConfig(0), transport)) {
            var writer1 = participant.createWriter(endpoint("one"), CODEC);
            var writer2 = participant.createWriter(endpoint("one"), CODEC);
            var reader1 = participant.createReader(endpoint("one"), CODEC);
            var reader2 = participant.createReader(endpoint("one"), CODEC);
            reader1.close();
            writer1.close();
            writer1.close();
            assertEquals(0, transport.closeCount);
            assertEquals(0, transport.receiverCloseCount);
            writer2.write(new byte[]{7});
            transport.deliverLastUserPacket();
            assertArrayEquals(new byte[]{7}, reader2.poll().orElseThrow());
            assertThrows(IllegalStateException.class, reader1::poll);
            assertThrows(IOException.class, () -> writer1.write(new byte[]{1}));
            var writer3 = participant.createWriter(endpoint("three"), CODEC);
            assertNotEquals(writer1.guid(), writer3.guid());
        }
    }

    @Test void participantCloseClosesEndpointsAndRejectsNewOnes() throws Exception {
        var transport = new FakeTransport();
        var participant = new RtpsParticipant(new RtpsParticipantConfig(0), transport);
        var reader = participant.createReader(endpoint("one"), CODEC);
        var writer = participant.createWriter(endpoint("one"), CODEC);
        participant.close();
        participant.close();
        reader.close();
        writer.close();
        assertThrows(IllegalStateException.class, reader::poll);
        assertThrows(IOException.class, () -> writer.write(new byte[]{1}));
        assertThrows(IllegalStateException.class, () -> participant.createReader(endpoint("one"), CODEC));
        assertThrows(IllegalStateException.class, () -> participant.createWriter(endpoint("one"), CODEC));
        assertEquals(1, transport.closeCount);
        assertEquals(2, transport.receiverCloseCount);
    }

    @Test void failedPhysicalSubscriptionCleansUpTransportAndFirstReceiver() {
        var transport = new FakeTransport();
        transport.failUserSubscription = true;
        assertThrows(IOException.class, () -> new RtpsParticipant(new RtpsParticipantConfig(0), transport));
        assertEquals(1, transport.closeCount);
        assertEquals(1, transport.receiverCloseCount);
    }

    @Test void disposalFailureStillClosesAllEndpointsAndTransport() throws Exception {
        var transport = new FakeTransport();
        var participant = new RtpsParticipant(new RtpsParticipantConfig(0), transport);
        var reader = participant.createReader(endpoint("one"), CODEC);
        var writer = participant.createWriter(endpoint("one"), CODEC);
        transport.failMetaSend = true;
        assertThrows(IOException.class, participant::close);
        assertThrows(IllegalStateException.class, reader::poll);
        assertThrows(IOException.class, () -> writer.write(new byte[]{1}));
        assertEquals(1, transport.closeCount);
        assertEquals(2, transport.receiverCloseCount);
    }

    @Test void legacyConstructorsStillOwnAndCloseTheirPrivateParticipant() throws Exception {
        var readerTransport = new FakeTransport();
        var writerTransport = new FakeTransport();
        var reader = new RtpsDataReader<>(new RtpsParticipantConfig(0), endpoint("one"), CODEC, readerTransport);
        var writer = new RtpsDataWriter<>(new RtpsParticipantConfig(0), endpoint("one"), CODEC, writerTransport);
        assertEquals(RtpsEntity.USER_READER_NO_KEY, reader.guid().entityId());
        assertEquals(RtpsEntity.USER_WRITER_NO_KEY, writer.guid().entityId());
        reader.close();
        writer.close();
        assertEquals(1, readerTransport.closeCount);
        assertEquals(1, writerTransport.closeCount);
    }

    @Test void discoveryPublishesAllEndpointsAndKeepsDisposalInSharedSequenceSpace() throws Exception {
        var transport = new FakeTransport();
        try (var participant = new RtpsParticipant(new RtpsParticipantConfig(0), transport)) {
            var writer1 = participant.createWriter(endpoint("one"), CODEC);
            var writer2 = participant.createWriter(endpoint("two"), CODEC);
            var reader = participant.createReader(endpoint("one"), CODEC);
            participant.announce();
            var publications = transport.metaSent.stream()
                    .flatMap(b -> RtpsDiscoveryReader.readRemotePublication(b, b.length).stream()).toList();
            assertEquals(Set.of(writer1.guid(), writer2.guid()), publications.stream().map(RemotePublication::endpointGuid).collect(java.util.stream.Collectors.toSet()));
            assertTrue(transport.metaSent.stream().anyMatch(b -> RtpsDiscoveryReader.readRemoteSubscription(b, b.length)
                    .map(s -> s.endpointGuid().equals(reader.guid())).orElse(false)));
            var sequences = transport.metaSent.stream().flatMap(b -> RtpsUserDataParser.readUserSamples(b, b.length,
                    RtpsEntity.PUBLICATIONS_BUILTIN_TOPIC_READER).stream()).map(UserDataSample::sequenceNumber).toList();
            assertEquals(Set.of(1L, 2L), Set.copyOf(sequences));
            writer1.close();
            transport.metaSent.clear();
            participant.announce();
            var changes = transport.metaSent.stream().flatMap(b -> RtpsDiscoveryReader.readRemotePublicationChange(b, b.length).stream()).toList();
            assertTrue(changes.stream().anyMatch(c -> c.endpointGuid().equals(writer1.guid()) && c.disposedOrUnregistered()));
            assertFalse(changes.stream().anyMatch(c -> c.endpointGuid().equals(writer1.guid()) && c.endpoint().isPresent()));
            assertTrue(changes.stream().anyMatch(c -> c.endpointGuid().equals(writer2.guid()) && c.endpoint().isPresent()));
            transport.metaSent.clear();
            var ack = new RtpsMessageBuilder(new GuidPrefix(new byte[12]));
            ack.ackNack(RtpsEntity.PUBLICATIONS_BUILTIN_TOPIC_READER, RtpsEntity.PUBLICATIONS_BUILTIN_TOPIC_WRITER, 3, Set.of(3L), 1);
            transport.deliverMeta(ack.bytes());
            assertTrue(transport.metaSent.stream().anyMatch(b -> RtpsDiscoveryReader.readRemotePublicationChange(b, b.length)
                    .map(c -> c.endpointGuid().equals(writer1.guid()) && c.disposedOrUnregistered()).orElse(false)));
        }
    }

    @Test void retransmissionTargetsTheAllocatedWriter() throws Exception {
        var transport = new FakeTransport();
        var qos = new EndpointQos(EndpointQos.ReliabilityKind.RELIABLE, EndpointQos.DurabilityKind.VOLATILE, EndpointQos.HistoryKind.KEEP_LAST, 10);
        var endpoint = new LocalEndpoint("topic", "bytes", qos);
        try (var participant = new RtpsParticipant(new RtpsParticipantConfig(0), transport)) {
            var first = participant.createWriter(endpoint, CODEC);
            var second = participant.createWriter(endpoint, CODEC);
            first.write(new byte[]{1});
            second.write(new byte[]{2});
            transport.userSent.clear();
            var ack = new RtpsMessageBuilder(new GuidPrefix(new byte[12]));
            ack.ackNack(RtpsEntity.USER_READER_NO_KEY, second.guid().entityId(), 1, Set.of(1L), 1);
            transport.deliverUser(ack.bytes());
            var samples = transport.userSent.stream().flatMap(b -> RtpsUserDataParser.readUserSamples(b, b.length, RtpsEntity.UNKNOWN).stream()).toList();
            assertEquals(1, samples.size());
            assertEquals(second.guid(), samples.getFirst().writerGuid());
            assertArrayEquals(new byte[]{2}, samples.getFirst().payload());
        }
    }

    @Test void failedEndpointInitializationReleasesItsSubscriptionsButKeepsSharedParticipant() throws Exception {
        var transport = new FakeTransport();
        var invalidQos = new EndpointQos(null, EndpointQos.DurabilityKind.VOLATILE,
                EndpointQos.HistoryKind.KEEP_LAST, 10);
        var invalid = new LocalEndpoint("invalid", "bytes", invalidQos);
        try (var participant = new RtpsParticipant(new RtpsParticipantConfig(0), transport)) {
            assertThrows(NullPointerException.class, () -> participant.createWriter(invalid, CODEC));
            assertThrows(NullPointerException.class, () -> participant.createReader(invalid, CODEC));
            assertEquals(0, transport.closeCount);
            var writer = participant.createWriter(endpoint("good"), CODEC);
            var reader = participant.createReader(endpoint("good"), CODEC);
            writer.write(new byte[]{9});
            transport.deliverLastUserPacket();
            assertArrayEquals(new byte[]{9}, reader.poll().orElseThrow());
        }
        assertEquals(1, transport.closeCount);
    }

    @Test void failedLegacyEndpointInitializationClosesItsPrivateParticipant() {
        var invalidQos = new EndpointQos(null, EndpointQos.DurabilityKind.VOLATILE,
                EndpointQos.HistoryKind.KEEP_LAST, 10);
        var invalid = new LocalEndpoint("invalid", "bytes", invalidQos);
        var writerTransport = new FakeTransport();
        var readerTransport = new FakeTransport();
        assertThrows(NullPointerException.class, () -> new RtpsDataWriter<>(new RtpsParticipantConfig(0), invalid, CODEC, writerTransport));
        assertThrows(NullPointerException.class, () -> new RtpsDataReader<>(new RtpsParticipantConfig(0), invalid, CODEC, readerTransport));
        assertEquals(1, writerTransport.closeCount);
        assertEquals(1, readerTransport.closeCount);
        assertEquals(2, writerTransport.receiverCloseCount);
        assertEquals(2, readerTransport.receiverCloseCount);
    }

    private static final class FakeTransport implements RtpsTransport {
        private final List<PacketHandler> metaHandlers = new CopyOnWriteArrayList<>();
        private final List<PacketHandler> userHandlers = new CopyOnWriteArrayList<>();
        private final List<byte[]> metaSent = new CopyOnWriteArrayList<>();
        private final List<byte[]> userSent = new CopyOnWriteArrayList<>();
        private int metaSubscriptions, userSubscriptions, closeCount, receiverCloseCount;
        private boolean failUserSubscription, failMetaSend;
        public InetAddress multicastGroup() { return InetAddress.getLoopbackAddress(); }
        public void sendMetatraffic(byte[] message) throws IOException {
            if (failMetaSend) throw new IOException("injected send failure");
            metaSent.add(message.clone());
        }
        public void sendUserData(byte[] message) { userSent.add(message.clone()); }
        public void send(byte[] message, InetSocketAddress address) { }
        public Closeable listenMetatraffic(PacketHandler handler) {
            metaSubscriptions++;
            metaHandlers.add(handler);
            return () -> { metaHandlers.remove(handler); receiverCloseCount++; };
        }
        public Closeable listenUserData(PacketHandler handler) throws IOException {
            userSubscriptions++;
            if (failUserSubscription) throw new IOException("injected subscription failure");
            userHandlers.add(handler);
            return () -> { userHandlers.remove(handler); receiverCloseCount++; };
        }
        public Locator unicastLocator(int port) { return new Locator(multicastGroup(), port); }
        public Locator multicastLocator(int port) { return unicastLocator(port); }
        public void close() { closeCount++; }
        void deliverLastUserPacket() { deliverUser(userSent.getLast()); }
        void deliverUser(byte[] bytes) { userHandlers.forEach(h -> h.handle(new RtpsPacket(bytes, bytes.length))); }
        void deliverMeta(byte[] bytes) { metaHandlers.forEach(h -> h.handle(new RtpsPacket(bytes, bytes.length))); }
    }
}
