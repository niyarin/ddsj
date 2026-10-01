package ddsj.rtps.runtime;

import ddsj.rtps.discovery.*;
import ddsj.rtps.qos.EndpointQos;
import ddsj.rtps.message.*;
import ddsj.rtps.protocol.RtpsEntity;
import ddsj.rtps.transport.*;
import ddsj.rtps.types.*;
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

    @Test void requestIdentityMatchesReceivedRequestAndRelatedResponse() throws Exception {
        var transport = new FakeTransport();
        try (var participant = new RtpsParticipant(new RtpsParticipantConfig(0), transport)) {
            var requests = participant.createWriter(endpoint("requests"), CODEC);
            var requestReader = participant.createReader(endpoint("requests"), CODEC);
            var responses = participant.createWriter(endpoint("responses"), CODEC);
            var responseReader = participant.createReader(endpoint("responses"), CODEC);
            requests.write(new byte[]{0});
            var identity = requests.writeRequest(new byte[]{1});
            assertEquals(new SampleIdentity(requests.guid(), 2), identity);
            transport.deliverLastUserPacket();
            var request = requestReader.pollWithMetadata().orElseThrow();
            assertEquals(identity, new SampleIdentity(request.writerGuid(), request.sequenceNumber()));
            assertTrue(request.relatedSampleIdentity().isEmpty());
            responses.write(new byte[]{2}, new SampleIdentity(request.writerGuid(), request.sequenceNumber()));
            transport.deliverLastUserPacket();
            var response = responseReader.pollWithMetadata().orElseThrow();
            assertEquals(identity, response.relatedSampleIdentity().orElseThrow());
            assertNotEquals(identity.writerGuid(), response.writerGuid());
            assertEquals(3, requests.writeRequest(new byte[]{3}).sequenceNumber());
        }
    }

    @Test void fragmentedRequestUsesReturnedIdentityForEveryFragment() throws Exception {
        var transport = new FakeTransport();
        try (var participant = new RtpsParticipant(new RtpsParticipantConfig(0), transport)) {
            var writer = participant.createWriter(endpoint("requests"), CODEC);
            var identity = writer.writeRequest(new byte[100_000]);
            var fragments = transport.userSent.stream().flatMap(packet ->
                    RtpsUserDataParser.readDataFragments(packet, packet.length, RtpsEntity.UNKNOWN).stream()).toList();
            assertTrue(fragments.size() > 1);
            for (var fragment : fragments) {
                assertEquals(identity, new SampleIdentity(fragment.writerGuid(), fragment.sequenceNumber()));
            }
        }
    }

    @Test void concurrentRequestsReturnTheirOwnPacketIdentity() throws Exception {
        var transport = new FakeTransport();
        try (var participant = new RtpsParticipant(new RtpsParticipantConfig(0), transport)) {
            var writer = participant.createWriter(endpoint("requests"), CODEC);
            var executor = java.util.concurrent.Executors.newFixedThreadPool(4);
            try {
                var results = new java.util.ArrayList<java.util.concurrent.Future<SampleIdentity>>();
                for (int i = 0; i < 32; i++) {
                    byte value = (byte) i;
                    results.add(executor.submit(() -> writer.writeRequest(new byte[]{value})));
                }
                var identities = new java.util.HashSet<SampleIdentity>();
                for (int i = 0; i < results.size(); i++) {
                    var identity = results.get(i).get(5, java.util.concurrent.TimeUnit.SECONDS);
                    assertTrue(identities.add(identity));
                    final byte value = (byte) i;
                    var sample = transport.userSent.stream().flatMap(packet ->
                            RtpsUserDataParser.readUserSamples(packet, packet.length, RtpsEntity.UNKNOWN).stream())
                            .filter(item -> item.payload()[0] == value).findFirst().orElseThrow();
                    assertEquals(identity, new SampleIdentity(sample.writerGuid(), sample.sequenceNumber()));
                }
            } finally {
                executor.shutdownNow();
            }
        }
    }

    @Test void failedRequestThrowsAndDoesNotReuseAnAttemptedSequence() throws Exception {
        var transport = new FakeTransport();
        try (var participant = new RtpsParticipant(new RtpsParticipantConfig(0), transport)) {
            var writer = participant.createWriter(endpoint("requests"), CODEC);
            transport.failUserSend = true;
            assertThrows(IOException.class, () -> writer.writeRequest(new byte[]{1}));
            transport.failUserSend = false;
            assertEquals(new SampleIdentity(writer.guid(), 2), writer.writeRequest(new byte[]{2}));
            writer.close();
            assertThrows(IOException.class, () -> writer.writeRequest(new byte[]{3}));
        }
    }

    @Test void sharedTransportPreservesUserUnicastLocator() throws Exception {
        var delegate = new FakeTransport();
        try (var transport = new ParticipantTransport(delegate)) {
            assertEquals(delegate.userUnicastLocator(), transport.userUnicastLocator());
        }
    }

    @Test void namedLivelinessCallbackWorksWithoutDeadlinePlaceholder() throws Exception {
        var transport = new FakeTransport();
        var events = new CopyOnWriteArrayList<LivelinessChangedStatus>();
        var qos = new EndpointQos(EndpointQos.ReliabilityKind.BEST_EFFORT,
                EndpointQos.DurabilityKind.VOLATILE, EndpointQos.HistoryKind.KEEP_LAST, 10,
                EndpointQos.LivelinessKind.MANUAL_BY_TOPIC, java.time.Duration.ofSeconds(30));
        var endpoint = new LocalEndpoint("one", "bytes", qos);
        try (var participant = new RtpsParticipant(new RtpsParticipantConfig(0), transport)) {
            var writer = participant.createWriter(endpoint, CODEC);
            var reader = participant.createReader(endpoint, CODEC,
                    ReaderListeners.builder().onLivelinessChanged(events::add).build());
            writer.write(new byte[]{1});
            transport.deliverLastUserPacket();
            assertEquals(1, events.size());
            assertEquals(writer.guid(), events.get(0).writerGuid());
            assertTrue(events.get(0).alive());
            assertEquals(1, reader.livelinessAliveCount());
        }
    }

    @Test void nullListenersDoNotDamageParticipant() throws Exception {
        try (var participant = new RtpsParticipant(new RtpsParticipantConfig(0), new FakeTransport())) {
            assertThrows(NullPointerException.class,
                    () -> participant.createReader(endpoint("one"), CODEC, (ReaderListeners) null));
            try (var reader = participant.createReader(endpoint("one"), CODEC, ReaderListeners.DEFAULT)) {
                assertNotNull(reader.guid());
            }
        }
        assertThrows(NullPointerException.class, () -> ReaderListeners.builder().onDeadlineMissed(null));
        assertThrows(NullPointerException.class, () -> ReaderListeners.builder().onLivelinessChanged(null));
        assertThrows(NullPointerException.class, () -> ReaderListeners.builder().onDeserializationError(null));
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
            assertEquals(second.guid(), samples.get(0).writerGuid());
            assertArrayEquals(new byte[]{2}, samples.get(0).payload());
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
        boolean failUserSend;
        public void sendUserData(byte[] message) throws IOException {
            if (failUserSend) throw new IOException("injected user send failure");
            userSent.add(message.clone());
        }
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
        public Locator userUnicastLocator() { return unicastLocator(7411); }
        public Locator unicastLocator(int port) { return new Locator(multicastGroup(), port); }
        public Locator multicastLocator(int port) { return unicastLocator(port); }
        public void close() { closeCount++; }
        void deliverLastUserPacket() { deliverUser(userSent.get(userSent.size() - 1)); }
        void deliverUser(byte[] bytes) { userHandlers.forEach(h -> h.handle(new RtpsPacket(bytes, bytes.length))); }
        void deliverMeta(byte[] bytes) { metaHandlers.forEach(h -> h.handle(new RtpsPacket(bytes, bytes.length))); }
    }
}
