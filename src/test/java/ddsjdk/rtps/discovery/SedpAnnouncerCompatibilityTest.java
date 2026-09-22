package ddsjdk.rtps.discovery;

import ddsjdk.rtps.qos.EndpointQos;
import ddsjdk.rtps.message.*;
import ddsjdk.rtps.protocol.RtpsEntity;
import ddsjdk.rtps.protocol.RtpsSubmessageKind;
import ddsjdk.rtps.transport.RtpsTransport;
import ddsjdk.rtps.types.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class SedpAnnouncerCompatibilityTest {
    private final GuidPrefix prefix = new GuidPrefix(new byte[12]);
    private final LocalEndpoint endpoint = new LocalEndpoint("topic", "type", EndpointQos.DEFAULT);

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void legacyAnnouncementResendAndDisposalRemainCompatible(boolean publication) throws Exception {
        var transport = new RecordingTransport();
        var locator = new Locator(InetAddress.getLoopbackAddress(), 7410);
        var participants = List.of(new RemoteParticipant(prefix, Set.of(locator), Set.of()));
        EntityId reader = publication ? RtpsEntity.PUBLICATIONS_BUILTIN_TOPIC_READER : RtpsEntity.SUBSCRIPTIONS_BUILTIN_TOPIC_READER;
        EntityId writer = publication ? RtpsEntity.PUBLICATIONS_BUILTIN_TOPIC_WRITER : RtpsEntity.SUBSCRIPTIONS_BUILTIN_TOPIC_WRITER;
        Guid endpointGuid = prefix.toGuid(publication ? RtpsEntity.USER_WRITER_NO_KEY : RtpsEntity.USER_READER_NO_KEY);
        Action announce;
        Action dispose;
        AckAction respond;
        if (publication) {
            var announcer = new SedpPublicationAnnouncer(endpoint, transport, prefix, participants);
            announce = announcer::announce;
            dispose = announcer::disposeAndUnregister;
            respond = announcer::respondTo;
        } else {
            var announcer = new SedpSubscriptionAnnouncer(endpoint, transport, prefix, participants);
            announce = announcer::announce;
            dispose = announcer::disposeAndUnregister;
            respond = announcer::respondTo;
        }
        // Retransmission also works before the first periodic announcement.
        respond.accept(new AckNack(reader, writer, Set.of(1L), 1));
        byte[] original = transport.last();
        var sample = RtpsUserDataParser.readUserSamples(original, original.length, reader).getFirst();
        assertEquals(1, sample.sequenceNumber());
        var discovered = publication
                ? RtpsDiscoveryReader.readRemotePublication(original, original.length).orElseThrow()
                : RtpsDiscoveryReader.readRemoteSubscription(original, original.length).orElseThrow();
        assertEquals(endpointGuid, discovered.endpointGuid());
        assertEquals(endpoint.topicName(), discovered.topicName());
        assertEquals(endpoint.typeName(), discovered.typeName());
        assertEquals(endpoint.qos(), discovered.qos());

        for (int count = 1; count <= 2; count++) {
            int before = transport.meta.size();
            announce.run();
            assertEquals(before + 1, transport.meta.size());
            byte[] packet = transport.last();
            assertEquals(List.of(RtpsSubmessageKind.DATA, RtpsSubmessageKind.HEARTBEAT),
                    new RtpsMessageParser(packet, packet.length).submessages().stream().map(RtpsSubmessage::kind).toList());
            var heartbeat = RtpsUserDataParser.readHeartbeats(packet, packet.length, reader).getFirst();
            assertEquals(new Heartbeat(reader, prefix.toGuid(writer), 1, 1, count), heartbeat);
            assertArrayEquals(sample.payload(), RtpsUserDataParser.readUserSamples(packet, packet.length, reader).getFirst().payload());
        }

        dispose.run();
        var expectedDispose = new RtpsMessageBuilder(prefix);
        expectedDispose.dataDispose(reader, writer, 2, endpointGuid);
        assertArrayEquals(expectedDispose.bytes(), transport.last());
        dispose.run();
        assertArrayEquals(expectedDispose.bytes(), transport.last());

        // Legacy disposal does not replace the retained DATA or consume a sequence number.
        respond.accept(new AckNack(reader, writer, Set.of(1L), 2));
        assertArrayEquals(original, transport.last());
        respond.accept(new AckNack(reader, writer, Set.of(2L), 3));
        var gap = new RtpsMessageBuilder(prefix);
        gap.gap(reader, writer, 2);
        assertArrayEquals(gap.bytes(), transport.last());
        announce.run();
        byte[] packet = transport.last();
        assertEquals(1, RtpsUserDataParser.readUserSamples(packet, packet.length, reader).getFirst().sequenceNumber());
        assertEquals(transport.meta.size(), transport.unicast.size());
        for (int i = 0; i < transport.meta.size(); i++) {
            assertArrayEquals(transport.meta.get(i), transport.unicast.get(i));
            assertEquals(locator.socketAddress(), transport.addresses.get(i));
        }
    }

    @FunctionalInterface private interface Action { void run() throws IOException; }
    @FunctionalInterface private interface AckAction { void accept(AckNack ack) throws IOException; }

    private static final class RecordingTransport implements RtpsTransport {
        final List<byte[]> meta = new ArrayList<>();
        final List<byte[]> unicast = new ArrayList<>();
        final List<InetSocketAddress> addresses = new ArrayList<>();
        byte[] last() { return meta.getLast(); }
        public InetAddress multicastGroup() { return InetAddress.getLoopbackAddress(); }
        public void sendMetatraffic(byte[] bytes) { meta.add(bytes.clone()); }
        public void send(byte[] bytes, InetSocketAddress address) { unicast.add(bytes.clone()); addresses.add(address); }
        public void sendUserData(byte[] bytes) { fail("unexpected user data send"); }
        public Closeable listenMetatraffic(PacketHandler handler) { throw new UnsupportedOperationException(); }
        public Closeable listenUserData(PacketHandler handler) { throw new UnsupportedOperationException(); }
        public Locator unicastLocator(int port) { return new Locator(multicastGroup(), port); }
        public Locator multicastLocator(int port) { return unicastLocator(port); }
        public void close() { }
    }
}
