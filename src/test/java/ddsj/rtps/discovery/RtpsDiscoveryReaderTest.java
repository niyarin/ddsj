package ddsj.rtps.discovery;

import ddsj.rtps.message.RtpsMessageBuilder;
import ddsj.rtps.parameter.RtpsParameterListWriter;
import ddsj.rtps.protocol.ParameterId;
import ddsj.rtps.protocol.RtpsEntity;
import ddsj.rtps.transport.RtpsPacket;
import ddsj.rtps.transport.RtpsTransport;
import ddsj.rtps.types.*;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RtpsDiscoveryReaderTest {
    private final GuidPrefix remote = new GuidPrefix(new byte[12]);
    private final GuidPrefix local = new GuidPrefix(new byte[]{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});

    @Test
    void readsAndRegistersEveryPublicationAndSubscriptionInPacketOrder() throws Exception {
        var message = new RtpsMessageBuilder(remote);
        var participant = new RtpsParameterListWriter();
        participant.writeRaw(new byte[]{0, 3, 0, 0});
        participant.parameter(ParameterId.PARTICIPANT_GUID, remote.toGuid(new EntityId(new byte[]{0, 0, 1, (byte) 0xc1})).bytes());
        participant.parameter(ParameterId.SENTINEL, new byte[0]);
        message.data(RtpsEntity.UNKNOWN, RtpsEntity.PARTICIPANT_BUILTIN_TOPIC_WRITER, 1, participant.bytes());
        var topics = List.of("ros_discovery_info", "send_goal_response", "get_result_response");
        for (int i = 0; i < topics.size(); i++) {
            announce(message, guid(i + 1, true), topics.get(i), true, i + 1);
            message.infoTsInvalid();
            announce(message, guid(i + 1, false), topics.get(i), false, i + 1);
        }
        byte[] packet = message.bytes();
        var changes = RtpsDiscoveryReader.readDiscoveryData(packet, packet.length);
        assertEquals(7, changes.size());
        assertInstanceOf(DiscoveryChange.Participant.class, changes.get(0));
        for (int i = 0; i < topics.size(); i++) {
            var publication = assertInstanceOf(DiscoveryChange.Publication.class, changes.get(1 + i * 2));
            var subscription = assertInstanceOf(DiscoveryChange.Subscription.class, changes.get(2 + i * 2));
            assertEquals(topics.get(i), publication.change().endpoint().orElseThrow().topicName());
            assertEquals(topics.get(i), subscription.change().endpoint().orElseThrow().topicName());
        }
        var transport = new ReceivingTransport();
        var participants = new RemoteParticipantStore();
        var publications = new RemoteEndpointStore<RemotePublication>();
        var subscriptions = new RemoteEndpointStore<RemoteSubscription>();
        try (var listener = new SpdpDiscoveryListener(transport, local, participants, publications, subscriptions)) {
            transport.deliver(packet);
            assertTrue(participants.get(remote).isPresent());
            assertEquals(3, publications.snapshot().size());
            assertEquals(3, subscriptions.snapshot().size());
            for (int i = 0; i < topics.size(); i++) {
                assertEquals(topics.get(i), publications.get(guid(i + 1, true)).orElseThrow().topicName());
                assertEquals(topics.get(i), subscriptions.get(guid(i + 1, false)).orElseThrow().topicName());
            }
        }
    }

    @Test
    void skipsInvalidAndLocalDataAndAppliesDisposalAndReannouncementInOrder() throws Exception {
        var message = new RtpsMessageBuilder(remote);
        for (boolean publication : List.of(true, false)) {
            var writer = publication ? RtpsEntity.PUBLICATIONS_BUILTIN_TOPIC_WRITER : RtpsEntity.SUBSCRIPTIONS_BUILTIN_TOPIC_WRITER;
            var first = guid(1, publication);
            var second = guid(2, publication);
            announce(message, local.toGuid(first.entityId()), "local", publication, 1);
            announce(message, first, "old", publication, 2);
            message.data(RtpsEntity.UNKNOWN, writer, 3, new byte[0]);
            announce(message, second, "removed", publication, 4);
            message.dataDispose(RtpsEntity.UNKNOWN, writer, 5, first);
            message.dataDispose(RtpsEntity.UNKNOWN, writer, 6, second);
            announce(message, first, "new", publication, 7);
        }
        var transport = new ReceivingTransport();
        var publications = new RemoteEndpointStore<RemotePublication>();
        var subscriptions = new RemoteEndpointStore<RemoteSubscription>();
        try (var listener = new SpdpDiscoveryListener(transport, local, new RemoteParticipantStore(), publications, subscriptions)) {
            transport.deliver(message.bytes());
            assertEquals(1, publications.snapshot().size());
            assertEquals(1, subscriptions.snapshot().size());
            assertEquals("new", publications.get(guid(1, true)).orElseThrow().topicName());
            assertEquals("new", subscriptions.get(guid(1, false)).orElseThrow().topicName());
            assertTrue(publications.get(guid(2, true)).isEmpty());
            assertTrue(subscriptions.get(guid(2, false)).isEmpty());
        }
    }

    @Test
    void returnsEmptyListWhenNoDiscoveryDataIsPresent() {
        var message = new RtpsMessageBuilder(remote);
        message.infoTsInvalid();
        message.data(RtpsEntity.UNKNOWN, RtpsEntity.USER_WRITER_NO_KEY, 1, new byte[0]);
        byte[] packet = message.bytes();
        assertTrue(RtpsDiscoveryReader.readDiscoveryData(packet, packet.length).isEmpty());
    }

    private Guid guid(int number, boolean publication) {
        return remote.toGuid(new EntityId(new byte[]{0, 0, (byte) number, (byte) (publication ? 3 : 4)}));
    }

    private void announce(RtpsMessageBuilder message, Guid guid, String topic, boolean publication, long sequence) {
        var parameters = new RtpsParameterListWriter();
        parameters.writeRaw(new byte[]{0, 3, 0, 0});
        parameters.parameter(ParameterId.ENDPOINT_GUID, guid.bytes());
        parameters.stringParameter(ParameterId.TOPIC_NAME, topic);
        parameters.stringParameter(ParameterId.TYPE_NAME, "type");
        parameters.parameter(ParameterId.SENTINEL, new byte[0]);
        message.data(RtpsEntity.UNKNOWN, publication ? RtpsEntity.PUBLICATIONS_BUILTIN_TOPIC_WRITER
                : RtpsEntity.SUBSCRIPTIONS_BUILTIN_TOPIC_WRITER, sequence, parameters.bytes());
    }

    private static final class ReceivingTransport implements RtpsTransport {
        private PacketHandler handler;
        void deliver(byte[] bytes) { handler.handle(new RtpsPacket(bytes, bytes.length)); }
        public InetAddress multicastGroup() { return InetAddress.getLoopbackAddress(); }
        public void sendMetatraffic(byte[] bytes) { }
        public void sendUserData(byte[] bytes) { }
        public void send(byte[] bytes, InetSocketAddress address) { }
        public Closeable listenMetatraffic(PacketHandler handler) { this.handler = handler; return () -> { }; }
        public Closeable listenUserData(PacketHandler handler) { throw new UnsupportedOperationException(); }
        public Locator userUnicastLocator() { return unicastLocator(7411); }
        public Locator unicastLocator(int port) { return new Locator(multicastGroup(), port); }
        public Locator multicastLocator(int port) { return unicastLocator(port); }
        public void close() { }
    }
}
