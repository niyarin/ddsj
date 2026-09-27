package ddsj.rtps.message;

import ddsj.rtps.protocol.RtpsEntity;
import ddsj.rtps.transport.RtpsPacket;
import ddsj.rtps.transport.RtpsTransport;
import ddsj.rtps.types.EntityId;
import ddsj.rtps.types.GuidPrefix;
import ddsj.rtps.types.Locator;
import ddsj.rtps.types.RtpsTimestamp;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class RtpsUserDataReaderTest {
    @Test
    void mixedPacketPreservesDeliveryOrderFilteringAndTimestampContext() throws Exception {
        var prefix = new GuidPrefix(new byte[12]);
        var readerId = new EntityId(new byte[]{0, 0, 2, 4});
        var otherReader = new EntityId(new byte[]{0, 0, 3, 4});
        var writerId = new EntityId(new byte[]{0, 0, 2, 3});
        var fragmentTimestamp = new RtpsTimestamp(10, 20);
        var dataTimestamp = new RtpsTimestamp(30, 40);
        var events = new ArrayList<String>();
        var samples = new ArrayList<UserDataSample>();
        var transport = new ReceivingTransport();
        try (var reader = new RtpsUserDataReader(transport, readerId, sample -> {
            samples.add(sample);
            events.add("sample:" + sample.sequenceNumber());
        }, heartbeat -> events.add("heartbeat:" + heartbeat.count()),
                heartbeat -> events.add("heartbeatFrag:" + heartbeat.count()))) {
            var message = new RtpsMessageBuilder(prefix);
            // Wire order deliberately differs from the existing delivery order by kind.
            message.heartbeatFrag(readerId, writerId, 2, 1, 7);
            message.heartbeat(readerId, writerId, 1, 2, 8);
            message.infoTs(fragmentTimestamp);
            message.dataFrag(readerId, writerId, 2, 1, 1, 4, 4, new byte[]{5, 6, 7, 8});
            message.infoTs(dataTimestamp);
            message.data(RtpsEntity.UNKNOWN, writerId, 1, new byte[]{1, 2, 3, 4});
            message.data(otherReader, writerId, 3, new byte[]{9});
            message.dataFrag(otherReader, writerId, 4, 1, 1, 4, 4, new byte[]{9, 9, 9, 9});
            message.heartbeat(otherReader, writerId, 1, 4, 9);
            message.heartbeatFrag(otherReader, writerId, 4, 1, 10);
            byte[] packet = message.bytes();
            transport.handler.handle(new RtpsPacket(packet, packet.length));

            assertEquals(List.of("sample:1", "sample:2", "heartbeat:8", "heartbeatFrag:7"), events);
            assertArrayEquals(new byte[]{1, 2, 3, 4}, samples.get(0).payload());
            assertArrayEquals(new byte[]{5, 6, 7, 8}, samples.get(1).payload());
            assertEquals(Optional.of(dataTimestamp), samples.get(0).timestamp());
            assertEquals(Optional.of(fragmentTimestamp), samples.get(1).timestamp());
            samples.forEach(sample -> assertEquals(prefix.toGuid(writerId), sample.writerGuid()));
        }
        assertNull(transport.handler);
    }

    private static final class ReceivingTransport implements RtpsTransport {
        private PacketHandler handler;

        public Closeable listenUserData(PacketHandler handler) {
            this.handler = handler;
            return () -> this.handler = null;
        }
        public Closeable listenMetatraffic(PacketHandler handler) { throw new UnsupportedOperationException(); }
        public InetAddress multicastGroup() { throw new UnsupportedOperationException(); }
        public void sendMetatraffic(byte[] message) { throw new UnsupportedOperationException(); }
        public void sendUserData(byte[] message) { throw new UnsupportedOperationException(); }
        public void send(byte[] message, InetSocketAddress address) { throw new UnsupportedOperationException(); }
        public Locator userUnicastLocator() { return unicastLocator(7411); }
        public Locator unicastLocator(int port) { throw new UnsupportedOperationException(); }
        public Locator multicastLocator(int port) { throw new UnsupportedOperationException(); }
        public void close() { handler = null; }
    }
}
