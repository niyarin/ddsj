package ddsj.rtps.message;

import ddsj.rtps.types.EntityId;
import ddsj.rtps.types.GuidPrefix;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class HeartbeatFragTest {

    private static final EntityId READER_ID = new EntityId(new byte[]{0x00, 0x00, 0x02, 0x04});
    private static final EntityId WRITER_ID = new EntityId(new byte[]{0x00, 0x00, 0x02, 0x03});

    @Test
    void buildAndParseHeartbeatFrag() {
        GuidPrefix guidPrefix = new GuidPrefix(new byte[]{
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06,
                0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c
        });

        RtpsMessageBuilder builder = new RtpsMessageBuilder(guidPrefix);
        builder.heartbeatFrag(READER_ID, WRITER_ID, 42L, 5, 100);

        byte[] packet = builder.bytes();
        List<HeartbeatFrag> heartbeatFrags = RtpsUserDataParser.readHeartbeatFrags(packet, packet.length, READER_ID);

        assertEquals(1, heartbeatFrags.size());
        HeartbeatFrag hbf = heartbeatFrags.get(0);

        assertEquals(READER_ID, hbf.readerId());
        assertEquals(WRITER_ID, hbf.writerId());
        assertEquals(guidPrefix, hbf.writerGuid().prefix());
        assertEquals(42L, hbf.writerSequenceNumber());
        assertEquals(5, hbf.lastFragmentNum());
        assertEquals(100, hbf.count());
    }

    @Test
    void parseHeartbeatFrag_wrongReaderId_ignored() {
        GuidPrefix guidPrefix = new GuidPrefix(new byte[]{
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06,
                0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c
        });

        EntityId differentReaderId = new EntityId(new byte[]{0x00, 0x00, (byte) 0x99, 0x04});

        RtpsMessageBuilder builder = new RtpsMessageBuilder(guidPrefix);
        builder.heartbeatFrag(differentReaderId, WRITER_ID, 42L, 5, 100);

        byte[] packet = builder.bytes();
        List<HeartbeatFrag> heartbeatFrags = RtpsUserDataParser.readHeartbeatFrags(packet, packet.length, READER_ID);

        assertTrue(heartbeatFrags.isEmpty());
    }

    @Test
    void parseHeartbeatFrag_unknownReaderId_accepted() {
        GuidPrefix guidPrefix = new GuidPrefix(new byte[]{
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06,
                0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c
        });

        EntityId unknownReaderId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x00});

        RtpsMessageBuilder builder = new RtpsMessageBuilder(guidPrefix);
        builder.heartbeatFrag(unknownReaderId, WRITER_ID, 42L, 5, 100);

        byte[] packet = builder.bytes();
        List<HeartbeatFrag> heartbeatFrags = RtpsUserDataParser.readHeartbeatFrags(packet, packet.length, READER_ID);

        assertEquals(1, heartbeatFrags.size());
    }

    @Test
    void parseHeartbeatFrag_largeSequenceNumber() {
        GuidPrefix guidPrefix = new GuidPrefix(new byte[]{
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06,
                0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c
        });

        long largeSequenceNumber = 0x1_0000_0001L; // > 32 bits

        RtpsMessageBuilder builder = new RtpsMessageBuilder(guidPrefix);
        builder.heartbeatFrag(READER_ID, WRITER_ID, largeSequenceNumber, 10, 50);

        byte[] packet = builder.bytes();
        List<HeartbeatFrag> heartbeatFrags = RtpsUserDataParser.readHeartbeatFrags(packet, packet.length, READER_ID);

        assertEquals(1, heartbeatFrags.size());
        assertEquals(largeSequenceNumber, heartbeatFrags.get(0).writerSequenceNumber());
    }
}
