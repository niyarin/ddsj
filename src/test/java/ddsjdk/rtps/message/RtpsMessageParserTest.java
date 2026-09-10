package ddsjdk.rtps.message;

import ddsjdk.rtps.protocol.RtpsSubmessageKind;
import ddsjdk.rtps.types.EntityId;
import ddsjdk.rtps.types.GuidPrefix;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class RtpsMessageParserTest {

    private static final GuidPrefix TEST_PREFIX = new GuidPrefix(new byte[]{
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c
    });

    @Test
    void emptyPacket_returnsEmptyList() {
        var parser = new RtpsMessageParser(new byte[0], 0);
        assertTrue(parser.submessages().isEmpty());
    }

    @Test
    void invalidMagic_returnsEmptyList() {
        byte[] packet = {'X', 'Y', 'Z', 'W', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        var parser = new RtpsMessageParser(packet, packet.length);
        assertTrue(parser.submessages().isEmpty());
    }

    @Test
    void headerOnly_returnsEmptyList() {
        var builder = new RtpsMessageBuilder(TEST_PREFIX);
        byte[] packet = builder.bytes();
        var parser = new RtpsMessageParser(packet, packet.length);
        assertTrue(parser.submessages().isEmpty());
    }

    @Test
    void data_parsed() {
        var builder = new RtpsMessageBuilder(TEST_PREFIX);
        var readerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x04});
        var writerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x03});
        builder.data(readerId, writerId, 42L, new byte[]{0x01, 0x02, 0x03});
        byte[] packet = builder.bytes();

        var parser = new RtpsMessageParser(packet, packet.length);
        List<RtpsSubmessage> submessages = parser.submessages();

        assertEquals(1, submessages.size());
        assertEquals(RtpsSubmessageKind.DATA, submessages.get(0).kind());
        assertTrue(submessages.get(0).littleEndian());
    }

    @Test
    void data_guidPrefixFromHeader() {
        var builder = new RtpsMessageBuilder(TEST_PREFIX);
        var readerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x04});
        var writerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x03});
        builder.data(readerId, writerId, 1L, new byte[]{0x01});
        byte[] packet = builder.bytes();

        var parser = new RtpsMessageParser(packet, packet.length);
        List<RtpsSubmessage> submessages = parser.submessages();

        assertArrayEquals(TEST_PREFIX.bytes(), submessages.get(0).sourceGuidPrefix().bytes());
    }

    @Test
    void heartbeat_parsed() {
        var builder = new RtpsMessageBuilder(TEST_PREFIX);
        var readerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x04});
        var writerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x03});
        builder.heartbeat(readerId, writerId, 1L, 100L, 5);
        byte[] packet = builder.bytes();

        var parser = new RtpsMessageParser(packet, packet.length);
        List<RtpsSubmessage> submessages = parser.submessages();

        assertEquals(1, submessages.size());
        assertEquals(RtpsSubmessageKind.HEARTBEAT, submessages.get(0).kind());
    }

    @Test
    void gap_parsed() {
        var builder = new RtpsMessageBuilder(TEST_PREFIX);
        var readerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x04});
        var writerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x03});
        builder.gap(readerId, writerId, 10L);
        byte[] packet = builder.bytes();

        var parser = new RtpsMessageParser(packet, packet.length);
        List<RtpsSubmessage> submessages = parser.submessages();

        assertEquals(1, submessages.size());
        assertEquals(RtpsSubmessageKind.GAP, submessages.get(0).kind());
    }

    @Test
    void ackNack_parsed() {
        var builder = new RtpsMessageBuilder(TEST_PREFIX);
        var readerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x04});
        var writerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x03});
        builder.ackNack(readerId, writerId, 1L, Set.of(2L, 5L), 1);
        byte[] packet = builder.bytes();

        var parser = new RtpsMessageParser(packet, packet.length);
        List<RtpsSubmessage> submessages = parser.submessages();

        assertEquals(1, submessages.size());
        assertEquals(RtpsSubmessageKind.ACKNACK, submessages.get(0).kind());
    }

    @Test
    void truncatedPacket_parsesAvailableSubmessages() {
        var builder = new RtpsMessageBuilder(TEST_PREFIX);
        var readerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x04});
        var writerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x03});
        builder.data(readerId, writerId, 1L, new byte[]{0x01, 0x02, 0x03, 0x04});
        builder.heartbeat(readerId, writerId, 1L, 1L, 1);
        byte[] fullPacket = builder.bytes();

        // truncate before heartbeat completes
        int truncatedLength = fullPacket.length - 10;
        var parser = new RtpsMessageParser(fullPacket, truncatedLength);
        List<RtpsSubmessage> submessages = parser.submessages();

        // should only parse DATA, not the incomplete HEARTBEAT
        assertEquals(1, submessages.size());
        assertEquals(RtpsSubmessageKind.DATA, submessages.get(0).kind());
    }

    @Test
    void bodyCloned_modificationsDoNotAffectOriginal() {
        var builder = new RtpsMessageBuilder(TEST_PREFIX);
        var readerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x04});
        var writerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x03});
        builder.data(readerId, writerId, 1L, new byte[]{0x01, 0x02, 0x03});
        byte[] packet = builder.bytes();

        var parser = new RtpsMessageParser(packet, packet.length);
        List<RtpsSubmessage> submessages = parser.submessages();

        byte[] body1 = submessages.get(0).body();
        byte[] body2 = submessages.get(0).body();

        body1[0] = (byte) 0xff;
        assertNotEquals(body1[0], body2[0]);
    }
}
