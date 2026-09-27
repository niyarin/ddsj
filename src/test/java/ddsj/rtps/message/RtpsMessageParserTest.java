package ddsj.rtps.message;

import ddsj.rtps.protocol.RtpsSubmessageKind;
import ddsj.rtps.types.EntityId;
import ddsj.rtps.types.GuidPrefix;
import ddsj.rtps.types.RtpsTimestamp;
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
    void infoTs_parsed() {
        var builder = new RtpsMessageBuilder(TEST_PREFIX);
        builder.infoTs(new RtpsTimestamp(1234, 5678));
        byte[] packet = builder.bytes();

        var parser = new RtpsMessageParser(packet, packet.length);
        List<RtpsSubmessage> submessages = parser.submessages();

        assertEquals(1, submessages.size());
        assertEquals(RtpsSubmessageKind.INFO_TS, submessages.get(0).kind());
    }

    @Test
    void infoTs_timestampExtracted() {
        var builder = new RtpsMessageBuilder(TEST_PREFIX);
        var originalTs = new RtpsTimestamp(1234, 5678);
        builder.infoTs(originalTs);
        byte[] packet = builder.bytes();

        var parser = new RtpsMessageParser(packet, packet.length);
        List<RtpsSubmessage> submessages = parser.submessages();

        assertTrue(submessages.get(0).timestamp().isPresent());
        RtpsTimestamp parsed = submessages.get(0).timestamp().get();
        assertEquals(originalTs.seconds(), parsed.seconds());
        assertEquals(originalTs.fraction(), parsed.fraction());
    }

    @Test
    void infoTsInvalid_timestampIsInvalid() {
        var builder = new RtpsMessageBuilder(TEST_PREFIX);
        builder.infoTsInvalid();
        byte[] packet = builder.bytes();

        var parser = new RtpsMessageParser(packet, packet.length);
        List<RtpsSubmessage> submessages = parser.submessages();

        assertTrue(submessages.get(0).timestamp().isPresent());
        assertEquals(RtpsTimestamp.INVALID, submessages.get(0).timestamp().get());
    }

    @Test
    void infoTs_propagatesToSubsequentSubmessages() {
        var builder = new RtpsMessageBuilder(TEST_PREFIX);
        var timestamp = new RtpsTimestamp(9999, 8888);
        builder.infoTs(timestamp);
        var readerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x04});
        var writerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x03});
        builder.data(readerId, writerId, 1L, new byte[]{0x01, 0x02});
        byte[] packet = builder.bytes();

        var parser = new RtpsMessageParser(packet, packet.length);
        List<RtpsSubmessage> submessages = parser.submessages();

        assertEquals(2, submessages.size());

        // INFO_TS submessage
        assertEquals(RtpsSubmessageKind.INFO_TS, submessages.get(0).kind());
        assertTrue(submessages.get(0).timestamp().isPresent());

        // DATA submessage should have the same timestamp
        assertEquals(RtpsSubmessageKind.DATA, submessages.get(1).kind());
        assertTrue(submessages.get(1).timestamp().isPresent());
        assertEquals(timestamp.seconds(), submessages.get(1).timestamp().get().seconds());
        assertEquals(timestamp.fraction(), submessages.get(1).timestamp().get().fraction());
    }

    @Test
    void infoDst_parsed() {
        var builder = new RtpsMessageBuilder(TEST_PREFIX);
        var destPrefix = new GuidPrefix(new byte[]{
                (byte) 0xaa, (byte) 0xbb, (byte) 0xcc, (byte) 0xdd,
                (byte) 0xee, (byte) 0xff, 0x11, 0x22,
                0x33, 0x44, 0x55, 0x66
        });
        builder.infoDst(destPrefix);
        byte[] packet = builder.bytes();

        var parser = new RtpsMessageParser(packet, packet.length);
        List<RtpsSubmessage> submessages = parser.submessages();

        assertEquals(1, submessages.size());
        assertEquals(RtpsSubmessageKind.INFO_DST, submessages.get(0).kind());
    }

    @Test
    void infoDst_destinationExtracted() {
        var builder = new RtpsMessageBuilder(TEST_PREFIX);
        var destPrefix = new GuidPrefix(new byte[]{
                (byte) 0xaa, (byte) 0xbb, (byte) 0xcc, (byte) 0xdd,
                (byte) 0xee, (byte) 0xff, 0x11, 0x22,
                0x33, 0x44, 0x55, 0x66
        });
        builder.infoDst(destPrefix);
        byte[] packet = builder.bytes();

        var parser = new RtpsMessageParser(packet, packet.length);
        List<RtpsSubmessage> submessages = parser.submessages();

        assertTrue(submessages.get(0).destinationGuidPrefix().isPresent());
        assertArrayEquals(destPrefix.bytes(), submessages.get(0).destinationGuidPrefix().get().bytes());
    }

    @Test
    void infoDst_propagatesToSubsequentSubmessages() {
        var builder = new RtpsMessageBuilder(TEST_PREFIX);
        var destPrefix = new GuidPrefix(new byte[]{
                (byte) 0xaa, (byte) 0xbb, (byte) 0xcc, (byte) 0xdd,
                (byte) 0xee, (byte) 0xff, 0x11, 0x22,
                0x33, 0x44, 0x55, 0x66
        });
        builder.infoDst(destPrefix);
        var readerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x04});
        var writerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x03});
        builder.data(readerId, writerId, 1L, new byte[]{0x01});
        byte[] packet = builder.bytes();

        var parser = new RtpsMessageParser(packet, packet.length);
        List<RtpsSubmessage> submessages = parser.submessages();

        assertEquals(2, submessages.size());

        // DATA submessage should have the destination
        assertEquals(RtpsSubmessageKind.DATA, submessages.get(1).kind());
        assertTrue(submessages.get(1).destinationGuidPrefix().isPresent());
        assertArrayEquals(destPrefix.bytes(), submessages.get(1).destinationGuidPrefix().get().bytes());
    }

    @Test
    void withoutInfoDst_destinationIsEmpty() {
        var builder = new RtpsMessageBuilder(TEST_PREFIX);
        var readerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x04});
        var writerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x03});
        builder.data(readerId, writerId, 1L, new byte[]{0x01});
        byte[] packet = builder.bytes();

        var parser = new RtpsMessageParser(packet, packet.length);
        List<RtpsSubmessage> submessages = parser.submessages();

        assertTrue(submessages.get(0).destinationGuidPrefix().isEmpty());
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
    void multipleSubmessages_allParsed() {
        var builder = new RtpsMessageBuilder(TEST_PREFIX);
        builder.infoTs(new RtpsTimestamp(100, 200));
        var readerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x04});
        var writerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x03});
        builder.data(readerId, writerId, 1L, new byte[]{0x01});
        builder.heartbeat(readerId, writerId, 1L, 1L, 1);
        byte[] packet = builder.bytes();

        var parser = new RtpsMessageParser(packet, packet.length);
        List<RtpsSubmessage> submessages = parser.submessages();

        assertEquals(3, submessages.size());
        assertEquals(RtpsSubmessageKind.INFO_TS, submessages.get(0).kind());
        assertEquals(RtpsSubmessageKind.DATA, submessages.get(1).kind());
        assertEquals(RtpsSubmessageKind.HEARTBEAT, submessages.get(2).kind());
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

    @Test
    void withoutInfoTs_timestampIsEmpty() {
        var builder = new RtpsMessageBuilder(TEST_PREFIX);
        var readerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x04});
        var writerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x03});
        builder.data(readerId, writerId, 1L, new byte[]{0x01});
        byte[] packet = builder.bytes();

        var parser = new RtpsMessageParser(packet, packet.length);
        List<RtpsSubmessage> submessages = parser.submessages();

        assertTrue(submessages.get(0).timestamp().isEmpty());
    }

    @Test
    void dataFrag_parsed() {
        var builder = new RtpsMessageBuilder(TEST_PREFIX);
        var readerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x04});
        var writerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x03});
        builder.dataFrag(readerId, writerId, 1L, 1, 1, 1024, 5000, new byte[]{0x01, 0x02});
        byte[] packet = builder.bytes();

        var parser = new RtpsMessageParser(packet, packet.length);
        List<RtpsSubmessage> submessages = parser.submessages();

        assertEquals(1, submessages.size());
        assertEquals(RtpsSubmessageKind.DATA_FRAG, submessages.get(0).kind());
    }

    @Test
    void dataFrag_fragmentInfoExtracted() {
        var builder = new RtpsMessageBuilder(TEST_PREFIX);
        var readerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x04});
        var writerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x03});
        byte[] fragmentData = {(byte) 0xaa, (byte) 0xbb, (byte) 0xcc};
        builder.dataFrag(readerId, writerId, 42L, 3, 1, 1024, 5000, fragmentData);
        byte[] packet = builder.bytes();

        List<DataFragment> fragments = RtpsUserDataParser.readDataFragments(packet, packet.length, readerId);

        assertEquals(1, fragments.size());
        DataFragment frag = fragments.get(0);
        assertEquals(42L, frag.sequenceNumber());
        assertEquals(3, frag.fragmentStartingNum());
        assertEquals(1, frag.fragmentsInSubmessage());
        assertEquals(1024, frag.fragmentSize());
        assertEquals(5000, frag.sampleSize());
        assertArrayEquals(fragmentData, frag.fragmentData());
    }

    @Test
    void dataFrag_totalFragmentsCalculated() {
        var builder = new RtpsMessageBuilder(TEST_PREFIX);
        var readerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x04});
        var writerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x03});
        builder.dataFrag(readerId, writerId, 1L, 1, 1, 1024, 5000, new byte[]{0x01});
        byte[] packet = builder.bytes();

        List<DataFragment> fragments = RtpsUserDataParser.readDataFragments(packet, packet.length, readerId);

        DataFragment frag = fragments.get(0);
        // 5000 / 1024 = 4.88 -> 5 fragments
        assertEquals(5, frag.totalFragments());
    }

    @Test
    void dataFrag_isLastFragment() {
        var builder = new RtpsMessageBuilder(TEST_PREFIX);
        var readerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x04});
        var writerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x03});

        // Not last fragment (fragment 1 of 5)
        builder.dataFrag(readerId, writerId, 1L, 1, 1, 1024, 5000, new byte[]{0x01});
        byte[] packet1 = builder.bytes();
        List<DataFragment> fragments1 = RtpsUserDataParser.readDataFragments(packet1, packet1.length, readerId);
        assertFalse(fragments1.get(0).isLastFragment());

        // Last fragment (fragment 5 of 5)
        var builder2 = new RtpsMessageBuilder(TEST_PREFIX);
        builder2.dataFrag(readerId, writerId, 1L, 5, 1, 1024, 5000, new byte[]{0x01});
        byte[] packet2 = builder2.bytes();
        List<DataFragment> fragments2 = RtpsUserDataParser.readDataFragments(packet2, packet2.length, readerId);
        assertTrue(fragments2.get(0).isLastFragment());
    }

    @Test
    void dataFrag_withTimestamp() {
        var builder = new RtpsMessageBuilder(TEST_PREFIX);
        var timestamp = new RtpsTimestamp(1234, 5678);
        builder.infoTs(timestamp);
        var readerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x04});
        var writerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x03});
        builder.dataFrag(readerId, writerId, 1L, 1, 1, 1024, 2048, new byte[]{0x01});
        byte[] packet = builder.bytes();

        List<DataFragment> fragments = RtpsUserDataParser.readDataFragments(packet, packet.length, readerId);

        assertEquals(1, fragments.size());
        assertTrue(fragments.get(0).timestamp().isPresent());
        assertEquals(timestamp.seconds(), fragments.get(0).timestamp().get().seconds());
    }
}
