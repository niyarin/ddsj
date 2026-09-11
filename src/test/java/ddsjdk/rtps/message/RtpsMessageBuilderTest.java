package ddsjdk.rtps.message;

import ddsjdk.rtps.protocol.RtpsSubmessageKind;
import ddsjdk.rtps.types.EntityId;
import ddsjdk.rtps.types.GuidPrefix;
import ddsjdk.rtps.types.RtpsTimestamp;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class RtpsMessageBuilderTest {

    private static final GuidPrefix TEST_PREFIX = new GuidPrefix(new byte[]{
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c
    });

    @Test
    void header_containsRtpsMagicAndVersion() {
        var builder = new RtpsMessageBuilder(TEST_PREFIX);
        byte[] bytes = builder.bytes();

        assertEquals('R', bytes[0]);
        assertEquals('T', bytes[1]);
        assertEquals('P', bytes[2]);
        assertEquals('S', bytes[3]);
        assertEquals(0x02, bytes[4]); // major version
        assertEquals(0x05, bytes[5]); // minor version
        assertEquals(0x01, bytes[6]); // vendor id high
        assertEquals(0x10, bytes[7]); // vendor id low
    }

    @Test
    void header_containsGuidPrefix() {
        var builder = new RtpsMessageBuilder(TEST_PREFIX);
        byte[] bytes = builder.bytes();

        for (int i = 0; i < 12; i++) {
            assertEquals(TEST_PREFIX.bytes()[i], bytes[8 + i]);
        }
    }

    @Test
    void header_size() {
        var builder = new RtpsMessageBuilder(TEST_PREFIX);
        byte[] bytes = builder.bytes();

        assertEquals(20, bytes.length); // RTPS header only
    }

    @Test
    void infoTs_submessageKindAndFlags() {
        var builder = new RtpsMessageBuilder(TEST_PREFIX);
        var timestamp = new RtpsTimestamp(1000, 500000);
        builder.infoTs(timestamp);
        byte[] bytes = builder.bytes();

        assertEquals(RtpsSubmessageKind.INFO_TS, bytes[20] & 0xff);
        assertEquals(0x01, bytes[21] & 0xff); // little endian flag
    }

    @Test
    void infoTs_bodySize() {
        var builder = new RtpsMessageBuilder(TEST_PREFIX);
        builder.infoTs(new RtpsTimestamp(1000, 500000));
        byte[] bytes = builder.bytes();

        int bodySize = (bytes[22] & 0xff) | ((bytes[23] & 0xff) << 8);
        assertEquals(8, bodySize); // seconds(4) + fraction(4)
    }

    @Test
    void infoTs_containsTimestampValues() {
        var builder = new RtpsMessageBuilder(TEST_PREFIX);
        int seconds = 0x12345678;
        int fraction = 0xabcdef01;
        builder.infoTs(new RtpsTimestamp(seconds, fraction));
        byte[] bytes = builder.bytes();

        // little endian seconds at offset 24
        assertEquals(0x78, bytes[24] & 0xff);
        assertEquals(0x56, bytes[25] & 0xff);
        assertEquals(0x34, bytes[26] & 0xff);
        assertEquals(0x12, bytes[27] & 0xff);

        // little endian fraction at offset 28
        assertEquals(0x01, bytes[28] & 0xff);
        assertEquals(0xef, bytes[29] & 0xff);
        assertEquals(0xcd, bytes[30] & 0xff);
        assertEquals(0xab, bytes[31] & 0xff);
    }

    @Test
    void infoTsInvalid_hasInvalidFlag() {
        var builder = new RtpsMessageBuilder(TEST_PREFIX);
        builder.infoTsInvalid();
        byte[] bytes = builder.bytes();

        assertEquals(RtpsSubmessageKind.INFO_TS, bytes[20] & 0xff);
        assertEquals(0x03, bytes[21] & 0xff); // little endian + invalid flag
        int bodySize = (bytes[22] & 0xff) | ((bytes[23] & 0xff) << 8);
        assertEquals(0, bodySize); // no body for invalid timestamp
    }

    @Test
    void data_submessageKindAndFlags() {
        var builder = new RtpsMessageBuilder(TEST_PREFIX);
        var readerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x04});
        var writerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x03});
        builder.data(readerId, writerId, 1L, new byte[]{0x01, 0x02, 0x03});
        byte[] bytes = builder.bytes();

        assertEquals(RtpsSubmessageKind.DATA, bytes[20] & 0xff);
        assertEquals(0x05, bytes[21] & 0xff); // flags: little endian + data present + inline qos absent
    }

    @Test
    void data_containsEntityIds() {
        var builder = new RtpsMessageBuilder(TEST_PREFIX);
        var readerId = new EntityId(new byte[]{0x11, 0x22, 0x33, 0x44});
        var writerId = new EntityId(new byte[]{0x55, 0x66, 0x77, (byte) 0x88});
        builder.data(readerId, writerId, 1L, new byte[]{0x01});
        byte[] bytes = builder.bytes();

        // reader ID at offset 24+4 = 28
        assertEquals(0x11, bytes[28] & 0xff);
        assertEquals(0x22, bytes[29] & 0xff);
        assertEquals(0x33, bytes[30] & 0xff);
        assertEquals(0x44, bytes[31] & 0xff);

        // writer ID at offset 32
        assertEquals(0x55, bytes[32] & 0xff);
        assertEquals(0x66, bytes[33] & 0xff);
        assertEquals(0x77, bytes[34] & 0xff);
        assertEquals(0x88, bytes[35] & 0xff);
    }

    @Test
    void data_containsSequenceNumber() {
        var builder = new RtpsMessageBuilder(TEST_PREFIX);
        var readerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x04});
        var writerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x03});
        long seqNum = 0x0000000100000002L;
        builder.data(readerId, writerId, seqNum, new byte[]{0x01});
        byte[] bytes = builder.bytes();

        // sequence number high (offset 36) = 1
        assertEquals(0x01, bytes[36] & 0xff);
        assertEquals(0x00, bytes[37] & 0xff);
        assertEquals(0x00, bytes[38] & 0xff);
        assertEquals(0x00, bytes[39] & 0xff);

        // sequence number low (offset 40) = 2
        assertEquals(0x02, bytes[40] & 0xff);
        assertEquals(0x00, bytes[41] & 0xff);
        assertEquals(0x00, bytes[42] & 0xff);
        assertEquals(0x00, bytes[43] & 0xff);
    }

    @Test
    void data_containsPayload() {
        var builder = new RtpsMessageBuilder(TEST_PREFIX);
        var readerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x04});
        var writerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x03});
        byte[] payload = {(byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef};
        builder.data(readerId, writerId, 1L, payload);
        byte[] bytes = builder.bytes();

        // payload starts at offset 44
        assertEquals(0xde, bytes[44] & 0xff);
        assertEquals(0xad, bytes[45] & 0xff);
        assertEquals(0xbe, bytes[46] & 0xff);
        assertEquals(0xef, bytes[47] & 0xff);
    }

    @Test
    void heartbeat_submessageKindAndFlags() {
        var builder = new RtpsMessageBuilder(TEST_PREFIX);
        var readerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x04});
        var writerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x03});
        builder.heartbeat(readerId, writerId, 1L, 10L, 5);
        byte[] bytes = builder.bytes();

        assertEquals(RtpsSubmessageKind.HEARTBEAT, bytes[20] & 0xff);
    }

    @Test
    void heartbeat_bodySize() {
        var builder = new RtpsMessageBuilder(TEST_PREFIX);
        var readerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x04});
        var writerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x03});
        builder.heartbeat(readerId, writerId, 1L, 10L, 5);
        byte[] bytes = builder.bytes();

        int bodySize = (bytes[22] & 0xff) | ((bytes[23] & 0xff) << 8);
        // reader(4) + writer(4) + firstSeq(8) + lastSeq(8) + count(4) = 28
        assertEquals(28, bodySize);
    }

    @Test
    void gap_submessageKind() {
        var builder = new RtpsMessageBuilder(TEST_PREFIX);
        var readerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x04});
        var writerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x03});
        builder.gap(readerId, writerId, 5L);
        byte[] bytes = builder.bytes();

        assertEquals(RtpsSubmessageKind.GAP, bytes[20] & 0xff);
    }

    @Test
    void ackNack_submessageKind() {
        var builder = new RtpsMessageBuilder(TEST_PREFIX);
        var readerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x04});
        var writerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x03});
        builder.ackNack(readerId, writerId, 1L, Set.of(2L, 3L), 1);
        byte[] bytes = builder.bytes();

        assertEquals(RtpsSubmessageKind.ACKNACK, bytes[20] & 0xff);
    }

    @Test
    void ackNack_emptyMissingSet() {
        var builder = new RtpsMessageBuilder(TEST_PREFIX);
        var readerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x04});
        var writerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x03});
        builder.ackNack(readerId, writerId, 10L, Set.of(), 1);
        byte[] bytes = builder.bytes();

        assertEquals(RtpsSubmessageKind.ACKNACK, bytes[20] & 0xff);
        // should not throw exception
    }

    @Test
    void multipleSubmessages_concatenated() {
        var builder = new RtpsMessageBuilder(TEST_PREFIX);
        builder.infoTs(new RtpsTimestamp(100, 200));
        var readerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x04});
        var writerId = new EntityId(new byte[]{0x00, 0x00, 0x00, 0x03});
        builder.data(readerId, writerId, 1L, new byte[]{0x01});
        byte[] bytes = builder.bytes();

        // header(20) + INFO_TS(4+8) + DATA(4+24+1)
        assertTrue(bytes.length > 20 + 12);
        assertEquals(RtpsSubmessageKind.INFO_TS, bytes[20] & 0xff);
        assertEquals(RtpsSubmessageKind.DATA, bytes[32] & 0xff);
    }
}
