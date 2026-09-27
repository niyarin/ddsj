package ddsjdk.dds.core;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.*;

class CdrRecordTypeSupportTest {
    record Message(int id, String text) {}
    record Point(double x, double y, double z) {}
    record Aligned(byte b, long l, short s, String text, int i) {}
    record Scalars(boolean flag, byte b, short s, char c, int i, long l, float f, double d) {}
    record LongValue(long value) {}
    record Flag(boolean value) {}
    record CharacterValue(char value) {}
    record Unsupported(Integer value) {}
    record Nested(Message value) {}
    record ArrayValue(byte[] value) {}
    record Empty() {}
    record Validated(int value) {
        Validated { if (value < 0) throw new IllegalArgumentException("negative"); }
    }

    private static byte[] hex(String value) { return HexFormat.of().parseHex(value.replace(" ", "")); }

    @Test void cachesSupportAndPreservesMetadata() {
        var support = TypeSupport.forCdrRecord(Message.class);
        assertSame(support, TypeSupport.forCdrRecord(Message.class));
        assertEquals(Message.class, support.getType());
        assertEquals(Message.class.getName(), support.getTypeName());
        assertFalse(support.hasKey());
        assertNull(support.extractKey(new Message(1, "Hi")));
        assertNotSame(support, TypeSupport.forRecord(Message.class));
    }

    @Test void explicitTypeNamesAreIndependentAndPreserveCdrEncoding() {
        var defaults = TypeSupport.forCdrRecord(Point.class);
        var rosName = TypeSupport.forCdrRecord(Point.class, "geometry_msgs/msg/Point");
        var ddsName = TypeSupport.forCdrRecord(Point.class, "geometry_msgs::msg::dds_::Point_");
        var point = new Point(1, 2, 3);
        assertEquals("geometry_msgs/msg/Point", rosName.getTypeName());
        assertEquals("geometry_msgs::msg::dds_::Point_", ddsName.getTypeName());
        assertEquals(Point.class.getName(), defaults.getTypeName());
        assertSame(defaults, TypeSupport.forCdrRecord(Point.class));
        assertEquals(Point.class, ddsName.getType());
        assertFalse(ddsName.hasKey());
        assertArrayEquals(defaults.serialize(point), ddsName.serialize(point));
        assertEquals(point, ddsName.deserialize(rosName.serialize(point)));
        assertThrows(NullPointerException.class, () -> TypeSupport.forCdrRecord(Point.class, null));
        assertThrows(NullPointerException.class, () -> TypeSupport.forCdrRecord(null, "Point"));
        assertThrows(IllegalArgumentException.class, () -> TypeSupport.forCdrRecord(Unsupported.class, "Unsupported"));
    }

    @Test void writesLittleEndianCdrWithTerminatedStringAndPaddingCount() {
        var support = TypeSupport.forCdrRecord(Message.class);
        byte[] expected = hex("00010001 01000000 03000000 48690000");
        assertArrayEquals(expected, support.serialize(new Message(1, "Hi")));
        assertEquals(new Message(1, "Hi"), support.deserialize(expected));
        assertEquals(new Message(1, "Hi"), support.deserialize(hex("00000001 00000001 00000003 48690000")));
        // Accept unpadded CDR and legacy RTPS padding with zero options as well.
        assertEquals(new Message(1, "Hi"), support.deserialize(hex("00010000 01000000 03000000 486900")));
        assertEquals(new Message(1, "Hi"), support.deserialize(hex("00010000 01000000 03000000 48690000")));
    }

    @Test void alignsRelativeToCdrBodyInBothByteOrders() {
        var support = TypeSupport.forCdrRecord(Aligned.class);
        var value = new Aligned((byte) 127, 0x0102030405060708L, (short) 0x1122, "Hi", 0x11223344);
        byte[] little = hex("00010000 7f00000000000000 0807060504030201 22110000 03000000 48690000 44332211");
        byte[] big = hex("00000000 7f00000000000000 0102030405060708 11220000 00000003 48690000 11223344");
        assertArrayEquals(little, support.serialize(value));
        assertEquals(value, support.deserialize(little));
        assertEquals(value, support.deserialize(big));
        assertArrayEquals(hex("00010000 0807060504030201"),
                TypeSupport.forCdrRecord(LongValue.class).serialize(new LongValue(0x0102030405060708L)));
    }

    @Test void readsEveryPrimitiveInBothByteOrders() {
        var support = TypeSupport.forCdrRecord(Scalars.class);
        var value = new Scalars(true, (byte) -128, Short.MIN_VALUE, '\u00ff',
                Integer.MIN_VALUE, Long.MAX_VALUE, -1.25f, -Math.PI);
        for (ByteOrder order : new ByteOrder[]{ByteOrder.LITTLE_ENDIAN, ByteOrder.BIG_ENDIAN}) {
            var expected = ByteBuffer.allocate(44).order(order);
            expected.put(new byte[]{0, (byte) (order == ByteOrder.LITTLE_ENDIAN ? 1 : 0), 0, 0});
            expected.put((byte) 1).put((byte) -128).putShort(Short.MIN_VALUE).put((byte) 255);
            expected.position(12);
            expected.putInt(Integer.MIN_VALUE);
            expected.position(20);
            expected.putLong(Long.MAX_VALUE).putFloat(-1.25f);
            expected.position(36);
            expected.putDouble(-Math.PI);
            assertEquals(value, support.deserialize(expected.array()));
            if (order == ByteOrder.LITTLE_ENDIAN) assertArrayEquals(expected.array(), support.serialize(value));
        }
        var special = new Scalars(false, (byte) 0, (short) 0, '\0', 0, Long.MIN_VALUE, -0.0f, Double.NaN);
        assertEquals(special, support.deserialize(support.serialize(special)));
    }

    @Test void encodesUtf8ByteLengthAndEmptyStrings() {
        var support = TypeSupport.forCdrRecord(Message.class);
        assertArrayEquals(hex("00010000 00000000 08000000 e38182f09f988000"),
                support.serialize(new Message(0, "あ😀")));
        assertArrayEquals(hex("00010003 00000000 01000000 00000000"),
                support.serialize(new Message(0, "")));
        for (String text : new String[]{"", "あ😀", "こんにちはDDS"}) {
            var value = new Message(42, text);
            assertEquals(value, support.deserialize(support.serialize(value)));
        }
    }

    @Test void rejectsInvalidComponentsAndValues() {
        assertThrows(IllegalArgumentException.class, () -> TypeSupport.forCdrRecord(Unsupported.class));
        assertThrows(IllegalArgumentException.class, () -> TypeSupport.forCdrRecord(Nested.class));
        assertThrows(IllegalArgumentException.class, () -> TypeSupport.forCdrRecord(ArrayValue.class));
        assertThrows(IllegalArgumentException.class, () -> TypeSupport.forCdrRecord(Record.class));
        assertThrows(NullPointerException.class, () -> TypeSupport.forCdrRecord(null));
        var support = TypeSupport.forCdrRecord(Message.class);
        assertThrows(NullPointerException.class, () -> support.serialize(null));
        assertThrows(NullPointerException.class, () -> support.deserialize(null));
        assertThrows(NullPointerException.class, () -> support.serialize(new Message(0, null)));
        assertThrows(IllegalArgumentException.class, () -> support.serialize(new Message(0, "a\0b")));
        assertThrows(IllegalArgumentException.class, () -> support.serialize(new Message(0, "\ud800")));
        assertThrows(IllegalArgumentException.class,
                () -> TypeSupport.forCdrRecord(CharacterValue.class).serialize(new CharacterValue('あ')));
    }

    @Test void rejectsTruncationMalformedStringsHeadersAndTrailingData() {
        var support = TypeSupport.forCdrRecord(Message.class);
        byte[] valid = support.serialize(new Message(1, "Hi"));
        for (int length = 0; length < valid.length; length++) {
            byte[] truncated = Arrays.copyOf(valid, length);
            assertThrows(IllegalArgumentException.class, () -> support.deserialize(truncated));
        }
        for (String invalid : new String[]{
                "00030000 01000000 03000000 48690000", // parameter-list encoding
                "00070000 01000000 03000000 48690000", // XCDR2
                "01010000 01000000 03000000 48690000", // invalid identifier
                "00010100 01000000 03000000 48690000", // unsupported options
                "00010004 01000000 03000000 48690000",
                "00010000 01000000 00000000", // zero length
                "00010000 01000000 ffffffff", // unsigned length overflow
                "00010000 01000000 03000000 486901", // missing NUL
                "00010000 01000000 02000000 ff00", // invalid UTF-8
                "00010000 01000000 04000000 61006200", // embedded NUL
                "00010001 01000000 03000000 486900ff", // invalid padding
                "00010000 01000000 03000000 4869000000000000" // excess data
        }) assertThrows(IllegalArgumentException.class, () -> support.deserialize(hex(invalid)), invalid);
        assertThrows(IllegalArgumentException.class,
                () -> TypeSupport.forCdrRecord(Flag.class).deserialize(hex("00010000 02")));
        assertThrows(IllegalArgumentException.class,
                () -> TypeSupport.forCdrRecord(Validated.class).deserialize(hex("00010000 ffffffff")));
    }

    @Test void readsCdrByteOrderIndependentlyOfRtpsSubmessageByteOrder() {
        var prefix = new ddsjdk.rtps.types.GuidPrefix(new byte[12]);
        var reader = ddsjdk.rtps.protocol.RtpsEntity.USER_READER_NO_KEY;
        var writer = ddsjdk.rtps.protocol.RtpsEntity.USER_WRITER_NO_KEY;
        var message = new ddsjdk.rtps.message.RtpsMessageBuilder(prefix);
        // RTPS builder uses little endian, while this payload is big endian CDR.
        message.data(reader, writer, 1, hex("00000001 00000001 00000003 48690000"));
        message.heartbeat(reader, writer, 1, 1, 1);
        byte[] packet = message.bytes();
        var samples = ddsjdk.rtps.message.RtpsUserDataParser.readUserSamples(packet, packet.length, reader);
        assertEquals(1, samples.size());
        assertEquals(new Message(1, "Hi"), TypeSupport.forCdrRecord(Message.class).deserialize(samples.getFirst().payload()));
        assertEquals(1, ddsjdk.rtps.message.RtpsUserDataParser.readHeartbeats(packet, packet.length, reader).size());
    }

    @Test void supportsEmptyRecords() {
        var support = TypeSupport.forCdrRecord(Empty.class);
        assertArrayEquals(hex("00010000"), support.serialize(new Empty()));
        assertEquals(new Empty(), support.deserialize(hex("00000000")));
    }
}
