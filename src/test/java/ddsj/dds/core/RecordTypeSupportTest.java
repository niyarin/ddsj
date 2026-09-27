package ddsj.dds.core;

import org.junit.jupiter.api.Test;
import java.util.Arrays;
import static org.junit.jupiter.api.Assertions.*;

class RecordTypeSupportTest {
    record Message(int id, String text) {}
    record Scalars(boolean flag, byte b, short s, char c, int i, long l, float f, double d) {}
    record Unsupported(Integer value) {}
    record Validated(int value) {
        Validated { if (value < 0) throw new IllegalArgumentException("negative"); }
    }

    @Test void roundTripsRecordsAndCachesMetadata() {
        var support = TypeSupport.forRecord(Message.class);
        var value = new Message(42, "こんにちは\u0000DDS 😀");
        assertEquals(value, support.deserialize(support.serialize(value)));
        assertSame(support, TypeSupport.forRecord(Message.class));
        assertEquals(Message.class.getName(), support.getTypeName());
        assertEquals(Message.class, support.getType());
        assertFalse(support.hasKey());
        var scalars = TypeSupport.forRecord(Scalars.class);
        var sample = new Scalars(true, (byte) -128, (short) -32768, 'あ',
                Integer.MIN_VALUE, Long.MAX_VALUE, -1.25f, Math.PI);
        assertEquals(sample, scalars.deserialize(scalars.serialize(sample)));
    }

    @Test void usesDocumentedWireFormat() {
        assertArrayEquals(new byte[]{0, 0, 0, 1, 0, 0, 0, 2, 72, 105},
                TypeSupport.forRecord(Message.class).serialize(new Message(1, "Hi")));
    }

    @Test void rejectsUnsupportedTypesAndNullStrings() {
        assertThrows(IllegalArgumentException.class, () -> TypeSupport.forRecord(Unsupported.class));
        assertThrows(NullPointerException.class,
                () -> TypeSupport.forRecord(Message.class).serialize(new Message(1, null)));
    }

    @Test void rejectsMalformedPayloadsAndHonorsConstructorValidation() {
        var support = TypeSupport.forRecord(Message.class);
        byte[] valid = support.serialize(new Message(1, "Hi"));
        for (int size = 0; size < valid.length; size++) {
            byte[] truncated = Arrays.copyOf(valid, size);
            assertThrows(IllegalArgumentException.class, () -> support.deserialize(truncated));
        }
        assertThrows(IllegalArgumentException.class, () -> support.deserialize(Arrays.copyOf(valid, 11)));
        assertThrows(IllegalArgumentException.class,
                () -> support.deserialize(new byte[]{0, 0, 0, 1, -1, -1, -1, -1}));
        assertThrows(IllegalArgumentException.class,
                () -> support.deserialize(new byte[]{0, 0, 0, 1, 0, 0, 0, 1, (byte) 0xff}));
        assertThrows(IllegalArgumentException.class,
                () -> TypeSupport.forRecord(Validated.class).deserialize(new byte[]{-1, -1, -1, -1}));
    }
}
