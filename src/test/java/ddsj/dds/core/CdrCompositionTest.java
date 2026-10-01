package ddsj.dds.core;

import ddsj.cdr.*;
import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.HexFormat;
import static org.junit.jupiter.api.Assertions.*;

class CdrCompositionTest {
    record Inner(int x, long y) {}
    record Outer(byte prefix, Inner inner, byte tail) {}
    record Wide(int first, long second, double third) {}
    record Tiny(byte value) {}
    record Records(Tiny[] values, int tail) {}
    record FixedRecords(@CdrFixedLength(2) Tiny[] values, int tail) {}
    record Strings(String[] values) {}
    record FixedStrings(@CdrFixedLength(2) String[] values) {}
    record Longs(long[] values, byte tail) {}
    record FixedInts(byte prefix, @CdrFixedLength(2) int[] values) {}
    record AllArrays(boolean[] booleans, byte[] bytes, char[] chars, short[] shorts,
                     int[] ints, long[] longs, float[] floats, double[] doubles) {}
    record Group(String[] names) {}
    record Groups(Group[] groups) {}
    record Recursive(Recursive child) {}
    record RecursiveArray(RecursiveArray[] children) {}
    record MutualA(MutualB child) {}
    record MutualB(MutualA child) {}
    record Matrix(int[][] values) {}
    record Boxed(Integer[] values) {}
    record Empty() {}
    record Empties(Empty[] values) {}
    record HugeFixed(@CdrFixedLength(Integer.MAX_VALUE) long[] values) {}

    static byte[] hex(String text) { return HexFormat.of().parseHex(text.replace(" ", "")); }

    @Test void nestedRecordUsesTheEnclosingAlignmentOriginAndOnlyOneHeader() {
        var support = CdrRecordTypeSupport.of(Outer.class);
        var value = new Outer((byte) 0xaa, new Inner(0x11223344, 0x0102030405060708L), (byte) 0xff);
        byte[] little = hex("00010003 aa000000 44332211 0807060504030201 ff000000");
        byte[] big = hex("00000003 aa000000 11223344 0102030405060708 ff000000");
        assertArrayEquals(little, support.serialize(value));
        assertEquals(value, support.deserialize(little));
        assertEquals(value, support.deserialize(big));
        assertThrows(NullPointerException.class, () -> support.serialize(new Outer((byte) 0, null, (byte) 0)));
    }

    @Test void cdr2CapsAlignmentButStillWritesEightByteValues() {
        var support = CdrRecordTypeSupport.of(Wide.class);
        var value = new Wide(0x11223344, 0x0102030405060708L, 1.0);
        byte[] cdr1 = hex("00010000 44332211 00000000 0807060504030201 000000000000f03f");
        byte[] cdr2 = hex("00070000 44332211 0807060504030201 000000000000f03f");
        byte[] big2 = hex("00060000 11223344 0102030405060708 3ff0000000000000");
        assertArrayEquals(cdr1, support.serialize(value));
        assertArrayEquals(cdr2, support.serializeCdr2(value));
        assertEquals(value, support.deserialize(cdr1));
        assertEquals(value, support.deserialize(cdr2));
        assertEquals(value, support.deserialize(big2));
    }

    @Test void recordSequencesHaveADelimiterOnlyInCdr2() {
        var support = CdrRecordTypeSupport.of(Records.class);
        var value = new Records(new Tiny[]{new Tiny((byte) 0x11), new Tiny((byte) 0x22)}, 0x11223344);
        byte[] cdr1 = hex("00010000 02000000 11220000 44332211");
        byte[] cdr2 = hex("00070000 06000000 02000000 11220000 44332211");
        byte[] big2 = hex("00060000 00000006 00000002 11220000 11223344");
        assertArrayEquals(cdr1, support.serialize(value));
        assertArrayEquals(cdr2, support.serializeCdr2(value));
        for (byte[] payload : new byte[][]{cdr1, cdr2, big2}) {
            var result = support.deserialize(payload);
            assertArrayEquals(value.values(), result.values());
            assertEquals(value.tail(), result.tail());
        }
        for (int length : new int[]{-1, 0, 4, 5, 7, Integer.MAX_VALUE}) {
            byte[] corrupt = cdr2.clone();
            java.nio.ByteBuffer.wrap(corrupt).order(java.nio.ByteOrder.LITTLE_ENDIAN).putInt(4, length);
            assertThrows(IllegalArgumentException.class, () -> support.deserialize(corrupt));
        }
        var empty = new Records(new Tiny[0], 1);
        assertArrayEquals(hex("00070000 04000000 00000000 01000000"), support.serializeCdr2(empty));
        assertEquals(0, support.deserialize(support.serializeCdr2(empty)).values().length);
    }

    @Test void fixedRecordArraysHaveNoCountAndCdr2HasADelimiter() {
        var support = CdrRecordTypeSupport.of(FixedRecords.class);
        var value = new FixedRecords(new Tiny[]{new Tiny((byte) 0x11), new Tiny((byte) 0x22)}, 0x11223344);
        assertArrayEquals(hex("00010000 11220000 44332211"), support.serialize(value));
        byte[] cdr2 = hex("00070000 02000000 11220000 44332211");
        assertArrayEquals(cdr2, support.serializeCdr2(value));
        assertArrayEquals(value.values(), support.deserialize(cdr2).values());
        assertArrayEquals(value.values(), support.deserialize(hex("00060000 00000002 11220000 11223344")).values());
        assertThrows(IllegalArgumentException.class,
                () -> support.serialize(new FixedRecords(new Tiny[1], 0)));
    }

    @Test void stringCollectionsIncludeTheirCdr2DelimiterAndPreserveAlignment() {
        var sequence = CdrRecordTypeSupport.of(Strings.class);
        var fixed = CdrRecordTypeSupport.of(FixedStrings.class);
        String[] values = {"A", ""};
        byte[] seq1 = hex("00010003 02000000 02000000 41000000 01000000 00000000");
        byte[] seq2 = hex("00070003 11000000 02000000 02000000 41000000 01000000 00000000");
        byte[] fixed2 = hex("00070003 0d000000 02000000 41000000 01000000 00000000");
        assertArrayEquals(seq1, sequence.serialize(new Strings(values)));
        assertArrayEquals(seq2, sequence.serializeCdr2(new Strings(values)));
        assertArrayEquals(fixed2, fixed.serializeCdr2(new FixedStrings(values)));
        assertArrayEquals(values, sequence.deserialize(seq2).values());
        assertArrayEquals(values, fixed.deserialize(fixed2).values());
        assertThrows(NullPointerException.class, () -> sequence.serialize(new Strings(new String[]{null})));
        assertThrows(NullPointerException.class, () -> sequence.serialize(new Strings(null)));
    }

    @Test void primitiveSequencesHaveNoDelimiterAndAlignElementsByVersion() {
        var support = CdrRecordTypeSupport.of(Longs.class);
        var value = new Longs(new long[]{0x0102030405060708L}, (byte) 0x11);
        byte[] cdr1 = hex("00010003 01000000 00000000 0807060504030201 11000000");
        byte[] cdr2 = hex("00070003 01000000 0807060504030201 11000000");
        assertArrayEquals(cdr1, support.serialize(value));
        assertArrayEquals(cdr2, support.serializeCdr2(value));
        assertArrayEquals(value.values(), support.deserialize(cdr1).values());
        assertArrayEquals(value.values(), support.deserialize(cdr2).values());
        assertArrayEquals(value.values(), support.deserialize(
                hex("00060003 00000001 0102030405060708 11000000")).values());
        // Empty sequence must not align a nonexistent first long.
        assertArrayEquals(hex("00010003 00000000 11000000"),
                support.serialize(new Longs(new long[0], (byte) 0x11)));
        var fixed = CdrRecordTypeSupport.of(FixedInts.class);
        assertArrayEquals(hex("00010000 11000000 02000000 03000000"),
                fixed.serialize(new FixedInts((byte) 0x11, new int[]{2, 3})));
    }

    @Test void allPrimitiveArraysRoundTripInBothVersions() {
        var support = CdrRecordTypeSupport.of(AllArrays.class);
        var value = new AllArrays(new boolean[]{true, false}, new byte[]{-128, 127},
                new char[]{0, 255}, new short[]{Short.MIN_VALUE, Short.MAX_VALUE},
                new int[]{Integer.MIN_VALUE, Integer.MAX_VALUE}, new long[]{Long.MIN_VALUE, Long.MAX_VALUE},
                new float[]{-0.0f, Float.NaN}, new double[]{Double.POSITIVE_INFINITY, Math.PI});
        for (byte[] bytes : new byte[][]{support.serialize(value), support.serializeCdr2(value)}) {
            var result = support.deserialize(bytes);
            assertArrayEquals(value.booleans(), result.booleans());
            assertArrayEquals(value.bytes(), result.bytes());
            assertArrayEquals(value.chars(), result.chars());
            assertArrayEquals(value.shorts(), result.shorts());
            assertArrayEquals(value.ints(), result.ints());
            assertArrayEquals(value.longs(), result.longs());
            assertArrayEquals(value.floats(), result.floats());
            assertArrayEquals(value.doubles(), result.doubles());
        }
    }

    @Test void nestedDelimitersRestoreTheirParentBoundary() {
        var support = CdrRecordTypeSupport.of(Groups.class);
        var value = new Groups(new Group[]{new Group(new String[]{"a", "b"}), new Group(new String[]{"c"})});
        byte[] bytes = support.serializeCdr2(value);
        var result = support.deserialize(bytes);
        assertEquals(2, result.groups().length);
        assertArrayEquals(value.groups()[0].names(), result.groups()[0].names());
        assertArrayEquals(value.groups()[1].names(), result.groups()[1].names());
        for (int size = 0; size < bytes.length; size++) {
            byte[] truncated = Arrays.copyOf(bytes, size);
            assertThrows(IllegalArgumentException.class, () -> support.deserialize(truncated));
        }
    }

    @Test void rejectsRecursiveAndUnsupportedSchemasBeforeEncoding() {
        for (Class<? extends Record> type : Arrays.asList(Recursive.class, RecursiveArray.class,
                MutualA.class, Matrix.class, Boxed.class)) {
            assertThrows(IllegalArgumentException.class, () -> CdrRecordTypeSupport.of(type));
        }
        // A failed recursive schema must not poison a subsequent lookup.
        assertThrows(IllegalArgumentException.class, () -> CdrRecordTypeSupport.of(Recursive.class));
        assertNotNull(CdrRecordTypeSupport.of(Inner.class));
    }

    @Test void rejectsOversizedCountsBeforeAllocatingEvenForEmptyRecords() {
        var support = CdrRecordTypeSupport.of(Longs.class);
        for (String bad : new String[]{"00010000 ffffffff", "00010000 ffffff7f", "00010000 02000000"}) {
            assertThrows(IllegalArgumentException.class, () -> support.deserialize(hex(bad)));
        }
        assertThrows(IllegalArgumentException.class,
                () -> CdrRecordTypeSupport.of(HugeFixed.class).deserialize(hex("00010000")));
        var empty = CdrRecordTypeSupport.of(Empties.class);
        assertThrows(IllegalArgumentException.class, () -> empty.deserialize(hex("00010000 ffffff7f")));
        assertEquals(2, empty.deserialize(hex("00010000 02000000")).values().length);
    }

    @SuppressWarnings("deprecation")
    @Test void legacyServiceFramingUsesTheSharedCodecButIsNotPlainCdr2() {
        var support = CdrRecordTypeSupport.of(Inner.class);
        byte[] legacy = support.serializeDCdr2(new Inner(1, 2));
        assertArrayEquals(hex("83001000 01000000 00000000 0200000000000000"), legacy);
        assertThrows(IllegalArgumentException.class, () -> support.deserialize(legacy));
    }
}
