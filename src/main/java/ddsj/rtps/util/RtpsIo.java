package ddsj.rtps.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class RtpsIo {
    private RtpsIo() {
    }

    public static byte[] shortLe(int value) {
        return ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN).putShort((short) value).array();
    }

    public static byte[] intLe(int value) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array();
    }

    public static byte[] longLe(long value) {
        return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array();
    }

    public static int readUShort(byte[] bytes, int offset, boolean littleEndian) {
        int first = bytes[offset] & 0xff;
        int second = bytes[offset + 1] & 0xff;
        return littleEndian ? first | (second << 8) : (first << 8) | second;
    }

    public static int readInt(byte[] bytes, int offset, boolean littleEndian) {
        int b0 = bytes[offset] & 0xff;
        int b1 = bytes[offset + 1] & 0xff;
        int b2 = bytes[offset + 2] & 0xff;
        int b3 = bytes[offset + 3] & 0xff;
        if (littleEndian) {
            return b0 | (b1 << 8) | (b2 << 16) | (b3 << 24);
        }
        return (b0 << 24) | (b1 << 16) | (b2 << 8) | b3;
    }


    public static long readSequenceNumber(byte[] bytes, int offset, boolean littleEndian) {
        long high = readInt(bytes, offset, littleEndian);
        long low = readInt(bytes, offset + 4, littleEndian) & 0xffff_ffffL;
        return (high << 32) | low;
    }

    public static void writeInt(byte[] bytes, int offset, int value, boolean littleEndian) {
        if (littleEndian) {
            bytes[offset] = (byte) value;
            bytes[offset + 1] = (byte) (value >>> 8);
            bytes[offset + 2] = (byte) (value >>> 16);
            bytes[offset + 3] = (byte) (value >>> 24);
        } else {
            bytes[offset] = (byte) (value >>> 24);
            bytes[offset + 1] = (byte) (value >>> 16);
            bytes[offset + 2] = (byte) (value >>> 8);
            bytes[offset + 3] = (byte) value;
        }
    }
}
