package ddsj.rtps.types;

import java.util.Arrays;

public final class EntityId {
    public static final int SIZE = 4;

    private final byte[] bytes;

    public EntityId(byte[] bytes) {
        if (bytes.length != SIZE) {
            throw new IllegalArgumentException("RTPS entityId must be " + SIZE + " bytes");
        }
        this.bytes = bytes.clone();
    }

    public byte[] bytes() {
        return bytes.clone();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof EntityId that && Arrays.equals(bytes, that.bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }
}
