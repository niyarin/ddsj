package ddsj.rtps.types;

import java.util.Arrays;

public final class GuidPrefix {
    public static final int SIZE = 12;

    private final byte[] bytes;

    public GuidPrefix(byte[] bytes) {
        if (bytes.length != SIZE) {
            throw new IllegalArgumentException("RTPS guidPrefix must be " + SIZE + " bytes");
        }
        this.bytes = bytes.clone();
    }

    public byte[] bytes() {
        return bytes.clone();
    }

    public Guid toGuid(EntityId entityId) {
        return new Guid(this, entityId);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof GuidPrefix that && Arrays.equals(bytes, that.bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }
}
