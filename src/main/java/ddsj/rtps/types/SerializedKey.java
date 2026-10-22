package ddsj.rtps.types;

import java.util.Arrays;

/** Immutable value identity for serialized keys, including array and composite key fields. */
public final class SerializedKey {
    private final byte[] bytes;
    public SerializedKey(byte[] bytes) { this.bytes = bytes.clone(); }
    @Override public boolean equals(Object other) {
        return other instanceof SerializedKey key && Arrays.equals(bytes, key.bytes);
    }
    @Override public int hashCode() { return Arrays.hashCode(bytes); }
}
