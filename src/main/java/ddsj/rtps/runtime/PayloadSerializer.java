package ddsj.rtps.runtime;

public interface PayloadSerializer<T> {
    byte[] serialize(T value);

    /** Decodes a payload to a non-null value; throw a RuntimeException for invalid input. */
    T deserialize(byte[] payload);

    default boolean hasKey() { return false; }

    /** Keys must have stable value equality; null is reserved for unkeyed types. */
    default Object extractKey(T value) { return null; }

    /** True when key-only CDR encoding is available for lifecycle notifications. */
    default boolean supportsKeySerialization() { return false; }

    default byte[] serializeKey(T value) { throw new UnsupportedOperationException("key serialization"); }

    default T deserializeKey(byte[] payload) { throw new UnsupportedOperationException("key deserialization"); }
}
