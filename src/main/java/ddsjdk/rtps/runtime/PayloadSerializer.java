package ddsjdk.rtps.runtime;

public interface PayloadSerializer<T> {
    byte[] serialize(T value);

    /** Decodes a payload to a non-null value; throw a RuntimeException for invalid input. */
    T deserialize(byte[] payload);
}
