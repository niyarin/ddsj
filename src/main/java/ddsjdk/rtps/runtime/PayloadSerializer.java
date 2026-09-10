package ddsjdk.rtps.runtime;

public interface PayloadSerializer<T> {
    byte[] serialize(T value);

    T deserialize(byte[] payload);
}
