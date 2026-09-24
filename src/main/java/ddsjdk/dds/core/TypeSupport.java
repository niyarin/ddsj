package ddsjdk.dds.core;

import ddsjdk.dds.exception.ReturnCode;
import ddsjdk.rtps.runtime.PayloadSerializer;

import java.util.Objects;

/**
 * Type support interface for DDS data types.
 * <p>
 * TypeSupport provides serialization, deserialization, and key extraction
 * for user-defined data types. Each Topic has an associated TypeSupport
 * that defines how data is serialized over the wire.
 *
 * @param <T> the data type
 */
public interface TypeSupport<T> {

    /**
     * Returns the type name used for discovery and matching.
     *
     * @return the type name
     */
    String getTypeName();

    /**
     * Returns the Java class for this type.
     *
     * @return the data class
     */
    Class<T> getType();

    /**
     * Serializes a data value to bytes.
     *
     * @param value the value to serialize
     * @return the serialized bytes
     */
    byte[] serialize(T value);

    /**
     * Deserializes bytes to a data value.
     *
     * @param data the serialized data
     * @return the deserialized value
     */
    T deserialize(byte[] data);

    /**
     * Returns true if this type has a key.
     * <p>
     * Keyed types support multiple instances where each unique key
     * identifies a distinct instance.
     *
     * @return true if keyed
     */
    default boolean hasKey() {
        return false;
    }

    /**
     * Extracts the key from a data value.
     * <p>
     * For keyed types, returns an object that can be used as a map key
     * (must implement equals and hashCode properly).
     *
     * @param value the data value
     * @return the key, or null if unkeyed
     */
    default Object extractKey(T value) {
        return null;
    }

    /**
     * Registers this type with the participant.
     *
     * @param participant the participant
     * @param typeName the type name to register
     * @return OK if successful
     */
    default ReturnCode registerType(DomainParticipant participant, String typeName) {
        // Default implementation does nothing; participant manages type registry
        return ReturnCode.OK;
    }

    /**
     * Creates a TypeSupport from a PayloadSerializer.
     * <p>
     * This is a convenience method for adapting RTPS serializers to DDS TypeSupport.
     *
     * @param type the data class
     * @param typeName the type name
     * @param serializer the RTPS serializer
     * @param <T> the data type
     * @return a TypeSupport wrapping the serializer
     */
    static <T> TypeSupport<T> fromSerializer(Class<T> type, String typeName, PayloadSerializer<T> serializer) {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(typeName, "typeName");
        Objects.requireNonNull(serializer, "serializer");
        return new TypeSupport<>() {
            @Override
            public String getTypeName() {
                return typeName;
            }

            @Override
            public Class<T> getType() {
                return type;
            }

            @Override
            public byte[] serialize(T value) {
                return serializer.serialize(value);
            }

            @Override
            public T deserialize(byte[] data) {
                return serializer.deserialize(data);
            }
        };
    }

    /**
     * Creates a keyed TypeSupport from a PayloadSerializer.
     *
     * @param type the data class
     * @param typeName the type name
     * @param serializer the RTPS serializer
     * @param keyExtractor function to extract the key from data
     * @param <T> the data type
     * @param <K> the key type
     * @return a keyed TypeSupport wrapping the serializer
     */
    static <T, K> TypeSupport<T> fromSerializer(Class<T> type, String typeName,
                                                 PayloadSerializer<T> serializer,
                                                 java.util.function.Function<T, K> keyExtractor) {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(typeName, "typeName");
        Objects.requireNonNull(serializer, "serializer");
        Objects.requireNonNull(keyExtractor, "keyExtractor");
        return new TypeSupport<>() {
            @Override
            public String getTypeName() {
                return typeName;
            }

            @Override
            public Class<T> getType() {
                return type;
            }

            @Override
            public byte[] serialize(T value) {
                return serializer.serialize(value);
            }

            @Override
            public T deserialize(byte[] data) {
                return serializer.deserialize(data);
            }

            @Override
            public boolean hasKey() {
                return true;
            }

            @Override
            public Object extractKey(T value) {
                return keyExtractor.apply(value);
            }
        };
    }
}
