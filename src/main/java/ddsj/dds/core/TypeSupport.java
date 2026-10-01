package ddsj.dds.core;

import ddsj.rtps.runtime.PayloadSerializer;

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
     * Creates cached, unkeyed plain CDR (XCDR1) support for a record.
     * Components are encoded in declaration order with CDR alignment. Serialization
     * includes a CDR_LE encapsulation header; deserialization accepts plain XCDR1
     * and XCDR2 in either byte order. Records are treated as FINAL structs.
     * Primitives map to their IDL counterparts: byte to octet, short to short,
     * int to long, long to long long, and char to an 8-bit IDL char (0..255).
     * Strings use UTF-8 with a length including the terminating NUL. Nested records
     * and one-dimensional arrays of supported types are accepted. Arrays are sequences
     * unless annotated with {@link CdrFixedLength}. Null values, embedded NULs,
     * invalid Unicode, boxed primitives, and recursive record schemas are rejected.
     * The discovery type name is the record's binary class name. Both endpoints must
     * use the same schema. Keys and schema evolution are not supported.
     * Decoding limits total array elements to {@link ddsj.cdr.CdrReader#DEFAULT_MAX_ARRAY_ELEMENTS};
     * use {@link ddsj.cdr.CdrPayloadSerializer} with {@link CdrRecordTypeSupport#codec()}
     * to configure another limit.
     * Use with {@code participant.createTopic(name, MyRecord.class, TypeSupport.forCdrRecord(MyRecord.class))}.
     */
    static <T extends Record> TypeSupport<T> forCdrRecord(Class<T> type) {
        return CdrRecordTypeSupport.of(type);
    }

    /**
     * Creates unkeyed CDR record support with an explicit DDS discovery type name.
     * The name is used verbatim; ROS message names are not converted to DDS names.
     * Encoding and supported components are the same as {@link #forCdrRecord(Class)}.
     * Record metadata is shared with the cached default support.
     *
     * @param type the record class
     * @param typeName the exact type name advertised in DDS discovery
     */
    static <T extends Record> TypeSupport<T> forCdrRecord(Class<T> type, String typeName) {
        Objects.requireNonNull(typeName, "typeName");
        TypeSupport<T> codec = forCdrRecord(type);
        return new TypeSupport<>() {
            @Override public String getTypeName() { return typeName; }
            @Override public Class<T> getType() { return codec.getType(); }
            @Override public byte[] serialize(T value) { return codec.serialize(value); }
            @Override public T deserialize(byte[] data) { return codec.deserialize(data); }
        };
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
