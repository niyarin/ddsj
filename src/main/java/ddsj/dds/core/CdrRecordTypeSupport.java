package ddsj.dds.core;

import ddsj.cdr.CdrCodec;
import ddsj.cdr.CdrCodecs;
import ddsj.cdr.CdrEncoding;
import ddsj.cdr.CdrPayloadSerializer;
import ddsj.cdr.CdrReader;
import ddsj.cdr.CdrWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.lang.reflect.Array;
import ddsj.rtps.types.SerializedKey;

/** Derives FINAL struct codecs from records; all wire encoding is delegated to ddsj.cdr. */
public final class CdrRecordTypeSupport<T extends Record> implements TypeSupport<T> {
    private static final ClassValue<CdrRecordTypeSupport<?>> CACHE = new ClassValue<>() {
        @Override protected CdrRecordTypeSupport<?> computeValue(Class<?> type) {
            if (!type.isRecord()) throw new IllegalArgumentException("Not a record: " + type.getName());
            return new CdrRecordTypeSupport<>(type.asSubclass(Record.class));
        }
    };

    @SuppressWarnings("unchecked")
    public static <T extends Record> CdrRecordTypeSupport<T> of(Class<T> type) {
        return (CdrRecordTypeSupport<T>) CACHE.get(Objects.requireNonNull(type, "type"));
    }

    private final Class<T> type;
    private final CdrCodec<T> codec;
    private final CdrPayloadSerializer<T> serializer;
    private final CdrPayloadSerializer<T> cdr2Serializer;
    private final CdrPayloadSerializer<T> keySerializer;

    private CdrRecordTypeSupport(Class<T> type) {
        this.type = type;
        this.codec = new RecordCodec<>(type, new HashSet<>());
        this.serializer = new CdrPayloadSerializer<>(codec);
        this.cdr2Serializer = new CdrPayloadSerializer<>(codec, CdrEncoding.XCDR2);
        boolean keyed = java.util.Arrays.stream(type.getRecordComponents()).anyMatch(c -> c.isAnnotationPresent(CdrKey.class));
        this.keySerializer = keyed ? new CdrPayloadSerializer<>(new RecordCodec<>(type, new HashSet<>(), true)) : null;
    }

    /** Body codec, also usable inside a hand-written enclosing codec. */
    public CdrCodec<T> codec() { return codec; }

    @Override public String getTypeName() { return type.getName(); }
    @Override public Class<T> getType() { return type; }
    @Override public byte[] serialize(T value) { return serializer.serialize(value); }
    @Override public T deserialize(byte[] data) { return serializer.deserialize(data); }
    @Override public boolean hasKey() { return keySerializer != null; }
    @Override public boolean supportsKeySerialization() { return hasKey(); }
    @Override public byte[] serializeKey(T value) {
        if (!hasKey()) throw new IllegalStateException("unkeyed type");
        return keySerializer.serialize(value);
    }
    @Override public T deserializeKey(byte[] data) {
        if (!hasKey()) throw new IllegalStateException("unkeyed type");
        return keySerializer.deserialize(data);
    }
    @Override public Object extractKey(T value) {
        return hasKey() ? new SerializedKey(serializeKey(value)) : null;
    }

    /** Serializes using plain XCDR2 little-endian encoding. */
    public byte[] serializeCdr2(T value) { return cdr2Serializer.serialize(value); }

    /**
     * Preserves the historical DDSJ service framing (0x83, 0x00, 16-bit body size).
     * This is NOT standard DELIMITED_CDR and is not accepted by deserialize.
     * @deprecated Use an explicitly defined service serializer for custom framing.
     */
    @Deprecated
    public byte[] serializeDCdr2(T value) {
        CdrWriter out = new CdrWriter();
        codec.write(out, Objects.requireNonNull(value, "value"));
        byte[] body = out.bodyBytes();
        if (body.length > 65535) throw new IllegalArgumentException("Legacy service body exceeds 65535 bytes");
        byte[] payload = new byte[body.length + 4];
        payload[0] = (byte) 0x83;
        payload[2] = (byte) body.length;
        payload[3] = (byte) (body.length >>> 8);
        System.arraycopy(body, 0, payload, 4, body.length);
        return payload;
    }

    private static CdrCodec<?> componentCodec(Class<?> type, Set<Class<?>> visiting) {
        if (type == boolean.class) return CdrCodecs.BOOLEAN;
        if (type == byte.class) return CdrCodecs.BYTE;
        if (type == char.class) return CdrCodecs.CHAR;
        if (type == short.class) return CdrCodecs.SHORT;
        if (type == int.class) return CdrCodecs.INT;
        if (type == long.class) return CdrCodecs.LONG;
        if (type == float.class) return CdrCodecs.FLOAT;
        if (type == double.class) return CdrCodecs.DOUBLE;
        if (type == String.class) return CdrCodecs.STRING;
        if (type.isRecord()) return new RecordCodec<>(type, visiting);
        throw new IllegalArgumentException("Unsupported CDR component type: " + type.getTypeName());
    }

    private static final class RecordCodec<T> implements CdrCodec<T> {
        private final Constructor<T> constructor;
        private final Method[] accessors;
        private final CdrCodec<Object>[] members;
        private final int minimumSize;
        private final boolean[] included;
        private final Class<?>[] types;
        private final CdrFixedLength[] fixedLengths;

        @SuppressWarnings("unchecked")
        RecordCodec(Class<T> type, Set<Class<?>> visiting) {
            this(type, visiting, false);
        }

        @SuppressWarnings("unchecked")
        RecordCodec(Class<T> type, Set<Class<?>> visiting, boolean keyOnly) {
            if (!visiting.add(type)) throw new IllegalArgumentException("Recursive CDR record type: " + type.getName());
            try {
                RecordComponent[] components = type.getRecordComponents();
                types = new Class<?>[components.length];
                included = new boolean[components.length];
                fixedLengths = new CdrFixedLength[components.length];
                accessors = new Method[components.length];
                members = (CdrCodec<Object>[]) new CdrCodec<?>[components.length];
                long size = 0;
                for (int i = 0; i < components.length; i++) {
                    RecordComponent component = components[i];
                    types[i] = component.getType();
                    accessors[i] = component.getAccessor();
                    accessors[i].setAccessible(true);
                    CdrFixedLength fixed = component.getAnnotation(CdrFixedLength.class);
                    fixedLengths[i] = fixed;
                    included[i] = !keyOnly || component.isAnnotationPresent(CdrKey.class);
                    if (fixed != null && (!types[i].isArray() || fixed.value() <= 0)) {
                        throw new IllegalArgumentException("@CdrFixedLength requires an array and a positive length: " + component);
                    }
                    CdrCodec<?> member;
                    if (types[i].isArray()) {
                        CdrCodec<?> element = componentCodec(types[i].getComponentType(), visiting);
                        member = fixed == null ? CdrCodecs.sequence(types[i], element)
                                : CdrCodecs.fixedArray(types[i], element, fixed.value());
                    } else {
                        member = componentCodec(types[i], visiting);
                    }
                    members[i] = (CdrCodec<Object>) member;
                    if (included[i]) size = Math.min(Integer.MAX_VALUE, size + member.minimumSize());
                }
                minimumSize = (int) size;
                constructor = type.getDeclaredConstructor(types);
                constructor.setAccessible(true);
            } catch (ReflectiveOperationException | java.lang.reflect.InaccessibleObjectException | SecurityException e) {
                throw new IllegalArgumentException("Cannot inspect record: " + type.getName(), e);
            } finally {
                visiting.remove(type);
            }
        }

        @Override public int minimumSize() { return minimumSize; }

        @Override public void write(CdrWriter out, T value) {
            Objects.requireNonNull(value, "record");
            try {
                for (int i = 0; i < members.length; i++) {
                    if (included[i]) members[i].write(out, accessors[i].invoke(value));
                }
            } catch (ReflectiveOperationException e) {
                throw new IllegalArgumentException("Cannot read record components", e);
            }
        }

        @Override public T read(CdrReader in) {
            Object[] values = new Object[members.length];
            for (int i = 0; i < members.length; i++) {
                values[i] = included[i] ? members[i].read(in) : defaultValue(types[i], fixedLengths[i]);
            }
            try {
                return constructor.newInstance(values);
            } catch (ReflectiveOperationException e) {
                throw new IllegalArgumentException("Cannot construct record", e);
            }
        }
    }

    private static Object defaultValue(Class<?> type, CdrFixedLength fixed) {
        if (type == String.class) return "";
        if (type.isArray()) return Array.newInstance(type.getComponentType(), fixed == null ? 0 : fixed.value());
        if (type.isPrimitive()) return Array.get(Array.newInstance(type, 1), 0);
        if (type.isRecord()) {
            try {
                var components = type.getRecordComponents();
                Class<?>[] types = new Class<?>[components.length];
                Object[] values = new Object[components.length];
                for (int i = 0; i < components.length; i++) {
                    types[i] = components[i].getType();
                    values[i] = defaultValue(types[i], components[i].getAnnotation(CdrFixedLength.class));
                }
                var constructor = type.getDeclaredConstructor(types);
                constructor.setAccessible(true);
                return constructor.newInstance(values);
            } catch (ReflectiveOperationException e) {
                throw new IllegalArgumentException("Cannot construct key holder", e);
            }
        }
        throw new IllegalArgumentException("Unsupported key holder component: " + type);
    }
}
