package ddsjdk.dds.core;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Internal codec: declaration order, big endian primitives, length-prefixed UTF-8. */
final class RecordTypeSupport<T extends Record> implements TypeSupport<T> {
    private static final ClassValue<RecordTypeSupport<?>> CACHE = new ClassValue<>() {
        @Override
        protected RecordTypeSupport<?> computeValue(Class<?> type) {
            if (!type.isRecord()) throw new IllegalArgumentException("Not a record: " + type.getName());
            return new RecordTypeSupport<>(type.asSubclass(Record.class));
        }
    };

    @SuppressWarnings("unchecked")
    static <T extends Record> TypeSupport<T> of(Class<T> type) {
        return (TypeSupport<T>) CACHE.get(Objects.requireNonNull(type, "type"));
    }

    private final Class<T> type;
    private final Constructor<T> constructor;
    private final Class<?>[] types;
    private final Method[] accessors;

    private RecordTypeSupport(Class<T> type) {
        this.type = type;
        RecordComponent[] components = type.getRecordComponents();
        types = new Class<?>[components.length];
        accessors = new Method[components.length];
        try {
            for (int i = 0; i < components.length; i++) {
                types[i] = components[i].getType();
                if (!types[i].isPrimitive() && types[i] != String.class) {
                    throw new IllegalArgumentException("Unsupported record component: "
                            + type.getName() + "." + components[i].getName());
                }
                accessors[i] = components[i].getAccessor();
                if (!accessors[i].trySetAccessible()) {
                    throw new IllegalArgumentException("Inaccessible record component: " + components[i]);
                }
            }
            constructor = type.getDeclaredConstructor(types);
            if (!constructor.trySetAccessible()) {
                throw new IllegalArgumentException("Inaccessible record constructor: " + type.getName());
            }
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException("Cannot inspect record: " + type.getName(), e);
        }
    }

    @Override public String getTypeName() { return type.getName(); }
    @Override public Class<T> getType() { return type; }

    @Override
    public byte[] serialize(T value) {
        Objects.requireNonNull(value, "value");
        try {
            var bytes = new ByteArrayOutputStream();
            var out = new DataOutputStream(bytes);
            for (int i = 0; i < types.length; i++) {
                Object v = accessors[i].invoke(value);
                Class<?> t = types[i];
                if (t == boolean.class) out.writeBoolean((boolean) v);
                else if (t == byte.class) out.writeByte((byte) v);
                else if (t == short.class) out.writeShort((short) v);
                else if (t == char.class) out.writeChar((char) v);
                else if (t == int.class) out.writeInt((int) v);
                else if (t == long.class) out.writeLong((long) v);
                else if (t == float.class) out.writeFloat((float) v);
                else if (t == double.class) out.writeDouble((double) v);
                else {
                    Objects.requireNonNull(v, "Null String component: " + accessors[i].getName());
                    ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                            .onMalformedInput(CodingErrorAction.REPORT).encode(java.nio.CharBuffer.wrap((String) v));
                    byte[] text = new byte[encoded.remaining()];
                    encoded.get(text);
                    out.writeInt(text.length);
                    out.write(text);
                }
            }
            return bytes.toByteArray();
        } catch (IOException | ReflectiveOperationException e) {
            throw new IllegalArgumentException("Cannot serialize " + type.getName(), e);
        }
    }

    @Override
    public T deserialize(byte[] data) {
        Objects.requireNonNull(data, "data");
        try {
            var in = new DataInputStream(new ByteArrayInputStream(data));
            Object[] values = new Object[types.length];
            for (int i = 0; i < types.length; i++) {
                Class<?> t = types[i];
                if (t == boolean.class) {
                    int b = in.readUnsignedByte();
                    if (b > 1) throw new IOException("Invalid boolean");
                    values[i] = b == 1;
                } else if (t == byte.class) values[i] = in.readByte();
                else if (t == short.class) values[i] = in.readShort();
                else if (t == char.class) values[i] = in.readChar();
                else if (t == int.class) values[i] = in.readInt();
                else if (t == long.class) values[i] = in.readLong();
                else if (t == float.class) values[i] = in.readFloat();
                else if (t == double.class) values[i] = in.readDouble();
                else {
                    int length = in.readInt();
                    if (length < 0 || length > in.available()) throw new IOException("Invalid string length");
                    values[i] = decode(in.readNBytes(length));
                }
            }
            if (in.available() != 0) throw new IOException("Trailing payload bytes");
            return constructor.newInstance(values);
        } catch (IOException | ReflectiveOperationException e) {
            throw new IllegalArgumentException("Cannot deserialize " + type.getName(), e);
        }
    }

    private static String decode(byte[] bytes) throws CharacterCodingException {
        return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(bytes)).toString();
    }
}
