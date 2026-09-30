package ddsj.dds.core;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Plain XCDR1/XCDR2 record codec. Alignment is relative to the end of the encapsulation header. */
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
    private final Constructor<T> constructor;
    private final Class<?>[] types;
    private final Method[] accessors;
    private final int[] fixedLengths; // -1 for variable length, >= 0 for fixed

    private CdrRecordTypeSupport(Class<T> type) {
        this.type = type;
        RecordComponent[] components = type.getRecordComponents();
        types = new Class<?>[components.length];
        accessors = new Method[components.length];
        fixedLengths = new int[components.length];
        try {
            for (int i = 0; i < components.length; i++) {
                types[i] = components[i].getType();
                if (!types[i].isPrimitive() && types[i] != String.class && types[i] != byte[].class) {
                    throw new IllegalArgumentException("Unsupported CDR component: " + components[i]);
                }
                accessors[i] = components[i].getAccessor();
                try {
                    accessors[i].setAccessible(true);
                } catch (RuntimeException e) {
                    throw new IllegalArgumentException("Inaccessible record component: " + components[i], e);
                }
                // Check for @CdrFixedLength annotation
                CdrFixedLength fixedLen = components[i].getAnnotation(CdrFixedLength.class);
                if (fixedLen != null) {
                    if (types[i] != byte[].class) {
                        throw new IllegalArgumentException("@CdrFixedLength only applies to byte[]: " + components[i]);
                    }
                    if (fixedLen.value() <= 0) {
                        throw new IllegalArgumentException("@CdrFixedLength must be positive: " + components[i]);
                    }
                    fixedLengths[i] = fixedLen.value();
                } else {
                    fixedLengths[i] = -1;
                }
            }
            constructor = type.getDeclaredConstructor(types);
            try {
                constructor.setAccessible(true);
            } catch (RuntimeException e) {
                throw new IllegalArgumentException("Inaccessible record constructor: " + type.getName(), e);
            }
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException("Cannot inspect record: " + type.getName(), e);
        }
    }

    @Override public String getTypeName() { return type.getName(); }
    @Override public Class<T> getType() { return type; }

    /** Serialize using CDR (XCDR1) little-endian encapsulation. */
    @Override public byte[] serialize(T value) {
        return serializeWithEncapsulation(value, (byte) 0x01); // CDR_LE
    }

    /** Serialize using CDR2 (XCDR2) little-endian encapsulation. */
    public byte[] serializeCdr2(T value) {
        return serializeWithEncapsulation(value, (byte) 0x07); // CDR2_LE
    }

    /** Serialize using D_CDR2 (Delimited CDR2) for ROS2 services. */
    public byte[] serializeDCdr2(T value) {
        // D_CDR2 format: [0x83][0x00][options 2 bytes][serialized data]
        Objects.requireNonNull(value, "value");
        try {
            var out = new ByteArrayOutputStream();
            for (int i = 0; i < types.length; i++) {
                Class<?> t = types[i];
                Object v = accessors[i].invoke(value);
                int width = alignment(t, i);
                while (out.size() % width != 0) out.write(0);
                if (t == String.class) {
                    String text = (String) Objects.requireNonNull(v, "Null String component");
                    ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                            .onMalformedInput(CodingErrorAction.REPORT).encode(CharBuffer.wrap(text));
                    writeNumber(out, (long) encoded.remaining() + 1, 4);
                    while (encoded.hasRemaining()) out.write(encoded.get());
                    out.write(0);
                } else if (t == byte[].class) {
                    byte[] bytes = (byte[]) Objects.requireNonNull(v, "Null byte[]");
                    int fixedLen = fixedLengths[i];
                    if (fixedLen >= 0) {
                        out.writeBytes(bytes);
                    } else {
                        writeNumber(out, bytes.length, 4);
                        out.writeBytes(bytes);
                    }
                } else {
                    long bits;
                    if (t == boolean.class) bits = (boolean) v ? 1 : 0;
                    else if (t == char.class) bits = (char) v;
                    else if (t == float.class) bits = Float.floatToRawIntBits((float) v);
                    else if (t == double.class) bits = Double.doubleToRawLongBits((double) v);
                    else bits = ((Number) v).longValue();
                    writeNumber(out, bits, width);
                }
            }
            byte[] serialized = out.toByteArray();
            var payload = new ByteArrayOutputStream();
            // D_CDR2 header: 0x83, 0x00, size as options (little-endian)
            payload.write(0x83);
            payload.write(0x00);
            payload.write(serialized.length & 0xFF);
            payload.write((serialized.length >> 8) & 0xFF);
            payload.writeBytes(serialized);
            return payload.toByteArray();
        } catch (ReflectiveOperationException | CharacterCodingException e) {
            throw new IllegalArgumentException("Cannot serialize " + type.getName(), e);
        }
    }

    private byte[] serializeWithEncapsulation(T value, byte encapsulationId) {
        Objects.requireNonNull(value, "value");
        try {
            var out = new ByteArrayOutputStream();
            for (int i = 0; i < types.length; i++) {
                Class<?> t = types[i];
                Object v = accessors[i].invoke(value);
                int width = alignment(t, i);
                while (out.size() % width != 0) out.write(0);
                if (t == String.class) {
                    String text = (String) Objects.requireNonNull(v, "Null String component: " + accessors[i].getName());
                    if (text.indexOf('\0') >= 0) throw new IllegalArgumentException("CDR strings cannot contain NUL");
                    ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                            .onMalformedInput(CodingErrorAction.REPORT).encode(CharBuffer.wrap(text));
                    writeNumber(out, (long) encoded.remaining() + 1, 4);
                    while (encoded.hasRemaining()) out.write(encoded.get());
                    out.write(0);
                } else if (t == byte[].class) {
                    byte[] bytes = (byte[]) Objects.requireNonNull(v, "Null byte[] component: " + accessors[i].getName());
                    int fixedLen = fixedLengths[i];
                    if (fixedLen >= 0) {
                        // Fixed length array - no length prefix
                        if (bytes.length != fixedLen) {
                            throw new IllegalArgumentException("Expected byte[" + fixedLen + "] but got byte[" + bytes.length + "]");
                        }
                        out.writeBytes(bytes);
                    } else {
                        // Variable length sequence - 4 byte length prefix
                        writeNumber(out, bytes.length, 4);
                        out.writeBytes(bytes);
                    }
                } else {
                    long bits;
                    if (t == boolean.class) bits = (boolean) v ? 1 : 0;
                    else if (t == char.class) {
                        bits = (char) v;
                        if (bits > 255) throw new IllegalArgumentException("CDR char must fit in 8 bits");
                    } else if (t == float.class) bits = Float.floatToRawIntBits((float) v);
                    else if (t == double.class) bits = Double.doubleToRawLongBits((double) v);
                    else bits = ((Number) v).longValue();
                    writeNumber(out, bits, width);
                }
            }
            int padding = (-out.size()) & 3;
            var payload = new ByteArrayOutputStream();
            payload.writeBytes(new byte[]{0, encapsulationId, 0, (byte) padding});
            payload.writeBytes(out.toByteArray());
            for (int i = 0; i < padding; i++) payload.write(0);
            return payload.toByteArray();
        } catch (ReflectiveOperationException | CharacterCodingException e) {
            throw new IllegalArgumentException("Cannot serialize " + type.getName(), e);
        }
    }

    /** Deserialize from CDR (XCDR1) or CDR2 (XCDR2) encapsulation. */
    @Override public T deserialize(byte[] data) {
        Objects.requireNonNull(data, "data");
        try {
            int dataStart = 4; // Standard CDR header is 4 bytes
            boolean littleEndian;
            int padding = 0;

            if (data.length < 4 || data[0] != 0) {
                throw new IllegalArgumentException("Invalid CDR encapsulation header");
            } else {
                // Standard CDR format: [0x00][encapId][options (2 bytes)]
                int encapId = Byte.toUnsignedInt(data[1]);
                littleEndian = switch (encapId) {
                    case 0x00 -> false; // CDR_BE (XCDR1)
                    case 0x01 -> true;  // CDR_LE (XCDR1)
                    case 0x06 -> false; // CDR2_BE (XCDR2)
                    case 0x07 -> true;  // CDR2_LE (XCDR2)
                    default -> throw new IllegalArgumentException(
                            "Unsupported CDR encapsulation: 0x" + Integer.toHexString(encapId));
                };
                if (data[2] != 0 || (data[3] & 0xfc) != 0) {
                    throw new IllegalArgumentException("Unsupported CDR encapsulation options");
                }
                padding = data[3] & 3;
            }
            if (padding > data.length - dataStart || (padding != 0 && data.length % 4 != 0)) {
                throw new IllegalArgumentException("Invalid CDR padding");
            }
            for (int i = data.length - padding; i < data.length; i++) {
                if (data[i] != 0) throw new IllegalArgumentException("Invalid CDR padding");
            }
            var in = ByteBuffer.wrap(data, dataStart, data.length - dataStart - padding).slice()
                    .order(littleEndian ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);
            Object[] values = new Object[types.length];
            for (int i = 0; i < types.length; i++) {
                Class<?> t = types[i];
                int skip = (-in.position()) & (alignment(t, i) - 1);
                if (skip > in.remaining()) throw new IllegalArgumentException("Truncated CDR alignment");
                in.position(in.position() + skip);
                if (t == boolean.class) {
                    byte b = in.get();
                    if (b != 0 && b != 1) throw new IllegalArgumentException("Invalid CDR boolean");
                    values[i] = b == 1;
                } else if (t == byte.class) values[i] = in.get();
                else if (t == char.class) values[i] = (char) Byte.toUnsignedInt(in.get());
                else if (t == short.class) values[i] = in.getShort();
                else if (t == int.class) values[i] = in.getInt();
                else if (t == long.class) values[i] = in.getLong();
                else if (t == float.class) values[i] = in.getFloat();
                else if (t == double.class) values[i] = in.getDouble();
                else if (t == byte[].class) {
                    int fixedLen = fixedLengths[i];
                    int length;
                    if (fixedLen >= 0) {
                        // Fixed length array - no length prefix
                        length = fixedLen;
                    } else {
                        // Variable length sequence - 4 byte length prefix
                        length = in.getInt();
                        if (length < 0 || length > in.remaining()) {
                            throw new IllegalArgumentException("Invalid CDR sequence length");
                        }
                    }
                    byte[] bytes = new byte[length];
                    in.get(bytes);
                    values[i] = bytes;
                } else {
                    // String
                    int length = in.getInt();
                    if (length < 1 || length > in.remaining()) throw new IllegalArgumentException("Invalid CDR string length");
                    int end = in.position() + length - 1;
                    if (in.get(end) != 0) throw new IllegalArgumentException("Missing CDR string terminator");
                    ByteBuffer text = in.slice();
                    text.limit(length - 1);
                    String decoded = StandardCharsets.UTF_8.newDecoder()
                            .onMalformedInput(CodingErrorAction.REPORT).decode(text).toString();
                    if (decoded.indexOf('\0') >= 0) throw new IllegalArgumentException("CDR strings cannot contain NUL");
                    values[i] = decoded;
                    in.position(end + 1);
                }
            }
            // Older RTPS senders may append alignment bytes without setting the options count.
            if (in.hasRemaining()) {
                if (padding != 0 || in.remaining() > 3 || data.length % 4 != 0) {
                    throw new IllegalArgumentException("Trailing CDR payload bytes");
                }
                while (in.hasRemaining()) {
                    if (in.get() != 0) throw new IllegalArgumentException("Invalid CDR padding");
                }
            }
            return constructor.newInstance(values);
        } catch (ReflectiveOperationException | CharacterCodingException | java.nio.BufferUnderflowException e) {
            throw new IllegalArgumentException("Cannot deserialize " + type.getName(), e);
        }
    }

    private int alignment(Class<?> type, int componentIndex) {
        if (type == boolean.class || type == byte.class || type == char.class) return 1;
        if (type == short.class) return 2;
        if (type == long.class || type == double.class) return 8;
        if (type == byte[].class) {
            // Fixed length has no prefix (1-byte alignment), variable has 4-byte length prefix
            return fixedLengths[componentIndex] >= 0 ? 1 : 4;
        }
        return 4;
    }

    private static void writeNumber(ByteArrayOutputStream out, long bits, int width) {
        for (int i = 0; i < width; i++) out.write((int) (bits >>> (i * 8)));
    }
}
