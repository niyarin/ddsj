package ddsj.cdr;

import java.lang.reflect.Array;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;

/** Reusable codecs for IDL scalars, one-dimensional arrays and sequences. */
public final class CdrCodecs {
    private CdrCodecs() {}

    public static final CdrCodec<Boolean> BOOLEAN = scalar(1, CdrWriter::writeBoolean, CdrReader::readBoolean);
    public static final CdrCodec<Byte> BYTE = scalar(1, CdrWriter::writeByte, CdrReader::readByte);
    public static final CdrCodec<Character> CHAR = scalar(1, CdrWriter::writeChar, CdrReader::readChar);
    public static final CdrCodec<Short> SHORT = scalar(2, CdrWriter::writeShort, CdrReader::readShort);
    public static final CdrCodec<Integer> INT = scalar(4, CdrWriter::writeInt, CdrReader::readInt);
    public static final CdrCodec<Long> LONG = scalar(8, CdrWriter::writeLong, CdrReader::readLong);
    public static final CdrCodec<Float> FLOAT = scalar(4, CdrWriter::writeFloat, CdrReader::readFloat);
    public static final CdrCodec<Double> DOUBLE = scalar(8, CdrWriter::writeDouble, CdrReader::readDouble);
    public static final CdrCodec<String> STRING = new CdrCodec<String>() {
        @Override public void write(CdrWriter out, String value) { out.writeString(value); }
        @Override public String read(CdrReader in) { return in.readString(); }
        @Override public int minimumSize() { return 5; }
    };

    private static <T> CdrCodec<T> scalar(int size, BiConsumer<CdrWriter, T> write, Function<CdrReader, T> read) {
        return new CdrCodec<T>() {
            @Override public void write(CdrWriter out, T value) { write.accept(out, Objects.requireNonNull(value, "value")); }
            @Override public T read(CdrReader in) { return read.apply(in); }
            @Override public int minimumSize() { return size; }
            @Override public boolean isPrimitive() { return true; }
        };
    }

    /**
     * Creates a length-prefixed sequence codec. The element codec must match the
     * array component type (boxed for primitive arrays), e.g. int[].class and INT.
     */
    public static <A> CdrCodec<A> sequence(Class<A> arrayType, CdrCodec<?> elementCodec) {
        return array(arrayType, elementCodec, -1);
    }

    /** Creates a fixed-length IDL array codec without an element-count prefix. */
    public static <A> CdrCodec<A> fixedArray(Class<A> arrayType, CdrCodec<?> elementCodec, int length) {
        if (length <= 0) throw new IllegalArgumentException("Fixed array length must be positive");
        return array(arrayType, elementCodec, length);
    }

    @SuppressWarnings("unchecked")
    private static <A> CdrCodec<A> array(Class<A> arrayType, CdrCodec<?> elementCodec, int fixedLength) {
        Objects.requireNonNull(arrayType, "arrayType");
        Objects.requireNonNull(elementCodec, "elementCodec");
        if (!arrayType.isArray() || arrayType.getComponentType().isArray()) {
            throw new IllegalArgumentException("Expected a one-dimensional array: " + arrayType.getName());
        }
        if (elementCodec.minimumSize() < 0) throw new IllegalArgumentException("Negative minimum element size");
        CdrCodec<Object> element = (CdrCodec<Object>) elementCodec;
        return new CdrCodec<A>() {
            @Override public void write(CdrWriter out, A value) {
                arrayType.cast(Objects.requireNonNull(value, "array"));
                int count = Array.getLength(value);
                if (fixedLength >= 0 && count != fixedLength) {
                    throw new IllegalArgumentException("Expected array length " + fixedLength + " but got " + count);
                }
                boolean delimited = out.encoding() == CdrEncoding.XCDR2 && !element.isPrimitive();
                int start = delimited ? out.beginDelimited() : 0;
                if (fixedLength < 0) out.writeInt(count);
                for (int i = 0; i < count; i++) {
                    element.write(out, Objects.requireNonNull(Array.get(value, i), "Null array element"));
                }
                if (delimited) out.endDelimited(start);
            }

            @Override public A read(CdrReader in) {
                boolean delimited = in.encoding() == CdrEncoding.XCDR2 && !element.isPrimitive();
                int oldLimit = delimited ? in.beginDelimited() : 0;
                int count = fixedLength < 0 ? in.readInt() : fixedLength;
                in.claimElements(count, element.minimumSize());
                A result = arrayType.cast(Array.newInstance(arrayType.getComponentType(), count));
                for (int i = 0; i < count; i++) Array.set(result, i, Objects.requireNonNull(element.read(in), "Null array element"));
                if (delimited) in.endDelimited(oldLimit);
                return result;
            }

            @Override public int minimumSize() {
                return fixedLength < 0 ? 4 : (int) Math.min(Integer.MAX_VALUE, (long) fixedLength * element.minimumSize());
            }
        };
    }
}
