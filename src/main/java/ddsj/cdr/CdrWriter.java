package ddsj.cdr;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Little-endian CDR writer. Positions and alignment are relative to the body. */
public final class CdrWriter {
    private static final class Buffer extends ByteArrayOutputStream {
        void patchInt(int position, int value) {
            for (int i = 0; i < 4; i++) buf[position + i] = (byte) (value >>> (8 * i));
        }
    }

    private final Buffer body = new Buffer();
    private final CdrEncoding encoding;

    public CdrWriter() { this(CdrEncoding.XCDR1); }

    public CdrWriter(CdrEncoding encoding) {
        this.encoding = Objects.requireNonNull(encoding, "encoding");
    }

    public CdrEncoding encoding() { return encoding; }

    private void number(long value, int width) {
        int alignment = Math.min(width, encoding.maxAlignment);
        while (body.size() % alignment != 0) body.write(0);
        for (int i = 0; i < width; i++) body.write((int) (value >>> (8 * i)));
    }

    public void writeBoolean(boolean value) { number(value ? 1 : 0, 1); }
    public void writeByte(byte value) { number(value, 1); }
    public void writeChar(char value) {
        if (value > 255) throw new IllegalArgumentException("CDR char must fit in 8 bits");
        number(value, 1);
    }
    public void writeShort(short value) { number(value, 2); }
    public void writeInt(int value) { number(value, 4); }
    public void writeLong(long value) { number(value, 8); }
    public void writeFloat(float value) { number(Float.floatToRawIntBits(value), 4); }
    public void writeDouble(double value) { number(Double.doubleToRawLongBits(value), 8); }

    public void writeString(String value) {
        Objects.requireNonNull(value, "value");
        if (value.indexOf('\0') >= 0) throw new IllegalArgumentException("CDR strings cannot contain NUL");
        try {
            ByteBuffer bytes = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT).encode(CharBuffer.wrap(value));
            writeInt(Math.addExact(bytes.remaining(), 1));
            while (bytes.hasRemaining()) body.write(bytes.get());
            body.write(0);
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("Invalid Unicode string", e);
        }
    }

    int beginDelimited() {
        writeInt(0);
        return body.size();
    }

    void endDelimited(int start) { body.patchInt(start - 4, body.size() - start); }

    /** Returns the body without a header or final padding, primarily for custom framing. */
    public byte[] bodyBytes() { return body.toByteArray(); }

    /** Returns a complete encapsulated payload, including its final padding count. */
    public byte[] toByteArray() {
        int padding = (-body.size()) & 3;
        byte[] result = new byte[Math.addExact(body.size(), 4 + padding)];
        result[1] = (byte) encoding.littleEndianId;
        result[3] = (byte) padding;
        System.arraycopy(body.toByteArray(), 0, result, 4, body.size());
        return result;
    }
}
