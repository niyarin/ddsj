package ddsj.cdr;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** CDR reader with one alignment origin shared by all nested codecs. */
public final class CdrReader {
    /** Total array elements that may be allocated while reading one payload. */
    public static final int DEFAULT_MAX_ARRAY_ELEMENTS = 1_000_000;

    private final ByteBuffer body;
    private final CdrEncoding encoding;
    private final int padding;
    private final int payloadLength;
    private int elementsLeft;

    public CdrReader(byte[] payload) { this(payload, DEFAULT_MAX_ARRAY_ELEMENTS); }

    public CdrReader(byte[] payload, int maxArrayElements) {
        Objects.requireNonNull(payload, "payload");
        if (maxArrayElements < 0) throw new IllegalArgumentException("Negative array element limit");
        elementsLeft = maxArrayElements;
        payloadLength = payload.length;
        if (payload.length < 4 || payload[0] != 0) {
            throw new IllegalArgumentException("Invalid CDR encapsulation header");
        }
        int id = payload[1] & 255;
        if (id == 0 || id == 1) encoding = CdrEncoding.XCDR1;
        else if (id == 6 || id == 7) encoding = CdrEncoding.XCDR2;
        else throw new IllegalArgumentException("Unsupported CDR encapsulation: " + id);
        if (payload[2] != 0 || (payload[3] & 0xfc) != 0) {
            throw new IllegalArgumentException("Unsupported CDR encapsulation options");
        }
        padding = payload[3] & 3;
        if (padding > payload.length - 4 || (padding != 0 && payload.length % 4 != 0)) {
            throw new IllegalArgumentException("Invalid CDR padding");
        }
        for (int i = payload.length - padding; i < payload.length; i++) {
            if (payload[i] != 0) throw new IllegalArgumentException("Invalid CDR padding");
        }
        body = ByteBuffer.wrap(payload, 4, payload.length - 4 - padding).slice()
                .order((id & 1) != 0 ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);
    }

    public CdrEncoding encoding() { return encoding; }

    private void require(int count) {
        if (count < 0 || count > body.remaining()) throw new IllegalArgumentException("Truncated CDR payload");
    }

    private void align(int width) {
        int skip = (-body.position()) & (Math.min(width, encoding.maxAlignment) - 1);
        require(skip + width);
        body.position(body.position() + skip);
    }

    public boolean readBoolean() {
        byte value = readByte();
        if (value != 0 && value != 1) throw new IllegalArgumentException("Invalid CDR boolean");
        return value == 1;
    }
    public byte readByte() { align(1); return body.get(); }
    public char readChar() { return (char) (readByte() & 255); }
    public short readShort() { align(2); return body.getShort(); }
    public int readInt() { align(4); return body.getInt(); }
    public long readLong() { align(8); return body.getLong(); }
    public float readFloat() { return Float.intBitsToFloat(readInt()); }
    public double readDouble() { return Double.longBitsToDouble(readLong()); }

    public String readString() {
        int length = readInt();
        if (length < 1) throw new IllegalArgumentException("Invalid CDR string length");
        require(length);
        int end = body.position() + length - 1;
        if (body.get(end) != 0) throw new IllegalArgumentException("Missing CDR string terminator");
        ByteBuffer text = body.slice();
        text.limit(length - 1);
        try {
            String value = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .decode(text).toString();
            if (value.indexOf('\0') >= 0) throw new IllegalArgumentException("CDR strings cannot contain NUL");
            body.position(end + 1);
            return value;
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("Invalid CDR UTF-8", e);
        }
    }

    void claimElements(int count, int minimumSize) {
        if (count < 0 || count > elementsLeft || minimumSize < 0
                || (minimumSize != 0 && count > body.remaining() / minimumSize)) {
            throw new IllegalArgumentException("Invalid or excessive CDR array length: " + count);
        }
        elementsLeft -= count;
    }

    int beginDelimited() {
        int length = readInt();
        require(length);
        int oldLimit = body.limit();
        body.limit(body.position() + length);
        return oldLimit;
    }

    void endDelimited(int oldLimit) {
        if (body.hasRemaining()) throw new IllegalArgumentException("Trailing CDR collection bytes");
        body.limit(oldLimit);
    }

    /** Validates complete consumption, allowing at most three legacy RTPS padding bytes. */
    public void finish() {
        if (body.hasRemaining()) {
            if (padding != 0 || body.remaining() > 3 || payloadLength % 4 != 0) {
                throw new IllegalArgumentException("Trailing CDR payload bytes");
            }
            while (body.hasRemaining()) {
                if (body.get() != 0) throw new IllegalArgumentException("Invalid CDR padding");
            }
        }
    }
}
