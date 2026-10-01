package ddsj.cdr;

import ddsj.rtps.runtime.PayloadSerializer;
import java.util.Objects;

/** Adds encapsulation and final validation around a reusable body codec. */
public final class CdrPayloadSerializer<T> implements PayloadSerializer<T> {
    private final CdrCodec<T> codec;
    private final CdrEncoding encoding;
    private final int maxArrayElements;

    public CdrPayloadSerializer(CdrCodec<T> codec) { this(codec, CdrEncoding.XCDR1); }

    public CdrPayloadSerializer(CdrCodec<T> codec, CdrEncoding encoding) {
        this(codec, encoding, CdrReader.DEFAULT_MAX_ARRAY_ELEMENTS);
    }

    /** The element limit is cumulative across all arrays in a received payload. */
    public CdrPayloadSerializer(CdrCodec<T> codec, CdrEncoding encoding, int maxArrayElements) {
        this.codec = Objects.requireNonNull(codec, "codec");
        this.encoding = Objects.requireNonNull(encoding, "encoding");
        if (maxArrayElements < 0) throw new IllegalArgumentException("Negative array element limit");
        this.maxArrayElements = maxArrayElements;
    }

    @Override public byte[] serialize(T value) {
        CdrWriter out = new CdrWriter(encoding);
        codec.write(out, Objects.requireNonNull(value, "value"));
        return out.toByteArray();
    }

    /** Accepts either byte order and either supported encoding, independent of output encoding. */
    @Override public T deserialize(byte[] payload) {
        CdrReader in = new CdrReader(payload, maxArrayElements);
        T value = Objects.requireNonNull(codec.read(in), "Decoded value");
        in.finish();
        return value;
    }
}
