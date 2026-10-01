package ddsj.cdr;

/**
 * Reads and writes a value inside a CDR stream, without an encapsulation header.
 * Nested codecs must use the supplied stream so that alignment is preserved.
 * Implementations used by shared serializers must be thread-safe.
 *
 * @param <T> Java value type
 */
public interface CdrCodec<T> {
    void write(CdrWriter out, T value);
    T read(CdrReader in);

    /** Conservative lower bound, excluding alignment; used before allocating arrays. */
    default int minimumSize() { return 0; }

    /** True only for an IDL primitive, not a struct containing a primitive. */
    default boolean isPrimitive() { return false; }
}
