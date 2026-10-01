package ddsj.cdr;

/** Supported plain CDR encodings. Structs are encoded as FINAL types. */
public enum CdrEncoding {
    XCDR1(1, 8), XCDR2(7, 4);

    final int littleEndianId;
    final int maxAlignment;

    CdrEncoding(int littleEndianId, int maxAlignment) {
        this.littleEndianId = littleEndianId;
        this.maxAlignment = maxAlignment;
    }
}
