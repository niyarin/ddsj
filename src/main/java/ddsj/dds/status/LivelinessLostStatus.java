package ddsj.dds.status;

/**
 * Status for DataWriter liveliness lost.
 *
 * @param totalCount total cumulative count
 * @param totalCountChange change since last read
 */
public record LivelinessLostStatus(int totalCount, int totalCountChange) {
    public static final LivelinessLostStatus INITIAL = new LivelinessLostStatus(0, 0);
}
