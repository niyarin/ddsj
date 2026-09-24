package ddsjdk.dds.status;

/**
 * Status for samples lost.
 *
 * @param totalCount total cumulative count
 * @param totalCountChange change since last read
 */
public record SampleLostStatus(int totalCount, int totalCountChange) {
    public static final SampleLostStatus INITIAL = new SampleLostStatus(0, 0);
}
