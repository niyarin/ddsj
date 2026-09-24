package ddsjdk.dds.status;

/**
 * Status for inconsistent topic QoS.
 *
 * @param totalCount total cumulative count of inconsistencies
 * @param totalCountChange change since last read
 */
public record InconsistentTopicStatus(int totalCount, int totalCountChange) {

    /**
     * Initial status with no inconsistencies.
     */
    public static final InconsistentTopicStatus INITIAL = new InconsistentTopicStatus(0, 0);
}
