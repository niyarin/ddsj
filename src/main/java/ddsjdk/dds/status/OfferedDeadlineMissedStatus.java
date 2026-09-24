package ddsjdk.dds.status;

import ddsjdk.dds.instance.InstanceHandle;

/**
 * Status for DataWriter deadline missed.
 *
 * @param totalCount total cumulative count
 * @param totalCountChange change since last read
 * @param lastInstanceHandle handle of the last instance that missed deadline
 */
public record OfferedDeadlineMissedStatus(
        int totalCount,
        int totalCountChange,
        InstanceHandle lastInstanceHandle
) {
    public static final OfferedDeadlineMissedStatus INITIAL =
            new OfferedDeadlineMissedStatus(0, 0, InstanceHandle.NIL);
}
