package ddsjdk.dds.status;

import ddsjdk.dds.instance.InstanceHandle;

/**
 * Status for DataReader deadline missed.
 *
 * @param totalCount total cumulative count
 * @param totalCountChange change since last read
 * @param lastInstanceHandle handle of the last instance that missed deadline
 */
public record RequestedDeadlineMissedStatus(
        int totalCount,
        int totalCountChange,
        InstanceHandle lastInstanceHandle
) {
    public static final RequestedDeadlineMissedStatus INITIAL =
            new RequestedDeadlineMissedStatus(0, 0, InstanceHandle.NIL);
}
