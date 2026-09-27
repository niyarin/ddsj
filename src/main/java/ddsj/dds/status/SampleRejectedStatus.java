package ddsj.dds.status;

import ddsj.dds.instance.InstanceHandle;

/**
 * Status for samples rejected.
 *
 * @param totalCount total cumulative count
 * @param totalCountChange change since last read
 * @param lastReason reason for last rejection
 * @param lastInstanceHandle handle of the instance whose sample was rejected
 */
public record SampleRejectedStatus(
        int totalCount,
        int totalCountChange,
        SampleRejectedReason lastReason,
        InstanceHandle lastInstanceHandle
) {
    public static final SampleRejectedStatus INITIAL =
            new SampleRejectedStatus(0, 0, SampleRejectedReason.NOT_REJECTED, InstanceHandle.NIL);

    public enum SampleRejectedReason {
        NOT_REJECTED,
        REJECTED_BY_INSTANCES_LIMIT,
        REJECTED_BY_SAMPLES_LIMIT,
        REJECTED_BY_SAMPLES_PER_INSTANCE_LIMIT
    }
}
