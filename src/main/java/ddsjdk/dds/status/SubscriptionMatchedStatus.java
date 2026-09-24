package ddsjdk.dds.status;

import ddsjdk.dds.instance.InstanceHandle;

/**
 * Status for DataReader subscription matched.
 *
 * @param totalCount total cumulative count of matches
 * @param totalCountChange change since last read
 * @param currentCount current number of matched DataWriters
 * @param currentCountChange change in current count since last read
 * @param lastPublicationHandle handle of last matched DataWriter
 */
public record SubscriptionMatchedStatus(
        int totalCount,
        int totalCountChange,
        int currentCount,
        int currentCountChange,
        InstanceHandle lastPublicationHandle
) {
    public static final SubscriptionMatchedStatus INITIAL =
            new SubscriptionMatchedStatus(0, 0, 0, 0, InstanceHandle.NIL);
}
