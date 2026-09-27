package ddsj.dds.status;

import ddsj.dds.instance.InstanceHandle;

/**
 * Status for DataWriter publication matched.
 *
 * @param totalCount total cumulative count of matches
 * @param totalCountChange change since last read
 * @param currentCount current number of matched DataReaders
 * @param currentCountChange change in current count since last read
 * @param lastSubscriptionHandle handle of last matched DataReader
 */
public record PublicationMatchedStatus(
        int totalCount,
        int totalCountChange,
        int currentCount,
        int currentCountChange,
        InstanceHandle lastSubscriptionHandle
) {
    public static final PublicationMatchedStatus INITIAL =
            new PublicationMatchedStatus(0, 0, 0, 0, InstanceHandle.NIL);
}
