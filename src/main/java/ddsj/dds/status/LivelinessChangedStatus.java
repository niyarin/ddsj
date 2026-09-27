package ddsj.dds.status;

import ddsj.dds.instance.InstanceHandle;

/**
 * Status for DataReader liveliness changed.
 *
 * @param aliveCount number of currently alive DataWriters
 * @param aliveCountChange change since last read
 * @param notAliveCount number of not-alive DataWriters
 * @param notAliveCountChange change since last read
 * @param lastPublicationHandle handle of last writer whose liveliness changed
 */
public record LivelinessChangedStatus(
        int aliveCount,
        int aliveCountChange,
        int notAliveCount,
        int notAliveCountChange,
        InstanceHandle lastPublicationHandle
) {
    public static final LivelinessChangedStatus INITIAL =
            new LivelinessChangedStatus(0, 0, 0, 0, InstanceHandle.NIL);
}
