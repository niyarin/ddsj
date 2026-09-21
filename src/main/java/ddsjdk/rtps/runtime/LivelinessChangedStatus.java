package ddsjdk.rtps.runtime;

import ddsjdk.rtps.types.Guid;

/**
 * Status information provided when liveliness changes.
 */
public record LivelinessChangedStatus(
        Guid writerGuid,
        boolean alive,
        long aliveCount,
        long notAliveCount) {}
