package ddsj.rtps.runtime;

import ddsj.rtps.types.Guid;

/**
 * Status information provided when liveliness changes.
 */
public record LivelinessChangedStatus(
        Guid writerGuid,
        boolean alive,
        long aliveCount,
        long notAliveCount) {}
