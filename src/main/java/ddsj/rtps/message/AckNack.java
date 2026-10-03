package ddsj.rtps.message;

import ddsj.rtps.types.EntityId;
import ddsj.rtps.types.GuidPrefix;

import java.util.Set;

public record AckNack(
        EntityId readerId,
        EntityId writerId,
        Set<Long> requestedSequenceNumbers,
        int count,
        GuidPrefix sourceGuidPrefix) {
    public AckNack(EntityId readerId, EntityId writerId, Set<Long> requestedSequenceNumbers, int count) {
        this(readerId, writerId, requestedSequenceNumbers, count, null);
    }
    public AckNack {
        requestedSequenceNumbers = Set.copyOf(requestedSequenceNumbers);
    }
}
