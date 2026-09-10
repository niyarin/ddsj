package ddsjdk.rtps.message;

import ddsjdk.rtps.types.EntityId;

import java.util.Set;

public record AckNack(
        EntityId readerId,
        EntityId writerId,
        Set<Long> requestedSequenceNumbers,
        int count) {
    public AckNack {
        requestedSequenceNumbers = Set.copyOf(requestedSequenceNumbers);
    }
}
