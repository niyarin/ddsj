package ddsj.rtps.message;

import ddsj.rtps.types.EntityId;
import ddsj.rtps.types.Guid;

public record Heartbeat(
        EntityId readerId,
        Guid writerGuid,
        long firstSequenceNumber,
        long lastSequenceNumber,
        int count) {
    public EntityId writerId() {
        return writerGuid.entityId();
    }
}
