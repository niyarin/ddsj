package ddsjdk.rtps.message;

import ddsjdk.rtps.types.EntityId;
import ddsjdk.rtps.types.Guid;

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
