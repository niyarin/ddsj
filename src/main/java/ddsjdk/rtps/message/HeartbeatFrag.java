package ddsjdk.rtps.message;

import ddsjdk.rtps.types.EntityId;
import ddsjdk.rtps.types.Guid;

public record HeartbeatFrag(
        EntityId readerId,
        Guid writerGuid,
        long writerSequenceNumber,
        int lastFragmentNum,
        int count) {

    public EntityId writerId() {
        return writerGuid.entityId();
    }
}
