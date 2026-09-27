package ddsj.rtps.message;

import ddsj.rtps.types.EntityId;
import ddsj.rtps.types.Guid;

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
