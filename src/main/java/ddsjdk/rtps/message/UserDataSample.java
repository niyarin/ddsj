package ddsjdk.rtps.message;

import ddsjdk.rtps.types.EntityId;
import ddsjdk.rtps.types.Guid;

public record UserDataSample(Guid writerGuid, long sequenceNumber, byte[] payload) {
    public UserDataSample {
        payload = payload.clone();
    }

    public EntityId writerId() {
        return writerGuid.entityId();
    }

    @Override
    public byte[] payload() {
        return payload.clone();
    }
}
