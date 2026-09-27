package ddsj.rtps.message;

import ddsj.rtps.types.EntityId;
import ddsj.rtps.types.Guid;
import ddsj.rtps.types.RtpsTimestamp;

import java.util.Optional;

public record UserDataSample(
        Guid writerGuid,
        long sequenceNumber,
        byte[] payload,
        Optional<RtpsTimestamp> timestamp) {

    public UserDataSample(Guid writerGuid, long sequenceNumber, byte[] payload) {
        this(writerGuid, sequenceNumber, payload.clone(), Optional.empty());
    }

    public UserDataSample(Guid writerGuid, long sequenceNumber, byte[] payload, Optional<RtpsTimestamp> timestamp) {
        this.writerGuid = writerGuid;
        this.sequenceNumber = sequenceNumber;
        this.payload = payload.clone();
        this.timestamp = timestamp;
    }

    public EntityId writerId() {
        return writerGuid.entityId();
    }

    @Override
    public byte[] payload() {
        return payload.clone();
    }
}
