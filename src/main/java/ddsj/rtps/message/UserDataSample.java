package ddsj.rtps.message;

import ddsj.rtps.types.EntityId;
import ddsj.rtps.types.Guid;
import ddsj.rtps.types.RtpsTimestamp;

import java.util.Optional;

public record UserDataSample(
        Guid writerGuid,
        long sequenceNumber,
        byte[] payload,
        Optional<RtpsTimestamp> timestamp,
        Optional<SampleIdentity> relatedSampleIdentity) {

    public UserDataSample(Guid writerGuid, long sequenceNumber, byte[] payload) {
        this(writerGuid, sequenceNumber, payload.clone(), Optional.empty(), Optional.empty());
    }

    public UserDataSample(Guid writerGuid, long sequenceNumber, byte[] payload, Optional<RtpsTimestamp> timestamp) {
        this(writerGuid, sequenceNumber, payload.clone(), timestamp, Optional.empty());
    }

    public UserDataSample(Guid writerGuid, long sequenceNumber, byte[] payload, Optional<RtpsTimestamp> timestamp, Optional<SampleIdentity> relatedSampleIdentity) {
        this.writerGuid = writerGuid;
        this.sequenceNumber = sequenceNumber;
        this.payload = payload.clone();
        this.timestamp = timestamp;
        this.relatedSampleIdentity = relatedSampleIdentity;
    }

    public EntityId writerId() {
        return writerGuid.entityId();
    }

    @Override
    public byte[] payload() {
        return payload.clone();
    }
}
