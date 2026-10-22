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
        Optional<SampleIdentity> relatedSampleIdentity,
        int statusInfo,
        boolean keyOnly) {

    public UserDataSample(Guid writerGuid, long sequenceNumber, byte[] payload) {
        this(writerGuid, sequenceNumber, payload.clone(), Optional.empty(), Optional.empty());
    }

    public UserDataSample(Guid writerGuid, long sequenceNumber, byte[] payload, Optional<RtpsTimestamp> timestamp) {
        this(writerGuid, sequenceNumber, payload.clone(), timestamp, Optional.empty());
    }

    public UserDataSample(Guid writerGuid, long sequenceNumber, byte[] payload, Optional<RtpsTimestamp> timestamp, Optional<SampleIdentity> relatedSampleIdentity) {
        this(writerGuid, sequenceNumber, payload, timestamp, relatedSampleIdentity, 0, false);
    }

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
