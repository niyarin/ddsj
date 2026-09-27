package ddsj.rtps.discovery;

import ddsj.rtps.types.Guid;

public record SedpEndpointSample(Guid endpointGuid, long sequenceNumber, byte[] payload) {
    public SedpEndpointSample {
        payload = payload.clone();
    }

    @Override
    public byte[] payload() {
        return payload.clone();
    }
}
