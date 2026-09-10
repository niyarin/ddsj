package ddsjdk.rtps.discovery;

import ddsjdk.rtps.types.Guid;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public final class SedpEndpointSampleHistory {
    private final AtomicLong nextSequenceNumber = new AtomicLong(1);
    private final ConcurrentHashMap<Guid, SedpEndpointSample> latestSampleByEndpoint = new ConcurrentHashMap<>();
    private final ConcurrentSkipListMap<Long, SedpEndpointSample> sampleBySequenceNumber = new ConcurrentSkipListMap<>();

    public SedpEndpointSample addOrUpdate(Guid endpointGuid, byte[] payload) {
        SedpEndpointSample current = latestSampleByEndpoint.get(endpointGuid);
        if (current != null && java.util.Arrays.equals(current.payload(), payload)) {
            return current;
        }

        SedpEndpointSample sample = new SedpEndpointSample(endpointGuid, nextSequenceNumber.getAndIncrement(), payload);
        latestSampleByEndpoint.put(endpointGuid, sample);
        sampleBySequenceNumber.put(sample.sequenceNumber(), sample);
        return sample;
    }

    public List<SedpEndpointSample> samples() {
        return sampleBySequenceNumber.values().stream().toList();
    }

    public Optional<SedpEndpointSample> get(long sequenceNumber) {
        return Optional.ofNullable(sampleBySequenceNumber.get(sequenceNumber));
    }

    public long firstSequenceNumber() {
        return sampleBySequenceNumber.isEmpty() ? 1L : sampleBySequenceNumber.firstKey();
    }

    public long lastSequenceNumber() {
        return sampleBySequenceNumber.isEmpty() ? 0L : sampleBySequenceNumber.lastKey();
    }
}
