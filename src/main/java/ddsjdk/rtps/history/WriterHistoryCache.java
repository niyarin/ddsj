package ddsjdk.rtps.history;

import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;

public final class WriterHistoryCache {
    private static final int DEFAULT_MAX_SAMPLES = 128;

    private final int maxSamples;
    private final ConcurrentSkipListMap<Long, byte[]> samples = new ConcurrentSkipListMap<>();

    public WriterHistoryCache() {
        this(DEFAULT_MAX_SAMPLES);
    }

    public WriterHistoryCache(int maxSamples) {
        if (maxSamples <= 0) {
            throw new IllegalArgumentException("maxSamples must be positive");
        }
        this.maxSamples = maxSamples;
    }

    public void put(long sequenceNumber, byte[] serializedPayload) {
        samples.put(sequenceNumber, serializedPayload.clone());
        while (samples.size() > maxSamples) {
            samples.pollFirstEntry();
        }
    }

    public Optional<byte[]> get(long sequenceNumber) {
        byte[] payload = samples.get(sequenceNumber);
        return payload == null ? Optional.empty() : Optional.of(payload.clone());
    }

    public Optional<Long> firstSequenceNumber() {
        return samples.isEmpty() ? Optional.empty() : Optional.of(samples.firstKey());
    }

    public Optional<Long> lastSequenceNumber() {
        return samples.isEmpty() ? Optional.empty() : Optional.of(samples.lastKey());
    }
}
