package ddsj.rtps.history;

import ddsj.rtps.qos.EndpointQos.HistoryKind;
import java.util.Optional;
import java.util.TreeMap;
import java.util.Objects;

/** Sole owner of serialized writer samples, including fragmented samples. */
public final class WriterHistoryCache {
    private final int capacity;
    private final HistoryKind kind;
    private final TreeMap<Long, byte[]> samples = new TreeMap<>();

    public WriterHistoryCache() {
        this(128);
    }
    public WriterHistoryCache(int maxSamples) {
        this(HistoryKind.KEEP_LAST, maxSamples, new ResourceLimits(maxSamples));
    }
    public WriterHistoryCache(HistoryKind kind, int depth, ResourceLimits limits) {
        this.kind = Objects.requireNonNull(kind);
        if (depth <= 0 || (kind == HistoryKind.KEEP_LAST && depth > limits.maxSamples())) {
            throw new IllegalArgumentException("invalid history depth");
        }
        capacity = kind == HistoryKind.KEEP_LAST ? depth : limits.maxSamples();
    }

    /** Rejects a new KEEP_ALL sample when full; existing samples are never evicted. */
    public synchronized boolean tryPut(long sequenceNumber, byte[] payload) {
        if (kind == HistoryKind.KEEP_ALL && samples.size() >= capacity && !samples.containsKey(sequenceNumber)) {
            return false;
        }
        samples.put(sequenceNumber, payload.clone());
        while (samples.size() > capacity) {
            samples.pollFirstEntry();
        }
        return true;
    }
    public void put(long sequenceNumber, byte[] payload) {
        if (!tryPut(sequenceNumber, payload)) {
            throw new IllegalStateException("writer history is full");
        }
    }
    public synchronized Optional<byte[]> get(long sequenceNumber) {
        return Optional.ofNullable(samples.get(sequenceNumber)).map(byte[]::clone);
    }
    public synchronized Optional<Long> firstSequenceNumber() {
        return samples.isEmpty() ? Optional.empty() : Optional.of(samples.firstKey());
    }
    public synchronized Optional<Long> lastSequenceNumber() {
        return samples.isEmpty() ? Optional.empty() : Optional.of(samples.lastKey());
    }
    public synchronized void clear() {
        samples.clear();
    }
}
