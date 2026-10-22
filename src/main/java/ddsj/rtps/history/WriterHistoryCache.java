package ddsj.rtps.history;

import ddsj.rtps.qos.EndpointQos.HistoryKind;
import ddsj.rtps.message.SampleIdentity;
import java.util.*;

/** Owns serialized samples; KEEP_LAST depth is applied independently to each key. */
public final class WriterHistoryCache {
    private final HistoryKind kind;
    private final int perInstanceCapacity;
    private final ResourceLimits limits;
    private final TreeMap<Long, Entry> samples = new TreeMap<>();
    private final Map<Object, NavigableSet<Long>> instances = new HashMap<>();

    public WriterHistoryCache() { this(128); }
    public WriterHistoryCache(int maxSamples) {
        this(HistoryKind.KEEP_LAST, maxSamples, new ResourceLimits(maxSamples));
    }
    public WriterHistoryCache(HistoryKind kind, int depth, ResourceLimits limits) {
        this.kind = Objects.requireNonNull(kind);
        this.limits = Objects.requireNonNull(limits);
        if (depth <= 0 || (kind == HistoryKind.KEEP_LAST
                && (depth > limits.maxSamples() || depth > limits.maxSamplesPerInstance()))) {
            throw new IllegalArgumentException("invalid history depth");
        }
        perInstanceCapacity = kind == HistoryKind.KEEP_LAST ? depth : limits.maxSamplesPerInstance();
    }
    public boolean tryPut(long sequence, byte[] payload) { return tryPut(sequence, payload, null); }
    public boolean tryPut(long sequence, byte[] payload, SampleIdentity related) {
        return tryPut(sequence, payload, related, null, 0, false);
    }
    /** Rejection is atomic: limits never evict another instance's retained history. */
    public synchronized boolean tryPut(long sequence, byte[] payload, SampleIdentity related,
            Object key, int statusInfo, boolean keyOnly) {
        NavigableSet<Long> retained = instances.get(key);
        if (retained == null && instances.size() >= limits.maxInstances()) return false;
        boolean replacing = samples.containsKey(sequence);
        boolean evict = !replacing && kind == HistoryKind.KEEP_LAST && retained != null
                && retained.size() >= perInstanceCapacity;
        if (!replacing && !evict && samples.size() >= limits.maxSamples()) return false;
        if (!replacing && kind == HistoryKind.KEEP_ALL && retained != null
                && retained.size() >= perInstanceCapacity) return false;
        if (replacing) remove(sequence);
        if (evict) remove(retained.first());
        samples.put(sequence, new Entry(payload.clone(), related, key, statusInfo, keyOnly));
        instances.computeIfAbsent(key, ignored -> new TreeSet<>()).add(sequence);
        return true;
    }
    private void remove(long sequence) {
        Entry entry = samples.remove(sequence);
        if (entry == null) return;
        var retained = instances.get(entry.key);
        retained.remove(sequence);
        if (retained.isEmpty()) instances.remove(entry.key);
    }
    public void put(long sequence, byte[] payload) {
        if (!tryPut(sequence, payload)) throw new IllegalStateException("writer history is full");
    }
    public synchronized Optional<byte[]> get(long sequence) {
        return Optional.ofNullable(samples.get(sequence)).map(e -> e.payload.clone());
    }
    public synchronized Optional<SampleIdentity> relatedSampleIdentity(long sequence) {
        return Optional.ofNullable(samples.get(sequence)).map(Entry::related);
    }
    public synchronized int statusInfo(long sequence) {
        Entry entry = samples.get(sequence);
        return entry == null ? 0 : entry.statusInfo;
    }
    public synchronized boolean keyOnly(long sequence) {
        Entry entry = samples.get(sequence);
        return entry != null && entry.keyOnly;
    }
    /** Snapshot avoids scanning gaps between different instances' retained sequences. */
    public synchronized List<Long> sequences(long first, long last) {
        return first > last ? List.of() : List.copyOf(samples.subMap(first, true, last, true).keySet());
    }
    private record Entry(byte[] payload, SampleIdentity related, Object key, int statusInfo, boolean keyOnly) { }
    public synchronized Optional<Long> firstSequenceNumber() {
        return samples.isEmpty() ? Optional.empty() : Optional.of(samples.firstKey());
    }
    public synchronized Optional<Long> lastSequenceNumber() {
        return samples.isEmpty() ? Optional.empty() : Optional.of(samples.lastKey());
    }
    public synchronized void clear() { samples.clear(); instances.clear(); }
}
