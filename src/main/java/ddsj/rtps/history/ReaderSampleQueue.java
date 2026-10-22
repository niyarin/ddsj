package ddsj.rtps.history;

import ddsj.rtps.qos.EndpointQos.HistoryKind;
import java.util.*;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.ToIntFunction;

/** Retains unread samples with independent history depth for each instance key. */
public final class ReaderSampleQueue<T> {
    private final ArrayDeque<T> samples = new ArrayDeque<>();
    private final Map<Object, Integer> counts = new HashMap<>();
    private final HistoryKind kind;
    private final int perInstanceCapacity;
    private final ResourceLimits limits;
    private final Function<T, Object> keyExtractor;
    private IntSupplier retainedTotal = () -> 0;
    private ToIntFunction<Object> retainedPerInstance = ignored -> 0;
    private Comparator<T> sequenceComparator = (a, b) -> 0;

    public synchronized void setRetainedUsage(IntSupplier total, ToIntFunction<Object> perInstance) {
        retainedTotal = Objects.requireNonNull(total);
        retainedPerInstance = Objects.requireNonNull(perInstance);
    }
    public synchronized void setSequenceComparator(Comparator<T> comparator) {
        sequenceComparator = Objects.requireNonNull(comparator);
    }

    public ReaderSampleQueue(HistoryKind kind, int depth, ResourceLimits limits) {
        this(kind, depth, limits, ignored -> null);
    }
    public ReaderSampleQueue(HistoryKind kind, int depth, ResourceLimits limits, Function<T, Object> keyExtractor) {
        this.kind = Objects.requireNonNull(kind);
        this.limits = Objects.requireNonNull(limits);
        this.keyExtractor = Objects.requireNonNull(keyExtractor);
        if (depth <= 0 || (kind == HistoryKind.KEEP_LAST
                && (depth > limits.maxSamples() || depth > limits.maxSamplesPerInstance()))) {
            throw new IllegalArgumentException("invalid history depth");
        }
        perInstanceCapacity = kind == HistoryKind.KEEP_LAST ? depth : limits.maxSamplesPerInstance();
    }
    public synchronized boolean offer(T sample) {
        Objects.requireNonNull(sample);
        Object key = keyExtractor.apply(sample);
        int count = counts.getOrDefault(key, 0);
        if (count == 0 && counts.size() >= limits.maxInstances()) return false;
        boolean evict = kind == HistoryKind.KEEP_LAST && count >= perInstanceCapacity;
        if (kind == HistoryKind.KEEP_ALL && (samples.size() + (long) retainedTotal.getAsInt() >= limits.maxSamples()
                || count + (long) retainedPerInstance.applyAsInt(key) >= perInstanceCapacity)) return false;
        if (!evict && (samples.size() >= limits.maxSamples() || count >= perInstanceCapacity)) return false;
        if (evict) {
            T oldest = null;
            for (T candidate : samples) {
                if (Objects.equals(keyExtractor.apply(candidate), key)
                        && (oldest == null || sequenceComparator.compare(candidate, oldest) < 0)) oldest = candidate;
            }
            if (sequenceComparator.compare(sample, oldest) < 0) return true;
            var iterator = samples.iterator();
            while (iterator.hasNext()) {
                if (iterator.next() == oldest) {
                    iterator.remove(); count--; break;
                }
            }
        }
        samples.addLast(sample);
        counts.put(key, count + 1);
        return true;
    }
    public synchronized T poll() {
        T sample = samples.pollFirst();
        if (sample != null) {
            Object key = keyExtractor.apply(sample);
            int count = counts.get(key);
            if (count == 1) counts.remove(key);
            else counts.put(key, count - 1);
        }
        return sample;
    }
    public synchronized void clear() { samples.clear(); counts.clear(); }
}
