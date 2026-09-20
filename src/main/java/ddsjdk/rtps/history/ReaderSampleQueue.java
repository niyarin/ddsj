package ddsjdk.rtps.history;

import ddsjdk.rtps.discovery.EndpointQos.HistoryKind;
import java.util.ArrayDeque;
import java.util.Objects;

/** Unread samples for the currently supported no-key endpoint (one instance). */
public final class ReaderSampleQueue<T> {
    private final ArrayDeque<T> samples = new ArrayDeque<>();
    private final HistoryKind kind;
    private final int capacity;
    public ReaderSampleQueue(HistoryKind kind, int depth, ResourceLimits limits) {
        this.kind = Objects.requireNonNull(kind);
        if (depth <= 0 || (kind == HistoryKind.KEEP_LAST && depth > limits.maxSamples())) {
            throw new IllegalArgumentException("invalid history depth");
        }
        capacity = kind == HistoryKind.KEEP_LAST ? depth : limits.maxSamples();
    }
    public synchronized boolean offer(T sample) {
        Objects.requireNonNull(sample);
        if (samples.size() == capacity) {
            if (kind == HistoryKind.KEEP_ALL) {
                return false;
            }
            samples.removeFirst();
        }
        samples.addLast(sample);
        return true;
    }
    public synchronized T poll() {
        return samples.pollFirst();
    }
    public synchronized void clear() {
        samples.clear();
    }
}
