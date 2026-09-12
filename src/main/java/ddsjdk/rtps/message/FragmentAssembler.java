package ddsjdk.rtps.message;

import ddsjdk.rtps.types.Guid;
import ddsjdk.rtps.types.RtpsTimestamp;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Assembles fragmented data (DATA_FRAG) into complete samples.
 * Thread-safe for concurrent fragment reception.
 */
public final class FragmentAssembler {
    private static final int DEFAULT_MAX_PENDING_SAMPLES = 64;

    private final Map<SampleKey, PendingSample> pendingSamples = new ConcurrentHashMap<>();
    private final int maxPendingSamples;

    public FragmentAssembler() {
        this(DEFAULT_MAX_PENDING_SAMPLES);
    }

    public FragmentAssembler(int maxPendingSamples) {
        this.maxPendingSamples = maxPendingSamples;
    }

    /**
     * Processes a DATA_FRAG and returns the assembled sample if complete.
     *
     * @param fragment the received fragment
     * @return the complete UserDataSample if all fragments received, empty otherwise
     */
    public Optional<UserDataSample> addFragment(DataFragment fragment) {
        SampleKey key = new SampleKey(fragment.writerGuid(), fragment.sequenceNumber());

        PendingSample pending = pendingSamples.computeIfAbsent(key, k -> {
            evictOldestIfNeeded();
            return new PendingSample(
                    fragment.sampleSize(),
                    fragment.fragmentSize(),
                    fragment.totalFragments(),
                    fragment.timestamp());
        });

        pending.addFragment(fragment);

        if (pending.isComplete()) {
            pendingSamples.remove(key);
            byte[] assembled = pending.assemble();
            return Optional.of(new UserDataSample(
                    fragment.writerGuid(),
                    fragment.sequenceNumber(),
                    assembled,
                    fragment.timestamp()));
        }

        return Optional.empty();
    }

    /**
     * Returns the number of samples currently being assembled.
     */
    public int pendingCount() {
        return pendingSamples.size();
    }

    /**
     * Clears all pending samples.
     */
    public void clear() {
        pendingSamples.clear();
    }

    private void evictOldestIfNeeded() {
        while (pendingSamples.size() >= maxPendingSamples) {
            // Remove oldest entry (first one in iteration order)
            var iterator = pendingSamples.keySet().iterator();
            if (iterator.hasNext()) {
                iterator.next();
                iterator.remove();
            } else {
                break;
            }
        }
    }

    private record SampleKey(Guid writerGuid, long sequenceNumber) {}

    private static final class PendingSample {
        private final byte[] buffer;
        private final int fragmentSize;
        private final int totalFragments;
        private final boolean[] received;
        private final Optional<RtpsTimestamp> timestamp;
        private int receivedCount;

        PendingSample(int sampleSize, int fragmentSize, int totalFragments, Optional<RtpsTimestamp> timestamp) {
            this.buffer = new byte[sampleSize];
            this.fragmentSize = fragmentSize;
            this.totalFragments = totalFragments;
            this.received = new boolean[totalFragments];
            this.timestamp = timestamp;
            this.receivedCount = 0;
        }

        synchronized void addFragment(DataFragment fragment) {
            byte[] data = fragment.fragmentData();
            int startingNum = fragment.fragmentStartingNum();
            int fragmentsInSubmessage = fragment.fragmentsInSubmessage();

            int dataOffset = 0;
            for (int i = 0; i < fragmentsInSubmessage; i++) {
                int fragmentIndex = startingNum - 1 + i; // fragmentStartingNum is 1-based
                if (fragmentIndex < 0 || fragmentIndex >= totalFragments) {
                    continue;
                }
                if (received[fragmentIndex]) {
                    // Skip already received fragment
                    dataOffset += fragmentSize;
                    continue;
                }

                int bufferOffset = fragmentIndex * fragmentSize;
                int copyLength = Math.min(fragmentSize, buffer.length - bufferOffset);
                if (dataOffset + copyLength <= data.length) {
                    System.arraycopy(data, dataOffset, buffer, bufferOffset, copyLength);
                    received[fragmentIndex] = true;
                    receivedCount++;
                }
                dataOffset += fragmentSize;
            }
        }

        synchronized boolean isComplete() {
            return receivedCount >= totalFragments;
        }

        synchronized byte[] assemble() {
            return buffer.clone();
        }
    }
}
