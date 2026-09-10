package ddsjdk.rtps.history;

import ddsjdk.rtps.message.Heartbeat;
import ddsjdk.rtps.message.UserDataSample;
import ddsjdk.rtps.types.Guid;

import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class ReaderHistoryCache {
    private static final int DEFAULT_MAX_TRACKED_SEQUENCES_PER_WRITER = 1024;
    private static final int MAX_BITMAP_BITS = 256;

    private final int maxTrackedSequencesPerWriter;
    private final ConcurrentHashMap<Guid, Set<Long>> receivedSequencesByWriter = new ConcurrentHashMap<>();

    public ReaderHistoryCache() {
        this(DEFAULT_MAX_TRACKED_SEQUENCES_PER_WRITER);
    }

    public ReaderHistoryCache(int maxTrackedSequencesPerWriter) {
        if (maxTrackedSequencesPerWriter <= 0) {
            throw new IllegalArgumentException("maxTrackedSequencesPerWriter must be positive");
        }
        this.maxTrackedSequencesPerWriter = maxTrackedSequencesPerWriter;
    }

    public boolean record(UserDataSample sample) {
        Set<Long> receivedSequences = receivedSequencesByWriter.computeIfAbsent(sample.writerGuid(), ignored -> ConcurrentHashMap.newKeySet());
        boolean isNew = receivedSequences.add(sample.sequenceNumber());
        trim(receivedSequences);
        return isNew;
    }

    public MissingSequenceSet missingSequences(Heartbeat heartbeat) {
        if (heartbeat.lastSequenceNumber() < heartbeat.firstSequenceNumber()) {
            return new MissingSequenceSet(heartbeat.firstSequenceNumber(), Set.of());
        }

        Set<Long> receivedSequences = receivedSequencesByWriter.computeIfAbsent(heartbeat.writerGuid(), ignored -> ConcurrentHashMap.newKeySet());
        Set<Long> missing = new LinkedHashSet<>();
        long sequenceNumber = heartbeat.firstSequenceNumber();
        while (sequenceNumber <= heartbeat.lastSequenceNumber() && missing.size() < MAX_BITMAP_BITS) {
            if (!receivedSequences.contains(sequenceNumber)) {
                missing.add(sequenceNumber);
            }
            sequenceNumber++;
        }

        long base = missing.stream().min(Long::compareTo).orElse(heartbeat.lastSequenceNumber() + 1);
        return new MissingSequenceSet(base, missing);
    }

    private void trim(Set<Long> receivedSequences) {
        while (receivedSequences.size() > maxTrackedSequencesPerWriter) {
            receivedSequences.stream().min(Comparator.naturalOrder()).ifPresentOrElse(receivedSequences::remove, () -> { });
        }
    }
}
