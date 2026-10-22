package ddsj.rtps.history;

import ddsj.rtps.message.Heartbeat;
import ddsj.rtps.message.Gap;
import ddsj.rtps.message.UserDataSample;
import ddsj.rtps.types.Guid;

import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class ReaderHistoryCache {
    private static final int DEFAULT_MAX_TRACKED_SEQUENCES_PER_WRITER = 1024;
    private static final int MAX_BITMAP_BITS = 256;

    private final int maxTrackedSequencesPerWriter;
    private final ConcurrentHashMap<Guid, Set<Long>> receivedSequencesByWriter = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Guid, java.util.NavigableMap<Long, Long>> irrelevantByWriter = new ConcurrentHashMap<>();

    public void recordGap(Gap gap) {
        var ranges = irrelevantByWriter.computeIfAbsent(gap.writerGuid(), ignored -> new java.util.TreeMap<>());
        synchronized (ranges) {
            addRange(ranges, gap.start(), gap.base());
            for (long sequence : gap.sequences()) {
                if (sequence < Long.MAX_VALUE) addRange(ranges, sequence, sequence + 1);
            }
        }
    }

    private static void addRange(java.util.NavigableMap<Long, Long> ranges, long first, long end) {
        if (first >= end) return;
        var previous = ranges.floorEntry(first);
        if (previous != null && previous.getValue() >= first) {
            first = previous.getKey();
            end = Math.max(end, previous.getValue());
            ranges.remove(previous.getKey());
        }
        var next = ranges.ceilingEntry(first);
        while (next != null && next.getKey() <= end) {
            end = Math.max(end, next.getValue());
            ranges.remove(next.getKey());
            next = ranges.ceilingEntry(first);
        }
        ranges.put(first, end);
    }

    public ReaderHistoryCache() {
        this(DEFAULT_MAX_TRACKED_SEQUENCES_PER_WRITER);
    }

    public ReaderHistoryCache(int maxTrackedSequencesPerWriter) {
        if (maxTrackedSequencesPerWriter <= 0) {
            throw new IllegalArgumentException("maxTrackedSequencesPerWriter must be positive");
        }
        this.maxTrackedSequencesPerWriter = maxTrackedSequencesPerWriter;
    }

    public boolean contains(UserDataSample sample) {
        Set<Long> sequences = receivedSequencesByWriter.get(sample.writerGuid());
        return sequences != null && sequences.contains(sample.sequenceNumber());
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
        var ranges = irrelevantByWriter.computeIfAbsent(heartbeat.writerGuid(), ignored -> new java.util.TreeMap<>());
        synchronized (ranges) {
            ranges.entrySet().removeIf(e -> e.getValue() <= heartbeat.firstSequenceNumber());
            long sequenceNumber = heartbeat.firstSequenceNumber();
            long bitmapBase = 0;
            while (sequenceNumber <= heartbeat.lastSequenceNumber()) {
                if (bitmapBase != 0 && sequenceNumber - bitmapBase >= MAX_BITMAP_BITS) break;
                var range = ranges.floorEntry(sequenceNumber);
                if (range != null && range.getValue() > sequenceNumber) {
                    sequenceNumber = range.getValue();
                    continue;
                }
                if (!receivedSequences.contains(sequenceNumber)) {
                    if (bitmapBase == 0) bitmapBase = sequenceNumber;
                    missing.add(sequenceNumber);
                }
                if (sequenceNumber == Long.MAX_VALUE) break;
                sequenceNumber++;
            }
        }

        long base = missing.stream().min(Long::compareTo).orElse(heartbeat.lastSequenceNumber() == Long.MAX_VALUE
                ? Long.MAX_VALUE : heartbeat.lastSequenceNumber() + 1);
        return new MissingSequenceSet(base, missing);
    }

    private void trim(Set<Long> receivedSequences) {
        while (receivedSequences.size() > maxTrackedSequencesPerWriter) {
            receivedSequences.stream().min(Comparator.naturalOrder()).ifPresentOrElse(receivedSequences::remove, () -> { });
        }
    }
}
