package ddsj.rtps.history;

import ddsj.rtps.message.Gap;
import ddsj.rtps.message.Heartbeat;
import ddsj.rtps.message.UserDataSample;
import ddsj.rtps.protocol.RtpsEntity;
import ddsj.rtps.types.GuidPrefix;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import java.util.Set;
import static ddsj.rtps.qos.EndpointQos.HistoryKind.*;
import static org.junit.jupiter.api.Assertions.*;

class KeyedHistoryTest {
    @Test void keepLastEvictsOnlyTheSameKeyAndPreservesOtherKeysWhenGloballyFull() {
        var cache = new WriterHistoryCache(KEEP_LAST, 1, new ResourceLimits(2, 3, 1));
        assertTrue(cache.tryPut(1, new byte[]{1}, null, "A", 0, false));
        assertTrue(cache.tryPut(2, new byte[]{2}, null, "B", 0, false));
        assertFalse(cache.tryPut(3, new byte[]{3}, null, "C", 0, false));
        assertTrue(cache.tryPut(3, new byte[]{4}, null, "A", 0, false));
        assertTrue(cache.get(1).isEmpty());
        assertArrayEquals(new byte[]{2}, cache.get(2).orElseThrow());
        assertArrayEquals(new byte[]{4}, cache.get(3).orElseThrow());
    }

    @Test void keepAllEnforcesPerInstanceAndInstanceCountLimitsIndependently() {
        var cache = new WriterHistoryCache(KEEP_ALL, 1, new ResourceLimits(10, 2, 2));
        assertTrue(cache.tryPut(1, new byte[]{1}, null, "A", 0, false));
        assertTrue(cache.tryPut(2, new byte[]{1}, null, "A", 0, false));
        assertFalse(cache.tryPut(3, new byte[]{1}, null, "A", 0, false));
        assertTrue(cache.tryPut(3, new byte[]{1}, null, "B", 0, false));
        assertFalse(cache.tryPut(4, new byte[]{1}, null, "C", 0, false));
        assertEquals(3, cache.sequences(1, 10).size());
    }

    @Test void readerRejectsExcessKeysWithoutEvictingTheirLatestValues() {
        var cache = new ReaderSampleQueue<String>(KEEP_LAST, 1, new ResourceLimits(3, 2, 1), s -> s.substring(0, 1));
        assertTrue(cache.offer("A1"));
        assertTrue(cache.offer("B1"));
        assertFalse(cache.offer("C1"));
        assertTrue(cache.offer("A2"));
        assertEquals("B1", cache.poll());
        assertEquals("A2", cache.poll());
    }

    @Test @Timeout(1) void sparseGapRangesAreSkippedWithoutScanningEverySequence() {
        var history = new ReaderHistoryCache();
        var writer = new GuidPrefix(new byte[12]).toGuid(RtpsEntity.USER_WRITER_NO_KEY);
        history.record(new UserDataSample(writer, 1, new byte[]{1}));
        history.recordGap(new Gap(writer, 2, 1_000_000_000, Set.of()));
        var missing = history.missingSequences(new Heartbeat(RtpsEntity.USER_READER_NO_KEY, writer, 1, 1_000_000_000, 1));
        assertEquals(Set.of(1_000_000_000L), missing.missing());
        assertEquals(1_000_000_000L, missing.baseSequenceNumber());
    }

    @Test void gapBitmapAndContiguousRangesAreBothApplied() {
        var history = new ReaderHistoryCache();
        var writer = new GuidPrefix(new byte[12]).toGuid(RtpsEntity.USER_WRITER_NO_KEY);
        history.recordGap(new Gap(writer, 1, 3, Set.of(4L, 6L)));
        assertEquals(Set.of(3L, 5L, 7L), history.missingSequences(
                new Heartbeat(RtpsEntity.USER_READER_NO_KEY, writer, 1, 7, 1)).missing());
    }
}
