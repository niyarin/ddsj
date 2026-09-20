package ddsjdk.rtps.history;

import ddsjdk.rtps.discovery.EndpointQos.HistoryKind;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class HistoryRetentionTest {
    @Test void keepLastEvictsOldestPayloadAndDefensivelyCopies() {
        var history = new WriterHistoryCache(HistoryKind.KEEP_LAST, 2, new ResourceLimits(5));
        byte[] payload = {1};
        history.put(1, payload);
        payload[0] = 9;
        assertArrayEquals(new byte[]{1}, history.get(1).orElseThrow());
        history.put(2, new byte[]{2});
        history.put(3, new byte[]{3});
        assertTrue(history.get(1).isEmpty());
        assertEquals(2L, history.firstSequenceNumber().orElseThrow());
        assertEquals(3L, history.lastSequenceNumber().orElseThrow());
    }
    @Test void keepAllUsesResourceLimitRatherThanDepthAndRejectsWithoutEviction() {
        var history = new WriterHistoryCache(HistoryKind.KEEP_ALL, 1, new ResourceLimits(150));
        for (int i = 1; i <= 150; i++) assertTrue(history.tryPut(i, new byte[]{1}));
        assertFalse(history.tryPut(151, new byte[]{2}));
        assertTrue(history.get(1).isPresent());
        assertTrue(history.get(150).isPresent());
        assertTrue(history.get(151).isEmpty());
        history.clear();
        assertTrue(history.firstSequenceNumber().isEmpty());
        assertTrue(history.tryPut(151, new byte[]{2}));
    }
    @Test void readerKeepLastRetainsNewestUnreadSamples() {
        var queue = new ReaderSampleQueue<Integer>(HistoryKind.KEEP_LAST, 2, new ResourceLimits(5));
        queue.offer(1); queue.offer(2); queue.offer(3);
        assertEquals(2, queue.poll());
        assertEquals(3, queue.poll());
        assertNull(queue.poll());
    }
    @Test void readerKeepAllRejectsUntilApplicationTakesData() {
        var queue = new ReaderSampleQueue<Integer>(HistoryKind.KEEP_ALL, 1, new ResourceLimits(2));
        assertTrue(queue.offer(1)); assertTrue(queue.offer(2)); assertFalse(queue.offer(3));
        assertEquals(1, queue.poll());
        assertTrue(queue.offer(3));
        assertEquals(2, queue.poll()); assertEquals(3, queue.poll());
    }
    @Test void rejectsInconsistentLimits() {
        assertThrows(IllegalArgumentException.class, () -> new ResourceLimits(0));
        assertThrows(IllegalArgumentException.class, () -> new WriterHistoryCache(HistoryKind.KEEP_LAST, 3, new ResourceLimits(2)));
        assertThrows(IllegalArgumentException.class, () -> new ReaderSampleQueue<>(HistoryKind.KEEP_LAST, 3, new ResourceLimits(2)));
    }
}
