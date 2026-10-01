package ddsj.dds.sample;

import ddsj.dds.instance.InstanceHandle;
import ddsj.dds.instance.SampleInfo;
import ddsj.dds.instance.SampleState;
import ddsj.rtps.message.SampleIdentity;
import ddsj.rtps.protocol.RtpsEntity;
import ddsj.rtps.types.Guid;
import ddsj.rtps.types.GuidPrefix;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class SampleTest {
    private static final SampleInfo INFO = SampleInfo.forValidData(
            Instant.EPOCH, InstanceHandle.NIL, InstanceHandle.NIL);
    private static final Guid WRITER = new GuidPrefix(new byte[12]).toGuid(RtpsEntity.USER_WRITER_NO_KEY);
    private static final SampleIdentity RELATED = new SampleIdentity(
            new GuidPrefix(new byte[]{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
                    .toGuid(RtpsEntity.USER_WRITER_NO_KEY), 7);

    @Test void sourceWriterIsIndependentOfRelatedIdentity() {
        var sample = new Sample<>(42, INFO, Optional.of(RELATED), 23, Optional.of(WRITER));
        assertEquals(WRITER, sample.writerGuid().orElseThrow());
        assertEquals(23, sample.writerSequenceNumber());
        assertEquals(RELATED, sample.relatedSampleIdentity().orElseThrow());
        assertNotEquals(RELATED.writerGuid(), sample.writerGuid().orElseThrow());
    }

    @Test void legacyConstructorsLeaveSourceWriterUnknown() {
        assertTrue(new Sample<>(42, INFO).writerGuid().isEmpty());
        assertTrue(new Sample<>(42, INFO, Optional.of(RELATED)).writerGuid().isEmpty());
        var sample = new Sample<>(42, INFO, Optional.of(RELATED), 23);
        assertTrue(sample.writerGuid().isEmpty());
        assertEquals(23, sample.writerSequenceNumber());
        assertEquals(Optional.of(RELATED), sample.relatedSampleIdentity());
    }

    @Test void rejectsNullWriterOptional() {
        assertThrows(NullPointerException.class,
                () -> new Sample<>(42, INFO, Optional.empty(), 23, null));
    }

    @Test void readerCachePreservesSourceWriterAcrossReadStateChanges() throws Exception {
        // Exercise the cache conversion without opening participant UDP sockets.
        var cacheType = Class.forName("ddsj.dds.core.DataReader$CachedSample");
        var constructor = cacheType.getDeclaredConstructor(
                Object.class, SampleInfo.class, Optional.class, long.class, Guid.class);
        constructor.setAccessible(true);
        var toSample = cacheType.getDeclaredMethod("toSample");
        toSample.setAccessible(true);
        var markRead = cacheType.getDeclaredMethod("markRead");
        markRead.setAccessible(true);
        for (var related : java.util.List.of(Optional.<SampleIdentity>empty(), Optional.of(RELATED))) {
            var cache = constructor.newInstance(42, INFO, related, 23L, WRITER);
            var unread = (Sample<?>) toSample.invoke(cache);
            assertEquals(Optional.of(WRITER), unread.writerGuid());
            assertEquals(SampleState.NOT_READ, unread.sampleState());
            markRead.invoke(cache);
            var read = (Sample<?>) toSample.invoke(cache);
            assertEquals(Optional.of(WRITER), read.writerGuid());
            assertEquals(23, read.writerSequenceNumber());
            assertEquals(related, read.relatedSampleIdentity());
            assertEquals(SampleState.READ, read.sampleState());
        }
    }
}
