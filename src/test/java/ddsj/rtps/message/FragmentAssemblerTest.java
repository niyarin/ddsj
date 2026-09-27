package ddsj.rtps.message;

import ddsj.rtps.types.EntityId;
import ddsj.rtps.types.Guid;
import ddsj.rtps.types.GuidPrefix;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class FragmentAssemblerTest {

    private static final GuidPrefix GUID_PREFIX = new GuidPrefix(new byte[]{
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06,
            0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c
    });
    private static final EntityId WRITER_ID = new EntityId(new byte[]{0x00, 0x00, 0x02, 0x03});
    private static final Guid WRITER_GUID = new Guid(GUID_PREFIX, WRITER_ID);

    @Test
    void assemblesTwoFragments() {
        FragmentAssembler assembler = new FragmentAssembler();

        // Sample size = 10 bytes, fragment size = 5 bytes -> 2 fragments
        byte[] frag1Data = new byte[]{0x01, 0x02, 0x03, 0x04, 0x05};
        byte[] frag2Data = new byte[]{0x06, 0x07, 0x08, 0x09, 0x0A};

        DataFragment fragment1 = new DataFragment(WRITER_GUID, 1L, 1, 1, 5, 10, frag1Data);
        DataFragment fragment2 = new DataFragment(WRITER_GUID, 1L, 2, 1, 5, 10, frag2Data);

        // First fragment - not complete yet
        Optional<UserDataSample> result1 = assembler.addFragment(fragment1);
        assertTrue(result1.isEmpty());
        assertEquals(1, assembler.pendingCount());

        // Second fragment - now complete
        Optional<UserDataSample> result2 = assembler.addFragment(fragment2);
        assertTrue(result2.isPresent());
        assertEquals(0, assembler.pendingCount());

        UserDataSample sample = result2.get();
        assertEquals(WRITER_GUID, sample.writerGuid());
        assertEquals(1L, sample.sequenceNumber());
        assertArrayEquals(new byte[]{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A}, sample.payload());
    }

    @Test
    void assemblesFragmentsOutOfOrder() {
        FragmentAssembler assembler = new FragmentAssembler();

        byte[] frag1Data = new byte[]{0x01, 0x02, 0x03, 0x04, 0x05};
        byte[] frag2Data = new byte[]{0x06, 0x07, 0x08, 0x09, 0x0A};

        DataFragment fragment1 = new DataFragment(WRITER_GUID, 1L, 1, 1, 5, 10, frag1Data);
        DataFragment fragment2 = new DataFragment(WRITER_GUID, 1L, 2, 1, 5, 10, frag2Data);

        // Receive fragment 2 first
        Optional<UserDataSample> result2 = assembler.addFragment(fragment2);
        assertTrue(result2.isEmpty());

        // Receive fragment 1 - now complete
        Optional<UserDataSample> result1 = assembler.addFragment(fragment1);
        assertTrue(result1.isPresent());

        assertArrayEquals(new byte[]{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A},
                result1.get().payload());
    }

    @Test
    void assemblesThreeFragmentsWithLastFragmentSmaller() {
        FragmentAssembler assembler = new FragmentAssembler();

        // Sample size = 12 bytes, fragment size = 5 bytes -> 3 fragments (5 + 5 + 2)
        byte[] frag1Data = new byte[]{0x01, 0x02, 0x03, 0x04, 0x05};
        byte[] frag2Data = new byte[]{0x06, 0x07, 0x08, 0x09, 0x0A};
        byte[] frag3Data = new byte[]{0x0B, 0x0C};

        DataFragment fragment1 = new DataFragment(WRITER_GUID, 1L, 1, 1, 5, 12, frag1Data);
        DataFragment fragment2 = new DataFragment(WRITER_GUID, 1L, 2, 1, 5, 12, frag2Data);
        DataFragment fragment3 = new DataFragment(WRITER_GUID, 1L, 3, 1, 5, 12, frag3Data);

        assembler.addFragment(fragment1);
        assembler.addFragment(fragment2);
        Optional<UserDataSample> result = assembler.addFragment(fragment3);

        assertTrue(result.isPresent());
        assertArrayEquals(new byte[]{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C},
                result.get().payload());
    }

    @Test
    void handlesDuplicateFragments() {
        FragmentAssembler assembler = new FragmentAssembler();

        byte[] frag1Data = new byte[]{0x01, 0x02, 0x03, 0x04, 0x05};
        byte[] frag2Data = new byte[]{0x06, 0x07, 0x08, 0x09, 0x0A};

        DataFragment fragment1 = new DataFragment(WRITER_GUID, 1L, 1, 1, 5, 10, frag1Data);
        DataFragment fragment2 = new DataFragment(WRITER_GUID, 1L, 2, 1, 5, 10, frag2Data);

        assembler.addFragment(fragment1);
        // Duplicate
        Optional<UserDataSample> dup = assembler.addFragment(fragment1);
        assertTrue(dup.isEmpty());

        Optional<UserDataSample> result = assembler.addFragment(fragment2);
        assertTrue(result.isPresent());
    }

    @Test
    void handlesMultipleSamplesConcurrently() {
        FragmentAssembler assembler = new FragmentAssembler();

        // Sample 1, seq=1
        DataFragment s1f1 = new DataFragment(WRITER_GUID, 1L, 1, 1, 4, 8, new byte[]{0x01, 0x02, 0x03, 0x04});
        DataFragment s1f2 = new DataFragment(WRITER_GUID, 1L, 2, 1, 4, 8, new byte[]{0x05, 0x06, 0x07, 0x08});

        // Sample 2, seq=2
        DataFragment s2f1 = new DataFragment(WRITER_GUID, 2L, 1, 1, 4, 8, new byte[]{0x11, 0x12, 0x13, 0x14});
        DataFragment s2f2 = new DataFragment(WRITER_GUID, 2L, 2, 1, 4, 8, new byte[]{0x15, 0x16, 0x17, 0x18});

        // Interleave fragments
        assembler.addFragment(s1f1);
        assembler.addFragment(s2f1);
        assertEquals(2, assembler.pendingCount());

        Optional<UserDataSample> r1 = assembler.addFragment(s1f2);
        assertTrue(r1.isPresent());
        assertEquals(1L, r1.get().sequenceNumber());

        Optional<UserDataSample> r2 = assembler.addFragment(s2f2);
        assertTrue(r2.isPresent());
        assertEquals(2L, r2.get().sequenceNumber());

        assertEquals(0, assembler.pendingCount());
    }

    @Test
    void handlesMultipleFragmentsInOneSubmessage() {
        FragmentAssembler assembler = new FragmentAssembler();

        // Sample size = 10, fragment size = 5, 2 fragments
        // Both fragments in one submessage
        byte[] bothFragsData = new byte[]{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A};
        DataFragment fragment = new DataFragment(WRITER_GUID, 1L, 1, 2, 5, 10, bothFragsData);

        Optional<UserDataSample> result = assembler.addFragment(fragment);
        assertTrue(result.isPresent());
        assertArrayEquals(bothFragsData, result.get().payload());
    }

    @Test
    void evictsOldSamplesWhenLimitReached() {
        FragmentAssembler assembler = new FragmentAssembler(2);

        // Start 3 samples, only 2 should remain
        DataFragment s1f1 = new DataFragment(WRITER_GUID, 1L, 1, 1, 4, 8, new byte[4]);
        DataFragment s2f1 = new DataFragment(WRITER_GUID, 2L, 1, 1, 4, 8, new byte[4]);
        DataFragment s3f1 = new DataFragment(WRITER_GUID, 3L, 1, 1, 4, 8, new byte[4]);

        assembler.addFragment(s1f1);
        assembler.addFragment(s2f1);
        assertEquals(2, assembler.pendingCount());

        assembler.addFragment(s3f1);
        // Should have evicted oldest (s1)
        assertEquals(2, assembler.pendingCount());
    }

    @Test
    void singleFragmentSample() {
        FragmentAssembler assembler = new FragmentAssembler();

        // Sample size = 5, fragment size = 10 -> only 1 fragment needed
        byte[] data = new byte[]{0x01, 0x02, 0x03, 0x04, 0x05};
        DataFragment fragment = new DataFragment(WRITER_GUID, 1L, 1, 1, 10, 5, data);

        Optional<UserDataSample> result = assembler.addFragment(fragment);
        assertTrue(result.isPresent());
        assertArrayEquals(data, result.get().payload());
    }
}
