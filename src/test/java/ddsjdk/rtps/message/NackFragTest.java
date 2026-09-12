package ddsjdk.rtps.message;

import ddsjdk.rtps.types.EntityId;
import ddsjdk.rtps.types.GuidPrefix;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class NackFragTest {

    private static final EntityId READER_ID = new EntityId(new byte[]{0x00, 0x00, 0x02, 0x04});
    private static final EntityId WRITER_ID = new EntityId(new byte[]{0x00, 0x00, 0x02, 0x03});

    private static GuidPrefix testGuidPrefix() {
        return new GuidPrefix(new byte[]{
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06,
                0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c
        });
    }

    @Test
    void buildAndParseNackFrag() {
        GuidPrefix guidPrefix = testGuidPrefix();
        Set<Integer> missingFragments = Set.of(1, 3, 5);

        RtpsMessageBuilder builder = new RtpsMessageBuilder(guidPrefix);
        builder.nackFrag(READER_ID, WRITER_ID, 42L, missingFragments, 100);

        byte[] packet = builder.bytes();
        List<NackFrag> nackFrags = RtpsUserDataParser.readNackFrags(packet, packet.length);

        assertEquals(1, nackFrags.size());
        NackFrag nf = nackFrags.get(0);

        assertEquals(READER_ID, nf.readerId());
        assertEquals(WRITER_ID, nf.writerId());
        assertEquals(guidPrefix, nf.writerGuid().prefix());
        assertEquals(42L, nf.writerSequenceNumber());
        assertEquals(Set.of(1, 3, 5), nf.requestedFragmentNumbers());
        assertEquals(100, nf.count());
    }

    @Test
    void buildAndParseNackFrag_emptyBitmap() {
        GuidPrefix guidPrefix = testGuidPrefix();
        Set<Integer> missingFragments = Set.of();

        RtpsMessageBuilder builder = new RtpsMessageBuilder(guidPrefix);
        builder.nackFrag(READER_ID, WRITER_ID, 10L, missingFragments, 50);

        byte[] packet = builder.bytes();
        List<NackFrag> nackFrags = RtpsUserDataParser.readNackFrags(packet, packet.length);

        assertEquals(1, nackFrags.size());
        NackFrag nf = nackFrags.get(0);

        assertEquals(10L, nf.writerSequenceNumber());
        assertTrue(nf.requestedFragmentNumbers().isEmpty());
        assertEquals(50, nf.count());
    }

    @Test
    void buildAndParseNackFrag_consecutiveFragments() {
        GuidPrefix guidPrefix = testGuidPrefix();
        Set<Integer> missingFragments = Set.of(10, 11, 12, 13, 14);

        RtpsMessageBuilder builder = new RtpsMessageBuilder(guidPrefix);
        builder.nackFrag(READER_ID, WRITER_ID, 99L, missingFragments, 1);

        byte[] packet = builder.bytes();
        List<NackFrag> nackFrags = RtpsUserDataParser.readNackFrags(packet, packet.length);

        assertEquals(1, nackFrags.size());
        NackFrag nf = nackFrags.get(0);

        assertEquals(99L, nf.writerSequenceNumber());
        assertEquals(Set.of(10, 11, 12, 13, 14), nf.requestedFragmentNumbers());
    }

    @Test
    void buildAndParseNackFrag_sparseFragments() {
        GuidPrefix guidPrefix = testGuidPrefix();
        Set<Integer> missingFragments = Set.of(1, 10, 20, 30);

        RtpsMessageBuilder builder = new RtpsMessageBuilder(guidPrefix);
        builder.nackFrag(READER_ID, WRITER_ID, 5L, missingFragments, 7);

        byte[] packet = builder.bytes();
        List<NackFrag> nackFrags = RtpsUserDataParser.readNackFrags(packet, packet.length);

        assertEquals(1, nackFrags.size());
        NackFrag nf = nackFrags.get(0);

        assertEquals(Set.of(1, 10, 20, 30), nf.requestedFragmentNumbers());
    }

    @Test
    void buildAndParseNackFrag_largeSequenceNumber() {
        GuidPrefix guidPrefix = testGuidPrefix();
        long largeSeq = 0x1_0000_0001L;

        RtpsMessageBuilder builder = new RtpsMessageBuilder(guidPrefix);
        builder.nackFrag(READER_ID, WRITER_ID, largeSeq, Set.of(1), 1);

        byte[] packet = builder.bytes();
        List<NackFrag> nackFrags = RtpsUserDataParser.readNackFrags(packet, packet.length);

        assertEquals(1, nackFrags.size());
        assertEquals(largeSeq, nackFrags.get(0).writerSequenceNumber());
    }
}
