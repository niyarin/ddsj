package ddsjdk.rtps.message;

import ddsjdk.rtps.types.EntityId;
import ddsjdk.rtps.types.GuidPrefix;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class FragmentSenderTest {
    private final ddsjdk.rtps.history.WriterHistoryCache history = new ddsjdk.rtps.history.WriterHistoryCache(1);

    private static final GuidPrefix GUID_PREFIX = new GuidPrefix(new byte[]{
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06,
            0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c
    });
    private static final EntityId READER_ID = new EntityId(new byte[]{0x00, 0x00, 0x02, 0x04});
    private static final EntityId WRITER_ID = new EntityId(new byte[]{0x00, 0x00, 0x02, 0x03});

    @Test
    void requiresFragmentation_belowThreshold() {
        FragmentSender sender = new FragmentSender(1024, 64000);
        assertFalse(sender.requiresFragmentation(new byte[1000]));
        assertFalse(sender.requiresFragmentation(new byte[64000]));
    }

    @Test
    void requiresFragmentation_aboveThreshold() {
        FragmentSender sender = new FragmentSender(1024, 64000);
        assertTrue(sender.requiresFragmentation(new byte[64001]));
        assertTrue(sender.requiresFragmentation(new byte[100000]));
    }

    @Test
    void sendFragmented_splitsIntoCorrectNumberOfFragments() throws Exception {
        FragmentSender sender = new FragmentSender(100, 50, history::get); // fragment size 100, threshold 50

        byte[] payload = new byte[250]; // Should split into 3 fragments (100 + 100 + 50)
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) i;
        }

        List<byte[]> sentMessages = new ArrayList<>();
        history.put(1L, payload);
        sender.sendFragmented(GUID_PREFIX, READER_ID, WRITER_ID, 1L, payload, sentMessages::add);

        assertEquals(3, sentMessages.size());
        assertTrue(sender.hasFragmentedSample(1L));
        assertEquals(Optional.of(3), sender.getTotalFragments(1L));
    }

    @Test
    void sendFragmented_fragmentsCanBeReassembled() throws Exception {
        FragmentSender sender = new FragmentSender(100, 50, history::get);
        FragmentAssembler assembler = new FragmentAssembler();

        byte[] payload = new byte[250];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) i;
        }

        List<byte[]> sentMessages = new ArrayList<>();
        history.put(1L, payload);
        sender.sendFragmented(GUID_PREFIX, READER_ID, WRITER_ID, 1L, payload, sentMessages::add);

        // Parse and reassemble the fragments
        Optional<UserDataSample> result = Optional.empty();
        for (byte[] msg : sentMessages) {
            List<DataFragment> fragments = RtpsUserDataParser.readDataFragments(msg, msg.length, READER_ID);
            for (DataFragment frag : fragments) {
                result = assembler.addFragment(frag);
            }
        }

        assertTrue(result.isPresent());
        assertArrayEquals(payload, result.get().payload());
    }

    @Test
    void resendFragments_resendsRequestedFragments() throws Exception {
        FragmentSender sender = new FragmentSender(100, 50, history::get);

        byte[] payload = new byte[250];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) i;
        }

        // Send initial fragments
        history.put(1L, payload);
        sender.sendFragmented(GUID_PREFIX, READER_ID, WRITER_ID, 1L, payload, msg -> {});

        // Request resend of fragment 2
        List<byte[]> resentMessages = new ArrayList<>();
        boolean success = sender.resendFragments(
                GUID_PREFIX, READER_ID, WRITER_ID, 1L,
                Set.of(2), resentMessages::add);

        assertTrue(success);
        assertEquals(1, resentMessages.size());

        // Verify it's fragment 2
        List<DataFragment> fragments = RtpsUserDataParser.readDataFragments(
                resentMessages.get(0), resentMessages.get(0).length, READER_ID);
        assertEquals(1, fragments.size());
        assertEquals(2, fragments.get(0).fragmentStartingNum());
    }

    @Test
    void resendFragments_returnsFalseForUnknownSample() throws Exception {
        FragmentSender sender = new FragmentSender();

        List<byte[]> messages = new ArrayList<>();
        boolean success = sender.resendFragments(
                GUID_PREFIX, READER_ID, WRITER_ID, 999L,
                Set.of(1, 2), messages::add);

        assertFalse(success);
        assertTrue(messages.isEmpty());
    }

    @Test
    void sendFragmentedPropagatesFailureAndStopsSending() {
        FragmentSender sender = new FragmentSender(100, 50);
        IOException failure = new IOException("send failed");
        List<byte[]> attempted = new ArrayList<>();

        IOException actual = assertThrows(IOException.class, () -> sender.sendFragmented(
                GUID_PREFIX, READER_ID, WRITER_ID, 1L, new byte[250], message -> {
                    attempted.add(message);
                    if (attempted.size() == 2) throw failure;
                }));

        assertSame(failure, actual);
        assertEquals(2, attempted.size());
    }

    @Test
    void resendFragmentsPropagatesFailureAndStopsSending() {
        FragmentSender sender = new FragmentSender(100, 50, history::get);
        history.put(1L, new byte[250]);
        IOException failure = new IOException("resend failed");
        List<byte[]> attempted = new ArrayList<>();

        IOException actual = assertThrows(IOException.class, () -> sender.resendFragments(
                GUID_PREFIX, READER_ID, WRITER_ID, 1L,
                new LinkedHashSet<>(List.of(1, 2, 3)), message -> {
                    attempted.add(message);
                    if (attempted.size() == 2) throw failure;
                }));

        assertSame(failure, actual);
        assertEquals(2, attempted.size());
    }

    @Test
    void buildHeartbeatFrag_returnsMessageForCachedSample() throws Exception {
        FragmentSender sender = new FragmentSender(100, 50, history::get);

        byte[] payload = new byte[250];
        history.put(1L, payload);
        sender.sendFragmented(GUID_PREFIX, READER_ID, WRITER_ID, 1L, payload, msg -> {});

        Optional<byte[]> heartbeatFrag = sender.buildHeartbeatFrag(
                GUID_PREFIX, READER_ID, WRITER_ID, 1L);

        assertTrue(heartbeatFrag.isPresent());

        // Parse and verify
        List<HeartbeatFrag> hbFrags = RtpsUserDataParser.readHeartbeatFrags(
                heartbeatFrag.get(), heartbeatFrag.get().length, READER_ID);
        assertEquals(1, hbFrags.size());
        assertEquals(1L, hbFrags.get(0).writerSequenceNumber());
        assertEquals(3, hbFrags.get(0).lastFragmentNum()); // 3 fragments total
    }

    @Test
    void buildHeartbeatFrag_returnsEmptyForUnknownSample() {
        FragmentSender sender = new FragmentSender();

        Optional<byte[]> heartbeatFrag = sender.buildHeartbeatFrag(
                GUID_PREFIX, READER_ID, WRITER_ID, 999L);

        assertTrue(heartbeatFrag.isEmpty());
    }

    @Test
    void writerHistoryEvictionRemovesFragmentRetransmission() throws Exception {
        FragmentSender sender = new FragmentSender(100, 50, history::get);

        byte[] payload = new byte[250];
        history.put(1L, payload);
        sender.sendFragmented(GUID_PREFIX, READER_ID, WRITER_ID, 1L, payload, msg -> {});

        assertTrue(sender.hasFragmentedSample(1L));

        history.put(2L, new byte[250]);
        assertFalse(sender.resendFragments(GUID_PREFIX, READER_ID, WRITER_ID, 1L, Set.of(1), message -> fail("evicted sample retransmitted")));
        assertTrue(sender.buildHeartbeatFrag(GUID_PREFIX, READER_ID, WRITER_ID, 1L).isEmpty());

        assertFalse(sender.hasFragmentedSample(1L));
    }
}
