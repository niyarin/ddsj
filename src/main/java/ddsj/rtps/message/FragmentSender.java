package ddsj.rtps.message;

import ddsj.rtps.types.EntityId;
import ddsj.rtps.types.GuidPrefix;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.LongFunction;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Handles fragmenting large payloads into DATA_FRAG submessages.
 */
public final class FragmentSender {
    /** Sends a built RTPS message, propagating transport failures to the caller. */
    @FunctionalInterface
    public interface MessageSender {
        void send(byte[] message) throws IOException;
    }

    /** Default fragment size (1024 bytes, conservative for UDP) */
    public static final int DEFAULT_FRAGMENT_SIZE = 1024;

    /** Threshold above which fragmentation is used */
    public static final int DEFAULT_FRAGMENTATION_THRESHOLD = 64000;

    private final int fragmentSize;
    private final int fragmentationThreshold;
    private final LongFunction<Optional<byte[]>> payloadLookup;
    private final AtomicInteger heartbeatFragCount = new AtomicInteger(1);

    public FragmentSender() {
        this(DEFAULT_FRAGMENT_SIZE, DEFAULT_FRAGMENTATION_THRESHOLD);
    }

    public FragmentSender(int fragmentSize, int fragmentationThreshold) {
        this(fragmentSize, fragmentationThreshold, ignored -> Optional.empty());
    }

    /** The lookup borrows samples from the writer history; this sender retains no payloads. */
    public FragmentSender(int fragmentSize, int fragmentationThreshold, LongFunction<Optional<byte[]>> payloadLookup) {
        this.payloadLookup = Objects.requireNonNull(payloadLookup);
        if (fragmentSize <= 0) {
            throw new IllegalArgumentException("fragmentSize must be positive");
        }
        this.fragmentSize = fragmentSize;
        this.fragmentationThreshold = fragmentationThreshold;
    }

    /**
     * Checks if a payload requires fragmentation.
     */
    public boolean requiresFragmentation(byte[] payload) {
        return payload.length > fragmentationThreshold;
    }

    /**
     * Sends a payload as DATA_FRAG submessages.
     *
     * @param guidPrefix the writer's GUID prefix
     * @param readerId target reader entity ID
     * @param writerId writer entity ID
     * @param sequenceNumber the sample's sequence number
     * @param payload the full payload to fragment
     * @param sender callback to send each built message
     * @throws IOException if sending fails; remaining fragments are not sent
     */
    public void sendFragmented(
            GuidPrefix guidPrefix,
            EntityId readerId,
            EntityId writerId,
            long sequenceNumber,
            byte[] payload,
            MessageSender sender) throws IOException {

        int sampleSize = payload.length;
        int totalFragments = (sampleSize + fragmentSize - 1) / fragmentSize;

        // Send all fragments
        for (int fragNum = 1; fragNum <= totalFragments; fragNum++) {
            byte[] fragMessage = buildFragmentMessage(
                    guidPrefix, readerId, writerId, sequenceNumber,
                    fragNum, payload, sampleSize);
            sender.send(fragMessage);
        }
    }

    /**
     * Resends specific fragments for a sequence number.
     *
     * @return true if fragments were sent, false if sample not found
     * @throws IOException if sending fails; remaining fragments are not sent
     */
    public boolean resendFragments(
            GuidPrefix guidPrefix,
            EntityId readerId,
            EntityId writerId,
            long sequenceNumber,
            Set<Integer> fragmentNumbers,
            MessageSender sender) throws IOException {

        FragmentedSample sample = lookup(sequenceNumber);
        if (sample == null) {
            return false;
        }

        for (int fragNum : fragmentNumbers) {
            if (fragNum < 1 || fragNum > sample.totalFragments) {
                continue;
            }
            byte[] fragMessage = buildFragmentMessage(
                    guidPrefix, readerId, writerId, sequenceNumber,
                    fragNum, sample.payload, sample.payload.length);
            sender.send(fragMessage);
        }
        return true;
    }

    /**
     * Builds a HEARTBEAT_FRAG message for a fragmented sample.
     */
    public Optional<byte[]> buildHeartbeatFrag(
            GuidPrefix guidPrefix,
            EntityId readerId,
            EntityId writerId,
            long sequenceNumber) {

        FragmentedSample sample = lookup(sequenceNumber);
        if (sample == null) {
            return Optional.empty();
        }

        RtpsMessageBuilder builder = new RtpsMessageBuilder(guidPrefix);
        builder.heartbeatFrag(readerId, writerId, sequenceNumber,
                sample.totalFragments, heartbeatFragCount.getAndIncrement());
        return Optional.of(builder.bytes());
    }

    /**
     * Checks whether the supplied history retains a fragmented sample.
     */
    public boolean hasFragmentedSample(long sequenceNumber) {
        return lookup(sequenceNumber) != null;
    }

    /**
     * Returns the total number of fragments for a sample retained in the supplied history.
     */
    public Optional<Integer> getTotalFragments(long sequenceNumber) {
        FragmentedSample sample = lookup(sequenceNumber);
        return sample == null ? Optional.empty() : Optional.of(sample.totalFragments);
    }

    /**
     * Returns the fragment size used by this sender.
     */
    public int fragmentSize() {
        return fragmentSize;
    }

    /**
     * Returns the fragmentation threshold.
     */
    public int fragmentationThreshold() {
        return fragmentationThreshold;
    }

    private byte[] buildFragmentMessage(
            GuidPrefix guidPrefix,
            EntityId readerId,
            EntityId writerId,
            long sequenceNumber,
            int fragmentNum,
            byte[] payload,
            int sampleSize) {

        int startOffset = (fragmentNum - 1) * fragmentSize;
        int endOffset = Math.min(startOffset + fragmentSize, sampleSize);
        byte[] fragmentData = Arrays.copyOfRange(payload, startOffset, endOffset);

        RtpsMessageBuilder builder = new RtpsMessageBuilder(guidPrefix);
        builder.dataFrag(readerId, writerId, sequenceNumber,
                fragmentNum, 1, fragmentSize, sampleSize, fragmentData);
        return builder.bytes();
    }

    private FragmentedSample lookup(long sequenceNumber) {
        return payloadLookup.apply(sequenceNumber)
                .filter(this::requiresFragmentation)
                .map(payload -> new FragmentedSample(payload, (payload.length + fragmentSize - 1) / fragmentSize))
                .orElse(null);
    }

    private record FragmentedSample(byte[] payload, int totalFragments) {}
}
