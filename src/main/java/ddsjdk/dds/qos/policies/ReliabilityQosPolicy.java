package ddsjdk.dds.qos.policies;

import java.time.Duration;
import java.util.Objects;

/**
 * Reliability QoS policy controlling data delivery guarantees.
 *
 * @param kind the reliability kind
 * @param maxBlockingTime maximum time to block on write when resources are unavailable (for RELIABLE)
 */
public record ReliabilityQosPolicy(Kind kind, Duration maxBlockingTime) {

    /** Default max blocking time for reliable writers */
    public static final Duration DEFAULT_MAX_BLOCKING_TIME = Duration.ofMillis(100);

    public ReliabilityQosPolicy {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(maxBlockingTime, "maxBlockingTime");
        if (maxBlockingTime.isNegative()) {
            throw new IllegalArgumentException("maxBlockingTime must not be negative");
        }
    }

    /**
     * Creates a BEST_EFFORT reliability policy.
     * <p>
     * Best-effort does not guarantee delivery; samples may be lost.
     *
     * @return a best-effort policy
     */
    public static ReliabilityQosPolicy bestEffort() {
        return new ReliabilityQosPolicy(Kind.BEST_EFFORT, Duration.ZERO);
    }

    /**
     * Creates a RELIABLE reliability policy with default blocking time.
     * <p>
     * Reliable delivery guarantees that samples are delivered in order,
     * retransmitting as necessary.
     *
     * @return a reliable policy
     */
    public static ReliabilityQosPolicy reliable() {
        return new ReliabilityQosPolicy(Kind.RELIABLE, DEFAULT_MAX_BLOCKING_TIME);
    }

    /**
     * Creates a RELIABLE reliability policy with specified blocking time.
     *
     * @param maxBlockingTime maximum blocking time
     * @return a reliable policy
     */
    public static ReliabilityQosPolicy reliable(Duration maxBlockingTime) {
        return new ReliabilityQosPolicy(Kind.RELIABLE, maxBlockingTime);
    }

    public enum Kind {
        /** Samples may be lost; no retransmission */
        BEST_EFFORT,
        /** Samples are guaranteed to be delivered in order */
        RELIABLE
    }
}
