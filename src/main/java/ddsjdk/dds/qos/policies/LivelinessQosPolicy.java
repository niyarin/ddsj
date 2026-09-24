package ddsjdk.dds.qos.policies;

import java.time.Duration;
import java.util.Objects;

/**
 * Liveliness QoS policy controlling writer activity monitoring.
 * <p>
 * Determines how the service detects whether a DataWriter is alive
 * and how readers are notified when writers become inactive.
 *
 * @param kind the liveliness kind
 * @param leaseDuration the lease duration
 */
public record LivelinessQosPolicy(Kind kind, Duration leaseDuration) {

    /** Infinite lease duration (no liveliness checking) */
    public static final Duration INFINITE = Duration.ofSeconds(Integer.MAX_VALUE);

    public LivelinessQosPolicy {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(leaseDuration, "leaseDuration");
        if (leaseDuration.isNegative()) {
            throw new IllegalArgumentException("leaseDuration must not be negative");
        }
    }

    /**
     * Creates an AUTOMATIC liveliness policy with infinite lease.
     * <p>
     * The service automatically asserts liveliness on any writer activity.
     *
     * @return an automatic policy
     */
    public static LivelinessQosPolicy automatic() {
        return new LivelinessQosPolicy(Kind.AUTOMATIC, INFINITE);
    }

    /**
     * Creates an AUTOMATIC liveliness policy with the specified lease duration.
     *
     * @param leaseDuration the lease duration
     * @return an automatic policy
     */
    public static LivelinessQosPolicy automatic(Duration leaseDuration) {
        return new LivelinessQosPolicy(Kind.AUTOMATIC, leaseDuration);
    }

    /**
     * Creates a MANUAL_BY_PARTICIPANT liveliness policy.
     * <p>
     * The application must assert liveliness at the participant level.
     *
     * @param leaseDuration the lease duration
     * @return a manual-by-participant policy
     */
    public static LivelinessQosPolicy manualByParticipant(Duration leaseDuration) {
        return new LivelinessQosPolicy(Kind.MANUAL_BY_PARTICIPANT, leaseDuration);
    }

    /**
     * Creates a MANUAL_BY_TOPIC liveliness policy.
     * <p>
     * The application must assert liveliness for each DataWriter.
     *
     * @param leaseDuration the lease duration
     * @return a manual-by-topic policy
     */
    public static LivelinessQosPolicy manualByTopic(Duration leaseDuration) {
        return new LivelinessQosPolicy(Kind.MANUAL_BY_TOPIC, leaseDuration);
    }

    /**
     * Returns true if this lease duration is infinite.
     *
     * @return true if infinite
     */
    public boolean isInfinite() {
        return leaseDuration.getSeconds() >= Integer.MAX_VALUE;
    }

    public enum Kind {
        /** Service maintains liveliness automatically on activity */
        AUTOMATIC,
        /** Application asserts liveliness at participant level */
        MANUAL_BY_PARTICIPANT,
        /** Application asserts liveliness per DataWriter */
        MANUAL_BY_TOPIC
    }
}
