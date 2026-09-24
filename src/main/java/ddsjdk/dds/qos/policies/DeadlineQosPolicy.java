package ddsjdk.dds.qos.policies;

import java.time.Duration;
import java.util.Objects;

/**
 * Deadline QoS policy specifying the expected data update rate.
 * <p>
 * Writers commit to providing data within the deadline period.
 * Readers expect data within the deadline period and are notified
 * when the deadline is missed.
 *
 * @param period the deadline period
 */
public record DeadlineQosPolicy(Duration period) {

    /** Infinite deadline (no deadline checking) */
    public static final Duration INFINITE = Duration.ofSeconds(Integer.MAX_VALUE);

    public DeadlineQosPolicy {
        Objects.requireNonNull(period, "period");
        if (period.isNegative()) {
            throw new IllegalArgumentException("period must not be negative");
        }
    }

    /**
     * Creates a policy with infinite deadline (no deadline checking).
     *
     * @return an infinite deadline policy
     */
    public static DeadlineQosPolicy infinite() {
        return new DeadlineQosPolicy(INFINITE);
    }

    /**
     * Creates a policy with the specified deadline period.
     *
     * @param period the deadline period
     * @return a deadline policy
     */
    public static DeadlineQosPolicy of(Duration period) {
        return new DeadlineQosPolicy(period);
    }

    /**
     * Returns true if this deadline is infinite.
     *
     * @return true if infinite
     */
    public boolean isInfinite() {
        return period.getSeconds() >= Integer.MAX_VALUE;
    }
}
