package ddsjdk.dds.qos.policies;

import java.time.Duration;
import java.util.Objects;

/**
 * Latency budget QoS policy specifying delivery urgency.
 * <p>
 * Provides a hint to the service about the acceptable delay
 * between data write and delivery. This is advisory only;
 * the service may not meet the budget.
 *
 * @param duration the acceptable latency
 */
public record LatencyBudgetQosPolicy(Duration duration) {

    public LatencyBudgetQosPolicy {
        Objects.requireNonNull(duration, "duration");
        if (duration.isNegative()) {
            throw new IllegalArgumentException("duration must not be negative");
        }
    }

    /**
     * Creates a zero latency budget (deliver as fast as possible).
     *
     * @return a zero latency policy
     */
    public static LatencyBudgetQosPolicy zero() {
        return new LatencyBudgetQosPolicy(Duration.ZERO);
    }

    /**
     * Creates a latency budget with the specified duration.
     *
     * @param duration the acceptable latency
     * @return a latency budget policy
     */
    public static LatencyBudgetQosPolicy of(Duration duration) {
        return new LatencyBudgetQosPolicy(duration);
    }
}
