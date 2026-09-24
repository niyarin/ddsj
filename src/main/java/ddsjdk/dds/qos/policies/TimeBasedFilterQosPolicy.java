package ddsjdk.dds.qos.policies;

import java.time.Duration;
import java.util.Objects;

/**
 * Time-based filter QoS policy for DataReaders.
 * <p>
 * Specifies the minimum separation between samples that the DataReader
 * wants to receive. Samples arriving faster than this rate are filtered out.
 *
 * @param minimumSeparation the minimum time between samples
 */
public record TimeBasedFilterQosPolicy(Duration minimumSeparation) {

    /** No filtering (receive all samples) */
    public static final Duration ZERO = Duration.ZERO;

    public TimeBasedFilterQosPolicy {
        Objects.requireNonNull(minimumSeparation, "minimumSeparation");
        if (minimumSeparation.isNegative()) {
            throw new IllegalArgumentException("minimumSeparation must not be negative");
        }
    }

    /**
     * Creates a policy with no filtering (all samples received).
     *
     * @return a no-filter policy
     */
    public static TimeBasedFilterQosPolicy none() {
        return new TimeBasedFilterQosPolicy(ZERO);
    }

    /**
     * Creates a policy with the specified minimum separation.
     *
     * @param minimumSeparation the minimum time between samples
     * @return a time-based filter policy
     */
    public static TimeBasedFilterQosPolicy of(Duration minimumSeparation) {
        return new TimeBasedFilterQosPolicy(minimumSeparation);
    }

    /**
     * Returns true if this filter is disabled (zero separation).
     *
     * @return true if no filtering
     */
    public boolean isDisabled() {
        return minimumSeparation.isZero();
    }
}
