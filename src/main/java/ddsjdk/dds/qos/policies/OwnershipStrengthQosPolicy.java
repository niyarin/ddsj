package ddsjdk.dds.qos.policies;

/**
 * Ownership strength QoS policy for EXCLUSIVE ownership.
 * <p>
 * When ownership is EXCLUSIVE, the writer with the highest strength
 * value owns each instance. Only applies to DataWriters.
 *
 * @param value the ownership strength value (higher wins)
 */
public record OwnershipStrengthQosPolicy(int value) {

    /** Default ownership strength */
    public static final int DEFAULT_VALUE = 0;

    public OwnershipStrengthQosPolicy {
        if (value < 0) {
            throw new IllegalArgumentException("value must not be negative");
        }
    }

    /**
     * Creates a policy with default strength (0).
     *
     * @return a default strength policy
     */
    public static OwnershipStrengthQosPolicy defaultStrength() {
        return new OwnershipStrengthQosPolicy(DEFAULT_VALUE);
    }

    /**
     * Creates a policy with the specified strength.
     *
     * @param value the strength value
     * @return an ownership strength policy
     */
    public static OwnershipStrengthQosPolicy of(int value) {
        return new OwnershipStrengthQosPolicy(value);
    }
}
