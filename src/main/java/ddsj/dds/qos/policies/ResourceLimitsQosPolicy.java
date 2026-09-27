package ddsj.dds.qos.policies;

/**
 * Resource limits QoS policy controlling cache sizes.
 * <p>
 * Specifies the maximum resources that the DataWriter or DataReader
 * can allocate for sample storage.
 *
 * @param maxSamples maximum number of samples across all instances
 * @param maxInstances maximum number of instances
 * @param maxSamplesPerInstance maximum samples per instance
 */
public record ResourceLimitsQosPolicy(int maxSamples, int maxInstances, int maxSamplesPerInstance) {

    /** Unlimited resource value */
    public static final int UNLIMITED = Integer.MAX_VALUE;

    /** Default policy with unlimited resources */
    public static final ResourceLimitsQosPolicy UNLIMITED_POLICY = new ResourceLimitsQosPolicy(UNLIMITED, UNLIMITED, UNLIMITED);

    public ResourceLimitsQosPolicy {
        if (maxSamples <= 0 && maxSamples != UNLIMITED) {
            throw new IllegalArgumentException("maxSamples must be positive or UNLIMITED");
        }
        if (maxInstances <= 0 && maxInstances != UNLIMITED) {
            throw new IllegalArgumentException("maxInstances must be positive or UNLIMITED");
        }
        if (maxSamplesPerInstance <= 0 && maxSamplesPerInstance != UNLIMITED) {
            throw new IllegalArgumentException("maxSamplesPerInstance must be positive or UNLIMITED");
        }
    }

    /**
     * Creates an unlimited resource limits policy.
     *
     * @return an unlimited policy
     */
    public static ResourceLimitsQosPolicy unlimited() {
        return UNLIMITED_POLICY;
    }

    /**
     * Creates a resource limits policy with the specified limits.
     *
     * @param maxSamples maximum samples
     * @param maxInstances maximum instances
     * @param maxSamplesPerInstance maximum samples per instance
     * @return a resource limits policy
     */
    public static ResourceLimitsQosPolicy of(int maxSamples, int maxInstances, int maxSamplesPerInstance) {
        return new ResourceLimitsQosPolicy(maxSamples, maxInstances, maxSamplesPerInstance);
    }

    /**
     * Returns true if all limits are unlimited.
     *
     * @return true if unlimited
     */
    public boolean isUnlimited() {
        return maxSamples == UNLIMITED && maxInstances == UNLIMITED && maxSamplesPerInstance == UNLIMITED;
    }
}
