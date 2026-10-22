package ddsj.rtps.history;

/** Local limits for retained samples and keyed instances. */
public record ResourceLimits(int maxSamples, int maxInstances, int maxSamplesPerInstance) {
    public static final ResourceLimits DEFAULT = new ResourceLimits(128);

    public ResourceLimits(int maxSamples) {
        this(maxSamples, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    public ResourceLimits {
        if (maxSamples <= 0 || maxInstances <= 0 || maxSamplesPerInstance <= 0) {
            throw new IllegalArgumentException("resource limits must be positive");
        }
    }
}
