package ddsj.rtps.history;

/** Local sample-count limit, independent of the advertised history QoS. */
public record ResourceLimits(int maxSamples) {
    public static final ResourceLimits DEFAULT = new ResourceLimits(128);

    public ResourceLimits {
        if (maxSamples <= 0) {
            throw new IllegalArgumentException("maxSamples must be positive");
        }
    }
}
