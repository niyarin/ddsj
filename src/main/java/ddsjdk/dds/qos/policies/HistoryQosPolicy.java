package ddsjdk.dds.qos.policies;

import java.util.Objects;

/**
 * History QoS policy controlling sample retention.
 * <p>
 * History determines how many samples are kept per instance
 * in the DataWriter and DataReader caches.
 *
 * @param kind the history kind
 * @param depth the number of samples to keep (for KEEP_LAST)
 */
public record HistoryQosPolicy(Kind kind, int depth) {

    /** Default depth for KEEP_LAST */
    public static final int DEFAULT_DEPTH = 1;

    public HistoryQosPolicy {
        Objects.requireNonNull(kind, "kind");
        if (depth <= 0) {
            throw new IllegalArgumentException("depth must be positive");
        }
    }

    /**
     * Creates a KEEP_LAST history policy with default depth (1).
     *
     * @return a keep-last policy
     */
    public static HistoryQosPolicy keepLast() {
        return new HistoryQosPolicy(Kind.KEEP_LAST, DEFAULT_DEPTH);
    }

    /**
     * Creates a KEEP_LAST history policy with specified depth.
     *
     * @param depth number of samples to keep per instance
     * @return a keep-last policy
     */
    public static HistoryQosPolicy keepLast(int depth) {
        return new HistoryQosPolicy(Kind.KEEP_LAST, depth);
    }

    /**
     * Creates a KEEP_ALL history policy.
     * <p>
     * All samples are kept until taken by the application or removed
     * by resource limits.
     *
     * @return a keep-all policy
     */
    public static HistoryQosPolicy keepAll() {
        return new HistoryQosPolicy(Kind.KEEP_ALL, 1);
    }

    public enum Kind {
        /** Keep only the most recent depth samples per instance */
        KEEP_LAST,
        /** Keep all samples until explicitly removed */
        KEEP_ALL
    }
}
