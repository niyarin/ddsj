package ddsj.dds.qos.policies;

import java.util.Objects;

/**
 * Ownership QoS policy controlling instance ownership among writers.
 * <p>
 * Determines whether multiple DataWriters can update the same instance
 * simultaneously or whether only the strongest writer owns each instance.
 *
 * @param kind the ownership kind
 */
public record OwnershipQosPolicy(Kind kind) {

    public OwnershipQosPolicy {
        Objects.requireNonNull(kind, "kind");
    }

    /**
     * Creates a SHARED ownership policy.
     * <p>
     * Multiple writers can update the same instance. All updates are delivered.
     *
     * @return a shared ownership policy
     */
    public static OwnershipQosPolicy shared() {
        return new OwnershipQosPolicy(Kind.SHARED);
    }

    /**
     * Creates an EXCLUSIVE ownership policy.
     * <p>
     * Only the writer with the highest ownership strength owns each instance.
     * Updates from non-owning writers are ignored.
     *
     * @return an exclusive ownership policy
     */
    public static OwnershipQosPolicy exclusive() {
        return new OwnershipQosPolicy(Kind.EXCLUSIVE);
    }

    public enum Kind {
        /** Multiple writers can update the same instance */
        SHARED,
        /** Only the highest-strength writer owns each instance */
        EXCLUSIVE
    }
}
