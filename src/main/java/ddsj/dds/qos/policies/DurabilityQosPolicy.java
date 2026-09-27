package ddsj.dds.qos.policies;

import java.util.Objects;

/**
 * Durability QoS policy controlling data persistence.
 * <p>
 * Durability determines whether late-joining readers can receive
 * data that was written before they joined.
 *
 * @param kind the durability kind
 */
public record DurabilityQosPolicy(Kind kind) {

    public DurabilityQosPolicy {
        Objects.requireNonNull(kind, "kind");
    }

    /**
     * Creates a VOLATILE durability policy.
     * <p>
     * Data is not persisted; late-joining readers do not receive old samples.
     *
     * @return a volatile policy
     */
    public static DurabilityQosPolicy volatile_() {
        return new DurabilityQosPolicy(Kind.VOLATILE);
    }

    /**
     * Creates a TRANSIENT_LOCAL durability policy.
     * <p>
     * Data is kept in the DataWriter's memory. Late-joining readers
     * can receive samples that are still in the writer's history.
     *
     * @return a transient local policy
     */
    public static DurabilityQosPolicy transientLocal() {
        return new DurabilityQosPolicy(Kind.TRANSIENT_LOCAL);
    }

    /**
     * Creates a TRANSIENT durability policy.
     * <p>
     * Data outlives the DataWriter but not the system.
     *
     * @return a transient policy
     */
    public static DurabilityQosPolicy transient_() {
        return new DurabilityQosPolicy(Kind.TRANSIENT);
    }

    /**
     * Creates a PERSISTENT durability policy.
     * <p>
     * Data outlives the system (stored to permanent storage).
     *
     * @return a persistent policy
     */
    public static DurabilityQosPolicy persistent() {
        return new DurabilityQosPolicy(Kind.PERSISTENT);
    }

    public enum Kind {
        /** Data not persisted; late joiners miss old samples */
        VOLATILE,
        /** Data kept in writer memory */
        TRANSIENT_LOCAL,
        /** Data outlives writer but not system */
        TRANSIENT,
        /** Data stored to permanent storage */
        PERSISTENT
    }
}
