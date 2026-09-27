package ddsj.dds.qos.policies;

import java.util.Objects;

/**
 * Destination order QoS policy controlling sample ordering.
 * <p>
 * Determines the order in which samples are presented to the DataReader
 * when multiple DataWriters are writing the same instance.
 *
 * @param kind the destination order kind
 */
public record DestinationOrderQosPolicy(Kind kind) {

    public DestinationOrderQosPolicy {
        Objects.requireNonNull(kind, "kind");
    }

    /**
     * Creates a BY_RECEPTION_TIMESTAMP policy.
     * <p>
     * Samples are ordered by the time they were received by the DataReader.
     *
     * @return a by-reception-timestamp policy
     */
    public static DestinationOrderQosPolicy byReceptionTimestamp() {
        return new DestinationOrderQosPolicy(Kind.BY_RECEPTION_TIMESTAMP);
    }

    /**
     * Creates a BY_SOURCE_TIMESTAMP policy.
     * <p>
     * Samples are ordered by the source timestamp set by the DataWriter.
     *
     * @return a by-source-timestamp policy
     */
    public static DestinationOrderQosPolicy bySourceTimestamp() {
        return new DestinationOrderQosPolicy(Kind.BY_SOURCE_TIMESTAMP);
    }

    public enum Kind {
        /** Order by reception time at the reader */
        BY_RECEPTION_TIMESTAMP,
        /** Order by source timestamp from the writer */
        BY_SOURCE_TIMESTAMP
    }
}
