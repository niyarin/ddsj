package ddsj.dds.qos;

import ddsj.dds.qos.policies.*;

import java.util.Objects;

/**
 * QoS policies for a DataWriter.
 */
public record DataWriterQos(
        ReliabilityQosPolicy reliability,
        DurabilityQosPolicy durability,
        HistoryQosPolicy history,
        DeadlineQosPolicy deadline,
        OwnershipQosPolicy ownership,
        OwnershipStrengthQosPolicy ownershipStrength,
        LivelinessQosPolicy liveliness,
        LatencyBudgetQosPolicy latencyBudget,
        ResourceLimitsQosPolicy resourceLimits
) {
    /** Default DataWriter QoS */
    public static final DataWriterQos DEFAULT = builder().build();

    public DataWriterQos {
        Objects.requireNonNull(reliability, "reliability");
        Objects.requireNonNull(durability, "durability");
        Objects.requireNonNull(history, "history");
        Objects.requireNonNull(deadline, "deadline");
        Objects.requireNonNull(ownership, "ownership");
        Objects.requireNonNull(ownershipStrength, "ownershipStrength");
        Objects.requireNonNull(liveliness, "liveliness");
        Objects.requireNonNull(latencyBudget, "latencyBudget");
        Objects.requireNonNull(resourceLimits, "resourceLimits");
    }

    /**
     * Creates a builder initialized with default values.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a builder initialized with this QoS's values.
     *
     * @return a new builder
     */
    public Builder toBuilder() {
        return new Builder(this);
    }

    /**
     * Builder for DataWriterQos.
     */
    public static final class Builder {
        private ReliabilityQosPolicy reliability = ReliabilityQosPolicy.reliable();
        private DurabilityQosPolicy durability = DurabilityQosPolicy.volatile_();
        private HistoryQosPolicy history = HistoryQosPolicy.keepLast();
        private DeadlineQosPolicy deadline = DeadlineQosPolicy.infinite();
        private OwnershipQosPolicy ownership = OwnershipQosPolicy.shared();
        private OwnershipStrengthQosPolicy ownershipStrength = OwnershipStrengthQosPolicy.defaultStrength();
        private LivelinessQosPolicy liveliness = LivelinessQosPolicy.automatic();
        private LatencyBudgetQosPolicy latencyBudget = LatencyBudgetQosPolicy.zero();
        private ResourceLimitsQosPolicy resourceLimits = ResourceLimitsQosPolicy.unlimited();

        private Builder() {}

        private Builder(DataWriterQos qos) {
            this.reliability = qos.reliability;
            this.durability = qos.durability;
            this.history = qos.history;
            this.deadline = qos.deadline;
            this.ownership = qos.ownership;
            this.ownershipStrength = qos.ownershipStrength;
            this.liveliness = qos.liveliness;
            this.latencyBudget = qos.latencyBudget;
            this.resourceLimits = qos.resourceLimits;
        }

        public Builder reliability(ReliabilityQosPolicy reliability) {
            this.reliability = Objects.requireNonNull(reliability, "reliability");
            return this;
        }

        public Builder durability(DurabilityQosPolicy durability) {
            this.durability = Objects.requireNonNull(durability, "durability");
            return this;
        }

        public Builder history(HistoryQosPolicy history) {
            this.history = Objects.requireNonNull(history, "history");
            return this;
        }

        public Builder deadline(DeadlineQosPolicy deadline) {
            this.deadline = Objects.requireNonNull(deadline, "deadline");
            return this;
        }

        public Builder ownership(OwnershipQosPolicy ownership) {
            this.ownership = Objects.requireNonNull(ownership, "ownership");
            return this;
        }

        public Builder ownershipStrength(OwnershipStrengthQosPolicy ownershipStrength) {
            this.ownershipStrength = Objects.requireNonNull(ownershipStrength, "ownershipStrength");
            return this;
        }

        public Builder liveliness(LivelinessQosPolicy liveliness) {
            this.liveliness = Objects.requireNonNull(liveliness, "liveliness");
            return this;
        }

        public Builder latencyBudget(LatencyBudgetQosPolicy latencyBudget) {
            this.latencyBudget = Objects.requireNonNull(latencyBudget, "latencyBudget");
            return this;
        }

        public Builder resourceLimits(ResourceLimitsQosPolicy resourceLimits) {
            this.resourceLimits = Objects.requireNonNull(resourceLimits, "resourceLimits");
            return this;
        }

        public DataWriterQos build() {
            return new DataWriterQos(
                    reliability, durability, history, deadline,
                    ownership, ownershipStrength, liveliness,
                    latencyBudget, resourceLimits
            );
        }
    }
}
