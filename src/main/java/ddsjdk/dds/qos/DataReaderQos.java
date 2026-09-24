package ddsjdk.dds.qos;

import ddsjdk.dds.qos.policies.*;

import java.util.Objects;

/**
 * QoS policies for a DataReader.
 */
public record DataReaderQos(
        ReliabilityQosPolicy reliability,
        DurabilityQosPolicy durability,
        HistoryQosPolicy history,
        DeadlineQosPolicy deadline,
        OwnershipQosPolicy ownership,
        LivelinessQosPolicy liveliness,
        TimeBasedFilterQosPolicy timeBasedFilter,
        DestinationOrderQosPolicy destinationOrder,
        LatencyBudgetQosPolicy latencyBudget,
        ResourceLimitsQosPolicy resourceLimits
) {
    /** Default DataReader QoS */
    public static final DataReaderQos DEFAULT = builder().build();

    public DataReaderQos {
        Objects.requireNonNull(reliability, "reliability");
        Objects.requireNonNull(durability, "durability");
        Objects.requireNonNull(history, "history");
        Objects.requireNonNull(deadline, "deadline");
        Objects.requireNonNull(ownership, "ownership");
        Objects.requireNonNull(liveliness, "liveliness");
        Objects.requireNonNull(timeBasedFilter, "timeBasedFilter");
        Objects.requireNonNull(destinationOrder, "destinationOrder");
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
     * Builder for DataReaderQos.
     */
    public static final class Builder {
        private ReliabilityQosPolicy reliability = ReliabilityQosPolicy.bestEffort();
        private DurabilityQosPolicy durability = DurabilityQosPolicy.volatile_();
        private HistoryQosPolicy history = HistoryQosPolicy.keepLast();
        private DeadlineQosPolicy deadline = DeadlineQosPolicy.infinite();
        private OwnershipQosPolicy ownership = OwnershipQosPolicy.shared();
        private LivelinessQosPolicy liveliness = LivelinessQosPolicy.automatic();
        private TimeBasedFilterQosPolicy timeBasedFilter = TimeBasedFilterQosPolicy.none();
        private DestinationOrderQosPolicy destinationOrder = DestinationOrderQosPolicy.byReceptionTimestamp();
        private LatencyBudgetQosPolicy latencyBudget = LatencyBudgetQosPolicy.zero();
        private ResourceLimitsQosPolicy resourceLimits = ResourceLimitsQosPolicy.unlimited();

        private Builder() {}

        private Builder(DataReaderQos qos) {
            this.reliability = qos.reliability;
            this.durability = qos.durability;
            this.history = qos.history;
            this.deadline = qos.deadline;
            this.ownership = qos.ownership;
            this.liveliness = qos.liveliness;
            this.timeBasedFilter = qos.timeBasedFilter;
            this.destinationOrder = qos.destinationOrder;
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

        public Builder liveliness(LivelinessQosPolicy liveliness) {
            this.liveliness = Objects.requireNonNull(liveliness, "liveliness");
            return this;
        }

        public Builder timeBasedFilter(TimeBasedFilterQosPolicy timeBasedFilter) {
            this.timeBasedFilter = Objects.requireNonNull(timeBasedFilter, "timeBasedFilter");
            return this;
        }

        public Builder destinationOrder(DestinationOrderQosPolicy destinationOrder) {
            this.destinationOrder = Objects.requireNonNull(destinationOrder, "destinationOrder");
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

        public DataReaderQos build() {
            return new DataReaderQos(
                    reliability, durability, history, deadline,
                    ownership, liveliness, timeBasedFilter,
                    destinationOrder, latencyBudget, resourceLimits
            );
        }
    }
}
