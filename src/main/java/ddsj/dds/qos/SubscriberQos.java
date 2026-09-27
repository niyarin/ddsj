package ddsj.dds.qos;

import ddsj.dds.qos.policies.PartitionQosPolicy;

import java.util.Objects;

/**
 * QoS policies for a Subscriber.
 */
public record SubscriberQos(
        PartitionQosPolicy partition
) {
    /** Default Subscriber QoS */
    public static final SubscriberQos DEFAULT = builder().build();

    public SubscriberQos {
        Objects.requireNonNull(partition, "partition");
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
     * Builder for SubscriberQos.
     */
    public static final class Builder {
        private PartitionQosPolicy partition = PartitionQosPolicy.DEFAULT;

        private Builder() {}

        private Builder(SubscriberQos qos) {
            this.partition = qos.partition;
        }

        public Builder partition(PartitionQosPolicy partition) {
            this.partition = Objects.requireNonNull(partition, "partition");
            return this;
        }

        public SubscriberQos build() {
            return new SubscriberQos(partition);
        }
    }
}
