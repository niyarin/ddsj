package ddsj.dds.qos;

import ddsj.dds.qos.policies.PartitionQosPolicy;

import java.util.Objects;

/**
 * QoS policies for a Publisher.
 */
public record PublisherQos(
        PartitionQosPolicy partition
) {
    /** Default Publisher QoS */
    public static final PublisherQos DEFAULT = builder().build();

    public PublisherQos {
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
     * Builder for PublisherQos.
     */
    public static final class Builder {
        private PartitionQosPolicy partition = PartitionQosPolicy.DEFAULT;

        private Builder() {}

        private Builder(PublisherQos qos) {
            this.partition = qos.partition;
        }

        public Builder partition(PartitionQosPolicy partition) {
            this.partition = Objects.requireNonNull(partition, "partition");
            return this;
        }

        public PublisherQos build() {
            return new PublisherQos(partition);
        }
    }
}
