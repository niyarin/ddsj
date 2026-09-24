package ddsjdk.dds.qos;

import java.net.NetworkInterface;
import java.util.Objects;
import java.util.Optional;

/**
 * QoS policies for a DomainParticipant.
 *
 * @param participantIndex the participant index for port calculation (0-119)
 * @param networkInterface the network interface to use, or empty for default
 */
public record DomainParticipantQos(
        int participantIndex,
        Optional<NetworkInterface> networkInterface
) {
    /** Default DomainParticipant QoS */
    public static final DomainParticipantQos DEFAULT = new DomainParticipantQos(0, Optional.empty());

    public DomainParticipantQos {
        if (participantIndex < 0 || participantIndex > 119) {
            throw new IllegalArgumentException("participantIndex must be 0-119");
        }
        networkInterface = networkInterface != null ? networkInterface : Optional.empty();
    }

    /**
     * Creates a QoS with the specified participant index.
     *
     * @param participantIndex the participant index
     * @return a new QoS
     */
    public static DomainParticipantQos withParticipantIndex(int participantIndex) {
        return new DomainParticipantQos(participantIndex, Optional.empty());
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
     * Builder for DomainParticipantQos.
     */
    public static final class Builder {
        private int participantIndex = 0;
        private Optional<NetworkInterface> networkInterface = Optional.empty();

        private Builder() {}

        private Builder(DomainParticipantQos qos) {
            this.participantIndex = qos.participantIndex;
            this.networkInterface = qos.networkInterface;
        }

        public Builder participantIndex(int participantIndex) {
            this.participantIndex = participantIndex;
            return this;
        }

        public Builder networkInterface(NetworkInterface networkInterface) {
            this.networkInterface = Optional.ofNullable(networkInterface);
            return this;
        }

        public DomainParticipantQos build() {
            return new DomainParticipantQos(participantIndex, networkInterface);
        }
    }
}
