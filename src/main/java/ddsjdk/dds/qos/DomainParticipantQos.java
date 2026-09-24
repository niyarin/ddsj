package ddsjdk.dds.qos;

/**
 * QoS policies for a DomainParticipant.
 * <p>
 * Currently minimal; may be extended with UserData, EntityFactory, etc.
 */
public record DomainParticipantQos() {
    /** Default DomainParticipant QoS */
    public static final DomainParticipantQos DEFAULT = new DomainParticipantQos();

    /**
     * Creates a builder initialized with default values.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for DomainParticipantQos.
     */
    public static final class Builder {
        private Builder() {}

        public DomainParticipantQos build() {
            return new DomainParticipantQos();
        }
    }
}
