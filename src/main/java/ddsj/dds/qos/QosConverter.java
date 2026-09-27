package ddsj.dds.qos;

import ddsj.dds.qos.policies.*;
import ddsj.rtps.history.ResourceLimits;
import ddsj.rtps.qos.EndpointQos;

/**
 * Converts between DDS QoS policies and RTPS QoS.
 */
public final class QosConverter {
    private QosConverter() {}

    /**
     * Converts DataWriterQos to RTPS EndpointQos.
     *
     * @param qos the DDS QoS
     * @return the RTPS QoS
     */
    public static EndpointQos toEndpointQos(DataWriterQos qos) {
        return EndpointQos.builder()
                .reliability(convertReliability(qos.reliability()))
                .durability(convertDurability(qos.durability()))
                .history(convertHistory(qos.history()), qos.history().depth())
                .deadline(qos.deadline().period())
                .ownership(convertOwnership(qos.ownership()), qos.ownershipStrength().value())
                .liveliness(convertLiveliness(qos.liveliness()), qos.liveliness().leaseDuration())
                .build();
    }

    /**
     * Converts DataReaderQos to RTPS EndpointQos.
     *
     * @param qos the DDS QoS
     * @return the RTPS QoS
     */
    public static EndpointQos toEndpointQos(DataReaderQos qos) {
        return EndpointQos.builder()
                .reliability(convertReliability(qos.reliability()))
                .durability(convertDurability(qos.durability()))
                .history(convertHistory(qos.history()), qos.history().depth())
                .deadline(qos.deadline().period())
                .ownership(convertOwnership(qos.ownership()), 0)
                .liveliness(convertLiveliness(qos.liveliness()), qos.liveliness().leaseDuration())
                .build();
    }

    /**
     * Converts TopicQos to RTPS EndpointQos.
     *
     * @param qos the DDS QoS
     * @return the RTPS QoS
     */
    public static EndpointQos toEndpointQos(TopicQos qos) {
        return EndpointQos.builder()
                .reliability(convertReliability(qos.reliability()))
                .durability(convertDurability(qos.durability()))
                .history(convertHistory(qos.history()), qos.history().depth())
                .deadline(qos.deadline().period())
                .ownership(convertOwnership(qos.ownership()), 0)
                .liveliness(convertLiveliness(qos.liveliness()), qos.liveliness().leaseDuration())
                .build();
    }

    /**
     * Converts ResourceLimitsQosPolicy to RTPS ResourceLimits.
     * <p>
     * Note: RTPS ResourceLimits only supports maxSamples.
     *
     * @param qos the DDS resource limits
     * @return the RTPS resource limits
     */
    public static ResourceLimits toResourceLimits(ResourceLimitsQosPolicy qos) {
        return new ResourceLimits(qos.maxSamples());
    }

    private static EndpointQos.ReliabilityKind convertReliability(ReliabilityQosPolicy policy) {
        return switch (policy.kind()) {
            case BEST_EFFORT -> EndpointQos.ReliabilityKind.BEST_EFFORT;
            case RELIABLE -> EndpointQos.ReliabilityKind.RELIABLE;
        };
    }

    private static EndpointQos.DurabilityKind convertDurability(DurabilityQosPolicy policy) {
        return switch (policy.kind()) {
            case VOLATILE -> EndpointQos.DurabilityKind.VOLATILE;
            case TRANSIENT_LOCAL, TRANSIENT, PERSISTENT -> EndpointQos.DurabilityKind.TRANSIENT_LOCAL;
        };
    }

    private static EndpointQos.HistoryKind convertHistory(HistoryQosPolicy policy) {
        return switch (policy.kind()) {
            case KEEP_LAST -> EndpointQos.HistoryKind.KEEP_LAST;
            case KEEP_ALL -> EndpointQos.HistoryKind.KEEP_ALL;
        };
    }

    private static EndpointQos.OwnershipKind convertOwnership(OwnershipQosPolicy policy) {
        return switch (policy.kind()) {
            case SHARED -> EndpointQos.OwnershipKind.SHARED;
            case EXCLUSIVE -> EndpointQos.OwnershipKind.EXCLUSIVE;
        };
    }

    private static EndpointQos.LivelinessKind convertLiveliness(LivelinessQosPolicy policy) {
        return switch (policy.kind()) {
            case AUTOMATIC -> EndpointQos.LivelinessKind.AUTOMATIC;
            case MANUAL_BY_PARTICIPANT -> EndpointQos.LivelinessKind.MANUAL_BY_PARTICIPANT;
            case MANUAL_BY_TOPIC -> EndpointQos.LivelinessKind.MANUAL_BY_TOPIC;
        };
    }
}
