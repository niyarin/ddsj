package ddsjdk.rtps.discovery;

import ddsjdk.rtps.history.ResourceLimits;

public record LocalEndpoint(String topicName, String typeName, EndpointQos qos, ResourceLimits resourceLimits) {
    public LocalEndpoint(String topicName, String typeName, EndpointQos qos) {
        this(topicName, typeName, qos, ResourceLimits.DEFAULT);
    }

    public LocalEndpoint {
        if (topicName == null || topicName.isBlank()) {
            throw new IllegalArgumentException("topicName must not be blank");
        }
        if (typeName == null || typeName.isBlank()) {
            throw new IllegalArgumentException("typeName must not be blank");
        }
        qos = qos == null ? EndpointQos.DEFAULT : qos;
        resourceLimits = resourceLimits == null ? ResourceLimits.DEFAULT : resourceLimits;
        if (qos.history() == EndpointQos.HistoryKind.KEEP_LAST && qos.depth() > resourceLimits.maxSamples()) {
            throw new IllegalArgumentException("history depth exceeds maxSamples");
        }
    }
}
