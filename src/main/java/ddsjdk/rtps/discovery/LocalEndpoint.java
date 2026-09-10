package ddsjdk.rtps.discovery;

public record LocalEndpoint(String topicName, String typeName, EndpointQos qos) {
    public LocalEndpoint {
        if (topicName == null || topicName.isBlank()) {
            throw new IllegalArgumentException("topicName must not be blank");
        }
        if (typeName == null || typeName.isBlank()) {
            throw new IllegalArgumentException("typeName must not be blank");
        }
        qos = qos == null ? EndpointQos.DEFAULT : qos;
    }
}
