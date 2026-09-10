package ddsjdk.rtps.discovery;

import ddsjdk.rtps.types.Guid;

public interface RemoteEndpoint {
    Guid endpointGuid();

    String topicName();

    String typeName();

    EndpointQos qos();
}
