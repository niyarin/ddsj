package ddsjdk.rtps.discovery;

import ddsjdk.rtps.qos.EndpointQos;
import ddsjdk.rtps.types.Guid;

public record RemotePublication(
        Guid endpointGuid,
        String topicName,
        String typeName,
        EndpointQos qos) implements RemoteEndpoint {
}
