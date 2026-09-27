package ddsj.rtps.discovery;

import ddsj.rtps.qos.EndpointQos;
import ddsj.rtps.types.Guid;

public record RemoteSubscription(
        Guid endpointGuid,
        String topicName,
        String typeName,
        EndpointQos qos) implements RemoteEndpoint {
}
