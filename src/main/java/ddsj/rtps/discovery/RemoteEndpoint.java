package ddsj.rtps.discovery;

import ddsj.rtps.qos.EndpointQos;
import ddsj.rtps.types.Guid;

public interface RemoteEndpoint {
    Guid endpointGuid();

    String topicName();

    String typeName();

    EndpointQos qos();
}
