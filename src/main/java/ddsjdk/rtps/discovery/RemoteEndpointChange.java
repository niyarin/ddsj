package ddsjdk.rtps.discovery;

import ddsjdk.rtps.types.Guid;

import java.util.Optional;

public record RemoteEndpointChange<T extends RemoteEndpoint>(
        Guid endpointGuid,
        Optional<T> endpoint,
        boolean disposedOrUnregistered) {
    public RemoteEndpointChange {
        endpoint = endpoint == null ? Optional.empty() : endpoint;
    }
}
