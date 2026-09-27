package ddsj.rtps.discovery;

import ddsj.rtps.types.Guid;

import java.util.Optional;

public record RemoteEndpointChange<T extends RemoteEndpoint>(
        Guid endpointGuid,
        Optional<T> endpoint,
        boolean disposedOrUnregistered) {
    public RemoteEndpointChange {
        endpoint = endpoint == null ? Optional.empty() : endpoint;
    }
}
