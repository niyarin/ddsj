package ddsj.rtps.discovery;

import ddsj.rtps.types.Guid;
import ddsj.rtps.types.GuidPrefix;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public final class RemoteEndpointStore<T extends RemoteEndpoint> implements Iterable<T> {
    private final ConcurrentHashMap<Guid, T> endpointsByGuid = new ConcurrentHashMap<>();

    public void upsert(T endpoint) {
        endpointsByGuid.put(endpoint.endpointGuid(), endpoint);
    }

    public Optional<T> get(Guid endpointGuid) {
        return Optional.ofNullable(endpointsByGuid.get(endpointGuid));
    }

    public Optional<T> remove(Guid endpointGuid) {
        return Optional.ofNullable(endpointsByGuid.remove(endpointGuid));
    }

    public void removeByParticipant(GuidPrefix guidPrefix) {
        endpointsByGuid.keySet().removeIf(guid -> guid.prefix().equals(guidPrefix));
    }

    public List<T> snapshot() {
        return endpointsByGuid.values().stream().toList();
    }

    @Override
    public Iterator<T> iterator() {
        return snapshot().iterator();
    }
}
