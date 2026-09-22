package ddsjdk.rtps.runtime;

import ddsjdk.rtps.qos.EndpointQos;
import ddsjdk.rtps.discovery.LocalEndpoint;
import ddsjdk.rtps.discovery.RemoteEndpoint;
import ddsjdk.rtps.discovery.RemoteEndpointStore;
import ddsjdk.rtps.discovery.RemoteParticipant;
import ddsjdk.rtps.discovery.RemoteParticipantStore;
import ddsjdk.rtps.discovery.RemotePublication;
import ddsjdk.rtps.discovery.RemoteSubscription;
import ddsjdk.rtps.types.Guid;
import ddsjdk.rtps.types.GuidPrefix;

import java.util.HashSet;
import java.util.Set;

/** Resolves matches and destinations against the current discovery state. */
final class EndpointResolver {
    private final LocalEndpoint local;
    private final RemoteParticipantStore participants;
    private final RemoteEndpointStore<RemotePublication> publications;
    private final RemoteEndpointStore<RemoteSubscription> subscriptions;

    EndpointResolver(LocalEndpoint local, RemoteParticipantStore participants,
            RemoteEndpointStore<RemotePublication> publications,
            RemoteEndpointStore<RemoteSubscription> subscriptions) {
        this.local = local;
        this.participants = participants;
        this.publications = publications;
        this.subscriptions = subscriptions;
    }

    boolean acceptsPublication(Guid writerGuid, boolean allowUndiscovered) {
        return publications.get(writerGuid)
                .map(remote -> matches(remote, remote.qos(), local.qos()))
                .orElse(allowUndiscovered);
    }

    Set<RemoteParticipant> participantsForPublication(Guid writerGuid, boolean allowUndiscovered) {
        if (!acceptsPublication(writerGuid, allowUndiscovered)) return Set.of();
        return participants.get(writerGuid.prefix()).map(Set::of).orElseGet(Set::of);
    }

    Set<RemoteParticipant> participantsForSubscriptions() {
        Set<GuidPrefix> prefixes = new HashSet<>();
        for (RemoteSubscription remote : subscriptions) {
            if (matches(remote, local.qos(), remote.qos())) {
                prefixes.add(remote.endpointGuid().prefix());
            }
        }
        Set<RemoteParticipant> result = new HashSet<>();
        for (GuidPrefix prefix : prefixes) {
            participants.get(prefix).ifPresent(result::add);
        }
        return result;
    }

    private boolean matches(RemoteEndpoint remote, EndpointQos offered, EndpointQos requested) {
        return remote.topicName().equals(local.topicName())
                && remote.typeName().equals(local.typeName())
                && offered.isCompatibleWithRequested(requested);
    }
}
