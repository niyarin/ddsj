package ddsj.rtps.discovery;

/** A discovery notification in RTPS submessage order. */
public sealed interface DiscoveryChange {
    record Participant(RemoteParticipant participant) implements DiscoveryChange { }

    record Publication(RemoteEndpointChange<RemotePublication> change) implements DiscoveryChange { }

    record Subscription(RemoteEndpointChange<RemoteSubscription> change) implements DiscoveryChange { }
}
