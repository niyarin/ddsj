package ddsjdk.rtps.runtime;

import ddsjdk.rtps.qos.EndpointQos;
import ddsjdk.rtps.discovery.LocalEndpoint;
import ddsjdk.rtps.discovery.RemoteEndpointStore;
import ddsjdk.rtps.discovery.RemoteParticipant;
import ddsjdk.rtps.discovery.RemoteParticipantStore;
import ddsjdk.rtps.discovery.RemotePublication;
import ddsjdk.rtps.discovery.RemoteSubscription;
import ddsjdk.rtps.protocol.RtpsEntity;
import ddsjdk.rtps.types.Guid;
import ddsjdk.rtps.types.GuidPrefix;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static ddsjdk.rtps.qos.EndpointQos.ReliabilityKind.BEST_EFFORT;
import static ddsjdk.rtps.qos.EndpointQos.ReliabilityKind.RELIABLE;
import static org.junit.jupiter.api.Assertions.*;

class EndpointResolverTest {
    private final RemoteParticipantStore participants = new RemoteParticipantStore();
    private final RemoteEndpointStore<RemotePublication> publications = new RemoteEndpointStore<>();
    private final RemoteEndpointStore<RemoteSubscription> subscriptions = new RemoteEndpointStore<>();
    private final GuidPrefix prefix = new GuidPrefix(new byte[12]);
    private final Guid writer = prefix.toGuid(RtpsEntity.USER_WRITER_NO_KEY);
    private final Guid reader = prefix.toGuid(RtpsEntity.USER_READER_NO_KEY);
    private final RemoteParticipant participant = new RemoteParticipant(prefix, Set.of(), Set.of());

    private EndpointResolver resolver(EndpointQos.ReliabilityKind reliability) {
        var local = new LocalEndpoint("topic", "type", qos(reliability));
        return new EndpointResolver(local, participants, publications, subscriptions);
    }

    private static EndpointQos qos(EndpointQos.ReliabilityKind reliability) {
        return EndpointQos.builder().reliability(reliability).build();
    }

    @Test void compatibilityAlwaysUsesWriterAsOfferedAndReaderAsRequested() {
        participants.upsert(participant);
        for (var offered : EndpointQos.ReliabilityKind.values()) {
            for (var requested : EndpointQos.ReliabilityKind.values()) {
                publications.upsert(new RemotePublication(writer, "topic", "type", qos(offered)));
                subscriptions.upsert(new RemoteSubscription(reader, "topic", "type", qos(requested)));
                boolean compatible = offered == RELIABLE || requested == BEST_EFFORT;
                assertEquals(compatible, resolver(requested).acceptsPublication(writer, false));
                assertEquals(compatible ? Set.of(participant) : Set.of(),
                        resolver(requested).participantsForPublication(writer, false));
                assertEquals(compatible ? Set.of(participant) : Set.of(),
                        resolver(offered).participantsForSubscriptions());
            }
        }
    }

    @Test void topicAndTypeMustMatchEvenWhenUndiscoveredWritersAreAllowed() {
        participants.upsert(participant);
        var resolver = resolver(BEST_EFFORT);
        for (String[] names : new String[][]{{"other", "type"}, {"topic", "other"}}) {
            publications.upsert(new RemotePublication(writer, names[0], names[1], qos(RELIABLE)));
            subscriptions.upsert(new RemoteSubscription(reader, names[0], names[1], qos(BEST_EFFORT)));
            assertFalse(resolver.acceptsPublication(writer, true));
            assertTrue(resolver.participantsForPublication(writer, true).isEmpty());
            assertTrue(resolver.participantsForSubscriptions().isEmpty());
        }
    }

    @Test void undiscoveredPolicyOnlyAppliesUntilPublicationIsDiscovered() {
        var resolver = resolver(RELIABLE);
        assertTrue(resolver.acceptsPublication(writer, true));
        assertFalse(resolver.acceptsPublication(writer, false));
        assertTrue(resolver.participantsForPublication(writer, true).isEmpty());
        participants.upsert(participant);
        assertEquals(Set.of(participant), resolver.participantsForPublication(writer, true));
        assertTrue(resolver.participantsForPublication(writer, false).isEmpty());
        publications.upsert(new RemotePublication(writer, "topic", "type", qos(BEST_EFFORT)));
        assertFalse(resolver.acceptsPublication(writer, true));
        assertTrue(resolver.participantsForPublication(writer, true).isEmpty());
        publications.upsert(new RemotePublication(writer, "topic", "type", qos(RELIABLE)));
        assertTrue(resolver.acceptsPublication(writer, false));
        assertEquals(Set.of(participant), resolver.participantsForPublication(writer, false));
        participants.remove(prefix);
        assertTrue(resolver.participantsForPublication(writer, false).isEmpty());
        publications.remove(writer);
        assertFalse(resolver.acceptsPublication(writer, false));
        assertTrue(resolver.acceptsPublication(writer, true));
    }

    @Test void subscriptionDestinationsAreDistinctAndFollowDiscoveryChanges() {
        var resolver = resolver(RELIABLE);
        byte[] otherBytes = new byte[12];
        otherBytes[0] = 1;
        var otherPrefix = new GuidPrefix(otherBytes);
        var otherParticipant = new RemoteParticipant(otherPrefix, Set.of(), Set.of());
        var otherReader = otherPrefix.toGuid(RtpsEntity.USER_READER_NO_KEY);
        // Two subscriptions belonging to the same participant must yield one destination.
        subscriptions.upsert(new RemoteSubscription(reader, "topic", "type", qos(BEST_EFFORT)));
        subscriptions.upsert(new RemoteSubscription(prefix.toGuid(RtpsEntity.SUBSCRIPTIONS_BUILTIN_TOPIC_READER),
                "topic", "type", qos(RELIABLE)));
        subscriptions.upsert(new RemoteSubscription(otherReader, "topic", "type", qos(RELIABLE)));
        assertTrue(resolver.participantsForSubscriptions().isEmpty());
        participants.upsert(participant);
        participants.upsert(otherParticipant);
        assertEquals(Set.of(participant, otherParticipant), resolver.participantsForSubscriptions());
        subscriptions.removeByParticipant(prefix);
        assertEquals(Set.of(otherParticipant), resolver.participantsForSubscriptions());
        participants.remove(otherPrefix);
        assertTrue(resolver.participantsForSubscriptions().isEmpty());
    }
}
