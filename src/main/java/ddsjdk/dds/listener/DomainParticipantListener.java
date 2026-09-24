package ddsjdk.dds.listener;

/**
 * Listener for DomainParticipant status changes.
 * <p>
 * Extends all other listener interfaces to support hierarchical listener
 * callback propagation.
 */
public interface DomainParticipantListener extends TopicListener, PublisherListener, SubscriberListener {
}
