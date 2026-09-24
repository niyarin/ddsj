package ddsjdk.dds.core;

import ddsjdk.dds.condition.StatusCondition;
import ddsjdk.dds.exception.DdsException;
import ddsjdk.dds.exception.ReturnCode;
import ddsjdk.dds.instance.InstanceHandle;
import ddsjdk.dds.listener.DomainParticipantListener;
import ddsjdk.dds.qos.DomainParticipantQos;
import ddsjdk.dds.qos.PublisherQos;
import ddsjdk.dds.qos.SubscriberQos;
import ddsjdk.dds.qos.TopicQos;
import ddsjdk.dds.status.StatusMask;
import ddsjdk.rtps.runtime.RtpsParticipant;
import ddsjdk.rtps.transport.RtpsParticipantConfig;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.List;

/**
 * DDS DomainParticipant representing participation in a domain.
 * <p>
 * DomainParticipant is the entry point for DDS communication. It acts as
 * a factory for Publishers, Subscribers, and Topics, and wraps an RTPS
 * Participant internally.
 */
public final class DomainParticipant implements Entity<DomainParticipantListener> {
    private static final AtomicLong HANDLE_COUNTER = new AtomicLong(1);

    private final int domainId;
    private final RtpsParticipant rtpsParticipant;
    private final AtomicReference<DomainParticipantQos> qos;
    private final InstanceHandle instanceHandle;
    private final StatusCondition statusCondition;
    private final AtomicReference<DomainParticipantListener> listener = new AtomicReference<>();
    private final AtomicReference<StatusMask> listenerMask = new AtomicReference<>(StatusMask.NONE);
    private final AtomicBoolean enabled = new AtomicBoolean(true);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    // Contained entities
    private final Map<String, Topic<?>> topics = new ConcurrentHashMap<>();
    private final List<Publisher> publishers = new CopyOnWriteArrayList<>();
    private final List<Subscriber> subscribers = new CopyOnWriteArrayList<>();

    // Default QoS for contained entities
    private volatile TopicQos defaultTopicQos = TopicQos.DEFAULT;
    private volatile PublisherQos defaultPublisherQos = PublisherQos.DEFAULT;
    private volatile SubscriberQos defaultSubscriberQos = SubscriberQos.DEFAULT;

    DomainParticipant(int domainId, DomainParticipantQos qos) throws IOException {
        this.domainId = domainId;
        DomainParticipantQos effectiveQos = qos != null ? qos : DomainParticipantQos.DEFAULT;
        this.qos = new AtomicReference<>(effectiveQos);
        this.instanceHandle = InstanceHandle.of(HANDLE_COUNTER.getAndIncrement());
        this.statusCondition = new StatusCondition(this);

        RtpsParticipantConfig config = new RtpsParticipantConfig(
                domainId,
                RtpsParticipantConfig.defaultMulticastGroup(),
                effectiveQos.networkInterface(),
                effectiveQos.participantIndex()
        );
        this.rtpsParticipant = new RtpsParticipant(config);
    }

    /**
     * Returns the domain ID.
     *
     * @return the domain ID
     */
    public int getDomainId() {
        return domainId;
    }

    /**
     * Returns the internal RTPS participant.
     * <p>
     * This is for internal use by DDS entities.
     *
     * @return the RTPS participant
     */
    RtpsParticipant rtpsParticipant() {
        return rtpsParticipant;
    }

    // ========== Topic Operations ==========

    /**
     * Creates a Topic with default QoS.
     *
     * @param topicName the topic name
     * @param type the data class
     * @param typeSupport the type support
     * @param <T> the data type
     * @return the created topic
     */
    public <T> Topic<T> createTopic(String topicName, Class<T> type, TypeSupport<T> typeSupport) {
        return createTopic(topicName, type, typeSupport, defaultTopicQos);
    }

    /**
     * Creates a Topic with specified QoS.
     *
     * @param topicName the topic name
     * @param type the data class
     * @param typeSupport the type support
     * @param qos the QoS policies
     * @param <T> the data type
     * @return the created topic
     */
    public <T> Topic<T> createTopic(String topicName, Class<T> type, TypeSupport<T> typeSupport, TopicQos qos) {
        ensureOpen();
        Objects.requireNonNull(topicName, "topicName");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(typeSupport, "typeSupport");

        Topic<T> topic = new Topic<>(this, topicName, type, typeSupport, qos);
        Topic<?> existing = topics.putIfAbsent(topicName, topic);
        if (existing != null) {
            throw new DdsException(ReturnCode.PRECONDITION_NOT_MET,
                    "Topic already exists: " + topicName);
        }
        return topic;
    }

    /**
     * Looks up an existing topic by name.
     *
     * @param name the topic name
     * @param <T> the data type
     * @return the topic, or empty if not found
     */
    @SuppressWarnings("unchecked")
    public <T> Optional<Topic<T>> lookupTopicDescription(String name) {
        return Optional.ofNullable((Topic<T>) topics.get(name));
    }

    /**
     * Deletes a topic.
     *
     * @param topic the topic to delete
     * @return OK if successful
     */
    public ReturnCode deleteTopic(Topic<?> topic) {
        if (topics.remove(topic.getName(), topic)) {
            return ReturnCode.OK;
        }
        return ReturnCode.PRECONDITION_NOT_MET;
    }

    // ========== Publisher Operations ==========

    /**
     * Creates a Publisher with default QoS.
     *
     * @return the created publisher
     */
    public Publisher createPublisher() {
        return createPublisher(defaultPublisherQos, null, StatusMask.NONE);
    }

    /**
     * Creates a Publisher with specified QoS.
     *
     * @param qos the QoS policies
     * @return the created publisher
     */
    public Publisher createPublisher(PublisherQos qos) {
        return createPublisher(qos, null, StatusMask.NONE);
    }

    /**
     * Creates a Publisher with specified QoS and listener.
     *
     * @param qos the QoS policies
     * @param listener the listener
     * @param mask the status mask
     * @return the created publisher
     */
    public Publisher createPublisher(PublisherQos qos, ddsjdk.dds.listener.PublisherListener listener, StatusMask mask) {
        ensureOpen();
        Publisher publisher = new Publisher(this, qos != null ? qos : defaultPublisherQos);
        publisher.setListener(listener, mask);
        publishers.add(publisher);
        return publisher;
    }

    /**
     * Deletes a publisher.
     *
     * @param publisher the publisher to delete
     * @return OK if successful
     */
    public ReturnCode deletePublisher(Publisher publisher) {
        if (publishers.remove(publisher)) {
            return ReturnCode.OK;
        }
        return ReturnCode.PRECONDITION_NOT_MET;
    }

    // ========== Subscriber Operations ==========

    /**
     * Creates a Subscriber with default QoS.
     *
     * @return the created subscriber
     */
    public Subscriber createSubscriber() {
        return createSubscriber(defaultSubscriberQos, null, StatusMask.NONE);
    }

    /**
     * Creates a Subscriber with specified QoS.
     *
     * @param qos the QoS policies
     * @return the created subscriber
     */
    public Subscriber createSubscriber(SubscriberQos qos) {
        return createSubscriber(qos, null, StatusMask.NONE);
    }

    /**
     * Creates a Subscriber with specified QoS and listener.
     *
     * @param qos the QoS policies
     * @param listener the listener
     * @param mask the status mask
     * @return the created subscriber
     */
    public Subscriber createSubscriber(SubscriberQos qos, ddsjdk.dds.listener.SubscriberListener listener, StatusMask mask) {
        ensureOpen();
        Subscriber subscriber = new Subscriber(this, qos != null ? qos : defaultSubscriberQos);
        subscriber.setListener(listener, mask);
        subscribers.add(subscriber);
        return subscriber;
    }

    /**
     * Deletes a subscriber.
     *
     * @param subscriber the subscriber to delete
     * @return OK if successful
     */
    public ReturnCode deleteSubscriber(Subscriber subscriber) {
        if (subscribers.remove(subscriber)) {
            return ReturnCode.OK;
        }
        return ReturnCode.PRECONDITION_NOT_MET;
    }

    // ========== Default QoS Operations ==========

    public TopicQos getDefaultTopicQos() {
        return defaultTopicQos;
    }

    public void setDefaultTopicQos(TopicQos qos) {
        this.defaultTopicQos = Objects.requireNonNull(qos, "qos");
    }

    public PublisherQos getDefaultPublisherQos() {
        return defaultPublisherQos;
    }

    public void setDefaultPublisherQos(PublisherQos qos) {
        this.defaultPublisherQos = Objects.requireNonNull(qos, "qos");
    }

    public SubscriberQos getDefaultSubscriberQos() {
        return defaultSubscriberQos;
    }

    public void setDefaultSubscriberQos(SubscriberQos qos) {
        this.defaultSubscriberQos = Objects.requireNonNull(qos, "qos");
    }

    // ========== Utility ==========

    /**
     * Returns the current time.
     *
     * @return the current instant
     */
    public Instant getCurrentTime() {
        return Instant.now();
    }

    /**
     * Checks if this participant contains the specified entity.
     *
     * @param handle the entity handle
     * @return true if contained
     */
    public boolean containsEntity(InstanceHandle handle) {
        for (Topic<?> topic : topics.values()) {
            if (topic.getInstanceHandle().equals(handle)) return true;
        }
        for (Publisher pub : publishers) {
            if (pub.getInstanceHandle().equals(handle)) return true;
        }
        for (Subscriber sub : subscribers) {
            if (sub.getInstanceHandle().equals(handle)) return true;
        }
        return false;
    }

    /**
     * Deletes all contained entities.
     *
     * @return OK if successful
     */
    public ReturnCode deleteContainedEntities() {
        for (Publisher pub : publishers) {
            pub.deleteContainedEntities();
        }
        publishers.clear();

        for (Subscriber sub : subscribers) {
            sub.deleteContainedEntities();
        }
        subscribers.clear();

        topics.clear();
        return ReturnCode.OK;
    }

    // ========== Entity Implementation ==========

    @Override
    public InstanceHandle getInstanceHandle() {
        return instanceHandle;
    }

    @Override
    public StatusCondition getStatusCondition() {
        return statusCondition;
    }

    @Override
    public StatusMask getStatusChanges() {
        return statusCondition.getTriggerValue() ? listenerMask.get() : StatusMask.NONE;
    }

    @Override
    public void setListener(DomainParticipantListener listener, StatusMask mask) {
        this.listener.set(listener);
        this.listenerMask.set(mask != null ? mask : StatusMask.NONE);
    }

    @Override
    public DomainParticipantListener getListener() {
        return listener.get();
    }

    @Override
    public void enable() {
        enabled.set(true);
    }

    @Override
    public boolean isEnabled() {
        return enabled.get();
    }

    public DomainParticipantQos getQos() {
        return qos.get();
    }

    public void setQos(DomainParticipantQos qos) {
        this.qos.set(Objects.requireNonNull(qos, "qos"));
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            deleteContainedEntities();
            try {
                rtpsParticipant.close();
            } catch (IOException e) {
                throw new DdsException(ReturnCode.ERROR, "Failed to close RTPS participant", e);
            }
            DomainParticipantFactory.getInstance().removeParticipant(this);
        }
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new DdsException(ReturnCode.ALREADY_DELETED, "Participant is closed");
        }
    }
}
