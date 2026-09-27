package ddsj.dds.core;

import ddsj.dds.condition.StatusCondition;
import ddsj.dds.instance.InstanceHandle;
import ddsj.dds.listener.TopicListener;
import ddsj.dds.qos.TopicQos;
import ddsj.dds.status.InconsistentTopicStatus;
import ddsj.dds.status.StatusKind;
import ddsj.dds.status.StatusMask;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * DDS Topic representing a data type on a named channel.
 * <p>
 * Topics define the data type and name for pub/sub communication.
 * DataWriters and DataReaders are created for specific Topics.
 *
 * @param <T> the data type
 */
public final class Topic<T> implements Entity<TopicListener> {
    private static final AtomicLong HANDLE_COUNTER = new AtomicLong(1);

    private final DomainParticipant participant;
    private final String name;
    private final String typeName;
    private final Class<T> type;
    private final TypeSupport<T> typeSupport;
    private final AtomicReference<TopicQos> qos;
    private final InstanceHandle instanceHandle;
    private final StatusCondition statusCondition;
    private final AtomicReference<TopicListener> listener = new AtomicReference<>();
    private final AtomicReference<StatusMask> listenerMask = new AtomicReference<>(StatusMask.NONE);
    private final AtomicBoolean enabled = new AtomicBoolean(true);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    // Status tracking
    private volatile InconsistentTopicStatus inconsistentTopicStatus = InconsistentTopicStatus.INITIAL;

    Topic(DomainParticipant participant, String name, Class<T> type,
          TypeSupport<T> typeSupport, TopicQos qos) {
        this.participant = Objects.requireNonNull(participant, "participant");
        this.name = Objects.requireNonNull(name, "name");
        this.type = Objects.requireNonNull(type, "type");
        this.typeSupport = Objects.requireNonNull(typeSupport, "typeSupport");
        this.typeName = typeSupport.getTypeName();
        this.qos = new AtomicReference<>(qos != null ? qos : TopicQos.DEFAULT);
        this.instanceHandle = InstanceHandle.of(HANDLE_COUNTER.getAndIncrement());
        this.statusCondition = new StatusCondition(this);
    }

    /**
     * Returns the topic name.
     *
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the type name.
     *
     * @return the type name
     */
    public String getTypeName() {
        return typeName;
    }

    /**
     * Returns the Java class for this topic's data type.
     *
     * @return the data class
     */
    public Class<T> getType() {
        return type;
    }

    /**
     * Returns the TypeSupport for this topic.
     *
     * @return the type support
     */
    public TypeSupport<T> getTypeSupport() {
        return typeSupport;
    }

    /**
     * Returns the participant that created this topic.
     *
     * @return the participant
     */
    public DomainParticipant getParticipant() {
        return participant;
    }

    /**
     * Returns the current QoS.
     *
     * @return the QoS
     */
    public TopicQos getQos() {
        return qos.get();
    }

    /**
     * Sets new QoS policies.
     *
     * @param qos the new QoS
     */
    public void setQos(TopicQos qos) {
        this.qos.set(Objects.requireNonNull(qos, "qos"));
    }

    /**
     * Returns the inconsistent topic status.
     *
     * @return the status
     */
    public InconsistentTopicStatus getInconsistentTopicStatus() {
        InconsistentTopicStatus current = inconsistentTopicStatus;
        inconsistentTopicStatus = new InconsistentTopicStatus(current.totalCount(), 0);
        statusCondition.clearStatus(StatusKind.INCONSISTENT_TOPIC);
        return current;
    }

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
    public void setListener(TopicListener listener, StatusMask mask) {
        this.listener.set(listener);
        this.listenerMask.set(mask != null ? mask : StatusMask.NONE);
    }

    @Override
    public TopicListener getListener() {
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

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            participant.deleteTopic(this);
        }
    }
}
