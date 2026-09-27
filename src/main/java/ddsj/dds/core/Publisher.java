package ddsj.dds.core;

import ddsj.dds.condition.StatusCondition;
import ddsj.dds.exception.DdsException;
import ddsj.dds.exception.ReturnCode;
import ddsj.dds.instance.InstanceHandle;
import ddsj.dds.listener.DataWriterListener;
import ddsj.dds.listener.PublisherListener;
import ddsj.dds.qos.DataWriterQos;
import ddsj.dds.qos.PublisherQos;
import ddsj.dds.status.StatusMask;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * DDS Publisher that creates and manages DataWriters.
 */
public final class Publisher implements Entity<PublisherListener> {
    private static final AtomicLong HANDLE_COUNTER = new AtomicLong(1);

    private final DomainParticipant participant;
    private final AtomicReference<PublisherQos> qos;
    private final InstanceHandle instanceHandle;
    private final StatusCondition statusCondition;
    private final AtomicReference<PublisherListener> listener = new AtomicReference<>();
    private final AtomicReference<StatusMask> listenerMask = new AtomicReference<>(StatusMask.NONE);
    private final AtomicBoolean enabled = new AtomicBoolean(true);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final List<DataWriter<?>> writers = new CopyOnWriteArrayList<>();
    private volatile DataWriterQos defaultDataWriterQos = DataWriterQos.DEFAULT;

    Publisher(DomainParticipant participant, PublisherQos qos) {
        this.participant = Objects.requireNonNull(participant, "participant");
        this.qos = new AtomicReference<>(qos != null ? qos : PublisherQos.DEFAULT);
        this.instanceHandle = InstanceHandle.of(HANDLE_COUNTER.getAndIncrement());
        this.statusCondition = new StatusCondition(this);
    }

    /**
     * Creates a DataWriter with default QoS.
     *
     * @param topic the topic
     * @param <T> the data type
     * @return the created writer
     */
    public <T> DataWriter<T> createDataWriter(Topic<T> topic) {
        return createDataWriter(topic, defaultDataWriterQos, null, StatusMask.NONE);
    }

    /**
     * Creates a DataWriter with specified QoS.
     *
     * @param topic the topic
     * @param qos the QoS policies
     * @param <T> the data type
     * @return the created writer
     */
    public <T> DataWriter<T> createDataWriter(Topic<T> topic, DataWriterQos qos) {
        return createDataWriter(topic, qos, null, StatusMask.NONE);
    }

    /**
     * Creates a DataWriter with specified QoS and listener.
     *
     * @param topic the topic
     * @param qos the QoS policies
     * @param listener the listener
     * @param mask the status mask
     * @param <T> the data type
     * @return the created writer
     */
    public <T> DataWriter<T> createDataWriter(Topic<T> topic, DataWriterQos qos,
                                               DataWriterListener listener, StatusMask mask) {
        ensureOpen();
        Objects.requireNonNull(topic, "topic");
        try {
            DataWriter<T> writer = new DataWriter<>(this, topic, qos != null ? qos : defaultDataWriterQos);
            writer.setListener(listener, mask);
            writers.add(writer);
            return writer;
        } catch (IOException e) {
            throw new DdsException(ReturnCode.ERROR, "Failed to create DataWriter", e);
        }
    }

    /**
     * Deletes a DataWriter.
     *
     * @param writer the writer to delete
     * @return OK if successful
     */
    public ReturnCode deleteDataWriter(DataWriter<?> writer) {
        if (writers.remove(writer)) {
            writer.close();
            return ReturnCode.OK;
        }
        return ReturnCode.PRECONDITION_NOT_MET;
    }

    /**
     * Looks up a DataWriter by topic name.
     *
     * @param topicName the topic name
     * @param <T> the data type
     * @return the writer, or empty if not found
     */
    @SuppressWarnings("unchecked")
    public <T> Optional<DataWriter<T>> lookupDataWriter(String topicName) {
        for (DataWriter<?> writer : writers) {
            if (writer.getTopic().getName().equals(topicName)) {
                return Optional.of((DataWriter<T>) writer);
            }
        }
        return Optional.empty();
    }

    /**
     * Deletes all contained DataWriters.
     *
     * @return OK if successful
     */
    public ReturnCode deleteContainedEntities() {
        for (DataWriter<?> writer : writers) {
            writer.close();
        }
        writers.clear();
        return ReturnCode.OK;
    }

    /**
     * Returns the participant that created this publisher.
     *
     * @return the participant
     */
    public DomainParticipant getParticipant() {
        return participant;
    }

    public DataWriterQos getDefaultDataWriterQos() {
        return defaultDataWriterQos;
    }

    public void setDefaultDataWriterQos(DataWriterQos qos) {
        this.defaultDataWriterQos = Objects.requireNonNull(qos, "qos");
    }

    public PublisherQos getQos() {
        return qos.get();
    }

    public void setQos(PublisherQos qos) {
        this.qos.set(Objects.requireNonNull(qos, "qos"));
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
    public void setListener(PublisherListener listener, StatusMask mask) {
        this.listener.set(listener);
        this.listenerMask.set(mask != null ? mask : StatusMask.NONE);
    }

    @Override
    public PublisherListener getListener() {
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
            deleteContainedEntities();
            participant.deletePublisher(this);
        }
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new DdsException(ReturnCode.ALREADY_DELETED, "Publisher is closed");
        }
    }
}
