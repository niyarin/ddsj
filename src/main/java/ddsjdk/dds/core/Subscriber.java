package ddsjdk.dds.core;

import ddsjdk.dds.condition.StatusCondition;
import ddsjdk.dds.exception.DdsException;
import ddsjdk.dds.exception.ReturnCode;
import ddsjdk.dds.instance.InstanceHandle;
import ddsjdk.dds.listener.DataReaderListener;
import ddsjdk.dds.listener.SubscriberListener;
import ddsjdk.dds.qos.DataReaderQos;
import ddsjdk.dds.qos.SubscriberQos;
import ddsjdk.dds.status.StatusMask;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * DDS Subscriber that creates and manages DataReaders.
 */
public final class Subscriber implements Entity<SubscriberListener> {
    private static final AtomicLong HANDLE_COUNTER = new AtomicLong(1);

    private final DomainParticipant participant;
    private final AtomicReference<SubscriberQos> qos;
    private final InstanceHandle instanceHandle;
    private final StatusCondition statusCondition;
    private final AtomicReference<SubscriberListener> listener = new AtomicReference<>();
    private final AtomicReference<StatusMask> listenerMask = new AtomicReference<>(StatusMask.NONE);
    private final AtomicBoolean enabled = new AtomicBoolean(true);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final List<DataReader<?>> readers = new CopyOnWriteArrayList<>();
    private volatile DataReaderQos defaultDataReaderQos = DataReaderQos.DEFAULT;

    Subscriber(DomainParticipant participant, SubscriberQos qos) {
        this.participant = Objects.requireNonNull(participant, "participant");
        this.qos = new AtomicReference<>(qos != null ? qos : SubscriberQos.DEFAULT);
        this.instanceHandle = InstanceHandle.of(HANDLE_COUNTER.getAndIncrement());
        this.statusCondition = new StatusCondition(this);
    }

    /**
     * Creates a DataReader with default QoS.
     *
     * @param topic the topic
     * @param <T> the data type
     * @return the created reader
     */
    public <T> DataReader<T> createDataReader(Topic<T> topic) {
        return createDataReader(topic, defaultDataReaderQos, null, StatusMask.NONE);
    }

    /**
     * Creates a DataReader with specified QoS.
     *
     * @param topic the topic
     * @param qos the QoS policies
     * @param <T> the data type
     * @return the created reader
     */
    public <T> DataReader<T> createDataReader(Topic<T> topic, DataReaderQos qos) {
        return createDataReader(topic, qos, null, StatusMask.NONE);
    }

    /**
     * Creates a DataReader with specified QoS and listener.
     *
     * @param topic the topic
     * @param qos the QoS policies
     * @param listener the listener
     * @param mask the status mask
     * @param <T> the data type
     * @return the created reader
     */
    public <T> DataReader<T> createDataReader(Topic<T> topic, DataReaderQos qos,
                                               DataReaderListener listener, StatusMask mask) {
        ensureOpen();
        Objects.requireNonNull(topic, "topic");
        try {
            DataReader<T> reader = new DataReader<>(this, topic, qos != null ? qos : defaultDataReaderQos);
            reader.setListener(listener, mask);
            readers.add(reader);
            return reader;
        } catch (IOException e) {
            throw new DdsException(ReturnCode.ERROR, "Failed to create DataReader", e);
        }
    }

    /**
     * Deletes a DataReader.
     *
     * @param reader the reader to delete
     * @return OK if successful
     */
    public ReturnCode deleteDataReader(DataReader<?> reader) {
        if (readers.remove(reader)) {
            reader.close();
            return ReturnCode.OK;
        }
        return ReturnCode.PRECONDITION_NOT_MET;
    }

    /**
     * Looks up a DataReader by topic name.
     *
     * @param topicName the topic name
     * @param <T> the data type
     * @return the reader, or empty if not found
     */
    @SuppressWarnings("unchecked")
    public <T> Optional<DataReader<T>> lookupDataReader(String topicName) {
        for (DataReader<?> reader : readers) {
            if (reader.getTopic().getName().equals(topicName)) {
                return Optional.of((DataReader<T>) reader);
            }
        }
        return Optional.empty();
    }

    /**
     * Returns all DataReaders.
     *
     * @return the collection of readers
     */
    public Collection<DataReader<?>> getDataReaders() {
        return List.copyOf(readers);
    }

    /**
     * Deletes all contained DataReaders.
     *
     * @return OK if successful
     */
    public ReturnCode deleteContainedEntities() {
        for (DataReader<?> reader : readers) {
            reader.close();
        }
        readers.clear();
        return ReturnCode.OK;
    }

    /**
     * Returns the participant that created this subscriber.
     *
     * @return the participant
     */
    public DomainParticipant getParticipant() {
        return participant;
    }

    public DataReaderQos getDefaultDataReaderQos() {
        return defaultDataReaderQos;
    }

    public void setDefaultDataReaderQos(DataReaderQos qos) {
        this.defaultDataReaderQos = Objects.requireNonNull(qos, "qos");
    }

    public SubscriberQos getQos() {
        return qos.get();
    }

    public void setQos(SubscriberQos qos) {
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
    public void setListener(SubscriberListener listener, StatusMask mask) {
        this.listener.set(listener);
        this.listenerMask.set(mask != null ? mask : StatusMask.NONE);
    }

    @Override
    public SubscriberListener getListener() {
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
            participant.deleteSubscriber(this);
        }
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new DdsException(ReturnCode.ALREADY_DELETED, "Subscriber is closed");
        }
    }
}
