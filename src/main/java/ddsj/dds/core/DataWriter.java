package ddsj.dds.core;

import ddsj.dds.condition.StatusCondition;
import ddsj.dds.exception.DdsException;
import ddsj.dds.exception.ReturnCode;
import ddsj.dds.instance.InstanceHandle;
import ddsj.dds.listener.DataWriterListener;
import ddsj.dds.qos.DataWriterQos;
import ddsj.dds.qos.QosConverter;
import ddsj.dds.status.*;
import ddsj.rtps.discovery.LocalEndpoint;
import ddsj.rtps.runtime.PayloadSerializer;
import ddsj.rtps.runtime.RtpsDataWriter;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * DDS DataWriter for publishing data.
 * <p>
 * DataWriter wraps an RTPS DataWriter and provides DDS-level features
 * including instance management and status tracking.
 *
 * @param <T> the data type
 */
public final class DataWriter<T> implements Entity<DataWriterListener> {
    private static final AtomicLong HANDLE_COUNTER = new AtomicLong(1);

    private final Publisher publisher;
    private final Topic<T> topic;
    private final AtomicReference<DataWriterQos> qos;
    private final RtpsDataWriter<T> rtpsWriter;
    private final InstanceHandle instanceHandle;
    private final StatusCondition statusCondition;
    private final AtomicReference<DataWriterListener> listener = new AtomicReference<>();
    private final AtomicReference<StatusMask> listenerMask = new AtomicReference<>(StatusMask.NONE);
    private final AtomicBoolean enabled = new AtomicBoolean(true);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    // Instance management for keyed types
    private final Map<Object, InstanceHandle> instanceRegistry = new ConcurrentHashMap<>();
    private final AtomicLong instanceCounter = new AtomicLong(1);

    // Status tracking
    private volatile LivelinessLostStatus livelinessLostStatus = LivelinessLostStatus.INITIAL;
    private volatile OfferedDeadlineMissedStatus offeredDeadlineMissedStatus = OfferedDeadlineMissedStatus.INITIAL;
    private volatile OfferedIncompatibleQosStatus offeredIncompatibleQosStatus = OfferedIncompatibleQosStatus.INITIAL;
    private volatile PublicationMatchedStatus publicationMatchedStatus = PublicationMatchedStatus.INITIAL;

    DataWriter(Publisher publisher, Topic<T> topic, DataWriterQos qos) throws IOException {
        this.publisher = Objects.requireNonNull(publisher, "publisher");
        this.topic = Objects.requireNonNull(topic, "topic");
        this.qos = new AtomicReference<>(qos != null ? qos : DataWriterQos.DEFAULT);
        this.instanceHandle = InstanceHandle.of(HANDLE_COUNTER.getAndIncrement());
        this.statusCondition = new StatusCondition(this);

        // Create RTPS writer
        LocalEndpoint endpoint = new LocalEndpoint(
                topic.getName(),
                topic.getTypeName(),
                QosConverter.toEndpointQos(this.qos.get()),
                QosConverter.toResourceLimits(this.qos.get().resourceLimits())
        );
        PayloadSerializer<T> serializer = createSerializer(topic.getTypeSupport());
        this.rtpsWriter = publisher.getParticipant().rtpsParticipant().createWriter(endpoint, serializer);
    }

    private PayloadSerializer<T> createSerializer(TypeSupport<T> typeSupport) {
        return new PayloadSerializer<>() {
            @Override
            public byte[] serialize(T value) {
                return typeSupport.serialize(value);
            }

            @Override
            public T deserialize(byte[] payload) {
                return typeSupport.deserialize(payload);
            }
        };
    }

    // ========== Write Operations ==========

    /**
     * Writes a data value.
     *
     * @param data the data to write
     * @return OK if successful
     */
    public ReturnCode write(T data) {
        return write(data, InstanceHandle.NIL);
    }

    /**
     * Writes a data value for a specific instance.
     *
     * @param data the data to write
     * @param handle the instance handle (NIL for auto-detect)
     * @return OK if successful
     */
    public ReturnCode write(T data, InstanceHandle handle) {
        ensureOpen();
        try {
            if (topic.getTypeSupport().hasKey() && handle.isNil()) {
                registerInstance(data);
            }
            rtpsWriter.write(data);
            return ReturnCode.OK;
        } catch (IOException e) {
            return ReturnCode.ERROR;
        }
    }

    /**
     * Writes a data value with a timestamp.
     *
     * @param data the data to write
     * @param handle the instance handle
     * @param timestamp the source timestamp
     * @return OK if successful
     */
    public ReturnCode writeWithTimestamp(T data, InstanceHandle handle, Instant timestamp) {
        // Current RTPS layer doesn't support explicit timestamps, so just write
        return write(data, handle);
    }

    // ========== Instance Management ==========

    /**
     * Registers an instance.
     *
     * @param data the data containing the key
     * @return the instance handle, or NIL if unkeyed
     */
    public InstanceHandle registerInstance(T data) {
        if (!topic.getTypeSupport().hasKey()) {
            return InstanceHandle.NIL;
        }
        Object key = topic.getTypeSupport().extractKey(data);
        return instanceRegistry.computeIfAbsent(key,
                k -> InstanceHandle.of(instanceCounter.getAndIncrement()));
    }

    /**
     * Registers an instance with a timestamp.
     *
     * @param data the data containing the key
     * @param timestamp the timestamp
     * @return the instance handle
     */
    public InstanceHandle registerInstanceWithTimestamp(T data, Instant timestamp) {
        return registerInstance(data);
    }

    /**
     * Unregisters an instance.
     *
     * @param data the data containing the key
     * @param handle the instance handle
     * @return OK if successful
     */
    public ReturnCode unregisterInstance(T data, InstanceHandle handle) {
        if (!topic.getTypeSupport().hasKey()) {
            return ReturnCode.OK;
        }
        Object key = topic.getTypeSupport().extractKey(data);
        instanceRegistry.remove(key);
        return ReturnCode.OK;
    }

    /**
     * Disposes an instance.
     *
     * @param data the data containing the key
     * @param handle the instance handle
     * @return OK if successful
     */
    public ReturnCode dispose(T data, InstanceHandle handle) {
        // Dispose is similar to unregister in simple implementation
        return unregisterInstance(data, handle);
    }

    /**
     * Looks up an instance handle by key.
     *
     * @param keyHolder data containing the key to look up
     * @return the instance handle, or NIL if not found
     */
    public InstanceHandle lookupInstance(T keyHolder) {
        if (!topic.getTypeSupport().hasKey()) {
            return InstanceHandle.NIL;
        }
        Object key = topic.getTypeSupport().extractKey(keyHolder);
        return instanceRegistry.getOrDefault(key, InstanceHandle.NIL);
    }

    // ========== Liveliness ==========

    /**
     * Asserts liveliness for this writer.
     *
     * @return OK if successful
     */
    public ReturnCode assertLiveliness() {
        rtpsWriter.assertLiveliness();
        return ReturnCode.OK;
    }

    // ========== Status Accessors ==========

    public LivelinessLostStatus getLivelinessLostStatus() {
        LivelinessLostStatus current = livelinessLostStatus;
        livelinessLostStatus = new LivelinessLostStatus(current.totalCount(), 0);
        statusCondition.clearStatus(StatusKind.LIVELINESS_LOST);
        return current;
    }

    public OfferedDeadlineMissedStatus getOfferedDeadlineMissedStatus() {
        OfferedDeadlineMissedStatus current = offeredDeadlineMissedStatus;
        offeredDeadlineMissedStatus = new OfferedDeadlineMissedStatus(current.totalCount(), 0, current.lastInstanceHandle());
        statusCondition.clearStatus(StatusKind.OFFERED_DEADLINE_MISSED);
        return current;
    }

    public OfferedIncompatibleQosStatus getOfferedIncompatibleQosStatus() {
        OfferedIncompatibleQosStatus current = offeredIncompatibleQosStatus;
        offeredIncompatibleQosStatus = new OfferedIncompatibleQosStatus(current.totalCount(), 0, current.lastPolicyId(), current.policies());
        statusCondition.clearStatus(StatusKind.OFFERED_INCOMPATIBLE_QOS);
        return current;
    }

    public PublicationMatchedStatus getPublicationMatchedStatus() {
        PublicationMatchedStatus current = publicationMatchedStatus;
        publicationMatchedStatus = new PublicationMatchedStatus(
                current.totalCount(), 0, current.currentCount(), 0, current.lastSubscriptionHandle());
        statusCondition.clearStatus(StatusKind.PUBLICATION_MATCHED);
        return current;
    }

    // ========== Accessors ==========

    public Topic<T> getTopic() {
        return topic;
    }

    public Publisher getPublisher() {
        return publisher;
    }

    public DataWriterQos getQos() {
        return qos.get();
    }

    public void setQos(DataWriterQos qos) {
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
    public void setListener(DataWriterListener listener, StatusMask mask) {
        this.listener.set(listener);
        this.listenerMask.set(mask != null ? mask : StatusMask.NONE);
    }

    @Override
    public DataWriterListener getListener() {
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
            try {
                rtpsWriter.close();
            } catch (IOException e) {
                throw new DdsException(ReturnCode.ERROR, "Failed to close RTPS writer", e);
            }
        }
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new DdsException(ReturnCode.ALREADY_DELETED, "DataWriter is closed");
        }
    }
}
