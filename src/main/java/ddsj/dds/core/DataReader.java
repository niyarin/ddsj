package ddsj.dds.core;

import ddsj.dds.condition.ReadCondition;
import ddsj.dds.condition.StatusCondition;
import ddsj.dds.exception.DdsException;
import ddsj.dds.exception.ReturnCode;
import ddsj.dds.instance.*;
import ddsj.dds.listener.DataReaderListener;
import ddsj.dds.qos.DataReaderQos;
import ddsj.dds.qos.QosConverter;
import ddsj.dds.sample.Sample;
import ddsj.dds.status.*;
import ddsj.rtps.discovery.LocalEndpoint;
import ddsj.rtps.message.SampleIdentity;
import ddsj.rtps.runtime.PayloadSerializer;
import ddsj.rtps.runtime.ReceivedSample;
import ddsj.rtps.runtime.RtpsDataReader;
import ddsj.rtps.types.Guid;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * DDS DataReader for receiving data.
 * <p>
 * DataReader wraps an RTPS DataReader and provides DDS-level features
 * including sample state tracking and condition support.
 *
 * @param <T> the data type
 */
public final class DataReader<T> implements Entity<DataReaderListener> {
    private static final AtomicLong HANDLE_COUNTER = new AtomicLong(1);

    private final Subscriber subscriber;
    private final Topic<T> topic;
    private final AtomicReference<DataReaderQos> qos;
    private final RtpsDataReader<T> rtpsReader;
    private final InstanceHandle instanceHandle;
    private final StatusCondition statusCondition;
    private final AtomicReference<DataReaderListener> listener = new AtomicReference<>();
    private final AtomicReference<StatusMask> listenerMask = new AtomicReference<>(StatusMask.NONE);
    private final AtomicBoolean enabled = new AtomicBoolean(true);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    // Sample cache with state tracking
    private final List<CachedSample<T>> sampleCache = new CopyOnWriteArrayList<>();
    private final Map<Object, InstanceInfo> instanceInfoMap = new HashMap<>();
    private final AtomicLong instanceCounter = new AtomicLong(1);

    // Read conditions
    private final List<ReadCondition> readConditions = new CopyOnWriteArrayList<>();

    // Status tracking
    private volatile LivelinessChangedStatus livelinessChangedStatus = LivelinessChangedStatus.INITIAL;
    private volatile RequestedDeadlineMissedStatus requestedDeadlineMissedStatus = RequestedDeadlineMissedStatus.INITIAL;
    private volatile RequestedIncompatibleQosStatus requestedIncompatibleQosStatus = RequestedIncompatibleQosStatus.INITIAL;
    private volatile SubscriptionMatchedStatus subscriptionMatchedStatus = SubscriptionMatchedStatus.INITIAL;
    private volatile SampleRejectedStatus sampleRejectedStatus = SampleRejectedStatus.INITIAL;
    private volatile SampleLostStatus sampleLostStatus = SampleLostStatus.INITIAL;

    DataReader(Subscriber subscriber, Topic<T> topic, DataReaderQos qos) throws IOException {
        this.subscriber = Objects.requireNonNull(subscriber, "subscriber");
        this.topic = Objects.requireNonNull(topic, "topic");
        this.qos = new AtomicReference<>(qos != null ? qos : DataReaderQos.DEFAULT);
        this.instanceHandle = InstanceHandle.of(HANDLE_COUNTER.getAndIncrement());
        this.statusCondition = new StatusCondition(this);

        // Create RTPS reader
        LocalEndpoint endpoint = new LocalEndpoint(
                topic.getName(),
                topic.getTypeName(),
                QosConverter.toEndpointQos(this.qos.get()),
                QosConverter.toResourceLimits(this.qos.get().resourceLimits())
        );
        PayloadSerializer<T> serializer = createSerializer(topic.getTypeSupport());
        this.rtpsReader = subscriber.getParticipant().rtpsParticipant().createReader(endpoint, serializer);
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

    // ========== Read Operations ==========

    /**
     * Reads all available samples without removing them.
     *
     * @return list of samples
     */
    public List<Sample<T>> read() {
        return read(Integer.MAX_VALUE, SampleState.ANY, ViewState.ANY, InstanceState.ANY);
    }

    /**
     * Reads up to maxSamples without removing them.
     *
     * @param maxSamples maximum samples to return
     * @return list of samples
     */
    public List<Sample<T>> read(int maxSamples) {
        return read(maxSamples, SampleState.ANY, ViewState.ANY, InstanceState.ANY);
    }

    /**
     * Reads samples matching the specified states.
     *
     * @param maxSamples maximum samples to return
     * @param sampleStates sample states to match
     * @param viewStates view states to match
     * @param instanceStates instance states to match
     * @return list of samples
     */
    public List<Sample<T>> read(int maxSamples, Set<SampleState> sampleStates,
                                Set<ViewState> viewStates, Set<InstanceState> instanceStates) {
        ensureOpen();
        fetchFromRtps();
        List<Sample<T>> result = new ArrayList<>();

        for (CachedSample<T> cached : sampleCache) {
            if (result.size() >= maxSamples) break;
            if (matchesStates(cached, sampleStates, viewStates, instanceStates)) {
                result.add(cached.toSample());
                cached.markRead();
            }
        }
        return result;
    }

    /**
     * Reads samples matching the condition.
     *
     * @param condition the read condition
     * @param maxSamples maximum samples to return
     * @return list of samples
     */
    public List<Sample<T>> readWithCondition(ReadCondition condition, int maxSamples) {
        return read(maxSamples, condition.getSampleStateMask(),
                condition.getViewStateMask(), condition.getInstanceStateMask());
    }

    /**
     * Reads the next sample without removing it.
     *
     * @return the next sample, or empty
     */
    public Optional<Sample<T>> readNextSample() {
        List<Sample<T>> samples = read(1);
        return samples.isEmpty() ? Optional.empty() : Optional.of(samples.get(0));
    }

    // ========== Take Operations ==========

    /**
     * Takes all available samples, removing them from the cache.
     *
     * @return list of samples
     */
    public List<Sample<T>> take() {
        return take(Integer.MAX_VALUE, SampleState.ANY, ViewState.ANY, InstanceState.ANY);
    }

    /**
     * Takes up to maxSamples, removing them from the cache.
     *
     * @param maxSamples maximum samples to return
     * @return list of samples
     */
    public List<Sample<T>> take(int maxSamples) {
        return take(maxSamples, SampleState.ANY, ViewState.ANY, InstanceState.ANY);
    }

    /**
     * Takes samples matching the specified states.
     *
     * @param maxSamples maximum samples to return
     * @param sampleStates sample states to match
     * @param viewStates view states to match
     * @param instanceStates instance states to match
     * @return list of samples
     */
    public List<Sample<T>> take(int maxSamples, Set<SampleState> sampleStates,
                                Set<ViewState> viewStates, Set<InstanceState> instanceStates) {
        ensureOpen();
        fetchFromRtps();
        List<Sample<T>> result = new ArrayList<>();
        List<CachedSample<T>> toRemove = new ArrayList<>();

        for (CachedSample<T> cached : sampleCache) {
            if (result.size() >= maxSamples) break;
            if (matchesStates(cached, sampleStates, viewStates, instanceStates)) {
                result.add(cached.toSample());
                toRemove.add(cached);
            }
        }
        sampleCache.removeAll(toRemove);
        return result;
    }

    /**
     * Takes samples matching the condition.
     *
     * @param condition the read condition
     * @param maxSamples maximum samples to return
     * @return list of samples
     */
    public List<Sample<T>> takeWithCondition(ReadCondition condition, int maxSamples) {
        return take(maxSamples, condition.getSampleStateMask(),
                condition.getViewStateMask(), condition.getInstanceStateMask());
    }

    /**
     * Takes the next sample, removing it from the cache.
     *
     * @return the next sample, or empty
     */
    public Optional<Sample<T>> takeNextSample() {
        List<Sample<T>> samples = take(1);
        return samples.isEmpty() ? Optional.empty() : Optional.of(samples.get(0));
    }

    // ========== Condition Management ==========

    /**
     * Creates a ReadCondition.
     *
     * @param sampleStates sample states to match
     * @param viewStates view states to match
     * @param instanceStates instance states to match
     * @return the created condition
     */
    public ReadCondition createReadCondition(Set<SampleState> sampleStates,
                                              Set<ViewState> viewStates,
                                              Set<InstanceState> instanceStates) {
        ReadCondition condition = new ReadCondition(this, sampleStates, viewStates, instanceStates);
        readConditions.add(condition);
        return condition;
    }

    /**
     * Deletes a ReadCondition.
     *
     * @param condition the condition to delete
     * @return OK if successful
     */
    public ReturnCode deleteReadCondition(ReadCondition condition) {
        if (readConditions.remove(condition)) {
            return ReturnCode.OK;
        }
        return ReturnCode.PRECONDITION_NOT_MET;
    }

    // ========== Instance Operations ==========

    /**
     * Looks up an instance handle by key.
     *
     * @param keyHolder data containing the key
     * @return the instance handle, or NIL if not found
     */
    public InstanceHandle lookupInstance(T keyHolder) {
        if (!topic.getTypeSupport().hasKey()) {
            return InstanceHandle.NIL;
        }
        Object key = topic.getTypeSupport().extractKey(keyHolder);
        InstanceInfo info = instanceInfoMap.get(key);
        return info != null ? info.handle : InstanceHandle.NIL;
    }

    // ========== Status Accessors ==========

    public LivelinessChangedStatus getLivelinessChangedStatus() {
        LivelinessChangedStatus current = livelinessChangedStatus;
        livelinessChangedStatus = new LivelinessChangedStatus(
                current.aliveCount(), 0, current.notAliveCount(), 0, current.lastPublicationHandle());
        statusCondition.clearStatus(StatusKind.LIVELINESS_CHANGED);
        return current;
    }

    public RequestedDeadlineMissedStatus getRequestedDeadlineMissedStatus() {
        RequestedDeadlineMissedStatus current = requestedDeadlineMissedStatus;
        requestedDeadlineMissedStatus = new RequestedDeadlineMissedStatus(
                current.totalCount(), 0, current.lastInstanceHandle());
        statusCondition.clearStatus(StatusKind.REQUESTED_DEADLINE_MISSED);
        return current;
    }

    public RequestedIncompatibleQosStatus getRequestedIncompatibleQosStatus() {
        RequestedIncompatibleQosStatus current = requestedIncompatibleQosStatus;
        requestedIncompatibleQosStatus = new RequestedIncompatibleQosStatus(
                current.totalCount(), 0, current.lastPolicyId(), current.policies());
        statusCondition.clearStatus(StatusKind.REQUESTED_INCOMPATIBLE_QOS);
        return current;
    }

    public SubscriptionMatchedStatus getSubscriptionMatchedStatus() {
        SubscriptionMatchedStatus current = subscriptionMatchedStatus;
        subscriptionMatchedStatus = new SubscriptionMatchedStatus(
                current.totalCount(), 0, current.currentCount(), 0, current.lastPublicationHandle());
        statusCondition.clearStatus(StatusKind.SUBSCRIPTION_MATCHED);
        return current;
    }

    public SampleRejectedStatus getSampleRejectedStatus() {
        SampleRejectedStatus current = sampleRejectedStatus;
        sampleRejectedStatus = new SampleRejectedStatus(
                current.totalCount(), 0, current.lastReason(), current.lastInstanceHandle());
        statusCondition.clearStatus(StatusKind.SAMPLE_REJECTED);
        return current;
    }

    public SampleLostStatus getSampleLostStatus() {
        SampleLostStatus current = sampleLostStatus;
        sampleLostStatus = new SampleLostStatus(current.totalCount(), 0);
        statusCondition.clearStatus(StatusKind.SAMPLE_LOST);
        return current;
    }

    // ========== Accessors ==========

    public Topic<T> getTopic() {
        return topic;
    }

    public Subscriber getSubscriber() {
        return subscriber;
    }

    public DataReaderQos getQos() {
        return qos.get();
    }

    public void setQos(DataReaderQos qos) {
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
    public void setListener(DataReaderListener listener, StatusMask mask) {
        this.listener.set(listener);
        this.listenerMask.set(mask != null ? mask : StatusMask.NONE);
    }

    @Override
    public DataReaderListener getListener() {
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
                rtpsReader.close();
            } catch (IOException e) {
                throw new DdsException(ReturnCode.ERROR, "Failed to close RTPS reader", e);
            }
        }
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new DdsException(ReturnCode.ALREADY_DELETED, "DataReader is closed");
        }
    }

    // ========== Internal ==========

    private void fetchFromRtps() {
        List<ReceivedSample<T>> newSamples = rtpsReader.drainWithMetadata();
        for (ReceivedSample<T> received : newSamples) {
            T data = received.data();
            InstanceHandle handle = getOrCreateInstanceHandle(data);
            InstanceInfo info = getOrCreateInstanceInfo(data);

            SampleInfo sampleInfo = new SampleInfo(
                    SampleState.NOT_READ,
                    info.isNew ? ViewState.NEW : ViewState.NOT_NEW,
                    InstanceState.ALIVE,
                    Instant.now(),
                    handle,
                    InstanceHandle.NIL, // publication handle not available in simple impl
                    0, 0, 0, 0, 0,
                    true
            );
            sampleCache.add(new CachedSample<>(data, sampleInfo, received.relatedSampleIdentity(), received.sequenceNumber(), received.writerGuid()));
            info.isNew = false;
        }
    }

    private InstanceHandle getOrCreateInstanceHandle(T data) {
        if (!topic.getTypeSupport().hasKey()) {
            return InstanceHandle.NIL;
        }
        Object key = topic.getTypeSupport().extractKey(data);
        return instanceInfoMap.computeIfAbsent(key, k ->
                new InstanceInfo(InstanceHandle.of(instanceCounter.getAndIncrement()))).handle;
    }

    private InstanceInfo getOrCreateInstanceInfo(T data) {
        if (!topic.getTypeSupport().hasKey()) {
            return instanceInfoMap.computeIfAbsent(null,
                    k -> new InstanceInfo(InstanceHandle.NIL));
        }
        Object key = topic.getTypeSupport().extractKey(data);
        return instanceInfoMap.computeIfAbsent(key, k ->
                new InstanceInfo(InstanceHandle.of(instanceCounter.getAndIncrement())));
    }

    private boolean matchesStates(CachedSample<T> cached, Set<SampleState> sampleStates,
                                  Set<ViewState> viewStates, Set<InstanceState> instanceStates) {
        return sampleStates.contains(cached.sampleState)
                && viewStates.contains(cached.viewState)
                && instanceStates.contains(cached.instanceState);
    }

    // Internal helper classes
    private static class CachedSample<T> {
        final T data;
        final SampleInfo originalInfo;
        final Optional<SampleIdentity> relatedSampleIdentity;
        final long writerSequenceNumber;
        final Guid writerGuid;
        SampleState sampleState;
        ViewState viewState;
        InstanceState instanceState;

        CachedSample(T data, SampleInfo info, Optional<SampleIdentity> relatedSampleIdentity, long writerSequenceNumber, Guid writerGuid) {
            this.data = data;
            this.originalInfo = info;
            this.relatedSampleIdentity = relatedSampleIdentity;
            this.writerSequenceNumber = writerSequenceNumber;
            this.writerGuid = writerGuid;
            this.sampleState = info.sampleState();
            this.viewState = info.viewState();
            this.instanceState = info.instanceState();
        }

        void markRead() {
            this.sampleState = SampleState.READ;
            this.viewState = ViewState.NOT_NEW;
        }

        Sample<T> toSample() {
            SampleInfo currentInfo = new SampleInfo(
                    sampleState, viewState, instanceState,
                    originalInfo.sourceTimestamp(),
                    originalInfo.instanceHandle(),
                    originalInfo.publicationHandle(),
                    originalInfo.disposedGenerationCount(),
                    originalInfo.noWritersGenerationCount(),
                    originalInfo.sampleRank(),
                    originalInfo.generationRank(),
                    originalInfo.absoluteGenerationRank(),
                    originalInfo.validData()
            );
            return new Sample<>(data, currentInfo, relatedSampleIdentity, writerSequenceNumber, Optional.of(writerGuid));
        }
    }

    private static class InstanceInfo {
        final InstanceHandle handle;
        boolean isNew = true;

        InstanceInfo(InstanceHandle handle) {
            this.handle = handle;
        }
    }
}
