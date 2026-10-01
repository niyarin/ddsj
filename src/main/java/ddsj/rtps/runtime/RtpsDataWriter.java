package ddsj.rtps.runtime;

import ddsj.rtps.util.Closeables;
import ddsj.rtps.qos.EndpointQos.LivelinessKind;
import ddsj.rtps.qos.EndpointQos.ReliabilityKind;
import ddsj.rtps.discovery.LocalEndpoint;
import ddsj.rtps.discovery.RemoteParticipant;
import ddsj.rtps.history.WriterHistoryCache;
import ddsj.rtps.message.AckNack;
import ddsj.rtps.message.AckNackListener;
import ddsj.rtps.message.FragmentSender;
import ddsj.rtps.message.NackFrag;
import ddsj.rtps.message.NackFragListener;
import ddsj.rtps.message.RtpsMessageBuilder;
import ddsj.rtps.message.SampleIdentity;
import ddsj.rtps.protocol.RtpsEntity;
import ddsj.rtps.transport.RtpsParticipantConfig;
import ddsj.rtps.transport.RtpsTransport;
import ddsj.rtps.types.Guid;
import ddsj.rtps.types.EntityId;
import ddsj.rtps.types.GuidPrefix;
import ddsj.rtps.types.RtpsTimestamp;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public final class RtpsDataWriter<T> implements Closeable {

    private final LocalEndpoint endpoint;
    private final PayloadSerializer<T> serializer;
    private final RtpsTransport transport;
    private final RtpsParticipant participant;
    private final boolean ownsParticipant;
    private final EntityId entityId;
    private final GuidPrefix guidPrefix;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong userSequence = new AtomicLong(1);
    private final AtomicLong userHeartbeatCount = new AtomicLong(1);
    private final WriterHistoryCache history;
    private final EndpointResolver endpointResolver;
    private final AckNackListener ackNackListener;
    private final NackFragListener nackFragListener;
    private final FragmentSender fragmentSender;
    private final LivelinessAsserter livelinessAsserter;

    public RtpsDataWriter(RtpsParticipantConfig config, LocalEndpoint endpoint, PayloadSerializer<T> serializer) throws IOException {
        this(RtpsParticipant.owned(config, endpoint, serializer), endpoint, serializer, true);
    }

    public RtpsDataWriter(LocalEndpoint endpoint, PayloadSerializer<T> serializer) throws IOException {
        this(new RtpsParticipantConfig(0), endpoint, serializer);
    }

    public RtpsDataWriter(RtpsParticipantConfig config, LocalEndpoint endpoint, PayloadSerializer<T> serializer, RtpsTransport transport) throws IOException {
        this(RtpsParticipant.owned(config, endpoint, serializer, transport), endpoint, serializer, true);
    }

    RtpsDataWriter(RtpsParticipant participant, LocalEndpoint endpoint, PayloadSerializer<T> serializer, boolean ownsParticipant) throws IOException {
        Objects.requireNonNull(endpoint, "endpoint");
        Objects.requireNonNull(serializer, "serializer");
        this.participant = participant;
        this.ownsParticipant = ownsParticipant;
        List<Closeable> opened = new ArrayList<>();
        if (ownsParticipant) opened.add(participant);
        try {
            this.entityId = participant.allocateEntityId(true);
            this.guidPrefix = participant.guidPrefix();
            this.endpointResolver = new EndpointResolver(endpoint, participant.remoteParticipants(),
                    participant.publications(), participant.subscriptions());
            this.endpoint = endpoint;
            this.serializer = serializer;
            this.transport = participant.transport();
            this.history = new WriterHistoryCache(endpoint.qos().history(), endpoint.qos().depth(), endpoint.resourceLimits());
            this.ackNackListener = new AckNackListener(
                    transport,
                    Set.of(entityId),
                    this::handleAckNack);
            opened.add(ackNackListener);
            this.nackFragListener = new NackFragListener(
                    transport,
                    Set.of(entityId),
                    this::handleNackFrag);
            opened.add(nackFragListener);
            this.fragmentSender = new FragmentSender(FragmentSender.DEFAULT_FRAGMENT_SIZE, FragmentSender.DEFAULT_FRAGMENTATION_THRESHOLD, history::get);
            this.livelinessAsserter = new LivelinessAsserter(
                    endpoint.qos().liveliness(),
                    endpoint.qos().leaseDuration(),
                    this::sendLivelinessHeartbeat);
            opened.add(livelinessAsserter);
            participant.register(guid(), endpoint, this, true);
            running.set(true);
        } catch (IOException | RuntimeException e) {
            Closeables.rollback(e, opened);
            throw e;
        }
    }

    public synchronized void write(T value) throws IOException {
        write(value, null);
    }

    /**
     * Writes a value with an optional related sample identity.
     * Used for DDS-RPC service responses to correlate with requests.
     *
     * @param value the value to write
     * @param relatedSampleIdentity for service responses, identifies the related request
     */
    public synchronized void write(T value, SampleIdentity relatedSampleIdentity) throws IOException {
        writeInternal(value, relatedSampleIdentity);
    }

    /**
     * Writes a request and returns the identity used in its DATA or DATA_FRAG messages.
     * The sequence number shares the same sequence space as ordinary writes.
     * Successful return does not imply delivery or acknowledgement by a reader.
     *
     * @param value the request to write
     * @return the actual writer GUID and sequence number of the request
     * @throws IOException if the writer is closed, history is full, or sending fails
     */
    public synchronized SampleIdentity writeRequest(T value) throws IOException {
        return writeInternal(value, null);
    }

    private SampleIdentity writeInternal(T value, SampleIdentity relatedSampleIdentity) throws IOException {
        if (!running.get()) {
            throw new IOException("writer is closed");
        }
        byte[] payload = serializer.serialize(value);
        long sequenceNumber = userSequence.get();
        if (!history.tryPut(sequenceNumber, payload, relatedSampleIdentity)) {
            throw new IOException("writer history resource limit reached");
        }
        userSequence.incrementAndGet();
        sendUserData(sequenceNumber, payload, relatedSampleIdentity);
        livelinessAsserter.onDataWritten();
        return new SampleIdentity(guid(), sequenceNumber);
    }

    /**
     * Manually asserts liveliness. Required for MANUAL_BY_TOPIC liveliness kind.
     * For AUTOMATIC kind, this is a no-op as liveliness is asserted automatically on write().
     */
    public void assertLiveliness() {
        livelinessAsserter.assertLiveliness();
    }

    /**
     * Returns the liveliness kind configured for this writer.
     */
    public LivelinessKind livelinessKind() {
        return endpoint.qos().liveliness();
    }

    synchronized void announceHeartbeat() throws IOException {
        if (running.get() && endpoint.qos().reliability() == ReliabilityKind.RELIABLE) sendUserHeartbeat();
    }

    private synchronized void handleAckNack(AckNack ackNack) {
        if (!running.get()) return;
        try {
            if (ackNack.writerId().equals(entityId)) {
                resendRequestedSamples(ackNack);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private synchronized void handleNackFrag(NackFrag nackFrag) {
        if (!running.get()) return;
        if (endpoint.qos().reliability() != ReliabilityKind.RELIABLE) {
            return;
        }
        try {
            var relatedIdentity = history.relatedSampleIdentity(nackFrag.writerSequenceNumber()).orElse(null);
            boolean resent = fragmentSender.resendFragments(
                    guidPrefix,
                    nackFrag.readerId(),
                    entityId,
                    nackFrag.writerSequenceNumber(),
                    nackFrag.requestedFragmentNumbers(),
                    relatedIdentity,
                    message -> sendToUserLocators(message, relatedIdentity == null
                            ? null : relatedIdentity.writerGuid().prefix()));
            if (!resent && history.get(nackFrag.writerSequenceNumber()).isEmpty()) {
                sendGap(nackFrag.readerId(), nackFrag.writerSequenceNumber());
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void resendRequestedSamples(AckNack ackNack) throws IOException {
        if (endpoint.qos().reliability() != ReliabilityKind.RELIABLE) {
            return;
        }
        for (long sequenceNumber : ackNack.requestedSequenceNumbers()) {
            var payload = history.get(sequenceNumber);
            if (payload.isPresent()) {
                sendUserData(sequenceNumber, payload.get(), history.relatedSampleIdentity(sequenceNumber).orElse(null));
            } else {
                sendGap(ackNack.readerId(), sequenceNumber);
            }
        }
    }

    private void sendUserData(long sequenceNumber, byte[] payload, SampleIdentity relatedSampleIdentity) throws IOException {
        if (fragmentSender.requiresFragmentation(payload)) {
            sendUserDataFragmented(sequenceNumber, payload, relatedSampleIdentity);
        } else {
            sendUserDataComplete(sequenceNumber, payload, relatedSampleIdentity);
        }
    }

    private void sendUserDataComplete(long sequenceNumber, byte[] payload, SampleIdentity relatedSampleIdentity) throws IOException {
        RtpsMessageBuilder message = new RtpsMessageBuilder(guidPrefix);
        message.infoTs(RtpsTimestamp.now());
        if (relatedSampleIdentity != null) {
            message.dataWithRelatedSampleIdentity(
                    RtpsEntity.UNKNOWN, entityId, sequenceNumber,
                    relatedSampleIdentity.writerGuid(), relatedSampleIdentity.sequenceNumber(),
                    payload);
            // For DDS-RPC responses: send directly to the client's participant using the
            // GUID from related_sample_identity, even if SEDP discovery hasn't completed yet.
            // This avoids a race condition where the response is sent before SEDP discovers
            // the client's reply subscription.
            sendToUserLocators(message.bytes(), relatedSampleIdentity.writerGuid().prefix());
        } else {
            message.data(RtpsEntity.UNKNOWN, entityId, sequenceNumber, payload);
            sendToUserLocators(message.bytes(), null);
        }
    }

    private void sendUserDataFragmented(long sequenceNumber, byte[] payload, SampleIdentity relatedSampleIdentity) throws IOException {
        fragmentSender.sendFragmented(
                guidPrefix,
                RtpsEntity.UNKNOWN,
                entityId,
                sequenceNumber,
                payload,
                relatedSampleIdentity,
                message -> sendToUserLocators(message, relatedSampleIdentity == null
                        ? null : relatedSampleIdentity.writerGuid().prefix()));
    }

    private void sendUserHeartbeat() throws IOException {
        var firstSequenceNumber = history.firstSequenceNumber();
        var lastSequenceNumber = history.lastSequenceNumber();
        if (firstSequenceNumber.isEmpty() || lastSequenceNumber.isEmpty()) {
            return;
        }
        RtpsMessageBuilder message = new RtpsMessageBuilder(guidPrefix);
        message.heartbeat(
                RtpsEntity.UNKNOWN,
                entityId,
                firstSequenceNumber.get(),
                lastSequenceNumber.get(),
                (int) userHeartbeatCount.getAndIncrement());
        sendToUserLocators(message.bytes());
    }

    private synchronized void sendLivelinessHeartbeat() {
        if (!running.get()) return;
        try {
            sendUserHeartbeat();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void sendGap(EntityId readerId, long sequenceNumber) throws IOException {
        RtpsMessageBuilder message = new RtpsMessageBuilder(guidPrefix);
        message.gap(readerId, entityId, sequenceNumber);
        sendToUserLocators(message.bytes());
    }

    private void sendToUserLocators(byte[] message) throws IOException {
        sendToUserLocators(message, null);
    }

    private void sendToUserLocators(byte[] message, GuidPrefix targetParticipant) throws IOException {
        transport.sendUserData(message);

        // If we have a specific target participant (e.g., for DDS-RPC responses),
        // send to it directly even if SEDP hasn't discovered it yet
        if (targetParticipant != null) {
            var targetOpt = participant.remoteParticipants().get(targetParticipant);
            if (targetOpt.isPresent()) {
                var target = targetOpt.get();
                for (var locator : target.userUnicast()) {
                    transport.send(message, locator);
                }
                return;
            }
        }

        var subs = endpointResolver.participantsForSubscriptions();
        for (RemoteParticipant remoteParticipant : subs) {
            for (var locator : remoteParticipant.userUnicast()) {
                transport.send(message, locator);
            }
        }
    }

    /** Identity allocated by the owning participant. */
    public Guid guid() { return guidPrefix.toGuid(entityId); }

    @Override
    public synchronized void close() throws IOException {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        List<Closeable> resources = new ArrayList<>();
        resources.add(livelinessAsserter);
        resources.add(() -> participant.unregister(guid(), true));
        resources.add(nackFragListener);
        resources.add(ackNackListener);
        if (ownsParticipant) resources.add(participant);
        try {
            Closeables.closeAll(resources);
        } finally {
            history.clear();
        }
    }
}
