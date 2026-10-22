package ddsj.rtps.runtime;

import ddsj.rtps.util.Closeables;
import ddsj.rtps.qos.EndpointQos.LivelinessKind;
import ddsj.rtps.qos.EndpointQos.ReliabilityKind;
import ddsj.rtps.qos.EndpointQos.DurabilityKind;
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
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
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
    private final Map<Guid, ReaderMatch> readerMatches = new HashMap<>();
    private final Map<Object, byte[]> registeredInstances = new HashMap<>();

    private static final class ReaderMatch {
        final long firstSequence;
        final long historicalLastSequence;
        boolean replayPending;

        ReaderMatch(long firstSequence, long historicalLastSequence, boolean replayPending) {
            this.firstSequence = firstSequence;
            this.historicalLastSequence = historicalLastSequence;
            this.replayPending = replayPending;
        }
    }

    /** Observe matches before allocating a sequence number, so live samples remain repairable. */
    private void refreshReaderMatches() {
        Set<Guid> current = new HashSet<>();
        for (var subscription : endpointResolver.matchingSubscriptions()) {
            Guid reader = subscription.endpointGuid();
            current.add(reader);
            readerMatches.computeIfAbsent(reader, ignored -> {
                boolean replay = endpoint.qos().durability() == DurabilityKind.TRANSIENT_LOCAL
                        && subscription.qos().durability() == DurabilityKind.TRANSIENT_LOCAL;
                return new ReaderMatch(replay ? history.firstSequenceNumber().orElse(userSequence.get())
                        : userSequence.get(), userSequence.get() - 1, replay);
            });
        }
        readerMatches.keySet().retainAll(current);
    }

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
            this.entityId = participant.allocateEntityId(true, serializer.hasKey());
            this.guidPrefix = participant.guidPrefix();
            this.endpointResolver = new EndpointResolver(endpoint, participant.remoteParticipants(),
                    participant.publications(), participant.subscriptions(), serializer.hasKey());
            this.endpoint = endpoint;
            this.serializer = serializer;
            this.transport = participant.transport();
            this.history = new WriterHistoryCache(endpoint.qos().history(), endpoint.qos().depth(), endpoint.resourceLimits());
            this.ackNackListener = new AckNackListener(
                    transport,
                    guidPrefix,
                    Set.of(entityId),
                    this::handleAckNack);
            opened.add(ackNackListener);
            this.nackFragListener = new NackFragListener(
                    transport,
                    guidPrefix,
                    Set.of(entityId),
                    this::handleNackFrag);
            opened.add(nackFragListener);
            this.fragmentSender = new FragmentSender(FragmentSender.DEFAULT_FRAGMENT_SIZE,
                    FragmentSender.DEFAULT_FRAGMENTATION_THRESHOLD, history::get, history::statusInfo, history::keyOnly);
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
        return writeInternal(value, relatedSampleIdentity, 0);
    }

    /** Sends a dispose notification while retaining the instance's writer registration. */
    public synchronized void dispose(T keyHolder) throws IOException {
        if (!serializer.hasKey()) throw new IllegalStateException("dispose requires a keyed type");
        writeInternal(keyHolder, null, 1);
    }

    /** Sends an unregister notification; readers track whether other writers remain. */
    public synchronized void unregisterInstance(T keyHolder) throws IOException {
        if (!serializer.hasKey()) throw new IllegalStateException("unregister requires a keyed type");
        writeInternal(keyHolder, null, 2);
    }

    private SampleIdentity writeInternal(T value, SampleIdentity relatedSampleIdentity, int statusInfo) throws IOException {
        if (!running.get()) {
            throw new IOException("writer is closed");
        }
        refreshReaderMatches();
        replayPendingHistory();
        boolean keyOnly = statusInfo != 0 && serializer.supportsKeySerialization();
        Object key = serializer.hasKey() ? Objects.requireNonNull(serializer.extractKey(value), "instance key") : null;
        byte[] payload = keyOnly ? serializer.serializeKey(value) : serializer.serialize(value);
        // Snapshot keys before callers can mutate arrays or other user-owned values.
        byte[] registrationPayload = serializer.hasKey() && (statusInfo & 2) == 0
                ? (serializer.supportsKeySerialization() ? serializer.serializeKey(value) : payload) : null;
        long sequenceNumber = userSequence.get();
        if (!history.tryPut(sequenceNumber, payload, relatedSampleIdentity, key, statusInfo, keyOnly)) {
            throw new ResourceLimitException("writer history resource limit reached");
        }
        userSequence.incrementAndGet();
        if (serializer.hasKey()) {
            if ((statusInfo & 2) != 0) registeredInstances.remove(key);
            else registeredInstances.put(key, registrationPayload.clone());
        }
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
        if (!running.get()) return;
        refreshReaderMatches();
        replayPendingHistory();
        if (endpoint.qos().reliability() == ReliabilityKind.RELIABLE) sendUserHeartbeat();
    }

    private void replayPendingHistory() throws IOException {
        for (var entry : readerMatches.entrySet()) {
            ReaderMatch match = entry.getValue();
            if (match.replayPending) {
                long first = Math.max(match.firstSequence, history.firstSequenceNumber().orElse(userSequence.get()));
                for (long sequence : history.sequences(first, match.historicalLastSequence)) {
                    var payload = history.get(sequence);
                    if (payload.isPresent()) sendDirectedData(entry.getKey(), sequence, payload.get());
                }
                match.replayPending = false;
            }
        }
    }

    private synchronized void handleAckNack(AckNack ackNack) {
        if (!running.get()) return;
        try {
            refreshReaderMatches();
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
            refreshReaderMatches();
            // NACK_FRAG carries the requester's prefix in the parsed message context.
            Guid reader = nackFrag.writerGuid().prefix().toGuid(nackFrag.readerId());
            ReaderMatch match = readerMatches.get(reader);
            if (match == null) return;
            if (nackFrag.writerSequenceNumber() < match.firstSequence) {
                sendGap(reader, nackFrag.writerSequenceNumber());
                return;
            }
            var relatedIdentity = history.relatedSampleIdentity(nackFrag.writerSequenceNumber()).orElse(null);
            if (relatedIdentity != null && !relatedIdentity.writerGuid().prefix().equals(reader.prefix())) return;
            boolean resent = fragmentSender.resendFragments(
                    guidPrefix,
                    nackFrag.readerId(),
                    entityId,
                    nackFrag.writerSequenceNumber(),
                    nackFrag.requestedFragmentNumbers(),
                    relatedIdentity,
                    message -> sendDirected(message, reader));
            if (!resent && history.get(nackFrag.writerSequenceNumber()).isEmpty()) {
                sendGap(reader, nackFrag.writerSequenceNumber());
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void resendRequestedSamples(AckNack ackNack) throws IOException {
        if (endpoint.qos().reliability() != ReliabilityKind.RELIABLE) {
            return;
        }
        if (ackNack.sourceGuidPrefix() == null) return;
        Guid reader = ackNack.sourceGuidPrefix().toGuid(ackNack.readerId());
        ReaderMatch match = readerMatches.get(reader);
        if (match == null) return;
        for (long sequenceNumber : ackNack.requestedSequenceNumbers()) {
            var payload = history.get(sequenceNumber);
            if (sequenceNumber >= match.firstSequence && payload.isPresent()) {
                sendDirectedData(reader, sequenceNumber, payload.get());
            } else {
                sendGap(reader, sequenceNumber);
            }
        }
    }

    private void sendDirectedData(Guid reader, long sequence, byte[] payload) throws IOException {
        var related = history.relatedSampleIdentity(sequence).orElse(null);
        if (related != null && !related.writerGuid().prefix().equals(reader.prefix())) return;
        if (fragmentSender.requiresFragmentation(payload)) {
            fragmentSender.sendFragmented(guidPrefix, reader.entityId(), entityId, sequence, payload,
                    related, message -> sendDirected(message, reader));
        } else {
            RtpsMessageBuilder message = new RtpsMessageBuilder(guidPrefix);
            message.infoTs(RtpsTimestamp.now());
            if (history.statusInfo(sequence) != 0) message.instanceChange(reader.entityId(), entityId,
                    sequence, payload, history.statusInfo(sequence), history.keyOnly(sequence));
            else if (related == null) message.data(reader.entityId(), entityId, sequence, payload);
            else message.dataWithRelatedSampleIdentity(reader.entityId(), entityId, sequence,
                    related.writerGuid(), related.sequenceNumber(), payload);
            sendDirected(message.bytes(), reader);
        }
    }

    /** INFO_DST prevents multicast repairs from reaching readers in other participants. */
    private void sendDirected(byte[] message, Guid reader) throws IOException {
        RtpsMessageBuilder header = new RtpsMessageBuilder(guidPrefix);
        header.infoDst(reader.prefix());
        byte[] prefix = header.bytes();
        byte[] directed = new byte[prefix.length + message.length - 20];
        System.arraycopy(prefix, 0, directed, 0, prefix.length);
        System.arraycopy(message, 20, directed, prefix.length, message.length - 20);
        transport.sendUserData(directed);
        var remote = participant.remoteParticipants().get(reader.prefix());
        if (remote.isPresent()) {
            for (var locator : remote.get().userUnicast()) transport.send(directed, locator);
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
        if (history.statusInfo(sequenceNumber) != 0) {
            message.instanceChange(RtpsEntity.UNKNOWN, entityId, sequenceNumber, payload,
                    history.statusInfo(sequenceNumber), history.keyOnly(sequenceNumber));
            sendToUserLocators(message.bytes());
        } else if (relatedSampleIdentity != null) {
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
        refreshReaderMatches();
        for (var entry : readerMatches.entrySet()) {
            RtpsMessageBuilder message = new RtpsMessageBuilder(guidPrefix);
            message.heartbeat(entry.getKey().entityId(), entityId,
                    Math.max(entry.getValue().firstSequence, history.firstSequenceNumber().orElse(userSequence.get())),
                    userSequence.get() - 1, (int) userHeartbeatCount.getAndIncrement());
            sendDirected(message.bytes(), entry.getKey());
        }
    }

    private synchronized void sendLivelinessHeartbeat() {
        if (!running.get()) return;
        try {
            sendUserHeartbeat();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void sendGap(Guid reader, long sequenceNumber) throws IOException {
        RtpsMessageBuilder message = new RtpsMessageBuilder(guidPrefix);
        message.gap(reader.entityId(), entityId, sequenceNumber);
        sendDirected(message.bytes(), reader);
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
        if (!running.get()) return;
        List<Closeable> resources = new ArrayList<>();
        // Closing a writer unregisters its instances. Shutdown notifications do not need
        // cache space: the writer's retained history is released immediately afterwards.
        for (byte[] payload : registeredInstances.values()) {
            try {
                boolean keyOnly = serializer.supportsKeySerialization();
                long sequence = userSequence.getAndIncrement();
                if (fragmentSender.requiresFragmentation(payload)) {
                    var sender = new FragmentSender(FragmentSender.DEFAULT_FRAGMENT_SIZE,
                            FragmentSender.DEFAULT_FRAGMENTATION_THRESHOLD, ignored -> Optional.empty(),
                            ignored -> 2, ignored -> keyOnly);
                    sender.sendFragmented(guidPrefix, RtpsEntity.UNKNOWN, entityId, sequence,
                            payload, message -> sendToUserLocators(message));
                } else {
                    var message = new RtpsMessageBuilder(guidPrefix);
                    message.instanceChange(RtpsEntity.UNKNOWN, entityId, sequence, payload, 2, keyOnly);
                    sendToUserLocators(message.bytes());
                }
            } catch (IOException | RuntimeException failure) {
                resources.add(() -> { throw new IOException("Failed to unregister instance on close", failure); });
            }
        }
        if (!running.compareAndSet(true, false)) {
            return;
        }
        resources.add(livelinessAsserter);
        resources.add(() -> participant.unregister(guid(), true));
        resources.add(nackFragListener);
        resources.add(ackNackListener);
        if (ownsParticipant) resources.add(participant);
        try {
            Closeables.closeAll(resources);
        } finally {
            history.clear();
            readerMatches.clear();
            registeredInstances.clear();
        }
    }
}
