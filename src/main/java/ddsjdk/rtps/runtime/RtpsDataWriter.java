package ddsjdk.rtps.runtime;

import ddsjdk.rtps.discovery.EndpointQos.LivelinessKind;
import ddsjdk.rtps.discovery.EndpointQos.ReliabilityKind;
import ddsjdk.rtps.discovery.LocalEndpoint;
import ddsjdk.rtps.discovery.RemoteParticipant;
import ddsjdk.rtps.discovery.RemoteParticipantStore;
import ddsjdk.rtps.discovery.RemoteEndpointStore;
import ddsjdk.rtps.discovery.RemoteSubscription;
import ddsjdk.rtps.history.WriterHistoryCache;
import ddsjdk.rtps.message.AckNack;
import ddsjdk.rtps.message.AckNackListener;
import ddsjdk.rtps.message.FragmentSender;
import ddsjdk.rtps.message.NackFrag;
import ddsjdk.rtps.message.NackFragListener;
import ddsjdk.rtps.message.RtpsMessageBuilder;
import ddsjdk.rtps.protocol.RtpsEntity;
import ddsjdk.rtps.transport.RtpsParticipantConfig;
import ddsjdk.rtps.transport.RtpsTransport;
import ddsjdk.rtps.types.Guid;
import ddsjdk.rtps.types.EntityId;
import ddsjdk.rtps.types.GuidPrefix;
import ddsjdk.rtps.types.RtpsTimestamp;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
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
    private final RemoteParticipantStore remoteParticipants;
    private final RemoteEndpointStore<RemoteSubscription> remoteSubscriptions;
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
            this.remoteParticipants = participant.remoteParticipants();
            this.remoteSubscriptions = participant.subscriptions();
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
            RtpsParticipant.cleanupInitialization(e, opened);
            throw e;
        }
    }

    public synchronized void write(T value) throws IOException {
        if (!running.get()) {
            throw new IOException("writer is closed");
        }
        byte[] payload = serializer.serialize(value);
        long sequenceNumber = userSequence.get();
        if (!history.tryPut(sequenceNumber, payload)) {
            throw new IOException("writer history resource limit reached");
        }
        userSequence.incrementAndGet();
        sendUserData(sequenceNumber, payload);
        livelinessAsserter.onDataWritten();
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
            boolean resent = fragmentSender.resendFragments(
                    guidPrefix,
                    nackFrag.readerId(),
                    entityId,
                    nackFrag.writerSequenceNumber(),
                    nackFrag.requestedFragmentNumbers(),
                    msg -> {
                        try {
                            sendToUserLocators(msg);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
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
                sendUserData(sequenceNumber, payload.get());
            } else {
                sendGap(ackNack.readerId(), sequenceNumber);
            }
        }
    }

    private void sendUserData(long sequenceNumber, byte[] payload) throws IOException {
        if (fragmentSender.requiresFragmentation(payload)) {
            sendUserDataFragmented(sequenceNumber, payload);
        } else {
            sendUserDataComplete(sequenceNumber, payload);
        }
    }

    private void sendUserDataComplete(long sequenceNumber, byte[] payload) throws IOException {
        RtpsMessageBuilder message = new RtpsMessageBuilder(guidPrefix);
        message.infoTs(RtpsTimestamp.now());
        message.data(RtpsEntity.UNKNOWN, entityId, sequenceNumber, payload);
        sendToUserLocators(message.bytes());
    }

    private void sendUserDataFragmented(long sequenceNumber, byte[] payload) throws IOException {
        fragmentSender.sendFragmented(
                guidPrefix,
                RtpsEntity.UNKNOWN,
                entityId,
                sequenceNumber,
                payload,
                msg -> {
                    try {
                        sendToUserLocators(msg);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
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
        transport.sendUserData(message);
        for (RemoteParticipant participant : remoteParticipantsForMatchedSubscriptions()) {
            for (var locator : participant.userUnicast()) {
                transport.send(message, locator);
            }
        }
    }

    private Set<RemoteParticipant> remoteParticipantsForMatchedSubscriptions() {
        Set<GuidPrefix> matchedPrefixes = new HashSet<>();
        for (RemoteSubscription subscription : remoteSubscriptions) {
            if (subscription.topicName().equals(endpoint.topicName())
                    && subscription.typeName().equals(endpoint.typeName())
                    && endpoint.qos().isCompatibleWithRequested(subscription.qos())) {
                matchedPrefixes.add(subscription.endpointGuid().prefix());
            }
        }
        if (matchedPrefixes.isEmpty()) {
            return Set.of();
        }
        Set<RemoteParticipant> result = new HashSet<>();
        for (RemoteParticipant participant : remoteParticipants) {
            if (matchedPrefixes.contains(participant.guidPrefix())) {
                result.add(participant);
            }
        }
        return result;
    }

    /** Identity allocated by the owning participant. */
    public Guid guid() { return guidPrefix.toGuid(entityId); }

    @Override
    public synchronized void close() throws IOException {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        livelinessAsserter.close();
        IOException first = null;
        first = closeOrCapture(() -> participant.unregister(guid(), true), first);
        first = closeOrCapture(nackFragListener, first);
        first = closeOrCapture(ackNackListener, first);
        if (ownsParticipant) first = closeOrCapture(participant, first);
        history.clear();
        if (first != null) {
            throw first;
        }
    }

    private static IOException closeOrCapture(Closeable closeable, IOException first) {
        try {
            closeable.close();
        } catch (IOException e) {
            if (first == null) {
                return e;
            }
            first.addSuppressed(e);
        }
        return first;
    }
}
