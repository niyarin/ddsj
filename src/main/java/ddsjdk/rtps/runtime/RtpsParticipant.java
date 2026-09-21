package ddsjdk.rtps.runtime;

import ddsjdk.rtps.util.Closeables;
import ddsjdk.rtps.discovery.LocalEndpoint;
import ddsjdk.rtps.discovery.RemoteEndpointStore;
import ddsjdk.rtps.discovery.RemoteParticipantStore;
import ddsjdk.rtps.discovery.RemotePublication;
import ddsjdk.rtps.discovery.RemoteSubscription;
import ddsjdk.rtps.discovery.SedpEndpointAnnouncer;
import ddsjdk.rtps.discovery.SpdpAnnouncer;
import ddsjdk.rtps.discovery.SpdpDiscoveryListener;
import ddsjdk.rtps.message.AckNackListener;
import ddsjdk.rtps.protocol.RtpsEntity;
import ddsjdk.rtps.transport.RtpsParticipantConfig;
import ddsjdk.rtps.transport.RtpsTransport;
import ddsjdk.rtps.transport.UdpRtpsTransport;
import ddsjdk.rtps.types.EntityId;
import ddsjdk.rtps.types.Guid;
import ddsjdk.rtps.types.GuidPrefix;
import ddsjdk.rtps.types.RtpsGuid;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/** Owns transport, discovery and endpoint identities. Closing it closes all its endpoints. */
public final class RtpsParticipant implements Closeable {
    private final GuidPrefix prefix = RtpsGuid.newGuidPrefix();
    private final ParticipantTransport transport;
    private final RemoteParticipantStore remoteParticipants = new RemoteParticipantStore();
    private final RemoteEndpointStore<RemotePublication> publications = new RemoteEndpointStore<>();
    private final RemoteEndpointStore<RemoteSubscription> subscriptions = new RemoteEndpointStore<>();
    private final SpdpAnnouncer spdp;
    private final SpdpDiscoveryListener discovery;
    private final SedpEndpointAnnouncer publicationAnnouncer;
    private final SedpEndpointAnnouncer subscriptionAnnouncer;
    private final AckNackListener discoveryAcks;
    private final ScheduledExecutorService scheduler;
    private final Map<Guid, Closeable> endpoints = new LinkedHashMap<>();
    private int nextWriterKey = 2;
    private int nextReaderKey = 2;
    private boolean closed;

    public RtpsParticipant(RtpsParticipantConfig config) throws IOException {
        this(config, new UdpRtpsTransport(Objects.requireNonNull(config, "config")));
    }

    /** Takes ownership of transport, including cleanup if initialization fails. */
    public RtpsParticipant(RtpsParticipantConfig config, RtpsTransport transport) throws IOException {
        Objects.requireNonNull(config, "config");
        this.transport = new ParticipantTransport(Objects.requireNonNull(transport, "transport"));
        List<Closeable> opened = new ArrayList<>();
        opened.add(this.transport);
        ScheduledExecutorService openedScheduler = null;
        try {
            spdp = new SpdpAnnouncer(config, this.transport, prefix);
            publicationAnnouncer = new SedpEndpointAnnouncer(this.transport, prefix, remoteParticipants, true);
            subscriptionAnnouncer = new SedpEndpointAnnouncer(this.transport, prefix, remoteParticipants, false);
            discovery = new SpdpDiscoveryListener(this.transport, prefix, remoteParticipants, publications, subscriptions);
            opened.add(discovery);
            discoveryAcks = new AckNackListener(this.transport,
                    Set.of(RtpsEntity.PUBLICATIONS_BUILTIN_TOPIC_WRITER, RtpsEntity.SUBSCRIPTIONS_BUILTIN_TOPIC_WRITER), ack -> {
                        try {
                            if (ack.writerId().equals(RtpsEntity.PUBLICATIONS_BUILTIN_TOPIC_WRITER)) publicationAnnouncer.respondTo(ack);
                            else subscriptionAnnouncer.respondTo(ack);
                        } catch (IOException e) { throw new UncheckedIOException(e); }
                    });
            opened.add(discoveryAcks);
            scheduler = Executors.newSingleThreadScheduledExecutor(task -> {
                Thread thread = new Thread(task, "ddsjdk-rtps-participant-announcer");
                thread.setDaemon(true);
                return thread;
            });
            openedScheduler = scheduler;
            scheduler.scheduleWithFixedDelay(this::announce, 1, 1, TimeUnit.SECONDS);
        } catch (IOException | RuntimeException e) {
            if (openedScheduler != null) openedScheduler.shutdownNow();
            Closeables.rollback(e, opened);
            throw e;
        }
    }

    public GuidPrefix guidPrefix() { return prefix; }

    public synchronized <T> RtpsDataWriter<T> createWriter(LocalEndpoint endpoint, PayloadSerializer<T> serializer) throws IOException {
        ensureOpen();
        return new RtpsDataWriter<>(this, endpoint, serializer, false);
    }

    public <T> RtpsDataReader<T> createReader(LocalEndpoint endpoint, PayloadSerializer<T> serializer) throws IOException {
        return createReader(endpoint, serializer, ReaderListeners.DEFAULT);
    }

    /** Creates a reader with named notification listeners. */
    public synchronized <T> RtpsDataReader<T> createReader(LocalEndpoint endpoint, PayloadSerializer<T> serializer,
            ReaderListeners listeners) throws IOException {
        ensureOpen();
        return new RtpsDataReader<>(this, endpoint, serializer, false, listeners);
    }

    /** @deprecated Use {@link #createReader(LocalEndpoint, PayloadSerializer, ReaderListeners)}. */
    @Deprecated
    public <T> RtpsDataReader<T> createReader(LocalEndpoint endpoint, PayloadSerializer<T> serializer,
            Consumer<DeadlineMissedStatus> onDeadlineMissed,
            Consumer<LivelinessChangedStatus> onLivelinessChanged) throws IOException {
        return createReader(endpoint, serializer, ReaderListeners.legacy(onDeadlineMissed, onLivelinessChanged));
    }

    static RtpsParticipant owned(RtpsParticipantConfig config, LocalEndpoint endpoint,
            PayloadSerializer<?> serializer, RtpsTransport transport) throws IOException {
        Objects.requireNonNull(endpoint, "endpoint");
        Objects.requireNonNull(serializer, "serializer");
        return new RtpsParticipant(config, transport);
    }

    static RtpsParticipant owned(RtpsParticipantConfig config, LocalEndpoint endpoint,
            PayloadSerializer<?> serializer) throws IOException {
        Objects.requireNonNull(endpoint, "endpoint");
        Objects.requireNonNull(serializer, "serializer");
        return new RtpsParticipant(config);
    }

    synchronized EntityId allocateEntityId(boolean writer) {
        ensureOpen();
        int key = writer ? nextWriterKey++ : nextReaderKey++;
        if (key > 0xffffff) throw new IllegalStateException("participant entity IDs exhausted");
        return new EntityId(new byte[]{(byte) (key >>> 16), (byte) (key >>> 8), (byte) key, (byte) (writer ? 3 : 4)});
    }

    synchronized void register(Guid guid, LocalEndpoint endpoint, Closeable resource, boolean writer) {
        ensureOpen();
        if (writer) {
            publicationAnnouncer.register(guid, endpoint);
            publications.upsert(new RemotePublication(guid, endpoint.topicName(), endpoint.typeName(), endpoint.qos()));
        } else {
            subscriptionAnnouncer.register(guid, endpoint);
            subscriptions.upsert(new RemoteSubscription(guid, endpoint.topicName(), endpoint.typeName(), endpoint.qos()));
        }
        endpoints.put(guid, resource);
    }

    void unregister(Guid guid, boolean writer) throws IOException {
        synchronized (this) {
            if (endpoints.remove(guid) == null) return;
            if (writer) publications.remove(guid);
            else subscriptions.remove(guid);
        }
        if (writer) publicationAnnouncer.unregister(guid);
        else subscriptionAnnouncer.unregister(guid);
    }

    RtpsTransport transport() { return transport; }
    RemoteParticipantStore remoteParticipants() { return remoteParticipants; }
    RemoteEndpointStore<RemotePublication> publications() { return publications; }
    RemoteEndpointStore<RemoteSubscription> subscriptions() { return subscriptions; }

    void announce() {
        List<Closeable> snapshot;
        synchronized (this) {
            if (closed) return;
            snapshot = new ArrayList<>(endpoints.values());
        }
        try {
            spdp.announce();
            publicationAnnouncer.announce();
            subscriptionAnnouncer.announce();
            for (Closeable endpoint : snapshot) {
                if (endpoint instanceof RtpsDataWriter<?> writer) writer.announceHeartbeat();
            }
        } catch (IOException | RuntimeException e) {
            System.getLogger(RtpsParticipant.class.getName()).log(System.Logger.Level.WARNING, "RTPS announcement failed", e);
        }
    }

    private void ensureOpen() {
        if (closed) throw new IllegalStateException("participant is closed");
    }

    @Override
    public void close() throws IOException {
        List<Closeable> resources;
        synchronized (this) {
            if (closed) return;
            closed = true;
            resources = new ArrayList<>(endpoints.values());
        }
        scheduler.shutdownNow();
        resources.add(discoveryAcks);
        resources.add(discovery);
        resources.add(transport);
        Closeables.closeAll(resources);
    }
}
