package ddsjdk.rtps.discovery;

import ddsjdk.rtps.message.AckNack;
import ddsjdk.rtps.message.RtpsMessageBuilder;
import ddsjdk.rtps.parameter.RtpsParameterLists;
import ddsjdk.rtps.protocol.ParameterId;
import ddsjdk.rtps.protocol.RtpsEntity;
import ddsjdk.rtps.transport.RtpsTransport;
import ddsjdk.rtps.types.EntityId;
import ddsjdk.rtps.types.Guid;
import ddsjdk.rtps.types.GuidPrefix;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/** A participant's SEDP builtin writer, with one sequence space for all its endpoints. */
public final class SedpEndpointAnnouncer {
    private final RtpsTransport transport;
    private final GuidPrefix prefix;
    private final Iterable<RemoteParticipant> participants;
    private final EntityId readerId;
    private final EntityId writerId;
    private final Map<Guid, Change> changes = new LinkedHashMap<>();
    private long sequence;
    private int heartbeatCount;

    public SedpEndpointAnnouncer(RtpsTransport transport, GuidPrefix prefix,
            Iterable<RemoteParticipant> participants, boolean publications) {
        this.transport = transport;
        this.prefix = prefix;
        this.participants = participants;
        readerId = publications ? RtpsEntity.PUBLICATIONS_BUILTIN_TOPIC_READER : RtpsEntity.SUBSCRIPTIONS_BUILTIN_TOPIC_READER;
        writerId = publications ? RtpsEntity.PUBLICATIONS_BUILTIN_TOPIC_WRITER : RtpsEntity.SUBSCRIPTIONS_BUILTIN_TOPIC_WRITER;
    }

    public synchronized void register(Guid guid, LocalEndpoint endpoint) {
        byte[] payload = RtpsParameterLists.payload(writer -> {
            writer.stringParameter(ParameterId.TOPIC_NAME, endpoint.topicName());
            writer.stringParameter(ParameterId.TYPE_NAME, endpoint.typeName());
            writer.parameter(ParameterId.PROTOCOL_VERSION, new byte[]{2, 5, 0, 0});
            writer.parameter(ParameterId.VENDOR_ID, new byte[]{1, 16, 0, 0});
            RtpsQosParameters.write(writer, endpoint.qos());
            writer.parameter(ParameterId.ENDPOINT_GUID, guid.bytes());
        });
        changes.put(guid, new Change(guid, ++sequence, payload));
    }

    public synchronized void unregister(Guid guid) throws IOException {
        if (!changes.containsKey(guid)) return;
        Change disposed = new Change(guid, ++sequence, null);
        changes.put(guid, disposed);
        send(sampleMessage(disposed));
    }

    public synchronized void announce() throws IOException {
        // Separate packets also keep large endpoint lists below the datagram size limit.
        for (Change change : changes.values()) send(sampleMessage(change));
        if (changes.isEmpty()) return;
        RtpsMessageBuilder message = new RtpsMessageBuilder(prefix);
        long first = changes.values().stream().mapToLong(Change::sequence).min().orElse(1);
        message.heartbeat(readerId, writerId, first, sequence, ++heartbeatCount);
        send(message.bytes());
    }

    public synchronized void respondTo(AckNack ack) throws IOException {
        for (long requested : ack.requestedSequenceNumbers()) {
            Change change = changes.values().stream().filter(c -> c.sequence == requested).findFirst().orElse(null);
            if (change != null) send(sampleMessage(change));
            else {
                RtpsMessageBuilder gap = new RtpsMessageBuilder(prefix);
                gap.gap(ack.readerId(), writerId, requested);
                send(gap.bytes());
            }
        }
    }

    private byte[] sampleMessage(Change change) {
        RtpsMessageBuilder message = new RtpsMessageBuilder(prefix);
        if (change.payload == null) message.dataDispose(readerId, writerId, change.sequence, change.guid);
        else message.data(readerId, writerId, change.sequence, change.payload);
        return message.bytes();
    }

    private void send(byte[] bytes) throws IOException {
        transport.sendMetatraffic(bytes);
        for (RemoteParticipant participant : participants) {
            for (var locator : participant.metatrafficUnicast()) transport.send(bytes, locator);
        }
    }

    private record Change(Guid guid, long sequence, byte[] payload) { }
}
