package ddsj.rtps.discovery;

import ddsj.rtps.message.AckNack;
import ddsj.rtps.message.RtpsMessageBuilder;
import ddsj.rtps.protocol.RtpsEntity;
import ddsj.rtps.transport.RtpsTransport;
import ddsj.rtps.types.EntityId;
import ddsj.rtps.types.GuidPrefix;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/** Preserves the single-endpoint announcement and disposal semantics of the legacy API. */
final class SedpSingleEndpointAnnouncer {
    private final EntityId readerId;
    private final EntityId writerId;
    private final EntityId endpointId;
    private final LocalEndpoint endpoint;
    private final RtpsTransport transport;
    private final GuidPrefix guidPrefix;
    private final Iterable<RemoteParticipant> remoteParticipants;
    private final SedpEndpointSampleHistory endpointHistory = new SedpEndpointSampleHistory();
    private final AtomicLong heartbeatCount = new AtomicLong(1);

    SedpSingleEndpointAnnouncer(
            LocalEndpoint endpoint,
            RtpsTransport transport,
            GuidPrefix guidPrefix,
            Iterable<RemoteParticipant> remoteParticipants, boolean publications) {
        readerId = publications ? RtpsEntity.PUBLICATIONS_BUILTIN_TOPIC_READER : RtpsEntity.SUBSCRIPTIONS_BUILTIN_TOPIC_READER;
        writerId = publications ? RtpsEntity.PUBLICATIONS_BUILTIN_TOPIC_WRITER : RtpsEntity.SUBSCRIPTIONS_BUILTIN_TOPIC_WRITER;
        endpointId = publications ? RtpsEntity.USER_WRITER_NO_KEY : RtpsEntity.USER_READER_NO_KEY;
        this.endpoint = endpoint;
        this.transport = transport;
        this.guidPrefix = guidPrefix;
        this.remoteParticipants = remoteParticipants;
    }

    public void announce() throws IOException {
        recordLocalEndpoint();
        sendToMetatrafficLocators(historyMessage());
    }

    public void respondTo(AckNack ackNack) throws IOException {
        recordLocalEndpoint();
        for (long sequenceNumber : ackNack.requestedSequenceNumbers()) {
            byte[] message = endpointHistory.get(sequenceNumber)
                    .map(this::sampleMessage)
                    .orElseGet(() -> gapMessage(ackNack.readerId(), sequenceNumber));
            sendToMetatrafficLocators(message);
        }
    }

    public void disposeAndUnregister() throws IOException {
        SedpEndpointSample sample = recordLocalEndpoint();
        RtpsMessageBuilder message = new RtpsMessageBuilder(guidPrefix);
        message.dataDispose(
                readerId,
                writerId,
                sample.sequenceNumber() + 1,
                sample.endpointGuid());
        sendToMetatrafficLocators(message.bytes());
    }

    private SedpEndpointSample recordLocalEndpoint() {
        return endpointHistory.addOrUpdate(guidPrefix.toGuid(endpointId), endpointPayload());
    }

    private byte[] endpointPayload() {
        return SedpAnnouncements.payload(guidPrefix.toGuid(endpointId), endpoint, transport);
    }

    private byte[] historyMessage() {
        RtpsMessageBuilder message = new RtpsMessageBuilder(guidPrefix);
        for (SedpEndpointSample sample : endpointHistory.samples()) {
            appendData(message, sample);
        }
        appendHeartbeat(message);
        return message.bytes();
    }

    private byte[] sampleMessage(SedpEndpointSample sample) {
        RtpsMessageBuilder message = new RtpsMessageBuilder(guidPrefix);
        appendData(message, sample);
        return message.bytes();
    }

    private void appendData(RtpsMessageBuilder message, SedpEndpointSample sample) {
        message.data(
                readerId,
                writerId,
                sample.sequenceNumber(),
                sample.payload());
    }

    private void appendHeartbeat(RtpsMessageBuilder message) {
        message.heartbeat(
                readerId,
                writerId,
                endpointHistory.firstSequenceNumber(),
                endpointHistory.lastSequenceNumber(),
                (int) heartbeatCount.getAndIncrement());
    }

    private byte[] gapMessage(EntityId readerId, long sequenceNumber) {
        RtpsMessageBuilder message = new RtpsMessageBuilder(guidPrefix);
        message.gap(readerId, writerId, sequenceNumber);
        return message.bytes();
    }

    private void sendToMetatrafficLocators(byte[] message) throws IOException {
        SedpAnnouncements.send(transport, remoteParticipants, message);
    }
}
