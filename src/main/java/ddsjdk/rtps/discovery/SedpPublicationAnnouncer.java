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
import java.util.concurrent.atomic.AtomicLong;

public final class SedpPublicationAnnouncer {
    private final LocalEndpoint endpoint;
    private final RtpsTransport transport;
    private final GuidPrefix guidPrefix;
    private final Iterable<RemoteParticipant> remoteParticipants;
    private final SedpEndpointSampleHistory endpointHistory = new SedpEndpointSampleHistory();
    private final AtomicLong heartbeatCount = new AtomicLong(1);

    public SedpPublicationAnnouncer(
            LocalEndpoint endpoint,
            RtpsTransport transport,
            GuidPrefix guidPrefix,
            Iterable<RemoteParticipant> remoteParticipants) {
        this.endpoint = endpoint;
        this.transport = transport;
        this.guidPrefix = guidPrefix;
        this.remoteParticipants = remoteParticipants;
    }

    public void announce() throws IOException {
        recordLocalPublication();
        sendToMetatrafficLocators(publicationHistoryMessage());
    }

    public void respondTo(AckNack ackNack) throws IOException {
        recordLocalPublication();
        for (long sequenceNumber : ackNack.requestedSequenceNumbers()) {
            byte[] message = endpointHistory.get(sequenceNumber)
                    .map(this::publicationSampleMessage)
                    .orElseGet(() -> gapMessage(ackNack.readerId(), sequenceNumber));
            sendToMetatrafficLocators(message);
        }
    }

    public void disposeAndUnregister() throws IOException {
        SedpEndpointSample sample = recordLocalPublication();
        RtpsMessageBuilder message = new RtpsMessageBuilder(guidPrefix);
        message.dataDispose(
                RtpsEntity.PUBLICATIONS_BUILTIN_TOPIC_READER,
                RtpsEntity.PUBLICATIONS_BUILTIN_TOPIC_WRITER,
                sample.sequenceNumber() + 1,
                sample.endpointGuid());
        sendToMetatrafficLocators(message.bytes());
    }

    private SedpEndpointSample recordLocalPublication() {
        return endpointHistory.addOrUpdate(guidPrefix.toGuid(RtpsEntity.USER_WRITER_NO_KEY), publicationPayload());
    }

    private byte[] publicationPayload() {
        Guid writerGuid = guidPrefix.toGuid(RtpsEntity.USER_WRITER_NO_KEY);
        return RtpsParameterLists.payload(writer -> {
            writer.stringParameter(ParameterId.TOPIC_NAME, endpoint.topicName());
            writer.stringParameter(ParameterId.TYPE_NAME, endpoint.typeName());
            writer.parameter(ParameterId.PROTOCOL_VERSION, new byte[] {0x02, 0x05, 0x00, 0x00});
            writer.parameter(ParameterId.VENDOR_ID, new byte[] {0x01, 0x10, 0x00, 0x00});
            RtpsQosParameters.write(writer, endpoint.qos());
            writer.parameter(ParameterId.ENDPOINT_GUID, writerGuid.bytes());
        });
    }

    private byte[] publicationHistoryMessage() {
        RtpsMessageBuilder message = new RtpsMessageBuilder(guidPrefix);
        for (SedpEndpointSample sample : endpointHistory.samples()) {
            publicationData(message, sample);
        }
        publicationHeartbeat(message);
        return message.bytes();
    }

    private byte[] publicationSampleMessage(SedpEndpointSample sample) {
        RtpsMessageBuilder message = new RtpsMessageBuilder(guidPrefix);
        publicationData(message, sample);
        return message.bytes();
    }

    private void publicationData(RtpsMessageBuilder message, SedpEndpointSample sample) {
        message.data(
                RtpsEntity.PUBLICATIONS_BUILTIN_TOPIC_READER,
                RtpsEntity.PUBLICATIONS_BUILTIN_TOPIC_WRITER,
                sample.sequenceNumber(),
                sample.payload());
    }

    private void publicationHeartbeat(RtpsMessageBuilder message) {
        message.heartbeat(
                RtpsEntity.PUBLICATIONS_BUILTIN_TOPIC_READER,
                RtpsEntity.PUBLICATIONS_BUILTIN_TOPIC_WRITER,
                endpointHistory.firstSequenceNumber(),
                endpointHistory.lastSequenceNumber(),
                (int) heartbeatCount.getAndIncrement());
    }

    private byte[] gapMessage(EntityId readerId, long sequenceNumber) {
        RtpsMessageBuilder message = new RtpsMessageBuilder(guidPrefix);
        message.gap(readerId, RtpsEntity.PUBLICATIONS_BUILTIN_TOPIC_WRITER, sequenceNumber);
        return message.bytes();
    }

    private void sendToMetatrafficLocators(byte[] message) throws IOException {
        transport.sendMetatraffic(message);
        for (RemoteParticipant participant : remoteParticipants) {
            for (ddsjdk.rtps.types.Locator locator : participant.metatrafficUnicast()) {
                transport.send(message, locator);
            }
        }
    }
}
