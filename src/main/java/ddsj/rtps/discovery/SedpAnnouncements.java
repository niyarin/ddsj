package ddsj.rtps.discovery;

import ddsj.rtps.parameter.RtpsParameterLists;
import ddsj.rtps.protocol.ParameterId;
import ddsj.rtps.transport.RtpsTransport;
import ddsj.rtps.types.Guid;

import java.io.IOException;

/** Wire payload and delivery shared by participant and single-endpoint SEDP writers. */
final class SedpAnnouncements {
    private SedpAnnouncements() { }

    static byte[] payload(Guid guid, LocalEndpoint endpoint, RtpsTransport transport) {
        return RtpsParameterLists.payload(writer -> {
            writer.stringParameter(ParameterId.TOPIC_NAME, endpoint.topicName());
            writer.stringParameter(ParameterId.TYPE_NAME, endpoint.typeName());
            writer.parameter(ParameterId.PROTOCOL_VERSION, new byte[]{2, 5, 0, 0});
            writer.parameter(ParameterId.VENDOR_ID, new byte[]{1, 16, 0, 0});
            RtpsQosParameters.write(writer, endpoint.qos());
            writer.parameter(ParameterId.ENDPOINT_GUID, guid.bytes());
            writer.parameter(ParameterId.UNICAST_LOCATOR, transport.userUnicastLocator().parameterValue());
        });
    }

    static void send(RtpsTransport transport, Iterable<RemoteParticipant> participants, byte[] bytes) throws IOException {
        transport.sendMetatraffic(bytes);
        for (RemoteParticipant participant : participants) {
            for (var locator : participant.metatrafficUnicast()) transport.send(bytes, locator);
        }
    }
}
