package ddsj.rtps.discovery;

import ddsj.rtps.message.RtpsMessageBuilder;
import ddsj.rtps.parameter.RtpsParameterLists;
import ddsj.rtps.protocol.ParameterId;
import ddsj.rtps.protocol.RtpsEntity;
import ddsj.rtps.protocol.RtpsPort;
import ddsj.rtps.transport.RtpsParticipantConfig;
import ddsj.rtps.transport.RtpsTransport;
import ddsj.rtps.types.EntityId;
import ddsj.rtps.types.GuidPrefix;
import ddsj.rtps.util.RtpsIo;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

public final class SpdpAnnouncer {
    private static final EntityId PARTICIPANT_ENTITY_ID = new EntityId(new byte[] {0x00, 0x00, 0x01, (byte) 0xc1});
    private static final byte[] PROTOCOL_VERSION = new byte[] {0x02, 0x05, 0x00, 0x00};
    private static final byte[] VENDOR_ID = new byte[] {0x01, 0x10, 0x00, 0x00};
    private static final int BUILTIN_ENDPOINT_SET = 0x00003ffc;

    private final RtpsParticipantConfig config;
    private final RtpsTransport transport;
    private final GuidPrefix guidPrefix;
    private final AtomicLong sequence = new AtomicLong(1);

    public SpdpAnnouncer(RtpsParticipantConfig config, RtpsTransport transport, GuidPrefix guidPrefix) {
        this.config = config;
        this.transport = transport;
        this.guidPrefix = guidPrefix;
    }

    public void announce() throws IOException {
        byte[] payload = RtpsParameterLists.payload(writer -> {
            writer.parameter(ParameterId.PROTOCOL_VERSION, PROTOCOL_VERSION);
            writer.parameter(ParameterId.VENDOR_ID, VENDOR_ID);
            writer.parameter(ParameterId.PARTICIPANT_GUID, guidPrefix.toGuid(PARTICIPANT_ENTITY_ID).bytes());
            writer.parameter(ParameterId.BUILTIN_ENDPOINT_SET, RtpsIo.intLe(BUILTIN_ENDPOINT_SET));
            writer.parameter(ParameterId.DOMAIN_ID, RtpsIo.intLe(config.domainId()));
            writer.parameter(ParameterId.PARTICIPANT_LEASE_DURATION, durationParameter(RemoteParticipant.DEFAULT_PARTICIPANT_LEASE_DURATION));
            writer.parameter(ParameterId.DEFAULT_UNICAST_LOCATOR, transport.unicastLocator(RtpsPort.userUnicast(config.domainId(), config.participantIndex())).parameterValue());
            writer.parameter(ParameterId.DEFAULT_MULTICAST_LOCATOR, transport.multicastLocator(RtpsPort.userMulticast(config.domainId())).parameterValue());
            writer.parameter(ParameterId.METATRAFFIC_UNICAST_LOCATOR, transport.unicastLocator(RtpsPort.metatrafficUnicast(config.domainId(), config.participantIndex())).parameterValue());
            writer.parameter(ParameterId.METATRAFFIC_MULTICAST_LOCATOR, transport.multicastLocator(RtpsPort.metatrafficMulticast(config.domainId())).parameterValue());
        });
        RtpsMessageBuilder message = new RtpsMessageBuilder(guidPrefix);
        message.data(
                RtpsEntity.PARTICIPANT_BUILTIN_TOPIC_READER,
                RtpsEntity.PARTICIPANT_BUILTIN_TOPIC_WRITER,
                sequence.getAndIncrement(),
                payload);
        transport.sendMetatraffic(message.bytes());
    }

    private static byte[] durationParameter(Duration duration) {
        return concat(RtpsIo.intLe((int) duration.getSeconds()), RtpsIo.intLe(duration.getNano()));
    }

    private static byte[] concat(byte[] first, byte[] second) {
        byte[] result = java.util.Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
    }
}
