package ddsjdk.rtps.discovery;

import ddsjdk.rtps.qos.EndpointQos;
import ddsjdk.rtps.message.RtpsMessageParser;
import ddsjdk.rtps.message.RtpsSubmessage;
import ddsjdk.rtps.parameter.RtpsParameterList;
import ddsjdk.rtps.protocol.ParameterId;
import ddsjdk.rtps.protocol.RtpsEntity;
import ddsjdk.rtps.protocol.RtpsSubmessageKind;
import ddsjdk.rtps.types.EntityId;
import ddsjdk.rtps.types.Guid;
import ddsjdk.rtps.types.GuidPrefix;
import ddsjdk.rtps.types.Locator;
import ddsjdk.rtps.util.RtpsIo;

import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public final class RtpsDiscoveryReader {
    private static final int INLINE_QOS_FLAG = 0x0002;

    private RtpsDiscoveryReader() {
    }

    public static Optional<RemoteParticipant> readRemoteParticipant(byte[] packet, int length) {
        return readDiscoveryData(packet, length, (writerId, payload, inlineQos, littleEndian) -> {
            if (!writerId.equals(RtpsEntity.PARTICIPANT_BUILTIN_TOPIC_WRITER)) {
                return Optional.empty();
            }
            return readRemoteParticipantPayload(payload, littleEndian);
        });
    }

    public static Optional<RemotePublication> readRemotePublication(byte[] packet, int length) {
        return readRemotePublicationChange(packet, length).flatMap(RemoteEndpointChange::endpoint);
    }

    public static Optional<RemoteSubscription> readRemoteSubscription(byte[] packet, int length) {
        return readRemoteSubscriptionChange(packet, length).flatMap(RemoteEndpointChange::endpoint);
    }

    public static Optional<RemoteEndpointChange<RemotePublication>> readRemotePublicationChange(byte[] packet, int length) {
        return readDiscoveryData(packet, length, (writerId, payload, inlineQos, littleEndian) -> {
            if (!writerId.equals(RtpsEntity.PUBLICATIONS_BUILTIN_TOPIC_WRITER)) {
                return Optional.empty();
            }
            return readRemoteEndpointChange(payload, inlineQos, littleEndian, RtpsDiscoveryReader::readRemotePublicationPayload);
        });
    }

    public static Optional<RemoteEndpointChange<RemoteSubscription>> readRemoteSubscriptionChange(byte[] packet, int length) {
        return readDiscoveryData(packet, length, (writerId, payload, inlineQos, littleEndian) -> {
            if (!writerId.equals(RtpsEntity.SUBSCRIPTIONS_BUILTIN_TOPIC_WRITER)) {
                return Optional.empty();
            }
            return readRemoteEndpointChange(payload, inlineQos, littleEndian, RtpsDiscoveryReader::readRemoteSubscriptionPayload);
        });
    }

    private static <T> Optional<T> readDiscoveryData(byte[] packet, int length, DiscoveryPayloadParser<T> parsePayload) {
        for (RtpsSubmessage submessage : new RtpsMessageParser(packet, length).submessages()) {
            if (submessage.kind() != RtpsSubmessageKind.DATA) {
                continue;
            }
            Optional<T> result = parseDiscoveryDataSubmessage(
                    submessage.body(),
                    (submessage.flags() & INLINE_QOS_FLAG) != 0,
                    submessage.littleEndian(),
                    parsePayload);
            if (result.isPresent()) {
                return result;
            }
        }
        return Optional.empty();
    }

    private static <T> Optional<T> parseDiscoveryDataSubmessage(
            byte[] body,
            boolean hasInlineQos,
            boolean littleEndian,
            DiscoveryPayloadParser<T> parsePayload) {
        if (body.length < 20) {
            return Optional.empty();
        }
        int octetsToInlineQos = RtpsIo.readUShort(body, 2, littleEndian);
        EntityId writerId = new EntityId(Arrays.copyOfRange(body, 8, 12));

        int payloadOffset = 4 + octetsToInlineQos;
        if (payloadOffset > body.length) {
            return Optional.empty();
        }
        InlineQos inlineQos = new InlineQos(Optional.empty(), Optional.empty());
        if (hasInlineQos) {
            Optional<InlineQosRead> inline = readInlineQos(body, payloadOffset, body.length, littleEndian);
            if (inline.isEmpty()) {
                return Optional.empty();
            }
            payloadOffset = inline.get().endOffset();
            inlineQos = inline.get().qos();
        }
        if (payloadOffset > body.length) {
            return Optional.empty();
        }
        return parsePayload.parse(writerId, Arrays.copyOfRange(body, payloadOffset, body.length), inlineQos, littleEndian);
    }

    private static Optional<RemoteParticipant> readRemoteParticipantPayload(byte[] payload, boolean littleEndian) {
        Optional<RtpsParameterList> parameterList = RtpsParameterList.read(payload, littleEndian);
        if (parameterList.isEmpty()) {
            return Optional.empty();
        }
        RtpsParameterList params = parameterList.get();
        Optional<byte[]> guid = params.first(ParameterId.PARTICIPANT_GUID);
        if (guid.isEmpty() || guid.get().length < GuidPrefix.SIZE) {
            return Optional.empty();
        }
        Set<Locator> metatraffic = params.get(ParameterId.METATRAFFIC_UNICAST_LOCATOR).stream()
                .flatMap(bytes -> Locator.fromParameterValue(bytes, littleEndian).stream())
                .collect(Collectors.toUnmodifiableSet());
        Set<Locator> user = params.get(ParameterId.DEFAULT_UNICAST_LOCATOR).stream()
                .flatMap(bytes -> Locator.fromParameterValue(bytes, littleEndian).stream())
                .collect(Collectors.toUnmodifiableSet());
        Duration leaseDuration = params.first(ParameterId.PARTICIPANT_LEASE_DURATION)
                .flatMap(bytes -> readDuration(bytes, littleEndian))
                .orElse(RemoteParticipant.DEFAULT_PARTICIPANT_LEASE_DURATION);
        return Optional.of(new RemoteParticipant(
                new GuidPrefix(Arrays.copyOfRange(guid.get(), 0, GuidPrefix.SIZE)),
                metatraffic,
                user,
                leaseDuration));
    }

    private static <T extends RemoteEndpoint> Optional<RemoteEndpointChange<T>> readRemoteEndpointChange(
            byte[] payload,
            InlineQos inlineQos,
            boolean littleEndian,
            BiFunction<byte[], Boolean, Optional<T>> readEndpoint) {
        Optional<T> endpoint = payload.length >= 4 ? readEndpoint.apply(payload, littleEndian) : Optional.empty();
        Optional<Guid> endpointGuid = endpoint
                .map(RemoteEndpoint::endpointGuid)
                .or(() -> inlineQos.keyHash()
                        .filter(keyHash -> keyHash.length == 16)
                        .map(RtpsDiscoveryReader::guidFromBytes));

        if (inlineQos.statusInfo().map(StatusInfo::isDisposedOrUnregistered).orElse(false) && endpointGuid.isPresent()) {
            return Optional.of(new RemoteEndpointChange<>(endpointGuid.get(), Optional.empty(), true));
        }
        if (endpoint.isPresent()) {
            return Optional.of(new RemoteEndpointChange<>(endpoint.get().endpointGuid(), endpoint, false));
        }
        return Optional.empty();
    }

    private static Optional<RemotePublication> readRemotePublicationPayload(byte[] payload, boolean littleEndian) {
        return readRemoteEndpointPayload(payload, littleEndian)
                .map(endpoint -> new RemotePublication(endpoint.endpointGuid(), endpoint.topicName(), endpoint.typeName(), endpoint.qos()));
    }

    private static Optional<RemoteSubscription> readRemoteSubscriptionPayload(byte[] payload, boolean littleEndian) {
        return readRemoteEndpointPayload(payload, littleEndian)
                .map(endpoint -> new RemoteSubscription(endpoint.endpointGuid(), endpoint.topicName(), endpoint.typeName(), endpoint.qos()));
    }

    private static Optional<RemoteEndpointPayload> readRemoteEndpointPayload(byte[] payload, boolean littleEndian) {
        Optional<RtpsParameterList> parameterList = RtpsParameterList.read(payload, littleEndian);
        if (parameterList.isEmpty()) {
            return Optional.empty();
        }
        RtpsParameterList params = parameterList.get();
        Optional<byte[]> endpointGuid = params.first(ParameterId.ENDPOINT_GUID);
        if (endpointGuid.isEmpty() || endpointGuid.get().length != 16) {
            return Optional.empty();
        }
        Optional<String> topicName = params.firstString(ParameterId.TOPIC_NAME, littleEndian);
        Optional<String> typeName = params.firstString(ParameterId.TYPE_NAME, littleEndian);
        if (topicName.isEmpty() || typeName.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new RemoteEndpointPayload(
                guidFromBytes(endpointGuid.get()),
                topicName.get(),
                typeName.get(),
                RtpsQosParameters.read(params, littleEndian)));
    }

    private static Optional<InlineQosRead> readInlineQos(byte[] body, int offset, int end, boolean littleEndian) {
        int position = offset;
        Optional<StatusInfo> statusInfo = Optional.empty();
        Optional<byte[]> keyHash = Optional.empty();
        while (position + 4 <= end) {
            int id = RtpsIo.readUShort(body, position, littleEndian);
            int size = RtpsIo.readUShort(body, position + 2, littleEndian);
            position += 4;
            if (id == ParameterId.SENTINEL) {
                return Optional.of(new InlineQosRead(new InlineQos(statusInfo, keyHash), position));
            }
            if (position + size > end) {
                return Optional.empty();
            }
            byte[] value = Arrays.copyOfRange(body, position, position + size);
            if (id == ParameterId.STATUS_INFO) {
                statusInfo = readStatusInfo(value, littleEndian);
            } else if (id == ParameterId.KEY_HASH) {
                keyHash = Optional.of(value);
            }
            position += size;
            while (position % 4 != 0) {
                position++;
            }
        }
        return Optional.empty();
    }

    private static Optional<StatusInfo> readStatusInfo(byte[] bytes, boolean littleEndian) {
        if (bytes.length < 4) {
            return Optional.empty();
        }
        return Optional.of(new StatusInfo(RtpsIo.readInt(bytes, 0, littleEndian)));
    }

    private static Optional<Duration> readDuration(byte[] bytes, boolean littleEndian) {
        if (bytes.length < 8) {
            return Optional.empty();
        }
        int seconds = RtpsIo.readInt(bytes, 0, littleEndian);
        int nanos = RtpsIo.readInt(bytes, 4, littleEndian);
        if (seconds == Integer.MAX_VALUE && nanos == -1) {
            return Optional.of(Duration.ZERO);
        }
        if (seconds < 0 || nanos < 0 || nanos >= 1_000_000_000) {
            return Optional.empty();
        }
        return Optional.of(Duration.ofSeconds(seconds, nanos));
    }

    private static Guid guidFromBytes(byte[] bytes) {
        return new Guid(
                new GuidPrefix(Arrays.copyOfRange(bytes, 0, 12)),
                new EntityId(Arrays.copyOfRange(bytes, 12, 16)));
    }

    @FunctionalInterface
    private interface DiscoveryPayloadParser<T> {
        Optional<T> parse(EntityId writerId, byte[] payload, InlineQos inlineQos, boolean littleEndian);
    }

    private record InlineQos(Optional<StatusInfo> statusInfo, Optional<byte[]> keyHash) {
        private InlineQos {
            statusInfo = statusInfo == null ? Optional.empty() : statusInfo;
            keyHash = keyHash == null ? Optional.empty() : keyHash.map(byte[]::clone);
        }

        @Override
        public Optional<byte[]> keyHash() {
            return keyHash.map(byte[]::clone);
        }
    }

    private record InlineQosRead(InlineQos qos, int endOffset) {
    }

    private record StatusInfo(int value) {
        private static final int DISPOSED = 0x00000001;
        private static final int UNREGISTERED = 0x00000002;

        boolean isDisposedOrUnregistered() {
            return (value & (DISPOSED | UNREGISTERED)) != 0;
        }
    }

    private record RemoteEndpointPayload(Guid endpointGuid, String topicName, String typeName, EndpointQos qos) {
    }
}
