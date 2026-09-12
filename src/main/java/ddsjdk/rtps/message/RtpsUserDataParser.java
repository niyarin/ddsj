package ddsjdk.rtps.message;

import ddsjdk.rtps.protocol.ParameterId;
import ddsjdk.rtps.protocol.RtpsSubmessageKind;
import ddsjdk.rtps.types.EntityId;
import ddsjdk.rtps.types.Guid;
import ddsjdk.rtps.types.GuidPrefix;
import ddsjdk.rtps.types.RtpsTimestamp;
import ddsjdk.rtps.util.RtpsIo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public final class RtpsUserDataParser {
    private RtpsUserDataParser() {
    }

    public static Optional<byte[]> readUserPayload(byte[] packet, int length, EntityId expectedReaderId) {
        return readUserSamples(packet, length, expectedReaderId).stream().findFirst().map(UserDataSample::payload);
    }

    public static List<UserDataSample> readUserSamples(byte[] packet, int length, EntityId expectedReaderId) {
        List<UserDataSample> result = new ArrayList<>();
        for (RtpsSubmessage submessage : new RtpsMessageParser(packet, length).submessages()) {
            if (submessage.kind() == RtpsSubmessageKind.DATA) {
                parseUserDataSubmessage(
                        submessage.sourceGuidPrefix(),
                        submessage.body(),
                        expectedReaderId,
                        submessage.littleEndian(),
                        submessage.timestamp()).ifPresent(result::add);
            }
        }
        return result;
    }

    public static List<Heartbeat> readHeartbeats(byte[] packet, int length, EntityId expectedReaderId) {
        List<Heartbeat> result = new ArrayList<>();
        for (RtpsSubmessage submessage : new RtpsMessageParser(packet, length).submessages()) {
            if (submessage.kind() == RtpsSubmessageKind.HEARTBEAT) {
                parseHeartbeat(
                        submessage.sourceGuidPrefix(),
                        submessage.body(),
                        expectedReaderId,
                        submessage.littleEndian()).ifPresent(result::add);
            }
        }
        return result;
    }

    public static List<DataFragment> readDataFragments(byte[] packet, int length, EntityId expectedReaderId) {
        List<DataFragment> result = new ArrayList<>();
        for (RtpsSubmessage submessage : new RtpsMessageParser(packet, length).submessages()) {
            if (submessage.kind() == RtpsSubmessageKind.DATA_FRAG) {
                parseDataFragmentSubmessage(
                        submessage.sourceGuidPrefix(),
                        submessage.body(),
                        expectedReaderId,
                        submessage.littleEndian(),
                        submessage.timestamp()).ifPresent(result::add);
            }
        }
        return result;
    }

    private static Optional<UserDataSample> parseUserDataSubmessage(
            GuidPrefix sourceGuidPrefix,
            byte[] body,
            EntityId expectedReaderId,
            boolean littleEndian,
            Optional<RtpsTimestamp> timestamp) {
        if (body.length < 20) {
            return Optional.empty();
        }
        int extraFlags = RtpsIo.readUShort(body, 0, littleEndian);
        int octetsToInlineQos = RtpsIo.readUShort(body, 2, littleEndian);
        EntityId readerId = new EntityId(Arrays.copyOfRange(body, 4, 8));
        if (!readerId.equals(expectedReaderId) && !isUnknownEntity(readerId)) {
            return Optional.empty();
        }
        EntityId writerId = new EntityId(Arrays.copyOfRange(body, 8, 12));
        long sequenceNumber = RtpsIo.readSequenceNumber(body, 12, littleEndian);

        int payloadOffset = 4 + octetsToInlineQos;
        if (payloadOffset > body.length) {
            return Optional.empty();
        }
        if ((extraFlags & 0x0002) != 0) {
            Optional<Integer> inlineEnd = skipInlineQos(body, payloadOffset, body.length, littleEndian);
            if (inlineEnd.isEmpty()) {
                return Optional.empty();
            }
            payloadOffset = inlineEnd.get();
        }
        if (payloadOffset >= body.length) {
            return Optional.empty();
        }
        return Optional.of(new UserDataSample(
                new Guid(sourceGuidPrefix, writerId),
                sequenceNumber,
                Arrays.copyOfRange(body, payloadOffset, body.length),
                timestamp));
    }

    private static Optional<DataFragment> parseDataFragmentSubmessage(
            GuidPrefix sourceGuidPrefix,
            byte[] body,
            EntityId expectedReaderId,
            boolean littleEndian,
            Optional<RtpsTimestamp> timestamp) {
        // Minimum size: extraFlags(2) + octetsToInlineQos(2) + readerId(4) + writerId(4)
        //              + sequenceNumber(8) + fragmentStartingNum(4) + fragmentsInSubmessage(2)
        //              + fragmentSize(2) + sampleSize(4) = 32 bytes
        if (body.length < 32) {
            return Optional.empty();
        }
        int octetsToInlineQos = RtpsIo.readUShort(body, 2, littleEndian);
        EntityId readerId = new EntityId(Arrays.copyOfRange(body, 4, 8));
        if (!readerId.equals(expectedReaderId) && !isUnknownEntity(readerId)) {
            return Optional.empty();
        }
        EntityId writerId = new EntityId(Arrays.copyOfRange(body, 8, 12));
        long sequenceNumber = RtpsIo.readSequenceNumber(body, 12, littleEndian);
        int fragmentStartingNum = RtpsIo.readInt(body, 20, littleEndian);
        int fragmentsInSubmessage = RtpsIo.readUShort(body, 24, littleEndian);
        int fragmentSize = RtpsIo.readUShort(body, 26, littleEndian);
        int sampleSize = RtpsIo.readInt(body, 28, littleEndian);

        int fragmentDataOffset = 4 + octetsToInlineQos;
        if (fragmentDataOffset > body.length) {
            return Optional.empty();
        }
        byte[] fragmentData = Arrays.copyOfRange(body, fragmentDataOffset, body.length);

        return Optional.of(new DataFragment(
                new Guid(sourceGuidPrefix, writerId),
                sequenceNumber,
                fragmentStartingNum,
                fragmentsInSubmessage,
                fragmentSize,
                sampleSize,
                fragmentData,
                timestamp));
    }

    private static Optional<Heartbeat> parseHeartbeat(
            GuidPrefix sourceGuidPrefix,
            byte[] body,
            EntityId expectedReaderId,
            boolean littleEndian) {
        if (body.length < 28) {
            return Optional.empty();
        }
        EntityId readerId = new EntityId(Arrays.copyOfRange(body, 0, 4));
        if (!readerId.equals(expectedReaderId) && !isUnknownEntity(readerId)) {
            return Optional.empty();
        }
        return Optional.of(new Heartbeat(
                readerId,
                new Guid(sourceGuidPrefix, new EntityId(Arrays.copyOfRange(body, 4, 8))),
                RtpsIo.readSequenceNumber(body, 8, littleEndian),
                RtpsIo.readSequenceNumber(body, 16, littleEndian),
                RtpsIo.readInt(body, 24, littleEndian)));
    }

    private static Optional<Integer> skipInlineQos(byte[] packet, int offset, int end, boolean littleEndian) {
        int position = offset;
        while (position + 4 <= end) {
            int id = RtpsIo.readUShort(packet, position, littleEndian);
            int size = RtpsIo.readUShort(packet, position + 2, littleEndian);
            position += 4;
            if (id == ParameterId.SENTINEL) {
                return Optional.of(position);
            }
            position += size;
            while (position % 4 != 0) {
                position++;
            }
        }
        return Optional.empty();
    }

    private static boolean isUnknownEntity(EntityId entityId) {
        byte[] bytes = entityId.bytes();
        return bytes[0] == 0 && bytes[1] == 0 && bytes[2] == 0 && bytes[3] == 0;
    }
}
