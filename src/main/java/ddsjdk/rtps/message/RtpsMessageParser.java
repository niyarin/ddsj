package ddsjdk.rtps.message;

import ddsjdk.rtps.protocol.RtpsSubmessageKind;
import ddsjdk.rtps.types.GuidPrefix;
import ddsjdk.rtps.types.RtpsTimestamp;
import ddsjdk.rtps.util.RtpsIo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public final class RtpsMessageParser {
    private static final int GUID_PREFIX_OFFSET = 8;
    private static final int GUID_PREFIX_SIZE = 12;
    private static final int RTPS_HEADER_SIZE = 20;
    private static final int SUBMESSAGE_HEADER_SIZE = 4;
    private static final int INFO_SOURCE_GUID_PREFIX_OFFSET = 4;
    private static final int INFO_SOURCE_SIZE = 16;
    private static final int INFO_TS_SIZE = 8;
    private static final int LITTLE_ENDIAN_FLAG = 0x01;
    private static final int INFO_TS_INVALID_FLAG = 0x02;

    private final byte[] packet;
    private final int length;

    public RtpsMessageParser(byte[] packet, int length) {
        this.packet = packet;
        this.length = length;
    }

    public List<RtpsSubmessage> submessages() {
        int end = Math.min(length, packet.length);
        if (end < RTPS_HEADER_SIZE || !hasRtpsHeader()) {
            return List.of();
        }

        GuidPrefix sourceGuidPrefix = new GuidPrefix(Arrays.copyOfRange(packet, GUID_PREFIX_OFFSET, RTPS_HEADER_SIZE));
        Optional<GuidPrefix> currentDestination = Optional.empty();
        Optional<RtpsTimestamp> currentTimestamp = Optional.empty();
        List<RtpsSubmessage> result = new ArrayList<>();
        int offset = RTPS_HEADER_SIZE;
        while (offset + SUBMESSAGE_HEADER_SIZE <= end) {
            int kind = packet[offset] & 0xff;
            int flags = packet[offset + 1] & 0xff;
            boolean littleEndian = (flags & LITTLE_ENDIAN_FLAG) != 0;
            int bodySize = RtpsIo.readUShort(packet, offset + 2, littleEndian);
            offset += SUBMESSAGE_HEADER_SIZE;
            if (offset + bodySize > end) {
                return result;
            }
            byte[] body = Arrays.copyOfRange(packet, offset, offset + bodySize);
            if (kind == RtpsSubmessageKind.INFO_SRC) {
                Optional<GuidPrefix> infoSourcePrefix = readInfoSourceGuidPrefix(body);
                if (infoSourcePrefix.isPresent()) {
                    sourceGuidPrefix = infoSourcePrefix.get();
                }
            } else if (kind == RtpsSubmessageKind.INFO_DST) {
                currentDestination = readInfoDestinationGuidPrefix(body);
            } else if (kind == RtpsSubmessageKind.INFO_TS) {
                currentTimestamp = readInfoTimestamp(body, flags, littleEndian);
            }
            result.add(new RtpsSubmessage(sourceGuidPrefix, currentDestination, kind, flags, littleEndian, body, currentTimestamp));
            offset += bodySize;
        }
        return result;
    }

    private boolean hasRtpsHeader() {
        return packet[0] == 'R' && packet[1] == 'T' && packet[2] == 'P' && packet[3] == 'S';
    }

    private Optional<GuidPrefix> readInfoSourceGuidPrefix(byte[] body) {
        if (body.length < INFO_SOURCE_SIZE) {
            return Optional.empty();
        }
        return Optional.of(new GuidPrefix(Arrays.copyOfRange(body, INFO_SOURCE_GUID_PREFIX_OFFSET, INFO_SOURCE_SIZE)));
    }

    private Optional<GuidPrefix> readInfoDestinationGuidPrefix(byte[] body) {
        if (body.length < GUID_PREFIX_SIZE) {
            return Optional.empty();
        }
        return Optional.of(new GuidPrefix(Arrays.copyOfRange(body, 0, GUID_PREFIX_SIZE)));
    }

    private Optional<RtpsTimestamp> readInfoTimestamp(byte[] body, int flags, boolean littleEndian) {
        if ((flags & INFO_TS_INVALID_FLAG) != 0) {
            return Optional.of(RtpsTimestamp.INVALID);
        }
        if (body.length < INFO_TS_SIZE) {
            return Optional.empty();
        }
        int seconds = RtpsIo.readInt(body, 0, littleEndian);
        int fraction = RtpsIo.readInt(body, 4, littleEndian);
        return Optional.of(new RtpsTimestamp(seconds, fraction));
    }
}
