package ddsjdk.rtps.message;

import ddsjdk.rtps.protocol.RtpsSubmessageKind;
import ddsjdk.rtps.types.EntityId;
import ddsjdk.rtps.util.RtpsIo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public final class AckNackParser {
    private AckNackParser() {
    }

    public static List<AckNack> readAckNacks(byte[] packet, int length) {
        List<AckNack> result = new ArrayList<>();
        for (RtpsSubmessage submessage : new RtpsMessageParser(packet, length).submessages()) {
            if (submessage.kind() == RtpsSubmessageKind.ACKNACK) {
                parseAckNack(submessage.body(), submessage.littleEndian()).ifPresent(result::add);
            }
        }
        return result;
    }

    private static Optional<AckNack> parseAckNack(byte[] body, boolean littleEndian) {
        if (body.length < 24) {
            return Optional.empty();
        }
        EntityId readerId = new EntityId(Arrays.copyOfRange(body, 0, 4));
        EntityId writerId = new EntityId(Arrays.copyOfRange(body, 4, 8));
        long bitmapBase = RtpsIo.readSequenceNumber(body, 8, littleEndian);
        int numBits = RtpsIo.readInt(body, 16, littleEndian);
        if (numBits < 0 || numBits > 256) {
            return Optional.empty();
        }

        int wordCount = (numBits + 31) / 32;
        int bitmapOffset = 20;
        int countOffset = bitmapOffset + wordCount * 4;
        if (countOffset + 4 > body.length) {
            return Optional.empty();
        }

        Set<Long> requested = new LinkedHashSet<>();
        for (int bitIndex = 0; bitIndex < numBits; bitIndex++) {
            int word = RtpsIo.readInt(body, bitmapOffset + (bitIndex / 32) * 4, littleEndian);
            int mask = 1 << (31 - bitIndex % 32);
            if ((word & mask) != 0) {
                requested.add(bitmapBase + bitIndex);
            }
        }

        return Optional.of(new AckNack(readerId, writerId, requested, RtpsIo.readInt(body, countOffset, littleEndian)));
    }
}
