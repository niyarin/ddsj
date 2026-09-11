package ddsjdk.rtps.message;

import ddsjdk.rtps.parameter.RtpsParameterListWriter;
import ddsjdk.rtps.protocol.ParameterId;
import ddsjdk.rtps.protocol.RtpsSubmessageKind;
import ddsjdk.rtps.types.EntityId;
import ddsjdk.rtps.types.Guid;
import ddsjdk.rtps.types.GuidPrefix;
import ddsjdk.rtps.types.RtpsTimestamp;
import ddsjdk.rtps.util.RtpsIo;

import java.io.ByteArrayOutputStream;
import java.util.Set;

public final class RtpsMessageBuilder {
    private static final int STATUS_INFO_DISPOSED_UNREGISTERED = 0x00000003;

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();

    public RtpsMessageBuilder(GuidPrefix guidPrefix) {
        out.writeBytes(new byte[] {'R', 'T', 'P', 'S'});
        out.writeBytes(new byte[] {0x02, 0x05});
        out.writeBytes(new byte[] {0x01, 0x10});
        out.writeBytes(guidPrefix.bytes());
    }

    public void infoTs(RtpsTimestamp timestamp) {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.writeBytes(RtpsIo.intLe(timestamp.seconds()));
        body.writeBytes(RtpsIo.intLe(timestamp.fraction()));
        submessage(RtpsSubmessageKind.INFO_TS, 0x01, body.toByteArray());
    }

    public void infoTsInvalid() {
        submessage(RtpsSubmessageKind.INFO_TS, 0x03, new byte[0]);
    }

    public void infoDst(GuidPrefix destinationGuidPrefix) {
        submessage(RtpsSubmessageKind.INFO_DST, 0x01, destinationGuidPrefix.bytes());
    }

    public void data(EntityId readerId, EntityId writerId, long sequenceNumber, byte[] serializedPayload) {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.writeBytes(RtpsIo.shortLe(0));
        body.writeBytes(RtpsIo.shortLe(16));
        body.writeBytes(readerId.bytes());
        body.writeBytes(writerId.bytes());
        body.writeBytes(RtpsIo.intLe((int) (sequenceNumber >>> 32)));
        body.writeBytes(RtpsIo.intLe((int) sequenceNumber));
        body.writeBytes(serializedPayload);
        submessage(RtpsSubmessageKind.DATA, 0x05, body.toByteArray());
    }

    public void dataDispose(EntityId readerId, EntityId writerId, long sequenceNumber, Guid endpointGuid) {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.writeBytes(RtpsIo.shortLe(0));
        body.writeBytes(RtpsIo.shortLe(16));
        body.writeBytes(readerId.bytes());
        body.writeBytes(writerId.bytes());
        body.writeBytes(RtpsIo.intLe((int) (sequenceNumber >>> 32)));
        body.writeBytes(RtpsIo.intLe((int) sequenceNumber));
        parameterList(body, writer -> {
            writer.parameter(ParameterId.STATUS_INFO, RtpsIo.intLe(STATUS_INFO_DISPOSED_UNREGISTERED));
            writer.parameter(ParameterId.KEY_HASH, endpointGuid.bytes());
        });
        submessage(RtpsSubmessageKind.DATA, 0x03, body.toByteArray());
    }

    public void heartbeat(EntityId readerId, EntityId writerId, long firstSequenceNumber, long lastSequenceNumber, int count) {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.writeBytes(readerId.bytes());
        body.writeBytes(writerId.bytes());
        body.writeBytes(RtpsIo.intLe((int) (firstSequenceNumber >>> 32)));
        body.writeBytes(RtpsIo.intLe((int) firstSequenceNumber));
        body.writeBytes(RtpsIo.intLe((int) (lastSequenceNumber >>> 32)));
        body.writeBytes(RtpsIo.intLe((int) lastSequenceNumber));
        body.writeBytes(RtpsIo.intLe(count));
        submessage(RtpsSubmessageKind.HEARTBEAT, 0x07, body.toByteArray());
    }

    public void gap(EntityId readerId, EntityId writerId, long gapStart) {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.writeBytes(readerId.bytes());
        body.writeBytes(writerId.bytes());
        body.writeBytes(RtpsIo.intLe((int) (gapStart >>> 32)));
        body.writeBytes(RtpsIo.intLe((int) gapStart));
        body.writeBytes(RtpsIo.intLe((int) ((gapStart + 1) >>> 32)));
        body.writeBytes(RtpsIo.intLe((int) (gapStart + 1)));
        body.writeBytes(RtpsIo.intLe(0));
        submessage(RtpsSubmessageKind.GAP, 0x01, body.toByteArray());
    }

    public void ackNack(EntityId readerId, EntityId writerId, long baseSequenceNumber, Set<Long> missingSequenceNumbers, int count) {
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.writeBytes(readerId.bytes());
        body.writeBytes(writerId.bytes());
        body.writeBytes(RtpsIo.intLe((int) (baseSequenceNumber >>> 32)));
        body.writeBytes(RtpsIo.intLe((int) baseSequenceNumber));

        Long maxMissing = missingSequenceNumbers.stream().max(Long::compareTo).orElse(null);
        int numBits = maxMissing == null ? 0 : Math.clamp(maxMissing - baseSequenceNumber + 1, 0, 256);
        body.writeBytes(RtpsIo.intLe(numBits));
        int wordCount = (numBits + 31) / 32;
        for (int wordIndex = 0; wordIndex < wordCount; wordIndex++) {
            int word = 0;
            for (int bit = 0; bit < 32; bit++) {
                int bitIndex = wordIndex * 32 + bit;
                if (bitIndex < numBits && missingSequenceNumbers.contains(baseSequenceNumber + bitIndex)) {
                    word |= 1 << (31 - bit);
                }
            }
            body.writeBytes(RtpsIo.intLe(word));
        }
        body.writeBytes(RtpsIo.intLe(count));
        submessage(RtpsSubmessageKind.ACKNACK, 0x01, body.toByteArray());
    }

    public byte[] bytes() {
        return out.toByteArray();
    }

    private void submessage(int kind, int flags, byte[] body) {
        out.write(kind);
        out.write(flags);
        out.writeBytes(RtpsIo.shortLe(body.length));
        out.writeBytes(body);
    }

    private static void parameterList(ByteArrayOutputStream out, java.util.function.Consumer<RtpsParameterListWriter> writeParameters) {
        RtpsParameterListWriter writer = new RtpsParameterListWriter();
        writeParameters.accept(writer);
        writer.parameter(ParameterId.SENTINEL, new byte[0]);
        out.writeBytes(writer.bytes());
    }
}
