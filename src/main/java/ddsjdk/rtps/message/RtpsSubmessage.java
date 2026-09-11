package ddsjdk.rtps.message;

import ddsjdk.rtps.types.GuidPrefix;
import ddsjdk.rtps.types.RtpsTimestamp;

import java.util.Optional;

public record RtpsSubmessage(
        GuidPrefix sourceGuidPrefix,
        int kind,
        int flags,
        boolean littleEndian,
        byte[] body,
        Optional<RtpsTimestamp> timestamp) {

    public RtpsSubmessage(GuidPrefix sourceGuidPrefix, int kind, int flags, boolean littleEndian, byte[] body) {
        this(sourceGuidPrefix, kind, flags, littleEndian, body.clone(), Optional.empty());
    }

    public RtpsSubmessage(GuidPrefix sourceGuidPrefix, int kind, int flags, boolean littleEndian, byte[] body, Optional<RtpsTimestamp> timestamp) {
        this.sourceGuidPrefix = sourceGuidPrefix;
        this.kind = kind;
        this.flags = flags;
        this.littleEndian = littleEndian;
        this.body = body.clone();
        this.timestamp = timestamp;
    }

    @Override
    public byte[] body() {
        return body.clone();
    }
}
