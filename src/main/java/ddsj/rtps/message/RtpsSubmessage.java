package ddsj.rtps.message;

import ddsj.rtps.types.GuidPrefix;
import ddsj.rtps.types.RtpsTimestamp;

import java.util.Optional;

public record RtpsSubmessage(
        GuidPrefix sourceGuidPrefix,
        Optional<GuidPrefix> destinationGuidPrefix,
        int kind,
        int flags,
        boolean littleEndian,
        byte[] body,
        Optional<RtpsTimestamp> timestamp) {

    public RtpsSubmessage(GuidPrefix sourceGuidPrefix, int kind, int flags, boolean littleEndian, byte[] body) {
        this(sourceGuidPrefix, Optional.empty(), kind, flags, littleEndian, body.clone(), Optional.empty());
    }

    public RtpsSubmessage(GuidPrefix sourceGuidPrefix, int kind, int flags, boolean littleEndian, byte[] body, Optional<RtpsTimestamp> timestamp) {
        this(sourceGuidPrefix, Optional.empty(), kind, flags, littleEndian, body.clone(), timestamp);
    }

    public RtpsSubmessage(GuidPrefix sourceGuidPrefix, Optional<GuidPrefix> destinationGuidPrefix, int kind, int flags, boolean littleEndian, byte[] body, Optional<RtpsTimestamp> timestamp) {
        this.sourceGuidPrefix = sourceGuidPrefix;
        this.destinationGuidPrefix = destinationGuidPrefix;
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
