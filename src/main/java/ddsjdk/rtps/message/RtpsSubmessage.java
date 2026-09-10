package ddsjdk.rtps.message;

import ddsjdk.rtps.types.GuidPrefix;

public record RtpsSubmessage(
        GuidPrefix sourceGuidPrefix,
        int kind,
        int flags,
        boolean littleEndian,
        byte[] body) {
    public RtpsSubmessage {
        body = body.clone();
    }

    @Override
    public byte[] body() {
        return body.clone();
    }
}
