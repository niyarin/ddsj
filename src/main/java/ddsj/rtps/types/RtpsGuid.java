package ddsj.rtps.types;

import java.nio.ByteBuffer;
import java.util.UUID;

public final class RtpsGuid {
    private RtpsGuid() {
    }

    public static GuidPrefix newGuidPrefix() {
        UUID uuid = UUID.randomUUID();
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putLong(uuid.getMostSignificantBits());
        buffer.putLong(uuid.getLeastSignificantBits());
        byte[] bytes = new byte[GuidPrefix.SIZE];
        buffer.flip();
        buffer.get(bytes);
        return new GuidPrefix(bytes);
    }
}
