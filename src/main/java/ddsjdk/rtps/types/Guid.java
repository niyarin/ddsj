package ddsjdk.rtps.types;

import java.util.Arrays;

public record Guid(GuidPrefix prefix, EntityId entityId) {
    public byte[] bytes() {
        byte[] prefixBytes = prefix.bytes();
        byte[] entityBytes = entityId.bytes();
        byte[] result = Arrays.copyOf(prefixBytes, prefixBytes.length + entityBytes.length);
        System.arraycopy(entityBytes, 0, result, prefixBytes.length, entityBytes.length);
        return result;
    }
}
