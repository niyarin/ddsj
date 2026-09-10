package ddsjdk.rtps.protocol;

import ddsjdk.rtps.types.EntityId;

public final class RtpsEntity {
    public static final EntityId PARTICIPANT_BUILTIN_TOPIC_READER = entity(0x00, 0x01, 0x00, 0xc7);
    public static final EntityId PARTICIPANT_BUILTIN_TOPIC_WRITER = entity(0x00, 0x01, 0x00, 0xc2);
    public static final EntityId PUBLICATIONS_BUILTIN_TOPIC_READER = entity(0x00, 0x00, 0x03, 0xc7);
    public static final EntityId PUBLICATIONS_BUILTIN_TOPIC_WRITER = entity(0x00, 0x00, 0x03, 0xc2);
    public static final EntityId SUBSCRIPTIONS_BUILTIN_TOPIC_READER = entity(0x00, 0x00, 0x04, 0xc7);
    public static final EntityId SUBSCRIPTIONS_BUILTIN_TOPIC_WRITER = entity(0x00, 0x00, 0x04, 0xc2);
    public static final EntityId USER_READER_NO_KEY = entity(0x00, 0x00, 0x02, 0x04);
    public static final EntityId USER_WRITER_NO_KEY = entity(0x00, 0x00, 0x02, 0x03);

    private RtpsEntity() {
    }

    private static EntityId entity(int b0, int b1, int b2, int b3) {
        return new EntityId(new byte[] {(byte) b0, (byte) b1, (byte) b2, (byte) b3});
    }
}
