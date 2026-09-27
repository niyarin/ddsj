package ddsj.rtps.protocol;

public final class ParameterId {
    public static final int SENTINEL = 0x0001;
    public static final int PARTICIPANT_LEASE_DURATION = 0x0002;
    public static final int TOPIC_NAME = 0x0005;
    public static final int TYPE_NAME = 0x0007;
    public static final int DOMAIN_ID = 0x000f;
    public static final int PROTOCOL_VERSION = 0x0015;
    public static final int VENDOR_ID = 0x0016;
    public static final int RELIABILITY = 0x001a;
    public static final int DURABILITY = 0x001d;
    public static final int HISTORY = 0x0040;
    public static final int UNICAST_LOCATOR = 0x002f;
    public static final int DEFAULT_UNICAST_LOCATOR = 0x0031;
    public static final int METATRAFFIC_UNICAST_LOCATOR = 0x0032;
    public static final int METATRAFFIC_MULTICAST_LOCATOR = 0x0033;
    public static final int DEFAULT_MULTICAST_LOCATOR = 0x0048;
    public static final int PARTICIPANT_GUID = 0x0050;
    public static final int BUILTIN_ENDPOINT_SET = 0x0058;
    public static final int ENDPOINT_GUID = 0x005a;
    public static final int KEY_HASH = 0x0070;
    public static final int STATUS_INFO = 0x0071;
    public static final int DEADLINE = 0x0023;
    public static final int OWNERSHIP = 0x001f;
    public static final int OWNERSHIP_STRENGTH = 0x0006;

    private ParameterId() {
    }
}
