package ddsjdk.rtps.protocol;

public final class RtpsSubmessageKind {
    public static final int INFO_SRC = 0x0c;
    public static final int ACKNACK = 0x06;
    public static final int HEARTBEAT = 0x07;
    public static final int GAP = 0x08;
    public static final int DATA = 0x15;

    private RtpsSubmessageKind() {
    }
}
