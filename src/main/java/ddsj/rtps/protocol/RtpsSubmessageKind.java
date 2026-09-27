package ddsj.rtps.protocol;

public final class RtpsSubmessageKind {
    public static final int INFO_SRC = 0x0c;
    public static final int INFO_DST = 0x0e;
    public static final int INFO_TS = 0x09;
    public static final int ACKNACK = 0x06;
    public static final int HEARTBEAT = 0x07;
    public static final int GAP = 0x08;
    public static final int DATA = 0x15;
    public static final int DATA_FRAG = 0x16;
    public static final int HEARTBEAT_FRAG = 0x13;
    public static final int NACK_FRAG = 0x12;

    private RtpsSubmessageKind() {
    }
}
