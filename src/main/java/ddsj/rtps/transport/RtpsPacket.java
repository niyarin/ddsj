package ddsj.rtps.transport;

public record RtpsPacket(byte[] data, int length) {
    public RtpsPacket {
        data = data.clone();
        if (length < 0 || length > data.length) {
            throw new IllegalArgumentException("invalid RTPS packet length: " + length);
        }
    }

    @Override
    public byte[] data() {
        return data.clone();
    }
}
