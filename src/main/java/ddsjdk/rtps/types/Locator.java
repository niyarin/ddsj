package ddsjdk.rtps.types;

import ddsjdk.rtps.util.RtpsIo;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Optional;

public record Locator(InetAddress address, int port) {
    private static final int LOCATOR_KIND_UDP_V4 = 1;

    public InetSocketAddress socketAddress() {
        return new InetSocketAddress(address, port);
    }

    public byte[] parameterValue() {
        byte[] value = new byte[24];
        RtpsIo.writeInt(value, 0, LOCATOR_KIND_UDP_V4, true);
        RtpsIo.writeInt(value, 4, port, true);
        byte[] addressBytes = address.getAddress();
        if (addressBytes.length != 4) {
            throw new IllegalArgumentException("only UDPv4 locators are supported");
        }
        System.arraycopy(addressBytes, 0, value, 20, 4);
        return value;
    }

    public static Optional<Locator> fromParameterValue(byte[] bytes, boolean littleEndian) {
        if (bytes.length != 24) {
            return Optional.empty();
        }
        int kind = RtpsIo.readInt(bytes, 0, littleEndian);
        if (kind != LOCATOR_KIND_UDP_V4) {
            return Optional.empty();
        }
        byte[] addressBytes = Arrays.copyOfRange(bytes, 20, 24);
        if (addressBytes[0] == 0 && addressBytes[1] == 0 && addressBytes[2] == 0 && addressBytes[3] == 0) {
            return Optional.empty();
        }
        try {
            return Optional.of(new Locator(InetAddress.getByAddress(addressBytes), RtpsIo.readInt(bytes, 4, littleEndian)));
        } catch (UnknownHostException e) {
            return Optional.empty();
        }
    }
}
