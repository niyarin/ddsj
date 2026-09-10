package ddsjdk.rtps.transport;

import ddsjdk.rtps.types.Locator;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

public interface RtpsTransport extends Closeable {
    InetAddress multicastGroup();

    void sendMetatraffic(byte[] message) throws IOException;

    void sendUserData(byte[] message) throws IOException;

    void send(byte[] message, InetSocketAddress address) throws IOException;

    default void send(byte[] message, Locator locator) throws IOException {
        send(message, locator.socketAddress());
    }

    Closeable listenMetatraffic(PacketHandler onPacket) throws IOException;

    Closeable listenUserData(PacketHandler onPacket) throws IOException;

    Locator unicastLocator(int port);

    Locator multicastLocator(int port);

    @FunctionalInterface
    interface PacketHandler {
        void handle(RtpsPacket packet);
    }
}
