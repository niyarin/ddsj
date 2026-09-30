package ddsj.rtps.runtime;

import ddsj.rtps.util.Closeables;
import ddsj.rtps.transport.RtpsPacket;
import ddsj.rtps.transport.RtpsTransport;
import ddsj.rtps.types.Locator;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/** One physical subscription per traffic channel, shared by all participant endpoints. */
final class ParticipantTransport implements RtpsTransport {
    private static final Logger LOGGER = Logger.getLogger(ParticipantTransport.class.getName());

    private final RtpsTransport delegate;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final List<PacketHandler> metaHandlers = new CopyOnWriteArrayList<>();
    private final List<PacketHandler> userHandlers = new CopyOnWriteArrayList<>();
    private final Closeable metaReceiver;
    private final Closeable userReceiver;

    ParticipantTransport(RtpsTransport delegate) throws IOException {
        this.delegate = delegate;
        List<Closeable> opened = new ArrayList<>();
        opened.add(delegate);
        try {
            metaReceiver = delegate.listenMetatraffic(packet -> dispatch(metaHandlers, packet));
            opened.add(metaReceiver);
            userReceiver = delegate.listenUserData(packet -> dispatch(userHandlers, packet));
        } catch (IOException | RuntimeException e) {
            Closeables.rollback(e, opened);
            throw e;
        }
    }

    private static void dispatch(List<PacketHandler> handlers, RtpsPacket packet) {
        for (PacketHandler handler : handlers) {
            try {
                handler.handle(packet);
            } catch (RuntimeException e) {
                LOGGER.log(
                        Level.WARNING, "RTPS packet handler failed", e);
            }
        }
    }

    public InetAddress multicastGroup() { return delegate.multicastGroup(); }
    public void sendMetatraffic(byte[] message) throws IOException { delegate.sendMetatraffic(message); }
    public void sendUserData(byte[] message) throws IOException {
        delegate.sendUserData(message);
    }
    public void send(byte[] message, InetSocketAddress address) throws IOException { delegate.send(message, address); }
    public Locator userUnicastLocator() { return delegate.userUnicastLocator(); }
    public Locator unicastLocator(int port) { return delegate.unicastLocator(port); }
    public Locator multicastLocator(int port) { return delegate.multicastLocator(port); }
    public Closeable listenMetatraffic(PacketHandler handler) {
        metaHandlers.add(handler);
        return () -> metaHandlers.remove(handler);
    }
    public Closeable listenUserData(PacketHandler handler) {
        userHandlers.add(handler);
        return () -> userHandlers.remove(handler);
    }
    public void close() throws IOException {
        if (!closed.compareAndSet(false, true)) return;
        metaHandlers.clear();
        userHandlers.clear();
        Closeables.closeAll(List.of(metaReceiver, userReceiver, delegate));
    }
}
