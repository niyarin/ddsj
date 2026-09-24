package ddsjdk.rtps.message;

import ddsjdk.rtps.transport.RtpsTransport;
import ddsjdk.rtps.types.Locator;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class ListenerResourceTest {
    @Test void failedSecondSubscriptionReleasesFirstButDoesNotCloseTransport() {
        for (boolean ack : new boolean[]{true, false}) {
            for (boolean unchecked : new boolean[]{true, false}) {
                var transport = new FakeTransport();
                transport.openFailure = unchecked ? new IllegalStateException("open") : new IOException("open");
                transport.metaCloseFailure = new IOException("close");
                var actual = assertThrows(Exception.class, () -> listener(ack, transport));
                assertSame(transport.openFailure, actual);
                assertArrayEquals(new Throwable[]{transport.metaCloseFailure}, actual.getSuppressed());
                assertEquals(1, transport.metaClosed);
                assertEquals(0, transport.userClosed);
                assertEquals(0, transport.transportClosed);
            }
        }
    }

    @Test void closeReleasesBothSubscriptionsAndAggregatesFailures() throws Exception {
        for (boolean ack : new boolean[]{true, false}) {
            var transport = new FakeTransport();
            var listener = listener(ack, transport);
            transport.metaCloseFailure = new IllegalStateException("meta");
            transport.userCloseFailure = new IOException("user");
            var actual = assertThrows(IllegalStateException.class, listener::close);
            assertSame(transport.metaCloseFailure, actual);
            assertArrayEquals(new Throwable[]{transport.userCloseFailure}, actual.getSuppressed());
            assertEquals(1, transport.metaClosed);
            assertEquals(1, transport.userClosed);
            assertEquals(0, transport.transportClosed);
        }
    }

    private static Closeable listener(boolean ack, RtpsTransport transport) throws IOException {
        return ack ? new AckNackListener(transport, Set.of(), ignored -> { })
                : new NackFragListener(transport, Set.of(), ignored -> { });
    }

    private static final class FakeTransport implements RtpsTransport {
        Exception openFailure, metaCloseFailure, userCloseFailure;
        int metaClosed, userClosed, transportClosed;
        public Closeable listenMetatraffic(PacketHandler handler) {
            return () -> { metaClosed++; fail(metaCloseFailure); };
        }
        public Closeable listenUserData(PacketHandler handler) throws IOException {
            fail(openFailure);
            return () -> { userClosed++; fail(userCloseFailure); };
        }
        private static void fail(Exception failure) throws IOException {
            if (failure instanceof IOException e) throw e;
            if (failure instanceof RuntimeException e) throw e;
        }
        public InetAddress multicastGroup() { return InetAddress.getLoopbackAddress(); }
        public void sendMetatraffic(byte[] message) { }
        public void sendUserData(byte[] message) { }
        public void send(byte[] message, InetSocketAddress address) { }
        public Locator userUnicastLocator() { return unicastLocator(7411); }
        public Locator unicastLocator(int port) { return new Locator(multicastGroup(), port); }
        public Locator multicastLocator(int port) { return unicastLocator(port); }
        public void close() { transportClosed++; }
    }
}
