package ddsj.rtps.transport;

import ddsj.rtps.protocol.RtpsPort;
import ddsj.rtps.types.Locator;

import java.io.Closeable;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

public final class UdpRtpsTransport implements RtpsTransport {
    private final RtpsParticipantConfig config;
    private final MulticastSocket socket;
    private final InetAddress localAddress;

    public UdpRtpsTransport(RtpsParticipantConfig config) throws IOException {
        this.config = config;
        this.socket = new MulticastSocket();
        this.socket.setTimeToLive(1);
        if (config.networkInterface().isPresent()) {
            socket.setNetworkInterface(config.networkInterface().get());
        }
        Optional<InetAddress> selectedAddress = config.networkInterface()
                .flatMap(UdpRtpsTransport::firstIpv4Address);
        this.localAddress = selectedAddress.isPresent() ? selectedAddress.get() : InetAddress.getLocalHost();
    }

    @Override
    public InetAddress multicastGroup() {
        return config.multicastGroup();
    }

    @Override
    public void sendMetatraffic(byte[] message) throws IOException {
        send(message, RtpsPort.metatrafficMulticast(config.domainId()));
    }

    @Override
    public void sendUserData(byte[] message) throws IOException {
        send(message, RtpsPort.userMulticast(config.domainId()));
    }

    private void send(byte[] message, int port) throws IOException {
        send(message, new InetSocketAddress(multicastGroup(), port));
    }

    @Override
    public void send(byte[] message, InetSocketAddress address) throws IOException {
        socket.send(new DatagramPacket(message, message.length, address));
    }

    @Override
    public Closeable listenMetatraffic(PacketHandler onPacket) throws IOException {
        return new UdpRtpsReceiver(
                config,
                multicastGroup(),
                RtpsPort.metatrafficMulticast(config.domainId()),
                RtpsPort.metatrafficUnicast(config.domainId(), config.participantIndex()),
                onPacket);
    }

    @Override
    public Closeable listenUserData(PacketHandler onPacket) throws IOException {
        return new UdpRtpsReceiver(
                config,
                multicastGroup(),
                RtpsPort.userMulticast(config.domainId()),
                RtpsPort.userUnicast(config.domainId(), config.participantIndex()),
                onPacket);
    }

    @Override
    public Locator userUnicastLocator() {
        return unicastLocator(RtpsPort.userUnicast(config.domainId(), config.participantIndex()));
    }

    @Override
    public Locator unicastLocator(int port) {
        return new Locator(localAddress, port);
    }

    @Override
    public Locator multicastLocator(int port) {
        return new Locator(multicastGroup(), port);
    }

    @Override
    public void close() {
        socket.close();
    }

    private static Optional<InetAddress> firstIpv4Address(NetworkInterface networkInterface) {
        Enumeration<InetAddress> addresses = networkInterface.getInetAddresses();
        while (addresses.hasMoreElements()) {
            InetAddress address = addresses.nextElement();
            if (address instanceof Inet4Address) {
                return Optional.of(address);
            }
        }
        return Optional.empty();
    }

    private static final class UdpRtpsReceiver implements Closeable {
        private static final int MAX_PACKET_SIZE = 64 * 1024;

        private final RtpsParticipantConfig config;
        private final InetAddress multicastGroup;
        private final int multicastPort;
        private final int unicastPort;
        private final PacketHandler onPacket;
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final java.util.List<MulticastSocket> sockets = new java.util.ArrayList<>();
        private final java.util.List<Thread> workers;

        private UdpRtpsReceiver(
                RtpsParticipantConfig config,
                InetAddress multicastGroup,
                int multicastPort,
                int unicastPort,
                PacketHandler onPacket) throws IOException {
            this.config = config;
            this.multicastGroup = multicastGroup;
            this.multicastPort = multicastPort;
            this.unicastPort = unicastPort;
            this.onPacket = onPacket;
            this.workers = java.util.List.of(
                    startWorker("ddsj-rtps-receiver-" + multicastPort, openSocket(multicastPort, true)),
                    startWorker("ddsj-rtps-receiver-" + unicastPort, openSocket(unicastPort, false)));
        }

        private Thread startWorker(String name, MulticastSocket socket) {
            Thread worker = new Thread(() -> receiveLoop(socket), name);
            worker.setDaemon(true);
            worker.start();
            return worker;
        }

        private MulticastSocket openSocket(int port, boolean joinMulticast) throws IOException {
            MulticastSocket receiver = new MulticastSocket(port);
            receiver.setReuseAddress(true);
            if (config.networkInterface().isPresent()) {
                NetworkInterface selectedInterface = config.networkInterface().get();
                receiver.setNetworkInterface(selectedInterface);
                if (joinMulticast) {
                    receiver.joinGroup(new InetSocketAddress(multicastGroup, port), selectedInterface);
                }
            } else if (joinMulticast) {
                receiver.joinGroup(multicastGroup);
            }
            synchronized (sockets) {
                sockets.add(receiver);
            }
            return receiver;
        }

        private void receiveLoop(MulticastSocket receiver) {
            byte[] buffer = new byte[MAX_PACKET_SIZE];
            try {
                while (running.get()) {
                    try {
                        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                        receiver.receive(packet);
                        onPacket.handle(new RtpsPacket(java.util.Arrays.copyOf(packet.getData(), packet.getLength()), packet.getLength()));
                    } catch (Exception ignored) {
                        if (running.get()) {
                            try {
                                Thread.sleep(10);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                return;
                            }
                        }
                    }
                }
            } finally {
                receiver.close();
            }
        }

        @Override
        public void close() {
            if (!running.compareAndSet(true, false)) {
                return;
            }
            synchronized (sockets) {
                sockets.forEach(MulticastSocket::close);
                sockets.clear();
            }
            workers.forEach(Thread::interrupt);
        }
    }
}
