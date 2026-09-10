package ddsjdk.rtps.transport;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.Optional;

public record RtpsParticipantConfig(
        int domainId,
        InetAddress multicastGroup,
        Optional<NetworkInterface> networkInterface,
        int participantIndex) {
    public static final String DEFAULT_MULTICAST_ADDRESS = "239.255.0.1";

    public RtpsParticipantConfig {
        networkInterface = networkInterface == null ? Optional.empty() : networkInterface;
    }

    public RtpsParticipantConfig(int domainId) {
        this(domainId, defaultMulticastGroup(), Optional.empty(), 0);
    }

    public RtpsParticipantConfig(int domainId, NetworkInterface networkInterface) {
        this(domainId, defaultMulticastGroup(), Optional.ofNullable(networkInterface), 0);
    }

    private static InetAddress defaultMulticastGroup() {
        try {
            return InetAddress.getByName(DEFAULT_MULTICAST_ADDRESS);
        } catch (UnknownHostException e) {
            throw new IllegalStateException("invalid default RTPS multicast address", e);
        }
    }
}
