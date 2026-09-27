package ddsj.rtps.transport;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

class UdpRtpsTransportTest {
    @ParameterizedTest
    @CsvSource({"0, 0, 7411", "3, 7, 8175"})
    void userUnicastLocatorUsesConfiguredDomainAndParticipant(int domain, int participant, int port) throws Exception {
        var config = new RtpsParticipantConfig(domain, RtpsParticipantConfig.defaultMulticastGroup(),
                Optional.empty(), participant);
        try (var transport = new UdpRtpsTransport(config)) {
            assertEquals(transport.unicastLocator(port), transport.userUnicastLocator());
        }
    }
}
