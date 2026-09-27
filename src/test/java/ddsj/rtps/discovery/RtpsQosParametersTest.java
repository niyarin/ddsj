package ddsj.rtps.discovery;

import ddsj.rtps.parameter.RtpsParameterList;
import ddsj.rtps.parameter.RtpsParameterLists;
import ddsj.rtps.protocol.ParameterId;
import ddsj.rtps.qos.EndpointQos;
import ddsj.rtps.util.RtpsIo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class RtpsQosParametersTest {
    static Stream<Duration> infiniteDeadlines() {
        return Stream.of(EndpointQos.DEADLINE_INFINITE,
                Duration.ofSeconds(Integer.MAX_VALUE),
                Duration.ofSeconds((long) Integer.MAX_VALUE + 1),
                Duration.ofSeconds(Long.MAX_VALUE));
    }

    @ParameterizedTest
    @MethodSource("infiniteDeadlines")
    void infiniteDeadlineIsOmittedAndDefaultsToInfinite(Duration deadline) {
        var qos = EndpointQos.DEFAULT.toBuilder().deadline(deadline).build();
        var parameters = encode(qos);
        assertTrue(parameters.first(ParameterId.DEADLINE).isEmpty());
        assertEquals(EndpointQos.DEADLINE_INFINITE, RtpsQosParameters.read(parameters, true).deadline());
    }

    static Stream<Duration> finiteDeadlines() {
        return Stream.of(Duration.ZERO, Duration.ofNanos(1), Duration.ofMillis(250),
                Duration.ofSeconds(3, 123_456_789),
                Duration.ofSeconds(Integer.MAX_VALUE - 1, 999_999_999));
    }

    @ParameterizedTest
    @MethodSource("finiteDeadlines")
    void finiteDeadlineRetainsSecondsAndNanoseconds(Duration deadline) {
        var qos = EndpointQos.DEFAULT.toBuilder().deadline(deadline).build();
        var parameters = encode(qos);
        byte[] value = parameters.first(ParameterId.DEADLINE).orElseThrow();
        assertEquals(8, value.length);
        assertEquals(deadline.getSeconds(), RtpsIo.readInt(value, 0, true));
        assertEquals(deadline.getNano(), RtpsIo.readInt(value, 4, true));
        assertEquals(qos, RtpsQosParameters.read(parameters, true));
    }

    private static RtpsParameterList encode(EndpointQos qos) {
        return RtpsParameterList.read(RtpsParameterLists.payload(writer -> RtpsQosParameters.write(writer, qos)),
                true).orElseThrow();
    }
}
