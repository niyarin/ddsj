package ddsjdk.rtps.discovery;

import ddsjdk.rtps.parameter.RtpsParameterLists;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import static ddsjdk.rtps.discovery.EndpointQos.*;
import static org.junit.jupiter.api.Assertions.*;

class EndpointQosBuilderTest {
    @Test
    void omittedPoliciesUseDefaults() {
        assertEquals(DEFAULT, builder().build());
        var qos = builder().reliability(ReliabilityKind.RELIABLE).build();
        assertEquals(new EndpointQos(ReliabilityKind.RELIABLE, DurabilityKind.VOLATILE,
                HistoryKind.KEEP_LAST, 10), qos);
    }

    @Test
    void namedPoliciesPreserveConstructorValuesAndWireEncoding() {
        var expected = new EndpointQos(ReliabilityKind.RELIABLE, DurabilityKind.TRANSIENT_LOCAL,
                HistoryKind.KEEP_LAST, 37, Duration.ofMillis(250), OwnershipKind.EXCLUSIVE, 42,
                LivelinessKind.MANUAL_BY_TOPIC, Duration.ofSeconds(3));
        var actual = builder().reliability(ReliabilityKind.RELIABLE)
                .durability(DurabilityKind.TRANSIENT_LOCAL).keepLast(37)
                .deadline(Duration.ofMillis(250)).ownership(OwnershipKind.EXCLUSIVE, 42)
                .liveliness(LivelinessKind.MANUAL_BY_TOPIC, Duration.ofSeconds(3)).build();
        assertEquals(expected, actual);
        assertArrayEquals(RtpsParameterLists.payload(w -> RtpsQosParameters.write(w, expected)),
                RtpsParameterLists.payload(w -> RtpsQosParameters.write(w, actual)));
        assertEquals(actual, actual.toBuilder().build());
        var copy = actual.toBuilder().deadline(Duration.ofSeconds(1)).build();
        assertEquals(new EndpointQos(expected.reliability(), expected.durability(), expected.history(),
                expected.depth(), Duration.ofSeconds(1), expected.ownership(), expected.ownershipStrength(),
                expected.liveliness(), expected.leaseDuration()), copy);
        assertEquals(Duration.ofMillis(250), actual.deadline());
    }

    @Test
    void reusingBuilderDoesNotModifyPreviousResultsOrDefaults() {
        var builder = builder().keepLast(4);
        var first = builder.build();
        var second = builder.keepAll().build();
        assertEquals(HistoryKind.KEEP_LAST, first.history());
        assertEquals(4, first.depth());
        assertEquals(HistoryKind.KEEP_ALL, second.history());
        assertEquals(1, second.depth());
        assertEquals(10, DEFAULT.depth());
        assertEquals(HistoryKind.KEEP_LAST, DEFAULT.history());
        assertEquals(7, builder.history(HistoryKind.KEEP_ALL, 7).build().depth());
    }

    @Test
    void validatesNumericPoliciesAtBuildTime() {
        assertThrows(IllegalArgumentException.class, () -> builder().keepLast(0).build());
        assertThrows(IllegalArgumentException.class, () -> builder().deadline(Duration.ofNanos(-1)).build());
        assertThrows(IllegalArgumentException.class, () -> builder().ownership(OwnershipKind.EXCLUSIVE, -1).build());
        assertThrows(IllegalArgumentException.class,
                () -> builder().liveliness(LivelinessKind.AUTOMATIC, Duration.ofNanos(-1)).build());
    }

    @Test
    void rejectsNullKinds() {
        assertThrows(NullPointerException.class, () -> builder().reliability(null));
        assertThrows(NullPointerException.class, () -> builder().durability(null));
        assertThrows(NullPointerException.class, () -> builder().history(null, 1));
        assertThrows(NullPointerException.class, () -> builder().ownership(null, 0));
        assertThrows(NullPointerException.class, () -> builder().liveliness(null, Duration.ZERO));
    }

    @Test
    void nullDurationsRetainConstructorDefaults() {
        var qos = builder().deadline(null).liveliness(LivelinessKind.AUTOMATIC, null).build();
        assertEquals(DEFAULT, qos);
        assertFalse(qos.hasFiniteDeadline());
        assertFalse(qos.hasFiniteLeaseDuration());
    }
}
