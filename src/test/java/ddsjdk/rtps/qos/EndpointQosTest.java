package ddsjdk.rtps.qos;

import ddsjdk.rtps.qos.EndpointQos.DurabilityKind;
import ddsjdk.rtps.qos.EndpointQos.HistoryKind;
import ddsjdk.rtps.qos.EndpointQos.OwnershipKind;
import ddsjdk.rtps.qos.EndpointQos.ReliabilityKind;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class EndpointQosTest {

    @Test
    void deadline_writerFasterThanReader_compatible() {
        // Writer offers 100ms deadline (can send every 100ms)
        // Reader requests 200ms deadline (needs data every 200ms)
        // Compatible: 100ms <= 200ms
        var writerQos = new EndpointQos(
                ReliabilityKind.BEST_EFFORT,
                DurabilityKind.VOLATILE,
                HistoryKind.KEEP_LAST,
                10,
                Duration.ofMillis(100));
        var readerQos = new EndpointQos(
                ReliabilityKind.BEST_EFFORT,
                DurabilityKind.VOLATILE,
                HistoryKind.KEEP_LAST,
                10,
                Duration.ofMillis(200));

        assertTrue(writerQos.isCompatibleWithRequested(readerQos));
    }

    @Test
    void deadline_writerSlowerThanReader_incompatible() {
        // Writer offers 500ms deadline (can only send every 500ms)
        // Reader requests 100ms deadline (needs data every 100ms)
        // Incompatible: 500ms > 100ms
        var writerQos = new EndpointQos(
                ReliabilityKind.BEST_EFFORT,
                DurabilityKind.VOLATILE,
                HistoryKind.KEEP_LAST,
                10,
                Duration.ofMillis(500));
        var readerQos = new EndpointQos(
                ReliabilityKind.BEST_EFFORT,
                DurabilityKind.VOLATILE,
                HistoryKind.KEEP_LAST,
                10,
                Duration.ofMillis(100));

        assertFalse(writerQos.isCompatibleWithRequested(readerQos));
    }

    @Test
    void deadline_infiniteIsAlwaysCompatible() {
        var infiniteDeadlineQos = EndpointQos.DEFAULT;
        var finiteDeadlineQos = new EndpointQos(
                ReliabilityKind.BEST_EFFORT,
                DurabilityKind.VOLATILE,
                HistoryKind.KEEP_LAST,
                10,
                Duration.ofMillis(100));

        assertTrue(infiniteDeadlineQos.isCompatibleWithRequested(finiteDeadlineQos));
        assertTrue(finiteDeadlineQos.isCompatibleWithRequested(infiniteDeadlineQos));
    }

    @Test
    void deadline_sameDeadline_compatible() {
        var qos1 = new EndpointQos(
                ReliabilityKind.BEST_EFFORT,
                DurabilityKind.VOLATILE,
                HistoryKind.KEEP_LAST,
                10,
                Duration.ofMillis(100));
        var qos2 = new EndpointQos(
                ReliabilityKind.BEST_EFFORT,
                DurabilityKind.VOLATILE,
                HistoryKind.KEEP_LAST,
                10,
                Duration.ofMillis(100));

        assertTrue(qos1.isCompatibleWithRequested(qos2));
    }

    @Test
    void reliability_bestEffortWriter_reliableReader_incompatible() {
        var writerQos = new EndpointQos(
                ReliabilityKind.BEST_EFFORT,
                DurabilityKind.VOLATILE,
                HistoryKind.KEEP_LAST,
                10);
        var readerQos = new EndpointQos(
                ReliabilityKind.RELIABLE,
                DurabilityKind.VOLATILE,
                HistoryKind.KEEP_LAST,
                10);

        assertFalse(writerQos.isCompatibleWithRequested(readerQos));
    }

    @Test
    void reliability_reliableWriter_bestEffortReader_compatible() {
        var writerQos = new EndpointQos(
                ReliabilityKind.RELIABLE,
                DurabilityKind.VOLATILE,
                HistoryKind.KEEP_LAST,
                10);
        var readerQos = new EndpointQos(
                ReliabilityKind.BEST_EFFORT,
                DurabilityKind.VOLATILE,
                HistoryKind.KEEP_LAST,
                10);

        assertTrue(writerQos.isCompatibleWithRequested(readerQos));
    }

    @Test
    void durability_volatileWriter_transientLocalReader_incompatible() {
        var writerQos = new EndpointQos(
                ReliabilityKind.BEST_EFFORT,
                DurabilityKind.VOLATILE,
                HistoryKind.KEEP_LAST,
                10);
        var readerQos = new EndpointQos(
                ReliabilityKind.BEST_EFFORT,
                DurabilityKind.TRANSIENT_LOCAL,
                HistoryKind.KEEP_LAST,
                10);

        assertFalse(writerQos.isCompatibleWithRequested(readerQos));
    }

    @Test
    void durability_transientLocalWriter_volatileReader_compatible() {
        var writerQos = new EndpointQos(
                ReliabilityKind.BEST_EFFORT,
                DurabilityKind.TRANSIENT_LOCAL,
                HistoryKind.KEEP_LAST,
                10);
        var readerQos = new EndpointQos(
                ReliabilityKind.BEST_EFFORT,
                DurabilityKind.VOLATILE,
                HistoryKind.KEEP_LAST,
                10);

        assertTrue(writerQos.isCompatibleWithRequested(readerQos));
    }

    @Test
    void hasFiniteDeadline() {
        var infinite = EndpointQos.DEFAULT;
        var finite = new EndpointQos(
                ReliabilityKind.BEST_EFFORT,
                DurabilityKind.VOLATILE,
                HistoryKind.KEEP_LAST,
                10,
                Duration.ofMillis(100));

        assertFalse(infinite.hasFiniteDeadline());
        assertTrue(finite.hasFiniteDeadline());
    }

    @Test
    void constructor_negativeDeadline_throws() {
        assertThrows(IllegalArgumentException.class, () ->
                new EndpointQos(
                        ReliabilityKind.BEST_EFFORT,
                        DurabilityKind.VOLATILE,
                        HistoryKind.KEEP_LAST,
                        10,
                        Duration.ofMillis(-100)));
    }

    @Test
    void constructor_zeroDepth_throws() {
        assertThrows(IllegalArgumentException.class, () ->
                new EndpointQos(
                        ReliabilityKind.BEST_EFFORT,
                        DurabilityKind.VOLATILE,
                        HistoryKind.KEEP_LAST,
                        0));
    }

    @Test
    void ownership_sameKind_compatible() {
        var sharedWriter = new EndpointQos(
                ReliabilityKind.BEST_EFFORT,
                DurabilityKind.VOLATILE,
                HistoryKind.KEEP_LAST,
                10,
                EndpointQos.DEADLINE_INFINITE,
                OwnershipKind.SHARED,
                0);
        var sharedReader = new EndpointQos(
                ReliabilityKind.BEST_EFFORT,
                DurabilityKind.VOLATILE,
                HistoryKind.KEEP_LAST,
                10,
                EndpointQos.DEADLINE_INFINITE,
                OwnershipKind.SHARED,
                0);

        assertTrue(sharedWriter.isCompatibleWithRequested(sharedReader));
    }

    @Test
    void ownership_exclusiveBoth_compatible() {
        var exclusiveWriter = new EndpointQos(
                ReliabilityKind.BEST_EFFORT,
                DurabilityKind.VOLATILE,
                HistoryKind.KEEP_LAST,
                10,
                EndpointQos.DEADLINE_INFINITE,
                OwnershipKind.EXCLUSIVE,
                100);
        var exclusiveReader = new EndpointQos(
                ReliabilityKind.BEST_EFFORT,
                DurabilityKind.VOLATILE,
                HistoryKind.KEEP_LAST,
                10,
                EndpointQos.DEADLINE_INFINITE,
                OwnershipKind.EXCLUSIVE,
                0);

        assertTrue(exclusiveWriter.isCompatibleWithRequested(exclusiveReader));
    }

    @Test
    void ownership_differentKinds_incompatible() {
        var sharedWriter = new EndpointQos(
                ReliabilityKind.BEST_EFFORT,
                DurabilityKind.VOLATILE,
                HistoryKind.KEEP_LAST,
                10,
                EndpointQos.DEADLINE_INFINITE,
                OwnershipKind.SHARED,
                0);
        var exclusiveReader = new EndpointQos(
                ReliabilityKind.BEST_EFFORT,
                DurabilityKind.VOLATILE,
                HistoryKind.KEEP_LAST,
                10,
                EndpointQos.DEADLINE_INFINITE,
                OwnershipKind.EXCLUSIVE,
                0);

        assertFalse(sharedWriter.isCompatibleWithRequested(exclusiveReader));
        assertFalse(exclusiveReader.isCompatibleWithRequested(sharedWriter));
    }

    @Test
    void ownership_defaultIsShared() {
        assertEquals(OwnershipKind.SHARED, EndpointQos.DEFAULT.ownership());
        assertEquals(0, EndpointQos.DEFAULT.ownershipStrength());
    }

    @Test
    void constructor_negativeOwnershipStrength_throws() {
        assertThrows(IllegalArgumentException.class, () ->
                new EndpointQos(
                        ReliabilityKind.BEST_EFFORT,
                        DurabilityKind.VOLATILE,
                        HistoryKind.KEEP_LAST,
                        10,
                        EndpointQos.DEADLINE_INFINITE,
                        OwnershipKind.EXCLUSIVE,
                        -1));
    }
}
