package ddsj.rtps.util;

import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CloseablesTest {
    @Test void acquisitionFailureRollsBackInReverseOrderAndPreservesOriginalFailure() {
        var closed = new ArrayList<Integer>();
        var failure = new IOException("open");
        var cleanup = new IllegalStateException("close");
        var actual = assertThrows(IOException.class, () -> Closeables.openAll(
                () -> () -> closed.add(1),
                () -> () -> { closed.add(2); throw cleanup; },
                () -> { throw failure; }));
        assertSame(failure, actual);
        assertEquals(List.of(2, 1), closed);
        assertArrayEquals(new Throwable[]{cleanup}, actual.getSuppressed());
    }

    @Test void closeContinuesAfterCheckedAndUncheckedFailuresInSuppliedOrder() {
        for (boolean uncheckedFirst : List.of(false, true)) {
            var closed = new ArrayList<Integer>();
            var checked = new IOException("checked");
            var unchecked = new IllegalStateException("unchecked");
            Closeable first = () -> { closed.add(1); if (uncheckedFirst) throw unchecked; else throw checked; };
            Closeable second = () -> { closed.add(2); if (uncheckedFirst) throw checked; else throw unchecked; };
            var actual = assertThrows(Exception.class, () -> Closeables.closeAll(
                    List.of(first, second, () -> closed.add(3))));
            assertSame(uncheckedFirst ? unchecked : checked, actual);
            assertArrayEquals(new Throwable[]{uncheckedFirst ? checked : unchecked}, actual.getSuppressed());
            assertEquals(List.of(1, 2, 3), closed);
        }
    }

    @Test void repeatedFailureInstanceDoesNotInterruptCleanup() {
        var failure = new IOException("shared");
        var closed = new ArrayList<Integer>();
        Closeable failing = () -> { throw failure; };
        assertSame(failure, assertThrows(IOException.class,
                () -> Closeables.closeAll(List.of(failing, failing, () -> closed.add(1)))));
        assertEquals(List.of(1), closed);
        assertEquals(0, failure.getSuppressed().length);
    }
}
