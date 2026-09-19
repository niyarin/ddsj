package ddsjdk.rtps.runtime;

import ddsjdk.rtps.discovery.EndpointQos.LivelinessKind;

import java.io.Closeable;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Handles liveliness assertion for DataWriters.
 *
 * For AUTOMATIC: automatically asserts liveliness when write() is called
 * For MANUAL_BY_TOPIC: application must call assertLiveliness()
 */
public final class LivelinessAsserter implements Closeable {
    private final LivelinessKind kind;
    private final Duration leaseDuration;
    private final Runnable onAssert;
    private final AtomicLong lastAssertTime = new AtomicLong(0);
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Thread asserterThread;

    /**
     * Creates a liveliness asserter.
     *
     * @param kind the liveliness kind
     * @param leaseDuration the lease duration
     * @param onAssert callback invoked when liveliness should be asserted (sends message)
     */
    public LivelinessAsserter(LivelinessKind kind, Duration leaseDuration, Runnable onAssert) {
        this.kind = kind;
        this.leaseDuration = leaseDuration;
        this.onAssert = onAssert;

        // Only start automatic assertion thread for AUTOMATIC kind
        if (kind == LivelinessKind.AUTOMATIC && isFiniteLease()) {
            this.asserterThread = new Thread(this::autoAssertLoop, "ddsjdk-liveliness-asserter");
            this.asserterThread.setDaemon(true);
            this.asserterThread.start();
        } else {
            this.asserterThread = null;
        }
    }

    /**
     * Called when data is written. For AUTOMATIC kind, this resets the liveliness timer.
     */
    public void onDataWritten() {
        if (kind == LivelinessKind.AUTOMATIC) {
            lastAssertTime.set(System.nanoTime());
        }
    }

    /**
     * Manually asserts liveliness. Required for MANUAL_BY_TOPIC kind.
     * For AUTOMATIC, this is a no-op (liveliness is asserted automatically).
     */
    public void assertLiveliness() {
        if (kind == LivelinessKind.MANUAL_BY_TOPIC || kind == LivelinessKind.MANUAL_BY_PARTICIPANT) {
            lastAssertTime.set(System.nanoTime());
            try {
                onAssert.run();
            } catch (RuntimeException ignored) {
            }
        }
    }

    /**
     * Returns the liveliness kind.
     */
    public LivelinessKind kind() {
        return kind;
    }

    /**
     * Returns the lease duration.
     */
    public Duration leaseDuration() {
        return leaseDuration;
    }

    private boolean isFiniteLease() {
        return leaseDuration.getSeconds() < Integer.MAX_VALUE;
    }

    private void autoAssertLoop() {
        long leaseNanos = leaseDuration.toNanos();
        // Assert at 1/3 of lease duration to ensure we don't miss
        long assertInterval = leaseNanos / 3;

        while (running.get()) {
            try {
                Thread.sleep(assertInterval / 1_000_000, (int) (assertInterval % 1_000_000));

                if (!running.get()) {
                    break;
                }

                long now = System.nanoTime();
                long lastAssert = lastAssertTime.get();

                // If we haven't written data recently, send a liveliness assertion
                if (lastAssert == 0 || (now - lastAssert) > assertInterval) {
                    lastAssertTime.set(now);
                    try {
                        onAssert.run();
                    } catch (RuntimeException ignored) {
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    @Override
    public void close() {
        if (running.compareAndSet(true, false)) {
            if (asserterThread != null) {
                asserterThread.interrupt();
            }
        }
    }
}
