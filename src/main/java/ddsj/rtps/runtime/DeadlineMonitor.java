package ddsj.rtps.runtime;

import java.io.Closeable;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Monitors deadline violations for DDS endpoints.
 *
 * For Reader: detects when data hasn't been received within the deadline period.
 * For Writer: detects when write() hasn't been called within the deadline period.
 */
public final class DeadlineMonitor implements Closeable {
    private final Duration deadline;
    private final Consumer<DeadlineMissedStatus> onDeadlineMissed;
    private final AtomicLong lastActivityTime = new AtomicLong(0); // 0 means not started
    private final AtomicLong totalMissedCount = new AtomicLong(0);
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final Thread monitorThread;

    /**
     * Creates a deadline monitor.
     *
     * @param deadline the deadline duration
     * @param onDeadlineMissed callback invoked when deadline is missed; null disables notifications only
     */
    public DeadlineMonitor(Duration deadline, Consumer<DeadlineMissedStatus> onDeadlineMissed) {
        this.deadline = deadline;
        this.onDeadlineMissed = onDeadlineMissed;
        this.monitorThread = new Thread(this::monitorLoop, "ddsj-deadline-monitor");
        this.monitorThread.setDaemon(true);
        this.monitorThread.start();
    }

    /**
     * Call this when activity occurs (data received or written).
     * Resets the deadline timer. First call starts the monitoring.
     */
    public void notifyActivity() {
        lastActivityTime.set(System.nanoTime());
        started.set(true);
    }

    /**
     * Returns the total number of deadline misses since creation.
     */
    public long totalMissedCount() {
        return totalMissedCount.get();
    }

    /**
     * Returns the deadline duration being monitored.
     */
    public Duration deadline() {
        return deadline;
    }

    private void monitorLoop() {
        long deadlineNanos = deadline.toNanos();
        // Check at reasonable intervals (at least every 10ms, at most deadline/2)
        long checkInterval = Math.max(10_000_000L, Math.min(deadlineNanos / 2, 100_000_000L));

        while (running.get()) {
            try {
                Thread.sleep(checkInterval / 1_000_000, (int) (checkInterval % 1_000_000));

                if (!running.get()) {
                    break;
                }

                // Don't check until first activity
                if (!started.get()) {
                    continue;
                }

                long now = System.nanoTime();
                long lastActivity = lastActivityTime.get();
                long elapsed = now - lastActivity;

                if (elapsed > deadlineNanos) {
                    // Deadline missed
                    long missedCount = totalMissedCount.incrementAndGet();

                    // Reset the timer to avoid repeated immediate callbacks
                    lastActivityTime.set(now);

                    if (onDeadlineMissed != null) {
                        try {
                            onDeadlineMissed.accept(new DeadlineMissedStatus(missedCount, deadline));
                        } catch (RuntimeException ignored) {
                            // Don't let callback exceptions kill the monitor thread
                        }
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
            monitorThread.interrupt();
        }
    }
}
