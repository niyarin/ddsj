package ddsjdk.rtps.runtime;

import ddsjdk.rtps.types.Guid;

import java.io.Closeable;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Monitors liveliness of remote writers.
 * Detects when writers become "not alive" based on lease duration.
 */
public final class LivelinessMonitor implements Closeable {
    private final Duration leaseDuration;
    private final Consumer<LivelinessChangedStatus> onLivelinessChanged;
    private final Map<Guid, WriterState> writers = new ConcurrentHashMap<>();
    private final AtomicLong aliveCount = new AtomicLong(0);
    private final AtomicLong notAliveCount = new AtomicLong(0);
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Thread monitorThread;

    /**
     * Creates a liveliness monitor.
     *
     * @param leaseDuration the lease duration for writers
     * @param onLivelinessChanged callback invoked when a writer's liveliness changes
     */
    public LivelinessMonitor(Duration leaseDuration, Consumer<LivelinessChangedStatus> onLivelinessChanged) {
        this.leaseDuration = leaseDuration;
        this.onLivelinessChanged = onLivelinessChanged;
        this.monitorThread = new Thread(this::monitorLoop, "ddsjdk-liveliness-monitor");
        this.monitorThread.setDaemon(true);
        this.monitorThread.start();
    }

    /**
     * Notifies that a writer is alive (data received or liveliness asserted).
     *
     * @param writerGuid the GUID of the writer
     */
    public void assertLiveliness(Guid writerGuid) {
        WriterState state = writers.computeIfAbsent(writerGuid, k -> new WriterState());
        boolean wasAlive = state.isAlive();
        state.updateActivity();

        if (!wasAlive && state.isAlive()) {
            // Writer became alive
            aliveCount.incrementAndGet();
            notifyChange(writerGuid, true);
        }
    }

    /**
     * Returns the number of currently alive writers.
     */
    public long aliveCount() {
        return aliveCount.get();
    }

    /**
     * Returns the number of writers that have become not alive.
     */
    public long notAliveCount() {
        return notAliveCount.get();
    }

    /**
     * Returns the lease duration being monitored.
     */
    public Duration leaseDuration() {
        return leaseDuration;
    }

    /**
     * Checks if a specific writer is currently alive.
     */
    public boolean isWriterAlive(Guid writerGuid) {
        WriterState state = writers.get(writerGuid);
        return state != null && state.isAlive();
    }

    private void monitorLoop() {
        long leaseNanos = leaseDuration.toNanos();
        // Check at reasonable intervals
        long checkInterval = Math.max(10_000_000L, Math.min(leaseNanos / 4, 100_000_000L));

        while (running.get()) {
            try {
                Thread.sleep(checkInterval / 1_000_000, (int) (checkInterval % 1_000_000));

                if (!running.get()) {
                    break;
                }

                long now = System.nanoTime();

                for (Map.Entry<Guid, WriterState> entry : writers.entrySet()) {
                    WriterState state = entry.getValue();
                    if (state.isAlive() && state.hasExpired(now, leaseNanos)) {
                        // Writer became not alive
                        state.setNotAlive();
                        aliveCount.decrementAndGet();
                        notAliveCount.incrementAndGet();
                        notifyChange(entry.getKey(), false);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void notifyChange(Guid writerGuid, boolean alive) {
        try {
            onLivelinessChanged.accept(new LivelinessChangedStatus(
                    writerGuid, alive, aliveCount.get(), notAliveCount.get()));
        } catch (RuntimeException ignored) {
            // Don't let callback exceptions kill the monitor thread
        }
    }

    @Override
    public void close() {
        if (running.compareAndSet(true, false)) {
            monitorThread.interrupt();
        }
    }

    /**
     * Status information provided when liveliness changes.
     */
    public record LivelinessChangedStatus(
            Guid writerGuid,
            boolean alive,
            long aliveCount,
            long notAliveCount) {}

    private static final class WriterState {
        private volatile long lastActivityTime = 0;
        private volatile boolean alive = false;

        void updateActivity() {
            lastActivityTime = System.nanoTime();
            alive = true;
        }

        boolean isAlive() {
            return alive;
        }

        void setNotAlive() {
            alive = false;
        }

        boolean hasExpired(long now, long leaseNanos) {
            return lastActivityTime > 0 && (now - lastActivityTime) > leaseNanos;
        }
    }
}
