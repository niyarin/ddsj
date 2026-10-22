package ddsj.dds.condition;

import ddsj.dds.status.StatusKind;
import ddsj.dds.status.StatusMask;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.IntConsumer;

import static org.junit.jupiter.api.Assertions.*;

class StatusConditionTest {
    @Test
    void onlyEnabledStatusesTriggerAndClearingPreservesOthers() {
        var condition = new StatusCondition(new Object());
        condition.notifyStatusChange(StatusKind.DATA_AVAILABLE);
        assertFalse(condition.getTriggerValue());
        condition.setEnabledStatuses(StatusMask.of(StatusKind.DATA_AVAILABLE));
        assertTrue(condition.getTriggerValue());
        condition.notifyStatusChange(StatusKind.SAMPLE_LOST);
        condition.clearStatus(StatusKind.DATA_AVAILABLE);
        assertFalse(condition.getTriggerValue());
        condition.setEnabledStatuses(StatusMask.of(StatusKind.SAMPLE_LOST));
        assertTrue(condition.getTriggerValue());
        condition.clearAllStatuses();
        assertFalse(condition.getTriggerValue());
    }

    @Test
    void concurrentNotificationsPreserveEveryKind() throws Exception {
        var condition = new StatusCondition(new Object());
        StatusKind[] kinds = StatusKind.values();
        race(kinds.length, () -> condition.clearAllStatuses(),
                i -> condition.notifyStatusChange(kinds[i]), () -> {
                    for (StatusKind kind : kinds) {
                        condition.setEnabledStatuses(StatusMask.of(kind));
                        assertTrue(condition.getTriggerValue(), kind.toString());
                    }
                });
    }

    @Test
    void clearingOneKindDoesNotLoseConcurrentNotification() throws Exception {
        var condition = new StatusCondition(new Object());
        race(2, () -> {
            condition.clearAllStatuses();
            condition.notifyStatusChange(StatusKind.DATA_AVAILABLE);
        }, i -> {
            if (i == 0) condition.clearStatus(StatusKind.DATA_AVAILABLE);
            else condition.notifyStatusChange(StatusKind.SAMPLE_LOST);
        }, () -> {
            condition.setEnabledStatuses(StatusMask.of(StatusKind.DATA_AVAILABLE));
            assertFalse(condition.getTriggerValue());
            condition.setEnabledStatuses(StatusMask.of(StatusKind.SAMPLE_LOST));
            assertTrue(condition.getTriggerValue());
        });
    }

    private static void race(int workers, Runnable setup, IntConsumer action, Runnable verify)
            throws Exception {
        var executor = Executors.newFixedThreadPool(workers);
        var barrier = new CyclicBarrier(workers + 1);
        try {
            var tasks = new java.util.ArrayList<java.util.concurrent.Future<?>>();
            for (int i = 0; i < workers; i++) {
                int index = i;
                tasks.add(executor.submit(() -> {
                    for (int round = 0; round < 500; round++) {
                        barrier.await(10, TimeUnit.SECONDS);
                        action.accept(index);
                        barrier.await(10, TimeUnit.SECONDS);
                    }
                    return null;
                }));
            }
            for (int round = 0; round < 500; round++) {
                setup.run();
                barrier.await(10, TimeUnit.SECONDS);
                barrier.await(10, TimeUnit.SECONDS);
                verify.run();
            }
            for (var task : tasks) task.get(10, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }
}
