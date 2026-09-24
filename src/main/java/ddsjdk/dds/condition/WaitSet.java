package ddsjdk.dds.condition;

import ddsjdk.dds.exception.DdsException;
import ddsjdk.dds.exception.ReturnCode;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * DDS WaitSet for waiting on multiple conditions.
 * <p>
 * WaitSet allows an application to wait for one or more conditions
 * to trigger. This provides an alternative to polling for data.
 */
public final class WaitSet {
    private final Set<ddsjdk.dds.condition.Condition> attachedConditions = ConcurrentHashMap.newKeySet();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition conditionSignal = lock.newCondition();

    /**
     * Creates an empty WaitSet.
     */
    public WaitSet() {
    }

    /**
     * Attaches a condition to this WaitSet.
     *
     * @param condition the condition to attach
     * @return OK if successful
     */
    public ReturnCode attachCondition(ddsjdk.dds.condition.Condition condition) {
        Objects.requireNonNull(condition, "condition");
        attachedConditions.add(condition);
        return ReturnCode.OK;
    }

    /**
     * Detaches a condition from this WaitSet.
     *
     * @param condition the condition to detach
     * @return OK if successful, PRECONDITION_NOT_MET if not attached
     */
    public ReturnCode detachCondition(ddsjdk.dds.condition.Condition condition) {
        if (attachedConditions.remove(condition)) {
            return ReturnCode.OK;
        }
        return ReturnCode.PRECONDITION_NOT_MET;
    }

    /**
     * Waits for conditions to trigger with a timeout.
     * <p>
     * Returns when at least one attached condition is triggered,
     * or when the timeout expires.
     *
     * @param timeout the maximum time to wait
     * @return the list of triggered conditions
     * @throws InterruptedException if interrupted while waiting
     */
    public List<ddsjdk.dds.condition.Condition> wait(Duration timeout) throws InterruptedException {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative()) {
            throw new DdsException(ReturnCode.BAD_PARAMETER, "timeout must not be negative");
        }

        long deadlineNanos = System.nanoTime() + timeout.toNanos();

        lock.lock();
        try {
            while (true) {
                // Check for triggered conditions
                List<ddsjdk.dds.condition.Condition> triggered = getTriggeredConditions();
                if (!triggered.isEmpty()) {
                    return triggered;
                }

                // Calculate remaining time
                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0) {
                    return List.of(); // Timeout
                }

                // Wait with timeout
                conditionSignal.await(Math.min(remainingNanos, TimeUnit.MILLISECONDS.toNanos(100)),
                        TimeUnit.NANOSECONDS);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Waits indefinitely for conditions to trigger.
     *
     * @return the list of triggered conditions
     * @throws InterruptedException if interrupted while waiting
     */
    public List<ddsjdk.dds.condition.Condition> waitIndefinitely() throws InterruptedException {
        return wait(Duration.ofDays(365)); // Effectively infinite
    }

    /**
     * Returns all attached conditions.
     *
     * @return the conditions
     */
    public List<ddsjdk.dds.condition.Condition> getConditions() {
        return List.copyOf(attachedConditions);
    }

    /**
     * Signals that a condition may have changed.
     * <p>
     * This is called internally when conditions are potentially triggered.
     */
    public void signal() {
        lock.lock();
        try {
            conditionSignal.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private List<ddsjdk.dds.condition.Condition> getTriggeredConditions() {
        List<ddsjdk.dds.condition.Condition> result = new ArrayList<>();
        for (ddsjdk.dds.condition.Condition condition : attachedConditions) {
            if (condition.getTriggerValue()) {
                result.add(condition);
            }
        }
        return result;
    }
}
