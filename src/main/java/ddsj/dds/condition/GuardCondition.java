package ddsj.dds.condition;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Condition with application-controlled trigger value.
 * <p>
 * GuardCondition allows the application to manually trigger
 * a WaitSet wait by setting the trigger value.
 */
public final class GuardCondition implements Condition {
    private final AtomicBoolean triggerValue = new AtomicBoolean(false);

    /**
     * Creates a GuardCondition with initial trigger value of false.
     */
    public GuardCondition() {
    }

    @Override
    public boolean getTriggerValue() {
        return triggerValue.get();
    }

    /**
     * Sets the trigger value.
     * <p>
     * Setting to true will wake any WaitSets waiting on this condition.
     *
     * @param value the new trigger value
     */
    public void setTriggerValue(boolean value) {
        triggerValue.set(value);
    }
}
