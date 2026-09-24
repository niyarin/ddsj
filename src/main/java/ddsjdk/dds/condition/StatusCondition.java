package ddsjdk.dds.condition;

import ddsjdk.dds.status.StatusKind;
import ddsjdk.dds.status.StatusMask;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Condition that triggers when enabled statuses change.
 * <p>
 * Each Entity has an associated StatusCondition that can be used
 * with a WaitSet to wait for status changes.
 */
public final class StatusCondition implements Condition {
    private final Object entity;
    private final AtomicReference<StatusMask> enabledStatuses;
    private volatile StatusMask triggeredStatuses = StatusMask.NONE;

    /**
     * Creates a StatusCondition for the specified entity.
     *
     * @param entity the owning entity
     */
    public StatusCondition(Object entity) {
        this.entity = Objects.requireNonNull(entity, "entity");
        this.enabledStatuses = new AtomicReference<>(StatusMask.NONE);
    }

    /**
     * Returns the mask of enabled statuses.
     * <p>
     * The condition triggers when any enabled status changes.
     *
     * @return the enabled statuses
     */
    public StatusMask getEnabledStatuses() {
        return enabledStatuses.get();
    }

    /**
     * Sets the mask of enabled statuses.
     *
     * @param mask the statuses to enable
     */
    public void setEnabledStatuses(StatusMask mask) {
        Objects.requireNonNull(mask, "mask");
        enabledStatuses.set(mask);
    }

    /**
     * Returns the entity that owns this condition.
     *
     * @return the owning entity
     */
    public Object getEntity() {
        return entity;
    }

    @Override
    public boolean getTriggerValue() {
        StatusMask enabled = enabledStatuses.get();
        StatusMask triggered = triggeredStatuses;
        return !enabled.and(triggered).isEmpty();
    }

    /**
     * Called internally when a status changes.
     *
     * @param kind the status that changed
     */
    public void notifyStatusChange(StatusKind kind) {
        StatusMask current = triggeredStatuses;
        triggeredStatuses = current.or(StatusMask.of(kind));
    }

    /**
     * Clears the triggered status for the specified kind.
     *
     * @param kind the status kind to clear
     */
    public void clearStatus(StatusKind kind) {
        // For simplicity, we rebuild the mask without the specified kind
        StatusMask current = triggeredStatuses;
        java.util.Set<StatusKind> kinds = new java.util.HashSet<>(current.kinds());
        kinds.remove(kind);
        triggeredStatuses = kinds.isEmpty() ? StatusMask.NONE : StatusMask.of(kinds);
    }

    /**
     * Clears all triggered statuses.
     */
    public void clearAllStatuses() {
        triggeredStatuses = StatusMask.NONE;
    }
}
