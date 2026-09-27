package ddsj.dds.core;

import ddsj.dds.condition.StatusCondition;
import ddsj.dds.instance.InstanceHandle;
import ddsj.dds.listener.Listener;
import ddsj.dds.status.StatusMask;

/**
 * Base interface for all DDS entities.
 * <p>
 * Entity provides common operations for DomainParticipant, Publisher,
 * Subscriber, Topic, DataWriter, and DataReader.
 *
 * @param <L> the listener type for this entity
 */
public interface Entity<L extends Listener> extends AutoCloseable {

    /**
     * Returns the instance handle for this entity.
     *
     * @return the instance handle
     */
    InstanceHandle getInstanceHandle();

    /**
     * Returns the StatusCondition for this entity.
     * <p>
     * The StatusCondition can be used with a WaitSet to wait for
     * status changes on this entity.
     *
     * @return the status condition
     */
    StatusCondition getStatusCondition();

    /**
     * Returns the statuses that have changed since the last time
     * they were read.
     *
     * @return mask of changed statuses
     */
    StatusMask getStatusChanges();

    /**
     * Sets the listener and status mask for this entity.
     * <p>
     * The listener will be called when any of the enabled statuses change.
     *
     * @param listener the listener, or null to remove
     * @param mask the statuses to enable for the listener
     */
    void setListener(L listener, StatusMask mask);

    /**
     * Returns the current listener.
     *
     * @return the listener, or null if none
     */
    L getListener();

    /**
     * Enables this entity.
     * <p>
     * An entity must be enabled before it can participate in
     * communication. By default, entities are enabled on creation.
     */
    void enable();

    /**
     * Returns whether this entity is enabled.
     *
     * @return true if enabled
     */
    boolean isEnabled();

    /**
     * Closes this entity and releases resources.
     */
    @Override
    void close();
}
