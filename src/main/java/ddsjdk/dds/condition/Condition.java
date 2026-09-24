package ddsjdk.dds.condition;

/**
 * Base interface for all DDS conditions.
 * <p>
 * Conditions are used with WaitSet to wait for specific events.
 * Each condition has a trigger value that indicates whether
 * the condition's criteria are met.
 */
public sealed interface Condition permits StatusCondition, ReadCondition, GuardCondition {

    /**
     * Returns the current trigger value.
     * <p>
     * A condition is triggered when its criteria are met.
     * For StatusCondition, this means enabled statuses have changed.
     * For ReadCondition, this means matching samples are available.
     * For GuardCondition, this is set manually by the application.
     *
     * @return true if the condition is triggered
     */
    boolean getTriggerValue();
}
