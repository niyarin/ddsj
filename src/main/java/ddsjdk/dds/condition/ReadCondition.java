package ddsjdk.dds.condition;

import ddsjdk.dds.instance.InstanceState;
import ddsjdk.dds.instance.SampleState;
import ddsjdk.dds.instance.ViewState;

import java.util.Objects;
import java.util.Set;

/**
 * Condition that triggers when matching samples are available.
 * <p>
 * ReadCondition is associated with a DataReader and triggers when
 * samples matching the specified state criteria are available.
 */
public non-sealed class ReadCondition implements Condition {
    private final Object dataReader;
    private final Set<SampleState> sampleStates;
    private final Set<ViewState> viewStates;
    private final Set<InstanceState> instanceStates;

    /**
     * Creates a ReadCondition for the specified DataReader.
     *
     * @param dataReader the owning DataReader
     * @param sampleStates the sample states to match
     * @param viewStates the view states to match
     * @param instanceStates the instance states to match
     */
    public ReadCondition(Object dataReader,
                         Set<SampleState> sampleStates,
                         Set<ViewState> viewStates,
                         Set<InstanceState> instanceStates) {
        this.dataReader = Objects.requireNonNull(dataReader, "dataReader");
        this.sampleStates = Set.copyOf(Objects.requireNonNull(sampleStates, "sampleStates"));
        this.viewStates = Set.copyOf(Objects.requireNonNull(viewStates, "viewStates"));
        this.instanceStates = Set.copyOf(Objects.requireNonNull(instanceStates, "instanceStates"));
    }

    /**
     * Returns the DataReader associated with this condition.
     *
     * @return the DataReader
     */
    public Object getDataReader() {
        return dataReader;
    }

    /**
     * Returns the sample states this condition matches.
     *
     * @return the sample states
     */
    public Set<SampleState> getSampleStateMask() {
        return sampleStates;
    }

    /**
     * Returns the view states this condition matches.
     *
     * @return the view states
     */
    public Set<ViewState> getViewStateMask() {
        return viewStates;
    }

    /**
     * Returns the instance states this condition matches.
     *
     * @return the instance states
     */
    public Set<InstanceState> getInstanceStateMask() {
        return instanceStates;
    }

    @Override
    public boolean getTriggerValue() {
        // Actual implementation will check DataReader for matching samples
        // For now, return false; will be connected when DataReader is implemented
        return false;
    }

    /**
     * Checks if the given sample info matches this condition's criteria.
     *
     * @param sampleState the sample's sample state
     * @param viewState the sample's view state
     * @param instanceState the sample's instance state
     * @return true if the sample matches
     */
    public boolean matches(SampleState sampleState, ViewState viewState, InstanceState instanceState) {
        return sampleStates.contains(sampleState)
                && viewStates.contains(viewState)
                && instanceStates.contains(instanceState);
    }
}
