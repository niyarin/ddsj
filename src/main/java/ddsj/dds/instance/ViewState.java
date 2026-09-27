package ddsj.dds.instance;

import java.util.EnumSet;
import java.util.Set;

/**
 * Indicates whether an instance is new to the DataReader.
 * <p>
 * The view state tracks whether the instance has been seen before by
 * the DataReader. An instance transitions from NEW to NOT_NEW after
 * the first sample for that instance is accessed.
 */
public enum ViewState {
    /**
     * Instance has not been seen before by this DataReader.
     * This is the state for instances that have been created or re-created
     * since the last read or take operation.
     */
    NEW(1),

    /**
     * Instance has been seen before by this DataReader.
     */
    NOT_NEW(2);

    /**
     * Mask matching any view state.
     */
    public static final Set<ViewState> ANY = EnumSet.allOf(ViewState.class);

    private final int value;

    ViewState(int value) {
        this.value = value;
    }

    /**
     * Returns the bitmask value for this state.
     *
     * @return the bitmask value
     */
    public int value() {
        return value;
    }

    /**
     * Converts a bitmask to a set of view states.
     *
     * @param mask the bitmask
     * @return the set of states
     */
    public static Set<ViewState> fromMask(int mask) {
        EnumSet<ViewState> result = EnumSet.noneOf(ViewState.class);
        for (ViewState state : values()) {
            if ((mask & state.value) != 0) {
                result.add(state);
            }
        }
        return result;
    }

    /**
     * Converts a set of view states to a bitmask.
     *
     * @param states the set of states
     * @return the bitmask
     */
    public static int toMask(Set<ViewState> states) {
        int mask = 0;
        for (ViewState state : states) {
            mask |= state.value;
        }
        return mask;
    }
}
