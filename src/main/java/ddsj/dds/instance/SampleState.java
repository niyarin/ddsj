package ddsj.dds.instance;

import java.util.EnumSet;
import java.util.Set;

/**
 * Indicates whether a sample has been read by the application.
 * <p>
 * Each sample in the DataReader cache has a sample state that tracks
 * whether the application has accessed it via a read operation.
 */
public enum SampleState {
    /**
     * Sample has been read by at least one read or take operation.
     */
    READ(1),

    /**
     * Sample has not been read by any read or take operation.
     */
    NOT_READ(2);

    /**
     * Mask matching any sample state.
     */
    public static final Set<SampleState> ANY = EnumSet.allOf(SampleState.class);

    private final int value;

    SampleState(int value) {
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
     * Converts a bitmask to a set of sample states.
     *
     * @param mask the bitmask
     * @return the set of states
     */
    public static Set<SampleState> fromMask(int mask) {
        EnumSet<SampleState> result = EnumSet.noneOf(SampleState.class);
        for (SampleState state : values()) {
            if ((mask & state.value) != 0) {
                result.add(state);
            }
        }
        return result;
    }

    /**
     * Converts a set of sample states to a bitmask.
     *
     * @param states the set of states
     * @return the bitmask
     */
    public static int toMask(Set<SampleState> states) {
        int mask = 0;
        for (SampleState state : states) {
            mask |= state.value;
        }
        return mask;
    }
}
