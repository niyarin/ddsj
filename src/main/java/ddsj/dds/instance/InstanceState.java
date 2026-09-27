package ddsj.dds.instance;

import java.util.EnumSet;
import java.util.Set;

/**
 * Indicates the lifecycle state of an instance.
 * <p>
 * The instance state reflects whether there are active writers for the instance
 * and whether the instance has been disposed.
 */
public enum InstanceState {
    /**
     * Instance is being actively updated by at least one DataWriter.
     */
    ALIVE(1),

    /**
     * Instance has been explicitly disposed by a DataWriter.
     * The instance may be re-created by writing new data.
     */
    NOT_ALIVE_DISPOSED(2),

    /**
     * Instance has no active DataWriters.
     * All DataWriters that were writing this instance have either
     * unregistered the instance or lost liveliness.
     */
    NOT_ALIVE_NO_WRITERS(4);

    /**
     * Mask matching any instance state.
     */
    public static final Set<InstanceState> ANY = EnumSet.allOf(InstanceState.class);

    /**
     * Mask matching any not-alive state (disposed or no writers).
     */
    public static final Set<InstanceState> NOT_ALIVE = EnumSet.of(NOT_ALIVE_DISPOSED, NOT_ALIVE_NO_WRITERS);

    private final int value;

    InstanceState(int value) {
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
     * Returns true if this instance state indicates the instance is alive.
     *
     * @return true if alive
     */
    public boolean isAlive() {
        return this == ALIVE;
    }

    /**
     * Converts a bitmask to a set of instance states.
     *
     * @param mask the bitmask
     * @return the set of states
     */
    public static Set<InstanceState> fromMask(int mask) {
        EnumSet<InstanceState> result = EnumSet.noneOf(InstanceState.class);
        for (InstanceState state : values()) {
            if ((mask & state.value) != 0) {
                result.add(state);
            }
        }
        return result;
    }

    /**
     * Converts a set of instance states to a bitmask.
     *
     * @param states the set of states
     * @return the bitmask
     */
    public static int toMask(Set<InstanceState> states) {
        int mask = 0;
        for (InstanceState state : states) {
            mask |= state.value;
        }
        return mask;
    }
}
