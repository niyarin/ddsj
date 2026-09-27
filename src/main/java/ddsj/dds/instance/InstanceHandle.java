package ddsj.dds.instance;

import java.util.Objects;

/**
 * Opaque handle identifying an instance within a DataWriter or DataReader.
 * <p>
 * For keyed data types, each unique key value corresponds to a distinct instance.
 * For unkeyed data types, there is a single implicit instance.
 * <p>
 * Instance handles are local to a single DataWriter or DataReader and should not
 * be compared across different entities.
 */
public final class InstanceHandle implements Comparable<InstanceHandle> {
    /**
     * Special handle indicating no instance or an invalid handle.
     */
    public static final InstanceHandle NIL = new InstanceHandle(0);

    private final long value;

    private InstanceHandle(long value) {
        this.value = value;
    }

    /**
     * Creates an instance handle with the given value.
     * <p>
     * This is primarily for internal use. Application code should obtain
     * instance handles through DataWriter or DataReader operations.
     *
     * @param value the handle value
     * @return the instance handle
     */
    public static InstanceHandle of(long value) {
        return value == 0 ? NIL : new InstanceHandle(value);
    }

    /**
     * Returns the raw value of this handle.
     *
     * @return the handle value
     */
    public long value() {
        return value;
    }

    /**
     * Returns true if this handle is the NIL handle.
     *
     * @return true if this is NIL
     */
    public boolean isNil() {
        return value == 0;
    }

    @Override
    public int compareTo(InstanceHandle other) {
        return Long.compare(this.value, other.value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof InstanceHandle that)) return false;
        return value == that.value;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(value);
    }

    @Override
    public String toString() {
        return isNil() ? "InstanceHandle.NIL" : "InstanceHandle(" + value + ")";
    }
}
