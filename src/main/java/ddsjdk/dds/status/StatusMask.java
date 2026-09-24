package ddsjdk.dds.status;

import java.util.EnumSet;
import java.util.Set;

/**
 * Bitmask for selecting which status changes to monitor.
 * <p>
 * StatusMask is used to configure which statuses trigger listener callbacks
 * and which statuses are monitored by StatusConditions.
 */
public final class StatusMask {
    private final Set<StatusKind> kinds;

    private StatusMask(Set<StatusKind> kinds) {
        this.kinds = EnumSet.copyOf(kinds);
    }

    /**
     * An empty mask that matches no statuses.
     */
    public static final StatusMask NONE = new StatusMask(EnumSet.noneOf(StatusKind.class));

    /**
     * A mask that matches all statuses.
     */
    public static final StatusMask ALL = new StatusMask(EnumSet.allOf(StatusKind.class));

    /**
     * Creates a mask for a single status kind.
     *
     * @param kind the status kind
     * @return a mask for the specified kind
     */
    public static StatusMask of(StatusKind kind) {
        return new StatusMask(EnumSet.of(kind));
    }

    /**
     * Creates a mask for multiple status kinds.
     *
     * @param kinds the status kinds
     * @return a mask for the specified kinds
     */
    public static StatusMask of(StatusKind... kinds) {
        if (kinds.length == 0) {
            return NONE;
        }
        return new StatusMask(EnumSet.of(kinds[0], kinds));
    }

    /**
     * Creates a mask from a set of status kinds.
     *
     * @param kinds the status kinds
     * @return a mask for the specified kinds
     */
    public static StatusMask of(Set<StatusKind> kinds) {
        if (kinds.isEmpty()) {
            return NONE;
        }
        return new StatusMask(kinds);
    }

    /**
     * Returns whether this mask includes the specified status kind.
     *
     * @param kind the status kind
     * @return true if included
     */
    public boolean contains(StatusKind kind) {
        return kinds.contains(kind);
    }

    /**
     * Returns the union of this mask and another.
     *
     * @param other the other mask
     * @return the union
     */
    public StatusMask or(StatusMask other) {
        EnumSet<StatusKind> result = EnumSet.copyOf(this.kinds);
        result.addAll(other.kinds);
        return new StatusMask(result);
    }

    /**
     * Returns the intersection of this mask and another.
     *
     * @param other the other mask
     * @return the intersection
     */
    public StatusMask and(StatusMask other) {
        EnumSet<StatusKind> result = EnumSet.copyOf(this.kinds);
        result.retainAll(other.kinds);
        return result.isEmpty() ? NONE : new StatusMask(result);
    }

    /**
     * Returns the status kinds in this mask.
     *
     * @return an unmodifiable set of status kinds
     */
    public Set<StatusKind> kinds() {
        return Set.copyOf(kinds);
    }

    /**
     * Returns true if this mask is empty.
     *
     * @return true if empty
     */
    public boolean isEmpty() {
        return kinds.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StatusMask that)) return false;
        return kinds.equals(that.kinds);
    }

    @Override
    public int hashCode() {
        return kinds.hashCode();
    }

    @Override
    public String toString() {
        return "StatusMask" + kinds;
    }
}
