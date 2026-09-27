package ddsj.rtps.qos;

import java.time.Duration;
import java.util.Objects;

/** Immutable endpoint policies. Prefer {@link #builder()} for named configuration. */
public record EndpointQos(
        ReliabilityKind reliability,
        DurabilityKind durability,
        HistoryKind history,
        int depth,
        Duration deadline,
        OwnershipKind ownership,
        int ownershipStrength,
        LivelinessKind liveliness,
        Duration leaseDuration) {

    /** Infinite deadline (no deadline checking) */
    public static final Duration DEADLINE_INFINITE = Duration.ofSeconds(Integer.MAX_VALUE);

    /** Infinite lease duration (no liveliness checking) */
    public static final Duration LEASE_DURATION_INFINITE = Duration.ofSeconds(Integer.MAX_VALUE);

    /** Default ownership strength */
    public static final int DEFAULT_OWNERSHIP_STRENGTH = 0;

    public static final EndpointQos DEFAULT = new EndpointQos(
            ReliabilityKind.BEST_EFFORT,
            DurabilityKind.VOLATILE,
            HistoryKind.KEEP_LAST,
            10,
            DEADLINE_INFINITE,
            OwnershipKind.SHARED,
            DEFAULT_OWNERSHIP_STRENGTH,
            LivelinessKind.AUTOMATIC,
            LEASE_DURATION_INFINITE);

    public EndpointQos {
        if (depth <= 0) {
            throw new IllegalArgumentException("endpoint QoS history depth must be positive");
        }
        if (deadline == null) {
            deadline = DEADLINE_INFINITE;
        }
        if (deadline.isNegative()) {
            throw new IllegalArgumentException("deadline must not be negative");
        }
        if (ownership == null) {
            ownership = OwnershipKind.SHARED;
        }
        if (ownershipStrength < 0) {
            throw new IllegalArgumentException("ownership strength must not be negative");
        }
        if (liveliness == null) {
            liveliness = LivelinessKind.AUTOMATIC;
        }
        if (leaseDuration == null) {
            leaseDuration = LEASE_DURATION_INFINITE;
        }
        if (leaseDuration.isNegative()) {
            throw new IllegalArgumentException("lease duration must not be negative");
        }
    }

    /** Constructor without deadline, ownership, and liveliness (uses defaults) */
    public EndpointQos(ReliabilityKind reliability, DurabilityKind durability, HistoryKind history, int depth) {
        this(reliability, durability, history, depth, DEADLINE_INFINITE, OwnershipKind.SHARED, DEFAULT_OWNERSHIP_STRENGTH,
                LivelinessKind.AUTOMATIC, LEASE_DURATION_INFINITE);
    }

    /** Constructor without ownership and liveliness (uses defaults) */
    public EndpointQos(ReliabilityKind reliability, DurabilityKind durability, HistoryKind history, int depth, Duration deadline) {
        this(reliability, durability, history, depth, deadline, OwnershipKind.SHARED, DEFAULT_OWNERSHIP_STRENGTH,
                LivelinessKind.AUTOMATIC, LEASE_DURATION_INFINITE);
    }

    /** Constructor with liveliness */
    public EndpointQos(ReliabilityKind reliability, DurabilityKind durability, HistoryKind history, int depth,
                       LivelinessKind liveliness, Duration leaseDuration) {
        this(reliability, durability, history, depth, DEADLINE_INFINITE, OwnershipKind.SHARED, DEFAULT_OWNERSHIP_STRENGTH,
                liveliness, leaseDuration);
    }

    /** Constructor with deadline and ownership (uses default liveliness) */
    public EndpointQos(ReliabilityKind reliability, DurabilityKind durability, HistoryKind history, int depth,
                       Duration deadline, OwnershipKind ownership, int ownershipStrength) {
        this(reliability, durability, history, depth, deadline, ownership, ownershipStrength,
                LivelinessKind.AUTOMATIC, LEASE_DURATION_INFINITE);
    }

    /** Starts with DEFAULT; omitted policies retain their default values. */
    public static Builder builder() {
        return DEFAULT.toBuilder();
    }

    /** Copies every policy for independent modification. */
    public Builder toBuilder() {
        return new Builder(this);
    }

    /**
     * Mutable builder producing immutable snapshots. Enum setters reject null.
     * Numeric and duration validation uses the constructor rules at build time.
     */
    public static final class Builder {
        private ReliabilityKind reliability;
        private DurabilityKind durability;
        private HistoryKind history;
        private int depth;
        private Duration deadline;
        private OwnershipKind ownership;
        private int ownershipStrength;
        private LivelinessKind liveliness;
        private Duration leaseDuration;

        private Builder(EndpointQos qos) {
            reliability = qos.reliability();
            durability = qos.durability();
            history = qos.history();
            depth = qos.depth();
            deadline = qos.deadline();
            ownership = qos.ownership();
            ownershipStrength = qos.ownershipStrength();
            liveliness = qos.liveliness();
            leaseDuration = qos.leaseDuration();
        }

        public Builder reliability(ReliabilityKind kind) {
            reliability = Objects.requireNonNull(kind, "reliability");
            return this;
        }

        public Builder durability(DurabilityKind kind) {
            durability = Objects.requireNonNull(kind, "durability");
            return this;
        }

        /** Configures history kind and its positive wire depth together. */
        public Builder history(HistoryKind kind, int depth) {
            this.history = Objects.requireNonNull(kind, "history");
            this.depth = depth;
            return this;
        }

        public Builder keepLast(int depth) {
            return history(HistoryKind.KEEP_LAST, depth);
        }

        /** Selects KEEP_ALL with a canonical positive wire depth of 1. */
        public Builder keepAll() {
            return history(HistoryKind.KEEP_ALL, 1);
        }

        /** Null selects DEADLINE_INFINITE, as in the constructors. */
        public Builder deadline(Duration deadline) {
            this.deadline = deadline;
            return this;
        }

        public Builder ownership(OwnershipKind kind, int strength) {
            this.ownership = Objects.requireNonNull(kind, "ownership");
            this.ownershipStrength = strength;
            return this;
        }

        /** Null lease duration selects LEASE_DURATION_INFINITE. */
        public Builder liveliness(LivelinessKind kind, Duration leaseDuration) {
            this.liveliness = Objects.requireNonNull(kind, "liveliness");
            this.leaseDuration = leaseDuration;
            return this;
        }

        public EndpointQos build() {
            Objects.requireNonNull(reliability, "reliability");
            Objects.requireNonNull(durability, "durability");
            Objects.requireNonNull(history, "history");
            return new EndpointQos(reliability, durability, history, depth, deadline,
                    ownership, ownershipStrength, liveliness, leaseDuration);
        }
    }

    public boolean isCompatibleWithRequested(EndpointQos requested) {
        if (requested.reliability == ReliabilityKind.RELIABLE && reliability != ReliabilityKind.RELIABLE) {
            return false;
        }
        if (!durabilityCompatible(requested)) {
            return false;
        }
        // Deadline: offered must be <= requested (writer can provide data faster than reader requires)
        if (!deadlineCompatible(requested)) {
            return false;
        }
        // Ownership: must be the same kind
        if (ownership != requested.ownership) {
            return false;
        }
        // Liveliness: offered kind must be >= requested kind, and offered lease must be <= requested lease
        if (!livelinessCompatible(requested)) {
            return false;
        }
        return true;
    }

    private boolean durabilityCompatible(EndpointQos requested) {
        return durabilityCompatibilityRank(durability) >= durabilityCompatibilityRank(requested.durability);
    }

    private boolean deadlineCompatible(EndpointQos requested) {
        // Infinite deadline is always compatible
        if (isInfiniteDeadline(deadline) || isInfiniteDeadline(requested.deadline)) {
            return true;
        }
        // Offered deadline must be <= requested deadline
        return deadline.compareTo(requested.deadline) <= 0;
    }

    private static boolean isInfiniteDeadline(Duration d) {
        return d.getSeconds() >= Integer.MAX_VALUE;
    }

    private static int durabilityCompatibilityRank(DurabilityKind durability) {
        return switch (durability) {
            case VOLATILE -> 0;
            case TRANSIENT_LOCAL -> 1;
        };
    }

    public boolean hasFiniteDeadline() {
        return !isInfiniteDeadline(deadline);
    }

    public boolean hasFiniteLeaseDuration() {
        return !isInfiniteDuration(leaseDuration);
    }

    private boolean livelinessCompatible(EndpointQos requested) {
        // Offered liveliness kind must be >= requested (more strict is compatible)
        // Ranking: AUTOMATIC < MANUAL_BY_PARTICIPANT < MANUAL_BY_TOPIC
        if (livelinessRank(liveliness) < livelinessRank(requested.liveliness)) {
            return false;
        }
        // If both have finite lease durations, offered must be <= requested
        if (hasFiniteLeaseDuration() && requested.hasFiniteLeaseDuration()) {
            return leaseDuration.compareTo(requested.leaseDuration) <= 0;
        }
        return true;
    }

    private static int livelinessRank(LivelinessKind kind) {
        return switch (kind) {
            case AUTOMATIC -> 0;
            case MANUAL_BY_PARTICIPANT -> 1;
            case MANUAL_BY_TOPIC -> 2;
        };
    }

    private static boolean isInfiniteDuration(Duration d) {
        return d.getSeconds() >= Integer.MAX_VALUE;
    }

    public enum ReliabilityKind {
        BEST_EFFORT,
        RELIABLE
    }

    public enum DurabilityKind {
        VOLATILE,
        TRANSIENT_LOCAL
    }

    public enum HistoryKind {
        KEEP_LAST,
        KEEP_ALL
    }

    public enum OwnershipKind {
        /** Multiple writers can update the same instance */
        SHARED,
        /** Only the highest-strength writer owns each instance */
        EXCLUSIVE
    }

    public enum LivelinessKind {
        /** Service automatically maintains liveliness on any activity */
        AUTOMATIC,
        /** Application must assert liveliness at participant level */
        MANUAL_BY_PARTICIPANT,
        /** Application must assert liveliness for each DataWriter */
        MANUAL_BY_TOPIC
    }
}
