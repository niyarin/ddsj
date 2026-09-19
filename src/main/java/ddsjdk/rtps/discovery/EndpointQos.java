package ddsjdk.rtps.discovery;

import java.time.Duration;

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
