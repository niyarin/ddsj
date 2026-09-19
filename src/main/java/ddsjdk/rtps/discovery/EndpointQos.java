package ddsjdk.rtps.discovery;

import java.time.Duration;

public record EndpointQos(
        ReliabilityKind reliability,
        DurabilityKind durability,
        HistoryKind history,
        int depth,
        Duration deadline) {

    /** Infinite deadline (no deadline checking) */
    public static final Duration DEADLINE_INFINITE = Duration.ofSeconds(Integer.MAX_VALUE);

    public static final EndpointQos DEFAULT = new EndpointQos(
            ReliabilityKind.BEST_EFFORT,
            DurabilityKind.VOLATILE,
            HistoryKind.KEEP_LAST,
            10,
            DEADLINE_INFINITE);

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
    }

    /** Constructor without deadline (uses infinite) */
    public EndpointQos(ReliabilityKind reliability, DurabilityKind durability, HistoryKind history, int depth) {
        this(reliability, durability, history, depth, DEADLINE_INFINITE);
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
}
