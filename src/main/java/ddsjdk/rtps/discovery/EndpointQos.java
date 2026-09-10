package ddsjdk.rtps.discovery;

public record EndpointQos(
        ReliabilityKind reliability,
        DurabilityKind durability,
        HistoryKind history,
        int depth) {
    public static final EndpointQos DEFAULT = new EndpointQos(
            ReliabilityKind.BEST_EFFORT,
            DurabilityKind.VOLATILE,
            HistoryKind.KEEP_LAST,
            10);

    public EndpointQos {
        if (depth <= 0) {
            throw new IllegalArgumentException("endpoint QoS history depth must be positive");
        }
    }

    public boolean isCompatibleWithRequested(EndpointQos requested) {
        if (requested.reliability == ReliabilityKind.RELIABLE && reliability != ReliabilityKind.RELIABLE) {
            return false;
        }
        return durabilityCompatibilityRank(durability) >= durabilityCompatibilityRank(requested.durability);
    }

    private static int durabilityCompatibilityRank(DurabilityKind durability) {
        return switch (durability) {
            case VOLATILE -> 0;
            case TRANSIENT_LOCAL -> 1;
        };
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
