package ddsjdk.rtps.discovery;

import ddsjdk.rtps.discovery.EndpointQos.DurabilityKind;
import ddsjdk.rtps.discovery.EndpointQos.HistoryKind;
import ddsjdk.rtps.discovery.EndpointQos.ReliabilityKind;
import ddsjdk.rtps.parameter.RtpsParameterList;
import ddsjdk.rtps.parameter.RtpsParameterListWriter;
import ddsjdk.rtps.protocol.ParameterId;
import ddsjdk.rtps.util.RtpsIo;

import java.time.Duration;
import java.util.Optional;

public final class RtpsQosParameters {
    private static final int DEFAULT_MAX_BLOCKING_SECONDS = 0;
    private static final int DEFAULT_MAX_BLOCKING_NANOSECONDS = 100_000_000;

    private RtpsQosParameters() {
    }

    public static void write(RtpsParameterListWriter writer, EndpointQos qos) {
        writer.int32Parameter(ParameterId.DURABILITY, rtpsKind(qos.durability()));
        writer.parameter(
                ParameterId.RELIABILITY,
                concat(
                        RtpsIo.intLe(rtpsKind(qos.reliability())),
                        RtpsIo.intLe(DEFAULT_MAX_BLOCKING_SECONDS),
                        RtpsIo.intLe(DEFAULT_MAX_BLOCKING_NANOSECONDS)));
        writer.parameter(
                ParameterId.HISTORY,
                concat(RtpsIo.intLe(rtpsKind(qos.history())), RtpsIo.intLe(qos.depth())));
        // Deadline: Duration as {seconds, nanoseconds fraction}
        writer.parameter(
                ParameterId.DEADLINE,
                concat(
                        RtpsIo.intLe((int) qos.deadline().getSeconds()),
                        RtpsIo.intLe(qos.deadline().getNano())));
    }

    public static EndpointQos read(RtpsParameterList parameters, boolean littleEndian) {
        return read(parameters, littleEndian, EndpointQos.DEFAULT);
    }

    public static EndpointQos read(RtpsParameterList parameters, boolean littleEndian, EndpointQos defaults) {
        ReliabilityKind reliability = parameters.first(ParameterId.RELIABILITY)
                .flatMap(bytes -> readReliabilityKind(bytes, littleEndian))
                .orElse(defaults.reliability());
        DurabilityKind durability = parameters.first(ParameterId.DURABILITY)
                .flatMap(bytes -> readDurabilityKind(bytes, littleEndian))
                .orElse(defaults.durability());
        HistoryKind history = parameters.first(ParameterId.HISTORY)
                .flatMap(bytes -> readHistoryKind(bytes, littleEndian))
                .orElse(defaults.history());
        int depth = parameters.first(ParameterId.HISTORY)
                .flatMap(bytes -> readHistoryDepth(bytes, littleEndian))
                .orElse(defaults.depth());
        Duration deadline = parameters.first(ParameterId.DEADLINE)
                .flatMap(bytes -> readDeadline(bytes, littleEndian))
                .orElse(defaults.deadline());
        return new EndpointQos(reliability, durability, history, depth, deadline);
    }

    private static int rtpsKind(ReliabilityKind reliability) {
        return switch (reliability) {
            case BEST_EFFORT -> 1;
            case RELIABLE -> 2;
        };
    }

    private static Optional<ReliabilityKind> readReliabilityKind(byte[] bytes, boolean littleEndian) {
        if (bytes.length < 4) {
            return Optional.empty();
        }
        return switch (RtpsIo.readInt(bytes, 0, littleEndian)) {
            case 1 -> Optional.of(ReliabilityKind.BEST_EFFORT);
            case 2 -> Optional.of(ReliabilityKind.RELIABLE);
            default -> Optional.empty();
        };
    }

    private static int rtpsKind(HistoryKind history) {
        return switch (history) {
            case KEEP_LAST -> 0;
            case KEEP_ALL -> 1;
        };
    }

    private static Optional<HistoryKind> readHistoryKind(byte[] bytes, boolean littleEndian) {
        if (bytes.length < 4) {
            return Optional.empty();
        }
        return switch (RtpsIo.readInt(bytes, 0, littleEndian)) {
            case 0 -> Optional.of(HistoryKind.KEEP_LAST);
            case 1 -> Optional.of(HistoryKind.KEEP_ALL);
            default -> Optional.empty();
        };
    }

    private static Optional<Integer> readHistoryDepth(byte[] bytes, boolean littleEndian) {
        if (bytes.length < 8) {
            return Optional.empty();
        }
        int depth = RtpsIo.readInt(bytes, 4, littleEndian);
        return depth > 0 ? Optional.of(depth) : Optional.empty();
    }

    private static int rtpsKind(DurabilityKind durability) {
        return switch (durability) {
            case VOLATILE -> 0;
            case TRANSIENT_LOCAL -> 1;
        };
    }

    private static Optional<DurabilityKind> readDurabilityKind(byte[] bytes, boolean littleEndian) {
        if (bytes.length < 4) {
            return Optional.empty();
        }
        return switch (RtpsIo.readInt(bytes, 0, littleEndian)) {
            case 0 -> Optional.of(DurabilityKind.VOLATILE);
            case 1 -> Optional.of(DurabilityKind.TRANSIENT_LOCAL);
            default -> Optional.empty();
        };
    }

    private static Optional<Duration> readDeadline(byte[] bytes, boolean littleEndian) {
        if (bytes.length < 8) {
            return Optional.empty();
        }
        int seconds = RtpsIo.readInt(bytes, 0, littleEndian);
        int nanos = RtpsIo.readInt(bytes, 4, littleEndian);
        // Handle infinite deadline (0x7fffffff seconds)
        if (seconds == Integer.MAX_VALUE) {
            return Optional.of(EndpointQos.DEADLINE_INFINITE);
        }
        return Optional.of(Duration.ofSeconds(seconds, nanos));
    }

    private static byte[] concat(byte[]... parts) {
        int size = 0;
        for (byte[] part : parts) {
            size += part.length;
        }
        byte[] result = new byte[size];
        int offset = 0;
        for (byte[] part : parts) {
            System.arraycopy(part, 0, result, offset, part.length);
            offset += part.length;
        }
        return result;
    }
}
