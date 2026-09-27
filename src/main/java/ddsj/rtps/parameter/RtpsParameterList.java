package ddsj.rtps.parameter;

import ddsj.rtps.protocol.ParameterId;
import ddsj.rtps.util.RtpsIo;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class RtpsParameterList {
    private static final int ENCAPSULATION_HEADER_SIZE = 4;
    private static final int PARAMETER_HEADER_SIZE = 4;

    private final Map<Integer, List<byte[]>> valuesById;

    public RtpsParameterList(Map<Integer, List<byte[]>> valuesById) {
        this.valuesById = copyValues(valuesById);
    }

    public List<byte[]> get(int id) {
        List<byte[]> values = valuesById.get(id);
        if (values == null) {
            return List.of();
        }
        return values.stream().map(byte[]::clone).toList();
    }

    public Optional<byte[]> first(int id) {
        List<byte[]> values = valuesById.get(id);
        if (values == null || values.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(values.get(0).clone());
    }

    public Optional<String> firstString(int id, boolean littleEndian) {
        return first(id).flatMap(bytes -> readParameterString(bytes, littleEndian));
    }

    public static Optional<RtpsParameterList> read(byte[] payload, boolean littleEndian) {
        if (payload.length < ENCAPSULATION_HEADER_SIZE) {
            return Optional.empty();
        }
        if (payload[0] != 0x00 || payload[1] != 0x03) {
            return Optional.empty();
        }

        Map<Integer, List<byte[]>> values = new HashMap<>();
        int position = ENCAPSULATION_HEADER_SIZE;
        while (position + PARAMETER_HEADER_SIZE <= payload.length) {
            int id = RtpsIo.readUShort(payload, position, littleEndian);
            int size = RtpsIo.readUShort(payload, position + 2, littleEndian);
            position += PARAMETER_HEADER_SIZE;
            if (id == ParameterId.SENTINEL) {
                break;
            }
            if (position + size > payload.length) {
                return Optional.empty();
            }
            values.computeIfAbsent(id, ignored -> new ArrayList<>())
                    .add(Arrays.copyOfRange(payload, position, position + size));
            position += size;
            while (position % 4 != 0) {
                position++;
            }
        }
        return Optional.of(new RtpsParameterList(values));
    }

    private static Optional<String> readParameterString(byte[] bytes, boolean littleEndian) {
        if (bytes.length < 4) {
            return Optional.empty();
        }
        int size = RtpsIo.readInt(bytes, 0, littleEndian);
        if (size <= 0 || size > bytes.length - 4) {
            return Optional.empty();
        }
        return Optional.of(new String(bytes, 4, size - 1, StandardCharsets.UTF_8));
    }

    private static Map<Integer, List<byte[]>> copyValues(Map<Integer, List<byte[]>> valuesById) {
        Map<Integer, List<byte[]>> copy = new HashMap<>();
        valuesById.forEach((id, values) -> copy.put(id, values.stream().map(byte[]::clone).toList()));
        return Map.copyOf(copy);
    }
}
