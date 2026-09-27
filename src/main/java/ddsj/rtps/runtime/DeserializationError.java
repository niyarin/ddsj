package ddsj.rtps.runtime;

import ddsj.rtps.types.Guid;
import java.util.Objects;

/** A failed decoding attempt. The sample is not queued or marked as received, so it can be retried. */
public record DeserializationError(Guid writerGuid, long sequenceNumber, RuntimeException cause) {
    public DeserializationError {
        Objects.requireNonNull(writerGuid, "writerGuid");
        Objects.requireNonNull(cause, "cause");
    }
}
