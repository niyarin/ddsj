package ddsj.rtps.runtime;

import java.util.Objects;
import java.util.function.Consumer;

/** Immutable collection of reader notification callbacks. Monitoring is enabled by endpoint QoS,
 * independently of these listeners. Omitted callbacks disable notifications only.
 * Callbacks execute on receiving or monitoring threads and should not block.
 */
public final class ReaderListeners {
    public static final ReaderListeners DEFAULT = builder().build();

    private final Consumer<DeadlineMissedStatus> onDeadlineMissed;
    private final Consumer<LivelinessChangedStatus> onLivelinessChanged;
    private final Consumer<DeserializationError> onDeserializationError;

    private ReaderListeners(Builder builder) {
        onDeadlineMissed = builder.onDeadlineMissed;
        onLivelinessChanged = builder.onLivelinessChanged;
        onDeserializationError = builder.onDeserializationError;
    }

    public static Builder builder() { return new Builder(); }

    Consumer<DeadlineMissedStatus> deadlineListener() { return onDeadlineMissed; }
    Consumer<LivelinessChangedStatus> livelinessListener() { return onLivelinessChanged; }
    Consumer<DeserializationError> deserializationListener() { return onDeserializationError; }

    static ReaderListeners legacy(Consumer<DeadlineMissedStatus> deadline,
            Consumer<LivelinessChangedStatus> liveliness) {
        Builder builder = builder();
        if (deadline != null) builder.onDeadlineMissed(deadline);
        if (liveliness != null) builder.onLivelinessChanged(liveliness);
        return builder.build();
    }

    /** Mutable builder; each build creates an independent listener snapshot. */
    public static final class Builder {
        private Consumer<DeadlineMissedStatus> onDeadlineMissed;
        private Consumer<LivelinessChangedStatus> onLivelinessChanged;
        private Consumer<DeserializationError> onDeserializationError = ignored -> { };

        private Builder() {}

        /** Sets the deadline notification callback. Omit to disable notifications. */
        public Builder onDeadlineMissed(Consumer<DeadlineMissedStatus> listener) {
            onDeadlineMissed = Objects.requireNonNull(listener, "listener");
            return this;
        }

        /** Sets the liveliness notification callback. Omit to disable notifications. */
        public Builder onLivelinessChanged(Consumer<LivelinessChangedStatus> listener) {
            onLivelinessChanged = Objects.requireNonNull(listener, "listener");
            return this;
        }

        /** Sets the initial decoding failure callback; the reader can replace it later. */
        public Builder onDeserializationError(Consumer<DeserializationError> listener) {
            onDeserializationError = Objects.requireNonNull(listener, "listener");
            return this;
        }

        public ReaderListeners build() { return new ReaderListeners(this); }
    }
}
