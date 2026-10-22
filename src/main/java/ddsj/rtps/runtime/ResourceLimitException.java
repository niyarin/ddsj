package ddsj.rtps.runtime;

import java.io.IOException;

/** A write was rejected without advancing its sequence or evicting retained history. */
public final class ResourceLimitException extends IOException {
    public ResourceLimitException(String message) { super(message); }
}
