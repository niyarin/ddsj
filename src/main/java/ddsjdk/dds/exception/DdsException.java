package ddsjdk.dds.exception;

import java.util.Objects;

/**
 * Base exception for DDS operations.
 * <p>
 * DDS operations that throw exceptions use this class or its subclasses.
 * Many operations return {@link ReturnCode} instead for compatibility with
 * the DDS specification.
 */
public class DdsException extends RuntimeException {
    private final ReturnCode returnCode;

    /**
     * Creates a DdsException with the specified return code.
     *
     * @param returnCode the return code indicating the type of error
     */
    public DdsException(ReturnCode returnCode) {
        super(Objects.requireNonNull(returnCode, "returnCode").name());
        this.returnCode = returnCode;
    }

    /**
     * Creates a DdsException with the specified return code and message.
     *
     * @param returnCode the return code indicating the type of error
     * @param message    detailed error message
     */
    public DdsException(ReturnCode returnCode, String message) {
        super(message);
        this.returnCode = Objects.requireNonNull(returnCode, "returnCode");
    }

    /**
     * Creates a DdsException with the specified return code, message, and cause.
     *
     * @param returnCode the return code indicating the type of error
     * @param message    detailed error message
     * @param cause      the underlying cause
     */
    public DdsException(ReturnCode returnCode, String message, Throwable cause) {
        super(message, cause);
        this.returnCode = Objects.requireNonNull(returnCode, "returnCode");
    }

    /**
     * Creates a DdsException with the specified return code and cause.
     *
     * @param returnCode the return code indicating the type of error
     * @param cause      the underlying cause
     */
    public DdsException(ReturnCode returnCode, Throwable cause) {
        super(cause);
        this.returnCode = Objects.requireNonNull(returnCode, "returnCode");
    }

    /**
     * Returns the return code associated with this exception.
     *
     * @return the return code
     */
    public ReturnCode getReturnCode() {
        return returnCode;
    }
}
