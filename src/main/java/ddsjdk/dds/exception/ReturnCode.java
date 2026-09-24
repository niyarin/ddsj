package ddsjdk.dds.exception;

/**
 * Standard return codes for DDS operations.
 * <p>
 * Operations that can partially succeed or have multiple failure modes return ReturnCode
 * instead of throwing exceptions. Lookup operations typically return Optional instead.
 */
public enum ReturnCode {
    /** Operation completed successfully */
    OK,

    /** Generic unspecified error */
    ERROR,

    /** Unsupported operation */
    UNSUPPORTED,

    /** Invalid parameter value */
    BAD_PARAMETER,

    /** Precondition not met (e.g., entity not enabled) */
    PRECONDITION_NOT_MET,

    /** Insufficient resources to complete operation */
    OUT_OF_RESOURCES,

    /** Entity is not enabled */
    NOT_ENABLED,

    /** Immutable QoS policy cannot be changed */
    IMMUTABLE_POLICY,

    /** Inconsistent QoS policies */
    INCONSISTENT_POLICY,

    /** Entity has already been deleted */
    ALREADY_DELETED,

    /** Operation timed out */
    TIMEOUT,

    /** No data available */
    NO_DATA,

    /** Illegal operation in current state */
    ILLEGAL_OPERATION;

    /**
     * Returns true if this return code indicates success.
     */
    public boolean isSuccess() {
        return this == OK;
    }

    /**
     * Returns true if this return code indicates an error.
     */
    public boolean isError() {
        return this != OK;
    }

    /**
     * Throws a DdsException if this return code indicates an error.
     *
     * @throws DdsException if this is not OK
     */
    public void checkSuccess() {
        if (this != OK) {
            throw new DdsException(this);
        }
    }
}
