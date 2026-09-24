package ddsjdk.dds.status;

/**
 * Enumeration of all DDS status kinds.
 */
public enum StatusKind {
    /** Topic has inconsistent QoS among matched endpoints */
    INCONSISTENT_TOPIC,

    /** DataWriter missed a deadline for one or more instances */
    OFFERED_DEADLINE_MISSED,

    /** DataReader missed a deadline for one or more instances */
    REQUESTED_DEADLINE_MISSED,

    /** DataWriter offered incompatible QoS */
    OFFERED_INCOMPATIBLE_QOS,

    /** DataReader requested incompatible QoS */
    REQUESTED_INCOMPATIBLE_QOS,

    /** Sample was lost (not delivered to reader) */
    SAMPLE_LOST,

    /** Sample was rejected due to resource limits */
    SAMPLE_REJECTED,

    /** DataReader received data */
    DATA_ON_READERS,

    /** DataReader has data available */
    DATA_AVAILABLE,

    /** DataWriter lost liveliness */
    LIVELINESS_LOST,

    /** Liveliness of matched DataWriter changed */
    LIVELINESS_CHANGED,

    /** DataWriter matched with a DataReader */
    PUBLICATION_MATCHED,

    /** DataReader matched with a DataWriter */
    SUBSCRIPTION_MATCHED
}
