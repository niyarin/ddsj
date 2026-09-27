package ddsj.dds.listener;

import ddsj.dds.status.LivelinessLostStatus;
import ddsj.dds.status.OfferedDeadlineMissedStatus;
import ddsj.dds.status.OfferedIncompatibleQosStatus;
import ddsj.dds.status.PublicationMatchedStatus;

/**
 * Listener for DataWriter status changes.
 */
public interface DataWriterListener extends Listener {

    /**
     * Called when the DataWriter misses a deadline.
     *
     * @param status the status
     */
    default void onOfferedDeadlineMissed(OfferedDeadlineMissedStatus status) {
    }

    /**
     * Called when the DataWriter offers incompatible QoS.
     *
     * @param status the status
     */
    default void onOfferedIncompatibleQos(OfferedIncompatibleQosStatus status) {
    }

    /**
     * Called when the DataWriter loses liveliness.
     *
     * @param status the status
     */
    default void onLivelinessLost(LivelinessLostStatus status) {
    }

    /**
     * Called when the DataWriter matches with a DataReader.
     *
     * @param status the status
     */
    default void onPublicationMatched(PublicationMatchedStatus status) {
    }
}
