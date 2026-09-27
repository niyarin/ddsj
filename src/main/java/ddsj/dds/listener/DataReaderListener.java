package ddsj.dds.listener;

import ddsj.dds.status.LivelinessChangedStatus;
import ddsj.dds.status.RequestedDeadlineMissedStatus;
import ddsj.dds.status.RequestedIncompatibleQosStatus;
import ddsj.dds.status.SampleLostStatus;
import ddsj.dds.status.SampleRejectedStatus;
import ddsj.dds.status.SubscriptionMatchedStatus;

/**
 * Listener for DataReader status changes.
 */
public interface DataReaderListener extends Listener {

    /**
     * Called when the DataReader misses a deadline.
     *
     * @param status the status
     */
    default void onRequestedDeadlineMissed(RequestedDeadlineMissedStatus status) {
    }

    /**
     * Called when the DataReader requests incompatible QoS.
     *
     * @param status the status
     */
    default void onRequestedIncompatibleQos(RequestedIncompatibleQosStatus status) {
    }

    /**
     * Called when a sample is lost.
     *
     * @param status the status
     */
    default void onSampleLost(SampleLostStatus status) {
    }

    /**
     * Called when a sample is rejected.
     *
     * @param status the status
     */
    default void onSampleRejected(SampleRejectedStatus status) {
    }

    /**
     * Called when the liveliness of a matched DataWriter changes.
     *
     * @param status the status
     */
    default void onLivelinessChanged(LivelinessChangedStatus status) {
    }

    /**
     * Called when the DataReader matches with a DataWriter.
     *
     * @param status the status
     */
    default void onSubscriptionMatched(SubscriptionMatchedStatus status) {
    }

    /**
     * Called when data is available.
     */
    default void onDataAvailable() {
    }
}
