package ddsjdk.dds.listener;

/**
 * Listener for Subscriber status changes.
 * <p>
 * Extends DataReaderListener to support hierarchical callback propagation.
 */
public interface SubscriberListener extends DataReaderListener {

    /**
     * Called when data is available on any of the Subscriber's DataReaders.
     */
    default void onDataOnReaders() {
    }
}
