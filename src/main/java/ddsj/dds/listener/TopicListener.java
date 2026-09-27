package ddsj.dds.listener;

import ddsj.dds.status.InconsistentTopicStatus;

/**
 * Listener for Topic status changes.
 */
public interface TopicListener extends Listener {

    /**
     * Called when the topic has inconsistent QoS with matched endpoints.
     *
     * @param status the inconsistent topic status
     */
    default void onInconsistentTopic(InconsistentTopicStatus status) {
    }
}
