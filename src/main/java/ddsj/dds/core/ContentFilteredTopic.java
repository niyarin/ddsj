package ddsj.dds.core;

import ddsj.dds.condition.StatusCondition;
import ddsj.dds.instance.InstanceHandle;
import ddsj.dds.listener.TopicListener;
import ddsj.dds.qos.TopicQos;
import ddsj.dds.status.StatusMask;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A Topic with a content-based filter.
 * <p>
 * ContentFilteredTopic is created from an existing Topic and adds
 * a filter expression that determines which samples are delivered
 * to DataReaders.
 *
 * @param <T> the data type
 */
public final class ContentFilteredTopic<T> implements Entity<TopicListener> {
    private static final AtomicLong HANDLE_COUNTER = new AtomicLong(1);

    private final String name;
    private final Topic<T> relatedTopic;
    private final String filterExpression;
    private final AtomicReference<List<String>> expressionParameters;
    private final InstanceHandle instanceHandle;
    private final StatusCondition statusCondition;
    private final AtomicReference<TopicListener> listener = new AtomicReference<>();
    private final AtomicReference<StatusMask> listenerMask = new AtomicReference<>(StatusMask.NONE);

    /**
     * Creates a ContentFilteredTopic.
     *
     * @param name the filter topic name
     * @param relatedTopic the related Topic
     * @param filterExpression the SQL-like filter expression
     * @param expressionParameters parameters for the filter expression
     */
    public ContentFilteredTopic(String name, Topic<T> relatedTopic,
                                 String filterExpression, List<String> expressionParameters) {
        this.name = Objects.requireNonNull(name, "name");
        this.relatedTopic = Objects.requireNonNull(relatedTopic, "relatedTopic");
        this.filterExpression = Objects.requireNonNull(filterExpression, "filterExpression");
        this.expressionParameters = new AtomicReference<>(
                expressionParameters != null ? List.copyOf(expressionParameters) : List.of());
        this.instanceHandle = InstanceHandle.of(HANDLE_COUNTER.getAndIncrement());
        this.statusCondition = new StatusCondition(this);
    }

    /**
     * Returns the filter topic name.
     *
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the type name (from the related topic).
     *
     * @return the type name
     */
    public String getTypeName() {
        return relatedTopic.getTypeName();
    }

    /**
     * Returns the related Topic.
     *
     * @return the related topic
     */
    public Topic<T> getRelatedTopic() {
        return relatedTopic;
    }

    /**
     * Returns the filter expression.
     *
     * @return the filter expression
     */
    public String getFilterExpression() {
        return filterExpression;
    }

    /**
     * Returns the expression parameters.
     *
     * @return the parameters
     */
    public List<String> getExpressionParameters() {
        return expressionParameters.get();
    }

    /**
     * Sets new expression parameters.
     *
     * @param parameters the new parameters
     */
    public void setExpressionParameters(List<String> parameters) {
        this.expressionParameters.set(
                parameters != null ? List.copyOf(parameters) : List.of());
    }

    /**
     * Returns the QoS from the related topic.
     *
     * @return the QoS
     */
    public TopicQos getQos() {
        return relatedTopic.getQos();
    }

    /**
     * Returns the participant from the related topic.
     *
     * @return the participant
     */
    public DomainParticipant getParticipant() {
        return relatedTopic.getParticipant();
    }

    /**
     * Returns the TypeSupport from the related topic.
     *
     * @return the type support
     */
    public TypeSupport<T> getTypeSupport() {
        return relatedTopic.getTypeSupport();
    }

    // ========== Entity Implementation ==========

    @Override
    public InstanceHandle getInstanceHandle() {
        return instanceHandle;
    }

    @Override
    public StatusCondition getStatusCondition() {
        return statusCondition;
    }

    @Override
    public StatusMask getStatusChanges() {
        return statusCondition.getTriggerValue() ? listenerMask.get() : StatusMask.NONE;
    }

    @Override
    public void setListener(TopicListener listener, StatusMask mask) {
        this.listener.set(listener);
        this.listenerMask.set(mask != null ? mask : StatusMask.NONE);
    }

    @Override
    public TopicListener getListener() {
        return listener.get();
    }

    @Override
    public void enable() {
        // ContentFilteredTopic is always enabled
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public void close() {
        // ContentFilteredTopic doesn't own resources
    }
}
