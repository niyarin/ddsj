package ddsj.dds.sample;

import ddsj.dds.instance.SampleInfo;

import java.util.Objects;

/**
 * A data sample with associated metadata.
 * <p>
 * Sample wraps a data value together with its {@link SampleInfo} metadata.
 * For dispose or unregister notifications, the data may be null and
 * {@link SampleInfo#validData()} will return false.
 *
 * @param <T> the data type
 * @param data the data value, may be null for invalid samples
 * @param info the sample metadata
 */
public record Sample<T>(T data, SampleInfo info) {
    /**
     * Creates a sample with validated parameters.
     *
     * @throws NullPointerException if info is null
     */
    public Sample {
        Objects.requireNonNull(info, "info");
    }

    /**
     * Returns true if this sample contains valid data.
     * <p>
     * Invalid samples occur for dispose or unregister notifications where
     * no data is associated with the lifecycle change.
     *
     * @return true if data is valid
     */
    public boolean hasValidData() {
        return info.validData();
    }

    /**
     * Returns the data value, or throws if the sample is invalid.
     *
     * @return the data value
     * @throws IllegalStateException if the sample does not contain valid data
     */
    public T getData() {
        if (!hasValidData()) {
            throw new IllegalStateException("Sample does not contain valid data");
        }
        return data;
    }

    /**
     * Returns the sample state from the info.
     *
     * @return the sample state
     */
    public ddsj.dds.instance.SampleState sampleState() {
        return info.sampleState();
    }

    /**
     * Returns the view state from the info.
     *
     * @return the view state
     */
    public ddsj.dds.instance.ViewState viewState() {
        return info.viewState();
    }

    /**
     * Returns the instance state from the info.
     *
     * @return the instance state
     */
    public ddsj.dds.instance.InstanceState instanceState() {
        return info.instanceState();
    }

    /**
     * Returns the instance handle from the info.
     *
     * @return the instance handle
     */
    public ddsj.dds.instance.InstanceHandle instanceHandle() {
        return info.instanceHandle();
    }
}
