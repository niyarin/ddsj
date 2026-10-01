package ddsj.dds.sample;

import ddsj.dds.instance.SampleInfo;
import ddsj.rtps.message.SampleIdentity;
import ddsj.rtps.types.Guid;

import java.util.Objects;
import java.util.Optional;

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
 * @param relatedSampleIdentity for DDS-RPC, the identity to correlate request/response
 * @param writerSequenceNumber the DATA writerSN (for DDS-RPC response correlation)
 * @param writerGuid GUID of the writer that produced this sample; empty when not supplied
 */
public record Sample<T>(T data, SampleInfo info, Optional<SampleIdentity> relatedSampleIdentity, long writerSequenceNumber, Optional<Guid> writerGuid) {
    /**
     * Creates a sample with validated parameters.
     *
     * @throws NullPointerException if info, relatedSampleIdentity, or writerGuid is null
     */
    public Sample {
        Objects.requireNonNull(info, "info");
        Objects.requireNonNull(relatedSampleIdentity, "relatedSampleIdentity");
        Objects.requireNonNull(writerGuid, "writerGuid");
    }

    /**
     * Creates a sample without a known source writer GUID.
     */
    public Sample(T data, SampleInfo info, Optional<SampleIdentity> relatedSampleIdentity,
                  long writerSequenceNumber) {
        this(data, info, relatedSampleIdentity, writerSequenceNumber, Optional.empty());
    }

    /**
     * Creates a sample without relatedSampleIdentity (for normal pub/sub).
     */
    public Sample(T data, SampleInfo info) {
        this(data, info, Optional.empty(), 0);
    }

    /**
     * Creates a sample with relatedSampleIdentity but no writerSequenceNumber.
     */
    public Sample(T data, SampleInfo info, Optional<SampleIdentity> relatedSampleIdentity) {
        this(data, info, relatedSampleIdentity, 0);
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
