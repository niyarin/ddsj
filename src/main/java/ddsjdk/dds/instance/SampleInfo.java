package ddsjdk.dds.instance;

import java.time.Instant;
import java.util.Objects;

/**
 * Metadata associated with each data sample.
 * <p>
 * SampleInfo provides information about the state and origin of a sample,
 * including its read state, the instance it belongs to, and timing information.
 *
 * @param sampleState             whether the sample has been read
 * @param viewState               whether the instance is new to the reader
 * @param instanceState           lifecycle state of the instance
 * @param sourceTimestamp         timestamp when the data was written
 * @param instanceHandle          handle identifying the instance
 * @param publicationHandle       handle identifying the DataWriter
 * @param disposedGenerationCount number of times the instance was disposed before this sample
 * @param noWritersGenerationCount number of times the instance transitioned to NO_WRITERS before this sample
 * @param sampleRank              number of samples of the same instance after this one in the collection
 * @param generationRank          difference in generation counts between this sample and most recent in collection
 * @param absoluteGenerationRank  difference in generation counts between this sample and most recent overall
 * @param validData               true if the sample contains valid data (false for dispose/unregister notifications)
 */
public record SampleInfo(
        SampleState sampleState,
        ViewState viewState,
        InstanceState instanceState,
        Instant sourceTimestamp,
        InstanceHandle instanceHandle,
        InstanceHandle publicationHandle,
        long disposedGenerationCount,
        long noWritersGenerationCount,
        long sampleRank,
        long generationRank,
        long absoluteGenerationRank,
        boolean validData
) {
    /**
     * Creates a SampleInfo with validated parameters.
     */
    public SampleInfo {
        Objects.requireNonNull(sampleState, "sampleState");
        Objects.requireNonNull(viewState, "viewState");
        Objects.requireNonNull(instanceState, "instanceState");
        Objects.requireNonNull(instanceHandle, "instanceHandle");
        Objects.requireNonNull(publicationHandle, "publicationHandle");
        if (disposedGenerationCount < 0) {
            throw new IllegalArgumentException("disposedGenerationCount must not be negative");
        }
        if (noWritersGenerationCount < 0) {
            throw new IllegalArgumentException("noWritersGenerationCount must not be negative");
        }
    }

    /**
     * Creates a minimal SampleInfo for valid data with default values.
     *
     * @param sourceTimestamp the source timestamp
     * @param instanceHandle  the instance handle
     * @param publicationHandle the publication handle
     * @return a new SampleInfo
     */
    public static SampleInfo forValidData(Instant sourceTimestamp, InstanceHandle instanceHandle,
                                          InstanceHandle publicationHandle) {
        return new SampleInfo(
                SampleState.NOT_READ,
                ViewState.NEW,
                InstanceState.ALIVE,
                sourceTimestamp,
                instanceHandle,
                publicationHandle,
                0, 0, 0, 0, 0,
                true
        );
    }

    /**
     * Returns a copy of this SampleInfo with the sample state set to READ.
     *
     * @return a new SampleInfo with READ state
     */
    public SampleInfo asRead() {
        if (sampleState == SampleState.READ) {
            return this;
        }
        return new SampleInfo(
                SampleState.READ,
                viewState,
                instanceState,
                sourceTimestamp,
                instanceHandle,
                publicationHandle,
                disposedGenerationCount,
                noWritersGenerationCount,
                sampleRank,
                generationRank,
                absoluteGenerationRank,
                validData
        );
    }

    /**
     * Returns a copy of this SampleInfo with the view state set to NOT_NEW.
     *
     * @return a new SampleInfo with NOT_NEW view state
     */
    public SampleInfo asNotNew() {
        if (viewState == ViewState.NOT_NEW) {
            return this;
        }
        return new SampleInfo(
                sampleState,
                ViewState.NOT_NEW,
                instanceState,
                sourceTimestamp,
                instanceHandle,
                publicationHandle,
                disposedGenerationCount,
                noWritersGenerationCount,
                sampleRank,
                generationRank,
                absoluteGenerationRank,
                validData
        );
    }
}
