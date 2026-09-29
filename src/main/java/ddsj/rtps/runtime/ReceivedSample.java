package ddsj.rtps.runtime;

import ddsj.rtps.message.SampleIdentity;
import ddsj.rtps.types.Guid;

import java.util.Optional;

/**
 * A received sample with deserialized data and RTPS-level metadata.
 *
 * @param <T> the data type
 * @param data the deserialized data
 * @param writerGuid GUID of the writer that produced this sample
 * @param sequenceNumber sequence number of this sample
 * @param relatedSampleIdentity for DDS-RPC responses, identifies the related request
 */
public record ReceivedSample<T>(
        T data,
        Guid writerGuid,
        long sequenceNumber,
        Optional<SampleIdentity> relatedSampleIdentity) {
}
