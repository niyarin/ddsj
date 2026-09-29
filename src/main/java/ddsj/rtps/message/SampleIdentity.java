package ddsj.rtps.message;

import ddsj.rtps.types.Guid;

/**
 * DDS-RPC Sample Identity - used to correlate service requests and responses.
 *
 * @param writerGuid the GUID of the writer (for requests: client's response reader GUID)
 * @param sequenceNumber the sequence number of the sample
 */
public record SampleIdentity(Guid writerGuid, long sequenceNumber) {
}
