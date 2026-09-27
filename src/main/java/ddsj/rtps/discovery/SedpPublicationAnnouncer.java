package ddsj.rtps.discovery;

import ddsj.rtps.message.AckNack;
import ddsj.rtps.transport.RtpsTransport;
import ddsj.rtps.types.GuidPrefix;

import java.io.IOException;

/** Single-endpoint SEDP API; retains its original packet and disposal behavior. */
public final class SedpPublicationAnnouncer {
    private final SedpSingleEndpointAnnouncer delegate;

    public SedpPublicationAnnouncer(
            LocalEndpoint endpoint,
            RtpsTransport transport,
            GuidPrefix guidPrefix,
            Iterable<RemoteParticipant> remoteParticipants) {
        delegate = new SedpSingleEndpointAnnouncer(endpoint, transport, guidPrefix, remoteParticipants, true);
    }

    public void announce() throws IOException {
        delegate.announce();
    }

    public void respondTo(AckNack ackNack) throws IOException {
        delegate.respondTo(ackNack);
    }

    public void disposeAndUnregister() throws IOException {
        delegate.disposeAndUnregister();
    }
}
