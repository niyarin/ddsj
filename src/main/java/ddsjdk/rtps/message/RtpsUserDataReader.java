package ddsjdk.rtps.message;

import ddsjdk.rtps.transport.RtpsPacket;
import ddsjdk.rtps.transport.RtpsTransport;
import ddsjdk.rtps.types.EntityId;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Consumer;

public final class RtpsUserDataReader implements Closeable {
    private final Closeable listener;
    private final FragmentAssembler fragmentAssembler = new FragmentAssembler();

    public RtpsUserDataReader(
            RtpsTransport transport,
            EntityId localReaderId,
            Consumer<UserDataSample> onSample,
            Consumer<Heartbeat> onHeartbeat) throws IOException {
        this(transport, localReaderId, onSample, onHeartbeat, ignored -> { });
    }

    public RtpsUserDataReader(
            RtpsTransport transport,
            EntityId localReaderId,
            Consumer<UserDataSample> onSample,
            Consumer<Heartbeat> onHeartbeat,
            Consumer<HeartbeatFrag> onHeartbeatFrag) throws IOException {
        this.listener = transport.listenUserData(packet ->
                handlePacket(packet, localReaderId, onSample, onHeartbeat, onHeartbeatFrag));
    }

    public RtpsUserDataReader(RtpsTransport transport, EntityId localReaderId, Consumer<UserDataSample> onSample) throws IOException {
        this(transport, localReaderId, onSample, ignored -> { });
    }

    private void handlePacket(
            RtpsPacket packet,
            EntityId localReaderId,
            Consumer<UserDataSample> onSample,
            Consumer<Heartbeat> onHeartbeat,
            Consumer<HeartbeatFrag> onHeartbeatFrag) {
        // Share packet framing and context while preserving delivery order by message kind.
        var submessages = new RtpsMessageParser(packet.data(), packet.length()).submessages();
        // Handle complete DATA samples
        RtpsUserDataParser.readUserSamples(submessages, localReaderId).forEach(onSample);

        // Handle DATA_FRAG and assemble
        for (DataFragment fragment : RtpsUserDataParser.readDataFragments(submessages, localReaderId)) {
            fragmentAssembler.addFragment(fragment).ifPresent(onSample);
        }

        // Handle heartbeats
        RtpsUserDataParser.readHeartbeats(submessages, localReaderId).forEach(onHeartbeat);
        RtpsUserDataParser.readHeartbeatFrags(submessages, localReaderId).forEach(onHeartbeatFrag);
    }

    /**
     * Returns the fragment assembler for monitoring or testing.
     */
    public FragmentAssembler fragmentAssembler() {
        return fragmentAssembler;
    }

    @Override
    public void close() throws IOException {
        listener.close();
    }
}
