package ddsj.rtps.message;

import ddsj.rtps.transport.RtpsPacket;
import ddsj.rtps.transport.RtpsTransport;
import ddsj.rtps.types.EntityId;
import ddsj.rtps.types.GuidPrefix;

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
        this(transport, null, localReaderId, onSample, onHeartbeat, onHeartbeatFrag);
    }

    public RtpsUserDataReader(RtpsTransport transport, GuidPrefix localPrefix, EntityId localReaderId,
            Consumer<UserDataSample> onSample, Consumer<Heartbeat> onHeartbeat,
            Consumer<HeartbeatFrag> onHeartbeatFrag) throws IOException {
        this(transport, localPrefix, localReaderId, onSample, onHeartbeat, onHeartbeatFrag, ignored -> { });
    }

    public RtpsUserDataReader(RtpsTransport transport, GuidPrefix localPrefix, EntityId localReaderId,
            Consumer<UserDataSample> onSample, Consumer<Heartbeat> onHeartbeat,
            Consumer<HeartbeatFrag> onHeartbeatFrag, Consumer<Gap> onGap) throws IOException {
        this.listener = transport.listenUserData(packet ->
                handlePacket(packet, localPrefix, localReaderId, onSample, onHeartbeat, onHeartbeatFrag, onGap));
    }

    public RtpsUserDataReader(RtpsTransport transport, EntityId localReaderId, Consumer<UserDataSample> onSample) throws IOException {
        this(transport, localReaderId, onSample, ignored -> { });
    }

    private void handlePacket(
            RtpsPacket packet,
            GuidPrefix localPrefix,
            EntityId localReaderId,
            Consumer<UserDataSample> onSample,
            Consumer<Heartbeat> onHeartbeat,
            Consumer<HeartbeatFrag> onHeartbeatFrag,
            Consumer<Gap> onGap) {
        // Share packet framing and context while preserving delivery order by message kind.
        var submessages = new RtpsMessageParser(packet.data(), packet.length()).submessages().stream()
                .filter(s -> s.acceptsDestination(localPrefix))
                .toList();
        // Handle complete DATA samples
        RtpsUserDataParser.readUserSamples(submessages, localReaderId).forEach(onSample);

        // Handle DATA_FRAG and assemble
        for (DataFragment fragment : RtpsUserDataParser.readDataFragments(submessages, localReaderId)) {
            fragmentAssembler.addFragment(fragment).ifPresent(onSample);
        }

        // Handle heartbeats
        RtpsUserDataParser.readGaps(submessages, localReaderId).forEach(onGap);
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
