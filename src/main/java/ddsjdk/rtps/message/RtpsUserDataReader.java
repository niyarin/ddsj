package ddsjdk.rtps.message;

import ddsjdk.rtps.transport.RtpsPacket;
import ddsjdk.rtps.transport.RtpsTransport;
import ddsjdk.rtps.types.EntityId;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Consumer;

public final class RtpsUserDataReader implements Closeable {
    private final Closeable listener;

    public RtpsUserDataReader(
            RtpsTransport transport,
            EntityId localReaderId,
            Consumer<UserDataSample> onSample,
            Consumer<Heartbeat> onHeartbeat) throws IOException {
        this.listener = transport.listenUserData(packet -> handlePacket(packet, localReaderId, onSample, onHeartbeat));
    }

    public RtpsUserDataReader(RtpsTransport transport, EntityId localReaderId, Consumer<UserDataSample> onSample) throws IOException {
        this(transport, localReaderId, onSample, ignored -> { });
    }

    private void handlePacket(
            RtpsPacket packet,
            EntityId localReaderId,
            Consumer<UserDataSample> onSample,
            Consumer<Heartbeat> onHeartbeat) {
        RtpsUserDataParser.readUserSamples(packet.data(), packet.length(), localReaderId).forEach(onSample);
        RtpsUserDataParser.readHeartbeats(packet.data(), packet.length(), localReaderId).forEach(onHeartbeat);
    }

    @Override
    public void close() throws IOException {
        listener.close();
    }
}
