package ddsj.rtps.message;

import ddsj.rtps.util.Closeables;
import ddsj.rtps.transport.RtpsPacket;
import ddsj.rtps.transport.RtpsTransport;
import ddsj.rtps.types.EntityId;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

public final class AckNackListener implements Closeable {
    private final Set<EntityId> writerIds;
    private final Consumer<AckNack> onAckNack;
    private final List<Closeable> listeners;

    public AckNackListener(RtpsTransport transport, Set<EntityId> writerIds, Consumer<AckNack> onAckNack) throws IOException {
        this.writerIds = Set.copyOf(writerIds);
        this.onAckNack = onAckNack;
        this.listeners = Closeables.openAll(
                () -> transport.listenMetatraffic(this::handlePacket),
                () -> transport.listenUserData(this::handlePacket));
    }

    private void handlePacket(RtpsPacket packet) {
        AckNackParser.readAckNacks(packet.data(), packet.length()).stream()
                .filter(ackNack -> writerIds.contains(ackNack.writerId()))
                .forEach(onAckNack);
    }

    @Override
    public void close() throws IOException {
        Closeables.closeAll(listeners);
    }
}
