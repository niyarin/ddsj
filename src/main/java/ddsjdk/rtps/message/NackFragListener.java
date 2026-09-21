package ddsjdk.rtps.message;

import ddsjdk.rtps.util.Closeables;
import ddsjdk.rtps.transport.RtpsPacket;
import ddsjdk.rtps.transport.RtpsTransport;
import ddsjdk.rtps.types.EntityId;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

public final class NackFragListener implements Closeable {
    private final Set<EntityId> writerIds;
    private final Consumer<NackFrag> onNackFrag;
    private final List<Closeable> listeners;

    public NackFragListener(RtpsTransport transport, Set<EntityId> writerIds, Consumer<NackFrag> onNackFrag) throws IOException {
        this.writerIds = Set.copyOf(writerIds);
        this.onNackFrag = onNackFrag;
        this.listeners = Closeables.openAll(
                () -> transport.listenMetatraffic(this::handlePacket),
                () -> transport.listenUserData(this::handlePacket));
    }

    private void handlePacket(RtpsPacket packet) {
        RtpsUserDataParser.readNackFrags(packet.data(), packet.length()).stream()
                .filter(nackFrag -> writerIds.contains(nackFrag.writerId()))
                .forEach(onNackFrag);
    }

    @Override
    public void close() throws IOException {
        Closeables.closeAll(listeners);
    }
}
