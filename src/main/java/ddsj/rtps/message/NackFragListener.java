package ddsj.rtps.message;

import ddsj.rtps.util.Closeables;
import ddsj.rtps.transport.RtpsPacket;
import ddsj.rtps.transport.RtpsTransport;
import ddsj.rtps.types.EntityId;
import ddsj.rtps.types.GuidPrefix;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

public final class NackFragListener implements Closeable {
    private final Set<EntityId> writerIds;
    private final GuidPrefix localPrefix;
    private final Consumer<NackFrag> onNackFrag;
    private final List<Closeable> listeners;

    public NackFragListener(RtpsTransport transport, Set<EntityId> writerIds, Consumer<NackFrag> onNackFrag) throws IOException {
        this(transport, null, writerIds, onNackFrag);
    }

    public NackFragListener(RtpsTransport transport, GuidPrefix localPrefix, Set<EntityId> writerIds,
            Consumer<NackFrag> onNackFrag) throws IOException {
        this.localPrefix = localPrefix;
        this.writerIds = Set.copyOf(writerIds);
        this.onNackFrag = onNackFrag;
        this.listeners = Closeables.openAll(
                () -> transport.listenMetatraffic(this::handlePacket),
                () -> transport.listenUserData(this::handlePacket));
    }

    private void handlePacket(RtpsPacket packet) {
        RtpsUserDataParser.readNackFrags(packet.data(), packet.length(), localPrefix).stream()
                .filter(nackFrag -> writerIds.contains(nackFrag.writerId()))
                .forEach(onNackFrag);
    }

    @Override
    public void close() throws IOException {
        Closeables.closeAll(listeners);
    }
}
