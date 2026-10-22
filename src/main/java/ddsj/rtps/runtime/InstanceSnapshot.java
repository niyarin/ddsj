package ddsj.rtps.runtime;

import ddsj.rtps.types.Guid;
import java.util.Set;
import java.util.HashSet;

/** State maintained independently of unread history: 0=alive, 1=disposed, 2=no writers. */
public record InstanceSnapshot(Set<Guid> writers, int state, long disposedGenerations, long noWritersGenerations) {
    public static final InstanceSnapshot INITIAL = new InstanceSnapshot(Set.of(), 0, 0, 0);
    public InstanceSnapshot { writers = Set.copyOf(writers); }
    public InstanceSnapshot apply(Guid writer, int statusInfo) {
        var next = new HashSet<>(writers);
        if ((statusInfo & 2) != 0) next.remove(writer);
        else next.add(writer);
        if (statusInfo == 0) return new InstanceSnapshot(next, 0,
                disposedGenerations + (state == 1 ? 1 : 0), noWritersGenerations + (state == 2 ? 1 : 0));
        int nextState = (statusInfo & 1) != 0 ? 1 : (state == 1 ? 1 : (next.isEmpty() ? 2 : 0));
        return new InstanceSnapshot(next, nextState, disposedGenerations, noWritersGenerations);
    }
}
