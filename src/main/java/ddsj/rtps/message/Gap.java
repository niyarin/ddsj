package ddsj.rtps.message;

import ddsj.rtps.types.Guid;
import java.util.Set;

/** Sequences in [start, base) and in the bitmap set are unavailable to this reader. */
public record Gap(Guid writerGuid, long start, long base, Set<Long> sequences) {
    public Gap { sequences = Set.copyOf(sequences); }
}
