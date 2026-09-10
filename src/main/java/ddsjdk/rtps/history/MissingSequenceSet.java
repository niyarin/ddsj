package ddsjdk.rtps.history;

import java.util.Set;

public record MissingSequenceSet(long baseSequenceNumber, Set<Long> missing) {
    public MissingSequenceSet {
        missing = Set.copyOf(missing);
    }
}
