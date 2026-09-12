package ddsjdk.rtps.message;

import ddsjdk.rtps.types.EntityId;
import ddsjdk.rtps.types.Guid;

import java.util.Set;

public record NackFrag(
        EntityId readerId,
        Guid writerGuid,
        long writerSequenceNumber,
        Set<Integer> requestedFragmentNumbers,
        int count) {

    public NackFrag {
        requestedFragmentNumbers = Set.copyOf(requestedFragmentNumbers);
    }

    public EntityId writerId() {
        return writerGuid.entityId();
    }
}
