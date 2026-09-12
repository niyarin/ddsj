package ddsjdk.rtps.message;

import ddsjdk.rtps.types.Guid;
import ddsjdk.rtps.types.RtpsTimestamp;

import java.util.Optional;

public record DataFragment(
        Guid writerGuid,
        long sequenceNumber,
        int fragmentStartingNum,
        int fragmentsInSubmessage,
        int fragmentSize,
        int sampleSize,
        byte[] fragmentData,
        Optional<RtpsTimestamp> timestamp) {

    public DataFragment {
        fragmentData = fragmentData.clone();
    }

    public DataFragment(Guid writerGuid, long sequenceNumber, int fragmentStartingNum,
                        int fragmentsInSubmessage, int fragmentSize, int sampleSize, byte[] fragmentData) {
        this(writerGuid, sequenceNumber, fragmentStartingNum, fragmentsInSubmessage,
                fragmentSize, sampleSize, fragmentData.clone(), Optional.empty());
    }

    @Override
    public byte[] fragmentData() {
        return fragmentData.clone();
    }

    public int totalFragments() {
        return (sampleSize + fragmentSize - 1) / fragmentSize;
    }

    public boolean isLastFragment() {
        return fragmentStartingNum + fragmentsInSubmessage - 1 >= totalFragments();
    }
}
