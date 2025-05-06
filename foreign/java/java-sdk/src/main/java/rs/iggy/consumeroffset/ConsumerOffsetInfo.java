package rs.iggy.consumeroffset;

import java.math.BigInteger;

public record ConsumerOffsetInfo(
        Long partitionId,
        BigInteger currentOffset,
        BigInteger storedOffset
) {
}
