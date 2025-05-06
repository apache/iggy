package rs.iggy.message;

import java.math.BigInteger;
import java.util.List;

public record PolledMessages(
        Long partitionId,
        BigInteger currentOffset,
        List<PolledMessage> messages
) {
}
