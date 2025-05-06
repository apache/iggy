package rs.iggy.consumergroup;

import java.util.List;

public record ConsumerGroupMember(
        Long id,
        Long partitionsCount,
        List<Long> partitions
) {
}
