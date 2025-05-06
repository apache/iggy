package rs.iggy.consumergroup;

import java.util.List;

public record ConsumerGroupDetails(
        Long id,
        String name,
        Long partitionsCount,
        Long membersCount,
        List<ConsumerGroupMember> members
) {
    public ConsumerGroupDetails(ConsumerGroup consumerGroup, List<ConsumerGroupMember> members) {
        this(consumerGroup.id(),
                consumerGroup.name(),
                consumerGroup.partitionsCount(),
                consumerGroup.membersCount(),
                members);
    }
}
