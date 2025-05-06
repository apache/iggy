package rs.iggy.consumergroup;

public record ConsumerGroup(
        Long id,
        String name,
        Long partitionsCount,
        Long membersCount
) {
}
