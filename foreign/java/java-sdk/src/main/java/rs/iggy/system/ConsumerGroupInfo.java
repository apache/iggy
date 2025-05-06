package rs.iggy.system;

public record ConsumerGroupInfo(
        Long streamId,
        Long topicId,
        Long consumerGroupId
) {
}
