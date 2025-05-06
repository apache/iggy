package rs.iggy.clients.blocking;

import rs.iggy.consumergroup.ConsumerGroup;
import rs.iggy.consumergroup.ConsumerGroupDetails;
import rs.iggy.identifier.ConsumerId;
import rs.iggy.identifier.StreamId;
import rs.iggy.identifier.TopicId;
import java.util.List;
import java.util.Optional;

public interface ConsumerGroupsClient {

    default Optional<ConsumerGroupDetails> getConsumerGroup(Long streamId, Long topicId, Long groupId) {
        return getConsumerGroup(StreamId.of(streamId), TopicId.of(topicId), ConsumerId.of(groupId));
    }

    Optional<ConsumerGroupDetails> getConsumerGroup(StreamId streamId, TopicId topicId, ConsumerId groupId);

    default List<ConsumerGroup> getConsumerGroups(Long streamId, Long topicId) {
        return getConsumerGroups(StreamId.of(streamId), TopicId.of(topicId));
    }

    List<ConsumerGroup> getConsumerGroups(StreamId streamId, TopicId topicId);

    default ConsumerGroupDetails createConsumerGroup(Long streamId, Long topicId, Optional<Long> groupId, String name) {
        return createConsumerGroup(StreamId.of(streamId), TopicId.of(topicId), groupId, name);
    }

    ConsumerGroupDetails createConsumerGroup(StreamId streamId, TopicId topicId, Optional<Long> groupId, String name);

    default void deleteConsumerGroup(Long streamId, Long topicId, Long groupId) {
        deleteConsumerGroup(StreamId.of(streamId), TopicId.of(topicId), ConsumerId.of(groupId));
    }

    void deleteConsumerGroup(StreamId streamId, TopicId topicId, ConsumerId groupId);

    default void joinConsumerGroup(Long streamId, Long topicId, Long groupId) {
        joinConsumerGroup(StreamId.of(streamId), TopicId.of(topicId), ConsumerId.of(groupId));
    }

    void joinConsumerGroup(StreamId streamId, TopicId topicId, ConsumerId groupId);

    default void leaveConsumerGroup(Long streamId, Long topicId, Long groupId) {
        leaveConsumerGroup(StreamId.of(streamId), TopicId.of(topicId), ConsumerId.of(groupId));
    }

    void leaveConsumerGroup(StreamId streamId, TopicId topicId, ConsumerId groupId);

}
