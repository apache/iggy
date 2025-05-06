package rs.iggy.clients.blocking;

import rs.iggy.consumergroup.Consumer;
import rs.iggy.identifier.StreamId;
import rs.iggy.identifier.TopicId;
import rs.iggy.message.Message;
import rs.iggy.message.Partitioning;
import rs.iggy.message.PolledMessages;
import rs.iggy.message.PollingStrategy;
import java.util.List;
import java.util.Optional;

public interface MessagesClient {

    default PolledMessages pollMessages(Long streamId, Long topicId, Optional<Long> partitionId, Long consumerId, PollingStrategy strategy, Long count, boolean autoCommit) {
        return pollMessages(StreamId.of(streamId), TopicId.of(topicId), partitionId, Consumer.of(consumerId),
                strategy, count, autoCommit);
    }

    PolledMessages pollMessages(StreamId streamId, TopicId topicId, Optional<Long> partitionId, Consumer consumer, PollingStrategy strategy, Long count, boolean autoCommit);

    default void sendMessages(Long streamId, Long topicId, Partitioning partitioning, List<Message> messages) {
        sendMessages(StreamId.of(streamId), TopicId.of(topicId), partitioning, messages);
    }

    void sendMessages(StreamId streamId, TopicId topicId, Partitioning partitioning, List<Message> messages);

}
