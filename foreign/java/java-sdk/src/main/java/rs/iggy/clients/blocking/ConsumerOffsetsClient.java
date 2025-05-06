package rs.iggy.clients.blocking;

import rs.iggy.consumergroup.Consumer;
import rs.iggy.consumeroffset.ConsumerOffsetInfo;
import rs.iggy.identifier.StreamId;
import rs.iggy.identifier.TopicId;
import java.math.BigInteger;
import java.util.Optional;

public interface ConsumerOffsetsClient {

    default void storeConsumerOffset(Long streamId, Long topicId, Optional<Long> partitionId, Long consumerId, BigInteger offset) {
        storeConsumerOffset(StreamId.of(streamId), TopicId.of(topicId), partitionId, Consumer.of(consumerId), offset);
    }

    void storeConsumerOffset(StreamId streamId, TopicId topicId, Optional<Long> partitionId, Consumer consumer, BigInteger offset);

    default Optional<ConsumerOffsetInfo> getConsumerOffset(Long streamId, Long topicId, Optional<Long> partitionId, Long consumerId) {
        return getConsumerOffset(StreamId.of(streamId), TopicId.of(topicId), partitionId, Consumer.of(consumerId));
    }

    Optional<ConsumerOffsetInfo> getConsumerOffset(StreamId streamId, TopicId topicId, Optional<Long> partitionId, Consumer consumer);

}
