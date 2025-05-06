package rs.iggy.clients.blocking;

import rs.iggy.identifier.StreamId;
import rs.iggy.identifier.TopicId;
import rs.iggy.topic.CompressionAlgorithm;
import rs.iggy.topic.Topic;
import rs.iggy.topic.TopicDetails;
import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

public interface TopicsClient {

    default Optional<TopicDetails> getTopic(Long streamId, Long topicId) {
        return getTopic(StreamId.of(streamId), TopicId.of(topicId));
    }

    Optional<TopicDetails> getTopic(StreamId streamId, TopicId topicId);

    default List<Topic> getTopics(Long streamId) {
        return getTopics(StreamId.of(streamId));
    }

    List<Topic> getTopics(StreamId streamId);

    default TopicDetails createTopic(Long streamId,
                                     Optional<Long> topicId,
                                     Long partitionsCount,
                                     CompressionAlgorithm compressionAlgorithm,
                                     BigInteger messageExpiry,
                                     BigInteger maxTopicSize,
                                     Optional<Short> replicationFactor,
                                     String name) {
        return createTopic(StreamId.of(streamId),
                topicId,
                partitionsCount,
                compressionAlgorithm,
                messageExpiry,
                maxTopicSize,
                replicationFactor,
                name);
    }

    TopicDetails createTopic(StreamId streamId,
                             Optional<Long> topicId,
                             Long partitionsCount,
                             CompressionAlgorithm compressionAlgorithm,
                             BigInteger messageExpiry,
                             BigInteger maxTopicSize,
                             Optional<Short> replicationFactor,
                             String name);

    default void updateTopic(Long streamId,
                             Long topicId,
                             CompressionAlgorithm compressionAlgorithm,
                             BigInteger messageExpiry,
                             BigInteger maxTopicSize,
                             Optional<Short> replicationFactor,
                             String name) {
        updateTopic(StreamId.of(streamId),
                TopicId.of(topicId),
                compressionAlgorithm,
                messageExpiry,
                maxTopicSize,
                replicationFactor,
                name);
    }

    void updateTopic(StreamId streamId,
                     TopicId topicId,
                     CompressionAlgorithm compressionAlgorithm,
                     BigInteger messageExpiry,
                     BigInteger maxTopicSize,
                     Optional<Short> replicationFactor,
                     String name);

    default void deleteTopic(Long streamId, Long topicId) {
        deleteTopic(StreamId.of(streamId), TopicId.of(topicId));
    }

    void deleteTopic(StreamId streamId, TopicId topicId);

}
