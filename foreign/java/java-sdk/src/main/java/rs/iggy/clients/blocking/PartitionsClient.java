package rs.iggy.clients.blocking;

import rs.iggy.identifier.StreamId;
import rs.iggy.identifier.TopicId;

public interface PartitionsClient {

    default void createPartitions(Long streamId, Long topicId, Long partitionsCount) {
        createPartitions(StreamId.of(streamId), TopicId.of(topicId), partitionsCount);
    }

    void createPartitions(StreamId streamId, TopicId topicId, Long partitionsCount);

    default void deletePartitions(Long streamId, Long topicId, Long partitionsCount) {
        deletePartitions(StreamId.of(streamId), TopicId.of(topicId), partitionsCount);
    }

    void deletePartitions(StreamId streamId, TopicId topicId, Long partitionsCount);

}
