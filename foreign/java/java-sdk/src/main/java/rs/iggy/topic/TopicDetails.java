package rs.iggy.topic;

import rs.iggy.partition.Partition;
import java.math.BigInteger;
import java.util.List;

public record TopicDetails(
        Long id,
        BigInteger createdAt,
        String name,
        String size,
        BigInteger messageExpiry,
        CompressionAlgorithm compressionAlgorithm,
        BigInteger maxTopicSize,
        Short replicationFactor,
        BigInteger messagesCount,
        Long partitionsCount,
        List<Partition> partitions
) {
    public TopicDetails(Topic topic, List<Partition> partitions) {
        this(topic.id(),
                topic.createdAt(),
                topic.name(),
                topic.size(),
                topic.messageExpiry(),
                topic.compressionAlgorithm(),
                topic.maxTopicSize(),
                topic.replicationFactor(),
                topic.messagesCount(),
                topic.partitionsCount(),
                partitions);
    }
}
