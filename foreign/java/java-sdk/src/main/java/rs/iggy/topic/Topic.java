package rs.iggy.topic;

import java.math.BigInteger;

public record Topic(
        Long id,
        BigInteger createdAt,
        String name,
        String size,
        BigInteger messageExpiry,
        CompressionAlgorithm compressionAlgorithm,
        BigInteger maxTopicSize,
        Short replicationFactor,
        BigInteger messagesCount,
        Long partitionsCount
) {
}
