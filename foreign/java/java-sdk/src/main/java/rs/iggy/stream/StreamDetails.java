package rs.iggy.stream;

import rs.iggy.topic.Topic;
import java.math.BigInteger;
import java.util.List;

public record StreamDetails(
        Long id,
        BigInteger createdAt,
        String name,
        String size,
        BigInteger messagesCount,
        Long topicsCount,
        List<Topic> topics
) {
    public StreamDetails(StreamBase base, List<Topic> topics) {
        this(base.id(), base.createdAt(), base.name(), base.size(), base.messagesCount(), base.topicsCount(), topics);
    }
}
