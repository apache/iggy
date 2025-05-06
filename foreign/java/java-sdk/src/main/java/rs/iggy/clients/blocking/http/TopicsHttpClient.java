package rs.iggy.clients.blocking.http;

import com.fasterxml.jackson.core.type.TypeReference;
import rs.iggy.clients.blocking.TopicsClient;
import rs.iggy.identifier.StreamId;
import rs.iggy.identifier.TopicId;
import rs.iggy.topic.CompressionAlgorithm;
import rs.iggy.topic.Topic;
import rs.iggy.topic.TopicDetails;
import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

class TopicsHttpClient implements TopicsClient {

    private static final String STREAMS = "/streams";
    private static final String TOPICS = "/topics";
    private final InternalHttpClient httpClient;

    public TopicsHttpClient(InternalHttpClient httpClient) {
        this.httpClient = httpClient;
    }

    @Override
    public Optional<TopicDetails> getTopic(StreamId streamId, TopicId topicId) {
        var request = httpClient.prepareGetRequest(STREAMS + "/" + streamId + TOPICS + "/" + topicId);
        return httpClient.executeWithOptionalResponse(request, TopicDetails.class);
    }

    @Override
    public List<Topic> getTopics(StreamId streamId) {
        var request = httpClient.prepareGetRequest(STREAMS + "/" + streamId + TOPICS);
        return httpClient.execute(request, new TypeReference<>() {
        });
    }

    @Override
    public TopicDetails createTopic(StreamId streamId,
                                    Optional<Long> topicId,
                                    Long partitionsCount,
                                    CompressionAlgorithm compressionAlgorithm,
                                    BigInteger messageExpiry,
                                    BigInteger maxTopicSize,
                                    Optional<Short> replicationFactor,
                                    String name) {
        var request = httpClient.preparePostRequest(STREAMS + "/" + streamId + TOPICS,
                new CreateTopic(topicId,
                        partitionsCount,
                        compressionAlgorithm,
                        messageExpiry,
                        maxTopicSize,
                        replicationFactor,
                        name));
        return httpClient.execute(request, new TypeReference<>() {
        });
    }

    @Override
    public void updateTopic(StreamId streamId,
                            TopicId topicId,
                            CompressionAlgorithm compressionAlgorithm,
                            BigInteger messageExpiry,
                            BigInteger maxTopicSize,
                            Optional<Short> replicationFactor,
                            String name) {
        var request = httpClient.preparePutRequest(STREAMS + "/" + streamId + TOPICS + "/" + topicId,
                new UpdateTopic(compressionAlgorithm, messageExpiry, maxTopicSize, replicationFactor, name));
        httpClient.execute(request);
    }

    @Override
    public void deleteTopic(StreamId streamId, TopicId topicId) {
        var request = httpClient.prepareDeleteRequest(STREAMS + "/" + streamId + TOPICS + "/" + topicId);
        httpClient.execute(request);
    }

    record CreateTopic(
            Optional<Long> topicId,
            Long partitionsCount,
            CompressionAlgorithm compressionAlgorithm,
            BigInteger messageExpiry,
            BigInteger maxTopicSize,
            Optional<Short> replicationFactor,
            String name
    ) {
    }

    record UpdateTopic(
            CompressionAlgorithm compressionAlgorithm,
            BigInteger messageExpiry,
            BigInteger maxTopicSize,
            Optional<Short> replicationFactor,
            String name
    ) {
    }
}
