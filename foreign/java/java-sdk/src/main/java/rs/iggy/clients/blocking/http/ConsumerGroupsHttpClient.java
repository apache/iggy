package rs.iggy.clients.blocking.http;

import com.fasterxml.jackson.core.type.TypeReference;
import rs.iggy.clients.blocking.ConsumerGroupsClient;
import rs.iggy.consumergroup.ConsumerGroup;
import rs.iggy.consumergroup.ConsumerGroupDetails;
import rs.iggy.identifier.ConsumerId;
import rs.iggy.identifier.StreamId;
import rs.iggy.identifier.TopicId;
import java.util.List;
import java.util.Optional;

class ConsumerGroupsHttpClient implements ConsumerGroupsClient {

    private final InternalHttpClient httpClient;

    public ConsumerGroupsHttpClient(InternalHttpClient httpClient) {
        this.httpClient = httpClient;
    }

    @Override
    public Optional<ConsumerGroupDetails> getConsumerGroup(StreamId streamId, TopicId topicId, ConsumerId groupId) {
        var request = httpClient.prepareGetRequest(path(streamId, topicId) + "/" + groupId);
        return httpClient.executeWithOptionalResponse(request, ConsumerGroupDetails.class);
    }

    @Override
    public List<ConsumerGroup> getConsumerGroups(StreamId streamId, TopicId topicId) {
        var request = httpClient.prepareGetRequest(path(streamId, topicId));
        return httpClient.execute(request, new TypeReference<>() {
        });
    }

    @Override
    public ConsumerGroupDetails createConsumerGroup(StreamId streamId, TopicId topicId, Optional<Long> groupId, String name) {
        var request = httpClient.preparePostRequest(path(streamId, topicId),
                new CreateConsumerGroup(groupId, name));
        return httpClient.execute(request, new TypeReference<>() {
        });
    }

    @Override
    public void deleteConsumerGroup(StreamId streamId, TopicId topicId, ConsumerId groupId) {
        var request = httpClient.prepareDeleteRequest(path(streamId, topicId) + "/" + groupId);
        httpClient.execute(request);
    }

    @Override
    public void joinConsumerGroup(StreamId streamId, TopicId topicId, ConsumerId groupId) {
        throw new UnsupportedOperationException("Method not available in HTTP client");
    }

    @Override
    public void leaveConsumerGroup(StreamId streamId, TopicId topicId, ConsumerId groupId) {
        throw new UnsupportedOperationException("Method not available in HTTP client");
    }

    private static String path(StreamId streamId, TopicId topicId) {
        return "/streams/" + streamId + "/topics/" + topicId + "/consumer-groups";
    }

    private record CreateConsumerGroup(Optional<Long> groupId, String name) {
    }

}
