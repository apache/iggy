package rs.iggy.clients.blocking.http;

import org.apache.hc.core5.http.message.BasicNameValuePair;
import rs.iggy.clients.blocking.MessagesClient;
import rs.iggy.consumergroup.Consumer;
import rs.iggy.identifier.StreamId;
import rs.iggy.identifier.TopicId;
import rs.iggy.message.Message;
import rs.iggy.message.Partitioning;
import rs.iggy.message.PolledMessages;
import rs.iggy.message.PollingStrategy;
import java.util.List;
import java.util.Optional;

class MessagesHttpClient implements MessagesClient {

    private final InternalHttpClient httpClient;

    public MessagesHttpClient(InternalHttpClient httpClient) {
        this.httpClient = httpClient;
    }

    @Override
    public PolledMessages pollMessages(StreamId streamId, TopicId topicId, Optional<Long> partitionId, Consumer consumer, PollingStrategy strategy, Long count, boolean autoCommit) {
        var request = httpClient.prepareGetRequest(path(streamId, topicId),
                new BasicNameValuePair("consumer_id", consumer.id().toString()),
                partitionId.map(id -> new BasicNameValuePair("partition_id", id.toString())).orElse(null),
                new BasicNameValuePair("strategy_kind", strategy.kind().name()),
                new BasicNameValuePair("strategy_value", strategy.value().toString()),
                new BasicNameValuePair("count", count.toString()),
                new BasicNameValuePair("auto_commit", Boolean.toString(autoCommit)));
        return httpClient.execute(request, PolledMessages.class);
    }

    @Override
    public void sendMessages(StreamId streamId, TopicId topicId, Partitioning partitioning, List<Message> messages) {
        var request = httpClient.preparePostRequest(path(streamId, topicId), new SendMessages(partitioning, messages));
        httpClient.execute(request);
    }

    private static String path(StreamId streamId, TopicId topicId) {
        return "/streams/" + streamId + "/topics/" + topicId + "/messages";
    }

    private record SendMessages(Partitioning partitioning, List<Message> messages) {
    }
}
