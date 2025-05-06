package rs.iggy.clients.blocking.http;

import org.apache.hc.core5.http.message.BasicNameValuePair;
import rs.iggy.clients.blocking.ConsumerOffsetsClient;
import rs.iggy.consumergroup.Consumer;
import rs.iggy.consumeroffset.ConsumerOffsetInfo;
import rs.iggy.identifier.StreamId;
import rs.iggy.identifier.TopicId;
import java.math.BigInteger;
import java.util.Optional;

class ConsumerOffsetsHttpClient implements ConsumerOffsetsClient {

    private static final String DEFAULT_PARTITION_ID = "1";
    private final InternalHttpClient httpClient;

    public ConsumerOffsetsHttpClient(InternalHttpClient httpClient) {
        this.httpClient = httpClient;
    }

    @Override
    public void storeConsumerOffset(StreamId streamId, TopicId topicId, Optional<Long> partitionId, Consumer consumer, BigInteger offset) {
        var request = httpClient.preparePutRequest(path(streamId, topicId),
                new StoreConsumerOffset(consumer.id().toString(), partitionId, offset));
        httpClient.execute(request);
    }

    @Override
    public Optional<ConsumerOffsetInfo> getConsumerOffset(StreamId streamId, TopicId topicId, Optional<Long> partitionId, Consumer consumer) {
        var request = httpClient.prepareGetRequest(path(streamId, topicId),
                new BasicNameValuePair("consumer_id", consumer.id().toString()),
                new BasicNameValuePair("partition_id", partitionId.map(Object::toString).orElse(DEFAULT_PARTITION_ID)));
        return httpClient.executeWithOptionalResponse(request, ConsumerOffsetInfo.class);
    }

    private static String path(StreamId streamId, TopicId topicId) {
        return "/streams/" + streamId + "/topics/" + topicId + "/consumer-offsets";
    }

    private record StoreConsumerOffset(String consumerId, Optional<Long> partitionId, BigInteger offset) {
    }
}
