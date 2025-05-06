package rs.iggy.clients.blocking.tcp;

import rs.iggy.clients.blocking.ConsumerOffsetsClient;
import rs.iggy.consumergroup.Consumer;
import rs.iggy.consumeroffset.ConsumerOffsetInfo;
import rs.iggy.identifier.StreamId;
import rs.iggy.identifier.TopicId;
import java.math.BigInteger;
import java.util.Optional;
import static rs.iggy.clients.blocking.tcp.BytesDeserializer.readConsumerOffsetInfo;
import static rs.iggy.clients.blocking.tcp.BytesSerializer.toBytes;
import static rs.iggy.clients.blocking.tcp.BytesSerializer.toBytesAsU64;

class ConsumerOffsetTcpClient implements ConsumerOffsetsClient {

    private static final int GET_CONSUMER_OFFSET_CODE = 120;
    private static final int STORE_CONSUMER_OFFSET_CODE = 121;

    private final InternalTcpClient tcpClient;

    public ConsumerOffsetTcpClient(InternalTcpClient tcpClient) {
        this.tcpClient = tcpClient;
    }

    @Override
    public void storeConsumerOffset(StreamId streamId, TopicId topicId, Optional<Long> partitionId, Consumer consumer, BigInteger offset) {
        var payload = toBytes(consumer);
        payload.writeBytes(toBytes(streamId));
        payload.writeBytes(toBytes(topicId));
        payload.writeIntLE(partitionId.orElse(0L).intValue());
        payload.writeBytes(toBytesAsU64(offset));

        tcpClient.send(STORE_CONSUMER_OFFSET_CODE, payload);
    }

    @Override
    public Optional<ConsumerOffsetInfo> getConsumerOffset(StreamId streamId, TopicId topicId, Optional<Long> partitionId, Consumer consumer) {
        var payload = toBytes(consumer);
        payload.writeBytes(toBytes(streamId));
        payload.writeBytes(toBytes(topicId));
        payload.writeIntLE(partitionId.orElse(0L).intValue());

        var response = tcpClient.send(GET_CONSUMER_OFFSET_CODE, payload);
        if (response.isReadable()) {
            return Optional.of(readConsumerOffsetInfo(response));
        }
        return Optional.empty();
    }

}
