package rs.iggy.clients.blocking.tcp;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import rs.iggy.clients.blocking.ConsumerGroupsClient;
import rs.iggy.consumergroup.ConsumerGroup;
import rs.iggy.consumergroup.ConsumerGroupDetails;
import rs.iggy.identifier.ConsumerId;
import rs.iggy.identifier.StreamId;
import rs.iggy.identifier.TopicId;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import static rs.iggy.clients.blocking.tcp.BytesDeserializer.readConsumerGroup;
import static rs.iggy.clients.blocking.tcp.BytesDeserializer.readConsumerGroupDetails;
import static rs.iggy.clients.blocking.tcp.BytesSerializer.nameToBytes;
import static rs.iggy.clients.blocking.tcp.BytesSerializer.toBytes;

class ConsumerGroupsTcpClient implements ConsumerGroupsClient {

    private static final int GET_CONSUMER_GROUP_CODE = 600;
    private static final int GET_CONSUMER_GROUPS_CODE = 601;
    private static final int CREATE_CONSUMER_GROUP_CODE = 602;
    private static final int DELETE_CONSUMER_GROUP_CODE = 603;
    private static final int JOIN_CONSUMER_GROUP_CODE = 604;
    private static final int LEAVE_CONSUMER_GROUP_CODE = 605;

    private final InternalTcpClient tcpClient;

    public ConsumerGroupsTcpClient(InternalTcpClient tcpClient) {
        this.tcpClient = tcpClient;
    }

    @Override
    public Optional<ConsumerGroupDetails> getConsumerGroup(StreamId streamId, TopicId topicId, ConsumerId groupId) {
        var payload = toBytes(streamId);
        payload.writeBytes(toBytes(topicId));
        payload.writeBytes(toBytes(groupId));
        var response = tcpClient.send(GET_CONSUMER_GROUP_CODE, payload);
        if (response.isReadable()) {
            return Optional.of(readConsumerGroupDetails(response));
        }
        return Optional.empty();
    }

    @Override
    public List<ConsumerGroup> getConsumerGroups(StreamId streamId, TopicId topicId) {
        var payload = toBytes(streamId);
        payload.writeBytes(toBytes(topicId));
        var response = tcpClient.send(GET_CONSUMER_GROUPS_CODE, payload);
        List<ConsumerGroup> groups = new ArrayList<>();
        while (response.isReadable()) {
            groups.add(readConsumerGroup(response));
        }
        return groups;
    }

    @Override
    public ConsumerGroupDetails createConsumerGroup(StreamId streamId, TopicId topicId, Optional<Long> groupId, String name) {
        var streamIdBytes = toBytes(streamId);
        var topicIdBytes = toBytes(topicId);
        var payload = Unpooled.buffer(5 + streamIdBytes.readableBytes() + topicIdBytes.readableBytes() + name.length());

        payload.writeBytes(streamIdBytes);
        payload.writeBytes(topicIdBytes);
        payload.writeIntLE(groupId.orElse(0L).intValue());
        payload.writeBytes(nameToBytes(name));

        ByteBuf response = tcpClient.send(CREATE_CONSUMER_GROUP_CODE, payload);
        return readConsumerGroupDetails(response);
    }

    @Override
    public void deleteConsumerGroup(StreamId streamId, TopicId topicId, ConsumerId groupId) {
        var payload = toBytes(streamId);
        payload.writeBytes(toBytes(topicId));
        payload.writeBytes(toBytes(groupId));
        tcpClient.send(DELETE_CONSUMER_GROUP_CODE, payload);
    }

    @Override
    public void joinConsumerGroup(StreamId streamId, TopicId topicId, ConsumerId groupId) {
        var payload = toBytes(streamId);
        payload.writeBytes(toBytes(topicId));
        payload.writeBytes(toBytes(groupId));

        tcpClient.send(JOIN_CONSUMER_GROUP_CODE, payload);
    }

    @Override
    public void leaveConsumerGroup(StreamId streamId, TopicId topicId, ConsumerId groupId) {
        var payload = toBytes(streamId);
        payload.writeBytes(toBytes(topicId));
        payload.writeBytes(toBytes(groupId));

        tcpClient.send(LEAVE_CONSUMER_GROUP_CODE, payload);
    }

}
