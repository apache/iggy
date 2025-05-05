package rs.iggy.clients.blocking.tcp;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.ArrayUtils;
import rs.iggy.consumergroup.ConsumerGroup;
import rs.iggy.consumergroup.ConsumerGroupDetails;
import rs.iggy.consumergroup.ConsumerGroupMember;
import rs.iggy.consumeroffset.ConsumerOffsetInfo;
import rs.iggy.message.*;
import rs.iggy.partition.Partition;
import rs.iggy.personalaccesstoken.PersonalAccessTokenInfo;
import rs.iggy.personalaccesstoken.RawPersonalAccessToken;
import rs.iggy.stream.StreamBase;
import rs.iggy.stream.StreamDetails;
import rs.iggy.system.ClientInfo;
import rs.iggy.system.ClientInfoDetails;
import rs.iggy.system.ConsumerGroupInfo;
import rs.iggy.system.Stats;
import rs.iggy.topic.CompressionAlgorithm;
import rs.iggy.topic.Topic;
import rs.iggy.topic.TopicDetails;
import rs.iggy.user.*;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.*;

final class BytesDeserializer {

    private BytesDeserializer() {
    }

    static StreamBase readStreamBase(ByteBuf response) {
        var streamId = response.readUnsignedIntLE();
        var createdAt = readU64AsBigInteger(response);
        var topicsCount = response.readUnsignedIntLE();
        var size = readU64AsBigInteger(response);
        var messagesCount = readU64AsBigInteger(response);
        var nameLength = response.readByte();
        var name = response.readCharSequence(nameLength, StandardCharsets.UTF_8).toString();

        return new StreamBase(streamId, createdAt, name, size.toString(), messagesCount, topicsCount);
    }

    static StreamDetails readStreamDetails(ByteBuf response) {
        var streamBase = readStreamBase(response);

        List<Topic> topics = new ArrayList<>();
        if (response.isReadable()) {
            topics.add(readTopic(response));
        }

        return new StreamDetails(streamBase, topics);
    }

    public static TopicDetails readTopicDetails(ByteBuf response) {
        var topic = readTopic(response);

        List<Partition> partitions = new ArrayList<>();
        while (response.isReadable()) {
            partitions.add(readPartition(response));
        }

        return new TopicDetails(topic, partitions);
    }

    static Partition readPartition(ByteBuf response) {
        var partitionId = response.readUnsignedIntLE();
        var createdAt = readU64AsBigInteger(response);
        var segmentsCount = response.readUnsignedIntLE();
        var currentOffset = readU64AsBigInteger(response);
        var size = readU64AsBigInteger(response);
        var messagesCount = readU64AsBigInteger(response);
        return new Partition(partitionId, createdAt, segmentsCount, currentOffset, size.toString(), messagesCount);
    }

    public static Topic readTopic(ByteBuf response) {
        var topicId = response.readUnsignedIntLE();
        var createdAt = readU64AsBigInteger(response);
        var partitionsCount = response.readUnsignedIntLE();
        var messageExpiry = readU64AsBigInteger(response);
        var compressionAlgorithmCode = response.readByte();
        var maxTopicSize = readU64AsBigInteger(response);
        var replicationFactor = response.readByte();
        var size = readU64AsBigInteger(response);
        var messagesCount = readU64AsBigInteger(response);
        var nameLength = response.readByte();
        var name = response.readCharSequence(nameLength, StandardCharsets.UTF_8).toString();
        return new Topic(topicId,
                createdAt,
                name,
                size.toString(),
                messageExpiry,
                CompressionAlgorithm.fromCode(compressionAlgorithmCode),
                maxTopicSize,
                (short) replicationFactor,
                messagesCount,
                partitionsCount);
    }

    public static ConsumerGroupDetails readConsumerGroupDetails(ByteBuf response) {
        var consumerGroup = readConsumerGroup(response);

        List<ConsumerGroupMember> members = new ArrayList<>();
        while (response.isReadable()) {
            members.add(readConsumerGroupMember(response));
        }

        return new ConsumerGroupDetails(consumerGroup, members);
    }

    static ConsumerGroupMember readConsumerGroupMember(ByteBuf response) {
        var memberId = response.readUnsignedIntLE();
        var partitionsCount = response.readUnsignedIntLE();
        List<Long> partitionIds = new ArrayList<>();
        for (int i = 0; i < partitionsCount; i++) {
            partitionIds.add(response.readUnsignedIntLE());
        }
        return new ConsumerGroupMember(memberId, partitionsCount, partitionIds);
    }

    public static ConsumerGroup readConsumerGroup(ByteBuf response) {
        var groupId = response.readUnsignedIntLE();
        var partitionsCount = response.readUnsignedIntLE();
        var membersCount = response.readUnsignedIntLE();
        var nameLength = response.readByte();
        var name = response.readCharSequence(nameLength, StandardCharsets.UTF_8).toString();
        return new ConsumerGroup(groupId, name, partitionsCount, membersCount);
    }

    public static ConsumerOffsetInfo readConsumerOffsetInfo(ByteBuf response) {
        var partitionId = response.readUnsignedIntLE();
        var currentOffset = readU64AsBigInteger(response);
        var storedOffset = readU64AsBigInteger(response);
        return new ConsumerOffsetInfo(partitionId, currentOffset, storedOffset);
    }

    public static PolledMessages readPolledMessages(ByteBuf response) {
        var partitionId = response.readUnsignedIntLE();
        var currentOffset = readU64AsBigInteger(response);
        var _messagesCount = response.readUnsignedIntLE();
        var messages = new ArrayList<PolledMessage>();
        while (response.isReadable()) {
            messages.add(readPolledMessage(response));
        }
        return new PolledMessages(partitionId, currentOffset, messages);
    }

    static PolledMessage readPolledMessage(ByteBuf response) {
        var offset = readU64AsBigInteger(response);
        var stateCode = response.readByte();
        var state = MessageState.fromCode(stateCode);
        var timestamp = readU64AsBigInteger(response);
        var id = readBytesMessageId(response);
        var checksum = response.readUnsignedIntLE();
        var headersLength = response.readUnsignedIntLE();
        var headers = Optional.<Map<String, HeaderValue>>empty();
        if (headersLength > 0) {
            var headersMap = new HashMap<String, HeaderValue>();
            ByteBuf headersBytes = response.readBytes(toInt(headersLength));
            while (headersBytes.isReadable()) {
                var keyLength = headersBytes.readUnsignedIntLE();
                var key = headersBytes.readCharSequence(toInt(keyLength), StandardCharsets.UTF_8).toString();
                var kindCode = headersBytes.readByte();
                var kind = HeaderKind.fromCode(kindCode);
                var valueLength = headersBytes.readUnsignedIntLE();
                var value = headersBytes.readCharSequence(toInt(valueLength), StandardCharsets.UTF_8);
                headersMap.put(key, new HeaderValue(kind, String.valueOf(value)));
            }
            headers = Optional.of(headersMap);
        }
        var payloadLength = response.readUnsignedIntLE();
        var payload = newByteArray(payloadLength);
        response.readBytes(payload);
        return new PolledMessage(offset, state, timestamp, id, checksum, headers, payload);
    }

    static Stats readStats(ByteBuf response) {
        var processId = response.readUnsignedIntLE();
        var cpuUsage = response.readFloatLE();
        var totalCpuUsage = response.readFloatLE();
        var memoryUsage = readU64AsBigInteger(response);
        var totalMemory = readU64AsBigInteger(response);
        var availableMemory = readU64AsBigInteger(response);
        var runTime = readU64AsBigInteger(response);
        var startTime = readU64AsBigInteger(response);
        var readBytes = readU64AsBigInteger(response);
        var writtenBytes = readU64AsBigInteger(response);
        var messagesSizeBytes = readU64AsBigInteger(response);
        var streamsCount = response.readUnsignedIntLE();
        var topicsCount = response.readUnsignedIntLE();
        var partitionsCount = response.readUnsignedIntLE();
        var segmentsCount = response.readUnsignedIntLE();
        var messagesCount = readU64AsBigInteger(response);
        var clientsCount = response.readUnsignedIntLE();
        var consumerGroupsCount = response.readUnsignedIntLE();
        var hostnameLength = response.readUnsignedIntLE();
        var hostname = response.readCharSequence(toInt(hostnameLength), StandardCharsets.UTF_8).toString();
        var osNameLength = response.readUnsignedIntLE();
        var osName = response.readCharSequence(toInt(osNameLength), StandardCharsets.UTF_8).toString();
        var osVersionLength = response.readUnsignedIntLE();
        var osVersion = response.readCharSequence(toInt(osVersionLength), StandardCharsets.UTF_8).toString();
        var kernelVersionLength = response.readUnsignedIntLE();
        var kernelVersion = response.readCharSequence(toInt(kernelVersionLength), StandardCharsets.UTF_8).toString();

        return new Stats(processId,
                cpuUsage,
                totalCpuUsage,
                memoryUsage.toString(),
                totalMemory.toString(),
                availableMemory.toString(),
                runTime,
                startTime,
                readBytes.toString(),
                writtenBytes.toString(),
                messagesSizeBytes.toString(),
                streamsCount,
                topicsCount,
                partitionsCount,
                segmentsCount,
                messagesCount,
                clientsCount,
                consumerGroupsCount,
                hostname,
                osName,
                osVersion,
                kernelVersion);
    }

    static ClientInfoDetails readClientInfoDetails(ByteBuf response) {
        var clientInfo = readClientInfo(response);
        var consumerGroups = new ArrayList<ConsumerGroupInfo>();
        for (int i = 0; i < clientInfo.consumerGroupsCount(); i++) {
            consumerGroups.add(readConsumerGroupInfo(response));
        }

        return new ClientInfoDetails(clientInfo, consumerGroups);
    }

    static ClientInfo readClientInfo(ByteBuf response) {
        var clientId = response.readUnsignedIntLE();
        var userId = response.readUnsignedIntLE();
        var userIdOptional = Optional.<Long>empty();
        if (userId != 0) {
            userIdOptional = Optional.of(userId);
        }
        var transport = response.readByte();
        var transportString = "Tcp";
        if (transport == 2) {
            transportString = "Quic";
        }
        var addressLength = response.readUnsignedIntLE();
        var address = response.readCharSequence(toInt(addressLength), StandardCharsets.UTF_8).toString();
        var consumerGroupsCount = response.readUnsignedIntLE();
        return new ClientInfo(clientId, userIdOptional, address, transportString, consumerGroupsCount);
    }

    static ConsumerGroupInfo readConsumerGroupInfo(ByteBuf response) {
        var streamId = response.readUnsignedIntLE();
        var topicId = response.readUnsignedIntLE();
        var groupId = response.readUnsignedIntLE();

        return new ConsumerGroupInfo(streamId, topicId, groupId);
    }

    static UserInfoDetails readUserInfoDetails(ByteBuf response) {
        var userInfo = readUserInfo(response);

        Optional<Permissions> permissionsOptional = Optional.empty();
        if (response.readBoolean()) {
            var permissions = readPermissions(response);
            permissionsOptional = Optional.of(permissions);
        }

        return new UserInfoDetails(userInfo, permissionsOptional);
    }

    static Permissions readPermissions(ByteBuf response) {
        var _permissionsLength = response.readUnsignedIntLE();
        var globalPermissions = readGlobalPermissions(response);
        Map<Long, StreamPermissions> streamPermissionsMap = new HashMap<>();
        while (response.readBoolean()) {
            var streamId = response.readUnsignedIntLE();
            var streamPermissions = readStreamPermissions(response);
            streamPermissionsMap.put(streamId, streamPermissions);
        }
        return new Permissions(globalPermissions, streamPermissionsMap);
    }

    static StreamPermissions readStreamPermissions(ByteBuf response) {
        var manageStream = response.readBoolean();
        var readStream = response.readBoolean();
        var manageTopics = response.readBoolean();
        var readTopics = response.readBoolean();
        var pollMessages = response.readBoolean();
        var sendMessages = response.readBoolean();
        Map<Long, TopicPermissions> topicPermissionsMap = new HashMap<>();
        while (response.readBoolean()) {
            var topicId = response.readUnsignedIntLE();
            var topicPermissions = readTopicPermissions(response);
            topicPermissionsMap.put(topicId, topicPermissions);
        }
        return new StreamPermissions(manageStream,
                readStream,
                manageTopics,
                readTopics,
                pollMessages,
                sendMessages,
                topicPermissionsMap);
    }

    static TopicPermissions readTopicPermissions(ByteBuf response) {
        var manageTopic = response.readBoolean();
        var readTopic = response.readBoolean();
        var pollMessages = response.readBoolean();
        var sendMessages = response.readBoolean();
        return new TopicPermissions(manageTopic, readTopic, pollMessages, sendMessages);
    }

    static GlobalPermissions readGlobalPermissions(ByteBuf response) {
        var manageServers = response.readBoolean();
        var readServers = response.readBoolean();
        var manageUsers = response.readBoolean();
        var readUsers = response.readBoolean();
        var manageStreams = response.readBoolean();
        var readStreams = response.readBoolean();
        var manageTopics = response.readBoolean();
        var readTopics = response.readBoolean();
        var pollMessages = response.readBoolean();
        var sendMessages = response.readBoolean();
        return new GlobalPermissions(manageServers,
                readServers,
                manageUsers,
                readUsers,
                manageStreams,
                readStreams,
                manageTopics,
                readTopics,
                pollMessages,
                sendMessages);
    }

    static UserInfo readUserInfo(ByteBuf response) {
        var userId = response.readUnsignedIntLE();
        var createdAt = readU64AsBigInteger(response);
        var statusCode = response.readByte();
        var status = UserStatus.fromCode(statusCode);
        var usernameLength = response.readByte();
        var username = response.readCharSequence(usernameLength, StandardCharsets.UTF_8).toString();
        return new UserInfo(userId, createdAt, status, username);
    }

    static RawPersonalAccessToken readRawPersonalAccessToken(ByteBuf response) {
        var tokenLength = response.readByte();
        var token = response.readCharSequence(tokenLength, StandardCharsets.UTF_8).toString();
        return new RawPersonalAccessToken(token);
    }

    static PersonalAccessTokenInfo readPersonalAccessTokenInfo(ByteBuf response) {
        var nameLength = response.readByte();
        var name = response.readCharSequence(nameLength, StandardCharsets.UTF_8).toString();
        var expiry = readU64AsBigInteger(response);
        Optional<BigInteger> expiryOptional = expiry.equals(BigInteger.ZERO) ? Optional.empty() : Optional.of(expiry);
        return new PersonalAccessTokenInfo(name, expiryOptional);
    }

    private static BigInteger readU64AsBigInteger(ByteBuf buffer) {
        var bytesArray = new byte[9];
        buffer.readBytes(bytesArray, 0, 8);
        ArrayUtils.reverse(bytesArray);
        return new BigInteger(bytesArray);
    }

    private static BytesMessageId readBytesMessageId(ByteBuf buffer) {
        var bytesArray = new byte[16];
        buffer.readBytes(bytesArray);
        ArrayUtils.reverse(bytesArray);
        return new BytesMessageId(bytesArray);
    }

    private static int toInt(Long size) {
        return size.intValue();
    }

    private static byte[] newByteArray(Long size) {
        return new byte[size.intValue()];
    }

}
