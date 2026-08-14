/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iggy.serde;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.iggy.cluster.ClusterMetadata;
import org.apache.iggy.cluster.ClusterNode;
import org.apache.iggy.cluster.ClusterNodeRole;
import org.apache.iggy.cluster.ClusterNodeStatus;
import org.apache.iggy.cluster.TransportEndpoints;
import org.apache.iggy.consumergroup.ConsumerGroup;
import org.apache.iggy.consumergroup.ConsumerGroupAssignment;
import org.apache.iggy.consumergroup.ConsumerGroupDetails;
import org.apache.iggy.consumergroup.ConsumerGroupMember;
import org.apache.iggy.consumeroffset.ConsumerOffsetInfo;
import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.apache.iggy.exception.IggyMalformedResponseException;
import org.apache.iggy.message.BytesMessageId;
import org.apache.iggy.message.HeaderKey;
import org.apache.iggy.message.HeaderKind;
import org.apache.iggy.message.HeaderValue;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.MessageHeader;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.SendConfirmation;
import org.apache.iggy.message.SendMessagesResponse;
import org.apache.iggy.partition.Partition;
import org.apache.iggy.personalaccesstoken.PersonalAccessTokenInfo;
import org.apache.iggy.personalaccesstoken.RawPersonalAccessToken;
import org.apache.iggy.stream.StreamBase;
import org.apache.iggy.stream.StreamDetails;
import org.apache.iggy.system.CacheMetrics;
import org.apache.iggy.system.CacheMetricsKey;
import org.apache.iggy.system.ClientInfo;
import org.apache.iggy.system.ClientInfoDetails;
import org.apache.iggy.system.ConsumerGroupInfo;
import org.apache.iggy.system.Stats;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.apache.iggy.topic.Topic;
import org.apache.iggy.topic.TopicDetails;
import org.apache.iggy.user.GlobalPermissions;
import org.apache.iggy.user.Permissions;
import org.apache.iggy.user.StreamPermissions;
import org.apache.iggy.user.TopicPermissions;
import org.apache.iggy.user.UserInfo;
import org.apache.iggy.user.UserInfoDetails;
import org.apache.iggy.user.UserStatus;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Unified deserializer for both blocking and async clients.
 * Provides deserialization of ByteBuf to domain objects according to Iggy wire protocol.
 */
public final class BytesDeserializer {

    private static final int CONSUMER_GROUP_ASSIGNMENT_ENTRY_BYTES = Integer.BYTES;
    private static final int SEND_CONFIRMATION_BYTES = 3 * Integer.BYTES + Long.BYTES;
    private static final int MIN_CLUSTER_NODE_BYTES = 18;
    // 50-byte fixed part + a one-character name (the server rejects an empty
    // one) + two u32 options-block length prefixes.
    private static final int MIN_TOPIC_BYTES = 59;

    private BytesDeserializer() {}

    public static StreamBase readStreamBase(ByteBuf response) {
        var streamId = response.readUnsignedIntLE();
        var createdAt = readU64AsBigInteger(response);
        var topicsCount = response.readUnsignedIntLE();
        var size = readU64AsBigInteger(response);
        var messagesCount = readU64AsBigInteger(response);
        var nameLength = response.readByte();
        var name = response.readCharSequence(nameLength, StandardCharsets.UTF_8).toString();
        var options = readOptionsBlock(response, "stream options");

        return new StreamBase(streamId, createdAt, name, size.toString(), messagesCount, topicsCount, options);
    }

    public static StreamDetails readStreamDetails(ByteBuf response) {
        var streamBase = readStreamBase(response);

        // Count-driven: a topic element carries variable-length options
        // blocks, so "consume until the buffer ends" no longer delimits it.
        int topicsCount = validatedCollectionSize(
                streamBase.topicsCount(), response.readableBytes(), MIN_TOPIC_BYTES, "Stream topics count");
        List<Topic> topics = new ArrayList<>(topicsCount);
        for (int i = 0; i < topicsCount; i++) {
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

    public static Partition readPartition(ByteBuf response) {
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
        var size = readU64AsBigInteger(response);
        var messagesCount = readU64AsBigInteger(response);
        var nameLength = response.readByte();
        var name = response.readCharSequence(nameLength, StandardCharsets.UTF_8).toString();
        var options = readOptionsBlock(response, "topic explicit options");
        var derivedOptions = readOptionsBlock(response, "topic derived options");
        return new Topic(
                topicId,
                createdAt,
                name,
                size.toString(),
                messageExpiry,
                CompressionAlgorithm.fromCode(compressionAlgorithmCode),
                maxTopicSize,
                messagesCount,
                partitionsCount,
                options,
                derivedOptions);
    }

    public static ConsumerGroupDetails readConsumerGroupDetails(ByteBuf response) {
        var consumerGroup = readConsumerGroup(response);

        List<ConsumerGroupMember> members = new ArrayList<>();
        while (response.isReadable()) {
            members.add(readConsumerGroupMember(response));
        }

        return new ConsumerGroupDetails(consumerGroup, members);
    }

    public static ConsumerGroupMember readConsumerGroupMember(ByteBuf response) {
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

    public static ConsumerGroupAssignment readConsumerGroupAssignment(ByteBuf response) {
        // The generation is a monotonic rebalance counter compared only for
        // equality, so reading the u64 as a signed long is safe.
        var generation = response.readLongLE();
        var partitionsCount = response.readUnsignedIntLE();
        int capacity = validatedCollectionSize(
                partitionsCount,
                response.readableBytes(),
                CONSUMER_GROUP_ASSIGNMENT_ENTRY_BYTES,
                "Consumer group partitions count");
        List<Long> partitions = new ArrayList<>(capacity);
        for (long i = 0; i < partitionsCount; i++) {
            partitions.add(response.readUnsignedIntLE());
        }
        return new ConsumerGroupAssignment(generation, partitions);
    }

    public static ConsumerOffsetInfo readConsumerOffsetInfo(ByteBuf response) {
        var partitionId = response.readUnsignedIntLE();
        var currentOffset = readU64AsBigInteger(response);
        var storedOffset = readU64AsBigInteger(response);
        return new ConsumerOffsetInfo(partitionId, currentOffset, storedOffset);
    }

    public static SendMessagesResponse readSendMessagesResponse(ByteBuf response) {
        if (!response.isReadable()) {
            return SendMessagesResponse.empty();
        }
        var confirmationsCount = response.readUnsignedIntLE();
        int capacity = validatedCollectionSize(
                confirmationsCount, response.readableBytes(), SEND_CONFIRMATION_BYTES, "Send confirmations count");
        var confirmations = new ArrayList<SendConfirmation>(capacity);
        for (long i = 0; i < confirmationsCount; i++) {
            var streamId = response.readUnsignedIntLE();
            var topicId = response.readUnsignedIntLE();
            var partitionId = response.readUnsignedIntLE();
            var baseOffset = readU64AsBigInteger(response);
            confirmations.add(new SendConfirmation(streamId, topicId, partitionId, baseOffset));
        }
        if (response.isReadable()) {
            throw new IggyMalformedResponseException(
                    "send messages response has " + response.readableBytes() + " trailing bytes");
        }
        return new SendMessagesResponse(confirmations);
    }

    public static PolledMessages readPolledMessages(ByteBuf response) {
        var partitionId = response.readUnsignedIntLE();
        var currentOffset = readU64AsBigInteger(response);
        var messagesCount = response.readUnsignedIntLE();
        var messages = new ArrayList<Message>();
        while (response.isReadable()) {
            messages.add(readPolledMessage(response));
        }
        return new PolledMessages(partitionId, currentOffset, messagesCount, messages);
    }

    public static Message readPolledMessage(ByteBuf response) {
        var checksum = readU64AsBigInteger(response);
        var id = readBytesMessageId(response);
        var offset = readU64AsBigInteger(response);
        var timestamp = readU64AsBigInteger(response);
        var originTimestamp = readU64AsBigInteger(response);
        var userHeadersLength = response.readUnsignedIntLE();
        var payloadLength = response.readUnsignedIntLE();
        var reserved = readU64AsBigInteger(response);
        var header = new MessageHeader(
                checksum, id, offset, timestamp, originTimestamp, userHeadersLength, payloadLength, reserved);
        var payload = newByteArray(payloadLength);
        response.readBytes(payload);
        Map<HeaderKey, HeaderValue> userHeaders = new HashMap<>();
        if (userHeadersLength > 0) {
            ByteBuf userHeadersBuffer = response.readSlice(toInt(userHeadersLength));
            Map<HeaderKey, HeaderValue> headers = new HashMap<>();
            while (userHeadersBuffer.isReadable()) {
                var userHeaderKeyKindCode = userHeadersBuffer.readUnsignedByte();
                var userHeaderKeyLength = userHeadersBuffer.readUnsignedIntLE();
                byte[] userHeaderKeyBytes = new byte[toInt(userHeaderKeyLength)];
                userHeadersBuffer.readBytes(userHeaderKeyBytes);
                var userHeaderKey = new HeaderKey(HeaderKind.fromCode(userHeaderKeyKindCode), userHeaderKeyBytes);

                var userHeaderValueKindCode = userHeadersBuffer.readUnsignedByte();
                var userHeaderValueLength = userHeadersBuffer.readUnsignedIntLE();
                byte[] userHeaderValueBytes = new byte[toInt(userHeaderValueLength)];
                userHeadersBuffer.readBytes(userHeaderValueBytes);
                headers.put(
                        userHeaderKey,
                        new HeaderValue(HeaderKind.fromCode(userHeaderValueKindCode), userHeaderValueBytes));
            }
            userHeaders = headers;
        }

        return new Message(header, payload, userHeaders);
    }

    public static Stats readStats(ByteBuf response) {
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
        var hostname = response.readCharSequence(toInt(hostnameLength), StandardCharsets.UTF_8)
                .toString();
        var osNameLength = response.readUnsignedIntLE();
        var osName = response.readCharSequence(toInt(osNameLength), StandardCharsets.UTF_8)
                .toString();
        var osVersionLength = response.readUnsignedIntLE();
        var osVersion = response.readCharSequence(toInt(osVersionLength), StandardCharsets.UTF_8)
                .toString();
        var kernelVersionLength = response.readUnsignedIntLE();
        var kernelVersion = response.readCharSequence(toInt(kernelVersionLength), StandardCharsets.UTF_8)
                .toString();

        var iggyServerVersionLength = response.readUnsignedIntLE();
        var iggyServerVersion = response.readCharSequence(toInt(iggyServerVersionLength), StandardCharsets.UTF_8)
                .toString();

        var semverValue = response.readUnsignedIntLE();
        var iggyServerSemver = semverValue != 0 ? Optional.of(semverValue) : Optional.<Long>empty();

        var metricsCount = response.readUnsignedIntLE();
        Map<CacheMetricsKey, CacheMetrics> cacheMetrics = new HashMap<>();
        for (int i = 0; i < metricsCount; i++) {
            var streamId = response.readUnsignedIntLE();
            var topicId = response.readUnsignedIntLE();
            var partitionId = response.readUnsignedIntLE();
            var hits = readU64AsBigInteger(response);
            var misses = readU64AsBigInteger(response);
            var hitRatio = response.readFloatLE();
            var key = new CacheMetricsKey(streamId, topicId, partitionId);
            cacheMetrics.put(key, new CacheMetrics(hits, misses, hitRatio));
        }

        var threadsCount = response.readUnsignedIntLE();
        var freeDiskSpace = readU64AsBigInteger(response);
        var totalDiskSpace = readU64AsBigInteger(response);

        return new Stats(
                processId,
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
                kernelVersion,
                iggyServerVersion,
                iggyServerSemver,
                cacheMetrics,
                threadsCount,
                freeDiskSpace.toString(),
                totalDiskSpace.toString());
    }

    public static ClientInfoDetails readClientInfoDetails(ByteBuf response) {
        var clientInfo = readClientInfo(response);
        var consumerGroups = new ArrayList<ConsumerGroupInfo>();
        for (int i = 0; i < clientInfo.consumerGroupsCount(); i++) {
            consumerGroups.add(readConsumerGroupInfo(response));
        }

        return new ClientInfoDetails(clientInfo, consumerGroups);
    }

    public static ClientInfo readClientInfo(ByteBuf response) {
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
        var address = response.readCharSequence(toInt(addressLength), StandardCharsets.UTF_8)
                .toString();
        var consumerGroupsCount = response.readUnsignedIntLE();
        return new ClientInfo(clientId, userIdOptional, address, transportString, consumerGroupsCount);
    }

    public static ConsumerGroupInfo readConsumerGroupInfo(ByteBuf response) {
        var streamId = response.readUnsignedIntLE();
        var topicId = response.readUnsignedIntLE();
        var groupId = response.readUnsignedIntLE();

        return new ConsumerGroupInfo(streamId, topicId, groupId);
    }

    public static ClusterMetadata readClusterMetadata(ByteBuf response) {
        var name = readU32PrefixedString(response, "cluster name");
        var nodesCount = response.readUnsignedIntLE();
        if (nodesCount > response.readableBytes() / MIN_CLUSTER_NODE_BYTES) {
            throw new IggyMalformedResponseException("Cluster nodes count " + nodesCount
                    + " exceeds remaining payload of " + response.readableBytes() + " bytes");
        }
        List<ClusterNode> nodes = new ArrayList<>(toInt(nodesCount));
        for (int i = 0; i < nodesCount; i++) {
            nodes.add(readClusterNode(response));
        }
        return new ClusterMetadata(name, nodes);
    }

    public static ClusterNode readClusterNode(ByteBuf response) {
        var name = readU32PrefixedString(response, "cluster node name");
        var ip = readU32PrefixedString(response, "cluster node ip");
        var tcpPort = response.readUnsignedShortLE();
        var quicPort = response.readUnsignedShortLE();
        var httpPort = response.readUnsignedShortLE();
        var websocketPort = response.readUnsignedShortLE();
        var role = ClusterNodeRole.fromCode(response.readUnsignedByte());
        var status = ClusterNodeStatus.fromCode(response.readUnsignedByte());
        return new ClusterNode(
                name, ip, new TransportEndpoints(tcpPort, quicPort, httpPort, websocketPort), role, status);
    }

    public static UserInfoDetails readUserInfoDetails(ByteBuf response) {
        var userInfo = readUserInfo(response);

        Optional<Permissions> permissionsOptional = Optional.empty();
        if (response.readBoolean()) {
            var permissions = readPermissions(response);
            permissionsOptional = Optional.of(permissions);
        } else {
            // No-permissions marker is u32_le(0): the flag byte above was its
            // first byte, skip the remaining three zero bytes.
            response.skipBytes(3);
        }

        return new UserInfoDetails(userInfo, permissionsOptional);
    }

    public static Permissions readPermissions(ByteBuf response) {
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

    public static StreamPermissions readStreamPermissions(ByteBuf response) {
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
        return new StreamPermissions(
                manageStream, readStream, manageTopics, readTopics, pollMessages, sendMessages, topicPermissionsMap);
    }

    public static TopicPermissions readTopicPermissions(ByteBuf response) {
        var manageTopic = response.readBoolean();
        var readTopic = response.readBoolean();
        var pollMessages = response.readBoolean();
        var sendMessages = response.readBoolean();
        return new TopicPermissions(manageTopic, readTopic, pollMessages, sendMessages);
    }

    public static GlobalPermissions readGlobalPermissions(ByteBuf response) {
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
        return new GlobalPermissions(
                manageServers,
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

    public static UserInfo readUserInfo(ByteBuf response) {
        var userId = response.readUnsignedIntLE();
        var createdAt = readU64AsBigInteger(response);
        var statusCode = response.readByte();
        var status = UserStatus.fromCode(statusCode);
        var usernameLength = response.readByte();
        var username = response.readCharSequence(usernameLength, StandardCharsets.UTF_8)
                .toString();
        // Validated and dropped: users have no catalog keys yet, so the server
        // refuses every one and the block is always empty.
        readOptionsBlock(response, "user options");
        return new UserInfo(userId, createdAt, status, username);
    }

    public static RawPersonalAccessToken readRawPersonalAccessToken(ByteBuf response) {
        var tokenLength = response.readByte();
        var token =
                response.readCharSequence(tokenLength, StandardCharsets.UTF_8).toString();
        return new RawPersonalAccessToken(token);
    }

    public static PersonalAccessTokenInfo readPersonalAccessTokenInfo(ByteBuf response) {
        var nameLength = response.readByte();
        var name = response.readCharSequence(nameLength, StandardCharsets.UTF_8).toString();
        var expiry = readU64AsBigInteger(response);
        Optional<BigInteger> expiryOptional = expiry.equals(BigInteger.ZERO) ? Optional.empty() : Optional.of(expiry);
        return new PersonalAccessTokenInfo(name, expiryOptional);
    }

    /**
     * Reads a {@code u32}-length-prefixed options block into its entries.
     *
     * <p>Keys are always UTF-8 strings; values keep the kind the server sent, so a kind this
     * build has no name for is dropped rather than failing the whole response - the wire
     * contract forwards unknown value kinds so a mixed-version cluster can round-trip them.
     */
    private static Map<String, HeaderValue> readOptionsBlock(ByteBuf buffer, String field) {
        if (buffer.readableBytes() < Integer.BYTES) {
            throw new IggyMalformedResponseException("Missing length prefix for " + field);
        }
        var optionsLength = buffer.readUnsignedIntLE();
        if (optionsLength > buffer.readableBytes()) {
            throw new IggyMalformedResponseException("Length " + optionsLength + " for " + field
                    + " exceeds remaining payload of " + buffer.readableBytes() + " bytes");
        }
        if (optionsLength == 0) {
            return Map.of();
        }
        ByteBuf options = buffer.readSlice(toInt(optionsLength));
        Map<String, HeaderValue> entries = new LinkedHashMap<>();
        while (options.isReadable()) {
            if (options.readableBytes() < 1 + Integer.BYTES) {
                throw new IggyMalformedResponseException("Truncated key header in " + field);
            }
            options.readUnsignedByte();
            var keyLength = options.readUnsignedIntLE();
            if (keyLength > options.readableBytes()) {
                throw new IggyMalformedResponseException("Truncated key in " + field);
            }
            var key = options.readCharSequence(toInt(keyLength), StandardCharsets.UTF_8)
                    .toString();

            if (options.readableBytes() < 1 + Integer.BYTES) {
                throw new IggyMalformedResponseException("Truncated value header for '" + key + "' in " + field);
            }
            var valueKindCode = options.readUnsignedByte();
            var valueLength = options.readUnsignedIntLE();
            if (valueLength > options.readableBytes()) {
                throw new IggyMalformedResponseException("Truncated value for '" + key + "' in " + field);
            }
            byte[] value = newByteArray(valueLength);
            options.readBytes(value);
            try {
                entries.put(key, new HeaderValue(HeaderKind.fromCode(valueKindCode), value));
            } catch (IggyInvalidArgumentException unknownKind) {
                // A newer peer's value kind: keep every other entry readable.
            }
        }
        return entries;
    }

    private static String readU32PrefixedString(ByteBuf buffer, String field) {
        if (buffer.readableBytes() < Integer.BYTES) {
            throw new IggyMalformedResponseException("Missing length prefix for " + field);
        }
        var length = buffer.readUnsignedIntLE();
        if (length > buffer.readableBytes()) {
            throw new IggyMalformedResponseException("Length " + length + " for " + field
                    + " exceeds remaining payload of " + buffer.readableBytes() + " bytes");
        }
        return buffer.readCharSequence(toInt(length), StandardCharsets.UTF_8).toString();
    }

    private static int validatedCollectionSize(long count, int readableBytes, int entryBytes, String field) {
        if (count > readableBytes / entryBytes) {
            throw new IggyMalformedResponseException(
                    field + " " + count + " exceeds remaining payload of " + readableBytes + " bytes");
        }
        return Math.toIntExact(count);
    }

    static BigInteger readU64AsBigInteger(ByteBuf buffer) {
        var bytesArray = new byte[8];
        buffer.readBytes(bytesArray, 0, 8);
        ArrayUtils.reverse(bytesArray);
        return new BigInteger(1, bytesArray);
    }

    private static BytesMessageId readBytesMessageId(ByteBuf buffer) {
        var bytesArray = new byte[16];
        buffer.readBytes(bytesArray);
        ArrayUtils.reverse(bytesArray);
        return new BytesMessageId(bytesArray);
    }

    private static int toInt(Long size) {
        return Math.toIntExact(size);
    }

    private static byte[] newByteArray(Long size) {
        return new byte[size.intValue()];
    }
}
