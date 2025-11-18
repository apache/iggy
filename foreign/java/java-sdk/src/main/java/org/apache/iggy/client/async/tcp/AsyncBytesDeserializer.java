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

package org.apache.iggy.client.async.tcp;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.iggy.message.BytesMessageId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.MessageHeader;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.partition.Partition;
import org.apache.iggy.stream.StreamBase;
import org.apache.iggy.stream.StreamDetails;
import org.apache.iggy.topic.CompressionAlgorithm;
import org.apache.iggy.topic.Topic;
import org.apache.iggy.topic.TopicDetails;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Async version of BytesDeserializer for the async package.
 * Provides the same wire protocol deserialization as the blocking version.
 */
public final class AsyncBytesDeserializer {

    private AsyncBytesDeserializer() {}

    /**
     * Reads PolledMessages from the response buffer.
     * @param response The ByteBuf containing the response data
     * @return The deserialized PolledMessages
     */
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

    /**
     * Reads a single Message from the buffer.
     */
    private static Message readPolledMessage(ByteBuf response) {
        var checksum = readU64AsBigInteger(response);
        var id = readBytesMessageId(response);
        var offset = readU64AsBigInteger(response);
        var timestamp = readU64AsBigInteger(response);
        var originTimestamp = readU64AsBigInteger(response);
        var userHeadersLength = response.readUnsignedIntLE();
        var payloadLength = response.readUnsignedIntLE();
        var header =
                new MessageHeader(checksum, id, offset, timestamp, originTimestamp, userHeadersLength, payloadLength);
        var payload = new byte[toInt(payloadLength)];
        response.readBytes(payload);
        // TODO: Add support for user headers.
        return new Message(header, payload, Optional.empty());
    }

    /**
     * Reads an unsigned 64-bit integer as BigInteger.
     */
    private static BigInteger readU64AsBigInteger(ByteBuf buffer) {
        var bytesArray = new byte[8];
        buffer.readBytes(bytesArray, 0, 8);
        ArrayUtils.reverse(bytesArray);
        // Ensure it's treated as unsigned
        byte[] unsigned = new byte[9];
        System.arraycopy(bytesArray, 0, unsigned, 1, 8);
        return new BigInteger(unsigned);
    }

    /**
     * Reads a 16-byte message ID.
     */
    private static BytesMessageId readBytesMessageId(ByteBuf buffer) {
        var bytesArray = new byte[16];
        buffer.readBytes(bytesArray);
        ArrayUtils.reverse(bytesArray);
        return new BytesMessageId(bytesArray);
    }

    /**
     * Converts a long to int safely.
     */
    private static int toInt(long value) {
        if (value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Value too large for int: " + value);
        }
        return (int) value;
    }

    /**
     * Reads StreamBase from the buffer.
     * @param response The ByteBuf containing the response data
     * @return The deserialized StreamBase
     */
    public static StreamBase readStreamBase(ByteBuf response) {
        var streamId = response.readUnsignedIntLE();
        var createdAt = readU64AsBigInteger(response);
        var topicsCount = response.readUnsignedIntLE();
        var size = readU64AsBigInteger(response);
        var messagesCount = readU64AsBigInteger(response);
        var nameLength = response.readByte();
        var name = response.readCharSequence(nameLength, StandardCharsets.UTF_8).toString();

        return new StreamBase(streamId, createdAt, name, size.toString(), messagesCount, topicsCount);
    }

    /**
     * Reads StreamDetails from the buffer.
     * @param response The ByteBuf containing the response data
     * @return The deserialized StreamDetails
     */
    public static StreamDetails readStreamDetails(ByteBuf response) {
        var streamBase = readStreamBase(response);

        List<Topic> topics = new ArrayList<>();
        while (response.isReadable()) {
            topics.add(readTopic(response));
        }

        return new StreamDetails(streamBase, topics);
    }

    /**
     * Reads Topic from the buffer.
     * @param response The ByteBuf containing the response data
     * @return The deserialized Topic
     */
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

        return new Topic(
                topicId,
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

    /**
     * Reads TopicDetails from the buffer.
     * @param response The ByteBuf containing the response data
     * @return The deserialized TopicDetails
     */
    public static TopicDetails readTopicDetails(ByteBuf response) {
        var topic = readTopic(response);

        List<Partition> partitions = new ArrayList<>();
        while (response.isReadable()) {
            partitions.add(readPartition(response));
        }

        return new TopicDetails(topic, partitions);
    }

    /**
     * Reads Partition from the buffer.
     */
    private static Partition readPartition(ByteBuf response) {
        var partitionId = response.readUnsignedIntLE();
        var createdAt = readU64AsBigInteger(response);
        var segmentsCount = response.readUnsignedIntLE();
        var currentOffset = readU64AsBigInteger(response);
        var size = readU64AsBigInteger(response);
        var messagesCount = readU64AsBigInteger(response);

        return new Partition(partitionId, createdAt, segmentsCount, currentOffset, size.toString(), messagesCount);
    }
}
