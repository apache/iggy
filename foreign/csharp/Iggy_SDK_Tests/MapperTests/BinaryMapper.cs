// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using System.Buffers.Binary;
using System.Security.Cryptography;
using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Auth;
using Apache.Iggy.Encryption;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Extensions;
using Apache.Iggy.IggyClient.Implementations;
using Apache.Iggy.Shared;
using Apache.Iggy.Tests.Utils;
using Apache.Iggy.Tests.Utils.Groups;
using Apache.Iggy.Tests.Utils.Messages;
using Apache.Iggy.Tests.Utils.Stats;
using Apache.Iggy.Tests.Utils.Topics;
using StreamFactory = Apache.Iggy.Tests.Utils.Streams.StreamFactory;

namespace Apache.Iggy.Tests.MapperTests;

public sealed class BinaryMapper
{
    [Fact]
    public void MapPersonalAccessTokens_ReturnsValidPersonalAccessTokenResponse()
    {
        // Arrange
        var name = "test";
        uint expiry = 69420;
        var assertExpiry = DateTimeOffsetUtils.FromUnixTimeMicroSeconds(expiry).LocalDateTime;
        var payload = BinaryFactory.CreatePersonalAccessTokensPayload(name, expiry);

        // Act
        IReadOnlyList<PersonalAccessTokenResponse> response = Mappers.BinaryMapper.MapPersonalAccessTokens(payload);

        // Assert
        Assert.NotNull(response);
        Assert.Equal(name, response[0].Name);
        Assert.Equal(assertExpiry, response[0].ExpiryAt);
    }

    [Fact]
    public void MapOffsets_ReturnsValidOffsetResponse()
    {
        // Arrange
        var partitionId = Random.Shared.Next(1, 19);
        var currentOffset = (ulong)Random.Shared.Next(420, 69420);
        var storedOffset = (ulong)Random.Shared.Next(69, 420);
        var payload = BinaryFactory.CreateOffsetPayload(partitionId, currentOffset, storedOffset);

        // Act
        var response = Mappers.BinaryMapper.MapOffsets(payload);

        // Assert
        Assert.NotNull(response);
        Assert.Equal(partitionId, response.PartitionId);
        Assert.Equal(currentOffset, response.CurrentOffset);
        Assert.Equal(storedOffset, response.StoredOffset);
    }

    [Fact]
    public void MapMessages_NoHeaders_ReturnsValidMessageResponses()
    {
        // Arrange
        var (offset, timestamp, guid, headersLength, checkSum, payload) = MessageFactory.CreateMessageResponseFields();
        var msgOnePayload = BinaryFactory.CreateMessagePayload(offset, timestamp, 0, checkSum,
            guid, payload);
        var (offset1, timestamp1, guid1, headersLength2, checkSum2, payload1)
            = MessageFactory.CreateMessageResponseFields();
        var msgTwoPayload = BinaryFactory.CreateMessagePayload(offset1, timestamp1, 0, checkSum2,
            guid1, payload1);

        var combinedPayload = new byte[16 + msgOnePayload.Length + msgTwoPayload.Length];
        msgOnePayload.CopyTo(combinedPayload.AsSpan(16));
        msgTwoPayload.CopyTo(combinedPayload.AsSpan(16 + msgOnePayload.Length));

        // Act
        var responses
            = Mappers.BinaryMapper.MapRentedMessages(combinedPayload, TcpMessageStream.EmptyMemoryOwner.Instance);

        // Assert
        Assert.NotNull(responses);
        Assert.Equal(2, responses.Messages.Count());

        var response1 = responses.Messages.ElementAt(0);
        Assert.Equal(payload, response1.Payload);

        var response2 = responses.Messages.ElementAt(1);
        Assert.Equal(payload1, response2.Payload);
    }

    [Fact]
    public void MapStreams_ReturnsValidStreamsResponses()
    {
        // Arrange
        var (id1, topicsCount1, sizeBytes, messagesCount, name1, createdAt)
            = StreamFactory.CreateStreamsResponseFields();
        var payload1 = BinaryFactory.CreateStreamPayload(id1, topicsCount1, name1, sizeBytes, messagesCount, createdAt);
        var (id2, topicsCount2, sizeBytes2, messagesCount2, name2, createdAt2)
            = StreamFactory.CreateStreamsResponseFields();
        var payload2
            = BinaryFactory.CreateStreamPayload(id2, topicsCount2, name2, sizeBytes2, messagesCount2, createdAt2);

        var combinedPayload = new byte[payload1.Length + payload2.Length];
        payload1.CopyTo(combinedPayload.AsSpan());
        payload2.CopyTo(combinedPayload.AsSpan(payload1.Length));

        // Act
        IEnumerable<StreamResponse> responses = Mappers.BinaryMapper.MapStreams(combinedPayload).ToList();

        // Assert
        Assert.NotNull(responses);
        Assert.Equal(2, responses.Count());

        var response1 = responses.ElementAt(0);
        Assert.Equal(id1, response1.Id);
        Assert.Equal(topicsCount1, response1.TopicsCount);
        Assert.Equal(sizeBytes, response1.Size);
        Assert.Equal(messagesCount, response1.MessagesCount);
        Assert.Equal(name1, response1.Name);

        var response2 = responses.ElementAt(1);
        Assert.Equal(id2, response2.Id);
        Assert.Equal(topicsCount2, response2.TopicsCount);
        Assert.Equal(sizeBytes2, response2.Size);
        Assert.Equal(messagesCount2, response2.MessagesCount);
        Assert.Equal(name2, response2.Name);
    }

    [Fact]
    public void MapStream_ReturnsValidStreamResponse()
    {
        // Arrange
        var (id, topicsCount, sizeBytes, messagesCount, name, createdAt) = StreamFactory.CreateStreamsResponseFields();
        var streamPayload
            = BinaryFactory.CreateStreamPayload(id, topicsCount, name, sizeBytes, messagesCount, createdAt);
        var (topicId1, partitionsCount1, topicName1, messageExpiry1, topicSizeBytes1, messagesCountTopic1,
                createdAtTopic, replicationFactor, maxTopicSize) =
            TopicFactory.CreateTopicResponseFields();
        var topicPayload1 = BinaryFactory.CreateTopicPayload(topicId1,
            partitionsCount1,
            messageExpiry1,
            topicName1,
            topicSizeBytes1,
            messagesCountTopic1,
            createdAt,
            replicationFactor,
            maxTopicSize,
            1);

        var topicCombinedPayload = new byte[topicPayload1.Length];
        topicPayload1.CopyTo(topicCombinedPayload.AsSpan());

        var streamCombinedPayload = new byte[streamPayload.Length + topicCombinedPayload.Length];
        streamPayload.CopyTo(streamCombinedPayload.AsSpan());
        topicCombinedPayload.CopyTo(streamCombinedPayload.AsSpan(streamPayload.Length));

        // Act
        var response = Mappers.BinaryMapper.MapStream(streamCombinedPayload);

        // Assert
        Assert.NotNull(response);
        Assert.Equal(id, response.Id);
        Assert.Equal(topicsCount, response.TopicsCount);
        Assert.Equal(name, response.Name);
        Assert.Equal(sizeBytes, response.Size);
        Assert.Equal(messagesCount, response.MessagesCount);
        Assert.NotNull(response.Topics);
        Assert.Single(response.Topics.ToList());

        var topicResponse = response.Topics.First();
        Assert.Equal(topicId1, topicResponse.Id);
        Assert.Equal(partitionsCount1, topicResponse.PartitionsCount);
        Assert.Equal(messagesCountTopic1, topicResponse.MessagesCount);
        Assert.Equal(topicName1, topicResponse.Name);
        Assert.Equal(CompressionAlgorithm.None, topicResponse.CompressionAlgorithm);
    }

    [Fact]
    public void MapTopics_ReturnsValidTopicsResponses()
    {
        // Arrange
        var (id1, partitionsCount1, name1, messageExpiry1, sizeBytesTopic1, messagesCountTopic1, createdAt,
                replicationFactor1, maxTopicSize1) =
            TopicFactory.CreateTopicResponseFields();
        var payload1 = BinaryFactory.CreateTopicPayload(id1, partitionsCount1, messageExpiry1, name1,
            sizeBytesTopic1, messagesCountTopic1, createdAt, replicationFactor1, maxTopicSize1, 1);
        var (id2, partitionsCount2, name2, messageExpiry2, sizeBytesTopic2, messagesCountTopic2, createdAt2,
                replicationFactor2, maxTopicSize2) =
            TopicFactory.CreateTopicResponseFields();
        var payload2 = BinaryFactory.CreateTopicPayload(id2, partitionsCount2, messageExpiry2, name2,
            sizeBytesTopic2, messagesCountTopic2, createdAt2, replicationFactor2, maxTopicSize2, 2);

        var combinedPayload = new byte[payload1.Length + payload2.Length];
        payload1.CopyTo(combinedPayload.AsSpan());
        payload2.CopyTo(combinedPayload.AsSpan(payload1.Length));

        // Act
        IReadOnlyList<TopicResponse> responses = Mappers.BinaryMapper.MapTopics(combinedPayload);

        // Assert
        Assert.NotNull(responses);
        Assert.Equal(2, responses.Count());

        var response1 = responses.ElementAt(0);
        Assert.Equal(id1, response1.Id);
        Assert.Equal(partitionsCount1, response1.PartitionsCount);
        Assert.Equal(sizeBytesTopic1, response1.Size);
        Assert.Equal(messagesCountTopic1, response1.MessagesCount);
        Assert.Equal(name1, response1.Name);
        Assert.Equal(CompressionAlgorithm.None, response1.CompressionAlgorithm);

        var response2 = responses.ElementAt(1);
        Assert.Equal(id2, response2.Id);
        Assert.Equal(sizeBytesTopic2, response2.Size);
        Assert.Equal(messagesCountTopic2, response2.MessagesCount);
        Assert.Equal(partitionsCount2, response2.PartitionsCount);
        Assert.Equal(name2, response2.Name);
        Assert.Equal(CompressionAlgorithm.Gzip, response2.CompressionAlgorithm);
    }

    [Fact]
    public void MapTopic_ReturnsValidTopicResponse()
    {
        // Arrange
        var (topicId, partitionsCount, topicName, messageExpiry, sizeBytes, messagesCount, createdAt2, replicationFactor
            , maxTopicSize) = TopicFactory.CreateTopicResponseFields();
        var topicPayload = BinaryFactory.CreateTopicPayload(topicId, partitionsCount, messageExpiry, topicName,
            sizeBytes, messagesCount, createdAt2, replicationFactor, maxTopicSize, 1);

        var combinedPayload = new byte[topicPayload.Length];
        topicPayload.CopyTo(combinedPayload.AsSpan());

        // Act
        var response = Mappers.BinaryMapper.MapTopic(combinedPayload);

        // Assert
        Assert.NotNull(response);
        Assert.Equal(messagesCount, response.MessagesCount);
        Assert.Equal(partitionsCount, response.PartitionsCount);
        Assert.Equal(sizeBytes, response.Size);
        Assert.Equal(topicId, response.Id);
        Assert.Equal(topicName, response.Name);
        Assert.Equal(CompressionAlgorithm.None, response.CompressionAlgorithm);
    }

    [Fact]
    public void MapConsumerGroups_ReturnsValidConsumerGroupsResponses()
    {
        // Arrange
        var (id1, membersCount1, partitionsCount1, name) = ConsumerGroupFactory.CreateConsumerGroupResponseFields();
        var payload1 = BinaryFactory.CreateGroupPayload(id1, membersCount1, partitionsCount1, name);
        var (id2, membersCount2, partitionsCount2, name2) = ConsumerGroupFactory.CreateConsumerGroupResponseFields();
        var payload2 = BinaryFactory.CreateGroupPayload(id2, membersCount2, partitionsCount2, name2);

        var combinedPayload = new byte[payload1.Length + payload2.Length];
        payload1.CopyTo(combinedPayload.AsSpan());
        payload2.CopyTo(combinedPayload.AsSpan(payload1.Length));

        // Act
        List<ConsumerGroupResponse> responses = Mappers.BinaryMapper.MapConsumerGroups(combinedPayload);

        // Assert
        Assert.NotNull(responses);
        Assert.Equal(2, responses.Count);

        var response1 = responses[0];
        Assert.Equal(id1, response1.Id);
        Assert.Equal(membersCount1, response1.MembersCount);
        Assert.Equal(partitionsCount1, response1.PartitionsCount);

        var response2 = responses[1];
        Assert.Equal(id2, response2.Id);
        Assert.Equal(membersCount2, response2.MembersCount);
        Assert.Equal(partitionsCount2, response2.PartitionsCount);
    }

    [Fact]
    public void MapConsumerGroup_ReturnsValidConsumerGroupResponse()
    {
        // Arrange
        var (groupId, membersCount, partitionsCount, name) = ConsumerGroupFactory.CreateConsumerGroupResponseFields();
        List<int> memberPartitions = Enumerable.Range(0, (int)partitionsCount).ToList();
        var groupPayload
            = BinaryFactory.CreateGroupPayload(groupId, membersCount, partitionsCount, name, memberPartitions);

        // Act
        var response = Mappers.BinaryMapper.MapConsumerGroup(groupPayload);

        // Assert
        Assert.NotNull(response);
        Assert.Equal(groupId, response.Id);
        Assert.Equal(membersCount, response.MembersCount);
        Assert.Equal(partitionsCount, response.PartitionsCount);
        Assert.Equal(memberPartitions.Count, (int)partitionsCount);
        Assert.NotNull(response.Members);
        Assert.Single(response.Members);
    }

    [Fact]
    public void MapStats_ReturnsValidStatsResponse()
    {
        //Arrange
        var stats = StatsFactory.CreateFakeStatsObject();
        var payload = BinaryFactory.CreateStatsPayload(stats);

        //Act
        var response = Mappers.BinaryMapper.MapStats(payload);

        //Assert
        Assert.Equal(stats.ProcessId, response.ProcessId);
        Assert.Equal(stats.MessagesCount, response.MessagesCount);
        Assert.Equal(stats.ConsumerGroupsCount, response.ConsumerGroupsCount);
        Assert.Equal(stats.TopicsCount, response.TopicsCount);
        Assert.Equal(stats.StreamsCount, response.StreamsCount);
        Assert.Equal(stats.PartitionsCount, response.PartitionsCount);
        Assert.Equal(stats.SegmentsCount, response.SegmentsCount);
        Assert.Equal(stats.MessagesSizeBytes, response.MessagesSizeBytes);
        Assert.Equal(stats.CpuUsage, response.CpuUsage);
        Assert.Equal(stats.TotalCpuUsage, response.TotalCpuUsage);
        Assert.Equal(stats.TotalMemory, response.TotalMemory);
        Assert.Equal(stats.AvailableMemory, response.AvailableMemory);
        Assert.Equal(stats.MemoryUsage, response.MemoryUsage);
        Assert.Equal(stats.RunTime, response.RunTime);
        Assert.Equal(stats.StartTime, response.StartTime);
        Assert.Equal(stats.ReadBytes, response.ReadBytes);
        Assert.Equal(stats.WrittenBytes, stats.WrittenBytes);
        Assert.Equal(stats.ClientsCount, response.ClientsCount);
        Assert.Equal(stats.ConsumerGroupsCount, response.ConsumerGroupsCount);
        Assert.Equal(stats.Hostname, response.Hostname);
        Assert.Equal(stats.OsName, response.OsName);
        Assert.Equal(stats.OsVersion, stats.OsVersion);
        Assert.Equal(stats.KernelVersion, response.KernelVersion);
    }

    [Fact]
    public void MapRentedMessages_WithEncryptor_DecryptsPayloadsAndHeadersIntoPooledBuffer()
    {
        var encryptor = new AesMessageEncryptor(AesMessageEncryptor.GenerateKey());
        var payload1 = "first-secret-payload"u8.ToArray();
        var headers1 = "first-secret-headers"u8.ToArray();
        var payload2 = "second-secret-payload"u8.ToArray();

        var frame1 = BuildEncryptedFrame(encryptor, 100, payload1, headers1);
        var frame2 = BuildEncryptedFrame(encryptor, 101, payload2, ReadOnlySpan<byte>.Empty);

        var combined = new byte[16 + frame1.Length + frame2.Length];
        BinaryPrimitives.WriteInt32LittleEndian(combined.AsSpan(0, 4), 7);
        BinaryPrimitives.WriteUInt64LittleEndian(combined.AsSpan(4, 8), 101);
        BinaryPrimitives.WriteUInt32LittleEndian(combined.AsSpan(12, 4), 2);
        frame1.CopyTo(combined.AsSpan(16));
        frame2.CopyTo(combined.AsSpan(16 + frame1.Length));

        using var rental = Mappers.BinaryMapper.MapRentedMessages(combined, TcpMessageStream.EmptyMemoryOwner.Instance,
            encryptor);

        Assert.Equal(7, rental.PartitionId);
        Assert.Equal(101ul, rental.CurrentOffset);
        Assert.Equal(2, rental.Messages.Count);

        var first = rental.Messages[0];
        Assert.Equal(payload1, first.Payload.ToArray());
        Assert.Equal(headers1, first.RawUserHeaders.ToArray());
        Assert.Equal(payload1.Length, first.Header.PayloadLength);
        Assert.Equal(headers1.Length, first.Header.UserHeadersLength);

        var second = rental.Messages[1];
        Assert.Equal(payload2, second.Payload.ToArray());
        Assert.True(second.RawUserHeaders.IsEmpty);
        Assert.Equal(payload2.Length, second.Header.PayloadLength);
        Assert.Equal(0, second.Header.UserHeadersLength);

        var materialized = Mappers.BinaryMapper.MaterializeMessages(rental);
        Assert.Equal(payload1, materialized.Messages[0].Payload);
        Assert.Equal(headers1, materialized.Messages[0].RawUserHeaders);
        Assert.Equal(payload2, materialized.Messages[1].Payload);
    }

    [Fact]
    public void MapRentedMessages_WithEncryptor_NegativePayloadLength_ThrowsInsteadOfSpinning()
    {
        var encryptor = new AesMessageEncryptor(AesMessageEncryptor.GenerateKey());

        var frame = new byte[64];
        BinaryPrimitives.WriteInt32LittleEndian(frame.AsSpan(48, 4), 0); // headersLength
        BinaryPrimitives.WriteInt32LittleEndian(frame.AsSpan(52, 4), -64); // payloadLength

        var combined = new byte[16 + frame.Length];
        BinaryPrimitives.WriteInt32LittleEndian(combined.AsSpan(0, 4), 7);
        BinaryPrimitives.WriteUInt64LittleEndian(combined.AsSpan(4, 8), 1);
        BinaryPrimitives.WriteUInt32LittleEndian(combined.AsSpan(12, 4), 1);
        frame.CopyTo(combined.AsSpan(16));

        Assert.Throws<MalformedResponseException>(() =>
            Mappers.BinaryMapper.MapRentedMessages(combined, TcpMessageStream.EmptyMemoryOwner.Instance, encryptor));
    }

    [Fact]
    public void MapRentedMessages_WithEncryptor_TamperedCiphertext_ThrowsMessageDecryptionException()
    {
        var encryptor = new AesMessageEncryptor(AesMessageEncryptor.GenerateKey());
        var frame = BuildEncryptedFrame(encryptor, 42, "secret-payload"u8, ReadOnlySpan<byte>.Empty);

        frame[64 + 12] ^= 0xFF;

        var combined = new byte[16 + frame.Length];
        BinaryPrimitives.WriteInt32LittleEndian(combined.AsSpan(0, 4), 7);
        BinaryPrimitives.WriteUInt64LittleEndian(combined.AsSpan(4, 8), 42);
        BinaryPrimitives.WriteUInt32LittleEndian(combined.AsSpan(12, 4), 1);
        frame.CopyTo(combined.AsSpan(16));

        var ex = Assert.Throws<MessageDecryptionException>(() =>
            Mappers.BinaryMapper.MapRentedMessages(combined, TcpMessageStream.EmptyMemoryOwner.Instance, encryptor));

        Assert.Equal(42ul, ex.Offset);
        Assert.Equal(7u, ex.PartitionId);
        Assert.IsAssignableFrom<CryptographicException>(ex.InnerException);
    }

    private static byte[] BuildEncryptedFrame(AesMessageEncryptor encryptor, ulong offset,
        ReadOnlySpan<byte> plainPayload, ReadOnlySpan<byte> plainHeaders)
    {
        var cipherPayload = encryptor.EncryptToArray(plainPayload);
        var cipherHeaders = plainHeaders.Length > 0 ? encryptor.EncryptToArray(plainHeaders) : [];

        var frame = new byte[64 + cipherPayload.Length + cipherHeaders.Length];
        Span<byte> span = frame.AsSpan();
        BinaryPrimitives.WriteUInt64LittleEndian(span[..8], 0);
        BinaryPrimitives.WriteUInt128LittleEndian(span[8..24], Guid.NewGuid().ToUInt128());
        BinaryPrimitives.WriteUInt64LittleEndian(span[24..32], offset);
        BinaryPrimitives.WriteUInt64LittleEndian(span[32..40], 12345);
        BinaryPrimitives.WriteUInt64LittleEndian(span[40..48], 12345);
        BinaryPrimitives.WriteInt32LittleEndian(span[48..52], cipherHeaders.Length);
        BinaryPrimitives.WriteInt32LittleEndian(span[52..56], cipherPayload.Length);
        BinaryPrimitives.WriteUInt64LittleEndian(span[56..64], 0);
        cipherPayload.CopyTo(span[64..]);
        cipherHeaders.CopyTo(span[(64 + cipherPayload.Length)..]);

        return frame;
    }
}
