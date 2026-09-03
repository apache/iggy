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
using System.Text;
using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Auth;
using Apache.Iggy.Encryption;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Extensions;
using Apache.Iggy.Headers;
using Apache.Iggy.IggyClient.Implementations;
using Apache.Iggy.Shared;
using Apache.Iggy.Tests.Utils;
using Apache.Iggy.Tests.Utils.Groups;
using Apache.Iggy.Tests.Utils.Messages;
using Apache.Iggy.Tests.Utils.Stats;
using Apache.Iggy.Tests.Utils.Topics;
using Apache.Iggy.Vsr;
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
        var partitionId = (uint)Random.Shared.Next(1, 19);
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
        var (offset, timestamp, guid, _, checkSum, payload) = MessageFactory.CreateMessageResponseFields();
        var msgOneFrame = BinaryFactory.CreateMessageFrame(checkSum, guid, 0, 0, [], payload);
        var (_, _, guid1, _, checkSum2, payload1) = MessageFactory.CreateMessageResponseFields();
        var msgTwoFrame = BinaryFactory.CreateMessageFrame(checkSum2, guid1, 1, 5, [], payload1);
        var record = BinaryFactory.CreateBatchRecord(offset, timestamp, timestamp, msgOneFrame, msgTwoFrame);

        var combinedPayload = new byte[16 + record.Length];
        BinaryPrimitives.WriteUInt32LittleEndian(combinedPayload.AsSpan(12, 4), 2);
        record.CopyTo(combinedPayload.AsSpan(16));

        // Act
        var responses
            = Mappers.BinaryMapper.MapRentedMessages(combinedPayload, EmptyMemoryOwner.Instance);

        // Assert
        Assert.NotNull(responses);
        Assert.Equal(2, responses.Messages.Count());

        var response1 = responses.Messages.ElementAt(0);
        Assert.Equal(payload, response1.Payload);
        Assert.Equal(offset, response1.Header.Offset);
        Assert.Equal(timestamp, response1.Header.OriginTimestamp);

        var response2 = responses.Messages.ElementAt(1);
        Assert.Equal(payload1, response2.Payload);
        Assert.Equal(offset + 1, response2.Header.Offset);
        Assert.Equal(timestamp + 5, response2.Header.OriginTimestamp);
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
        var (id, _, sizeBytes, messagesCount, name, createdAt) = StreamFactory.CreateStreamsResponseFields();
        // Topics are decoded count-driven, so the header count must match the appended topics.
        var topicsCount = 1u;
        var streamPayload
            = BinaryFactory.CreateStreamPayload(id, topicsCount, name, sizeBytes, messagesCount, createdAt);
        var (topicId1, partitionsCount1, topicName1, messageExpiry1, topicSizeBytes1, messagesCountTopic1,
                createdAtTopic, maxTopicSize) =
            TopicFactory.CreateTopicResponseFields();
        var topicPayload1 = BinaryFactory.CreateTopicPayload(topicId1,
            partitionsCount1,
            messageExpiry1,
            topicName1,
            topicSizeBytes1,
            messagesCountTopic1,
            createdAt,
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
                maxTopicSize1) =
            TopicFactory.CreateTopicResponseFields();
        var payload1 = BinaryFactory.CreateTopicPayload(id1, partitionsCount1, messageExpiry1, name1,
            sizeBytesTopic1, messagesCountTopic1, createdAt, maxTopicSize1, 1);
        var (id2, partitionsCount2, name2, messageExpiry2, sizeBytesTopic2, messagesCountTopic2, createdAt2,
                maxTopicSize2) =
            TopicFactory.CreateTopicResponseFields();
        var payload2 = BinaryFactory.CreateTopicPayload(id2, partitionsCount2, messageExpiry2, name2,
            sizeBytesTopic2, messagesCountTopic2, createdAt2, maxTopicSize2, 2);

        // GetTopics replies start with the topics count.
        var combinedPayload = new byte[4 + payload1.Length + payload2.Length];
        BinaryPrimitives.WriteUInt32LittleEndian(combinedPayload.AsSpan(0, 4), 2);
        payload1.CopyTo(combinedPayload.AsSpan(4));
        payload2.CopyTo(combinedPayload.AsSpan(4 + payload1.Length));

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
        var (topicId, partitionsCount, topicName, messageExpiry, sizeBytes, messagesCount, createdAt2,
            maxTopicSize) = TopicFactory.CreateTopicResponseFields();
        var topicPayload = BinaryFactory.CreateTopicPayload(topicId, partitionsCount, messageExpiry, topicName,
            sizeBytes, messagesCount, createdAt2, maxTopicSize, 1);

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
    public void MapTopic_WithAnOptionOfAnUnknownKind_KeepsTheOtherEntries()
    {
        // Arrange: an option kind a newer server may introduce, between two this build knows.
        const byte stringKind = 2;
        const byte unknownKind = 200;
        var (topicId, partitionsCount, topicName, messageExpiry, sizeBytes, messagesCount, createdAt,
            maxTopicSize) = TopicFactory.CreateTopicResponseFields();
        var options = new List<byte>();
        options.AddRange(BinaryFactory.CreateOptionEntry(stringKind, "segment_size", stringKind, "1GB"u8.ToArray()));
        options.AddRange(BinaryFactory.CreateOptionEntry(stringKind, "future_option", unknownKind, [0xAA, 0xBB]));
        options.AddRange(BinaryFactory.CreateOptionEntry(stringKind, "enforce_fsync", 3, [1]));
        var topicPayload = BinaryFactory.CreateTopicPayload(topicId, partitionsCount, messageExpiry, topicName,
            sizeBytes, messagesCount, createdAt, maxTopicSize, 1, options.ToArray());

        // Act
        var response = Mappers.BinaryMapper.MapTopic(topicPayload);

        // Assert: the unknown entry is dropped, everything around it still decodes.
        Assert.Equal(topicName, response.Name);
        Assert.NotNull(response.Options);
        Assert.Equal(2, response.Options.Count);
        Assert.Equal("1GB", response.Options[HeaderKey.FromString("segment_size")].ToString());
        Assert.Equal(HeaderKind.Bool, response.Options[HeaderKey.FromString("enforce_fsync")].Kind);
        Assert.False(response.Options.ContainsKey(HeaderKey.FromString("future_option")));
        Assert.NotNull(response.DerivedOptions);
        Assert.Empty(response.DerivedOptions);
    }

    [Fact]
    public void MapTopic_WithAnOptionValueOfTheWrongWidth_Throws()
    {
        // Arrange: a Uint64 value carrying three bytes, which the Rust decoder rejects too.
        const byte stringKind = 2;
        const byte uint64Kind = 12;
        var (topicId, partitionsCount, topicName, messageExpiry, sizeBytes, messagesCount, createdAt,
            maxTopicSize) = TopicFactory.CreateTopicResponseFields();
        var options = BinaryFactory.CreateOptionEntry(stringKind, "segment_size", uint64Kind, [1, 2, 3]);
        var topicPayload = BinaryFactory.CreateTopicPayload(topicId, partitionsCount, messageExpiry, topicName,
            sizeBytes, messagesCount, createdAt, maxTopicSize, 1, options);

        // Act + Assert
        Assert.Throws<MalformedResponseException>(() => Mappers.BinaryMapper.MapTopic(topicPayload));
    }

    [Fact]
    public void MapOptionSpecs_ReturnsTheCatalogWithKindsAndDefaults()
    {
        // Arrange: [count][key_len][key][kind][default_len][default][description_len][description]
        const string key = "segment_size";
        const string description = "Segment size in bytes";
        var defaultValue = new byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(defaultValue, 1024UL * 1024 * 1024);

        var payload = new List<byte>();
        payload.AddRange(BitConverter.GetBytes(1u));
        payload.Add((byte)key.Length);
        payload.AddRange(Encoding.UTF8.GetBytes(key));
        payload.Add(12); // Uint64 wire code
        payload.AddRange(BitConverter.GetBytes((uint)defaultValue.Length));
        payload.AddRange(defaultValue);
        payload.AddRange(BitConverter.GetBytes((uint)description.Length));
        payload.AddRange(Encoding.UTF8.GetBytes(description));

        // Act
        var specs = Mappers.BinaryMapper.MapOptionSpecs(payload.ToArray());

        // Assert
        var spec = Assert.Single(specs);
        Assert.Equal(key, spec.Key);
        Assert.Equal(HeaderKind.Uint64, spec.Kind);
        Assert.Equal(defaultValue, spec.DefaultValue);
        Assert.Equal(description, spec.Description);
    }

    [Fact]
    public void MapOptionSpecs_RejectsAnEntryThatOverrunsThePayload()
    {
        // A declared length past the end must not read adjacent memory.
        var payload = new List<byte>();
        payload.AddRange(BitConverter.GetBytes(1u));
        payload.Add(4);
        payload.AddRange(Encoding.UTF8.GetBytes("size"));
        payload.Add(12);
        payload.AddRange(BitConverter.GetBytes(64u)); // claims 64 bytes that are not there

        Assert.Throws<MalformedResponseException>(() =>
            Mappers.BinaryMapper.MapOptionSpecs(payload.ToArray()));
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
        List<uint> memberPartitions = Enumerable.Range(0, (int)partitionsCount).Select(i => (uint)i).ToList();
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
    public void MapConsumerGroup_NegativeMemberPartitionsCount_Throws()
    {
        var payload = new byte[21];
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(0, 4), 1); // group id
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(4, 4), 3); // group partitions_count
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(8, 4), 1); // members_count
        payload[12] = 0; // name_len
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(13, 4), 42); // member id
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(17, 4), -2); // member partitions_count

        Assert.Throws<MalformedResponseException>(() => Mappers.BinaryMapper.MapConsumerGroup(payload));
    }

    [Fact]
    public void MapConsumerGroup_MemberPartitionsCountExceedsPayload_Throws()
    {
        var payload = BinaryFactory.CreateGroupPayload(1, 1, 3, "group", [0, 1, 2]);
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(13 + "group".Length + 4, 4), uint.MaxValue);

        Assert.Throws<MalformedResponseException>(() => Mappers.BinaryMapper.MapConsumerGroup(payload));
    }

    [Fact]
    public void MapConsumerGroup_TruncatedMemberHeader_Throws()
    {
        var payload = BinaryFactory.CreateGroupPayload(1, 1, 3, "group", [0, 1, 2]);
        var truncated = payload.AsSpan(0, 13 + "group".Length + 5).ToArray();

        Assert.Throws<MalformedResponseException>(() => Mappers.BinaryMapper.MapConsumerGroup(truncated));
    }

    [Fact]
    public void MapConsumerGroup_NameLengthExceedsPayload_Throws()
    {
        var payload = BinaryFactory.CreateGroupPayload(1, 0, 3, "group");
        payload[12] = byte.MaxValue;

        Assert.Throws<MalformedResponseException>(() => Mappers.BinaryMapper.MapConsumerGroup(payload));
    }

    [Fact]
    public void MapConsumerGroups_MultiByteName_KeepsWalkAligned()
    {
        var first = BinaryFactory.CreateGroupPayload(1, 2, 3, "grüppe");
        var second = BinaryFactory.CreateGroupPayload(2, 4, 5, "other");
        var combined = new byte[first.Length + second.Length];
        first.CopyTo(combined, 0);
        second.CopyTo(combined, first.Length);

        var responses = Mappers.BinaryMapper.MapConsumerGroups(combined);

        Assert.Equal(2, responses.Count);
        Assert.Equal("grüppe", responses[0].Name);
        Assert.Equal(2u, responses[1].Id);
        Assert.Equal("other", responses[1].Name);
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

        var frame1 = BuildEncryptedFrame(encryptor, 0, payload1, headers1);
        var frame2 = BuildEncryptedFrame(encryptor, 1, payload2, ReadOnlySpan<byte>.Empty);
        var record = BinaryFactory.CreateBatchRecord(100, 12345, 12345, frame1, frame2);

        var combined = new byte[16 + record.Length];
        BinaryPrimitives.WriteInt32LittleEndian(combined.AsSpan(0, 4), 7);
        BinaryPrimitives.WriteUInt64LittleEndian(combined.AsSpan(4, 8), 101);
        BinaryPrimitives.WriteUInt32LittleEndian(combined.AsSpan(12, 4), 2);
        record.CopyTo(combined.AsSpan(16));

        using var rental = Mappers.BinaryMapper.MapRentedMessages(combined, EmptyMemoryOwner.Instance,
            encryptor);

        Assert.Equal(7u, rental.PartitionId);
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
    public void MapRentedMessages_WithEncryptor_NegativePayloadLength_Throws()
    {
        var encryptor = new AesMessageEncryptor(AesMessageEncryptor.GenerateKey());

        var frame = new byte[48];
        BinaryPrimitives.WriteInt32LittleEndian(frame.AsSpan(32, 4), 0); // headersLength
        BinaryPrimitives.WriteInt32LittleEndian(frame.AsSpan(36, 4), -48); // payloadLength
        var record = BinaryFactory.CreateBatchRecord(1, 12345, 12345, frame);

        var combined = new byte[16 + record.Length];
        BinaryPrimitives.WriteInt32LittleEndian(combined.AsSpan(0, 4), 7);
        BinaryPrimitives.WriteUInt64LittleEndian(combined.AsSpan(4, 8), 1);
        BinaryPrimitives.WriteUInt32LittleEndian(combined.AsSpan(12, 4), 1);
        record.CopyTo(combined.AsSpan(16));

        Assert.Throws<MalformedResponseException>(() =>
            Mappers.BinaryMapper.MapRentedMessages(combined, EmptyMemoryOwner.Instance, encryptor));
    }

    [Fact]
    public void MapRentedMessages_NonzeroFrameReserved_Throws()
    {
        var frame = BinaryFactory.CreateMessageFrame(0, Guid.NewGuid(), 0, 0, [], "payload"u8);
        BinaryPrimitives.WriteUInt64LittleEndian(frame.AsSpan(40, 8), 1);
        var record = BinaryFactory.CreateBatchRecord(1, 12345, 12345, frame);

        var combined = new byte[16 + record.Length];
        BinaryPrimitives.WriteUInt32LittleEndian(combined.AsSpan(12, 4), 1);
        record.CopyTo(combined.AsSpan(16));

        Assert.Throws<MalformedResponseException>(() =>
            Mappers.BinaryMapper.MapRentedMessages(combined, EmptyMemoryOwner.Instance));
    }

    [Fact]
    public void MapRentedMessages_WithEncryptor_TamperedCiphertext_ThrowsMessageDecryptionException()
    {
        var encryptor = new AesMessageEncryptor(AesMessageEncryptor.GenerateKey());
        var frame = BuildEncryptedFrame(encryptor, 0, "secret-payload"u8, ReadOnlySpan<byte>.Empty);

        frame[48 + 12] ^= 0xFF;
        var record = BinaryFactory.CreateBatchRecord(42, 12345, 12345, frame);

        var combined = new byte[16 + record.Length];
        BinaryPrimitives.WriteInt32LittleEndian(combined.AsSpan(0, 4), 7);
        BinaryPrimitives.WriteUInt64LittleEndian(combined.AsSpan(4, 8), 42);
        BinaryPrimitives.WriteUInt32LittleEndian(combined.AsSpan(12, 4), 1);
        record.CopyTo(combined.AsSpan(16));

        var ex = Assert.Throws<MessageDecryptionException>(() =>
            Mappers.BinaryMapper.MapRentedMessages(combined, EmptyMemoryOwner.Instance, encryptor));

        Assert.Equal(42ul, ex.Offset);
        Assert.Equal(7u, ex.PartitionId);
        Assert.IsAssignableFrom<CryptographicException>(ex.InnerException);
    }

    [Fact]
    public void MapSendMessages_ReturnsConfirmations()
    {
        // Wire layout mirrors core/binary_protocol responses/messages/send_messages.rs:
        // [count:4][stream_id:4][topic_id:4][partition_id:4][base_offset:8]*
        var payload = new byte[4 + 20];
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(0, 4), 1);
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(4, 4), 1);
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(8, 4), 2);
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(12, 4), 3);
        BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(16, 8), 42);

        var response = Mappers.BinaryMapper.MapSendMessages(payload);

        var confirmation = Assert.Single(response.Confirmations);
        Assert.Equal(1u, confirmation.StreamId);
        Assert.Equal(2u, confirmation.TopicId);
        Assert.Equal(3u, confirmation.PartitionId);
        Assert.Equal(42ul, confirmation.BaseOffset);
    }

    [Fact]
    public void MapSendMessages_MultipleConfirmations_ReturnsAll()
    {
        var payload = new byte[4 + 3 * 20];
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(0, 4), 3);
        for (var i = 0; i < 3; i++)
        {
            var position = 4 + i * 20;
            BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(position, 4), 1);
            BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(position + 4, 4), 2);
            BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(position + 8, 4), (uint)i);
            BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(position + 12, 8), (ulong)(100 + i));
        }

        var response = Mappers.BinaryMapper.MapSendMessages(payload);

        Assert.Equal(3, response.Confirmations.Count);
        Assert.Equal(2u, response.Confirmations[2].PartitionId);
        Assert.Equal(102ul, response.Confirmations[2].BaseOffset);
    }

    [Fact]
    public void MapSendMessages_EmptyBody_Throws()
    {
        Assert.Throws<InvalidResponseException>(() => Mappers.BinaryMapper.MapSendMessages([]));
    }

    [Fact]
    public void MapSendMessages_ZeroCount_ReturnsNoConfirmations()
    {
        var response = Mappers.BinaryMapper.MapSendMessages(new byte[4]);

        Assert.Empty(response.Confirmations);
    }

    [Theory]
    [InlineData(4 + 19)] // truncated entry
    [InlineData(4 + 21)] // trailing byte
    public void MapSendMessages_ShapeMismatch_Throws(int payloadLength)
    {
        var payload = new byte[payloadLength];
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(0, 4), 1);

        Assert.Throws<InvalidResponseException>(() => Mappers.BinaryMapper.MapSendMessages(payload));
    }

    [Fact]
    public void MapSendMessages_BogusCount_DoesNotOverflow()
    {
        var payload = new byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(payload, uint.MaxValue);

        Assert.Throws<InvalidResponseException>(() => Mappers.BinaryMapper.MapSendMessages(payload));
    }

    private static byte[] BuildEncryptedFrame(AesMessageEncryptor encryptor, uint offsetDelta,
        ReadOnlySpan<byte> plainPayload, ReadOnlySpan<byte> plainHeaders)
    {
        var cipherPayload = encryptor.EncryptToArray(plainPayload);
        var cipherHeaders = plainHeaders.Length > 0 ? encryptor.EncryptToArray(plainHeaders) : [];

        return BinaryFactory.CreateMessageFrame(0, Guid.NewGuid(), offsetDelta, 0, cipherHeaders, cipherPayload);
    }

    [Fact]
    public void MapClusterMetadata_OversizedNodesCount_Throws()
    {
        var payload = new byte[8];
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(0, 4), 0); // cluster name length
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(4, 4), uint.MaxValue); // nodes count

        Assert.Throws<MalformedResponseException>(() => Mappers.BinaryMapper.MapClusterMetadata(payload));
    }

    [Fact]
    public void MapClusterMetadata_MultiByteNodeName_KeepsWalkAligned()
    {
        var payload = CreateClusterMetadataPayload("cluster",
            ("nödé-1", "10.0.0.1", 8090, 1, 1),
            ("node-2", "10.0.0.2", 8091, 0, 1));

        var metadata = Mappers.BinaryMapper.MapClusterMetadata(payload);

        Assert.Equal("cluster", metadata.Name);
        Assert.Equal(2, metadata.Nodes.Length);
        Assert.Equal("nödé-1", metadata.Nodes[0].Name);
        Assert.Equal("10.0.0.1", metadata.Nodes[0].Ip);
        Assert.Equal(8090, metadata.Nodes[0].Endpoints.Tcp);
        Assert.Equal("node-2", metadata.Nodes[1].Name);
        Assert.Equal("10.0.0.2", metadata.Nodes[1].Ip);
        Assert.Equal(8091, metadata.Nodes[1].Endpoints.Tcp);
    }

    [Fact]
    public void MapClusterMetadata_TruncatedNode_Throws()
    {
        var payload = CreateClusterMetadataPayload("cluster", ("node-1", "10.0.0.1", 8090, 1, 1));
        var truncated = payload.AsSpan(0, payload.Length - 3).ToArray();

        Assert.Throws<MalformedResponseException>(() => Mappers.BinaryMapper.MapClusterMetadata(truncated));
    }

    [Fact]
    public void MapClusterMetadata_TruncatedInsideLengthPrefix_Throws()
    {
        var payload = CreateClusterMetadataPayload("cluster", ("node-1", "10.0.0.1", 8090, 1, 1));
        // cut inside the u32 length prefix of the node ip, right after the node name
        var cut = 4 + "cluster".Length + 4 + 4 + "node-1".Length + 2;
        var truncated = payload.AsSpan(0, cut).ToArray();

        Assert.Throws<MalformedResponseException>(() => Mappers.BinaryMapper.MapClusterMetadata(truncated));
    }

    [Fact]
    public void MapClusterMetadata_TruncatedBeforeNodesCount_Throws()
    {
        var payload = CreateClusterMetadataPayload("cluster");
        var truncated = payload.AsSpan(0, payload.Length - 2).ToArray();

        Assert.Throws<MalformedResponseException>(() => Mappers.BinaryMapper.MapClusterMetadata(truncated));
    }

    [Fact]
    public void MapClusterMetadata_NameLengthExceedsPayload_Throws()
    {
        var payload = CreateClusterMetadataPayload("cluster", ("node-1", "10.0.0.1", 8090, 1, 1));
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(0, 4), uint.MaxValue);

        Assert.Throws<MalformedResponseException>(() => Mappers.BinaryMapper.MapClusterMetadata(payload));
    }

    [Fact]
    public void MapClient_OversizedConsumerGroupsCount_Throws()
    {
        var payload = new byte[17];
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(0, 4), 1); // client id
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(4, 4), 1); // user id
        payload[8] = 1; // transport tcp
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(9, 4), 0); // address length
        BinaryPrimitives.WriteInt32LittleEndian(payload.AsSpan(13, 4), int.MaxValue); // consumer groups count

        Assert.Throws<MalformedResponseException>(() => Mappers.BinaryMapper.MapClient(payload));
    }

    [Theory]
    [InlineData(0, ClientTransport.Unknown)]
    [InlineData(1, ClientTransport.Tcp)]
    [InlineData(2, ClientTransport.Quic)]
    [InlineData(3, ClientTransport.Http)]
    [InlineData(4, ClientTransport.WebSocket)]
    [InlineData(9, ClientTransport.Unknown)]
    public void MapClient_MapsEveryWireTransport(byte wire, ClientTransport expected)
    {
        var payload = CreateClientPayload(userId: 7, transport: wire);

        var response = Mappers.BinaryMapper.MapClient(payload);

        Assert.Equal(expected, response.Transport);
        Assert.Equal(7u, response.UserId);
    }

    [Fact]
    public void MapClient_UnauthenticatedSentinel_YieldsNullUserId()
    {
        var payload = CreateClientPayload(userId: uint.MaxValue, transport: 1);

        var response = Mappers.BinaryMapper.MapClient(payload);

        Assert.Null(response.UserId);
    }

    [Theory]
    [InlineData(5)]
    [InlineData(6)]
    [InlineData(200)]
    public void MapClusterMetadata_UnknownStatus_MapsToUnknown(byte status)
    {
        var payload = CreateClusterMetadataPayload("cluster", ("node", "127.0.0.1", 8090, 1, status));

        var metadata = Mappers.BinaryMapper.MapClusterMetadata(payload);

        Assert.Equal(ClusterNodeStatus.Unknown, metadata.Nodes[0].Status);
    }

    [Fact]
    public void MapClusterMetadata_UnknownRole_Throws()
    {
        var payload = CreateClusterMetadataPayload("cluster", ("node", "127.0.0.1", 8090, 2, 0));

        Assert.Throws<MalformedResponseException>(() => Mappers.BinaryMapper.MapClusterMetadata(payload));
    }

    [Fact]
    public void MapClient_AddressLengthAboveInt32_ThrowsMalformed()
    {
        var payload = CreateClientPayload(userId: 7, transport: 1);
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(9, 4), 0x8000_0000);

        Assert.Throws<MalformedResponseException>(() => Mappers.BinaryMapper.MapClient(payload));
    }

    private static byte[] CreateClientPayload(uint userId, byte transport)
    {
        var payload = new byte[17];
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(0, 4), 1);
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(4, 4), userId);
        payload[8] = transport;
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(9, 4), 0);
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(13, 4), 0);
        return payload;
    }

    [Fact]
    public void MapTopics_OversizedTopicsCount_Throws()
    {
        var payload = new byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(payload, uint.MaxValue);

        Assert.Throws<MalformedResponseException>(() => Mappers.BinaryMapper.MapTopics(payload));
    }

    [Fact]
    public void MapHeaders_KeyLengthDisagreesWithKind_Throws()
    {
        // key: kind Uint32 (11) with a single byte; value: string "v".
        var bytes = new byte[] { 11, 1, 0, 0, 0, 65, 2, 1, 0, 0, 0, 118 };

        Assert.Throws<MalformedResponseException>(() => Mappers.BinaryMapper.MapHeaders(bytes));
    }

    [Fact]
    public void MapHeaders_FixedWidthValueOfMatchingLength_Parses()
    {
        var bytes = new byte[] { 2, 1, 0, 0, 0, 65, 12, 8, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0 };

        Dictionary<HeaderKey, HeaderValue> headers = Mappers.BinaryMapper.MapHeaders(bytes);

        var value = Assert.Single(headers).Value;
        Assert.Equal(HeaderKind.Uint64, value.Kind);
        Assert.Equal(7ul, value.ToUInt64());
    }

    [Fact]
    public void MapHeaders_ValueLengthDisagreesWithKind_Throws()
    {
        var bytes = new byte[] { 2, 1, 0, 0, 0, 65, 6, 2, 0, 0, 0, 7, 0 };

        Assert.Throws<MalformedResponseException>(() => Mappers.BinaryMapper.MapHeaders(bytes));
    }

    private static byte[] CreateClusterMetadataPayload(string clusterName,
        params (string name, string ip, ushort tcp, byte role, byte status)[] nodes)
    {
        var buffer = new List<byte>();
        WriteString(buffer, clusterName);
        buffer.AddRange(BitConverter.GetBytes((uint)nodes.Length));
        foreach (var (name, ip, tcp, role, status) in nodes)
        {
            WriteString(buffer, name);
            WriteString(buffer, ip);
            buffer.AddRange(BitConverter.GetBytes(tcp));
            buffer.AddRange(BitConverter.GetBytes((ushort)0)); // quic
            buffer.AddRange(BitConverter.GetBytes((ushort)0)); // http
            buffer.AddRange(BitConverter.GetBytes((ushort)0)); // websocket
            buffer.Add(role);
            buffer.Add(status);
        }

        return buffer.ToArray();

        static void WriteString(List<byte> buffer, string value)
        {
            var bytes = Encoding.UTF8.GetBytes(value);
            buffer.AddRange(BitConverter.GetBytes((uint)bytes.Length));
            buffer.AddRange(bytes);
        }
    }
}
