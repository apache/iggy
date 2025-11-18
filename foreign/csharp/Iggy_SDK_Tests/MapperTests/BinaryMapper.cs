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

using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Auth;
using Apache.Iggy.Enums;
using Apache.Iggy.Extensions;
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
        var responses = Mappers.BinaryMapper.MapMessages(combinedPayload);

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
}
