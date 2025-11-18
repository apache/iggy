// // Licensed to the Apache Software Foundation (ASF) under one
// // or more contributor license agreements.  See the NOTICE file
// // distributed with this work for additional information
// // regarding copyright ownership.  The ASF licenses this file
// // to you under the Apache License, Version 2.0 (the
// // "License"); you may not use this file except in compliance
// // with the License.  You may obtain a copy of the License at
// //
// //   http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing,
// // software distributed under the License is distributed on an
// // "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// // KIND, either express or implied.  See the License for the
// // specific language governing permissions and limitations
// // under the License.

using System.Text;
using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Http;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Messages;
using Apache.Iggy.Tests.Integrations.Attributes;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Apache.Iggy.Tests.Integrations.Helpers;
using Shouldly;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Tests.Integrations;

public class TopicsTests
{
    private static readonly CreateTopicRequest TopicRequest = new("Test Topic", CompressionAlgorithm.Gzip, 1000, 1,
        2, 2_000_000_000);

    private static readonly CreateTopicRequest TopicRequestSecond
        = new("Test Topic 2", CompressionAlgorithm.Gzip, 1000, 1, 2, 2_000_000_000);

    private static readonly UpdateTopicRequest UpdateTopicRequest
        = new("Updated Topic", CompressionAlgorithm.Gzip, 3_000_000_000, 2000, 3);

    [ClassDataSource<TopicsFixture>(Shared = SharedType.PerClass)]
    public required TopicsFixture Fixture { get; init; }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task Create_NewTopic_Should_Return_Successfully(Protocol protocol)
    {
        var response = await Fixture.Clients[protocol].CreateTopicAsync(
            Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)), TopicRequest.Name,
            TopicRequest.PartitionsCount, TopicRequest.CompressionAlgorithm, TopicRequest.ReplicationFactor,
            TopicRequest.MessageExpiry, TopicRequest.MaxTopicSize);

        response.ShouldNotBeNull();
        response.Id.ShouldBeGreaterThanOrEqualTo(0u);
        response.CreatedAt.UtcDateTime.ShouldBe(DateTimeOffset.UtcNow.UtcDateTime, TimeSpan.FromMinutes(1));
        response.Name.ShouldBe(TopicRequest.Name);
        response.CompressionAlgorithm.ShouldBe(TopicRequest.CompressionAlgorithm);
        response.Partitions!.Count().ShouldBe((int)TopicRequest.PartitionsCount);
        response.MessageExpiry.ShouldBe(TopicRequest.MessageExpiry);
        response.Size.ShouldBe(0u);
        response.PartitionsCount.ShouldBe(TopicRequest.PartitionsCount);
        response.ReplicationFactor.ShouldBe(TopicRequest.ReplicationFactor);
        response.MaxTopicSize.ShouldBe(TopicRequest.MaxTopicSize);
        response.MessagesCount.ShouldBe(0u);
    }

    [Test]
    [DependsOn(nameof(Create_NewTopic_Should_Return_Successfully))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task Create_DuplicateTopic_Should_Throw_InvalidResponse(Protocol protocol)
    {
        await Should.ThrowAsync<IggyInvalidStatusCodeException>(Fixture.Clients[protocol].CreateTopicAsync(
            Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)), TopicRequest.Name,
            TopicRequest.PartitionsCount, TopicRequest.CompressionAlgorithm, TopicRequest.ReplicationFactor,
            TopicRequest.MessageExpiry, TopicRequest.MaxTopicSize));
    }

    [Test]
    [DependsOn(nameof(Create_DuplicateTopic_Should_Throw_InvalidResponse))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task Get_ExistingTopic_Should_ReturnValidResponse(Protocol protocol)
    {
        var response = await Fixture.Clients[protocol]
            .GetTopicByIdAsync(Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)), Identifier.Numeric(0));

        response.ShouldNotBeNull();
        response.Id.ShouldBeGreaterThanOrEqualTo(0u);
        response.CreatedAt.UtcDateTime.ShouldBe(DateTimeOffset.UtcNow.UtcDateTime, TimeSpan.FromMinutes(1));
        response.Name.ShouldBe(TopicRequest.Name);
        response.CompressionAlgorithm.ShouldBe(TopicRequest.CompressionAlgorithm);
        response.Partitions!.Count().ShouldBe((int)TopicRequest.PartitionsCount);
        response.MessageExpiry.ShouldBe(TopicRequest.MessageExpiry);
        response.Size.ShouldBe(0u);
        response.PartitionsCount.ShouldBe(TopicRequest.PartitionsCount);
        response.ReplicationFactor.ShouldBe(TopicRequest.ReplicationFactor);
        response.MaxTopicSize.ShouldBe(TopicRequest.MaxTopicSize);
        response.MessagesCount.ShouldBe(0u);
    }

    [Test]
    [DependsOn(nameof(Get_ExistingTopic_Should_ReturnValidResponse))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task Get_ExistingTopic_ByName_Should_ReturnValidResponse(Protocol protocol)
    {
        var response = await Fixture.Clients[protocol]
            .GetTopicByIdAsync(Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
                Identifier.String(TopicRequest.Name));

        response.ShouldNotBeNull();
        response.Id.ShouldBeGreaterThanOrEqualTo(0u);
        response.CreatedAt.UtcDateTime.ShouldBe(DateTimeOffset.UtcNow.UtcDateTime, TimeSpan.FromMinutes(1));
        response.Name.ShouldBe(TopicRequest.Name);
        response.CompressionAlgorithm.ShouldBe(TopicRequest.CompressionAlgorithm);
        response.Partitions!.Count().ShouldBe((int)TopicRequest.PartitionsCount);
        response.MessageExpiry.ShouldBe(TopicRequest.MessageExpiry);
        response.Size.ShouldBe(0u);
        response.PartitionsCount.ShouldBe(TopicRequest.PartitionsCount);
        response.ReplicationFactor.ShouldBe(TopicRequest.ReplicationFactor);
        response.MaxTopicSize.ShouldBe(TopicRequest.MaxTopicSize);
        response.MessagesCount.ShouldBe(0u);
    }

    [Test]
    [DependsOn(nameof(Get_ExistingTopic_ByName_Should_ReturnValidResponse))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task Get_ExistingTopics_Should_ReturnValidResponse(Protocol protocol)
    {
        await Fixture.Clients[protocol].CreateTopicAsync(Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
            TopicRequestSecond.Name, TopicRequestSecond.PartitionsCount, TopicRequestSecond.CompressionAlgorithm,
            TopicRequestSecond.ReplicationFactor, TopicRequestSecond.MessageExpiry,
            TopicRequestSecond.MaxTopicSize);

        IReadOnlyList<TopicResponse> response = await Fixture.Clients[protocol]
            .GetTopicsAsync(Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)));

        response.ShouldNotBeNull();
        response.Count().ShouldBe(2);
        response.Select(x => x.Name).ShouldContain(TopicRequest.Name);
        response.Select(x => x.Name).ShouldContain(TopicRequestSecond.Name);

        var firstTopic = response.First(x => x.Name == TopicRequest.Name);
        firstTopic.Id.ShouldBeGreaterThanOrEqualTo(0u);
        firstTopic.CreatedAt.UtcDateTime.ShouldBe(DateTimeOffset.UtcNow.UtcDateTime, TimeSpan.FromMinutes(1));
        firstTopic.Name.ShouldBe(TopicRequest.Name);
        firstTopic.CompressionAlgorithm.ShouldBe(TopicRequest.CompressionAlgorithm);
        firstTopic.Partitions.ShouldBeNull();
        firstTopic.MessageExpiry.ShouldBe(TopicRequest.MessageExpiry);
        firstTopic.Size.ShouldBe(0u);
        firstTopic.PartitionsCount.ShouldBe(TopicRequest.PartitionsCount);
        firstTopic.ReplicationFactor.ShouldBe(TopicRequest.ReplicationFactor);
        firstTopic.MaxTopicSize.ShouldBe(TopicRequest.MaxTopicSize);
        firstTopic.MessagesCount.ShouldBe(0u);

        var secondTopic = response.First(x => x.Name == TopicRequestSecond.Name);
        secondTopic.Id.ShouldBeGreaterThanOrEqualTo(0u);
        secondTopic.CreatedAt.UtcDateTime.ShouldBe(DateTimeOffset.UtcNow.UtcDateTime, TimeSpan.FromMinutes(1));
        secondTopic.Name.ShouldBe(TopicRequestSecond.Name);
        secondTopic.CompressionAlgorithm.ShouldBe(TopicRequestSecond.CompressionAlgorithm);
        secondTopic.Partitions.ShouldBeNull();
        secondTopic.MessageExpiry.ShouldBe(TopicRequestSecond.MessageExpiry);
        secondTopic.Size.ShouldBe(0u);
        secondTopic.PartitionsCount.ShouldBe(TopicRequestSecond.PartitionsCount);
        secondTopic.ReplicationFactor.ShouldBe(TopicRequestSecond.ReplicationFactor);
        secondTopic.MaxTopicSize.ShouldBe(TopicRequestSecond.MaxTopicSize);
        secondTopic.MessagesCount.ShouldBe(0u);
    }

    [Test]
    [DependsOn(nameof(Get_ExistingTopics_Should_ReturnValidResponse))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task Get_Topic_WithPartitions_Should_ReturnValidResponse(Protocol protocol)
    {
        await Fixture.Clients[protocol]
            .CreatePartitionsAsync(Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)), Identifier.String(TopicRequest.Name),
                2);

        for (var i = 0; i < 3; i++)
        {
            await Fixture.Clients[protocol]
                .SendMessagesAsync(Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)), Identifier.String(TopicRequest.Name),
                    Partitioning.None(), GetMessages(i + 2));
        }

        var response = await Fixture.Clients[protocol]
            .GetTopicByIdAsync(Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)), Identifier.String(TopicRequest.Name));

        response.ShouldNotBeNull();
        response.Id.ShouldBeGreaterThanOrEqualTo(0u);
        response.CreatedAt.UtcDateTime.ShouldBe(DateTimeOffset.UtcNow.UtcDateTime, TimeSpan.FromMinutes(1));
        response.Name.ShouldBe(TopicRequest.Name);
        response.CompressionAlgorithm.ShouldBe(TopicRequest.CompressionAlgorithm);
        response.Partitions!.Count().ShouldBe(3);
        response.MessageExpiry.ShouldBe(TopicRequest.MessageExpiry);
        response.Size.ShouldBe(630u);
        response.PartitionsCount.ShouldBe(3u);
        response.ReplicationFactor.ShouldBe(TopicRequest.ReplicationFactor);
        response.MaxTopicSize.ShouldBe(TopicRequest.MaxTopicSize);
        response.MessagesCount.ShouldBe(9u);
        response.Partitions.ShouldNotBeNull();
        response.Partitions.ShouldAllBe(x => x.MessagesCount > 0);
        response.Partitions.ShouldAllBe(x => x.CreatedAt > DateTimeOffset.UtcNow.AddMinutes(-5));
        response.Partitions.ShouldAllBe(x => x.SegmentsCount > 0);
        response.Partitions.ShouldAllBe(x => x.CurrentOffset > 0);
        response.Partitions.ShouldAllBe(x => x.Size > 0);
        response.Partitions.ShouldAllBe(x => x.Id >= 0);
    }

    [Test]
    [DependsOn(nameof(Get_Topic_WithPartitions_Should_ReturnValidResponse))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task Update_ExistingTopic_Should_UpdateTopic_Successfully(Protocol protocol)
    {
        var topicToUpdate = await Fixture.Clients[protocol]
            .CreateTopicAsync(Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)), "topic-to-update", 1);
        topicToUpdate.ShouldNotBeNull();

        await Should.NotThrowAsync(Fixture.Clients[protocol].UpdateTopicAsync(
            Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
            Identifier.Numeric(topicToUpdate.Id), UpdateTopicRequest.Name,
            UpdateTopicRequest.CompressionAlgorithm, UpdateTopicRequest.MaxTopicSize, UpdateTopicRequest.MessageExpiry,
            UpdateTopicRequest.ReplicationFactor));

        var result = await Fixture.Clients[protocol].GetTopicByIdAsync(
            Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
            Identifier.Numeric(topicToUpdate.Id));
        result.ShouldNotBeNull();
        result!.Name.ShouldBe(UpdateTopicRequest.Name);
        result.MessageExpiry.ShouldBe(UpdateTopicRequest.MessageExpiry);
        result.CompressionAlgorithm.ShouldBe(UpdateTopicRequest.CompressionAlgorithm);
        result.MaxTopicSize.ShouldBe(UpdateTopicRequest.MaxTopicSize);
        result.ReplicationFactor.ShouldBe(UpdateTopicRequest.ReplicationFactor);
    }

    [Test]
    [DependsOn(nameof(Update_ExistingTopic_Should_UpdateTopic_Successfully))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task Purge_ExistingTopic_Should_PurgeTopic_Successfully(Protocol protocol)
    {
        var beforePurge = await Fixture.Clients[protocol]
            .GetTopicByIdAsync(Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
                Identifier.String(TopicRequest.Name));

        beforePurge.ShouldNotBeNull();
        beforePurge.MessagesCount.ShouldBe(9u);
        beforePurge.Size.ShouldBeGreaterThan(0u);

        await Should.NotThrowAsync(Fixture.Clients[protocol].PurgeTopicAsync(
            Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
            Identifier.String(TopicRequest.Name)));

        var afterPurge = await Fixture.Clients[protocol]
            .GetTopicByIdAsync(Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
                Identifier.String(TopicRequest.Name));
        afterPurge.ShouldNotBeNull();
        afterPurge!.MessagesCount.ShouldBe(0u);
        afterPurge.Size.ShouldBe(0u);
    }

    [Test]
    [DependsOn(nameof(Purge_ExistingTopic_Should_PurgeTopic_Successfully))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task Delete_ExistingTopic_Should_DeleteTopic_Successfully(Protocol protocol)
    {
        var topicToDelete = await Fixture.Clients[protocol]
            .CreateTopicAsync(Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)), "topic-to-delete", 1);
        topicToDelete.ShouldNotBeNull();

        await Should.NotThrowAsync(Fixture.Clients[protocol].DeleteTopicAsync(
            Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
            Identifier.Numeric(topicToDelete.Id)));
    }

    [Test]
    [DependsOn(nameof(Delete_ExistingTopic_Should_DeleteTopic_Successfully))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task Delete_NonExistingTopic_Should_Throw_InvalidResponse(Protocol protocol)
    {
        await Should.ThrowAsync<IggyInvalidStatusCodeException>(Fixture.Clients[protocol].DeleteTopicAsync(
            Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
            Identifier.String("topic-to-delete")));
    }

    [Test]
    [DependsOn(nameof(Delete_NonExistingTopic_Should_Throw_InvalidResponse))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task Get_NonExistingTopic_Should_Throw_InvalidResponse(Protocol protocol)
    {
        var topic = await Fixture.Clients[protocol].GetTopicByIdAsync(
            Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
            Identifier.String("topic-to-delete"));

        topic.ShouldBeNull();
    }

    private static Message[] GetMessages(int count)
    {
        var messages = new List<Message>(count);
        for (var i = 0; i < count; i++)
        {
            messages.Add(new Message(Guid.NewGuid(), Encoding.UTF8.GetBytes($"Test message {i + 1}")));
        }

        return messages.ToArray();
    }
}
