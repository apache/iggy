﻿// // Licensed to the Apache Software Foundation (ASF) under one
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
using Apache.Iggy.Contracts.Http;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Messages;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Apache.Iggy.Tests.Integrations.Helpers;
using Shouldly;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Tests.Integrations;

[MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
public class TopicsTests(Protocol protocol)
{
    private static readonly TopicRequest TopicRequest = new(1, "Test Topic", CompressionAlgorithm.Gzip, 1000, 1, 2, 2_000_000_000);
    private static readonly TopicRequest TopicRequestSecond = new(2, "Test Topic 2", CompressionAlgorithm.Gzip, 1000, 1, 2, 2_000_000_000);
    private static readonly UpdateTopicRequest UpdateTopicRequest = new("Updated Topic", CompressionAlgorithm.Gzip, 3_000_000_000, 2000, 3);

    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerClass)]
    public required IggyServerFixture Fixture { get; init; }

    [Test]
    public async Task Create_NewTopic_Should_Return_Successfully()
    {
        await Fixture.Clients[protocol].CreateStreamAsync(StreamFactory.CreateStream());

        var response = await Fixture.Clients[protocol].CreateTopicAsync(Identifier.Numeric(1), TopicRequest);

        response.ShouldNotBeNull();
        response.Id.ShouldBe(TopicRequest.TopicId!.Value);
        response.CreatedAt.UtcDateTime.ShouldBe(DateTimeOffset.UtcNow.UtcDateTime, TimeSpan.FromSeconds(10));
        response.Name.ShouldBe(TopicRequest.Name);
        response.CompressionAlgorithm.ShouldBe(TopicRequest.CompressionAlgorithm);
        response.Partitions!.Count().ShouldBe(TopicRequest.PartitionsCount);
        response.MessageExpiry.ShouldBe(TopicRequest.MessageExpiry);
        response.Size.ShouldBe(0u);
        response.PartitionsCount.ShouldBe(TopicRequest.PartitionsCount);
        response.ReplicationFactor.ShouldBe(TopicRequest.ReplicationFactor);
        response.MaxTopicSize.ShouldBe(TopicRequest.MaxTopicSize);
        response.MessagesCount.ShouldBe(0u);
    }

    [Test]
    [DependsOn(nameof(Create_NewTopic_Should_Return_Successfully))]
    public async Task Create_DuplicateTopic_Should_Throw_InvalidResponse()
    {
        await Should.ThrowAsync<InvalidResponseException>(Fixture.Clients[protocol].CreateTopicAsync(Identifier.Numeric(1), TopicRequest));
    }

    [Test]
    [DependsOn(nameof(Create_DuplicateTopic_Should_Throw_InvalidResponse))]
    public async Task Get_ExistingTopic_Should_ReturnValidResponse()
    {
        var response = await Fixture.Clients[protocol].GetTopicByIdAsync(Identifier.Numeric(1), Identifier.Numeric(1));

        response.ShouldNotBeNull();
        response.Id.ShouldBe(TopicRequest.TopicId!.Value);
        response.CreatedAt.UtcDateTime.ShouldBe(DateTimeOffset.UtcNow.UtcDateTime, TimeSpan.FromSeconds(10));
        response.Name.ShouldBe(TopicRequest.Name);
        response.CompressionAlgorithm.ShouldBe(TopicRequest.CompressionAlgorithm);
        response.Partitions!.Count().ShouldBe(TopicRequest.PartitionsCount);
        response.MessageExpiry.ShouldBe(TopicRequest.MessageExpiry);
        response.Size.ShouldBe(0u);
        response.PartitionsCount.ShouldBe(TopicRequest.PartitionsCount);
        response.ReplicationFactor.ShouldBe(TopicRequest.ReplicationFactor);
        response.MaxTopicSize.ShouldBe(TopicRequest.MaxTopicSize);
        response.MessagesCount.ShouldBe(0u);
    }

    [Test]
    [DependsOn(nameof(Get_ExistingTopic_Should_ReturnValidResponse))]
    public async Task Get_ExistingTopics_Should_ReturnValidResponse()
    {
        await Fixture.Clients[protocol].CreateTopicAsync(Identifier.Numeric(1), TopicRequestSecond);

        IReadOnlyList<TopicResponse> response = await Fixture.Clients[protocol].GetTopicsAsync(Identifier.Numeric(1));

        response.ShouldNotBeNull();
        response.Count().ShouldBe(2);
        response.Select(x => x.Id).ShouldContain(TopicRequest.TopicId!.Value);
        response.Select(x => x.Id).ShouldContain(TopicRequestSecond.TopicId!.Value);

        var firstTopic = response.First(x => x.Id == TopicRequest.TopicId!.Value);
        firstTopic.Id.ShouldBe(TopicRequest.TopicId!.Value);
        firstTopic.CreatedAt.UtcDateTime.ShouldBe(DateTimeOffset.UtcNow.UtcDateTime, TimeSpan.FromSeconds(10));
        firstTopic.Name.ShouldBe(TopicRequest.Name);
        firstTopic.CompressionAlgorithm.ShouldBe(TopicRequest.CompressionAlgorithm);
        firstTopic.Partitions.ShouldBeNull();
        firstTopic.MessageExpiry.ShouldBe(TopicRequest.MessageExpiry);
        firstTopic.Size.ShouldBe(0u);
        firstTopic.PartitionsCount.ShouldBe(TopicRequest.PartitionsCount);
        firstTopic.ReplicationFactor.ShouldBe(TopicRequest.ReplicationFactor);
        firstTopic.MaxTopicSize.ShouldBe(TopicRequest.MaxTopicSize);
        firstTopic.MessagesCount.ShouldBe(0u);

        var secondTopic = response.First(x => x.Id == TopicRequestSecond.TopicId!.Value);
        secondTopic.Id.ShouldBe(TopicRequestSecond.TopicId!.Value);
        secondTopic.CreatedAt.UtcDateTime.ShouldBe(DateTimeOffset.UtcNow.UtcDateTime, TimeSpan.FromSeconds(10));
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
    public async Task Get_Topic_WithPartitions_Should_ReturnValidResponse()
    {
        await Fixture.Clients[protocol].CreatePartitionsAsync(new CreatePartitionsRequest
        {
            PartitionsCount = 2,
            StreamId = Identifier.Numeric(1),
            TopicId = Identifier.Numeric(1)
        });

        for (var i = 0; i < 3; i++)
        {
            await Fixture.Clients[protocol].SendMessagesAsync(new MessageSendRequest
            {
                StreamId = Identifier.Numeric(1),
                TopicId = Identifier.Numeric(1),
                Partitioning = Partitioning.None(),
                Messages = GetMessages(i + 2)
            });
        }

        var response = await Fixture.Clients[protocol].GetTopicByIdAsync(Identifier.Numeric(1), Identifier.Numeric(1));

        response.ShouldNotBeNull();
        response.Id.ShouldBe(TopicRequest.TopicId!.Value);
        response.CreatedAt.UtcDateTime.ShouldBe(DateTimeOffset.UtcNow.UtcDateTime, TimeSpan.FromSeconds(10));
        response.Name.ShouldBe(TopicRequest.Name);
        response.CompressionAlgorithm.ShouldBe(TopicRequest.CompressionAlgorithm);
        response.Partitions!.Count().ShouldBe(3);
        response.MessageExpiry.ShouldBe(TopicRequest.MessageExpiry);
        response.Size.ShouldBe(630u);
        response.PartitionsCount.ShouldBe(3);
        response.ReplicationFactor.ShouldBe(TopicRequest.ReplicationFactor);
        response.MaxTopicSize.ShouldBe(TopicRequest.MaxTopicSize);
        response.MessagesCount.ShouldBe(9u);
        response.Partitions.ShouldNotBeNull();
        response.Partitions!.ShouldAllBe(x => x.MessagesCount > 0);
        response.Partitions.ShouldAllBe(x => x.CreatedAt > DateTimeOffset.UtcNow.AddMinutes(-5));
        response.Partitions.ShouldAllBe(x => x.SegmentsCount > 0);
        response.Partitions.ShouldAllBe(x => x.CurrentOffset > 0);
        response.Partitions.ShouldAllBe(x => x.Size > 0);
        response.Partitions.ShouldAllBe(x => x.Id > 0);
    }

    private static List<Message> GetMessages(int count)
    {
        var messages = new List<Message>();
        for (var i = 0; i < count; i++)
        {
            messages.Add(new Message(Guid.NewGuid(), Encoding.UTF8.GetBytes($"Test message {i + 1}")));
        }

        return messages;
    }

    [Test]
    [DependsOn(nameof(Get_Topic_WithPartitions_Should_ReturnValidResponse))]
    public async Task Update_ExistingTopic_Should_UpdateTopic_Successfully()
    {
        await Should.NotThrowAsync(Fixture.Clients[protocol].UpdateTopicAsync(Identifier.Numeric(1),
            Identifier.Numeric(TopicRequest.TopicId!.Value), UpdateTopicRequest));

        var result = await Fixture.Clients[protocol].GetTopicByIdAsync(Identifier.Numeric(1),
            Identifier.Numeric(TopicRequest.TopicId!.Value));
        result.ShouldNotBeNull();
        result!.Name.ShouldBe(UpdateTopicRequest.Name);
        result.MessageExpiry.ShouldBe(UpdateTopicRequest.MessageExpiry);
        result.CompressionAlgorithm.ShouldBe(UpdateTopicRequest.CompressionAlgorithm);
        result.MaxTopicSize.ShouldBe(UpdateTopicRequest.MaxTopicSize);
        result.ReplicationFactor.ShouldBe(UpdateTopicRequest.ReplicationFactor);
    }

    [Test]
    [DependsOn(nameof(Update_ExistingTopic_Should_UpdateTopic_Successfully))]
    public async Task Purge_ExistingTopic_Should_PurgeTopic_Successfully()
    {
        var beforePurge = await Fixture.Clients[protocol].GetTopicByIdAsync(Identifier.Numeric(1), Identifier.Numeric(TopicRequest.TopicId!.Value));

        beforePurge.ShouldNotBeNull();
        beforePurge.MessagesCount.ShouldBe(9u);
        beforePurge.Size.ShouldBeGreaterThan(0u);

        await Should.NotThrowAsync(Fixture.Clients[protocol].PurgeTopicAsync(Identifier.Numeric(1),
            Identifier.Numeric(TopicRequest.TopicId!.Value)));

        var afterPurge = await Fixture.Clients[protocol].GetTopicByIdAsync(Identifier.Numeric(1), Identifier.Numeric(TopicRequest.TopicId!.Value));
        afterPurge.ShouldNotBeNull();
        afterPurge!.MessagesCount.ShouldBe(0u);
        afterPurge.Size.ShouldBe(0u);
    }

    [Test]
    [DependsOn(nameof(Purge_ExistingTopic_Should_PurgeTopic_Successfully))]
    public async Task Delete_ExistingTopic_Should_DeleteTopic_Successfully()
    {
        await Should.NotThrowAsync(Fixture.Clients[protocol].DeleteTopicAsync(Identifier.Numeric(1),
            Identifier.Numeric(TopicRequest.TopicId!.Value)));
    }

    [Test]
    [DependsOn(nameof(Delete_ExistingTopic_Should_DeleteTopic_Successfully))]
    public async Task Delete_NonExistingTopic_Should_Throw_InvalidResponse()
    {
        await Should.ThrowAsync<InvalidResponseException>(Fixture.Clients[protocol].DeleteTopicAsync(Identifier.Numeric(1),
            Identifier.Numeric(TopicRequest.TopicId!.Value)));
    }

    [Test]
    [DependsOn(nameof(Delete_NonExistingTopic_Should_Throw_InvalidResponse))]
    public async Task Get_NonExistingTopic_Should_Throw_InvalidResponse()
    {
        await Should.ThrowAsync<InvalidResponseException>(Fixture.Clients[protocol].GetTopicByIdAsync(Identifier.Numeric(1),
            Identifier.Numeric(TopicRequest.TopicId!.Value)));
    }
}