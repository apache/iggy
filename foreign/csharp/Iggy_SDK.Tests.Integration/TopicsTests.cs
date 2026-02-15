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
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Messages;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Shouldly;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Tests.Integrations;

public class TopicsTests
{
    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerAssembly)]
    public required IggyServerFixture Fixture { get; init; }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task Create_NewTopic_Should_Return_Successfully(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var streamName = $"topic-create-{Guid.NewGuid():N}";
        await client.CreateStreamAsync(streamName);

        var response = await client.CreateTopicAsync(
            Identifier.String(streamName), "Test Topic", 2, CompressionAlgorithm.Gzip,
            1, TimeSpan.FromMinutes(10), 2_000_000_000);

        response.ShouldNotBeNull();
        response.Id.ShouldBeGreaterThanOrEqualTo(0u);
        response.CreatedAt.UtcDateTime.ShouldBe(DateTimeOffset.UtcNow.UtcDateTime, TimeSpan.FromMinutes(1));
        response.Name.ShouldBe("Test Topic");
        response.CompressionAlgorithm.ShouldBe(CompressionAlgorithm.Gzip);
        response.Partitions!.Count().ShouldBe(2);
        response.MessageExpiry.ShouldBe(TimeSpan.FromMinutes(10));
        response.Size.ShouldBe(0u);
        response.PartitionsCount.ShouldBe(2u);
        response.ReplicationFactor.ShouldBe((byte?)1);
        response.MaxTopicSize.ShouldBe(2_000_000_000u);
        response.MessagesCount.ShouldBe(0u);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task Create_DuplicateTopic_Should_Throw_InvalidResponse(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var streamName = $"topic-dup-{Guid.NewGuid():N}";
        await client.CreateStreamAsync(streamName);
        await client.CreateTopicAsync(Identifier.String(streamName), "Dup Topic", 1);

        await Should.ThrowAsync<IggyInvalidStatusCodeException>(
            client.CreateTopicAsync(Identifier.String(streamName), "Dup Topic", 1));
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task Get_ExistingTopic_Should_ReturnValidResponse(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var streamName = $"topic-get-{Guid.NewGuid():N}";
        await client.CreateStreamAsync(streamName);
        await client.CreateTopicAsync(Identifier.String(streamName), "Get Topic", 2,
            CompressionAlgorithm.Gzip, 1, TimeSpan.FromMinutes(10), 2_000_000_000);

        var response = await client.GetTopicByIdAsync(Identifier.String(streamName), Identifier.Numeric(0));

        response.ShouldNotBeNull();
        response.Id.ShouldBeGreaterThanOrEqualTo(0u);
        response.CreatedAt.UtcDateTime.ShouldBe(DateTimeOffset.UtcNow.UtcDateTime, TimeSpan.FromMinutes(1));
        response.Name.ShouldBe("Get Topic");
        response.CompressionAlgorithm.ShouldBe(CompressionAlgorithm.Gzip);
        response.Partitions!.Count().ShouldBe(2);
        response.MessageExpiry.ShouldBe(TimeSpan.FromMinutes(10));
        response.Size.ShouldBe(0u);
        response.PartitionsCount.ShouldBe(2u);
        response.ReplicationFactor.ShouldBe((byte?)1);
        response.MaxTopicSize.ShouldBe(2_000_000_000u);
        response.MessagesCount.ShouldBe(0u);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task Get_ExistingTopic_ByName_Should_ReturnValidResponse(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var streamName = $"topic-getname-{Guid.NewGuid():N}";
        await client.CreateStreamAsync(streamName);
        await client.CreateTopicAsync(Identifier.String(streamName), "Name Topic", 2,
            CompressionAlgorithm.Gzip, 1, TimeSpan.FromMinutes(10), 2_000_000_000);

        var response = await client.GetTopicByIdAsync(Identifier.String(streamName),
            Identifier.String("Name Topic"));

        response.ShouldNotBeNull();
        response.Id.ShouldBeGreaterThanOrEqualTo(0u);
        response.Name.ShouldBe("Name Topic");
        response.CompressionAlgorithm.ShouldBe(CompressionAlgorithm.Gzip);
        response.Partitions!.Count().ShouldBe(2);
        response.MessageExpiry.ShouldBe(TimeSpan.FromMinutes(10));
        response.Size.ShouldBe(0u);
        response.PartitionsCount.ShouldBe(2u);
        response.ReplicationFactor.ShouldBe((byte?)1);
        response.MaxTopicSize.ShouldBe(2_000_000_000u);
        response.MessagesCount.ShouldBe(0u);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task Get_ExistingTopics_Should_ReturnValidResponse(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var streamName = $"topic-list-{Guid.NewGuid():N}";
        await client.CreateStreamAsync(streamName);
        await client.CreateTopicAsync(Identifier.String(streamName), "List Topic 1", 2,
            CompressionAlgorithm.Gzip, 1, TimeSpan.FromMinutes(10), 2_000_000_000);
        await client.CreateTopicAsync(Identifier.String(streamName), "List Topic 2", 2,
            CompressionAlgorithm.Gzip, 1, TimeSpan.FromMinutes(10), 2_000_000_000);

        IReadOnlyList<TopicResponse> response = await client.GetTopicsAsync(Identifier.String(streamName));

        response.ShouldNotBeNull();
        response.Count().ShouldBe(2);
        response.Select(x => x.Name).ShouldContain("List Topic 1");
        response.Select(x => x.Name).ShouldContain("List Topic 2");
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task Get_Topic_WithPartitions_Should_ReturnValidResponse(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var streamName = $"topic-parts-{Guid.NewGuid():N}";
        await client.CreateStreamAsync(streamName);
        await client.CreateTopicAsync(Identifier.String(streamName), "Parts Topic", 1);

        await client.CreatePartitionsAsync(Identifier.String(streamName),
            Identifier.String("Parts Topic"), 2);

        for (var i = 0; i < 3; i++)
        {
            await client.SendMessagesAsync(Identifier.String(streamName),
                Identifier.String("Parts Topic"),
                Partitioning.None(), GetMessages(i + 2));
        }

        var response = await client.GetTopicByIdAsync(Identifier.String(streamName),
            Identifier.String("Parts Topic"));

        response.ShouldNotBeNull();
        response.Id.ShouldBeGreaterThanOrEqualTo(0u);
        response.Name.ShouldBe("Parts Topic");
        response.Partitions!.Count().ShouldBe(3);
        response.Size.ShouldBeGreaterThan(0u);
        response.PartitionsCount.ShouldBe(3u);
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
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task Update_ExistingTopic_Should_UpdateTopic_Successfully(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var streamName = $"topic-update-{Guid.NewGuid():N}";
        await client.CreateStreamAsync(streamName);
        var topicToUpdate = await client.CreateTopicAsync(Identifier.String(streamName), "topic-to-update", 1);
        topicToUpdate.ShouldNotBeNull();

        await Should.NotThrowAsync(client.UpdateTopicAsync(
            Identifier.String(streamName),
            Identifier.Numeric(topicToUpdate.Id), "Updated Topic",
            CompressionAlgorithm.Gzip, 3_000_000_000, TimeSpan.FromMinutes(10), 3));

        var result = await client.GetTopicByIdAsync(
            Identifier.String(streamName),
            Identifier.Numeric(topicToUpdate.Id));
        result.ShouldNotBeNull();
        result!.Name.ShouldBe("Updated Topic");
        result.MessageExpiry.ShouldBe(TimeSpan.FromMinutes(10));
        result.CompressionAlgorithm.ShouldBe(CompressionAlgorithm.Gzip);
        result.MaxTopicSize.ShouldBe(3_000_000_000u);
        result.ReplicationFactor.ShouldBe((byte?)3);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task Purge_ExistingTopic_Should_PurgeTopic_Successfully(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var streamName = $"topic-purge-{Guid.NewGuid():N}";
        await client.CreateStreamAsync(streamName);
        await client.CreateTopicAsync(Identifier.String(streamName), "Purge Topic", 1);

        await client.SendMessagesAsync(Identifier.String(streamName),
            Identifier.String("Purge Topic"), Partitioning.None(), GetMessages(5));

        var beforePurge = await client.GetTopicByIdAsync(Identifier.String(streamName),
            Identifier.String("Purge Topic"));
        beforePurge.ShouldNotBeNull();
        beforePurge.MessagesCount.ShouldBe(5u);
        beforePurge.Size.ShouldBeGreaterThan(0u);

        await Should.NotThrowAsync(client.PurgeTopicAsync(
            Identifier.String(streamName), Identifier.String("Purge Topic")));

        var afterPurge = await client.GetTopicByIdAsync(Identifier.String(streamName),
            Identifier.String("Purge Topic"));
        afterPurge.ShouldNotBeNull();
        afterPurge!.MessagesCount.ShouldBe(0u);
        afterPurge.Size.ShouldBe(0u);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task Delete_ExistingTopic_Should_DeleteTopic_Successfully(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var streamName = $"topic-del-{Guid.NewGuid():N}";
        await client.CreateStreamAsync(streamName);
        var topicToDelete = await client.CreateTopicAsync(Identifier.String(streamName), "topic-to-delete", 1);
        topicToDelete.ShouldNotBeNull();

        await Should.NotThrowAsync(client.DeleteTopicAsync(
            Identifier.String(streamName), Identifier.Numeric(topicToDelete.Id)));
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task Delete_NonExistingTopic_Should_Throw_InvalidResponse(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var streamName = $"topic-delnone-{Guid.NewGuid():N}";
        await client.CreateStreamAsync(streamName);

        await Should.ThrowAsync<IggyInvalidStatusCodeException>(client.DeleteTopicAsync(
            Identifier.String(streamName), Identifier.String("nonexistent-topic")));
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task Get_NonExistingTopic_Should_Throw_InvalidResponse(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var streamName = $"topic-getnone-{Guid.NewGuid():N}";
        await client.CreateStreamAsync(streamName);

        var topic = await client.GetTopicByIdAsync(
            Identifier.String(streamName), Identifier.String("nonexistent-topic"));

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
