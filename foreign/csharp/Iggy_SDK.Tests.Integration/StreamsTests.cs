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

using Apache.Iggy.Contracts;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Messages;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Shouldly;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Tests.Integrations;

public class StreamsTests
{
    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerAssembly)]
    public required IggyServerFixture Fixture { get; init; }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task CreateStream_HappyPath_Should_CreateStream_Successfully(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var name = $"create-stream-{Guid.NewGuid():N}";
        var response = await client.CreateStreamAsync(name);

        response.ShouldNotBeNull();
        response.Id.ShouldBeGreaterThanOrEqualTo(0u);
        response.Name.ShouldBe(name);
        response.Size.ShouldBe(0u);
        response.CreatedAt.UtcDateTime.ShouldBe(DateTimeOffset.UtcNow.UtcDateTime, TimeSpan.FromMinutes(1));
        response.MessagesCount.ShouldBe(0u);
        response.TopicsCount.ShouldBe(0);
        response.Topics.ShouldBeEmpty();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task CreateStream_Duplicate_Should_Throw_InvalidResponse(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var name = $"dup-stream-{Guid.NewGuid():N}";
        await client.CreateStreamAsync(name);

        await Should.ThrowAsync<IggyInvalidStatusCodeException>(client.CreateStreamAsync(name));
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetStreams_Should_ReturnValidResponse(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var name1 = $"get-streams-1-{Guid.NewGuid():N}";
        var name2 = $"get-streams-2-{Guid.NewGuid():N}";
        await client.CreateStreamAsync(name1);
        await client.CreateStreamAsync(name2);

        IReadOnlyList<StreamResponse> response = await client.GetStreamsAsync();

        response.ShouldNotBeNull();
        response.Count.ShouldBeGreaterThanOrEqualTo(2);
        response.ShouldContain(x => x.Name == name1);
        response.ShouldContain(x => x.Name == name2);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetStreamById_Should_ReturnValidResponse(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var name = $"get-by-id-{Guid.NewGuid():N}";
        var newStream = await client.CreateStreamAsync(name);
        newStream.ShouldNotBeNull();

        var response = await client.GetStreamByIdAsync(Identifier.Numeric(newStream.Id));
        response.ShouldNotBeNull();
        response.Id.ShouldBe(newStream.Id);
        response.Name.ShouldBe(name);
        response.Size.ShouldBe(0u);
        response.CreatedAt.UtcDateTime.ShouldBe(DateTimeOffset.UtcNow.UtcDateTime, TimeSpan.FromMinutes(1));
        response.MessagesCount.ShouldBe(0u);
        response.TopicsCount.ShouldBe(0);
        response.Topics.ShouldBeEmpty();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetStreams_ByStreamName_Should_ReturnValidResponse(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var name = $"get-by-name-{Guid.NewGuid():N}";
        await client.CreateStreamAsync(name);

        var response = await client.GetStreamByIdAsync(Identifier.String(name));

        response.ShouldNotBeNull();
        response.Id.ShouldNotBe(0u);
        response.Name.ShouldBe(name);
        response.Size.ShouldBe(0u);
        response.CreatedAt.UtcDateTime.ShouldBe(DateTimeOffset.UtcNow.UtcDateTime, TimeSpan.FromMinutes(1));
        response.MessagesCount.ShouldBe(0u);
        response.TopicsCount.ShouldBe(0);
        response.Topics.ShouldBeEmpty();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetStreamById_WithTopics_Should_ReturnValidResponse(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var streamName = $"topics-stream-{Guid.NewGuid():N}";
        await client.CreateStreamAsync(streamName);

        var topicName1 = "Topic1";
        var topicName2 = "Topic2";
        await client.CreateTopicAsync(Identifier.String(streamName),
            topicName1, 1, messageExpiry: TimeSpan.FromHours(1));
        await client.CreateTopicAsync(Identifier.String(streamName),
            topicName2, 1, messageExpiry: TimeSpan.FromHours(1));

        await client.SendMessagesAsync(Identifier.String(streamName),
            Identifier.String(topicName1), Partitioning.None(),
            [
                new Message(Guid.NewGuid(), "Test message 1"u8.ToArray()),
                new Message(Guid.NewGuid(), "Test message 2"u8.ToArray()),
                new Message(Guid.NewGuid(), "Test message 3"u8.ToArray())
            ]);

        await client.SendMessagesAsync(Identifier.String(streamName),
            Identifier.String(topicName2), Partitioning.None(), [
                new Message(Guid.NewGuid(), "Test message 4"u8.ToArray()),
                new Message(Guid.NewGuid(), "Test message 5"u8.ToArray()),
                new Message(Guid.NewGuid(), "Test message 6"u8.ToArray()),
                new Message(Guid.NewGuid(), "Test message 7"u8.ToArray())
            ]);

        var response = await client.GetStreamByIdAsync(Identifier.String(streamName));
        response.ShouldNotBeNull();
        response.Id.ShouldBeGreaterThanOrEqualTo(0u);
        response.Name.ShouldBe(streamName);
        response.Size.ShouldBeGreaterThan(0u);
        response.CreatedAt.UtcDateTime.ShouldBe(DateTimeOffset.UtcNow.UtcDateTime, TimeSpan.FromMinutes(1));
        response.MessagesCount.ShouldBe(7u);
        response.TopicsCount.ShouldBe(2);
        response.Topics.Count().ShouldBe(2);

        var topic = response.Topics.First(x => x.Name == topicName1);
        topic.CreatedAt.UtcDateTime.ShouldBe(DateTimeOffset.UtcNow.UtcDateTime, TimeSpan.FromMinutes(1));
        topic.Name.ShouldBe(topicName1);
        topic.Partitions.ShouldBeNull();
        topic.MessageExpiry.ShouldBe(TimeSpan.FromHours(1));
        topic.Size.ShouldBeGreaterThan(0u);
        topic.PartitionsCount.ShouldBe(1u);
        topic.MaxTopicSize.ShouldBeGreaterThan(0u);
        topic.MessagesCount.ShouldBe(3u);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task UpdateStream_Should_UpdateStream_Successfully(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var name = $"update-stream-{Guid.NewGuid():N}";
        var updatedName = $"updated-stream-{Guid.NewGuid():N}";
        var streamToUpdate = await client.CreateStreamAsync(name);
        streamToUpdate.ShouldNotBeNull();

        await client.UpdateStreamAsync(Identifier.String(streamToUpdate.Name), updatedName);

        var result = await client.GetStreamByIdAsync(Identifier.Numeric(streamToUpdate.Id));
        result.ShouldNotBeNull();
        result.Name.ShouldBe(updatedName);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task PurgeStream_Should_PurgeStream_Successfully(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var streamName = $"purge-stream-{Guid.NewGuid():N}";
        await client.CreateStreamAsync(streamName);
        await client.CreateTopicAsync(Identifier.String(streamName), "purge-topic", 1);

        await client.SendMessagesAsync(Identifier.String(streamName),
            Identifier.String("purge-topic"), Partitioning.None(),
            [
                new Message(Guid.NewGuid(), "Test message 1"u8.ToArray()),
                new Message(Guid.NewGuid(), "Test message 2"u8.ToArray())
            ]);

        var stream = await client.GetStreamByIdAsync(Identifier.String(streamName));
        stream.ShouldNotBeNull();
        stream.MessagesCount.ShouldBe(2u);

        await Should.NotThrowAsync(() => client.PurgeStreamAsync(Identifier.String(streamName)));

        stream = await client.GetStreamByIdAsync(Identifier.String(streamName));
        stream.ShouldNotBeNull();
        stream.MessagesCount.ShouldBe(0u);
        stream.TopicsCount.ShouldBe(1);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DeleteStream_Should_DeleteStream_Successfully(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var name = $"del-stream-{Guid.NewGuid():N}";
        var streamToDelete = await client.CreateStreamAsync(name);
        streamToDelete.ShouldNotBeNull();

        await Should.NotThrowAsync(() => client.DeleteStreamAsync(Identifier.Numeric(streamToDelete.Id)));
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DeleteStream_NotExists_Should_Throw_InvalidResponse(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        await Should.ThrowAsync<IggyInvalidStatusCodeException>(() =>
            client.DeleteStreamAsync(Identifier.String($"nonexistent-{Guid.NewGuid():N}")));
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetStreamById_AfterDelete_Should_Throw_InvalidResponse(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var name = $"del-get-stream-{Guid.NewGuid():N}";
        var stream = await client.CreateStreamAsync(name);
        stream.ShouldNotBeNull();

        await client.DeleteStreamAsync(Identifier.Numeric(stream.Id));

        var result = await client.GetStreamByIdAsync(Identifier.String(name));
        result.ShouldBeNull();
    }
}
