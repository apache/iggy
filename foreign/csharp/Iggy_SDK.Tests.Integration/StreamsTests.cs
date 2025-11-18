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
using Apache.Iggy.Tests.Integrations.Attributes;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Apache.Iggy.Tests.Integrations.Helpers;
using Shouldly;
using TUnit.Core.Logging;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Tests.Integrations;

public class StreamsTests
{
    private const string Name = "StreamTests";

    [ClassDataSource<StreamsFixture>(Shared = SharedType.PerClass)]
    public required StreamsFixture Fixture { get; init; }


    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task CreateStream_HappyPath_Should_CreateStream_Successfully(Protocol protocol)
    {
        var response = await Fixture.Clients[protocol]
            .CreateStreamAsync(Name.GetWithProtocol(protocol));

        response.ShouldNotBeNull();
        response.Id.ShouldBeGreaterThanOrEqualTo(0u);
        response.Name.ShouldBe(Name.GetWithProtocol(protocol));
        response.Size.ShouldBe(0u);
        response.CreatedAt.UtcDateTime.ShouldBe(DateTimeOffset.UtcNow.UtcDateTime, TimeSpan.FromMinutes(1));
        response.MessagesCount.ShouldBe(0u);
        response.TopicsCount.ShouldBe(0);
        response.Topics.ShouldBeEmpty();
    }

    [Test]
    [DependsOn(nameof(CreateStream_HappyPath_Should_CreateStream_Successfully))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task CreateStream_Duplicate_Should_Throw_InvalidResponse(Protocol protocol)
    {
        await Should.ThrowAsync<IggyInvalidStatusCodeException>(Fixture.Clients[protocol]
            .CreateStreamAsync(Name.GetWithProtocol(protocol)));
    }

    [Test]
    [DependsOn(nameof(CreateStream_Duplicate_Should_Throw_InvalidResponse))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetStreams_Should_ReturnValidResponse(Protocol protocol)
    {
        await Fixture.Clients[protocol].CreateStreamAsync("test-stream-2".GetWithProtocol(protocol));

        IReadOnlyList<StreamResponse> response = await Fixture.Clients[protocol].GetStreamsAsync();

        response.ShouldNotBeNull();
        response.Count.ShouldBeGreaterThanOrEqualTo(2);
        response.ShouldContain(x => x.Name == Name.GetWithProtocol(protocol));
        response.ShouldContain(x => x.Name == "test-stream-2".GetWithProtocol(protocol));
    }

    [Test]
    [DependsOn(nameof(GetStreams_Should_ReturnValidResponse))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetStreamById_Should_ReturnValidResponse(Protocol protocol)
    {
        var newStream = await Fixture.Clients[protocol].CreateStreamAsync("test-stream-3".GetWithProtocol(protocol));
        newStream.ShouldNotBeNull();

        var response = await Fixture.Clients[protocol]
            .GetStreamByIdAsync(Identifier.Numeric(newStream.Id));
        response.ShouldNotBeNull();
        response.Id.ShouldBe(newStream.Id);
        response.Name.ShouldBe(newStream.Name);
        response.Size.ShouldBe(0u);
        response.CreatedAt.UtcDateTime.ShouldBe(DateTimeOffset.UtcNow.UtcDateTime, TimeSpan.FromMinutes(1));
        response.MessagesCount.ShouldBe(0u);
        response.TopicsCount.ShouldBe(0);
        response.Topics.ShouldBeEmpty();
    }


    [Test]
    [DependsOn(nameof(GetStreamById_Should_ReturnValidResponse))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetStreams_ByStreamName_Should_ReturnValidResponse(Protocol protocol)
    {
        var name = "test-stream-3".GetWithProtocol(protocol);
        var response = await Fixture.Clients[protocol].GetStreamByIdAsync(Identifier.String(name));

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
    [DependsOn(nameof(GetStreams_ByStreamName_Should_ReturnValidResponse))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetStreamById_WithTopics_Should_ReturnValidResponse(Protocol protocol)
    {
        var topicRequest1 = TopicFactory.CreateTopic("Topic1", messageExpiry: 100_000);
        var topicRequest2 = TopicFactory.CreateTopic("Topic2", messageExpiry: 100_000);

        await Fixture.Clients[protocol].CreateTopicAsync(Identifier.String(Name.GetWithProtocol(protocol)),
            topicRequest1.Name, topicRequest1.PartitionsCount, messageExpiry: topicRequest1.MessageExpiry);
        await Fixture.Clients[protocol].CreateTopicAsync(Identifier.String(Name.GetWithProtocol(protocol)),
            topicRequest2.Name, topicRequest2.PartitionsCount, messageExpiry: topicRequest2.MessageExpiry);

        await Fixture.Clients[protocol].SendMessagesAsync(Identifier.String(Name.GetWithProtocol(protocol)),
            Identifier.String(topicRequest1.Name), Partitioning.None(),
            [
                new Message(Guid.NewGuid(), "Test message 1"u8.ToArray()),
                new Message(Guid.NewGuid(), "Test message 2"u8.ToArray()),
                new Message(Guid.NewGuid(), "Test message 3"u8.ToArray())
            ]);


        await Fixture.Clients[protocol].SendMessagesAsync(Identifier.String(Name.GetWithProtocol(protocol)),
            Identifier.String(topicRequest2.Name), Partitioning.None(), [
                new Message(Guid.NewGuid(), "Test message 4"u8.ToArray()),
                new Message(Guid.NewGuid(), "Test message 5"u8.ToArray()),
                new Message(Guid.NewGuid(), "Test message 6"u8.ToArray()),
                new Message(Guid.NewGuid(), "Test message 7"u8.ToArray())
            ]);

        var response = await Fixture.Clients[protocol]
            .GetStreamByIdAsync(Identifier.String(Name.GetWithProtocol(protocol)));
        response.ShouldNotBeNull();
        response.Id.ShouldBeGreaterThanOrEqualTo(0u);
        response.Name.ShouldBe(Name.GetWithProtocol(protocol));
        response.Size.ShouldBe(490u);
        response.CreatedAt.UtcDateTime.ShouldBe(DateTimeOffset.UtcNow.UtcDateTime, TimeSpan.FromMinutes(1));
        response.MessagesCount.ShouldBe(7u);
        response.TopicsCount.ShouldBe(2);
        response.Topics.Count().ShouldBe(2);

        var topic = response.Topics.First(x => x.Name == topicRequest1.Name);
        topic.CreatedAt.UtcDateTime.ShouldBe(DateTimeOffset.UtcNow.UtcDateTime, TimeSpan.FromMinutes(1));
        topic.Name.ShouldBe(topicRequest1.Name);
        topic.CompressionAlgorithm.ShouldBe(topicRequest1.CompressionAlgorithm);
        topic.Partitions.ShouldBeNull();
        topic.MessageExpiry.ShouldBe(topicRequest1.MessageExpiry);
        topic.Size.ShouldBe(210u);
        topic.PartitionsCount.ShouldBe(topicRequest1.PartitionsCount);
        topic.ReplicationFactor.ShouldBe(topicRequest1.ReplicationFactor);
        topic.MaxTopicSize.ShouldBeGreaterThan(0u);
        topic.MessagesCount.ShouldBe(3u);
    }

    [Test]
    [DependsOn(nameof(GetStreamById_WithTopics_Should_ReturnValidResponse))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task UpdateStream_Should_UpdateStream_Successfully(Protocol protocol)
    {
        var streamToUpdate = await Fixture.Clients[protocol]
            .CreateStreamAsync("stream-to-update".GetWithProtocol(protocol));
        streamToUpdate.ShouldNotBeNull();

        await Fixture.Clients[protocol].UpdateStreamAsync(Identifier.String(streamToUpdate.Name),
            "updated-test-stream".GetWithProtocol(protocol));

        var result = await Fixture.Clients[protocol]
            .GetStreamByIdAsync(Identifier.Numeric(streamToUpdate.Id));
        result.ShouldNotBeNull();
        result.Name.ShouldBe("updated-test-stream".GetWithProtocol(protocol));
    }

    [Test]
    [DependsOn(nameof(UpdateStream_Should_UpdateStream_Successfully))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task PurgeStream_Should_PurgeStream_Successfully(Protocol protocol)
    {
        // Ensure the stream has messages (created in previous steps) before purging
        var stream = await Fixture.Clients[protocol]
            .GetStreamByIdAsync(Identifier.String(Name.GetWithProtocol(protocol)));

        stream.ShouldNotBeNull();
        stream.MessagesCount.ShouldBe(7u);
        stream.TopicsCount.ShouldBe(2);

        await Should.NotThrowAsync(() =>
            Fixture.Clients[protocol].PurgeStreamAsync(Identifier.String(Name.GetWithProtocol(protocol))));

        stream = await Fixture.Clients[protocol]
            .GetStreamByIdAsync(Identifier.String(Name.GetWithProtocol(protocol)));
        stream.ShouldNotBeNull();
        stream.MessagesCount.ShouldBe(0u);
        stream.TopicsCount.ShouldBe(2);
    }

    [Test]
    [DependsOn(nameof(PurgeStream_Should_PurgeStream_Successfully))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DeleteStream_Should_DeleteStream_Successfully(Protocol protocol)
    {
        var streamToDelete = await Fixture.Clients[protocol]
            .CreateStreamAsync("stream-to-delete".GetWithProtocol(protocol));
        streamToDelete.ShouldNotBeNull();

        await Should.NotThrowAsync(() =>
            Fixture.Clients[protocol].DeleteStreamAsync(Identifier.Numeric(streamToDelete.Id)));
    }

    [Test]
    [DependsOn(nameof(DeleteStream_Should_DeleteStream_Successfully))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DeleteStream_NotExists_Should_Throw_InvalidResponse(Protocol protocol)
    {
        await Should.ThrowAsync<IggyInvalidStatusCodeException>(() =>
            Fixture.Clients[protocol].DeleteStreamAsync(Identifier.String("stream-to-delete".GetWithProtocol(protocol))));
    }

    [Test]
    [DependsOn(nameof(DeleteStream_NotExists_Should_Throw_InvalidResponse))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetStreamById_AfterDelete_Should_Throw_InvalidResponse(Protocol protocol)
    {
        var stream = await Fixture.Clients[protocol]
            .GetStreamByIdAsync(Identifier.String("stream-to-delete".GetWithProtocol(protocol)));

        stream.ShouldBeNull();
    }
}
