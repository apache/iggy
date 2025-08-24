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
using Apache.Iggy.Tests.Integrations.Helpers;
using Shouldly;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Tests.Integrations;

public class StreamsTests
{
    private const string Name = "test-stream";
    private const uint StreamId = 1;


    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerClass)]
    public required IggyServerFixture Fixture { get; init; }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task CreateStream_HappyPath_Should_CreateStream_Successfully(Protocol protocol)
    {
        var response = await Fixture.Clients[protocol].CreateStreamAsync(Name, StreamId);

        response.ShouldNotBeNull();
        response.Id.ShouldBe(StreamId);
        response.Name.ShouldBe(Name);
        response.Size.ShouldBe(0u);
        response.CreatedAt.UtcDateTime.ShouldBe(DateTimeOffset.UtcNow.UtcDateTime, TimeSpan.FromSeconds(20));
        response.MessagesCount.ShouldBe(0u);
        response.TopicsCount.ShouldBe(0);
        response.Topics.ShouldBeEmpty();
    }

    [Test]
    [DependsOn(nameof(CreateStream_HappyPath_Should_CreateStream_Successfully))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task CreateStream_Duplicate_Should_Throw_InvalidResponse(Protocol protocol)
    {
        await Should.ThrowAsync<InvalidResponseException>(Fixture.Clients[protocol].CreateStreamAsync(Name, StreamId));
    }

    [Test]
    [DependsOn(nameof(CreateStream_Duplicate_Should_Throw_InvalidResponse))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetStreams_Should_ReturnValidResponse(Protocol protocol)
    {
        await Fixture.Clients[protocol].CreateStreamAsync("Test Stream 2", 2);

        IReadOnlyList<StreamResponse> response = await Fixture.Clients[protocol].GetStreamsAsync();

        response.ShouldNotBeNull();
        response.Count.ShouldBe(2);
        response.ShouldContain(x => x.Id == 1);
        response.ShouldContain(x => x.Id == 2);
    }

    [Test]
    [DependsOn(nameof(GetStreams_Should_ReturnValidResponse))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetStreamById_Should_ReturnValidResponse(Protocol protocol)
    {
        var response = await Fixture.Clients[protocol].GetStreamByIdAsync(Identifier.Numeric(StreamId));
        response.ShouldNotBeNull();
        response.Id.ShouldBe(StreamId);
        response.Name.ShouldBe(Name);
        response.Size.ShouldBe(0u);
        response.CreatedAt.UtcDateTime.ShouldBe(DateTimeOffset.UtcNow.UtcDateTime, TimeSpan.FromSeconds(20));
        response.MessagesCount.ShouldBe(0u);
        response.TopicsCount.ShouldBe(0);
        response.Topics.ShouldBeEmpty();
    }

    [Test]
    [DependsOn(nameof(GetStreamById_Should_ReturnValidResponse))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetStreamById_WithTopics_Should_ReturnValidResponse(Protocol protocol)
    {
        var topicRequest1 = TopicFactory.CreateTopic(messageExpiry: 100_000);
        var topicRequest2 = TopicFactory.CreateTopic(2, messageExpiry: 100_000);

        await Fixture.Clients[protocol].CreateTopicAsync(Identifier.Numeric(StreamId), topicRequest1.Name,
            topicRequest1.PartitionsCount, messageExpiry: topicRequest1.MessageExpiry, topicId: topicRequest1.TopicId);
        await Fixture.Clients[protocol].CreateTopicAsync(Identifier.Numeric(StreamId), topicRequest2.Name,
            topicRequest2.PartitionsCount, messageExpiry: topicRequest2.MessageExpiry, topicId: topicRequest2.TopicId);


        await Fixture.Clients[protocol].SendMessagesAsync(new MessageSendRequest
        {
            Partitioning = Partitioning.None(),
            StreamId = Identifier.Numeric(1),
            TopicId = Identifier.Numeric(topicRequest1.TopicId!.Value),
            Messages = new List<Message>
            {
                new(Guid.NewGuid(), "Test message 1"u8.ToArray()),
                new(Guid.NewGuid(), "Test message 2"u8.ToArray()),
                new(Guid.NewGuid(), "Test message 3"u8.ToArray())
            }
        });

        await Fixture.Clients[protocol].SendMessagesAsync(new MessageSendRequest
        {
            Partitioning = Partitioning.None(),
            StreamId = Identifier.Numeric(1),
            TopicId = Identifier.Numeric(2),
            Messages = new List<Message>
            {
                new(Guid.NewGuid(), "Test message 4"u8.ToArray()),
                new(Guid.NewGuid(), "Test message 5"u8.ToArray()),
                new(Guid.NewGuid(), "Test message 6"u8.ToArray()),
                new(Guid.NewGuid(), "Test message 7"u8.ToArray())
            }
        });

        var response = await Fixture.Clients[protocol].GetStreamByIdAsync(Identifier.Numeric(StreamId));
        response.ShouldNotBeNull();
        response.Id.ShouldBe(StreamId);
        response.Name.ShouldBe(Name);
        response.Size.ShouldBe(490u);
        response.CreatedAt.UtcDateTime.ShouldBe(DateTimeOffset.UtcNow.UtcDateTime, TimeSpan.FromSeconds(20));
        response.MessagesCount.ShouldBe(7u);
        response.TopicsCount.ShouldBe(2);
        response.Topics.Count().ShouldBe(2);

        var topic = response.Topics.First(x => x.Id == topicRequest1.TopicId!.Value);
        topic.Id.ShouldBe(topicRequest1.TopicId!.Value);
        topic.CreatedAt.UtcDateTime.ShouldBe(DateTimeOffset.UtcNow.UtcDateTime, TimeSpan.FromSeconds(10));
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
        await Fixture.Clients[protocol].UpdateStreamAsync(Identifier.Numeric(StreamId), "updated-test-stream");

        var result = await Fixture.Clients[protocol].GetStreamByIdAsync(Identifier.Numeric(StreamId!));
        result.ShouldNotBeNull();
        result.Name.ShouldBe("updated-test-stream");
    }

    [Test]
    [DependsOn(nameof(UpdateStream_Should_UpdateStream_Successfully))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task PurgeStream_Should_PurgeStream_Successfully(Protocol protocol)
    {
        // Ensure the stream has messages (created in previous steps) before purging
        var stream = await Fixture.Clients[protocol].GetStreamByIdAsync(Identifier.Numeric(StreamId!));
        stream.ShouldNotBeNull();
        stream.MessagesCount.ShouldBe(7u);
        stream.TopicsCount.ShouldBe(2);

        await Should.NotThrowAsync(() => Fixture.Clients[protocol].PurgeStreamAsync(Identifier.Numeric(StreamId)));

        stream = await Fixture.Clients[protocol].GetStreamByIdAsync(Identifier.Numeric(StreamId));
        stream.ShouldNotBeNull();
        stream.MessagesCount.ShouldBe(0u);
        stream.TopicsCount.ShouldBe(2);
    }

    [Test]
    [DependsOn(nameof(PurgeStream_Should_PurgeStream_Successfully))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DeleteStream_Should_DeleteStream_Successfully(Protocol protocol)
    {
        await Should.NotThrowAsync(() => Fixture.Clients[protocol].DeleteStreamAsync(Identifier.Numeric(StreamId)));
    }

    [Test]
    [DependsOn(nameof(DeleteStream_Should_DeleteStream_Successfully))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DeleteStream_NotExists_Should_Throw_InvalidResponse(Protocol protocol)
    {
        await Should.ThrowAsync<InvalidResponseException>(() =>
            Fixture.Clients[protocol].DeleteStreamAsync(Identifier.Numeric(StreamId)));
    }

    [Test]
    [DependsOn(nameof(DeleteStream_NotExists_Should_Throw_InvalidResponse))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetStreamById_AfterDelete_Should_Throw_InvalidResponse(Protocol protocol)
    {
        await Should.ThrowAsync<InvalidResponseException>(() =>
            Fixture.Clients[protocol].GetStreamByIdAsync(Identifier.Numeric(StreamId)));
    }
}