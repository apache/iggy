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

using System.Text;
using Apache.Iggy.Contracts;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Headers;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Shouldly;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Tests.Integrations;

public class SendMessagesTests
{
    private static readonly string DummyJson = """
                                               {
                                                 "userId": 1,
                                                 "id": 1,
                                                 "title": "delete",
                                                 "completed": false
                                               }
                                               """;

    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerAssembly)]
    public required IggyServerFixture Fixture { get; init; }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendMessages_NoHeaders_Should_SendMessages_Successfully(Protocol protocol)
    {
        var (client, streamName, topicName) = await CreateStreamAndTopic(protocol);

        var messages = new Message[]
        {
            new(Guid.NewGuid(), Encoding.UTF8.GetBytes(DummyJson)),
            new(Guid.NewGuid(), Encoding.UTF8.GetBytes(DummyJson))
        };

        await Should.NotThrowAsync(() =>
            client.SendMessagesAsync(Identifier.String(streamName),
                Identifier.String(topicName), Partitioning.None(), messages));
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendMessages_NoHeaders_Should_Throw_InvalidResponse(Protocol protocol)
    {
        var (client, streamName, _) = await CreateStreamAndTopic(protocol);

        var messages = new Message[]
        {
            new(Guid.NewGuid(), Encoding.UTF8.GetBytes(DummyJson)),
            new(Guid.NewGuid(), Encoding.UTF8.GetBytes(DummyJson))
        };

        await Should.ThrowAsync<IggyInvalidStatusCodeException>(() =>
            client.SendMessagesAsync(Identifier.String(streamName),
                Identifier.Numeric(69), Partitioning.None(), messages));
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendMessages_WithHeaders_Should_SendMessages_Successfully(Protocol protocol)
    {
        var (client, streamName, topicName) = await CreateStreamAndTopic(protocol);

        var messages = new Message[]
        {
            new(Guid.NewGuid(), Encoding.UTF8.GetBytes(DummyJson),
                new Dictionary<HeaderKey, HeaderValue>
                {
                    { HeaderKey.FromString("header1"), HeaderValue.FromString("value1") },
                    { HeaderKey.FromString("header2"), HeaderValue.FromInt32(444) }
                }),
            new(Guid.NewGuid(), Encoding.UTF8.GetBytes(DummyJson),
                new Dictionary<HeaderKey, HeaderValue>
                {
                    { HeaderKey.FromString("header1"), HeaderValue.FromString("value1") },
                    { HeaderKey.FromString("header2"), HeaderValue.FromInt32(444) }
                })
        };

        await Should.NotThrowAsync(() =>
            client.SendMessagesAsync(Identifier.String(streamName),
                Identifier.String(topicName), Partitioning.None(), messages));
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendMessages_WithHeaders_Should_Throw_InvalidResponse(Protocol protocol)
    {
        var (client, streamName, _) = await CreateStreamAndTopic(protocol);

        var messages = new Message[]
        {
            new(Guid.NewGuid(), Encoding.UTF8.GetBytes(DummyJson),
                new Dictionary<HeaderKey, HeaderValue>
                {
                    { HeaderKey.FromString("header1"), HeaderValue.FromString("value1") },
                    { HeaderKey.FromString("header2"), HeaderValue.FromInt32(444) }
                }),
            new(Guid.NewGuid(), Encoding.UTF8.GetBytes(DummyJson),
                new Dictionary<HeaderKey, HeaderValue>
                {
                    { HeaderKey.FromString("header1"), HeaderValue.FromString("value1") },
                    { HeaderKey.FromString("header2"), HeaderValue.FromInt32(444) }
                })
        };

        await Should.ThrowAsync<IggyInvalidStatusCodeException>(() =>
            client.SendMessagesAsync(Identifier.String(streamName),
                Identifier.Numeric(69), Partitioning.None(), messages));
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendMessages_Should_ReturnConfirmation_WithStreamTopicPartitionAndBaseOffset(Protocol protocol)
    {
        var context = await CreateStreamAndTopicWithIds(protocol, 4);

        var response = await SendBatchAsync(context.Client, context.StreamName, context.TopicName,
            Partitioning.PartitionId(2), 2);

        var confirmation = response.Confirmations.ShouldHaveSingleItem();
        confirmation.StreamId.ShouldBe(context.StreamId);
        confirmation.TopicId.ShouldBe(context.TopicId);
        confirmation.PartitionId.ShouldBe(2u);
        confirmation.BaseOffset.ShouldBe(0u);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendMessages_Consecutive_Should_AdvanceBaseOffset_ByBatchSize(Protocol protocol)
    {
        var (client, streamName, topicName) = await CreateStreamAndTopic(protocol);

        var first = await SendBatchAsync(client, streamName, topicName, Partitioning.PartitionId(0), 3);
        var second = await SendBatchAsync(client, streamName, topicName, Partitioning.PartitionId(0), 2);
        var third = await SendBatchAsync(client, streamName, topicName, Partitioning.PartitionId(0), 1);

        first.Confirmations.ShouldHaveSingleItem().BaseOffset.ShouldBe(0u);
        second.Confirmations.ShouldHaveSingleItem().BaseOffset.ShouldBe(3u);
        third.Confirmations.ShouldHaveSingleItem().BaseOffset.ShouldBe(5u);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendMessages_Confirmation_Should_MatchPolledMessageOffsets(Protocol protocol)
    {
        var (client, streamName, topicName) = await CreateStreamAndTopic(protocol);

        await SendBatchAsync(client, streamName, topicName, Partitioning.PartitionId(0), 4);
        var response = await SendBatchAsync(client, streamName, topicName, Partitioning.PartitionId(0), 3);
        var confirmation = response.Confirmations.ShouldHaveSingleItem();

        var polled = await PollAsync(client, streamName, topicName, 0,
            PollingStrategy.Offset(confirmation.BaseOffset));

        polled.Messages.Count.ShouldBe(3);
        polled.Messages[0].Header.Offset.ShouldBe(confirmation.BaseOffset);
    }

    /// <summary>
    ///     Balanced partitioning is resolved client-side, so the confirmation is the only place the caller
    ///     learns where the batch landed. It must name the partition the messages actually poll back from.
    /// </summary>
    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendMessages_Balanced_Confirmation_Should_NameThePartition_MessagesLandedIn(Protocol protocol)
    {
        const uint partitionsCount = 4;
        var context = await CreateStreamAndTopicWithIds(protocol, partitionsCount);

        var response = await SendBatchAsync(context.Client, context.StreamName, context.TopicName,
            Partitioning.None(), 2);

        var confirmation = response.Confirmations.ShouldHaveSingleItem();
        confirmation.PartitionId.ShouldBeLessThan(partitionsCount);

        var polled = await PollAsync(context.Client, context.StreamName, context.TopicName, confirmation.PartitionId,
            PollingStrategy.Offset(0));
        polled.Messages.Count.ShouldBe(2);
        polled.Messages[0].Header.Offset.ShouldBe(confirmation.BaseOffset);
    }

    private static Task<SendMessagesResponse> SendBatchAsync(IIggyClient client, string streamName, string topicName,
        Partitioning partitioning, int count)
    {
        Message[] messages = Enumerable.Range(0, count)
            .Select(index => new Message(Guid.NewGuid(), Encoding.UTF8.GetBytes($"confirm-payload-{index}")))
            .ToArray();

        return client.SendMessagesAsync(Identifier.String(streamName), Identifier.String(topicName), partitioning,
            messages);
    }

    private static Task<PolledMessages> PollAsync(IIggyClient client, string streamName, string topicName,
        uint partitionId, PollingStrategy strategy)
    {
        return client.PollMessagesAsync(new MessageFetchRequest
        {
            Count = 100,
            AutoCommit = false,
            Consumer = Consumer.New(1),
            PartitionId = partitionId,
            PollingStrategy = strategy,
            StreamId = Identifier.String(streamName),
            TopicId = Identifier.String(topicName)
        });
    }

    private async Task<(IIggyClient client, string streamName, string topicName)> CreateStreamAndTopic(
        Protocol protocol)
    {
        var context = await CreateStreamAndTopicWithIds(protocol);

        return (context.Client, context.StreamName, context.TopicName);
    }

    private async Task<StreamTopicContext> CreateStreamAndTopicWithIds(Protocol protocol, uint partitionsCount = 1)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var streamName = $"send-msg-{Guid.NewGuid():N}";
        var topicName = "test-topic";

        var stream = await client.CreateStreamAsync(streamName);
        stream.ShouldNotBeNull();
        var topic = await client.CreateTopicAsync(Identifier.String(streamName), topicName, partitionsCount);
        topic.ShouldNotBeNull();

        return new StreamTopicContext(client, streamName, topicName, stream.Id, topic.Id);
    }

    private sealed record StreamTopicContext(
        IIggyClient Client,
        string StreamName,
        string TopicName,
        uint StreamId,
        uint TopicId);
}
