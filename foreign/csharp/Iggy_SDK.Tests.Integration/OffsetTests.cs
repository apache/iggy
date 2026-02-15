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

using Apache.Iggy.Enums;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;
using Apache.Iggy.Tests.Integrations.Attributes;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Shouldly;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Tests.Integrations;

public class OffsetTests
{
    private const ulong SetOffset = 2;

    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerAssembly)]
    public required IggyServerFixture Fixture { get; init; }

    private async Task<(IIggyClient client, string streamName, string topicName)> CreateStreamWithMessages(
        Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var streamName = $"offset-{Guid.NewGuid():N}";
        var topicName = "test-topic";

        await client.CreateStreamAsync(streamName);
        await client.CreateTopicAsync(Identifier.String(streamName), topicName, 1);

        await client.SendMessagesAsync(Identifier.String(streamName),
            Identifier.String(topicName), Partitioning.None(),
            [
                new Message(Guid.NewGuid(), "Test message 1"u8.ToArray()),
                new Message(Guid.NewGuid(), "Test message 2"u8.ToArray()),
                new Message(Guid.NewGuid(), "Test message 3"u8.ToArray()),
                new Message(Guid.NewGuid(), "Test message 4"u8.ToArray())
            ]);

        return (client, streamName, topicName);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task StoreOffset_IndividualConsumer_Should_StoreOffset_Successfully(Protocol protocol)
    {
        var (client, streamName, topicName) = await CreateStreamWithMessages(protocol);

        await Should.NotThrowAsync(() => client
            .StoreOffsetAsync(Consumer.New("test-consumer"), Identifier.String(streamName),
                Identifier.String(topicName), SetOffset, 0));
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetOffset_IndividualConsumer_Should_GetOffset_Successfully(Protocol protocol)
    {
        var (client, streamName, topicName) = await CreateStreamWithMessages(protocol);

        await client.StoreOffsetAsync(Consumer.New("test-consumer"), Identifier.String(streamName),
            Identifier.String(topicName), SetOffset, 0);

        var offset = await client.GetOffsetAsync(Consumer.New("test-consumer"), Identifier.String(streamName),
            Identifier.String(topicName), 0);

        offset.ShouldNotBeNull();
        offset.StoredOffset.ShouldBe(SetOffset);
        offset.PartitionId.ShouldBe(0);
        offset.CurrentOffset.ShouldBe(3u);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task StoreOffset_ConsumerGroup_Should_StoreOffset_Successfully(Protocol protocol)
    {
        var (client, streamName, topicName) = await CreateStreamWithMessages(protocol);

        await client.CreateConsumerGroupAsync(Identifier.String(streamName),
            Identifier.String(topicName), "test_consumer_group");

        // Consumer group membership is per-connection. For TCP the same client must join.
        // For HTTP, a separate TCP client joins (HTTP is stateless and doesn't track membership).
        if (protocol == Protocol.Tcp)
        {
            await client.JoinConsumerGroupAsync(
                Identifier.String(streamName),
                Identifier.String(topicName), Identifier.String("test_consumer_group"));
        }
        else
        {
            var tcpClient = await Fixture.CreateTcpClient();
            await tcpClient.JoinConsumerGroupAsync(
                Identifier.String(streamName),
                Identifier.String(topicName), Identifier.String("test_consumer_group"));
        }

        await Should.NotThrowAsync(() => client
            .StoreOffsetAsync(Consumer.Group("test_consumer_group"), Identifier.String(streamName),
                Identifier.String(topicName), SetOffset, 0));
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetOffset_ConsumerGroup_Should_GetOffset_Successfully(Protocol protocol)
    {
        var (client, streamName, topicName) = await CreateStreamWithMessages(protocol);

        await client.CreateConsumerGroupAsync(Identifier.String(streamName),
            Identifier.String(topicName), "test_consumer_group");

        if (protocol == Protocol.Tcp)
        {
            await client.JoinConsumerGroupAsync(
                Identifier.String(streamName),
                Identifier.String(topicName), Identifier.String("test_consumer_group"));
        }
        else
        {
            var tcpClient = await Fixture.CreateTcpClient();
            await tcpClient.JoinConsumerGroupAsync(
                Identifier.String(streamName),
                Identifier.String(topicName), Identifier.String("test_consumer_group"));
        }

        await client.StoreOffsetAsync(Consumer.Group("test_consumer_group"), Identifier.String(streamName),
            Identifier.String(topicName), SetOffset, 0);

        var offset = await client.GetOffsetAsync(Consumer.Group("test_consumer_group"),
            Identifier.String(streamName), Identifier.String(topicName), 0);

        offset.ShouldNotBeNull();
        offset.StoredOffset.ShouldBe(SetOffset);
        offset.PartitionId.ShouldBe(0);
        offset.CurrentOffset.ShouldBe(3u);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetOffset_ConsumerGroup_ByName_Should_GetOffset_Successfully(Protocol protocol)
    {
        var (client, streamName, topicName) = await CreateStreamWithMessages(protocol);

        await client.CreateConsumerGroupAsync(Identifier.String(streamName),
            Identifier.String(topicName), "test_consumer_group");

        if (protocol == Protocol.Tcp)
        {
            await client.JoinConsumerGroupAsync(
                Identifier.String(streamName),
                Identifier.String(topicName), Identifier.String("test_consumer_group"));
        }
        else
        {
            var tcpClient = await Fixture.CreateTcpClient();
            await tcpClient.JoinConsumerGroupAsync(
                Identifier.String(streamName),
                Identifier.String(topicName), Identifier.String("test_consumer_group"));
        }

        await client.StoreOffsetAsync(Consumer.Group("test_consumer_group"), Identifier.String(streamName),
            Identifier.String(topicName), SetOffset, 0);

        var offset = await client.GetOffsetAsync(Consumer.Group("test_consumer_group"),
            Identifier.String(streamName), Identifier.String(topicName), 0);

        offset.ShouldNotBeNull();
        offset.StoredOffset.ShouldBe(SetOffset);
        offset.PartitionId.ShouldBe(0);
        offset.CurrentOffset.ShouldBe(3u);
    }

    [Test]
    [SkipHttp]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DeleteOffset_ConsumerGroup_Should_DeleteOffset_Successfully(Protocol protocol)
    {
        var (client, streamName, topicName) = await CreateStreamWithMessages(protocol);

        await client.CreateConsumerGroupAsync(Identifier.String(streamName),
            Identifier.String(topicName), "test_consumer_group");

        await client.JoinConsumerGroupAsync(
            Identifier.String(streamName),
            Identifier.String(topicName), Identifier.String("test_consumer_group"));

        await client.StoreOffsetAsync(Consumer.Group("test_consumer_group"), Identifier.String(streamName),
            Identifier.String(topicName), SetOffset, 0);

        await client.DeleteOffsetAsync(Consumer.Group("test_consumer_group"),
            Identifier.String(streamName), Identifier.String(topicName), 0);

        var offset = await client.GetOffsetAsync(Consumer.Group("test_consumer_group"),
            Identifier.String(streamName), Identifier.String(topicName), 0);

        offset.ShouldBeNull();
    }
}
