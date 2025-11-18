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
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;
using Apache.Iggy.Publishers;
using Apache.Iggy.Tests.Integrations.Attributes;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Shouldly;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Tests.Integrations;

public class IggyPublisherTests
{
    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerAssembly)]
    public required IggyServerFixture Fixture { get; init; }

    private async Task<TestStreamInfo> CreateTestStream(IIggyClient client, Protocol protocol, uint partitionsCount = 5)
    {
        var streamId = $"stream_{Guid.NewGuid()}_{protocol.ToString().ToLowerInvariant()}";
        var topicId = "test_topic";

        await client.CreateStreamAsync(streamId);
        await client.CreateTopicAsync(Identifier.String(streamId), topicId, partitionsCount);

        return new TestStreamInfo(streamId, topicId, partitionsCount);
    }

    private record TestStreamInfo(string StreamId, string TopicId, uint PartitionsCount);

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task InitAsync_Should_Initialize_Successfully(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStream(client, protocol);

        var publisher = IggyPublisherBuilder
            .Create(client, Identifier.String(testStream.StreamId), Identifier.String(testStream.TopicId))
            .WithPartitioning(Partitioning.PartitionId(1))
            .Build();

        await Should.NotThrowAsync(() => publisher.InitAsync());
        await publisher.DisposeAsync();
    }

    [Test]
    [SkipHttp]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task InitAsync_NewClient_Should_Initialize_Successfully(Protocol protocol)
    {
        var client = Fixture.GetIggyAddress(protocol); ;

        var stream = Guid.NewGuid().ToString();
        var topic = Guid.NewGuid().ToString();

        var publisher = IggyPublisherBuilder
            .Create(Identifier.String(stream), Identifier.String(topic))
            .CreateStreamIfNotExists(stream)
            .CreateTopicIfNotExists(topic)
            .WithConnection(protocol, client, "iggy", "iggy")
            .WithPartitioning(Partitioning.PartitionId(1))
            .Build();

        await Should.NotThrowAsync(() => publisher.InitAsync());
        await publisher.DisposeAsync();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task InitAsync_CalledTwice_Should_NotThrow(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStream(client, protocol);

        var publisher = IggyPublisherBuilder
            .Create(client, Identifier.String(testStream.StreamId), Identifier.String(testStream.TopicId))
            .WithPartitioning(Partitioning.PartitionId(1))
            .Build();

        await publisher.InitAsync();
        await Should.NotThrowAsync(() => publisher.InitAsync());
        await publisher.DisposeAsync();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendMessages_WithoutInit_Should_Throw_PublisherNotInitializedException(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStream(client, protocol);

        var publisher = IggyPublisherBuilder
            .Create(client, Identifier.String(testStream.StreamId), Identifier.String(testStream.TopicId))
            .WithPartitioning(Partitioning.PartitionId(1))
            .Build();

        var messages = new List<Message>
        {
            new(Guid.NewGuid(), Encoding.UTF8.GetBytes("Test message"))
        };

        await Should.ThrowAsync<PublisherNotInitializedException>(() => publisher.SendMessages(messages));
        await publisher.DisposeAsync();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendMessages_Should_SendMessages_Successfully(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStream(client, protocol);

        var publisher = IggyPublisherBuilder
            .Create(client, Identifier.String(testStream.StreamId), Identifier.String(testStream.TopicId))
            .WithPartitioning(Partitioning.PartitionId(0))
            .Build();

        await publisher.InitAsync();

        var messages = new List<Message>();
        for (int i = 0; i < 10; i++)
        {
            messages.Add(new Message(Guid.NewGuid(), Encoding.UTF8.GetBytes($"Test message {i}")));
        }

        await Should.NotThrowAsync(() => publisher.SendMessages(messages));

        // Verify messages were sent by polling them
        var polledMessages = await client.PollMessagesAsync(
            Identifier.String(testStream.StreamId),
            Identifier.String(testStream.TopicId),
            null,
            Consumer.New(0),
            PollingStrategy.Next(),
            10,
            false);

        polledMessages.Messages.Count.ShouldBeGreaterThanOrEqualTo(10);
        await publisher.DisposeAsync();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendMessages_WithEmptyList_Should_NotThrow(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStream(client, protocol);

        var publisher = IggyPublisherBuilder
            .Create(client, Identifier.String(testStream.StreamId), Identifier.String(testStream.TopicId))
            .WithPartitioning(Partitioning.PartitionId(1))
            .Build();

        await publisher.InitAsync();

        var emptyMessages = new List<Message>();
        await Should.NotThrowAsync(() => publisher.SendMessages(emptyMessages));
        await publisher.DisposeAsync();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task InitAsync_WithStreamAutoCreate_Should_CreateStream(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var streamId = $"auto_stream_{Guid.NewGuid()}_{protocol.ToString().ToLowerInvariant()}";
        var topicId = "auto_topic";

        // First create topic manually to test only stream creation
        var publisher = IggyPublisherBuilder
            .Create(client, Identifier.String(streamId), Identifier.String(topicId))
            .CreateStreamIfNotExists(streamId)
            .CreateTopicIfNotExists(topicId, 3)
            .WithPartitioning(Partitioning.PartitionId(1))
            .Build();

        await publisher.InitAsync();

        // Verify stream was created
        var stream = await client.GetStreamByIdAsync(Identifier.String(streamId));
        stream.ShouldNotBeNull();
        stream.Name.ShouldBe(streamId);

        await publisher.DisposeAsync();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task InitAsync_WithTopicAutoCreate_Should_CreateTopic(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var streamId = $"stream_{Guid.NewGuid()}_{protocol.ToString().ToLowerInvariant()}";
        var topicId = "auto_topic";

        // Create stream first
        await client.CreateStreamAsync(streamId);

        var publisher = IggyPublisherBuilder
            .Create(client, Identifier.String(streamId), Identifier.String(topicId))
            .CreateTopicIfNotExists(topicId, 3)
            .WithPartitioning(Partitioning.PartitionId(1))
            .Build();

        await publisher.InitAsync();

        // Verify topic was created
        var topic = await client.GetTopicByIdAsync(Identifier.String(streamId), Identifier.String(topicId));
        topic.ShouldNotBeNull();
        topic.Name.ShouldBe(topicId);
        topic.PartitionsCount.ShouldBe(3u);

        await publisher.DisposeAsync();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task InitAsync_WithoutAutoCreate_Should_Throw_WhenStreamNotExists(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var streamId = $"nonexistent_stream_{Guid.NewGuid()}";
        var topicId = "test_topic";

        var publisher = IggyPublisherBuilder
            .Create(client, Identifier.String(streamId), Identifier.String(topicId))
            .WithPartitioning(Partitioning.PartitionId(1))
            .Build();

        await Should.ThrowAsync<StreamNotFoundException>(() => publisher.InitAsync());
        await publisher.DisposeAsync();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task InitAsync_WithoutAutoCreate_Should_Throw_WhenTopicNotExists(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var streamId = $"stream_{Guid.NewGuid()}_{protocol.ToString().ToLowerInvariant()}";
        var topicId = "nonexistent_topic";

        // Create stream but not topic
        await client.CreateStreamAsync(streamId);

        var publisher = IggyPublisherBuilder
            .Create(client, Identifier.String(streamId), Identifier.String(topicId))
            .WithPartitioning(Partitioning.PartitionId(1))
            .Build();

        await Should.ThrowAsync<TopicNotFoundException>(() => publisher.InitAsync());
        await publisher.DisposeAsync();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendMessages_WithBackgroundSending_Should_SendMessages_Successfully(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStream(client, protocol);

        var publisher = IggyPublisherBuilder
            .Create(client, Identifier.String(testStream.StreamId), Identifier.String(testStream.TopicId))
            .WithPartitioning(Partitioning.PartitionId(0))
            .WithBackgroundSending(true, queueCapacity: 1000, batchSize: 10, flushInterval: TimeSpan.FromMilliseconds(50))
            .Build();

        await publisher.InitAsync();

        var messages = new List<Message>();
        for (var i = 0; i < 50; i++)
        {
            messages.Add(new Message(Guid.NewGuid(), Encoding.UTF8.GetBytes($"Background message {i}")));
        }

        await publisher.SendMessages(messages);
        await publisher.WaitUntilAllSends();

        // Verify messages were sent
        var polledMessages = await client.PollMessagesAsync(
            Identifier.String(testStream.StreamId),
            Identifier.String(testStream.TopicId),
            null,
            Consumer.New(2),
            PollingStrategy.Next(),
            50,
            false);

        polledMessages.Messages.Count.ShouldBeGreaterThanOrEqualTo(50);
        await publisher.DisposeAsync();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task WaitUntilAllSends_Should_WaitForPendingMessages(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStream(client, protocol);

        var publisher = IggyPublisherBuilder
            .Create(client, Identifier.String(testStream.StreamId), Identifier.String(testStream.TopicId))
            .WithPartitioning(Partitioning.PartitionId(1))
            .WithBackgroundSending(true, queueCapacity: 1000, batchSize: 10, flushInterval: TimeSpan.FromMilliseconds(100))
            .Build();

        await publisher.InitAsync();

        var messages = new List<Message>();
        for (int i = 0; i < 20; i++)
        {
            messages.Add(new Message(Guid.NewGuid(), Encoding.UTF8.GetBytes($"Wait test message {i}")));
        }

        await publisher.SendMessages(messages);

        // Should not return immediately
        var startTime = DateTime.UtcNow;
        await publisher.WaitUntilAllSends();
        var elapsed = DateTime.UtcNow - startTime;

        // Should have taken some time to flush
        elapsed.ShouldBeGreaterThan(TimeSpan.Zero);

        await publisher.DisposeAsync();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task WaitUntilAllSends_WithoutBackgroundSending_Should_ReturnImmediately(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStream(client, protocol);

        var publisher = IggyPublisherBuilder
            .Create(client, Identifier.String(testStream.StreamId), Identifier.String(testStream.TopicId))
            .WithPartitioning(Partitioning.PartitionId(1))
            .WithBackgroundSending(false)
            .Build();

        await publisher.InitAsync();

        // Should return immediately without background sending
        await Should.NotThrowAsync(() => publisher.WaitUntilAllSends());
        await publisher.DisposeAsync();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendMessages_ToMultiplePartitions_Should_DistributeMessages(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStream(client, protocol, partitionsCount: 5);

        // Send messages to different partitions
        for (uint partitionId = 0; partitionId < 5; partitionId++)
        {
            var publisher = IggyPublisherBuilder
                .Create(client, Identifier.String(testStream.StreamId), Identifier.String(testStream.TopicId))
                .WithPartitioning(Partitioning.PartitionId((int)partitionId))
                .Build();

            await publisher.InitAsync();

            var messages = new List<Message>();
            for (int i = 0; i < 10; i++)
            {
                messages.Add(new Message(Guid.NewGuid(),
                    Encoding.UTF8.GetBytes($"Partition {partitionId} message {i}")));
            }

            await publisher.SendMessages(messages);
            await publisher.DisposeAsync();
        }

        // Verify messages are in different partitions
        for (uint partitionId = 0; partitionId < 5; partitionId++)
        {
            var polledMessages = await client.PollMessagesAsync(
                Identifier.String(testStream.StreamId),
                Identifier.String(testStream.TopicId),
                partitionId,
                Consumer.New((int)partitionId + 10),
                PollingStrategy.Next(),
                10,
                false);

            polledMessages.Messages.Count.ShouldBeGreaterThanOrEqualTo(10);
            polledMessages.PartitionId.ShouldBe((int)partitionId);
        }
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendMessages_WithBalancedPartitioning_Should_DistributeAcrossPartitions(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStream(client, protocol, partitionsCount: 3);

        var publisher = IggyPublisherBuilder
            .Create(client, Identifier.String(testStream.StreamId), Identifier.String(testStream.TopicId))
            .WithPartitioning(Partitioning.None())
            .Build();

        await publisher.InitAsync();

        var messages = new List<Message>();
        for (int i = 0; i < 30; i++)
        {
            messages.Add(new Message(Guid.NewGuid(), Encoding.UTF8.GetBytes($"Balanced message {i}")));
        }

        await publisher.SendMessages(messages);

        // Verify messages are distributed across partitions
        var totalMessages = 0;
        for (uint partitionId = 0; partitionId < 3; partitionId++)
        {
            var polledMessages = await client.PollMessagesAsync(
                Identifier.String(testStream.StreamId),
                Identifier.String(testStream.TopicId),
                partitionId,
                Consumer.New((int)partitionId + 20),
                PollingStrategy.Next(),
                30,
                false);

            totalMessages += polledMessages.Messages.Count;
        }

        totalMessages.ShouldBeGreaterThanOrEqualTo(30);
        await publisher.DisposeAsync();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DisposeAsync_Should_NotThrow(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStream(client, protocol);

        var publisher = IggyPublisherBuilder
            .Create(client, Identifier.String(testStream.StreamId), Identifier.String(testStream.TopicId))
            .WithPartitioning(Partitioning.PartitionId(1))
            .Build();

        await publisher.InitAsync();
        await Should.NotThrowAsync(async () => await publisher.DisposeAsync());
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DisposeAsync_CalledTwice_Should_NotThrow(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStream(client, protocol);

        var publisher = IggyPublisherBuilder
            .Create(client, Identifier.String(testStream.StreamId), Identifier.String(testStream.TopicId))
            .WithPartitioning(Partitioning.PartitionId(1))
            .Build();

        await publisher.InitAsync();
        await publisher.DisposeAsync();
        await Should.NotThrowAsync(async () => await publisher.DisposeAsync());
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DisposeAsync_WithoutInit_Should_NotThrow(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStream(client, protocol);

        var publisher = IggyPublisherBuilder
            .Create(client, Identifier.String(testStream.StreamId), Identifier.String(testStream.TopicId))
            .WithPartitioning(Partitioning.PartitionId(1))
            .Build();

        await Should.NotThrowAsync(async () => await publisher.DisposeAsync());
    }


    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task StreamId_Should_ReturnConfiguredStreamId(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStream(client, protocol);

        var publisher = IggyPublisherBuilder
            .Create(client, Identifier.String(testStream.StreamId), Identifier.String(testStream.TopicId))
            .WithPartitioning(Partitioning.PartitionId(1))
            .Build();

        publisher.StreamId.ToString().ShouldBe(testStream.StreamId);
        await publisher.DisposeAsync();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task TopicId_Should_ReturnConfiguredTopicId(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStream(client, protocol);

        var publisher = IggyPublisherBuilder
            .Create(client, Identifier.String(testStream.StreamId), Identifier.String(testStream.TopicId))
            .WithPartitioning(Partitioning.PartitionId(1))
            .Build();

        publisher.TopicId.ToString().ShouldBe(testStream.TopicId);
        await publisher.DisposeAsync();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendMessages_LargeMessageCount_Should_HandleCorrectly(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStream(client, protocol);

        var publisher = IggyPublisherBuilder
            .Create(client, Identifier.String(testStream.StreamId), Identifier.String(testStream.TopicId))
            .WithPartitioning(Partitioning.PartitionId(0))
            .WithBackgroundSending(true, queueCapacity: 1000, batchSize: 50)
            .Build();

        await publisher.InitAsync();

        var messages = new List<Message>();
        for (int i = 0; i < 200; i++)
        {
            messages.Add(new Message(Guid.NewGuid(), Encoding.UTF8.GetBytes($"Large batch message {i}")));
        }

        await publisher.SendMessages(messages);
        await publisher.WaitUntilAllSends();

        // Verify at least most messages were sent
        var polledMessages = await client.PollMessagesAsync(
            Identifier.String(testStream.StreamId),
            Identifier.String(testStream.TopicId),
            null,
            Consumer.New(40),
            PollingStrategy.Next(),
            200,
            false);

        polledMessages.Messages.Count.ShouldBeGreaterThanOrEqualTo(200);
        await publisher.DisposeAsync();
    }
}
