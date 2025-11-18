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
using Apache.Iggy.Consumers;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;
using Apache.Iggy.Tests.Integrations.Attributes;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Shouldly;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Tests.Integrations;

public class IggyConsumerTests
{
    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerAssembly)]
    public required IggyServerFixture Fixture { get; init; }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task InitAsync_WithSingleConsumer_Should_Initialize_Successfully(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStreamWithMessages(client, protocol);

        var consumer = IggyConsumerBuilder
            .Create(client,
                Identifier.String(testStream.StreamId),
                Identifier.String(testStream.TopicId),
                Consumer.New(1))
            .WithPollingStrategy(PollingStrategy.Next())
            .WithBatchSize(10)
            .WithPartitionId(0)
            .Build();

        await Should.NotThrowAsync(() => consumer.InitAsync());
        await consumer.DisposeAsync();
    }

    [Test]
    [SkipHttp]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task InitAsync_WithConsumerGroup_Should_Initialize_Successfully(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStreamWithMessages(client, protocol);

        var consumer = IggyConsumerBuilder
            .Create(client,
                Identifier.String(testStream.StreamId),
                Identifier.String(testStream.TopicId),
                Consumer.Group("test-group-init"))
            .WithPollingStrategy(PollingStrategy.Next())
            .WithBatchSize(10)
            .WithConsumerGroup("test-group-init")
            .Build();

        await Should.NotThrowAsync(() => consumer.InitAsync());
        await consumer.DisposeAsync();
    }

    [Test]
    [SkipHttp]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task InitAsync_NewClient_Should_Initialize_Successfully(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStreamWithMessages(client, protocol);

        var clientAddress = Fixture.GetIggyAddress(protocol); ;

        var consumer = IggyConsumerBuilder
            .Create(Identifier.String(testStream.StreamId),
                Identifier.String(testStream.TopicId),
                Consumer.New(2))
            .WithConnection(protocol, clientAddress, "iggy", "iggy")
            .WithPollingStrategy(PollingStrategy.Next())
            .WithBatchSize(10)
            .WithConsumerGroup("test-group-init")
            .Build();

        await Should.NotThrowAsync(() => consumer.InitAsync());
        await consumer.DisposeAsync();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task InitAsync_CalledTwice_Should_NotThrow(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStreamWithMessages(client, protocol);

        var consumer = IggyConsumerBuilder
            .Create(client,
                Identifier.String(testStream.StreamId),
                Identifier.String(testStream.TopicId),
                Consumer.New(2))
            .WithPollingStrategy(PollingStrategy.Next())
            .WithBatchSize(10)
            .WithPartitionId(1)
            .Build();

        await consumer.InitAsync();
        await Should.NotThrowAsync(() => consumer.InitAsync());
        await consumer.DisposeAsync();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task ReceiveAsync_WithoutInit_Should_Throw_ConsumerNotInitializedException(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStreamWithMessages(client, protocol);

        var consumer = IggyConsumerBuilder
            .Create(client,
                Identifier.String(testStream.StreamId),
                Identifier.String(testStream.TopicId),
                Consumer.New(3))
            .WithPollingStrategy(PollingStrategy.Next())
            .WithBatchSize(10)
            .WithPartitionId(1)
            .Build();

        await Should.ThrowAsync<ConsumerNotInitializedException>(async () =>
        {
            IAsyncEnumerator<ReceivedMessage> enumerator = consumer.ReceiveAsync().GetAsyncEnumerator();
            await enumerator.MoveNextAsync();
        });

        await consumer.DisposeAsync();
    }

    [Test]
    [SkipHttp]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task InitAsync_WithConsumerGroup_Should_CreateGroup_WhenNotExists(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStreamWithMessages(client, protocol);

        var groupName = $"test-group-create-{Guid.NewGuid()}";
        var consumer = IggyConsumerBuilder
            .Create(client,
                Identifier.String(testStream.StreamId),
                Identifier.String(testStream.TopicId),
                Consumer.Group(groupName))
            .WithPollingStrategy(PollingStrategy.Next())
            .WithBatchSize(10)
            .WithConsumerGroup(groupName)
            .Build();

        await consumer.InitAsync();

        var group = await client.GetConsumerGroupByIdAsync(Identifier.String(testStream.StreamId),
            Identifier.String(testStream.TopicId),
            Identifier.String(groupName));

        group.ShouldNotBeNull();
        group.Name.ShouldBe(groupName);

        await consumer.DisposeAsync();
    }

    [Test]
    [SkipHttp]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task InitAsync_WithConsumerGroup_Should_Throw_WhenGroupNotExists_AndAutoCreateDisabled(
        Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStreamWithMessages(client, protocol);

        var groupName = $"nonexistent-group-{Guid.NewGuid()}";
        var consumer = IggyConsumerBuilder
            .Create(client,
                Identifier.String(testStream.StreamId),
                Identifier.String(testStream.TopicId),
                Consumer.Group(groupName))
            .WithPollingStrategy(PollingStrategy.Next())
            .WithBatchSize(10)
            .WithConsumerGroup(groupName, false)
            .Build();

        await Should.ThrowAsync<ConsumerGroupNotFoundException>(() => consumer.InitAsync());
        await consumer.DisposeAsync();
    }

    [Test]
    [SkipHttp]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task InitAsync_WithConsumerGroup_Should_JoinGroup_Successfully(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStreamWithMessages(client, protocol);

        var groupName = $"test-group-join-{Guid.NewGuid()}";

        await client.CreateConsumerGroupAsync(Identifier.String(testStream.StreamId),
            Identifier.String(testStream.TopicId),
            groupName);

        var consumer = IggyConsumerBuilder
            .Create(client,
                Identifier.String(testStream.StreamId),
                Identifier.String(testStream.TopicId),
                Consumer.Group(groupName))
            .WithPollingStrategy(PollingStrategy.Next())
            .WithBatchSize(10)
            .WithConsumerGroup(groupName, false)
            .Build();

        await Should.NotThrowAsync(() => consumer.InitAsync());
        await consumer.DisposeAsync();
    }

    [Test]
    [SkipHttp]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DisposeAsync_Should_LeaveConsumerGroup(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStreamWithMessages(client, protocol);

        var groupName = $"test-group-leave-{Guid.NewGuid()}";

        var consumer = IggyConsumerBuilder
            .Create(client,
                Identifier.String(testStream.StreamId),
                Identifier.String(testStream.TopicId),
                Consumer.Group(groupName))
            .WithPollingStrategy(PollingStrategy.Next())
            .WithBatchSize(10)
            .WithConsumerGroup(groupName)
            .Build();

        await consumer.InitAsync();
        await consumer.DisposeAsync();

        var group = await client.GetConsumerGroupByIdAsync(Identifier.String(testStream.StreamId),
            Identifier.String(testStream.TopicId),
            Identifier.String(groupName));

        group.ShouldNotBeNull();
        group.MembersCount.ShouldBe(0u);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task ReceiveAsync_WithSingleConsumer_Should_ReceiveMessages_Successfully(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStreamWithMessages(client, protocol);

        var consumer = IggyConsumerBuilder
            .Create(client,
                Identifier.String(testStream.StreamId),
                Identifier.String(testStream.TopicId),
                Consumer.New(10))
            .WithPollingStrategy(PollingStrategy.Next())
            .WithBatchSize(10)
            .WithPartitionId(1)
            .WithAutoCommitMode(AutoCommitMode.Disabled)
            .Build();

        await consumer.InitAsync();

        var receivedCount = 0;
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        await foreach (var message in consumer.ReceiveAsync(cts.Token))
        {
            message.ShouldNotBeNull();
            message.Message.ShouldNotBeNull();
            message.Message.Payload.ShouldNotBeNull();
            message.Status.ShouldBe(MessageStatus.Success);
            message.PartitionId.ShouldBe(1u);

            receivedCount++;
            if (receivedCount >= 10)
            {
                break;
            }
        }

        receivedCount.ShouldBeGreaterThanOrEqualTo(10);
        await consumer.DisposeAsync();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task ReceiveAsync_WithBatchSize_Should_RespectBatchSize(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStreamWithMessages(client, protocol);

        var batchSize = 5u;
        var consumer = IggyConsumerBuilder
            .Create(client,
                Identifier.String(testStream.StreamId),
                Identifier.String(testStream.TopicId),
                Consumer.New(11))
            .WithPollingStrategy(PollingStrategy.Next())
            .WithBatchSize(batchSize)
            .WithPartitionId(1)
            .WithAutoCommitMode(AutoCommitMode.Disabled)
            .Build();

        await consumer.InitAsync();

        var receivedCount = 0;
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        await foreach (var message in consumer.ReceiveAsync(cts.Token))
        {
            message.ShouldNotBeNull();
            receivedCount++;
            if (receivedCount >= batchSize)
            {
                break;
            }
        }

        receivedCount.ShouldBe((int)batchSize);
        await consumer.DisposeAsync();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task ReceiveAsync_WithPollingInterval_Should_RespectInterval(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStreamWithMessages(client, protocol);

        var pollingInterval = TimeSpan.FromMilliseconds(100);
        var consumer = IggyConsumerBuilder
            .Create(client,
                Identifier.String(testStream.StreamId),
                Identifier.String(testStream.TopicId),
                Consumer.New(12))
            .WithPollingStrategy(PollingStrategy.Next())
            .WithBatchSize(5)
            .WithPartitionId(1)
            .WithPollingInterval(pollingInterval)
            .WithAutoCommitMode(AutoCommitMode.Disabled)
            .Build();

        await consumer.InitAsync();

        var receivedCount = 0;
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        await foreach (var message in consumer.ReceiveAsync(cts.Token))
        {
            message.ShouldNotBeNull();
            receivedCount++;
            if (receivedCount >= 3)
            {
                break;
            }
        }

        receivedCount.ShouldBeGreaterThanOrEqualTo(3);
        await consumer.DisposeAsync();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task ReceiveAsync_WithAutoCommitAfterReceive_Should_StoreOffset(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStreamWithMessages(client, protocol);

        var consumerId = protocol == Protocol.Tcp ? 20 : 120;
        var consumer = IggyConsumerBuilder
            .Create(client,
                Identifier.String(testStream.StreamId),
                Identifier.String(testStream.TopicId),
                Consumer.New(consumerId))
            .WithPollingStrategy(PollingStrategy.First())
            .WithBatchSize(10)
            .WithPartitionId(1)
            .WithAutoCommitMode(AutoCommitMode.AfterReceive)
            .Build();

        await consumer.InitAsync();

        ulong lastOffset = 0;
        var receivedCount = 0;
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        await foreach (var message in consumer.ReceiveAsync(cts.Token))
        {
            if (receivedCount >= 5)
            {
                break;
            }

            lastOffset = message.CurrentOffset;
            receivedCount++;
        }

        await consumer.DisposeAsync();

        var offset = await client.GetOffsetAsync(Consumer.New(consumerId),
            Identifier.String(testStream.StreamId),
            Identifier.String(testStream.TopicId),
            1u);

        offset.ShouldNotBeNull();
        offset.StoredOffset.ShouldBe(4ul);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task ReceiveAsync_WithAutoCommitAfterPoll_Should_StoreOffset(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStreamWithMessages(client, protocol);

        var consumerId = protocol == Protocol.Tcp ? 21 : 121;
        var consumer = IggyConsumerBuilder
            .Create(client,
                Identifier.String(testStream.StreamId),
                Identifier.String(testStream.TopicId),
                Consumer.New(consumerId))
            .WithPollingStrategy(PollingStrategy.First())
            .WithBatchSize(5)
            .WithPartitionId(1)
            .WithAutoCommitMode(AutoCommitMode.Auto)
            .Build();

        await consumer.InitAsync();

        ulong lastOffset = 0;
        var receivedCount = 0;
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        await foreach (var message in consumer.ReceiveAsync(cts.Token))
        {
            lastOffset = message.CurrentOffset;
            receivedCount++;
            if (receivedCount >= 5)
            {
                break;
            }
        }

        await consumer.DisposeAsync();

        var offset = await client.GetOffsetAsync(Consumer.New(consumerId),
            Identifier.String(testStream.StreamId),
            Identifier.String(testStream.TopicId),
            1u);

        offset.ShouldNotBeNull();
        offset.StoredOffset.ShouldBe(4ul);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task StoreOffsetAsync_Should_StoreOffset_Successfully(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStreamWithMessages(client, protocol);

        var consumerId = protocol == Protocol.Tcp ? 30 : 130;
        var consumer = IggyConsumerBuilder
            .Create(client,
                Identifier.String(testStream.StreamId),
                Identifier.String(testStream.TopicId),
                Consumer.New(consumerId))
            .WithPollingStrategy(PollingStrategy.Next())
            .WithBatchSize(5)
            .WithPartitionId(1)
            .WithAutoCommitMode(AutoCommitMode.Disabled)
            .Build();

        await consumer.InitAsync();

        var testOffset = 42ul;
        await Should.NotThrowAsync(() => consumer.StoreOffsetAsync(testOffset, 1));

        var offset = await client.GetOffsetAsync(Consumer.New(consumerId),
            Identifier.String(testStream.StreamId),
            Identifier.String(testStream.TopicId),
            1u);

        offset.ShouldNotBeNull();
        offset.StoredOffset.ShouldBe(testOffset);

        await consumer.DisposeAsync();
    }

    [Test]
    [SkipHttp]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DeleteOffsetAsync_Should_DeleteOffset_Successfully(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStreamWithMessages(client, protocol);

        var consumerId = protocol == Protocol.Tcp ? 31 : 131;
        var consumer = IggyConsumerBuilder
            .Create(client,
                Identifier.String(testStream.StreamId),
                Identifier.String(testStream.TopicId),
                Consumer.New(consumerId))
            .WithPollingStrategy(PollingStrategy.Next())
            .WithBatchSize(5)
            .WithPartitionId(1)
            .WithAutoCommitMode(AutoCommitMode.Disabled)
            .Build();

        await consumer.InitAsync();

        var testOffset = 50ul;
        await consumer.StoreOffsetAsync(testOffset, 1);

        var offset = await client.GetOffsetAsync(Consumer.New(consumerId),
            Identifier.String(testStream.StreamId),
            Identifier.String(testStream.TopicId),
            1u);
        offset.ShouldNotBeNull();

        await Should.NotThrowAsync(() => consumer.DeleteOffsetAsync(1));

        var deletedOffset = await client.GetOffsetAsync(Consumer.New(consumerId),
            Identifier.String(testStream.StreamId),
            Identifier.String(testStream.TopicId),
            1u);

        deletedOffset.ShouldBeNull();

        await consumer.DisposeAsync();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DisposeAsync_Should_NotThrow(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStreamWithMessages(client, protocol);

        var consumer = IggyConsumerBuilder
            .Create(client,
                Identifier.String(testStream.StreamId),
                Identifier.String(testStream.TopicId),
                Consumer.New(40))
            .WithPollingStrategy(PollingStrategy.Next())
            .WithBatchSize(10)
            .WithPartitionId(1)
            .Build();

        await consumer.InitAsync();
        await Should.NotThrowAsync(async () => await consumer.DisposeAsync());
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DisposeAsync_CalledTwice_Should_NotThrow(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStreamWithMessages(client, protocol);

        var consumer = IggyConsumerBuilder
            .Create(client,
                Identifier.String(testStream.StreamId),
                Identifier.String(testStream.TopicId),
                Consumer.New(41))
            .WithPollingStrategy(PollingStrategy.Next())
            .WithBatchSize(10)
            .WithPartitionId(1)
            .Build();

        await consumer.InitAsync();
        await consumer.DisposeAsync();
        await Should.NotThrowAsync(async () => await consumer.DisposeAsync());
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DisposeAsync_WithoutInit_Should_NotThrow(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStreamWithMessages(client, protocol);

        var consumer = IggyConsumerBuilder
            .Create(client,
                Identifier.String(testStream.StreamId),
                Identifier.String(testStream.TopicId),
                Consumer.New(42))
            .WithPollingStrategy(PollingStrategy.Next())
            .WithBatchSize(10)
            .WithPartitionId(1)
            .Build();

        await Should.NotThrowAsync(async () => await consumer.DisposeAsync());
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task OnPollingError_Should_Fire_WhenPollingFails(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStreamWithMessages(client, protocol);

        var errorFired = false;
        Exception? capturedError = null;

        var consumer = IggyConsumerBuilder
            .Create(client,
                Identifier.String(testStream.StreamId),
                Identifier.String(testStream.TopicId),
                Consumer.New(50))
            .WithPollingStrategy(PollingStrategy.Next())
            .WithBatchSize(10)
            .WithPartitionId(999)
            .WithAutoCommitMode(AutoCommitMode.Disabled)
            .SubscribeOnPollingError(error =>
            {
                errorFired = true;
                capturedError = error.Exception;
                return Task.CompletedTask;
            })
            .Build();

        await consumer.InitAsync();

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));

        try
        {
            await foreach (var message in consumer.ReceiveAsync(cts.Token))
            {
                // Should not receive any messages
            }
        }
        catch (OperationCanceledException)
        {
        }

        errorFired.ShouldBeTrue();
        capturedError.ShouldNotBeNull();

        await consumer.DisposeAsync();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task ReceiveAsync_WithOffsetStrategy_Should_StartFromOffset(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStreamWithMessages(client, protocol);

        var startOffset = 10ul;
        var consumer = IggyConsumerBuilder
            .Create(client,
                Identifier.String(testStream.StreamId),
                Identifier.String(testStream.TopicId),
                Consumer.New(60))
            .WithPollingStrategy(PollingStrategy.Offset(startOffset))
            .WithBatchSize(5)
            .WithPartitionId(1)
            .WithAutoCommitMode(AutoCommitMode.Disabled)
            .Build();

        await consumer.InitAsync();

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var firstMessage = true;

        await foreach (var message in consumer.ReceiveAsync(cts.Token))
        {
            if (firstMessage)
            {
                message.CurrentOffset.ShouldBeGreaterThanOrEqualTo(startOffset);
                break;
            }
        }

        await consumer.DisposeAsync();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task ReceiveAsync_WithFirstStrategy_Should_StartFromBeginning(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStreamWithMessages(client, protocol);

        var consumer = IggyConsumerBuilder
            .Create(client,
                Identifier.String(testStream.StreamId),
                Identifier.String(testStream.TopicId),
                Consumer.New(61))
            .WithPollingStrategy(PollingStrategy.First())
            .WithBatchSize(1)
            .WithPartitionId(1)
            .WithAutoCommitMode(AutoCommitMode.Disabled)
            .Build();

        await consumer.InitAsync();

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        await foreach (var message in consumer.ReceiveAsync(cts.Token))
        {
            message.CurrentOffset.ShouldBe(0ul);
            break;
        }

        await consumer.DisposeAsync();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task ReceiveAsync_WithLastStrategy_Should_StartFromEnd(Protocol protocol)
    {
        var client = protocol == Protocol.Tcp
            ? await Fixture.CreateTcpClient()
            : await Fixture.CreateHttpClient();

        var testStream = await CreateTestStreamWithMessages(client, protocol);

        var consumer = IggyConsumerBuilder
            .Create(client,
                Identifier.String(testStream.StreamId),
                Identifier.String(testStream.TopicId),
                Consumer.New(62))
            .WithPollingStrategy(PollingStrategy.Last())
            .WithBatchSize(1)
            .WithPartitionId(1)
            .WithAutoCommitMode(AutoCommitMode.Disabled)
            .Build();

        await consumer.InitAsync();

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var messageReceived = false;

        await foreach (var message in consumer.ReceiveAsync(cts.Token))
        {
            message.CurrentOffset.ShouldBeGreaterThan(0ul);
            messageReceived = true;
            break;
        }

        messageReceived.ShouldBeTrue();
        await consumer.DisposeAsync();
    }

    private async Task<TestStreamInfo> CreateTestStreamWithMessages(IIggyClient client, Protocol protocol,
        uint partitionsCount = 5, int messagesPerPartition = 100)
    {
        var streamId = $"stream_{Guid.NewGuid()}_{protocol.ToString().ToLowerInvariant()}";
        var topicId = "test_topic";

        await client.CreateStreamAsync(streamId);
        await client.CreateTopicAsync(Identifier.String(streamId), topicId, partitionsCount);

        for (uint partitionId = 0; partitionId < partitionsCount; partitionId++)
        {
            var messages = new List<Message>();
            for (var i = 0; i < messagesPerPartition; i++)
            {
                var message = new Message(Guid.NewGuid(),
                    Encoding.UTF8.GetBytes($"Test message {i} for partition {partitionId}"));
                messages.Add(message);
            }

            await client.SendMessagesAsync(Identifier.String(streamId),
                Identifier.String(topicId),
                Partitioning.PartitionId((int)partitionId),
                messages);
        }

        return new TestStreamInfo(streamId, topicId, partitionsCount, messagesPerPartition);
    }

    private record TestStreamInfo(string StreamId, string TopicId, uint PartitionsCount, int MessagesPerPartition);
}
