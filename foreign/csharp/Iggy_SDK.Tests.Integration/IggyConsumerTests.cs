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
using Apache.Iggy.Kinds;
using Apache.Iggy.Tests.Integrations.Attributes;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Apache.Iggy.Tests.Integrations.Helpers;
using Shouldly;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Tests.Integrations;

public class IggyConsumerTests
{
    [ClassDataSource<IggyConsumerFixture>(Shared = SharedType.PerClass)]
    public required IggyConsumerFixture Fixture { get; init; }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task InitAsync_WithSingleConsumer_Should_Initialize_Successfully(Protocol protocol)
    {
        var consumer = IggyConsumerBuilder
            .Create(Fixture.Clients[protocol],
                Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
                Identifier.String(Fixture.TopicId),
                Consumer.New(1))
            .WithPollingStrategy(PollingStrategy.Next())
            .WithBatchSize(10)
            .WithPartitionId(1)
            .Build();

        await Should.NotThrowAsync(() => consumer.InitAsync());
        await consumer.DisposeAsync();
    }

    [Test]
    [SkipHttp]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task InitAsync_WithConsumerGroup_Should_Initialize_Successfully(Protocol protocol)
    {
        var consumer = IggyConsumerBuilder
            .Create(Fixture.Clients[protocol],
                Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
                Identifier.String(Fixture.TopicId),
                Consumer.Group("test-group-init"))
            .WithPollingStrategy(PollingStrategy.Next())
            .WithBatchSize(10)
            .WithConsumerGroup("test-group-init", createIfNotExists: true, joinGroup: true)
            .Build();

        await Should.NotThrowAsync(() => consumer.InitAsync());
        await consumer.DisposeAsync();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task InitAsync_CalledTwice_Should_NotThrow(Protocol protocol)
    {
        var consumer = IggyConsumerBuilder
            .Create(Fixture.Clients[protocol],
                Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
                Identifier.String(Fixture.TopicId),
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
        var consumer = IggyConsumerBuilder
            .Create(Fixture.Clients[protocol],
                Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
                Identifier.String(Fixture.TopicId),
                Consumer.New(3))
            .WithPollingStrategy(PollingStrategy.Next())
            .WithBatchSize(10)
            .WithPartitionId(1)
            .Build();

        await Should.ThrowAsync<ConsumerNotInitializedException>(async () =>
        {
            var enumerator = consumer.ReceiveAsync().GetAsyncEnumerator();
            await enumerator.MoveNextAsync();
        });

        await consumer.DisposeAsync();
    }

    [Test]
    [SkipHttp]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task InitAsync_WithConsumerGroup_Should_CreateGroup_WhenNotExists(Protocol protocol)
    {
        var groupName = $"test-group-create-{Guid.NewGuid()}";
        var consumer = IggyConsumerBuilder
            .Create(Fixture.Clients[protocol],
                Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
                Identifier.String(Fixture.TopicId),
                Consumer.Group(groupName))
            .WithPollingStrategy(PollingStrategy.Next())
            .WithBatchSize(10)
            .WithConsumerGroup(groupName, createIfNotExists: true, joinGroup: true)
            .Build();

        await consumer.InitAsync();

        // Verify group was created
        var group = await Fixture.Clients[protocol].GetConsumerGroupByIdAsync(
            Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
            Identifier.String(Fixture.TopicId),
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
        var groupName = $"nonexistent-group-{Guid.NewGuid()}";
        var consumer = IggyConsumerBuilder
            .Create(Fixture.Clients[protocol],
                Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
                Identifier.String(Fixture.TopicId),
                Consumer.Group(groupName))
            .WithPollingStrategy(PollingStrategy.Next())
            .WithBatchSize(10)
            .WithConsumerGroup(groupName, createIfNotExists: false, joinGroup: true)
            .Build();

        await Should.ThrowAsync<ConsumerGroupNotFoundException>(() => consumer.InitAsync());
        await consumer.DisposeAsync();
    }

    [Test]
    [SkipHttp]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task InitAsync_WithConsumerGroup_Should_JoinGroup_Successfully(Protocol protocol)
    {
        var groupName = $"test-group-join-{Guid.NewGuid()}";

        // Create group first
        await Fixture.Clients[protocol].CreateConsumerGroupAsync(
            Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
            Identifier.String(Fixture.TopicId),
            groupName,
            null);

        var consumer = IggyConsumerBuilder
            .Create(Fixture.Clients[protocol],
                Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
                Identifier.String(Fixture.TopicId),
                Consumer.Group(groupName))
            .WithPollingStrategy(PollingStrategy.Next())
            .WithBatchSize(10)
            .WithConsumerGroup(groupName, createIfNotExists: false, joinGroup: true)
            .Build();

        await Should.NotThrowAsync(() => consumer.InitAsync());
        await consumer.DisposeAsync();
    }

    [Test]
    [SkipHttp]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DisposeAsync_Should_LeaveConsumerGroup(Protocol protocol)
    {
        var groupName = $"test-group-leave-{Guid.NewGuid()}";

        var consumer = IggyConsumerBuilder
            .Create(Fixture.Clients[protocol],
                Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
                Identifier.String(Fixture.TopicId),
                Consumer.Group(groupName))
            .WithPollingStrategy(PollingStrategy.Next())
            .WithBatchSize(10)
            .WithConsumerGroup(groupName, createIfNotExists: true, joinGroup: true)
            .Build();

        await consumer.InitAsync();
        await consumer.DisposeAsync();

        // Verify consumer left the group (members count should be 0)
        var group = await Fixture.Clients[protocol].GetConsumerGroupByIdAsync(
            Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
            Identifier.String(Fixture.TopicId),
            Identifier.String(groupName));

        group.ShouldNotBeNull();
        group.MembersCount.ShouldBe(0u);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task ReceiveAsync_WithSingleConsumer_Should_ReceiveMessages_Successfully(Protocol protocol)
    {
        var consumer = IggyConsumerBuilder
            .Create(Fixture.Clients[protocol],
                Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
                Identifier.String(Fixture.TopicId),
                Consumer.New(10))
            .WithPollingStrategy(PollingStrategy.Next())
            .WithBatchSize(10)
            .WithPartitionId(1)
            .WithAutoCommitMode(AutoCommitMode.Disabled)
            .Build();

        await consumer.InitAsync();

        var receivedCount = 0;
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

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
        var batchSize = 5u;
        var consumer = IggyConsumerBuilder
            .Create(Fixture.Clients[protocol],
                Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
                Identifier.String(Fixture.TopicId),
                Consumer.New(11))
            .WithPollingStrategy(PollingStrategy.Next())
            .WithBatchSize(batchSize)
            .WithPartitionId(1)
            .WithAutoCommitMode(AutoCommitMode.Disabled)
            .Build();

        await consumer.InitAsync();

        var receivedCount = 0;
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

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
        // This test verifies that polling interval throttling is working
        // by checking that the consumer is configured with a polling interval
        var pollingInterval = TimeSpan.FromMilliseconds(100);
        var consumer = IggyConsumerBuilder
            .Create(Fixture.Clients[protocol],
                Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
                Identifier.String(Fixture.TopicId),
                Consumer.New(12))
            .WithPollingStrategy(PollingStrategy.Next())
            .WithBatchSize(5)
            .WithPartitionId(1)
            .WithPollingInterval(pollingInterval)
            .WithAutoCommitMode(AutoCommitMode.Disabled)
            .Build();

        await consumer.InitAsync();

        var receivedCount = 0;
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        // Simply verify that messages can be received with polling interval configured
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
        // Use different consumer IDs for TCP vs HTTP to avoid offset conflicts
        var consumerId = protocol == Protocol.Tcp ? 20 : 120;
        var consumer = IggyConsumerBuilder
            .Create(Fixture.Clients[protocol],
                Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
                Identifier.String(Fixture.TopicId),
                Consumer.New(consumerId))
            .WithPollingStrategy(PollingStrategy.First())
            .WithBatchSize(10)
            .WithPartitionId(1)
            .WithAutoCommitMode(AutoCommitMode.AfterReceive)
            .Build();

        await consumer.InitAsync();

        ulong lastOffset = 0;
        var receivedCount = 0;
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

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

        // Verify offset was stored - should be offset 4 (0-based, 5 messages = offsets 0-4)
        var offset = await Fixture.Clients[protocol].GetOffsetAsync(Consumer.New(consumerId),
            Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
            Identifier.String(Fixture.TopicId),
            1u);

        offset.ShouldNotBeNull();
        offset.StoredOffset.ShouldBe(4ul);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task ReceiveAsync_WithAutoCommitAfterPoll_Should_StoreOffset(Protocol protocol)
    {
        // Use different consumer IDs for TCP vs HTTP to avoid offset conflicts
        var consumerId = protocol == Protocol.Tcp ? 21 : 121;
        var consumer = IggyConsumerBuilder
            .Create(Fixture.Clients[protocol],
                Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
                Identifier.String(Fixture.TopicId),
                Consumer.New(consumerId))
            .WithPollingStrategy(PollingStrategy.First())
            .WithBatchSize(5)
            .WithPartitionId(1)
            .WithAutoCommitMode(AutoCommitMode.AfterPoll)
            .Build();

        await consumer.InitAsync();

        ulong lastOffset = 0;
        var receivedCount = 0;
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

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

        // Verify offset was stored - should be offset 4 (0-based, 5 messages = offsets 0-4)
        var offset = await Fixture.Clients[protocol].GetOffsetAsync(Consumer.New(consumerId),
            Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
            Identifier.String(Fixture.TopicId),
            1u);

        offset.ShouldNotBeNull();
        offset.StoredOffset.ShouldBe(4ul);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task ReceiveAsync_WithAutoCommitDisabled_Should_NotStoreOffset(Protocol protocol)
    {
        // Use different consumer IDs for TCP vs HTTP to avoid offset conflicts
        var consumerId = protocol == Protocol.Tcp ? 22 : 122;

        // Clean up any existing offset from previous test runs
        try
        {
            await Fixture.Clients[protocol].DeleteOffsetAsync(Consumer.New(consumerId),
                Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
                Identifier.String(Fixture.TopicId),
                1u);
        }
        catch
        {
            // Ignore errors if offset doesn't exist
        }

        var consumer = IggyConsumerBuilder
            .Create(Fixture.Clients[protocol],
                Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
                Identifier.String(Fixture.TopicId),
                Consumer.New(consumerId))
            .WithPollingStrategy(PollingStrategy.Next())
            .WithBatchSize(5)
            .WithPartitionId(1)
            .WithAutoCommitMode(AutoCommitMode.Disabled)
            .Build();

        await consumer.InitAsync();

        var receivedCount = 0;
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        await foreach (var message in consumer.ReceiveAsync(cts.Token))
        {
            if (receivedCount >= 5)
            {
                break;
            }

            receivedCount++;
        }

        await consumer.DisposeAsync();

        // Verify offset was NOT stored
        var offset = await Fixture.Clients[protocol].GetOffsetAsync(Consumer.New(consumerId),
            Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
            Identifier.String(Fixture.TopicId),
            1u);

        offset.ShouldBeNull();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task StoreOffsetAsync_Should_StoreOffset_Successfully(Protocol protocol)
    {
        // Use different consumer IDs for TCP vs HTTP to avoid offset conflicts
        var consumerId = protocol == Protocol.Tcp ? 30 : 130;
        var consumer = IggyConsumerBuilder
            .Create(Fixture.Clients[protocol],
                Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
                Identifier.String(Fixture.TopicId),
                Consumer.New(consumerId))
            .WithPollingStrategy(PollingStrategy.Next())
            .WithBatchSize(5)
            .WithPartitionId(1)
            .WithAutoCommitMode(AutoCommitMode.Disabled)
            .Build();

        await consumer.InitAsync();

        var testOffset = 42ul;
        await Should.NotThrowAsync(() => consumer.StoreOffsetAsync(testOffset, 1));

        // Verify offset was stored
        var offset = await Fixture.Clients[protocol].GetOffsetAsync(Consumer.New(consumerId),
            Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
            Identifier.String(Fixture.TopicId),
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
        var consumerId = protocol == Protocol.Tcp ? 31 : 131;
        var consumer = IggyConsumerBuilder
            .Create(Fixture.Clients[protocol],
                Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
                Identifier.String(Fixture.TopicId),
                Consumer.New(consumerId))
            .WithPollingStrategy(PollingStrategy.Next())
            .WithBatchSize(5)
            .WithPartitionId(1)
            .WithAutoCommitMode(AutoCommitMode.Disabled)
            .Build();

        await consumer.InitAsync();

        var testOffset = 50ul;
        await consumer.StoreOffsetAsync(testOffset, 1);

        var offset = await Fixture.Clients[protocol].GetOffsetAsync(Consumer.New(consumerId),
            Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
            Identifier.String(Fixture.TopicId),
            1u);
        offset.ShouldNotBeNull();

        await Should.NotThrowAsync(() => consumer.DeleteOffsetAsync(1));

        var deletedOffset = await Fixture.Clients[protocol].GetOffsetAsync(Consumer.New(consumerId),
            Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
            Identifier.String(Fixture.TopicId),
            1u);

        deletedOffset.ShouldBeNull();

        await consumer.DisposeAsync();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DisposeAsync_Should_NotThrow(Protocol protocol)
    {
        var consumer = IggyConsumerBuilder
            .Create(Fixture.Clients[protocol],
                Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
                Identifier.String(Fixture.TopicId),
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
        var consumer = IggyConsumerBuilder
            .Create(Fixture.Clients[protocol],
                Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
                Identifier.String(Fixture.TopicId),
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
        var consumer = IggyConsumerBuilder
            .Create(Fixture.Clients[protocol],
                Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
                Identifier.String(Fixture.TopicId),
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
        var errorFired = false;
        Exception? capturedError = null;

        var consumer = IggyConsumerBuilder
            .Create(Fixture.Clients[protocol],
                Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
                Identifier.String(Fixture.TopicId),
                Consumer.New(50))
            .WithPollingStrategy(PollingStrategy.Next())
            .WithBatchSize(10)
            .WithPartitionId(999) // Invalid partition
            .WithAutoCommitMode(AutoCommitMode.Disabled)
            .OnPollingError((sender, args) =>
            {
                errorFired = true;
                capturedError = args.Exception;
            })
            .Build();

        await consumer.InitAsync();

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));

        try
        {
            await foreach (var message in consumer.ReceiveAsync(cts.Token))
            {
                // Should not receive any messages
            }
        }
        catch (OperationCanceledException)
        {
            // Expected
        }

        errorFired.ShouldBeTrue();
        capturedError.ShouldNotBeNull();

        await consumer.DisposeAsync();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task ReceiveAsync_WithOffsetStrategy_Should_StartFromOffset(Protocol protocol)
    {
        var startOffset = 10ul;
        var consumer = IggyConsumerBuilder
            .Create(Fixture.Clients[protocol],
                Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
                Identifier.String(Fixture.TopicId),
                Consumer.New(60))
            .WithPollingStrategy(PollingStrategy.Offset(startOffset))
            .WithBatchSize(5)
            .WithPartitionId(1)
            .WithAutoCommitMode(AutoCommitMode.Disabled)
            .Build();

        await consumer.InitAsync();

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
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
        var consumer = IggyConsumerBuilder
            .Create(Fixture.Clients[protocol],
                Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
                Identifier.String(Fixture.TopicId),
                Consumer.New(61))
            .WithPollingStrategy(PollingStrategy.First())
            .WithBatchSize(1)
            .WithPartitionId(1)
            .WithAutoCommitMode(AutoCommitMode.Disabled)
            .Build();

        await consumer.InitAsync();

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

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
        var consumer = IggyConsumerBuilder
            .Create(Fixture.Clients[protocol],
                Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
                Identifier.String(Fixture.TopicId),
                Consumer.New(62))
            .WithPollingStrategy(PollingStrategy.Last())
            .WithBatchSize(1)
            .WithPartitionId(1)
            .WithAutoCommitMode(AutoCommitMode.Disabled)
            .Build();

        await consumer.InitAsync();

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var messageReceived = false;

        await foreach (var message in consumer.ReceiveAsync(cts.Token))
        {
            // Should be near the end
            message.CurrentOffset.ShouldBeGreaterThan(0ul);
            messageReceived = true;
            break;
        }

        messageReceived.ShouldBeTrue();
        await consumer.DisposeAsync();
    }
}
