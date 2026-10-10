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

using System.Collections.Concurrent;
using Apache.Iggy.Consumers;
using Apache.Iggy.Contracts;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Apache.Iggy.Tests.ConsumerTests;

/// <summary>
///     Offsets compare only within one partition incarnation and owner. A recreated partition restarts at offset
///     0, so the consumer has to deliver its messages even below the last offset the old incarnation delivered,
///     and must never commit an offset of the old incarnation into it.
/// </summary>
public sealed class PartitionIncarnationTests
{
    private const uint PartitionId = 3;
    private const ulong OriginalOffset = 100;
    private static readonly PartitionContext Original = new(17, 0, 52);
    private static readonly PartitionContext Recreated = new(18, 0, 60);

    [Fact]
    public async Task given_a_recreated_partition_when_receiving_should_deliver_it_and_not_commit_the_old_offset()
    {
        var polls = new ConcurrentQueue<PolledMessages>([Batch(OriginalOffset, Original), Batch(0, Recreated)]);
        var stores = new ConcurrentQueue<(ulong Offset, PartitionContext Context)>();
        var client = ClientMock(stores);
        client.Setup(mock => mock.PollMessagesAsync(It.IsAny<Identifier>(), It.IsAny<Identifier>(),
                It.IsAny<uint?>(), It.IsAny<Consumer>(), It.IsAny<Func<uint, PollingStrategy>>(), It.IsAny<uint>(),
                It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .Returns((Identifier _, Identifier _, uint? _, Consumer _, Func<uint, PollingStrategy> _, uint _, bool _,
                CancellationToken token) => polls.TryDequeue(out var polled) ? Task.FromResult(polled) : Idle<PolledMessages>(token));
        await using var consumer = new IggyConsumer(client.Object, Config(), NullLoggerFactory.Instance);
        await consumer.InitAsync(TestContext.Current.CancellationToken);
        using var cancellation = Timeout();

        await using var messages = consumer.ReceiveAsync(cancellation.Token).GetAsyncEnumerator(cancellation.Token);
        Assert.True(await messages.MoveNextAsync());
        Assert.Equal(OriginalOffset, messages.Current.CurrentOffset);
        var delivered = await NextAsync(messages);

        Assert.DoesNotContain((OriginalOffset, Recreated), stores);
        Assert.Equal(0ul, Assert.IsType<ReceivedMessage>(delivered).CurrentOffset);
        Assert.Equal([(OriginalOffset, Original)], stores);
    }

    [Fact]
    public async Task given_a_recreated_partition_when_receiving_rented_should_deliver_it_and_not_commit_the_old_offset()
    {
        var polls = new ConcurrentQueue<PolledMessagesRental>([Rental(OriginalOffset, Original), Rental(0, Recreated)]);
        var stores = new ConcurrentQueue<(ulong Offset, PartitionContext Context)>();
        var client = ClientMock(stores);
        client.Setup(mock => mock.PollMessagesRentedAsync(It.IsAny<Identifier>(), It.IsAny<Identifier>(),
                It.IsAny<uint?>(), It.IsAny<Consumer>(), It.IsAny<Func<uint, PollingStrategy>>(), It.IsAny<uint>(),
                It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .Returns((Identifier _, Identifier _, uint? _, Consumer _, Func<uint, PollingStrategy> _, uint _, bool _,
                CancellationToken token) => polls.TryDequeue(out var polled) ? Task.FromResult(polled) : Idle<PolledMessagesRental>(token));
        await using var consumer = new IggyConsumer(client.Object, Config(), NullLoggerFactory.Instance);
        await consumer.InitAsync(TestContext.Current.CancellationToken);
        using var cancellation = Timeout();

        await using var messages = consumer.ReceiveRentedAsync(cancellation.Token)
            .GetAsyncEnumerator(cancellation.Token);
        Assert.True(await messages.MoveNextAsync());
        Assert.Equal(OriginalOffset, messages.Current.CurrentOffset);
        messages.Current.Dispose();
        using var delivered = await NextAsync(messages);

        Assert.DoesNotContain((OriginalOffset, Recreated), stores);
        Assert.Equal(0ul, Assert.IsType<ReceivedRentedMessage>(delivered).CurrentOffset);
        Assert.Equal([(OriginalOffset, Original)], stores);
    }

    [Fact]
    public async Task given_a_message_of_an_earlier_poll_when_stored_late_should_store_it_under_its_own_context()
    {
        var polls = new ConcurrentQueue<PolledMessages>([Batch(OriginalOffset, Original), Batch(0, Recreated)]);
        var stores = new ConcurrentQueue<(ulong Offset, PartitionContext Context)>();
        var client = ClientMock(stores);
        client.Setup(mock => mock.PollMessagesAsync(It.IsAny<Identifier>(), It.IsAny<Identifier>(),
                It.IsAny<uint?>(), It.IsAny<Consumer>(), It.IsAny<Func<uint, PollingStrategy>>(), It.IsAny<uint>(),
                It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .Returns((Identifier _, Identifier _, uint? _, Consumer _, Func<uint, PollingStrategy> _, uint _, bool _,
                CancellationToken token) => polls.TryDequeue(out var polled) ? Task.FromResult(polled) : Idle<PolledMessages>(token));
        await using var consumer = new IggyConsumer(client.Object, Config(AutoCommitMode.Disabled),
            NullLoggerFactory.Instance);
        await consumer.InitAsync(TestContext.Current.CancellationToken);
        using var cancellation = Timeout();
        await using var messages = consumer.ReceiveAsync(cancellation.Token).GetAsyncEnumerator(cancellation.Token);
        Assert.True(await messages.MoveNextAsync());
        var early = messages.Current;
        Assert.True(await messages.MoveNextAsync());
        Assert.Equal(Recreated, messages.Current.Context);

        await consumer.StoreOffsetAsync(early.CurrentOffset, early.PartitionId, early.Context,
            TestContext.Current.CancellationToken);

        Assert.Equal([(OriginalOffset, Original)], stores);
    }

    private static IggyConsumerConfig Config(AutoCommitMode mode = AutoCommitMode.AfterReceive)
    {
        return new IggyConsumerConfig
        {
            StreamId = Identifier.Numeric(1),
            TopicId = Identifier.Numeric(2),
            PartitionId = PartitionId,
            Consumer = Consumer.New(1),
            PollingStrategy = PollingStrategy.Next(),
            BatchSize = 10,
            AutoCommit = false,
            AutoCommitMode = mode,
            PollingIntervalMs = 0
        };
    }

    private static Mock<IIggyClient> ClientMock(ConcurrentQueue<(ulong Offset, PartitionContext Context)> stores)
    {
        var client = new Mock<IIggyClient>(MockBehavior.Loose);
        client.Setup(mock => mock.StoreOffsetAsync(It.IsAny<Consumer>(), It.IsAny<Identifier>(),
                It.IsAny<Identifier>(), It.IsAny<ulong>(), It.IsAny<uint>(), It.IsAny<PartitionContext>(),
                It.IsAny<CancellationToken>()))
            .Callback((Consumer _, Identifier _, Identifier _, ulong offset, uint _, PartitionContext context,
                CancellationToken _) => stores.Enqueue((offset, context)))
            .Returns(Task.CompletedTask);
        client.Setup(mock => mock.StoreOffsetAsync(It.IsAny<Consumer>(), It.IsAny<Identifier>(),
                It.IsAny<Identifier>(), It.IsAny<ulong>(), It.IsAny<uint?>(), It.IsAny<CancellationToken>()))
            .Callback((Consumer _, Identifier _, Identifier _, ulong offset, uint? _, CancellationToken _) =>
                stores.Enqueue((offset, default)))
            .Returns(Task.CompletedTask);
        return client;
    }

    private static PolledMessages Batch(ulong offset, PartitionContext context)
    {
        return new PolledMessages
        {
            Context = context,
            PartitionId = PartitionId,
            CurrentOffset = offset,
            Messages =
            [
                new MessageResponse
                {
                    Header = new MessageHeader { Offset = offset, PayloadLength = 1 },
                    Payload = [1],
                    UserHeaders = null
                }
            ]
        };
    }

    private static PolledMessagesRental Rental(ulong offset, PartitionContext context)
    {
        var owner = new RentedConsumerTests.TrackingMemoryOwner(16);
        return new PolledMessagesRental(owner)
        {
            Context = context,
            PartitionId = PartitionId,
            CurrentOffset = offset,
            Messages =
            [
                new RentedMessageResponse
                {
                    Header = new MessageHeader { Offset = offset, PayloadLength = 1 },
                    Payload = owner.Memory[..1]
                }
            ]
        };
    }

    /// <summary>A poll that never answers, so a consumer that dropped the replacement waits for the timeout.</summary>
    private static async Task<T> Idle<T>(CancellationToken token)
    {
        await Task.Delay(System.Threading.Timeout.Infinite, token);
        throw new OperationCanceledException(token);
    }

    private static CancellationTokenSource Timeout()
    {
        var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cancellation.CancelAfter(TimeSpan.FromSeconds(5));
        return cancellation;
    }

    /// <summary>The next message, or null when the consumer delivered nothing before the timeout.</summary>
    private static async Task<T?> NextAsync<T>(IAsyncEnumerator<T> messages) where T : class
    {
        try
        {
            return await messages.MoveNextAsync() ? messages.Current : null;
        }
        catch (OperationCanceledException)
        {
            return null;
        }
    }
}
