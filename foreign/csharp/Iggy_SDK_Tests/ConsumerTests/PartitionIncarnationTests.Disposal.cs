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
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Kinds;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Apache.Iggy.Tests.ConsumerTests;

public sealed partial class PartitionIncarnationTests
{
    [Theory]
    [InlineData("plain", AutoCommitMode.AfterReceive)]
    [InlineData("rented", AutoCommitMode.AfterReceive)]
    [InlineData("typed", AutoCommitMode.AfterReceive)]
    [InlineData("plain", AutoCommitMode.Auto)]
    [InlineData("rented", AutoCommitMode.Auto)]
    [InlineData("typed", AutoCommitMode.Auto)]
    [InlineData("plain", AutoCommitMode.Disabled)]
    [InlineData("rented", AutoCommitMode.Disabled)]
    [InlineData("typed", AutoCommitMode.Disabled)]
    public async Task given_buffered_messages_when_disposed_inside_receive_should_checkpoint_only_delivered_positions_before_leaving(
        string receive, AutoCommitMode mode)
    {
        var stores = new ConcurrentQueue<(ulong Offset, PartitionContext Context)>();
        var events = new ConcurrentQueue<string>();
        var client = ClientMock(stores);
        SetupDisposalGroup(client, events);
        client.Setup(mock => mock.StoreOffsetAsync(It.IsAny<Consumer>(), It.IsAny<Identifier>(),
                It.IsAny<Identifier>(), It.IsAny<ulong>(), It.IsAny<uint>(), It.IsAny<PartitionContext>(), It.IsAny<CancellationToken>()))
            .Callback((Consumer _, Identifier _, Identifier _, ulong offset, uint _, PartitionContext context, CancellationToken _) =>
            {
                stores.Enqueue((offset, context));
                events.Enqueue("store");
            }).Returns(Task.CompletedTask);
        var owner = new RentedConsumerTests.TrackingMemoryOwner(1024);
        var rental = new PolledMessagesRental(owner)
        {
            Context = Original,
            PartitionId = PartitionId,
            CurrentOffset = 2,
            Messages = RentedConsumerTests.BuildMessages(owner, 3)
        };
        var polled = new PolledMessages
        {
            Context = Original,
            PartitionId = PartitionId,
            CurrentOffset = 2,
            Messages = [.. Batch(0, Original).Messages, .. Batch(1, Original).Messages, .. Batch(2, Original).Messages]
        };
        client.Setup(mock => mock.PollMessagesAsync(It.IsAny<Identifier>(), It.IsAny<Identifier>(),
                It.IsAny<uint?>(), It.IsAny<Consumer>(), It.IsAny<Func<uint, PollingStrategy>>(), It.IsAny<uint>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(polled);
        client.Setup(mock => mock.PollMessagesRentedAsync(It.IsAny<Identifier>(), It.IsAny<Identifier>(),
                It.IsAny<uint?>(), It.IsAny<Consumer>(), It.IsAny<Func<uint, PollingStrategy>>(), It.IsAny<uint>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(rental);
        var config = new IggyConsumerConfig<string>
        {
            StreamId = Identifier.Numeric(1),
            TopicId = Identifier.Numeric(2),
            Consumer = Consumer.Group("group"),
            PollingStrategy = PollingStrategy.Next(),
            BatchSize = 10,
            AutoCommitMode = mode,
            PollingIntervalMs = 0,
            Deserializer = new DisposalDeserializer(),
            CreateIggyClient = true
        };
        await using var consumer = receive == "typed"
            ? new IggyConsumer<string>(client.Object, config, NullLoggerFactory.Instance)
            : new IggyConsumer(client.Object, config, NullLoggerFactory.Instance);
        using var timeout = Timeout();
        await consumer.InitAsync(timeout.Token);
        if (receive == "plain")
        {
            await using var messages = consumer.ReceiveAsync(timeout.Token).GetAsyncEnumerator(timeout.Token);
            Assert.True(await messages.MoveNextAsync());
            Assert.Equal(0ul, messages.Current.CurrentOffset);
            await consumer.DisposeAsync().AsTask().WaitAsync(timeout.Token);
            Assert.False(await messages.MoveNextAsync());
        }
        else if (receive == "rented")
        {
            await using var messages = consumer.ReceiveRentedAsync(timeout.Token).GetAsyncEnumerator(timeout.Token);
            Assert.True(await messages.MoveNextAsync());
            Assert.Equal(0ul, messages.Current.CurrentOffset);
            await consumer.DisposeAsync().AsTask().WaitAsync(timeout.Token);
            Assert.Equal(0, owner.DisposeCount);
            messages.Current.Dispose();
            Assert.Equal(1, owner.DisposeCount);
            Assert.False(await messages.MoveNextAsync());
        }
        else
        {
            await using var messages = ((IggyConsumer<string>)consumer).ReceiveDeserializedAsync(timeout.Token).GetAsyncEnumerator(timeout.Token);
            Assert.True(await messages.MoveNextAsync());
            Assert.Equal(0ul, messages.Current.CurrentOffset);
            await consumer.DisposeAsync().AsTask().WaitAsync(timeout.Token);
            Assert.False(await messages.MoveNextAsync());
            Assert.Equal(1, owner.DisposeCount);
        }
        await consumer.DisposeAsync();
        Assert.Equal(mode == AutoCommitMode.Auto ? [(0ul, Original)] : [], stores);
        Assert.Equal(mode == AutoCommitMode.Auto ? ["store", "leave", "logout", "dispose"] : ["leave", "logout", "dispose"], events);
        if (receive == "plain")
        {
            rental.Dispose();
        }
    }

    [Fact]
    public async Task given_after_receive_when_a_loop_body_throws_should_not_commit_its_message_on_dispose()
    {
        var stores = new ConcurrentQueue<(ulong Offset, PartitionContext Context)>();
        var client = ClientMock(stores);
        client.Setup(mock => mock.PollMessagesAsync(It.IsAny<Identifier>(), It.IsAny<Identifier>(),
                It.IsAny<uint?>(), It.IsAny<Consumer>(), It.IsAny<Func<uint, PollingStrategy>>(), It.IsAny<uint>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new PolledMessages
            {
                Context = Original,
                PartitionId = PartitionId,
                CurrentOffset = 1,
                Messages = [.. Batch(0, Original).Messages, .. Batch(1, Original).Messages]
            });
        await using var consumer = new IggyConsumer(client.Object, Config(), NullLoggerFactory.Instance);
        using var timeout = Timeout();
        await consumer.InitAsync(timeout.Token);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var message in consumer.ReceiveAsync(timeout.Token))
            {
                if (message.CurrentOffset == 1)
                {
                    throw new InvalidOperationException("Processing failed.");
                }
            }
        });
        await consumer.DisposeAsync();

        Assert.Equal([(0ul, Original)], stores);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_a_consumed_message_when_a_final_checkpoint_fails_or_would_rewind_should_still_leave_once(bool alreadyStored)
    {
        var events = new ConcurrentQueue<string>();
        var client = ClientMock(new());
        SetupDisposalGroup(client, events);
        client.Setup(mock => mock.PollMessagesAsync(It.IsAny<Identifier>(), It.IsAny<Identifier>(),
                It.IsAny<uint?>(), It.IsAny<Consumer>(), It.IsAny<Func<uint, PollingStrategy>>(), It.IsAny<uint>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Batch(OriginalOffset, Original));
        var stores = new ConcurrentQueue<ulong>();
        client.Setup(mock => mock.StoreOffsetAsync(It.IsAny<Consumer>(), It.IsAny<Identifier>(), It.IsAny<Identifier>(),
                It.IsAny<ulong>(), It.IsAny<uint>(), It.IsAny<PartitionContext>(), It.IsAny<CancellationToken>()))
            .Returns((Consumer _, Identifier _, Identifier _, ulong offset, uint _, PartitionContext _, CancellationToken _) =>
            {
                stores.Enqueue(offset);
                events.Enqueue("store");
                return alreadyStored ? Task.CompletedTask : Task.FromException(new IOException("Checkpoint failed."));
            });
        var config = Config(AutoCommitMode.Auto);
        config.Consumer = Consumer.Group("group");
        await using var consumer = new IggyConsumer(client.Object, config, NullLoggerFactory.Instance);
        using var timeout = Timeout();
        await consumer.InitAsync(timeout.Token);
        await using var messages = consumer.ReceiveAsync(timeout.Token).GetAsyncEnumerator(timeout.Token);
        Assert.True(await messages.MoveNextAsync());
        if (alreadyStored)
        {
            await consumer.StoreOffsetAsync(OriginalOffset + 1, PartitionId, Original, timeout.Token);
        }
        await consumer.DisposeAsync();
        await consumer.DisposeAsync();
        Assert.False(await messages.MoveNextAsync());
        Assert.Equal([alreadyStored ? OriginalOffset + 1 : OriginalOffset], stores);
        Assert.Equal(["store", "leave"], events);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_an_after_receive_store_in_flight_when_disposed_should_drain_it_before_checkpointing_and_leaving(bool sent)
    {
        var events = new ConcurrentQueue<string>();
        var client = ClientMock(new());
        SetupDisposalGroup(client, events);
        client.Setup(mock => mock.PollMessagesAsync(It.IsAny<Identifier>(), It.IsAny<Identifier>(),
                It.IsAny<uint?>(), It.IsAny<Consumer>(), It.IsAny<Func<uint, PollingStrategy>>(), It.IsAny<uint>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Batch(OriginalOffset, Original));
        var storing = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var attempts = 0;
        client.Setup(mock => mock.ResolveOperationAsync(1, It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .Callback(() => events.Enqueue("resolve")).Returns(Task.CompletedTask);
        client.Setup(mock => mock.StoreOffsetAsync(It.IsAny<Consumer>(), It.IsAny<Identifier>(), It.IsAny<Identifier>(),
                It.IsAny<ulong>(), It.IsAny<uint>(), It.IsAny<PartitionContext>(), It.IsAny<CancellationToken>()))
            .Returns(async (Consumer _, Identifier _, Identifier _, ulong _, uint _, PartitionContext _, CancellationToken token) =>
            {
                var attempt = Interlocked.Increment(ref attempts);
                if (attempt == 1)
                {
                    storing.TrySetResult();
                    using var registration = token.Register(() => cancelled.TrySetResult());
                    await release.Task;
                    events.Enqueue("drained");
                    if (sent)
                    {
                        throw new VsrRequestOutcomeUnknownException(new OperationCanceledException(token), 1);
                    }
                    token.ThrowIfCancellationRequested();
                }
                else if (sent && attempt == 2)
                {
                    throw new VsrLaneBlockedException(1);
                }
                else
                {
                    events.Enqueue("checkpoint");
                }
            });
        var config = Config();
        config.Consumer = Consumer.Group("group");
        await using var consumer = new IggyConsumer(client.Object, config, NullLoggerFactory.Instance);
        using var timeout = Timeout();
        await consumer.InitAsync(timeout.Token);
        await using var messages = consumer.ReceiveAsync(timeout.Token).GetAsyncEnumerator(timeout.Token);
        Assert.True(await messages.MoveNextAsync());
        var receive = messages.MoveNextAsync().AsTask();
        await storing.Task.WaitAsync(timeout.Token);
        var dispose = consumer.DisposeAsync().AsTask();
        try
        {
            Assert.False(dispose.IsCompleted);
            await cancelled.Task.WaitAsync(timeout.Token);
            Assert.Empty(events);
        }
        finally
        {
            release.TrySetResult();
            await receive;
        }
        await dispose.WaitAsync(timeout.Token);
        Assert.False(await receive);
        Assert.Equal(sent ? ["drained", "resolve", "checkpoint", "leave"] : ["drained", "checkpoint", "leave"], events);
    }

    private static void SetupDisposalGroup(Mock<IIggyClient> client, ConcurrentQueue<string> events)
    {
        client.Setup(mock => mock.GetConsumerGroupByIdAsync(It.IsAny<Identifier>(), It.IsAny<Identifier>(),
                It.IsAny<Identifier>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ConsumerGroupResponse { Id = 1, Name = "group", MembersCount = 1, PartitionsCount = 1 });
        client.Setup(mock => mock.LeaveConsumerGroupAsync(It.IsAny<Identifier>(), It.IsAny<Identifier>(),
                It.IsAny<Identifier>(), It.IsAny<CancellationToken>()))
            .Callback(() => events.Enqueue("leave")).Returns(Task.CompletedTask);
        client.Setup(mock => mock.LogoutUserAsync(It.IsAny<CancellationToken>()))
            .Callback(() => events.Enqueue("logout")).Returns(Task.CompletedTask);
        client.Setup(mock => mock.Dispose()).Callback(() => events.Enqueue("dispose"));
    }

    private sealed class DisposalDeserializer : IDeserializer<string>
    {
        public string Deserialize(ReadOnlyMemory<byte> data) => System.Text.Encoding.UTF8.GetString(data.Span);
    }
}
