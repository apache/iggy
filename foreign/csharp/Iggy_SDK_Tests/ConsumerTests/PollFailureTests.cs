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
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Kinds;
using Apache.Iggy.Vsr;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Apache.Iggy.Tests.ConsumerTests;

public sealed class PollFailureTests
{
    private const string GroupName = "group-1";

    // The event id of the log line a poll writes before it rejoins the group.
    private const int NotJoinedEventId = 3;

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_an_in_flight_poll_when_canceled_should_end_receiving_without_an_error_event(bool rented)
    {
        var polling = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var mock = new Mock<IIggyClient>(MockBehavior.Loose);
        // A canceled progress poll that already reached the server ends with an unknown outcome, not a cancellation.
        async Task<T> Interrupted<T>(CancellationToken token)
        {
            polling.TrySetResult();
            await Task.Delay(Timeout.Infinite, token).ContinueWith(_ => { }, TaskScheduler.Default);
            throw new VsrRequestOutcomeUnknownException(new IOException("The poll was interrupted."));
        }

        SetupPoll(mock, rented, Interrupted<PolledMessages>, Interrupted<PolledMessagesRental>);
        var consumer = new IggyConsumer(mock.Object, Config(), NullLoggerFactory.Instance);
        var errors = 0;
        consumer.SubscribeToErrorEvents(_ =>
        {
            Interlocked.Increment(ref errors);
            return Task.CompletedTask;
        });
        await consumer.InitAsync(TestContext.Current.CancellationToken);

        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        var receiving = rented ? MoveNextRentedAsync(consumer, cancellation.Token) : MoveNextAsync(consumer, cancellation.Token);
        await polling.Task.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        await cancellation.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => receiving)
            .WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        Assert.Equal(0, Volatile.Read(ref errors));
        await consumer.DisposeAsync();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_a_failing_client_when_receiving_without_a_polling_interval_should_wait_before_polling_again(
        bool rented)
    {
        const int WindowMs = 300;
        // The window is shorter than the retry delay, so only a starved scheduler lets a second poll in.
        const int MaxPolls = 2;
        var polls = 0;
        var mock = new Mock<IIggyClient>(MockBehavior.Loose);
        Task<T> Failed<T>(CancellationToken _)
        {
            Interlocked.Increment(ref polls);
            return Task.FromException<T>(new IOException("The client is disconnected."));
        }

        SetupPoll(mock, rented, Failed<PolledMessages>, Failed<PolledMessagesRental>);
        var consumer = new IggyConsumer(mock.Object, Config(), NullLoggerFactory.Instance);
        await consumer.InitAsync(TestContext.Current.CancellationToken);

        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cancellation.CancelAfter(WindowMs);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => rented
            ? MoveNextRentedAsync(consumer, cancellation.Token)
            : MoveNextAsync(consumer, cancellation.Token));

        Assert.InRange(Volatile.Read(ref polls), 1, MaxPolls);
        await consumer.DisposeAsync();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_a_poll_in_its_retry_delay_when_the_consumer_is_disposed_should_end_receiving_without_an_error(
        bool rented)
    {
        var polls = 0;
        var mock = new Mock<IIggyClient>(MockBehavior.Loose);
        Task<T> Failed<T>(CancellationToken _)
        {
            Interlocked.Increment(ref polls);
            return Task.FromException<T>(new IOException("The client is disconnected."));
        }

        SetupPoll(mock, rented, Failed<PolledMessages>, Failed<PolledMessagesRental>);
        var consumer = new IggyConsumer(mock.Object, Config(), NullLoggerFactory.Instance);
        var reported = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        // The error event goes out right before the retry delay starts.
        consumer.SubscribeToErrorEvents(_ =>
        {
            reported.TrySetResult();
            return Task.CompletedTask;
        });
        await consumer.InitAsync(TestContext.Current.CancellationToken);

        var receiving = rented
            ? MoveNextRentedAsync(consumer, TestContext.Current.CancellationToken)
            : MoveNextAsync(consumer, TestContext.Current.CancellationToken);
        await reported.Task.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        await consumer.DisposeAsync();

        Assert.False(await receiving.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken));
        Assert.Equal(1, Volatile.Read(ref polls));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_a_disposed_consumer_when_receiving_should_end_without_polling(bool rented)
    {
        var polls = 0;
        var mock = new Mock<IIggyClient>(MockBehavior.Loose);
        Task<T> Counted<T>(CancellationToken _)
        {
            Interlocked.Increment(ref polls);
            return Task.FromException<T>(new IOException("The client is disposed."));
        }

        SetupPoll(mock, rented, Counted<PolledMessages>, Counted<PolledMessagesRental>);
        var consumer = new IggyConsumer(mock.Object, Config(), NullLoggerFactory.Instance);
        await consumer.InitAsync(TestContext.Current.CancellationToken);
        await consumer.DisposeAsync();

        Assert.False(await (rented
            ? MoveNextRentedAsync(consumer, TestContext.Current.CancellationToken)
            : MoveNextAsync(consumer, TestContext.Current.CancellationToken)));
        Assert.Equal(0, Volatile.Read(ref polls));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_a_group_rejoin_in_flight_when_the_consumer_is_disposed_should_end_receiving_without_an_error(
        bool rented)
    {
        var rejoin = new HeldLookup(2);
        var client = new GroupClient(rejoin.Lookup);
        var consumer = new IggyConsumer(client.Mock.Object, GroupConfig(), NullLoggerFactory.Instance);
        await consumer.InitAsync(TestContext.Current.CancellationToken);
        client.ReplaceSession();

        var receiving = rented
            ? MoveNextRentedAsync(consumer, TestContext.Current.CancellationToken)
            : MoveNextAsync(consumer, TestContext.Current.CancellationToken);
        await rejoin.Started.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        var disposing = consumer.DisposeAsync().AsTask();
        rejoin.Answer.SetResult(Group());
        await disposing;

        Assert.False(await receiving.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task given_a_group_lookup_in_flight_when_the_consumer_is_disposed_should_not_join_after_leaving()
    {
        var rejoin = new HeldLookup(2);
        var client = new GroupClient(rejoin.Lookup);
        var consumer = new IggyConsumer(client.Mock.Object, GroupConfig(), NullLoggerFactory.Instance);
        await consumer.InitAsync(TestContext.Current.CancellationToken);
        client.ReplaceSession();

        var receiving = MoveNextAsync(consumer, TestContext.Current.CancellationToken);
        await rejoin.Started.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        var disposing = consumer.DisposeAsync().AsTask();
        rejoin.Answer.SetResult(Group());
        await disposing;
        await receiving.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

        Assert.Equal([true], client.Memberships);
    }

    [Fact]
    public async Task given_a_poll_waiting_for_an_event_rejoin_when_the_consumer_is_disposed_should_end_without_rejoining()
    {
        var rejoin = new HeldLookup(2);
        var client = new GroupClient(rejoin.Lookup);
        var skipped = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var logger = new Mock<ILogger>();
        logger.Setup(log => log.IsEnabled(It.IsAny<LogLevel>())).Returns(true);
        // The poll logs that it skips polling after its dispose check and right before it waits for the rejoin.
        logger.Setup(log => log.Log(It.IsAny<LogLevel>(), It.Is<EventId>(id => id.Id == NotJoinedEventId),
                It.IsAny<It.IsAnyType>(), It.IsAny<Exception?>(), It.IsAny<Func<It.IsAnyType, Exception?, string>>()))
            .Callback(() => skipped.TrySetResult());
        var loggers = new Mock<ILoggerFactory>();
        loggers.Setup(factory => factory.CreateLogger(It.IsAny<string>())).Returns(logger.Object);
        var consumer = new IggyConsumer(client.Mock.Object, GroupConfig(), loggers.Object);
        await consumer.InitAsync(TestContext.Current.CancellationToken);
        client.ReplaceSession();
        var eventRejoin = client.RaiseAuthenticatedAsync();
        await rejoin.Started.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

        var receiving = MoveNextAsync(consumer, TestContext.Current.CancellationToken);
        await skipped.Task.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        var disposing = consumer.DisposeAsync().AsTask();
        rejoin.Answer.SetException(new IOException("The client is disposed."));
        await disposing;
        await eventRejoin.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

        Assert.False(await receiving.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken));
        Assert.Equal(1, client.Joins);
    }

    [Fact]
    public async Task given_a_disposed_group_consumer_when_a_late_event_reports_a_new_session_should_not_rejoin()
    {
        var client = new GroupClient();
        var consumer = new IggyConsumer(client.Mock.Object, GroupConfig(), NullLoggerFactory.Instance);
        await consumer.InitAsync(TestContext.Current.CancellationToken);
        await consumer.DisposeAsync();
        client.ReplaceSession();

        // An event published before the dispose unsubscribed the handler still reaches it.
        await client.RaiseAuthenticatedAsync();

        Assert.Equal(1, client.Joins);
    }

    [Fact]
    public async Task given_a_consumer_disposed_before_init_when_initializing_should_throw_object_disposed()
    {
        var mock = new Mock<IIggyClient>(MockBehavior.Loose);
        var consumer = new IggyConsumer(mock.Object, Config(), NullLoggerFactory.Instance);
        await consumer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            consumer.InitAsync(TestContext.Current.CancellationToken));
        mock.Verify(c => c.ConnectAsync(It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task given_a_disposed_consumer_when_a_subclass_polls_should_throw_object_disposed()
    {
        var consumer = new PollingConsumer(new Mock<IIggyClient>(MockBehavior.Loose).Object);
        await consumer.InitAsync(TestContext.Current.CancellationToken);
        await consumer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            consumer.PollAsync(TestContext.Current.CancellationToken));
    }

    private static void SetupPoll(Mock<IIggyClient> mock, bool rented,
        Func<CancellationToken, Task<PolledMessages>> poll, Func<CancellationToken, Task<PolledMessagesRental>> rental)
    {
        if (rented)
        {
            mock.Setup(c => c.PollMessagesRentedAsync(It.IsAny<Identifier>(), It.IsAny<Identifier>(),
                    It.IsAny<uint?>(), It.IsAny<Consumer>(), It.IsAny<Func<uint, PollingStrategy>>(), It.IsAny<uint>(),
                    It.IsAny<bool>(), It.IsAny<CancellationToken>()))
                .Returns((Identifier _, Identifier _, uint? _, Consumer _, Func<uint, PollingStrategy> _, uint _,
                    bool _, CancellationToken token) => rental(token));
            return;
        }

        mock.Setup(c => c.PollMessagesAsync(It.IsAny<Identifier>(), It.IsAny<Identifier>(), It.IsAny<uint?>(),
                It.IsAny<Consumer>(), It.IsAny<Func<uint, PollingStrategy>>(), It.IsAny<uint>(), It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .Returns((Identifier _, Identifier _, uint? _, Consumer _, Func<uint, PollingStrategy> _, uint _, bool _,
                CancellationToken token) => poll(token));
    }

    private static async Task<bool> MoveNextAsync(IggyConsumer consumer, CancellationToken token)
    {
        await using var messages = consumer.ReceiveAsync(token).GetAsyncEnumerator(token);
        return await messages.MoveNextAsync();
    }

    private static async Task<bool> MoveNextRentedAsync(IggyConsumer consumer, CancellationToken token)
    {
        await using var messages = consumer.ReceiveRentedAsync(token).GetAsyncEnumerator(token);
        return await messages.MoveNextAsync();
    }

    private static IggyConsumerConfig Config()
    {
        return new IggyConsumerConfig
        {
            StreamId = Identifier.Numeric(1),
            TopicId = Identifier.Numeric(1),
            Consumer = Consumer.New(1),
            PollingStrategy = PollingStrategy.Next(),
            BatchSize = 10,
            PartitionId = 1,
            AutoCommitMode = AutoCommitMode.Disabled,
            AutoCommit = false,
            PollingIntervalMs = 0
        };
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_initialization_in_flight_when_disposed_should_finish_cleanup_before_returning(bool joining)
    {
        var lookup = new HeldLookup(1);
        var client = new GroupClient(joining ? null : lookup.Lookup);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        if (joining)
        {
            client.Mock.Setup(mock => mock.JoinConsumerGroupAsync(It.IsAny<Identifier>(), It.IsAny<Identifier>(),
                    It.IsAny<Identifier>(), It.IsAny<CancellationToken>()))
                .Returns(() =>
                {
                    started.TrySetResult();
                    return release.Task;
                });
        }
        var consumer = new IggyConsumer(client.Mock.Object, GroupConfig(), NullLoggerFactory.Instance);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var initialize = consumer.InitAsync(timeout.Token);
        await (joining ? started.Task : lookup.Started).WaitAsync(timeout.Token);
        var dispose = consumer.DisposeAsync().AsTask();
        try
        {
            Assert.False(dispose.IsCompleted);
        }
        finally
        {
            release.TrySetResult();
            lookup.Answer.TrySetResult(Group());
        }
        Assert.NotNull(await Record.ExceptionAsync(() => initialize));
        await dispose.WaitAsync(timeout.Token);
        await consumer.DisposeAsync();
        client.Mock.Verify(mock => mock.SubscribeConnectionEvents(It.IsAny<Func<ConnectionStateChangedEventArgs, Task>>()), Times.Never);
        client.Mock.Verify(mock => mock.LeaveConsumerGroupAsync(It.IsAny<Identifier>(), It.IsAny<Identifier>(),
            It.IsAny<Identifier>(), It.IsAny<CancellationToken>()), joining ? Times.Once() : Times.Never());
        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.InitAsync(timeout.Token));
    }

    private static IggyConsumerConfig GroupConfig()
    {
        var config = Config();
        config.Consumer = Consumer.Group(GroupName);
        return config;
    }

    private static ConsumerGroupResponse Group() =>
        new() { Id = 1, Name = GroupName, MembersCount = 1, PartitionsCount = 1 };

    /// <summary>
    ///     A client whose session generation keys the group membership, as the TCP client's does. It counts the joins
    ///     and keeps the state handler after the consumer unsubscribes it.
    /// </summary>
    private sealed class GroupClient
    {
        private ulong _generation;
        private int _joins;
        private int _lookups;
        private Func<ConnectionStateChangedEventArgs, Task>? _handler;
        private readonly ConcurrentQueue<bool> _memberships = new();

        /// <param name="lookup">Answers the group lookup of the given 1-based number, or null for the group.</param>
        internal GroupClient(Func<int, Task<ConsumerGroupResponse?>?>? lookup = null)
        {
            Mock.Setup(c => c.LeaveConsumerGroupAsync(It.IsAny<Identifier>(), It.IsAny<Identifier>(),
                    It.IsAny<Identifier>(), It.IsAny<CancellationToken>()))
                .Callback(() => _memberships.Enqueue(false))
                .Returns(Task.CompletedTask);
            Mock.As<ISessionGenerationProvider>().SetupGet(c => c.SessionGeneration)
                .Returns(() => Interlocked.Read(ref _generation));
            Mock.Setup(c => c.SubscribeConnectionEvents(It.IsAny<Func<ConnectionStateChangedEventArgs, Task>>()))
                .Callback<Func<ConnectionStateChangedEventArgs, Task>>(handler => _handler = handler);
            Mock.Setup(c => c.GetConsumerGroupByIdAsync(It.IsAny<Identifier>(), It.IsAny<Identifier>(),
                    It.IsAny<Identifier>(), It.IsAny<CancellationToken>()))
                .Returns(() => lookup?.Invoke(Interlocked.Increment(ref _lookups))
                    ?? Task.FromResult<ConsumerGroupResponse?>(Group()));
            Mock.Setup(c => c.JoinConsumerGroupAsync(It.IsAny<Identifier>(), It.IsAny<Identifier>(),
                    It.IsAny<Identifier>(), It.IsAny<CancellationToken>()))
                .Callback(() =>
                {
                    Interlocked.Increment(ref _joins);
                    _memberships.Enqueue(true);
                })
                .Returns(Task.CompletedTask);
        }

        internal Mock<IIggyClient> Mock { get; } = new(MockBehavior.Loose);

        internal int Joins => Volatile.Read(ref _joins);

        /// <summary>Whether the consumer is a group member after each join and leave, in the order of the calls.</summary>
        internal bool[] Memberships => _memberships.ToArray();

        /// <summary>A new session ends the membership, so the next poll or state event rejoins the group.</summary>
        internal void ReplaceSession() => Interlocked.Increment(ref _generation);

        internal Task RaiseAuthenticatedAsync() =>
            _handler!(new ConnectionStateChangedEventArgs(ConnectionState.Connected, ConnectionState.Authenticated));
    }

    /// <summary>Holds the group lookup of one number until the test completes <see cref="Answer" />.</summary>
    private sealed class HeldLookup(int number)
    {
        private readonly TaskCompletionSource _started = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal TaskCompletionSource<ConsumerGroupResponse?> Answer { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal Task Started => _started.Task;

        internal Task<ConsumerGroupResponse?>? Lookup(int lookup)
        {
            if (lookup != number)
            {
                return null;
            }

            _started.TrySetResult();
            return Answer.Task;
        }
    }

    private sealed class PollingConsumer : IggyConsumer
    {
        public PollingConsumer(IIggyClient client) : base(client, Config(), NullLoggerFactory.Instance)
        {
        }

        public Task PollAsync(CancellationToken token) => PollRentedMessagesAsync(token);
    }
}
