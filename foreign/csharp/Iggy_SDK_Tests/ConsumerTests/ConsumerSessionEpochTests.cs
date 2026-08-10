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

using Apache.Iggy.Consumers;
using Apache.Iggy.Contracts;
using Apache.Iggy.Enums;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;
using Apache.Iggy.Vsr;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Apache.Iggy.Tests.ConsumerTests;

/// <summary>
///     Group membership on a transport that exposes a consensus session is keyed off the session epoch rather
///     than off connection-state edges, so these cover both arms of that branch.
/// </summary>
public sealed class ConsumerSessionEpochTests
{
    [Fact]
    public async Task
        given_group_consumer_when_reauthenticated_on_the_same_session_epoch_should_not_rejoin_the_group()
    {
        var client = new EpochClient();
        var consumer = new IggyConsumer(client.Object, BuildGroupConfig(), NullLoggerFactory.Instance);
        await consumer.InitAsync(TestContext.Current.CancellationToken);
        Assert.Equal(1, client.JoinCount);

        await client.RaiseAsync(ConnectionState.Connected, ConnectionState.Authenticated);

        Assert.Equal(1, client.JoinCount);
        await consumer.DisposeAsync();
    }

    [Fact]
    public async Task given_group_consumer_when_the_session_epoch_moved_should_rejoin_the_group()
    {
        var client = new EpochClient();
        var consumer = new IggyConsumer(client.Object, BuildGroupConfig(), NullLoggerFactory.Instance);
        await consumer.InitAsync(TestContext.Current.CancellationToken);
        Assert.Equal(1, client.JoinCount);

        // The session the membership was established under is gone.
        client.SessionEpoch++;
        await client.RaiseAsync(ConnectionState.Connected, ConnectionState.Authenticated);

        Assert.Equal(2, client.JoinCount);
        await consumer.DisposeAsync();
    }

    /// <summary>
    ///     A disconnect surrenders the membership even though the epoch has not moved yet. Holding it would let
    ///     the poll loop keep issuing requests the server refuses for as long as the reconnect takes.
    /// </summary>
    [Fact]
    public async Task given_group_consumer_when_disconnected_should_surrender_membership_and_rejoin_on_reconnect()
    {
        var client = new EpochClient();
        var consumer = new IggyConsumer(client.Object, BuildGroupConfig(), NullLoggerFactory.Instance);
        await consumer.InitAsync(TestContext.Current.CancellationToken);
        Assert.Equal(1, client.JoinCount);

        await client.RaiseAsync(ConnectionState.Authenticated, ConnectionState.Disconnected);
        await client.RaiseAsync(ConnectionState.Connecting, ConnectionState.Authenticated);

        Assert.Equal(2, client.JoinCount);
        await consumer.DisposeAsync();
    }

    /// <summary>
    ///     A plain consumer holds no membership, so nothing may clear its joined flag: no code path would ever
    ///     set it again and every later poll would be skipped.
    /// </summary>
    [Fact]
    public async Task given_plain_consumer_when_disconnected_should_keep_polling_after_reconnect()
    {
        var client = new EpochClient();
        var config = BuildGroupConfig();
        config.Consumer = Consumer.New(1);
        var consumer = new IggyConsumer(client.Object, config, NullLoggerFactory.Instance);
        await consumer.InitAsync(TestContext.Current.CancellationToken);
        Assert.Equal(0, client.JoinCount);

        await client.RaiseAsync(ConnectionState.Authenticated, ConnectionState.Disconnected);
        client.SessionEpoch++;
        await client.RaiseAsync(ConnectionState.Connecting, ConnectionState.Authenticated);

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await using IAsyncEnumerator<ReceivedMessage> messages = consumer.ReceiveAsync(cts.Token)
            .GetAsyncEnumerator(TestContext.Current.CancellationToken);

        Assert.True(await messages.MoveNextAsync());
        Assert.Equal(0, client.JoinCount);
        await consumer.DisposeAsync();
    }

    private static IggyConsumerConfig BuildGroupConfig()
    {
        return new IggyConsumerConfig
        {
            StreamId = Identifier.Numeric(1),
            TopicId = Identifier.Numeric(1),
            Consumer = Consumer.Group("group-1"),
            PollingStrategy = PollingStrategy.Next(),
            BatchSize = 10,
            AutoCommitMode = AutoCommitMode.Disabled,
            AutoCommit = false,
            PollingIntervalMs = 0
        };
    }

    /// <summary>
    ///     A client that carries a consensus session, counts group joins, and replays connection-state events on
    ///     demand so a test can drive the reconnection handler without a socket.
    /// </summary>
    private sealed class EpochClient
    {
        private readonly List<Func<ConnectionStateChangedEventArgs, Task>> _subscribers = [];

        public IIggyClient Object { get; }

        public ulong SessionEpoch { get; set; }

        public int JoinCount { get; private set; }

        public EpochClient()
        {
            var mock = new Mock<IIggyClient>(MockBehavior.Loose);
            mock.As<ISessionEpochProvider>().SetupGet(c => c.SessionEpoch).Returns(() => SessionEpoch);
            mock.Setup(c => c.ConnectAsync(It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
            mock.Setup(c => c.SubscribeConnectionEvents(It.IsAny<Func<ConnectionStateChangedEventArgs, Task>>()))
                .Callback<Func<ConnectionStateChangedEventArgs, Task>>(_subscribers.Add);
            mock.Setup(c => c.UnsubscribeConnectionEvents(It.IsAny<Func<ConnectionStateChangedEventArgs, Task>>()))
                .Callback<Func<ConnectionStateChangedEventArgs, Task>>(callback => _subscribers.Remove(callback));
            mock.Setup(c => c.GetConsumerGroupByIdAsync(It.IsAny<Identifier>(), It.IsAny<Identifier>(),
                    It.IsAny<Identifier>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new ConsumerGroupResponse
                {
                    Id = 1,
                    Name = "group-1",
                    MembersCount = 1,
                    PartitionsCount = 1
                });
            mock.Setup(c => c.JoinConsumerGroupAsync(It.IsAny<Identifier>(), It.IsAny<Identifier>(),
                    It.IsAny<Identifier>(), It.IsAny<CancellationToken>()))
                .Callback(() => JoinCount++)
                .Returns(Task.CompletedTask);
            mock.Setup(c => c.PollMessagesAsync(It.IsAny<Identifier>(), It.IsAny<Identifier>(), It.IsAny<uint?>(),
                    It.IsAny<Consumer>(), It.IsAny<PollingStrategy>(), It.IsAny<uint>(), It.IsAny<bool>(),
                    It.IsAny<CancellationToken>()))
                .ReturnsAsync(() => new PolledMessages
                {
                    PartitionId = 1,
                    CurrentOffset = 0,
                    Messages =
                    [
                        new MessageResponse
                        {
                            Header = new MessageHeader
                            {
                                Offset = 0,
                                PayloadLength = 1
                            },
                            Payload = new byte[] { 1 },
                            UserHeaders = null
                        }
                    ]
                });

            Object = mock.Object;
        }

        public async Task RaiseAsync(ConnectionState previousState, ConnectionState currentState)
        {
            foreach (Func<ConnectionStateChangedEventArgs, Task> subscriber in _subscribers.ToArray())
            {
                await subscriber(new ConnectionStateChangedEventArgs(previousState, currentState));
            }
        }
    }
}
