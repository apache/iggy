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

using System.Diagnostics;
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
///     A group poll that reports <see cref="PolledMessages.NoAssignedPartition" /> must not be re-issued at once:
///     with <c>PollingIntervalMs</c> at zero that would spin against the coordinator until a partition arrives.
/// </summary>
public sealed class NoAssignedPartitionBackoffTests
{
    private const int MinimumGapMs = 80;

    [Fact]
    public async Task given_no_assigned_partition_when_receiving_should_back_off_before_polling_again()
    {
        var polls = new List<long>();
        var stopwatch = Stopwatch.StartNew();
        var mock = new Mock<IIggyClient>(MockBehavior.Loose);
        mock.Setup(c => c.PollMessagesAsync(It.IsAny<Identifier>(), It.IsAny<Identifier>(), It.IsAny<uint?>(),
                It.IsAny<Consumer>(), It.IsAny<PollingStrategy>(), It.IsAny<uint>(), It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(() =>
            {
                polls.Add(stopwatch.ElapsedMilliseconds);

                return polls.Count < 3 ? NoAssignment() : OneMessage();
            });
        var consumer = new IggyConsumer(mock.Object, BuildConfig(), NullLoggerFactory.Instance);
        await consumer.InitAsync(TestContext.Current.CancellationToken);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await using var messages = consumer.ReceiveAsync(cts.Token)
            .GetAsyncEnumerator(TestContext.Current.CancellationToken);
        Assert.True(await messages.MoveNextAsync());

        Assert.True(polls.Count >= 3);
        Assert.True(polls[1] - polls[0] >= MinimumGapMs, $"second poll came {polls[1] - polls[0]}ms after the first");
        Assert.True(polls[2] - polls[1] >= MinimumGapMs, $"third poll came {polls[2] - polls[1]}ms after the second");
        await consumer.DisposeAsync();
    }

    [Fact]
    public async Task given_no_assigned_partition_when_receiving_rented_should_back_off_before_polling_again()
    {
        var polls = new List<long>();
        var stopwatch = Stopwatch.StartNew();
        var mock = new Mock<IIggyClient>(MockBehavior.Loose);
        mock.Setup(c => c.PollMessagesRentedAsync(It.IsAny<Identifier>(), It.IsAny<Identifier>(),
                It.IsAny<uint?>(), It.IsAny<Consumer>(), It.IsAny<PollingStrategy>(), It.IsAny<uint>(),
                It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(() =>
            {
                polls.Add(stopwatch.ElapsedMilliseconds);

                return polls.Count < 3 ? NoAssignmentRental() : OneMessageRental();
            });
        var consumer = new IggyConsumer(mock.Object, BuildConfig(), NullLoggerFactory.Instance);
        await consumer.InitAsync(TestContext.Current.CancellationToken);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await using var messages = consumer.ReceiveRentedAsync(cts.Token)
            .GetAsyncEnumerator(TestContext.Current.CancellationToken);
        Assert.True(await messages.MoveNextAsync());
        messages.Current.Dispose();

        Assert.True(polls.Count >= 3);
        Assert.True(polls[1] - polls[0] >= MinimumGapMs, $"second poll came {polls[1] - polls[0]}ms after the first");
        Assert.True(polls[2] - polls[1] >= MinimumGapMs, $"third poll came {polls[2] - polls[1]}ms after the second");
        await consumer.DisposeAsync();
    }

    [Fact]
    public async Task given_no_assigned_partition_when_cancelled_mid_backoff_should_not_publish_an_error()
    {
        var errors = 0;
        var mock = new Mock<IIggyClient>(MockBehavior.Loose);
        mock.Setup(c => c.PollMessagesAsync(It.IsAny<Identifier>(), It.IsAny<Identifier>(), It.IsAny<uint?>(),
                It.IsAny<Consumer>(), It.IsAny<PollingStrategy>(), It.IsAny<uint>(), It.IsAny<bool>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(NoAssignment);
        var consumer = new IggyConsumer(mock.Object, BuildConfig(), NullLoggerFactory.Instance);
        consumer.SubscribeToErrorEvents(_ =>
        {
            Interlocked.Increment(ref errors);
            return Task.CompletedTask;
        });
        await consumer.InitAsync(TestContext.Current.CancellationToken);

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(30));
        await using var messages = consumer.ReceiveAsync(cts.Token)
            .GetAsyncEnumerator(TestContext.Current.CancellationToken);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await messages.MoveNextAsync());

        Assert.Equal(0, Volatile.Read(ref errors));
        await consumer.DisposeAsync();
    }

    private static IggyConsumerConfig BuildConfig()
    {
        return new IggyConsumerConfig
        {
            StreamId = Identifier.Numeric(1),
            TopicId = Identifier.Numeric(1),
            Consumer = Consumer.Group("group-1"),
            PollingStrategy = PollingStrategy.Next(),
            BatchSize = 10,
            PartitionId = null,
            AutoCommitMode = AutoCommitMode.Disabled,
            AutoCommit = false,
            PollingIntervalMs = 0
        };
    }

    private static PolledMessages NoAssignment()
    {
        return new PolledMessages
        {
            PartitionId = PolledMessages.NoAssignedPartition,
            CurrentOffset = 0,
            Messages = []
        };
    }

    private static PolledMessages OneMessage()
    {
        return new PolledMessages
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
                    Payload = [1],
                    UserHeaders = null
                }
            ]
        };
    }

    private static PolledMessagesRental NoAssignmentRental()
    {
        return new PolledMessagesRental(EmptyMemoryOwner.Instance)
        {
            PartitionId = PolledMessages.NoAssignedPartition,
            CurrentOffset = 0,
            Messages = []
        };
    }

    private static PolledMessagesRental OneMessageRental()
    {
        var owner = new RentedConsumerTests.TrackingMemoryOwner(16);

        return new PolledMessagesRental(owner)
        {
            PartitionId = 1,
            CurrentOffset = 0,
            Messages = RentedConsumerTests.BuildMessages(owner, 1)
        };
    }
}
