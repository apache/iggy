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
using Apache.Iggy.Messages;
using Apache.Iggy.Vsr;
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
    private const ulong BlockingOperationId = 73;
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

    public enum OffsetMutation
    {
        Store,
        StoreWithContext,
        Delete
    }

    public enum Resolution
    {
        Success,
        NotRetained,
        CommittedFailure
    }

    [Theory]
    [InlineData(OffsetMutation.Store, Resolution.Success)]
    [InlineData(OffsetMutation.StoreWithContext, Resolution.Success)]
    [InlineData(OffsetMutation.Delete, Resolution.Success)]
    [InlineData(OffsetMutation.Store, Resolution.NotRetained)]
    [InlineData(OffsetMutation.StoreWithContext, Resolution.NotRetained)]
    [InlineData(OffsetMutation.Delete, Resolution.NotRetained)]
    [InlineData(OffsetMutation.Store, Resolution.CommittedFailure)]
    [InlineData(OffsetMutation.StoreWithContext, Resolution.CommittedFailure)]
    [InlineData(OffsetMutation.Delete, Resolution.CommittedFailure)]
    public async Task given_a_blocked_offset_mutation_when_its_blocker_resolves_should_retry_once_with_the_same_scope(
        OffsetMutation mutation, Resolution resolution)
    {
        var attempts = new List<CancellationToken>();
        var client = MutationClient(token =>
        {
            attempts.Add(token);
            return attempts.Count == 1
                ? Task.FromException(new VsrLaneBlockedException(BlockingOperationId))
                : Task.CompletedTask;
        });
        client.Setup(mock => mock.ResolveOperationAsync(BlockingOperationId, It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .Returns((ulong _, TimeSpan timeout, CancellationToken token) =>
            {
                Assert.Equal(attempts[0], token);
                Assert.InRange(timeout.TotalMilliseconds, 1, 30_000);
                return resolution switch
                {
                    Resolution.NotRetained => Task.FromException(new VsrOperationNotRetainedException(BlockingOperationId)),
                    Resolution.CommittedFailure => Task.FromException(new IggyInvalidStatusCodeException(5009,
                        "The blocking operation was refused.", true)
                    { Committed = true }),
                    _ => Task.CompletedTask
                };
            });
        await using var consumer = new IggyConsumer(client.Object, Config(), NullLoggerFactory.Instance);
        using var cancellation = Timeout();

        await Mutate(consumer, mutation, cancellation.Token);

        Assert.Equal(2, attempts.Count);
        Assert.Equal(attempts[0], attempts[1]);
        client.Verify(mock => mock.ResolveOperationAsync(BlockingOperationId, It.IsAny<TimeSpan>(),
            It.IsAny<CancellationToken>()), Times.Once);
        client.Verify(mock => mock.AbandonOperationAsync(It.IsAny<ulong>()), Times.Never);
    }

    [Theory]
    [InlineData(OffsetMutation.Store)]
    [InlineData(OffsetMutation.StoreWithContext)]
    [InlineData(OffsetMutation.Delete)]
    public async Task given_a_blocked_offset_mutation_when_resolution_is_unknown_should_leave_it_unsent(
        OffsetMutation mutation)
    {
        var attempts = 0;
        var client = MutationClient(_ =>
        {
            attempts++;
            return Task.FromException(new VsrLaneBlockedException(BlockingOperationId));
        });
        client.Setup(mock => mock.ResolveOperationAsync(BlockingOperationId, It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new VsrRequestOutcomeUnknownException(new IOException(), BlockingOperationId));
        await using var consumer = new IggyConsumer(client.Object, Config(), NullLoggerFactory.Instance);

        var error = await Assert.ThrowsAsync<VsrLaneBlockedException>(() =>
            Mutate(consumer, mutation, TestContext.Current.CancellationToken));

        Assert.Equal(BlockingOperationId, error.OperationId);
        Assert.Equal(1, attempts);
        client.Verify(mock => mock.ResolveOperationAsync(BlockingOperationId, It.IsAny<TimeSpan>(),
            It.IsAny<CancellationToken>()), Times.Once);
        client.Verify(mock => mock.AbandonOperationAsync(It.IsAny<ulong>()), Times.Never);
    }

    [Fact]
    public async Task given_a_recovered_lane_when_another_blocker_wins_should_not_resolve_again()
    {
        var attempts = 0;
        var client = MutationClient(_ => Task.FromException(
            new VsrLaneBlockedException(BlockingOperationId + (ulong)attempts++)));
        client.Setup(mock => mock.ResolveOperationAsync(BlockingOperationId, It.IsAny<TimeSpan>(),
            It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
        await using var consumer = new IggyConsumer(client.Object, Config(), NullLoggerFactory.Instance);

        var error = await Assert.ThrowsAsync<VsrLaneBlockedException>(() =>
            consumer.StoreOffsetAsync(OriginalOffset, PartitionId, ct: TestContext.Current.CancellationToken));

        Assert.Equal(BlockingOperationId + 1, error.OperationId);
        Assert.Equal(2, attempts);
        client.Verify(mock => mock.ResolveOperationAsync(It.IsAny<ulong>(), It.IsAny<TimeSpan>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task given_a_blocked_offset_mutation_when_cancelled_during_resolution_should_not_retry()
    {
        var resolving = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var attempts = 0;
        var client = MutationClient(_ =>
        {
            attempts++;
            return Task.FromException(new VsrLaneBlockedException(BlockingOperationId));
        });
        client.Setup(mock => mock.ResolveOperationAsync(BlockingOperationId, It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .Returns(async (ulong _, TimeSpan _, CancellationToken token) =>
            {
                resolving.SetResult();
                await Task.Delay(System.Threading.Timeout.Infinite, token);
            });
        await using var consumer = new IggyConsumer(client.Object, Config(), NullLoggerFactory.Instance);
        using var cancellation = Timeout();

        var mutation = consumer.StoreOffsetAsync(OriginalOffset, PartitionId, ct: cancellation.Token);
        await resolving.Task.WaitAsync(cancellation.Token);
        await cancellation.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => mutation);
        Assert.Equal(1, attempts);
    }

    private enum DeadlineStage
    {
        InitialAttempt,
        BlockerResolution,
        NewAttempt
    }

    [Fact]
    public async Task given_a_consumer_operation_when_its_internal_deadline_expires_should_preserve_non_admission_or_blocker()
    {
        var outcomes = await Task.WhenAll(Enum.GetValues<DeadlineStage>().Select(ExpireAsync));

        Assert.All(outcomes, outcome =>
        {
            Assert.False(outcome.CallerCancelled);
            if (outcome.Stage == DeadlineStage.BlockerResolution)
            {
                Assert.Equal(BlockingOperationId, Assert.IsType<VsrLaneBlockedException>(outcome.Error).OperationId);
            }
            else
            {
                Assert.Equal(VsrError.TRANSIENT_NOT_ACCEPTED,
                    Assert.IsType<IggyInvalidStatusCodeException>(outcome.Error).StatusCode);
            }

            Assert.Equal(outcome.Stage == DeadlineStage.NewAttempt ? 2 : 1, outcome.Attempts);
            Assert.Equal(outcome.Stage == DeadlineStage.InitialAttempt ? 0 : 1, outcome.Resolutions);
        });
        static async Task<(DeadlineStage Stage, Exception? Error, int Attempts, int Resolutions, bool CallerCancelled)>
            ExpireAsync(DeadlineStage stage)
        {
            var attempts = 0;
            var resolutions = 0;
            var client = MutationClient(async token =>
            {
                if (++attempts == 1 && stage != DeadlineStage.InitialAttempt)
                {
                    throw new VsrLaneBlockedException(BlockingOperationId);
                }

                await Task.Delay(System.Threading.Timeout.Infinite, token);
            });
            client.Setup(mock => mock.ResolveOperationAsync(BlockingOperationId, It.IsAny<TimeSpan>(),
                    It.IsAny<CancellationToken>()))
                .Returns(async (ulong _, TimeSpan _, CancellationToken token) =>
                {
                    resolutions++;
                    if (stage == DeadlineStage.BlockerResolution)
                    {
                        await Task.Delay(System.Threading.Timeout.Infinite, token);
                    }
                });
            await using var consumer = new IggyConsumer(client.Object, Config(), NullLoggerFactory.Instance);
            using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
            cancellation.CancelAfter(TimeSpan.FromSeconds(40));

            var error = await Record.ExceptionAsync(() =>
                consumer.StoreOffsetAsync(OriginalOffset, PartitionId, ct: cancellation.Token));

            client.Verify(mock => mock.AbandonOperationAsync(It.IsAny<ulong>()), Times.Never);
            return (stage, error, attempts, resolutions, cancellation.IsCancellationRequested);
        }
    }

    [Fact]
    public async Task given_a_delayed_lane_refusal_when_resolving_should_use_the_remaining_operation_budget()
    {
        var attempts = 0;
        var client = MutationClient(async token =>
        {
            if (++attempts == 1)
            {
                await Task.Delay(50, token);
                throw new VsrLaneBlockedException(BlockingOperationId);
            }
        });
        TimeSpan? resolutionTimeout = null;
        client.Setup(mock => mock.ResolveOperationAsync(BlockingOperationId, It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .Callback((ulong _, TimeSpan timeout, CancellationToken _) => resolutionTimeout = timeout)
            .Returns(Task.CompletedTask);
        await using var consumer = new IggyConsumer(client.Object, Config(), NullLoggerFactory.Instance);

        await consumer.StoreOffsetAsync(OriginalOffset, PartitionId, ct: TestContext.Current.CancellationToken);

        Assert.NotNull(resolutionTimeout);
        Assert.InRange(resolutionTimeout.Value.TotalMilliseconds, 1, 29_975);
        Assert.Equal(2, attempts);
    }

    [Fact]
    public async Task given_a_progress_poll_behind_a_retained_mutation_when_receiving_should_stop_without_resolving_it()
    {
        var client = ClientMock(new ConcurrentQueue<(ulong, PartitionContext)>());
        var attempts = 0;
        client.Setup(mock => mock.PollMessagesAsync(It.IsAny<Identifier>(), It.IsAny<Identifier>(),
                It.IsAny<uint?>(), It.IsAny<Consumer>(), It.IsAny<Func<uint, PollingStrategy>>(), It.IsAny<uint>(),
                It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .Returns((Identifier _, Identifier _, uint? _, Consumer _, Func<uint, PollingStrategy> strategy, uint _,
                bool _, CancellationToken token) =>
            {
                strategy(PartitionId);
                return ++attempts > 1
                    ? Idle<PolledMessages>(token)
                    : Task.FromException<PolledMessages>(new VsrLaneBlockedException(BlockingOperationId));
            });
        await using var consumer = new IggyConsumer(client.Object, Config(), NullLoggerFactory.Instance);
        await consumer.InitAsync(TestContext.Current.CancellationToken);
        using var cancellation = Timeout();
        await using var messages = consumer.ReceiveAsync(cancellation.Token).GetAsyncEnumerator(cancellation.Token);

        var error = await Assert.ThrowsAsync<VsrLaneBlockedException>(() => messages.MoveNextAsync().AsTask());

        Assert.Equal(BlockingOperationId, error.OperationId);
        Assert.Equal(1, attempts);
        client.Verify(mock => mock.ResolveOperationAsync(It.IsAny<ulong>(), It.IsAny<TimeSpan>(),
            It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task given_an_uncertain_progress_poll_when_receiving_should_report_it_and_poll_again()
    {
        var client = ClientMock(new ConcurrentQueue<(ulong, PartitionContext)>());
        var attempts = 0;
        var unknown = new VsrRequestOutcomeUnknownException(new IOException());
        client.Setup(mock => mock.PollMessagesAsync(It.IsAny<Identifier>(), It.IsAny<Identifier>(),
                It.IsAny<uint?>(), It.IsAny<Consumer>(), It.IsAny<Func<uint, PollingStrategy>>(), It.IsAny<uint>(),
                It.IsAny<bool>(), It.IsAny<CancellationToken>()))
            .Returns((Identifier _, Identifier _, uint? _, Consumer _, Func<uint, PollingStrategy> strategy, uint _,
                bool _, CancellationToken _) =>
            {
                strategy(PartitionId);
                return ++attempts > 1
                    ? Task.FromResult(Batch(OriginalOffset, Original))
                    : Task.FromException<PolledMessages>(unknown);
            });
        await using var consumer = new IggyConsumer(client.Object, Config(), NullLoggerFactory.Instance);
        var errors = new ConcurrentQueue<Exception>();
        consumer.SubscribeToErrorEvents(error =>
        {
            errors.Enqueue(error.Exception);
            return Task.CompletedTask;
        });
        await consumer.InitAsync(TestContext.Current.CancellationToken);
        using var cancellation = Timeout();
        await using var messages = consumer.ReceiveAsync(cancellation.Token).GetAsyncEnumerator(cancellation.Token);

        Assert.True(await messages.MoveNextAsync());

        Assert.Equal(OriginalOffset, messages.Current.CurrentOffset);
        Assert.Same(unknown, Assert.Single(errors));
        Assert.Equal(2, attempts);
        client.Verify(mock => mock.ResolveOperationAsync(It.IsAny<ulong>(), It.IsAny<TimeSpan>(),
            It.IsAny<CancellationToken>()), Times.Never);
    }

    private static Mock<IIggyClient> MutationClient(Func<CancellationToken, Task> mutation)
    {
        var client = new Mock<IIggyClient>(MockBehavior.Loose);
        client.Setup(mock => mock.StoreOffsetAsync(It.IsAny<Consumer>(), It.IsAny<Identifier>(),
                It.IsAny<Identifier>(), OriginalOffset, (uint?)PartitionId, It.IsAny<CancellationToken>()))
            .Returns((Consumer _, Identifier _, Identifier _, ulong _, uint? _, CancellationToken token) => mutation(token));
        client.Setup(mock => mock.StoreOffsetAsync(It.IsAny<Consumer>(), It.IsAny<Identifier>(),
                It.IsAny<Identifier>(), OriginalOffset, PartitionId, Original, It.IsAny<CancellationToken>()))
            .Returns((Consumer _, Identifier _, Identifier _, ulong _, uint _, PartitionContext _, CancellationToken token) => mutation(token));
        client.Setup(mock => mock.DeleteOffsetAsync(It.IsAny<Consumer>(), It.IsAny<Identifier>(),
                It.IsAny<Identifier>(), PartitionId, It.IsAny<CancellationToken>()))
            .Returns((Consumer _, Identifier _, Identifier _, uint? _, CancellationToken token) => mutation(token));
        return client;
    }

    private static Task Mutate(IggyConsumer consumer, OffsetMutation mutation, CancellationToken token)
    {
        return mutation switch
        {
            OffsetMutation.Store => consumer.StoreOffsetAsync(OriginalOffset, PartitionId, ct: token),
            OffsetMutation.StoreWithContext => consumer.StoreOffsetAsync(OriginalOffset, PartitionId, Original, token),
            OffsetMutation.Delete => consumer.DeleteOffsetAsync(PartitionId, token),
            _ => throw new ArgumentOutOfRangeException(nameof(mutation))
        };
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
