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

using System.Buffers;
using Apache.Iggy.Configuration;
using Apache.Iggy.Contracts;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Utils;
using Apache.Iggy.Vsr;

namespace Apache.Iggy.Tests.VsrTests;

public sealed class MutationLaneTests
{
    // The generation of a session that a test never resets.
    private const ulong FirstGeneration = 0;

    private static readonly TimeSpan WaitLimit = TimeSpan.FromSeconds(2);

    // Long enough for every waiter that a release woke to run its admission check.
    private static readonly TimeSpan SettleTime = TimeSpan.FromMilliseconds(200);

    public enum BuildInterruption
    {
        EncodeFailure,
        Cancellation,
        SessionReset
    }

    [Fact]
    public async Task given_a_retained_operation_when_an_alias_enters_the_group_should_block_without_consuming_an_id()
    {
        var session = Session();
        var lanes = new MutationLanes(session, Configuration());
        var lane = new MutationLane(17, 3);
        var operation = await EnterAsync(lanes, lane);
        lanes.Retain(operation);
        var counter = session.RequestCounter;

        var blocked = await Assert.ThrowsAsync<VsrLaneBlockedException>(() => EnterAsync(lanes, lane));
        Assert.Equal(operation.Identity.RequestId, blocked.OperationId);
        Assert.Equal(counter, session.RequestCounter);
        var other = await EnterAsync(lanes, new MutationLane(17, 4));
        lanes.Complete(other);
        lanes.Complete(operation);
    }

    [Fact]
    public async Task given_fifo_waiters_when_a_later_waiter_is_canceled_should_preserve_admission_order()
    {
        var lanes = new MutationLanes(Session(), Configuration());
        var lane = new MutationLane(17, 3);
        var active = await EnterAsync(lanes, lane);
        var first = EnterAsync(lanes, lane);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        var canceled = EnterAsync(lanes, lane, cancellation.Token);
        var last = EnterAsync(lanes, lane);
        await cancellation.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => canceled);
        lanes.Complete(active);
        var firstOperation = await first;
        Assert.False(last.IsCompleted);
        lanes.Complete(firstOperation);
        lanes.Complete(await last);
    }

    [Fact]
    public async Task given_full_active_and_retained_capacity_when_waiting_should_share_one_budget()
    {
        var session = Session();
        var lanes = new MutationLanes(session, Configuration(1));
        var active = await EnterAsync(lanes, new MutationLane(17, 3));
        lanes.Retain(active);
        var counter = session.RequestCounter;
        var waiting = EnterAsync(lanes, new MutationLane(17, 4));
        Assert.False(waiting.IsCompleted);
        Assert.Equal(counter, session.RequestCounter);
        lanes.Complete(active);
        lanes.Complete(await waiting);
    }

    [Fact]
    public async Task given_a_writer_reference_when_abandoning_should_hold_bytes_until_the_frame_is_released()
    {
        var session = Session();
        var lanes = new MutationLanes(session, Configuration(1, bytes: VsrHeader.HEADER_SIZE));
        var active = await EnterAsync(lanes, new MutationLane(17, 3));
        active.Frame.Acquire();
        lanes.Complete(active);
        var waiting = EnterAsync(lanes, new MutationLane(17, 4));
        Assert.False(waiting.IsCompleted);
        active.Frame.Release();
        lanes.Complete(await waiting);
    }

    [Fact]
    public async Task given_a_writer_reference_when_abandoning_should_release_the_logical_count_before_its_bytes()
    {
        var lanes = new MutationLanes(Session(), Configuration(1, bytes: VsrHeader.HEADER_SIZE * 2));
        var original = await EnterAsync(lanes, new MutationLane(17, 3));
        original.Frame.Acquire();
        try
        {
            lanes.Complete(original);
            var next = await EnterAsync(lanes, new MutationLane(17, 4))
                .WaitAsync(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
            lanes.Complete(next);
        }
        finally
        {
            original.Frame.Release();
        }
    }

    [Fact]
    public async Task given_an_oversize_frame_when_entering_should_not_consume_identity_or_capacity()
    {
        var session = Session();
        var lanes = new MutationLanes(session, Configuration(bytes: VsrHeader.HEADER_SIZE));
        var counter = session.RequestCounter;
        var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => lanes.EnterAsync(
            new MutationLane(17, 3), CommandCodes.SEND_MESSAGES_CODE, new byte[1], default, true, session.Generation,
            TestContext.Current.CancellationToken));
        Assert.Equal(VsrError.INVALID_SIZE_BYTES, error.StatusCode);
        Assert.False(error.FromServer);
        Assert.Equal(counter, session.RequestCounter);
        lanes.Complete(await EnterAsync(lanes, new MutationLane(17, 3)));
    }

    [Fact]
    public async Task given_a_reset_when_an_old_operation_is_retained_should_free_capacity_without_reusing_its_id()
    {
        var session = Session();
        var lanes = new MutationLanes(session, Configuration(1));
        var old = await EnterAsync(lanes, new MutationLane(17, 3));
        lanes.Retain(old);
        session.Reset();
        lanes.Reset();
        session.Resolve(VsrOperation.Register);
        session.Bind(43, new byte[LoginRegister.BIND_SECRET_BYTES]);
        Assert.Throws<VsrOperationNotRetainedException>(() => lanes.Retained(old.Identity.RequestId));
        var next = await EnterAsync(lanes, new MutationLane(17, 3), generation: session.Generation);
        Assert.True(next.Identity.RequestId > old.Identity.RequestId);
        lanes.Complete(next);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task given_a_session_recovery_when_operations_wait_should_go_on_only_under_a_session_that_continues_theirs(
        bool continues)
    {
        var session = Session();
        var recovery = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var lanes = new MutationLanes(session, Configuration(2), () => recovery.Task.IsCompleted ? null : recovery.Task);
        var live = await EnterAsync(lanes, new MutationLane(17, 3));
        var retained = await EnterAsync(lanes, new MutationLane(17, 4));
        lanes.Retain(retained);
        var waiting = EnterAsync(lanes, new MutationLane(17, 5));

        session.Reset(continues: true);
        lanes.DropRetained();
        Assert.True(lanes.Owns(live));
        Assert.Throws<VsrOperationNotRetainedException>(() => lanes.Retained(retained.Identity.RequestId));
        var restarting = lanes.RestartAsync(live, ReadOnlyMemory<byte>.Empty, TestContext.Current.CancellationToken);
        session.Resolve(VsrOperation.Register);
        session.Bind(43, new byte[LoginRegister.BIND_SECRET_BYTES], continues);
        Assert.False(waiting.IsCompleted);
        Assert.False(restarting.IsCompleted);
        recovery.SetResult();

        if (continues)
        {
            ClientOperation[] operations =
            [
                await restarting.WaitAsync(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken),
                await waiting.WaitAsync(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken)
            ];
            Assert.All(operations, operation => Assert.Equal((session.Generation, session.ClientId),
                (operation.Generation, operation.Identity.ClientId)));
            Array.ForEach(operations, lanes.Complete);
        }
        else
        {
            await AssertUnauthenticatedAsync(restarting);
            await AssertUnauthenticatedAsync(waiting);
            lanes.Complete(live);
        }
    }

    [Fact]
    public async Task given_a_running_replay_when_abandoned_should_free_its_slot_at_once_and_its_lane_when_it_ends()
    {
        var lanes = new MutationLanes(Session(), Configuration(1));
        var lane = new MutationLane(17, 3);
        var operation = await EnterAsync(lanes, lane);
        var id = operation.Identity.RequestId;
        lanes.Retain(operation);
        var replay = Assert.IsType<ClientOperation>(lanes.AcquireRetained(id));
        Assert.True(lanes.BeginReplay(replay));
        var other = EnterAsync(lanes, new MutationLane(17, 4));
        Assert.False(other.IsCompleted);

        lanes.Abandon(id);
        lanes.Retain(replay);

        Assert.Throws<VsrOperationNotRetainedException>(() => lanes.Retained(id));
        Assert.False(replay.Lifetime.IsCancellationRequested);
        lanes.Complete(await other.WaitAsync(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken));
        var same = EnterAsync(lanes, lane);
        Assert.False(same.IsCompleted);
        lanes.EndReplay(replay);
        replay.Frame.Release();
        var follower = await same.WaitAsync(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        var third = EnterAsync(lanes, new MutationLane(17, 5));
        Assert.False(third.IsCompleted);
        lanes.Complete(follower);
        lanes.Complete(await third.WaitAsync(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_full_mutation_capacity_when_progress_enters_another_lane_should_ignore_mutation_budgets(bool retained)
    {
        var lanes = new MutationLanes(Session(), Configuration(1, VsrHeader.HEADER_SIZE));
        var mutation = await EnterAsync(lanes, new MutationLane(17, 3));
        if (retained)
        {
            lanes.Retain(mutation);
        }
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        try
        {
            var progress = await lanes.EnterAsync(new MutationLane(17, 4), CommandCodes.POLL_MESSAGES_CODE,
                ReadOnlyMemory<byte>.Empty, new PartitionContext(17, 0, 1), false, FirstGeneration, cancellation.Token)
                .WaitAsync(TimeSpan.FromSeconds(2), cancellation.Token);
            lanes.Complete(progress);
            Assert.True(lanes.Owns(mutation));
        }
        finally
        {
            await cancellation.CancelAsync();
            lanes.Reset();
        }
    }

    [Fact]
    public async Task given_a_large_progress_frame_when_entering_should_hold_its_lane_without_charging_mutation_capacity()
    {
        var lanes = new MutationLanes(Session(), Configuration(1, VsrHeader.HEADER_SIZE));
        var lane = new MutationLane(17, 3);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        try
        {
            var progress = await lanes.EnterAsync(lane, CommandCodes.POLL_MESSAGES_CODE, new byte[1],
                new PartitionContext(17, 0, 1), false, FirstGeneration, cancellation.Token);
            var follower = EnterAsync(lanes, lane, cancellation.Token);
            var independent = await EnterAsync(lanes, new MutationLane(17, 4), cancellation.Token)
                .WaitAsync(TimeSpan.FromSeconds(2), cancellation.Token);
            lanes.Complete(independent);
            Assert.False(follower.IsCompleted);
            lanes.Complete(progress);
            lanes.Complete(await follower.WaitAsync(TimeSpan.FromSeconds(2), cancellation.Token));
        }
        finally
        {
            await cancellation.CancelAsync();
            lanes.Reset();
        }
    }

    [Fact]
    public async Task given_a_retained_operation_when_progress_enters_its_lane_should_report_the_blocker_without_allocating_an_id()
    {
        var session = Session();
        var lanes = new MutationLanes(session, Configuration());
        var lane = new MutationLane(17, 3);
        var mutation = await EnterAsync(lanes, lane);
        lanes.Retain(mutation);
        var counter = session.RequestCounter;
        var blocked = await Assert.ThrowsAsync<VsrLaneBlockedException>(() => lanes.EnterAsync(lane,
            CommandCodes.POLL_MESSAGES_CODE, ReadOnlyMemory<byte>.Empty, new PartitionContext(17, 0, 1),
            false, session.Generation, TestContext.Current.CancellationToken));
        Assert.Equal(mutation.Identity.RequestId, blocked.OperationId);
        Assert.Equal(counter, session.RequestCounter);
        lanes.Complete(mutation);
    }

    [Fact]
    public async Task given_a_queued_follower_when_restarting_should_keep_the_original_lane_position()
    {
        var lanes = new MutationLanes(Session(), Configuration());
        var lane = MutationLane.Metadata;
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        try
        {
            var previous = await EnterAsync(lanes, lane, cancellation.Token);
            var follower = EnterAsync(lanes, lane, cancellation.Token);
            var restarting = lanes.RestartAsync(previous, ReadOnlyMemory<byte>.Empty, cancellation.Token);
            var first = await Task.WhenAny(restarting, follower).WaitAsync(TimeSpan.FromSeconds(2), cancellation.Token);
            Assert.Same(restarting, first);
            var restarted = await restarting;
            Assert.True(restarted.Identity.RequestId > previous.Identity.RequestId);
            Assert.Equal(previous.Context, restarted.Context);
            Assert.False(lanes.Owns(previous));
            Assert.True(previous.Lifetime.IsCancellationRequested);
            Assert.False(follower.IsCompleted);
            lanes.Complete(restarted);
            lanes.Complete(await follower.WaitAsync(TimeSpan.FromSeconds(2), cancellation.Token));
        }
        finally
        {
            await cancellation.CancelAsync();
            lanes.Reset();
        }
    }

    [Fact]
    public async Task given_a_writer_alias_when_restarting_should_hold_the_lane_and_slot_while_waiting_for_bytes()
    {
        var lanes = new MutationLanes(Session(), Configuration(1, VsrHeader.HEADER_SIZE));
        var lane = MutationLane.Metadata;
        var previous = await EnterAsync(lanes, lane);
        previous.Frame.Acquire();
        var writerReleased = false;
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        try
        {
            var follower = EnterAsync(lanes, lane, cancellation.Token);
            var independent = EnterAsync(lanes, new MutationLane(17, 4), cancellation.Token);
            var restarting = lanes.RestartAsync(previous, ReadOnlyMemory<byte>.Empty, cancellation.Token);
            Assert.False(restarting.IsCompleted);
            Assert.True(lanes.Owns(previous));
            Assert.False(follower.IsCompleted);
            Assert.False(independent.IsCompleted);
            previous.Frame.Release();
            writerReleased = true;
            var first = await Task.WhenAny(restarting, follower, independent)
                .WaitAsync(TimeSpan.FromSeconds(2), cancellation.Token);
            Assert.Same(restarting, first);
            lanes.Complete(await restarting);
            var next = await Task.WhenAny(follower, independent).WaitAsync(TimeSpan.FromSeconds(2), cancellation.Token);
            lanes.Complete(await next);
            var last = ReferenceEquals(next, follower) ? independent : follower;
            lanes.Complete(await last.WaitAsync(TimeSpan.FromSeconds(2), cancellation.Token));
        }
        finally
        {
            if (!writerReleased)
            {
                previous.Frame.Release();
            }
            await cancellation.CancelAsync();
            lanes.Reset();
        }
    }

    [Fact]
    public async Task given_capacity_for_two_frames_when_restarting_should_charge_the_old_writer_until_released()
    {
        var lanes = new MutationLanes(Session(), Configuration(2, VsrHeader.HEADER_SIZE * 2));
        var previous = await EnterAsync(lanes, MutationLane.Metadata);
        previous.Frame.Acquire();
        var writerReleased = false;
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        try
        {
            var restarted = await lanes.RestartAsync(previous, ReadOnlyMemory<byte>.Empty, cancellation.Token)
                .WaitAsync(TimeSpan.FromSeconds(2), cancellation.Token);
            var independent = EnterAsync(lanes, new MutationLane(17, 4), cancellation.Token);
            Assert.False(independent.IsCompleted);
            previous.Frame.Release();
            writerReleased = true;
            lanes.Complete(await independent.WaitAsync(TimeSpan.FromSeconds(2), cancellation.Token));
            Assert.True(lanes.Owns(restarted));
            lanes.Complete(restarted);
        }
        finally
        {
            if (!writerReleased)
            {
                previous.Frame.Release();
            }
            await cancellation.CancelAsync();
            lanes.Reset();
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_a_blocked_restart_when_canceled_or_reset_should_release_its_slot_and_preserve_writer_bytes(bool reset)
    {
        var session = Session();
        var lanes = new MutationLanes(session, Configuration(1, VsrHeader.HEADER_SIZE));
        var previous = await EnterAsync(lanes, MutationLane.Metadata);
        previous.Frame.Acquire();
        var writerReleased = false;
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        try
        {
            var restarting = lanes.RestartAsync(previous, ReadOnlyMemory<byte>.Empty, cancellation.Token);
            Assert.False(restarting.IsCompleted);
            if (reset)
            {
                session.Reset();
                lanes.Reset();
                session.Resolve(VsrOperation.Register);
                session.Bind(43, new byte[LoginRegister.BIND_SECRET_BYTES]);
                await AssertUnauthenticatedAsync(restarting);
            }
            else
            {
                await cancellation.CancelAsync();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => restarting);
                lanes.Complete(previous);
            }
            Assert.False(lanes.Owns(previous));
            var next = EnterAsync(lanes, new MutationLane(17, 4), generation: session.Generation);
            Assert.False(next.IsCompleted);
            previous.Frame.Release();
            writerReleased = true;
            lanes.Complete(await next.WaitAsync(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken));
        }
        finally
        {
            if (!writerReleased)
            {
                previous.Frame.Release();
            }
            await cancellation.CancelAsync();
            lanes.Reset();
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_a_large_frame_waiting_for_bytes_when_a_smaller_one_enters_should_wait_until_the_large_one_leaves_the_queue(
        bool canceled)
    {
        var lanes = new MutationLanes(Session(), Configuration(bytes: VsrHeader.HEADER_SIZE * 2 + 1));
        var active = await EnterAsync(lanes, new MutationLane(17, 3));
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        var large = EnterAsync(lanes, new MutationLane(17, 4), cancellation.Token,
            body: new byte[VsrHeader.HEADER_SIZE + 1]);
        var small = EnterAsync(lanes, new MutationLane(17, 5));
        Assert.False(large.IsCompleted);
        Assert.False(small.IsCompleted);

        if (canceled)
        {
            await cancellation.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => large);
            lanes.Complete(await small.WaitAsync(WaitLimit, TestContext.Current.CancellationToken));
            lanes.Complete(active);
        }
        else
        {
            lanes.Complete(active);
            var admitted = await large.WaitAsync(WaitLimit, TestContext.Current.CancellationToken);
            Assert.False(small.IsCompleted);
            lanes.Complete(admitted);
            lanes.Complete(await small.WaitAsync(WaitLimit, TestContext.Current.CancellationToken));
        }
    }

    [Fact]
    public async Task given_waiters_on_many_lanes_when_one_lane_completes_should_wake_only_its_waiter()
    {
        const int LaneCount = 8;
        var session = Session();
        var recovery = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var woken = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var checks = 0;
        // While the session signs in again, every admission check asks for the recovery, once per wake.
        var lanes = new MutationLanes(session, Configuration(), () =>
        {
            if (Interlocked.Increment(ref checks) > LaneCount)
            {
                woken.TrySetResult();
            }
            return recovery.Task;
        });
        var active = new ClientOperation[LaneCount];
        for (var index = 0; index < LaneCount; index++)
        {
            active[index] = await EnterAsync(lanes, new MutationLane(17, (uint)index));
        }
        session.Reset(continues: true);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        var waiting = active.Select(operation => EnterAsync(lanes, operation.Lane, cancellation.Token)).ToArray();
        Assert.Equal(LaneCount, Volatile.Read(ref checks));

        lanes.Complete(active[0]);
        await woken.Task.WaitAsync(WaitLimit, TestContext.Current.CancellationToken);
        await Task.Delay(SettleTime, TestContext.Current.CancellationToken);

        Assert.Equal(LaneCount + 1, Volatile.Read(ref checks));
        await cancellation.CancelAsync();
        foreach (var task in waiting)
        {
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => task);
        }
        lanes.Reset();
    }

    [Theory]
    [InlineData(BuildInterruption.EncodeFailure, false)]
    [InlineData(BuildInterruption.Cancellation, false)]
    [InlineData(BuildInterruption.SessionReset, false)]
    [InlineData(BuildInterruption.EncodeFailure, true)]
    [InlineData(BuildInterruption.Cancellation, true)]
    [InlineData(BuildInterruption.SessionReset, true)]
    public async Task given_a_reserved_frame_when_its_build_is_interrupted_should_give_the_reservation_to_the_next_waiter(
        BuildInterruption interruption, bool restart)
    {
        var session = Session();
        var lanes = new MutationLanes(session, Configuration(2, VsrHeader.HEADER_SIZE + 1));
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        var failure = new InvalidOperationException("The body failed to encode.");
        Task<ClientOperation>? next = null;
        // The build reads the body, so the interruption lands after the reservation and before the commit.
        var body = new InterruptingBody(() =>
        {
            if (interruption == BuildInterruption.SessionReset)
            {
                session.Reset();
                lanes.Reset();
                session.Resolve(VsrOperation.Register);
                session.Bind(43, new byte[LoginRegister.BIND_SECRET_BYTES]);
            }
            next = EnterAsync(lanes, new MutationLane(17, 4), generation: session.Generation);
            if (interruption == BuildInterruption.EncodeFailure)
            {
                throw failure;
            }
            if (interruption == BuildInterruption.Cancellation)
            {
                cancellation.Cancel();
            }
        });
        var previous = restart ? await EnterAsync(lanes, MutationLane.Metadata) : null;

        var building = previous is null
            ? EnterAsync(lanes, new MutationLane(17, 3), cancellation.Token, body: body.Body)
            : lanes.RestartAsync(previous, body.Body, cancellation.Token);

        switch (interruption)
        {
            case BuildInterruption.EncodeFailure:
                Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() => building));
                break;
            case BuildInterruption.Cancellation:
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => building);
                break;
            default:
                await AssertUnauthenticatedAsync(building);
                break;
        }
        Assert.NotNull(next);
        lanes.Complete(await next.WaitAsync(WaitLimit, TestContext.Current.CancellationToken));
        if (previous is not null)
        {
            Assert.Equal(interruption != BuildInterruption.SessionReset, lanes.Owns(previous));
            lanes.Complete(previous);
        }
    }

    [Fact]
    public async Task given_waiters_for_a_lane_a_slot_and_bytes_when_the_lanes_reset_should_fail_each_of_them()
    {
        var session = Session();
        var lanes = new MutationLanes(session, Configuration(2, VsrHeader.HEADER_SIZE * 2));
        var lane = new MutationLane(17, 3);
        var active = await EnterAsync(lanes, lane);
        var restarted = await EnterAsync(lanes, MutationLane.Metadata);
        // Writer aliases keep both frames charged, so no release after the reset reaches the slot and byte waiters.
        active.Frame.Acquire();
        restarted.Frame.Acquire();
        try
        {
            Task[] waiting =
            [
                EnterAsync(lanes, lane),
                EnterAsync(lanes, new MutationLane(17, 4)),
                lanes.RestartAsync(restarted, ReadOnlyMemory<byte>.Empty, TestContext.Current.CancellationToken)
            ];
            Assert.DoesNotContain(waiting, task => task.IsCompleted);

            session.Reset();
            lanes.Reset();

            foreach (var task in waiting)
            {
                await AssertUnauthenticatedAsync(task);
            }
        }
        finally
        {
            active.Frame.Release();
            restarted.Frame.Release();
        }
    }

    [Fact]
    public async Task given_a_waiter_made_while_no_session_is_bound_when_the_caller_signs_in_should_end_it()
    {
        var session = Session();
        var lanes = new MutationLanes(session, Configuration(1));
        var holder = await EnterAsync(lanes, new MutationLane(17, 3));
        session.Reset(continues: true);
        var waiting = EnterAsync(lanes, new MutationLane(17, 4), generation: session.Generation);
        Assert.False(waiting.IsCompleted);

        session.Resolve(VsrOperation.Register);
        session.Bind(43, new byte[LoginRegister.BIND_SECRET_BYTES]);
        lanes.Complete(holder);

        await AssertUnauthenticatedAsync(waiting);
    }

    private static async Task AssertUnauthenticatedAsync(Task task)
    {
        var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => task)
            .WaitAsync(WaitLimit, TestContext.Current.CancellationToken);
        Assert.Equal(VsrError.UNAUTHENTICATED, error.StatusCode);
        Assert.False(error.FromServer);
    }

    private static Task<ClientOperation> EnterAsync(MutationLanes lanes, MutationLane lane,
        CancellationToken? token = null, ulong generation = FirstGeneration, ReadOnlyMemory<byte> body = default) =>
        lanes.EnterAsync(lane, CommandCodes.SEND_MESSAGES_CODE, body, new PartitionContext(lane.Incarnation, 0, 1), true,
            generation, token ?? TestContext.Current.CancellationToken);

    private static ConsensusSession Session() => new(7, 42, new byte[LoginRegister.BIND_SECRET_BYTES]);

    private static IggyClientConfigurator Configuration(int count = 256, long bytes = 64 * 1024 * 1024) => new()
    {
        BaseAddress = "127.0.0.1:8090",
        Protocol = Protocol.Tcp,
        MaxPendingOperations = count,
        MaxPendingOperationBytes = bytes
    };

    /// <summary>A one-byte body that runs <paramref name="interrupt" /> the first time its bytes are read.</summary>
    private sealed class InterruptingBody(Action interrupt) : MemoryManager<byte>
    {
        private readonly byte[] _bytes = new byte[1];
        private Action? _interrupt = interrupt;

        // MemoryManager.Memory reads the span for its length, which would interrupt before the build.
        internal ReadOnlyMemory<byte> Body => CreateMemory(_bytes.Length);

        public override Span<byte> GetSpan()
        {
            Interlocked.Exchange(ref _interrupt, null)?.Invoke();
            return _bytes;
        }

        public override MemoryHandle Pin(int elementIndex = 0) => throw new NotSupportedException();

        public override void Unpin()
        {
        }

        protected override void Dispose(bool disposing)
        {
        }
    }
}
