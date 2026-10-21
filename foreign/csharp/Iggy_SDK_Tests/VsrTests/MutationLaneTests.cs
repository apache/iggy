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

using Apache.Iggy.Configuration;
using Apache.Iggy.Contracts;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Utils;
using Apache.Iggy.Vsr;

namespace Apache.Iggy.Tests.VsrTests;

public sealed class MutationLaneTests
{
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
            new MutationLane(17, 3), CommandCodes.SEND_MESSAGES_CODE, new byte[1], default, true,
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
        var next = await EnterAsync(lanes, new MutationLane(17, 3));
        Assert.True(next.Identity.RequestId > old.Identity.RequestId);
        lanes.Complete(next);
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
                ReadOnlyMemory<byte>.Empty, new PartitionContext(17, 0, 1), false, cancellation.Token)
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
                new PartitionContext(17, 0, 1), false, cancellation.Token);
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
            false, TestContext.Current.CancellationToken));
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
                await Assert.ThrowsAsync<NotConnectedException>(() => restarting)
                    .WaitAsync(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
            }
            else
            {
                await cancellation.CancelAsync();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => restarting);
                lanes.Complete(previous);
            }
            Assert.False(lanes.Owns(previous));
            var next = EnterAsync(lanes, new MutationLane(17, 4));
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

    private static Task<ClientOperation> EnterAsync(MutationLanes lanes, MutationLane lane,
        CancellationToken? token = null) => lanes.EnterAsync(lane, CommandCodes.SEND_MESSAGES_CODE,
        ReadOnlyMemory<byte>.Empty, new PartitionContext(lane.Incarnation, 0, 1), true,
        token ?? TestContext.Current.CancellationToken);

    private static ConsensusSession Session() => new(7, 42, new byte[LoginRegister.BIND_SECRET_BYTES]);

    private static IggyClientConfigurator Configuration(int count = 256, long bytes = 64 * 1024 * 1024) => new()
    {
        BaseAddress = "127.0.0.1:8090",
        Protocol = Protocol.Tcp,
        MaxPendingOperations = count,
        MaxPendingOperationBytes = bytes
    };
}
