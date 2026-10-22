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

using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Reflection;
using Apache.Iggy.Configuration;
using Apache.Iggy.Consumers;
using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Tcp;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient.Implementations;
using Apache.Iggy.Kinds;
using Apache.Iggy.Tests.Utils;
using Apache.Iggy.Utils;
using Apache.Iggy.Messages;
using Partitioning = Apache.Iggy.Kinds.Partitioning;
using Apache.Iggy.Vsr;
using Microsoft.Extensions.Logging.Abstractions;
using static Apache.Iggy.Tests.VsrTests.MockFrames;

namespace Apache.Iggy.Tests.VsrTests;

public sealed partial class NativeRecoveryTests
{
    private const int ZeroCommittedOperation = -1;
    private static readonly Identifier Stream = Identifier.Numeric(1);
    private static readonly Identifier Topic = Identifier.Numeric(2);

    [Fact]
    public async Task given_an_uncertain_write_when_resolving_should_replay_its_frozen_identity_and_unblock_only_its_group()
    {
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync();
        var operationId = await RetainWriteAsync(cluster, client);
        var original = Assert.Single(cluster.Writes);
        var blocked = await Assert.ThrowsAsync<VsrLaneBlockedException>(() => StoreAsync(client, cluster.Token));
        Assert.Equal(operationId, blocked.OperationId);
        var alias = await Assert.ThrowsAsync<VsrLaneBlockedException>(() => client.StoreOffsetAsync(Consumer.New(3),
            Identifier.String("stream"), Identifier.String("topic"), 20, 0, cluster.Token));
        Assert.Equal(operationId, alias.OperationId);
        await StoreAsync(client, cluster.Token, partition: 1);
        await client.GetOffsetAsync(Consumer.New(3), Stream, Topic, 0, cluster.Token);
        await client.ResolveOperationAsync(operationId, TimeSpan.FromSeconds(5), cluster.Token);
        var replay = cluster.Writes.Last();
        Assert.Equal(original.RequestId, replay.RequestId);
        Assert.Equal(original.ClientId, replay.ClientId);
        Assert.Equal(original.Session, replay.Session);
        Assert.Equal(original.Context, replay.Context);
        Assert.Equal(original.Body, replay.Body);
        await StoreAsync(client, cluster.Token);
        Assert.True(cluster.Writes.Last().RequestId > operationId);
        await client.ResolveOperationAsync(operationId, TimeSpan.FromSeconds(1), cluster.Token);
        Assert.Equal(4, cluster.Writes.Count);
    }

    [Theory]
    [InlineData((byte)0, false)]
    [InlineData((byte)1, true)]
    [InlineData((byte)2, false)]
    public async Task given_an_uncertain_write_when_a_business_rejection_arrives_should_require_commit_proof(
        byte marker, bool committed)
    {
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync();
        var operationId = await RetainWriteAsync(cluster, client);
        cluster.OnWrite = request => Result(request, VsrError.FEATURE_UNAVAILABLE, marker);
        if (committed)
        {
            var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() =>
                client.ResolveOperationAsync(operationId, TimeSpan.FromSeconds(5), cluster.Token));
            Assert.Equal(VsrError.FEATURE_UNAVAILABLE, error.StatusCode);
            Assert.True(error.Committed);
            cluster.OnWrite = request => Result(request);
            await StoreAsync(client, cluster.Token);
        }
        else
        {
            var error = await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() =>
                client.ResolveOperationAsync(operationId, TimeSpan.FromSeconds(5), cluster.Token));
            Assert.Equal(operationId, error.OperationId);
            var blocked = await Assert.ThrowsAsync<VsrLaneBlockedException>(() => StoreAsync(client, cluster.Token));
            Assert.Equal(operationId, blocked.OperationId);
            await client.AbandonOperationAsync(operationId);
        }
        await Assert.ThrowsAsync<VsrOperationNotRetainedException>(() => client.AbandonOperationAsync(operationId));
    }

    [Fact]
    public async Task given_an_uncertain_write_when_abandoned_should_release_admission_without_resending_it()
    {
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync(maxOperations: 1);
        var operationId = await RetainWriteAsync(cluster, client);
        using var waiting = CancellationTokenSource.CreateLinkedTokenSource(cluster.Token);
        var other = StoreAsync(client, waiting.Token, partition: 1);
        await waiting.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => other);
        Assert.Single(cluster.Writes);
        await client.AbandonOperationAsync(operationId);
        await Assert.ThrowsAsync<VsrOperationNotRetainedException>(() =>
            client.ResolveOperationAsync(operationId, TimeSpan.FromSeconds(1), cluster.Token));
        Assert.Single(cluster.Writes);
        await StoreAsync(client, cluster.Token, partition: 1);
        Assert.Equal(2, cluster.Writes.Count);
    }

    [Fact]
    public async Task given_an_uncertain_write_when_relogin_resets_the_session_should_forget_it_and_preserve_the_counter()
    {
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync(maxOperations: 1);
        var operationId = await RetainWriteAsync(cluster, client);
        var original = Assert.Single(cluster.Writes);
        var generation = ((ISessionGenerationProvider)client).SessionGeneration;
        await client.LogoutUserAsync(cluster.Token);
        await client.LoginUserAsync("other", "secret", cluster.Token);
        Assert.NotEqual(generation, ((ISessionGenerationProvider)client).SessionGeneration);
        await Assert.ThrowsAsync<VsrOperationNotRetainedException>(() =>
            client.ResolveOperationAsync(operationId, TimeSpan.FromSeconds(1), cluster.Token));
        Assert.Single(cluster.Writes);
        await StoreAsync(client, cluster.Token);
        var replacement = cluster.Writes.Last();
        Assert.True(replacement.RequestId > operationId);
        Assert.NotEqual(original.ClientId, replacement.ClientId);
    }

    [Fact]
    public async Task given_a_metadata_watermark_when_the_session_resets_should_preserve_it_and_reject_old_reader_updates()
    {
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync();
        cluster.BeforeReply = (request, reply) =>
        {
            if (request.Operation == (byte)VsrOperation.DeleteStream)
            {
                BinaryPrimitives.WriteUInt64LittleEndian(reply.AsSpan(VsrHeader.REPLY_OP_OFFSET), 47);
                BinaryPrimitives.WriteUInt64LittleEndian(reply.AsSpan(VsrHeader.REPLY_COMMIT_OFFSET), 47);
            }
        };
        await client.DeleteStreamAsync(Stream, cluster.Token);
        var generation = ((ISessionGenerationProvider)client).SessionGeneration;
        var observe = typeof(TcpMessageStream).GetMethod("ObserveMetadataCommit", BindingFlags.Instance | BindingFlags.NonPublic)!
            .CreateDelegate<Action<ulong, ulong>>(client);
        await client.LogoutUserAsync(cluster.Token);
        await client.LoginUserAsync("other", "secret", cluster.Token);
        observe(999, generation);
        ulong observedFloor = 0;
        cluster.BeforeReply = (request, _) =>
        {
            if (request.Code == CommandCodes.GET_CLUSTER_METADATA_CODE)
            {
                observedFloor = request.Context.MetadataOp;
            }
        };
        await client.GetClusterMetadataAsync(cluster.Token);
        Assert.Equal(47UL, observedFloor);
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public async Task given_a_progress_poll_when_its_outcome_is_uncertain_should_not_replay_or_retain_it(
        bool autoCommit, bool next)
    {
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync();
        cluster.OnPoll = request => Reply(request.Operation, [], VsrError.TRANSIENT_NOT_COMMITTED);
        var error = await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() => client.PollMessagesAsync(
            Stream, Topic, 0, Consumer.New(3), next ? PollingStrategy.Next() : PollingStrategy.Offset(0), 1,
            autoCommit, cluster.Token));
        Assert.Null(error.OperationId);
        Assert.Single(cluster.Polls);
        await StoreAsync(client, cluster.Token);
        Assert.Single(cluster.Writes);
    }

    [Fact]
    public async Task given_a_retained_group_when_polling_should_block_progress_but_allow_a_pure_read()
    {
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync();
        var operationId = await RetainWriteAsync(cluster, client);
        var blocked = await Assert.ThrowsAsync<VsrLaneBlockedException>(() => client.PollMessagesAsync(
            Stream, Topic, 0, Consumer.New(3), PollingStrategy.Next(), 1, false, cluster.Token));
        Assert.Equal(operationId, blocked.OperationId);
        await client.PollMessagesAsync(Stream, Topic, 0, Consumer.New(3), PollingStrategy.Offset(0), 1, false,
            cluster.Token);
        Assert.Single(cluster.Polls);
        await client.AbandonOperationAsync(operationId);
    }

    [Fact]
    public async Task given_an_uncertain_write_when_a_consumer_polls_its_lane_should_leave_the_resolution_to_its_owner()
    {
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync();
        var operationId = await RetainWriteAsync(cluster, client);
        var original = Assert.Single(cluster.Writes);
        await using var consumer = PartitionConsumer(client);
        await consumer.InitAsync(cluster.Token);
        await using var messages = consumer.ReceiveAsync(cluster.Token).GetAsyncEnumerator(cluster.Token);

        var error = await Record.ExceptionAsync(() => messages.MoveNextAsync().AsTask());

        Assert.Single(cluster.Writes);
        Assert.Equal(operationId, Assert.IsType<VsrLaneBlockedException>(error).OperationId);
        await client.ResolveOperationAsync(operationId, TimeSpan.FromSeconds(5), cluster.Token);
        Assert.Equal(original.RequestId, cluster.Writes.Last().RequestId);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(VsrError.HISTORY_UNAVAILABLE)]
    public async Task given_an_uncertain_write_when_a_consumer_store_resolves_it_first_should_keep_the_outcome_for_its_owner(
        int status)
    {
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync();
        var operationId = await RetainWriteAsync(cluster, client);
        cluster.OnWrite = request => Result(request, request.RequestId == operationId ? status : 0);
        await using var consumer = PartitionConsumer(client);
        await consumer.InitAsync(cluster.Token);
        await consumer.StoreOffsetAsync(20, 0, ct: cluster.Token);

        var resolved = await Record.ExceptionAsync(() =>
            client.ResolveOperationAsync(operationId, TimeSpan.FromSeconds(5), cluster.Token));

        if (status == 0)
        {
            Assert.Null(resolved);
        }
        else
        {
            var rejection = Assert.IsType<IggyInvalidStatusCodeException>(resolved);
            Assert.Equal(status, rejection.StatusCode);
            Assert.True(rejection.Committed);
        }
        Assert.Equal(2, cluster.Writes.Count(write => write.RequestId == operationId));
        Assert.Equal(3, cluster.Writes.Count);
    }

    [Fact]
    public async Task given_a_running_replay_when_the_owner_resolves_concurrently_should_return_the_proved_outcome()
    {
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync();
        var operationId = await RetainWriteAsync(cluster, client);
        Task? owner = null;
        cluster.OnWrite = request =>
        {
            // The replay holds the operation, so this resolve waits for its outcome.
            owner ??= client.ResolveOperationAsync(operationId, TimeSpan.FromSeconds(5), cluster.Token);
            return Result(request);
        };

        await client.ResolveOperationAsync(operationId, TimeSpan.FromSeconds(5), cluster.Token);

        Assert.NotNull(owner);
        await owner;
        Assert.Equal(2, cluster.Writes.Count);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task given_a_running_replay_when_abandoned_should_free_its_slot_and_keep_its_lane_until_the_replay_ends(bool proved)
    {
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync(maxOperations: 1);
        var operationId = await RetainWriteAsync(cluster, client);
        var replaying = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var answer = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        cluster.OnWrite = request =>
        {
            if (request.RequestId != operationId)
            {
                return Result(request);
            }
            replaying.TrySetResult();
            answer.Task.Wait(cluster.Token);
            return Result(request, marker: proved ? VsrHeader.COMMITTED_RESULT_MARKER : (byte)0);
        };
        var resolving = Record.ExceptionAsync(() =>
            client.ResolveOperationAsync(operationId, TimeSpan.FromSeconds(5), cluster.Token));
        try
        {
            await replaying.Task.WaitAsync(cluster.Token);
            await client.AbandonOperationAsync(operationId);
            await client.DeleteStreamAsync(Stream, cluster.Token).WaitAsync(TimeSpan.FromSeconds(2), cluster.Token);
            using var shortDeadline = CancellationTokenSource.CreateLinkedTokenSource(cluster.Token);
            shortDeadline.CancelAfter(TimeSpan.FromMilliseconds(100));

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => StoreAsync(client, shortDeadline.Token));
        }
        finally
        {
            answer.TrySetResult();
        }

        var outcome = await resolving;
        if (proved)
        {
            Assert.Null(outcome);
            await client.ResolveOperationAsync(operationId, TimeSpan.FromSeconds(1), cluster.Token);
        }
        else
        {
            Assert.Equal(operationId, Assert.IsType<VsrRequestOutcomeUnknownException>(outcome).OperationId);
            await Assert.ThrowsAsync<VsrOperationNotRetainedException>(() =>
                client.ResolveOperationAsync(operationId, TimeSpan.FromSeconds(1), cluster.Token));
        }
        await StoreAsync(client, cluster.Token, partition: 1);
        await StoreAsync(client, cluster.Token);
        var writes = cluster.Writes.ToArray();
        Assert.Equal(4, writes.Length);
        Assert.Equal(operationId, writes[1].RequestId);
        Assert.True(writes[3].RequestId > operationId);
    }

    [Fact]
    public async Task given_more_proved_replays_than_the_operation_limit_when_resolving_after_a_reset_should_keep_only_the_newest()
    {
        const int Limit = 2;
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync(maxOperations: Limit);
        var proved = new List<ulong>();
        foreach (var status in new[] { 0, VsrError.HISTORY_UNAVAILABLE, 0 })
        {
            var operationId = await RetainWriteAsync(cluster, client);
            cluster.OnWrite = request => Result(request, status);
            await Record.ExceptionAsync(() =>
                client.ResolveOperationAsync(operationId, TimeSpan.FromSeconds(5), cluster.Token));
            proved.Add(operationId);
        }
        var writes = cluster.Writes.Count;

        await client.LogoutUserAsync(cluster.Token);
        await client.LoginUserAsync("user", "secret", cluster.Token);

        var rejection = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() =>
            client.ResolveOperationAsync(proved[1], TimeSpan.FromSeconds(1), cluster.Token));
        Assert.Equal(VsrError.HISTORY_UNAVAILABLE, rejection.StatusCode);
        Assert.True(rejection.Committed);
        await client.ResolveOperationAsync(proved[2], TimeSpan.FromSeconds(1), cluster.Token);
        await Assert.ThrowsAsync<VsrOperationNotRetainedException>(() =>
            client.ResolveOperationAsync(proved[0], TimeSpan.FromSeconds(1), cluster.Token));
        Assert.Equal(writes, cluster.Writes.Count);
    }

    [Fact]
    public async Task given_a_healthy_progress_poll_when_heartbeats_time_out_should_keep_its_connection_and_reply()
    {
        const int HeartbeatsWhileHeld = 3;
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync(heartbeat: TimeSpan.FromMilliseconds(25));
        await cluster.UntilAsync(() => cluster.Primary.Pings > 0);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        cluster.OnPoll = request =>
        {
            started.TrySetResult();
            release.Task.GetAwaiter().GetResult();
            return Reply(request.Operation, new byte[40]);
        };
        var generation = ((ISessionGenerationProvider)client).SessionGeneration;
        var poll = client.PollMessagesAsync(Stream, Topic, 0, Consumer.New(3), PollingStrategy.Next(), 1,
            true, cluster.Token);
        try
        {
            await started.Task.WaitAsync(cluster.Token);
            var coordinatorPings = cluster.Coordinator.Pings;
            await cluster.UntilAsync(() => cluster.Coordinator.Pings >= coordinatorPings + HeartbeatsWhileHeld);
            Assert.False(poll.IsCompleted, "A delayed heartbeat must not fail the in-flight progress poll.");
            release.TrySetResult();
            await poll;

            Assert.Single(cluster.Polls);
            Assert.Equal(1, cluster.Primary.Connections);
            Assert.Equal(1, cluster.Primary.Requests(CommandCodes.BIND_SESSION_CODE));
            Assert.Equal(generation, ((ISessionGenerationProvider)client).SessionGeneration);
            await StoreAsync(client, cluster.Token);
            Assert.Single(cluster.Writes);
            Assert.Equal(1, cluster.Primary.Connections);
        }
        finally
        {
            release.TrySetResult();
        }
    }

    [Fact]
    public async Task given_a_lost_metadata_link_when_another_replica_is_ready_should_keep_heartbeats_and_refresh_the_leader()
    {
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync(heartbeat: TimeSpan.FromMilliseconds(25));
        await cluster.UntilAsync(() => cluster.Primary.Pings > 0);
        Assert.Equal(1, cluster.Primary.Requests(CommandCodes.BIND_SESSION_CODE));
        Assert.Equal(0, cluster.Primary.Registrations);
        var events = new ConcurrentQueue<ConnectionState>();
        client.SubscribeConnectionEvents(change =>
        {
            events.Enqueue(change.CurrentState);
            return Task.CompletedTask;
        });
        var generation = ((ISessionGenerationProvider)client).SessionGeneration;
        var pings = cluster.Primary.Pings;
        cluster.PrimaryLeads = true;
        cluster.Coordinator.Kill();
        await cluster.UntilAsync(() => cluster.Primary.Pings > pings);
        await client.DeleteStreamAsync(Stream, cluster.Token);
        Assert.Equal($"127.0.0.1:{cluster.Primary.Port}", client.GetCurrentAddress());
        Assert.Equal(generation, ((ISessionGenerationProvider)client).SessionGeneration);
        Assert.DoesNotContain(ConnectionState.Disconnected, events);
        Assert.Equal(0, cluster.Primary.Registrations);
    }

    [Fact]
    public async Task given_a_logged_out_client_when_idle_should_stop_heartbeats_and_all_replica_links()
    {
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync(heartbeat: TimeSpan.FromMilliseconds(20));
        await cluster.UntilAsync(() => cluster.Primary.Pings > 0 && cluster.Coordinator.Pings > 0);
        await client.LogoutUserAsync(cluster.Token);
        var primaryPings = cluster.Primary.Pings;
        var coordinatorPings = cluster.Coordinator.Pings;
        await Task.Delay(100, cluster.Token);
        Assert.Equal(primaryPings, cluster.Primary.Pings);
        Assert.Equal(coordinatorPings, cluster.Coordinator.Pings);
        var primaryConnections = cluster.Primary.Connections;
        var coordinatorConnections = cluster.Coordinator.Connections;
        await Task.Delay(100, cluster.Token);
        Assert.Equal(primaryConnections, cluster.Primary.Connections);
        Assert.Equal(coordinatorConnections, cluster.Coordinator.Connections);
        await client.LoginUserAsync("user", "secret", cluster.Token);
        await cluster.UntilAsync(() => cluster.Primary.Pings > primaryPings && cluster.Coordinator.Pings > coordinatorPings);
        Assert.Equal(2, cluster.Coordinator.Registrations);
        Assert.Equal(0, cluster.Primary.Registrations);
    }

    [Fact]
    public async Task given_a_session_eviction_when_idle_should_run_one_recovery_and_preserve_the_request_counter()
    {
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync(heartbeat: TimeSpan.FromMilliseconds(25));
        await StoreAsync(client, cluster.Token);
        var original = Assert.Single(cluster.Writes);
        var generation = ((ISessionGenerationProvider)client).SessionGeneration;
        cluster.OnBind = request => request.ClientId == original.ClientId
            ? Reply(request.Operation, [], VsrError.UNAUTHENTICATED) : Answer(request);
        cluster.PingEviction = EvictionReason.NoSession;
        await UntilSessionReplacedAsync(cluster, client);
        await StoreAsync(client, cluster.Token);
        Assert.NotEqual(generation, ((ISessionGenerationProvider)client).SessionGeneration);
        Assert.Equal(2, cluster.Coordinator.Registrations);
        Assert.Equal(0, cluster.Primary.Registrations);
        Assert.True(cluster.Writes.Last().RequestId > original.RequestId);
        Assert.NotEqual(cluster.Writes.Last().ClientId, original.ClientId);
    }

    [Theory]
    [InlineData((byte)EvictionReason.StaleClient)]
    [InlineData((byte)EvictionReason.NoSession)]
    public async Task given_a_detached_session_when_idle_should_bind_the_same_session_on_a_new_link(byte reason)
    {
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync(heartbeat: TimeSpan.FromMilliseconds(25));
        await StoreAsync(client, cluster.Token);
        var original = Assert.Single(cluster.Writes);
        var generation = ((ISessionGenerationProvider)client).SessionGeneration;

        cluster.PingEviction = (EvictionReason)reason;
        await cluster.UntilAsync(() => Binds(cluster) > 1);
        await StoreAsync(client, cluster.Token);

        Assert.Equal(1, cluster.Coordinator.Registrations);
        Assert.Equal(2, Binds(cluster));
        Assert.Equal(generation, ((ISessionGenerationProvider)client).SessionGeneration);
        Assert.Equal(original.ClientId, cluster.Writes.Last().ClientId);
        Assert.True(cluster.Writes.Last().RequestId > original.RequestId);
    }

    [Theory]
    [InlineData(true, (byte)EvictionReason.StaleClient)]
    [InlineData(false, (byte)EvictionReason.StaleClient)]
    [InlineData(true, (byte)EvictionReason.NoSession)]
    [InlineData(false, (byte)EvictionReason.NoSession)]
    public async Task given_a_detached_mutation_should_replay_it_over_the_same_session(bool send, byte reason)
    {
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync();
        var generation = ((ISessionGenerationProvider)client).SessionGeneration;
        await cluster.UntilAsync(() => Binds(cluster) == 1);
        var writes = AnswerWrites(cluster, send, _ => Eviction((EvictionReason)reason));

        await WriteAsync(client, send, cluster.Token);

        var attempts = writes.ToArray();
        Assert.Equal(2, attempts.Length);
        Assert.Equal(attempts[0].RequestId, attempts[1].RequestId);
        Assert.Equal(attempts[0].ClientId, attempts[1].ClientId);
        Assert.Equal(2, Binds(cluster));
        Assert.Equal(1, cluster.Coordinator.Registrations);
        Assert.Equal(generation, ((ISessionGenerationProvider)client).SessionGeneration);
    }

    [Theory]
    [InlineData((byte)EvictionReason.NoSession, true)]
    [InlineData((byte)EvictionReason.NoSession, false)]
    [InlineData((byte)EvictionReason.SessionError, true)]
    [InlineData((byte)EvictionReason.SessionError, false)]
    [InlineData((byte)EvictionReason.UserInactive, true)]
    [InlineData((byte)EvictionReason.UserInactive, false)]
    [InlineData((byte)EvictionReason.SessionTooLow, true)]
    [InlineData((byte)EvictionReason.SessionTooLow, false)]
    [InlineData((byte)EvictionReason.SessionReleaseMismatch, true)]
    [InlineData((byte)EvictionReason.SessionReleaseMismatch, false)]
    public async Task given_a_session_eviction_of_a_written_mutation_should_end_uncertain_until_the_session_resets(
        byte reason, bool send)
    {
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync();
        var writes = AnswerWrites(cluster, send, _ => Eviction((EvictionReason)reason));
        cluster.OnBind = request => writes.TryPeek(out var first) && request.ClientId == first.ClientId
            ? Reply(request.Operation, [], VsrError.UNAUTHENTICATED) : Answer(request);

        var error = await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() =>
            WriteAsync(client, send, cluster.Token));

        var written = Assert.Single(writes);
        Assert.Equal(written.RequestId, error.OperationId);
        await UntilSessionReplacedAsync(cluster, client);
        await Assert.ThrowsAsync<VsrOperationNotRetainedException>(() =>
            client.ResolveOperationAsync(written.RequestId, TimeSpan.FromSeconds(1), cluster.Token));
        await WriteAsync(client, send, cluster.Token);
        Assert.NotEqual(written.ClientId, writes.Last().ClientId);
    }

    [Theory]
    [InlineData("stale client")]
    [InlineData("eviction")]
    [InlineData("failed sign-in")]
    public async Task given_a_lost_reply_when_the_session_then_ends_should_end_uncertain(string end)
    {
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync();
        // A partition read waits until the primary link has bound, so OnBind sees only binds after the lost reply.
        await client.GetOffsetAsync(Consumer.New(3), Stream, Topic, 0, cluster.Token);
        // The session ends the way it ends for a deactivated or deleted user: every later bind and sign-in is refused.
        cluster.OnBind = request => Volatile.Read(ref cluster.RejectRegister)
            ? Reply(request.Operation, [], VsrError.UNAUTHENTICATED) : Answer(request);
        cluster.OnWrite = request =>
        {
            var replay = cluster.Writes.Count > 1;
            if (replay || end == "failed sign-in")
            {
                Volatile.Write(ref cluster.RejectRegister, true);
            }
            if (!replay)
            {
                throw new IOException("The reply is lost.");
            }
            return end == "stale client"
                ? Reply(request.Operation, [], VsrError.STALE_CLIENT) : Eviction(EvictionReason.UserInactive);
        };

        var error = await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() => StoreAsync(client, cluster.Token));

        var written = cluster.Writes.First();
        Assert.Equal(written.RequestId, error.OperationId);
        Assert.Equal(end == "failed sign-in" ? 1 : 2, cluster.Writes.Count);
        Assert.All(cluster.Writes, write => Assert.Equal((written.ClientId, written.RequestId),
            (write.ClientId, write.RequestId)));
        await cluster.UntilAsync(() => cluster.Coordinator.Registrations == 2);
        await Assert.ThrowsAsync<VsrOperationNotRetainedException>(() =>
            client.ResolveOperationAsync(written.RequestId, TimeSpan.FromSeconds(1), cluster.Token));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task given_a_call_waiting_for_an_operation_slot_when_the_session_ends_should_wait_for_the_next_one(
        bool signsIn)
    {
        const int DeadlineMs = 3_000;
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync(maxOperations: 1);
        // Both routes are known, so the second write only waits for the slot that the first one holds.
        await client.GetOffsetAsync(Consumer.New(3), Stream, Topic, 0, cluster.Token);
        await client.GetOffsetAsync(Consumer.New(3), Stream, Topic, 1, cluster.Token);
        var sent = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var evict = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var signIn = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        if (signsIn)
        {
            signIn.SetResult();
        }
        cluster.OnWrite = request =>
        {
            if (cluster.Writes.Count > 1)
            {
                return Result(request);
            }
            sent.SetResult();
            return Eviction(EvictionReason.SessionError);
        };
        cluster.HoldReply = request => request.Operation switch
        {
            (byte)VsrOperation.StoreConsumerOffset => evict.Task,
            (byte)VsrOperation.Register => signIn.Task,
            _ => Task.CompletedTask
        };
        try
        {
            var first = StoreAsync(client, cluster.Token);
            await sent.Task.WaitAsync(cluster.Token);
            // The recovery sign-in that never ends has a 30 s deadline of its own, so only a call deadline far
            // below it ends the call while that sign-in still runs.
            var second = signsIn
                ? StoreAsync(client, cluster.Token, partition: 1)
                : SendWithinAsync(client, CommandCodes.STORE_CONSUMER_OFFSET_CODE,
                    TcpContracts.UpdateOffset(Stream, Topic, Consumer.New(3), 10, 1),
                    TimeSpan.FromMilliseconds(DeadlineMs), cluster.Token);
            await cluster.UntilAsync(() => client.Mutations.BudgetWaiterCount == 1);
            evict.SetResult();

            var error = await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() => first);
            Assert.Equal(cluster.Writes.First().RequestId, error.OperationId);
            if (signsIn)
            {
                await second;
                var writes = cluster.Writes.ToArray();
                Assert.Equal(2, writes.Length);
                Assert.NotEqual(writes[0].ClientId, writes[1].ClientId);
                Assert.Equal(2, cluster.Coordinator.Registrations);
            }
            else
            {
                var refusal = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => second);
                Assert.Equal(VsrError.TRANSIENT_NOT_ACCEPTED, refusal.StatusCode);
                Assert.False(refusal.FromServer);
                Assert.Single(cluster.Writes);
            }
        }
        finally
        {
            evict.TrySetResult();
            signIn.TrySetResult();
        }
    }

    [Fact]
    public async Task given_an_admitted_mutation_when_the_session_recovers_before_it_is_sent_should_send_it_once_under_the_new_session()
    {
        using var cluster = new RecoveryCluster();
        var refuse = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var binds = 0;
        UInt128 replaced = default;
        // The link of the partition primary binds only after the write holds its lane, and the refused bind ends
        // the session.
        cluster.OnBind = request =>
        {
            if (Interlocked.Increment(ref binds) > 1)
            {
                return Answer(request);
            }
            replaced = request.ClientId;
            return Reply(request.Operation, [], VsrError.UNAUTHENTICATED);
        };
        cluster.HoldReply = request =>
            request.Code == CommandCodes.BIND_SESSION_CODE ? refuse.Task : Task.CompletedTask;
        using var client = await cluster.ConnectAsync();
        try
        {
            var store = StoreAsync(client, cluster.Token);
            await cluster.UntilAsync(() => client.Mutations.OperationCount == 1);
            refuse.SetResult();

            await store;
        }
        finally
        {
            refuse.TrySetResult();
        }

        var write = Assert.Single(cluster.Writes);
        Assert.NotEqual(replaced, write.ClientId);
        Assert.Equal(2, cluster.Coordinator.Registrations);
        Assert.Equal(0, client.Mutations.OperationCount);
    }

    [Fact]
    public async Task given_reconnection_off_when_the_session_ends_before_an_admitted_mutation_is_sent_should_end_it_at_once()
    {
        using var cluster = new RecoveryCluster();
        var binding = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        // The link of the partition primary does not bind, so the write holds its lane and waits for that link.
        cluster.OnBind = request =>
        {
            binding.TrySetResult();
            return Answer(request);
        };
        cluster.HoldReply = request =>
            request.Code == CommandCodes.BIND_SESSION_CODE ? release.Task : Task.CompletedTask;
        using var client = await cluster.ConnectAsync(reconnect: false);
        try
        {
            var store = StoreAsync(client, cluster.Token);
            await binding.Task.WaitAsync(cluster.Token);
            await cluster.UntilAsync(() => client.Mutations.OperationCount == 1);
            cluster.PingEviction = EvictionReason.SessionError;

            // No sign-in follows the eviction, so the client stops and the write ends at once as not connected,
            // like any call that nothing reconnects for.
            await Assert.ThrowsAnyAsync<Exception>(() => client.PingAsync(cluster.Token));
            await Assert.ThrowsAsync<NotConnectedException>(() => store);
        }
        finally
        {
            release.TrySetResult();
        }

        Assert.Empty(cluster.Writes);
        Assert.Equal(0, client.Mutations.OperationCount);
    }

    [Fact]
    public async Task given_reconnection_off_when_the_session_ends_while_a_read_waits_for_its_link_should_end_it_at_once()
    {
        using var cluster = new RecoveryCluster();
        var binding = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        // The link of the partition primary does not bind, so the read waits for that link.
        cluster.OnBind = request =>
        {
            binding.TrySetResult();
            return Answer(request);
        };
        cluster.HoldReply = request =>
            request.Code == CommandCodes.BIND_SESSION_CODE ? release.Task : Task.CompletedTask;
        using var client = await cluster.ConnectAsync(reconnect: false);
        try
        {
            var poll = PollAsync(client, cluster.Token);
            await binding.Task.WaitAsync(cluster.Token);
            await cluster.UntilAsync(() => cluster.Coordinator.Requests(CommandCodes.GET_PARTITION_ROUTE_CODE) > 0);
            cluster.PingEviction = EvictionReason.SessionError;

            await Assert.ThrowsAnyAsync<Exception>(() => client.PingAsync(cluster.Token));
            await Assert.ThrowsAsync<NotConnectedException>(() => poll).WaitAsync(cluster.Token);
        }
        finally
        {
            release.TrySetResult();
        }

        Assert.Empty(cluster.Polls);
    }

    [Fact]
    public async Task given_a_read_waiting_for_a_session_recovery_when_the_client_is_disposed_should_end_it_as_disposed()
    {
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync();
        var registering = Signal();
        var release = Signal();
        // The recovery register gets no reply, so the read waits for the recovery.
        cluster.HoldReply = request => request.Operation == (byte)VsrOperation.Register
            ? Hold(registering, release) : Task.CompletedTask;
        cluster.PingEviction = EvictionReason.SessionError;
        await Assert.ThrowsAnyAsync<Exception>(() => client.PingAsync(cluster.Token));
        await registering.Task.WaitAsync(cluster.Token);
        try
        {
            var poll = PollAsync(client, cluster.Token);

            client.Dispose();

            await Assert.ThrowsAsync<ObjectDisposedException>(() => poll).WaitAsync(cluster.Token);
        }
        finally
        {
            release.TrySetResult();
        }

        Assert.Empty(cluster.Polls);
    }

    [Theory]
    [InlineData("invalid credentials")]
    [InlineData("malformed login")]
    [InlineData("eviction after the bind")]
    public async Task given_a_failed_recovery_sign_in_when_a_mutation_waits_should_end_it_and_forget_only_a_refused_credential(
        string failure)
    {
        using var cluster = new RecoveryCluster();
        var malformed = failure == "malformed login";
        var refused = malformed || failure == "invalid credentials";
        var binding = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        // The link of the partition primary does not bind, so the write holds its lane and waits for that link.
        cluster.OnBind = request =>
        {
            binding.TrySetResult();
            return Answer(request);
        };
        cluster.HoldReply = request =>
            request.Code == CommandCodes.BIND_SESSION_CODE ? release.Task : Task.CompletedTask;
        using var client = await cluster.ConnectAsync();
        try
        {
            var store = StoreAsync(client, cluster.Token);
            await binding.Task.WaitAsync(cluster.Token);
            await cluster.UntilAsync(() => client.Mutations.OperationCount == 1);
            if (refused)
            {
                cluster.RegisterRefusal = malformed ? EvictionReason.MalformedLogin : EvictionReason.InvalidCredentials;
                cluster.RejectRegister = true;
            }
            else
            {
                // The server accepts the sign-in and ends the new session at the next read, which refuses no
                // credential.
                cluster.RosterEviction = EvictionReason.SessionError;
            }
            cluster.PingEviction = EvictionReason.SessionError;

            await Assert.ThrowsAnyAsync<Exception>(() => client.PingAsync(cluster.Token));

            if (refused)
            {
                var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => store);
                Assert.Equal(VsrError.UNAUTHENTICATED, error.StatusCode);
                Assert.False(error.FromServer);
                var refusal = Assert.IsType<IggyInvalidStatusCodeException>(error.InnerException);
                Assert.Equal(malformed ? VsrError.INVALID_FORMAT : VsrError.INVALID_CREDENTIALS, refusal.StatusCode);
                Assert.True(refusal.FromServer);
            }
            else
            {
                await Assert.ThrowsAsync<NotConnectedException>(() => store);
            }
        }
        finally
        {
            release.TrySetResult();
        }

        Assert.Empty(cluster.Writes);
        Assert.Equal(2, cluster.Coordinator.Registrations);
        // Only a refused credential is forgotten, so a connect signs in again with any other.
        await client.ConnectAsync(cluster.Token);
        Assert.Equal(refused ? 2 : 3, cluster.Coordinator.Registrations);
    }

    [Fact]
    public async Task given_auto_login_when_the_caller_signed_in_as_another_user_should_recover_as_that_user()
    {
        using var cluster = new RecoveryCluster();
        using var client = new TcpMessageStream(new IggyClientConfigurator
        {
            BaseAddress = $"127.0.0.1:{cluster.Coordinator.Port}",
            Protocol = Protocol.Tcp,
            HeartbeatInterval = TimeSpan.FromMilliseconds(25),
            AutoLoginSettings = AutoLoginSettings.For("user", "secret"),
            ReconnectionSettings = new ReconnectionSettings
            {
                Enabled = true,
                InitialDelay = TimeSpan.FromMilliseconds(1),
                WaitAfterReconnect = TimeSpan.Zero
            }
        }, NullLoggerFactory.Instance);
        await client.ConnectAsync(cluster.Token);
        await client.LoginUserAsync("other", "secret", cluster.Token);
        var registrations = new ConcurrentQueue<MockRequest>();
        cluster.BeforeReply = (request, _) =>
        {
            if (request.Operation == (byte)VsrOperation.Register)
            {
                registrations.Enqueue(request);
            }
        };

        cluster.PingEviction = EvictionReason.SessionError;
        await cluster.UntilAsync(() => !registrations.IsEmpty);
        await StoreAsync(client, cluster.Token);

        var registration = Assert.Single(registrations);
        Assert.Equal(LoginRegister.Serialize("other", "secret", LoginRegister.ReadBindSecret(registration.Body)),
            registration.Body);
        Assert.Equal(registration.ClientId, Assert.Single(cluster.Writes).ClientId);
        // An explicit sign-out leaves the client nothing to sign in as by itself, auto login included.
        await client.LogoutUserAsync(cluster.Token);
        var signedIn = cluster.Coordinator.Registrations;
        await client.ConnectAsync(cluster.Token);
        Assert.Equal(signedIn, cluster.Coordinator.Registrations);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task given_an_unbound_mutation_when_the_bind_succeeds_should_replay_the_same_request(bool send)
    {
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync();
        var writes = AnswerWrites(cluster, send, request => Reply(request.Operation, [], VsrError.UNAUTHENTICATED));

        await WriteAsync(client, send, cluster.Token);

        var attempts = writes.ToArray();
        Assert.Equal(2, attempts.Length);
        Assert.Equal(attempts[0].RequestId, attempts[1].RequestId);
        Assert.Equal(attempts[0].ClientId, attempts[1].ClientId);
        Assert.Equal(attempts[0].Body, attempts[1].Body);
        Assert.Equal(1, cluster.Coordinator.Registrations);
    }

    [Theory]
    [InlineData(true, VsrError.STALE_CLIENT)]
    [InlineData(false, VsrError.STALE_CLIENT)]
    [InlineData(true, VsrError.UNAUTHENTICATED)]
    [InlineData(false, VsrError.UNAUTHENTICATED)]
    public async Task given_a_detached_link_when_the_bind_succeeds_should_preserve_unrelated_retained_operations(
        bool metadata, int status)
    {
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync();
        await cluster.UntilAsync(() => Binds(cluster) == 1);
        var generation = ((ISessionGenerationProvider)client).SessionGeneration;
        await client.PollMessagesAsync(Stream, Topic, null, Consumer.Group(4), PollingStrategy.Next(), 1, false,
            cluster.Token);
        var operationId = await RetainWriteAsync(cluster, client);
        var original = Assert.Single(cluster.Writes);
        var refusals = 0;
        cluster.OnMetadata = cluster.OnWrite = request => Interlocked.Increment(ref refusals) == 1
            ? Reply(request.Operation, [], (uint)status) : Result(request);

        Task MutateAsync() => metadata ? client.DeleteStreamAsync(Stream, cluster.Token)
            : StoreAsync(client, cluster.Token, partition: 1);
        if (status == VsrError.STALE_CLIENT)
        {
            var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(MutateAsync);
            Assert.Equal(status, error.StatusCode);
            Assert.False(error.Committed);
        }
        else
        {
            await MutateAsync();
        }
        cluster.OnMetadata = cluster.OnWrite = request => Result(request);
        await cluster.UntilAsync(() => Binds(cluster) == 2);
        await client.ResolveOperationAsync(operationId, TimeSpan.FromSeconds(5), cluster.Token);
        await client.PingAsync(cluster.Token);
        Assert.Equal(1, cluster.Coordinator.Registrations);
        Assert.Equal(generation, ((ISessionGenerationProvider)client).SessionGeneration);
        Assert.Equal(operationId, cluster.Writes.Last().RequestId);
        Assert.Equal(2, cluster.Syncs.Count);
        Assert.All(cluster.Writes.Concat(cluster.Syncs), request => Assert.Equal(original.ClientId, request.ClientId));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task given_a_stale_client_refusal_when_the_bind_is_refused_should_fail_the_call_and_replace_the_session(
        bool send)
    {
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync();
        // A partition read waits until the primary link has bound, so OnBind sees only binds after the refusal.
        await client.GetOffsetAsync(Consumer.New(3), Stream, Topic, 0, cluster.Token);
        var writes = AnswerWrites(cluster, send, request => Reply(request.Operation, [], VsrError.STALE_CLIENT));
        var endedBinds = new ConcurrentQueue<MockRequest>();
        cluster.OnBind = request =>
        {
            if (!writes.TryPeek(out var first) || first.ClientId != request.ClientId)
            {
                return Answer(request);
            }
            endedBinds.Enqueue(request);
            return Reply(request.Operation, [], VsrError.UNAUTHENTICATED);
        };

        var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() =>
            WriteAsync(client, send, cluster.Token));

        Assert.Equal(VsrError.STALE_CLIENT, error.StatusCode);
        Assert.False(error.Committed);
        var refused = Assert.Single(writes);
        await Assert.ThrowsAsync<VsrOperationNotRetainedException>(() =>
            client.ResolveOperationAsync(refused.RequestId, TimeSpan.FromSeconds(1), cluster.Token));
        await UntilSessionReplacedAsync(cluster, client);
        Assert.Single(endedBinds);
        await WriteAsync(client, send, cluster.Token);
        Assert.NotEqual(refused.ClientId, writes.Last().ClientId);
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(true, true)]
    [InlineData(false, false)]
    [InlineData(false, true)]
    public async Task given_a_committed_stale_client_result_to_a_written_mutation_should_stay_definitive_and_replace_the_session(
        bool metadata, bool raw)
    {
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync();
        var attempts = new ConcurrentQueue<MockRequest>();
        cluster.OnMetadata = cluster.OnWrite = request =>
        {
            attempts.Enqueue(request);
            return Result(request, attempts.Count == 1 ? VsrError.STALE_CLIENT : 0);
        };

        var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(MutateAsync);

        Assert.Equal(VsrError.STALE_CLIENT, error.StatusCode);
        Assert.True(error.Committed);
        var ended = Assert.Single(attempts);
        await UntilSessionReplacedAsync(cluster, client);
        await MutateAsync();
        Assert.NotEqual(ended.ClientId, attempts.Last().ClientId);

        Task MutateAsync() => (metadata, raw) switch
        {
            (true, false) => client.DeleteStreamAsync(Stream, cluster.Token),
            (true, true) => client.SendBinaryRequestAsync(CommandCodes.DELETE_STREAM_CODE,
                TcpMessageStreamHelpers.GetBytesFromIdentifier(Stream), cluster.Token),
            (false, false) => StoreAsync(client, cluster.Token),
            (false, true) => client.SendBinaryRequestAsync(CommandCodes.STORE_CONSUMER_OFFSET_CODE,
                TcpContracts.UpdateOffset(Stream, Topic, Consumer.New(3), 10, 0), cluster.Token)
        };
    }

    [Fact]
    public async Task given_an_uncertain_write_when_its_replay_proves_a_committed_stale_client_result_should_keep_it_and_replace_the_session()
    {
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync();
        var operationId = await RetainWriteAsync(cluster, client);
        var original = Assert.Single(cluster.Writes);
        cluster.OnWrite = request => Result(request, request.RequestId == operationId ? VsrError.STALE_CLIENT : 0);

        var rejection = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() =>
            client.ResolveOperationAsync(operationId, TimeSpan.FromSeconds(5), cluster.Token));

        Assert.Equal(VsrError.STALE_CLIENT, rejection.StatusCode);
        Assert.True(rejection.Committed);
        await UntilSessionReplacedAsync(cluster, client);
        Assert.Same(rejection, await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() =>
            client.ResolveOperationAsync(operationId, TimeSpan.FromSeconds(1), cluster.Token)));
        Assert.Equal(2, cluster.Writes.Count);
        await StoreAsync(client, cluster.Token);
        Assert.NotEqual(original.ClientId, cluster.Writes.Last().ClientId);
    }

    [Fact]
    public async Task given_a_replaced_replica_endpoint_when_roster_refreshes_should_close_the_old_link_and_bind_once()
    {
        using var cluster = new RecoveryCluster();
        using var replacement = new MockNode();
        var replacementWrites = 0;
        replacement.Serve(request =>
        {
            if (request.Operation == (byte)VsrOperation.StoreConsumerOffset)
            {
                Interlocked.Increment(ref replacementWrites);
            }
            return cluster.Handle(request);
        });
        using var client = await cluster.ConnectAsync();
        await StoreAsync(client, cluster.Token);
        var original = Assert.Single(cluster.Writes);
        var generation = ((ISessionGenerationProvider)client).SessionGeneration;
        cluster.PrimaryEndpoint = replacement.Port;
        await client.GetClusterMetadataAsync(cluster.Token);
        await cluster.UntilAsync(() => replacement.Requests(CommandCodes.BIND_SESSION_CODE) == 1
            && cluster.Primary.ActiveConnections == 0);
        await StoreAsync(client, cluster.Token);
        Assert.Equal(1, replacementWrites);
        Assert.Equal(1, replacement.Connections);
        Assert.Equal(0, replacement.Registrations);
        Assert.Equal(original.ClientId, cluster.Writes.Last().ClientId);
        Assert.Equal(generation, ((ISessionGenerationProvider)client).SessionGeneration);
        client.Dispose();
        await cluster.UntilAsync(() => cluster.Coordinator.ActiveConnections == 0 && replacement.ActiveConnections == 0);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_a_proved_lifecycle_busy_reply_when_uncertainty_differs_should_retry_only_the_initial_call(bool uncertain)
    {
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync();
        if (uncertain)
        {
            var id = await RetainWriteAsync(cluster, client);
            cluster.OnWrite = request => Result(request, VsrError.LIFECYCLE_BUSY);
            var rejection = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() =>
                client.ResolveOperationAsync(id, TimeSpan.FromSeconds(2), cluster.Token));
            Assert.Equal(VsrError.LIFECYCLE_BUSY, rejection.StatusCode);
            Assert.True(rejection.Committed);
            Assert.Equal(id, cluster.Writes.Last().RequestId);
            Assert.Same(rejection, await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() =>
                client.ResolveOperationAsync(id, TimeSpan.FromSeconds(2), cluster.Token)));
            cluster.OnWrite = request => Result(request);
            await StoreAsync(client, cluster.Token);
            Assert.True(cluster.Writes.Last().RequestId > id);
        }
        else
        {
            cluster.OnWrite = request => Result(request, cluster.Writes.Count == 1 ? VsrError.LIFECYCLE_BUSY : 0);
            await StoreAsync(client, cluster.Token);
            Assert.Equal(2, cluster.Writes.Count);
            var original = cluster.Writes.First();
            var retried = cluster.Writes.Last();
            Assert.True(retried.RequestId > original.RequestId);
            Assert.Equal(original.ClientId, retried.ClientId);
            Assert.Equal(original.Body, retried.Body);
        }
    }

    [Theory]
    [InlineData(ZeroCommittedOperation, false)]
    [InlineData(ZeroCommittedOperation, true)]
    [InlineData(VsrHeader.REPLY_CLIENT_OFFSET, false)]
    [InlineData(VsrHeader.REPLY_CHECKSUM_OFFSET, false)]
    [InlineData(VsrHeader.REPLY_INCARNATION_OFFSET, false)]
    [InlineData(VsrHeader.REPLY_OP_OFFSET, false)]
    [InlineData(VsrHeader.REPLY_COMMIT_OFFSET, false)]
    [InlineData(VsrHeader.REPLY_CLIENT_OFFSET, true)]
    [InlineData(VsrHeader.REPLY_CHECKSUM_OFFSET, true)]
    [InlineData(VsrHeader.REPLY_INCARNATION_OFFSET, true)]
    [InlineData(VsrHeader.REPLY_OP_OFFSET, true)]
    [InlineData(VsrHeader.REPLY_COMMIT_OFFSET, true)]
    public async Task given_an_uncertain_write_when_a_receipt_field_mismatches_should_keep_the_lane_retained(
        int offset, bool rejection)
    {
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync();
        var id = await RetainWriteAsync(cluster, client);
        cluster.OnWrite = request => Result(request, rejection ? VsrError.UNAUTHENTICATED : 0);
        cluster.BeforeReply = (request, reply) =>
        {
            if (request.Operation == (byte)VsrOperation.StoreConsumerOffset)
            {
                if (offset == ZeroCommittedOperation)
                {
                    BinaryPrimitives.WriteUInt64LittleEndian(reply.AsSpan(VsrHeader.REPLY_OP_OFFSET), 0);
                    BinaryPrimitives.WriteUInt64LittleEndian(reply.AsSpan(VsrHeader.REPLY_COMMIT_OFFSET), 0);
                }
                else
                {
                    reply[offset] ^= 2;
                }
            }
        };
        var error = await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() =>
            client.ResolveOperationAsync(id, TimeSpan.FromSeconds(2), cluster.Token));
        Assert.Equal(id, error.OperationId);
        var invalid = Assert.IsType<IggyInvalidStatusCodeException>(error.InnerException);
        Assert.False(invalid.FromServer);
        Assert.False(invalid.Committed);
        var blocked = await Assert.ThrowsAsync<VsrLaneBlockedException>(() => StoreAsync(client, cluster.Token));
        Assert.Equal(id, blocked.OperationId);
        Assert.Equal(1, cluster.Coordinator.Registrations);
        cluster.BeforeReply = null;
        cluster.OnWrite = request => Result(request);
        await client.ResolveOperationAsync(id, TimeSpan.FromSeconds(2), cluster.Token);
        await StoreAsync(client, cluster.Token);
    }

    [Fact]
    public async Task given_invalid_register_credentials_when_evicted_should_surface_the_rejection_and_allow_a_new_login()
    {
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync();
        cluster.RejectRegister = true;
        var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() =>
            client.LoginUserAsync("user", "invalid", cluster.Token));
        Assert.Equal(VsrError.INVALID_CREDENTIALS, error.StatusCode);
        cluster.RejectRegister = false;
        await client.LoginUserAsync("user", "secret", cluster.Token);
        await StoreAsync(client, cluster.Token);
        Assert.Single(cluster.Writes);
    }

    [Theory]
    [InlineData(true, false, 0, "user", "rotated")]
    [InlineData(true, true, 0, "user", "rotated")]
    [InlineData(false, true, 0, "renamed", "secret")]
    [InlineData(true, true, VsrError.INVALID_CREDENTIALS, "user", "secret")]
    public async Task given_a_credential_change_when_the_session_recovers_should_sign_in_with_the_committed_credentials(
        bool changePassword, bool resolve, int status, string username, string password)
    {
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync(heartbeat: TimeSpan.FromMilliseconds(25));
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(cluster.Token);
        if (resolve)
        {
            cluster.OnMetadata = request =>
            {
                cancellation.Cancel();
                return Reply(request.Operation, [], VsrError.TRANSIENT_NOT_COMMITTED);
            };
        }
        var change = changePassword
            ? client.ChangePasswordAsync(Identifier.String("user"), "secret", "rotated", cancellation.Token)
            : client.UpdateUserAsync(Identifier.String("user"), "renamed", token: cancellation.Token);
        if (resolve)
        {
            var uncertain = await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() => change);
            cluster.OnMetadata = request => Result(request, status);
            change = client.ResolveOperationAsync(Assert.IsType<ulong>(uncertain.OperationId), TimeSpan.FromSeconds(5),
                cluster.Token);
        }
        if (status == 0)
        {
            await change;
        }
        else
        {
            Assert.True((await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => change)).Committed);
        }
        var registrations = new ConcurrentQueue<MockRequest>();
        cluster.BeforeReply = (request, _) =>
        {
            if (request.Operation == (byte)VsrOperation.Register)
            {
                registrations.Enqueue(request);
            }
        };

        cluster.PingEviction = EvictionReason.SessionError;
        await cluster.UntilAsync(() => !registrations.IsEmpty);

        var registration = Assert.Single(registrations);
        Assert.Equal(LoginRegister.Serialize(username, password, LoginRegister.ReadBindSecret(registration.Body)),
            registration.Body);
    }

    [Fact]
    public async Task given_mutated_raw_credentials_when_the_session_recovers_should_use_the_submitted_password()
    {
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync(heartbeat: TimeSpan.FromMilliseconds(25));
        var payload = TcpContracts.ChangePassword(Identifier.String("user"), "secret", "rotated");
        var replacement = TcpContracts.ChangePassword(Identifier.String("user"), "secret", "changed");
        var submitted = payload.ToArray();
        cluster.OnMetadata = request =>
        {
            Assert.Equal(submitted, request.Body);
            replacement.CopyTo(payload, 0);
            return Result(request);
        };

        _ = await client.SendBinaryRequestAsync(CommandCodes.CHANGE_PASSWORD_CODE, payload, cluster.Token);
        var registrations = new ConcurrentQueue<MockRequest>();
        cluster.BeforeReply = (request, _) =>
        {
            if (request.Operation == (byte)VsrOperation.Register)
            {
                registrations.Enqueue(request);
            }
        };
        cluster.PingEviction = EvictionReason.SessionError;
        await cluster.UntilAsync(() => !registrations.IsEmpty);

        var registration = Assert.Single(registrations);
        Assert.Equal(LoginRegister.Serialize("user", "rotated", LoginRegister.ReadBindSecret(registration.Body)), registration.Body);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task given_a_selected_partition_when_not_found_should_reselect_once_only_before_uncertainty(
        bool messageKey, bool uncertain)
    {
        // Only a miss from the metadata primary is final, so the roster names the partition primary the leader.
        using var cluster = new RecoveryCluster { PrimaryLeads = true };
        cluster.PartitionCount = 1;
        cluster.OnSend = request =>
        {
            if (uncertain && cluster.Sends.Count == 1)
            {
                return Reply(request.Operation, [], VsrError.TRANSIENT_NOT_COMMITTED);
            }
            cluster.PartitionCount = 2;
            return cluster.Sends.Count == (uncertain ? 2 : 1)
                ? Reply(request.Operation, [], VsrError.PARTITION_NOT_FOUND) : Reply(request.Operation, new byte[4]);
        };
        using var client = await cluster.ConnectAsync();
        var partitioning = messageKey ? Partitioning.EntityIdString("key") : Partitioning.None();
        var message = new Message(Guid.NewGuid(), "payload"u8.ToArray());
        var send = client.SendMessagesAsync(Stream, Topic, partitioning, message, cluster.Token);
        if (uncertain)
        {
            var error = await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() => send);
            var attempts = cluster.Sends.ToArray();
            Assert.Equal(2, attempts.Length);
            Assert.Equal(attempts[0].RequestId, attempts[1].RequestId);
            Assert.Equal(attempts[0].Body, attempts[1].Body);
            Assert.Equal(2, cluster.RouteLookups);
            await client.AbandonOperationAsync(Assert.IsType<ulong>(error.OperationId));
        }
        else
        {
            await send;
            var attempts = cluster.Sends.ToArray();
            Assert.Equal(2, attempts.Length);
            Assert.True(attempts[1].RequestId > attempts[0].RequestId);
            Assert.True(cluster.RouteLookups >= 2);
        }
    }

    [Fact]
    public async Task given_an_explicit_partition_when_not_found_should_return_the_original_error_without_reselection()
    {
        // Only a miss from the metadata primary is final, so the roster names the partition primary the leader.
        using var cluster = new RecoveryCluster { PrimaryLeads = true };
        cluster.OnSend = request => Reply(request.Operation, [], VsrError.PARTITION_NOT_FOUND);
        using var client = await cluster.ConnectAsync();
        var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => client.SendMessagesAsync(
            Stream, Topic, Partitioning.PartitionId(0), new Message(Guid.NewGuid(), "payload"u8.ToArray()), cluster.Token));
        Assert.Equal(VsrError.PARTITION_NOT_FOUND, error.StatusCode);
        Assert.Single(cluster.Sends);
        Assert.Equal(1, cluster.RouteLookups);
    }

    [Theory]
    [InlineData(VsrError.STREAM_ID_NOT_FOUND)]
    [InlineData(VsrError.STREAM_NAME_NOT_FOUND)]
    [InlineData(VsrError.TOPIC_ID_NOT_FOUND)]
    [InlineData(VsrError.TOPIC_NAME_NOT_FOUND)]
    [InlineData(VsrError.PARTITION_NOT_FOUND)]
    public async Task given_a_backup_namespace_miss_when_sending_should_resend_after_a_lookup_at_the_metadata_primary(
        int status)
    {
        using var cluster = new RecoveryCluster();
        cluster.OnSend = request => cluster.Sends.Count == 1
            ? Reply(request.Operation, [], (uint)status) : Reply(request.Operation, new byte[4]);
        using var client = await cluster.ConnectAsync();

        await client.SendMessagesAsync(Stream, Topic, Partitioning.PartitionId(0),
            new Message(Guid.NewGuid(), "payload"u8.ToArray()), cluster.Token);

        var attempts = cluster.Sends.ToArray();
        Assert.Equal(2, attempts.Length);
        Assert.Equal(attempts[0].RequestId, attempts[1].RequestId);
        Assert.Equal(2, cluster.Coordinator.Requests(CommandCodes.GET_PARTITION_ROUTE_CODE));
        Assert.Equal(0, cluster.Primary.Requests(CommandCodes.GET_PARTITION_ROUTE_CODE));
    }

    [Fact]
    public async Task given_a_backup_namespace_miss_when_storing_an_offset_should_resend_after_a_lookup_at_the_metadata_primary()
    {
        using var cluster = new RecoveryCluster();
        cluster.OnWrite = request => cluster.Writes.Count == 1
            ? Reply(request.Operation, [], VsrError.TOPIC_ID_NOT_FOUND) : Result(request);
        using var client = await cluster.ConnectAsync();

        await StoreAsync(client, cluster.Token);

        var attempts = cluster.Writes.ToArray();
        Assert.Equal(2, attempts.Length);
        Assert.Equal(attempts[0].RequestId, attempts[1].RequestId);
        Assert.Equal(2, cluster.Coordinator.Requests(CommandCodes.GET_PARTITION_ROUTE_CODE));
        Assert.Equal(0, cluster.Primary.Requests(CommandCodes.GET_PARTITION_ROUTE_CODE));
    }

    [Fact]
    public async Task given_an_uncertain_send_when_a_backup_misses_the_namespace_should_resend_until_it_settles()
    {
        using var cluster = new RecoveryCluster();
        cluster.OnSend = request => cluster.Sends.Count switch
        {
            1 => Reply(request.Operation, [], VsrError.TRANSIENT_NOT_COMMITTED),
            2 => Reply(request.Operation, [], VsrError.STREAM_ID_NOT_FOUND),
            _ => Reply(request.Operation, new byte[4])
        };
        using var client = await cluster.ConnectAsync();

        await client.SendMessagesAsync(Stream, Topic, Partitioning.PartitionId(0),
            new Message(Guid.NewGuid(), "payload"u8.ToArray()), cluster.Token);

        var attempts = cluster.Sends.ToArray();
        Assert.Equal(3, attempts.Length);
        Assert.All(attempts, attempt => Assert.Equal(attempts[0].RequestId, attempt.RequestId));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task given_a_partition_write_answered_not_committed_when_its_replay_commits_should_succeed_with_the_same_request(
        bool send)
    {
        using var cluster = new RecoveryCluster();
        var attempts = AnswerPartitionWrites(cluster, send, (request, count) => count == 1
            ? Reply(request.Operation, [], VsrError.TRANSIENT_NOT_COMMITTED) : Result(request));
        using var client = await cluster.ConnectAsync();

        await PartitionWriteAsync(client, send, cluster.Token);

        var sent = attempts.ToArray();
        Assert.Equal(2, sent.Length);
        Assert.Equal(sent[0].ClientId, sent[1].ClientId);
        Assert.Equal(sent[0].RequestId, sent[1].RequestId);
        Assert.Equal(sent[0].Body, sent[1].Body);
        Assert.Equal(0, client.Mutations.OperationCount);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task given_a_partition_write_answered_not_committed_until_the_deadline_should_end_uncertain(bool send)
    {
        const int DeadlineMs = 2_000;
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync();
        // A public write gives the body that the call with the earlier deadline sends.
        await PartitionWriteAsync(client, send, cluster.Token);
        var attempts = AnswerPartitionWrites(cluster, send,
            (request, _) => Reply(request.Operation, [], VsrError.TRANSIENT_NOT_COMMITTED));
        var body = Assert.Single(attempts).Body;

        var error = await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() => SendWithinAsync(client,
            send ? CommandCodes.SEND_MESSAGES_CODE : CommandCodes.STORE_CONSUMER_OFFSET_CODE, body,
            TimeSpan.FromMilliseconds(DeadlineMs), cluster.Token));

        var operationId = Assert.IsType<ulong>(error.OperationId);
        var replays = attempts.Skip(1).ToArray();
        Assert.True(replays.Length > 1);
        Assert.All(replays, replay => Assert.Equal(operationId, replay.RequestId));
        Assert.True(client.RetainsOperation(operationId));
    }

    [Fact]
    public async Task given_a_backup_namespace_miss_when_polling_should_poll_again_after_a_lookup_at_the_metadata_primary()
    {
        using var cluster = new RecoveryCluster();
        var poll = cluster.OnPoll;
        cluster.OnPoll = request => cluster.Polls.Count == 1
            ? Reply(request.Operation, [], VsrError.TOPIC_ID_NOT_FOUND) : poll(request);
        using var client = await cluster.ConnectAsync();

        await PollAsync(client, cluster.Token);

        Assert.Equal(2, cluster.Polls.Count);
        Assert.Equal(2, cluster.Coordinator.Requests(CommandCodes.GET_PARTITION_ROUTE_CODE));
        Assert.Equal(0, cluster.Primary.Requests(CommandCodes.GET_PARTITION_ROUTE_CODE));
    }

    [Fact]
    public async Task given_a_backup_namespace_miss_until_the_deadline_when_polling_should_report_not_accepted()
    {
        const int DeadlineMs = 1_000;
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync();
        // A public poll gives the body that the call with the earlier deadline sends.
        await PollAsync(client, cluster.Token);
        var poll = Assert.Single(cluster.Polls).Body;
        cluster.OnPoll = request => Reply(request.Operation, [], VsrError.TOPIC_ID_NOT_FOUND);

        var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => SendWithinAsync(client,
            CommandCodes.POLL_MESSAGES_CODE, poll, TimeSpan.FromMilliseconds(DeadlineMs), cluster.Token));

        Assert.Equal(VsrError.TRANSIENT_NOT_ACCEPTED, error.StatusCode);
        Assert.True(cluster.Polls.Count > 2);
        Assert.Equal(0, cluster.Primary.Requests(CommandCodes.GET_PARTITION_ROUTE_CODE));
    }

    [Fact]
    public async Task given_not_primary_until_the_deadline_when_polling_should_report_not_accepted()
    {
        const int DeadlineMs = 1_000;
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync();
        // A public poll gives the body that the call with the earlier deadline sends.
        await PollAsync(client, cluster.Token);
        var poll = Assert.Single(cluster.Polls).Body;
        cluster.OnPoll = request => Reply(request.Operation, [], VsrError.NOT_PRIMARY);

        var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => SendWithinAsync(client,
            CommandCodes.POLL_MESSAGES_CODE, poll, TimeSpan.FromMilliseconds(DeadlineMs), cluster.Token));

        Assert.Equal(VsrError.TRANSIENT_NOT_ACCEPTED, error.StatusCode);
        Assert.True(error.FromServer);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task given_a_believed_metadata_primary_without_a_link_when_the_elected_one_misses_the_namespace_should_fail_with_that_miss(
        bool cached)
    {
        using var cluster = new RecoveryCluster();
        using var client = await cluster.ConnectAsync();
        // The send binds the link of the partition primary and caches the route of its partition only.
        await client.SendMessagesAsync(Stream, Topic, Partitioning.PartitionId(cached ? 0u : 1u),
            new Message(Guid.NewGuid(), "payload"u8.ToArray()), cluster.Token);
        cluster.Coordinator.Kill();
        cluster.PrimaryLeads = true;
        cluster.OnRoute = request => Reply(request.Operation, [], VsrError.TOPIC_ID_NOT_FOUND);
        cluster.OnSend = request => Reply(request.Operation, [], VsrError.TOPIC_ID_NOT_FOUND);

        var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => client.SendMessagesAsync(Stream,
            Topic, Partitioning.PartitionId(0), new Message(Guid.NewGuid(), "payload"u8.ToArray()), cluster.Token));

        Assert.Equal(VsrError.TOPIC_ID_NOT_FOUND, error.StatusCode);
        Assert.Equal(cached ? 2 : 1, cluster.Sends.Count);
    }

    [Theory]
    [InlineData(CommandCodes.GET_PARTITION_ROUTE_CODE)]
    [InlineData(CommandCodes.SEND_MESSAGES_CODE)]
    public async Task given_a_namespace_miss_from_a_deposed_metadata_primary_when_its_roster_names_another_should_ask_another_replica(
        int code)
    {
        using var cluster = new RecoveryCluster { PrimaryLeads = true };
        using var client = await cluster.ConnectAsync();
        // The send binds the link of the partition primary, which the client believes is the metadata primary.
        await client.SendMessagesAsync(Stream, Topic, Partitioning.PartitionId(0),
            new Message(Guid.NewGuid(), "payload"u8.ToArray()), cluster.Token);
        cluster.PrimaryLeads = false;
        var missed = 0;
        byte[]? MissOnce(MockRequest request) => Interlocked.Exchange(ref missed, 1) == 0
            ? Reply(request.Operation, [], VsrError.TOPIC_ID_NOT_FOUND) : null;
        if (code == CommandCodes.GET_PARTITION_ROUTE_CODE)
        {
            cluster.OnRoute = MissOnce;
        }
        else
        {
            var send = cluster.OnSend;
            cluster.OnSend = request => MissOnce(request) ?? send(request);
        }
        var deposedLookups = cluster.Primary.Requests(CommandCodes.GET_PARTITION_ROUTE_CODE);
        var electedLookups = cluster.Coordinator.Requests(CommandCodes.GET_PARTITION_ROUTE_CODE);

        await client.SendMessagesAsync(Stream, Topic, Partitioning.PartitionId(1),
            new Message(Guid.NewGuid(), "payload"u8.ToArray()), cluster.Token);

        Assert.Equal(1, Volatile.Read(ref missed));
        Assert.Equal(deposedLookups + 1, cluster.Primary.Requests(CommandCodes.GET_PARTITION_ROUTE_CODE));
        Assert.Equal(electedLookups + 1, cluster.Coordinator.Requests(CommandCodes.GET_PARTITION_ROUTE_CODE));
    }

    private static Task StoreAsync(TcpMessageStream client, CancellationToken token, uint partition = 0) =>
        client.StoreOffsetAsync(Consumer.New(3), Stream, Topic, 10, partition, token);

    private static Task PollAsync(TcpMessageStream client, CancellationToken token) =>
        client.PollMessagesAsync(Stream, Topic, 0, Consumer.New(3), PollingStrategy.Offset(0), 10, false, token);

    // The public calls run until the fixed 30 s call deadline. The entry point under them takes an earlier one.
    private static async Task SendWithinAsync(TcpMessageStream client, int code, byte[] body, TimeSpan budget,
        CancellationToken token)
    {
        using var reply = await client.SendWithResponseAsync(code, body, token,
            deadline: Environment.TickCount64 + (long)budget.TotalMilliseconds);
    }

    private static IggyConsumer PartitionConsumer(TcpMessageStream client) => new(client, new IggyConsumerConfig
    {
        StreamId = Stream,
        TopicId = Topic,
        PartitionId = 0,
        Consumer = Consumer.New(3),
        PollingStrategy = PollingStrategy.Next(),
        AutoCommitMode = AutoCommitMode.Disabled
    }, NullLoggerFactory.Instance);

    private static async Task<ulong> RetainWriteAsync(RecoveryCluster cluster, TcpMessageStream client)
    {
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(cluster.Token);
        cluster.OnWrite = request =>
        {
            cancellation.Cancel();
            return Reply(request.Operation, [], VsrError.TRANSIENT_NOT_COMMITTED);
        };
        var error = await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() => StoreAsync(client, cancellation.Token));
        cluster.OnWrite = request => Result(request);
        // The reply to the canceled write can still be on its way, and it would answer a replay, which reuses the
        // request id. This read takes the same link, where replies keep their order, so it returns after that reply.
        await client.GetOffsetAsync(Consumer.New(3), Stream, Topic, 0, cluster.Token);
        return Assert.IsType<ulong>(error.OperationId);
    }

    private static byte[] Result(MockRequest request, int status = 0, byte marker = VsrHeader.COMMITTED_RESULT_MARKER)
    {
        var body = new byte[status == 0 ? 4 : 12];
        if (status != 0)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(body, 1);
            BinaryPrimitives.WriteUInt32LittleEndian(body.AsSpan(8), (uint)status);
        }
        var reply = Reply(request.Operation, body);
        reply[VsrHeader.REPLY_COMMITTED_OFFSET] = marker;
        return reply;
    }

    private static byte[] Eviction(EvictionReason reason)
    {
        var eviction = new byte[VsrHeader.HEADER_SIZE];
        BinaryPrimitives.WriteUInt32LittleEndian(eviction.AsSpan(SIZE_OFFSET), VsrHeader.HEADER_SIZE);
        eviction[COMMAND_OFFSET] = COMMAND_EVICTION;
        eviction[EVICTION_REASON_OFFSET] = (byte)reason;
        return eviction;
    }

    private static Task WriteAsync(TcpMessageStream client, bool send, CancellationToken token) => send
        ? client.SendMessagesAsync(Stream, Topic, Partitioning.PartitionId(0),
            new Message(Guid.NewGuid(), "payload"u8.ToArray()), token)
        : client.CreateStreamAsync("created", token);

    private static Task PartitionWriteAsync(TcpMessageStream client, bool send, CancellationToken token) => send
        ? WriteAsync(client, send: true, token)
        : StoreAsync(client, token);

    /// <summary>
    ///     Answers every SendMessages, or every StoreConsumerOffset, with <paramref name="answer" />, which also takes
    ///     how many of them the cluster recorded so far, and returns those records.
    /// </summary>
    private static ConcurrentQueue<MockRequest> AnswerPartitionWrites(RecoveryCluster cluster, bool send,
        Func<MockRequest, int, byte[]> answer)
    {
        var attempts = send ? cluster.Sends : cluster.Writes;
        Func<MockRequest, byte[]> handler = request => answer(request, attempts.Count);
        if (send)
        {
            cluster.OnSend = handler;
        }
        else
        {
            cluster.OnWrite = handler;
        }
        return attempts;
    }

    /// <summary>
    ///     Answers the first SendMessages, or the first CreateStream, with <paramref name="first" /> and every later
    ///     one with a proved success, and records them.
    /// </summary>
    private static ConcurrentQueue<MockRequest> AnswerWrites(RecoveryCluster cluster, bool send,
        Func<MockRequest, byte[]> first)
    {
        var writes = new ConcurrentQueue<MockRequest>();
        if (send)
        {
            cluster.OnSend = request => Answer(request, () => Reply(request.Operation, new byte[4]));
        }
        else
        {
            var metadata = cluster.OnMetadata;
            cluster.OnMetadata = request => request.Operation == (byte)VsrOperation.CreateStream
                ? Answer(request, () => Reply(request.Operation,
                    [.. new byte[4], .. BinaryFactory.CreateStreamPayload(1, 0, "created", 0, 0, 0)]))
                : metadata(request);
        }
        return writes;

        byte[] Answer(MockRequest request, Func<byte[]> proved)
        {
            writes.Enqueue(request);
            return writes.Count == 1 ? first(request) : proved();
        }
    }

    private static int Binds(RecoveryCluster cluster) => cluster.Coordinator.Requests(CommandCodes.BIND_SESSION_CODE)
        + cluster.Primary.Requests(CommandCodes.BIND_SESSION_CODE);

    // The mock counts a register when it arrives, before the client binds the new session from the reply.
    private static Task UntilSessionReplacedAsync(RecoveryCluster cluster, TcpMessageStream client)
    {
        var session = (ConsensusSession)typeof(TcpMessageStream)
            .GetField("_consensusSession", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(client)!;
        return cluster.UntilAsync(() => cluster.Coordinator.Registrations == 2 && session.IsBound);
    }

    private sealed class RecoveryCluster : IDisposable
    {
        private readonly MockNode _coordinator = new();
        private readonly MockNode _primary = new();
        internal MockNode Coordinator => _coordinator;
        internal MockNode Primary => _primary;
        internal int RouteLookups => _coordinator.Requests(CommandCodes.GET_PARTITION_ROUTE_CODE)
            + _primary.Requests(CommandCodes.GET_PARTITION_ROUTE_CODE);
        internal volatile bool PrimaryLeads;
        internal ushort PrimaryEndpoint;
        internal EvictionReason? PingEviction;
        /// <summary>Evicts the next cluster metadata read once.</summary>
        internal EvictionReason? RosterEviction;
        internal bool RejectRegister;
        internal EvictionReason RegisterRefusal = EvictionReason.InvalidCredentials;
        private int _evicted;
        private int _rosterEvicted;
        private readonly CancellationTokenSource _timeout = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        internal readonly ConcurrentQueue<MockRequest> Writes = new();
        internal readonly ConcurrentQueue<MockRequest> Polls = new();
        internal readonly ConcurrentQueue<MockRequest> Sends = new();
        internal readonly ConcurrentQueue<MockRequest> Syncs = new();
        internal uint PartitionCount = 2;
        internal Func<MockRequest, byte[]> OnBind = Answer;
        internal Func<MockRequest, byte[]> OnRegister = Answer;
        internal Func<MockRequest, byte[]?>? OnCoordinator;
        internal Func<MockRequest, byte[]?>? OnPrimary;
        internal Func<MockRequest, byte[]?>? OnRoster;
        /// <summary>Answers a route lookup, or returns null for the default route.</summary>
        internal Func<MockRequest, byte[]?>? OnRoute;
        internal Func<MockRequest, byte[]> OnSend = request => Reply(request.Operation, new byte[4]);
        internal Func<MockRequest, byte[]> OnWrite = request => Result(request);
        internal Action<MockRequest, byte[]>? BeforeReply;
        /// <summary>Holds the reply to a request until the returned task ends, without blocking the node.</summary>
        internal Func<MockRequest, Task>? HoldReply;
        internal Func<MockRequest, byte[]> OnPoll = request => Reply(request.Operation, new byte[40]);
        internal Func<MockRequest, byte[]> OnMetadata = request => Result(request);
        internal CancellationToken Token => _timeout.Token;

        internal RecoveryCluster(TimeSpan? timeout = null)
        {
            _timeout.CancelAfter(timeout ?? TimeSpan.FromSeconds(10));
            PrimaryEndpoint = _primary.Port;
            _coordinator.Serve(request => OnCoordinator?.Invoke(request) ?? Handle(request), BeforeReplyAsync);
            _primary.Serve(request => OnPrimary?.Invoke(request) ?? Handle(request), BeforeReplyAsync);
        }

        private Task BeforeReplyAsync(MockRequest request, byte[] reply)
        {
            BeforeReply?.Invoke(request, reply);
            return (HoldReply?.Invoke(request) ?? Task.CompletedTask).WaitAsync(Token);
        }

        internal byte[] Handle(MockRequest request)
        {
            if (request.Operation == (byte)VsrOperation.Register && RejectRegister)
            {
                return Eviction(RegisterRefusal);
            }
            if (request.Code == CommandCodes.PING_CODE && PingEviction is { } reason
                && Interlocked.Exchange(ref _evicted, 1) == 0)
            {
                return Eviction(reason);
            }
            if (request.Code == CommandCodes.GET_CLUSTER_METADATA_CODE && RosterEviction is { } rosterReason
                && Interlocked.Exchange(ref _rosterEvicted, 1) == 0)
            {
                return Eviction(rosterReason);
            }
            if (request.Operation == (byte)VsrOperation.Register)
            {
                return OnRegister(request);
            }
            if (request.Code == CommandCodes.GET_CLUSTER_METADATA_CODE)
            {
                if (OnRoster?.Invoke(request) is { } roster)
                {
                    return roster;
                }
                var body = new List<byte>();
                WriteString(body, "recovery");
                body.AddRange(BitConverter.GetBytes(2u));
                WriteNode(body, _coordinator.Port, !PrimaryLeads, 0);
                WriteNode(body, PrimaryEndpoint, PrimaryLeads, 1);
                return Reply(request.Operation, body.ToArray());
            }
            if (request.Code == CommandCodes.GET_PARTITION_ROUTE_CODE)
            {
                return OnRoute?.Invoke(request) ?? RouteReply(request, new PartitionContext(17, 0, 1), 1, PartitionCount);
            }
            if (request.Code == CommandCodes.BIND_SESSION_CODE)
            {
                return OnBind(request);
            }
            if (request.Code == CommandCodes.SYNC_CONSUMER_GROUP_CODE)
            {
                Syncs.Enqueue(request);
                return Reply(request.Operation, AssignmentBody(1, [0]));
            }
            if (request.Operation == (byte)VsrOperation.SendMessages)
            {
                Sends.Enqueue(request);
                return OnSend(request);
            }
            if (request.Operation == (byte)VsrOperation.StoreConsumerOffset)
            {
                Writes.Enqueue(request);
                return OnWrite(request);
            }
            if (request.Code == CommandCodes.POLL_MESSAGES_CODE)
            {
                Polls.Enqueue(request);
                return OnPoll(request);
            }
            if (request.Code == CommandCodes.GET_CONSUMER_OFFSET_CODE)
            {
                return Reply(request.Operation, []);
            }
            if (request.Operation == (byte)VsrOperation.Logout)
            {
                return Reply(request.Operation, []);
            }
            return ((VsrOperation)request.Operation).IsMetadata() ? OnMetadata(request) : Answer(request);
        }

        internal async Task UntilAsync(Func<bool> predicate)
        {
            while (!predicate())
            {
                await Task.Delay(5, Token);
            }
        }

        internal async Task<TcpMessageStream> ConnectAsync(int maxOperations = 256, TimeSpan? heartbeat = null,
            bool reconnect = true)
        {
            var client = new TcpMessageStream(new IggyClientConfigurator
            {
                BaseAddress = $"127.0.0.1:{_coordinator.Port}",
                Protocol = Protocol.Tcp,
                HeartbeatInterval = heartbeat ?? TimeSpan.FromHours(1),
                MaxPendingOperations = maxOperations,
                ReconnectionSettings = new ReconnectionSettings
                {
                    Enabled = reconnect,
                    InitialDelay = TimeSpan.FromMilliseconds(1),
                    WaitAfterReconnect = TimeSpan.Zero
                }
            }, NullLoggerFactory.Instance);
            await client.ConnectAsync(Token);
            await client.LoginUserAsync("user", "secret", Token);
            return client;
        }

        public void Dispose()
        {
            _timeout.Cancel();
            _coordinator.Dispose();
            _primary.Dispose();
            _timeout.Dispose();
        }
    }
}
