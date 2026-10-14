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
using Apache.Iggy.Contracts;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient.Implementations;
using Apache.Iggy.Kinds;
using Apache.Iggy.Utils;
using Apache.Iggy.Messages;
using Partitioning = Apache.Iggy.Kinds.Partitioning;
using Apache.Iggy.Vsr;
using Microsoft.Extensions.Logging.Abstractions;
using static Apache.Iggy.Tests.VsrTests.MockFrames;

namespace Apache.Iggy.Tests.VsrTests;

public sealed class NativeRecoveryTests
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
        await Assert.ThrowsAsync<VsrOperationNotRetainedException>(() =>
            client.ResolveOperationAsync(operationId, TimeSpan.FromSeconds(1), cluster.Token));
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
        cluster.EvictSession = true;
        await cluster.UntilAsync(() => cluster.Coordinator.Registrations == 2);
        await StoreAsync(client, cluster.Token);
        Assert.NotEqual(generation, ((ISessionGenerationProvider)client).SessionGeneration);
        Assert.Equal(2, cluster.Coordinator.Registrations);
        Assert.Equal(0, cluster.Primary.Registrations);
        Assert.True(cluster.Writes.Last().RequestId > original.RequestId);
        Assert.NotEqual(cluster.Writes.Last().ClientId, original.ClientId);
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
            await Assert.ThrowsAsync<VsrOperationNotRetainedException>(() =>
                client.ResolveOperationAsync(id, TimeSpan.FromSeconds(2), cluster.Token));
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
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task given_a_selected_partition_when_not_found_should_reselect_once_only_before_uncertainty(
        bool messageKey, bool uncertain)
    {
        using var cluster = new RecoveryCluster();
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
            Assert.Equal(1, cluster.Coordinator.Requests(CommandCodes.GET_PARTITION_ROUTE_CODE));
            await client.AbandonOperationAsync(Assert.IsType<ulong>(error.OperationId));
        }
        else
        {
            await send;
            var attempts = cluster.Sends.ToArray();
            Assert.Equal(2, attempts.Length);
            Assert.True(attempts[1].RequestId > attempts[0].RequestId);
            Assert.True(cluster.Coordinator.Requests(CommandCodes.GET_PARTITION_ROUTE_CODE)
                + cluster.Primary.Requests(CommandCodes.GET_PARTITION_ROUTE_CODE) >= 2);
        }
    }

    [Fact]
    public async Task given_an_explicit_partition_when_not_found_should_return_the_original_error_without_reselection()
    {
        using var cluster = new RecoveryCluster();
        cluster.OnSend = request => Reply(request.Operation, [], VsrError.PARTITION_NOT_FOUND);
        using var client = await cluster.ConnectAsync();
        var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => client.SendMessagesAsync(
            Stream, Topic, Partitioning.PartitionId(0), new Message(Guid.NewGuid(), "payload"u8.ToArray()), cluster.Token));
        Assert.Equal(VsrError.PARTITION_NOT_FOUND, error.StatusCode);
        Assert.Single(cluster.Sends);
        Assert.Equal(1, cluster.Coordinator.Requests(CommandCodes.GET_PARTITION_ROUTE_CODE));
    }

    private static Task StoreAsync(TcpMessageStream client, CancellationToken token, uint partition = 0) =>
        client.StoreOffsetAsync(Consumer.New(3), Stream, Topic, 10, partition, token);

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

    private sealed class RecoveryCluster : IDisposable
    {
        private readonly MockNode _coordinator = new();
        private readonly MockNode _primary = new();
        internal MockNode Coordinator => _coordinator;
        internal MockNode Primary => _primary;
        internal volatile bool PrimaryLeads;
        internal ushort PrimaryEndpoint;
        internal bool EvictSession;
        internal bool RejectRegister;
        private int _evicted;
        private readonly CancellationTokenSource _timeout = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        internal readonly ConcurrentQueue<MockRequest> Writes = new();
        internal readonly ConcurrentQueue<MockRequest> Polls = new();
        internal readonly ConcurrentQueue<MockRequest> Sends = new();
        internal uint PartitionCount = 2;
        internal Func<MockRequest, byte[]> OnSend = request => Reply(request.Operation, new byte[4]);
        internal Func<MockRequest, byte[]> OnWrite = request => Result(request);
        internal Action<MockRequest, byte[]>? BeforeReply;
        internal Func<MockRequest, byte[]> OnPoll = request => Reply(request.Operation, new byte[40]);
        internal CancellationToken Token => _timeout.Token;

        internal RecoveryCluster()
        {
            _timeout.CancelAfter(TimeSpan.FromSeconds(10));
            PrimaryEndpoint = _primary.Port;
            _coordinator.Serve(Handle, (request, reply) => BeforeReply?.Invoke(request, reply));
            _primary.Serve(Handle, (request, reply) => BeforeReply?.Invoke(request, reply));
        }

        internal byte[] Handle(MockRequest request)
        {
            if (request.Operation == (byte)VsrOperation.Register && RejectRegister)
            {
                var eviction = new byte[VsrHeader.HEADER_SIZE];
                BinaryPrimitives.WriteUInt32LittleEndian(eviction.AsSpan(SIZE_OFFSET), VsrHeader.HEADER_SIZE);
                eviction[COMMAND_OFFSET] = COMMAND_EVICTION;
                eviction[EVICTION_REASON_OFFSET] = (byte)EvictionReason.InvalidCredentials;
                return eviction;
            }
            if (request.Code == CommandCodes.PING_CODE && EvictSession && Interlocked.Exchange(ref _evicted, 1) == 0)
            {
                var eviction = new byte[VsrHeader.HEADER_SIZE];
                BinaryPrimitives.WriteUInt32LittleEndian(eviction.AsSpan(SIZE_OFFSET), VsrHeader.HEADER_SIZE);
                eviction[COMMAND_OFFSET] = COMMAND_EVICTION;
                eviction[EVICTION_REASON_OFFSET] = EVICTION_STALE_CLIENT;
                return eviction;
            }
            if (request.Code == CommandCodes.GET_CLUSTER_METADATA_CODE)
            {
                var body = new List<byte>();
                WriteString(body, "recovery");
                body.AddRange(BitConverter.GetBytes(2u));
                WriteNode(body, _coordinator.Port, !PrimaryLeads, 0);
                WriteNode(body, PrimaryEndpoint, PrimaryLeads, 1);
                return Reply(request.Operation, body.ToArray());
            }
            if (request.Code == CommandCodes.GET_PARTITION_ROUTE_CODE)
            {
                return RouteReply(request, new PartitionContext(17, 0, 1), 1, PartitionCount);
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
            return ((VsrOperation)request.Operation).IsMetadata() ? Result(request) : Answer(request);
        }

        internal async Task UntilAsync(Func<bool> predicate)
        {
            while (!predicate())
            {
                await Task.Delay(5, Token);
            }
        }

        internal async Task<TcpMessageStream> ConnectAsync(int maxOperations = 256, TimeSpan? heartbeat = null)
        {
            var client = new TcpMessageStream(new IggyClientConfigurator
            {
                BaseAddress = $"127.0.0.1:{_coordinator.Port}",
                Protocol = Protocol.Tcp,
                HeartbeatInterval = heartbeat ?? TimeSpan.FromHours(1),
                MaxPendingOperations = maxOperations,
                ReconnectionSettings = new ReconnectionSettings
                {
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
