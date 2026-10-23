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
using System.Net.Sockets;
using System.Reflection;
using System.Runtime.ExceptionServices;
using Apache.Iggy.Configuration;
using Apache.Iggy.Consumers;
using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Tcp;
using Apache.Iggy.Encryption;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient.Implementations;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;
using Apache.Iggy.Tests.ContractsTests;
using Apache.Iggy.Utils;
using Apache.Iggy.Vsr;
using Microsoft.Extensions.Logging.Abstractions;
using static Apache.Iggy.Tests.VsrTests.MockFrames;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Tests.VsrTests;

public sealed class PrimaryPollingTests
{
    private static readonly Identifier Stream = Identifier.Numeric(1);
    private static readonly Identifier Topic = Identifier.Numeric(2);
    private static readonly Identifier Group = Identifier.Numeric(3);
    private const ulong MetadataCommit = 50;

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_balanced_send_refusals_when_its_budget_ends_should_distinguish_caller_cancellation(
        bool cancelCaller)
    {
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cancellation.CancelAfter(TimeSpan.FromSeconds(40));
        using var cluster = new PollCluster();
        var sent = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        cluster.OnPrimarySend = request =>
        {
            sent.TrySetResult();
            return Reply(request.Operation, [], VsrError.TRANSIENT_NOT_ACCEPTED);
        };
        using var client = await cluster.ConnectAsync();
        if (cancelCaller)
        {
            await cancellation.CancelAsync();
        }
        var sending = client.SendMessagesAsync(Stream, Topic, Partitioning.None(),
            new Message(Guid.NewGuid(), "payload"u8.ToArray()), cancellation.Token);
        if (cancelCaller)
        {
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => sending);
            Assert.False(sent.Task.IsCompleted);
        }
        else
        {
            await sent.Task.WaitAsync(cancellation.Token);
            var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => sending);
            Assert.Equal(VsrError.TRANSIENT_NOT_ACCEPTED, error.StatusCode);
            Assert.False(cancellation.IsCancellationRequested);
        }
    }

    [Theory]
    [InlineData(0, 50)]
    [InlineData(1, 100)]
    [InlineData(2, 200)]
    [InlineData(3, 400)]
    [InlineData(4, 800)]
    [InlineData(5, 1000)]
    [InlineData(31, 1000)]
    [InlineData(int.MaxValue, 1000)]
    public void given_routing_retries_when_drawing_backoff_should_cover_full_jitter_up_to_the_cap(
        int retry, int ceilingMilliseconds)
    {
        foreach (var sample in new[] { 0.0, 0.25, 0.5, 1.0 })
        {
            Assert.Equal(TimeSpan.FromMilliseconds(ceilingMilliseconds * sample),
                TcpMessageStream.PollRoutingRetryDelay(retry, sample));
        }
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task given_split_primaries_when_polling_should_reuse_data_connections_and_keep_membership(
        bool group, bool resolvePartition)
    {
        using var cluster = new PollCluster();
        using var client = await cluster.ConnectAsync();
        await client.JoinConsumerGroupAsync(Stream, Topic, Group, TestContext.Current.CancellationToken);
        var generation = ((ISessionGenerationProvider)client).SessionGeneration;
        for (uint index = 0; index < 4; index++)
        {
            var result = await client.PollMessagesAsync(Stream, Topic, resolvePartition ? null : index % 2,
                group ? Consumer.Group(3) : Consumer.New(3), PollingStrategy.Next(), 10, true,
                TestContext.Current.CancellationToken);
            Assert.Equal(index % 2, result.PartitionId);
        }

        Assert.Equal(1, cluster.Coordinator.Connections);
        Assert.Equal(1, cluster.Coordinator.Registrations);
        Assert.Equal(generation, ((ISessionGenerationProvider)client).SessionGeneration);
        Assert.Equal(0, cluster.Coordinator.Requests(CommandCodes.POLL_MESSAGES_CODE));
        Assert.Equal(2, cluster.RouteRequests.Length);
        Assert.All(cluster.Primaries, primary =>
        {
            Assert.Equal(1, primary.Connections);
            Assert.Equal(0, primary.Registrations);
            Assert.Equal(1, primary.Requests(CommandCodes.BIND_SESSION_CODE));
            Assert.Equal(2, primary.Requests(CommandCodes.POLL_MESSAGES_CODE));
        });
        var parent = Assert.Single(cluster.CoordinatorRequests,
            request => request.Operation == (byte)VsrOperation.JoinConsumerGroup);
        var registration = Assert.Single(cluster.CoordinatorRequests,
            request => request.Operation == (byte)VsrOperation.Register);
        var bindSecret = LoginRegister.ReadBindSecret(registration.Body);
        Assert.All(cluster.Attachments, attachment =>
        {
            Assert.Equal(parent.ClientId, BinaryPrimitives.ReadUInt128LittleEndian(BindIdentity(attachment.Body)));
            Assert.Equal(parent.Session, BinaryPrimitives.ReadUInt64LittleEndian(BindIdentity(attachment.Body).AsSpan(16)));
            Assert.Equal(bindSecret, attachment.Body.AsSpan(^LoginRegister.BIND_SECRET_BYTES).ToArray());
            Assert.Equal(parent.ClientId, attachment.ClientId);
            Assert.Equal(parent.Session, attachment.Session);
        });
    }

    [Fact]
    public async Task given_cached_route_when_metadata_commits_should_refresh_at_the_acknowledged_floor()
    {
        using var cluster = new PollCluster();
        using var client = await cluster.ConnectAsync();
        await PollAsync(client);
        await client.DeleteTopicAsync(Stream, Topic, TestContext.Current.CancellationToken);
        await PollAsync(client);

        Assert.Equal(2, cluster.RouteRequests.Length);
        Assert.Equal(new ulong[] { 0, MetadataCommit }, cluster.RouteRequests.Select(request => request.Context.MetadataOp));
        Assert.Equal(1, cluster.Primaries[0].Connections);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task given_unknown_topology_when_the_probe_fails_should_wait_for_a_valid_roster_before_polling(
        bool standalone, bool unavailable)
    {
        using var cluster = new PollCluster(standalone ? 0 : 2);
        var refuseMetadata = true;
        cluster.OnCoordinator = request => request.Code == CommandCodes.GET_CLUSTER_METADATA_CODE && refuseMetadata
            ? Reply(request.Operation, [], unavailable ? (uint)VsrError.FEATURE_UNAVAILABLE : 0)
            : null;
        using var client = await cluster.ConnectAsync();
        var generation = ((ISessionGenerationProvider)client).SessionGeneration;
        if (unavailable)
        {
            var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => PollAsync(client));
            Assert.Equal(VsrError.FEATURE_UNAVAILABLE, error.StatusCode);
        }
        else
        {
            await Assert.ThrowsAsync<MalformedResponseException>(() => PollAsync(client));
        }

        Assert.Equal(0, cluster.Coordinator.Requests(CommandCodes.POLL_MESSAGES_CODE));
        Assert.Empty(cluster.RouteRequests);
        Assert.All(cluster.Primaries, primary => Assert.Equal(0, primary.Connections));
        Assert.Equal(generation, ((ISessionGenerationProvider)client).SessionGeneration);

        refuseMetadata = false;
        await PollAsync(client);
        refuseMetadata = true;
        if (unavailable)
        {
            await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() =>
                client.GetClusterMetadataAsync(TestContext.Current.CancellationToken));
        }
        else
        {
            Assert.Null(await client.GetClusterMetadataAsync(TestContext.Current.CancellationToken));
        }

        var metadataReads = cluster.Coordinator.Requests(CommandCodes.GET_CLUSTER_METADATA_CODE);
        await PollAsync(client);

        Assert.Equal(metadataReads, cluster.Coordinator.Requests(CommandCodes.GET_CLUSTER_METADATA_CODE));
        Assert.Equal(standalone ? 2 : 0, cluster.Coordinator.Requests(CommandCodes.POLL_MESSAGES_CODE));
        Assert.Single(cluster.RouteRequests);
        Assert.Equal(generation, ((ISessionGenerationProvider)client).SessionGeneration);
        if (!standalone)
        {
            Assert.Equal(2, cluster.Primaries[0].Requests(CommandCodes.POLL_MESSAGES_CODE));
        }
    }

    [Theory]
    [InlineData((byte)VsrOperation.TruncatePartition)]
    [InlineData((byte)VsrOperation.DeleteSegments)]
    public async Task given_delete_segments_commit_when_polling_should_raise_the_route_discovery_floor(byte replyOperation)
    {
        using var cluster = new PollCluster();
        cluster.OnCoordinator = request =>
        {
            if (request.Operation != (byte)VsrOperation.DeleteSegments)
            {
                return null;
            }

            var reply = Reply(replyOperation,
                new byte[4]);
            BinaryPrimitives.WriteUInt64LittleEndian(reply.AsSpan(VsrHeader.REPLY_COMMIT_OFFSET), MetadataCommit);
            return reply;
        };
        using var client = await cluster.ConnectAsync();
        await PollAsync(client);
        await client.DeleteSegmentsAsync(Stream, Topic, 0, 1, TestContext.Current.CancellationToken);
        await PollAsync(client);

        Assert.Equal(new ulong[] { 0, MetadataCommit }, cluster.RouteRequests.Select(request => request.Context.MetadataOp));
        Assert.Equal(1, cluster.Primaries[0].Connections);
    }

    [Fact]
    public async Task given_group_assignment_read_when_polling_should_refresh_at_its_metadata_floor()
    {
        using var cluster = new PollCluster();
        cluster.OnCoordinator = request =>
        {
            if (request.Code != CommandCodes.GET_CONSUMER_GROUP_CODE)
            {
                return null;
            }
            var reply = Reply(request.Operation, []);
            BinaryPrimitives.WriteUInt64LittleEndian(reply.AsSpan(VsrHeader.REPLY_COMMIT_OFFSET), MetadataCommit);
            return reply;
        };
        using var client = await cluster.ConnectAsync();
        await PollAsync(client);
        _ = await client.GetConsumerGroupByIdAsync(Stream, Topic, Group, TestContext.Current.CancellationToken);
        await PollAsync(client);

        Assert.Equal(new ulong[] { 0, MetadataCommit }, cluster.RouteRequests.Select(request => request.Context.MetadataOp));
    }

    [Fact]
    public async Task given_roster_without_nodes_when_polling_should_preserve_the_last_valid_topology()
    {
        using var cluster = new PollCluster();
        var emptyRoster = false;
        cluster.OnCoordinator = request => request.Code == CommandCodes.GET_CLUSTER_METADATA_CODE && emptyRoster
            ? Reply(request.Operation, new byte[8])
            : null;
        using var client = await cluster.ConnectAsync();
        await PollAsync(client);
        emptyRoster = true;
        await Assert.ThrowsAsync<MalformedResponseException>(() =>
            client.GetClusterMetadataAsync(TestContext.Current.CancellationToken));
        var reads = cluster.Coordinator.Requests(CommandCodes.GET_CLUSTER_METADATA_CODE);
        await PollAsync(client);

        Assert.Equal(reads, cluster.Coordinator.Requests(CommandCodes.GET_CLUSTER_METADATA_CODE));
        Assert.Equal(0, cluster.Coordinator.Requests(CommandCodes.POLL_MESSAGES_CODE));
        Assert.Equal(2, cluster.Primaries[0].Requests(CommandCodes.POLL_MESSAGES_CODE));
    }

    [Fact]
    public async Task given_only_route_refusals_when_canceled_should_send_no_poll()
    {
        using var cluster = new PollCluster();
        byte[]? Refuse(MockRequest request) => request.Code == CommandCodes.GET_PARTITION_ROUTE_CODE
            ? Reply(request.Operation, [], VsrError.TRANSIENT_NOT_ACCEPTED) : null;
        cluster.OnCoordinator = Refuse;
        cluster.OnPrimaryControl = Refuse;
        using var client = await cluster.ConnectAsync();
        var generation = ((ISessionGenerationProvider)client).SessionGeneration;
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cancellation.CancelAfter(TimeSpan.FromMilliseconds(200));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => client.PollMessagesAsync(Stream, Topic, 0,
            Consumer.New(3), PollingStrategy.Offset(0), 10, false, cancellation.Token));
        Assert.Equal(generation, ((ISessionGenerationProvider)client).SessionGeneration);
        Assert.All(cluster.Primaries, primary => Assert.Equal(0, primary.Requests(CommandCodes.POLL_MESSAGES_CODE)));
        Assert.Equal(0, cluster.Coordinator.Requests(CommandCodes.POLL_MESSAGES_CODE));
    }

    [Fact]
    public async Task given_primary_refusal_when_the_budget_expires_in_the_next_route_lookup_should_report_not_accepted()
    {
        using var cluster = new PollCluster();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var lookups = 0;
        cluster.OnPrimaryPoll = request => Reply(request.Operation, [], TRANSIENT_NOT_ACCEPTED);
        cluster.OnCoordinator = request =>
        {
            if (request.Code == CommandCodes.GET_PARTITION_ROUTE_CODE && Interlocked.Increment(ref lookups) >= 2)
            {
                release.Task.GetAwaiter().GetResult();
            }

            return null;
        };
        cluster.OnPrimaryControl = cluster.OnCoordinator;
        using var client = await cluster.ConnectAsync();
        try
        {
            var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => PollAsync(client));

            Assert.Equal(VsrError.TRANSIENT_NOT_ACCEPTED, error.StatusCode);
            Assert.True(error.FromServer);
            Assert.True(Volatile.Read(ref lookups) >= 2);
            Assert.Equal(1, cluster.Primaries[0].Requests(CommandCodes.POLL_MESSAGES_CODE));
        }
        finally
        {
            release.SetResult();
        }
    }

    [Fact]
    public async Task given_a_waiting_progress_poll_when_metadata_commits_should_keep_its_context_and_refresh_the_next_call()
    {
        using var cluster = new PollCluster();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var polls = 0;
        cluster.OnPrimaryPoll = request =>
        {
            if (Interlocked.Increment(ref polls) == 1)
            {
                started.SetResult();
                release.Task.GetAwaiter().GetResult();
            }

            return Batch(request);
        };
        using var client = await cluster.ConnectAsync();
        var first = PollAsync(client);
        try
        {
            await started.Task.WaitAsync(TestContext.Current.CancellationToken);
            var waiting = PollAsync(client);
            Assert.False(waiting.IsCompleted);
            await client.DeleteTopicAsync(Stream, Topic, TestContext.Current.CancellationToken);
            release.SetResult();
            await Task.WhenAll(first, waiting);

            Assert.Equal(2, polls);
            Assert.All(cluster.PrimaryRequests.Where(request => request.Code == CommandCodes.POLL_MESSAGES_CODE),
                request => Assert.Equal(cluster.RouteContext, request.Context));
            await PollAsync(client);
            Assert.Equal(new ulong[] { 0, MetadataCommit }, cluster.RouteRequests.Select(request => request.Context.MetadataOp));
            Assert.Equal(2, cluster.RouteRequests.Length);
            Assert.Equal(1, cluster.Primaries[0].Connections);
        }
        finally
        {
            release.TrySetResult();
        }
    }

    [Fact]
    public async Task given_a_large_roster_when_links_start_should_bound_dials_and_preserve_ready_progress()
    {
        using var cluster = new PollCluster(7);
        var fourBindings = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allBindings = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var bindings = 0;
        cluster.OnPrimaryAttachment = request =>
        {
            var count = Interlocked.Increment(ref bindings);
            if (count == 4)
            {
                fourBindings.TrySetResult();
            }
            if (count == cluster.Primaries.Length)
            {
                allBindings.TrySetResult();
            }
            release.Task.GetAwaiter().GetResult();
            return BindingReply(request);
        };
        using var client = await cluster.ConnectAsync();
        try
        {
            await fourBindings.Task.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
            Assert.Equal(4, cluster.Primaries.Sum(node => node.Connections));
            await client.PingAsync(TestContext.Current.CancellationToken);
            Assert.Equal(4, Volatile.Read(ref bindings));
            release.TrySetResult();
            await allBindings.Task.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
            await client.PollMessagesAsync(Stream, Topic, 6, Consumer.New(3), PollingStrategy.Offset(0), 10, false,
                TestContext.Current.CancellationToken);
            Assert.All(cluster.Primaries, node =>
            {
                Assert.Equal(1, node.Connections);
                Assert.Equal(0, node.Registrations);
            });
            Assert.Equal(1, cluster.Coordinator.Connections);
        }
        finally
        {
            release.TrySetResult();
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_auxiliary_poll_outcome_unknown_when_polling_should_not_replay_or_move_coordinator(
        bool lostConnection)
    {
        using var cluster = new PollCluster();
        var polls = 0;
        cluster.OnPrimaryPoll = request =>
        {
            if (Interlocked.Increment(ref polls) == 1)
            {
                if (lostConnection)
                {
                    throw new IOException("The primary lost the reply after admitting the poll.");
                }

                return Reply(request.Operation, [], VsrError.TRANSIENT_NOT_COMMITTED);
            }

            return Batch(request);
        };
        using var client = await cluster.ConnectAsync();
        var generation = ((ISessionGenerationProvider)client).SessionGeneration;

        await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() => PollAsync(client));
        Assert.Equal(1, polls);
        Assert.Equal(1, cluster.Coordinator.Connections);
        await PollAsync(client);

        Assert.Equal(2, polls);
        Assert.Equal(lostConnection ? 2 : 1, cluster.Primaries[0].Connections);
        Assert.Equal(1, cluster.Coordinator.Registrations);
        Assert.Equal(generation, ((ISessionGenerationProvider)client).SessionGeneration);
    }

    [Theory]
    [InlineData(SIZE_OFFSET, false)]
    [InlineData(SIZE_OFFSET, true)]
    [InlineData(COMMAND_OFFSET, false)]
    [InlineData(COMMAND_OFFSET, true)]
    [InlineData(REPLY_OPERATION_OFFSET, false)]
    [InlineData(REPLY_OPERATION_OFFSET, true)]
    public async Task given_a_progress_poll_when_its_reply_frame_is_malformed_should_report_unknown_without_replaying(
        int invalidField, bool next)
    {
        const int MaxResponseFrameSize = 1024;
        using var cluster = new PollCluster();
        var polls = 0;
        cluster.OnPrimaryPoll = request =>
        {
            Interlocked.Increment(ref polls);
            var reply = Batch(request);
            if (invalidField == SIZE_OFFSET)
            {
                BinaryPrimitives.WriteUInt32LittleEndian(reply.AsSpan(SIZE_OFFSET), MaxResponseFrameSize + 1);
            }
            else
            {
                reply[invalidField] = byte.MaxValue;
            }
            return reply;
        };
        using var client = await cluster.ConnectAsync(MaxResponseFrameSize);
        var generation = ((ISessionGenerationProvider)client).SessionGeneration;
        var connection = await (Task<VsrConnection?>)typeof(TcpMessageStream)
            .GetMethod("FindReadyReplicaAsync", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(client, [(byte)1, TestContext.Current.CancellationToken])!;
        Assert.NotNull(connection);
        var lifetime = Assert.IsType<CancellationTokenSource>(typeof(VsrConnection)
            .GetField("_lifetime", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(connection));
        var closing = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var finishClosing = new ManualResetEventSlim();
        using var registration = lifetime.Token.Register(() =>
        {
            closing.TrySetResult();
            finishClosing.Wait(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        });

        try
        {
            var error = await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() => client.PollMessagesAsync(
                Stream, Topic, 0, Consumer.New(3), next ? PollingStrategy.Next() : PollingStrategy.Offset(0), 1,
                !next, TestContext.Current.CancellationToken));
            Assert.Null(error.OperationId);
            var invalid = Assert.IsType<IggyInvalidStatusCodeException>(error.InnerException);
            Assert.False(invalid.FromServer);
            Assert.False(invalid.Committed);
            Assert.Equal(VsrError.INVALID_COMMAND, invalid.StatusCode);
            Assert.Equal(1, polls);
            await closing.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
            Assert.False(connection.Closed.IsCompleted);
            Assert.False(connection.IsReady);
        }
        finally
        {
            finishClosing.Set();
        }

        await client.StoreOffsetAsync(Consumer.New(3), Stream, Topic, 10, 0,
            TestContext.Current.CancellationToken);
        Assert.Single(cluster.OffsetWrites);
        Assert.Equal(1, polls);
        Assert.Equal(generation, ((ISessionGenerationProvider)client).SessionGeneration);
        Assert.Equal(1, cluster.Coordinator.Registrations);
    }

    [Theory]
    [InlineData(VsrError.TRANSIENT_NOT_ACCEPTED)]
    [InlineData(VsrError.NOT_PRIMARY)]
    public async Task given_primary_refusal_when_polling_should_refresh_route_without_moving_membership(int status)
    {
        using var cluster = new PollCluster();
        var polls = 0;
        cluster.OnPrimaryPoll = request => Interlocked.Increment(ref polls) == 1
            ? Reply(request.Operation, [], (uint)status)
            : Batch(request);
        cluster.OnPrimaryControl = request => request.Code == CommandCodes.GET_PARTITION_ROUTE_CODE
            ? RouteReply(request, cluster.RouteContext, 1, 2, view: 1) : null;
        using var client = await cluster.ConnectAsync();
        await PollAsync(client);

        Assert.Equal(2, polls);
        Assert.Equal(2, cluster.RouteRequests.Length);
        Assert.Equal(1, cluster.Primaries[0].Requests(CommandCodes.BIND_SESSION_CODE));
        Assert.Equal(1, cluster.Primaries[0].Connections);
        Assert.Equal(1, cluster.Coordinator.Registrations);
    }

    [Theory]
    [InlineData(0u)]
    [InlineData((uint)VsrError.STALE_CLIENT)]
    [InlineData((uint)VsrError.UNAUTHENTICATED)]
    public async Task given_coordinator_control_failure_when_polling_should_restore_the_manually_authenticated_session(
        uint status)
    {
        using var cluster = new PollCluster();
        var routes = 0;
        cluster.OnCoordinator = request =>
        {
            if (request.Code == CommandCodes.GET_PARTITION_ROUTE_CODE && Interlocked.Increment(ref routes) == 1)
            {
                if (status != 0)
                {
                    return Reply(request.Operation, [], status);
                }

                throw new IOException("Coordinator control connection was lost before routing completed.");
            }

            return null;
        };
        using var client = await cluster.ConnectAsync();
        var generation = ((ISessionGenerationProvider)client).SessionGeneration;
        if (status == (uint)VsrError.STALE_CLIENT)
        {
            var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => PollAsync(client));
            Assert.Equal((int)status, error.StatusCode);
        }
        await PollAsync(client);
        await cluster.UntilAsync(() => cluster.Coordinator.Requests(CommandCodes.BIND_SESSION_CODE) == 1);
        Assert.Equal(generation, ((ISessionGenerationProvider)client).SessionGeneration);
        Assert.Equal(1, cluster.Coordinator.Registrations);
        Assert.Equal(1, cluster.Primaries[0].Requests(CommandCodes.POLL_MESSAGES_CODE));
    }

    [Fact]
    public async Task given_a_live_session_when_a_primary_refuses_a_consumer_poll_with_stale_client_should_keep_polling()
    {
        using var cluster = new PollCluster();
        var polls = 0;
        cluster.OnPrimaryPoll = request => Interlocked.Increment(ref polls) == 1
            ? Reply(request.Operation, [], VsrError.STALE_CLIENT)
            : Reply(request.Operation, Convert.FromHexString(MessageBatchGoldenVectorTests.POLL_BODY));
        using var client = await cluster.ConnectAsync();
        var generation = ((ISessionGenerationProvider)client).SessionGeneration;
        var states = RecordStates(client);
        await using var consumer = PartitionConsumer(client, PollingStrategy.Next());
        var errors = RecordErrors(consumer);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cancellation.CancelAfter(TimeSpan.FromSeconds(10));
        await consumer.InitAsync(cancellation.Token);
        await using var messages = consumer.ReceiveAsync(cancellation.Token).GetAsyncEnumerator(cancellation.Token);

        Assert.True(await messages.MoveNextAsync());

        var refusal = Assert.IsType<IggyInvalidStatusCodeException>(Assert.Single(errors));
        Assert.Equal(VsrError.STALE_CLIENT, refusal.StatusCode);
        Assert.Equal(2, polls);
        Assert.Equal(2, cluster.Primaries[0].Requests(CommandCodes.BIND_SESSION_CODE));
        Assert.Equal(1, cluster.Coordinator.Registrations);
        Assert.Equal(generation, ((ISessionGenerationProvider)client).SessionGeneration);
        Assert.Empty(states);
    }

    [Fact]
    public async Task given_a_consumer_poll_in_flight_when_a_stale_client_refusal_detaches_its_link_should_poll_again()
    {
        using var cluster = new PollCluster();
        var polls = 0;
        var consumerPolling = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        cluster.OnPrimaryPoll = request =>
        {
            if (Interlocked.Increment(ref polls) > 1)
            {
                return Reply(request.Operation, Convert.FromHexString(MessageBatchGoldenVectorTests.POLL_BODY));
            }
            // The refusal waits until the consumer poll follows this request on the same link.
            consumerPolling.Task.Wait(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
            Thread.Sleep(100);
            return Reply(request.Operation, [], VsrError.STALE_CLIENT);
        };
        using var client = await cluster.ConnectAsync();
        var generation = ((ISessionGenerationProvider)client).SessionGeneration;
        var states = RecordStates(client);
        var refused = client.PollMessagesAsync(Stream, Topic, 0, Consumer.New(4), PollingStrategy.First(), 1, false,
            TestContext.Current.CancellationToken);
        await cluster.UntilAsync(() => Volatile.Read(ref polls) == 1);
        await using var consumer = PartitionConsumer(client, PollingStrategy.Next());
        var errors = RecordErrors(consumer);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cancellation.CancelAfter(TimeSpan.FromSeconds(10));
        await consumer.InitAsync(cancellation.Token);
        await using var messages = consumer.ReceiveAsync(cancellation.Token).GetAsyncEnumerator(cancellation.Token);
        var receiving = messages.MoveNextAsync().AsTask();
        consumerPolling.TrySetResult();

        Assert.True(await receiving);

        var refusal = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => refused);
        Assert.Equal(VsrError.STALE_CLIENT, refusal.StatusCode);
        Assert.Null(Assert.IsType<VsrRequestOutcomeUnknownException>(Assert.Single(errors)).OperationId);
        Assert.Equal(2, cluster.Primaries[0].Requests(CommandCodes.BIND_SESSION_CODE));
        Assert.Equal(1, cluster.Coordinator.Registrations);
        Assert.Equal(generation, ((ISessionGenerationProvider)client).SessionGeneration);
        Assert.Empty(states);
    }

    [Fact]
    public async Task given_a_consumer_poll_in_flight_when_its_link_is_lost_should_wait_for_the_link_and_poll_again()
    {
        using var cluster = new PollCluster();
        var down = false;
        cluster.OnPrimaryPoll = request => Volatile.Read(ref down)
            ? throw new IOException("outage")
            : Reply(request.Operation, Convert.FromHexString(MessageBatchGoldenVectorTests.POLL_BODY));
        cluster.OnPrimaryAttachment = request => Volatile.Read(ref down)
            ? throw new IOException("outage")
            : BindingReply(request);
        using var client = await cluster.ConnectAsync();
        // Both primary links bind before the outage, so it fails only the link of the consumer poll.
        foreach (var partition in new uint[] { 0, 1 })
        {
            await client.PollMessagesAsync(Stream, Topic, partition, Consumer.New(3), PollingStrategy.First(), 1, false,
                TestContext.Current.CancellationToken);
        }
        var generation = ((ISessionGenerationProvider)client).SessionGeneration;
        var states = RecordStates(client);
        await using var consumer = PartitionConsumer(client, PollingStrategy.Next());
        var errors = RecordErrors(consumer);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cancellation.CancelAfter(TimeSpan.FromSeconds(10));
        await consumer.InitAsync(cancellation.Token);
        await using var messages = consumer.ReceiveAsync(cancellation.Token).GetAsyncEnumerator(cancellation.Token);
        Volatile.Write(ref down, true);
        var receiving = messages.MoveNextAsync().AsTask();

        // While the link fails eight binds, the next poll waits for it instead of failing at once.
        await cluster.UntilAsync(() => cluster.Primaries[0].Requests(CommandCodes.BIND_SESSION_CODE) >= 9);
        Assert.False(receiving.IsCompleted, receiving.Exception?.GetBaseException().ToString());
        Assert.Null(Assert.IsType<VsrRequestOutcomeUnknownException>(Assert.Single(errors)).OperationId);
        Volatile.Write(ref down, false);

        Assert.True(await receiving);
        Assert.Single(errors);
        Assert.Equal(3, cluster.Primaries[0].Requests(CommandCodes.POLL_MESSAGES_CODE));
        Assert.Equal(1, cluster.Primaries[1].Connections);
        Assert.Equal(1, cluster.Coordinator.Registrations);
        Assert.Equal(generation, ((ISessionGenerationProvider)client).SessionGeneration);
        Assert.Empty(states);
    }

    [Fact]
    public async Task given_a_stale_binding_when_links_start_should_replace_the_logical_session_once()
    {
        using var cluster = new PollCluster();
        var attachments = 0;
        cluster.OnPrimaryAttachment = request => Interlocked.Increment(ref attachments) == 1
            ? Reply(request.Operation, [], VsrError.STALE_CLIENT)
            : BindingReply(request);
        using var client = await cluster.ConnectAsync();
        await cluster.UntilAsync(() => cluster.Coordinator.Registrations == 2);
        await PollAsync(client);
        Assert.True(attachments >= 3);
        Assert.Equal(1, cluster.Primaries[0].Requests(CommandCodes.POLL_MESSAGES_CODE));
        Assert.Equal(2, cluster.Coordinator.Registrations);
        var registrations = cluster.CoordinatorRequests.Where(request => request.Operation == OPERATION_REGISTER).ToArray();
        Assert.NotEqual(registrations[0].ClientId, registrations[1].ClientId);
    }

    [Fact]
    public async Task given_wrong_binding_epoch_when_polling_should_refuse_before_polling()
    {
        using var cluster = new PollCluster();
        cluster.OnPrimaryAttachment = request => Reply(request.Operation, RegisterBody(session: 129)[4..]);
        using var client = await cluster.ConnectAsync();

        var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => PollAsync(client));

        Assert.Equal(VsrError.SESSION_MISMATCH, error.StatusCode);
        Assert.All(cluster.Primaries, primary =>
            Assert.Equal(0, primary.Requests(CommandCodes.POLL_MESSAGES_CODE)));
        Assert.Equal(1, cluster.Coordinator.Registrations);
    }

    [Fact]
    public async Task given_a_canceled_progress_poll_when_its_reply_arrives_late_should_reuse_the_healthy_link()
    {
        using var cluster = new PollCluster();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var polls = 0;
        cluster.OnPrimaryPoll = request =>
        {
            if (Interlocked.Increment(ref polls) == 1)
            {
                started.TrySetResult();
                release.Task.GetAwaiter().GetResult();
            }
            return Batch(request);
        };
        using var client = await cluster.ConnectAsync();
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        var pending = client.PollMessagesAsync(Stream, Topic, 0, Consumer.Group(3), PollingStrategy.Next(), 10,
            true, cancellation.Token);
        try
        {
            await started.Task.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
            await cancellation.CancelAsync();
            var error = await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() => pending);
            Assert.Null(error.OperationId);
            release.TrySetResult();
            await PollAsync(client);
            Assert.Equal(2, polls);
            Assert.Equal(1, cluster.Primaries[0].Connections);
            Assert.Equal(1, cluster.Coordinator.Connections);
        }
        finally
        {
            release.TrySetResult();
        }
    }

    [Fact]
    public async Task given_self_credentials_changed_when_opening_new_primary_should_reuse_the_registered_proof()
    {
        using var cluster = new PollCluster();
        using var client = await cluster.ConnectAsync();
        await PollAsync(client);
        await client.UpdateUserAsync(Identifier.Numeric(7), "renamed", token: TestContext.Current.CancellationToken);
        await client.ChangePasswordAsync(Identifier.String("renamed"), "secret", "new-secret",
            TestContext.Current.CancellationToken);
        await client.PollMessagesAsync(Stream, Topic, 1, Consumer.Group(3), PollingStrategy.Next(), 10, true,
            TestContext.Current.CancellationToken);

        Assert.Empty(cluster.Registrations);
        var attachments = cluster.Attachments.ToArray();
        var registration = Assert.Single(cluster.CoordinatorRequests,
            request => request.Operation == (byte)VsrOperation.Register);
        var expectedSecret = LoginRegister.ReadBindSecret(registration.Body);
        Assert.All(attachments, attachment =>
            Assert.Equal(expectedSecret, attachment.Body.AsSpan(^LoginRegister.BIND_SECRET_BYTES).ToArray()));
        Assert.Equal(1, cluster.Coordinator.Registrations);
    }

    [Fact]
    public async Task given_route_refresh_when_its_session_is_replaced_should_not_retry_in_the_new_session()
    {
        using var cluster = new PollCluster();
        var refreshing = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        cluster.OnPrimaryControl = request =>
        {
            if (request.Code != CommandCodes.GET_PARTITION_ROUTE_CODE)
            {
                return null;
            }
            refreshing.TrySetResult();
            release.Task.GetAwaiter().GetResult();
            return RouteReply(request, cluster.RouteContext, 1, 2, view: 1);
        };
        using var client = await cluster.ConnectAsync();
        await client.PollMessagesAsync(Stream, Topic, 0, Consumer.New(3), PollingStrategy.Offset(0), 10, false,
            TestContext.Current.CancellationToken);
        cluster.OnPrimaryPoll = request => Reply(request.Operation, [], VsrError.NOT_PRIMARY);
        cluster.OnCoordinator = request => request.Code == CommandCodes.GET_PARTITION_ROUTE_CODE
            ? Reply(request.Operation, [], VsrError.FEATURE_UNAVAILABLE) : null;
        var generation = ((ISessionGenerationProvider)client).SessionGeneration;
        var pending = client.PollMessagesAsync(Stream, Topic, 0, Consumer.New(3), PollingStrategy.Offset(0), 10,
            false, TestContext.Current.CancellationToken);
        try
        {
            await refreshing.Task.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
            await client.LogoutUserAsync(TestContext.Current.CancellationToken);
            await client.LoginUserAsync("other", "secret", TestContext.Current.CancellationToken);
            Assert.NotEqual(generation, ((ISessionGenerationProvider)client).SessionGeneration);
            release.TrySetResult();
            var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => pending);
            Assert.Equal(VsrError.UNAUTHENTICATED, error.StatusCode);
            Assert.False(error.FromServer);
            Assert.Equal(1, cluster.Coordinator.Requests(CommandCodes.GET_PARTITION_ROUTE_CODE));
            Assert.Equal(2, cluster.Coordinator.Registrations);
            Assert.Equal(2, cluster.Primaries[0].Requests(CommandCodes.POLL_MESSAGES_CODE));
            var registration = cluster.CoordinatorRequests.First(request => request.Operation == OPERATION_REGISTER);
            Assert.All(cluster.PrimaryRequests.Where(request => request.Code == CommandCodes.POLL_MESSAGES_CODE),
                request => Assert.Equal(registration.ClientId, request.ClientId));
        }
        finally
        {
            release.TrySetResult();
        }
    }

    [Theory]
    [InlineData(VsrError.LIFECYCLE_BUSY)]
    [InlineData(VsrError.HISTORY_UNAVAILABLE)]
    public async Task given_a_refused_send_when_relogin_precedes_route_refresh_should_not_send_in_the_new_session(
        int status)
    {
        using var cluster = new PollCluster(primaryCount: 1);
        var sends = new ConcurrentQueue<MockRequest>();
        cluster.OnPrimarySend = request =>
        {
            sends.Enqueue(request);
            return sends.Count == 1
                ? Reply(request.Operation, [], (uint)status)
                : Reply(request.Operation, new byte[4]);
        };
        using var client = await cluster.ConnectAsync();
        var generation = ((ISessionGenerationProvider)client).SessionGeneration;
        var observed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var operation = new AsyncLocal<bool>();
        var intercepted = 0;
        void HoldRefusal(object? sender, FirstChanceExceptionEventArgs args)
        {
            if (operation.Value && args.Exception is IggyInvalidStatusCodeException
                { FromServer: true, CommandCode: CommandCodes.SEND_MESSAGES_CODE } error
                && error.StatusCode == status && Interlocked.Exchange(ref intercepted, 1) == 0)
            {
                observed.SetResult();
                release.Task.GetAwaiter().GetResult();
            }
        }

        // Relogin must follow decoding, otherwise closing the socket replaces the refusal with a transport failure.
        AppDomain.CurrentDomain.FirstChanceException += HoldRefusal;
        try
        {
            var pending = Task.Run(async () =>
            {
                operation.Value = true;
                await client.SendMessagesAsync(Stream, Topic, Partitioning.PartitionId(0),
                    new Message(Guid.NewGuid(), "payload"u8.ToArray()), TestContext.Current.CancellationToken);
            }, TestContext.Current.CancellationToken);
            await observed.Task.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
            await client.LogoutUserAsync(TestContext.Current.CancellationToken);
            await client.LoginUserAsync("other", "secret", TestContext.Current.CancellationToken);
            Assert.NotEqual(generation, ((ISessionGenerationProvider)client).SessionGeneration);
            await client.PollMessagesAsync(Stream, Topic, 0, Consumer.New(3), PollingStrategy.Offset(0), 10,
                false, TestContext.Current.CancellationToken);
            var session = SessionOf(client);
            var counter = session.RequestCounter;
            var routes = cluster.RouteRequests.Length;
            release.SetResult();

            var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => pending);
            Assert.Equal(VsrError.UNAUTHENTICATED, error.StatusCode);
            Assert.False(error.FromServer);
            Assert.Single(sends);
            Assert.Equal(routes, cluster.RouteRequests.Length);
            Assert.Equal(counter, session.RequestCounter);
            Assert.Equal(2, cluster.Coordinator.Registrations);

            await client.SendMessagesAsync(Stream, Topic, Partitioning.PartitionId(0),
                new Message(Guid.NewGuid(), "fresh"u8.ToArray()), TestContext.Current.CancellationToken);
            var writes = sends.ToArray();
            Assert.Equal(2, writes.Length);
            Assert.NotEqual(writes[0].ClientId, writes[1].ClientId);
            var registrations = cluster.CoordinatorRequests.Where(request => request.Operation == OPERATION_REGISTER)
                .ToArray();
            Assert.Equal(registrations[0].ClientId, writes[0].ClientId);
            Assert.Equal(registrations[1].ClientId, writes[1].ClientId);
        }
        finally
        {
            AppDomain.CurrentDomain.FirstChanceException -= HoldRefusal;
            release.TrySetResult();
        }
    }

    [Fact]
    public async Task given_a_balanced_send_when_relogin_completes_before_its_partition_resolves_should_not_send_it()
    {
        using var cluster = new PollCluster(primaryCount: 1);
        var refuseRoutes = true;
        cluster.OnCoordinator = RefuseRoute;
        cluster.OnPrimaryControl = RefuseRoute;
        var sends = new ConcurrentQueue<MockRequest>();
        cluster.OnPrimarySend = request =>
        {
            sends.Enqueue(request);
            return Reply(request.Operation, new byte[4]);
        };
        using var client = await cluster.ConnectAsync();

        var pending = client.SendMessagesAsync(Stream, Topic, Partitioning.None(),
            new Message(Guid.NewGuid(), "payload"u8.ToArray()), TestContext.Current.CancellationToken);
        await cluster.UntilAsync(() => cluster.RouteRequests.Length > 0);
        await client.LogoutUserAsync(TestContext.Current.CancellationToken);
        await client.LoginUserAsync("other", "secret", TestContext.Current.CancellationToken);
        Volatile.Write(ref refuseRoutes, false);

        var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => pending);
        Assert.Equal(VsrError.UNAUTHENTICATED, error.StatusCode);
        Assert.False(error.FromServer);
        Assert.Empty(sends);

        byte[]? RefuseRoute(MockRequest request) =>
            request.Code == CommandCodes.GET_PARTITION_ROUTE_CODE && Volatile.Read(ref refuseRoutes)
                ? Reply(request.Operation, [], VsrError.TRANSIENT_NOT_ACCEPTED) : null;
    }

    [Fact]
    public async Task given_a_balanced_send_when_relogin_completes_while_its_messages_encode_should_not_send_it()
    {
        using var cluster = new PollCluster(primaryCount: 1);
        var sends = new ConcurrentQueue<MockRequest>();
        cluster.OnPrimarySend = request =>
        {
            sends.Enqueue(request);
            return Reply(request.Operation, new byte[4]);
        };
        using var encryptor = new HeldEncryptor(TestContext.Current.CancellationToken);
        using var client = await cluster.ConnectAsync(encryptor: encryptor);
        try
        {
            // The partition resolves first, so the send then holds a pool thread while its messages encode.
            var pending = Task.Run(() => client.SendMessagesAsync(Stream, Topic, Partitioning.None(),
                new Message(Guid.NewGuid(), "payload"u8.ToArray()), TestContext.Current.CancellationToken));
            await encryptor.Entered.Task.WaitAsync(TestContext.Current.CancellationToken);
            await client.LogoutUserAsync(TestContext.Current.CancellationToken);
            await client.LoginUserAsync("other", "secret", TestContext.Current.CancellationToken);
            encryptor.Release();

            var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => pending);
            Assert.Equal(VsrError.UNAUTHENTICATED, error.StatusCode);
            Assert.False(error.FromServer);
            Assert.Empty(sends);
        }
        finally
        {
            encryptor.Release();
        }
    }

    [Fact]
    public async Task given_a_cached_poll_waiting_for_a_replica_when_relogin_completes_should_reject_the_old_session()
    {
        using var cluster = new PollCluster(primaryCount: 1);
        using var replacement = new MockNode();
        var binding = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseBinding = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var continuation = new PausedContinuationContext();
        var bindings = 0;
        replacement.Serve(request =>
        {
            if (request.Code == CommandCodes.BIND_SESSION_CODE)
            {
                if (Interlocked.Increment(ref bindings) == 1)
                {
                    binding.TrySetResult();
                    releaseBinding.Task.GetAwaiter().GetResult();
                }
                return BindingReply(request);
            }
            return request.Code == CommandCodes.POLL_MESSAGES_CODE ? Batch(request) : Answer(request);
        });
        using var client = await cluster.ConnectAsync();
        Task Read() => client.PollMessagesAsync(Stream, Topic, 0, Consumer.New(3), PollingStrategy.Offset(0),
            10, false, TestContext.Current.CancellationToken);
        await Read();
        cluster.OnCoordinator = request =>
        {
            if (request.Code != CommandCodes.GET_CLUSTER_METADATA_CODE)
            {
                return null;
            }
            var roster = new List<byte>();
            WriteString(roster, "moved");
            roster.AddRange(BitConverter.GetBytes(2u));
            WriteNode(roster, cluster.Coordinator.Port, true);
            WriteNode(roster, replacement.Port, false, 1);
            return Reply(request.Operation, roster.ToArray());
        };
        try
        {
            await client.GetClusterMetadataAsync(TestContext.Current.CancellationToken);
            await binding.Task.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
            var session = SessionOf(client);
            var generation = session.Generation;
            var counter = session.RequestCounter;
            var previousContext = SynchronizationContext.Current;
            Task pending;
            try
            {
                SynchronizationContext.SetSynchronizationContext(continuation);
                pending = Read();
            }
            finally
            {
                SynchronizationContext.SetSynchronizationContext(previousContext);
            }
            Assert.False(pending.IsCompleted);
            Assert.Single(cluster.RouteRequests);
            Assert.Equal(counter + 1, session.RequestCounter);

            await client.LogoutUserAsync(TestContext.Current.CancellationToken);
            await continuation.Posted.Task.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
            await client.LoginUserAsync("other", "secret", TestContext.Current.CancellationToken);
            Assert.NotEqual(generation, session.Generation);
            await Read();
            Assert.Equal(1, replacement.Requests(CommandCodes.POLL_MESSAGES_CODE));
            continuation.Release.TrySetResult();

            var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => pending);
            Assert.Equal(VsrError.UNAUTHENTICATED, error.StatusCode);
            Assert.False(error.FromServer);
            Assert.Equal(1, replacement.Requests(CommandCodes.POLL_MESSAGES_CODE));
        }
        finally
        {
            continuation.Release.TrySetResult();
            releaseBinding.TrySetResult();
        }
    }

    [Fact]
    public async Task given_new_coordinator_session_when_polling_should_retire_old_data_connections()
    {
        using var cluster = new PollCluster();
        using var client = await cluster.ConnectAsync();
        await PollAsync(client);
        var oldParent = BinaryPrimitives.ReadUInt128LittleEndian(BindIdentity(cluster.Attachments.First().Body));
        await client.LogoutUserAsync(TestContext.Current.CancellationToken);
        await client.LoginUserAsync("other", "secret", TestContext.Current.CancellationToken);
        await PollAsync(client);

        Assert.Equal(2, cluster.Primaries[0].Connections);
        Assert.NotEqual(oldParent, BinaryPrimitives.ReadUInt128LittleEndian(BindIdentity(cluster.Attachments.Last().Body)));
    }

    [Fact]
    public async Task given_cluster_manual_commit_poll_when_polling_should_poll_the_partition_primary()
    {
        using var cluster = new PollCluster();
        var polls = new ConcurrentQueue<MockRequest>();
        cluster.OnPrimaryPoll = request =>
        {
            polls.Enqueue(request);
            return Batch(request);
        };
        using var client = await cluster.ConnectAsync();
        await client.PollMessagesAsync(Stream, Topic, 0, Consumer.Group(3), PollingStrategy.Next(), 10, false,
            TestContext.Current.CancellationToken);

        Assert.Equal(0, cluster.Coordinator.Requests(CommandCodes.POLL_MESSAGES_CODE));
        Assert.Single(cluster.RouteRequests);
        Assert.Equal(0, Assert.Single(polls).Body[^1]);
    }

    [Fact]
    public async Task given_a_pure_poll_when_the_primary_connection_drops_should_retry_the_same_read()
    {
        using var cluster = new PollCluster();
        var polls = 0;
        cluster.OnPrimaryPoll = request => Interlocked.Increment(ref polls) == 1 ? [] : Batch(request);
        using var client = await cluster.ConnectAsync();
        await client.PollMessagesAsync(Stream, Topic, 0, Consumer.New(3), PollingStrategy.Offset(0), 10, false,
            TestContext.Current.CancellationToken);

        Assert.Equal(2, Volatile.Read(ref polls));
        Assert.Equal(2, cluster.Primaries[0].Connections);
    }

    [Fact]
    public async Task given_cluster_offset_writes_when_storing_and_deleting_should_write_on_the_partition_primary()
    {
        using var cluster = new PollCluster { RouteContext = new PartitionContext(17, 0, 52) };
        using var client = await cluster.ConnectAsync();
        await client.StoreOffsetAsync(Consumer.New(3), Stream, Topic, 10, 1, TestContext.Current.CancellationToken);
        await client.DeleteOffsetAsync(Consumer.New(3), Stream, Topic, 1, TestContext.Current.CancellationToken);

        Assert.DoesNotContain(cluster.CoordinatorRequests, IsOffsetWrite);
        Assert.Single(cluster.RouteRequests);
        Assert.Equal([(byte)VsrOperation.StoreConsumerOffset, (byte)VsrOperation.DeleteConsumerOffset],
            cluster.OffsetWrites.Select(write => write.Operation));
        Assert.All(cluster.OffsetWrites, write => Assert.Equal(cluster.RouteContext, write.Context));
    }

    /// <summary>
    ///     The partition dedups replicated requests by client id, session and request id, so an offset write on a
    ///     data connection has to take its id from the counter of the session it is bound to.
    /// </summary>
    [Fact]
    public async Task given_a_metadata_write_when_storing_an_offset_on_the_primary_should_take_a_later_shared_request_id()
    {
        using var cluster = new PollCluster();
        using var client = await cluster.ConnectAsync();
        await client.UpdateStreamAsync(Stream, "renamed", TestContext.Current.CancellationToken);
        await client.StoreOffsetAsync(Consumer.New(3), Stream, Topic, 10, 1, TestContext.Current.CancellationToken);

        var update = Assert.Single(cluster.CoordinatorRequests,
            request => request.Operation == (byte)VsrOperation.UpdateStream);
        Assert.True(Assert.Single(cluster.OffsetWrites).RequestId > update.RequestId);
    }

    [Fact]
    public async Task given_a_dropped_offset_reply_when_resolving_should_reuse_the_retained_request_id()
    {
        using var cluster = new PollCluster();
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cluster.OnPrimaryOffsetWrite = request =>
        {
            if (cluster.OffsetWrites.Count == 1)
            {
                cancellation.Cancel();
                return [];
            }
            return Reply(request.Operation, new byte[4]);
        };
        using var client = await cluster.ConnectAsync();
        var error = await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() => client.StoreOffsetAsync(
            Consumer.New(3), Stream, Topic, 10, 1, cancellation.Token));
        var operationId = Assert.IsType<ulong>(error.OperationId);
        await Assert.ThrowsAsync<VsrLaneBlockedException>(() => client.StoreOffsetAsync(Consumer.New(3), Stream,
            Topic, 10, 1, TestContext.Current.CancellationToken));
        await client.ResolveOperationAsync(operationId, TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.Equal(2, cluster.Primaries[1].Connections);
        Assert.Equal([operationId, operationId], cluster.OffsetWrites.Select(write => write.RequestId));
    }

    [Fact]
    public async Task given_a_caller_context_when_a_primary_refuses_should_keep_it_on_the_retry()
    {
        using var cluster = new PollCluster { RouteContext = new PartitionContext(18, 0, 60) };
        var caller = new PartitionContext(17, 0, 52);
        var polls = new ConcurrentQueue<MockRequest>();
        cluster.OnPrimaryPoll = request =>
        {
            polls.Enqueue(request);
            return polls.Count == 1 ? Reply(request.Operation, [], TRANSIENT_NOT_ACCEPTED) : Batch(request);
        };
        using var client = await cluster.ConnectAsync();
        await client.PollMessagesAsync(Stream, Topic, 0, Consumer.New(3), PollingStrategy.Next().WithContext(caller),
            10, false, TestContext.Current.CancellationToken);

        Assert.Equal([caller, caller], polls.Select(poll => poll.Context));
    }

    [Fact]
    public async Task given_no_caller_context_when_a_primary_refuses_should_keep_the_first_route_context()
    {
        using var cluster = new PollCluster { RouteContext = new PartitionContext(17, 0, 52) };
        var first = cluster.RouteContext;
        var polls = new ConcurrentQueue<MockRequest>();
        cluster.OnPrimaryPoll = request =>
        {
            polls.Enqueue(request);
            cluster.RouteContext = new PartitionContext(18, 0, 60);
            return polls.Count == 1 ? Reply(request.Operation, [], TRANSIENT_NOT_ACCEPTED) : Batch(request);
        };
        using var client = await cluster.ConnectAsync();
        await client.PollMessagesAsync(Stream, Topic, 0, Consumer.New(3), PollingStrategy.Next(), 10, false,
            TestContext.Current.CancellationToken);

        Assert.Equal(2, cluster.RouteRequests.Length);
        Assert.Equal([first, first], polls.Select(poll => poll.Context));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_a_history_refusal_when_polling_should_report_it_and_route_the_next_poll_again(
        bool autoCommit)
    {
        using var cluster = new PollCluster { RouteContext = new PartitionContext(17, 0, 52) };
        var replaced = new PartitionContext(18, 0, 60);
        var polls = new ConcurrentQueue<MockRequest>();
        cluster.OnPrimaryPoll = request =>
        {
            polls.Enqueue(request);
            cluster.RouteContext = replaced;
            return polls.Count == 1 ? Reply(request.Operation, [], VsrError.HISTORY_UNAVAILABLE) : Batch(request);
        };
        using var client = await cluster.ConnectAsync();

        var refusal = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => client.PollMessagesAsync(Stream,
            Topic, 0, Consumer.New(3), PollingStrategy.Next(), 10, autoCommit, TestContext.Current.CancellationToken));
        Assert.Equal(VsrError.HISTORY_UNAVAILABLE, refusal.StatusCode);
        Assert.Single(polls);

        await client.PollMessagesAsync(Stream, Topic, 0, Consumer.New(3), PollingStrategy.Next(), 10, autoCommit,
            TestContext.Current.CancellationToken);

        Assert.Equal(2, cluster.RouteRequests.Length);
        Assert.Equal(replaced, polls.Last().Context);
    }

    [Fact]
    public async Task given_a_history_refusal_when_storing_an_offset_should_report_it_and_route_the_next_write_again()
    {
        using var cluster = new PollCluster { RouteContext = new PartitionContext(17, 0, 52) };
        var replaced = new PartitionContext(18, 0, 60);
        cluster.OnPrimaryOffsetWrite = request =>
        {
            cluster.RouteContext = replaced;
            return cluster.OffsetWrites.Count == 1
                ? Reply(request.Operation, [], VsrError.HISTORY_UNAVAILABLE)
                : Reply(request.Operation, new byte[4]);
        };
        using var client = await cluster.ConnectAsync();

        var refusal = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => client.StoreOffsetAsync(
            Consumer.New(3), Stream, Topic, 10, 1, TestContext.Current.CancellationToken));
        Assert.Equal(VsrError.HISTORY_UNAVAILABLE, refusal.StatusCode);
        Assert.Single(cluster.OffsetWrites);

        await client.StoreOffsetAsync(Consumer.New(3), Stream, Topic, 10, 1, TestContext.Current.CancellationToken);

        Assert.Equal(2, cluster.RouteRequests.Length);
        Assert.Equal(replaced, cluster.OffsetWrites.Last().Context);
    }

    [Fact]
    public async Task given_shuffled_replica_ids_when_accessing_a_partition_should_share_its_route_and_session()
    {
        using var cluster = new PollCluster { RouteContext = new PartitionContext(17, 0, 52) };
        cluster.OnCoordinator = request =>
        {
            if (request.Code == CommandCodes.GET_CLUSTER_METADATA_CODE)
            {
                var roster = new List<byte>();
                WriteString(roster, "shuffled");
                roster.AddRange(BitConverter.GetBytes(3u));
                WriteNode(roster, cluster.Primaries[1].Port, false, 99);
                WriteNode(roster, cluster.Coordinator.Port, true, 42);
                WriteNode(roster, cluster.Primaries[0].Port, false, 7);
                return Reply(request.Operation, roster.ToArray());
            }
            return request.Code == CommandCodes.GET_PARTITION_ROUTE_CODE
                ? RouteReply(request, cluster.RouteContext, 99, 2)
                : null;
        };
        using var client = await cluster.ConnectAsync();
        var token = TestContext.Current.CancellationToken;
        await client.SendMessagesAsync(Stream, Topic, Partitioning.PartitionId(0),
            new Message(Guid.NewGuid(), "payload"u8.ToArray()), token);
        await client.PollMessagesAsync(Stream, Topic, null, Consumer.New(3), PollingStrategy.Next(), 10, false, token);
        await client.StoreOffsetAsync(Consumer.New(3), Stream, Topic, 10, 0, token);
        await client.GetOffsetAsync(Consumer.New(3), Stream, Topic, null, token);
        await client.DeleteOffsetAsync(Consumer.New(3), Stream, Topic, 0, token);

        var requests = cluster.PrimaryRequests.Where(request => request.Code != CommandCodes.BIND_SESSION_CODE).ToArray();
        Assert.Equal(5, requests.Length);
        Assert.All(requests, request => Assert.Equal(cluster.RouteContext, request.Context));
        Assert.Single(cluster.RouteRequests);
        Assert.Equal(1, cluster.Coordinator.Connections);
        await cluster.UntilAsync(() => cluster.Attachments.Count == 2);
        Assert.All(cluster.Primaries, primary => Assert.Equal(1, primary.Connections));
        Assert.Equal(0, cluster.Primaries[0].Requests(CommandCodes.POLL_MESSAGES_CODE));
        Assert.Equal(0, cluster.Primaries[0].Requests(CommandCodes.SEND_MESSAGES_CODE));
        var writes = requests.Where(request => request.Operation != OPERATION_NON_REPLICATED).ToArray();
        Assert.Equal(3, writes.Length);
        Assert.Equal(requests.Length, requests.Select(request => request.RequestId).Distinct().Count());
        Assert.True(writes[0].RequestId < writes[1].RequestId && writes[1].RequestId < writes[2].RequestId);
        var register = Assert.Single(cluster.CoordinatorRequests, request => request.Operation == OPERATION_REGISTER);
        Assert.All(requests, request => Assert.Equal(register.ClientId, request.ClientId));
        Assert.All(cluster.Primaries, primary => Assert.Equal(0, primary.Registrations));
    }

    [Theory]
    [InlineData(1, VsrError.NOT_PRIMARY)]
    [InlineData(3, VsrError.NOT_PRIMARY)]
    [InlineData(3, VsrError.TRANSIENT_NOT_ACCEPTED)]
    public async Task given_a_new_primary_when_a_write_is_refused_should_retry_its_identity_and_context_on_the_route(
        int refusals, int status)
    {
        using var cluster = new PollCluster();
        byte primary = 1;
        cluster.OnCoordinator = request => request.Code == CommandCodes.GET_PARTITION_ROUTE_CODE
            ? RouteReply(request, cluster.RouteContext, primary, 2, view: (uint)cluster.OffsetWrites.Count)
            : null;
        cluster.OnPrimaryControl = cluster.OnCoordinator;
        cluster.OnPrimaryOffsetWrite = request =>
        {
            primary = 2;
            cluster.RouteContext = new PartitionContext(18, 0, 60);
            return cluster.OffsetWrites.Count <= refusals
                ? Reply(request.Operation, [], (uint)status)
                : Reply(request.Operation, new byte[4]);
        };
        using var client = await cluster.ConnectAsync();
        await client.StoreOffsetAsync(Consumer.New(3), Stream, Topic, 10, 0, TestContext.Current.CancellationToken);

        var writes = cluster.OffsetWrites.ToArray();
        Assert.Equal(refusals + 1, writes.Length);
        Assert.All(writes, retry =>
        {
            Assert.Equal(writes[0].RequestId, retry.RequestId);
            Assert.Equal(writes[0].Context, retry.Context);
            Assert.Equal(writes[0].ClientId, retry.ClientId);
            Assert.Equal(writes[0].Session, retry.Session);
            Assert.Equal(writes[0].Body, retry.Body);
        });
        Assert.Equal(refusals + 1, cluster.RouteRequests.Length);
        Assert.Equal(1, cluster.Coordinator.Connections);
        Assert.All(cluster.Primaries, node => Assert.Equal(1, node.Connections));
    }

    [Theory]
    [InlineData(VsrError.TRANSIENT_NOT_ACCEPTED)]
    [InlineData(VsrError.NOT_PRIMARY)]
    [InlineData(VsrError.LIFECYCLE_BUSY)]
    [InlineData(VsrError.FEATURE_UNAVAILABLE)]
    public async Task given_an_uncertain_write_when_a_later_refusal_arrives_should_keep_its_retained_identity(int refusal)
    {
        using var cluster = new PollCluster();
        cluster.OnPrimaryOffsetWrite = request => cluster.OffsetWrites.Count switch
        {
            1 => Reply(request.Operation, [], VsrError.TRANSIENT_NOT_COMMITTED),
            2 => Reply(request.Operation, [], (uint)refusal),
            _ => []
        };
        using var client = await cluster.ConnectAsync();

        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cancellation.CancelAfter(TimeSpan.FromMilliseconds(500));
        var error = await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() => client.StoreOffsetAsync(Consumer.New(3),
            Stream, Topic, 10, 0, cancellation.Token));
        Assert.IsType<ulong>(error.OperationId);
        Assert.True(cluster.OffsetWrites.Count >= 2);
        Assert.Single(cluster.OffsetWrites.Select(request => request.RequestId).Distinct());
        var original = cluster.OffsetWrites.First();
        Assert.All(cluster.OffsetWrites, request =>
        {
            Assert.Equal(original.Context, request.Context);
            Assert.Equal(original.Body, request.Body);
        });
        await Assert.ThrowsAsync<VsrLaneBlockedException>(() => client.StoreOffsetAsync(Consumer.New(3),
            Stream, Topic, 10, 0, TestContext.Current.CancellationToken));
        Assert.Equal(0, cluster.Primaries[1].Requests(CommandCodes.STORE_CONSUMER_OFFSET_CODE));
    }

    [Theory]
    [InlineData(CommandCodes.GET_PARTITION_ROUTE_CODE)]
    [InlineData(CommandCodes.BIND_SESSION_CODE)]
    public async Task given_a_busy_control_reply_when_routing_should_surface_it_without_retrying(int controlCode)
    {
        using var cluster = new PollCluster();
        cluster.OnCoordinator = request => request.Code == controlCode
            ? Reply(request.Operation, [], VsrError.LIFECYCLE_BUSY)
            : null;
        cluster.OnPrimaryAttachment = request => controlCode == CommandCodes.BIND_SESSION_CODE
            ? Reply(request.Operation, [], VsrError.LIFECYCLE_BUSY)
            : BindingReply(request);
        using var client = await cluster.ConnectAsync();
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cancellation.CancelAfter(TimeSpan.FromSeconds(2));

        var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => client.StoreOffsetAsync(
            Consumer.New(3), Stream, Topic, 10, 0, cancellation.Token));

        Assert.Equal(VsrError.LIFECYCLE_BUSY, error.StatusCode);
        Assert.Single(cluster.RouteRequests);
        Assert.All(cluster.Primaries, primary => Assert.InRange(primary.Requests(CommandCodes.BIND_SESSION_CODE), 0, 1));
        Assert.Empty(cluster.OffsetWrites);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_a_busy_partition_when_retrying_should_back_off_and_allocate_new_request_ids(bool send)
    {
        using var cluster = new PollCluster();
        var timestamps = new ConcurrentQueue<long>();
        var requests = new ConcurrentQueue<MockRequest>();
        byte[] RefuseWhileBusy(MockRequest request)
        {
            requests.Enqueue(request);
            timestamps.Enqueue(Environment.TickCount64);
            return requests.Count < 3
                ? Reply(request.Operation, [], VsrError.LIFECYCLE_BUSY)
                : Reply(request.Operation, new byte[4]);
        }
        cluster.OnPrimaryOffsetWrite = RefuseWhileBusy;
        cluster.OnPrimarySend = RefuseWhileBusy;
        using var client = await cluster.ConnectAsync();
        if (send)
        {
            await client.SendMessagesAsync(Stream, Topic, Partitioning.PartitionId(0),
                new Message(Guid.NewGuid(), "payload"u8.ToArray()), TestContext.Current.CancellationToken);
        }
        else
        {
            await client.StoreOffsetAsync(Consumer.New(3), Stream, Topic, 10, 0, TestContext.Current.CancellationToken);
        }

        var writes = requests.ToArray();
        Assert.Equal(3, writes.Length);
        Assert.True(writes[0].RequestId < writes[1].RequestId && writes[1].RequestId < writes[2].RequestId);
        Assert.All(writes, request => Assert.Equal(writes[0].Context, request.Context));
        var sentAt = timestamps.ToArray();
        Assert.True(sentAt[1] - sentAt[0] >= 45);
        Assert.True(sentAt[2] - sentAt[1] >= 90);
        Assert.All(cluster.Primaries, primary => Assert.InRange(primary.Requests(CommandCodes.BIND_SESSION_CODE), 0, 1));
        Assert.Single(cluster.RouteRequests);
    }

    [Fact]
    public async Task given_a_lost_coordinator_when_discovering_a_route_should_use_an_already_ready_replica()
    {
        using var cluster = new PollCluster();
        cluster.OnCoordinator = request =>
        {
            if (request.Code != CommandCodes.GET_PARTITION_ROUTE_CODE)
            {
                return null;
            }
            cluster.Coordinator.Kill();
            return [];
        };
        using var client = await cluster.ConnectAsync();

        await PollAsync(client);
        Assert.Equal(1, cluster.Primaries[0].Requests(CommandCodes.POLL_MESSAGES_CODE));
        Assert.All(cluster.Primaries, node => Assert.Equal(0, node.Registrations));
    }

    [Theory]
    [InlineData(VsrError.STREAM_ID_NOT_FOUND, false)]
    [InlineData(VsrError.STREAM_NAME_NOT_FOUND, false)]
    [InlineData(VsrError.TOPIC_ID_NOT_FOUND, false)]
    [InlineData(VsrError.TOPIC_NAME_NOT_FOUND, false)]
    [InlineData(VsrError.PARTITION_NOT_FOUND, false)]
    [InlineData(VsrError.STREAM_ID_NOT_FOUND, true)]
    [InlineData(VsrError.STREAM_NAME_NOT_FOUND, true)]
    [InlineData(VsrError.TOPIC_ID_NOT_FOUND, true)]
    [InlineData(VsrError.TOPIC_NAME_NOT_FOUND, true)]
    [InlineData(VsrError.PARTITION_NOT_FOUND, true)]
    public async Task given_a_backup_route_lookup_miss_when_metadata_is_materializing_should_continue_discovery(
        int status, bool remainingReplica)
    {
        using var cluster = new PollCluster(primaryCount: remainingReplica ? 2 : 1);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cancellation.CancelAfter(TimeSpan.FromSeconds(5));
        var metadataLookups = 0;
        var backupLookups = 0;
        var resolved = 0;
        cluster.OnCoordinator = request =>
        {
            if (request.Code != CommandCodes.GET_PARTITION_ROUTE_CODE)
            {
                return null;
            }
            if (Interlocked.Increment(ref metadataLookups) == 1 || remainingReplica)
            {
                return Reply(request.Operation, [], VsrError.TRANSIENT_NOT_ACCEPTED);
            }
            Volatile.Write(ref resolved, 1);
            return RouteReply(request, cluster.RouteContext, 1, (uint)cluster.Primaries.Length);
        };
        cluster.OnPrimaryControl = request =>
        {
            if (request.Code != CommandCodes.GET_PARTITION_ROUTE_CODE)
            {
                return null;
            }
            if (Interlocked.Increment(ref backupLookups) == 1 || !remainingReplica)
            {
                return Reply(request.Operation, [], (uint)status);
            }
            Volatile.Write(ref resolved, 1);
            return RouteReply(request, cluster.RouteContext, 1, (uint)cluster.Primaries.Length);
        };
        cluster.OnPrimarySend = request =>
        {
            Assert.Equal(1, Volatile.Read(ref resolved));
            return Reply(request.Operation, new byte[4]);
        };
        using var client = await cluster.ConnectAsync();
        for (byte replica = 1; replica <= cluster.Primaries.Length; replica++)
        {
            var connection = await (Task<VsrConnection?>)typeof(TcpMessageStream)
                .GetMethod("FindReadyReplicaAsync", BindingFlags.Instance | BindingFlags.NonPublic)!
                .Invoke(client, [replica, cancellation.Token])!;
            Assert.NotNull(connection);
        }

        await client.SendMessagesAsync(Stream, Topic, Partitioning.PartitionId(0),
            new Message(Guid.NewGuid(), "payload"u8.ToArray()), cancellation.Token);

        Assert.False(cancellation.IsCancellationRequested);
        Assert.Equal(remainingReplica ? 1 : 2, metadataLookups);
        Assert.Equal(remainingReplica ? 2 : 1, backupLookups);
        var routes = cluster.RouteRequests;
        Assert.Equal(3, routes.Length);
        var send = Assert.Single(cluster.PrimaryRequests, request => request.Operation == (byte)VsrOperation.SendMessages);
        Assert.Equal(cluster.RouteContext, send.Context);
        Assert.All(routes, route => Assert.True(route.RequestId < send.RequestId));
    }

    [Theory]
    [InlineData(VsrError.STREAM_ID_NOT_FOUND)]
    [InlineData(VsrError.STREAM_NAME_NOT_FOUND)]
    [InlineData(VsrError.TOPIC_ID_NOT_FOUND)]
    [InlineData(VsrError.TOPIC_NAME_NOT_FOUND)]
    [InlineData(VsrError.PARTITION_NOT_FOUND)]
    public async Task given_a_metadata_primary_route_lookup_miss_when_sending_should_surface_it_without_retrying(int status)
    {
        using var cluster = new PollCluster();
        cluster.OnCoordinator = request => request.Code == CommandCodes.GET_PARTITION_ROUTE_CODE
            ? Reply(request.Operation, [], (uint)status) : null;
        using var client = await cluster.ConnectAsync();
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cancellation.CancelAfter(TimeSpan.FromSeconds(5));

        var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => client.SendMessagesAsync(
            Stream, Topic, Partitioning.PartitionId(0), new Message(Guid.NewGuid(), "payload"u8.ToArray()),
            cancellation.Token));

        Assert.Equal(status, error.StatusCode);
        Assert.True(error.FromServer);
        Assert.False(cancellation.IsCancellationRequested);
        Assert.Single(cluster.RouteRequests);
        Assert.DoesNotContain(cluster.PrimaryRequests, request => request.Operation == (byte)VsrOperation.SendMessages);
    }

    [Fact]
    public async Task given_a_malformed_write_reply_when_using_a_primary_should_report_an_unknown_outcome()
    {
        using var cluster = new PollCluster();
        cluster.OnPrimaryOffsetWrite = _ => Reply(byte.MaxValue, []);
        using var client = await cluster.ConnectAsync();

        await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() => client.StoreOffsetAsync(Consumer.New(3),
            Stream, Topic, 10, 0, TestContext.Current.CancellationToken));

        Assert.Single(cluster.OffsetWrites);
        Assert.Single(cluster.RouteRequests);
    }

    [Fact]
    public async Task given_a_coordinator_primary_refusal_when_retrying_should_recheck_without_walking_the_roster()
    {
        const uint command = 60_042;
        using var cluster = new PollCluster();
        var attempts = 0;
        cluster.OnCoordinator = request => request.Code == command && ++attempts == 1
            ? Reply(request.Operation, [], VsrError.NOT_PRIMARY)
            : null;
        using var client = await cluster.ConnectAsync();
        var reads = cluster.Coordinator.Requests(CommandCodes.GET_CLUSTER_METADATA_CODE);
        await client.SendBinaryRequestAsync(command, [], TestContext.Current.CancellationToken);

        Assert.Equal(2, attempts);
        Assert.Equal(reads + 1, cluster.Coordinator.Requests(CommandCodes.GET_CLUSTER_METADATA_CODE));
        Assert.Equal(1, cluster.Coordinator.Connections);
        Assert.All(cluster.Primaries, node => Assert.Equal(0, node.Requests((int)command)));
    }

    [Fact]
    public async Task given_an_unknown_primary_replica_when_routing_should_refresh_the_roster_once()
    {
        using var cluster = new PollCluster();
        var advertisePrimary = false;
        cluster.OnCoordinator = request =>
        {
            if (request.Code == CommandCodes.GET_PARTITION_ROUTE_CODE)
            {
                advertisePrimary = true;
                return RouteReply(request, cluster.RouteContext, 99, 2);
            }
            if (request.Code != CommandCodes.GET_CLUSTER_METADATA_CODE || !advertisePrimary)
            {
                return null;
            }
            var roster = new List<byte>();
            WriteString(roster, "refreshed");
            roster.AddRange(BitConverter.GetBytes(2u));
            WriteNode(roster, cluster.Coordinator.Port, true);
            WriteNode(roster, cluster.Primaries[1].Port, false, 99);
            return Reply(request.Operation, roster.ToArray());
        };
        using var client = await cluster.ConnectAsync();
        var reads = cluster.Coordinator.Requests(CommandCodes.GET_CLUSTER_METADATA_CODE);
        await PollAsync(client);

        Assert.Equal(reads + 1, cluster.Coordinator.Requests(CommandCodes.GET_CLUSTER_METADATA_CODE));
        Assert.Single(cluster.RouteRequests);
        Assert.Equal(0, cluster.Primaries[0].Requests(CommandCodes.POLL_MESSAGES_CODE));
        Assert.Equal(1, cluster.Primaries[1].Requests(CommandCodes.POLL_MESSAGES_CODE));
    }

    [Fact]
    public async Task given_partition_growth_when_balancing_should_use_the_refreshed_route_count()
    {
        using var cluster = new PollCluster(3);
        uint count = 1;
        var refuseFirstRoute = true;
        cluster.OnCoordinator = request =>
        {
            if (request.Code == CommandCodes.GET_PARTITION_ROUTE_CODE)
            {
                Assert.Equal(TcpContracts.GetTopicById(Stream, Topic), request.Body[..^sizeof(uint)]);
                if (refuseFirstRoute)
                {
                    refuseFirstRoute = false;
                    return Reply(request.Operation, [], TRANSIENT_NOT_ACCEPTED);
                }
                var partition = BinaryPrimitives.ReadUInt32LittleEndian(request.Body.AsSpan(request.Body.Length - sizeof(uint)));
                return RouteReply(request, cluster.RouteContext, checked((byte)(partition + 1)), count);
            }
            if (request.Operation != (byte)VsrOperation.CreatePartitions)
            {
                return null;
            }
            count = 3;
            var reply = Reply(request.Operation, new byte[4]);
            BinaryPrimitives.WriteUInt64LittleEndian(reply.AsSpan(VsrHeader.REPLY_COMMIT_OFFSET), MetadataCommit);
            return reply;
        };
        using var client = await cluster.ConnectAsync();
        var message = new Message(Guid.NewGuid(), "payload"u8.ToArray());
        var token = TestContext.Current.CancellationToken;
        await client.SendMessagesAsync(Stream, Topic, Partitioning.None(), message, token);
        await client.CreatePartitionsAsync(Stream, Topic, 2, token);
        await client.SendMessagesAsync(Stream, Topic, Partitioning.None(), message, token);
        await client.SendMessagesAsync(Stream, Topic, Partitioning.None(), message, token);

        Assert.All(cluster.Primaries, node => Assert.Equal(1, node.Connections));
        Assert.Equal(0, cluster.Coordinator.Requests(CommandCodes.GET_TOPIC_CODE));
        Assert.Equal(5, cluster.RouteRequests.Length);
    }

    [Fact]
    public async Task given_missing_partition_ids_when_mutating_offsets_or_raw_group_polling_should_refuse_locally()
    {
        using var cluster = new PollCluster();
        using var client = await cluster.ConnectAsync();
        var token = TestContext.Current.CancellationToken;
        var store = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() =>
            client.StoreOffsetAsync(Consumer.New(3), Stream, Topic, 10, null, token));
        var delete = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() =>
            client.DeleteOffsetAsync(Consumer.New(3), Stream, Topic, null, token));
        var body = TcpContracts.GetOffset(Stream, Topic, Consumer.Group(3), null);
        var poll = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() =>
            client.SendBinaryRequestAsync(CommandCodes.POLL_MESSAGES_CODE, body, token));

        Assert.All(new[] { store, delete, poll }, error =>
        {
            Assert.Equal(VsrError.INVALID_COMMAND, error.StatusCode);
            Assert.False(error.FromServer);
        });
        Assert.Empty(cluster.RouteRequests);
        Assert.DoesNotContain(cluster.PrimaryRequests, request => request.Code == CommandCodes.POLL_MESSAGES_CODE || IsOffsetWrite(request));
    }

    private static async Task PollAsync(TcpMessageStream client)
    {
        await client.PollMessagesAsync(Stream, Topic, 0, Consumer.Group(3), PollingStrategy.Next(), 10, true,
            TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task given_a_full_route_cache_when_one_entry_is_added_should_preserve_other_entries()
    {
        const int Capacity = 4096;
        using var cluster = new PollCluster(primaryCount: 1);
        cluster.OnCoordinator = request => request.Code == CommandCodes.GET_PARTITION_ROUTE_CODE
            ? RouteReply(request, cluster.RouteContext, 1, Capacity + 1) : null;
        cluster.OnPrimaryControl = cluster.OnCoordinator;
        using var client = await cluster.ConnectAsync();
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cancellation.CancelAfter(TimeSpan.FromSeconds(15));
        Task Read(uint partition) => client.PollMessagesAsync(Stream, Topic, partition, Consumer.New(3),
            PollingStrategy.Offset(0), 1, false, cancellation.Token);
        for (uint partition = 0; partition <= Capacity; partition++)
        {
            await Read(partition);
        }
        Assert.Equal(Capacity + 1, cluster.RouteRequests.Length);
        await Read(1);
        await Read(Capacity);
        Assert.Equal(Capacity + 1, cluster.RouteRequests.Length);
        await Read(0);
        Assert.Equal(Capacity + 2, cluster.RouteRequests.Length);
        Assert.All(cluster.Primaries, node => Assert.Equal(1, node.Connections));
    }

    [Fact]
    public async Task given_a_queued_metadata_write_when_lifecycle_busy_restarts_should_preserve_fifo_and_frozen_bytes()
    {
        using var cluster = new PollCluster(primaryCount: 0);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cancellation.CancelAfter(TimeSpan.FromSeconds(10));
        var firstReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var writes = new ConcurrentQueue<MockRequest>();
        cluster.OnCoordinator = request =>
        {
            if (request.Operation != (byte)VsrOperation.CreateStream)
            {
                return null;
            }
            writes.Enqueue(request);
            if (writes.Count == 1)
            {
                firstReceived.TrySetResult();
                release.Task.GetAwaiter().GetResult();
                return Reply(request.Operation, [], VsrError.LIFECYCLE_BUSY);
            }
            return Reply(request.Operation, new byte[4]);
        };
        using var client = await cluster.ConnectAsync();
        var firstBody = TcpContracts.CreateStream("first");
        var frozenFirst = firstBody.ToArray();
        var secondBody = TcpContracts.CreateStream("second");
        try
        {
            var first = client.SendBinaryRequestAsync(CommandCodes.CREATE_STREAM_CODE, firstBody, cancellation.Token);
            await firstReceived.Task.WaitAsync(cancellation.Token);
            var second = client.SendBinaryRequestAsync(CommandCodes.CREATE_STREAM_CODE, secondBody, cancellation.Token);
            Assert.False(second.IsCompleted);
            Array.Fill(firstBody, byte.MaxValue);
            release.TrySetResult();
            await Task.WhenAll(first, second);

            var attempts = writes.ToArray();
            Assert.Equal(3, attempts.Length);
            Assert.Equal(frozenFirst, attempts[0].Body);
            Assert.Equal(frozenFirst, attempts[1].Body);
            Assert.Equal(secondBody, attempts[2].Body);
            Assert.True(attempts[0].RequestId < attempts[1].RequestId);
            Assert.True(attempts[1].RequestId < attempts[2].RequestId);
            Assert.Equal(attempts[0].Context, attempts[1].Context);
        }
        finally
        {
            release.TrySetResult();
        }
    }

    [Fact]
    public async Task given_a_queued_send_when_its_old_primary_refuses_after_refresh_should_preserve_the_newer_route()
    {
        using var cluster = new PollCluster();
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cancellation.CancelAfter(TimeSpan.FromSeconds(5));
        var firstSend = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var sends = new ConcurrentQueue<MockRequest>();
        uint view = 0;
        cluster.OnCoordinator = request => request.Code == CommandCodes.GET_PARTITION_ROUTE_CODE
            ? RouteReply(request, cluster.RouteContext, Volatile.Read(ref view) == 0 ? (byte)1 : (byte)2,
                2, view: Volatile.Read(ref view)) : null;
        cluster.OnPrimaryControl = cluster.OnCoordinator;
        cluster.OnPrimarySend = request =>
        {
            sends.Enqueue(request);
            if (sends.Count == 1)
            {
                firstSend.TrySetResult();
                release.Task.GetAwaiter().GetResult();
                Volatile.Write(ref view, 1);
                return Reply(request.Operation, [], VsrError.NOT_PRIMARY);
            }
            return sends.Count == 3
                ? Reply(request.Operation, [], VsrError.NOT_PRIMARY)
                : Reply(request.Operation, new byte[4]);
        };
        using var client = await cluster.ConnectAsync();
        try
        {
            var first = client.SendMessagesAsync(Stream, Topic, Partitioning.PartitionId(0),
                new Message(Guid.NewGuid(), "first"u8.ToArray()), cancellation.Token);
            await firstSend.Task.WaitAsync(cancellation.Token);
            var queued = client.SendMessagesAsync(Stream, Topic, Partitioning.PartitionId(0),
                new Message(Guid.NewGuid(), "queued"u8.ToArray()), cancellation.Token);
            release.TrySetResult();
            await Task.WhenAll(first, queued);

            await client.SendMessagesAsync(Stream, Topic, Partitioning.PartitionId(0),
                new Message(Guid.NewGuid(), "fresh"u8.ToArray()), cancellation.Token);
            var attempts = sends.ToArray();
            Assert.Equal(5, attempts.Length);
            Assert.Equal(attempts[0].RequestId, attempts[1].RequestId);
            Assert.Equal(attempts[0].Body, attempts[1].Body);
            Assert.Equal(attempts[2].RequestId, attempts[3].RequestId);
            Assert.Equal(attempts[2].Body, attempts[3].Body);
            Assert.True(attempts[0].RequestId < attempts[2].RequestId);
            Assert.True(attempts[2].RequestId < attempts[4].RequestId);
            Assert.Equal(2, cluster.RouteRequests.Length);
        }
        finally
        {
            release.TrySetResult();
        }
    }

    [Fact]
    public async Task given_route_aliases_when_not_primary_raises_the_view_floor_should_reject_stale_alias_routes()
    {
        using var cluster = new PollCluster();
        uint view = 0;
        byte primary = 1;
        var refused = false;
        var staleReplies = 0;
        cluster.OnCoordinator = request => request.Code == CommandCodes.GET_PARTITION_ROUTE_CODE
            ? RouteReply(request, cluster.RouteContext, primary, 2, view: view) : null;
        cluster.OnPrimaryControl = request =>
        {
            if (request.Code != CommandCodes.GET_PARTITION_ROUTE_CODE)
            {
                return null;
            }
            if (view == 2 && request.Body[0] == IdKind.String.GetByte())
            {
                Interlocked.Increment(ref staleReplies);
                return RouteReply(request, cluster.RouteContext, 1, 2, view: 1);
            }
            return RouteReply(request, cluster.RouteContext, primary, 2, view: view);
        };
        cluster.OnPrimaryPoll = request =>
        {
            if (refused)
            {
                refused = false;
                primary = 2;
                view = 2;
                return Reply(request.Operation, [], VsrError.NOT_PRIMARY);
            }
            return Batch(request);
        };
        using var client = await cluster.ConnectAsync();
        await cluster.UntilAsync(() => cluster.Attachments.Count == 2);
        Task Read(Identifier stream) => client.PollMessagesAsync(stream, Topic, 0, Consumer.New(3),
            PollingStrategy.Offset(0), 1, false, TestContext.Current.CancellationToken);
        await Read(Stream);
        await Read(Identifier.String("stream-alias"));
        refused = true;
        await Read(Stream);
        var previous = cluster.Primaries[0].Requests(CommandCodes.POLL_MESSAGES_CODE);
        await Read(Identifier.String("stream-alias"));
        Assert.True(staleReplies >= 1);
        Assert.Equal(previous, cluster.Primaries[0].Requests(CommandCodes.POLL_MESSAGES_CODE));
        Assert.Equal(2, cluster.Primaries[1].Requests(CommandCodes.POLL_MESSAGES_CODE));
    }

    private static ConsensusSession SessionOf(TcpMessageStream client) =>
        Assert.IsType<ConsensusSession>(typeof(TcpMessageStream)
            .GetField("_consensusSession", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(client));

    /// <summary>Copies each message as is, and holds the first encryption until <see cref="Release" />.</summary>
    private sealed class HeldEncryptor(CancellationToken token) : IMessageEncryptor, IDisposable
    {
        private readonly ManualResetEventSlim _released = new();
        internal readonly TaskCompletionSource Entered = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal void Release() => _released.Set();

        public int GetMaxEncryptedLength(int plaintextLength) => plaintextLength;

        public int GetMaxDecryptedLength(int encryptedLength) => encryptedLength;

        public int Encrypt(ReadOnlySpan<byte> plaintext, Span<byte> destination)
        {
            Entered.TrySetResult();
            _released.Wait(token);
            plaintext.CopyTo(destination);
            return plaintext.Length;
        }

        public int Decrypt(ReadOnlySpan<byte> encryptedData, Span<byte> destination)
        {
            encryptedData.CopyTo(destination);
            return encryptedData.Length;
        }

        public void Dispose() => _released.Dispose();
    }

    private sealed class PausedContinuationContext : SynchronizationContext
    {
        internal readonly TaskCompletionSource Posted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal readonly TaskCompletionSource Release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public override void Post(SendOrPostCallback callback, object? state)
        {
            _ = Task.Run(async () =>
            {
                Posted.TrySetResult();
                await Release.Task;
                callback(state);
            });
        }
    }

    private static byte[] Batch(MockRequest request)
    {
        var body = new byte[40];
        BinaryPrimitives.WriteUInt32LittleEndian(body, Partition(request));
        return Reply(request.Operation, body);
    }

    private static bool IsOffsetWrite(MockRequest request) =>
        request.Operation is (byte)VsrOperation.StoreConsumerOffset or (byte)VsrOperation.DeleteConsumerOffset;

    private static uint Partition(MockRequest request) =>
        BinaryPrimitives.ReadUInt32LittleEndian(request.Body.AsSpan(request.Body.Length - 18));

    private static byte[] BindIdentity(byte[] body)
    {
        var position = 4;
        for (var index = 0; index < 2; index++)
        {
            position += 1 + body[position];
        }
        Assert.Equal(position + LoginRegister.SESSION_IDENTITY_BYTES + LoginRegister.BIND_SECRET_BYTES, body.Length);
        return body.AsSpan(position, LoginRegister.SESSION_IDENTITY_BYTES).ToArray();
    }

    private static byte[] BindingReply(MockRequest request) => Reply(request.Operation,
        RegisterBody(BinaryPrimitives.ReadUInt64LittleEndian(BindIdentity(request.Body).AsSpan(16)))[4..]);

    private static IggyConsumer PartitionConsumer(TcpMessageStream client, PollingStrategy strategy) => new(client,
        new IggyConsumerConfig
        {
            StreamId = Stream,
            TopicId = Topic,
            PartitionId = 0,
            Consumer = Consumer.New(3),
            PollingStrategy = strategy,
            AutoCommitMode = AutoCommitMode.Disabled,
            PollingIntervalMs = 0
        }, NullLoggerFactory.Instance);

    private static ConcurrentQueue<ConnectionState> RecordStates(TcpMessageStream client)
    {
        var states = new ConcurrentQueue<ConnectionState>();
        client.SubscribeConnectionEvents(change =>
        {
            states.Enqueue(change.CurrentState);
            return Task.CompletedTask;
        });
        return states;
    }

    private static ConcurrentQueue<Exception> RecordErrors(IggyConsumer consumer)
    {
        var errors = new ConcurrentQueue<Exception>();
        consumer.SubscribeToErrorEvents(error =>
        {
            errors.Enqueue(error.Exception);
            return Task.CompletedTask;
        });
        return errors;
    }

    private sealed class PollCluster : IDisposable
    {
        internal readonly MockNode Coordinator = new();
        internal readonly MockNode[] Primaries;
        internal readonly ConcurrentQueue<MockRequest> CoordinatorRequests = new();
        internal readonly ConcurrentQueue<MockRequest> Attachments = new();
        internal readonly ConcurrentQueue<MockRequest> Registrations = new();
        internal readonly ConcurrentQueue<MockRequest> OffsetWrites = new();
        internal readonly ConcurrentQueue<MockRequest> PrimaryRequests = new();
        internal PartitionContext RouteContext = new(17, 0, 1);
        internal Func<MockRequest, byte[]> OnPrimaryPoll = Batch;
        internal Func<MockRequest, byte[]> OnPrimaryOffsetWrite = request => Reply(request.Operation, new byte[4]);
        internal Func<MockRequest, byte[]> OnPrimarySend = request => Reply(request.Operation, new byte[4]);
        internal Func<MockRequest, byte[]> OnPrimaryAttachment = BindingReply;
        internal Func<MockRequest, byte[]?>? OnPrimaryControl;
        internal Func<MockRequest, byte[]?>? OnCoordinator;

        internal PollCluster(int primaryCount = 2)
        {
            Primaries = Enumerable.Range(0, primaryCount).Select(_ => new MockNode()).ToArray();
            Coordinator.Serve(request =>
            {
                CoordinatorRequests.Enqueue(request);
                if (OnCoordinator?.Invoke(request) is { } overridden)
                {
                    return overridden;
                }

                if (DefaultControl(request) is { } control)
                {
                    return control;
                }

                if (request.Code == CommandCodes.SYNC_CONSUMER_GROUP_CODE)
                {
                    var body = new byte[20];
                    BinaryPrimitives.WriteUInt64LittleEndian(body, 1);
                    BinaryPrimitives.WriteUInt32LittleEndian(body.AsSpan(8), 2);
                    BinaryPrimitives.WriteUInt32LittleEndian(body.AsSpan(16), 1);
                    return Reply(request.Operation, body);
                }

                if (request.Operation == (byte)VsrOperation.DeleteTopic)
                {
                    var reply = Reply(request.Operation, new byte[4]);
                    BinaryPrimitives.WriteUInt64LittleEndian(reply.AsSpan(VsrHeader.REPLY_COMMIT_OFFSET), MetadataCommit);
                    return reply;
                }

                return request.Operation >= (byte)VsrOperation.CreateStream
                    ? Reply(request.Operation, new byte[4])
                    : Answer(request);
            });
            foreach (var primary in Primaries)
            {
                primary.Serve(request =>
                {
                    PrimaryRequests.Enqueue(request);
                    if (request.Operation == OPERATION_REGISTER)
                    {
                        Registrations.Enqueue(request);
                        return Reply(request.Operation, [], VsrError.UNAUTHENTICATED);
                    }

                    if (request.Code == CommandCodes.BIND_SESSION_CODE)
                    {
                        Attachments.Enqueue(request);
                        return OnPrimaryAttachment(request);
                    }

                    if (IsOffsetWrite(request))
                    {
                        OffsetWrites.Enqueue(request);
                        return OnPrimaryOffsetWrite(request);
                    }

                    if (request.Operation == (byte)VsrOperation.SendMessages)
                    {
                        return OnPrimarySend(request);
                    }
                    return request.Code == CommandCodes.POLL_MESSAGES_CODE
                        ? OnPrimaryPoll(request)
                        : OnPrimaryControl?.Invoke(request) ?? DefaultControl(request) ?? Answer(request);
                });
            }
        }

        internal MockRequest[] RouteRequests => CoordinatorRequests.Concat(PrimaryRequests)
            .Where(request => request.Code == CommandCodes.GET_PARTITION_ROUTE_CODE)
            .OrderBy(request => request.RequestId).ToArray();

        private byte[]? DefaultControl(MockRequest request)
        {
            if (request.Code == CommandCodes.GET_CLUSTER_METADATA_CODE)
            {
                var roster = new List<byte>();
                WriteString(roster, "cluster");
                roster.AddRange(BitConverter.GetBytes((uint)Primaries.Length + 1));
                WriteNode(roster, Coordinator.Port, true);
                for (var index = 0; index < Primaries.Length; index++)
                {
                    WriteNode(roster, Primaries[index].Port, false, checked((byte)(index + 1)));
                }

                return Reply(request.Operation, roster.ToArray());
            }

            if (request.Code == CommandCodes.GET_PARTITION_ROUTE_CODE)
            {
                return RouteReply(request, RouteContext,
                    Primaries.Length == 0 ? (byte)0 : checked((byte)(RoutedPartition(request) + 1)),
                    (uint)Math.Max(1, Primaries.Length));
            }

            return null;
        }

        internal async Task UntilAsync(Func<bool> predicate)
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
            timeout.CancelAfter(TimeSpan.FromSeconds(10));
            while (!predicate())
            {
                await Task.Delay(5, timeout.Token);
            }
        }

        internal async Task<TcpMessageStream> ConnectAsync(int? maxResponseFrameSize = null,
            IMessageEncryptor? encryptor = null)
        {
            var configuration = new IggyClientConfigurator
            {
                BaseAddress = $"127.0.0.1:{Coordinator.Port}",
                Protocol = Protocol.Tcp,
                HeartbeatInterval = TimeSpan.FromHours(1),
                MessageEncryptor = encryptor,
                ReconnectionSettings = new ReconnectionSettings
                {
                    InitialDelay = TimeSpan.Zero,
                    WaitAfterReconnect = TimeSpan.Zero
                }
            };
            if (maxResponseFrameSize is { } frameLimit)
            {
                configuration.MaxResponseFrameSize = frameLimit;
            }
            var client = new TcpMessageStream(configuration, NullLoggerFactory.Instance);
            await client.ConnectAsync(TestContext.Current.CancellationToken);
            await client.LoginUserAsync("user", "secret", TestContext.Current.CancellationToken);
            return client;
        }

        public void Dispose()
        {
            Coordinator.Dispose();
            foreach (var primary in Primaries)
            {
                primary.Dispose();
            }
        }

        private static uint RoutedPartition(MockRequest request) =>
            BinaryPrimitives.ReadUInt32LittleEndian(request.Body.AsSpan(request.Body.Length - sizeof(uint)));
    }
}
