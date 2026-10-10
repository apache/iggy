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
using Apache.Iggy.Configuration;
using Apache.Iggy.Contracts;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient.Implementations;
using Apache.Iggy.Kinds;
using Apache.Iggy.Utils;
using Apache.Iggy.Vsr;
using Microsoft.Extensions.Logging.Abstractions;
using static Apache.Iggy.Tests.VsrTests.MockFrames;

namespace Apache.Iggy.Tests.VsrTests;

public sealed class PrimaryPollingTests
{
    private static readonly Identifier Stream = Identifier.Numeric(1);
    private static readonly Identifier Topic = Identifier.Numeric(2);
    private static readonly Identifier Group = Identifier.Numeric(3);
    private const ulong MetadataCommit = 50;

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
        Assert.Equal(0, cluster.Coordinator.Requests(CommandCodes.POLL_MESSAGES_ON_PRIMARY_CODE));
        Assert.Equal(2, cluster.Coordinator.Requests(CommandCodes.GET_POLL_ROUTING_CODE));
        Assert.All(cluster.Primaries, primary =>
        {
            Assert.Equal(1, primary.Connections);
            Assert.Equal(0, primary.Registrations);
            Assert.Equal(1, primary.Requests(CommandCodes.BIND_SESSION_CODE));
            Assert.Equal(2, primary.Requests(CommandCodes.POLL_MESSAGES_ON_PRIMARY_CODE));
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
    public async Task given_cached_route_when_metadata_commits_should_reattach_at_the_acknowledged_floor()
    {
        using var cluster = new PollCluster();
        using var client = await cluster.ConnectAsync();
        await PollAsync(client);
        await client.DeleteTopicAsync(Stream, Topic, TestContext.Current.CancellationToken);
        await PollAsync(client);

        Assert.Equal(2, cluster.Coordinator.Requests(CommandCodes.GET_POLL_ROUTING_CODE));
        Assert.Equal(new ulong[] { 1, MetadataCommit }, cluster.Attachments.Select(request =>
            BinaryPrimitives.ReadUInt64LittleEndian(BindIdentity(request.Body).AsSpan(24))));
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
        Assert.Equal(0, cluster.Coordinator.Requests(CommandCodes.GET_POLL_ROUTING_CODE));
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
        Assert.Equal(1, cluster.Coordinator.Requests(CommandCodes.GET_POLL_ROUTING_CODE));
        Assert.Equal(generation, ((ISessionGenerationProvider)client).SessionGeneration);
        if (!standalone)
        {
            Assert.Equal(2, cluster.Primaries[0].Requests(CommandCodes.POLL_MESSAGES_ON_PRIMARY_CODE));
        }
    }

    [Theory]
    [InlineData((byte)VsrOperation.TruncatePartition)]
    [InlineData((byte)VsrOperation.DeleteSegments)]
    public async Task given_delete_segments_commit_when_polling_should_raise_the_attachment_floor(byte replyOperation)
    {
        using var cluster = new PollCluster();
        cluster.OnCoordinator = request =>
        {
            if (request.Operation != (byte)VsrOperation.DeleteSegments)
            {
                return null;
            }

            var reply = Reply(replyOperation,
                replyOperation == (byte)VsrOperation.TruncatePartition ? new byte[4] : []);
            BinaryPrimitives.WriteUInt64LittleEndian(reply.AsSpan(VsrHeader.REPLY_COMMIT_OFFSET), MetadataCommit);
            return reply;
        };
        using var client = await cluster.ConnectAsync();
        await PollAsync(client);
        await client.DeleteSegmentsAsync(Stream, Topic, 0, 1, TestContext.Current.CancellationToken);
        await PollAsync(client);

        Assert.Equal(new ulong[] { 1, MetadataCommit }, cluster.Attachments.Select(request =>
            BinaryPrimitives.ReadUInt64LittleEndian(BindIdentity(request.Body).AsSpan(24))));
        Assert.Equal(1, cluster.Primaries[0].Connections);
    }

    [Fact]
    public async Task given_roster_without_nodes_when_polling_should_preserve_the_last_valid_topology()
    {
        using var cluster = new PollCluster();
        var emptyRoster = true;
        cluster.OnCoordinator = request => request.Code == CommandCodes.GET_CLUSTER_METADATA_CODE && emptyRoster
            ? Reply(request.Operation, new byte[8])
            : null;
        using var client = await cluster.ConnectAsync();
        await Assert.ThrowsAsync<MalformedResponseException>(() => PollAsync(client));
        Assert.Equal(0, cluster.Coordinator.Requests(CommandCodes.POLL_MESSAGES_CODE));
        Assert.All(cluster.Primaries, primary => Assert.Equal(0, primary.Connections));

        emptyRoster = false;
        await PollAsync(client);
        emptyRoster = true;
        await Assert.ThrowsAsync<MalformedResponseException>(() =>
            client.GetClusterMetadataAsync(TestContext.Current.CancellationToken));
        var reads = cluster.Coordinator.Requests(CommandCodes.GET_CLUSTER_METADATA_CODE);
        await PollAsync(client);

        Assert.Equal(reads, cluster.Coordinator.Requests(CommandCodes.GET_CLUSTER_METADATA_CODE));
        Assert.Equal(0, cluster.Coordinator.Requests(CommandCodes.POLL_MESSAGES_CODE));
        Assert.Equal(2, cluster.Primaries[0].Requests(CommandCodes.POLL_MESSAGES_ON_PRIMARY_CODE));
    }

    [Fact]
    public async Task given_only_refusals_when_the_poll_budget_expires_should_report_not_accepted()
    {
        using var cluster = new PollCluster();
        cluster.OnCoordinator = request => request.Code == CommandCodes.GET_POLL_ROUTING_CODE
            ? Reply(request.Operation, [], VsrError.TRANSIENT_NOT_ACCEPTED)
            : null;
        using var client = await cluster.ConnectAsync();
        var generation = ((ISessionGenerationProvider)client).SessionGeneration;
        var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => PollAsync(client));

        Assert.Equal(VsrError.TRANSIENT_NOT_ACCEPTED, error.StatusCode);
        Assert.Equal(generation, ((ISessionGenerationProvider)client).SessionGeneration);
        Assert.All(cluster.Primaries, primary => Assert.Equal(0, primary.Connections));
    }

    [Fact]
    public async Task given_poll_waiting_for_connection_when_metadata_commits_should_revalidate_its_cached_route()
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

            Assert.Equal(new ulong[] { 1, MetadataCommit }, cluster.Attachments.Select(request =>
                BinaryPrimitives.ReadUInt64LittleEndian(BindIdentity(request.Body).AsSpan(24))));
            Assert.Equal(2, cluster.Coordinator.Requests(CommandCodes.GET_POLL_ROUTING_CODE));
            Assert.Equal(1, cluster.Primaries[0].Connections);
        }
        finally
        {
            release.TrySetResult();
        }
    }

    [Fact]
    public async Task given_more_endpoints_than_pool_capacity_when_polling_should_retire_idle_connections_only()
    {
        using var cluster = new PollCluster(TcpMessageStream.MAX_POLL_CONNECTIONS + 1);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        cluster.OnPrimaryPoll = request =>
        {
            if (Partition(request) == 0)
            {
                started.TrySetResult();
                release.Task.GetAwaiter().GetResult();
            }

            return Batch(request);
        };
        using var client = await cluster.ConnectAsync();
        var active = PollAsync(client);
        try
        {
            await started.Task.WaitAsync(TestContext.Current.CancellationToken);
            for (uint partition = 1; partition < cluster.Primaries.Length; partition++)
            {
                var result = await client.PollMessagesAsync(Stream, Topic, partition, Consumer.Group(3),
                    PollingStrategy.Next(), 10, true, TestContext.Current.CancellationToken);
                Assert.Equal(partition, result.PartitionId);
            }

            release.SetResult();
            await active;
            await client.PollMessagesAsync(Stream, Topic, 1, Consumer.Group(3), PollingStrategy.Next(), 10, true,
                TestContext.Current.CancellationToken);

            Assert.Equal(1, cluster.Primaries[0].Connections);
            Assert.Equal(2, cluster.Primaries[1].Connections);
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
        Assert.Equal(2, cluster.Primaries[0].Connections);
        Assert.Equal(1, cluster.Coordinator.Registrations);
        Assert.Equal(generation, ((ISessionGenerationProvider)client).SessionGeneration);
    }

    [Fact]
    public async Task given_primary_refusal_when_polling_should_refresh_route_without_moving_membership()
    {
        using var cluster = new PollCluster();
        var polls = 0;
        cluster.OnPrimaryPoll = request => Interlocked.Increment(ref polls) == 1
            ? Reply(request.Operation, [], TRANSIENT_NOT_ACCEPTED)
            : Batch(request);
        using var client = await cluster.ConnectAsync();
        await PollAsync(client);

        Assert.Equal(2, polls);
        Assert.Equal(2, cluster.Coordinator.Requests(CommandCodes.GET_POLL_ROUTING_CODE));
        Assert.Equal(2, cluster.Primaries[0].Requests(CommandCodes.BIND_SESSION_CODE));
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
            if (request.Code == CommandCodes.GET_POLL_ROUTING_CODE && Interlocked.Increment(ref routes) == 1)
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
        await PollAsync(client);

        Assert.Equal(2, routes);
        Assert.Equal(1, cluster.Coordinator.Registrations);
        Assert.Equal(1, cluster.Coordinator.Requests(CommandCodes.BIND_SESSION_CODE));
        Assert.Equal(generation, ((ISessionGenerationProvider)client).SessionGeneration);
        Assert.Equal(1, cluster.Primaries[0].Requests(CommandCodes.POLL_MESSAGES_ON_PRIMARY_CODE));
    }

    [Fact]
    public async Task given_stale_attachment_when_polling_should_replace_only_the_auxiliary_session()
    {
        using var cluster = new PollCluster();
        var attachments = 0;
        cluster.OnPrimaryAttachment = request => Interlocked.Increment(ref attachments) == 1
            ? Reply(request.Operation, [], VsrError.STALE_CLIENT)
            : BindingReply(request);
        using var client = await cluster.ConnectAsync();
        var generation = ((ISessionGenerationProvider)client).SessionGeneration;
        await PollAsync(client);

        Assert.Equal(2, attachments);
        Assert.Equal(2, cluster.Primaries[0].Connections);
        Assert.Equal(1, cluster.Primaries[0].Requests(CommandCodes.POLL_MESSAGES_ON_PRIMARY_CODE));
        Assert.Equal(1, cluster.Coordinator.Registrations);
        Assert.Equal(generation, ((ISessionGenerationProvider)client).SessionGeneration);
    }

    [Fact]
    public async Task given_wrong_binding_epoch_when_polling_should_refuse_before_polling()
    {
        using var cluster = new PollCluster();
        cluster.OnPrimaryAttachment = request => Reply(request.Operation, RegisterBody(session: 129)[4..]);
        using var client = await cluster.ConnectAsync();

        var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => PollAsync(client));

        Assert.Equal(VsrError.INVALID_FORMAT, error.StatusCode);
        Assert.All(cluster.Primaries, primary =>
            Assert.Equal(0, primary.Requests(CommandCodes.POLL_MESSAGES_ON_PRIMARY_CODE)));
        Assert.Equal(1, cluster.Coordinator.Registrations);
    }

    [Fact]
    public async Task given_partial_primary_reply_when_canceled_should_dispose_it_before_the_next_poll()
    {
        using var cluster = new PollCluster();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var polls = 0;
        cluster.OnPrimaryPoll = request =>
        {
            var batch = Batch(request);
            if (Interlocked.Increment(ref polls) == 1)
            {
                started.SetResult();
                return batch[..^8];
            }

            return batch;
        };
        using var client = await cluster.ConnectAsync();
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        var pending = client.PollMessagesAsync(Stream, Topic, 0, Consumer.Group(3), PollingStrategy.Next(), 10,
            true, cancellation.Token);
        await started.Task.WaitAsync(TestContext.Current.CancellationToken);
        await cancellation.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
        await PollAsync(client);

        Assert.Equal(2, polls);
        Assert.Equal(2, cluster.Primaries[0].Connections);
        Assert.Equal(1, cluster.Coordinator.Connections);
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
    public async Task given_new_coordinator_session_when_polling_should_retire_old_data_connections()
    {
        using var cluster = new PollCluster();
        using var client = await cluster.ConnectAsync();
        await PollAsync(client);
        var oldParent = BinaryPrimitives.ReadUInt128LittleEndian(BindIdentity(cluster.Attachments.Single().Body));
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
        Assert.Equal(1, cluster.Coordinator.Requests(CommandCodes.GET_POLL_ROUTING_CODE));
        Assert.Equal(0, Assert.Single(polls).Body[^1]);
    }

    [Fact]
    public async Task given_cluster_manual_commit_poll_when_the_primary_connection_drops_should_poll_again()
    {
        using var cluster = new PollCluster();
        var polls = 0;
        cluster.OnPrimaryPoll = request => Interlocked.Increment(ref polls) == 1 ? [] : Batch(request);
        using var client = await cluster.ConnectAsync();
        await client.PollMessagesAsync(Stream, Topic, 0, Consumer.New(3), PollingStrategy.Next(), 10, false,
            TestContext.Current.CancellationToken);

        Assert.Equal(2, Volatile.Read(ref polls));
        Assert.Equal(2, cluster.Primaries[0].Connections);
    }

    [Fact]
    public async Task given_cluster_offset_writes_when_storing_and_deleting_should_write_on_the_partition_primary()
    {
        using var cluster = new PollCluster { RouteContext = new PartitionContext(17, 9, 52) };
        using var client = await cluster.ConnectAsync();
        await client.StoreOffsetAsync(Consumer.New(3), Stream, Topic, 10, 1, TestContext.Current.CancellationToken);
        await client.DeleteOffsetAsync(Consumer.New(3), Stream, Topic, 1, TestContext.Current.CancellationToken);

        Assert.DoesNotContain(cluster.CoordinatorRequests, IsOffsetWrite);
        Assert.Equal(1, cluster.Coordinator.Requests(CommandCodes.GET_CONSUMER_OFFSET_ROUTING_CODE));
        Assert.Equal([(byte)VsrOperation.StoreConsumerOffset, (byte)VsrOperation.DeleteConsumerOffset],
            cluster.OffsetWrites.Select(write => write.Operation));
        Assert.All(cluster.OffsetWrites, write => Assert.Equal(cluster.RouteContext, write.Context));
    }

    /// <summary>
    ///     The partition dedups replicated requests by client id, session and request id, so an offset write on a
    ///     data connection has to take its id from the counter of the session it is bound to.
    /// </summary>
    [Fact]
    public async Task given_a_metadata_write_when_storing_an_offset_on_the_primary_should_take_the_next_request_id()
    {
        using var cluster = new PollCluster();
        using var client = await cluster.ConnectAsync();
        await client.UpdateStreamAsync(Stream, "renamed", TestContext.Current.CancellationToken);
        await client.StoreOffsetAsync(Consumer.New(3), Stream, Topic, 10, 1, TestContext.Current.CancellationToken);

        var update = Assert.Single(cluster.CoordinatorRequests,
            request => request.Operation == (byte)VsrOperation.UpdateStream);
        Assert.Equal(update.RequestId + 1, Assert.Single(cluster.OffsetWrites).RequestId);
    }

    [Fact]
    public async Task given_a_dropped_primary_connection_when_storing_an_offset_again_should_not_repeat_the_request_id()
    {
        using var cluster = new PollCluster();
        cluster.OnPrimaryOffsetWrite = request =>
            cluster.OffsetWrites.Count == 1 ? [] : Reply(request.Operation, new byte[4]);
        using var client = await cluster.ConnectAsync();

        await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() => client.StoreOffsetAsync(Consumer.New(3),
            Stream, Topic, 10, 1, TestContext.Current.CancellationToken));
        await client.StoreOffsetAsync(Consumer.New(3), Stream, Topic, 10, 1, TestContext.Current.CancellationToken);

        Assert.Equal(2, cluster.Primaries[1].Connections);
        var first = cluster.OffsetWrites.First().RequestId;
        Assert.Equal([first, first + 1], cluster.OffsetWrites.Select(write => write.RequestId));
    }

    [Fact]
    public async Task given_a_caller_context_when_a_primary_refuses_should_keep_it_on_the_retry()
    {
        using var cluster = new PollCluster { RouteContext = new PartitionContext(18, 1, 60) };
        var caller = new PartitionContext(17, 9, 52);
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
        using var cluster = new PollCluster { RouteContext = new PartitionContext(17, 9, 52) };
        var first = cluster.RouteContext;
        var polls = new ConcurrentQueue<MockRequest>();
        cluster.OnPrimaryPoll = request =>
        {
            polls.Enqueue(request);
            cluster.RouteContext = new PartitionContext(18, 1, 60);
            return polls.Count == 1 ? Reply(request.Operation, [], TRANSIENT_NOT_ACCEPTED) : Batch(request);
        };
        using var client = await cluster.ConnectAsync();
        await client.PollMessagesAsync(Stream, Topic, 0, Consumer.New(3), PollingStrategy.Next(), 10, false,
            TestContext.Current.CancellationToken);

        Assert.Equal(2, cluster.Coordinator.Requests(CommandCodes.GET_POLL_ROUTING_CODE));
        Assert.Equal([first, first], polls.Select(poll => poll.Context));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_a_history_refusal_when_polling_should_report_it_and_route_the_next_poll_again(
        bool autoCommit)
    {
        using var cluster = new PollCluster { RouteContext = new PartitionContext(17, 9, 52) };
        var replaced = new PartitionContext(18, 1, 60);
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

        Assert.Equal(2, cluster.Coordinator.Requests(CommandCodes.GET_POLL_ROUTING_CODE));
        Assert.Equal(replaced, polls.Last().Context);
    }

    [Fact]
    public async Task given_a_history_refusal_when_storing_an_offset_should_report_it_and_route_the_next_write_again()
    {
        using var cluster = new PollCluster { RouteContext = new PartitionContext(17, 9, 52) };
        var replaced = new PartitionContext(18, 1, 60);
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

        Assert.Equal(2, cluster.Coordinator.Requests(CommandCodes.GET_CONSUMER_OFFSET_ROUTING_CODE));
        Assert.Equal(replaced, cluster.OffsetWrites.Last().Context);
    }

    private static async Task PollAsync(TcpMessageStream client)
    {
        await client.PollMessagesAsync(Stream, Topic, 0, Consumer.Group(3), PollingStrategy.Next(), 10, true,
            TestContext.Current.CancellationToken);
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

    private sealed class PollCluster : IDisposable
    {
        internal readonly MockNode Coordinator = new();
        internal readonly MockNode[] Primaries;
        internal readonly ConcurrentQueue<MockRequest> CoordinatorRequests = new();
        internal readonly ConcurrentQueue<MockRequest> Attachments = new();
        internal readonly ConcurrentQueue<MockRequest> Registrations = new();
        internal readonly ConcurrentQueue<MockRequest> OffsetWrites = new();
        internal PartitionContext RouteContext;
        internal Func<MockRequest, byte[]> OnPrimaryPoll = Batch;
        internal Func<MockRequest, byte[]> OnPrimaryOffsetWrite = request => Reply(request.Operation, new byte[4]);
        internal Func<MockRequest, byte[]> OnPrimaryAttachment = BindingReply;
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

                if (request.Code == CommandCodes.GET_CLUSTER_METADATA_CODE)
                {
                    var roster = new List<byte>();
                    WriteString(roster, "cluster");
                    roster.AddRange(BitConverter.GetBytes((uint)Primaries.Length + 1));
                    WriteNode(roster, Coordinator.Port, true);
                    foreach (var primary in Primaries)
                    {
                        WriteNode(roster, primary.Port, false);
                    }

                    return Reply(request.Operation, roster.ToArray());
                }

                if (request.Code is CommandCodes.GET_POLL_ROUTING_CODE or CommandCodes.GET_CONSUMER_OFFSET_ROUTING_CODE)
                {
                    return RouteReply(request, RouteContext,
                        Primaries.Length == 0 ? Coordinator.Port : Primaries[RoutedPartition(request)].Port,
                        Primaries.Length == 0);
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

                    return request.Code == CommandCodes.POLL_MESSAGES_ON_PRIMARY_CODE
                        ? OnPrimaryPoll(request)
                        : Answer(request);
                });
            }
        }

        internal async Task<TcpMessageStream> ConnectAsync()
        {
            var client = new TcpMessageStream(new IggyClientConfigurator
            {
                BaseAddress = $"127.0.0.1:{Coordinator.Port}",
                Protocol = Protocol.Tcp,
                HeartbeatInterval = TimeSpan.FromHours(1),
                ReconnectionSettings = new ReconnectionSettings
                {
                    InitialDelay = TimeSpan.Zero,
                    WaitAfterReconnect = TimeSpan.Zero
                }
            }, NullLoggerFactory.Instance);
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

        /// <summary>An offset routing body ends with the partition id, a poll routing body with the poll options.</summary>
        private static uint RoutedPartition(MockRequest request) =>
            request.Code == CommandCodes.GET_POLL_ROUTING_CODE
                ? Partition(request)
                : BinaryPrimitives.ReadUInt32LittleEndian(request.Body.AsSpan(request.Body.Length - sizeof(uint)));
    }
}
