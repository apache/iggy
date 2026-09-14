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
using System.Text;
using Apache.Iggy.Configuration;
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
            Assert.Equal(1, primary.Registrations);
            Assert.Equal(1, primary.Requests(CommandCodes.ATTACH_CONSUMER_SESSION_CODE));
            Assert.Equal(2, primary.Requests(CommandCodes.POLL_MESSAGES_ON_PRIMARY_CODE));
        });
        var parent = Assert.Single(cluster.CoordinatorRequests,
            request => request.Operation == (byte)VsrOperation.JoinConsumerGroup);
        Assert.All(cluster.Attachments, attachment =>
        {
            Assert.Equal(parent.ClientId, BinaryPrimitives.ReadUInt128LittleEndian(attachment.Body));
            Assert.Equal(parent.Session, BinaryPrimitives.ReadUInt64LittleEndian(attachment.Body.AsSpan(16)));
            Assert.NotEqual(parent.ClientId, attachment.ClientId);
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
            BinaryPrimitives.ReadUInt64LittleEndian(request.Body.AsSpan(24))));
        Assert.Equal(1, cluster.Primaries[0].Connections);
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
                BinaryPrimitives.ReadUInt64LittleEndian(request.Body.AsSpan(24))));
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
        Assert.Equal(2, cluster.Primaries[0].Requests(CommandCodes.ATTACH_CONSUMER_SESSION_CODE));
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
        Assert.Equal(2, cluster.Coordinator.Registrations);
        Assert.NotEqual(generation, ((ISessionGenerationProvider)client).SessionGeneration);
        Assert.Equal(1, cluster.Primaries[0].Requests(CommandCodes.POLL_MESSAGES_ON_PRIMARY_CODE));
    }

    [Fact]
    public async Task given_stale_attachment_when_polling_should_replace_only_the_auxiliary_session()
    {
        using var cluster = new PollCluster();
        var attachments = 0;
        cluster.OnPrimaryAttachment = request => Interlocked.Increment(ref attachments) == 1
            ? Reply(request.Operation, [], VsrError.STALE_CLIENT)
            : Answer(request);
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
    public async Task given_self_credentials_changed_when_opening_new_primary_should_use_the_updated_login()
    {
        using var cluster = new PollCluster();
        using var client = await cluster.ConnectAsync();
        await PollAsync(client);
        await client.UpdateUserAsync(Identifier.Numeric(7), "renamed", token: TestContext.Current.CancellationToken);
        await client.ChangePasswordAsync(Identifier.String("renamed"), "secret", "new-secret",
            TestContext.Current.CancellationToken);
        await client.PollMessagesAsync(Stream, Topic, 1, Consumer.Group(3), PollingStrategy.Next(), 10, true,
            TestContext.Current.CancellationToken);

        Assert.Equal(LoginRegister.Serialize("renamed", "new-secret"), cluster.Registrations.Last().Body);
        Assert.Equal(1, cluster.Coordinator.Registrations);
    }

    [Fact]
    public async Task given_new_coordinator_session_when_polling_should_retire_old_data_connections()
    {
        using var cluster = new PollCluster();
        using var client = await cluster.ConnectAsync();
        await PollAsync(client);
        var oldParent = BinaryPrimitives.ReadUInt128LittleEndian(cluster.Attachments.Single().Body);
        await client.LogoutUserAsync(TestContext.Current.CancellationToken);
        await client.LoginUserAsync("other", "secret", TestContext.Current.CancellationToken);
        await PollAsync(client);

        Assert.Equal(2, cluster.Primaries[0].Connections);
        Assert.NotEqual(oldParent, BinaryPrimitives.ReadUInt128LittleEndian(cluster.Attachments.Last().Body));
    }

    [Fact]
    public async Task given_cluster_non_auto_commit_poll_when_polling_should_use_the_coordinator()
    {
        using var cluster = new PollCluster();
        using var client = await cluster.ConnectAsync();
        await client.PollMessagesAsync(Stream, Topic, 0, Consumer.Group(3), PollingStrategy.Next(), 10, false,
            TestContext.Current.CancellationToken);

        Assert.Equal(1, cluster.Coordinator.Requests(CommandCodes.POLL_MESSAGES_CODE));
        Assert.Equal(0, cluster.Coordinator.Requests(CommandCodes.GET_POLL_ROUTING_CODE));
        Assert.All(cluster.Primaries, primary => Assert.Equal(0, primary.Connections));
    }

    private static async Task PollAsync(TcpMessageStream client)
    {
        await client.PollMessagesAsync(Stream, Topic, 0, Consumer.Group(3), PollingStrategy.Next(), 10, true,
            TestContext.Current.CancellationToken);
    }

    private static byte[] Batch(MockRequest request)
    {
        var body = new byte[16];
        BinaryPrimitives.WriteUInt32LittleEndian(body, Partition(request));
        return Reply(request.Operation, body);
    }

    private static uint Partition(MockRequest request) =>
        BinaryPrimitives.ReadUInt32LittleEndian(request.Body.AsSpan(request.Body.Length - 18));

    private sealed class PollCluster : IDisposable
    {
        internal readonly MockNode Coordinator = new();
        internal readonly MockNode[] Primaries;
        internal readonly ConcurrentQueue<MockRequest> CoordinatorRequests = new();
        internal readonly ConcurrentQueue<MockRequest> Attachments = new();
        internal readonly ConcurrentQueue<MockRequest> Registrations = new();
        internal Func<MockRequest, byte[]> OnPrimaryPoll = Batch;
        internal Func<MockRequest, byte[]> OnPrimaryAttachment = Answer;
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

                if (request.Code == CommandCodes.GET_POLL_ROUTING_CODE)
                {
                    var attachment = new byte[32];
                    BinaryPrimitives.WriteUInt128LittleEndian(attachment, request.ClientId);
                    BinaryPrimitives.WriteUInt64LittleEndian(attachment.AsSpan(16), request.Session);
                    BinaryPrimitives.WriteUInt64LittleEndian(attachment.AsSpan(24), 1);
                    var body = new List<byte>(attachment);
                    WriteNode(body, Primaries[Partition(request)].Port, false);
                    return Reply(request.Operation, body.ToArray());
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
                    }

                    if (request.Code == CommandCodes.ATTACH_CONSUMER_SESSION_CODE)
                    {
                        Attachments.Enqueue(request);
                        return OnPrimaryAttachment(request);
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

        private static void WriteNode(List<byte> body, ushort port, bool leader)
        {
            WriteString(body, $"node-{port}");
            WriteString(body, "127.0.0.1");
            body.AddRange(BitConverter.GetBytes(port));
            body.AddRange(new byte[6]);
            body.Add(leader ? (byte)0 : (byte)1);
            body.Add(0);
        }

        private static void WriteString(List<byte> body, string value)
        {
            var bytes = Encoding.UTF8.GetBytes(value);
            body.AddRange(BitConverter.GetBytes((uint)bytes.Length));
            body.AddRange(bytes);
        }
    }
}
