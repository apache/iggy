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
using Apache.Iggy.Utils;
using Apache.Iggy.Vsr;
using Microsoft.Extensions.Logging.Abstractions;
using static Apache.Iggy.Tests.VsrTests.MockFrames;

namespace Apache.Iggy.Tests.VsrTests;

/// <summary>
///     Raw requests that name the replica to answer them. Mirrors the replica request tests of
///     <c>core/sdk/src/driver.rs</c>.
/// </summary>
public sealed class ReplicaRequestTests
{
    private const uint GetStatsCode = 10;
    private const uint NotPrimary = 89;
    private const ulong MetadataCommit = 7;
    private static readonly byte[] LeaderReply = "leader"u8.ToArray();
    private static readonly byte[] BackupReply = "backup"u8.ToArray();

    [Fact]
    public async Task given_two_replicas_when_a_raw_request_names_each_should_reach_it_with_the_session_watermark()
    {
        var token = TestContext.Current.CancellationToken;
        using var cluster = new TwoReplicas(_ => Reply(OPERATION_NON_REPLICATED, BackupReply));
        using var client = await cluster.SignedInAsync();
        await client.SendBinaryRequestAsync((uint)CommandCodes.CREATE_STREAM_CODE, [], token);

        var backup = await client.SendBinaryRequestToReplicaAsync(GetStatsCode, [1], 1, token);
        var leader = await client.SendBinaryRequestToReplicaAsync(GetStatsCode, [0], 0, token);

        Assert.Equal(BackupReply, backup);
        Assert.Equal(LeaderReply, leader);
        var backupRequest = Assert.Single(cluster.BackupStats);
        Assert.Equal([1], backupRequest.Body);
        Assert.Equal(MetadataCommit, backupRequest.Context.MetadataOp);
        var leaderRequest = Assert.Single(cluster.LeaderStats);
        Assert.Equal([0], leaderRequest.Body);
        Assert.Equal(MetadataCommit, leaderRequest.Context.MetadataOp);
    }

    [Fact]
    public async Task given_a_backup_that_answers_not_primary_when_a_raw_request_names_it_should_return_the_refusal()
    {
        var token = TestContext.Current.CancellationToken;
        using var cluster = new TwoReplicas(_ => Reply(OPERATION_NON_REPLICATED, [], NotPrimary));
        using var client = await cluster.SignedInAsync();

        var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() =>
            client.SendBinaryRequestToReplicaAsync(GetStatsCode, [], 1, token));

        Assert.Equal((int)NotPrimary, error.StatusCode);
        Assert.Single(cluster.BackupStats);
        Assert.Empty(cluster.LeaderStats);
    }

    [Fact]
    public async Task given_a_backup_that_refuses_and_drops_its_link_when_a_raw_request_names_it_should_ask_it_again()
    {
        var token = TestContext.Current.CancellationToken;
        using var cluster = new TwoReplicas(attempt => attempt switch
        {
            1 => Reply(OPERATION_NON_REPLICATED, [], TRANSIENT_NOT_ACCEPTED),
            2 => throw new IOException("lost link"),
            _ => Reply(OPERATION_NON_REPLICATED, BackupReply)
        });
        using var client = await cluster.SignedInAsync();

        var backup = await client.SendBinaryRequestToReplicaAsync(GetStatsCode, [], 1, token);

        Assert.Equal(BackupReply, backup);
        Assert.Equal(3, cluster.BackupStats.Count);
        Assert.Empty(cluster.LeaderStats);
    }

    [Fact]
    public async Task given_an_id_the_roster_does_not_name_when_a_raw_request_names_it_should_fail_after_one_roster_read()
    {
        var token = TestContext.Current.CancellationToken;
        using var cluster = new TwoReplicas(_ => Reply(OPERATION_NON_REPLICATED, BackupReply));
        using var client = await cluster.SignedInAsync();
        var rosterReads = cluster.Leader.Requests(GET_CLUSTER_METADATA_CODE)
            + cluster.Backup.Requests(GET_CLUSTER_METADATA_CODE);

        var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() =>
            client.SendBinaryRequestToReplicaAsync(GetStatsCode, [], 2, token));

        Assert.Equal(VsrError.INVALID_IDENTIFIER, error.StatusCode);
        Assert.Equal(rosterReads + 1, cluster.Leader.Requests(GET_CLUSTER_METADATA_CODE)
            + cluster.Backup.Requests(GET_CLUSTER_METADATA_CODE));
        Assert.Empty(cluster.BackupStats);
        Assert.Empty(cluster.LeaderStats);
    }

    [Fact]
    public async Task given_a_replicated_partition_read_or_session_command_when_sent_to_a_replica_should_refuse_it_before_sending()
    {
        var token = TestContext.Current.CancellationToken;
        using var cluster = new TwoReplicas(_ => Reply(OPERATION_NON_REPLICATED, BackupReply));
        using var client = await cluster.SignedInAsync();
        var requests = cluster.Recorded;

        foreach (var code in new[]
                 {
                     (uint)CommandCodes.CREATE_STREAM_CODE, (uint)CommandCodes.POLL_MESSAGES_CODE,
                     (uint)CommandCodes.GET_CONSUMER_OFFSET_CODE, (uint)CommandCodes.LOGIN_USER_CODE,
                     (uint)CommandCodes.LOGOUT_USER_CODE
                 })
        {
            var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() =>
                client.SendBinaryRequestToReplicaAsync(code, [], 1, token));
            Assert.Equal(VsrError.INVALID_COMMAND, error.StatusCode);
        }

        Assert.Equal(requests, cluster.Recorded);
        Assert.Empty(cluster.BackupStats);
        Assert.Equal(BackupReply, await client.SendBinaryRequestToReplicaAsync(GetStatsCode, [], 1, token));
    }

    /// <summary>
    ///     Replica 0 leads metadata and is the node the client signs in on. Replica 1 is a backup that answers the
    ///     stats requests with what the test scripts for each attempt, numbered from 1.
    /// </summary>
    private sealed class TwoReplicas : IDisposable
    {
        internal readonly MockNode Leader = new();
        internal readonly MockNode Backup = new();
        internal readonly ConcurrentQueue<MockRequest> LeaderStats = new();
        internal readonly ConcurrentQueue<MockRequest> BackupStats = new();
        private int _recorded;

        internal TwoReplicas(Func<int, byte[]> backupAnswer)
        {
            Leader.Serve(request =>
            {
                Record(request);
                if (request.Code == (int)GetStatsCode)
                {
                    LeaderStats.Enqueue(request);
                    return Reply(OPERATION_NON_REPLICATED, LeaderReply);
                }
                return Handle(request);
            });
            Backup.Serve(request =>
            {
                Record(request);
                if (request.Code == (int)GetStatsCode)
                {
                    BackupStats.Enqueue(request);
                    return backupAnswer(BackupStats.Count);
                }
                return Handle(request);
            });
        }

        /// <summary>How many requests reached a node, leaving out the sign-in, link binds, pings and roster reads.</summary>
        internal int Recorded => Volatile.Read(ref _recorded);

        internal async Task<TcpMessageStream> SignedInAsync()
        {
            var client = new TcpMessageStream(new IggyClientConfigurator
            {
                BaseAddress = $"127.0.0.1:{Leader.Port}",
                Protocol = Protocol.Tcp,
                HeartbeatInterval = TimeSpan.FromMinutes(1)
            }, NullLoggerFactory.Instance);
            await client.ConnectAsync(TestContext.Current.CancellationToken);
            await client.LoginUserAsync("iggy", "iggy", TestContext.Current.CancellationToken);
            return client;
        }

        public void Dispose()
        {
            Leader.Dispose();
            Backup.Dispose();
        }

        private void Record(MockRequest request)
        {
            if (request.Operation != OPERATION_REGISTER
                && request.Code is not (BIND_SESSION_CODE or PING_CODE or GET_CLUSTER_METADATA_CODE))
            {
                Interlocked.Increment(ref _recorded);
            }
        }

        private byte[] Handle(MockRequest request)
        {
            if (request.Code == GET_CLUSTER_METADATA_CODE)
            {
                var body = new List<byte>();
                WriteString(body, "replicas");
                body.AddRange(BitConverter.GetBytes(2u));
                WriteNode(body, Leader.Port, true, 0);
                WriteNode(body, Backup.Port, false, 1);
                return Reply(OPERATION_NON_REPLICATED, body.ToArray());
            }
            var reply = Answer(request);
            if (request.Operation == (byte)VsrOperation.CreateStream)
            {
                BinaryPrimitives.WriteUInt64LittleEndian(reply.AsSpan(VsrHeader.REPLY_COMMIT_OFFSET), MetadataCommit);
            }
            return reply;
        }
    }
}
