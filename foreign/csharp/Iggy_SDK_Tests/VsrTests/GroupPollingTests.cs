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

/// <summary>
///     Group polls resolve the partition client-side from the coordinator's assignment. Mirrors
///     <c>foreign/go/client/tcp/tcp_group_polling_test.go</c>.
/// </summary>
public sealed class GroupPollingTests
{
    private const int SyncGroupCode = CommandCodes.SYNC_CONSUMER_GROUP_CODE;
    private const int PollMessagesCode = CommandCodes.POLL_MESSAGES_CODE;

    [Fact]
    public async Task given_member_holding_no_partitions_when_polled_twice_should_report_no_assignment_and_sync_once()
    {
        using var node = new MockNode();
        node.Serve(request => request.Code == SyncGroupCode
            ? Reply(OPERATION_NON_REPLICATED, AssignmentBody(9, []))
            : Answer(request));
        using var client = await ConnectAsync(node);

        var first = await PollOnceAsync(client);
        var second = await PollOnceAsync(client);

        Assert.Equal(PolledMessages.NoAssignedPartition, first.PartitionId);
        Assert.Empty(first.Messages);
        Assert.Equal(PolledMessages.NoAssignedPartition, second.PartitionId);
        Assert.Empty(second.Messages);
        // The empty assignment is cached: a member that owns nothing must not re-sync on every poll.
        Assert.Equal(1, node.Requests(SyncGroupCode));
        Assert.Equal(0, node.Requests(PollMessagesCode));
    }

    [Fact]
    public async Task given_rebalance_outlasting_the_attempts_when_polled_should_report_no_assignment()
    {
        using var node = new MockNode();
        node.Serve(request => request.Code switch
        {
            SyncGroupCode => Reply(OPERATION_NON_REPLICATED, AssignmentBody(9, [0])),
            // The server marks a stale assignment with the resync sentinel.
            PollMessagesCode => Reply(request.Operation, EmptyBatchBody(uint.MaxValue)),
            GET_CLUSTER_METADATA_CODE => StandaloneRoster(node.Port),
            _ => Answer(request)
        });
        using var client = await ConnectAsync(node);

        var polled = await PollOnceAsync(client);

        Assert.Equal(PolledMessages.NoAssignedPartition, polled.PartitionId);
        Assert.Empty(polled.Messages);
        Assert.Equal(2, node.Requests(PollMessagesCode));
        Assert.Equal(3, node.Requests(SyncGroupCode));
    }

    [Fact]
    public async Task given_fenced_poll_when_resynced_should_poll_the_new_assignment()
    {
        using var node = new MockNode();
        var generation = 1ul;
        node.Serve(request =>
        {
            switch (request.Code)
            {
                case SyncGroupCode:
                    return Reply(OPERATION_NON_REPLICATED, AssignmentBody(generation, [(uint)generation]));
                case PollMessagesCode when generation == 1:
                    // The member no longer owns the partition at this generation.
                    generation = 2;
                    return Reply(request.Operation, EmptyBatchBody(uint.MaxValue));
                case PollMessagesCode:
                    return Reply(request.Operation, EmptyBatchBody(2));
                case GET_CLUSTER_METADATA_CODE:
                    return StandaloneRoster(node.Port);
                default:
                    return Answer(request);
            }
        });
        using var client = await ConnectAsync(node);

        var polled = await PollOnceAsync(client);

        Assert.Equal(2u, polled.PartitionId);
        Assert.Equal(2, node.Requests(SyncGroupCode));
        Assert.Equal(2, node.Requests(PollMessagesCode));
    }

    [Fact]
    public async Task given_a_caller_context_when_a_group_poll_is_fenced_should_resync_and_report_the_fence()
    {
        using var node = new MockNode();
        node.Serve(request => request.Code switch
        {
            SyncGroupCode => Reply(OPERATION_NON_REPLICATED, AssignmentBody(9, [0])),
            PollMessagesCode => Reply(request.Operation, [], VsrError.CONSUMER_GROUP_PARTITION_NOT_OWNED),
            GET_CLUSTER_METADATA_CODE => StandaloneRoster(node.Port),
            _ => Answer(request)
        });
        using var client = await ConnectAsync(node);

        var fence = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => client.PollMessagesAsync(
            Identifier.Numeric(1), Identifier.Numeric(2), null, Consumer.Group(3),
            PollingStrategy.Next().WithContext(new PartitionContext(17, 9, 52)), 10, false,
            TestContext.Current.CancellationToken));

        Assert.Equal(VsrError.CONSUMER_GROUP_PARTITION_NOT_OWNED, fence.StatusCode);
        Assert.Equal(1, node.Requests(PollMessagesCode));
        Assert.Equal(2, node.Requests(SyncGroupCode));
    }

    private static async Task<TcpMessageStream> ConnectAsync(MockNode node)
    {
        var configuration = new IggyClientConfigurator
        {
            BaseAddress = $"127.0.0.1:{node.Port}",
            Protocol = Protocol.Tcp
        };
        var client = new TcpMessageStream(configuration, NullLoggerFactory.Instance);
        await client.ConnectAsync(TestContext.Current.CancellationToken);

        return client;
    }

    private static Task<PolledMessages> PollOnceAsync(TcpMessageStream client)
    {
        return client.PollMessagesAsync(Identifier.Numeric(1), Identifier.Numeric(2), null, Consumer.Group(3),
            PollingStrategy.Next(), 10, false, TestContext.Current.CancellationToken);
    }

    private static byte[] EmptyBatchBody(uint partitionId)
    {
        var body = new byte[40];
        BinaryPrimitives.WriteUInt32LittleEndian(body.AsSpan(0, 4), partitionId);

        return body;
    }
}
