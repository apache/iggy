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
using Apache.Iggy.Consumers;
using Apache.Iggy.Contracts;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient.Implementations;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;
using Apache.Iggy.Tests.ContractsTests;
using Apache.Iggy.Tests.Utils;
using Apache.Iggy.Utils;
using Apache.Iggy.Vsr;
using Microsoft.Extensions.Logging.Abstractions;
using static Apache.Iggy.Tests.VsrTests.MockFrames;

namespace Apache.Iggy.Tests.VsrTests;

/// <summary>
///     The partition context a request carries: captured once per partition and reused until a request that
///     carries it fails. Mirrors <c>core/common/src/traits/binary_impls/messages.rs</c> and
///     <c>core/sdk/src/poll_routing.rs</c>.
/// </summary>
public sealed class PartitionContextTests
{
    private const uint PartitionId = 3;
    private const ulong Offset = 100;

    /// <summary>
    ///     A metadata op above the one of the route, as the reader raises it for an owner it did not see.
    /// </summary>
    private const ulong CallerMetadataOp = 100;

    /// <summary>The context the golden poll body carries.</summary>
    private static readonly PartitionContext Polled = new(17, 9, 52);

    private static readonly PartitionContext Replaced = new(18, 9, 53);

    [Fact]
    public async Task given_repeated_sends_to_a_partition_should_capture_its_context_once()
    {
        using var node = new MockNode();
        var sends = new ConcurrentQueue<MockRequest>();
        node.Serve(request =>
        {
            if (request.Code == CommandCodes.GET_PARTITION_ROUTE_CODE)
            {
                return RouteReply(request, Polled, 0);
            }

            if (request.Operation == (byte)VsrOperation.SendMessages)
            {
                sends.Enqueue(request);
                return Reply(request.Operation, new byte[4]);
            }

            return Standalone(node, request);
        });
        using var client = new TcpMessageStream(Configuration(node), NullLoggerFactory.Instance);
        await SignInAsync(client);

        await SendAsync(client);
        await SendAsync(client);

        Assert.Equal(1, node.Requests(CommandCodes.GET_PARTITION_ROUTE_CODE));
        Assert.Equal(new[] { Polled with { OwnerGeneration = 0 }, Polled with { OwnerGeneration = 0 } }, sends.Select(send => send.Context));
    }

    [Fact]
    public async Task given_a_replaced_incarnation_when_a_cached_send_is_refused_should_resend_once_with_a_fresh_context()
    {
        using var node = new MockNode();
        var replaced = 0;
        var sends = new ConcurrentQueue<MockRequest>();
        node.Serve(request =>
        {
            if (request.Code == CommandCodes.GET_PARTITION_ROUTE_CODE)
            {
                return RouteReply(request, Live(), 0);
            }

            if (request.Operation == (byte)VsrOperation.SendMessages)
            {
                sends.Enqueue(request);
                return request.Context == Live() with { OwnerGeneration = 0 }
                    ? Reply(request.Operation, new byte[4])
                    : Reply(request.Operation, [], VsrError.HISTORY_UNAVAILABLE);
            }

            return Standalone(node, request);
        });
        using var client = new TcpMessageStream(Configuration(node), NullLoggerFactory.Instance);
        await SignInAsync(client);

        await SendAsync(client);
        Volatile.Write(ref replaced, 1);
        await SendAsync(client);

        Assert.Equal(2, node.Requests(CommandCodes.GET_PARTITION_ROUTE_CODE));
        Assert.Equal(new[] { Polled with { OwnerGeneration = 0 }, Polled with { OwnerGeneration = 0 }, Replaced with { OwnerGeneration = 0 } }, sends.Select(send => send.Context));
        Assert.NotEqual(sends.ElementAt(1).RequestId, sends.ElementAt(2).RequestId);

        PartitionContext Live()
        {
            return Volatile.Read(ref replaced) == 0 ? Polled : Replaced;
        }
    }

    [Fact]
    public async Task given_a_delete_by_this_client_when_sending_again_should_capture_a_fresh_context()
    {
        const ulong DeleteCommit = 50;
        using var node = new MockNode();
        node.Serve(request =>
        {
            if (request.Operation == (byte)VsrOperation.DeleteTopic)
            {
                var reply = Reply(request.Operation, new byte[4]);
                BinaryPrimitives.WriteUInt64LittleEndian(reply.AsSpan(VsrHeader.REPLY_COMMIT_OFFSET), DeleteCommit);
                return reply;
            }

            return request.Operation == (byte)VsrOperation.SendMessages
                ? Reply(request.Operation, new byte[4])
                : Standalone(node, request);
        });
        using var client = new TcpMessageStream(Configuration(node), NullLoggerFactory.Instance);
        await SignInAsync(client);

        await SendAsync(client);
        await client.DeleteTopicAsync(Identifier.Numeric(1), Identifier.Numeric(2),
            TestContext.Current.CancellationToken);
        await SendAsync(client);

        Assert.Equal(2, node.Requests(CommandCodes.GET_PARTITION_ROUTE_CODE));
    }

    [Fact]
    public async Task given_repeated_history_refusals_when_sending_should_refresh_only_once()
    {
        using var node = new MockNode();
        var sends = 0;
        node.Serve(request =>
        {
            if (request.Operation == (byte)VsrOperation.SendMessages)
            {
                Interlocked.Increment(ref sends);
                return Reply(request.Operation, [], VsrError.HISTORY_UNAVAILABLE);
            }

            return Standalone(node, request);
        });
        using var client = new TcpMessageStream(Configuration(node), NullLoggerFactory.Instance);
        await SignInAsync(client);

        var refusal = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => SendAsync(client));

        Assert.Equal(VsrError.HISTORY_UNAVAILABLE, refusal.StatusCode);
        Assert.Equal(2, Volatile.Read(ref sends));
        Assert.Equal(2, node.Requests(CommandCodes.GET_PARTITION_ROUTE_CODE));
    }

    /// <summary>
    ///     A first history refusal is the server's verdict: it refuses the request before admission. Only a
    ///     refusal that follows an uncertain replay leaves the outcome unknown.
    /// </summary>
    [Fact]
    public async Task given_a_history_refusal_when_storing_an_offset_should_report_it_and_capture_a_fresh_context()
    {
        using var node = new MockNode();
        var stores = 0;
        node.Serve(request =>
        {
            if (request.Operation == (byte)VsrOperation.StoreConsumerOffset)
            {
                return Interlocked.Increment(ref stores) == 1
                    ? Reply(request.Operation, [], VsrError.HISTORY_UNAVAILABLE)
                    : Reply(request.Operation, new byte[4]);
            }

            return Standalone(node, request);
        });
        using var client = new TcpMessageStream(Configuration(node), NullLoggerFactory.Instance);
        await SignInAsync(client);

        var refusal = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => StoreAsync(client));
        await StoreAsync(client);

        Assert.Equal(VsrError.HISTORY_UNAVAILABLE, refusal.StatusCode);
        Assert.True(refusal.FromServer);
        Assert.Equal(2, Volatile.Read(ref stores));
        Assert.Equal(2, node.Requests(CommandCodes.GET_PARTITION_ROUTE_CODE));
    }

    [Fact]
    public async Task given_repeated_offset_stores_should_capture_their_context_once()
    {
        using var node = new MockNode();
        var stores = 0;
        node.Serve(request =>
        {
            if (request.Operation == (byte)VsrOperation.StoreConsumerOffset)
            {
                Interlocked.Increment(ref stores);
                return Reply(request.Operation, new byte[4]);
            }

            return Standalone(node, request);
        });
        using var client = new TcpMessageStream(Configuration(node), NullLoggerFactory.Instance);
        await SignInAsync(client);

        await StoreAsync(client);
        await StoreAsync(client);

        Assert.Equal(2, Volatile.Read(ref stores));
        Assert.Equal(1, node.Requests(CommandCodes.GET_PARTITION_ROUTE_CODE));
    }

    /// <summary>
    ///     The server refuses an offset write of incarnation 0. Such a context takes only the incarnation of the route,
    ///     so the write keeps the owner generation and the metadata op of the caller.
    /// </summary>
    [Theory]
    [InlineData(9ul)]
    [InlineData(0ul)]
    public async Task given_a_context_without_an_incarnation_when_storing_an_offset_should_take_only_the_incarnation_of_the_route(
        ulong owner)
    {
        using var node = new MockNode();
        var stores = new ConcurrentQueue<MockRequest>();
        node.Serve(request =>
        {
            if (request.Operation == (byte)VsrOperation.StoreConsumerOffset)
            {
                stores.Enqueue(request);
                return Reply(request.Operation, new byte[4]);
            }

            return Standalone(node, request);
        });
        using var client = new TcpMessageStream(Configuration(node), NullLoggerFactory.Instance);
        await SignInAsync(client);
        var caller = new PartitionContext(0, owner, CallerMetadataOp);

        await client.StoreOffsetAsync(Consumer.New(1), Identifier.Numeric(1), Identifier.Numeric(2), Offset,
            PartitionId, caller, TestContext.Current.CancellationToken);

        Assert.Equal(caller with { Incarnation = AnsweredRoute.Incarnation }, Assert.Single(stores).Context);
    }

    /// <summary>
    ///     A fully zero context carries nothing, so a request of one partition history takes the context of the
    ///     route, as a request without one does. A group write under owner 0 and metadata op 0 binds to no installed
    ///     owner.
    /// </summary>
    [Theory]
    [InlineData(CommandCodes.STORE_CONSUMER_OFFSET_CODE)]
    [InlineData(CommandCodes.DELETE_CONSUMER_OFFSET_CODE)]
    [InlineData(CommandCodes.POLL_MESSAGES_CODE)]
    public async Task given_a_fully_zero_context_when_sending_a_partition_request_should_take_the_context_of_the_route(
        int code)
    {
        using var node = new MockNode();
        var sent = new ConcurrentQueue<MockRequest>();
        node.Serve(request =>
        {
            if (request.Code == CommandCodes.POLL_MESSAGES_CODE
                || request.Operation == (byte)VsrOperation.StoreConsumerOffset
                || request.Operation == (byte)VsrOperation.DeleteConsumerOffset)
            {
                sent.Enqueue(request);
            }

            return Standalone(node, request);
        });
        using var client = new TcpMessageStream(Configuration(node), NullLoggerFactory.Instance);
        await SignInAsync(client);
        var group = Consumer.Group(1);
        var token = TestContext.Current.CancellationToken;

        await (code switch
        {
            CommandCodes.STORE_CONSUMER_OFFSET_CODE => client.StoreOffsetAsync(group, Identifier.Numeric(1),
                Identifier.Numeric(2), Offset, PartitionId, default(PartitionContext), token),
            CommandCodes.DELETE_CONSUMER_OFFSET_CODE => client.DeleteOffsetAsync(group, Identifier.Numeric(1),
                Identifier.Numeric(2), PartitionId, default(PartitionContext), token),
            _ => client.PollMessagesAsync(Identifier.Numeric(1), Identifier.Numeric(2), PartitionId, group,
                PollingStrategy.Offset(Offset).WithContext(default), 10, false, token)
        });

        Assert.Equal(AnsweredRoute, Assert.Single(sent).Context);
    }

    /// <summary>
    ///     A route without an incarnation names a partition that is not ready. The lookup goes on as after a
    ///     transient refusal, so the store waits for a route with an incarnation instead of naming incarnation 0.
    /// </summary>
    [Fact]
    public async Task given_a_route_without_an_incarnation_when_storing_an_offset_should_look_it_up_again_until_it_has_one()
    {
        using var node = new MockNode();
        var lookups = 0;
        var stores = new ConcurrentQueue<MockRequest>();
        node.Serve(request =>
        {
            if (request.Code == CommandCodes.GET_PARTITION_ROUTE_CODE && Interlocked.Increment(ref lookups) == 1)
            {
                return RouteReply(request, AnsweredRoute with { Incarnation = 0 }, 0);
            }

            if (request.Operation == (byte)VsrOperation.StoreConsumerOffset)
            {
                stores.Enqueue(request);
            }

            return Standalone(node, request);
        });
        using var client = new TcpMessageStream(Configuration(node), NullLoggerFactory.Instance);
        await SignInAsync(client);

        await StoreAsync(client);

        Assert.Equal(AnsweredRoute, Assert.Single(stores).Context);
        Assert.Equal(2, node.Requests(CommandCodes.GET_PARTITION_ROUTE_CODE));
    }

    [Fact]
    public async Task given_plain_polls_at_different_offsets_should_capture_their_context_once()
    {
        using var node = new MockNode();
        node.Serve(request => Standalone(node, request));
        using var client = new TcpMessageStream(Configuration(node), NullLoggerFactory.Instance);
        await SignInAsync(client);

        await PollAsync(client, PollingStrategy.Offset(0));
        await PollAsync(client, PollingStrategy.Offset(Offset));

        Assert.Equal(2, node.Requests(CommandCodes.POLL_MESSAGES_CODE));
        Assert.Equal(1, node.Requests(CommandCodes.GET_PARTITION_ROUTE_CODE));
    }

    [Fact]
    public async Task given_a_send_refused_for_another_reason_should_keep_the_cached_context()
    {
        using var node = new MockNode();
        var sends = 0;
        node.Serve(request =>
        {
            if (request.Operation == (byte)VsrOperation.SendMessages)
            {
                return Interlocked.Increment(ref sends) == 1
                    ? Reply(request.Operation, [], VsrError.TOPIC_ID_NOT_FOUND)
                    : Reply(request.Operation, new byte[4]);
            }

            return Standalone(node, request);
        });
        using var client = new TcpMessageStream(Configuration(node), NullLoggerFactory.Instance);
        await SignInAsync(client);

        await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => SendAsync(client));
        await SendAsync(client);

        Assert.Equal(2, Volatile.Read(ref sends));
        Assert.Equal(1, node.Requests(CommandCodes.GET_PARTITION_ROUTE_CODE));
    }

    [Fact]
    public async Task given_a_history_refusal_when_sending_should_preserve_other_partition_routes()
    {
        using var node = new MockNode();
        var refused = 0;
        node.Serve(request =>
        {
            if (request.Operation == (byte)VsrOperation.SendMessages)
            {
                return Interlocked.Exchange(ref refused, 0) == 1
                    ? Reply(request.Operation, [], VsrError.HISTORY_UNAVAILABLE)
                    : Reply(request.Operation, new byte[4]);
            }

            return Standalone(node, request);
        });
        using var client = new TcpMessageStream(Configuration(node), NullLoggerFactory.Instance);
        await SignInAsync(client);
        await SendAsync(client, PartitionId);
        await SendAsync(client, PartitionId + 1);

        Volatile.Write(ref refused, 1);
        await SendAsync(client, PartitionId);
        await SendAsync(client, PartitionId + 1);

        Assert.Equal(3, node.Requests(CommandCodes.GET_PARTITION_ROUTE_CODE));
    }

    [Fact]
    public async Task given_a_metadata_commit_when_sending_again_should_refresh_the_shared_route_floor()
    {
        const ulong UpdateCommit = 50;
        using var node = new MockNode();
        node.Serve(request =>
        {
            if (request.Operation == (byte)VsrOperation.UpdateResource)
            {
                var reply = Reply(request.Operation, new byte[4]);
                BinaryPrimitives.WriteUInt64LittleEndian(reply.AsSpan(VsrHeader.REPLY_COMMIT_OFFSET), UpdateCommit);
                return reply;
            }

            return request.Operation == (byte)VsrOperation.SendMessages
                ? Reply(request.Operation, new byte[4])
                : Standalone(node, request);
        });
        using var client = new TcpMessageStream(Configuration(node), NullLoggerFactory.Instance);
        await SignInAsync(client);

        await SendAsync(client);
        await client.UpdateStreamAsync(Identifier.Numeric(1), new StreamUpdate { Name = "renamed" },
            TestContext.Current.CancellationToken);
        await SendAsync(client);

        Assert.Equal(2, node.Requests(CommandCodes.GET_PARTITION_ROUTE_CODE));
    }

    [Fact]
    public async Task given_a_stream_and_a_topic_created_by_this_client_when_sending_again_should_keep_the_context()
    {
        using var node = new MockNode();
        node.Serve(request => request.Operation switch
        {
            // A replicated reply leads with an empty committed result section.
            (byte)VsrOperation.CreateStream => Reply(request.Operation,
                [.. new byte[4], .. BinaryFactory.CreateStreamPayload(1, 0, "created", 0, 0, 0)]),
            (byte)VsrOperation.CreateTopic or (byte)VsrOperation.SendMessages => Reply(request.Operation, new byte[4]),
            _ => Standalone(node, request)
        });
        using var client = new TcpMessageStream(Configuration(node), NullLoggerFactory.Instance);
        await SignInAsync(client);

        await SendAsync(client);
        await client.CreateStreamAsync("created", token: TestContext.Current.CancellationToken);
        await client.CreateTopicAsync(Identifier.Numeric(1), "created", 1,
            token: TestContext.Current.CancellationToken);
        await SendAsync(client);

        Assert.Equal(1, node.Requests(CommandCodes.GET_PARTITION_ROUTE_CODE));
    }

    [Theory]
    [InlineData(Enums.Partitioning.Balanced)]
    [InlineData(Enums.Partitioning.MessageKey)]
    public async Task given_a_raw_send_without_a_partition_id_should_refuse_it_before_sending(
        Enums.Partitioning partitioning)
    {
        using var node = new MockNode();
        node.Serve(request => Standalone(node, request));
        using var client = new TcpMessageStream(Configuration(node), NullLoggerFactory.Instance);
        await SignInAsync(client);
        byte[] body = [0, 0, 0, 0, 1, 4, 1, 0, 0, 0, 1, 4, 2, 0, 0, 0, (byte)partitioning, 1, 7, 0, 0, 0, 0];

        var refusal = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() =>
            client.SendBinaryRequestAsync(CommandCodes.SEND_MESSAGES_CODE, body,
                TestContext.Current.CancellationToken));

        Assert.Equal(VsrError.FEATURE_UNAVAILABLE, refusal.StatusCode);
        Assert.False(refusal.FromServer);
        Assert.Equal(0, node.Requests(CommandCodes.SEND_MESSAGES_CODE));
        Assert.Equal(0, node.Requests(CommandCodes.GET_PARTITION_ROUTE_CODE));
    }

    [Fact]
    public async Task given_a_caller_context_on_a_standalone_poll_should_preserve_it_while_routing()
    {
        using var node = new MockNode();
        var polls = new ConcurrentQueue<MockRequest>();
        node.Serve(request =>
        {
            if (request.Code == CommandCodes.POLL_MESSAGES_CODE)
            {
                polls.Enqueue(request);
            }

            return Standalone(node, request);
        });
        using var client = new TcpMessageStream(Configuration(node), NullLoggerFactory.Instance);
        await SignInAsync(client);

        await PollAsync(client, PollingStrategy.Offset(Offset).WithContext(Replaced));

        Assert.Equal(Replaced, Assert.Single(polls).Context);
        Assert.Equal(1, node.Requests(CommandCodes.GET_PARTITION_ROUTE_CODE));
    }

    [Fact]
    public async Task given_lifecycle_busy_refusals_should_resend_as_new_requests_with_growing_pauses()
    {
        using var node = new MockNode();
        var deletes = new ConcurrentQueue<(ulong RequestId, long At)>();
        node.Serve(request =>
        {
            if (request.Operation == (byte)VsrOperation.DeleteStream)
            {
                deletes.Enqueue((request.RequestId, Environment.TickCount64));
                return deletes.Count < 3
                    ? Reply(request.Operation, [], VsrError.LIFECYCLE_BUSY)
                    : Reply(request.Operation, new byte[4]);
            }

            return Standalone(node, request);
        });
        using var client = new TcpMessageStream(Configuration(node), NullLoggerFactory.Instance);
        await SignInAsync(client);

        await client.DeleteStreamAsync(Identifier.Numeric(1), TestContext.Current.CancellationToken);

        var attempts = deletes.ToArray();
        Assert.Equal(3, attempts.Select(attempt => attempt.RequestId).Distinct().Count());
        // TickCount64 can round a pause down by its granularity.
        Assert.InRange(attempts[1].At - attempts[0].At, 40, long.MaxValue);
        Assert.InRange(attempts[2].At - attempts[1].At, 90, long.MaxValue);
    }

    [Fact]
    public async Task given_a_lifecycle_busy_retry_when_pausing_should_let_other_requests_through()
    {
        using var node = new MockNode();
        var refused = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        node.Serve(request =>
        {
            if (request.Operation != (byte)VsrOperation.DeleteStream)
            {
                return Standalone(node, request);
            }

            if (node.Pings > 0)
            {
                return Reply(request.Operation, new byte[4]);
            }

            refused.TrySetResult();
            return Reply(request.Operation, [], VsrError.LIFECYCLE_BUSY);
        });
        using var client = new TcpMessageStream(Configuration(node), NullLoggerFactory.Instance);
        await SignInAsync(client);

        var delete = client.DeleteStreamAsync(Identifier.Numeric(1), TestContext.Current.CancellationToken);
        await refused.Task.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        await client.PingAsync(TestContext.Current.CancellationToken)
            .WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        await delete.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
    }

    /// <summary>The roster read that routes polls and offset writes finds a standalone server.</summary>
    private static byte[] Standalone(MockNode node, MockRequest request)
    {
        return request.Code == GET_CLUSTER_METADATA_CODE ? StandaloneRoster(node.Port) : Answer(request);
    }

    private static IggyClientConfigurator Configuration(MockNode node)
    {
        return new IggyClientConfigurator
        {
            BaseAddress = $"127.0.0.1:{node.Port}",
            Protocol = Enums.Protocol.Tcp,
            HeartbeatInterval = TimeSpan.FromHours(1)
        };
    }

    private static async Task SignInAsync(TcpMessageStream client)
    {
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        await client.LoginUserAsync("iggy", "iggy", TestContext.Current.CancellationToken);
    }

    private static Task SendAsync(TcpMessageStream client, uint partitionId = PartitionId)
    {
        return client.SendMessagesAsync(Identifier.Numeric(1), Identifier.Numeric(2),
            Partitioning.PartitionId(partitionId), [new Message(Guid.NewGuid(), "payload"u8.ToArray())],
            TestContext.Current.CancellationToken);
    }

    private static Task StoreAsync(TcpMessageStream client)
    {
        return client.StoreOffsetAsync(Consumer.New(1), Identifier.Numeric(1), Identifier.Numeric(2), Offset,
            PartitionId, TestContext.Current.CancellationToken);
    }

    private static Task PollAsync(TcpMessageStream client, PollingStrategy strategy)
    {
        return client.PollMessagesAsync(Identifier.Numeric(1), Identifier.Numeric(2), PartitionId, Consumer.New(1),
            strategy, 10, false, TestContext.Current.CancellationToken);
    }

}
