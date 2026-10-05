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
            if (request.Code == CommandCodes.GET_SEND_CONTEXT_CODE)
            {
                return Reply(OPERATION_NON_REPLICATED, Encode(Polled));
            }

            if (request.Operation == (byte)VsrOperation.SendMessages)
            {
                sends.Enqueue(request);
                return Reply(request.Operation, new byte[4]);
            }

            return Answer(request);
        });
        using var client = new TcpMessageStream(Configuration(node), NullLoggerFactory.Instance);
        await SignInAsync(client);

        await SendAsync(client);
        await SendAsync(client);

        Assert.Equal(1, node.Requests(CommandCodes.GET_SEND_CONTEXT_CODE));
        Assert.Equal(new[] { Polled, Polled }, sends.Select(send => send.Context));
    }

    [Fact]
    public async Task given_a_replaced_incarnation_when_a_cached_send_is_refused_should_resend_once_with_a_fresh_context()
    {
        using var node = new MockNode();
        var replaced = 0;
        var sends = new ConcurrentQueue<MockRequest>();
        node.Serve(request =>
        {
            if (request.Code == CommandCodes.GET_SEND_CONTEXT_CODE)
            {
                return Reply(OPERATION_NON_REPLICATED, Encode(Live()));
            }

            if (request.Operation == (byte)VsrOperation.SendMessages)
            {
                sends.Enqueue(request);
                return request.Context == Live()
                    ? Reply(request.Operation, new byte[4])
                    : Reply(request.Operation, [], VsrError.HISTORY_UNAVAILABLE);
            }

            return Answer(request);
        });
        using var client = new TcpMessageStream(Configuration(node), NullLoggerFactory.Instance);
        await SignInAsync(client);

        await SendAsync(client);
        Volatile.Write(ref replaced, 1);
        await SendAsync(client);

        Assert.Equal(2, node.Requests(CommandCodes.GET_SEND_CONTEXT_CODE));
        Assert.Equal(new[] { Polled, Polled, Replaced }, sends.Select(send => send.Context));
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
                : Answer(request);
        });
        using var client = new TcpMessageStream(Configuration(node), NullLoggerFactory.Instance);
        await SignInAsync(client);

        await SendAsync(client);
        await client.DeleteTopicAsync(Identifier.Numeric(1), Identifier.Numeric(2),
            TestContext.Current.CancellationToken);
        await SendAsync(client);

        Assert.Equal(2, node.Requests(CommandCodes.GET_SEND_CONTEXT_CODE));
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

            return Answer(request);
        });
        using var client = new TcpMessageStream(Configuration(node), NullLoggerFactory.Instance);
        await SignInAsync(client);

        var refusal = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => SendAsync(client));

        Assert.Equal(VsrError.HISTORY_UNAVAILABLE, refusal.StatusCode);
        Assert.Equal(2, Volatile.Read(ref sends));
        Assert.Equal(2, node.Requests(CommandCodes.GET_SEND_CONTEXT_CODE));
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

            return Answer(request);
        });
        using var client = new TcpMessageStream(Configuration(node), NullLoggerFactory.Instance);
        await SignInAsync(client);

        var refusal = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => StoreAsync(client));
        await StoreAsync(client);

        Assert.Equal(VsrError.HISTORY_UNAVAILABLE, refusal.StatusCode);
        Assert.True(refusal.FromServer);
        Assert.Equal(2, Volatile.Read(ref stores));
        Assert.Equal(2, node.Requests(CommandCodes.GET_CONSUMER_OFFSET_ROUTING_CODE));
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

            return Answer(request);
        });
        using var client = new TcpMessageStream(Configuration(node), NullLoggerFactory.Instance);
        await SignInAsync(client);

        await StoreAsync(client);
        await StoreAsync(client);

        Assert.Equal(2, Volatile.Read(ref stores));
        Assert.Equal(1, node.Requests(CommandCodes.GET_CONSUMER_OFFSET_ROUTING_CODE));
    }

    [Fact]
    public async Task given_plain_polls_at_different_offsets_should_capture_their_context_once()
    {
        using var node = new MockNode();
        node.Serve(Answer);
        using var client = new TcpMessageStream(Configuration(node), NullLoggerFactory.Instance);
        await SignInAsync(client);

        await PollAsync(client, PollingStrategy.Offset(0));
        await PollAsync(client, PollingStrategy.Offset(Offset));

        Assert.Equal(2, node.Requests(CommandCodes.POLL_MESSAGES_CODE));
        Assert.Equal(1, node.Requests(CommandCodes.GET_POLL_ROUTING_CODE));
    }

    /// <summary>
    ///     A recreation between the poll and the commit replaces the incarnation. The commit carries the
    ///     incarnation its messages came from, so the server refuses it instead of storing the offset in the
    ///     replacement.
    /// </summary>
    [Fact]
    public async Task given_an_after_receive_consumer_when_committing_should_store_the_polled_context()
    {
        using var node = new MockNode();
        var stores = new ConcurrentQueue<MockRequest>();
        node.Serve(request =>
        {
            if (request.Code == CommandCodes.POLL_MESSAGES_CODE)
            {
                return Reply(OPERATION_NON_REPLICATED, Convert.FromHexString(MessageBatchGoldenVectorTests.POLL_BODY));
            }

            if (request.Operation == (byte)VsrOperation.StoreConsumerOffset)
            {
                stores.Enqueue(request);
                return Reply(request.Operation, new byte[4]);
            }

            return Answer(request);
        });
        using var client = new TcpMessageStream(Configuration(node), NullLoggerFactory.Instance);
        await using var consumer = new IggyConsumer(client, new IggyConsumerConfig
        {
            StreamId = Identifier.Numeric(1),
            TopicId = Identifier.Numeric(2),
            PartitionId = PartitionId,
            Consumer = Consumer.New(1),
            PollingStrategy = PollingStrategy.Next(),
            AutoCommitMode = AutoCommitMode.AfterReceive,
            PollingIntervalMs = 0,
            Login = "iggy",
            Password = "iggy"
        }, NullLoggerFactory.Instance);
        await consumer.InitAsync(TestContext.Current.CancellationToken);

        await using (var messages = consumer.ReceiveAsync(TestContext.Current.CancellationToken)
                         .GetAsyncEnumerator(TestContext.Current.CancellationToken))
        {
            Assert.True(await messages.MoveNextAsync());
            Assert.True(await messages.MoveNextAsync());
        }

        var store = Assert.Single(stores);
        Assert.Equal(Polled, store.Context);
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

    private static Task SendAsync(TcpMessageStream client)
    {
        return client.SendMessagesAsync(Identifier.Numeric(1), Identifier.Numeric(2),
            Partitioning.PartitionId(PartitionId), [new Message(Guid.NewGuid(), "payload"u8.ToArray())],
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

    private static byte[] Encode(PartitionContext context)
    {
        var bytes = new byte[PartitionContext.ENCODED_SIZE];
        BinaryPrimitives.WriteUInt64LittleEndian(bytes, context.Incarnation);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(sizeof(ulong)), context.OwnerGeneration);
        BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(2 * sizeof(ulong)), context.MetadataOp);
        return bytes;
    }
}
