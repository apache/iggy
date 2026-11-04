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
using Apache.Iggy.Contracts.Tcp;
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
///     The poll of one partition with <see cref="PollOptions" />: one deferred poll (code 106), the commit policy,
///     the checkpoints of a batch and the continuation bound to its partition.
/// </summary>
public sealed class PartitionPollTests
{
    private const int PollCode = CommandCodes.POLL_MESSAGES_DEFERRED_CODE;
    private const uint Partition = 1;

    /// <summary>A StoreConsumerOffset body ends with the partition id, the offset and the ack quorum.</summary>
    private const int StoredPartitionFromEnd = sizeof(uint) + sizeof(ulong) + sizeof(byte);

    private static readonly Identifier Stream = Identifier.Numeric(1);
    private static readonly Identifier Topic = Identifier.Numeric(2);

    /// <summary>The context of the golden batch, which holds the offsets 100 and 101.</summary>
    private static readonly PartitionContext Served = new(17, 9, 52);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_poll_options_when_polling_a_partition_should_send_one_deferred_poll_with_their_limits(
        bool onRead)
    {
        using var node = new MockNode();
        var polls = new ConcurrentQueue<MockRequest>();
        node.Serve(request =>
        {
            if (request.Code is PollCode or CommandCodes.POLL_MESSAGES_CODE)
            {
                polls.Enqueue(request);
                return Golden(request);
            }
            return Standalone(node, request);
        });
        using var client = await ConnectAsync(node);
        var options = new PollOptions
        {
            Count = 50,
            MaxWait = TimeSpan.FromMilliseconds(1500),
            MinCount = 2,
            MaxBytes = 4096,
            Commit = onRead ? CommitPolicy.OnRead : CommitPolicy.Manual
        };

        await client.PollMessagesAsync(Stream, Topic, Partition, Consumer.New(4), PollingStrategy.Next(), options,
            TestContext.Current.CancellationToken);

        var poll = Assert.Single(polls);
        Assert.Equal(PollCode, poll.Code);
        Assert.Equal((Partition, MessagePolling.Next, 0ul, 50u), ReadPoll(poll));
        var tail = poll.Body[^TcpContracts.DEFERRED_POLL_TAIL_SIZE..];
        Assert.Equal(onRead ? 1 : 0, poll.Body[^(TcpContracts.DEFERRED_POLL_TAIL_SIZE + 1)]);
        Assert.Equal(2u, BinaryPrimitives.ReadUInt32LittleEndian(tail.AsSpan(8)));
        Assert.Equal(4096u, BinaryPrimitives.ReadUInt32LittleEndian(tail.AsSpan(12)));
        // Each attempt sends what is left of the wait and of the request timeout, which is the wait plus 10 s.
        var waitUs = BinaryPrimitives.ReadUInt64LittleEndian(tail);
        var requestTimeoutUs = BinaryPrimitives.ReadUInt64LittleEndian(tail.AsSpan(16));
        Assert.InRange(waitUs, 1_000_000ul, 1_500_000ul);
        Assert.InRange(requestTimeoutUs - waitUs, 9_999_000ul, 10_000_000ul);
    }

    [Fact]
    public async Task given_auto_commit_when_polling_a_partition_should_refuse_it_before_sending()
    {
        using var node = new MockNode();
        node.Serve(request => Standalone(node, request));
        using var client = await ConnectAsync(node);
        var options = new PollOptions { Commit = CommitPolicy.Auto() };

        var refused = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => client.PollMessagesAsync(
            Stream, Topic, Partition, Consumer.New(4), PollingStrategy.Next(), options,
            TestContext.Current.CancellationToken));

        Assert.Equal(VsrError.INVALID_CONFIGURATION, refused.StatusCode);
        Assert.Equal(0, node.Requests(PollCode));
    }

    [Fact]
    public async Task given_a_batch_when_taking_its_checkpoints_should_name_the_position_after_each_message()
    {
        using var node = new MockNode();
        node.Serve(request => request.Code == PollCode ? Golden(request) : Standalone(node, request));
        using var client = await ConnectAsync(node);

        var polled = await PollAsync(client, PollingStrategy.Next());

        Assert.Equal(new Checkpoint(Partition, 101, Served), polled.Checkpoint);
        Assert.Equal(new Checkpoint(Partition, 100, Served), polled.CheckpointAt(0));
        Assert.Null(polled.CheckpointAt(2));
        Assert.Null(polled.CheckpointAt(-1));
    }

    [Fact]
    public async Task given_a_batch_when_continuing_should_read_after_its_last_message_of_the_same_partition_only()
    {
        using var node = new MockNode();
        var polls = new ConcurrentQueue<MockRequest>();
        node.Serve(request =>
        {
            if (request.Code != PollCode)
            {
                return Standalone(node, request);
            }
            polls.Enqueue(request);
            return Golden(request);
        });
        using var client = await ConnectAsync(node);
        var polled = await PollAsync(client, PollingStrategy.Next());

        await PollAsync(client, polled.Continuation);
        var foreign = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => client.PollMessagesAsync(
            Stream, Topic, Partition + 1, Consumer.New(4), polled.Continuation, new PollOptions(),
            TestContext.Current.CancellationToken));

        Assert.Equal(VsrError.INVALID_COMMAND, foreign.StatusCode);
        Assert.Equal(2, polls.Count);
        var continued = polls.Last();
        Assert.Equal((Partition, MessagePolling.Offset, 102ul, 1000u), ReadPoll(continued));
        Assert.Equal(Served, continued.Context);
    }

    [Theory]
    [InlineData(MessagePolling.Next, 0ul, MessagePolling.Next, 0ul)]
    [InlineData(MessagePolling.First, 0ul, MessagePolling.Offset, 0ul)]
    [InlineData(MessagePolling.Offset, 7ul, MessagePolling.Offset, 7ul)]
    [InlineData(MessagePolling.Timestamp, 9ul, MessagePolling.Timestamp, 9ul)]
    [InlineData(MessagePolling.Last, 0ul, MessagePolling.Last, 0ul)]
    public async Task given_an_empty_batch_when_continuing_should_keep_the_start_of_its_request(
        MessagePolling startKind, ulong startValue, MessagePolling continuedKind, ulong continuedValue)
    {
        using var node = new MockNode();
        node.Serve(request => request.Code == PollCode ? Empty(request) : Standalone(node, request));
        using var client = await ConnectAsync(node);

        var polled = await PollAsync(client, new PollingStrategy { Kind = startKind, Value = startValue });

        Assert.Empty(polled.Messages);
        Assert.Null(polled.Checkpoint);
        Assert.Equal((continuedKind, continuedValue, Served), (polled.Continuation.Kind, polled.Continuation.Value,
            polled.Continuation.Context));
    }

    [Fact]
    public async Task given_a_checkpoint_when_storing_and_deleting_it_should_fence_its_partition_with_its_context()
    {
        using var node = new MockNode();
        var writes = new ConcurrentQueue<(VsrOperation Operation, uint Partition, PartitionContext Context)>();
        node.Serve(request =>
        {
            switch ((VsrOperation)request.Operation)
            {
                case VsrOperation.StoreConsumerOffset:
                    writes.Enqueue((VsrOperation.StoreConsumerOffset,
                        BinaryPrimitives.ReadUInt32LittleEndian(request.Body.AsSpan(^StoredPartitionFromEnd)),
                        request.Context));
                    Assert.Equal(101ul,
                        BinaryPrimitives.ReadUInt64LittleEndian(request.Body.AsSpan(^(sizeof(ulong) + 1))));
                    return Answer(request);
                case VsrOperation.DeleteConsumerOffset:
                    writes.Enqueue((VsrOperation.DeleteConsumerOffset, Partition, request.Context));
                    return Answer(request);
            }
            return request.Code == PollCode ? Golden(request) : Standalone(node, request);
        });
        using var client = await ConnectAsync(node);
        var checkpoint = (await PollAsync(client, PollingStrategy.Next())).Checkpoint!;

        await client.StoreConsumerPositionAsync(Consumer.New(4), Stream, Topic, checkpoint,
            TestContext.Current.CancellationToken);
        await client.DeleteConsumerPositionAsync(Consumer.New(4), Stream, Topic, checkpoint,
            TestContext.Current.CancellationToken);

        Assert.Equal([(VsrOperation.StoreConsumerOffset, Partition, Served),
            (VsrOperation.DeleteConsumerOffset, Partition, Served)], writes);
    }

    private static Task<PolledMessages> PollAsync(TcpMessageStream client, PollingStrategy strategy) =>
        client.PollMessagesAsync(Stream, Topic, Partition, Consumer.New(4), strategy, new PollOptions(),
            TestContext.Current.CancellationToken);

    /// <summary>The golden batch of offsets 100 and 101 from the polled partition.</summary>
    private static byte[] Golden(MockRequest request) =>
        Reply(request.Operation, GoldenPollBody(PolledPartition(request), 100, Served));

    /// <summary>A reply without messages, under <see cref="Served" />.</summary>
    private static byte[] Empty(MockRequest request)
    {
        var body = GoldenPollBody(PolledPartition(request), 100, Served)[..DeferredPollOptions.POLL_REPLY_HEADER_SIZE];
        BinaryPrimitives.WriteUInt32LittleEndian(body.AsSpan(sizeof(uint) + sizeof(ulong)), 0);
        return Reply(request.Operation, body);
    }

    private static byte[] Standalone(MockNode node, MockRequest request) => request.Code switch
    {
        GET_CLUSTER_METADATA_CODE => StandaloneRoster(node.Port),
        CommandCodes.GET_PARTITION_ROUTE_CODE => RouteReply(request, Served, 0),
        _ => Answer(request)
    };

    private static async Task<TcpMessageStream> ConnectAsync(MockNode node)
    {
        var client = new TcpMessageStream(new IggyClientConfigurator
        {
            BaseAddress = $"127.0.0.1:{node.Port}",
            Protocol = Protocol.Tcp
        }, NullLoggerFactory.Instance);
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        await client.LoginUserAsync("user", "secret", TestContext.Current.CancellationToken);

        return client;
    }
}
