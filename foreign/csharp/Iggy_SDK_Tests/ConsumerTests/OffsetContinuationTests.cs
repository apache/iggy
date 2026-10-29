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
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient.Implementations;
using Apache.Iggy.Kinds;
using Apache.Iggy.Tests.VsrTests;
using Apache.Iggy.Utils;
using Apache.Iggy.Vsr;
using Microsoft.Extensions.Logging.Abstractions;
using static Apache.Iggy.Tests.VsrTests.MockFrames;

namespace Apache.Iggy.Tests.ConsumerTests;

/// <summary>
///     The configured polling strategy only says where reading a partition starts. Every later poll of the partition
///     continues after the last message it delivered, under the context of that batch, so a recreated partition
///     refuses the poll instead of serving a later offset of its new incarnation. Mirrors the next offsets of
///     <c>core/sdk/src/clients/consumer.rs</c>.
/// </summary>
public sealed class OffsetContinuationTests
{
    private const uint PartitionId = 3;
    private const uint OtherPartitionId = 4;
    private static readonly PartitionContext Original = new(17, 9, 52);
    private static readonly PartitionContext Recreated = new(18, 9, 60);
    private static readonly PartitionContext Reassigned = new(17, 10, 60);
    private static readonly PartitionContext Other = new(21, 9, 52);

    [Theory]
    [InlineData(MessagePolling.Offset, false)]
    [InlineData(MessagePolling.Offset, true)]
    [InlineData(MessagePolling.First, false)]
    [InlineData(MessagePolling.First, true)]
    public async Task given_a_start_strategy_when_a_batch_arrives_should_continue_after_it_under_its_context(
        MessagePolling kind, bool rented)
    {
        using var server = new PartitionServer(new Dictionary<uint, PartitionContext> { [PartitionId] = Original });
        using var client = Client(server);
        await using var consumer = await StartAsync(client, Consumer.New(1), PartitionId,
            kind == MessagePolling.First ? PollingStrategy.First() : PollingStrategy.Offset(0));

        var received = await ReceiveAsync(consumer, rented, 4);

        Assert.Equal([(PartitionId, 0ul, Original), (PartitionId, 1ul, Original), (PartitionId, 2ul, Original),
            (PartitionId, 3ul, Original)], received);
        Assert.Equal([new Poll(PartitionId, kind, 0, PartitionServer.Route(Original)),
            new Poll(PartitionId, MessagePolling.Offset, 2, Original)], server.Polls.Take(2));
    }

    [Fact]
    public async Task given_the_next_strategy_when_a_batch_arrives_should_leave_the_position_to_the_server()
    {
        using var server = new PartitionServer(new Dictionary<uint, PartitionContext> { [PartitionId] = Original });
        using var client = Client(server);
        await using var consumer = await StartAsync(client, Consumer.New(1), PartitionId, PollingStrategy.Next());

        var received = await ReceiveAsync(consumer, false, 4);

        Assert.Equal([0ul, 1ul, 2ul, 3ul], received.Select(message => message.Offset));
        Assert.Equal([new Poll(PartitionId, MessagePolling.Next, 0, PartitionServer.Route(Original)),
            new Poll(PartitionId, MessagePolling.Next, 0, PartitionServer.Route(Original))], server.Polls.Take(2));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_a_recreated_partition_when_its_continuation_is_refused_should_report_it_and_start_over(
        bool rented)
    {
        using var server = new PartitionServer(new Dictionary<uint, PartitionContext> { [PartitionId] = Original });
        server.ReplaceAfterNextBatch(PartitionId, Recreated);
        using var client = Client(server);
        await using var consumer = await StartAsync(client, Consumer.New(1), PartitionId, PollingStrategy.Offset(0));
        var errors = new ConcurrentQueue<Exception>();
        consumer.SubscribeToErrorEvents(error =>
        {
            errors.Enqueue(error.Exception);
            return Task.CompletedTask;
        });

        var received = await ReceiveAsync(consumer, rented, 4);

        Assert.Equal([(PartitionId, 0ul, Original), (PartitionId, 1ul, Original), (PartitionId, 0ul, Recreated),
            (PartitionId, 1ul, Recreated)], received);
        Assert.Equal([new Poll(PartitionId, MessagePolling.Offset, 2, Original),
                new Poll(PartitionId, MessagePolling.Offset, 0, PartitionServer.Route(Recreated))],
            server.Polls.Skip(1).Take(2));
        Assert.Equal(VsrError.HISTORY_UNAVAILABLE,
            Assert.IsType<IggyInvalidStatusCodeException>(Assert.Single(errors)).StatusCode);
    }

    /// <summary>
    ///     Another member may have read and committed the partition while it was away, so the member starts it over
    ///     from the configured strategy, as the Rust consumer does.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_a_reassigned_partition_when_its_continuation_is_fenced_should_report_it_and_start_over(
        bool rented)
    {
        using var server = new PartitionServer(new Dictionary<uint, PartitionContext> { [PartitionId] = Original });
        server.ReplaceAfterNextBatch(PartitionId, Reassigned);
        using var client = Client(server);
        await using var consumer = await StartAsync(client, Consumer.Group("group"), null, PollingStrategy.Offset(0));
        var errors = new ConcurrentQueue<Exception>();
        consumer.SubscribeToErrorEvents(error =>
        {
            errors.Enqueue(error.Exception);
            return Task.CompletedTask;
        });

        var received = await ReceiveAsync(consumer, rented, 4);

        Assert.Equal([(PartitionId, 0ul, Original), (PartitionId, 1ul, Original), (PartitionId, 0ul, Reassigned),
            (PartitionId, 1ul, Reassigned)], received);
        Assert.Equal([new Poll(PartitionId, MessagePolling.Offset, 2, Fence(Original)),
                new Poll(PartitionId, MessagePolling.Offset, 0, Fence(PartitionServer.Route(Reassigned)))],
            server.Polls.Skip(1).Take(2));
        Assert.Equal(VsrError.CONSUMER_GROUP_PARTITION_NOT_OWNED,
            Assert.IsType<IggyInvalidStatusCodeException>(Assert.Single(errors)).StatusCode);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_a_group_member_when_continuing_should_keep_each_partition_on_its_own_offset(bool rented)
    {
        using var server = new PartitionServer(new Dictionary<uint, PartitionContext>
        {
            [PartitionId] = Original,
            [OtherPartitionId] = Other
        });
        using var client = Client(server);
        await using var consumer = await StartAsync(client, Consumer.Group("group"), null, PollingStrategy.Offset(0));

        var received = await ReceiveAsync(consumer, rented, 8);

        Assert.Equal([(PartitionId, 0ul), (PartitionId, 1ul), (OtherPartitionId, 0ul), (OtherPartitionId, 1ul),
                (PartitionId, 2ul), (PartitionId, 3ul), (OtherPartitionId, 2ul), (OtherPartitionId, 3ul)],
            received.Select(message => (message.PartitionId, message.Offset)));
        Assert.Equal([new Poll(PartitionId, MessagePolling.Offset, 0, Fence(PartitionServer.Route(Original))),
            new Poll(OtherPartitionId, MessagePolling.Offset, 0, Fence(PartitionServer.Route(Other))),
            new Poll(PartitionId, MessagePolling.Offset, 2, Fence(Original)),
            new Poll(OtherPartitionId, MessagePolling.Offset, 2, Fence(Other))], server.Polls.Take(4));
        Assert.Equal([[PartitionId, OtherPartitionId], [PartitionId, OtherPartitionId]],
            server.FetchedPartitions.Take(2));
    }

    /// <summary>A fetch fences each target with its incarnation and owner, not with a metadata operation.</summary>
    private static PartitionContext Fence(PartitionContext context)
    {
        return context with { MetadataOp = 0 };
    }

    private static TcpMessageStream Client(PartitionServer server)
    {
        return new TcpMessageStream(new IggyClientConfigurator
        {
            BaseAddress = $"127.0.0.1:{server.Port}",
            Protocol = Protocol.Tcp,
            HeartbeatInterval = TimeSpan.FromHours(1)
        }, NullLoggerFactory.Instance);
    }

    private static async Task<IggyConsumer> StartAsync(TcpMessageStream client, Consumer consumer, uint? partitionId,
        PollingStrategy strategy)
    {
        var started = new IggyConsumer(client, new IggyConsumerConfig
        {
            StreamId = Identifier.Numeric(1),
            TopicId = Identifier.Numeric(2),
            PartitionId = partitionId,
            Consumer = consumer,
            PollingStrategy = strategy,
            BatchSize = 10,
            AutoCommitMode = AutoCommitMode.Disabled,
            Login = "iggy",
            Password = "iggy"
        }, NullLoggerFactory.Instance);
        await started.InitAsync(TestContext.Current.CancellationToken);

        return started;
    }

    /// <summary>
    ///     The first <paramref name="count" /> messages, or fewer when the consumer stalls until the timeout.
    /// </summary>
    internal static async Task<List<(uint PartitionId, ulong Offset, PartitionContext Context)>> ReceiveAsync(
        IggyConsumer consumer, bool rented, int count)
    {
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cancellation.CancelAfter(TimeSpan.FromSeconds(5));
        var received = new List<(uint PartitionId, ulong Offset, PartitionContext Context)>();
        try
        {
            if (rented)
            {
                await foreach (var message in consumer.ReceiveRentedAsync(cancellation.Token))
                {
                    using (message)
                    {
                        received.Add((message.PartitionId, message.CurrentOffset, message.Context));
                    }

                    if (received.Count == count)
                    {
                        break;
                    }
                }
            }
            else
            {
                await foreach (var message in consumer.ReceiveAsync(cancellation.Token))
                {
                    received.Add((message.PartitionId, message.CurrentOffset, message.Context));
                    if (received.Count == count)
                    {
                        break;
                    }
                }
            }
        }
        catch (OperationCanceledException) when (!TestContext.Current.CancellationToken.IsCancellationRequested)
        {
            // The timeout ends a stalled consumer, and the assertions then show what it delivered.
        }

        return received;
    }

    /// <summary>A poll as the node saw it: the partition, the strategy and the context in the request header.</summary>
    private readonly record struct Poll(uint PartitionId, MessagePolling Kind, ulong Value, PartitionContext Context);

    /// <summary>
    ///     A standalone node that serves two messages of a partition from where a poll asks, under the partition's
    ///     live context. It refuses a poll stamped with another incarnation with status 87 and one stamped with
    ///     another owner with status 5009. A route reports the live context at an older metadata operation, so a poll
    ///     stamped with the context of its batch tells itself apart from one that took the context of its route.
    ///     A fetch serves each target as a poll of it, and a refused target fails the whole fetch, as the server does
    ///     before its first read.
    /// </summary>
    private sealed class PartitionServer : IDisposable
    {
        private const ulong RouteMetadataOp = 50;

        /// <summary>How many messages the golden poll body holds.</summary>
        private const ulong MessagesPerBatch = 2;

        private readonly ConcurrentDictionary<uint, ulong> _cursors = new();
        private readonly ConcurrentDictionary<uint, PartitionContext> _live;
        private readonly MockNode _node = new();
        private readonly ConcurrentDictionary<uint, PartitionContext> _replacements = new();

        internal PartitionServer(Dictionary<uint, PartitionContext> live)
        {
            _live = new ConcurrentDictionary<uint, PartitionContext>(live);
            _node.Serve(Handle);
        }

        internal ushort Port => _node.Port;

        internal ConcurrentQueue<Poll> Polls { get; } = new();

        /// <summary>The partitions of each fetch, in target order.</summary>
        internal ConcurrentQueue<uint[]> FetchedPartitions { get; } = new();

        public void Dispose()
        {
            _node.Dispose();
        }

        internal static PartitionContext Route(PartitionContext live)
        {
            return live with { OwnerGeneration = 0, MetadataOp = RouteMetadataOp };
        }

        /// <summary>
        ///     Replaces the context of the partition once its next batch is served, as a recreation or a reassignment
        ///     would.
        /// </summary>
        internal void ReplaceAfterNextBatch(uint partitionId, PartitionContext replacement)
        {
            _replacements[partitionId] = replacement;
        }

        private byte[] Handle(MockRequest request)
        {
            return request.Code switch
            {
                GET_CLUSTER_METADATA_CODE => StandaloneRoster(_node.Port),
                CommandCodes.GET_PARTITION_ROUTE_CODE => RouteReply(request,
                    Route(_live[BinaryPrimitives.ReadUInt32LittleEndian(request.Body.AsSpan(request.Body.Length - sizeof(uint)))]),
                    0, (uint)_live.Count),
                CommandCodes.SYNC_CONSUMER_GROUP_CODE => Reply(OPERATION_NON_REPLICATED,
                    AssignmentBody(1, [.. _live.Keys.Order()])),
                CommandCodes.POLL_MESSAGES_CODE or CommandCodes.POLL_MESSAGES_DEFERRED_CODE => Serve(request),
                CommandCodes.FETCH_MESSAGES_CODE => ServeFetch(request),
                _ => request.Operation >= (byte)VsrOperation.CreateStream
                    ? Reply(request.Operation, new byte[4])
                    : Answer(request)
            };
        }

        private byte[] Serve(MockRequest request)
        {
            var (partitionId, kind, value, _) = ReadPoll(request);
            var poll = new Poll(partitionId, kind, value, request.Context);
            Polls.Enqueue(poll);
            if (Refusal(poll) is { } status)
            {
                return Reply(request.Operation, [], status);
            }

            var (first, live) = Read(poll);
            return Reply(request.Operation, GoldenPollBody(poll.PartitionId, first, live));
        }

        private byte[] ServeFetch(MockRequest request)
        {
            var polls = ReadFetch(request).Targets.Select(target => new Poll(target.PartitionId, target.Kind,
                target.Value, new PartitionContext(target.Incarnation, target.OwnerGeneration, 0))).ToList();
            FetchedPartitions.Enqueue([.. polls.Select(poll => poll.PartitionId)]);
            foreach (var poll in polls)
            {
                Polls.Enqueue(poll);
            }

            if (polls.Select(Refusal).FirstOrDefault(status => status is not null) is { } refused)
            {
                return Reply(request.Operation, [], refused);
            }

            return Reply(request.Operation, FetchBody([
                .. polls.Select(poll =>
                {
                    var (first, live) = Read(poll);
                    return GoldenSection(poll.PartitionId, first, live);
                })
            ]));
        }

        private uint? Refusal(Poll poll)
        {
            var live = _live[poll.PartitionId];
            if (poll.Context.Incarnation != live.Incarnation)
            {
                return VsrError.HISTORY_UNAVAILABLE;
            }

            return poll.Context.OwnerGeneration != 0 && poll.Context.OwnerGeneration != live.OwnerGeneration
                ? VsrError.CONSUMER_GROUP_PARTITION_NOT_OWNED
                : null;
        }

        /// <summary>The first offset the poll reads and the context it reads under, then moves the partition on.</summary>
        private (ulong First, PartitionContext Live) Read(Poll poll)
        {
            var live = _live[poll.PartitionId];
            var first = poll.Kind switch
            {
                MessagePolling.Offset => poll.Value,
                MessagePolling.Next => _cursors.GetValueOrDefault(poll.PartitionId),
                _ => 0ul
            };
            _cursors[poll.PartitionId] = first + MessagesPerBatch;
            if (_replacements.TryRemove(poll.PartitionId, out var replacement))
            {
                _live[poll.PartitionId] = replacement;
            }

            return (first, live);
        }
    }
}
