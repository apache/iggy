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
///     Fetches (code 109) against one node, which parks a fetch by not answering it. Mirrors the fetch of
///     core/sdk/src/driver.rs.
/// </summary>
public sealed class FetchMessagesTests
{
    private const int FetchCode = CommandCodes.FETCH_MESSAGES_CODE;
    private const int PollCode = CommandCodes.POLL_MESSAGES_DEFERRED_CODE;
    private const int CancelCode = CommandCodes.CANCEL_DEFERRED_REQUEST_CODE;

    /// <summary>A frame limit that binds below the byte limit of the options, also with 1000 targets.</summary>
    private const int SmallFrameLimit = 100_000;

    /// <summary>The partitions a route names, so that every partition of a test exists.</summary>
    private const uint TopicPartitions = TcpContracts.MAX_PARTITIONS_PER_REQUEST + 1;

    private static readonly Identifier Stream = Identifier.Numeric(1);
    private static readonly Identifier Topic = Identifier.Numeric(2);
    private static readonly PartitionContext Route = new(17, 0, 1);
    private static readonly PartitionContext Served = new(17, 9, 52);

    /// <summary>Longer than any test, so a fetch the node does not answer stays parked until the test ends it.</summary>
    private static readonly DeferredPollOptions LongWait = new()
    {
        MaxWait = TimeSpan.FromSeconds(30),
        RequestTimeout = TimeSpan.FromSeconds(31)
    };

    /// <summary>How long a negative check waits for a request that must not come.</summary>
    private static readonly TimeSpan SettleDelay = TimeSpan.FromMilliseconds(200);

    private static readonly TimeSpan PromptLimit = TimeSpan.FromSeconds(5);

    /// <summary>A refusal that comes later than the request timeout of a retry less the reply margin.</summary>
    private static readonly TimeSpan SlowRefusal = TimeSpan.FromMilliseconds(600);

    private static readonly DeferredPollOptions ShortTimeout = new()
    {
        MaxWait = TimeSpan.Zero,
        RequestTimeout = TimeSpan.FromMicroseconds(DeferredPollOptions.REPLY_MARGIN_US) + TimeSpan.FromMilliseconds(500)
    };

    /// <summary>
    ///     The fetch goes to the primary of its first target with header context zero. A target without a fence
    ///     takes the context of its route, and each target gets the byte limit of the options.
    /// </summary>
    [Fact]
    public async Task given_targets_with_and_without_a_fence_when_fetched_should_send_one_request_with_context_zero()
    {
        using var node = new MockNode();
        var fetches = new ConcurrentQueue<MockRequest>();
        node.Serve(request =>
        {
            if (request.Code != FetchCode)
            {
                return Standalone(node, request);
            }

            fetches.Enqueue(request);
            return Reply(request.Operation, FetchBody(GoldenSection(1, 0, Served), EmptySection(2, 7, Served)));
        });
        using var client = await ConnectAsync(node);
        var fenced = new PartitionContext(18, 4, 0);

        using var reply = await client.FetchMessagesAsync(Stream, Topic,
            [(1, PollingStrategy.Offset(0)), (2, PollingStrategy.Next().WithContext(fenced))], Consumer.Group(3), 10,
            false, LongWait, TestContext.Current.CancellationToken);

        var request = Assert.Single(fetches);
        Assert.Equal(default, request.Context);
        var fetch = ReadFetch(request);
        Assert.Equal((1u, 10u, false, LongWait.MaxBytes), (fetch.MinCount, fetch.Count, fetch.AutoCommit,
            fetch.MaxBytes));
        Assert.Equal([
            new FetchTarget(1, MessagePolling.Offset, 0, LongWait.MaxBytes, Route.Incarnation, Route.OwnerGeneration),
            new FetchTarget(2, MessagePolling.Next, 0, LongWait.MaxBytes, fenced.Incarnation, fenced.OwnerGeneration)
        ], fetch.Targets);
        Assert.True(fetch.RequestTimeoutUs >= fetch.WaitUs + DeferredPollOptions.REPLY_MARGIN_US);
        Assert.Equal([0ul, 1ul], reply.Sections[0].Decode().Messages.Select(message => message.Header.Offset));
        Assert.Equal((7ul, 0u), (reply.Sections[1].NextOffset, reply.Sections[1].MessagesCount));
    }

    /// <summary>
    ///     The reply holds the section table and the records, so the records of each target may fill the frame limit
    ///     less the table. The client applies the limit before it sends the fetch.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(1000)]
    public async Task given_a_frame_limit_below_the_byte_limit_when_fetched_should_cap_each_target_by_it(int targets)
    {
        using var node = new MockNode();
        var fetches = new ConcurrentQueue<FetchRequest>();
        node.Serve(request =>
        {
            if (request.Code != FetchCode)
            {
                return Standalone(node, request);
            }

            fetches.Enqueue(ReadFetch(request));
            return Reply(request.Operation, FetchBody());
        });
        using var client = await ConnectAsync(node, SmallFrameLimit);

        using var reply = await client.FetchMessagesAsync(Stream, Topic, Targets(targets), Consumer.New(4), 10, false,
            LongWait, TestContext.Current.CancellationToken);

        var cap = (uint)(SmallFrameLimit - HEADER_SIZE - FetchedMessages.HeadSize(targets));
        var fetch = Assert.Single(fetches);
        Assert.Equal(cap, fetch.MaxBytes);
        Assert.All(fetch.Targets, target => Assert.Equal(cap, target.MaxBytes));
        Assert.Empty(reply.Sections);
    }

    [Fact]
    public async Task given_a_frame_limit_without_room_for_one_message_when_fetched_should_refuse_it_before_the_wire()
    {
        using var node = new MockNode();
        node.Serve(request => Standalone(node, request));
        const int targets = TcpContracts.MAX_PARTITIONS_PER_REQUEST;
        using var client = await ConnectAsync(node,
            HEADER_SIZE + FetchedMessages.HeadSize(targets) + (int)DeferredPollOptions.MIN_MAX_BYTES - 1);

        var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => client.FetchMessagesAsync(Stream,
            Topic, Targets(targets), Consumer.New(4), 10, false, LongWait, TestContext.Current.CancellationToken));

        Assert.Equal(VsrError.INVALID_SIZE_BYTES, error.StatusCode);
        Assert.Equal(0, node.Requests(FetchCode));
    }

    [Theory]
    [InlineData("no target", VsrError.INVALID_COMMAND)]
    [InlineData("too many targets", VsrError.INVALID_COMMAND)]
    [InlineData("a partition twice", VsrError.INVALID_COMMAND)]
    [InlineData("a first start", VsrError.INVALID_COMMAND)]
    [InlineData("no reply margin", VsrError.INVALID_COMMAND)]
    [InlineData("count zero", VsrError.INVALID_MESSAGES_COUNT)]
    public async Task given_a_fetch_the_server_refuses_when_fetched_should_refuse_it_before_the_wire(string fetch,
        int status)
    {
        using var node = new MockNode();
        node.Serve(request => Standalone(node, request));
        using var client = await ConnectAsync(node);
        var targets = fetch switch
        {
            "no target" => [],
            "too many targets" => Targets(TcpContracts.MAX_PARTITIONS_PER_REQUEST + 1),
            "a partition twice" => [(1, PollingStrategy.Offset(0)), (1, PollingStrategy.Next())],
            "a first start" => [(1, PollingStrategy.First())],
            _ => Targets(1)
        };
        var options = fetch == "no reply margin"
            ? new DeferredPollOptions { MaxWait = TimeSpan.Zero, RequestTimeout = TimeSpan.FromMilliseconds(500) }
            : LongWait;

        var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => client.FetchMessagesAsync(Stream,
            Topic, targets, Consumer.New(4), fetch == "count zero" ? 0u : 10u, false, options,
            TestContext.Current.CancellationToken));

        Assert.Equal(status, error.StatusCode);
        Assert.Equal(0, node.Requests(FetchCode));
    }

    [Fact]
    public async Task given_a_zero_wait_fetch_with_only_the_reply_margin_when_fetched_should_refuse_it_before_routing()
    {
        using var node = new MockNode();
        node.Serve(request => Standalone(node, request));
        using var client = await ConnectAsync(node);
        var options = new DeferredPollOptions
        {
            MaxWait = TimeSpan.Zero,
            RequestTimeout = TimeSpan.FromSeconds(1)
        };

        var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => client.FetchMessagesAsync(Stream,
            Topic, [(1, PollingStrategy.Offset(0))], Consumer.New(4), 10, false, options,
            TestContext.Current.CancellationToken));

        Assert.Equal(VsrError.INVALID_COMMAND, error.StatusCode);
        Assert.Equal(0, node.Requests(CommandCodes.GET_PARTITION_ROUTE_CODE));
        Assert.Equal(0, node.Requests(FetchCode));
    }

    [Fact]
    public async Task given_a_zero_wait_fetch_with_reply_headroom_when_fetched_should_send_it_with_the_remaining_budget()
    {
        using var node = new MockNode();
        var fetches = new ConcurrentQueue<FetchRequest>();
        node.Serve(request =>
        {
            if (request.Code != FetchCode)
            {
                return Standalone(node, request);
            }

            fetches.Enqueue(ReadFetch(request));
            return Reply(request.Operation, FetchBody(GoldenSection(1, 0, Served)));
        });
        using var client = await ConnectAsync(node);

        using var reply = await client.FetchMessagesAsync(Stream, Topic, Targets(1), Consumer.New(4), 10, false,
            ShortTimeout, TestContext.Current.CancellationToken);

        var sent = Assert.Single(fetches);
        Assert.Equal(0ul, sent.WaitUs);
        Assert.InRange(sent.RequestTimeoutUs, DeferredPollOptions.REPLY_MARGIN_US, ShortTimeout.RequestTimeoutUs);
        Assert.Equal(GOLDEN_BATCH_MESSAGES, Assert.Single(reply.Sections).MessagesCount);
    }

    /// <summary>
    ///     Every attempt sends what is left of the wait and the request timeout, and a fetch keeps the reply margin
    ///     also without a wait. A retry with less than the margin left fails with 58 and is not sent.
    /// </summary>
    [Fact]
    public async Task given_a_refused_fetch_when_less_than_the_reply_margin_is_left_should_fail_without_a_retry()
    {
        using var node = new MockNode();
        var attempts = new ConcurrentQueue<FetchRequest>();
        node.Serve(request =>
        {
            if (request.Code != FetchCode)
            {
                return Standalone(node, request);
            }

            attempts.Enqueue(ReadFetch(request));
            return Reply(request.Operation, [], VsrError.DEFERRED_POLL_CAPACITY_EXCEEDED);
        }, (request, _) => request.Code == FetchCode ? Task.Delay(SlowRefusal) : Task.CompletedTask);
        using var client = await ConnectAsync(node);

        var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => client.FetchMessagesAsync(Stream,
            Topic, Targets(1), Consumer.New(4), 10, false, ShortTimeout, TestContext.Current.CancellationToken));

        Assert.Equal(VsrError.TRANSIENT_NOT_ACCEPTED, error.StatusCode);
        var attempt = Assert.Single(attempts);
        Assert.Equal(0ul, attempt.WaitUs);
        Assert.InRange(attempt.RequestTimeoutUs, DeferredPollOptions.REPLY_MARGIN_US,
            (ulong)ShortTimeout.RequestTimeout.TotalMicroseconds);
    }

    /// <summary>A fetch parks as one request, so it takes one slot of the parked request limit for all its targets.</summary>
    [Fact]
    public async Task given_parked_polls_and_a_parked_fetch_at_the_limit_when_another_poll_starts_should_wait_for_the_fetch()
    {
        const int Limit = DeferredPollOptions.MAX_PARKED_REQUESTS;
        using var node = new MockNode();
        node.Serve(request => request.Code is PollCode or FetchCode or CancelCode ? null : Standalone(node, request));
        using var client = await ConnectAsync(node);
        var token = TestContext.Current.CancellationToken;
        using var parked = CancellationTokenSource.CreateLinkedTokenSource(token);
        using var fetching = CancellationTokenSource.CreateLinkedTokenSource(token);
        var polls = Enumerable.Range(0, Limit - 1).Select(_ => PollPartitionAsync(client, parked.Token)).ToList();
        await UntilAsync(() => node.Requests(PollCode) == Limit - 1);
        var fetch = client.FetchMessagesAsync(Stream, Topic, Targets(2), Consumer.New(4), 10, false, LongWait,
            fetching.Token);
        await UntilAsync(() => node.Requests(FetchCode) == 1);

        polls.Add(PollPartitionAsync(client, parked.Token));
        await Task.Delay(SettleDelay, token);
        Assert.Equal(Limit - 1, node.Requests(PollCode));
        await fetching.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => fetch);
        await UntilAsync(() => node.Requests(PollCode) == Limit);
        await parked.CancelAsync();
        foreach (var poll in polls)
        {
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => poll);
        }
    }

    /// <summary>
    ///     A refusal as not primary of the whole fetch does not say which target moved, so the next grouping looks up
    ///     every route of the fetch again. A section refused as not primary marks only its own route. The fetch is
    ///     not sent to another node.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_a_refusal_as_not_primary_when_fetched_should_regroup_the_partitions_it_names(bool whole)
    {
        using var node = new MockNode();
        var routes = new ConcurrentDictionary<uint, int>();
        node.Serve(request =>
        {
            switch (request.Code)
            {
                case CommandCodes.GET_PARTITION_ROUTE_CODE:
                    routes.AddOrUpdate(RoutedPartition(request), 1, (_, count) => count + 1);
                    return Standalone(node, request);
                case FetchCode:
                    return whole
                        ? Reply(request.Operation, [], VsrError.NOT_PRIMARY)
                        : Reply(request.Operation, FetchBody(GoldenSection(1, 0, Served),
                            EmptySection(2, 0, default, VsrError.NOT_PRIMARY)));
                default:
                    return Standalone(node, request);
            }
        });
        using var client = await ConnectAsync(node);
        var token = TestContext.Current.CancellationToken;
        Assert.Equal<IReadOnlyList<uint>>([[1u, 2u]], await client.ReadinessGroupsAsync(Stream, Topic, [1, 2], token));

        var fetch = client.FetchMessagesAsync(Stream, Topic, Targets(2), Consumer.New(4), 10, false, LongWait, token);
        if (whole)
        {
            Assert.Equal(VsrError.NOT_PRIMARY,
                (await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => fetch)).StatusCode);
        }
        else
        {
            (await fetch).Dispose();
        }

        await client.ReadinessGroupsAsync(Stream, Topic, [1, 2], token);
        Assert.Equal((whole ? 2 : 1, 2), (routes[1], routes[2]));
        Assert.Equal(1, node.Requests(FetchCode));
    }

    /// <summary>
    ///     A fetch that can move an offset takes the progress lanes of all its targets, so a store on one of them
    ///     waits for the fetch. A store on another partition does not, and neither does a store behind a fetch that
    ///     moves nothing.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_a_parked_fetch_when_the_client_stores_an_offset_should_wait_only_behind_progress(
        bool autoCommit)
    {
        using var node = new MockNode();
        var stored = new ConcurrentQueue<uint>();
        var parked = new TaskCompletionSource<MockRequest>(TaskCreationOptions.RunContinuationsAsynchronously);
        node.Serve(request =>
        {
            if (request.Operation == (byte)VsrOperation.StoreConsumerOffset)
            {
                stored.Enqueue(StoredPartition(request));
            }

            if (request.Code == FetchCode)
            {
                parked.TrySetResult(request);
                return null;
            }

            return request.Code == CancelCode ? null : Standalone(node, request);
        });
        using var client = await ConnectAsync(node);
        var token = TestContext.Current.CancellationToken;
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(token);
        var fetch = client.FetchMessagesAsync(Stream, Topic, [(5, PollingStrategy.Offset(0)),
            (3, PollingStrategy.Offset(0))], Consumer.New(4), 10, autoCommit, LongWait, cancellation.Token);
        var request = await parked.Task.WaitAsync(PromptLimit, token);

        var behind = client.StoreOffsetAsync(Consumer.New(4), Stream, Topic, 0, 3, token);
        await client.StoreOffsetAsync(Consumer.New(4), Stream, Topic, 0, 4, token).WaitAsync(PromptLimit, token);
        await Task.Delay(SettleDelay, token);

        Assert.Equal(autoCommit ? [4u] : [3u, 4u], stored.Order());
        if (autoCommit)
        {
            await node.AnswerParkedAsync(request, Reply(request.Operation, FetchBody()));
            (await fetch.WaitAsync(PromptLimit, token)).Dispose();
        }
        else
        {
            await cancellation.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => fetch);
        }

        await behind.WaitAsync(PromptLimit, token);
        Assert.Equal([3u, 4u], stored.Order());
    }

    [Fact]
    public async Task given_a_live_committing_fetch_when_its_deadline_passes_should_block_each_target_until_abandoned()
    {
        using var node = new MockNode();
        using var replacement = new MockNode();
        byte primary = 0;
        var roster = new List<byte>();
        WriteString(roster, "fetch-recovery");
        roster.AddRange(BitConverter.GetBytes(2u));
        WriteNode(roster, node.Port, true, 0);
        WriteNode(roster, replacement.Port, false, 1);
        byte[] RouteOrControl(MockRequest request) => request.Code switch
        {
            GET_CLUSTER_METADATA_CODE => Reply(request.Operation, roster.ToArray()),
            CommandCodes.GET_PARTITION_ROUTE_CODE => RouteReply(request, Route, primary, TopicPartitions, view: primary),
            _ => Answer(request)
        };
        var parked = new TaskCompletionSource<MockRequest>(TaskCreationOptions.RunContinuationsAsynchronously);
        var stored = new ConcurrentQueue<uint>();
        node.Serve(request =>
        {
            if (request.Operation == (byte)VsrOperation.StoreConsumerOffset)
            {
                stored.Enqueue(StoredPartition(request));
            }
            if (request.Code == FetchCode)
            {
                return parked.TrySetResult(request) ? null
                    : Reply(request.Operation, [], VsrError.NOT_PRIMARY);
            }
            return RouteOrControl(request);
        });
        var replacementStores = new ConcurrentQueue<uint>();
        replacement.Serve(request =>
        {
            if (request.Operation == (byte)VsrOperation.StoreConsumerOffset)
            {
                replacementStores.Enqueue(StoredPartition(request));
            }
            return request.Code == FetchCode ? Reply(request.Operation, FetchBody()) : RouteOrControl(request);
        });
        using var client = await ConnectAsync(node);
        var token = TestContext.Current.CancellationToken;
        var unknown = await Assert.ThrowsAnyAsync<VsrRequestOutcomeUnknownException>(() => client.FetchMessagesAsync(
            Stream, Topic, [(5, PollingStrategy.Offset(0)), (3, PollingStrategy.Offset(0))], Consumer.New(4), 10,
            true, ShortTimeout, token));
        var request = await parked.Task.WaitAsync(PromptLimit, token);
        primary = 1;
        await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => client.FetchMessagesAsync(Stream, Topic,
            [(3, PollingStrategy.Offset(0))], Consumer.New(4), 10, false, LongWait, token));

        var blocked = await Assert.ThrowsAsync<VsrPartitionLaneBlockedException>(() => client.StoreOffsetAsync(
            Consumer.New(4), Stream, Topic, 0, 3, token));
        Assert.Equal(3u, blocked.PartitionId);
        Assert.Equal(unknown.OperationId, blocked.OperationId);
        Assert.Empty(stored);
        Assert.Empty(replacementStores);
        var second = await Assert.ThrowsAsync<VsrPartitionLaneBlockedException>(() => client.FetchMessagesAsync(Stream,
            Topic, [(1, PollingStrategy.Offset(0)), (5, PollingStrategy.Offset(0))], Consumer.New(4), 10, true,
            LongWait, token));
        Assert.Equal(5u, second.PartitionId);
        Assert.Equal(blocked.OperationId, second.OperationId);
        using var unrelated = await client.FetchMessagesAsync(Stream, Topic, Targets(1), Consumer.New(4), 10,
            true, LongWait, token);
        using var stateless = await client.FetchMessagesAsync(Stream, Topic, [(3, PollingStrategy.Offset(0))],
            Consumer.New(4), 10, false, LongWait, token);
        await client.GetOffsetAsync(Consumer.New(4), Stream, Topic, 3, token);
        await Assert.ThrowsAnyAsync<VsrRequestOutcomeUnknownException>(() => client.ResolveOperationAsync(
            blocked.OperationId, PromptLimit, token));

        await node.AnswerParkedAsync(request, Reply(request.Operation, FetchBody()));
        await Assert.ThrowsAsync<VsrPartitionLaneBlockedException>(() => client.StoreOffsetAsync(Consumer.New(4),
            Stream, Topic, 0, 3, token));
        var fetches = node.Requests(FetchCode);
        await client.AbandonOperationAsync(blocked.OperationId);
        await client.StoreOffsetAsync(Consumer.New(4), Stream, Topic, 0, 3, token);
        Assert.Equal(fetches, node.Requests(FetchCode));
        Assert.Empty(stored);
        Assert.Equal([3u], replacementStores);
    }

    [Theory]
    [InlineData(VsrError.TRANSIENT_NOT_COMMITTED)]
    [InlineData(VsrError.REQUEST_ALREADY_APPLIED)]
    [InlineData(VsrError.INVALID_COMMAND)]
    [InlineData(11001)]
    public async Task given_partial_committing_uncertainty_when_fetching_should_preserve_healthy_sections_and_block_only_uncertain_targets(
        int status)
    {
        using var node = new MockNode();
        node.Serve(request => request.Code == FetchCode
            ? Reply(request.Operation, FetchBody(GoldenSection(1, 0, Served), EmptySection(3, 0, default, (uint)status)))
            : Standalone(node, request));
        using var client = await ConnectAsync(node);
        var token = TestContext.Current.CancellationToken;
        using var reply = await client.FetchMessagesAsync(Stream, Topic,
            [(1, PollingStrategy.Offset(0)), (3, PollingStrategy.Offset(0))], Consumer.New(4), 10, true, LongWait, token);
        Assert.Equal((int)GOLDEN_BATCH_MESSAGES, reply.Sections[0].Decode().Messages.Count);
        var uncertain = reply.Sections[1];
        Assert.Equal((uint)VsrFetchProgressUncertainException.STATUS_CODE, uncertain.Status);
        var error = Assert.Throws<VsrFetchProgressUncertainException>(() => uncertain.Decode());
        Assert.NotNull(error.OperationId);
        await client.StoreOffsetAsync(Consumer.New(4), Stream, Topic, 0, 1, token);
        var blocked = await Assert.ThrowsAsync<VsrPartitionLaneBlockedException>(() => client.StoreOffsetAsync(
            Consumer.New(4), Stream, Topic, 0, 3, token));
        Assert.Equal(error.OperationId, blocked.OperationId);
        Assert.Equal(3u, blocked.PartitionId);
        Assert.Equal(14009, blocked.StatusCode);
        Assert.True(blocked.RequiresExplicitRecovery);
        await client.AbandonOperationAsync(blocked.OperationId);
        await client.StoreOffsetAsync(Consumer.New(4), Stream, Topic, 0, 3, token);
    }

    [Theory]
    [InlineData("missing table")]
    [InlineData("duplicate target")]
    [InlineData("foreign target")]
    public async Task given_a_malformed_committing_table_when_fetching_should_block_every_target(string malformed)
    {
        using var node = new MockNode();
        var body = malformed switch
        {
            "duplicate target" => FetchBody(EmptySection(3, 0, Served), EmptySection(3, 0, Served)),
            "foreign target" => FetchBody(EmptySection(9, 0, Served)),
            _ => []
        };
        node.Serve(request => request.Code == FetchCode ? Reply(request.Operation, body) : Standalone(node, request));
        using var client = await ConnectAsync(node);
        var token = TestContext.Current.CancellationToken;
        var failure = await Assert.ThrowsAsync<VsrFetchProgressUncertainException>(() => client.FetchMessagesAsync(
            Stream, Topic, [(1, PollingStrategy.Offset(0)), (3, PollingStrategy.Offset(0))], Consumer.New(4), 10,
            true, LongWait, token));
        var blocked = await Assert.ThrowsAsync<VsrPartitionLaneBlockedException>(() => client.StoreOffsetAsync(
            Consumer.New(4), Stream, Topic, 0, 3, token));
        Assert.Equal(failure.OperationId, blocked.OperationId);
    }

    [Fact]
    public async Task given_uncertain_fetch_progress_when_signing_in_again_should_preserve_alias_blocks_and_separate_new_histories()
    {
        using var node = new MockNode();
        var route = Route;
        node.Serve(request => request.Code switch
        {
            FetchCode => Reply(request.Operation, [], VsrError.TRANSIENT_NOT_COMMITTED),
            _ when request.Operation == (byte)VsrOperation.StoreConsumerOffset && request.Context.Incarnation == 0 =>
                Reply(request.Operation, [], VsrError.HISTORY_UNAVAILABLE),
            CommandCodes.GET_PARTITION_ROUTE_CODE => RouteReply(request, route, 0, TopicPartitions),
            _ => Standalone(node, request)
        });
        using var client = await ConnectAsync(node);
        var token = TestContext.Current.CancellationToken;
        var failure = await Assert.ThrowsAsync<VsrFetchProgressUncertainException>(() => client.FetchMessagesAsync(
            Stream, Topic, [(3, PollingStrategy.Offset(0).WithContext(default))], Consumer.New(4), 10, true, LongWait, token));
        await client.LoginUserAsync("user", "secret", token);
        var blocked = await Assert.ThrowsAsync<VsrPartitionLaneBlockedException>(() => client.StoreOffsetAsync(
            Consumer.New(4), Identifier.String("stream-alias"), Identifier.String("topic-alias"), 0, 3, token));
        Assert.Equal(failure.OperationId, blocked.OperationId);
        var zeroContext = await Assert.ThrowsAsync<VsrPartitionLaneBlockedException>(() => client.StoreOffsetAsync(
            Consumer.New(4), Stream, Topic, 0, 3, default(PartitionContext), token));
        Assert.Equal(failure.OperationId, zeroContext.OperationId);
        route = route with { Incarnation = route.Incarnation + 1 };
        await client.StoreOffsetAsync(Consumer.New(4), Identifier.String("new-stream"), Identifier.String("new-topic"),
            0, 3, token);
        await Assert.ThrowsAsync<VsrFetchProgressUncertainException>(() => client.ResolveOperationAsync(
            blocked.OperationId, PromptLimit, token));
        await client.AbandonOperationAsync(blocked.OperationId);
        await Assert.ThrowsAsync<VsrOperationNotRetainedException>(() => client.ResolveOperationAsync(
            blocked.OperationId, PromptLimit, token));
        var stale = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => client.StoreOffsetAsync(
            Consumer.New(4), Stream, Topic, 0, 3, default(PartitionContext), token));
        Assert.Equal(VsrError.HISTORY_UNAVAILABLE, stale.StatusCode);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_full_fetch_recovery_capacity_when_fetching_should_refuse_before_send_until_abandonment(bool bytes)
    {
        using var node = new MockNode();
        var fetches = 0;
        node.Serve(request => request.Code == FetchCode
            ? Interlocked.Increment(ref fetches) == 1
                ? Reply(request.Operation, [], VsrError.TRANSIENT_NOT_COMMITTED)
                : Reply(request.Operation, FetchBody())
            : Standalone(node, request));
        var frameSize = VsrHeader.HEADER_SIZE + TcpContracts.FetchMessages(Consumer.New(4), Stream, Topic,
            LongWait, 10, true, LongWait.MaxBytes, [(3, PollingStrategy.Offset(0))]).Length;
        using var client = await ConnectAsync(node, maxPendingOperations: bytes ? null : 1,
            maxPendingOperationBytes: bytes ? frameSize : null);
        var token = TestContext.Current.CancellationToken;
        var failure = await Assert.ThrowsAsync<VsrFetchProgressUncertainException>(() => client.FetchMessagesAsync(
            Stream, Topic, [(3, PollingStrategy.Offset(0))], Consumer.New(4), 10, true, LongWait, token));
        var capacity = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => client.FetchMessagesAsync(
            Stream, Topic, Targets(1), Consumer.New(4), 10, true, ShortTimeout, token));
        Assert.Equal(VsrError.TRANSIENT_NOT_ACCEPTED, capacity.StatusCode);
        Assert.Equal(1, Volatile.Read(ref fetches));
        using var stateless = await client.FetchMessagesAsync(Stream, Topic, Targets(1), Consumer.New(4), 10, false,
            LongWait, token);
        await client.AbandonOperationAsync(failure.OperationId!.Value);
        using var admitted = await client.FetchMessagesAsync(Stream, Topic, Targets(1), Consumer.New(4), 10, true,
            LongWait, token);
        using var afterProof = await client.FetchMessagesAsync(Stream, Topic, Targets(1), Consumer.New(4), 10, true,
            LongWait, token);
        Assert.Equal(4, Volatile.Read(ref fetches));
    }

    [Fact]
    public async Task given_a_committing_fetch_waiting_behind_progress_when_canceled_should_release_without_sending()
    {
        using var node = new MockNode();
        var parked = new TaskCompletionSource<MockRequest>(TaskCreationOptions.RunContinuationsAsynchronously);
        node.Serve(request => request.Code == FetchCode
            ? parked.TrySetResult(request) ? null : Reply(request.Operation, FetchBody())
            : Standalone(node, request));
        using var client = await ConnectAsync(node, maxPendingOperations: 1);
        var token = TestContext.Current.CancellationToken;
        var active = client.FetchMessagesAsync(Stream, Topic, [(3, PollingStrategy.Next())], Consumer.New(4), 10,
            false, LongWait, token);
        var request = await parked.Task.WaitAsync(PromptLimit, token);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(token);
        var waiting = client.FetchMessagesAsync(Stream, Topic, [(3, PollingStrategy.Offset(0))], Consumer.New(4), 10,
            true, LongWait, cancellation.Token);
        await Task.Delay(SettleDelay, token);
        await cancellation.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting);
        Assert.Equal(1, node.Requests(FetchCode));
        await node.AnswerParkedAsync(request, Reply(request.Operation, FetchBody()));
        (await active).Dispose();
        for (var index = 0; index < 2; index++)
        {
            using var reply = await client.FetchMessagesAsync(Stream, Topic, [(3, PollingStrategy.Offset(0))],
                Consumer.New(4), 10, true, LongWait, token);
        }
        Assert.Equal(3, node.Requests(FetchCode));
        Assert.Equal(0, node.Requests(CancelCode));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_a_correlated_fetch_reply_when_the_caller_signed_out_should_settle_progress_before_session_isolation(bool partial)
    {
        using var node = new MockNode();
        var fetch = new TaskCompletionSource<MockRequest>(TaskCreationOptions.RunContinuationsAsynchronously);
        var logout = new TaskCompletionSource<MockRequest>(TaskCreationOptions.RunContinuationsAsynchronously);
        node.Serve(request =>
        {
            if (request.Code == FetchCode)
            {
                fetch.TrySetResult(request);
                return null;
            }
            if (request.Operation == (byte)VsrOperation.Logout)
            {
                logout.TrySetResult(request);
                return null;
            }
            return Standalone(node, request);
        });
        using var client = await ConnectAsync(node);
        var token = TestContext.Current.CancellationToken;
        var pending = client.FetchMessagesAsync(Stream, Topic,
            [(1, PollingStrategy.Offset(0)), (3, PollingStrategy.Offset(0))], Consumer.New(4), 10, true, LongWait, token);
        var request = await fetch.Task.WaitAsync(PromptLimit, token);
        var signingOut = client.LogoutUserAsync(token);
        var logoutRequest = await logout.Task.WaitAsync(PromptLimit, token);
        await node.AnswerParkedAsync(request, Reply(request.Operation, partial
            ? FetchBody(EmptySection(1, 0, Served), EmptySection(3, 0, default, VsrError.TRANSIENT_NOT_COMMITTED))
            : FetchBody(EmptySection(1, 0, Served), EmptySection(3, 0, Served))));
        var ended = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => pending);
        Assert.Equal(VsrError.UNAUTHENTICATED, ended.StatusCode);
        await node.AnswerParkedAsync(logoutRequest, Answer(logoutRequest));
        await signingOut;
        await client.LoginUserAsync("user", "secret", token);
        await client.StoreOffsetAsync(Consumer.New(4), Stream, Topic, 0, 1, token);
        if (partial)
        {
            var blocked = await Assert.ThrowsAsync<VsrPartitionLaneBlockedException>(() => client.StoreOffsetAsync(
                Consumer.New(4), Stream, Topic, 0, 3, token));
            Assert.Equal(3u, blocked.PartitionId);
            Assert.Equal(14009, blocked.StatusCode);
            Assert.True(blocked.RequiresExplicitRecovery);
            await client.AbandonOperationAsync(blocked.OperationId);
        }
        await client.StoreOffsetAsync(Consumer.New(4), Stream, Topic, 0, 3, token);
        Assert.Equal(1, node.Requests(FetchCode));
    }

    /// <summary>
    ///     A fetch that can move an offset is never sent twice, so after a lost reply its outcome is unknown. A fetch
    ///     that moves nothing is a plain read, and it goes out again.
    /// </summary>
    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public async Task given_a_lost_fetch_reply_when_the_fetch_moves_a_consumer_should_not_send_it_again(bool next,
        bool autoCommit)
    {
        using var node = new MockNode();
        var fetches = 0;
        node.Serve(request =>
        {
            if (request.Code != FetchCode)
            {
                return Standalone(node, request);
            }

            if (Interlocked.Increment(ref fetches) == 1)
            {
                throw new IOException("The node lost the reply after it read the fetch.");
            }

            return Reply(request.Operation, FetchBody(GoldenSection(1, 0, Served)));
        });
        using var client = await ConnectAsync(node);
        var strategy = next ? PollingStrategy.Next() : PollingStrategy.Offset(0);

        var fetch = client.FetchMessagesAsync(Stream, Topic, [(1, strategy.WithContext(Route))], Consumer.New(4), 10,
            autoCommit, LongWait, TestContext.Current.CancellationToken);

        if (!next && !autoCommit)
        {
            using var reply = await fetch;
            Assert.Equal(GOLDEN_BATCH_MESSAGES, Assert.Single(reply.Sections).MessagesCount);
            Assert.Equal(2, Volatile.Read(ref fetches));
            return;
        }

        if (autoCommit)
        {
            Assert.NotNull((await Assert.ThrowsAsync<VsrFetchProgressUncertainException>(() => fetch)).OperationId);
        }
        else
        {
            Assert.Null((await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() => fetch)).OperationId);
        }
        Assert.Equal(1, Volatile.Read(ref fetches));
    }

    /// <summary>Targets 1 to <paramref name="count" /> at offset 0, fenced as their routes, so none needs a lookup.</summary>
    private static (uint PartitionId, PollingStrategy Strategy)[] Targets(int count)
    {
        return
        [
            .. Enumerable.Range(1, count)
                .Select(partition => ((uint)partition, PollingStrategy.Offset(0).WithContext(Route)))
        ];
    }

    private static Task<PolledMessages> PollPartitionAsync(TcpMessageStream client, CancellationToken token) =>
        client.PollMessagesAsync(Stream, Topic, 1, Consumer.New(4), PollingStrategy.Offset(0), 10, false, LongWait,
            token);

    private static byte[] Standalone(MockNode node, MockRequest request) => request.Code switch
    {
        GET_CLUSTER_METADATA_CODE => StandaloneRoster(node.Port),
        CommandCodes.GET_PARTITION_ROUTE_CODE => RouteReply(request, Route, 0, TopicPartitions),
        _ => Answer(request)
    };

    private static uint RoutedPartition(MockRequest request)
    {
        return BinaryPrimitives.ReadUInt32LittleEndian(request.Body.AsSpan(request.Body.Length - sizeof(uint)));
    }

    /// <summary>A StoreConsumerOffset body ends with the partition id, the offset and the ack quorum.</summary>
    private static uint StoredPartition(MockRequest request)
    {
        return BinaryPrimitives.ReadUInt32LittleEndian(
            request.Body.AsSpan(request.Body.Length - sizeof(byte) - sizeof(ulong) - sizeof(uint)));
    }

    private static async Task<TcpMessageStream> ConnectAsync(MockNode node, int? maxResponseFrameSize = null,
        int? maxPendingOperations = null, long? maxPendingOperationBytes = null)
    {
        var configurator = new IggyClientConfigurator
        {
            BaseAddress = $"127.0.0.1:{node.Port}",
            Protocol = Protocol.Tcp,
            MaxPendingOperations = maxPendingOperations ?? IggyClientConfigurator.DEFAULT_MAX_PENDING_OPERATIONS,
            MaxPendingOperationBytes = maxPendingOperationBytes ?? IggyClientConfigurator.DEFAULT_MAX_PENDING_OPERATION_BYTES
        };
        if (maxResponseFrameSize is { } limit)
        {
            configurator.MaxResponseFrameSize = limit;
        }

        var client = new TcpMessageStream(configurator, NullLoggerFactory.Instance);
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        await client.LoginUserAsync("user", "secret", TestContext.Current.CancellationToken);

        return client;
    }

    private static async Task UntilAsync(Func<bool> predicate)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(10));
        while (!predicate())
        {
            await Task.Delay(5, timeout.Token);
        }
    }
}
