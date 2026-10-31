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
using System.Diagnostics;
using Apache.Iggy.Configuration;
using Apache.Iggy.Consumers;
using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Tcp;
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

/// <summary>
///     Deferred polls (code 106) and fetches (code 109) against one node, which parks a request by not
///     answering it.
/// </summary>
public sealed class DeferredPollingTests
{
    private const int PollCode = CommandCodes.POLL_MESSAGES_DEFERRED_CODE;
    private const int FetchCode = CommandCodes.FETCH_MESSAGES_CODE;
    private const int CancelCode = CommandCodes.CANCEL_DEFERRED_REQUEST_CODE;

    /// <summary>The golden batch holds the offsets 100 and 101, so this current offset drains its partition.</summary>
    private const ulong DrainedOffset = 101;

    /// <summary>Enough retried calls that a clock counting in whole milliseconds overstates the time of some.</summary>
    private const int RetriedCalls = 20;

    /// <summary>A send reply that confirms no batch: the confirmation count alone.</summary>
    private static readonly byte[] NoConfirmations = new byte[sizeof(uint)];

    private static readonly Identifier Stream = Identifier.Numeric(1);
    private static readonly Identifier Topic = Identifier.Numeric(2);
    private static readonly uint[] Assignment = [1, 3, 5];

    /// <summary>Longer than any test, so a request the node does not answer stays parked until the test ends it.</summary>
    private static readonly DeferredPollOptions LongWait = new()
    {
        MaxWait = TimeSpan.FromSeconds(30),
        RequestTimeout = TimeSpan.FromSeconds(31)
    };

    /// <summary>How long a negative check waits for a request that must not come.</summary>
    private static readonly TimeSpan SettleDelay = TimeSpan.FromMilliseconds(200);

    /// <summary>Well under <see cref="LongWait" />, so a call that waits for a parked request fails the check.</summary>
    private static readonly TimeSpan PromptLimit = TimeSpan.FromSeconds(5);

    /// <summary>
    ///     The node never answers a parked request or its cancel, as the server does. The cancel goes out on the link
    ///     of the parked request and names it, and the link keeps serving.
    /// </summary>
    [Theory]
    [InlineData(PollCode)]
    [InlineData(FetchCode)]
    public async Task given_a_parked_request_when_the_caller_cancels_should_cancel_it_on_its_link(int code)
    {
        using var node = new MockNode();
        var requests = new ConcurrentQueue<MockRequest>();
        node.Serve(request =>
        {
            requests.Enqueue(request);
            return request.Code is PollCode or FetchCode or CancelCode ? null : Standalone(node, request);
        });
        using var client = await ConnectAsync(node);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        Task parked = code == PollCode
            ? PollPartitionAsync(client, cancellation.Token)
            : client.FetchMessagesAsync(Stream, Topic, [(1, PollingStrategy.Offset(0))], Consumer.New(4), 10, false,
                LongWait, cancellation.Token);
        await UntilAsync(() => node.Requests(code) == 1);
        var connections = node.Connections;

        await cancellation.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => parked);
        await UntilAsync(() => node.Requests(CancelCode) == 1);
        var request = Assert.Single(requests, request => request.Code == code);
        var cancel = Assert.Single(requests, request => request.Code == CancelCode);
        Assert.Equal(request.RequestId, cancel.RequestId);
        Assert.Equal(request.ClientId, cancel.ClientId);
        Assert.Equal(request.Session, cancel.Session);
        Assert.Equal(OPERATION_NON_REPLICATED, cancel.Operation);
        Assert.Empty(cancel.Body);
        await client.PollMessagesAsync(Stream, Topic, 1, Consumer.New(4), PollingStrategy.Offset(0), 10, false,
            TestContext.Current.CancellationToken);
        Assert.Equal(connections, node.Connections);
    }

    /// <summary>
    ///     A parked request that commits gets the cancel too and keeps its reply slot. The server answers it once:
    ///     refused when the cancel won and nothing was stored, so the call ends as canceled, or with the messages it
    ///     stored when the read passed first, so the call delivers them.
    /// </summary>
    [Theory]
    [InlineData(PollCode, true)]
    [InlineData(PollCode, false)]
    [InlineData(FetchCode, true)]
    [InlineData(FetchCode, false)]
    public async Task given_a_parked_committing_request_when_the_caller_cancels_should_cancel_it_and_wait_for_its_reply(
        int code, bool cancelWins)
    {
        using var node = new MockNode();
        var stores = 0;
        var parked = new TaskCompletionSource<MockRequest>(TaskCreationOptions.RunContinuationsAsynchronously);
        node.Serve(request =>
        {
            if (request.Code == code)
            {
                parked.TrySetResult(request);
                return null;
            }
            if (request.Operation == (byte)VsrOperation.StoreConsumerOffset)
            {
                Interlocked.Increment(ref stores);
            }
            return request.Code == CancelCode ? null : Standalone(node, request);
        });
        using var client = await ConnectAsync(node);
        var token = TestContext.Current.CancellationToken;
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(token);
        var call = ReadCommittingAsync(client, code, cancellation.Token);
        var request = await parked.Task.WaitAsync(PromptLimit, token);

        await cancellation.CancelAsync();

        await UntilAsync(() => node.Requests(CancelCode) == 1);
        var storing = client.StoreOffsetAsync(Consumer.New(4), Stream, Topic, 0, 1, token);
        await Task.Delay(SettleDelay, token);
        Assert.False(call.IsCompleted);
        Assert.Equal((false, 0), (storing.IsCompleted, Volatile.Read(ref stores)));
        if (cancelWins)
        {
            await node.AnswerParkedAsync(request, Reply(request.Operation, [], VsrError.TRANSIENT_NOT_ACCEPTED));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => call.WaitAsync(PromptLimit, token));
        }
        else
        {
            await node.AnswerParkedAsync(request, code == PollCode
                ? Batch(request, DrainedOffset)
                : Reply(request.Operation, FetchBody(GoldenSection(1, 0, default))));
            Assert.Equal(GOLDEN_BATCH_MESSAGES, await call.WaitAsync(PromptLimit, token));
        }

        await storing.WaitAsync(PromptLimit, token);
        Assert.Equal(1, Volatile.Read(ref stores));
    }

    /// <summary>
    ///     A plain poll that commits is not parked and has no cancel on the server, so once it went out the caller's
    ///     cancel leaves it running and the call delivers what it stored.
    /// </summary>
    [Fact]
    public async Task given_a_committing_poll_in_flight_when_the_caller_cancels_should_deliver_its_reply()
    {
        using var node = new MockNode();
        var parked = new TaskCompletionSource<MockRequest>(TaskCreationOptions.RunContinuationsAsynchronously);
        node.Serve(request =>
        {
            if (request.Code == CommandCodes.POLL_MESSAGES_CODE)
            {
                parked.TrySetResult(request);
                return null;
            }
            return Standalone(node, request);
        });
        using var client = await ConnectAsync(node);
        var token = TestContext.Current.CancellationToken;
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(token);
        var call = client.PollMessagesAsync(Stream, Topic, 1, Consumer.New(4), PollingStrategy.Offset(0), 10, true,
            cancellation.Token);
        var request = await parked.Task.WaitAsync(PromptLimit, token);

        await cancellation.CancelAsync();

        await Task.Delay(SettleDelay, token);
        Assert.False(call.IsCompleted);
        Assert.Equal(0, node.Requests(CancelCode));
        await node.AnswerParkedAsync(request, Batch(request, DrainedOffset));
        Assert.Equal((int)GOLDEN_BATCH_MESSAGES, (await call.WaitAsync(PromptLimit, token)).Messages.Count);
    }

    /// <summary>
    ///     A poll that moves a consumer holds the progress lane of its partition while it is parked. A send takes the
    ///     other lane of the partition, and an offset store moves the consumer too, so only the store waits for the poll.
    /// </summary>
    [Theory]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public async Task given_a_parked_progress_poll_when_the_client_sends_to_its_partition_should_send_at_once(
        bool autoCommit, bool next)
    {
        using var node = new MockNode();
        var stores = 0;
        var parked = new TaskCompletionSource<MockRequest>(TaskCreationOptions.RunContinuationsAsynchronously);
        node.Serve(request =>
        {
            if (request.Operation == (byte)VsrOperation.StoreConsumerOffset)
            {
                Interlocked.Increment(ref stores);
            }
            if (request.Code == PollCode)
            {
                parked.TrySetResult(request);
            }
            return request.Code is PollCode or CancelCode ? null : Standalone(node, request);
        });
        using var client = await ConnectAsync(node);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        var poll = client.PollMessagesAsync(Stream, Topic, 1, Consumer.New(4),
            next ? PollingStrategy.Next() : PollingStrategy.Offset(0), 10, autoCommit, LongWait, cancellation.Token);
        var request = await parked.Task.WaitAsync(PromptLimit, TestContext.Current.CancellationToken);
        var store = client.StoreOffsetAsync(Consumer.New(4), Stream, Topic, 0, 1, TestContext.Current.CancellationToken);

        await client.SendMessagesAsync(Stream, Topic, Partitioning.PartitionId(1),
                new Message(Guid.NewGuid(), "payload"u8.ToArray()), TestContext.Current.CancellationToken)
            .WaitAsync(PromptLimit, TestContext.Current.CancellationToken);

        await Task.Delay(SettleDelay, TestContext.Current.CancellationToken);
        Assert.Equal(0, Volatile.Read(ref stores));
        await cancellation.CancelAsync();
        if (autoCommit)
        {
            // A committing poll keeps its lane until the server answers the cancel.
            await UntilAsync(() => node.Requests(CancelCode) == 1);
            await node.AnswerParkedAsync(request, Reply(request.Operation, [], VsrError.TRANSIENT_NOT_ACCEPTED));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => poll);
        }
        else
        {
            await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() => poll);
        }
        await store.WaitAsync(PromptLimit, TestContext.Current.CancellationToken);
        Assert.Equal(1, Volatile.Read(ref stores));
    }

    /// <summary>The server holds a bounded number of parked requests per session and shard, so the client never sends more.</summary>
    [Fact]
    public async Task given_the_parked_request_limit_when_another_deferred_poll_starts_should_wait_for_a_free_slot()
    {
        const int Limit = DeferredPollOptions.MAX_PARKED_REQUESTS;
        using var node = new MockNode();
        node.Serve(request => request.Code is PollCode or CancelCode ? null : Standalone(node, request));
        using var client = await ConnectAsync(node);
        using var parked = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        using var waiting = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        var held = Enumerable.Range(0, Limit).Select(_ => PollPartitionAsync(client, parked.Token)).ToArray();
        await UntilAsync(() => node.Requests(PollCode) == Limit);

        var next = PollPartitionAsync(client, waiting.Token);
        await Task.Delay(SettleDelay, TestContext.Current.CancellationToken);
        Assert.Equal(Limit, node.Requests(PollCode));
        await parked.CancelAsync();

        await UntilAsync(() => node.Requests(PollCode) == Limit + 1 && node.Requests(CancelCode) == Limit);
        foreach (var poll in held)
        {
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => poll);
        }
        await waiting.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => next);
    }

    [Fact]
    public async Task given_a_full_server_when_it_refuses_a_deferred_poll_should_retry_on_the_same_route()
    {
        using var node = new MockNode();
        var polls = 0;
        node.Serve(request => request.Code != PollCode ? Standalone(node, request)
            : Interlocked.Increment(ref polls) == 1
                ? Reply(request.Operation, [], VsrError.DEFERRED_POLL_CAPACITY_EXCEEDED)
                : Batch(request, DrainedOffset));
        using var client = await ConnectAsync(node);

        var polled = await PollPartitionAsync(client, TestContext.Current.CancellationToken);

        Assert.Equal(2, polled.Messages.Count);
        Assert.Equal(2, Volatile.Read(ref polls));
        Assert.Equal(1, node.Requests(CommandCodes.GET_PARTITION_ROUTE_CODE));
    }

    /// <summary>
    ///     Every attempt asks for the wait less the time its call spent, and never for less. A clock that moves in
    ///     steps counts time that did not pass, and an idle call then ends before its max wait. The retry pause of
    ///     a refused first attempt spreads the attempts over many steps.
    /// </summary>
    [Theory]
    [InlineData(PollCode)]
    [InlineData(FetchCode)]
    public async Task given_refused_deferred_calls_when_retried_should_shorten_the_wait_by_at_most_the_time_spent(
        int code)
    {
        using var node = new MockNode();
        var options = new DeferredPollOptions();
        var callStarted = 0L;
        var attempts = new ConcurrentQueue<(long ShortenedUs, long SpentUs)>();
        node.Serve(request =>
        {
            if (request.Code != code)
            {
                return Standalone(node, request);
            }

            var spentUs = (long)Stopwatch.GetElapsedTime(Volatile.Read(ref callStarted)).TotalMicroseconds;
            var waitUs = code == PollCode
                ? BinaryPrimitives.ReadUInt64LittleEndian(request.Body.AsSpan(^TcpContracts.DEFERRED_POLL_TAIL_SIZE))
                : ReadFetch(request).WaitUs;
            attempts.Enqueue(((long)options.MaxWait.TotalMicroseconds - (long)waitUs, spentUs));
            if (attempts.Count % 2 == 1)
            {
                return Reply(request.Operation, [], VsrError.DEFERRED_POLL_CAPACITY_EXCEEDED);
            }

            return code == PollCode ? Batch(request, DrainedOffset) : Reply(request.Operation, FetchBody());
        });
        using var client = await ConnectAsync(node);

        for (var call = 0; call < RetriedCalls; call++)
        {
            Volatile.Write(ref callStarted, Stopwatch.GetTimestamp());
            Task sent = code == PollCode
                ? client.PollMessagesAsync(Stream, Topic, 1, Consumer.New(4), PollingStrategy.Offset(0), 10, false,
                    options, TestContext.Current.CancellationToken)
                : client.FetchMessagesAsync(Stream, Topic, [(1, PollingStrategy.Offset(0))], Consumer.New(4), 10, false,
                    options, TestContext.Current.CancellationToken);
            await sent;
        }

        Assert.Equal(2 * RetriedCalls, attempts.Count);
        Assert.All(attempts, attempt => Assert.InRange(attempt.ShortenedUs, 0, attempt.SpentUs));
    }

    /// <summary>
    ///     A deferred group poll without a partition would park on one partition while another holds messages, so a
    ///     group member fetches its partitions instead.
    /// </summary>
    /// <summary>
    ///     A committing poll that the server answers as not committed may have stored its offset. It fails as
    ///     uncertain with its operation, and its partition takes no committing poll and no offset store until the
    ///     caller abandons that operation, also after a sign-in.
    /// </summary>
    [Fact]
    public async Task given_a_committing_poll_with_an_unknown_outcome_when_reading_its_partition_should_block_it_until_abandoned()
    {
        using var node = new MockNode();
        var answered = 0;
        node.Serve(request => request.Code != PollCode ? Standalone(node, request)
            : Interlocked.Exchange(ref answered, 1) == 0
                ? Reply(request.Operation, [], VsrError.TRANSIENT_NOT_COMMITTED)
                : Batch(request, DrainedOffset));
        using var client = await ConnectAsync(node);
        var token = TestContext.Current.CancellationToken;

        var uncertain = await Assert.ThrowsAsync<VsrFetchProgressUncertainException>(() => PollOnReadAsync(client, token));
        await client.LoginUserAsync("user", "secret", token);
        var blocked = await Assert.ThrowsAsync<VsrPartitionLaneBlockedException>(() => PollOnReadAsync(client, token));
        var store = await Assert.ThrowsAsync<VsrPartitionLaneBlockedException>(() =>
            client.StoreOffsetAsync(Consumer.New(4), Stream, Topic, 0, 1, token));
        var delete = await Assert.ThrowsAsync<VsrPartitionLaneBlockedException>(() =>
            client.DeleteOffsetAsync(Consumer.New(4), Stream, Topic, 1, token));
        await client.AbandonOperationAsync(uncertain.OperationId!.Value);
        var polled = await PollOnReadAsync(client, token);

        Assert.Equal(VsrError.FETCH_PROGRESS_UNCERTAIN, uncertain.StatusCode);
        Assert.Equal((uncertain.OperationId.Value, 1u), (blocked.OperationId, blocked.PartitionId));
        Assert.Equal((uncertain.OperationId.Value, 1u), (store.OperationId, store.PartitionId));
        Assert.Equal((uncertain.OperationId.Value, 1u), (delete.OperationId, delete.PartitionId));
        Assert.Equal((int)GOLDEN_BATCH_MESSAGES, polled.Messages.Count);
        Assert.Equal(2, node.Requests(PollCode));
    }

    /// <summary>
    ///     The client drops its links while a committing poll waits on the server, which can still store its offset.
    ///     The poll fails as uncertain, and after the client connects and signs in again its partition stays blocked
    ///     until the caller abandons that poll.
    /// </summary>
    [Fact]
    public async Task given_a_parked_committing_poll_when_the_client_disconnects_should_fail_it_as_uncertain_until_abandoned()
    {
        using var node = new MockNode();
        var parked = new TaskCompletionSource<MockRequest>(TaskCreationOptions.RunContinuationsAsynchronously);
        node.Serve(request => request.Code != PollCode ? Standalone(node, request)
            : parked.TrySetResult(request) ? null
            : Batch(request, DrainedOffset));
        using var client = await ConnectAsync(node);
        var token = TestContext.Current.CancellationToken;
        var poll = PollOnReadAsync(client, token);
        await parked.Task.WaitAsync(PromptLimit, token);

        await client.DisconnectAsync();
        var uncertain = await Assert.ThrowsAsync<VsrFetchProgressUncertainException>(() =>
            poll.WaitAsync(PromptLimit, token));
        await client.ConnectAsync(token);
        await client.LoginUserAsync("user", "secret", token);
        var blocked = await Assert.ThrowsAsync<VsrPartitionLaneBlockedException>(() => PollOnReadAsync(client, token));
        await client.AbandonOperationAsync(blocked.OperationId);
        var polled = await PollOnReadAsync(client, token);

        Assert.Equal(uncertain.OperationId, blocked.OperationId);
        Assert.Equal((int)GOLDEN_BATCH_MESSAGES, polled.Messages.Count);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_a_group_poll_without_a_partition_when_it_is_deferred_should_refuse_it_before_the_wire(
        bool rented)
    {
        using var node = new MockNode();
        node.Serve(request => Standalone(node, request));
        using var client = await ConnectAsync(node);
        var token = TestContext.Current.CancellationToken;

        var error = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => rented
            ? (Task)client.PollMessagesRentedAsync(Stream, Topic, null, Consumer.Group(3), PollingStrategy.Next(), 10,
                false, LongWait, token)
            : client.PollMessagesAsync(Stream, Topic, null, Consumer.Group(3), PollingStrategy.Next(), 10, false,
                LongWait, token));

        Assert.Equal(VsrError.INVALID_COMMAND, error.StatusCode);
        Assert.Contains("FetchMessagesAsync", error.Message);
        Assert.Equal(0, node.Requests(PollCode));
        Assert.Equal(0, node.Requests(FetchCode));
    }

    [Theory]
    [InlineData(PollCode)]
    [InlineData(FetchCode)]
    [InlineData(CancelCode)]
    public async Task given_a_deferred_code_when_sent_raw_should_refuse_it_before_the_wire(int code)
    {
        using var node = new MockNode();
        node.Serve(request => Standalone(node, request));
        using var client = await ConnectAsync(node);

        var raw = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() =>
            client.SendBinaryRequestAsync((uint)code, [], TestContext.Current.CancellationToken));
        var toReplica = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() =>
            client.SendBinaryRequestToReplicaAsync((uint)code, [], 0, TestContext.Current.CancellationToken));

        Assert.Equal(VsrError.INVALID_COMMAND, raw.StatusCode);
        Assert.Equal(VsrError.INVALID_COMMAND, toReplica.StatusCode);
        Assert.Equal(0, node.Requests(code));
    }

    private static byte[] Standalone(MockNode node, MockRequest request) => request.Code switch
    {
        CommandCodes.SYNC_CONSUMER_GROUP_CODE => Reply(OPERATION_NON_REPLICATED, AssignmentBody(1, Assignment)),
        GET_CLUSTER_METADATA_CODE => StandaloneRoster(node.Port),
        _ when request.Operation == (byte)VsrOperation.SendMessages => Reply(request.Operation, NoConfirmations),
        _ => Answer(request)
    };

    /// <summary>The golden batch, served from the polled partition with <paramref name="currentOffset" /> as its last offset.</summary>
    private static byte[] Batch(MockRequest request, ulong currentOffset)
    {
        var body = Convert.FromHexString(MessageBatchGoldenVectorTests.POLL_BODY);
        BinaryPrimitives.WriteUInt32LittleEndian(body, PolledPartition(request));
        BinaryPrimitives.WriteUInt64LittleEndian(body.AsSpan(sizeof(uint)), currentOffset);
        return Reply(request.Operation, body);
    }

    private static Task<PolledMessages> PollOnReadAsync(TcpMessageStream client, CancellationToken token) =>
        client.PollMessagesAsync(Stream, Topic, 1, Consumer.New(4), PollingStrategy.Next(),
            new PollOptions { MaxWait = TimeSpan.FromSeconds(1), Commit = CommitPolicy.OnRead }, token);

    private static Task<PolledMessages> PollPartitionAsync(TcpMessageStream client, CancellationToken token) =>
        client.PollMessagesAsync(Stream, Topic, 1, Consumer.New(4), PollingStrategy.Offset(0), 10, false, LongWait,
            token);

    /// <summary>A deferred poll or a fetch of partition 1 that commits, answered with its message count.</summary>
    private static async Task<uint> ReadCommittingAsync(TcpMessageStream client, int code, CancellationToken token)
    {
        if (code == PollCode)
        {
            var polled = await client.PollMessagesAsync(Stream, Topic, 1, Consumer.New(4), PollingStrategy.Offset(0),
                10, true, LongWait, token);
            return (uint)polled.Messages.Count;
        }

        using var fetched = await client.FetchMessagesAsync(Stream, Topic, [(1, PollingStrategy.Offset(0))],
            Consumer.New(4), 10, true, LongWait, token);
        return Assert.Single(fetched.Sections).MessagesCount;
    }

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
