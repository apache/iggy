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
using System.Net;
using System.Net.Sockets;
using Apache.Iggy.Configuration;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Utils;
using Apache.Iggy.Vsr;
using Microsoft.Extensions.Logging.Abstractions;
using static Apache.Iggy.Tests.VsrTests.MockFrames;

namespace Apache.Iggy.Tests.VsrTests;

public sealed class VsrMultiplexingTests
{
    [Fact]
    public async Task given_concurrent_reads_when_replies_are_reordered_should_match_request_ids()
    {
        using var pair = await LinkPair.ConnectAsync();
        var first = pair.SendAsync(token: TestContext.Current.CancellationToken);
        var second = pair.SendAsync(token: TestContext.Current.CancellationToken);
        var firstRequest = await pair.ReadAsync();
        var secondRequest = await pair.ReadAsync();
        Assert.NotEqual(firstRequest.RequestId, secondRequest.RequestId);
        await pair.ReplyAsync(secondRequest, [2]);
        await pair.ReplyAsync(firstRequest, [1]);
        using var firstReply = (await first).Response;
        using var secondReply = (await second).Response;
        Assert.NotNull(firstReply);
        Assert.NotNull(secondReply);
        Assert.Equal([1], firstReply.Memory.ToArray());
        Assert.Equal([2], secondReply.Memory.ToArray());
        Assert.Equal(0, pair.Dropped);
    }

    [Fact]
    public async Task given_a_canceled_exchange_when_its_reply_arrives_late_should_preserve_other_calls()
    {
        using var pair = await LinkPair.ConnectAsync();
        using var canceled = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        var first = pair.SendAsync(token: canceled.Token);
        var second = pair.SendAsync(token: TestContext.Current.CancellationToken);
        var firstRequest = await pair.ReadAsync();
        var secondRequest = await pair.ReadAsync();
        await canceled.CancelAsync();
        var cancellation = await first;
        Assert.IsType<OperationCanceledException>(cancellation.Error);
        Assert.True(cancellation.RequestStarted);
        await pair.ReplyAsync(firstRequest, [1]);
        await pair.ReplyAsync(secondRequest, [2]);
        var secondResult = await second;
        using var secondReply = secondResult.Response;
        Assert.Null(secondResult.Error);
        Assert.NotNull(secondReply);
        Assert.Equal([2], secondReply.Memory.ToArray());
        var third = pair.SendAsync(token: TestContext.Current.CancellationToken);
        await pair.ReplyAsync(await pair.ReadAsync(), [3]);
        using var thirdReply = (await third).Response;
        Assert.NotNull(thirdReply);
        Assert.Equal([3], thirdReply.Memory.ToArray());
        Assert.Equal(0, pair.Dropped);
    }

    [Theory]
    [InlineData((byte)0, false)]
    [InlineData((byte)1, true)]
    [InlineData((byte)2, false)]
    public async Task given_a_business_rejection_when_correlated_should_preserve_only_explicit_commit_proof(
        byte marker, bool committed)
    {
        using var pair = await LinkPair.ConnectAsync();
        var pending = pair.SendAsync(CommandCodes.STORE_CONSUMER_OFFSET_CODE, TestContext.Current.CancellationToken);
        var request = await pair.ReadAsync();
        var rejection = new byte[12];
        BinaryPrimitives.WriteUInt32LittleEndian(rejection, 1);
        BinaryPrimitives.WriteUInt32LittleEndian(rejection.AsSpan(8), VsrError.UNAUTHORIZED);
        await pair.ReplyAsync(request with { RequestId = ulong.MaxValue }, rejection, marker);
        await pair.ReplyAsync(request, rejection, marker);
        var result = await pending;
        var error = Assert.IsType<IggyInvalidStatusCodeException>(result.Error);
        Assert.Equal(VsrError.UNAUTHORIZED, error.StatusCode);
        Assert.Equal(committed, error.Committed);
        Assert.Equal(committed, result.Committed);
        Assert.Equal(CommandCodes.STORE_CONSUMER_OFFSET_CODE, error.CommandCode);
        Assert.Equal(0, pair.Dropped);
    }

    [Fact]
    public async Task given_a_full_pending_table_when_another_call_is_canceled_should_not_enqueue_it()
    {
        using var pair = await LinkPair.ConnectAsync();
        var pending = Enumerable.Range(0, VsrConnection.MAX_PENDING_REQUESTS).Select(_ => pair.SendAsync(token: TestContext.Current.CancellationToken)).ToArray();
        var requests = new List<MockRequest>(pending.Length);
        for (var index = 0; index < pending.Length; index++)
        {
            requests.Add(await pair.ReadAsync());
        }
        using var canceled = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        var refused = pair.SendAsync(token: canceled.Token);
        await canceled.CancelAsync();
        var result = await refused;
        Assert.IsType<OperationCanceledException>(result.Error);
        Assert.False(result.Encoded);
        Assert.False(result.RequestStarted);
        foreach (var request in requests)
        {
            await pair.ReplyAsync(request, []);
        }
        foreach (var attempt in await Task.WhenAll(pending))
        {
            Assert.Null(attempt.Error);
            attempt.Response?.Dispose();
        }
        var next = pair.SendAsync(token: TestContext.Current.CancellationToken);
        await pair.ReplyAsync(await pair.ReadAsync(), [4]);
        using var nextReply = (await next).Response;
        Assert.NotNull(nextReply);
        Assert.Equal([4], nextReply.Memory.ToArray());
        Assert.Equal(0, pair.Dropped);
    }

    [Fact]
    public async Task given_a_silent_peer_when_requests_expire_should_retire_the_link()
    {
        const int DeadlineMs = 100;
        using var pair = await LinkPair.ConnectAsync(frameTimeoutMs: DeadlineMs);
        var pending = pair.SendAsync(token: TestContext.Current.CancellationToken, timeoutMs: DeadlineMs);
        _ = await pair.ReadAsync();

        Assert.IsType<IOException>((await pending).Error);
        await pair.DropNotified.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.Equal(1, pair.Dropped);
    }

    [Fact]
    public async Task given_a_short_deadline_when_another_request_is_active_should_allow_its_late_reply()
    {
        const int ShortDeadlineMs = 100;
        using var pair = await LinkPair.ConnectAsync(frameTimeoutMs: ShortDeadlineMs);
        var shortCall = pair.SendAsync(token: TestContext.Current.CancellationToken, timeoutMs: ShortDeadlineMs);
        var longCall = pair.SendAsync(token: TestContext.Current.CancellationToken);
        var shortRequest = await pair.ReadAsync();
        var longRequest = await pair.ReadAsync();
        Assert.IsType<IOException>((await shortCall).Error);
        await Task.Delay(2 * ShortDeadlineMs, TestContext.Current.CancellationToken);

        await pair.ReplyAsync(shortRequest, [1]);
        await pair.ReplyAsync(longRequest, [2]);

        using var reply = (await longCall).Response;
        Assert.NotNull(reply);
        Assert.Equal([2], reply.Memory.ToArray());
        await Task.Delay(2 * ShortDeadlineMs, TestContext.Current.CancellationToken);
        Assert.Equal(0, pair.Dropped);
    }

    [Fact]
    public async Task given_a_silent_peer_when_more_requests_arrive_should_bound_the_deadline_grace()
    {
        const int FrameDeadlineMs = 100;
        const int ActiveDeadlineMs = 1000;
        using var pair = await LinkPair.ConnectAsync(frameTimeoutMs: FrameDeadlineMs);
        var activeCall = pair.SendAsync(token: TestContext.Current.CancellationToken, timeoutMs: ActiveDeadlineMs);
        _ = await pair.ReadAsync();
        await Task.Delay(3 * FrameDeadlineMs, TestContext.Current.CancellationToken);
        var laterCall = pair.SendAsync(token: TestContext.Current.CancellationToken);
        _ = await pair.ReadAsync();

        await pair.DropNotified.WaitAsync(TimeSpan.FromMilliseconds(2 * ActiveDeadlineMs), TestContext.Current.CancellationToken);

        Assert.IsType<IOException>((await activeCall).Error);
        Assert.IsType<IOException>((await laterCall).Error);
        Assert.Equal(1, pair.Dropped);
    }

    [Fact]
    public async Task given_a_partial_reply_when_the_frame_deadline_expires_should_fail_only_that_link()
    {
        const int CompletionTimeoutSeconds = 5;
        using var broken = await LinkPair.ConnectAsync(frameTimeoutMs: 100);
        using var healthy = await LinkPair.ConnectAsync();
        using var canceled = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        var first = broken.SendAsync(token: canceled.Token);
        var second = broken.SendAsync(token: TestContext.Current.CancellationToken);
        var firstRequest = await broken.ReadAsync();
        _ = await broken.ReadAsync();
        await broken.PartialReplyAsync(firstRequest);
        await canceled.CancelAsync();
        Assert.IsType<OperationCanceledException>((await first).Error);
        var failure = await second.WaitAsync(TimeSpan.FromSeconds(CompletionTimeoutSeconds), TestContext.Current.CancellationToken);
        Assert.IsType<IOException>(failure.Error);
        await broken.DropNotified.WaitAsync(TimeSpan.FromSeconds(CompletionTimeoutSeconds), TestContext.Current.CancellationToken);
        Assert.Equal(1, broken.Dropped);
        var next = healthy.SendAsync(token: TestContext.Current.CancellationToken);
        await healthy.ReplyAsync(await healthy.ReadAsync(), [9]);
        var result = await next;
        using var reply = result.Response;
        Assert.Null(result.Error);
        Assert.NotNull(reply);
        Assert.Equal([9], reply.Memory.ToArray());
        Assert.Equal(0, healthy.Dropped);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_a_stamped_request_when_a_receipt_arrives_should_match_its_request_checksum_not_frame_checksum(
        bool wrongChecksum)
    {
        const ulong RequestStamp = 7;
        const ulong FrameChecksum = 9;
        using var pair = await LinkPair.ConnectAsync();
        var session = new ConsensusSession(7, 42, new byte[LoginRegister.BIND_SECRET_BYTES]);
        var lanes = new MutationLanes(session, new IggyClientConfigurator { BaseAddress = "127.0.0.1:1", Protocol = Protocol.Tcp });
        var operation = await lanes.EnterAsync(new MutationLane(17, 0), CommandCodes.STORE_CONSUMER_OFFSET_CODE,
            ReadOnlyMemory<byte>.Empty, default, true, TestContext.Current.CancellationToken);
        try
        {
            BinaryPrimitives.WriteUInt128LittleEndian(operation.Frame.Bytes.AsSpan(VsrHeader.REQUEST_CHECKSUM_OFFSET), RequestStamp);
            BinaryPrimitives.WriteUInt128LittleEndian(operation.Frame.Bytes, FrameChecksum);
            var pending = pair.SendFrozenAsync(operation);
            var request = await pair.ReadAsync();
            Assert.Equal((UInt128)RequestStamp, request.RequestChecksum);
            await pair.ReplyAsync(request, new byte[4], VsrHeader.COMMITTED_RESULT_MARKER,
                checksum: wrongChecksum ? FrameChecksum : RequestStamp);
            var result = await pending;
            using var reply = result.Response;
            Assert.Equal(!wrongChecksum, result.Committed);
            if (wrongChecksum)
            {
                var error = Assert.IsType<IggyInvalidStatusCodeException>(result.Error);
                Assert.False(error.Committed);
                Assert.False(error.FromServer);
            }
            else
            {
                Assert.Null(result.Error);
            }
        }
        finally
        {
            lanes.Complete(operation);
        }
    }

    [Fact]
    public async Task given_a_sensitive_frozen_request_when_its_reply_is_disposed_should_clear_the_pooled_token()
    {
        using var pair = await LinkPair.ConnectAsync();
        var session = new ConsensusSession(7, 42, new byte[LoginRegister.BIND_SECRET_BYTES]);
        var lanes = new MutationLanes(session, new IggyClientConfigurator { BaseAddress = "127.0.0.1:1", Protocol = Protocol.Tcp });
        var operation = await lanes.EnterAsync(MutationLane.Metadata, CommandCodes.CREATE_PERSONAL_ACCESS_TOKEN_CODE,
            ReadOnlyMemory<byte>.Empty, default, true, TestContext.Current.CancellationToken);
        try
        {
            var pending = pair.SendFrozenAsync(operation, clearSensitiveReply: true);
            var request = await pair.ReadAsync();
            byte[] token = [7, 8, 9];
            await pair.ReplyAsync(request, new byte[4].Concat(token).ToArray());
            var result = await pending;
            Assert.Null(result.Error);
            using var response = result.Response;
            Assert.NotNull(response);
            var memory = response.Memory;
            Assert.Equal(token, memory.ToArray());
            response.Dispose();
            Assert.Equal(new byte[token.Length], memory.ToArray());
        }
        finally
        {
            lanes.Complete(operation);
        }
    }

    private sealed class LinkPair(TcpClient client, TcpClient server) : IDisposable
    {
        private const int RequestTimeoutMs = 5_000;
        private const int MaxResponseFrameSize = 1024 * 1024;
        private readonly NetworkStream _server = server.GetStream();
        private readonly TaskCompletionSource _dropNotified = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private VsrConnection? _connection;
        private int _dropped;
        internal int Dropped => Volatile.Read(ref _dropped);
        internal Task DropNotified => _dropNotified.Task;

        internal static async Task<LinkPair> ConnectAsync(int frameTimeoutMs = RequestTimeoutMs)
        {
            using var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            var client = new TcpClient { NoDelay = true };
            await client.ConnectAsync(IPAddress.Loopback, ((IPEndPoint)listener.LocalEndpoint).Port,
                TestContext.Current.CancellationToken);
            var server = await listener.AcceptTcpClientAsync(TestContext.Current.CancellationToken);
            server.NoDelay = true;
            var pair = new LinkPair(client, server);
            pair._connection = new VsrConnection(client.GetStream(),
                new ConsensusSession(7, 42, new byte[LoginRegister.BIND_SECRET_BYTES]), MaxResponseFrameSize,
                frameTimeoutMs, _ =>
                {
                    Interlocked.Increment(ref pair._dropped);
                    pair._dropNotified.TrySetResult();
                }, NullLogger.Instance);
            return pair;
        }

        internal Task<VsrAttempt> SendAsync(int code = CommandCodes.GET_STATS_CODE, CancellationToken token = default,
            int timeoutMs = RequestTimeoutMs)
        {
            var deadline = Environment.TickCount64 + timeoutMs;
            return _connection!.SendAttemptAsync(code, ReadOnlyMemory<byte>.Empty, deadline, deadline, false,
                token == default ? TestContext.Current.CancellationToken : token, retryTransient: false).AsTask();
        }

        internal async Task<MockRequest> ReadAsync()
        {
            using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
            cancellation.CancelAfter(RequestTimeoutMs);
            var header = new byte[VsrHeader.HEADER_SIZE];
            await _server.ReadExactlyAsync(header, cancellation.Token);
            var body = new byte[VsrHeader.ReadSize(header) - VsrHeader.HEADER_SIZE];
            await _server.ReadExactlyAsync(body, cancellation.Token);
            return new MockRequest(header[VsrHeader.REQUEST_OPERATION_OFFSET],
                BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(VsrHeader.REQUEST_RESERVED_OFFSET)),
                BinaryPrimitives.ReadUInt64LittleEndian(header.AsSpan(VsrHeader.REQUEST_ID_OFFSET)))
            {
                Body = body,
                ClientId = BinaryPrimitives.ReadUInt128LittleEndian(header.AsSpan(VsrHeader.REQUEST_CLIENT_OFFSET)),
                RequestChecksum = BinaryPrimitives.ReadUInt128LittleEndian(header.AsSpan(VsrHeader.REQUEST_CHECKSUM_OFFSET))
            };
        }

        internal async Task ReplyAsync(MockRequest request, byte[] body, byte marker = 0, UInt128? checksum = null)
        {
            var reply = Reply(request.Operation, body);
            reply[VsrHeader.REPLY_COMMITTED_OFFSET] = marker;
            CorrelateReply(request, reply);
            if (checksum is { } stamp)
            {
                BinaryPrimitives.WriteUInt128LittleEndian(reply.AsSpan(VsrHeader.REPLY_CHECKSUM_OFFSET), stamp);
            }
            await _server.WriteAsync(reply, TestContext.Current.CancellationToken);
        }

        internal Task<VsrAttempt> SendFrozenAsync(ClientOperation operation, bool clearSensitiveReply = false) =>
            _connection!.SendFrameAsync(operation, Environment.TickCount64 + RequestTimeoutMs,
                clearSensitiveReply, TestContext.Current.CancellationToken);

        internal async Task PartialReplyAsync(MockRequest request)
        {
            var reply = Reply(request.Operation, []);
            BinaryPrimitives.WriteUInt64LittleEndian(reply.AsSpan(VsrHeader.REPLY_REQUEST_ID_OFFSET), request.RequestId);
            await _server.WriteAsync(reply.AsMemory(0, 1), TestContext.Current.CancellationToken);
        }

        public void Dispose()
        {
            _connection?.Dispose();
            client.Dispose();
            server.Dispose();
        }
    }
}
