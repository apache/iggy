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

using System.Buffers;
using System.Buffers.Binary;
using System.Net.Sockets;
using System.Threading.Channels;
using Apache.Iggy.Contracts;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Utils;
using Microsoft.Extensions.Logging;

namespace Apache.Iggy.Vsr;

/// <summary>
///     A bounded multiplexed connection. The writer owns frame boundaries; the reader correlates replies
///     by request id. Canceling one exchange never interrupts either task or another request.
/// </summary>
internal sealed class VsrConnection : IDisposable
{
    internal const int WRITE_QUEUE_CAPACITY = 32;
    internal const int MAX_PENDING_REQUESTS = 256;
    private const int TransientRetryIntervalMs = 50;

    private sealed class PendingRequest(byte[] frame, int code, long deadline, bool clearSensitiveReply, SharedRequestFrame? owner)
    {
        internal readonly byte[] Frame = frame;
        internal readonly int Code = code;
        internal readonly long Deadline = deadline;
        internal readonly bool ClearSensitiveReply = clearSensitiveReply;
        internal readonly SharedRequestFrame? Owner = owner;
        internal readonly TaskCompletionSource<VsrAttempt> Completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal readonly ulong RequestId = BinaryPrimitives.ReadUInt64LittleEndian(frame.AsSpan(VsrHeader.REQUEST_ID_OFFSET));
        internal readonly UInt128 ClientId = BinaryPrimitives.ReadUInt128LittleEndian(frame.AsSpan(VsrHeader.REQUEST_CLIENT_OFFSET));
        internal readonly UInt128 RequestChecksum = BinaryPrimitives.ReadUInt128LittleEndian(frame.AsSpan(VsrHeader.REQUEST_CHECKSUM_OFFSET));
        internal readonly ulong Incarnation = BinaryPrimitives.ReadUInt64LittleEndian(frame.AsSpan(VsrHeader.REQUEST_INCARNATION_OFFSET));
        internal bool Started;
    }

    private readonly object _gate = new();
    private readonly Dictionary<ulong, PendingRequest> _pending = [];
    private readonly SemaphoreSlim _pendingCapacity = new(MAX_PENDING_REQUESTS, MAX_PENDING_REQUESTS);
    private readonly Channel<PendingRequest> _writes = Channel.CreateBounded<PendingRequest>(
        new BoundedChannelOptions(WRITE_QUEUE_CAPACITY) { SingleReader = true, FullMode = BoundedChannelFullMode.Wait });
    private readonly CancellationTokenSource _lifetime = new();
    private readonly Stream _stream;
    private readonly ConsensusSession _session;
    private readonly long _maxResponseFrameSize;
    private readonly int _requestTimeoutMs;
    private readonly Action<VsrConnection> _onDropped;
    private readonly Action<ulong>? _observeMetadataCommit;
    private readonly ILogger _logger;
    private readonly TaskCompletionSource _closed = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private bool _started;
    private long _replySilenceDeadline;
    private bool _waitingForActiveDeadlines;
    private Exception? _failure;

    internal Task Closed => _closed.Task;
    internal bool IsReady => Failure is null && !_closed.Task.IsCompleted;
    internal Exception? Failure => Volatile.Read(ref _failure);

    internal VsrConnection(Stream stream, ConsensusSession session, long maxResponseFrameSize, int requestTimeoutMs,
        Action<VsrConnection> onDropped, ILogger logger, Action<ulong>? observeMetadataCommit = null)
    {
        _stream = stream;
        _session = session;
        _maxResponseFrameSize = maxResponseFrameSize;
        _requestTimeoutMs = requestTimeoutMs;
        _onDropped = onDropped;
        _logger = logger;
        _observeMetadataCommit = observeMetadataCommit;
    }

    public void Dispose() => Fail(new ObjectDisposedException(nameof(VsrConnection)), notify: false);

    internal static bool IsConnectionException(Exception error) => error is IggyZeroBytesException
        or NotConnectedException or SocketException or IOException or ObjectDisposedException
        || IsDetachedSession(error);

    internal static bool IsDetachedSession(Exception? error) => error is VsrSessionEvictedException
    {
        Reason: EvictionReason.NoSession or EvictionReason.StaleClient
    };

    internal Task<VsrAttempt> SendFrameAsync(ClientOperation operation, long deadline, bool clearSensitiveReply,
        CancellationToken token) =>
        ExchangeAsync(operation.Code, operation.Body, operation.Identity, operation.Context, deadline, clearSensitiveReply, token,
            operation.Frame);

    internal async ValueTask<VsrAttempt> SendAttemptAsync(int code, ReadOnlyMemory<byte> body,
        long transientDeadline, long readDeadline, bool clearSensitiveReply, CancellationToken token,
        bool retryTransient = true, PartitionContext context = default, SessionFrame? retainedFrame = null)
    {
        SessionFrame frame;
        try
        {
            frame = retainedFrame ?? _session.Resolve(VsrOperations.ForCode(code));
        }
        catch (Exception error)
        {
            return VsrAttempt.Failed(false, error, false, this);
        }

        var uncertain = false;
        while (true)
        {
            var attempt = await ExchangeAsync(code, body, frame, context, readDeadline, clearSensitiveReply, token);
            if (attempt.Error is not IggyInvalidStatusCodeException error)
            {
                return attempt;
            }
            uncertain |= error is { FromServer: true, StatusCode: VsrError.TRANSIENT_NOT_COMMITTED };
            var masked = uncertain && error.FromServer && !error.Committed
                && error.StatusCode != VsrError.TRANSIENT_NOT_COMMITTED;
            var verdict = masked
                ? VsrError.FromServer(VsrError.TRANSIENT_NOT_COMMITTED,
                    "A later refusal does not resolve the original request outcome.")
                : error;
            verdict.CommandCode = code;
            var deadline = verdict.StatusCode == VsrError.TRANSIENT_NOT_COMMITTED
                ? readDeadline : transientDeadline;
            if (!retryTransient || !verdict.FromServer || verdict.Committed
                || (masked && error.StatusCode is VsrError.REQUEST_TOO_OLD or VsrError.INVALID_COMMAND)
                || verdict.StatusCode is not (VsrError.TRANSIENT_NOT_COMMITTED or VsrError.TRANSIENT_NOT_ACCEPTED)
                || Environment.TickCount64 >= deadline)
            {
                return attempt with { Error = verdict };
            }
            try
            {
                await Task.Delay((int)Math.Clamp(deadline - Environment.TickCount64, 0, TransientRetryIntervalMs), token);
                if (Environment.TickCount64 + TransientRetryIntervalMs >= readDeadline)
                {
                    return attempt with { Error = verdict };
                }
            }
            catch (OperationCanceledException cancellation)
            {
                return VsrAttempt.Failed(true, cancellation, attempt.RequestStarted, this);
            }
        }
    }

    private async Task<VsrAttempt> ExchangeAsync(int code, ReadOnlyMemory<byte> body, SessionFrame frame,
        PartitionContext context, long deadline, bool clearSensitiveReply, CancellationToken token,
        SharedRequestFrame? owner = null)
    {
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(token, _lifetime.Token);
        cancellation.CancelAfter((int)Math.Clamp(deadline - Environment.TickCount64, 0, int.MaxValue));
        var admitted = false;
        PendingRequest? request = null;
        try
        {
            await _pendingCapacity.WaitAsync(cancellation.Token);
            admitted = true;
            var encoded = owner?.Bytes ?? new byte[checked(VsrHeader.HEADER_SIZE + body.Length)];
            if (owner is null)
            {
                VsrHeader.EncodeRequestHeader(encoded, _session, code, body.Span, context, frame);
                body.CopyTo(encoded.AsMemory(VsrHeader.HEADER_SIZE));
            }
            request = new PendingRequest(encoded, code, deadline, clearSensitiveReply, owner);
            lock (_gate)
            {
                if (_failure is { } failure)
                {
                    return VsrAttempt.Failed(true, failure, false, this);
                }
                if (!_pending.TryAdd(frame.RequestId, request))
                {
                    return VsrAttempt.Failed(true, VsrError.Exception(VsrError.INVALID_COMMAND,
                        "The request id already has an active exchange."), false, this);
                }
                if (!_started)
                {
                    _started = true;
                    _ = Task.Run(ReadRepliesAsync);
                    _ = Task.Run(WriteFramesAsync);
                }
            }
            using var registration = cancellation.Token.Register(() => Cancel(request, token));
            try
            {
                while (await _writes.Writer.WaitToWriteAsync(cancellation.Token))
                {
                    lock (_gate)
                    {
                        if (request.Completion.Task.IsCompleted)
                        {
                            break;
                        }
                        owner?.Acquire();
                        request.Started = true;
                        if (_writes.Writer.TryWrite(request))
                        {
                            if (_replySilenceDeadline == 0)
                            {
                                _replySilenceDeadline = Environment.TickCount64 + _requestTimeoutMs;
                            }
                            break;
                        }
                        owner?.Release();
                        request.Started = false;
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Cancel(request, token);
            }
            return await request.Completion.Task;
        }
        catch (Exception error)
        {
            if (request is not null)
            {
                lock (_gate)
                {
                    if (_pending.TryGetValue(request.RequestId, out var active) && ReferenceEquals(active, request))
                    {
                        _pending.Remove(request.RequestId);
                    }
                }
            }
            if (error is OperationCanceledException && !token.IsCancellationRequested)
            {
                error = _failure ?? new IOException($"Timed out after {_requestTimeoutMs} ms waiting for a consensus reply.");
            }
            return VsrAttempt.Failed(request is not null, error, request?.Started ?? false, this);
        }
        finally
        {
            if (admitted)
            {
                _pendingCapacity.Release();
            }
        }
    }

    private void Cancel(PendingRequest request, CancellationToken token)
    {
        lock (_gate)
        {
            if (_pending.TryGetValue(request.RequestId, out var active) && ReferenceEquals(active, request))
            {
                _pending.Remove(request.RequestId);
            }
            var error = token.IsCancellationRequested ? new OperationCanceledException(token)
                : _failure ?? new IOException($"Timed out after {_requestTimeoutMs} ms waiting for a consensus reply.");
            request.Completion.TrySetResult(VsrAttempt.Failed(true, error, request.Started, this));
        }
    }

    private async Task WriteFramesAsync()
    {
        try
        {
            await foreach (var request in _writes.Reader.ReadAllAsync(_lifetime.Token))
            {
                try
                {
                    if (!request.Completion.Task.IsCompleted)
                    {
                        using var writeCancellation = CancellationTokenSource.CreateLinkedTokenSource(_lifetime.Token);
                        writeCancellation.CancelAfter(_requestTimeoutMs);
                        await _stream.WriteAsync(request.Frame, writeCancellation.Token);
                        await _stream.FlushAsync(writeCancellation.Token);
                    }
                }
                finally
                {
                    request.Owner?.Release();
                }
            }
        }
        catch (Exception error)
        {
            Fail(error);
        }
        finally
        {
            while (_writes.Reader.TryRead(out var queued))
            {
                queued.Owner?.Release();
            }
        }
    }

    private async Task ReadRepliesAsync()
    {
        var header = new byte[VsrHeader.HEADER_SIZE];
        try
        {
            while (true)
            {
                await ReadFirstReplyByteAsync(header);
                using var frameCancellation = CancellationTokenSource.CreateLinkedTokenSource(_lifetime.Token);
                frameCancellation.CancelAfter(_requestTimeoutMs);
                await _stream.ReadExactlyAsync(header.AsMemory(1), frameCancellation.Token);
                if (VsrHeader.PeekCommand(header) == Command.Eviction)
                {
                    var eviction = VsrHeader.ReadEviction(header);
                    _logger.LogWarning("Consensus link evicted by the server: {Reason}", eviction.Reason);
                    var verdict = VsrReplyDecoder.ToException(eviction);
                    throw new VsrSessionEvictedException(eviction.Reason, verdict);
                }
                if (VsrHeader.PeekCommand(header) != Command.Reply)
                {
                    throw VsrError.Exception(VsrError.INVALID_COMMAND, "Unexpected consensus frame on a client connection.");
                }
                var bodySize = VsrReplyDecoder.ReadBodySize(header);
                if (VsrHeader.HEADER_SIZE + (long)bodySize > _maxResponseFrameSize)
                {
                    throw VsrError.Exception(VsrError.INVALID_COMMAND, "Reply frame exceeds the configured maximum.");
                }
                var requestId = BinaryPrimitives.ReadUInt64LittleEndian(header.AsSpan(VsrHeader.REPLY_REQUEST_ID_OFFSET));
                var buffer = ArrayPool<byte>.Shared.Rent(Math.Max(bodySize, 1));
                try
                {
                    await _stream.ReadExactlyAsync(buffer.AsMemory(0, bodySize), frameCancellation.Token);
                    PendingRequest? request;
                    lock (_gate)
                    {
                        _pending.TryGetValue(requestId, out request);
                        _replySilenceDeadline = _pending.Count > (request is null ? 0 : 1)
                            ? Environment.TickCount64 + _requestTimeoutMs : 0;
                        _waitingForActiveDeadlines = false;
                    }
                    if (request is null)
                    {
                        continue;
                    }
                    var operation = VsrHeader.ReadReplyOperation(header);
                    var expected = VsrOperations.ForCode(request.Code);
                    var alias = (expected, operation) is
                        (VsrOperation.CreateTopic, VsrOperation.CreateTopicWithAssignments)
                        or (VsrOperation.CreatePartitions, VsrOperation.CreatePartitionsWithAssignments)
                        or (VsrOperation.DeleteSegments, VsrOperation.TruncatePartition);
                    if (operation != expected && !alias)
                    {
                        throw VsrError.Exception(VsrError.INVALID_COMMAND, "Reply operation does not match its request.");
                    }
                    VsrAttempt result;
                    IggyInvalidStatusCodeException? detached = null;
                    try
                    {
                        if (VsrHeader.ReadCommitted(header) && !MatchesCommittedRequest(header, request))
                        {
                            throw VsrError.Exception(VsrError.INVALID_COMMAND, "Committed reply does not match the original request.");
                        }
                        var payload = VsrReplyDecoder.Decode(header, buffer.AsMemory(0, bodySize));
                        var committed = VsrHeader.ReadCommitted(header);
                        if (committed && (operation.IsMetadata() || operation == VsrOperation.DeleteSegments))
                        {
                            _observeMetadataCommit?.Invoke(BinaryPrimitives.ReadUInt64LittleEndian(
                                header.AsSpan(VsrHeader.REPLY_COMMIT_OFFSET)));
                        }
                        IMemoryOwner<byte> response = payload.IsEmpty ? EmptyMemoryOwner.Instance
                            : new PooledMemoryOwner(buffer, bodySize - payload.Length, payload.Length, request.ClearSensitiveReply);
                        if (!payload.IsEmpty)
                        {
                            buffer = null;
                        }
                        result = VsrAttempt.Ok(response, this, committed);
                    }
                    catch (IggyInvalidStatusCodeException error)
                    {
                        error.CommandCode = request.Code;
                        if (error.Committed && (operation.IsMetadata() || operation == VsrOperation.DeleteSegments))
                        {
                            _observeMetadataCommit?.Invoke(BinaryPrimitives.ReadUInt64LittleEndian(
                                header.AsSpan(VsrHeader.REPLY_COMMIT_OFFSET)));
                        }
                        if (error.FromServer && !error.Committed && request.Code != CommandCodes.BIND_SESSION_CODE
                            && BinaryPrimitives.ReadUInt64LittleEndian(request.Frame.AsSpan(VsrHeader.REQUEST_SESSION_OFFSET)) != 0
                            && error.StatusCode == VsrError.UNAUTHENTICATED)
                        {
                            detached = error;
                            var refusal = VsrError.FromServer(VsrError.TRANSIENT_NOT_ACCEPTED,
                                "The server detached this connection from its session.");
                            refusal.CommandCode = request.Code;
                            result = VsrAttempt.Failed(true, refusal, true, this);
                        }
                        else
                        {
                            result = VsrAttempt.Failed(true, error, true, this);
                        }
                    }
                    lock (_gate)
                    {
                        if (_pending.TryGetValue(requestId, out var active) && ReferenceEquals(active, request))
                        {
                            _pending.Remove(requestId);
                        }
                        if (!request.Completion.TrySetResult(result))
                        {
                            result.Response?.Dispose();
                        }
                    }
                    detached ??= result.Error is IggyInvalidStatusCodeException
                    { FromServer: true, Committed: false, StatusCode: VsrError.STALE_CLIENT } stale ? stale : null;
                    if (detached is not null)
                    {
                        throw new IOException("The server detached this link from the session.", detached);
                    }
                }
                finally
                {
                    if (buffer is not null)
                    {
                        ArrayPool<byte>.Shared.Return(buffer, clearArray: true);
                    }
                }
            }
        }
        catch (Exception error)
        {
            Fail(error);
        }
    }

    private async Task ReadFirstReplyByteAsync(byte[] header)
    {
        var read = _stream.ReadExactlyAsync(header.AsMemory(0, 1), _lifetime.Token).AsTask();
        while (true)
        {
            try
            {
                await read.WaitAsync(TimeSpan.FromMilliseconds(_requestTimeoutMs), _lifetime.Token);
                return;
            }
            catch (TimeoutException)
            {
                lock (_gate)
                {
                    var now = Environment.TickCount64;
                    if (_replySilenceDeadline == 0 || _replySilenceDeadline > now)
                    {
                        continue;
                    }
                    if (!_waitingForActiveDeadlines)
                    {
                        // Later arrivals must not keep a silent connection alive indefinitely.
                        _waitingForActiveDeadlines = true;
                        foreach (var request in _pending.Values)
                        {
                            if (request.Started)
                            {
                                _replySilenceDeadline = Math.Max(_replySilenceDeadline, request.Deadline);
                            }
                        }
                        if (_replySilenceDeadline > now)
                        {
                            continue;
                        }
                    }
                }
                throw new IOException("The connection stopped replying before its requests completed.");
            }
        }
    }

    private static bool MatchesCommittedRequest(ReadOnlySpan<byte> header, PendingRequest request)
    {
        var op = BinaryPrimitives.ReadUInt64LittleEndian(header[VsrHeader.REPLY_OP_OFFSET..]);
        return BinaryPrimitives.ReadUInt128LittleEndian(header[VsrHeader.REPLY_CLIENT_OFFSET..]) == request.ClientId
            && BinaryPrimitives.ReadUInt128LittleEndian(header[VsrHeader.REPLY_CHECKSUM_OFFSET..]) == request.RequestChecksum
            && BinaryPrimitives.ReadUInt64LittleEndian(header[VsrHeader.REPLY_INCARNATION_OFFSET..]) == request.Incarnation
            && op != 0 && op == BinaryPrimitives.ReadUInt64LittleEndian(header[VsrHeader.REPLY_COMMIT_OFFSET..]);
    }

    private void Fail(Exception error, bool notify = true)
    {
        if (error is OperationCanceledException && !_lifetime.IsCancellationRequested)
        {
            error = new IOException("Consensus frame transfer timed out.", error);
        }
        lock (_gate)
        {
            if (_failure is not null)
            {
                return;
            }
            _failure = error;
            _writes.Writer.TryComplete(error);
            foreach (var request in _pending.Values)
            {
                request.Completion.TrySetResult(VsrAttempt.Failed(true, error, request.Started, this));
            }
            _pending.Clear();
        }
        _lifetime.Cancel();
        _stream.Dispose();
        _closed.TrySetResult();
        if (notify)
        {
            _onDropped(this);
        }
    }
}

/// <summary>A correlated attempt outcome; Committed is independent of a successful weak acknowledgement.</summary>
internal readonly record struct VsrAttempt(
    bool Encoded,
    IMemoryOwner<byte>? Response,
    Exception? Error,
    bool RequestStarted,
    VsrConnection? Connection,
    bool Committed = false)
{
    public static VsrAttempt Ok(IMemoryOwner<byte> response, VsrConnection connection, bool committed = false) =>
        new(true, response, null, true, connection, committed);

    public static VsrAttempt Failed(bool encoded, Exception error, bool requestStarted, VsrConnection? connection) =>
        new(encoded, null, error, requestStarted, connection,
            error is IggyInvalidStatusCodeException { Committed: true });
}

/// <summary>Shared empty reply payload; disposes to nothing, so it is safe to hand out repeatedly.</summary>
internal sealed class EmptyMemoryOwner : IMemoryOwner<byte>
{
    public static readonly EmptyMemoryOwner Instance = new();

    private EmptyMemoryOwner()
    {
    }

    public Memory<byte> Memory => Memory<byte>.Empty;

    public void Dispose()
    {
    }
}

/// <summary>
///     Owns a pooled buffer while exposing only the decoded payload slice inside it. A buffer that carried a
///     credential is zeroed on the way back to the pool so the secret cannot resurface in a later rental.
/// </summary>
internal sealed class PooledMemoryOwner(byte[] buffer, int start, int length, bool clearOnReturn = false)
    : IMemoryOwner<byte>
{
    private int _disposed;

    public Memory<byte> Memory => buffer.AsMemory(start, length);

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            ArrayPool<byte>.Shared.Return(buffer, clearOnReturn);
        }
    }
}
