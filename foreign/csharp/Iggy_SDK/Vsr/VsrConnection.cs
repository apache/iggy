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
using System.Net.Sockets;
using Apache.Iggy.Exceptions;
using Microsoft.Extensions.Logging;

namespace Apache.Iggy.Vsr;

/// <summary>
///     One consensus-framed socket: encodes request frames, replays them while the server answers
///     transiently, and reads and decodes reply frames. The caller owns the sending lock that serialises
///     attempts, the leader redirection above this layer, and the reconnect that replaces a dropped
///     connection.
/// </summary>
internal sealed class VsrConnection : IDisposable
{
    /// <summary>Backoff between replays of a transiently refused request.</summary>
    private const int TransientRetryIntervalMs = 50;

    private readonly ILogger _logger;
    private readonly long _maxResponseFrameSize;

    /// <summary>
    ///     Tears this connection down at the transport level - session reset, state event - when a frame-level
    ///     failure makes the socket unusable. Runs under the sending lock the caller holds, and it is the
    ///     transport's job to ignore the call when a reconnect already replaced this connection.
    /// </summary>
    private readonly Action<VsrConnection> _onDropped;

    /// <summary>
    ///     Frames up to this size go out as one write: header and body coalesced in
    ///     <see cref="_requestFrameBuffer" />. With Nagle disabled every write is its own segment (and its own
    ///     TLS record under a wrapping stream), so the second write a small request would pay is measurable
    ///     even against the round trip the lockstep protocol already costs.
    /// </summary>
    private const int CoalescedFrameLimit = 4096;

    private readonly byte[] _replyHeaderBuffer = new byte[VsrHeader.HEADER_SIZE];
    private readonly byte[] _requestFrameBuffer = new byte[CoalescedFrameLimit];
    private readonly int _requestTimeoutMs;

    /// <summary>The session identity requests on this connection encode from.</summary>
    private readonly ConsensusSession _session;

    private readonly Stream _stream;

    internal VsrConnection(Stream stream, ConsensusSession session, long maxResponseFrameSize, int requestTimeoutMs,
        Action<VsrConnection> onDropped, ILogger logger)
    {
        _stream = stream;
        _session = session;
        _maxResponseFrameSize = maxResponseFrameSize;
        _requestTimeoutMs = requestTimeoutMs;
        _onDropped = onDropped;
        _logger = logger;
    }

    public void Dispose()
    {
        _stream.Dispose();
    }

    internal static bool IsConnectionException(Exception ex)
    {
        return ex is IggyZeroBytesException or
            NotConnectedException or
            SocketException or
            IOException or
            ObjectDisposedException;
    }

    /// <summary>
    ///     One attempt on this connection: encode the header, write the frame, and replay it - same session,
    ///     same request id - while the server answers transiently. The caller must hold the sending lock,
    ///     which also guards the reuse of the per-connection frame buffer.
    /// </summary>
    internal async ValueTask<VsrAttempt> SendAttemptAsync(int code, ReadOnlyMemory<byte> body,
        long transientDeadline, long readDeadline, CancellationToken token)
    {
        var encoded = false;
        var requestStarted = false;

        try
        {
            VsrHeader.EncodeRequestHeader(_requestFrameBuffer, _session, code, body.Span);

            var frameSize = VsrHeader.HEADER_SIZE + body.Length;
            var coalesced = frameSize <= CoalescedFrameLimit;
            if (coalesced)
            {
                body.Span.CopyTo(_requestFrameBuffer.AsSpan(VsrHeader.HEADER_SIZE));
            }

            encoded = true;

            while (true)
            {
                try
                {
                    // Everything that fails without reaching the socket has to fail before this point: past it a
                    // failure is reported as an outcome the server alone knows, which for a replicated write
                    // tells the caller its request may have committed twice.
                    token.ThrowIfCancellationRequested();
                    requestStarted = true;

                    if (coalesced)
                    {
                        await _stream.WriteAsync(_requestFrameBuffer.AsMemory(0, frameSize), token);
                    }
                    else
                    {
                        // A large body goes out straight from the buffer it was serialised into: no
                        // frame-sized rent and no copy, at the price of a second segment.
                        await _stream.WriteAsync(_requestFrameBuffer.AsMemory(0, VsrHeader.HEADER_SIZE), token);
                        await _stream.WriteAsync(body, token);
                    }

                    await _stream.FlushAsync(token);

                    IMemoryOwner<byte> response = await ReadReplyAsync(readDeadline, token);

                    return VsrAttempt.Ok(response, this);
                }
                catch (IggyInvalidStatusCodeException e) when (IsReplayableTransient(e, transientDeadline,
                                                                   readDeadline))
                {
                    var governingDeadline = e.StatusCode == VsrError.TRANSIENT_NOT_COMMITTED
                        ? readDeadline
                        : transientDeadline;
                    var remaining = governingDeadline - Environment.TickCount64;
                    await Task.Delay((int)Math.Clamp(remaining, 0, TransientRetryIntervalMs), token);
                }
                catch (Exception e) when (IsConnectionException(e))
                {
                    _onDropped(this);

                    return VsrAttempt.Failed(encoded, e, requestStarted, this);
                }
                catch (OperationCanceledException e)
                {
                    _onDropped(this);

                    return VsrAttempt.Failed(encoded, e, requestStarted, this);
                }
                catch (Exception e)
                {
                    return VsrAttempt.Failed(encoded, e, requestStarted, this);
                }
            }
        }
        catch (OperationCanceledException e)
        {
            if (encoded)
            {
                _onDropped(this);
            }

            return VsrAttempt.Failed(encoded, e, requestStarted, this);
        }
        catch (Exception e)
        {
            return VsrAttempt.Failed(encoded, e, requestStarted, this);
        }
    }

    private async Task<IMemoryOwner<byte>> ReadReplyAsync(long readDeadline, CancellationToken token)
    {
        var remaining = readDeadline - Environment.TickCount64;
        if (remaining <= 0)
        {
            throw new IOException($"Timed out after {_requestTimeoutMs} ms waiting for a consensus reply.");
        }

        // One timer for the whole reply: the deadline covers the frame, not each partial read, so a per-read
        // source would both re-arm the budget and allocate a timer per socket read.
        using var readCancellation = CancellationTokenSource.CreateLinkedTokenSource(token);
        readCancellation.CancelAfter((int)Math.Min(remaining, _requestTimeoutMs));

        await ReadExactAsync(_replyHeaderBuffer, readCancellation.Token, token);

        var command = VsrHeader.PeekCommand(_replyHeaderBuffer);
        if (command == Command2.Eviction)
        {
            var eviction = VsrHeader.ReadEviction(_replyHeaderBuffer);
            _logger.LogWarning("Consensus session evicted by the server: {Reason}", eviction.Reason);
            _onDropped(this);

            throw new VsrSessionEvictedException(VsrReplyDecoder.ToException(eviction));
        }

        if (command != Command2.Reply)
        {
            // Neither a reply nor an eviction: this frame was never an answer to the outstanding request, so
            // whatever the peer does send for it would be read as the next request's reply and handed to the
            // wrong caller. The size field of a frame the client cannot model is no basis for resynchronising.
            _onDropped(this);

            throw VsrError.Exception(VsrError.INVALID_COMMAND,
                $"Unexpected consensus frame {command} on a client connection.");
        }

        int bodySize;
        try
        {
            bodySize = VsrReplyDecoder.ReadBodySize(_replyHeaderBuffer);
            if (VsrHeader.HEADER_SIZE + (long)bodySize > _maxResponseFrameSize)
            {
                throw VsrError.Exception(VsrError.INVALID_COMMAND,
                    $"Reply frame of {VsrHeader.HEADER_SIZE + bodySize} bytes exceeds the configured maximum of " +
                    $"{_maxResponseFrameSize} bytes.");
            }
        }
        catch
        {
            // An announced size the client refuses to read - undersized, oversized - leaves the body on the
            // wire, so the stream no longer sits on a frame boundary and the next reply would decode body bytes
            // as a header.
            _onDropped(this);

            throw;
        }

        if (bodySize == 0)
        {
            VsrReplyDecoder.Decode(_replyHeaderBuffer, ReadOnlyMemory<byte>.Empty);

            return EmptyMemoryOwner.Instance;
        }

        var buffer = ArrayPool<byte>.Shared.Rent(bodySize);
        try
        {
            await ReadExactAsync(buffer.AsMemory(0, bodySize), readCancellation.Token, token);
            ReadOnlyMemory<byte> decoded = VsrReplyDecoder.Decode(_replyHeaderBuffer, buffer.AsMemory(0, bodySize));
            if (decoded.IsEmpty)
            {
                ArrayPool<byte>.Shared.Return(buffer);

                return EmptyMemoryOwner.Instance;
            }

            // The decoded payload is always a suffix of the body - the funnel only strips the leading
            // committed result section.
            return new PooledMemoryOwner(buffer, bodySize - decoded.Length, decoded.Length);
        }
        catch
        {
            ArrayPool<byte>.Shared.Return(buffer);
            throw;
        }
    }

    private async ValueTask ReadExactAsync(Memory<byte> buffer, CancellationToken readToken, CancellationToken token)
    {
        var totalRead = 0;
        while (totalRead < buffer.Length)
        {
            int readBytes;
            try
            {
                readBytes = await _stream.ReadAsync(buffer[totalRead..], readToken);
            }
            catch (OperationCanceledException) when (!token.IsCancellationRequested)
            {
                throw new IOException($"Timed out after {_requestTimeoutMs} ms waiting for a consensus reply.");
            }

            if (readBytes == 0)
            {
                throw new IggyZeroBytesException();
            }

            totalRead += readBytes;
        }
    }

    private static bool IsReplayableTransient(IggyInvalidStatusCodeException error, long transientDeadline,
        long readDeadline)
    {
        if (!error.FromServer)
        {
            return false;
        }

        return error.StatusCode switch
        {
            VsrError.TRANSIENT_NOT_COMMITTED => Environment.TickCount64 < readDeadline,
            VsrError.TRANSIENT_NOT_ACCEPTED => Environment.TickCount64 < transientDeadline,
            _ => false
        };
    }
}

/// <summary>Outcome of one <see cref="VsrConnection.SendAttemptAsync" /> call.</summary>
/// <param name="Encoded">Whether the header was encoded, i.e. whether a request id may have been consumed.</param>
/// <param name="Response">The decoded reply payload, non-null exactly when <paramref name="Error" /> is null.</param>
/// <param name="Error">The failure that ended the attempt, or null on success.</param>
/// <param name="RequestStarted">
///     Whether any byte of the frame was written, which makes the server-side outcome unknowable on failure.
/// </param>
/// <param name="Connection">
///     The connection the attempt ran on, so a caller that drops it after releasing the sending lock can tell
///     its own connection from a replacement a reconnect installed since.
/// </param>
internal readonly record struct VsrAttempt(
    bool Encoded,
    IMemoryOwner<byte>? Response,
    Exception? Error,
    bool RequestStarted,
    VsrConnection? Connection)
{
    public static VsrAttempt Ok(IMemoryOwner<byte> response, VsrConnection connection)
    {
        return new VsrAttempt(true, response, null, true, connection);
    }

    public static VsrAttempt Failed(bool encoded, Exception error, bool requestStarted, VsrConnection? connection)
    {
        return new VsrAttempt(encoded, null, error, requestStarted, connection);
    }
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

/// <summary>Owns a pooled buffer while exposing only the decoded payload slice inside it.</summary>
internal sealed class PooledMemoryOwner(byte[] buffer, int start, int length) : IMemoryOwner<byte>
{
    private int _disposed;

    public Memory<byte> Memory => buffer.AsMemory(start, length);

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}
