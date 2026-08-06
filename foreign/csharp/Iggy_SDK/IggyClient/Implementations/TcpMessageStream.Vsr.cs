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
using System.IO.Hashing;
using System.Runtime.ExceptionServices;
using Apache.Iggy.ConnectionStream;
using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Auth;
using Apache.Iggy.Contracts.Tcp;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;
using Apache.Iggy.Utils;
using Apache.Iggy.Vsr;
using Microsoft.Extensions.Logging;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.IggyClient.Implementations;

/// <summary>
///     The consensus (VSR) half of the TCP client: the framed request path, the leader redirection, the
///     register handshake, and the client-side partitioning and consumer-group assignment the broker does not
///     resolve server-side. The command surface lives in <see cref="TcpMessageStream" />.
/// </summary>
public sealed partial class TcpMessageStream
{
    /// <summary>
    ///     Upper bound for a whole VSR request: the transient replays and the leader failovers share it, and so
    ///     do the reply header and body reads. The connection is lockstep, so an unanswered read would hold the
    ///     sending semaphore forever and wedge every later request.
    /// </summary>
    private const int VsrRequestTimeoutMs = 30_000;

    /// <summary>Backoff between replays of a transiently refused request.</summary>
    private const int VsrTransientRetryIntervalMs = 50;

    /// <summary>
    ///     Largest body still sent as one contiguous frame with its header. Beyond this the copy outweighs the
    ///     syscall and the extra segment it saves, so header and body go out as two writes.
    /// </summary>
    private const int VsrContiguousFrameLimit = 4 * 1024;

    /// <summary>
    ///     How long a <see cref="VsrError.TRANSIENT_NOT_ACCEPTED" /> request replays on the same connection
    ///     before the leader roster is re-checked. A node that stopped being primary refuses forever, so
    ///     replaying alone never recovers.
    /// </summary>
    private const int VsrTransientFailoverCheckMs = 2_000;

    /// <summary>
    ///     How long a transiently leaderless roster is polled before the connection proceeds on the current
    ///     node anyway. A restarted node cedes the primaryship its stale view assigns it, and the peers need
    ///     about one heartbeat timeout to elect.
    /// </summary>
    private const int VsrLeaderlessWaitMs = 5_000;

    private const int VsrLeaderlessPollMs = 250;

    /// <summary>
    ///     Cap on consecutive leader redirects, so a flapping roster cannot spin the connect loop or the
    ///     transient failover path. The budget is client-wide and resets on a roster check that finds the
    ///     current node is the leader, and on every request that completes, so a client that outlives more
    ///     leader changes than the cap does not latch onto a follower for good.
    /// </summary>
    private const int VsrMaxLeaderRedirects = 3;

    /// <summary>
    ///     Attempts a consumer-group poll gets before it gives up and reports an empty poll: one re-sync after
    ///     the coordinator fences a stale assignment, then one retry.
    /// </summary>
    private const int VsrGroupPollMaxAttempts = 2;

    /// <summary>
    ///     Partition id a fenced group poll echoes instead of a typed error, matching
    ///     <c>RESYNC_REQUIRED_PARTITION_SENTINEL</c> (<c>u32::MAX</c>). The reply header carries no status for an
    ///     empty poll, so the sentinel is the only channel the coordinator has to ask for a re-sync.
    /// </summary>
    private const int VsrResyncRequiredPartitionSentinel = -1;

    /// <summary>
    ///     Shared empty poll result. An idle consumer loop returns one on every iteration, and the instance owns
    ///     no rented buffer - <see cref="EmptyMemoryOwner" /> disposes to nothing - so it is safe to hand out
    ///     repeatedly even after a caller disposes it.
    /// </summary>
    private static readonly PolledMessagesRental EmptyPolledMessages = new(EmptyMemoryOwner.Instance)
    {
        PartitionId = 0,
        CurrentOffset = 0,
        Messages = []
    };

    private readonly ConsensusSession _consensusSession = new();
    private readonly ConsumerGroupClientState _groupState = new();
    private readonly byte[] _vsrReplyHeaderBuffer = new byte[VsrHeader.HEADER_SIZE];

    // The redirect budget is refunded by a completed request, and the roster check a redirect runs is itself a
    // request. Without this the refund lands between the check and the increment that reads the budget, and the
    // counter never leaves zero. Nonzero for the duration of a roster read, so that refund is skipped.
    private int _leaderProbeDepth;

    /// <summary>
    ///     Runs the consensus register handshake and binds the session it commits. Everything before the bind
    ///     is a consumed register on the server, so any failure resets the session: the next attempt must
    ///     re-register under a fresh client id rather than send requests the primary would fence.
    /// </summary>
    /// <remarks>
    ///     A bound connection must commit logout before it can register again. The server treats a register on
    ///     an already-bound transport as an idempotent replay of the existing binding, so re-arming only the
    ///     local session would pair a fresh client id and request counter with the old server session.
    /// </remarks>
    private async Task<AuthResponse?> LoginRegisterAsync(int code, byte[] message, CancellationToken token)
    {
        for (var redirects = 0;; redirects++)
        {
            if (_consensusSession.IsBound)
            {
                await LogoutUserAsync(token);
            }
            else if (_state == ConnectionState.Authenticated)
            {
                SetConnectionStateAsync(ConnectionState.Connected);
            }

            var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
            TcpMessageStreamHelpers.CreatePayload(payload, message, code);

            SetConnectionStateAsync(ConnectionState.Authenticating);

            LoginRegisterResponse response;
            try
            {
                Interlocked.Exchange(ref _skipAutoLoginOnce, 1);
                using IMemoryOwner<byte> responseBuffer = await SendWithResponseAsync(payload, token);

                response = LoginRegister.Deserialize(responseBuffer.Memory.Span);
                _consensusSession.Bind(response.Session);
            }
            catch
            {
                await ResetConsensusSessionAsync();
                if (_state == ConnectionState.Authenticating)
                {
                    SetConnectionStateAsync(ConnectionState.Connected);
                }

                throw;
            }
            finally
            {
                Interlocked.Exchange(ref _skipAutoLoginOnce, 0);
            }

            _logger.LogInformation(
                "Authenticated against the server, version {ServerVersion}, protocol version {ServerProtocolVersion}",
                response.ServerVersion, response.ServerProtocolVersion);
            SetConnectionStateAsync(ConnectionState.Authenticated);

            var authResponse = new AuthResponse((int)response.UserId, null);
            if (IsConnecting)
            {
                return authResponse;
            }

            if (redirects >= VsrMaxLeaderRedirects)
            {
                _logger.LogWarning("Maximum leader redirections reached while registering, staying on {Address}",
                    _currentAddress);

                return authResponse;
            }

            if (!await RedirectAsync(token))
            {
                return authResponse;
            }

            await ConnectAsync(false, token);
        }
    }

    /// <summary>
    ///     Whether the partitioning has to be resolved to an explicit partition id before the request is framed.
    ///     The broker never picks a partition, so balanced and message-key kinds resolve client-side.
    /// </summary>
    private static bool NeedsClientSidePartitioning(Partitioning partitioning)
    {
        return partitioning.Kind != Enums.Partitioning.PartitionId;
    }

    private async Task<SendMessagesResponse> SendMessagesResolvedAsync(Identifier streamId, Identifier topicId,
        Partitioning partitioning, IList<Message> messages, CancellationToken token)
    {
        var resolved = await ResolvePartitioningAsync(streamId, topicId, partitioning, token);

        return await SendMessagesCoreAsync(streamId, topicId, resolved, AsSpan(messages), token);
    }

    /// <summary>
    ///     Resolves balanced and message-key partitioning to an explicit partition id, mirroring
    ///     <c>core/common/src/traits/binary_impls/messages.rs</c>. The VSR broker never picks a partition, so
    ///     sending either kind on the wire would fail to route.
    /// </summary>
    private async ValueTask<Partitioning> ResolvePartitioningAsync(Identifier streamId, Identifier topicId,
        Partitioning partitioning, CancellationToken token)
    {
        var partitionCount = await TopicPartitionCountAsync(streamId, topicId, token);
        if (partitionCount == 0)
        {
            throw VsrError.Exception(VsrError.TOPIC_ID_NOT_FOUND,
                $"Topic {topicId} in stream {streamId} has no partitions to resolve the message to.");
        }

        var partition = partitioning.Kind switch
        {
            Enums.Partitioning.Balanced => _groupState.NextBalancedPartition(new TopicKey(streamId, topicId),
                partitionCount),
            Enums.Partitioning.MessageKey => XxHash32.HashToUInt32(partitioning.Value) % partitionCount,
            _ => throw VsrError.Exception(VsrError.FEATURE_UNAVAILABLE,
                $"Partitioning kind {partitioning.Kind} cannot be resolved to a partition id.")
        };

        return Partitioning.PartitionId((int)partition);
    }

    private async ValueTask<uint> TopicPartitionCountAsync(Identifier streamId, Identifier topicId,
        CancellationToken token)
    {
        var key = new TopicKey(streamId, topicId);
        if (_groupState.PartitionCount(key) is { } cached)
        {
            return cached;
        }

        var topic = await GetTopicByIdAsync(streamId, topicId, token);
        if (topic is null)
        {
            throw VsrError.Exception(VsrError.TOPIC_ID_NOT_FOUND,
                $"Topic {topicId} was not found in stream {streamId}.");
        }

        _groupState.SetPartitionCount(key, topic.PartitionsCount);

        return topic.PartitionsCount;
    }

    /// <summary>
    ///     Polls one of the group member's assigned partitions, round-robin. A fence rejection - either the typed
    ///     error or the sentinel partition id an empty poll carries - re-syncs the assignment and retries once.
    /// </summary>
    private async Task<PolledMessagesRental> PollGroupMessagesRentedAsync(Identifier streamId, Identifier topicId,
        Consumer consumer, PollingStrategy pollingStrategy, uint count, bool autoCommit, CancellationToken token)
    {
        var key = new GroupKey(streamId, topicId, consumer.ConsumerId);
        if (!_groupState.HasAssignment(key))
        {
            await SyncGroupAssignmentAsync(streamId, topicId, consumer.ConsumerId, token);
        }

        for (var attempt = 0; attempt < VsrGroupPollMaxAttempts; attempt++)
        {
            if (_groupState.NextGroupPartition(key) is not { } partitionId)
            {
                if (!_groupState.IsRegistered(key))
                {
                    throw VsrError.Exception(VsrError.CONSUMER_GROUP_MEMBER_NOT_FOUND,
                        $"Client is not a member of consumer group {consumer.ConsumerId} on topic {topicId}.");
                }

                return EmptyPolledMessages;
            }

            PolledMessagesRental? rental = null;
            try
            {
                rental = await PollPartitionMessagesRentedAsync(streamId, topicId, partitionId, consumer,
                    pollingStrategy, count, autoCommit, token);
            }
            catch (IggyInvalidStatusCodeException e) when (e is
                                                           {
                                                               StatusCode: VsrError.CONSUMER_GROUP_PARTITION_NOT_OWNED,
                                                               FromServer: true
                                                           })
            {
                // Both fence shapes - the typed error and the sentinel an empty poll carries - land on the same
                // re-sync below.
            }

            if (rental is not null)
            {
                if (rental.Messages.Count != 0 || rental.PartitionId != VsrResyncRequiredPartitionSentinel)
                {
                    return rental;
                }

                rental.Dispose();
            }

            _groupState.InvalidateAssignment(key);
            await SyncGroupAssignmentAsync(streamId, topicId, consumer.ConsumerId, token);
        }

        return EmptyPolledMessages;
    }

    /// <summary>
    ///     Pulls the requesting member's assignment from the coordinator into the cache. An empty reply means the
    ///     client is not a member: the coordinator answers with an assignment header for any member, including
    ///     one holding zero partitions.
    /// </summary>
    private async Task SyncGroupAssignmentAsync(Identifier streamId, Identifier topicId, Identifier groupId,
        CancellationToken token)
    {
        var message = TcpContracts.GetGroup(streamId, topicId, groupId);
        var payload = new byte[4 + BufferSizes.INITIAL_BYTES_LENGTH + message.Length];
        TcpMessageStreamHelpers.CreatePayload(payload, message, CommandCodes.SYNC_CONSUMER_GROUP_CODE);

        using IMemoryOwner<byte> responseBuffer = await SendWithResponseAsync(payload, token);

        var key = new GroupKey(streamId, topicId, groupId);
        if (responseBuffer.Memory.Length == 0)
        {
            // Deregistering is the only thing that observes a server-side removal (group deleted, member
            // evicted); without it membership latches true and every later poll returns empty.
            _groupState.DeregisterGroup(key);

            return;
        }

        var assignment = SyncConsumerGroupAssignment.Decode(responseBuffer.Memory.Span);
        _groupState.RegisterGroup(key, streamId, topicId, groupId);
        _groupState.SetAssignment(key, assignment.Generation, assignment.Partitions);
    }

    /// <summary>
    ///     Re-syncs every joined group so a widened assignment (a partition-count change, say) is picked up
    ///     without first hitting an ownership fence. One failing group is logged and skipped so it cannot stall
    ///     the rest.
    /// </summary>
    private async Task RefreshGroupAssignmentsAsync(CancellationToken token)
    {
        foreach (var group in _groupState.RegisteredGroups())
        {
            try
            {
                await SyncGroupAssignmentAsync(group.StreamId, group.TopicId, group.GroupId, token);
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                _logger.LogWarning(e,
                    "Failed to refresh the consumer group assignment for {StreamId}|{TopicId}|{GroupId}",
                    group.StreamId, group.TopicId, group.GroupId);
            }
        }
    }


    /// <summary>
    ///     Points the client at the current leader when it is not the node this connection is on, leaving the
    ///     stream closed for the caller to reconnect. The redirect budget is client-wide: the connect loop and
    ///     the transient failover path spend the same counter, and it is refunded as soon as a roster check
    ///     lands on the leader.
    /// </summary>
    private async Task<bool> RedirectAsync(CancellationToken token)
    {
        // The probe issues a request of its own, which takes the sending semaphore, so it cannot run under that
        // lock. Only the commit below does.
        var currentLeaderNode = await GetCurrentLeaderNodeAsync(token);
        if (currentLeaderNode == null)
        {
            Interlocked.Exchange(ref _leaderRedirectCount, 0);
            return false;
        }

        var leaderAddress = ServerAddress.HostPort(currentLeaderNode.Ip, currentLeaderNode.Endpoints.Tcp);
        if (ServerAddress.IsSame(leaderAddress, _currentAddress))
        {
            Interlocked.Exchange(ref _leaderRedirectCount, 0);
            return false;
        }

        if (Interlocked.Increment(ref _leaderRedirectCount) > VsrMaxLeaderRedirects)
        {
            _logger.LogWarning("Maximum leader redirections reached, continuing on {Address}", _currentAddress);
            return false;
        }

        _logger.LogInformation("Leader address changed. Trying to reconnect to {Address}",
            leaderAddress);

        // The address move and the drop are one step: a reader that saw the new address but the old stream would
        // find every later redirect short-circuited by the address check above, with no path back to the leader.
        await _sendingSemaphore.WaitAsync(token);
        try
        {
            _currentAddress = leaderAddress;
            DropVsrConnectionLocked(_stream);
        }
        finally
        {
            _sendingSemaphore.Release();
        }

        return true;
    }

    private async Task<ClusterNode?> GetCurrentLeaderNodeAsync(CancellationToken token)
    {
        var leaderlessDeadline = Environment.TickCount64 + VsrLeaderlessWaitMs;
        Interlocked.Increment(ref _leaderProbeDepth);
        try
        {
            while (true)
            {
                var clusterMetadata = await GetClusterMetadataAsync(token);
                if (clusterMetadata == null)
                {
                    return null;
                }

                if (clusterMetadata.Nodes.Count() == 1)
                {
                    return null;
                }

                var leaderNode = clusterMetadata.Nodes.FirstOrDefault(x =>
                    x.Role == ClusterNodeRole.Leader && x.Status == ClusterNodeStatus.Healthy);
                if (leaderNode != null)
                {
                    return leaderNode;
                }

                if (Environment.TickCount64 >= leaderlessDeadline)
                {
                    _logger.LogWarning("No leader in the cluster metadata after {WaitMs} ms, continuing on {Address}",
                        VsrLeaderlessWaitMs, _currentAddress);

                    return null;
                }

                await Task.Delay(VsrLeaderlessPollMs, token);
            }
        }
        // todo: change after error refactoring, error code 5 is for feature not supported
        catch (IggyInvalidStatusCodeException e) when (e is
                                                       {
                                                           StatusCode: VsrError.FEATURE_UNAVAILABLE, FromServer: true
                                                       })
        {
            return null;
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            _logger.LogWarning(e, "Failed to read the cluster metadata, continuing on {Address}", _currentAddress);

            return null;
        }
        finally
        {
            Interlocked.Decrement(ref _leaderProbeDepth);
        }
    }

    /// <summary>
    ///     Sends a consensus-framed request. The call sites still build the classic
    ///     <c>[size u32][code u32][body]</c> buffer, so the code is read back from it here and the body is written
    ///     right after the 256-byte consensus header - two writes, no concatenation.
    /// </summary>
    /// <remarks>
    ///     One deadline bounds the whole request across transient replays AND leader failovers. Login and
    ///     register replay on this connection for the whole budget instead: the connect flow owns leader
    ///     redirection for the handshake, and reconnecting from underneath it would recurse.
    /// </remarks>
    private async Task<IMemoryOwner<byte>> SendRawVsrAsync(ReadOnlyMemory<byte> payload, CancellationToken token)
    {
        var code = (int)BinaryPrimitives.ReadUInt32LittleEndian(payload.Span.Slice(4, 4));
        ReadOnlyMemory<byte> body = payload[8..];
        var isLoginRegister = code is CommandCodes.LOGIN_REGISTER_CODE or CommandCodes.LOGIN_REGISTER_WITH_PAT_CODE;
        var overallDeadline = Environment.TickCount64 + VsrRequestTimeoutMs;
        var headerBuffer = ArrayPool<byte>.Shared.Rent(VsrHeader.HEADER_SIZE);
        Memory<byte> header = headerBuffer.AsMemory(0, VsrHeader.HEADER_SIZE);
        var requestEncoded = false;
        TcpConnectionStream? lastStream = null;

        try
        {
            while (true)
            {
                var transientDeadline = isLoginRegister
                    ? overallDeadline
                    : Math.Min(overallDeadline, Environment.TickCount64 + VsrTransientFailoverCheckMs);

                var attempt = await SendVsrAttemptAsync(code, body, header, transientDeadline, overallDeadline,
                    token);
                requestEncoded |= attempt.Encoded;
                lastStream = attempt.Stream;

                if (attempt.Error is null)
                {
                    // A roster read taken by RedirectAsync must not refund the budget it is about to be charged
                    // against, or the cap can never be reached.
                    if (Volatile.Read(ref _leaderRedirectCount) != 0 && Volatile.Read(ref _leaderProbeDepth) == 0)
                    {
                        Interlocked.Exchange(ref _leaderRedirectCount, 0);
                    }

                    return attempt.Response!;
                }

                if (attempt.Error is IggyInvalidStatusCodeException
                    {
                        StatusCode: VsrError.TRANSIENT_NOT_ACCEPTED, FromServer: true
                    }
                    && !isLoginRegister
                    && Environment.TickCount64 < overallDeadline)
                {
                    if (await RedirectAsync(token))
                    {
                        await ConnectAsync(token);
                    }

                    continue;
                }

                if (attempt.Error is VsrSessionEvictedException evicted)
                {
                    if (attempt.RequestStarted && !VsrOperations.IsReplaySafeRead(code, isLoginRegister, body.Span))
                    {
                        throw new VsrRequestOutcomeUnknownException(evicted);
                    }

                    ExceptionDispatchInfo.Throw(evicted.Verdict);
                }

                if (attempt.RequestStarted
                    && !VsrOperations.IsReplaySafeRead(code, isLoginRegister, body.Span)
                    && !IsDefinitiveVerdict(attempt.Error))
                {
                    throw new VsrRequestOutcomeUnknownException(attempt.Error);
                }

                ExceptionDispatchInfo.Throw(attempt.Error);
            }
        }
        catch (OperationCanceledException)
        {
            if (requestEncoded)
            {
                await DropVsrConnectionAsync(lastStream);
            }

            throw;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(headerBuffer);
        }
    }

    /// <summary>
    ///     Whether the failure carries the server's verdict on this request. A lost connection, a reply frame
    ///     the client refused or discarded, and a NOT_COMMITTED that outlived its replay deadline all leave the
    ///     outcome of a request the server may still commit unknowable.
    /// </summary>
    private static bool IsDefinitiveVerdict(Exception error)
    {
        return error is IggyInvalidStatusCodeException
        {
            FromServer: true,
            StatusCode: not VsrError.TRANSIENT_NOT_COMMITTED
        };
    }

    /// <summary>
    ///     One attempt on the current connection: encode the header into <paramref name="header" />, write the
    ///     frame, and replay it - same session, same request id - while the server answers transiently.
    /// </summary>
    private async ValueTask<VsrAttempt> SendVsrAttemptAsync(int code, ReadOnlyMemory<byte> body, Memory<byte> header,
        long transientDeadline, long readDeadline, CancellationToken token)
    {
        await _sendingSemaphore.WaitAsync(token);

        var encoded = false;
        var requestStarted = false;
        byte[]? frameBuffer = null;

        // Read the stream once for the whole attempt. Nothing may swap the field without the sending lock this
        // call holds, so the frame and its reply cannot be split across two sockets, and a teardown after the
        // lock is gone can tell this connection from a replacement a reconnect installed since.
        var stream = _stream;
        try
        {
            // A small request goes out as one write. Two writes cost two syscalls and, with Nagle disabled, two
            // TCP segments (two TLS records when encrypted) for what the reference encoder sends as a single
            // contiguous frame. Above the threshold the copy costs more than the extra write saves. Renting
            // inside the try keeps the semaphore paired with its release even if the pool throws.
            if (body.Length <= VsrContiguousFrameLimit)
            {
                frameBuffer = ArrayPool<byte>.Shared.Rent(VsrHeader.HEADER_SIZE + body.Length);
            }

            VsrHeader.EncodeRequestHeader(header.Span, _consensusSession, code, body.Span);

            encoded = true;

            var frame = Memory<byte>.Empty;
            if (frameBuffer is not null)
            {
                frame = frameBuffer.AsMemory(0, VsrHeader.HEADER_SIZE + body.Length);
                header.CopyTo(frame);
                body.CopyTo(frame[VsrHeader.HEADER_SIZE..]);
            }

            while (true)
            {
                try
                {
                    // Everything that fails without reaching the socket has to fail before this point: past it a
                    // failure is reported as an outcome the server alone knows, which for a replicated write
                    // tells the caller its request may have committed twice.
                    token.ThrowIfCancellationRequested();
                    requestStarted = true;

                    if (frameBuffer is not null)
                    {
                        await stream.SendAsync(frame, token);
                    }
                    else
                    {
                        await stream.SendAsync(header, token);
                        await stream.SendAsync(body, token);
                    }

                    await stream.FlushAsync(token);

                    IMemoryOwner<byte> response = await ReadVsrReplyAsync(stream, readDeadline, token);

                    return VsrAttempt.Ok(response, stream);
                }
                catch (IggyInvalidStatusCodeException e) when (IsReplayableTransient(e, transientDeadline,
                                                                   readDeadline))
                {
                    var governingDeadline = e.StatusCode == VsrError.TRANSIENT_NOT_COMMITTED
                        ? readDeadline
                        : transientDeadline;
                    var remaining = governingDeadline - Environment.TickCount64;
                    await Task.Delay((int)Math.Clamp(remaining, 0, VsrTransientRetryIntervalMs), token);
                }
                catch (Exception e) when (IsConnectionException(e))
                {
                    DropVsrConnectionLocked(stream);

                    return VsrAttempt.Failed(encoded, e, requestStarted, stream);
                }
                catch (OperationCanceledException e)
                {
                    DropVsrConnectionLocked(stream);

                    return VsrAttempt.Failed(encoded, e, requestStarted, stream);
                }
                catch (Exception e)
                {
                    return VsrAttempt.Failed(encoded, e, requestStarted, stream);
                }
            }
        }
        catch (OperationCanceledException e)
        {
            if (encoded)
            {
                DropVsrConnectionLocked(stream);
            }

            return VsrAttempt.Failed(encoded, e, requestStarted, stream);
        }
        catch (Exception e)
        {
            return VsrAttempt.Failed(encoded, e, requestStarted, stream);
        }
        finally
        {
            if (frameBuffer is not null)
            {
                ArrayPool<byte>.Shared.Return(frameBuffer);
            }

            _sendingSemaphore.Release();
        }
    }

    private async Task<IMemoryOwner<byte>> ReadVsrReplyAsync(TcpConnectionStream stream, long readDeadline,
        CancellationToken token)
    {
        var remaining = readDeadline - Environment.TickCount64;
        if (remaining <= 0)
        {
            throw new IOException($"Timed out after {VsrRequestTimeoutMs} ms waiting for a consensus reply.");
        }

        // One timer for the whole reply: the deadline covers the frame, not each partial read, so a per-read
        // source would both re-arm the budget and allocate a timer per socket read.
        using var readCancellation = CancellationTokenSource.CreateLinkedTokenSource(token);
        readCancellation.CancelAfter((int)Math.Min(remaining, VsrRequestTimeoutMs));

        await ReadExactVsrAsync(stream, _vsrReplyHeaderBuffer, readCancellation.Token, token);

        var command = VsrHeader.PeekCommand(_vsrReplyHeaderBuffer);
        if (command == Command2.Eviction)
        {
            var eviction = VsrHeader.ReadEviction(_vsrReplyHeaderBuffer);
            _logger.LogWarning("Consensus session evicted by the server: {Reason}", eviction.Reason);
            DropVsrConnectionLocked(stream);

            throw new VsrSessionEvictedException(VsrReplyDecoder.ToException(eviction));
        }

        if (command != Command2.Reply)
        {
            // Neither a reply nor an eviction: this frame was never an answer to the outstanding request, so
            // whatever the peer does send for it would be read as the next request's reply and handed to the
            // wrong caller. The size field of a frame the client cannot model is no basis for resynchronising.
            DropVsrConnectionLocked(stream);

            throw VsrError.Exception(VsrError.INVALID_COMMAND,
                $"Unexpected consensus frame {command} on a client connection.");
        }

        int bodySize;
        try
        {
            bodySize = VsrReplyDecoder.ReadBodySize(_vsrReplyHeaderBuffer);
            if (VsrHeader.HEADER_SIZE + (long)bodySize > _configuration.MaxResponseFrameSize)
            {
                throw VsrError.Exception(VsrError.INVALID_COMMAND,
                    $"Reply frame of {VsrHeader.HEADER_SIZE + bodySize} bytes exceeds the configured maximum of " +
                    $"{_configuration.MaxResponseFrameSize} bytes.");
            }
        }
        catch
        {
            // An announced size the client refuses to read - undersized, oversized - leaves the body on the
            // wire, so the stream no longer sits on a frame boundary and the next reply would decode body bytes
            // as a header.
            DropVsrConnectionLocked(stream);

            throw;
        }

        if (bodySize == 0)
        {
            VsrReplyDecoder.Decode(_vsrReplyHeaderBuffer, ReadOnlyMemory<byte>.Empty);

            return EmptyMemoryOwner.Instance;
        }

        var buffer = ArrayPool<byte>.Shared.Rent(bodySize);
        try
        {
            await ReadExactVsrAsync(stream, buffer.AsMemory(0, bodySize), readCancellation.Token, token);
            ReadOnlyMemory<byte> decoded = VsrReplyDecoder.Decode(_vsrReplyHeaderBuffer, buffer.AsMemory(0, bodySize));
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

    private async ValueTask ReadExactVsrAsync(TcpConnectionStream stream, Memory<byte> buffer,
        CancellationToken readToken,
        CancellationToken token)
    {
        var totalRead = 0;
        while (totalRead < buffer.Length)
        {
            int readBytes;
            try
            {
                readBytes = await stream.ReadAsync(buffer[totalRead..], readToken);
            }
            catch (OperationCanceledException) when (!token.IsCancellationRequested)
            {
                throw new IOException($"Timed out after {VsrRequestTimeoutMs} ms waiting for a consensus reply.");
            }

            if (readBytes == 0)
            {
                throw new IggyZeroBytesException();
            }

            totalRead += readBytes;
        }
    }

    /// <summary>
    ///     Drops the consensus session and the group state scoped to it. Consumer-group assignments are fenced by
    ///     a generation the coordinator tracks per session, so carrying them into a new session would fence every
    ///     poll until the first re-sync. The balanced cursors and the partition counts survive: neither is bound
    ///     to a session, and dropping them costs a metadata round trip per topic on the next produce.
    /// </summary>
    private void ResetConsensusSession()
    {
        _consensusSession.Reset();
        _groupState.ClearSessionScoped();
    }

    /// <summary>
    ///     Resets the session on behalf of a caller that does not hold the sending lock, so no request can be
    ///     encoding against the identity while it is re-armed.
    /// </summary>
    private async ValueTask ResetConsensusSessionAsync()
    {
        // Taking a disposed semaphore would replace the failure the caller is about to rethrow with an
        // ObjectDisposedException, and a disposed client has nothing left to fence. Dispose can still land
        // between the check and the wait, so the wait itself has to tolerate it.
        if (_disposed || !await TryEnterSendingSemaphoreAsync())
        {
            return;
        }

        try
        {
            ResetConsensusSession();
        }
        finally
        {
            _sendingSemaphore.Release();
        }
    }

    /// <summary>
    ///     Takes the sending lock for a teardown that must not fail. Returns false once the client is disposed:
    ///     the caller is unwinding an earlier failure and has nothing left to fence.
    /// </summary>
    private async ValueTask<bool> TryEnterSendingSemaphoreAsync()
    {
        try
        {
            await _sendingSemaphore.WaitAsync(CancellationToken.None);
            return true;
        }
        catch (ObjectDisposedException)
        {
            return false;
        }
    }

    /// <summary>
    ///     Drops the connection along with the session. A late or half-read reply would desync the framing of the
    ///     next request, so the stream cannot be reused. The caller must hold <see cref="_sendingSemaphore" />,
    ///     which owns every write to <see cref="_stream" />.
    /// </summary>
    /// <param name="stream">
    ///     The connection the caller was using. A reconnect that completed in the meantime already closed it and
    ///     re-armed the session, so dropping anything but the live one would tear down a healthy replacement.
    /// </param>
    private void DropVsrConnectionLocked(TcpConnectionStream? stream)
    {
        if (!ReferenceEquals(_stream, stream))
        {
            return;
        }

        ResetConsensusSession();
        _stream?.Close();
        SetConnectionStateAsync(ConnectionState.Disconnected);
    }

    /// <summary>Drops the connection on behalf of a caller that no longer holds the sending lock.</summary>
    private async ValueTask DropVsrConnectionAsync(TcpConnectionStream? stream)
    {
        // Dispose already closed the stream, and taking a disposed semaphore here would replace the
        // cancellation the caller is about to rethrow with an ObjectDisposedException. Dispose can still land
        // between the check and the wait, so the wait itself has to tolerate it.
        // The request this drop belongs to was cancelled; the drop itself still has to run to completion.
        if (_disposed || !await TryEnterSendingSemaphoreAsync())
        {
            return;
        }

        try
        {
            DropVsrConnectionLocked(stream);
        }
        finally
        {
            _sendingSemaphore.Release();
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

    /// <summary>Outcome of one <see cref="SendVsrAttemptAsync" /> call on the current connection.</summary>
    /// <param name="Encoded">Whether the header was encoded, i.e. whether a request id may have been consumed.</param>
    /// <param name="Response">The decoded reply payload, non-null exactly when <paramref name="Error" /> is null.</param>
    /// <param name="Error">The failure that ended the attempt, or null on success.</param>
    /// <param name="RequestStarted">
    ///     Whether any byte of the frame was written, which makes the server-side outcome unknowable on failure.
    /// </param>
    /// <param name="Stream">
    ///     The connection the attempt ran on, so a caller that drops it after releasing the sending lock can tell
    ///     its own connection from a replacement a reconnect installed since.
    /// </param>
    private readonly record struct VsrAttempt(
        bool Encoded,
        IMemoryOwner<byte>? Response,
        Exception? Error,
        bool RequestStarted,
        TcpConnectionStream? Stream)
    {
        public static VsrAttempt Ok(IMemoryOwner<byte> response, TcpConnectionStream stream)
        {
            return new VsrAttempt(true, response, null, true, stream);
        }

        public static VsrAttempt Failed(bool encoded, Exception error, bool requestStarted,
            TcpConnectionStream? stream)
        {
            return new VsrAttempt(encoded, null, error, requestStarted, stream);
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
}
