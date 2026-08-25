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
using System.IO.Hashing;
using System.Runtime.ExceptionServices;
using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Auth;
using Apache.Iggy.Contracts.Tcp;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Kinds;
using Apache.Iggy.Mappers;
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
public sealed partial class TcpMessageStream : ISessionGenerationProvider
{
    /// <summary>
    ///     Upper bound for a whole VSR request: the transient replays and the leader failovers share it, and so
    ///     do the reply header and body reads. The connection is lockstep, so an unanswered read would hold the
    ///     sending semaphore forever and wedge every later request.
    /// </summary>
    private const int VsrRequestTimeoutMs = 30_000;

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
    ///     transient failover path. Each operation - a request, a connect, a register - spends its own local
    ///     budget, so a long-lived client never latches onto a follower for good.
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

    /// <inheritdoc />
    ulong ISessionGenerationProvider.SessionGeneration => _consensusSession.Generation;

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
        for (var redirects = 0; ; redirects++)
        {
            if (_consensusSession.IsBound)
            {
                await LogoutUserAsync(token);
            }
            else if (State == ConnectionState.Authenticated)
            {
                SetConnectionState(ConnectionState.Connected);
            }

            SetConnectionState(ConnectionState.Authenticating);

            LoginRegisterResponse response;
            try
            {
                using IMemoryOwner<byte> responseBuffer =
                    await SendWithResponseAsync(code, message, autoLoginOnReconnect: false, token: token);

                response = LoginRegister.Deserialize(responseBuffer.Memory.Span);
                _consensusSession.Bind(response.Session);
            }
            catch
            {
                await ResetConsensusSessionAsync();
                if (State == ConnectionState.Authenticating)
                {
                    SetConnectionState(ConnectionState.Connected);
                }

                throw;
            }

            _logger.LogInformation(
                "Authenticated against the server, version {ServerVersion}, protocol version {ServerProtocolVersion}",
                response.ServerVersion, response.ServerProtocolVersion);
            SetConnectionState(ConnectionState.Authenticated);

            var authResponse = new AuthResponse((int)response.UserId, null);
            if (IsConnecting)
            {
                return authResponse;
            }

            if (redirects >= VsrMaxLeaderRedirects)
            {
                _logger.LogWarning("Maximum leader redirections reached while registering, staying on {Address}",
                    _currentAddress);
            }
            else if (await RedirectAsync(token))
            {
                await ConnectAsync(false, token);
                continue;
            }

            // The redirect probe can tear the connection down without throwing, and success on a client that is
            // no longer bound would leave the caller unauthenticated with nothing left to re-authenticate it.
            if (State != ConnectionState.Authenticated)
            {
                throw new NotConnectedException();
            }

            return authResponse;
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
        using IMemoryOwner<byte> responseBuffer =
            await SendWithResponseAsync(CommandCodes.SYNC_CONSUMER_GROUP_CODE, message, token: token);

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
    ///     stream closed for the caller to reconnect. The caller owns the redirect budget: every operation that
    ///     follows redirects - a request, a connect, a register - counts them locally against
    ///     <see cref="VsrMaxLeaderRedirects" />.
    /// </summary>
    private async Task<bool> RedirectAsync(CancellationToken token)
    {
        // The probe issues a request of its own, which takes the sending semaphore, so it cannot run under that
        // lock. Only the commit below does.
        var currentLeaderNode = await GetCurrentLeaderNodeAsync(token);
        if (currentLeaderNode == null)
        {
            return false;
        }

        var leaderAddress = ServerAddress.HostPort(currentLeaderNode.Ip, currentLeaderNode.Endpoints.Tcp);
        // The roster may name the leader by either side of this connection: the address the client dialed
        // (a hostname, an advertised_address) or the endpoint the socket resolved to. IsSame does no DNS
        // resolution, so a match on either means the client is already on the leader; checking only one
        // side redirect-loops the other kind of roster. Both are evidence of where the socket landed, so
        // both come from the connect loop: _currentAddress may already name a leader the client never
        // reached, and matching on it would silently refuse the redirect that gets it there.
        if ((_currentRemoteAddress.Length > 0 && ServerAddress.IsSame(leaderAddress, _currentRemoteAddress))
            || (_connectedAddress.Length > 0 && ServerAddress.IsSame(leaderAddress, _connectedAddress)))
        {
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
            DropVsrConnectionLocked(_connection);
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
        try
        {
            while (true)
            {
                var clusterMetadata = await ReadClusterMetadataNoRedirectAsync(token);
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
        catch (Exception e) when (e is not OperationCanceledException && !VsrConnection.IsConnectionException(e))
        {
            _logger.LogWarning(e, "Failed to read the cluster metadata, continuing on {Address}", _currentAddress);

            return null;
        }
    }

    /// <summary>
    ///     Reads the roster without following redirects: the probe is what redirects are decided from, so a
    ///     probe that redirected or reconnected would reenter the very loop that called it. A probe the current
    ///     node keeps refusing simply fails, and the caller stays where it is.
    /// </summary>
    private async Task<ClusterMetadata?> ReadClusterMetadataNoRedirectAsync(CancellationToken token)
    {
        using IMemoryOwner<byte> responseBuffer = await SendRawAsync(CommandCodes.GET_CLUSTER_METADATA_CODE,
            ReadOnlyMemory<byte>.Empty, token, allowRedirect: false);

        if (responseBuffer.Memory.Length == 0)
        {
            return null;
        }

        return BinaryMapper.MapClusterMetadata(responseBuffer.Memory.Span);
    }

    /// <summary>
    ///     Sends a consensus-framed request: small frames go out as a single coalesced write, larger bodies
    ///     as a second write straight from the caller's buffer.
    /// </summary>
    /// <remarks>
    ///     One deadline bounds the whole request across transient replays AND leader failovers. Login and
    ///     register replay on this connection for the whole budget instead: the connect flow owns leader
    ///     redirection for the handshake, and reconnecting from underneath it would recurse.
    /// </remarks>
    private async Task<IMemoryOwner<byte>> SendRawAsync(int code, ReadOnlyMemory<byte> body,
        CancellationToken token, bool allowRedirect = true)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (State is ConnectionState.Disconnected or ConnectionState.Connecting)
        {
            throw new NotConnectedException();
        }

        var isLoginRegister = code is CommandCodes.LOGIN_REGISTER_CODE or CommandCodes.LOGIN_REGISTER_WITH_PAT_CODE;
        var clearSensitiveReply = HasSensitiveReply(code);
        var overallDeadline = Environment.TickCount64 + VsrRequestTimeoutMs;
        var requestEncoded = false;
        var redirects = 0;
        var redirectBudgetLogged = false;
        VsrConnection? lastConnection = null;

        try
        {
            while (true)
            {
                var transientDeadline = isLoginRegister
                    ? overallDeadline
                    : Math.Min(overallDeadline, Environment.TickCount64 + VsrTransientFailoverCheckMs);

                var attempt = await SendVsrAttemptAsync(code, body, transientDeadline, overallDeadline,
                    clearSensitiveReply, token);
                requestEncoded |= attempt.Encoded;
                lastConnection = attempt.Connection;

                if (attempt.Error is null)
                {
                    return attempt.Response!;
                }

                if (attempt.Error is IggyInvalidStatusCodeException
                    {
                        StatusCode: VsrError.TRANSIENT_NOT_ACCEPTED, FromServer: true
                    }
                    && !isLoginRegister
                    && allowRedirect
                    && Environment.TickCount64 < overallDeadline)
                {
                    if (redirects < VsrMaxLeaderRedirects && await RedirectAsync(token))
                    {
                        redirects++;
                        await ConnectAsync(token);
                    }
                    else if (redirects >= VsrMaxLeaderRedirects && !redirectBudgetLogged)
                    {
                        redirectBudgetLogged = true;
                        _logger.LogWarning("Maximum leader redirections reached, continuing on {Address}",
                            _currentAddress);
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
                await DropVsrConnectionAsync(lastConnection);
            }

            throw;
        }
    }

    /// <summary>
    ///     Whether the failure carries the server's verdict on this request. A lost connection, a reply frame
    ///     the client refused or discarded, and a NOT_COMMITTED that outlived its replay deadline all leave the
    ///     outcome of a request the server may still commit unknowable.
    /// </summary>
    /// <summary>
    ///     Whether the reply body may carry a credential - a raw personal access token, a session secret - and
    ///     therefore must be zeroed before its pooled buffer is handed back for reuse.
    /// </summary>
    private static bool HasSensitiveReply(int code)
    {
        return code is CommandCodes.LOGIN_USER_CODE
            or CommandCodes.LOGIN_REGISTER_CODE
            or CommandCodes.LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE
            or CommandCodes.LOGIN_REGISTER_WITH_PAT_CODE
            or CommandCodes.CREATE_PERSONAL_ACCESS_TOKEN_CODE;
    }

    private static bool IsDefinitiveVerdict(Exception error)
    {
        return error is IggyInvalidStatusCodeException
        {
            FromServer: true,
            StatusCode: not VsrError.TRANSIENT_NOT_COMMITTED
        };
    }

    /// <summary>
    ///     One attempt on the current connection, resolved under the sending lock so the frame and its reply
    ///     cannot be split across two sockets, and so a teardown after the lock is gone can tell this
    ///     connection from a replacement a reconnect installed since.
    /// </summary>
    private async ValueTask<VsrAttempt> SendVsrAttemptAsync(int code, ReadOnlyMemory<byte> body,
        long transientDeadline, long readDeadline, bool clearSensitiveReply, CancellationToken token)
    {
        await _sendingSemaphore.WaitAsync(token);
        try
        {
            var connection = _connection;
            if (connection is null)
            {
                return VsrAttempt.Failed(false, new NotConnectedException(), false, null);
            }

            return await connection.SendAttemptAsync(code, body, transientDeadline, readDeadline,
                clearSensitiveReply, token);
        }
        finally
        {
            _sendingSemaphore.Release();
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
    ///     next request, so the socket cannot be reused. The caller must hold <see cref="_sendingSemaphore" />,
    ///     which owns every write to <see cref="_connection" />.
    /// </summary>
    /// <param name="connection">
    ///     The connection the caller was using. A reconnect that completed in the meantime already closed it and
    ///     re-armed the session, so dropping anything but the live one would tear down a healthy replacement.
    /// </param>
    private void DropVsrConnectionLocked(VsrConnection? connection)
    {
        if (connection is null || !ReferenceEquals(_connection, connection))
        {
            return;
        }

        ResetConsensusSession();
        _connection = null;
        SetConnectionState(ConnectionState.Disconnected);
        connection.Dispose();
    }

    /// <summary>Drops the connection on behalf of a caller that no longer holds the sending lock.</summary>
    private async ValueTask DropVsrConnectionAsync(VsrConnection? connection)
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
            DropVsrConnectionLocked(connection);
        }
        finally
        {
            _sendingSemaphore.Release();
        }
    }
}
