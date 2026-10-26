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
using Apache.Iggy.Configuration;
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
///     The consensus (VSR) session lifecycle, register handshake, client-side partitioning, and consumer-group
///     assignment the broker does not
///     resolve server-side. The command surface lives in <see cref="TcpMessageStream" />.
/// </summary>
public sealed partial class TcpMessageStream : ISessionGenerationProvider
{
    /// <summary>
    ///     Admission, routing, and retries share one call deadline. Each link also bounds incomplete frame
    ///     transfers independently, so cancellation cannot leave its reader or writer stalled forever.
    /// </summary>
    private const int VsrRequestTimeoutMs = 30_000;


    /// <summary>
    ///     First pause before a <see cref="VsrError.LIFECYCLE_BUSY" /> request goes out again. Every retry is a
    ///     new request that commits another refusal, so the pause doubles up to
    ///     <see cref="LifecycleRetryMaxIntervalMs" />.
    /// </summary>
    private const int LifecycleRetryIntervalMs = 50;

    private const int LifecycleRetryMaxIntervalMs = 1_000;




    /// <summary>
    ///     Bound on one endpoint's dial while other endpoints are queued behind it. Neither the connect nor the
    ///     TLS handshake has a deadline of its own, so a node whose syns are dropped would hold the sweep for
    ///     the whole kernel connect timeout - minutes - while a survivor goes untried. Matches the Rust SDK.
    /// </summary>
    private const int FailoverDialTimeout = 2_000;

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
    private const uint VsrResyncRequiredPartitionSentinel = uint.MaxValue;

    /// <summary>
    ///     Shared empty poll result for a group member that currently owns no partition, carrying
    ///     <see cref="PolledMessages.NoAssignedPartition" /> so a consumer can back off instead of re-polling at
    ///     once. Same ownership rules as <see cref="EmptyPolledMessages" />.
    /// </summary>
    private static readonly PolledMessagesRental NoAssignedPartitionPolledMessages = new(EmptyMemoryOwner.Instance)
    {
        PartitionId = PolledMessages.NoAssignedPartition,
        CurrentOffset = 0,
        Messages = []
    };

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

    private ulong BeginExplicitSignIn()
    {
        lock (_pollRoutingGate)
        {
            var owner = _consensusSession.BeginCallerSession();
            _signInWithdrawn = true;
            _rememberedLogin = null;
            _rememberedUserId = null;
            _sessionRecoveryCancellation?.Cancel();
            return owner;
        }
    }

    private void EnsureSignInOwner(ulong owner)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_consensusSession.Ended(owner) is { } ended)
        {
            throw ended;
        }
    }

    private async Task<AuthResponse?> LoginRegisterAsync(AutoLoginSettings credentials, bool personalAccessToken,
        CancellationToken token)
    {
        var owner = BeginExplicitSignIn();
        var acquired = false;
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token, _clientLifetime.Token);
        deadline.CancelAfter(VsrRequestTimeoutMs);
        try
        {
            var message = SerializeSignIn(credentials, personalAccessToken);
            if (!personalAccessToken && State == ConnectionState.Disconnected)
            {
                throw new NotConnectedException();
            }
            await _connectGate.WaitAsync(deadline.Token);
            acquired = true;
            EnsureSignInOwner(owner);
            var code = personalAccessToken ? CommandCodes.LOGIN_REGISTER_WITH_PAT_CODE : CommandCodes.LOGIN_REGISTER_CODE;
            return await LoginRegisterCoreAsync(code, message, credentials, deadline.Token, owner, continues: true,
                explicitSignIn: true);
        }
        catch (Exception error)
        {
            await ResetConsensusSessionAsync(error, owner: owner, interrupt: true);
            if (deadline.IsCancellationRequested && !token.IsCancellationRequested
                && !_clientLifetime.IsCancellationRequested
                && error is VsrRequestOutcomeUnknownException or OperationCanceledException)
            {
                throw VsrError.Exception(VsrError.TRANSIENT_NOT_ACCEPTED,
                    "No primary accepted the sign-in before its deadline.");
            }
            throw;
        }
        finally
        {
            if (acquired)
            {
                _connectGate.Release();
            }
        }
    }

    private async Task<AuthResponse?> LoginRegisterCoreAsync(int code, byte[] message, AutoLoginSettings credentials,
        CancellationToken token, ulong owner, bool continues, bool explicitSignIn = false)
    {
        EnsureSignInOwner(owner);
        if (_consensusSession.IsBound)
        {
            await LogoutCoreAsync(token, owner);
        }
        else if (explicitSignIn && _consensusSession.RegisterPending)
        {
            await ResetConsensusSessionAsync(owner: owner, continues: true, interrupt: true);
        }
        if (ReadyReplicaLinks().Length == 0)
        {
            await TryEstablishConnectionAsync(false, token, owner);
        }
        EnsureSignInOwner(owner);
        SetConnectionState(ConnectionState.Authenticating);
        var bound = false;
        try
        {
            using var responseBuffer = await SendWithResponseAsync(code, message, token: token, startedUnder: owner);
            var response = LoginRegister.Deserialize(responseBuffer.Memory.Span);
            lock (_pollRoutingGate)
            {
                EnsureSignInOwner(owner);
                _consensusSession.Bind(response.Session, LoginRegister.ReadBindSecret(message), continues);
                owner = _consensusSession.Generation;
                bound = true;
                SetConnectionState(ConnectionState.Authenticated);
            }
            try
            {
                var metadata = await ReadClusterMetadataOnReadyLinkAsync(token);
                lock (_pollRoutingGate)
                {
                    EnsureSignInOwner(owner);
                    if (metadata is not null)
                    {
                        RememberRoster(metadata);
                    }
                }
            }
            catch (IggyInvalidStatusCodeException error) when (error.StatusCode == VsrError.FEATURE_UNAVAILABLE)
            {
            }
            lock (_pollRoutingGate)
            {
                EnsureSignInOwner(owner);
                // A committed password update can replace the snapshot used by this automatic attempt.
                if (explicitSignIn || _rememberedLogin is null || ReferenceEquals(_rememberedLogin, credentials))
                {
                    _rememberedLogin = credentials;
                }
                _rememberedUserId = response.UserId;
                _signInWithdrawn = false;
                StartHeartbeat();
            }
            return new AuthResponse(response.UserId, null);
        }
        catch (Exception error) when (!explicitSignIn)
        {
            var refused = !bound && IsSignInRefusal(error);
            var updated = false;
            lock (_pollRoutingGate)
            {
                if (_consensusSession.Ended(owner) is null)
                {
                    updated = refused && _rememberedLogin is { } latest && !ReferenceEquals(credentials, latest);
                    if (refused && !updated)
                    {
                        _signInWithdrawn = true;
                        _rememberedLogin = null;
                        _rememberedUserId = null;
                    }
                }
            }
            var canceled = token.IsCancellationRequested;
            // A session that the recovery bound outlives a failed roster read, so its next attempt resumes it.
            if (continues && bound && !canceled && !IsSessionFailure(error))
            {
                await DropConnectionAsync();
                SetConnectionState(ConnectionState.Disconnected);
                throw;
            }
            // After a register that was not sent or admitted, lost its link or has an unknown outcome, the recovery
            // signs in again under a new client id. A register that did commit leaves an idle session, which lease
            // expiry retires.
            var retryable = continues && !bound && (IsTransientFailure(error)
                || error is VsrRequestOutcomeUnknownException or OperationCanceledException);
            await ResetConsensusSessionAsync(canceled ? null : error,
                stops: continues && !canceled && !refused && !retryable, owner: owner,
                continues: updated || retryable);
            throw;
        }
    }

    private async Task<bool> ResumeSessionAsync(CancellationToken token, ulong owner)
    {
        await _sendingSemaphore.WaitAsync(token);
        try
        {
            if (_consensusSession.Session is not { } session || _connection is not { } connection)
            {
                return false;
            }
            var attempt = await BindSessionAsync(connection, _consensusSession.ClientId, session, token);
            if (attempt.Error is IggyInvalidStatusCodeException
                {
                    FromServer: true,
                    StatusCode: VsrError.UNAUTHENTICATED or VsrError.STALE_CLIENT
                })
            {
                lock (_pollRoutingGate)
                {
                    EnsureSignInOwner(owner);
                    ResetConsensusSession(continues: true);
                }
                return false;
            }
            if (attempt.Error is { } error)
            {
                ExceptionDispatchInfo.Throw(CallerError(error));
            }
            using var response = attempt.Response!;
            var bound = LoginRegister.Deserialize(response.Memory.Span);
            if (bound.Session != session)
            {
                throw VsrError.Exception(VsrError.SESSION_MISMATCH, "Bind reply changed the logical session.");
            }
            lock (_pollRoutingGate)
            {
                EnsureSignInOwner(owner);
                SetConnectionState(ConnectionState.Authenticated);
            }
            return true;
        }
        finally
        {
            _sendingSemaphore.Release();
        }
    }

    /// <summary>
    ///     Sends one BindSession for the session on <paramref name="connection" />, stamped with the metadata
    ///     watermark. The caller decides what a refusal or another session in the reply means.
    /// </summary>
    private ValueTask<VsrAttempt> BindSessionAsync(VsrConnection connection, UInt128 clientId, ulong session,
        CancellationToken token)
    {
        var identity = new byte[ConsumerSessionSize];
        BinaryPrimitives.WriteUInt128LittleEndian(identity, clientId);
        BinaryPrimitives.WriteUInt64LittleEndian(identity.AsSpan(ConsumerSessionEpochOffset), session);
        lock (_pollRoutingGate)
        {
            BinaryPrimitives.WriteUInt64LittleEndian(identity.AsSpan(ConsumerSessionWatermarkOffset), _metadataWatermark);
        }
        var deadline = Environment.TickCount64 + VsrRequestTimeoutMs;
        return connection.SendAttemptAsync(CommandCodes.BIND_SESSION_CODE,
            LoginRegister.SerializeBindSession(identity, _consensusSession.BindSecret), deadline, deadline,
            true, token, retryTransient: false);
    }

    /// <summary>
    ///     Whether the partitioning has to be resolved to an explicit partition id before the request is framed.
    ///     This client resolves balanced and message-key kinds using its partition cache and cursor.
    /// </summary>
    private static bool NeedsClientSidePartitioning(Partitioning partitioning)
    {
        return partitioning.Kind != Enums.Partitioning.PartitionId;
    }

    private async Task<SendMessagesResponse> SendMessagesResolvedAsync(Identifier streamId, Identifier topicId,
        Partitioning partitioning, IList<Message> messages, CancellationToken token)
    {
        var deadline = Environment.TickCount64 + VsrRequestTimeoutMs;
        var generation = _consensusSession.Generation;
        using var selection = CancellationTokenSource.CreateLinkedTokenSource(token);
        selection.CancelAfter(VsrRequestTimeoutMs);
        try
        {
            for (var attempt = 0; ; attempt++)
            {
                // The send takes the session it starts under, so a sign-in or sign-out by the caller while the
                // partition resolves ends the call, whatever the resolution came to.
                Partitioning resolved;
                try
                {
                    resolved = await ResolvePartitioningAsync(streamId, topicId, partitioning, selection.Token);
                }
                catch (Exception) when (!token.IsCancellationRequested
                    && _consensusSession.Ended(generation) is { } endedWhileResolving)
                {
                    throw endedWhileResolving;
                }
                if (_consensusSession.Ended(generation) is { } ended)
                {
                    throw ended;
                }
                try
                {
                    return await SendMessagesCoreAsync(streamId, topicId, resolved, AsSpan(messages), token, deadline,
                        generation);
                }
                catch (IggyInvalidStatusCodeException error) when (attempt == 0 && error.FromServer
                    && error.StatusCode == VsrError.PARTITION_NOT_FOUND)
                {
                    _groupState.InvalidateTopic(new TopicKey(streamId, topicId));
                }
            }
        }
        catch (OperationCanceledException) when (!token.IsCancellationRequested)
        {
            throw PollNotAccepted();
        }
    }

    /// <summary>
    ///     Resolves balanced and message-key partitioning to an explicit partition id, mirroring
    ///     <c>core/common/src/traits/binary_impls/messages.rs</c> and retaining the client's routing cursor.
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

        return Partitioning.PartitionId(partition);
    }

    private async ValueTask<uint> TopicPartitionCountAsync(Identifier streamId, Identifier topicId,
        CancellationToken token)
    {
        var key = new TopicKey(streamId, topicId);
        if (_groupState.PartitionCount(key) is { } cached)
        {
            return cached;
        }

        var routeKey = new PartitionRouteKey(streamId, topicId, 0);
        RemovePartitionRoute(routeKey);
        var query = RouteQuery(routeKey);
        var deadline = Environment.TickCount64 + VsrRequestTimeoutMs;
        while (true)
        {
            try
            {
                var route = await GetPartitionRouteAsync(routeKey, query, deadline, token);
                return route.Route.PartitionsCount;
            }
            catch (IggyInvalidStatusCodeException error) when (error.StatusCode == VsrError.TRANSIENT_NOT_ACCEPTED
                && Environment.TickCount64 + PollRoutingRetryIntervalMs < deadline)
            {
                await Task.Delay(PollRoutingRetryIntervalMs, token);
            }
        }
    }

    /// <summary>
    ///     Polls one of the group member's assigned partitions, round-robin, with the strategy
    ///     <paramref name="pollingStrategyFor" /> gives for it. A fence rejection - either the typed error or the
    ///     sentinel partition id an empty poll carries - re-syncs the assignment and retries once.
    /// </summary>
    private async Task<PolledMessagesRental> PollGroupMessagesRentedAsync(Identifier streamId, Identifier topicId,
        Consumer consumer, Func<uint, PollingStrategy> pollingStrategyFor, uint count, bool autoCommit,
        CancellationToken token, ulong generation)
    {
        var key = new GroupKey(streamId, topicId, consumer.ConsumerId);
        if (!_groupState.HasAssignment(key))
        {
            await SyncGroupAssignmentAsync(streamId, topicId, consumer.ConsumerId, token, startedUnder: generation);
        }

        for (var attempt = 0; attempt < VsrGroupPollMaxAttempts; attempt++)
        {
            EnsureSignInOwner(generation);
            if (_groupState.NextGroupPartition(key) is not { } partitionId)
            {
                if (!_groupState.IsRegistered(key))
                {
                    throw VsrError.Exception(VsrError.CONSUMER_GROUP_MEMBER_NOT_FOUND,
                        $"Client is not a member of consumer group {consumer.ConsumerId} on topic {topicId}.");
                }

                return NoAssignedPartitionPolledMessages;
            }

            // Asked per attempt: a fence retry can land on another partition.
            var pollingStrategy = pollingStrategyFor(partitionId);
            PolledMessagesRental? rental = null;
            try
            {
                rental = await PollPartitionMessagesRentedAsync(streamId, topicId, partitionId, consumer,
                    pollingStrategy, count, autoCommit, token, generation);
            }
            catch (IggyInvalidStatusCodeException e) when (e is
            {
                StatusCode: VsrError.CONSUMER_GROUP_PARTITION_NOT_OWNED,
                FromServer: true
            })
            {
                // Both fence shapes - the typed error and the sentinel an empty poll carries - land on the same
                // re-sync below. A caller context names the owner the poll was meant for, so the caller gets the
                // fence instead of a poll of another partition.
                if (pollingStrategy.Context is not null)
                {
                    _groupState.InvalidateAssignment(key);
                    await SyncGroupAssignmentAsync(streamId, topicId, consumer.ConsumerId, token, startedUnder: generation);
                    throw;
                }
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
            await SyncGroupAssignmentAsync(streamId, topicId, consumer.ConsumerId, token, startedUnder: generation);
        }

        // Running out of attempts mid-rebalance is not a failure: report it like a member that owns nothing
        // yet, so the consumer backs off and polls again.
        return NoAssignedPartitionPolledMessages;
    }

    /// <summary>
    ///     Pulls the requesting member's assignment from the coordinator into the cache. An empty reply means the
    ///     client is not a member: the coordinator answers with an assignment header for any member, including
    ///     one holding zero partitions.
    /// </summary>
    private async Task SyncGroupAssignmentAsync(Identifier streamId, Identifier topicId, Identifier groupId,
        CancellationToken token, long? deadline = null, ulong? startedUnder = null)
    {
        var generation = startedUnder ?? _consensusSession.Generation;
        var message = TcpContracts.GetGroup(streamId, topicId, groupId);
        using IMemoryOwner<byte> responseBuffer =
            await SendWithResponseAsync(CommandCodes.SYNC_CONSUMER_GROUP_CODE, message, token: token,
                deadline: deadline, startedUnder: generation);

        lock (_pollRoutingGate)
        {
            EnsureSignInOwner(generation);
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
    }

    /// <summary>
    ///     Re-syncs every joined group so a widened assignment (a partition-count change, say) is picked up
    ///     without first hitting an ownership fence. Failed syncs are logged; all groups share the refresh
    ///     budget. A sync that outlasts <paramref name="deadline" /> fails as not accepted rather than canceled,
    ///     so it is logged like any other failure, and the groups after it wait for the next refresh. Only
    ///     <paramref name="token" /> cancels the refresh.
    /// </summary>
    private async Task RefreshGroupAssignmentsAsync(CancellationToken token, long? deadline = null)
    {
        foreach (var group in _groupState.RegisteredGroups())
        {
            if (Environment.TickCount64 >= deadline)
            {
                return;
            }
            try
            {
                await SyncGroupAssignmentAsync(group.StreamId, group.TopicId, group.GroupId, token, deadline);
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
    ///     Keeps every node the roster names as a dial candidate. Replaced wholesale rather than merged: the
    ///     roster is the cluster's own answer about where its nodes are, so a node it dropped stops being dialed.
    ///     The configured address is kept separately and outlives it. A node that does not expose the tcp
    ///     transport reports port 0 and is skipped, since dialing it would burn an attempt on an endpoint that
    ///     cannot answer.
    /// </summary>
    private void RememberRoster(ClusterMetadata clusterMetadata)
    {
        var nodeCount = clusterMetadata.Nodes.Count();
        if (nodeCount == 0)
        {
            throw new MalformedResponseException("Cluster metadata contains no nodes.");
        }

        var replicas = new Dictionary<byte, string>();
        foreach (var node in clusterMetadata.Nodes)
        {
            if (!replicas.TryAdd(node.ReplicaId, ServerAddress.HostPort(node.Ip, node.Endpoints.Tcp)))
            {
                throw new MalformedResponseException($"Duplicate replica id {node.ReplicaId} in cluster metadata.");
            }
        }
        lock (_pollRoutingGate)
        {
            _replicaEndpoints.Clear();
            foreach (var node in clusterMetadata.Nodes.Where(node => node.Endpoints.Tcp != 0))
            {
                _replicaEndpoints.Add(node.ReplicaId, replicas[node.ReplicaId]);
            }
        }
        Volatile.Write(ref _clusterNodeCount, nodeCount);
        var endpoints = clusterMetadata.Nodes
            .Where(node => node.Endpoints.Tcp != 0)
            .Select(node => ServerAddress.HostPort(node.Ip, node.Endpoints.Tcp))
            .ToArray();
        if (endpoints.Length > 0)
        {
            _rosterAddresses = endpoints;
        }
        InstallReplicaLinks(clusterMetadata);
    }

    private async Task<ClusterMetadata?> ReadClusterMetadataOnReadyLinkAsync(CancellationToken token)
    {
        using IMemoryOwner<byte> responseBuffer = await SendPollControlAsync(CommandCodes.GET_CLUSTER_METADATA_CODE,
            ReadOnlyMemory<byte>.Empty, Environment.TickCount64 + VsrRequestTimeoutMs, token);

        if (responseBuffer.Memory.Length == 0)
        {
            return null;
        }

        return BinaryMapper.MapClusterMetadata(responseBuffer.Memory.Span);
    }

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

    /// <summary>
    ///     Drops the consensus session and the group state scoped to it. Consumer-group assignments are fenced by
    ///     a generation the coordinator tracks per session, so carrying them into a new session would fence every
    ///     poll until the first re-sync. The balanced cursors and the partition counts survive: neither is bound
    ///     to a session, and dropping them costs a metadata round trip per topic on the next produce. When the client
    ///     signs in again by itself, <paramref name="continues" /> keeps the calls in flight for the next session.
    ///     When the client <paramref name="stops" /> without signing in again, they end as not connected. Otherwise
    ///     they end as unauthenticated, with <paramref name="cause" /> as the cause.
    /// </summary>
    private void ResetConsensusSession(ConnectionState? state = null, bool continues = false, Exception? cause = null,
        bool stops = false)
    {
        StopHeartbeat();
        lock (_pollRoutingGate)
        {
            // A call that fails on the reset must already see the new state. The replica links report their state
            // under this lock while the session is bound, so none of them can restore Authenticated in between.
            if (state is { } next)
            {
                SetConnectionState(next);
            }
            if (stops)
            {
                _consensusSession.Stop();
            }
            else
            {
                _consensusSession.Reset(continues, cause);
            }
        }
        if (continues)
        {
            _mutations.DropRetained();
        }
        else
        {
            _mutations.Reset();
        }
        StopReplicaLinks();
        _connection?.Dispose();
        _connection = null;
        _metadataReplica = null;
        _groupState.ClearSessionScoped();
        ClearPollSession();
    }

    /// <summary>
    ///     Resets the session on behalf of a caller that does not hold the sending lock, so no request can be
    ///     encoding against the identity while it is re-armed.
    /// </summary>
    private async ValueTask ResetConsensusSessionAsync(Exception? cause = null, bool stops = false,
        ulong? owner = null, bool continues = false, bool interrupt = false)
    {
        lock (_pollRoutingGate)
        {
            if (_disposed || owner is { } generation && _consensusSession.Ended(generation) is not null)
            {
                return;
            }
            if (interrupt)
            {
                // A failed caller attempt must also release an older registration holding the sending gate.
                StopReplicaLinks();
                _connection?.Dispose();
            }
        }
        if (!await TryEnterSendingSemaphoreAsync())
        {
            return;
        }
        try
        {
            lock (_pollRoutingGate)
            {
                if (!_disposed && (owner is null || _consensusSession.Ended(owner.Value) is null))
                {
                    ResetConsensusSession(stops ? ConnectionState.Disconnected : ConnectionState.Connected, continues, cause, stops);
                }
            }
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

}
