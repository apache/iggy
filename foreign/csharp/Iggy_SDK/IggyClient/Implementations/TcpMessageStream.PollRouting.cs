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
using System.Diagnostics;
using System.Net.Sockets;
using System.Runtime.ExceptionServices;
using System.Text;
using Apache.Iggy.Configuration;
using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Tcp;
using Apache.Iggy.Enums;
using Apache.Iggy.Extensions;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Kinds;
using Apache.Iggy.Mappers;
using Apache.Iggy.Utils;
using Apache.Iggy.Vsr;

namespace Apache.Iggy.IggyClient.Implementations;

public sealed partial class TcpMessageStream
{
    private const int MaxCachedPartitionRoutes = 4096;
    private const int PollRoutingRetryIntervalMs = 50;
    private const int PollRoutingRetryMaxIntervalMs = 1_000;
    private const int ConsumerSessionSize = 32;
    private const int ConsumerSessionEpochOffset = 16;
    private const int ConsumerSessionWatermarkOffset = 24;
    private const int IdentifierPrefixSize = 2;
    private const byte ConsumerGroupWireKind = 2;
    private const byte StringKind = 2;

    private const int MaxRouteLookupsInFlight = 16;

    private readonly SemaphoreSlim _deferredRequests =
        new(DeferredPollOptions.MAX_PARKED_REQUESTS, DeferredPollOptions.MAX_PARKED_REQUESTS);
    private readonly object _pollRoutingGate = new();
    private readonly Dictionary<PartitionRouteKey, CachedRoute> _partitionRoutes = [];
    private readonly Dictionary<byte, string> _replicaEndpoints = [];
    private ulong _metadataWatermark;
    private int _clusterNodeCount;
    private uint? _rememberedUserId;
    private AutoLoginSettings? _configuredLoginOverride;

    private readonly record struct PartitionRouteKey(Identifier StreamId, Identifier TopicId, uint PartitionId);
    private sealed record CachedRoute(ulong Generation, ulong Watermark, PartitionRoute Route, uint MinimumView,
        bool Stale, long Sequence);
    private long _routeSequence;
    private readonly record struct SendTarget(PartitionRouteKey Key, byte[] Query);

    /// <summary>
    ///     Where the wait and the request timeout of a deferred request sit in its body, and the values the caller
    ///     encoded there. Every attempt sends what is left of both: a non-replicated read has no replay receipt,
    ///     so its budget shrinks without a new request identity. A fetch keeps the reply margin also without a wait.
    /// </summary>
    private readonly record struct DeferredBudget(int WaitOffset, int TimeoutOffset, ulong WaitUs, ulong TimeoutUs,
        bool KeepsMargin)
    {
        internal static DeferredBudget? Read(int code, ReadOnlySpan<byte> body)
        {
            int waitOffset;
            int timeoutOffset;
            if (code == CommandCodes.POLL_MESSAGES_DEFERRED_CODE)
            {
                waitOffset = body.Length - TcpContracts.DEFERRED_POLL_TAIL_SIZE;
                timeoutOffset = body.Length - sizeof(ulong);
            }
            else if (code == CommandCodes.FETCH_MESSAGES_CODE)
            {
                waitOffset = 1;
                _ = ReadIdentifier(body, ref waitOffset);
                _ = ReadIdentifier(body, ref waitOffset);
                _ = ReadIdentifier(body, ref waitOffset);
                timeoutOffset = waitOffset + sizeof(ulong);
            }
            else
            {
                return null;
            }

            if (waitOffset < 0 || timeoutOffset + sizeof(ulong) > body.Length)
            {
                throw VsrError.Exception(VsrError.INVALID_COMMAND, "The deferred request body is truncated.");
            }

            return new DeferredBudget(waitOffset, timeoutOffset,
                BinaryPrimitives.ReadUInt64LittleEndian(body[waitOffset..]),
                BinaryPrimitives.ReadUInt64LittleEndian(body[timeoutOffset..]),
                code == CommandCodes.FETCH_MESSAGES_CODE);
        }

        /// <summary>
        ///     Writes into <paramref name="body" /> what is left after <paramref name="spent" />, measured with a
        ///     stopwatch as for <see cref="DeferredPollOptions.Remaining" />. The timeout also ends within
        ///     <paramref name="budget" />, the time this client gives the call. The wait keeps its reply margin, so an
        ///     idle wait still answers before the client gives up.
        /// </summary>
        /// <exception cref="IggyInvalidStatusCodeException">Status 58 once nothing is left to send.</exception>
        internal void Spend(Span<byte> body, TimeSpan budget, TimeSpan spent)
        {
            var spentUs = DeferredPollOptions.Micros(spent);
            var timeoutUs = Math.Min(TimeoutUs > spentUs ? TimeoutUs - spentUs : 0,
                DeferredPollOptions.Micros(budget - spent));
            if (timeoutUs == 0 || KeepsMargin && timeoutUs < DeferredPollOptions.REPLY_MARGIN_US)
            {
                throw PollNotAccepted();
            }

            var waitUs = Math.Min(WaitUs > spentUs ? WaitUs - spentUs : 0,
                timeoutUs > DeferredPollOptions.REPLY_MARGIN_US ? timeoutUs - DeferredPollOptions.REPLY_MARGIN_US : 0);
            BinaryPrimitives.WriteUInt64LittleEndian(body[WaitOffset..], waitUs);
            BinaryPrimitives.WriteUInt64LittleEndian(body[TimeoutOffset..], timeoutUs);
        }
    }

    internal bool RetainsOperation(ulong operationId)
    {
        try
        {
            _mutations.Retained(operationId);
            return true;
        }
        catch (VsrOperationNotRetainedException)
        {
            return false;
        }
    }

    private static SendTarget ParseSendTarget(ReadOnlySpan<byte> body)
    {
        var position = sizeof(uint);
        var streamId = ReadIdentifier(body, ref position);
        var topicId = ReadIdentifier(body, ref position);
        if (position + IdentifierPrefixSize > body.Length)
        {
            throw TruncatedSend();
        }

        if (body[position] != (byte)Apache.Iggy.Enums.Partitioning.PartitionId)
        {
            throw VsrError.Exception(VsrError.FEATURE_UNAVAILABLE,
                "A raw SendMessages request has to name an explicit partition id, so the client can capture the " +
                "partition context. Use SendMessagesAsync, which resolves balanced and message-key partitioning, " +
                "or send to an explicit partition id.");
        }

        if (body[position + 1] != sizeof(uint) || position + IdentifierPrefixSize + sizeof(uint) > body.Length)
        {
            throw TruncatedSend();
        }

        var partitionId = BinaryPrimitives.ReadUInt32LittleEndian(body[(position + IdentifierPrefixSize)..]);
        var query = new byte[position];
        body[sizeof(uint)..position].CopyTo(query);
        BinaryPrimitives.WriteUInt32LittleEndian(query.AsSpan(position - sizeof(uint)), partitionId);
        return new SendTarget(new PartitionRouteKey(streamId, topicId, partitionId), query);
    }

    private static Identifier ReadIdentifier(ReadOnlySpan<byte> body, ref int position)
    {
        if (position + IdentifierPrefixSize > body.Length
            || position + IdentifierPrefixSize + body[position + 1] > body.Length)
        {
            throw TruncatedSend();
        }

        var identifier = new Identifier
        {
            Kind = body[position] switch
            {
                1 => IdKind.Numeric,
                2 => IdKind.String,
                _ => throw VsrError.Exception(VsrError.INVALID_COMMAND, "Invalid identifier kind.")
            },
            Value = body.Slice(position + IdentifierPrefixSize, body[position + 1]).ToArray()
        };
        position += IdentifierPrefixSize + body[position + 1];
        return identifier;
    }

    private static IggyInvalidStatusCodeException TruncatedSend()
    {
        return VsrError.Exception(VsrError.INVALID_COMMAND, "The SendMessages body is truncated.");
    }

    private static SendTarget PartitionTarget(int code, ReadOnlySpan<byte> body)
    {
        if (code == CommandCodes.SEND_MESSAGES_CODE)
        {
            return ParseSendTarget(body);
        }

        if (body.IsEmpty)
        {
            throw TruncatedSend();
        }
        var group = body[0] == ConsumerGroupWireKind;
        var position = 1;
        _ = ReadIdentifier(body, ref position);
        var identifiersStart = position;
        var streamId = ReadIdentifier(body, ref position);
        var topicId = ReadIdentifier(body, ref position);
        var identifiersLength = position - identifiersStart;
        if (position + 1 + sizeof(uint) > body.Length)
        {
            throw TruncatedSend();
        }
        var present = body[position++] != 0;
        if (!present && (code is CommandCodes.STORE_CONSUMER_OFFSET_CODE or CommandCodes.DELETE_CONSUMER_OFFSET_CODE
            || code is CommandCodes.POLL_MESSAGES_CODE or CommandCodes.POLL_MESSAGES_DEFERRED_CODE && group))
        {
            throw VsrError.Exception(VsrError.INVALID_COMMAND, "This partition request requires an explicit partition id.");
        }
        var partitionId = present ? BinaryPrimitives.ReadUInt32LittleEndian(body[position..]) : 0;
        var query = new byte[identifiersLength + sizeof(uint)];
        body.Slice(identifiersStart, identifiersLength).CopyTo(query);
        BinaryPrimitives.WriteUInt32LittleEndian(query.AsSpan(identifiersLength), partitionId);
        return new SendTarget(new PartitionRouteKey(streamId, topicId, partitionId), query);
    }

    private static byte[] RouteQuery(PartitionRouteKey key)
    {
        var query = new byte[IdentifierPrefixSize * 2 + key.StreamId.Length + key.TopicId.Length + sizeof(uint)];
        query.AsSpan().WriteBytesFromStreamAndTopicIdentifiers(key.StreamId, key.TopicId);
        BinaryPrimitives.WriteUInt32LittleEndian(query.AsSpan(query.Length - sizeof(uint)), key.PartitionId);
        return query;
    }

    private Task<IMemoryOwner<byte>> SendRoutedAsync(int code, SendTarget target,
        ReadOnlyMemory<byte> payload, PartitionContext? context, CancellationToken token, TimeSpan? timeout = null,
        ulong? startedUnder = null) =>
        RunClientOperationAsync(code, payload, context, target, token, timeout: timeout, startedUnder: startedUnder);

    private async Task<IMemoryOwner<byte>> RunClientOperationAsync(int code, ReadOnlyMemory<byte> payload,
        PartitionContext? context, SendTarget? target, CancellationToken token, ClientOperation? retained = null,
        TimeSpan? timeout = null, ulong? startedUnder = null, byte? replica = null)
    {
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(token, _clientLifetime.Token);
        var budget = timeout ?? TimeSpan.FromMilliseconds(VsrRequestTimeoutMs);
        if (budget <= TimeSpan.Zero || budget.TotalMilliseconds > int.MaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout));
        }
        var started = Stopwatch.GetTimestamp();
        cancellation.CancelAfter(budget);
        var deadline = Environment.TickCount64 + (long)budget.TotalMilliseconds;
        var captured = retained?.Context ?? context;
        var operation = retained;
        SessionFrame? frame = retained?.Identity;
        // A call that started before its request was encoded keeps the generation it started under, so a sign-in or
        // sign-out by the caller meanwhile ends it.
        var generation = retained?.Generation ?? startedUnder ?? _consensusSession.Generation;
        var retainable = VsrOperations.ForCode(code) is not (VsrOperation.NonReplicated or VsrOperation.Register or VsrOperation.Logout);
        var progress = IsProgressPoll(code, payload.Span);
        var fetchLanes = progress && code == CommandCodes.FETCH_MESSAGES_CODE ? FetchProgressLanes(payload.Span) : null;
        var commits = code == CommandCodes.FETCH_MESSAGES_CODE && TcpContracts.FetchAutoCommits(payload.Span);
        var committing = TcpContracts.ReadAutoCommits(code, payload.Span);
        var deferred = DeferredBudget.Read(code, payload.Span);
        // A progress poll spends its budget in the frame of its lane. Any other deferred call spends it in a copy,
        // so the caller's buffer keeps what the caller encoded.
        var deferredBody = deferred is not null && !progress ? payload.ToArray() : null;
        if (deferredBody is not null)
        {
            payload = deferredBody;
        }
        var leased = false;
        var refreshHistory = code == CommandCodes.SEND_MESSAGES_CODE && context is null && retained is null;
        var retryDelay = LifecycleRetryIntervalMs;
        var routingRetries = 0;
        var lastWasNotPrimary = false;
        var askMetadataPrimaryFirst = false;
        var rosterRead = false;
        var terminal = false;
        var frameAcquired = false;
        var fetchMayHaveCommitted = false;
        var startOver = false;
        IggyInvalidStatusCodeException? refusal = null;
        IggyInvalidStatusCodeException? rejection = null;
        CancellationTokenRegistration operationCancellation = default;
        try
        {
            if (deferred is not null)
            {
                await _deferredRequests.WaitAsync(cancellation.Token);
                leased = true;
            }
            while (true)
            {
                CachedRoute? route = null;
                var sending = false;
                try
                {
                    if (retained is null && operation is not { Uncertain: true })
                    {
                        // A call that cannot have been applied yet waits for a session recovery, and starts over
                        // under the session that continues its own, in the lane it holds.
                        while (SignInRecovery() is { } recovery)
                        {
                            await recovery.WaitAsync(cancellation.Token);
                        }
                        startOver |= _consensusSession.Successor(generation) is { } successor && successor != generation;
                    }
                    if (_consensusSession.Ended(generation) is { } ended)
                    {
                        throw ended;
                    }
                    if (startOver)
                    {
                        startOver = false;
                        if (operation is not null)
                        {
                            var restartBody = operation.Body.ToArray();
                            operationCancellation.Dispose();
                            if (frameAcquired)
                            {
                                operation.Frame.Release();
                                frameAcquired = false;
                            }
                            operation = await _mutations.RestartAsync(operation, restartBody, cancellation.Token);
                            _mutations.Acquire(operation);
                            frameAcquired = true;
                            operationCancellation = operation.Lifetime.Token.Register(cancellation.Cancel);
                        }
                        frame = operation?.Identity;
                        generation = operation?.Generation ?? _consensusSession.Successor(generation) ?? generation;
                    }
                    if (target is { } partition)
                    {
                        if (operation is null && fetchLanes is null && (retainable || progress))
                        {
                            PartitionContext? known = captured;
                            lock (_pollRoutingGate)
                            {
                                if (known is null && _partitionRoutes.TryGetValue(partition.Key, out var cachedRoute)
                                    && cachedRoute.Generation == generation)
                                {
                                    known = cachedRoute.Route.Context;
                                }
                            }
                            if (known is { } knownContext)
                            {
                                _mutations.ThrowIfRetained(new MutationLane(knownContext.Incarnation,
                                    partition.Key.PartitionId, progress));
                            }
                        }
                        route = await GetPartitionRouteAsync(partition.Key, partition.Query, deadline, cancellation.Token,
                            askMetadataPrimaryFirst);
                        askMetadataPrimaryFirst = false;
                        captured ??= route.Route.Context;
                    }
                    else
                    {
                        captured ??= new PartitionContext(0, 0, _metadataWatermark);
                    }
                    EnsureOperationSession(code, generation);
                    if (operation is null && (retainable || progress))
                    {
                        // A caller's zero wire fence must not bypass the resolved history's local recovery block.
                        var laneContext = captured.Value.Incarnation == 0 && route is not null
                            ? route.Route.Context : captured.Value;
                        var lane = target is { } selected
                            ? new MutationLane(laneContext.Incarnation, selected.Key.PartitionId, progress)
                            : MutationLane.Metadata;
                        operation = await (fetchLanes is null
                            ? _mutations.EnterAsync(lane, code, payload, captured.Value, retainable, generation,
                                cancellation.Token)
                            : _mutations.EnterAsync(fetchLanes, code, payload, captured.Value, retainable, generation,
                                cancellation.Token));
                        _mutations.Acquire(operation);
                        frameAcquired = true;
                        operationCancellation = operation.Lifetime.Token.Register(cancellation.Cancel);
                        frame = operation.Identity;
                        generation = operation.Generation;
                    }
                    frame ??= _consensusSession.Resolve(VsrOperations.ForCode(code));
                    VsrConnection connection;
                    if (route is not null)
                    {
                        if (await FindReadyReplicaAsync(route.Route.PrimaryReplica, cancellation.Token) is not { } primary)
                        {
                            if (target is { } waiting)
                            {
                                RemovePartitionRoute(waiting.Key);
                            }
                            await Task.Delay(PollRoutingRetryDelay(routingRetries++, Random.Shared.NextDouble()), cancellation.Token);
                            continue;
                        }
                        connection = primary;
                    }
                    else if (replica is { } chosen)
                    {
                        if (await FindReadyReplicaAsync(chosen, cancellation.Token) is not { } ready)
                        {
                            bool known;
                            lock (_pollRoutingGate)
                            {
                                known = _replicaLinks.ContainsKey(chosen);
                            }
                            if (!known && rosterRead)
                            {
                                throw VsrError.Exception(VsrError.INVALID_IDENTIFIER,
                                    $"Cluster metadata names no replica {chosen}.");
                            }
                            if (!known)
                            {
                                rosterRead = true;
                                await RefreshPartitionRosterAsync(deadline, cancellation.Token);
                                continue;
                            }
                            await Task.Delay(PollRoutingRetryDelay(routingRetries++, Random.Shared.NextDouble()), cancellation.Token);
                            continue;
                        }
                        connection = ready;
                    }
                    else if (_metadataReplica is { } metadataPrimary)
                    {
                        if (await FindReadyReplicaAsync(metadataPrimary, cancellation.Token) is not { } primary)
                        {
                            await Task.Delay(PollRoutingRetryDelay(routingRetries++, Random.Shared.NextDouble()), cancellation.Token);
                            await RefreshPartitionRosterAsync(deadline, cancellation.Token);
                            continue;
                        }
                        connection = primary;
                    }
                    else
                    {
                        connection = ReadyReplicaLinks().FirstOrDefault() ?? throw new NotConnectedException();
                    }
                    EnsureOperationSession(code, generation);
                    if (operation is not null && !_mutations.Owns(operation))
                    {
                        throw PollNotAccepted();
                    }
                    cancellation.Token.ThrowIfCancellationRequested();
                    if (refusal is not null && Environment.TickCount64 >= deadline)
                    {
                        ExceptionDispatchInfo.Throw(refusal);
                    }
                    refusal = null;
                    if (deferred is { } deferredBudget)
                    {
                        deferredBudget.Spend(
                            operation is null ? deferredBody : operation.Frame.Bytes.AsSpan(VsrHeader.HEADER_SIZE),
                            budget, Stopwatch.GetElapsedTime(started));
                    }
                    sending = true;
                    // A read that commits can store its offset before a cancel reaches the server, so once it goes
                    // out the caller's cancel only asks the server to cancel it. Its reply, its deadline, or the end
                    // of the client or the lane ends the call.
                    using var unattended = committing && operation is not null
                        ? CancellationTokenSource.CreateLinkedTokenSource(_clientLifetime.Token,
                            operation.Lifetime.Token)
                        : null;
                    var attempt = operation is null
                        ? await connection.SendAttemptAsync(code, payload, deadline, deadline, HasSensitiveReply(code),
                            cancellation.Token, retryTransient: false, context: captured.Value, retainedFrame: frame)
                        : await connection.SendFrameAsync(operation, deadline, HasSensitiveReply(code),
                            unattended?.Token ?? cancellation.Token, unattended is null ? default : token);
                    if (unattended is not null && token.IsCancellationRequested
                        && attempt.Error is IggyInvalidStatusCodeException
                        {
                            FromServer: true, StatusCode: VsrError.TRANSIENT_NOT_ACCEPTED
                        })
                    {
                        // The cancel won before the read stored anything.
                        throw new OperationCanceledException(token);
                    }
                    if (commits && attempt.RequestStarted)
                    {
                        fetchMayHaveCommitted = attempt.Error is not IggyInvalidStatusCodeException { FromServer: true } refusalStatus
                            || !FetchRefusedBeforeRead(refusalStatus.StatusCode);
                    }
                    if (fetchMayHaveCommitted && attempt.Error is not null)
                    {
                        if (IsSessionFailure(attempt.Error))
                        {
                            ScheduleSessionRecovery(generation);
                        }
                        throw new VsrFetchProgressUncertainException(operation!.Identity.RequestId, attempt.Error);
                    }
                    if (attempt.Error is null)
                    {
                        if (operation is { Uncertain: true } && !attempt.Committed)
                        {
                            attempt.Response?.Dispose();
                            throw new VsrRequestOutcomeUnknownException(
                                VsrError.Exception(VsrError.TRANSIENT_NOT_COMMITTED, "The reply does not prove commit."),
                                operation.Identity.RequestId);
                        }
                        var response = attempt.Response!;
                        if (commits && operation is not null)
                        {
                            try
                            {
                                var sections = BinaryMapper.ReadFetchSections(response.Memory.Span);
                                var requested = fetchLanes!.Select(lane => lane.PartitionId).ToHashSet();
                                if (sections.Any(section => !requested.Contains(section.PartitionId)))
                                {
                                    throw new MalformedResponseException("Fetch reply names an unrequested partition.");
                                }
                                var uncertain = sections.Where(section => section.Status != 0
                                    && !FetchSectionRefusedBeforeRead(section.Status)).Select(section => section.PartitionId).ToHashSet();
                                if (uncertain.Count > 0)
                                {
                                    _mutations.RetainFetch(operation, uncertain);
                                    response = new FetchProgressReply(response, operation.Identity.RequestId, uncertain);
                                }
                                fetchMayHaveCommitted = false;
                                if (_consensusSession.Ended(generation) is { } replySessionEnded)
                                {
                                    ExceptionDispatchInfo.Throw(replySessionEnded);
                                }
                            }
                            catch
                            {
                                response.Dispose();
                                throw;
                            }
                        }
                        terminal = true;
                        RefreshCommittedCredentials(code, (operation?.Body ?? payload).Span, generation);
                        return response;
                    }
                    if (IsSessionFailure(attempt.Error))
                    {
                        ScheduleSessionRecovery(generation);
                    }
                    if (attempt.Error is IggyInvalidStatusCodeException status)
                    {
                        status.CommandCode = code;
                        if (route is not null
                            && await IsBackupNamespaceMissAsync(status, connection, deadline, cancellation.Token))
                        {
                            // The backup refused the frame before admission, so the same frame goes out again. A
                            // lookup asks the primary of the stale route first, which is this backup, so the next
                            // one starts at the metadata primary.
                            askMetadataPrimaryFirst = true;
                            status = VsrError.FromServer(VsrError.TRANSIENT_NOT_ACCEPTED,
                                "A backup replica does not know the stream, topic or partition yet.");
                            status.CommandCode = code;
                        }
                        if (operation is not null
                            && (status.StatusCode == VsrError.TRANSIENT_NOT_COMMITTED
                                || attempt.RequestStarted && (!status.FromServer || status.StatusCode == VsrError.REQUEST_TOO_OLD)))
                        {
                            if (!operation.Retainable)
                            {
                                throw new VsrRequestOutcomeUnknownException(status);
                            }
                            operation.Uncertain = true;
                        }
                        if (operation is { Uncertain: true } && !status.Committed
                            && status.StatusCode is not (VsrError.TRANSIENT_NOT_COMMITTED or VsrError.TRANSIENT_NOT_ACCEPTED or VsrError.NOT_PRIMARY))
                        {
                            throw new VsrRequestOutcomeUnknownException(status, operation.Identity.RequestId);
                        }
                        if (status.Committed && operation is { Uncertain: true })
                        {
                            terminal = true;
                            rejection = status;
                        }
                        if (status.StatusCode == VsrError.HISTORY_UNAVAILABLE && target is { } staleHistory)
                        {
                            RemovePartitionRoute(staleHistory.Key);
                        }
                        if (operation is not { Uncertain: true } && status.FromServer
                            && status.StatusCode is VsrError.HISTORY_UNAVAILABLE or VsrError.LIFECYCLE_BUSY)
                        {
                            operationCancellation.Dispose();
                        }
                        ExceptionDispatchInfo.Throw(status);
                    }
                    if (attempt.RequestStarted && (retainable || progress || !VsrOperations.IsReplaySafeRead(code, payload.Span)))
                    {
                        if (operation is { Retainable: true })
                        {
                            operation.Uncertain = true;
                        }
                        else
                        {
                            throw new VsrRequestOutcomeUnknownException(attempt.Error);
                        }
                    }
                    if (attempt.Error is OperationCanceledException)
                    {
                        ExceptionDispatchInfo.Throw(attempt.Error);
                    }
                    if (!VsrConnection.IsConnectionException(attempt.Error))
                    {
                        throw operation is { Uncertain: true }
                            ? new VsrRequestOutcomeUnknownException(attempt.Error, operation.Identity.RequestId)
                            : CallerError(attempt.Error);
                    }
                    if (target is { } failed)
                    {
                        RemovePartitionRoute(failed.Key);
                    }
                    lastWasNotPrimary = false;
                    await Task.Delay(PollRoutingRetryDelay(routingRetries++, Random.Shared.NextDouble()), cancellation.Token);
                }
                // A fetch names several partitions and its refusal as not primary does not say
                // which one moved, so the caller looks up every route of the call again.
                catch (IggyInvalidStatusCodeException error) when (error.StatusCode is
                    VsrError.TRANSIENT_NOT_ACCEPTED or VsrError.TRANSIENT_NOT_COMMITTED
                    or VsrError.DEFERRED_POLL_CAPACITY_EXCEEDED
                    || error.StatusCode == VsrError.NOT_PRIMARY && replica is null
                    && code != CommandCodes.FETCH_MESSAGES_CODE)
                {
                    if (generation != _consensusSession.Generation && retained is null
                        && operation is not { Uncertain: true } && ResumesUnderNewSession(generation))
                    {
                        continue;
                    }
                    if (error.FromServer && error.CommandCode == code)
                    {
                        refusal = error;
                        if (error.StatusCode == VsrError.NOT_PRIMARY)
                        {
                            // Neither 58 nor 89 admitted the call, so the deadline reports both as 58.
                            refusal = VsrError.FromServer(VsrError.TRANSIENT_NOT_ACCEPTED,
                                "No replica admitted the call as its primary.");
                            refusal.CommandCode = code;
                        }
                    }
                    var retryImmediately = error.StatusCode == VsrError.NOT_PRIMARY && !lastWasNotPrimary;
                    lastWasNotPrimary = error.StatusCode == VsrError.NOT_PRIMARY;
                    var pause = retryImmediately ? TimeSpan.Zero
                        : PollRoutingRetryDelay(routingRetries++, Random.Shared.NextDouble());
                    if (generation != _consensusSession.Generation
                        || Environment.TickCount64 + (long)pause.TotalMilliseconds >= deadline)
                    {
                        if (generation == _consensusSession.Generation && refusal is not null)
                        {
                            ExceptionDispatchInfo.Throw(refusal);
                        }
                        if (_consensusSession.Ended(generation) is { } ended)
                        {
                            throw ended;
                        }
                        throw;
                    }
                    // A full server refuses a deferred request on the right route, so the route is kept.
                    if (target is { } failed && error.StatusCode != VsrError.DEFERRED_POLL_CAPACITY_EXCEEDED)
                    {
                        RemovePartitionRoute(failed.Key, error.StatusCode == VsrError.NOT_PRIMARY ? route : null);
                    }
                    await Task.Delay(pause, cancellation.Token);
                    if (target is null && replica is null)
                    {
                        try
                        {
                            await RefreshPartitionRosterAsync(deadline, cancellation.Token);
                        }
                        catch (Exception refreshError) when (retained is null && operation is not { Uncertain: true }
                            && (refreshError is NotConnectedException || IsSessionFailure(refreshError))
                            && ResumesUnderNewSession(generation))
                        {
                            // The refused call was never admitted, so the next pass waits for the new session.
                        }
                    }
                }
                catch (Exception error) when (!sending && retained is null && operation is not { Uncertain: true }
                    && (error is NotConnectedException || IsSessionFailure(error))
                    && (ResumesUnderNewSession(generation) || _consensusSession.Ended(generation) is not null))
                {
                    // The session ended before the call was sent. The next pass waits for the new one. A call that no
                    // session can take ends here: the check that ends it on the next pass throws into this catch, and
                    // a not connected end would come back to it with no wait.
                    if (_consensusSession.Ended(generation) is { } ended)
                    {
                        if (ended is NotConnectedException && IsSessionFailure(error))
                        {
                            ExceptionDispatchInfo.Throw(CallerError(error));
                        }
                        throw ended;
                    }
                }
                catch (IggyInvalidStatusCodeException error) when (retained is null
                    && operation is not { Uncertain: true } && error.StatusCode == VsrError.HISTORY_UNAVAILABLE && refreshHistory)
                {
                    lastWasNotPrimary = false;
                    refreshHistory = false;
                    if (target is { } failed)
                    {
                        RemovePartitionRoute(failed.Key);
                    }
                    if (operation is not null)
                    {
                        operationCancellation.Dispose();
                        _mutations.Complete(operation);
                        if (frameAcquired)
                        {
                            operation.Frame.Release();
                            frameAcquired = false;
                        }
                        operation = null;
                    }
                    captured = null;
                    frame = null;
                    refusal = error;
                }
                catch (IggyInvalidStatusCodeException error) when (retained is null
                    && operation is not { Uncertain: true }
                    && error.FromServer && error.CommandCode == code
                    && error.StatusCode == VsrError.LIFECYCLE_BUSY)
                {
                    if (!ResumesUnderNewSession(generation))
                    {
                        EnsureOperationSession(code, generation);
                    }
                    lastWasNotPrimary = false;
                    terminal = false;
                    refusal = error;
                    startOver = true;
                    await Task.Delay(retryDelay, cancellation.Token);
                    retryDelay = Math.Min(retryDelay * 2, LifecycleRetryMaxIntervalMs);
                }
            }
        }
        catch (Exception error) when (operation?.FetchProgress is not null && fetchMayHaveCommitted)
        {
            _mutations.RetainFetch(operation);
            throw error is VsrFetchProgressUncertainException ? error
                : new VsrFetchProgressUncertainException(operation.Identity.RequestId, error);
        }
        catch (Exception error) when (operation is { Retainable: true, Uncertain: true }
            && error is not IggyInvalidStatusCodeException { Committed: true })
        {
            _mutations.Retain(operation);
            throw error is VsrRequestOutcomeUnknownException ? error
                : new VsrRequestOutcomeUnknownException(error, operation.Identity.RequestId);
        }
        catch (OperationCanceledException) when (!token.IsCancellationRequested)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (generation == _consensusSession.Generation && refusal is not null)
            {
                ExceptionDispatchInfo.Throw(refusal);
            }
            throw _consensusSession.Ended(generation)
                ?? (replica is { } chosen ? ReplicaNotAccepted(chosen) : PollNotAccepted());
        }
        catch (Exception error) when (error is IOException or SocketException
            && refusal is not null
            && generation == _consensusSession.Generation
            && Environment.TickCount64 >= deadline)
        {
            ExceptionDispatchInfo.Throw(refusal);
            throw;
        }
        finally
        {
            if (leased)
            {
                _deferredRequests.Release();
            }
            operationCancellation.Dispose();
            if (terminal && retained is not null)
            {
                _mutations.Prove(retained, rejection);
            }
            else if (operation is not null && (terminal || !operation.Retained))
            {
                _mutations.Complete(operation);
            }
            if (operation is not null && frameAcquired)
            {
                operation.Frame.Release();
            }
        }
    }

    private static bool FetchRefusedBeforeRead(int status) =>
        status is VsrError.FEATURE_UNAVAILABLE or VsrError.INVALID_IDENTIFIER or VsrError.TRANSIENT_NOT_ACCEPTED
            or VsrError.HISTORY_UNAVAILABLE or VsrError.NOT_PRIMARY or VsrError.DEFERRED_POLL_CAPACITY_EXCEEDED
            or VsrError.STREAM_ID_NOT_FOUND or VsrError.STREAM_NAME_NOT_FOUND or VsrError.TOPIC_ID_NOT_FOUND
            or VsrError.TOPIC_NAME_NOT_FOUND or VsrError.PARTITION_NOT_FOUND or VsrError.INVALID_OFFSET
            or VsrError.CONSUMER_GROUP_PARTITION_NOT_OWNED;

    private static bool FetchSectionRefusedBeforeRead(uint status) =>
        status is VsrError.INVALID_SIZE_BYTES or VsrError.HISTORY_UNAVAILABLE or VsrError.NOT_PRIMARY
            or VsrError.CONSUMER_GROUP_PARTITION_NOT_OWNED;

    /// <inheritdoc />
    public async Task ResolveOperationAsync(ulong operationId, TimeSpan timeout, CancellationToken token = default)
    {
        if (timeout <= TimeSpan.Zero || timeout.TotalMilliseconds > int.MaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout));
        }
        if (_mutations.AcquireRetained(operationId) is not { } operation)
        {
            _mutations.RepeatProvedOutcome(operationId);
            return;
        }
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(token, operation.Lifetime.Token);
        cancellation.CancelAfter(timeout);
        var deadline = Environment.TickCount64 + (long)timeout.TotalMilliseconds;
        var acquired = false;
        var replaying = false;
        try
        {
            await operation.Resolution.WaitAsync(cancellation.Token);
            acquired = true;
            if (!_mutations.BeginReplay(operation))
            {
                _mutations.RepeatProvedOutcome(operationId);
                return;
            }
            replaying = true;
            var target = operation.Lane == MutationLane.Metadata ? (SendTarget?)null
                : PartitionTarget(operation.Code, operation.Body.Span);
            using var response = await RunClientOperationAsync(operation.Code, operation.Body, operation.Context,
                target, cancellation.Token, operation, TimeSpan.FromMilliseconds(Math.Max(1, deadline - Environment.TickCount64)));
        }
        catch (OperationCanceledException) when (!_mutations.Owns(operation))
        {
            // The release of the operation ended the wait: a concurrent replay proved it, or an abandon or a reset
            // dropped it.
            _mutations.RepeatProvedOutcome(operationId);
        }
        catch (OperationCanceledException error)
        {
            throw new VsrRequestOutcomeUnknownException(error, operationId);
        }
        finally
        {
            if (replaying)
            {
                _mutations.EndReplay(operation);
            }
            if (acquired)
            {
                operation.Resolution.Release();
            }
            operation.Frame.Release();
        }
    }

    /// <inheritdoc />
    public Task AbandonOperationAsync(ulong operationId)
    {
        _mutations.Abandon(operationId);
        return Task.CompletedTask;
    }

    private void EnsureOperationSession(int code, ulong generation)
    {
        lock (_pollRoutingGate)
        {
            if (_signInWithdrawn && code != CommandCodes.PING_CODE)
            {
                throw _consensusSession.Ended(generation)
                    ?? VsrError.Exception(VsrError.UNAUTHENTICATED, "An explicit sign-in is required.");
            }
        }
        if (generation != _consensusSession.Generation)
        {
            throw _consensusSession.Ended(generation) ?? PollNotAccepted();
        }
    }

    private static bool IsProgressPoll(int code, ReadOnlySpan<byte> body)
    {
        const int PollTailSize = 14;
        const byte NextStrategy = 5;
        if (code == CommandCodes.FETCH_MESSAGES_CODE)
        {
            return TcpContracts.FetchMovesConsumer(body);
        }

        if (code == CommandCodes.POLL_MESSAGES_DEFERRED_CODE && body.Length >= TcpContracts.DEFERRED_POLL_TAIL_SIZE)
        {
            body = body[..^TcpContracts.DEFERRED_POLL_TAIL_SIZE];
        }
        else if (code != CommandCodes.POLL_MESSAGES_CODE)
        {
            return false;
        }

        return body.Length >= PollTailSize && (body[^1] != 0 || body[^PollTailSize] == NextStrategy);
    }

    // Two fetches that share partitions take their lanes in the same order, so neither waits for the other in a
    // cycle.
    private static MutationLane[] FetchProgressLanes(ReadOnlySpan<byte> body) =>
    [
        .. TcpContracts.FetchTargetFences(body).OrderBy(target => target.PartitionId)
            .Select(target => new MutationLane(target.Incarnation, target.PartitionId, Progress: true))
    ];

    /// <summary>
    ///     Sends one fetch to the primary of the first target. A target without a fence, or with incarnation zero as
    ///     the Rust SDK reads it, takes the context of its route, and each target gets the whole byte limit. A
    ///     section or a refusal as not primary, or with a lost history, marks the routes of its partitions stale, so
    ///     the next grouping looks them up again. A refusal of the whole fetch does not name its partition, so it
    ///     marks all of them.
    /// </summary>
    private async Task<FetchedMessages> FetchMessagesCoreAsync(Identifier streamId, Identifier topicId,
        IReadOnlyList<(uint PartitionId, PollingStrategy Strategy)> targets, Consumer consumer, uint count,
        bool autoCommit, DeferredPollOptions options, CancellationToken token, ulong generation)
    {
        var started = Stopwatch.GetTimestamp();
        var deadline = Environment.TickCount64 + (long)options.RequestTimeout.TotalMilliseconds;
        // A link drops a reply wider than it reads, so the records get what the frame leaves after the table.
        var records = _configuration.MaxResponseFrameSize - VsrHeader.HEADER_SIZE - FetchedMessages.HeadSize(targets.Count);
        if (records < DeferredPollOptions.MIN_MAX_BYTES)
        {
            throw VsrError.Exception(VsrError.INVALID_SIZE_BYTES,
                $"A reply frame of {_configuration.MaxResponseFrameSize} bytes cannot hold a fetch of {targets.Count} partitions.");
        }

        var fenced = new (uint PartitionId, PollingStrategy Strategy)[targets.Count];
        for (var index = 0; index < fenced.Length; index++)
        {
            var (partitionId, strategy) = targets[index];
            if (strategy.Context is not { Incarnation: not 0 })
            {
                var key = new PartitionRouteKey(streamId, topicId, partitionId);
                var route = await GetPartitionRouteAsync(key, RouteQuery(key), deadline, token);
                strategy = strategy.WithContext(route.Route.Context);
            }

            fenced[index] = (partitionId, strategy);
        }

        var left = options.Remaining(Stopwatch.GetElapsedTime(started)) with
        {
            MaxBytes = Math.Min(options.MaxBytes, (uint)records)
        };
        var first = new PartitionRouteKey(streamId, topicId, targets[0].PartitionId);
        IMemoryOwner<byte> reply;
        try
        {
            reply = await SendRoutedAsync(CommandCodes.FETCH_MESSAGES_CODE, new SendTarget(first, RouteQuery(first)),
                TcpContracts.FetchMessages(consumer, streamId, topicId, left, count, autoCommit, left.MaxBytes, fenced),
                default(PartitionContext), token, left.RequestTimeout, generation);
        }
        catch (IggyInvalidStatusCodeException error) when (error.StatusCode is VsrError.NOT_PRIMARY
                                                               or VsrError.HISTORY_UNAVAILABLE)
        {
            foreach (var (partitionId, _) in targets)
            {
                RemovePartitionRoute(new PartitionRouteKey(streamId, topicId, partitionId));
            }

            throw;
        }

        FetchedMessages fetched;
        try
        {
            fetched = BinaryMapper.MapFetchedMessages(reply, _configuration.MessageEncryptor);
        }
        catch
        {
            reply.Dispose();
            throw;
        }

        foreach (var section in fetched.Sections)
        {
            if (section.Status is (uint)VsrError.NOT_PRIMARY or (uint)VsrError.HISTORY_UNAVAILABLE)
            {
                RemovePartitionRoute(new PartitionRouteKey(streamId, topicId, section.PartitionId));
            }
        }

        return fetched;
    }

    /// <summary>
    ///     <paramref name="targets" /> grouped by the primary of <paramref name="partitionOf" /> and split to fit one
    ///     fetch each, keeping their order. A lookup that no replica answers is retried until
    ///     <paramref name="deadline" />.
    /// </summary>
    private async Task<List<T[]>> ReadinessGroupsAsync<T>(Identifier streamId, Identifier topicId,
        IReadOnlyList<T> targets, Func<T, uint> partitionOf, long deadline, CancellationToken token)
    {
        var primaries = new byte[targets.Count];
        var lookups = new ParallelOptions { MaxDegreeOfParallelism = MaxRouteLookupsInFlight, CancellationToken = token };
        for (var retries = 0; ; retries++)
        {
            try
            {
                await Parallel.ForEachAsync(Enumerable.Range(0, targets.Count), lookups, async (index, lookup) =>
                {
                    var key = new PartitionRouteKey(streamId, topicId, partitionOf(targets[index]));
                    var route = await GetPartitionRouteAsync(key, RouteQuery(key), deadline, lookup);
                    primaries[index] = route.Route.PrimaryReplica;
                });
                return
                [
                    .. targets.Select((target, index) => (Primary: primaries[index], Target: target))
                        .GroupBy(entry => entry.Primary, entry => entry.Target)
                        .OrderBy(group => group.Key)
                        .SelectMany(group => group.Chunk(TcpContracts.MAX_PARTITIONS_PER_REQUEST))
                ];
            }
            catch (Exception error) when (VsrConnection.IsConnectionException(error)
                || error is IggyInvalidStatusCodeException { StatusCode: VsrError.TRANSIENT_NOT_ACCEPTED or VsrError.NOT_PRIMARY })
            {
                var pause = PollRoutingRetryDelay(retries, Random.Shared.NextDouble());
                if (Environment.TickCount64 + (long)pause.TotalMilliseconds >= deadline)
                {
                    throw PollNotAccepted();
                }
                await Task.Delay(pause, token);
            }
        }
    }

    private async Task<CachedRoute> GetPartitionRouteAsync(PartitionRouteKey key, ReadOnlyMemory<byte> query,
        long deadline, CancellationToken token, bool askMetadataPrimaryFirst = false)
    {
        CachedRoute? previous;
        lock (_pollRoutingGate)
        {
            if (_partitionRoutes.TryGetValue(key, out previous) && !previous.Stale
                && previous.Watermark >= _metadataWatermark && previous.Generation == _consensusSession.Generation)
            {
                return previous;
            }
        }
        if (Volatile.Read(ref _clusterNodeCount) == 0)
        {
            await RefreshPartitionRosterAsync(deadline, token);
        }
        var generation = _consensusSession.Generation;
        var links = ReadyReplicaLinksOrThrow().ToList();
        lock (_pollRoutingGate)
        {
            var preferred = askMetadataPrimaryFirst ? _metadataReplica : previous?.Route.PrimaryReplica ?? _metadataReplica;
            if (preferred is { } replica && _replicaLinks.TryGetValue(replica, out var slot)
                && slot.Connection is { IsReady: true } connection)
            {
                links.Remove(connection);
                links.Insert(0, connection);
            }
        }
        foreach (var connection in links)
        {
            var attempt = await connection.SendAttemptAsync(CommandCodes.GET_PARTITION_ROUTE_CODE, query,
                deadline, deadline, false, token, retryTransient: false,
                context: new PartitionContext(0, 0, _metadataWatermark));
            if (attempt.Error is not null)
            {
                if (IsSessionFailure(attempt.Error))
                {
                    ScheduleSessionRecovery(generation);
                }
                if (attempt.Error is IggyInvalidStatusCodeException
                    { StatusCode: VsrError.TRANSIENT_NOT_ACCEPTED or VsrError.NOT_PRIMARY }
                    || VsrConnection.IsConnectionException(attempt.Error))
                {
                    continue;
                }
                if (attempt.Error is IggyInvalidStatusCodeException lookupError && IsNamespaceMiss(lookupError.StatusCode)
                    && !await IsMetadataPrimaryAsync(connection, deadline, token))
                {
                    continue;
                }
                ExceptionDispatchInfo.Throw(CallerError(attempt.Error));
            }
            using var response = attempt.Response!;
            var route = BinaryMapper.MapPartitionRoute(response.Memory.Span);
            bool knownReplica;
            lock (_pollRoutingGate)
            {
                knownReplica = _replicaEndpoints.ContainsKey(route.PrimaryReplica);
            }
            if (!knownReplica)
            {
                await RefreshPartitionRosterAsync(deadline, token);
            }
            lock (_pollRoutingGate)
            {
                if (generation != _consensusSession.Generation || !_replicaEndpoints.ContainsKey(route.PrimaryReplica))
                {
                    throw PollNotAccepted();
                }
                _partitionRoutes.TryGetValue(key, out previous);
                if (previous is not null && (route.MetadataOp < previous.Route.MetadataOp
                    || route.Incarnation == previous.Route.Incarnation && (route.View < previous.MinimumView
                        || route.View == previous.Route.View && route.PrimaryReplica != previous.Route.PrimaryReplica)))
                {
                    continue;
                }
                var aliases = _partitionRoutes.Where(entry => entry.Key.PartitionId == key.PartitionId
                    && entry.Value.Route.Incarnation == route.Incarnation).ToArray();
                var groupFloor = aliases.Select(entry => entry.Value.MinimumView).DefaultIfEmpty(route.View).Max();
                if (route.View < groupFloor || aliases.Any(entry => entry.Value.Route.View == route.View
                    && entry.Value.Route.PrimaryReplica != route.PrimaryReplica))
                {
                    continue;
                }
                foreach (var (alias, cachedAlias) in aliases)
                {
                    _partitionRoutes[alias] = cachedAlias with
                    {
                        MinimumView = Math.Max(cachedAlias.MinimumView, route.View),
                        Stale = cachedAlias.Stale || cachedAlias.Route.View < route.View
                    };
                }
                _groupState.SetPartitionCount(new TopicKey(key.StreamId, key.TopicId), route.PartitionsCount);
                var cached = new CachedRoute(generation, Math.Max(_metadataWatermark, route.MetadataOp), route,
                    route.View, false, checked(++_routeSequence));
                if (_partitionRoutes.Count >= MaxCachedPartitionRoutes && !_partitionRoutes.ContainsKey(key))
                {
                    var oldest = _partitionRoutes.MinBy(entry => entry.Value.Sequence).Key;
                    _partitionRoutes.Remove(oldest);
                }
                _partitionRoutes[key] = cached;
                return cached;
            }
        }
        throw PollNotAccepted();
    }

    /// <summary>
    ///     Whether a replica that is not the metadata primary answered that it has no such stream, topic or
    ///     partition. A backup applies metadata commits after the metadata primary, so only the metadata
    ///     primary's miss is final.
    /// </summary>
    private async Task<bool> IsBackupNamespaceMissAsync(IggyInvalidStatusCodeException status,
        VsrConnection connection, long deadline, CancellationToken token) =>
        status.FromServer && !status.Committed && IsNamespaceMiss(status.StatusCode)
        && !await IsMetadataPrimaryAsync(connection, deadline, token);

    /// <summary>
    ///     Whether <paramref name="connection" /> is the link of the metadata primary. The belief can predate a view
    ///     change, so the roster that <paramref name="connection" /> reads must still name its replica. No call
    ///     reaches a believed metadata primary without a ready link, so then that roster read also moves the belief.
    /// </summary>
    private async Task<bool> IsMetadataPrimaryAsync(VsrConnection connection, long deadline, CancellationToken token)
    {
        lock (_pollRoutingGate)
        {
            if (MetadataPrimaryLinkLocked() is { IsReady: true } believed && !ReferenceEquals(believed, connection))
            {
                return false;
            }
        }
        var attempt = await connection.SendAttemptAsync(CommandCodes.GET_CLUSTER_METADATA_CODE,
            ReadOnlyMemory<byte>.Empty, deadline, deadline, false, token, retryTransient: false,
            context: new PartitionContext(0, 0, _metadataWatermark));
        if (attempt.Error is not null)
        {
            return false;
        }
        using var response = attempt.Response!;
        RememberRoster(BinaryMapper.MapClusterMetadata(response.Memory.Span));
        lock (_pollRoutingGate)
        {
            return ReferenceEquals(MetadataPrimaryLinkLocked(), connection);
        }
    }

    private VsrConnection? MetadataPrimaryLinkLocked() =>
        _metadataReplica is { } replica && _replicaLinks.TryGetValue(replica, out var slot) ? slot.Connection : null;

    private static bool IsNamespaceMiss(int status) => status is VsrError.STREAM_ID_NOT_FOUND
        or VsrError.STREAM_NAME_NOT_FOUND or VsrError.TOPIC_ID_NOT_FOUND or VsrError.TOPIC_NAME_NOT_FOUND
        or VsrError.PARTITION_NOT_FOUND;

    private async Task RefreshPartitionRosterAsync(long deadline, CancellationToken token)
    {
        using var response = await SendPollControlAsync(CommandCodes.GET_CLUSTER_METADATA_CODE,
            ReadOnlyMemory<byte>.Empty, deadline, token);
        RememberRoster(BinaryMapper.MapClusterMetadata(response.Memory.Span));
    }

    private async Task<IMemoryOwner<byte>> SendPollControlAsync(int code, ReadOnlyMemory<byte> payload,
        long deadline, CancellationToken token)
    {
        var retries = 0;
        while (true)
        {
            foreach (var connection in ReadyReplicaLinksOrThrow())
            {
                var context = new PartitionContext(0, 0, _metadataWatermark);
                var attempt = await connection.SendAttemptAsync(code, payload, deadline, deadline, false, token,
                    retryTransient: false, context: context);
                if (attempt.Error is null)
                {
                    return attempt.Response!;
                }
                if (IsSessionFailure(attempt.Error))
                {
                    ScheduleSessionRecovery(_consensusSession.Generation);
                }
                if (attempt.Error is not IggyInvalidStatusCodeException
                    { StatusCode: VsrError.TRANSIENT_NOT_ACCEPTED or VsrError.NOT_PRIMARY }
                    && !VsrConnection.IsConnectionException(attempt.Error))
                {
                    ExceptionDispatchInfo.Throw(CallerError(attempt.Error));
                }
            }
            var pause = PollRoutingRetryDelay(retries++, Random.Shared.NextDouble());
            if (Environment.TickCount64 + (long)pause.TotalMilliseconds >= deadline)
            {
                throw PollNotAccepted();
            }
            await Task.Delay(pause, token);
        }
    }

    internal static TimeSpan PollRoutingRetryDelay(int retry, double sample)
    {
        var ceiling = Math.Min(PollRoutingRetryMaxIntervalMs, PollRoutingRetryIntervalMs * Math.Pow(2, retry));
        return TimeSpan.FromMilliseconds(ceiling * sample);
    }

    private void ObserveMetadataCommit(ulong commit, ulong dialedUnder)
    {
        lock (_pollRoutingGate)
        {
            if (_consensusSession.LinkGeneration(dialedUnder) is not null)
            {
                _metadataWatermark = Math.Max(_metadataWatermark, commit);
            }
        }
    }

    private void ClearPollSession()
    {
        lock (_pollRoutingGate)
        {
            _partitionRoutes.Clear();
        }
    }

    private void RemovePartitionRoute(PartitionRouteKey key, CachedRoute? refusedRoute = null)
    {
        lock (_pollRoutingGate)
        {
            var refused = refusedRoute;
            if (refused is null && !_partitionRoutes.TryGetValue(key, out refused))
            {
                return;
            }
            if (refused.Generation != _consensusSession.Generation)
            {
                return;
            }
            var minimumView = refusedRoute is not null && refused.Route.View < uint.MaxValue
                ? refused.Route.View + 1 : refused.MinimumView;
            foreach (var (alias, route) in _partitionRoutes.Where(entry => entry.Key.PartitionId == key.PartitionId
                && entry.Value.Route.Incarnation == refused.Route.Incarnation).ToArray())
            {
                _partitionRoutes[alias] = route with
                {
                    Stale = route.Stale || refusedRoute is null || route.Route.View <= refused.Route.View,
                    MinimumView = Math.Max(route.MinimumView, minimumView)
                };
            }
        }
    }

    // Typed, raw and replayed calls share this success path, and only the request body carries the new credentials.
    private void RefreshCommittedCredentials(int code, ReadOnlySpan<byte> body, ulong generation)
    {
        if (code == CommandCodes.CHANGE_PASSWORD_CODE)
        {
            var position = 0;
            var user = ReadIdentifier(body, ref position);
            var currentPasswordLength = body[position];
            RefreshRememberedCredentials(user, null, ReadName(body, position + 1 + currentPasswordLength), generation);
        }
        else if (code == CommandCodes.UPDATE_RESOURCE_CODE && body.Length > 0 && body[0] == (byte)OptionsScope.User)
        {
            var position = 1;
            var user = ReadIdentifier(body, ref position);
            var resetCount = body[position++];
            for (var reset = 0; reset < resetCount; reset++)
            {
                position += 1 + body[position];
            }

            if (PatchedUsername(body[position..]) is { } username)
            {
                RefreshRememberedCredentials(user, username, null, generation);
            }
        }

        static string ReadName(ReadOnlySpan<byte> body, int position) =>
            Encoding.UTF8.GetString(body.Slice(position + 1, body[position]));
    }

    /// <summary>The username a user update sets, read from the options block that ends its body.</summary>
    private static string? PatchedUsername(ReadOnlySpan<byte> options)
    {
        var position = 0;
        while (position < options.Length)
        {
            var keyKind = options[position];
            var keyLength = BinaryPrimitives.ReadInt32LittleEndian(options[(position + 1)..]);
            var key = options.Slice(position + 5, keyLength);
            position += 5 + keyLength;
            var valueKind = options[position];
            var valueLength = BinaryPrimitives.ReadInt32LittleEndian(options[(position + 1)..]);
            var value = options.Slice(position + 5, valueLength);
            position += 5 + valueLength;
            if (keyKind == StringKind && valueKind == StringKind && key.SequenceEqual("username"u8))
            {
                return Encoding.UTF8.GetString(value);
            }
        }

        return null;
    }

    private void RefreshRememberedCredentials(Identifier user, string? username, string? password, ulong generation)
    {
        lock (_pollRoutingGate)
        {
            var remembered = _rememberedLogin;
            if (_consensusSession.Ended(generation) is not null || remembered is null
                || !string.IsNullOrEmpty(remembered.PersonalAccessToken)
                || !(user.Kind == IdKind.Numeric ? user.GetUInt32() == _rememberedUserId
                    : user.GetString() == remembered.Username))
            {
                return;
            }

            _rememberedLogin = AutoLoginSettings.For(username ?? remembered.Username, password ?? remembered.Password);
            var configured = _configuredLoginOverride ?? _configuration.AutoLoginSettings;
            if (configured.Enabled && string.IsNullOrEmpty(configured.PersonalAccessToken)
                                   && configured.Username == remembered.Username)
            {
                _configuredLoginOverride = AutoLoginSettings.For(username ?? configured.Username,
                    password ?? configured.Password);
            }
        }
    }

    private static IggyInvalidStatusCodeException PollNotAccepted()
    {
        return VsrError.Exception(VsrError.TRANSIENT_NOT_ACCEPTED, "The primary poll was not admitted.");
    }

    private static IggyInvalidStatusCodeException ReplicaNotAccepted(byte replica)
    {
        return VsrError.Exception(VsrError.TRANSIENT_NOT_ACCEPTED, $"Replica {replica} did not answer before the deadline.");
    }
}
