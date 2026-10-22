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
using System.Runtime.ExceptionServices;
using System.Text;
using Apache.Iggy.Configuration;
using Apache.Iggy.Contracts;
using Apache.Iggy.Enums;
using Apache.Iggy.Extensions;
using Apache.Iggy.Exceptions;
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
            || code == CommandCodes.POLL_MESSAGES_CODE && group))
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
        TimeSpan? timeout = null, ulong? startedUnder = null)
    {
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(token, _clientLifetime.Token);
        var budget = timeout ?? TimeSpan.FromMilliseconds(VsrRequestTimeoutMs);
        if (budget <= TimeSpan.Zero || budget.TotalMilliseconds > int.MaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout));
        }
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
        var refreshHistory = code == CommandCodes.SEND_MESSAGES_CODE && context is null && retained is null;
        var retryDelay = LifecycleRetryIntervalMs;
        var routingRetries = 0;
        var lastWasNotPrimary = false;
        var askMetadataPrimaryFirst = false;
        var terminal = false;
        var frameAcquired = false;
        var startOver = false;
        IggyInvalidStatusCodeException? refusal = null;
        IggyInvalidStatusCodeException? rejection = null;
        CancellationTokenRegistration operationCancellation = default;
        try
        {
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
                        if (operation is null && (retainable || progress))
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
                                _mutations.ThrowIfRetained(new MutationLane(knownContext.Incarnation, partition.Key.PartitionId));
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
                        var lane = target is { } selected
                            ? new MutationLane(captured.Value.Incarnation, selected.Key.PartitionId) : MutationLane.Metadata;
                        operation = await _mutations.EnterAsync(lane, code, payload, captured.Value, retainable, generation,
                            cancellation.Token);
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
                    sending = true;
                    var attempt = operation is null
                        ? await connection.SendAttemptAsync(code, payload, deadline, deadline, HasSensitiveReply(code),
                            cancellation.Token, retryTransient: false, context: captured.Value, retainedFrame: frame)
                        : await connection.SendFrameAsync(operation, deadline, HasSensitiveReply(code), cancellation.Token);
                    if (attempt.Error is null)
                    {
                        if (operation is { Uncertain: true } && !attempt.Committed)
                        {
                            attempt.Response?.Dispose();
                            throw new VsrRequestOutcomeUnknownException(
                                VsrError.Exception(VsrError.TRANSIENT_NOT_COMMITTED, "The reply does not prove commit."),
                                operation.Identity.RequestId);
                        }
                        terminal = true;
                        RefreshCommittedCredentials(code, (operation?.Body ?? payload).Span, generation);
                        return attempt.Response!;
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
                catch (IggyInvalidStatusCodeException error) when (error.StatusCode is
                    VsrError.TRANSIENT_NOT_ACCEPTED or VsrError.NOT_PRIMARY or VsrError.TRANSIENT_NOT_COMMITTED)
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
                    if (target is { } failed)
                    {
                        RemovePartitionRoute(failed.Key, error.StatusCode == VsrError.NOT_PRIMARY ? route : null);
                    }
                    await Task.Delay(pause, cancellation.Token);
                    if (target is null)
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
            throw _consensusSession.Ended(generation) ?? PollNotAccepted();
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
        return code == CommandCodes.POLL_MESSAGES_CODE && body.Length >= PollTailSize
            && (body[^1] != 0 || body[^PollTailSize] == NextStrategy);
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
}
