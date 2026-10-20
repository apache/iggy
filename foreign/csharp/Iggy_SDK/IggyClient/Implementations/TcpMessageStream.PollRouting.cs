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
    internal const int MAX_POLL_CONNECTIONS = 256;
    private const int PollRoutingRetryIntervalMs = 50;
    private const int ConsumerSessionSize = 32;
    private const int ConsumerSessionEpochOffset = 16;
    private const int ConsumerSessionWatermarkOffset = 24;
    private const int IdentifierPrefixSize = 2;
    private const byte ConsumerGroupWireKind = 2;

    private readonly object _pollRoutingGate = new();
    private readonly Dictionary<PartitionRouteKey, CachedRoute> _partitionRoutes = [];
    private readonly Dictionary<string, PollConnectionSlot> _pollConnections = [];
    private readonly Dictionary<byte, string> _replicaEndpoints = [];
    private ulong _metadataWatermark;
    private int _clusterNodeCount;
    private uint? _rememberedUserId;
    private AutoLoginSettings? _configuredLoginOverride;

    private readonly record struct PartitionRouteKey(Identifier StreamId, Identifier TopicId, uint PartitionId);
    private sealed record CachedRoute(string Endpoint, byte[] Attachment, ulong Generation, ulong Watermark,
        PartitionRoute Route);
    private readonly record struct SendTarget(PartitionRouteKey Key, byte[] Query);

    private sealed class PollConnectionSlot
    {
        internal readonly SemaphoreSlim Gate = new(1, 1);
        internal VsrConnection? Connection;
        internal byte[]? Attachment;
        internal volatile bool Retired;
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

    private async Task<IMemoryOwner<byte>> SendRoutedAsync(int code, SendTarget target,
        ReadOnlyMemory<byte> payload, PartitionContext? context, CancellationToken token)
    {
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(token);
        cancellation.CancelAfter(VsrRequestTimeoutMs);
        var deadline = Environment.TickCount64 + VsrRequestTimeoutMs;
        // A new route can change the destination, but not the request's identity or captured position.
        var captured = context;
        SessionFrame? frame = null;
        ulong? generation = null;
        var refreshHistory = code == CommandCodes.SEND_MESSAGES_CODE && context is null;
        var refusedPrimary = false;
        var lifecycleInterval = LifecycleRetryIntervalMs;
        IggyInvalidStatusCodeException? refusal = null;
        try
        {
            while (true)
            {
                try
                {
                    var route = await GetPartitionRouteAsync(target.Key, target.Query, deadline, cancellation.Token);
                    captured ??= route.Route.Context;
                    if (generation is not null && generation != route.Generation)
                    {
                        throw PollNotAccepted();
                    }
                    generation ??= route.Generation;
                    frame ??= _consensusSession.Resolve(VsrOperations.ForCode(code));
                    refusal = null;
                    if (Volatile.Read(ref _clusterNodeCount) <= 1)
                    {
                        var attempt = await SendVsrAttemptAsync(code, payload, Environment.TickCount64, deadline,
                            false, cancellation.Token, captured.Value, frame,
                            retryTransient: VsrOperations.ForCode(code) != VsrOperation.NonReplicated);
                        if (attempt.Error is not null)
                        {
                            if (attempt.RequestStarted && !VsrOperations.IsReplaySafeRead(code, false, payload.Span)
                                && !IsDefinitiveVerdict(attempt.Error))
                            {
                                throw new VsrRequestOutcomeUnknownException(attempt.Error);
                            }
                            ExceptionDispatchInfo.Throw(attempt.Error);
                        }
                        return attempt.Response!;
                    }
                    return await SendPrimaryOnceAsync(target.Key, code, payload, route, captured.Value,
                        frame.Value, deadline, cancellation.Token);
                }
                catch (IggyInvalidStatusCodeException error) when (error.StatusCode is
                    VsrError.TRANSIENT_NOT_ACCEPTED or VsrError.NOT_PRIMARY)
                {
                    RemovePartitionRoute(target.Key);
                    refusal = error;
                    if ((generation is not null && generation != _consensusSession.Generation)
                        || Environment.TickCount64 + PollRoutingRetryIntervalMs >= deadline)
                    {
                        throw;
                    }
                    if (error.StatusCode != VsrError.NOT_PRIMARY || refusedPrimary)
                    {
                        await Task.Delay(PollRoutingRetryIntervalMs, cancellation.Token);
                    }
                    refusedPrimary |= error.StatusCode == VsrError.NOT_PRIMARY;
                }
                catch (IggyInvalidStatusCodeException error) when (error.StatusCode == VsrError.HISTORY_UNAVAILABLE)
                {
                    RemovePartitionRoute(target.Key);
                    if (!refreshHistory)
                    {
                        throw;
                    }
                    refreshHistory = false;
                    captured = null;
                    frame = null;
                    refusal = error;
                }
                // The partition plane does not commit LIFECYCLE_BUSY today. A routed request still handles it, so a
                // server that starts committing it needs no client change.
                catch (IggyInvalidStatusCodeException error) when (error.FromServer
                    && error.StatusCode == VsrError.LIFECYCLE_BUSY
                    && error.CommandCode == code
                    && Environment.TickCount64 + lifecycleInterval < deadline)
                {
                    refusal = error;
                    await Task.Delay(lifecycleInterval, cancellation.Token);
                    lifecycleInterval = Math.Min(lifecycleInterval * 2, LifecycleRetryMaxIntervalMs);
                    frame = null;
                }
            }
        }
        catch (OperationCanceledException error) when (!token.IsCancellationRequested)
        {
            RemovePartitionRoute(target.Key);
            // The budget can expire in the backoff or in a read-only control request that follows it, before the
            // send call for the request starts again, so the last refusal still decides the outcome. An expiry
            // inside that send call reports an unknown outcome even before the write, which is conservative.
            if (refusal is not null)
            {
                ExceptionDispatchInfo.Throw(refusal);
            }

            throw new VsrRequestOutcomeUnknownException(error);
        }
        // The read timer of a route lookup fires at the same deadline as the budget and turns into a timeout
        // IOException when it wins, and a redial can fail the same way. A kept refusal means no send started
        // since, so it still decides the outcome.
        catch (Exception error) when (error is IOException or SocketException
            && refusal is not null
            && Environment.TickCount64 + PollRoutingRetryIntervalMs >= deadline)
        {
            RemovePartitionRoute(target.Key);
            ExceptionDispatchInfo.Throw(refusal);
            throw;
        }
    }

    private async Task<IMemoryOwner<byte>> SendPrimaryOnceAsync(PartitionRouteKey key, int code,
        ReadOnlyMemory<byte> payload, CachedRoute route, PartitionContext captured, SessionFrame frame,
        long deadline, CancellationToken token)
    {
        PollConnectionSlot slot;
        lock (_pollRoutingGate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (!_pollConnections.TryGetValue(route.Endpoint, out slot!))
            {
                if (_pollConnections.Count >= MAX_POLL_CONNECTIONS)
                {
                    RetireIdlePollConnection();
                }
                slot = new PollConnectionSlot();
                _pollConnections.Add(route.Endpoint, slot);
            }
        }

        await slot.Gate.WaitAsync(token);
        var started = false;
        try
        {
            ValidatePollRoute(route, slot);
            if (slot.Connection is null)
            {
                var connection = await ConnectPollConnectionAsync(route, token);
                lock (_pollRoutingGate)
                {
                    if (slot.Retired || _disposed)
                    {
                        connection.Dispose();
                        throw PollNotAccepted();
                    }
                    slot.Connection = connection;
                }
            }
            ValidatePollRoute(route, slot);
            if (slot.Attachment is null || !slot.Attachment.AsSpan().SequenceEqual(route.Attachment))
            {
                using var attached = await SendPollExchangeAsync(slot.Connection, CommandCodes.BIND_SESSION_CODE,
                    LoginRegister.SerializeBindSession(route.Attachment, _consensusSession.BindSecret), deadline, token);
                var binding = LoginRegister.Deserialize(attached.Memory.Span);
                if (binding.Session != frame.SessionId)
                {
                    throw VsrError.Exception(VsrError.INVALID_FORMAT, "BindSession reply carried a different session epoch.");
                }
                slot.Attachment = route.Attachment;
            }
            ValidatePollRoute(route, slot);
            started = true;
            return await SendPollExchangeAsync(slot.Connection, code, payload, deadline, token, captured, frame);
        }
        catch (IggyInvalidStatusCodeException error) when (error.StatusCode is
            VsrError.TRANSIENT_NOT_ACCEPTED or VsrError.NOT_PRIMARY or VsrError.LIFECYCLE_BUSY)
        {
            throw;
        }
        catch (Exception error)
        {
            slot.Connection?.Dispose();
            slot.Connection = null;
            slot.Attachment = null;
            RemovePartitionRoute(key);
            if (started && !VsrOperations.IsReplaySafeRead(code, false, payload.Span)
                && !IsDefinitiveVerdict(error) && error is not OperationCanceledException)
            {
                throw new VsrRequestOutcomeUnknownException(error);
            }
            if (IsPollConnectionFailure(error)
                || error is IggyInvalidStatusCodeException
                { StatusCode: VsrError.TRANSIENT_NOT_COMMITTED or VsrError.REQUEST_TOO_OLD })
            {
                throw PollNotAccepted();
            }
            throw;
        }
        finally
        {
            slot.Gate.Release();
        }
    }

    private void ValidatePollRoute(CachedRoute route, PollConnectionSlot slot)
    {
        lock (_pollRoutingGate)
        {
            if (slot.Retired || route.Generation != _consensusSession.Generation
                || route.Watermark < _metadataWatermark)
            {
                throw PollNotAccepted();
            }
        }
    }

    private void RetireIdlePollConnection()
    {
        foreach (var (endpoint, slot) in _pollConnections)
        {
            if (!slot.Gate.Wait(0))
            {
                continue;
            }
            try
            {
                slot.Retired = true;
                slot.Connection?.Dispose();
                _pollConnections.Remove(endpoint);
                return;
            }
            finally
            {
                slot.Gate.Release();
            }
        }
        throw PollNotAccepted();
    }

    private async Task<CachedRoute> GetPartitionRouteAsync(PartitionRouteKey key, ReadOnlyMemory<byte> query,
        long deadline, CancellationToken token)
    {
        lock (_pollRoutingGate)
        {
            if (_partitionRoutes.TryGetValue(key, out var cached) && cached.Watermark >= _metadataWatermark
                && cached.Generation == _consensusSession.Generation)
            {
                return cached;
            }
        }
        if (Volatile.Read(ref _clusterNodeCount) == 0)
        {
            await RefreshPartitionRosterAsync(deadline, token);
        }
        var generation = _consensusSession.Generation;
        using var response = await SendPollControlAsync(CommandCodes.GET_PARTITION_ROUTE_CODE, query, deadline, token);
        var route = BinaryMapper.MapPartitionRoute(response.Memory.Span);
        _groupState.SetPartitionCount(new TopicKey(key.StreamId, key.TopicId), route.PartitionsCount);
        bool known;
        lock (_pollRoutingGate)
        {
            known = _replicaEndpoints.ContainsKey(route.PrimaryReplica);
        }
        if (!known)
        {
            await RefreshPartitionRosterAsync(deadline, token);
        }
        lock (_pollRoutingGate)
        {
            if (generation != _consensusSession.Generation
                || !_replicaEndpoints.TryGetValue(route.PrimaryReplica, out var endpoint))
            {
                throw PollNotAccepted();
            }
            var session = _consensusSession.Resolve(VsrOperation.NonReplicated);
            if (session.SessionId == 0)
            {
                throw PollNotAccepted();
            }
            var watermark = Math.Max(_metadataWatermark, route.MetadataOp);
            var attachment = new byte[ConsumerSessionSize];
            BinaryPrimitives.WriteUInt128LittleEndian(attachment, session.ClientId);
            BinaryPrimitives.WriteUInt64LittleEndian(attachment.AsSpan(ConsumerSessionEpochOffset), session.SessionId);
            BinaryPrimitives.WriteUInt64LittleEndian(attachment.AsSpan(ConsumerSessionWatermarkOffset), watermark);
            var cached = new CachedRoute(endpoint, attachment, generation, watermark, route);
            if (_partitionRoutes.Count >= MaxCachedPartitionRoutes)
            {
                _partitionRoutes.Clear();
            }
            _partitionRoutes[key] = cached;
            return cached;
        }
    }

    private async Task RefreshPartitionRosterAsync(long deadline, CancellationToken token)
    {
        using var response = await SendPollControlAsync(CommandCodes.GET_CLUSTER_METADATA_CODE,
            ReadOnlyMemory<byte>.Empty, deadline, token);
        RememberRoster(BinaryMapper.MapClusterMetadata(response.Memory.Span));
    }

    private async Task<IMemoryOwner<byte>> SendPollControlAsync(int code, ReadOnlyMemory<byte> payload,
        long deadline, CancellationToken token)
    {
        ulong watermark;
        lock (_pollRoutingGate)
        {
            watermark = _metadataWatermark;
        }
        var context = new PartitionContext(0, 0, watermark);
        var attempt = await SendVsrAttemptAsync(code, payload, Environment.TickCount64, deadline, false, token, context);
        if (attempt.Error is not null && IsPollConnectionFailure(attempt.Error)
            && _configuration.ReconnectionSettings.Enabled)
        {
            await DropVsrConnectionAsync(attempt.Connection);
            using var reconnectCancellation = CancellationTokenSource.CreateLinkedTokenSource(token);
            reconnectCancellation.CancelAfter((int)Math.Clamp(deadline - Environment.TickCount64, 0, int.MaxValue));
            await ConnectAsync(true, false, reconnectCancellation.Token, sameEndpointOnly: true);
            attempt = await SendVsrAttemptAsync(code, payload, Environment.TickCount64, deadline, false, token, context);
        }
        if (attempt.Error is not null)
        {
            ExceptionDispatchInfo.Throw(attempt.Error);
        }
        return attempt.Response!;
    }

    private async Task<VsrConnection> ConnectPollConnectionAsync(CachedRoute route, CancellationToken token)
    {
        var endpoint = route.Endpoint;
        if (!ServerAddress.TryParse(endpoint, out var host, out var port))
        {
            throw new InvalidBaseAddressException();
        }

        using var dialCancellation = CancellationTokenSource.CreateLinkedTokenSource(token);
        dialCancellation.CancelAfter(FailoverDialTimeout);
        Socket? socket = new(ServerAddress.AddressFamilyOf(host), SocketType.Stream, ProtocolType.Tcp);
        VsrConnection? connection = null;
        try
        {
            socket.NoDelay = true;
            if (_configuration.SendBufferSize is { } sendBufferSize)
            {
                socket.SendBufferSize = sendBufferSize;
            }

            if (_configuration.ReceiveBufferSize is { } receiveBufferSize)
            {
                socket.ReceiveBufferSize = receiveBufferSize;
            }

            await socket.ConnectAsync(host, port, dialCancellation.Token);
            var stream = _configuration.TlsSettings.Enabled
                ? await CreateSslStreamAndAuthenticate(socket, _configuration.TlsSettings, dialCancellation.Token)
                : new NetworkStream(socket, true);
            socket = null;
            // An offset write on this connection is a replicated request of the bound session, so its request id
            // comes from the same counter as the membership connection's, or the two would reuse ids.
            connection = new VsrConnection(stream, _consensusSession, _configuration.MaxResponseFrameSize,
                VsrRequestTimeoutMs, dropped => dropped.Dispose(), _logger);
            return connection;
        }
        catch (OperationCanceledException) when (!token.IsCancellationRequested)
        {
            connection?.Dispose();
            throw new IOException($"Timed out connecting to partition primary {endpoint}.");
        }
        catch
        {
            connection?.Dispose();
            throw;
        }
        finally
        {
            socket?.Dispose();
        }
    }

    private static async Task<IMemoryOwner<byte>> SendPollExchangeAsync(VsrConnection connection, int code,
        ReadOnlyMemory<byte> payload, long deadline, CancellationToken token, PartitionContext context = default,
        SessionFrame? frame = null)
    {
        var attempt = await connection.SendAttemptAsync(code, payload, Environment.TickCount64, deadline,
            HasSensitiveReply(code), token, retryTransient: VsrOperations.ForCode(code) != VsrOperation.NonReplicated,
            context: context, retainedFrame: frame);
        if (attempt.Error is not null)
        {
            ExceptionDispatchInfo.Throw(attempt.Error);
        }
        return attempt.Response!;
    }

    private void ObserveMetadataCommit(ulong commit)
    {
        lock (_pollRoutingGate)
        {
            _metadataWatermark = Math.Max(_metadataWatermark, commit);
        }
    }

    private void ClearPollSession()
    {
        lock (_pollRoutingGate)
        {
            _partitionRoutes.Clear();
            foreach (var slot in _pollConnections.Values)
            {
                slot.Retired = true;
                slot.Connection?.Dispose();
            }
            _pollConnections.Clear();
        }
    }

    private void RemovePartitionRoute(PartitionRouteKey key)
    {
        lock (_pollRoutingGate)
        {
            _partitionRoutes.Remove(key);
        }
    }

    private void RefreshRememberedCredentials(Identifier user, string? username, string? password)
    {
        lock (_pollRoutingGate)
        {
            var remembered = _rememberedLogin;
            if (remembered is null || !string.IsNullOrEmpty(remembered.PersonalAccessToken)
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

    private static bool IsPollConnectionFailure(Exception error)
    {
        return IsLostConnection(error) || error is VsrSessionEvictedException
            or IggyInvalidStatusCodeException { StatusCode: VsrError.UNAUTHENTICATED or VsrError.STALE_CLIENT };
    }

    private static IggyInvalidStatusCodeException PollNotAccepted()
    {
        return VsrError.Exception(VsrError.TRANSIENT_NOT_ACCEPTED, "The primary poll was not admitted.");
    }
}
