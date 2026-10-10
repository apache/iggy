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
using Apache.Iggy.Exceptions;
using Apache.Iggy.Mappers;
using Apache.Iggy.Utils;
using Apache.Iggy.Vsr;

namespace Apache.Iggy.IggyClient.Implementations;

public sealed partial class TcpMessageStream
{
    private const int MaxCachedPollRoutes = 4096;
    private const int MaxCachedPartitionContexts = 4096;
    internal const int MAX_POLL_CONNECTIONS = 256;
    private const int PollRoutingRetryIntervalMs = 50;
    private const int ConsumerSessionSize = 32;
    private const int ConsumerSessionEpochOffset = 16;
    private const int ConsumerSessionWatermarkOffset = 24;
    private const int PollOptionsSize = 1 + sizeof(ulong) + sizeof(uint) + 1;
    private const int IdentifierPrefixSize = 2;
    private const int StoreOffsetSuffixSize = sizeof(ulong) + 1;
    private const int DeleteOffsetSuffixSize = 1;

    private readonly object _pollRoutingGate = new();
    private readonly Dictionary<PollRouteKey, PollRoute> _pollRoutes = [];
    private readonly Dictionary<string, PollConnectionSlot> _pollConnections = [];
    private readonly Dictionary<(int QueryCode, string Route), CachedContext> _partitionContexts = [];
    private ulong _metadataWatermark;
    private int _clusterNodeCount;
    private uint? _rememberedUserId;
    private AutoLoginSettings? _configuredLoginOverride;

    /// <summary>Polls route through GetPollRouting and offset writes through GetConsumerOffsetRouting.</summary>
    private readonly record struct PollRouteKey(int RoutingCode, Identifier StreamId, Identifier TopicId,
        ConsumerType ConsumerType, Identifier ConsumerId, uint? PartitionId);

    private sealed record PollRoute(string Endpoint, byte[] Attachment, ulong Generation, ulong Watermark, PartitionContext Context);

    private sealed class PollConnectionSlot
    {
        internal readonly SemaphoreSlim Gate = new(1, 1);
        internal VsrConnection? Connection;
        internal byte[]? Attachment;
        internal volatile bool Retired;
    }

    /// <summary>
    ///     The routing request whose reply carries the context of a poll or an offset write. Requests with the same
    ///     <c>Route</c> share the context, so polls of a partition share it whatever their options.
    /// </summary>
    private sealed record ContextQuery(int Code, byte[] Body, int ContextOffset, string Route);

    private readonly record struct CachedContext(PartitionContext Context, ulong Generation, ulong Watermark);

    /// <summary>The partition a send targets and the GetSendContext body that captures its context.</summary>
    private readonly record struct SendTarget(TopicKey Topic, uint PartitionId, byte[] Query);

    private async Task<PartitionContext> CapturePartitionContextAsync(int code, ReadOnlyMemory<byte> body,
        CancellationToken token)
    {
        var query = TryBuildContextQuery(code, body.Span);
        return query is null ? default : await GetPartitionContextAsync(query, token);
    }

    /// <summary>
    ///     A cached context is reused until a request carrying it fails, the session changes, or this client
    ///     commits a metadata operation, such as a delete that ends the partition's incarnation.
    /// </summary>
    private async Task<PartitionContext> GetPartitionContextAsync(ContextQuery query, CancellationToken token)
    {
        ulong generation;
        ulong watermark;
        lock (_pollRoutingGate)
        {
            generation = _consensusSession.Generation;
            watermark = _metadataWatermark;
            if (_partitionContexts.TryGetValue((query.Code, query.Route), out var cached)
                && cached.Generation == generation && cached.Watermark >= watermark)
            {
                return cached.Context;
            }
        }

        using var response = await SendWithResponseAsync(query.Code, query.Body, token: token);
        var context = BinaryMapper.MapPartitionContext(response.Memory.Span[query.ContextOffset..]);
        lock (_pollRoutingGate)
        {
            if (_partitionContexts.Count >= MaxCachedPartitionContexts)
            {
                _partitionContexts.Clear();
            }

            _partitionContexts[(query.Code, query.Route)] = new CachedContext(context, generation, watermark);
        }

        return context;
    }

    private void ForgetPartitionContext(ContextQuery query)
    {
        lock (_pollRoutingGate)
        {
            _partitionContexts.Remove((query.Code, query.Route));
        }
    }

    /// <summary>
    ///     The context of a send is cached per partition until a send to its topic is refused with status 87, or
    ///     this client deletes a stream or a topic, or creates or deletes partitions. Other failures keep it.
    /// </summary>
    private async Task<PartitionContext> GetSendContextAsync(SendTarget target, CancellationToken token)
    {
        if (_groupState.SendContext(target.Topic, target.PartitionId) is { } cached)
        {
            return cached;
        }

        using var response = await SendWithResponseAsync(CommandCodes.GET_SEND_CONTEXT_CODE, target.Query,
            token: token);
        var context = BinaryMapper.MapPartitionContext(response.Memory.Span);
        _groupState.SetSendContext(target.Topic, target.PartitionId, context);
        return context;
    }

    /// <summary>
    ///     Reads the partition a SendMessages body targets. Only an explicit partition id names one: balanced and
    ///     message-key partitioning leave no partition to capture a context for, and a send without a context
    ///     would land on whatever incarnation the partition has when it arrives.
    /// </summary>
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
        return new SendTarget(new TopicKey(streamId, topicId), partitionId, query);
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
            Kind = (IdKind)body[position],
            Value = body.Slice(position + IdentifierPrefixSize, body[position + 1]).ToArray()
        };
        position += IdentifierPrefixSize + body[position + 1];
        return identifier;
    }

    private static IggyInvalidStatusCodeException TruncatedSend()
    {
        return VsrError.Exception(VsrError.INVALID_COMMAND, "The SendMessages body is truncated.");
    }

    private static ContextQuery? TryBuildContextQuery(int code, ReadOnlySpan<byte> body)
    {
        if (code is CommandCodes.STORE_CONSUMER_OFFSET_CODE or CommandCodes.DELETE_CONSUMER_OFFSET_CODE)
        {
            var suffixSize = code == CommandCodes.STORE_CONSUMER_OFFSET_CODE
                ? StoreOffsetSuffixSize : DeleteOffsetSuffixSize;
            if (body.Length <= suffixSize)
            {
                return null;
            }
            var query = body[..^suffixSize].ToArray();
            return new ContextQuery(CommandCodes.GET_CONSUMER_OFFSET_ROUTING_CODE, query, ConsumerSessionSize,
                Convert.ToBase64String(query));
        }
        if (code != CommandCodes.POLL_MESSAGES_CODE || body.Length <= PollOptionsSize)
        {
            return null;
        }
        return new ContextQuery(CommandCodes.GET_POLL_ROUTING_CODE, body.ToArray(), ConsumerSessionSize,
            Convert.ToBase64String(body[..^PollOptionsSize]));
    }

    /// <summary>
    ///     Sends a poll or an offset write to the partition primary, over a data connection that leaves the
    ///     membership connection where it is. A caller context, or else the first context a route reports, is
    ///     retained across every retry, so a retry never moves the request onto a newer incarnation or owner.
    /// </summary>
    /// <param name="code">The poll or offset write command code.</param>
    /// <param name="key">The cached route of the request.</param>
    /// <param name="routing">The body of the routing request.</param>
    /// <param name="payload">The body of the request.</param>
    /// <param name="context">The caller context, or null to take the context the route reports.</param>
    /// <param name="token">The cancellation token.</param>
    private async Task<IMemoryOwner<byte>> SendRoutedAsync(int code, PollRouteKey key, ReadOnlyMemory<byte> routing,
        ReadOnlyMemory<byte> payload, PartitionContext? context, CancellationToken token)
    {
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(token);
        cancellation.CancelAfter(VsrRequestTimeoutMs);
        var deadline = Environment.TickCount64 + VsrRequestTimeoutMs;
        var captured = context;
        try
        {
            while (true)
            {
                try
                {
                    if (Volatile.Read(ref _clusterNodeCount) == 0)
                    {
                        using var metadata = await SendPollControlAsync(CommandCodes.GET_CLUSTER_METADATA_CODE,
                            ReadOnlyMemory<byte>.Empty, deadline, cancellation.Token);
                        if (metadata.Memory.Length == 0)
                        {
                            throw new MalformedResponseException("Poll routing requires a nonempty cluster roster.");
                        }

                        RememberRoster(BinaryMapper.MapClusterMetadata(metadata.Memory.Span));
                    }

                    if (Volatile.Read(ref _clusterNodeCount) <= 1)
                    {
                        captured ??= await CapturePartitionContextAsync(code, payload, cancellation.Token);
                        return await SendWithResponseAsync(code, payload, token: cancellation.Token,
                            capturedContext: captured);
                    }
                    var route = await GetPollRouteAsync(key, routing, deadline, cancellation.Token);
                    captured ??= route.Context;
                    return await SendPrimaryOnceAsync(key,
                        code == CommandCodes.POLL_MESSAGES_CODE ? CommandCodes.POLL_MESSAGES_ON_PRIMARY_CODE : code,
                        payload, route, captured.Value, deadline, cancellation.Token);
                }
                catch (IggyInvalidStatusCodeException error) when (error.StatusCode == VsrError.TRANSIENT_NOT_ACCEPTED)
                {
                    RemovePollRoute(key);
                    if (Environment.TickCount64 + PollRoutingRetryIntervalMs >= deadline)
                    {
                        throw;
                    }

                    try
                    {
                        await Task.Delay(PollRoutingRetryIntervalMs, cancellation.Token);
                    }
                    catch (OperationCanceledException) when (!token.IsCancellationRequested)
                    {
                        ExceptionDispatchInfo.Throw(error);
                    }
                }
            }
        }
        catch (OperationCanceledException error) when (!token.IsCancellationRequested)
        {
            RemovePollRoute(key);
            throw new VsrRequestOutcomeUnknownException(error);
        }
    }

    private async Task<IMemoryOwner<byte>> SendPrimaryOnceAsync(PollRouteKey key, int code,
        ReadOnlyMemory<byte> payload, PollRoute route, PartitionContext captured, long deadline,
        CancellationToken token)
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
        var polling = false;
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
                using var attached = await SendPollExchangeAsync(slot.Connection,
                    CommandCodes.BIND_SESSION_CODE,
                    LoginRegister.SerializeBindSession(route.Attachment, _consensusSession.BindSecret), deadline, token);
                var binding = LoginRegister.Deserialize(attached.Memory.Span);
                if (binding.Session != BinaryPrimitives.ReadUInt64LittleEndian(route.Attachment.AsSpan(ConsumerSessionEpochOffset)))
                {
                    throw VsrError.Exception(VsrError.INVALID_FORMAT, "BindSession reply carried a different session epoch.");
                }
                slot.Attachment = route.Attachment;
            }

            ValidatePollRoute(route, slot);
            polling = true;
            return await SendPollExchangeAsync(slot.Connection, code, payload, deadline, token, captured);
        }
        catch (IggyInvalidStatusCodeException error) when (error.StatusCode == VsrError.TRANSIENT_NOT_ACCEPTED)
        {
            slot.Attachment = null;
            throw;
        }
        catch (Exception error)
        {
            slot.Connection?.Dispose();
            slot.Connection = null;
            slot.Attachment = null;
            RemovePollRoute(key);
            // A poll that commits no offset changes nothing on the server, so it goes out again on a fresh route.
            var uncertain = polling && !VsrOperations.IsReplaySafeRead(code, false, payload.Span);
            if (IsPollConnectionFailure(error))
            {
                if (!uncertain)
                {
                    throw PollNotAccepted();
                }

                // This socket owns no coordinator lifecycle. Report the uncertain data outcome
                // without publishing a disconnect for the healthy membership connection.
                throw new VsrRequestOutcomeUnknownException(error);
            }

            if (polling && error is IggyInvalidStatusCodeException { StatusCode: VsrError.TRANSIENT_NOT_COMMITTED })
            {
                throw uncertain ? new VsrRequestOutcomeUnknownException(error) : PollNotAccepted();
            }

            throw;
        }
        finally
        {
            slot.Gate.Release();
        }
    }

    private void ValidatePollRoute(PollRoute route, PollConnectionSlot slot)
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

    // Called under _pollRoutingGate. Waiting exchanges retain the retired slot and
    // retry route acquisition; an exchange already holding the slot keeps its socket.
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

    private async Task<PollRoute> GetPollRouteAsync(PollRouteKey key, ReadOnlyMemory<byte> routing,
        long deadline, CancellationToken token)
    {
        lock (_pollRoutingGate)
        {
            if (_pollRoutes.TryGetValue(key, out var cached) && cached.Watermark >= _metadataWatermark
                && cached.Generation == _consensusSession.Generation)
            {
                return cached;
            }
        }

        using var response = await SendPollControlAsync(key.RoutingCode, routing, deadline, token);
        if (response.Memory.Length < ConsumerSessionSize + PartitionContext.ENCODED_SIZE)
        {
            throw new MalformedResponseException("Poll routing reply has a truncated consumer session.");
        }

        var attachment = response.Memory[..ConsumerSessionSize].ToArray();
        var context = BinaryMapper.MapPartitionContext(response.Memory.Span[ConsumerSessionSize..]);
        var position = ConsumerSessionSize + PartitionContext.ENCODED_SIZE;
        var primary = BinaryMapper.MapClusterNode(response.Memory.Span, ref position);
        if (position != response.Memory.Length)
        {
            throw new MalformedResponseException("Poll routing reply contains trailing bytes.");
        }

        if (primary.Endpoints.Tcp == 0)
        {
            throw new FeatureUnavailableException();
        }

        var generation = _consensusSession.Generation;
        var session = _consensusSession.Resolve(VsrOperation.NonReplicated);
        var clientId = BinaryPrimitives.ReadUInt128LittleEndian(attachment);
        var epoch = BinaryPrimitives.ReadUInt64LittleEndian(attachment.AsSpan(ConsumerSessionEpochOffset));
        if (clientId != session.ClientId || epoch != session.SessionId || epoch == 0)
        {
            throw PollNotAccepted();
        }

        lock (_pollRoutingGate)
        {
            if (generation != _consensusSession.Generation)
            {
                throw PollNotAccepted();
            }

            var watermark = Math.Max(_metadataWatermark,
                BinaryPrimitives.ReadUInt64LittleEndian(attachment.AsSpan(ConsumerSessionWatermarkOffset)));
            BinaryPrimitives.WriteUInt64LittleEndian(attachment.AsSpan(ConsumerSessionWatermarkOffset), watermark);
            var route = new PollRoute(ServerAddress.HostPort(primary.Ip, primary.Endpoints.Tcp), attachment,
                generation, watermark, context);
            if (_pollRoutes.Count >= MaxCachedPollRoutes)
            {
                _pollRoutes.Clear();
            }

            _pollRoutes[key] = route;
            return route;
        }
    }

    private async Task<IMemoryOwner<byte>> SendPollControlAsync(int code, ReadOnlyMemory<byte> payload,
        long deadline, CancellationToken token)
    {
        var attempt = await SendVsrAttemptAsync(code, payload, Environment.TickCount64, deadline, false, token);
        if (attempt.Error is not null && IsPollConnectionFailure(attempt.Error))
        {
            // Only a safe control request may recover the coordinator through its usual reconnect path.
            await DropVsrConnectionAsync(attempt.Connection);
            await PingAsync(token);
            attempt = await SendVsrAttemptAsync(code, payload, Environment.TickCount64, deadline, false, token);
        }

        if (attempt.Error is not null)
        {
            ExceptionDispatchInfo.Throw(attempt.Error);
        }

        return attempt.Response!;
    }

    private async Task<VsrConnection> ConnectPollConnectionAsync(PollRoute route, CancellationToken token)
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
        ReadOnlyMemory<byte> payload, long deadline, CancellationToken token, PartitionContext context = default)
    {
        var attempt = await connection.SendAttemptAsync(code, payload, deadline, deadline,
            HasSensitiveReply(code), token, retryTransient: false, context: context);
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
            _pollRoutes.Clear();
            foreach (var slot in _pollConnections.Values)
            {
                slot.Retired = true;
                slot.Connection?.Dispose();
            }

            _pollConnections.Clear();
        }
    }

    private void RemovePollRoute(PollRouteKey key)
    {
        lock (_pollRoutingGate)
        {
            _pollRoutes.Remove(key);
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
