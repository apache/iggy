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
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Mappers;
using Apache.Iggy.Utils;
using Apache.Iggy.Vsr;

namespace Apache.Iggy.IggyClient.Implementations;

public sealed partial class TcpMessageStream
{
    private const int MaxCachedPollRoutes = 4096;
    internal const int MAX_POLL_CONNECTIONS = 256;
    private const int PollRoutingRetryIntervalMs = 50;
    private const int ConsumerSessionSize = 32;
    private const int ConsumerSessionWatermarkOffset = 24;

    private readonly object _pollRoutingGate = new();
    private readonly Dictionary<PollRouteKey, PollRoute> _pollRoutes = [];
    private readonly Dictionary<string, PollConnectionSlot> _pollConnections = [];
    private ulong _metadataWatermark;
    private int _clusterNodeCount;
    private uint? _rememberedUserId;
    private AutoLoginSettings? _configuredLoginOverride;

    private readonly record struct PollRouteKey(Identifier StreamId, Identifier TopicId, ConsumerType ConsumerType,
        Identifier ConsumerId, uint? PartitionId);

    private sealed record PollRoute(string Endpoint, byte[] Attachment, ulong Generation, ulong Watermark);

    private sealed class PollConnectionSlot
    {
        internal readonly SemaphoreSlim Gate = new(1, 1);
        internal VsrConnection? Connection;
        internal byte[]? Attachment;
        internal volatile bool Retired;
    }

    private async Task<IMemoryOwner<byte>> PollAutoCommitAsync(PollRouteKey key, ReadOnlyMemory<byte> payload,
        CancellationToken token)
    {
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(token);
        cancellation.CancelAfter(VsrRequestTimeoutMs);
        var deadline = Environment.TickCount64 + VsrRequestTimeoutMs;
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

                    return Volatile.Read(ref _clusterNodeCount) > 1
                        ? await PollPrimaryOnceAsync(key, payload, deadline, cancellation.Token)
                        : await SendWithResponseAsync(CommandCodes.POLL_MESSAGES_CODE, payload,
                            token: cancellation.Token);
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

    private async Task<IMemoryOwner<byte>> PollPrimaryOnceAsync(PollRouteKey key, ReadOnlyMemory<byte> payload,
        long deadline, CancellationToken token)
    {
        var route = await GetPollRouteAsync(key, payload, deadline, token);
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
                var connection = await ConnectPollConnectionAsync(route.Endpoint, deadline, token);
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
                    CommandCodes.ATTACH_CONSUMER_SESSION_CODE, route.Attachment, deadline, token);
                slot.Attachment = route.Attachment;
            }

            ValidatePollRoute(route, slot);
            polling = true;
            return await SendPollExchangeAsync(slot.Connection, CommandCodes.POLL_MESSAGES_ON_PRIMARY_CODE,
                payload, deadline, token);
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
            if (IsPollConnectionFailure(error))
            {
                if (!polling)
                {
                    throw PollNotAccepted();
                }

                // This socket owns no coordinator lifecycle. Report the uncertain data outcome
                // without publishing a disconnect for the healthy membership connection.
                throw new VsrRequestOutcomeUnknownException(error);
            }

            if (polling && error is IggyInvalidStatusCodeException { StatusCode: VsrError.TRANSIENT_NOT_COMMITTED })
            {
                throw new VsrRequestOutcomeUnknownException(error);
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

    private async Task<PollRoute> GetPollRouteAsync(PollRouteKey key, ReadOnlyMemory<byte> payload,
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

        using var response = await SendPollControlAsync(CommandCodes.GET_POLL_ROUTING_CODE, payload, deadline, token);
        if (response.Memory.Length < ConsumerSessionSize)
        {
            throw new MalformedResponseException("Poll routing reply has a truncated consumer session.");
        }

        var attachment = response.Memory[..ConsumerSessionSize].ToArray();
        var position = ConsumerSessionSize;
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
        var epoch = BinaryPrimitives.ReadUInt64LittleEndian(attachment.AsSpan(16));
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
                generation, watermark);
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

    private async Task<VsrConnection> ConnectPollConnectionAsync(string endpoint, long deadline, CancellationToken token)
    {
        var credentials = _rememberedLogin ?? throw VsrError.Exception(VsrError.UNAUTHENTICATED,
            "Primary polling requires the credentials of the authenticated coordinator.");
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
            var session = new ConsensusSession();
            connection = new VsrConnection(stream, session, _configuration.MaxResponseFrameSize,
                VsrRequestTimeoutMs, dropped => dropped.Dispose(), _logger);
            var useToken = !string.IsNullOrEmpty(credentials.PersonalAccessToken);
            var body = useToken
                ? LoginRegister.SerializeWithPersonalAccessToken(credentials.PersonalAccessToken)
                : LoginRegister.Serialize(credentials.Username, credentials.Password);
            using var response = await SendPollExchangeAsync(connection,
                useToken ? CommandCodes.LOGIN_REGISTER_WITH_PAT_CODE : CommandCodes.LOGIN_REGISTER_CODE,
                body, deadline, token);
            session.Bind(LoginRegister.Deserialize(response.Memory.Span).Session);
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
        ReadOnlyMemory<byte> payload, long deadline, CancellationToken token)
    {
        var attempt = await connection.SendAttemptAsync(code, payload, deadline, deadline,
            HasSensitiveReply(code), token, retryTransient: false);
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

    private void RefreshPollCredentials(Identifier user, string? username, string? password)
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
