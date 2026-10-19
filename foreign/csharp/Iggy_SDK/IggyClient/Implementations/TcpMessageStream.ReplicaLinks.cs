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

using System.Buffers.Binary;
using System.Net.Sockets;
using System.Security.Authentication;
using Apache.Iggy.Contracts;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Mappers;
using Apache.Iggy.Utils;
using Apache.Iggy.Vsr;
using Microsoft.Extensions.Logging;

namespace Apache.Iggy.IggyClient.Implementations;

public sealed partial class TcpMessageStream
{
    private const int MaxReplicaLinks = 256;
    private const int MaxConcurrentReplicaDials = 4;
    private const int MaxReconnectBackoffMs = 10_000;
    private readonly Dictionary<byte, ReplicaSlot> _replicaLinks = [];
    private readonly SemaphoreSlim _replicaDials = new(MaxConcurrentReplicaDials, MaxConcurrentReplicaDials);
    private byte? _metadataReplica;
    private TaskCompletionSource _replicaChanged = new(TaskCreationOptions.RunContinuationsAsynchronously);

    private sealed class ReplicaSlot(byte replicaId, string endpoint, ulong generation)
    {
        internal readonly byte ReplicaId = replicaId;
        internal readonly string Endpoint = endpoint;
        internal readonly ulong Generation = generation;
        internal readonly CancellationTokenSource Lifetime = new();
        internal VsrConnection? Connection;
        internal Task? Heartbeat;
        internal Exception? Failure;
        internal bool Attached;
        internal bool RetriesExhausted;
    }

    private void InstallReplicaLinks(ClusterMetadata metadata)
    {
        var generation = _consensusSession.Generation;
        lock (_pollRoutingGate)
        {
            _metadataReplica = metadata.Nodes.FirstOrDefault(node => node.Role == ClusterNodeRole.Leader
                && node.Status == ClusterNodeStatus.Healthy)?.ReplicaId;
            var nodes = metadata.Nodes.Where(node => node.Endpoints.Tcp is > 0).ToArray();
            // A single node advertises its own view of its address, such as a bind address or a container name,
            // which a port mapping or a port-forward hides. Only the configured endpoint is known to reach it.
            var single = nodes.Length == 1;
            var endpoints = nodes.ToDictionary(node => node.ReplicaId, node => single
                ? _configuration.BaseAddress : ServerAddress.HostPort(node.Ip, node.Endpoints.Tcp));
            if (endpoints.Count > MaxReplicaLinks)
            {
                throw VsrError.Exception(VsrError.INVALID_FORMAT, "Replica roster exceeds the protocol limit.");
            }
            foreach (var slot in _replicaLinks.Values.ToArray())
            {
                if (!endpoints.TryGetValue(slot.ReplicaId, out var endpoint) || endpoint != slot.Endpoint
                    || generation != slot.Generation)
                {
                    _replicaLinks.Remove(slot.ReplicaId);
                    slot.Lifetime.Cancel();
                    slot.Connection?.Dispose();
                }
            }
            foreach (var (replicaId, endpoint) in endpoints)
            {
                if (_replicaLinks.ContainsKey(replicaId))
                {
                    continue;
                }
                var slot = new ReplicaSlot(replicaId, endpoint, generation);
                // With one node in the roster, the bootstrap reaches that node, because no other one serves TCP.
                if (_connection is { IsReady: true } bootstrap
                    && (single || ServerAddress.IsSame(endpoint, _connectedAddress)))
                {
                    slot.Connection = bootstrap;
                    slot.Attached = true;
                }
                _replicaLinks.Add(replicaId, slot);
                _ = Task.Run(() => RunReplicaLinkAsync(slot));
            }
            if (_metadataReplica is { } primary && endpoints.TryGetValue(primary, out var metadataEndpoint))
            {
                _currentAddress = metadataEndpoint;
            }
            NotifyReplicaChange();
        }
    }

    private async Task RunReplicaLinkAsync(ReplicaSlot slot)
    {
        var token = slot.Lifetime.Token;
        var attempts = 0;
        var first = true;
        var exhausted = false;
        try
        {
            while (!token.IsCancellationRequested && !_disposed && slot.Generation == _consensusSession.Generation)
            {
                if (slot.Connection is { } existing)
                {
                    await existing.Closed.WaitAsync(token);
                    if (IsSessionFailure(existing.Failure))
                    {
                        ScheduleSessionRecovery(slot.Generation);
                        return;
                    }
                    lock (_pollRoutingGate)
                    {
                        slot.Connection = null;
                    }
                    first = false;
                }
                if (!first)
                {
                    if (!_configuration.ReconnectionSettings.Enabled
                        || _configuration.ReconnectionSettings.MaxRetries > 0
                        && attempts >= _configuration.ReconnectionSettings.MaxRetries)
                    {
                        exhausted = true;
                        return;
                    }
                    await Task.Delay(ReconnectDelay(attempts), token);
                    attempts++;
                }
                first = false;
                VsrConnection? connection = null;
                try
                {
                    await _replicaDials.WaitAsync(token);
                    try
                    {
                        connection = await DialReplicaAsync(slot.Endpoint, token);
                        var snapshot = _consensusSession.Resolve(VsrOperation.NonReplicated);
                        var identity = new byte[ConsumerSessionSize];
                        BinaryPrimitives.WriteUInt128LittleEndian(identity, snapshot.ClientId);
                        BinaryPrimitives.WriteUInt64LittleEndian(identity.AsSpan(ConsumerSessionEpochOffset), snapshot.SessionId);
                        lock (_pollRoutingGate)
                        {
                            BinaryPrimitives.WriteUInt64LittleEndian(identity.AsSpan(ConsumerSessionWatermarkOffset), _metadataWatermark);
                        }
                        var deadline = Environment.TickCount64 + VsrRequestTimeoutMs;
                        var binding = await connection.SendAttemptAsync(CommandCodes.BIND_SESSION_CODE,
                            LoginRegister.SerializeBindSession(identity, _consensusSession.BindSecret), deadline, deadline,
                            true, token, retryTransient: false);
                        if (binding.Error is { } error)
                        {
                            throw error;
                        }
                        using var response = binding.Response!;
                        if (LoginRegister.Deserialize(response.Memory.Span).Session != snapshot.SessionId)
                        {
                            throw VsrError.Exception(VsrError.SESSION_MISMATCH, "BindSession changed the session epoch.");
                        }
                    }
                    finally
                    {
                        _replicaDials.Release();
                    }
                    if (slot.Attached && _configuration.ReconnectionSettings.WaitAfterReconnect > TimeSpan.Zero)
                    {
                        await Task.Delay(_configuration.ReconnectionSettings.WaitAfterReconnect, token);
                    }
                    lock (_pollRoutingGate)
                    {
                        if (slot.Generation != _consensusSession.Generation || token.IsCancellationRequested || _disposed)
                        {
                            connection.Dispose();
                            return;
                        }
                        slot.Connection = connection;
                        slot.Attached = true;
                        attempts = 0;
                        if (_metadataReplica == slot.ReplicaId && _connection is { } bootstrap
                            && !_replicaLinks.Values.Any(candidate => ReferenceEquals(candidate.Connection, bootstrap)))
                        {
                            _connection = null;
                            bootstrap.Dispose();
                        }
                        NotifyReplicaChange();
                    }
                    connection = null;
                }
                catch (Exception error) when (!token.IsCancellationRequested && !_disposed
                    && error is not VsrSessionEvictedException
                    && (VsrConnection.IsConnectionException(error) || error is AuthenticationException or IggyInvalidStatusCodeException
                    { StatusCode: VsrError.TRANSIENT_NOT_COMMITTED or VsrError.TRANSIENT_NOT_ACCEPTED or VsrError.NOT_PRIMARY }
                    or IggyInvalidStatusCodeException { FromServer: false, StatusCode: VsrError.INVALID_FORMAT }))
                {
                    connection?.Dispose();
                }
                catch (Exception error)
                {
                    connection?.Dispose();
                    // The bind is what decides, so a StaleClient refusal of the bind ends the session.
                    if (IsSessionFailure(error, binding: true)
                        || error is IggyInvalidStatusCodeException { FromServer: true, StatusCode: VsrError.STALE_CLIENT })
                    {
                        ScheduleSessionRecovery(slot.Generation);
                    }
                    lock (_pollRoutingGate)
                    {
                        slot.Failure = CallerError(error);
                    }
                    return;
                }
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
        finally
        {
            lock (_pollRoutingGate)
            {
                slot.Connection?.Dispose();
                slot.Connection = null;
                // Marked only here, so a restart cannot start before this cleanup and lose its new connection.
                if (exhausted)
                {
                    slot.Failure = _configuration.ReconnectionSettings.Enabled
                        ? new IOException("Replica reconnection attempts exhausted.") : new NotConnectedException();
                    slot.RetriesExhausted = true;
                }
                NotifyReplicaChange();
            }
        }
    }

    private TimeSpan ReconnectDelay(int attempts)
    {
        var settings = _configuration.ReconnectionSettings;
        var initial = Math.Max(1, settings.InitialDelay.TotalMilliseconds);
        var maximum = Math.Min(MaxReconnectBackoffMs, Math.Max(initial, settings.MaxDelay.TotalMilliseconds));
        var multiplier = settings.UseExponentialBackoff ? Math.Max(1, settings.BackoffMultiplier) : 1;
        var upper = Math.Min(maximum, initial * Math.Pow(multiplier, Math.Min(attempts, 16)));
        return TimeSpan.FromMilliseconds(Random.Shared.NextDouble() * upper);
    }

    private async Task<VsrConnection> DialReplicaAsync(string endpoint, CancellationToken token)
    {
        if (!ServerAddress.TryParse(endpoint, out var host, out var port))
        {
            throw new InvalidBaseAddressException();
        }
        var generation = _consensusSession.Generation;
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(token, _clientLifetime.Token);
        cancellation.CancelAfter(FailoverDialTimeout);
        Socket? socket = new(ServerAddress.AddressFamilyOf(host), SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
        try
        {
            if (_configuration.SendBufferSize is { } sendBufferSize)
            {
                socket.SendBufferSize = sendBufferSize;
            }
            if (_configuration.ReceiveBufferSize is { } receiveBufferSize)
            {
                socket.ReceiveBufferSize = receiveBufferSize;
            }
            await socket.ConnectAsync(host, port, cancellation.Token);
            var stream = _configuration.TlsSettings.Enabled
                ? await CreateSslStreamAndAuthenticate(socket, _configuration.TlsSettings, cancellation.Token)
                : new NetworkStream(socket, true);
            socket = null;
            return new VsrConnection(stream, _consensusSession, _configuration.MaxResponseFrameSize,
                VsrRequestTimeoutMs, connection => NotifyReplicaDrop(connection, generation), _logger, commit => ObserveMetadataCommit(commit, generation));
        }
        catch (OperationCanceledException error) when (!token.IsCancellationRequested && !_disposed)
        {
            throw new IOException("Replica connection establishment timed out.", error);
        }
        finally
        {
            socket?.Dispose();
        }
    }

    private async Task<VsrConnection?> FindReadyReplicaAsync(byte replica, CancellationToken token)
    {
        while (true)
        {
            Task changed;
            lock (_pollRoutingGate)
            {
                ObjectDisposedException.ThrowIf(_disposed, this);
                if (!_replicaLinks.TryGetValue(replica, out var slot))
                {
                    return null;
                }
                if (slot.Connection is { IsReady: true } ready)
                {
                    return ready;
                }
                if (slot.Failure is { } failure)
                {
                    if (!IsFinalLinkFailure(failure))
                    {
                        return null;
                    }
                    throw failure;
                }
                if (slot.Attached)
                {
                    return null;
                }
                changed = _replicaChanged.Task;
            }
            await changed.WaitAsync(token);
        }
    }

    /// <summary>
    ///     The ready replica links. When none is ready and every link has a final failure, it throws one of those
    ///     failures, because no wait can bring a link back. With reconnection off and no link left, as after a
    ///     session failure, it throws NotConnectedException for the same reason.
    /// </summary>
    private VsrConnection[] ReadyReplicaLinksOrThrow()
    {
        lock (_pollRoutingGate)
        {
            var links = ReadyReplicaLinks();
            if (links.Length == 0 && _replicaLinks.Count == 0 && !_configuration.ReconnectionSettings.Enabled)
            {
                throw new NotConnectedException();
            }
            if (links.Length == 0 && _replicaLinks.Values.FirstOrDefault()?.Failure is { } failure
                && _replicaLinks.Values.All(slot => slot.Failure is { } other && IsFinalLinkFailure(other)))
            {
                throw failure;
            }
            return links;
        }
    }

    /// <summary>
    ///     Whether a link failure is final. A lost connection is not while reconnection is on, because the link
    ///     reconnects or the heartbeat restarts it.
    /// </summary>
    private bool IsFinalLinkFailure(Exception failure) =>
        !VsrConnection.IsConnectionException(failure) || !_configuration.ReconnectionSettings.Enabled;

    private VsrConnection[] ReadyReplicaLinks()
    {
        lock (_pollRoutingGate)
        {
            var ready = _replicaLinks.Values.Select(slot => slot.Connection).OfType<VsrConnection>()
                .Where(connection => connection.IsReady).ToList();
            if (_connection is { IsReady: true } bootstrap && !ready.Contains(bootstrap))
            {
                ready.Add(bootstrap);
            }
            return ready.ToArray();
        }
    }

    /// <summary>
    ///     Restarts each link that used up its reconnection retries, with a fresh budget. A request never dials,
    ///     so without this the budget would limit the life of the client instead of one reconnect burst. Logout
    ///     ends the session and Dispose marks the client disposed before either stops the links, so a restart
    ///     never undoes them.
    /// </summary>
    private void RestartExhaustedReplicaLinks()
    {
        lock (_pollRoutingGate)
        {
            if (_disposed || !_consensusSession.IsBound || !_configuration.ReconnectionSettings.Enabled)
            {
                return;
            }
            foreach (var slot in _replicaLinks.Values.Where(slot => slot.RetriesExhausted
                && slot.Generation == _consensusSession.Generation))
            {
                slot.RetriesExhausted = false;
                slot.Failure = null;
                _ = Task.Run(() => RunReplicaLinkAsync(slot));
            }
        }
    }

    private void PingReadyReplicas()
    {
        lock (_pollRoutingGate)
        {
            foreach (var slot in _replicaLinks.Values)
            {
                if (slot.Connection is { IsReady: true } connection && slot.Heartbeat is null or { IsCompleted: true })
                {
                    slot.Heartbeat = PingReplicaAsync(connection, slot.Lifetime.Token);
                }
            }
        }
    }

    private async Task PingReplicaAsync(VsrConnection connection, CancellationToken token)
    {
        var deadline = Environment.TickCount64 + (long)Math.Min(VsrRequestTimeoutMs,
            _configuration.HeartbeatInterval.TotalMilliseconds);
        var attempt = await connection.SendAttemptAsync(CommandCodes.PING_CODE, ReadOnlyMemory<byte>.Empty,
            deadline, deadline, false, token, retryTransient: false);
        attempt.Response?.Dispose();
        if (attempt.Error is not null && !token.IsCancellationRequested)
        {
            _logger.LogWarning(attempt.Error, "Replica heartbeat failed");
        }
    }

    private void StopReplicaLinks()
    {
        lock (_pollRoutingGate)
        {
            foreach (var slot in _replicaLinks.Values)
            {
                slot.Lifetime.Cancel();
                slot.Connection?.Dispose();
            }
            _replicaLinks.Clear();
            NotifyReplicaChange();
        }
    }

    private void NotifyReplicaDrop(VsrConnection connection, ulong generation)
    {
        lock (_pollRoutingGate)
        {
            if (IsSessionFailure(connection.Failure))
            {
                ScheduleSessionRecovery(generation);
            }
            NotifyReplicaChange();
        }
    }

    private void NotifyReplicaChange()
    {
        var changed = _replicaChanged;
        _replicaChanged = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        changed.TrySetResult();
        if (_disposed || !_consensusSession.IsBound)
        {
            return;
        }
        if (ReadyReplicaLinks().Length > 0)
        {
            SetConnectionState(ConnectionState.Authenticated);
        }
        else if (!_configuration.ReconnectionSettings.Enabled || _replicaLinks.Values.All(slot => slot.Failure is not null))
        {
            SetConnectionState(ConnectionState.Disconnected);
        }
    }

    /// <summary>
    ///     Whether a verdict ends the session. A StaleClient refusal before commit does not: it fails only the link
    ///     that carried it, and the bind on a new link decides. A committed StaleClient result comes from the
    ///     replicated session epoch, so it does.
    /// </summary>
    private static bool IsSessionFailure(Exception? error, bool binding = false) =>
        (error is VsrSessionEvictedException && (binding || !VsrConnection.IsDetachedSession(error)))
        || error is IggyInvalidStatusCodeException
        {
            FromServer: true,
            StatusCode: VsrError.UNAUTHENTICATED or VsrError.SESSION_MISMATCH
        }
        or IggyInvalidStatusCodeException { FromServer: true, Committed: true, StatusCode: VsrError.STALE_CLIENT };

    /// <summary>
    ///     The error a caller sees for a failed attempt. The eviction type is internal, so an eviction surfaces as
    ///     the verdict it carries. Apply it only where the error leaves for a caller: the session failure and
    ///     unknown outcome checks need the eviction itself.
    /// </summary>
    private static Exception CallerError(Exception error) =>
        error is VsrSessionEvictedException evicted ? evicted.Verdict : error;

    private void ScheduleSessionRecovery(ulong generation)
    {
        lock (_pollRoutingGate)
        {
            if (_disposed || !_consensusSession.IsBound || generation != _consensusSession.Generation
                || _sessionRecovery is { IsCompleted: false })
            {
                return;
            }
            var settings = SignInSettings();
            _sessionRecoveryCancellation?.Dispose();
            var cancellation = CancellationTokenSource.CreateLinkedTokenSource(_clientLifetime.Token);
            _sessionRecoveryCancellation = cancellation;
            _sessionRecovery = Task.Run(() => RecoverSessionAsync(generation, settings, cancellation.Token));
        }
    }

    private async Task RecoverSessionAsync(ulong generation, Apache.Iggy.Configuration.AutoLoginSettings? settings,
        CancellationToken token)
    {
        var acquired = false;
        try
        {
            await _connectGate.WaitAsync(token);
            acquired = true;
            if (generation != _consensusSession.Generation)
            {
                return;
            }
            ResetConsensusSession();
            SetConnectionState(ConnectionState.Disconnected);
            if (!_configuration.ReconnectionSettings.Enabled || settings is null)
            {
                return;
            }
            await TryEstablishConnectionAsync(false, token);
            await AutoLoginAsync(settings, token);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
        catch (Exception error)
        {
            SetConnectionState(ConnectionState.Disconnected);
            _logger.LogWarning(error, "Consensus session recovery failed");
        }
        finally
        {
            if (acquired)
            {
                _connectGate.Release();
            }
        }
    }

    private void CancelSessionRecovery()
    {
        lock (_pollRoutingGate)
        {
            _sessionRecoveryCancellation?.Cancel();
        }
    }
}
