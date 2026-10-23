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

using Apache.Iggy.Configuration;
using Apache.Iggy.Encryption;
using Apache.Iggy.Enums;
using Apache.Iggy.Factory;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Tests.Integrations.Helpers;
using TUnit.Core.Interfaces;

namespace Apache.Iggy.Tests.Integrations.Fixtures;

/// <summary>
///     Runs the suite against an iggy-server <see cref="VsrCluster" />: a standalone node by default,
///     or a replicated cluster when IGGY_TEST_CLUSTER_NODES asks for one, so every test commits through
///     consensus. The SDK frames TCP with the VSR wire protocol.
/// </summary>
public class IggyServerFixture : IAsyncInitializer, IAsyncDisposable
{
    private readonly string _containerId = Guid.NewGuid().ToString();
    private readonly SemaphoreSlim _startGate = new(1, 1);

    private VsrCluster? _cluster;

    /// <summary>
    ///     Docker image to use. Can be overridden via IGGY_SERVER_DOCKER_IMAGE environment variable
    ///     or by subclasses. Defaults to the locally built <c>iggy-server:test</c>; build it with
    ///     <c>docker build -f core/server/Dockerfile -t iggy-server:test .</c> from the repository root.
    /// </summary>
    protected virtual string DockerImage =>
        Environment.GetEnvironmentVariable("IGGY_SERVER_DOCKER_IMAGE") ?? "iggy-server:test";

    /// <summary>
    ///     Names the containers and network of this fixture's cluster, so a `docker ps` during a run
    ///     shows which fixture owns which node.
    /// </summary>
    protected virtual string ClusterName => "general";

    /// <summary>
    ///     Enables iggy server trace logs.
    /// </summary>
    protected bool EnabledServerTraceLogs => true;

    /// <summary>
    ///     Server-side heartbeat interval. 1.2 intervals is the session timeout: a client that stops pinging is
    ///     evicted once idle past it and loses its session about one timeout later. 13 s is the shortest
    ///     whole-second interval whose timeout exceeds the cluster's 15 s heartbeat timeout, as the server
    ///     requires. Clients from <see cref="CreateClient" /> ping at half this interval; any client built by
    ///     hand must set <see cref="IggyClientConfigurator.HeartbeatInterval" /> below 1.2 intervals or be
    ///     evicted once idle past them.
    /// </summary>
    public static readonly TimeSpan ServerHeartbeatInterval = TimeSpan.FromSeconds(13);

    /// <summary>
    ///     How often every node reports its sessions to the metadata primary. The server accepts the short
    ///     heartbeat interval above only with reports this frequent.
    /// </summary>
    public static readonly TimeSpan SessionReportInterval = TimeSpan.FromMilliseconds(500);

    private static readonly TimeSpan StatsConvergenceTimeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan StatsPollInterval = TimeSpan.FromMilliseconds(100);

    /// <summary>Node-local counters catch up after partition commit broadcasts.</summary>
    public static async Task<T> ReadConvergedStatsAsync<T>(Func<Task<T>> read, Func<T, bool> converged)
    {
        var deadline = Environment.TickCount64 + (long)StatsConvergenceTimeout.TotalMilliseconds;
        var result = await read();
        while (!converged(result) && Environment.TickCount64 < deadline)
        {
            await Task.Delay(StatsPollInterval);
            result = await read();
        }
        return result;
    }

    /// <summary>
    ///     Server encryption key every node shares. With encryption enabled a stream created without its own key
    ///     gets one the server generates, so the stream options tests read a server sourced key.
    /// </summary>
    public static readonly string ServerEncryptionKey = "/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=";

    /// <summary>
    ///     Extra environment variables for every node, layered over the cluster's base configuration.
    ///     Heartbeat verification is on for every fixture so the eviction tests share the general server
    ///     instead of starting their own. Override in subclasses to customize.
    /// </summary>
    protected virtual Dictionary<string, string> EnvironmentVariables => new()
    {
        { "IGGY_HEARTBEAT_ENABLED", "true" },
        { "IGGY_HEARTBEAT_INTERVAL", $"{ServerHeartbeatInterval.TotalSeconds}s" },
        { "IGGY_CONSUMER_GROUP_HEARTBEAT_INTERVAL", $"{SessionReportInterval.TotalMilliseconds}ms" },
        { "IGGY_ENCRYPTION_ENABLED", "true" },
        { "IGGY_ENCRYPTION_KEY", ServerEncryptionKey }
    };

    /// <summary>
    ///     Resource mappings (certificates, etc.) mounted into every node. Override in subclasses.
    /// </summary>
    protected virtual ResourceMapping[] ResourceMappings => [];

    /// <summary>
    ///     Cluster size, mirroring the Rust integration harness knob of the same name: the
    ///     IGGY_TEST_CLUSTER_NODES environment variable decides (default 1, a standalone node with
    ///     clustering disabled; 2 or more, a replicated cluster). A subclass can pin its own size
    ///     instead, the way the redirection cluster stays three nodes whatever the knob says.
    /// </summary>
    protected virtual int NodeCount =>
        int.TryParse(Environment.GetEnvironmentVariable("IGGY_TEST_CLUSTER_NODES"), out var count) && count >= 1
            ? count
            : 1;

    public async ValueTask DisposeAsync()
    {
        if (_cluster != null)
        {
            await _cluster.DisposeAsync();
        }
    }

    /// <summary>
    ///     The cluster starts on first use, so a run that never dials the server does not pay for it.
    /// </summary>
    public Task InitializeAsync()
    {
        return Task.CompletedTask;
    }

    /// <summary>
    ///     Under VSR the register handshake is the login, so auto-login on top of the explicit one would register
    ///     twice on the same connection: the server answers the second one by replaying the binding it already
    ///     holds, and the client then carries a client id the server never bound, which consumer-group
    ///     membership is keyed by.
    /// </summary>
    public async Task<IIggyClient> CreateAuthenticatedClient(Protocol protocol, string userName = "iggy",
        string password = "iggy", IMessageEncryptor? encryptor = null)
    {
        var client = await CreateClient(protocol, protocol == Protocol.Http,
            encryptor: encryptor, userName: userName, password: password);
        await client.LoginUserAsync(userName, password);

        return client;
    }

    /// <summary>
    ///     A connected client that has not logged in, so the caller owns the handshake.
    /// </summary>
    public async Task<IIggyClient> CreateUnauthenticatedClient(Protocol protocol)
    {
        return await CreateClient(protocol);
    }

    /// <summary>
    ///     <paramref name="address" /> overrides the cluster address, so a test can dial the server through a
    ///     proxy while keeping the rest of the configuration identical.
    /// </summary>
    public async Task<IIggyClient> CreateClient(Protocol protocol, bool autoLogin = false, bool connect = true,
        IMessageEncryptor? encryptor = null, string? address = null, string userName = "iggy",
        string password = "iggy")
    {
        var client = IggyClientFactory.CreateClient(new IggyClientConfigurator
        {
            BaseAddress = address ?? await GetIggyAddressAsync(protocol),
            Protocol = protocol,
            ReconnectionSettings = new ReconnectionSettings { Enabled = true },
            HeartbeatInterval = ServerHeartbeatInterval / 2,
            AutoLoginSettings = new AutoLoginSettings
            {
                Enabled = autoLogin,
                Username = userName,
                Password = password
            },
            MessageEncryptor = encryptor
        });

        if (connect)
        {
            await client.ConnectAsync();
        }

        return client;
    }

    public async Task<string> GetIggyAddressAsync(Protocol protocol)
    {
        var cluster = await EnsureClusterStartedAsync();

        return protocol == Protocol.Tcp ? cluster.LeaderTcpAddress : cluster.LeaderHttpAddress;
    }

    /// <summary>
    ///     A backup node of the initial view, for tests that dial a follower and expect the client to be
    ///     redirected to the primary.
    /// </summary>
    public async Task<string> GetFollowerTcpAddressAsync()
    {
        var cluster = await EnsureClusterStartedAsync();

        return cluster.FollowerTcpAddress;
    }

    public static IEnumerable<Func<Protocol>> ProtocolData()
    {
        return [() => Protocol.Http, () => Protocol.Tcp];
    }

    private async Task<VsrCluster> EnsureClusterStartedAsync()
    {
        await _startGate.WaitAsync();
        try
        {
            if (_cluster == null)
            {
                var cluster = new VsrCluster(DockerImage, ClusterName, _containerId,
                    EnabledServerTraceLogs, NodeCount, EnvironmentVariables, ResourceMappings);
                try
                {
                    await cluster.StartAsync();
                }
                catch
                {
                    // A later retry rebuilds the cluster under the same name, so a half-started one
                    // must not leave its network or containers behind.
                    await cluster.DisposeAsync();
                    throw;
                }

                _cluster = cluster;
            }

            return _cluster;
        }
        finally
        {
            _startGate.Release();
        }
    }
}
