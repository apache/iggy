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

using System.Net;
using System.Net.Sockets;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using TUnit.Core.Interfaces;

namespace Apache.Iggy.Tests.Integrations.Fixtures;

public class IggyClusterFixture : IAsyncInitializer, IAsyncDisposable
{
    private const string LeaderAlias = "iggy-leader";
    private const string FollowerAlias = "iggy-follower";

    // TcpListeners held open until just before the containers are started so that the
    // OS keeps the chosen ports reserved across the whole fixture setup. Parallel test
    // hosts (net8.0 + net10.0) would otherwise race on the gap between picking the port
    // and docker binding it.
    private readonly List<TcpListener> _portReservations = [];
    private readonly IContainer _followerContainer;
    private readonly ushort _followerHttpPort;
    private readonly ushort _followerQuicPort;

    private readonly ushort _followerTcpPort;
    private readonly ushort _followerWsPort;
    private readonly IContainer _leaderContainer;
    private readonly ushort _leaderHttpPort;
    private readonly ushort _leaderQuicPort;

    private readonly ushort _leaderTcpPort;
    private readonly ushort _leaderWsPort;

    private readonly INetwork _network;

    private string DockerImage =>
        Environment.GetEnvironmentVariable("IGGY_SERVER_DOCKER_IMAGE") ?? "apache/iggy:edge";

    private static string? LogDirectory =>
        Environment.GetEnvironmentVariable("IGGY_TEST_LOGS_DIR");

    public IggyClusterFixture()
    {
        _leaderTcpPort = ReservePort();
        _leaderHttpPort = ReservePort();
        _leaderQuicPort = ReservePort();
        _leaderWsPort = ReservePort();
        _followerTcpPort = ReservePort();
        _followerHttpPort = ReservePort();
        _followerQuicPort = ReservePort();
        _followerWsPort = ReservePort();

        _network = new NetworkBuilder()
            .WithName($"iggy-cluster-{Guid.NewGuid():N}")
            .Build();

        _leaderContainer = new ContainerBuilder(DockerImage)
            .WithName($"iggy-leader-{Guid.NewGuid():N}")
            .WithNetwork(_network)
            .WithNetworkAliases(LeaderAlias)
            .WithPortBinding(_leaderTcpPort.ToString(), _leaderTcpPort.ToString())
            .WithPortBinding(_leaderHttpPort.ToString(), _leaderHttpPort.ToString())
            .WithEnvironment("RUST_LOG", "trace")
            .WithEnvironment("IGGY_SYSTEM_LOGGING_LEVEL", "trace")
            .WithEnvironment("IGGY_ROOT_USERNAME", "iggy")
            .WithEnvironment("IGGY_ROOT_PASSWORD", "iggy")
            .WithEnvironment("IGGY_SYSTEM_PATH", "local_data_leader")
            .WithEnvironment("IGGY_TCP_ADDRESS", $"0.0.0.0:{_leaderTcpPort}")
            .WithEnvironment("IGGY_HTTP_ADDRESS", $"0.0.0.0:{_leaderHttpPort}")
            .WithEnvironment("IGGY_QUIC_ADDRESS", $"0.0.0.0:{_leaderQuicPort}")
            .WithEnvironment("IGGY_WEBSOCKET_ADDRESS", $"0.0.0.0:{_leaderWsPort}")
            .WithEnvironment("IGGY_CLUSTER_ENABLED", "true")
            .WithEnvironment("IGGY_CLUSTER_NAME", "test-cluster")
            .WithEnvironment("IGGY_CLUSTER_NODE_CURRENT_NAME", "leader-node")
            .WithEnvironment("IGGY_CLUSTER_NODE_CURRENT_IP", "127.0.0.1")
            .WithEnvironment("IGGY_CLUSTER_NODE_OTHERS_0_NAME", "follower-node")
            .WithEnvironment("IGGY_CLUSTER_NODE_OTHERS_0_IP", "127.0.0.1")
            .WithEnvironment("IGGY_CLUSTER_NODE_OTHERS_0_PORTS_TCP", _followerTcpPort.ToString())
            .WithEnvironment("IGGY_CLUSTER_NODE_OTHERS_0_PORTS_HTTP", _followerHttpPort.ToString())
            .WithEnvironment("IGGY_CLUSTER_NODE_OTHERS_0_PORTS_QUIC", _followerQuicPort.ToString())
            .WithEnvironment("IGGY_CLUSTER_NODE_OTHERS_0_PORTS_WEBSOCKET", _followerWsPort.ToString())
            .WithPrivileged(true)
            .WithCleanUp(true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(_leaderTcpPort))
            .Build();

        _followerContainer = new ContainerBuilder(DockerImage)
            .WithName($"iggy-follower-{Guid.NewGuid():N}")
            .WithCommand("--follower")
            .WithNetwork(_network)
            .WithNetworkAliases(FollowerAlias)
            .WithPortBinding(_followerTcpPort.ToString(), _followerTcpPort.ToString())
            .WithPortBinding(_followerHttpPort.ToString(), _followerHttpPort.ToString())
            .WithEnvironment("RUST_LOG", "trace")
            .WithEnvironment("IGGY_SYSTEM_LOGGING_LEVEL", "trace")
            .WithEnvironment("IGGY_ROOT_USERNAME", "iggy")
            .WithEnvironment("IGGY_ROOT_PASSWORD", "iggy")
            .WithEnvironment("IGGY_SYSTEM_PATH", "local_data_follower")
            .WithEnvironment("IGGY_TCP_ADDRESS", $"0.0.0.0:{_followerTcpPort}")
            .WithEnvironment("IGGY_HTTP_ADDRESS", $"0.0.0.0:{_followerHttpPort}")
            .WithEnvironment("IGGY_QUIC_ADDRESS", $"0.0.0.0:{_followerQuicPort}")
            .WithEnvironment("IGGY_WEBSOCKET_ADDRESS", $"0.0.0.0:{_followerWsPort}")
            .WithEnvironment("IGGY_CLUSTER_ENABLED", "true")
            .WithEnvironment("IGGY_CLUSTER_NAME", "test-cluster")
            .WithEnvironment("IGGY_CLUSTER_NODE_CURRENT_NAME", "follower-node")
            .WithEnvironment("IGGY_CLUSTER_NODE_CURRENT_IP", "127.0.0.1")
            .WithEnvironment("IGGY_CLUSTER_NODE_OTHERS_0_NAME", "leader-node")
            .WithEnvironment("IGGY_CLUSTER_NODE_OTHERS_0_IP", "127.0.0.1")
            .WithEnvironment("IGGY_CLUSTER_NODE_OTHERS_0_PORTS_TCP", _leaderTcpPort.ToString())
            .WithEnvironment("IGGY_CLUSTER_NODE_OTHERS_0_PORTS_HTTP", _leaderHttpPort.ToString())
            .WithEnvironment("IGGY_CLUSTER_NODE_OTHERS_0_PORTS_QUIC", _leaderQuicPort.ToString())
            .WithEnvironment("IGGY_CLUSTER_NODE_OTHERS_0_PORTS_WEBSOCKET", _leaderWsPort.ToString())
            .WithPrivileged(true)
            .WithCleanUp(true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(_followerTcpPort))
            .Build();
    }

    public async ValueTask DisposeAsync()
    {
        ReleaseReservedPorts();
        await SaveContainerLogsAsync(_leaderContainer, "leader");
        await SaveContainerLogsAsync(_followerContainer, "follower");
        await _followerContainer.StopAsync();
        await _leaderContainer.StopAsync();
        await _network.DeleteAsync();
    }

    public async Task InitializeAsync()
    {
        await _network.CreateAsync();
        // Release the reservations at the last possible moment so the window between
        // giving the port back to the OS and docker re-binding it is as small as we can
        // make it.
        ReleaseReservedPorts();
        await Task.WhenAll(_leaderContainer.StartAsync(), _followerContainer.StartAsync());
    }

    public string GetLeaderAddress()
    {
        return $"127.0.0.1:{_leaderTcpPort}";
    }

    public string GetFollowerAddress()
    {
        return $"127.0.0.1:{_followerTcpPort}";
    }

    private ushort ReservePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        _portReservations.Add(listener);
        return (ushort)((IPEndPoint)listener.LocalEndpoint).Port;
    }

    private void ReleaseReservedPorts()
    {
        foreach (var listener in _portReservations)
        {
            listener.Stop();
        }

        _portReservations.Clear();
    }

    private static async Task SaveContainerLogsAsync(IContainer container, string role)
    {
        if (string.IsNullOrEmpty(LogDirectory))
        {
            return;
        }

        try
        {
            Directory.CreateDirectory(LogDirectory);
            var dotnetVersion = $"net{Environment.Version.Major}.{Environment.Version.Minor}";
            var logFilePath = Path.Combine(LogDirectory, $"iggy-{role}-{dotnetVersion}-{container.Name}.log");

            var (stdout, stderr) = await container.GetLogsAsync();

            await using var writer = new StreamWriter(logFilePath);
            if (!string.IsNullOrEmpty(stdout))
            {
                await writer.WriteLineAsync("=== STDOUT ===");
                await writer.WriteLineAsync(stdout);
            }

            if (!string.IsNullOrEmpty(stderr))
            {
                await writer.WriteLineAsync("=== STDERR ===");
                await writer.WriteLineAsync(stderr);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to save {role} container logs: {ex.Message}");
        }
    }
}
