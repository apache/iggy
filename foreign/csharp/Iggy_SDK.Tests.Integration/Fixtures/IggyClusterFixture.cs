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

using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using TUnit.Core.Interfaces;

namespace Apache.Iggy.Tests.Integrations.Fixtures;

public class IggyClusterFixture : IAsyncInitializer, IAsyncDisposable
{
    private const string LeaderAlias = "iggy-leader";
    private const string FollowerAlias = "iggy-follower";

    private static readonly Random Random = new();
    private static readonly HashSet<ushort> UsedPorts = [];
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
        _leaderTcpPort = GetRandomPort();
        _leaderHttpPort = GetRandomPort();
        _leaderQuicPort = GetRandomPort();
        _leaderWsPort = GetRandomPort();
        _followerTcpPort = GetRandomPort();
        _followerHttpPort = GetRandomPort();
        _followerQuicPort = GetRandomPort();
        _followerWsPort = GetRandomPort();

        _network = new NetworkBuilder()
            .WithName($"iggy-cluster-{Guid.NewGuid():N}")
            .Build();

        // Cluster.nodes roster env vars are byte-identical on both
        // containers; only the bind addresses and the --replica-id CLI arg
        // differ per node.
        var clusterRosterEnv = new Dictionary<string, string>
        {
            ["IGGY_CLUSTER_ENABLED"] = "true",
            ["IGGY_CLUSTER_NAME"] = "test-cluster",
            ["IGGY_CLUSTER_NODES_0_NAME"] = "leader-node",
            ["IGGY_CLUSTER_NODES_0_IP"] = "127.0.0.1",
            ["IGGY_CLUSTER_NODES_0_REPLICA_ID"] = "0",
            ["IGGY_CLUSTER_NODES_0_PORTS_TCP"] = _leaderTcpPort.ToString(),
            ["IGGY_CLUSTER_NODES_0_PORTS_QUIC"] = _leaderQuicPort.ToString(),
            ["IGGY_CLUSTER_NODES_0_PORTS_HTTP"] = _leaderHttpPort.ToString(),
            ["IGGY_CLUSTER_NODES_0_PORTS_WEBSOCKET"] = _leaderWsPort.ToString(),
            ["IGGY_CLUSTER_NODES_1_NAME"] = "follower-node",
            ["IGGY_CLUSTER_NODES_1_IP"] = "127.0.0.1",
            ["IGGY_CLUSTER_NODES_1_REPLICA_ID"] = "1",
            ["IGGY_CLUSTER_NODES_1_PORTS_TCP"] = _followerTcpPort.ToString(),
            ["IGGY_CLUSTER_NODES_1_PORTS_QUIC"] = _followerQuicPort.ToString(),
            ["IGGY_CLUSTER_NODES_1_PORTS_HTTP"] = _followerHttpPort.ToString(),
            ["IGGY_CLUSTER_NODES_1_PORTS_WEBSOCKET"] = _followerWsPort.ToString(),
        };

        _leaderContainer = new ContainerBuilder(DockerImage)
            .WithName($"iggy-leader-{Guid.NewGuid():N}")
            .WithCommand("--replica-id", "0")
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
            .WithEnvironment(clusterRosterEnv)
            .WithPrivileged(true)
            .WithCleanUp(true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(_leaderTcpPort))
            .Build();

        _followerContainer = new ContainerBuilder(DockerImage)
            .WithName($"iggy-follower-{Guid.NewGuid():N}")
            .WithCommand("--follower", "--replica-id", "1")
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
            .WithEnvironment(clusterRosterEnv)
            .WithPrivileged(true)
            .WithCleanUp(true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(_followerTcpPort))
            .Build();
    }

    public async ValueTask DisposeAsync()
    {
        await SaveContainerLogsAsync(_leaderContainer, "leader");
        await SaveContainerLogsAsync(_followerContainer, "follower");
        await _followerContainer.StopAsync();
        await _leaderContainer.StopAsync();
        await _network.DeleteAsync();
    }

    public async Task InitializeAsync()
    {
        await _network.CreateAsync();
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

    private static ushort GetRandomPort()
    {
        lock (UsedPorts)
        {
            ushort port;
            do
            {
                port = (ushort)Random.Next(30000, 40000);
            } while (!UsedPorts.Add(port));

            return port;
        }
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
