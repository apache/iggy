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
using Docker.DotNet.Models;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;

namespace Apache.Iggy.Tests.Integrations.Fixtures;

/// <summary>
///     The iggy-server-ng cluster a run scoped to that server uses. A single node commits with a quorum of
///     one, so it would exercise the wire protocol without ever replicating; a real roster puts every test
///     through consensus instead.
/// </summary>
internal sealed class VsrCluster : IAsyncDisposable
{
    private const int NodeCount = 3;
    private const string ClusterName = "test-vsr-cluster";

    // Host-port pool, partitioned per .NET major the same way IggyClusterFixture partitions its own, and
    // disjoint from it so both fixtures can be live in one process:
    //   net8.0  - 29800..29899
    //   net10.0 - 30000..30099
    // Ten ports per cluster (five transports x two nodes) fit the range with room for a third node.
    private const ushort PortRangeSize = 100;

    // Roster peers dial each other by literal IP - the ip field is parsed, not resolved - so the containers
    // need addresses known before they start. A per-TFM subnet keeps the two `dotnet test` processes off
    // each other's network.
    private static readonly string Subnet = $"172.30.{Environment.Version.Major}.0/24";
    private static readonly string Gateway = $"172.30.{Environment.Version.Major}.1";
    private static readonly ushort BasePort = (ushort)(29000 + Environment.Version.Major * 100);
    private static readonly ushort EndPort = (ushort)(BasePort + PortRangeSize);
    private readonly IContainer[] _containers = new IContainer[NodeCount];
    private readonly ushort[] _httpPorts = new ushort[NodeCount];
    private readonly INetwork _network;

    private readonly List<TcpListener> _portReservations = [];
    private readonly ushort[] _tcpPorts = new ushort[NodeCount];

    private static string? LogDirectory =>
        Environment.GetEnvironmentVariable("IGGY_TEST_LOGS_DIR");

    /// <summary>Replica 0 - primary of the initial view, and the node the tests talk to.</summary>
    public string LeaderTcpAddress => $"127.0.0.1:{_tcpPorts[0]}";

    /// <summary>The same node's REST surface, which serves classic framing rather than VSR.</summary>
    public string LeaderHttpAddress => $"http://127.0.0.1:{_httpPorts[0]}";

    public VsrCluster(string image, string idSuffix, bool traceLogs)
    {
        var quicPorts = new ushort[NodeCount];
        var websocketPorts = new ushort[NodeCount];
        var replicaPorts = new ushort[NodeCount];

        try
        {
            for (var node = 0; node < NodeCount; node++)
            {
                _tcpPorts[node] = ReservePort();
                _httpPorts[node] = ReservePort();
                quicPorts[node] = ReservePort();
                websocketPorts[node] = ReservePort();
                replicaPorts[node] = ReservePort();
            }
        }
        finally
        {
            ReleaseReservedPorts();
        }

        var networkName = $"iggy-vsr-{idSuffix}";
        _network = new NetworkBuilder()
            .WithName(networkName)
            .WithCreateParameterModifier(parameters => parameters.IPAM = new IPAM
            {
                Config =
                [
                    new IPAMConfig
                    {
                        Subnet = Subnet,
                        Gateway = Gateway
                    }
                ]
            })
            .Build();

        var roster = new Dictionary<string, string>
        {
            ["IGGY_CLUSTER_ENABLED"] = "true",
            ["IGGY_CLUSTER_NAME"] = ClusterName,
            ["IGGY_MESSAGE_BUS_RECONNECT_PERIOD"] = "100ms"
        };

        for (var node = 0; node < NodeCount; node++)
        {
            roster[$"IGGY_CLUSTER_NODES_{node}_NAME"] = $"vsr-node-{node}";
            roster[$"IGGY_CLUSTER_NODES_{node}_IP"] = NodeAddress(node);
            roster[$"IGGY_CLUSTER_NODES_{node}_ADVERTISED_ADDRESS"] = "127.0.0.1";
            roster[$"IGGY_CLUSTER_NODES_{node}_REPLICA_ID"] = node.ToString();
            roster[$"IGGY_CLUSTER_NODES_{node}_PORTS_TCP"] = _tcpPorts[node].ToString();
            roster[$"IGGY_CLUSTER_NODES_{node}_PORTS_HTTP"] = _httpPorts[node].ToString();
            roster[$"IGGY_CLUSTER_NODES_{node}_PORTS_QUIC"] = quicPorts[node].ToString();
            roster[$"IGGY_CLUSTER_NODES_{node}_PORTS_WEBSOCKET"] = websocketPorts[node].ToString();
            roster[$"IGGY_CLUSTER_NODES_{node}_PORTS_TCP_REPLICA"] = replicaPorts[node].ToString();
        }

        for (var node = 0; node < NodeCount; node++)
        {
            var address = NodeAddress(node);
            var builder = new ContainerBuilder(image)
                .WithName($"iggy-vsr-{node}-{idSuffix}")
                .WithCommand("--replica-id", node.ToString())
                .WithNetwork(_network)
                .WithNetworkAliases($"vsr-node-{node}")
                .WithCreateParameterModifier(parameters => AssignStaticAddress(parameters, networkName, address))
                // Host binding mirrors the container port so the loopback address advertised in cluster
                // metadata resolves to the node that advertised it.
                .WithPortBinding(_tcpPorts[node].ToString(), _tcpPorts[node].ToString())
                .WithPortBinding(_httpPorts[node].ToString(), _httpPorts[node].ToString())
                .WithEnvironment("IGGY_ROOT_USERNAME", "iggy")
                .WithEnvironment("IGGY_ROOT_PASSWORD", "iggy")
                .WithEnvironment("IGGY_SYSTEM_TOPIC_MESSAGE_EXPIRY", "10m")
                .WithEnvironment("IGGY_SYSTEM_PATH", $"local_data_vsr_{node}")
                .WithEnvironment("IGGY_TCP_ADDRESS", $"0.0.0.0:{_tcpPorts[node]}")
                .WithEnvironment("IGGY_HTTP_ADDRESS", $"0.0.0.0:{_httpPorts[node]}")
                .WithEnvironment("IGGY_QUIC_ADDRESS", $"0.0.0.0:{quicPorts[node]}")
                .WithEnvironment("IGGY_WEBSOCKET_ADDRESS", $"0.0.0.0:{websocketPorts[node]}")
                .WithEnvironment(roster)
                .WithPrivileged(true)
                .WithCleanUp(true)
                .WithWaitStrategy(Wait.ForUnixContainer()
                    .UntilInternalTcpPortIsAvailable(_tcpPorts[node])
                    .UntilInternalTcpPortIsAvailable(_httpPorts[node]));

            if (traceLogs)
            {
                builder = builder
                    .WithEnvironment("IGGY_SYSTEM_LOGGING_LEVEL", "trace")
                    .WithEnvironment("RUST_LOG", "trace");
            }

            _containers[node] = builder.Build();
        }
    }

    public async ValueTask DisposeAsync()
    {
        for (var node = 0; node < NodeCount; node++)
        {
            try
            {
                await SaveContainerLogsAsync(_containers[node], $"iggy-server-ng-{node}");
            }
            catch (Exception e)
            {
                Console.WriteLine($"Failed to save the logs of iggy-server-ng-{node}: {e}");
            }
        }

        foreach (var container in _containers)
        {
            try
            {
                await container.DisposeAsync();
            }
            catch (Exception e)
            {
                Console.WriteLine($"Failed to dispose an iggy-server-ng container: {e}");
            }
        }

        try
        {
            await _network.DeleteAsync();
        }
        catch (Exception e)
        {
            Console.WriteLine($"Failed to delete the iggy-server-ng network: {e}");
        }
    }

    public async Task StartAsync()
    {
        await _network.CreateAsync();
        // A two-node roster needs both replicas for a quorum, so nothing commits until the pair is up.
        await Task.WhenAll(_containers.Select(container => container.StartAsync()));
    }

    /// <summary>
    ///     Pins the container's address on the cluster network. Testcontainers has no first-class knob for it,
    ///     and the roster needs the address before any container starts.
    /// </summary>
    private static void AssignStaticAddress(CreateContainerParameters parameters, string networkName,
        string address)
    {
        parameters.NetworkingConfig ??= new NetworkingConfig();
        parameters.NetworkingConfig.EndpointsConfig ??= new Dictionary<string, EndpointSettings>();

        if (!parameters.NetworkingConfig.EndpointsConfig.TryGetValue(networkName, out var endpoint))
        {
            endpoint = new EndpointSettings();
            parameters.NetworkingConfig.EndpointsConfig[networkName] = endpoint;
        }

        endpoint.IPAMConfig = new EndpointIPAMConfig { IPv4Address = address };
    }

    private static string NodeAddress(int node)
    {
        return $"172.30.{Environment.Version.Major}.{10 + node}";
    }

    private ushort ReservePort()
    {
        for (var candidate = BasePort; candidate < EndPort; candidate++)
        {
            try
            {
                var listener = new TcpListener(IPAddress.Loopback, candidate);
                listener.Start();
                _portReservations.Add(listener);
                return candidate;
            }
            catch (SocketException)
            {
                // Held by an earlier ReservePort() in this cluster, or by something else on the host.
            }
        }

        throw new InvalidOperationException(
            $"No free ports available in [{BasePort}, {EndPort}) for .NET {Environment.Version.Major}.x.");
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
            // Docker hands back names with a leading slash, which Path.Combine would read as a directory.
            var containerName = container.Name.TrimStart('/');
            var logFilePath = Path.Combine(LogDirectory, $"{role}-{dotnetVersion}-{containerName}.log");

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
