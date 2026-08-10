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
using Apache.Iggy.Tests.Integrations.Helpers;
using Docker.DotNet;
using Docker.DotNet.Models;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;

namespace Apache.Iggy.Tests.Integrations.Fixtures;

/// <summary>
///     The iggy-server deployment a run scoped to that server uses. A single node runs with
///     clustering disabled, exercising the wire protocol without replication; two or more nodes form
///     a real roster that puts every test through consensus.
/// </summary>
internal sealed class VsrCluster : IAsyncDisposable
{
    private const string ClusterName = "test-vsr-cluster";

    // TCP and HTTP get real host ports on every node: a view change can make any replica the
    // primary clients are redirected to, so every advertised 127.0.0.1:port must be dialable from
    // the host. QUIC/WebSocket/replica listeners are reachable only on the cluster network and get
    // base + node. The offset is not for the network (each node has its own IP there) but because
    // the server rejects a roster where two advertised endpoints collide.
    private const ushort InternalQuicPortBase = 8200;
    private const ushort InternalWebSocketPortBase = 8300;
    private const ushort InternalReplicaPortBase = 8400;

    // Host-port pool, partitioned per .NET major so the parallel `dotnet test` processes
    // (net8.0 -> 29800..29949, net10.0 -> 30000..30149) never race each other for a port.
    private static readonly ushort BasePort = (ushort)(29000 + Environment.Version.Major * 100);
    private static readonly ushort EndPort = (ushort)(BasePort + 150);

    // A reserved port is released before its container binds it, so another cluster in this process
    // could scan onto it in that window. Ports stay claimed for the process lifetime to close it.
    private static readonly HashSet<ushort> ClaimedPorts = [];

    private readonly IContainer?[] _containers;
    private readonly IReadOnlyDictionary<string, string>? _extraEnvironment;
    private readonly string _idSuffix;
    private readonly string _image;
    private readonly string _name;
    private INetwork? _network;
    private readonly int _nodeCount;
    private readonly NodePorts[] _ports;
    private readonly IReadOnlyList<ResourceMapping>? _resourceMappings;
    private readonly bool _traceLogs;

    /// <summary>Replica 0 - primary of the initial view, and the node the tests talk to.</summary>
    public string LeaderTcpAddress => $"127.0.0.1:{_ports[0].Tcp}";

    /// <summary>The same node's REST surface, which serves classic framing rather than VSR.</summary>
    public string LeaderHttpAddress => $"http://127.0.0.1:{_ports[0].Http}";

    /// <summary>A backup of the initial view, so a client dialing it gets redirected to the primary.</summary>
    public string FollowerTcpAddress => ClusterEnabled
        ? $"127.0.0.1:{_ports[1].Tcp}"
        : throw new InvalidOperationException(
            "A follower address requires a cluster; set IGGY_TEST_CLUSTER_NODES to 2 or more.");

    /// <summary>The same backup node's REST surface.</summary>
    public string FollowerHttpAddress => ClusterEnabled
        ? $"http://127.0.0.1:{_ports[1].Http}"
        : throw new InvalidOperationException(
            "A follower address requires a cluster; set IGGY_TEST_CLUSTER_NODES to 2 or more.");

    private bool ClusterEnabled => _nodeCount > 1;

    private static string? LogDirectory =>
        Environment.GetEnvironmentVariable("IGGY_TEST_LOGS_DIR");

    /// <summary>
    ///     <paramref name="nodeCount" /> of 1 starts a standalone node with clustering disabled, higher
    ///     values a cluster with that many replicas. <paramref name="name" /> labels the containers and
    ///     network after the owning fixture. <paramref name="extraEnvironment" /> wins over the
    ///     base configuration, and <paramref name="resourceMappings" /> are mounted into every node.
    /// </summary>
    public VsrCluster(string image, string name, string idSuffix, bool traceLogs, int nodeCount,
        IReadOnlyDictionary<string, string>? extraEnvironment = null,
        IReadOnlyList<ResourceMapping>? resourceMappings = null)
    {
        _image = image;
        _name = name;
        _idSuffix = idSuffix;
        _traceLogs = traceLogs;
        _extraEnvironment = extraEnvironment;
        _resourceMappings = resourceMappings;
        _nodeCount = nodeCount;
        _ports = ReservePorts(_nodeCount);
        _containers = new IContainer[_nodeCount];
    }

    public async ValueTask DisposeAsync()
    {
        for (var node = 0; node < _nodeCount; node++)
        {
            if (_containers[node] == null)
            {
                continue;
            }

            try
            {
                await SaveContainerLogsAsync(_containers[node]!, $"iggy-server-{node}");
            }
            catch (Exception e)
            {
                Console.WriteLine($"Failed to save the logs of iggy-server-{node}: {e}");
            }
        }

        foreach (var container in _containers)
        {
            if (container == null)
            {
                continue;
            }

            try
            {
                await container.DisposeAsync();
            }
            catch (Exception e)
            {
                Console.WriteLine($"Failed to dispose an iggy-server container: {e}");
            }
        }

        if (_network != null)
        {
            try
            {
                await _network.DeleteAsync();
            }
            catch (Exception e)
            {
                Console.WriteLine($"Failed to delete the iggy-server network: {e}");
            }
        }
    }

    /// <summary>
    ///     The containers are built here rather than in the constructor because a roster needs every
    ///     node's IP before any node starts, and those IPs come from the subnet the network is
    ///     created with.
    /// </summary>
    public async Task StartAsync()
    {
        var nodeAddresses = Array.Empty<string>();
        if (ClusterEnabled)
        {
            var subnetPrefix = await CreateNetworkAsync();
            // The gateway takes .1, so node addresses start at .10.
            nodeAddresses = Enumerable.Range(0, _nodeCount)
                .Select(node => $"{subnetPrefix}.{10 + node}")
                .ToArray();
        }

        Dictionary<string, string> clusterEnvironment = BuildClusterEnvironment(nodeAddresses);
        for (var node = 0; node < _nodeCount; node++)
        {
            _containers[node] = BuildNodeContainer(node, nodeAddresses, clusterEnvironment);
        }

        // A multi-node roster needs a quorum of replicas, so nothing commits until enough nodes are up.
        await Task.WhenAll(_containers.Select(container => container!.StartAsync()));
    }

    /// <summary>
    ///     Roster peers dial each other by literal IP - the ip field is parsed, not resolved - so every
    ///     node gets a static address, and Docker only honors static addresses on networks created with
    ///     an explicit subnet. The subnet is picked from a private /16 here; a candidate overlapping
    ///     another network on the host fails the create and the next one is tried.
    /// </summary>
    private async Task<string> CreateNetworkAsync()
    {
        for (var candidate = 0; candidate < 256; candidate++)
        {
            var subnetPrefix = $"10.213.{candidate}";
            var network = new NetworkBuilder()
                .WithName($"iggy-vsr-{_name}-{_idSuffix}")
                .WithCreateParameterModifier(parameters => parameters.IPAM = new IPAM
                {
                    Config = [new IPAMConfig { Subnet = $"{subnetPrefix}.0/24" }]
                })
                .Build();

            try
            {
                await network.CreateAsync();
                _network = network;
                return subnetPrefix;
            }
            catch (DockerApiException)
            {
                // The subnet overlaps a network already on the host.
            }
        }

        throw new InvalidOperationException("No free /24 subnet in 10.213.0.0/16 for the cluster network.");
    }

    private IContainer BuildNodeContainer(int node, IReadOnlyList<string> nodeAddresses,
        IReadOnlyDictionary<string, string> clusterEnvironment)
    {
        var ports = _ports[node];
        var builder = new ContainerBuilder(_image)
            .WithName($"iggy-vsr-{_name}-{node}-{_idSuffix}")
            .WithEnvironment("IGGY_ROOT_USERNAME", "iggy")
            .WithEnvironment("IGGY_ROOT_PASSWORD", "iggy")
            .WithEnvironment("IGGY_SYSTEM_TOPIC_MESSAGE_EXPIRY", "10m")
            .WithEnvironment("IGGY_SYSTEM_PATH", $"local_data_vsr_{node}")
            .WithEnvironment("IGGY_TCP_ADDRESS", $"0.0.0.0:{ports.Tcp}")
            .WithEnvironment("IGGY_HTTP_ADDRESS", $"0.0.0.0:{ports.Http}")
            .WithEnvironment("IGGY_QUIC_ADDRESS", $"0.0.0.0:{ports.Quic}")
            .WithEnvironment("IGGY_WEBSOCKET_ADDRESS", $"0.0.0.0:{ports.WebSocket}")
            .WithEnvironment(clusterEnvironment)
            .WithPrivileged(true)
            .WithCleanUp(true)
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilInternalTcpPortIsAvailable(ports.Tcp)
                .UntilInternalTcpPortIsAvailable(ports.Http));

        // Host bindings mirror the container port so the loopback address advertised in cluster
        // metadata resolves to the node that advertised it. Every node needs one: view changes
        // can make any replica the primary clients get redirected to.
        builder = builder
            .WithPortBinding(ports.Tcp.ToString(), ports.Tcp.ToString())
            .WithPortBinding(ports.Http.ToString(), ports.Http.ToString());

        if (ClusterEnabled)
        {
            var address = nodeAddresses[node];
            builder = builder
                .WithCommand("--replica-id", node.ToString())
                .WithNetwork(_network)
                .WithNetworkAliases($"vsr-node-{node}")
                .WithCreateParameterModifier(parameters =>
                    AssignStaticAddress(parameters, _network!.Name, address));
        }

        if (_traceLogs)
        {
            builder = builder
                .WithEnvironment("IGGY_SYSTEM_LOGGING_LEVEL", "trace")
                .WithEnvironment("RUST_LOG", "trace");
        }

        if (_extraEnvironment != null)
        {
            foreach (var (key, value) in _extraEnvironment)
            {
                builder = builder.WithEnvironment(key, value);
            }
        }

        if (_resourceMappings != null)
        {
            foreach (var mapping in _resourceMappings)
            {
                builder = builder.WithResourceMapping(mapping.Source, mapping.Destination);
            }
        }

        return builder.Build();
    }

    private Dictionary<string, string> BuildClusterEnvironment(IReadOnlyList<string> nodeAddresses)
    {
        var environment = new Dictionary<string, string>
        {
            ["IGGY_CLUSTER_ENABLED"] = ClusterEnabled ? "true" : "false"
        };

        if (!ClusterEnabled)
        {
            return environment;
        }

        environment["IGGY_CLUSTER_NAME"] = ClusterName;
        environment["IGGY_MESSAGE_BUS_RECONNECT_PERIOD"] = "100ms";

        for (var node = 0; node < _nodeCount; node++)
        {
            var ports = _ports[node];
            environment[$"IGGY_CLUSTER_NODES_{node}_NAME"] = $"vsr-node-{node}";
            environment[$"IGGY_CLUSTER_NODES_{node}_IP"] = nodeAddresses[node];
            environment[$"IGGY_CLUSTER_NODES_{node}_ADVERTISED_ADDRESS"] = "127.0.0.1";
            environment[$"IGGY_CLUSTER_NODES_{node}_REPLICA_ID"] = node.ToString();
            environment[$"IGGY_CLUSTER_NODES_{node}_PORTS_TCP"] = ports.Tcp.ToString();
            environment[$"IGGY_CLUSTER_NODES_{node}_PORTS_HTTP"] = ports.Http.ToString();
            environment[$"IGGY_CLUSTER_NODES_{node}_PORTS_QUIC"] = ports.Quic.ToString();
            environment[$"IGGY_CLUSTER_NODES_{node}_PORTS_WEBSOCKET"] = ports.WebSocket.ToString();
            environment[$"IGGY_CLUSTER_NODES_{node}_PORTS_TCP_REPLICA"] = ports.Replica.ToString();
        }

        return environment;
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

    /// <summary>
    ///     Finds free host ports for the host-dialed endpoints by briefly binding them, so concurrent
    ///     clusters cannot pick the same port. The listeners are released before the containers start.
    /// </summary>
    private static NodePorts[] ReservePorts(int nodeCount)
    {
        var reservations = new List<TcpListener>();
        try
        {
            var ports = new NodePorts[nodeCount];
            for (var node = 0; node < nodeCount; node++)
            {
                ports[node] = new NodePorts(ReservePort(reservations),
                    ReservePort(reservations),
                    (ushort)(InternalQuicPortBase + node),
                    (ushort)(InternalWebSocketPortBase + node),
                    (ushort)(InternalReplicaPortBase + node));
            }

            return ports;
        }
        finally
        {
            foreach (var listener in reservations)
            {
                listener.Stop();
            }
        }
    }

    private static ushort ReservePort(List<TcpListener> reservations)
    {
        lock (ClaimedPorts)
        {
            for (var candidate = BasePort; candidate < EndPort; candidate++)
            {
                if (ClaimedPorts.Contains(candidate))
                {
                    continue;
                }

                try
                {
                    var listener = new TcpListener(IPAddress.Loopback, candidate);
                    listener.Start();
                    reservations.Add(listener);
                    ClaimedPorts.Add(candidate);
                    return candidate;
                }
                catch (SocketException)
                {
                    // Held by something else on the host.
                }
            }
        }

        throw new InvalidOperationException(
            $"No free ports available in [{BasePort}, {EndPort}) for .NET {Environment.Version.Major}.x.");
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

    /// <summary>
    ///     The ports one node listens on. The leader's and first follower's TCP/HTTP are host ports
    ///     mirrored into the container; the rest are the fixed cluster-network ports.
    /// </summary>
    private readonly record struct NodePorts(ushort Tcp, ushort Http, ushort Quic, ushort WebSocket, ushort Replica);
}
