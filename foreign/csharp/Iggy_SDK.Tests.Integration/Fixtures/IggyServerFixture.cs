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
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using TUnit.Core.Interfaces;

namespace Apache.Iggy.Tests.Integrations.Fixtures;

public class IggyServerFixture : IAsyncInitializer, IAsyncDisposable
{
    /// <summary>
    ///     Server the whole suite runs against: <c>classic</c> (default) or <c>ng</c>. The two servers ship as
    ///     separate images, so the choice is a property of the run rather than of a test: CI runs the suite once
    ///     per value, and only the second run frames TCP with the VSR wire protocol.
    /// </summary>
    private const string ServerVariable = "IGGY_TEST_SERVER";

    private readonly string _containerId = Guid.NewGuid().ToString();

    private readonly IContainer _iggyContainer;
    private readonly HashSet<IContainer> _started = [];
    private readonly SemaphoreSlim _startGate = new(1, 1);

    private VsrCluster? _serverNgCluster;

    /// <summary>
    ///     Docker image to use. Can be overridden via IGGY_SERVER_DOCKER_IMAGE environment variable
    ///     or by subclasses. Defaults to the locally built <c>iggy-server:test</c>; build it with
    ///     <c>docker build -f Dockerfile -t iggy-server:test .</c> from the repository root, or point
    ///     IGGY_SERVER_DOCKER_IMAGE at a published image such as <c>apache/iggy:edge</c>.
    /// </summary>
    protected virtual string DockerImage =>
        Environment.GetEnvironmentVariable("IGGY_SERVER_DOCKER_IMAGE") ?? "iggy-server:test";

    /// <summary>
    ///     Image used when the run targets server-ng. Built from <c>core/server-ng/Dockerfile</c>. It comes up as
    ///     a <see cref="VsrCluster" />, so every test commits through real replication.
    /// </summary>
    protected virtual string ServerNgDockerImage =>
        Environment.GetEnvironmentVariable("IGGY_SERVER_NG_DOCKER_IMAGE") ?? "iggy-server-ng:test";

    /// <summary>The run targets iggy-server-ng, so both protocols dial the VSR cluster instead of the classic image.</summary>
    public static bool IsServerNg =>
        string.Equals(Environment.GetEnvironmentVariable(ServerVariable), "ng", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    ///     Environment variables for the container. Override in subclasses to customize.
    /// </summary>
    protected virtual Dictionary<string, string> EnvironmentVariables => new()
    {
        { "IGGY_ROOT_USERNAME", "iggy" },
        { "IGGY_ROOT_PASSWORD", "iggy" },
        { "IGGY_TCP_ADDRESS", "0.0.0.0:8090" },
        { "IGGY_HTTP_ADDRESS", "0.0.0.0:3000" },
        { "IGGY_SYSTEM_TOPIC_MESSAGE_EXPIRY", "10m" }
    };

    /// <summary>
    ///     Enables iggy server trace logs.
    /// </summary>
    protected bool EnabledServerTraceLogs => true;

    /// <summary>
    ///     Resource mappings (volumes, etc.) for the container. Override in subclasses to add custom mappings.
    /// </summary>
    protected virtual ResourceMapping[] ResourceMappings => [];

    /// <summary>
    ///     Directory for container log files. Set via IGGY_TEST_LOGS_DIR environment variable.
    ///     If not set, container logs will not be saved to file.
    /// </summary>
    private static string? LogDirectory =>
        Environment.GetEnvironmentVariable("IGGY_TEST_LOGS_DIR");

    public IggyServerFixture()
    {
        _iggyContainer = BuildContainer(DockerImage, _containerId);
    }

    public async ValueTask DisposeAsync()
    {
        await StopContainerAsync(_iggyContainer, "iggy-server");

        if (_serverNgCluster != null)
        {
            await _serverNgCluster.DisposeAsync();
        }
    }

    /// <summary>
    ///     Containers start on first use: a run scoped to one server must not pay for - or fail on - the image
    ///     the job never built.
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
        var client = await CreateClient(protocol, WireProtocolFor(protocol) == WireProtocol.Classic,
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
    ///     <paramref name="address" /> overrides the container address, so a test can dial the server through a
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
            WireProtocol = WireProtocolFor(protocol),
            ReconnectionSettings = new ReconnectionSettings { Enabled = true },
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

    /// <summary>VSR framing exists on TCP only, so the REST surface stays classic on either server.</summary>
    public static WireProtocol WireProtocolFor(Protocol protocol)
    {
        return IsServerNg && protocol == Protocol.Tcp ? WireProtocol.Vsr : WireProtocol.Classic;
    }

    public async Task<string> GetIggyAddressAsync(Protocol protocol)
    {
        if (IsServerNg)
        {
            var cluster = await EnsureServerNgClusterStartedAsync();

            return protocol == Protocol.Tcp ? cluster.LeaderTcpAddress : cluster.LeaderHttpAddress;
        }

        var container = await EnsureStartedAsync();

        return protocol == Protocol.Tcp
            ? $"127.0.0.1:{container.GetMappedPublicPort(8090)}"
            : $"http://127.0.0.1:{container.GetMappedPublicPort(3000)}";
    }

    /// <summary>
    ///     server-ng serves REST too, but its reads default to serializable consistency, so under the suite's
    ///     parallelism one can trail the commit it just made and a create-then-read fails. Restore the HTTP row
    ///     once that is settled.
    /// </summary>
    public static IEnumerable<Func<Protocol>> ProtocolData()
    {
        return IsServerNg ? [() => Protocol.Tcp] : [() => Protocol.Http, () => Protocol.Tcp];
    }

    protected virtual IContainer BuildContainer(string image, string name)
    {
        var builder = new ContainerBuilder(image)
            .WithPortBinding(3000, true)
            .WithPortBinding(8090, true)
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilInternalTcpPortIsAvailable(8090)
                .UntilHttpRequestIsSucceeded(request => request
                    .ForPort(3000)
                    .ForPath("/ping")))
            .WithName(name)
            .WithPrivileged(true)
            .WithCleanUp(true);

        foreach (var (key, value) in EnvironmentVariables)
        {
            builder = builder.WithEnvironment(key, value);
        }

        if (EnabledServerTraceLogs)
        {
            builder = builder
                .WithEnvironment("IGGY_SYSTEM_LOGGING_LEVEL", "trace")
                .WithEnvironment("RUST_LOG", "trace");
        }

        foreach (var mapping in ResourceMappings)
        {
            builder = builder.WithResourceMapping(mapping.Source, mapping.Destination);
        }

        return builder.Build();
    }

    private async Task<IContainer> EnsureStartedAsync()
    {
        await _startGate.WaitAsync();
        try
        {
            if (_started.Add(_iggyContainer))
            {
                await _iggyContainer.StartAsync();
            }

            return _iggyContainer;
        }
        finally
        {
            _startGate.Release();
        }
    }

    private async Task<VsrCluster> EnsureServerNgClusterStartedAsync()
    {
        await _startGate.WaitAsync();
        try
        {
            if (_serverNgCluster == null)
            {
                var cluster = new VsrCluster(ServerNgDockerImage, _containerId, EnabledServerTraceLogs);
                await cluster.StartAsync();
                _serverNgCluster = cluster;
            }

            return _serverNgCluster;
        }
        finally
        {
            _startGate.Release();
        }
    }

    private async Task StopContainerAsync(IContainer? container, string role)
    {
        if (container == null || !_started.Contains(container))
        {
            return;
        }

        await SaveContainerLogsAsync(container, role);
        await container.StopAsync();
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
            var logFilePath = Path.Combine(LogDirectory, $"{role}-{dotnetVersion}-{container.Name}.log");

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
