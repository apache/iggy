// // Licensed to the Apache Software Foundation (ASF) under one
// // or more contributor license agreements.  See the NOTICE file
// // distributed with this work for additional information
// // regarding copyright ownership.  The ASF licenses this file
// // to you under the Apache License, Version 2.0 (the
// // "License"); you may not use this file except in compliance
// // with the License.  You may obtain a copy of the License at
// //
// //   http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing,
// // software distributed under the License is distributed on an
// // "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// // KIND, either express or implied.  See the License for the
// // specific language governing permissions and limitations
// // under the License.

using Apache.Iggy.Configuration;
using Apache.Iggy.Enums;
using Apache.Iggy.Factory;
using Apache.Iggy.IggyClient;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using TUnit.Core.Interfaces;
using TUnit.Core.Logging;

namespace Apache.Iggy.Tests.Integrations.Fixtures;

public class IggyServerFixture : IAsyncInitializer, IAsyncDisposable
{
    private readonly IContainer _iggyContainer = new ContainerBuilder().WithImage("apache/iggy:0.6.0-edge.2")
        .WithPortBinding(3000, true)
        .WithPortBinding(8090, true)
        .WithOutputConsumer(Consume.RedirectStdoutAndStderrToConsole())
        .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(8090))
        .WithName($"{Guid.NewGuid()}")
        .WithEnvironment("IGGY_ROOT_USERNAME", "iggy")
        .WithEnvironment("IGGY_ROOT_PASSWORD", "iggy")
        .WithEnvironment("IGGY_TCP_ADDRESS", "0.0.0.0:8090")
        .WithEnvironment("IGGY_HTTP_ADDRESS", "0.0.0.0:3000")
        //.WithEnvironment("IGGY_CLUSTER_ENABLED", "true")
        // .WithEnvironment("IGGY_CLUSTER_NODES_0_NAME", "iggy-node-1")
        // .WithEnvironment("IGGY_CLUSTER_NODES_0_ADDRESS", "127.0.0.1:8090")
        // .WithEnvironment("IGGY_CLUSTER_NODES_1_NAME", "iggy-node-2")
        // .WithEnvironment("IGGY_CLUSTER_NODES_1_ADDRESS", "127.0.0.1:8092")
        // .WithEnvironment("IGGY_CLUSTER_ENABLED", "true")
        // .WithEnvironment("IGGY_CLUSTER_ENABLED", "true")
        // .WithEnvironment("IGGY_CLUSTER_ENABLED", "true")
        //.WithEnvironment("IGGY_SYSTEM_LOGGING_LEVEL", "trace")
        //.WithEnvironment("RUST_LOG", "trace")
        .WithPrivileged(true)
        .WithCleanUp(true)
        .Build();

    private string? _iggyServerHost;

    private Action<MessageBatchingSettings> BatchingSettings { get; } = options =>
    {
        options.Enabled = false;
        options.Interval = TimeSpan.FromMilliseconds(100);
        options.MaxMessagesPerBatch = 1000;
        options.MaxRequests = 4096;
    };

    private Action<MessagePollingSettings> PollingSettings { get; } = options =>
    {
        options.Interval = TimeSpan.FromMilliseconds(100);
        options.StoreOffsetStrategy = StoreOffset.WhenMessagesAreReceived;
    };

    public async ValueTask DisposeAsync()
    {
        await _iggyContainer.StopAsync();
    }

    public virtual async Task InitializeAsync()
    {
        var logger = TestContext.Current!.GetDefaultLogger();
        _iggyServerHost = Environment.GetEnvironmentVariable("IGGY_SERVER_HOST");

        await logger.LogInformationAsync($"Iggy server host: {_iggyServerHost}");
        if (string.IsNullOrEmpty(_iggyServerHost))
        {
            await _iggyContainer.StartAsync();
        }

        await CreateTcpClient();
        await CreateHttpClient();
    }

    public async Task<Dictionary<Protocol, IIggyClient>> CreateClients()
    {
        var dictionary = new Dictionary<Protocol, IIggyClient>();
        dictionary[Protocol.Tcp] = await CreateTcpClient();
        dictionary[Protocol.Http] = await CreateHttpClient();

        return dictionary;
    }

    public async Task<IIggyClient> CreateTcpClient(string userName = "iggy", string password = "iggy", bool connect = true)
    {
        var client = await CreateClient(Protocol.Tcp, connect: connect);

        if (connect)
        {
            await client.LoginUser(userName, password);
        }

        return client;
    }

    public async Task<IIggyClient> CreateHttpClient(string userName = "iggy", string password = "iggy")
    {
        var client = await CreateClient(Protocol.Http);

        await client.LoginUser(userName, password);

        return client;
    }

    public async Task<IIggyClient> CreateClient(Protocol protocol, Protocol? targetContainer = null, bool connect = true)
    {
        var address = GetIggyAddress(protocol);

        var client = IggyClientFactory.CreateClient(new IggyClientConfigurator()
        {
            BaseAddress = address,
            Protocol = protocol,
            ReconnectionSettings = new ReconnectionSettings()
            {
                Enabled = true
            },
            AutoLoginSettings = new AutoLoginSettings()
            {
                Enabled = true,
                Username = "iggy",
                Password = "iggy"
            }
        });

        if (connect)
        {
            await client.ConnectAsync();
        }

        return client;
    }

    public string GetIggyAddress(Protocol protocol)
    {
        if (string.IsNullOrEmpty(_iggyServerHost))
        {
            var port = protocol == Protocol.Tcp
                ? _iggyContainer.GetMappedPublicPort(8090)
                : _iggyContainer.GetMappedPublicPort(3000);

            return protocol == Protocol.Tcp
                ? $"127.0.0.1:{port}"
                : $"http://127.0.0.1:{port}";
        }

        return protocol == Protocol.Tcp
            ? $"{_iggyServerHost}:8090"
            : $"http://{_iggyServerHost}:3000";

    }

    public static IEnumerable<Func<Protocol>> ProtocolData()
    {
        yield return () => Protocol.Http;
        yield return () => Protocol.Tcp;
    }
}
