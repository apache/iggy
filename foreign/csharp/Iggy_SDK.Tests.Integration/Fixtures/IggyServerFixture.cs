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
using Microsoft.Extensions.Logging.Abstractions;
using TUnit.Core.Interfaces;

namespace Apache.Iggy.Tests.Integrations.Fixtures;

public class IggyServerFixture : IAsyncInitializer, IAsyncDisposable
{
    private readonly IContainer _httpContainer = new ContainerBuilder().WithImage("apache/iggy:edge")
        // Container name is just to be used locally for debbuging effects
        //.WithName($"SutIggyContainerHTTP")
        .WithPortBinding(3000, true)
        .WithPortBinding(8090, true)
        .WithOutputConsumer(Consume.RedirectStdoutAndStderrToConsole())
        .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(3000))
        .WithName($"HTTP_{Guid.NewGuid()}")
        //.WithEnvironment("IGGY_SYSTEM_LOGGING_LEVEL", "trace")
        //.WithEnvironment("RUST_LOG", "trace")
        .WithCleanUp(true)
        .Build();

    private readonly IContainer _tcpContainer = new ContainerBuilder().WithImage("apache/iggy:edge")
        // Container name is just to be used locally for debbuging effects
        //.WithName($"SutIggyContainerTCP")
        .WithPortBinding(3000, true)
        .WithPortBinding(8090, true)
        .WithOutputConsumer(Consume.RedirectStdoutAndStderrToConsole())
        .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(8090))
        .WithName($"TCP_{Guid.NewGuid()}")
        //.WithEnvironment("IGGY_SYSTEM_LOGGING_LEVEL", "trace")
        //.WithEnvironment("RUST_LOG", "trace")
        .WithCleanUp(true)
        .Build();

    public Dictionary<Protocol, IIggyClient> Clients { get; } = new();

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
        await Task.WhenAll(_tcpContainer.StopAsync(), _httpContainer.StopAsync());
    }

    public virtual async Task InitializeAsync()
    {
        await Task.WhenAll(_tcpContainer.StartAsync(), _httpContainer.StartAsync());

        await CreateTcpClient();
        await CreateHttpClient();
    }

    public async Task CreateTcpClient()
    {
        var tcpClient = CreateClient(Protocol.Tcp);

        await tcpClient.LoginUser("iggy", "iggy");

        Clients[Protocol.Tcp] = tcpClient;
    }

    public async Task CreateHttpClient()
    {
        var client = CreateClient(Protocol.Http);

        await client.LoginUser("iggy", "iggy");

        Clients[Protocol.Http] = client;
    }

    public IIggyClient CreateClient(Protocol protocol, Protocol? targetContainer = null)
    {
        var port = protocol == Protocol.Tcp
            ? _tcpContainer.GetMappedPublicPort(8090)
            : _httpContainer.GetMappedPublicPort(3000);

        if (targetContainer != null
            && targetContainer != protocol)
        {
            port = targetContainer == Protocol.Tcp
                ? _tcpContainer.GetMappedPublicPort(3000)
                : _httpContainer.GetMappedPublicPort(8090);
        }

        var address = protocol == Protocol.Tcp
            ? $"127.0.0.1:{port}"
            : $"http://127.0.0.1:{port}";

        return MessageStreamFactory.CreateMessageStream(options =>
        {
            options.BaseAdress = address;
            options.Protocol = protocol;
            options.MessageBatchingSettings = BatchingSettings;
            options.MessagePollingSettings = PollingSettings;
        }, NullLoggerFactory.Instance);
    }

    public static IEnumerable<Func<Protocol>> ProtocolData()
    {
        yield return () => Protocol.Http;
        yield return () => Protocol.Tcp;
    }
}