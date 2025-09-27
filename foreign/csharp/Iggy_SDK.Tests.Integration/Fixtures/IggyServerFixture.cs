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
using TUnit.Core.Logging;

namespace Apache.Iggy.Tests.Integrations.Fixtures;

public class IggyServerFixture : IAsyncInitializer, IAsyncDisposable
{
    private readonly IContainer _iggyContainer = new ContainerBuilder().WithImage("apache/iggy:edge")
        .WithPortBinding(3000, true)
        .WithPortBinding(8090, true)
        .WithOutputConsumer(Consume.RedirectStdoutAndStderrToConsole())
        .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(8090))
        .WithName($"{Guid.NewGuid()}")
        //.WithEnvironment("IGGY_SYSTEM_LOGGING_LEVEL", "trace")
        //.WithEnvironment("RUST_LOG", "trace")
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

    public async Task<IIggyClient> CreateTcpClient(string userName = "iggy", string password = "iggy")
    {
        var client = CreateClient(Protocol.Tcp);

        await client.LoginUser(userName, password);
        ;

        return client;
    }

    public async Task<IIggyClient> CreateHttpClient(string userName = "iggy", string password = "iggy")
    {
        var client = CreateClient(Protocol.Http);

        await client.LoginUser(userName, password);

        return client;
    }

    public IIggyClient CreateClient(Protocol protocol, Protocol? targetContainer = null)
    {
        string? address;

        if (string.IsNullOrEmpty(_iggyServerHost))
        {
            var port = protocol == Protocol.Tcp
                ? _iggyContainer.GetMappedPublicPort(8090)
                : _iggyContainer.GetMappedPublicPort(3000);

            address = protocol == Protocol.Tcp
                ? $"127.0.0.1:{port}"
                : $"http://127.0.0.1:{port}";
        }
        else
        {
            address = protocol == Protocol.Tcp
                ? $"{_iggyServerHost}:8090"
                : $"http://{_iggyServerHost}:3000";
        }

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
