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
    protected IContainer? IggyContainer;

    /// <summary>
    ///     Docker image to use. Can be overridden via IGGY_SERVER_DOCKER_IMAGE environment variable
    ///     or by subclasses. Defaults to apache/iggy:edge if not specified.
    /// </summary>
    private string DockerImage =>
        Environment.GetEnvironmentVariable("IGGY_SERVER_DOCKER_IMAGE") ?? "apache/iggy:edge";

    /// <summary>
    ///     Environment variables for the container. Override in subclasses to customize.
    /// </summary>
    protected virtual Dictionary<string, string> EnvironmentVariables => new()
    {
        { "IGGY_ROOT_USERNAME", "iggy" },
        { "IGGY_ROOT_PASSWORD", "iggy" },
        { "IGGY_TCP_ADDRESS", "0.0.0.0:8090" },
        { "IGGY_HTTP_ADDRESS", "0.0.0.0:3000" }
    };

    /// <summary>
    ///     Enables iggy server trace logs.
    /// </summary>
    protected bool EnabledServerTraceLogs => false;

    /// <summary>
    ///     Resource mappings (volumes, etc.) for the container. Override in subclasses to add custom mappings.
    /// </summary>
    protected virtual ResourceMapping[] ResourceMappings => [];

    public IggyServerFixture()
    {
        var builder = new ContainerBuilder()
            .WithImage(DockerImage)
            .WithPortBinding(3000, true)
            .WithPortBinding(8090, true)
            .WithOutputConsumer(Consume.RedirectStdoutAndStderrToConsole())
            .WithWaitStrategy(Wait.ForUnixContainer().UntilInternalTcpPortIsAvailable(8090))
            .WithName($"{Guid.NewGuid()}")
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

        IggyContainer = builder.Build();
    }

    public async ValueTask DisposeAsync()
    {
        if (IggyContainer == null)
        {
            return;
        }

        await IggyContainer.StopAsync();
    }

    public virtual async Task InitializeAsync()
    {
        await IggyContainer!.StartAsync();

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

    public async Task<IIggyClient> CreateTcpClient(string userName = "iggy", string password = "iggy",
        bool connect = true)
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

    public async Task<IIggyClient> CreateClient(Protocol protocol, Protocol? targetContainer = null,
        bool connect = true)
    {
        var address = GetIggyAddress(protocol);

        var client = IggyClientFactory.CreateClient(new IggyClientConfigurator
        {
            BaseAddress = address,
            Protocol = protocol,
            ReconnectionSettings = new ReconnectionSettings { Enabled = true },
            AutoLoginSettings = new AutoLoginSettings
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

    public virtual string GetIggyAddress(Protocol protocol)
    {
        var port = protocol == Protocol.Tcp
            ? IggyContainer!.GetMappedPublicPort(8090)
            : IggyContainer!.GetMappedPublicPort(3000);

        return protocol == Protocol.Tcp
            ? $"127.0.0.1:{port}"
            : $"http://127.0.0.1:{port}";
    }

    public static IEnumerable<Func<Protocol>> ProtocolData()
    {
        yield return () => Protocol.Http;
        yield return () => Protocol.Tcp;
    }
}
