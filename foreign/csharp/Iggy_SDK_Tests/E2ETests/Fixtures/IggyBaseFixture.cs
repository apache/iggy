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
using Iggy_SDK_Tests.E2ETests.Fixtures.Models;
using Iggy_SDK.Configuration;
using Iggy_SDK.Contracts.Http.Auth;
using Iggy_SDK.Enums;
using Iggy_SDK.Factory;
using Microsoft.Extensions.Logging.Abstractions;

namespace Iggy_SDK_Tests.E2ETests.Fixtures;

public abstract class IggyBaseFixture : IAsyncLifetime
{
    private readonly IIggyBootstrap _bootstraper;
    private readonly Action<MessagePollingSettings> _pollingSettings;
    private readonly Action<MessageBatchingSettings> _batchingSettings;

    private readonly IContainer _tcpContainer = new ContainerBuilder().WithImage("apache/iggy:edge")
        // Container name is just to be used locally for debbuging effects
        //.WithName($"SutIggyContainerTCP")
        .WithPortBinding(3000, true)
        .WithPortBinding(8090, true)
        .WithOutputConsumer(Consume.RedirectStdoutAndStderrToConsole())
        .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(8090))
        .WithEnvironment("IGGY_SYSTEM_LOGGING_LEVEL", "debug")
        .WithEnvironment("RUST_LOG", "trace")
        .WithCleanUp(true)
        .Build();
    
    private readonly IContainer _httpContainer = new ContainerBuilder().WithImage("apache/iggy:edge")
        // Container name is just to be used locally for debbuging effects
        //.WithName($"SutIggyContainerHTTP")
        .WithPortBinding(3000, true)
        .WithPortBinding(8090, true)
        .WithOutputConsumer(Consume.RedirectStdoutAndStderrToConsole())
        .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(3000))
        .WithEnvironment("IGGY_SYSTEM_LOGGING_LEVEL", "debug")
        .WithEnvironment("RUST_LOG", "trace")
        .WithCleanUp(true)
        .Build();
    
    public IggyClientModel[] SubjectsUnderTest { get; } = new IggyClientModel[2];
    public IggyClientModel TcpClient => SubjectsUnderTest[0];
    public IggyClientModel HttpClient => SubjectsUnderTest[1];
    
    protected IggyBaseFixture(IIggyBootstrap bootstraper, Action<MessagePollingSettings> pollingSettings,
        Action<MessageBatchingSettings> batchingSettings)
    {
        _bootstraper = bootstraper;
        _pollingSettings = pollingSettings;
        _batchingSettings = batchingSettings;
    }

    public async Task InitializeAsync()
    {
        await _tcpContainer.StartAsync();
        await _httpContainer.StartAsync();
        var tcpClient = new IggyClientModel { Name = "Tcp" };
        var httpClient = new IggyClientModel { Name = "Http" };
        
        tcpClient.TcpPort = _tcpContainer.GetMappedPublicPort(8090);
        tcpClient.HttpPort = _tcpContainer.GetMappedPublicPort(3000);
        
        httpClient.TcpPort = _httpContainer.GetMappedPublicPort(8090);
        httpClient.HttpPort = _httpContainer.GetMappedPublicPort(3000);
        
        tcpClient.Client = MessageStreamFactory.CreateMessageStream(options =>
        {
            options.BaseAdress = $"127.0.0.1:{tcpClient.TcpPort}";
            options.Protocol = Protocol.Tcp;
            options.MessageBatchingSettings = _batchingSettings;
            options.MessagePollingSettings = _pollingSettings;
        }, NullLoggerFactory.Instance);
        
        await tcpClient.Client.LoginUser(new LoginUserRequest
        {
            Password = "iggy",
            Username = "iggy"
        });
        
        httpClient.Client = MessageStreamFactory.CreateMessageStream(options =>
        {
            options.BaseAdress = $"http://127.0.0.1:{httpClient.HttpPort}";
            options.Protocol = Protocol.Http;
            options.MessageBatchingSettings = _batchingSettings;
            options.MessagePollingSettings = _pollingSettings;
        }, NullLoggerFactory.Instance);
        await httpClient.Client.LoginUser(new LoginUserRequest
        {
            Password = "iggy",
            Username = "iggy"
        });
        
        SubjectsUnderTest[0] = tcpClient;
        SubjectsUnderTest[1] = httpClient;
        
        await _bootstraper.BootstrapResourcesAsync(httpClient, tcpClient);
    }

    public async Task DisposeAsync()
    {
        await _tcpContainer.StopAsync();
        await _httpContainer.StopAsync();
    }
}