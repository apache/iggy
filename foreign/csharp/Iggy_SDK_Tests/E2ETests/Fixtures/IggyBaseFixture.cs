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
using Iggy_SDK.Configuration;
using Iggy_SDK.Contracts.Http;
using Iggy_SDK.Contracts.Http.Auth;
using Iggy_SDK.Enums;
using Iggy_SDK.Factory;
using Iggy_SDK.IggyClient;
using Microsoft.Extensions.Logging.Abstractions;

namespace Iggy_SDK_Tests.E2ETests.Fixtures;

public abstract class IggyBaseFixture : IAsyncLifetime
{
    public IIggyClient TcpSut => SubjectsUnderTest[0];
    public IIggyClient HttpSut => SubjectsUnderTest[1];
    
    private readonly IIggyBootstrap _bootstraper;
    private readonly Action<MessagePollingSettings> _pollingSettings;
    private readonly Action<MessageBatchingSettings> _batchingSettings;

    private readonly IContainer _tcpContainer = new ContainerBuilder().WithImage("iggyrs/iggy:latest")
        // Container name is just to be used locally for debbuging effects
        //.WithName($"SutIggyContainerTCP")
        .WithPortBinding(8090, true)
        .WithOutputConsumer(Consume.RedirectStdoutAndStderrToConsole())
        .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(8090))
        .WithCleanUp(true)
        .Build();
    
    private readonly IContainer _httpContainer = new ContainerBuilder().WithImage("iggyrs/iggy:latest")
        // Container name is just to be used locally for debbuging effects
        //.WithName($"SutIggyContainerHTTP")
        .WithPortBinding(3000, true)
        .WithOutputConsumer(Consume.RedirectStdoutAndStderrToConsole())
        .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(3000))
        .WithCleanUp(true)
        .Build();
    
    public IIggyClient[] SubjectsUnderTest { get; } = new IIggyClient[2];
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
        var tcpPort = _tcpContainer.GetMappedPublicPort(8090);
        var httpPort = _httpContainer.GetMappedPublicPort(3000);
        
         var firstSubject = MessageStreamFactory.CreateMessageStream(options =>
        {
            options.BaseAdress = $"127.0.0.1:{tcpPort}";
            options.Protocol = Protocol.Tcp;
            options.MessageBatchingSettings = _batchingSettings;
            options.MessagePollingSettings = _pollingSettings;
        }, NullLoggerFactory.Instance);
        await firstSubject.LoginUser(new LoginUserRequest
        {
            Password = "iggy",
            Username = "iggy"
        });
        SubjectsUnderTest[0] = firstSubject;
        
        var secondSubject = (MessageStreamFactory.CreateMessageStream(options =>
        {
            options.BaseAdress = $"http://127.0.0.1:{httpPort}";
            options.Protocol = Protocol.Http;
            options.MessageBatchingSettings = _batchingSettings;
            options.MessagePollingSettings = _pollingSettings;
        }, NullLoggerFactory.Instance));
        await secondSubject.LoginUser(new LoginUserRequest
        {
            Password = "iggy",
            Username = "iggy"
        });
        SubjectsUnderTest[1] = secondSubject;
        
        await _bootstraper.BootstrapResourcesAsync(tcpPort, httpPort, firstSubject, secondSubject);
    }

    public async Task DisposeAsync()
    {
        await _tcpContainer.StopAsync();
        await _httpContainer.StopAsync();
    }
}