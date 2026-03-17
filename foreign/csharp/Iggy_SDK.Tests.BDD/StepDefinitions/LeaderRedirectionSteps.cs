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

using System.Net.Sockets;
using Apache.Iggy.Configuration;
using Apache.Iggy.Contracts;
using Apache.Iggy.Enums;
using Apache.Iggy.Factory;
using Apache.Iggy.IggyClient;
using Reqnroll;
using Shouldly;
using TestContext = Apache.Iggy.Tests.BDD.Context.TestContext;

namespace Apache.Iggy.Tests.BDD.StepDefinitions;

[Binding]
public class LeaderRedirectionSteps
{
    private readonly TestContext _context;

    public LeaderRedirectionSteps(TestContext context)
    {
        _context = context;
    }

    [Given(@"I have cluster configuration enabled with (\d+) nodes")]
    public void GivenIHaveClusterConfigurationEnabledWithNodes(int nodeCount)
    {
        nodeCount.ShouldBe(2);
    }

    [Given(@"node (\d+) is configured on port (\d+)")]
    public void GivenNodeIsConfiguredOnPort(int nodeId, int port)
    {
        _ = nodeId;
        ResolveAddressForPort(port).ShouldNotBeNullOrEmpty();
    }

    [Given(@"I start server (\d+) on port (\d+) as (leader|follower)")]
    public async Task GivenIStartServerOnPortAs(int nodeId, int port, string role)
    {
        _ = nodeId;
        var address = ResolveAddressForRole(role);
        address.ShouldEndWith($":{port}");
        await WaitForServerPortAsync(address);
    }

    [Given(@"I start a single server on port (\d+) without clustering enabled")]
    public async Task GivenIStartASingleServerOnPortWithoutClusteringEnabled(int port)
    {
        _context.TcpUrl.ShouldEndWith($":{port}");
        await WaitForServerPortAsync(_context.TcpUrl);
    }

    [When(@"I create a client connecting to (follower|leader) on port (\d+)")]
    public async Task WhenICreateAClientConnectingToOnPort(string role, int port)
    {
        var address = ResolveAddressForRole(role);
        address.ShouldEndWith($":{port}");

        await CreateAndConnectClient("main", address);
        _context.InitialAddress = address;
    }

    [When(@"I create a client connecting directly to leader on port (\d+)")]
    public async Task WhenICreateAClientConnectingDirectlyToLeaderOnPort(int port)
    {
        var address = _context.LeaderTcpUrl;
        address.ShouldEndWith($":{port}");

        await CreateAndConnectClient("main", address);
        _context.InitialAddress = address;
    }

    [When(@"I create a client connecting to port (\d+)")]
    public async Task WhenICreateAClientConnectingToPort(int port)
    {
        var address = ResolveAddressForPort(port);
        await CreateAndConnectClient("main", address);
        _context.InitialAddress = address;
    }

    [When(@"I create client ([A-Z]) connecting to port (\d+)")]
    public async Task WhenICreateClientConnectingToPort(string clientName, int port)
    {
        var address = ResolveAddressForPort(port);
        await CreateAndConnectClient(clientName, address);
    }

    [When(@"I authenticate as root user")]
    public async Task WhenIAuthenticateAsRootUser()
    {
        var client = GetClient("main");
        var loginResult = await client.LoginUser("iggy", "iggy");

        loginResult.ShouldNotBeNull();
        loginResult.UserId.ShouldBe(0);
    }

    [When(@"both clients authenticate as root user")]
    public async Task WhenBothClientsAuthenticateAsRootUser()
    {
        foreach (var clientName in _context.Clients.Keys.OrderBy(name => name).ToList())
        {
            var loginResult = await _context.Clients[clientName].LoginUser("iggy", "iggy");
            loginResult.ShouldNotBeNull();
            loginResult.UserId.ShouldBe(0);
        }
    }

    [When(@"I create a stream named ""([^""]+)""")]
    public async Task WhenICreateAStreamNamed(string streamName)
    {
        _context.CreatedStream = await GetClient("main").CreateStreamAsync(streamName);
    }

    [Then(@"the client should automatically redirect to leader on port (\d+)")]
    public async Task ThenTheClientShouldAutomaticallyRedirectToLeaderOnPort(int port)
    {
        await AssertClientAddress("main", port);
        GetClient("main").GetCurrentAddress().ShouldNotBe(_context.InitialAddress);
    }

    [Then(@"the stream should be created successfully on the leader")]
    public void ThenTheStreamShouldBeCreatedSuccessfullyOnTheLeader()
    {
        _context.CreatedStream.ShouldNotBeNull();
        _context.CreatedStream.Name.ShouldNotBeNullOrEmpty();
    }

    [Then(@"the client should not perform any redirection")]
    public void ThenTheClientShouldNotPerformAnyRedirection()
    {
        GetClient("main").GetCurrentAddress().ShouldBe(_context.InitialAddress);
    }

    [Then(@"the connection should remain on port (\d+)")]
    public async Task ThenTheConnectionShouldRemainOnPort(int port)
    {
        await AssertClientAddress("main", port);
    }

    [Then(@"the client should connect successfully without redirection")]
    public void ThenTheClientShouldConnectSuccessfullyWithoutRedirection()
    {
        GetClient("main").ShouldNotBeNull();
        GetClient("main").GetCurrentAddress().ShouldBe(_context.InitialAddress);
    }

    [Then(@"client ([A-Z]) should stay connected to port (\d+)")]
    public async Task ThenClientShouldStayConnectedToPort(string clientName, int port)
    {
        await AssertClientAddress(clientName, port);
    }

    [Then(@"client ([A-Z]) should redirect to port (\d+)")]
    public async Task ThenClientShouldRedirectToPort(string clientName, int port)
    {
        await AssertClientAddress(clientName, port);
    }

    [Then(@"both clients should be using the same server")]
    public void ThenBothClientsShouldBeUsingTheSameServer()
    {
        GetClient("A").GetCurrentAddress().ShouldBe(GetClient("B").GetCurrentAddress());
    }

    private async Task CreateAndConnectClient(string name, string address)
    {
        var client = IggyClientFactory.CreateClient(new IggyClientConfigurator
        {
            BaseAddress = address,
            Protocol = Protocol.Tcp,
            ReconnectionSettings = new ReconnectionSettings { Enabled = true },
            AutoLoginSettings = new AutoLoginSettings { Enabled = false }
        });

        await client.ConnectAsync();
        await client.PingAsync();

        _context.Clients[name] = client;
        if (name == "main")
        {
            _context.IggyClient = client;
        }
    }

    private IIggyClient GetClient(string name)
    {
        return _context.Clients[name];
    }

    private async Task AssertClientAddress(string clientName, int expectedPort)
    {
        var client = GetClient(clientName);
        await client.PingAsync();
        client.GetCurrentAddress().ShouldEndWith($":{expectedPort}");
    }

    private string ResolveAddressForRole(string role)
    {
        return role switch
        {
            "leader" => _context.LeaderTcpUrl,
            "follower" => _context.FollowerTcpUrl,
            _ => throw new ArgumentOutOfRangeException(nameof(role), role, "Unsupported server role")
        };
    }

    private string ResolveAddressForPort(int port)
    {
        return port switch
        {
            8090 => _context.TcpUrl,
            8091 => _context.LeaderTcpUrl,
            8092 => _context.FollowerTcpUrl,
            _ => throw new ArgumentOutOfRangeException(nameof(port), port, "Unsupported test port")
        };
    }

    private static async Task WaitForServerPortAsync(string address)
    {
        var (host, port) = ParseTcpEndpoint(address);
        const int maxAttempts = 50;

        for (var attempt = 0; attempt < maxAttempts; attempt++)
        {
            using var tcpClient = new TcpClient();

            try
            {
                var connectTask = tcpClient.ConnectAsync(host, port);
                var completedTask = await Task.WhenAny(connectTask, Task.Delay(200));
                if (completedTask == connectTask && tcpClient.Connected)
                {
                    return;
                }
            }
            catch (SocketException)
            {
                // Retry until the container starts accepting connections.
            }

            await Task.Delay(200);
        }

        throw new TimeoutException($"Timed out waiting for server at {address} to accept TCP connections.");
    }

    private static (string Host, int Port) ParseTcpEndpoint(string address)
    {
        if (Uri.TryCreate(address, UriKind.Absolute, out var absoluteUri))
        {
            return (absoluteUri.Host, absoluteUri.Port);
        }

        if (Uri.TryCreate($"tcp://{address}", UriKind.Absolute, out var tcpUri))
        {
            return (tcpUri.Host, tcpUri.Port);
        }

        throw new ArgumentException($"Invalid server address: {address}", nameof(address));
    }
}
