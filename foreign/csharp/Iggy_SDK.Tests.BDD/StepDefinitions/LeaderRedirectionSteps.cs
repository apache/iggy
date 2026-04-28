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
using Apache.Iggy.Contracts;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Factory;
using Apache.Iggy.IggyClient;
using Reqnroll;
using Shouldly;
using TestContext = Apache.Iggy.Tests.BDD.Context.TestContext;

namespace Apache.Iggy.Tests.BDD.StepDefinitions;

[Binding]
public class LeaderRedirectionSteps
{
    private const string RootUsername = "iggy";
    private const string RootPassword = "iggy";

    private readonly TestContext _context;

    public LeaderRedirectionSteps(TestContext context)
    {
        _context = context;
    }

    // ---------- Background ----------

    [Given(@"I have cluster configuration enabled with (\d+) nodes")]
    public void GivenIHaveClusterConfigurationEnabledWithNodes(int nodeCount)
    {
        nodeCount.ShouldBeGreaterThan(0);
    }

    [Given(@"node (\d+) is configured on port (\d+)")]
    public void GivenNodeIsConfiguredOnPort(int nodeId, int port)
    {
        ResolveAddressForPort(port).ShouldNotBeNullOrEmpty();
    }

    // ---------- Server start steps (just validate addresses are configured) ----------

    [Given(@"I start server (\d+) on port (\d+) as (leader|follower)")]
    public void GivenIStartServerOnPortAs(int nodeId, int port, string role)
    {
        var address = ResolveAddressForRole(role);
        if (!address.EndsWith($":{port}"))
        {
            throw new InvalidOperationException(
                $"{role} address {address} does not match expected port {port}");
        }
    }

    [Given(@"I start a single server on port (\d+) without clustering enabled")]
    public void GivenIStartASingleServerOnPortWithoutClusteringEnabled(int port)
    {
        if (!_context.TcpUrl.EndsWith($":{port}"))
        {
            throw new InvalidOperationException(
                $"Single-server address {_context.TcpUrl} does not match expected port {port}");
        }
    }

    // ---------- Client creation ----------

    [When(@"I create a client connecting to (follower|leader) on port (\d+)")]
    public async Task WhenICreateAClientConnectingToOnPort(string role, int port)
    {
        var address = ResolveAddressForRole(role);
        if (!address.EndsWith($":{port}"))
        {
            throw new InvalidOperationException(
                $"{role} address {address} does not match expected port {port}");
        }
        await CreateAndConnectClient("main", address);
    }

    [When(@"I create a client connecting directly to leader on port (\d+)")]
    public async Task WhenICreateAClientConnectingDirectlyToLeaderOnPort(int port)
    {
        var address = _context.LeaderTcpUrl;
        if (!address.EndsWith($":{port}"))
        {
            throw new InvalidOperationException(
                $"Leader address {address} does not match expected port {port}");
        }
        await CreateAndConnectClient("main", address);
        _context.RedirectionOccurred = false;
    }

    [When(@"I create a client connecting to port (\d+)")]
    public async Task WhenICreateAClientConnectingToPort(int port)
    {
        await CreateAndConnectClient("main", ResolveAddressForPort(port));
    }

    [When(@"I create client ([A-Z]) connecting to port (\d+)")]
    public async Task WhenICreateNamedClientConnectingToPort(string clientName, int port)
    {
        await CreateAndConnectClient(clientName, ResolveAddressForPort(port));
    }

    // ---------- Auth ----------

    [When(@"I authenticate as root user")]
    public async Task WhenIAuthenticateAsRootUser()
    {
        await AuthenticateAllClients();
    }

    [When(@"both clients authenticate as root user")]
    public async Task WhenBothClientsAuthenticateAsRootUser()
    {
        await AuthenticateAllClients();
    }

    private async Task AuthenticateAllClients()
    {
        var names = _context.Clients.Count > 1
            ? _context.Clients.Keys.ToList()
            : new List<string> { "main" };

        foreach (var name in names)
        {
            var client = GetClient(name);
            var initialAddress = client.GetCurrentAddress();
            var result = await client.LoginUserAsync(RootUsername, RootPassword);
            result.ShouldNotBeNull("Failed to login as root");

            // RedirectAsync runs inside LoginUserAsync; if address changed, redirection happened.
            if (client.GetCurrentAddress() != initialAddress)
            {
                _context.RedirectionOccurred = true;
            }

            if (_context.Clients.Count > 1)
            {
                await Task.Delay(100);
            }
        }
    }

    // ---------- Stream operations ----------

    [When(@"I create a stream named ""(.+)""")]
    public async Task WhenICreateAStreamNamed(string streamName)
    {
        var client = GetClient("main");
        var stream = await client.CreateStreamAsync(streamName);
        stream.ShouldNotBeNull("Should be able to create stream");
        _context.LastStreamId = stream.Id;
    }

    [Then(@"the stream should be created successfully on the leader")]
    public void ThenTheStreamShouldBeCreatedSuccessfullyOnTheLeader()
    {
        _context.LastStreamId.ShouldNotBeNull("Stream should have been created on leader");
    }

    // ---------- Connection / redirection assertions ----------

    [Then(@"the client should automatically redirect to leader on port (\d+)")]
    public async Task ThenTheClientShouldAutomaticallyRedirectToLeaderOnPort(int expectedPort)
    {
        await VerifyClientPort("main", expectedPort, markRedirection: true);
    }

    [Then(@"client ([A-Z]) should stay connected to port (\d+)")]
    public async Task ThenNamedClientShouldStayConnectedToPort(string clientName, int expectedPort)
    {
        await VerifyClientPort(clientName, expectedPort, markRedirection: false);
    }

    [Then(@"client ([A-Z]) should redirect to port (\d+)")]
    public async Task ThenNamedClientShouldRedirectToPort(string clientName, int expectedPort)
    {
        await VerifyClientPort(clientName, expectedPort, markRedirection: true);
    }

    [Then(@"the client should not perform any redirection")]
    public void ThenTheClientShouldNotPerformAnyRedirection()
    {
        _context.RedirectionOccurred.ShouldBeFalse(
            "No redirection should occur when connecting directly to leader");
    }

    [Then(@"the connection should remain on port (\d+)")]
    public async Task ThenTheConnectionShouldRemainOnPort(int port)
    {
        var client = GetClient("main");
        await VerifyClientConnection(client, port);
        _context.RedirectionOccurred.ShouldBeFalse("Connection should not have been redirected");
    }

    [Then(@"the client should connect successfully without redirection")]
    public async Task ThenTheClientShouldConnectSuccessfullyWithoutRedirection()
    {
        var client = GetClient("main");
        await client.PingAsync();
        _context.RedirectionOccurred.ShouldBeFalse("No redirection should occur without clustering");
    }

    [Then(@"both clients should be using the same server")]
    public async Task ThenBothClientsShouldBeUsingTheSameServer()
    {
        var clientA = GetClient("A");
        var clientB = GetClient("B");

        clientA.GetCurrentAddress().ShouldBe(clientB.GetCurrentAddress(),
            "Both clients should be connected to the same server");

        await clientA.PingAsync();
        await clientB.PingAsync();

        var leaderA = await GetLeaderFromMetadata(clientA);
        var leaderB = await GetLeaderFromMetadata(clientB);
        if (leaderA is not null && leaderB is not null)
        {
            $"{leaderA.Ip}:{leaderA.Endpoints.Tcp}".ShouldBe(
                $"{leaderB.Ip}:{leaderB.Endpoints.Tcp}",
                "Both clients should see the same leader");
        }
    }

    // ---------- Helpers ----------

    private string ResolveAddressForRole(string role) => role.ToLowerInvariant() switch
    {
        "leader" => _context.LeaderTcpUrl,
        "follower" => _context.FollowerTcpUrl,
        "single" => _context.TcpUrl,
        _ => throw new ArgumentOutOfRangeException(nameof(role), role, "Unknown role"),
    };

    private string ResolveAddressForPort(int port) => port switch
    {
        8090 => _context.TcpUrl,
        8091 => _context.LeaderTcpUrl,
        8092 => _context.FollowerTcpUrl,
        _ => throw new ArgumentOutOfRangeException(nameof(port), port, "Unknown port"),
    };

    private async Task CreateAndConnectClient(string name, string address)
    {
        var client = IggyClientFactory.CreateClient(new IggyClientConfigurator
        {
            BaseAddress = address,
            Protocol = Protocol.Tcp,
        });
        await client.ConnectAsync();
        _context.Clients[name] = client;
        if (name == "main")
        {
            _context.IggyClient = client;
        }
    }

    private IIggyClient GetClient(string name)
    {
        return _context.Clients.TryGetValue(name, out var c)
            ? c
            : throw new InvalidOperationException($"Client {name} should exist");
    }

    private async Task VerifyClientPort(string clientName, int expectedPort, bool markRedirection)
    {
        var client = GetClient(clientName);
        await VerifyClientConnection(client, expectedPort);

        var leader = await GetLeaderFromMetadata(client);
        if (leader is not null && markRedirection && leader.Endpoints.Tcp == expectedPort)
        {
            _context.RedirectionOccurred = true;
        }
    }

    private static async Task VerifyClientConnection(IIggyClient client, int expectedPort)
    {
        var addr = client.GetCurrentAddress();
        if (!addr.EndsWith($":{expectedPort}"))
        {
            throw new ShouldAssertException(
                $"Expected connection to port {expectedPort}, but connected to: {addr}");
        }
        await client.PingAsync();
    }

    private static async Task<ClusterNode?> GetLeaderFromMetadata(IIggyClient client)
    {
        try
        {
            var metadata = await client.GetClusterMetadataAsync();
            return metadata?.Nodes.FirstOrDefault(n =>
                n.Role == ClusterNodeRole.Leader && n.Status == ClusterNodeStatus.Healthy);
        }
        catch (IggyInvalidStatusCodeException e) when (e.StatusCode == 5)
        {
            return null;
        }
    }
}
