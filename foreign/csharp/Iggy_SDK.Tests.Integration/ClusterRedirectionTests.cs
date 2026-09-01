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
using Apache.Iggy.Contracts.Auth;
using Apache.Iggy.Enums;
using Apache.Iggy.Factory;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Shouldly;

namespace Apache.Iggy.Tests.Integrations;

public class ClusterRedirectionTests
{
    [ClassDataSource<RedirectionClusterFixture>(Shared = SharedType.PerAssembly)]
    public required RedirectionClusterFixture Fixture { get; init; }

    [Test]
    public async Task ConnectToFollower_Should_ReturnClusterMetadataWithAllNodes()
    {
        using var client = IggyClientFactory.CreateClient(new IggyClientConfigurator
        {
            BaseAddress = await Fixture.GetFollowerTcpAddressAsync(),
            Protocol = Protocol.Tcp,
            ReconnectionSettings = new ReconnectionSettings { Enabled = false },
            AutoLoginSettings = new AutoLoginSettings { Enabled = false }
        });
        await client.ConnectAsync();
        await client.LoginUserAsync("iggy", "iggy");

        var metadata = await client.GetClusterMetadataAsync();

        metadata.ShouldNotBeNull();
        metadata.Name.ShouldBe("test-vsr-cluster");
        metadata.Nodes.Length.ShouldBe(3);
        metadata.Nodes.ShouldContain(n => n.Role == ClusterNodeRole.Leader);
        metadata.Nodes.ShouldContain(n => n.Role == ClusterNodeRole.Follower);
    }

    [Test]
    public async Task ConnectToFollowerWithAutoLogin_Should_RedirectToLeader()
    {
        using var client = IggyClientFactory.CreateClient(new IggyClientConfigurator
        {
            BaseAddress = await Fixture.GetFollowerTcpAddressAsync(),
            Protocol = Protocol.Tcp,
            ReconnectionSettings = new ReconnectionSettings { Enabled = true },
            AutoLoginSettings = new AutoLoginSettings
            {
                Enabled = true,
                Username = "iggy",
                Password = "iggy"
            }
        });
        await client.ConnectAsync();

        var address = client.GetCurrentAddress();
        address.ShouldNotBeNullOrEmpty();
        address.ShouldBe(await Fixture.GetIggyAddressAsync(Protocol.Tcp));
    }

    [Test]
    public async Task ConnectToFollowerWithManualLogin_Should_RedirectToLeader()
    {
        using var client = IggyClientFactory.CreateClient(new IggyClientConfigurator
        {
            BaseAddress = await Fixture.GetFollowerTcpAddressAsync(),
            Protocol = Protocol.Tcp,
            ReconnectionSettings = new ReconnectionSettings { Enabled = true },
            AutoLoginSettings = new AutoLoginSettings { Enabled = false }
        });
        await client.ConnectAsync();
        await client.LoginUserAsync("iggy", "iggy");

        var address = client.GetCurrentAddress();
        address.ShouldNotBeNullOrEmpty();
        address.ShouldBe(await Fixture.GetIggyAddressAsync(Protocol.Tcp));
    }

    [Test]
    public async Task ConnectToFollowerWithPersonalAccessToken_Should_RedirectToLeader()
    {
        using var leaderClient = IggyClientFactory.CreateClient(new IggyClientConfigurator
        {
            BaseAddress = await Fixture.GetIggyAddressAsync(Protocol.Tcp),
            Protocol = Protocol.Tcp,
            ReconnectionSettings = new ReconnectionSettings { Enabled = false },
            AutoLoginSettings = new AutoLoginSettings { Enabled = false }
        });
        await leaderClient.ConnectAsync();
        await leaderClient.LoginUserAsync("iggy", "iggy");

        var name = $"pat-{Guid.NewGuid():N}"[..20];
        var pat = await leaderClient.CreatePersonalAccessTokenAsync(name, TimeSpan.FromHours(1));
        pat.ShouldNotBeNull();

        using var client = IggyClientFactory.CreateClient(new IggyClientConfigurator
        {
            BaseAddress = await Fixture.GetFollowerTcpAddressAsync(),
            Protocol = Protocol.Tcp,
            ReconnectionSettings = new ReconnectionSettings { Enabled = true },
            AutoLoginSettings = new AutoLoginSettings { Enabled = false }
        });
        await client.ConnectAsync();
        // The follower verifies the token against its own replicated copy, so it refuses until the mint has
        // replicated. Fail-closed by design; poll through the window rather than asserting on its width.
        var replicationDeadline = DateTimeOffset.UtcNow.AddSeconds(10);
        AuthResponse? authResponse;
        while (true)
        {
            try
            {
                authResponse = await client.LoginWithPersonalAccessTokenAsync(pat.Token);
                break;
            }
            catch (Exception) when (DateTimeOffset.UtcNow < replicationDeadline)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(250));
            }
        }

        authResponse.ShouldNotBeNull();
        authResponse!.UserId.ShouldNotBe(uint.MaxValue);

        var address = client.GetCurrentAddress();
        address.ShouldNotBeNullOrEmpty();
        address.ShouldBe(await Fixture.GetIggyAddressAsync(Protocol.Tcp));
    }
}
