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
using Apache.Iggy.Tests.Integrations.Fixtures;
using Shouldly;

namespace Apache.Iggy.Tests.Integrations;

public class ConnectionStringTests
{
    // Below the fixture's server heartbeat, or an idle client is evicted mid-test.
    private const string Heartbeat = "heartbeat_interval=1s";

    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerAssembly)]
    public required IggyServerFixture Fixture { get; init; }

    [Test]
    public async Task ConnectionString_WithEveryOption_Should_SignIn()
    {
        var address = await Fixture.GetIggyAddressAsync(Protocol.Tcp);
        var connectionString = $"iggy://iggy:iggy@{address}?nodelay=true&reconnection_retries=1"
                               + $"&reconnection_interval=1s&reestablish_after=5s&tls=false&{Heartbeat}";

        var config = IggyClientConfigurator.FromConnectionString(connectionString);
        config.ReconnectionSettings.MaxRetries.ShouldBe(1);
        config.ReconnectionSettings.InitialDelay.ShouldBe(TimeSpan.FromSeconds(1));
        config.HeartbeatInterval.ShouldBe(TimeSpan.FromSeconds(1));
        config.TlsSettings.Enabled.ShouldBeFalse();

        using var client = IggyClientFactory.CreateClient(config);
        await client.ConnectAsync();

        await Should.NotThrowAsync(client.PingAsync());
        var me = await client.GetMeAsync();
        me.ShouldNotBeNull();
        me.UserId.ShouldBe(0u);
    }

    [Test]
    [Arguments("iggy")]
    [Arguments("iggy+tcp")]
    public async Task ConnectionString_WithPassword_Should_SignIn(string scheme)
    {
        var address = await Fixture.GetIggyAddressAsync(Protocol.Tcp);

        using var client = IggyClientFactory.CreateClient($"{scheme}://iggy:iggy@{address}?{Heartbeat}");
        await client.ConnectAsync();

        var me = await client.GetMeAsync();
        me.ShouldNotBeNull();
        me.UserId.ShouldBe(0u);
    }

    [Test]
    public async Task ConnectionString_WithPersonalAccessToken_Should_SignIn()
    {
        using var admin = await Fixture.CreateAuthenticatedClient(Protocol.Tcp);
        var name = $"cs-{Guid.NewGuid():N}"[..20];
        var token = await admin.CreatePersonalAccessTokenAsync(name, TimeSpan.FromHours(1));
        var address = await Fixture.GetIggyAddressAsync(Protocol.Tcp);

        using var client = IggyClientFactory.CreateClient($"iggy+tcp://{token!.Token}@{address}?{Heartbeat}");
        await client.ConnectAsync();

        var me = await client.GetMeAsync();
        me.ShouldNotBeNull();
        me.UserId.ShouldBe(0u);
    }

    [Test]
    [Arguments("reconnection_retries=unlimited")]
    [Arguments("reconnection_retries=10&reconnection_interval=250ms")]
    [Arguments("reconnection_interval=1m30s")]
    [Arguments("reconnection_retries=0")]
    [Arguments("nodelay=false")]
    [Arguments("reestablish_after=7s")]
    [Arguments("reestablish_after=0")]
    public async Task ConnectionString_WithAcceptedOptionValues_Should_ReachTheServer(string options)
    {
        var address = await Fixture.GetIggyAddressAsync(Protocol.Tcp);

        using var client = IggyClientFactory.CreateClient($"iggy://iggy:iggy@{address}?{options}&{Heartbeat}");
        await client.ConnectAsync();

        await Should.NotThrowAsync(client.PingAsync());
    }
}
