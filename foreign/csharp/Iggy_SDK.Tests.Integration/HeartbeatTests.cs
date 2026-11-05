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
using Apache.Iggy.Exceptions;
using Apache.Iggy.Factory;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Shouldly;

namespace Apache.Iggy.Tests.Integrations;

/// <summary>
///     Runs against a server that evicts consumer-group members it has not heard from. Only a group member is
///     evicted, so every test joins a group before going idle.
/// </summary>
public class HeartbeatTests
{
    private const string TopicName = "heartbeat-topic";
    private const string GroupName = "heartbeat-group";

    // Transport eviction removes the binding; committed lease expiry removes membership. A silent session stops
    // renewing after 1.2 intervals and ends 1.2 intervals later. The rest covers the report and expiry passes.
    private static readonly TimeSpan IdleFor = IggyServerFixture.ServerHeartbeatInterval * 3;

    // A stale eviction drops only the link, and its redial must bind the session before that lease ends. The
    // default first retry can wait up to 5 seconds, which can come after the end of the lease.
    private static readonly TimeSpan ReconnectDelay = TimeSpan.FromMilliseconds(500);

    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerAssembly)]
    public required IggyServerFixture Fixture { get; init; }

    [Test]
    public async Task IdleGroupMember_WithHeartbeat_Should_StayMember()
    {
        using var client = await CreateClient(TimeSpan.FromSeconds(1));
        var (streamName, groupId) = await JoinFreshGroup(client);

        await Task.Delay(IdleFor);

        using var observer = await Fixture.CreateAuthenticatedClient(Protocol.Tcp);
        var group = await observer.GetConsumerGroupByIdAsync(Identifier.String(streamName),
            Identifier.String(TopicName), Identifier.Numeric(groupId));
        group.ShouldNotBeNull();
        group.MembersCount.ShouldBe(1u);

        var me = await client.GetMeAsync();
        me.ShouldNotBeNull();
        me.ConsumerGroupsCount.ShouldBe(1u);
    }

    [Test]
    public async Task IdleGroupMember_WithSlowHeartbeat_Should_RebindAndStayMember()
    {
        // Heartbeat cannot be turned off, so a ping interval far past the server's threshold stands in for one.
        using var client = await CreateClient(TimeSpan.FromHours(1));
        var (streamName, groupId) = await JoinFreshGroup(client);

        await Task.Delay(IdleFor);

        // The server evicts the silent link, and the redial binds the same session again. That renews its lease,
        // so the membership stays.
        using var observer = await Fixture.CreateAuthenticatedClient(Protocol.Tcp);
        var group = await observer.GetConsumerGroupByIdAsync(Identifier.String(streamName),
            Identifier.String(TopicName), Identifier.Numeric(groupId));
        group.ShouldNotBeNull();
        group.MembersCount.ShouldBe(1u);

        var me = await client.GetMeAsync();
        me.ShouldNotBeNull();
        me.ConsumerGroupsCount.ShouldBe(1u);
    }

    [Test]
    public async Task EvictedClient_WithPersonalAccessTokenAutoLogin_Should_Reconnect()
    {
        using var issuer = await Fixture.CreateAuthenticatedClient(Protocol.Tcp);
        var pat = await issuer.CreatePersonalAccessTokenAsync($"heartbeat-{Guid.NewGuid():N}",
            TimeSpan.FromHours(1));

        using var client = await CreateClient(TimeSpan.FromHours(1),
            AutoLoginSettings.ForPersonalAccessToken(pat!.Token));
        var (streamName, groupId) = await JoinFreshGroup(client);

        await Task.Delay(IdleFor);

        // The rebind keeps the session inside its lease, so the member survives the eviction.
        var group = await issuer.GetConsumerGroupByIdAsync(Identifier.String(streamName),
            Identifier.String(TopicName), Identifier.Numeric(groupId));
        group.ShouldNotBeNull();
        group.MembersCount.ShouldBe(1u);

        var me = await client.GetMeAsync();
        me.ShouldNotBeNull();
        me.ConsumerGroupsCount.ShouldBe(1u);
    }

    /// <summary>
    ///     An eviction is the server's heartbeat verifier reacting to silence, not caller intent. It drops only the
    ///     link, so a client that signed in by hand binds the same session again, exactly like one whose credentials
    ///     were configured. Only an explicit sign-out, Dispose or the end of the lease ends the session.
    /// </summary>
    [Test]
    public async Task EvictedClient_WithoutAutoLogin_Should_ReestablishItsSession()
    {
        using var client = await CreateClient(TimeSpan.FromHours(1), false);
        await client.LoginUserAsync("iggy", "iggy");
        var (streamName, _) = await JoinFreshGroup(client);

        await Task.Delay(IdleFor);

        var stream = await client.GetStreamByIdAsync(Identifier.String(streamName));
        stream.ShouldNotBeNull();

        // The rebind keeps the session inside its lease, so its group membership survives the eviction.
        var me = await client.GetMeAsync();
        me.ShouldNotBeNull();
        me.ConsumerGroupsCount.ShouldBe(1u);
    }

    private Task<IIggyClient> CreateClient(TimeSpan heartbeatInterval, bool autoLogin = true)
    {
        return CreateClient(heartbeatInterval,
            autoLogin ? AutoLoginSettings.For("iggy", "iggy") : new AutoLoginSettings());
    }

    private async Task<IIggyClient> CreateClient(TimeSpan heartbeatInterval, AutoLoginSettings autoLogin)
    {
        var client = IggyClientFactory.CreateClient(new IggyClientConfigurator
        {
            BaseAddress = await Fixture.GetIggyAddressAsync(Protocol.Tcp),
            Protocol = Protocol.Tcp,
            HeartbeatInterval = heartbeatInterval,
            ReconnectionSettings = new ReconnectionSettings { InitialDelay = ReconnectDelay },
            AutoLoginSettings = autoLogin
        });
        await client.ConnectAsync();

        return client;
    }

    private static async Task<(string streamName, uint groupId)> JoinFreshGroup(IIggyClient client)
    {
        var streamName = $"heartbeat-stream-{Guid.NewGuid():N}";
        await client.CreateStreamAsync(streamName);
        await client.CreateTopicAsync(Identifier.String(streamName), TopicName, 2);
        var group = await client.CreateConsumerGroupAsync(Identifier.String(streamName),
            Identifier.String(TopicName), GroupName);
        await client.JoinConsumerGroupAsync(Identifier.String(streamName), Identifier.String(TopicName),
            Identifier.Numeric(group!.Id));

        return (streamName, group.Id);
    }
}
