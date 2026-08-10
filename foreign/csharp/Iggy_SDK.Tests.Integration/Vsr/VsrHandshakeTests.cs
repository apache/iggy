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

using Apache.Iggy.Contracts;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Shouldly;

namespace Apache.Iggy.Tests.Integrations.Vsr;

/// <summary>
///     The register handshake and the session it binds. Every other VSR suite depends on this one passing:
///     without a bound session the server fences every replicated request.
/// </summary>
public class VsrHandshakeTests
{
    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerAssembly)]
    public required IggyServerFixture Fixture { get; init; }

    [Test]
    public async Task Login_Should_BindSession_And_ServeReplicatedRequests()
    {
        var client = await Fixture.CreateAuthenticatedClient(Protocol.Tcp);

        var name = $"vsr-handshake-{Guid.NewGuid():N}";
        var stream = await client.CreateStreamAsync(name);

        stream.ShouldNotBeNull();
        stream.Name.ShouldBe(name);
    }

    /// <summary>
    ///     A re-login first logs out the bound session, then registers a fresh client identity. The metadata write
    ///     after re-login proves the request counter belongs to that new binding instead of replaying a cached
    ///     response from the old client table entry.
    /// </summary>
    [Test]
    public async Task ReLogin_Should_AllowReplicatedRequests()
    {
        var client = await Fixture.CreateAuthenticatedClient(Protocol.Tcp);
        var name = $"vsr-relogin-{Guid.NewGuid():N}";
        await client.CreateStreamAsync(name);

        var response = await client.LoginUserAsync("iggy", "iggy");
        response.ShouldNotBeNull();

        var afterRelogin = $"vsr-relogin-after-{Guid.NewGuid():N}";
        var stream = await client.CreateStreamAsync(afterRelogin);
        stream.ShouldNotBeNull();
        stream.Name.ShouldBe(afterRelogin);
    }

    [Test]
    public async Task Logout_Should_UnbindTheSession_Until_TheClientRegistersAgain()
    {
        var client = await Fixture.CreateAuthenticatedClient(Protocol.Tcp);
        await client.LogoutUserAsync();

        await Should.ThrowAsync<Exception>(client.CreateStreamAsync($"vsr-logout-{Guid.NewGuid():N}"));

        await client.LoginUserAsync("iggy", "iggy");

        var name = $"vsr-logout-relogin-{Guid.NewGuid():N}";
        (await client.CreateStreamAsync(name)).ShouldNotBeNull();
    }

    /// <summary>
    ///     A rejected register is a consumed one on the server, so the failure has to reset the session and
    ///     unwind the connection state. The retry with valid credentials is the assertion that matters: a client
    ///     left in <c>Authenticating</c> with the failed register's session still bound would fence it.
    ///     Wrong credentials fall through the server's PAT attempt and come back as a MalformedLogin eviction,
    ///     not as the empty register reply, and an eviction is terminal for the connection: the retry has to
    ///     reconnect first.
    /// </summary>
    [Test]
    public async Task Login_WithInvalidCredentials_Should_ResetTheSession_And_AllowARetry()
    {
        var client = await Fixture.CreateUnauthenticatedClient(Protocol.Tcp);

        var exception = await Should.ThrowAsync<IggyInvalidStatusCodeException>(
            client.LoginUserAsync("iggy", "not-the-password"));

        exception.StatusCode.ShouldBe(4);
        exception.Message.ShouldContain("Malformed login body");

        await client.ConnectAsync();
        (await client.LoginUserAsync("iggy", "iggy")).ShouldNotBeNull();

        var name = $"vsr-failed-login-{Guid.NewGuid():N}";
        (await client.CreateStreamAsync(name)).ShouldNotBeNull();
    }

    [Test]
    public async Task LoginWithPersonalAccessToken_Should_BindTheSession()
    {
        var client = await Fixture.CreateAuthenticatedClient(Protocol.Tcp);
        var token = await client.CreatePersonalAccessTokenAsync($"vsr-pat-{Guid.NewGuid():N}");
        token.ShouldNotBeNull();

        var patClient = await Fixture.CreateUnauthenticatedClient(Protocol.Tcp);
        var response = await patClient.LoginWithPersonalAccessTokenAsync(token.Token);

        response.ShouldNotBeNull();

        var name = $"vsr-pat-stream-{Guid.NewGuid():N}";
        (await patClient.CreateStreamAsync(name)).ShouldNotBeNull();
    }

    [Test]
    public async Task Ping_Should_Succeed_Before_TheSessionIsBound()
    {
        var client = await Fixture.CreateUnauthenticatedClient(Protocol.Tcp);

        // Non-replicated ops are sessionless, so an unbound client still pings.
        await client.PingAsync();

        await Should.ThrowAsync<Exception>(client.CreateStreamAsync($"vsr-unbound-{Guid.NewGuid():N}"));
    }

    [Test]
    public async Task Ping_Should_Ride_NonReplicated_Without_GappingTheRequestCounter()
    {
        var client = await Fixture.CreateAuthenticatedClient(Protocol.Tcp);

        // A ping that consumed a request id would gap the next metadata request, and the primary silently
        // drops a gapped one - the create below would hang instead of failing loudly.
        await client.PingAsync();
        var first = await client.CreateStreamAsync($"vsr-ping-first-{Guid.NewGuid():N}");
        await client.PingAsync();
        var second = await client.CreateStreamAsync($"vsr-ping-second-{Guid.NewGuid():N}");

        first.ShouldNotBeNull();
        second.ShouldNotBeNull();
    }

    [Test]
    public async Task Reads_Should_Ride_NonReplicated_Between_MetadataWrites()
    {
        var client = await Fixture.CreateAuthenticatedClient(Protocol.Tcp);

        var name = $"vsr-reads-{Guid.NewGuid():N}";
        await client.CreateStreamAsync(name);

        IReadOnlyList<StreamResponse> streams = await client.GetStreamsAsync();
        streams.ShouldContain(stream => stream.Name == name);

        var second = $"vsr-reads-second-{Guid.NewGuid():N}";
        (await client.CreateStreamAsync(second)).ShouldNotBeNull();
    }
}
