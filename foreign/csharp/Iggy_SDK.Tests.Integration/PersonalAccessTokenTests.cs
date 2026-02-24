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

using Apache.Iggy.Contracts.Auth;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Shouldly;

namespace Apache.Iggy.Tests.Integrations;

public class PersonalAccessTokenTests
{
    private static readonly TimeSpan Expiry = TimeSpan.FromHours(1);

    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerAssembly)]
    public required IggyServerFixture Fixture { get; init; }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task CreatePersonalAccessToken_HappyPath_Should_CreatePersonalAccessToken_Successfully(
        Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var name = $"pat-{Guid.NewGuid():N}"[..20];
        var result = await client.CreatePersonalAccessTokenAsync(name, Expiry);

        result.ShouldNotBeNull();
        result.Token.ShouldNotBeNullOrEmpty();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task CreatePersonalAccessToken_Duplicate_Should_Throw_InvalidResponse(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var name = $"dup-{Guid.NewGuid():N}"[..20];
        await client.CreatePersonalAccessTokenAsync(name, Expiry);

        await Should.ThrowAsync<IggyInvalidStatusCodeException>(() =>
            client.CreatePersonalAccessTokenAsync(name, Expiry));
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetPersonalAccessTokens_Should_ReturnValidResponse(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var name = $"get-{Guid.NewGuid():N}"[..20];
        await client.CreatePersonalAccessTokenAsync(name, Expiry);

        IReadOnlyList<PersonalAccessTokenResponse> response
            = await client.GetPersonalAccessTokensAsync();

        response.ShouldNotBeNull();
        response.Count.ShouldBeGreaterThanOrEqualTo(1);
        response.ShouldContain(x => x.Name == name);

        var token = response.First(x => x.Name == name);
        var tokenExpiryDateTimeOffset = DateTimeOffset.UtcNow.Add(Expiry);
        token.ExpiryAt!.Value.ToUniversalTime().ShouldBe(tokenExpiryDateTimeOffset, TimeSpan.FromMinutes(1));
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task LoginWithPersonalAccessToken_Should_Be_Successfully(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var name = $"lgn-{Guid.NewGuid():N}"[..20];
        var response = await client.CreatePersonalAccessTokenAsync(name, Expiry);

        var loginClient = await Fixture.CreateClient(protocol);
        var authResponse = await loginClient.LoginWithPersonalAccessToken(response!.Token);

        authResponse.ShouldNotBeNull();
        authResponse.UserId.ShouldBeGreaterThanOrEqualTo(0);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DeletePersonalAccessToken_Should_DeletePersonalAccessToken_Successfully(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var name = $"del-{Guid.NewGuid():N}"[..20];
        await client.CreatePersonalAccessTokenAsync(name, Expiry);

        await Should.NotThrowAsync(() => client.DeletePersonalAccessTokenAsync(name));
    }
}
