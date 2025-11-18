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
using Apache.Iggy.Tests.Integrations.Attributes;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Shouldly;

namespace Apache.Iggy.Tests.Integrations;

public class PersonalAccessTokenTests
{
    private const string Name = "test-pat";
    private const ulong Expiry = 100_000_000;

    [ClassDataSource<PersonalAccessTokenFixture>(Shared = SharedType.PerClass)]
    public required PersonalAccessTokenFixture Fixture { get; init; }


    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task CreatePersonalAccessToken_HappyPath_Should_CreatePersonalAccessToken_Successfully(
        Protocol protocol)
    {
        var result = await Fixture.Clients[protocol].CreatePersonalAccessTokenAsync(Name, Expiry);

        result.ShouldNotBeNull();
        result.Token.ShouldNotBeNullOrEmpty();
    }

    [Test]
    [DependsOn(nameof(CreatePersonalAccessToken_HappyPath_Should_CreatePersonalAccessToken_Successfully))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task CreatePersonalAccessToken_Duplicate_Should_Throw_InvalidResponse(Protocol protocol)
    {
        await Should.ThrowAsync<IggyInvalidStatusCodeException>(() =>
            Fixture.Clients[protocol].CreatePersonalAccessTokenAsync(Name, Expiry));
    }

    [Test]
    [DependsOn(nameof(CreatePersonalAccessToken_Duplicate_Should_Throw_InvalidResponse))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task GetPersonalAccessTokens_Should_ReturnValidResponse(Protocol protocol)
    {
        IReadOnlyList<PersonalAccessTokenResponse> response
            = await Fixture.Clients[protocol].GetPersonalAccessTokensAsync();

        response.ShouldNotBeNull();
        response.Count.ShouldBe(1);
        response[0].Name.ShouldBe(Name);
        var tokenExpiryDateTimeOffset = DateTimeOffset.UtcNow.AddMicroseconds(Expiry);
        response[0].ExpiryAt!.Value.ToUniversalTime().ShouldBe(tokenExpiryDateTimeOffset, TimeSpan.FromMinutes(1));
    }

    [Test]
    [DependsOn(nameof(GetPersonalAccessTokens_Should_ReturnValidResponse))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task LoginWithPersonalAccessToken_Should_Be_Successfully(Protocol protocol)
    {
        var response = await Fixture.Clients[protocol].CreatePersonalAccessTokenAsync("test-pat-login", 100_000_000);

        var client = await Fixture.IggyServerFixture.CreateClient(protocol);

        var authResponse = await client.LoginWithPersonalAccessToken(response!.Token);

        authResponse.ShouldNotBeNull();
        authResponse.UserId.ShouldBeGreaterThan(0);
    }

    [Test]
    [DependsOn(nameof(LoginWithPersonalAccessToken_Should_Be_Successfully))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DeletePersonalAccessToken_Should_DeletePersonalAccessToken_Successfully(Protocol protocol)
    {
        await Should.NotThrowAsync(() => Fixture.Clients[protocol].DeletePersonalAccessTokenAsync(Name));
    }
}
