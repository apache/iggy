﻿// // Licensed to the Apache Software Foundation (ASF) under one
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

using Apache.Iggy.Contracts.Http.Auth;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Shouldly;

namespace Apache.Iggy.Tests.Integrations;

[MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
public class PersonalAccessTokenTests(Protocol protocol)
{
    private static readonly CreatePersonalAccessTokenRequest CreatePersonalAccessTokenRequest = new()
    {
        Name = "test-pat",
        Expiry = 100_000_000
    };

    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerClass)]
    public required IggyServerFixture Fixture { get; init; }


    [Test]
    public async Task CreatePersonalAccessToken_HappyPath_Should_CreatePersonalAccessToken_Successfully()
    {
        var result = await Fixture.Clients[protocol].CreatePersonalAccessTokenAsync(CreatePersonalAccessTokenRequest);

        result.ShouldNotBeNull();
        result.Token.ShouldNotBeNullOrEmpty();
    }

    [Test]
    [DependsOn(nameof(CreatePersonalAccessToken_HappyPath_Should_CreatePersonalAccessToken_Successfully))]
    public async Task CreatePersonalAccessToken_Duplicate_Should_Throw_InvalidResponse()
    {
        await Should.ThrowAsync<InvalidResponseException>(() => Fixture.Clients[protocol].CreatePersonalAccessTokenAsync(CreatePersonalAccessTokenRequest));
    }

    [Test]
    [DependsOn(nameof(CreatePersonalAccessToken_Duplicate_Should_Throw_InvalidResponse))]
    public async Task GetPersonalAccessTokens_Should_ReturnValidResponse()
    {
        IReadOnlyList<PersonalAccessTokenResponse> response = await Fixture.Clients[protocol].GetPersonalAccessTokensAsync();

        response.ShouldNotBeNull();
        response.Count.ShouldBe(1);
        response[0].Name.ShouldBe(CreatePersonalAccessTokenRequest.Name);
        var tokenExpiryDateTimeOffset = DateTimeOffset.UtcNow.AddMicroseconds((double)CreatePersonalAccessTokenRequest.Expiry!);
        response[0].ExpiryAt!.Value.ToUniversalTime().ShouldBe(tokenExpiryDateTimeOffset, TimeSpan.FromMinutes(1));
    }

    [Test]
    [DependsOn(nameof(GetPersonalAccessTokens_Should_ReturnValidResponse))]
    public async Task LoginWithPersonalAccessToken_Should_Be_Successfully()
    {
        var response = await Fixture.Clients[protocol].CreatePersonalAccessTokenAsync(new CreatePersonalAccessTokenRequest
        {
            Name = "test-pat-login",
            Expiry = 100_000_000
        });

        var client = Fixture.CreateClient(protocol);

        var authResponse = await client.LoginWithPersonalAccessToken(new LoginWithPersonalAccessToken
        {
            Token = response!.Token
        });

        authResponse.ShouldNotBeNull();
        authResponse.UserId.ShouldBe(1);
    }

    [Test]
    [DependsOn(nameof(LoginWithPersonalAccessToken_Should_Be_Successfully))]
    public async Task DeletePersonalAccessToken_Should_DeletePersonalAccessToken_Successfully()
    {
        await Should.NotThrowAsync(() => Fixture.Clients[protocol].DeletePersonalAccessTokenAsync(new DeletePersonalAccessTokenRequest
        {
            Name = CreatePersonalAccessTokenRequest.Name
        }));
    }
}