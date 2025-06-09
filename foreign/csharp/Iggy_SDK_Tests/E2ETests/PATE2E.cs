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

using Apache.Iggy.Contracts.Http.Auth;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Tests.E2ETests.Fixtures;
using Apache.Iggy.Tests.E2ETests.Fixtures.Bootstraps;
using Apache.Iggy.Tests.Utils;
using FluentAssertions;
using FluentAssertions.Common;

namespace Apache.Iggy.Tests.E2ETests;


[TestCaseOrderer("Apache.Iggy.Tests.Utils.PriorityOrderer", "Apache.Iggy.Tests")]
public sealed class PATE2E : IClassFixture<IggyPATFixture>
{
    private const string SkipMessage = "TCP implementation needs to be aligned with Iggyrs core changes";
    private readonly IggyPATFixture _fixture;
    
    public PATE2E(IggyPATFixture fixture)
    {
        _fixture = fixture;
    }
    
    [Fact, TestPriority(1)]
    public async Task CreatePersonalAccessToken_HappyPath_Should_CreatePersonalAccessToken_Successfully()
    {
        // act & assert
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            await sut.Invoking(x => x.Client.CreatePersonalAccessTokenAsync(PATFixtureBootstrap.CreatePersonalAccessTokenRequest))
                .Should()
                .NotThrowAsync();
        })).ToArray();
        
        await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(2)]
    public async Task CreatePersonalAccessToken_Duplicate_Should_Throw_InvalidResponse()
    {
        // act & assert
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            await sut.Invoking(x => x.Client.CreatePersonalAccessTokenAsync(PATFixtureBootstrap.CreatePersonalAccessTokenRequest))
                .Should()
                .ThrowExactlyAsync<InvalidResponseException>();
        })).ToArray();
        
        await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(3)]
    public async Task GetPersonalAccessTokens_Should_ReturnValidResponse()
    {
        // act
        var tasks = _fixture.SubjectsUnderTest.Select( sut => Task.Run(async () =>
        {
            var response = await sut.Client.GetPersonalAccessTokensAsync();
            response.Should().NotBeNull();
            response.Count.Should().Be(1);
            response[0].Name.Should().Be(PATFixtureBootstrap.CreatePersonalAccessTokenRequest.Name);
            var tokenExpiryDateTimeOffset = DateTime.UtcNow.AddMicroseconds((double)PATFixtureBootstrap.CreatePersonalAccessTokenRequest.Expiry!).ToDateTimeOffset();
            response[0].ExpiryAt!.Value.Date.Should().Be(tokenExpiryDateTimeOffset.LocalDateTime.Date);
        })).ToArray();
        
        await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(4)]
    public async Task LoginWithPersonalAccessToken_Should_Be_Successfull()
    {
        // act
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            var response = await sut.Client.CreatePersonalAccessTokenAsync(new CreatePersonalAccessTokenRequest
            {
                Name = "test-login",
                Expiry = 69420
            });
            await sut.Client.LogoutUser();
            await sut.Invoking(x => x.Client.LoginWithPersonalAccessToken(new LoginWithPersonalAccessToken
            {
                Token = response!.Token
            })).Should().NotThrowAsync();
        })).ToArray();
        
        await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(5)]
    public async Task DeletePersonalAccessToken_Should_DeletePersonalAccessToken_Successfully()
    {
        // act & assert
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            await sut.Invoking(x => x.Client.DeletePersonalAccessTokenAsync(new DeletePersonalAccessTokenRequest
            {
                Name = PATFixtureBootstrap.CreatePersonalAccessTokenRequest.Name
            }))
            .Should()
            .NotThrowAsync();
        })).ToArray();
        
        await Task.WhenAll(tasks);
    }
}