using FluentAssertions;
using FluentAssertions.Common;
using Iggy_SDK_Tests.E2ETests.Fixtures;
using Iggy_SDK_Tests.E2ETests.Fixtures.Bootstraps;
using Iggy_SDK_Tests.Utils;
using Iggy_SDK.Contracts.Http;
using Iggy_SDK.Exceptions;

namespace Iggy_SDK_Tests.E2ETests;


[TestCaseOrderer("Iggy_SDK_Tests.Utils.PriorityOrderer", "Iggy_SDK_Tests")]
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
        await _fixture.HttpSut.Invoking(y =>
                y.CreatePersonalAccessTokenAsync(PATFixtureBootstrap.CreatePersonalAccessTokenRequest)
            ).Should()
            .NotThrowAsync();
        
        // TODO: This code block is commmented bacause TCP implementation is not working properly.
        // var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        // {
        //     await sut.Invoking(x => x.CreatePersonalAccessTokenAsync(PATFixtureBootstrap.CreatePersonalAccessTokenRequest))
        //         .Should()
        //         .NotThrowAsync();
        // })).ToArray();
        //
        // await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(2)]
    public async Task CreatePersonalAccessToken_Duplicate_Should_Throw_InvalidResponse()
    {
        // act & assert
        await _fixture.HttpSut.Invoking(y =>
                y.CreatePersonalAccessTokenAsync(PATFixtureBootstrap.CreatePersonalAccessTokenRequest)
            ).Should()
            .ThrowExactlyAsync<InvalidResponseException>();
        
        // TODO: This code block is commmented bacause TCP implementation is not working properly.
        // var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        // {
        //     await sut.Invoking(x => x.CreatePersonalAccessTokenAsync(PATFixtureBootstrap.CreatePersonalAccessTokenRequest))
        //         .Should()
        //         .ThrowExactlyAsync<InvalidResponseException>();
        // })).ToArray();
        //
        // await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(3)]
    public async Task GetPersonalAccessTokens_Should_ReturnValidResponse()
    {
        // arrange
        var expectedTokenExpiryLocaltime = DateTime.UtcNow.AddMicroseconds((double)PATFixtureBootstrap.CreatePersonalAccessTokenRequest.Expiry!).ToLocalTime();
        var expectedPersonalTokenName = PATFixtureBootstrap.CreatePersonalAccessTokenRequest.Name;
        
        // act
        var response = await _fixture.HttpSut.GetPersonalAccessTokensAsync();
        
        // assert
        response.Should()
            .NotBeNull()
            .And
            .NotBeEmpty()
            .And
            .ContainSingle(x =>
                x.Name.Equals(expectedPersonalTokenName) &&
                x.ExpiryAt!.Value.Date == expectedTokenExpiryLocaltime.Date);
        
        // TODO: This code block is commmented bacause TCP implementation is not working properly.
        // var tasks = _fixture.SubjectsUnderTest.Select( sut => Task.Run(async () =>
        // {
        //     var response = await sut.GetPersonalAccessTokensAsync();
        //     response.Should().NotBeNull();
        //     response.Count.Should().Be(1);
        //     response[0].Name.Should().Be(PATFixtureBootstrap.CreatePersonalAccessTokenRequest.Name);
        //     var tokenExpiryDateTimeOffset = DateTime.UtcNow.AddSeconds((double)PATFixtureBootstrap.CreatePersonalAccessTokenRequest.Expiry!).ToDateTimeOffset();
        //     response[0].Expiry!.Value.Date.Should().Be(tokenExpiryDateTimeOffset.Date);
        // })).ToArray();
        //
        // await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(4)]
    public async Task LoginWithPersonalAccessToken_Should_Be_Successfull()
    {
        // act
        var response = await _fixture.HttpSut.CreatePersonalAccessTokenAsync(new CreatePersonalAccessTokenRequest
        {
            Name = "test-login",
            Expiry = 1726574121
        });

        await _fixture.HttpSut.LogoutUser();
        
        // assert
        await _fixture.HttpSut.Invoking(x => x.LoginWithPersonalAccessToken(new LoginWithPersonalAccessToken
        {
            Token = response!.Token
        }))
        .Should()
        .NotThrowAsync();
        
        // TODO: This code block is commmented bacause TCP implementation is not working properly.
        // var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        // {
        //     var response = await sut.CreatePersonalAccessTokenAsync(new CreatePersonalAccessTokenRequest
        //     {
        //         Name = "test-login",
        //         Expiry = 69420
        //     });
        //     await sut.LogoutUser();
        //     await sut.Invoking(x => x.LoginWithPersonalAccessToken(new LoginWithPersonalAccessToken
        //     {
        //         Token = response!.Token
        //     })).Should().NotThrowAsync();
        // })).ToArray();
        //
        // await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(5)]
    public async Task DeletePersonalAccessToken_Should_DeletePersonalAccessToken_Successfully()
    {
        // act & assert
        await _fixture.HttpSut.Invoking(x => x.DeletePersonalAccessTokenAsync(new DeletePersonalAccessTokenRequest
            {
                Name = PATFixtureBootstrap.CreatePersonalAccessTokenRequest.Name
            }))
            .Should()
            .NotThrowAsync();
        
        // TODO: This code block is commmented bacause TCP implementation is not working properly.
        // var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        // {
        //     await sut.Invoking(x => x.DeletePersonalAccessTokenAsync(new DeletePersonalAccessTokenRequest
        //     {
        //         Name = PATFixtureBootstrap.CreatePersonalAccessTokenRequest.Name
        //     }))
        //     .Should()
        //     .NotThrowAsync();
        // })).ToArray();
        //
        // await Task.WhenAll(tasks);
    }
}