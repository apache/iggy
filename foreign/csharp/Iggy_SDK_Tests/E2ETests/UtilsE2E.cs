using FluentAssertions;
using Iggy_SDK_Tests.E2ETests.Fixtures;
using Iggy_SDK_Tests.Utils;
using Microsoft.VisualStudio.TestPlatform.CrossPlatEngine.Helpers;
using System.Runtime.CompilerServices;

namespace Iggy_SDK_Tests.E2ETests;

[TestCaseOrderer("Iggy_SDK_Tests.Utils.PriorityOrderer", "Iggy_SDK_Tests")]
public sealed class UtilsE2E : IClassFixture<IggyGeneralFixture>
{
    private const string SkipMessage = "TCP implementation needs to be aligned with Iggyrs core changes";
    private readonly IggyGeneralFixture _fixture;

    public UtilsE2E(IggyGeneralFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact, TestPriority(1)]
    public async Task GetStats_Should_ReturnValidResponse()
    {
        // act
        var response = await _fixture.HttpSut.GetStatsAsync();
        
        // assert
        response.Should().NotBeNull();
        response!.MessagesCount.Should().Be(0);
        response.PartitionsCount.Should().Be(0);
        response.StreamsCount.Should().Be(0);
        response.TopicsCount.Should().Be(0);
        
        // TODO: This code block is commented because TCP implementation is not working properly.
        // var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        // {
        //     var response = await sut.GetStatsAsync();
        //     response.Should().NotBeNull();
        //     response!.MessagesCount.Should().Be(0);
        //     response.PartitionsCount.Should().Be(0);
        //     response.StreamsCount.Should().Be(0);
        //     response.TopicsCount.Should().Be(0);
        // })).ToArray();
        //
        // await Task.WhenAll(tasks);
    }
}