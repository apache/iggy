using FluentAssertions;
using Iggy_SDK_Tests.E2ETests.Fixtures;
using Iggy_SDK_Tests.E2ETests.Fixtures.Bootstraps;
using Iggy_SDK_Tests.Utils;
using Iggy_SDK_Tests.Utils.Offset;
using Iggy_SDK.Contracts.Http;

namespace Iggy_SDK_Tests.E2ETests;

[TestCaseOrderer("Iggy_SDK_Tests.Utils.PriorityOrderer", "Iggy_SDK_Tests")]
public sealed class OffsetE2E : IClassFixture<IggyOffsetFixture>
{
    private const string SkipMessage = "TCP implementation needs to be aligned with Iggyrs core changes";
    private readonly IggyOffsetFixture _fixture;
    private readonly StoreOffsetRequest _storeOffsetIndividualConsumer;
    private readonly OffsetRequest _offsetIndividualConsumer;

    private const int GET_INDIVIDUAL_CONSUMER_ID = 1;
    private const int GET_PARTITION_ID = 1;
    private const ulong GET_OFFSET = 0;

    public OffsetE2E(IggyOffsetFixture fixture)
    {
        _fixture = fixture;
        _storeOffsetIndividualConsumer = OffsetFactory.CreateOffsetContract(
            (int)OffsetFixtureBootstrap.StreamRequest.StreamId!, (int)OffsetFixtureBootstrap.TopicRequest.TopicId!, GET_INDIVIDUAL_CONSUMER_ID, GET_OFFSET,
            GET_PARTITION_ID);
        _offsetIndividualConsumer = OffsetFactory.CreateOffsetRequest((int)OffsetFixtureBootstrap.StreamRequest.StreamId,
            (int)OffsetFixtureBootstrap.TopicRequest.TopicId, GET_PARTITION_ID, GET_INDIVIDUAL_CONSUMER_ID);
    }

    [Fact, TestPriority(1)]
    public async Task StoreOffset_IndividualConsumer_Should_StoreOffset_Successfully()
    {
        // act & assert
        await _fixture.HttpSut.Invoking(y =>
                y.StoreOffsetAsync(_storeOffsetIndividualConsumer)
            ).Should()
            .NotThrowAsync();
        
        // TODO: This code block is commmented bacause TCP implementation is not working properly.
        // var tasks = _fixture.SubjectsUnderTest.Select( sut => Task.Run(async () =>
        // {
        //     await sut.Invoking(x => x.StoreOffsetAsync(_storeOffsetIndividualConsumer))
        //         .Should()
        //         .NotThrowAsync();
        // })).ToArray();
        //
        // await Task.WhenAll(tasks);
    }

    [Fact, TestPriority(2)]
    public async Task GetOffset_IndividualConsumer_Should_GetOffset_Successfully()
    {
        // act
        var offset = await _fixture.HttpSut.GetOffsetAsync(_offsetIndividualConsumer);
        
        // assert
        offset.Should().NotBeNull();
        offset!.StoredOffset.Should().Be(_storeOffsetIndividualConsumer.Offset);
        
        // TODO: This code block is commmented bacause TCP implementation is not working properly.
        // var tasks = _fixture.SubjectsUnderTest.Select( sut => Task.Run(async () =>
        // {
        //     var offset = await sut.GetOffsetAsync(_offsetIndividualConsumer);
        //     offset.Should().NotBeNull();
        //     offset!.StoredOffset.Should().Be(_storeOffsetIndividualConsumer.Offset);
        // })).ToArray();
        //
        // await Task.WhenAll(tasks);
    }
}