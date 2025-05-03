using FluentAssertions;
using Iggy_SDK;
using Iggy_SDK_Tests.E2ETests.Fixtures;
using Iggy_SDK_Tests.E2ETests.Fixtures.Bootstraps;
using Iggy_SDK_Tests.Utils;
using Iggy_SDK.Exceptions;

namespace Iggy_SDK_Tests.E2ETests;

[TestCaseOrderer("Iggy_SDK_Tests.Utils.PriorityOrderer", "Iggy_SDK_Tests")]
public sealed class TopicsE2E : IClassFixture<IggyTopicFixture>
{
    private const string SkipMessage = "TCP implementation needs to be aligned with Iggyrs core changes";
    private readonly IggyTopicFixture _fixture;
    
    public TopicsE2E(IggyTopicFixture fixture)
    {
        _fixture = fixture;
    }
    
    [Fact, TestPriority(1)]
    public async Task Create_NewTopic_Should_Return_Successfully()
    {
        // act & assert
        await _fixture.HttpSut.Invoking(y => y
                .CreateTopicAsync(
                    Identifier.Numeric((int)TopicsFixtureBootstrap.StreamRequest.StreamId!),
                    TopicsFixtureBootstrap.TopicRequest)
            ).Should()
            .NotThrowAsync();
        
        // TODO: This code block is commmented bacause TCP implementation is not working properly.
        // var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        // {
        //     await sut.Invoking(async x =>
        //          await x.CreateTopicAsync(Identifier.Numeric((int)TopicsFixtureBootstrap.StreamRequest.StreamId!), TopicsFixtureBootstrap.TopicRequest))
        //         .Should()
        //         .NotThrowAsync();
        // })).ToArray();
        // await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(2)]
    public async Task Create_DuplicateTopic_Should_Throw_InvalidResponse()
    {
        // act & assert
        await _fixture.HttpSut.Invoking(y => y
                .CreateTopicAsync(
                    Identifier.Numeric((int)TopicsFixtureBootstrap.StreamRequest.StreamId!),
                    TopicsFixtureBootstrap.TopicRequest)
            ).Should()
            .ThrowExactlyAsync<InvalidResponseException>();
        
        // TODO: This code block is commmented bacause TCP implementation is not working properly.
        // var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        // {
        //     await sut.Invoking(async x =>
        //             await x.CreateTopicAsync(Identifier.Numeric((int)TopicsFixtureBootstrap.StreamRequest.StreamId!), TopicsFixtureBootstrap.TopicRequest))
        //         .Should()
        //         .ThrowExactlyAsync<InvalidResponseException>();
        // })).ToArray();
        //
        // await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(3)]
    public async Task Get_ExistingTopic_Should_ReturnValidResponse()
    {
        // act
        var response = await _fixture.HttpSut
            .GetTopicByIdAsync(
                Identifier.Numeric((int)TopicsFixtureBootstrap.StreamRequest.StreamId!), 
                Identifier.Numeric((int)TopicsFixtureBootstrap.TopicRequest.TopicId!));
        
        // assert
        response.Should().NotBeNull();
        response!.Id.Should().Be(TopicsFixtureBootstrap.TopicRequest.TopicId);
        response.Name.Should().Be(TopicsFixtureBootstrap.TopicRequest.Name);
        response.Partitions.Should().HaveCount(TopicsFixtureBootstrap.TopicRequest.PartitionsCount);
        response.MessageExpiry.Should().Be(TopicsFixtureBootstrap.TopicRequest.MessageExpiry);
        response.Size.Should().Be(0);
        response.MessagesCount.Should().Be(0);
        response.MaxTopicSize.Should().Be(TopicsFixtureBootstrap.TopicRequest.MaxTopicSize);
        response.MessageExpiry.Should().Be(TopicsFixtureBootstrap.TopicRequest.MessageExpiry);
        response.CreatedAt.Year.Should().Be(DateTimeOffset.UtcNow.Year);
        response.CreatedAt.Month.Should().Be(DateTimeOffset.UtcNow.Month);
        response.CreatedAt.Day.Should().Be(DateTimeOffset.UtcNow.Day);
        response.CreatedAt.Minute.Should().Be(DateTimeOffset.UtcNow.Minute);
            
        // TODO: This code block is commmented bacause TCP implementation is not working properly.
        // var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        // {
        //     var response = await sut.GetTopicByIdAsync(Identifier.Numeric((int)TopicsFixtureBootstrap.StreamRequest.StreamId!),
        //         Identifier.Numeric((int)TopicsFixtureBootstrap.TopicRequest.TopicId!));
        //     response.Should().NotBeNull();
        //     response!.Id.Should().Be(TopicsFixtureBootstrap.TopicRequest.TopicId);
        //     response.Name.Should().Be(TopicsFixtureBootstrap.TopicRequest.Name);
        //     response.Partitions.Should().HaveCount(TopicsFixtureBootstrap.TopicRequest.PartitionsCount);
        //     response.MessageExpiry.Should().Be(TopicsFixtureBootstrap.TopicRequest.MessageExpiry);
        //     response.Size.Should().Be(0);
        //     response.MessagesCount.Should().Be(0);
        //     response.MaxTopicSize.Should().Be(TopicsFixtureBootstrap.TopicRequest.MaxTopicSize);
        //     response.MessageExpiry.Should().Be(TopicsFixtureBootstrap.TopicRequest.MessageExpiry);
        //     response.CreatedAt.Year.Should().Be(DateTimeOffset.UtcNow.Year);
        //     response.CreatedAt.Month.Should().Be(DateTimeOffset.UtcNow.Month);
        //     response.CreatedAt.Day.Should().Be(DateTimeOffset.UtcNow.Day);
        //     response.CreatedAt.Minute.Should().Be(DateTimeOffset.UtcNow.Minute);
        // })).ToArray();
        //
        // await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(4)]
    public async Task Update_ExistingTopic_Should_UpdateTopic_Successfully()
    {
        // act
        await _fixture.HttpSut.Invoking(y => y
            .UpdateTopicAsync(
                Identifier.Numeric((int)TopicsFixtureBootstrap.StreamRequest.StreamId!), 
                Identifier.Numeric((int)TopicsFixtureBootstrap.TopicRequest.TopicId!),
                TopicsFixtureBootstrap.UpdateTopicRequest)
            ).Should()
            .NotThrowAsync();
        
        var result = await _fixture.HttpSut.
            GetTopicByIdAsync(
                Identifier.Numeric((int)TopicsFixtureBootstrap.StreamRequest.StreamId!),
                Identifier.Numeric((int)TopicsFixtureBootstrap.TopicRequest.TopicId!));
        
        // assert
        result.Should().NotBeNull();
        result!.Name.Should().Be(TopicsFixtureBootstrap.UpdateTopicRequest.Name);
        result.MessageExpiry.Should().Be(TopicsFixtureBootstrap.UpdateTopicRequest.MessageExpiry);
        
        // TODO: This code block is commmented bacause TCP implementation is not working properly.
        // var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        // {
        //     await sut.Invoking(async x =>
        //             await x.UpdateTopicAsync(Identifier.Numeric((int)TopicsFixtureBootstrap.StreamRequest.StreamId!),
        //                 Identifier.Numeric((int)TopicsFixtureBootstrap.TopicRequest.TopicId!), TopicsFixtureBootstrap.UpdateTopicRequest))
        //         .Should()
        //         .NotThrowAsync();
        //
        //     var result = await sut.GetTopicByIdAsync(Identifier.Numeric((int)TopicsFixtureBootstrap.StreamRequest.StreamId!),
        //         Identifier.Numeric((int)TopicsFixtureBootstrap.TopicRequest.TopicId!));
        //     result.Should().NotBeNull();
        //     result!.Name.Should().Be(TopicsFixtureBootstrap.UpdateTopicRequest.Name);
        //     result.MessageExpiry.Should().Be(TopicsFixtureBootstrap.UpdateTopicRequest.MessageExpiry);
        // })).ToArray();
        //
        // await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(5)]
    public async Task Delete_ExistingTopic_Should_DeleteTopic_Successfully()
    {
        // act & assert
        await _fixture.HttpSut.Invoking(y => y
            .DeleteTopicAsync(
                Identifier.Numeric((int)TopicsFixtureBootstrap.StreamRequest.StreamId!),
                Identifier.Numeric((int)TopicsFixtureBootstrap.TopicRequest.TopicId!))
            ).Should()
            .NotThrowAsync();
        
        // TODO: This code block is commmented bacause TCP implementation is not working properly.
        // var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        // {
        //     await sut.Invoking(async x =>
        //             await x.DeleteTopicAsync(Identifier.Numeric((int)TopicsFixtureBootstrap.StreamRequest.StreamId!),
        //                 Identifier.Numeric((int)TopicsFixtureBootstrap.TopicRequest.TopicId!)))
        //         .Should()
        //         .NotThrowAsync();
        // })).ToArray();
        //
        // await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(6)]
    public async Task Delete_NonExistingTopic_Should_Throw_InvalidResponse()
    {
        // act & assert
        await _fixture.HttpSut.Invoking(y => y
            .DeleteTopicAsync(
                Identifier.Numeric((int)TopicsFixtureBootstrap.StreamRequest.StreamId!),
                Identifier.Numeric((int)TopicsFixtureBootstrap.TopicRequest.TopicId!))
            ).Should()
            .ThrowExactlyAsync<InvalidResponseException>();
        
        // TODO: This code block is commmented bacause TCP implementation is not working properly.
        // var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        // {
        //     await sut.Invoking(async x =>
        //             await x.DeleteTopicAsync(Identifier.Numeric((int)TopicsFixtureBootstrap.StreamRequest.StreamId!),
        //                 Identifier.Numeric((int)TopicsFixtureBootstrap.TopicRequest.TopicId!)))
        //         .Should()
        //         .ThrowExactlyAsync<InvalidResponseException>();
        // })).ToArray();
        //
        // await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(7)]
    public async Task Get_NonExistingTopic_Should_Throw_InvalidResponse()
    {
        // act & assert
        await _fixture.HttpSut.Invoking(y => y
            .GetTopicByIdAsync(
                Identifier.Numeric((int)TopicsFixtureBootstrap.StreamRequest.StreamId!),
                Identifier.Numeric((int)TopicsFixtureBootstrap.TopicRequest.TopicId!))
            ).Should()
            .ThrowExactlyAsync<InvalidResponseException>();
        
        // TODO: This code block is commmented bacause TCP implementation is not working properly.
        // var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        // {
        //     await sut.Invoking(async x =>
        //             await x.GetTopicByIdAsync(Identifier.Numeric((int)TopicsFixtureBootstrap.StreamRequest.StreamId!),
        //                 Identifier.Numeric((int)TopicsFixtureBootstrap.TopicRequest.TopicId!)))
        //         .Should()
        //         .ThrowExactlyAsync<InvalidResponseException>();
        // })).ToArray();
        //
        // await Task.WhenAll(tasks);
    }
}