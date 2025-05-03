using FluentAssertions;
using Iggy_SDK_Tests.E2ETests.Fixtures;
using Iggy_SDK_Tests.E2ETests.Fixtures.Bootstraps;
using Iggy_SDK_Tests.Utils;
using Iggy_SDK_Tests.Utils.Messages;
using Iggy_SDK.Contracts.Http;
using Iggy_SDK.Enums;
using Iggy_SDK.Exceptions;

namespace Iggy_SDK_Tests.E2ETests;

[TestCaseOrderer("Iggy_SDK_Tests.Utils.PriorityOrderer", "Iggy_SDK_Tests")]
public sealed class FetchMessagesE2E : IClassFixture<IggyFetchMessagesFixture>
{
    private const string SkipMessage = "TCP implementation needs to be aligned with Iggyrs core changes";
    private readonly IggyFetchMessagesFixture _fixture;

    private static readonly MessageFetchRequest _messageFetchRequest =
        MessageFactory.CreateMessageFetchRequestConsumer(10, FetchMessagesFixtureBootstrap.StreamId,
            FetchMessagesFixtureBootstrap.TopicId, FetchMessagesFixtureBootstrap.PartitionId);

    private static readonly MessageFetchRequest _headersMessageFetchRequest =
        MessageFactory.CreateMessageFetchRequestConsumer(10, FetchMessagesFixtureBootstrap.StreamId,
            FetchMessagesFixtureBootstrap.HeadersTopicId, FetchMessagesFixtureBootstrap.PartitionId);

    private static readonly MessageFetchRequest _invalidFetchRequest =
        MessageFactory.CreateMessageFetchRequestConsumer(10, FetchMessagesFixtureBootstrap.InvalidStreamId,
            FetchMessagesFixtureBootstrap.InvalidTopicId, FetchMessagesFixtureBootstrap.PartitionId);
    public FetchMessagesE2E(IggyFetchMessagesFixture fixture)
    {
        _fixture = fixture;
    }
    
    [Fact, TestPriority(1)]
    public async Task PollMessagesTMessage_WithNoHeaders_Should_PollMessages_Successfully()
    {
        // act
        var response = await _fixture.HttpSut
            .FetchMessagesAsync(_messageFetchRequest, MessageFactory.DeserializeDummyMessage);
        
        // assert
        response.PartitionId.Should()
            .Be(FetchMessagesFixtureBootstrap.PartitionId);
        
        response.CurrentOffset.Should().Be(19);
        
        response.Messages.Should()
            .NotBeEmpty()
            .And
            .HaveCount(10);
        
        response.Messages.Should()
            .OnlyContain(x => x.State == MessageState.Available);
        
        // TODO: This code block is commmented bacause TCP implementation is not working properly.
        // var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        // {
        //     var response = await sut.FetchMessagesAsync(_messageFetchRequest, MessageFactory.DeserializeDummyMessage);
        //     response.Messages.Count.Should().Be(10);
        //     response.PartitionId.Should().Be(FetchMessagesFixtureBootstrap.PartitionId);
        //     response.CurrentOffset.Should().Be(19);
        //     foreach (var responseMessage in response.Messages)
        //     {
        //         responseMessage.Headers.Should().BeNull();
        //         responseMessage.State.Should().Be(MessageState.Available);
        //     }
        // })).ToArray();
        //
        // await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(2)]
    public async Task PollMessagesTMessage_Should_Throw_InvalidResponse()
    {
        // act & assert
        await _fixture.HttpSut.Invoking(y =>
                y.FetchMessagesAsync(_invalidFetchRequest, MessageFactory.DeserializeDummyMessage)
                ).Should()
            .ThrowExactlyAsync<InvalidResponseException>();
        
        // TODO: This code block is commmented bacause TCP implementation is not working properly.
        // var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        // {
        //     await sut.Invoking(x => x.FetchMessagesAsync(_invalidFetchRequest, MessageFactory.DeserializeDummyMessage))
        //         .Should()
        //         .ThrowExactlyAsync<InvalidResponseException>();
        // })).ToArray();
        //
        // await Task.WhenAll(tasks);
    }

    [Fact, TestPriority(3)]
    public async Task PollMessages_WithNoHeaders_Should_PollMessages_Successfully()
    {
        // act
        var response = await _fixture.HttpSut.FetchMessagesAsync(_messageFetchRequest);
        
        // assert
        response.PartitionId.Should()
            .Be(FetchMessagesFixtureBootstrap.PartitionId);
        
        response.CurrentOffset.Should().Be(19);
        
        response.Messages.Should()
            .NotBeNull()
            .And
            .NotBeEmpty()
            .And
            .HaveCount(10)
            .And
            .OnlyContain(x =>
                x.State == MessageState.Available &&
                x.Headers == null);
        
        // TODO: This code block is commmented bacause TCP implementation is not working properly.
        // var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        // {
        //     var response = await sut.FetchMessagesAsync(_messageFetchRequest);
        //     response.Messages.Count.Should().Be(10);
        //     response.PartitionId.Should().Be(FetchMessagesFixtureBootstrap.PartitionId);
        //     response.CurrentOffset.Should().Be(19);
        //     foreach (var responseMessage in response.Messages)
        //     {
        //         responseMessage.Headers.Should().BeNull();
        //         responseMessage.State.Should().Be(MessageState.Available);
        //     }
        // })).ToArray();
        //
        // await Task.WhenAll(tasks);
    }

    [Fact, TestPriority(4)]
    public async Task PollMessages_Should_Throw_InvalidResponse()
    {
        // act & assert
        await _fixture.HttpSut.Invoking(y => y.FetchMessagesAsync(_invalidFetchRequest))
            .Should()
            .ThrowExactlyAsync<InvalidResponseException>();
        
        // TODO: This code block is commmented bacause TCP implementation is not working properly.
        // var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        // {
        //     await sut.Invoking(x => x.FetchMessagesAsync(_invalidFetchRequest))
        //         .Should()
        //         .ThrowExactlyAsync<InvalidResponseException>();
        // })).ToArray();
        //
        // await Task.WhenAll(tasks);
    }

    [Fact, TestPriority(5)]
    public async Task PollMessages_WithHeaders_Should_PollMessages_Successfully()
    {
        // act
        var response = await _fixture.HttpSut.FetchMessagesAsync(_headersMessageFetchRequest);
        
        // assert
        response.PartitionId.Should()
            .Be(FetchMessagesFixtureBootstrap.PartitionId);
        
        response.CurrentOffset.Should().Be(19);
        
        response.Messages
            .Should()
            .NotBeNull()
            .And
            .NotBeEmpty()
            .And
            .HaveCount(10)
            .And
            .OnlyContain(x =>
                x.State == MessageState.Available &&
                x.Headers != null &&
                x.Headers.Count == 3);
        
        // TODO: This code block is commmented bacause TCP implementation is not working properly.
        // var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        // {
        //     var response = await sut.FetchMessagesAsync(_headersMessageFetchRequest);
        //     response.Messages.Count.Should().Be(10);
        //     response.PartitionId.Should().Be(FetchMessagesFixtureBootstrap.PartitionId);
        //     response.CurrentOffset.Should().Be(19);
        //     foreach (var responseMessage in response.Messages)
        //     {
        //         responseMessage.Headers.Should().NotBeNull();
        //         responseMessage.State.Should().Be(MessageState.Available);
        //         responseMessage.Headers!.Count.Should().Be(3);
        //     }
        // })).ToArray();
        //
        // await Task.WhenAll(tasks);
    }

    [Fact, TestPriority(6)]
    public async Task PollMessagesTMessage_WithHeaders_Should_PollMessages_Successfully()
    {
        // act
        var response = await _fixture.HttpSut
            .FetchMessagesAsync(_headersMessageFetchRequest, MessageFactory.DeserializeDummyMessage);
        
        // assert
        response.PartitionId.Should()
            .Be(FetchMessagesFixtureBootstrap.PartitionId);
        
        response.CurrentOffset.Should().Be(19);
        
        response.Messages
            .Should()
            .NotBeNull()
            .And
            .NotBeEmpty()
            .And
            .HaveCount(10)
            .And
            .OnlyContain(x =>
                x.State == MessageState.Available &&
                x.Headers != null &&
                x.Headers.Count == 3);
        
        // TODO: This code block is commmented bacause TCP implementation is not working properly.
        // var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        // {
        //     var response = await sut.FetchMessagesAsync(_headersMessageFetchRequest, MessageFactory.DeserializeDummyMessage);
        //     response.Messages.Count.Should().Be(10);
        //     response.PartitionId.Should().Be(FetchMessagesFixtureBootstrap.PartitionId);
        //     response.CurrentOffset.Should().Be(19);
        //     foreach (var responseMessage in response.Messages)
        //     {
        //         responseMessage.Headers.Should().NotBeNull();
        //         responseMessage.State.Should().Be(MessageState.Available);
        //         responseMessage.Headers!.Count.Should().Be(3);
        //     }
        // })).ToArray();
        //
        // await Task.WhenAll(tasks);
    }
}