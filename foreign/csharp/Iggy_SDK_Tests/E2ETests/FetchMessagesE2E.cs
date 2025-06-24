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

using Apache.Iggy.Contracts.Http;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Tests.E2ETests.Fixtures;
using Apache.Iggy.Tests.E2ETests.Fixtures.Bootstraps;
using Apache.Iggy.Tests.Utils;
using Apache.Iggy.Tests.Utils.Messages;
using FluentAssertions;

namespace Apache.Iggy.Tests.E2ETests;

[TestCaseOrderer("Apache.Iggy.Tests.Utils.PriorityOrderer", "Apache.Iggy.Tests")]
public sealed class FetchMessagesE2E : IClassFixture<IggyFetchMessagesFixture>
{
    private const string SkipMessage = "TCP implementation needs to be aligned with Iggy core changes";
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
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            var response = await sut.Client.FetchMessagesAsync(_messageFetchRequest, MessageFactory.DeserializeDummyMessage);
            response.Messages.Count.Should().Be(10);
            response.PartitionId.Should().Be(FetchMessagesFixtureBootstrap.PartitionId);
            response.CurrentOffset.Should().Be(19);
            foreach (var responseMessage in response.Messages)
            {
                responseMessage.UserHeaders.Should().BeNull();
                responseMessage.Message.Text.Should().NotBeNullOrEmpty();
            }
        })).ToArray();

        await Task.WhenAll(tasks);
    }

    [Fact, TestPriority(2)]
    public async Task PollMessagesTMessage_Should_Throw_InvalidResponse()
    {
        // act & assert
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            await sut.Invoking(x => x.Client.FetchMessagesAsync(_invalidFetchRequest, MessageFactory.DeserializeDummyMessage))
                .Should()
                .ThrowExactlyAsync<InvalidResponseException>();
        })).ToArray();

        await Task.WhenAll(tasks);
    }

    [Fact, TestPriority(3)]
    public async Task PollMessages_WithNoHeaders_Should_PollMessages_Successfully()
    {
        // act
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            var response = await sut.Client.FetchMessagesAsync(_messageFetchRequest);
            response.Messages.Count.Should().Be(10);
            response.PartitionId.Should().Be(FetchMessagesFixtureBootstrap.PartitionId);
            response.CurrentOffset.Should().Be(19);
            foreach (var responseMessage in response.Messages)
            {
                responseMessage.UserHeaders.Should().BeNull();
                responseMessage.Payload.Should().NotBeNull();
                responseMessage.Payload.Length.Should().BeGreaterThan(0);
            }
        })).ToArray();

        await Task.WhenAll(tasks);
    }

    [Fact, TestPriority(4)]
    public async Task PollMessages_Should_Throw_InvalidResponse()
    {
        // act & assert
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            await sut.Invoking(x => x.Client.FetchMessagesAsync(_invalidFetchRequest))
                .Should()
                .ThrowExactlyAsync<InvalidResponseException>();
        })).ToArray();

        await Task.WhenAll(tasks);
    }

    [Fact, TestPriority(5)]
    public async Task PollMessages_WithHeaders_Should_PollMessages_Successfully()
    {
        // act
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            var response = await sut.Client.FetchMessagesAsync(_headersMessageFetchRequest);
            response.Messages.Count.Should().Be(10);
            response.PartitionId.Should().Be(FetchMessagesFixtureBootstrap.PartitionId);
            response.CurrentOffset.Should().Be(19);
            foreach (var responseMessage in response.Messages)
            {
                responseMessage.UserHeaders.Should().NotBeNull();
                responseMessage.UserHeaders!.Count.Should().Be(3);
            }
        })).ToArray();

        await Task.WhenAll(tasks);
    }

    [Fact, TestPriority(6)]
    public async Task PollMessagesTMessage_WithHeaders_Should_PollMessages_Successfully()
    {
        // act
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            var response = await sut.Client.FetchMessagesAsync(_headersMessageFetchRequest, MessageFactory.DeserializeDummyMessage);
            response.Messages.Count.Should().Be(10);
            response.PartitionId.Should().Be(FetchMessagesFixtureBootstrap.PartitionId);
            response.CurrentOffset.Should().Be(19);
            foreach (var responseMessage in response.Messages)
            {
                responseMessage.UserHeaders.Should().NotBeNull();
                responseMessage.UserHeaders!.Count.Should().Be(3);
            }
        })).ToArray();

        await Task.WhenAll(tasks);
    }
}
