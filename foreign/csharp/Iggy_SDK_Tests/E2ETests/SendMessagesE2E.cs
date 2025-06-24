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
public sealed class SendMessagesE2E : IClassFixture<IggySendMessagesFixture>
{
    private const string SkipMessage = "TCP implementation needs to be aligned with Iggy core changes";
    private readonly IggySendMessagesFixture _fixture;

    private readonly MessageSendRequest _messageNoHeadersSendRequest;
    private readonly MessageSendRequest _messageWithHeadersSendRequest;
    private readonly MessageSendRequest _invalidMessageNoHeadersSendRequest;
    private readonly MessageSendRequest _invalidMessageWithHeadersSendRequest;

    public SendMessagesE2E(IggySendMessagesFixture fixture)
    {
        _fixture = fixture;

        var messageWithHeaders = MessageFactory.GenerateDummyMessages(
            Random.Shared.Next(20, 50),
            Random.Shared.Next(69, 420),
            MessageFactory.GenerateMessageHeaders(2));

        _messageNoHeadersSendRequest = MessageFactory.CreateMessageSendRequest(SendMessagesFixtureBootstrap.StreamId,
            SendMessagesFixtureBootstrap.TopicId, SendMessagesFixtureBootstrap.PartitionId);
        _invalidMessageNoHeadersSendRequest = MessageFactory.CreateMessageSendRequest(SendMessagesFixtureBootstrap.InvalidStreamId,
            SendMessagesFixtureBootstrap.InvalidTopicId, SendMessagesFixtureBootstrap.PartitionId);

        _messageWithHeadersSendRequest = MessageFactory.CreateMessageSendRequest(SendMessagesFixtureBootstrap.StreamId,
            SendMessagesFixtureBootstrap.TopicId, SendMessagesFixtureBootstrap.PartitionId, messageWithHeaders);
        _invalidMessageWithHeadersSendRequest = MessageFactory.CreateMessageSendRequest(SendMessagesFixtureBootstrap.InvalidStreamId,
            SendMessagesFixtureBootstrap.InvalidTopicId, SendMessagesFixtureBootstrap.PartitionId, messageWithHeaders);
    }

    [Fact, TestPriority(1)]
    public async Task SendMessages_NoHeaders_Should_SendMessages_Successfully()
    {
        // act & assert
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            await sut.Invoking(x => x.Client.SendMessagesAsync(_messageNoHeadersSendRequest))
                .Should()
                .NotThrowAsync();
        })).ToArray();

        await Task.WhenAll(tasks);
    }

    [Fact, TestPriority(2)]
    public async Task SendMessages_NoHeaders_Should_Throw_InvalidResponse()
    {
        // act & assert
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            await sut.Invoking(x => x.Client.SendMessagesAsync(_invalidMessageNoHeadersSendRequest))
                .Should()
                .ThrowAsync<InvalidResponseException>();
        })).ToArray();

        await Task.WhenAll(tasks);
    }

    [Fact, TestPriority(3)]
    public async Task SendMessages_WithHeaders_Should_SendMessages_Successfully()
    {
        // act & assert
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            await sut.Invoking(x => x.Client.SendMessagesAsync(_messageWithHeadersSendRequest))
                .Should()
                .NotThrowAsync();
        })).ToArray();

        await Task.WhenAll(tasks);
    }

    [Fact, TestPriority(4)]
    public async Task SendMessages_WithHeaders_Should_Throw_InvalidResponse()
    {
        // act & assert
        var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            await sut.Invoking(x => x.Client.SendMessagesAsync(_invalidMessageWithHeadersSendRequest))
                .Should()
                .ThrowAsync<InvalidResponseException>();
        })).ToArray();

        await Task.WhenAll(tasks);
    }
}
