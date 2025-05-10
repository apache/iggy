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

using FluentAssertions;
using Iggy_SDK_Tests.E2ETests.Fixtures;
using Iggy_SDK_Tests.E2ETests.Fixtures.Bootstraps;
using Iggy_SDK_Tests.Utils;
using Iggy_SDK_Tests.Utils.Messages;
using Iggy_SDK.Contracts.Http;
using Iggy_SDK.Exceptions;

namespace Iggy_SDK_Tests.E2ETests;

[TestCaseOrderer("Iggy_SDK_Tests.Utils.PriorityOrderer", "Iggy_SDK_Tests")]
public sealed class SendMessagesE2E : IClassFixture<IggySendMessagesFixture>
{
    private const string SkipMessage = "TCP implementation needs to be aligned with Iggyrs core changes";
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
            MessageFactory.GenerateMessageHeaders(Random.Shared.Next(1, 20)));

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
        await _fixture.HttpSut.Invoking(y => y.SendMessagesAsync(_messageNoHeadersSendRequest))
            .Should()
            .NotThrowAsync();
        
        // TODO: This code block is commmented bacause TCP implementation is not working properly.
        // var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        // {
        //     await sut.Invoking(x => x.SendMessagesAsync(_messageNoHeadersSendRequest))
        //         .Should()
        //         .NotThrowAsync();
        // })).ToArray();
        //
        // await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(2)]
    public async Task SendMessages_NoHeaders_Should_Throw_InvalidResponse()
    {
        // act & assert
        await _fixture.HttpSut.Invoking(y => y.SendMessagesAsync(_invalidMessageNoHeadersSendRequest))
            .Should()
            .ThrowAsync<InvalidResponseException>();
        
        // TODO: This code block is commmented bacause TCP implementation is not working properly.
        // var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        // {
        //     await sut.Invoking(x => x.SendMessagesAsync(_invalidMessageNoHeadersSendRequest))
        //         .Should()
        //         .ThrowAsync<InvalidResponseException>();
        // })).ToArray();
        //
        // await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(3)]
    public async Task SendMessages_WithHeaders_Should_SendMessages_Successfully()
    {
        // act & assert
        await _fixture.HttpSut.Invoking(y => y.SendMessagesAsync(_messageWithHeadersSendRequest))
            .Should()
            .NotThrowAsync();
        
        // TODO: This code block is commmented bacause TCP implementation is not working properly.
        // var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        // {
        //     await sut.Invoking(x => x.SendMessagesAsync(_messageWithHeadersSendRequest))
        //         .Should()
        //         .NotThrowAsync();
        // })).ToArray();
        //
        // await Task.WhenAll(tasks);
    }
    
    [Fact, TestPriority(4)]
    public async Task SendMessages_WithHeaders_Should_Throw_InvalidResponse()
    {
        // act & assert
        await _fixture.HttpSut.Invoking(y => y.SendMessagesAsync(_invalidMessageWithHeadersSendRequest))
            .Should()
            .ThrowAsync<InvalidResponseException>();
        
        // TODO: This code block is commmented bacause TCP implementation is not working properly.
        // var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        // {
        //     await sut.Invoking(x => x.SendMessagesAsync(_invalidMessageWithHeadersSendRequest))
        //         .Should()
        //         .ThrowAsync<InvalidResponseException>();
        // })).ToArray();
        //
        // await Task.WhenAll(tasks);
    }
}