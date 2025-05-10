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
using Iggy_SDK;
using Iggy_SDK_Tests.E2ETests.Fixtures;
using Iggy_SDK_Tests.E2ETests.Fixtures.Bootstraps;
using Iggy_SDK_Tests.Utils;
using Iggy_SDK_Tests.Utils.Messages;
using Iggy_SDK.Contracts.Http;
using Iggy_SDK.Enums;
using Iggy_SDK.Kinds;

namespace Iggy_SDK_Tests.E2ETests;

[TestCaseOrderer("Iggy_SDK_Tests.Utils.PriorityOrderer", "Iggy_SDK_Tests")]
public sealed class PollMessagesE2E(IggyPollMessagesFixture fixture) : IClassFixture<IggyPollMessagesFixture>
{
    private const string SkipMessage = "TCP implementation needs to be aligned with Iggyrs core changes";

    [Fact, TestPriority(1)]
    public async Task PollMessagesTMessage_Should_PollMessages_Successfully()
    {
        // arrange
        var pollMessageRequest = new PollMessagesRequest
        {
            Consumer = Consumer.New(1),
            Count = 10,
            PartitionId = PollMessagesFixtureBootstrap.PartitionId,
            PollingStrategy = PollingStrategy.Next(),
            StreamId = Identifier.Numeric(PollMessagesFixtureBootstrap.StreamId),
            TopicId = Identifier.Numeric(PollMessagesFixtureBootstrap.TopicId)
        };
        
        // act
        var messagesCount = 0;
        await foreach (var msgResponse in fixture.HttpSut.PollMessagesAsync(
                           pollMessageRequest,
                           MessageFactory.DeserializeDummyMessage)
                      )
        {
            msgResponse.Headers.Should()
                .NotBeNull()
                .And
                .NotBeEmpty()
                .And
                .HaveCount(PollMessagesFixtureBootstrap.HeadersCount);
            
            msgResponse.State.Should().Be(MessageState.Available);
            
            messagesCount++;
            
            if (messagesCount == PollMessagesFixtureBootstrap.MessageCount)
            {
                break;
            }
        }
        
        messagesCount.Should().Be(PollMessagesFixtureBootstrap.MessageCount);
        
        // TODO: This code block is commmented bacause TCP implementation is not working properly.
        // var tasks = _fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        // {
        //     var i = 0;
        //     await foreach (var msgResponse in sut.PollMessagesAsync(new PollMessagesRequest
        //                    {
        //                        Consumer = Consumer.New(1),
        //                        Count = 10,
        //                        PartitionId = PollMessagesFixtureBootstrap.PartitionId,
        //                        PollingStrategy = PollingStrategy.Next(),
        //                        StreamId = Identifier.Numeric(PollMessagesFixtureBootstrap.StreamId),
        //                        TopicId = Identifier.Numeric(PollMessagesFixtureBootstrap.TopicId)
        //                    }, MessageFactory.DeserializeDummyMessage))
        //     {
        //         msgResponse.Headers.Should().NotBeNull();
        //         msgResponse.Headers.Should().HaveCount(PollMessagesFixtureBootstrap.HeadersCount);
        //         msgResponse.State.Should().Be(MessageState.Available);
        //         i++;
        //         if (i == PollMessagesFixtureBootstrap.MessageCount)
        //         {
        //             break;
        //         }
        //     }
        //
        //     i.Should().Be(PollMessagesFixtureBootstrap.MessageCount);
        // })).ToArray();
        //
        // await Task.WhenAll(tasks);
    }
    
}