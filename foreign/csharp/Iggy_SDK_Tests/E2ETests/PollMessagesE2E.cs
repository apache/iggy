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
using Apache.Iggy.Kinds;
using Apache.Iggy.Tests.E2ETests.Fixtures;
using Apache.Iggy.Tests.E2ETests.Fixtures.Bootstraps;
using Apache.Iggy.Tests.Utils;
using Apache.Iggy.Tests.Utils.Messages;
using FluentAssertions;

namespace Apache.Iggy.Tests.E2ETests;

[TestCaseOrderer("Apache.Iggy.Tests.Utils.PriorityOrderer", "Apache.Iggy.Tests")]
public sealed class PollMessagesE2E(IggyPollMessagesFixture fixture) : IClassFixture<IggyPollMessagesFixture>
{

    [Fact, TestPriority(1)]
    public async Task PollMessagesTMessage_Should_PollMessages_Successfully()
    {
        
        var tasks = fixture.SubjectsUnderTest.Select(sut => Task.Run(async () =>
        {
            var i = 0;
            await foreach (var msgResponse in sut.Client.PollMessagesAsync(new PollMessagesRequest
                           {
                               Consumer = Consumer.New(1),
                               Count = 10,
                               PartitionId = PollMessagesFixtureBootstrap.PartitionId,
                               PollingStrategy = PollingStrategy.Next(),
                               StreamId = Identifier.Numeric(PollMessagesFixtureBootstrap.StreamId),
                               TopicId = Identifier.Numeric(PollMessagesFixtureBootstrap.TopicId)
                           }, MessageFactory.DeserializeDummyMessage))
            {
                msgResponse.UserHeaders.Should().NotBeNull();
                msgResponse.UserHeaders.Should().HaveCount(PollMessagesFixtureBootstrap.HeadersCount);
                i++;
                if (i == PollMessagesFixtureBootstrap.MessageCount)
                {
                    break;
                }
            }
        
            i.Should().Be(PollMessagesFixtureBootstrap.MessageCount);
        })).ToArray();
        
        await Task.WhenAll(tasks);
    }
    
}