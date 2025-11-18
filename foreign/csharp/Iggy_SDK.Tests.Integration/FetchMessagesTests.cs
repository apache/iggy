// // Licensed to the Apache Software Foundation (ASF) under one
// // or more contributor license agreements.  See the NOTICE file
// // distributed with this work for additional information
// // regarding copyright ownership.  The ASF licenses this file
// // to you under the Apache License, Version 2.0 (the
// // "License"); you may not use this file except in compliance
// // with the License.  You may obtain a copy of the License at
// //
// //   http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing,
// // software distributed under the License is distributed on an
// // "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// // KIND, either express or implied.  See the License for the
// // specific language governing permissions and limitations
// // under the License.

using Apache.Iggy.Contracts;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Headers;
using Apache.Iggy.Kinds;
using Apache.Iggy.Tests.Integrations.Attributes;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Apache.Iggy.Tests.Integrations.Helpers;
using Shouldly;

namespace Apache.Iggy.Tests.Integrations;

public class FetchMessagesTests
{
    [ClassDataSource<FetchMessagesFixture>(Shared = SharedType.PerClass)]
    public required FetchMessagesFixture Fixture { get; init; }


    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task PollMessages_WithNoHeaders_Should_PollMessages_Successfully(Protocol protocol)
    {
        var response = await Fixture.Clients[protocol].PollMessagesAsync(new MessageFetchRequest
        {
            Count = 10,
            AutoCommit = true,
            Consumer = Consumer.New(1),
            PartitionId = 0,
            PollingStrategy = PollingStrategy.Next(),
            StreamId = Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
            TopicId = Identifier.String(Fixture.TopicRequest.Name)
        });

        response.Messages.Count.ShouldBe(10);
        response.PartitionId.ShouldBe(0);
        response.CurrentOffset.ShouldBe(19u);

        foreach (var responseMessage in response.Messages)
        {
            responseMessage.UserHeaders.ShouldBeNull();
            responseMessage.Payload.ShouldNotBeNull();
            responseMessage.Payload.Length.ShouldBeGreaterThan(0);
        }
    }

    [Test]
    [DependsOn(nameof(PollMessages_WithNoHeaders_Should_PollMessages_Successfully))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task PollMessages_InvalidTopic_Should_Throw_InvalidResponse(Protocol protocol)
    {
        var invalidFetchRequest = new MessageFetchRequest
        {
            Count = 10,
            AutoCommit = true,
            Consumer = Consumer.New(1),
            PartitionId = 0,
            PollingStrategy = PollingStrategy.Next(),
            StreamId = Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
            TopicId = Identifier.Numeric(2137)
        };

        await Should.ThrowAsync<IggyInvalidStatusCodeException>(() =>
            Fixture.Clients[protocol].PollMessagesAsync(invalidFetchRequest));
    }

    [Test]
    [DependsOn(nameof(PollMessages_InvalidTopic_Should_Throw_InvalidResponse))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task PollMessages_WithHeaders_Should_PollMessages_Successfully(Protocol protocol)
    {
        var headersMessageFetchRequest = new MessageFetchRequest
        {
            Count = 10,
            AutoCommit = true,
            Consumer = Consumer.New(1),
            PartitionId = 0,
            PollingStrategy = PollingStrategy.Next(),
            StreamId = Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
            TopicId = Identifier.String(Fixture.TopicHeadersRequest.Name)
        };


        var response = await Fixture.Clients[protocol].PollMessagesAsync(headersMessageFetchRequest);
        response.Messages.Count.ShouldBe(10);
        response.PartitionId.ShouldBe(0);
        response.CurrentOffset.ShouldBe(19u);
        foreach (var responseMessage in response.Messages)
        {
            responseMessage.UserHeaders.ShouldNotBeNull();
            responseMessage.UserHeaders.Count.ShouldBe(2);
            responseMessage.UserHeaders[HeaderKey.New("header1")].ToString().ShouldBe("value1");
            responseMessage.UserHeaders[HeaderKey.New("header2")].ToInt32().ShouldBeGreaterThan(0);
        }
    }
}
