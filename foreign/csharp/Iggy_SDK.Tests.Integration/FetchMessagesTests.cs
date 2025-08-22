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

using System.Text;
using Apache.Iggy.Contracts;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Headers;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Apache.Iggy.Tests.Integrations.Models;
using Shouldly;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Tests.Integrations;

public class FetchMessagesTests
{
    [ClassDataSource<FetchMessagesFixture>(Shared = SharedType.PerClass)]
    public required FetchMessagesFixture Fixture { get; init; }


    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task PollMessagesTMessage_WithNoHeaders_Should_PollMessages_Successfully(Protocol protocol)
    {
        PolledMessages<DummyMessage> response = await Fixture.Clients[protocol].PollMessagesAsync(
            new MessageFetchRequest
            {
                Count = 10,
                AutoCommit = true,
                Consumer = Consumer.New(1),
                PartitionId = 1,
                PollingStrategy = PollingStrategy.Next(),
                StreamId = Identifier.Numeric(Fixture.StreamId),
                TopicId = Identifier.Numeric(Fixture.TopicDummyRequest.TopicId!.Value)
            }, DummyMessage.DeserializeDummyMessage);

        response.Messages.Count.ShouldBe(10);
        response.PartitionId.ShouldBe(1);
        response.CurrentOffset.ShouldBe(19u);
        uint offset = 0;
        foreach (MessageResponse<DummyMessage> responseMessage in response.Messages)
        {
            responseMessage.UserHeaders.ShouldBeNull();
            responseMessage.Message.Text.ShouldNotBeNullOrEmpty();
            responseMessage.Message.Text.ShouldContain("Dummy message");
            responseMessage.Header.Checksum.ShouldNotBe(0u);
            responseMessage.Header.Id.ShouldNotBe(0u);
            responseMessage.Header.Offset.ShouldBe(offset++);
            responseMessage.Header.PayloadLength.ShouldNotBe(0);
            responseMessage.Header.UserHeadersLength.ShouldBe(0);
        }
    }

    [Test]
    [DependsOn(nameof(PollMessagesTMessage_WithNoHeaders_Should_PollMessages_Successfully))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task PollMessagesTMessage_Should_Throw_InvalidResponse(Protocol protocol)
    {
        var invalidFetchRequest = new MessageFetchRequest
        {
            Count = 10,
            AutoCommit = true,
            Consumer = Consumer.New(1),
            PartitionId = 1,
            PollingStrategy = PollingStrategy.Next(),
            StreamId = Identifier.Numeric(Fixture.StreamId),
            TopicId = Identifier.Numeric(55)
        };
        await Should.ThrowAsync<InvalidResponseException>(() =>
            Fixture.Clients[protocol].PollMessagesAsync(invalidFetchRequest, DummyMessage.DeserializeDummyMessage));
    }

    [Test]
    [DependsOn(nameof(PollMessagesTMessage_Should_Throw_InvalidResponse))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task PollMessages_WithNoHeaders_Should_PollMessages_Successfully(Protocol protocol)
    {
        var response = await Fixture.Clients[protocol].PollMessagesAsync(new MessageFetchRequest
        {
            Count = 10,
            AutoCommit = true,
            Consumer = Consumer.New(1),
            PartitionId = 1,
            PollingStrategy = PollingStrategy.Next(),
            StreamId = Identifier.Numeric(Fixture.StreamId),
            TopicId = Identifier.Numeric(Fixture.TopicRequest.TopicId!.Value)
        });

        response.Messages.Count.ShouldBe(10);
        response.PartitionId.ShouldBe(1);
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
    public async Task PollMessages_Should_Throw_InvalidResponse(Protocol protocol)
    {
        var invalidFetchRequest = new MessageFetchRequest
        {
            Count = 10,
            AutoCommit = true,
            Consumer = Consumer.New(1),
            PartitionId = 1,
            PollingStrategy = PollingStrategy.Next(),
            StreamId = Identifier.Numeric(Fixture.StreamId),
            TopicId = Identifier.Numeric(55)
        };

        await Should.ThrowAsync<InvalidResponseException>(() =>
            Fixture.Clients[protocol].PollMessagesAsync(invalidFetchRequest));
    }

    [Test]
    [DependsOn(nameof(PollMessages_Should_Throw_InvalidResponse))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task PollMessages_WithHeaders_Should_PollMessages_Successfully(Protocol protocol)
    {
        var headersMessageFetchRequest = new MessageFetchRequest
        {
            Count = 10,
            AutoCommit = true,
            Consumer = Consumer.New(1),
            PartitionId = 1,
            PollingStrategy = PollingStrategy.Next(),
            StreamId = Identifier.Numeric(Fixture.StreamId),
            TopicId = Identifier.Numeric(Fixture.HeadersTopicRequest.TopicId!.Value)
        };


        var response = await Fixture.Clients[protocol].PollMessagesAsync(headersMessageFetchRequest);
        response.Messages.Count.ShouldBe(10);
        response.PartitionId.ShouldBe(1);
        response.CurrentOffset.ShouldBe(19u);
        foreach (var responseMessage in response.Messages)
        {
            responseMessage.UserHeaders.ShouldNotBeNull();
            responseMessage.UserHeaders.Count.ShouldBe(2);
            responseMessage.UserHeaders[HeaderKey.New("header1")].ToString().ShouldBe("value1");
            responseMessage.UserHeaders[HeaderKey.New("header2")].ToInt32().ShouldBeGreaterThan(0);
        }
    }

    [Test]
    [DependsOn(nameof(PollMessages_WithHeaders_Should_PollMessages_Successfully))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task PollMessagesTMessage_WithHeaders_Should_PollMessages_Successfully(Protocol protocol)
    {
        var headersMessageFetchRequest = new MessageFetchRequest
        {
            Count = 10,
            AutoCommit = true,
            Consumer = Consumer.New(1),
            PartitionId = 1,
            PollingStrategy = PollingStrategy.Next(),
            StreamId = Identifier.Numeric(Fixture.StreamId),
            TopicId = Identifier.Numeric(Fixture.TopicDummyHeaderRequest.TopicId!.Value)
        };

        PolledMessages<DummyMessage> response = await Fixture.Clients[protocol]
            .PollMessagesAsync(headersMessageFetchRequest, DummyMessage.DeserializeDummyMessage);
        response.Messages.Count.ShouldBe(10);
        response.PartitionId.ShouldBe(1);
        response.CurrentOffset.ShouldBe(19u);
        foreach (MessageResponse<DummyMessage> responseMessage in response.Messages)
        {
            responseMessage.UserHeaders.ShouldNotBeNull();
            responseMessage.UserHeaders.Count.ShouldBe(2);
            responseMessage.UserHeaders[HeaderKey.New("header1")].ToString().ShouldBe("value1");
            responseMessage.UserHeaders[HeaderKey.New("header2")].ToInt32().ShouldBeGreaterThan(0);
        }
    }

    [Test]
    [DependsOn(nameof(PollMessagesTMessage_WithHeaders_Should_PollMessages_Successfully))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task PollMessagesMessage_WithEncryptor_Should_PollMessages_Successfully(Protocol protocol)
    {
        await Fixture.Clients[protocol].SendMessagesAsync(Identifier.Numeric(Fixture.StreamId),
            Identifier.Numeric(Fixture.TopicRequest.TopicId!.Value), Partitioning.None(),
            [new Message(Guid.NewGuid(), "Test message"u8.ToArray())], bytes =>
            {
                Array.Reverse(bytes);
                return bytes;
            });

        var messageFetchRequest = new MessageFetchRequest
        {
            Count = 1,
            AutoCommit = true,
            Consumer = Consumer.New(1),
            PartitionId = 1,
            PollingStrategy = PollingStrategy.Last(),
            StreamId = Identifier.Numeric(Fixture.StreamId),
            TopicId = Identifier.Numeric(Fixture.TopicRequest.TopicId!.Value)
        };

        var response = await Fixture.Clients[protocol].PollMessagesAsync(messageFetchRequest, bytes =>
        {
            Array.Reverse(bytes);
            return bytes;
        }, CancellationToken.None);

        response.Messages.Count.ShouldBe(1);
        Encoding.UTF8.GetString(response.Messages[0].Payload).ShouldBe("Test message");
    }
}