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
using Apache.Iggy.IggyClient;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Shouldly;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Tests.Integrations;

public class FetchMessagesTests
{
    private const int MessageCount = 20;
    private const string TopicName = "topic";
    private const string HeadersTopicName = "headers-topic";

    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerAssembly)]
    public required IggyServerFixture Fixture { get; init; }

    private async Task<(IIggyClient client, string streamName)> CreateStreamWithMessages(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var streamName = $"fetch-msg-{Guid.NewGuid():N}";

        await client.CreateStreamAsync(streamName);
        await client.CreateTopicAsync(Identifier.String(streamName), TopicName, 1);
        await client.CreateTopicAsync(Identifier.String(streamName), HeadersTopicName, 1);

        await client.SendMessagesAsync(Identifier.String(streamName),
            Identifier.String(TopicName), Partitioning.None(),
            CreateMessagesWithoutHeader(MessageCount));

        await client.SendMessagesAsync(Identifier.String(streamName),
            Identifier.String(HeadersTopicName), Partitioning.None(),
            CreateMessagesWithHeader(MessageCount));

        return (client, streamName);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task PollMessages_WithNoHeaders_Should_PollMessages_Successfully(Protocol protocol)
    {
        var (client, streamName) = await CreateStreamWithMessages(protocol);

        var response = await client.PollMessagesAsync(new MessageFetchRequest
        {
            Count = 10,
            AutoCommit = true,
            Consumer = Consumer.New(1),
            PartitionId = 0,
            PollingStrategy = PollingStrategy.Next(),
            StreamId = Identifier.String(streamName),
            TopicId = Identifier.String(TopicName)
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
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task PollMessages_InvalidTopic_Should_Throw_InvalidResponse(Protocol protocol)
    {
        var (client, streamName) = await CreateStreamWithMessages(protocol);

        var invalidFetchRequest = new MessageFetchRequest
        {
            Count = 10,
            AutoCommit = true,
            Consumer = Consumer.New(1),
            PartitionId = 0,
            PollingStrategy = PollingStrategy.Next(),
            StreamId = Identifier.String(streamName),
            TopicId = Identifier.Numeric(2137)
        };

        await Should.ThrowAsync<IggyInvalidStatusCodeException>(() =>
            client.PollMessagesAsync(invalidFetchRequest));
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task PollMessages_WithHeaders_Should_PollMessages_Successfully(Protocol protocol)
    {
        var (client, streamName) = await CreateStreamWithMessages(protocol);

        var headersMessageFetchRequest = new MessageFetchRequest
        {
            Count = 10,
            AutoCommit = true,
            Consumer = Consumer.New(1),
            PartitionId = 0,
            PollingStrategy = PollingStrategy.Next(),
            StreamId = Identifier.String(streamName),
            TopicId = Identifier.String(HeadersTopicName)
        };

        var response = await client.PollMessagesAsync(headersMessageFetchRequest);
        response.Messages.Count.ShouldBe(10);
        response.PartitionId.ShouldBe(0);
        response.CurrentOffset.ShouldBe(19u);
        foreach (var responseMessage in response.Messages)
        {
            responseMessage.UserHeaders.ShouldNotBeNull();
            responseMessage.UserHeaders.Count.ShouldBe(2);
            responseMessage.UserHeaders[HeaderKey.FromString("header1")].ToString().ShouldBe("value1");
            responseMessage.UserHeaders[HeaderKey.FromString("header2")].ToInt32().ShouldBeGreaterThan(0);
        }
    }

    private static Message[] CreateMessagesWithoutHeader(int count)
    {
        var messages = new List<Message>();
        for (var i = 0; i < count; i++)
        {
            var dummyJson = $$"""
                              {
                                "userId": {{i + 1}},
                                "id": {{i + 1}},
                                "title": "delete",
                                "completed": false
                              }
                              """;
            messages.Add(new Message(Guid.NewGuid(), Encoding.UTF8.GetBytes(dummyJson)));
        }

        return messages.ToArray();
    }

    private static Message[] CreateMessagesWithHeader(int count)
    {
        var messages = new List<Message>();
        for (var i = 0; i < count; i++)
        {
            var dummyJson = $$"""
                              {
                                "userId": {{i + 1}},
                                "id": {{i + 1}},
                                "title": "delete",
                                "completed": false
                              }
                              """;
            messages.Add(new Message(Guid.NewGuid(), Encoding.UTF8.GetBytes(dummyJson),
                new Dictionary<HeaderKey, HeaderValue>
                {
                    { HeaderKey.FromString("header1"), HeaderValue.FromString("value1") },
                    { HeaderKey.FromString("header2"), HeaderValue.FromInt32(14 + i) }
                }));
        }

        return messages.ToArray();
    }
}
