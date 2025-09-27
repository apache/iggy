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
using Apache.Iggy.Contracts.Http;
using Apache.Iggy.Enums;
using Apache.Iggy.Headers;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Messages;
using Apache.Iggy.Tests.Integrations.Helpers;
using Apache.Iggy.Tests.Integrations.Models;
using TUnit.Core.Interfaces;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Tests.Integrations.Fixtures;

public class FetchMessagesFixture : IAsyncInitializer
{
    internal readonly CreateTopicRequest HeadersTopicRequest = TopicFactory.CreateTopic("HeadersTopic");
    internal readonly int MessageCount = 20;
    internal readonly string StreamId = "FetchMessagesStream";
    internal readonly CreateTopicRequest TopicDummyHeaderRequest = TopicFactory.CreateTopic("DummyHeaderTopic");
    internal readonly CreateTopicRequest TopicDummyRequest = TopicFactory.CreateTopic("DummyTopic");
    internal readonly CreateTopicRequest TopicRequest = TopicFactory.CreateTopic("Topic");

    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerAssembly)]
    public required IggyServerFixture IggyServerFixture { get; init; }

    public Dictionary<Protocol, IIggyClient> Clients { get; set; } = new();

    public async Task InitializeAsync()
    {
        Clients = await IggyServerFixture.CreateClients();
        foreach (KeyValuePair<Protocol, IIggyClient> client in Clients)
        {
            var streamId = StreamId.GetWithProtocol(client.Key);
            var request = new MessageSendRequest
            {
                StreamId = Identifier.String(streamId),
                TopicId = Identifier.String(TopicRequest.Name),
                Partitioning = Partitioning.None(),
                Messages = CreateMessagesWithoutHeader(MessageCount)
            };

            var requestWithHeaders = new MessageSendRequest
            {
                StreamId = Identifier.String(streamId),
                TopicId = Identifier.String(HeadersTopicRequest.Name),
                Partitioning = Partitioning.None(),
                Messages = CreateMessagesWithHeader(MessageCount)
            };

            var requestDummyMessage = new MessageSendRequest<DummyMessage>
            {
                StreamId = Identifier.String(streamId),
                TopicId = Identifier.String(TopicDummyRequest.Name),
                Partitioning = Partitioning.None(),
                Messages = CreateDummyMessagesWithoutHeader(MessageCount)
            };

            var requestDummyMessageWithHeaders = new MessageSendRequest<DummyMessage>
            {
                StreamId = Identifier.String(streamId),
                TopicId = Identifier.String(TopicDummyHeaderRequest.Name),
                Partitioning = Partitioning.None(),
                Messages = CreateDummyMessagesWithoutHeader(MessageCount)
            };

            await client.Value.CreateStreamAsync(streamId);
            await client.Value.CreateTopicAsync(Identifier.String(streamId), TopicRequest.Name,
                TopicRequest.PartitionsCount,
                topicId: TopicRequest.TopicId);
            await client.Value.CreateTopicAsync(Identifier.String(streamId), TopicDummyRequest.Name,
                TopicDummyRequest.PartitionsCount, topicId: TopicDummyRequest.TopicId);
            await client.Value.CreateTopicAsync(Identifier.String(streamId), HeadersTopicRequest.Name,
                HeadersTopicRequest.PartitionsCount, topicId: HeadersTopicRequest.TopicId);
            await client.Value.CreateTopicAsync(Identifier.String(streamId), TopicDummyHeaderRequest.Name,
                TopicDummyHeaderRequest.PartitionsCount, topicId: TopicDummyHeaderRequest.TopicId);

            await client.Value.SendMessagesAsync(request);
            await client.Value.SendMessagesAsync(requestDummyMessage, message => message.SerializeDummyMessage());
            await client.Value.SendMessagesAsync(requestWithHeaders);
            await client.Value.SendMessagesAsync(requestDummyMessageWithHeaders,
                message => message.SerializeDummyMessage(),
                headers: new Dictionary<HeaderKey, HeaderValue>
                {
                    { HeaderKey.New("header1"), HeaderValue.FromString("value1") },
                    { HeaderKey.New("header2"), HeaderValue.FromInt32(14) }
                });
        }
    }

    private static List<Message> CreateMessagesWithoutHeader(int count)
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

        return messages;
    }

    private static List<DummyMessage> CreateDummyMessagesWithoutHeader(int count)
    {
        var messages = new List<DummyMessage>();
        for (var i = 0; i < count; i++)
        {
            messages.Add(new DummyMessage
            {
                Text = $"Dummy message {i}",
                Id = i
            });
        }

        return messages;
    }

    private static List<Message> CreateMessagesWithHeader(int count)
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
                    { HeaderKey.New("header1"), HeaderValue.FromString("value1") },
                    { HeaderKey.New("header2"), HeaderValue.FromInt32(14 + i) }
                }));
        }

        return messages;
    }
}
