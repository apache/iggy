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
using Apache.Iggy.Contracts.Http;
using Apache.Iggy.Headers;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;
using Apache.Iggy.Tests.Integrations.Helpers;
using Apache.Iggy.Tests.Integrations.Models;

namespace Apache.Iggy.Tests.Integrations.Fixtures;

public class FetchMessagesFixture : IggyServerFixture
{
    public readonly TopicRequest HeadersTopicRequest = TopicFactory.CreateTopic(2);
    public readonly int MessageCount = 20;
    public readonly StreamRequest StreamRequest = StreamFactory.CreateStream();
    public readonly TopicRequest TopicDummyHeaderRequest = TopicFactory.CreateTopic(4);
    public readonly TopicRequest TopicDummyRequest = TopicFactory.CreateTopic(3);
    public readonly TopicRequest TopicRequest = TopicFactory.CreateTopic();

    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();

        var request = new MessageSendRequest
        {
            StreamId = Identifier.Numeric(StreamRequest.StreamId!.Value),
            TopicId = Identifier.Numeric(TopicRequest.TopicId!.Value),
            Partitioning = Partitioning.None(),
            Messages = CreateMessagesWithoutHeader(MessageCount)
        };

        var requestWithHeaders = new MessageSendRequest
        {
            StreamId = Identifier.Numeric(StreamRequest.StreamId.Value),
            TopicId = Identifier.Numeric(HeadersTopicRequest.TopicId!.Value),
            Partitioning = Partitioning.None(),
            Messages = CreateMessagesWithHeader(MessageCount)
        };

        var requestDummyMessage = new MessageSendRequest<DummyMessage>
        {
            StreamId = Identifier.Numeric(StreamRequest.StreamId.Value),
            TopicId = Identifier.Numeric(TopicDummyRequest.TopicId!.Value),
            Partitioning = Partitioning.None(),
            Messages = CreateDummyMessagesWithoutHeader(MessageCount)
        };

        var requestDummyMessageWithHeaders = new MessageSendRequest<DummyMessage>
        {
            StreamId = Identifier.Numeric(StreamRequest.StreamId.Value),
            TopicId = Identifier.Numeric(TopicDummyHeaderRequest.TopicId!.Value),
            Partitioning = Partitioning.None(),
            Messages = CreateDummyMessagesWithoutHeader(MessageCount)
        };

        foreach (var client in Clients.Values)
        {
            await client.CreateStreamAsync(StreamRequest);
            await client.CreateTopicAsync(Identifier.Numeric(StreamRequest.StreamId.Value), TopicRequest);
            await client.CreateTopicAsync(Identifier.Numeric(StreamRequest.StreamId.Value), TopicDummyRequest);
            await client.CreateTopicAsync(Identifier.Numeric(StreamRequest.StreamId.Value), HeadersTopicRequest);
            await client.CreateTopicAsync(Identifier.Numeric(StreamRequest.StreamId.Value), TopicDummyHeaderRequest);

            await client.SendMessagesAsync(request);
            await client.SendMessagesAsync(requestDummyMessage, message => message.SerializeDummyMessage());
            await client.SendMessagesAsync(requestWithHeaders);
            await client.SendMessagesAsync(requestDummyMessageWithHeaders, message => message.SerializeDummyMessage(), headers: new Dictionary<HeaderKey, HeaderValue>
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
            messages.Add(new Message(Guid.NewGuid(), Encoding.UTF8.GetBytes(dummyJson), new Dictionary<HeaderKey, HeaderValue>
            {
                { HeaderKey.New("header1"), HeaderValue.FromString("value1") },
                { HeaderKey.New("header2"), HeaderValue.FromInt32(14 + i) }
            }));
        }

        return messages;
    }
}