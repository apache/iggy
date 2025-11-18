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
using Apache.Iggy.Enums;
using Apache.Iggy.Headers;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Messages;
using Apache.Iggy.Tests.Integrations.Helpers;
using TUnit.Core.Interfaces;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Tests.Integrations.Fixtures;

public class FetchMessagesFixture : IAsyncInitializer
{
    internal readonly int MessageCount = 20;
    internal readonly string StreamId = "FetchMessagesStream";
    internal readonly CreateTopicRequest TopicHeadersRequest = TopicFactory.CreateTopic("HeadersTopic");
    internal readonly CreateTopicRequest TopicRequest = TopicFactory.CreateTopic("Topic");

    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerAssembly)]
    public required IggyServerFixture IggyServerFixture { get; init; }

    public Dictionary<Protocol, IIggyClient> Clients { get; set; } = new();

    public async Task InitializeAsync()
    {
        Clients = await IggyServerFixture.CreateClients();
        foreach (KeyValuePair<Protocol, IIggyClient> client in Clients)
        {
            var streamId = Identifier.String(StreamId.GetWithProtocol(client.Key));

            await client.Value.CreateStreamAsync(streamId.GetString());
            await client.Value.CreateTopicAsync(streamId, TopicRequest.Name,
                TopicRequest.PartitionsCount);
            await client.Value.CreateTopicAsync(streamId, TopicHeadersRequest.Name,
                TopicHeadersRequest.PartitionsCount);

            await client.Value.SendMessagesAsync(streamId, Identifier.String(TopicRequest.Name), Partitioning.None(),
                CreateMessagesWithoutHeader(MessageCount));

            await client.Value.SendMessagesAsync(streamId, Identifier.String(TopicHeadersRequest.Name),
                Partitioning.None(), CreateMessagesWithHeader(MessageCount));
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
                    { HeaderKey.New("header1"), HeaderValue.FromString("value1") },
                    { HeaderKey.New("header2"), HeaderValue.FromInt32(14 + i) }
                }));
        }

        return messages.ToArray();
    }
}
