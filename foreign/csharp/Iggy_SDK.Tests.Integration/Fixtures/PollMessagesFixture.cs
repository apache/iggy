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
using Apache.Iggy.Contracts.Http;
using Apache.Iggy.Enums;
using Apache.Iggy.Headers;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Tests.Integrations.Helpers;
using Apache.Iggy.Tests.Integrations.Models;
using TUnit.Core.Interfaces;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Tests.Integrations.Fixtures;

public class PollMessagesFixture : IAsyncInitializer
{
    internal readonly int MessageCount = 10;
    internal readonly string StreamId = "PollMessagesStream";
    internal readonly CreateTopicRequest TopicRequest = TopicFactory.CreateTopic("Topic");

    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerAssembly)]
    public required IggyServerFixture IggyServerFixture { get; init; }

    public Dictionary<Protocol, IIggyClient> Clients { get; set; } = new();

    public async Task InitializeAsync()
    {
        Clients = await IggyServerFixture.CreateClients();
        foreach (KeyValuePair<Protocol, IIggyClient> client in Clients)
        {
            await client.Value.CreateStreamAsync(StreamId.GetWithProtocol(client.Key));
            await client.Value.CreateTopicAsync(Identifier.String(StreamId.GetWithProtocol(client.Key)),
                TopicRequest.Name, TopicRequest.PartitionsCount, topicId: TopicRequest.TopicId);
            await client.Value.SendMessagesAsync(
                new MessageSendRequest<DummyMessage>
                {
                    Messages = CreateDummyMessagesWithoutHeader(MessageCount),
                    Partitioning = Partitioning.None(),
                    StreamId = Identifier.String(StreamId.GetWithProtocol(client.Key)),
                    TopicId = Identifier.String(TopicRequest.Name)
                },
                message => message.SerializeDummyMessage(),
                headers: new Dictionary<HeaderKey, HeaderValue>
                {
                    { HeaderKey.New("header1"), HeaderValue.FromString("value1") },
                    { HeaderKey.New("header2"), HeaderValue.FromInt32(14) }
                });
        }
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
}
