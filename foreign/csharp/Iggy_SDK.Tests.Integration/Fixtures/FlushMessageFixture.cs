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
using Apache.Iggy.IggyClient;
using Apache.Iggy.Messages;
using Apache.Iggy.Tests.Integrations.Helpers;
using TUnit.Core.Interfaces;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Tests.Integrations.Fixtures;

public class FlushMessageFixture : IAsyncInitializer
{
    internal readonly string StreamId = "FlushMessageStream";
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
                TopicRequest.Name, TopicRequest.PartitionsCount);

            await client.Value.SendMessagesAsync(new MessageSendRequest
            {
                Partitioning = Partitioning.None(),
                StreamId = Identifier.String(StreamId.GetWithProtocol(client.Key)),
                TopicId = Identifier.String(TopicRequest.Name),
                Messages = new List<Message>
                {
                    new(Guid.NewGuid(), "Test message 1"u8.ToArray()),
                    new(Guid.NewGuid(), "Test message 2"u8.ToArray()),
                    new(Guid.NewGuid(), "Test message 3"u8.ToArray()),
                    new(Guid.NewGuid(), "Test message 4"u8.ToArray())
                }
            });
        }
    }
}
