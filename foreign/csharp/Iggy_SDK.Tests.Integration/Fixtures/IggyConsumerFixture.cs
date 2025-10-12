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
using Apache.Iggy.Enums;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;
using Apache.Iggy.Tests.Integrations.Helpers;
using TUnit.Core.Interfaces;

namespace Apache.Iggy.Tests.Integrations.Fixtures;

public class IggyConsumerFixture : IAsyncInitializer
{
    internal readonly uint PartitionsCount = 5;
    internal readonly string StreamId = "IggyConsumerStream";
    internal readonly string TopicId = "IggyConsumerTopic";
    internal readonly int MessageCount = 100;

    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerAssembly)]
    public required IggyServerFixture IggyServerFixture { get; init; }

    public Dictionary<Protocol, IIggyClient> Clients { get; set; } = new();

    public async Task InitializeAsync()
    {
        Clients = await IggyServerFixture.CreateClients();

        foreach (KeyValuePair<Protocol, IIggyClient> client in Clients)
        {
            // Create stream and topic
            await client.Value.CreateStreamAsync(StreamId.GetWithProtocol(client.Key));
            await client.Value.CreateTopicAsync(Identifier.String(StreamId.GetWithProtocol(client.Key)), TopicId,
                PartitionsCount);

            // Send test messages to each partition
            for (uint partitionId = 1; partitionId <= PartitionsCount; partitionId++)
            {
                var messages = new List<Message>();
                for (int i = 0; i < MessageCount; i++)
                {
                    var message = new Message(
                        Guid.NewGuid(),
                        Encoding.UTF8.GetBytes($"Test message {i} for partition {partitionId}"));
                    messages.Add(message);
                }

                await client.Value.SendMessagesAsync(
                    Identifier.String(StreamId.GetWithProtocol(client.Key)),
                    Identifier.String(TopicId),
                    Apache.Iggy.Kinds.Partitioning.PartitionId((int)partitionId),
                    messages);
            }
        }
    }
}
