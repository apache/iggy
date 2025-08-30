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
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;
using Apache.Iggy.Tests.Integrations.Helpers;

namespace Apache.Iggy.Tests.Integrations.Fixtures;

public class OffsetFixtures : IggyServerFixture
{
    internal readonly uint StreamId = 1;
    internal readonly CreateTopicRequest TopicRequest = TopicFactory.CreateTopic();

    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();

        foreach (var client in Clients.Values)
        {
            await client.CreateStreamAsync("TestStream", StreamId);
            await client.CreateTopicAsync(Identifier.Numeric(StreamId), TopicRequest.Name, TopicRequest.PartitionsCount,
                topicId: TopicRequest.TopicId);

            await client.SendMessagesAsync(new MessageSendRequest
            {
                Partitioning = Partitioning.None(),
                StreamId = Identifier.Numeric(StreamId),
                TopicId = Identifier.Numeric(TopicRequest.TopicId!.Value),
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