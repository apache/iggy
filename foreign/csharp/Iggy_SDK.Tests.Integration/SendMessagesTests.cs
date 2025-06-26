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
using Apache.Iggy.Exceptions;
using Apache.Iggy.Headers;
using Apache.Iggy.Messages;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Shouldly;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Tests.Integrations;

[MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
public class SendMessagesTests(Protocol protocol)
{
    private static Message[] _messagesWithoutHeaders = [];
    private static Message[] _messagesWithHeaders = [];

    [ClassDataSource<SendMessageFixture>(Shared = SharedType.PerClass)]
    public required SendMessageFixture Fixture { get; set; }

    [Before(Class)]
    public static Task Before()
    {
        var dummyJson = """
                        {
                          "userId": 1,
                          "id": 1,
                          "title": "delete",
                          "completed": false
                        }
                        """;
        _messagesWithoutHeaders = [new Message(Guid.NewGuid(), Encoding.UTF8.GetBytes(dummyJson)), new Message(Guid.NewGuid(), Encoding.UTF8.GetBytes(dummyJson))];
        _messagesWithHeaders =
        [
            new Message(Guid.NewGuid(), Encoding.UTF8.GetBytes(dummyJson), new Dictionary<HeaderKey, HeaderValue>
            {
                { HeaderKey.New("header1"), HeaderValue.FromString("value1") },
                { HeaderKey.New("header2"), HeaderValue.FromInt32(444) }
            }),
            new Message(Guid.NewGuid(), Encoding.UTF8.GetBytes(dummyJson), new Dictionary<HeaderKey, HeaderValue>
            {
                { HeaderKey.New("header1"), HeaderValue.FromString("value1") },
                { HeaderKey.New("header2"), HeaderValue.FromInt32(444) }
            })
        ];

        return Task.CompletedTask;
    }

    [Test]
    public async Task SendMessages_NoHeaders_Should_SendMessages_Successfully()
    {
        await Should.NotThrowAsync(() => Fixture.Clients[protocol].SendMessagesAsync(new MessageSendRequest
        {
            Messages = _messagesWithoutHeaders,
            Partitioning = Partitioning.None(),
            StreamId = Identifier.Numeric(Fixture.StreamRequest.StreamId!.Value),
            TopicId = Identifier.Numeric(Fixture.TopicRequest.TopicId!.Value)
        }));
    }

    [Test]
    [DependsOn(nameof(SendMessages_NoHeaders_Should_SendMessages_Successfully))]
    public async Task SendMessages_NoHeaders_Should_Throw_InvalidResponse()
    {
        await Should.ThrowAsync<InvalidResponseException>(() => Fixture.Clients[protocol].SendMessagesAsync(new MessageSendRequest
        {
            Messages = _messagesWithoutHeaders,
            Partitioning = Partitioning.None(),
            StreamId = Identifier.Numeric(Fixture.StreamRequest.StreamId!.Value),
            TopicId = Identifier.Numeric(69)
        }));
    }

    [Test]
    [DependsOn(nameof(SendMessages_NoHeaders_Should_Throw_InvalidResponse))]
    public async Task SendMessages_WithHeaders_Should_SendMessages_Successfully()
    {
        await Should.NotThrowAsync(() => Fixture.Clients[protocol].SendMessagesAsync(new MessageSendRequest
        {
            Messages = _messagesWithHeaders,
            Partitioning = Partitioning.None(),
            StreamId = Identifier.Numeric(Fixture.StreamRequest.StreamId!.Value),
            TopicId = Identifier.Numeric(Fixture.TopicRequest.TopicId!.Value)
        }));
    }

    [Test]
    [DependsOn(nameof(SendMessages_WithHeaders_Should_SendMessages_Successfully))]
    public async Task SendMessages_WithHeaders_Should_Throw_InvalidResponse()
    {
        await Should.ThrowAsync<InvalidResponseException>(() => Fixture.Clients[protocol].SendMessagesAsync(new MessageSendRequest
        {
            Messages = _messagesWithHeaders,
            Partitioning = Partitioning.None(),
            StreamId = Identifier.Numeric(Fixture.StreamRequest.StreamId!.Value),
            TopicId = Identifier.Numeric(69)
        }));
    }
}