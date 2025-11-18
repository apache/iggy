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
using Apache.Iggy.Tests.Integrations.Attributes;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Apache.Iggy.Tests.Integrations.Helpers;
using Shouldly;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Tests.Integrations;

public class SendMessagesTests
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
        _messagesWithoutHeaders =
        [
            new Message(Guid.NewGuid(), Encoding.UTF8.GetBytes(dummyJson)),
            new Message(Guid.NewGuid(), Encoding.UTF8.GetBytes(dummyJson))
        ];
        _messagesWithHeaders =
        [
            new Message(Guid.NewGuid(), Encoding.UTF8.GetBytes(dummyJson),
                new Dictionary<HeaderKey, HeaderValue>
                {
                    { HeaderKey.New("header1"), HeaderValue.FromString("value1") },
                    { HeaderKey.New("header2"), HeaderValue.FromInt32(444) }
                }),
            new Message(Guid.NewGuid(), Encoding.UTF8.GetBytes(dummyJson),
                new Dictionary<HeaderKey, HeaderValue>
                {
                    { HeaderKey.New("header1"), HeaderValue.FromString("value1") },
                    { HeaderKey.New("header2"), HeaderValue.FromInt32(444) }
                })
        ];

        return Task.CompletedTask;
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendMessages_NoHeaders_Should_SendMessages_Successfully(Protocol protocol)
    {
        await Should.NotThrowAsync(() =>
            Fixture.Clients[protocol].SendMessagesAsync(Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
                Identifier.String(Fixture.TopicRequest.Name), Partitioning.None(), _messagesWithoutHeaders));
    }

    [Test]
    [DependsOn(nameof(SendMessages_NoHeaders_Should_SendMessages_Successfully))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendMessages_NoHeaders_Should_Throw_InvalidResponse(Protocol protocol)
    {
        await Should.ThrowAsync<IggyInvalidStatusCodeException>(() =>
            Fixture.Clients[protocol].SendMessagesAsync(Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
                Identifier.Numeric(69), Partitioning.None(), _messagesWithoutHeaders));
    }

    [Test]
    [DependsOn(nameof(SendMessages_NoHeaders_Should_Throw_InvalidResponse))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendMessages_WithHeaders_Should_SendMessages_Successfully(Protocol protocol)
    {
        await Should.NotThrowAsync(() =>
            Fixture.Clients[protocol].SendMessagesAsync(Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
                Identifier.String(Fixture.TopicRequest.Name), Partitioning.None(), _messagesWithHeaders));
    }

    [Test]
    [DependsOn(nameof(SendMessages_WithHeaders_Should_SendMessages_Successfully))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendMessages_WithHeaders_Should_Throw_InvalidResponse(Protocol protocol)
    {
        await Should.ThrowAsync<IggyInvalidStatusCodeException>(() =>
            Fixture.Clients[protocol].SendMessagesAsync(Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
                Identifier.Numeric(69), Partitioning.None(), _messagesWithHeaders));
    }
}
