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
using Apache.Iggy.Exceptions;
using Apache.Iggy.Headers;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Messages;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Shouldly;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Tests.Integrations;

public class SendMessagesTests
{
    private static readonly string DummyJson = """
                                               {
                                                 "userId": 1,
                                                 "id": 1,
                                                 "title": "delete",
                                                 "completed": false
                                               }
                                               """;

    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerAssembly)]
    public required IggyServerFixture Fixture { get; init; }

    private async Task<(IIggyClient client, string streamName, string topicName)> CreateStreamAndTopic(
        Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var streamName = $"send-msg-{Guid.NewGuid():N}";
        var topicName = "test-topic";

        await client.CreateStreamAsync(streamName);
        await client.CreateTopicAsync(Identifier.String(streamName), topicName, 1);

        return (client, streamName, topicName);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendMessages_NoHeaders_Should_SendMessages_Successfully(Protocol protocol)
    {
        var (client, streamName, topicName) = await CreateStreamAndTopic(protocol);

        var messages = new Message[]
        {
            new(Guid.NewGuid(), Encoding.UTF8.GetBytes(DummyJson)),
            new(Guid.NewGuid(), Encoding.UTF8.GetBytes(DummyJson))
        };

        await Should.NotThrowAsync(() =>
            client.SendMessagesAsync(Identifier.String(streamName),
                Identifier.String(topicName), Partitioning.None(), messages));
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendMessages_NoHeaders_Should_Throw_InvalidResponse(Protocol protocol)
    {
        var (client, streamName, _) = await CreateStreamAndTopic(protocol);

        var messages = new Message[]
        {
            new(Guid.NewGuid(), Encoding.UTF8.GetBytes(DummyJson)),
            new(Guid.NewGuid(), Encoding.UTF8.GetBytes(DummyJson))
        };

        await Should.ThrowAsync<IggyInvalidStatusCodeException>(() =>
            client.SendMessagesAsync(Identifier.String(streamName),
                Identifier.Numeric(69), Partitioning.None(), messages));
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendMessages_WithHeaders_Should_SendMessages_Successfully(Protocol protocol)
    {
        var (client, streamName, topicName) = await CreateStreamAndTopic(protocol);

        var messages = new Message[]
        {
            new(Guid.NewGuid(), Encoding.UTF8.GetBytes(DummyJson),
                new Dictionary<HeaderKey, HeaderValue>
                {
                    { HeaderKey.FromString("header1"), HeaderValue.FromString("value1") },
                    { HeaderKey.FromString("header2"), HeaderValue.FromInt32(444) }
                }),
            new(Guid.NewGuid(), Encoding.UTF8.GetBytes(DummyJson),
                new Dictionary<HeaderKey, HeaderValue>
                {
                    { HeaderKey.FromString("header1"), HeaderValue.FromString("value1") },
                    { HeaderKey.FromString("header2"), HeaderValue.FromInt32(444) }
                })
        };

        await Should.NotThrowAsync(() =>
            client.SendMessagesAsync(Identifier.String(streamName),
                Identifier.String(topicName), Partitioning.None(), messages));
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task SendMessages_WithHeaders_Should_Throw_InvalidResponse(Protocol protocol)
    {
        var (client, streamName, _) = await CreateStreamAndTopic(protocol);

        var messages = new Message[]
        {
            new(Guid.NewGuid(), Encoding.UTF8.GetBytes(DummyJson),
                new Dictionary<HeaderKey, HeaderValue>
                {
                    { HeaderKey.FromString("header1"), HeaderValue.FromString("value1") },
                    { HeaderKey.FromString("header2"), HeaderValue.FromInt32(444) }
                }),
            new(Guid.NewGuid(), Encoding.UTF8.GetBytes(DummyJson),
                new Dictionary<HeaderKey, HeaderValue>
                {
                    { HeaderKey.FromString("header1"), HeaderValue.FromString("value1") },
                    { HeaderKey.FromString("header2"), HeaderValue.FromInt32(444) }
                })
        };

        await Should.ThrowAsync<IggyInvalidStatusCodeException>(() =>
            client.SendMessagesAsync(Identifier.String(streamName),
                Identifier.Numeric(69), Partitioning.None(), messages));
    }
}
