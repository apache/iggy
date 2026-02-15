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

using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Messages;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Shouldly;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Tests.Integrations;

public class FlushMessagesTests
{
    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerAssembly)]
    public required IggyServerFixture Fixture { get; init; }

    private async Task<(IIggyClient client, string streamName, string topicName)> CreateStreamWithMessages(
        Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var streamName = $"flush-{Guid.NewGuid():N}";
        var topicName = "test-topic";

        await client.CreateStreamAsync(streamName);
        await client.CreateTopicAsync(Identifier.String(streamName), topicName, 1);

        await client.SendMessagesAsync(Identifier.String(streamName),
            Identifier.String(topicName), Partitioning.None(),
            [
                new Message(Guid.NewGuid(), "Test message 1"u8.ToArray()),
                new Message(Guid.NewGuid(), "Test message 2"u8.ToArray()),
                new Message(Guid.NewGuid(), "Test message 3"u8.ToArray()),
                new Message(Guid.NewGuid(), "Test message 4"u8.ToArray())
            ]);

        return (client, streamName, topicName);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task FlushUnsavedBuffer_WithFsync_Should_Flush_Successfully(Protocol protocol)
    {
        var (client, streamName, topicName) = await CreateStreamWithMessages(protocol);

        await Should.NotThrowAsync(() =>
            client.FlushUnsavedBufferAsync(
                Identifier.String(streamName),
                Identifier.String(topicName), 0, true));
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task FlushUnsavedBuffer_WithOutFsync_Should_Flush_Successfully(Protocol protocol)
    {
        var (client, streamName, topicName) = await CreateStreamWithMessages(protocol);

        await Should.NotThrowAsync(() =>
            client.FlushUnsavedBufferAsync(
                Identifier.String(streamName),
                Identifier.String(topicName), 0, false));
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task FlushUnsavedBuffer_Should_Throw_WhenPartition_DoesNotExist(Protocol protocol)
    {
        var (client, streamName, topicName) = await CreateStreamWithMessages(protocol);

        await Should.ThrowAsync<IggyInvalidStatusCodeException>(() =>
            client.FlushUnsavedBufferAsync(
                Identifier.String(streamName),
                Identifier.String(topicName), 55, false));
    }
}
