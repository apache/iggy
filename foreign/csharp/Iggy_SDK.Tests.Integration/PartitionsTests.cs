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
using Apache.Iggy.Tests.Integrations.Fixtures;
using Shouldly;

namespace Apache.Iggy.Tests.Integrations;

public class PartitionsTests
{
    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerAssembly)]
    public required IggyServerFixture Fixture { get; init; }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task CreatePartition_HappyPath_Should_CreatePartition_Successfully(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var streamName = $"part-create-{Guid.NewGuid():N}";
        var topicName = "test-topic";

        await client.CreateStreamAsync(streamName);
        await client.CreateTopicAsync(Identifier.String(streamName), topicName, 1);

        await Should.NotThrowAsync(() =>
            client.CreatePartitionsAsync(Identifier.String(streamName),
                Identifier.String(topicName), 3));

        var response = await client.GetTopicByIdAsync(
            Identifier.String(streamName), Identifier.String(topicName));
        response.ShouldNotBeNull();
        response.PartitionsCount.ShouldBe(4u);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DeletePartition_Should_DeletePartition_Successfully(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var streamName = $"part-delete-{Guid.NewGuid():N}";
        var topicName = "test-topic";

        await client.CreateStreamAsync(streamName);
        await client.CreateTopicAsync(Identifier.String(streamName), topicName, 4);

        await Should.NotThrowAsync(() =>
            client.DeletePartitionsAsync(Identifier.String(streamName),
                Identifier.String(topicName), 1));

        var response = await client.GetTopicByIdAsync(
            Identifier.String(streamName), Identifier.String(topicName));
        response.ShouldNotBeNull();
        response.PartitionsCount.ShouldBe(3u);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DeletePartition_Should_Throw_WhenTopic_DoesNotExist(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var streamName = $"part-notopic-{Guid.NewGuid():N}";

        await client.CreateStreamAsync(streamName);

        await Should.ThrowAsync<IggyInvalidStatusCodeException>(() =>
            client.DeletePartitionsAsync(Identifier.String(streamName),
                Identifier.String("nonexistent-topic"), 1));
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DeletePartition_Should_Throw_WhenStream_DoesNotExist(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        await Should.ThrowAsync<IggyInvalidStatusCodeException>(() =>
            client.DeletePartitionsAsync(Identifier.String($"nonexistent-{Guid.NewGuid():N}"),
                Identifier.String("nonexistent-topic"), 1));
    }
}
