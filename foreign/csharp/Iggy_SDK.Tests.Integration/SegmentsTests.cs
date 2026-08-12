// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Tests.Integrations.Attributes;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Shouldly;

namespace Apache.Iggy.Tests.Integrations;

public class SegmentsTests
{
    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerAssembly)]
    public required IggyServerFixture Fixture { get; init; }

    [Test]
    [SkipHttp]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DeleteSegments_WithZeroCount_Should_Succeed(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var streamName = $"seg-zero-{Guid.NewGuid():N}";
        var topicName = $"seg-zero-topic-{Guid.NewGuid():N}";
        await client.CreateStreamAsync(streamName);
        await client.CreateTopicAsync(Identifier.String(streamName), topicName, 1);

        // Deleting 0 segments should succeed without error (no-op)
        await Should.NotThrowAsync(() =>
            client.DeleteSegmentsAsync(
                Identifier.String(streamName),
                Identifier.String(topicName),
                0, // partition_id (0-indexed)
                0)); // segments_count = 0
    }

    [Test]
    [SkipTcp]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DeleteSegments_Http_Should_Throw_FeatureUnavailable(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var streamName = $"seg-http-{Guid.NewGuid():N}";
        var topicName = $"seg-http-topic-{Guid.NewGuid():N}";
        await client.CreateStreamAsync(streamName);
        await client.CreateTopicAsync(Identifier.String(streamName), topicName, 1);

        await Should.ThrowAsync<FeatureUnavailableException>(() =>
            client.DeleteSegmentsAsync(
                Identifier.String(streamName),
                Identifier.String(topicName),
                0,
                0));
    }

    [Test]
    [SkipHttp]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DeleteSegments_Should_Throw_WhenTopic_DoesNotExist(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var streamName = $"seg-notopic-{Guid.NewGuid():N}";
        await client.CreateStreamAsync(streamName);

        await Should.ThrowAsync<IggyInvalidStatusCodeException>(() =>
            client.DeleteSegmentsAsync(
                Identifier.String(streamName),
                Identifier.String("non-existent-topic"),
                0, // partition_id (0-indexed)
                1)); // segments_count
    }

    [Test]
    [SkipHttp]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DeleteSegments_Should_Throw_WhenStream_DoesNotExist(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        await Should.ThrowAsync<IggyInvalidStatusCodeException>(() =>
            client.DeleteSegmentsAsync(
                Identifier.String($"nonexistent-stream-{Guid.NewGuid():N}"),
                Identifier.String("any-topic"),
                0, // partition_id (0-indexed)
                1)); // segments_count
    }
}
