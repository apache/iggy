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
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Shouldly;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Tests.Integrations.Vsr;

/// <summary>
///     Control-plane operations through the consensus path: every one of these consumes a request id and
///     comes back with a committed result section the decoder has to strip before the typed mapper runs.
/// </summary>
public class VsrMetadataTests
{
    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerAssembly)]
    public required IggyServerFixture Fixture { get; init; }

    [Test]
    public async Task StreamLifecycle_Should_CommitThroughConsensus()
    {
        var client = await Fixture.CreateAuthenticatedClient(Protocol.Tcp);

        var name = $"vsr-meta-stream-{Guid.NewGuid():N}";
        var created = await client.CreateStreamAsync(name);
        created.ShouldNotBeNull();

        var fetched = await client.GetStreamByIdAsync(Identifier.Numeric(created.Id));
        fetched.ShouldNotBeNull();
        fetched.Name.ShouldBe(name);

        await client.DeleteStreamAsync(Identifier.Numeric(created.Id));
        (await client.GetStreamsAsync()).ShouldNotContain(stream => stream.Name == name);
    }

    [Test]
    public async Task TopicLifecycle_Should_CommitThroughConsensus()
    {
        var client = await Fixture.CreateAuthenticatedClient(Protocol.Tcp);

        var streamName = $"vsr-meta-topic-{Guid.NewGuid():N}";
        await client.CreateStreamAsync(streamName);

        var topic = await client.CreateTopicAsync(Identifier.String(streamName), "vsr-topic", 3);
        topic.ShouldNotBeNull();
        topic.PartitionsCount.ShouldBe(3u);

        var fetched = await client.GetTopicByIdAsync(Identifier.String(streamName), Identifier.Numeric(topic.Id));
        fetched.ShouldNotBeNull();
        fetched.PartitionsCount.ShouldBe(3u);

        await client.DeleteTopicAsync(Identifier.String(streamName), Identifier.Numeric(topic.Id));
        (await client.GetTopicsAsync(Identifier.String(streamName))).ShouldBeEmpty();
    }

    /// <summary>
    ///     A committed rejection rides the result section with status 0 in the header, so the decoder has to
    ///     read the first result entry to see it. A silent success here would mean the section was skipped.
    /// </summary>
    [Test]
    public async Task DuplicateStream_Should_Surface_TheCommittedRejection()
    {
        var client = await Fixture.CreateAuthenticatedClient(Protocol.Tcp);

        var name = $"vsr-meta-dup-{Guid.NewGuid():N}";
        await client.CreateStreamAsync(name);

        await Should.ThrowAsync<IggyInvalidStatusCodeException>(client.CreateStreamAsync(name));
    }

    [Test]
    public async Task UserLifecycle_Should_CommitThroughConsensus()
    {
        var client = await Fixture.CreateAuthenticatedClient(Protocol.Tcp);

        var name = $"vsr-user-{Guid.NewGuid():N}";
        var user = await client.CreateUserAsync(name, "secret-password", UserStatus.Active);
        user.ShouldNotBeNull();

        var fetched = await client.GetUserAsync(Identifier.Numeric(user.Id));
        fetched.ShouldNotBeNull();
        fetched.Username.ShouldBe(name);

        await client.DeleteUserAsync(Identifier.Numeric(user.Id));
        (await client.GetUserAsync(Identifier.Numeric(user.Id))).ShouldBeNull();
    }

    /// <summary>
    ///     Partition ops read the request counter without advancing it. Interleaving them with metadata
    ///     writes catches the asymmetry: a partition op that consumed an id would gap the next metadata one
    ///     and the primary would silently drop it.
    /// </summary>
    [Test]
    public async Task MetadataWrites_Should_KeepCommitting_Around_PartitionOps()
    {
        var client = await Fixture.CreateAuthenticatedClient(Protocol.Tcp);

        var streamName = $"vsr-meta-mixed-{Guid.NewGuid():N}";
        await client.CreateStreamAsync(streamName);
        await client.CreateTopicAsync(Identifier.String(streamName), "vsr-mixed-topic", 1);

        await client.SendMessagesAsync(Identifier.String(streamName), Identifier.String("vsr-mixed-topic"),
            Partitioning.PartitionId(0),
            [new Message(Guid.NewGuid(), "vsr-mixed"u8.ToArray())]);
        await client.StoreOffsetAsync(Consumer.New(1), Identifier.String(streamName),
            Identifier.String("vsr-mixed-topic"), 0, 0);

        var second = $"vsr-meta-mixed-second-{Guid.NewGuid():N}";
        var created = await client.CreateStreamAsync(second);

        created.ShouldNotBeNull();
        created.Name.ShouldBe(second);
    }
}
