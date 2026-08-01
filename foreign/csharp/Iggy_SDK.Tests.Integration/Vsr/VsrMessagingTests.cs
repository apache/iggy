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

using System.Text;
using Apache.Iggy.Contracts;
using Apache.Iggy.Enums;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;
using Apache.Iggy.Tests.Integrations.Attributes;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Shouldly;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Tests.Integrations.Vsr;

/// <summary>
///     The partition plane. Under VSR the broker never picks a partition, so the client resolves every
///     partitioning kind to an explicit id before the request leaves - these tests assert the resolution
///     lands where the Rust SDK's does.
/// </summary>
[RequiresServerNg]
public class VsrMessagingTests
{
    private const uint PartitionsCount = 4;
    private const string TopicName = "vsr-messages";

    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerAssembly)]
    public required IggyServerFixture Fixture { get; init; }

    [Test]
    public async Task SendMessages_ToAnExplicitPartition_Should_PollBack_FromThatPartition()
    {
        var (client, streamName) = await CreateStreamAndTopic();

        await SendAsync(client, streamName, Partitioning.PartitionId(2), 5);

        var polled = await PollAsync(client, streamName, 2);
        polled.Messages.Count.ShouldBe(5);
        polled.PartitionId.ShouldBe(2);

        (await PollAsync(client, streamName, 1)).Messages.ShouldBeEmpty();
    }

    /// <summary>
    ///     Balanced partitioning is resolved client-side by round-robin over the topic's partition count, so
    ///     a batch per partition ends up one message on each.
    /// </summary>
    [Test]
    public async Task SendMessages_Balanced_Should_RoundRobin_AcrossEveryPartition()
    {
        var (client, streamName) = await CreateStreamAndTopic();

        for (var i = 0; i < PartitionsCount; i++)
        {
            await SendAsync(client, streamName, Partitioning.None(), 1);
        }

        List<int> counts = await PollEveryPartitionAsync(client, streamName);

        counts.Sum().ShouldBe((int)PartitionsCount);
        counts.ShouldAllBe(count => count == 1);
    }

    /// <summary>
    ///     The message key hashes to one partition, so every message under the same key lands together and
    ///     two different keys are free to differ. Only the first is asserted: the hash is pinned by the unit
    ///     tests against the Rust vectors, and asserting two keys differ would be a coin flip.
    /// </summary>
    [Test]
    public async Task SendMessages_ByMessageKey_Should_LandOn_ASinglePartition()
    {
        var (client, streamName) = await CreateStreamAndTopic();
        var key = Partitioning.EntityIdString($"key-{Guid.NewGuid():N}");

        for (var i = 0; i < 6; i++)
        {
            await SendAsync(client, streamName, key, 1);
        }

        List<int> counts = await PollEveryPartitionAsync(client, streamName);

        counts.Sum().ShouldBe(6);
        counts.Count(count => count > 0).ShouldBe(1);
    }

    [Test]
    public async Task ConsumerOffsets_Should_RoundTrip_ThroughTheResultSection()
    {
        var (client, streamName) = await CreateStreamAndTopic();
        await SendAsync(client, streamName, Partitioning.PartitionId(0), 3);

        var consumer = Consumer.New($"vsr-offset-{Guid.NewGuid():N}");
        await client.StoreOffsetAsync(consumer, Identifier.String(streamName), Identifier.String(TopicName), 1, 0);

        var stored = await client.GetOffsetAsync(consumer, Identifier.String(streamName),
            Identifier.String(TopicName), 0);
        stored.ShouldNotBeNull();
        stored.StoredOffset.ShouldBe(1u);

        await client.DeleteOffsetAsync(consumer, Identifier.String(streamName), Identifier.String(TopicName), 0);

        var cleared = await client.GetOffsetAsync(consumer, Identifier.String(streamName),
            Identifier.String(TopicName), 0);
        cleared.ShouldSatisfyAllConditions(() => (cleared is null || cleared.StoredOffset == 0).ShouldBeTrue());
    }

    private async Task<(IIggyClient Client, string StreamName)> CreateStreamAndTopic()
    {
        var client = await Fixture.CreateAuthenticatedClient(Protocol.Tcp);
        var streamName = $"vsr-msg-{Guid.NewGuid():N}";

        await client.CreateStreamAsync(streamName);
        await client.CreateTopicAsync(Identifier.String(streamName), TopicName, PartitionsCount);

        return (client, streamName);
    }

    private static Task SendAsync(IIggyClient client, string streamName, Partitioning partitioning, int count)
    {
        Message[] messages = Enumerable.Range(0, count)
            .Select(index => new Message(Guid.NewGuid(), Encoding.UTF8.GetBytes($"vsr-payload-{index}")))
            .ToArray();

        return client.SendMessagesAsync(Identifier.String(streamName), Identifier.String(TopicName), partitioning,
            messages);
    }

    private static Task<PolledMessages> PollAsync(IIggyClient client, string streamName, uint partitionId)
    {
        return client.PollMessagesAsync(new MessageFetchRequest
        {
            Count = 100,
            AutoCommit = false,
            Consumer = Consumer.New(1),
            PartitionId = partitionId,
            PollingStrategy = PollingStrategy.Offset(0),
            StreamId = Identifier.String(streamName),
            TopicId = Identifier.String(TopicName)
        });
    }

    private static async Task<List<int>> PollEveryPartitionAsync(IIggyClient client, string streamName)
    {
        var counts = new List<int>();
        for (uint partitionId = 0; partitionId < PartitionsCount; partitionId++)
        {
            counts.Add((await PollAsync(client, streamName, partitionId)).Messages.Count);
        }

        return counts;
    }
}
