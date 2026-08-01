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
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;
using Apache.Iggy.Tests.Integrations.Attributes;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Shouldly;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Tests.Integrations.Vsr;

/// <summary>
///     Group polls under VSR: the server hands out an assignment, the client caches it and round-robins the
///     assigned partitions itself, so a poll without an explicit partition id never reaches the broker as one.
/// </summary>
[RequiresServerNg]
public class VsrConsumerGroupTests
{
    private const uint PartitionsCount = 3;
    private const string TopicName = "vsr-group-topic";

    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerAssembly)]
    public required IggyServerFixture Fixture { get; init; }

    [Test]
    public async Task GroupPoll_Should_Drain_EveryAssignedPartition()
    {
        var (client, streamName, groupName) = await CreateGroup();
        await client.JoinConsumerGroupAsync(Identifier.String(streamName), Identifier.String(TopicName),
            Identifier.String(groupName));

        for (uint partitionId = 0; partitionId < PartitionsCount; partitionId++)
        {
            await SendAsync(client, streamName, partitionId);
        }

        // One poll per assigned partition drains it; the sole member owns them all, so the round-robin has to
        // hand out every partition exactly once before it wraps.
        var polled = await DrainGroupAsync(client, streamName, groupName, PartitionsCount);

        polled.ShouldBe((int)PartitionsCount);
    }

    [Test]
    public async Task GroupPoll_Without_Joining_Should_Throw_MemberNotFound()
    {
        var (client, streamName, groupName) = await CreateGroup();

        var exception = await Should.ThrowAsync<IggyInvalidStatusCodeException>(
            PollGroupAsync(client, streamName, groupName));

        exception.StatusCode.ShouldBe(5006);
    }

    [Test]
    public async Task GroupPoll_After_Leaving_Should_Throw_MemberNotFound()
    {
        var (client, streamName, groupName) = await CreateGroup();
        await client.JoinConsumerGroupAsync(Identifier.String(streamName), Identifier.String(TopicName),
            Identifier.String(groupName));
        await PollGroupAsync(client, streamName, groupName);

        await client.LeaveConsumerGroupAsync(Identifier.String(streamName), Identifier.String(TopicName),
            Identifier.String(groupName));

        var exception = await Should.ThrowAsync<IggyInvalidStatusCodeException>(
            PollGroupAsync(client, streamName, groupName));

        exception.StatusCode.ShouldBe(5006);
    }

    /// <summary>
    ///     A partition count change widens the assignment, and the ping is where the client re-syncs it. The
    ///     cached generation is asserted first: without it the test would pass on a client that re-synced on
    ///     every poll and never needed the heartbeat.
    /// </summary>
    [Test]
    public async Task Ping_Should_Refresh_TheGroupAssignment_After_PartitionsAreAdded()
    {
        var (client, streamName, groupName) = await CreateGroup();
        await client.JoinConsumerGroupAsync(Identifier.String(streamName), Identifier.String(TopicName),
            Identifier.String(groupName));
        await PollGroupAsync(client, streamName, groupName);

        await client.CreatePartitionsAsync(Identifier.String(streamName), Identifier.String(TopicName), 1);
        await SendAsync(client, streamName, PartitionsCount);

        (await DrainGroupAsync(client, streamName, groupName, PartitionsCount + 1)).ShouldBe(0);

        await client.PingAsync();

        (await DrainGroupAsync(client, streamName, groupName, PartitionsCount + 1)).ShouldBe(1);
    }

    /// <summary>
    ///     A member holding no partitions is still a member, so its poll has to come back empty instead of
    ///     surfacing the not-a-member error the unassigned cursor otherwise looks like. One partition and two
    ///     members guarantees exactly one of them is in that state.
    /// </summary>
    [Test]
    public async Task GroupPoll_By_AMemberWithoutPartitions_Should_ReturnEmpty()
    {
        var (first, streamName, groupName) = await CreateGroup();
        var topicName = $"vsr-single-partition-{Guid.NewGuid():N}";
        await first.CreateTopicAsync(Identifier.String(streamName), topicName, 1);
        await first.CreateConsumerGroupAsync(Identifier.String(streamName), Identifier.String(topicName), groupName);

        var second = await Fixture.CreateAuthenticatedClient(Protocol.Tcp);
        await JoinAsync(first, streamName, topicName, groupName);
        await JoinAsync(second, streamName, topicName, groupName);

        await first.SendMessagesAsync(Identifier.String(streamName), Identifier.String(topicName),
            Partitioning.PartitionId(0),
            [new Message(Guid.NewGuid(), Encoding.UTF8.GetBytes("vsr-single-partition-payload"))]);

        var firstPoll = await PollGroupAsync(first, streamName, topicName, groupName);
        var secondPoll = await PollGroupAsync(second, streamName, topicName, groupName);

        // Whichever member drew the partition drains it, and the other one has nothing to poll.
        (firstPoll.Messages.Count + secondPoll.Messages.Count).ShouldBe(1);
        Math.Min(firstPoll.Messages.Count, secondPoll.Messages.Count).ShouldBe(0);
    }

    /// <summary>
    ///     A second member rebalances the group, which leaves the first one round-robining partitions it no
    ///     longer owns. The fence has to re-sync the assignment underneath the poll: a client that surfaced the
    ///     ownership error instead would break every group app that did not special-case it.
    /// </summary>
    [Test]
    public async Task GroupPoll_After_ASecondMemberJoins_Should_ResyncTheStaleAssignment()
    {
        var (first, streamName, groupName) = await CreateGroup();
        await JoinAsync(first, streamName, TopicName, groupName);
        await DrainGroupAsync(first, streamName, groupName, PartitionsCount);

        var second = await Fixture.CreateAuthenticatedClient(Protocol.Tcp);
        await JoinAsync(second, streamName, TopicName, groupName);

        for (uint partitionId = 0; partitionId < PartitionsCount; partitionId++)
        {
            await SendAsync(first, streamName, partitionId);
        }

        // The first member still holds the pre-rebalance assignment, so its polls fence until they re-sync.
        // Between them the two members have to see every partition; a lost one means a fence was swallowed.
        var drained = await DrainGroupAsync(first, streamName, groupName, PartitionsCount);
        drained += await DrainGroupAsync(second, streamName, groupName, PartitionsCount);

        drained.ShouldBe((int)PartitionsCount);
    }

    private static Task JoinAsync(IIggyClient client, string streamName, string topicName, string groupName)
    {
        return client.JoinConsumerGroupAsync(Identifier.String(streamName), Identifier.String(topicName),
            Identifier.String(groupName));
    }

    private async Task<(IIggyClient Client, string StreamName, string GroupName)> CreateGroup()
    {
        var client = await Fixture.CreateAuthenticatedClient(Protocol.Tcp);
        var streamName = $"vsr-group-{Guid.NewGuid():N}";
        var groupName = $"vsr-group-name-{Guid.NewGuid():N}";

        await client.CreateStreamAsync(streamName);
        await client.CreateTopicAsync(Identifier.String(streamName), TopicName, PartitionsCount);
        await client.CreateConsumerGroupAsync(Identifier.String(streamName), Identifier.String(TopicName), groupName);

        return (client, streamName, groupName);
    }

    private static Task SendAsync(IIggyClient client, string streamName, uint partitionId)
    {
        return client.SendMessagesAsync(Identifier.String(streamName), Identifier.String(TopicName),
            Partitioning.PartitionId((int)partitionId),
            [new Message(Guid.NewGuid(), Encoding.UTF8.GetBytes($"vsr-group-payload-{partitionId}"))]);
    }

    /// <summary>Polls once per assigned partition and returns how many messages came back in total.</summary>
    private static async Task<int> DrainGroupAsync(IIggyClient client, string streamName, string groupName,
        uint polls)
    {
        var drained = 0;
        for (var poll = 0; poll < polls; poll++)
        {
            drained += (await PollGroupAsync(client, streamName, groupName)).Messages.Count;
        }

        return drained;
    }

    private static Task<PolledMessages> PollGroupAsync(IIggyClient client, string streamName, string groupName)
    {
        return PollGroupAsync(client, streamName, TopicName, groupName);
    }

    private static Task<PolledMessages> PollGroupAsync(IIggyClient client, string streamName, string topicName,
        string groupName)
    {
        return client.PollMessagesAsync(new MessageFetchRequest
        {
            Count = 10,
            AutoCommit = true,
            Consumer = Consumer.Group(groupName),
            PartitionId = null,
            PollingStrategy = PollingStrategy.Next(),
            StreamId = Identifier.String(streamName),
            TopicId = Identifier.String(topicName)
        });
    }
}
