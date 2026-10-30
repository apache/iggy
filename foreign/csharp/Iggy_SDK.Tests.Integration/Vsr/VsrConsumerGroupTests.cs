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

using System.Diagnostics;
using System.Text;
using Apache.Iggy.Consumers;
using Apache.Iggy.Contracts;
using Apache.Iggy.Extensions;
using Apache.Iggy.IggyClient.Implementations;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Apache.Iggy.Tests.Integrations.Helpers;
using Shouldly;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Tests.Integrations.Vsr;

/// <summary>
///     Group polls under VSR: the server hands out an assignment, the client caches it and round-robins the
///     assigned partitions itself, so a poll without an explicit partition id never reaches the broker as one.
/// </summary>
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
        await JoinAsync(client, streamName, TopicName, groupName, 1);

        for (uint partitionId = 0; partitionId < PartitionsCount; partitionId++)
        {
            await SendAsync(client, streamName, partitionId);
        }

        // One poll per assigned partition drains it. The sole member owns them all, so the round-robin has to
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
        await JoinAsync(client, streamName, TopicName, groupName, 1);
        await PollGroupAsync(client, streamName, groupName);

        await client.CreatePartitionsAsync(Identifier.String(streamName), Identifier.String(TopicName), 1);
        await SendAsync(client, streamName, PartitionsCount);

        (await DrainGroupAsync(client, streamName, groupName, PartitionsCount + 1)).ShouldBe(0);

        // The new partition gets its owner asynchronously. Waiting only now keeps the drain above on the cached
        // assignment, which a poll re-syncs once it is five seconds old.
        await client.WaitForConsumerGroupAssignmentAsync(Identifier.String(streamName), Identifier.String(TopicName),
            Identifier.String(groupName), 1);
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
        await JoinAsync(first, streamName, topicName, groupName, 1);
        await JoinAsync(second, streamName, topicName, groupName, 2);

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
        await JoinAsync(first, streamName, TopicName, groupName, 1);
        await DrainGroupAsync(first, streamName, groupName, PartitionsCount);

        var second = await Fixture.CreateAuthenticatedClient(Protocol.Tcp);
        await JoinAsync(second, streamName, TopicName, groupName, 2);

        for (uint partitionId = 0; partitionId < PartitionsCount; partitionId++)
        {
            await SendAsync(first, streamName, partitionId);
        }

        // The first member still holds the pre-rebalance assignment, so its polls fence until they re-sync.
        // Between them the two members have to see every partition. A lost one means a fence was swallowed.
        var drained = await DrainGroupAsync(first, streamName, groupName, PartitionsCount);
        drained += await DrainGroupAsync(second, streamName, groupName, PartitionsCount);

        drained.ShouldBe((int)PartitionsCount);
    }

    /// <summary>
    ///     A group reader waits on every assigned partition, one read per primary, and returns the one that holds
    ///     messages. The partitions get messages in descending order, so a reader that waited on one partition alone
    ///     would park in at least one round. It runs alone, because the load of the other tests stretches a reply past
    ///     the bound.
    /// </summary>
    [Test]
    [NotInParallel]
    public async Task GroupReader_Should_Read_ThePartitionThatHoldsMessages()
    {
        var promptReply = TimeSpan.FromSeconds(2);
        var (client, streamName, groupName) = await CreateGroup();
        await using var reader = await client.Reader(Identifier.String(streamName), Identifier.String(TopicName),
                Consumer.Group(groupName))
            .WithOptions(new PollOptions { Count = 10, MaxWait = TimeSpan.FromSeconds(10) })
            .BuildAsync();
        await WaitForAssignmentAsync(reader, (int)PartitionsCount);

        for (var partition = (int)PartitionsCount - 1; partition >= 0; partition--)
        {
            await SendAsync(client, streamName, (uint)partition);
            var started = Stopwatch.StartNew();
            var batch = await reader.NextAsync();

            started.Elapsed.ShouldBeLessThan(promptReply);
            batch.PartitionId.ShouldBe((uint)partition);
            batch.Messages.Count.ShouldBe(1);
        }
    }

    /// <summary>
    ///     A partition with a backlog does not hide a partition of the same primary that holds a message too: the
    ///     reader gives the partition that the backlog left out a fetch of its own beside the next read of the
    ///     backlog. One partition more than nodes puts two partitions on one primary.
    /// </summary>
    [Test]
    public async Task GroupReader_With_ABacklog_Should_Serve_AnotherPartitionOfItsPrimary()
    {
        const int BacklogMessages = 20;
        var client = await Fixture.CreateAuthenticatedClient(Protocol.Tcp);
        var metadata = (await client.GetClusterMetadataAsync()).ShouldNotBeNull();
        var partitionsCount = (uint)metadata.Nodes.Length + 1;
        var (streamName, groupName) = await CreateGroup(client, partitionsCount);
        var assigned = Enumerable.Range(0, (int)partitionsCount).Select(partitionId => (uint)partitionId).ToList();
        var shared = (await ((TcpMessageStream)client).ReadinessGroupsAsync(Identifier.String(streamName),
            Identifier.String(TopicName), assigned)).FirstOrDefault(group => group.Count > 1).ShouldNotBeNull();
        for (var message = 0; message < BacklogMessages; message++)
        {
            await SendAsync(client, streamName, shared[0]);
        }

        await SendAsync(client, streamName, shared[^1]);
        await using var reader = await client.Reader(Identifier.String(streamName), Identifier.String(TopicName),
                Consumer.Group(groupName))
            .WithStart(PollingStrategy.Offset(0))
            .WithOptions(new PollOptions { Count = 10, MaxWait = TimeSpan.FromSeconds(10) })
            .BuildAsync();
        await WaitForAssignmentAsync(reader, (int)partitionsCount);

        // A fetch from an offset fills its count from the backlog in request order, so the first reply may hold the
        // backlog alone. The other partition then gets a fetch of its own beside the next backlog fetch, whose replies
        // land in either order, so it shows by the third batch, and one more backlog reply may overtake its reply.
        List<uint> read = [];
        while (read.Count < 4 && !read.Contains(shared[^1]))
        {
            read.Add((await reader.NextAsync()).PartitionId);
        }

        read.ShouldContain(shared[^1], $"read {string.Join(", ", read)} before {shared[^1]}");
    }

    private static async Task WaitForAssignmentAsync(PartitionReader reader, int partitions)
    {
        var deadline = Stopwatch.StartNew();
        while (reader.Partitions.Count < partitions)
        {
            deadline.Elapsed.ShouldBeLessThan(TimeSpan.FromSeconds(10), "the member never owned every partition");
            await Task.Delay(100);
        }
    }

    /// <summary>Joins, then waits until the members own every partition, so the next group poll syncs them.</summary>
    private static async Task JoinAsync(IIggyClient client, string streamName, string topicName, string groupName,
        uint membersCount)
    {
        await client.JoinConsumerGroupAsync(Identifier.String(streamName), Identifier.String(topicName),
            Identifier.String(groupName));
        await client.WaitForConsumerGroupAssignmentAsync(Identifier.String(streamName), Identifier.String(topicName),
            Identifier.String(groupName), membersCount);
    }

    private async Task<(IIggyClient Client, string StreamName, string GroupName)> CreateGroup()
    {
        var client = await Fixture.CreateAuthenticatedClient(Protocol.Tcp);
        var (streamName, groupName) = await CreateGroup(client, PartitionsCount);

        return (client, streamName, groupName);
    }

    private static async Task<(string StreamName, string GroupName)> CreateGroup(IIggyClient client,
        uint partitionsCount)
    {
        var streamName = $"vsr-group-{Guid.NewGuid():N}";
        var groupName = $"vsr-group-name-{Guid.NewGuid():N}";

        await client.CreateStreamAsync(streamName);
        await client.CreateTopicAsync(Identifier.String(streamName), TopicName, partitionsCount);
        await client.CreateConsumerGroupAsync(Identifier.String(streamName), Identifier.String(TopicName), groupName);

        return (streamName, groupName);
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
