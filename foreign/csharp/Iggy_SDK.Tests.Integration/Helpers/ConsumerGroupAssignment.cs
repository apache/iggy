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

using Apache.Iggy.Contracts;
using Apache.Iggy.IggyClient;
using Shouldly;

namespace Apache.Iggy.Tests.Integrations.Helpers;

/// <summary>
///     A join commits the membership at once, but a partition activates its new owner only after it durably installs
///     the owner fence. Until then the member reports no partitions, syncs an empty assignment and is refused group
///     polls and group offset writes with status 5009, so a test whose next step needs ownership waits here first.
/// </summary>
public static class ConsumerGroupAssignment
{
    private static readonly TimeSpan ConvergenceTimeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(100);

    /// <summary>
    ///     Waits until the group has <paramref name="membersCount" /> members that own every partition between them,
    ///     at most one partition apart, and returns the group as last read.
    /// </summary>
    public static async Task<ConsumerGroupResponse> WaitForConsumerGroupAssignmentAsync(
        this IIggyConsumerGroup client, Identifier streamId, Identifier topicId, Identifier groupId,
        uint membersCount)
    {
        var deadline = DateTimeOffset.UtcNow + ConvergenceTimeout;
        while (true)
        {
            var group = await client.GetConsumerGroupByIdAsync(streamId, topicId, groupId);
            group.ShouldNotBeNull();
            List<ConsumerGroupMember> members = group.Members ?? [];
            List<uint> owned = members.SelectMany(member => member.Partitions).ToList();
            owned.ShouldBeUnique($"A partition has two owners: {Describe(members)}");

            List<uint> partitionCounts = members.Select(member => member.PartitionsCount).DefaultIfEmpty().ToList();
            if (group.MembersCount == membersCount && owned.Count == group.PartitionsCount
                && partitionCounts.Max() - partitionCounts.Min() <= 1)
            {
                return group;
            }

            if (DateTimeOffset.UtcNow >= deadline)
            {
                throw new TimeoutException(
                    $"Consumer group {groupId} did not converge to {membersCount} members owning all " +
                    $"{group.PartitionsCount} partitions within {ConvergenceTimeout}: {Describe(members)}");
            }

            await Task.Delay(PollInterval);
        }
    }

    private static string Describe(IEnumerable<ConsumerGroupMember> members)
    {
        return string.Join("; ",
            members.Select(member => $"member {member.Id} owns [{string.Join(", ", member.Partitions)}]"));
    }
}
