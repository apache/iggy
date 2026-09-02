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

using System.IO.Hashing;
using System.Text;
using Apache.Iggy.Vsr;

namespace Apache.Iggy.Tests.VsrTests;

public sealed class ConsumerGroupClientStateTests
{
    private static readonly GroupKey Key =
        new(Identifier.Numeric(1), Identifier.Numeric(2), Identifier.Numeric(3));

    private static readonly TopicKey Topic = new(Identifier.Numeric(1), Identifier.Numeric(2));

    [Fact]
    public void NextGroupPartition_RoundRobinsThenWraps()
    {
        var state = new ConsumerGroupClientState();
        state.SetAssignment(Key, 1, [0, 1, 2]);

        Assert.Equal<uint?[]>([0, 1, 2, 0], [
            state.NextGroupPartition(Key), state.NextGroupPartition(Key),
            state.NextGroupPartition(Key), state.NextGroupPartition(Key)
        ]);
    }

    [Fact]
    public void SetAssignment_OnNewGenerationResetsCursor()
    {
        var state = new ConsumerGroupClientState();
        state.SetAssignment(Key, 1, [0, 1, 2]);
        state.NextGroupPartition(Key);
        state.NextGroupPartition(Key);

        state.SetAssignment(Key, 2, [5]);

        Assert.Equal(5u, state.NextGroupPartition(Key));
    }

    [Fact]
    public void SetAssignment_OnSameGenerationKeepsCursor()
    {
        var state = new ConsumerGroupClientState();
        state.SetAssignment(Key, 1, [0, 1, 2]);
        state.NextGroupPartition(Key);

        state.SetAssignment(Key, 1, [0, 1, 2]);

        Assert.Equal(1u, state.NextGroupPartition(Key));
    }

    [Fact]
    public void NextGroupPartition_WithoutAssignmentIsNull()
    {
        var state = new ConsumerGroupClientState();

        Assert.False(state.HasAssignment(Key));
        Assert.Null(state.NextGroupPartition(Key));
    }

    [Fact]
    public void InvalidateAssignment_KeepsMembership()
    {
        var state = new ConsumerGroupClientState();
        state.RegisterGroup(Key, Identifier.Numeric(1), Identifier.Numeric(2), Identifier.Numeric(3));
        state.SetAssignment(Key, 1, [0]);

        state.InvalidateAssignment(Key);

        Assert.False(state.HasAssignment(Key));
        Assert.True(state.IsRegistered(Key));
    }

    [Fact]
    public void MemberHoldingNoPartitions_StaysRegistered()
    {
        var state = new ConsumerGroupClientState();
        state.RegisterGroup(Key, Identifier.Numeric(1), Identifier.Numeric(2), Identifier.Numeric(3));
        state.SetAssignment(Key, 1, []);

        Assert.True(state.HasAssignment(Key));
        Assert.Null(state.NextGroupPartition(Key));
        Assert.True(state.IsRegistered(Key));

        state.DeregisterGroup(Key);

        Assert.False(state.IsRegistered(Key));
    }

    [Fact]
    public void RegisteredGroups_ReturnsJoinedIdentifiers()
    {
        var state = new ConsumerGroupClientState();
        state.RegisterGroup(Key, Identifier.Numeric(1), Identifier.Numeric(2), Identifier.String("group"));

        IReadOnlyList<ConsumerGroupClientState.GroupIdentifiers> groups = state.RegisteredGroups();

        var group = Assert.Single(groups);

        Assert.Equal("1", group.StreamId.ToString());
        Assert.Equal("2", group.TopicId.ToString());
        Assert.Equal("group", group.GroupId.ToString());
    }

    [Fact]
    public void NextBalancedPartition_RoundRobinsThenWraps()
    {
        var state = new ConsumerGroupClientState();

        Assert.Equal<uint[]>([0, 1, 2, 0], [
            state.NextBalancedPartition(Topic, 3), state.NextBalancedPartition(Topic, 3),
            state.NextBalancedPartition(Topic, 3), state.NextBalancedPartition(Topic, 3)
        ]);
    }

    [Fact]
    public void NextBalancedPartition_WithNoPartitionsIsZero()
    {
        Assert.Equal(0u, new ConsumerGroupClientState().NextBalancedPartition(Topic, 0));
    }

    [Fact]
    public void ClearSessionScoped_DropsMembershipAndAssignmentsButKeepsTopicState()
    {
        var state = new ConsumerGroupClientState();
        state.RegisterGroup(Key, Identifier.Numeric(1), Identifier.Numeric(2), Identifier.Numeric(3));
        state.SetAssignment(Key, 1, [0]);
        state.SetPartitionCount(Topic, 4);
        state.NextBalancedPartition(Topic, 4);

        state.ClearSessionScoped();

        Assert.False(state.IsRegistered(Key));
        Assert.False(state.HasAssignment(Key));
        Assert.Equal(4u, state.PartitionCount(Topic));
        Assert.Equal(1u, state.NextBalancedPartition(Topic, 4));
    }

    [Fact]
    public void TopicKey_SeparatesNumericFromNamedIdentifiers()
    {
        Assert.NotEqual(new TopicKey(Identifier.Numeric(1), Identifier.Numeric(1)),
            new TopicKey(Identifier.String("1"), Identifier.String("1")));
    }

    /// <summary>
    ///     Message-key partitioning has to agree with the Rust client byte for byte, or the two SDKs put the same
    ///     key on different partitions. Vectors come from <c>calculate_32</c> (<c>XxHash32::oneshot(0, data)</c>).
    /// </summary>
    [Theory]
    [InlineData("", 0x02cc5d05u)]
    [InlineData("a", 0x550d7456u)]
    [InlineData("abc", 0x32d153ffu)]
    [InlineData("hello world", 0xcebb6622u)]
    [InlineData("iggy-message-key", 0xf54b51c9u)]
    [InlineData("1234567890123456789012345", 0xb10c970eu)]
    public void XxHash32_MatchesRustVectors(string value, uint expected)
    {
        Assert.Equal(expected, XxHash32.HashToUInt32(Encoding.UTF8.GetBytes(value)));
    }
}
