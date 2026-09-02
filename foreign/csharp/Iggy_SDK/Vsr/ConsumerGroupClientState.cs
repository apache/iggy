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

namespace Apache.Iggy.Vsr;

/// <summary>
///     Per-connection cache of consumer-group assignments and topic partition counts, mirroring
///     <c>core/common/src/consumer_group_client_state.rs</c>. Under VSR the broker never picks a partition, so
///     the client resolves group polls and balanced / message-key produce locally. The cursors have to survive
///     across calls, which is why this lives on the long-lived transport rather than on a request.
/// </summary>
internal sealed class ConsumerGroupClientState
{
    private readonly Dictionary<GroupKey, GroupAssignment> _assignments = [];
    private readonly Dictionary<TopicKey, int> _balancedCursors = [];
#if NET10_0_OR_GREATER
    private readonly Lock _gate = new();
#else
    private readonly object _gate = new();
#endif
    private readonly Dictionary<GroupKey, GroupIdentifiers> _joinedGroups = [];
    private readonly Dictionary<TopicKey, CachedPartitionCount> _partitionCounts = [];

    /// <summary>Nothing tells this client when another one resizes a topic, so the count expires on its own.</summary>
    private const long PartitionCountTtlMs = 30_000;

    /// <summary>
    ///     True when an assignment is cached for the group, even one holding zero partitions. Treating an empty
    ///     assignment as missing would re-sync on every poll of a member that owns nothing.
    /// </summary>
    internal bool HasAssignment(GroupKey key)
    {
        lock (_gate)
        {
            return _assignments.ContainsKey(key);
        }
    }

    /// <summary>
    ///     Replaces a group's cached assignment. A generation change is a rebalance, so the round-robin cursor
    ///     restarts rather than carrying an index that meant something else.
    /// </summary>
    internal void SetAssignment(GroupKey key, ulong generation, IReadOnlyList<uint> partitions)
    {
        lock (_gate)
        {
            if (!_assignments.TryGetValue(key, out var assignment))
            {
                assignment = new GroupAssignment();
                _assignments[key] = assignment;
            }

            if (assignment.Generation != generation)
            {
                assignment.Cursor = 0;
            }

            assignment.Generation = generation;
            assignment.Partitions = partitions;
        }
    }

    internal void InvalidateAssignment(GroupKey key)
    {
        lock (_gate)
        {
            _assignments.Remove(key);
        }
    }

    /// <summary>
    ///     The next assigned partition for a group poll, advancing the cursor. <c>null</c> when nothing is cached
    ///     or the member holds no partitions.
    /// </summary>
    internal uint? NextGroupPartition(GroupKey key)
    {
        lock (_gate)
        {
            if (!_assignments.TryGetValue(key, out var assignment) || assignment.Partitions.Count == 0)
            {
                return null;
            }

            var index = assignment.Cursor % assignment.Partitions.Count;
            assignment.Cursor = assignment.Cursor == int.MaxValue ? 0 : assignment.Cursor + 1;

            return assignment.Partitions[index];
        }
    }

    /// <summary>The next balanced produce partition for a topic, advancing the cursor.</summary>
    internal uint NextBalancedPartition(TopicKey key, uint partitionCount)
    {
        if (partitionCount == 0)
        {
            return 0;
        }

        lock (_gate)
        {
            _balancedCursors.TryGetValue(key, out var cursor);
            var partition = (uint)(cursor % partitionCount);
            _balancedCursors[key] = cursor == int.MaxValue ? 0 : cursor + 1;

            return partition;
        }
    }

    internal uint? PartitionCount(TopicKey key)
    {
        lock (_gate)
        {
            if (!_partitionCounts.TryGetValue(key, out var cached))
            {
                return null;
            }

            if (Environment.TickCount64 >= cached.ExpiresAt)
            {
                _partitionCounts.Remove(key);

                return null;
            }

            return cached.Count;
        }
    }

    internal void SetPartitionCount(TopicKey key, uint partitionCount)
    {
        lock (_gate)
        {
            _partitionCounts[key] = new CachedPartitionCount(partitionCount,
                Environment.TickCount64 + PartitionCountTtlMs);
        }
    }

    /// <summary>
    ///     Forgets a topic's cached partition count immediately, for the changes this client makes itself.
    ///     Changes made by anyone else are covered by <see cref="PartitionCountTtlMs" />.
    /// </summary>
    internal void InvalidatePartitionCount(TopicKey key)
    {
        lock (_gate)
        {
            _partitionCounts.Remove(key);
        }
    }

    /// <summary>Records a joined group's identifiers so a later refresh can rebuild its sync request.</summary>
    internal void RegisterGroup(GroupKey key, Identifier streamId, Identifier topicId, Identifier groupId)
    {
        lock (_gate)
        {
            _joinedGroups[key] = new GroupIdentifiers(streamId, topicId, groupId);
        }
    }

    internal void DeregisterGroup(GroupKey key)
    {
        lock (_gate)
        {
            _joinedGroups.Remove(key);
            _assignments.Remove(key);
        }
    }

    /// <summary>
    ///     True when the last assignment sync saw this client as a member. A member mid-rebalance, or one holding
    ///     zero partitions, is still registered, so this asks a different question than
    ///     <see cref="HasAssignment" />.
    /// </summary>
    internal bool IsRegistered(GroupKey key)
    {
        lock (_gate)
        {
            return _joinedGroups.ContainsKey(key);
        }
    }

    internal IReadOnlyList<GroupIdentifiers> RegisteredGroups()
    {
        lock (_gate)
        {
            return _joinedGroups.Count == 0 ? [] : [.. _joinedGroups.Values];
        }
    }

    /// <summary>
    ///     Drops what a consensus session owns. The assignments are fenced by a generation the coordinator tracks
    ///     per session, so carrying them across a reset would fence every poll, and membership has to be re-synced
    ///     before it can be trusted again. The balanced cursors and the cached partition counts stay: they belong
    ///     to a topic, not to a session, and clearing them restarts the produce round-robin at partition 0 and
    ///     costs a metadata round trip per topic on every reconnect.
    /// </summary>
    internal void ClearSessionScoped()
    {
        lock (_gate)
        {
            _assignments.Clear();
            _joinedGroups.Clear();
        }
    }

    internal readonly record struct GroupIdentifiers(Identifier StreamId, Identifier TopicId, Identifier GroupId);

    private readonly record struct CachedPartitionCount(uint Count, long ExpiresAt);

    private sealed class GroupAssignment
    {
        internal IReadOnlyList<uint> Partitions { get; set; } = [];
        internal ulong Generation { get; set; }
        internal int Cursor { get; set; }
    }
}

/// <summary>
///     Cache key for a topic. The identifier kind is part of the key because a stream named "1" and the stream
///     with id 1 are different streams that share a rendering. Identifiers are compared by their wire bytes:
///     <see cref="Identifier" /> compares its value array by reference, and every call site builds a fresh one.
/// </summary>
internal readonly struct TopicKey(Identifier streamId, Identifier topicId) : IEquatable<TopicKey>
{
    private Identifier StreamId { get; } = streamId;
    private Identifier TopicId { get; } = topicId;

    public bool Equals(TopicKey other)
    {
        return IdentifierKey.Equal(StreamId, other.StreamId) && IdentifierKey.Equal(TopicId, other.TopicId);
    }

    public override bool Equals(object? obj)
    {
        return obj is TopicKey other && Equals(other);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(IdentifierKey.Hash(StreamId), IdentifierKey.Hash(TopicId));
    }

    public override string ToString()
    {
        return $"{StreamId}|{TopicId}";
    }
}

/// <summary>Cache key for a consumer group on a topic.</summary>
internal readonly struct GroupKey(Identifier streamId, Identifier topicId, Identifier groupId) : IEquatable<GroupKey>
{
    private TopicKey Topic { get; } = new(streamId, topicId);
    private Identifier GroupId { get; } = groupId;

    public bool Equals(GroupKey other)
    {
        return Topic.Equals(other.Topic) && IdentifierKey.Equal(GroupId, other.GroupId);
    }

    public override bool Equals(object? obj)
    {
        return obj is GroupKey other && Equals(other);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(Topic.GetHashCode(), IdentifierKey.Hash(GroupId));
    }

    public override string ToString()
    {
        return $"{Topic}|{GroupId}";
    }
}

internal static class IdentifierKey
{
    internal static bool Equal(Identifier first, Identifier second)
    {
        return first.Kind == second.Kind && first.Value.AsSpan().SequenceEqual(second.Value);
    }

    internal static int Hash(Identifier identifier)
    {
        var hash = new HashCode();
        hash.Add((byte)identifier.Kind);
        hash.AddBytes(identifier.Value);

        return hash.ToHashCode();
    }
}
