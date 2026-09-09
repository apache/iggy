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

/// Client-side consumer-group and partitioning state, a port of
/// `core/common/src/consumer_group_client_state.rs`.
///
/// Under the consensus protocol the client routes partition operations
/// itself: consumer-group polls pick the next of the member's assigned
/// partitions, balanced produces round-robin per topic, and keyed produces
/// hash the key. Cursors live here because they must persist across calls.
actor ConsumerGroupState {
    struct GroupKey: Hashable {
        let stream: Identifier
        let topic: Identifier
        let group: Identifier
    }

    struct TopicKey: Hashable {
        let stream: Identifier
        let topic: Identifier
    }

    private struct Assignment {
        var partitions: [UInt32]
        var generation: UInt64
        var cursor: Int
    }

    private var assignments: [GroupKey: Assignment] = [:]
    private var balancedCursors: [TopicKey: Int] = [:]
    private var partitionCounts: [TopicKey: UInt32] = [:]
    private var joinedGroups: Set<GroupKey> = []

    /// True if a non-empty assignment is cached for the group.
    func hasAssignment(_ key: GroupKey) -> Bool {
        !(assignments[key]?.partitions.isEmpty ?? true)
    }

    /// Replaces the cached assignment. A generation change (a rebalance)
    /// resets the round-robin cursor.
    func setAssignment(_ key: GroupKey, generation: UInt64, partitions: [UInt32]) {
        var entry = assignments[key] ?? Assignment(partitions: [], generation: generation, cursor: 0)
        if entry.generation != generation {
            entry.cursor = 0
        }
        entry.generation = generation
        entry.partitions = partitions
        assignments[key] = entry
    }

    func invalidateAssignment(_ key: GroupKey) {
        assignments[key] = nil
    }

    /// The next assigned partition for a group poll, advancing the cursor.
    func nextGroupPartition(_ key: GroupKey) -> UInt32? {
        guard var entry = assignments[key], !entry.partitions.isEmpty else {
            return nil
        }
        let partition = entry.partitions[entry.cursor % entry.partitions.count]
        entry.cursor &+= 1
        assignments[key] = entry
        return partition
    }

    /// The next balanced produce partition for a topic, advancing the cursor.
    func nextBalancedPartition(_ key: TopicKey, partitionCount: UInt32) -> UInt32 {
        guard partitionCount > 0 else {
            return 0
        }
        let cursor = balancedCursors[key] ?? 0
        balancedCursors[key] = cursor &+ 1
        return UInt32(cursor % Int(partitionCount))
    }

    func partitionCount(_ key: TopicKey) -> UInt32? {
        partitionCounts[key]
    }

    func setPartitionCount(_ key: TopicKey, _ count: UInt32) {
        partitionCounts[key] = count
    }

    func invalidatePartitionCount(_ key: TopicKey) {
        partitionCounts[key] = nil
    }

    func registerGroup(_ key: GroupKey) {
        joinedGroups.insert(key)
    }

    func deregisterGroup(_ key: GroupKey) {
        joinedGroups.remove(key)
    }

    /// Whether the last sync observed this client as a member. A member
    /// holding zero partitions is still registered, which is a different
    /// question than ``hasAssignment(_:)``.
    func isRegistered(_ key: GroupKey) -> Bool {
        joinedGroups.contains(key)
    }

    var registeredGroups: [GroupKey] {
        Array(joinedGroups)
    }

    /// Drops what the consensus session owned. Membership is per connection,
    /// so nothing synced under the old session holds once it is reset. Topic
    /// state stays: it belongs to a topic, not a session.
    func clearSessionScoped() {
        assignments.removeAll()
        joinedGroups.removeAll()
    }
}
