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

import NIOConcurrencyHelpers

/// Client-side partitioning state, a port of `ConsumerGroupClientState` in
/// `core/common`.
///
/// The server routes explicit partitions only, so the client resolves the
/// rest before it encodes a request: a consumer-group poll takes the next of
/// the member's assigned partitions, round-robin, from the assignment synced
/// from the coordinator; a `balanced` send round-robins per topic, and a
/// messages key is hashed. The cursors outlive single calls, so this lives on
/// the client. The coordinator fences a stale selection after a rebalance,
/// which makes the client sync again and restart the cursor.
final class ConsumerGroupState: Sendable {
    struct TopicKey: Hashable, Sendable {
        let stream: Identifier
        let topic: Identifier
    }

    struct GroupKey: Hashable, Sendable {
        let stream: Identifier
        let topic: Identifier
        let group: Identifier
    }

    private struct Assignment {
        var partitions: [UInt32] = []
        var generation: UInt64 = 0
        var cursor: UInt = 0
    }

    private struct TopicPartitions {
        var count: UInt32?
        var contexts: [UInt32: PartitionContext] = [:]
        var cursor: UInt = 0
    }

    private struct Storage {
        var assignments: [GroupKey: Assignment] = [:]
        var topics: [TopicKey: TopicPartitions] = [:]
        /// The groups the last sync found this client a member of, so the
        /// heartbeat can sync them again.
        var joinedGroups: Set<GroupKey> = []
        /// Moves on with every session that ends, so a sync answered under a
        /// replaced session is told apart.
        var sessionScope: UInt64 = 0
    }

    private let storage = NIOLockedValueBox(Storage())

    // MARK: Group assignments

    /// Whether a non-empty assignment is cached for the group.
    func hasAssignment(_ key: GroupKey) -> Bool {
        storage.withLockedValue { $0.assignments[key].map { !$0.partitions.isEmpty } ?? false }
    }

    /// The session scope a sync reads before it is sent and hands to
    /// ``applySync(_:member:scope:)``.
    var sessionScope: UInt64 {
        storage.withLockedValue { $0.sessionScope }
    }

    /// Records what a sync read under `scope` found: the member's assignment,
    /// or nil for a client that is not a member. A new generation, a
    /// rebalance, restarts the round-robin. A reply of a session that ended
    /// since is dropped, so it cannot repopulate the session that replaced
    /// it. Reports whether the reply was applied.
    @discardableResult
    func applySync(_ key: GroupKey, member: (generation: UInt64, partitions: [UInt32])?, scope: UInt64) -> Bool {
        storage.withLockedValue { storage in
            guard storage.sessionScope == scope else {
                return false
            }
            guard let member else {
                storage.assignments.removeValue(forKey: key)
                storage.joinedGroups.remove(key)
                return true
            }
            storage.joinedGroups.insert(key)
            var assignment = storage.assignments[key] ?? Assignment()
            if assignment.generation != member.generation {
                assignment.cursor = 0
            }
            assignment.generation = member.generation
            assignment.partitions = member.partitions
            storage.assignments[key] = assignment
            return true
        }
    }

    /// Drops the group's assignment, so the next poll syncs it again.
    func invalidateAssignment(_ key: GroupKey) {
        _ = storage.withLockedValue { $0.assignments.removeValue(forKey: key) }
    }

    /// The next assigned partition to poll, or nil when nothing is assigned.
    func nextGroupPartition(_ key: GroupKey) -> UInt32? {
        storage.withLockedValue { storage in
            guard var assignment = storage.assignments[key], !assignment.partitions.isEmpty else {
                return nil
            }
            let partition = assignment.partitions[Int(assignment.cursor % UInt(assignment.partitions.count))]
            assignment.cursor &+= 1
            storage.assignments[key] = assignment
            return partition
        }
    }

    // MARK: Membership

    /// Forgets a group after leaving it.
    func deregisterGroup(_ key: GroupKey) {
        _ = storage.withLockedValue { $0.joinedGroups.remove(key) }
    }

    /// Whether the last sync found the client a member. A member mid-rebalance
    /// or holding no partitions is still one, unlike ``hasAssignment(_:)``.
    func isRegistered(_ key: GroupKey) -> Bool {
        storage.withLockedValue { $0.joinedGroups.contains(key) }
    }

    var registeredGroups: [GroupKey] {
        storage.withLockedValue { Array($0.joinedGroups) }
    }

    // MARK: Topic partitions

    /// The next `balanced` partition of the topic, advancing its cursor.
    func nextBalancedPartition(_ key: TopicKey, partitionCount: UInt32) -> UInt32 {
        guard partitionCount > 0 else {
            return 0
        }
        return storage.withLockedValue { storage in
            var topic = storage.topics[key] ?? TopicPartitions()
            let partition = UInt32(topic.cursor % UInt(partitionCount))
            topic.cursor &+= 1
            storage.topics[key] = topic
            return partition
        }
    }

    func partitionCount(_ key: TopicKey) -> UInt32? {
        storage.withLockedValue { $0.topics[key]?.count }
    }

    /// Caches what a topic read reported: the partition count, and each
    /// partition's context for the sends that follow.
    func setTopicPartitions(_ key: TopicKey, partitions: [Partition]) {
        guard let count = UInt32(exactly: partitions.count) else {
            return
        }
        storage.withLockedValue { storage in
            var topic = storage.topics[key] ?? TopicPartitions()
            topic.count = count
            topic.contexts = Dictionary(partitions.map { ($0.id, $0.context) }, uniquingKeysWith: { _, last in last })
            storage.topics[key] = topic
        }
    }

    func partitionContext(_ key: TopicKey, partitionID: UInt32) -> PartitionContext? {
        storage.withLockedValue { $0.topics[key]?.contexts[partitionID] }
    }

    func setPartitionContext(_ key: TopicKey, partitionID: UInt32, _ context: PartitionContext) {
        storage.withLockedValue { storage in
            var topic = storage.topics[key] ?? TopicPartitions()
            topic.contexts[partitionID] = context
            storage.topics[key] = topic
        }
    }

    /// Drops the topic's partition count and contexts after the server said
    /// they are stale. The cursor stays.
    func invalidateTopic(_ key: TopicKey) {
        storage.withLockedValue { storage in
            storage.topics[key]?.count = nil
            storage.topics[key]?.contexts.removeAll()
        }
    }

    /// Drops every topic's partition count and contexts after a change that
    /// can renumber partitions. A name and a numeric id can cache the same
    /// topic under two keys, so no single key can be dropped instead.
    func invalidateTopicDiscovery() {
        storage.withLockedValue { storage in
            for key in storage.topics.keys {
                storage.topics[key]?.count = nil
                storage.topics[key]?.contexts.removeAll()
            }
        }
    }

    /// Drops what belonged to the consensus session. Membership is per
    /// session and the coordinator fences assignments by a generation it
    /// tracks per session, so nothing synced under a replaced session holds.
    /// The balanced cursors and the partition counts belong to a topic and
    /// stay, so a reconnect neither restarts the round-robin at partition 0
    /// nor costs a topic read per topic.
    func clearSessionScoped() {
        storage.withLockedValue { storage in
            storage.assignments.removeAll()
            storage.joinedGroups.removeAll()
            storage.sessionScope &+= 1
        }
    }
}
