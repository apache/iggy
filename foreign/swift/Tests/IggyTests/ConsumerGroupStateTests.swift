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

import Testing

@testable import Iggy

/// A port of the tests of `ConsumerGroupClientState` in `core/common`.
@Suite("Consumer group state")
struct ConsumerGroupStateTests {
    private let group = ConsumerGroupState.GroupKey(stream: 1, topic: 2, group: 3)
    private let topic = ConsumerGroupState.TopicKey(stream: 1, topic: 2)

    private static func partitions(_ count: UInt32) -> [Partition] {
        (0..<count).map { id in
            Partition(
                id: id, createdAt: .zero, segmentsCount: 1, currentOffset: 0, sizeBytes: 0, messagesCount: 0,
                context: PartitionContext(incarnation: UInt64(id) + 1, ownerGeneration: 0, metadataOp: 10))
        }
    }

    @Test func topicIdentifiersDoNotAliasAndDiscoveryRefreshKeepsTheCursor() {
        let state = ConsumerGroupState()
        let numeric = ConsumerGroupState.TopicKey(stream: 1, topic: 2)
        let named = ConsumerGroupState.TopicKey(stream: 1, topic: "orders")
        state.setTopicPartitions(numeric, partitions: Self.partitions(3))
        #expect(state.partitionCount(named) == nil)
        #expect(state.nextBalancedPartition(numeric, partitionCount: 3) == 0)
        state.invalidateTopicDiscovery()
        #expect(state.partitionCount(numeric) == nil)
        #expect(state.nextBalancedPartition(numeric, partitionCount: 3) == 1)
    }

    @Test func groupPartitionsRoundRobinThenWrap() {
        let state = ConsumerGroupState()
        state.applySync(group, member: (1, [0, 1, 2]), scope: state.sessionScope)
        #expect((0..<4).map { _ in state.nextGroupPartition(group) } == [0, 1, 2, 0])
    }

    @Test func aGenerationChangeRestartsTheCursor() {
        let state = ConsumerGroupState()
        state.applySync(group, member: (1, [0, 1, 2]), scope: state.sessionScope)
        #expect(state.nextGroupPartition(group) == 0)
        #expect(state.nextGroupPartition(group) == 1)
        state.applySync(group, member: (2, [5, 6]), scope: state.sessionScope)
        #expect(state.nextGroupPartition(group) == 5)
    }

    @Test func balancedRoundRobins() {
        let state = ConsumerGroupState()
        #expect((0..<4).map { _ in state.nextBalancedPartition(topic, partitionCount: 3) } == [0, 1, 2, 0])
        #expect(state.nextBalancedPartition(topic, partitionCount: 0) == 0)
    }

    @Test func aRouteContextDoesNotInventAPartitionCount() {
        let state = ConsumerGroupState()
        let context = PartitionContext(incarnation: 9, ownerGeneration: 0, metadataOp: 4)
        state.setPartitionContext(topic, partitionID: 2, context)
        #expect(state.partitionContext(topic, partitionID: 2) == context)
        #expect(state.partitionCount(topic) == nil)
        state.setTopicPartitions(topic, partitions: Self.partitions(3))
        #expect(state.partitionCount(topic) == 3)
        #expect(state.partitionContext(topic, partitionID: 2) == Self.partitions(3)[2].context)
        state.invalidateTopic(topic)
        #expect(state.partitionContext(topic, partitionID: 2) == nil)
        #expect(state.partitionCount(topic) == nil)
    }

    @Test func aMissingAssignmentYieldsNothing() {
        let state = ConsumerGroupState()
        #expect(!state.hasAssignment(group))
        #expect(state.nextGroupPartition(group) == nil)
    }

    @Test func aMemberHoldingNoPartitionsStaysRegistered() {
        let state = ConsumerGroupState()
        state.applySync(group, member: (1, []), scope: state.sessionScope)
        #expect(!state.hasAssignment(group))
        #expect(state.isRegistered(group))
        state.deregisterGroup(group)
        #expect(!state.isRegistered(group))
    }

    @Test func aSessionResetDropsMembershipAndKeepsTopicState() {
        let state = ConsumerGroupState()
        state.applySync(group, member: (1, [0]), scope: state.sessionScope)
        state.setTopicPartitions(topic, partitions: Self.partitions(2))
        #expect(state.nextBalancedPartition(topic, partitionCount: 2) == 0)
        state.clearSessionScoped()
        #expect(state.registeredGroups.isEmpty)
        #expect(!state.isRegistered(group))
        #expect(!state.hasAssignment(group))
        #expect(state.partitionCount(topic) == 2)
        #expect(state.nextBalancedPartition(topic, partitionCount: 2) == 1)
    }

    @Test func aSyncAnsweredUnderAnEndedSessionIsDropped() {
        let state = ConsumerGroupState()
        let scope = state.sessionScope
        state.clearSessionScoped()
        #expect(!state.applySync(group, member: (1, [0]), scope: scope))
        #expect(!state.isRegistered(group))
        #expect(!state.hasAssignment(group))
        state.applySync(group, member: (1, [0]), scope: state.sessionScope)
        #expect(!state.applySync(group, member: nil, scope: scope))
        #expect(state.isRegistered(group))
    }
}
