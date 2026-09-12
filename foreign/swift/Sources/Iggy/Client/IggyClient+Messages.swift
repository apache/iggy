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

extension IggyClient {
    /// Max attempts to resolve a fenced consumer-group poll: one re-sync after
    /// the coordinator rejects a stale assignment, then retry once.
    private static let groupPollMaxAttempts = 2

    /// Sends a batch of messages. Delivery is at-least-once.
    ///
    /// `balanced` and `messagesKey` partitioning are resolved on the client
    /// before the batch is sent, since the server only routes explicit
    /// partitions. A zero message id is replaced with a random one.
    ///
    /// The returned confirmations may be empty: the server can commit a batch
    /// it has no offsets to describe. A `baseOffset` reports an in-memory
    /// commit, not an fsync, and an earlier replay may already have committed
    /// the same batch at a lower offset.
    @discardableResult
    public func sendMessages(
        streamID: Identifier, topicID: Identifier, partitioning: Partitioning = .balanced, messages: [IggyMessage]
    ) async throws -> SendMessagesResponse {
        try await requireAuthenticated()
        guard !messages.isEmpty else {
            throw IggyError(.invalidMessagesCount)
        }
        try partitioning.validate()
        let resolved = try await resolvePartitioning(partitioning, streamID: streamID, topicID: topicID)
        let outgoing = messages.map { message in
            Batch.OutgoingMessage(
                id: message.id.isZero ? .random() : message.id,
                originTimestamp: message.originTimestamp.microseconds,
                payload: message.payload,
                userHeaders: message.rawUserHeaders)
        }
        let payload = try Batch.encodeSend(streamID: streamID, topicID: topicID, partitioning: resolved, messages: outgoing)
        let body: [UInt8]
        do {
            body = try await send(.sendMessages, payload)
        } catch let error as IggyError where error.code == .partitionNotFound {
            // The cached count pointed this send at a partition the server does
            // not have, so the topic was likely recreated smaller.
            await core.groupState.invalidatePartitionCount(.init(stream: streamID, topic: topicID))
            throw error
        }
        // The batch already committed once the reply arrived. An unreadable
        // confirmation must not make a retrying caller write it twice.
        return (try? Responses.sendConfirmations(body[...])) ?? SendMessagesResponse()
    }

    /// Polls up to `count` messages.
    ///
    /// A consumer-group poll with `partitionID` nil picks one of the member's
    /// assigned partitions on the client, round-robin, syncing the assignment
    /// from the coordinator when needed. A member that holds no partitions
    /// gets an empty result whose partition id is
    /// ``PolledMessages/noAssignedPartition``; a non-member gets
    /// ``IggyErrorCode/consumerGroupMemberNotFound`` so it can rejoin.
    ///
    /// With `autoCommit` the server stores the offset of the last polled
    /// message as consumed.
    public func pollMessages(
        streamID: Identifier, topicID: Identifier, partitionID: UInt32?, consumer: Consumer, strategy: PollingStrategy, count: UInt32, autoCommit: Bool
    ) async throws -> PolledMessages {
        try await pollMessages(streamID: streamID, topicID: topicID, partitionID: partitionID, consumer: consumer, count: count, autoCommit: autoCommit) { _ in
            strategy
        }
    }

    /// ``pollMessages(streamID:topicID:partitionID:consumer:strategy:count:autoCommit:)``
    /// whose strategy is chosen once the partition is known, so a caller can
    /// continue every partition from its own position.
    public func pollMessages(
        streamID: Identifier, topicID: Identifier, partitionID: UInt32?, consumer: Consumer, count: UInt32, autoCommit: Bool,
        strategyFor: @Sendable (UInt32) -> PollingStrategy
    ) async throws -> PolledMessages {
        try await requireAuthenticated()
        guard count > 0 else {
            throw IggyError(.invalidMessagesCount)
        }
        if consumer.kind == .consumerGroup, partitionID == nil {
            return try await pollGroup(streamID: streamID, topicID: topicID, consumer: consumer, count: count, autoCommit: autoCommit, strategyFor: strategyFor)
        }
        let strategy = strategyFor(partitionID ?? 0)
        let payload = Requests.pollMessages(
            consumer: consumer, streamID: streamID, topicID: topicID, partitionID: partitionID, strategy: strategy, count: count, autoCommit: autoCommit)
        return try Responses.polledMessages(try await send(.pollMessages, payload)[...])
    }

    /// Forces the partition's unsaved buffer to disk, optionally with fsync.
    public func flushUnsavedBuffer(streamID: Identifier, topicID: Identifier, partitionID: UInt32, fsync: Bool) async throws {
        try await requireAuthenticated()
        _ = try await send(.flushUnsavedBuffer, Requests.flushUnsavedBuffer(streamID: streamID, topicID: topicID, partitionID: partitionID, fsync: fsync))
    }

    // MARK: Client-side partitioning

    private func resolvePartitioning(_ partitioning: Partitioning, streamID: Identifier, topicID: Identifier) async throws -> Partitioning {
        switch partitioning {
        case .partition:
            return partitioning
        case .balanced:
            let count = try await partitionCount(streamID: streamID, topicID: topicID)
            let partition = await core.groupState.nextBalancedPartition(.init(stream: streamID, topic: topicID), partitionCount: count)
            return .partition(partition)
        case .messagesKey(let key):
            let count = try await partitionCount(streamID: streamID, topicID: topicID)
            return .partition(XXH32.hash(key) % count)
        }
    }

    /// The topic's partition count, cached so a send does not pay a metadata
    /// round trip per batch.
    private func partitionCount(streamID: Identifier, topicID: Identifier) async throws -> UInt32 {
        let key = ConsumerGroupState.TopicKey(stream: streamID, topic: topicID)
        if let cached = await core.groupState.partitionCount(key) {
            return cached
        }
        guard let topic = try await getTopic(streamID: streamID, topicID: topicID), topic.partitionsCount > 0 else {
            throw IggyError(.topicIdNotFound, context: "topic \(topicID) in stream \(streamID)")
        }
        await core.groupState.setPartitionCount(key, topic.partitionsCount)
        return topic.partitionsCount
    }

    // MARK: Consumer-group polling

    /// Syncs the member's assignment from the coordinator into the cache. An
    /// empty reply means the client is not a member.
    func syncGroupAssignment(streamID: Identifier, topicID: Identifier, groupID: Identifier) async throws -> ConsumerGroupAssignment? {
        let body = try await send(.syncConsumerGroup, Requests.consumerGroup(streamID: streamID, topicID: topicID, groupID: groupID))
        let key = ConsumerGroupState.GroupKey(stream: streamID, topic: topicID, group: groupID)
        if body.isEmpty {
            // The deregister is the only thing that observes a server-side
            // removal; without it every later poll would return empty instead
            // of surfacing the missing membership.
            await core.groupState.invalidateAssignment(key)
            await core.groupState.deregisterGroup(key)
            return nil
        }
        let assignment = try Responses.consumerGroupAssignment(body[...])
        await core.groupState.registerGroup(key)
        await core.groupState.setAssignment(key, generation: assignment.generation, partitions: assignment.partitions)
        return assignment
    }

    /// Re-syncs every joined group so a member picks up a widened assignment
    /// without first hitting an ownership fence. Driven by the heartbeat.
    func refreshConsumerGroupAssignments() async {
        for key in await core.groupState.registeredGroups {
            do {
                _ = try await syncGroupAssignment(streamID: key.stream, topicID: key.topic, groupID: key.group)
            } catch {
                logger.warning("Failed to refresh the consumer-group assignment for \(key.stream)|\(key.topic)|\(key.group): \(error)")
            }
        }
    }

    private func pollGroup(
        streamID: Identifier, topicID: Identifier, consumer: Consumer, count: UInt32, autoCommit: Bool, strategyFor: @Sendable (UInt32) -> PollingStrategy
    ) async throws -> PolledMessages {
        let key = ConsumerGroupState.GroupKey(stream: streamID, topic: topicID, group: consumer.id)
        if await !core.groupState.hasAssignment(key) {
            _ = try await syncGroupAssignment(streamID: streamID, topicID: topicID, groupID: consumer.id)
        }
        for _ in 0..<Self.groupPollMaxAttempts {
            guard let partitionID = await core.groupState.nextGroupPartition(key) else {
                // A real member can hold zero partitions, while a non-member
                // must learn it is one so it can rejoin.
                if await !core.groupState.isRegistered(key) {
                    throw IggyError(.consumerGroupMemberNotFound, context: "group \(consumer.id) in topic \(topicID)")
                }
                var empty = PolledMessages.empty
                empty.partitionID = PolledMessages.noAssignedPartition
                return empty
            }
            let strategy = strategyFor(partitionID)
            let payload = Requests.pollMessages(
                consumer: consumer, streamID: streamID, topicID: topicID, partitionID: partitionID, strategy: strategy, count: count, autoCommit: autoCommit)
            do {
                let polled = try Responses.polledMessages(try await send(.pollMessages, payload)[...])
                if polled.messages.isEmpty, polled.partitionID == PolledMessages.resyncRequiredPartition {
                    // The coordinator signals a generation fence through the
                    // empty-poll body; re-sync and retry.
                    await core.groupState.invalidateAssignment(key)
                    _ = try await syncGroupAssignment(streamID: streamID, topicID: topicID, groupID: consumer.id)
                    continue
                }
                return polled
            } catch let error as IggyError where error.code == .consumerGroupPartitionNotOwned {
                await core.groupState.invalidateAssignment(key)
                _ = try await syncGroupAssignment(streamID: streamID, topicID: topicID, groupID: consumer.id)
            }
        }
        // Back-to-back fences exhausted the budget; the cursor is re-synced and
        // the caller simply re-polls.
        return .empty
    }
}
