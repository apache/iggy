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

// MARK: - Messages

extension IggyClient {
    /// One sync and one retry: a poll the coordinator fences for a stale
    /// assignment syncs it again and polls once more.
    private static let groupPollMaxAttempts = 2

    /// Sends a batch of messages to one partition. Delivery is at least once.
    ///
    /// The server routes explicit partitions only, so `balanced` and
    /// `messagesKey` partitioning are resolved on the client: balanced
    /// round-robins over the topic's partitions, a key is hashed with XXH32.
    /// A zero message id is replaced with a random one and written back into
    /// `messages`, as the Rust client does, so a send retried after
    /// ``IggyErrorCode/transientNotCommitted`` carries the same ids.
    ///
    /// The returned confirmations may be empty: the server can commit a batch
    /// it has no offsets to describe.
    @discardableResult
    public func sendMessages(
        streamID: Identifier, topicID: Identifier, partitioning: Partitioning = .balanced, messages: inout [IggyMessage]
    ) async throws -> SendMessagesResponse {
        try await sendMessages(streamID: streamID, topicID: topicID, partitioning: partitioning, messages: &messages, lineage: nil)
    }

    /// ``sendMessages(streamID:topicID:partitioning:messages:)`` for a send
    /// pinned to `captured`, so a retry after the caller signed in or out is
    /// never written under the new session.
    func sendMessages(
        streamID: Identifier, topicID: Identifier, partitioning: Partitioning = .balanced, messages: inout [IggyMessage], lineage captured: UInt64?
    ) async throws -> SendMessagesResponse {
        let lineage = try await requireAuthenticated(under: captured)
        // Checked before a balanced or keyed send reads the topic.
        guard !messages.isEmpty else {
            throw IggyError(.invalidMessagesCount, context: "a batch needs at least one message")
        }
        // The key is hashed to a partition before the batch is encoded, so its
        // length is checked first.
        try partitioning.validate()
        let partitionID = try await resolvePartition(partitioning, streamID: streamID, topicID: topicID, lineage: lineage)
        for index in messages.indices where messages[index].id.isZero {
            messages[index].id = .random()
        }
        let outgoing = messages.map { message in
            Batch.OutgoingMessage(
                id: message.id, originTimestamp: message.originTimestamp.microseconds, payload: message.payload, userHeaders: message.rawUserHeaders)
        }
        let payload = try Batch.encodeSend(streamID: streamID, topicID: topicID, partitioning: .partition(partitionID), messages: outgoing)
        let body = try await sendWithPartitionContext(streamID: streamID, topicID: topicID, partitionID: partitionID, payload: payload, lineage: lineage)
        // The batch committed once the reply arrived. An unreadable
        // confirmation must not make a retrying caller write it twice.
        return (try? Responses.sendConfirmations(body[...])) ?? SendMessagesResponse()
    }

    /// Polls up to `count` messages.
    ///
    /// A consumer-group poll with `partitionID` nil takes the next of the
    /// member's assigned partitions, round-robin, syncing the assignment from
    /// the coordinator when needed. A member holding no partitions gets an
    /// empty result whose partition id is ``PolledMessages/noAssignedPartition``,
    /// and a client that is not a member gets
    /// ``IggyErrorCode/consumerGroupMemberNotFound``, so it can join again.
    ///
    /// With `autoCommit` the server stores the offset of the last polled
    /// message as consumed. A `strategy` with a ``PollingStrategy/context``
    /// is refused once its partition was recreated.
    public func pollMessages(
        streamID: Identifier, topicID: Identifier, partitionID: UInt32?, consumer: Consumer = .default, strategy: PollingStrategy, count: UInt32,
        autoCommit: Bool
    ) async throws -> PolledMessages {
        try await pollMessages(streamID: streamID, topicID: topicID, partitionID: partitionID, consumer: consumer, count: count, autoCommit: autoCommit) {
            _ in strategy
        }
    }

    /// ``pollMessages(streamID:topicID:partitionID:consumer:strategy:count:autoCommit:)``
    /// with the strategy chosen once the partition is known, so a caller can
    /// continue each partition from its own position.
    public func pollMessages(
        streamID: Identifier, topicID: Identifier, partitionID: UInt32?, consumer: Consumer = .default, count: UInt32, autoCommit: Bool,
        strategyFor: @Sendable (UInt32) -> PollingStrategy
    ) async throws -> PolledMessages {
        let lineage = try await requireAuthenticated()
        if consumer.kind == .consumerGroup, partitionID == nil {
            return try await pollGroup(
                streamID: streamID, topicID: topicID, consumer: consumer, count: count, autoCommit: autoCommit, lineage: lineage, strategyFor: strategyFor)
        }
        let strategy = strategyFor(partitionID ?? 0)
        let payload = try Requests.pollMessages(
            consumer: consumer, streamID: streamID, topicID: topicID, partitionID: partitionID, strategy: strategy, count: count, autoCommit: autoCommit)
        return try Responses.polledMessages(
            try await routedPartitionRequest(.pollMessages, payload: payload, context: strategy.context, lineage: lineage)[...])
    }

    // MARK: Partition contexts

    /// Sends under the partition's cached context, or one read from its
    /// route. A history refusal means the context is stale: the topic's
    /// contexts are dropped and one fresh route is tried. A refusal, such as
    /// a namespace miss the metadata primary did not confirm, sends again on
    /// a fresh route until `requestTimeout` from the call passes.
    private func sendWithPartitionContext(
        streamID: Identifier, topicID: Identifier, partitionID: UInt32, payload: [UInt8], lineage: UInt64
    ) async throws -> [UInt8] {
        let deadline = ContinuousClock.now + core.configuration.requestTimeout
        let key = ConsumerGroupState.TopicKey(stream: streamID, topic: topicID)
        let route = PollRouter.RouteKey(stream: streamID, topic: topicID, partitionID: partitionID)
        let state = core.groupState
        var context = state.partitionContext(key, partitionID: partitionID)
        var refreshAvailable = true
        var backoff = PollRouter.Backoff()
        while true {
            do {
                let captured: PartitionContext
                if let context {
                    captured = context
                } else {
                    captured = try await core.pollRouter.sendContext(route, lineage: lineage, coordinator: core, deadline: deadline)
                    state.setPartitionContext(key, partitionID: partitionID, captured)
                }
                return try await core.pollRouter.sendMessages(payload, context: captured, lineage: lineage, coordinator: core, deadline: deadline)
            } catch let error as IggyError where error.code == .historyUnavailable {
                state.invalidateTopic(key)
                guard refreshAvailable else {
                    throw error
                }
                refreshAvailable = false
                context = nil
            } catch let error as IggyError where error.code == .transientNotAccepted {
                context = nil
                try await backoff.pause(after: error, before: deadline)
            }
        }
    }

    /// A poll or a consumer-offset request on the route of the partition its
    /// payload names, as `OperationDriver::send_raw` in the Rust SDK picks
    /// it. A plain consumer's poll and an offset read without a partition go
    /// to partition 0. A group poll, a store, and a delete must name one.
    func routedPartitionRequest(_ code: CommandCode, payload: [UInt8], context: PartitionContext?, lineage: UInt64) async throws -> [UInt8] {
        let target = try RawRequest.consumerTarget(payload)
        let partitionID: UInt32
        if let named = target.partitionID {
            partitionID = named
        } else if code == .getConsumerOffset || (code == .pollMessages && target.consumerKind == Consumer.Kind.consumer.rawValue) {
            partitionID = 0
        } else {
            throw IggyError(.invalidIdentifier, context: "command \(code.rawValue) needs a partition id")
        }
        let key = PollRouter.RouteKey(stream: target.stream, topic: target.topic, partitionID: partitionID)
        return try await core.pollRouter.send(
            code, payload: payload, key: key, context: context, lineage: lineage, coordinator: core,
            deadline: ContinuousClock.now + PollRouter.pollTimeout)
    }

    // MARK: Client-side partitioning

    private func resolvePartition(_ partitioning: Partitioning, streamID: Identifier, topicID: Identifier, lineage: UInt64) async throws -> UInt32 {
        switch partitioning {
        case .partition(let partitionID):
            return partitionID
        case .balanced:
            let count = try await partitionCount(streamID: streamID, topicID: topicID, lineage: lineage)
            return core.groupState.nextBalancedPartition(.init(stream: streamID, topic: topicID), partitionCount: count)
        case .messagesKey(let key):
            return XXH32.hash(key) % (try await partitionCount(streamID: streamID, topicID: topicID, lineage: lineage))
        }
    }

    /// The topic's partition count, cached by the topic read so a send does
    /// not pay a metadata round trip per batch.
    private func partitionCount(streamID: Identifier, topicID: Identifier, lineage: UInt64) async throws -> UInt32 {
        var count = core.groupState.partitionCount(.init(stream: streamID, topic: topicID))
        if count == nil {
            count = try await getTopic(streamID: streamID, topicID: topicID, lineage: lineage)?.partitionsCount
        }
        guard let count, count > 0 else {
            throw IggyError(.topicIdNotFound, context: "topic \(topicID) in stream \(streamID)")
        }
        return count
    }

    // MARK: Consumer-group polling

    /// Syncs the member's assignment from the coordinator into the cache. An
    /// empty reply means the client is not a member. A reply that arrives
    /// after the session was replaced is dropped. `lineage` is the one of the
    /// call the sync belongs to, nil for a sync of its own.
    func syncGroupAssignment(streamID: Identifier, topicID: Identifier, groupID: Identifier, lineage: UInt64?) async throws {
        let state = core.groupState
        let scope = state.sessionScope
        let body = try await send(.syncConsumerGroup, Requests.consumerGroup(streamID: streamID, topicID: topicID, groupID: groupID), lineage: lineage)
        let key = ConsumerGroupState.GroupKey(stream: streamID, topic: topicID, group: groupID)
        if body.isEmpty {
            // The coordinator answers any member, even one holding no
            // partitions, with an assignment, so this observes a removal: a
            // deleted group or an evicted member. Without the deregister every
            // later poll would come back empty instead of telling the caller.
            state.applySync(key, member: nil, scope: scope)
            return
        }
        let assignment = try Responses.consumerGroupAssignment(body[...])
        state.applySync(key, member: (assignment.generation, assignment.partitions), scope: scope)
    }

    /// Syncs every joined group, so a member picks up a widened assignment
    /// without first hitting an ownership fence. The heartbeat runs it; one
    /// group that fails does not stop the others.
    func refreshConsumerGroupAssignments() async {
        for key in core.groupState.registeredGroups {
            do {
                try await syncGroupAssignment(streamID: key.stream, topicID: key.topic, groupID: key.group, lineage: nil)
            } catch {
                logger.warning("Failed to refresh the consumer group assignment of \(key.stream)|\(key.topic)|\(key.group): \(error)")
            }
        }
    }

    private func pollGroup(
        streamID: Identifier, topicID: Identifier, consumer: Consumer, count: UInt32, autoCommit: Bool, lineage: UInt64,
        strategyFor: @Sendable (UInt32) -> PollingStrategy
    ) async throws -> PolledMessages {
        let key = ConsumerGroupState.GroupKey(stream: streamID, topic: topicID, group: consumer.id)
        let state = core.groupState
        if !state.hasAssignment(key) {
            try await syncGroupAssignment(streamID: streamID, topicID: topicID, groupID: consumer.id, lineage: lineage)
        }
        for _ in 0..<Self.groupPollMaxAttempts {
            guard let partitionID = state.nextGroupPartition(key) else {
                // A member can hold no partitions, while a client that is not
                // a member must learn it, or it polls an empty assignment
                // forever instead of joining again.
                guard state.isRegistered(key) else {
                    throw IggyError(.consumerGroupMemberNotFound, context: "group \(consumer.id) in topic \(topicID) of stream \(streamID)")
                }
                var empty = PolledMessages.empty
                empty.partitionID = PolledMessages.noAssignedPartition
                return empty
            }
            // A retry after a fence can land on another partition, so the
            // strategy is asked for each attempt.
            let strategy = strategyFor(partitionID)
            let payload = try Requests.pollMessages(
                consumer: consumer, streamID: streamID, topicID: topicID, partitionID: partitionID, strategy: strategy, count: count, autoCommit: autoCommit)
            do {
                let polled = try Responses.polledMessages(
                    try await routedPartitionRequest(.pollMessages, payload: payload, context: strategy.context, lineage: lineage)[...])
                if polled.messages.isEmpty, polled.partitionID == PolledMessages.resyncRequiredPartition {
                    // The coordinator fences a stale assignment through the
                    // empty poll, for want of a typed error.
                    state.invalidateAssignment(key)
                    try await syncGroupAssignment(streamID: streamID, topicID: topicID, groupID: consumer.id, lineage: lineage)
                    continue
                }
                return polled
            } catch let error as IggyError where error.code == .consumerGroupPartitionNotOwned {
                state.invalidateAssignment(key)
                try await syncGroupAssignment(streamID: streamID, topicID: topicID, groupID: consumer.id, lineage: lineage)
                if strategy.context != nil {
                    // A continuation of the partition the member lost cannot
                    // move to another one.
                    throw error
                }
            }
        }
        // Every attempt met a rebalance. The assignment is synced again and
        // an empty poll lets the caller poll again, where an error would make
        // a normal rebalance look like a failure.
        return .empty
    }
}

// MARK: - Consumer offsets

extension IggyClient {
    /// Stores the offset of a consumer on a partition. A consumer-group
    /// consumer stores it for the group. A nil `partitionID` fails with
    /// ``IggyErrorCode/invalidIdentifier`` before anything is sent.
    public func storeConsumerOffset(
        consumer: Consumer = .default, streamID: Identifier, topicID: Identifier, partitionID: UInt32?, offset: UInt64
    )
        async throws
    {
        let lineage = try await requireAuthenticated()
        _ = try await routedPartitionRequest(
            .storeConsumerOffset,
            payload: Requests.storeConsumerOffset(
                consumer: consumer, streamID: streamID, topicID: topicID, partitionID: partitionID, offset: offset, ack: .quorum),
            context: nil, lineage: lineage)
    }

    /// Stores a checkpoint under the partition context of the poll that
    /// delivered it. It is refused once the partition was recreated or, for a
    /// consumer group, reassigned since.
    public func storeConsumerPosition(consumer: Consumer = .default, streamID: Identifier, topicID: Identifier, position: ConsumerPosition) async throws {
        let lineage = try await requireAuthenticated()
        _ = try await routedPartitionRequest(
            .storeConsumerOffset,
            payload: Requests.storeConsumerOffset(
                consumer: consumer, streamID: streamID, topicID: topicID, partitionID: position.partitionID, offset: position.offset, ack: .quorum),
            context: position.context, lineage: lineage)
    }

    /// The stored offset of a consumer on a partition, or nil when none is
    /// stored. A nil `partitionID` reads partition 0.
    public func getConsumerOffset(
        consumer: Consumer = .default, streamID: Identifier, topicID: Identifier, partitionID: UInt32?
    ) async throws
        -> ConsumerOffsetInfo?
    {
        let lineage = try await requireAuthenticated()
        let body = try await routedPartitionRequest(
            .getConsumerOffset, payload: Requests.getConsumerOffset(consumer: consumer, streamID: streamID, topicID: topicID, partitionID: partitionID),
            context: nil, lineage: lineage)
        return body.isEmpty ? nil : try Responses.consumerOffset(body[...])
    }

    /// Deletes the stored offset of a consumer on a partition. A nil
    /// `partitionID` fails with ``IggyErrorCode/invalidIdentifier`` before
    /// anything is sent.
    public func deleteConsumerOffset(consumer: Consumer = .default, streamID: Identifier, topicID: Identifier, partitionID: UInt32?) async throws {
        let lineage = try await requireAuthenticated()
        _ = try await routedPartitionRequest(
            .deleteConsumerOffset,
            payload: Requests.deleteConsumerOffset(consumer: consumer, streamID: streamID, topicID: topicID, partitionID: partitionID, ack: .quorum),
            context: nil, lineage: lineage)
    }
}

// MARK: - Raw partition requests

extension IggyClient {
    /// Sends a raw partition request the way its typed call sends it, so it
    /// carries the partition context the server requires. Nil for any other
    /// code.
    func sendRawPartitionRequest(code: UInt32, payload: [UInt8], lineage: UInt64) async throws -> [UInt8]? {
        guard let command = CommandCode(rawValue: code) else {
            return nil
        }
        switch command {
        case .sendMessages:
            let target = try RawRequest.sendTarget(payload)
            return try await sendWithPartitionContext(
                streamID: target.stream, topicID: target.topic, partitionID: target.partitionID, payload: payload, lineage: lineage)
        case .pollMessages:
            try RawRequest.validatePoll(payload)
            return try await routedPartitionRequest(.pollMessages, payload: payload, context: nil, lineage: lineage)
        case .getConsumerOffset, .storeConsumerOffset, .deleteConsumerOffset:
            return try await routedPartitionRequest(command, payload: payload, context: nil, lineage: lineage)
        default:
            return nil
        }
    }
}

/// Reads the partition a raw request names. A payload that does not decode
/// fails with ``IggyErrorCode/invalidCommand`` before anything is sent.
private enum RawRequest {
    /// Strategy kind and value, count, and the auto-commit flag.
    static let pollParametersSize = 9 + 4 + 1

    struct SendTarget {
        let stream: Identifier
        let topic: Identifier
        let partitionID: UInt32
    }

    struct ConsumerTarget {
        let consumerKind: UInt8
        let stream: Identifier
        let topic: Identifier
        /// Nil when the request names no partition.
        let partitionID: UInt32?
    }

    /// `[metadata_len:u32][stream][topic][partitioning][messages_count:u32]`
    /// leads the batch, and the fields must fit the declared length. Balanced
    /// and key partitioning are resolved by the typed send from its cache and
    /// cursor, so a raw send names no partition to route.
    static func sendTarget(_ payload: [UInt8]) throws -> SendTarget {
        let (stream, topic, partitioning) = try decode(payload) { reader in
            var metadata = ByteReader(try reader.readPrefixedBytes())
            let stream = try Identifier.decode(from: &metadata)
            let topic = try Identifier.decode(from: &metadata)
            let partitioning = try Partitioning.decode(from: &metadata)
            _ = try metadata.readUInt32()
            return (stream, topic, partitioning)
        }
        guard case .partition(let partitionID) = partitioning else {
            throw IggyError(
                .featureUnavailable, context: "a raw send needs an explicit partition id; use sendMessages for balanced or key partitioning")
        }
        return SendTarget(stream: stream, topic: topic, partitionID: partitionID)
    }

    /// `[consumer kind:1][consumer][stream][topic][has partition:1][partition:4]`
    /// leads a poll and every consumer-offset request.
    static func consumerTarget(_ payload: [UInt8]) throws -> ConsumerTarget {
        try decode(payload) { reader in try readConsumerTarget(&reader) }
    }

    static func validatePoll(_ payload: [UInt8]) throws {
        try decode(payload) { reader in
            _ = try readConsumerTarget(&reader)
            try reader.skip(pollParametersSize)
        }
    }

    private static func readConsumerTarget(_ reader: inout ByteReader) throws -> ConsumerTarget {
        let consumerKind = try reader.readUInt8()
        _ = try Identifier.decode(from: &reader)
        let stream = try Identifier.decode(from: &reader)
        let topic = try Identifier.decode(from: &reader)
        let hasPartition = try reader.readUInt8() != 0
        let partitionID = try reader.readUInt32()
        return ConsumerTarget(consumerKind: consumerKind, stream: stream, topic: topic, partitionID: hasPartition ? partitionID : nil)
    }

    private static func decode<T>(_ payload: [UInt8], _ body: (inout ByteReader) throws -> T) throws -> T {
        do {
            return try Responses.decode(payload[...], body)
        } catch let error as IggyError {
            throw IggyError(.invalidCommand, context: "the raw payload does not decode: \(error)")
        }
    }
}
