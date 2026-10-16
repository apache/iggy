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
        try await requireAuthenticated()
        // Checked before a balanced or keyed send reads the topic.
        guard !messages.isEmpty else {
            throw IggyError(.invalidMessagesCount, context: "a batch needs at least one message")
        }
        // The key is hashed to a partition before the batch is encoded, so its
        // length is checked first.
        try partitioning.validate()
        let partitionID = try await resolvePartition(partitioning, streamID: streamID, topicID: topicID)
        for index in messages.indices where messages[index].id.isZero {
            messages[index].id = .random()
        }
        let outgoing = messages.map { message in
            Batch.OutgoingMessage(
                id: message.id, originTimestamp: message.originTimestamp.microseconds, payload: message.payload, userHeaders: message.rawUserHeaders)
        }
        let payload = try Batch.encodeSend(streamID: streamID, topicID: topicID, partitioning: .partition(partitionID), messages: outgoing)
        let body = try await sendWithPartitionContext(streamID: streamID, topicID: topicID, partitionID: partitionID, payload: payload)
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
        try await requireAuthenticated()
        if consumer.kind == .consumerGroup, partitionID == nil {
            return try await pollGroup(streamID: streamID, topicID: topicID, consumer: consumer, count: count, autoCommit: autoCommit, strategyFor: strategyFor)
        }
        let strategy = strategyFor(partitionID ?? 0)
        let payload = try Requests.pollMessages(
            consumer: consumer, streamID: streamID, topicID: topicID, partitionID: partitionID, strategy: strategy, count: count, autoCommit: autoCommit)
        return try Responses.polledMessages(try await routedPoll(payload, context: strategy.context)[...])
    }

    // MARK: Partition contexts

    /// Sends under the partition's cached context, or one read from its
    /// route. A history refusal means the context is stale: the topic's
    /// contexts are dropped and one fresh route is tried.
    private func sendWithPartitionContext(streamID: Identifier, topicID: Identifier, partitionID: UInt32, payload: [UInt8]) async throws -> [UInt8] {
        let key = ConsumerGroupState.TopicKey(stream: streamID, topic: topicID)
        let state = core.groupState
        var context = state.partitionContext(key, partitionID: partitionID)
        var refreshAvailable = true
        while true {
            let captured: PartitionContext
            if let context {
                captured = context
            } else {
                let body = try await send(.getPartitionRoute, Requests.partitionRoute(streamID: streamID, topicID: topicID, partitionID: partitionID))
                captured = try Responses.partitionRoute(body[...])
                state.setPartitionContext(key, partitionID: partitionID, captured)
            }
            do {
                return try await core.exchange(code: CommandCode.sendMessages.rawValue, payload: payload, context: captured)
            } catch let error as IggyError where error.code == .historyUnavailable {
                state.invalidateTopic(key)
                guard refreshAvailable else {
                    throw error
                }
                refreshAvailable = false
                context = nil
            }
        }
    }

    /// A poll on its route. The route key is the poll without its strategy,
    /// count, and auto-commit flag, which do not change where it goes.
    func routedPoll(_ payload: [UInt8], context: PartitionContext?) async throws -> [UInt8] {
        let key = PollRouter.RouteKey(code: .getPollRouting, prefix: payload.dropLast(RawRequest.pollParametersSize))
        return try await core.pollRouter.send(.pollMessagesOnPrimary, payload: payload, key: key, routePayload: payload, context: context, coordinator: core)
    }

    /// An offset write on its route. `routePayload` is the consumer, stream,
    /// topic, and partition the write names.
    func routedOffsetWrite(_ code: CommandCode, payload: [UInt8], routePayload: [UInt8], context: PartitionContext?) async throws {
        let key = PollRouter.RouteKey(code: .getConsumerOffsetRouting, prefix: routePayload[...])
        _ = try await core.pollRouter.send(code, payload: payload, key: key, routePayload: routePayload, context: context, coordinator: core)
    }

    // MARK: Client-side partitioning

    private func resolvePartition(_ partitioning: Partitioning, streamID: Identifier, topicID: Identifier) async throws -> UInt32 {
        switch partitioning {
        case .partition(let partitionID):
            return partitionID
        case .balanced:
            let count = try await partitionCount(streamID: streamID, topicID: topicID)
            return core.groupState.nextBalancedPartition(.init(stream: streamID, topic: topicID), partitionCount: count)
        case .messagesKey(let key):
            return XXH32.hash(key) % (try await partitionCount(streamID: streamID, topicID: topicID))
        }
    }

    /// The topic's partition count, cached by the topic read so a send does
    /// not pay a metadata round trip per batch.
    private func partitionCount(streamID: Identifier, topicID: Identifier) async throws -> UInt32 {
        var count = core.groupState.partitionCount(.init(stream: streamID, topic: topicID))
        if count == nil {
            count = try await getTopic(streamID: streamID, topicID: topicID)?.partitionsCount
        }
        guard let count, count > 0 else {
            throw IggyError(.topicIdNotFound, context: "topic \(topicID) in stream \(streamID)")
        }
        return count
    }

    // MARK: Consumer-group polling

    /// Syncs the member's assignment from the coordinator into the cache. An
    /// empty reply means the client is not a member. A reply that arrives
    /// after the session was replaced is dropped.
    func syncGroupAssignment(streamID: Identifier, topicID: Identifier, groupID: Identifier) async throws {
        let state = core.groupState
        let scope = state.sessionScope
        let body = try await send(.syncConsumerGroup, Requests.consumerGroup(streamID: streamID, topicID: topicID, groupID: groupID))
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
                try await syncGroupAssignment(streamID: key.stream, topicID: key.topic, groupID: key.group)
            } catch {
                logger.warning("Failed to refresh the consumer group assignment of \(key.stream)|\(key.topic)|\(key.group): \(error)")
            }
        }
    }

    private func pollGroup(
        streamID: Identifier, topicID: Identifier, consumer: Consumer, count: UInt32, autoCommit: Bool, strategyFor: @Sendable (UInt32) -> PollingStrategy
    ) async throws -> PolledMessages {
        let key = ConsumerGroupState.GroupKey(stream: streamID, topic: topicID, group: consumer.id)
        let state = core.groupState
        if !state.hasAssignment(key) {
            try await syncGroupAssignment(streamID: streamID, topicID: topicID, groupID: consumer.id)
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
                let polled = try Responses.polledMessages(try await routedPoll(payload, context: strategy.context)[...])
                if polled.messages.isEmpty, polled.partitionID == PolledMessages.resyncRequiredPartition {
                    // The coordinator fences a stale assignment through the
                    // empty poll, for want of a typed error.
                    state.invalidateAssignment(key)
                    try await syncGroupAssignment(streamID: streamID, topicID: topicID, groupID: consumer.id)
                    continue
                }
                return polled
            } catch let error as IggyError where error.code == .consumerGroupPartitionNotOwned {
                state.invalidateAssignment(key)
                try await syncGroupAssignment(streamID: streamID, topicID: topicID, groupID: consumer.id)
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
    /// consumer stores it for the group.
    public func storeConsumerOffset(
        consumer: Consumer = .default, streamID: Identifier, topicID: Identifier, partitionID: UInt32?, offset: UInt64
    )
        async throws
    {
        try await requireAuthenticated()
        try await routedOffsetWrite(
            .storeConsumerOffset,
            payload: Requests.storeConsumerOffset(
                consumer: consumer, streamID: streamID, topicID: topicID, partitionID: partitionID, offset: offset, ack: .quorum),
            routePayload: Requests.getConsumerOffset(consumer: consumer, streamID: streamID, topicID: topicID, partitionID: partitionID), context: nil)
    }

    /// Stores a checkpoint under the partition context of the poll that
    /// delivered it. It is refused once the partition was recreated or, for a
    /// consumer group, reassigned since.
    public func storeConsumerPosition(consumer: Consumer = .default, streamID: Identifier, topicID: Identifier, position: ConsumerPosition) async throws {
        try await requireAuthenticated()
        try await routedOffsetWrite(
            .storeConsumerOffset,
            payload: Requests.storeConsumerOffset(
                consumer: consumer, streamID: streamID, topicID: topicID, partitionID: position.partitionID, offset: position.offset, ack: .quorum),
            routePayload: Requests.getConsumerOffset(consumer: consumer, streamID: streamID, topicID: topicID, partitionID: position.partitionID),
            context: position.context)
    }

    /// The stored offset of a consumer on a partition, or nil when none is
    /// stored.
    public func getConsumerOffset(
        consumer: Consumer = .default, streamID: Identifier, topicID: Identifier, partitionID: UInt32?
    ) async throws
        -> ConsumerOffsetInfo?
    {
        try await requireAuthenticated()
        let body = try await send(
            .getConsumerOffset, Requests.getConsumerOffset(consumer: consumer, streamID: streamID, topicID: topicID, partitionID: partitionID))
        return body.isEmpty ? nil : try Responses.consumerOffset(body[...])
    }

    /// Deletes the stored offset of a consumer on a partition.
    public func deleteConsumerOffset(consumer: Consumer = .default, streamID: Identifier, topicID: Identifier, partitionID: UInt32?) async throws {
        try await requireAuthenticated()
        try await routedOffsetWrite(
            .deleteConsumerOffset,
            payload: Requests.deleteConsumerOffset(consumer: consumer, streamID: streamID, topicID: topicID, partitionID: partitionID, ack: .quorum),
            routePayload: Requests.getConsumerOffset(consumer: consumer, streamID: streamID, topicID: topicID, partitionID: partitionID), context: nil)
    }
}

// MARK: - Raw partition requests

extension IggyClient {
    /// Sends a raw partition request the way its typed call sends it, so it
    /// carries the partition context the server requires. Nil for any other
    /// code.
    func sendRawPartitionRequest(code: UInt32, payload: [UInt8]) async throws -> [UInt8]? {
        switch CommandCode(rawValue: code) {
        case .sendMessages:
            let target = try RawRequest.sendTarget(payload)
            return try await sendWithPartitionContext(streamID: target.stream, topicID: target.topic, partitionID: target.partitionID, payload: payload)
        case .pollMessages:
            try RawRequest.validatePoll(payload)
            return try await routedPoll(payload, context: nil)
        case .storeConsumerOffset, .deleteConsumerOffset:
            let prefix = try RawRequest.consumerTargetPrefix(payload)
            let command: CommandCode = code == CommandCode.storeConsumerOffset.rawValue ? .storeConsumerOffset : .deleteConsumerOffset
            let key = PollRouter.RouteKey(code: .getConsumerOffsetRouting, prefix: prefix[...])
            return try await core.pollRouter.send(command, payload: payload, key: key, routePayload: prefix, context: nil, coordinator: core)
        case .getPollRouting, .pollMessagesOnPrimary, .getConsumerOffsetRouting:
            // The routing protocol itself, sent once as the Rust client sends
            // it: a replay after a reconnect could run under another session.
            // A dropped client reconnects first, as for a typed call.
            try await requireAuthenticated()
            return try await core.exchangeOnce(code: code, payload: payload, context: .empty)
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

    /// The consumer, stream, topic, and partition that lead a poll and an
    /// offset request, which is the payload of the offset route lookup.
    static func consumerTargetPrefix(_ payload: [UInt8]) throws -> [UInt8] {
        let length = try decode(payload) { reader in
            try skipConsumerTarget(&reader)
            return reader.position
        }
        return Array(payload[..<length])
    }

    static func validatePoll(_ payload: [UInt8]) throws {
        try decode(payload) { reader in
            try skipConsumerTarget(&reader)
            try reader.skip(pollParametersSize)
        }
    }

    private static func skipConsumerTarget(_ reader: inout ByteReader) throws {
        _ = try reader.readUInt8()
        _ = try Identifier.decode(from: &reader)
        _ = try Identifier.decode(from: &reader)
        _ = try Identifier.decode(from: &reader)
        try reader.skip(1 + 4)
    }

    private static func decode<T>(_ payload: [UInt8], _ body: (inout ByteReader) throws -> T) throws -> T {
        do {
            return try Responses.decode(payload[...], body)
        } catch let error as IggyError {
            throw IggyError(.invalidCommand, context: "the raw payload does not decode: \(error)")
        }
    }
}
