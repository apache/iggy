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

    /// Polls one partition, as `poll_messages` in the Rust SDK. The request
    /// is deferred: the server answers once `minCount` messages are
    /// readable, the next one would pass `maxBytes`, or `maxWait` ran out,
    /// and then returns what there is, possibly nothing. A first message
    /// larger than `maxBytes` comes alone in its own reply. The call takes
    /// at most the request timeout of `options`, `maxWait` plus ten seconds.
    ///
    /// With ``CommitPolicy/onRead`` the server stores the offset of the last
    /// message it returns, as part of the read. A cancelled caller of such a
    /// poll that was sent waits for its one reply: `CancellationError` when
    /// the server stored nothing, or the messages it stored as consumed.
    /// ``CommitPolicy/auto(_:)`` is refused with
    /// ``IggyErrorCode/invalidConfiguration``: a single poll has nothing to
    /// count. Use a ``PartitionReader`` or an ``IggyConsumer`` for it.
    ///
    /// Continue with ``PolledMessages/continuation``, which this call refuses
    /// for another partition with ``IggyErrorCode/invalidCommand``, and
    /// commit with ``storeConsumerPosition(consumer:streamID:topicID:checkpoint:)``.
    /// Invalid options fail before anything is sent.
    public func pollMessages(
        streamID: Identifier, topicID: Identifier, partitionID: UInt32, consumer: Consumer = .default, strategy: PollingStrategy,
        options: PollOptions = PollOptions()
    ) async throws -> PolledMessages {
        if case .auto = options.commit {
            throw IggyError(.invalidConfiguration, context: "a single poll has nothing to count, a PartitionReader commits automatically")
        }
        try strategy.validate(partitionID: partitionID)
        try options.validate()
        var polled = try await pollMessages(
            streamID: streamID, topicID: topicID, partitionID: partitionID, consumer: consumer, strategy: strategy, count: options.count,
            autoCommit: options.commitsOnRead, options: options.deferred)
        polled.start = strategy
        return polled
    }

    /// One poll with the wire limits of the caller: command 100 without
    /// `options`, else command 106. A consumer-group poll with `partitionID`
    /// nil takes the next of the member's assigned partitions, round-robin,
    /// syncing the assignment from the coordinator when needed. A member
    /// holding no partitions gets an empty result whose partition id is
    /// ``PolledMessages/noAssignedPartition``, and a client that is not a
    /// member gets ``IggyErrorCode/consumerGroupMemberNotFound``. A deferred
    /// consumer-group poll names its partition.
    func pollMessages(
        streamID: Identifier, topicID: Identifier, partitionID: UInt32?, consumer: Consumer = .default, strategy: PollingStrategy, count: UInt32,
        autoCommit: Bool, options: DeferredPollOptions? = nil
    ) async throws -> PolledMessages {
        try await pollMessages(
            streamID: streamID, topicID: topicID, partitionID: partitionID, consumer: consumer, count: count, autoCommit: autoCommit, options: options
        ) { _ in strategy }
    }

    /// ``pollMessages(streamID:topicID:partitionID:consumer:strategy:count:autoCommit:options:)``
    /// with the strategy chosen once the partition is known, so a caller can
    /// continue each partition from its own position.
    func pollMessages(
        streamID: Identifier, topicID: Identifier, partitionID: UInt32?, consumer: Consumer = .default, count: UInt32, autoCommit: Bool,
        options: DeferredPollOptions? = nil, strategyFor: @Sendable (UInt32) -> PollingStrategy
    ) async throws -> PolledMessages {
        let started = ContinuousClock.now
        try options?.validate(count: count)
        if consumer.kind == .consumerGroup, partitionID == nil {
            guard options == nil else {
                throw IggyError(
                    .invalidCommand, context: "a deferred consumer-group poll names its partition; use fetchMessages for a readiness group instead")
            }
            let lineage = try await requireAuthenticated()
            return try await pollGroup(
                streamID: streamID, topicID: topicID, consumer: consumer, count: count, autoCommit: autoCommit, lineage: lineage, strategyFor: strategyFor)
        }
        let lineage = try await requireAuthenticated(within: options.map { started + $0.requestTimeout })
        return try await poll(
            consumer: consumer, streamID: streamID, topicID: topicID, partitionID: partitionID, strategy: strategyFor(partitionID ?? 0), count: count,
            autoCommit: autoCommit, options: options, started: started, lineage: lineage)
    }

    /// Fetches up to `count` messages from the partitions of one readiness
    /// group, in one request to the primary of the group, as
    /// `fetch_messages` in the Rust SDK. Only a ``PartitionReader`` builds
    /// its targets: every target must share the primary of the first one.
    ///
    /// Each start point is an offset or `next`, and each partition appears
    /// once, at most 1000 of them. A target without a context is fenced with
    /// the incarnation of its route. The reply holds a section per target
    /// that was read or failed. A `notPrimary` section or refusal marks the
    /// routes of its partitions moved, a `historyUnavailable` or
    /// `consumerGroupPartitionNotOwned` one drops them. A fetch with
    /// `autoCommit` or a `next` target orders progress polls and offset
    /// stores of its targets. A cancelled caller of a fetch with
    /// `autoCommit` that was sent waits for its one reply. A lost reply of
    /// such a fetch fails with ``IggyErrorCode/transientNotCommitted``, and
    /// it is never sent again.
    func fetchMessages(
        streamID: Identifier, topicID: Identifier, targets: [(partitionID: UInt32, strategy: PollingStrategy)], consumer: Consumer = .default,
        count: UInt32, autoCommit: Bool, options: DeferredPollOptions = DeferredPollOptions()
    ) async throws -> [FetchSection] {
        let started = ContinuousClock.now
        try Self.validateFetch(targets, count: count, options: options)
        let lineage = try await requireAuthenticated(within: started + options.requestTimeout)
        var options = options
        options.maxBytes = Self.fetchMaxBytes(options.maxBytes, targets: targets.count)
        // Each target may fill the whole reply: the server sizes the reads by
        // the bytes each target holds, and a smaller cap would slice batches.
        let wire = targets.map { target in
            Requests.FetchTarget(
                partitionID: target.partitionID, strategy: target.strategy, maxBytes: options.maxBytes, fence: target.strategy.context ?? .empty)
        }
        return try await fetch(
            streamID: streamID, topicID: topicID, consumer: consumer, targets: wire, count: count, autoCommit: autoCommit, options: options,
            started: started, lineage: lineage
        ).sections
    }

    /// `partitions` grouped by the replica that is their primary, as
    /// `readiness_groups` in the Rust SDK: one group per primary, in replica
    /// order, split into groups of at most 1000 partitions, each in the
    /// order of `partitions`.
    func readinessGroups(streamID: Identifier, topicID: Identifier, partitions: [UInt32]) async throws -> [[UInt32]] {
        let lineage = try await requireAuthenticated()
        return try await core.pollRouter.primaryGroups(
            stream: streamID, topic: topicID, partitions: partitions, lineage: lineage, coordinator: core,
            deadline: ContinuousClock.now + PollRouter.pollTimeout)
    }

    /// A wait and a fetch name 1 to 1000 partitions, each once.
    fileprivate static func validateTargets(_ partitions: [(partitionID: UInt32, strategy: PollingStrategy)], of request: String) throws {
        guard (1...Requests.maxPartitionsPerRequest).contains(partitions.count), Set(partitions.map(\.partitionID)).count == partitions.count else {
            throw IggyError(.invalidCommand, context: "\(request) names 1 to \(Requests.maxPartitionsPerRequest) partitions, each once")
        }
    }

    fileprivate static func validateFetch(_ targets: [(partitionID: UInt32, strategy: PollingStrategy)], count: UInt32, options: DeferredPollOptions) throws {
        try validateTargets(targets, of: "a fetch")
        guard targets.allSatisfy({ fetchTakes($0.strategy) }) else {
            throw IggyError(.invalidCommand, context: "a fetch starts each partition at an offset or after its stored offset")
        }
        try options.validateFetch(count: count)
    }

    /// A fetch takes only these start points. The others would loop or
    /// misread under a count that the reply splits between its targets.
    static func fetchTakes(_ strategy: PollingStrategy) -> Bool {
        strategy.kind == .next || strategy.kind == .offset
    }

    /// A reply larger than a frame fails the connection that reads it, so the
    /// records of a fetch leave room for its section table.
    fileprivate static func fetchMaxBytes(_ maxBytes: UInt32, targets: Int) -> UInt32 {
        min(maxBytes, DeferredPollOptions.maxReplyBytes - UInt32(Responses.fetchHeadSize(targets)))
    }

    /// One fetch on the route of its first target, the primary of its group.
    /// Each target carries its own fence, so the header carries no partition
    /// context. A fetch with `autoCommit` runs as a task of its own, which
    /// the cancellation of its caller does not reach.
    private func fetch(
        streamID: Identifier, topicID: Identifier, consumer: Consumer, targets: [Requests.FetchTarget], count: UInt32, autoCommit: Bool,
        options: DeferredPollOptions, started: ContinuousClock.Instant, lineage: UInt64
    ) async throws -> (body: [UInt8], sections: [FetchSection]) {
        let core = self.core
        let router = core.pollRouter
        let keys = targets.map { PollRouter.RouteKey(stream: streamID, topic: topicID, partitionID: $0.partitionID) }
        guard let routing = keys.first else {
            throw IggyError(.invalidCommand, context: "a fetch names at least one partition")
        }
        let fences = try await router.fences(
            zip(keys, targets).map { ($0, $1.fence) }, lineage: lineage, coordinator: core, deadline: started + options.requestTimeout)
        let targets = zip(targets, fences).map { target, fence in
            Requests.FetchTarget(partitionID: target.partitionID, strategy: target.strategy, maxBytes: target.maxBytes, fence: fence)
        }
        let progress = autoCommit || targets.contains { $0.strategy.kind == .next }
        let progressKeys = progress ? targets.map { PollRouter.ProgressKey(partitionID: $0.partitionID, incarnation: $0.fence.incarnation) } : []
        let result: (body: [UInt8], sections: [FetchSection])
        do {
            result = try await router.withProgress(progressKeys, deadline: started + options.requestTimeout) {
                let deferred = PollRouter.Deferred(options: options, started: started, commits: autoCommit) { remaining in
                    try Requests.fetchMessages(
                        consumer: consumer, streamID: streamID, topicID: topicID, targets: targets, count: count, autoCommit: autoCommit, options: remaining)
                }
                let body = try await router.send(
                    .fetchMessages, body: .deferred(deferred), key: routing, context: .empty, lineage: lineage, coordinator: core,
                    deadline: deferred.deadline)
                let sections = try Responses.fetchSections(body[...])
                let requested = Set(targets.map(\.partitionID))
                let returned = Set(sections.map(\.partitionID))
                guard returned.count == sections.count, returned.isSubset(of: requested) else {
                    throw IggyError(.invalidCommand, context: "a fetch reply repeats or adds a partition")
                }
                return (body, sections)
            }
        } catch let error as IggyError where error.code == .notPrimary {
            router.mayHaveMoved(keys)
            throw error
        } catch let error as IggyError where Self.staleRoute(error.code) {
            router.dropRoutes(keys)
            throw error
        }
        let sections = result.sections
        let failed = { (matches: (IggyErrorCode) -> Bool) in
            sections.filter { $0.error.map { matches($0.code) } ?? false }.map {
                PollRouter.RouteKey(stream: streamID, topic: topicID, partitionID: $0.partitionID)
            }
        }
        router.mayHaveMoved(failed { $0 == .notPrimary })
        router.dropRoutes(failed(Self.staleRoute))
        return result
    }

    /// The server refuses a fence this way once the partition was recreated
    /// or its owner changed, so the route is looked up again.
    private static func staleRoute(_ code: IggyErrorCode) -> Bool {
        code == .historyUnavailable || code == .consumerGroupPartitionNotOwned
    }

    /// One deferred poll of one partition, its reply body undecoded, so a
    /// reader can name the bytes of a message that does not decode.
    func pollBody(
        streamID: Identifier, topicID: Identifier, partitionID: UInt32, consumer: Consumer, strategy: PollingStrategy, count: UInt32,
        autoCommit: Bool, options: DeferredPollOptions
    ) async throws -> [UInt8] {
        let started = ContinuousClock.now
        try options.validate(count: count)
        let lineage = try await requireAuthenticated(within: started + options.requestTimeout)
        let payload = try Requests.pollMessages(
            consumer: consumer, streamID: streamID, topicID: topicID, partitionID: partitionID, strategy: strategy, count: count, autoCommit: autoCommit)
        let deferred = PollRouter.Deferred(options: options, started: started, commits: autoCommit) { remaining in
            try Requests.pollMessages(
                consumer: consumer, streamID: streamID, topicID: topicID, partitionID: partitionID, strategy: strategy, count: count,
                autoCommit: autoCommit, options: remaining)
        }
        return try await routedPartitionRequest(
            .pollMessagesDeferred, payload: payload, deferred: deferred, context: strategy.context, lineage: lineage)
    }

    /// One poll of one partition. With `options` it is deferred: command 106,
    /// the body of a poll followed by the options left for each attempt.
    private func poll(
        consumer: Consumer, streamID: Identifier, topicID: Identifier, partitionID: UInt32?, strategy: PollingStrategy, count: UInt32,
        autoCommit: Bool, options: DeferredPollOptions?, started: ContinuousClock.Instant, lineage: UInt64
    ) async throws -> PolledMessages {
        let payload = try Requests.pollMessages(
            consumer: consumer, streamID: streamID, topicID: topicID, partitionID: partitionID, strategy: strategy, count: count, autoCommit: autoCommit)
        guard let options else {
            return try Responses.polledMessages(
                try await routedPartitionRequest(.pollMessages, payload: payload, context: strategy.context, lineage: lineage)[...])
        }
        let deferred = PollRouter.Deferred(options: options, started: started, commits: autoCommit) { remaining in
            try Requests.pollMessages(
                consumer: consumer, streamID: streamID, topicID: topicID, partitionID: partitionID, strategy: strategy, count: count,
                autoCommit: autoCommit, options: remaining)
        }
        let body = try await routedPartitionRequest(
            .pollMessagesDeferred, payload: payload, deferred: deferred, context: strategy.context, lineage: lineage)
        return try Responses.polledMessages(body[...])
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
    /// to partition 0. A group poll, a store, and a delete must name one. A
    /// `deferred` poll sends its own body, `payload` only routes it, and its
    /// request timeout is its deadline.
    func routedPartitionRequest(
        _ code: CommandCode, payload: [UInt8], deferred: PollRouter.Deferred? = nil, context: PartitionContext?, lineage: UInt64
    ) async throws -> [UInt8] {
        let target = try RawRequest.consumerTarget(payload)
        let poll = code == .pollMessages || code == .pollMessagesDeferred
        let partitionID: UInt32
        if let named = target.partitionID {
            partitionID = named
        } else if code == .getConsumerOffset || (poll && target.consumerKind == Consumer.Kind.consumer.rawValue) {
            partitionID = 0
        } else {
            throw IggyError(.invalidIdentifier, context: "command \(code.rawValue) needs a partition id")
        }
        let key = PollRouter.RouteKey(stream: target.stream, topic: target.topic, partitionID: partitionID)
        let deadline = deferred?.deadline ?? ContinuousClock.now + PollRouter.pollTimeout
        let router = core.pollRouter
        let progress = try code == .storeConsumerOffset || code == .deleteConsumerOffset || (poll && RawRequest.pollChangesProgress(payload))
        let captured: PartitionContext?
        let keys: [PollRouter.ProgressKey]
        if progress {
            let resolved = try await router.progressContext(key, context: context, lineage: lineage, coordinator: core, deadline: deadline)
            captured = resolved
            keys = [PollRouter.ProgressKey(partitionID: partitionID, incarnation: resolved.incarnation)]
        } else {
            captured = context
            keys = []
        }
        return try await router.withProgress(keys, deadline: deadline) {
            try await router.send(
                code, body: deferred.map { .deferred($0) } ?? .fixed(payload), key: key, context: captured, lineage: lineage, coordinator: core,
                deadline: deadline)
        }
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
        let payload = try Requests.consumerGroup(streamID: streamID, topicID: topicID, groupID: groupID)
        // A cancelled exchange takes the coordinator connection with it, so a
        // caller cancelled meanwhile lets the sync finish.
        let body = try await Task {
            try await self.core.exchange(code: CommandCode.syncConsumerGroup.rawValue, payload: payload, lineage: lineage)
        }.value
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
        logger.debug(
            "Synced the consumer group assignment",
            metadata: ["group": "\(groupID)", "generation": "\(assignment.generation)", "partitions": "\(assignment.partitions)"])
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

    /// Stores the offset of a checkpoint under the partition context of the
    /// read that delivered it, as `store_consumer_position` in the Rust SDK.
    /// It is refused with ``IggyErrorCode/historyUnavailable`` once the
    /// partition was recreated and, for a consumer group, with
    /// ``IggyErrorCode/consumerGroupPartitionNotOwned`` once it was
    /// reassigned.
    public func storeConsumerPosition(consumer: Consumer = .default, streamID: Identifier, topicID: Identifier, checkpoint: Checkpoint) async throws {
        let lineage = try await requireAuthenticated()
        _ = try await routedPartitionRequest(
            .storeConsumerOffset,
            payload: Requests.storeConsumerOffset(
                consumer: consumer, streamID: streamID, topicID: topicID, partitionID: checkpoint.partitionID, offset: checkpoint.offset, ack: .quorum),
            context: checkpoint.context, lineage: lineage)
    }

    /// Deletes the stored offset of the partition of a checkpoint under the
    /// partition context of the read that delivered it, refused as
    /// ``storeConsumerPosition(consumer:streamID:topicID:checkpoint:)`` is.
    /// The offset of the checkpoint is not used.
    public func deleteConsumerPosition(consumer: Consumer = .default, streamID: Identifier, topicID: Identifier, checkpoint: Checkpoint) async throws {
        try await deleteConsumerOffset(
            consumer: consumer, streamID: streamID, topicID: topicID, partitionID: checkpoint.partitionID, context: checkpoint.context)
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

    func consumerOffsetNext(
        consumer: Consumer, streamID: Identifier, topicID: Identifier, partitionID: UInt32, context: PartitionContext
    ) async throws -> UInt64 {
        let lineage = try await requireAuthenticated()
        let body = try await routedPartitionRequest(
            .getConsumerOffset, payload: Requests.getConsumerOffset(consumer: consumer, streamID: streamID, topicID: topicID, partitionID: partitionID),
            context: context, lineage: lineage)
        let offset = body.isEmpty ? nil : try Responses.consumerOffset(body[...])
        return offset.map { $0.storedOffset == .max ? .max : $0.storedOffset + 1 } ?? 0
    }

    /// Deletes the stored offset of a consumer on a partition. A nil
    /// `partitionID` fails with ``IggyErrorCode/invalidIdentifier`` before
    /// anything is sent.
    public func deleteConsumerOffset(consumer: Consumer = .default, streamID: Identifier, topicID: Identifier, partitionID: UInt32?) async throws {
        try await deleteConsumerOffset(consumer: consumer, streamID: streamID, topicID: topicID, partitionID: partitionID, context: nil)
    }

    /// Deletes the offset under the context of a poll, so the server refuses
    /// it once the partition was recreated or, for a consumer group,
    /// reassigned since. A nil context takes the one of the route.
    func deleteConsumerOffset(consumer: Consumer, streamID: Identifier, topicID: Identifier, partitionID: UInt32?, context: PartitionContext?) async throws {
        let lineage = try await requireAuthenticated()
        _ = try await routedPartitionRequest(
            .deleteConsumerOffset,
            payload: Requests.deleteConsumerOffset(consumer: consumer, streamID: streamID, topicID: topicID, partitionID: partitionID, ack: .quorum),
            context: context, lineage: lineage)
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
        case .pollMessagesDeferred:
            let started = ContinuousClock.now
            let request = try RawRequest.deferredPoll(payload)
            let deferred = PollRouter.Deferred(options: request.options, started: started, commits: request.autoCommit) { remaining in
                var writer = ByteWriter(capacity: payload.count)
                writer.write(request.poll)
                try remaining.encode(into: &writer)
                return writer.bytes
            }
            return try await routedPartitionRequest(.pollMessagesDeferred, payload: request.poll, deferred: deferred, context: nil, lineage: lineage)
        case .fetchMessages:
            // The fences and limits of the targets are the caller's, as the
            // payload carries them.
            let started = ContinuousClock.now
            let request = try RawRequest.fetch(payload)
            var options = request.options
            options.maxBytes = Self.fetchMaxBytes(options.maxBytes, targets: request.targets.count)
            let targets = request.targets.map { target in
                Requests.FetchTarget(
                    partitionID: target.partitionID, strategy: target.strategy, maxBytes: min(target.maxBytes, options.maxBytes), fence: target.fence)
            }
            return try await fetch(
                streamID: request.stream, topicID: request.topic, consumer: request.consumer, targets: targets, count: request.count,
                autoCommit: request.autoCommit, options: options, started: started, lineage: lineage
            ).body
        case .getConsumerOffset, .storeConsumerOffset, .deleteConsumerOffset:
            return try await routedPartitionRequest(command, payload: payload, context: nil, lineage: lineage)
        default:
            return nil
        }
    }
}

/// Reads the partition a raw request names. A payload that does not decode
/// fails with ``IggyErrorCode/invalidCommand`` before anything is sent.
enum RawRequest {
    /// `[kind:1][value:8]`.
    static let strategySize = 9

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

    struct DeferredPoll {
        /// The poll without its options.
        let poll: [UInt8]
        let options: DeferredPollOptions
        let autoCommit: Bool
    }

    struct Fetch {
        let consumer: Consumer
        let stream: Identifier
        let topic: Identifier
        let options: DeferredPollOptions
        let count: UInt32
        let autoCommit: Bool
        let targets: [Requests.FetchTarget]
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
        try decode(payload) { reader in _ = try readPollCount(&reader) }
    }

    static func pollChangesProgress(_ payload: [UInt8]) throws -> Bool {
        try decode(payload) { reader in
            _ = try readConsumerTarget(&reader)
            let strategy = try readStrategy(&reader)
            _ = try reader.readUInt32()
            let autoCommit = try reader.readBool()
            return autoCommit || strategy.kind == .next
        }
    }

    /// A poll followed by `[wait_us:8][min_count:4][max_bytes:4][request_timeout_us:8]`.
    /// Options the wire refuses fail as a payload that does not decode, as in
    /// the Rust SDK.
    static func deferredPoll(_ payload: [UInt8]) throws -> DeferredPoll {
        try decode(payload) { reader in
            let (count, autoCommit) = try readPollCount(&reader)
            let poll = Array(payload[..<reader.position])
            let options = DeferredPollOptions(
                maxWait: .microseconds(try reader.readUInt64()), minCount: try reader.readUInt32(), maxBytes: try reader.readUInt32(),
                requestTimeout: .microseconds(try reader.readUInt64()))
            try requireEnd(reader)
            try options.validate(count: count)
            return DeferredPoll(poll: poll, options: options, autoCommit: autoCommit)
        }
    }

    /// `[consumer][stream][topic][wait_us:8][request_timeout_us:8][min_count:4][count:4][max_bytes:4][auto_commit:1][targets_count:4]`,
    /// then `targets_count` x `[partition_id:4][strategy:9][max_bytes:4][incarnation:8][owner_generation:8]`.
    static func fetch(_ payload: [UInt8]) throws -> Fetch {
        try decode(payload) { reader in
            let consumer = try readConsumer(&reader)
            let stream = try Identifier.decode(from: &reader)
            let topic = try Identifier.decode(from: &reader)
            let wait = Duration.microseconds(try reader.readUInt64())
            let timeout = Duration.microseconds(try reader.readUInt64())
            let minCount = try reader.readUInt32()
            let count = try reader.readUInt32()
            let options = DeferredPollOptions(maxWait: wait, minCount: minCount, maxBytes: try reader.readUInt32(), requestTimeout: timeout)
            let autoCommit = try reader.readBool()
            let targetsCount = try reader.readUInt32()
            // Compared as `UInt32` so a hostile count never traps a 32-bit `Int`.
            guard targetsCount <= UInt32(Requests.maxPartitionsPerRequest) else {
                throw WireError.validation("a fetch cannot name \(targetsCount) partitions")
            }
            let targets = try (0..<targetsCount).map { _ in
                Requests.FetchTarget(
                    partitionID: try reader.readUInt32(), strategy: try readStrategy(&reader), maxBytes: try reader.readUInt32(),
                    fence: PartitionContext(incarnation: try reader.readUInt64(), ownerGeneration: try reader.readUInt64(), metadataOp: 0))
            }
            try requireEnd(reader)
            try IggyClient.validateFetch(targets.map { ($0.partitionID, $0.strategy) }, count: count, options: options)
            guard targets.allSatisfy({ $0.maxBytes >= DeferredPollOptions.minMaxBytes && $0.maxBytes <= options.maxBytes }) else {
                throw WireError.validation("a fetch target fills \(DeferredPollOptions.minMaxBytes) bytes up to the limit of the fetch")
            }
            return Fetch(consumer: consumer, stream: stream, topic: topic, options: options, count: count, autoCommit: autoCommit, targets: targets)
        }
    }

    /// Reads a poll to its end and returns the count it asks for and its
    /// auto-commit flag.
    private static func readPollCount(_ reader: inout ByteReader) throws -> (count: UInt32, autoCommit: Bool) {
        _ = try readConsumerTarget(&reader)
        try reader.skip(strategySize)
        return (try reader.readUInt32(), try reader.readBool())
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

    private static func readConsumer(_ reader: inout ByteReader) throws -> Consumer {
        let kind = try reader.readUInt8()
        guard let consumerKind = Consumer.Kind(rawValue: kind) else {
            throw WireError.validation("unknown consumer kind \(kind)")
        }
        return Consumer(kind: consumerKind, id: try Identifier.decode(from: &reader))
    }

    private static func readStrategy(_ reader: inout ByteReader) throws -> PollingStrategy {
        let kind = try reader.readUInt8()
        guard let strategyKind = PollingStrategy.Kind(rawValue: kind) else {
            throw WireError.validation("unknown polling strategy kind \(kind)")
        }
        return PollingStrategy(kind: strategyKind, value: try reader.readUInt64())
    }

    /// A request the client sends again with fresh options must hold nothing
    /// that the next attempt would drop.
    private static func requireEnd(_ reader: ByteReader) throws {
        guard reader.isAtEnd else {
            throw WireError.validation("\(reader.remaining) bytes follow the request")
        }
    }

    private static func decode<T>(_ payload: [UInt8], _ body: (inout ByteReader) throws -> T) throws -> T {
        do {
            return try Responses.decode(payload[...], body)
        } catch let error as IggyError {
            throw IggyError(.invalidCommand, context: "the raw payload does not decode: \(error)")
        }
    }
}
