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

import Logging
import NIOConcurrencyHelpers

/// Reads the partitions of one topic and hands their messages over one at a
/// time, a port of `IggyConsumer` in the Rust SDK.
///
/// Build a standalone consumer of one partition with
/// ``IggyClient/consumer(name:streamID:topicID:partitionID:configuration:)``,
/// or a member of a consumer group with
/// ``IggyClient/consumerGroup(name:streamID:topicID:configuration:)``, whose
/// partitions the server assigns. Call ``initialize()`` once, then read with
/// ``next()``, `for try await`, or ``consumeMessages(_:)``:
///
/// ```swift
/// let consumer = try client.consumerGroup(name: "billing", streamID: "orders", topicID: "created")
/// try await consumer.initialize()
/// for try await received in consumer {
///     print(received.partitionID, received.message.offset)
/// }
/// ```
///
/// Messages come from a buffer that a poll refills once it runs empty. A
/// failed poll is thrown after ``ConsumerConfiguration/pollingRetryInterval``
/// and leaves the consumer usable, so a caller of ``next()`` can call it again;
/// a `for try await` loop ends at the first error instead. ``next()`` returns
/// nil only after ``shutdown()``. One reader consumes a consumer, as in the
/// Rust SDK: two loops over the same consumer split its messages between them.
///
/// The consumer remembers the last message handed over from each partition.
/// Without ``ConsumerConfiguration/allowReplay`` a message at or below it is
/// not handed over again, and ``ConsumerConfiguration/autoCommit`` decides
/// when the reading position is stored on the server.
///
/// Unlike the Rust consumer, this one has no encryptor, since the Swift
/// client has none, and does not resolve a group create or join whose outcome
/// was lost: the Swift client has no operation lanes to resolve them through.
public actor IggyConsumer: AsyncSequence {
    public typealias Element = ReceivedMessage

    public nonisolated let name: String
    public nonisolated let streamID: Identifier
    public nonisolated let topicID: Identifier
    public nonisolated let consumer: Consumer
    /// The partition a standalone consumer reads; nil for a group member.
    public nonisolated let partitionID: UInt32?
    /// The partition of the last batch handed over. Before the first one it
    /// is the partition of a standalone consumer and zero for a group member.
    public private(set) var currentPartitionID: UInt32

    private let client: IggyClient
    private let configuration: ConsumerConfiguration
    private let logger: Logger
    private let isConsumerGroup: Bool
    /// One poll at a time: a reader that finds the buffer empty waits here
    /// while another one polls.
    private let reading = AsyncMutex()
    /// Held by a join and by the final leave, so a join in flight ends
    /// before the leave and none starts after it.
    private let membership = AsyncMutex()
    /// Held by every store and delete, so a coverage check, its request, and
    /// the cache update see no other store in between.
    private let offsetWrites = AsyncMutex()
    private let commitWakeups: AsyncStream<Void>
    private let commitWakeup: AsyncStream<Void>.Continuation

    private var isInitialized = false
    /// Shared by concurrent calls of ``initialize()``.
    private var initialization: Task<Void, any Error>?
    private var isShutDown = false
    private var shutdownTask: Task<Void, Never>?
    /// Follows the client's events: polling waits while the client is signed
    /// out and can come back on its own.
    private var canPoll = true
    private var joinedGroup = false
    private var buffer: [IggyMessage] = []
    private var bufferIndex = 0
    private var bufferedContext = PartitionContext.empty
    private var bufferedCurrentOffset: UInt64 = 0
    private var lastPolledAt: ContinuousClock.Instant?
    /// Where each partition continues after its last message handed over,
    /// unless a `next` poll commits, which leaves that to the offset it stores.
    private var nextPositions: [UInt32: ConsumerPosition] = [:]
    private var lastConsumed: [UInt32: ConsumerPosition] = [:]
    private var lastStored: [UInt32: ConsumerPosition] = [:]
    /// Commits queued by the message triggers, the latest per partition.
    private var pendingCommits: [UInt32: ConsumerPosition] = [:]
    private var eventsTask: Task<Void, Never>?
    private var intervalTask: Task<Void, Never>?
    private var intervalIsStoring = false
    private var commitTask: Task<Void, Never>?

    init(
        client: IggyClient, name: String, consumer: Consumer, streamID: Identifier, topicID: Identifier, partitionID: UInt32?,
        configuration: ConsumerConfiguration
    ) throws {
        try configuration.validate()
        self.client = client
        self.name = name
        self.consumer = consumer
        self.streamID = streamID
        self.topicID = topicID
        self.partitionID = partitionID
        currentPartitionID = partitionID ?? 0
        self.configuration = configuration
        logger = client.logger
        isConsumerGroup = consumer.kind == .consumerGroup
        (commitWakeups, commitWakeup) = AsyncStream<Void>.makeStream(bufferingPolicy: .bufferingNewest(1))
    }

    deinit {
        eventsTask?.cancel()
        intervalTask?.cancel()
        commitTask?.cancel()
        commitWakeup.finish()
    }

    public nonisolated func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(consumer: self)
    }

    public struct AsyncIterator: AsyncIteratorProtocol {
        /// Nil once an error or nil ended the iteration, as the protocol
        /// requires. The consumer itself can be read on after an error.
        var consumer: IggyConsumer?

        public mutating func next() async throws -> ReceivedMessage? {
            guard let consumer else {
                return nil
            }
            do {
                let received = try await consumer.next()
                if received == nil {
                    self.consumer = nil
                }
                return received
            } catch {
                self.consumer = nil
                throw error
            }
        }
    }

    // MARK: Lifecycle

    /// Looks the stream and topic up, retrying a missing one as
    /// ``ConsumerConfiguration/initRetries`` says, joins the group of a member
    /// that joins on its own, and starts the commit tasks. Concurrent calls
    /// share one run, and calling it again after it succeeded, or after
    /// ``shutdown()``, does nothing.
    public func initialize() async throws {
        if isInitialized || isShutDown {
            return
        }
        if let initialization {
            return try await initialization.value
        }
        let task = Task { try await self.start() }
        initialization = task
        defer { initialization = nil }
        try await task.value
    }

    private func start() async throws {
        logger.info("Initializing the consumer", metadata: metadata)
        try await waitForStreamAndTopic()
        let events = await client.events
        // A retried initialize after a failed join replaces the subscription
        // with nothing suspending in between, so none is left behind.
        eventsTask?.cancel()
        eventsTask = Task { [weak self] in
            for await event in events {
                guard let self, await self.observe(event) else {
                    return
                }
            }
        }
        if isConsumerGroup, configuration.autoJoinConsumerGroup {
            try await joinGroup()
        }
        guard !isShutDown else {
            // A shutdown came in meanwhile and already stopped what it found.
            eventsTask?.cancel()
            eventsTask = nil
            return
        }
        if let interval = configuration.autoCommit.interval {
            intervalTask = Task { [weak self] in
                while (try? await Task.sleep(for: interval)) != nil {
                    guard let self, await self.storeOnTick() else {
                        return
                    }
                }
            }
        }
        commitTask = Task { [weak self, commitWakeups] in
            for await _ in commitWakeups {
                guard let self, await self.sendPendingCommits() else {
                    return
                }
            }
        }
        isInitialized = true
        logger.info("The consumer has been initialized", metadata: metadata)
    }

    /// Stops reading, sends the queued commits, stores the reading position
    /// of every partition read unless auto-commit is disabled, and leaves the
    /// group. Every caller waits for the same shutdown. Failures are logged:
    /// nothing is left for the caller to undo.
    public func shutdown() async {
        if let shutdownTask {
            await shutdownTask.value
            return
        }
        isShutDown = true
        let task = Task { await self.drain() }
        shutdownTask = task
        await task.value
    }

    private func drain() async {
        logger.info("Shutting down the consumer", metadata: metadata)
        let timeout = configuration.offsetDrainTimeout
        if let intervalTask {
            // A sleeping tick is cancelled. One that is storing ends after its
            // store.
            if intervalIsStoring {
                await Self.wait(for: intervalTask, upTo: timeout)
            } else {
                intervalTask.cancel()
            }
        }
        if let commitTask {
            commitWakeup.yield()
            await Self.wait(for: commitTask, upTo: timeout)
        }
        if configuration.autoCommit != .disabled {
            let positions = Array(lastConsumed.values)
            // One deadline for every final store, as in the Rust SDK.
            _ = try? await withDeadline(.now + client.configuration.requestTimeout) {
                await self.storeFinalPositions(positions)
            }
        }
        // Every join checks the shutdown under this lock, so none comes after
        // the leave.
        if isConsumerGroup, (try? await membership.lock()) != nil {
            defer { membership.unlock() }
            if joinedGroup {
                joinedGroup = false
                do {
                    try await client.leaveConsumerGroup(streamID: streamID, topicID: topicID, groupID: consumer.id)
                } catch {
                    // Expected after an explicit leave or a deleted group.
                    logger.debug("Failed to leave the consumer group", metadata: metadata.merging(["error": "\(error)"]) { $1 })
                }
            }
        }
        eventsTask?.cancel()
        eventsTask = nil
        logger.info("The consumer has been shut down", metadata: metadata)
    }

    // MARK: Reading

    /// The next message: from the buffer, or from a poll once it is empty.
    /// Throws what a poll or join ran into, after the retry interval, and can
    /// be called again after it threw. Nil once the consumer is shut down.
    public func next() async throws -> ReceivedMessage? {
        if isShutDown {
            return nil
        }
        // Nothing suspends between the check and the handover, so a
        // buffered message needs no lock.
        if bufferIndex < buffer.count {
            return handOverBuffered()
        }
        try await reading.lock()
        defer { reading.unlock() }
        while !isShutDown {
            // The poll this reader waited for can have refilled the buffer.
            if bufferIndex < buffer.count {
                return handOverBuffered()
            }
            try Task.checkCancellation()
            let polled = try await poll()
            if !polled.messages.isEmpty, !isShutDown {
                return handOverPolled(polled)
            }
        }
        return nil
    }

    /// Hands every message to `handler` until the consumer shuts down, the
    /// task is cancelled, or an error ends the reading: a client that is shut
    /// down or cannot come back without the caller, or an invalid address.
    /// Other errors are logged and reading goes on, and a handler that throws
    /// is logged too. The ``AutoCommitAfter`` triggers fire here, after the
    /// handler returned or threw.
    public func consumeMessages(_ handler: @Sendable (ReceivedMessage) async throws -> Void) async throws {
        let after = configuration.autoCommit.afterTrigger
        while true {
            let received: ReceivedMessage
            do {
                guard let next = try await next() else {
                    return
                }
                received = next
            } catch let error as IggyError {
                let ends = Self.endsConsumption(error) ? true : !(await client.canRecover)
                if ends {
                    logger.error("Reading stopped", metadata: metadata.merging(["error": "\(error)"]) { $1 })
                    throw error
                }
                logger.error("Failed to read a message", metadata: metadata.merging(["error": "\(error)"]) { $1 })
                continue
            }
            do {
                try await handler(received)
            } catch {
                logger.error(
                    "The handler failed", metadata: metadata.merging(["offset": "\(received.message.offset)", "error": "\(error)"]) { $1 })
            }
            switch after {
            case .consumingEachMessage?:
                queueCommit(received.position)
            case .consumingEveryNthMessage(let n)?:
                if received.message.offset % UInt64(n) == 0 {
                    queueCommit(received.position)
                }
            case .consumingAllMessages?:
                if received.message.offset == received.currentOffset {
                    queueCommit(received.position)
                }
            case nil:
                break
            }
        }
    }

    // MARK: Offsets

    /// Stores `offset` for a partition, the current one when nil, under the
    /// context of the last message handed over from it. Before any was, it
    /// is stored without a context.
    public func storeOffset(_ offset: UInt64, partitionID: UInt32? = nil) async throws {
        let partition = partitionID ?? currentPartitionID
        if let last = lastConsumed[partition] {
            try await storePosition(ConsumerPosition(partitionID: partition, offset: offset, context: last.context))
            return
        }
        try await offsetWrites.lock()
        defer { offsetWrites.unlock() }
        try await client.storeConsumerOffset(consumer: consumer, streamID: streamID, topicID: topicID, partitionID: partition, offset: offset)
        lastStored[partition] = ConsumerPosition(partitionID: partition, offset: offset, context: .empty)
    }

    /// Stores the exact position delivered, as
    /// ``ReceivedMessage/position`` gives it, even after later polls. Without
    /// ``ConsumerConfiguration/allowReplay`` a position at or below the one
    /// already stored in the same partition history is not sent.
    public func storePosition(_ position: ConsumerPosition) async throws {
        try await store(position, allowReplay: configuration.allowReplay)
    }

    /// Deletes the stored offset of a partition, the current one when nil.
    /// Unlike the Rust consumer, a group member's nil is not left to the
    /// server, since the Swift client routes the request by its partition.
    public func deleteOffset(partitionID: UInt32? = nil) async throws {
        let partition = partitionID ?? currentPartitionID
        try await offsetWrites.lock()
        defer { offsetWrites.unlock() }
        // A cancelled delete can still have committed, so a later store must
        // not be skipped as covered.
        lastStored[partition] = nil
        try await client.deleteConsumerOffset(consumer: consumer, streamID: streamID, topicID: topicID, partitionID: partition)
    }

    /// The offset of the last message handed over from the partition.
    public func lastConsumedOffset(partitionID: UInt32) -> UInt64? {
        lastConsumed[partitionID]?.offset
    }

    /// The offset this consumer last stored for the partition.
    public func lastStoredOffset(partitionID: UInt32) -> UInt64? {
        lastStored[partitionID]?.offset
    }

    // MARK: Internals

    private var metadata: Logger.Metadata {
        ["consumer": "\(name)", "stream": "\(streamID)", "topic": "\(topicID)"]
    }

    private func waitForStreamAndTopic() async throws {
        let retries = configuration.initRetries ?? 0
        var ticker = RetryTicker(interval: configuration.initRetryInterval)
        try await ticker.tick()
        var streamExists = try await client.getStream(streamID) != nil
        var topicExists = false
        if streamExists {
            topicExists = try await client.getTopic(streamID: streamID, topicID: topicID) != nil
        }
        var attempt: UInt32 = 0
        // A stream or topic may still be being created by someone else.
        while !(streamExists && topicExists), attempt < retries {
            attempt += 1
            logger.warning(
                "The stream or topic does not exist yet, retrying",
                metadata: metadata.merging(["retry": "\(attempt)/\(retries)", "stream_exists": "\(streamExists)"]) { $1 })
            try await ticker.tick()
            if !streamExists {
                streamExists = try await client.getStream(streamID) != nil
            }
            if streamExists {
                topicExists = try await client.getTopic(streamID: streamID, topicID: topicID) != nil
            }
        }
        guard streamExists else {
            throw streamID.name.map { IggyError(.streamNameNotFound, context: "stream \($0) was not found") }
                ?? IggyError(.streamIdNotFound, context: "stream \(streamID) was not found")
        }
        guard topicExists else {
            throw topicID.name.map { IggyError(.topicNameNotFound, context: "topic \($0) was not found in stream \(streamID)") }
                ?? IggyError(.topicIdNotFound, context: "topic \(topicID) was not found in stream \(streamID)")
        }
    }

    private func joinGroup() async throws {
        try await membership.lock()
        defer { membership.unlock() }
        // The final leave of a shutdown may have gone already.
        if joinedGroup || isShutDown {
            return
        }
        if try await client.getConsumerGroup(streamID: streamID, topicID: topicID, groupID: consumer.id) == nil {
            guard configuration.createConsumerGroupIfNotExists else {
                throw IggyError(.consumerGroupNameNotFound, context: "consumer group \(name) does not exist and auto-creation is disabled")
            }
            logger.info("Creating the consumer group", metadata: metadata)
            do {
                _ = try await client.createConsumerGroup(streamID: streamID, topicID: topicID, name: name)
            } catch let error as IggyError where error.code == .consumerGroupNameAlreadyExists {
                // Another member created it first.
            }
        }
        logger.info("Joining the consumer group", metadata: metadata)
        try await client.joinConsumerGroup(streamID: streamID, topicID: topicID, groupID: consumer.id)
        joinedGroup = true
    }

    /// Returns false once the client shut down, which ends the subscription.
    private func observe(_ event: DiagnosticEvent) async -> Bool {
        switch event {
        case .shutdown:
            logger.warning("The client has been shut down", metadata: metadata)
            joinedGroup = false
            canPoll = false
            return false
        case .connected, .disconnected, .signedOut:
            joinedGroup = false
        case .signedIn:
            break
        }
        // Events can arrive after one that reports a later state, so the
        // state of the client decides.
        canPoll = await client.state == .authenticated
        return true
    }

    /// One poll, as the Rust consumer's poll future: the interval wait, the
    /// sign-in wait, a join when the membership was lost, and the request.
    /// An empty result is not an error.
    private func poll() async throws -> PolledMessages {
        if let pollInterval = configuration.pollInterval, let lastPolledAt {
            try await Task.sleep(until: lastPolledAt + pollInterval, clock: .continuous)
        }
        let retryInterval = configuration.pollingRetryInterval
        // A client that only the caller brings back never recovers, so the
        // poll goes on and fails with the reason instead of waiting for good.
        // Without a heartbeat or a commit to send, nothing else signs the
        // client in again, so the wait does.
        while !canPoll, !isShutDown, await client.canRecover {
            if (try? await client.requireAuthenticated()) != nil {
                break
            }
            try await Task.sleep(for: retryInterval)
        }
        if isConsumerGroup, configuration.autoJoinConsumerGroup, !joinedGroup {
            do {
                try await joinGroup()
            } catch {
                logger.error("Failed to join the consumer group", metadata: metadata.merging(["error": "\(error)"]) { $1 })
                try await Task.sleep(for: retryInterval)
                throw error
            }
        }
        // A shutdown may have sent its final commits and leave already.
        if isShutDown {
            return .empty
        }
        lastPolledAt = .now
        let continuations = nextPositions
        let strategy = configuration.pollingStrategy
        // A refusal does not name its partition, and it answers the last one
        // the strategy was asked for.
        let polledPartition = NIOLockedValueBox(PolledMessages.noAssignedPartition)
        do {
            var polled = try await client.pollMessages(
                streamID: streamID, topicID: topicID, partitionID: partitionID, consumer: consumer, count: configuration.batchLength,
                autoCommit: configuration.autoCommit.whenTrigger == .pollingMessages
            ) { partition in
                polledPartition.withLockedValue { $0 = partition }
                return continuations[partition].map { .offset($0.offset, context: $0.context) } ?? strategy
            }
            if polled.partitionID == PolledMessages.noAssignedPartition {
                nextPositions.removeAll()
                try await Task.sleep(for: retryInterval)
                return polled
            }
            if !configuration.allowReplay, let consumed = lastConsumed[polled.partitionID], Self.sameHistory(consumed.context, polled.context) {
                polled.messages.removeAll { $0.offset <= consumed.offset }
                polled.count = UInt32(polled.messages.count)
            }
            return polled
        } catch let error as IggyError {
            if error.code == .historyUnavailable || error.code == .consumerGroupPartitionNotOwned {
                nextPositions[polledPartition.withLockedValue { $0 }] = nil
            }
            logger.error("Failed to poll messages", metadata: metadata.merging(["error": "\(error)"]) { $1 })
            if isConsumerGroup, configuration.autoJoinConsumerGroup, error.code == .consumerGroupMemberNotFound {
                // The membership was revoked; the next poll joins again.
                joinedGroup = false
                return PolledMessages(partitionID: PolledMessages.noAssignedPartition, currentOffset: 0, count: 0, messages: [])
            }
            if isConsumerGroup, [.disconnected, .unauthenticated, .staleClient].contains(error.code) {
                joinedGroup = false
            }
            try await Task.sleep(for: retryInterval)
            throw error
        }
    }

    private func handOverPolled(_ polled: PolledMessages) -> ReceivedMessage {
        let partition = polled.partitionID
        currentPartitionID = partition
        buffer = polled.messages
        bufferIndex = 0
        bufferedContext = polled.context
        bufferedCurrentOffset = polled.currentOffset
        return handOverBuffered()
    }

    private func handOverBuffered() -> ReceivedMessage {
        let partition = currentPartitionID
        let message = buffer[bufferIndex]
        bufferIndex += 1
        let position = ConsumerPosition(partitionID: partition, offset: message.offset, context: bufferedContext)
        lastConsumed[partition] = position
        let drained = bufferIndex == buffer.count
        switch configuration.autoCommit.whenTrigger {
        case .consumingEachMessage?:
            queueCommit(position)
        case .consumingEveryNthMessage(let n)?:
            if message.offset % UInt64(n) == 0 {
                queueCommit(position)
            }
        case .consumingAllMessages?:
            if drained {
                queueCommit(position)
            }
        case .pollingMessages?, nil:
            break
        }
        let received = ReceivedMessage(message: message, currentOffset: bufferedCurrentOffset, partitionID: partition, context: bufferedContext)
        if drained {
            // The strategy only says where a partition starts; the next poll
            // of it continues after the last message handed over. A `next`
            // poll that commits starts after the offset it stored instead.
            if configuration.pollingStrategy.kind != .next || configuration.autoCommit.whenTrigger != .pollingMessages {
                nextPositions[partition] = ConsumerPosition(partitionID: partition, offset: message.offset + 1, context: bufferedContext)
            }
            buffer.removeAll(keepingCapacity: true)
            bufferIndex = 0
        }
        return received
    }

    /// Queues a commit for the commit task. A later one for the same
    /// partition replaces a queued one not sent yet.
    private func queueCommit(_ position: ConsumerPosition) {
        guard isInitialized, !isShutDown else {
            logger.error("The commit was not queued because the consumer is not running", metadata: metadata)
            return
        }
        pendingCommits[position.partitionID] = position
        commitWakeup.yield()
    }

    /// Sends the queued commits. Returns false once the consumer is shut
    /// down and nothing is left, which ends the commit task.
    private func sendPendingCommits() async -> Bool {
        while let partition = pendingCommits.keys.first, let position = pendingCommits.removeValue(forKey: partition) {
            await storeInBackground(position)
        }
        return !isShutDown
    }

    /// Returns false once the consumer is shut down, which ends the interval
    /// task before it stores anything more, or right after the store that a
    /// shutdown waits for.
    private func storeOnTick() async -> Bool {
        guard !isShutDown else {
            return false
        }
        intervalIsStoring = true
        defer { intervalIsStoring = false }
        for position in lastConsumed.values {
            await storeInBackground(position)
        }
        return !isShutDown
    }

    private func storeFinalPositions(_ positions: [ConsumerPosition]) async {
        for position in positions {
            do {
                try await store(position, allowReplay: false)
            } catch {
                logger.warning(
                    "The final checkpoint was not acknowledged",
                    metadata: metadata.merging(["partition": "\(position.partitionID)", "offset": "\(position.offset)", "error": "\(error)"]) { $1 })
            }
        }
    }

    /// Background commits have no caller to report to, so a failure is
    /// logged. They never move a stored offset back: replay governs reading.
    private func storeInBackground(_ position: ConsumerPosition) async {
        do {
            try await store(position, allowReplay: false)
        } catch {
            logger.error(
                "Failed to store the offset",
                metadata: metadata.merging(["partition": "\(position.partitionID)", "offset": "\(position.offset)", "error": "\(error)"]) { $1 })
        }
    }

    private func store(_ position: ConsumerPosition, allowReplay: Bool) async throws {
        try await offsetWrites.lock()
        defer { offsetWrites.unlock() }
        if !allowReplay, storedPositionCovers(position) {
            return
        }
        try await client.storeConsumerPosition(consumer: consumer, streamID: streamID, topicID: topicID, position: position)
        lastStored[position.partitionID] = position
    }

    /// A store without a context proves nothing about a partition history.
    private func storedPositionCovers(_ position: ConsumerPosition) -> Bool {
        guard let stored = lastStored[position.partitionID], stored.context.incarnation != 0, position.context.incarnation != 0 else {
            return false
        }
        return Self.sameHistory(stored.context, position.context) && position.offset <= stored.offset
    }

    /// Offsets compare only within one incarnation and owner. The metadata
    /// frontier is left out: another group's change can move it between two
    /// polls of the same partition.
    static func sameHistory(_ left: PartitionContext, _ right: PartitionContext) -> Bool {
        left.incarnation == right.incarnation && left.ownerGeneration == right.ownerGeneration
    }

    static func endsConsumption(_ error: IggyError) -> Bool {
        switch error.code {
        case .invalidServerAddress, .invalidClientAddress, .clientShutdown: true
        default: false
        }
    }

    /// Waits for `task` up to `timeout`, then cancels it and waits for it to
    /// end: a waiter that gives up does not cancel what it waits for.
    private static func wait(for task: Task<Void, Never>, upTo timeout: Duration) async {
        await withTaskGroup(of: Void.self) { group in
            group.addTask { await task.value }
            group.addTask {
                if (try? await Task.sleep(for: timeout)) != nil {
                    task.cancel()
                }
            }
            _ = await group.next()
            group.cancelAll()
        }
    }
}

extension IggyClient {
    /// A standalone consumer of one partition, identified by `name`. Building
    /// it never talks to the server; ``IggyConsumer/initialize()`` does.
    /// Throws ``IggyErrorCode/invalidConfiguration`` for an option out of
    /// range.
    public func consumer(
        name: String, streamID: Identifier, topicID: Identifier, partitionID: UInt32, configuration: ConsumerConfiguration = ConsumerConfiguration()
    ) throws -> IggyConsumer {
        try IggyConsumer(
            client: self, name: name, consumer: .consumer(try Identifier(named: name)), streamID: streamID, topicID: topicID, partitionID: partitionID,
            configuration: configuration)
    }

    /// A member of the consumer group `name`, which reads the partitions the
    /// server assigns to it. Building it never talks to the server;
    /// ``IggyConsumer/initialize()`` does.
    public func consumerGroup(
        name: String, streamID: Identifier, topicID: Identifier, configuration: ConsumerConfiguration = ConsumerConfiguration()
    ) throws -> IggyConsumer {
        try IggyConsumer(
            client: self, name: name, consumer: .group(try Identifier(named: name)), streamID: streamID, topicID: topicID, partitionID: nil,
            configuration: configuration)
    }
}
