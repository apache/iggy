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
/// Messages come from a buffer that deferred requests refill, with
/// ``ConsumerConfiguration/pollOptions``: the server holds each request
/// until a message arrives or the wait runs out, so an idle consumer gets a
/// new message at once without polling in a loop. A standalone consumer
/// polls its partition once the buffer runs empty.
///
/// A group member fetches the partitions of each readiness group, the ones
/// that share a primary, with one request. Unless it commits with
/// ``AutoCommitWhen/pollingMessages``, the next fetch of a group goes out as
/// soon as the reply before it arrives, before that reply is handed over,
/// and a partition that turns idle while the fetch waits joins a second
/// fetch. A member that commits on poll fetches a group only once the reader
/// found nothing left to hand over, so at most one batch per group is stored
/// as consumed before it was asked for. A partition that starts at `first`,
/// `last` or a timestamp is polled alone until it has a position: a member
/// that commits on poll only finds that position with a poll that commits
/// nothing. A message larger than `maxBytes` is polled alone too. A member
/// continues after an offset stored at or after its position, so a store
/// can skip messages.
///
/// A failed poll of a standalone consumer is thrown after
/// ``ConsumerConfiguration/pollingRetryInterval`` and leaves the consumer
/// usable, so a caller of ``next()`` can call it again; a `for try await`
/// loop ends at the first error instead. A group member throws a failed read
/// at once and reads its partitions again after the same interval, and so
/// does a fetched batch that does not decode, from its first message.
/// ``next()`` returns nil only after ``shutdown()``. One reader consumes a
/// consumer, as in the Rust SDK: two loops over the same consumer split its
/// messages between them.
///
/// The consumer remembers the last message handed over from each partition.
/// Without ``ConsumerConfiguration/allowReplay`` a message at or below it is
/// not handed over again, and ``ConsumerConfiguration/autoCommit`` decides
/// when the reading position is stored on the server.
///
/// Unlike the Rust consumer, this one has no encryptor, since the Swift
/// client has none, and does not resolve a group create or join whose outcome
/// was lost: the Swift client has no operation lanes to resolve them through.
/// For the same reason a store or delete of a partition waits for the reply
/// of a group read in flight that reads or moves the stored offset of that
/// partition, one that commits or starts after the stored offset, and no
/// such read starts while the store waits. So the two never reorder on the
/// server.
public actor IggyConsumer: AsyncSequence {
    public typealias Element = ReceivedMessage

    /// The most requests of a group member in flight, as
    /// `MAX_DEFERRED_REQUESTS_PER_CONSUMER` in the Rust SDK, so a member
    /// leaves deferred connections of its client to others.
    static let maxRequestsInFlight = 16
    /// How often a waiting group member checks the assignment that its
    /// client syncs, as `ASSIGNMENT_REFRESH_INTERVAL` in the Rust SDK.
    static let assignmentCheckInterval: Duration = .seconds(1)
    /// A group member that commits nothing on poll fetches a group while
    /// fewer replies of the group wait for the reader. So the next fetch goes
    /// out before the reader takes the reply before it, and the buffer stays
    /// bounded.
    static let waitingRepliesPerGroup = 2

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
    /// One reader at a time: a reader that finds the buffer empty waits here
    /// while another one polls or fetches.
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
    /// Partitions whose next message did not fit `maxBytes`, which their next
    /// poll reads alone, as a poll without a byte limit would.
    private var oversizedPartitions: Set<UInt32> = []
    /// The request of the poll in flight of a standalone consumer, which a
    /// shutdown waits for or cancels.
    private var polling: Task<PolledMessages, any Error>?
    /// A batch that a committing read brought back after the shutdown began.
    /// The server stored it as consumed, so the final stores rewind it.
    private var undelivered: PolledMessages?
    /// A recovery block can stop the re-read of a committed undecodable batch.
    /// Keep its rewind position without retaining the records.
    private var unreadableStarts: [UInt32: ConsumerPosition] = [:]
    /// Where each partition continues after the last batch read from it,
    /// unless a `next` poll of a standalone consumer commits, which leaves
    /// that to the offset it stores.
    private var nextPositions: [UInt32: ConsumerPosition] = [:]
    private var lastConsumed: [UInt32: ConsumerPosition] = [:]
    private var lastStored: [UInt32: ConsumerPosition] = [:]
    /// Commits queued by the message triggers, the latest per partition.
    private var pendingCommits: [UInt32: ConsumerPosition] = [:]
    private var eventsTask: Task<Void, Never>?
    private var intervalTask: Task<Void, Never>?
    private var intervalIsStoring = false
    private var commitTask: Task<Void, Never>?

    /// The assigned partitions of a group member grouped by primary, each
    /// group in the order of its next fetch.
    private var fetchGroups: [[UInt32]] = []
    /// The assignment that the member reads.
    private var groupedAssignment: Set<UInt32> = []
    /// Moves on when a primary or the assignment can have moved. The member
    /// reads nothing until the reader grouped the partitions again.
    private var groupingEpoch: UInt64 = 1
    private var groupedEpoch: UInt64 = 0
    private var requests: [UInt64: GroupRequest] = [:]
    /// The request that reads each partition.
    private var activeReads: [UInt32: UInt64] = [:]
    private var lastRequestID: UInt64 = 0
    /// What the requests of a group member brought back, in arrival order.
    private var arrivals: [Arrival] = []
    private var fetchRecovery: IggyError?
    /// Partitions that a failed read leaves alone until then.
    private var pausedUntil: [UInt32: ContinuousClock.Instant] = [:]
    /// Partitions of a fetch that the server refused before it read any of
    /// them. Each is probed alone, so its own refusal names it.
    private var probedPartitions: Set<UInt32> = []
    /// The partition polled last, so polls take turns when the request
    /// limit binds.
    private var pollCursor: UInt32?
    /// Set while the reader waits for a request to end.
    private var readerWakeup: AsyncStream<Void>.Continuation?
    /// The stores and deletes of each partition that wait for a request.
    private var storesWaiting: [UInt32: Int] = [:]
    private var storeWakeups: [AsyncStream<Void>.Continuation] = []

    /// A request of a group member in flight.
    private struct GroupRequest {
        let kind: RequestKind
        let partitions: [UInt32]
        /// A poll of a message larger than `maxBytes`.
        let readsAlone: Bool
        /// Whether the request reads or moves the stored offsets of its
        /// partitions: it commits, or one of them starts after its stored
        /// offset.
        let progresses: Bool
        let commits: Bool
        let task: Task<Void, Never>
    }

    private enum RequestKind {
        /// The fetch of a readiness group.
        case main
        /// The fetch of the partitions of a group that turned idle while
        /// its main fetch waited. It starts again with each one that does.
        case second
        /// A poll of one partition whose batch is handed over.
        case alone
        /// A poll of one partition that hands nothing over and commits
        /// nothing. It finds where a fetch that commits starts, or which
        /// partition the server refuses.
        case check
    }

    private enum PollSize {
        case batch
        /// The message alone, without a byte limit, as a plain poll reads it.
        case oversized
        /// Only a refusal answers a probe, so it reads one message and does
        /// not wait.
        case probe
    }

    private enum Reply: Sendable {
        case fetched([FetchSection])
        case polled(PolledMessages)
    }

    /// What a request of a group member brought back for the reader.
    private enum Arrival {
        /// A section of a fetch that holds messages, not decoded yet.
        case section(FetchSection, request: UInt64)
        case batch(PolledMessages)
        case failure(any Error)

        var partitionID: UInt32? {
            switch self {
            case .section(let section, _): section.partitionID
            case .batch(let batch): batch.partitionID
            case .failure: nil
            }
        }

        var isFailure: Bool {
            if case .failure = self { true } else { false }
        }
    }

    init(
        client: IggyClient, name: String, consumer: Consumer, streamID: Identifier, topicID: Identifier, partitionID: UInt32?,
        configuration: ConsumerConfiguration
    ) throws {
        try configuration.validate()
        if consumer.kind == .consumerGroup {
            try configuration.pollOptions.validateFetch(count: configuration.batchLength)
        }
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
        // A request that commits ends with its reply, which the server stored.
        for request in requests.values where !request.commits {
            request.task.cancel()
        }
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
        if let polling {
            // The server stores the batch of a committing poll as it reads it,
            // so such a poll gets the drain time to bring the batch back, as in
            // the Rust SDK. Any other poll in flight is cancelled at once.
            if commitsOnPoll {
                await Self.wait(for: polling, upTo: timeout)
            } else {
                polling.cancel()
            }
        }
        // A group read that commits is never cancelled: the shutdown waits for
        // its reply, at most its request timeout, and rewinds what it stored.
        for (id, request) in requests where !request.commits {
            cancel(id)
        }
        readerWakeup?.yield()
        for task in requests.values.map(\.task) {
            await task.value
        }
        // The reader records the batch the poll brought back before it lets
        // go. One that pauses after a failed poll holds none.
        _ = try? await withDeadline(.now + timeout) { [reading] in
            try await reading.lock()
            reading.unlock()
        }
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
        if let fetchRecovery, let operationID = fetchRecovery.operationID, client.core.pollRouter.hasUncertainFetch(operationID) {
            logger.warning("Consumer progress remains blocked", metadata: metadata.merging(["error": "\(fetchRecovery)"]) { $1 })
        }
        if configuration.autoCommit != .disabled {
            let positions = Array(lastConsumed.values)
            let undelivered = undeliveredStarts()
            // One deadline for every final store, as in the Rust SDK.
            _ = try? await withDeadline(.now + client.configuration.requestTimeout) {
                for start in undelivered {
                    await self.rewind(start)
                }
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

    /// The next message: from the buffer, or from a poll or fetch once it is
    /// empty. Throws what a read or join ran into and can be called again
    /// after it threw. Nil once the consumer is shut down.
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
            let polled: PolledMessages
            do {
                if let partitionID {
                    polled = try await poll(partitionID)
                } else {
                    polled = try await fetched()
                }
            } catch {
                // Reading ends with nil after a shutdown, which can have
                // cancelled this poll.
                guard isShutDown else {
                    throw error
                }
                return nil
            }
            if isShutDown {
                if commitsOnPoll, !polled.messages.isEmpty {
                    undelivered = polled
                }
                return nil
            }
            if !polled.messages.isEmpty {
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
        try await afterProgress(of: partition) {
            try await offsetWrites.lock()
            defer { offsetWrites.unlock() }
            try await client.storeConsumerOffset(consumer: consumer, streamID: streamID, topicID: topicID, partitionID: partition, offset: offset)
            lastStored[partition] = ConsumerPosition(partitionID: partition, offset: offset, context: .empty)
        }
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
        try await deleteOffset(partitionID ?? currentPartitionID, context: nil)
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

    private var groupKey: ConsumerGroupState.GroupKey {
        ConsumerGroupState.GroupKey(stream: streamID, topic: topicID, group: consumer.id)
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

    /// The server stores the offset of each batch it returns, before any of
    /// the batch is handed over.
    private var commitsOnPoll: Bool {
        configuration.autoCommit.whenTrigger == .pollingMessages
    }

    /// A client that only the caller brings back never recovers, so reading
    /// goes on and fails with the reason instead of waiting for good.
    /// Without a heartbeat or a commit to send, nothing else signs the client
    /// in again, so the wait does.
    private func waitForSignIn() async throws {
        while !canPoll, !isShutDown, await client.canRecover {
            if (try? await client.requireAuthenticated()) != nil {
                break
            }
            try await Task.sleep(for: configuration.pollingRetryInterval)
        }
    }

    /// Runs the request of a poll as a task that a shutdown can cancel, as
    /// the caller of ``next()`` can. A join or a pause is left to finish, so
    /// no join outcome is lost.
    private func cancellable(_ request: @escaping @Sendable () async throws -> PolledMessages) async throws -> PolledMessages {
        let task = Task(operation: request)
        polling = task
        defer { polling = nil }
        return try await withTaskCancellationHandler {
            try await task.value
        } onCancel: {
            task.cancel()
        }
    }

    /// One poll of a standalone consumer, as the Rust consumer's poll future:
    /// the sign-in wait and the deferred request. An empty result is not an
    /// error.
    private func poll(_ partition: UInt32) async throws -> PolledMessages {
        try await waitForSignIn()
        // A shutdown may have sent its final commits already.
        if isShutDown {
            return .empty
        }
        let strategy = startPoint(of: partition)
        let autoCommit = commitsOnPoll
        let alone = oversizedPartitions.remove(partition) != nil
        let (count, options) = pollLimits(alone ? .oversized : .batch)
        do {
            var polled = try await cancellable { [client, streamID, topicID, consumer] in
                try await client.pollMessages(
                    streamID: streamID, topicID: topicID, partitionID: partition, consumer: consumer, strategy: strategy, count: count,
                    autoCommit: autoCommit, options: options)
            }
            // The strategy only says where a partition starts. The next poll of
            // it continues after this batch, even one that replay skips whole,
            // as the Rust SDK moves its fetch position after every batch. A
            // `next` poll that commits starts after the offset it stored instead.
            if let last = polled.messages.last, configuration.pollingStrategy.kind != .next || !autoCommit {
                nextPositions[partition] = ConsumerPosition(partitionID: partition, offset: last.offset + 1, context: polled.context)
            }
            skipReplayed(&polled)
            return polled
        } catch let cancelled as CancellationError {
            if autoCommit {
                forgetStored(partition)
            }
            throw cancelled
        } catch let error as IggyError {
            if autoCommit, error.code == .transientNotCommitted {
                forgetStored(partition)
            }
            if error.code == .invalidSizeBytes, !alone {
                // The first message of the partition does not fit `maxBytes`.
                // Only a failure of the poll that reads it alone is reported.
                oversizedPartitions.insert(partition)
                return .empty
            }
            if error.code == .historyUnavailable {
                nextPositions[partition] = nil
            }
            logger.error("Failed to poll messages", metadata: metadata.merging(["error": "\(error)"]) { $1 })
            try await Task.sleep(for: configuration.pollingRetryInterval)
            throw error
        }
    }

    private func pollLimits(_ size: PollSize) -> (count: UInt32, options: DeferredPollOptions) {
        var options = configuration.pollOptions
        switch size {
        case .batch:
            return (configuration.batchLength, options)
        case .oversized:
            options.maxBytes = .max
        case .probe:
            options.maxWait = .zero
        }
        options.minCount = 1
        return (1, options)
    }

    /// Where the next read of a partition starts: after the last batch read
    /// from it, under the context of that batch, or where the strategy says.
    /// A group member continues after an offset stored at or after that
    /// position in the same partition history, as in the Rust SDK, so a store
    /// can skip messages that never decode.
    private func startPoint(of partition: UInt32) -> PollingStrategy {
        guard let position = nextPositions[partition] else {
            return configuration.pollingStrategy
        }
        if isConsumerGroup, let stored = lastStored[partition], stored.offset >= position.offset, stored.offset < .max,
            stored.context.incarnation != 0, Self.sameHistory(stored.context, position.context)
        {
            return .offset(stored.offset + 1, context: position.context)
        }
        return .offset(position.offset, context: position.context)
    }

    /// Drops the messages at or below the last one handed over from the same
    /// partition history, unless replay is allowed.
    private func skipReplayed(_ polled: inout PolledMessages) {
        guard !configuration.allowReplay, let consumed = lastConsumed[polled.partitionID], Self.sameHistory(consumed.context, polled.context) else {
            return
        }
        polled.messages.removeAll { $0.offset <= consumed.offset }
        polled.count = UInt32(polled.messages.count)
    }

    // MARK: Group reads

    /// The next batch of a group member. It is empty when the reader woke
    /// without one, as after a pause, a regroup, or at the shutdown.
    private func fetched() async throws -> PolledMessages {
        if let polled = try takeArrival() {
            return polled
        }
        if let fetchRecovery {
            if let operationID = fetchRecovery.operationID, client.core.pollRouter.hasUncertainFetch(operationID) {
                throw fetchRecovery
            }
            self.fetchRecovery = nil
        }
        try await waitForSignIn()
        guard try await prepareGroup(), !isShutDown, !needsRegroup else {
            return .empty
        }
        await waitForArrival()
        // A batch left at the shutdown waits for the rewind of the final
        // stores.
        guard !isShutDown, let polled = try takeArrival() else {
            return .empty
        }
        return polled
    }

    private var needsRegroup: Bool {
        groupedEpoch != groupingEpoch
    }

    /// Joins again once the membership was lost, syncs an assignment the
    /// client does not hold, and groups the assigned partitions by primary.
    /// Returns false when the member cannot read yet: it holds no partitions,
    /// or it lost its membership and joins again on the next call, each after
    /// the retry interval. Any other failure is thrown after the retry
    /// interval.
    private func prepareGroup() async throws -> Bool {
        let retryInterval = configuration.pollingRetryInterval
        do {
            if configuration.autoJoinConsumerGroup, !joinedGroup {
                try await joinGroup()
            }
            // A shutdown may have sent its final commits and leave already.
            if isShutDown {
                return false
            }
            let state = client.core.groupState
            if !state.hasAssignment(groupKey) {
                try await client.syncGroupAssignment(streamID: streamID, topicID: topicID, groupID: consumer.id, lineage: nil)
            }
            // A member can hold no partitions, while a client that is not a
            // member must learn it, or it reads an empty assignment forever
            // instead of joining again.
            guard state.isRegistered(groupKey) else {
                throw IggyError(.consumerGroupMemberNotFound, context: "group \(consumer.id) in topic \(topicID) of stream \(streamID)")
            }
            let assigned = Set(state.assignedPartitions(groupKey) ?? [])
            if assigned != groupedAssignment {
                reassign(assigned)
            }
            if assigned.isEmpty {
                try await Task.sleep(for: retryInterval)
                return false
            }
            if needsRegroup {
                let epoch = groupingEpoch
                // The partitions keep their turns in the fetches of their groups.
                let kept = fetchGroups.joined().filter(assigned.contains)
                let order = kept + assigned.subtracting(kept).sorted()
                fetchGroups = try await client.readinessGroups(streamID: streamID, topicID: topicID, partitions: order)
                groupedEpoch = epoch
            }
            return true
        } catch {
            logger.error("Failed to join the consumer group or to read its assignment", metadata: metadata.merging(["error": "\(error)"]) { $1 })
            let code = (error as? IggyError)?.code
            if let code, [.consumerGroupMemberNotFound, .disconnected, .unauthenticated, .staleClient].contains(code) {
                joinedGroup = false
            }
            // The member joins again after the pause, as in the Rust SDK, or
            // a member that the server keeps refusing joins in a loop.
            try await Task.sleep(for: retryInterval)
            if code == .consumerGroupMemberNotFound, configuration.autoJoinConsumerGroup {
                return false
            }
            throw error
        }
    }

    /// Moves to a new assignment. The reads that commit nothing are
    /// cancelled, so the next ones read the new groups, and what they read of
    /// a lost partition is not handed over. A read that commits ends with its
    /// reply, which the reader still takes, since the server stored it as
    /// consumed.
    private func reassign(_ assigned: Set<UInt32>) {
        for (id, request) in requests where !request.commits {
            cancel(id)
        }
        if !commitsOnPoll {
            arrivals.removeAll { arrival in arrival.partitionID.map { !assigned.contains($0) } ?? false }
        }
        nextPositions = nextPositions.filter { assigned.contains($0.key) }
        unreadableStarts = unreadableStarts.filter { assigned.contains($0.key) }
        pausedUntil = pausedUntil.filter { assigned.contains($0.key) }
        oversizedPartitions.formIntersection(assigned)
        probedPartitions.formIntersection(assigned)
        groupedAssignment = assigned
        groupingEpoch += 1
    }

    /// Starts what the member can read, then waits until a request ends, the
    /// shutdown begins, a pause ends, or the assignment is due for a check.
    private func waitForArrival() async {
        let (wakeups, wakeup) = AsyncStream<Void>.makeStream(bufferingPolicy: .bufferingNewest(1))
        readerWakeup = wakeup
        defer { readerWakeup = nil }
        dispatch()
        // A request can have ended while the reader prepared the group.
        guard arrivals.isEmpty else {
            return
        }
        var wakeAt = ContinuousClock.now + Self.assignmentCheckInterval
        if let pauseEnd = pausedUntil.values.min() {
            wakeAt = Swift.min(wakeAt, pauseEnd)
        }
        let timer = Task {
            if (try? await Task.sleep(until: wakeAt, clock: .continuous)) != nil {
                wakeup.yield()
            }
        }
        defer { timer.cancel() }
        for await _ in wakeups {
            break
        }
    }

    /// The next batch a request brought back, without the messages handed
    /// over before, or the error queued next. A section is decoded only now,
    /// after the next fetch of its group went out.
    private func takeArrival() throws -> PolledMessages? {
        guard !arrivals.isEmpty else {
            return nil
        }
        let arrival = arrivals.removeFirst()
        // A reply that the reader took makes room for the next fetch of its
        // group.
        defer { dispatch() }
        var polled: PolledMessages
        switch arrival {
        case .section(let section, _):
            do {
                polled = try section.decode()
            } catch {
                readAgain(section)
                throw error
            }
        case .batch(let batch):
            polled = batch
        case .failure(let error):
            throw error
        }
        skipReplayed(&polled)
        return polled
    }

    /// Reads a section that does not decode again, as the Rust SDK does: its
    /// partition moves back to the first message of the section and waits
    /// for the retry interval, and the reads of the partition that started
    /// past the section are dropped. A fetch that commits stored the section
    /// as consumed, so the final store sends the last message handed over
    /// again.
    private func readAgain(_ section: FetchSection) {
        let partition = section.partitionID
        if groupedAssignment.contains(partition) {
            nextPositions[partition] = section.firstOffset.map { ConsumerPosition(partitionID: partition, offset: $0, context: section.context) }
        }
        pausedUntil[partition] = .now + configuration.pollingRetryInterval
        arrivals.removeAll { $0.partitionID == partition }
        if let id = activeReads[partition], requests[id]?.commits == false {
            cancel(id)
        }
        if commitsOnPoll {
            if let first = section.firstOffset {
                unreadableStarts[partition] = ConsumerPosition(partitionID: partition, offset: first, context: section.context)
            }
            forgetStored(partition)
        }
    }

    /// Starts what the group member can read now. A partition that a request
    /// reads or that waits out a pause is left alone.
    private func dispatch() {
        guard isConsumerGroup, !isShutDown, !needsRegroup, fetchRecovery == nil else {
            return
        }
        let now = ContinuousClock.now
        pausedUntil = pausedUntil.filter { $0.value > now }
        if !commitsOnPoll {
            for group in fetchGroups {
                fetchAhead(group)
            }
        } else if readerWakeup != nil, arrivals.isEmpty {
            // Only a reader that waits with nothing queued asks for more, so
            // the server stores a batch as consumed only once it was asked for.
            for group in fetchGroups {
                takeCommittingTurn(of: group)
            }
        }
        pollAlone()
    }

    /// Fetches the partitions of a group that no request reads. They join
    /// the main fetch of the group once the last one ended, else its second
    /// fetch, which starts again with them, so they do not wait out the wait
    /// of the main fetch. A group whose replies fill the room of the reader
    /// waits for the reader.
    private func fetchAhead(_ group: [UInt32]) {
        guard waitingReplies(of: group) < Self.waitingRepliesPerGroup else {
            return
        }
        let joining = group.filter { isIdle($0) && isFetchable($0) }
        guard !joining.isEmpty else {
            return
        }
        var hasMain = false
        var second: UInt64?
        for partition in group {
            guard let id = activeReads[partition], let request = requests[id] else {
                continue
            }
            switch request.kind {
            case .main: hasMain = true
            case .second: second = id
            case .alone, .check: break
            }
        }
        guard second != nil || requests.count < Self.maxRequestsInFlight else {
            return
        }
        var targets = Set(joining)
        if let second, let request = finish(second) {
            request.task.cancel()
            targets.formUnion(request.partitions)
        }
        var ordered = group.filter(targets.contains)
        if ordered.contains(where: { startPoint(of: $0).kind == .next }) {
            ordered.removeAll { storesWaiting[$0] != nil }
        }
        if !ordered.isEmpty {
            startFetch(ordered, kind: hasMain ? .second : .main)
        }
    }

    /// The read of a group that commits on poll: a message larger than
    /// `maxBytes` alone, or else one fetch of the partitions the group can
    /// fetch. A fetch without the partition of a pending store would wait
    /// without it, so the group waits for the store, which ends within one
    /// round trip.
    private func takeCommittingTurn(of group: [UInt32]) {
        let turnTaken = group.contains { partition in
            storesWaiting[partition] != nil || activeReads[partition].flatMap { requests[$0] }.map { $0.kind != .check } ?? false
        }
        guard !turnTaken, requests.count < Self.maxRequestsInFlight else {
            return
        }
        let idle = group.filter(isIdle)
        if let oversized = idle.first(where: oversizedPartitions.contains) {
            startPoll(oversized, kind: .alone)
            return
        }
        let targets = idle.filter(isFetchable)
        if !targets.isEmpty {
            startFetch(targets, kind: .main)
        }
    }

    /// Polls the partitions that a fetch cannot read, each alone, taking
    /// turns when the request limit binds. A request per group stays free of
    /// them, so polls that wait for a first message never hold every slot.
    private func pollAlone() {
        let partitions = Array(fetchGroups.joined())
        let limit = Swift.max(Self.maxRequestsInFlight - fetchGroups.count, 1)
        var polls = requests.values.filter { $0.kind == .alone || $0.kind == .check }.count
        let start = pollCursor.flatMap(partitions.firstIndex(of:)).map { $0 + 1 } ?? 0
        for index in partitions.indices {
            guard polls < limit, requests.count < Self.maxRequestsInFlight else {
                return
            }
            let partition = partitions[(start + index) % partitions.count]
            guard isIdle(partition), !isFetchable(partition) else {
                continue
            }
            let kind: RequestKind
            if probedPartitions.contains(partition) {
                kind = .check
            } else if !commitsOnPoll {
                kind = .alone
            } else if oversizedPartitions.contains(partition) {
                // The turn of its group reads it.
                continue
            } else {
                kind = .check
            }
            if startPoll(partition, kind: kind) {
                pollCursor = partition
                polls += 1
            }
        }
    }

    private func isIdle(_ partition: UInt32) -> Bool {
        activeReads[partition] == nil && pausedUntil[partition] == nil
    }

    /// A fetch reads a partition that starts at an offset or after its
    /// stored offset, whose next message fits `maxBytes`, and whose probe is
    /// answered.
    private func isFetchable(_ partition: UInt32) -> Bool {
        !oversizedPartitions.contains(partition) && !probedPartitions.contains(partition) && IggyClient.fetchTakes(startPoint(of: partition))
    }

    /// How many replies of fetches of `group` wait for the reader.
    private func waitingReplies(of group: [UInt32]) -> Int {
        let members = Set(group)
        var replies: Set<UInt64> = []
        for case .section(let section, let request) in arrivals where members.contains(section.partitionID) {
            replies.insert(request)
        }
        return replies.count
    }

    private func startFetch(_ partitions: [UInt32], kind: RequestKind) {
        let targets = partitions.map { (partitionID: $0, strategy: startPoint(of: $0)) }
        let commits = commitsOnPoll
        let progresses = commits || targets.contains { $0.strategy.kind == .next }
        start(kind, partitions: partitions, readsAlone: false, progresses: progresses, commits: commits) {
            [client, streamID, topicID, consumer, configuration] in
            .fetched(
                try await client.fetchMessages(
                    streamID: streamID, topicID: topicID, targets: targets, consumer: consumer, count: configuration.batchLength,
                    autoCommit: commits, options: configuration.pollOptions))
        }
    }

    /// Starts a poll of one partition, unless it would read or move the
    /// stored offset while a store of the partition waits.
    @discardableResult
    private func startPoll(_ partition: UInt32, kind: RequestKind) -> Bool {
        let strategy = startPoint(of: partition)
        let commits = kind == .alone && commitsOnPoll
        let progresses = kind == .alone && (commits || strategy.kind == .next)
        if progresses, storesWaiting[partition] != nil {
            return false
        }
        let readsAlone = kind == .alone && oversizedPartitions.contains(partition)
        let size: PollSize
        if readsAlone {
            size = .oversized
        } else if kind == .check, probedPartitions.contains(partition) {
            size = .probe
        } else {
            size = .batch
        }
        let (count, options) = pollLimits(size)
        start(kind, partitions: [partition], readsAlone: readsAlone, progresses: progresses, commits: commits) {
            [client, streamID, topicID, consumer] in
            .polled(
                try await client.pollMessages(
                    streamID: streamID, topicID: topicID, partitionID: partition, consumer: consumer, strategy: strategy, count: count,
                    autoCommit: commits, options: options))
        }
        return true
    }

    private func start(
        _ kind: RequestKind, partitions: [UInt32], readsAlone: Bool, progresses: Bool, commits: Bool,
        _ read: @escaping @Sendable () async throws -> Reply
    ) {
        lastRequestID += 1
        let id = lastRequestID
        let task = Task { [weak self] in
            let outcome: Result<Reply, any Error>
            do {
                outcome = .success(try await read())
            } catch {
                outcome = .failure(error)
            }
            await self?.requestEnded(id, outcome)
        }
        requests[id] = GroupRequest(kind: kind, partitions: partitions, readsAlone: readsAlone, progresses: progresses, commits: commits, task: task)
        for partition in partitions {
            activeReads[partition] = id
        }
    }

    /// Forgets a request, so its partitions can be read again, and wakes the
    /// stores that wait for it.
    @discardableResult
    private func finish(_ id: UInt64) -> GroupRequest? {
        guard let request = requests.removeValue(forKey: id) else {
            return nil
        }
        for partition in request.partitions where activeReads[partition] == id {
            activeReads[partition] = nil
        }
        if request.progresses {
            for wakeup in storeWakeups {
                wakeup.finish()
            }
            storeWakeups.removeAll()
        }
        return request
    }

    /// Cancels a request that commits nothing: its reply is dropped and its
    /// partitions are read again.
    private func cancel(_ id: UInt64) {
        finish(id)?.task.cancel()
    }

    /// Records what a request of the group member brought back, then starts
    /// what it freed and wakes the reader.
    private func requestEnded(_ id: UInt64, _ outcome: Result<Reply, any Error>) {
        // A request that the consumer cancelled was forgotten, and so is its
        // reply.
        guard let request = finish(id) else {
            return
        }
        switch outcome {
        case .success(.fetched(let sections)):
            received(sections, from: id, of: request)
        case .success(.polled(let polled)):
            received(polled, of: request)
        case .failure(let error):
            failed(request, with: error)
        }
        dispatch()
        readerWakeup?.yield()
    }

    /// Moves each partition that the fetch read to its next offset and
    /// queues the sections that hold messages, still undecoded. A partition
    /// that the server could not read waits for the retry interval. The
    /// server reads the targets in request order, so the partitions that
    /// returned messages go last in the next fetch of their group, and each
    /// one gets its turn.
    private func received(_ sections: [FetchSection], from id: UInt64, of request: GroupRequest) {
        var unanswered = Set(request.partitions)
        var served: Set<UInt32> = []
        var failure: IggyError?
        for section in sections {
            let partition = section.partitionID
            // Only the first section of a partition the fetch named counts.
            guard unanswered.remove(partition) != nil else {
                continue
            }
            guard let error = section.error else {
                if groupedAssignment.contains(partition) {
                    nextPositions[partition] = ConsumerPosition(partitionID: partition, offset: section.nextOffset, context: section.context)
                }
                if section.messagesCount > 0 {
                    served.insert(partition)
                    arrivals.append(.section(section, request: id))
                }
                continue
            }
            logger.error("Failed to fetch messages", metadata: metadata.merging(["partition": "\(partition)", "error": "\(error)"]) { $1 })
            if error.code == .invalidSizeBytes {
                oversizedPartitions.insert(partition)
                continue
            }
            pausedUntil[partition] = .now + configuration.pollingRetryInterval
            if repair(error, of: [partition]), failure == nil {
                failure = error
            }
        }
        if !served.isEmpty {
            fetchGroups = fetchGroups.map { group in group.filter { !served.contains($0) } + group.filter(served.contains) }
        }
        if let failure {
            report(failure)
        }
    }

    /// Moves a partition polled alone past its batch and queues the batch. A
    /// check hands nothing over: its first message only says where a fetch
    /// that commits starts.
    private func received(_ polled: PolledMessages, of request: GroupRequest) {
        guard let partition = request.partitions.first else {
            return
        }
        if polled.messages.isEmpty, polled.partitionID == PolledMessages.resyncRequiredPartition {
            // The coordinator fences a stale assignment through the empty
            // poll, for want of a typed error.
            resyncAssignment()
            return
        }
        probedPartitions.remove(partition)
        if request.readsAlone {
            oversizedPartitions.remove(partition)
        }
        let assigned = groupedAssignment.contains(partition)
        guard request.kind == .alone else {
            if assigned, nextPositions[partition] == nil, !IggyClient.fetchTakes(configuration.pollingStrategy), let first = polled.messages.first {
                nextPositions[partition] = ConsumerPosition(partitionID: partition, offset: first.offset, context: polled.context)
            }
            return
        }
        guard let last = polled.messages.last else {
            return
        }
        if assigned {
            nextPositions[partition] = ConsumerPosition(partitionID: partition, offset: last.offset + 1, context: polled.context)
        }
        arrivals.append(.batch(polled))
    }

    /// Repairs what a failed request points at, and its partitions wait for
    /// the retry interval. A poll refused because its message does not fit
    /// `maxBytes` reads it alone next instead, and only a failure of that
    /// poll is reported. A fence that fails a fetch of many partitions does
    /// not name the one it refused, so each of them is probed alone at once,
    /// as in the Rust SDK, and only the refusal of a probe is reported.
    private func failed(_ request: GroupRequest, with error: any Error) {
        logger.error("Failed to read messages", metadata: metadata.merging(["partitions": "\(request.partitions)", "error": "\(error)"]) { $1 })
        let code = (error as? IggyError)?.code
        let polls = request.kind == .alone || request.kind == .check
        if polls, let partition = request.partitions.first {
            // The refusal of a poll names its partition, which answers a probe.
            probedPartitions.remove(partition)
            if code == .invalidSizeBytes, !request.readsAlone {
                oversizedPartitions.insert(partition)
                return
            }
        }
        if !polls, request.partitions.count > 1, code == .historyUnavailable || code == .consumerGroupPartitionNotOwned {
            probedPartitions.formUnion(request.partitions)
            return
        }
        let until = ContinuousClock.now + configuration.pollingRetryInterval
        for partition in request.partitions {
            pausedUntil[partition] = until
        }
        if request.commits, code == .transientNotCommitted {
            for partition in request.partitions {
                forgetStored(partition)
            }
        }
        let reports = (error as? IggyError).map { repair($0, of: request.partitions) } ?? true
        if reports {
            report(error)
        }
    }

    /// Repairs what `error` of `partitions` points at, as the Rust consumer
    /// does: a moved primary regroups, a lost history drops the positions, a
    /// fence also syncs the assignment again, and a lost membership joins
    /// again. Returns whether the reader sees the error.
    private func repair(_ error: IggyError, of partitions: [UInt32]) -> Bool {
        switch error.code {
        case .notPrimary:
            groupingEpoch += 1
            return false
        case .consumerGroupPartitionNotOwned, .historyUnavailable:
            for partition in partitions {
                nextPositions[partition] = nil
            }
            if error.code == .consumerGroupPartitionNotOwned {
                resyncAssignment()
            }
            return error.code == .historyUnavailable
        case .consumerGroupMemberNotFound:
            joinedGroup = false
            return !configuration.autoJoinConsumerGroup
        case .disconnected, .unauthenticated, .staleClient:
            joinedGroup = false
            return true
        default:
            return true
        }
    }

    /// Drops the cached assignment. The reader syncs it again before the
    /// member reads on.
    private func resyncAssignment() {
        client.core.groupState.invalidateAssignment(groupKey)
        groupingEpoch += 1
    }

    /// Queues an error for the reader. One waits at a time, so a reader that
    /// stays away finds the first one, after the batches before it.
    private func report(_ error: any Error) {
        if let error = error as? IggyError, error.code == .fetchProgressUncertain || error.code == .partitionLaneBlocked, fetchRecovery == nil {
            fetchRecovery = error
        }
        guard !arrivals.contains(where: \.isFailure) else {
            return
        }
        arrivals.append(.failure(error))
    }

    // MARK: Commits

    /// The server can have stored the batch of a committing poll whose reply
    /// never arrived, so the final store sends the last message handed over
    /// again, as in the Rust SDK. A record that differs from that message
    /// makes it go out anyway.
    private func forgetStored(_ partition: UInt32) {
        if lastStored[partition] == lastConsumed[partition] {
            lastStored[partition] = nil
        }
    }

    /// The first message of each batch that the server stored as consumed
    /// but that was never handed over, the earliest per partition, which the
    /// final stores rewind. A section needs no decode for it.
    private func undeliveredStarts() -> [ConsumerPosition] {
        guard commitsOnPoll else {
            return []
        }
        var starts = Array(unreadableStarts.values)
        unreadableStarts.removeAll()
        if let undelivered, let first = undelivered.messages.first {
            starts.append(ConsumerPosition(partitionID: undelivered.partitionID, offset: first.offset, context: undelivered.context))
        }
        undelivered = nil
        for arrival in arrivals {
            switch arrival {
            case .section(let section, _):
                if let first = section.firstOffset {
                    starts.append(ConsumerPosition(partitionID: section.partitionID, offset: first, context: section.context))
                }
            case .batch(let batch):
                if let first = batch.messages.first {
                    starts.append(ConsumerPosition(partitionID: batch.partitionID, offset: first.offset, context: batch.context))
                }
            case .failure:
                break
            }
        }
        arrivals.removeAll()
        var rewound: Set<UInt32> = []
        return starts.filter { rewound.insert($0.partitionID).inserted }
    }

    /// Moves the offset back over messages that the server stored as consumed
    /// but that were never handed over, from `start` on, as
    /// `rewind_undelivered_offsets` in the Rust SDK: the final store of its
    /// partition sends the last message handed over again. A `next` consumer
    /// that handed none over in the partition history of `start` resumes after
    /// the stored offset, so it stores the one before `start`, or deletes it
    /// before offset zero. Both carry the context of `start`, so neither lands
    /// in a newer history.
    private func rewind(_ start: ConsumerPosition) async {
        let partition = start.partitionID
        // Another member reads a partition that left the assignment, so its
        // offset is left alone, as in the Rust SDK. A member that does not
        // know its assignment owns none.
        if isConsumerGroup, client.core.groupState.assignedPartitions(groupKey)?.contains(partition) != true {
            return
        }
        // The server refuses the final store of a message handed over in
        // another history, so that store cannot rewind the batch.
        if let consumed = lastConsumed[partition], Self.sameHistory(consumed.context, start.context) {
            lastStored[partition] = nil
            return
        }
        guard configuration.pollingStrategy.kind == .next else {
            return
        }
        do {
            if start.offset == 0 {
                try await deleteOffset(partition, context: start.context)
            } else {
                try await store(ConsumerPosition(partitionID: partition, offset: start.offset - 1, context: start.context), allowReplay: true)
            }
        } catch {
            logger.error(
                "Failed to move the offset back over messages that were not handed over",
                metadata: metadata.merging(["partition": "\(partition)", "offset": "\(start.offset)", "error": "\(error)"]) { $1 })
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
        unreadableStarts[partition] = nil
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
        // A read that holds the stored offset can wait as long as `maxWait`,
        // and the tick with it. The next tick stores that partition.
        for position in lastConsumed.values where !storedOffsetIsHeld(position.partitionID) {
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

    private func deleteOffset(_ partition: UInt32, context: PartitionContext?) async throws {
        try await afterProgress(of: partition) {
            try await offsetWrites.lock()
            defer { offsetWrites.unlock() }
            // A cancelled delete can still have committed, so a later store must
            // not be skipped as covered.
            lastStored[partition] = nil
            try await client.deleteConsumerOffset(consumer: consumer, streamID: streamID, topicID: topicID, partitionID: partition, context: context)
        }
    }

    private func store(_ position: ConsumerPosition, allowReplay: Bool) async throws {
        if let blocked = client.core.pollRouter.progressRefusal(partitionID: position.partitionID, incarnation: position.context.incarnation) {
            throw blocked
        }
        // A store that is covered already would wait for a read for nothing.
        if !allowReplay, storedPositionCovers(position) {
            return
        }
        try await afterProgress(of: position.partitionID) {
            try await offsetWrites.lock()
            defer { offsetWrites.unlock() }
            if !allowReplay, storedPositionCovers(position) {
                return
            }
            try await client.storeConsumerPosition(consumer: consumer, streamID: streamID, topicID: topicID, position: position)
            lastStored[position.partitionID] = position
        }
    }

    /// Runs a store or delete of `partition` once no group read that reads
    /// or moves its stored offset is in flight, and no such read starts
    /// meanwhile. So the two never reorder on the server, as the progress
    /// lanes of the Rust SDK order them.
    private func afterProgress(of partition: UInt32, _ write: () async throws -> Void) async throws {
        storesWaiting[partition, default: 0] += 1
        defer {
            if storesWaiting[partition] == 1 {
                storesWaiting[partition] = nil
            } else {
                storesWaiting[partition]? -= 1
            }
            dispatch()
        }
        while storedOffsetIsHeld(partition) {
            let (ended, wakeup) = AsyncStream<Void>.makeStream()
            storeWakeups.append(wakeup)
            for await _ in ended {}
            try Task.checkCancellation()
        }
        try await write()
    }

    /// Whether a group read in flight reads or moves the stored offset of
    /// the partition.
    private func storedOffsetIsHeld(_ partition: UInt32) -> Bool {
        activeReads[partition].flatMap { requests[$0] }?.progresses == true
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
    private static func wait<Success: Sendable, Failure: Error>(for task: Task<Success, Failure>, upTo timeout: Duration) async {
        await withTaskGroup(of: Void.self) { group in
            group.addTask { _ = await task.result }
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
    /// range, and what the poll options' validation throws when they do not
    /// fit ``ConsumerConfiguration/batchLength``.
    public func consumer(
        name: String, streamID: Identifier, topicID: Identifier, partitionID: UInt32, configuration: ConsumerConfiguration = ConsumerConfiguration()
    ) throws -> IggyConsumer {
        try IggyConsumer(
            client: self, name: name, consumer: .consumer(try Identifier(named: name)), streamID: streamID, topicID: topicID, partitionID: partitionID,
            configuration: configuration)
    }

    /// A member of the consumer group `name`, which reads the partitions the
    /// server assigns to it. Building it never talks to the server;
    /// ``IggyConsumer/initialize()`` does. Throws what building a standalone
    /// consumer throws, and ``IggyErrorCode/invalidCommand`` for poll options
    /// whose request timeout is not one second past their wait, which a
    /// fetch needs even without a wait.
    public func consumerGroup(
        name: String, streamID: Identifier, topicID: Identifier, configuration: ConsumerConfiguration = ConsumerConfiguration()
    ) throws -> IggyConsumer {
        try IggyConsumer(
            client: self, name: name, consumer: .group(try Identifier(named: name)), streamID: streamID, topicID: topicID, partitionID: nil,
            configuration: configuration)
    }
}
