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

/// Reads messages from one topic and hands them over one at a time.
///
/// Build one with ``IggyClient/consumer(name:stream:topic:partition:configuration:)``
/// for a standalone consumer bound to one partition, or
/// ``IggyClient/consumerGroup(name:stream:topic:configuration:)`` for a member
/// of a group whose partitions the server hands out. Call ``initialize()``
/// once, then iterate:
///
/// ```swift
/// let consumer = try client.consumerGroup(name: "workers", stream: "orders", topic: "created")
/// try await consumer.initialize()
/// for try await received in consumer {
///     print(received.partitionID, received.message.offset, received.message.payloadString)
/// }
/// ```
///
/// A poll failure is thrown from the iteration. The consumer stays usable:
/// iterating again continues from where it was, so a caller that wants to
/// ride out errors loops around a `do`/`catch`. The iteration ends with nil
/// only after ``shutdown()``.
///
/// The consumer records how far it has read per partition and stores that on
/// the server as ``ConsumerConfiguration/autoCommit`` says; ``shutdown()``
/// stores the final positions and leaves the group.
public final class IggyConsumer: AsyncSequence, Sendable {
    public typealias Element = ReceivedMessage

    public let name: String
    public let consumer: Consumer
    public let streamID: Identifier
    public let topicID: Identifier
    public let configuration: ConsumerConfiguration

    let core: ConsumerCore

    /// A consumer on `client`. `partition` is the partition a standalone
    /// consumer reads and is ignored, with a warning, for a group member.
    public convenience init(
        client: IggyClient, name: String, consumer: Consumer, stream: Identifier, topic: Identifier, partition: UInt32?,
        configuration: ConsumerConfiguration = ConsumerConfiguration()
    ) {
        self.init(
            backend: client, name: name, consumer: consumer, stream: stream, topic: topic, partition: partition, configuration: configuration,
            logger: client.logger)
    }

    init(
        backend: any MessagingBackend, name: String, consumer: Consumer, stream: Identifier, topic: Identifier, partition: UInt32?,
        configuration: ConsumerConfiguration, logger: Logger
    ) {
        self.name = name
        self.consumer = consumer
        streamID = stream
        topicID = topic
        self.configuration = configuration
        var partition = partition
        if consumer.kind == .consumerGroup, partition != nil {
            logger.warning(
                "A consumer-group member ignores the partition it was built with and reads the server's assignment", metadata: ["consumer": "\(name)"])
            partition = nil
        }
        core = ConsumerCore(
            backend: backend, name: name, consumer: consumer, streamID: stream, topicID: topic, partitionID: partition, configuration: configuration,
            logger: logger)
    }

    deinit {
        let core = core
        Task { await core.abandon() }
    }

    public var isInitialized: Bool {
        get async { await core.isInitialized }
    }

    /// The partition the most recent batch with messages came from, or zero
    /// before the first one.
    public var partitionID: UInt32 {
        get async { await core.currentPartitionID }
    }

    /// Makes the consumer ready to poll: the stream and topic are looked up,
    /// a group member joins its group, and the commit tasks start.
    public func initialize() async throws {
        try await core.initialize()
    }

    /// The next message, or nil once the consumer is shut down. Throws a poll
    /// failure; the consumer remains usable afterwards.
    public func next() async throws -> ReceivedMessage? {
        try await core.next()
    }

    public func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(consumer: self)
    }

    public struct AsyncIterator: AsyncIteratorProtocol {
        let consumer: IggyConsumer

        public mutating func next() async throws -> ReceivedMessage? {
            try await consumer.next()
        }
    }

    /// Hands every message to `handler` until the consumer is shut down or the
    /// task is cancelled, committing after the handler as
    /// ``AutoCommitAfter`` says. A thrown handler error ends the loop.
    public func consume(_ handler: @Sendable (ReceivedMessage) async throws -> Void) async throws {
        while !Task.isCancelled {
            guard let received = try await next() else {
                return
            }
            try await handler(received)
            await core.afterConsumed(received)
        }
    }

    /// Stores an offset on the server for the partition, or the current one
    /// when nil. An offset that does not advance the last stored one is
    /// skipped unless ``ConsumerConfiguration/allowReplay`` is set.
    public func storeOffset(_ offset: UInt64, partitionID: UInt32? = nil) async throws {
        try await core.storeOffset(offset, partitionID: partitionID)
    }

    /// Deletes the stored offset of the partition, or of the current one for
    /// a standalone consumer; a group passes nil through to the server.
    public func deleteOffset(partitionID: UInt32? = nil) async throws {
        try await core.deleteOffset(partitionID: partitionID)
    }

    /// The offset of the last message handed over from the partition, or nil
    /// while it was not polled yet.
    public func lastConsumedOffset(partitionID: UInt32) async -> UInt64? {
        await core.lastConsumedOffset(partitionID: partitionID)
    }

    /// The offset this consumer last stored for the partition, from its own
    /// record rather than the server.
    public func lastStoredOffset(partitionID: UInt32) async -> UInt64? {
        await core.lastStoredOffset(partitionID: partitionID)
    }

    /// Drains the commit tasks, stores the final reading positions unless
    /// auto-commit is disabled, leaves the consumer group, and stops
    /// following client events. Later polls return nil.
    public func shutdown() async {
        await core.shutdown()
    }
}

actor ConsumerCore {
    private let backend: any MessagingBackend
    private let name: String
    private let consumer: Consumer
    private let streamID: Identifier
    private let topicID: Identifier
    private let partitionID: UInt32?
    private let configuration: ConsumerConfiguration
    private let logger: Logger
    private let isGroup: Bool
    private let commitsOnPoll: Bool
    private let nextLock = AsyncMutex()

    private(set) var isInitialized = false
    private(set) var isShutdown = false
    private(set) var canPoll = true
    private(set) var joined = false
    private(set) var currentPartitionID: UInt32 = 0
    private var lastConsumedOffsets: [UInt32: UInt64] = [:]
    private var lastStoredOffsets: [UInt32: UInt64] = [:]
    private var currentOffsets: [UInt32: UInt64] = [:]
    private var nextOffsets: [UInt32: UInt64] = [:]
    private var buffer: [IggyMessage] = []
    private var bufferIndex = 0
    private var pendingCommits: [UInt32: UInt64] = [:]
    private var lastPolledAt: ContinuousClock.Instant?
    private let intervalNotify = AsyncNotify()
    private let storeNotify = AsyncNotify()
    private var intervalTask: Task<Void, Never>?
    private var storeTask: Task<Void, Never>?
    private var eventsTask: Task<Void, Never>?

    init(
        backend: any MessagingBackend, name: String, consumer: Consumer, streamID: Identifier, topicID: Identifier, partitionID: UInt32?,
        configuration: ConsumerConfiguration, logger: Logger
    ) {
        self.backend = backend
        self.name = name
        self.consumer = consumer
        self.streamID = streamID
        self.topicID = topicID
        self.partitionID = partitionID
        self.configuration = configuration
        self.logger = logger
        isGroup = consumer.kind == .consumerGroup
        commitsOnPoll = configuration.autoCommit.when == .pollingMessages
    }

    // MARK: Lifecycle

    func initialize() async throws {
        if isInitialized {
            return
        }
        logger.info("Initializing the consumer", metadata: ["consumer": "\(name)", "stream": "\(streamID)", "topic": "\(topicID)"])
        try await waitForStreamAndTopic()
        subscribeEvents()
        if isGroup, configuration.autoJoinConsumerGroup {
            try await joinGroup()
        }
        if let interval = configuration.autoCommit.interval {
            intervalTask = Task { await self.storeOffsetsPeriodically(every: interval) }
        }
        storeTask = Task { await self.storePendingCommits() }
        isInitialized = true
        logger.info("The consumer has been initialized", metadata: ["consumer": "\(name)", "stream": "\(streamID)", "topic": "\(topicID)"])
    }

    /// A missing stream or topic is not necessarily permanent: the producer
    /// may still be creating it.
    private func waitForStreamAndTopic() async throws {
        let retries = configuration.initRetries ?? 0
        var attempt: UInt32 = 0
        while true {
            let streamExists = try await backend.getStream(streamID) != nil
            var topicExists = false
            if streamExists {
                topicExists = try await backend.getTopic(streamID: streamID, topicID: topicID) != nil
            }
            if streamExists, topicExists {
                return
            }
            if attempt >= retries {
                if !streamExists {
                    throw IggyError(.streamNameNotFound, context: "stream \(streamID) was not found")
                }
                throw IggyError(.topicNameNotFound, context: "topic \(topicID) was not found in stream \(streamID)")
            }
            attempt += 1
            logger.warning(
                "The stream or topic does not exist yet, retrying",
                metadata: ["stream": "\(streamID)", "topic": "\(topicID)", "retry": "\(attempt)/\(retries)", "interval": "\(configuration.initRetryInterval)"])
            try await Task.sleep(for: configuration.initRetryInterval)
        }
    }

    func shutdown() async {
        guard !isShutdown else {
            return
        }
        isShutdown = true
        logger.info("Shutting down the consumer", metadata: ["consumer": "\(name)"])
        // Drain the commit tasks while still a member, so nothing is stored
        // for a group already left.
        await intervalNotify.notify()
        await drain(intervalTask, label: "interval commit")
        intervalTask = nil
        await storeNotify.notify()
        await drain(storeTask, label: "pending commit")
        storeTask = nil
        if configuration.autoCommit != .disabled {
            for (partition, consumed) in lastConsumedOffsets where consumed > (lastStoredOffsets[partition] ?? 0) {
                try? await store(partition: partition, offset: consumed, allowReplay: configuration.allowReplay)
            }
        }
        if isGroup, joined {
            joined = false
            do {
                try await backend.leaveConsumerGroup(streamID: streamID, topicID: topicID, groupID: consumer.id)
            } catch {
                logger.debug("Failed to leave the consumer group", metadata: ["group": "\(consumer.id)", "error": "\(error)"])
            }
        }
        eventsTask?.cancel()
        eventsTask = nil
        logger.info("The consumer has been shut down", metadata: ["consumer": "\(name)"])
    }

    /// Teardown without the final commits, for a consumer that was dropped
    /// without ``shutdown()``.
    func abandon() {
        isShutdown = true
        intervalTask?.cancel()
        storeTask?.cancel()
        eventsTask?.cancel()
    }

    private func drain(_ task: Task<Void, Never>?, label: String) async {
        guard let task else { return }
        let finished: Void? = await withTimeout(configuration.offsetDrainTimeout) { await task.value }
        if finished == nil {
            task.cancel()
            logger.warning("Timed out draining the \(label) task, aborted", metadata: ["consumer": "\(name)"])
        }
    }

    // MARK: Reading

    func next() async throws -> ReceivedMessage? {
        await nextLock.lock()
        do {
            let received = try await nextUnlocked()
            await nextLock.unlock()
            return received
        } catch {
            await nextLock.unlock()
            throw error
        }
    }

    private func nextUnlocked() async throws -> ReceivedMessage? {
        if isShutdown {
            return nil
        }
        if bufferIndex < buffer.count {
            let message = buffer[bufferIndex]
            bufferIndex += 1
            let partition = currentPartitionID
            lastConsumedOffsets[partition] = message.offset
            if shouldCommitOnHandover(offset: message.offset) {
                queueCommit(partition: partition, offset: message.offset)
            }
            if bufferIndex == buffer.count {
                buffer.removeAll()
                bufferIndex = 0
                if configuration.pollingStrategy.kind != .next {
                    nextOffsets[partition] = message.offset + 1
                }
                if configuration.autoCommit.when == .consumingAllMessages {
                    queueCommit(partition: partition, offset: message.offset)
                }
            }
            return ReceivedMessage(message: message, currentOffset: currentOffsets[partition] ?? 0, partitionID: partition)
        }
        while true {
            let polled = try await poll()
            if isShutdown {
                return nil
            }
            guard let first = polled.messages.first else {
                continue
            }
            let partition = polled.partitionID
            currentPartitionID = partition
            currentOffsets[partition] = polled.currentOffset
            buffer = polled.messages
            bufferIndex = 1
            if configuration.pollingStrategy.kind != .next {
                nextOffsets[partition] = first.offset + 1
            }
            lastConsumedOffsets[partition] = first.offset
            let last = bufferIndex == buffer.count
            if last {
                buffer.removeAll()
                bufferIndex = 0
            }
            if shouldCommitOnHandover(offset: first.offset) || (last && configuration.autoCommit.when == .consumingAllMessages) {
                queueCommit(partition: partition, offset: first.offset)
            }
            return ReceivedMessage(message: first, currentOffset: polled.currentOffset, partitionID: partition)
        }
    }

    private func shouldCommitOnHandover(offset: UInt64) -> Bool {
        switch configuration.autoCommit.when {
        case .consumingEachMessage: true
        case .consumingEveryNthMessage(let nth): nth > 0 && offset % UInt64(nth) == 0
        case .pollingMessages, .consumingAllMessages, nil: false
        }
    }

    /// The commit after a handler returned, for ``IggyConsumer/consume(_:)``.
    func afterConsumed(_ received: ReceivedMessage) {
        let commit: Bool
        switch configuration.autoCommit.after {
        case .consumingEachMessage: commit = true
        case .consumingEveryNthMessage(let nth): commit = nth > 0 && received.message.offset % UInt64(nth) == 0
        case .consumingAllMessages: commit = bufferIndex >= buffer.count
        case nil: commit = false
        }
        if commit {
            queueCommit(partition: received.partitionID, offset: received.message.offset)
        }
    }

    private func poll() async throws -> PolledMessages {
        if let interval = configuration.pollInterval, interval > .zero, let lastPolledAt {
            let elapsed = ContinuousClock.now - lastPolledAt
            if elapsed < interval {
                try? await Task.sleep(for: interval - elapsed)
            }
        }
        while !canPoll {
            if isShutdown {
                return .empty
            }
            try? await Task.sleep(for: configuration.pollingRetryInterval)
        }
        if isGroup, configuration.autoJoinConsumerGroup, !joined {
            do {
                try await joinGroup()
            } catch {
                logger.error("Failed to join the consumer group", metadata: ["consumer": "\(name)", "error": "\(error)"])
                try? await Task.sleep(for: configuration.pollingRetryInterval)
                throw error
            }
        }
        lastPolledAt = .now
        let offsets = nextOffsets
        let strategy = configuration.pollingStrategy
        let polled: PolledMessages
        do {
            polled = try await backend.pollMessages(
                streamID: streamID, topicID: topicID, partitionID: partitionID, consumer: consumer, count: configuration.batchLength, autoCommit: commitsOnPoll
            ) { partition in
                offsets[partition].map { .offset($0) } ?? strategy
            }
        } catch {
            return try await handlePollFailure(ProducerCore.iggyError(error))
        }
        if polled.partitionID == PolledMessages.noAssignedPartition {
            try? await Task.sleep(for: configuration.pollingRetryInterval)
        }
        guard !polled.messages.isEmpty else {
            return polled
        }
        let partition = polled.partitionID
        var result = polled
        let consumed = lastConsumedOffsets[partition]
        if consumed == nil {
            lastConsumedOffsets[partition] = 0
        }
        if !configuration.allowReplay, let consumed {
            result.messages.removeAll { $0.offset <= consumed }
            result.count = UInt32(result.messages.count)
            if result.messages.isEmpty {
                return result
            }
        }
        // Under commit-on-poll the request stored the batch on the server, so
        // the local record moves to what was handed over before it.
        if commitsOnPoll {
            lastStoredOffsets[partition] = consumed ?? 0
        } else if lastStoredOffsets[partition] == nil {
            lastStoredOffsets[partition] = 0
        }
        return result
    }

    private func handlePollFailure(_ error: IggyError) async throws -> PolledMessages {
        logger.error("Failed to poll messages", metadata: ["consumer": "\(name)", "error": "\(error)"])
        if isGroup, configuration.autoJoinConsumerGroup, error.code == .consumerGroupMemberNotFound {
            logger.info("The consumer-group membership was revoked, rejoining on the next poll", metadata: ["consumer": "\(name)"])
            joined = false
            return .empty
        }
        if error.isConnectionLoss || error.code == .unauthenticated || error.code == .staleClient {
            // Polling resumes once the events report a sign-in.
            canPoll = false
            if isGroup {
                joined = false
            }
            throw error
        }
        try? await Task.sleep(for: configuration.pollingRetryInterval)
        throw error
    }

    private func joinGroup() async throws {
        if joined {
            return
        }
        let groupName = consumer.id.name ?? name
        let groupID = try Identifier(named: groupName)
        if try await backend.getConsumerGroup(streamID: streamID, topicID: topicID, groupID: groupID) == nil {
            guard configuration.createConsumerGroupIfNotExists else {
                throw IggyError(
                    .consumerGroupNameNotFound, context: "consumer group \(groupName) does not exist in topic \(topicID) and auto-creation is disabled")
            }
            logger.info("Creating the consumer group", metadata: ["group": "\(groupName)", "stream": "\(streamID)", "topic": "\(topicID)"])
            do {
                _ = try await backend.createConsumerGroup(streamID: streamID, topicID: topicID, name: groupName)
            } catch let error as IggyError where error.code == .consumerGroupNameAlreadyExists {
                // Another member won the race, which is fine.
            }
        }
        try await backend.joinConsumerGroup(streamID: streamID, topicID: topicID, groupID: groupID)
        joined = true
        logger.info("Joined the consumer group", metadata: ["group": "\(groupName)", "stream": "\(streamID)", "topic": "\(topicID)"])
    }

    // MARK: Offsets

    func lastConsumedOffset(partitionID: UInt32) -> UInt64? {
        lastConsumedOffsets[partitionID]
    }

    func lastStoredOffset(partitionID: UInt32) -> UInt64? {
        lastStoredOffsets[partitionID]
    }

    func storeOffset(_ offset: UInt64, partitionID: UInt32?) async throws {
        try await store(partition: partitionID ?? currentPartitionID, offset: offset, allowReplay: configuration.allowReplay)
    }

    func deleteOffset(partitionID: UInt32?) async throws {
        let partition = partitionID ?? (isGroup ? nil : currentPartitionID)
        try await backend.deleteConsumerOffset(consumer: consumer, streamID: streamID, topicID: topicID, partitionID: partition)
    }

    private func store(partition: UInt32, offset: UInt64, allowReplay: Bool) async throws {
        let stored = lastStoredOffsets[partition] ?? 0
        if lastStoredOffsets[partition] == nil {
            lastStoredOffsets[partition] = 0
        }
        // Offset zero is always sent: it is how a first poll gets recorded.
        if !allowReplay, offset <= stored, offset >= 1 {
            return
        }
        do {
            try await backend.storeConsumerOffset(consumer: consumer, streamID: streamID, topicID: topicID, partitionID: partition, offset: offset)
        } catch {
            logger.error(
                "Failed to store the consumer offset",
                metadata: ["consumer": "\(name)", "partition": "\(partition)", "offset": "\(offset)", "error": "\(error)"])
            throw error
        }
        lastStoredOffsets[partition] = offset
    }

    /// A later offset for the same partition replaces a queued one that has
    /// not been sent yet, so a burst of triggers costs one round trip.
    private func queueCommit(partition: UInt32, offset: UInt64) {
        guard isInitialized, !isShutdown else {
            logger.error(
                "The offset was not queued for storing: the consumer is not initialized or has been shut down",
                metadata: ["partition": "\(partition)", "offset": "\(offset)"])
            return
        }
        pendingCommits[partition] = offset
        Task { await storeNotify.notify() }
    }

    private func storeOffsetsPeriodically(every interval: Duration) async {
        while true {
            _ = await withTimeout(interval) { await self.intervalNotify.wait() }
            if isShutdown {
                return
            }
            for (partition, consumed) in lastConsumedOffsets {
                try? await store(partition: partition, offset: consumed, allowReplay: false)
            }
        }
    }

    private func storePendingCommits() async {
        while true {
            await storeNotify.wait()
            for partition in pendingCommits.keys.sorted() {
                guard let offset = pendingCommits.removeValue(forKey: partition) else {
                    continue
                }
                try? await store(partition: partition, offset: offset, allowReplay: false)
            }
            if isShutdown, pendingCommits.isEmpty {
                return
            }
        }
    }

    // MARK: Events

    /// Keeps the polling flags in step with the connection. Rejoining the
    /// group after a reconnect is left to the poll path.
    private func subscribeEvents() {
        eventsTask?.cancel()
        eventsTask = Task { [weak self, backend] in
            let events = await backend.events
            for await event in events {
                guard let self else { return }
                await self.handle(event)
                if event == .shutdown {
                    return
                }
            }
        }
    }

    private func handle(_ event: DiagnosticEvent) {
        switch event {
        case .shutdown:
            logger.warning("The client has been shut down, the consumer cannot poll anymore", metadata: ["consumer": "\(name)"])
            joined = false
            canPoll = false
        case .connected:
            // A fresh connection has not joined anything yet.
            joined = false
            if !isGroup {
                canPoll = true
            }
        case .disconnected, .signedOut:
            joined = false
            canPoll = false
        case .signedIn:
            canPoll = true
        }
    }
}
