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

/// Options of a ``PartitionReader``.
public struct ReaderConfiguration: Sendable {
    /// The default wait of a read, as in the Rust SDK.
    public static let defaultMaxWait: Duration = .seconds(5)

    /// The partitions a standalone consumer reads, each once. A group member
    /// reads the partitions the server assigns to it and leaves this nil.
    public var partitions: [UInt32]?
    /// Where a partition without a position starts. ``PollingStrategy/first``
    /// reads from offset zero.
    public var start: PollingStrategy
    /// The limits of each read and the commit policy. `count` caps a read of
    /// many partitions as a whole.
    public var options: PollOptions
    /// The pause after a failed read before its partitions are read again.
    public var retryInterval: Duration
    /// Joins the group when the reader is built, and again after the
    /// membership was lost.
    public var autoJoinConsumerGroup: Bool
    /// Creates a missing group before joining it. It needs a group named by
    /// name.
    public var createConsumerGroupIfNotExists: Bool

    public init(
        partitions: [UInt32]? = nil, start: PollingStrategy = .next, options: PollOptions = .wait(ReaderConfiguration.defaultMaxWait),
        retryInterval: Duration = .seconds(1), autoJoinConsumerGroup: Bool = true, createConsumerGroupIfNotExists: Bool = true
    ) {
        self.partitions = partitions
        self.start = start
        self.options = options
        self.retryInterval = retryInterval
        self.autoJoinConsumerGroup = autoJoinConsumerGroup
        self.createConsumerGroupIfNotExists = createConsumerGroupIfNotExists
    }

    func validate(group: Bool) throws {
        try options.validate()
        try options.deferred.validateFetch(count: options.count)
        guard retryInterval > .zero, retryInterval.wholeMicroseconds != nil else {
            throw IggyError(.invalidConfiguration, context: "retryInterval must be positive, got \(retryInterval)")
        }
        if group {
            guard partitions == nil else {
                throw IggyError(.invalidConfiguration, context: "a consumer group reads its assignment, not chosen partitions")
            }
        } else {
            guard let partitions, !partitions.isEmpty, partitions.count <= Requests.maxPartitionsPerRequest, Set(partitions).count == partitions.count
            else {
                throw IggyError(.invalidConfiguration, context: "a standalone reader names 1 to \(Requests.maxPartitionsPerRequest) partitions, each once")
            }
        }
    }
}

/// An error of a ``PartitionReader``. The reader stays usable after it and
/// reads on by itself.
public struct ReadError: Error, Sendable, CustomStringConvertible {
    /// The partition of the error, nil for an error of the whole reader.
    public let partitionID: UInt32?
    public let error: IggyError
    /// For a message that does not decode: the continuation after it. Its
    /// partition stops until a ``PartitionReader/seek(partitionID:to:)``,
    /// to this continuation to step over the message. The messages before it
    /// arrive in a batch before this error.
    public let skip: PollingStrategy?
    /// The offset of the message that does not decode.
    public let offset: UInt64?
    /// The record of the message that does not decode, as the server sent it.
    public let raw: [UInt8]?

    init(partitionID: UInt32?, error: IggyError, skip: PollingStrategy? = nil, offset: UInt64? = nil, raw: [UInt8]? = nil) {
        self.partitionID = partitionID
        self.error = error
        self.skip = skip
        self.offset = offset
        self.raw = raw
    }

    public var description: String {
        let partition = partitionID.map { "partition \($0): " } ?? ""
        let offset = offset.map { " at offset \($0)" } ?? ""
        return "\(partition)\(error)\(offset)"
    }
}

/// Reads the partitions of one topic in batches, a port of `PartitionReader`
/// in the Rust SDK. Build one with
/// ``IggyClient/reader(streamID:topicID:consumer:configuration:)``.
///
/// - A standalone consumer reads the partitions of its configuration. A
///   consumer group member reads its assignment: the reader joins the group,
///   joins again after a lost membership, and leaves it in ``close()``.
/// - It groups the partitions by their primary and reads each group with one
///   request, which the server holds until a message arrives or the wait of
///   ``PollOptions/maxWait`` ran out. A moved primary, a rebalance and a lost
///   connection are repaired inside.
/// - ``next()`` returns the next batch of one partition, never empty, in
///   offset order within one generation of the partition: from its first
///   read, from each ``seek(partitionID:to:)``, and from each new assignment
///   to this member.
/// - The ``CommitPolicy`` of the options decides who stores offsets. With
///   `manual`, ``commit(_:)`` stores a checkpoint. With `onRead`, the server
///   stores the offset of each read, the reader reads a group only once
///   ``next()`` waits with nothing queued, and ``close()`` moves the offset
///   back to the last delivered message. After a reply lost to a broken
///   connection, a read that the server stored but never delivered shows as
///   a gap before the next reply, and the reader reads that gap again
///   without a commit and delivers it first. A partition that leaves the
///   assignment still hands over the reads that the server stored for it.
///   With `auto`, the reader counts
///   each batch as its ``CommitAfter`` says, or each ``mark(_:)``, and stores
///   the latest counted position at the interval or the event of the policy,
///   before a rebalance takes a partition away, and in ``close()``.
/// - A failed read is thrown as a ``ReadError`` and its partitions are read
///   again after ``ReaderConfiguration/retryInterval``. With a zero
///   ``PollOptions/maxWait``, a partition that a read found empty waits it
///   too.
///   ``IggyErrorCode/historyUnavailable`` for a position read in a history
///   that is gone, and a message that does not decode, stop the partition
///   until a seek.
public actor PartitionReader: AsyncSequence {
    public typealias Element = PolledMessages

    /// The most requests of a reader in flight, as
    /// `MAX_DEFERRED_REQUESTS_PER_CONSUMER` in the Rust SDK, so a reader
    /// leaves deferred connections of its client to others.
    static let maxRequestsInFlight = 16
    /// How often a waiting group member checks the assignment that its
    /// client syncs, as `ASSIGNMENT_REFRESH_INTERVAL` in the Rust SDK.
    static let assignmentCheckInterval: Duration = .seconds(1)
    /// A reader without ``CommitPolicy/onRead`` fetches a group while fewer
    /// replies of the group wait for ``next()``, so the next fetch goes out
    /// before the reply before it is taken, and the queue stays bounded.
    static let waitingRepliesPerGroup = 2
    /// `[partition:4][current offset:8][count:4][context:24]` leads the reply
    /// of a poll.
    private static let pollReplyHeadSize = 40
    private static let lastReaderID = NIOLockedValueBox<UInt64>(0)

    public nonisolated let streamID: Identifier
    public nonisolated let topicID: Identifier
    public nonisolated let consumer: Consumer

    let id: UInt64
    private let client: IggyClient
    private let configuration: ReaderConfiguration
    private let logger: Logger
    private let isGroup: Bool
    /// One reader of batches at a time.
    private let reading = AsyncMutex()
    /// Held by a join and by the final leave, so a join in flight ends
    /// before the leave and none starts after it.
    private let membership = AsyncMutex()
    /// Held by every store and delete, so a coverage check, its request, and
    /// the record of it see no other store in between.
    private let offsetWrites = AsyncMutex()
    private let commitWakeups: AsyncStream<Void>
    private let commitWakeup: AsyncStream<Void>.Continuation

    private var isStarted = false
    private var isClosed = false
    private var closeTask: Task<Void, any Error>?
    /// Follows the client's events: reading waits while the client is signed
    /// out and can come back on its own.
    private var canRead = true
    private var joinedGroup = false
    private var joinedSessionScope: UInt64?
    private var lastAssignmentSync: ContinuousClock.Instant?
    private var eventsTask: Task<Void, Never>?
    private var intervalTask: Task<Void, Never>?
    private var commitTask: Task<Void, Never>?

    /// The partitions the reader reads grouped by primary, each group in the
    /// order of its next fetch.
    private var fetchGroups: [[UInt32]] = []
    private var assigned: Set<UInt32> = []
    /// Moves on when a primary or the assignment can have moved. Nothing is
    /// read until the partitions are grouped again.
    private var groupingEpoch: UInt64 = 1
    private var groupedEpoch: UInt64 = 0
    private var generations: [UInt32: UInt64] = [:]
    private var sought: [UInt32: UInt64] = [:]
    private var lastGeneration: UInt64 = 0
    private var requests: [UInt64: Request] = [:]
    /// The request that reads each partition.
    private var activeReads: [UInt32: UInt64] = [:]
    private var lastRequestID: UInt64 = 0
    /// What the requests brought back, in arrival order.
    private var arrivals: [Arrival] = []
    /// Where each partition reads next. Without one it starts as the
    /// configuration says.
    private var positions: [UInt32: PollingStrategy] = [:]
    /// With `onRead`: the offset the next reply of each partition starts at,
    /// unless a lost read stored messages before it.
    private var expected: [UInt32: UInt64] = [:]
    private var baselines: [UInt32: (next: UInt64, context: PartitionContext)] = [:]
    private var uncertain: Set<UInt32> = []
    /// With `onRead`: the messages each partition read and the server stored
    /// in its current generation, which the close moves back over.
    private var stored: [UInt32: Span] = [:]
    private var gaps: [UInt32: Gap] = [:]
    private var isolations: [UInt32: Span] = [:]
    /// Partitions read with a poll of their own next: a fetch section that
    /// the server refused with `invalidSizeBytes`.
    private var pollAlone: Set<UInt32> = []
    private var pausedUntil: [UInt32: ContinuousClock.Instant] = [:]
    /// Partitions that read nothing until a seek.
    private var stopped: Set<UInt32> = []
    private var pollCursor: UInt32?
    private var readerWakeup: AsyncStream<Void>.Continuation?
    private var storesWaiting: [UInt32: Int] = [:]
    private var storeWakeups: [AsyncStream<Void>.Continuation] = []

    private var delivered: [UInt32: Checkpoint] = [:]
    private var committed: [UInt32: Checkpoint] = [:]
    private var counted: [UInt32: Counted] = [:]
    /// Counted positions of partitions a rebalance took away, stored first by
    /// the next flush.
    private var retiring: [Checkpoint] = []
    /// With ``CommitAfter/processing``: the last batch ``next()`` returned,
    /// which counts once the application asks for the next one.
    private var uncounted: (checkpoint: Checkpoint, messages: UInt32)?

    private struct Request {
        let kind: RequestKind
        let partitions: [UInt32]
        /// Whether the request reads or moves the stored offsets of its
        /// partitions: it commits, or one of them starts after its stored
        /// offset.
        let progresses: Bool
        let commits: Bool
        let generations: [UInt32: UInt64]
        let task: Task<Void, Never>
    }

    private enum RequestKind {
        /// The fetch of a readiness group.
        case main
        /// The fetch of the partitions of a group that turned idle while
        /// its main fetch waited.
        case second
        /// A poll of one partition.
        case single
        /// A poll that reads a gap again, without a commit.
        case gap
        /// A poll of one message of a section that did not decode.
        case isolate
    }

    private enum Reply: Sendable {
        case fetched([FetchSection])
        case polled([UInt8])
    }

    private enum Arrival {
        case section(FetchSection, generation: UInt64, request: UInt64)
        case batch(PolledMessages, generation: UInt64)
        case failure(ReadError)

        var partitionID: UInt32? {
            switch self {
            case .section(let section, _, _): section.partitionID
            case .batch(let batch, _): batch.partitionID
            case .failure(let error): error.partitionID
            }
        }

        var isFailure: Bool {
            if case .failure = self { true } else { false }
        }
    }

    /// Offsets `first` up to `last`, or `first` up to before `end`, of one
    /// partition history.
    private struct Span {
        var first: UInt64
        var last: UInt64
        var context: PartitionContext
    }

    /// A range that a lost read stored and never delivered, read again from
    /// `next` up to before `end`, then the replies held behind it.
    private struct Gap {
        var next: UInt64
        let end: UInt64
        let context: PartitionContext
        var held: [Arrival]
    }

    private struct Counted {
        var checkpoint: Checkpoint
        var sinceStore: UInt32
    }

    init(client: IggyClient, streamID: Identifier, topicID: Identifier, consumer: Consumer, configuration: ReaderConfiguration) throws {
        try configuration.validate(group: consumer.kind == .consumerGroup)
        id = Self.lastReaderID.withLockedValue { last in
            last += 1
            return last
        }
        self.client = client
        self.streamID = streamID
        self.topicID = topicID
        self.consumer = consumer
        var configuration = configuration
        if configuration.start.kind == .first {
            configuration.start = .offset(0)
        }
        self.configuration = configuration
        logger = client.logger
        isGroup = consumer.kind == .consumerGroup
        (commitWakeups, commitWakeup) = AsyncStream<Void>.makeStream(bufferingPolicy: .bufferingNewest(1))
    }

    deinit {
        eventsTask?.cancel()
        intervalTask?.cancel()
        commitTask?.cancel()
        commitWakeup.finish()
        for request in requests.values where !request.commits {
            request.task.cancel()
        }
    }

    public nonisolated func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(reader: self)
    }

    public struct AsyncIterator: AsyncIteratorProtocol {
        /// Nil once an error or the close ended the iteration. The reader
        /// itself can be read on after an error.
        var reader: PartitionReader?

        public mutating func next() async throws -> PolledMessages? {
            guard let reader else {
                return nil
            }
            do {
                let batch = try await reader.next()
                if batch == nil {
                    self.reader = nil
                }
                return batch
            } catch {
                self.reader = nil
                throw error
            }
        }
    }

    // MARK: Lifecycle

    /// Joins the group of a member that joins on its own and starts the
    /// tasks of the reader.
    func start() async throws {
        guard !isStarted else {
            return
        }
        let events = await client.events
        eventsTask = Task { [weak self] in
            for await event in events {
                guard let self, await self.observe(event) else {
                    return
                }
            }
        }
        if isGroup, configuration.autoJoinConsumerGroup {
            try await joinGroup()
        }
        if !isGroup {
            reassign(Set(configuration.partitions ?? []))
        }
        if let auto = configuration.options.autoCommit {
            if let interval = auto.interval {
                intervalTask = Task { [weak self] in
                    while (try? await Task.sleep(for: interval)) != nil {
                        guard let self, await self.flushOnTick() else {
                            return
                        }
                    }
                }
            }
            commitTask = Task { [weak self, commitWakeups] in
                for await _ in commitWakeups {
                    guard let self, await self.flushOnTick() else {
                        return
                    }
                }
            }
        }
        isStarted = true
    }

    /// Stops the reader. It waits for the committing reads in flight, at most
    /// their request timeout. With `manual` it stores nothing. With `auto` it
    /// counts the last batch under ``CommitAfter/processing`` and stores the
    /// latest counted position of each partition. With `onRead` it moves the
    /// offset of each partition back to its last delivered message, and a
    /// partition that delivered nothing and started at `next` gets the offset
    /// before the first message it read, or none at offset zero. A group
    /// member then leaves its group. Every caller waits for the same close.
    ///
    /// Throws the first failed store.
    public func close() async throws {
        if let closeTask {
            return try await closeTask.value
        }
        isClosed = true
        let task = Task { try await self.drain() }
        closeTask = task
        try await task.value
    }

    private func drain() async throws {
        for (id, request) in requests where !request.commits {
            cancel(id)
        }
        readerWakeup?.yield()
        for task in requests.values.map(\.task) {
            await task.value
        }
        _ = try? await withDeadline(.now + configuration.options.requestTimeout) { [reading] in
            try await reading.lock()
            reading.unlock()
        }
        countUncounted()
        intervalTask?.cancel()
        commitTask?.cancel()
        commitWakeup.finish()
        var failure: (any Error)?
        if configuration.options.commitsOnRead {
            for partition in uncertain {
                guard let baseline = baselines[partition] else {
                    continue
                }
                do {
                    let current = try await client.consumerOffsetNext(
                        consumer: consumer, streamID: streamID, topicID: topicID, partitionID: partition, context: baseline.context)
                    if current > baseline.next {
                        recordStored(partition, through: current, from: baseline.next, context: baseline.context)
                    }
                } catch {
                    failure = failure ?? error
                }
            }
            for (partition, span) in stored {
                do {
                    try await rewind(partition, over: span)
                } catch {
                    failure = failure ?? error
                }
            }
        }
        if configuration.options.autoCommit != nil {
            do {
                try await flushCounted()
            } catch {
                failure = failure ?? error
            }
        }
        // The server refuses a store from a member that left, so the stores go
        // first.
        if isGroup, (try? await membership.lock()) != nil {
            defer { membership.unlock() }
            if joinedGroup {
                joinedGroup = false
                do {
                    try await client.leaveConsumerGroup(streamID: streamID, topicID: topicID, groupID: consumer.id)
                } catch {
                    logger.debug("Failed to leave the consumer group", metadata: metadata.merging(["error": "\(error)"]) { $1 })
                }
            }
        }
        eventsTask?.cancel()
        eventsTask = nil
        if let failure {
            throw failure
        }
    }

    // MARK: Reading

    /// The next batch of one partition, never empty, or nil once the reader
    /// is closed. Throws a ``ReadError``, after which the reader can be read
    /// on.
    public func next() async throws -> PolledMessages? {
        countUncounted()
        guard let batch = try await nextBatch() else {
            return nil
        }
        if let checkpoint = batch.checkpoint {
            deliver(checkpoint)
            switch configuration.options.autoCommit?.after {
            case .delivery?:
                count(checkpoint, messages: UInt32(batch.messages.count), endsBatch: true)
            case .processing?:
                uncounted = (checkpoint, UInt32(batch.messages.count))
            case .marked?, nil:
                break
            }
        }
        return batch
    }

    /// The next batch without recording its delivery, for a caller that
    /// records each message it hands over with ``deliver(_:)``.
    func nextBatch() async throws -> PolledMessages? {
        try await reading.lock()
        defer { reading.unlock() }
        while !isClosed {
            if let batch = try takeArrival() {
                return batch
            }
            try Task.checkCancellation()
            try await waitForSignIn()
            guard await prepare(), !isClosed, !needsRegroup else {
                continue
            }
            await waitForArrival()
        }
        return nil
    }

    /// Records the last message the application took from its partition.
    /// With `onRead` the close moves the offset back to it.
    func deliver(_ checkpoint: Checkpoint) {
        guard isCurrent(checkpoint) else {
            return
        }
        delivered[checkpoint.partitionID] = checkpoint
    }

    /// Stores the offset of a checkpoint that this reader delivered, now.
    /// A checkpoint of an older generation of its partition, or at or below
    /// the last commit of its generation, stores nothing.
    ///
    /// Throws ``IggyErrorCode/invalidCommand`` with ``CommitPolicy/onRead``
    /// and for a checkpoint that this reader did not deliver, and the errors
    /// of the store.
    public func commit(_ checkpoint: Checkpoint) async throws {
        guard !configuration.options.commitsOnRead else {
            throw IggyError(.invalidCommand, context: "the server stores the offsets of an onRead reader")
        }
        guard checkpoint.stamp?.reader == id else {
            throw IggyError(.invalidCommand, context: "checkpoint \(checkpoint) was not delivered by this reader")
        }
        guard isCurrent(checkpoint) else {
            return
        }
        try await store(checkpoint)
    }

    /// Counts a checkpoint that this reader delivered, under
    /// ``CommitPolicy/auto(_:)``. It stores nothing now: the policy stores
    /// the latest counted position of the partition. With
    /// ``CommitAfter/marked`` only marks count.
    ///
    /// Throws ``IggyErrorCode/invalidCommand`` without `auto` and for a
    /// checkpoint that this reader did not deliver.
    public func mark(_ checkpoint: Checkpoint) throws {
        guard configuration.options.autoCommit != nil, checkpoint.stamp?.reader == id else {
            throw IggyError(.invalidCommand, context: "only an auto commit reader marks the checkpoints it delivered")
        }
        count(checkpoint, messages: 1, endsBatch: false)
    }

    /// Sets the next read position of `partitionID` and starts a new
    /// generation of it. It drops the queued batches of the partition,
    /// ignores its late replies, and resumes a stopped partition. Without
    /// `onRead` it stores no offset. With `onRead` an offset start stores
    /// the offset before it first, so the reads go on after it.
    ///
    /// Throws ``IggyErrorCode/invalidCommand`` for a partition the reader
    /// does not read and for a continuation of another partition.
    public func seek(partitionID: UInt32, to start: PollingStrategy) async throws {
        try start.validate(partitionID: partitionID)
        guard assigned.contains(partitionID) else {
            throw IggyError(.invalidCommand, context: "the reader does not read partition \(partitionID)")
        }
        var start = start
        if start.kind == .first {
            start = PollingStrategy(kind: .offset, value: 0, context: start.context, partitionID: start.partitionID)
        }
        newGeneration(of: partitionID, seeking: true)
        if configuration.options.commitsOnRead, start.kind == .offset {
            let context = start.context
            try await afterProgress(of: partitionID) {
                try await offsetWrites.lock()
                defer { offsetWrites.unlock() }
                if start.value == 0 {
                    try await client.deleteConsumerOffset(
                        consumer: consumer, streamID: streamID, topicID: topicID, partitionID: partitionID, context: context)
                } else {
                    try await client.storeConsumerPosition(
                        consumer: consumer, streamID: streamID, topicID: topicID,
                        checkpoint: Checkpoint(partitionID: partitionID, offset: start.value - 1, context: context ?? .empty))
                }
            }
            positions[partitionID] = .next
            expected[partitionID] = start.value
        } else {
            positions[partitionID] = start
        }
        readerWakeup?.yield()
    }

    /// The partitions the reader reads: those of a standalone consumer, or
    /// the current assignment of a group member, which the first read
    /// learns.
    public var partitions: [UInt32] {
        assigned.sorted()
    }

    // MARK: Internals

    private var metadata: Logger.Metadata {
        ["stream": "\(streamID)", "topic": "\(topicID)", "consumer": "\(consumer)"]
    }

    private var groupKey: ConsumerGroupState.GroupKey {
        ConsumerGroupState.GroupKey(stream: streamID, topic: topicID, group: consumer.id)
    }

    private var needsRegroup: Bool {
        groupedEpoch != groupingEpoch
    }

    /// The server hands a member its partitions as its shards learn of the
    /// join, so the assignment a sync holds can be part of it. It is read
    /// again at the check interval, as the Rust reader refreshes it.
    private var assignmentSyncDue: Bool {
        lastAssignmentSync.map { ContinuousClock.now - $0 >= Self.assignmentCheckInterval } ?? true
    }

    private func joinGroup() async throws {
        try await membership.lock()
        defer { membership.unlock() }
        if isClosed || (joinedGroup && joinedSessionScope == client.core.groupState.sessionScope) {
            return
        }
        if try await client.getConsumerGroup(streamID: streamID, topicID: topicID, groupID: consumer.id) == nil {
            guard configuration.createConsumerGroupIfNotExists, let name = consumer.id.name else {
                throw IggyError(.consumerGroupNameNotFound, context: "consumer group \(consumer.id) does not exist and is not created")
            }
            logger.info("Creating the consumer group", metadata: metadata)
            do {
                _ = try await client.createConsumerGroup(streamID: streamID, topicID: topicID, name: name)
            } catch let error as IggyError where error.code == .consumerGroupNameAlreadyExists {
                // Another member created it first.
            }
        }
        try await client.joinConsumerGroup(streamID: streamID, topicID: topicID, groupID: consumer.id)
        joinedGroup = true
        joinedSessionScope = client.core.groupState.sessionScope
    }

    /// Returns false once the client shut down, which ends the subscription.
    private func observe(_ event: DiagnosticEvent) async -> Bool {
        switch event {
        case .shutdown:
            joinedGroup = false
            canRead = false
            readerWakeup?.yield()
            return false
        case .signedOut:
            joinedGroup = false
        case .connected, .disconnected, .signedIn:
            if joinedSessionScope != client.core.groupState.sessionScope {
                joinedGroup = false
            }
        }
        canRead = await client.state == .authenticated
        return true
    }

    /// A client that only the caller brings back never recovers, so reading
    /// goes on and fails with the reason instead of waiting for good.
    private func waitForSignIn() async throws {
        while !canRead, !isClosed, await client.canRecover {
            if (try? await client.requireAuthenticated()) != nil {
                break
            }
            try await Task.sleep(for: configuration.retryInterval)
        }
    }

    /// Joins again once the membership was lost, syncs an assignment the
    /// client does not hold, and groups the partitions by primary. Returns
    /// false when nothing can be read yet, after the retry interval. A
    /// failure is queued for ``next()``.
    private func prepare() async -> Bool {
        do {
            if isGroup {
                if configuration.autoJoinConsumerGroup, !joinedGroup || joinedSessionScope != client.core.groupState.sessionScope {
                    try await joinGroup()
                }
                if isClosed {
                    return false
                }
                let state = client.core.groupState
                if !state.hasAssignment(groupKey) || assignmentSyncDue {
                    try await client.syncGroupAssignment(streamID: streamID, topicID: topicID, groupID: consumer.id, lineage: nil)
                    lastAssignmentSync = .now
                }
                guard state.isRegistered(groupKey) else {
                    throw IggyError(.consumerGroupMemberNotFound, context: "group \(consumer.id) in topic \(topicID) of stream \(streamID)")
                }
                let current = Set(state.assignedPartitions(groupKey) ?? [])
                if current != assigned {
                    reassign(current)
                }
            }
            if assigned.isEmpty {
                try await Task.sleep(for: configuration.retryInterval)
                return false
            }
            if needsRegroup {
                let epoch = groupingEpoch
                let kept = fetchGroups.joined().filter(assigned.contains)
                let order = kept + assigned.subtracting(kept).sorted()
                fetchGroups = try await client.readinessGroups(streamID: streamID, topicID: topicID, partitions: order)
                groupedEpoch = epoch
            }
            if configuration.options.commitsOnRead {
                for partition in assigned.sorted() where needsBaseline(partition) && activeReads[partition] == nil {
                    let generation = generations[partition]
                    var options = configuration.options.deferred
                    options.maxWait = .zero
                    options.minCount = 1
                    let sections = try await client.fetchMessages(
                        streamID: streamID, topicID: topicID, targets: [(partitionID: partition, strategy: .next)], consumer: consumer,
                        count: 1, autoCommit: false, options: options)
                    guard let section = sections.first(where: { $0.partitionID == partition }) else {
                        throw IggyError(.cannotReadMessage, context: "the baseline read returned no section for partition \(partition)")
                    }
                    if let error = section.error {
                        throw error
                    }
                    let baseline = (next: section.firstOffset ?? section.nextOffset, context: section.context)
                    guard generations[partition] == generation, !isClosed else {
                        continue
                    }
                    baselines[partition] = baseline
                    expected[partition] = baseline.next
                    positions[partition] = .continuation(partitionID: partition, kind: .next, value: 0, context: baseline.context)
                }
            }
            return true
        } catch is CancellationError {
            return false
        } catch {
            let code = (error as? IggyError)?.code
            if let code, [.consumerGroupMemberNotFound, .disconnected, .unauthenticated, .staleClient].contains(code) {
                joinedGroup = false
            }
            logger.error("Failed to join the consumer group or to group its partitions", metadata: metadata.merging(["error": "\(error)"]) { $1 })
            try? await Task.sleep(for: configuration.retryInterval)
            if code == .consumerGroupMemberNotFound, configuration.autoJoinConsumerGroup {
                return false
            }
            if let code, [.transientNotAccepted, .transientNotCommitted, .deferredPollCapacityExceeded, .notPrimary].contains(code) {
                return false
            }
            report(ReadError(partitionID: nil, error: Self.iggyError(error)))
            return false
        }
    }

    /// Moves to a new set of partitions. The reads that commit nothing are
    /// cancelled. A read that commits ends with its reply, whose batches stay
    /// queued, as the server stored them. Counted positions of the partitions
    /// that leave are stored first.
    private func reassign(_ partitions: Set<UInt32>) {
        for (id, request) in requests where !request.commits {
            cancel(id)
        }
        let leaving = assigned.subtracting(partitions)
        if !leaving.isEmpty {
            let retired = leaving.compactMap { counted.removeValue(forKey: $0)?.checkpoint }.filter(isCurrent)
            if !retired.isEmpty {
                retiring.append(contentsOf: retired)
                commitWakeup.yield()
            }
            if !configuration.options.commitsOnRead {
                arrivals.removeAll { arrival in arrival.partitionID.map(leaving.contains) ?? false }
            }
        }
        for partition in leaving {
            if configuration.options.commitsOnRead, let gap = gaps[partition] {
                giveUp(gap, of: partition)
            }
            forget(partition)
            generations[partition] = nil
            stored[partition] = nil
            delivered[partition] = nil
        }
        for partition in partitions.subtracting(assigned) {
            newGeneration(of: partition)
        }
        assigned = partitions
        groupingEpoch += 1
    }

    /// Starts a generation of `partition` that reads from its start.
    private func newGeneration(of partition: UInt32, seeking: Bool = false) {
        lastGeneration += 1
        generations[partition] = lastGeneration
        if seeking {
            sought[partition] = lastGeneration
        }
        if seeking || !configuration.options.commitsOnRead {
            arrivals.removeAll { $0.partitionID == partition && !$0.isFailure }
        }
        if let id = activeReads[partition], requests[id]?.commits == false {
            cancel(id)
        }
        forget(partition)
        stored[partition] = nil
        delivered[partition] = nil
        if uncounted?.checkpoint.partitionID == partition {
            uncounted = nil
        }
    }

    private func forget(_ partition: UInt32) {
        positions[partition] = nil
        expected[partition] = nil
        baselines[partition] = nil
        uncertain.remove(partition)
        gaps[partition] = nil
        isolations[partition] = nil
        pollAlone.remove(partition)
        pausedUntil[partition] = nil
        stopped.remove(partition)
    }

    /// Whether a read of `partition` under `generation` still counts. A seek
    /// or a new assignment of the partition drops it. A partition that left
    /// the assignment keeps the reads that the server stored under `onRead`.
    private func keeps(_ partition: UInt32, _ generation: UInt64) -> Bool {
        if generations[partition] == generation {
            return true
        }
        return configuration.options.commitsOnRead && generation >= sought[partition, default: 0]
    }

    private func isCurrent(_ checkpoint: Checkpoint) -> Bool {
        guard let stamp = checkpoint.stamp else {
            return assigned.contains(checkpoint.partitionID)
        }
        return stamp.reader == id && generations[checkpoint.partitionID] == stamp.generation
    }

    /// Starts what the reader can read, then waits until a request ends, the
    /// close begins, a pause ends, or the assignment is due for a check.
    private func waitForArrival() async {
        let (wakeups, wakeup) = AsyncStream<Void>.makeStream(bufferingPolicy: .bufferingNewest(1))
        readerWakeup = wakeup
        defer { readerWakeup = nil }
        dispatch()
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

    /// The next batch that a request brought back, or the error queued next.
    /// A batch of an older generation of its partition is dropped. A section
    /// is decoded only now, after the next fetch of its group went out. A
    /// section that does not decode is read again one message at a time.
    private func takeArrival() throws -> PolledMessages? {
        while !arrivals.isEmpty {
            let arrival = arrivals.removeFirst()
            defer { dispatch() }
            var batch: PolledMessages
            let generation: UInt64
            switch arrival {
            case .failure(let error):
                throw error
            case .section(let section, let sectionGeneration, _):
                generation = sectionGeneration
                guard keeps(section.partitionID, generation) else {
                    continue
                }
                do {
                    batch = try section.decode()
                } catch {
                    isolate(section)
                    continue
                }
            case .batch(let polled, let batchGeneration):
                generation = batchGeneration
                guard keeps(polled.partitionID, generation) else {
                    continue
                }
                batch = polled
            }
            guard !batch.messages.isEmpty else {
                continue
            }
            batch.stamp = ReaderStamp(reader: id, generation: generation)
            return batch
        }
        return nil
    }

    /// Reads a section that does not decode again, one message at a time
    /// from its first message, so the messages before the bad one are
    /// delivered and the error names the bad one.
    private func isolate(_ section: FetchSection) {
        let partition = section.partitionID
        guard let first = section.firstOffset else {
            return
        }
        arrivals.removeAll { $0.partitionID == partition && !$0.isFailure }
        if let id = activeReads[partition], requests[id]?.commits == false {
            cancel(id)
        }
        isolations[partition] = Span(first: first, last: section.nextOffset - 1, context: section.context)
    }

    // MARK: Requests

    /// Starts what the reader can read now.
    private func dispatch() {
        guard !isClosed, !needsRegroup else {
            return
        }
        let now = ContinuousClock.now
        pausedUntil = pausedUntil.filter { $0.value > now }
        if !configuration.options.commitsOnRead {
            for group in fetchGroups {
                fetchAhead(group)
            }
        } else if onDemand {
            for group in fetchGroups {
                takeCommittingTurn(of: group)
            }
        }
        pollSingles()
    }

    /// With `onRead` a read goes out only for a reader that waits with
    /// nothing queued, so the server stores a batch only once it was asked
    /// for.
    private var onDemand: Bool {
        readerWakeup != nil && arrivals.isEmpty
    }

    /// Fetches the partitions of a group that no request reads. They join
    /// the main fetch of the group once the last one ended, else its second
    /// fetch, which starts again with them.
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
            case .single, .gap, .isolate: break
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

    /// One committing fetch of the partitions of a group that the fetch can
    /// read, unless the group is read already or a store of it waits.
    private func takeCommittingTurn(of group: [UInt32]) {
        let turnTaken = group.contains { partition in
            storesWaiting[partition] != nil || activeReads[partition].flatMap { requests[$0] }.map { $0.commits } ?? false
        }
        guard !turnTaken, requests.count < Self.maxRequestsInFlight else {
            return
        }
        let targets = group.filter { isIdle($0) && isFetchable($0) }
        if !targets.isEmpty {
            startFetch(targets, kind: .main)
        }
    }

    /// Polls, one partition each, the gaps, the sections that did not
    /// decode, and the partitions a fetch cannot read, taking turns when the
    /// request limit binds. A request per group stays free of them.
    private func pollSingles() {
        let partitions = Array(fetchGroups.joined())
        let limit = Swift.max(Self.maxRequestsInFlight - fetchGroups.count, 1)
        var polls = requests.values.filter { $0.kind != .main && $0.kind != .second }.count
        let start = pollCursor.flatMap(partitions.firstIndex(of:)).map { $0 + 1 } ?? 0
        for index in partitions.indices {
            guard polls < limit, requests.count < Self.maxRequestsInFlight else {
                return
            }
            let partition = partitions[(start + index) % partitions.count]
            guard isIdle(partition), let (kind, strategy, count) = singleRead(of: partition) else {
                continue
            }
            if startPoll(partition, kind: kind, strategy: strategy, count: count) {
                pollCursor = partition
                polls += 1
            }
        }
    }

    /// The poll a partition needs, if a fetch cannot read it.
    private func singleRead(of partition: UInt32) -> (RequestKind, PollingStrategy, UInt32)? {
        if let gap = gaps[partition] {
            let count = UInt32(clamping: Swift.min(gap.end - gap.next, UInt64(configuration.options.count)))
            return (.gap, .continuation(partitionID: partition, kind: .offset, value: gap.next, context: gap.context), count)
        }
        if let isolation = isolations[partition] {
            return (.isolate, .continuation(partitionID: partition, kind: .offset, value: isolation.first, context: isolation.context), 1)
        }
        guard !isFetchable(partition), !configuration.options.commitsOnRead || onDemand else {
            return nil
        }
        return (.single, startPoint(of: partition), configuration.options.count)
    }

    private func isIdle(_ partition: UInt32) -> Bool {
        assigned.contains(partition) && activeReads[partition] == nil && pausedUntil[partition] == nil && !stopped.contains(partition)
            && !needsBaseline(partition)
    }

    private func needsBaseline(_ partition: UInt32) -> Bool {
        configuration.options.commitsOnRead && baselines[partition] == nil && startPoint(of: partition).kind == .next
    }

    /// A fetch reads a partition that starts at an offset or after its
    /// stored offset, outside a gap and a section that did not decode.
    private func isFetchable(_ partition: UInt32) -> Bool {
        !pollAlone.contains(partition) && gaps[partition] == nil && isolations[partition] == nil && IggyClient.fetchTakes(startPoint(of: partition))
    }

    /// Where the next read of a partition starts. With `onRead` a partition
    /// that was read goes on after its stored offset.
    private func startPoint(of partition: UInt32) -> PollingStrategy {
        positions[partition] ?? configuration.start
    }

    private func waitingReplies(of group: [UInt32]) -> Int {
        let members = Set(group)
        var replies: Set<UInt64> = []
        for case .section(let section, _, let request) in arrivals where members.contains(section.partitionID) {
            replies.insert(request)
        }
        return replies.count
    }

    private func startFetch(_ partitions: [UInt32], kind: RequestKind) {
        let targets = partitions.map { (partitionID: $0, strategy: startPoint(of: $0)) }
        let commits = configuration.options.commitsOnRead
        let progresses = commits || targets.contains { $0.strategy.kind == .next }
        let options = configuration.options
        start(kind, partitions: partitions, progresses: progresses, commits: commits) { [client, streamID, topicID, consumer] in
            .fetched(
                try await client.fetchMessages(
                    streamID: streamID, topicID: topicID, targets: targets, consumer: consumer, count: options.count, autoCommit: commits,
                    options: options.deferred))
        }
    }

    /// Starts a poll of one partition, unless it would read or move the
    /// stored offset while a store of the partition waits. A gap and a
    /// section that did not decode are read again without a wait and without
    /// a commit.
    @discardableResult
    private func startPoll(_ partition: UInt32, kind: RequestKind, strategy: PollingStrategy, count: UInt32) -> Bool {
        let commits = kind == .single && configuration.options.commitsOnRead
        let progresses = commits || strategy.kind == .next
        if progresses, storesWaiting[partition] != nil {
            return false
        }
        var options = configuration.options.deferred
        if kind != .single {
            options.maxWait = .zero
        }
        options.minCount = 1
        start(kind, partitions: [partition], progresses: progresses, commits: commits) { [client, streamID, topicID, consumer, options] in
            .polled(
                try await client.pollBody(
                    streamID: streamID, topicID: topicID, partitionID: partition, consumer: consumer, strategy: strategy, count: count,
                    autoCommit: commits, options: options))
        }
        return true
    }

    private func start(
        _ kind: RequestKind, partitions: [UInt32], progresses: Bool, commits: Bool, _ read: @escaping @Sendable () async throws -> Reply
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
        let requestGenerations = Dictionary(uniqueKeysWithValues: partitions.map { ($0, generations[$0] ?? 0) })
        requests[id] = Request(kind: kind, partitions: partitions, progresses: progresses, commits: commits, generations: requestGenerations, task: task)
        for partition in partitions {
            activeReads[partition] = id
        }
    }

    /// Forgets a request, so its partitions can be read again, and wakes the
    /// stores that wait for it.
    @discardableResult
    private func finish(_ id: UInt64) -> Request? {
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

    /// Cancels a request that commits nothing: its reply is dropped.
    private func cancel(_ id: UInt64) {
        finish(id)?.task.cancel()
    }

    private func requestEnded(_ id: UInt64, _ outcome: Result<Reply, any Error>) {
        guard let request = finish(id) else {
            return
        }
        switch outcome {
        case .success(.fetched(let sections)):
            received(sections, from: id, of: request)
        case .success(.polled(let body)):
            received(body, of: request)
        case .failure(let error):
            failed(request, with: error)
        }
        dispatch()
        readerWakeup?.yield()
    }

    /// Moves each partition the fetch read to its next position and queues
    /// the sections that hold messages, still undecoded. The partitions that
    /// returned messages go last in the next fetch of their group, so each
    /// one gets its turn.
    private func received(_ sections: [FetchSection], from id: UInt64, of request: Request) {
        var unanswered = Set(request.partitions)
        var served: Set<UInt32> = []
        var idle: Set<UInt32> = []
        for section in sections {
            let partition = section.partitionID
            guard unanswered.remove(partition) != nil, let generation = request.generations[partition] else {
                continue
            }
            guard generations[partition] == generation else {
                if request.commits, keeps(partition, generation), section.error == nil, section.messagesCount > 0 {
                    // The server stored this read, so a partition that left the assignment still hands it over.
                    arrivals.append(.section(section, generation: generation, request: id))
                }
                continue
            }
            if let error = section.error {
                if request.commits, error.code == .shardCommunicationError {
                    uncertain.insert(partition)
                    failed([partition], with: IggyError(.transientNotCommitted, context: "\(error)", partitionID: partition))
                    continue
                }
                if error.code == .invalidSizeBytes {
                    // The server takes one record past `maxBytes` per fetch reply, so
                    // this partition reads alone next.
                    pollAlone.insert(partition)
                    continue
                }
                failed([partition], with: error)
                continue
            }
            if section.messagesCount == 0 {
                idle.insert(partition)
            }
            let arrival = Arrival.section(section, generation: generation, request: id)
            if !accept(arrival, of: partition, reachingThrough: section.nextOffset, context: section.context, firstOffset: section.firstOffset) {
                continue
            }
            if section.messagesCount > 0 {
                served.insert(partition)
            }
        }
        pauseIdle(idle.union(unanswered.filter { request.generations[$0] == generations[$0] }))
        if !served.isEmpty {
            fetchGroups = fetchGroups.map { group in group.filter { !served.contains($0) } + group.filter(served.contains) }
        }
    }

    /// A read that does not wait answers an idle partition at once, so the
    /// partition reads again only after the retry interval.
    private func pauseIdle(_ partitions: Set<UInt32>) {
        guard configuration.options.maxWait == .zero, !partitions.isEmpty else {
            return
        }
        let until = ContinuousClock.now + configuration.retryInterval
        for partition in partitions {
            pausedUntil[partition] = until
        }
    }

    /// Records a read of a partition that ends before `end`, and queues it.
    /// With `onRead` a read that starts past the offset the partition
    /// expected found a gap that a lost read stored: the reply waits until
    /// the gap was read again. Returns whether the arrival was queued.
    private func accept(
        _ arrival: Arrival, of partition: UInt32, reachingThrough end: UInt64, context: PartitionContext, firstOffset: UInt64?
    ) -> Bool {
        let onRead = configuration.options.commitsOnRead
        let start = firstOffset ?? end
        if onRead, let expected = expected[partition], start > expected, context.incarnation != 0 {
            logger.warning(
                "A lost read stored messages that were never delivered, reading them again",
                metadata: metadata.merging(["partition": "\(partition)", "from": "\(expected)", "to": "\(start)"]) { $1 })
            gaps[partition] = Gap(next: expected, end: start, context: context, held: firstOffset == nil ? [] : [arrival])
            recordStored(partition, through: end, from: expected, context: context)
            self.expected[partition] = end
            positions[partition] = .next
            return false
        }
        if onRead {
            if let firstOffset {
                recordStored(partition, through: end, from: firstOffset, context: context)
            }
            if firstOffset != nil || expected[partition] != nil {
                expected[partition] = end
            }
            positions[partition] = firstOffset == nil && positions[partition] == nil ? nil : .next
        } else if firstOffset != nil {
            positions[partition] = .continuation(partitionID: partition, kind: .offset, value: end, context: context)
        }
        guard firstOffset != nil else {
            return false
        }
        arrivals.append(arrival)
        return true
    }

    private func recordStored(_ partition: UInt32, through end: UInt64, from first: UInt64, context: PartitionContext) {
        guard end > 0 else {
            return
        }
        if var span = stored[partition], Self.sameHistory(span.context, context) {
            span.last = Swift.max(span.last, end - 1)
            stored[partition] = span
        } else {
            stored[partition] = Span(first: first, last: end - 1, context: context)
        }
    }

    /// A poll of one partition: a read that a fetch cannot take, a gap read
    /// again, or one message of a section that did not decode.
    private func received(_ body: [UInt8], of request: Request) {
        guard let partition = request.partitions.first, let generation = request.generations[partition] else {
            return
        }
        guard generations[partition] == generation else {
            if request.commits, keeps(partition, generation), let polled = try? Responses.polledMessages(body[...]), !polled.messages.isEmpty {
                // The server stored this read, so a partition that left the assignment still hands it over.
                arrivals.append(.batch(polled, generation: generation))
            }
            return
        }
        let polled: PolledMessages
        do {
            polled = try Responses.polledMessages(body[...])
        } catch {
            undecodable(partition, body: body, error: error, of: request)
            return
        }
        switch request.kind {
        case .gap:
            guard var gap = gaps[partition] else {
                return
            }
            var batch = polled
            batch.messages.removeAll { $0.offset < gap.next || $0.offset >= gap.end }
            batch.count = UInt32(batch.messages.count)
            if let last = batch.messages.last {
                gap.next = last.offset + 1
                arrivals.append(.batch(batch, generation: generation))
            }
            if batch.messages.isEmpty || gap.next >= gap.end {
                arrivals.append(contentsOf: gap.held)
                gaps[partition] = nil
            } else {
                gaps[partition] = gap
            }
        case .isolate:
            guard var isolation = isolations[partition] else {
                return
            }
            if let message = polled.messages.first {
                arrivals.append(.batch(polled, generation: generation))
                isolation.first = message.offset + 1
            }
            if polled.messages.isEmpty || isolation.first > isolation.last {
                isolations[partition] = nil
            } else {
                isolations[partition] = isolation
            }
        default:
            pollAlone.remove(partition)
            let end = polled.messages.last.map { $0.offset + 1 } ?? 0
            _ = accept(
                .batch(polled, generation: generation), of: partition, reachingThrough: end, context: polled.context,
                firstOffset: polled.messages.first?.offset)
            if polled.messages.isEmpty {
                pauseIdle([partition])
            }
        }
    }

    /// A poll reply that does not decode. One message read alone names the
    /// bad message: its partition stops until a seek steps over it.
    private func undecodable(_ partition: UInt32, body: [UInt8], error: any Error, of request: Request) {
        let raw = Array(body.dropFirst(Self.pollReplyHeadSize))
        guard request.kind == .isolate, let isolation = isolations[partition] else {
            pausedUntil[partition] = .now + configuration.retryInterval
            report(ReadError(partitionID: partition, error: Self.iggyError(error), raw: raw))
            return
        }
        isolations[partition] = nil
        stopped.insert(partition)
        let skip = PollingStrategy.continuation(partitionID: partition, kind: .offset, value: isolation.first + 1, context: isolation.context)
        report(ReadError(partitionID: partition, error: Self.iggyError(error), skip: skip, offset: isolation.first, raw: raw))
    }

    private func failed(_ request: Request, with error: any Error) {
        if error is CancellationError {
            return
        }
        let partitions = request.partitions.filter { request.generations[$0] == generations[$0] }
        guard !partitions.isEmpty else {
            return
        }
        if request.commits, (error as? IggyError)?.code == .transientNotCommitted {
            uncertain.formUnion(partitions)
        }
        if request.kind == .gap, let partition = partitions.first, (error as? IggyError)?.code == .consumerGroupPartitionNotOwned,
            let gap = gaps.removeValue(forKey: partition)
        {
            giveUp(gap, of: partition)
        }
        failed(partitions, with: Self.iggyError(error))
    }

    /// The partition moved to another member before its gap was read again:
    /// those messages are lost to this reader, the batches after them are not.
    private func giveUp(_ gap: Gap, of partition: UInt32) {
        arrivals.append(contentsOf: gap.held)
        report(
            ReadError(
                partitionID: partition,
                error: IggyError(
                    .fetchProgressUncertain,
                    context: "offsets \(gap.next) to \(gap.end - 1) of partition \(partition) were stored by a lost read and moved away",
                    partitionID: partition)))
    }

    /// Repairs what a failed read of `partitions` points at, and they wait
    /// for the retry interval: a moved primary groups again, a lost owner
    /// syncs the assignment, a position without a history reads again, a lost
    /// membership joins again. What the reader cannot repair is queued for
    /// ``next()``.
    private func failed(_ partitions: [UInt32], with error: IggyError) {
        let until = ContinuousClock.now + configuration.retryInterval
        switch error.code {
        case .notPrimary:
            groupingEpoch += 1
            return
        case .consumerGroupPartitionNotOwned:
            for partition in partitions {
                positions[partition] = nil
                expected[partition] = nil
            }
            client.core.groupState.invalidateAssignment(groupKey)
            groupingEpoch += 1
            return
        case .historyUnavailable:
            for partition in partitions {
                if positions[partition]?.context != nil {
                    stopped.insert(partition)
                    report(ReadError(partitionID: partition, error: error))
                } else {
                    forget(partition)
                    pausedUntil[partition] = until
                }
            }
            client.core.pollRouter.dropRoutes(partitions.map { PollRouter.RouteKey(stream: streamID, topic: topicID, partitionID: $0) })
            return
        case .consumerGroupMemberNotFound:
            joinedGroup = false
            if configuration.autoJoinConsumerGroup {
                return
            }
        case .transientNotAccepted, .transientNotCommitted, .deferredPollCapacityExceeded:
            for partition in partitions {
                pausedUntil[partition] = until
            }
            logger.warning("A read was not answered, reading again", metadata: metadata.merging(["error": "\(error)"]) { $1 })
            return
        case .disconnected, .unauthenticated, .staleClient:
            joinedGroup = false
        default:
            break
        }
        for partition in partitions {
            pausedUntil[partition] = until
        }
        logger.error("Failed to read messages", metadata: metadata.merging(["partitions": "\(partitions)", "error": "\(error)"]) { $1 })
        report(ReadError(partitionID: partitions.count == 1 ? partitions.first : nil, error: error))
    }

    /// Queues an error for ``next()``. One waits at a time.
    private func report(_ error: ReadError) {
        guard !arrivals.contains(where: \.isFailure) else {
            return
        }
        arrivals.append(.failure(error))
    }

    // MARK: Commits

    /// Under ``CommitAfter/processing``, the batch that ``next()`` returned
    /// last counts once the application asks for the next one.
    private func countUncounted() {
        if let (checkpoint, messages) = uncounted {
            uncounted = nil
            count(checkpoint, messages: messages, endsBatch: true)
        }
    }

    /// Counts `messages` up to `checkpoint` under ``CommitPolicy/auto(_:)``
    /// and wakes the store task when its event makes a store due. A count
    /// only moves forward within a generation, and a newer generation
    /// replaces it.
    func count(_ checkpoint: Checkpoint, messages: UInt32, endsBatch: Bool) {
        guard let auto = configuration.options.autoCommit, isCurrent(checkpoint) else {
            return
        }
        var entry = counted[checkpoint.partitionID] ?? Counted(checkpoint: checkpoint, sinceStore: 0)
        let generation = { (checkpoint: Checkpoint) in checkpoint.stamp?.generation ?? 0 }
        if generation(checkpoint) > generation(entry.checkpoint) {
            entry = Counted(checkpoint: checkpoint, sinceStore: 0)
        } else if generation(checkpoint) < generation(entry.checkpoint) {
            return
        } else if !Self.sameHistory(entry.checkpoint.context, checkpoint.context) || checkpoint.offset > entry.checkpoint.offset {
            entry.checkpoint = checkpoint
        }
        entry.sinceStore = entry.sinceStore &+ messages
        counted[checkpoint.partitionID] = entry
        let due: Bool
        switch auto.every {
        case nil: due = false
        case .message?: due = true
        case .messages(let every)?: due = entry.sinceStore >= every
        case .batch?: due = endsBatch
        }
        if due {
            commitWakeup.yield()
        }
    }

    /// Returns false once the reader is closed, which ends the store tasks.
    private func flushOnTick() async -> Bool {
        guard !isClosed else {
            return false
        }
        do {
            try await flushCounted()
        } catch {
            logger.error("Failed to store the counted offsets", metadata: metadata.merging(["error": "\(error)"]) { $1 })
        }
        return !isClosed
    }

    /// Stores each counted position that is ahead of the last store of its
    /// generation, the retired ones first. Every partition is tried, and the
    /// first failure is thrown.
    private func flushCounted() async throws {
        var failure: (any Error)?
        let retired = retiring
        retiring.removeAll()
        for checkpoint in retired {
            do {
                try await store(checkpoint)
            } catch {
                failure = failure ?? error
            }
        }
        counted = counted.filter { isCurrent($0.value.checkpoint) }
        for (partition, entry) in counted {
            do {
                try await store(entry.checkpoint)
                if var current = counted[partition] {
                    current.sinceStore = current.sinceStore >= entry.sinceStore ? current.sinceStore - entry.sinceStore : 0
                    counted[partition] = current
                }
            } catch {
                logger.error(
                    "Failed to store the counted offset", metadata: metadata.merging(["checkpoint": "\(entry.checkpoint)", "error": "\(error)"]) { $1 })
                failure = failure ?? error
            }
        }
        if let failure {
            throw failure
        }
    }

    /// Stores a checkpoint unless the last store of its history covers it.
    private func store(_ checkpoint: Checkpoint) async throws {
        if committedCovers(checkpoint) {
            return
        }
        try await afterProgress(of: checkpoint.partitionID) {
            try await offsetWrites.lock()
            defer { offsetWrites.unlock() }
            if committedCovers(checkpoint) {
                return
            }
            try await client.storeConsumerPosition(consumer: consumer, streamID: streamID, topicID: topicID, checkpoint: checkpoint)
            committed[checkpoint.partitionID] = checkpoint
        }
    }

    private func committedCovers(_ checkpoint: Checkpoint) -> Bool {
        guard let last = committed[checkpoint.partitionID], Self.sameHistory(last.context, checkpoint.context),
            last.stamp?.generation == checkpoint.stamp?.generation
        else {
            return false
        }
        return checkpoint.offset <= last.offset
    }

    /// Moves the offset of a partition back over the messages the server
    /// stored but nobody received, as `rewind` in the Rust SDK.
    private func rewind(_ partition: UInt32, over span: Span) async throws {
        if isGroup, client.core.groupState.assignedPartitions(groupKey)?.contains(partition) != true {
            return
        }
        let last = delivered[partition].flatMap { Self.sameHistory($0.context, span.context) && $0.offset >= span.first ? $0.offset : nil }
        if let last {
            guard last < span.last else {
                return
            }
        } else if configuration.start.kind != .next {
            // A partition that started elsewhere had its own stored offset
            // before, which the reads may not have moved.
            return
        }
        try await afterProgress(of: partition) {
            try await offsetWrites.lock()
            defer { offsetWrites.unlock() }
            if let last {
                try await client.storeConsumerPosition(
                    consumer: consumer, streamID: streamID, topicID: topicID,
                    checkpoint: Checkpoint(partitionID: partition, offset: last, context: span.context))
            } else if span.first == 0 {
                try await client.deleteConsumerOffset(
                    consumer: consumer, streamID: streamID, topicID: topicID, partitionID: partition, context: span.context)
            } else {
                try await client.storeConsumerPosition(
                    consumer: consumer, streamID: streamID, topicID: topicID,
                    checkpoint: Checkpoint(partitionID: partition, offset: span.first - 1, context: span.context))
            }
        }
    }

    /// Runs a store or delete of `partition` once no read in flight reads or
    /// moves its stored offset, and no such read starts meanwhile, so the two
    /// never reorder on the server.
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
        while activeReads[partition].flatMap({ requests[$0] })?.progresses == true {
            let (ended, wakeup) = AsyncStream<Void>.makeStream()
            storeWakeups.append(wakeup)
            for await _ in ended {}
            try Task.checkCancellation()
        }
        try await write()
    }

    /// Offsets compare only within one incarnation and owner.
    static func sameHistory(_ left: PartitionContext, _ right: PartitionContext) -> Bool {
        left.incarnation == right.incarnation && left.ownerGeneration == right.ownerGeneration
    }

    private static func iggyError(_ error: any Error) -> IggyError {
        (error as? IggyError) ?? IggyError(.cannotReadMessage, context: "\(error)")
    }
}

extension IggyClient {
    /// A reader of the partitions of one topic. A standalone `consumer`
    /// reads ``ReaderConfiguration/partitions``. A consumer group member,
    /// `.group(id)`, reads its assignment and joins the group here, after
    /// creating it when it is missing and named by name. Throws
    /// ``IggyErrorCode/invalidConfiguration`` for options out of range, and
    /// what the join throws.
    public func reader(
        streamID: Identifier, topicID: Identifier, consumer: Consumer, configuration: ReaderConfiguration = ReaderConfiguration()
    ) async throws -> PartitionReader {
        let reader = try PartitionReader(client: self, streamID: streamID, topicID: topicID, consumer: consumer, configuration: configuration)
        try await reader.start()
        return reader
    }
}
