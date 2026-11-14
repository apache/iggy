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
    /// reads from offset zero. With ``CommitPolicy/onRead``, the start applies
    /// only to a partition without a stored offset. A partition with one
    /// resumes after it. A seek moves the stored offset.
    public var start: PollingStrategy
    /// The limits of each read and the commit policy. `count` caps a read of
    /// many partitions as a whole.
    public var options: PollOptions
    /// The pause after a failed read before its partitions are read again.
    /// After a read refused as not owned, the pause doubles with each such
    /// refusal in a row, up to 32 times this interval.
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
    /// Whether the partition reads nothing until a
    /// ``PartitionReader/seek(partitionID:to:)``: a message that does not
    /// decode, a position whose history is gone, or tiered data that the
    /// object store refuses for good. A partition that a lost
    /// committing read of another caller blocks (``IggyErrorCode/partitionLaneBlocked``
    /// under ``CommitPolicy/onRead``) reads again by itself once
    /// ``IggyClient/abandonOperation(_:)`` took that operation.
    public let stopsPartition: Bool
    /// For ``IggyErrorCode/fetchProgressUncertain``: the first and the last
    /// offset of a range that a lost read stored and the reader never
    /// delivered, or that the history no longer holds. This report stops
    /// nothing and needs no ``PartitionReader/seek(partitionID:to:)``. A
    /// batch that the reader holds after the range follows it, and then, if
    /// the partition stopped, an error whose ``stopsPartition`` is true.
    public let lost: ClosedRange<UInt64>?

    init(
        partitionID: UInt32?, error: IggyError, skip: PollingStrategy? = nil, offset: UInt64? = nil, raw: [UInt8]? = nil, stopsPartition: Bool = false,
        lost: ClosedRange<UInt64>? = nil
    ) {
        self.partitionID = partitionID
        self.error = error
        self.skip = skip
        self.offset = offset
        self.raw = raw
        self.stopsPartition = stopsPartition
        self.lost = lost
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
///   ``PollOptions/maxWait`` ran out. Partitions whose last read ended before
///   their head spread over the free requests, each share in a fetch of its
///   own beside that one. A moved primary, a rebalance and a lost connection
///   are repaired inside. After a good lookup of the primaries, a failed one
///   keeps the last groups, the reads go on with them, and the lookup runs
///   again after ``ReaderConfiguration/retryInterval``. Unless its refusal
///   is transient, a failed lookup is thrown once as a ``ReadError`` of the
///   whole reader. A partition refused as not primary waits for the next
///   good lookup.
/// - ``next()`` returns the next batch of one partition, never empty, in
///   offset order within one generation of the partition: from its first
///   read, from each ``seek(partitionID:to:)``, and from each new assignment
///   to this member.
/// - The ``CommitPolicy`` of the options decides who stores offsets. With
///   `manual`, ``commit(_:)`` stores a checkpoint. With `onRead`, the server
///   stores the offset of each read, the reader reads only once ``next()``
///   waits with nothing queued and then fills its free requests, a seek to an
///   offset above zero stores the offset before it, a seek to zero deletes the
///   stored offset, and ``close()`` moves the offset back to the last
///   delivered message. The reader reads a timestamp or `last` position
///   first without a commit, then reads on from its first message. After a
///   reply lost to a broken
///   connection, a read that the server stored but never delivered shows as
///   a gap before the next reply, and the reader reads that gap again
///   without a commit and delivers it first. A partition that leaves the
///   assignment still hands over the reads that the server stored for it.
///   If a lost read of that partition stored a known range that the reader
///   did not read again, ``next()`` first throws a ``ReadError`` with
///   ``IggyErrorCode/fetchProgressUncertain`` that names that range.
///   ``close()`` does not move the offset of a partition that left. So the
///   group loses its batches that ``next()`` did not return before
///   ``close()``.
///   A crash, or a reader that stops without ``close()``, loses the batches
///   that the server stored before the application took them: up to the
///   replies of 32 requests. `manual` and `auto` store offsets after
///   delivery instead. With `auto`, the reader counts
///   each batch as its ``CommitAfter`` says, or each ``mark(_:)``, and stores
///   the latest counted position at the interval or the event of the policy,
///   before a rebalance takes a partition away, and in ``close()``.
/// - A failed read is thrown as a ``ReadError`` and its partitions are read
///   again after ``ReaderConfiguration/retryInterval``. With a zero
///   ``PollOptions/maxWait``, a partition that a read found empty waits it
///   too.
///   ``IggyErrorCode/historyUnavailable`` for a position read in a history
///   that is gone, ``IggyErrorCode/tieredReadFailed`` and a message that does
///   not decode stop the partition until a seek, and the read is not sent
///   again.
public actor PartitionReader: AsyncSequence {
    public typealias Element = PolledMessages

    /// Thirty-two requests can wait or read concurrently, as
    /// `MAX_DEFERRED_REQUESTS_PER_CONSUMER` in the Rust SDK, so a reader
    /// leaves deferred connections of its client to others.
    static let maxRequestsInFlight = 32
    /// The longest pause between two assignment syncs of a waiting group
    /// member, as `ASSIGNMENT_REFRESH_INTERVAL` in the Rust SDK.
    static let assignmentCheckInterval: Duration = .seconds(1)
    /// The pause before the next assignment sync when the last one came right
    /// after a join or moved the member's partitions, as
    /// `ASSIGNMENT_SETTLE_PAUSE` in the Rust SDK. A join moves the ownership
    /// of a partition only after the partition log persists the new owner,
    /// so a sync right after it can hold part of the assignment. Each sync
    /// that finds the same partitions doubles the pause, up to
    /// ``assignmentCheckInterval``.
    static let assignmentSettlePause: Duration = .milliseconds(10)
    /// A reader without ``CommitPolicy/onRead`` fetches a group while fewer
    /// replies of the group wait for ``next()``, so the next fetch goes out
    /// before the reply before it is taken, and the queue stays bounded.
    static let waitingRepliesPerGroup = 2
    /// A partition whose reads are refused as not owned this many times in a
    /// row reports one error and reads on, as `MAX_LASTING_REFUSALS` in the
    /// Rust SDK.
    static let notOwnedRefusalsPerReport = 6
    /// The wait after a read refused as not owned doubles with each refusal
    /// in a row at most this many times, as `MAX_REFUSAL_BACKOFF_STEPS` in
    /// the Rust SDK.
    static let maxRefusalBackoffSteps = 5
    /// The reads that name the owner of a refused offset write of the close
    /// stop this long after the close starts, so a refusal that stays
    /// transient does not hold the close.
    static let closeBudget: Duration = .seconds(10)
    /// Refusals that settle by themselves, so a join, a sync or a lookup that
    /// fails on one is not reported, as `refuse_refresh` in the Rust SDK, and
    /// the read that names the owner of a refused offset write goes again.
    private static let transientRefusals: Set<IggyErrorCode> = [.transientNotAccepted, .transientNotCommitted, .deferredPollCapacityExceeded, .notPrimary]
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
    /// Held by every store and delete of a partition, so a coverage check,
    /// its request, and the record of it see no other write of the partition
    /// in between. A write that waits for the owner of its partition holds
    /// no write of another partition.
    private var offsetWriteLocks: [UInt32: AsyncMutex] = [:]
    /// Opened for good by a cancel of the first caller of ``close()``. No
    /// read that names the owner of a refused offset write starts after it,
    /// and the pause of such a read ends at once. Each pause that it ends
    /// takes a permit, so the cancel releases all of them.
    private let closeCancel = AsyncSemaphore(permits: 0)
    /// Opened for good at the start of ``close()``, which ends the reads that
    /// name the owner of a refused offset write other than a write of the
    /// close, as `closeCancel` ends them for the writes of the close. It
    /// also ends each pause of a read call.
    private let closeStart = AsyncSemaphore(permits: 0)
    private let commitWakeups: AsyncStream<Void>
    private let commitWakeup: AsyncStream<Void>.Continuation

    private var isStarted = false
    private(set) var isClosed = false
    private var closeTask: Task<Void, any Error>?
    /// Follows the client's events: reading waits while the client is signed
    /// out and can come back on its own.
    private var canRead = true
    private var joinedGroup = false
    /// The session of the last join of the reader. For a member that the
    /// application joins, the last session that ``prepare()`` saw.
    private var joinedSessionScope: UInt64?
    private var lastAssignmentSync: ContinuousClock.Instant?
    /// Nil until the first sync after a join.
    private var assignmentSyncPause: Duration?
    /// The assignment sync in flight, of ``prepare()`` or beside the reads.
    /// Its value tells whether it succeeded.
    private var assignmentSync: Task<Bool, Never>?
    private var eventsTask: Task<Void, Never>?
    private var intervalTask: Task<Void, Never>?
    private var commitTask: Task<Void, Never>?

    /// The partitions the reader reads grouped by primary, each group in the
    /// order of its next fetch.
    private var fetchGroups: [[UInt32]] = []
    private var assigned: Set<UInt32> = []
    /// Moves on when a primary or the assignment can have moved, so the
    /// partitions are grouped again. Nothing is read before the first
    /// grouping. After it the reads go on with the last groups while a
    /// lookup runs and after one failed.
    private var groupingEpoch: UInt64 = 1
    private var groupedEpoch: UInt64 = 0
    /// The lookup of the groups that runs beside the reads.
    private var groupLookup: Task<Void, Never>?
    /// When the lookup after a failed one starts.
    private var groupLookupRetry: ContinuousClock.Instant?
    /// The partitions that the server refused as not primary or not owned,
    /// by the grouping epoch of the refusal. Each waits until a lookup of
    /// that epoch or a later one succeeded, so the last groups do not send it
    /// to the same node again while lookups fail.
    private var awaitingLookup: [UInt32: UInt64] = [:]
    private var generations: [UInt32: UInt64] = [:]
    private var sought: [UInt32: UInt64] = [:]
    /// The generation that started the current assignment of each
    /// partition. A read of an earlier assignment decides nothing at the
    /// close.
    private var assignedSince: [UInt32: UInt64] = [:]
    /// The lowest owner generation that a read of each partition named in a
    /// session, with the scope of that session. A new session installs the
    /// owner again under a higher generation.
    private var owners: [UInt32: (session: UInt64, generation: UInt64)] = [:]
    /// The reads of each partition refused as not owned since its last read
    /// that succeeded or the last join of the group.
    private var notOwnedRefusals: [UInt32: Int] = [:]
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
    /// With `onRead`: the partitions with a committing read or a seek write
    /// whose outcome is unknown, until a read after the stored offset
    /// arrives. Only they read a forward gap again, any other jump is a
    /// store of the application or of another session.
    private var lostReads: Set<UInt32> = []
    /// With `onRead`: the partitions that a committing read of another
    /// caller blocks, by its operation. They read again once it is
    /// abandoned.
    private var blockedBy: [UInt32: UInt128Value] = [:]
    /// With `onRead`: the partitions whose lookup at the start, or before a
    /// seek write, found no stored offset.
    private var noStoredOffset: Set<UInt32> = []
    /// With `onRead`: where the first committing read of a partition started,
    /// or where the next read starts after a seek write with an unknown
    /// outcome, until a batch arrives. When that read lost its reply, or
    /// that write moved the stored offset, the close moves the partition
    /// before it if the stored offset reached it.
    private var rewindPoints: [UInt32: (next: UInt64, context: PartitionContext)] = [:]
    /// With `onRead`: the messages each partition read and the server stored
    /// in its current generation, which the close moves back over.
    private var stored: [UInt32: Span] = [:]
    private var gaps: [UInt32: Gap] = [:]
    private var isolations: [UInt32: Span] = [:]
    /// The partitions whose last fetch section ended before the head of the
    /// partition, which a read answers at once.
    private var backlogged: Set<UInt32> = []
    /// Partitions read with a poll of their own next: a fetch section that
    /// the server refused with `invalidSizeBytes`.
    private var pollAlone: Set<UInt32> = []
    private var pausedUntil: [UInt32: ContinuousClock.Instant] = [:]
    /// Partitions that read nothing until a seek.
    private var stopped: Set<UInt32> = []
    private var pollCursor: UInt32?
    private var readerWakeup: AsyncStream<Void>.Continuation?
    private(set) var storesWaiting: [UInt32: Int] = [:]
    private var storeWakeups: [AsyncStream<Void>.Continuation] = []
    /// With `onRead`: the seeks in flight of each partition, oldest first. A
    /// seek moves its partition only after the seeks before it ended, so a
    /// seek that ends in the background never replaces a later position.
    private var seeksInFlight: [UInt32: [UInt64]] = [:]
    private var lastSeek: UInt64 = 0

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
        /// The session scope of the client, so a check of an older session
        /// moves nothing.
        let session: UInt64
        /// Where a poll starts and how many messages it asks for.
        var start: PollingStrategy?
        var count: UInt32 = 0
        let task: Task<Void, Never>
    }

    private enum RequestKind {
        /// The fetch of a readiness group.
        case main
        /// The fetch of the partitions of a group that turned idle while
        /// its main fetch waited.
        case second
        /// With `onRead`: a committing fetch of backlogged partitions beside
        /// the fetch of the group that may wait.
        case backlog
        /// A poll of one partition.
        case single
        /// A poll that reads a gap again, without a commit.
        case gap
        /// A poll of one message of a section that did not decode.
        case isolate
        /// With `onRead`: a poll of a timestamp or `last` position that
        /// neither commits nor delivers. The committing reads start at its
        /// first message, so the close knows where they started.
        case check

        var isPoll: Bool {
            switch self {
            case .main, .second, .backlog: false
            case .single, .gap, .isolate, .check: true
            }
        }
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

    /// Who waits for an offset write. The end of its wait ends the retry of
    /// the read that names the owner of a refused write, and the write keeps
    /// its refusal.
    private enum WriteCaller: Equatable {
        /// The open reader, for a commit of the application, a seek write
        /// and an `auto` store in the background, until the close starts.
        case reader
        /// The close, until `deadline` or a cancel of its first caller.
        case close(deadline: ContinuousClock.Instant)
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
            // Without a stored offset the server answers `next` from offset zero.
            configuration.start = configuration.options.commitsOnRead ? .next : .offset(0)
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
        groupLookup?.cancel()
        assignmentSync?.cancel()
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

    /// Stops the reader. It cancels the committing reads in flight, and each
    /// waits for its one reply: nothing stored when the cancel won, or the
    /// messages when the store won, which count as undelivered. So the close
    /// takes about a round trip. With `manual` it stores nothing. With `auto` it
    /// counts the last batch under ``CommitAfter/processing`` and stores the
    /// latest counted position of each partition. With `onRead` it moves the
    /// offset of each partition back to its last delivered message. A
    /// partition that delivered nothing gets the offset before the first
    /// message it read. At offset zero, if a stored offset exists, the close
    /// deletes it. A partition whose start found no stored offset, and that
    /// delivered nothing, gets no stored offset again after a seek write
    /// with an unknown outcome. A partition that left the assignment keeps
    /// its offset. A group member then leaves its group.
    ///
    /// The start of the close ends the retry of each read that names the
    /// owner of a refused offset write of a ``commit(_:)``, a seek or an
    /// `auto` store. Such a write fails with its refusal and is not sent
    /// again, so the close waits for it no longer than one request in
    /// flight.
    ///
    /// The close belongs to its first caller. For the writes of the close
    /// itself, a cancel of that caller ends that retry at once, and the
    /// close deadline ends it too, 10 seconds after the start of the close.
    /// Such a write keeps its refusal, also when the read was in flight at
    /// that end. The close still sends once each offset write and lookup
    /// that it did not send yet, with no such read, and then the leave. So a
    /// close that starts in a cancelled task also sends its offset writes
    /// and the leave.
    ///
    /// A read in flight ends within the request timeout of
    /// ``ReaderConfiguration/options``. An offset write or lookup ends within
    /// 30 seconds, a fixed limit that includes each resend after a transient
    /// refusal. The close sends them one after another. So during an outage
    /// it can take about 30 seconds for each partition with a pending store.
    /// The leave first waits for a join in flight, which sends up to three
    /// requests. So the leave can take up to four times
    /// ``ClientConfiguration/requestTimeout``. The close ends after the last
    /// request.
    ///
    /// A later caller waits for the same close and gets its result, and a
    /// cancel of that caller cancels nothing. ``IggyConsumer/shutdown()``
    /// cancels the close at ``ConsumerConfiguration/offsetDrainTimeout``.
    ///
    /// Throws the first failed store.
    public func close() async throws {
        if let closeTask {
            return try await closeTask.value
        }
        isClosed = true
        closeStart.release(.max)
        let deadline = ContinuousClock.now + Self.closeBudget
        // The drain does not inherit a cancel, so the writes that it did not send yet and the leave still go.
        let task = Task { try await self.drain(ownerReadsUntil: deadline) }
        closeTask = task
        try await withTaskCancellationHandler {
            try await task.value
        } onCancel: { [closeCancel] in
            closeCancel.release(.max)
        }
    }

    private func drain(ownerReadsUntil deadline: ContinuousClock.Instant) async throws {
        let caller = WriteCaller.close(deadline: deadline)
        groupLookup?.cancel()
        assignmentSync?.cancel()
        for (id, request) in requests {
            if request.commits {
                // It sends its cancel and keeps waiting for its one reply, which
                // says whether the server stored it.
                request.task.cancel()
            } else {
                cancel(id)
            }
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
        // A seek that stores while the close starts applies before the rewind. No code cancels the drain.
        try? await waitForEnds { storesWaiting.isEmpty }
        var failure: (any Error)?
        if configuration.options.commitsOnRead {
            for partition in uncertain {
                // Only what the server stored moves back: before a start past the end a store is refused.
                guard let baseline = baselines[partition] ?? rewindPoints[partition] else {
                    continue
                }
                do {
                    let current = try await client.consumerOffsetNext(
                        consumer: consumer, streamID: streamID, topicID: topicID, partitionID: partition,
                        context: requestContext(baseline.context, of: partition))
                    if current > baseline.next {
                        recordStored(partition, through: current, from: baseline.next, context: baseline.context)
                    }
                } catch {
                    failure = failure ?? error
                }
            }
            for (partition, span) in stored {
                do {
                    try await rewind(partition, over: span, for: caller)
                } catch {
                    failure = failure ?? error
                }
            }
        }
        if configuration.options.autoCommit != nil {
            do {
                try await flushCounted(for: caller)
            } catch {
                failure = failure ?? error
            }
        }
        // The server refuses a store from a member that left, so the stores go
        // first. A lost session error clears `joinedGroup` while the client can
        // still hold the member under the same session, so the leave always goes.
        if isGroup, configuration.autoJoinConsumerGroup, (try? await membership.lock()) != nil {
            defer { membership.unlock() }
            joinedGroup = false
            do {
                try await client.leaveConsumerGroup(streamID: streamID, topicID: topicID, groupID: consumer.id)
            } catch {
                logger.debug("Failed to leave the consumer group", metadata: metadata.merging(["error": "\(error)"]) { $1 })
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
            guard await prepare(), !isClosed else {
                continue
            }
            await waitForArrival()
        }
        return nil
    }

    /// Whether a batch that this reader returned still counts. A seek of its
    /// partition, or a new assignment without `onRead`, drops it.
    func holds(_ batch: PolledMessages) -> Bool {
        guard let stamp = batch.stamp, stamp.reader == id else {
            return true
        }
        return keeps(batch.partitionID, stamp.generation)
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
    /// the last commit of its generation, stores nothing. For a group member,
    /// a store refused as not owned goes once more, under the owner that a
    /// read without a commit names. A transient refusal of that read goes
    /// again after ``ReaderConfiguration/retryInterval`` until the caller is
    /// cancelled or ``close()`` starts. After that end the commit fails with
    /// the refusal and stores nothing again, also when that read was in
    /// flight.
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
        try await store(checkpoint, for: .reader)
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
    /// `onRead` it stores no offset. With `onRead` a seek to an offset above
    /// zero first stores the offset before it, under the history of a
    /// continuation or else of the last delivered message, once the
    /// committing read of the partition in flight ended, so a restart resumes
    /// at the new position. If a stored offset exists, a seek to offset zero
    /// or to ``PollingStrategy/first`` deletes it at that point instead. A
    /// seek to a timestamp first reads the message at the timestamp without
    /// a commit, then works as a seek to its offset. Without such a message
    /// it stores nothing. A read never moves the stored offset back. No
    /// committing read of the partition starts while the seek runs. A
    /// refused read, store, lookup or delete fails the seek, and nothing
    /// moves. For a group member that still reads the partition, a store or
    /// delete refused as not owned goes once more, under the owner that a
    /// read without a commit names. A transient refusal of that read goes
    /// again after ``ReaderConfiguration/retryInterval`` until ``close()``
    /// starts. A store or delete with an unknown outcome also fails the seek,
    /// and the next read reads again what that write may have skipped. After
    /// such a delete the partition reads on after the deleted offset. Unless
    /// a later message was delivered, ``close()`` stores that offset again.
    /// For a group member, an unknown outcome that ends after a new session,
    /// or after the partition left the assignment, changes nothing.
    /// A caller cancelled before the first store or delete gets
    /// `CancellationError`, and nothing moves. A caller cancelled later gets
    /// `CancellationError` at once, and the seek still finishes and moves
    /// on success. The reader then logs a failure as a warning. A later seek
    /// and ``close()`` wait for it. Every other seek stores nothing. The
    /// seeks of a partition move it in the order of their calls. Each seek
    /// waits for the writes of the seeks before it. A seek that stores
    /// nothing waits too.
    ///
    /// Throws ``IggyErrorCode/invalidCommand`` for a partition the reader
    /// does not read, for a continuation of another partition and once the
    /// close started, and the errors of the read, the store, the lookup and
    /// the delete.
    public func seek(partitionID: UInt32, to start: PollingStrategy) async throws {
        try start.validate(partitionID: partitionID)
        guard !isClosed else {
            throw IggyError(.invalidCommand, context: "the reader is closed")
        }
        guard assigned.contains(partitionID) else {
            throw IggyError(.invalidCommand, context: "the reader does not read partition \(partitionID)")
        }
        var start = start
        if start.kind == .first {
            start = PollingStrategy(kind: .offset, value: 0, context: start.context, partitionID: start.partitionID)
        }
        guard configuration.options.commitsOnRead else {
            move(partitionID, to: start, after: nil)
            readerWakeup?.yield()
            return
        }
        lastSeek += 1
        let seek = lastSeek
        seeksInFlight[partitionID, default: []].append(seek)
        hold(partitionID)
        // Once a write goes out, the part beside the reads ends the seek.
        var holds = true
        defer {
            if holds {
                endSeek(seek, of: partitionID)
            }
        }
        if start.kind == .timestamp {
            let probe = try await client.pollMessages(
                streamID: streamID, topicID: topicID, partitionID: partitionID, consumer: consumer, strategy: start, count: 1, autoCommit: false,
                options: probeOptions)
            // The read after the first one continues after the stored offset, which can lie past the timestamp.
            if let first = probe.messages.first {
                start = .continuation(partitionID: partitionID, kind: .offset, value: first.offset, context: probe.context)
            }
        }
        // An earlier seek of the partition can still write in the background, and it moves when its write ends.
        try await waitForEnds { seeksInFlight[partitionID]?.first == seek }
        guard start.kind == .offset else {
            move(partitionID, to: start, after: nil)
            return
        }
        try await waitForReads(of: partitionID)
        let writes = offsetWrites(of: partitionID)
        try await writes.lock()
        var locks = true
        defer {
            if locks {
                writes.unlock()
            }
        }
        if needsStartLookup(partitionID) || needsBaseline(partitionID) {
            // A position that follows the stored offset learns it before the write. After a write with an
            // unknown outcome, the next read then finds the jump of that write and reads the skipped range again.
            try await findStart(of: partitionID)
        }
        let context = start.context ?? delivered[partitionID]?.context ?? .empty
        let before = start.value > 0 ? Checkpoint(partitionID: partitionID, offset: start.value - 1, context: context) : nil
        var prior: UInt64?
        if before == nil || !knowsRestorePoint(of: partitionID) {
            prior = try await client.getConsumerOffset(consumer: consumer, streamID: streamID, topicID: topicID, partitionID: partitionID)?
                .storedOffset
            if prior == nil {
                // The server refuses to delete a missing offset.
                guard before != nil else {
                    move(partitionID, to: start, after: nil)
                    return
                }
                noStoredOffset.insert(partitionID)
            }
        }
        let fence = requestContext(context, of: partitionID)
        try Task.checkCancellation()
        // An outcome that ends after a new assignment of the partition, or after a new session of a member, belongs to the
        // one the write started in.
        let (generation, session) = (generations[partitionID], client.core.groupState.sessionScope)
        holds = false
        locks = false
        let (outcome, ends) = AsyncThrowingStream<Void, any Error>.makeStream()
        Task { [start, prior] in
            defer {
                writes.unlock()
                endSeek(seek, of: partitionID)
            }
            do {
                try await writeOffset(of: partitionID, under: fence, generation: generation, for: .reader) { context in
                    if let before {
                        try await client.storeConsumerPosition(
                            consumer: consumer, streamID: streamID, topicID: topicID,
                            checkpoint: Checkpoint(partitionID: partitionID, offset: before.offset, context: context))
                    } else {
                        try await client.deleteConsumerOffset(
                            consumer: consumer, streamID: streamID, topicID: topicID, partitionID: partitionID, context: context)
                    }
                }
                move(partitionID, to: start, after: before)
                ends.finish()
            } catch {
                if let error = error as? IggyError, error.code == .transientNotCommitted, generations[partitionID] == generation,
                    !isGroup || client.core.groupState.sessionScope == session
                {
                    writeLost(partitionID, prior: prior, context: context)
                    lost([partitionID], to: error)
                }
                // A cancelled caller ended the stream and hears nothing of the error.
                if case .terminated = ends.yield() {
                    logger.warning(
                        "A seek failed after its caller was cancelled",
                        metadata: metadata.merging(["partition": "\(partitionID)", "error": "\(error)"]) { $1 })
                }
                ends.finish(throwing: error)
            }
        }
        for try await _ in outcome {}
        try Task.checkCancellation()
    }

    /// Starts the generation of a seek of `partition` at `start`. `before` is
    /// the offset that the seek stored with `onRead`.
    private func move(_ partition: UInt32, to start: PollingStrategy, after before: Checkpoint?) {
        newGeneration(of: partition, seeking: true)
        guard let before else {
            positions[partition] = start
            return
        }
        positions[partition] =
            before.context == .empty ? start : .continuation(partitionID: partition, kind: .offset, value: start.value, context: before.context)
        // The stored offset counts as delivered, so the close moves back to it over a read nobody received.
        expected[partition] = start.value
        delivered[partition] = Checkpoint(
            partitionID: partition, offset: before.offset, context: before.context, stamp: ReaderStamp(reader: id, generation: lastGeneration))
        stored[partition] = Span(first: before.offset, last: before.offset, context: before.context)
    }

    /// A seek write of `partition` with an unknown outcome. The next read
    /// finds a jump of that write and reads the skipped range again. A
    /// partition without a stored offset gets that back at the close, while
    /// no batch of it arrived. A read never moves the stored offset back, but
    /// the write may have replaced the stored offset `prior` that the seek
    /// looked up. So a position after the stored offset reads on after
    /// `prior`, and the close stores `prior` again unless a later message
    /// was delivered. After a store without a lookup, the close writes the
    /// last delivered offset, or the one before the baseline, because a store
    /// can move the offset back.
    private func writeLost(_ partition: UInt32, prior: UInt64?, context: PartitionContext) {
        let history = stored[partition]?.context ?? baselines[partition]?.context ?? context
        if let prior, startPoint(of: partition).kind == .next {
            positions[partition] =
                history == .empty ? .offset(prior + 1) : .continuation(partitionID: partition, kind: .offset, value: prior + 1, context: history)
        }
        if noStoredOffset.contains(partition), delivered[partition] == nil, stored[partition] == nil {
            rewindPoints[partition] = (0, .empty)
        } else if let prior {
            // A stored range past `prior` makes the close store it.
            recordStored(partition, through: prior + 2, from: prior + 1, context: history)
        } else if let restored = delivered[partition].map({ $0.offset + 1 }) ?? baselines[partition]?.next {
            // The store may have moved the offset back, which a read never does. A stored range past the delivered
            // offset or the baseline makes the close write it even when the reads stored no more.
            recordStored(partition, through: restored + 1, from: restored, context: history)
        } else if rewindPoints[partition] == nil {
            rewindPoints[partition] = rewindPoint(of: partition)
        }
    }

    /// Whether the close knows what a lost seek write of `partition` replaced:
    /// a batch, a baseline, the rewind point of a lost read, or a missing
    /// stored offset.
    private func knowsRestorePoint(of partition: UInt32) -> Bool {
        stored[partition] != nil || baselines[partition] != nil || rewindPoints[partition] != nil || noStoredOffset.contains(partition)
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

    /// When the group member syncs its assignment next: one pause after the
    /// last sync, or at once after a join and after a refusal that names a
    /// partition the member does not own.
    private var nextAssignmentSync: ContinuousClock.Instant {
        guard let lastAssignmentSync, let assignmentSyncPause else {
            return .now
        }
        return lastAssignmentSync + assignmentSyncPause
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
        assignmentSyncPause = nil
        // A refusal of the earlier membership says nothing of this one, so the count of refusals as not owned and every
        // retry wait start again, as at the epoch rewind in the Rust SDK.
        notOwnedRefusals.removeAll()
        pausedUntil.removeAll()
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
            try await pause(until: ContinuousClock.now + configuration.retryInterval)
        }
    }

    /// Pauses a read call until `deadline`. The close ends the pause at once,
    /// so neither a waiting `next()` nor the close waits for it.
    private func pause(until deadline: ContinuousClock.Instant) async throws {
        _ = try await closeStart.acquire(1, deadline: deadline)
        try Task.checkCancellation()
    }

    /// Joins again once the membership was lost, waits for the sync of a
    /// member that holds no partitions to read under, and groups the
    /// partitions by primary the first time. A later sync that is due starts
    /// here without a wait, and the later lookups also run beside the reads.
    /// So while a later sync fails or hangs, the reads go on under the last
    /// assignment. Returns false when nothing can be read yet: a member
    /// without partitions once its next sync is due, else after the retry
    /// interval. A failure is queued for ``next()``.
    private func prepare() async -> Bool {
        do {
            if isGroup {
                let session = client.core.groupState.sessionScope
                if configuration.autoJoinConsumerGroup, !joinedGroup || joinedSessionScope != session {
                    try await joinGroup()
                } else if !configuration.autoJoinConsumerGroup, joinedSessionScope != session {
                    // The application joins this member, so no join of the reader starts the count of refusals as not
                    // owned and the retry waits again. A new session does.
                    joinedSessionScope = session
                    notOwnedRefusals.removeAll()
                    pausedUntil.removeAll()
                }
                if isClosed {
                    return false
                }
                let syncIsPending = assignmentSync != nil || nextAssignmentSync <= .now
                if !client.core.groupState.hasAssignment(groupKey) || (assigned.isEmpty && syncIsPending) {
                    guard await syncAssignment().value else {
                        if !isClosed {
                            try? await pause(until: ContinuousClock.now + configuration.retryInterval)
                        }
                        return false
                    }
                } else if syncIsPending {
                    _ = syncAssignment()
                }
                // The end of a sync in flight takes the assignment and sets the pause before the next sync from what
                // it changed. A pass that took a new assignment first would hide that change from it.
                if assignmentSync == nil {
                    try takeAssignment(synced: false)
                }
            }
            if assigned.isEmpty {
                try await pause(until: isGroup ? nextAssignmentSync : ContinuousClock.now + configuration.retryInterval)
                return false
            }
            if needsRegroup, fetchGroups.isEmpty {
                // Without groups nothing is read, so the first lookup runs here and its failure goes to the caller.
                let epoch = groupingEpoch
                fetchGroups = try await client.readinessGroups(streamID: streamID, topicID: topicID, partitions: groupingOrder)
                groupedEpoch = epoch
            }
            if configuration.options.commitsOnRead {
                let now = ContinuousClock.now
                resumeUnblocked()
                // A sync beside the reads can take a partition away meanwhile. A stopped partition reads nothing
                // until a seek, and a lookup of it would send the refused read again on each pass.
                for partition in assigned.sorted()
                where assigned.contains(partition) && activeReads[partition] == nil && (pausedUntil[partition] ?? now) <= now
                    && awaitingLookup[partition] == nil && !stopped.contains(partition)
                {
                    // The close waits for this call, so a lookup after the close start only delays it.
                    guard !isClosed else {
                        break
                    }
                    let generation = generations[partition]
                    do {
                        try await findStart(of: partition)
                    } catch let error as CancellationError {
                        throw error
                    } catch {
                        // It fails as a read of its partition does, not the whole read pass.
                        if generations[partition] == generation {
                            failed([partition], with: Self.iggyError(error))
                        }
                    }
                }
            }
            return true
        } catch is CancellationError {
            return false
        } catch {
            refuseRefresh(error)
            try? await pause(until: ContinuousClock.now + configuration.retryInterval)
            return false
        }
    }

    /// Takes the assignment that the client holds for the member. After a
    /// sync, one that finds the same partitions doubles the pause before the
    /// next sync.
    private func takeAssignment(synced: Bool) throws {
        let state = client.core.groupState
        if synced {
            lastAssignmentSync = .now
        }
        guard state.isRegistered(groupKey) else {
            throw IggyError(.consumerGroupMemberNotFound, context: "group \(consumer.id) in topic \(topicID) of stream \(streamID)")
        }
        let current = Set(state.assignedPartitions(groupKey) ?? [])
        if synced {
            if let pause = assignmentSyncPause, current == assigned {
                assignmentSyncPause = Swift.min(pause * 2, Self.assignmentCheckInterval)
            } else {
                assignmentSyncPause = Self.assignmentSettlePause
            }
        }
        if current != assigned {
            reassign(current)
        }
    }

    /// The assignment sync in flight, or a new one. It runs as a task of its
    /// own, so ``dispatch()`` starts a due sync also while batches wait for
    /// ``next()``.
    private func syncAssignment() -> Task<Bool, Never> {
        if let assignmentSync {
            return assignmentSync
        }
        let sync = Task { [weak self, client, streamID, topicID, groupID = consumer.id] in
            let synced: Result<Void, any Error>
            do {
                try await client.syncGroupAssignment(streamID: streamID, topicID: topicID, groupID: groupID, lineage: nil)
                synced = .success(())
            } catch {
                synced = .failure(error)
            }
            return await self?.assignmentSynced(synced) ?? false
        }
        assignmentSync = sync
        return sync
    }

    /// Takes the assignment of a sync. After a failed sync the next one
    /// waits for the retry interval.
    private func assignmentSynced(_ synced: Result<Void, any Error>) -> Bool {
        assignmentSync = nil
        guard !isClosed else {
            return false
        }
        defer {
            dispatch()
            readerWakeup?.yield()
        }
        do {
            try synced.get()
            try takeAssignment(synced: true)
            return true
        } catch {
            lastAssignmentSync = .now
            assignmentSyncPause = configuration.retryInterval
            refuseRefresh(error)
            return false
        }
    }

    /// A failed join, sync or first grouping, as `refuse_refresh` in the Rust
    /// SDK: a lost membership joins again, and a refusal that settles by
    /// itself is not reported.
    private func refuseRefresh(_ error: any Error) {
        let code = (error as? IggyError)?.code
        if let code, [.consumerGroupMemberNotFound, .disconnected, .unauthenticated, .staleClient].contains(code) {
            joinedGroup = false
        }
        let failure = metadata.merging(["error": "\(error)"]) { $1 }
        if let code, Self.transientRefusals.contains(code) {
            logger.warning("Failed to join the consumer group or to group its partitions", metadata: failure)
            return
        }
        logger.error("Failed to join the consumer group or to group its partitions", metadata: failure)
        if code == .consumerGroupMemberNotFound, configuration.autoJoinConsumerGroup {
            return
        }
        report(ReadError(partitionID: nil, error: Self.iggyError(error)))
    }

    /// With `onRead`, before the first committing read of `partition`: looks
    /// up its stored offset for a start other than `next`, then reads the
    /// baseline of a partition that reads at `next`. A seek, a new session or
    /// another lookup of the partition meanwhile drops the result.
    private func findStart(of partition: UInt32) async throws {
        if needsStartLookup(partition) {
            let (generation, session) = (generations[partition], client.core.groupState.sessionScope)
            let offset = try await client.getConsumerOffset(consumer: consumer, streamID: streamID, topicID: topicID, partitionID: partition)
            if isCurrent(partition, generation: generation, session: session), needsStartLookup(partition) {
                if offset == nil {
                    noStoredOffset.insert(partition)
                } else {
                    positions[partition] = .next
                }
            }
        }
        guard !isClosed, needsBaseline(partition) else {
            return
        }
        let (generation, session, position) = (generations[partition], client.core.groupState.sessionScope, positions[partition])
        let sections = try await client.fetchMessages(
            streamID: streamID, topicID: topicID, targets: [(partitionID: partition, strategy: .next)], consumer: consumer,
            count: 1, autoCommit: false, options: probeOptions)
        guard let section = sections.first(where: { $0.partitionID == partition }) else {
            throw IggyError(.cannotReadMessage, context: "the baseline read returned no section for partition \(partition)")
        }
        if let error = section.error {
            throw error
        }
        let baseline = (next: section.firstOffset ?? section.nextOffset, context: section.context)
        guard isCurrent(partition, generation: generation, session: session), positions[partition] == position else {
            return
        }
        baselines[partition] = baseline
        // A read from an offset already set where the next reply starts. The baseline would hide a jump of a lost seek write.
        expected[partition] = expected[partition] ?? baseline.next
        positions[partition] = .continuation(partitionID: partition, kind: .next, value: 0, context: baseline.context)
    }

    /// The assigned partitions in the order of their last groups, the new
    /// ones last.
    private var groupingOrder: [UInt32] {
        let kept = fetchGroups.joined().filter(assigned.contains)
        return kept + assigned.subtracting(kept).sorted()
    }

    /// Groups the partitions by primary beside the reads, which go on with
    /// the last groups. After a failed lookup the next one starts after the
    /// retry interval.
    private func lookUpGroups() {
        guard groupLookup == nil, (groupLookupRetry ?? .now) <= .now else {
            return
        }
        let epoch = groupingEpoch
        let order = groupingOrder
        groupLookup = Task { [weak self, client, streamID, topicID] in
            let groups: Result<[[UInt32]], any Error>
            do {
                groups = .success(try await client.readinessGroups(streamID: streamID, topicID: topicID, partitions: order))
            } catch {
                groups = .failure(error)
            }
            await self?.lookedUp(groups, epoch: epoch)
        }
    }

    /// A good lookup releases the partitions that waited for it. A failed one
    /// keeps the last groups and, unless its refusal is transient, is
    /// reported for ``next()``.
    private func lookedUp(_ groups: Result<[[UInt32]], any Error>, epoch: UInt64) {
        groupLookup = nil
        switch groups {
        case .success(let groups):
            fetchGroups = groups
            groupedEpoch = epoch
            groupLookupRetry = nil
            awaitingLookup = awaitingLookup.filter { $0.value > epoch }
        case .failure where isClosed:
            break
        case .failure(let error):
            groupLookupRetry = .now + configuration.retryInterval
            let failure = metadata.merging(["error": "\(error)"]) { $1 }
            if let code = (error as? IggyError)?.code, Self.transientRefusals.contains(code) {
                logger.warning("Failed to group the partitions by primary, reading on with the last groups", metadata: failure)
            } else {
                logger.error("Failed to group the partitions by primary, reading on with the last groups", metadata: failure)
                report(ReadError(partitionID: nil, error: Self.iggyError(error)))
            }
        }
        dispatch()
        readerWakeup?.yield()
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
            assignedSince[partition] = lastGeneration
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
        lostReads.remove(partition)
        blockedBy[partition] = nil
        noStoredOffset.remove(partition)
        rewindPoints[partition] = nil
        backlogged.remove(partition)
        gaps[partition] = nil
        isolations[partition] = nil
        pollAlone.remove(partition)
        pausedUntil[partition] = nil
        stopped.remove(partition)
        notOwnedRefusals[partition] = nil
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

    /// Whether a lookup or a baseline read of `partition` that started in
    /// `generation` and `session` still counts, as `complete_check` in the
    /// Rust SDK decides it. Its result also needs the position it started
    /// from.
    private func isCurrent(_ partition: UInt32, generation: UInt64?, session: UInt64) -> Bool {
        !isClosed && generations[partition] == generation && client.core.groupState.sessionScope == session
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
        // A sync in flight is past its due time and wakes the reader when it ends. A wait until that time would spin.
        var wakeAt = isGroup && assignmentSync == nil ? nextAssignmentSync : ContinuousClock.now + Self.assignmentCheckInterval
        if let pauseEnd = pausedUntil.values.min() {
            wakeAt = Swift.min(wakeAt, pauseEnd)
        }
        if needsRegroup, groupLookup == nil {
            // The next pass starts the lookup.
            wakeAt = Swift.min(wakeAt, groupLookupRetry ?? .now)
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

    /// Starts what the reader can read now, and a due lookup or sync, whether
    /// or not batches wait for ``next()``.
    private func dispatch() {
        guard !isClosed else {
            return
        }
        if isGroup, assignmentSync == nil, joinedGroup || !configuration.autoJoinConsumerGroup, nextAssignmentSync <= .now {
            _ = syncAssignment()
        }
        // A sync can move the assignment, so the lookup groups what it left.
        if needsRegroup, !fetchGroups.isEmpty, assignmentSync == nil {
            lookUpGroups()
        }
        let now = ContinuousClock.now
        pausedUntil = pausedUntil.filter { $0.value > now }
        resumeUnblocked()
        if !configuration.options.commitsOnRead || onDemand {
            fetchAhead()
        }
        pollSingles()
    }

    /// Resumes the partitions that a lost committing read blocked once that
    /// read was abandoned.
    private func resumeUnblocked() {
        for (partition, operation) in blockedBy where !client.core.pollRouter.retainsFetch(operation) {
            blockedBy[partition] = nil
            stopped.remove(partition)
        }
    }

    /// With `onRead` a read goes out only for a reader that waits with
    /// nothing queued, so the server stores a batch only once it was asked
    /// for.
    private var onDemand: Bool {
        readerWakeup != nil && arrivals.isEmpty
    }

    /// The idle partitions of a group that its fetches read now.
    private struct GroupTurn {
        let group: [UInt32]
        /// Partitions whose last section ended before their head, least
        /// recently served first.
        let backlogged: [UInt32]
        let quiet: [UInt32]
        let hasMain: Bool
        let second: UInt64?
        /// Whether the turn fetches the quiet partitions. With `onRead` they
        /// wait for the main fetch of the group in flight instead.
        let startsQuiet: Bool
        var extra = 0

        /// A turn that restarts the second fetch of its group takes no new
        /// request.
        var needsRequest: Bool { !startsQuiet || second == nil }

        /// Every fetch holds at least one partition.
        var extraLimit: Int { startsQuiet ? backlogged.count : backlogged.count - 1 }
    }

    /// Fetches the partitions of each group that no request reads, as the
    /// Rust reader schedules its groups. Every group gets one fetch before
    /// any gets more: its quiet partitions join the main fetch of the group
    /// once none is in flight, else its second fetch, which starts again
    /// with them, or with `onRead` they wait for the main fetch to end. The
    /// requests left free, less those the polls of single partitions need
    /// and those the queued replies hold, go round robin to groups with
    /// backlogged partitions, which a read answers at once, each share in a
    /// fetch of its own with the full limits.
    private func fetchAhead() {
        let commits = configuration.options.commitsOnRead
        var turns: [GroupTurn] = []
        for group in fetchGroups where waitingReplies(of: group) < Self.waitingRepliesPerGroup {
            let joining = group.filter { isIdle($0) && isFetchable($0) }
            var hasMain = false
            var second: UInt64?
            for partition in group {
                guard let id = activeReads[partition], let request = requests[id] else {
                    continue
                }
                switch request.kind {
                case .main: hasMain = true
                case .second: second = id
                case .backlog, .single, .gap, .isolate, .check: break
                }
            }
            let quiet = joining.filter { !backlogged.contains($0) }
            let turn = GroupTurn(
                group: group, backlogged: joining.filter(backlogged.contains), quiet: quiet, hasMain: hasMain, second: second,
                startsQuiet: !quiet.isEmpty && !(commits && hasMain))
            if turn.startsQuiet || !turn.backlogged.isEmpty {
                turns.append(turn)
            }
        }
        var free = Self.maxRequestsInFlight - requests.count
        var taking: [Int] = []
        for index in turns.indices.filter({ !turns[$0].hasMain }) + turns.indices.filter({ turns[$0].hasMain }) {
            if !turns[index].needsRequest {
                taking.append(index)
            } else if free > 0 {
                free -= 1
                taking.append(index)
            }
        }
        var extra = free - singlePollReserve - queuedReplies
        var dealt = true
        while extra > 0, dealt {
            dealt = false
            for index in taking where extra > 0 && turns[index].extra < turns[index].extraLimit {
                turns[index].extra += 1
                extra -= 1
                dealt = true
            }
        }
        for index in taking {
            startFetches(of: turns[index])
        }
    }

    private func startFetches(of turn: GroupTurn) {
        let spread: RequestKind = configuration.options.commitsOnRead ? .backlog : .main
        guard turn.startsQuiet else {
            for share in Self.deal(turn.backlogged, into: turn.extra + 1) {
                startGroupFetch(share, of: turn.group, kind: spread)
            }
            return
        }
        let shared = turn.extra == 0 ? turn.quiet + turn.backlogged : turn.quiet
        var targets = Set(shared)
        if let second = turn.second, let request = finish(second) {
            request.task.cancel()
            targets.formUnion(request.partitions)
        }
        startGroupFetch(Array(targets), of: turn.group, kind: turn.hasMain ? .second : .main)
        if turn.extra > 0 {
            for share in Self.deal(turn.backlogged, into: turn.extra) {
                startGroupFetch(share, of: turn.group, kind: spread)
            }
        }
    }

    /// A fetch of `partitions` in the order of their group. A fetch that
    /// moves a stored offset leaves out the partitions whose store waits.
    private func startGroupFetch(_ partitions: [UInt32], of group: [UInt32], kind: RequestKind) {
        let members = Set(partitions)
        var ordered = group.filter(members.contains)
        if configuration.options.commitsOnRead || ordered.contains(where: { startPoint(of: $0).kind == .next }) {
            ordered.removeAll { storesWaiting[$0] != nil }
        }
        if !ordered.isEmpty {
            startFetch(ordered, kind: kind)
        }
    }

    /// `partitions` dealt round robin into `count` shares.
    private static func deal(_ partitions: [UInt32], into count: Int) -> [[UInt32]] {
        var shares = [[UInt32]](repeating: [], count: Swift.max(count, 1))
        for (index, partition) in partitions.enumerated() {
            shares[index % shares.count].append(partition)
        }
        return shares.filter { !$0.isEmpty }
    }

    /// The requests that the polls of single partitions waiting now take,
    /// within the share that ``pollSingles()`` keeps for them.
    private var singlePollReserve: Int {
        let limit = Swift.max(Self.maxRequestsInFlight - fetchGroups.count, 1)
        let polls = requests.values.filter { $0.kind.isPoll }.count
        let waiting = fetchGroups.joined().filter { isIdle($0) && singleRead(of: $0) != nil }.count
        return Swift.min(waiting, Swift.max(limit - polls, 0))
    }

    /// The fetch replies that wait for ``next()``. Each holds a request of
    /// the budget.
    private var queuedReplies: Int {
        Set(
            arrivals.compactMap { arrival -> UInt64? in
                if case .section(_, _, let request) = arrival { request } else { nil }
            }
        ).count
    }

    /// Polls, one partition each, the gaps, the sections that did not
    /// decode, and the partitions a fetch cannot read, taking turns when the
    /// request limit binds. A request per group stays free of them.
    private func pollSingles() {
        let partitions = Array(fetchGroups.joined())
        let limit = Swift.max(Self.maxRequestsInFlight - fetchGroups.count, 1)
        var polls = requests.values.filter { $0.kind.isPoll }.count
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
        if let isolation = isolations[partition] {
            return (.isolate, .continuation(partitionID: partition, kind: .offset, value: isolation.first, context: isolation.context), 1)
        }
        if let gap = gaps[partition] {
            let count = UInt32(clamping: Swift.min(gap.end - gap.next, UInt64(configuration.options.count)))
            return (.gap, .continuation(partitionID: partition, kind: .offset, value: gap.next, context: gap.context), count)
        }
        let start = startPoint(of: partition)
        if configuration.options.commitsOnRead, start.kind == .timestamp || start.kind == .last {
            // A committing read from there stores a range whose start no offset names, so the close could not move
            // back before it. With the count, the first message of `last` starts the last `count` messages.
            return (.check, start, start.kind == .last ? configuration.options.count : 1)
        }
        guard !isFetchable(partition), !configuration.options.commitsOnRead || onDemand else {
            return nil
        }
        return (.single, start, configuration.options.count)
    }

    private func isIdle(_ partition: UInt32) -> Bool {
        assigned.contains(partition) && activeReads[partition] == nil && pausedUntil[partition] == nil && !stopped.contains(partition)
            && awaitingLookup[partition] == nil && !needsStartLookup(partition) && !needsBaseline(partition)
    }

    /// With `onRead` the configured start applies only to a partition without
    /// a stored offset, and only `next` reads after one by itself.
    private func needsStartLookup(_ partition: UInt32) -> Bool {
        configuration.options.commitsOnRead && positions[partition] == nil && configuration.start.kind != .next
            && !noStoredOffset.contains(partition)
    }

    private func needsBaseline(_ partition: UInt32) -> Bool {
        configuration.options.commitsOnRead && baselines[partition] == nil && startPoint(of: partition).kind == .next
    }

    /// The options of a read of one message that neither waits nor commits.
    private var probeOptions: DeferredPollOptions {
        var options = configuration.options.deferred
        options.maxWait = .zero
        options.minCount = 1
        return options
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
        let targets = partitions.map { (partitionID: $0, strategy: requestStrategy(startPoint(of: $0), of: $0)) }
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
    /// stored offset while a store of the partition waits, or it is a check
    /// while a seek holds the partition and owns its position. A gap and a
    /// section that did not decode are read again without a wait and without
    /// a commit.
    @discardableResult
    private func startPoll(_ partition: UInt32, kind: RequestKind, strategy: PollingStrategy, count: UInt32) -> Bool {
        let commits = kind == .single && configuration.options.commitsOnRead
        let progresses = commits || strategy.kind == .next
        if progresses || kind == .check, storesWaiting[partition] != nil {
            return false
        }
        var options = configuration.options.deferred
        if kind == .gap || kind == .isolate {
            options.maxWait = .zero
        }
        options.minCount = 1
        let sent = requestStrategy(strategy, of: partition)
        start(kind, partitions: [partition], progresses: progresses, commits: commits, poll: (strategy, count)) {
            [client, streamID, topicID, consumer, options] in
            .polled(
                try await client.pollBody(
                    streamID: streamID, topicID: topicID, partitionID: partition, consumer: consumer, strategy: sent, count: count,
                    autoCommit: commits, options: options))
        }
        return true
    }

    private func start(
        _ kind: RequestKind, partitions: [UInt32], progresses: Bool, commits: Bool, poll: (start: PollingStrategy, count: UInt32)? = nil,
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
        let requestGenerations = Dictionary(uniqueKeysWithValues: partitions.map { ($0, generations[$0] ?? 0) })
        requests[id] = Request(
            kind: kind, partitions: partitions, progresses: progresses, commits: commits, generations: requestGenerations,
            session: client.core.groupState.sessionScope, start: poll?.start, count: poll?.count ?? 0, task: task)
        for partition in partitions {
            activeReads[partition] = id
            if commits, rewindPoints[partition] == nil {
                rewindPoints[partition] = rewindPoint(of: partition)
            }
        }
    }

    /// Where the next committing read of `partition` starts, as the close
    /// counts it: the offset it names. A `next` read has its baseline
    /// instead. A timestamp or `last` start that found no stored offset
    /// counts from zero until its check names an offset, so after a seek
    /// write with an unknown outcome the close restores that state.
    private func rewindPoint(of partition: UInt32) -> (next: UInt64, context: PartitionContext)? {
        let start = startPoint(of: partition)
        switch start.kind {
        case .offset: return (start.value, start.context ?? .empty)
        case .timestamp, .last: return noStoredOffset.contains(partition) ? (0, .empty) : nil
        case .first, .next: return nil
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
            if let operation = section.uncertainty?.operationID {
                // Abandoned first, so a seek or a new assignment meanwhile leaves no block.
                try? client.abandonOperation(operation)
            }
            guard generations[partition] == generation else {
                if request.commits, section.error == nil, let first = section.firstOffset {
                    if keeps(partition, generation) {
                        // The server stored this read, so a partition that left the assignment still hands it over.
                        arrivals.append(.section(section, generation: generation, request: id))
                    } else if assigned.contains(partition), generation >= assignedSince[partition, default: 0] {
                        // A seek of this assignment dropped the batch, so the close moves the offset back over it.
                        recordStored(partition, through: section.nextOffset, from: first, context: section.context)
                    }
                }
                continue
            }
            if let error = section.error {
                backlogged.remove(partition)
                if request.commits, error.code == .fetchProgressUncertain {
                    lost([partition], to: error)
                    failed([partition], with: error)
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
            // The fail-fast reply of the server: nothing read, and a fully zero context.
            if section.messagesCount == 0, section.context == .empty {
                notServed(partition)
                continue
            }
            sawOwner(section.context, of: partition, in: request.session)
            notOwnedRefusals[partition] = nil
            if section.messagesCount == 0 {
                idle.insert(partition)
            }
            if section.nextOffset <= section.currentOffset, section.messagesCount > 0 || section.currentOffset > 0 {
                backlogged.insert(partition)
            } else {
                backlogged.remove(partition)
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
        // Only a read after the stored offset shows a jump that a lost read or a lost seek write made.
        let showsJump = startPoint(of: partition).kind == .next
        if firstOffset != nil {
            // From here on `stored` holds what the reads stored, in their history.
            rewindPoints[partition] = nil
        }
        if onRead, lostReads.contains(partition), let expected = expected[partition], start > expected, context.incarnation != 0 {
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
                if showsJump {
                    // This store fences out any older read of the partition parked on the server.
                    lostReads.remove(partition)
                }
            }
            if firstOffset != nil || expected[partition] != nil {
                expected[partition] = end
            }
            // An empty reply stored nothing. After a write or a read with an unknown outcome, the stored offset that
            // `next` follows can lie before an offset position, so that position holds until a batch arrives.
            if firstOffset != nil || !lostReads.contains(partition) || startPoint(of: partition).kind != .offset {
                positions[partition] = firstOffset == nil && positions[partition] == nil ? nil : .next
            }
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
            if request.commits, let polled = try? Responses.polledMessages(body[...]), let first = polled.messages.first, let last = polled.messages.last {
                if keeps(partition, generation) {
                    // The server stored this read, so a partition that left the assignment still hands it over.
                    arrivals.append(.batch(polled, generation: generation))
                } else if assigned.contains(partition), generation >= assignedSince[partition, default: 0] {
                    // A seek of this assignment dropped the batch, so the close moves the offset back over it.
                    recordStored(partition, through: last.offset + 1, from: first.offset, context: polled.context)
                }
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
        // An empty reply with a fully zero context is the fail-fast reply of the server, which decides nothing.
        if polled.messages.isEmpty, polled.context == .empty {
            notServed(partition)
            return
        }
        // Only a reply in the incarnation of its gap tells what that history holds.
        if request.kind == .gap, let gap = gaps[partition], polled.context.incarnation != gap.context.incarnation {
            notServed(partition)
            return
        }
        sawOwner(polled.context, of: partition, in: request.session)
        notOwnedRefusals[partition] = nil
        switch request.kind {
        case .gap:
            guard var gap = gaps[partition] else {
                return
            }
            var batch = polled
            batch.messages.removeAll { $0.offset < gap.next || $0.offset >= gap.end }
            batch.count = UInt32(batch.messages.count)
            guard let first = batch.messages.first, let last = batch.messages.last else {
                // The history no longer holds the rest of the gap, as after retention.
                gaps[partition] = nil
                giveUp(gap, of: partition)
                return
            }
            if first.offset > gap.next {
                // The history no longer holds the start of the gap, as after retention: a read below the first kept offset starts there.
                reportLost(gap.next..<first.offset, of: partition)
            }
            gap.next = last.offset + 1
            arrivals.append(.batch(batch, generation: generation))
            if gap.next >= gap.end {
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
                stepPast(message.offset, of: partition, context: polled.context)
            }
            if polled.messages.isEmpty || isolation.first > isolation.last {
                isolations[partition] = nil
            } else {
                isolations[partition] = isolation
            }
        case .check:
            // Only a seek, a rebalance and this reply move a partition while its check is in flight, and the first two
            // cancel the check. A seek owns the position while it holds the partition, so a reply then is dropped.
            guard client.core.groupState.sessionScope == request.session, storesWaiting[partition] == nil else {
                return
            }
            guard let first = polled.messages.first else {
                pauseIdle([partition])
                return
            }
            positions[partition] = .continuation(partitionID: partition, kind: .offset, value: first.offset, context: polled.context)
        default:
            pollAlone.remove(partition)
            // An empty reply tells nothing about where the next read starts.
            guard let first = polled.messages.first, let last = polled.messages.last else {
                pauseIdle([partition])
                return
            }
            _ = accept(
                .batch(polled, generation: generation), of: partition, reachingThrough: last.offset + 1, context: polled.context,
                firstOffset: first.offset)
        }
    }

    /// A poll reply that does not decode is read again one message at a
    /// time from its start. One message read alone names the bad message:
    /// its partition stops until a seek steps over it. A committing poll
    /// stored what it read, so the next read finds that as a gap. A poll
    /// without a known start names no message and stops its partition.
    private func undecodable(_ partition: UInt32, body: [UInt8], error: any Error, of request: Request) {
        let raw = Array(body.dropFirst(Self.pollReplyHeadSize))
        if request.kind == .isolate, let isolation = isolations[partition] {
            isolations[partition] = nil
            stopped.insert(partition)
            let skip = PollingStrategy.continuation(partitionID: partition, kind: .offset, value: isolation.first + 1, context: isolation.context)
            report(ReadError(partitionID: partition, error: Self.iggyError(error), skip: skip, offset: isolation.first, raw: raw, stopsPartition: true))
            return
        }
        if request.kind == .gap, let gap = gaps[partition] {
            isolations[partition] = Span(first: gap.next, last: Swift.min(gap.end, gap.next + UInt64(request.count)) - 1, context: gap.context)
            return
        }
        if request.commits, expected[partition] != nil {
            lost([partition], to: Self.iggyError(error))
            pausedUntil[partition] = .now + configuration.retryInterval
            return
        }
        if !request.commits, let start = request.start, start.kind == .offset, request.count > 0 {
            isolations[partition] = Span(first: start.value, last: start.value + UInt64(request.count) - 1, context: start.context ?? .empty)
            return
        }
        stopped.insert(partition)
        report(ReadError(partitionID: partition, error: Self.iggyError(error), raw: raw, stopsPartition: true))
    }

    /// A message that an isolation delivered moves on the gap it lies in, or
    /// the position that a poll which did not decode left at its start.
    private func stepPast(_ offset: UInt64, of partition: UInt32, context: PartitionContext) {
        if var gap = gaps[partition] {
            gap.next = Swift.max(gap.next, offset + 1)
            if gap.next >= gap.end {
                arrivals.append(contentsOf: gap.held)
                gaps[partition] = nil
            } else {
                gaps[partition] = gap
            }
            return
        }
        let position = startPoint(of: partition)
        if !configuration.options.commitsOnRead, position.kind == .offset, position.value <= offset {
            positions[partition] = .continuation(partitionID: partition, kind: .offset, value: offset + 1, context: context)
        }
    }

    /// A reply that tells nothing about `partition`, as the fail-fast reply
    /// of the server, counts as a transient refusal: the partition reads the
    /// same range again after the retry interval, and nothing is reported.
    private func notServed(_ partition: UInt32) {
        failed([partition], with: IggyError(.transientNotAccepted, context: "the server could not serve the read", partitionID: partition))
    }

    private func failed(_ request: Request, with error: any Error) {
        if error is CancellationError {
            return
        }
        if let operation = (error as? IggyError)?.operationID, (error as? IggyError)?.code == .fetchProgressUncertain {
            try? client.abandonOperation(operation)
        }
        let partitions = request.partitions.filter { request.generations[$0] == generations[$0] }
        guard !partitions.isEmpty else {
            return
        }
        if request.commits, let error = error as? IggyError, error.code == .transientNotCommitted || error.code == .fetchProgressUncertain {
            lost(partitions, to: error)
        }
        failed(partitions, with: Self.iggyError(error))
    }

    /// A committing read or a seek write whose outcome is unknown: the close
    /// reconciles its partitions, the next read finds what it stored as a
    /// gap, and the block of a read goes now, as `abandon_lost` in the Rust
    /// SDK. A lost read reads again after the retry interval.
    private func lost(_ partitions: [UInt32], to error: IggyError) {
        uncertain.formUnion(partitions)
        lostReads.formUnion(partitions)
        if let operation = error.operationID {
            try? client.abandonOperation(operation)
        }
    }

    /// The partition moved to another member before its gap was read again,
    /// the history no longer holds the rest of the gap, or the server lost
    /// that history: those messages are lost to this reader, the batches
    /// after them are not.
    /// The error comes first, as the lost offsets precede those batches.
    private func giveUp(_ gap: Gap, of partition: UInt32) {
        reportLost(gap.next..<gap.end, of: partition)
        arrivals.append(contentsOf: gap.held)
    }

    private func reportLost(_ offsets: Range<UInt64>, of partition: UInt32) {
        let lost = offsets.lowerBound...offsets.upperBound - 1
        report(
            ReadError(
                partitionID: partition,
                error: IggyError(
                    .fetchProgressUncertain, context: "offsets \(lost.lowerBound) to \(lost.upperBound) were stored by a read whose reply was lost",
                    partitionID: partition),
                lost: lost))
    }

    /// Repairs what a failed read of `partitions` points at, and they wait
    /// for the retry interval: a moved primary groups again and waits for a
    /// good lookup instead, a refused owner syncs the assignment and waits
    /// for a good lookup too, a position without a history reads again, a
    /// lost membership joins again. Tiered data that the object store refuses
    /// for good stops its partitions without a wait. What the reader cannot
    /// repair is queued for ``next()``, and so is each
    /// ``notOwnedRefusalsPerReport``th refusal of an owner in a row. The wait
    /// after a refusal of an owner doubles with each refusal in a row, as
    /// `backoff` in the Rust SDK.
    private func failed(_ partitions: [UInt32], with error: IggyError) {
        let until = ContinuousClock.now + configuration.retryInterval
        var stops = false
        switch error.code {
        case .notPrimary:
            regroup(awaiting: partitions)
            return
        case .consumerGroupPartitionNotOwned:
            // Only a leave that a sync shows drops what the close and the next read of a partition need. Until a read
            // names an owner again, the requests of the partition name owner generation 0.
            for partition in partitions {
                owners[partition] = nil
                let refusals = notOwnedRefusals[partition, default: 0] + 1
                pausedUntil[partition] = .now + configuration.retryInterval * (1 << Swift.min(refusals - 1, Self.maxRefusalBackoffSteps))
                if refusals == Self.notOwnedRefusalsPerReport {
                    notOwnedRefusals[partition] = nil
                    report(ReadError(partitionID: partition, error: error))
                } else {
                    notOwnedRefusals[partition] = refusals
                }
            }
            // The next turn syncs the assignment. The cache of the client
            // still holds the other partitions of this member.
            lastAssignmentSync = nil
            regroup(awaiting: partitions)
            return
        case .historyUnavailable:
            for partition in partitions {
                // A gap is read in the history of its context. The server lost that history, so the gap is lost and the partition stops.
                if gaps[partition] != nil || positions[partition]?.context != nil {
                    stop(partition, with: error)
                } else {
                    forget(partition)
                    pausedUntil[partition] = until
                }
            }
            client.core.pollRouter.dropRoutes(partitions.map { PollRouter.RouteKey(stream: streamID, topic: topicID, partitionID: $0) })
            return
        case .tieredReadFailed:
            // The object store refused the read for good, so the same read
            // gets the same answer: the partition stops at the first one.
            for partition in partitions {
                stop(partition, with: error)
            }
            return
        case .consumerGroupMemberNotFound:
            joinedGroup = false
            if configuration.autoJoinConsumerGroup {
                return
            }
        case .transientNotAccepted, .transientNotCommitted, .deferredPollCapacityExceeded, .fetchProgressUncertain:
            for partition in partitions {
                pausedUntil[partition] = until
            }
            logger.warning("A read was not answered, reading again", metadata: metadata.merging(["error": "\(error)"]) { $1 })
            return
        case .partitionLaneBlocked where configuration.options.commitsOnRead:
            for partition in partitions {
                stopped.insert(partition)
                blockedBy[partition] = error.operationID
            }
            stops = true
        case .disconnected, .unauthenticated, .staleClient:
            joinedGroup = false
        default:
            break
        }
        for partition in partitions {
            pausedUntil[partition] = until
        }
        logger.error("Failed to read messages", metadata: metadata.merging(["partitions": "\(partitions)", "error": "\(error)"]) { $1 })
        report(ReadError(partitionID: partitions.count == 1 ? partitions.first : nil, error: error, stopsPartition: stops))
    }

    /// Stops `partition` until a seek and queues `error` for ``next()``. A
    /// seek drops the gap of the partition and the replies held behind it,
    /// and a seek to `next` then reads after both. So the stop gives the gap
    /// up first: its range is reported as lost and those replies are queued
    /// before `error`.
    private func stop(_ partition: UInt32, with error: IggyError) {
        if let gap = gaps.removeValue(forKey: partition) {
            giveUp(gap, of: partition)
        }
        stopped.insert(partition)
        report(ReadError(partitionID: partition, error: error, stopsPartition: true))
    }

    /// Groups the partitions again, and `partitions` read again once a lookup
    /// from now on succeeded.
    private func regroup(awaiting partitions: [UInt32]) {
        groupingEpoch += 1
        for partition in partitions {
            awaitingLookup[partition] = groupingEpoch
        }
    }

    /// Queues an error for ``next()``. The latest one of each partition and
    /// of the whole reader waits, as `pending_errors` in the Rust SDK. A
    /// lost range is the exception: it stays until ``next()`` takes it, as
    /// only its report names the offsets that the reader skipped.
    private func report(_ error: ReadError) {
        arrivals.removeAll { arrival in
            if case .failure(let queued) = arrival { queued.partitionID == error.partitionID && queued.lost == nil } else { false }
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
            try await flushCounted(for: .reader)
        } catch {
            logger.error("Failed to store the counted offsets", metadata: metadata.merging(["error": "\(error)"]) { $1 })
        }
        return !isClosed
    }

    /// Stores each counted position that is ahead of the last store of its
    /// generation, the retired ones first. Every partition is tried, and the
    /// first failure is thrown.
    private func flushCounted(for caller: WriteCaller) async throws {
        var failure: (any Error)?
        // Owner generation 0 would store the position of an owner of an earlier session for the assignment the member
        // holds now, so a partition that left stores nothing then.
        let retired = retiring.filter { !namesLostOwner($0.context, of: $0.partitionID) }
        retiring.removeAll()
        for checkpoint in retired {
            do {
                try await store(checkpoint, for: caller)
            } catch {
                failure = failure ?? error
            }
        }
        counted = counted.filter { isCurrent($0.value.checkpoint) }
        for (partition, entry) in counted {
            do {
                try await store(entry.checkpoint, for: caller)
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
    private func store(_ checkpoint: Checkpoint, for caller: WriteCaller) async throws {
        if committedCovers(checkpoint) {
            return
        }
        try await afterProgress(of: checkpoint.partitionID) {
            let writes = offsetWrites(of: checkpoint.partitionID)
            try await writes.lock()
            defer { writes.unlock() }
            if committedCovers(checkpoint) {
                return
            }
            let fence = requestContext(checkpoint.context, of: checkpoint.partitionID)
            try await writeOffset(of: checkpoint.partitionID, under: fence, generation: checkpoint.stamp?.generation, for: caller) { context in
                try await client.storeConsumerPosition(
                    consumer: consumer, streamID: streamID, topicID: topicID,
                    checkpoint: Checkpoint(partitionID: checkpoint.partitionID, offset: checkpoint.offset, context: context))
            }
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
    private func rewind(_ partition: UInt32, over span: Span, for caller: WriteCaller) async throws {
        if isGroup, !assigned.contains(partition) {
            return
        }
        let last = delivered[partition].flatMap { Self.sameHistory($0.context, span.context) && $0.offset >= span.first ? $0.offset : nil }
        if let last, last >= span.last {
            return
        }
        let generation = generations[partition]
        try await afterProgress(of: partition) {
            let writes = offsetWrites(of: partition)
            try await writes.lock()
            defer { writes.unlock() }
            try await writeOffset(of: partition, under: requestContext(span.context, of: partition), generation: generation, for: caller) { context in
                if let last {
                    try await client.storeConsumerPosition(
                        consumer: consumer, streamID: streamID, topicID: topicID,
                        checkpoint: Checkpoint(partitionID: partition, offset: last, context: context))
                } else if span.first == 0 {
                    try await deleteStoredOffset(of: partition, context: context)
                } else {
                    try await client.storeConsumerPosition(
                        consumer: consumer, streamID: streamID, topicID: topicID,
                        checkpoint: Checkpoint(partitionID: partition, offset: span.first - 1, context: context))
                }
            }
        }
    }

    /// Deletes the stored offset of `partition`, if one exists. The server
    /// refuses to delete a missing offset.
    private func deleteStoredOffset(of partition: UInt32, context: PartitionContext) async throws {
        guard try await client.getConsumerOffset(consumer: consumer, streamID: streamID, topicID: topicID, partitionID: partition) != nil else {
            return
        }
        try await client.deleteConsumerOffset(consumer: consumer, streamID: streamID, topicID: topicID, partitionID: partition, context: context)
    }

    /// Runs an offset write of `partition` under `context`, a write of the
    /// generation `generation` of the partition, nil when not known. A
    /// member's write refused as not owned is not a leave. Another member can
    /// own a partition that left, or can have read it while it was away, so
    /// the refusal stands for a partition that left the assignment and for a
    /// write of an assignment that ended. Otherwise the owner counts as lost,
    /// the member syncs its assignment, a read without a commit names the
    /// installed owner, and the write goes once more under the owner and the
    /// metadata op of that read. A rebalance by another member installs the
    /// owner at an op of that member, which no reply to this client carries,
    /// so the op that the client raises at send time stays below it. A write
    /// that names no incarnation goes again in the history of that read, any
    /// other write only in its own history. `caller` says when the retry of
    /// that read ends.
    private func writeOffset(
        of partition: UInt32, under context: PartitionContext, generation: UInt64?, for caller: WriteCaller,
        _ write: (PartitionContext) async throws -> Void
    ) async throws {
        do {
            try await write(context)
        } catch let refusal as IggyError where isGroup && refusal.code == .consumerGroupPartitionNotOwned {
            func isOfCurrentAssignment() -> Bool {
                assigned.contains(partition) && (generation.map { $0 >= assignedSince[partition, default: 0] } ?? true)
            }
            guard isOfCurrentAssignment() else {
                throw refusal
            }
            owners[partition] = nil
            lastAssignmentSync = nil
            dispatch()
            let session = client.core.groupState.sessionScope
            // The partition can leave and come back during the read, which ends the assignment of the write.
            guard let installed = await installedContext(of: partition, in: context, for: caller), isOfCurrentAssignment(),
                context.incarnation == 0 || installed.incarnation == context.incarnation
            else {
                throw refusal
            }
            sawOwner(installed, of: partition, in: session)
            try await write(installed)
        }
    }

    /// The context of a read of `partition` without a commit under owner
    /// generation 0 in the history of `context`, which the server serves for
    /// the installed owner. Only the context of the reply counts, so a
    /// message that does not decode does not fail the read. After a
    /// transient refusal it reads again once the retry interval passed,
    /// until the task of the caller is cancelled or the retry ends as
    /// `caller` says. No read starts after that end, and a pause ends at
    /// once. Nil after any other failure and once the retry ends, also when
    /// it ends while the read is in flight.
    private func installedContext(of partition: UInt32, in context: PartitionContext, for caller: WriteCaller) async -> PartitionContext? {
        let unowned = PartitionContext(incarnation: context.incarnation, ownerGeneration: 0, metadataOp: context.metadataOp)
        let end = caller == .reader ? closeStart : closeCancel
        let deadline: ContinuousClock.Instant? = if case .close(let deadline) = caller { deadline } else { nil }
        func ended() -> Bool {
            end.availablePermits > 0 || Task.isCancelled || deadline.map { ContinuousClock.now >= $0 } == true
        }
        while !ended() {
            do {
                let body = try await client.pollBody(
                    streamID: streamID, topicID: topicID, partitionID: partition, consumer: consumer,
                    strategy: .continuation(partitionID: partition, kind: .next, value: 0, context: unowned), count: 1, autoCommit: false,
                    options: probeOptions)
                // After the end of the retry the first refusal stands, so the answer of a read that was in flight sends no write.
                guard !ended() else {
                    return nil
                }
                return try Responses.polledContext(body[...])
            } catch let refusal as IggyError where Self.transientRefusals.contains(refusal.code) {
                let pause = ContinuousClock.now + configuration.retryInterval
                _ = try? await end.acquire(1, deadline: deadline.map { Swift.min($0, pause) } ?? pause)
            } catch {
                return nil
            }
        }
        return nil
    }

    /// The context that a read or an offset write of `partition` names. An
    /// owner that no read of the current session named goes as owner
    /// generation 0, which the server reads and writes for the installed
    /// owner. When the client sends a group write with owner generation 0, it
    /// raises the metadata op of the write to the newest op that it saw.
    /// After a refusal as not owned, each request of the partition names
    /// owner generation 0 until a read names an owner again. An owner below
    /// that one still goes as owner generation 0. Before such a refusal,
    /// inside one session an older owner goes out unchanged, so the server
    /// refuses a write of an earlier assignment.
    private func requestContext(_ context: PartitionContext, of partition: UInt32) -> PartitionContext {
        guard namesLostOwner(context, of: partition) else {
            return context
        }
        return PartitionContext(incarnation: context.incarnation, ownerGeneration: 0, metadataOp: context.metadataOp)
    }

    /// `strategy` with the context that a read of `partition` names.
    private func requestStrategy(_ strategy: PollingStrategy, of partition: UInt32) -> PollingStrategy {
        var sent = strategy
        sent.context = strategy.context.map { requestContext($0, of: partition) }
        return sent
    }

    /// Whether `context` names an owner that no read of the current session
    /// named. A new session installs the owner again under a higher
    /// generation, so an owner below the lowest one of the session belongs
    /// to an earlier session.
    private func namesLostOwner(_ context: PartitionContext, of partition: UInt32) -> Bool {
        guard context.ownerGeneration != 0 else {
            return false
        }
        guard let owner = owners[partition], owner.session == client.core.groupState.sessionScope else {
            return true
        }
        return context.ownerGeneration < owner.generation
    }

    /// Keeps the lowest owner that a read of `partition` named in `session`,
    /// while that session is the current one.
    private func sawOwner(_ context: PartitionContext, of partition: UInt32, in session: UInt64) {
        guard session == client.core.groupState.sessionScope, context.ownerGeneration != 0 else {
            return
        }
        if let owner = owners[partition], owner.session == session, owner.generation <= context.ownerGeneration {
            return
        }
        owners[partition] = (session, context.ownerGeneration)
    }

    private func offsetWrites(of partition: UInt32) -> AsyncMutex {
        if let lock = offsetWriteLocks[partition] {
            return lock
        }
        let lock = AsyncMutex()
        offsetWriteLocks[partition] = lock
        return lock
    }

    /// Runs a store or delete of `partition` as ``holdingReads(of:_:)``, then
    /// starts the reads that waited for it.
    private func afterProgress(of partition: UInt32, _ write: () async throws -> Void) async throws {
        defer { dispatch() }
        try await holdingReads(of: partition, write)
    }

    /// Runs a store or delete of `partition` once no read in flight reads or
    /// moves its stored offset, and no such read starts meanwhile, so the two
    /// never reorder on the server.
    private func holdingReads(of partition: UInt32, _ write: () async throws -> Void) async throws {
        hold(partition)
        defer { release(partition) }
        try await waitForReads(of: partition)
        try await write()
    }

    /// No read of `partition` that reads or moves its stored offset starts
    /// until the matching ``release(_:)``, and the close waits for it.
    private func hold(_ partition: UInt32) {
        storesWaiting[partition, default: 0] += 1
    }

    private func release(_ partition: UInt32) {
        if storesWaiting[partition] == 1 {
            storesWaiting[partition] = nil
        } else {
            storesWaiting[partition]? -= 1
        }
        // The next seek of the partition waits for this end, the close for the last one.
        for wakeup in storeWakeups {
            wakeup.finish()
        }
        storeWakeups.removeAll()
    }

    /// Ends a seek under `onRead`: the next seek of its partition takes its
    /// turn, and the reads of the partition go on.
    private func endSeek(_ seek: UInt64, of partition: UInt32) {
        seeksInFlight[partition]?.removeAll { $0 == seek }
        if seeksInFlight[partition]?.isEmpty == true {
            seeksInFlight[partition] = nil
        }
        release(partition)
        readerWakeup?.yield()
    }

    /// Waits until no read in flight reads or moves the stored offset of
    /// `partition`.
    private func waitForReads(of partition: UInt32) async throws {
        try await waitForEnds { activeReads[partition].flatMap({ requests[$0] })?.progresses != true }
    }

    /// Waits until `done` holds, checking it again at each end of a read
    /// that reads or moves a stored offset, of a store and of a seek.
    private func waitForEnds(until done: () -> Bool) async throws {
        while !done() {
            let (ended, wakeup) = AsyncStream<Void>.makeStream()
            storeWakeups.append(wakeup)
            for await _ in ended {}
            try Task.checkCancellation()
        }
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
    /// what the join throws. A join or a group create whose outcome is
    /// unknown throws ``IggyErrorCode/transientNotCommitted``. The client
    /// keeps nothing of it, so the next call starts over. A join that landed
    /// keeps the member in the group until its session ends.
    public func reader(
        streamID: Identifier, topicID: Identifier, consumer: Consumer, configuration: ReaderConfiguration = ReaderConfiguration()
    ) async throws -> PartitionReader {
        let reader = try PartitionReader(client: self, streamID: streamID, topicID: topicID, consumer: consumer, configuration: configuration)
        try await reader.start()
        return reader
    }
}
