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

import Foundation
import Logging
import Testing

@testable import Iggy

private func signedInClient(_ server: FakeVSRServer, logger: Logger = testLogger, requestTimeout: Duration = .seconds(10)) async throws -> IggyClient {
    var configuration = ClientConfiguration(address: server.address, autoLogin: .usernamePassword(username: "iggy", password: "iggy"))
    configuration.reconnection = ReconnectionOptions(maxRetries: nil, interval: .milliseconds(20), reestablishAfter: .zero)
    configuration.requestTimeout = requestTimeout
    configuration.connectTimeout = .seconds(2)
    configuration.heartbeatInterval = .seconds(60)
    let client = IggyClient(configuration: configuration, connector: NIOConnector(), logger: logger)
    try await client.connect()
    return client
}

/// Keeps the level and the metadata of each record, for a test that checks
/// what the reader logged.
private struct RecordingLogHandler: LogHandler {
    let records: Locked<[(level: Logger.Level, metadata: Logger.Metadata)]>
    var metadata: Logger.Metadata = [:]
    var logLevel: Logger.Level = .trace

    subscript(metadataKey key: String) -> Logger.Metadata.Value? {
        get { metadata[key] }
        set { metadata[key] = newValue }
    }

    func log(level: Logger.Level, message: Logger.Message, metadata: Logger.Metadata?, source: String, file: String, function: String, line: UInt) {
        let merged = self.metadata.merging(metadata ?? [:]) { $1 }
        records.withLock { $0.append((level, merged)) }
    }
}

private func requests(_ server: FakeVSRServer, _ code: CommandCode) async -> [FakeVSRServer.Request] {
    await server.requests.filter { $0.code == code.rawValue }
}

/// The strategy, count and auto-commit flag a poll carries before its
/// options: `[kind:1][value:8][count:4][auto_commit:1]`.
private func polledStrategy(_ request: FakeVSRServer.Request) -> (kind: UInt8, value: UInt64, count: UInt32, autoCommit: Bool) {
    let end = request.payload.count - FakeVSRServer.Request.deferredOptionsSize
    return (
        request.payload[end - 14], UInt64(littleEndianBytes: request.payload[end - 13..<end - 5]),
        UInt32(littleEndianBytes: request.payload[end - 5..<end - 1]),
        request.payload[end - 1] == 1
    )
}

/// The offset a store ends with: `[partition:4][offset:8][ack:1]`.
private func storedOffset(_ request: FakeVSRServer.Request) -> UInt64 {
    let end = request.payload.count
    return UInt64(littleEndianBytes: request.payload[end - 9..<end - 1])
}

/// The partition of a store, before its offset.
private func storedPartition(_ request: FakeVSRServer.Request) -> UInt32 {
    let end = request.payload.count
    return UInt32(littleEndianBytes: request.payload[end - 13..<end - 9])
}

/// The partition a poll reads, right before its strategy.
private func polledPartition(_ request: FakeVSRServer.Request) -> UInt32 {
    let end = request.payload.count - FakeVSRServer.Request.deferredOptionsSize
    return UInt32(littleEndianBytes: request.payload[end - 18..<end - 14])
}

/// Whether a request reads one message without a commit, as the read that
/// names the installed owner after an offset write refused as not owned.
private func isOwnerRead(_ request: FakeVSRServer.Request) -> Bool {
    guard request.code == CommandCode.pollMessagesDeferred.rawValue else {
        return false
    }
    let strategy = polledStrategy(request)
    return strategy.count == 1 && !strategy.autoCommit
}

/// Whether a log record reports a refusal as not owned.
private func reportsRefusalAsNotOwned(_ record: (level: Logger.Level, metadata: Logger.Metadata)) -> Bool {
    record.level >= .warning && "\(record.metadata["error"] ?? "")".contains("consumer_group_partition_not_owned")
}

/// A topic as the server reads it. Partition `p` holds the messages at
/// `0..<ends[p]`, and the message at offset `o` carries timestamp `o`. A
/// `next` read starts after the stored offset, a read that
/// commits moves the stored offset forward to the last offset it returns, a
/// store past the last message and a delete of a missing offset fail, and a
/// read that finds nothing
/// waits for a message until its wait ends. A fetch target that starts
/// other than at an offset or at `next` fails the fetch with
/// `invalidCommand`. A message in `undecodable` does not decode. A message
/// in `oversized` comes alone to a poll, and a fetch refuses its section
/// with `invalidSizeBytes`. `losesReply` drops the reply of that many
/// committing fetches and polls after they stored their offsets. As rule H2
/// of the server, a poll, a store, a delete, a lookup and a fetch target
/// that name incarnation 0 fail with `historyUnavailable`.
private final class FakeTopic: Sendable {
    private struct State {
        var ends: [UInt32: UInt64]
        var stored: [UInt32: UInt64] = [:]
        var assignment: [UInt32]
        var generation: UInt64 = 1
        var undecodable: Set<UInt64> = []
        var oversized: Set<UInt64> = []
        var losesReply = 0
        var reportsHead = false
        var ownerGeneration: UInt64 = 0
        var ownerInstalledAt: UInt64 = 0

        /// The context of a read reply, which names the installed owner.
        var context: PartitionContext {
            PartitionContext(
                incarnation: FakeVSRServer.routeContext.incarnation, ownerGeneration: ownerGeneration, metadataOp: FakeVSRServer.routeContext.metadataOp)
        }

        func start(_ partition: UInt32, kind: PollingStrategy.Kind, value: UInt64, count: UInt32) -> UInt64 {
            let end = ends[partition, default: 0]
            switch kind {
            case .next: return stored[partition].map { $0 + 1 } ?? 0
            case .offset: return value
            case .last: return end > UInt64(count) ? end - UInt64(count) : 0
            case .first: return 0
            case .timestamp: return value
            }
        }

        func holds(_ partition: UInt32, from start: UInt64) -> Bool {
            start < ends[partition, default: 0]
        }

        mutating func read(_ partition: UInt32, from start: UInt64, count: UInt64, commits: Bool) -> [UInt64] {
            var end = Swift.min(ends[partition, default: 0], start + count)
            if oversized.contains(start) {
                end = Swift.min(end, start + 1)
            } else if let large = oversized.filter({ $0 > start && $0 < end }).min() {
                end = large
            }
            guard start < end else {
                return []
            }
            if commits {
                stored[partition] = Swift.max(stored[partition] ?? 0, end - 1)
            }
            return Array(start..<end)
        }
    }

    private static let waitStep: Duration = .milliseconds(10)
    private static let historyFenced: Set<CommandCode> = [
        .pollMessages, .pollMessagesDeferred, .storeConsumerOffset, .deleteConsumerOffset, .getConsumerOffset,
    ]
    private let state: Locked<State>

    /// With `reportsHead` a fetch section carries the newest offset of its
    /// partition, as the server sends it, else the last offset it read.
    init(
        ends: [UInt32: UInt64], assignment: [UInt32] = [1], undecodable: Set<UInt64> = [], oversized: Set<UInt64> = [], losesReply: Int = 0,
        reportsHead: Bool = false
    ) {
        state = Locked(State(ends: ends, assignment: assignment, undecodable: undecodable, oversized: oversized))
        state.withLock { state in
            state.losesReply = losesReply
            state.reportsHead = reportsHead
        }
    }

    var stored: [UInt32: UInt64] {
        state.withLock { $0.stored }
    }

    func append(_ count: UInt64, to partition: UInt32) {
        state.withLock { $0.ends[partition, default: 0] += count }
    }

    func loseReplies(_ count: Int) {
        state.withLock { $0.losesReply = count }
    }

    /// Moves the stored offset of `partition` as a store of another client does.
    func store(_ offset: UInt64, for partition: UInt32) {
        state.withLock { $0.stored[partition] = offset }
    }

    /// Removes the stored offset of `partition` as a delete of another client does.
    func deleteStored(for partition: UInt32) {
        state.withLock { _ = $0.stored.removeValue(forKey: partition) }
    }

    /// Installs the owner of the member again with `generation` at metadata
    /// op `op`, as the server does after a session change. A read, a store
    /// or a delete with an owner generation other than 0 and this one fails
    /// with `consumerGroupPartitionNotOwned`, and so does a store or a delete
    /// with owner generation 0 that routes by an op before `op`. A join
    /// commits at `op`.
    func installOwner(_ generation: UInt64, at op: UInt64 = 0) {
        state.withLock { state in
            state.ownerGeneration = generation
            state.ownerInstalledAt = op
        }
    }

    /// A rebalance: the member holds `partitions` in the next generation.
    func assign(_ partitions: [UInt32]) {
        state.withLock { state in
            state.assignment = partitions
            state.generation += 1
        }
    }

    /// The answer to an assignment sync, a store, a delete, a poll or a
    /// fetch. Nil for any other request.
    func reply(to request: FakeVSRServer.Request) async -> FakeVSRServer.Reply? {
        let code = request.code.flatMap(CommandCode.init(rawValue:))
        if let code, Self.historyFenced.contains(code), request.context.incarnation == 0 {
            return .status(IggyErrorCode.historyUnavailable.rawValue)
        }
        switch code {
        case .syncConsumerGroup:
            let (generation, partitions) = state.withLock { ($0.generation, $0.assignment) }
            return .ok(FakeVSRServer.assignmentBody(generation: generation, partitions: partitions))
        case .getConsumerOffset:
            let partition = UInt32(littleEndianBytes: request.payload.suffix(4))
            return state.withLock { state in
                guard let offset = state.stored[partition] else {
                    return .ok([])
                }
                var writer = ByteWriter()
                writer.write(partition)
                writer.write(state.ends[partition, default: 1] - 1)
                writer.write(offset)
                return .ok(writer.bytes)
            }
        case .joinConsumerGroup:
            return .committed([], commit: state.withLock { $0.ownerInstalledAt })
        case .getPartitionRoute:
            // A route that a client reads after a session change carries the metadata of that change.
            let route = FakeVSRServer.routeContext
            let op = Swift.max(route.metadataOp, state.withLock { $0.ownerInstalledAt })
            return .ok(
                FakeVSRServer.partitionRouteBody(PartitionContext(incarnation: route.incarnation, ownerGeneration: route.ownerGeneration, metadataOp: op)))
        case .storeConsumerOffset where !owns(request), .deleteConsumerOffset where !owns(request), .pollMessagesDeferred where !owns(request),
            .fetchMessages where !owns(request):
            return .status(IggyErrorCode.consumerGroupPartitionNotOwned.rawValue)
        case .storeConsumerOffset:
            let (partition, offset) = (storedPartition(request), storedOffset(request))
            let accepted = state.withLock { state in
                guard offset < state.ends[partition, default: 0] else {
                    return false
                }
                state.stored[partition] = offset
                return true
            }
            return accepted ? .ok([]) : .status(IggyErrorCode.invalidOffset.rawValue)
        case .deleteConsumerOffset:
            let end = request.payload.count
            let partition = UInt32(littleEndianBytes: request.payload[end - 5..<end - 1])
            let deleted = state.withLock { $0.stored.removeValue(forKey: partition) != nil }
            return deleted ? .ok([]) : .status(IggyErrorCode.consumerOffsetNotFound.rawValue)
        case .pollMessagesDeferred:
            return await poll(request)
        case .fetchMessages:
            return await fetch(request)
        default:
            return nil
        }
    }

    /// A read without an owner generation reads for the installed owner. A
    /// store or a delete without one binds to it only when it routes by an op
    /// at or after the install, as rule H1 of the server does.
    private func owns(_ request: FakeVSRServer.Request) -> Bool {
        let (installed, installedAt) = state.withLock { ($0.ownerGeneration, $0.ownerInstalledAt) }
        let writes = request.code == CommandCode.storeConsumerOffset.rawValue || request.code == CommandCode.deleteConsumerOffset.rawValue
        let generations = [request.context.ownerGeneration] + ((try? request.fetch)?.targets.map(\.ownerGeneration) ?? [])
        return generations.allSatisfy { $0 == installed || $0 == 0 && (!writes || installedAt <= request.context.metadataOp) }
    }

    private func poll(_ request: FakeVSRServer.Request) async -> FakeVSRServer.Reply {
        let partition = polledPartition(request)
        let poll = polledStrategy(request)
        guard let kind = PollingStrategy.Kind(rawValue: poll.kind) else {
            return .status(IggyErrorCode.invalidCommand.rawValue)
        }
        await waitForMessages(upTo: request.deferredOptions.wait) { state in
            state.holds(partition, from: state.start(partition, kind: kind, value: poll.value, count: poll.count))
        }
        let (offsets, undecodable, losesReply, context) = state.withLock { state in
            let offsets = state.read(
                partition, from: state.start(partition, kind: kind, value: poll.value, count: poll.count), count: UInt64(poll.count),
                commits: poll.autoCommit)
            let losesReply = poll.autoCommit && state.losesReply > 0
            if losesReply {
                state.losesReply -= 1
            }
            return (offsets, !state.undecodable.isDisjoint(with: offsets), losesReply, state.context)
        }
        if losesReply {
            return .close
        }
        let body = FakeVSRServer.polledBody(partitionID: partition, offsets: offsets, context: context)
        // A record cut short does not decode.
        return .ok(undecodable ? Array(body.dropLast(4)) : body)
    }

    /// Reads the targets in request order while the count leaves room.
    private func fetch(_ request: FakeVSRServer.Request) async -> FakeVSRServer.Reply {
        guard let fetch = try? request.fetch, fetch.targets.allSatisfy({ $0.kind == .offset || $0.kind == .next }) else {
            return .status(IggyErrorCode.invalidCommand.rawValue)
        }
        await waitForMessages(upTo: fetch.wait) { state in
            fetch.targets.contains { state.holds($0.partitionID, from: state.start($0.partitionID, kind: $0.kind, value: $0.value, count: fetch.count)) }
        }
        let (sections, losesReply) = state.withLock { state in
            var room = UInt64(fetch.count)
            let sections = fetch.targets.map { target in
                guard target.incarnation != 0 else {
                    return FakeVSRServer.FetchedSection(partitionID: target.partitionID, status: IggyErrorCode.historyUnavailable.rawValue)
                }
                let start = state.start(target.partitionID, kind: target.kind, value: target.value, count: fetch.count)
                if state.oversized.contains(start), state.holds(target.partitionID, from: start) {
                    return FakeVSRServer.FetchedSection(partitionID: target.partitionID, status: IggyErrorCode.invalidSizeBytes.rawValue)
                }
                let offsets = state.read(target.partitionID, from: start, count: room, commits: fetch.autoCommit)
                room -= UInt64(offsets.count)
                var section = FakeVSRServer.FetchedSection(
                    partitionID: target.partitionID, offsets: offsets, nextOffset: offsets.last.map { $0 + 1 } ?? start,
                    currentOffset: state.reportsHead ? state.ends[target.partitionID].map { $0 > 0 ? $0 - 1 : 0 } : nil, context: state.context)
                if !state.undecodable.isDisjoint(with: offsets) {
                    section.records = [1, 2, 3]
                }
                return section
            }
            let losesReply = fetch.autoCommit && state.losesReply > 0
            if losesReply {
                state.losesReply -= 1
            }
            return (sections, losesReply)
        }
        return losesReply ? .close : .ok(FakeVSRServer.fetchedBody(sections))
    }

    private func waitForMessages(upTo wait: UInt64, until holds: (State) -> Bool) async {
        let deadline = ContinuousClock.now + .microseconds(wait)
        while ContinuousClock.now < deadline, !state.withLock({ holds($0) }) {
            try? await Task.sleep(for: Self.waitStep)
        }
    }
}

/// Answers as a server that holds `topic` does: the stream, the topic and
/// the group exist. `script` answers first, where it returns a reply.
private func serve(
    _ server: FakeVSRServer, topic: FakeTopic, script: @escaping @Sendable (FakeVSRServer.Request) async -> FakeVSRServer.Reply? = { _ in nil }
) async {
    await server.respond { request in
        if let reply = await script(request) {
            return reply
        }
        if let reply = await topic.reply(to: request) {
            return reply
        }
        switch request.code.flatMap(CommandCode.init(rawValue:)) {
        case .getStream: return .ok(GoldenFixture.shared.bytes("response.get_stream"))
        case .getTopic: return .ok(GoldenFixture.shared.bytes("response.get_topic"))
        case .getConsumerGroup, .createConsumerGroup: return .ok(GoldenFixture.shared.bytes("response.consumer_group_details"))
        default: return FakeVSRServer.defaultReply(request, address: server.address)
        }
    }
}

/// The fetches the server got, decoded.
private func fetches(_ server: FakeVSRServer) async throws -> [FakeVSRServer.Request.Fetch] {
    try await requests(server, .fetchMessages).map { try $0.fetch }
}

/// The committing reads of `partition` that the server got, fetches and
/// polls of one partition.
private func committingReads(_ server: FakeVSRServer, of partition: UInt32) async throws -> Int {
    let fetched = try await fetches(server).filter { $0.autoCommit && $0.targets.contains { $0.partitionID == partition } }
    let polled = await requests(server, .pollMessagesDeferred).filter { polledPartition($0) == partition && polledStrategy($0).autoCommit }
    return fetched.count + polled.count
}

/// A script whose first `write`, a store or a delete, reaches `topic` and
/// answers `transientNotCommitted`, so its outcome is unknown. A replay of
/// that write proves nothing either. The later writes go to `topic`.
private func firstWriteWithUnknownOutcome(
    _ topic: FakeTopic, _ write: CommandCode = .storeConsumerOffset
) -> @Sendable (FakeVSRServer.Request) async -> FakeVSRServer.Reply? {
    let uncertainWrite = Locked<UInt64?>(nil)
    return { request in
        guard request.code == write.rawValue else {
            return nil
        }
        let first = uncertainWrite.withLock { written in
            if written == nil {
                written = request.requestID
                return true
            }
            return false
        }
        if first {
            _ = await topic.reply(to: request)
            return .status(IggyErrorCode.transientNotCommitted.rawValue)
        }
        return uncertainWrite.withLock { $0 } == request.requestID ? .status(IggyErrorCode.unauthorized.rawValue) : nil
    }
}

/// A script that answers the first read without a commit, the gap read of a
/// lost read, with `reply`. The later reads go to the topic.
private func firstGapRead(answeredWith reply: [UInt8]) -> @Sendable (FakeVSRServer.Request) async -> FakeVSRServer.Reply? {
    let answered = Locked(false)
    return { request in
        guard request.code == CommandCode.pollMessagesDeferred.rawValue, !polledStrategy(request).autoCommit else {
            return nil
        }
        let first = answered.withLock { done in
            defer { done = true }
            return !done
        }
        return first ? .ok(reply) : nil
    }
}

/// How long a test waits to see that a read, a close or a wake-up did not
/// happen. A read that is free to start starts well within it.
private let quietPeriod: Duration = .milliseconds(300)

/// How long a test lets a call reach its wait inside the reader, far below
/// the idle check of ``PartitionReader/assignmentCheckInterval``.
private let waitEntryPause: Duration = .milliseconds(50)

/// When a seek runs beside a committing read whose batch then waits in the
/// queue.
enum QueuedBatchSeek: CaseIterable {
    case beforeTheRead
    case whileTheReadWaits
    case whileTheReadAndTheCloseWait
}

/// Where the read that names the owner of a refused seek write stands
/// when the close starts.
enum OwnerReadAtTheCloseStart: CaseIterable {
    /// The refusal of the write comes after the close start.
    case notStarted
    /// The read waits for its reply.
    case inFlight
    /// The read waits for the retry interval after a transient refusal.
    case paused
}

private func read(_ consumer: IggyConsumer, _ count: Int) async throws -> [UInt64] {
    var offsets: [UInt64] = []
    while offsets.count < count, let received = try await consumer.next() {
        offsets.append(received.message.offset)
    }
    return offsets
}

/// The offsets of the next `count` batches of `reader`.
private func batches(_ reader: PartitionReader, _ count: Int) async throws -> [[UInt64]] {
    var batches: [[UInt64]] = []
    while batches.count < count, let batch = try await reader.next() {
        batches.append(batch.messages.map(\.offset))
    }
    return batches
}

/// The error `reader` throws next, or nil when it returns a batch or nothing
/// within two seconds.
private func readError(_ reader: PartitionReader) async -> ReadError? {
    do {
        _ = try await withDeadline(.now + .seconds(2)) { try await reader.next() }
    } catch let error as ReadError {
        return error
    } catch {}
    return nil
}

/// The retry interval of the readers that ``standalone(_:partitions:start:count:commit:)``
/// builds, and the default of ``member(_:commit:maxWait:start:retryInterval:)``.
private let readerRetryInterval: Duration = .milliseconds(50)

/// A commit interval that no test outlives, so only a rebalance and the
/// close store counted positions.
private let unendingCommitInterval: Duration = .seconds(3600)

/// The metadata op of a session change in the tests, after the op of every
/// partition context that the fake server hands out.
private let sessionChangeOp = FakeVSRServer.routeContext.metadataOp + 10

/// The context of a group offset write without an owner once a join at
/// ``sessionChangeOp`` installed the owner: the incarnation of the route and
/// the op of that join. The fake server binds it to the installed owner, so
/// such a first write lands without the read and the second write that a
/// refusal starts.
private let ownerlessWriteContext = PartitionContext(incarnation: FakeVSRServer.routeContext.incarnation, ownerGeneration: 0, metadataOp: sessionChangeOp)

/// A new session of `client` as the server makes it: the member joins
/// again, the join installs its owner with `generation` at the op of the
/// session change, and the join reply raises the watermark of the client to
/// that op.
private func changeSession(of client: IggyClient, _ topic: FakeTopic, owner generation: UInt64) async throws {
    client.core.groupState.clearSessionScoped()
    topic.installOwner(generation, at: sessionChangeOp)
    try await client.joinConsumerGroup(streamID: 1, topicID: 2, groupID: 3)
}

private func standalone(
    _ client: IggyClient, partitions: [UInt32] = [1], start: PollingStrategy = .next, count: UInt32 = 2, commit: CommitPolicy = .manual
) async throws -> PartitionReader {
    try await client.reader(
        streamID: 1, topicID: 2, consumer: .consumer(9),
        configuration: ReaderConfiguration(
            partitions: partitions, start: start, options: PollOptions(count: count, maxWait: .milliseconds(200), commit: commit),
            retryInterval: readerRetryInterval))
}

/// A member of group 3 that reads two messages a time.
private func member(
    _ client: IggyClient, commit: CommitPolicy, maxWait: Duration = .milliseconds(200), start: PollingStrategy = .next,
    retryInterval: Duration = readerRetryInterval
) async throws -> PartitionReader {
    try await client.reader(
        streamID: 1, topicID: 2, consumer: .group(3),
        configuration: ReaderConfiguration(
            start: start, options: PollOptions(count: 2, maxWait: maxWait, commit: commit), retryInterval: retryInterval))
}

private let memberGroupKey = ConsumerGroupState.GroupKey(stream: 1, topic: 2, group: 3)

@Suite("Partition reader and consumer", .serialized, .timeLimit(.minutes(1)))
struct ConsumerTests {
    // MARK: Single polls

    @Test func aSinglePollRefusesAutoCommitAndAContinuationOfAnotherPartition() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 3])
        await serve(server, topic: topic)
        let client = try await signedInClient(server)
        await expectCode(.invalidConfiguration) {
            _ = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: 1, strategy: .first, options: PollOptions(commit: .auto(.default)))
        }
        let polled = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: 1, strategy: .first, options: PollOptions(count: 2))
        #expect(polled.messages.map(\.offset) == [0, 1])
        #expect(polled.checkpoint?.offset == 1 && polled.checkpoint(at: 0)?.offset == 0 && polled.checkpoint(at: 2) == nil)
        await expectCode(.invalidCommand) {
            _ = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: 2, strategy: polled.continuation)
        }
        let rest = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: 1, strategy: polled.continuation)
        #expect(rest.messages.map(\.offset) == [2])
        let empty = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: 1, strategy: rest.continuation)
        #expect(empty.messages.isEmpty && empty.continuation.kind == .offset && empty.continuation.value == 3)
        // Every poll is deferred, with the request timeout ten seconds past
        // its wait, less what routing took.
        let sent = await requests(server, .pollMessagesDeferred)
        #expect(sent.count == 3 && sent.allSatisfy { (9_000_000...10_000_000).contains($0.deferredOptions.timeout) })
        await client.shutdown()
        await server.stop()
    }

    /// The server refuses a request that names incarnation 0 as
    /// `historyUnavailable`. So a poll, a store, a lookup and a delete whose
    /// context names no incarnation take the one of the route and keep their
    /// owner and op. A fully zero context takes the route context.
    @Test(arguments: [PartitionContext.empty, PartitionContext(incarnation: 0, ownerGeneration: 0, metadataOp: 3)])
    func aRequestWhoseContextNamesNoIncarnationNamesTheOneOfTheRoute(context: PartitionContext) async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 2])
        await serve(server, topic: topic)
        let client = try await signedInClient(server)
        let route = FakeVSRServer.routeContext
        let named = context == .empty ? route : PartitionContext(incarnation: route.incarnation, ownerGeneration: 0, metadataOp: context.metadataOp)
        let checkpoint = Checkpoint(partitionID: 1, offset: 1, context: context)

        let polled = try await client.pollMessages(
            streamID: 1, topicID: 2, partitionID: 1, consumer: .consumer(9),
            strategy: .continuation(partitionID: 1, kind: .offset, value: 0, context: context), options: PollOptions(count: 2))
        try await client.storeConsumerPosition(consumer: .consumer(9), streamID: 1, topicID: 2, checkpoint: checkpoint)
        let next = try await client.consumerOffsetNext(consumer: .consumer(9), streamID: 1, topicID: 2, partitionID: 1, context: context)
        try await client.deleteConsumerPosition(consumer: .consumer(9), streamID: 1, topicID: 2, checkpoint: checkpoint)

        #expect(polled.messages.map(\.offset) == [0, 1])
        #expect(next == 2)
        #expect(topic.stored[1] == nil)
        for code in [CommandCode.pollMessagesDeferred, .storeConsumerOffset, .getConsumerOffset, .deleteConsumerOffset] {
            #expect(await requests(server, code).map(\.context) == [named], "\(code)")
        }
        await client.shutdown()
        await server.stop()
    }

    // MARK: Reading

    /// The partitions of one primary share a fetch, and the ones a fetch
    /// served go last in the next one, so a backlog does not starve another
    /// partition.
    @Test func aReaderFetchesItsPartitionsTogetherAndServedOnesGoLast() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 6, 2: 6])
        await serve(server, topic: topic)
        let client = try await signedInClient(server)
        let reader = try await standalone(client, partitions: [1, 2], start: .first)
        var partitions: [UInt32] = []
        for _ in 0..<3 {
            partitions.append(try #require(try await reader.next()).partitionID)
        }
        #expect(partitions == [1, 2, 1])
        let sent = try await fetches(server)
        #expect(sent.first?.targets.map(\.partitionID) == [1, 2])
        #expect(sent.dropFirst().first?.targets.map(\.partitionID) == [2, 1])
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    @Test(arguments: [false, true])
    func thirtyTwoPrimariesWaitConcurrentlyWhileOneDrains(onRead: Bool) async throws {
        let primaryCount = 32
        let partitions = (1...primaryCount).map(UInt32.init)
        let topic = FakeTopic(ends: [:], reportsHead: true)
        var started: [FakeVSRServer] = []
        for _ in partitions {
            started.append(try await FakeVSRServer.started())
        }
        let primaries = started
        let roster = FakeVSRServer.rosterBody(primaries.map(\.address))
        let replies = partitions.map { _ in AsyncSemaphore(permits: 0) }
        for (index, primary) in primaries.enumerated() {
            let reply = replies[index]
            await serve(primary, topic: topic) { request in
                switch request.code.flatMap(CommandCode.init(rawValue:)) {
                case .getClusterMetadata:
                    return .ok(roster)
                case .getPartitionRoute:
                    let partition = UInt32(littleEndianBytes: request.payload.suffix(4))
                    return .ok(
                        FakeVSRServer.partitionRouteBody(
                            FakeVSRServer.routeContext, partitionsCount: UInt32(primaryCount), primaryReplica: UInt8(partition - 1)))
                case .fetchMessages:
                    if let fetch = try? request.fetch, fetch.wait == 0 {
                        return nil
                    }
                    guard (try? await reply.acquire(1)) == true else {
                        return .status(IggyErrorCode.transientNotAccepted.rawValue)
                    }
                    return nil
                default:
                    return nil
                }
            }
        }
        let client = try await signedInClient(primaries[0])
        let reader = try await standalone(client, partitions: partitions, start: .offset(0), commit: onRead ? .onRead : .manual)
        let reading = Task {
            try await withDeadline(.now + .seconds(8)) { try await batches(reader, 3) }
        }
        let allWaiting = await eventually {
            for primary in primaries {
                if await requests(primary, .fetchMessages).isEmpty {
                    return false
                }
            }
            return true
        }
        #expect(allWaiting, "Every primary has a fetch in flight before any fetch replies")
        if allWaiting {
            let busyPartition = UInt32(primaryCount)
            topic.append(6, to: busyPartition)
            replies[primaryCount - 1].release(3)
            #expect(try await reading.value == [[0, 1], [2, 3], [4, 5]])
            #expect(topic.stored[busyPartition] == (onRead ? 5 : nil))
            for primary in primaries.dropLast() {
                #expect(await requests(primary, .fetchMessages).count == 1, "The quiet primary keeps its waiting fetch")
            }
        }
        reading.cancel()
        for (index, partition) in partitions.enumerated() {
            topic.append(1, to: partition)
            replies[index].release(primaryCount)
        }
        try await reader.close()
        _ = try? await reading.value
        await client.shutdown()
        for primary in primaries {
            await primary.stop()
        }
    }

    /// Partitions whose last section ended before their head read at once,
    /// so each one takes a fetch of its own while requests are free, with
    /// the full limits, as the base consumer kept a poll per partition.
    @Test func backloggedPartitionsOfOneGroupReadInFetchesOfTheirOwn() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 20, 2: 20, 3: 20, 4: 20], reportsHead: true)
        await serve(server, topic: topic)
        let client = try await signedInClient(server)
        let reader = try await standalone(client, partitions: [1, 2, 3, 4], start: .first)
        _ = try #require(try await reader.next())

        let read = { @Sendable in Set(((try? await fetches(server)) ?? []).dropFirst().flatMap { $0.targets.map(\.partitionID) }) }
        #expect(await eventually { await read() == [1, 2, 3, 4] })

        let spread = try await fetches(server).dropFirst()
        #expect(spread.allSatisfy { $0.targets.count == 1 && $0.count == 2 }, "\(spread.map { $0.targets.map(\.partitionID) })")
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// Once `next()` waits, an `onRead` reader deals its backlogged partitions
    /// over the free requests too, one committing fetch each with the full
    /// limits, beside the one fetch of the quiet partitions that may wait.
    @Test func backloggedPartitionsOfAnOnReadGroupReadInCommittingFetchesOfTheirOwn() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 20, 2: 20, 3: 20, 4: 20, 5: 0], reportsHead: true)
        await serve(server, topic: topic)
        let client = try await signedInClient(server)
        let reader = try await standalone(client, partitions: [1, 2, 3, 4, 5], start: .first, commit: .onRead)
        #expect(try await batches(reader, 1) == [[0, 1]])

        _ = try #require(try await reader.next())

        let committing = { @Sendable in (try? await fetches(server).filter(\.autoCommit)) ?? [] }
        #expect(await eventually { await committing().count == 6 })
        let spread = await committing().dropFirst()
        #expect(Set(spread.map { $0.targets.map(\.partitionID) }) == [[5], [1], [2], [3], [4]], "\(spread.map { $0.targets.map(\.partitionID) })")
        #expect(spread.allSatisfy { $0.count == 2 })
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// A section refused as not primary groups the partitions again, and
    /// the partition is read on without an error for the caller.
    @Test func aReaderGroupsAgainAfterNotPrimary() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 2])
        let refused = Locked(false)
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.fetchMessages.rawValue, !refused.exchange(true) else {
                return nil
            }
            return .ok(FakeVSRServer.fetchedBody([.init(partitionID: 1, status: IggyErrorCode.notPrimary.rawValue)]))
        }
        let client = try await signedInClient(server)
        let reader = try await standalone(client, start: .first)
        #expect(try await batches(reader, 1) == [[0, 1]])
        #expect(try await fetches(server).count >= 2)
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// A partition refused as not primary waits for a good lookup, so its
    /// sibling reads on in a fetch of its own while a lookup runs. A lookup
    /// that fails on a lasting refusal is thrown once as an error of the
    /// whole reader. The refused partition still waits, and the next good
    /// lookup releases it.
    @Test func aPartitionRefusedAsNotPrimaryWaitsForAGoodLookupWhileItsSiblingReadsOn() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 2, 2: 2])
        let refused = Locked(false)
        let lookups = Locked(0)
        let lookupEnds = AsyncSemaphore(permits: 0)
        await serve(server, topic: topic) { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .fetchMessages:
                guard let fetch = try? request.fetch, fetch.targets.contains(where: { $0.partitionID == 1 }), !refused.exchange(true) else {
                    return nil
                }
                return .ok(FakeVSRServer.fetchedBody([.init(partitionID: 1, status: IggyErrorCode.notPrimary.rawValue)]))
            case .getPartitionRoute:
                guard refused.withLock({ $0 }), UInt32(littleEndianBytes: request.payload.suffix(4)) == 1 else {
                    return nil
                }
                let lookup = lookups.withLock { count in
                    count += 1
                    return count
                }
                guard lookup <= 2 else {
                    return nil
                }
                _ = try? await lookupEnds.acquire(1)
                return lookup == 1 ? .status(IggyErrorCode.shardCommunicationError.rawValue) : nil
            default:
                return nil
            }
        }
        let client = try await signedInClient(server)
        let reader = try await standalone(client, partitions: [1, 2], start: .first)

        let sibling = try await withDeadline(.now + .seconds(2)) { try await reader.next() }
        #expect(sibling??.partitionID == 2 && sibling??.messages.map(\.offset) == [0, 1], "the sibling did not read while the lookup ran")
        #expect(await eventually { lookups.withLock { $0 } == 1 })

        lookupEnds.release(1)
        let failure = await readError(reader)
        #expect(failure?.partitionID == nil && failure?.error.code == .shardCommunicationError, "thrown: \(String(describing: failure))")
        let moving = Task { try await withDeadline(.now + .seconds(5)) { try await reader.next() } }
        #expect(await eventually { lookups.withLock { $0 } == 2 })
        let refusedReads = try await fetches(server).filter { $0.targets.contains { $0.partitionID == 1 } }.count
        let polls = await requests(server, .pollMessagesDeferred).filter { polledPartition($0) == 1 }.count
        #expect(refusedReads == 1 && polls == 0, "the refused partition read after a failed lookup")
        lookupEnds.release(1)
        let moved = try await moving.value
        #expect(moved??.partitionID == 1 && moved??.messages.map(\.offset) == [0, 1], "the refused partition did not read after the good lookup")
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// After a good lookup, one that fails on a transient refusal only logs:
    /// the reads go on with the last groups, and the lookup runs again after
    /// the retry interval. A partition refused as not owned reads again only
    /// once a lookup succeeded, though a failed one already found its route.
    @Test func aTransientLookupFailureKeepsTheLastGroupsAndARefusedPartitionWaitsForAGoodOne() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 2, 2: 2, 3: 2], assignment: [1, 2])
        let refused = Locked(false)
        let lookupsFail = Locked(true)
        let failedLookups = Locked<[ContinuousClock.Instant]>([])
        await serve(server, topic: topic) { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .fetchMessages:
                guard (try? request.fetch)?.targets.contains(where: { $0.partitionID == 1 }) == true, !refused.exchange(true) else {
                    return nil
                }
                // The rebalance hands the member a partition that the lookups cannot route.
                topic.assign([1, 2, 3])
                return .ok(FakeVSRServer.fetchedBody([.init(partitionID: 1, status: IggyErrorCode.consumerGroupPartitionNotOwned.rawValue)]))
            case .getPartitionRoute:
                guard UInt32(littleEndianBytes: request.payload.suffix(4)) == 3, lookupsFail.withLock({ $0 }) else {
                    return nil
                }
                failedLookups.withLock { $0.append(.now) }
                return .status(IggyErrorCode.deferredPollCapacityExceeded.rawValue)
            default:
                return nil
            }
        }
        let client = try await signedInClient(server)
        let reader = try await member(client, commit: .manual)
        // The first `next()` groups the partitions, and the lookups after it run beside the reads.
        let reading = Task {
            try await withDeadline(.now + .seconds(5)) {
                var partitions: [UInt32] = []
                while !Set(partitions).isSuperset(of: [1, 3]), let batch = try await reader.next() {
                    partitions.append(batch.partitionID)
                }
                return partitions
            }
        }
        #expect(await eventually { failedLookups.withLock { $0.count } >= 3 })

        let refusedReads = try await fetches(server).filter { $0.targets.contains { $0.partitionID == 1 } }.count
        #expect(refusedReads == 1, "a failed lookup released the refused partition")
        let times = failedLookups.withLock { $0 }
        #expect(zip(times, times.dropFirst()).allSatisfy { $1 - $0 >= readerRetryInterval }, "the lookups ran at \(times)")

        lookupsFail.withLock { $0 = false }
        let read = try await reading.value
        #expect(read?.first == 2 && Set(read ?? []) == [1, 2, 3], "partition 2 reads on, and the good lookup releases 1 and 3: \(String(describing: read))")
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// A refusal that settles by itself fails an assignment sync here. The
    /// reader reports nothing, logs the refusal as a warning, as it logs a
    /// failed lookup of the primaries, and syncs again.
    @Test func aTransientRefusalOfAnAssignmentSyncIsLoggedAsAWarning() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 2], assignment: [1])
        let refused = Locked(false)
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.syncConsumerGroup.rawValue, !refused.exchange(true) else {
                return nil
            }
            return .status(IggyErrorCode.deferredPollCapacityExceeded.rawValue)
        }
        let records = Locked<[(level: Logger.Level, metadata: Logger.Metadata)]>([])
        let client = try await signedInClient(server, logger: Logger(label: "reader") { _ in RecordingLogHandler(records: records) })
        let reader = try await member(client, commit: .manual)

        #expect(try await batches(reader, 1) == [[0, 1]])

        let levels = records.withLock { records in
            records.filter { $0.level >= .warning && "\($0.metadata["error"] ?? "")".contains("deferred_poll_capacity_exceeded") }.map(\.level)
        }
        #expect(levels == [.warning], "the refused sync was logged at \(levels)")
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// A member reads on under its last assignment while a later sync of the
    /// assignment hangs, and after one failed. The read call waits neither
    /// for that sync nor for the retry pause after its refusal, so the next
    /// batch arrives long before either ends.
    @Test(arguments: [false, true], [CommitPolicy.manual, .onRead])
    func aMemberReadsOnWhileALaterAssignmentSyncHangsOrFails(fails: Bool, commit: CommitPolicy) async throws {
        let retryPause: Duration = .seconds(20)
        let readBound: Duration = .seconds(2)
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 2], assignment: [1])
        let syncs = Locked(0)
        let laterSyncReplies = AsyncSemaphore(permits: 0)
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.syncConsumerGroup.rawValue else {
                return nil
            }
            let sync = syncs.withLock { count in
                count += 1
                return count
            }
            guard sync > 1 else {
                return nil
            }
            _ = try? await laterSyncReplies.acquire(1)
            return fails ? .status(IggyErrorCode.deferredPollCapacityExceeded.rawValue) : nil
        }
        let client = try await signedInClient(server)
        let reader = try await member(client, commit: commit, retryInterval: retryPause)
        #expect(try await batches(reader, 1) == [[0, 1]])

        let reading = Task { try await withDeadline(.now + readBound) { try await batches(reader, 1) } }
        try #require(await eventually { syncs.withLock { $0 } > 1 }, "the member did not sync a second time")
        // The read call is at the held sync before the messages arrive.
        try await Task.sleep(for: waitEntryPause)
        topic.append(2, to: 1)
        if fails {
            laterSyncReplies.release(.max)
        }

        #expect(try await reading.value == [[2, 3]], "the read call waited for the sync, or for the retry pause after its refusal")
        if !fails {
            laterSyncReplies.release(.max)
        }
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// A slow application under `manual` leaves replies of a backlog waiting
    /// for `next()`. The lookup after a refusal as not primary, and the
    /// assignment sync after a refusal as not owned, still run beside the
    /// reads: the refused partition reads on, and a partition that the sync
    /// adds reads, while the backlog lasts.
    @Test(arguments: [IggyErrorCode.notPrimary, .consumerGroupPartitionNotOwned])
    func aRefusedPartitionReadsAgainWhileABacklogKeepsBatchesWaiting(refusal: IggyErrorCode) async throws {
        let server = try await FakeVSRServer.started()
        let backlog: UInt64 = 400
        let topic = FakeTopic(ends: [1: 4, 2: backlog, 3: 2], assignment: [1, 2], reportsHead: true)
        let refused = Locked(false)
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.fetchMessages.rawValue, (try? request.fetch)?.targets.map(\.partitionID) == [1], !refused.exchange(true)
            else {
                return nil
            }
            if refusal == .consumerGroupPartitionNotOwned {
                topic.assign([1, 2, 3])
            }
            return .ok(FakeVSRServer.fetchedBody([.init(partitionID: 1, status: refusal.rawValue)]))
        }
        let client = try await signedInClient(server)
        let reader = try await member(client, commit: .manual)
        // The refused partition reads its last message, or the partition that the sync adds does.
        let awaited: (partition: UInt32, last: UInt64) = refusal == .notPrimary ? (1, 3) : (3, 1)
        var read: [UInt32: [UInt64]] = [:]
        // The application takes a batch every 20 ms, so the backlog lasts seconds, and the reads end with the awaited
        // message or with the backlog.
        while read[awaited.partition]?.last != awaited.last, (read[2]?.last ?? 0) < backlog - 1, let batch = try await reader.next() {
            read[batch.partitionID, default: []] += batch.messages.map(\.offset)
            try await Task.sleep(for: .milliseconds(20))
        }

        #expect(read[awaited.partition]?.last == awaited.last, "partition \(awaited.partition) did not read beside the backlog: \(read)")
        #expect((read[2]?.last ?? 0) < backlog - 1, "the backlog ran out first")
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// A committing fetch that lost its reply stored its messages without
    /// delivering them. The next read starts past them, so the reader reads
    /// that gap again without a commit and delivers it first.
    @Test func aLostCommittingReplyIsReadAgainWithoutACommit() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 6])
        await serve(server, topic: topic)
        let client = try await signedInClient(server)
        let reader = try await standalone(client, commit: .onRead)
        #expect(try await batches(reader, 1) == [[0, 1]])
        topic.loseReplies(1)
        #expect(try await batches(reader, 2) == [[2, 3], [4, 5]])
        #expect(topic.stored[1] == 5)
        let gapReads = await requests(server, .pollMessagesDeferred).map(polledStrategy)
        #expect(gapReads.map(\.kind) == [PollingStrategy.Kind.offset.rawValue] && gapReads.map(\.value) == [2] && gapReads.allSatisfy { !$0.autoCommit })
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// The fail-fast reply of the server tells nothing about the gap of a
    /// lost read. The reader reports nothing, reads the same range again
    /// after the retry interval and delivers it before the batch after it.
    @Test func theFailFastReplyToAGapReadReadsTheSameRangeAgain() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 6])
        await serve(server, topic: topic, script: firstGapRead(answeredWith: FakeVSRServer.polledBody(partitionID: 1, context: .empty)))
        let client = try await signedInClient(server)
        let reader = try await standalone(client, commit: .onRead)
        #expect(try await batches(reader, 1) == [[0, 1]])
        topic.loseReplies(1)

        try #require(try await batches(reader, 1) == [[2, 3]], "the fail-fast reply ended the gap")
        #expect(try await batches(reader, 1) == [[4, 5]])
        let gapReads = await requests(server, .pollMessagesDeferred).map(polledStrategy)
        #expect(gapReads.map(\.value) == [2, 2] && gapReads.allSatisfy { !$0.autoCommit })
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// A gap read that the history answers with none of the gap finds the
    /// range gone, as after retention. The reader reports the range once,
    /// then delivers the batch after the gap and reads on.
    @Test func aGapThatTheHistoryNoLongerHoldsIsReportedOnceAndTheReaderReadsOn() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 6])
        await serve(server, topic: topic, script: firstGapRead(answeredWith: FakeVSRServer.polledBody(partitionID: 1, offsets: [])))
        let client = try await signedInClient(server)
        let reader = try await standalone(client, commit: .onRead)
        #expect(try await batches(reader, 1) == [[0, 1]])
        topic.loseReplies(1)

        let lost = try #require(await readError(reader), "the gap that the history no longer holds was not reported")
        #expect(lost.partitionID == 1 && lost.error.code == .fetchProgressUncertain)
        #expect(lost.error.context == "offsets 2 to 3 were stored by a read whose reply was lost")
        #expect(lost.lost == 2...3)
        #expect(try await batches(reader, 1) == [[4, 5]])
        topic.append(2, to: 1)
        #expect(try await batches(reader, 1) == [[6, 7]])
        #expect(await requests(server, .pollMessagesDeferred).count == 1)
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// A stop during the gap read leaves the partition to a seek, which
    /// drops the gap and the batch held behind it, and a seek to `next` reads
    /// after both. So the stop reports the range once and delivers that
    /// batch before its own error.
    @Test func aStopDuringAGapReadReportsTheRangeAndDeliversTheHeldBatchFirst() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 6])
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.pollMessagesDeferred.rawValue, !polledStrategy(request).autoCommit else {
                return nil
            }
            return .status(IggyErrorCode.tieredReadFailed.rawValue)
        }
        let client = try await signedInClient(server)
        let reader = try await standalone(client, commit: .onRead)
        #expect(try await batches(reader, 1) == [[0, 1]])
        topic.loseReplies(1)

        let lost = try #require(await readError(reader), "the gap that the stop gave up was not reported")
        #expect(lost.partitionID == 1 && lost.error.code == .fetchProgressUncertain)
        #expect(lost.lost == 2...3)
        #expect(try await batches(reader, 1) == [[4, 5]])
        let stop = try #require(await readError(reader), "the stop was not reported")
        #expect(stop.partitionID == 1 && stop.error.code == .tieredReadFailed)
        #expect(stop.stopsPartition)
        #expect(await requests(server, .pollMessagesDeferred).count == 1)
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// A gap read that the history answers from inside the gap finds the
    /// start of the gap gone, as after retention. The reader reports that
    /// start once, then delivers the rest of the gap and the batch after it.
    @Test func theGoneStartOfAGapIsReportedOnceBeforeTheRestOfTheGap() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 6])
        await serve(server, topic: topic, script: firstGapRead(answeredWith: FakeVSRServer.polledBody(partitionID: 1, offsets: [3])))
        let client = try await signedInClient(server)
        let reader = try await standalone(client, commit: .onRead)
        #expect(try await batches(reader, 1) == [[0, 1]])
        topic.loseReplies(1)

        let lost = try #require(await readError(reader), "the gone start of the gap was not reported")
        #expect(lost.partitionID == 1 && lost.error.code == .fetchProgressUncertain)
        #expect(lost.error.context == "offsets 2 to 2 were stored by a read whose reply was lost")
        #expect(lost.lost == 2...2)
        #expect(try await batches(reader, 2) == [[3], [4, 5]])
        #expect(await requests(server, .pollMessagesDeferred).count == 1)
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// A lost range that waits for `next()` stays queued when a later read
    /// of its partition fails. The reader throws the range first, delivers
    /// the batch after it, then throws the later error and reads on.
    @Test func aLaterErrorOfThePartitionKeepsTheQueuedLostRange() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 9])
        let gapReads = Locked(0)
        let gapReadWaits = AsyncSemaphore(permits: 0)
        let gapReply = AsyncSemaphore(permits: 0)
        let gapReadRefused = AsyncSemaphore(permits: 0)
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.pollMessagesDeferred.rawValue, !polledStrategy(request).autoCommit else {
                return nil
            }
            let gapRead = gapReads.withLock { count in
                count += 1
                return count
            }
            switch gapRead {
            case 1:
                gapReadWaits.release(1)
                _ = try? await gapReply.acquire(1)
                return .ok(FakeVSRServer.polledBody(partitionID: 1, offsets: [4]))
            case 2:
                gapReadRefused.release(1)
                return .status(IggyErrorCode.invalidCommand.rawValue)
            default:
                return nil
            }
        }
        let client = try await signedInClient(server)
        let reader = try await standalone(client, count: 3, commit: .onRead)
        #expect(try await batches(reader, 1) == [[0, 1, 2]])
        topic.loseReplies(1)
        // The application takes nothing while the gap reads end, so both errors wait in the queue.
        let pending = Task { try? await reader.next() }
        try #require(try await gapReadWaits.acquire(1, deadline: ContinuousClock.now + .seconds(2)), "the gap read did not start")
        pending.cancel()
        _ = await pending.value
        gapReply.release(1)
        try #require(try await gapReadRefused.acquire(1, deadline: ContinuousClock.now + .seconds(2)), "the second gap read did not start")
        // The reader takes the refusal well within the quiet period.
        try await Task.sleep(for: quietPeriod)

        let lost = try #require(await readError(reader), "the later error replaced the lost range")
        #expect(lost.partitionID == 1 && lost.error.code == .fetchProgressUncertain)
        #expect(lost.lost == 3...3)
        #expect(try await batches(reader, 1) == [[4]])
        let refusal = try #require(await readError(reader), "the later error was not reported")
        #expect(refusal.partitionID == 1 && refusal.error.code == .invalidCommand)
        #expect(refusal.lost == nil)
        #expect(try await batches(reader, 2) == [[5], [6, 7, 8]])
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// A gap read that the server refuses with `historyUnavailable` stops
    /// its partition. The reader first throws the gap as lost and delivers
    /// the batch after it. Only then it throws the refusal, so the seek that
    /// resumes the partition drops no batch.
    @Test func aLostHistoryThatStopsAGapReadReportsTheGapAndHandsOverTheBatchFirst() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 6])
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.pollMessagesDeferred.rawValue, !polledStrategy(request).autoCommit else {
                return nil
            }
            return .status(IggyErrorCode.historyUnavailable.rawValue)
        }
        let client = try await signedInClient(server)
        let reader = try await standalone(client, commit: .onRead)
        #expect(try await batches(reader, 1) == [[0, 1]])
        topic.loseReplies(1)

        let lost = try #require(await readError(reader), "the gap of the partition that stopped was not reported")
        #expect(lost.partitionID == 1 && lost.error.code == .fetchProgressUncertain)
        #expect(lost.lost == 2...3 && !lost.stopsPartition)
        #expect(try await batches(reader, 1) == [[4, 5]])
        let refusal = try #require(await readError(reader), "the refusal that stopped the partition was not reported")
        #expect(refusal.partitionID == 1 && refusal.error.code == .historyUnavailable)
        #expect(refusal.stopsPartition && refusal.lost == nil)
        topic.append(2, to: 1)
        try await Task.sleep(for: quietPeriod)
        #expect(topic.stored[1] == 5, "the stopped partition read on without a seek")
        try await reader.seek(partitionID: 1, to: .next)
        #expect(try await batches(reader, 1) == [[6, 7]])
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// The fail-fast reply of the server tells nothing about a message of a
    /// section that did not decode. The reader reports nothing, reads the
    /// same message again after the retry interval and delivers it.
    @Test func theFailFastReplyToAReadOfOneMessageReadsTheSameMessageAgain() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 5], undecodable: [1])
        // Without a commit on read, the first poll is the read of the first message of the section.
        await serve(server, topic: topic, script: firstGapRead(answeredWith: FakeVSRServer.polledBody(partitionID: 1, context: .empty)))
        let client = try await signedInClient(server)
        let reader = try await standalone(client, start: .first, count: 3)

        try #require(try await batches(reader, 1) == [[0]], "the fail-fast reply ended the reads of the section")
        let reads = await requests(server, .pollMessagesDeferred).map(polledStrategy).prefix(2)
        #expect(reads.map(\.value) == [0, 0] && reads.allSatisfy { $0.count == 1 })
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// The fail-fast section of the server tells nothing about its partition,
    /// so it moves nothing: the gap that a later lost read leaves starts after
    /// the last batch that the reader delivered.
    @Test func theFailFastFetchSectionMovesNothing() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 6])
        let committingFetches = Locked(0)
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.fetchMessages.rawValue, (try? request.fetch.autoCommit) == true else {
                return nil
            }
            let fetch = committingFetches.withLock { count in
                count += 1
                return count
            }
            return fetch == 2 ? .ok(FakeVSRServer.fetchedBody([.init(partitionID: 1, context: .empty)])) : nil
        }
        let client = try await signedInClient(server)
        let reader = try await standalone(client, commit: .onRead)
        #expect(try await batches(reader, 1) == [[0, 1]])
        topic.loseReplies(1)

        #expect(try await batches(reader, 1) == [[2, 3]])
        #expect(try await batches(reader, 1) == [[4, 5]])
        #expect(committingFetches.withLock { $0 } >= 2, "the fail-fast section was never sent")
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// A read that starts past the expected offset while no read of its
    /// partition was lost follows a store of the application or another
    /// session, so the reader delivers from there and reads nothing again.
    @Test func anOnReadReaderTakesAForwardJumpWithoutALostReadAsIs() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 6])
        await serve(server, topic: topic)
        let client = try await signedInClient(server)
        let reader = try await standalone(client, commit: .onRead)
        #expect(try await batches(reader, 1) == [[0, 1]])
        topic.store(3, for: 1)

        #expect(try await batches(reader, 1) == [[4, 5]])

        #expect(await requests(server, .pollMessagesDeferred).isEmpty)
        try await reader.close()
        #expect(topic.stored[1] == 5)
        await client.shutdown()
        await server.stop()
    }

    @Test(arguments: [UInt64?.none, .some(4)], [false, true])
    func aLostFirstOnReadReplyIsReadAgainFromTheStoredBaseline(stored: UInt64?, trailing: Bool) async throws {
        let server = try await FakeVSRServer.started()
        let start = stored.map { $0 + 1 } ?? 0
        let topic = FakeTopic(ends: [1: start + (trailing ? 3 : 2)], losesReply: 1)
        if let stored {
            topic.store(stored, for: 1)
        }
        await serve(server, topic: topic)
        let client = try await signedInClient(server)
        let reader = try await standalone(client, commit: .onRead)

        let batch = try #require(try await reader.next())

        #expect(batch.messages.map(\.offset) == [start, start + 1])
        if trailing {
            #expect(try await batches(reader, 1) == [[start + 2]])
        }
        #expect(try await fetches(server).filter { !$0.autoCommit }.count == 1)
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    @Test(arguments: [UInt64?.none, .some(4)])
    func closingAfterTheFirstOnReadReplyIsLostRestoresTheStoredBaseline(stored: UInt64?) async throws {
        let server = try await FakeVSRServer.started()
        let start = stored.map { $0 + 1 } ?? 0
        let topic = FakeTopic(ends: [1: start + 2], losesReply: 1)
        if let stored {
            topic.store(stored, for: 1)
        }
        let ready = Locked(false)
        let released = Locked(false)
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.fetchMessages.rawValue, (try? request.fetch.autoCommit) == true else {
                return nil
            }
            let reply = await topic.reply(to: request)
            ready.withLock { $0 = true }
            while !released.withLock({ $0 }) {
                try? await Task.sleep(for: .milliseconds(10))
            }
            return reply
        }
        let client = try await signedInClient(server)
        let reader = try await standalone(client, commit: .onRead)
        let pending = Task { try? await reader.next() }
        #expect(await eventually { ready.withLock { $0 } && topic.stored[1] == start + 1 })
        pending.cancel()
        _ = await pending.value
        let closing = Task { try await reader.close() }
        released.withLock { $0 = true }
        try await closing.value

        #expect(topic.stored[1] == stored)
        await client.shutdown()
        await server.stop()
    }

    /// A committing section whose error leaves the outcome unknown may have
    /// stored its read, so the reader reads that range again, or the close
    /// moves the offset back over it.
    @Test(arguments: [false, true], [IggyErrorCode.shardCommunicationError, .cannotDecryptData, .staleClient, .unauthorized])
    func anUncertainCommittedSectionKeepsItsSiblingAndRecoversOrRewinds(closing: Bool, status: IggyErrorCode) async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 7, 2: 1])
        topic.store(4, for: 1)
        let answered = Locked(false)
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.fetchMessages.rawValue, (try? request.fetch.autoCommit) == true,
                !answered.withLock({ $0 })
            else {
                return nil
            }
            answered.withLock { $0 = true }
            _ = await topic.reply(to: request)
            return .ok(
                FakeVSRServer.fetchedBody([
                    FakeVSRServer.FetchedSection(partitionID: 1, status: status.rawValue),
                    FakeVSRServer.FetchedSection(partitionID: 2, offsets: [0], nextOffset: 1),
                ]))
        }
        let client = try await signedInClient(server)
        let reader = try await standalone(client, partitions: [1, 2], count: 3, commit: .onRead)

        let sibling = try #require(try await reader.next())
        #expect(sibling.partitionID == 2 && sibling.messages.map(\.offset) == [0])
        if !closing {
            let recovered = try #require(try await reader.next())
            #expect(recovered.partitionID == 1 && recovered.messages.map(\.offset) == [5, 6])
        }
        try await reader.close()
        #expect(topic.stored[1] == (closing ? 4 : 6))
        #expect(topic.stored[2] == 0)
        await client.shutdown()
        await server.stop()
    }

    /// The close of an `onRead` reader cancels a committing fetch that waits
    /// on the server, and that fetch waits for its one reply: nothing stored
    /// when the cancel won, or the messages when the store won, which the
    /// close moves back as undelivered. So the close takes a round trip, not
    /// the wait of the fetch.
    @Test(arguments: [true, false])
    func anOnReadCloseCancelsAParkedCommittingFetch(cancelWins: Bool) async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 0])
        let cancelled = Locked(false)
        await serve(server, topic: topic) { request in
            switch request.code {
            case CommandCode.cancelDeferredRequest.rawValue:
                cancelled.withLock { $0 = true }
                return .stall
            case CommandCode.fetchMessages.rawValue where (try? request.fetch.autoCommit) == true:
                let parked = ContinuousClock.now + .seconds(10)
                while !cancelled.withLock({ $0 }), ContinuousClock.now < parked {
                    try? await Task.sleep(for: .milliseconds(10))
                }
                if cancelWins {
                    return .status(IggyErrorCode.transientNotAccepted.rawValue)
                }
                topic.append(2, to: 1)
                return await topic.reply(to: request)
            default:
                return nil
            }
        }
        let client = try await signedInClient(server)
        let reader = try await standalone(client, commit: .onRead)
        let pending = Task { try? await reader.next() }
        #expect(await eventually { (try? await fetches(server).filter(\.autoCommit).count) == 1 })

        let started = ContinuousClock.now
        try await reader.close()
        let closing = ContinuousClock.now - started

        #expect(closing < .seconds(2), "the close took \(closing)")
        #expect(cancelled.withLock { $0 })
        #expect(topic.stored[1] == nil)
        _ = await pending.value
        await client.shutdown()
        await server.stop()
    }

    /// The offset a seek under `onRead` stored is the last delivered message
    /// of the new generation, so the close moves the offset back to it over a
    /// read that the server stored and nobody received, whatever the start.
    @Test func anOnReadCloseMovesTheOffsetBackToTheSoughtPosition() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 4])
        let held = Locked(false)
        let released = Locked(false)
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.fetchMessages.rawValue, (try? request.fetch.autoCommit) == true, topic.stored[1] == 0 else {
                return nil
            }
            let reply = await topic.reply(to: request)
            held.withLock { $0 = true }
            while !released.withLock({ $0 }) {
                try? await Task.sleep(for: .milliseconds(10))
            }
            return reply
        }
        let client = try await signedInClient(server)
        let reader = try await standalone(client, start: .first, commit: .onRead)
        #expect(try await batches(reader, 1) == [[0, 1]])
        try await reader.seek(partitionID: 1, to: .offset(1))
        #expect(topic.stored[1] == 0)
        let pending = Task { try? await reader.next() }
        #expect(await eventually { held.withLock { $0 } && topic.stored[1] == 2 })
        pending.cancel()
        _ = await pending.value
        released.withLock { $0 = true }

        try await reader.close()

        #expect(topic.stored[1] == 0)
        await client.shutdown()
        await server.stop()
    }

    /// While the store of a seek under `onRead` waits, no committing read of
    /// the partition starts, and the first committing read of the new
    /// generation starts at the sought offset.
    @Test func anOnReadSeekBeforeTheFirstReadReadsFromItsOffsetAfterTheStore() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 5])
        let held = Locked(false)
        let released = Locked(false)
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.storeConsumerOffset.rawValue else {
                return nil
            }
            let reply = await topic.reply(to: request)
            held.withLock { $0 = true }
            while !released.withLock({ $0 }) {
                try? await Task.sleep(for: .milliseconds(10))
            }
            return reply
        }
        let client = try await signedInClient(server)
        let reader = try await standalone(client, start: .first, commit: .onRead)
        let seeking = Task { try await reader.seek(partitionID: 1, to: .offset(3)) }
        #expect(await eventually { held.withLock { $0 } })
        let pending = Task { try? await reader.next() }
        try await Task.sleep(for: quietPeriod)
        #expect(try await committingReads(server, of: 1) == 0, "a committing read started while the store of a seek waited")
        released.withLock { $0 = true }
        try await seeking.value

        let batch = (await pending.value) ?? nil
        #expect(batch?.messages.map(\.offset) == [3, 4])
        let first = try #require(try await fetches(server).first(where: \.autoCommit))
        #expect(first.targets.map(\.kind) == [.offset] && first.targets.map(\.value) == [3])
        #expect(topic.stored[1] == 4)
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// A read never moves the stored offset back, so a seek under `onRead`
    /// to offset zero deletes it, and the reads from zero go on past their
    /// first batch.
    @Test func anOnReadSeekToOffsetZeroDeletesTheStoredOffsetAndReadsThePartitionAgain() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 6])
        await serve(server, topic: topic)
        let client = try await signedInClient(server)
        let reader = try await standalone(client, commit: .onRead)
        #expect(try await batches(reader, 2) == [[0, 1], [2, 3]])

        try await reader.seek(partitionID: 1, to: .offset(0))

        #expect(topic.stored[1] == nil)
        #expect(await requests(server, .getConsumerOffset).count == 1)
        #expect(await requests(server, .deleteConsumerOffset).count == 1)
        #expect(await requests(server, .storeConsumerOffset).isEmpty)
        #expect(try await batches(reader, 2) == [[0, 1], [2, 3]])
        #expect(topic.stored[1] == 3)
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// The server refuses to delete a missing offset, so a seek under
    /// `onRead` to the first message of a partition without one only looks
    /// it up.
    @Test func anOnReadSeekToTheFirstMessageWithoutAStoredOffsetDeletesNothing() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 2])
        await serve(server, topic: topic)
        let client = try await signedInClient(server)
        let reader = try await standalone(client, commit: .onRead)

        try await reader.seek(partitionID: 1, to: .first)

        #expect(await requests(server, .getConsumerOffset).count == 1)
        #expect(await requests(server, .deleteConsumerOffset).isEmpty)
        #expect(try await batches(reader, 1) == [[0, 1]])
        #expect(topic.stored[1] == 1)
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// A seek under `onRead` whose store, or whose delete for offset zero,
    /// the server refuses fails with that error and moves nothing: the
    /// batch of the partition that waits for `next()` stays, and the
    /// partition reads on in its generation.
    @Test(arguments: [UInt64(4), 0])
    func anOnReadSeekWhoseStoreOrDeleteIsRefusedMovesNothing(offset: UInt64) async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 2, 2: 4])
        await serve(server, topic: topic) { request in
            [CommandCode.storeConsumerOffset, .deleteConsumerOffset].contains { request.code == $0.rawValue }
                ? .status(IggyErrorCode.unauthorized.rawValue) : nil
        }
        let client = try await signedInClient(server)
        let reader = try await standalone(client, partitions: [1, 2], count: 4, commit: .onRead)
        let sibling = try #require(try await reader.next())
        #expect(sibling.partitionID == 1)

        let refused = await #expect(throws: IggyError.self) { try await reader.seek(partitionID: 2, to: .offset(offset)) }

        #expect(refused?.code == .unauthorized)
        let queued = try #require(try await reader.next())
        #expect(queued.partitionID == 2 && queued.messages.map(\.offset) == [0, 1])
        let after = try #require(try await reader.next())
        #expect(after.partitionID == 2 && after.messages.map(\.offset) == [2, 3] && after.stamp?.generation == queued.stamp?.generation)
        #expect(topic.stored[2] == 3)
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// A seek under `onRead` whose store the server refuses wakes a `next()`
    /// that waits behind the hold of the seek, so the partition reads on at
    /// once.
    @Test func anOnReadSeekWhoseStoreIsRefusedWakesAWaitingNext() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 4])
        let storing = Locked(false)
        let storeEnds = AsyncSemaphore(permits: 0)
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.storeConsumerOffset.rawValue else {
                return nil
            }
            storing.withLock { $0 = true }
            _ = try? await storeEnds.acquire(1)
            return .status(IggyErrorCode.unauthorized.rawValue)
        }
        let client = try await signedInClient(server)
        let reader = try await standalone(client, commit: .onRead)
        #expect(try await batches(reader, 1) == [[0, 1]])
        let seeking = Task { try await reader.seek(partitionID: 1, to: .offset(3)) }
        #expect(await eventually { storing.withLock { $0 } })
        let waitStarted = ContinuousClock.now
        let pending = Task { try await withDeadline(.now + .seconds(5)) { try await reader.next() } }
        try await Task.sleep(for: waitEntryPause)

        storeEnds.release(1)
        await #expect(throws: IggyError.self) { try await seeking.value }
        let batch = try await pending.value

        #expect(batch??.messages.map(\.offset) == [2, 3])
        // Without a wake-up, a waiting next() reads only at the idle check of the reader.
        let waited = ContinuousClock.now - waitStarted
        #expect(waited < PartitionReader.assignmentCheckInterval, "the next read waited \(waited) for the idle check of the reader")
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// A seek under `onRead` whose caller is cancelled after its store went
    /// out throws `CancellationError` at once and still finishes: the
    /// partition moves once the store succeeds, and the close waits for it.
    @Test(arguments: [false, true])
    func anOnReadSeekCancelledAfterItsStoreWentOutStillFinishes(closes: Bool) async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 10])
        let storing = Locked(false)
        let storeEnds = AsyncSemaphore(permits: 0)
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.storeConsumerOffset.rawValue else {
                return nil
            }
            storing.withLock { $0 = true }
            _ = try? await storeEnds.acquire(1)
            return await topic.reply(to: request)
        }
        let client = try await signedInClient(server)
        let reader = try await standalone(client, commit: .onRead)
        let before = try #require(try await reader.next())
        let seeking = Task { try await reader.seek(partitionID: 1, to: .offset(6)) }
        #expect(await eventually { storing.withLock { $0 } })

        seeking.cancel()
        // The store waits until the test releases it, so a seek that waits for it cannot end in time.
        let outcome = try await withDeadline(.now + .seconds(5)) { await seeking.result }
        var thrown: (any Error)?
        if case .failure(let error)? = outcome {
            thrown = error
        }
        #expect(thrown is CancellationError, "the cancelled seek did not end at once: \(String(describing: outcome))")

        if closes {
            let closed = Locked(false)
            let closing = Task {
                try await reader.close()
                closed.withLock { $0 = true }
            }
            try await Task.sleep(for: quietPeriod)
            #expect(!closed.withLock { $0 }, "the close ended before the store of a cancelled seek")
            storeEnds.release(1)
            try await closing.value
            #expect(topic.stored[1] == 5)
        } else {
            storeEnds.release(1)
            #expect(await eventually { topic.stored[1] == 5 })
            let after = try #require(try await withDeadline(.now + .seconds(2)) { try await reader.next() } ?? nil)
            #expect(after.messages.map(\.offset) == [6, 7] && after.stamp?.generation != before.stamp?.generation)
            try await reader.close()
        }
        await client.shutdown()
        await server.stop()
    }

    /// A seek under `onRead` whose store has an unknown outcome fails and
    /// keeps the position. The store moved the offset forward, so the next
    /// read reads the skipped messages again first. A later seek and the
    /// close still work.
    @Test func anOnReadSeekWhoseStoreHasAnUnknownOutcomeSkipsNoMessage() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 10])
        await serve(server, topic: topic, script: firstWriteWithUnknownOutcome(topic))
        let client = try await signedInClient(server)
        let reader = try await standalone(client, commit: .onRead)
        #expect(try await batches(reader, 1) == [[0, 1]])

        let refused = await #expect(throws: IggyError.self) { try await reader.seek(partitionID: 1, to: .offset(6)) }

        #expect(refused?.code == .transientNotCommitted)
        #expect(topic.stored[1] == 5)
        let read = try await withDeadline(.now + .seconds(3)) { try await batches(reader, 3) }
        #expect(read == [[2, 3], [4, 5], [6, 7]])
        try await reader.seek(partitionID: 1, to: .offset(9))
        try await reader.close()
        #expect(topic.stored[1] == 8)
        await client.shutdown()
        await server.stop()
    }

    /// A seek under `onRead` before the first read whose store has an unknown
    /// outcome and moved the offset forward skips no message: from a `next`
    /// start, or from a start whose lookup did not run yet, with or without
    /// a stored offset. Each origin reads on at offset 2. The next reads
    /// read the skipped messages first, and a close without a read moves the
    /// offset back to where it was.
    @Test(
        arguments: [(start: PollingStrategy.next, stored: UInt64?.some(1)), (start: .offset(0), stored: .some(1)), (start: .offset(2), stored: nil)],
        [false, true])
    func anOnReadSeekBeforeTheFirstReadWhoseStoreHasAnUnknownOutcomeSkipsNoMessage(
        origin: (start: PollingStrategy, stored: UInt64?), closes: Bool
    ) async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 10])
        if let stored = origin.stored {
            topic.store(stored, for: 1)
        }
        await serve(server, topic: topic, script: firstWriteWithUnknownOutcome(topic))
        let client = try await signedInClient(server)
        let reader = try await standalone(client, start: origin.start, commit: .onRead)

        let refused = await #expect(throws: IggyError.self) { try await reader.seek(partitionID: 1, to: .offset(6)) }

        #expect(refused?.code == .transientNotCommitted)
        #expect(topic.stored[1] == 5)
        if closes {
            try await reader.close()
            #expect(topic.stored[1] == origin.stored)
        } else {
            let read = try await withDeadline(.now + .seconds(3)) { try await batches(reader, 3) }
            #expect(read == [[2, 3], [4, 5], [6, 7]])
            try await reader.close()
        }
        await client.shutdown()
        await server.stop()
    }

    /// A seek under `onRead` that stores while the close starts keeps its
    /// store: the read in flight ends in the old generation, and the close
    /// moves the offset back to the sought position.
    @Test func anOnReadSeekThatWaitsWhenTheCloseStartsKeepsItsStore() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 4])
        let held = Locked(false)
        let released = Locked(false)
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.fetchMessages.rawValue, (try? request.fetch.autoCommit) == true, topic.stored[1] == 1 else {
                return nil
            }
            let reply = await topic.reply(to: request)
            held.withLock { $0 = true }
            while !released.withLock({ $0 }) {
                try? await Task.sleep(for: .milliseconds(10))
            }
            return reply
        }
        let client = try await signedInClient(server)
        let reader = try await standalone(client, commit: .onRead)
        #expect(try await batches(reader, 1) == [[0, 1]])
        let pending = Task { try? await reader.next() }
        #expect(await eventually { held.withLock { $0 } && topic.stored[1] == 3 })
        // The seek holds the partition before its probe goes out, so the close starts after the seek.
        let seeking = Task { try await reader.seek(partitionID: 1, to: .timestamp(IggyTimestamp(microseconds: 1))) }
        #expect(await eventually { await !requests(server, .pollMessagesDeferred).isEmpty })
        let closing = Task { try await reader.close() }
        released.withLock { $0 = true }
        try await seeking.value
        try await closing.value
        _ = await pending.value

        #expect(topic.stored[1] == 0)
        await client.shutdown()
        await server.stop()
    }

    /// A member under `onRead` whose read is refused as not owned starts the
    /// partition again with a new baseline, so the next read that loses its
    /// reply is read again, and no message is skipped.
    @Test func anOnReadPartitionRefusedAsNotOwnedTakesANewBaseline() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 6])
        let refused = Locked(false)
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.fetchMessages.rawValue, (try? request.fetch)?.autoCommit == true, topic.stored[1] == 1,
                !refused.exchange(true)
            else {
                return nil
            }
            topic.loseReplies(1)
            return .ok(FakeVSRServer.fetchedBody([.init(partitionID: 1, status: IggyErrorCode.consumerGroupPartitionNotOwned.rawValue)]))
        }
        let client = try await signedInClient(server)
        let reader = try await member(client, commit: .onRead)

        let read = try await withDeadline(.now + .seconds(5)) { try await batches(reader, 3) }

        #expect(read == [[0, 1], [2, 3], [4, 5]])
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// A section that the member no longer owns says nothing about its other
    /// partitions, so the close still moves back what the same reply stored
    /// for them.
    @Test func anOnReadCloseRewindsTheSiblingOfASectionThatLostItsOwner() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 0, 2: 2], assignment: [1, 2])
        let ready = Locked(false)
        let released = Locked(false)
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.fetchMessages.rawValue, (try? request.fetch.autoCommit) == true, !ready.withLock({ $0 }) else {
                return nil
            }
            _ = await topic.reply(to: request)
            ready.withLock { $0 = true }
            while !released.withLock({ $0 }) {
                try? await Task.sleep(for: .milliseconds(10))
            }
            return .ok(
                FakeVSRServer.fetchedBody([
                    FakeVSRServer.FetchedSection(partitionID: 1, status: IggyErrorCode.consumerGroupPartitionNotOwned.rawValue),
                    FakeVSRServer.FetchedSection(partitionID: 2, offsets: [0, 1]),
                ]))
        }
        let client = try await signedInClient(server)
        let reader = try await member(client, commit: .onRead)
        let pending = Task { try? await reader.next() }
        #expect(await eventually { ready.withLock { $0 } && topic.stored[2] == 1 })
        pending.cancel()
        _ = await pending.value

        let closing = Task { try await reader.close() }
        released.withLock { $0 = true }
        try await closing.value

        #expect(topic.stored[2] == nil)
        await client.shutdown()
        await server.stop()
    }

    /// Under `onRead` the start applies only to a partition without a stored
    /// offset, so a partition with one resumes after it and reads nothing
    /// from the start.
    @Test func anOnReadStartResumesAfterAStoredOffset() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 8])
        topic.store(5, for: 1)
        await serve(server, topic: topic)
        let client = try await signedInClient(server)
        let reader = try await standalone(client, start: .offset(0), commit: .onRead)

        #expect(try await batches(reader, 1) == [[6, 7]])

        #expect(try await fetches(server).flatMap(\.targets).allSatisfy { $0.kind != .offset || $0.value != 0 })
        #expect(await requests(server, .pollMessagesDeferred).isEmpty)
        try await reader.close()
        #expect(topic.stored[1] == 7)
        await client.shutdown()
        await server.stop()
    }

    /// Under `onRead` a partition without a stored offset reads from the
    /// start, and a reader opened after it resumes after the offset that the
    /// first one stored.
    @Test func anOnReadStartAppliesOnlyUntilAnOffsetIsStored() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 6])
        await serve(server, topic: topic)
        let client = try await signedInClient(server)
        let first = try await standalone(client, start: .offset(3), commit: .onRead)
        #expect(try await batches(first, 1) == [[3, 4]])
        try await first.close()

        let reopened = try await standalone(client, start: .offset(3), commit: .onRead)

        #expect(try await batches(reopened, 1) == [[5]])
        try await reopened.close()
        #expect(topic.stored[1] == 5)
        await client.shutdown()
        await server.stop()
    }

    /// Under `onRead` a member resumes each partition it gets after the
    /// stored offset of its group, also when a rebalance brings the partition
    /// back.
    @Test func anOnReadMemberResumesEachAssignedPartitionAfterItsStoredOffset() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 4, 2: 4], assignment: [1])
        topic.store(1, for: 1)
        topic.store(2, for: 2)
        await serve(server, topic: topic)
        let client = try await signedInClient(server)
        let reader = try await member(client, commit: .onRead, start: .offset(0))
        #expect(try await batches(reader, 1) == [[2, 3]])

        topic.assign([2])
        client.core.groupState.invalidateAssignment(memberGroupKey)
        let moved = try #require(try await reader.next())
        #expect(moved.partitionID == 2 && moved.messages.map(\.offset) == [3])

        topic.assign([1])
        client.core.groupState.invalidateAssignment(memberGroupKey)
        topic.append(2, to: 1)
        let returned = try #require(try await reader.next())
        #expect(returned.partitionID == 1 && returned.messages.map(\.offset) == [4, 5])
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// Under `onRead` the close moves a partition that delivered nothing
    /// before the first batch it read, whatever the start. At offset zero it
    /// deletes the offset that the read stored.
    @Test(arguments: [UInt64(3), 0])
    func anOnReadCloseMovesAnUndeliveredPartitionBeforeItsFirstBatch(start: UInt64) async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: start + 2, 2: start + 2])
        await serve(server, topic: topic)
        let client = try await signedInClient(server)
        let reader = try await standalone(client, partitions: [1, 2], start: .offset(start), count: 4, commit: .onRead)
        let delivered = try #require(try await reader.next())
        let undelivered: UInt32 = delivered.partitionID == 1 ? 2 : 1
        #expect(topic.stored[undelivered] == start + 1)

        try await reader.close()

        #expect(topic.stored[delivered.partitionID] == start + 1)
        #expect(topic.stored[undelivered] == (start == 0 ? nil : start - 1))
        #expect(await requests(server, .deleteConsumerOffset).count == (start == 0 ? 1 : 0))
        await client.shutdown()
        await server.stop()
    }

    /// The server refuses to delete a missing offset, so a close that moves
    /// a partition before offset zero sends no delete when nothing is stored.
    @Test func anOnReadCloseBeforeOffsetZeroSendsNoDeleteWithoutAStoredOffset() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 2, 2: 2])
        await serve(server, topic: topic)
        let client = try await signedInClient(server)
        let reader = try await standalone(client, partitions: [1, 2], count: 4, commit: .onRead)
        let delivered = try #require(try await reader.next())
        let undelivered: UInt32 = delivered.partitionID == 1 ? 2 : 1
        topic.deleteStored(for: undelivered)

        try await reader.close()

        #expect(await requests(server, .deleteConsumerOffset).isEmpty)
        #expect(topic.stored[undelivered] == nil)
        #expect(topic.stored[delivered.partitionID] == 1)
        await client.shutdown()
        await server.stop()
    }

    /// Under `onRead` a seek to a timestamp stores the offset before the
    /// first message at the timestamp, so a stored offset ahead of it does
    /// not make the reads jump.
    @Test func anOnReadSeekToATimestampReadsOnWithoutAJumpToAStoredOffsetAhead() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 8])
        topic.store(5, for: 1)
        await serve(server, topic: topic)
        let client = try await signedInClient(server)
        let reader = try await standalone(client, commit: .onRead)

        try await reader.seek(partitionID: 1, to: .timestamp(IggyTimestamp(microseconds: 2)))

        #expect(topic.stored[1] == 1)
        try #require(try await batches(reader, 2) == [[2, 3], [4, 5]])
        #expect(try await batches(reader, 1) == [[6, 7]])
        let probes = await requests(server, .pollMessagesDeferred).map(polledStrategy)
        #expect(probes.map(\.kind) == [PollingStrategy.Kind.timestamp.rawValue] && probes.allSatisfy { !$0.autoCommit && $0.count == 1 })
        try await reader.close()
        #expect(topic.stored[1] == 7)
        await client.shutdown()
        await server.stop()
    }

    /// The hold of a seek to a timestamp under `onRead` covers its probe: no
    /// committing read of the partition starts during the probe, and the
    /// close waits for the seek. A seek after the start of the close fails.
    @Test(arguments: [false, true])
    func anOnReadSeekToATimestampHoldsTheReadsAndTheCloseDuringItsProbe(closes: Bool) async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 8])
        let probing = Locked(false)
        let probeEnds = AsyncSemaphore(permits: 0)
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.pollMessagesDeferred.rawValue, polledStrategy(request).kind == PollingStrategy.Kind.timestamp.rawValue
            else {
                return nil
            }
            probing.withLock { $0 = true }
            _ = try? await probeEnds.acquire(1)
            return await topic.reply(to: request)
        }
        let client = try await signedInClient(server)
        let reader = try await standalone(client, commit: .onRead)
        #expect(try await batches(reader, 1) == [[0, 1]])
        let seeking = Task { try await reader.seek(partitionID: 1, to: .timestamp(IggyTimestamp(microseconds: 4))) }
        #expect(await eventually { probing.withLock { $0 } })

        if closes {
            let closed = Locked(false)
            let closing = Task {
                try await reader.close()
                closed.withLock { $0 = true }
            }
            try await Task.sleep(for: quietPeriod)
            #expect(!closed.withLock { $0 }, "the close ended during the probe of a seek")
            probeEnds.release(1)
            try await seeking.value
            try await closing.value
            #expect(topic.stored[1] == 3)
            let late = await #expect(throws: IggyError.self) { try await reader.seek(partitionID: 1, to: .offset(0)) }
            #expect(late?.code == .invalidCommand)
            #expect(topic.stored[1] == 3)
        } else {
            let pending = Task { try await withDeadline(.now + .seconds(5)) { try await reader.next() } }
            try await Task.sleep(for: quietPeriod)
            #expect(try await committingReads(server, of: 1) == 1, "a committing read started during the probe of a seek")
            probeEnds.release(1)
            try await seeking.value
            let batch = try await pending.value
            #expect(batch??.messages.map(\.offset) == [4, 5])
            #expect(topic.stored[1] == 5)
            try await reader.close()
        }
        await client.shutdown()
        await server.stop()
    }

    /// Under `onRead` a lost first read, of the configured start or of a
    /// seek, counts from where it started: the close moves the partition
    /// before it, or deletes the stored offset at zero. A check finds that
    /// start for a `last` or timestamp position without a commit, with the
    /// count of the reader for `last` and one message for a timestamp, also
    /// after a seek to a timestamp that found no message yet.
    @Test(
        arguments: [
            (position: PollingStrategy.offset(3), first: UInt64(3)), (.offset(0), 0), (.last, 8), (.timestamp(IggyTimestamp(microseconds: 12)), 12),
        ], [false, true])
    func anOnReadCloseMovesBeforeALostFirstRead(origin: (position: PollingStrategy, first: UInt64), seeks: Bool) async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: origin.first], losesReply: 1)
        let ready = Locked(false)
        let released = Locked(false)
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.fetchMessages.rawValue, (try? request.fetch.autoCommit) == true else {
                return nil
            }
            let reply = await topic.reply(to: request)
            ready.withLock { $0 = true }
            while !released.withLock({ $0 }) {
                try? await Task.sleep(for: .milliseconds(10))
            }
            return reply
        }
        let client = try await signedInClient(server)
        let reader = try await standalone(client, start: seeks ? .next : origin.position, commit: .onRead)
        if seeks {
            try await reader.seek(partitionID: 1, to: origin.position)
        }
        topic.append(2, to: 1)
        let pending = Task { try? await reader.next() }
        #expect(await eventually { ready.withLock { $0 } && topic.stored[1] == origin.first + 1 })
        pending.cancel()
        _ = await pending.value
        let closing = Task { try await reader.close() }
        released.withLock { $0 = true }
        try await closing.value

        #expect(topic.stored[1] == (origin.first == 0 ? nil : origin.first - 1))
        let polls = await requests(server, .pollMessagesDeferred).map(polledStrategy)
        #expect(polls.allSatisfy { !$0.autoCommit && $0.count == (origin.position == .last ? 2 : 1) }, "\(polls)")
        await client.shutdown()
        await server.stop()
    }

    /// A seek under `onRead` before the first read of a start without a
    /// stored offset, whose store has an unknown outcome, leaves no stored
    /// offset at a close without a read. So a restart takes the start again,
    /// also after a seek back from an offset start.
    @Test(arguments: [PollingStrategy.last, .timestamp(IggyTimestamp(microseconds: 3)), .offset(8)])
    func anOnReadCloseRestoresNoStoredOffsetAfterASeekWithAnUnknownOutcome(start: PollingStrategy) async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 10])
        await serve(server, topic: topic, script: firstWriteWithUnknownOutcome(topic))
        let client = try await signedInClient(server)
        let reader = try await standalone(client, start: start, commit: .onRead)

        let refused = await #expect(throws: IggyError.self) { try await reader.seek(partitionID: 1, to: .offset(6)) }

        #expect(refused?.code == .transientNotCommitted)
        #expect(topic.stored[1] == 5)
        try await reader.close()
        #expect(topic.stored[1] == nil)
        await client.shutdown()
        await server.stop()
    }

    /// A queued batch decides the rewind of a start without a stored offset.
    /// A seek under `onRead` whose store has an unknown outcome, before or
    /// after a batch at 8 arrived that waits undelivered, leaves 7 stored at
    /// the close. This holds also when the close begins while the seek waits
    /// for that batch, so the seek reads no baseline. Without the stored
    /// offset, a restart from `last` skips that batch once more messages
    /// arrive.
    @Test(arguments: [PollingStrategy.last, .offset(8)], QueuedBatchSeek.allCases)
    func anOnReadCloseStoresBeforeAQueuedBatchAfterASeekWithAnUnknownOutcome(start: PollingStrategy, timing: QueuedBatchSeek) async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 10])
        let queuedFrom = Locked<UInt64?>(nil)
        let readEnds = AsyncSemaphore(permits: 0)
        let unknownStore = firstWriteWithUnknownOutcome(topic)
        await serve(server, topic: topic) { request in
            guard let fetch = try? request.fetch, fetch.autoCommit, let target = fetch.targets.first, queuedFrom.withLock({ $0 }) == nil else {
                return await unknownStore(request)
            }
            queuedFrom.withLock { $0 = target.value }
            _ = try? await readEnds.acquire(1)
            return await topic.reply(to: request)
        }
        let client = try await signedInClient(server)
        let reader = try await standalone(client, start: start, commit: .onRead)
        if timing == .beforeTheRead {
            let refused = await #expect(throws: IggyError.self) { try await reader.seek(partitionID: 1, to: .offset(6)) }
            #expect(refused?.code == .transientNotCommitted)
        }
        // The read outlives its cancelled caller, so its batch waits in the queue.
        let pending = Task { try await reader.next() }
        #expect(await eventually { queuedFrom.withLock { $0 } == 8 })
        pending.cancel()
        _ = await pending.result
        let seeking = timing == .beforeTheRead ? nil : Task { try await reader.seek(partitionID: 1, to: .offset(6)) }
        var closing: Task<Void, any Error>?
        if timing == .whileTheReadAndTheCloseWait {
            #expect(await eventually { await reader.storesWaiting[1] != nil })
            closing = Task { try await reader.close() }
            #expect(await eventually { await reader.isClosed })
        }
        readEnds.release(1)
        if let seeking {
            let refused = await #expect(throws: IggyError.self) { try await seeking.value }
            #expect(refused?.code == .transientNotCommitted)
        }

        if let closing {
            try await closing.value
        } else {
            try await reader.close()
        }
        #expect(topic.stored[1] == 7, "the close deleted the offset despite the queued batch at 8")
        await client.shutdown()
        await server.stop()
    }

    /// A seek to `first` under `onRead` whose delete landed with an unknown
    /// outcome keeps the position after the deleted offset. An empty read
    /// keeps it too, so the reader never reads from zero again.
    @Test func anOnReadPositionAfterADeleteWithAnUnknownOutcomeOutlivesAnEmptyRead() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 5])
        topic.store(4, for: 1)
        let emptyRead = Locked(false)
        let unknownDelete = firstWriteWithUnknownOutcome(topic, .deleteConsumerOffset)
        await serve(server, topic: topic) { request in
            guard let fetch = try? request.fetch, fetch.autoCommit, fetch.targets.contains(where: { $0.kind == .offset && $0.value == 5 }) else {
                return await unknownDelete(request)
            }
            let reply = await topic.reply(to: request)
            emptyRead.withLock { $0 = true }
            return reply
        }
        let client = try await signedInClient(server)
        let reader = try await standalone(client, commit: .onRead)

        let refused = await #expect(throws: IggyError.self) { try await reader.seek(partitionID: 1, to: .first) }

        #expect(refused?.code == .transientNotCommitted && topic.stored[1] == nil)
        let pending = Task { try await withDeadline(.now + .seconds(5)) { try await reader.next() } }
        #expect(await eventually { emptyRead.withLock { $0 } })
        topic.append(2, to: 1)
        let read = try #require(try await pending.value ?? nil)
        #expect(read.messages.map(\.offset) == [5, 6], "an empty read moved the position to the deleted stored offset")
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// A seek to an offset under `onRead` whose store has an unknown outcome,
    /// after a seek to `last` that wrote nothing and before the check of
    /// `last` ends, makes the close restore the stored offset from before
    /// that store, or delete the offset when none existed.
    @Test(arguments: [UInt64?.some(50), nil])
    func anOnReadCloseRestoresTheStoredOffsetAfterALostStoreBehindASeekToLast(stored: UInt64?) async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 100])
        if let stored {
            topic.store(stored, for: 1)
        }
        let checking = Locked(false)
        let checkEnds = AsyncSemaphore(permits: 0)
        let unknownStore = firstWriteWithUnknownOutcome(topic)
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.pollMessagesDeferred.rawValue, polledStrategy(request).kind == PollingStrategy.Kind.last.rawValue,
                !checking.exchange(true)
            else {
                return await unknownStore(request)
            }
            _ = try? await checkEnds.acquire(1)
            return await topic.reply(to: request)
        }
        let client = try await signedInClient(server)
        let reader = try await standalone(client, count: 10, commit: .onRead)
        try await reader.seek(partitionID: 1, to: .last)
        let pending = Task { try? await reader.next() }
        #expect(await eventually { checking.withLock { $0 } })

        let refused = await #expect(throws: IggyError.self) { try await reader.seek(partitionID: 1, to: .offset(20)) }

        #expect(refused?.code == .transientNotCommitted && topic.stored[1] == 19)
        try await reader.close()
        #expect(topic.stored[1] == stored, "the close did not restore the stored offset from before the lost store")
        checkEnds.release(1)
        _ = await pending.value
        await client.shutdown()
        await server.stop()
    }

    /// An unknown outcome of a seek write under `onRead` that ends after the
    /// session changed belongs to the session the write started in. For a
    /// member it changes nothing in the new one, so the close keeps the
    /// offset that another member stored meanwhile. A standalone reader has
    /// no owner, so its close still restores the offset from before the
    /// write.
    @Test(arguments: [true, false])
    func anOnReadUnknownOutcomeOfASeekWriteAcrossASessionChange(group: Bool) async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 10], assignment: [1])
        topic.store(4, for: 1)
        let groupState = Locked<ConsumerGroupState?>(nil)
        let sessionEnded = Locked(false)
        let unknownStore = firstWriteWithUnknownOutcome(topic)
        await serve(server, topic: topic) { request in
            if request.code == CommandCode.storeConsumerOffset.rawValue, !sessionEnded.exchange(true) {
                groupState.withLock { $0 }?.clearSessionScoped()
            }
            return await unknownStore(request)
        }
        let client = try await signedInClient(server)
        groupState.withLock { $0 = client.core.groupState }
        let reader = try await (group ? member(client, commit: .onRead) : standalone(client, commit: .onRead))
        #expect(try await batches(reader, 1) == [[5, 6]])

        let refused = await #expect(throws: IggyError.self) { try await reader.seek(partitionID: 1, to: .offset(2)) }

        #expect(refused?.code == .transientNotCommitted && topic.stored[1] == 1)
        topic.store(8, for: 1)
        try await reader.close()
        #expect(topic.stored[1] == (group ? 8 : 6), "the close followed the outcome of the wrong session")
        await client.shutdown()
        await server.stop()
    }

    /// A seek under `onRead` whose store has an unknown outcome, after a
    /// committing read that lost its reply, makes the close store the offset
    /// before that read. Nothing of that read reached the application.
    @Test func anOnReadCloseMovesBeforeALostReadAfterASeekWithAnUnknownOutcome() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 7], losesReply: 1)
        topic.store(4, for: 1)
        await serve(server, topic: topic, script: firstWriteWithUnknownOutcome(topic))
        let client = try await signedInClient(server)
        let reader = try await standalone(client, commit: .onRead)
        let pending = Task { try? await reader.next() }
        #expect(await eventually { topic.stored[1] == 6 })
        pending.cancel()
        _ = await pending.value

        let refused = await #expect(throws: IggyError.self) { try await reader.seek(partitionID: 1, to: .offset(2)) }

        #expect(refused?.code == .transientNotCommitted && topic.stored[1] == 1)
        try await reader.close()
        #expect(topic.stored[1] == 4, "the close kept the end of the lost read")
        await client.shutdown()
        await server.stop()
    }

    /// An empty read under `onRead` stored nothing. Without a write or a read
    /// with an unknown outcome it moves an offset position to `next`, so the
    /// reader goes on after an offset that another client stored meanwhile.
    @Test func anOnReadEmptyReadAfterASeekGoesOnAfterTheStoredOffset() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 10])
        let emptyRead = Locked(false)
        await serve(server, topic: topic) { request in
            guard let fetch = try? request.fetch, fetch.autoCommit, fetch.targets.contains(where: { $0.kind == .offset && $0.value == 10 }) else {
                return nil
            }
            let reply = await topic.reply(to: request)
            emptyRead.withLock { $0 = true }
            return reply
        }
        let client = try await signedInClient(server)
        let reader = try await standalone(client, commit: .onRead)
        #expect(try await batches(reader, 1) == [[0, 1]])
        try await reader.seek(partitionID: 1, to: .offset(10))
        let pending = Task { try await withDeadline(.now + .seconds(5)) { try await reader.next() } }
        #expect(await eventually { emptyRead.withLock { $0 } })

        topic.store(12, for: 1)
        topic.append(5, to: 1)

        let read = try #require(try await pending.value ?? nil)
        #expect(read.messages.map(\.offset) == [13, 14], "an empty read kept an offset position without a lost write")
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// After a session change the server installs the owner again with a
    /// new generation. Until a read of the new session names it, a seek
    /// write and the close rewind of a lost read carry owner generation 0.
    /// When the client sends them, it raises their metadata op to the one of
    /// the new session, so the server binds them to the installed owner.
    @Test(arguments: [false, true])
    func anOnReadWriteAfterASessionChangeCarriesNoEarlierOwner(seeks: Bool) async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 7], assignment: [1], losesReply: seeks ? 0 : 1)
        topic.installOwner(1)
        topic.store(4, for: 1)
        await serve(server, topic: topic)
        let client = try await signedInClient(server)
        let reader = try await member(client, commit: .onRead)
        if seeks {
            #expect(try await batches(reader, 1) == [[5, 6]])
        } else {
            // The read outlives its cancelled caller and loses its reply.
            let pending = Task { try? await reader.next() }
            #expect(await eventually { topic.stored[1] == 6 })
            pending.cancel()
            _ = await pending.value
        }
        try await changeSession(of: client, topic, owner: 2)

        if seeks {
            try await reader.seek(partitionID: 1, to: .offset(2))
        }
        try await reader.close()

        #expect(topic.stored[1] == (seeks ? 1 : 4))
        #expect(await requests(server, .storeConsumerOffset).map(\.context) == [ownerlessWriteContext], "the first write did not land")
        await client.shutdown()
        await server.stop()
    }

    /// A read that finds the gap of a lost read gets its reply after a
    /// session change. Until a read of the new session names the new owner,
    /// the read of that gap names owner generation 0, so the server does not
    /// refuse it and the reader delivers the range of the lost read.
    @Test func anOnReadGapOfALostReadIsReadAfterASessionChange() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 7], assignment: [1])
        topic.installOwner(1)
        topic.store(4, for: 1)
        let groupState = Locked<ConsumerGroupState?>(nil)
        let sessionEnded = Locked(false)
        await serve(server, topic: topic) { request in
            guard let fetch = try? request.fetch, fetch.autoCommit, topic.stored[1] == 8, !sessionEnded.exchange(true) else {
                return nil
            }
            let reply = await topic.reply(to: request)
            groupState.withLock { $0 }?.clearSessionScoped()
            topic.installOwner(2)
            return reply
        }
        let client = try await signedInClient(server)
        groupState.withLock { $0 = client.core.groupState }
        let reader = try await member(client, commit: .onRead)
        #expect(try await batches(reader, 1) == [[5, 6]])
        topic.loseReplies(1)
        topic.append(3, to: 1)

        #expect(try await batches(reader, 2) == [[7, 8], [9]], "the reader skipped the range of the lost read")

        try await reader.close()
        #expect(topic.stored[1] == 9)
        await client.shutdown()
        await server.stop()
    }

    /// A commit, or the flush of a counted position at the close, after a
    /// session change names owner generation 0 until a read of the new
    /// session names the new owner. When the client sends it, it raises its
    /// metadata op to the one of the new session, so the server binds it to
    /// the installed owner.
    @Test(arguments: [false, true])
    func aStoreAfterASessionChangeCarriesNoEarlierOwner(flushes: Bool) async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 2], assignment: [1])
        topic.installOwner(1)
        await serve(server, topic: topic)
        let client = try await signedInClient(server)
        let reader = try await member(client, commit: flushes ? .auto(AutoCommit(interval: unendingCommitInterval, after: .delivery)) : .manual)
        let read = try #require(try await reader.next())
        try await changeSession(of: client, topic, owner: 2)

        if !flushes {
            try await reader.commit(try #require(read.checkpoint))
        }
        try await reader.close()

        #expect(topic.stored[1] == 1)
        #expect(await requests(server, .storeConsumerOffset).map(\.context) == [ownerlessWriteContext], "the first store did not land")
        await client.shutdown()
        await server.stop()
    }

    /// A read refused as not owned is not a leave, so its partition reads
    /// on. Each sixth refusal in a row reports one error, and a read that
    /// succeeds starts the count again. Five refusals and a read come first,
    /// then thirteen refusals, which report twice.
    @Test func eachSixthReadInARowRefusedAsNotOwnedReportsOneErrorAndThePartitionReadsOn() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 4], assignment: [1])
        let perReport = PartitionReader.notOwnedRefusalsPerReport
        let refused = Set(0..<perReport - 1).union(perReport..<3 * perReport + 1)
        let reads = Locked(0)
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.fetchMessages.rawValue || request.code == CommandCode.pollMessagesDeferred.rawValue else {
                return nil
            }
            let read = reads.withLock { count in
                defer { count += 1 }
                return count
            }
            return refused.contains(read) ? .status(IggyErrorCode.consumerGroupPartitionNotOwned.rawValue) : nil
        }
        let client = try await signedInClient(server)
        // The wait doubles with each refusal in a row, so a short interval keeps the eighteen refusals quick.
        let reader = try await member(client, commit: .manual, retryInterval: .milliseconds(10))
        var delivered: [[UInt64]] = []
        var reportsBefore: [[ReadError]] = [[]]

        while delivered.count < 2 {
            do {
                guard let batch = try await reader.next() else {
                    break
                }
                delivered.append(batch.messages.map(\.offset))
                reportsBefore.append([])
            } catch let error as ReadError {
                reportsBefore[reportsBefore.count - 1].append(error)
            }
        }

        #expect(delivered == [[0, 1], [2, 3]])
        #expect(reportsBefore[0].isEmpty, "a run of refusals that a read ended reported an error")
        #expect(reportsBefore[1].count == 2, "thirteen refusals in a row reported \(reportsBefore[1].count) errors")
        #expect(reportsBefore[1].allSatisfy { $0.partitionID == 1 && $0.error.code == .consumerGroupPartitionNotOwned && !$0.stopsPartition })
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// The wait after a read refused as not owned doubles with each refusal
    /// in a row, from the retry interval up to 32 times it at the sixth
    /// refusal, which reports one error. Then the count starts again, so the
    /// seventh refusal waits the retry interval.
    @Test func theWaitAfterEachReadInARowRefusedAsNotOwnedDoubles() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 2], assignment: [1])
        let perReport = PartitionReader.notOwnedRefusalsPerReport
        let reads = Locked<[ContinuousClock.Instant]>([])
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.fetchMessages.rawValue || request.code == CommandCode.pollMessagesDeferred.rawValue else {
                return nil
            }
            let refused = reads.withLock { reads in
                reads.append(.now)
                return reads.count <= perReport + 1
            }
            return refused ? .status(IggyErrorCode.consumerGroupPartitionNotOwned.rawValue) : nil
        }
        let client = try await signedInClient(server)
        let retryInterval: Duration = .milliseconds(10)
        let reader = try await member(client, commit: .manual, retryInterval: retryInterval)

        let report = try #require(await readError(reader), "the sixth refusal in a row reported nothing")
        #expect(report.partitionID == 1 && report.error.code == .consumerGroupPartitionNotOwned)
        #expect(try await batches(reader, 1) == [[0, 1]])

        // A fetch after the delivered batch can be in flight already.
        let times = reads.withLock { $0 }
        try #require(times.count >= perReport + 2, "\(times.count) reads")
        let waits = zip(times, times.dropFirst()).map { $1 - $0 }
        let doubled = (0..<perReport).map { retryInterval * (1 << Swift.min($0, PartitionReader.maxRefusalBackoffSteps)) }
        #expect(zip(waits, doubled).allSatisfy { $0 >= $1 }, "the reads after the refusals waited \(waits)")
        #expect(waits[perReport] < doubled[perReport - 1], "the count did not start again after the report: \(waits)")
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// A new session starts the count and the wait of the reads refused as
    /// not owned again, as the epoch rewind in the Rust SDK does. A partition
    /// that three refusals in a row paused for four retry intervals reads at
    /// once after the rejoin, and its next refusal waits one retry interval.
    @Test func aPartitionRefusedAsNotOwnedBeforeASessionChangeReadsAtOnceAfterTheRejoin() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 2], assignment: [1])
        let refusedBefore = 3
        let reads = Locked<[ContinuousClock.Instant]>([])
        let sessionChanged = Locked(false)
        let rejoinedAt = Locked<ContinuousClock.Instant?>(nil)
        await serve(server, topic: topic) { request in
            let code = request.code.flatMap(CommandCode.init(rawValue:))
            if code == .joinConsumerGroup, sessionChanged.withLock({ $0 }) {
                rejoinedAt.withLock { $0 = $0 ?? .now }
            }
            guard code == .fetchMessages || code == .pollMessagesDeferred else {
                return nil
            }
            let refused = reads.withLock { reads in
                reads.append(.now)
                return reads.count <= refusedBefore + 1
            }
            return refused ? .status(IggyErrorCode.consumerGroupPartitionNotOwned.rawValue) : nil
        }
        let client = try await signedInClient(server)
        // The pause after the third refusal, four retry intervals, outlasts the longest wait for the sync that leads to the rejoin.
        let retryInterval = PartitionReader.assignmentCheckInterval / 2
        let reader = try await member(client, commit: .manual, retryInterval: retryInterval)
        let reading = Task { try await batches(reader, 1) }
        try #require(await eventually { reads.withLock { $0.count } == refusedBefore }, "the reads before the session change were not refused")
        // The reader takes the third refusal before the session changes.
        try await Task.sleep(for: waitEntryPause)

        sessionChanged.withLock { $0 = true }
        client.core.groupState.clearSessionScoped()

        #expect(try await reading.value == [[0, 1]])
        let times = reads.withLock { $0 }
        try #require(times.count >= refusedBefore + 2, "\(times.count) reads")
        let rejoined = try #require(rejoinedAt.withLock { $0 }, "the reader did not join again")
        let sinceRejoin = times[refusedBefore] - rejoined
        #expect(sinceRejoin >= .zero && sinceRejoin < retryInterval, "the read after the rejoin went \(sinceRejoin) after it")
        let wait = times[refusedBefore + 1] - times[refusedBefore]
        #expect(wait >= retryInterval && wait < retryInterval * 2, "the first refusal after the rejoin waited \(wait)")
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// A member that the application joins has no join of the reader that
    /// starts the count of the reads refused as not owned again. So the
    /// reader starts the count and the wait again when it sees a new
    /// session. A partition that three refusals in a row paused for four
    /// retry intervals reads before that pause ends, and its next refusal
    /// waits one retry interval.
    @Test func aNewSessionStartsTheCountOfNotOwnedRefusalsAgainForAMemberThatTheApplicationJoins() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 2], assignment: [1])
        let refusedBefore = 3
        let reads = Locked<[ContinuousClock.Instant]>([])
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.fetchMessages.rawValue || request.code == CommandCode.pollMessagesDeferred.rawValue else {
                return nil
            }
            let refused = reads.withLock { reads in
                reads.append(.now)
                return reads.count <= refusedBefore + 1
            }
            return refused ? .status(IggyErrorCode.consumerGroupPartitionNotOwned.rawValue) : nil
        }
        let client = try await signedInClient(server)
        try await client.joinConsumerGroup(streamID: 1, topicID: 2, groupID: 3)
        // The pause after the third refusal, four retry intervals, outlasts the longest wait for the next check of the assignment.
        let retryInterval = PartitionReader.assignmentCheckInterval / 2
        let reader = try await client.reader(
            streamID: 1, topicID: 2, consumer: .group(3),
            configuration: ReaderConfiguration(
                start: .next, options: PollOptions(count: 2, maxWait: .milliseconds(200), commit: .manual), retryInterval: retryInterval,
                autoJoinConsumerGroup: false))
        let reading = Task { try await batches(reader, 1) }
        try #require(await eventually { reads.withLock { $0.count } == refusedBefore }, "the reads before the session change were not refused")
        // The reader takes the third refusal before the session changes.
        try await Task.sleep(for: waitEntryPause)

        client.core.groupState.clearSessionScoped()
        try await client.joinConsumerGroup(streamID: 1, topicID: 2, groupID: 3)

        #expect(try await reading.value == [[0, 1]])
        let times = reads.withLock { $0 }
        try #require(times.count >= refusedBefore + 2, "\(times.count) reads")
        let pauseBefore = retryInterval * (1 << (refusedBefore - 1))
        let sinceRefusal = times[refusedBefore] - times[refusedBefore - 1]
        #expect(sinceRefusal < pauseBefore, "the read after the session change waited \(sinceRefusal), the pause of the session before")
        let wait = times[refusedBefore + 1] - times[refusedBefore]
        #expect(wait >= retryInterval && wait < retryInterval * 2, "the first refusal of the new session waited \(wait)")
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// A read refused as not owned before a sync shows a leave keeps the
    /// state of its partition. After the retry interval the partition reads
    /// again, finds the gap of the lost read before it and delivers its range.
    @Test(arguments: [false, true])
    func anOnReadReadRefusedAsNotOwnedKeepsALostRead(gapRead: Bool) async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 7], assignment: [1])
        topic.store(4, for: 1)
        let refused = Locked(false)
        await serve(server, topic: topic) { request in
            let read =
                gapRead
                ? request.code == CommandCode.pollMessagesDeferred.rawValue : (try? request.fetch)?.autoCommit == true && topic.stored[1] == 8
            guard read, !refused.exchange(true) else {
                return nil
            }
            return .status(IggyErrorCode.consumerGroupPartitionNotOwned.rawValue)
        }
        let client = try await signedInClient(server)
        let reader = try await member(client, commit: .onRead)
        #expect(try await batches(reader, 1) == [[5, 6]])
        topic.loseReplies(1)
        topic.append(3, to: 1)

        let read = try await withDeadline(.now + .seconds(5)) { try await batches(reader, 2) }

        #expect(read == [[7, 8], [9]], "the refused read dropped the lost read")
        try await reader.close()
        #expect(topic.stored[1] == 9)
        await client.shutdown()
        await server.stop()
    }

    /// When the client sends a group offset write with owner generation 0, it
    /// raises its metadata op to the newest op that it saw, here the op of
    /// the join that installed the owner. A standalone write and a write that
    /// names an owner keep the op of their context.
    @Test(arguments: [false, true], [UInt64(0), 1])
    func onlyAGroupOffsetWriteWithoutAnOwnerRoutesByTheNewestOp(group: Bool, owner: UInt64) async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 2], assignment: [1])
        topic.installOwner(1, at: sessionChangeOp)
        await serve(server, topic: topic)
        let client = try await signedInClient(server)
        try await client.joinConsumerGroup(streamID: 1, topicID: 2, groupID: 3)
        let route = FakeVSRServer.routeContext
        let checkpoint = Checkpoint(
            partitionID: 1, offset: 1, context: PartitionContext(incarnation: route.incarnation, ownerGeneration: owner, metadataOp: route.metadataOp))

        // The fake refuses a standalone write by the owner rules of the group, which does not matter here.
        try? await client.storeConsumerPosition(consumer: group ? .group(3) : .consumer(9), streamID: 1, topicID: 2, checkpoint: checkpoint)

        let sent = try #require(await requests(server, .storeConsumerOffset).last).context
        #expect(sent.incarnation == route.incarnation && sent.ownerGeneration == owner)
        #expect(sent.metadataOp == (group && owner == 0 ? sessionChangeOp : route.metadataOp))
        await client.shutdown()
        await server.stop()
    }

    /// A seek of a partition before any reply of it writes with the empty
    /// context. The client sends that write under the route context, which
    /// it read after the join that installed the owner. A rebalance by
    /// another member after that read installs the owner at a later op that
    /// no reply to the client carries, so the server refuses the write. The
    /// seek then reads the partition without a commit and stores again under
    /// the owner that read names, in any history, as the write named none.
    @Test(arguments: [false, true])
    func aMemberSeekBeforeTheFirstReadOfAPartitionLands(rebalanced: Bool) async throws {
        let server = try await FakeVSRServer.started()
        // Nothing follows the stored offset of partition 2, so no read delivers from it before the seek.
        let topic = FakeTopic(ends: [1: 2, 2: 3], assignment: [1])
        topic.store(2, for: 2)
        topic.installOwner(1, at: sessionChangeOp)
        await serve(server, topic: topic)
        let client = try await signedInClient(server)
        let reader = try await member(client, commit: .onRead)
        #expect(try await batches(reader, 1) == [[0, 1]])
        topic.assign([1, 2])
        client.core.groupState.invalidateAssignment(memberGroupKey)
        let syncing = Task { try? await reader.next() }
        #expect(await eventually { await reader.partitions == [1, 2] })
        let routeOfPartition2 = try Requests.partitionRoute(streamID: 1, topicID: 2, partitionID: 2)
        #expect(await eventually { await requests(server, .getPartitionRoute).contains { $0.payload == routeOfPartition2 } })
        if rebalanced {
            topic.installOwner(1, at: sessionChangeOp + 10)
        }
        syncing.cancel()
        _ = await syncing.value

        try await reader.seek(partitionID: 2, to: .offset(1))

        #expect(topic.stored[2] == 0)
        let stores = await requests(server, .storeConsumerOffset)
        #expect(stores.count == (rebalanced ? 2 : 1))
        #expect(stores.first?.context == ownerlessWriteContext, "the client sent the first write without the incarnation of the route or the op of the join")
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// The close rewinds before a lost read, and the server refuses that
    /// store once as not owned. The store goes again under the owner that a
    /// read without a commit names. The close is the caller of its own
    /// writes, so its start does not end the retry of that read after a
    /// transient refusal.
    @Test(arguments: [0, 2])
    func anOnReadCloseRewindRefusedAsNotOwnedLandsOnTheRetry(transientRefusals: Int) async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 7], assignment: [1], losesReply: 1)
        topic.store(4, for: 1)
        let refused = Locked(false)
        let ownerReads = Locked(0)
        await serve(server, topic: topic) { request in
            if request.code == CommandCode.storeConsumerOffset.rawValue, !refused.exchange(true) {
                return .status(IggyErrorCode.consumerGroupPartitionNotOwned.rawValue)
            }
            guard isOwnerRead(request), refused.withLock({ $0 }) else {
                return nil
            }
            let ownerRead = ownerReads.withLock { count in
                count += 1
                return count
            }
            return ownerRead <= transientRefusals ? .status(IggyErrorCode.transientNotCommitted.rawValue) : nil
        }
        let client = try await signedInClient(server)
        let reader = try await member(client, commit: .onRead)
        // The read outlives its cancelled caller and loses its reply.
        let pending = Task { try? await reader.next() }
        #expect(await eventually { topic.stored[1] == 6 })
        pending.cancel()
        _ = await pending.value

        try await reader.close()

        #expect(ownerReads.withLock { $0 } == transientRefusals + 1)
        #expect(topic.stored[1] == 4, "the refused rewind left the offset of the lost read")
        await client.shutdown()
        await server.stop()
    }

    /// A store refused as not owned is not a leave. For a partition that the
    /// member still reads, the reader syncs its assignment at once, reads the
    /// partition without a commit, and stores again under the owner of that
    /// read. The next read of the partition waits for a message meanwhile,
    /// so no read that ends starts a sync.
    @Test func aCommitRefusedAsNotOwnedSyncsTheAssignmentAndStoresUnderTheInstalledOwner() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 2], assignment: [1])
        let refused = Locked(false)
        let synced = Locked(false)
        let syncedBeforeOwnerRead = Locked(false)
        await serve(server, topic: topic) { request in
            if request.code == CommandCode.storeConsumerOffset.rawValue, !refused.exchange(true) {
                return .status(IggyErrorCode.consumerGroupPartitionNotOwned.rawValue)
            }
            if request.code == CommandCode.syncConsumerGroup.rawValue, refused.withLock({ $0 }) {
                synced.withLock { $0 = true }
            } else if isOwnerRead(request) {
                let answered = await eventually(timeout: .seconds(1)) { synced.withLock { $0 } }
                syncedBeforeOwnerRead.withLock { $0 = answered }
            }
            return nil
        }
        let client = try await signedInClient(server)
        let reader = try await member(client, commit: .manual, maxWait: .seconds(10))
        let checkpoint = try #require(try await reader.next()?.checkpoint)
        let reads: [UInt32?] = [CommandCode.fetchMessages.rawValue, CommandCode.pollMessagesDeferred.rawValue]
        #expect(await eventually { await server.requests.filter { reads.contains($0.code) }.count >= 2 }, "the next read did not go out")

        try await reader.commit(checkpoint)

        #expect(syncedBeforeOwnerRead.withLock { $0 }, "the refused store did not sync the assignment")
        #expect(topic.stored[1] == 1)
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// A counted position of a partition that left goes out after the sync
    /// that showed the leave. The partition belongs to the member no more,
    /// so the reader logs the refusal of that store as not owned, and
    /// neither reads the partition for its owner nor stores again.
    @Test func aStoreRefusedAsNotOwnedForAPartitionThatLeftStandsWithoutAnOwnerRead() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 2, 2: 2], assignment: [1])
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.storeConsumerOffset.rawValue, storedPartition(request) == 1 else {
                return nil
            }
            return .status(IggyErrorCode.consumerGroupPartitionNotOwned.rawValue)
        }
        let records = Locked<[(level: Logger.Level, metadata: Logger.Metadata)]>([])
        let client = try await signedInClient(server, logger: Logger(label: "reader") { _ in RecordingLogHandler(records: records) })
        let reader = try await member(client, commit: .auto(AutoCommit(interval: unendingCommitInterval, after: .delivery)))
        #expect(try await batches(reader, 1) == [[0, 1]])
        topic.assign([2])
        client.core.groupState.invalidateAssignment(memberGroupKey)
        #expect(try await batches(reader, 1) == [[0, 1]])

        let logged = await eventually { records.withLock { $0.contains(where: reportsRefusalAsNotOwned) } }

        #expect(logged, "the refusal of the store was not logged")
        #expect(await requests(server, .storeConsumerOffset).filter { storedPartition($0) == 1 }.count == 1, "the refused store went again")
        #expect(await server.requests.filter(isOwnerRead).isEmpty, "the refused store read the partition that left")
        try await reader.close()
        #expect(topic.stored[1] == nil && topic.stored[2] == 1)
        await client.shutdown()
        await server.stop()
    }

    /// The store of a counted position of a partition that left can be
    /// refused as not owned only after the partition came back, as the
    /// client sends a store that the server did not accept again. The store
    /// still belongs to the assignment that ended, and another member can
    /// have stored a later offset meanwhile. So the refusal stands, and the
    /// reader neither reads the partition for its owner nor stores again.
    @Test func aStoreOfAnAssignmentThatEndedStandsWhenItsPartitionCameBack() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 2, 2: 2], assignment: [1])
        topic.installOwner(1)
        let storing = Locked(false)
        let cameBack = Locked(false)
        let storesAfterReturn = Locked(0)
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.storeConsumerOffset.rawValue, storedPartition(request) == 1 else {
                return nil
            }
            storing.withLock { $0 = true }
            guard cameBack.withLock({ $0 }) else {
                // The coordinator connection carries one request at a time, so a store that the server held would also hold
                // the sync of the return. A store that is not accepted goes again, and the sync goes between its attempts.
                return .status(IggyErrorCode.transientNotAccepted.rawValue)
            }
            storesAfterReturn.withLock { $0 += 1 }
            // The topic refuses it, as the owner was installed again meanwhile.
            return nil
        }
        let records = Locked<[(level: Logger.Level, metadata: Logger.Metadata)]>([])
        let client = try await signedInClient(server, logger: Logger(label: "reader") { _ in RecordingLogHandler(records: records) })
        let reader = try await member(client, commit: .auto(AutoCommit(interval: unendingCommitInterval, after: .delivery)))
        #expect(try await batches(reader, 1) == [[0, 1]])
        topic.assign([2])
        client.core.groupState.invalidateAssignment(memberGroupKey)
        #expect(try await batches(reader, 1) == [[0, 1]])
        try #require(await eventually { storing.withLock { $0 } }, "the counted position of the partition that left was not stored")
        topic.installOwner(2)
        topic.assign([1, 2])
        client.core.groupState.invalidateAssignment(memberGroupKey)
        let reading = Task { try await batches(reader, 1) }
        try #require(await eventually { await reader.partitions == [1, 2] }, "partition 1 did not come back")

        cameBack.withLock { $0 = true }

        #expect(await eventually { records.withLock { $0.contains(where: reportsRefusalAsNotOwned) } }, "the refusal of the store was not logged")
        #expect(await server.requests.filter(isOwnerRead).isEmpty, "the store of the assignment that ended read the owner")
        #expect(storesAfterReturn.withLock { $0 } == 1, "the store went again")
        try await reader.close()
        _ = await reading.result
        await client.shutdown()
        await server.stop()
    }

    /// The partition of a commit refused as not owned can leave and come
    /// back while the read that names its owner keeps getting a transient
    /// refusal. The commit still belongs to the assignment that ended, and
    /// another member can have stored a later offset meanwhile. So the
    /// refusal stands once that read is served, and the store does not go
    /// again.
    @Test func aCommitRefusedAsNotOwnedStandsWhenItsPartitionLeftAndCameBackDuringItsOwnerRead() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 2, 2: 2], assignment: [1, 2])
        let refused = Locked(false)
        let ownerReads = Locked(0)
        let cameBack = Locked(false)
        await serve(server, topic: topic) { request in
            if request.code == CommandCode.storeConsumerOffset.rawValue, !refused.exchange(true) {
                return .status(IggyErrorCode.consumerGroupPartitionNotOwned.rawValue)
            }
            guard isOwnerRead(request), !cameBack.withLock({ $0 }) else {
                return nil
            }
            ownerReads.withLock { $0 += 1 }
            // The client hands `notPrimary` of a deferred poll over at once, so the reader reads again after each pause.
            return .status(IggyErrorCode.notPrimary.rawValue)
        }
        let client = try await signedInClient(server)
        let reader = try await member(client, commit: .manual)
        var checkpoints: [UInt32: Checkpoint] = [:]
        while checkpoints.count < 2, let batch = try await reader.next() {
            checkpoints[batch.partitionID] = batch.checkpoint
        }
        let checkpoint = try #require(checkpoints[1])
        let committing = Task { try await reader.commit(checkpoint) }
        try #require(await eventually { ownerReads.withLock { $0 } >= 1 }, "the refused commit did not read its owner")
        topic.assign([2])
        client.core.groupState.invalidateAssignment(memberGroupKey)
        // A waiting read takes each new assignment.
        let reading = Task { try? await reader.next() }
        try #require(await eventually { await reader.partitions == [2] }, "partition 1 did not leave")
        topic.assign([1, 2])
        client.core.groupState.invalidateAssignment(memberGroupKey)
        try #require(await eventually { await reader.partitions == [1, 2] }, "partition 1 did not come back")

        cameBack.withLock { $0 = true }

        let refusal = await #expect(throws: IggyError.self) { try await committing.value }
        #expect(refusal?.code == .consumerGroupPartitionNotOwned)
        #expect(await requests(server, .storeConsumerOffset).count == 1, "the commit of the assignment that ended stored again")
        #expect(topic.stored[1] == nil)
        try await reader.close()
        _ = await reading.value
        await client.shutdown()
        await server.stop()
    }

    /// A commit refused as not owned reads its partition without a commit,
    /// for the owner that the server installed. After a transient refusal,
    /// `notPrimary` from a primary that moves included, that read goes again
    /// after the retry interval until the caller is cancelled. Then, and
    /// after any other failure of that read, the commit fails with the first
    /// refusal, and that read does not go again.
    @Test(arguments: [IggyErrorCode.transientNotCommitted, .notPrimary, .historyUnavailable])
    func aCommitRefusedAsNotOwnedFailsWithTheRefusalOnceItsOwnerReadEnds(ownerReadRefusal: IggyErrorCode) async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 2], assignment: [1])
        let refused = Locked(false)
        let ownerReads = Locked(0)
        await serve(server, topic: topic) { request in
            if request.code == CommandCode.storeConsumerOffset.rawValue, !refused.exchange(true) {
                return .status(IggyErrorCode.consumerGroupPartitionNotOwned.rawValue)
            }
            guard isOwnerRead(request), refused.withLock({ $0 }) else {
                return nil
            }
            ownerReads.withLock { $0 += 1 }
            return .status(ownerReadRefusal.rawValue)
        }
        let client = try await signedInClient(server)
        let reader = try await member(client, commit: .manual)
        let checkpoint = try #require(try await reader.next()?.checkpoint)
        let committing = Task { try await reader.commit(checkpoint) }
        let retries = ownerReadRefusal != .historyUnavailable
        if retries {
            #expect(await eventually { ownerReads.withLock { $0 } >= 2 }, "the owner read did not go again after a transient refusal")
            committing.cancel()
        }

        let refusal = await #expect(throws: IggyError.self) { try await committing.value }

        #expect(refusal?.code == .consumerGroupPartitionNotOwned)
        #expect(retries || ownerReads.withLock { $0 } == 1)
        let readsAtEnd = ownerReads.withLock { $0 }
        try await Task.sleep(for: quietPeriod)
        #expect(ownerReads.withLock { $0 } == readsAtEnd, "the owner read went again after the commit ended")
        #expect(topic.stored[1] == nil)
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// A commit cancelled while the read that names the owner of its refused
    /// store waits for its reply fails with the refusal. The reply that
    /// names the owner then sends no store.
    @Test func aCommitCancelledDuringItsOwnerReadFailsWithTheRefusalAndSendsNoStoreAgain() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 2], assignment: [1])
        let refused = Locked(false)
        let ownerReads = Locked(0)
        let ownerReplies = AsyncSemaphore(permits: 0)
        await serve(server, topic: topic) { request in
            if request.code == CommandCode.storeConsumerOffset.rawValue, !refused.exchange(true) {
                return .status(IggyErrorCode.consumerGroupPartitionNotOwned.rawValue)
            }
            guard isOwnerRead(request) else {
                return nil
            }
            ownerReads.withLock { $0 += 1 }
            _ = try? await ownerReplies.acquire(1)
            return await topic.reply(to: request)
        }
        let client = try await signedInClient(server)
        let reader = try await member(client, commit: .manual)
        let checkpoint = try #require(try await reader.next()?.checkpoint)
        let committing = Task { try await reader.commit(checkpoint) }
        try #require(await eventually { ownerReads.withLock { $0 } == 1 }, "the refused commit did not read its owner")

        committing.cancel()
        ownerReplies.release(1)

        let refusal = await #expect(throws: IggyError.self) { try await committing.value }
        #expect(refusal?.code == .consumerGroupPartitionNotOwned)
        try await Task.sleep(for: quietPeriod)
        #expect(await requests(server, .storeConsumerOffset).count == 1, "the store went again after the commit was cancelled")
        #expect(ownerReads.withLock { $0 } == 1)
        #expect(topic.stored[1] == nil)
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// The read that names the owner of a refused write needs only the
    /// context of its reply. So a message of that read that does not decode
    /// does not fail the write.
    @Test func aCommitRefusedAsNotOwnedLandsWhenItsOwnerReadFindsAMessageThatDoesNotDecode() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 2], assignment: [1])
        let refused = Locked(false)
        await serve(server, topic: topic) { request in
            if request.code == CommandCode.storeConsumerOffset.rawValue, !refused.exchange(true) {
                return .status(IggyErrorCode.consumerGroupPartitionNotOwned.rawValue)
            }
            guard isOwnerRead(request), case .ok(let body)? = await topic.reply(to: request) else {
                return nil
            }
            // A record cut short does not decode.
            return .ok(Array(body.dropLast(4)))
        }
        let client = try await signedInClient(server)
        let reader = try await member(client, commit: .manual)
        let checkpoint = try #require(try await reader.next()?.checkpoint)

        try await reader.commit(checkpoint)

        #expect(topic.stored[1] == 1)
        #expect(await requests(server, .storeConsumerOffset).count == 2)
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// The offset writes of a partition run one at a time, and those of
    /// another partition do not wait for them. While the read that names the
    /// owner of a commit refused as not owned keeps getting a transient
    /// refusal, a commit of another partition lands.
    @Test func aCommitLandsWhileTheOwnerReadOfARefusedCommitOfAnotherPartitionRetries() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 2, 2: 2], assignment: [1, 2])
        let ownerReads = Locked(0)
        await serve(server, topic: topic) { request in
            if request.code == CommandCode.storeConsumerOffset.rawValue, storedPartition(request) == 1 {
                return .status(IggyErrorCode.consumerGroupPartitionNotOwned.rawValue)
            }
            guard isOwnerRead(request), polledPartition(request) == 1 else {
                return nil
            }
            ownerReads.withLock { $0 += 1 }
            return .status(IggyErrorCode.transientNotCommitted.rawValue)
        }
        let client = try await signedInClient(server)
        let reader = try await member(client, commit: .manual)
        var checkpoints: [UInt32: Checkpoint] = [:]
        while checkpoints.count < 2, let batch = try await reader.next() {
            checkpoints[batch.partitionID] = batch.checkpoint
        }
        let (first, second) = (try #require(checkpoints[1]), try #require(checkpoints[2]))
        let refused = Task { try await reader.commit(first) }
        #expect(await eventually { ownerReads.withLock { $0 } >= 2 }, "the owner read did not go again after a transient refusal")

        let other = Task { try await reader.commit(second) }

        #expect(await eventually { topic.stored[2] == 1 }, "the commit of partition 2 waited for the owner read of partition 1")
        refused.cancel()
        let refusal = await #expect(throws: IggyError.self) { try await refused.value }
        #expect(refusal?.code == .consumerGroupPartitionNotOwned)
        try await other.value
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// A seek write after its caller stopped waiting, and an `.auto` store,
    /// run in the background, so the reader is the caller of the read that
    /// names the owner of their refused store. The start of the close ends
    /// the retry of that read, so the close does not wait for it, and the
    /// reader logs the refusal.
    @Test(arguments: [true, false])
    func theCloseEndsTheOwnerReadOfARefusedWriteInTheBackground(seeks: Bool) async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 10], assignment: [1])
        let storing = Locked(false)
        let storeEnds = AsyncSemaphore(permits: 0)
        let ownerReads = Locked(0)
        await serve(server, topic: topic) { request in
            if request.code == CommandCode.storeConsumerOffset.rawValue, !storing.exchange(true) {
                _ = try? await storeEnds.acquire(1)
                return .status(IggyErrorCode.consumerGroupPartitionNotOwned.rawValue)
            }
            guard isOwnerRead(request), storing.withLock({ $0 }) else {
                return nil
            }
            ownerReads.withLock { $0 += 1 }
            return .status(IggyErrorCode.transientNotCommitted.rawValue)
        }
        let records = Locked<[(level: Logger.Level, metadata: Logger.Metadata)]>([])
        let client = try await signedInClient(server, logger: Logger(label: "reader") { _ in RecordingLogHandler(records: records) })
        let reader = try await member(client, commit: seeks ? .onRead : .auto(AutoCommit(every: .batch, after: .delivery)))
        #expect(try await batches(reader, 1) == [[0, 1]])
        if seeks {
            let seeking = Task { try await reader.seek(partitionID: 1, to: .offset(6)) }
            #expect(await eventually { storing.withLock { $0 } })
            seeking.cancel()
            _ = await seeking.result
        }
        #expect(await eventually { storing.withLock { $0 } })
        storeEnds.release(1)
        #expect(await eventually { ownerReads.withLock { $0 } >= 2 }, "the owner read did not go again after a transient refusal")

        try await reader.close()

        #expect(records.withLock { $0.contains(where: reportsRefusalAsNotOwned) }, "the refusal of the write was not logged")
        #expect(topic.stored[1] == 1)
        await client.shutdown()
        await server.stop()
    }

    /// A seek write runs in the background, so the start of the close ends
    /// the retry of the read that names the owner of its refused store. A
    /// refusal that comes after the close start reads no owner, the answer
    /// of a read in flight at the close start sends no store, and a pause
    /// after a transient refusal ends at once. The seek fails with the
    /// refusal, and nothing moves.
    @Test(arguments: OwnerReadAtTheCloseStart.allCases)
    func theCloseStartEndsTheOwnerReadOfARefusedSeekWrite(at moment: OwnerReadAtTheCloseStart) async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 10], assignment: [1])
        let refused = Locked(false)
        let storeReplies = AsyncSemaphore(permits: moment == .notStarted ? 0 : 1)
        let ownerReads = Locked(0)
        let ownerReplies = AsyncSemaphore(permits: moment == .inFlight ? 0 : 1)
        await serve(server, topic: topic) { request in
            if request.code == CommandCode.storeConsumerOffset.rawValue, !refused.exchange(true) {
                _ = try? await storeReplies.acquire(1)
                return .status(IggyErrorCode.consumerGroupPartitionNotOwned.rawValue)
            }
            guard isOwnerRead(request), refused.withLock({ $0 }) else {
                return nil
            }
            ownerReads.withLock { $0 += 1 }
            if moment == .paused {
                // The client hands `notPrimary` of a deferred poll over at once, so the reader pauses.
                return .status(IggyErrorCode.notPrimary.rawValue)
            }
            _ = try? await ownerReplies.acquire(1)
            return await topic.reply(to: request)
        }
        let client = try await signedInClient(server)
        // A pause that the test does not wait for.
        let retryInterval: Duration = .seconds(5)
        let reader = try await member(client, commit: .onRead, retryInterval: retryInterval)
        #expect(try await batches(reader, 1) == [[0, 1]])
        let seeking = Task { try await reader.seek(partitionID: 1, to: .offset(6)) }
        try #require(
            await eventually { moment == .notStarted ? refused.withLock { $0 } : ownerReads.withLock { $0 } == 1 },
            "the seek write did not reach the point \(moment)")
        let started = ContinuousClock.now

        let closing = Task { try await reader.close() }
        try #require(await eventually { await reader.isClosed })
        storeReplies.release(1)
        ownerReplies.release(1)

        let refusal = await #expect(throws: IggyError.self) { try await seeking.value }
        #expect(refusal?.code == .consumerGroupPartitionNotOwned)
        try await closing.value
        #expect(ContinuousClock.now - started < retryInterval / 2, "the close waited for the pause of the owner read")
        #expect(ownerReads.withLock { $0 } == (moment == .notStarted ? 0 : 1), "an owner read went out after the close started")
        #expect(await requests(server, .storeConsumerOffset).count == 1, "the seek write went again after the close started")
        #expect(topic.stored[1] == 1)
        await client.shutdown()
        await server.stop()
    }

    /// The close belongs to its first caller, and a cancel of a later caller
    /// cancels nothing. A cancel of the first caller, before the close or
    /// while the read that names the owner of a refused rewind keeps getting
    /// a transient refusal, ends the retry of that read at once. The refusal
    /// stands for every caller. Each other rewind still goes once, with no
    /// such read, and the member leaves the group. The reader sends nothing
    /// after the close.
    @Test(arguments: [false, true])
    func aCancelOfTheFirstCallerOfCloseEndsTheOwnerReadsAndTheCloseGoesOn(cancelsBeforeTheClose: Bool) async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 7, 2: 7], assignment: [1, 2], losesReply: 2)
        topic.store(4, for: 1)
        topic.store(4, for: 2)
        let ownerReads = Locked<[UInt32: Int]>([:])
        let allOwnerReads: @Sendable () -> Int = { ownerReads.withLock { $0.values.reduce(0, +) } }
        await serve(server, topic: topic) { request in
            if request.code == CommandCode.storeConsumerOffset.rawValue {
                return .status(IggyErrorCode.consumerGroupPartitionNotOwned.rawValue)
            }
            guard isOwnerRead(request) else {
                return nil
            }
            ownerReads.withLock { $0[polledPartition(request), default: 0] += 1 }
            return .status(IggyErrorCode.transientNotCommitted.rawValue)
        }
        let client = try await signedInClient(server)
        let reader = try await member(client, commit: .onRead)
        // The reads outlive their cancelled caller and lose their replies.
        let pending = Task { try? await reader.next() }
        #expect(await eventually { topic.stored == [1: 6, 2: 6] })
        pending.cancel()
        _ = await pending.value
        let closed = Locked(false)
        let first = Task {
            defer { closed.withLock { $0 = true } }
            if cancelsBeforeTheClose {
                withUnsafeCurrentTask { $0?.cancel() }
            }
            try await reader.close()
        }
        var callers = [first]
        if !cancelsBeforeTheClose {
            #expect(await eventually { allOwnerReads() >= 2 }, "the owner read did not go again after a transient refusal")
            let later = Task { try await reader.close() }
            later.cancel()
            let readsAtLaterCancel = allOwnerReads()
            #expect(await eventually { allOwnerReads() >= readsAtLaterCancel + 2 }, "the cancel of a later caller ended the close")
            callers.append(later)

            first.cancel()
        }

        try #require(await eventually { closed.withLock { $0 } }, "the cancel of the first caller did not end the close")
        for caller in callers {
            let refusal = await #expect(throws: IggyError.self) { try await caller.value }
            #expect(refusal?.code == .consumerGroupPartitionNotOwned)
        }
        let sent = await server.requests.filter { $0.code != CommandCode.ping.rawValue }.count
        try await Task.sleep(for: quietPeriod)
        #expect(await server.requests.filter { $0.code != CommandCode.ping.rawValue }.count == sent, "the reader sent a request after its close ended")
        let rewound = await requests(server, .storeConsumerOffset).map(storedPartition).sorted()
        #expect(rewound == [1, 2], "a rewind did not go exactly once")
        #expect(ownerReads.withLock { $0.count } == (cancelsBeforeTheClose ? 0 : 1), "a rewind sent after the cancel read its owner")
        #expect(await requests(server, .leaveConsumerGroup).count == 1, "the cancelled close did not leave the group")
        #expect(topic.stored == [1: 6, 2: 6])
        await client.shutdown()
        await server.stop()
    }

    /// A cancel of the first caller of the close while the read that names
    /// the owner of a refused rewind waits for its reply keeps the refusal.
    /// The reply then names the owner, and the rewind does not go again. The
    /// rewind of the other partition still goes once, and the member leaves
    /// the group.
    @Test func anOwnerReadInFlightAtTheCancelOfTheCloseSendsNoRewindAgain() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 7, 2: 7], assignment: [1, 2], losesReply: 2)
        topic.store(4, for: 1)
        topic.store(4, for: 2)
        let refused = Locked(false)
        let ownerReads = Locked(0)
        let ownerReplies = AsyncSemaphore(permits: 0)
        await serve(server, topic: topic) { request in
            if request.code == CommandCode.storeConsumerOffset.rawValue, !refused.exchange(true) {
                return .status(IggyErrorCode.consumerGroupPartitionNotOwned.rawValue)
            }
            guard isOwnerRead(request) else {
                return nil
            }
            ownerReads.withLock { $0 += 1 }
            _ = try? await ownerReplies.acquire(1)
            return await topic.reply(to: request)
        }
        let client = try await signedInClient(server)
        let reader = try await member(client, commit: .onRead)
        // The reads outlive their cancelled caller and lose their replies.
        let pending = Task { try? await reader.next() }
        #expect(await eventually { topic.stored == [1: 6, 2: 6] })
        pending.cancel()
        _ = await pending.value
        let closing = Task { try await reader.close() }
        try #require(await eventually { ownerReads.withLock { $0 } == 1 }, "the refused rewind did not read its owner")

        closing.cancel()
        ownerReplies.release(1)

        let refusal = await #expect(throws: IggyError.self) { try await closing.value }
        #expect(refusal?.code == .consumerGroupPartitionNotOwned)
        let rewound = await requests(server, .storeConsumerOffset).map(storedPartition).sorted()
        #expect(rewound == [1, 2], "a rewind went again after the cancel")
        #expect(topic.stored.values.sorted() == [4, 6], "\(topic.stored)")
        #expect(ownerReads.withLock { $0 } == 1)
        #expect(await requests(server, .leaveConsumerGroup).count == 1, "the cancelled close did not leave the group")
        await client.shutdown()
        await server.stop()
    }

    /// The close deadline, ``PartitionReader/closeBudget`` after the start
    /// of the close, ends the retry of the read that names the owner of a
    /// refused rewind without a cancel. The refusal stands, the rewind of
    /// the other partition still goes once, and the member leaves the group.
    @Test func theCloseDeadlineEndsTheOwnerReadOfARefusedRewind() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 7, 2: 7], assignment: [1, 2], losesReply: 2)
        topic.store(4, for: 1)
        topic.store(4, for: 2)
        let refused = Locked(false)
        let ownerReads = Locked(0)
        await serve(server, topic: topic) { request in
            if request.code == CommandCode.storeConsumerOffset.rawValue, !refused.exchange(true) {
                return .status(IggyErrorCode.consumerGroupPartitionNotOwned.rawValue)
            }
            guard isOwnerRead(request) else {
                return nil
            }
            ownerReads.withLock { $0 += 1 }
            // The client hands `notPrimary` of a deferred poll over at once, so the reader reads again after each pause.
            return .status(IggyErrorCode.notPrimary.rawValue)
        }
        let client = try await signedInClient(server)
        let reader = try await member(client, commit: .onRead)
        // The reads outlive their cancelled caller and lose their replies.
        let pending = Task { try? await reader.next() }
        #expect(await eventually { topic.stored == [1: 6, 2: 6] })
        pending.cancel()
        _ = await pending.value
        let closedAt = Locked<ContinuousClock.Instant?>(nil)
        let started = ContinuousClock.now

        let closing = Task {
            defer { closedAt.withLock { $0 = .now } }
            try await reader.close()
        }

        let ended = await eventually(timeout: PartitionReader.closeBudget + .seconds(5)) { closedAt.withLock { $0 } != nil }
        if !ended {
            // Only a cancel ends the owner reads without the deadline.
            closing.cancel()
        }
        #expect(ended, "the close deadline did not end the owner reads")
        let refusal = await #expect(throws: IggyError.self) { try await closing.value }
        #expect(refusal?.code == .consumerGroupPartitionNotOwned)
        let elapsed = try #require(closedAt.withLock { $0 }) - started
        #expect(elapsed >= PartitionReader.closeBudget, "the owner reads ended after \(elapsed), before the close deadline")
        #expect(ownerReads.withLock { $0 } >= 2, "the owner read did not go again after a transient refusal")
        let readsAtTheEnd = ownerReads.withLock { $0 }
        try await Task.sleep(for: quietPeriod)
        #expect(ownerReads.withLock { $0 } == readsAtTheEnd, "an owner read went out after the close ended")
        let rewound = await requests(server, .storeConsumerOffset).map(storedPartition).sorted()
        #expect(rewound == [1, 2], "a rewind did not go exactly once")
        #expect(topic.stored.values.sorted() == [4, 6], "\(topic.stored)")
        #expect(await requests(server, .leaveConsumerGroup).count == 1, "the close did not leave the group")
        await client.shutdown()
        await server.stop()
    }

    /// The start of the close ends the retry of the read that names the
    /// owner of a refused commit, with no cancel. The commit fails with its
    /// refusal and stores nothing again. So the close does not wait for the
    /// close deadline, and the member leaves the group.
    @Test func theCloseStartEndsTheOwnerReadOfARefusedCommit() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 2], assignment: [1])
        let ownerReads = Locked(0)
        await serve(server, topic: topic) { request in
            if request.code == CommandCode.storeConsumerOffset.rawValue {
                return .status(IggyErrorCode.consumerGroupPartitionNotOwned.rawValue)
            }
            guard isOwnerRead(request) else {
                return nil
            }
            ownerReads.withLock { $0 += 1 }
            // The client hands `notPrimary` of a deferred poll over at once, so the reader reads again after each pause.
            return .status(IggyErrorCode.notPrimary.rawValue)
        }
        let client = try await signedInClient(server)
        let reader = try await member(client, commit: .manual)
        let checkpoint = try #require(try await reader.next()?.checkpoint)
        let committing = Task { try await reader.commit(checkpoint) }
        try #require(await eventually { ownerReads.withLock { $0 } >= 2 }, "the owner read did not go again after a transient refusal")
        let closed = Locked(false)

        let closing = Task {
            defer { closed.withLock { $0 = true } }
            try await reader.close()
        }

        let ended = await eventually(timeout: PartitionReader.closeBudget / 2) { closed.withLock { $0 } }
        if !ended {
            // Only a cancel ends the owner reads of the commit when the close start does not.
            closing.cancel()
        }
        #expect(ended, "the close waited for the owner read of the commit")
        try await closing.value
        let refusal = await #expect(throws: IggyError.self) { try await committing.value }
        #expect(refusal?.code == .consumerGroupPartitionNotOwned)
        #expect(await requests(server, .storeConsumerOffset).count == 1, "the commit stored again after the close started")
        #expect(await requests(server, .leaveConsumerGroup).count == 1, "the close did not leave the group")
        await client.shutdown()
        await server.stop()
    }

    /// A counted position of a partition that left stores nothing once it
    /// names an owner of an earlier session. Owner generation 0 would store
    /// it for the assignment that the member holds in the new session.
    @Test func aCountedPositionOfAnEarlierSessionStoresNothingOnceThePartitionLeft() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 2, 2: 2], assignment: [1])
        topic.installOwner(1)
        await serve(server, topic: topic)
        let client = try await signedInClient(server)
        let reader = try await member(client, commit: .auto(AutoCommit(interval: unendingCommitInterval, after: .delivery)))
        #expect(try await batches(reader, 1) == [[0, 1]])
        client.core.groupState.clearSessionScoped()
        topic.installOwner(2)
        topic.assign([2])

        #expect(try await batches(reader, 1) == [[0, 1]])
        try await reader.close()

        #expect(topic.stored[1] == nil, "the counted position of an earlier owner was stored after the partition left")
        #expect(topic.stored[2] == 1)
        await client.shutdown()
        await server.stop()
    }

    /// A committing read of an earlier assignment of a partition decides
    /// nothing at the close, also when it ends after a seek of the current
    /// assignment dropped its batch. So the close leaves the offset that
    /// the read stored.
    @Test func anOnReadCloseLeavesTheOffsetOfAReadOfAnEarlierAssignment() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 10, 2: 4], assignment: [1])
        topic.store(4, for: 1)
        let holding = Locked(false)
        let readEnds = AsyncSemaphore(permits: 0)
        await serve(server, topic: topic) { request in
            guard let fetch = try? request.fetch, fetch.autoCommit, fetch.targets.contains(where: { $0.partitionID == 1 }), !holding.exchange(true)
            else {
                return nil
            }
            _ = try? await readEnds.acquire(1)
            return await topic.reply(to: request)
        }
        let client = try await signedInClient(server)
        let reader = try await member(client, commit: .onRead)
        let held = Task { try? await reader.next() }
        #expect(await eventually { holding.withLock { $0 } })
        held.cancel()
        _ = await held.value
        topic.assign([2])
        client.core.groupState.invalidateAssignment(memberGroupKey)
        let moved = try #require(try await withDeadline(.now + .seconds(5)) { try await reader.next() } ?? nil)
        #expect(moved.partitionID == 2)
        topic.assign([1])
        client.core.groupState.invalidateAssignment(memberGroupKey)
        let returning = Task { try? await reader.next() }
        #expect(await eventually { await reader.partitions == [1] })
        returning.cancel()
        _ = await returning.value
        try await reader.seek(partitionID: 1, to: .last)

        readEnds.release(1)
        try await reader.close()

        #expect(topic.stored[1] == 6, "the close moved the offset back over a read of an earlier assignment")
        await client.shutdown()
        await server.stop()
    }

    /// A seek under `onRead` whose store has an unknown outcome keeps the
    /// position, so the close stores the offset from before that store. The
    /// store may have moved the offset back, which a read never does. So the
    /// close does not first check that the server is past the delivered
    /// offset, or a restart reads 10 to 19 again.
    @Test(arguments: [PollingStrategy.next, .offset(0)])
    func anOnReadCloseRestoresTheDeliveredOffsetAfterASeekBackWithAnUnknownOutcome(start: PollingStrategy) async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 30])
        await serve(server, topic: topic, script: firstWriteWithUnknownOutcome(topic))
        let client = try await signedInClient(server)
        let reader = try await standalone(client, start: start, count: 20, commit: .onRead)
        #expect(try await batches(reader, 1) == [Array(UInt64(0)...19)])

        let refused = await #expect(throws: IggyError.self) { try await reader.seek(partitionID: 1, to: .offset(10)) }

        #expect(refused?.code == .transientNotCommitted)
        #expect(topic.stored[1] == 9)
        try await reader.close()
        #expect(topic.stored[1] == 19, "the close kept the offset that the lost seek store moved back")
        await client.shutdown()
        await server.stop()
    }

    /// A delete with an unknown outcome of a seek under `onRead` to the first
    /// message may have removed the stored offset, which a read never does.
    /// So the next reads go on after that offset, and a close without a read
    /// stores it again. Otherwise a restart reads from zero, or skips to an
    /// offset start past it.
    @Test(arguments: [PollingStrategy.next, .offset(8)], [false, true])
    func anOnReadSeekWhoseDeleteHasAnUnknownOutcomeKeepsTheStoredOffset(start: PollingStrategy, closes: Bool) async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 10])
        topic.store(4, for: 1)
        await serve(server, topic: topic, script: firstWriteWithUnknownOutcome(topic, .deleteConsumerOffset))
        let client = try await signedInClient(server)
        let reader = try await standalone(client, start: start, commit: .onRead)

        let refused = await #expect(throws: IggyError.self) { try await reader.seek(partitionID: 1, to: .first) }

        #expect(refused?.code == .transientNotCommitted)
        #expect(topic.stored[1] == nil)
        if closes {
            try await reader.close()
            #expect(topic.stored[1] == 4)
        } else {
            let read = try await withDeadline(.now + .seconds(3)) { try await batches(reader, 1) }
            #expect(read == [[5, 6]])
            try await reader.close()
        }
        await client.shutdown()
        await server.stop()
    }

    /// The seeks of a partition under `onRead` move it in the order of their
    /// calls. A seek that stores nothing, to `last` or to a timestamp without
    /// a message, waits for the store of an earlier seek, also when the
    /// caller of that seek was cancelled. So the earlier seek never replaces
    /// the position of the later one.
    @Test(arguments: [false, true], [PollingStrategy.last, .timestamp(IggyTimestamp(microseconds: 10))])
    func anOnReadSeekMovesAfterTheStoreOfAnEarlierSeekOfItsPartition(cancels: Bool, later: PollingStrategy) async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 10])
        let storing = Locked(false)
        let storeEnds = AsyncSemaphore(permits: 0)
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.storeConsumerOffset.rawValue else {
                return nil
            }
            storing.withLock { $0 = true }
            _ = try? await storeEnds.acquire(1)
            return await topic.reply(to: request)
        }
        let client = try await signedInClient(server)
        let reader = try await standalone(client, commit: .onRead)
        #expect(try await batches(reader, 1) == [[0, 1]])
        let earlier = Task { try await reader.seek(partitionID: 1, to: .offset(6)) }
        #expect(await eventually { storing.withLock { $0 } })
        if cancels {
            earlier.cancel()
            _ = await earlier.result
        }
        let moved = Locked(false)
        let seeking = Task {
            try await reader.seek(partitionID: 1, to: later)
            moved.withLock { $0 = true }
        }
        try await Task.sleep(for: quietPeriod)
        #expect(!moved.withLock { $0 }, "the later seek moved before the store of the earlier one ended")

        storeEnds.release(1)
        try await seeking.value
        if !cancels {
            try await earlier.value
        }
        topic.append(2, to: 1)
        let after = try #require(try await withDeadline(.now + .seconds(3)) { try await reader.next() } ?? nil)

        #expect(after.messages.map(\.offset) == [10, 11], "the earlier seek replaced the position of the later one")
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// A seek under `onRead` owns the position of the partition it holds. No
    /// check of a `last` position goes out while the store of the seek
    /// waits, and a check that ends then changes nothing. So after the store
    /// failed, the partition checks again and reads the last messages of
    /// that time.
    @Test(arguments: [false, true])
    func anOnReadCheckChangesNothingWhileASeekHoldsItsPartition(checkFirst: Bool) async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 10])
        let storing = Locked(false)
        let checked = Locked(false)
        let storeEnds = AsyncSemaphore(permits: 0)
        let unknownStore = firstWriteWithUnknownOutcome(topic)
        await serve(server, topic: topic) { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .pollMessagesDeferred where checkFirst && !checked.withLock({ $0 }):
                // The check of the start ends while the store of the seek waits.
                while !storing.withLock({ $0 }) {
                    try? await Task.sleep(for: .milliseconds(10))
                }
                checked.withLock { $0 = true }
                return await topic.reply(to: request)
            case .storeConsumerOffset:
                if !storing.exchange(true) {
                    _ = try? await storeEnds.acquire(1)
                }
                return await unknownStore(request)
            default:
                return nil
            }
        }
        let client = try await signedInClient(server)
        let reader = try await standalone(client, start: .last, commit: .onRead)
        var seeking: Task<Void, any Error>?
        if !checkFirst {
            seeking = Task { try await reader.seek(partitionID: 1, to: .offset(6)) }
            #expect(await eventually { storing.withLock { $0 } })
        }
        let pending = Task { try? await reader.next() }
        if checkFirst {
            #expect(await eventually { await !requests(server, .pollMessagesDeferred).isEmpty })
            seeking = Task { try await reader.seek(partitionID: 1, to: .offset(6)) }
            #expect(await eventually { checked.withLock { $0 } })
        }
        let checks = await requests(server, .pollMessagesDeferred).count
        try await Task.sleep(for: quietPeriod)
        #expect(await requests(server, .pollMessagesDeferred).count == checks, "a check went out while a seek held its partition")
        pending.cancel()
        _ = await pending.value
        storeEnds.release(1)
        let refused = await #expect(throws: IggyError.self) { try await seeking?.value }
        #expect(refused?.code == .transientNotCommitted)
        topic.append(4, to: 1)

        let after = try #require(try await withDeadline(.now + .seconds(3)) { try await reader.next() } ?? nil)

        #expect(after.messages.map(\.offset) == [12, 13], "a check that ended during the hold of a seek moved its partition")
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// A close ends a `next()` that pauses after a refused sync or route
    /// lookup at once. The close does not wait for that pause either, so
    /// neither waits for the retry interval of the reader.
    @Test(arguments: [CommandCode.syncConsumerGroup, .getPartitionRoute])
    func aCloseEndsTheRetryPauseOfAWaitingNext(refused: CommandCode) async throws {
        let retryPause: Duration = .seconds(20)
        let closeBound: Duration = .seconds(5)
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 2], assignment: [1])
        let refusing = Locked(false)
        await serve(server, topic: topic) { request in
            request.code == refused.rawValue && refusing.withLock({ $0 }) ? .status(IggyErrorCode.invalidCommand.rawValue) : nil
        }
        let client = try await signedInClient(server)
        let reader = try await client.reader(
            streamID: 1, topicID: 2, consumer: .group(3),
            configuration: ReaderConfiguration(
                options: PollOptions(count: 2, maxWait: .milliseconds(200), commit: .onRead), retryInterval: retryPause))
        refusing.withLock { $0 = true }
        let refusals = await requests(server, refused).count
        let pending = Task { try? await reader.next() }
        #expect(await eventually { await requests(server, refused).count > refusals })
        try await Task.sleep(for: waitEntryPause)
        let started = ContinuousClock.now

        try await reader.close()
        _ = await pending.value

        #expect(ContinuousClock.now - started < closeBound, "the close or the waiting next() waited for the retry pause")
        await client.shutdown()
        await server.stop()
    }

    /// Under `onRead` a start other than `next` looks up the stored offset
    /// of each partition before its first read. The close waits for that
    /// pass, so the lookup in flight at the close start is its last one, and
    /// the waiting `next()` returns nil.
    @Test func theCloseStartEndsTheStartLookupsOfTheStoredOffsets() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 4, 2: 4, 3: 4])
        let lookups = Locked(0)
        let lookupReplies = AsyncSemaphore(permits: 0)
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.getConsumerOffset.rawValue else {
                return nil
            }
            if lookups.withLock({ count in
                count += 1
                return count == 1
            }) {
                _ = try? await lookupReplies.acquire(1)
            }
            return nil
        }
        let client = try await signedInClient(server)
        let reader = try await standalone(client, partitions: [1, 2, 3], start: .last, commit: .onRead)
        let reading = Task { try await reader.next() }
        try #require(await eventually { lookups.withLock { $0 } == 1 }, "the start did not look up a stored offset")

        let closing = Task { try await reader.close() }
        try #require(await eventually { await reader.isClosed })
        lookupReplies.release(1)

        try await closing.value
        #expect(try await reading.value == nil)
        #expect(lookups.withLock { $0 } == 1, "the start looked up stored offsets after the close started")
        await client.shutdown()
        await server.stop()
    }

    /// A start lookup of `next()` that the server refuses until a seek to
    /// `last` under `onRead` moved the partition changes nothing once the
    /// client sends it again and it ends. So the next read reads the last
    /// messages, not those after the stored offset.
    @Test func aStartLookupThatEndsAfterASeekToLastChangesNothing() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 10])
        topic.store(1, for: 1)
        let delayedLookup = Locked<UInt64?>(nil)
        let sought = Locked(false)
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.getConsumerOffset.rawValue else {
                return nil
            }
            let delayed = delayedLookup.withLock { first in
                first = first ?? request.requestID
                return first == request.requestID
            }
            // The client sends the refused lookup again under its request id.
            return delayed && !sought.withLock({ $0 }) ? .status(IggyErrorCode.transientNotAccepted.rawValue) : nil
        }
        let client = try await signedInClient(server)
        let reader = try await standalone(client, start: .offset(0), commit: .onRead)
        let pending = Task { try await withDeadline(.now + .seconds(5)) { try await reader.next() } }
        #expect(await eventually { delayedLookup.withLock { $0 } != nil })

        try await reader.seek(partitionID: 1, to: .last)
        sought.withLock { $0 = true }

        let read = try #require(try await pending.value ?? nil)
        #expect(read.messages.map(\.offset) == [8, 9], "the late start lookup moved the partition to the stored offset")
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// A baseline read of `next()` that ends after a seek to `last` under
    /// `onRead` changes nothing, since the seek moved the partition. Such a
    /// seek writes nothing, so it does not wait for that read. A seek that
    /// stores waits for it, because the client orders a `next` read and a
    /// store of one partition. So the next read reads the last messages.
    @Test func aBaselineReadThatEndsAfterASeekToLastChangesNothing() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 10])
        topic.store(1, for: 1)
        let holding = Locked(false)
        let baselineEnds = AsyncSemaphore(permits: 0)
        await serve(server, topic: topic) { request in
            guard let fetch = try? request.fetch, !fetch.autoCommit, !holding.exchange(true) else {
                return nil
            }
            _ = try? await baselineEnds.acquire(1)
            return await topic.reply(to: request)
        }
        let client = try await signedInClient(server)
        let reader = try await standalone(client, start: .next, commit: .onRead)
        let pending = Task { try await withDeadline(.now + .seconds(5)) { try await reader.next() } }
        #expect(await eventually { holding.withLock { $0 } })

        try await reader.seek(partitionID: 1, to: .last)
        baselineEnds.release(1)

        let read = try #require(try await pending.value ?? nil)
        #expect(read.messages.map(\.offset) == [8, 9], "the late baseline read moved the partition back to the stored offset")
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// A seek under `onRead` whose caller was cancelled after its store went
    /// out, and whose store then fails, logs a warning with the partition
    /// and the error.
    @Test func anOnReadSeekThatFailsAfterItsCallerWasCancelledLogsAWarning() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 10])
        let storing = Locked(false)
        let storeEnds = AsyncSemaphore(permits: 0)
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.storeConsumerOffset.rawValue else {
                return nil
            }
            storing.withLock { $0 = true }
            _ = try? await storeEnds.acquire(1)
            return .status(IggyErrorCode.unauthorized.rawValue)
        }
        let records = Locked<[(level: Logger.Level, metadata: Logger.Metadata)]>([])
        let client = try await signedInClient(server, logger: Logger(label: "reader") { _ in RecordingLogHandler(records: records) })
        let reader = try await standalone(client, commit: .onRead)
        let seeking = Task { try await reader.seek(partitionID: 1, to: .offset(6)) }
        #expect(await eventually { storing.withLock { $0 } })
        seeking.cancel()
        _ = await seeking.result

        storeEnds.release(1)

        let warned = await eventually {
            records.withLock { records in
                records.contains { $0.level == .warning && $0.metadata["partition"] == "1" && "\($0.metadata["error"] ?? "")".contains("unauthorized") }
            }
        }
        #expect(warned, "the failed store of a cancelled seek was not logged")
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// Under `onRead` a read from an offset past the end stores nothing, so
    /// the close after its empty reply, or after its lost reply, finds no
    /// stored offset at that start and leaves the offset: the server refuses
    /// a store before an offset past the end.
    @Test(arguments: [false, true])
    func anOnReadCloseLeavesTheOffsetAfterAReadPastTheEndStoredNothing(lost: Bool) async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 2], losesReply: lost ? 1 : 0)
        let ready = Locked(false)
        let released = Locked(false)
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.fetchMessages.rawValue, (try? request.fetch.autoCommit) == true else {
                return nil
            }
            let reply = await topic.reply(to: request)
            ready.withLock { $0 = true }
            while !released.withLock({ $0 }) {
                try? await Task.sleep(for: .milliseconds(10))
            }
            return reply
        }
        let client = try await signedInClient(server)
        let reader = try await standalone(client, start: .offset(5), commit: .onRead)
        let pending = Task { try? await reader.next() }
        #expect(await eventually { ready.withLock { $0 } })
        pending.cancel()
        _ = await pending.value
        let closing = Task { try await reader.close() }
        released.withLock { $0 = true }
        try await closing.value

        #expect(topic.stored[1] == nil)
        #expect(await requests(server, .storeConsumerOffset).isEmpty)
        await client.shutdown()
        await server.stop()
    }

    /// Under `onRead` a failed start lookup or baseline read fails as a read
    /// of its partition: the error names it, the other partition reads on,
    /// and it reads again after the retry pause.
    @Test(arguments: [false, true])
    func anOnReadFailedStartLookupOrBaselineReadFailsOnlyItsPartition(baseline: Bool) async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 2, 2: 2])
        let refused = Locked(false)
        await serve(server, topic: topic) { request in
            let lookup = request.code == CommandCode.getConsumerOffset.rawValue && UInt32(littleEndianBytes: request.payload.suffix(4)) == 1
            let probe =
                request.code == CommandCode.fetchMessages.rawValue
                && (try? request.fetch).map { !$0.autoCommit && $0.targets.map(\.partitionID) == [1] } == true
            guard baseline ? probe : lookup, !refused.exchange(true) else {
                return nil
            }
            return .status(IggyErrorCode.unauthorized.rawValue)
        }
        let client = try await signedInClient(server)
        let reader = try await standalone(client, partitions: [1, 2], start: baseline ? .next : .offset(0), commit: .onRead)

        let failure = try #require(await readError(reader))

        #expect(failure.partitionID == 1 && failure.error.code == .unauthorized)
        var read: [UInt32: [UInt64]] = [:]
        while read.count < 2, let batch = try await reader.next() {
            read[batch.partitionID] = batch.messages.map(\.offset)
        }
        #expect(read == [1: [0, 1], 2: [0, 1]])
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// A section that does not decode is read again one message at a time:
    /// the messages before the bad one arrive, the error names the bad one
    /// with its offset, its record and the continuation after it, and its
    /// partition reads again only after a seek.
    @Test func anUndecodableMessageIsNamedAndSkippedWithASeek() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 3], undecodable: [1])
        await serve(server, topic: topic)
        let client = try await signedInClient(server)
        let reader = try await standalone(client, start: .first, count: 3)
        #expect(try await batches(reader, 1) == [[0]])
        var failure: ReadError?
        do {
            _ = try await reader.next()
        } catch let error as ReadError {
            failure = error
        }
        let error = try #require(failure)
        #expect(error.partitionID == 1 && error.offset == 1 && error.raw?.isEmpty == false)
        let skip = try #require(error.skip)
        #expect(skip.kind == .offset && skip.value == 2)
        try await reader.seek(partitionID: 1, to: skip)
        #expect(try await batches(reader, 1) == [[2]])
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// A poll of one partition whose reply does not decode is read again one
    /// message at a time, so the error names the bad message and the
    /// continuation after it.
    @Test func anUndecodablePollIsReadAgainOneMessageAtATime() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 3], undecodable: [0], oversized: [0])
        await serve(server, topic: topic)
        let client = try await signedInClient(server)
        let reader = try await standalone(client, start: .first)

        let error = try #require(await readError(reader))

        #expect(error.partitionID == 1 && error.offset == 0 && error.skip?.value == 1)
        try await reader.seek(partitionID: 1, to: try #require(error.skip))
        #expect(try await batches(reader, 1) == [[1, 2]])
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// A gap that a lost read stored is read again one message at a time
    /// once its reply does not decode, so the messages before the bad one
    /// arrive and the error names the bad one.
    @Test func anUndecodableGapReadNamesTheBadMessage() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 6], undecodable: [3])
        await serve(server, topic: topic)
        let client = try await signedInClient(server)
        let reader = try await standalone(client, commit: .onRead)
        #expect(try await batches(reader, 1) == [[0, 1]])
        topic.loseReplies(1)

        #expect(try await batches(reader, 1) == [[2]])
        let error = try #require(await readError(reader))

        #expect(error.partitionID == 1 && error.offset == 3 && error.skip?.value == 4)
        try await reader.seek(partitionID: 1, to: try #require(error.skip))
        #expect(try await batches(reader, 1) == [[4, 5]])
        #expect(topic.stored[1] == 5)
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// An empty committing poll says nothing about where the next read
    /// starts, so the next reply opens no gap.
    @Test func anEmptyCommittingPollKeepsTheExpectedStart() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 2])
        let committingFetches = Locked(0)
        await serve(server, topic: topic) { request in
            if request.code == CommandCode.pollMessagesDeferred.rawValue, polledStrategy(request).autoCommit {
                return .ok(FakeVSRServer.polledBody(partitionID: 1, offsets: []))
            }
            guard request.code == CommandCode.fetchMessages.rawValue, (try? request.fetch.autoCommit) == true else {
                return nil
            }
            let fetch = committingFetches.withLock { count in
                count += 1
                return count
            }
            return fetch == 2 ? .ok(FakeVSRServer.fetchedBody([.init(partitionID: 1, status: IggyErrorCode.invalidSizeBytes.rawValue)])) : nil
        }
        let client = try await signedInClient(server)
        let reader = try await standalone(client, commit: .onRead)
        #expect(try await batches(reader, 1) == [[0, 1]])
        let pending = Task { try await batches(reader, 1) }
        #expect(await eventually { await requests(server, .pollMessagesDeferred).count == 1 })
        topic.append(1, to: 1)

        #expect(try await pending.value == [[2]])
        #expect(await requests(server, .pollMessagesDeferred).count == 1)
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// Two partitions whose history is gone stop in one reply, and each one
    /// reports its own error.
    @Test func everyPartitionThatStopsInOneReplyReportsItsError() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 1, 2: 1])
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.fetchMessages.rawValue, let fetch = try? request.fetch, fetch.targets.allSatisfy({ $0.value == 1 })
            else {
                return nil
            }
            return .ok(
                FakeVSRServer.fetchedBody(
                    fetch.targets.map { FakeVSRServer.FetchedSection(partitionID: $0.partitionID, status: IggyErrorCode.historyUnavailable.rawValue) }))
        }
        let client = try await signedInClient(server)
        let reader = try await standalone(client, partitions: [1, 2], start: .first)
        #expect(try await batches(reader, 2) == [[0], [0]])

        var reported: [UInt32] = []
        for _ in 0..<2 {
            if let error = await readError(reader) {
                #expect(error.error.code == .historyUnavailable)
                reported += error.partitionID.map { [$0] } ?? []
            }
        }

        #expect(reported.sorted() == [1, 2])
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// Tiered data that the object store refuses for good stops its
    /// partition at the first refusal: the read is not sent again after the
    /// retry interval.
    @Test func aReadRefusedWithTieredReadFailedStopsItsPartitionWithoutARetry() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 2])
        await serve(server, topic: topic) { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .fetchMessages:
                guard let fetch = try? request.fetch else {
                    return nil
                }
                return .ok(
                    FakeVSRServer.fetchedBody(
                        fetch.targets.map { FakeVSRServer.FetchedSection(partitionID: $0.partitionID, status: IggyErrorCode.tieredReadFailed.rawValue) }))
            case .pollMessagesDeferred:
                return .status(IggyErrorCode.tieredReadFailed.rawValue)
            default:
                return nil
            }
        }
        let client = try await signedInClient(server)
        let reader = try await standalone(client, start: .first)

        let error = await readError(reader)

        #expect(error?.partitionID == 1 && error?.error.code == .tieredReadFailed)
        #expect(error?.stopsPartition == true)
        // Several retry intervals of the reader pass.
        try await Task.sleep(for: .milliseconds(400))
        let reads = await requests(server, .fetchMessages).count + requests(server, .pollMessagesDeferred).count
        #expect(reads == 1)
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// With `onRead` a partition that reads at `next` reads a baseline
    /// before its first committing read. A baseline that the object store
    /// refuses for good stops the partition, so a later `next()` sends no
    /// baseline read again until a seek.
    @Test func aBaselineReadRefusedWithTieredReadFailedIsNotSentAgainByALaterNext() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 2])
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.fetchMessages.rawValue, let fetch = try? request.fetch else {
                return nil
            }
            return .ok(
                FakeVSRServer.fetchedBody(
                    fetch.targets.map { FakeVSRServer.FetchedSection(partitionID: $0.partitionID, status: IggyErrorCode.tieredReadFailed.rawValue) }))
        }
        let client = try await signedInClient(server)
        let reader = try await standalone(client, commit: .onRead)

        let error = await readError(reader)
        #expect(error?.partitionID == 1 && error?.error.code == .tieredReadFailed)
        #expect(error?.stopsPartition == true)
        let later = Task { try? await reader.next() }
        // Several retry intervals of the reader pass.
        try await Task.sleep(for: .milliseconds(400))

        #expect(await requests(server, .fetchMessages).count == 1)
        later.cancel()
        _ = await later.value
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// A fetch reply takes one record past `maxBytes`. A section refused for
    /// its first message is read with a poll of its own, which returns that
    /// message alone with the same limits.
    @Test func anOversizedFirstMessageOfAFetchSectionIsPolledAlone() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 3], oversized: [0])
        await serve(server, topic: topic)
        let client = try await signedInClient(server)
        let reader = try await standalone(client, start: .first, count: 3)
        #expect(try await batches(reader, 2) == [[0], [1, 2]])
        let polls = await requests(server, .pollMessagesDeferred)
        #expect(polls.count == 1 && polls.first?.deferredOptions.maxBytes == 1_048_576)
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    // MARK: Commit policies

    @Test func aManualReaderStoresOnlyTheCheckpointsItCommits() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 4])
        await serve(server, topic: topic)
        let client = try await signedInClient(server)
        let reader = try await standalone(client, start: .first)
        let first = try #require(try await reader.next())
        _ = try #require(try await reader.next())
        #expect(topic.stored.isEmpty)
        try await reader.commit(try #require(first.checkpoint(at: 0)))
        #expect(topic.stored[1] == 0)
        await expectCode(.invalidCommand) { try await reader.mark(try #require(first.checkpoint)) }
        let other = try await standalone(client, start: .first)
        let foreign = try #require(try await other.next())
        await expectCode(.invalidCommand) { try await reader.commit(try #require(foreign.checkpoint)) }
        try await reader.close()
        try await other.close()
        #expect(topic.stored[1] == 0)

        let onRead = try await standalone(client, start: .first, commit: .onRead)
        let read = try #require(try await onRead.next())
        await expectCode(.invalidCommand) { try await onRead.commit(try #require(read.checkpoint)) }
        try await onRead.close()
        await client.shutdown()
        await server.stop()
    }

    /// `delivery` counts a batch when it is returned, `processing` once the
    /// next one is asked for or the reader closes, `marked` only when it is
    /// marked.
    @Test(arguments: [CommitAfter.delivery, .processing, .marked])
    func anAutoReaderStoresWhatItCounted(after: CommitAfter) async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 4])
        await serve(server, topic: topic)
        let client = try await signedInClient(server)
        let reader = try await standalone(client, start: .first, commit: .auto(AutoCommit(every: .message, after: after)))
        let first = try #require(try await reader.next())
        switch after {
        case .delivery:
            #expect(await eventually { topic.stored[1] == 1 })
        case .processing:
            _ = try #require(try await reader.next())
            #expect(await eventually { topic.stored[1] == 1 })
        case .marked:
            try await reader.mark(try #require(first.checkpoint(at: 0)))
            #expect(await eventually { topic.stored[1] == 0 })
        }
        try await reader.close()
        let closed: [CommitAfter: UInt64] = [.delivery: 1, .processing: 3, .marked: 0]
        #expect(topic.stored[1] == closed[after], "\(after)")
        await client.shutdown()
        await server.stop()
    }

    /// Without an event or an interval the policy stores only on close,
    /// which the options refuse.
    @Test func autoCommitNeedsAnIntervalOrAnEvent() async throws {
        #expect(throws: IggyError.self) { try PollOptions(commit: .auto(AutoCommit())).validate() }
        #expect(throws: IggyError.self) { try PollOptions(commit: .auto(AutoCommit(every: .messages(0)))).validate() }
        try PollOptions(commit: .auto(.default)).validate()
        #expect(AutoCommit.default == AutoCommit(interval: .seconds(5), every: .messages(10_000), after: .processing))
    }

    /// A rebalance stores the counted position of a partition before the
    /// member gives it up.
    @Test func aRebalanceStoresTheCountedPositionFirst() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 2, 2: 2], assignment: [1])
        await serve(server, topic: topic)
        let client = try await signedInClient(server)
        let reader = try await client.reader(
            streamID: 1, topicID: 2, consumer: .group(3),
            configuration: ReaderConfiguration(
                options: PollOptions(count: 2, maxWait: .milliseconds(200), commit: .auto(AutoCommit(interval: .seconds(60), after: .delivery))),
                retryInterval: .milliseconds(50)))
        #expect(try await batches(reader, 1) == [[0, 1]])
        #expect(topic.stored.isEmpty)
        topic.assign([2])
        client.core.groupState.invalidateAssignment(ConsumerGroupState.GroupKey(stream: 1, topic: 2, group: 3))
        #expect(try await batches(reader, 1) == [[0, 1]])
        #expect(topic.stored[1] == 1)
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// With reads that do not wait, a partition that a reply left out or
    /// found empty waits the retry interval instead of being read again at
    /// once.
    @Test(arguments: ["left out", "empty section", "empty poll"])
    func aPartitionAReplyFoundNothingForWaitsTheRetryIntervalWhenReadsDoNotWait(reply: String) async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 0])
        await serve(server, topic: topic) { request in
            reply == "left out" && request.code == CommandCode.fetchMessages.rawValue ? .ok(FakeVSRServer.fetchedBody([])) : nil
        }
        let client = try await signedInClient(server)
        let reader = try await client.reader(
            streamID: 1, topicID: 2, consumer: .consumer(9),
            configuration: ReaderConfiguration(
                partitions: [1], start: reply == "empty poll" ? .last : .next, options: PollOptions(count: 2, maxWait: .zero),
                retryInterval: .milliseconds(50)))
        let pending = Task { try await reader.next() }
        let window: Duration = .milliseconds(500)
        try await Task.sleep(for: window)
        let fetched = await requests(server, .fetchMessages).count
        let polled = await requests(server, .pollMessagesDeferred).count
        pending.cancel()
        try await reader.close()

        #expect((1...12).contains(fetched + polled), "\(reply): \(fetched) fetches and \(polled) polls in \(window)")
        await client.shutdown()
        await server.stop()
    }

    /// Under `onRead` the server stored a read in flight, so its batch
    /// reaches the application after its partition left the assignment.
    @Test func anOnReadReaderHandsOverAReadThatEndsAfterItsPartitionLeft() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 0, 2: 0], assignment: [1, 2])
        await serve(server, topic: topic)
        let client = try await signedInClient(server)
        let reader = try await member(client, commit: .onRead, maxWait: .seconds(10))
        let pending = Task { try await reader.next() }
        #expect(await eventually { (try? await fetches(server).filter(\.autoCommit).count) == 1 })
        topic.assign([1])
        client.core.groupState.invalidateAssignment(memberGroupKey)
        #expect(await eventually { await reader.partitions == [1] })
        topic.append(2, to: 2)
        #expect(await eventually { topic.stored[2] == 1 })
        topic.append(1, to: 1)

        let batch = try #require(try await pending.value)

        #expect(batch.partitionID == 2 && batch.messages.map(\.offset) == [0, 1])
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// A batch that waits for the application when its partition leaves
    /// holds messages that the server stored under `onRead`, so it still
    /// reaches the application. Under `manual` the leave drops it.
    @Test(arguments: [CommitPolicy.onRead, .manual])
    func aQueuedBatchOfAPartitionThatLeavesReachesTheApplicationOnlyUnderOnRead(commit: CommitPolicy) async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 1, 2: 1, 3: 0], assignment: [1, 2, 3])
        let answered = Locked(false)
        await serve(server, topic: topic) { request in
            guard let fetch = try? request.fetch, fetch.targets.count == 3, !answered.exchange(true) else {
                return nil
            }
            // Partition 2 leaves in the sync that the refusal of partition 3 starts at once, while the batches of
            // partitions 1 and 2 wait for the application.
            _ = await topic.reply(to: request)
            topic.assign([1, 3])
            return .ok(
                FakeVSRServer.fetchedBody([
                    FakeVSRServer.FetchedSection(partitionID: 1, offsets: [0]), FakeVSRServer.FetchedSection(partitionID: 2, offsets: [0]),
                    FakeVSRServer.FetchedSection(partitionID: 3, status: IggyErrorCode.consumerGroupPartitionNotOwned.rawValue),
                ]))
        }
        let client = try await signedInClient(server)
        let reader = try await member(client, commit: commit)
        #expect(try #require(try await reader.next()).partitionID == 1)
        try #require(await eventually { await reader.partitions == [1, 3] })
        topic.append(1, to: 1)

        let batch = try #require(try await reader.next())

        let handedOver = commit == .onRead
        #expect(batch.partitionID == (handedOver ? 2 : 1) && batch.messages.map(\.offset) == [handedOver ? 0 : 1])
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// Under `onRead` a partition that leaves while it reads again what a
    /// lost read stored first reports that gap as lost. Then it hands over
    /// the batch that the server stored after the gap. The close does not
    /// move the offset of the partition that left.
    @Test func anOnReadReaderHandsOverTheBatchAfterAGapWhenItsPartitionLeaves() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 6, 2: 1], assignment: [1])
        let gapReads = Locked(0)
        let released = Locked(false)
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.pollMessagesDeferred.rawValue, !polledStrategy(request).autoCommit else {
                return nil
            }
            gapReads.withLock { $0 += 1 }
            while !released.withLock({ $0 }) {
                try? await Task.sleep(for: .milliseconds(10))
            }
            return nil
        }
        let client = try await signedInClient(server)
        let reader = try await member(client, commit: .onRead)
        #expect(try await batches(reader, 1) == [[0, 1]])
        topic.loseReplies(1)
        let pending = Task { try await reader.next() }
        #expect(await eventually { gapReads.withLock { $0 } == 1 })
        topic.assign([2])
        client.core.groupState.invalidateAssignment(memberGroupKey)

        let lost = await #expect(throws: ReadError.self, "the gap of the partition that left was not reported first") { try await pending.value }
        released.withLock { $0 = true }

        #expect(lost?.partitionID == 1 && lost?.error.code == .fetchProgressUncertain)
        #expect(lost?.error.context == "offsets 2 to 3 were stored by a read whose reply was lost")
        #expect(lost?.lost == 2...3)
        let batch = try #require(try await reader.next())
        #expect(batch.partitionID == 1 && batch.messages.map(\.offset) == [4, 5])
        try await reader.close()
        #expect(topic.stored[1] == 5, "the close moved the offset of a partition that left")
        await client.shutdown()
        await server.stop()
    }

    /// Under `onRead` a committing read in flight when its partition leaves
    /// and returns hands over its batch. From a `last` start, the checks of
    /// both partitions wait before it.
    @Test(arguments: [false, true])
    func anOnReadReaderHandsOverAReadAfterItsPartitionLeavesAndReturns(fromLast: Bool) async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 0, 2: 0], assignment: [1, 2])
        let ready = Locked(false)
        let released = Locked(false)
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.fetchMessages.rawValue, (try? request.fetch.autoCommit) == true, !ready.withLock({ $0 }) else {
                return nil
            }
            let reply = await topic.reply(to: request)
            ready.withLock { $0 = true }
            while !released.withLock({ $0 }) {
                try? await Task.sleep(for: .milliseconds(10))
            }
            return reply
        }
        let client = try await signedInClient(server)
        let reader = try await client.reader(
            streamID: 1, topicID: 2, consumer: .group(3),
            configuration: ReaderConfiguration(
                start: fromLast ? .last : .next, options: PollOptions(count: 2, maxWait: .seconds(10), commit: .onRead),
                retryInterval: .milliseconds(50)))
        let finished = Locked(false)
        let pending = Task {
            defer { finished.withLock { $0 = true } }
            return try await reader.next()
        }
        #expect(
            await eventually {
                if fromLast {
                    return await requests(server, .pollMessagesDeferred).count == 2
                }
                return (try? await fetches(server).filter(\.autoCommit).count) == 1
            })
        topic.append(2, to: 2)
        #expect(await eventually { ready.withLock { $0 } && topic.stored[2] == 1 })
        topic.assign([1])
        client.core.groupState.invalidateAssignment(memberGroupKey)
        #expect(await eventually { await reader.partitions == [1] })
        topic.assign([1, 2])
        client.core.groupState.invalidateAssignment(memberGroupKey)
        #expect(await eventually { await reader.partitions == [1, 2] })
        released.withLock { $0 = true }
        #expect(await eventually { finished.withLock { $0 } })
        topic.append(1, to: 1)

        let batch = try #require(try await pending.value)

        #expect(batch.partitionID == 2 && batch.messages.map(\.offset) == [0, 1])
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    @Test(arguments: [false, true])
    func aResumedSessionDoesNotRejoinTheReadersConsumerGroup(resumes: Bool) async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 1], assignment: [1])
        await serve(server, topic: topic)
        let client = try await signedInClient(server)
        let reader = try await member(client, commit: .manual)
        let scope = client.core.groupState.sessionScope
        if resumes {
            await client.core.disconnectTransport()
            try await client.ping()
        } else {
            await client.disconnect()
            try await client.connect()
            _ = try await client.login(username: "iggy", password: "iggy")
        }
        #expect((client.core.groupState.sessionScope == scope) == resumes)

        #expect(try await batches(reader, 1) == [[0]])
        #expect(await requests(server, .joinConsumerGroup).count == (resumes ? 1 : 2))

        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// A read refused for the session leaves the member joined on the server
    /// when the client keeps that session, so the close leaves the group
    /// whatever the reader saw.
    @Test func aMemberLeavesItsGroupOnCloseAfterALostSessionError() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 2], assignment: [1])
        let refused = Locked(false)
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.fetchMessages.rawValue, !refused.exchange(true) else {
                return nil
            }
            return .ok(FakeVSRServer.fetchedBody([.init(partitionID: 1, status: IggyErrorCode.staleClient.rawValue)]))
        }
        let client = try await signedInClient(server)
        let reader = try await member(client, commit: .manual)
        #expect(await readError(reader)?.error.code == .staleClient)

        try await reader.close()

        #expect(await requests(server, .leaveConsumerGroup).count == 1)
        await client.shutdown()
        await server.stop()
    }

    /// The server hands a member a partition once the partition log persisted
    /// the new owner, so a sync right after the join can hold part of the
    /// assignment. The reader syncs again after the settle pause, without a
    /// fence or a heartbeat, and reads the partition the first sync did not
    /// hold.
    @Test func aMemberReadsAPartitionItsFirstSyncDidNotHold() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [2: 1], assignment: [1, 2])
        let syncs = Locked(0)
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.syncConsumerGroup.rawValue else {
                return nil
            }
            let sync = syncs.withLock { count in
                count += 1
                return count
            }
            return sync == 1 ? .ok(FakeVSRServer.assignmentBody(generation: 1, partitions: [1])) : nil
        }
        let client = try await signedInClient(server)
        let reader = try await member(client, commit: .manual)

        let batch = try await withDeadline(.now + .seconds(2)) { try await reader.next() }
        #expect(batch??.partitionID == 2 && batch??.messages.map(\.offset) == [0], "the reader did not read partition 2 within 2 seconds")
        #expect(await requests(server, .syncConsumerGroup).count >= 2)

        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// A first sync that holds no partitions does not wait out the retry
    /// interval, one second by default: the member syncs again after the
    /// settle pause and reads the partition that the second sync holds.
    @Test func aMemberWhoseFirstSyncHoldsNothingSyncsAgainAfterTheSettlePause() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 1], assignment: [])
        let firstSync = Locked(true)
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.syncConsumerGroup.rawValue, firstSync.exchange(false) else {
                return nil
            }
            let reply = await topic.reply(to: request)
            // The server moves the partitions of a member in a new generation.
            topic.assign([1])
            return reply
        }
        let client = try await signedInClient(server)
        let readWithin = PartitionReader.assignmentCheckInterval / 2
        let started = ContinuousClock.now
        let reader = try await client.reader(
            streamID: 1, topicID: 2, consumer: .group(3), configuration: ReaderConfiguration(options: PollOptions(count: 2, maxWait: .milliseconds(200))))

        let batch = try await withDeadline(.now + .seconds(2)) { try await reader.next() }
        let elapsed = ContinuousClock.now - started

        #expect(batch??.partitionID == 1 && batch??.messages.map(\.offset) == [0])
        #expect(elapsed < readWithin, "the member read its partition \(elapsed) after the start")
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// The pause between syncs doubles while the member's partitions hold
    /// still, even as hand-overs between other members move the generation,
    /// so once it reached the check interval the member syncs once per
    /// interval instead of looping.
    @Test func aMemberWithASteadyAssignmentSyncsOncePerCheckInterval() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 0])
        let syncs = Locked<[ContinuousClock.Instant]>([])
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.syncConsumerGroup.rawValue else {
                return nil
            }
            let generation = syncs.withLock { syncs in
                syncs.append(.now)
                return UInt64(syncs.count)
            }
            return .ok(FakeVSRServer.assignmentBody(generation: generation, partitions: [1]))
        }
        let client = try await signedInClient(server)
        let started = ContinuousClock.now
        // The pauses of 10, 20, ... 640 ms add up to 1.27 seconds.
        let settled = started + .seconds(2)
        let window = settled..<settled + PartitionReader.assignmentCheckInterval * 2
        let reader = try await member(client, commit: .manual)
        let pending = Task { try await reader.next() }

        try await Task.sleep(until: window.upperBound, clock: .continuous)
        let synced = syncs.withLock { $0 }

        #expect(synced.filter(window.contains).count == 2, "syncs at \(synced.map { $0 - started }) after the start")
        pending.cancel()
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    // MARK: Consumer

    /// The shutdown of an onRead consumer moves the offset back to the last
    /// message handed over: the server stored the whole batch with the read.
    @Test func anOnReadConsumerMovesTheOffsetBackOnShutdown() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 3])
        await serve(server, topic: topic)
        let client = try await signedInClient(server)
        let consumer = try client.consumer(
            name: "reader", streamID: 1, topicID: 2, partitions: [1],
            configuration: ConsumerConfiguration(pollOptions: PollOptions(count: 3, maxWait: .milliseconds(200), commit: .onRead)))
        try await consumer.initialize()
        #expect(try await read(consumer, 1) == [0])
        #expect(topic.stored[1] == 2)
        await consumer.shutdown()
        #expect(topic.stored[1] == 0)
        #expect(try await consumer.next() == nil)
        await client.shutdown()
        await server.stop()
    }

    /// `delivery` counts a message as it is handed over, `processing` once
    /// the next one is asked for or the consumer shuts down.
    @Test(arguments: [CommitAfter.delivery, .processing])
    func anAutoConsumerFlushesWhatItCountedOnShutdown(after: CommitAfter) async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 5])
        await serve(server, topic: topic)
        let client = try await signedInClient(server)
        let consumer = try client.consumer(
            name: "reader", streamID: 1, topicID: 2, partitions: [1],
            configuration: ConsumerConfiguration(
                pollOptions: PollOptions(count: 5, maxWait: .milliseconds(200), commit: .auto(AutoCommit(interval: .seconds(60), after: after)))))
        try await consumer.initialize()
        #expect(try await read(consumer, 2) == [0, 1])
        #expect(topic.stored.isEmpty)
        await consumer.shutdown()
        #expect(topic.stored[1] == 1, "\(after)")
        await client.shutdown()
        await server.stop()
    }

    /// The shutdown of a consumer is the first caller of the close of its
    /// reader, so at the offset drain timeout it cancels that close. The
    /// read that names the owner of the first rewind, refused as not owned,
    /// then stops. The second rewind still goes, and the server never
    /// answers it, so it ends at the request timeout of the client. The
    /// member then leaves the group, and the shutdown returns.
    @Test func theOffsetDrainTimeoutEndsTheOwnerReadsAndTheShutdownWaitsForTheLastStoreAndTheLeave() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 8, 2: 8], assignment: [1, 2])
        topic.store(4, for: 1)
        topic.store(4, for: 2)
        let refused = Locked(false)
        let ownerReads = Locked(0)
        let ownerReadsAtTheStall = Locked<Int?>(nil)
        await serve(server, topic: topic) { request in
            if request.code == CommandCode.storeConsumerOffset.rawValue {
                if !refused.exchange(true) {
                    return .status(IggyErrorCode.consumerGroupPartitionNotOwned.rawValue)
                }
                let reads = ownerReads.withLock { $0 }
                ownerReadsAtTheStall.withLock { $0 = reads }
                return .stall
            }
            guard isOwnerRead(request) else {
                return nil
            }
            ownerReads.withLock { $0 += 1 }
            return .status(IggyErrorCode.transientNotCommitted.rawValue)
        }
        let requestTimeout: Duration = .seconds(1)
        let client = try await signedInClient(server, requestTimeout: requestTimeout)
        let drainTimeout: Duration = .milliseconds(200)
        let consumer = try client.consumerGroup(
            name: "group", streamID: 1, topicID: 2,
            configuration: ConsumerConfiguration(
                pollOptions: PollOptions(count: 6, maxWait: .milliseconds(200), commit: .onRead), pollingRetryInterval: readerRetryInterval,
                offsetDrainTimeout: drainTimeout))
        try await consumer.initialize()
        // The count leaves room for both partitions in one fetch, so each one has a rewind.
        #expect(try await read(consumer, 1).count == 1)
        #expect(await eventually { topic.stored == [1: 7, 2: 7] }, "\(topic.stored)")
        let started = ContinuousClock.now

        await consumer.shutdown()

        let elapsed = ContinuousClock.now - started
        #expect(ownerReads.withLock { $0 } >= 2, "the owner read did not go again before the drain timeout")
        #expect(ownerReadsAtTheStall.withLock { $0 } == ownerReads.withLock { $0 }, "an owner read went out after the drain timeout")
        #expect(await requests(server, .storeConsumerOffset).count == 2)
        #expect(await requests(server, .leaveConsumerGroup).count == 1, "the cancelled close did not leave the group")
        #expect(elapsed >= drainTimeout + requestTimeout, "the shutdown ended after \(elapsed), before the store that went after the drain timeout timed out")
        #expect(elapsed < drainTimeout + requestTimeout + quietPeriod, "the shutdown waited \(elapsed), past the timeout of that store and the leave")
        await client.shutdown()
        await server.stop()
    }

    /// Under `processing` a handler that throws leaves its message uncounted
    /// and ends the reading with its error.
    @Test func aHandlerThatThrowsEndsProcessingWithoutCountingItsMessage() async throws {
        struct HandlerFailure: Error {}
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 3])
        await serve(server, topic: topic)
        let client = try await signedInClient(server)
        let consumer = try client.consumer(
            name: "reader", streamID: 1, topicID: 2, partitions: [1],
            configuration: ConsumerConfiguration(
                pollOptions: PollOptions(count: 3, maxWait: .milliseconds(200), commit: .auto(AutoCommit(interval: .seconds(60), after: .processing)))))
        try await consumer.initialize()
        await #expect(throws: HandlerFailure.self) {
            try await consumer.consumeMessages { received in
                if received.message.offset == 1 {
                    throw HandlerFailure()
                }
            }
        }
        await consumer.shutdown()
        #expect(topic.stored[1] == 0)
        await client.shutdown()
        await server.stop()
    }

    /// A position whose history is gone stops its partition until a seek, so
    /// the handler run ends with that error instead of going on without the
    /// partition.
    @Test func aReadErrorThatStopsItsPartitionEndsConsumeMessages() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 2])
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.fetchMessages.rawValue, let fetch = try? request.fetch, fetch.targets.allSatisfy({ $0.value == 1 })
            else {
                return nil
            }
            return .ok(FakeVSRServer.fetchedBody([.init(partitionID: 1, status: IggyErrorCode.historyUnavailable.rawValue)]))
        }
        let client = try await signedInClient(server)
        let consumer = try client.consumer(
            name: "reader", streamID: 1, topicID: 2, partitions: [1],
            configuration: ConsumerConfiguration(pollingStrategy: .first, pollOptions: PollOptions(count: 1, maxWait: .milliseconds(200))))
        try await consumer.initialize()
        let handled = Locked<[UInt64]>([])

        let error = await #expect(throws: ReadError.self, "the run did not end within 2 seconds") {
            try await withDeadline(.now + .seconds(2)) {
                try await consumer.consumeMessages { received in
                    handled.withLock { $0.append(received.message.offset) }
                }
            }
        }

        #expect(error?.partitionID == 1 && error?.error.code == .historyUnavailable)
        #expect(handled.withLock { $0 } == [0])
        await consumer.shutdown()
        await client.shutdown()
        await server.stop()
    }

    /// A lost range is the only report of that loss, so the handler run ends
    /// with it instead of logging it and reading on. Its partition does not
    /// stop: the next read hands over the batch after the range.
    @Test func aLostRangeEndsConsumeMessages() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 6])
        await serve(server, topic: topic, script: firstGapRead(answeredWith: FakeVSRServer.polledBody(partitionID: 1, offsets: [])))
        let client = try await signedInClient(server)
        let consumer = try client.consumer(
            name: "reader", streamID: 1, topicID: 2, partitions: [1],
            configuration: ConsumerConfiguration(
                pollOptions: PollOptions(count: 2, maxWait: .milliseconds(200), commit: .onRead), pollingRetryInterval: readerRetryInterval))
        try await consumer.initialize()
        #expect(try await read(consumer, 2) == [0, 1])
        topic.loseReplies(1)
        let handled = Locked<[UInt64]>([])

        let error = try await #require(throws: ReadError.self, "the run did not end with the lost range within 2 seconds") {
            try await withDeadline(.now + .seconds(2)) {
                try await consumer.consumeMessages { received in
                    handled.withLock { $0.append(received.message.offset) }
                }
            }
        }

        #expect(error.partitionID == 1 && error.lost == 2...3)
        #expect(handled.withLock { $0 } == [])
        #expect(try await read(consumer, 2) == [4, 5])
        await consumer.shutdown()
        await client.shutdown()
        await server.stop()
    }

    /// A seek can replace the generation of a partition while the consumer
    /// waits for the batch it then buffers. That batch is dropped, and the
    /// batches of the other partitions still arrive.
    @Test func aConsumerDropsABufferedBatchOfAGenerationASeekReplaced() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 3, 2: 3])
        await serve(server, topic: topic)
        let client = try await signedInClient(server)
        let consumer = try client.consumer(
            name: "reader", streamID: 1, topicID: 2, partitions: [1, 2],
            configuration: ConsumerConfiguration(pollingStrategy: .first, pollOptions: PollOptions(count: 6, maxWait: .milliseconds(200))))
        try await consumer.initialize()
        let first = try #require(try await consumer.next())
        let stale = first.checkpoint.stamp?.generation
        let other: UInt32 = first.partitionID == 1 ? 2 : 1

        try await #require(await consumer.reader).seek(partitionID: first.partitionID, to: .offset(0))
        var received: [ReceivedMessage] = []
        while received.count < 5, let message = try await withDeadline(.now + .seconds(2), { try await consumer.next() }) ?? nil {
            received.append(message)
        }

        let replaced = received.filter { $0.partitionID == first.partitionID && $0.checkpoint.stamp?.generation == stale }
        #expect(replaced.isEmpty, "\(replaced.map(\.message.offset))")
        #expect(received.filter { $0.partitionID == other }.map(\.message.offset) == [0, 1, 2])
        await consumer.shutdown()
        await client.shutdown()
        await server.stop()
    }

    /// Without replay, a message at or below the last one handed over is
    /// skipped, unless a seek moved the partition back.
    @Test func aSeekHandsMessagesOverAgain() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 3])
        await serve(server, topic: topic)
        let client = try await signedInClient(server)
        let consumer = try client.consumer(
            name: "reader", streamID: 1, topicID: 2, partitions: [1],
            configuration: ConsumerConfiguration(pollingStrategy: .first, pollOptions: PollOptions(count: 3, maxWait: .milliseconds(200))))
        try await consumer.initialize()
        #expect(try await read(consumer, 3) == [0, 1, 2])
        try await consumer.seek(partitionID: 1, to: .offset(1))
        #expect(try await read(consumer, 2) == [1, 2])
        #expect(await consumer.lastConsumedOffset(partitionID: 1) == 2)
        await consumer.shutdown()
        await client.shutdown()
        await server.stop()
    }
}
