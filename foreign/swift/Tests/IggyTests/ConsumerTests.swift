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
import Testing

@testable import Iggy

private func signedInClient(_ server: FakeVSRServer) async throws -> IggyClient {
    var configuration = ClientConfiguration(address: server.address, autoLogin: .usernamePassword(username: "iggy", password: "iggy"))
    configuration.reconnection = ReconnectionOptions(maxRetries: nil, interval: .milliseconds(20), reestablishAfter: .zero)
    configuration.requestTimeout = .seconds(10)
    configuration.connectTimeout = .seconds(2)
    configuration.heartbeatInterval = .seconds(60)
    let client = IggyClient(configuration: configuration, connector: NIOConnector(), logger: testLogger)
    try await client.connect()
    return client
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

/// A topic as the server reads it. Partition `p` holds the messages at
/// `0..<ends[p]`. A `next` read starts after the stored offset, a read that
/// commits stores the last offset it returns, and a read that finds nothing
/// waits for a message until its wait ends. A message in `undecodable` does
/// not decode. A message in `oversized` comes alone to a poll, and a fetch
/// refuses its section with `invalidSizeBytes`. `losesReply` drops the reply
/// of that many committing fetches after they stored their offsets.
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

        func start(_ partition: UInt32, kind: PollingStrategy.Kind, value: UInt64, count: UInt32) -> UInt64 {
            let end = ends[partition, default: 0]
            switch kind {
            case .next: return stored[partition].map { $0 + 1 } ?? 0
            case .offset: return value
            case .last: return end > UInt64(count) ? end - UInt64(count) : 0
            case .first, .timestamp: return 0
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
        switch request.code.flatMap(CommandCode.init(rawValue:)) {
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
        case .storeConsumerOffset:
            state.withLock { $0.stored[storedPartition(request)] = storedOffset(request) }
            return .ok([])
        case .deleteConsumerOffset:
            let end = request.payload.count
            let partition = UInt32(littleEndianBytes: request.payload[end - 5..<end - 1])
            state.withLock { $0.stored[partition] = nil }
            return .ok([])
        case .pollMessagesDeferred:
            return await poll(request)
        case .fetchMessages:
            return await fetch(request)
        default:
            return nil
        }
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
        let (offsets, undecodable) = state.withLock { state in
            let offsets = state.read(
                partition, from: state.start(partition, kind: kind, value: poll.value, count: poll.count), count: UInt64(poll.count),
                commits: poll.autoCommit)
            return (offsets, !state.undecodable.isDisjoint(with: offsets))
        }
        let body = FakeVSRServer.polledBody(partitionID: partition, offsets: offsets)
        // A record cut short does not decode.
        return .ok(undecodable ? Array(body.dropLast(4)) : body)
    }

    /// Reads the targets in request order while the count leaves room.
    private func fetch(_ request: FakeVSRServer.Request) async -> FakeVSRServer.Reply {
        guard let fetch = try? request.fetch else {
            return .status(IggyErrorCode.invalidCommand.rawValue)
        }
        await waitForMessages(upTo: fetch.wait) { state in
            fetch.targets.contains { state.holds($0.partitionID, from: state.start($0.partitionID, kind: $0.kind, value: $0.value, count: fetch.count)) }
        }
        let (sections, losesReply) = state.withLock { state in
            var room = UInt64(fetch.count)
            let sections = fetch.targets.map { target in
                let start = state.start(target.partitionID, kind: target.kind, value: target.value, count: fetch.count)
                if state.oversized.contains(start), state.holds(target.partitionID, from: start) {
                    return FakeVSRServer.FetchedSection(partitionID: target.partitionID, status: IggyErrorCode.invalidSizeBytes.rawValue)
                }
                let offsets = state.read(target.partitionID, from: start, count: room, commits: fetch.autoCommit)
                room -= UInt64(offsets.count)
                var section = FakeVSRServer.FetchedSection(
                    partitionID: target.partitionID, offsets: offsets, nextOffset: offsets.last.map { $0 + 1 } ?? start,
                    currentOffset: state.reportsHead ? state.ends[target.partitionID].map { $0 > 0 ? $0 - 1 : 0 } : nil)
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

private func standalone(
    _ client: IggyClient, partitions: [UInt32] = [1], start: PollingStrategy = .next, count: UInt32 = 2, commit: CommitPolicy = .manual
) async throws -> PartitionReader {
    try await client.reader(
        streamID: 1, topicID: 2, consumer: .consumer(9),
        configuration: ReaderConfiguration(
            partitions: partitions, start: start, options: PollOptions(count: count, maxWait: .milliseconds(200), commit: commit),
            retryInterval: .milliseconds(50)))
}

/// A member of group 3 that reads two messages a time.
private func member(_ client: IggyClient, commit: CommitPolicy, maxWait: Duration = .milliseconds(200)) async throws -> PartitionReader {
    try await client.reader(
        streamID: 1, topicID: 2, consumer: .group(3),
        configuration: ReaderConfiguration(
            options: PollOptions(count: 2, maxWait: maxWait, commit: commit), retryInterval: .milliseconds(50)))
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
    /// the partition starts, and the new generation then reads from the
    /// sought offset without a round trip for its stored offset.
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
        released.withLock { $0 = true }
        try await seeking.value

        let batch = (await pending.value) ?? nil
        #expect(batch?.messages.map(\.offset) == [3, 4])
        let first = try #require(try await fetches(server).first)
        #expect(first.autoCommit && first.targets.map(\.kind) == [.offset] && first.targets.map(\.value) == [3])
        #expect(topic.stored[1] == 4)
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// There is no offset before zero, so a seek under `onRead` to offset
    /// zero stores and deletes nothing: the first read from zero stores.
    @Test func anOnReadSeekToOffsetZeroStoresNothing() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 2])
        await serve(server, topic: topic)
        let client = try await signedInClient(server)
        let reader = try await standalone(client, commit: .onRead)
        #expect(try await batches(reader, 1) == [[0, 1]])

        try await reader.seek(partitionID: 1, to: .offset(0))

        #expect(topic.stored[1] == 1)
        #expect(await requests(server, .storeConsumerOffset).isEmpty)
        #expect(await requests(server, .deleteConsumerOffset).isEmpty)
        #expect(try await batches(reader, 1) == [[0, 1]])
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    /// A seek under `onRead` whose store the server refuses fails with that
    /// error and moves nothing: the partition reads on in its generation.
    @Test func anOnReadSeekWhoseStoreIsRefusedMovesNothing() async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 4])
        await serve(server, topic: topic) { request in
            request.code == CommandCode.storeConsumerOffset.rawValue ? .status(IggyErrorCode.unauthorized.rawValue) : nil
        }
        let client = try await signedInClient(server)
        let reader = try await standalone(client, commit: .onRead)
        let before = try #require(try await reader.next())

        let refused = await #expect(throws: IggyError.self) { try await reader.seek(partitionID: 1, to: .offset(4)) }

        #expect(refused?.code == .unauthorized)
        let after = try #require(try await reader.next())
        #expect(after.messages.map(\.offset) == [2, 3] && after.stamp?.generation == before.stamp?.generation)
        #expect(topic.stored[1] == 3)
        try await reader.close()
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
        let seeking = Task { try await reader.seek(partitionID: 1, to: .offset(1)) }
        let closing = Task { try await reader.close() }
        released.withLock { $0 = true }
        try await seeking.value
        try await closing.value
        _ = await pending.value

        #expect(topic.stored[1] == 0)
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

    /// Under `onRead` a partition that leaves while it reads again what a
    /// lost read stored hands over the batch the server stored after that
    /// gap, and reports the gap as lost.
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

        let batch = try #require(try await pending.value)

        #expect(batch.partitionID == 1 && batch.messages.map(\.offset) == [4, 5])
        do {
            _ = try await reader.next()
            Issue.record("the gap of the partition that left was not reported")
        } catch let error as ReadError {
            #expect(error.partitionID == 1 && error.error.code == .fetchProgressUncertain)
        }
        released.withLock { $0 = true }
        try await reader.close()
        await client.shutdown()
        await server.stop()
    }

    @Test(arguments: [false, true])
    func anOnReadReaderHandsOverAReadAfterItsPartitionLeavesAndReturns(polls: Bool) async throws {
        let server = try await FakeVSRServer.started()
        let topic = FakeTopic(ends: [1: 0, 2: 0], assignment: [1, 2])
        let ready = Locked(false)
        let released = Locked(false)
        let command: CommandCode = polls ? .pollMessagesDeferred : .fetchMessages
        await serve(server, topic: topic) { request in
            guard request.code == command.rawValue, polls || (try? request.fetch.autoCommit) == true,
                !polls || polledPartition(request) == 2, !ready.withLock({ $0 })
            else {
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
                start: polls ? .last : .next, options: PollOptions(count: 2, maxWait: .seconds(10), commit: .onRead),
                retryInterval: .milliseconds(50)))
        let finished = Locked(false)
        let pending = Task {
            defer { finished.withLock { $0 = true } }
            return try await reader.next()
        }
        #expect(
            await eventually {
                if polls {
                    return await requests(server, command).count == 2
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

    /// The server hands a member its partitions as its shards learn of the
    /// join, so a sync right after it can hold part of the assignment. The
    /// reader syncs again at its check interval, without a fence or a
    /// heartbeat, and reads the partition the first sync did not hold.
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
