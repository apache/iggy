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

private func signedInClient(
    _ server: FakeVSRServer, requestTimeout: Duration = .seconds(10), heartbeatInterval: Duration = .seconds(60)
) async throws -> IggyClient {
    var configuration = ClientConfiguration(address: server.address, autoLogin: .usernamePassword(username: "iggy", password: "iggy"))
    configuration.reconnection = ReconnectionOptions(maxRetries: nil, interval: .milliseconds(20), reestablishAfter: .zero)
    configuration.requestTimeout = requestTimeout
    configuration.connectTimeout = .seconds(2)
    configuration.heartbeatInterval = heartbeatInterval
    let client = IggyClient(configuration: configuration, connector: NIOConnector(), logger: testLogger)
    try await client.connect()
    return client
}

/// Answers the lookups a consumer makes before it reads: the stream, the
/// topic and the group exist. `poll` answers every poll.
private func serve(_ server: FakeVSRServer, poll: @escaping @Sendable (FakeVSRServer.Request) async -> FakeVSRServer.Reply) async {
    await server.respond { request in
        switch request.code.flatMap(CommandCode.init(rawValue:)) {
        case .getStream: return .ok(GoldenFixture.shared.bytes("response.get_stream"))
        case .getTopic: return .ok(GoldenFixture.shared.bytes("response.get_topic"))
        case .getConsumerGroup, .createConsumerGroup: return .ok(GoldenFixture.shared.bytes("response.consumer_group_details"))
        case .syncConsumerGroup: return .ok(FakeVSRServer.assignmentBody(generation: 1, partitions: [1]))
        case .pollMessagesDeferred: return await poll(request)
        default: return FakeVSRServer.defaultReply(request, address: server.address)
        }
    }
}

/// Answers each poll with one message, as a partition holding `count`
/// messages does: `next` starts after the stored offset, or at zero when
/// none is stored, and a poll that commits stores the offset it returns.
private func serveOneByOne(_ server: FakeVSRServer, count: UInt64) async {
    let stored = Locked<UInt64?>(nil)
    await server.respond { request in
        switch request.code.flatMap(CommandCode.init(rawValue:)) {
        case .getStream: return .ok(GoldenFixture.shared.bytes("response.get_stream"))
        case .getTopic: return .ok(GoldenFixture.shared.bytes("response.get_topic"))
        case .storeConsumerOffset:
            stored.withLock { $0 = storedOffset(request) }
            return .ok([])
        case .pollMessagesDeferred:
            let poll = polledStrategy(request)
            let offset = poll.kind == PollingStrategy.Kind.next.rawValue ? stored.withLock { $0.map { $0 + 1 } ?? 0 } : poll.value
            guard offset < count else {
                return .ok(FakeVSRServer.polledBody(partitionID: 1, offsets: []))
            }
            if poll.autoCommit {
                stored.withLock { $0 = offset }
            }
            return .ok(FakeVSRServer.polledBody(partitionID: 1, offsets: [offset]))
        default: return FakeVSRServer.defaultReply(request, address: server.address)
        }
    }
}

/// Answers the polls with the batches in turn, then with empty polls.
private func batches(
    _ batches: [[UInt64]], partitionID: UInt32 = 1, context: PartitionContext = FakeVSRServer.routeContext
) -> @Sendable (FakeVSRServer.Request) async -> FakeVSRServer.Reply {
    let next = Locked(0)
    return { _ in
        let index = next.withLock { index in
            defer { index += 1 }
            return index
        }
        let offsets = index < batches.count ? batches[index] : []
        return .ok(FakeVSRServer.polledBody(partitionID: partitionID, offsets: offsets, context: context))
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

/// The topic of a group member as the server reads it. Partition `p` holds
/// the messages at `0..<ends[p]`. A `next` read starts after the stored
/// offset, a read that commits stores the last offset it returns, and a
/// read that finds nothing waits for a message until its wait ends.
private final class FakeTopic: Sendable {
    private struct State {
        var ends: [UInt32: UInt64]
        var stored: [UInt32: UInt64] = [:]
        var assignment: [UInt32]
        var generation: UInt64 = 1

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
            let end = Swift.min(ends[partition, default: 0], start + count)
            guard start < end else {
                return []
            }
            if commits {
                stored[partition] = end - 1
            }
            return Array(start..<end)
        }
    }

    private static let waitStep: Duration = .milliseconds(10)
    private let state: Locked<State>

    init(ends: [UInt32: UInt64], assignment: [UInt32]) {
        state = Locked(State(ends: ends, assignment: assignment))
    }

    var stored: [UInt32: UInt64] {
        state.withLock { $0.stored }
    }

    func append(_ count: UInt64, to partition: UInt32) {
        state.withLock { $0.ends[partition, default: 0] += count }
    }

    /// A rebalance: the member holds `partitions` in the next generation.
    func assign(_ partitions: [UInt32]) {
        state.withLock { state in
            state.assignment = partitions
            state.generation += 1
        }
    }

    /// The answer to an assignment sync, a store, a poll or a fetch. Nil for
    /// any other request.
    func reply(to request: FakeVSRServer.Request) async -> FakeVSRServer.Reply? {
        switch request.code.flatMap(CommandCode.init(rawValue:)) {
        case .syncConsumerGroup:
            let (generation, partitions) = state.withLock { ($0.generation, $0.assignment) }
            return .ok(FakeVSRServer.assignmentBody(generation: generation, partitions: partitions))
        case .storeConsumerOffset:
            state.withLock { $0.stored[storedPartition(request)] = storedOffset(request) }
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
        let offsets = state.withLock { state in
            state.read(
                partition, from: state.start(partition, kind: kind, value: poll.value, count: poll.count), count: UInt64(poll.count),
                commits: poll.autoCommit)
        }
        return .ok(FakeVSRServer.polledBody(partitionID: partition, offsets: offsets))
    }

    /// Reads the targets in request order while the count leaves room.
    private func fetch(_ request: FakeVSRServer.Request) async -> FakeVSRServer.Reply {
        guard let fetch = try? request.fetch else {
            return .status(IggyErrorCode.invalidCommand.rawValue)
        }
        await waitForMessages(upTo: fetch.wait) { state in
            fetch.targets.contains { state.holds($0.partitionID, from: state.start($0.partitionID, kind: $0.kind, value: $0.value, count: fetch.count)) }
        }
        let sections = state.withLock { state in
            var room = UInt64(fetch.count)
            return fetch.targets.map { target in
                let start = state.start(target.partitionID, kind: target.kind, value: target.value, count: fetch.count)
                let offsets = state.read(target.partitionID, from: start, count: room, commits: fetch.autoCommit)
                room -= UInt64(offsets.count)
                return FakeVSRServer.FetchedSection(partitionID: target.partitionID, offsets: offsets, nextOffset: offsets.last.map { $0 + 1 } ?? start)
            }
        }
        return .ok(FakeVSRServer.fetchedBody(sections))
    }

    private func waitForMessages(upTo wait: UInt64, until holds: (State) -> Bool) async {
        let deadline = ContinuousClock.now + .microseconds(wait)
        while ContinuousClock.now < deadline, !state.withLock({ holds($0) }) {
            try? await Task.sleep(for: Self.waitStep)
        }
    }
}

/// Answers a member of a group as a server that holds `topic` does: the
/// stream, the topic and the group exist. `script` answers first, where it
/// returns a reply.
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

private final class WeakConsumer: @unchecked Sendable {
    weak var value: IggyConsumer?
}

@Suite("Consumer", .serialized)
struct ConsumerTests {
    // MARK: Reading

    /// A batch is handed over one message at a time, and the next poll of the
    /// partition continues after the last one handed over, under the context
    /// that delivered it.
    @Test func aPollContinuesAfterTheLastMessageHandedOver() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await serve(server, poll: batches([[0, 1, 2], [3, 4]]))
        let client = try await signedInClient(server)
        let configuration = ConsumerConfiguration(pollingStrategy: .offset(0), batchLength: 3, autoCommit: .disabled)
        let consumer = try client.consumer(name: "reader", streamID: 1, topicID: 2, partitionID: 1, configuration: configuration)
        try await consumer.initialize()
        #expect(try await read(consumer, 5) == [0, 1, 2, 3, 4])
        let polls = await requests(server, .pollMessagesDeferred)
        #expect(polls.prefix(2).map { polledStrategy($0).value } == [0, 3])
        #expect(polls[1].context == FakeVSRServer.routeContext)
        #expect(await consumer.currentPartitionID == 1)
        #expect(await consumer.lastConsumedOffset(partitionID: 1) == 4)
        await consumer.shutdown()
        await client.shutdown()
    }

    /// A `next` poll starts after the offset stored on the server. Unless the
    /// poll commits that offset itself, it can return the same messages
    /// again, so the consumer continues after the last one handed over.
    @Test func nextContinuesAfterTheLastMessageUnlessThePollCommits() async throws {
        let next = PollingStrategy.Kind.next.rawValue
        let offset = PollingStrategy.Kind.offset.rawValue
        let cases: [(AutoCommit, [UInt8])] = [
            (.when(.consumingEveryNthMessage(2)), [next, offset, offset]), (.intervalOrWhen(.seconds(1), .pollingMessages), [next, next, next]),
        ]
        for (autoCommit, kinds) in cases {
            let server = try await FakeVSRServer.started()
            await serveOneByOne(server, count: 3)
            let client = try await signedInClient(server)
            let configuration = ConsumerConfiguration(batchLength: 1, autoCommit: autoCommit)
            let consumer = try client.consumer(name: "reader", streamID: 1, topicID: 2, partitionID: 1, configuration: configuration)
            try await consumer.initialize()
            // A consumer that stalls reads nothing more, so the read is bounded.
            #expect(try await withDeadline(.now + .seconds(5)) { try await read(consumer, 3) } == [0, 1, 2], "\(autoCommit)")
            #expect(await requests(server, .pollMessagesDeferred).prefix(3).map { polledStrategy($0).kind } == kinds, "\(autoCommit)")
            await consumer.shutdown()
            await client.shutdown()
            await server.stop()
        }
    }

    /// A message at or below the last one handed over from the same
    /// partition history is skipped, unless replay is allowed. Another
    /// incarnation is another history.
    @Test func aReturnedMessageIsHandedOverAgainOnlyWithReplay() async throws {
        let renewed = PartitionContext(incarnation: 8, ownerGeneration: 0, metadataOp: 11)
        for (allowReplay, expected) in [(false, [UInt64(0), 1, 2, 1]), (true, [0, 1, 1, 2, 1])] {
            let server = try await FakeVSRServer.started()
            let returned = Locked(0)
            await serve(server) { _ in
                let poll = returned.withLock { count in
                    defer { count += 1 }
                    return count
                }
                switch poll {
                case 0: return .ok(FakeVSRServer.polledBody(partitionID: 1, offsets: [0, 1]))
                case 1: return .ok(FakeVSRServer.polledBody(partitionID: 1, offsets: [1, 2]))
                case 2: return .ok(FakeVSRServer.polledBody(partitionID: 1, offsets: [1], context: renewed))
                default: return .ok(FakeVSRServer.polledBody(partitionID: 1, offsets: []))
                }
            }
            let client = try await signedInClient(server)
            let configuration = ConsumerConfiguration(autoCommit: .disabled, allowReplay: allowReplay)
            let consumer = try client.consumer(name: "reader", streamID: 1, topicID: 2, partitionID: 1, configuration: configuration)
            try await consumer.initialize()
            #expect(try await read(consumer, expected.count) == expected)
            await consumer.shutdown()
            await client.shutdown()
            await server.stop()
        }
    }

    /// A partition that a fence took away is read again from the strategy,
    /// and the messages handed over before are skipped. A batch that held
    /// nothing else still moves the partition on, as the Rust SDK moves its
    /// fetch position after every batch, so it is not read again forever.
    /// The fence only syncs the assignment again, so the reader sees no error.
    @Test func aBatchOfOnlyMessagesHandedOverMovesThePartitionOn() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let fenced = Locked(false)
        await serve(server, topic: FakeTopic(ends: [1: 3], assignment: [1])) { request in
            guard request.code == CommandCode.fetchMessages.rawValue, !fenced.exchange(true) else {
                return nil
            }
            return .ok(FakeVSRServer.fetchedBody([.init(partitionID: 1, status: IggyErrorCode.consumerGroupPartitionNotOwned.rawValue)]))
        }
        let client = try await signedInClient(server)
        let configuration = ConsumerConfiguration(pollingStrategy: .first, batchLength: 2, autoCommit: .disabled, pollingRetryInterval: .milliseconds(10))
        let consumer = try client.consumerGroup(name: "billing", streamID: 1, topicID: 2, configuration: configuration)
        try await consumer.initialize()
        #expect(try await withDeadline(.now + .seconds(5)) { try await read(consumer, 3) } == [0, 1, 2])
        let first = PollingStrategy.Kind.first.rawValue
        #expect(await requests(server, .pollMessagesDeferred).map { polledStrategy($0).kind } == [first, first])
        #expect(try await fetches(server).prefix(2).map { $0.targets.map(\.value) } == [[2], [2]])
        #expect(await requests(server, .syncConsumerGroup).count >= 2)
        await consumer.shutdown()
        await client.shutdown()
    }

    /// A failed poll is thrown after the retry interval and the consumer
    /// keeps reading on the next call, while an iterator ends at the error.
    @Test func aFailedPollIsThrownAndReadingGoesOn() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let failed = Locked(false)
        await serve(server) { _ in
            failed.exchange(true) ? .ok(FakeVSRServer.polledBody(partitionID: 1, offsets: [0])) : .status(IggyErrorCode.partitionNotFound.rawValue)
        }
        let client = try await signedInClient(server)
        let configuration = ConsumerConfiguration(autoCommit: .disabled, pollingRetryInterval: .milliseconds(150))
        let consumer = try client.consumer(name: "reader", streamID: 1, topicID: 2, partitionID: 1, configuration: configuration)
        try await consumer.initialize()
        var iterator = consumer.makeAsyncIterator()
        let clock = ContinuousClock()
        let elapsed = await clock.measure {
            await expectCode(.partitionNotFound) { _ = try await iterator.next() }
        }
        #expect(elapsed >= .milliseconds(140))
        // An iterator that reads on takes the message, and the read below
        // then waits for good.
        try #require(try await iterator.next() == nil)
        #expect(try await consumer.next()?.message.offset == 0)
        await consumer.shutdown()
        await client.shutdown()
    }

    /// With no heartbeat and no commit to send, nothing else brings the
    /// client back after a lost poll, so the next poll signs it in again. A
    /// poll still waiting for the client ends with nil at shutdown.
    @Test func aPollSignsTheClientInAgainUntilTheConsumerShutsDown() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let polls = Locked(0)
        let refusesSignIn = Locked(false)
        let refusedSignIns = Locked(0)
        await server.respond { request in
            if refusesSignIn.withLock({ $0 }), FakeVSRServer.sessionReply(request) != nil {
                refusedSignIns.withLock { $0 += 1 }
                return .stall
            }
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .getStream: return .ok(GoldenFixture.shared.bytes("response.get_stream"))
            case .getTopic: return .ok(GoldenFixture.shared.bytes("response.get_topic"))
            case .pollMessagesDeferred:
                let poll = polls.withLock { poll in
                    defer { poll += 1 }
                    return poll
                }
                return poll == 1 ? .ok(FakeVSRServer.polledBody(partitionID: 1, offsets: [0])) : .close
            default: return FakeVSRServer.defaultReply(request, address: server.address)
            }
        }
        let client = try await signedInClient(server, requestTimeout: .milliseconds(500), heartbeatInterval: .zero)
        let configuration = ConsumerConfiguration(autoCommit: .disabled, pollingRetryInterval: .milliseconds(100))
        let consumer = try client.consumer(name: "reader", streamID: 1, topicID: 2, partitionID: 1, configuration: configuration)
        try await consumer.initialize()
        // A poll written and lost with its connection may have committed.
        await expectCode(.transientNotCommitted) { _ = try await consumer.next() }
        // withDeadline returns nil at the deadline. A client that is not
        // signed in again stays down, and the read below then waits for good.
        try #require(try await withDeadline(.now + .seconds(5)) { try await consumer.next()?.message.offset } == 0)
        await expectCode(.transientNotCommitted) { _ = try await consumer.next() }
        refusesSignIn.withLock { $0 = true }
        async let ended = withDeadline(.now + .seconds(5)) { try await consumer.next() == nil }
        #expect(await eventually { refusedSignIns.withLock { $0 } > 0 })
        await consumer.shutdown()
        #expect(try await ended == true)
        await client.shutdown()
    }

    /// A refused continuation is dropped, so the next poll of the partition
    /// starts where the strategy says again.
    @Test func aRecreatedPartitionDropsItsContinuation() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let poll = Locked(0)
        await serve(server) { _ in
            let index = poll.withLock { index in
                defer { index += 1 }
                return index
            }
            switch index {
            case 0: return .ok(FakeVSRServer.polledBody(partitionID: 1, offsets: [5]))
            case 1: return .status(IggyErrorCode.historyUnavailable.rawValue)
            default: return .ok(FakeVSRServer.polledBody(partitionID: 1, offsets: []))
            }
        }
        let client = try await signedInClient(server)
        let configuration = ConsumerConfiguration(pollingStrategy: .first, autoCommit: .disabled, pollingRetryInterval: .milliseconds(10))
        let consumer = try client.consumer(name: "reader", streamID: 1, topicID: 2, partitionID: 1, configuration: configuration)
        try await consumer.initialize()
        #expect(try await consumer.next()?.message.offset == 5)
        await expectCode(.historyUnavailable) { _ = try await consumer.next() }
        let reader = Task { try await consumer.next() }
        #expect(await eventually { await requests(server, .pollMessagesDeferred).count >= 3 })
        let kinds = await requests(server, .pollMessagesDeferred).prefix(3).map { polledStrategy($0).kind }
        #expect(kinds == [PollingStrategy.Kind.first.rawValue, PollingStrategy.Kind.offset.rawValue, PollingStrategy.Kind.first.rawValue])
        await consumer.shutdown()
        _ = try? await reader.value
        await client.shutdown()
    }

    // MARK: Committing

    @Test func pollingMessagesCommitsWithThePollItself() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await serve(server, poll: batches([[0, 1]]))
        let client = try await signedInClient(server)
        let consumer = try client.consumer(name: "reader", streamID: 1, topicID: 2, partitionID: 1)
        try await consumer.initialize()
        #expect(try await read(consumer, 2) == [0, 1])
        #expect(await requests(server, .pollMessagesDeferred).first.map { polledStrategy($0).autoCommit } == true)
        await consumer.shutdown()
        await client.shutdown()
    }

    /// The message triggers queue a commit as messages are handed over:
    /// every message, an offset that divides by n, or the end of a batch.
    @Test func messageTriggersCommitAsMessagesAreHandedOver() async throws {
        let cases: [(AutoCommitWhen, [UInt64])] = [
            (.consumingEachMessage, [0, 1, 2, 3]), (.consumingEveryNthMessage(2), [0, 2]), (.consumingAllMessages, [2, 3]),
        ]
        for (trigger, expected) in cases {
            let server = try await FakeVSRServer.started()
            await serve(server, poll: batches([[0, 1, 2], [3]]))
            let client = try await signedInClient(server)
            let consumer = try client.consumer(
                name: "reader", streamID: 1, topicID: 2, partitionID: 1, configuration: ConsumerConfiguration(autoCommit: .when(trigger)))
            try await consumer.initialize()
            #expect(try await read(consumer, 4) == [0, 1, 2, 3])
            #expect(await requests(server, .pollMessagesDeferred).first.map { polledStrategy($0).autoCommit } == false)
            // Queued commits collapse to the latest per partition, so wait
            // for the last one and check every store is one of the expected.
            #expect(await eventually { await requests(server, .storeConsumerOffset).map(storedOffset).last == expected.last })
            let stored = await requests(server, .storeConsumerOffset).map(storedOffset)
            #expect(stored.allSatisfy(expected.contains), "\(trigger) stored \(stored)")
            #expect(await requests(server, .storeConsumerOffset).allSatisfy { $0.context == FakeVSRServer.routeContext })
            await consumer.shutdown()
            await client.shutdown()
            await server.stop()
        }
    }

    @Test func anIntervalStoresTheReadingPositionOfEveryPartitionRead() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await serve(server, poll: batches([[0, 1]]))
        let client = try await signedInClient(server)
        let consumer = try client.consumer(
            name: "reader", streamID: 1, topicID: 2, partitionID: 1, configuration: ConsumerConfiguration(autoCommit: .interval(.milliseconds(50))))
        try await consumer.initialize()
        #expect(try await read(consumer, 2) == [0, 1])
        #expect(await eventually { await requests(server, .storeConsumerOffset).map(storedOffset).contains(1) })
        // A position already stored in the same history is not sent again.
        let stores = await requests(server, .storeConsumerOffset).count
        try await Task.sleep(for: .milliseconds(150))
        #expect(await requests(server, .storeConsumerOffset).count == stores)
        await consumer.shutdown()
        await client.shutdown()
    }

    /// A store by hand keeps the context of the last message handed over,
    /// and without replay a position the stored one covers is not sent.
    @Test func aStoreByHandKeepsTheContextAndSkipsCoveredPositions() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await serve(server, poll: batches([[4, 5]]))
        let client = try await signedInClient(server)
        let consumer = try client.consumer(
            name: "reader", streamID: 1, topicID: 2, partitionID: 1, configuration: ConsumerConfiguration(autoCommit: .disabled))
        // Before anything was read the store has no captured context, so it
        // covers nothing later.
        try await consumer.storeOffset(1)
        try await consumer.initialize()
        let first = try #require(try await consumer.next())
        _ = try await consumer.next()
        try await consumer.storeOffset(5)
        try await consumer.storePosition(first.position)
        let stores = await requests(server, .storeConsumerOffset)
        // The position at 4 is covered by the store at 5 and is not sent.
        #expect(stores.map(storedOffset) == [1, 5])
        #expect(await consumer.lastStoredOffset(partitionID: 1) == 5)
        // After a delete the same position goes again.
        try await consumer.deleteOffset()
        try await consumer.storeOffset(5)
        #expect(await requests(server, .storeConsumerOffset).count == 3)
        #expect(await requests(server, .deleteConsumerOffset).count == 1)
        await consumer.shutdown()
        await client.shutdown()
    }

    /// Stores run one at a time, so a position that the store in flight
    /// covers is not sent after it, even when the two calls overlap.
    @Test func overlappingStoresNeverMoveTheStoredOffsetBack() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let poll = batches([[3, 5]])
        await server.respond { [poll] request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .getStream: return .ok(GoldenFixture.shared.bytes("response.get_stream"))
            case .getTopic: return .ok(GoldenFixture.shared.bytes("response.get_topic"))
            case .pollMessagesDeferred: return await poll(request)
            case .storeConsumerOffset:
                try? await Task.sleep(for: .milliseconds(200))
                return .ok([])
            default: return FakeVSRServer.defaultReply(request, address: server.address)
            }
        }
        let client = try await signedInClient(server)
        let consumer = try client.consumer(
            name: "reader", streamID: 1, topicID: 2, partitionID: 1, configuration: ConsumerConfiguration(autoCommit: .disabled))
        try await consumer.initialize()
        let older = try #require(try await consumer.next())
        let newer = try #require(try await consumer.next())
        let storing = Task { try await consumer.storePosition(newer.position) }
        #expect(await eventually { await requests(server, .storeConsumerOffset).count == 1 })
        try await consumer.storePosition(older.position)
        try await storing.value
        #expect(await requests(server, .storeConsumerOffset).map(storedOffset) == [5])
        #expect(await consumer.lastStoredOffset(partitionID: 1) == 5)
        await consumer.shutdown()
        await client.shutdown()
    }

    /// The after triggers fire once the handler returned, whether it
    /// succeeded or threw: every message, an offset that divides by n, or the
    /// message at the partition's newest offset.
    @Test func consumeMessagesCommitsAfterTheHandler() async throws {
        let cases: [(AutoCommitAfter, [UInt64])] = [
            (.consumingEachMessage, [0, 1, 2, 3]), (.consumingEveryNthMessage(3), [0, 3]), (.consumingAllMessages, [3]),
        ]
        for (trigger, expected) in cases {
            let server = try await FakeVSRServer.started()
            await serve(server, poll: batches([[0, 1, 2, 3]]))
            let client = try await signedInClient(server)
            let consumer = try client.consumer(
                name: "reader", streamID: 1, topicID: 2, partitionID: 1, configuration: ConsumerConfiguration(autoCommit: .after(trigger)))
            try await consumer.initialize()
            struct Refused: Error {}
            let handled = Locked<[UInt64]>([])
            let consuming = Task {
                try await consumer.consumeMessages { received in
                    handled.withLock { $0.append(received.message.offset) }
                    // Slow enough that every commit goes out before the
                    // next one is queued and could replace it.
                    try? await Task.sleep(for: .milliseconds(30))
                    if received.message.offset == 3 {
                        throw Refused()
                    }
                }
            }
            #expect(await eventually { await requests(server, .storeConsumerOffset).map(storedOffset).last == 3 })
            #expect(handled.withLock { $0 } == [0, 1, 2, 3])
            #expect(await requests(server, .storeConsumerOffset).map(storedOffset) == expected, "\(trigger)")
            await consumer.shutdown()
            try await consuming.value
            await client.shutdown()
            await server.stop()
        }
    }

    @Test func consumeMessagesEndsWhenTheClientCannotComeBack() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await serve(server, poll: batches([]))
        let client = try await signedInClient(server)
        let configuration = ConsumerConfiguration(autoCommit: .disabled, pollingRetryInterval: .milliseconds(10))
        let consumer = try client.consumer(name: "reader", streamID: 1, topicID: 2, partitionID: 1, configuration: configuration)
        try await consumer.initialize()
        await client.disconnect()
        await #expect(throws: IggyError.self) { try await consumer.consumeMessages { _ in } }
        await consumer.shutdown()
        await client.shutdown()
    }

    // MARK: Groups

    /// A member joins on initialize, creating the group when it is missing,
    /// and joins again once a fetch finds the membership gone.
    @Test func aGroupMemberJoinsAndJoinsAgainAfterLosingTheMembership() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let revoked = Locked(false)
        let groupExists = Locked(false)
        await serve(server, topic: FakeTopic(ends: [1: 1], assignment: [1])) { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .getConsumerGroup: return groupExists.withLock { $0 } ? .ok(GoldenFixture.shared.bytes("response.consumer_group_details")) : .ok([])
            case .createConsumerGroup:
                groupExists.withLock { $0 = true }
                return .ok(GoldenFixture.shared.bytes("response.consumer_group_details"))
            case .fetchMessages: return revoked.exchange(true) ? nil : .status(IggyErrorCode.consumerGroupMemberNotFound.rawValue)
            default: return nil
            }
        }
        let client = try await signedInClient(server)
        let consumer = try client.consumerGroup(
            name: "billing", streamID: 1, topicID: 2, configuration: ConsumerConfiguration(autoCommit: .disabled, pollingRetryInterval: .milliseconds(10)))
        try await consumer.initialize()
        #expect(await requests(server, .createConsumerGroup).count == 1)
        #expect(try await consumer.next()?.message.offset == 0)
        #expect(await requests(server, .joinConsumerGroup).count == 2)
        await consumer.shutdown()
        #expect(await requests(server, .leaveConsumerGroup).count == 1)
        await client.shutdown()
    }

    /// A fetch waits on the server only while the member can read. A member
    /// without partitions syncs its assignment and one that keeps losing its
    /// membership joins again, each after the retry interval, as the Rust SDK
    /// retries after `polling_retry_interval`.
    @Test func aMemberThatCannotReadTriesAgainOncePerRetryInterval() async throws {
        let window = Duration.milliseconds(600)
        let retryInterval = Duration.milliseconds(100)
        for revoked in [false, true] {
            let server = try await FakeVSRServer.started()
            await serve(server, topic: FakeTopic(ends: [:], assignment: revoked ? [1] : [])) { request in
                request.code == CommandCode.fetchMessages.rawValue ? .status(IggyErrorCode.consumerGroupMemberNotFound.rawValue) : nil
            }
            let client = try await signedInClient(server)
            let configuration = ConsumerConfiguration(autoCommit: .disabled, pollingRetryInterval: retryInterval)
            let consumer = try client.consumerGroup(name: "billing", streamID: 1, topicID: 2, configuration: configuration)
            try await consumer.initialize()
            let reading = Task { try await consumer.next() }
            try await Task.sleep(for: window)
            let rounds = await requests(server, revoked ? .joinConsumerGroup : .syncConsumerGroup).count
            await consumer.shutdown()
            #expect(try await reading.value == nil)
            let most = Int(window / retryInterval) + 2
            #expect((2...most).contains(rounds), "revoked: \(revoked), rounds: \(rounds)")
            await client.shutdown()
            await server.stop()
        }
    }

    /// A shutdown during a join leaves the group once the join is answered,
    /// whether initialize() or a fetch that lost the membership started it.
    /// Nothing is read after the shutdown.
    @Test func aShutdownDuringAJoinLeavesTheGroupAfterIt() async throws {
        for joinsOnRead in [false, true] {
            let server = try await FakeVSRServer.started()
            let joins = Locked(0)
            let slowJoin = joinsOnRead ? 2 : 1
            await serve(server, topic: FakeTopic(ends: [:], assignment: [1])) { request in
                switch request.code.flatMap(CommandCode.init(rawValue:)) {
                case .joinConsumerGroup:
                    let join = joins.withLock { join in
                        join += 1
                        return join
                    }
                    if join == slowJoin {
                        try? await Task.sleep(for: .milliseconds(200))
                    }
                    return .ok([])
                case .fetchMessages: return .status(IggyErrorCode.consumerGroupMemberNotFound.rawValue)
                default: return nil
                }
            }
            let client = try await signedInClient(server)
            let consumer = try client.consumerGroup(name: "billing", streamID: 1, topicID: 2, configuration: ConsumerConfiguration(autoCommit: .disabled))
            let reading = Task { () -> ReceivedMessage? in
                try await consumer.initialize()
                guard joinsOnRead else {
                    return nil
                }
                return try await consumer.next()
            }
            #expect(await eventually { joins.withLock { $0 } == slowJoin })
            await consumer.shutdown()
            #expect(await requests(server, .leaveConsumerGroup).count == 1, "joins on read: \(joinsOnRead)")
            #expect(try await reading.value == nil)
            #expect(await requests(server, .fetchMessages).count == (joinsOnRead ? 1 : 0), "joins on read: \(joinsOnRead)")
            await client.shutdown()
            await server.stop()
        }
    }

    /// A read that would join again once the consumer shut down sends
    /// neither the join nor the fetch, so no membership outlives the
    /// consumer. The shutdown cancels a fetch that commits nothing and waits
    /// for one that commits.
    @Test func aReadAfterShutdownDoesNotJoinAgain() async throws {
        for autoCommit in [AutoCommit.disabled, .when(.pollingMessages)] {
            let server = try await FakeVSRServer.started()
            await serve(server, topic: FakeTopic(ends: [:], assignment: [1])) { request in
                guard request.code == CommandCode.fetchMessages.rawValue else {
                    return nil
                }
                try? await Task.sleep(for: .milliseconds(300))
                return .status(IggyErrorCode.consumerGroupMemberNotFound.rawValue)
            }
            let client = try await signedInClient(server)
            let consumer = try client.consumerGroup(name: "billing", streamID: 1, topicID: 2, configuration: ConsumerConfiguration(autoCommit: autoCommit))
            try await consumer.initialize()
            let reading = Task { try await consumer.next() }
            #expect(await eventually { await requests(server, .fetchMessages).count == 1 })
            await consumer.shutdown()
            #expect(try await reading.value == nil)
            #expect(await requests(server, .joinConsumerGroup).count == 1, "\(autoCommit)")
            #expect(await requests(server, .fetchMessages).count == 1, "\(autoCommit)")
            let cancellations = autoCommit == .disabled ? 1 : 0
            #expect(await eventually { await requests(server, .cancelDeferredRequest).count == cancellations }, "\(autoCommit)")
            await client.shutdown()
            await server.stop()
        }
    }

    /// A member fetches its readiness group with one request, which the
    /// server reads in target order while the count leaves room. Each
    /// partition continues at the next offset of its section, also after a
    /// `next` start, and the ones that returned messages go last in the next
    /// fetch, so every partition gets its turn. The next fetch goes out when
    /// a reply arrives, before the reader asks for more.
    @Test func aMemberFetchesItsGroupInOneRequestAndServedPartitionsGoLast() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await serve(server, topic: FakeTopic(ends: [1: 4, 2: 4, 3: 4], assignment: [3, 1, 2]))
        let client = try await signedInClient(server)
        let configuration = ConsumerConfiguration(batchLength: 2, autoCommit: .disabled)
        let consumer = try client.consumerGroup(name: "billing", streamID: 1, topicID: 2, configuration: configuration)
        try await consumer.initialize()
        let first = try #require(try await consumer.next())
        #expect(await eventually { await requests(server, .fetchMessages).count >= 2 })
        let rest = try await withDeadline(.now + .seconds(5)) {
            var rest: [String] = []
            while rest.count < 11, let next = try await consumer.next() {
                rest.append("\(next.partitionID):\(next.message.offset)")
            }
            return rest
        }
        #expect(
            ["\(first.partitionID):\(first.message.offset)"] + (rest ?? []) == [
                "1:0", "1:1", "2:0", "2:1", "3:0", "3:1", "1:2", "1:3", "2:2", "2:3", "3:2", "3:3",
            ])
        let sent = try await fetches(server)
        let targets = sent.prefix(3).map(\.targets)
        #expect(targets.map { $0.map(\.partitionID) } == [[1, 2, 3], [2, 3, 1], [3, 1, 2]])
        #expect(targets.map { $0.map(\.kind) } == [[.next, .next, .next], [.offset, .offset, .offset], [.offset, .offset, .offset]])
        #expect(targets.map { $0.map(\.value) } == [[0, 0, 0], [0, 0, 2], [0, 2, 2]])
        for fetch in sent {
            #expect(fetch.count == configuration.batchLength && !fetch.autoCommit)
            #expect(fetch.targets.allSatisfy { $0.maxBytes == configuration.pollOptions.maxBytes && $0.incarnation == FakeVSRServer.routeContext.incarnation })
        }
        #expect(await requests(server, .fetchMessages).allSatisfy { $0.context == .empty })
        await consumer.shutdown()
        await client.shutdown()
    }

    /// A partition that turns idle while the fetch of its group waits joins a
    /// second fetch, so it does not wait out the wait of the first one. The
    /// next fetch of the group takes the partitions of the second one over.
    @Test func aPartitionThatTurnsIdleJoinsASecondFetch() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let topic = FakeTopic(ends: [1: 1, 2: 1], assignment: [1, 2])
        await serve(server, topic: topic) { request in
            // Partition 2 finds its start once the fetch of partition 1 waits.
            if request.code == CommandCode.pollMessagesDeferred.rawValue, polledPartition(request) == 2 {
                _ = await eventually { await requests(server, .fetchMessages).count == 1 }
            }
            return nil
        }
        let client = try await signedInClient(server)
        let configuration = ConsumerConfiguration(
            pollingStrategy: .first, pollOptions: DeferredPollOptions(maxWait: .seconds(5), requestTimeout: .seconds(10)), autoCommit: .disabled)
        let consumer = try client.consumerGroup(name: "billing", streamID: 1, topicID: 2, configuration: configuration)
        try await consumer.initialize()
        #expect(try await withDeadline(.now + .seconds(5)) { try await read(consumer, 2) } == [0, 0])
        #expect(await eventually { await requests(server, .fetchMessages).count == 2 })
        topic.append(1, to: 1)
        #expect(try await withDeadline(.now + .seconds(5)) { try await read(consumer, 1) } == [1])
        #expect(await eventually { await requests(server, .fetchMessages).count == 3 })
        #expect(try await fetches(server).prefix(3).map { $0.targets.map(\.partitionID) } == [[1], [2], [2, 1]])
        #expect(await eventually { await requests(server, .cancelDeferredRequest).count == 1 })
        await consumer.shutdown()
        await client.shutdown()
    }

    /// A member that commits on poll fetches a group only once the reader
    /// asks with nothing left to hand over, so the server stores at most one
    /// batch per group as consumed before the reader asked for it.
    @Test func aMemberThatCommitsOnPollFetchesOnlyWhenTheReaderAsks() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let topic = FakeTopic(ends: [1: 4, 2: 4], assignment: [1, 2])
        await serve(server, topic: topic)
        let client = try await signedInClient(server)
        let configuration = ConsumerConfiguration(batchLength: 2, autoCommit: .when(.pollingMessages))
        let consumer = try client.consumerGroup(name: "billing", streamID: 1, topicID: 2, configuration: configuration)
        try await consumer.initialize()
        #expect(try await read(consumer, 2) == [0, 1])
        try await Task.sleep(for: .milliseconds(300))
        #expect(await requests(server, .fetchMessages).count == 1)
        #expect(topic.stored == [1: 1])
        #expect(try await read(consumer, 2) == [0, 1])
        #expect(topic.stored == [1: 1, 2: 1])
        let sent = try await fetches(server)
        #expect(sent.map { $0.targets.map(\.partitionID) } == [[1, 2], [2, 1]])
        #expect(sent.map { $0.targets.map(\.kind) } == [[.next, .next], [.offset, .offset]])
        #expect(sent.allSatisfy { $0.autoCommit })
        await consumer.shutdown()
        await client.shutdown()
    }

    @Test func aMemberDeliversHealthySectionsBeforeReportingUncertaintyAndClosesWithoutRecovery() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let topic = FakeTopic(ends: [1: 2, 2: 1], assignment: [1, 2])
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.fetchMessages.rawValue else { return nil }
            return .ok(
                FakeVSRServer.fetchedBody([
                    .init(partitionID: 2, status: IggyErrorCode.transientNotCommitted.rawValue), .init(partitionID: 1, offsets: [0, 1]),
                ]))
        }
        let client = try await signedInClient(server)
        let configuration = ConsumerConfiguration(batchLength: 2, autoCommit: .when(.pollingMessages))
        let consumer = try client.consumerGroup(name: "billing", streamID: 1, topicID: 2, configuration: configuration)
        try await consumer.initialize()
        #expect(try await read(consumer, 2) == [0, 1])
        let failure = try await reportedError { _ = try await consumer.next() }
        #expect(failure.code == .fetchProgressUncertain && failure.partitionID == 2)
        let operationID = try #require(failure.operationID)
        let repeated = try await reportedError { _ = try await consumer.next() }
        #expect(repeated.operationID == operationID)
        #expect(await requests(server, .fetchMessages).count == 1)
        let elapsed = await ContinuousClock().measure { await consumer.shutdown() }
        #expect(elapsed < .seconds(1))
        #expect(await requests(server, .storeConsumerOffset).map(storedOffset) == [1])
        #expect(await requests(server, .storeConsumerOffset).map(storedPartition) == [1])
        #expect(await requests(server, .deleteConsumerOffset).isEmpty)
        let blocked = try await reportedError { try await client.storeConsumerOffset(streamID: 1, topicID: 2, partitionID: 2, offset: 0) }
        #expect(blocked.operationID == operationID && blocked.partitionID == 2)
        try client.abandonOperation(operationID)
        try await client.storeConsumerOffset(streamID: 1, topicID: 2, partitionID: 2, offset: 0)
        #expect(await requests(server, .fetchMessages).count == 1)
        await client.shutdown()
    }

    @Test func anUndecodableCommittedSectionIsRewoundBesideABlockedPartition() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let topic = FakeTopic(ends: [1: 7, 2: 1], assignment: [1, 2])
        await serve(server, topic: topic) { request in
            guard request.code == CommandCode.fetchMessages.rawValue else { return nil }
            return .ok(
                FakeVSRServer.fetchedBody([
                    .init(partitionID: 2, status: IggyErrorCode.transientNotCommitted.rawValue),
                    .init(partitionID: 1, offsets: [5, 6], records: [0]),
                ]))
        }
        let client = try await signedInClient(server)
        let consumer = try client.consumerGroup(
            name: "billing", streamID: 1, topicID: 2, configuration: ConsumerConfiguration(batchLength: 2, autoCommit: .when(.pollingMessages)))
        try await consumer.initialize()
        let decodeFailure = try await reportedError { _ = try await consumer.next() }
        #expect(decodeFailure.operationID == nil)
        let uncertainty = try await reportedError { _ = try await consumer.next() }
        #expect(uncertainty.code == .fetchProgressUncertain && uncertainty.partitionID == 2)
        await consumer.shutdown()
        let stores = await requests(server, .storeConsumerOffset)
        #expect(stores.map(storedPartition) == [1])
        #expect(stores.map(storedOffset) == [4])
        #expect(stores.map(\.context) == [FakeVSRServer.routeContext])
        #expect(await requests(server, .fetchMessages).count == 1)
        let blocked = try await reportedError { try await client.deleteConsumerOffset(streamID: 1, topicID: 2, partitionID: 2) }
        #expect(blocked.operationID == uncertainty.operationID)
        try client.abandonOperation(try #require(uncertainty.operationID))
        await client.shutdown()
    }

    /// A new assignment cancels the fetches that commit nothing, so the next
    /// fetch reads the new group. A fetch that commits is never cancelled:
    /// the new group waits for its reply.
    @Test func aNewAssignmentCancelsOnlyTheFetchesThatCommitNothing() async throws {
        for autoCommit in [AutoCommit.disabled, .when(.pollingMessages)] {
            let server = try await FakeVSRServer.started()
            let topic = FakeTopic(ends: [:], assignment: [1])
            await serve(server, topic: topic)
            let client = try await signedInClient(server)
            let configuration = ConsumerConfiguration(
                pollOptions: DeferredPollOptions(maxWait: .seconds(3), requestTimeout: .seconds(5)), autoCommit: autoCommit)
            let consumer = try client.consumerGroup(name: "billing", streamID: 1, topicID: 2, configuration: configuration)
            try await consumer.initialize()
            let reading = Task { try await consumer.next() }
            #expect(await eventually { await requests(server, .fetchMessages).count == 1 })
            topic.assign([1, 2])
            await client.refreshConsumerGroupAssignments()
            #expect(await eventually(timeout: .seconds(8)) { await requests(server, .fetchMessages).count == 2 }, "\(autoCommit)")
            #expect(try await fetches(server).last?.targets.map(\.partitionID) == [1, 2], "\(autoCommit)")
            #expect(await requests(server, .cancelDeferredRequest).count == (autoCommit == .disabled ? 1 : 0), "\(autoCommit)")
            // Ends the fetch that waits, which a committing shutdown waits for.
            topic.append(1, to: 2)
            await consumer.shutdown()
            _ = try? await reading.value
            await client.shutdown()
            await server.stop()
        }
    }

    /// The Swift client has no progress lanes, so a store waits for a group
    /// read in flight that reads or moves the stored offset of its
    /// partition: one that commits, or one that starts after the stored
    /// offset. So the two never reorder on the server.
    @Test func aStoreWaitsForAReadThatHoldsItsStoredOffset() async throws {
        for autoCommit in [AutoCommit.disabled, .when(.pollingMessages)] {
            let server = try await FakeVSRServer.started()
            let topic = FakeTopic(ends: [:], assignment: [1])
            let fetched = Locked<ContinuousClock.Instant?>(nil)
            let stored = Locked<ContinuousClock.Instant?>(nil)
            await serve(server, topic: topic) { request in
                switch request.code.flatMap(CommandCode.init(rawValue:)) {
                case .fetchMessages:
                    let reply = await topic.reply(to: request)
                    fetched.withLock { $0 = $0 ?? .now }
                    return reply
                case .storeConsumerOffset:
                    stored.withLock { $0 = $0 ?? .now }
                    return nil
                default:
                    return nil
                }
            }
            let client = try await signedInClient(server)
            let configuration = ConsumerConfiguration(
                pollOptions: DeferredPollOptions(maxWait: .seconds(3), requestTimeout: .seconds(5)), autoCommit: autoCommit)
            let consumer = try client.consumerGroup(name: "billing", streamID: 1, topicID: 2, configuration: configuration)
            try await consumer.initialize()
            let reading = Task { try await consumer.next()?.message.offset }
            #expect(await eventually { await requests(server, .fetchMessages).count == 1 })
            let store = Task { try await consumer.storeOffset(3, partitionID: 1) }
            try await Task.sleep(for: .milliseconds(200))
            #expect(await requests(server, .storeConsumerOffset).isEmpty, "\(autoCommit)")
            topic.append(1, to: 1)
            try await store.value
            #expect(try await reading.value == 0, "\(autoCommit)")
            let fetchEnded = try #require(fetched.withLock { $0 })
            let storeArrived = try #require(stored.withLock { $0 })
            #expect(storeArrived > fetchEnded, "\(autoCommit)")
            await consumer.shutdown()
            await client.shutdown()
            await server.stop()
        }
    }

    /// A batch that does not decode is thrown, and its partition is read
    /// again from the first message of the batch after the retry interval,
    /// as in the Rust SDK, also when its fetch committed it.
    @Test func aBatchThatDoesNotDecodeIsReadAgainFromItsFirstMessage() async throws {
        for autoCommit in [AutoCommit.disabled, .when(.pollingMessages)] {
            let server = try await FakeVSRServer.started()
            let corrupted = Locked(false)
            await serve(server, topic: FakeTopic(ends: [1: 7], assignment: [1])) { request in
                guard request.code == CommandCode.fetchMessages.rawValue, !corrupted.exchange(true) else {
                    return nil
                }
                let unreadable = [UInt8](repeating: 0xFF, count: 16)
                return .ok(FakeVSRServer.fetchedBody([.init(partitionID: 1, offsets: [5, 6], records: unreadable)]))
            }
            let client = try await signedInClient(server)
            let configuration = ConsumerConfiguration(batchLength: 2, autoCommit: autoCommit, pollingRetryInterval: .milliseconds(50))
            let consumer = try client.consumerGroup(name: "billing", streamID: 1, topicID: 2, configuration: configuration)
            try await consumer.initialize()
            await #expect(throws: IggyError.self, "\(autoCommit)") { _ = try await consumer.next() }
            #expect(try await withDeadline(.now + .seconds(5)) { try await read(consumer, 2) } == [5, 6], "\(autoCommit)")
            let starts = try await fetches(server).map { $0.targets.map(\.value) }
            #expect(starts.first == [0] && starts.contains([5]), "\(autoCommit): \(starts)")
            await consumer.shutdown()
            await client.shutdown()
            await server.stop()
        }
    }

    /// A fetch reads from an offset or after the stored offset only, so a
    /// partition that starts at `first` or `last` is polled alone until it
    /// has a position. A member that commits on poll finds that position with
    /// a poll that commits and hands over nothing, and its fetch then reads
    /// and commits the batch.
    @Test func aPartitionThatStartsElsewhereIsPolledAloneUntilItHasAPosition() async throws {
        let cases: [(strategy: PollingStrategy, autoCommit: AutoCommit, read: [UInt64], fetchedFrom: UInt64)] = [
            (.first, .disabled, [0, 1, 2, 3], 2), (.first, .when(.pollingMessages), [0, 1, 2, 3], 0), (.last, .when(.pollingMessages), [2, 3], 2),
        ]
        for scenario in cases {
            let server = try await FakeVSRServer.started()
            await serve(server, topic: FakeTopic(ends: [1: 4], assignment: [1]))
            let client = try await signedInClient(server)
            let configuration = ConsumerConfiguration(pollingStrategy: scenario.strategy, batchLength: 2, autoCommit: scenario.autoCommit)
            let consumer = try client.consumerGroup(name: "billing", streamID: 1, topicID: 2, configuration: configuration)
            try await consumer.initialize()
            let label = "\(scenario.strategy), \(scenario.autoCommit)"
            #expect(try await withDeadline(.now + .seconds(5)) { try await read(consumer, scenario.read.count) } == scenario.read, "\(label)")
            let polls = await requests(server, .pollMessagesDeferred).map(polledStrategy)
            #expect(polls.map { $0.kind } == [scenario.strategy.kind.rawValue], "\(label)")
            #expect(polls.allSatisfy { !$0.autoCommit }, "\(label)")
            #expect(try await fetches(server).first?.targets.map(\.value) == [scenario.fetchedFrom], "\(label)")
            await consumer.shutdown()
            await client.shutdown()
            await server.stop()
        }
    }

    /// A gone history that fails a whole fetch does not name its partition,
    /// so each partition of the fetch is probed alone at once, with a poll
    /// that neither waits nor commits, and only the probe that names the
    /// partition reports the error.
    @Test func aFenceThatFailsAFetchProbesEachOfItsPartitionsAlone() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let topic = FakeTopic(ends: [:], assignment: [1, 2])
        let refused = Locked(false)
        await serve(server, topic: topic) { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .fetchMessages: return refused.exchange(true) ? nil : .status(IggyErrorCode.historyUnavailable.rawValue)
            case .pollMessagesDeferred where polledPartition(request) == 1: return .status(IggyErrorCode.historyUnavailable.rawValue)
            default: return nil
            }
        }
        let client = try await signedInClient(server)
        let configuration = ConsumerConfiguration(autoCommit: .when(.pollingMessages), pollingRetryInterval: .milliseconds(50))
        let consumer = try client.consumerGroup(name: "billing", streamID: 1, topicID: 2, configuration: configuration)
        try await consumer.initialize()
        await expectCode(.historyUnavailable) { _ = try await consumer.next() }
        #expect(await eventually { await requests(server, .pollMessagesDeferred).count == 2 })
        let probes = await requests(server, .pollMessagesDeferred)
        #expect(Set(probes.map(polledPartition)) == [1, 2])
        for probe in probes {
            #expect(polledStrategy(probe).count == 1 && !polledStrategy(probe).autoCommit)
            #expect(probe.deferredOptions.wait == 0 && probe.deferredOptions.minCount == 1)
        }
        topic.append(1, to: 2)
        #expect(try await withDeadline(.now + .seconds(5)) { try await consumer.next()?.message.offset } == 0)
        await consumer.shutdown()
        await client.shutdown()
    }

    /// A message larger than `maxBytes` fails its section, so the member
    /// polls it alone without the limit, committing as its fetch would, and
    /// then fetches the partition again.
    @Test func aMessageLargerThanTheByteLimitIsPolledAloneByAGroupMember() async throws {
        for autoCommit in [AutoCommit.disabled, .when(.pollingMessages)] {
            let server = try await FakeVSRServer.started()
            let refused = Locked(false)
            await serve(server, topic: FakeTopic(ends: [1: 3], assignment: [1])) { request in
                guard request.code == CommandCode.fetchMessages.rawValue, !refused.exchange(true) else {
                    return nil
                }
                return .ok(FakeVSRServer.fetchedBody([.init(partitionID: 1, status: IggyErrorCode.invalidSizeBytes.rawValue)]))
            }
            let client = try await signedInClient(server)
            let consumer = try client.consumerGroup(name: "billing", streamID: 1, topicID: 2, configuration: ConsumerConfiguration(autoCommit: autoCommit))
            try await consumer.initialize()
            #expect(try await withDeadline(.now + .seconds(5)) { try await read(consumer, 3) } == [0, 1, 2], "\(autoCommit)")
            let polls = await requests(server, .pollMessagesDeferred)
            #expect(polls.map { polledStrategy($0).count } == [1], "\(autoCommit)")
            #expect(polls.map { polledStrategy($0).autoCommit } == [autoCommit != .disabled], "\(autoCommit)")
            #expect(polls.map { $0.deferredOptions.maxBytes } == [DeferredPollOptions.maxReplyBytes], "\(autoCommit)")
            #expect(try await fetches(server).prefix(2).map { $0.targets.map(\.value) } == [[0], [1]], "\(autoCommit)")
            await consumer.shutdown()
            await client.shutdown()
            await server.stop()
        }
    }

    @Test func aMissingGroupIsNotCreatedWhenAutoCreationIsOff() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await server.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .getStream: return .ok(GoldenFixture.shared.bytes("response.get_stream"))
            case .getTopic: return .ok(GoldenFixture.shared.bytes("response.get_topic"))
            default: return FakeVSRServer.defaultReply(request, address: server.address)
            }
        }
        let client = try await signedInClient(server)
        let consumer = try client.consumerGroup(
            name: "billing", streamID: 1, topicID: 2, configuration: ConsumerConfiguration(createConsumerGroupIfNotExists: false))
        await expectCode(.consumerGroupNameNotFound) { try await consumer.initialize() }
        #expect(await requests(server, .createConsumerGroup).isEmpty)
        await client.shutdown()
    }

    // MARK: Lifecycle

    @Test func initializeWaitsForAMissingTopicWithinItsRetries() async throws {
        for (retries, succeeds) in [(UInt32(3), true), (1, false)] {
            let server = try await FakeVSRServer.started()
            let lookups = Locked(0)
            await server.respond { request in
                switch request.code.flatMap(CommandCode.init(rawValue:)) {
                case .getStream: return .ok(GoldenFixture.shared.bytes("response.get_stream"))
                case .getTopic:
                    let lookup = lookups.withLock {
                        $0 += 1; return $0
                    }
                    return lookup > 2 ? .ok(GoldenFixture.shared.bytes("response.get_topic")) : .ok([])
                default: return FakeVSRServer.defaultReply(request, address: server.address)
                }
            }
            let client = try await signedInClient(server)
            let configuration = ConsumerConfiguration(initRetries: retries, initRetryInterval: .milliseconds(10))
            let consumer = try client.consumer(name: "reader", streamID: 1, topicID: "created", partitionID: 1, configuration: configuration)
            if succeeds {
                try await consumer.initialize()
            } else {
                await expectCode(.topicNameNotFound) { try await consumer.initialize() }
            }
            #expect(lookups.withLock { $0 } == (succeeds ? 3 : 2))
            await consumer.shutdown()
            await client.shutdown()
            await server.stop()
        }
    }

    /// Shutdown sends the reading position that was not stored yet, and a
    /// second shutdown waits for the first. Reading ends with nil.
    @Test func shutdownStoresTheReadingPositionOnce() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let poll = batches([[0, 1]])
        await server.respond { [poll] request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .getStream: return .ok(GoldenFixture.shared.bytes("response.get_stream"))
            case .getTopic: return .ok(GoldenFixture.shared.bytes("response.get_topic"))
            case .pollMessagesDeferred: return await poll(request)
            case .storeConsumerOffset:
                try? await Task.sleep(for: .milliseconds(200))
                return .ok([])
            default: return FakeVSRServer.defaultReply(request, address: server.address)
            }
        }
        let client = try await signedInClient(server)
        let consumer = try client.consumer(
            name: "reader", streamID: 1, topicID: 2, partitionID: 1, configuration: ConsumerConfiguration(autoCommit: .interval(.seconds(60))))
        try await consumer.initialize()
        #expect(try await read(consumer, 2) == [0, 1])
        let first = Task { await consumer.shutdown() }
        #expect(await eventually { await !requests(server, .storeConsumerOffset).isEmpty })
        await consumer.shutdown()
        #expect(await consumer.lastStoredOffset(partitionID: 1) == 1)
        await first.value
        #expect(await requests(server, .storeConsumerOffset).map(storedOffset) == [1])
        #expect(try await consumer.next() == nil)
        await client.shutdown()
    }

    /// A tick storing at shutdown ends right after its store. One whose store
    /// outlasts the drain timeout is cancelled there, so shutdown waits
    /// neither a full interval nor the request timeout.
    @Test func shutdownEndsAStoringTickAfterItsStoreOrAtTheDrainTimeout() async throws {
        for (stalls, bound) in [(false, Duration.milliseconds(700)), (true, .seconds(3))] {
            let server = try await FakeVSRServer.started()
            let poll = batches([[0]])
            let stores = Locked(0)
            await server.respond { [poll] request in
                switch request.code.flatMap(CommandCode.init(rawValue:)) {
                case .getStream: return .ok(GoldenFixture.shared.bytes("response.get_stream"))
                case .getTopic: return .ok(GoldenFixture.shared.bytes("response.get_topic"))
                case .pollMessagesDeferred: return await poll(request)
                case .storeConsumerOffset:
                    let store = stores.withLock { store in
                        store += 1
                        return store
                    }
                    // Only the store of the tick hangs, so the final store of
                    // the shutdown goes through.
                    if stalls, store == 1 {
                        return .stall
                    }
                    try? await Task.sleep(for: .milliseconds(200))
                    return .ok([])
                default: return FakeVSRServer.defaultReply(request, address: server.address)
                }
            }
            let client = try await signedInClient(server, requestTimeout: .seconds(5))
            let configuration = ConsumerConfiguration(autoCommit: .interval(.seconds(1)), offsetDrainTimeout: .seconds(1))
            let consumer = try client.consumer(name: "reader", streamID: 1, topicID: 2, partitionID: 1, configuration: configuration)
            try await consumer.initialize()
            #expect(try await read(consumer, 1) == [0])
            #expect(await eventually { await !requests(server, .storeConsumerOffset).isEmpty })
            let elapsed = await ContinuousClock().measure { await consumer.shutdown() }
            #expect(elapsed < bound, "stalls: \(stalls), took \(elapsed)")
            await client.shutdown()
            await server.stop()
        }
    }

    /// Concurrent initializations share one run: one lookup, one join.
    @Test func concurrentInitializationsShareOneRun() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await serve(server, topic: FakeTopic(ends: [1: 1], assignment: [1]))
        let client = try await signedInClient(server)
        let consumer = try client.consumerGroup(name: "billing", streamID: 1, topicID: 2, configuration: ConsumerConfiguration(autoCommit: .disabled))
        async let first: Void = consumer.initialize()
        async let second: Void = consumer.initialize()
        _ = try await (first, second)
        try await consumer.initialize()
        #expect(await requests(server, .getStream).count == 1)
        #expect(await requests(server, .joinConsumerGroup).count == 1)
        #expect(try await consumer.next()?.message.offset == 0)
        await consumer.shutdown()
        await client.shutdown()
    }

    @Test func aDroppedConsumerEndsItsTasks() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await serve(server, poll: batches([]))
        let client = try await signedInClient(server)
        let weak = WeakConsumer()
        do {
            let consumer = try client.consumer(
                name: "reader", streamID: 1, topicID: 2, partitionID: 1, configuration: ConsumerConfiguration(autoCommit: .interval(.milliseconds(10))))
            try await consumer.initialize()
            weak.value = consumer
        }
        #expect(await eventually { weak.value == nil })
        await client.shutdown()
    }

    @Test func outOfRangeOptionsAreRefusedWhenTheConsumerIsBuilt() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = try await signedInClient(server)
        let invalid = [
            ConsumerConfiguration(batchLength: 0), ConsumerConfiguration(autoCommit: .interval(.zero)),
            ConsumerConfiguration(autoCommit: .when(.consumingEveryNthMessage(0))), ConsumerConfiguration(autoCommit: .after(.consumingEveryNthMessage(0))),
            ConsumerConfiguration(pollingRetryInterval: .zero), ConsumerConfiguration(initRetryInterval: .milliseconds(-1)),
            ConsumerConfiguration(offsetDrainTimeout: ProducerConfiguration.maxDuration * 2),
        ]
        for configuration in invalid {
            await expectCode(.invalidConfiguration) {
                _ = try client.consumer(name: "reader", streamID: 1, topicID: 2, partitionID: 1, configuration: configuration)
            }
        }
        // Poll options fail with the codes of their own validation, as in the
        // Rust SDK.
        let invalidPolls: [(DeferredPollOptions, IggyErrorCode)] = [
            (DeferredPollOptions(minCount: 0), .invalidMessagesCount), (DeferredPollOptions(minCount: 1001), .invalidMessagesCount),
            (DeferredPollOptions(maxWait: .seconds(11)), .invalidCommand), (DeferredPollOptions(maxBytes: 343), .invalidSizeBytes),
        ]
        for (options, code) in invalidPolls {
            await expectCode(code) {
                _ = try client.consumer(name: "reader", streamID: 1, topicID: 2, partitionID: 1, configuration: ConsumerConfiguration(pollOptions: options))
            }
        }
        _ = try client.consumerGroup(
            name: "billing", streamID: 1, topicID: 2, configuration: ConsumerConfiguration(pollOptions: DeferredPollOptions(maxWait: .zero)))
        // A fetch keeps the reply margin without a wait too, which a poll
        // does not need.
        let before = await server.requests.count
        for timeout in [Duration.milliseconds(500), .seconds(1), .seconds(1) + .nanoseconds(999)] {
            let noMargin = ConsumerConfiguration(pollOptions: DeferredPollOptions(maxWait: .zero, requestTimeout: timeout))
            _ = try client.consumer(name: "reader", streamID: 1, topicID: 2, partitionID: 1, configuration: noMargin)
            await expectCode(.invalidCommand) { _ = try client.consumerGroup(name: "billing", streamID: 1, topicID: 2, configuration: noMargin) }
        }
        _ = try client.consumerGroup(
            name: "billing", streamID: 1, topicID: 2,
            configuration: ConsumerConfiguration(pollOptions: DeferredPollOptions(maxWait: .zero, requestTimeout: .microseconds(1_000_001))))
        #expect(await server.requests.count == before)
        await client.shutdown()
    }

    // MARK: Deferred polls

    /// The server holds a poll until a message arrives or its wait ends, so
    /// the consumer polls again at once after an empty reply, and hands a
    /// message over as soon as the parked poll returns it.
    @Test func aMessageAfterAnIdlePeriodIsHandedOverAtOnce() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let arrived = Locked<ContinuousClock.Instant?>(nil)
        let polls = Locked(0)
        await serve(server) { _ in
            let poll = polls.withLock { polls in
                polls += 1
                return polls
            }
            // Two waits end empty, the third is parked until the message arrives.
            guard poll > 2 else {
                return .ok(FakeVSRServer.polledBody(partitionID: 1, offsets: []))
            }
            _ = await eventually(timeout: .seconds(10)) { arrived.withLock { $0 } != nil }
            return .ok(FakeVSRServer.polledBody(partitionID: 1, offsets: [0]))
        }
        let client = try await signedInClient(server)
        let options = DeferredPollOptions(maxWait: .seconds(10), requestTimeout: .seconds(15))
        let configuration = ConsumerConfiguration(batchLength: 50, pollOptions: options, autoCommit: .disabled)
        let consumer = try client.consumer(name: "reader", streamID: 1, topicID: 2, partitionID: 1, configuration: configuration)
        try await consumer.initialize()
        let reading = Task { try await consumer.next() }
        #expect(await eventually(timeout: .seconds(1)) { polls.withLock { $0 } == 3 })
        try await Task.sleep(for: .milliseconds(300))
        let sent = ContinuousClock.now
        arrived.withLock { $0 = sent }
        #expect(try await reading.value?.message.offset == 0)
        #expect(ContinuousClock.now - sent < .milliseconds(500))
        let deferred = await requests(server, .pollMessagesDeferred)
        #expect(deferred.count == 3)
        for poll in deferred {
            #expect(polledStrategy(poll).count == 50)
            #expect(poll.deferredOptions.minCount == options.minCount)
            #expect(poll.deferredOptions.maxBytes == options.maxBytes)
            #expect(poll.deferredOptions.wait > 9_000_000 && poll.deferredOptions.timeout <= 15_000_000)
        }
        await consumer.shutdown()
        await client.shutdown()
    }

    /// A first message larger than `maxBytes` fails the poll with
    /// `invalidSizeBytes`. It is read alone without the limit, and the next
    /// poll keeps the configured options.
    @Test func aMessageLargerThanTheByteLimitIsReadAlone() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let polls = Locked(0)
        await serve(server) { _ in
            let poll = polls.withLock { polls in
                polls += 1
                return polls
            }
            switch poll {
            case 1: return .status(IggyErrorCode.invalidSizeBytes.rawValue)
            case 2: return .ok(FakeVSRServer.polledBody(partitionID: 1, offsets: [0]))
            default: return .ok(FakeVSRServer.polledBody(partitionID: 1, offsets: []))
            }
        }
        let client = try await signedInClient(server)
        let options = DeferredPollOptions(minCount: 5, maxBytes: 4096)
        let configuration = ConsumerConfiguration(pollingStrategy: .offset(0), batchLength: 10, pollOptions: options, autoCommit: .disabled)
        let consumer = try client.consumer(name: "reader", streamID: 1, topicID: 2, partitionID: 1, configuration: configuration)
        try await consumer.initialize()
        #expect(try await consumer.next()?.message.offset == 0)
        let reading = Task { try await consumer.next() }
        #expect(await eventually { await requests(server, .pollMessagesDeferred).count >= 3 })
        await consumer.shutdown()
        #expect(try await reading.value == nil)

        let deferred = await requests(server, .pollMessagesDeferred)
        let limits = deferred.prefix(3).map { (polledStrategy($0).count, $0.deferredOptions.minCount, $0.deferredOptions.maxBytes) }
        #expect(limits.map(\.0) == [10, 1, 10])
        #expect(limits.map(\.1) == [5, 1, 5])
        #expect(limits.map(\.2) == [4096, DeferredPollOptions.maxReplyBytes, 4096])
        // The message read alone is where the next poll continues.
        #expect(deferred.prefix(3).map { polledStrategy($0).value } == [0, 0, 1])
        await client.shutdown()
    }

    /// A shutdown cancels a poll that commits nothing at once. A committing
    /// poll gets the drain timeout to bring its batch back, then is cancelled.
    @Test func aShutdownCancelsAParkedPollUnlessItCommits() async throws {
        let drainTimeout: Duration = .seconds(2)
        for autoCommit in [AutoCommit.interval(.seconds(30)), .when(.pollingMessages)] {
            let server = try await FakeVSRServer.started()
            await serve(server) { _ in .stall }
            let client = try await signedInClient(server)
            let configuration = ConsumerConfiguration(autoCommit: autoCommit, offsetDrainTimeout: drainTimeout)
            let consumer = try client.consumer(name: "reader", streamID: 1, topicID: 2, partitionID: 1, configuration: configuration)
            try await consumer.initialize()
            let reading = Task { try await consumer.next() }
            #expect(await eventually { await requests(server, .pollMessagesDeferred).count == 1 })
            let elapsed = await ContinuousClock().measure { await consumer.shutdown() }
            #expect(try await reading.value == nil)
            if autoCommit == .when(.pollingMessages) {
                #expect(elapsed >= drainTimeout, "took \(elapsed)")
            } else {
                #expect(elapsed < drainTimeout, "took \(elapsed)")
            }
            #expect(await eventually { await requests(server, .cancelDeferredRequest).count == 1 }, "\(autoCommit)")
            let poll = try #require(await requests(server, .pollMessagesDeferred).first)
            #expect(await requests(server, .cancelDeferredRequest).map(\.requestID) == [poll.requestID])
            await client.shutdown()
            await server.stop()
        }
    }

    /// The server stores the batch of a committing poll as it reads it. One
    /// the shutdown waited for is never handed over, so the final store moves
    /// the offset back to the last message handed over, as
    /// `rewind_undelivered_offsets` in the Rust SDK, even when a tick already
    /// stored that message.
    @Test func aBatchStoredDuringTheShutdownIsRewoundToTheLastMessageHandedOver() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let polls = Locked(0)
        await server.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .getStream: return .ok(GoldenFixture.shared.bytes("response.get_stream"))
            case .getTopic: return .ok(GoldenFixture.shared.bytes("response.get_topic"))
            case .pollMessagesDeferred:
                let poll = polls.withLock { polls in
                    polls += 1
                    return polls
                }
                guard poll > 1 else {
                    return .ok(FakeVSRServer.polledBody(partitionID: 1, offsets: [0]))
                }
                try? await Task.sleep(for: .milliseconds(300))
                return .ok(FakeVSRServer.polledBody(partitionID: 1, offsets: [1, 2]))
            default: return FakeVSRServer.defaultReply(request, address: server.address)
            }
        }
        let client = try await signedInClient(server)
        let configuration = ConsumerConfiguration(autoCommit: .intervalOrWhen(.milliseconds(50), .pollingMessages))
        let consumer = try client.consumer(name: "reader", streamID: 1, topicID: 2, partitionID: 1, configuration: configuration)
        try await consumer.initialize()
        #expect(try await consumer.next()?.message.offset == 0)
        #expect(await eventually { await requests(server, .storeConsumerOffset).map(storedOffset) == [0] })
        let reading = Task { try await consumer.next() }
        #expect(await eventually { polls.withLock { $0 } == 2 })
        await consumer.shutdown()
        #expect(try await reading.value == nil)
        #expect(await requests(server, .storeConsumerOffset).map(storedOffset) == [0, 0])
        #expect(await consumer.lastStoredOffset(partitionID: 1) == 0)
        #expect(polls.withLock { $0 } == 2)
        await client.shutdown()
    }

    /// With nothing handed over, a `next` consumer resumes after the stored
    /// offset, so the offset is moved back before the batch, or deleted when
    /// the batch starts the partition. Any other start point stores nothing.
    /// The store and the delete carry the context of the batch, not the one
    /// of a newer route, so neither lands in a newer partition history.
    @Test func aBatchStoredBeforeAnythingWasHandedOverIsRewound() async throws {
        let cases: [(strategy: PollingStrategy, offsets: [UInt64], stored: [UInt64], deletes: Int)] = [
            (.next, [5, 6], [4], 0), (.next, [0, 1], [], 1), (.first, [5, 6], [], 0),
        ]
        let older = PartitionContext(
            incarnation: FakeVSRServer.routeContext.incarnation - 1, ownerGeneration: 0, metadataOp: FakeVSRServer.routeContext.metadataOp)
        for scenario in cases {
            let server = try await FakeVSRServer.started()
            await serve(server) { _ in
                try? await Task.sleep(for: .milliseconds(300))
                return .ok(FakeVSRServer.polledBody(partitionID: 1, offsets: scenario.offsets, context: older))
            }
            let client = try await signedInClient(server)
            let configuration = ConsumerConfiguration(pollingStrategy: scenario.strategy, autoCommit: .when(.pollingMessages))
            let consumer = try client.consumer(name: "reader", streamID: 1, topicID: 2, partitionID: 1, configuration: configuration)
            try await consumer.initialize()
            let reading = Task { try await consumer.next() }
            #expect(await eventually { await requests(server, .pollMessagesDeferred).count == 1 })
            await consumer.shutdown()
            #expect(try await reading.value == nil)
            let stores = await requests(server, .storeConsumerOffset)
            let deletes = await requests(server, .deleteConsumerOffset)
            #expect(stores.map(storedOffset) == scenario.stored, "\(scenario.strategy) \(scenario.offsets)")
            #expect(deletes.count == scenario.deletes, "\(scenario.strategy) \(scenario.offsets)")
            #expect((stores + deletes).allSatisfy { $0.context == older }, "\(scenario.strategy) \(scenario.offsets)")
            await client.shutdown()
            await server.stop()
        }
    }

    /// A message handed over in an older partition history does not cover a
    /// batch of a newer one: the server refuses the final store of that
    /// message. So the batch is rewound as if nothing was handed over, under
    /// its own context.
    @Test func aBatchOfANewerPartitionHistoryIsRewoundUnderItsOwnContext() async throws {
        let cases: [(offsets: [UInt64], stored: [UInt64], deletes: Int)] = [([5, 6], [4], 0), ([0, 1], [], 1)]
        let recreated = PartitionContext(
            incarnation: FakeVSRServer.routeContext.incarnation + 1, ownerGeneration: 0, metadataOp: FakeVSRServer.routeContext.metadataOp)
        for scenario in cases {
            let server = try await FakeVSRServer.started()
            let polls = Locked(0)
            await server.respond { request in
                switch request.code.flatMap(CommandCode.init(rawValue:)) {
                case .getStream: return .ok(GoldenFixture.shared.bytes("response.get_stream"))
                case .getTopic: return .ok(GoldenFixture.shared.bytes("response.get_topic"))
                case .storeConsumerOffset, .deleteConsumerOffset:
                    return request.context == recreated ? .ok([]) : .status(IggyErrorCode.historyUnavailable.rawValue)
                case .pollMessagesDeferred:
                    let poll = polls.withLock { polls in
                        polls += 1
                        return polls
                    }
                    guard poll > 1 else {
                        return .ok(FakeVSRServer.polledBody(partitionID: 1, offsets: [3]))
                    }
                    try? await Task.sleep(for: .milliseconds(300))
                    return .ok(FakeVSRServer.polledBody(partitionID: 1, offsets: scenario.offsets, context: recreated))
                default: return FakeVSRServer.defaultReply(request, address: server.address)
                }
            }
            let client = try await signedInClient(server)
            let configuration = ConsumerConfiguration(autoCommit: .when(.pollingMessages))
            let consumer = try client.consumer(name: "reader", streamID: 1, topicID: 2, partitionID: 1, configuration: configuration)
            try await consumer.initialize()
            let reading = Task { try await read(consumer, 2) }
            #expect(await eventually { polls.withLock { $0 } == 2 })
            await consumer.shutdown()
            #expect(try await reading.value == [3])
            let stores = await requests(server, .storeConsumerOffset).filter { $0.context == recreated }
            let deletes = await requests(server, .deleteConsumerOffset).filter { $0.context == recreated }
            #expect(stores.map(storedOffset) == scenario.stored, "\(scenario.offsets)")
            #expect(deletes.count == scenario.deletes, "\(scenario.offsets)")
            // The final store of the message handed over goes out and is refused.
            #expect(await requests(server, .storeConsumerOffset).filter { $0.context != recreated }.map(storedOffset) == [3], "\(scenario.offsets)")
            await client.shutdown()
            await server.stop()
        }
    }

    /// Another member reads a partition that left the assignment, so a batch
    /// of it that the shutdown waited for leaves its offset alone, as
    /// `rewind_undelivered_offsets` in the Rust SDK. A partition the member
    /// still owns is rewound.
    @Test func aBatchOfAPartitionThatLeftTheAssignmentIsNotRewound() async throws {
        for leaves in [false, true] {
            let server = try await FakeVSRServer.started()
            let topic = FakeTopic(ends: [:], assignment: [1])
            await serve(server, topic: topic) { request in
                guard request.code == CommandCode.fetchMessages.rawValue else {
                    return nil
                }
                try? await Task.sleep(for: .milliseconds(300))
                return .ok(FakeVSRServer.fetchedBody([.init(partitionID: 1, offsets: [5, 6])]))
            }
            let client = try await signedInClient(server)
            let configuration = ConsumerConfiguration(autoCommit: .when(.pollingMessages))
            let consumer = try client.consumerGroup(name: "billing", streamID: 1, topicID: 2, configuration: configuration)
            try await consumer.initialize()
            let reading = Task { try await consumer.next() }
            #expect(await eventually { await requests(server, .fetchMessages).count == 1 })
            if leaves {
                topic.assign([2])
            }
            await client.refreshConsumerGroupAssignments()
            await consumer.shutdown()
            #expect(try await reading.value == nil)
            #expect(await requests(server, .storeConsumerOffset).map(storedOffset) == (leaves ? [] : [4]), "leaves: \(leaves)")
            await client.shutdown()
            await server.stop()
        }
    }
}
