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

private func signedInClient(_ server: FakeVSRServer, requestTimeout: Duration = .seconds(10)) async throws -> IggyClient {
    var configuration = ClientConfiguration(address: server.address, autoLogin: .usernamePassword(username: "iggy", password: "iggy"))
    configuration.reconnection = ReconnectionOptions(maxRetries: nil, interval: .milliseconds(20), reestablishAfter: .zero)
    configuration.requestTimeout = requestTimeout
    configuration.connectTimeout = .seconds(2)
    configuration.heartbeatInterval = .seconds(60)
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
        case .pollMessages: return await poll(request)
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

/// The strategy and auto-commit flag a poll ends with:
/// `[kind:1][value:8][count:4][auto_commit:1]`.
private func polledStrategy(_ request: FakeVSRServer.Request) -> (kind: UInt8, value: UInt64, autoCommit: Bool) {
    let end = request.payload.count
    return (request.payload[end - 14], UInt64(littleEndianBytes: request.payload[end - 13..<end - 5]), request.payload[end - 1] == 1)
}

/// The offset a store ends with: `[partition:4][offset:8][ack:1]`.
private func storedOffset(_ request: FakeVSRServer.Request) -> UInt64 {
    let end = request.payload.count
    return UInt64(littleEndianBytes: request.payload[end - 9..<end - 1])
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
        let polls = await requests(server, .pollMessages)
        #expect(polls.prefix(2).map { polledStrategy($0).value } == [0, 3])
        #expect(polls[1].context == FakeVSRServer.routeContext)
        #expect(await consumer.currentPartitionID == 1)
        #expect(await consumer.lastConsumedOffset(partitionID: 1) == 4)
        await consumer.shutdown()
        await client.shutdown()
    }

    @Test func nextLeavesTheContinuationToTheServer() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await serve(server, poll: batches([[0, 1], [2]]))
        let client = try await signedInClient(server)
        let consumer = try client.consumer(
            name: "reader", streamID: 1, topicID: 2, partitionID: 1, configuration: ConsumerConfiguration(autoCommit: .disabled))
        try await consumer.initialize()
        #expect(try await read(consumer, 3) == [0, 1, 2])
        let kinds = await requests(server, .pollMessages).prefix(2).map { polledStrategy($0).kind }
        #expect(kinds == [PollingStrategy.Kind.next.rawValue, PollingStrategy.Kind.next.rawValue])
        await consumer.shutdown()
        await client.shutdown()
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

    /// A failed poll is thrown after the retry interval and the consumer
    /// keeps reading on the next call.
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
        let clock = ContinuousClock()
        let elapsed = await clock.measure {
            await expectCode(.partitionNotFound) { _ = try await consumer.next() }
        }
        #expect(elapsed >= .milliseconds(140))
        #expect(try await consumer.next()?.message.offset == 0)
        await consumer.shutdown()
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
        #expect(await eventually { await requests(server, .pollMessages).count >= 3 })
        let kinds = await requests(server, .pollMessages).prefix(3).map { polledStrategy($0).kind }
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
        #expect(await requests(server, .pollMessages).first.map { polledStrategy($0).autoCommit } == true)
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
            #expect(await requests(server, .pollMessages).first.map { polledStrategy($0).autoCommit } == false)
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
    /// and joins again once a poll finds the membership gone.
    @Test func aGroupMemberJoinsAndJoinsAgainAfterLosingTheMembership() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let revoked = Locked(false)
        let groupExists = Locked(false)
        await server.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .getStream: return .ok(GoldenFixture.shared.bytes("response.get_stream"))
            case .getTopic: return .ok(GoldenFixture.shared.bytes("response.get_topic"))
            case .getConsumerGroup: return groupExists.withLock { $0 } ? .ok(GoldenFixture.shared.bytes("response.consumer_group_details")) : .ok([])
            case .createConsumerGroup:
                groupExists.withLock { $0 = true }
                return .ok(GoldenFixture.shared.bytes("response.consumer_group_details"))
            case .syncConsumerGroup: return .ok(FakeVSRServer.assignmentBody(generation: 1, partitions: [1]))
            case .pollMessages:
                if !revoked.exchange(true) {
                    return .status(IggyErrorCode.consumerGroupMemberNotFound.rawValue)
                }
                return .ok(FakeVSRServer.polledBody(partitionID: 1, offsets: [0]))
            default: return FakeVSRServer.defaultReply(request, address: server.address)
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
            case .pollMessages: return await poll(request)
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

    /// Concurrent initializations share one run: one lookup, one join.
    @Test func concurrentInitializationsShareOneRun() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await serve(server, poll: batches([[0]]))
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
            ConsumerConfiguration(pollInterval: .milliseconds(-1)), ConsumerConfiguration(offsetDrainTimeout: ProducerConfiguration.maxDuration * 2),
        ]
        for configuration in invalid {
            await expectCode(.invalidConfiguration) {
                _ = try client.consumer(name: "reader", streamID: 1, topicID: 2, partitionID: 1, configuration: configuration)
            }
        }
        _ = try client.consumerGroup(name: "billing", streamID: 1, topicID: 2, configuration: ConsumerConfiguration(pollInterval: .zero))
        await client.shutdown()
    }
}
