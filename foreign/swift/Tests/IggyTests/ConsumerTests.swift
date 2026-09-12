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

import Testing

@testable import Iggy

private let standalone = Consumer.consumer("c")
private let member = Consumer.group("g")

private func makeConsumer(
    _ backend: FakeBackend, consumer: Consumer = standalone, partition: UInt32? = 0, _ configuration: ConsumerConfiguration = ConsumerConfiguration()
) -> IggyConsumer {
    IggyConsumer(
        backend: backend, name: consumer.id.name ?? "c", consumer: consumer, stream: "s", topic: "t", partition: partition, configuration: configuration,
        logger: testLogger)
}

/// A backend with `count` messages in partition 0 of the topic.
private func filledBackend(_ count: Int, partitions: Int = 1) async throws -> FakeBackend {
    let backend = await FakeBackend.prepared(partitions: partitions)
    _ = try await backend.sendMessages(streamID: "s", topicID: "t", partitioning: .partition(0), messages: makeMessages(count))
    await backend.reset()
    return backend
}

private func polls(_ backend: FakeBackend) async -> [FakeBackend.Call] {
    await backend.calls.filter {
        if case .poll = $0 { return true }
        return false
    }
}

private func stores(_ backend: FakeBackend) async -> [(UInt32?, UInt64)] {
    await backend.calls.compactMap {
        if case .storeOffset(_, let partition, let offset) = $0 { return (partition, offset) }
        return nil
    }
}

/// Auto-commit disabled and no interval, so every request is the test's own.
private let quiet = ConsumerConfiguration(autoCommit: .disabled, pollingRetryInterval: .milliseconds(5))

@Suite("Consumer initialization")
struct ConsumerInitializationTests {
    @Test func checksStreamAndTopic() async throws {
        let backend = await FakeBackend.prepared()
        let consumer = makeConsumer(backend)
        try await consumer.initialize()
        #expect(await consumer.isInitialized)
        #expect(await backend.calls == [.getStream("s"), .getTopic("s", "t")])
        try await consumer.initialize()
        #expect(await backend.calls.count == 2)
        await consumer.shutdown()
    }

    @Test func missingStreamOrTopicFailsWithoutRetries() async throws {
        let backend = FakeBackend()
        await expectCode(.streamNameNotFound) { try await makeConsumer(backend).initialize() }
        await backend.addStream("s")
        await expectCode(.topicNameNotFound) { try await makeConsumer(backend).initialize() }
        #expect(await backend.subscriberCount == 0)
    }

    @Test func retriesUntilTheTopicAppears() async throws {
        let backend = FakeBackend()
        await backend.addStream("s")
        let consumer = makeConsumer(backend, ConsumerConfiguration(initRetries: 5, initRetryInterval: .milliseconds(10)))
        let creator = Task {
            try await Task.sleep(for: .milliseconds(25))
            await backend.addTopic(stream: "s", name: "t", partitions: 1)
        }
        try await consumer.initialize()
        try await creator.value
        #expect(await backend.calls.filter { $0 == .getTopic("s", "t") }.count >= 2)
        await consumer.shutdown()
    }

    @Test func retryBudgetIsBounded() async throws {
        let backend = FakeBackend()
        let consumer = makeConsumer(backend, ConsumerConfiguration(initRetries: 2, initRetryInterval: .milliseconds(1)))
        await expectCode(.streamNameNotFound) { try await consumer.initialize() }
        #expect(await backend.calls.filter { $0 == .getStream("s") }.count == 3)
    }

    @Test func groupMemberCreatesAndJoins() async throws {
        let backend = await FakeBackend.prepared()
        let consumer = makeConsumer(backend, consumer: member, partition: 5)
        try await consumer.initialize()
        #expect(await backend.calls == [.getStream("s"), .getTopic("s", "t"), .getGroup("s", "t", "g"), .createGroup("s", "t", "g"), .join("s", "t", "g")])
        #expect(await backend.members(stream: "s", topic: "t", group: "g") == 1)
        await consumer.shutdown()
        #expect(await backend.members(stream: "s", topic: "t", group: "g") == 0)
        #expect(await backend.calls.last == .leave("s", "t", "g"))
    }

    @Test func existingGroupIsJoinedNotCreated() async throws {
        let backend = await FakeBackend.prepared()
        await backend.addGroup(stream: "s", topic: "t", name: "g")
        let consumer = makeConsumer(backend, consumer: member)
        try await consumer.initialize()
        #expect(await backend.calls.contains(.createGroup("s", "t", "g")) == false)
        #expect(await backend.calls.contains(.join("s", "t", "g")))
        await consumer.shutdown()
    }

    @Test func missingGroupWithCreationDisabled() async throws {
        let backend = await FakeBackend.prepared()
        let consumer = makeConsumer(backend, consumer: member, ConsumerConfiguration(createConsumerGroupIfNotExists: false))
        await expectCode(.consumerGroupNameNotFound) { try await consumer.initialize() }
    }

    @Test func autoJoinDisabledLeavesJoiningToTheCaller() async throws {
        let backend = await FakeBackend.prepared()
        let consumer = makeConsumer(
            backend, consumer: member, ConsumerConfiguration(autoCommit: .disabled, autoJoinConsumerGroup: false, pollingRetryInterval: .milliseconds(1)))
        try await consumer.initialize()
        #expect(await backend.calls.contains(.join("s", "t", "g")) == false)
        // The fake refuses a group poll from a non-member, like the server.
        await expectCode(.consumerGroupMemberNotFound) { _ = try await consumer.next() }
        await consumer.shutdown()
    }

    @Test func raceOnGroupCreationIsTolerated() async throws {
        let backend = await FakeBackend.prepared()
        let first = makeConsumer(backend, consumer: member, quiet)
        let second = makeConsumer(backend, consumer: member, quiet)
        try await first.initialize()
        try await second.initialize()
        #expect(await backend.members(stream: "s", topic: "t", group: "g") == 2)
        await first.shutdown()
        await second.shutdown()
    }
}

@Suite("Consumer reading")
struct ConsumerReadingTests {
    @Test func handsMessagesOverInOrderAcrossPolls() async throws {
        let backend = try await filledBackend(7)
        let consumer = makeConsumer(backend, ConsumerConfiguration(pollingStrategy: .first, batchLength: 3, autoCommit: .disabled))
        try await consumer.initialize()
        var received: [ReceivedMessage] = []
        for _ in 0..<7 {
            received.append(try #require(try await consumer.next()))
        }
        #expect(received.map(\.message.offset) == Array(0..<7))
        #expect(received.map(\.message.payloadString) == (0..<7).map { "m\($0)" })
        #expect(received.allSatisfy { $0.currentOffset == 6 && $0.partitionID == 0 })
        // The first poll starts where the strategy says, later ones continue
        // after the last message handed over.
        let strategies = await polls(backend).compactMap { call -> PollingStrategy? in
            if case .poll(_, _, _, _, let strategy, _, _) = call { return strategy }
            return nil
        }
        #expect(strategies == [.first, .offset(3), .offset(6)])
        #expect(await consumer.lastConsumedOffset(partitionID: 0) == 6)
        #expect(await consumer.partitionID == 0)
        await consumer.shutdown()
    }

    @Test func iteratesAsAnAsyncSequence() async throws {
        let backend = try await filledBackend(3)
        let consumer = makeConsumer(backend, ConsumerConfiguration(pollingStrategy: .first, autoCommit: .disabled))
        try await consumer.initialize()
        var payloads: [String] = []
        for try await received in consumer {
            payloads.append(received.message.payloadString)
            if payloads.count == 3 {
                await consumer.shutdown()
            }
        }
        #expect(payloads == ["m0", "m1", "m2"])
        #expect(try await consumer.next() == nil)
    }

    @Test func nextStrategyLeavesContinuationToTheServer() async throws {
        let backend = try await filledBackend(4)
        try await backend.storeConsumerOffset(consumer: standalone, streamID: "s", topicID: "t", partitionID: 0, offset: 1)
        await backend.reset()
        let consumer = makeConsumer(backend, ConsumerConfiguration(batchLength: 1, autoCommit: .when(.pollingMessages)))
        try await consumer.initialize()
        #expect(try await consumer.next()?.message.offset == 2)
        #expect(try await consumer.next()?.message.offset == 3)
        let strategies = await polls(backend).compactMap { call -> PollingStrategy? in
            if case .poll(_, _, _, _, let strategy, _, _) = call { return strategy }
            return nil
        }
        #expect(strategies == [.next, .next])
        await consumer.shutdown()
    }

    @Test func alreadyConsumedOffsetsAreSkippedUnlessReplayIsAllowed() async throws {
        let backend = try await filledBackend(3)
        let consumer = makeConsumer(backend, ConsumerConfiguration(pollingStrategy: .first, autoCommit: .disabled))
        try await consumer.initialize()
        _ = try await consumer.next()
        _ = try await consumer.next()
        _ = try await consumer.next()
        // A replayed batch (the server answered from offset zero again) only
        // yields what is new.
        let replay = PolledMessages(
            partitionID: 0, currentOffset: 3, count: 4,
            messages: await backend.messages(stream: "s", topic: "t", partition: 0)
                + makeMessages(1).map {
                    var m = $0; m.offset = 3; return m
                })
        await backend.scriptPolls([replay])
        #expect(try await consumer.next()?.message.offset == 3)
        await consumer.shutdown()

        let replaying = makeConsumer(backend, ConsumerConfiguration(pollingStrategy: .first, autoCommit: .disabled, allowReplay: true))
        try await replaying.initialize()
        for _ in 0..<3 {
            _ = try await replaying.next()
        }
        await backend.scriptPolls([replay])
        #expect(try await replaying.next()?.message.offset == 0)
        await replaying.shutdown()
    }

    @Test func emptyPollsAreRetriedAndPacedByThePollInterval() async throws {
        let backend = await FakeBackend.prepared()
        let consumer = makeConsumer(backend, ConsumerConfiguration(pollingStrategy: .first, pollInterval: .milliseconds(20), autoCommit: .disabled))
        try await consumer.initialize()
        let producer = Task {
            try await Task.sleep(for: .milliseconds(70))
            _ = try await backend.sendMessages(streamID: "s", topicID: "t", partitioning: .partition(0), messages: makeMessages(1))
        }
        let clock = ContinuousClock()
        let start = clock.now
        let received = try await consumer.next()
        try await producer.value
        #expect(received?.message.payloadString == "m0")
        #expect(clock.now - start >= .milliseconds(60))
        let count = await polls(backend).count
        #expect(count >= 3 && count <= 6)
        await consumer.shutdown()
    }

    @Test func noAssignedPartitionWaitsTheRetryInterval() async throws {
        let backend = await FakeBackend.prepared()
        await backend.scriptPolls([PolledMessages(partitionID: PolledMessages.noAssignedPartition, currentOffset: 0, count: 0, messages: [])])
        _ = try await backend.sendMessages(streamID: "s", topicID: "t", partitioning: .partition(0), messages: makeMessages(1))
        let consumer = makeConsumer(backend, ConsumerConfiguration(pollingStrategy: .first, autoCommit: .disabled, pollingRetryInterval: .milliseconds(40)))
        try await consumer.initialize()
        let clock = ContinuousClock()
        let elapsed = await clock.measure { _ = try? await consumer.next() }
        #expect(elapsed >= .milliseconds(35))
        await consumer.shutdown()
    }

    @Test func pollErrorsAreThrownAndPollingContinues() async throws {
        let backend = try await filledBackend(1)
        let consumer = makeConsumer(backend, ConsumerConfiguration(pollingStrategy: .first, autoCommit: .disabled, pollingRetryInterval: .milliseconds(1)))
        try await consumer.initialize()
        await backend.failPolls(1, with: IggyError(.topicIdNotFound))
        await expectCode(.topicIdNotFound) { _ = try await consumer.next() }
        #expect(try await consumer.next()?.message.offset == 0)
        await consumer.shutdown()
    }

    @Test func disconnectionPausesPollingUntilSignedIn() async throws {
        let backend = try await filledBackend(1)
        let consumer = makeConsumer(backend, ConsumerConfiguration(pollingStrategy: .first, autoCommit: .disabled, pollingRetryInterval: .milliseconds(5)))
        try await consumer.initialize()
        #expect(await eventually { await backend.subscriberCount == 1 })
        await backend.failPolls(1, with: IggyError(.disconnected))
        await expectCode(.disconnected) { _ = try await consumer.next() }
        let pollCount = await polls(backend).count
        let pending = Task { try await consumer.next() }
        try await Task.sleep(for: .milliseconds(30))
        #expect(await polls(backend).count == pollCount)
        await backend.emit(.signedIn)
        #expect(try await pending.value?.message.offset == 0)
        await consumer.shutdown()
    }

    @Test func revokedMembershipIsRejoined() async throws {
        let backend = await FakeBackend.prepared()
        _ = try await backend.sendMessages(streamID: "s", topicID: "t", partitioning: .partition(0), messages: makeMessages(1))
        let consumer = makeConsumer(
            backend, consumer: member, ConsumerConfiguration(pollingStrategy: .first, autoCommit: .disabled, pollingRetryInterval: .milliseconds(1)))
        try await consumer.initialize()
        await backend.reset()
        await backend.failPolls(1, with: IggyError(.consumerGroupMemberNotFound))
        #expect(try await consumer.next()?.message.offset == 0)
        let calls = await backend.calls
        #expect(calls.contains(.join("s", "t", "g")))
        #expect(calls.filter { if case .poll = $0 { return true } else { return false } }.count == 2)
        await consumer.shutdown()
    }

    @Test func reconnectionForcesAGroupRejoin() async throws {
        let backend = await FakeBackend.prepared()
        _ = try await backend.sendMessages(streamID: "s", topicID: "t", partitioning: .partition(0), messages: makeMessages(1))
        let consumer = makeConsumer(
            backend, consumer: member, ConsumerConfiguration(pollingStrategy: .first, autoCommit: .disabled, pollingRetryInterval: .milliseconds(1)))
        try await consumer.initialize()
        #expect(await eventually { await backend.subscriberCount == 1 })
        await backend.reset()
        await backend.emit(.disconnected)
        #expect(
            await eventually {
                let joined = await consumer.core.joined
                let canPoll = await consumer.core.canPoll
                return !joined && !canPoll
            })
        await backend.emit(.connected)
        #expect(await eventually { !(await consumer.core.canPoll) })
        await backend.emit(.signedIn)
        #expect(await eventually { await consumer.core.canPoll })
        #expect(try await consumer.next()?.message.offset == 0)
        #expect(await backend.calls.contains(.join("s", "t", "g")))
        await consumer.shutdown()
    }

    @Test func shutdownEndsTheIteration() async throws {
        let backend = await FakeBackend.prepared()
        let consumer = makeConsumer(backend, ConsumerConfiguration(pollingStrategy: .first, autoCommit: .disabled, pollingRetryInterval: .milliseconds(5)))
        try await consumer.initialize()
        // Block the poll on a disconnected client, then shut down under it.
        await backend.failPolls(1, with: IggyError(.disconnected))
        await expectCode(.disconnected) { _ = try await consumer.next() }
        let pending = Task { try await consumer.next() }
        try await Task.sleep(for: .milliseconds(10))
        await consumer.shutdown()
        #expect(try await pending.value == nil)
        #expect(try await consumer.next() == nil)
    }
}

@Suite("Consumer offsets")
struct ConsumerOffsetTests {
    @Test func commitOnPollSetsTheRequestFlag() async throws {
        let backend = try await filledBackend(2)
        let consumer = makeConsumer(backend, ConsumerConfiguration(pollingStrategy: .first, batchLength: 10, autoCommit: .when(.pollingMessages)))
        try await consumer.initialize()
        _ = try await consumer.next()
        if case .poll(_, _, _, _, _, _, let autoCommit) = try #require(await polls(backend).first) {
            #expect(autoCommit)
        }
        #expect(await backend.storedOffset(consumer: standalone, partition: 0) == 1)
        _ = try await consumer.next()
        // The local record trails the poll by one batch, so shutdown stores
        // the last message handed over even though the poll already covered it.
        await consumer.shutdown()
        #expect(await stores(backend).map(\.1) == [1])
    }

    @Test func commitOnPollThenShutdownMidBatch() async throws {
        let backend = try await filledBackend(3)
        let consumer = makeConsumer(backend, ConsumerConfiguration(pollingStrategy: .first, batchLength: 10, autoCommit: .when(.pollingMessages)))
        try await consumer.initialize()
        _ = try await consumer.next()
        _ = try await consumer.next()
        await consumer.shutdown()
        #expect(await stores(backend).map(\.1) == [1])
        #expect(await backend.storedOffset(consumer: standalone, partition: 0) == 1)
    }

    @Test func commitAfterEachMessage() async throws {
        let backend = try await filledBackend(3)
        let consumer = makeConsumer(backend, ConsumerConfiguration(pollingStrategy: .first, autoCommit: .when(.consumingEachMessage)))
        try await consumer.initialize()
        for _ in 0..<3 {
            _ = try await consumer.next()
        }
        #expect(await eventually { await backend.storedOffset(consumer: standalone, partition: 0) == 2 })
        if case .poll(_, _, _, _, _, _, let autoCommit) = try #require(await polls(backend).first) {
            #expect(!autoCommit)
        }
        await consumer.shutdown()
        #expect(await stores(backend).map(\.1).last == 2)
        #expect(await consumer.lastStoredOffset(partitionID: 0) == 2)
    }

    @Test func commitEveryNthMessage() async throws {
        let backend = try await filledBackend(7)
        let consumer = makeConsumer(backend, ConsumerConfiguration(pollingStrategy: .first, batchLength: 2, autoCommit: .when(.consumingEveryNthMessage(3))))
        try await consumer.initialize()
        for _ in 0..<7 {
            _ = try await consumer.next()
        }
        #expect(await eventually { await backend.storedOffset(consumer: standalone, partition: 0) == 6 })
        await consumer.shutdown()
        let offsets = await stores(backend).map(\.1)
        // Offsets 0, 3, and 6 are the multiples; a later one can replace an
        // earlier one still queued, so only the order and the end are fixed.
        #expect(Set(offsets).isSubset(of: [0, 3, 6]))
        #expect(offsets == offsets.sorted())
        #expect(offsets.last == 6)
    }

    @Test func commitAfterAllMessagesOfABatch() async throws {
        let backend = try await filledBackend(5)
        let consumer = makeConsumer(backend, ConsumerConfiguration(pollingStrategy: .first, batchLength: 3, autoCommit: .when(.consumingAllMessages)))
        try await consumer.initialize()
        _ = try await consumer.next()
        _ = try await consumer.next()
        try await Task.sleep(for: .milliseconds(20))
        #expect(await stores(backend).isEmpty)
        _ = try await consumer.next()
        #expect(await eventually { await backend.storedOffset(consumer: standalone, partition: 0) == 2 })
        _ = try await consumer.next()
        _ = try await consumer.next()
        #expect(await eventually { await backend.storedOffset(consumer: standalone, partition: 0) == 4 })
        await consumer.shutdown()
    }

    @Test func intervalCommitsTheReadingPosition() async throws {
        let backend = try await filledBackend(3)
        let consumer = makeConsumer(backend, ConsumerConfiguration(pollingStrategy: .first, batchLength: 10, autoCommit: .interval(.milliseconds(20))))
        try await consumer.initialize()
        try await Task.sleep(for: .milliseconds(50))
        // Nothing consumed yet, so nothing to store.
        #expect(await stores(backend).isEmpty)
        _ = try await consumer.next()
        _ = try await consumer.next()
        #expect(await eventually { await backend.storedOffset(consumer: standalone, partition: 0) == 1 })
        await consumer.shutdown()
        // The final flush has nothing new to add.
        #expect(await stores(backend).map(\.1).filter { $0 == 1 }.count >= 1)
        #expect(await stores(backend).map(\.1).allSatisfy { $0 <= 1 })
    }

    @Test func disabledAutoCommitStoresNothing() async throws {
        let backend = try await filledBackend(2)
        let consumer = makeConsumer(backend, ConsumerConfiguration(pollingStrategy: .first, autoCommit: .disabled))
        try await consumer.initialize()
        _ = try await consumer.next()
        _ = try await consumer.next()
        await consumer.shutdown()
        #expect(await stores(backend).isEmpty)
        if case .poll(_, _, _, _, _, _, let autoCommit) = try #require(await polls(backend).first) {
            #expect(!autoCommit)
        }
    }

    @Test func manualStoreSkipsOffsetsThatDoNotAdvance() async throws {
        let backend = try await filledBackend(5)
        let consumer = makeConsumer(backend, ConsumerConfiguration(pollingStrategy: .first, autoCommit: .disabled))
        try await consumer.initialize()
        _ = try await consumer.next()
        try await consumer.storeOffset(3)
        try await consumer.storeOffset(2)
        try await consumer.storeOffset(3)
        try await consumer.storeOffset(0)
        try await consumer.storeOffset(4, partitionID: 0)
        #expect(await stores(backend).map(\.1) == [3, 0, 4])
        #expect(await consumer.lastStoredOffset(partitionID: 0) == 4)
        await consumer.shutdown()

        let replaying = makeConsumer(backend, ConsumerConfiguration(pollingStrategy: .first, autoCommit: .disabled, allowReplay: true))
        try await replaying.initialize()
        await backend.reset()
        try await replaying.storeOffset(3)
        try await replaying.storeOffset(2)
        #expect(await stores(backend).map(\.1) == [3, 2])
        await replaying.shutdown()
    }

    @Test func storeFailuresPropagateAndKeepTheRecord() async throws {
        let backend = try await filledBackend(1)
        let consumer = makeConsumer(backend, ConsumerConfiguration(pollingStrategy: .first, autoCommit: .disabled))
        try await consumer.initialize()
        await backend.failStores(1, with: IggyError(.unauthorized))
        await expectCode(.unauthorized) { try await consumer.storeOffset(5) }
        #expect(await consumer.lastStoredOffset(partitionID: 0) == 0)
        try await consumer.storeOffset(5)
        #expect(await consumer.lastStoredOffset(partitionID: 0) == 5)
        await consumer.shutdown()
    }

    @Test func deleteOffsetResolvesThePartition() async throws {
        let backend = try await filledBackend(1)
        let consumer = makeConsumer(backend, ConsumerConfiguration(pollingStrategy: .first, autoCommit: .disabled))
        try await consumer.initialize()
        try await consumer.storeOffset(0)
        try await consumer.deleteOffset()
        await expectCode(.consumerOffsetNotFound) { try await consumer.deleteOffset(partitionID: 0) }
        #expect(await backend.calls.contains(.deleteOffset(standalone, 0)))
        await consumer.shutdown()

        let group = makeConsumer(backend, consumer: member, quiet)
        try await group.initialize()
        try? await group.deleteOffset()
        #expect(await backend.calls.contains(.deleteOffset(member, nil)))
        await group.shutdown()
    }

    @Test func shutdownStoresTheFinalPositionAndLeaves() async throws {
        let backend = await FakeBackend.prepared()
        _ = try await backend.sendMessages(streamID: "s", topicID: "t", partitioning: .partition(0), messages: makeMessages(3))
        let consumer = makeConsumer(
            backend, consumer: member, ConsumerConfiguration(pollingStrategy: .first, batchLength: 10, autoCommit: .interval(.seconds(30))))
        try await consumer.initialize()
        _ = try await consumer.next()
        _ = try await consumer.next()
        await backend.reset()
        await consumer.shutdown()
        #expect(await backend.calls == [.storeOffset(member, 0, 1), .leave("s", "t", "g")])
        await consumer.shutdown()
        #expect(await backend.calls.count == 2)
    }

    @Test func consumeCommitsAfterTheHandler() async throws {
        let backend = try await filledBackend(4)
        let consumer = makeConsumer(backend, ConsumerConfiguration(pollingStrategy: .first, batchLength: 2, autoCommit: .after(.consumingAllMessages)))
        try await consumer.initialize()
        let seen = Locked<[UInt64]>([])
        let runner = Task {
            try await consumer.consume { received in
                seen.withLock { $0.append(received.message.offset) }
                if received.message.offset == 3 {
                    await consumer.shutdown()
                }
            }
        }
        try await runner.value
        #expect(seen.withLock { $0 } == [0, 1, 2, 3])
        // One commit per batch end, at the last offset of the batch; the
        // second can replace the first while it is still queued.
        let batchEnds = await stores(backend).map(\.1)
        #expect(Set(batchEnds).isSubset(of: [1, 3]))
        #expect(batchEnds.last == 3)

        let each = makeConsumer(backend, ConsumerConfiguration(pollingStrategy: .first, autoCommit: .after(.consumingEveryNthMessage(2))))
        try await each.initialize()
        await backend.reset()
        let counter = Locked(0)
        try await each.consume { _ in
            let count = counter.withLock {
                $0 += 1; return $0
            }
            if count == 4 {
                await each.shutdown()
            }
        }
        // Offsets 0 and 2 are the multiples, and shutdown flushes the last
        // handed-over offset 3 on top of them.
        let everyOther = await stores(backend).map(\.1)
        #expect(Set(everyOther).isSubset(of: [0, 2, 3]))
        #expect(everyOther.contains(2))
        #expect(everyOther.last == 3)
    }

    @Test func handlerErrorsEndConsume() async throws {
        let backend = try await filledBackend(2)
        let consumer = makeConsumer(backend, quiet)
        try await consumer.initialize()
        struct Boom: Error {}
        await #expect(throws: Boom.self) {
            try await consumer.consume { _ in throw Boom() }
        }
        #expect(try await consumer.next()?.message.offset == 1)
        await consumer.shutdown()
    }
}

@Suite("Async primitives")
struct AsyncPrimitiveTests {
    @Test func semaphoreServesWaitersInOrder() async throws {
        let semaphore = AsyncSemaphore(permits: 2)
        #expect(await semaphore.tryAcquire(2))
        #expect(await !semaphore.tryAcquire(1))
        let order = Locked<[Int]>([])
        let big = Task {
            _ = await semaphore.acquire(2)
            order.withLock { $0.append(2) }
        }
        try await Task.sleep(for: .milliseconds(5))
        let small = Task {
            _ = await semaphore.acquire(1)
            order.withLock { $0.append(1) }
        }
        try await Task.sleep(for: .milliseconds(5))
        await semaphore.release(1)
        try await Task.sleep(for: .milliseconds(5))
        // One permit is free, but the big waiter is first in line.
        #expect(order.withLock { $0 }.isEmpty)
        await semaphore.release(1)
        await big.value
        #expect(order.withLock { $0 } == [2])
        await semaphore.release(1)
        await small.value
        #expect(order.withLock { $0 } == [2, 1])
    }

    @Test func semaphoreTimeoutAndCancellation() async throws {
        let semaphore = AsyncSemaphore(permits: 0)
        #expect(await !semaphore.acquire(1, timeout: .milliseconds(10)))
        let waiter = Task { await semaphore.acquire(1) }
        try await Task.sleep(for: .milliseconds(5))
        waiter.cancel()
        #expect(await !waiter.value)
        await semaphore.release(1)
        #expect(await semaphore.availablePermits == 1)
        #expect(await semaphore.acquire(1, timeout: .milliseconds(10)))
    }

    @Test func notifyRemembersOneSignal() async throws {
        let notify = AsyncNotify()
        await notify.notify()
        await notify.notify()
        await notify.wait()
        let waiter = Task { await notify.wait() }
        try await Task.sleep(for: .milliseconds(5))
        await notify.notify()
        await waiter.value
        let cancelled = Task { await notify.wait() }
        try await Task.sleep(for: .milliseconds(5))
        cancelled.cancel()
        await cancelled.value
        // The cancelled wait consumed nothing.
        let done = await withTimeout(.milliseconds(20)) { await notify.wait() }
        #expect(done == nil)
    }
}
