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

private func makeProducer(
    _ backend: FakeBackend, stream: Identifier = "s", topic: Identifier = "t", _ configuration: ProducerConfiguration = ProducerConfiguration()
) -> IggyProducer {
    IggyProducer(backend: backend, stream: stream, topic: topic, configuration: configuration, logger: testLogger)
}

private func noRetries(mode: SendMode = .direct) -> ProducerConfiguration {
    ProducerConfiguration(sendRetries: nil, mode: mode)
}

@Suite("Producer initialization")
struct ProducerInitializationTests {
    @Test func createsMissingStreamAndTopic() async throws {
        let backend = FakeBackend()
        let producer = makeProducer(backend, ProducerConfiguration(topicPartitionsCount: 3, topicMessageExpiry: .after(.seconds(60))))
        #expect(await !producer.isInitialized)
        try await producer.initialize()
        #expect(await producer.isInitialized)
        #expect(await backend.calls == [.getStream("s"), .createStream("s"), .getTopic("s", "t"), .createTopic("s", "t", 3)])
        // A second call is a no-op.
        try await producer.initialize()
        #expect(await backend.calls.count == 4)
        #expect(try await backend.getTopic(streamID: "s", topicID: "t")?.partitionsCount == 3)
    }

    @Test func existingResourcesAreNotRecreated() async throws {
        let backend = await FakeBackend.prepared()
        let producer = makeProducer(backend)
        try await producer.initialize()
        #expect(await backend.calls == [.getStream("s"), .getTopic("s", "t")])
    }

    @Test func refusesToCreateWhenDisabled() async throws {
        let backend = FakeBackend()
        let noStream = makeProducer(backend, ProducerConfiguration(createStreamIfNotExists: false))
        await expectCode(.streamNameNotFound) { try await noStream.initialize() }
        await backend.addStream("s")
        let noTopic = makeProducer(backend, ProducerConfiguration(createTopicIfNotExists: false))
        await expectCode(.topicNameNotFound) { try await noTopic.initialize() }
        #expect(await !noTopic.isInitialized)
    }

    @Test func numericIdentifiersCannotBeCreated() async throws {
        let backend = FakeBackend()
        let producer = makeProducer(backend, stream: 7, topic: 9)
        await expectCode(.streamIdNotFound) { try await producer.initialize() }
        await backend.addStream("s")
        let topicOnly = makeProducer(backend, stream: "s", topic: 9)
        await expectCode(.topicIdNotFound) { try await topicOnly.initialize() }
    }

    @Test func subscribesToEventsOnce() async throws {
        let backend = await FakeBackend.prepared()
        let producer = makeProducer(backend)
        try await producer.initialize()
        try await producer.initialize()
        #expect(await eventually { await backend.subscriberCount == 1 })
        await producer.shutdown()
    }
}

@Suite("Direct producer")
struct DirectProducerTests {
    @Test func sendsWholeBatchesAndReturnsConfirmations() async throws {
        let backend = await FakeBackend.prepared(partitions: 2)
        let producer = makeProducer(backend, ProducerConfiguration(partitioning: .partition(1)))
        try await producer.initialize()
        let response = try await producer.send(makeMessages(3))
        #expect(response.confirmations == [SendConfirmation(streamID: 1, topicID: 2, partitionID: 1, baseOffset: 0)])
        let again = try await producer.send(makeMessages(2))
        #expect(again.confirmations.first?.baseOffset == 3)
        #expect(await backend.messages(stream: "s", topic: "t", partition: 1).map(\.payloadString) == ["m0", "m1", "m2", "m0", "m1"])
    }

    @Test func emptySendIsANoOp() async throws {
        let backend = await FakeBackend.prepared()
        let producer = makeProducer(backend)
        let response = try await producer.send([])
        #expect(response.confirmations.isEmpty)
        #expect(await backend.calls.isEmpty)
    }

    @Test func chunksByBatchLengthInOrder() async throws {
        let backend = await FakeBackend.prepared()
        let producer = makeProducer(backend, ProducerConfiguration(mode: .direct(DirectSendOptions(batchLength: 4))))
        let response = try await producer.send(makeMessages(10))
        #expect(response.confirmations.map(\.baseOffset) == [0, 4, 8])
        let sends = await backend.calls.compactMap { call -> Int? in
            if case .send(_, _, _, let count) = call { return count }
            return nil
        }
        #expect(sends == [4, 4, 2])
        #expect(await backend.messages(stream: "s", topic: "t", partition: 0).map(\.offset) == Array(0..<10))
    }

    @Test func zeroBatchLengthSendsEverythingAtOnce() async throws {
        let backend = await FakeBackend.prepared()
        let producer = makeProducer(backend, ProducerConfiguration(mode: .direct(DirectSendOptions(batchLength: 0))))
        _ = try await producer.send(makeMessages(2500))
        #expect(await backend.calls.count == 1)
    }

    @Test func partitioningPrecedence() async throws {
        let backend = await FakeBackend.prepared(partitions: 3)
        let producer = makeProducer(backend, ProducerConfiguration(partitioning: .partition(2)))
        _ = try await producer.send(makeMessages(1))
        _ = try await producer.send(makeMessages(1), partitioning: .partition(1))
        _ = try await producer.send(makeMessages(1), partitioning: nil)
        let unconfigured = makeProducer(backend)
        _ = try await unconfigured.send(makeMessages(1))
        let partitionings = await backend.calls.compactMap { call -> Partitioning? in
            if case .send(_, _, let partitioning, _) = call { return partitioning }
            return nil
        }
        #expect(partitionings == [.partition(2), .partition(1), .partition(2), .balanced])
    }

    @Test func sendToAnotherDestination() async throws {
        let backend = await FakeBackend.prepared()
        await backend.addTopic(stream: "s", name: "other", partitions: 1)
        let producer = makeProducer(backend)
        _ = try await producer.send(makeMessages(2), to: "s", topicID: "other")
        #expect(await backend.messages(stream: "s", topic: "other", partition: 0).count == 2)
        #expect(await backend.messages(stream: "s", topic: "t", partition: 0).isEmpty)
    }

    @Test func failureReportsTheFailedTail() async throws {
        let backend = await FakeBackend.prepared()
        let producer = makeProducer(backend, ProducerConfiguration(sendRetries: nil, mode: .direct(DirectSendOptions(batchLength: 3))))
        await backend.failSends(1, with: IggyError(.disconnected))
        do {
            _ = try await producer.send(makeMessages(4))
            Issue.record("expected the send to fail")
        } catch let error as ProducerSendError {
            #expect(error.cause.code == .disconnected)
            #expect(error.failed.map(\.payloadString) == ["m0", "m1", "m2", "m3"])
            #expect(error.committed.isEmpty)
            #expect(error.streamID == "s" && error.topicID == "t")
            #expect(error.description.contains("4 unconfirmed"))
        }
    }

    @Test func midBatchFailureKeepsEarlierConfirmations() async throws {
        let backend = await FakeBackend.prepared()
        let producer = makeProducer(backend, ProducerConfiguration(sendRetries: 0, mode: .direct(DirectSendOptions(batchLength: 2))))
        // Chunk one succeeds, chunk two fails, so the tail is the last four.
        await backend.failSends(1, after: 1, with: IggyError(.error))
        do {
            _ = try await producer.send(makeMessages(6))
            Issue.record("expected the send to fail")
        } catch let error as ProducerSendError {
            #expect(error.committed.map(\.baseOffset) == [0])
            #expect(error.failed.map(\.payloadString) == ["m2", "m3", "m4", "m5"])
        }
    }

    @Test func retriesTransientFailures() async throws {
        let backend = await FakeBackend.prepared()
        let producer = makeProducer(backend, ProducerConfiguration(sendRetries: 3, sendRetryInterval: .milliseconds(1)))
        await backend.failSends(2, with: IggyError(.error))
        let response = try await producer.send(makeMessages(1))
        #expect(response.confirmations.count == 1)
        #expect(await backend.calls.filter { if case .send = $0 { return true } else { return false } }.count == 3)
    }

    @Test func retryBudgetIsExhausted() async throws {
        let backend = await FakeBackend.prepared()
        let producer = makeProducer(backend, ProducerConfiguration(sendRetries: 2, sendRetryInterval: nil))
        await backend.failSends(5, with: IggyError(.error))
        await #expect(throws: ProducerSendError.self) {
            try await producer.send(makeMessages(1))
        }
        #expect(await backend.calls.count == 3)
    }

    @Test func disconnectedClientBlocksTheSendUntilSignedIn() async throws {
        let backend = await FakeBackend.prepared()
        let producer = makeProducer(backend, ProducerConfiguration(sendRetries: 50, sendRetryInterval: .milliseconds(5)))
        try await producer.initialize()
        #expect(await eventually { await backend.subscriberCount == 1 })
        await backend.emit(.disconnected)
        #expect(await eventually { await !producer.core.canSend })
        let send = Task { try await producer.send(makeMessages(1)) }
        try await Task.sleep(for: .milliseconds(30))
        #expect(await backend.calls.filter { if case .send = $0 { return true } else { return false } }.isEmpty)
        await backend.emit(.signedIn)
        let response = try await send.value
        #expect(response.confirmations.count == 1)
        await producer.shutdown()
    }

    @Test func disconnectedClientExhaustsTheBudget() async throws {
        let backend = await FakeBackend.prepared()
        let producer = makeProducer(backend, ProducerConfiguration(sendRetries: 2, sendRetryInterval: .milliseconds(1)))
        try await producer.initialize()
        #expect(await eventually { await backend.subscriberCount == 1 })
        await backend.emit(.signedOut)
        #expect(await eventually { await !producer.core.canSend })
        do {
            _ = try await producer.send(makeMessages(1))
            Issue.record("expected the send to fail")
        } catch let error as ProducerSendError {
            #expect(error.cause.code == .cannotSendMessagesDueToClientDisconnection)
            #expect(error.failed.count == 1)
        }
        // Without a retry budget the gate is not consulted.
        let eager = makeProducer(backend, noRetries())
        _ = try await eager.send(makeMessages(1))
        await producer.shutdown()
    }

    @Test func lingerSpacesSequentialSends() async throws {
        let backend = await FakeBackend.prepared()
        let producer = makeProducer(backend, ProducerConfiguration(mode: .direct(DirectSendOptions(lingerTime: .milliseconds(40)))))
        let clock = ContinuousClock()
        _ = try await producer.send(makeMessages(1))
        let elapsed = await clock.measure {
            _ = try? await producer.send(makeMessages(1))
        }
        #expect(elapsed >= .milliseconds(35))
    }
}

@Suite("Background producer")
struct BackgroundProducerTests {
    @Test func flushesByBatchLength() async throws {
        let backend = await FakeBackend.prepared()
        let producer = makeProducer(backend, ProducerConfiguration(mode: .background(BackgroundSendOptions(batchLength: 3, lingerTime: .seconds(10)))))
        for _ in 0..<3 {
            let response = try await producer.send(makeMessages(1))
            #expect(response.confirmations.isEmpty)
        }
        #expect(await eventually { await backend.messages(stream: "s", topic: "t", partition: 0).count == 3 })
        // Three sends to one destination merge into one request.
        #expect(await backend.calls == [.send("s", "t", .balanced, 3)])
        await producer.shutdown()
    }

    @Test func flushesByBatchSize() async throws {
        let backend = await FakeBackend.prepared()
        let producer = makeProducer(
            backend, ProducerConfiguration(mode: .background(BackgroundSendOptions(batchSize: 200, batchLength: 0, lingerTime: .seconds(10)))))
        _ = try await producer.send([try IggyMessage(payload: [UInt8](repeating: 1, count: 100))])
        try await Task.sleep(for: .milliseconds(20))
        #expect(await backend.calls.isEmpty)
        _ = try await producer.send([try IggyMessage(payload: [UInt8](repeating: 1, count: 100))])
        #expect(await eventually { await backend.messages(stream: "s", topic: "t", partition: 0).count == 2 })
        await producer.shutdown()
    }

    @Test func flushesAfterLinger() async throws {
        let backend = await FakeBackend.prepared()
        let producer = makeProducer(backend, ProducerConfiguration(mode: .background(BackgroundSendOptions(batchLength: 0, lingerTime: .milliseconds(30)))))
        _ = try await producer.send(makeMessages(1))
        try await Task.sleep(for: .milliseconds(5))
        #expect(await backend.calls.isEmpty)
        #expect(await eventually { await backend.messages(stream: "s", topic: "t", partition: 0).count == 1 })
        await producer.shutdown()
    }

    @Test func zeroLingerFlushesAtOnce() async throws {
        let backend = await FakeBackend.prepared()
        let producer = makeProducer(backend, ProducerConfiguration(mode: .background(BackgroundSendOptions(batchLength: 0, lingerTime: .zero))))
        _ = try await producer.send(makeMessages(2))
        #expect(await eventually { await backend.messages(stream: "s", topic: "t", partition: 0).count == 2 })
        await producer.shutdown()
    }

    @Test func shutdownFlushesTheBuffer() async throws {
        let backend = await FakeBackend.prepared()
        let producer = makeProducer(backend, ProducerConfiguration(mode: .background(BackgroundSendOptions(batchLength: 0, lingerTime: .seconds(30)))))
        _ = try await producer.send(makeMessages(4))
        _ = try await producer.send(makeMessages(1))
        await producer.shutdown()
        #expect(await backend.messages(stream: "s", topic: "t", partition: 0).count == 5)
        await expectCode(.producerClosed) { try await producer.send(makeMessages(1)) }
    }

    @Test func differentDestinationsAreNotMerged() async throws {
        let backend = await FakeBackend.prepared(partitions: 2)
        let producer = makeProducer(backend, ProducerConfiguration(mode: .background(BackgroundSendOptions(batchLength: 0, lingerTime: .seconds(30)))))
        _ = try await producer.send(makeMessages(1), partitioning: .partition(0))
        _ = try await producer.send(makeMessages(1), partitioning: .partition(0))
        _ = try await producer.send(makeMessages(1), partitioning: .partition(1))
        _ = try await producer.send(makeMessages(1), partitioning: .partition(0))
        await producer.shutdown()
        #expect(await backend.calls == [.send("s", "t", .partition(0), 2), .send("s", "t", .partition(1), 1), .send("s", "t", .partition(0), 1)])
    }

    @Test func orderIsKeptPerDestination() async throws {
        let backend = await FakeBackend.prepared()
        let producer = makeProducer(
            backend, ProducerConfiguration(mode: .background(BackgroundSendOptions(workerCount: 4, batchLength: 5, lingerTime: .milliseconds(1)))))
        for index in 0..<200 {
            _ = try await producer.send([try IggyMessage("\(index)")])
        }
        await producer.shutdown()
        #expect(await backend.messages(stream: "s", topic: "t", partition: 0).map(\.payloadString) == (0..<200).map(String.init))
    }

    @Test func balancedShardingUsesEveryWorker() async throws {
        let backend = await FakeBackend.prepared()
        let seen = Locked<Set<Int>>([])
        let options = BackgroundSendOptions(
            workerCount: 3,
            sharding: .custom { count, _, _, _ in
                let index = seen.withLock { $0.count % count }
                seen.withLock { _ = $0.insert(index) }
                return index
            }, batchLength: 1, lingerTime: .zero)
        let producer = makeProducer(backend, ProducerConfiguration(mode: .background(options)))
        for _ in 0..<6 {
            _ = try await producer.send(makeMessages(1))
        }
        await producer.shutdown()
        #expect(seen.withLock { $0 } == [0, 1, 2])
        #expect(await backend.messages(stream: "s", topic: "t", partition: 0).count == 6)

        let balanced = makeProducer(
            backend, ProducerConfiguration(mode: .background(BackgroundSendOptions(workerCount: 2, sharding: .balanced, batchLength: 1, lingerTime: .zero))))
        for _ in 0..<4 {
            _ = try await balanced.send(makeMessages(1))
        }
        await balanced.shutdown()
        #expect(await backend.messages(stream: "s", topic: "t", partition: 0).count == 10)
    }

    @Test func oversizedBatchIsRefused() async throws {
        let backend = await FakeBackend.prepared()
        let producer = makeProducer(backend, ProducerConfiguration(mode: .background(BackgroundSendOptions(maxBufferSize: 100))))
        await expectCode(.backgroundSendBufferOverflow) {
            try await producer.send([try IggyMessage(payload: [UInt8](repeating: 0, count: 200))])
        }
        await producer.shutdown()
    }

    @Test func failImmediatelyWhenTheBudgetIsFull() async throws {
        let backend = await FakeBackend.prepared()
        await backend.setSendDelay(.milliseconds(200))
        let options = BackgroundSendOptions(batchLength: 1, lingerTime: .zero, maxBufferSize: 150, backpressure: .failImmediately)
        let producer = makeProducer(backend, ProducerConfiguration(sendRetries: nil, mode: .background(options)))
        _ = try await producer.send([try IggyMessage(payload: [UInt8](repeating: 0, count: 40))])
        await expectCode(.backgroundSendBufferOverflow) {
            try await producer.send([try IggyMessage(payload: [UInt8](repeating: 0, count: 40))])
        }
        await producer.shutdown()
        #expect(await backend.messages(stream: "s", topic: "t", partition: 0).count == 1)
    }

    @Test func blockWithTimeoutGivesUp() async throws {
        let backend = await FakeBackend.prepared()
        await backend.setSendDelay(.milliseconds(300))
        let options = BackgroundSendOptions(batchLength: 1, lingerTime: .zero, maxBufferSize: 150, backpressure: .blockWithTimeout(.milliseconds(20)))
        let producer = makeProducer(backend, ProducerConfiguration(sendRetries: nil, mode: .background(options)))
        _ = try await producer.send([try IggyMessage(payload: [UInt8](repeating: 0, count: 40))])
        await expectCode(.backgroundSendTimeout) {
            try await producer.send([try IggyMessage(payload: [UInt8](repeating: 0, count: 40))])
        }
        await producer.shutdown()
    }

    @Test func blockWaitsForCapacity() async throws {
        let backend = await FakeBackend.prepared()
        await backend.setSendDelay(.milliseconds(50))
        let options = BackgroundSendOptions(batchLength: 1, lingerTime: .zero, maxBufferSize: 150, backpressure: .block)
        let producer = makeProducer(backend, ProducerConfiguration(sendRetries: nil, mode: .background(options)))
        let clock = ContinuousClock()
        _ = try await producer.send([try IggyMessage(payload: [UInt8](repeating: 0, count: 40))])
        let elapsed = await clock.measure {
            _ = try? await producer.send([try IggyMessage(payload: [UInt8](repeating: 0, count: 40))])
        }
        #expect(elapsed >= .milliseconds(30))
        await producer.shutdown()
        #expect(await backend.messages(stream: "s", topic: "t", partition: 0).count == 2)
    }

    @Test func failuresReachTheErrorHandler() async throws {
        let backend = await FakeBackend.prepared()
        let failures = Locked<[ProducerSendFailure]>([])
        let options = BackgroundSendOptions(batchLength: 1, lingerTime: .zero) { failure in
            failures.withLock { $0.append(failure) }
        }
        let producer = makeProducer(backend, ProducerConfiguration(sendRetries: nil, mode: .background(options)))
        await backend.failSends(1, with: IggyError(.topicIdNotFound))
        _ = try await producer.send(makeMessages(2), partitioning: .partition(0))
        _ = try await producer.send(makeMessages(1))
        await producer.shutdown()
        let recorded = failures.withLock { $0 }
        #expect(recorded.count == 1)
        #expect(recorded.first?.cause.code == .topicIdNotFound)
        #expect(recorded.first?.messages.count == 2)
        #expect(recorded.first?.partitioning == .partition(0))
        #expect(recorded.first?.committed.isEmpty == true)
        #expect(await backend.messages(stream: "s", topic: "t", partition: 0).count == 1)
    }

    @Test func maxInFlightBoundsConcurrentWrites() async throws {
        let backend = await FakeBackend.prepared()
        await backend.setSendDelay(.milliseconds(30))
        let options = BackgroundSendOptions(workerCount: 4, sharding: .balanced, batchLength: 1, lingerTime: .zero, maxInFlight: 1)
        let producer = makeProducer(backend, ProducerConfiguration(sendRetries: nil, mode: .background(options)))
        let clock = ContinuousClock()
        let elapsed = await clock.measure {
            for _ in 0..<4 {
                _ = try? await producer.send(makeMessages(1))
            }
            await producer.shutdown()
        }
        // Four serialized 30 ms writes cannot finish in under 100 ms.
        #expect(elapsed >= .milliseconds(100))
        #expect(await backend.messages(stream: "s", topic: "t", partition: 0).count == 4)
    }

    @Test func mergeRespectsTheRequestSizeCap() {
        var first = QueuedBatch(streamID: "s", topicID: "t", messages: makeMessages(1), partitioning: nil)
        first.sizeBytes = Int(UInt32.max) - 10
        let second = QueuedBatch(streamID: "s", topicID: "t", messages: makeMessages(1), partitioning: nil)
        let third = QueuedBatch(streamID: "s", topicID: "t", messages: makeMessages(1), partitioning: .partition(1))
        let merged = ShardWorker.merge([first, second, third, third])
        #expect(merged.count == 3)
        #expect(merged[2].messages.count == 2)
    }
}

/// Asserts that `body` throws an ``IggyError`` with `code`.
func expectCode(_ code: IggyErrorCode, sourceLocation: SourceLocation = #_sourceLocation, _ body: () async throws -> Void) async {
    do {
        try await body()
        Issue.record("expected \(code) but nothing was thrown", sourceLocation: sourceLocation)
    } catch let error as IggyError {
        #expect(error.code == code, "expected \(code), got \(error)", sourceLocation: sourceLocation)
    } catch let error as ProducerSendError {
        #expect(error.cause.code == code, "expected \(code), got \(error)", sourceLocation: sourceLocation)
    } catch {
        Issue.record("expected \(code), got \(error)", sourceLocation: sourceLocation)
    }
}
