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

/// A backend that records what the dispatcher writes, optionally slowly or
/// failing.
private actor RecordingBackend: ProducerCoreBackend {
    struct Write: Sendable {
        let streamID: Identifier
        let topicID: Identifier
        let partitioning: Partitioning?
        let payloads: [String]
    }

    private(set) var writes: [Write] = []
    private(set) var mostInFlight = 0
    private var inFlight = 0
    private let delay: Duration
    private let failure: (@Sendable ([IggyMessage]) -> (any Error)?)?

    init(delay: Duration = .zero, failure: (@Sendable ([IggyMessage]) -> (any Error)?)? = nil) {
        self.delay = delay
        self.failure = failure
    }

    func sendInternal(streamID: Identifier, topicID: Identifier, messages: [IggyMessage], partitioning: Partitioning?) async throws -> SendMessagesResponse {
        inFlight += 1
        mostInFlight = max(mostInFlight, inFlight)
        defer { inFlight -= 1 }
        if delay > .zero {
            try? await Task.sleep(for: delay)
        }
        writes.append(Write(streamID: streamID, topicID: topicID, partitioning: partitioning, payloads: messages.map(\.payloadString)))
        if let error = failure?(messages) {
            throw error
        }
        return SendMessagesResponse()
    }
}

private func makeDispatcher(_ backend: RecordingBackend, _ options: BackgroundSendOptions) -> ProducerDispatcher {
    ProducerDispatcher(backend: backend, options: options, logger: testLogger)
}

private func message(_ text: String) -> IggyMessage {
    IggyMessage(stringLiteral: text)
}

/// A message whose batch to stream 1 and topic 2 is `bytes` long in the byte budget.
private func sized(_ bytes: Int) throws -> IggyMessage {
    try IggyMessage(payload: [UInt8](repeating: 7, count: bytes - 2 * 6 - QueuedBatch.messageHeaderSize))
}

@Suite("Producer dispatcher", .serialized)
struct ProducerDispatcherTests {
    @Test func aBatchIsChargedForItsDestinationAndMessages() throws {
        let batch = QueuedBatch(
            streamID: 1, topicID: "orders", messages: ["ab", try IggyMessage("c", userHeaders: ["k": .bool(true)])], partitioning: .partition(3))
        let headers = try #require(batch.messages[1].rawUserHeaders).count
        #expect(batch.sizeBytes == 6 + 8 + 6 + (64 + 2) + (64 + 1 + headers))
        #expect(try QueuedBatch(streamID: 1, topicID: 2, messages: [sized(300)], partitioning: nil).sizeBytes == 300)
    }

    @Test func aWorkerFlushesAtItsBatchLength() async throws {
        let backend = RecordingBackend()
        let dispatcher = makeDispatcher(backend, BackgroundSendOptions(batchSize: 0, batchLength: 2, lingerTime: .seconds(60)))
        try await dispatcher.dispatch(["a"], streamID: 1, topicID: 2, partitioning: nil)
        try await Task.sleep(for: .milliseconds(50))
        #expect(await backend.writes.isEmpty)
        try await dispatcher.dispatch(["b"], streamID: 1, topicID: 2, partitioning: nil)
        #expect(await eventually { await backend.writes.map(\.payloads) == [["a", "b"]] })
        await dispatcher.shutdown()
    }

    @Test func aWorkerFlushesAtItsBatchSize() async throws {
        let backend = RecordingBackend()
        let dispatcher = makeDispatcher(backend, BackgroundSendOptions(batchSize: 500, batchLength: 0, lingerTime: .seconds(60)))
        try await dispatcher.dispatch([sized(300)], streamID: 1, topicID: 2, partitioning: nil)
        try await Task.sleep(for: .milliseconds(50))
        #expect(await backend.writes.isEmpty)
        try await dispatcher.dispatch([sized(300)], streamID: 1, topicID: 2, partitioning: nil)
        #expect(await eventually { await backend.writes.map(\.payloads.count) == [2] })
        await dispatcher.shutdown()
    }

    @Test func aWorkerFlushesOnceItsLingerPassed() async throws {
        let backend = RecordingBackend()
        let dispatcher = makeDispatcher(backend, BackgroundSendOptions(batchSize: 0, batchLength: 0, lingerTime: .milliseconds(150)))
        let clock = ContinuousClock()
        let start = clock.now
        try await dispatcher.dispatch(["a"], streamID: 1, topicID: 2, partitioning: nil)
        #expect(await eventually { await backend.writes.count == 1 })
        #expect(clock.now - start >= .milliseconds(140))
        let zero = makeDispatcher(backend, BackgroundSendOptions(batchSize: 0, batchLength: 0, lingerTime: .zero))
        try await zero.dispatch(["b"], streamID: 1, topicID: 2, partitioning: nil)
        #expect(await eventually(timeout: .seconds(1)) { await backend.writes.count == 2 })
        await dispatcher.shutdown()
        await zero.shutdown()
    }

    @Test func adjacentSendsToOneDestinationAreMerged() async throws {
        let backend = RecordingBackend()
        let dispatcher = makeDispatcher(backend, BackgroundSendOptions(batchSize: 0, batchLength: 0, lingerTime: .seconds(60)))
        for (text, partition) in [("a", 1), ("b", 1), ("c", 2), ("d", 1)] {
            try await dispatcher.dispatch([message(text)], streamID: 1, topicID: 2, partitioning: .partition(UInt32(partition)))
        }
        try await dispatcher.dispatch(["e"], streamID: 1, topicID: 3, partitioning: .partition(1))
        await dispatcher.shutdown()
        #expect(await backend.writes.map(\.payloads) == [["a", "b"], ["c"], ["d"], ["e"]])
        #expect(ShardWorker.merge([]).isEmpty)
    }

    @Test func theByteBudgetBoundsWhatIsQueuedOrInFlight() async throws {
        let backend = RecordingBackend(delay: .milliseconds(300))
        let options = BackgroundSendOptions(batchLength: 1, lingerTime: .zero, maxBufferSize: 1000, backpressure: .failImmediately)
        let dispatcher = makeDispatcher(backend, options)
        await expectCode(.backgroundSendBufferOverflow) { try await dispatcher.dispatch([sized(1001)], streamID: 1, topicID: 2, partitioning: nil) }
        try await dispatcher.dispatch([sized(600)], streamID: 1, topicID: 2, partitioning: nil)
        // The first batch is charged until its slow write returned.
        await expectCode(.backgroundSendBufferOverflow) { try await dispatcher.dispatch([sized(600)], streamID: 1, topicID: 2, partitioning: nil) }
        #expect(await eventually { await backend.writes.count == 1 })
        try await dispatcher.dispatch([sized(600)], streamID: 1, topicID: 2, partitioning: nil)
        await dispatcher.shutdown()
        #expect(await backend.writes.count == 2)
    }

    @Test func aBlockedSendWaitsForTheBudgetOrItsTimeout() async throws {
        let backend = RecordingBackend(delay: .milliseconds(300))
        let timed = makeDispatcher(
            backend, BackgroundSendOptions(batchLength: 1, lingerTime: .zero, maxBufferSize: 1000, backpressure: .blockWithTimeout(.milliseconds(50))))
        try await timed.dispatch([sized(600)], streamID: 1, topicID: 2, partitioning: nil)
        await expectCode(.backgroundSendTimeout) { try await timed.dispatch([sized(600)], streamID: 1, topicID: 2, partitioning: nil) }
        await timed.shutdown()

        let blocking = makeDispatcher(backend, BackgroundSendOptions(batchLength: 1, lingerTime: .zero, maxBufferSize: 1000, backpressure: .block))
        try await blocking.dispatch([sized(600)], streamID: 1, topicID: 2, partitioning: nil)
        let clock = ContinuousClock()
        let waited = try await clock.measure { try await blocking.dispatch([sized(600)], streamID: 1, topicID: 2, partitioning: nil) }
        #expect(waited >= .milliseconds(200))
        await blocking.shutdown()
        #expect(await backend.writes.count == 3)
    }

    /// Failures reach the handler with the unconfirmed tail; the final flush
    /// of a shutdown reports its failures before the shutdown returns.
    @Test func writeFailuresReachTheErrorHandler() async throws {
        let failure = ProducerSendError(cause: IggyError(.transientNotCommitted), failed: ["b"], committed: [], streamID: 1, topicID: 2)
        let backend = RecordingBackend { messages in
            messages.first?.payloadString == "fails" ? failure : messages.first?.payloadString == "breaks" ? IggyError(.invalidCommand) : nil
        }
        let reported = Locked<[ProducerSendFailure]>([])
        let options = BackgroundSendOptions(lingerTime: .seconds(60), errorHandler: { failure in reported.withLock { $0.append(failure) } })
        let dispatcher = makeDispatcher(backend, options)
        try await dispatcher.dispatch(["fails"], streamID: 1, topicID: 2, partitioning: .partition(4))
        try await dispatcher.dispatch(["breaks"], streamID: 1, topicID: 3, partitioning: nil)
        await dispatcher.shutdown()
        let failures = reported.withLock { $0 }
        #expect(failures.count == 1)
        #expect(failures.first?.cause.code == .transientNotCommitted)
        #expect(failures.first?.messages.map(\.payloadString) == ["b"])
        #expect(failures.first?.partitioning == .partition(4))
        await expectCode(.producerClosed) { try await dispatcher.dispatch(["late"], streamID: 1, topicID: 2, partitioning: nil) }
    }

    @Test func shardingPicksTheWorker() async throws {
        let backend = RecordingBackend()
        let balanced = makeDispatcher(backend, BackgroundSendOptions(workerCount: 3, sharding: .balanced))
        let batch = QueuedBatch(streamID: 1, topicID: 2, messages: ["a"], partitioning: nil)
        #expect((0..<4).map { _ in balanced.pickWorker(for: batch) } == [0, 1, 2, 0])
        let ordered = makeDispatcher(backend, BackgroundSendOptions(workerCount: 3, sharding: .ordered))
        #expect(Set((0..<4).map { _ in ordered.pickWorker(for: batch) }).count == 1)
        let custom = makeDispatcher(
            backend, BackgroundSendOptions(workerCount: 3, sharding: .custom({ count, messages, _, _ in (count - 1) * messages.count % count })))
        #expect(custom.pickWorker(for: batch) == 2)
        for dispatcher in [balanced, ordered, custom] {
            await dispatcher.shutdown()
        }
    }

    @Test func inFlightWritesAreBoundedAcrossWorkers() async throws {
        for (limit, expected) in [(1, 1), (2, 2)] {
            let backend = RecordingBackend(delay: .milliseconds(100))
            let options = BackgroundSendOptions(workerCount: 2, sharding: .balanced, batchLength: 1, lingerTime: .zero, maxInFlight: limit)
            let dispatcher = makeDispatcher(backend, options)
            for index in 0..<4 {
                try await dispatcher.dispatch([message("\(index)")], streamID: 1, topicID: 2, partitioning: nil)
            }
            await dispatcher.shutdown()
            #expect(await backend.writes.count == 4)
            #expect(await backend.mostInFlight == expected)
        }
    }
}
