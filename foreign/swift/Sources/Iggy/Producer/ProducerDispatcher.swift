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

/// One queued send, charged against the byte budget until its write returns.
struct QueuedBatch: Sendable {
    /// What the Rust SDK charges per message on top of its payload and user
    /// headers (`IGGY_MESSAGE_HEADER_SIZE`), so a budget holds the same
    /// messages in both SDKs.
    static let messageHeaderSize = 64

    let streamID: Identifier
    let topicID: Identifier
    var messages: [IggyMessage]
    let partitioning: Partitioning?
    var sizeBytes: Int

    init(streamID: Identifier, topicID: Identifier, messages: [IggyMessage], partitioning: Partitioning?) {
        self.streamID = streamID
        self.topicID = topicID
        self.messages = messages
        self.partitioning = partitioning
        var size = streamID.encodedSize + topicID.encodedSize
        size += partitioning?.encodedSize ?? 0
        for message in messages {
            size += Self.messageHeaderSize + message.payload.count
            size += message.rawUserHeaders?.count ?? 0
        }
        sizeBytes = size
    }

    func sameDestination(as other: QueuedBatch) -> Bool {
        streamID == other.streamID && topicID == other.topicID && partitioning == other.partitioning
    }
}

/// The background machinery of an ``IggyProducer``, a port of
/// `ProducerDispatcher` in the Rust SDK. It routes each batch to a worker,
/// enforces the byte budget, and runs the error handler on its own task so a
/// slow handler never stalls a worker.
final class ProducerDispatcher: Sendable {
    private let options: BackgroundSendOptions
    private let workers: [ShardWorker]
    private let byteBudget: AsyncSemaphore?
    private let failures: AsyncStream<ProducerSendFailure>.Continuation
    private let errorTask: Task<Void, Never>
    private let state = NIOLockedValueBox(State())

    private struct State {
        var closed = false
        var nextWorker = 0
    }

    init(backend: any ProducerCoreBackend, options: BackgroundSendOptions, logger: Logger) {
        self.options = options
        let byteBudget = options.maxBufferSize > 0 ? AsyncSemaphore(permits: options.maxBufferSize) : nil
        self.byteBudget = byteBudget
        let inFlight = options.maxInFlight > 0 ? AsyncSemaphore(permits: options.maxInFlight) : nil
        let (stream, continuation) = AsyncStream<ProducerSendFailure>.makeStream()
        failures = continuation
        let handler = options.errorHandler ?? Self.logFailure(logger)
        errorTask = Task {
            for await failure in stream {
                await handler(failure)
            }
        }
        workers = (0..<max(options.workerCount, 1)).map { _ in
            ShardWorker(backend: backend, options: options, inFlight: inFlight, byteBudget: byteBudget, failures: continuation, logger: logger)
        }
    }

    /// Charges the batch against the byte budget and queues it on a worker.
    /// Returns once it is queued, not once it is written.
    func dispatch(_ messages: [IggyMessage], streamID: Identifier, topicID: Identifier, partitioning: Partitioning?) async throws {
        guard !state.withLockedValue({ $0.closed }) else {
            throw IggyError(.producerClosed)
        }
        let batch = QueuedBatch(streamID: streamID, topicID: topicID, messages: messages, partitioning: partitioning)
        // A merged request is at most this large, so no single batch may be.
        guard batch.sizeBytes <= UInt32.max else {
            throw IggyError(.backgroundSendBufferOverflow, context: "a batch of \(batch.sizeBytes) bytes exceeds a request")
        }
        if let byteBudget {
            guard batch.sizeBytes <= options.maxBufferSize else {
                throw IggyError(
                    .backgroundSendBufferOverflow, context: "a batch of \(batch.sizeBytes) bytes exceeds the \(options.maxBufferSize) byte budget")
            }
            if !byteBudget.tryAcquire(batch.sizeBytes) {
                try await waitForBudget(byteBudget, bytes: batch.sizeBytes)
            }
        }
        let worker = workers[pickWorker(for: batch)]
        do {
            try await worker.enqueue(batch)
        } catch {
            byteBudget?.release(batch.sizeBytes)
            throw error
        }
    }

    /// Stops taking sends, has every worker write what it holds, and waits
    /// for the writes and the error handler.
    func shutdown() async {
        let wasClosed = state.withLockedValue { state in
            defer { state.closed = true }
            return state.closed
        }
        guard !wasClosed else {
            return
        }
        for worker in workers {
            await worker.stop()
        }
        failures.finish()
        await errorTask.value
    }

    private func waitForBudget(_ byteBudget: AsyncSemaphore, bytes: Int) async throws {
        switch options.backpressure {
        case .failImmediately:
            throw IggyError(.backgroundSendBufferOverflow, context: "the byte budget is used up")
        case .block:
            do {
                _ = try await byteBudget.acquire(bytes)
            } catch {
                throw IggyError(.backgroundSendError, context: "the wait for buffer capacity was cancelled")
            }
        case .blockWithTimeout(let timeout):
            let acquired: Bool
            do {
                acquired = try await byteBudget.acquire(bytes, deadline: .now + timeout)
            } catch {
                throw IggyError(.backgroundSendError, context: "the wait for buffer capacity was cancelled")
            }
            guard acquired else {
                throw IggyError(.backgroundSendTimeout, context: "no buffer capacity within \(timeout)")
            }
        }
    }

    func pickWorker(for batch: QueuedBatch) -> Int {
        let count = workers.count
        switch options.sharding {
        case .ordered:
            var hasher = Hasher()
            hasher.combine(batch.streamID)
            hasher.combine(batch.topicID)
            return Int(UInt(bitPattern: hasher.finalize()) % UInt(count))
        case .balanced:
            return state.withLockedValue { state in
                defer { state.nextWorker = (state.nextWorker + 1) % count }
                return state.nextWorker
            }
        case .custom(let pick):
            let index = pick(count, batch.messages, batch.streamID, batch.topicID)
            precondition(index >= 0 && index < count, "the sharding strategy picked worker \(index) of \(count)")
            return index
        }
    }

    private static func logFailure(_ logger: Logger) -> @Sendable (ProducerSendFailure) async -> Void {
        { failure in
            logger.error(
                "Failed to send messages in the background",
                metadata: [
                    "cause": "\(failure.cause)", "stream": "\(failure.streamID)", "topic": "\(failure.topicID)",
                    "partitioning": "\(failure.partitioning.map { "\($0)" } ?? "none")", "messages": "\(failure.messages.count)",
                    "committed_confirmations": "\(failure.committed.count)",
                ])
        }
    }
}

/// A worker with a queue of its own, a port of `Shard` in the Rust SDK. It
/// buffers the sends routed to it and writes them one request at a time, so
/// the order of each destination is kept.
actor ShardWorker {
    /// Sends handed over but not picked up yet. The worker does not pick up
    /// while it writes, so a slow server makes a dispatch wait here.
    static let queueCapacity = 256

    private let backend: any ProducerCoreBackend
    private let options: BackgroundSendOptions
    private let inFlight: AsyncSemaphore?
    private let byteBudget: AsyncSemaphore?
    private let failures: AsyncStream<ProducerSendFailure>.Continuation
    private let logger: Logger
    private let queueSlots = AsyncSemaphore(permits: ShardWorker.queueCapacity)

    private var queue: [QueuedBatch] = []
    private var buffer: [QueuedBatch] = []
    private var bufferBytes = 0
    /// Armed by the first send buffered after a flush, so an idle worker never
    /// wakes up and a zero linger cannot spin.
    private var lingerDeadline: ContinuousClock.Instant?
    private var lingerTimer: Task<Void, Never>?
    private var wakeup: CheckedContinuation<Void, Never>?
    private var stopping = false
    private var closed = false
    private var loop: Task<Void, Never>?

    init(
        backend: any ProducerCoreBackend, options: BackgroundSendOptions, inFlight: AsyncSemaphore?, byteBudget: AsyncSemaphore?,
        failures: AsyncStream<ProducerSendFailure>.Continuation, logger: Logger
    ) {
        self.backend = backend
        self.options = options
        self.inFlight = inFlight
        self.byteBudget = byteBudget
        self.failures = failures
        self.logger = logger
    }

    func enqueue(_ batch: QueuedBatch) async throws {
        guard !closed else {
            throw IggyError(.producerClosed)
        }
        if loop == nil {
            loop = Task { await self.run() }
        }
        do {
            _ = try await queueSlots.acquire(1)
        } catch {
            throw IggyError(.backgroundSendError, context: "the wait for a queue slot was cancelled")
        }
        guard !closed else {
            // The worker stopped while this waited for a slot, as a channel
            // whose receiver is gone.
            queueSlots.release(1)
            throw IggyError(.backgroundSendError, context: "the worker stopped before the batch was queued")
        }
        queue.append(batch)
        wake()
    }

    /// Writes everything queued or buffered, then ends the worker.
    func stop() async {
        stopping = true
        guard let loop else {
            closed = true
            return
        }
        wake()
        await loop.value
    }

    private func run() async {
        while true {
            if stopping {
                closed = true
                while !queue.isEmpty {
                    pickUp(queue.removeFirst())
                }
                await flush()
                return
            }
            if !queue.isEmpty {
                pickUp(queue.removeFirst())
                let exceedsLength = options.batchLength > 0 && buffer.count >= options.batchLength
                let exceedsSize = options.batchSize > 0 && bufferBytes >= options.batchSize
                if exceedsLength || exceedsSize {
                    await flush()
                }
                continue
            }
            if !buffer.isEmpty, let lingerDeadline, ContinuousClock.now >= lingerDeadline {
                await flush()
                continue
            }
            await withCheckedContinuation { wakeup = $0 }
        }
    }

    private func pickUp(_ batch: QueuedBatch) {
        queueSlots.release(1)
        if buffer.isEmpty {
            let deadline = ContinuousClock.now + options.lingerTime
            lingerDeadline = deadline
            lingerTimer = Task { [weak self] in
                guard (try? await Task.sleep(until: deadline, clock: .continuous)) != nil else {
                    return
                }
                await self?.wake()
            }
        }
        bufferBytes += batch.sizeBytes
        buffer.append(batch)
    }

    private func flush() async {
        lingerTimer?.cancel()
        lingerTimer = nil
        lingerDeadline = nil
        guard !buffer.isEmpty else {
            return
        }
        let merged = Self.merge(buffer)
        buffer.removeAll()
        bufferBytes = 0
        for batch in merged {
            // The worker owns the batch until its write returned, so a
            // cancelled wait for a slot still writes it.
            var holdsSlot = false
            if let inFlight {
                holdsSlot = (try? await inFlight.acquire(1)) == true
            }
            do {
                _ = try await backend.sendInternal(
                    streamID: batch.streamID, topicID: batch.topicID, messages: batch.messages, partitioning: batch.partitioning)
            } catch let error as ProducerSendError {
                failures.yield(
                    ProducerSendFailure(
                        cause: error.cause, streamID: batch.streamID, topicID: batch.topicID, partitioning: batch.partitioning,
                        messages: error.failed, committed: error.committed))
            } catch {
                logger.error("Background send failed", metadata: ["error": "\(error)"])
            }
            if holdsSlot {
                inFlight?.release(1)
            }
            byteBudget?.release(batch.sizeBytes)
        }
    }

    /// Adjacent sends to one destination become one request while the merged
    /// size still fits a request.
    static func merge(_ batches: [QueuedBatch]) -> [QueuedBatch] {
        var merged: [QueuedBatch] = []
        merged.reserveCapacity(batches.count)
        for batch in batches {
            if var last = merged.last, last.sameDestination(as: batch), last.sizeBytes + batch.sizeBytes <= UInt32.max {
                last.messages.append(contentsOf: batch.messages)
                last.sizeBytes += batch.sizeBytes
                merged[merged.count - 1] = last
                continue
            }
            merged.append(batch)
        }
        return merged
    }

    private func wake() {
        let waiter = wakeup
        wakeup = nil
        waiter?.resume()
    }
}
