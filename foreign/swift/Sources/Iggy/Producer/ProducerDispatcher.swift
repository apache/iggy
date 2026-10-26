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

    /// The most a batch may be charged and still fit one request of
    /// ``VSRFrame/maxRequestSize``. The charge counts 64 bytes per message
    /// where the wire takes 48, which covers the explicit partition the
    /// client always sends.
    static let maxRequestBytes = VSRFrame.maxRequestSize - VSRFrame.headerSize - Batch.headerSize - 8

    let streamID: Identifier
    let topicID: Identifier
    var messages: [IggyMessage]
    let partitioning: Partitioning?
    /// The client lineage the send started in, which its write keeps.
    let lineage: UInt64
    var sizeBytes: Int
    /// The span of origin timestamps, in microseconds, which one request
    /// may not stretch past ``Batch/maxTimestampDelta``.
    var earliestTimestamp: UInt64
    var latestTimestamp: UInt64

    init(streamID: Identifier, topicID: Identifier, messages: [IggyMessage], partitioning: Partitioning?, lineage: UInt64 = 0) {
        self.streamID = streamID
        self.topicID = topicID
        self.messages = messages
        self.partitioning = partitioning
        self.lineage = lineage
        var size = streamID.encodedSize + topicID.encodedSize
        size += partitioning?.encodedSize ?? 0
        var earliest = UInt64.max
        var latest = UInt64.min
        for message in messages {
            size += Self.messageHeaderSize + message.payload.count
            size += message.rawUserHeaders?.count ?? 0
            earliest = min(earliest, message.originTimestamp.microseconds)
            latest = max(latest, message.originTimestamp.microseconds)
        }
        sizeBytes = size
        earliestTimestamp = earliest
        latestTimestamp = latest
    }

    func sameDestination(as other: QueuedBatch) -> Bool {
        streamID == other.streamID && topicID == other.topicID && partitioning == other.partitioning
    }

    /// Whether `other` can join this batch in one request: the same
    /// destination and lineage, within the request size, and within the
    /// timestamp span the client accepts.
    func canMerge(_ other: QueuedBatch) -> Bool {
        guard sameDestination(as: other), lineage == other.lineage, sizeBytes + other.sizeBytes <= Self.maxRequestBytes else {
            return false
        }
        let latest = max(latestTimestamp, other.latestTimestamp)
        let earliest = min(earliestTimestamp, other.earliestTimestamp)
        return latest < earliest || latest - earliest <= Batch.maxTimestampDelta
    }
}

/// The background machinery of an ``IggyProducer``, a port of
/// `ProducerDispatcher` in the Rust SDK. It routes each batch to a worker,
/// enforces the byte budget, and runs the error handler on its own task so a
/// slow handler never stalls a worker.
final class ProducerDispatcher: Sendable {
    private let options: BackgroundSendOptions
    let workers: [ShardWorker]
    let byteBudget: AsyncSemaphore?
    private let failures: AsyncStream<ProducerSendFailure>.Continuation
    private let errorTask: Task<Void, Never>
    private let state = NIOLockedValueBox(State())

    private struct State {
        /// Set once the dispatcher closes; every shutdown waits for it.
        var drain: Task<Void, Never>?
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
        workers = (0..<options.workerCount).map { _ in
            ShardWorker(backend: backend, options: options, inFlight: inFlight, byteBudget: byteBudget, failures: continuation, logger: logger)
        }
    }

    /// The workers' loops keep them alive, so a producer released without a
    /// shutdown has them write what they hold and end. An error handler that
    /// holds the producer strongly keeps this dispatcher alive too, and then
    /// only a shutdown ends them.
    deinit {
        state.withLockedValue { state in
            if state.drain == nil {
                state.drain = Self.drain(workers: workers, failures: failures, errorTask: errorTask)
            }
        }
    }

    /// Charges the batch against the byte budget and queues it on a worker.
    /// Returns once it is queued, not once it is written.
    func dispatch(_ messages: [IggyMessage], streamID: Identifier, topicID: Identifier, partitioning: Partitioning?, lineage: UInt64 = 0) async throws {
        guard state.withLockedValue({ $0.drain == nil }) else {
            throw IggyError(.producerClosed)
        }
        let batch = QueuedBatch(streamID: streamID, topicID: topicID, messages: messages, partitioning: partitioning, lineage: lineage)
        // A merged request is at most this large, so no single batch may be.
        guard batch.sizeBytes <= QueuedBatch.maxRequestBytes else {
            throw IggyError(.backgroundSendBufferOverflow, context: "a batch of \(batch.sizeBytes) bytes exceeds a request")
        }
        let worker = workers[try pickWorker(for: batch)]
        // One deadline covers the budget and the queue slot.
        var deadline: ContinuousClock.Instant?
        if case .blockWithTimeout(let timeout) = options.backpressure {
            deadline = .now + timeout
        }
        if let byteBudget {
            guard batch.sizeBytes <= options.maxBufferSize else {
                throw IggyError(
                    .backgroundSendBufferOverflow, context: "a batch of \(batch.sizeBytes) bytes exceeds the \(options.maxBufferSize) byte budget")
            }
            if !byteBudget.tryAcquire(batch.sizeBytes) {
                try await waitForBudget(byteBudget, bytes: batch.sizeBytes, deadline: deadline)
            }
        }
        do {
            try await worker.enqueue(batch, deadline: deadline)
        } catch {
            byteBudget?.release(batch.sizeBytes)
            throw error
        }
    }

    /// Stops taking sends, has every worker write what it holds, and waits
    /// for the writes and the error handler. A concurrent call waits for the
    /// same drain.
    func shutdown() async {
        let drain = state.withLockedValue { state in
            if let drain = state.drain {
                return drain
            }
            let drain = Self.drain(workers: workers, failures: failures, errorTask: errorTask)
            state.drain = drain
            return drain
        }
        await drain.value
    }

    private static func drain(
        workers: [ShardWorker], failures: AsyncStream<ProducerSendFailure>.Continuation, errorTask: Task<Void, Never>
    ) -> Task<Void, Never> {
        Task {
            for worker in workers {
                await worker.stop()
            }
            failures.finish()
            await errorTask.value
        }
    }

    /// A cancelled wait leaves nothing queued, so it is reported as the
    /// cancellation it is.
    private func waitForBudget(_ byteBudget: AsyncSemaphore, bytes: Int, deadline: ContinuousClock.Instant?) async throws {
        switch options.backpressure {
        case .failImmediately:
            throw IggyError(.backgroundSendBufferOverflow, context: "the byte budget is used up")
        case .block, .blockWithTimeout:
            guard try await byteBudget.acquire(bytes, deadline: deadline) else {
                throw IggyError(.backgroundSendTimeout, context: "no buffer capacity within the backpressure timeout")
            }
        }
    }

    func pickWorker(for batch: QueuedBatch) throws -> Int {
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
            guard index >= 0, index < count else {
                throw IggyError(.backgroundSendError, context: "the sharding strategy picked worker \(index) of \(count)")
            }
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
    /// The queue's previous storage, swapped in so picking up never
    /// allocates.
    private var pickedUp: [QueuedBatch] = []
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

    /// Queues `batch`. A full queue is met the way `backpressure` says, with
    /// `deadline` shared with the wait for the byte budget.
    func enqueue(_ batch: QueuedBatch, deadline: ContinuousClock.Instant? = nil) async throws {
        guard !closed else {
            throw IggyError(.producerClosed)
        }
        if loop == nil {
            loop = Task { await self.run() }
        }
        if !queueSlots.tryAcquire(1) {
            switch options.backpressure {
            case .failImmediately:
                throw IggyError(.backgroundSendBufferOverflow, context: "the queue of the worker is full")
            case .block, .blockWithTimeout:
                guard try await queueSlots.acquire(1, deadline: deadline) else {
                    throw IggyError(.backgroundSendTimeout, context: "no queue slot within the backpressure timeout")
                }
            }
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
                swap(&queue, &pickedUp)
                for batch in pickedUp {
                    pickUp(batch)
                }
                pickedUp.removeAll(keepingCapacity: true)
                await flush()
                return
            }
            if !queue.isEmpty {
                // Sends that arrive while these are picked up wait for the
                // next round, behind them.
                swap(&queue, &pickedUp)
                for batch in pickedUp {
                    pickUp(batch)
                    let exceedsLength = options.batchLength > 0 && buffer.count >= options.batchLength
                    let exceedsSize = options.batchSize > 0 && bufferBytes >= options.batchSize
                    if exceedsLength || exceedsSize {
                        await flush()
                    }
                }
                pickedUp.removeAll(keepingCapacity: true)
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
            // A zero linger is due as soon as the queue is drained, which the
            // loop checks without a timer.
            if options.lingerTime > .zero {
                lingerTimer = Task { [weak self] in
                    guard (try? await Task.sleep(until: deadline, clock: .continuous)) != nil else {
                        return
                    }
                    await self?.wake()
                }
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
        buffer.removeAll(keepingCapacity: true)
        bufferBytes = 0
        for batch in merged {
            // The worker owns the batch until its write returned, so a
            // cancelled wait for a slot still writes it.
            var holdsSlot = false
            if let inFlight {
                holdsSlot = inFlight.tryAcquire(1)
                if !holdsSlot {
                    holdsSlot = (try? await inFlight.acquire(1)) == true
                }
            }
            do {
                _ = try await backend.sendInternal(
                    streamID: batch.streamID, topicID: batch.topicID, messages: batch.messages, partitioning: batch.partitioning, lineage: batch.lineage)
            } catch let error as ProducerSendError {
                failures.yield(
                    ProducerSendFailure(
                        cause: error.cause, underlying: error.underlying, streamID: batch.streamID, topicID: batch.topicID,
                        partitioning: batch.partitioning, messages: error.failed, committed: error.committed))
            } catch {
                failures.yield(
                    ProducerSendFailure(
                        cause: (error as? IggyError) ?? IggyError(.error, context: "\(error)"), underlying: error is IggyError ? nil : error,
                        streamID: batch.streamID, topicID: batch.topicID, partitioning: batch.partitioning, messages: batch.messages, committed: []))
            }
            if holdsSlot {
                inFlight?.release(1)
            }
            byteBudget?.release(batch.sizeBytes)
        }
    }

    /// Adjacent sends to one destination become one request while it still
    /// fits a request and the client's timestamp span.
    static func merge(_ batches: [QueuedBatch]) -> [QueuedBatch] {
        var merged: [QueuedBatch] = []
        merged.reserveCapacity(batches.count)
        for batch in batches {
            if let last = merged.indices.last, merged[last].canMerge(batch) {
                // In place, so the growing array is not copied per send.
                merged[last].messages.append(contentsOf: batch.messages)
                merged[last].sizeBytes += batch.sizeBytes
                merged[last].earliestTimestamp = min(merged[last].earliestTimestamp, batch.earliestTimestamp)
                merged[last].latestTimestamp = max(merged[last].latestTimestamp, batch.latestTimestamp)
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
