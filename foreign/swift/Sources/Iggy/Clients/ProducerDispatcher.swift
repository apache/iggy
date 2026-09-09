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

/// One queued send, charged against the byte budget until its write returns.
struct QueuedBatch: Sendable {
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
        sizeBytes = Self.destinationSize(streamID) + Self.destinationSize(topicID) + messages.reduce(0) { $0 + $1.sizeBytes }
    }

    private static func destinationSize(_ identifier: Identifier) -> Int {
        2 + (identifier.name?.utf8.count ?? 4)
    }

    func sameDestination(as other: QueuedBatch) -> Bool {
        streamID == other.streamID && topicID == other.topicID && partitioning == other.partitioning
    }
}

/// The background machinery of an ``IggyProducer``: routes each batch to a
/// worker, enforces the byte budget and in-flight bound, and runs the error
/// handler on its own task so a slow handler never stalls batching.
actor ProducerDispatcher {
    private let core: ProducerCore
    private let options: BackgroundSendOptions
    private let workers: [ShardWorker]
    private let byteBudget: AsyncSemaphore?
    private let failures: AsyncStream<ProducerSendFailure>.Continuation
    private let errorTask: Task<Void, Never>
    private var closed = false
    private var roundRobin = 0

    init(core: ProducerCore, options: BackgroundSendOptions, logger: Logger) {
        self.core = core
        self.options = options
        byteBudget = options.maxBufferSize > 0 ? AsyncSemaphore(permits: options.maxBufferSize) : nil
        let inFlight = options.maxInFlight > 0 ? AsyncSemaphore(permits: options.maxInFlight) : nil
        let (stream, continuation) = AsyncStream<ProducerSendFailure>.makeStream()
        failures = continuation
        let handler = options.errorHandler ?? Self.logFailure(logger)
        errorTask = Task {
            for await failure in stream {
                await handler(failure)
            }
        }
        var workers: [ShardWorker] = []
        for _ in 0..<max(options.workerCount, 1) {
            workers.append(ShardWorker(core: core, options: options, inFlight: inFlight, byteBudget: byteBudget, failures: continuation, logger: logger))
        }
        self.workers = workers
    }

    func dispatch(streamID: Identifier, topicID: Identifier, messages: [IggyMessage], partitioning: Partitioning?) async throws {
        guard !closed else {
            throw IggyError(.producerClosed)
        }
        let batch = QueuedBatch(streamID: streamID, topicID: topicID, messages: messages, partitioning: partitioning)
        if let byteBudget {
            guard batch.sizeBytes <= options.maxBufferSize else {
                throw IggyError(.backgroundSendBufferOverflow, context: "a batch of \(batch.sizeBytes) bytes exceeds the \(options.maxBufferSize) byte budget")
            }
            if await !byteBudget.tryAcquire(batch.sizeBytes) {
                switch options.backpressure {
                case .failImmediately:
                    throw IggyError(.backgroundSendBufferOverflow, context: "the byte budget is exhausted")
                case .block:
                    guard await byteBudget.acquire(batch.sizeBytes) else {
                        throw IggyError(.backgroundSendError, context: "the wait for buffer capacity was cancelled")
                    }
                case .blockWithTimeout(let timeout):
                    guard await byteBudget.acquire(batch.sizeBytes, timeout: timeout) else {
                        throw IggyError(.backgroundSendTimeout, context: "no buffer capacity within \(timeout)")
                    }
                }
            }
        }
        let worker = workers[pickWorker(for: batch)]
        do {
            try await worker.enqueue(batch)
        } catch {
            await byteBudget?.release(batch.sizeBytes)
            throw error
        }
    }

    func shutdown() async {
        guard !closed else {
            return
        }
        closed = true
        for worker in workers {
            await worker.stop()
        }
        failures.finish()
        await errorTask.value
    }

    private func pickWorker(for batch: QueuedBatch) -> Int {
        let count = workers.count
        switch options.sharding {
        case .ordered:
            var hasher = Hasher()
            hasher.combine(batch.streamID)
            hasher.combine(batch.topicID)
            return Int(UInt(bitPattern: hasher.finalize()) % UInt(count))
        case .balanced:
            let index = roundRobin % count
            roundRobin = (roundRobin + 1) % count
            return index
        case .custom(let pick):
            let index = pick(count, batch.streamID, batch.topicID, batch.messages)
            precondition(index >= 0 && index < count, "the sharding strategy picked worker \(index) of \(count)")
            return index
        }
    }

    private static func logFailure(_ logger: Logger) -> @Sendable (ProducerSendFailure) async -> Void {
        { failure in
            logger.error(
                "Failed to send messages in the background",
                metadata: [
                    "stream": "\(failure.streamID)", "topic": "\(failure.topicID)", "partitioning": "\(failure.partitioning.map { "\($0)" } ?? "none")",
                    "messages": "\(failure.messages.count)", "committed_confirmations": "\(failure.committed.count)", "cause": "\(failure.cause)",
                ])
        }
    }
}

/// A worker with a queue of its own. It buffers the sends routed to it and
/// writes them one request at a time, so per-destination order is kept.
actor ShardWorker {
    /// Sends handed over but not yet picked up. The worker does not pick up
    /// while it writes, so a slow server makes dispatch wait here.
    static let queueCapacity = 256

    private let core: ProducerCore
    private let options: BackgroundSendOptions
    private let inFlight: AsyncSemaphore?
    private let byteBudget: AsyncSemaphore?
    private let failures: AsyncStream<ProducerSendFailure>.Continuation
    private let logger: Logger
    private let queueSlots = AsyncSemaphore(permits: ShardWorker.queueCapacity)

    private var queue: [QueuedBatch] = []
    private var buffer: [QueuedBatch] = []
    private var bufferBytes = 0
    private var lingerDeadline: ContinuousClock.Instant?
    private var lingerTimer: Task<Void, Never>?
    private var waiter: CheckedContinuation<Void, Never>?
    private var stopRequested = false
    private var closed = false
    private var loop: Task<Void, Never>?

    init(
        core: ProducerCore, options: BackgroundSendOptions, inFlight: AsyncSemaphore?, byteBudget: AsyncSemaphore?,
        failures: AsyncStream<ProducerSendFailure>.Continuation, logger: Logger
    ) {
        self.core = core
        self.options = options
        self.inFlight = inFlight
        self.byteBudget = byteBudget
        self.failures = failures
        self.logger = logger
    }

    /// The run loop starts with the first send, so an idle worker costs
    /// nothing.
    private func start() {
        guard loop == nil else { return }
        loop = Task { await self.run() }
    }

    func enqueue(_ batch: QueuedBatch) async throws {
        guard !closed else {
            throw IggyError(.producerClosed)
        }
        start()
        guard await queueSlots.acquire() else {
            throw IggyError(.backgroundSendError, context: "the wait for a queue slot was cancelled")
        }
        if closed {
            await queueSlots.release()
            throw IggyError(.producerClosed)
        }
        queue.append(batch)
        wake()
    }

    /// Flushes everything queued or buffered, then ends the worker.
    func stop() async {
        stopRequested = true
        guard let loop else {
            closed = true
            return
        }
        wake()
        await loop.value
    }

    private func run() async {
        while true {
            if stopRequested {
                closed = true
                while !queue.isEmpty {
                    await pickUp(queue.removeFirst())
                }
                await flush()
                return
            }
            if !queue.isEmpty {
                await pickUp(queue.removeFirst())
                let exceedsLength = options.batchLength > 0 && buffer.count >= options.batchLength
                let exceedsSize = options.batchSize > 0 && bufferBytes >= options.batchSize
                if exceedsLength || exceedsSize {
                    await flush()
                }
                continue
            }
            if let lingerDeadline, !buffer.isEmpty, ContinuousClock.now >= lingerDeadline {
                await flush()
                continue
            }
            await waitForWake()
        }
    }

    private func pickUp(_ batch: QueuedBatch) async {
        await queueSlots.release()
        if buffer.isEmpty {
            let deadline = ContinuousClock.now + options.lingerTime
            lingerDeadline = deadline
            lingerTimer?.cancel()
            lingerTimer = Task { [weak self] in
                try? await Task.sleep(until: deadline, clock: .continuous)
                guard !Task.isCancelled else { return }
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
            if let inFlight {
                _ = await inFlight.acquire()
            }
            do {
                _ = try await core.sendInternal(streamID: batch.streamID, topicID: batch.topicID, messages: batch.messages, partitioning: batch.partitioning)
            } catch let error as ProducerSendError {
                failures.yield(
                    ProducerSendFailure(
                        cause: error.cause, streamID: batch.streamID, topicID: batch.topicID, partitioning: batch.partitioning,
                        messages: error.failed, committed: error.committed))
            } catch {
                logger.error("Background send failed", metadata: ["error": "\(error)"])
            }
            await inFlight?.release()
            await byteBudget?.release(batch.sizeBytes)
        }
    }

    /// Consecutive sends to one destination become one request, as long as
    /// the merged size still fits a request.
    static func merge(_ batches: [QueuedBatch]) -> [QueuedBatch] {
        var merged: [QueuedBatch] = []
        merged.reserveCapacity(batches.count)
        for batch in batches {
            if var last = merged.last, last.sameDestination(as: batch), last.sizeBytes + batch.sizeBytes <= Int(UInt32.max) {
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
        guard let waiter else { return }
        self.waiter = nil
        waiter.resume()
    }

    private func waitForWake() async {
        await withCheckedContinuation { continuation in
            if let existing = waiter {
                // Only the run loop waits; a second waiter would mean a bug.
                existing.resume()
            }
            waiter = continuation
        }
    }
}
