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

/// Reads the partitions of one topic and hands their messages over one at a
/// time, a port of `IggyConsumer` in the Rust SDK, on one
/// ``PartitionReader``.
///
/// Build a standalone consumer of chosen partitions with
/// ``IggyClient/consumer(name:streamID:topicID:partitions:configuration:)``,
/// or a member of a consumer group with
/// ``IggyClient/consumerGroup(name:streamID:topicID:configuration:)``, whose
/// partitions the server assigns. Call ``initialize()`` once, then read with
/// ``next()``, `for try await`, or ``consumeMessages(_:)``:
///
/// ```swift
/// let consumer = try client.consumerGroup(name: "billing", streamID: "orders", topicID: "created")
/// try await consumer.initialize()
/// for try await received in consumer {
///     print(received.partitionID, received.message.offset)
/// }
/// ```
///
/// The ``CommitPolicy`` of ``ConsumerConfiguration/pollOptions`` decides
/// who stores offsets: the application with ``commit(_:)`` under `manual`,
/// the server with each read under `onRead`, the consumer under `auto`,
/// which counts a message when it is handed over (``CommitAfter/delivery``),
/// once the application asked for the next one or its handler returned
/// (``CommitAfter/processing``), or once it is marked
/// (``CommitAfter/marked``). ``shutdown()`` stores the counted positions,
/// moves an `onRead` offset back to the last message handed over, and
/// leaves the group.
///
/// A failed read is thrown as a ``ReadError`` and the consumer can be read
/// on. ``next()`` returns nil only after ``shutdown()``. One reader consumes
/// a consumer: two loops over the same consumer split its messages.
public actor IggyConsumer: AsyncSequence {
    public typealias Element = ReceivedMessage

    public nonisolated let name: String
    public nonisolated let streamID: Identifier
    public nonisolated let topicID: Identifier
    public nonisolated let consumer: Consumer
    /// The partitions a standalone consumer reads, nil for a group member.
    public nonisolated let partitions: [UInt32]?
    /// The partition of the last message handed over, zero before the first.
    public private(set) var currentPartitionID: UInt32 = 0

    private let client: IggyClient
    private let configuration: ConsumerConfiguration
    private let logger: Logger
    private let reading = AsyncMutex()
    private var reader: PartitionReader?
    private var initialization: Task<Void, any Error>?
    private var isShutDown = false
    private var shutdownTask: Task<Void, Never>?
    private var buffer: PolledMessages?
    private var bufferIndex = 0
    /// With ``CommitAfter/processing``: the message ``next()`` handed over
    /// last, which counts once the next one is asked for.
    private var unprocessed: (checkpoint: Checkpoint, endsBatch: Bool)?
    /// The last message handed over from each partition, which the replay
    /// filter skips up to.
    private var lastConsumed: [UInt32: Checkpoint] = [:]

    init(
        client: IggyClient, name: String, consumer: Consumer, streamID: Identifier, topicID: Identifier, partitions: [UInt32]?,
        configuration: ConsumerConfiguration
    ) throws {
        try configuration.validate()
        try configuration.reader(partitions: partitions).validate(group: consumer.kind == .consumerGroup)
        self.client = client
        self.name = name
        self.consumer = consumer
        self.streamID = streamID
        self.topicID = topicID
        self.partitions = partitions
        self.configuration = configuration
        logger = client.logger
    }

    public nonisolated func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(consumer: self)
    }

    public struct AsyncIterator: AsyncIteratorProtocol {
        /// Nil once an error or nil ended the iteration, as the protocol
        /// requires. The consumer itself can be read on after an error.
        var consumer: IggyConsumer?

        public mutating func next() async throws -> ReceivedMessage? {
            guard let consumer else {
                return nil
            }
            do {
                let received = try await consumer.next()
                if received == nil {
                    self.consumer = nil
                }
                return received
            } catch {
                self.consumer = nil
                throw error
            }
        }
    }

    // MARK: Lifecycle

    /// Looks the stream and topic up, retrying a missing one as
    /// ``ConsumerConfiguration/initRetries`` says, and builds the reader,
    /// which joins the group of a member that joins on its own. Concurrent
    /// calls share one run, and calling it again after it succeeded, or after
    /// ``shutdown()``, does nothing.
    public func initialize() async throws {
        if reader != nil || isShutDown {
            return
        }
        if let initialization {
            return try await initialization.value
        }
        let task = Task { try await self.start() }
        initialization = task
        defer { initialization = nil }
        try await task.value
    }

    private func start() async throws {
        logger.info("Initializing the consumer", metadata: metadata)
        try await waitForStreamAndTopic()
        let built = try await client.reader(
            streamID: streamID, topicID: topicID, consumer: consumer, configuration: configuration.reader(partitions: partitions))
        guard !isShutDown else {
            try? await built.close()
            return
        }
        reader = built
        logger.info("The consumer has been initialized", metadata: metadata)
    }

    /// Stops reading, counts the last message under
    /// ``CommitAfter/processing``, and closes the reader, which stores the
    /// counted positions, moves an `onRead` offset back to the last message
    /// handed over, and leaves the group, within
    /// ``ConsumerConfiguration/offsetDrainTimeout``. Every caller waits for
    /// the same shutdown. Failures are logged.
    public func shutdown() async {
        if let shutdownTask {
            await shutdownTask.value
            return
        }
        isShutDown = true
        let task = Task { await self.drain() }
        shutdownTask = task
        await task.value
    }

    private func drain() async {
        logger.info("Shutting down the consumer", metadata: metadata)
        if let initialization {
            _ = try? await initialization.value
        }
        guard let reader else {
            return
        }
        await countUnprocessed()
        do {
            let closed = try await withDeadline(.now + configuration.offsetDrainTimeout) {
                try await reader.close()
                return true
            }
            if closed == nil {
                logger.warning("The reader did not close within the offset drain timeout", metadata: metadata)
            }
        } catch {
            logger.warning("The reader closed with an error", metadata: metadata.merging(["error": "\(error)"]) { $1 })
        }
        logger.info("The consumer has been shut down", metadata: metadata)
    }

    // MARK: Reading

    /// The next message, or nil once the consumer is shut down. Throws a
    /// ``ReadError`` and can be called again after it threw.
    public func next() async throws -> ReceivedMessage? {
        await countUnprocessed()
        guard let received = try await take() else {
            return nil
        }
        if configuration.pollOptions.autoCommit?.after == .processing {
            unprocessed = (received.checkpoint, isLastOfBatch)
        }
        return received
    }

    /// Hands every message to `handler` until the consumer shuts down, the
    /// task is cancelled, or a read error ends the reading: a client that is
    /// shut down or cannot come back without the caller, or an invalid
    /// address. Other read errors are logged and reading goes on. Under
    /// ``CommitAfter/processing`` a message counts once its handler returned,
    /// and a handler that throws leaves its message uncounted and ends the
    /// reading with its error. Under any other policy a handler that throws
    /// is logged.
    public func consumeMessages(_ handler: @Sendable (ReceivedMessage) async throws -> Void) async throws {
        await countUnprocessed()
        let processing = configuration.pollOptions.autoCommit?.after == .processing
        while true {
            let received: ReceivedMessage
            do {
                guard let next = try await take() else {
                    return
                }
                received = next
            } catch let error as ReadError {
                let recovers = await client.canRecover
                if Self.endsConsumption(error.error) || !recovers {
                    logger.error("Reading stopped", metadata: metadata.merging(["error": "\(error)"]) { $1 })
                    throw error
                }
                logger.error("Failed to read a message", metadata: metadata.merging(["error": "\(error)"]) { $1 })
                continue
            }
            let endsBatch = isLastOfBatch
            do {
                try await handler(received)
            } catch {
                logger.error(
                    "The handler failed", metadata: metadata.merging(["offset": "\(received.message.offset)", "error": "\(error)"]) { $1 })
                if processing {
                    throw error
                }
                continue
            }
            if processing, let reader {
                await reader.count(received.checkpoint, messages: 1, endsBatch: endsBatch)
            }
        }
    }

    // MARK: Offsets

    /// Stores the offset of a checkpoint the consumer handed over, now. A
    /// checkpoint of an older generation of its partition stores nothing.
    /// Throws ``IggyErrorCode/invalidCommand`` under ``CommitPolicy/onRead``.
    public func commit(_ checkpoint: Checkpoint) async throws {
        try await requireReader().commit(checkpoint)
    }

    /// Counts a checkpoint the consumer handed over, under
    /// ``CommitPolicy/auto(_:)``, for messages processed concurrently with
    /// ``CommitAfter/marked``. Throws ``IggyErrorCode/invalidCommand``
    /// without `auto`.
    public func mark(_ checkpoint: Checkpoint) async throws {
        try await requireReader().mark(checkpoint)
    }

    /// Sets where `partitionID` reads next, as
    /// ``PartitionReader/seek(partitionID:to:)`` does, and drops its messages
    /// not handed over yet. A seek back hands the messages over again.
    public func seek(partitionID: UInt32, to start: PollingStrategy) async throws {
        try await requireReader().seek(partitionID: partitionID, to: start)
        if buffer?.partitionID == partitionID {
            buffer = nil
            bufferIndex = 0
        }
        lastConsumed[partitionID] = nil
        if unprocessed?.checkpoint.partitionID == partitionID {
            unprocessed = nil
        }
    }

    /// The offset of the last message handed over from the partition.
    public func lastConsumedOffset(partitionID: UInt32) -> UInt64? {
        lastConsumed[partitionID]?.offset
    }

    // MARK: Internals

    private var metadata: Logger.Metadata {
        ["consumer": "\(name)", "stream": "\(streamID)", "topic": "\(topicID)"]
    }

    private var isLastOfBatch: Bool {
        guard let buffer else {
            return true
        }
        return bufferIndex >= buffer.messages.count
    }

    private func requireReader() throws -> PartitionReader {
        guard let reader else {
            throw IggyError(.invalidCommand, context: "the consumer is not initialized")
        }
        return reader
    }

    /// The next message from the buffer, or from the next batch of the
    /// reader once it is empty, without those that replay skips.
    private func take() async throws -> ReceivedMessage? {
        try await reading.lock()
        defer { reading.unlock() }
        while !isShutDown {
            if let buffered = buffer, bufferIndex < buffered.messages.count {
                return await handOver(buffered)
            }
            buffer = nil
            guard let reader else {
                throw IggyError(.invalidCommand, context: "the consumer is not initialized")
            }
            guard var batch = try await reader.nextBatch(), !isShutDown else {
                return nil
            }
            skipReplayed(&batch)
            buffer = batch
            bufferIndex = 0
        }
        return nil
    }

    private func handOver(_ batch: PolledMessages) async -> ReceivedMessage {
        let index = bufferIndex
        bufferIndex += 1
        let message = batch.messages[index]
        let checkpoint = batch.checkpoint(at: index) ?? Checkpoint(partitionID: batch.partitionID, offset: message.offset, context: batch.context)
        currentPartitionID = batch.partitionID
        lastConsumed[batch.partitionID] = checkpoint
        let endsBatch = bufferIndex == batch.messages.count
        if let reader {
            await reader.deliver(checkpoint)
            if configuration.pollOptions.autoCommit?.after == .delivery {
                await reader.count(checkpoint, messages: 1, endsBatch: endsBatch)
            }
        }
        return ReceivedMessage(message: message, currentOffset: batch.currentOffset, partitionID: batch.partitionID, checkpoint: checkpoint)
    }

    private func countUnprocessed() async {
        guard let (checkpoint, endsBatch) = unprocessed, let reader else {
            return
        }
        unprocessed = nil
        await reader.count(checkpoint, messages: 1, endsBatch: endsBatch)
    }

    /// Drops the messages at or below the last one handed over from the same
    /// partition history, unless replay is allowed.
    private func skipReplayed(_ batch: inout PolledMessages) {
        guard !configuration.allowReplay, let consumed = lastConsumed[batch.partitionID],
            PartitionReader.sameHistory(consumed.context, batch.context)
        else {
            return
        }
        batch.messages.removeAll { $0.offset <= consumed.offset }
        batch.count = UInt32(batch.messages.count)
    }

    private func waitForStreamAndTopic() async throws {
        let retries = configuration.initRetries ?? 0
        var ticker = RetryTicker(interval: configuration.initRetryInterval)
        try await ticker.tick()
        var streamExists = try await client.getStream(streamID) != nil
        var topicExists = false
        if streamExists {
            topicExists = try await client.getTopic(streamID: streamID, topicID: topicID) != nil
        }
        var attempt: UInt32 = 0
        // A stream or topic may still be being created by someone else.
        while !(streamExists && topicExists), attempt < retries {
            attempt += 1
            logger.warning(
                "The stream or topic does not exist yet, retrying",
                metadata: metadata.merging(["retry": "\(attempt)/\(retries)", "stream_exists": "\(streamExists)"]) { $1 })
            try await ticker.tick()
            if !streamExists {
                streamExists = try await client.getStream(streamID) != nil
            }
            if streamExists {
                topicExists = try await client.getTopic(streamID: streamID, topicID: topicID) != nil
            }
        }
        guard streamExists else {
            throw streamID.name.map { IggyError(.streamNameNotFound, context: "stream \($0) was not found") }
                ?? IggyError(.streamIdNotFound, context: "stream \(streamID) was not found")
        }
        guard topicExists else {
            throw topicID.name.map { IggyError(.topicNameNotFound, context: "topic \($0) was not found in stream \(streamID)") }
                ?? IggyError(.topicIdNotFound, context: "topic \(topicID) was not found in stream \(streamID)")
        }
    }

    static func endsConsumption(_ error: IggyError) -> Bool {
        switch error.code {
        case .invalidServerAddress, .invalidClientAddress, .clientShutdown: true
        default: false
        }
    }
}

extension IggyClient {
    /// A standalone consumer of `partitions`, identified by `name`. Building
    /// it never talks to the server, ``IggyConsumer/initialize()`` does.
    /// Throws ``IggyErrorCode/invalidConfiguration`` for an option out of
    /// range or partitions that are not 1 to 1000 distinct ids.
    public func consumer(
        name: String, streamID: Identifier, topicID: Identifier, partitions: [UInt32], configuration: ConsumerConfiguration = ConsumerConfiguration()
    ) throws -> IggyConsumer {
        try IggyConsumer(
            client: self, name: name, consumer: .consumer(try Identifier(named: name)), streamID: streamID, topicID: topicID, partitions: partitions,
            configuration: configuration)
    }

    /// A member of the consumer group `name`, which reads the partitions the
    /// server assigns to it. Building it never talks to the server,
    /// ``IggyConsumer/initialize()`` does. Throws what building a standalone
    /// consumer throws.
    public func consumerGroup(
        name: String, streamID: Identifier, topicID: Identifier, configuration: ConsumerConfiguration = ConsumerConfiguration()
    ) throws -> IggyConsumer {
        try IggyConsumer(
            client: self, name: name, consumer: .group(try Identifier(named: name)), streamID: streamID, topicID: topicID, partitions: nil,
            configuration: configuration)
    }
}
