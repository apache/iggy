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

/// Appends messages to one topic of one stream, a port of `IggyProducer` in
/// the Rust SDK.
///
/// Build one with ``IggyClient/producer(streamID:topicID:configuration:)``,
/// call ``initialize()`` once, then send. A ``SendMode/direct`` producer
/// writes from the calling task and returns the confirmations. A
/// ``SendMode/background`` producer queues the batch and a worker writes it
/// later, so a send returns once the batch is queued and write failures go to
/// ``BackgroundSendOptions/errorHandler``.
///
/// ```swift
/// let producer = try client.producer(streamID: "orders", topicID: "created")
/// try await producer.initialize()
/// let response = try await producer.send(["placed"])
/// ```
public final class IggyProducer: Sendable {
    public let streamID: Identifier
    public let topicID: Identifier

    let core: ProducerCore
    let dispatcher: ProducerDispatcher?
    private let closed = NIOLockedValueBox(false)

    init(client: IggyClient, streamID: Identifier, topicID: Identifier, configuration: ProducerConfiguration) throws {
        try configuration.validate()
        self.streamID = streamID
        self.topicID = topicID
        core = ProducerCore(client: client, configuration: configuration)
        switch configuration.mode {
        case .direct:
            dispatcher = nil
        case .background(let options):
            dispatcher = ProducerDispatcher(backend: core, options: options, logger: client.logger)
        }
    }

    /// Makes the producer ready to send: the stream and topic are looked up
    /// and created when missing, named, and allowed by the configuration, and
    /// the producer starts following the client's sign-in state. Calling it
    /// again after it succeeded does nothing. Another producer creating the
    /// same stream or topic first counts as created.
    public func initialize() async throws {
        try checkOpen()
        try await core.initialize(streamID: streamID, topicID: topicID)
    }

    /// Sends `messages` with the producer's partitioning. Empty input is a
    /// no-op with no confirmations.
    ///
    /// A direct producer returns once every request of the batch was
    /// answered, and throws ``ProducerSendError`` naming the unconfirmed
    /// tail, also when the calling task is cancelled. A background producer
    /// returns once the batch is queued, with no confirmations, and throws
    /// only what queueing ran into: ``IggyErrorCode/backgroundSendBufferOverflow``,
    /// ``IggyErrorCode/backgroundSendTimeout``,
    /// ``IggyErrorCode/backgroundSendError``, or `CancellationError` with
    /// nothing queued. Both throw ``IggyErrorCode/producerClosed`` after
    /// ``shutdown()``.
    @discardableResult
    public func send(_ messages: [IggyMessage]) async throws -> SendMessagesResponse {
        try await send(messages, streamID: streamID, topicID: topicID, partitioning: nil)
    }

    /// Sends one message; see ``send(_:)``.
    @discardableResult
    public func send(_ message: IggyMessage) async throws -> SendMessagesResponse {
        try await send([message])
    }

    /// Sends with a per-call `partitioning`, or the producer's when nil.
    @discardableResult
    public func send(_ messages: [IggyMessage], partitioning: Partitioning?) async throws -> SendMessagesResponse {
        try await send(messages, streamID: streamID, topicID: topicID, partitioning: partitioning)
    }

    /// Sends to any stream and topic. The target has to exist, since
    /// ``initialize()`` creates only the producer's own.
    @discardableResult
    public func send(
        _ messages: [IggyMessage], streamID: Identifier, topicID: Identifier, partitioning: Partitioning? = nil
    ) async throws -> SendMessagesResponse {
        try checkOpen()
        guard !messages.isEmpty else {
            return SendMessagesResponse()
        }
        if let dispatcher {
            try await dispatcher.dispatch(messages, streamID: streamID, topicID: topicID, partitioning: partitioning)
            return SendMessagesResponse()
        }
        return try await core.sendInternal(streamID: streamID, topicID: topicID, messages: messages, partitioning: partitioning)
    }

    /// Flushes what a background producer still holds and waits for its
    /// writes and error handler, then stops following the client. Every
    /// caller waits for the same drain. A producer released without it
    /// still writes what it held, but nothing waits for those writes.
    public func shutdown() async {
        closed.withLockedValue { $0 = true }
        await dispatcher?.shutdown()
        await core.shutdown()
    }

    private func checkOpen() throws {
        if closed.withLockedValue({ $0 }) {
            throw IggyError(.producerClosed)
        }
    }
}

extension IggyClient {
    /// A producer for the stream and topic. Building it never talks to the
    /// server; ``IggyProducer/initialize()`` does. Throws
    /// ``IggyErrorCode/invalidConfiguration`` for a negative option.
    public func producer(
        streamID: Identifier, topicID: Identifier, configuration: ProducerConfiguration = ProducerConfiguration()
    ) throws -> IggyProducer {
        try IggyProducer(client: self, streamID: streamID, topicID: topicID, configuration: configuration)
    }
}

/// What a background worker writes through: the producer core, or a test
/// double.
protocol ProducerCoreBackend: Sendable {
    /// Sends `messages`, throwing ``ProducerSendError`` when a request fails.
    func sendInternal(streamID: Identifier, topicID: Identifier, messages: [IggyMessage], partitioning: Partitioning?) async throws -> SendMessagesResponse
}

/// The state both send modes share: readiness, retries, chunking, and the
/// stream and topic bootstrap, as `ProducerCore` in the Rust SDK. Sends run
/// outside the actor, so workers and direct callers do not queue on it; it
/// holds only the sign-in gate and the linger clock.
actor ProducerCore: ProducerCoreBackend {
    /// A request when chunking is disabled carries at most this many messages.
    static let maxBatchLength = 1_000_000

    private let client: IggyClient
    private let configuration: ProducerConfiguration
    private let directOptions: DirectSendOptions?
    private let logger: Logger

    private var isInitialized = false
    private var isShutDown = false
    /// Follows the client's events once initialized, and is read only while
    /// a retry budget exists.
    private(set) var canSend = true
    private var lastSentAt: ContinuousClock.Instant?
    private var eventsTask: Task<Void, Never>?

    init(client: IggyClient, configuration: ProducerConfiguration) {
        self.client = client
        self.configuration = configuration
        if case .direct(let options) = configuration.mode {
            directOptions = options
        } else {
            directOptions = nil
        }
        logger = client.logger
    }

    deinit {
        eventsTask?.cancel()
    }

    func initialize(streamID: Identifier, topicID: Identifier) async throws {
        if isInitialized {
            return
        }
        logger.info("Initializing the producer", metadata: ["stream": "\(streamID)", "topic": "\(topicID)"])
        await subscribeEvents()
        if try await client.getStream(streamID) == nil {
            guard let name = streamID.name else {
                throw IggyError(.streamIdNotFound, context: "stream \(streamID) does not exist and cannot be created from a numeric id")
            }
            guard configuration.createStreamIfNotExists else {
                throw IggyError(.streamNameNotFound, context: "stream \(name) does not exist and auto-creation is disabled")
            }
            logger.info("Creating the stream", metadata: ["stream": "\(name)"])
            do {
                _ = try await client.createStream(name: name)
            } catch let error as IggyError where error.code == .streamNameAlreadyExists {
                logger.info("The stream was created by someone else", metadata: ["stream": "\(name)"])
            }
        }
        if try await client.getTopic(streamID: streamID, topicID: topicID) == nil {
            guard let name = topicID.name else {
                throw IggyError(.topicIdNotFound, context: "topic \(topicID) does not exist and cannot be created from a numeric id")
            }
            guard configuration.createTopicIfNotExists else {
                throw IggyError(.topicNameNotFound, context: "topic \(name) does not exist in stream \(streamID) and auto-creation is disabled")
            }
            logger.info("Creating the topic", metadata: ["stream": "\(streamID)", "topic": "\(name)"])
            let options = TopicCreateOptions(
                partitionsCount: configuration.topicPartitionsCount,
                messageExpiry: configuration.topicMessageExpiry == .serverDefault ? nil : configuration.topicMessageExpiry,
                maxTopicSize: configuration.topicMaxSize == .serverDefault ? nil : configuration.topicMaxSize,
                durability: configuration.topicDurability)
            do {
                _ = try await client.createTopic(streamID: streamID, name: name, options: options)
            } catch let error as IggyError where error.code == .topicNameAlreadyExists {
                logger.info("The topic was created by someone else", metadata: ["stream": "\(streamID)", "topic": "\(name)"])
            }
        }
        isInitialized = true
        logger.info("The producer has been initialized", metadata: ["stream": "\(streamID)", "topic": "\(topicID)"])
    }

    func shutdown() {
        isShutDown = true
        eventsTask?.cancel()
        eventsTask = nil
    }

    /// Sends `messages`. A direct producer splits them into requests and
    /// returns every request's confirmations in order; a background worker
    /// sends them as one. Throws ``ProducerSendError`` naming the unconfirmed
    /// tail, with the ids the client minted for it, whatever stopped the send.
    nonisolated func sendInternal(
        streamID: Identifier, topicID: Identifier, messages: [IggyMessage], partitioning: Partitioning?
    ) async throws -> SendMessagesResponse {
        guard !messages.isEmpty else {
            return SendMessagesResponse()
        }
        var messages = messages
        let resolved: Partitioning
        do {
            resolved = try resolvePartitioning(streamID: streamID, topicID: topicID, messages: messages, override: partitioning)
        } catch {
            throw Self.failure(error, failed: messages, committed: [], streamID: streamID, topicID: topicID)
        }
        guard let directOptions else {
            do {
                return try await trySend(streamID: streamID, topicID: topicID, partitioning: resolved, messages: &messages)
            } catch {
                throw Self.failure(error, failed: messages, committed: [], streamID: streamID, topicID: topicID)
            }
        }
        do {
            try await waitOutLinger(directOptions.lingerTime)
        } catch {
            throw Self.failure(error, failed: messages, committed: [], streamID: streamID, topicID: topicID)
        }
        // Clamped so a `UInt32` length cannot trap where `Int` is 32 bits.
        let chunkLength = directOptions.batchLength == 0 ? Self.maxBatchLength : min(Int(clamping: directOptions.batchLength), Self.maxBatchLength)
        var confirmations: [SendConfirmation] = []
        confirmations.reserveCapacity((messages.count - 1) / chunkLength + 1)
        var index = 0
        while index < messages.count {
            let end = index + min(chunkLength, messages.count - index)
            var chunk = Array(messages[index..<end])
            do {
                let response = try await trySend(streamID: streamID, topicID: topicID, partitioning: resolved, messages: &chunk)
                confirmations.append(contentsOf: response.confirmations)
            } catch {
                messages.replaceSubrange(index..<end, with: chunk)
                throw Self.failure(error, failed: Array(messages[index...]), committed: confirmations, streamID: streamID, topicID: topicID)
            }
            await markSent()
            index = end
        }
        return SendMessagesResponse(confirmations: confirmations)
    }

    /// A partitioner wins over any partitioning; then the call's, the
    /// producer's, and balanced.
    private nonisolated func resolvePartitioning(
        streamID: Identifier, topicID: Identifier, messages: [IggyMessage], override: Partitioning?
    ) throws -> Partitioning {
        if let partitioner = configuration.partitioner {
            return .partition(try partitioner(streamID, topicID, messages))
        }
        return override ?? configuration.partitioning ?? .balanced
    }

    private nonisolated func waitOutLinger(_ lingerTime: Duration) async throws {
        guard lingerTime > .zero, let lastSentAt = await lastSentAt else {
            return
        }
        let elapsed = ContinuousClock.now - lastSentAt
        if elapsed < lingerTime {
            try await Task.sleep(for: lingerTime - elapsed)
        }
    }

    private func markSent() {
        lastSentAt = .now
    }

    /// One request with the producer's retry budget. The ids the client mints
    /// on the first attempt stay in `messages`, so every retry carries them.
    private nonisolated func trySend(
        streamID: Identifier, topicID: Identifier, partitioning: Partitioning, messages: inout [IggyMessage]
    ) async throws -> SendMessagesResponse {
        guard let maxRetries = configuration.sendRetries, maxRetries > 0 else {
            return try await client.sendMessages(streamID: streamID, topicID: topicID, partitioning: partitioning, messages: &messages)
        }
        try await waitUntilConnected(maxRetries: maxRetries, streamID: streamID, topicID: topicID)
        var retries: UInt32 = 0
        var ticker = RetryTicker(interval: configuration.sendRetryInterval)
        while true {
            do {
                return try await client.sendMessages(streamID: streamID, topicID: topicID, partitioning: partitioning, messages: &messages)
            } catch let error as IggyError {
                if Self.stopsSendRetries(error) {
                    logger.error("Send stopped", metadata: ["stream": "\(streamID)", "topic": "\(topicID)", "error": "\(error)"])
                    throw error
                }
                retries += 1
                if retries > maxRetries {
                    logger.error(
                        "Failed to send messages, giving up",
                        metadata: ["stream": "\(streamID)", "topic": "\(topicID)", "retries": "\(maxRetries)", "error": "\(error)"])
                    throw error
                }
                logger.error(
                    "Failed to send messages, retrying",
                    metadata: ["stream": "\(streamID)", "topic": "\(topicID)", "retry": "\(retries)/\(maxRetries)", "error": "\(error)"])
                try await ticker.tick()
            }
        }
    }

    /// Each check asks the client to sign in again, since a client whose
    /// heartbeat is off or that was never connected does not reconnect on
    /// its own.
    private nonisolated func waitUntilConnected(maxRetries: UInt32, streamID: Identifier, topicID: Identifier) async throws {
        var retries: UInt32 = 0
        var ticker = RetryTicker(interval: configuration.sendRetryInterval)
        while await !canSend {
            if (try? await client.requireAuthenticated()) != nil {
                await openGate()
                return
            }
            retries += 1
            if retries > maxRetries {
                logger.error(
                    "The client is disconnected, giving up sending", metadata: ["stream": "\(streamID)", "topic": "\(topicID)", "retries": "\(maxRetries)"])
                throw IggyError(.cannotSendMessagesDueToClientDisconnection)
            }
            logger.error(
                "The client is disconnected, retrying the send",
                metadata: ["stream": "\(streamID)", "topic": "\(topicID)", "retry": "\(retries)/\(maxRetries)"])
            try await ticker.tick()
        }
    }

    private func openGate() {
        canSend = true
    }

    /// A retry is a new request identity, so only a refusal or a failure
    /// before anything was written allows one. An unknown outcome belongs to
    /// the transport, which replays it under its own identity. The client
    /// raises `disconnected` for a send only before writing it: a write it
    /// lost becomes `transientNotCommitted`.
    static func stopsSendRetries(_ error: IggyError) -> Bool {
        switch error.code {
        case .transientNotAccepted, .notConnected, .cannotEstablishConnection, .disconnected:
            false
        default:
            true
        }
    }

    /// The gate starts open and follows the events seen after initialization:
    /// a connection, disconnection, sign-out, or shutdown closes it, and only
    /// a sign-in opens it again. Nothing suspends between replacing the task
    /// and storing it, so concurrent calls leave one subscription.
    private func subscribeEvents() async {
        let events = await client.events
        guard !isShutDown else {
            return
        }
        eventsTask?.cancel()
        eventsTask = Task { [weak self] in
            for await event in events {
                guard let self else {
                    return
                }
                await self.observe(event)
            }
        }
    }

    private func observe(_ event: DiagnosticEvent) {
        switch event {
        case .signedIn:
            canSend = true
        case .connected, .disconnected, .signedOut:
            canSend = false
        case .shutdown:
            canSend = false
            logger.warning("The client has been shut down")
        }
    }

    private static func failure(
        _ error: any Error, failed: [IggyMessage], committed: [SendConfirmation], streamID: Identifier, topicID: Identifier
    ) -> ProducerSendError {
        if let error = error as? IggyError {
            return ProducerSendError(cause: error, failed: failed, committed: committed, streamID: streamID, topicID: topicID)
        }
        let context = error is CancellationError ? "the send was cancelled" : "\(error)"
        return ProducerSendError(
            cause: IggyError(.error, context: context), underlying: error, failed: failed, committed: committed, streamID: streamID, topicID: topicID)
    }
}

/// The pacing of the Tokio interval the Rust producer retries on: the first
/// tick is immediate and each later one comes an interval after the one
/// before, at once when an attempt outlasted it.
struct RetryTicker {
    let interval: Duration?
    private var next: ContinuousClock.Instant?

    init(interval: Duration?) {
        self.interval = interval
    }

    mutating func tick() async throws {
        guard let interval else {
            return
        }
        guard let next else {
            next = .now + interval
            return
        }
        try await Task.sleep(until: next, clock: .continuous)
        self.next = next + interval
    }
}
