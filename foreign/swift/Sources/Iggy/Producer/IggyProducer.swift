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

/// Appends messages to one topic of one stream, a port of `IggyProducer` in
/// the Rust SDK.
///
/// Build one with ``IggyClient/producer(stream:topic:configuration:)``, call
/// ``initialize()`` once, then send. A ``SendMode/direct`` producer writes
/// from the calling task and returns the confirmations. A
/// ``SendMode/background`` producer queues the batch and a worker writes it
/// later, so a send returns once the batch is queued and write failures go to
/// ``BackgroundSendOptions/errorHandler``.
///
/// ```swift
/// let producer = client.producer(stream: "orders", topic: "created")
/// try await producer.initialize()
/// let response = try await producer.send(["placed"])
/// ```
public final class IggyProducer: Sendable {
    public let streamID: Identifier
    public let topicID: Identifier

    let core: ProducerCore
    private let dispatcher: ProducerDispatcher?

    init(client: IggyClient, stream: Identifier, topic: Identifier, configuration: ProducerConfiguration) {
        streamID = stream
        topicID = topic
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
    /// again after it succeeded does nothing.
    public func initialize() async throws {
        try await core.initialize(streamID: streamID, topicID: topicID)
    }

    /// Sends `messages` with the producer's partitioning. Empty input is a
    /// no-op with no confirmations.
    ///
    /// A direct producer returns once every request of the batch was
    /// answered, and throws ``ProducerSendError`` naming the unconfirmed
    /// tail. A background producer returns once the batch is queued, with no
    /// confirmations, and throws only what queueing ran into:
    /// ``IggyErrorCode/producerClosed``,
    /// ``IggyErrorCode/backgroundSendBufferOverflow``,
    /// ``IggyErrorCode/backgroundSendTimeout``, or
    /// ``IggyErrorCode/backgroundSendError``.
    @discardableResult
    public func send(_ messages: [IggyMessage]) async throws -> SendMessagesResponse {
        try await send(messages, to: streamID, topicID: topicID, partitioning: nil)
    }

    /// Sends one message; see ``send(_:)``.
    @discardableResult
    public func send(_ message: IggyMessage) async throws -> SendMessagesResponse {
        try await send([message])
    }

    /// Sends with a per-call `partitioning`, or the producer's when nil.
    @discardableResult
    public func send(_ messages: [IggyMessage], partitioning: Partitioning?) async throws -> SendMessagesResponse {
        try await send(messages, to: streamID, topicID: topicID, partitioning: partitioning)
    }

    /// Sends to any stream and topic. The target has to exist, since
    /// ``initialize()`` creates only the producer's own.
    @discardableResult
    public func send(
        _ messages: [IggyMessage], to streamID: Identifier, topicID: Identifier, partitioning: Partitioning? = nil
    ) async throws -> SendMessagesResponse {
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
    /// writes and error handler, then stops following the client. Without it
    /// a background producer can lose buffered messages.
    public func shutdown() async {
        await dispatcher?.shutdown()
        await core.shutdown()
    }
}

extension IggyClient {
    /// A producer for the stream and topic. Building it never talks to the
    /// server; ``IggyProducer/initialize()`` does.
    public func producer(stream: Identifier, topic: Identifier, configuration: ProducerConfiguration = ProducerConfiguration()) -> IggyProducer {
        IggyProducer(client: self, stream: stream, topic: topic, configuration: configuration)
    }
}

/// What a background worker writes through: the producer core, or a test
/// double.
protocol ProducerCoreBackend: Sendable {
    /// Sends `messages`, throwing ``ProducerSendError`` when a request fails.
    func sendInternal(streamID: Identifier, topicID: Identifier, messages: [IggyMessage], partitioning: Partitioning?) async throws -> SendMessagesResponse
}

/// The state both send modes share: readiness, retries, chunking, and the
/// stream and topic bootstrap, as `ProducerCore` in the Rust SDK.
actor ProducerCore: ProducerCoreBackend {
    /// A request when chunking is disabled carries at most this many messages.
    static let maxBatchLength = 1_000_000

    private let client: IggyClient
    private let configuration: ProducerConfiguration
    private let directOptions: DirectSendOptions?
    private let logger: Logger

    private var isInitialized = false
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
            _ = try await client.createStream(name: name)
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
            _ = try await client.createTopic(streamID: streamID, name: name, options: options)
        }
        isInitialized = true
        logger.info("The producer has been initialized", metadata: ["stream": "\(streamID)", "topic": "\(topicID)"])
    }

    func shutdown() {
        eventsTask?.cancel()
        eventsTask = nil
    }

    /// Sends `messages`. A direct producer splits them into requests and
    /// returns every request's confirmations in order; a background worker
    /// sends them as one. Throws ``ProducerSendError`` naming the unconfirmed
    /// tail, with the ids the client minted for it.
    func sendInternal(streamID: Identifier, topicID: Identifier, messages: [IggyMessage], partitioning: Partitioning?) async throws -> SendMessagesResponse {
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
                let response = try await trySend(streamID: streamID, topicID: topicID, partitioning: resolved, messages: &messages)
                lastSentAt = .now
                return response
            } catch let error as CancellationError {
                throw error
            } catch {
                throw Self.failure(error, failed: messages, committed: [], streamID: streamID, topicID: topicID)
            }
        }
        if directOptions.lingerTime > .zero, let lastSentAt {
            let elapsed = ContinuousClock.now - lastSentAt
            if elapsed < directOptions.lingerTime {
                try await Task.sleep(for: directOptions.lingerTime - elapsed)
            }
        }
        let chunkLength = directOptions.batchLength == 0 ? Self.maxBatchLength : Int(directOptions.batchLength)
        var confirmations: [SendConfirmation] = []
        confirmations.reserveCapacity((messages.count + chunkLength - 1) / chunkLength)
        var index = 0
        while index < messages.count {
            let end = min(index + chunkLength, messages.count)
            var chunk = Array(messages[index..<end])
            do {
                let response = try await trySend(streamID: streamID, topicID: topicID, partitioning: resolved, messages: &chunk)
                confirmations.append(contentsOf: response.confirmations)
            } catch let error as CancellationError {
                throw error
            } catch {
                messages.replaceSubrange(index..<end, with: chunk)
                throw Self.failure(error, failed: Array(messages[index...]), committed: confirmations, streamID: streamID, topicID: topicID)
            }
            lastSentAt = .now
            index = end
        }
        return SendMessagesResponse(confirmations: confirmations)
    }

    /// A partitioner wins over any partitioning; then the call's, the
    /// producer's, and balanced.
    private func resolvePartitioning(
        streamID: Identifier, topicID: Identifier, messages: [IggyMessage], override: Partitioning?
    ) throws -> Partitioning {
        if let partitioner = configuration.partitioner {
            return .partition(try partitioner(streamID, topicID, messages))
        }
        return override ?? configuration.partitioning ?? .balanced
    }

    /// One request with the producer's retry budget. The ids the client mints
    /// on the first attempt stay in `messages`, so every retry carries them.
    private func trySend(
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

    private func waitUntilConnected(maxRetries: UInt32, streamID: Identifier, topicID: Identifier) async throws {
        var retries: UInt32 = 0
        var ticker = RetryTicker(interval: configuration.sendRetryInterval)
        while !canSend {
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

    /// A retry is a new request identity, so only a refusal or a failure
    /// before anything was written allows one. An unknown outcome belongs to
    /// the transport, which replays it under its own identity.
    static func stopsSendRetries(_ error: IggyError) -> Bool {
        switch error.code {
        case .transientNotAccepted, .notConnected, .cannotEstablishConnection:
            false
        default:
            true
        }
    }

    /// The gate starts open and follows the events seen after initialization:
    /// a connection, disconnection, sign-out, or shutdown closes it, and only
    /// a sign-in opens it again.
    private func subscribeEvents() async {
        eventsTask?.cancel()
        let events = await client.events
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
        let cause = (error as? IggyError) ?? IggyError(.error, context: "\(error)")
        return ProducerSendError(cause: cause, failed: failed, committed: committed, streamID: streamID, topicID: topicID)
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
