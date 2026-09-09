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

/// Appends messages to one topic of one stream.
///
/// Build one with ``IggyClient/producer(stream:topic:configuration:)``, call
/// ``initialize()`` once, then ``send(_:)``. A ``SendMode/direct`` producer
/// writes from the calling task and returns confirmations; a
/// ``SendMode/background`` producer queues the batch and a worker writes it
/// later, so ``send(_:)`` returns as soon as the batch is queued and write
/// failures reach ``BackgroundSendOptions/errorHandler`` instead.
///
/// ```swift
/// let producer = client.producer(stream: "orders", topic: "created")
/// try await producer.initialize()
/// let response = try await producer.send([try IggyMessage("hello")])
/// ```
public final class IggyProducer: Sendable {
    public let streamID: Identifier
    public let topicID: Identifier
    public let configuration: ProducerConfiguration

    let core: ProducerCore
    private let dispatcher: ProducerDispatcher?

    /// A producer on `client`. Building never talks to the server.
    public convenience init(client: IggyClient, stream: Identifier, topic: Identifier, configuration: ProducerConfiguration = ProducerConfiguration()) {
        self.init(backend: client, stream: stream, topic: topic, configuration: configuration, logger: client.logger)
    }

    init(backend: any MessagingBackend, stream: Identifier, topic: Identifier, configuration: ProducerConfiguration, logger: Logger) {
        streamID = stream
        topicID = topic
        self.configuration = configuration
        let directOptions: DirectSendOptions?
        switch configuration.mode {
        case .direct(let options): directOptions = options
        case .background: directOptions = nil
        }
        core = ProducerCore(backend: backend, streamID: stream, topicID: topic, configuration: configuration, directOptions: directOptions, logger: logger)
        switch configuration.mode {
        case .direct: dispatcher = nil
        case .background(let options): dispatcher = ProducerDispatcher(core: core, options: options, logger: logger)
        }
    }

    public var isInitialized: Bool {
        get async { await core.isInitialized }
    }

    /// Makes the producer ready to send: the stream and topic are looked up
    /// and created when missing and allowed by the configuration, and the
    /// producer starts following the client's lifecycle events. Calling it
    /// again after success does nothing.
    public func initialize() async throws {
        try await core.initialize()
    }

    /// Sends `messages` to the producer's stream and topic with its configured
    /// partitioning. Empty input is a no-op with no confirmations.
    ///
    /// A direct producer returns once the server answered every request the
    /// batch was split into and throws ``ProducerSendError`` on failure. A
    /// background producer returns once the batch is queued, always with an
    /// empty confirmation list, and throws only what queueing ran into:
    /// ``IggyErrorCode/producerClosed``, ``IggyErrorCode/backgroundSendBufferOverflow``,
    /// ``IggyErrorCode/backgroundSendTimeout`` or ``IggyErrorCode/backgroundSendError``.
    @discardableResult
    public func send(_ messages: [IggyMessage]) async throws -> SendMessagesResponse {
        try await send(messages, to: streamID, topicID: topicID, partitioning: nil)
    }

    /// Sends one message; see ``send(_:)``.
    @discardableResult
    public func send(_ message: IggyMessage) async throws -> SendMessagesResponse {
        try await send([message])
    }

    /// Sends with a per-call `partitioning`, or the configured one when nil.
    @discardableResult
    public func send(_ messages: [IggyMessage], partitioning: Partitioning?) async throws -> SendMessagesResponse {
        try await send(messages, to: streamID, topicID: topicID, partitioning: partitioning)
    }

    /// Sends to any stream and topic. The target has to exist, since
    /// ``initialize()`` only creates the producer's own.
    @discardableResult
    public func send(
        _ messages: [IggyMessage], to streamID: Identifier, topicID: Identifier, partitioning: Partitioning? = nil
    ) async throws -> SendMessagesResponse {
        guard !messages.isEmpty else {
            return SendMessagesResponse()
        }
        if let dispatcher {
            try await dispatcher.dispatch(streamID: streamID, topicID: topicID, messages: messages, partitioning: partitioning)
            return SendMessagesResponse()
        }
        return try await core.sendInternal(streamID: streamID, topicID: topicID, messages: messages, partitioning: partitioning)
    }

    /// Flushes what a background producer still holds, waits for the writes
    /// and error handlers to finish, and stops following client events. A
    /// direct producer only stops following events. Stop every sender first:
    /// a send racing the shutdown can be lost without an error.
    public func shutdown() async {
        await dispatcher?.shutdown()
        await core.shutdown()
    }
}

/// The state shared by both send modes: readiness, retries, chunking, and the
/// stream and topic bootstrap.
actor ProducerCore {
    /// Requests are capped at a million messages when chunking is disabled.
    static let maxBatchLength = 1_000_000

    private let backend: any MessagingBackend
    private let streamID: Identifier
    private let topicID: Identifier
    private let configuration: ProducerConfiguration
    private let directOptions: DirectSendOptions?
    private let logger: Logger

    private(set) var isInitialized = false
    /// Follows the client's events once initialized. Checked only while a
    /// retry budget exists, so a producer without retries sends regardless.
    private(set) var canSend = true
    private var lastSentAt: ContinuousClock.Instant?
    private var eventsTask: Task<Void, Never>?

    init(
        backend: any MessagingBackend, streamID: Identifier, topicID: Identifier, configuration: ProducerConfiguration, directOptions: DirectSendOptions?,
        logger: Logger
    ) {
        self.backend = backend
        self.streamID = streamID
        self.topicID = topicID
        self.configuration = configuration
        self.directOptions = directOptions
        self.logger = logger
    }

    func initialize() async throws {
        if isInitialized {
            return
        }
        logger.info("Initializing the producer", metadata: ["stream": "\(streamID)", "topic": "\(topicID)"])
        subscribeEvents()
        if try await backend.getStream(streamID) == nil {
            guard configuration.createStreamIfNotExists else {
                throw IggyError(.streamNameNotFound, context: "stream \(streamID) does not exist and auto-creation is disabled")
            }
            guard let name = streamID.name else {
                throw IggyError(.streamIdNotFound, context: "stream \(streamID) does not exist and a numeric id cannot be created")
            }
            logger.info("Creating the stream", metadata: ["stream": "\(name)"])
            _ = try await backend.createStream(name: name)
        }
        if try await backend.getTopic(streamID: streamID, topicID: topicID) == nil {
            guard configuration.createTopicIfNotExists else {
                throw IggyError(.topicNameNotFound, context: "topic \(topicID) does not exist in stream \(streamID) and auto-creation is disabled")
            }
            guard let name = topicID.name else {
                throw IggyError(.topicIdNotFound, context: "topic \(topicID) does not exist and a numeric id cannot be created")
            }
            logger.info("Creating the topic", metadata: ["stream": "\(streamID)", "topic": "\(name)"])
            let options = TopicCreateOptions(
                partitionsCount: configuration.topicPartitionsCount,
                messageExpiry: configuration.topicMessageExpiry == .serverDefault ? nil : configuration.topicMessageExpiry,
                maxTopicSize: configuration.topicMaxSize == .serverDefault ? nil : configuration.topicMaxSize)
            _ = try await backend.createTopic(streamID: streamID, name: name, options: options)
        }
        isInitialized = true
        logger.info("The producer has been initialized", metadata: ["stream": "\(streamID)", "topic": "\(topicID)"])
    }

    func shutdown() {
        eventsTask?.cancel()
        eventsTask = nil
    }

    /// Sends `messages`; a direct producer chunks them and returns every
    /// chunk's confirmations in order, a background worker sends them whole.
    /// Throws ``ProducerSendError`` naming the unconfirmed tail.
    func sendInternal(streamID: Identifier, topicID: Identifier, messages: [IggyMessage], partitioning: Partitioning?) async throws -> SendMessagesResponse {
        guard !messages.isEmpty else {
            return SendMessagesResponse()
        }
        let resolved = partitioning ?? configuration.partitioning ?? .balanced
        guard let directOptions else {
            do {
                let response = try await trySend(streamID: streamID, topicID: topicID, partitioning: resolved, messages: messages)
                lastSentAt = .now
                return response
            } catch {
                throw ProducerSendError(cause: Self.iggyError(error), failed: messages, committed: [], streamID: streamID, topicID: topicID)
            }
        }
        if directOptions.lingerTime > .zero, let lastSentAt {
            let elapsed = ContinuousClock.now - lastSentAt
            if elapsed < directOptions.lingerTime {
                try? await Task.sleep(for: directOptions.lingerTime - elapsed)
            }
        }
        let chunkLength = directOptions.batchLength == 0 ? Self.maxBatchLength : Int(directOptions.batchLength)
        var confirmations: [SendConfirmation] = []
        var index = 0
        while index < messages.count {
            let end = min(index + chunkLength, messages.count)
            do {
                let response = try await trySend(streamID: streamID, topicID: topicID, partitioning: resolved, messages: Array(messages[index..<end]))
                confirmations.append(contentsOf: response.confirmations)
            } catch {
                throw ProducerSendError(
                    cause: Self.iggyError(error), failed: Array(messages[index...]), committed: confirmations, streamID: streamID, topicID: topicID)
            }
            lastSentAt = .now
            index = end
        }
        return SendMessagesResponse(confirmations: confirmations)
    }

    private func trySend(streamID: Identifier, topicID: Identifier, partitioning: Partitioning, messages: [IggyMessage]) async throws -> SendMessagesResponse {
        guard let maxRetries = configuration.sendRetries, maxRetries > 0 else {
            return try await backend.sendMessages(streamID: streamID, topicID: topicID, partitioning: partitioning, messages: messages)
        }
        var retries: UInt32 = 0
        while !canSend {
            retries += 1
            if retries > maxRetries {
                logger.error(
                    "The client is disconnected, giving up sending", metadata: ["stream": "\(streamID)", "topic": "\(topicID)", "retries": "\(maxRetries)"])
                throw IggyError(.cannotSendMessagesDueToClientDisconnection)
            }
            logger.warning(
                "The client is disconnected, retrying the send",
                metadata: ["stream": "\(streamID)", "topic": "\(topicID)", "retry": "\(retries)/\(maxRetries)"])
            if let interval = configuration.sendRetryInterval {
                try? await Task.sleep(for: interval)
            }
        }
        retries = 0
        while true {
            do {
                return try await backend.sendMessages(streamID: streamID, topicID: topicID, partitioning: partitioning, messages: messages)
            } catch {
                retries += 1
                if retries > maxRetries {
                    logger.error(
                        "Failed to send messages, giving up",
                        metadata: ["stream": "\(streamID)", "topic": "\(topicID)", "retries": "\(maxRetries)", "error": "\(error)"])
                    throw error
                }
                logger.warning(
                    "Failed to send messages, retrying",
                    metadata: ["stream": "\(streamID)", "topic": "\(topicID)", "retry": "\(retries)/\(maxRetries)", "error": "\(error)"])
                if let interval = configuration.sendRetryInterval {
                    try? await Task.sleep(for: interval)
                }
            }
        }
    }

    /// The gate starts open and follows the events seen after initialization:
    /// a connection, disconnection, sign-out, or shutdown closes it and only a
    /// sign-in opens it again.
    private func subscribeEvents() {
        eventsTask?.cancel()
        eventsTask = Task { [weak self, backend] in
            let events = await backend.events
            for await event in events {
                guard let self else { return }
                await self.handle(event)
                if event == .shutdown {
                    return
                }
            }
        }
    }

    private func handle(_ event: DiagnosticEvent) {
        switch event {
        case .signedIn:
            canSend = true
        case .connected, .disconnected, .signedOut:
            canSend = false
        case .shutdown:
            canSend = false
            logger.warning("The client has been shut down, the producer cannot send anymore")
        }
    }

    static func iggyError(_ error: any Error) -> IggyError {
        (error as? IggyError) ?? IggyError(.error, context: "\(error)")
    }
}

extension IggyMessage {
    /// The bytes a message costs on the wire, used for the background
    /// producer's byte budget and flush trigger.
    var sizeBytes: Int {
        Batch.messageHeaderSize + payload.count + (rawUserHeaders?.count ?? 0)
    }
}
