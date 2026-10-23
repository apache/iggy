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

private func signedInClient(_ server: FakeVSRServer, requestTimeout: Duration = .seconds(10)) async throws -> IggyClient {
    var configuration = ClientConfiguration(address: server.address, autoLogin: .usernamePassword(username: "iggy", password: "iggy"))
    configuration.reconnection = ReconnectionOptions(maxRetries: nil, interval: .milliseconds(20), reestablishAfter: .zero)
    configuration.requestTimeout = requestTimeout
    configuration.connectTimeout = .seconds(2)
    configuration.heartbeatInterval = .seconds(60)
    let client = IggyClient(configuration: configuration, connector: NIOConnector(), logger: testLogger)
    try await client.connect()
    return client
}

private func sends(_ server: FakeVSRServer) async -> [FakeVSRServer.Request] {
    await server.requests.filter { $0.code == CommandCode.sendMessages.rawValue }
}

/// The messages count of a send to a numeric stream and topic with an
/// explicit partition: `[metadata_len:4][stream:6][topic:6][partitioning:6][count:4]`.
private func sentCount(_ request: FakeVSRServer.Request) -> UInt32 {
    UInt32(littleEndianBytes: request.payload[22..<26])
}

/// The numeric stream and topic ids of a send: `[metadata_len:4][kind:1][len:1][stream:4][kind:1][len:1][topic:4]`.
private func sentDestination(_ request: FakeVSRServer.Request) -> [UInt32] {
    [UInt32(littleEndianBytes: request.payload[6..<10]), UInt32(littleEndianBytes: request.payload[12..<16])]
}

private func sentPartition(_ request: FakeVSRServer.Request) -> UInt32 {
    UInt32(littleEndianBytes: request.payload[18..<22])
}

private func carries(_ request: FakeVSRServer.Request, _ id: MessageID) -> Bool {
    var writer = ByteWriter()
    writer.write(id)
    let needle = writer.bytes
    return request.payload.indices.contains { request.payload[$0...].starts(with: needle) }
}

private func texts(_ count: Int) -> [IggyMessage] {
    (0..<count).map { IggyMessage(stringLiteral: "message \($0)") }
}

@Suite("Producer", .serialized)
struct ProducerTests {
    // MARK: Initialization

    @Test func initializeCreatesANamedStreamAndTopic() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await server.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .createStream: return .ok(GoldenFixture.shared.bytes("response.get_stream"))
            case .createTopic: return .ok(GoldenFixture.shared.bytes("response.get_topic"))
            default: return FakeVSRServer.defaultReply(request, address: server.address)
            }
        }
        let client = try await signedInClient(server)
        var configuration = ProducerConfiguration(topicPartitionsCount: 3, topicDurability: .persisted)
        configuration.sendRetries = nil
        let producer = try client.producer(streamID: "orders", topicID: "created", configuration: configuration)
        try await producer.initialize()
        try await producer.initialize()
        let creates = await server.requests.filter { $0.code == CommandCode.createStream.rawValue || $0.code == CommandCode.createTopic.rawValue }
        #expect(
            creates.map(\.payload) == [
                try Requests.createStream(name: "orders"),
                try Requests.createTopic(
                    streamID: "orders", partitionsCount: 3, name: "created",
                    options: try TopicCreateOptions(partitionsCount: 3, durability: .persisted).encode()),
            ])
        #expect(await server.requests.filter { $0.code == CommandCode.getStream.rawValue }.count == 1)
        await client.shutdown()
    }

    /// A numeric id cannot be created, which is reported before the
    /// auto-creation flag is consulted, as in the Rust SDK.
    @Test func initializeReportsWhatCannotBeCreated() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = try await signedInClient(server)
        await expectCode(.streamIdNotFound) { try await client.producer(streamID: 7, topicID: "created").initialize() }
        let noStreams = ProducerConfiguration(createStreamIfNotExists: false)
        await expectCode(.streamIdNotFound) { try await client.producer(streamID: 7, topicID: "created", configuration: noStreams).initialize() }
        await expectCode(.streamNameNotFound) { try await client.producer(streamID: "orders", topicID: "created", configuration: noStreams).initialize() }
        await server.respond { request in
            request.code == CommandCode.getStream.rawValue
                ? .ok(GoldenFixture.shared.bytes("response.get_stream")) : FakeVSRServer.defaultReply(request, address: server.address)
        }
        await expectCode(.topicIdNotFound) { try await client.producer(streamID: "orders", topicID: 9).initialize() }
        let noTopics = ProducerConfiguration(createTopicIfNotExists: false)
        await expectCode(.topicIdNotFound) { try await client.producer(streamID: "orders", topicID: 9, configuration: noTopics).initialize() }
        await expectCode(.topicNameNotFound) { try await client.producer(streamID: "orders", topicID: "created", configuration: noTopics).initialize() }
        #expect(await server.requests.filter { $0.code == CommandCode.createStream.rawValue || $0.code == CommandCode.createTopic.rawValue }.isEmpty)
        await client.shutdown()
    }

    /// Another producer creating the stream or topic between the lookup and
    /// the create is not a failure.
    @Test func aStreamOrTopicCreatedMeanwhileCountsAsCreated() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await server.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .createStream: return .status(IggyErrorCode.streamNameAlreadyExists.rawValue)
            case .createTopic: return .status(IggyErrorCode.topicNameAlreadyExists.rawValue)
            default: return FakeVSRServer.defaultReply(request, address: server.address)
            }
        }
        let client = try await signedInClient(server)
        try await client.producer(streamID: "orders", topicID: "created").initialize()
        #expect(await server.requests.filter { $0.code == CommandCode.createTopic.rawValue }.count == 1)
        await client.shutdown()
    }

    @Test func negativeOptionsAreRefusedWhenTheProducerIsBuilt() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = try await signedInClient(server)
        var invalid: [BackgroundSendOptions] = []
        for field in [\BackgroundSendOptions.batchSize, \.batchLength, \.maxBufferSize, \.maxInFlight] {
            var options = BackgroundSendOptions()
            options[keyPath: field] = -1
            invalid.append(options)
        }
        invalid.append(BackgroundSendOptions(workerCount: 0))
        invalid.append(BackgroundSendOptions(lingerTime: .milliseconds(-1)))
        invalid.append(BackgroundSendOptions(backpressure: .blockWithTimeout(.milliseconds(-1))))
        var configurations = invalid.map { ProducerConfiguration(mode: .background($0)) }
        configurations.append(ProducerConfiguration(mode: .direct(DirectSendOptions(lingerTime: .milliseconds(-1)))))
        configurations.append(ProducerConfiguration(sendRetryInterval: .milliseconds(-1)))
        // Past what `IggyDuration` holds, where `Task.sleep` would trap.
        let tooLong = ProducerConfiguration.maxDuration + .microseconds(1)
        configurations.append(ProducerConfiguration(sendRetryInterval: tooLong))
        configurations.append(ProducerConfiguration(mode: .direct(DirectSendOptions(lingerTime: tooLong))))
        configurations.append(ProducerConfiguration(mode: .background(BackgroundSendOptions(lingerTime: tooLong))))
        configurations.append(ProducerConfiguration(mode: .background(BackgroundSendOptions(backpressure: .blockWithTimeout(tooLong)))))
        configurations.append(ProducerConfiguration(topicPartitionsCount: 0))
        for configuration in configurations {
            await expectCode(.invalidConfiguration) { _ = try client.producer(streamID: 1, topicID: 2, configuration: configuration) }
        }
        let unbounded = BackgroundSendOptions(batchSize: 0, batchLength: 0, lingerTime: .zero, maxBufferSize: 0, maxInFlight: 0)
        _ = try client.producer(streamID: 1, topicID: 2, configuration: ProducerConfiguration(mode: .background(unbounded)))
        let longest = BackgroundSendOptions(lingerTime: ProducerConfiguration.maxDuration)
        _ = try client.producer(streamID: 1, topicID: 2, configuration: ProducerConfiguration(mode: .background(longest)))
        _ = try client.producer(streamID: 1, topicID: 2, configuration: ProducerConfiguration(createTopicIfNotExists: false, topicPartitionsCount: 0))
        await client.shutdown()
    }

    // MARK: Direct sends

    @Test func aDirectSendIsSplitIntoRequestsAndReturnsTheirConfirmations() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let offset = Locked<UInt64>(0)
        await server.respond { request in
            guard request.code == CommandCode.sendMessages.rawValue else { return FakeVSRServer.defaultReply(request, address: server.address) }
            let base = offset.withLock { offset in
                defer { offset += UInt64(sentCount(request)) }
                return offset
            }
            return .ok(FakeVSRServer.confirmationBody(partitionID: 1, baseOffset: base))
        }
        let client = try await signedInClient(server)
        let configuration = ProducerConfiguration(partitioning: .partition(1), mode: .direct(DirectSendOptions(batchLength: 2)))
        let producer = try client.producer(streamID: 1, topicID: 2, configuration: configuration)
        let response = try await producer.send(texts(5))
        #expect(await sends(server).map(sentCount) == [2, 2, 1])
        #expect(response.confirmations.map(\.baseOffset) == [0, 2, 4])
        await client.shutdown()
    }

    /// The tail of a failed send carries the ids the client minted for it,
    /// and the confirmations of the requests before it are kept.
    @Test func aFailedDirectSendNamesItsUnconfirmedTail() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let attempts = Locked(0)
        await server.respond { request in
            guard request.code == CommandCode.sendMessages.rawValue else { return FakeVSRServer.defaultReply(request, address: server.address) }
            let attempt = attempts.withLock {
                $0 += 1; return $0
            }
            return attempt == 1
                ? .ok(FakeVSRServer.confirmationBody(partitionID: 1, baseOffset: 0)) : .status(IggyErrorCode.partitionNotFound.rawValue)
        }
        let client = try await signedInClient(server)
        let configuration = ProducerConfiguration(partitioning: .partition(1), mode: .direct(DirectSendOptions(batchLength: 2)))
        let producer = try client.producer(streamID: 1, topicID: 2, configuration: configuration)
        do {
            try await producer.send(texts(5))
            Issue.record("the send did not fail")
        } catch let error as ProducerSendError {
            #expect(error.cause.code == .partitionNotFound)
            #expect(error.committed.map(\.baseOffset) == [0])
            #expect(error.failed.map(\.payloadString) == ["message 2", "message 3", "message 4"])
            // The messages of the failed request carry the ids it was sent
            // with; the one never sent has none yet.
            #expect(error.failed.map { !$0.id.isZero } == [true, true, false])
            let failedRequest = try #require(await sends(server).last)
            #expect(carries(failedRequest, error.failed[0].id) && carries(failedRequest, error.failed[1].id))
        }
        // A refusal that is not a transient admission refusal is not retried.
        #expect(await sends(server).count == 2)
        await client.shutdown()
    }

    /// A cancelled send is reported like a failed one, since the requests
    /// before it committed and the one it interrupted may have.
    @Test func aCancelledDirectSendNamesItsUnconfirmedTail() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let attempts = Locked(0)
        await server.respond { request in
            guard request.code == CommandCode.sendMessages.rawValue else { return FakeVSRServer.defaultReply(request, address: server.address) }
            let attempt = attempts.withLock {
                $0 += 1; return $0
            }
            return attempt == 1 ? .ok(FakeVSRServer.confirmationBody(partitionID: 1, baseOffset: 0)) : .stall
        }
        let client = try await signedInClient(server)
        var configuration = ProducerConfiguration(partitioning: .partition(1), mode: .direct(DirectSendOptions(batchLength: 2)))
        configuration.sendRetries = nil
        let producer = try client.producer(streamID: 1, topicID: 2, configuration: configuration)
        let send = Task { try await producer.send(texts(5)) }
        #expect(await eventually { await sends(server).count == 2 })
        send.cancel()
        do {
            _ = try await send.value
            Issue.record("the send did not fail")
        } catch let error as ProducerSendError {
            #expect(error.underlying is CancellationError)
            #expect(error.committed.map(\.baseOffset) == [0])
            #expect(error.failed.map(\.payloadString) == ["message 2", "message 3", "message 4"])
            #expect(error.failed.map { !$0.id.isZero } == [true, true, false])
            let interrupted = try #require(await sends(server).last)
            #expect(carries(interrupted, error.failed[0].id) && carries(interrupted, error.failed[1].id))
        }
        await client.shutdown()
    }

    /// A send that found the connection closed before writing is replayed by
    /// the client after the reconnect. When the replay is written and lost,
    /// its outcome is unknown, so the producer must not send it again: the
    /// server deduplicates only the client's own replays, which keep the
    /// request id, never a retry under a new one.
    @Test func aLostWriteIsNeverSentUnderANewRequestID() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let routeReads = Locked(0)
        await server.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .getPartitionRoute:
                let first = routeReads.withLock {
                    $0 += 1; return $0 == 1
                }
                // The connection drops right after the route read, so the
                // send finds it closed before writing anything.
                return first
                    ? .okThenClose(FakeVSRServer.partitionRouteBody(FakeVSRServer.routeContext))
                    : FakeVSRServer.defaultReply(request, address: server.address)
            case .sendMessages:
                return .close
            default:
                return FakeVSRServer.defaultReply(request, address: server.address)
            }
        }
        let client = try await signedInClient(server, requestTimeout: .milliseconds(500))
        let configuration = ProducerConfiguration(partitioning: .partition(1), sendRetries: 3, sendRetryInterval: nil)
        let producer = try client.producer(streamID: 1, topicID: 2, configuration: configuration)
        do {
            try await producer.send(["once"])
            Issue.record("the send did not fail")
        } catch let error as ProducerSendError {
            #expect(ProducerCore.stopsSendRetries(error.cause))
            #expect(error.failed.map(\.payloadString) == ["once"])
        }
        let writes = await sends(server)
        #expect(!writes.isEmpty)
        #expect(Set(writes.map { "\($0.session)/\($0.requestID)" }).count == 1)
        await client.shutdown()
    }

    /// A direct send in progress finishes the request it is writing when the
    /// producer shuts down, and stops before the next one.
    @Test func aShutdownStopsADirectSendBetweenRequests() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await server.respond { request in
            guard request.code == CommandCode.sendMessages.rawValue else { return FakeVSRServer.defaultReply(request, address: server.address) }
            try? await Task.sleep(for: .milliseconds(200))
            return .ok(FakeVSRServer.confirmationBody(partitionID: 1, baseOffset: 0))
        }
        let client = try await signedInClient(server)
        let configuration = ProducerConfiguration(partitioning: .partition(1), mode: .direct(DirectSendOptions(batchLength: 1)))
        let producer = try client.producer(streamID: 1, topicID: 2, configuration: configuration)
        let send = Task { try await producer.send(texts(3)) }
        #expect(await eventually { await sends(server).count == 1 })
        await producer.shutdown()
        do {
            _ = try await send.value
            Issue.record("the send did not fail")
        } catch let error as ProducerSendError {
            #expect(error.cause.code == .producerClosed)
            #expect(error.committed.count == 1)
            #expect(error.failed.map(\.payloadString) == ["message 1", "message 2"])
        }
        #expect(await sends(server).count == 1)
        await client.shutdown()
    }

    /// A direct send is pinned to the session it started in: once the caller
    /// signs in again, the requests not written yet fail as unauthenticated
    /// instead of going under the new session.
    @Test func aSignInEndsTheUnsentTailOfADirectSend() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await server.respond { request in
            guard request.code == CommandCode.sendMessages.rawValue else { return FakeVSRServer.defaultReply(request, address: server.address) }
            try? await Task.sleep(for: .milliseconds(200))
            return .ok(FakeVSRServer.confirmationBody(partitionID: 1, baseOffset: 0))
        }
        let client = try await signedInClient(server)
        let configuration = ProducerConfiguration(partitioning: .partition(1), mode: .direct(DirectSendOptions(batchLength: 1)))
        let producer = try client.producer(streamID: 1, topicID: 2, configuration: configuration)
        let send = Task { try await producer.send(["head", "tail"]) }
        #expect(await eventually { await sends(server).count == 1 })
        _ = try await client.login(username: "iggy", password: "iggy")
        do {
            _ = try await send.value
            Issue.record("the send did not fail")
        } catch let error as ProducerSendError {
            #expect(error.cause.code == .unauthenticated)
            #expect(error.committed.count == 1)
            #expect(error.failed.map(\.payloadString) == ["tail"])
        }
        #expect(await sends(server).count == 1)
        await client.shutdown()
    }

    /// A retry waiting out its interval when the caller signs in again is
    /// not written under the new session.
    @Test func aSignInDuringTheRetryWaitEndsTheSend() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await server.respond { request in
            request.code == CommandCode.sendMessages.rawValue
                ? .status(IggyErrorCode.transientNotAccepted.rawValue) : FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = try await signedInClient(server, requestTimeout: .milliseconds(200))
        let configuration = ProducerConfiguration(partitioning: .partition(1), sendRetries: 3, sendRetryInterval: .seconds(1))
        let producer = try client.producer(streamID: 1, topicID: 2, configuration: configuration)
        let send = Task { try await producer.send(["once"]) }
        // The first retry goes at once; the second waits out the interval.
        try await Task.sleep(for: .milliseconds(600))
        _ = try await client.login(username: "iggy", password: "iggy")
        let writtenBefore = await sends(server).count
        do {
            _ = try await send.value
            Issue.record("the send did not fail")
        } catch let error as ProducerSendError {
            #expect(error.cause.code == .unauthenticated)
        }
        #expect(writtenBefore > 0)
        #expect(await sends(server).count == writtenBefore)
        await client.shutdown()
    }

    /// Batches queued before a sign-in keep their session and are never
    /// merged with the ones queued after it.
    @Test func aBackgroundBatchQueuedBeforeASignInKeepsItsSession() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = try await signedInClient(server)
        let reported = Locked<[ProducerSendFailure]>([])
        let options = BackgroundSendOptions(lingerTime: .seconds(60), errorHandler: { failure in reported.withLock { $0.append(failure) } })
        let producer = try client.producer(
            streamID: 1, topicID: 2, configuration: ProducerConfiguration(partitioning: .partition(1), mode: .background(options)))
        try await producer.send(["old"])
        _ = try await client.login(username: "iggy", password: "iggy")
        try await producer.send(["new"])
        await producer.shutdown()
        let failures = reported.withLock { $0 }
        #expect(failures.map(\.cause.code) == [.unauthenticated])
        #expect(failures.first?.messages.map(\.payloadString) == ["old"])
        #expect(await sends(server).map(sentCount) == [1])
        await client.shutdown()
    }

    /// The bootstrap is pinned too: a sign-in while it runs creates nothing
    /// under the new session.
    @Test func aSignInEndsTheInitialization() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await server.respond { request in
            if request.code == CommandCode.getStream.rawValue {
                try? await Task.sleep(for: .milliseconds(200))
            }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = try await signedInClient(server)
        let producer = try client.producer(streamID: "orders", topicID: "created")
        let initialization = Task { try await producer.initialize() }
        #expect(await eventually { await server.requests.contains { $0.code == CommandCode.getStream.rawValue } })
        _ = try await client.login(username: "iggy", password: "iggy")
        await expectCode(.unauthenticated) { try await initialization.value }
        #expect(await server.requests.filter { $0.code == CommandCode.createStream.rawValue }.isEmpty)
        await client.shutdown()
    }

    /// A request carries at most `batchLength` messages and stays within the
    /// server's frame limit; a message too large for one goes alone.
    @Test func aDirectSendIsChunkedByLengthAndBytes() throws {
        let messages = try (0..<5).map { _ in try IggyMessage(payload: [UInt8](repeating: 1, count: 10)) }
        let size = Batch.messageHeaderSize + 10
        #expect(ProducerCore.chunkEnd(messages, from: 0, length: 10, budget: 2 * size) == 2)
        #expect(ProducerCore.chunkEnd(messages, from: 2, length: 10, budget: 2 * size) == 4)
        #expect(ProducerCore.chunkEnd(messages, from: 4, length: 10, budget: 2 * size) == 5)
        #expect(ProducerCore.chunkEnd(messages, from: 0, length: 3, budget: 100 * size) == 3)
        #expect(ProducerCore.chunkEnd(messages, from: 0, length: 10, budget: size - 1) == 1)
        let metadata = 4 + 6 + 6 + 6 + 4
        #expect(ProducerCore.requestBudget(streamID: 1, topicID: 2) == 64 * 1024 * 1024 - VSRFrame.headerSize - metadata - Batch.headerSize)
    }

    @Test func aSendCanTargetAnotherStreamAndTopic() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = try await signedInClient(server)
        let direct = try client.producer(streamID: 1, topicID: 2, configuration: ProducerConfiguration(partitioning: .partition(1)))
        try await direct.send(["direct"], streamID: 3, topicID: 4)
        let background = try client.producer(
            streamID: 1, topicID: 2, configuration: ProducerConfiguration(partitioning: .partition(1), mode: .background(BackgroundSendOptions())))
        try await background.send(["background"], streamID: 5, topicID: 6, partitioning: .partition(2))
        await background.shutdown()
        #expect(await sends(server).map(sentDestination) == [[3, 4], [5, 6]])
        #expect(await sends(server).map(sentPartition) == [1, 2])
        await client.shutdown()
    }

    @Test func aDirectProducerIsClosedByShutdown() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = try await signedInClient(server)
        let producer = try client.producer(streamID: 1, topicID: 2, configuration: ProducerConfiguration(partitioning: .partition(1)))
        try await producer.send(["before"])
        await producer.shutdown()
        await expectCode(.producerClosed) { try await producer.send(["after"]) }
        await expectCode(.producerClosed) { try await producer.initialize() }
        #expect(await sends(server).count == 1)
        await client.shutdown()
    }

    /// A send the server refused to admit is retried as a new request with
    /// the ids of the first attempt; the first retry goes at once.
    @Test func anAdmissionRefusalIsRetriedWithTheSameMessageIDs() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let refuseUntil = Locked<ContinuousClock.Instant?>(nil)
        await server.respond { request in
            guard request.code == CommandCode.sendMessages.rawValue else { return FakeVSRServer.defaultReply(request, address: server.address) }
            let until = refuseUntil.withLock { until in
                if until == nil { until = .now + .milliseconds(250) }
                return until!
            }
            return ContinuousClock.now < until ? .status(IggyErrorCode.transientNotAccepted.rawValue) : .ok([])
        }
        let client = try await signedInClient(server, requestTimeout: .milliseconds(300))
        let configuration = ProducerConfiguration(partitioning: .partition(1), sendRetries: 2, sendRetryInterval: .seconds(30))
        let producer = try client.producer(streamID: 1, topicID: 2, configuration: configuration)
        let clock = ContinuousClock()
        let elapsed = try await clock.measure { try await producer.send(["once"]) }
        #expect(elapsed < .seconds(5))
        let attempts = await sends(server)
        #expect(Set(attempts.map(\.requestID)).count == 2)
        let first = try #require(attempts.first)
        #expect(attempts.allSatisfy { $0.payload == first.payload })
        await client.shutdown()
    }

    @Test func onlyRefusalsBeforeAnythingWasWrittenAreRetried() {
        for code: IggyErrorCode in [.transientNotAccepted, .notConnected, .cannotEstablishConnection] {
            #expect(!ProducerCore.stopsSendRetries(IggyError(code)))
        }
        for code: IggyErrorCode in [.transientNotCommitted, .disconnected, .requestTooOld, .historyUnavailable, .invalidCommand] {
            #expect(ProducerCore.stopsSendRetries(IggyError(code)))
        }
    }

    /// While the client is signed out the producer waits for a sign-in, up
    /// to its retry budget, without writing anything.
    @Test func aSignedOutClientIsWaitedForWithinTheRetryBudget() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await server.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .getStream: return .ok(GoldenFixture.shared.bytes("response.get_stream"))
            case .getTopic: return .ok(GoldenFixture.shared.bytes("response.get_topic"))
            default: return FakeVSRServer.defaultReply(request, address: server.address)
            }
        }
        let client = try await signedInClient(server)
        let configuration = ProducerConfiguration(partitioning: .partition(1), sendRetries: 2, sendRetryInterval: .milliseconds(10))
        let producer = try client.producer(streamID: 1, topicID: 2, configuration: configuration)
        try await producer.initialize()
        try await client.logout()
        #expect(await eventually { await !producer.core.canSend })
        do {
            try await producer.send(["late"])
            Issue.record("the send did not fail")
        } catch let error as ProducerSendError {
            #expect(error.cause.code == .cannotSendMessagesDueToClientDisconnection)
            #expect(error.failed.count == 1)
        }
        #expect(await sends(server).isEmpty)
        _ = try await client.login(username: "iggy", password: "iggy")
        #expect(await eventually { await producer.core.canSend })
        try await producer.send(["on time"])
        #expect(await sends(server).count == 1)
        await producer.shutdown()
        await client.shutdown()
    }

    /// A client left disconnected by a reply timeout, with no heartbeat to
    /// bring it back, is reconnected by the wait for a sign-in.
    @Test func theSignInWaitReconnectsADroppedClient() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let stallPing = Locked(true)
        await server.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .ping where stallPing.withLock({ $0 }): return .stall
            case .getStream: return .ok(GoldenFixture.shared.bytes("response.get_stream"))
            case .getTopic: return .ok(GoldenFixture.shared.bytes("response.get_topic"))
            default: return FakeVSRServer.defaultReply(request, address: server.address)
            }
        }
        let client = try await signedInClient(server, requestTimeout: .milliseconds(300))
        let configuration = ProducerConfiguration(partitioning: .partition(1), sendRetries: 2, sendRetryInterval: .milliseconds(10))
        let producer = try client.producer(streamID: 1, topicID: 2, configuration: configuration)
        try await producer.initialize()
        await #expect(throws: IggyError.self) { try await client.ping() }
        stallPing.withLock { $0 = false }
        #expect(await client.state == .disconnected)
        #expect(await eventually { await !producer.core.canSend })
        try await producer.send(["after the outage"])
        #expect(await sends(server).count == 1)
        await producer.shutdown()
        await client.shutdown()
    }

    /// A shut-down client is not waited for: the send fails at once with
    /// the shutdown instead of spending its retry budget.
    @Test func theSignInWaitEndsAtAClientShutdown() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await server.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .getStream: return .ok(GoldenFixture.shared.bytes("response.get_stream"))
            case .getTopic: return .ok(GoldenFixture.shared.bytes("response.get_topic"))
            default: return FakeVSRServer.defaultReply(request, address: server.address)
            }
        }
        let client = try await signedInClient(server)
        let configuration = ProducerConfiguration(partitioning: .partition(1), sendRetries: 3, sendRetryInterval: .seconds(5))
        let producer = try client.producer(streamID: 1, topicID: 2, configuration: configuration)
        try await producer.initialize()
        await client.shutdown()
        #expect(await eventually { await !producer.core.canSend })
        let clock = ContinuousClock()
        let elapsed = await clock.measure {
            do {
                try await producer.send(["late"])
                Issue.record("the send did not fail")
            } catch let error as ProducerSendError {
                #expect(error.cause.code == .clientShutdown)
            } catch {
                Issue.record("unexpected \(error)")
            }
        }
        #expect(elapsed < .seconds(2))
        await producer.shutdown()
    }

    @Test func aPartitionerWinsOverAnyPartitioning() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = try await signedInClient(server)
        let configuration = ProducerConfiguration(partitioning: .partition(1), partitioner: { _, _, messages in UInt32(messages.count) })
        let producer = try client.producer(streamID: 1, topicID: 2, configuration: configuration)
        try await producer.send(texts(2), partitioning: .partition(0))
        #expect(await sends(server).map(sentPartition) == [2])

        struct Refused: Error {}
        let failing = try client.producer(streamID: 1, topicID: 2, configuration: ProducerConfiguration(partitioner: { _, _, _ in throw Refused() }))
        do {
            try await failing.send(texts(3))
            Issue.record("the send did not fail")
        } catch let error as ProducerSendError {
            #expect(error.failed.count == 3)
            #expect(error.committed.isEmpty)
            #expect(error.underlying is Refused)
        }
        #expect(await sends(server).count == 1)
        await client.shutdown()
    }

    @Test func lingerSpacesSequentialSends() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = try await signedInClient(server)
        let configuration = ProducerConfiguration(partitioning: .partition(1), mode: .direct(DirectSendOptions(lingerTime: .milliseconds(150))))
        let producer = try client.producer(streamID: 1, topicID: 2, configuration: configuration)
        let clock = ContinuousClock()
        try await producer.send(["first"])
        let elapsed = try await clock.measure { try await producer.send(["second"]) }
        #expect(elapsed >= .milliseconds(100))
        await client.shutdown()
    }

    @Test func retriesTickLikeAnInterval() async throws {
        var ticker = RetryTicker(interval: .milliseconds(100))
        let clock = ContinuousClock()
        let first = try await clock.measure { try await ticker.tick() }
        let second = try await clock.measure { try await ticker.tick() }
        #expect(first < .milliseconds(50))
        #expect(second >= .milliseconds(80))
        var immediate = RetryTicker(interval: nil)
        let none = try await clock.measure {
            for _ in 0..<5 { try await immediate.tick() }
        }
        #expect(none < .milliseconds(50))
    }

    // MARK: Background sends

    @Test func aBackgroundSendIsWrittenByShutdownAtTheLatest() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = try await signedInClient(server)
        let options = BackgroundSendOptions(lingerTime: .seconds(60))
        let producer = try client.producer(
            streamID: 1, topicID: 2, configuration: ProducerConfiguration(partitioning: .partition(1), mode: .background(options)))
        #expect(try await producer.send(["queued"]).confirmations.isEmpty)
        try await producer.send(["also queued"])
        #expect(await sends(server).isEmpty)
        await producer.shutdown()
        // Both sends share a destination, so they went as one request.
        #expect(await sends(server).map(sentCount) == [2])
        await expectCode(.producerClosed) { try await producer.send(["late"]) }
        await client.shutdown()
    }
}
