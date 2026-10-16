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
        let producer = client.producer(stream: "orders", topic: "created", configuration: configuration)
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
        await expectCode(.streamIdNotFound) { try await client.producer(stream: 7, topic: "created").initialize() }
        let noStreams = ProducerConfiguration(createStreamIfNotExists: false)
        await expectCode(.streamIdNotFound) { try await client.producer(stream: 7, topic: "created", configuration: noStreams).initialize() }
        await expectCode(.streamNameNotFound) { try await client.producer(stream: "orders", topic: "created", configuration: noStreams).initialize() }
        await server.respond { request in
            request.code == CommandCode.getStream.rawValue
                ? .ok(GoldenFixture.shared.bytes("response.get_stream")) : FakeVSRServer.defaultReply(request, address: server.address)
        }
        await expectCode(.topicIdNotFound) { try await client.producer(stream: "orders", topic: 9).initialize() }
        let noTopics = ProducerConfiguration(createTopicIfNotExists: false)
        await expectCode(.topicIdNotFound) { try await client.producer(stream: "orders", topic: 9, configuration: noTopics).initialize() }
        await expectCode(.topicNameNotFound) { try await client.producer(stream: "orders", topic: "created", configuration: noTopics).initialize() }
        #expect(await server.requests.filter { $0.code == CommandCode.createStream.rawValue || $0.code == CommandCode.createTopic.rawValue }.isEmpty)
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
        let producer = client.producer(stream: 1, topic: 2, configuration: configuration)
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
        let producer = client.producer(stream: 1, topic: 2, configuration: configuration)
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
        let producer = client.producer(stream: 1, topic: 2, configuration: configuration)
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
        let producer = client.producer(stream: 1, topic: 2, configuration: configuration)
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

    @Test func aPartitionerWinsOverAnyPartitioning() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = try await signedInClient(server)
        let configuration = ProducerConfiguration(partitioning: .partition(1), partitioner: { _, _, messages in UInt32(messages.count) })
        let producer = client.producer(stream: 1, topic: 2, configuration: configuration)
        try await producer.send(texts(2), partitioning: .partition(0))
        #expect(await sends(server).map(sentPartition) == [2])

        struct Refused: Error {}
        let failing = client.producer(stream: 1, topic: 2, configuration: ProducerConfiguration(partitioner: { _, _, _ in throw Refused() }))
        do {
            try await failing.send(texts(3))
            Issue.record("the send did not fail")
        } catch let error as ProducerSendError {
            #expect(error.failed.count == 3)
            #expect(error.committed.isEmpty)
        }
        #expect(await sends(server).count == 1)
        await client.shutdown()
    }

    @Test func lingerSpacesSequentialSends() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = try await signedInClient(server)
        let configuration = ProducerConfiguration(partitioning: .partition(1), mode: .direct(DirectSendOptions(lingerTime: .milliseconds(150))))
        let producer = client.producer(stream: 1, topic: 2, configuration: configuration)
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
        let producer = client.producer(stream: 1, topic: 2, configuration: ProducerConfiguration(partitioning: .partition(1), mode: .background(options)))
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
