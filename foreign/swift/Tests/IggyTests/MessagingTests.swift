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

import Testing

@testable import Iggy

/// A signed-in client on the fake server.
private func connectedClient(
    _ server: FakeVSRServer, heartbeat: Duration = .seconds(60), requestTimeout: Duration = .seconds(10), connectTimeout: Duration = .seconds(2),
    connector: any IggyConnector = NIOConnector()
) async throws -> IggyClient {
    var configuration = ClientConfiguration(address: server.address, autoLogin: .usernamePassword(username: "iggy", password: "iggy"))
    configuration.reconnection = ReconnectionOptions(maxRetries: nil, interval: .milliseconds(20), reestablishAfter: .zero)
    configuration.requestTimeout = requestTimeout
    configuration.connectTimeout = connectTimeout
    configuration.heartbeatInterval = heartbeat
    let client = IggyClient(configuration: configuration, connector: connector, logger: testLogger)
    try await client.connect()
    return client
}

/// A dialer that records every endpoint before it dials it. The fake server
/// counts a connection only some time after the client's connect returned,
/// so a check for a wrong dial reads this record instead.
private struct RecordingConnector: IggyConnector {
    let dialed = Locked<[Endpoint]>([])

    func connect(to endpoint: Endpoint, tls: TLSOptions?, timeout: Duration, noDelay: Bool) async throws -> any IggyConnection {
        dialed.withLock { $0.append(endpoint) }
        return try await NIOConnector().connect(to: endpoint, tls: tls, timeout: timeout, noDelay: noDelay)
    }

    func hasDialed(_ server: FakeVSRServer) throws -> Bool {
        let endpoint = try Endpoint(parsing: server.address)
        return dialed.withLock { $0.contains(endpoint) }
    }
}

/// A dialer that gets no answer from `unanswering`, as from a host that
/// drops the SYN without a reset: the dial fails once its timeout passes.
/// Every other endpoint is dialed for real.
private struct UnansweringConnector: IggyConnector {
    let unanswering: Endpoint
    let timeouts = Locked<[Duration]>([])

    func connect(to endpoint: Endpoint, tls: TLSOptions?, timeout: Duration, noDelay: Bool) async throws -> any IggyConnection {
        guard endpoint == unanswering else {
            return try await NIOConnector().connect(to: endpoint, tls: tls, timeout: timeout, noDelay: noDelay)
        }
        timeouts.withLock { $0.append(timeout) }
        try await Task.sleep(for: timeout)
        throw IggyError(.cannotEstablishConnection, context: "\(endpoint) did not answer the dial")
    }
}

/// A dialer that holds the first dial to `slow` for the whole timeout the
/// client gave it, as a host that answers at the last moment, then dials it
/// for real. Every dial is recorded.
private struct SlowConnector: IggyConnector {
    let slow: Endpoint
    let dialed = Locked<[Endpoint]>([])

    func connect(to endpoint: Endpoint, tls: TLSOptions?, timeout: Duration, noDelay: Bool) async throws -> any IggyConnection {
        let first = dialed.withLock { dialed in
            defer { dialed.append(endpoint) }
            return !dialed.contains(endpoint)
        }
        if endpoint == slow, first {
            try await Task.sleep(for: timeout)
        }
        return try await NIOConnector().connect(to: endpoint, tls: tls, timeout: timeout, noDelay: noDelay)
    }
}

private func requests(_ server: FakeVSRServer, _ code: CommandCode) async -> [FakeVSRServer.Request] {
    await server.requests.filter { $0.code == code.rawValue }
}

/// An address where nothing listens, so a dial is refused at once. Port 1
/// is privileged and below the ephemeral range, so no fake server can take
/// it, unlike the port of a stopped server, which another one can bind.
private let unreachableAddress = "127.0.0.1:1"

/// The partition a send names: `[metadata_len:4][stream:6][topic:6][kind:1][length:1][partition:4]`
/// with numeric ids.
private func sentPartition(_ request: FakeVSRServer.Request) -> UInt32 {
    UInt32(littleEndianBytes: request.payload[18..<22])
}

/// The partition a poll names: `[kind:1][consumer:6][stream:6][topic:6][flag:1][partition:4]`
/// with numeric ids.
private func polledPartition(_ request: FakeVSRServer.Request) -> UInt32 {
    UInt32(littleEndianBytes: request.payload[20..<24])
}

private let otherContext = PartitionContext(incarnation: 8, ownerGeneration: 4, metadataOp: 30)

/// Each partition write with whether it runs in a cluster. A send goes over
/// the coordinator, and so does an offset store on a single node, but in a
/// cluster the store goes over a data connection to the partition primary.
private let partitionWritePaths: [(CommandCode, Bool)] = [(.sendMessages, false), (.storeConsumerOffset, false), (.storeConsumerOffset, true)]

private func contains(_ payload: [UInt8], _ id: MessageID) -> Bool {
    var writer = ByteWriter()
    writer.write(id)
    let needle = writer.bytes
    return payload.indices.contains { payload[$0...].starts(with: needle) }
}

@Suite("Messaging", .serialized)
struct MessagingTests {
    // MARK: Sends

    @Test func sendsCaptureTheRouteContextOnce() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = try await connectedClient(server)
        try await client.sendBatch(streamID: 1, topicID: 2, partitioning: .partition(1), messages: ["a"])
        try await client.sendBatch(streamID: 1, topicID: 2, partitioning: .partition(1), messages: ["b"])
        let routes = await requests(server, .getPartitionRoute)
        #expect(routes.count == 1)
        #expect(routes.first?.payload == (try Requests.partitionRoute(streamID: 1, topicID: 2, partitionID: 1)))
        let sends = await requests(server, .sendMessages)
        #expect(sends.count == 2)
        // A route names no consumer, so the send carries owner 0.
        #expect(sends.allSatisfy { $0.context == FakeVSRServer.routeContext })
        await client.shutdown()
    }

    @Test func mintedIdsAreWrittenBackAndSentAgainUnchanged() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = try await connectedClient(server)
        let chosen = MessageID(low: 9, high: 0)
        var batch: [IggyMessage] = ["minted", try IggyMessage("chosen", id: chosen)]
        try await client.sendMessages(streamID: 1, topicID: 2, partitioning: .partition(1), messages: &batch)
        let minted = batch[0].id
        #expect(!minted.isZero)
        #expect(batch[1].id == chosen)
        // A retry of the same batch carries the same ids.
        try await client.sendMessages(streamID: 1, topicID: 2, partitioning: .partition(1), messages: &batch)
        #expect(batch[0].id == minted)
        let sends = await requests(server, .sendMessages)
        #expect(sends.count == 2)
        #expect(sends.allSatisfy { contains($0.payload, minted) && contains($0.payload, chosen) })
        await client.shutdown()
    }

    /// A send lost with its connection may have committed, so a history
    /// refusal of its resumed replay is an unknown outcome: the send is only
    /// replayed under its own request id, never sent again under a new
    /// route.
    @Test func aResumedSendRefusedForItsHistoryIsNotSentAgain() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let dropped = Locked(false)
        await server.respond { request in
            if request.code == CommandCode.sendMessages.rawValue {
                return dropped.exchange(true) ? .status(IggyErrorCode.historyUnavailable.rawValue) : .close
            }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = try await connectedClient(server, requestTimeout: .seconds(1))
        await expectCode(.transientNotCommitted) {
            try await client.sendBatch(streamID: 1, topicID: 2, partitioning: .partition(1), messages: ["a"])
        }
        let sends = await requests(server, .sendMessages)
        #expect(sends.count >= 2)
        #expect(Set(sends.map(\.requestID)).count == 1)
        #expect(await requests(server, .getPartitionRoute).count == 1)
        await client.shutdown()
    }

    @Test func aStaleHistoryReadsTheRouteAgainOnce() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let refusals = Locked(1)
        let routes = Locked(0)
        await server.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .getPartitionRoute:
                let count = routes.withLock {
                    $0 += 1; return $0
                }
                return .ok(FakeVSRServer.partitionRouteBody(count == 1 ? FakeVSRServer.routeContext : otherContext))
            case .sendMessages
            where refusals.withLock({
                $0 -= 1; return $0 >= 0
            }):
                return .status(IggyErrorCode.historyUnavailable.rawValue)
            default:
                return FakeVSRServer.defaultReply(request, address: server.address)
            }
        }
        let client = try await connectedClient(server)
        try await client.sendBatch(streamID: 1, topicID: 2, partitioning: .partition(0), messages: ["a"])
        let sends = await requests(server, .sendMessages)
        #expect(sends.map(\.context) == [FakeVSRServer.routeContext, PartitionContext(incarnation: 8, ownerGeneration: 0, metadataOp: 30)])
        #expect(sends[0].requestID != sends[1].requestID)

        // A second refusal in a row ends the send.
        refusals.withLock { $0 = 2 }
        await expectCode(.historyUnavailable) {
            try await client.sendBatch(streamID: 1, topicID: 2, partitioning: .partition(0), messages: ["b"])
        }
        #expect(await requests(server, .sendMessages).count == 4)
        #expect(routes.withLock { $0 } == 3)
        await client.shutdown()
    }

    @Test func balancedSendsRoundRobinUnderTheTopicContexts() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let contexts = (1...3).map { PartitionContext(incarnation: UInt64($0), ownerGeneration: 0, metadataOp: 5) }
        await server.respond { request in
            if request.code == CommandCode.getTopic.rawValue {
                return .ok(FakeVSRServer.topicBody(contexts: contexts))
            }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = try await connectedClient(server)
        for text in ["a", "b", "c", "d"] {
            try await client.sendBatch(streamID: 1, topicID: 2, messages: [IggyMessage(stringLiteral: text)])
        }
        let sends = await requests(server, .sendMessages)
        #expect(sends.map(sentPartition) == [0, 1, 2, 0])
        // The topic read cached the count and the contexts: no route lookups.
        #expect(sends.map(\.context) == [contexts[0], contexts[1], contexts[2], contexts[0]])
        #expect(await requests(server, .getTopic).count == 1)
        #expect(await requests(server, .getPartitionRoute).isEmpty)

        try await client.sendBatch(streamID: 1, topicID: 2, partitioning: .messagesKey("user-1"), messages: ["e"])
        #expect(await requests(server, .sendMessages).last.map(sentPartition) == XXH32.hash(Array("user-1".utf8)) % 3)

        // A deleted topic can come back with other partitions.
        try await client.deleteTopic(streamID: 1, topicID: 9)
        try await client.sendBatch(streamID: 1, topicID: 2, messages: ["f"])
        #expect(await requests(server, .getTopic).count == 2)
        await client.shutdown()
    }

    @Test func aBalancedSendToAMissingTopicFails() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = try await connectedClient(server)
        await expectCode(.topicIdNotFound) { try await client.sendBatch(streamID: 1, topicID: 2, messages: ["a"]) }
        #expect(await requests(server, .sendMessages).isEmpty)
        await client.shutdown()
    }

    // MARK: Polls

    @Test func pollsTakeTheirRouteOnASingleNode() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = try await connectedClient(server)
        let polled = try await client.pollMessages(
            streamID: 1, topicID: 2, partitionID: 1, consumer: .consumer(5), strategy: .next, count: 10, autoCommit: true)
        #expect(polled.context == FakeVSRServer.routeContext)
        let continued = PollingStrategy.offset(3, context: otherContext)
        _ = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: 1, consumer: .consumer(5), strategy: continued, count: 10, autoCommit: true)

        let lookups = await requests(server, .getPartitionRoute)
        #expect(lookups.count == 1)
        #expect(lookups.first?.payload == (try Requests.partitionRoute(streamID: 1, topicID: 2, partitionID: 1)))
        let payload = try Requests.pollMessages(
            consumer: .consumer(5), streamID: 1, topicID: 2, partitionID: 1, strategy: .next, count: 10, autoCommit: true)
        let polls = await requests(server, .pollMessages)
        #expect(polls.map(\.context) == [FakeVSRServer.routeContext, otherContext])
        #expect(polls.first?.payload == payload)
        // Every request went over the one coordinator connection.
        #expect(await server.connectionCount == 1)
        await client.shutdown()
    }

    @Test func aRefusedPollLooksUpItsRouteAgain() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let refused = Locked(false)
        await server.respond { request in
            if request.code == CommandCode.pollMessages.rawValue,
                !refused.exchange(true)
            {
                return .status(IggyErrorCode.transientNotAccepted.rawValue)
            }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = try await connectedClient(server)
        _ = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: 0, strategy: .first, count: 1, autoCommit: false)
        #expect(await requests(server, .getPartitionRoute).count == 2)
        #expect(await requests(server, .pollMessages).count == 2)
        await client.shutdown()
    }

    /// The poll may have been applied, with auto-commit, before the
    /// connection went: its outcome is unknown, as on a data connection.
    @Test func aSingleNodePollLostAfterItWasWrittenIsAnUnknownOutcome() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let dropped = Locked(false)
        await server.respond { request in
            if request.code == CommandCode.pollMessages.rawValue, !dropped.exchange(true) {
                return .close
            }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = try await connectedClient(server)
        let poll = { _ = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: 0, strategy: .first, count: 1, autoCommit: true) }
        await expectCode(.transientNotCommitted) { try await poll() }
        try await poll()
        await client.shutdown()
    }

    @Test func aNewerMetadataCommitReadsTheRouteAgain() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let committed = PartitionContext(incarnation: 7, ownerGeneration: 0, metadataOp: 50)
        let applied = Locked(FakeVSRServer.routeContext)
        await server.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .deleteStream:
                applied.withLock { $0 = committed }
                return .committed([], commit: committed.metadataOp)
            case .getPartitionRoute:
                return .ok(FakeVSRServer.partitionRouteBody(applied.withLock { $0 }))
            default:
                return FakeVSRServer.defaultReply(request, address: server.address)
            }
        }
        let client = try await connectedClient(server)
        let poll = { _ = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: 0, strategy: .first, count: 1, autoCommit: false) }
        try await poll()
        try await poll()
        #expect(await requests(server, .getPartitionRoute).count == 1)
        try await client.deleteStream(9)
        #expect(await client.core.metadataWatermark == 50)
        try await poll()
        try await poll()
        // The route read after the commit is as new as the commit.
        #expect(await requests(server, .getPartitionRoute).count == 2)
        #expect(await requests(server, .pollMessages).map(\.context) == [FakeVSRServer.routeContext, FakeVSRServer.routeContext, committed, committed])
        await client.shutdown()
    }

    /// A route older than a metadata commit the client saw is never used: it
    /// is read again until the coordinator applied the commit.
    @Test func aRouteOlderThanTheWatermarkIsReadAgain() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let lagging = Locked(1)
        await server.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .deleteStream:
                return .committed([], commit: 50)
            case .getPartitionRoute:
                let behind = lagging.withLock {
                    $0 -= 1; return $0 >= 0
                }
                let metadataOp: UInt64 = behind ? 49 : 50
                return .ok(FakeVSRServer.partitionRouteBody(PartitionContext(incarnation: 7, ownerGeneration: 0, metadataOp: metadataOp)))
            default:
                return FakeVSRServer.defaultReply(request, address: server.address)
            }
        }
        let client = try await connectedClient(server)
        try await client.deleteStream(9)
        _ = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: 0, strategy: .first, count: 1, autoCommit: false)
        #expect(await requests(server, .getPartitionRoute).count == 2)
        #expect(await requests(server, .pollMessages).map(\.context.metadataOp) == [50])
        await client.shutdown()
    }

    @Test func aNewSessionDropsTheRoutesAndTheMembership() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await server.respond { request in
            if request.code == CommandCode.syncConsumerGroup.rawValue {
                return .ok(FakeVSRServer.assignmentBody(generation: 1, partitions: [0]))
            }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = try await connectedClient(server)
        _ = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: nil, consumer: .group(3), strategy: .next, count: 1, autoCommit: true)
        #expect(client.core.groupState.isRegistered(.init(stream: 1, topic: 2, group: 3)))
        try await client.logout()
        _ = try await client.login(username: "iggy", password: "iggy")
        #expect(!client.core.groupState.isRegistered(.init(stream: 1, topic: 2, group: 3)))
        _ = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: 0, strategy: .first, count: 1, autoCommit: false)
        let lookups = await requests(server, .getPartitionRoute)
        #expect(lookups.count == 2)
        #expect(lookups[0].clientID != lookups[1].clientID)
        await client.shutdown()
    }

    // MARK: Sessions

    /// A lost connection resumes the session, so the member keeps polling
    /// without joining again.
    @Test func aReconnectResumesTheSessionAndKeepsTheMembership() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await server.respond { request in
            if request.code == CommandCode.syncConsumerGroup.rawValue {
                return .ok(FakeVSRServer.assignmentBody(generation: 1, partitions: [0]))
            }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = try await connectedClient(server)
        let poll = {
            try await client.pollMessages(streamID: 1, topicID: 2, partitionID: nil, consumer: .group(3), strategy: .next, count: 1, autoCommit: true)
        }
        _ = try await poll()
        await server.closeAllConnections()
        try await Task.sleep(for: .milliseconds(20))
        _ = try await poll()
        #expect(client.core.groupState.isRegistered(.init(stream: 1, topic: 2, group: 3)))
        #expect(await server.connectionCount == 2)
        #expect(await server.requests.filter { $0.operation == .register }.count == 1)
        let binds = await requests(server, .bindSession)
        #expect(binds.count == 1)
        let polls = await requests(server, .pollMessages)
        #expect(polls.count == 2)
        #expect(Set(polls.map(\.clientID)).count == 1)
        let bind = try #require(binds.first)
        #expect(polls.allSatisfy { $0.clientID == bind.clientID })
        await client.shutdown()
    }

    /// When the server no longer holds the session the client signs in under
    /// a new one, which is not a member until the caller joins again.
    @Test func aSessionTheServerLostEndsTheMembership() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let member = Locked<UInt128Value?>(nil)
        await server.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .bindSession:
                return .status(IggyErrorCode.unauthenticated.rawValue)
            case .syncConsumerGroup:
                let joined = member.withLock { joined in
                    if joined == nil { joined = request.clientID }
                    return joined == request.clientID
                }
                return .ok(joined ? FakeVSRServer.assignmentBody(generation: 1, partitions: [0]) : [])
            default:
                return FakeVSRServer.defaultReply(request, address: server.address)
            }
        }
        let client = try await connectedClient(server)
        let poll = {
            try await client.pollMessages(streamID: 1, topicID: 2, partitionID: nil, consumer: .group(3), strategy: .next, count: 1, autoCommit: true)
        }
        _ = try await poll()
        await server.closeAllConnections()
        try await Task.sleep(for: .milliseconds(20))
        try await client.ping()
        #expect(await server.requests.filter { $0.operation == .register }.count == 2)
        #expect(!client.core.groupState.isRegistered(.init(stream: 1, topic: 2, group: 3)))
        await expectCode(.consumerGroupMemberNotFound) { _ = try await poll() }
        await client.shutdown()
    }

    /// A data-connection frame is stamped only for the session its route was
    /// read under, and only while no newer metadata commit was seen: anything
    /// else is refused before it is written.
    @Test func dataConnectionFramesAreStampedOnlyForTheLiveSession() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await server.respond { request in
            if request.operation == .deleteStream {
                return .committed([], commit: 5)
            }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = try await connectedClient(server)
        try await client.ping()
        let live = await server.requests.last!
        let stamp = { (watermark: UInt64) in
            _ = try await client.core.stampRequest(
                code: CommandCode.ping.rawValue, payload: [], context: .empty,
                for: Responses.SessionIdentity(clientID: live.clientID, session: live.session, metadataWatermark: watermark),
                lineage: await client.core.lineage)
        }
        try await stamp(0)
        try await client.deleteStream(1)
        await expectCode(.transientNotAccepted) { try await stamp(4) }
        try await stamp(5)
        try await client.logout()
        await expectCode(.transientNotAccepted) { try await stamp(5) }
        _ = try await client.login(username: "iggy", password: "iggy")
        await expectCode(.transientNotAccepted) { try await stamp(5) }
        await client.shutdown()
    }

    /// A send that reads its topic first, for balanced or key partitioning,
    /// stays in the lineage it started in. The caller signs in as another
    /// user while the topic read is in flight, and the send ends
    /// unauthenticated instead of going out under the new session. The
    /// roster read at connect is refused, so the send reads the roster before
    /// it writes the batch, which lets the new session bind first.
    @Test(arguments: [Partitioning.balanced, .messagesKey("user-1")])
    func aSendEndsWhenTheCallerSignsInBetweenItsTopicReadAndItsBatch(_ partitioning: Partitioning) async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let rosterServed = Locked(false)
        let topicReleased = Locked(false)
        let contexts = (1...3).map { PartitionContext(incarnation: UInt64($0), ownerGeneration: 0, metadataOp: 5) }
        await server.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .getClusterMetadata where !rosterServed.withLock({ $0 }):
                return .status(IggyErrorCode.featureUnavailable.rawValue)
            case .getTopic:
                _ = await eventually { topicReleased.withLock { $0 } }
                return .ok(FakeVSRServer.topicBody(contexts: contexts))
            default:
                return FakeVSRServer.defaultReply(request, address: server.address)
            }
        }
        let client = try await connectedClient(server)
        let started = await client.core.lineage
        let send = Task { try await client.sendBatch(streamID: 1, topicID: 2, partitioning: partitioning, messages: ["a"]) }
        #expect(await eventually { await !requests(server, .getTopic).isEmpty })
        let signIn = Task { try await client.login(username: "other", password: "secret") }
        // The sign-in raises the lineage, then its logout waits for the topic read.
        #expect(await eventually { await client.core.lineage != started })
        rosterServed.withLock { $0 = true }
        topicReleased.withLock { $0 = true }
        _ = try await signIn.value

        await expectCode(.unauthenticated) { _ = try await send.value }
        #expect(await requests(server, .sendMessages).isEmpty)
        await client.shutdown()
    }

    /// A group poll stays in the lineage it started in. The caller signs in
    /// as another user while the assignment sync is in flight, and the poll
    /// ends unauthenticated instead of polling under the new session.
    @Test func aGroupPollEndsWhenTheCallerSignsInBetweenItsSyncAndItsPoll() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = try await connectedClient(server)
        let group = ConsumerGroupState.GroupKey(stream: 1, topic: 2, group: 3)
        let syncReleased = Locked(false)
        await server.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .syncConsumerGroup:
                _ = await eventually { syncReleased.withLock { $0 } }
                return .ok(FakeVSRServer.assignmentBody(generation: 1, partitions: [0]))
            case .logoutUser:
                // The end of the old session drops an assignment the poll has
                // not applied yet, so the logout waits for it.
                _ = await eventually { client.core.groupState.hasAssignment(group) }
                return .ok([])
            default:
                return FakeVSRServer.defaultReply(request, address: server.address)
            }
        }
        let started = await client.core.lineage
        let poll = Task {
            try await client.pollMessages(streamID: 1, topicID: 2, partitionID: nil, consumer: .group(3), strategy: .next, count: 1, autoCommit: true)
        }
        #expect(await eventually { await !requests(server, .syncConsumerGroup).isEmpty })
        let signIn = Task { try await client.login(username: "other", password: "secret") }
        #expect(await eventually { await client.core.lineage != started })
        syncReleased.withLock { $0 = true }
        _ = try await signIn.value

        await expectCode(.unauthenticated) { _ = try await poll.value }
        #expect(await requests(server, .getPartitionRoute).isEmpty)
        #expect(await requests(server, .pollMessages).isEmpty)
        await client.shutdown()
    }

    // MARK: Consumer groups

    @Test func groupPollsRoundRobinOverTheAssignment() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await server.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .syncConsumerGroup:
                return .ok(FakeVSRServer.assignmentBody(generation: 1, partitions: [1, 2]))
            case .pollMessages:
                return .ok(FakeVSRServer.polledBody(partitionID: polledPartition(request)))
            default:
                return FakeVSRServer.defaultReply(request, address: server.address)
            }
        }
        let client = try await connectedClient(server)
        var polled: [UInt32] = []
        for _ in 0..<3 {
            polled.append(
                try await client.pollMessages(streamID: 1, topicID: 2, partitionID: nil, consumer: .group(3), strategy: .next, count: 1, autoCommit: true)
                    .partitionID)
        }
        #expect(polled == [1, 2, 1])
        #expect(await requests(server, .syncConsumerGroup).count == 1)
        // Each partition has its own route.
        #expect(
            await requests(server, .getPartitionRoute).map(\.payload) == [
                try Requests.partitionRoute(streamID: 1, topicID: 2, partitionID: 1), try Requests.partitionRoute(streamID: 1, topicID: 2, partitionID: 2),
            ])
        await client.shutdown()
    }

    @Test func aFencedGroupPollSyncsAndPollsAgain() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let fenced = Locked(false)
        await server.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .syncConsumerGroup:
                return .ok(FakeVSRServer.assignmentBody(generation: fenced.withLock { $0 } ? 2 : 1, partitions: [1]))
            case .pollMessages:
                let first = !fenced.exchange(true)
                return .ok(FakeVSRServer.polledBody(partitionID: first ? PolledMessages.resyncRequiredPartition : 1))
            default:
                return FakeVSRServer.defaultReply(request, address: server.address)
            }
        }
        let client = try await connectedClient(server)
        let polled = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: nil, consumer: .group(3), strategy: .next, count: 1, autoCommit: true)
        #expect(polled.partitionID == 1)
        #expect(await requests(server, .syncConsumerGroup).count == 2)
        #expect(await requests(server, .pollMessages).count == 2)
        await client.shutdown()
    }

    @Test func aGroupPollTellsMembersFromNonMembers() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let member = Locked(true)
        await server.respond { request in
            if request.code == CommandCode.syncConsumerGroup.rawValue {
                return .ok(member.withLock { $0 } ? FakeVSRServer.assignmentBody(generation: 1, partitions: []) : [])
            }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = try await connectedClient(server)
        let poll = {
            try await client.pollMessages(streamID: 1, topicID: 2, partitionID: nil, consumer: .group(3), strategy: .next, count: 1, autoCommit: true)
        }
        #expect(try await poll().partitionID == PolledMessages.noAssignedPartition)
        // The member was removed, say by an eviction: the next sync finds
        // the client no longer a member and the poll tells it.
        member.withLock { $0 = false }
        await expectCode(.consumerGroupMemberNotFound) { _ = try await poll() }
        #expect(await requests(server, .syncConsumerGroup).count == 2)
        #expect(await requests(server, .pollMessages).isEmpty)
        await client.shutdown()
    }

    @Test func aLostPartitionMovesTheGroupPollOn() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let syncs = Locked(0)
        await server.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .syncConsumerGroup:
                let count = syncs.withLock {
                    $0 += 1; return $0
                }
                return .ok(FakeVSRServer.assignmentBody(generation: UInt64(count), partitions: count == 1 ? [1] : [2]))
            case .pollMessages where polledPartition(request) == 1:
                return .status(IggyErrorCode.consumerGroupPartitionNotOwned.rawValue)
            case .pollMessages:
                return .ok(FakeVSRServer.polledBody(partitionID: polledPartition(request)))
            default:
                return FakeVSRServer.defaultReply(request, address: server.address)
            }
        }
        let client = try await connectedClient(server)
        let polled = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: nil, consumer: .group(3), strategy: .next, count: 1, autoCommit: true)
        #expect(polled.partitionID == 2)

        // A continuation of the lost partition cannot move to another one.
        syncs.withLock { $0 = 0 }
        client.core.groupState.invalidateAssignment(.init(stream: 1, topic: 2, group: 3))
        let continued = PollingStrategy.offset(4, context: otherContext)
        await expectCode(.consumerGroupPartitionNotOwned) {
            _ = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: nil, consumer: .group(3), strategy: continued, count: 1, autoCommit: true)
        }
        await client.shutdown()
    }

    @Test func theHeartbeatSyncsJoinedGroupsUntilTheyAreLeft() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await server.respond { request in
            if request.code == CommandCode.syncConsumerGroup.rawValue {
                return .ok(FakeVSRServer.assignmentBody(generation: 1, partitions: [0]))
            }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = try await connectedClient(server, heartbeat: .milliseconds(30))
        try await client.joinConsumerGroup(streamID: 1, topicID: 2, groupID: 3)
        _ = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: nil, consumer: .group(3), strategy: .next, count: 1, autoCommit: true)
        #expect(await eventually { await requests(server, .syncConsumerGroup).count >= 3 })
        try await client.leaveConsumerGroup(streamID: 1, topicID: 2, groupID: 3)
        #expect(client.core.groupState.registeredGroups.isEmpty)
        await client.shutdown()
    }

    // MARK: Offsets

    @Test func offsetRequestsTakeThePartitionRoute() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = try await connectedClient(server)
        try await client.storeConsumerOffset(consumer: .consumer(5), streamID: 1, topicID: 2, partitionID: 3, offset: 40)
        let position = ConsumerPosition(partitionID: 3, offset: 41, context: otherContext)
        try await client.storeConsumerPosition(consumer: .consumer(5), streamID: 1, topicID: 2, position: position)
        #expect(try await client.getConsumerOffset(consumer: .consumer(5), streamID: 1, topicID: 2, partitionID: 3) == nil)
        try await client.deleteConsumerOffset(consumer: .consumer(5), streamID: 1, topicID: 2, partitionID: 3)

        let lookups = await requests(server, .getPartitionRoute)
        #expect(lookups.count == 1)
        #expect(lookups.first?.payload == (try Requests.partitionRoute(streamID: 1, topicID: 2, partitionID: 3)))
        let stores = await requests(server, .storeConsumerOffset)
        #expect(stores.map(\.context) == [FakeVSRServer.routeContext, otherContext])
        #expect(
            stores.first?.payload
                == (try Requests.storeConsumerOffset(consumer: .consumer(5), streamID: 1, topicID: 2, partitionID: 3, offset: 40, ack: .quorum)))
        #expect(await requests(server, .getConsumerOffset).map(\.context) == [FakeVSRServer.routeContext])
        #expect(await requests(server, .deleteConsumerOffset).map(\.context) == [FakeVSRServer.routeContext])
        await client.shutdown()
    }

    /// An offset read without a partition reads partition 0, as the server
    /// does. A write without one names no partition to route.
    @Test func offsetRequestsWithoutAPartition() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = try await connectedClient(server)
        _ = try await client.getConsumerOffset(consumer: .group(3), streamID: 1, topicID: 2, partitionID: nil)
        #expect(await requests(server, .getPartitionRoute).map(\.payload) == [try Requests.partitionRoute(streamID: 1, topicID: 2, partitionID: 0)])
        #expect(
            await requests(server, .getConsumerOffset).map(\.payload)
                == [try Requests.getConsumerOffset(consumer: .group(3), streamID: 1, topicID: 2, partitionID: nil)])

        let before = await server.requests.count
        await expectCode(.invalidIdentifier) {
            try await client.storeConsumerOffset(consumer: .group(3), streamID: 1, topicID: 2, partitionID: nil, offset: 1)
        }
        await expectCode(.invalidIdentifier) { try await client.deleteConsumerOffset(consumer: .group(3), streamID: 1, topicID: 2, partitionID: nil) }
        #expect(await server.requests.count == before)
        await client.shutdown()
    }

    // MARK: Raw requests

    /// A lookup over a coordinator that stands at the metadata primary needs
    /// no data connection, so no bound session either. A raw send made before
    /// a sign-in gets the server's refusal at once instead of waiting for a
    /// session until its deadline.
    @Test func aRawSendBeforeASignInGetsTheRefusalOfThePinnedCoordinator() async throws {
        let coordinator = try await FakeVSRServer.started()
        defer { Task { await coordinator.stop() } }
        await coordinator.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .getClusterMetadata:
                return .ok(FakeVSRServer.rosterBody([coordinator.address, unreachableAddress]))
            case .getPartitionRoute:
                return .status(IggyErrorCode.unauthenticated.rawValue)
            default:
                return FakeVSRServer.defaultReply(request, address: coordinator.address)
            }
        }
        var configuration = ClientConfiguration(address: coordinator.address)
        configuration.requestTimeout = .seconds(1)
        let client = IggyClient(configuration: configuration, connector: NIOConnector(), logger: testLogger)
        try await client.connect()
        let outgoing = [Batch.OutgoingMessage(id: .random(), originTimestamp: 0, payload: [1], userHeaders: nil)]
        let send = try Batch.encodeSend(streamID: 1, topicID: 2, partitioning: .partition(1), messages: outgoing)

        await expectCode(.unauthenticated) { _ = try await client.sendRawRequest(code: CommandCode.sendMessages.rawValue, payload: send) }
        #expect(await !requests(coordinator, .getPartitionRoute).isEmpty)
        await client.shutdown()
    }

    @Test func rawPartitionRequestsAreRoutedLikeTypedOnes() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = try await connectedClient(server)
        let outgoing = [Batch.OutgoingMessage(id: .random(), originTimestamp: 0, payload: [1], userHeaders: nil)]
        let send = try Batch.encodeSend(streamID: 1, topicID: 2, partitioning: .partition(1), messages: outgoing)
        _ = try await client.sendRawRequest(code: CommandCode.sendMessages.rawValue, payload: send)
        #expect(await requests(server, .getPartitionRoute).count == 1)
        #expect(await requests(server, .sendMessages).map(\.context) == [FakeVSRServer.routeContext])

        let poll = try Requests.pollMessages(consumer: .consumer(5), streamID: 1, topicID: 2, partitionID: 1, strategy: .first, count: 1, autoCommit: false)
        _ = try await client.sendRawRequest(code: CommandCode.pollMessages.rawValue, payload: poll)
        #expect(await requests(server, .pollMessages).map(\.context) == [FakeVSRServer.routeContext])

        // The offset requests take the route the poll read.
        let store = try Requests.storeConsumerOffset(consumer: .consumer(5), streamID: 1, topicID: 2, partitionID: 1, offset: 9, ack: .quorum)
        _ = try await client.sendRawRequest(code: CommandCode.storeConsumerOffset.rawValue, payload: store)
        let read = try Requests.getConsumerOffset(consumer: .consumer(5), streamID: 1, topicID: 2, partitionID: 1)
        _ = try await client.sendRawRequest(code: CommandCode.getConsumerOffset.rawValue, payload: read)
        let delete = try Requests.deleteConsumerOffset(consumer: .consumer(5), streamID: 1, topicID: 2, partitionID: 1, ack: .quorum)
        _ = try await client.sendRawRequest(code: CommandCode.deleteConsumerOffset.rawValue, payload: delete)
        for code in [CommandCode.storeConsumerOffset, .getConsumerOffset, .deleteConsumerOffset] {
            #expect(await requests(server, code).map(\.context) == [FakeVSRServer.routeContext])
        }
        #expect(await requests(server, .getPartitionRoute).count == 2)

        let before = await server.requests.count
        let balanced = try Batch.encodeSend(streamID: 1, topicID: 2, partitioning: .balanced, messages: outgoing)
        await expectCode(.featureUnavailable) { _ = try await client.sendRawRequest(code: CommandCode.sendMessages.rawValue, payload: balanced) }
        // The count is outside the declared metadata, then the partition id is
        // three bytes wide: neither is a header the server would accept.
        var shortMetadata = send
        shortMetadata[0] -= 4
        var narrowPartition = send
        narrowPartition[17] = 3
        for malformed in [shortMetadata, narrowPartition] {
            await expectCode(.invalidCommand) { _ = try await client.sendRawRequest(code: CommandCode.sendMessages.rawValue, payload: malformed) }
        }
        await expectCode(.invalidCommand) { _ = try await client.sendRawRequest(code: CommandCode.pollMessages.rawValue, payload: Array(poll.dropLast())) }
        await expectCode(.invalidCommand) { _ = try await client.sendRawRequest(code: CommandCode.storeConsumerOffset.rawValue, payload: [1, 9]) }
        await expectCode(.invalidCommand) { _ = try await client.sendRawRequest(code: CommandCode.bindSession.rawValue, payload: []) }
        // A group poll and an offset write must name their partition.
        let groupPoll = try Requests.pollMessages(consumer: .group(3), streamID: 1, topicID: 2, partitionID: nil, strategy: .next, count: 1, autoCommit: true)
        let unnamedStore = try Requests.storeConsumerOffset(consumer: .consumer(5), streamID: 1, topicID: 2, partitionID: nil, offset: 9, ack: .quorum)
        let unnamedDelete = try Requests.deleteConsumerOffset(consumer: .consumer(5), streamID: 1, topicID: 2, partitionID: nil, ack: .quorum)
        for (code, payload) in [(CommandCode.pollMessages, groupPoll), (.storeConsumerOffset, unnamedStore), (.deleteConsumerOffset, unnamedDelete)] {
            await expectCode(.invalidIdentifier) { _ = try await client.sendRawRequest(code: code.rawValue, payload: payload) }
        }
        #expect(await server.requests.count == before)
        await client.shutdown()
    }

    // MARK: Clusters

    /// A coordinator, replica 0, that routes every partition to `primary`,
    /// replica 1.
    private static func coordinator(routingTo primary: FakeVSRServer) async throws -> FakeVSRServer {
        let coordinator = try await FakeVSRServer.started()
        await coordinator.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .getClusterMetadata:
                return .ok(FakeVSRServer.rosterBody([coordinator.address, primary.address]))
            case .getPartitionRoute:
                return .ok(FakeVSRServer.partitionRouteBody(FakeVSRServer.routeContext, primaryReplica: 1))
            default:
                return FakeVSRServer.defaultReply(request, address: coordinator.address)
            }
        }
        return coordinator
    }

    @Test func aClusterPollGoesToThePrimaryOverABoundDataConnection() async throws {
        let primary = try await FakeVSRServer.started()
        let coordinator = try await Self.coordinator(routingTo: primary)
        defer {
            Task {
                await coordinator.stop()
                await primary.stop()
            }
        }
        let client = try await connectedClient(coordinator)
        _ = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: 0, strategy: .first, count: 1, autoCommit: false)
        _ = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: 0, strategy: .first, count: 1, autoCommit: false)
        try await client.storeConsumerOffset(streamID: 1, topicID: 2, partitionID: 0, offset: 1)

        let session = await coordinator.requests.last!
        let binds = await requests(primary, .bindSession)
        #expect(binds.count == 1)
        // The data connection binds at the metadata op of the route.
        #expect(
            binds.first?.payload
                == (try Requests.bindSession(
                    clientID: session.clientID, session: session.session, metadataWatermark: FakeVSRServer.routeContext.metadataOp,
                    bindSecret: await client.core.bindSecret)))
        let polls = await requests(primary, .pollMessages)
        #expect(polls.count == 2)
        #expect(polls.allSatisfy { $0.context == FakeVSRServer.routeContext && $0.clientID == session.clientID && $0.session == session.session })
        #expect(await requests(primary, .storeConsumerOffset).count == 1)
        #expect(await requests(coordinator, .pollMessages).isEmpty)
        #expect(await primary.connectionCount == 1)
        await client.shutdown()
    }

    /// After the leader redirection the coordinator stands at the metadata
    /// primary, so the lookup goes over it and the session binds no second
    /// connection there.
    @Test func aLookupGoesOverACoordinatorAtTheMetadataPrimary() async throws {
        let primary = try await FakeVSRServer.started()
        let coordinator = try await Self.coordinator(routingTo: primary)
        defer {
            Task {
                await coordinator.stop()
                await primary.stop()
            }
        }
        let client = try await connectedClient(coordinator)
        _ = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: 0, strategy: .first, count: 1, autoCommit: false)
        #expect(await requests(coordinator, .getPartitionRoute).count == 1)
        #expect(await requests(coordinator, .bindSession).isEmpty)
        #expect(await coordinator.connectionCount == 1)
        #expect(await requests(primary, .pollMessages).count == 1)
        await client.shutdown()
    }

    @Test func aDataConnectionLostMidRequestLeavesTheOutcomeUnknown() async throws {
        let primary = try await FakeVSRServer.started()
        let coordinator = try await Self.coordinator(routingTo: primary)
        defer {
            Task {
                await coordinator.stop()
                await primary.stop()
            }
        }
        let dropped = Locked(false)
        await primary.respond { request in
            if request.code == CommandCode.pollMessages.rawValue,
                !dropped.exchange(true)
            {
                return .close
            }
            return FakeVSRServer.defaultReply(request, address: primary.address)
        }
        let client = try await connectedClient(coordinator)
        let poll = { _ = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: 0, strategy: .first, count: 1, autoCommit: false) }
        await expectCode(.transientNotCommitted) { try await poll() }
        try await poll()
        // The next poll dials the primary again and binds the new connection.
        #expect(await primary.connectionCount == 2)
        #expect(await requests(primary, .bindSession).count == 2)
        await client.shutdown()
    }

    /// A backup answers "not primary" after a view change. The poll was never
    /// admitted, so it is routed again instead of failing.
    @Test func aPrimaryThatIsNoLongerPrimaryIsRoutedAgain() async throws {
        let primary = try await FakeVSRServer.started()
        let coordinator = try await Self.coordinator(routingTo: primary)
        defer {
            Task {
                await coordinator.stop()
                await primary.stop()
            }
        }
        let refused = Locked(false)
        await primary.respond { request in
            if request.code == CommandCode.getPartitionRoute.rawValue {
                return .ok(FakeVSRServer.partitionRouteBody(FakeVSRServer.routeContext, primaryReplica: 1, view: 3))
            }
            if request.code == CommandCode.pollMessages.rawValue, !refused.exchange(true) {
                return .status(IggyErrorCode.notPrimary.rawValue)
            }
            return FakeVSRServer.defaultReply(request, address: primary.address)
        }
        let client = try await connectedClient(coordinator)
        _ = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: 0, strategy: .first, count: 1, autoCommit: false)
        #expect(await requests(coordinator, .getPartitionRoute).count == 1)
        #expect(await requests(primary, .getPartitionRoute).count == 1)
        #expect(await requests(primary, .pollMessages).count == 2)
        // The refusal left the data connection at a frame boundary.
        #expect(await primary.connectionCount == 1)
        await client.shutdown()
    }

    @Test func anUnreachablePrimaryIsRoutedAgain() async throws {
        let primary = try await FakeVSRServer.started()
        let coordinator = try await FakeVSRServer.started()
        defer {
            Task {
                await coordinator.stop()
                await primary.stop()
            }
        }
        let lookups = Locked(0)
        await coordinator.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .getClusterMetadata:
                return .ok(FakeVSRServer.rosterBody([coordinator.address, unreachableAddress, primary.address]))
            case .getPartitionRoute:
                let first = lookups.withLock {
                    $0 += 1; return $0 == 1
                }
                return .ok(FakeVSRServer.partitionRouteBody(FakeVSRServer.routeContext, primaryReplica: first ? 1 : 2))
            default:
                return FakeVSRServer.defaultReply(request, address: coordinator.address)
            }
        }
        let client = try await connectedClient(coordinator)
        _ = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: 0, strategy: .first, count: 1, autoCommit: false)
        #expect(lookups.withLock { $0 } == 2)
        #expect(await requests(primary, .pollMessages).count == 1)
        await client.shutdown()
    }

    /// A primary that drops the dial without a reset holds it until the dial
    /// times out. The dial gets only what is left of the request's budget, so
    /// the request ends with its budget rather than after the connect timeout.
    @Test func aDialToAnUnansweringPrimaryEndsWithTheBudget() async throws {
        let unanswering = "192.0.2.1:8090"
        let coordinator = try await FakeVSRServer.started()
        defer { Task { await coordinator.stop() } }
        await coordinator.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .getClusterMetadata:
                return .ok(FakeVSRServer.rosterBody([coordinator.address, unanswering]))
            case .getPartitionRoute:
                return .ok(FakeVSRServer.partitionRouteBody(FakeVSRServer.routeContext, primaryReplica: 1))
            default:
                return FakeVSRServer.defaultReply(request, address: coordinator.address)
            }
        }
        let connector = UnansweringConnector(unanswering: try Endpoint(parsing: unanswering))
        let client = try await connectedClient(coordinator, connectTimeout: .seconds(30), connector: connector)
        let payload = try Requests.pollMessages(
            consumer: .consumer(5), streamID: 1, topicID: 2, partitionID: 0, strategy: .first, count: 1, autoCommit: false)
        let key = PollRouter.RouteKey(stream: 1, topic: 2, partitionID: 0)
        let budget: Duration = .milliseconds(500)
        let started = ContinuousClock.now
        await expectCode(.transientNotAccepted) {
            _ = try await client.core.pollRouter.send(
                .pollMessages, payload: payload, key: key, context: nil, lineage: await client.core.lineage, coordinator: client.core,
                deadline: started + budget)
        }
        #expect(ContinuousClock.now - started < .seconds(5))
        let timeouts = connector.timeouts.withLock { $0 }
        #expect(timeouts.count == 1)
        #expect(timeouts.allSatisfy { $0 <= budget }, "dial timeouts \(timeouts)")
        await client.shutdown()
    }

    /// A metadata primary that drops the dial without a reset must not spend
    /// the whole budget of a lookup, even when the connect timeout outlasts
    /// it: the coordinator is queued behind that dial, so the dial gets no
    /// longer than a failover dial and the lookup falls back in time.
    @Test func aLookupFallsBackInTimeFromAMetadataPrimaryThatDropsTheDial() async throws {
        let unanswering = "192.0.2.1:8090"
        let coordinator = try await FakeVSRServer.started()
        defer { Task { await coordinator.stop() } }
        await coordinator.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .getClusterMetadata:
                return .ok(FakeVSRServer.rosterBody([coordinator.address, unanswering]))
            case .getPartitionRoute:
                return .ok(FakeVSRServer.partitionRouteBody(FakeVSRServer.routeContext, primaryReplica: 0))
            default:
                return FakeVSRServer.defaultReply(request, address: coordinator.address)
            }
        }
        let connector = UnansweringConnector(unanswering: try Endpoint(parsing: unanswering))
        let client = try await connectedClient(coordinator, connectTimeout: .seconds(30), connector: connector)
        let afterViewChange = FakeVSRServer.rosterBody([coordinator.address, unanswering], leaderIndex: 1)
        client.core.pollRouter.learnRoster(try Responses.clusterMetadata(afterViewChange[...]))
        let payload = try Requests.pollMessages(
            consumer: .consumer(5), streamID: 1, topicID: 2, partitionID: 0, strategy: .first, count: 1, autoCommit: false)
        let key = PollRouter.RouteKey(stream: 1, topic: 2, partitionID: 0)
        _ = try await client.core.pollRouter.send(
            .pollMessages, payload: payload, key: key, context: nil, lineage: await client.core.lineage, coordinator: client.core,
            deadline: .now + .seconds(10))
        let timeouts = connector.timeouts.withLock { $0 }
        #expect(timeouts.count == 1)
        #expect(timeouts.allSatisfy { $0 <= ClientCore.failoverDialTimeout }, "dial timeouts \(timeouts)")
        #expect(await requests(coordinator, .getPartitionRoute).count == 1)
        #expect(await requests(coordinator, .pollMessages).count == 1)
        await client.shutdown()
    }

    /// A dial that takes the whole budget leaves none to bind the session
    /// with, so the client writes nothing to the metadata primary. That
    /// replica took no part in the failure, so the next lookup goes to it
    /// again instead of reading the roster for another one, and over the
    /// same connection, which no frame left out of step.
    @Test func aBudgetSpentOnTheDialKeepsTheMetadataPrimary() async throws {
        let metadataPrimary = try await FakeVSRServer.started()
        let coordinator = try await FakeVSRServer.started()
        defer {
            Task {
                await coordinator.stop()
                await metadataPrimary.stop()
            }
        }
        // The coordinator's roster names itself, so a roster read moves the
        // lookups to it.
        await coordinator.respond { request in
            if request.code == CommandCode.getClusterMetadata.rawValue {
                return .ok(FakeVSRServer.rosterBody([coordinator.address, metadataPrimary.address]))
            }
            return FakeVSRServer.defaultReply(request, address: coordinator.address)
        }
        await metadataPrimary.respond { request in
            if request.code == CommandCode.getPartitionRoute.rawValue {
                return .ok(FakeVSRServer.partitionRouteBody(FakeVSRServer.routeContext, primaryReplica: 1))
            }
            return FakeVSRServer.defaultReply(request, address: metadataPrimary.address)
        }
        let connector = SlowConnector(slow: try Endpoint(parsing: metadataPrimary.address))
        let client = try await connectedClient(coordinator, connector: connector)
        let roster = FakeVSRServer.rosterBody([coordinator.address, metadataPrimary.address], leaderIndex: 1)
        client.core.pollRouter.learnRoster(try Responses.clusterMetadata(roster[...]))
        let payload = try Requests.pollMessages(
            consumer: .consumer(5), streamID: 1, topicID: 2, partitionID: 0, strategy: .first, count: 1, autoCommit: false)
        let key = PollRouter.RouteKey(stream: 1, topic: 2, partitionID: 0)

        await expectCode(.transientNotAccepted) {
            _ = try await client.core.pollRouter.send(
                .pollMessages, payload: payload, key: key, context: nil, lineage: await client.core.lineage, coordinator: client.core,
                deadline: .now + .milliseconds(300))
        }
        _ = try await client.core.pollRouter.send(
            .pollMessages, payload: payload, key: key, context: nil, lineage: await client.core.lineage, coordinator: client.core,
            deadline: .now + .seconds(10))

        #expect(await requests(coordinator, .getPartitionRoute).isEmpty)
        #expect(await requests(metadataPrimary, .getPartitionRoute).count == 1)
        #expect(await requests(metadataPrimary, .pollMessages).count == 1)
        #expect(connector.dialed.withLock { $0.filter { $0 == connector.slow }.count } == 1)
        await client.shutdown()
    }

    /// A route can name a replica that the roster the client holds does not
    /// list. The roster is read again before the route is refused.
    @Test func aPrimaryMissingFromTheRosterReadsTheRosterAgain() async throws {
        let primary = try await FakeVSRServer.started()
        let other = try await FakeVSRServer.started()
        let coordinator = try await FakeVSRServer.started()
        defer {
            Task {
                await coordinator.stop()
                await other.stop()
                await primary.stop()
            }
        }
        let routed = Locked(false)
        await coordinator.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .getClusterMetadata:
                let addresses = [coordinator.address, other.address] + (routed.withLock { $0 } ? [primary.address] : [])
                return .ok(FakeVSRServer.rosterBody(addresses))
            case .getPartitionRoute:
                routed.withLock { $0 = true }
                return .ok(FakeVSRServer.partitionRouteBody(FakeVSRServer.routeContext, primaryReplica: 2))
            default:
                return FakeVSRServer.defaultReply(request, address: coordinator.address)
            }
        }
        let connector = RecordingConnector()
        let client = try await connectedClient(coordinator, connector: connector)
        let rosterReads = await requests(coordinator, .getClusterMetadata).count
        _ = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: 0, strategy: .first, count: 1, autoCommit: false)
        #expect(await requests(coordinator, .getClusterMetadata).count == rosterReads + 1)
        #expect(await requests(coordinator, .getPartitionRoute).count == 1)
        #expect(await requests(primary, .pollMessages).count == 1)
        #expect(try !connector.hasDialed(other))
        await client.shutdown()
    }

    // MARK: Namespace misses

    @Test func aPrimaryRefusalRefreshesEveryAliasPastTheRejectedView() async throws {
        let coordinator = try await FakeVSRServer.started()
        let previous = try await FakeVSRServer.started()
        let current = try await FakeVSRServer.started()
        defer {
            Task {
                await coordinator.stop()
                await previous.stop()
                await current.stop()
            }
        }
        let moved = Locked(false)
        let hints = Locked(0)
        await coordinator.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .getClusterMetadata:
                return .ok(FakeVSRServer.rosterBody([coordinator.address, previous.address, current.address]))
            case .getPartitionRoute:
                return .ok(FakeVSRServer.partitionRouteBody(FakeVSRServer.routeContext, primaryReplica: 1))
            default:
                return FakeVSRServer.defaultReply(request, address: coordinator.address)
            }
        }
        await previous.respond { request in
            if request.code == CommandCode.pollMessages.rawValue, moved.withLock({ $0 }) {
                return .status(IggyErrorCode.notPrimary.rawValue)
            }
            if request.code == CommandCode.getPartitionRoute.rawValue {
                let ready = hints.withLock {
                    $0 += 1; return $0 > 1
                }
                return .ok(FakeVSRServer.partitionRouteBody(FakeVSRServer.routeContext, primaryReplica: ready ? 2 : 1, view: ready ? 2 : 1))
            }
            return FakeVSRServer.defaultReply(request, address: previous.address)
        }
        let client = try await connectedClient(coordinator)
        let aliases: [Identifier] = [1, "same-stream"]
        for stream in aliases {
            _ = try await client.pollMessages(streamID: stream, topicID: 2, partitionID: 0, strategy: .first, count: 1, autoCommit: false)
        }
        moved.withLock { $0 = true }
        for stream in aliases {
            _ = try await client.pollMessages(streamID: stream, topicID: 2, partitionID: 0, strategy: .first, count: 1, autoCommit: false)
        }
        #expect(await requests(previous, .pollMessages).count == 3)
        #expect(await requests(current, .pollMessages).count == 2)
        #expect(hints.withLock { $0 } == 3)
        await client.shutdown()
    }

    /// A backup primary that has not applied the topic yet answers "not
    /// found". The route is looked up again on the metadata primary and the
    /// poll goes again.
    @Test func aNamespaceMissFromABackupPrimaryLooksUpTheRouteAgain() async throws {
        let primary = try await FakeVSRServer.started()
        let coordinator = try await Self.coordinator(routingTo: primary)
        defer {
            Task {
                await coordinator.stop()
                await primary.stop()
            }
        }
        let missed = Locked(false)
        await primary.respond { request in
            if request.code == CommandCode.pollMessages.rawValue, !missed.exchange(true) {
                return .status(IggyErrorCode.topicIdNotFound.rawValue)
            }
            return FakeVSRServer.defaultReply(request, address: primary.address)
        }
        let client = try await connectedClient(coordinator)
        _ = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: 0, strategy: .first, count: 1, autoCommit: false)
        #expect(await requests(coordinator, .getPartitionRoute).count == 2)
        #expect(await requests(primary, .getPartitionRoute).isEmpty)
        #expect(await requests(primary, .pollMessages).count == 2)
        await client.shutdown()
    }

    /// The metadata primary decides after a backup missed: here the user may
    /// no longer read the recreated topic, and that refusal is final.
    @Test func aMissFromABackupPrimaryEndsWithTheMetadataPrimaryAnswer() async throws {
        let primary = try await FakeVSRServer.started()
        let coordinator = try await FakeVSRServer.started()
        defer {
            Task {
                await coordinator.stop()
                await primary.stop()
            }
        }
        let lookups = Locked(0)
        await coordinator.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .getClusterMetadata:
                return .ok(FakeVSRServer.rosterBody([coordinator.address, primary.address]))
            case .getPartitionRoute:
                let first = lookups.withLock {
                    $0 += 1; return $0 == 1
                }
                return first
                    ? .ok(FakeVSRServer.partitionRouteBody(FakeVSRServer.routeContext, primaryReplica: 1))
                    : .status(IggyErrorCode.unauthorized.rawValue)
            default:
                return FakeVSRServer.defaultReply(request, address: coordinator.address)
            }
        }
        await primary.respond { request in
            if request.code == CommandCode.pollMessages.rawValue {
                return .status(IggyErrorCode.topicIdNotFound.rawValue)
            }
            return FakeVSRServer.defaultReply(request, address: primary.address)
        }
        let client = try await connectedClient(coordinator)
        await expectCode(.unauthorized) {
            _ = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: 0, strategy: .first, count: 1, autoCommit: false)
        }
        #expect(lookups.withLock { $0 } == 2)
        #expect(await requests(primary, .pollMessages).count == 1)
        await client.shutdown()
    }

    /// The metadata primary has applied every metadata commit, so its "not
    /// found" is final, whether it answers as the partition primary or to the
    /// lookup.
    @Test func aNamespaceMissFromTheMetadataPrimaryIsFinal() async throws {
        let backup = try await FakeVSRServer.started()
        let coordinator = try await FakeVSRServer.started()
        defer {
            Task {
                await coordinator.stop()
                await backup.stop()
            }
        }
        let routed = Locked(false)
        await coordinator.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .getClusterMetadata:
                return .ok(FakeVSRServer.rosterBody([coordinator.address, backup.address]))
            case .getPartitionRoute:
                if routed.exchange(true) {
                    return .status(IggyErrorCode.streamIdNotFound.rawValue)
                }
                return .ok(FakeVSRServer.partitionRouteBody(FakeVSRServer.routeContext, primaryReplica: 0))
            case .pollMessages:
                return .status(IggyErrorCode.topicIdNotFound.rawValue)
            default:
                return FakeVSRServer.defaultReply(request, address: coordinator.address)
            }
        }
        let connector = RecordingConnector()
        let client = try await connectedClient(coordinator, connector: connector)
        let poll = { _ = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: 0, strategy: .first, count: 1, autoCommit: false) }
        await expectCode(.topicIdNotFound) { try await poll() }
        #expect(await requests(coordinator, .pollMessages).count == 1)
        // The miss dropped the route, so the next poll looks it up.
        await expectCode(.streamIdNotFound) { try await poll() }
        #expect(await requests(coordinator, .getPartitionRoute).count == 2)
        #expect(await requests(coordinator, .pollMessages).count == 1)
        #expect(try !connector.hasDialed(backup))
        await client.shutdown()
    }

    /// A view change can leave the coordinator on a backup that has not
    /// applied the topic. The lookup goes to the metadata primary the roster
    /// names, not to the coordinator.
    @Test func aLookupAsksTheMetadataPrimaryRatherThanTheCoordinator() async throws {
        let metadataPrimary = try await FakeVSRServer.started()
        let coordinator = try await FakeVSRServer.started()
        defer {
            Task {
                await coordinator.stop()
                await metadataPrimary.stop()
            }
        }
        let roster = FakeVSRServer.rosterBody([coordinator.address, metadataPrimary.address])
        let afterViewChange = FakeVSRServer.rosterBody([coordinator.address, metadataPrimary.address], leaderIndex: 1)
        await metadataPrimary.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .getPartitionRoute:
                return .ok(FakeVSRServer.partitionRouteBody(FakeVSRServer.routeContext, primaryReplica: 0))
            default:
                return FakeVSRServer.defaultReply(request, address: metadataPrimary.address)
            }
        }
        await coordinator.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .getClusterMetadata:
                return .ok(roster)
            case .getPartitionRoute:
                return .status(IggyErrorCode.topicIdNotFound.rawValue)
            default:
                return FakeVSRServer.defaultReply(request, address: coordinator.address)
            }
        }
        let client = try await connectedClient(coordinator)
        client.core.pollRouter.learnRoster(try Responses.clusterMetadata(afterViewChange[...]))
        _ = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: 0, strategy: .first, count: 1, autoCommit: false)
        #expect(await requests(metadataPrimary, .getPartitionRoute).count == 1)
        #expect(await requests(coordinator, .getPartitionRoute).isEmpty)
        #expect(await requests(coordinator, .pollMessages).count == 1)
        await client.shutdown()
    }

    /// A view change deposed the metadata primary, and its own roster names
    /// the new leader while the coordinator still names the deposed one. As
    /// in the Rust SDK, the roster is read on the connection that answered
    /// the miss, so the miss is not final and the lookup goes to the leader
    /// that roster names.
    @Test func aMissFromADeposedMetadataPrimaryLooksUpOnTheLeaderItNames() async throws {
        let deposed = try await FakeVSRServer.started()
        let coordinator = try await FakeVSRServer.started()
        defer {
            Task {
                await coordinator.stop()
                await deposed.stop()
            }
        }
        let addresses = [coordinator.address, deposed.address]
        // The coordinator names itself until the client stands on it, so the
        // sign-in does not move the client to the deposed replica.
        let lagging = Locked(false)
        await coordinator.respond { request in
            if request.code == CommandCode.getClusterMetadata.rawValue {
                return .ok(FakeVSRServer.rosterBody(addresses, leaderIndex: lagging.withLock { $0 } ? 1 : 0))
            }
            return FakeVSRServer.defaultReply(request, address: coordinator.address)
        }
        await deposed.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .getClusterMetadata:
                return .ok(FakeVSRServer.rosterBody(addresses))
            case .getPartitionRoute:
                return .status(IggyErrorCode.streamIdNotFound.rawValue)
            default:
                return FakeVSRServer.defaultReply(request, address: deposed.address)
            }
        }
        let client = try await connectedClient(coordinator)
        lagging.withLock { $0 = true }
        let beforeViewChange = FakeVSRServer.rosterBody(addresses, leaderIndex: 1)
        client.core.pollRouter.learnRoster(try Responses.clusterMetadata(beforeViewChange[...]))

        _ = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: 0, strategy: .first, count: 1, autoCommit: false)

        #expect(await requests(deposed, .getPartitionRoute).count == 1)
        #expect(await requests(deposed, .getClusterMetadata).count == 1)
        #expect(await requests(coordinator, .getPartitionRoute).count == 1)
        await client.shutdown()
    }

    /// A lookup carries the metadata watermark, as `read_context` in the Rust
    /// SDK stamps it, so a replica that has not applied a commit the client
    /// saw holds the lookup instead of answering from older metadata: the
    /// coordinator on a single node, the metadata primary in a cluster.
    @Test(arguments: [false, true])
    func aLookupCarriesTheMetadataWatermark(clustered: Bool) async throws {
        let coordinator = try await FakeVSRServer.started()
        let metadataPrimary = try await FakeVSRServer.started()
        defer {
            Task {
                await coordinator.stop()
                await metadataPrimary.stop()
            }
        }
        let commit: UInt64 = 5
        await coordinator.respond { request in
            request.operation == .deleteStream ? .committed([], commit: commit) : FakeVSRServer.defaultReply(request, address: coordinator.address)
        }
        let client = try await connectedClient(coordinator)
        if clustered {
            let roster = FakeVSRServer.rosterBody([coordinator.address, metadataPrimary.address], leaderIndex: 1)
            client.core.pollRouter.learnRoster(try Responses.clusterMetadata(roster[...]))
        }
        try await client.deleteStream(1)
        _ = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: 0, strategy: .first, count: 1, autoCommit: false)
        let lookups = await requests(clustered ? metadataPrimary : coordinator, .getPartitionRoute)
        #expect(lookups.map(\.context.metadataOp) == [commit])
        await client.shutdown()
    }

    /// A leader the roster does not report healthy is not taken for the
    /// metadata primary, as the leader check does not move to it either. The
    /// lookup goes to the coordinator, as while the roster names no leader.
    @Test func aLeaderThatIsNotHealthyIsNotAskedForTheRoute() async throws {
        let leader = try await FakeVSRServer.started()
        let coordinator = try await FakeVSRServer.started()
        defer {
            Task {
                await coordinator.stop()
                await leader.stop()
            }
        }
        let client = try await connectedClient(coordinator)
        let roster = FakeVSRServer.rosterBody([coordinator.address, leader.address], leaderIndex: 1, leaderStatus: .starting)
        await coordinator.respond { request in
            request.code == CommandCode.getClusterMetadata.rawValue ? .ok(roster) : FakeVSRServer.defaultReply(request, address: coordinator.address)
        }
        client.core.pollRouter.learnRoster(try Responses.clusterMetadata(roster[...]))
        _ = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: 0, strategy: .first, count: 1, autoCommit: false)
        #expect(await requests(leader, .getPartitionRoute).isEmpty)
        #expect(await requests(coordinator, .getPartitionRoute).count == 1)
        await client.shutdown()
    }

    /// A metadata commit that arrives while a lookup binds its data
    /// connection leaves the lookup under an old watermark, so the client
    /// refuses to write it. That refusal says nothing about the metadata
    /// primary: the lookup goes to it again at the new watermark instead of
    /// falling back to the coordinator.
    @Test func aLookupTheClientRefusesKeepsTheMetadataPrimary() async throws {
        let metadataPrimary = try await FakeVSRServer.started()
        let coordinator = try await FakeVSRServer.started()
        defer {
            Task {
                await coordinator.stop()
                await metadataPrimary.stop()
            }
        }
        let bindReleased = Locked(false)
        await metadataPrimary.respond { request in
            if request.code == CommandCode.bindSession.rawValue {
                while !bindReleased.withLock({ $0 }) {
                    try? await Task.sleep(for: .milliseconds(5))
                }
            }
            return FakeVSRServer.defaultReply(request, address: metadataPrimary.address)
        }
        await coordinator.respond { request in
            request.operation == .deleteStream ? .committed([], commit: 5) : FakeVSRServer.defaultReply(request, address: coordinator.address)
        }
        let client = try await connectedClient(coordinator)
        let roster = FakeVSRServer.rosterBody([coordinator.address, metadataPrimary.address], leaderIndex: 1)
        client.core.pollRouter.learnRoster(try Responses.clusterMetadata(roster[...]))
        let poll = Task {
            try await client.pollMessages(streamID: 1, topicID: 2, partitionID: 0, strategy: .first, count: 1, autoCommit: false)
        }
        #expect(await eventually { await !requests(metadataPrimary, .bindSession).isEmpty })
        try await client.deleteStream(1)
        bindReleased.withLock { $0 = true }
        _ = try await poll.value
        #expect(await requests(metadataPrimary, .getPartitionRoute).count == 1)
        #expect(await requests(coordinator, .getPartitionRoute).isEmpty)
        await client.shutdown()
    }

    /// A miss of a lookup over the coordinator is final only while the
    /// connection that carried it stands. Once that connection is replaced
    /// before the roster read, nothing tells which node missed, so the
    /// lookup goes again.
    @Test func aLookupMissOverACoordinatorThatReconnectedIsRetried() async throws {
        let primary = try await FakeVSRServer.started()
        let coordinator = try await FakeVSRServer.started()
        defer {
            Task {
                await coordinator.stop()
                await primary.stop()
            }
        }
        let lookups = Locked(0)
        let dropped = Locked(false)
        await coordinator.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .getClusterMetadata:
                if lookups.withLock({ $0 }) == 1, !dropped.exchange(true) {
                    return .close
                }
                return .ok(FakeVSRServer.rosterBody([coordinator.address, primary.address]))
            case .getPartitionRoute:
                let first = lookups.withLock {
                    $0 += 1; return $0 == 1
                }
                return first
                    ? .status(IggyErrorCode.topicIdNotFound.rawValue)
                    : .ok(FakeVSRServer.partitionRouteBody(FakeVSRServer.routeContext, primaryReplica: 1))
            default:
                return FakeVSRServer.defaultReply(request, address: coordinator.address)
            }
        }
        let client = try await connectedClient(coordinator)
        _ = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: 0, strategy: .first, count: 1, autoCommit: false)
        #expect(lookups.withLock { $0 } == 2)
        #expect(dropped.withLock { $0 })
        #expect(await requests(primary, .pollMessages).count == 1)
        await client.shutdown()
    }

    @Test func namespaceMissesFromABackupPrimaryUntilTheDeadlineEndAsNotAccepted() async throws {
        let primary = try await FakeVSRServer.started()
        let coordinator = try await Self.coordinator(routingTo: primary)
        defer {
            Task {
                await coordinator.stop()
                await primary.stop()
            }
        }
        await primary.respond { request in
            if request.code == CommandCode.pollMessages.rawValue {
                return .status(IggyErrorCode.partitionNotFound.rawValue)
            }
            return FakeVSRServer.defaultReply(request, address: primary.address)
        }
        let client = try await connectedClient(coordinator)
        let payload = try Requests.pollMessages(
            consumer: .consumer(5), streamID: 1, topicID: 2, partitionID: 0, strategy: .first, count: 1, autoCommit: false)
        let key = PollRouter.RouteKey(stream: 1, topic: 2, partitionID: 0)
        await expectCode(.transientNotAccepted) {
            _ = try await client.core.pollRouter.send(
                .pollMessages, payload: payload, key: key, context: nil, lineage: await client.core.lineage, coordinator: client.core,
                deadline: .now + .milliseconds(500))
        }
        // Every miss dropped the route, so every poll took a new lookup.
        let polls = await requests(primary, .pollMessages).count
        #expect(polls > 1)
        #expect(await requests(coordinator, .getPartitionRoute).count >= polls)
        await client.shutdown()
    }

    /// A lookup starts at the metadata primary but does not depend on it.
    /// When the roster names a leader the client cannot reach, replica 2
    /// here, or names none, as during an election, the coordinator answers.
    @Test(arguments: [2, nil])
    func aLookupFallsBackToTheCoordinatorWithoutAReachableMetadataPrimary(_ leaderIndex: Int?) async throws {
        let primary = try await FakeVSRServer.started()
        let coordinator = try await Self.coordinator(routingTo: primary)
        defer {
            Task {
                await coordinator.stop()
                await primary.stop()
            }
        }
        let client = try await connectedClient(coordinator)
        let roster = FakeVSRServer.rosterBody([coordinator.address, primary.address, unreachableAddress], leaderIndex: leaderIndex)
        await coordinator.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .getClusterMetadata:
                return .ok(roster)
            case .getPartitionRoute:
                return .ok(FakeVSRServer.partitionRouteBody(FakeVSRServer.routeContext, primaryReplica: 1))
            default:
                return FakeVSRServer.defaultReply(request, address: coordinator.address)
            }
        }
        client.core.pollRouter.learnRoster(try Responses.clusterMetadata(roster[...]))
        let payload = try Requests.pollMessages(
            consumer: .consumer(5), streamID: 1, topicID: 2, partitionID: 0, strategy: .first, count: 1, autoCommit: false)
        let key = PollRouter.RouteKey(stream: 1, topic: 2, partitionID: 0)
        _ = try await client.core.pollRouter.send(
            .pollMessages, payload: payload, key: key, context: nil, lineage: await client.core.lineage, coordinator: client.core, deadline: .now + .seconds(3))
        #expect(await requests(coordinator, .getPartitionRoute).count == 1)
        #expect(await requests(primary, .pollMessages).count == 1)
        await client.shutdown()
    }

    /// The coordinator can be a backup that has not applied the topic, so
    /// its miss of a lookup in place of the metadata primary is retried.
    @Test func aCoordinatorMissInPlaceOfTheMetadataPrimaryIsRetried() async throws {
        let primary = try await FakeVSRServer.started()
        let coordinator = try await FakeVSRServer.started()
        defer {
            Task {
                await coordinator.stop()
                await primary.stop()
            }
        }
        let client = try await connectedClient(coordinator)
        let roster = FakeVSRServer.rosterBody([coordinator.address, primary.address], leaderIndex: nil)
        let missed = Locked(false)
        await coordinator.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .getClusterMetadata:
                return .ok(roster)
            case .getPartitionRoute:
                if !missed.exchange(true) {
                    return .status(IggyErrorCode.topicIdNotFound.rawValue)
                }
                return .ok(FakeVSRServer.partitionRouteBody(FakeVSRServer.routeContext, primaryReplica: 1))
            default:
                return FakeVSRServer.defaultReply(request, address: coordinator.address)
            }
        }
        client.core.pollRouter.learnRoster(try Responses.clusterMetadata(roster[...]))
        let payload = try Requests.pollMessages(
            consumer: .consumer(5), streamID: 1, topicID: 2, partitionID: 0, strategy: .first, count: 1, autoCommit: false)
        let key = PollRouter.RouteKey(stream: 1, topic: 2, partitionID: 0)
        _ = try await client.core.pollRouter.send(
            .pollMessages, payload: payload, key: key, context: nil, lineage: await client.core.lineage, coordinator: client.core, deadline: .now + .seconds(3))
        #expect(await requests(coordinator, .getPartitionRoute).count == 2)
        #expect(await requests(primary, .pollMessages).count == 1)
        await client.shutdown()
    }

    /// A view change the client has not seen leaves it taking a backup, the
    /// coordinator here, for the metadata primary. The roster is read again
    /// before a miss counts as final, so the backup's miss of the lookup or
    /// of the poll is retried, with the route from the new metadata primary.
    @Test(arguments: [CommandCode.getPartitionRoute, .pollMessages])
    func aMissFromAFormerMetadataPrimaryIsRetried(_ missing: CommandCode) async throws {
        let leader = try await FakeVSRServer.started()
        let coordinator = try await FakeVSRServer.started()
        defer {
            Task {
                await coordinator.stop()
                await leader.stop()
            }
        }
        let viewChanged = Locked(false)
        let missed = Locked(false)
        let route = FakeVSRServer.partitionRouteBody(FakeVSRServer.routeContext, primaryReplica: 0)
        await coordinator.respond { request in
            if request.code == missing.rawValue, !missed.exchange(true) {
                return .status(IggyErrorCode.topicIdNotFound.rawValue)
            }
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .getClusterMetadata:
                return .ok(FakeVSRServer.rosterBody([coordinator.address, leader.address], leaderIndex: viewChanged.withLock { $0 } ? 1 : 0))
            case .getPartitionRoute:
                return .ok(route)
            default:
                return FakeVSRServer.defaultReply(request, address: coordinator.address)
            }
        }
        await leader.respond { request in
            request.code == CommandCode.getPartitionRoute.rawValue ? .ok(route) : FakeVSRServer.defaultReply(request, address: leader.address)
        }
        let client = try await connectedClient(coordinator)
        viewChanged.withLock { $0 = true }
        _ = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: 0, strategy: .first, count: 1, autoCommit: false)
        #expect(await requests(leader, .getPartitionRoute).count == 1)
        #expect(await requests(coordinator, .pollMessages).count == (missing == .pollMessages ? 2 : 1))
        await client.shutdown()
    }

    /// A reply that refuses a routed call as a stale client ends the call
    /// with that refusal, as the Rust SDK ends any call, whether it refuses
    /// the route lookup or the request: neither was admitted.
    @Test(arguments: [CommandCode.getPartitionRoute, .pollMessages, .storeConsumerOffset, .deleteConsumerOffset], [false, true])
    func aStaleClientRefusalEndsARoutedCall(_ refused: CommandCode, clustered: Bool) async throws {
        let coordinator = try await FakeVSRServer.started()
        let primary = try await FakeVSRServer.started()
        defer {
            Task {
                await coordinator.stop()
                await primary.stop()
            }
        }
        let roster = FakeVSRServer.rosterBody(clustered ? [coordinator.address, primary.address] : [coordinator.address])
        let route = FakeVSRServer.partitionRouteBody(FakeVSRServer.routeContext, primaryReplica: clustered ? 1 : 0)
        let target = clustered && refused != .getPartitionRoute ? primary : coordinator
        let answered = Locked(false)
        for server in [coordinator, primary] {
            await server.respond { request in
                if server === target, request.code == refused.rawValue, !answered.exchange(true) {
                    return .status(IggyErrorCode.staleClient.rawValue)
                }
                switch request.code.flatMap(CommandCode.init(rawValue:)) {
                case .getClusterMetadata:
                    return .ok(roster)
                case .getPartitionRoute:
                    return .ok(route)
                default:
                    return FakeVSRServer.defaultReply(request, address: server.address)
                }
            }
        }
        let client = try await connectedClient(coordinator)

        await expectCode(.staleClient) {
            switch refused {
            case .storeConsumerOffset:
                try await client.storeConsumerOffset(streamID: 1, topicID: 2, partitionID: 0, offset: 7)
            case .deleteConsumerOffset:
                try await client.deleteConsumerOffset(streamID: 1, topicID: 2, partitionID: 0)
            default:
                _ = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: 0, strategy: .first, count: 1, autoCommit: false)
            }
        }

        #expect(await requests(target, refused).count == 1)
        await client.shutdown()
    }

    /// An offset write can commit before an eviction ends its session on the
    /// data connection, so it is not sent again: the call ends with the
    /// unknown outcome after one send.
    @Test(arguments: FakeVSRServer.sessionEndingEvictions, [CommandCode.storeConsumerOffset, .deleteConsumerOffset])
    func anOffsetWriteEvictedWithItsSessionIsSentOnceAndStaysUnknown(_ reason: VSRFrame.EvictionReason, _ write: CommandCode) async throws {
        let primary = try await FakeVSRServer.started()
        let coordinator = try await Self.coordinator(routingTo: primary)
        defer {
            Task {
                await coordinator.stop()
                await primary.stop()
            }
        }
        await primary.respond { request in
            if request.code == write.rawValue {
                return .eviction(reason)
            }
            return FakeVSRServer.defaultReply(request, address: primary.address)
        }
        let client = try await connectedClient(coordinator, requestTimeout: .seconds(2))

        await expectCode(.transientNotCommitted) {
            if write == .storeConsumerOffset {
                try await client.storeConsumerOffset(streamID: 1, topicID: 2, partitionID: 0, offset: 7)
            } else {
                try await client.deleteConsumerOffset(streamID: 1, topicID: 2, partitionID: 0)
            }
        }

        #expect(await requests(primary, write).count == 1)
        await client.shutdown()
    }

    /// An offset store is a write. A backup primary's miss refused it before
    /// admission, so the store goes again on a new route.
    @Test func anOffsetStoreMissedByABackupPrimaryIsSentAgain() async throws {
        let primary = try await FakeVSRServer.started()
        let coordinator = try await Self.coordinator(routingTo: primary)
        defer {
            Task {
                await coordinator.stop()
                await primary.stop()
            }
        }
        let missed = Locked(false)
        await primary.respond { request in
            if request.code == CommandCode.storeConsumerOffset.rawValue, !missed.exchange(true) {
                return .status(IggyErrorCode.topicIdNotFound.rawValue)
            }
            return FakeVSRServer.defaultReply(request, address: primary.address)
        }
        let client = try await connectedClient(coordinator)
        try await client.storeConsumerOffset(consumer: .consumer(5), streamID: 1, topicID: 2, partitionID: 0, offset: 7)
        #expect(await requests(primary, .storeConsumerOffset).count == 2)
        #expect(await requests(coordinator, .getPartitionRoute).count == 2)
        await client.shutdown()
    }

    // MARK: Unknown outcomes

    /// A partition write answered "not committed" may still have committed,
    /// so it goes again under its request id, as in the Rust SDK: the server
    /// answers the replay from its reply cache if the write committed.
    @Test(arguments: partitionWritePaths)
    func anUncommittedPartitionWriteGoesAgainUnderItsRequestID(_ write: CommandCode, clustered: Bool) async throws {
        let (attempts, failure) = try await Self.uncommittedWrite(write, clustered: clustered, uncommitted: 1)
        #expect(failure == nil)
        #expect(attempts.count == 2)
        #expect(Set(attempts.map(\.requestID)).count == 1)
    }

    /// A partition write that stays "not committed" until its deadline ends
    /// with that unknown outcome, after replays under its own request id.
    @Test(arguments: partitionWritePaths)
    func aPartitionWriteUncommittedUntilItsDeadlineStaysUnknown(_ write: CommandCode, clustered: Bool) async throws {
        let (attempts, failure) = try await Self.uncommittedWrite(write, clustered: clustered, uncommitted: nil)
        #expect(failure == .transientNotCommitted)
        #expect(attempts.count > 1)
        #expect(Set(attempts.map(\.requestID)).count == 1)
    }

    /// A write lost with its data connection goes again under its request id
    /// on the next one, also after a stale client eviction, which ends only
    /// the binding of the connection.
    @Test(arguments: [1, 3], [FakeVSRServer.Reply.close, .statusThenClose(IggyErrorCode.transientNotCommitted.rawValue), .eviction(.staleClient)])
    func anOffsetWriteReplaysAcrossLostDataConnections(_ losses: Int, loss: FakeVSRServer.Reply) async throws {
        let primary = try await FakeVSRServer.started()
        let coordinator = try await Self.coordinator(routingTo: primary)
        defer {
            Task {
                await coordinator.stop(); await primary.stop()
            }
        }
        let attempts = Locked(0)
        await primary.respond { request in
            if request.code == CommandCode.storeConsumerOffset.rawValue {
                let attempt = attempts.withLock {
                    $0 += 1; return $0
                }
                if attempt <= losses { return loss }
            }
            return FakeVSRServer.defaultReply(request, address: primary.address)
        }
        let client = try await connectedClient(coordinator, requestTimeout: .seconds(3))
        defer { Task { await client.shutdown() } }

        try await client.storeConsumerOffset(streamID: 1, topicID: 2, partitionID: 0, offset: 7)

        let writes = await requests(primary, .storeConsumerOffset)
        #expect(writes.count == losses + 1)
        #expect(Set(writes.map(\.connection)).count == losses + 1)
        #expect(Set(writes.map(\.requestID)).count == 1)
        #expect(Set(writes.map(\.session)).count == 1)
        #expect(Set(writes.map(\.clientID)).count == 1)
        #expect(Set(writes.map(\.checksum)).count == 1)
        #expect(await requests(primary, .bindSession).count == losses + 1)
        #expect(await requests(primary, .loginRegister).isEmpty)
    }

    @Test func anUncommittedOffsetWriteFollowsTheNewPrimaryWithItsFrame() async throws {
        let first = try await FakeVSRServer.started()
        let second = try await FakeVSRServer.started()
        let coordinator = try await FakeVSRServer.started()
        defer {
            Task {
                await coordinator.stop(); await first.stop(); await second.stop()
            }
        }
        let moved = Locked(false)
        let attempted = Locked(false)
        await coordinator.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .getClusterMetadata:
                return .ok(FakeVSRServer.rosterBody([coordinator.address, first.address, second.address]))
            case .getPartitionRoute:
                let hasMoved = moved.withLock { $0 }
                return .ok(FakeVSRServer.partitionRouteBody(FakeVSRServer.routeContext, primaryReplica: hasMoved ? 2 : 1, view: hasMoved ? 2 : 1))
            default:
                return FakeVSRServer.defaultReply(request, address: coordinator.address)
            }
        }
        await first.respond { request in
            if request.code == CommandCode.storeConsumerOffset.rawValue {
                if !attempted.exchange(true) { return .status(IggyErrorCode.transientNotCommitted.rawValue) }
                moved.withLock { $0 = true }
                return .status(IggyErrorCode.notPrimary.rawValue)
            }
            return FakeVSRServer.defaultReply(request, address: first.address)
        }
        let client = try await connectedClient(coordinator, requestTimeout: .seconds(2))
        defer { Task { await client.shutdown() } }

        try await client.storeConsumerOffset(streamID: 1, topicID: 2, partitionID: 0, offset: 7)

        let previous = await requests(first, .storeConsumerOffset)
        let next = await requests(second, .storeConsumerOffset)
        #expect(previous.count == 2)
        #expect(next.count == 1)
        #expect(Set((previous + next).map(\.requestID)).count == 1)
        #expect(Set((previous + next).map(\.checksum)).count == 1)
        #expect(Set((previous + next).map(\.clientID)).count == 1)
        #expect(Set((previous + next).map(\.session)).count == 1)
        #expect(await requests(coordinator, .getPartitionRoute).count == 2)
    }

    @Test func aRefusedBindClosesTheReplacedDataConnection() async throws {
        let primary = try await FakeVSRServer.started()
        let coordinator = try await Self.coordinator(routingTo: primary)
        defer {
            Task {
                await coordinator.stop(); await primary.stop()
            }
        }
        let refused = Locked(false)
        await primary.respond { request in
            if request.code == CommandCode.bindSession.rawValue, !refused.exchange(true) {
                return .status(IggyErrorCode.transientNotAccepted.rawValue)
            }
            return FakeVSRServer.defaultReply(request, address: primary.address)
        }
        let client = try await connectedClient(coordinator)
        defer { Task { await client.shutdown() } }

        try await client.storeConsumerOffset(streamID: 1, topicID: 2, partitionID: 0, offset: 7)

        #expect(await primary.connectionCount == 2)
        #expect(await eventually { await primary.openConnectionCount == 1 })
        #expect(await requests(primary, .storeConsumerOffset).count == 1)
    }

    /// Sends `write` to a target that answers it "not committed" for the
    /// first `uncommitted` attempts, or for every one when nil, and OK after:
    /// the partition primary for a store in a cluster, else the coordinator.
    /// Returns the attempts the target got and the code the call failed with.
    private static func uncommittedWrite(
        _ write: CommandCode, clustered: Bool, uncommitted: Int?
    ) async throws -> (attempts: [FakeVSRServer.Request], failure: IggyErrorCode?) {
        let primary = try await FakeVSRServer.started()
        let coordinator = try await FakeVSRServer.started()
        defer {
            Task {
                await coordinator.stop()
                await primary.stop()
            }
        }
        let target = clustered && write == .storeConsumerOffset ? primary : coordinator
        let answered = Locked(0)
        let reply: @Sendable (FakeVSRServer.Request, FakeVSRServer) -> FakeVSRServer.Reply = { request, server in
            if server === target, request.code == write.rawValue {
                let attempt = answered.withLock {
                    $0 += 1; return $0
                }
                if uncommitted.map({ attempt <= $0 }) ?? true {
                    return .status(IggyErrorCode.transientNotCommitted.rawValue)
                }
            }
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .getClusterMetadata where clustered:
                return .ok(FakeVSRServer.rosterBody([coordinator.address, primary.address]))
            case .getPartitionRoute where clustered:
                return .ok(FakeVSRServer.partitionRouteBody(FakeVSRServer.routeContext, primaryReplica: 1))
            default:
                return FakeVSRServer.defaultReply(request, address: server.address)
            }
        }
        await coordinator.respond { reply($0, coordinator) }
        await primary.respond { reply($0, primary) }
        let client = try await connectedClient(coordinator, requestTimeout: .milliseconds(500))
        var failure: IggyErrorCode?
        do {
            if write == .sendMessages {
                try await client.sendBatch(streamID: 1, topicID: 2, partitioning: .partition(1), messages: ["a"])
            } else {
                try await client.storeConsumerOffset(consumer: .consumer(5), streamID: 1, topicID: 2, partitionID: 1, offset: 9)
            }
        } catch let error as IggyError {
            failure = error.code
        }
        await client.shutdown()
        return (await requests(target, write), failure)
    }

    /// A send goes over the coordinator, which a view change the client has
    /// not seen can leave on a backup. Its miss of the route lookup or of the
    /// send is final only once a roster read confirms it as the metadata
    /// primary, so here the send goes again on a route from the new one.
    @Test(arguments: [CommandCode.getPartitionRoute, .sendMessages])
    func aSendMissedByAFormerMetadataPrimaryIsSentAgain(_ missing: CommandCode) async throws {
        let leader = try await FakeVSRServer.started()
        let coordinator = try await FakeVSRServer.started()
        defer {
            Task {
                await coordinator.stop()
                await leader.stop()
            }
        }
        let viewChanged = Locked(false)
        let missed = Locked(false)
        let route = FakeVSRServer.partitionRouteBody(FakeVSRServer.routeContext, primaryReplica: 0)
        await coordinator.respond { request in
            if request.code == missing.rawValue, !missed.exchange(true) {
                return .status(IggyErrorCode.topicIdNotFound.rawValue)
            }
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .getClusterMetadata:
                return .ok(FakeVSRServer.rosterBody([coordinator.address, leader.address], leaderIndex: viewChanged.withLock { $0 } ? 1 : 0))
            case .getPartitionRoute:
                return .ok(route)
            default:
                return FakeVSRServer.defaultReply(request, address: coordinator.address)
            }
        }
        await leader.respond { request in
            request.code == CommandCode.getPartitionRoute.rawValue ? .ok(route) : FakeVSRServer.defaultReply(request, address: leader.address)
        }
        let client = try await connectedClient(coordinator)
        viewChanged.withLock { $0 = true }
        try await client.sendBatch(streamID: 1, topicID: 2, partitioning: .partition(0), messages: ["a"])
        #expect(await requests(leader, .getPartitionRoute).count == 1)
        #expect(await requests(coordinator, .sendMessages).count == (missing == .sendMessages ? 2 : 1))
        #expect(await requests(leader, .sendMessages).isEmpty)
        await client.shutdown()
    }

    /// A coordinator at the metadata primary that a roster read confirms has
    /// applied every metadata commit, so its miss of the route lookup or of
    /// the send is final.
    @Test(arguments: [CommandCode.getPartitionRoute, .sendMessages])
    func aSendMissedByTheMetadataPrimaryIsFinal(_ missing: CommandCode) async throws {
        let backup = try await FakeVSRServer.started()
        let coordinator = try await FakeVSRServer.started()
        defer {
            Task {
                await coordinator.stop()
                await backup.stop()
            }
        }
        await coordinator.respond { request in
            if request.code == missing.rawValue {
                return .status(IggyErrorCode.topicIdNotFound.rawValue)
            }
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .getClusterMetadata:
                return .ok(FakeVSRServer.rosterBody([coordinator.address, backup.address]))
            case .getPartitionRoute:
                return .ok(FakeVSRServer.partitionRouteBody(FakeVSRServer.routeContext, primaryReplica: 1))
            default:
                return FakeVSRServer.defaultReply(request, address: coordinator.address)
            }
        }
        let client = try await connectedClient(coordinator)
        let rosterReads = await requests(coordinator, .getClusterMetadata).count
        await expectCode(.topicIdNotFound) {
            try await client.sendBatch(streamID: 1, topicID: 2, partitioning: .partition(0), messages: ["a"])
        }
        #expect(await requests(coordinator, missing).count == 1)
        #expect(await requests(coordinator, .getClusterMetadata).count == rosterReads + 1)
        await client.shutdown()
    }

    /// A coordinator that stays behind the metadata primary misses every
    /// send. Each miss is refused and the send goes again on a fresh route
    /// until the request timeout ends it as not accepted.
    @Test func sendMissesFromABackupCoordinatorUntilTheDeadlineEndAsNotAccepted() async throws {
        let leader = try await FakeVSRServer.started()
        let coordinator = try await FakeVSRServer.started()
        defer {
            Task {
                await coordinator.stop()
                await leader.stop()
            }
        }
        let viewChanged = Locked(false)
        let route = FakeVSRServer.partitionRouteBody(FakeVSRServer.routeContext, primaryReplica: 0)
        await coordinator.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .getClusterMetadata:
                return .ok(FakeVSRServer.rosterBody([coordinator.address, leader.address], leaderIndex: viewChanged.withLock { $0 } ? 1 : 0))
            case .getPartitionRoute:
                return .ok(route)
            case .sendMessages:
                return .status(IggyErrorCode.topicIdNotFound.rawValue)
            default:
                return FakeVSRServer.defaultReply(request, address: coordinator.address)
            }
        }
        await leader.respond { request in
            request.code == CommandCode.getPartitionRoute.rawValue ? .ok(route) : FakeVSRServer.defaultReply(request, address: leader.address)
        }
        let requestTimeout = Duration.seconds(1)
        let client = try await connectedClient(coordinator, requestTimeout: requestTimeout)
        viewChanged.withLock { $0 = true }
        let started = ContinuousClock.now
        await expectCode(.transientNotAccepted) {
            try await client.sendBatch(streamID: 1, topicID: 2, partitioning: .partition(0), messages: ["a"])
        }
        #expect(ContinuousClock.now - started < requestTimeout * 2)
        #expect(await requests(coordinator, .sendMessages).count > 1)
        #expect(await !requests(leader, .getPartitionRoute).isEmpty)
        await client.shutdown()
    }
}
