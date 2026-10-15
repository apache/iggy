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
private func connectedClient(_ server: FakeVSRServer, heartbeat: Duration = .seconds(60)) async throws -> IggyClient {
    var configuration = ClientConfiguration(address: server.address, autoLogin: .usernamePassword(username: "iggy", password: "iggy"))
    configuration.reconnection = ReconnectionOptions(maxRetries: nil, interval: .milliseconds(20), reestablishAfter: .zero)
    configuration.requestTimeout = .seconds(10)
    configuration.connectTimeout = .seconds(2)
    configuration.heartbeatInterval = heartbeat
    let client = IggyClient(configuration: configuration, connector: NIOConnector(), logger: testLogger)
    try await client.connect()
    return client
}

private func requests(_ server: FakeVSRServer, _ code: CommandCode) async -> [FakeVSRServer.Request] {
    await server.requests.filter { $0.code == code.rawValue }
}

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

@Suite("Messaging", .serialized)
struct MessagingTests {
    // MARK: Sends

    @Test func sendsCaptureTheRouteContextOnce() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = try await connectedClient(server)
        try await client.sendMessages(streamID: 1, topicID: 2, partitioning: .partition(1), messages: ["a"])
        try await client.sendMessages(streamID: 1, topicID: 2, partitioning: .partition(1), messages: ["b"])
        let routes = await requests(server, .getPartitionRoute)
        #expect(routes.count == 1)
        #expect(routes.first?.payload == (try Requests.partitionRoute(streamID: 1, topicID: 2, partitionID: 1)))
        let sends = await requests(server, .sendMessages)
        #expect(sends.count == 2)
        // A route names no consumer, so the send carries owner 0.
        #expect(sends.allSatisfy { $0.context == FakeVSRServer.routeContext })
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
        try await client.sendMessages(streamID: 1, topicID: 2, partitioning: .partition(0), messages: ["a"])
        let sends = await requests(server, .sendMessages)
        #expect(sends.map(\.context) == [FakeVSRServer.routeContext, PartitionContext(incarnation: 8, ownerGeneration: 0, metadataOp: 30)])
        #expect(sends[0].requestID != sends[1].requestID)

        // A second refusal in a row ends the send.
        refusals.withLock { $0 = 2 }
        await expectCode(.historyUnavailable) {
            try await client.sendMessages(streamID: 1, topicID: 2, partitioning: .partition(0), messages: ["b"])
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
            try await client.sendMessages(streamID: 1, topicID: 2, messages: [IggyMessage(stringLiteral: text)])
        }
        let sends = await requests(server, .sendMessages)
        #expect(sends.map(sentPartition) == [0, 1, 2, 0])
        // The topic read cached the count and the contexts: no route lookups.
        #expect(sends.map(\.context) == [contexts[0], contexts[1], contexts[2], contexts[0]])
        #expect(await requests(server, .getTopic).count == 1)
        #expect(await requests(server, .getPartitionRoute).isEmpty)

        try await client.sendMessages(streamID: 1, topicID: 2, partitioning: .messagesKey("user-1"), messages: ["e"])
        #expect(await requests(server, .sendMessages).last.map(sentPartition) == XXH32.hash(Array("user-1".utf8)) % 3)

        // A deleted topic can come back with other partitions.
        try await client.deleteTopic(streamID: 1, topicID: 9)
        try await client.sendMessages(streamID: 1, topicID: 2, messages: ["f"])
        #expect(await requests(server, .getTopic).count == 2)
        await client.shutdown()
    }

    @Test func aBalancedSendToAMissingTopicFails() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = try await connectedClient(server)
        await expectCode(.topicIdNotFound) { try await client.sendMessages(streamID: 1, topicID: 2, messages: ["a"]) }
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
        let continued = PollingStrategy(kind: .offset, value: 3, context: otherContext)
        _ = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: 1, consumer: .consumer(5), strategy: continued, count: 10, autoCommit: true)

        let lookups = await requests(server, .getPollRouting)
        #expect(lookups.count == 1)
        let payload = try Requests.pollMessages(
            consumer: .consumer(5), streamID: 1, topicID: 2, partitionID: 1, strategy: .next, count: 10, autoCommit: true)
        #expect(lookups.first?.payload == payload)
        let polls = await requests(server, .pollMessagesOnPrimary)
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
            if request.code == CommandCode.pollMessagesOnPrimary.rawValue,
                !refused.exchange(true)
            {
                return .status(IggyErrorCode.transientNotAccepted.rawValue)
            }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = try await connectedClient(server)
        _ = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: 0, strategy: .first, count: 1, autoCommit: false)
        #expect(await requests(server, .getPollRouting).count == 2)
        #expect(await requests(server, .pollMessagesOnPrimary).count == 2)
        await client.shutdown()
    }

    @Test func aNewerMetadataCommitReadsTheRouteAgain() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await server.respond { request in
            if request.operation == .deleteStream {
                return .committed([], commit: 50)
            }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = try await connectedClient(server)
        let poll = { _ = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: 0, strategy: .first, count: 1, autoCommit: false) }
        try await poll()
        try await poll()
        #expect(await requests(server, .getPollRouting).count == 1)
        try await client.deleteStream(9)
        #expect(await client.core.metadataWatermark == 50)
        try await poll()
        try await poll()
        // The route read after the commit is as new as the commit.
        #expect(await requests(server, .getPollRouting).count == 2)
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
        let lookups = await requests(server, .getPollRouting)
        #expect(lookups.count == 2)
        #expect(lookups[0].clientID != lookups[1].clientID)
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
            case .pollMessagesOnPrimary:
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
        #expect(await requests(server, .getPollRouting).count == 2)
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
            case .pollMessagesOnPrimary:
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
        #expect(await requests(server, .pollMessagesOnPrimary).count == 2)
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
        #expect(await requests(server, .pollMessagesOnPrimary).isEmpty)
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
            case .pollMessagesOnPrimary where polledPartition(request) == 1:
                return .status(IggyErrorCode.consumerGroupPartitionNotOwned.rawValue)
            case .pollMessagesOnPrimary:
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
        let continued = PollingStrategy(kind: .offset, value: 4, context: otherContext)
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

    @Test func offsetWritesTakeTheOffsetRoute() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = try await connectedClient(server)
        try await client.storeConsumerOffset(consumer: .consumer(5), streamID: 1, topicID: 2, partitionID: 3, offset: 40)
        let position = ConsumerPosition(partitionID: 3, offset: 41, context: otherContext)
        try await client.storeConsumerPosition(consumer: .consumer(5), streamID: 1, topicID: 2, position: position)
        try await client.deleteConsumerOffset(consumer: .consumer(5), streamID: 1, topicID: 2, partitionID: 3)

        let lookups = await requests(server, .getConsumerOffsetRouting)
        #expect(lookups.count == 1)
        #expect(lookups.first?.payload == (try Requests.getConsumerOffset(consumer: .consumer(5), streamID: 1, topicID: 2, partitionID: 3)))
        let stores = await requests(server, .storeConsumerOffset)
        #expect(stores.map(\.context) == [FakeVSRServer.routeContext, otherContext])
        #expect(
            stores.first?.payload
                == (try Requests.storeConsumerOffset(consumer: .consumer(5), streamID: 1, topicID: 2, partitionID: 3, offset: 40, ack: .quorum)))
        #expect(await requests(server, .deleteConsumerOffset).map(\.context) == [FakeVSRServer.routeContext])
        await client.shutdown()
    }

    // MARK: Raw requests

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
        #expect(await requests(server, .pollMessagesOnPrimary).map(\.context) == [FakeVSRServer.routeContext])

        let store = try Requests.storeConsumerOffset(consumer: .consumer(5), streamID: 1, topicID: 2, partitionID: 1, offset: 9, ack: .quorum)
        _ = try await client.sendRawRequest(code: CommandCode.storeConsumerOffset.rawValue, payload: store)
        #expect(
            await requests(server, .getConsumerOffsetRouting).map(\.payload)
                == [try Requests.getConsumerOffset(consumer: .consumer(5), streamID: 1, topicID: 2, partitionID: 1)])

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
        #expect(await server.requests.count == before)
        await client.shutdown()
    }

    // MARK: Clusters

    /// A coordinator that routes every poll and offset write to `primary`.
    private static func coordinator(routingTo primary: FakeVSRServer) async throws -> FakeVSRServer {
        let coordinator = try await FakeVSRServer.started()
        await coordinator.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .getClusterMetadata:
                return .ok(FakeVSRServer.rosterBody([coordinator.address, primary.address]))
            case .getPollRouting, .getConsumerOffsetRouting:
                return .ok(FakeVSRServer.pollRouteBody(for: request, context: FakeVSRServer.routeContext, primary: primary.address, watermark: 3))
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
        #expect(
            binds.first?.payload
                == (try Requests.bindSession(
                    clientID: session.clientID, session: session.session, metadataWatermark: 3, bindSecret: await client.core.bindSecret)))
        let polls = await requests(primary, .pollMessagesOnPrimary)
        #expect(polls.count == 2)
        #expect(polls.allSatisfy { $0.context == FakeVSRServer.routeContext && $0.clientID == session.clientID && $0.session == session.session })
        #expect(await requests(primary, .storeConsumerOffset).count == 1)
        #expect(await requests(coordinator, .pollMessagesOnPrimary).isEmpty)
        #expect(await primary.connectionCount == 1)
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
            if request.code == CommandCode.pollMessagesOnPrimary.rawValue,
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

    @Test func anUnreachablePrimaryIsRoutedAgain() async throws {
        let primary = try await FakeVSRServer.started()
        let gone = try await FakeVSRServer.started()
        let goneAddress = gone.address
        await gone.stop()
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
            case .getPollRouting:
                let first = lookups.withLock {
                    $0 += 1; return $0 == 1
                }
                return .ok(FakeVSRServer.pollRouteBody(for: request, context: FakeVSRServer.routeContext, primary: first ? goneAddress : primary.address))
            default:
                return FakeVSRServer.defaultReply(request, address: coordinator.address)
            }
        }
        let client = try await connectedClient(coordinator)
        _ = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: 0, strategy: .first, count: 1, autoCommit: false)
        #expect(lookups.withLock { $0 } == 2)
        #expect(await requests(primary, .pollMessagesOnPrimary).count == 1)
        await client.shutdown()
    }
}
