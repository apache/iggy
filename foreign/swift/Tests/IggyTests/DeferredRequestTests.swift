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

private func signedInClient(_ server: FakeVSRServer, requestTimeout: Duration = .seconds(30)) async throws -> IggyClient {
    var configuration = ClientConfiguration(address: server.address, autoLogin: .usernamePassword(username: "iggy", password: "iggy"))
    configuration.reconnection = ReconnectionOptions(maxRetries: nil, interval: .milliseconds(20), reestablishAfter: .zero)
    configuration.requestTimeout = requestTimeout
    configuration.connectTimeout = .seconds(2)
    configuration.heartbeatInterval = .seconds(60)
    let client = IggyClient(configuration: configuration, connector: NIOConnector(), logger: testLogger)
    try await client.connect()
    return client
}

private func requests(_ server: FakeVSRServer, _ code: CommandCode) async -> [FakeVSRServer.Request] {
    await server.requests.filter { $0.code == code.rawValue }
}

/// The partition a poll names: `[kind:1][consumer:6][stream:6][topic:6][flag:1][partition:4]`
/// with numeric ids.
private func polledPartition(_ request: FakeVSRServer.Request) -> UInt32 {
    UInt32(littleEndianBytes: request.payload[20..<24])
}

/// A coordinator, replica 0, and primaries where replica `n` is the primary
/// of partition `n`.
private struct Cluster {
    let coordinator: FakeVSRServer
    let primaries: [FakeVSRServer]

    static func started(primaries count: Int) async throws -> Cluster {
        let coordinator = try await FakeVSRServer.started()
        var primaries: [FakeVSRServer] = []
        for _ in 0..<count {
            primaries.append(try await FakeVSRServer.started())
        }
        let cluster = Cluster(coordinator: coordinator, primaries: primaries)
        for server in [coordinator] + primaries {
            await cluster.respond(on: server) { FakeVSRServer.defaultReply($0, address: server.address) }
        }
        return cluster
    }

    /// Answers a roster read and a route lookup as every replica of the
    /// cluster does, and anything else with `reply`.
    func respond(on server: FakeVSRServer, _ reply: @escaping FakeVSRServer.Responder) async {
        let roster = FakeVSRServer.rosterBody([coordinator.address] + primaries.map(\.address))
        await server.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .getClusterMetadata:
                return .ok(roster)
            case .getPartitionRoute:
                let partition = UInt32(littleEndianBytes: request.payload.suffix(4))
                return .ok(FakeVSRServer.partitionRouteBody(FakeVSRServer.routeContext, primaryReplica: UInt8(partition)))
            default:
                return await reply(request)
            }
        }
    }

    func stop() async {
        await coordinator.stop()
        for primary in primaries {
            await primary.stop()
        }
    }
}

@Suite("Deferred requests", .serialized)
struct DeferredRequestTests {
    private static let parkingOptions = DeferredPollOptions(maxWait: .seconds(5), requestTimeout: .seconds(10))
    /// Longer than `shortOptions` allow, so a call they bound ends well
    /// before a request of the client would.
    private static let clientRequestTimeout: Duration = .seconds(8)
    private static let shortOptions = DeferredPollOptions(maxWait: .seconds(1), requestTimeout: .seconds(2))

    // MARK: Polls

    @Test func aDeferredPollGoesOverADataConnectionWithWhatIsLeftOfItsOptions() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = try await signedInClient(server)
        let options = DeferredPollOptions(maxWait: .milliseconds(500), minCount: 2, maxBytes: 4096, requestTimeout: .seconds(3))
        let polled = try await client.pollMessages(
            streamID: 1, topicID: 2, partitionID: 1, consumer: .consumer(5), strategy: .next, count: 10, autoCommit: true, options: options)
        #expect(polled.context == FakeVSRServer.routeContext)

        let poll = try #require(await requests(server, .pollMessagesDeferred).first)
        let body = try Requests.pollMessages(consumer: .consumer(5), streamID: 1, topicID: 2, partitionID: 1, strategy: .next, count: 10, autoCommit: true)
        #expect(poll.payload.starts(with: body))
        #expect(poll.payload.count == body.count + FakeVSRServer.Request.deferredOptionsSize)
        #expect(poll.context == FakeVSRServer.routeContext)
        let sent = poll.deferredOptions
        #expect(sent.minCount == 2)
        #expect(sent.maxBytes == 4096)
        // The time the call spent before the attempt comes off both.
        #expect(sent.wait <= 500_000)
        #expect(sent.timeout <= 3_000_000)
        #expect(sent.timeout >= sent.wait + 1_000_000)
        // A parked poll would hold the lockstep coordinator connection.
        let login = try #require(await server.requests.first)
        #expect(poll.connection != login.connection)
        #expect(await requests(server, .bindSession).map(\.connection) == [poll.connection])
        #expect(await requests(server, .pollMessages).isEmpty)
        await client.shutdown()
    }

    /// A parked progress poll holds a data connection of its own, so a send
    /// of the same client to its partition goes out at once, as the separate
    /// mutation lane of the Rust SDK lets it.
    @Test func aParkedProgressPollOrdersStoresButDoesNotDelaySends() async throws {
        let server = try await FakeVSRServer.started()
        let reply = AsyncSemaphore(permits: 0)
        defer {
            reply.release(1)
            Task { await server.stop() }
        }
        await server.respond { request in
            if request.code == CommandCode.pollMessagesDeferred.rawValue {
                _ = try? await reply.acquire(1)
            }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = try await signedInClient(server)
        let parked = Task {
            try await client.pollMessages(
                streamID: 1, topicID: 2, partitionID: 1, consumer: .consumer(5), strategy: .next, count: 10, autoCommit: true,
                options: Self.parkingOptions)
        }
        #expect(await eventually { await !requests(server, .pollMessagesDeferred).isEmpty })
        let storing = Task { try await client.storeConsumerOffset(streamID: 1, topicID: 2, partitionID: 1, offset: 9) }
        defer { storing.cancel() }
        let started = ContinuousClock.now
        try await client.sendBatch(streamID: 1, topicID: 2, partitioning: .partition(1), messages: ["sent"])
        let elapsed = ContinuousClock.now - started
        #expect(elapsed < .seconds(1), "took \(elapsed)")
        #expect(await requests(server, .sendMessages).count == 1)
        try await Task.sleep(for: .milliseconds(100))
        #expect(await requests(server, .storeConsumerOffset).isEmpty)
        reply.release(1)
        _ = try await parked.value
        try await storing.value
        #expect(await requests(server, .storeConsumerOffset).count == 1)
        await client.shutdown()
    }

    @Test func optionsTheServerWouldRefuseFailBeforeAnythingIsSent() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = try await signedInClient(server)
        let before = await server.requests.count
        let refusedPolls: [(DeferredPollOptions, IggyErrorCode)] = [
            (DeferredPollOptions(minCount: 11), .invalidMessagesCount), (DeferredPollOptions(maxWait: .seconds(11)), .invalidCommand),
            (DeferredPollOptions(maxBytes: 100), .invalidSizeBytes),
        ]
        for (options, code) in refusedPolls {
            for partitionID: UInt32? in [1, nil] {
                await expectCode(code) {
                    _ = try await client.pollMessages(
                        streamID: 1, topicID: 2, partitionID: partitionID, consumer: .group(3), strategy: .next, count: 10, autoCommit: false,
                        options: options)
                }
            }
        }
        // A fetch of each readiness group replaces a deferred group poll that
        // names no partition.
        await expectCode(.invalidCommand) {
            _ = try await client.pollMessages(
                streamID: 1, topicID: 2, partitionID: nil, consumer: .group(3), strategy: .next, count: 10, autoCommit: false,
                options: Self.parkingOptions)
        }
        #expect(await server.requests.count == before)
        await client.shutdown()
    }

    /// The request timeout bounds the whole call, though the client allows
    /// each request longer. That includes the reconnect of a client that an
    /// outage left disconnected, which the call waits for first. A call the
    /// server would refuse fails before that reconnect.
    @Test func aDeferredPollEndsAReconnectAtItsRequestTimeout() async throws {
        let server = try await FakeVSRServer.started()
        let client = try await signedInClient(server, requestTimeout: Self.clientRequestTimeout)
        await server.stop()
        let poll = { (options: DeferredPollOptions) in
            _ = try await client.pollMessages(
                streamID: 1, topicID: 2, partitionID: 1, consumer: .consumer(5), strategy: .next, count: 10, autoCommit: false, options: options)
        }
        // The first poll finds the connection gone and gives up its reconnect
        // at its deadline, which leaves the client disconnected.
        await #expect(throws: IggyError.self) { try await poll(Self.shortOptions) }
        #expect(await eventually { await client.state == .disconnected })
        let elapsed = await ContinuousClock().measure {
            await expectCode(.transientNotAccepted) { try await poll(Self.shortOptions) }
        }
        #expect(elapsed < Self.clientRequestTimeout - Self.shortOptions.requestTimeout, "took \(elapsed)")
        await expectCode(.invalidMessagesCount) { try await poll(DeferredPollOptions(minCount: 11)) }
        await expectCode(.invalidCommand) {
            _ = try await client.fetchMessages(streamID: 1, topicID: 2, targets: [], count: 1, autoCommit: false)
        }
        await client.shutdown()
    }

    /// A rebalance on every attempt ends an immediate group poll empty, so
    /// its caller polls again.
    @Test func aGroupPollThatMeetsARebalanceOnEveryAttemptEndsEmpty() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await server.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .syncConsumerGroup:
                return .ok(FakeVSRServer.assignmentBody(generation: 1, partitions: [0, 1, 2]))
            case .pollMessages:
                return .status(IggyErrorCode.consumerGroupPartitionNotOwned.rawValue)
            default:
                return FakeVSRServer.defaultReply(request, address: server.address)
            }
        }
        let client = try await signedInClient(server)
        let polled = try await client.pollMessages(
            streamID: 1, topicID: 2, partitionID: nil, consumer: .group(3), strategy: .next, count: 10, autoCommit: true)
        #expect(polled.messages.isEmpty)
        #expect(await requests(server, .syncConsumerGroup).count == 3)
        await client.shutdown()
    }

    // MARK: Cancellation

    @Test func aCancelledRequestIsCancelledOnItsConnection() async throws {
        for code in [CommandCode.pollMessagesDeferred, .fetchMessages] {
            let server = try await FakeVSRServer.started()
            let parked = Locked(true)
            await server.respond { request in
                if request.code == code.rawValue, parked.exchange(false) {
                    return .stall
                }
                return FakeVSRServer.defaultReply(request, address: server.address)
            }
            let client = try await signedInClient(server)
            let send = { @Sendable in
                switch code {
                case .fetchMessages:
                    _ = try await client.fetchMessages(
                        streamID: 1, topicID: 2, targets: [(1, .next)], count: 1, autoCommit: false, options: Self.parkingOptions)
                default:
                    _ = try await client.pollMessages(
                        streamID: 1, topicID: 2, partitionID: 1, strategy: .next, count: 1, autoCommit: false, options: Self.parkingOptions)
                }
            }
            let parkedRequest = Task { try await send() }
            #expect(await eventually { await requests(server, code).count == 1 })
            parkedRequest.cancel()
            await #expect(throws: CancellationError.self, "\(code)") { try await parkedRequest.value }
            #expect(await eventually { await requests(server, .cancelDeferredRequest).count == 1 }, "\(code)")
            let request = try #require(await requests(server, code).first)
            let cancellation = try #require(await requests(server, .cancelDeferredRequest).first)
            #expect(cancellation.connection == request.connection, "\(code)")
            #expect(cancellation.requestID == request.requestID, "\(code)")
            #expect(cancellation.clientID == request.clientID && cancellation.session == request.session, "\(code)")
            #expect(cancellation.operation == .nonReplicated, "\(code)")
            #expect(cancellation.payload.isEmpty && cancellation.context == .empty, "\(code)")

            // The connection is still bound and in step.
            try await send()
            let next = try #require(await requests(server, code).last)
            #expect(next.connection == request.connection, "\(code)")
            #expect(next.requestID > request.requestID, "\(code)")
            #expect(await requests(server, .bindSession).count == 1, "\(code)")
            await client.shutdown()
            await server.stop()
        }
    }

    /// The server stores the messages of a fetch with auto-commit as
    /// consumed, so its cancelled caller waits for the reply, and the server
    /// gets no cancellation.
    @Test func aFetchThatCommitsIsNotCancelledWithItsCaller() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await server.respond { request in
            guard request.code == CommandCode.fetchMessages.rawValue else {
                return FakeVSRServer.defaultReply(request, address: server.address)
            }
            try? await Task.sleep(for: .milliseconds(300))
            return .ok(FakeVSRServer.fetchedBody([.init(partitionID: 1, offsets: [3])]))
        }
        let client = try await signedInClient(server)
        let fetch = Task {
            try await client.fetchMessages(streamID: 1, topicID: 2, targets: [(1, .next)], count: 10, autoCommit: true, options: Self.parkingOptions)
        }
        #expect(await eventually { await requests(server, .fetchMessages).count == 1 })
        fetch.cancel()
        #expect(try await fetch.value.map(\.partitionID) == [1])
        #expect(await requests(server, .cancelDeferredRequest).isEmpty)
        await client.shutdown()
    }

    /// The server can answer a poll before the cancellation reaches it. That
    /// reply does not answer the next request on the connection.
    @Test func aReplyToACancelledPollDoesNotAnswerTheNextOne() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let polls = Locked<UInt64>(0)
        await server.respond { request in
            guard request.code == CommandCode.pollMessagesDeferred.rawValue else {
                return FakeVSRServer.defaultReply(request, address: server.address)
            }
            let poll = polls.withLock { polls in
                polls += 1
                return polls
            }
            // The first reply arrives while the second poll waits for its own.
            try? await Task.sleep(for: poll == 1 ? .milliseconds(300) : .milliseconds(600))
            return .ok(FakeVSRServer.polledBody(partitionID: 1, offsets: [poll]))
        }
        let client = try await signedInClient(server)
        let poll = { @Sendable in
            try await client.pollMessages(streamID: 1, topicID: 2, partitionID: 1, strategy: .next, count: 1, autoCommit: false, options: Self.parkingOptions)
        }
        let cancelled = Task { try await poll() }
        #expect(await eventually { polls.withLock { $0 } == 1 })
        cancelled.cancel()
        await #expect(throws: CancellationError.self) { try await cancelled.value }
        #expect(try await poll().messages.map(\.offset) == [2])
        let sent = await requests(server, .pollMessagesDeferred)
        #expect(Set(sent.map(\.connection)).count == 1)
        await client.shutdown()
    }

    /// A poll or fetch may have committed what it read before losing its reply.
    @Test func aPollOrFetchThatLostItsReplyDoesNotGoAgain() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let dropped = Locked<Set<UInt32>>([])
        await server.respond { request in
            if let code = request.code, CommandCode.isDeferred(code), dropped.withLock({ $0.insert(code).inserted }) {
                return .close
            }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = try await signedInClient(server)
        let options = DeferredPollOptions(maxWait: .milliseconds(200), requestTimeout: .seconds(5))
        await expectCode(.transientNotCommitted) {
            _ = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: 1, strategy: .next, count: 1, autoCommit: true, options: options)
        }
        #expect(await requests(server, .pollMessagesDeferred).count == 1)
        await expectCode(.fetchProgressUncertain) {
            _ = try await client.fetchMessages(streamID: 1, topicID: 2, targets: [(1, .next)], count: 1, autoCommit: true, options: options)
        }
        #expect(await requests(server, .fetchMessages).count == 1)
        await client.shutdown()
    }

    /// A request waits for a lease within its own deadline, as one that
    /// never left the client. A fetch takes one lease for all its targets.
    @Test func atMostSixtyFourDeferredRequestsAreInFlight() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let stalled = Locked(0)
        await server.respond { request in
            if request.code == CommandCode.pollMessagesDeferred.rawValue || request.code == CommandCode.fetchMessages.rawValue {
                let count = stalled.withLock { stalled in
                    stalled += 1
                    return stalled
                }
                if count <= PollRouter.maxDeferredRequests {
                    return .stall
                }
            }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = try await signedInClient(server)
        let poll = { @Sendable (timeout: Duration) in
            try await client.pollMessages(
                streamID: 1, topicID: 2, partitionID: 1, strategy: .offset(0), count: 1, autoCommit: false,
                options: DeferredPollOptions(maxWait: .zero, requestTimeout: timeout))
        }
        let fetch = { @Sendable in
            _ = try await client.fetchMessages(
                streamID: 1, topicID: 2, targets: [(1, .offset(0)), (2, .offset(0))], count: 1, autoCommit: false,
                options: DeferredPollOptions(maxWait: .seconds(20), requestTimeout: .seconds(30)))
        }
        let parked = (0..<PollRouter.maxDeferredRequests).map { index in
            Task {
                if index.isMultiple(of: 2) {
                    try await fetch()
                } else {
                    _ = try await poll(.seconds(30))
                }
            }
        }
        #expect(await eventually(timeout: .seconds(10)) { stalled.withLock { $0 } == PollRouter.maxDeferredRequests })
        await expectCode(.transientNotAccepted) { _ = try await poll(.milliseconds(300)) }
        #expect(stalled.withLock { $0 } == PollRouter.maxDeferredRequests)

        parked[0].cancel()
        #expect(try await poll(.seconds(5)).messages.isEmpty)
        for request in parked {
            request.cancel()
        }
        for request in parked {
            _ = await request.result
        }
        #expect(await eventually { await requests(server, .cancelDeferredRequest).count == PollRouter.maxDeferredRequests })
        // The freed connection served the later poll.
        #expect(await server.connectionCount == PollRouter.maxDeferredRequests + 1)
        await client.shutdown()
    }

    /// The leases bound the deferred requests in flight, and an idle
    /// connection to one primary makes way for a request to another, so the
    /// connections they leave open stay within the same bound.
    @Test func anIdleDeferredConnectionMakesWayForAnotherPrimary() async throws {
        let cluster = try await Cluster.started(primaries: 2)
        defer { Task { await cluster.stop() } }
        let (busy, other) = (cluster.primaries[0], cluster.primaries[1])
        await cluster.respond(on: busy) { request in
            request.code == CommandCode.fetchMessages.rawValue ? .stall : FakeVSRServer.defaultReply(request, address: busy.address)
        }
        let client = try await signedInClient(cluster.coordinator)
        let fetch = { @Sendable (partition: UInt32) in
            try await client.fetchMessages(
                streamID: 1, topicID: 2, targets: [(partition, .offset(0))], count: 1, autoCommit: false, options: Self.parkingOptions)
        }
        let parked = (0..<PollRouter.maxDeferredRequests).map { _ in Task { try await fetch(1) } }
        #expect(await eventually(timeout: .seconds(10)) { await requests(busy, .fetchMessages).count == PollRouter.maxDeferredRequests })
        for request in parked {
            request.cancel()
        }
        for request in parked {
            _ = await request.result
        }
        #expect(await eventually { await requests(busy, .cancelDeferredRequest).count == PollRouter.maxDeferredRequests })
        #expect(await busy.openConnectionCount == PollRouter.maxDeferredRequests)

        #expect(try await fetch(2).isEmpty)
        #expect(await eventually { await busy.openConnectionCount == PollRouter.maxDeferredRequests - 1 })
        #expect(await other.openConnectionCount == 1)
        await client.shutdown()
    }

    // MARK: Fetches

    @Test(arguments: [false, true])
    func anUnknownLiveFetchBlocksEveryTargetUntilExplicitAbandon(cancelled: Bool) async throws {
        let server = try await FakeVSRServer.started()
        let finishOldRead = AsyncSemaphore(permits: 0)
        let oldReadCompleted = Locked(false)
        defer {
            finishOldRead.release(1)
            Task { await server.stop() }
        }
        await server.respond { request in
            if request.code == CommandCode.fetchMessages.rawValue, (try? request.fetch.autoCommit) == true {
                _ = try? await finishOldRead.acquire(1)
                _ = oldReadCompleted.exchange(true)
            }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = try await signedInClient(server)
        let fetch = Task {
            try await client.fetchMessages(
                streamID: 1, topicID: 2, targets: [(2, .next), (1, .next)], count: 1, autoCommit: true,
                options: DeferredPollOptions(maxWait: .zero, requestTimeout: .seconds(2)))
        }
        #expect(await eventually { await requests(server, .fetchMessages).count == 1 })
        let routesBeforeStore = await requests(server, .getPartitionRoute).count
        let queuedStore = Task { try await client.storeConsumerOffset(streamID: "stream", topicID: "topic", partitionID: 1, offset: 7) }
        defer { queuedStore.cancel() }
        #expect(await eventually { await requests(server, .getPartitionRoute).count > routesBeforeStore })
        if cancelled { fetch.cancel() }
        let failure = try await reportedError { _ = try await fetch.value }
        #expect(failure.code == .fetchProgressUncertain)
        let operationID = try #require(failure.operationID)
        let queuedFailure = try await reportedError { try await queuedStore.value }
        #expect(queuedFailure.code == .partitionLaneBlocked && queuedFailure.operationID == operationID && queuedFailure.partitionID == 1)
        #expect(!oldReadCompleted.withLock { $0 })
        #expect(await requests(server, .cancelDeferredRequest).isEmpty)

        for partitionID: UInt32 in [1, 2] {
            let blocked = try await reportedError {
                try await client.storeConsumerOffset(streamID: "stream", topicID: "topic", partitionID: partitionID, offset: 7)
            }
            #expect(blocked.code == .partitionLaneBlocked)
            #expect(blocked.partitionID == partitionID && blocked.operationID == operationID)
        }
        _ = try await client.getConsumerOffset(streamID: 1, topicID: 2, partitionID: 1)
        _ = try await client.fetchMessages(
            streamID: 1, topicID: 2, targets: [(1, .offset(0))], count: 1, autoCommit: false, options: Self.parkingOptions)
        let sentFetches = await requests(server, .fetchMessages)
        #expect(sentFetches.count == 2 && sentFetches[0].connection != sentFetches[1].connection)
        let afterReconnect = try await reportedError { try await client.deleteConsumerOffset(streamID: 1, topicID: 2, partitionID: 2) }
        #expect(afterReconnect.operationID == operationID)
        try await client.sendBatch(streamID: 1, topicID: 2, partitioning: .partition(1), messages: ["independent"])
        try await client.storeConsumerOffset(streamID: 1, topicID: 2, partitionID: 3, offset: 5)
        #expect(await requests(server, .storeConsumerOffset).map(polledPartition) == [3])
        #expect(!oldReadCompleted.withLock { $0 })

        // Replacing the session must not treat the old data connection as fenced.
        _ = try await client.login(username: "iggy", password: "iggy")
        let afterLogin = try await reportedError { try await client.deleteConsumerOffset(streamID: 1, topicID: 2, partitionID: 1) }
        #expect(afterLogin.code == .partitionLaneBlocked && afterLogin.operationID == operationID)
        let beforeAbandon = await server.requests.count
        try client.abandonOperation(operationID)
        #expect(await server.requests.count == beforeAbandon)
        try await client.storeConsumerOffset(streamID: 1, topicID: 2, partitionID: 1, offset: 9)
        #expect(await requests(server, .storeConsumerOffset).map(polledPartition) == [3, 1])
        #expect(await requests(server, .fetchMessages).count == 2)
        #expect(!oldReadCompleted.withLock { $0 })
        await expectCode(.operationNotRetained) { try client.abandonOperation(operationID) }
        finishOldRead.release(1)
        #expect(await eventually { oldReadCompleted.withLock { $0 } })
        await client.shutdown()
    }

    @Test func aPartialUnknownFetchKeepsHealthySectionsAndItsRawBytes() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let body = FakeVSRServer.fetchedBody([
            .init(partitionID: 1, offsets: [3, 4]), .init(partitionID: 2, status: IggyErrorCode.transientNotCommitted.rawValue),
            .init(partitionID: 3, status: IggyErrorCode.requestAlreadyApplied.rawValue),
        ])
        await server.respond { request in
            request.code == CommandCode.fetchMessages.rawValue ? .ok(body) : FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = try await signedInClient(server)
        let targets: [(partitionID: UInt32, strategy: PollingStrategy)] = [(1, .next), (2, .next), (3, .next), (4, .next)]
        let sections = try await client.fetchMessages(
            streamID: 1, topicID: 2, targets: targets, count: 10, autoCommit: true, options: Self.parkingOptions)
        #expect(try sections[0].decode().messages.map(\.offset) == [3, 4])
        let operationID = try #require(sections[1].error?.operationID)
        #expect(sections[1].error?.code == .fetchProgressUncertain && sections[1].error?.partitionID == 2)
        #expect(sections[2].error?.operationID == operationID && sections[2].error?.partitionID == 3)
        try await client.storeConsumerOffset(streamID: 1, topicID: 2, partitionID: 1, offset: 4)
        try await client.storeConsumerOffset(streamID: 1, topicID: 2, partitionID: 4, offset: 0)
        let blocked = try await reportedError { try await client.deleteConsumerOffset(streamID: 1, topicID: 2, partitionID: 3) }
        #expect(blocked.operationID == operationID)
        try client.abandonOperation(operationID)

        let raw = try Requests.fetchMessages(
            consumer: .default, streamID: 1, topicID: 2,
            targets: targets.map { .init(partitionID: $0.partitionID, strategy: $0.strategy, maxBytes: Self.parkingOptions.maxBytes, fence: .empty) },
            count: 10, autoCommit: true, options: Self.parkingOptions)
        #expect(try await client.sendRawRequest(code: CommandCode.fetchMessages.rawValue, payload: raw) == body)
        let rawBlocked = try await reportedError { try await client.storeConsumerOffset(streamID: 1, topicID: 2, partitionID: 2, offset: 0) }
        let rawID = try #require(rawBlocked.operationID)
        #expect(rawID != operationID && rawBlocked.partitionID == 2)
        try client.abandonOperation(rawID)
        await client.shutdown()
    }

    @Test func retainedFetchCapacityIsReservedBeforeSendAndFreedOnlyByRecovery() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await server.respond { request in
            request.code == CommandCode.fetchMessages.rawValue
                ? .status(IggyErrorCode.transientNotCommitted.rawValue) : FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = try await signedInClient(server)
        var operations: [UInt128Value] = []
        for partition in 0..<PollRouter.maxDeferredRequests {
            let failure = try await reportedError {
                _ = try await client.fetchMessages(
                    streamID: 1, topicID: 2, targets: [(UInt32(partition), .next)], count: 1, autoCommit: true, options: Self.parkingOptions)
            }
            #expect(failure.code == .fetchProgressUncertain)
            operations.append(try #require(failure.operationID))
        }
        #expect(Set(operations).count == PollRouter.maxDeferredRequests)
        let recreated = PartitionContext(incarnation: FakeVSRServer.routeContext.incarnation + 1, ownerGeneration: 0, metadataOp: 0)
        try await client.storeConsumerPosition(streamID: 1, topicID: 2, position: .init(partitionID: 0, offset: 0, context: recreated))
        let oldHistory = try await reportedError { try await client.storeConsumerOffset(streamID: 1, topicID: 2, partitionID: 0, offset: 0) }
        #expect(oldHistory.operationID == operations.first)
        let extraPartition = UInt32(PollRouter.maxDeferredRequests)
        await expectCode(.transientNotAccepted) {
            _ = try await client.fetchMessages(
                streamID: 1, topicID: 2, targets: [(extraPartition, .next)], count: 1, autoCommit: true, options: Self.parkingOptions)
        }
        #expect(await requests(server, .fetchMessages).count == PollRouter.maxDeferredRequests)
        await expectCode(.transientNotCommitted) {
            _ = try await client.fetchMessages(
                streamID: 1, topicID: 2, targets: [(extraPartition, .next)], count: 1, autoCommit: false, options: Self.parkingOptions)
        }
        try await client.storeConsumerOffset(streamID: 1, topicID: 2, partitionID: extraPartition, offset: 0)
        let old = operations.removeFirst()
        try client.abandonOperation(old)
        let next = try await reportedError {
            _ = try await client.fetchMessages(
                streamID: 1, topicID: 2, targets: [(extraPartition, .next)], count: 1, autoCommit: true, options: Self.parkingOptions)
        }
        let nextID = try #require(next.operationID)
        #expect(next.code == .fetchProgressUncertain && nextID != old && !operations.contains(nextID))
        operations.append(nextID)
        for operation in operations { try client.abandonOperation(operation) }
        try await client.storeConsumerOffset(streamID: 1, topicID: 2, partitionID: extraPartition, offset: 1)
        await client.shutdown()
    }

    @Test(arguments: [IggyErrorCode.transientNotCommitted, .requestAlreadyApplied, .invalidCommand, .unauthenticated, .unauthorized, .invalidSizeBytes])
    func aPossiblyAppliedWholeRefusalIsRetainedWithoutRetry(_ code: IggyErrorCode) async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await server.respond { request in
            request.code == CommandCode.fetchMessages.rawValue ? .status(code.rawValue) : FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = try await signedInClient(server)
        let failure = try await reportedError {
            _ = try await client.fetchMessages(
                streamID: 1, topicID: 2, targets: [(1, .next), (2, .next)], count: 1, autoCommit: true, options: Self.parkingOptions)
        }
        #expect(failure.code == .fetchProgressUncertain)
        #expect(await requests(server, .fetchMessages).count == 1)
        let operationID = try #require(failure.operationID)
        let blocked = try await reportedError { try await client.deleteConsumerOffset(streamID: 1, topicID: 2, partitionID: 2) }
        #expect(blocked.operationID == operationID)
        try client.abandonOperation(operationID)
        await client.shutdown()
    }

    @Test func malformedFetchTablesBlockProgressButBadRecordsAndDefiniteRefusalsDoNot() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let responses: [FakeVSRServer.Reply] = [
            .ok([1, 0, 0, 0]),
            .ok(FakeVSRServer.fetchedBody([.init(partitionID: 1), .init(partitionID: 1)])),
            .ok(FakeVSRServer.fetchedBody([.init(partitionID: 3)])),
            .ok(FakeVSRServer.fetchedBody([.init(partitionID: 1, offsets: [3, 4], records: [0])])),
            .status(IggyErrorCode.notPrimary.rawValue),
            .ok(FakeVSRServer.fetchedBody([.init(partitionID: 2, status: IggyErrorCode.invalidSizeBytes.rawValue)])),
            .ok(FakeVSRServer.fetchedBody([.init(partitionID: 2, status: IggyErrorCode.transientNotAccepted.rawValue)])),
        ]
        let index = Locked(0)
        await server.respond { request in
            guard request.code == CommandCode.fetchMessages.rawValue else {
                return FakeVSRServer.defaultReply(request, address: server.address)
            }
            return index.withLock { index in
                defer { index += 1 }
                return responses[min(index, responses.count - 1)]
            }
        }
        let client = try await signedInClient(server)
        let fetch = {
            try await client.fetchMessages(
                streamID: 1, topicID: 2, targets: [(1, .next), (2, .next)], count: 2, autoCommit: true, options: Self.parkingOptions)
        }
        for _ in 0..<3 {
            let failure = try await reportedError { _ = try await fetch() }
            #expect(failure.code == .fetchProgressUncertain)
            let operationID = try #require(failure.operationID)
            let blocked = try await reportedError { try await client.storeConsumerOffset(streamID: 1, topicID: 2, partitionID: 2, offset: 0) }
            #expect(blocked.operationID == operationID)
            try client.abandonOperation(operationID)
        }
        let records = try await fetch()
        #expect(throws: IggyError.self) { try records[0].decode() }
        try await client.storeConsumerOffset(streamID: 1, topicID: 2, partitionID: 1, offset: 2)
        await expectCode(.notPrimary) { _ = try await fetch() }
        try await client.storeConsumerOffset(streamID: 1, topicID: 2, partitionID: 2, offset: 0)
        let oversized = try await fetch()
        #expect(oversized[0].error?.code == .invalidSizeBytes && oversized[0].error?.operationID == nil)
        try await client.storeConsumerOffset(streamID: 1, topicID: 2, partitionID: 2, offset: 0)
        let partial = try await fetch()
        #expect(partial[0].status == IggyErrorCode.transientNotAccepted.rawValue)
        #expect(partial[0].error?.code == .fetchProgressUncertain)
        try client.abandonOperation(try #require(partial[0].error?.operationID))
        await client.shutdown()
    }

    @Test(arguments: [false, true], [false, true])
    func progressFetchesOrderEveryTargetAcrossAliasesAndRequestForms(raw: Bool, autoCommit: Bool) async throws {
        let server = try await FakeVSRServer.started()
        let reply = AsyncSemaphore(permits: 0)
        defer {
            reply.release(1)
            Task { await server.stop() }
        }
        let first = Locked(true)
        await server.respond { request in
            if request.code == CommandCode.fetchMessages.rawValue, first.exchange(false) {
                _ = try? await reply.acquire(1)
            }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = try await signedInClient(server)
        let parked = Task {
            let strategy: PollingStrategy = autoCommit ? .offset(0) : .next
            if raw {
                let payload = try Requests.fetchMessages(
                    consumer: .default, streamID: 1, topicID: 2,
                    targets: [2, 1].map { .init(partitionID: $0, strategy: strategy, maxBytes: Self.parkingOptions.maxBytes, fence: .empty) },
                    count: 10, autoCommit: autoCommit, options: Self.parkingOptions)
                _ = try await client.sendRawRequest(code: CommandCode.fetchMessages.rawValue, payload: payload)
            } else {
                _ = try await client.fetchMessages(
                    streamID: 1, topicID: 2, targets: [(2, strategy), (1, strategy)], count: 10, autoCommit: autoCommit, options: Self.parkingOptions)
            }
        }
        defer { parked.cancel() }
        #expect(await eventually { await requests(server, .fetchMessages).count == 1 })
        let sent = try #require(await requests(server, .fetchMessages).first)
        #expect(try sent.fetch.targets.allSatisfy { $0.incarnation == FakeVSRServer.routeContext.incarnation })

        let started = AsyncSemaphore(permits: 0)
        let operations: [@Sendable () async throws -> Void] = [
            {
                try await client.storeConsumerOffset(streamID: "stream", topicID: "topic", partitionID: 1, offset: 7)
            },
            {
                let payload = try Requests.deleteConsumerOffset(consumer: .default, streamID: 1, topicID: 2, partitionID: 1, ack: .quorum)
                _ = try await client.sendRawRequest(code: CommandCode.deleteConsumerOffset.rawValue, payload: payload)
            },
            {
                let payload = try Requests.pollMessages(
                    consumer: .default, streamID: 1, topicID: 2, partitionID: 1, strategy: .next, count: 1, autoCommit: false)
                _ = try await client.sendRawRequest(code: CommandCode.pollMessages.rawValue, payload: payload)
            },
            {
                _ = try await client.pollMessages(
                    streamID: "stream", topicID: "topic", partitionID: 1, strategy: .offset(0), count: 1, autoCommit: true, options: Self.parkingOptions)
            },
            {
                _ = try await client.fetchMessages(
                    streamID: 1, topicID: 2, targets: [(1, .next), (2, .next)], count: 1, autoCommit: false, options: Self.parkingOptions)
            },
        ]
        let waiting = operations.map { operation in
            Task {
                started.release(1)
                try await operation()
            }
        }
        defer { waiting.forEach { $0.cancel() } }
        #expect(try await started.acquire(waiting.count, deadline: .now + .seconds(2)))
        _ = try await client.getConsumerOffset(streamID: 1, topicID: 2, partitionID: 1)
        _ = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: 1, strategy: .offset(0), count: 1, autoCommit: false)
        _ = try await client.fetchMessages(streamID: 1, topicID: 2, targets: [(1, .offset(0))], count: 1, autoCommit: false, options: Self.parkingOptions)
        try await client.storeConsumerOffset(streamID: 1, topicID: 2, partitionID: 3, offset: 4)
        try await client.sendBatch(streamID: 1, topicID: 2, partitioning: .partition(1), messages: ["independent"])
        try await Task.sleep(for: .milliseconds(100))
        #expect(await requests(server, .fetchMessages).count == 2)
        #expect(await requests(server, .storeConsumerOffset).map(polledPartition) == [3])
        #expect(await requests(server, .deleteConsumerOffset).isEmpty)
        #expect(await requests(server, .pollMessages).count == 1)
        #expect(await requests(server, .pollMessagesDeferred).isEmpty)

        reply.release(1)
        try await parked.value
        for operation in waiting {
            try await operation.value
        }
        #expect(await requests(server, .fetchMessages).count == 3)
        #expect(await requests(server, .storeConsumerOffset).count == 2)
        #expect(await requests(server, .deleteConsumerOffset).count == 1)
        #expect(await requests(server, .pollMessages).count == 2)
        #expect(await requests(server, .pollMessagesDeferred).count == 1)
        await client.shutdown()
    }

    @Test(arguments: [false, true])
    func aPartlyAcquiredProgressRequestReleasesItsEarlierTargets(cancelled: Bool) async throws {
        let router = PollRouter(fence: 1)
        let first = PollRouter.ProgressKey(partitionID: 1, incarnation: 1)
        let second = PollRouter.ProgressKey(partitionID: 2, incarnation: 1)
        let entered = AsyncSemaphore(permits: 0)
        let release = AsyncSemaphore(permits: 0)
        let holder = Task {
            try await router.withProgress([second], deadline: .now + .seconds(5)) {
                entered.release(1)
                _ = try await release.acquire(1)
            }
        }
        defer {
            release.release(1)
            holder.cancel()
        }
        #expect(try await entered.acquire(1, deadline: .now + .seconds(1)))
        let called = Locked(false)
        let waiter = Task {
            try await router.withProgress([second, first, first], deadline: .now + .seconds(1)) {
                called.exchange(true)
            }
        }
        defer { waiter.cancel() }
        #expect(
            await eventually(timeout: .milliseconds(500)) {
                do {
                    try await router.withProgress([first], deadline: .now + .milliseconds(50)) {}
                    return false
                } catch let error as IggyError {
                    return error.code == .transientNotAccepted
                } catch {
                    return false
                }
            })
        if cancelled {
            waiter.cancel()
            await #expect(throws: CancellationError.self) { try await waiter.value }
        } else {
            await expectCode(.transientNotAccepted) { _ = try await waiter.value }
        }
        #expect(!called.withLock { $0 })
        try await router.withProgress([first], deadline: .now + .seconds(1)) {}
        // A recreated partition is independent of the still-held old history.
        try await router.withProgress([.init(partitionID: 2, incarnation: 2)], deadline: .now + .seconds(1)) {}
        release.release(1)
        try await holder.value
        try await router.withProgress([second, first], deadline: .now + .seconds(1)) {}
        await expectCode(.transientNotAccepted) {
            try await router.withProgress([.init(partitionID: 1, incarnation: 0)], deadline: .now + .seconds(1)) {}
        }
        await expectCode(.transientNotAccepted) {
            try await router.withProgress(
                (0...PollRouter.maxProgressLanes).map { .init(partitionID: UInt32($0), incarnation: 1) }, deadline: .now + .seconds(1)
            ) {}
        }
        try await router.withProgress([first], deadline: .now + .seconds(1)) {}
    }

    @Test(arguments: [false, true])
    func aCommittingFetchCanBeCancelledOrExpireWhileWaitingForProgress(cancelled: Bool) async throws {
        let server = try await FakeVSRServer.started()
        let reply = AsyncSemaphore(permits: 0)
        defer {
            reply.release(1)
            Task { await server.stop() }
        }
        let first = Locked(true)
        await server.respond { request in
            if request.code == CommandCode.fetchMessages.rawValue, first.exchange(false) {
                _ = try? await reply.acquire(1)
            }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = try await signedInClient(server)
        let parked = Task {
            try await client.fetchMessages(
                streamID: 1, topicID: 2, targets: [(2, .next)], count: 1, autoCommit: true, options: Self.parkingOptions)
        }
        defer { parked.cancel() }
        #expect(await eventually { await requests(server, .fetchMessages).count == 1 })
        let waiter = Task {
            try await client.fetchMessages(
                streamID: 1, topicID: 2, targets: [(1, .next), (2, .next)], count: 1, autoCommit: true,
                options: DeferredPollOptions(maxWait: .zero, requestTimeout: .seconds(2)))
        }
        defer { waiter.cancel() }
        if cancelled {
            try await Task.sleep(for: .milliseconds(100))
            waiter.cancel()
            await #expect(throws: CancellationError.self) { try await waiter.value }
        } else {
            await expectCode(.transientNotAccepted) { _ = try await waiter.value }
        }
        #expect(await requests(server, .fetchMessages).count == 1)
        #expect(await requests(server, .cancelDeferredRequest).isEmpty)
        // The waiting fetch released partition1 before partition2 replied.
        try await client.storeConsumerOffset(streamID: 1, topicID: 2, partitionID: 1, offset: 9)
        reply.release(1)
        _ = try await parked.value
        try await client.deleteConsumerOffset(streamID: 1, topicID: 2, partitionID: 2)
        await client.shutdown()
    }

    /// A fetch goes over a data connection with what is left of its options.
    /// Each target carries its own fence, from the context of its start point
    /// or the incarnation of its route, so the header carries none.
    @Test func aFetchFencesEachTargetAndCarriesNoHeaderContext() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await server.respond { request in
            guard request.code == CommandCode.fetchMessages.rawValue else {
                return FakeVSRServer.defaultReply(request, address: server.address)
            }
            return .ok(
                FakeVSRServer.fetchedBody([.init(partitionID: 2, offsets: [5, 6]), .init(partitionID: 1, status: IggyErrorCode.notPrimary.rawValue)]))
        }
        let client = try await signedInClient(server)
        let carried = PartitionContext(incarnation: 4, ownerGeneration: 2, metadataOp: 1)
        let unfenced = PartitionContext(incarnation: 0, ownerGeneration: 6, metadataOp: 0)
        let options = DeferredPollOptions(maxWait: .milliseconds(500), minCount: 2, maxBytes: 4096, requestTimeout: .seconds(3))
        let sections = try await client.fetchMessages(
            streamID: 1, topicID: 2, targets: [(2, .offset(5, context: carried)), (1, .next), (3, .offset(9, context: unfenced))], consumer: .group(3),
            count: 10, autoCommit: true, options: options)
        #expect(sections.map(\.partitionID) == [2, 1])
        #expect(try sections[0].decode().messages.map(\.offset) == [5, 6])
        #expect(sections[0].nextOffset == 7)
        #expect(sections[1].error == IggyError(.notPrimary))

        let request = try #require(await requests(server, .fetchMessages).first)
        #expect(request.context == .empty)
        #expect(request.operation == .nonReplicated)
        let fetch = try request.fetch
        let route = FakeVSRServer.routeContext
        #expect(
            fetch.targets == [
                .init(partitionID: 2, kind: .offset, value: 5, maxBytes: 4096, incarnation: 4, ownerGeneration: 2),
                .init(partitionID: 1, kind: .next, value: 0, maxBytes: 4096, incarnation: route.incarnation, ownerGeneration: 0),
                .init(partitionID: 3, kind: .offset, value: 9, maxBytes: 4096, incarnation: route.incarnation, ownerGeneration: 6),
            ])
        #expect(fetch.count == 10 && fetch.minCount == 2 && fetch.maxBytes == 4096 && fetch.autoCommit)
        // The time the call spent before the attempt comes off both.
        #expect(fetch.wait <= 500_000)
        #expect(fetch.timeout <= 3_000_000)
        #expect(fetch.timeout >= fetch.wait + 1_000_000)
        // A parked fetch would hold the lockstep coordinator connection.
        let login = try #require(await server.requests.first)
        #expect(request.connection != login.connection)
        #expect(await requests(server, .bindSession).map(\.connection) == [request.connection])
        await client.shutdown()
    }

    /// A fetch keeps the reply margin without a wait too, which a deferred
    /// poll does not need.
    @Test func fetchesTheServerWouldRefuseFailBeforeAnythingIsSent() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = try await signedInClient(server)
        let before = await server.requests.count
        let next: [(partitionID: UInt32, strategy: PollingStrategy)] = [(1, .next)]
        let noMargin = DeferredPollOptions(maxWait: .zero, requestTimeout: .milliseconds(999))
        let refused: [(targets: [(partitionID: UInt32, strategy: PollingStrategy)], count: UInt32, options: DeferredPollOptions, code: IggyErrorCode)] =
            [
                ([], 10, Self.parkingOptions, .invalidCommand), ([(1, .next), (1, .offset(3))], 10, Self.parkingOptions, .invalidCommand),
                ((0..<1001).map { ($0, .next) }, 10, Self.parkingOptions, .invalidCommand), ([(1, .first)], 10, Self.parkingOptions, .invalidCommand),
                ([(1, .last)], 10, Self.parkingOptions, .invalidCommand),
                ([(1, .timestamp(IggyTimestamp(microseconds: 1)))], 10, Self.parkingOptions, .invalidCommand),
                (next, 0, Self.parkingOptions, .invalidMessagesCount), (next, 10, DeferredPollOptions(minCount: 11), .invalidMessagesCount),
                (next, 10, DeferredPollOptions(maxBytes: 343), .invalidSizeBytes), (next, 10, DeferredPollOptions(maxWait: .seconds(11)), .invalidCommand),
                (next, 10, noMargin, .invalidCommand),
                (next, 10, DeferredPollOptions(maxWait: .zero, requestTimeout: .seconds(1)), .invalidCommand),
                (next, 10, DeferredPollOptions(maxWait: .zero, requestTimeout: .seconds(1) + .nanoseconds(999)), .invalidCommand),
            ]
        for scenario in refused {
            await expectCode(scenario.code) {
                _ = try await client.fetchMessages(
                    streamID: 1, topicID: 2, targets: scenario.targets, count: scenario.count, autoCommit: false, options: scenario.options)
            }
        }
        let exactlyOneSecond = DeferredPollOptions(maxWait: .zero, requestTimeout: .seconds(1))
        let raw = try Requests.fetchMessages(
            consumer: .default, streamID: 1, topicID: 2,
            targets: [.init(partitionID: 1, strategy: .next, maxBytes: exactlyOneSecond.maxBytes, fence: .empty)],
            count: 10, autoCommit: false, options: exactlyOneSecond)
        await expectCode(.invalidCommand) { _ = try await client.sendRawRequest(code: CommandCode.fetchMessages.rawValue, payload: raw) }
        #expect(await server.requests.count == before)
        _ = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: 1, strategy: .next, count: 10, autoCommit: false, options: noMargin)
        await client.shutdown()
    }

    @Test func aFetchNeedsItsReplyMarginAfterRouting() async throws {
        for routingDelay in [Duration.milliseconds(100), .milliseconds(1100)] {
            let server = try await FakeVSRServer.started()
            let delayed = Locked(false)
            await server.respond { request in
                if request.code == CommandCode.getPartitionRoute.rawValue, !delayed.exchange(true) {
                    try? await Task.sleep(for: routingDelay)
                }
                return FakeVSRServer.defaultReply(request, address: server.address)
            }
            let client = try await signedInClient(server)
            let options = DeferredPollOptions(maxWait: .zero, requestTimeout: .seconds(2))
            let fetch = {
                try await client.fetchMessages(streamID: 1, topicID: 2, targets: [(1, .next)], count: 10, autoCommit: false, options: options)
            }
            if routingDelay < .seconds(1) {
                #expect(try await fetch().isEmpty)
                let request = try #require(await requests(server, .fetchMessages).first)
                let budget = try request.fetch
                #expect(budget.wait == 0)
                #expect(budget.timeout >= DeferredPollOptions.replyMarginMicroseconds && budget.timeout < 2_000_000)
            } else {
                await expectCode(.transientNotAccepted) { _ = try await fetch() }
                #expect(await requests(server, .fetchMessages).isEmpty)
            }
            #expect(delayed.withLock { $0 })
            await client.shutdown()
            await server.stop()
        }
    }

    /// A reply larger than a frame fails the connection that reads it, so the
    /// limit of a fetch and of each target leaves room for its section table.
    @Test func aFetchLeavesRoomInItsReplyFrameForTheSectionTable() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = try await signedInClient(server)
        _ = try await client.fetchMessages(
            streamID: 1, topicID: 2, targets: [(1, .next), (2, .offset(3))], count: 10, autoCommit: false,
            options: DeferredPollOptions(maxWait: .zero, maxBytes: .max, requestTimeout: .seconds(5)))
        let request = try #require(await requests(server, .fetchMessages).first)
        let fetch = try request.fetch
        let limit = DeferredPollOptions.maxReplyBytes - UInt32(Responses.fetchHeadSize(2))
        #expect(fetch.maxBytes == limit)
        #expect(fetch.targets.map(\.maxBytes) == [limit, limit])
        await client.shutdown()
    }

    /// A group holds the partitions of one primary in the order the caller
    /// names them, at most 1000 of them, the most one fetch names. The groups
    /// come in replica order.
    @Test func readinessGroupsSplitThePartitionsByPrimary() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = try await signedInClient(server)
        #expect(try await client.readinessGroups(streamID: 1, topicID: 2, partitions: [5, 3, 4]) == [[5, 3, 4]])
        #expect(try await client.readinessGroups(streamID: 1, topicID: 2, partitions: Array(0..<1001)) == [Array(0..<1000), [1000]])
        await client.shutdown()

        let cluster = try await Cluster.started(primaries: 2)
        defer { Task { await cluster.stop() } }
        let clustered = try await signedInClient(cluster.coordinator)
        #expect(try await clustered.readinessGroups(streamID: 1, topicID: 2, partitions: [2, 0, 1]) == [[0], [1], [2]])
        await clustered.shutdown()
    }

    /// A fetch refused as not primary is not sent again. The refusal names no
    /// partition, so the routes of all its targets are marked moved, and the
    /// next grouping looks each of them up again, first on the primary of its
    /// route. A section refused so marks the route of its partition alone.
    @Test func aFetchRefusedAsNotPrimaryMarksItsRoutesMoved() async throws {
        let cluster = try await Cluster.started(primaries: 2)
        defer { Task { await cluster.stop() } }
        let (first, second) = (cluster.primaries[0], cluster.primaries[1])
        let refused = Locked(false)
        await cluster.respond(on: first) { request in
            guard request.code == CommandCode.fetchMessages.rawValue else {
                return FakeVSRServer.defaultReply(request, address: first.address)
            }
            guard refused.exchange(true) else {
                return .status(IggyErrorCode.notPrimary.rawValue)
            }
            return .ok(
                FakeVSRServer.fetchedBody([.init(partitionID: 1, offsets: [4]), .init(partitionID: 2, status: IggyErrorCode.notPrimary.rawValue)]))
        }
        let client = try await signedInClient(cluster.coordinator)
        let group = { try await client.readinessGroups(streamID: 1, topicID: 2, partitions: [2, 1]) }
        let fetch = { (targets: [UInt32]) in
            try await client.fetchMessages(
                streamID: 1, topicID: 2, targets: targets.map { ($0, .next) }, count: 10, autoCommit: false, options: Self.parkingOptions)
        }
        #expect(try await group() == [[1], [2]])
        await expectCode(.notPrimary) { _ = try await fetch([1]) }
        #expect(await requests(first, .fetchMessages).count == 1)
        #expect(try await group() == [[1], [2]])
        #expect(await requests(first, .getPartitionRoute).count == 1)
        #expect(await requests(second, .getPartitionRoute).isEmpty)

        #expect(try await fetch([1, 2]).map(\.error) == [nil, IggyError(.notPrimary)])
        #expect(try await group() == [[1], [2]])
        #expect(await requests(first, .getPartitionRoute).count == 1)
        #expect(await requests(second, .getPartitionRoute).count == 1)
        await client.shutdown()
    }

    /// A section refused for a gone history or a lost owner drops the route of
    /// its partition, so the next fetch fences it with a fresh context. The
    /// other targets keep theirs.
    @Test func aFencedSectionDropsTheRouteOfItsPartition() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let lookups = Locked<[UInt32: UInt64]>([:])
        await server.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .getPartitionRoute:
                let partition = UInt32(littleEndianBytes: request.payload.suffix(4))
                let lookup = lookups.withLock { lookups in
                    lookups[partition, default: 0] += 1
                    return lookups[partition, default: 0]
                }
                let route = FakeVSRServer.routeContext
                let context = PartitionContext(incarnation: route.incarnation + lookup - 1, ownerGeneration: 0, metadataOp: route.metadataOp)
                return .ok(FakeVSRServer.partitionRouteBody(context))
            case .fetchMessages:
                return .ok(
                    FakeVSRServer.fetchedBody([
                        .init(partitionID: 1, status: IggyErrorCode.historyUnavailable.rawValue),
                        .init(partitionID: 2, status: IggyErrorCode.consumerGroupPartitionNotOwned.rawValue), .init(partitionID: 3, offsets: [0]),
                    ]))
            default:
                return FakeVSRServer.defaultReply(request, address: server.address)
            }
        }
        let client = try await signedInClient(server)
        for _ in 0..<2 {
            _ = try await client.fetchMessages(
                streamID: 1, topicID: 2, targets: [(1, .next), (2, .next), (3, .next)], count: 10, autoCommit: false, options: Self.parkingOptions)
        }
        let route = FakeVSRServer.routeContext.incarnation
        let fenced = try await requests(server, .fetchMessages).map { try $0.fetch.targets.map(\.incarnation) }
        #expect(fenced == [[route, route, route], [route + 1, route + 1, route]])
        await client.shutdown()
    }

    // MARK: Raw requests

    @Test func rawDeferredRequestsTakeTheRouteOfTheirTypedCalls() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = try await signedInClient(server)
        let options = DeferredPollOptions(maxWait: .milliseconds(200), requestTimeout: .seconds(5))
        let poll = try Requests.pollMessages(
            consumer: .consumer(5), streamID: 1, topicID: 2, partitionID: 1, strategy: .first, count: 10, autoCommit: false, options: options)
        let polled = try await client.sendRawRequest(code: CommandCode.pollMessagesDeferred.rawValue, payload: poll)
        #expect(try Responses.polledMessages(polled[...]).context == FakeVSRServer.routeContext)
        let sent = try #require(await requests(server, .pollMessagesDeferred).first)
        #expect(sent.context == FakeVSRServer.routeContext)
        #expect(sent.payload.dropLast(FakeVSRServer.Request.deferredOptionsSize) == poll.dropLast(FakeVSRServer.Request.deferredOptionsSize))
        #expect(sent.deferredOptions.timeout <= 5_000_000)
        #expect(await requests(server, .bindSession).map(\.connection) == [sent.connection])

        let fence = PartitionContext(incarnation: 3, ownerGeneration: 9, metadataOp: 0)
        let fetch = try Requests.fetchMessages(
            consumer: .consumer(5), streamID: 1, topicID: 2, targets: [Requests.FetchTarget(partitionID: 1, strategy: .next, maxBytes: .max, fence: fence)],
            count: 10, autoCommit: false, options: DeferredPollOptions(maxWait: .milliseconds(200), maxBytes: .max, requestTimeout: .seconds(5)))
        #expect(try await client.sendRawRequest(code: CommandCode.fetchMessages.rawValue, payload: fetch) == FakeVSRServer.fetchedBody([]))
        let fetched = try #require(await requests(server, .fetchMessages).first)
        #expect(fetched.context == .empty)
        let limit = DeferredPollOptions.maxReplyBytes - UInt32(Responses.fetchHeadSize(1))
        #expect(try fetched.fetch.maxBytes == limit)
        #expect(try fetched.fetch.targets == [.init(partitionID: 1, kind: .next, value: 0, maxBytes: limit, incarnation: 3, ownerGeneration: 9)])

        let before = await server.requests.count
        let tooFew = try Requests.pollMessages(
            consumer: .consumer(5), streamID: 1, topicID: 2, partitionID: 1, strategy: .first, count: 10, autoCommit: false,
            options: DeferredPollOptions(minCount: 11))
        // 52 bytes come before the first target with numeric ids, which is
        // `[partition_id:4][kind:1][value:8][max_bytes:4]` and its fence.
        let fetchedKind = 52 + 4
        let fetchedLimit = fetchedKind + 9
        var unknownStrategy = fetch
        unknownStrategy[fetchedKind] = 9
        var unknownConsumer = fetch
        unknownConsumer[0] = 3
        var firstTarget = fetch
        firstTarget[fetchedKind] = PollingStrategy.Kind.first.rawValue
        var smallTarget = fetch
        smallTarget.replaceSubrange(fetchedLimit..<fetchedLimit + 4, with: (DeferredPollOptions.minMaxBytes - 1).littleEndianBytes)
        let refused: [(CommandCode, [UInt8])] = [
            (.pollMessagesDeferred, poll + [0]), (.pollMessagesDeferred, Array(poll.dropLast())), (.pollMessagesDeferred, tooFew),
            (.fetchMessages, unknownStrategy), (.fetchMessages, unknownConsumer), (.cancelDeferredRequest, []), (.fetchMessages, fetch + [0]),
            (.fetchMessages, Array(fetch.dropLast())),
            (.fetchMessages, firstTarget), (.fetchMessages, smallTarget),
        ]
        for (code, payload) in refused {
            await expectCode(.invalidCommand) { _ = try await client.sendRawRequest(code: code.rawValue, payload: payload) }
        }
        // A group poll must name its partition.
        let groupPoll = try Requests.pollMessages(
            consumer: .group(3), streamID: 1, topicID: 2, partitionID: nil, strategy: .next, count: 1, autoCommit: true, options: options)
        await expectCode(.invalidIdentifier) { _ = try await client.sendRawRequest(code: CommandCode.pollMessagesDeferred.rawValue, payload: groupPoll) }
        #expect(await server.requests.count == before)
        await client.shutdown()
    }
}
