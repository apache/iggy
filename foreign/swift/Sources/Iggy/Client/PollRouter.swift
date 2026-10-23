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

import NIOConcurrencyHelpers

/// Routes polls and consumer-offset requests to the primary of their
/// partition, as `OperationDriver` in `core/sdk/src/driver.rs` routes its
/// partition calls.
///
/// A request first looks up the route of its partition (`GetPartitionRoute`):
/// the partition context the request carries and the replica id of the
/// partition primary, which the cluster roster maps to an endpoint. On a
/// single node the lookup and the request go over the coordinator, the
/// connection the client signed in on. In a cluster both go over data
/// connections, bound to the client's session with `BindSession` so
/// consumer-group membership stays with that session: the lookup to the
/// metadata primary, the request to the partition primary. A lookup takes
/// the coordinator instead when it stands at the metadata primary. Routes
/// are cached until a request on them fails or the session is replaced.
///
/// A backup can miss a stream, topic, or partition that the metadata primary
/// already committed, so only the metadata primary's miss is final. A miss
/// from a backup partition primary drops the route and looks it up again.
/// Sends go over the coordinator, but their lookups and misses follow the
/// same rules.
final class PollRouter: Sendable {
    /// Bounds one routed request across its lookups, attempts, and pauses.
    static let pollTimeout: Duration = .seconds(30)
    static let retryInterval: Duration = .milliseconds(50)
    static let maxRetryInterval: Duration = .seconds(1)
    static let rosterReadTimeout: Duration = .seconds(5)
    static let maxCachedRoutes = 4096
    static let maxDataConnections = 256

    /// A partition as its caller spelled it. A stream or topic named by
    /// number and the same one named by name are two keys.
    struct RouteKey: Hashable, Sendable {
        let stream: Identifier
        let topic: Identifier
        let partitionID: UInt32
    }

    private struct Route: Sendable {
        /// The fence of the session the route was read under.
        let fence: UInt128Value
        let primaryReplica: UInt8
        /// The client's session, with the route's metadata op as the
        /// watermark a data connection binds at.
        let consumerSession: Responses.SessionIdentity
        let context: PartitionContext
        let view: UInt32
        var minimumView: UInt32 = 0
        var askFirst: UInt8?
    }

    private struct DataConnection {
        let connection: any IggyConnection
        var consumerSession: Responses.SessionIdentity?
        var usable: Bool
    }

    /// One data connection per primary. Its lock keeps the lockstep stream to
    /// one request at a time.
    private final class Slot: Sendable {
        let lock = AsyncMutex()
        let connection = NIOLockedValueBox<DataConnection?>(nil)
    }

    private struct State {
        /// The client id of the current session. A reset replaces it, so a
        /// route or a connection of a replaced session is never published
        /// or used.
        var fence: UInt128Value
        /// The roster by replica id; empty until a roster read succeeded.
        var nodes: [UInt8: ClusterNode] = [:]
        /// The replica the roster names as the healthy leader. Nil until a
        /// roster names one, and after the replica failed a lookup, so the
        /// next lookup reads the roster again.
        var metadataPrimary: UInt8?
        var routes: [RouteKey: Route] = [:]
        var slots: [String: Slot] = [:]
        var nextHeartbeat: ContinuousClock.Instant?
    }

    private let state: NIOLockedValueBox<State>

    init(fence: UInt128Value) {
        state = NIOLockedValueBox(State(fence: fence))
    }

    /// Records the roster a leader check or a roster read reported, which
    /// tells whether requests go to data connections and where each replica
    /// listens. An empty roster says nothing about the cluster, so the router
    /// keeps asking. A replica id the roster repeats keeps its first node. A
    /// leader the roster does not report healthy is not taken for the
    /// metadata primary, as the leader check does not move to it either.
    func learnRoster(_ metadata: ClusterMetadata) {
        guard !metadata.nodes.isEmpty else {
            return
        }
        let nodes = Dictionary(metadata.nodes.map { ($0.replicaID, $0) }, uniquingKeysWith: { first, _ in first })
        let metadataPrimary = metadata.nodes.first { $0.role == .leader && $0.status == .healthy }?.replicaID
        state.withLockedValue { state in
            state.nodes = nodes
            state.metadataPrimary = metadataPrimary
        }
    }

    /// Drops every route and data connection of the session that ended and
    /// moves to the fence of the one that replaced it. A route or a
    /// connection is published only under the lock after a fence check, so
    /// nothing of the replaced session survives.
    func clearSession(fence: UInt128Value) {
        state.withLockedValue { state in
            state.fence = fence
            state.routes.removeAll()
            state.slots.removeAll()
            state.nextHeartbeat = nil
        }
    }

    /// Sends a poll or an offset request on its route. `context` is the
    /// partition context the caller captured, else the route's is taken and
    /// kept for every retry.
    ///
    /// `deadline` is checked between attempts and caps each exchange instead
    /// of cancelling one in flight, which would take the coordinator
    /// connection, and the session's group membership, with it. A request
    /// still refused at the deadline fails with the refusal,
    /// `transientNotAccepted`. `lineage` is the one the call started under:
    /// once the caller signed in or out since, the request fails with
    /// `unauthenticated`, as nothing of the call goes out under a session the
    /// caller opened after the call started.
    func send(
        _ code: CommandCode, payload: [UInt8], key: RouteKey, context: PartitionContext?, lineage: UInt64, coordinator: ClientCore,
        deadline: ContinuousClock.Instant
    ) async throws -> [UInt8] {
        let clock = ContinuousClock()
        let now = clock.now
        let clustered = try await isClustered(coordinator, lineage: lineage, deadline: deadline)
        if clustered, heartbeatDue(at: now, interval: coordinator.configuration.heartbeatInterval) {
            // The requests leave the coordinator connection idle, so it is
            // pinged here once per heartbeat interval too.
            _ = try await control(.ping, [], lineage: lineage, coordinator: coordinator, deadline: deadline)
        }
        let request = Request(code: code, payload: payload, key: key, lineage: lineage, deadline: deadline)
        var captured = context
        var backoff = Backoff()
        var replay = Replay()
        var refusal = IggyError(.transientNotAccepted, context: "the budget ran out before command \(code.rawValue) was sent")
        while clock.now < deadline {
            if let ended = await coordinator.lineageRefusal(lineage, code: code.rawValue) {
                throw replay.outcome.observe(ended)
            }
            do {
                if clustered {
                    return try await sendOnPrimary(request, context: &captured, replay: &replay, coordinator: coordinator)
                }
                return try await sendOnCoordinator(request, context: &captured, coordinator: coordinator)
            } catch let error as IggyError {
                let reconnects =
                    replay.outcome.isUncertain && error.isConnectionLoss && error.code != .staleClient
                    && coordinator.configuration.reconnection.enabled
                guard error.code == .transientNotAccepted || reconnects else {
                    throw replay.outcome.observe(error)
                }
                refusal = replay.outcome.observe(error)
                if !replay.outcome.isUncertain {
                    replay.frame = nil
                }
                try await backoff.pause(after: refusal, before: deadline)
            }
        }
        throw refusal
    }

    /// The context of the route of `key` for a send, looked up as a routed
    /// request looks up its route. Sends keep their contexts in
    /// `ConsumerGroupState`, so the route is not cached here.
    func sendContext(_ key: RouteKey, lineage: UInt64, coordinator: ClientCore, deadline: ContinuousClock.Instant) async throws -> PartitionContext {
        let clustered = try await isClustered(coordinator, lineage: lineage, deadline: deadline)
        let watermark = await coordinator.metadataWatermark
        let fence = state.withLockedValue { $0.fence }
        return try await lookUp(
            key, clustered: clustered, fence: fence, watermark: watermark, lineage: lineage, coordinator: coordinator, deadline: deadline
        )
        .context
    }

    /// Sends a batch under `context` over the coordinator, which carries
    /// every send. The coordinator can be a backup that lags the metadata
    /// primary, so in a cluster its namespace miss is final only when the
    /// coordinator stood at the metadata primary, as a roster read on the
    /// same connection confirms. Any other miss is refused, and the caller
    /// sends again on a route from the metadata primary.
    func sendMessages(
        _ payload: [UInt8], context: PartitionContext, lineage: UInt64, coordinator: ClientCore, deadline: ContinuousClock.Instant
    ) async throws -> [UInt8] {
        let clustered = try await isClustered(coordinator, lineage: lineage, deadline: deadline)
        let pinned = clustered ? await coordinatorAtMetadataPrimary(coordinator) : nil
        do {
            return try await coordinator.exchange(
                code: CommandCode.sendMessages.rawValue, payload: payload, context: context, deadline: deadline, lineage: lineage)
        } catch let error as IggyError where clustered && error.isNamespaceMiss {
            throw await coordinatorMiss(error, pinned: pinned, lineage: lineage, coordinator: coordinator, deadline: deadline)
        }
    }

    /// The pauses between the refused attempts of one request: from
    /// `retryInterval`, doubling up to `maxRetryInterval`.
    struct Backoff {
        private var interval = PollRouter.retryInterval

        /// Waits before the next attempt, or throws `refusal` when that
        /// attempt would start past `deadline`.
        mutating func pause(after refusal: IggyError, before deadline: ContinuousClock.Instant) async throws {
            guard ContinuousClock.now + interval < deadline else {
                throw refusal
            }
            try await Task.sleep(for: interval)
            interval = min(interval * 2, PollRouter.maxRetryInterval)
        }
    }

    /// The coordinator connection, by its epoch, while it stands at the
    /// metadata primary `replica`.
    private struct CoordinatorPin {
        let replica: UInt8
        let epoch: UInt64
    }

    private struct Request {
        let code: CommandCode
        let payload: [UInt8]
        let key: RouteKey
        let lineage: UInt64
        let deadline: ContinuousClock.Instant
    }

    private struct Replay {
        var frame: VSRFrame.EncodedRequest?
        var outcome = RetryOutcome()
    }

    /// A frame the client refused to write: the session it was for was
    /// replaced, the client saw a newer metadata commit since, the caller
    /// signed in or out after the request started, or the budget ran out
    /// before the frame was dialed or sent. The replica took no part in the
    /// refusal, so it says nothing about that replica.
    private struct UnsentRequest: Error {
        let refusal: IggyError
    }

    /// Whether the roster has more than one node, reading it once when no
    /// leader check reported it yet.
    private func isClustered(_ coordinator: ClientCore, lineage: UInt64, deadline: ContinuousClock.Instant) async throws -> Bool {
        if state.withLockedValue({ $0.nodes.isEmpty }) {
            try await readRoster(coordinator, lineage: lineage, deadline: deadline)
        }
        return state.withLockedValue { $0.nodes.count } > 1
    }

    private func readRoster(_ coordinator: ClientCore, lineage: UInt64, deadline: ContinuousClock.Instant) async throws {
        let readDeadline = min(deadline, ContinuousClock.now + Self.rosterReadTimeout)
        try learnRoster(body: try await control(.getClusterMetadata, [], lineage: lineage, coordinator: coordinator, deadline: readDeadline))
    }

    /// Records the roster of a roster reply. An empty one is refused.
    private func learnRoster(body: [UInt8]) throws {
        let metadata = try Responses.clusterMetadata(body[...])
        guard !metadata.nodes.isEmpty else {
            throw IggyError(.transientNotAccepted, context: "the cluster roster is empty")
        }
        learnRoster(metadata)
    }

    /// The TCP address of `replica`. A roster that does not list it is read
    /// once more before the route is refused.
    private func endpoint(of replica: UInt8, lineage: UInt64, coordinator: ClientCore, deadline: ContinuousClock.Instant) async throws -> String {
        var node = state.withLockedValue { $0.nodes[replica] }
        if node == nil {
            try await readRoster(coordinator, lineage: lineage, deadline: deadline)
            node = state.withLockedValue { $0.nodes[replica] }
        }
        guard let node else {
            throw IggyError(.transientNotAccepted, context: "the cluster roster does not list replica \(replica)")
        }
        guard node.endpoints.tcp != 0 else {
            coordinator.logger.error("The cluster node \(node.name) does not expose TCP")
            throw IggyError(.featureUnavailable, context: "the cluster node \(node.name) does not expose TCP")
        }
        return node.tcpAddress
    }

    private func heartbeatDue(at now: ContinuousClock.Instant, interval: Duration) -> Bool {
        state.withLockedValue { state in
            let due = state.nextHeartbeat.map { now >= $0 } ?? false
            if state.nextHeartbeat == nil || due {
                state.nextHeartbeat = now + interval
            }
            return due
        }
    }

    /// One exchange on the coordinator connection. A connection that is gone
    /// is brought back by a ping, which reconnects, and the exchange runs
    /// once more. A stale client refusal ends the exchange, as it ends any
    /// call. `lineage` is the one of the call the exchange serves, also for
    /// a roster read or a ping on its way.
    private func control(
        _ code: CommandCode, _ payload: [UInt8], context: PartitionContext = .empty, lineage: UInt64, coordinator: ClientCore,
        deadline: ContinuousClock.Instant
    ) async throws -> [UInt8] {
        do {
            return try await coordinator.exchangeOnce(code: code.rawValue, payload: payload, context: context, deadline: deadline, lineage: lineage)
        } catch let error as IggyError where error.isReconnectable && error.code != .staleClient && ContinuousClock.now < deadline {
            _ = try await coordinator.exchange(code: CommandCode.ping.rawValue, payload: [], deadline: deadline, lineage: lineage)
            return try await coordinator.exchangeOnce(code: code.rawValue, payload: payload, context: context, deadline: deadline, lineage: lineage)
        }
    }

    /// The single-node path: the request goes over the coordinator connection
    /// with the context of its route.
    private func sendOnCoordinator(_ request: Request, context: inout PartitionContext?, coordinator: ClientCore) async throws -> [UInt8] {
        let route = try await route(request.key, clustered: false, lineage: request.lineage, coordinator: coordinator, deadline: request.deadline)
        let captured = context ?? route.context
        context = captured
        do {
            if request.code == .pollMessages {
                return try await coordinator.exchangeOnce(
                    code: request.code.rawValue, payload: request.payload, context: captured, deadline: request.deadline, lineage: request.lineage)
            }
            return try await coordinator.exchange(
                code: request.code.rawValue, payload: request.payload, context: captured, deadline: request.deadline, lineage: request.lineage)
        } catch let error as IggyError {
            removeRoute(request.key)
            throw Self.gradedLoss(error, on: "the coordinator")
        }
    }

    /// The cluster path: the request goes over a data connection to the
    /// partition primary.
    private func sendOnPrimary(
        _ request: Request, context: inout PartitionContext?, replay: inout Replay, coordinator: ClientCore
    ) async throws -> [UInt8] {
        let route = try await route(request.key, clustered: true, lineage: request.lineage, coordinator: coordinator, deadline: request.deadline)
        let captured = context ?? route.context
        context = captured
        do {
            return try await exchange(
                onReplica: route.primaryReplica, code: request.code.rawValue, payload: request.payload, context: captured,
                identity: route.consumerSession, lineage: request.lineage, fence: route.fence, coordinator: coordinator, deadline: request.deadline,
                dialDeadline: request.deadline, replay: &replay)
        } catch let unsent as UnsentRequest {
            removeRoute(request.key)
            throw unsent.refusal
        } catch let error as IggyError {
            if error.code == .notPrimary {
                markNotPrimary(route, partitionID: request.key.partitionID)
                throw ClientCore.graded(error)
            }
            removeRoute(request.key)
            let source = MissSource.replica(identity: route.consumerSession, lineage: request.lineage, fence: route.fence)
            if error.isNamespaceMiss, await !isMetadataPrimary(route.primaryReplica, answeredOn: source, coordinator: coordinator, deadline: request.deadline) {
                throw IggyError(.transientNotAccepted, context: "replica \(route.primaryReplica) is not the metadata primary: \(error)")
            }
            throw error
        }
    }

    /// One request over the data connection to `replica`, bound to
    /// `identity` first. Every frame is stamped under a check that the
    /// session is still the client's and the caller did not sign in or out
    /// after `lineage`, so nothing is written for a session that was
    /// replaced. A new connection is dialed until `dialDeadline`.
    private func exchange(
        onReplica replica: UInt8, code: UInt32, payload: [UInt8], context: PartitionContext, identity: Responses.SessionIdentity, lineage: UInt64,
        fence: UInt128Value, coordinator: ClientCore, deadline: ContinuousClock.Instant, dialDeadline: ContinuousClock.Instant
    ) async throws -> [UInt8] {
        var replay = Replay()
        return try await exchange(
            onReplica: replica, code: code, payload: payload, context: context, identity: identity, lineage: lineage, fence: fence,
            coordinator: coordinator, deadline: deadline, dialDeadline: dialDeadline, replay: &replay)
    }

    private func exchange(
        onReplica replica: UInt8, code: UInt32, payload: [UInt8], context: PartitionContext, identity: Responses.SessionIdentity, lineage: UInt64,
        fence: UInt128Value, coordinator: ClientCore, deadline: ContinuousClock.Instant, dialDeadline: ContinuousClock.Instant, replay: inout Replay
    ) async throws -> [UInt8] {
        let endpoint = try await endpoint(of: replica, lineage: lineage, coordinator: coordinator, deadline: deadline)
        let slot = try slot(fence: fence, at: endpoint)
        try await slot.lock.lock()
        defer { slot.lock.unlock() }

        var data = slot.connection.withLockedValue { $0 }
        if data?.usable == false {
            data = nil
        }
        if data == nil {
            let dialed: (any IggyConnection)?
            do {
                dialed = try await coordinator.dial(endpoint, deadline: dialDeadline)
            } catch let error as IggyError where error.isReconnectable {
                throw IggyError(.transientNotAccepted, context: "cannot reach replica \(replica) at \(endpoint): \(error)")
            }
            guard let dialed else {
                throw UnsentRequest(refusal: IggyError(.transientNotAccepted, context: "the budget ran out before \(endpoint) was dialed"))
            }
            data = DataConnection(connection: dialed, consumerSession: nil, usable: true)
        }
        guard var data else {
            throw IggyError(.notConnected)
        }
        // A cancelled exchange must not leave the connection reusable while
        // its reply can still arrive.
        data.usable = false
        slot.connection.withLockedValue { $0 = data }

        if data.consumerSession.map({ !Self.covers($0, identity) }) ?? true {
            do {
                try await bind(data.connection, to: identity, lineage: lineage, coordinator: coordinator, deadline: deadline)
            } catch let unsent as UnsentRequest {
                data.usable = true
                slot.connection.withLockedValue { $0 = data }
                throw unsent
            } catch let error as IggyError where error.isReconnectable {
                throw IggyError(.transientNotAccepted, context: "binding the session on \(endpoint) failed: \(error)")
            }
            data.consumerSession = identity
            slot.connection.withLockedValue { $0 = data }
        }

        do {
            let body = try await exchange(
                on: data.connection, code: code, payload: payload, context: context, identity: identity, lineage: lineage, coordinator: coordinator,
                deadline: deadline, replay: &replay)
            data.usable = true
            slot.connection.withLockedValue { $0 = data }
            return body
        } catch let unsent as UnsentRequest {
            data.usable = true
            slot.connection.withLockedValue { $0 = data }
            throw unsent
        } catch let error as IggyError {
            data.usable = !error.isReconnectable
            if error.code == .transientNotAccepted {
                data.consumerSession = nil
            }
            slot.connection.withLockedValue { $0 = data }
            if error.code == .notPrimary { throw error }
            if error.isConnectionLoss, error.code != .staleClient {
                if replay.frame?.operation.isPartition == true, error.code != .notConnected, error.code != .cannotEstablishConnection {
                    replay.outcome.markUncertain()
                }
                if replay.outcome.isUncertain { throw error }
            }
            throw Self.gradedLoss(ClientCore.graded(error), on: endpoint)
        }
    }

    /// A request that lost its connection after it was written may have been
    /// applied, so its outcome is unknown on either path. One that found no
    /// connection or no session was not written and is routed again. A reply
    /// that refused it as a stale client ends it with that refusal.
    private static func gradedLoss(_ error: IggyError, on endpoint: String) -> IggyError {
        switch error.code {
        case .notConnected, .cannotEstablishConnection, .unauthenticated:
            return IggyError(.transientNotAccepted, context: "\(endpoint) took no request: \(error)")
        case .staleClient:
            return error
        default:
            guard error.isReconnectable else {
                return error
            }
            return IggyError(.transientNotCommitted, context: "the connection to \(endpoint) failed: \(error)")
        }
    }

    /// Whether a connection bound to `bound` serves `wanted` too.
    private static func covers(_ bound: Responses.SessionIdentity, _ wanted: Responses.SessionIdentity) -> Bool {
        bound.clientID == wanted.clientID && bound.session == wanted.session && bound.metadataWatermark >= wanted.metadataWatermark
    }

    private func bind(
        _ connection: any IggyConnection, to identity: Responses.SessionIdentity, lineage: UInt64, coordinator: ClientCore,
        deadline: ContinuousClock.Instant
    ) async throws {
        let payload = try Requests.bindSession(
            clientID: identity.clientID, session: identity.session, metadataWatermark: identity.metadataWatermark,
            bindSecret: await coordinator.bindSecret)
        var replay = Replay()
        let body = try await exchange(
            on: connection, code: CommandCode.bindSession.rawValue, payload: payload, context: .empty, identity: identity, lineage: lineage,
            coordinator: coordinator, deadline: deadline, replay: &replay)
        let bound = try Responses.loginRegister(body[...])
        if bound.session == 0 {
            throw IggyError(.invalidSession, context: "the server bound session 0")
        }
        if identity.session != 0, bound.session != identity.session {
            throw IggyError(.sessionMismatch, context: "asked to bind session \(identity.session), the server bound \(bound.session)")
        }
    }

    /// One lockstep exchange on a data connection, stamped from the shared
    /// session for `identity`. `notPrimary` is graded as on the coordinator.
    /// A stamp refused for a stale `identity` or `lineage`, or a budget that
    /// ran out first, throws ``UnsentRequest``, since nothing was written.
    /// An offset write answered "not committed" goes again under its request
    /// id, as on the coordinator, until `requestTimeout` from the first
    /// attempt or `deadline` passes: the server answers the replay from its
    /// reply cache if the write committed. The retained frame also survives a
    /// route refresh or a lost connection, while the same session is bound.
    private func exchange(
        on connection: any IggyConnection, code: UInt32, payload: [UInt8], context: PartitionContext, identity: Responses.SessionIdentity,
        lineage: UInt64, coordinator: ClientCore, deadline: ContinuousClock.Instant, replay: inout Replay
    ) async throws -> [UInt8] {
        let replayDeadline = min(deadline, .now + coordinator.configuration.requestTimeout)
        guard replayDeadline > .now else {
            throw UnsentRequest(refusal: IggyError(.transientNotAccepted, context: "the budget ran out before command \(code) was sent"))
        }
        let request: VSRFrame.EncodedRequest
        if let retained = replay.frame {
            guard retained.clientID == identity.clientID, retained.session == identity.session else {
                throw UnsentRequest(refusal: IggyError(.unauthenticated, context: "the retained request belongs to a replaced session"))
            }
            request = retained
        } else {
            do {
                request = try await coordinator.stampRequest(code: code, payload: payload, context: context, for: identity, lineage: lineage)
            } catch let refusal as IggyError where refusal.code == .transientNotAccepted || refusal.code == .unauthenticated {
                throw UnsentRequest(refusal: refusal)
            }
            replay.frame = request
        }
        if let ended = await coordinator.lineageRefusal(lineage, code: code) {
            throw UnsentRequest(refusal: ended)
        }
        while true {
            let raw: RawFrame
            do {
                raw = try await connection.exchange(request.bytes, timeout: replayDeadline - .now)
            } catch let lost as IggyError where replay.outcome.isUncertain {
                // A replay that found the connection closed wrote nothing, but
                // `notConnected` would route the write again under a new id.
                throw IggyError(.disconnected, context: "an earlier attempt may have committed, then: \(lost)")
            }
            let header = raw.header[...]
            if VSRFrame.peekCommand(header) == .reply, VSRFrame.readReplyRequestID(header) != request.requestID {
                await connection.close()
                throw IggyError(.disconnected, context: "the data connection answered another request")
            }
            let answer: IggyError
            do {
                let body = Array(try VSRFrame.decodeReply(header: header, body: raw.body[...]))
                guard replay.outcome.isUncertain, !VSRFrame.hasCommittedResult(header) else {
                    return body
                }
                answer = IggyError(.transientNotCommitted, context: "the reply does not prove the earlier attempt committed")
            } catch let refusal as IggyError {
                if VSRFrame.peekCommand(header) == .eviction, refusal.code == .staleClient {
                    // As on the coordinator, a stale client eviction ends only
                    // the binding of the connection, so the request is lost with
                    // it and a new connection binds the same session again.
                    await connection.close()
                    throw IggyError(.disconnected, context: "\(refusal)")
                }
                if VSRFrame.peekCommand(header) == .eviction, ClientCore.endsSessionOfWrite(refusal, request.operation) {
                    // A write can commit before an eviction ends its session.
                    await connection.close()
                    throw IggyError(.transientNotCommitted, context: "\(refusal)")
                }
                answer = refusal
            }
            if answer.code == .notPrimary, !VSRFrame.hasCommittedResult(header) {
                throw answer
            }
            guard request.operation.isPartition, replay.outcome.isUncertain || answer.code == .transientNotCommitted else {
                throw ClientCore.graded(answer)
            }
            let error = replay.outcome.observe(ClientCore.graded(answer), committed: VSRFrame.hasCommittedResult(header))
            // Once the outcome is unknown, any answer but a transient one ends
            // the replay, as the same request id gets the same answer again.
            // A replay sent with a sliver of the budget would time out, and
            // the timeout takes the connection with it.
            let transient = answer.code == .transientNotCommitted || answer.code == .transientNotAccepted || answer.code == .notPrimary
            let pause = ClientCore.notReadyRetryInterval
            guard error.code == .transientNotCommitted, transient, replayDeadline - .now >= pause + pause else {
                throw error
            }
            try await Task.sleep(for: pause)
            if let ended = await coordinator.lineageRefusal(lineage, code: code) {
                throw replay.outcome.observe(ended)
            }
        }
    }

    /// The cached route of `key` if the session still holds it, else a new
    /// lookup.
    private func route(
        _ key: RouteKey, clustered: Bool, lineage: UInt64, coordinator: ClientCore, deadline: ContinuousClock.Instant
    ) async throws -> Route {
        let watermark = await coordinator.metadataWatermark
        let (fence, cached) = state.withLockedValue { ($0.fence, $0.routes[key]) }
        if let cached, cached.fence == fence, cached.askFirst == nil, cached.view >= cached.minimumView, cached.context.metadataOp >= watermark {
            return cached
        }
        let response = try await lookUp(
            key, clustered: clustered, fence: fence, watermark: watermark, lineage: lineage, coordinator: coordinator, deadline: deadline, preferred: cached)
        guard let session = await coordinator.sessionIdentity(at: response.context.metadataOp) else {
            throw IggyError(.transientNotAccepted, context: "no session is bound to route partition \(key.partitionID)")
        }
        var route = Route(fence: fence, primaryReplica: response.primaryReplica, consumerSession: session, context: response.context, view: response.view)
        try await validate(route, coordinator: coordinator)
        try state.withLockedValue { state in
            try Self.checkFence(route.fence, in: state)
            for (alias, known) in state.routes where alias.partitionID == key.partitionID && known.context.incarnation == route.context.incarnation {
                route.minimumView = max(route.minimumView, known.minimumView)
            }
            guard route.view >= route.minimumView else {
                throw IggyError(.transientNotAccepted, context: "the partition route predates a primary refusal")
            }
            if state.routes.count >= Self.maxCachedRoutes {
                state.routes.removeAll()
            }
            state.routes[key] = route
        }
        return route
    }

    /// Looks up the route of `key`: on the metadata primary in a cluster, on
    /// the coordinator on a single node.
    private func lookUp(
        _ key: RouteKey, clustered: Bool, fence: UInt128Value, watermark: UInt64, lineage: UInt64, coordinator: ClientCore,
        deadline: ContinuousClock.Instant, preferred: Route? = nil
    ) async throws -> Responses.PartitionRoute {
        let payload = try Requests.partitionRoute(streamID: key.stream, topicID: key.topic, partitionID: key.partitionID)
        let context = ClientCore.readContext(watermark)
        if clustered, let preferred, let replica = preferred.askFirst,
            let identity = await coordinator.sessionIdentity(at: watermark)
        {
            do {
                let body = try await exchange(
                    onReplica: replica, code: CommandCode.getPartitionRoute.rawValue, payload: payload, context: context,
                    identity: identity, lineage: lineage, fence: fence, coordinator: coordinator, deadline: deadline,
                    dialDeadline: min(deadline, .now + ClientCore.failoverDialTimeout))
                let response = try Responses.partitionRoute(body[...])
                if response.context.incarnation != preferred.context.incarnation || response.view >= preferred.minimumView {
                    return response
                }
            } catch let unsent as UnsentRequest {
                throw unsent.refusal
            } catch let error as IggyError
                where error.code == .transientNotAccepted || error.code == .transientNotCommitted || error.isNamespaceMiss || error.isConnectionLoss
            {
                // A failed hint leaves the metadata primary as the fallback.
            }
        }
        let body =
            clustered
            ? try await lookUpInCluster(payload, context: context, fence: fence, lineage: lineage, coordinator: coordinator, deadline: deadline)
            : try await control(.getPartitionRoute, payload, context: context, lineage: lineage, coordinator: coordinator, deadline: deadline)
        return try Responses.partitionRoute(body[...])
    }

    /// Looks up a route on the metadata primary, whose miss is final: over
    /// the coordinator when it stands there, so the session needs no second
    /// connection to it, else over a data connection. When the roster names
    /// none, or the one it names does not answer, the coordinator answers
    /// instead, and since it can be a backup its miss is a refusal. A
    /// metadata primary that does not answer is forgotten, so the next
    /// lookup reads the roster again and finds the one a view change elected.
    /// One the client refused to write to, for a stale session or watermark
    /// or a budget that ran out, is kept, and the refusal routes the lookup
    /// again.
    private func lookUpInCluster(
        _ payload: [UInt8], context: PartitionContext, fence: UInt128Value, lineage: UInt64, coordinator: ClientCore,
        deadline: ContinuousClock.Instant
    ) async throws -> [UInt8] {
        if state.withLockedValue({ $0.metadataPrimary == nil }) {
            try await readRoster(coordinator, lineage: lineage, deadline: deadline)
        }
        if let pinned = await coordinatorAtMetadataPrimary(coordinator) {
            do {
                return try await control(.getPartitionRoute, payload, context: context, lineage: lineage, coordinator: coordinator, deadline: deadline)
            } catch let error as IggyError where error.isNamespaceMiss {
                throw await coordinatorMiss(error, pinned: pinned, lineage: lineage, coordinator: coordinator, deadline: deadline)
            }
        }
        if let metadataPrimary = state.withLockedValue({ $0.metadataPrimary }) {
            guard let identity = await coordinator.sessionIdentity(at: context.metadataOp) else {
                throw IggyError(.transientNotAccepted, context: "no session is bound to look up a route")
            }
            do {
                // The coordinator is queued behind this dial, so a metadata
                // primary that drops it must not spend the whole budget.
                return try await exchange(
                    onReplica: metadataPrimary, code: CommandCode.getPartitionRoute.rawValue, payload: payload, context: context,
                    identity: identity, lineage: lineage, fence: fence, coordinator: coordinator, deadline: deadline,
                    dialDeadline: min(deadline, .now + ClientCore.failoverDialTimeout))
            } catch let unsent as UnsentRequest {
                throw unsent.refusal
            } catch let error as IggyError where error.code == .transientNotAccepted || error.code == .transientNotCommitted {
                // A lookup reads, so one lost in flight changed nothing.
                state.withLockedValue { $0.metadataPrimary = nil }
                coordinator.logger.debug("The metadata primary did not answer the route lookup: \(error)")
            } catch let error as IggyError where error.isNamespaceMiss {
                let source = MissSource.replica(identity: identity, lineage: lineage, fence: fence)
                guard await isMetadataPrimary(metadataPrimary, answeredOn: source, coordinator: coordinator, deadline: deadline) else {
                    throw IggyError(.transientNotAccepted, context: "replica \(metadataPrimary) is no longer the metadata primary: \(error)")
                }
                throw error
            }
        }
        do {
            return try await control(.getPartitionRoute, payload, context: context, lineage: lineage, coordinator: coordinator, deadline: deadline)
        } catch let error as IggyError where error.isNamespaceMiss {
            throw IggyError(.transientNotAccepted, context: "the coordinator answered the route lookup in place of the metadata primary: \(error)")
        }
    }

    /// The connection a namespace miss came back on: the coordinator
    /// connection of `epoch`, or the data connection to the replica, bound
    /// as for the request that missed.
    private enum MissSource {
        case coordinator(epoch: UInt64, lineage: UInt64)
        case replica(identity: Responses.SessionIdentity, lineage: UInt64, fence: UInt128Value)
    }

    /// Whether `replica` is the metadata primary. The roster the client holds
    /// can predate a view change, so, as in the Rust SDK, the roster is read
    /// again on the connection that answered the miss before the miss counts
    /// as final. A miss from the coordinator counts only while its connection
    /// stands, so the miss and the roster read came from the same replica.
    private func isMetadataPrimary(
        _ replica: UInt8, answeredOn source: MissSource, coordinator: ClientCore, deadline: ContinuousClock.Instant
    ) async -> Bool {
        guard state.withLockedValue({ $0.metadataPrimary }) == replica else {
            return false
        }
        do {
            switch source {
            case .coordinator(let epoch, let lineage):
                try await readRoster(coordinator, lineage: lineage, deadline: deadline)
                guard await coordinator.standingConnection?.epoch == epoch else {
                    return false
                }
            case .replica(let identity, let lineage, let fence):
                let readDeadline = min(deadline, ContinuousClock.now + Self.rosterReadTimeout)
                let body = try await exchange(
                    onReplica: replica, code: CommandCode.getClusterMetadata.rawValue, payload: [], context: .empty, identity: identity,
                    lineage: lineage, fence: fence, coordinator: coordinator, deadline: readDeadline, dialDeadline: readDeadline)
                try learnRoster(body: body)
            }
        } catch {
            return false
        }
        return state.withLockedValue({ $0.metadataPrimary }) == replica
    }

    /// The coordinator connection while it stands at the address the roster
    /// lists for the metadata primary. A coordinator dialed at another
    /// spelling of the address, such as a host name, is not taken for it.
    private func coordinatorAtMetadataPrimary(_ coordinator: ClientCore) async -> CoordinatorPin? {
        guard let standing = await coordinator.standingConnection else {
            return nil
        }
        return state.withLockedValue { state in
            guard let replica = state.metadataPrimary, let node = state.nodes[replica], isSameSpelling(node.tcpAddress, standing.address) else {
                return nil
            }
            return CoordinatorPin(replica: replica, epoch: standing.epoch)
        }
    }

    /// `miss`, which the coordinator answered, when `pinned` stood at the
    /// metadata primary and still does, as a roster read confirms. Else the
    /// refusal that routes the request again.
    private func coordinatorMiss(
        _ miss: IggyError, pinned: CoordinatorPin?, lineage: UInt64, coordinator: ClientCore, deadline: ContinuousClock.Instant
    ) async -> IggyError {
        guard let pinned,
            await isMetadataPrimary(
                pinned.replica, answeredOn: .coordinator(epoch: pinned.epoch, lineage: lineage), coordinator: coordinator, deadline: deadline)
        else {
            return IggyError(.transientNotAccepted, context: "the coordinator is not the confirmed metadata primary: \(miss)")
        }
        return miss
    }

    /// Refuses a route the session moved past: a reset replaced it, or a
    /// metadata commit newer than the route was seen since.
    private func validate(_ route: Route, coordinator: ClientCore) async throws {
        let watermark = await coordinator.metadataWatermark
        try state.withLockedValue { try Self.checkFence(route.fence, in: $0) }
        guard route.context.metadataOp >= watermark else {
            throw IggyError(.transientNotAccepted, context: "the route predates metadata commit \(watermark)")
        }
    }

    private static func checkFence(_ fence: UInt128Value, in state: State) throws {
        guard fence == state.fence else {
            throw IggyError(.transientNotAccepted, context: "the route belongs to a replaced session")
        }
    }

    /// The slot of the data connection to `endpoint`. A session replaced
    /// since `fence` throws ``UnsentRequest``, since nothing was written.
    private func slot(fence: UInt128Value, at endpoint: String) throws -> Slot {
        try state.withLockedValue { state in
            do {
                try Self.checkFence(fence, in: state)
            } catch let refusal as IggyError {
                throw UnsentRequest(refusal: refusal)
            }
            if let slot = state.slots[endpoint] {
                return slot
            }
            guard state.slots.count < Self.maxDataConnections else {
                throw IggyError(.invalidConfiguration, context: "the pool of \(Self.maxDataConnections) primary connections is full")
            }
            let slot = Slot()
            state.slots[endpoint] = slot
            return slot
        }
    }

    private func removeRoute(_ key: RouteKey) {
        _ = state.withLockedValue { $0.routes.removeValue(forKey: key) }
    }

    private func markNotPrimary(_ route: Route, partitionID: UInt32) {
        state.withLockedValue { state in
            for key in state.routes.keys where key.partitionID == partitionID && state.routes[key]?.context.incarnation == route.context.incarnation {
                let floor = max(state.routes[key]?.minimumView ?? 0, route.view == .max ? .max : route.view + 1)
                state.routes[key]?.minimumView = floor
                state.routes[key]?.askFirst = route.primaryReplica
            }
        }
    }
}
