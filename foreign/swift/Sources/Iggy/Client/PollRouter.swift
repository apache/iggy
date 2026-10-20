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
/// A request first asks the coordinator, the connection the client signed in
/// on, for the route of its partition (`GetPartitionRoute`): the partition
/// context the request carries and the replica id of the partition primary,
/// which the cluster roster maps to an endpoint. On a single node the request
/// then goes over the coordinator connection. In a cluster it goes over a
/// data connection to the primary, bound to the client's session with
/// `BindSession`, so consumer-group membership stays with that session.
/// Routes are cached until a request on them fails or the session is
/// replaced.
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
    /// keeps asking. A replica id the roster repeats keeps its first node.
    func learnRoster(_ metadata: ClusterMetadata) {
        guard !metadata.nodes.isEmpty else {
            return
        }
        let nodes = Dictionary(metadata.nodes.map { ($0.replicaID, $0) }, uniquingKeysWith: { first, _ in first })
        state.withLockedValue { $0.nodes = nodes }
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
    /// The budget is checked between attempts and caps each exchange instead
    /// of cancelling one in flight, which would take the coordinator
    /// connection, and the session's group membership, with it.
    func send(
        _ code: CommandCode, payload: [UInt8], key: RouteKey, context: PartitionContext?, coordinator: ClientCore
    ) async throws -> [UInt8] {
        let clock = ContinuousClock()
        let now = clock.now
        let deadline = now + Self.pollTimeout
        let clustered = try await isClustered(coordinator, deadline: deadline)
        if clustered, heartbeatDue(at: now, interval: coordinator.configuration.heartbeatInterval) {
            // The requests leave the coordinator connection idle, so it is
            // pinged here once per heartbeat interval too.
            _ = try await control(.ping, [], coordinator: coordinator, deadline: deadline)
        }
        let request = Request(code: code, payload: payload, key: key, deadline: deadline)
        var captured = context
        var interval = Self.retryInterval
        while clock.now < deadline {
            do {
                if clustered {
                    return try await sendOnPrimary(request, context: &captured, coordinator: coordinator)
                }
                return try await sendOnCoordinator(request, context: &captured, coordinator: coordinator)
            } catch let error as IggyError where error.code == .transientNotAccepted {
                removeRoute(key)
                guard clock.now + interval < deadline else {
                    throw error
                }
                try await Task.sleep(for: interval)
                interval = min(interval * 2, Self.maxRetryInterval)
            }
        }
        removeRoute(key)
        throw IggyError(.transientNotCommitted, context: "command \(code.rawValue) did not complete within \(Self.pollTimeout)")
    }

    private struct Request {
        let code: CommandCode
        let payload: [UInt8]
        let key: RouteKey
        let deadline: ContinuousClock.Instant
    }

    /// Whether the roster has more than one node, reading it once when no
    /// leader check reported it yet.
    private func isClustered(_ coordinator: ClientCore, deadline: ContinuousClock.Instant) async throws -> Bool {
        if state.withLockedValue({ $0.nodes.isEmpty }) {
            try await readRoster(coordinator, deadline: deadline)
        }
        return state.withLockedValue { $0.nodes.count } > 1
    }

    private func readRoster(_ coordinator: ClientCore, deadline: ContinuousClock.Instant) async throws {
        let readDeadline = min(deadline, ContinuousClock.now + Self.rosterReadTimeout)
        let body = try await control(.getClusterMetadata, [], coordinator: coordinator, deadline: readDeadline)
        let metadata = try Responses.clusterMetadata(body[...])
        guard !metadata.nodes.isEmpty else {
            throw IggyError(.transientNotAccepted, context: "the cluster roster is empty")
        }
        learnRoster(metadata)
    }

    /// The TCP address of `replica`. A roster that does not list it is read
    /// once more before the route is refused.
    private func endpoint(of replica: UInt8, coordinator: ClientCore, deadline: ContinuousClock.Instant) async throws -> String {
        var node = state.withLockedValue { $0.nodes[replica] }
        if node == nil {
            try await readRoster(coordinator, deadline: deadline)
            node = state.withLockedValue { $0.nodes[replica] }
        }
        guard let node else {
            throw IggyError(.transientNotAccepted, context: "the cluster roster does not list replica \(replica)")
        }
        guard node.endpoints.tcp != 0 else {
            coordinator.logger.error("The partition primary \(node.name) does not expose TCP")
            throw IggyError(.featureUnavailable, context: "the partition primary \(node.name) does not expose TCP")
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
    /// once more.
    private func control(_ code: CommandCode, _ payload: [UInt8], coordinator: ClientCore, deadline: ContinuousClock.Instant) async throws -> [UInt8] {
        do {
            return try await coordinator.exchangeOnce(code: code.rawValue, payload: payload, context: .empty, deadline: deadline)
        } catch let error as IggyError where error.isReconnectable && ContinuousClock.now < deadline {
            _ = try await coordinator.exchange(code: CommandCode.ping.rawValue, payload: [], deadline: deadline)
            return try await coordinator.exchangeOnce(code: code.rawValue, payload: payload, context: .empty, deadline: deadline)
        }
    }

    /// The single-node path: the request goes over the coordinator connection
    /// with the context of its route.
    private func sendOnCoordinator(_ request: Request, context: inout PartitionContext?, coordinator: ClientCore) async throws -> [UInt8] {
        let route = try await route(request.key, coordinator: coordinator, deadline: request.deadline)
        let captured = context ?? route.context
        context = captured
        do {
            if request.code == .pollMessages {
                return try await coordinator.exchangeOnce(
                    code: request.code.rawValue, payload: request.payload, context: captured, deadline: request.deadline)
            }
            return try await coordinator.exchange(code: request.code.rawValue, payload: request.payload, context: captured, deadline: request.deadline)
        } catch let error as IggyError {
            removeRoute(request.key)
            throw Self.gradedLoss(error, on: "the coordinator")
        }
    }

    /// The cluster path: the request goes over a data connection to the
    /// partition primary, bound to the client's session first. Every
    /// frame is stamped under a check that the route's session is still the
    /// client's, so nothing is written for a session that was replaced.
    private func sendOnPrimary(_ request: Request, context: inout PartitionContext?, coordinator: ClientCore) async throws -> [UInt8] {
        let route = try await route(request.key, coordinator: coordinator, deadline: request.deadline)
        let captured = context ?? route.context
        context = captured
        let endpoint = try await endpoint(of: route.primaryReplica, coordinator: coordinator, deadline: request.deadline)
        let slot = try slot(for: route, at: endpoint)
        try await slot.lock.lock()
        defer { slot.lock.unlock() }

        var data = slot.connection.withLockedValue { $0 }
        if data?.usable == false {
            data = nil
        }
        if data == nil {
            let connection: any IggyConnection
            do {
                connection = try await coordinator.dial(endpoint)
            } catch let error as IggyError where error.isReconnectable {
                throw IggyError(.transientNotAccepted, context: "cannot reach the partition primary at \(endpoint): \(error)")
            }
            data = DataConnection(connection: connection, consumerSession: nil, usable: true)
        }
        guard var data else {
            throw IggyError(.notConnected)
        }
        // A cancelled exchange must not leave the connection reusable while
        // its reply can still arrive.
        data.usable = false
        slot.connection.withLockedValue { $0 = data }

        if data.consumerSession.map({ !Self.covers($0, route.consumerSession) }) ?? true {
            do {
                try await bind(data.connection, to: route.consumerSession, coordinator: coordinator, deadline: request.deadline)
            } catch let error as IggyError where error.isReconnectable {
                throw IggyError(.transientNotAccepted, context: "binding the session on \(endpoint) failed: \(error)")
            }
            data.consumerSession = route.consumerSession
            slot.connection.withLockedValue { $0 = data }
        }

        do {
            let body = try await exchange(
                on: data.connection, code: request.code.rawValue, payload: request.payload, context: captured, identity: route.consumerSession,
                coordinator: coordinator, deadline: request.deadline)
            data.usable = true
            slot.connection.withLockedValue { $0 = data }
            return body
        } catch let error as IggyError {
            data.usable = !error.isReconnectable
            if error.code == .transientNotAccepted {
                data.consumerSession = nil
            }
            slot.connection.withLockedValue { $0 = data }
            removeRoute(request.key)
            throw Self.gradedLoss(error, on: endpoint)
        }
    }

    /// A request that lost its connection after it was written may have been
    /// applied, so its outcome is unknown on either path. One that found no
    /// connection or no session was not written and is routed again.
    private static func gradedLoss(_ error: IggyError, on endpoint: String) -> IggyError {
        switch error.code {
        case .notConnected, .cannotEstablishConnection, .unauthenticated:
            return IggyError(.transientNotAccepted, context: "\(endpoint) took no request: \(error)")
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
        _ connection: any IggyConnection, to identity: Responses.SessionIdentity, coordinator: ClientCore, deadline: ContinuousClock.Instant
    ) async throws {
        let payload = try Requests.bindSession(
            clientID: identity.clientID, session: identity.session, metadataWatermark: identity.metadataWatermark,
            bindSecret: await coordinator.bindSecret)
        let body = try await exchange(
            on: connection, code: CommandCode.bindSession.rawValue, payload: payload, context: .empty, identity: identity, coordinator: coordinator,
            deadline: deadline)
        let bound = try Responses.loginRegister(body[...])
        if bound.session == 0 {
            throw IggyError(.invalidSession, context: "the server bound session 0")
        }
        if identity.session != 0, bound.session != identity.session {
            throw IggyError(.sessionMismatch, context: "asked to bind session \(identity.session), the server bound \(bound.session)")
        }
    }

    /// One lockstep exchange on a data connection, stamped from the shared
    /// session for `identity`, with no replay. `notPrimary` is graded as on
    /// the coordinator.
    private func exchange(
        on connection: any IggyConnection, code: UInt32, payload: [UInt8], context: PartitionContext, identity: Responses.SessionIdentity,
        coordinator: ClientCore, deadline: ContinuousClock.Instant
    ) async throws -> [UInt8] {
        let timeout = min(coordinator.configuration.requestTimeout, deadline - .now)
        guard timeout > .zero else {
            throw IggyError(.transientNotAccepted, context: "the budget ran out before command \(code) was sent")
        }
        let request = try await coordinator.stampRequest(code: code, payload: payload, context: context, for: identity)
        let raw = try await connection.exchange(request.bytes, timeout: timeout)
        let header = raw.header[...]
        if VSRFrame.peekCommand(header) == .reply, VSRFrame.readReplyRequestID(header) != request.requestID {
            await connection.close()
            throw IggyError(.disconnected, context: "the data connection answered another request")
        }
        do {
            return Array(try VSRFrame.decodeReply(header: header, body: raw.body[...]))
        } catch let error as IggyError {
            throw ClientCore.graded(error)
        }
    }

    /// The cached route of `key` if the session still holds it, else a new
    /// lookup on the coordinator.
    private func route(_ key: RouteKey, coordinator: ClientCore, deadline: ContinuousClock.Instant) async throws -> Route {
        let watermark = await coordinator.metadataWatermark
        let (fence, cached) = state.withLockedValue { ($0.fence, $0.routes[key]) }
        if let cached, cached.fence == fence, cached.context.metadataOp >= watermark {
            return cached
        }
        let payload = try Requests.partitionRoute(streamID: key.stream, topicID: key.topic, partitionID: key.partitionID)
        let body = try await control(.getPartitionRoute, payload, coordinator: coordinator, deadline: deadline)
        let response = try Responses.partitionRoute(body[...])
        guard let session = await coordinator.sessionIdentity(at: response.context.metadataOp) else {
            throw IggyError(.transientNotAccepted, context: "no session is bound to route partition \(key.partitionID)")
        }
        let route = Route(fence: fence, primaryReplica: response.primaryReplica, consumerSession: session, context: response.context)
        try await validate(route, coordinator: coordinator)
        try state.withLockedValue { state in
            try Self.checkFence(of: route, in: state)
            if state.routes.count >= Self.maxCachedRoutes {
                state.routes.removeAll()
            }
            state.routes[key] = route
        }
        return route
    }

    /// Refuses a route the session moved past: a reset replaced it, or a
    /// metadata commit newer than the route was seen since.
    private func validate(_ route: Route, coordinator: ClientCore) async throws {
        let watermark = await coordinator.metadataWatermark
        try state.withLockedValue { try Self.checkFence(of: route, in: $0) }
        guard route.context.metadataOp >= watermark else {
            throw IggyError(.transientNotAccepted, context: "the route predates metadata commit \(watermark)")
        }
    }

    private static func checkFence(of route: Route, in state: State) throws {
        guard route.fence == state.fence else {
            throw IggyError(.transientNotAccepted, context: "the route belongs to a replaced session")
        }
    }

    private func slot(for route: Route, at endpoint: String) throws -> Slot {
        try state.withLockedValue { state in
            try Self.checkFence(of: route, in: state)
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
}
