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

/// Routes polls and consumer-offset writes to the primary of their
/// partition, a port of `PollRouter` in `core/sdk/src/driver/poll_router.rs`.
///
/// A request first asks the coordinator, the connection the client signed in
/// on, for its route (`GetPollRouting` or `GetConsumerOffsetRouting`): the
/// partition primary and the partition context the request carries. On a
/// single node the request then goes over the coordinator connection. In a
/// cluster it goes over a data connection to the primary, bound to the
/// coordinator's session with `BindSession`, so consumer-group membership
/// stays with the coordinator. Routes are cached until a request on them
/// fails or the session is replaced.
final class PollRouter: Sendable {
    /// Bounds one routed request across its lookups, attempts, and pauses.
    static let pollTimeout: Duration = .seconds(30)
    static let retryInterval: Duration = .milliseconds(50)
    static let maxRetryInterval: Duration = .seconds(1)
    static let rosterReadTimeout: Duration = .seconds(5)
    static let maxCachedRoutes = 4096
    static let maxDataConnections = 256

    /// A route is cached per lookup code and the request prefix that names
    /// the partition and the consumer.
    struct RouteKey: Hashable, Sendable {
        let code: CommandCode
        let prefix: [UInt8]
    }

    private struct Route: Sendable {
        /// The fence of the session the route was read under.
        let fence: UInt128Value
        let endpoint: String
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
        /// Nodes in the roster; zero until a roster read succeeded.
        var rosterSize = 0
        var routes: [RouteKey: Route] = [:]
        var slots: [String: Slot] = [:]
        var nextHeartbeat: ContinuousClock.Instant?
    }

    private let state: NIOLockedValueBox<State>

    init(fence: UInt128Value) {
        state = NIOLockedValueBox(State(fence: fence))
    }

    /// Records the roster size a leader check or a roster read reported.
    func learnRosterSize(_ size: Int) {
        state.withLockedValue { $0.rosterSize = size }
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

    /// Sends a poll or an offset write on its route. `context` is the
    /// partition context the caller captured, else the route's is taken and
    /// kept for every retry.
    func send(
        _ code: CommandCode, payload: [UInt8], key: RouteKey, routePayload: [UInt8], context: PartitionContext?, coordinator: ClientCore
    ) async throws -> [UInt8] {
        let clustered = try await isClustered(coordinator)
        let clock = ContinuousClock()
        let now = clock.now
        let deadline = now + Self.pollTimeout
        let routed: [UInt8]? = try await withDeadline(deadline) { [self] in
            if clustered, heartbeatDue(at: now, interval: coordinator.configuration.heartbeatInterval) {
                // The requests leave the coordinator connection idle, so it is
                // pinged here once per heartbeat interval too.
                _ = try await control(.ping, [], coordinator: coordinator)
            }
            var captured = context
            var interval = Self.retryInterval
            while true {
                do {
                    if clustered {
                        return try await sendOnPrimary(
                            code, payload: payload, key: key, routePayload: routePayload, context: &captured, coordinator: coordinator)
                    }
                    return try await sendOnCoordinator(
                        code, payload: payload, key: key, routePayload: routePayload, context: &captured, coordinator: coordinator)
                } catch let error as IggyError where error.code == .transientNotAccepted {
                    removeRoute(key)
                    guard clock.now + interval < deadline else {
                        throw error
                    }
                    try await Task.sleep(for: interval)
                    interval = min(interval * 2, Self.maxRetryInterval)
                }
            }
        }
        guard let routed else {
            removeRoute(key)
            throw IggyError(.transientNotCommitted, context: "command \(code.rawValue) did not complete within \(Self.pollTimeout)")
        }
        return routed
    }

    /// Whether the roster has more than one node, reading it once when no
    /// leader check reported it yet.
    private func isClustered(_ coordinator: ClientCore) async throws -> Bool {
        if state.withLockedValue({ $0.rosterSize }) == 0 {
            let body: [UInt8]? = try await withDeadline(ContinuousClock.now + Self.rosterReadTimeout) { [self] in
                try await control(.getClusterMetadata, [], coordinator: coordinator)
            }
            guard let body else {
                throw IggyError(.transientNotAccepted, context: "reading the cluster roster took longer than \(Self.rosterReadTimeout)")
            }
            let metadata = try Responses.clusterMetadata(body[...])
            guard !metadata.nodes.isEmpty else {
                throw IggyError(.transientNotAccepted, context: "the cluster roster is empty")
            }
            learnRosterSize(metadata.nodes.count)
        }
        return state.withLockedValue { $0.rosterSize } > 1
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
    private func control(_ code: CommandCode, _ payload: [UInt8], coordinator: ClientCore) async throws -> [UInt8] {
        do {
            return try await coordinator.exchangeOnce(code: code.rawValue, payload: payload, context: .empty)
        } catch let error as IggyError where error.isReconnectable {
            _ = try await coordinator.exchange(code: CommandCode.ping.rawValue, payload: [])
            return try await coordinator.exchangeOnce(code: code.rawValue, payload: payload, context: .empty)
        }
    }

    /// The single-node path: the request goes over the coordinator connection
    /// with the context of its route.
    private func sendOnCoordinator(
        _ code: CommandCode, payload: [UInt8], key: RouteKey, routePayload: [UInt8], context: inout PartitionContext?, coordinator: ClientCore
    ) async throws -> [UInt8] {
        let route = try await route(key, routePayload: routePayload, coordinator: coordinator)
        let captured = context ?? route.context
        context = captured
        do {
            if code == .pollMessagesOnPrimary {
                return try await coordinator.exchangeOnce(code: code.rawValue, payload: payload, context: captured)
            }
            return try await coordinator.exchange(code: code.rawValue, payload: payload, context: captured)
        } catch {
            removeRoute(key)
            throw error
        }
    }

    /// The cluster path: the request goes over a data connection to the
    /// partition primary, bound to the coordinator's session first.
    private func sendOnPrimary(
        _ code: CommandCode, payload: [UInt8], key: RouteKey, routePayload: [UInt8], context: inout PartitionContext?, coordinator: ClientCore
    ) async throws -> [UInt8] {
        let route = try await route(key, routePayload: routePayload, coordinator: coordinator)
        let captured = context ?? route.context
        context = captured
        try await validate(route, coordinator: coordinator)
        let slot = try slot(for: route)
        try await slot.lock.lock()
        defer { slot.lock.unlock() }
        try await validate(route, coordinator: coordinator)

        var data = slot.connection.withLockedValue { $0 }
        if data?.usable == false {
            data = nil
        }
        if data == nil {
            let connection: any IggyConnection
            do {
                connection = try await coordinator.dial(route.endpoint)
            } catch let error as IggyError where error.isReconnectable {
                throw IggyError(.transientNotAccepted, context: "cannot reach the partition primary at \(route.endpoint): \(error)")
            }
            try await validate(route, coordinator: coordinator)
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
                try await bind(data.connection, to: route.consumerSession, coordinator: coordinator)
            } catch let error as IggyError where error.isReconnectable {
                throw IggyError(.transientNotAccepted, context: "binding the session on \(route.endpoint) failed: \(error)")
            }
            data.consumerSession = route.consumerSession
            slot.connection.withLockedValue { $0 = data }
        }
        try await validate(route, coordinator: coordinator)

        do {
            let body = try await exchange(on: data.connection, code: code.rawValue, payload: payload, context: captured, coordinator: coordinator)
            data.usable = true
            slot.connection.withLockedValue { $0 = data }
            return body
        } catch let error as IggyError {
            data.usable = !error.isReconnectable
            if error.code == .transientNotAccepted {
                data.consumerSession = nil
            }
            slot.connection.withLockedValue { $0 = data }
            removeRoute(key)
            if error.isReconnectable {
                // The request was written, so it may have been applied.
                throw IggyError(.transientNotCommitted, context: "the data connection to \(route.endpoint) failed: \(error)")
            }
            throw error
        }
    }

    /// Whether a connection bound to `bound` serves `wanted` too.
    private static func covers(_ bound: Responses.SessionIdentity, _ wanted: Responses.SessionIdentity) -> Bool {
        bound.clientID == wanted.clientID && bound.session == wanted.session && bound.metadataWatermark >= wanted.metadataWatermark
    }

    private func bind(_ connection: any IggyConnection, to identity: Responses.SessionIdentity, coordinator: ClientCore) async throws {
        let payload = try Requests.bindSession(
            clientID: identity.clientID, session: identity.session, metadataWatermark: identity.metadataWatermark,
            bindSecret: await coordinator.bindSecret)
        let body = try await exchange(on: connection, code: CommandCode.bindSession.rawValue, payload: payload, context: .empty, coordinator: coordinator)
        let bound = try Responses.loginRegister(body[...])
        if bound.session == 0 {
            throw IggyError(.invalidSession, context: "the server bound session 0")
        }
        if identity.session != 0, bound.session != identity.session {
            throw IggyError(.sessionMismatch, context: "asked to bind session \(identity.session), the server bound \(bound.session)")
        }
    }

    /// One lockstep exchange on a data connection, stamped from the shared
    /// session, with no replay.
    private func exchange(
        on connection: any IggyConnection, code: UInt32, payload: [UInt8], context: PartitionContext, coordinator: ClientCore
    ) async throws -> [UInt8] {
        let request = try await coordinator.stampRequest(code: code, payload: payload, context: context)
        let raw = try await connection.exchange(request.bytes, timeout: coordinator.configuration.requestTimeout)
        let header = raw.header[...]
        if VSRFrame.peekCommand(header) == .reply, VSRFrame.readReplyRequestID(header) != request.requestID {
            await connection.close()
            throw IggyError(.disconnected, context: "the data connection answered another request")
        }
        return Array(try VSRFrame.decodeReply(header: header, body: raw.body[...]))
    }

    /// The cached route of `key` if the session still holds it, else a new
    /// lookup on the coordinator.
    private func route(_ key: RouteKey, routePayload: [UInt8], coordinator: ClientCore) async throws -> Route {
        let watermark = await coordinator.metadataWatermark
        let (fence, cached) = state.withLockedValue { ($0.fence, $0.routes[key]) }
        if let cached, cached.fence == fence, cached.consumerSession.metadataWatermark >= watermark {
            return cached
        }
        let body = try await control(key.code, routePayload, coordinator: coordinator)
        var response = try Responses.pollRoute(body[...])
        response.consumerSession.metadataWatermark = max(response.consumerSession.metadataWatermark, watermark)
        guard response.primary.endpoints.tcp != 0 else {
            coordinator.logger.error("The partition primary \(response.primary.name) does not expose TCP")
            throw IggyError(.featureUnavailable, context: "the partition primary \(response.primary.name) does not expose TCP")
        }
        let route = Route(fence: fence, endpoint: response.primary.tcpAddress, consumerSession: response.consumerSession, context: response.context)
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
        guard route.consumerSession.metadataWatermark >= watermark else {
            throw IggyError(.transientNotAccepted, context: "the route predates metadata commit \(watermark)")
        }
    }

    private static func checkFence(of route: Route, in state: State) throws {
        guard route.fence == state.fence else {
            throw IggyError(.transientNotAccepted, context: "the route belongs to a replaced session")
        }
    }

    private func slot(for route: Route) throws -> Slot {
        try state.withLockedValue { state in
            try Self.checkFence(of: route, in: state)
            let endpoint = route.endpoint
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
