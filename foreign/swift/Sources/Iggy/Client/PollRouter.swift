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
    /// `MAX_DEFERRED_REQUESTS` in the Rust SDK: the deferred requests one
    /// client keeps in flight. The server refuses more than it parks for one
    /// client with `deferredPollCapacityExceeded`.
    static let maxDeferredRequests = 64
    static let maxProgressLanes = maxDeferredRequests * Requests.maxPartitionsPerRequest + maxDataConnections

    /// A partition as its caller spelled it. A stream or topic named by
    /// number and the same one named by name are two keys.
    struct RouteKey: Hashable, Sendable {
        let stream: Identifier
        let topic: Identifier
        let partitionID: UInt32
    }

    struct ProgressKey: Hashable, Sendable {
        let partitionID: UInt32
        let incarnation: UInt64
    }

    private struct ProgressLane {
        let semaphore = AsyncSemaphore(permits: 1)
        var users = 0
    }

    final class FetchAttempt: Sendable {
        let id: UInt128Value
        private let possibleSend = NIOLockedValueBox(false)

        init(id: UInt128Value) {
            self.id = id
        }

        var mayHaveCommitted: Bool { possibleSend.withLockedValue { $0 } }

        func sent() { possibleSend.withLockedValue { $0 = true } }
        func refused() { possibleSend.withLockedValue { $0 = false } }
    }

    private struct FetchRetention {
        var keys: [ProgressKey]
        var blocked = false
    }

    /// What a routed request carries.
    enum Body: Sendable {
        case fixed([UInt8])
        case deferred(Deferred)

        var isDeferred: Bool {
            if case .deferred = self { true } else { false }
        }

        var fetchAttempt: FetchAttempt? {
            if case .deferred(let deferred) = self { deferred.fetchAttempt } else { nil }
        }

        /// The body of one attempt. A deferred request carries what is left
        /// of its wait and its timeout, as `deferred_payload` in the Rust SDK,
        /// so no attempt waits past the deadline of its call.
        func payload() throws -> [UInt8] {
            switch self {
            case .fixed(let payload):
                payload
            case .deferred(let deferred):
                try deferred.encode(deferred.options.remaining(after: .now - deferred.started))
            }
        }
    }

    /// A deferred poll or fetch: its options, when its call started,
    /// and its body under the options left for an attempt.
    struct Deferred: Sendable {
        let options: DeferredPollOptions
        let started: ContinuousClock.Instant
        var fetchAttempt: FetchAttempt? = nil
        let encode: @Sendable (DeferredPollOptions) throws -> [UInt8]

        /// The request timeout bounds the whole call, routing and retries
        /// included, not each attempt.
        var deadline: ContinuousClock.Instant { started + options.requestTimeout }
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
        /// The connections of deferred requests by endpoint, one per request
        /// in flight there, as a parked request holds its connection for its
        /// whole wait. At most ``PollRouter/maxDeferredRequests`` across all
        /// endpoints.
        var deferredSlots: [String: [Slot]] = [:]
        // A session reset must not release progress still held by an older call.
        var progressLanes: [ProgressKey: ProgressLane] = [:]
        var fetches: [UInt128Value: FetchRetention] = [:]
        var blockedProgress: [ProgressKey: UInt128Value] = [:]
        var nextHeartbeat: ContinuousClock.Instant?
    }

    private let state: NIOLockedValueBox<State>
    private let deferredLeases = AsyncSemaphore(permits: PollRouter.maxDeferredRequests)

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
            state.deferredSlots.removeAll()
            state.nextHeartbeat = nil
        }
    }

    /// Orders progress across connections and aliases of the same partition
    /// history. Sorted acquisition prevents overlapping fetches from deadlocking.
    /// Entries exist only while a holder or waiter references them.
    func withProgress<Value: Sendable>(
        _ keys: [ProgressKey], deadline: ContinuousClock.Instant, _ operation: () async throws -> Value
    ) async throws -> Value {
        guard !keys.isEmpty else {
            return try await operation()
        }
        let keys = Set(keys).sorted {
            ($0.partitionID, $0.incarnation) < ($1.partitionID, $1.incarnation)
        }
        guard keys.allSatisfy({ $0.incarnation != 0 }) else {
            throw IggyError(.transientNotAccepted, context: "progress requires a resolved partition incarnation")
        }
        let semaphores = try state.withLockedValue { state in
            if let blocked = Self.blockedProgress(keys, in: state) {
                throw blocked
            }
            let added = keys.reduce(0) { $0 + (state.progressLanes[$1] == nil ? 1 : 0) }
            guard added <= Self.maxProgressLanes - state.progressLanes.count else {
                throw IggyError(.transientNotAccepted, context: "the concurrent progress partition limit was reached")
            }
            return keys.map { key in
                var lane = state.progressLanes[key] ?? ProgressLane()
                lane.users += 1
                state.progressLanes[key] = lane
                return lane.semaphore
            }
        }
        var acquired = 0
        defer {
            for semaphore in semaphores.prefix(acquired) {
                semaphore.release(1)
            }
            state.withLockedValue { state in
                for key in keys {
                    state.progressLanes[key]?.users -= 1
                    if state.progressLanes[key]?.users == 0 {
                        state.progressLanes.removeValue(forKey: key)
                    }
                }
            }
        }
        for semaphore in semaphores {
            guard try await semaphore.acquire(1, deadline: deadline) else {
                throw IggyError(.transientNotAccepted, context: "the request deadline passed waiting for partition progress")
            }
            acquired += 1
        }
        if let blocked = state.withLockedValue({ Self.blockedProgress(keys, in: $0) }) {
            throw blocked
        }
        try Task.checkCancellation()
        guard ContinuousClock.now < deadline else {
            throw IggyError(.transientNotAccepted, context: "the request deadline passed before partition progress")
        }
        return try await operation()
    }

    func reserveFetch(_ keys: [ProgressKey]) throws -> FetchAttempt {
        try state.withLockedValue { state in
            guard state.fetches.count < Self.maxDeferredRequests else {
                throw IggyError(.transientNotAccepted, context: "the retained fetch limit was reached")
            }
            var id = UInt128Value.random()
            while state.fetches[id] != nil {
                id = .random()
            }
            state.fetches[id] = FetchRetention(keys: keys)
            return FetchAttempt(id: id)
        }
    }

    func releaseFetch(_ attempt: FetchAttempt) {
        state.withLockedValue { state in
            if state.fetches[attempt.id]?.blocked == false {
                state.fetches.removeValue(forKey: attempt.id)
            }
        }
    }

    func retainFetch(_ attempt: FetchAttempt, keys: [ProgressKey], cause: String? = nil) -> IggyError {
        state.withLockedValue { state in
            state.fetches[attempt.id] = FetchRetention(keys: keys, blocked: true)
            for key in keys {
                state.blockedProgress[key] = attempt.id
            }
        }
        return IggyError(.fetchProgressUncertain, context: cause, operationID: attempt.id)
    }

    func hasUncertainFetch(_ id: UInt128Value) -> Bool {
        state.withLockedValue { $0.fetches[id]?.blocked == true }
    }

    func progressRefusal(partitionID: UInt32, incarnation: UInt64) -> IggyError? {
        state.withLockedValue { Self.blockedProgress([ProgressKey(partitionID: partitionID, incarnation: incarnation)], in: $0) }
    }

    func abandonFetch(_ id: UInt128Value) throws {
        try state.withLockedValue { state in
            guard let retained = state.fetches[id], retained.blocked else {
                throw IggyError(.operationNotRetained, operationID: id)
            }
            for key in retained.keys where state.blockedProgress[key] == id {
                state.blockedProgress.removeValue(forKey: key)
            }
            state.fetches.removeValue(forKey: id)
        }
    }

    private static func blockedProgress(_ keys: [ProgressKey], in state: State) -> IggyError? {
        for key in keys {
            if let operationID = state.blockedProgress[key] {
                return IggyError(.partitionLaneBlocked, operationID: operationID, partitionID: key.partitionID)
            }
        }
        return nil
    }

    static func fetchRefused(_ code: IggyErrorCode) -> Bool {
        switch code {
        case .featureUnavailable, .invalidIdentifier, .transientNotAccepted, .historyUnavailable, .notPrimary, .deferredPollCapacityExceeded,
            .streamIdNotFound, .streamNameNotFound, .topicIdNotFound, .topicNameNotFound, .partitionNotFound, .invalidOffset, .consumerGroupPartitionNotOwned:
            true
        default: false
        }
    }

    static func fetchSectionRefused(_ code: IggyErrorCode) -> Bool {
        switch code {
        case .invalidSizeBytes, .historyUnavailable, .notPrimary, .consumerGroupPartitionNotOwned: true
        default: false
        }
    }

    func progressContext(
        _ key: RouteKey, context: PartitionContext?, lineage: UInt64, coordinator: ClientCore, deadline: ContinuousClock.Instant
    ) async throws -> PartitionContext {
        if let context, context.incarnation != 0 {
            return context
        }
        return try await retryingLookups(lineage: lineage, coordinator: coordinator, deadline: deadline) { clustered in
            let route = try await route(key, clustered: clustered, lineage: lineage, coordinator: coordinator, deadline: deadline)
            guard route.context.incarnation != 0 else {
                throw IggyError(.transientNotAccepted, context: "the route has no partition incarnation")
            }
            guard let context else {
                return route.context
            }
            return PartitionContext(
                incarnation: route.context.incarnation, ownerGeneration: context.ownerGeneration, metadataOp: context.metadataOp)
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
    ///
    /// A deferred request goes over a data connection of its own, on a single
    /// node too, as its wait would hold a lockstep connection. It holds one of
    /// ``maxDeferredRequests`` leases across its attempts, waited for until
    /// `deadline`. A cancelled caller sends command 108 after a request that
    /// is parked, so the server drops it, and the connection stays usable.
    func send(
        _ code: CommandCode, body: Body, key: RouteKey, context: PartitionContext?, lineage: UInt64, coordinator: ClientCore,
        deadline: ContinuousClock.Instant
    ) async throws -> [UInt8] {
        if body.isDeferred {
            guard try await deferredLeases.acquire(1, deadline: deadline) else {
                throw IggyError(.transientNotAccepted, context: "\(Self.maxDeferredRequests) deferred requests were in flight until the deadline")
            }
        }
        defer {
            if body.isDeferred {
                deferredLeases.release(1)
            }
        }
        let clock = ContinuousClock()
        let now = clock.now
        let clustered = try await isClustered(coordinator, lineage: lineage, deadline: deadline)
        if clustered, heartbeatDue(at: now, interval: coordinator.configuration.heartbeatInterval) {
            // The requests leave the coordinator connection idle, so it is
            // pinged here once per heartbeat interval too.
            _ = try await control(.ping, [], lineage: lineage, coordinator: coordinator, deadline: deadline)
        }
        let request = Request(code: code, body: body, key: key, lineage: lineage, deadline: deadline)
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
                return try await sendOnCoordinator(request, context: &captured, replay: &replay, coordinator: coordinator)
            } catch let error as IggyError {
                if body.fetchAttempt?.mayHaveCommitted == true {
                    throw error
                }
                let reconnects =
                    replay.outcome.isUncertain && error.isConnectionLoss && error.code != .staleClient
                    && coordinator.configuration.reconnection.enabled
                guard error.code == .transientNotAccepted || error.code == .deferredPollCapacityExceeded || reconnects else {
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

    /// Sends a non-replicated request to `replica` and returns its answer.
    /// The request carries the session's metadata watermark. On a single node
    /// it goes over the coordinator, in a cluster over a data connection to
    /// the replica, bound to the client's session. A replica the roster does
    /// not list after one more roster read fails with `invalidIdentifier`. A
    /// lost connection or a `transientNotAccepted` refusal sends the request
    /// to the same replica again until `deadline`, which ends the call with
    /// the last refusal. Any other answer, `notPrimary` included, is the
    /// caller's, as the caller chose the replica.
    func sendToReplica(
        _ replica: UInt8, code: UInt32, payload: [UInt8], lineage: UInt64, coordinator: ClientCore, deadline: ContinuousClock.Instant
    ) async throws -> [UInt8] {
        _ = try await isClustered(coordinator, lineage: lineage, deadline: deadline)
        if state.withLockedValue({ $0.nodes[replica] }) == nil {
            try await readRoster(coordinator, lineage: lineage, deadline: deadline)
        }
        let (listed, clustered) = state.withLockedValue { ($0.nodes[replica] != nil, $0.nodes.count > 1) }
        guard listed else {
            throw IggyError(.invalidIdentifier, context: "the cluster roster does not list replica \(replica)")
        }
        guard clustered else {
            return try await coordinator.exchange(code: code, payload: payload, deadline: deadline, lineage: lineage)
        }
        let clock = ContinuousClock()
        var backoff = Backoff()
        var refusal = IggyError(.transientNotAccepted, context: "the budget ran out before command \(code) reached replica \(replica)")
        while clock.now < deadline {
            let watermark = await coordinator.metadataWatermark
            guard let identity = await coordinator.sessionIdentity(at: watermark) else {
                throw IggyError(.unauthenticated, context: "no session is bound to send command \(code) to replica \(replica)")
            }
            let fence = state.withLockedValue { $0.fence }
            do {
                return try await exchange(
                    onReplica: replica, code: code, payload: payload, context: ClientCore.readContext(watermark), identity: identity,
                    lineage: lineage, fence: fence, coordinator: coordinator, deadline: deadline,
                    dialDeadline: min(deadline, .now + ClientCore.failoverDialTimeout))
            } catch let unsent as UnsentRequest {
                guard unsent.refusal.code == .transientNotAccepted else {
                    throw unsent.refusal
                }
                refusal = unsent.refusal
            } catch let error as IggyError where error.code == .transientNotAccepted || error.code == .transientNotCommitted {
                refusal = error
            }
            try await backoff.pause(after: refusal, before: deadline)
        }
        throw refusal
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
            lengthen()
        }

        /// Doubles the next pause without waiting, for a caller that hands a
        /// copy to an attempt that runs beside others.
        mutating func lengthen() {
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
        let body: Body
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
        // A cancelled exchange aborts the coordinator connection, so a caller
        // cancelled meanwhile lets the read or the ping finish instead.
        try await Task {
            do {
                return try await coordinator.exchangeOnce(code: code.rawValue, payload: payload, context: context, deadline: deadline, lineage: lineage)
            } catch let error as IggyError where error.isReconnectable && error.code != .staleClient && ContinuousClock.now < deadline {
                _ = try await coordinator.exchange(code: CommandCode.ping.rawValue, payload: [], deadline: deadline, lineage: lineage)
                return try await coordinator.exchangeOnce(code: code.rawValue, payload: payload, context: context, deadline: deadline, lineage: lineage)
            }
        }.value
    }

    /// The single-node path: the request goes over the coordinator connection
    /// with the context of its route, a deferred one over a data connection to
    /// the same node.
    private func sendOnCoordinator(
        _ request: Request, context: inout PartitionContext?, replay: inout Replay, coordinator: ClientCore
    ) async throws -> [UInt8] {
        let route = try await route(request.key, clustered: false, lineage: request.lineage, coordinator: coordinator, deadline: request.deadline)
        let captured = context ?? route.context
        context = captured
        do {
            if request.body.isDeferred {
                return try await exchange(
                    at: await coordinator.serverAddress, code: request.code.rawValue, body: request.body, context: captured,
                    identity: route.consumerSession, lineage: request.lineage, fence: route.fence, coordinator: coordinator, deadline: request.deadline,
                    dialDeadline: request.deadline, replay: &replay)
            }
            let payload = try request.body.payload()
            if request.code == .pollMessages {
                return try await coordinator.exchangeOnce(
                    code: request.code.rawValue, payload: payload, context: captured, deadline: request.deadline, lineage: request.lineage)
            }
            return try await coordinator.exchange(
                code: request.code.rawValue, payload: payload, context: captured, deadline: request.deadline, lineage: request.lineage)
        } catch let unsent as UnsentRequest {
            removeRoute(request.key)
            throw unsent.refusal
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
                onReplica: route.primaryReplica, code: request.code.rawValue, body: request.body, context: captured,
                identity: route.consumerSession, lineage: request.lineage, fence: route.fence, coordinator: coordinator, deadline: request.deadline,
                dialDeadline: request.deadline, replay: &replay)
        } catch let unsent as UnsentRequest {
            removeRoute(request.key)
            throw unsent.refusal
        } catch let error as IggyError {
            if error.code == .notPrimary {
                // A fetch names several partitions and the refusal
                // does not name the one that moved, so its caller looks up
                // every route again.
                guard request.code != .fetchMessages else {
                    throw error
                }
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
            onReplica: replica, code: code, body: .fixed(payload), context: context, identity: identity, lineage: lineage, fence: fence,
            coordinator: coordinator, deadline: deadline, dialDeadline: dialDeadline, replay: &replay)
    }

    private func exchange(
        onReplica replica: UInt8, code: UInt32, body: Body, context: PartitionContext, identity: Responses.SessionIdentity, lineage: UInt64,
        fence: UInt128Value, coordinator: ClientCore, deadline: ContinuousClock.Instant, dialDeadline: ContinuousClock.Instant, replay: inout Replay
    ) async throws -> [UInt8] {
        let endpoint = try await endpoint(of: replica, lineage: lineage, coordinator: coordinator, deadline: deadline)
        return try await exchange(
            at: endpoint, code: code, body: body, context: context, identity: identity, lineage: lineage, fence: fence, coordinator: coordinator,
            deadline: deadline, dialDeadline: dialDeadline, replay: &replay)
    }

    private func exchange(
        at endpoint: String, code: UInt32, body: Body, context: PartitionContext, identity: Responses.SessionIdentity, lineage: UInt64,
        fence: UInt128Value, coordinator: ClientCore, deadline: ContinuousClock.Instant, dialDeadline: ContinuousClock.Instant, replay: inout Replay
    ) async throws -> [UInt8] {
        let slot = try slot(fence: fence, at: endpoint, deferred: body.isDeferred)
        if !body.isDeferred {
            try await slot.lock.lock()
        }
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
                throw IggyError(.transientNotAccepted, context: "cannot reach \(endpoint): \(error)")
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
            let reply = try await exchange(
                on: data.connection, code: code, body: body, context: context, identity: identity, lineage: lineage, coordinator: coordinator,
                deadline: deadline, replay: &replay)
            data.usable = true
            slot.connection.withLockedValue { $0 = data }
            return reply
        } catch let unsent as UnsentRequest {
            data.usable = true
            slot.connection.withLockedValue { $0 = data }
            throw unsent
        } catch let cancelled as CancellationError where body.isDeferred {
            // The cancellation followed the request, and the connection drops
            // a reply that still arrives.
            data.usable = true
            slot.connection.withLockedValue { $0 = data }
            throw cancelled
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
            if body.fetchAttempt?.mayHaveCommitted == true {
                throw error
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
            on: connection, code: CommandCode.bindSession.rawValue, body: .fixed(payload), context: .empty, identity: identity, lineage: lineage,
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
    /// A deferred request waits until `deadline`, which its own request
    /// timeout set.
    private func exchange(
        on connection: any IggyConnection, code: UInt32, body: Body, context: PartitionContext, identity: Responses.SessionIdentity,
        lineage: UInt64, coordinator: ClientCore, deadline: ContinuousClock.Instant, replay: inout Replay
    ) async throws -> [UInt8] {
        let replayDeadline = body.isDeferred ? deadline : min(deadline, .now + coordinator.configuration.requestTimeout)
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
                request = try await coordinator.stampRequest(code: code, payload: try body.payload(), context: context, for: identity, lineage: lineage)
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
                body.fetchAttempt?.sent()
                raw = try await connection.exchange(request.bytes, timeout: replayDeadline - .now, cancellation: request.deferredCancellation)
            } catch let lost as IggyError {
                if lost.code == .notConnected || lost.code == .cannotEstablishConnection {
                    body.fetchAttempt?.refused()
                }
                guard replay.outcome.isUncertain else {
                    throw lost
                }
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
                if VSRFrame.peekCommand(header) == .reply, VSRFrame.readReplyStatus(header) == refusal.rawCode, Self.fetchRefused(refusal.code) {
                    body.fetchAttempt?.refused()
                }
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

    /// The slot of the data connection to `endpoint`. A deferred request gets
    /// a free one of its own, already locked for it. A session replaced
    /// since `fence` throws ``UnsentRequest``, since nothing was written.
    private func slot(fence: UInt128Value, at endpoint: String, deferred: Bool) throws -> Slot {
        try state.withLockedValue { state in
            do {
                try Self.checkFence(fence, in: state)
            } catch let refusal as IggyError {
                throw UnsentRequest(refusal: refusal)
            }
            if deferred {
                if let free = state.deferredSlots[endpoint]?.first(where: { $0.lock.tryLock() }) {
                    return free
                }
                // The leases keep at most `maxDeferredRequests` in flight, so
                // full pools hold an idle slot at another endpoint. Dropping
                // it closes its connection, which keeps the open connections
                // within the same bound.
                if state.deferredSlots.values.reduce(0, { $0 + $1.count }) >= Self.maxDeferredRequests {
                    Self.dropIdleDeferredSlot(in: &state)
                }
                let slot = Slot()
                _ = slot.lock.tryLock()
                state.deferredSlots[endpoint, default: []].append(slot)
                return slot
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

    private static func dropIdleDeferredSlot(in state: inout State) {
        for (endpoint, pool) in state.deferredSlots {
            if let idle = pool.firstIndex(where: { $0.lock.tryLock() }) {
                state.deferredSlots[endpoint]?.remove(at: idle)
                return
            }
        }
    }

    private func removeRoute(_ key: RouteKey) {
        _ = state.withLockedValue { $0.routes.removeValue(forKey: key) }
    }

    private func markNotPrimary(_ route: Route, partitionID: UInt32) {
        state.withLockedValue { state in
            for key in Self.aliases(of: route, partitionID: partitionID, in: state) {
                let floor = max(state.routes[key]?.minimumView ?? 0, route.view == .max ? .max : route.view + 1)
                state.routes[key]?.minimumView = floor
                state.routes[key]?.askFirst = route.primaryReplica
            }
        }
    }

    /// Drops the routes of `keys`, whose contexts a fetch found out of date,
    /// so the next request looks each of them up again.
    func dropRoutes(_ keys: [RouteKey]) {
        keys.forEach(removeRoute)
    }

    /// Marks the routes of `keys` stale after a primary answered a wait or a
    /// fetch that it is not the primary of one of them, as `may_have_moved`
    /// in the Rust SDK. A refusal of a whole request does not name that
    /// partition, so every view floor stays, and each next lookup asks the
    /// primary of its route first.
    func mayHaveMoved(_ keys: [RouteKey]) {
        state.withLockedValue { state in
            for key in keys {
                guard let route = state.routes[key] else {
                    continue
                }
                for alias in Self.aliases(of: route, partitionID: key.partitionID, in: state) {
                    state.routes[alias]?.askFirst = route.primaryReplica
                }
            }
        }
    }

    /// Every spelling of the partition history that `route` belongs to.
    private static func aliases(of route: Route, partitionID: UInt32, in state: State) -> [RouteKey] {
        state.routes.keys.filter { $0.partitionID == partitionID && state.routes[$0]?.context.incarnation == route.context.incarnation }
    }

    /// `partitions` grouped by the replica that is their primary, in replica
    /// order, as `readiness_groups` in the Rust SDK: each group keeps the
    /// order of `partitions` and is split into groups of at most the
    /// partitions one request names. On a single node they form one group.
    func primaryGroups(
        stream: Identifier, topic: Identifier, partitions: [UInt32], lineage: UInt64, coordinator: ClientCore, deadline: ContinuousClock.Instant
    ) async throws -> [[UInt32]] {
        let groups = try await retryingLookups(lineage: lineage, coordinator: coordinator, deadline: deadline) { clustered in
            var groups: [UInt8: [UInt32]] = [:]
            for partition in partitions {
                let key = RouteKey(stream: stream, topic: topic, partitionID: partition)
                let route = try await route(key, clustered: clustered, lineage: lineage, coordinator: coordinator, deadline: deadline)
                groups[route.primaryReplica, default: []].append(partition)
            }
            return groups
        }
        return groups.sorted { $0.key < $1.key }.flatMap { group in
            stride(from: 0, to: group.value.count, by: Requests.maxPartitionsPerRequest).map {
                Array(group.value[$0..<min($0 + Requests.maxPartitionsPerRequest, group.value.count)])
            }
        }
    }

    /// The fence of each fetch target: the context it carries, where one
    /// without an incarnation takes the incarnation of its route, as
    /// `fence_targets` in the Rust SDK fills it.
    func fences(
        _ targets: [(key: RouteKey, context: PartitionContext?)], lineage: UInt64, coordinator: ClientCore, deadline: ContinuousClock.Instant
    ) async throws -> [PartitionContext] {
        try await retryingLookups(lineage: lineage, coordinator: coordinator, deadline: deadline) { clustered in
            var fences: [PartitionContext] = []
            fences.reserveCapacity(targets.count)
            for target in targets {
                let fence = target.context ?? .empty
                guard fence.incarnation == 0 else {
                    fences.append(fence)
                    continue
                }
                let route = try await route(target.key, clustered: clustered, lineage: lineage, coordinator: coordinator, deadline: deadline)
                guard route.context.incarnation != 0 else {
                    throw IggyError(.transientNotAccepted, context: "the route has no partition incarnation")
                }
                fences.append(PartitionContext(incarnation: route.context.incarnation, ownerGeneration: fence.ownerGeneration, metadataOp: fence.metadataOp))
            }
            return fences
        }
    }

    /// Runs route lookups, one by one, as the lockstep connections they take
    /// would queue them anyway. A lookup refused, answered "not primary" or
    /// lost with its connection runs them again until `deadline`, as
    /// `retries_lookup` in the Rust SDK allows.
    private func retryingLookups<Value>(
        lineage: UInt64, coordinator: ClientCore, deadline: ContinuousClock.Instant, _ lookUps: (_ clustered: Bool) async throws -> Value
    ) async throws -> Value {
        let clustered = try await isClustered(coordinator, lineage: lineage, deadline: deadline)
        var backoff = Backoff()
        while true {
            do {
                return try await lookUps(clustered)
            } catch let error as IggyError
                where [.transientNotAccepted, .notPrimary, .disconnected, .emptyResponse, .notConnected].contains(error.code)
            {
                try await backoff.pause(after: IggyError(.transientNotAccepted, context: "\(error)"), before: deadline)
            }
        }
    }
}
