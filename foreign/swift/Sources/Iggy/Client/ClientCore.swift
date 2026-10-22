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
import Logging

/// The connection state machine behind ``IggyClient``: one lockstep
/// connection, its consensus session, the credentials to restore it with,
/// and the cluster roster to fail over to. Modelled on the Rust `TcpClient`
/// and `leader_aware`.
///
/// A reconnect binds the new connection to the session the lost one carried,
/// so a replicated write whose outcome is unknown goes again under its own
/// request id. When the server no longer holds the session, the client signs
/// in under a new one, and such a write ends with
/// ``IggyErrorCode/transientNotCommitted``.
///
/// One deliberate difference: the client reconnects lazily. Once a connection
/// is lost, or the server attaches it to no session any more, the client
/// reports itself disconnected, and the next request or heartbeat dials
/// again, then binds the session or signs in. The Rust SDK does that right
/// away. Either way no request goes out on the ended session, and every later
/// request waits for the new one.
actor ClientCore {
    /// Backoff before replaying a request the server answered with an explicit
    /// transient frame. The reply arrives promptly, so a short pause keeps the
    /// replay from spinning while the primary catches up.
    static let notReadyRetryInterval: Duration = .milliseconds(50)
    /// How long a request replays "not accepted" on the same connection before
    /// the client re-checks cluster leadership.
    static let transientFailoverCheckInterval: Duration = .seconds(2)
    /// Bound on one dial while other endpoints are queued behind it.
    static let failoverDialTimeout: Duration = .seconds(2)
    /// Bound on the roster read that follows a manual sign-in.
    static let rosterReadTimeout: Duration = .seconds(5)
    /// How long to wait for a transiently leaderless cluster to elect.
    static let leaderlessWaitBudget: Duration = .seconds(5)
    static let leaderlessPollInterval: Duration = .milliseconds(250)
    static let maxLeaderRedirects = 3

    let configuration: ClientConfiguration
    let connector: any IggyConnector
    let logger: Logger

    /// Shutdown is final: a teardown or a dial that finishes afterwards must
    /// not make the client usable again, so no later write replaces it.
    private(set) var state: ClientState = .disconnected {
        didSet {
            if oldValue == .shutdown {
                state = .shutdown
            }
        }
    }
    private var connection: (any IggyConnection)?
    /// Counts the connections installed so far, so a request that failed on
    /// an old one does not tear down the one that replaced it.
    private var connectionEpoch: UInt64 = 0
    /// The address the connection was dialed at. A redirect can name the
    /// next address in `currentAddress` while a connection still stands.
    private var connectionAddress = ""
    private var session: ConsensusSession
    private var currentAddress: String
    private var rosterEndpoints: [String] = []
    private var rosterLearned = false
    private var rememberedSignIn: (credentials: Credentials, userID: UInt32)?
    /// Withdraws configured auto-login after a logout or failed sign-in.
    /// A successful explicit sign-in supplies remembered credentials instead.
    private var signInWithdrawn = false
    /// A failed caller sign-in cannot be undone by disconnect and connect.
    private var explicitSignInRequired = false
    /// The password a committed change gave the configured auto-login user;
    /// the configuration cannot be rewritten and the old password is dead.
    private var configuredPassword: String?
    /// The name a committed rename gave the configured auto-login user.
    private var configuredUsername: String?
    private var connectedAt: ContinuousClock.Instant?
    private var redirects = 0
    /// Set by an explicit ``disconnect()`` and cleared by the caller's next
    /// ``connectByCaller()``: in between, nothing may bring the client back.
    private var disconnectedByCaller = false
    /// Raised by every sign-in and sign-out of the caller: a login, a logout
    /// or a disconnect. A call keeps the value it started under and ends once
    /// it changes, so no frame of it is written under a session the caller
    /// opened after it started.
    private(set) var lineage: UInt64 = 0
    private var eventSubscribers: [UUID: AsyncStream<DiagnosticEvent>.Continuation] = [:]
    private let exchangeLock = AsyncMutex()
    /// A register reply can bind the server connection before its caller
    /// resumes, so a second sign-in must wait for the first to settle it.
    private let signInLock = AsyncMutex()
    private let routingLock = AsyncMutex()
    private let connectGate = SingleFlight()
    private var heartbeatTask: Task<Void, Never>?
    /// The highest metadata commit a reply reported. A route read before it
    /// may predate a change the client already saw, so it is read again.
    /// A session reset keeps it.
    private(set) var metadataWatermark: UInt64 = 0
    /// Partitioning and consumer-group state that the typed calls share.
    nonisolated let groupState = ConsumerGroupState()
    nonisolated let pollRouter: PollRouter

    init(configuration: ClientConfiguration, connector: any IggyConnector, logger: Logger) {
        self.configuration = configuration
        self.connector = connector
        self.logger = logger
        currentAddress = configuration.address
        let session = ConsensusSession()
        self.session = session
        pollRouter = PollRouter(fence: session.clientID)
    }

    // MARK: State and events

    var clientAddress: String? { connection?.localAddress }
    var serverAddress: String { currentAddress }

    func subscribeEvents() -> AsyncStream<DiagnosticEvent> {
        let id = UUID()
        let (stream, continuation) = AsyncStream<DiagnosticEvent>.makeStream(bufferingPolicy: .bufferingNewest(256))
        guard state != .shutdown else {
            // Nothing is published after shutdown, so the stream ends here
            // instead of leaving its reader waiting for a first event.
            continuation.finish()
            return stream
        }
        eventSubscribers[id] = continuation
        continuation.onTermination = { [weak self] _ in
            guard let self else { return }
            Task { await self.removeSubscriber(id) }
        }
        return stream
    }

    private func removeSubscriber(_ id: UUID) {
        eventSubscribers[id] = nil
    }

    private func publish(_ event: DiagnosticEvent) {
        for continuation in eventSubscribers.values {
            continuation.yield(event)
        }
    }

    // MARK: Exchange

    /// Sends a request and returns its reply body, reconnecting and replaying
    /// when the failure is one a fresh connection recovers from. One deadline,
    /// `requestTimeout` from now, covers the request and every reconnect,
    /// sign-in, leader check, and replay.
    ///
    /// A replicated write that may have reached the log before the connection
    /// was lost, or before an eviction ended its session, is replayed only
    /// under the same session. Otherwise it fails with
    /// ``IggyErrorCode/transientNotCommitted``, as in the Rust SDK, so it is
    /// never mistaken for a request that was not sent.
    ///
    /// A cancelled caller gets `CancellationError`. A request it already wrote
    /// takes the connection with it, since its reply can no longer be told
    /// from the next one, and the next caller reconnects.
    ///
    /// A request of a call made before the caller signed in or out ends with
    /// ``IggyErrorCode/unauthenticated``, or with
    /// ``IggyErrorCode/transientNotCommitted`` once it may have committed.
    ///
    /// `context` is the partition context a partition request carries; every
    /// replay keeps it. A read without one carries the metadata watermark.
    /// `limit` caps the request timeout for a caller with a budget of its
    /// own. `lineage` is the one the call started under; nil takes the
    /// current one, for a call of this one request.
    func exchange(
        code: UInt32, payload: [UInt8], context: PartitionContext = .empty, deadline limit: ContinuousClock.Instant? = nil,
        lineage captured: UInt64? = nil
    ) async throws -> [UInt8] {
        if state == .shutdown { throw IggyError(.clientShutdown) }
        let lineage = captured ?? lineage
        if let refusal = lineageRefusal(lineage, code: code) { throw refusal }
        if disconnectedByCaller { throw IggyError(.notConnected) }
        let deadline = requestDeadline(within: limit)
        var outcome = RetryOutcome()
        var backoff = PollRouter.Backoff()
        let reads = context == .empty && ConsensusOperation.forCode(code) == .nonReplicated
        let context = reads ? Self.readContext(metadataWatermark) : context
        var failedEpoch = connectionEpoch
        var request: VSRFrame.EncodedRequest?
        // As in the Rust SDK, every lost connection is reconnected until the
        // deadline, so a write whose outcome is unknown keeps going under its
        // own session and request id.
        while true {
            do {
                return try await sendRaw(
                    code: code, payload: payload, context: context, lineage: lineage, deadline: deadline, request: &request, epoch: &failedEpoch,
                    outcome: &outcome)
            } catch let error as IggyError {
                // A cancelled caller cannot use a new connection: every exchange
                // it starts is abandoned at once.
                try Task.checkCancellation()
                // A reply refused the request as a stale client before admission:
                // as in the Rust SDK the call ends with the refusal, read or
                // write, and the next request binds the session again or signs in.
                guard error.isReconnectable, error.code != .staleClient else {
                    throw error
                }
                // The server deduplicates by (client, request). A replay under a
                // new session gets a new identity, so it is safe only for what
                // provably never reached the log.
                let replay = replayAfterSessionResetIsSafe(code: code, error: error, outcome: outcome)
                let failure = replay ? error : IggyError(.transientNotCommitted, context: "command \(code) may have committed before: \(error)")
                if let refusal = lineageRefusal(lineage, code: code) {
                    throw replay ? refusal : failure
                }
                if code == CommandCode.getClusterMetadata.rawValue {
                    // An unauthenticated roster read cannot be repaired by a
                    // reconnect; it would re-issue the same read forever.
                    throw error
                }
                guard configuration.reconnection.enabled else {
                    throw replay ? IggyError(.disconnected, context: "automatic reconnection is disabled") : failure
                }
                let isLogin = CommandCode.isRegister(code)
                if !isLogin, signInCredentials() == nil {
                    // With no credentials a reconnect cannot restore the session, so
                    // anything but a sign-in fails instead of replaying
                    // unauthenticated. The sign-in itself is the exception: the
                    // server stays silent on a transient register failure and
                    // relies on the client replaying it.
                    throw failure
                }
                if isLogin, connectGate.isActive {
                    // The sign-in ran inside a connect sweep, which owns the
                    // endpoint walk; the sweep reports the failure itself.
                    throw error
                }
                guard ContinuousClock.now < deadline else {
                    // Nothing is left of the deadline to reconnect in; the next
                    // request does it. As after a reconnect that outlasts the
                    // deadline, a request that can go again is not accepted.
                    throw replay ? IggyError(.transientNotAccepted, context: "the request deadline passed after: \(error)") : failure
                }
                if !replay {
                    // The request was written and lost, as the Rust SDK marks a
                    // call uncertain once a sent attempt lost its link: no later
                    // answer proves it never committed, so a refused replay is not
                    // stamped afresh.
                    outcome.markUncertain()
                }
                if ConsensusOperation.forCode(code) == .nonReplicated {
                    try await backoff.pause(after: failure, before: deadline)
                }
                let epoch = failedEpoch
                let reconnected: Void?
                do {
                    reconnected = try await withDeadline(deadline) { [self] in
                        // The retried sign-in is the sign-in: the reconnect must not
                        // run another one ahead of it.
                        try await self.reconnect(failedEpoch: epoch, skipAutoLogin: isLogin, cause: error)
                    }
                } catch let reconnectError as IggyError {
                    if !replay {
                        throw IggyError(.transientNotCommitted, context: "command \(code) may have committed before the reconnect failed: \(reconnectError)")
                    }
                    if let refusal = lineageRefusal(lineage, code: code) {
                        throw refusal
                    }
                    if reconnectError.isCredentialRejection {
                        throw Self.signInRefused(reconnectError)
                    }
                    throw outcome.observe(reconnectError)
                }
                if let refusal = lineageRefusal(lineage, code: code) {
                    throw replay ? refusal : failure
                }
                guard reconnected != nil else {
                    // A request that was not sent is refused as not accepted: the
                    // reconnect may still finish, so a retry can succeed.
                    throw IggyError(
                        replay ? .transientNotAccepted : failure.code, context: "the request deadline passed while reconnecting after: \(error)")
                }
                if let retained = request, retained.clientID == session.clientID,
                    retained.session == (session.session ?? 0) || retained.operation == .register
                {
                    // The session was resumed: the request goes again as it was
                    // stamped, and the server answers it from its reply cache if
                    // it committed.
                } else if replay {
                    request = nil
                } else {
                    logger.warning("Reconnected under a new session, but the outcome of command \(code) is unknown: replaying it could apply it twice")
                    throw failure
                }
            }
        }
    }

    /// One attempt on the current connection with no reconnect, failover, or
    /// replay, for the route lookups and the polls that `PollRouter` retries
    /// itself, bounded by `limit` when the caller has a budget. `notPrimary`
    /// is graded as on the retried path.
    func exchangeOnce(
        code: UInt32, payload: [UInt8], context: PartitionContext, deadline limit: ContinuousClock.Instant? = nil, lineage captured: UInt64? = nil
    ) async throws -> [UInt8] {
        if state == .shutdown { throw IggyError(.clientShutdown) }
        if let refusal = lineageRefusal(captured ?? lineage, code: code) { throw refusal }
        if disconnectedByCaller { throw IggyError(.notConnected) }
        var request: VSRFrame.EncodedRequest?
        var epoch = connectionEpoch
        var outcome = RetryOutcome()
        let now = ContinuousClock.now
        let attempted = try await attempt(
            code: code, payload: payload, context: context, lineage: captured ?? lineage, request: &request, epoch: &epoch, outcome: &outcome,
            transientDeadline: now, readDeadline: requestDeadline(within: limit), replaysUncommitted: false)
        switch attempted {
        case .success(let body):
            return body
        case .failure(let error):
            dropConnection(after: error, epoch: epoch)
            throw Self.graded(error)
        }
    }

    private func requestDeadline(within limit: ContinuousClock.Instant?) -> ContinuousClock.Instant {
        let full = ContinuousClock.now + configuration.requestTimeout
        return limit.map { min($0, full) } ?? full
    }

    var bindSecret: [UInt8] { session.bindSecret }

    /// The connection that stands now, by the epoch it was installed at and
    /// the address it was dialed at; nil while none stands. Requests made
    /// between two reads that agree all ran on that connection.
    var standingConnection: (epoch: UInt64, address: String)? {
        connection == nil ? nil : (connectionEpoch, connectionAddress)
    }

    /// The bound session as a data connection binds to it, at the metadata
    /// watermark `metadataOp`. Nil while no session is bound.
    func sessionIdentity(at metadataOp: UInt64) -> Responses.SessionIdentity? {
        session.session.map { Responses.SessionIdentity(clientID: session.clientID, session: $0, metadataWatermark: metadataOp) }
    }

    /// Stamps a frame for a data connection from the shared session, for the
    /// session `identity` names. Replicated requests take the next id of the
    /// session's counter; reads carry its current value. A session that was
    /// replaced or is not bound, or a route older than the newest metadata
    /// commit, is refused as not accepted, since nothing was written. A call
    /// that started under an older `lineage` is refused as unauthenticated.
    func stampRequest(
        code: UInt32, payload: [UInt8], context: PartitionContext, for identity: Responses.SessionIdentity, lineage captured: UInt64
    ) throws -> VSRFrame.EncodedRequest {
        guard session.clientID == identity.clientID, session.session == identity.session else {
            throw IggyError(.transientNotAccepted, context: "the route belongs to a replaced session")
        }
        guard identity.metadataWatermark >= metadataWatermark else {
            throw IggyError(.transientNotAccepted, context: "the route predates metadata commit \(metadataWatermark)")
        }
        if let refusal = lineageRefusal(captured, code: code) {
            throw refusal
        }
        return try stamp(code: code, payload: payload, context: context)
    }

    /// The context of a read without a partition context, as `read_context`
    /// in the Rust SDK: a replica answers it only once it has applied the
    /// metadata commits up to `watermark`, which the client already saw.
    static func readContext(_ watermark: UInt64) -> PartitionContext {
        PartitionContext(incarnation: 0, ownerGeneration: 0, metadataOp: watermark)
    }

    private func stamp(code: UInt32, payload: [UInt8], context: PartitionContext) throws -> VSRFrame.EncodedRequest {
        try VSRFrame.encodeRequest(session: &session, code: code, payload: payload, partitionContext: context)
    }

    /// The refusal of a call that started under `captured` once the caller
    /// signed in or out since: nothing of it may be written any more.
    func lineageRefusal(_ captured: UInt64, code: UInt32? = nil) -> IggyError? {
        guard captured != lineage else {
            return nil
        }
        let context = code.map { "after command \($0) was issued" } ?? "while the call was waiting"
        return IggyError(.unauthenticated, context: "the caller signed in or out \(context)")
    }

    /// Dials `address` with the client's TLS, for a data connection. A host
    /// that drops the dial without a reset holds it until the timeout, so
    /// the dial gets no longer than is left before `deadline`, which the
    /// call that needs the connection sets. Nil when nothing is left.
    func dial(_ address: String, deadline: ContinuousClock.Instant) async throws -> (any IggyConnection)? {
        let timeout = min(configuration.connectTimeout, deadline - .now)
        guard timeout > .zero else {
            return nil
        }
        return try await connector.connect(
            to: try Endpoint(parsing: address), tls: configuration.tls, timeout: timeout, noDelay: configuration.noDelay)
    }

    /// Fails fast with the error the Rust SDK raises for the state, after
    /// bringing back a connection that dropped on its own. A reply timeout, a
    /// cancelled request, or an outage longer than the request timeout leaves
    /// the client disconnected, and with credentials to sign in again the
    /// next typed call reconnects instead of failing until ``connect()``. A
    /// reconnect that outlasts the request timeout refuses the call as not
    /// accepted, since nothing was sent, and one whose sign-in the server
    /// refused ends it as unauthenticated.
    ///
    /// Returns the lineage the call started under, read before the reconnect
    /// can wait for a sign-in of the caller, so that no request of the call
    /// is written under the session that sign-in opens.
    func requireAuthenticated() async throws -> UInt64 {
        let started = lineage
        switch state {
        case .disconnected, .connecting, .authenticating:
            if !disconnectedByCaller, configuration.reconnection.enabled, signInCredentials() != nil {
                let reconnected: Void?
                do {
                    reconnected = try await withDeadline(ContinuousClock.now + configuration.requestTimeout) { [self] in
                        try await self.connect()
                    }
                } catch {
                    if let refusal = lineageRefusal(started) { throw refusal }
                    if let refusal = error as? IggyError, refusal.isCredentialRejection {
                        throw Self.signInRefused(refusal)
                    }
                    throw error
                }
                if let refusal = lineageRefusal(started) { throw refusal }
                if reconnected == nil {
                    throw IggyError(.transientNotAccepted, context: "could not reconnect within the request timeout")
                }
            }
        default:
            break
        }
        switch state {
        case .shutdown:
            throw IggyError(.clientShutdown)
        case .disconnected, .connecting, .authenticating:
            throw IggyError(disconnectedByCaller ? .notConnected : .disconnected)
        case .connected:
            throw IggyError(.unauthenticated)
        case .authenticated:
            return started
        }
    }

    /// Replaces the connection a request failed on. Serialized, so callers
    /// that failed together reconnect once.
    private func reconnect(failedEpoch: UInt64, skipAutoLogin: Bool, cause: IggyError) async throws {
        try await routingLock.lock()
        defer { routingLock.unlock() }
        // Another caller may have reconnected while this one waited for the
        // lock; its connection is not the one that failed.
        if failedEpoch == connectionEpoch {
            await disconnectTransport()
        }
        logger.info("Reconnecting to the server", metadata: ["server_address": "\(currentAddress)", "error": "\(cause)"])
        try await connect(skipAutoLogin: skipAutoLogin)
    }

    /// The end of a call that waited for a reconnect whose sign-in the server
    /// refused: no session comes back until the caller signs in, so the call
    /// is unauthenticated, with the refusal as its cause.
    private static func signInRefused(_ refusal: IggyError) -> IggyError {
        IggyError(.unauthenticated, context: "the automatic sign-in was refused: \(refusal)")
    }

    /// Whether replaying `code` after reconnecting with a new session cannot
    /// apply it twice. A write can be replayed only while no earlier attempt
    /// may have committed and `error` proves that this one was not written.
    private func replayAfterSessionResetIsSafe(code: UInt32, error: IggyError, outcome: RetryOutcome) -> Bool {
        if CommandCode.isRegister(code) {
            return true
        }
        let operation = ConsensusOperation.forCode(code)
        if operation == .nonReplicated || operation == .logout {
            return true
        }
        guard !outcome.isUncertain else {
            return false
        }
        switch error.code {
        case .notConnected, .cannotEstablishConnection, .unauthenticated:
            return true
        default:
            return false
        }
    }

    /// One request against the current connection, bounded by `deadline`
    /// across every same-connection replay and every leader failover. `epoch`
    /// reports the connection the last attempt ran on.
    private func sendRaw(
        code: UInt32, payload: [UInt8], context: PartitionContext, lineage: UInt64, deadline: ContinuousClock.Instant,
        request: inout VSRFrame.EncodedRequest?, epoch: inout UInt64, outcome: inout RetryOutcome
    ) async throws -> [UInt8] {
        switch state {
        case .shutdown:
            throw IggyError(.clientShutdown)
        case .disconnected, .connecting:
            throw IggyError(.notConnected)
        default:
            break
        }
        let clock = ContinuousClock()
        let isLogin = CommandCode.isRegister(code)
        var walk = RosterWalk()
        var refused: IggyError?
        while true {
            if let refused, deadline - clock.now < Self.notReadyRetryInterval {
                // An attempt sent with a sliver of the budget would time out,
                // and the timeout takes the connection with it.
                throw refused
            }
            let transientDeadline = isLogin ? deadline : min(deadline, clock.now + Self.transientFailoverCheckInterval)
            let attempted = try await attempt(
                code: code, payload: payload, context: context, lineage: lineage, request: &request, epoch: &epoch, outcome: &outcome,
                transientDeadline: transientDeadline,
                readDeadline: deadline, replaysUncommitted: true)
            switch attempted {
            case .success(let body):
                refreshCredentials(afterCommitting: code, payload: payload)
                return body
            case .failure(let failure):
                let error = outcome.observe(Self.graded(failure))
                guard error.code == .transientNotAccepted || failure.code == .notPrimary, !isLogin, clock.now < deadline else {
                    dropConnection(after: failure, epoch: epoch)
                    // A lost connection goes back as it is, so the caller
                    // reconnects even once the outcome is unknown.
                    throw failure.isConnectionLoss ? failure : error
                }
                if code == CommandCode.getClusterMetadata.rawValue {
                    throw error
                }
                refused = error
                let from = walk
                guard let step = try await withDeadline(deadline, { [self] in try await self.failOver(from) }) else {
                    throw error
                }
                walk = step.walk
                guard step.moved || rosterEndpoints.count <= 1 else {
                    throw error
                }
                if outcome.isUncertain {
                    guard let retained = request, retained.clientID == session.clientID, retained.session == session.session else {
                        throw error
                    }
                } else {
                    // A queued caller may have spent a later id during failover.
                    // Only a request proved unaccepted can be stamped afresh.
                    request = nil
                }
                if !step.moved {
                    // A single node: partition materialisation can outlast the
                    // short retry window, so keep replaying until the budget ends.
                    try? await Task.sleep(until: min(clock.now + Self.notReadyRetryInterval, deadline), clock: clock)
                }
            }
        }
    }

    /// The reply stream of a connection that failed mid-request is at an
    /// unknown boundary; a late reply would desync framing for the next
    /// request. One found closed before the request was written is dropped
    /// too, so the next request dials.
    private func dropConnection(after error: IggyError, epoch: UInt64) {
        if error.code == .disconnected || error.code == .notConnected {
            dropConnection(epoch: epoch)
        }
    }

    /// Drops the connection of `epoch` unless another one replaced it, and
    /// reports the client disconnected.
    private func dropConnection(epoch: UInt64) {
        guard epoch == connectionEpoch, connection != nil, state != .shutdown else {
            return
        }
        connection = nil
        state = .disconnected
        publish(.disconnected)
    }

    /// `notPrimary` takes the `transientNotAccepted` path, as in the Rust SDK:
    /// neither was admitted, and the primary is found by moving to another
    /// node. Unlike a not-accepted refusal it is not replayed on the same node
    /// first, since a backup keeps refusing until the view changes.
    static func graded(_ failure: IggyError) -> IggyError {
        guard failure.code == .notPrimary else {
            return failure
        }
        return IggyError(.transientNotAccepted, context: "\(failure)")
    }

    /// Whether an eviction read as `answer` ends the session of a replicated
    /// write, as `noSession`, `sessionTooLow`, `sessionReleaseMismatch`,
    /// `userInactive` and `sessionError` do. Every replicated operation is a
    /// write here, also a segment deletion, which is neither a metadata nor a
    /// partition operation.
    static func endsSessionOfWrite(_ answer: IggyError, _ operation: ConsensusOperation) -> Bool {
        answer.code == .unauthenticated && ![.nonReplicated, .register, .logout].contains(operation)
    }

    /// The cluster endpoints one request has already been refused on.
    private struct RosterWalk: Sendable {
        var visited = Set<String>()
        var checkedLeader = false
    }

    /// Moves the connection for a request the current node keeps refusing to
    /// admit: to the metadata leader once, then along the roster. Reports
    /// whether the client moved.
    private func failOver(_ from: RosterWalk) async throws -> (moved: Bool, walk: RosterWalk) {
        var walk = from
        try await routingLock.lock()
        defer { routingLock.unlock() }
        if !walk.checkedLeader {
            walk.checkedLeader = true
            walk.visited.insert(currentAddress)
            if try await handleLeaderRedirection() {
                try await connect()
                return (true, walk)
            }
        }
        guard let next = nextRosterEndpoint(excluding: walk.visited) else {
            return (false, walk)
        }
        // The roster names this node as the metadata leader, yet it keeps
        // refusing: its replica of the target partition group is not that
        // group's primary. Walk the roster instead of replaying into the same
        // refusal.
        walk.visited.insert(next)
        try await settle(on: next)
        try await connect(settleOffLeader: true)
        return (true, walk)
    }

    /// Holds the exchange lock for one request, so the lockstep stream sees
    /// one request at a time. Throws only for a caller cancelled before its
    /// turn, which leaves the connection untouched.
    private func attempt(
        code: UInt32, payload: [UInt8], context: PartitionContext, lineage: UInt64, request: inout VSRFrame.EncodedRequest?, epoch: inout UInt64,
        outcome: inout RetryOutcome, transientDeadline: ContinuousClock.Instant, readDeadline: ContinuousClock.Instant, replaysUncommitted: Bool
    ) async throws -> Result<[UInt8], IggyError> {
        try await exchangeLock.lock()
        defer { exchangeLock.unlock() }
        return await exchangeLocked(
            code: code, payload: payload, context: context, lineage: lineage, request: &request, epoch: &epoch, outcome: &outcome,
            transientDeadline: transientDeadline, readDeadline: readDeadline, replaysUncommitted: replaysUncommitted)
    }

    /// Stamps the request if it is not stamped yet and exchanges it, replaying
    /// in place while the server answers transiently: "not accepted" until
    /// `transientDeadline`, and "not committed" until `readDeadline` when
    /// `replaysUncommitted` is set. Once the outcome is unknown, any other
    /// answer ends the replay, as in the Rust SDK: the same request id gets
    /// the same answer again. Nothing is written once the caller signed in or
    /// out after `lineage`.
    private func exchangeLocked(
        code: UInt32, payload: [UInt8], context: PartitionContext, lineage: UInt64, request: inout VSRFrame.EncodedRequest?, epoch: inout UInt64,
        outcome: inout RetryOutcome, transientDeadline: ContinuousClock.Instant, readDeadline: ContinuousClock.Instant, replaysUncommitted: Bool
    ) async -> Result<[UInt8], IggyError> {
        if let refusal = lineageRefusal(lineage, code: code) {
            return .failure(refusal)
        }
        guard let connection else {
            return .failure(IggyError(.notConnected))
        }
        epoch = connectionEpoch
        let stamped: VSRFrame.EncodedRequest
        if let request {
            stamped = request
        } else {
            do {
                stamped = try stamp(code: code, payload: payload, context: context)
            } catch let error as IggyError {
                return .failure(error)
            } catch {
                return .failure(IggyError(.invalidConfiguration, context: "\(error)"))
            }
            request = stamped
        }
        let clock = ContinuousClock()
        // The answer to the last attempt. At the deadline it is the error, as
        // in the Rust SDK, and with no attempt written the request was never
        // admitted. Either way the stream sits at a frame boundary, so the
        // connection stays.
        var refusal: IggyError?
        while true {
            let remaining = readDeadline - clock.now
            // A replay sent with a sliver of the budget would time out, and the
            // timeout takes the connection with it: the refusal stands instead.
            guard remaining > .zero, refusal == nil || remaining >= Self.notReadyRetryInterval else {
                return .failure(refusal ?? IggyError(.transientNotAccepted, context: "the request deadline passed before command \(code) was sent"))
            }
            if let ended = lineageRefusal(lineage, code: code) {
                // The caller signed in or out during the pause before a replay.
                return .failure(ended)
            }
            let raw: RawFrame
            do {
                raw = try await connection.exchange(stamped.bytes, timeout: remaining)
            } catch let error as IggyError {
                logger.error("Request failed", metadata: ["code": "\(code)", "error": "\(error)"])
                switch error.code {
                case .disconnected, .tcpError, .notConnected:
                    return .failure(error)
                default:
                    return .failure(IggyError(.disconnected, context: "\(error)"))
                }
            } catch {
                return .failure(IggyError(.disconnected, context: "\(error)"))
            }
            let header = raw.header[...]
            if VSRFrame.peekCommand(header) == .reply, VSRFrame.readReplyRequestID(header) != stamped.requestID {
                // A reply must answer the request in flight; an unexpected echo
                // means every later reply would pair off by one.
                await connection.close()
                let answered = VSRFrame.readReplyRequestID(header).map(String.init) ?? "unknown"
                return .failure(IggyError(.disconnected, context: "the reply answers request \(answered), expected \(stamped.requestID)"))
            }
            observeMetadataReply(header)
            do {
                let body = try VSRFrame.decodeReply(header: header, body: raw.body[...])
                if outcome.isUncertain && stamped.operation != .nonReplicated && stamped.operation != .register && stamped.operation != .logout
                    && !VSRFrame.hasCommittedResult(header)
                {
                    throw IggyError(.transientNotCommitted, context: "the reply does not prove the earlier attempt committed")
                }
                return .success(Array(body))
            } catch let answer as IggyError {
                let evicted = VSRFrame.peekCommand(header) == .eviction
                if evicted, answer.code == .staleClient || Self.endsSessionOfWrite(answer, stamped.operation) {
                    // As in the Rust SDK, a stale client eviction ends only the
                    // binding of the connection and the session goes on over a
                    // new one, and a write can commit before an eviction ends
                    // its session. Either way the request is lost with the
                    // connection, not refused: only its own session replays it.
                    await connection.close()
                    return .failure(IggyError(.disconnected, context: "\(answer)"))
                }
                // After a stale client refusal or a session-ending eviction the
                // server attaches this connection to no session, so it refuses
                // every replay on it.
                let endsSession = answer.code == .staleClient || (answer.code == .unauthenticated && evicted)
                let transient = answer.code == .transientNotCommitted || answer.code == .transientNotAccepted || answer.code == .notPrimary
                // Once one attempt left the outcome unknown, a later refusal
                // of the same request no longer proves it never committed.
                let error = outcome.observe(answer, committed: VSRFrame.hasCommittedResult(header))
                refusal = error
                if answer.code == .notPrimary, !VSRFrame.hasCommittedResult(header) {
                    return .failure(answer)
                }
                switch error.code {
                case .transientNotCommitted where replaysUncommitted && transient && clock.now < readDeadline:
                    // The outcome is unknown; only a same-session replay of the
                    // same request id is safe, which the client table answers
                    // from its reply cache if it did commit.
                    try? await Task.sleep(for: min(Self.notReadyRetryInterval, readDeadline - clock.now))
                case .transientNotAccepted where clock.now < transientDeadline:
                    try? await Task.sleep(for: min(Self.notReadyRetryInterval, transientDeadline - clock.now))
                default:
                    if endsSession {
                        // The connection goes as a lost one does: the next
                        // request or heartbeat dials again, then binds the
                        // session or signs in.
                        await connection.close()
                        dropConnection(epoch: epoch)
                    }
                    return .failure(error)
                }
            } catch {
                return .failure(IggyError(.invalidCommand, context: "\(error)"))
            }
        }
    }

    /// Raises the metadata watermark to the commit of a metadata reply, as
    /// `observe_metadata_reply` in the Rust SDK. Reads and partition replies
    /// carry no metadata commit.
    private func observeMetadataReply(_ header: ArraySlice<UInt8>) {
        guard VSRFrame.peekCommand(header) == .reply, let operation = VSRFrame.readReplyOperation(header),
            operation != .nonReplicated, !operation.isPartition, let commit = VSRFrame.readReplyCommit(header)
        else {
            return
        }
        metadataWatermark = max(metadataWatermark, commit)
    }

    // MARK: Connecting

    /// The connect a caller asked for, which also lifts an earlier explicit
    /// disconnect.
    func connectByCaller() async throws {
        if disconnectedByCaller, !explicitSignInRequired {
            // The connect starts over, unless a caller sign-in failed.
            signInWithdrawn = false
        }
        disconnectedByCaller = false
        try await connect()
    }

    /// Connects unless a connection stands. Callers share one sweep; a caller
    /// that is cancelled leaves it, and the sweep ends when none is left.
    ///
    /// `skipAutoLogin` belongs to the caller whose own request is the sign-in.
    func connect(skipAutoLogin: Bool = false, settleOffLeader: Bool = false) async throws {
        try await connectGate.run { [self] in
            try await self.connectInner(skipAutoLogin: skipAutoLogin, settleOffLeader: settleOffLeader)
        }
    }

    private func connectInner(skipAutoLogin: Bool, settleOffLeader: Bool) async throws {
        do {
            try await sweep(skipAutoLogin: skipAutoLogin, settleOffLeader: settleOffLeader)
        } catch {
            // A sweep that ends early must not leave a half-open state behind.
            // `.connecting` would make the next connect return at the top
            // without dialing, and a connection whose sign-in was abandoned
            // with its last caller is closed already.
            switch state {
            case .connecting:
                failConnect()
            case .connected, .authenticating:
                if error is CancellationError {
                    await disconnectTransport()
                }
            default:
                break
            }
            throw error
        }
    }

    private func sweep(skipAutoLogin: Bool, settleOffLeader: Bool) async throws {
        while true {
            switch state {
            case .shutdown:
                throw IggyError(.clientShutdown)
            case .connected, .authenticating, .authenticated:
                return
            case .connecting:
                return
            case .disconnected:
                try checkNotDisconnectedByCaller()
                state = .connecting
            }

            var candidates = dialCandidates()
            let pacedEndpoint = currentAddress
            if candidates.count > 1, reestablishWait() != nil {
                // The paced endpoint goes last; the others owe no cooldown.
                candidates.append(candidates.removeFirst())
            }
            var retries: UInt32 = 0
            var candidateIndex = 0
            var configurationFault: IggyError?
            var signInFailure: IggyError?
            var dialFailure: IggyError?
            var shouldRedirect = false
            sweep: while true {
                try endSweepIfStopped()
                let address = candidates[candidateIndex]
                if address == pacedEndpoint, let remaining = reestablishWait() {
                    logger.info("Trying to connect to the server \(address) in \(remaining)")
                    try await Task.sleep(for: remaining)
                }
                try endSweepIfStopped()
                logger.info("Connecting to server \(address)")
                do {
                    let established = try await establishBounded(address, candidates: candidates)
                    if state == .shutdown || disconnectedByCaller || Task.isCancelled {
                        // The client was stopped while this was being dialed:
                        // the connection is dropped before it is installed,
                        // and the check at the top of the sweep ends it.
                        await established.close()
                        continue sweep
                    }
                    currentAddress = address
                    connection = established
                    connectionAddress = address
                    connectionEpoch += 1
                    state = .connected
                    connectedAt = ContinuousClock().now
                    publish(.connected)
                    logger.info(
                        "Connected to server", metadata: ["server_address": "\(address)", "client_address": "\(established.localAddress ?? "unknown")"])
                    do {
                        shouldRedirect = try await establishSession(skipAutoLogin: skipAutoLogin, settleOffLeader: settleOffLeader)
                        break sweep
                    } catch let failure as SignInFailure {
                        if failure.connectionLost {
                            logger.warning("The sign-in on \(address) did not complete: \(failure.error)")
                            signInFailure = failure.error
                            state = .connecting
                        } else {
                            // The connection stands and only the session is
                            // missing; no other endpoint would answer differently.
                            throw failure.error
                        }
                    }
                } catch let error as IggyError where error.code == .cannotEstablishConnection {
                    logger.warning("Cannot connect to \(address): \(error)")
                    dialFailure = error
                } catch let error as IggyError {
                    configurationFault = error
                }

                candidateIndex += 1
                if candidateIndex < candidates.count {
                    continue
                }
                candidateIndex = 0

                if let configurationFault {
                    failConnect()
                    throw configurationFault
                }
                guard configuration.reconnection.enabled else {
                    logger.warning("Automatic reconnection is disabled")
                    failConnect()
                    throw signInFailure ?? dialFailure ?? IggyError(.cannotEstablishConnection, context: currentAddress)
                }
                let maxRetries = configuration.reconnection.maxRetries
                if maxRetries.map({ retries < $0 }) ?? true {
                    retries += 1
                    let limit = maxRetries.map(String.init) ?? "unlimited"
                    logger.info("Retrying to connect (\(retries)/\(limit)), \(candidates.count) endpoint(s) in \(configuration.reconnection.interval)")
                    try await Task.sleep(for: configuration.reconnection.interval)
                    continue
                }
                failConnect()
                throw signInFailure ?? dialFailure ?? IggyError(.cannotEstablishConnection, context: currentAddress)
            }
            if shouldRedirect {
                continue
            }
            return
        }
    }

    private struct SignInFailure: Error {
        let error: IggyError
        let connectionLost: Bool
    }

    /// Re-establishes the session on a connection that just came up and
    /// settles it on the leader. Reports whether the leader check asks for a
    /// redirect.
    private func establishSession(skipAutoLogin: Bool, settleOffLeader: Bool) async throws -> Bool {
        let started = lineage
        guard let credentials = signInCredentials() else {
            logger.info("No credentials to sign in with")
            return false
        }
        if skipAutoLogin {
            logger.info("Skipping automatic sign-in for a retried login request")
            return false
        }
        if session.isBound {
            do {
                try await resumeSession()
                return settleOffLeader ? false : try await settleAfterSignIn()
            } catch let error as IggyError where error.code == .unauthenticated || error.code == .transientNotCommitted {
                if let refusal = lineageRefusal(started, code: CommandCode.bindSession.rawValue) {
                    throw SignInFailure(error: refusal, connectionLost: false)
                }
                // The server no longer holds the session, or cannot tell yet:
                // the client signs in under a new one.
                logger.info("Cannot resume the session, signing in again: \(error)")
                resetSession()
            } catch let error as IggyError {
                let connectionLost = error.isConnectionLoss || error.code == .transientNotAccepted
                await disconnectTransport()
                throw SignInFailure(error: error, connectionLost: connectionLost)
            }
        }
        state = .authenticating
        do {
            _ = try await signIn(credentials, byCaller: false, lineage: started)
            logger.info("Signed in with \(credentials)")
        } catch let error as IggyError {
            throw await failSignIn(error, refusedByServer: started == lineage)
        }
        if settleOffLeader {
            // A failover walking the roster past the metadata leader stays
            // where it dialed; the leader settlement would put it straight
            // back on the node whose partition replica keeps refusing.
            return false
        }
        return try await settleAfterSignIn()
    }

    private func settleAfterSignIn() async throws -> Bool {
        do {
            return try await handleLeaderRedirection()
        } catch let error as IggyError {
            throw SignInFailure(error: error, connectionLost: false)
        }
    }

    /// Binds a new connection to the session the lost one carried, as
    /// `resume_vsr_session` in the Rust SDK, so the session keeps its group
    /// memberships, routes, and request ids.
    private func resumeSession() async throws {
        guard let bound = session.session else {
            return
        }
        let clientID = session.clientID
        let started = lineage
        state = .authenticating
        let payload = try Requests.bindSession(
            clientID: clientID, session: bound, metadataWatermark: metadataWatermark, bindSecret: session.bindSecret)
        let body = try await exchangeOnce(code: CommandCode.bindSession.rawValue, payload: payload, context: .empty, lineage: started)
        if let refusal = lineageRefusal(started, code: CommandCode.bindSession.rawValue) {
            await disconnectTransport()
            throw refusal
        }
        let resumed = try Responses.loginRegister(body[...])
        guard resumed.session == bound else {
            throw IggyError(.sessionMismatch, context: "asked to resume session \(bound), the server bound \(resumed.session)")
        }
        // An explicit disconnect or a shutdown while the bind was in flight
        // replaced the session, and the reply must not revive it.
        guard session.clientID == clientID, !disconnectedByCaller, state == .authenticating else {
            throw IggyError(.notConnected, context: "the session was ended while it was being resumed")
        }
        logger.info("Resumed session \(bound)")
        state = .authenticated
        publish(.signedIn)
    }

    /// `refusedByServer` is false when the caller signed in or out during
    /// the sign-in, so its refusal came from the client and says nothing
    /// about the credential.
    private func failSignIn(_ error: IggyError, refusedByServer: Bool) async -> SignInFailure {
        let connectionLost = error.isConnectionLoss
        if connectionLost {
            await disconnectTransport()
        } else if state == .authenticating {
            state = .connected
        }
        if refusedByServer, error.isCredentialRejection {
            // A rejected credential does not become valid on the next reconnect
            // and replaying it costs an argon2 on the server every time. The
            // configured auto-login is no fallback: the caller may have signed
            // in as another user, and it can be the refused one.
            logger.warning("The server refused the automatic sign-in, the client stays signed out until the next login: \(error)")
            rememberedSignIn = nil
            signInWithdrawn = true
        }
        return SignInFailure(error: error, connectionLost: connectionLost)
    }

    private func failConnect() {
        state = .disconnected
        publish(.disconnected)
    }

    private func checkNotDisconnectedByCaller() throws {
        if disconnectedByCaller {
            throw IggyError(.notConnected, context: "the client was disconnected by its caller")
        }
    }

    /// A sweep checks this before each dial: once the client is shut down,
    /// disconnected by its caller, or left without a caller waiting for it,
    /// the sweep must not bring it back.
    private func endSweepIfStopped() throws {
        if state == .shutdown {
            throw IggyError(.clientShutdown)
        }
        try checkNotDisconnectedByCaller()
        try Task.checkCancellation()
    }

    /// Endpoints to dial for one connect, likeliest first: where the client
    /// currently is, the configured address, then the roster it learned.
    private func dialCandidates() -> [String] {
        var candidates = [currentAddress]
        for endpoint in [configuration.address] + rosterEndpoints where !candidates.contains(where: { isSameSpelling($0, endpoint) }) {
            candidates.append(endpoint)
        }
        return candidates
    }

    private func nextRosterEndpoint(excluding visited: Set<String>) -> String? {
        rosterEndpoints.first { candidate in !visited.contains { isSameSpelling($0, candidate) } }
    }

    private func establishBounded(_ address: String, candidates: [String]) async throws -> any IggyConnection {
        let endpoint = try Endpoint(parsing: address)
        let timeout = candidates.count > 1 ? min(Self.failoverDialTimeout, configuration.connectTimeout) : configuration.connectTimeout
        return try await connector.connect(to: endpoint, tls: configuration.tls, timeout: timeout, noDelay: configuration.noDelay)
    }

    private func reestablishWait() -> Duration? {
        guard let connectedAt else {
            return nil
        }
        let elapsed = ContinuousClock().now - connectedAt
        let interval = configuration.reconnection.reestablishAfter
        return elapsed < interval ? interval - elapsed : nil
    }

    /// Tears the connection down without touching the remembered sign-in or
    /// the session, so a reconnect can resume it.
    ///
    /// A frame another caller already wrote keeps the connection until its
    /// reply, as the Rust client's teardown waits on the link: cutting it would
    /// leave a request that may commit with an unknown outcome, and its replay
    /// on the next node could apply it twice.
    func disconnectTransport() async {
        guard canTearDown else {
            return
        }
        let epoch = connectionEpoch
        let holdsExchange = connection != nil
        if holdsExchange {
            await waitForExchange()
            guard canTearDown, epoch == connectionEpoch else {
                // The caller in flight dropped the connection, or a reconnect
                // replaced it, while this waited.
                exchangeLock.unlock()
                return
            }
        }
        logger.info("Disconnecting from server \(currentAddress)")
        state = .disconnected
        let old = connection
        connection = nil
        await old?.close()
        if holdsExchange {
            exchangeLock.unlock()
        }
        publish(.disconnected)
    }

    private var canTearDown: Bool {
        switch state {
        case .disconnected, .shutdown:
            false
        case .connecting:
            // A connect is already sweeping; tearing down under it would take
            // the stream it just installed.
            false
        default:
            true
        }
    }

    /// Takes the exchange lock once the exchange in flight has its reply. A
    /// teardown must happen even when the task that asked for it was
    /// cancelled, so the wait runs outside that task and cannot be abandoned.
    private func waitForExchange() async {
        let lock = exchangeLock
        _ = try? await Task { try await lock.lock() }.value
    }

    /// An explicit disconnect ends the session on purpose: the remembered
    /// sign-in and the session go with it so the next connect does not
    /// resurrect it.
    func disconnect() async {
        disconnectedByCaller = true
        lineage += 1
        rememberedSignIn = nil
        stopHeartbeat()
        await disconnectTransport()
        resetSession()
    }

    func shutdown() async {
        guard state != .shutdown else {
            return
        }
        stopHeartbeat()
        let old = connection
        connection = nil
        resetSession()
        state = .shutdown
        await old?.close()
        publish(.shutdown)
        for continuation in eventSubscribers.values {
            continuation.finish()
        }
        eventSubscribers.removeAll()
    }

    // MARK: Session

    /// Credentials to sign in with after connecting: the ones a sign-in on this
    /// client last succeeded with, else the configured ones with any committed
    /// password change applied, unless the sign-in was withdrawn.
    private func signInCredentials() -> Credentials? {
        if let rememberedSignIn {
            return rememberedSignIn.credentials
        }
        if signInWithdrawn {
            return nil
        }
        switch configuration.autoLogin {
        case .usernamePassword(let username, let password):
            return .usernamePassword(username: configuredUsername ?? username, password: configuredPassword ?? password)
        case .personalAccessToken(let token):
            return .personalAccessToken(token)
        case nil:
            return nil
        }
    }

    func login(username: String, password: String) async throws -> IdentityInfo {
        try await signIn(.usernamePassword(username: username, password: password), byCaller: true)
    }

    func login(personalAccessToken token: String) async throws -> IdentityInfo {
        try await signIn(.personalAccessToken(token), byCaller: true)
    }

    /// Registers a session with `credentials`, which the client signs in
    /// with again after a reconnect. A sign-in by the caller ends every call
    /// made before it.
    private func signIn(_ credentials: Credentials, byCaller: Bool, lineage captured: UInt64? = nil) async throws -> IdentityInfo {
        if byCaller {
            lineage += 1
            rememberedSignIn = nil
            signInWithdrawn = true
            explicitSignInRequired = true
        }
        let started = captured ?? lineage
        let deadline = ContinuousClock.now + configuration.requestTimeout
        do {
            let code: CommandCode
            let makePayload: (_ bindSecret: [UInt8]) throws -> [UInt8]
            switch credentials {
            case .usernamePassword(let username, let password):
                try Requests.validateUsername(username)
                try Requests.validatePassword(password)
                code = .loginRegister
                makePayload = { try Requests.loginRegister(username: username, password: password, bindSecret: $0) }
            case .personalAccessToken(let token):
                try Requests.validatePersonalAccessToken(token)
                code = .loginRegisterWithPersonalAccessToken
                makePayload = { try Requests.loginRegisterWithPersonalAccessToken(token: token, bindSecret: $0) }
            }
            if byCaller, connectGate.isActive {
                // Join only the transport sweep. Its obsolete sign-in cannot
                // supply the caller's identity or prevent their own register.
                do {
                    guard try await withDeadline(deadline, { [self] in try await self.connect(skipAutoLogin: true) }) != nil else {
                        throw IggyError(.transientNotAccepted, context: "the sign-in deadline passed while waiting for the connection")
                    }
                } catch let error as IggyError {
                    if ContinuousClock.now >= deadline { throw error }
                }
            }
            let response = try await register(code: code.rawValue, payload: makePayload, lineage: started, byCaller: byCaller, deadline: deadline)
            if let refusal = lineageRefusal(started, code: code.rawValue) { throw refusal }
            rememberSignIn(credentials, userID: response.userID)
            await learnRosterOnce()
            if byCaller, try await handleLeaderRedirection() {
                try await connect()
            }
            if let refusal = lineageRefusal(started, code: code.rawValue) { throw refusal }
            if byCaller { explicitSignInRequired = false }
            return IdentityInfo(userID: response.userID)
        } catch {
            if byCaller, started == lineage {
                // Recovery started during this attempt must not restore its
                // credentials after the caller has already seen it fail.
                lineage += 1
                rememberedSignIn = nil
                signInWithdrawn = true
                // Detach before awaiting close so a later sign-in owns its transport.
                let abandoned = connection
                dropConnection(epoch: connectionEpoch)
                resetSession()
                await abandoned?.close()
            }
            throw error
        }
    }

    private func register(
        code: UInt32, payload makePayload: (_ bindSecret: [UInt8]) throws -> [UInt8], lineage started: UInt64, byCaller: Bool,
        deadline: ContinuousClock.Instant
    ) async throws -> Responses.LoginRegister {
        try await signInLock.lock()
        defer { signInLock.unlock() }
        if let refusal = lineageRefusal(started, code: code) { throw refusal }
        // The server binds a connection to one session and a re-login on a
        // bound connection is served as an idempotent replay of the old
        // identity, so switching users needs a logout first.
        if state == .authenticated {
            try await logout(lineage: started, deadline: deadline)
        }
        if let refusal = lineageRefusal(started, code: code) { throw refusal }
        // An explicit sign-in cannot replay an earlier register's identity,
        // even if its reply was superseded before the local session was bound.
        if byCaller || session.isBound {
            resetSession()
        }
        // The login body carries the bind secret of the session the frame
        // encoder registers, and both survive a retry on a new connection.
        let payload = try makePayload(session.bindSecret)
        let body: [UInt8]
        do {
            body = try await exchange(code: code, payload: payload, deadline: deadline, lineage: started)
        } catch {
            if started == lineage { resetSession() }
            throw error
        }
        if let refusal = lineageRefusal(started, code: code) {
            await disconnectTransport()
            throw refusal
        }
        let response: Responses.LoginRegister
        do {
            response = try Responses.loginRegister(body[...])
            try session.bind(response.session)
        } catch {
            resetSession()
            throw error
        }
        // Nothing synced or routed before the bind belongs to this session.
        endSessionScope()
        logger.debug(
            "Authenticated against iggy server",
            metadata: ["server_version": "\(response.serverVersion)", "server_protocol_version": "\(response.serverProtocolVersion)"])
        state = .authenticated
        publish(.signedIn)
        return response
    }

    func logout(lineage captured: UInt64? = nil, deadline: ContinuousClock.Instant? = nil) async throws {
        if captured == nil { lineage += 1 }
        let started = captured ?? lineage
        guard state == .authenticated else {
            // No connection carries the session to end it on the server, so
            // it is dropped here: no reconnect binds it or signs in again.
            rememberedSignIn = nil
            signInWithdrawn = true
            resetSession()
            throw IggyError(.unauthenticated)
        }
        _ = try await exchange(code: CommandCode.logoutUser.rawValue, payload: [], deadline: deadline, lineage: started)
        if let refusal = lineageRefusal(started, code: CommandCode.logoutUser.rawValue) { throw refusal }
        rememberedSignIn = nil
        signInWithdrawn = true
        resetSession()
        state = .connected
        publish(.signedOut)
    }

    /// Starts a new consensus session. What was synced or routed under the
    /// old one goes with it.
    private func resetSession() {
        session.reset()
        endSessionScope()
    }

    private func endSessionScope() {
        groupState.clearSessionScoped()
        pollRouter.clearSession(fence: session.clientID)
    }

    private func rememberSignIn(_ credentials: Credentials, userID: UInt32) {
        rememberedSignIn = (credentials, userID)
    }

    /// Keeps the credentials a reconnect signs in with in step with a
    /// committed password change or rename, whether a typed call or a raw
    /// request sent it.
    private func refreshCredentials(afterCommitting code: UInt32, payload: [UInt8]) {
        do {
            switch CommandCode(rawValue: code) {
            case .changePassword:
                let change = try Requests.changedPassword(payload)
                refreshSessionPassword(user: change.userID, newPassword: change.newPassword)
            case .updateUser:
                let update = try Requests.renamedUser(payload)
                if let username = update.username {
                    refreshSessionUsername(user: update.userID, newUsername: username)
                }
            default:
                break
            }
        } catch {
            logger.warning("Cannot read committed command \(code) to refresh the sign-in credentials: \(error)")
        }
    }

    /// A committed password change for `user`: when it is the signed-in user
    /// or the configured auto-login user, the credentials the next reconnect
    /// signs in with switch to the new password.
    private func refreshSessionPassword(user: Identifier, newPassword: String) {
        var rememberedUsername: (name: String, isSessionUser: Bool)?
        if let remembered = rememberedSignIn, case .usernamePassword(let username, _) = remembered.credentials {
            let targetsSessionUser: Bool
            switch user.kind {
            case .numeric(let id): targetsSessionUser = id == remembered.userID
            case .named(let name): targetsSessionUser = name == username
            }
            if targetsSessionUser {
                rememberedSignIn = (.usernamePassword(username: username, password: newPassword), remembered.userID)
            }
            rememberedUsername = (username, targetsSessionUser)
        }
        guard case .usernamePassword(let configuredAtStart, _) = configuration.autoLogin else {
            return
        }
        let configured = configuredUsername ?? configuredAtStart
        let targetsConfiguredUser: Bool
        switch user.kind {
        case .named(let name): targetsConfiguredUser = name == configured
        case .numeric: targetsConfiguredUser = rememberedUsername.map { $0.isSessionUser && $0.name == configured } ?? false
        }
        if targetsConfiguredUser {
            configuredPassword = newPassword
        }
    }

    /// A committed rename of `user`: the credentials the next reconnect signs
    /// in with follow the new name when it is the signed-in user or the
    /// configured auto-login user, so a rename does not strand the session.
    private func refreshSessionUsername(user: Identifier, newUsername: String) {
        var renamed: String?
        if let remembered = rememberedSignIn, case .usernamePassword(let username, let password) = remembered.credentials {
            let targetsSessionUser: Bool
            switch user.kind {
            case .numeric(let id): targetsSessionUser = id == remembered.userID
            case .named(let name): targetsSessionUser = name == username
            }
            if targetsSessionUser {
                rememberedSignIn = (.usernamePassword(username: newUsername, password: password), remembered.userID)
                renamed = username
            }
        }
        guard case .usernamePassword(let configuredAtStart, _) = configuration.autoLogin else {
            return
        }
        let configured = configuredUsername ?? configuredAtStart
        if renamed == configured || user.name == configured {
            configuredUsername = newUsername
        }
    }

    // MARK: Cluster awareness

    /// Reads the roster once, on the first sign-in a caller ran by hand: a
    /// client with configured auto-login learns it from the connect flow's
    /// leader check instead.
    private func learnRosterOnce() async {
        guard configuration.autoLogin == nil, state == .authenticated, !rosterLearned else {
            return
        }
        rosterLearned = true
        let endpoints =
            (try? await withDeadline(ContinuousClock.now + Self.rosterReadTimeout) { [self] in
                await self.readTransportEndpoints()
            }) ?? []
        if !endpoints.isEmpty {
            logger.info("Learned \(endpoints.count) endpoint(s) to fail over to")
            rosterEndpoints = endpoints
        }
    }

    private func readTransportEndpoints() async -> [String] {
        do {
            let body = try await exchangeOnce(code: CommandCode.getClusterMetadata.rawValue, payload: [], context: .empty)
            let metadata = try Responses.clusterMetadata(body[...])
            pollRouter.learnRoster(metadata)
            return Self.transportEndpoints(metadata)
        } catch {
            logger.debug("Failed to read the cluster roster: \(error)")
            return []
        }
    }

    static func transportEndpoints(_ metadata: ClusterMetadata) -> [String] {
        metadata.nodes.filter { $0.endpoints.tcp != 0 }.map(\.tcpAddress)
    }

    /// Checks cluster metadata and moves off a non-leader node. Returns true
    /// when the client disconnected and the caller must reconnect.
    func handleLeaderRedirection() async throws -> Bool {
        let check = await checkLeader()
        if !check.endpoints.isEmpty {
            rosterEndpoints = check.endpoints
        }
        guard let leader = check.redirect else {
            redirects = 0
            return false
        }
        guard redirects < Self.maxLeaderRedirects else {
            logger.warning("Maximum leader redirections reached, continuing with the current connection")
            return false
        }
        redirects += 1
        logger.info("Current node is not the leader, redirecting to \(leader)")
        connectedAt = nil
        await disconnectTransport()
        currentAddress = leader
        return true
    }

    private struct LeaderCheck {
        var redirect: String?
        var endpoints: [String]
    }

    private func checkLeader() async -> LeaderCheck {
        let clock = ContinuousClock()
        let deadline = clock.now + Self.leaderlessWaitBudget
        while true {
            let metadata: ClusterMetadata
            do {
                let body = try await exchangeOnce(code: CommandCode.getClusterMetadata.rawValue, payload: [], context: .empty)
                metadata = try Responses.clusterMetadata(body[...])
            } catch {
                logger.debug("Failed to get cluster metadata: \(error), continuing on \(currentAddress)")
                return LeaderCheck(redirect: nil, endpoints: [])
            }
            pollRouter.learnRoster(metadata)
            let endpoints = Self.transportEndpoints(metadata)
            if metadata.nodes.count <= 1 {
                return LeaderCheck(redirect: nil, endpoints: endpoints)
            }
            guard let leader = metadata.nodes.first(where: { $0.role == .leader && $0.status == .healthy }) else {
                if clock.now >= deadline {
                    logger.warning("No active leader in the cluster roster, continuing on \(currentAddress)")
                    return LeaderCheck(redirect: nil, endpoints: endpoints)
                }
                try? await Task.sleep(for: Self.leaderlessPollInterval)
                continue
            }
            let leaderAddress = leader.tcpAddress
            if isSameSpelling(currentAddress, leaderAddress) {
                return LeaderCheck(redirect: nil, endpoints: endpoints)
            }
            if await isSameAddress(currentAddress, leaderAddress) {
                return LeaderCheck(redirect: nil, endpoints: endpoints)
            }
            return LeaderCheck(redirect: leaderAddress, endpoints: endpoints)
        }
    }

    /// Moves the connection to another roster endpoint for a request the
    /// current node keeps refusing to admit.
    private func settle(on next: String) async throws {
        logger.info("The request keeps being refused on \(currentAddress); trying the next cluster node at \(next)")
        connectedAt = nil
        await disconnectTransport()
        currentAddress = next
    }

    // MARK: Heartbeat

    /// Runs `body` every heartbeat interval until it returns false, which is
    /// how a client that was dropped without a shutdown ends its heartbeat.
    /// A heartbeat already running is kept: cancelling it could abort a ping
    /// in flight and take the connection with it.
    func startHeartbeat(_ body: @escaping @Sendable () async -> Bool) {
        let interval = configuration.heartbeatInterval
        guard heartbeatTask == nil, interval > .zero, state != .shutdown else {
            return
        }
        heartbeatTask = Task {
            while !Task.isCancelled {
                try? await Task.sleep(for: interval)
                if Task.isCancelled {
                    return
                }
                guard await body() else {
                    return
                }
            }
        }
    }

    func stopHeartbeat() {
        heartbeatTask?.cancel()
        heartbeatTask = nil
    }
}

/// Whether an earlier attempt of a request may have committed, as the Rust
/// SDK marks a call `uncertain` in `core/sdk/src/driver.rs`.
///
/// `transientNotCommitted` leaves the outcome unknown. A later refusal of the
/// same request, on this node or the next, cannot establish that the earlier
/// attempt did not commit, so it is reported as unknown too instead of as
/// proof that re-issuing the request under a new session is safe.
struct RetryOutcome {
    private(set) var isUncertain = false

    /// Records an attempt that was written and lost with its connection.
    mutating func markUncertain() {
        isUncertain = true
    }

    mutating func observe(_ error: IggyError, committed: Bool = false) -> IggyError {
        if committed {
            isUncertain = false
            return error
        }
        if error.code == .requestTooOld {
            // The server took a later request id of this session first, which
            // a replay or a data connection can cause. An earlier attempt of
            // this one may have committed.
            isUncertain = true
            return IggyError(.transientNotCommitted, context: "the server already took a later request: \(error)")
        }
        if error.code == .transientNotCommitted {
            isUncertain = true
        }
        guard isUncertain else {
            return error
        }
        return IggyError(.transientNotCommitted, context: "an earlier attempt may have committed, then: \(error)")
    }
}
