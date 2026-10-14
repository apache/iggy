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
/// One deliberate difference: a reconnect registers a new session instead of
/// resuming the old one, so a replicated write whose outcome is unknown is
/// never replayed across connections.
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
    private var session = ConsensusSession()
    private var currentAddress: String
    private var rosterEndpoints: [String] = []
    private var rosterLearned = false
    private var rememberedSignIn: (credentials: Credentials, userID: UInt32)?
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
    private var eventSubscribers: [UUID: AsyncStream<DiagnosticEvent>.Continuation] = [:]
    private let exchangeLock = AsyncMutex()
    private let routingLock = AsyncMutex()
    private let connectGate = SingleFlight()
    private var heartbeatTask: Task<Void, Never>?

    init(configuration: ClientConfiguration, connector: any IggyConnector, logger: Logger) {
        self.configuration = configuration
        self.connector = connector
        self.logger = logger
        currentAddress = configuration.address
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
    /// `requestTimeout` from now, covers the request, the reconnect, the
    /// sign-in, the leader check, and the replay.
    ///
    /// A replicated write that may have reached the log before the connection
    /// was lost is not replayed and fails with
    /// ``IggyErrorCode/transientNotCommitted``, as in the Rust SDK, so it is
    /// never mistaken for a request that was not sent.
    ///
    /// A cancelled caller gets `CancellationError`. A request it already wrote
    /// takes the connection with it, since its reply can no longer be told
    /// from the next one, and the next caller reconnects.
    func exchange(code: UInt32, payload: [UInt8]) async throws -> [UInt8] {
        if disconnectedByCaller {
            throw IggyError(state == .shutdown ? .clientShutdown : .notConnected)
        }
        let deadline = ContinuousClock.now + configuration.requestTimeout
        var outcome = RetryOutcome()
        var failedEpoch = connectionEpoch
        do {
            return try await sendRaw(code: code, payload: payload, deadline: deadline, epoch: &failedEpoch, outcome: &outcome)
        } catch let error as IggyError {
            // A cancelled caller cannot use a new connection: every exchange
            // it starts is abandoned at once.
            try Task.checkCancellation()
            guard error.isReconnectable else {
                throw error
            }
            // A replay over a new connection runs under a new session, and the
            // server deduplicates by (client, request). Replay only what
            // provably never reached the log.
            let replay = replayAfterSessionResetIsSafe(code: code, error: error)
            let failure = replay ? error : IggyError(.transientNotCommitted, context: "command \(code) may have committed before: \(error)")
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
                // request does it.
                throw failure
            }
            let epoch = failedEpoch
            let reconnected: Void? = try await withDeadline(deadline) { [self] in
                // The retried sign-in is the sign-in: the reconnect must not
                // run another one ahead of it.
                try await self.reconnect(failedEpoch: epoch, skipAutoLogin: isLogin, cause: error)
            }
            guard reconnected != nil else {
                throw IggyError(failure.code, context: "the request deadline passed while reconnecting after: \(error)")
            }
            guard replay else {
                logger.warning("Reconnected, but the outcome of command \(code) is unknown: replaying it could apply it twice")
                throw failure
            }
            return try await sendRaw(code: code, payload: payload, deadline: deadline, epoch: &failedEpoch, outcome: &outcome)
        }
    }

    /// Fails fast with the error the Rust SDK raises for the state, after
    /// bringing back a connection that dropped on its own. A reply timeout, a
    /// cancelled request, or an outage longer than the request timeout leaves
    /// the client disconnected, and with credentials to sign in again the
    /// next typed call reconnects instead of failing until ``connect()``.
    func requireAuthenticated() async throws {
        switch state {
        case .disconnected, .connecting, .authenticating:
            if !disconnectedByCaller, configuration.reconnection.enabled, signInCredentials() != nil {
                let reconnected: Void? = try await withDeadline(ContinuousClock.now + configuration.requestTimeout) { [self] in
                    try await self.connect()
                }
                if reconnected == nil {
                    throw IggyError(.disconnected, context: "could not reconnect within the request timeout")
                }
            }
        default:
            break
        }
        switch state {
        case .shutdown:
            throw IggyError(.clientShutdown)
        case .disconnected, .connecting, .authenticating:
            throw IggyError(.disconnected)
        case .connected:
            throw IggyError(.unauthenticated)
        case .authenticated:
            return
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

    /// Whether replaying `code` after reconnecting with a new session cannot
    /// apply it twice.
    private func replayAfterSessionResetIsSafe(code: UInt32, error: IggyError) -> Bool {
        if CommandCode.isRegister(code) {
            return true
        }
        switch error.code {
        case .notConnected, .cannotEstablishConnection, .unauthenticated:
            return true
        default:
            break
        }
        let operation = ConsensusOperation.forCode(code)
        return operation == .nonReplicated || operation == .logout
    }

    /// One request against the current connection, bounded by `deadline`
    /// across every same-connection replay and every leader failover. `epoch`
    /// reports the connection the last attempt ran on.
    private func sendRaw(
        code: UInt32, payload: [UInt8], deadline: ContinuousClock.Instant, epoch: inout UInt64, outcome: inout RetryOutcome
    ) async throws -> [UInt8] {
        switch state {
        case .shutdown:
            throw IggyError(.clientShutdown)
        case .disconnected, .connecting:
            throw outcome.observe(IggyError(.notConnected))
        default:
            break
        }
        let clock = ContinuousClock()
        let isLogin = CommandCode.isRegister(code)
        var request: VSRFrame.EncodedRequest?
        var walk = RosterWalk()
        while true {
            let transientDeadline = isLogin ? deadline : min(deadline, clock.now + Self.transientFailoverCheckInterval)
            let attempted = try await attempt(
                code: code, payload: payload, request: &request, epoch: &epoch, outcome: &outcome, transientDeadline: transientDeadline,
                readDeadline: deadline)
            switch attempted {
            case .success(let body):
                return body
            case .failure(let failure):
                let error = outcome.observe(Self.graded(failure))
                guard error.code == .transientNotAccepted, !isLogin, clock.now < deadline else {
                    if error.code == .disconnected, epoch == connectionEpoch, state != .shutdown {
                        // The reply stream is at an unknown boundary; a late
                        // reply would desync framing for the next request.
                        connection = nil
                        state = .disconnected
                        publish(.disconnected)
                    }
                    throw error
                }
                if code == CommandCode.getClusterMetadata.rawValue {
                    throw error
                }
                // The server never admitted the request, so re-issuing it
                // under a new request id cannot double-apply it.
                let from = walk
                guard let step = try await withDeadline(deadline, { [self] in try await self.failOver(from) }) else {
                    throw error
                }
                walk = step.walk
                guard step.moved || rosterEndpoints.count <= 1 else {
                    throw error
                }
                // A caller queued behind this one may have spent the next
                // request id while the exchange lock was free. Replaying the
                // old id after it would be refused as too old, so the frame is
                // stamped afresh.
                request = nil
                if !step.moved {
                    // A single node: partition materialisation can outlast the
                    // short retry window, so keep replaying until the budget ends.
                    try? await Task.sleep(until: min(clock.now + Self.notReadyRetryInterval, deadline), clock: clock)
                }
            }
        }
    }

    /// `notPrimary` takes the `transientNotAccepted` path, as in the Rust SDK:
    /// neither was admitted, and the primary is found by moving to another
    /// node. Unlike a not-accepted refusal it is not replayed on the same node
    /// first, since a backup keeps refusing until the view changes.
    private static func graded(_ failure: IggyError) -> IggyError {
        guard failure.code == .notPrimary else {
            return failure
        }
        return IggyError(.transientNotAccepted, context: "\(failure)")
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
        code: UInt32, payload: [UInt8], request: inout VSRFrame.EncodedRequest?, epoch: inout UInt64, outcome: inout RetryOutcome,
        transientDeadline: ContinuousClock.Instant, readDeadline: ContinuousClock.Instant
    ) async throws -> Result<[UInt8], IggyError> {
        try await exchangeLock.lock()
        defer { exchangeLock.unlock() }
        return await exchangeLocked(
            code: code, payload: payload, request: &request, epoch: &epoch, outcome: &outcome, transientDeadline: transientDeadline,
            readDeadline: readDeadline)
    }

    /// Stamps the request if it is not stamped yet and exchanges it, replaying
    /// in place while the server answers transiently.
    private func exchangeLocked(
        code: UInt32, payload: [UInt8], request: inout VSRFrame.EncodedRequest?, epoch: inout UInt64, outcome: inout RetryOutcome,
        transientDeadline: ContinuousClock.Instant, readDeadline: ContinuousClock.Instant
    ) async -> Result<[UInt8], IggyError> {
        guard let connection else {
            return .failure(IggyError(.notConnected))
        }
        epoch = connectionEpoch
        let stamped: VSRFrame.EncodedRequest
        if let request {
            stamped = request
        } else {
            do {
                stamped = try VSRFrame.encodeRequest(session: &session, code: code, payload: payload)
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
            guard remaining > .zero else {
                return .failure(refusal ?? IggyError(.transientNotAccepted, context: "the request deadline passed before command \(code) was sent"))
            }
            let raw: RawFrame
            do {
                raw = try await connection.exchange(stamped.bytes, timeout: remaining)
            } catch let error as IggyError {
                logger.error("Request failed", metadata: ["code": "\(code)", "error": "\(error)"])
                return .failure(error.code == .disconnected || error.code == .tcpError ? error : IggyError(.disconnected, context: "\(error)"))
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
            do {
                return .success(Array(try VSRFrame.decodeReply(header: header, body: raw.body[...])))
            } catch let answer as IggyError {
                // Once one attempt left the outcome unknown, a later refusal
                // of the same request no longer proves it never committed.
                let error = outcome.observe(answer)
                refusal = error
                switch error.code {
                case .transientNotCommitted where clock.now < readDeadline:
                    // The outcome is unknown; only a same-session replay of the
                    // same request id is safe, which the client table answers
                    // from its reply cache if it did commit.
                    try? await Task.sleep(for: min(Self.notReadyRetryInterval, readDeadline - clock.now))
                case .transientNotAccepted where clock.now < transientDeadline:
                    try? await Task.sleep(for: min(Self.notReadyRetryInterval, transientDeadline - clock.now))
                default:
                    if error.code == .staleClient || error.code == .unauthenticated, VSRFrame.peekCommand(header) == .eviction {
                        // A session-terminal eviction: the connection is done.
                        await connection.close()
                    }
                    return .failure(error)
                }
            } catch {
                return .failure(IggyError(.invalidCommand, context: "\(error)"))
            }
        }
    }

    // MARK: Connecting

    /// The connect a caller asked for, which also lifts an earlier explicit
    /// disconnect.
    func connectByCaller() async throws {
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
        guard let credentials = signInCredentials() else {
            logger.info("No credentials to sign in with")
            return false
        }
        if skipAutoLogin {
            logger.info("Skipping automatic sign-in for a retried login request")
            return false
        }
        state = .authenticating
        do {
            switch credentials {
            case .usernamePassword(let username, let password):
                _ = try await login(username: username, password: password)
            case .personalAccessToken(let token):
                _ = try await login(personalAccessToken: token)
            }
            logger.info("Signed in with \(credentials)")
        } catch let error as IggyError {
            throw await failSignIn(error)
        }
        if settleOffLeader {
            // A failover walking the roster past the metadata leader stays
            // where it dialed; the leader settlement would put it straight
            // back on the node whose partition replica keeps refusing.
            return false
        }
        do {
            return try await handleLeaderRedirection()
        } catch let error as IggyError {
            throw SignInFailure(error: error, connectionLost: false)
        }
    }

    private func failSignIn(_ error: IggyError) async -> SignInFailure {
        let connectionLost = error.isConnectionLoss
        if connectionLost {
            await disconnectTransport()
        } else if state == .authenticating {
            state = .connected
        }
        if error.isCredentialRejection {
            // A rejected credential does not become valid on the next reconnect
            // and replaying it costs an argon2 on the server every time.
            rememberedSignIn = nil
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

    /// Tears the connection down without touching the remembered sign-in, so
    /// a reconnect can restore the session.
    func disconnectTransport() async {
        switch state {
        case .disconnected, .shutdown:
            return
        case .connecting:
            // A connect is already sweeping; tearing down under it would take
            // the stream it just installed.
            return
        default:
            break
        }
        logger.info("Disconnecting from server \(currentAddress)")
        state = .disconnected
        let old = connection
        connection = nil
        // A Register still in flight is retried on the next connection and
        // must arrive under the identity and secret its body was built from.
        if session.isBound {
            session.reset()
        }
        await old?.close()
        publish(.disconnected)
    }

    /// An explicit disconnect ends the session on purpose: the remembered
    /// sign-in goes with it so the next reconnect does not resurrect it.
    func disconnect() async {
        disconnectedByCaller = true
        rememberedSignIn = nil
        stopHeartbeat()
        await disconnectTransport()
    }

    func shutdown() async {
        guard state != .shutdown else {
            return
        }
        stopHeartbeat()
        let old = connection
        connection = nil
        session.reset()
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
    /// password change applied.
    private func signInCredentials() -> Credentials? {
        if let rememberedSignIn {
            return rememberedSignIn.credentials
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
        try Requests.validateUsername(username)
        try Requests.validatePassword(password)
        let response = try await register(code: CommandCode.loginRegister.rawValue) { bindSecret in
            try Requests.loginRegister(username: username, password: password, bindSecret: bindSecret)
        }
        rememberSignIn(.usernamePassword(username: username, password: password), userID: response.userID)
        await learnRosterOnce()
        return IdentityInfo(userID: response.userID)
    }

    func login(personalAccessToken token: String) async throws -> IdentityInfo {
        try Requests.validatePersonalAccessToken(token)
        let response = try await register(code: CommandCode.loginRegisterWithPersonalAccessToken.rawValue) { bindSecret in
            try Requests.loginRegisterWithPersonalAccessToken(token: token, bindSecret: bindSecret)
        }
        rememberSignIn(.personalAccessToken(token), userID: response.userID)
        await learnRosterOnce()
        return IdentityInfo(userID: response.userID)
    }

    private func register(code: UInt32, payload makePayload: (_ bindSecret: [UInt8]) throws -> [UInt8]) async throws -> Responses.LoginRegister {
        // The server binds a connection to one session and a re-login on a
        // bound connection is served as an idempotent replay of the old
        // identity, so switching users needs a logout first.
        if state == .authenticated {
            try await logout()
        }
        // A connection lost under a bound session leaves that session behind.
        // The sign-in on the next one registers a new session, not a replay.
        if session.isBound {
            session.reset()
        }
        // The login body carries the bind secret of the session the frame
        // encoder registers, and both survive a retry on a new connection.
        let payload = try makePayload(session.bindSecret)
        let body: [UInt8]
        do {
            body = try await exchange(code: code, payload: payload)
        } catch {
            session.reset()
            throw error
        }
        let response: Responses.LoginRegister
        do {
            response = try Responses.loginRegister(body[...])
            try session.bind(response.session)
        } catch {
            session.reset()
            throw error
        }
        logger.debug(
            "Authenticated against iggy server",
            metadata: ["server_version": "\(response.serverVersion)", "server_protocol_version": "\(response.serverProtocolVersion)"])
        state = .authenticated
        publish(.signedIn)
        return response
    }

    func logout() async throws {
        guard state == .authenticated else {
            throw IggyError(.unauthenticated)
        }
        _ = try await exchange(code: CommandCode.logoutUser.rawValue, payload: [])
        rememberedSignIn = nil
        session.reset()
        state = .connected
        publish(.signedOut)
    }

    private func rememberSignIn(_ credentials: Credentials, userID: UInt32) {
        rememberedSignIn = (credentials, userID)
    }

    /// A committed password change for `user`: when it is the signed-in user
    /// or the configured auto-login user, the credentials the next reconnect
    /// signs in with switch to the new password.
    func refreshSessionPassword(user: Identifier, newPassword: String) {
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
    func refreshSessionUsername(user: Identifier, newUsername: String) {
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
            let body = try await exchange(code: CommandCode.getClusterMetadata.rawValue, payload: [])
            return Self.transportEndpoints(try Responses.clusterMetadata(body[...]))
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
                let body = try await exchange(code: CommandCode.getClusterMetadata.rawValue, payload: [])
                metadata = try Responses.clusterMetadata(body[...])
            } catch {
                logger.debug("Failed to get cluster metadata: \(error), continuing on \(currentAddress)")
                return LeaderCheck(redirect: nil, endpoints: [])
            }
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

/// Whether an earlier attempt of a request may have committed, a port of
/// `RetryOutcome` in `core/sdk/src/vsr.rs`.
///
/// `transientNotCommitted` leaves the outcome unknown. A later refusal of the
/// same request, on this node or the next, cannot establish that the earlier
/// attempt did not commit, so it is reported as unknown too instead of as
/// proof that re-issuing the request under a new session is safe.
struct RetryOutcome {
    private(set) var isUncertain = false

    mutating func observe(_ error: IggyError) -> IggyError {
        if error.code == .transientNotCommitted {
            isUncertain = true
        }
        guard isUncertain else {
            return error
        }
        switch error.code {
        case .transientNotAccepted, .historyUnavailable, .unauthorized, .unauthenticated, .staleClient, .notConnected, .cannotEstablishConnection:
            return IggyError(.transientNotCommitted, context: "an earlier attempt may have committed, then: \(error)")
        default:
            return error
        }
    }
}
