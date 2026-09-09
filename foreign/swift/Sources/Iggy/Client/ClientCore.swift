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
/// and the cluster roster to fail over to. A port of the Rust
/// `TcpClient`, `send_raw_with_response`, and `leader_aware`.
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
    let groupState = ConsumerGroupState()
    let logger: Logger

    private(set) var state: ClientState = .disconnected
    private var connection: (any IggyConnection)?
    private var session = ConsensusSession()
    private var currentAddress: String
    private var rosterEndpoints: [String] = []
    private var rosterLearned = false
    private var rememberedSignIn: (credentials: Credentials, userID: UInt32)?
    /// The password a committed change gave the configured auto-login user;
    /// the configuration cannot be rewritten and the old password is dead.
    private var configuredPassword: String?
    private var connectedAt: ContinuousClock.Instant?
    private var redirects = 0
    private var skipAutoLoginOnce = false
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
    var isSignedIn: Bool { state == .authenticated }

    private func setState(_ newState: ClientState) {
        state = newState
    }

    func subscribeEvents() -> AsyncStream<DiagnosticEvent> {
        let id = UUID()
        let (stream, continuation) = AsyncStream<DiagnosticEvent>.makeStream(bufferingPolicy: .bufferingNewest(256))
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
    /// when the failure is one a fresh connection recovers from.
    func exchange(code: UInt32, payload: [UInt8]) async throws -> [UInt8] {
        do {
            return try await sendRaw(code: code, payload: payload)
        } catch let error as IggyError {
            guard error.isReconnectable else {
                throw error
            }
            if code == CommandCode.getClusterMetadata.rawValue {
                // An unauthenticated roster read cannot be repaired by a
                // reconnect; it would re-issue the same read forever.
                throw error
            }
            guard configuration.reconnection.enabled else {
                throw IggyError(.disconnected, context: "automatic reconnection is disabled")
            }
            let isLogin = CommandCode.isRegister(code)
            if !isLogin, signInCredentials() == nil {
                // With no credentials a reconnect cannot restore the session, so
                // anything but a sign-in fails instead of replaying
                // unauthenticated. The sign-in itself is the exception: the
                // server stays silent on a transient register failure and
                // relies on the client replaying it.
                throw error
            }
            if isLogin, await connectGate.isActive {
                // The sign-in ran inside a connect sweep, which owns the
                // endpoint walk; the sweep reports the failure itself.
                throw error
            }
            // A replay over a new connection runs under a new session, and the
            // server deduplicates by (client, request). Replay only what
            // provably never reached the log.
            let replay = replayAfterSessionResetIsSafe(code: code, error: error)
            await routingLock.lock()
            defer { Task { await routingLock.unlock() } }
            await disconnectTransport()
            if isLogin {
                skipAutoLoginOnce = true
            }
            logger.info("Reconnecting to the server", metadata: ["server_address": "\(currentAddress)", "error": "\(error)"])
            do {
                try await connect()
            } catch {
                skipAutoLoginOnce = false
                throw error
            }
            guard replay else {
                logger.warning(
                    "Reconnected, but the outcome of command \(code) is unknown: replaying it could apply it twice, so the original error is returned instead")
                throw error
            }
            return try await sendRaw(code: code, payload: payload)
        }
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

    /// One request against the current connection, bounded by one deadline
    /// across every same-connection replay and every leader failover.
    private func sendRaw(code: UInt32, payload: [UInt8]) async throws -> [UInt8] {
        switch state {
        case .shutdown:
            throw IggyError(.clientShutdown)
        case .disconnected, .connecting:
            throw IggyError(.notConnected)
        default:
            break
        }
        let clock = ContinuousClock()
        let overallDeadline = clock.now + configuration.requestTimeout
        let isLogin = CommandCode.isRegister(code)
        var frame: [UInt8]?
        var visited = Set<String>()
        var walkingRoster = false
        var checkedLeader = false
        while true {
            let transientDeadline = isLogin ? overallDeadline : min(overallDeadline, clock.now + Self.transientFailoverCheckInterval)
            let outcome = await attempt(code: code, payload: payload, frame: &frame, transientDeadline: transientDeadline, readDeadline: overallDeadline)
            switch outcome {
            case .success(let body):
                return body
            case .failure(let error):
                guard error.code == .transientNotAccepted, !isLogin, clock.now < overallDeadline else {
                    if error.code == .disconnected {
                        // The reply stream is at an unknown boundary; a late
                        // reply would desync framing for the next request.
                        connection = nil
                        setState(.disconnected)
                        publish(.disconnected)
                    }
                    throw error
                }
                if code == CommandCode.getClusterMetadata.rawValue {
                    throw error
                }
                // The server never admitted the request, so re-issuing it
                // cannot double-apply: keep the stamped id for same-session
                // replays; a redirect registers again, so the frame is stamped
                // afresh.
                await routingLock.lock()
                var moved = false
                do {
                    if !walkingRoster, !checkedLeader {
                        checkedLeader = true
                        visited.insert(currentAddress)
                        if try await handleLeaderRedirection() {
                            try await connect()
                            moved = true
                        }
                    }
                    if !moved, let next = nextRosterEndpoint(excluding: visited) {
                        // The roster names this node as the metadata leader,
                        // yet it keeps refusing: its replica of the target
                        // partition group is not that group's primary. Walk
                        // the roster instead of replaying into the same refusal.
                        visited.insert(next)
                        walkingRoster = true
                        try await settle(on: next)
                        try await connect(settleOffLeader: true)
                        moved = true
                    }
                } catch {
                    await routingLock.unlock()
                    throw error
                }
                await routingLock.unlock()
                if moved {
                    frame = nil
                } else if rosterEndpoints.count <= 1 {
                    // A single node: partition materialisation can outlast the
                    // short retry window, so keep replaying until the budget ends.
                    continue
                } else {
                    throw error
                }
            }
        }
    }

    /// Stamps the frame if it is not stamped yet and exchanges it, replaying
    /// in place while the server answers transiently. Runs under the exchange
    /// lock so the lockstep stream sees one request at a time.
    private func attempt(
        code: UInt32, payload: [UInt8], frame: inout [UInt8]?, transientDeadline: ContinuousClock.Instant, readDeadline: ContinuousClock.Instant
    ) async -> Result<[UInt8], IggyError> {
        await exchangeLock.lock()
        defer { Task { await exchangeLock.unlock() } }
        guard let connection else {
            return .failure(IggyError(.notConnected))
        }
        if frame == nil {
            do {
                frame = try VSRFrame.encodeRequest(session: &session, code: code, payload: payload).bytes
            } catch let error as IggyError {
                return .failure(error)
            } catch {
                return .failure(IggyError(.invalidConfiguration, context: "\(error)"))
            }
        }
        let bytes = frame!
        let expectedRequestID = VSRFrame.stampedRequestID(bytes)
        let clock = ContinuousClock()
        while true {
            let remaining = readDeadline - clock.now
            guard remaining > .zero else {
                await connection.close()
                return .failure(IggyError(.disconnected, context: "timed out waiting for the reply to command \(code)"))
            }
            let raw: RawFrame
            do {
                raw = try await connection.exchange(bytes, timeout: remaining)
            } catch let error as IggyError {
                logger.error("Request failed", metadata: ["code": "\(code)", "error": "\(error)"])
                return .failure(error.code == .disconnected || error.code == .tcpError ? error : IggyError(.disconnected, context: "\(error)"))
            } catch {
                return .failure(IggyError(.disconnected, context: "\(error)"))
            }
            let header = raw.header[...]
            if VSRFrame.peekCommand(header) == .reply, VSRFrame.readReplyRequestID(header) != expectedRequestID {
                // A reply must answer the request in flight; an unexpected echo
                // means every later reply would pair off by one.
                await connection.close()
                return .failure(
                    IggyError(.disconnected, context: "the reply answers request \(VSRFrame.readReplyRequestID(header)), expected \(expectedRequestID)"))
            }
            do {
                return .success(Array(try VSRFrame.decodeReply(header: header, body: raw.body[...])))
            } catch let error as IggyError {
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

    func connect(settleOffLeader: Bool = false) async throws {
        try await connectGate.run { [self] in
            try await self.connectInner(settleOffLeader: settleOffLeader)
        }
    }

    private func connectInner(settleOffLeader: Bool) async throws {
        while true {
            switch state {
            case .shutdown:
                throw IggyError(.clientShutdown)
            case .connected, .authenticating, .authenticated:
                return
            case .connecting:
                return
            case .disconnected:
                setState(.connecting)
            }

            var candidates = dialCandidates()
            let pacedEndpoint = currentAddress
            if candidates.count > 1, reestablishWait() != nil {
                // The paced endpoint goes last; the others owe no cooldown.
                candidates.append(candidates.removeFirst())
            }
            let skipAutoLogin = skipAutoLoginOnce
            skipAutoLoginOnce = false

            var retries: UInt32 = 0
            var candidateIndex = 0
            var configurationFault: IggyError?
            var signInFailure: IggyError?
            var shouldRedirect = false
            sweep: while true {
                let address = candidates[candidateIndex]
                if address == pacedEndpoint, let remaining = reestablishWait() {
                    logger.info("Trying to connect to the server \(address) in \(remaining)")
                    try await Task.sleep(for: remaining)
                }
                logger.info("Connecting to server \(address)")
                do {
                    let established = try await establishBounded(address, candidates: candidates)
                    currentAddress = address
                    connection = established
                    setState(.connected)
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
                            setState(.connecting)
                        } else {
                            // The connection stands and only the session is
                            // missing; no other endpoint would answer differently.
                            throw failure.error
                        }
                    }
                } catch let error as IggyError where error.code == .cannotEstablishConnection {
                    logger.warning("Cannot connect to \(address): \(error)")
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
                    throw signInFailure ?? IggyError(.cannotEstablishConnection, context: currentAddress)
                }
                let unlimited = configuration.reconnection.maxRetries == nil
                if unlimited || retries < configuration.reconnection.maxRetries! {
                    retries += 1
                    logger.info(
                        "Retrying to connect (\(retries)/\(configuration.reconnection.maxRetries.map(String.init) ?? "unlimited")), \(candidates.count) endpoint(s) in \(configuration.reconnection.interval)"
                    )
                    try await Task.sleep(for: configuration.reconnection.interval)
                    if state == .shutdown {
                        throw IggyError(.clientShutdown)
                    }
                    continue
                }
                failConnect()
                throw signInFailure ?? IggyError(.cannotEstablishConnection, context: currentAddress)
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
        setState(.authenticating)
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
            setState(.connected)
        }
        if error.isCredentialRejection {
            // A rejected credential does not become valid on the next reconnect
            // and replaying it costs an argon2 on the server every time.
            rememberedSignIn = nil
        }
        return SignInFailure(error: error, connectionLost: connectionLost)
    }

    private func failConnect() {
        setState(.disconnected)
        publish(.disconnected)
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
        case .disconnected:
            return
        case .connecting:
            // A connect is already sweeping; tearing down under it would take
            // the stream it just installed.
            return
        default:
            break
        }
        logger.info("Disconnecting from server \(currentAddress)")
        setState(.disconnected)
        let old = connection
        connection = nil
        resetSession()
        await old?.close()
        publish(.disconnected)
    }

    /// An explicit disconnect ends the session on purpose: the remembered
    /// sign-in goes with it so the next reconnect does not resurrect it.
    func disconnect() async {
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
        resetSession()
        setState(.shutdown)
        await old?.close()
        publish(.shutdown)
        for continuation in eventSubscribers.values {
            continuation.finish()
        }
        eventSubscribers.removeAll()
    }

    // MARK: Session

    private func resetSession() {
        session.reset()
        Task { await groupState.clearSessionScoped() }
    }

    /// Credentials to sign in with after connecting: the ones a sign-in on this
    /// client last succeeded with, else the configured ones with any committed
    /// password change applied.
    private func signInCredentials() -> Credentials? {
        if let rememberedSignIn {
            return rememberedSignIn.credentials
        }
        switch configuration.autoLogin {
        case .usernamePassword(let username, let password):
            return .usernamePassword(username: username, password: configuredPassword ?? password)
        case .personalAccessToken(let token):
            return .personalAccessToken(token)
        case nil:
            return nil
        }
    }

    func login(username: String, password: String) async throws -> IdentityInfo {
        try Requests.validateUsername(username)
        try Requests.validatePassword(password)
        let payload = try Requests.loginRegister(username: username, password: password)
        let response = try await register(code: CommandCode.loginRegister.rawValue, payload: payload)
        rememberSignIn(.usernamePassword(username: username, password: password), userID: response.userID)
        await learnRosterOnce()
        return IdentityInfo(userID: response.userID)
    }

    func login(personalAccessToken token: String) async throws -> IdentityInfo {
        let payload = try Requests.loginRegisterWithPersonalAccessToken(token: token)
        let response = try await register(code: CommandCode.loginRegisterWithPersonalAccessToken.rawValue, payload: payload)
        rememberSignIn(.personalAccessToken(token), userID: response.userID)
        await learnRosterOnce()
        return IdentityInfo(userID: response.userID)
    }

    private func register(code: UInt32, payload: [UInt8]) async throws -> Responses.LoginRegister {
        // The server binds a connection to one session and a re-login on a
        // bound connection is served as an idempotent replay of the old
        // identity, so switching users needs a logout first.
        if state == .authenticated {
            try await logout()
        }
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
        await groupState.clearSessionScoped()
        logger.debug(
            "Authenticated against iggy server",
            metadata: ["server_version": "\(response.serverVersion)", "server_protocol_version": "\(response.serverProtocolVersion)"])
        setState(.authenticated)
        publish(.signedIn)
        return response
    }

    func logout() async throws {
        guard state == .authenticated else {
            throw IggyError(.unauthenticated)
        }
        _ = try await exchange(code: CommandCode.logoutUser.rawValue, payload: [])
        rememberedSignIn = nil
        resetSession()
        setState(.connected)
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
        guard case .usernamePassword(let configured, _) = configuration.autoLogin else {
            return
        }
        let targetsConfiguredUser: Bool
        switch user.kind {
        case .named(let name): targetsConfiguredUser = name == configured
        case .numeric: targetsConfiguredUser = rememberedUsername.map { $0.isSessionUser && $0.name == configured } ?? false
        }
        if targetsConfiguredUser {
            configuredPassword = newPassword
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
            await withTimeout(Self.rosterReadTimeout) { [self] in
                await self.readTransportEndpoints()
            } ?? []
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

    func startHeartbeat(_ body: @escaping @Sendable () async -> Void) {
        stopHeartbeat()
        let interval = configuration.heartbeatInterval
        heartbeatTask = Task {
            while !Task.isCancelled {
                try? await Task.sleep(for: interval)
                if Task.isCancelled {
                    return
                }
                await body()
            }
        }
    }

    func stopHeartbeat() {
        heartbeatTask?.cancel()
        heartbeatTask = nil
    }
}

/// Runs `body` with a bound; nil when the bound elapses first.
func withTimeout<T: Sendable>(_ timeout: Duration, _ body: @escaping @Sendable () async -> T) async -> T? {
    await withTaskGroup(of: T?.self) { group in
        group.addTask { await body() }
        group.addTask {
            try? await Task.sleep(for: timeout)
            return nil
        }
        let first = await group.next()!
        group.cancelAll()
        return first
    }
}

/// Whether two addresses are written the same way, up to canonicalization
/// of the host and port.
func isSameSpelling(_ lhs: String, _ rhs: String) -> Bool {
    guard let left = try? Endpoint(parsing: lhs), let right = try? Endpoint(parsing: rhs) else {
        return lhs == rhs
    }
    return left.port == right.port && canonicalHost(left.host) == canonicalHost(right.host)
}

private func canonicalHost(_ host: String) -> String {
    switch host.lowercased() {
    case "localhost", "127.0.0.1", "::1", "0.0.0.0", "::": "localhost"
    default: host.lowercased()
    }
}

/// Whether two addresses name the same endpoint once host names resolve.
func isSameAddress(_ lhs: String, _ rhs: String) async -> Bool {
    if isSameSpelling(lhs, rhs) {
        return true
    }
    guard let left = try? Endpoint(parsing: lhs), let right = try? Endpoint(parsing: rhs), left.port == right.port else {
        return false
    }
    if left.isIPLiteral && right.isIPLiteral {
        return false
    }
    let resolved = await withTimeout(.seconds(2)) { () -> Bool in
        let leftAddresses = resolve(left.host)
        let rightAddresses = resolve(right.host)
        return !leftAddresses.isEmpty && !leftAddresses.isDisjoint(with: rightAddresses)
    }
    return resolved ?? false
}

private func resolve(_ host: String) -> Set<String> {
    var hints = addrinfo()
    #if canImport(Glibc) || canImport(Musl)
        hints.ai_socktype = Int32(SOCK_STREAM.rawValue)
    #else
        hints.ai_socktype = SOCK_STREAM
    #endif
    var result: UnsafeMutablePointer<addrinfo>?
    guard getaddrinfo(host, nil, &hints, &result) == 0, let list = result else {
        return []
    }
    defer { freeaddrinfo(list) }
    var addresses = Set<String>()
    var current: UnsafeMutablePointer<addrinfo>? = list
    while let info = current {
        var buffer = [CChar](repeating: 0, count: Int(NI_MAXHOST))
        if getnameinfo(info.pointee.ai_addr, info.pointee.ai_addrlen, &buffer, socklen_t(buffer.count), nil, 0, NI_NUMERICHOST) == 0 {
            addresses.insert(String(decoding: buffer.prefix { $0 != 0 }.map { UInt8(bitPattern: $0) }, as: UTF8.self))
        }
        current = info.pointee.ai_next
    }
    return addresses
}
