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

import Logging

/// A client for an Apache Iggy server over TCP, with or without TLS.
///
/// The client speaks the consensus wire protocol, the only protocol the
/// server accepts. One client owns one connection and is safe to share
/// between tasks: requests are serialized on the connection, so a single
/// client feeds many producers and consumers.
///
/// ```swift
/// let client = try IggyClient(connectionString: "iggy://iggy:iggy@localhost:8090")
/// try await client.connect()
/// let stream = try await client.createStream(name: "orders")
/// let topic = try await client.createTopic(streamID: "orders", name: "created", options: .init(partitionsCount: 3))
/// try await client.sendMessages(streamID: "orders", topicID: "created", partitioning: .balanced, messages: ["hello"])
/// let polled = try await client.pollMessages(streamID: "orders", topicID: "created", partitionID: 0, consumer: .default, strategy: .first, count: 10, autoCommit: false)
/// try await client.shutdown()
/// ```
///
/// Lifecycle: construct, ``connect()``, use, ``shutdown()``. With
/// ``ClientConfiguration/autoLogin`` set, `connect()` also signs in and every
/// reconnect restores the session; otherwise call ``login(username:password:)``
/// after connecting. ``disconnect()`` closes the connection but keeps the
/// client usable; a client that was shut down cannot reconnect.
///
/// Every operation throws ``IggyError``. Delivery is at-least-once: a
/// request that hits a dropped connection is replayed over a fresh one
/// only when it provably never reached the log.
public final class IggyClient: Sendable {
    public let configuration: ClientConfiguration
    let core: ClientCore
    let logger: Logger

    /// Creates a client that dials `configuration.address` on ``connect()``.
    public convenience init(configuration: ClientConfiguration = ClientConfiguration(), logger: Logger = Logger(label: "org.apache.iggy")) {
        self.init(configuration: configuration, connector: NIOConnector(), logger: logger)
    }

    /// Creates a client for `address` (`host:port`).
    public convenience init(address: String, autoLogin: Credentials? = nil, tls: TLSOptions? = nil) {
        self.init(configuration: ClientConfiguration(address: address, tls: tls, autoLogin: autoLogin))
    }

    /// Creates a client from a connection string such as
    /// `iggy://user:password@localhost:8090?tls=true`. The credentials in
    /// the string become the auto-login.
    public convenience init(connectionString: String, logger: Logger = Logger(label: "org.apache.iggy")) throws {
        self.init(configuration: try ConnectionString.parse(connectionString), logger: logger)
    }

    init(configuration: ClientConfiguration, connector: any IggyConnector, logger: Logger) {
        self.configuration = configuration
        self.logger = logger
        core = ClientCore(configuration: configuration, connector: connector, logger: logger)
    }

    // MARK: Lifecycle

    /// Connects to the server, signing in when auto-login is configured and
    /// settling on the cluster leader. Does nothing when already connected.
    public func connect() async throws {
        try await core.connect()
        await core.startHeartbeat { [weak self] in
            await self?.heartbeat()
        }
    }

    /// Closes the connection and forgets the session. The client can
    /// ``connect()`` again later.
    public func disconnect() async throws {
        await core.disconnect()
    }

    /// Closes the connection and releases every resource. The client cannot
    /// be used afterwards.
    public func shutdown() async throws {
        await core.shutdown()
    }

    public var state: ClientState {
        get async { await core.state }
    }

    /// A new subscription to lifecycle events. Each access creates an
    /// independent stream that ends when the client shuts down.
    public var events: AsyncStream<DiagnosticEvent> {
        get async { await core.subscribeEvents() }
    }

    /// Local address of the current connection.
    public var clientAddress: String? {
        get async { await core.clientAddress }
    }

    /// The endpoint the client is connected to, or will dial next.
    public var serverAddress: String {
        get async { await core.serverAddress }
    }

    private func heartbeat() async {
        guard await core.state == .authenticated else {
            return
        }
        do {
            try await ping()
        } catch {
            logger.debug("Heartbeat ping failed: \(error)")
            return
        }
        await refreshConsumerGroupAssignments()
    }

    // MARK: Authentication

    /// Signs in with a username and password. Any existing session on this
    /// connection is ended first. Remembers the credentials so a reconnect
    /// restores the session.
    public func login(username: String, password: String) async throws -> IdentityInfo {
        let identity = try await core.login(username: username, password: password)
        try await settleOnLeaderAfterLogin()
        return identity
    }

    /// Signs in with a personal access token.
    public func login(personalAccessToken token: String) async throws -> IdentityInfo {
        let identity = try await core.login(personalAccessToken: token)
        try await settleOnLeaderAfterLogin()
        return identity
    }

    /// Ends the session. The next reconnect stays signed out unless
    /// auto-login is configured.
    public func logout() async throws {
        try await core.logout()
    }

    /// After a manual sign-in the roster is consulted once so later
    /// replicated writes land on the leader. The reconnect signs in again
    /// with the credentials just remembered.
    private func settleOnLeaderAfterLogin() async throws {
        if try await core.handleLeaderRedirection() {
            logger.info("Redirected to the leader, reconnecting")
            try await core.connect()
        }
    }

    // MARK: Raw requests

    /// Sends a command code with an already-encoded payload and returns the
    /// raw reply body. For commands without a typed method; the caller owns
    /// the wire format. Session-control codes are rejected because they must
    /// go through ``login(username:password:)`` and ``logout()``.
    public func sendRawRequest(code: UInt32, payload: [UInt8]) async throws -> [UInt8] {
        guard !CommandCode.isSessionControl(code) else {
            throw IggyError(.invalidCommand, context: "code \(code) controls the session; use login and logout")
        }
        return try await core.exchange(code: code, payload: payload)
    }

    // MARK: Internals shared by the domain extensions

    func send(_ code: CommandCode, _ payload: [UInt8] = []) async throws -> [UInt8] {
        try await core.exchange(code: code.rawValue, payload: payload)
    }

    /// Fails fast with the same error the Rust SDK raises for the state.
    func requireAuthenticated() async throws {
        switch await core.state {
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
}

// MARK: - System

extension IggyClient {
    /// Checks that the server is alive. Works before signing in.
    public func ping() async throws {
        _ = try await send(.ping)
    }

    /// Server statistics: process, memory, disk, counts.
    public func getStats() async throws -> Stats {
        try Responses.stats(try await send(.getStats)[...])
    }

    /// The server's view of this client.
    public func getMe() async throws -> ClientInfoDetails {
        try await requireAuthenticated()
        return try Responses.clientDetails(try await send(.getMe)[...])
    }

    /// A connected client by its server-assigned id, or nil if unknown.
    public func getClient(id: UInt32) async throws -> ClientInfoDetails? {
        try await requireAuthenticated()
        let body = try await send(.getClient, Requests.getClient(clientID: id))
        return body.isEmpty ? nil : try Responses.clientDetails(body[...])
    }

    /// Every connected client.
    public func getClients() async throws -> [ClientInfo] {
        try await requireAuthenticated()
        let body = try await send(.getClients)
        return body.isEmpty ? [] : try Responses.clients(body[...])
    }

    /// The option catalog of a resource scope: the keys create accepts, their
    /// kinds, and this server's defaults.
    public func describeOptions(scope: OptionsScope) async throws -> [OptionSpec] {
        try await requireAuthenticated()
        return try Responses.optionSpecs(try await send(.describeOptions, Requests.describeOptions(scope: scope))[...])
    }

    /// Captures a snapshot archive of the server state.
    public func snapshot(compression: SnapshotCompression = .deflated, types: [SystemSnapshotType] = [.all]) async throws -> Snapshot {
        try await requireAuthenticated()
        return Snapshot(data: try await send(.getSnapshotFile, try Requests.getSnapshot(compression: compression, types: types)))
    }

    /// The cluster roster: every node, its role, and its endpoints. Requires
    /// a signed-in session.
    public func getClusterMetadata() async throws -> ClusterMetadata {
        try Responses.clusterMetadata(try await send(.getClusterMetadata)[...])
    }
}
