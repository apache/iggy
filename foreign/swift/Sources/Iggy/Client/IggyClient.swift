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
/// client feeds many producers and consumers. In a cluster, polls and
/// offset writes go to each partition's primary over a second connection
/// bound to the same session.
///
/// ```swift
/// let client = IggyClient(address: "localhost:8090", autoLogin: .usernamePassword(username: "iggy", password: "iggy"))
/// try await client.connect()
/// let stream = try await client.createStream(name: "orders")
/// let topic = try await client.createTopic(streamID: "orders", name: "created", options: .init(partitionsCount: 3))
/// try await client.shutdown()
/// ```
///
/// Lifecycle: construct, ``connect()``, use, ``shutdown()``. With
/// ``ClientConfiguration/autoLogin`` set, `connect()` also signs in and every
/// reconnect restores the session; otherwise call ``login(username:password:)``
/// after connecting. ``disconnect()`` closes the connection but keeps the
/// client usable; a client that was shut down cannot reconnect.
///
/// Every operation throws ``IggyError``, or `CancellationError` when its
/// task is cancelled. A request that hits a dropped connection goes again
/// over a fresh one when it provably never reached the log, or under its own
/// request id when the reconnect binds the same session. Otherwise a
/// replicated write whose outcome is unknown fails with
/// ``IggyErrorCode/transientNotCommitted``. The other SDKs keep such
/// a write in a retained-operation registry and fail it with
/// ``IggyErrorCode/operationUncertain``. This SDK has no such registry, so
/// it keeps `transientNotCommitted`.
///
/// A request that was not sent before its deadline fails with
/// ``IggyErrorCode/transientNotAccepted``, and a retry can succeed. A call
/// that waited for an automatic sign-in whose credentials the server
/// refused, such as an invalid or expired personal access token, fails with
/// ``IggyErrorCode/unauthenticated``. So does a call made before the caller
/// signed in or out, at its next request, so that no request of the call
/// runs under a session opened after the call started. A send with balanced
/// or key partitioning reads the topic first, and a consumer-group poll
/// syncs the assignment first: a sign-in between the two ends the call
/// before the second request.
public final class IggyClient: Sendable {
    /// The configuration the client was created with.
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

    init(configuration: ClientConfiguration, connector: any IggyConnector, logger: Logger) {
        self.configuration = configuration
        self.logger = logger
        core = ClientCore(configuration: configuration, connector: connector, logger: logger)
    }

    // MARK: Lifecycle

    /// Connects to the server, signing in when auto-login is configured and
    /// settling on the cluster leader. Does nothing when already connected.
    public func connect() async throws {
        try await core.connectByCaller()
        await core.startHeartbeat { [weak self] in
            guard let self else {
                return false
            }
            await self.heartbeat()
            return true
        }
    }

    /// Closes the connection and forgets the session. A call made before it
    /// ends as after ``logout()``. Later requests fail with
    /// ``IggyErrorCode/notConnected`` until ``connect()`` is called again; no
    /// reconnect brings the client back on its own.
    public func disconnect() async {
        await core.disconnect()
    }

    /// Closes the connection and releases every resource. The client cannot
    /// be used afterwards.
    public func shutdown() async {
        await core.shutdown()
    }

    /// Where the client is in its lifecycle.
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

    /// Pings in every state but shutdown, as the Rust SDK does: a ping on a
    /// client whose connection dropped is what reconnects it between calls.
    /// After a ping that went through, the joined consumer groups are synced.
    private func heartbeat() async {
        guard await core.state != .shutdown else {
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
    /// connection is ended first, with every call made before the sign-in,
    /// as ``logout()`` ends them. Supersedes automatic sign-in and uses these
    /// credentials. A failure, including invalid input or a timeout, discards
    /// any session registered by the attempt and disables automatic sign-in
    /// until another explicit sign-in succeeds.
    public func login(username: String, password: String) async throws -> IdentityInfo {
        try await core.login(username: username, password: password)
    }

    /// Signs in with a personal access token, as
    /// ``login(username:password:)`` does.
    public func login(personalAccessToken token: String) async throws -> IdentityInfo {
        try await core.login(personalAccessToken: token)
    }

    /// Ends the session. A call made before it ends at its next request or
    /// attempt with ``IggyErrorCode/unauthenticated``, or with
    /// ``IggyErrorCode/transientNotCommitted`` when it may have committed,
    /// so it never runs under a session signed in after it. The client does
    /// not sign in by itself again, not even with the configured auto-login,
    /// until the next ``login(username:password:)``, or the first
    /// ``connect()`` after a ``disconnect()``. A logout while the client is
    /// not signed in, such as after a lost connection, fails with
    /// ``IggyErrorCode/unauthenticated`` and still signs out.
    public func logout() async throws {
        try await core.logout()
    }

    // MARK: Raw requests

    /// Sends a command code with an already-encoded payload and returns the
    /// raw reply body. For commands without a typed method; the caller owns
    /// the wire format. Session-control codes, `BindSession` among them, are
    /// rejected because they must go through ``login(username:password:)``
    /// and ``logout()``. A committed `ChangePassword` or `UpdateUser` moves
    /// the credentials reconnects sign in with, as the typed calls do.
    ///
    /// Partition requests carry the partition context their typed call
    /// sends: `SendMessages` with an explicit partition id, `PollMessages`,
    /// `GetConsumerOffset`, `StoreConsumerOffset`, and `DeleteConsumerOffset`
    /// take the route of
    /// ``sendMessages(streamID:topicID:partitioning:messages:)``,
    /// ``pollMessages(streamID:topicID:partitionID:consumer:strategy:count:autoCommit:)``,
    /// and the offset calls. A raw send with balanced or key partitioning
    /// fails with ``IggyErrorCode/featureUnavailable``. A group poll, a store,
    /// or a delete that names no partition fails with
    /// ``IggyErrorCode/invalidIdentifier``, and a partition payload that does
    /// not decode fails with ``IggyErrorCode/invalidCommand``. Each fails
    /// before anything is sent.
    public func sendRawRequest(code: UInt32, payload: [UInt8]) async throws -> [UInt8] {
        guard !CommandCode.isSessionControl(code) else {
            throw IggyError(.invalidCommand, context: "code \(code) controls the session; use login and logout")
        }
        let lineage = await core.lineage
        if let routed = try await sendRawPartitionRequest(code: code, payload: payload, lineage: lineage) {
            return routed
        }
        return try await core.exchange(code: code, payload: payload, lineage: lineage)
    }

    // MARK: Internals shared by the domain extensions

    /// One request of a call that started under `lineage`, or, for a call
    /// of this one request, under the lineage it is sent in.
    func send(_ code: CommandCode, _ payload: [UInt8] = [], lineage: UInt64? = nil) async throws -> [UInt8] {
        try await core.exchange(code: code.rawValue, payload: payload, lineage: lineage)
    }

    /// Fails unless the client is signed in, and returns the lineage the
    /// call started under, which every request of the call carries.
    func requireAuthenticated() async throws -> UInt64 {
        try await core.requireAuthenticated()
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
        let lineage = try await requireAuthenticated()
        return try Responses.clientDetails(try await send(.getMe, lineage: lineage)[...])
    }

    /// A connected client by its server-assigned id, or nil if unknown.
    public func getClient(id: UInt32) async throws -> ClientInfoDetails? {
        let lineage = try await requireAuthenticated()
        let body = try await send(.getClient, Requests.getClient(clientID: id), lineage: lineage)
        return body.isEmpty ? nil : try Responses.clientDetails(body[...])
    }

    /// Every connected client.
    public func getClients() async throws -> [ClientInfo] {
        let lineage = try await requireAuthenticated()
        let body = try await send(.getClients, lineage: lineage)
        return body.isEmpty ? [] : try Responses.clients(body[...])
    }

    /// The option catalog of a resource scope: every key a client can name,
    /// its kind, what an update may do with it, and this server's default.
    public func describeOptions(scope: OptionsScope) async throws -> [OptionSpec] {
        let lineage = try await requireAuthenticated()
        return try Responses.optionSpecs(try await send(.describeOptions, Requests.describeOptions(scope: scope), lineage: lineage)[...])
    }

    /// Captures a snapshot archive of the server state.
    public func snapshot(compression: SnapshotCompression = .deflated, types: [SystemSnapshotType] = [.all]) async throws -> Snapshot {
        let lineage = try await requireAuthenticated()
        return Snapshot(data: try await send(.getSnapshotFile, try Requests.getSnapshot(compression: compression, types: types), lineage: lineage))
    }

    /// The cluster roster: every node, its role, and its endpoints. Requires
    /// a signed-in session.
    public func getClusterMetadata() async throws -> ClusterMetadata {
        try Responses.clusterMetadata(try await send(.getClusterMetadata)[...])
    }
}
