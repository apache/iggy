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

import Testing

@testable import Iggy

private let rootLogin = Credentials.usernamePassword(username: "iggy", password: "iggy")
private let tokenLogin = Credentials.personalAccessToken("iggy-token")

/// A request timeout short enough for a test to wait it out.
private let shortTimeout: Duration = .milliseconds(500)

private let sessionEnds = FakeVSRServer.sessionEndingEvictions.map { FakeVSRServer.Reply.eviction($0) }

/// The replicated writes a client sends over its own connection: a metadata
/// write, a batch, an offset store, and a segment deletion, which is neither
/// a metadata nor a partition operation.
private let replicatedWrites: [CommandCode] = [.deleteStream, .sendMessages, .storeConsumerOffset, .deleteSegments]

/// A client on the fake server with fast reconnection. The request timeout is
/// generous unless the test is about running into it, so a slow machine does
/// not cut a reconnect short.
private func makeClient(
    _ server: FakeVSRServer, autoLogin: Credentials? = rootLogin, reconnection: Bool = true, maxRetries: UInt32? = nil,
    requestTimeout: Duration = .seconds(10), heartbeat: Duration = .seconds(60), connector: any IggyConnector = NIOConnector()
) -> IggyClient {
    var configuration = ClientConfiguration(address: server.address, autoLogin: autoLogin)
    configuration.reconnection = ReconnectionOptions(enabled: reconnection, maxRetries: maxRetries, interval: .milliseconds(20), reestablishAfter: .zero)
    configuration.requestTimeout = requestTimeout
    configuration.connectTimeout = .seconds(2)
    configuration.heartbeatInterval = heartbeat
    return IggyClient(configuration: configuration, connector: connector, logger: testLogger)
}

/// A dialer that holds every dial until it is released, to stop a client in
/// the middle of one.
private struct GatedConnector: IggyConnector {
    let released = Locked(false)

    func connect(to endpoint: Endpoint, tls: TLSOptions?, timeout: Duration, noDelay: Bool) async throws -> any IggyConnection {
        while !released.withLock({ $0 }) {
            try await Task.sleep(for: .milliseconds(2))
        }
        return try await NIOConnector().connect(to: endpoint, tls: tls, timeout: timeout, noDelay: noDelay)
    }
}

/// Holds the first connection's close after the socket is closed, so a new
/// sign-in can finish before an older failure finishes its cleanup.
private struct GatedCloseConnector: IggyConnector {
    let closed = Locked(false)
    let released = Locked(false)
    private let used = Locked(false)

    func connect(to endpoint: Endpoint, tls: TLSOptions?, timeout: Duration, noDelay: Bool) async throws -> any IggyConnection {
        let connection = try await NIOConnector().connect(to: endpoint, tls: tls, timeout: timeout, noDelay: noDelay)
        let first = used.withLock { used in
            let first = !used
            used = true
            return first
        }
        return first ? Connection(connection, closed: closed, released: released) : connection
    }

    private final class Connection: IggyConnection {
        let underlying: any IggyConnection
        let closed: Locked<Bool>
        let released: Locked<Bool>
        var localAddress: String? { underlying.localAddress }

        init(_ underlying: any IggyConnection, closed: Locked<Bool>, released: Locked<Bool>) {
            self.underlying = underlying
            self.closed = closed
            self.released = released
        }

        func exchange(_ frame: [UInt8], timeout: Duration) async throws -> RawFrame {
            try await underlying.exchange(frame, timeout: timeout)
        }

        func close() async {
            await underlying.close()
            closed.withLock { $0 = true }
            while !released.withLock({ $0 }) { try? await Task.sleep(for: .milliseconds(2)) }
        }
    }
}

private func operations(_ server: FakeVSRServer) async -> [ConsensusOperation] {
    await server.requests.map(\.operation)
}

private func codes(_ server: FakeVSRServer) async -> [UInt32?] {
    await server.requests.map(\.code)
}

private func pingCount(_ server: FakeVSRServer) async -> Int {
    await codes(server).filter { $0 == CommandCode.ping.rawValue }.count
}

@Suite("Client transport", .serialized)
struct ClientTransportTests {
    @Test(arguments: [false, true])
    func aCallStartedBeforeDisconnectEndsUnauthenticated(singleAttempt: Bool) async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = makeClient(server)
        defer { Task { await client.shutdown() } }
        try await client.connect()
        let lineage = await client.core.lineage
        await client.disconnect()

        await expectCode(.unauthenticated) {
            if singleAttempt {
                _ = try await client.core.exchangeOnce(code: CommandCode.ping.rawValue, payload: [], context: .empty, lineage: lineage)
            } else {
                _ = try await client.core.exchange(code: CommandCode.ping.rawValue, payload: [], lineage: lineage)
            }
        }
        await expectCode(.notConnected) { try await client.ping() }
        await expectCode(.notConnected) { _ = try await client.getMe() }
        #expect(await pingCount(server) == 0)
    }

    @Test func aCallWaitingForReconnectEndsUnauthenticatedWhenTheCallerDisconnects() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = makeClient(server)
        defer { Task { await client.shutdown() } }
        try await client.connect()
        await client.core.disconnectTransport()
        let signingIn = Locked(false)
        let released = Locked(false)
        await server.respond { request in
            if request.code == CommandCode.bindSession.rawValue { return .status(IggyErrorCode.unauthenticated.rawValue) }
            if request.operation == .register {
                signingIn.withLock { $0 = true }
                _ = await eventually { released.withLock { $0 } }
            }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        let call = Task { try await client.getStreams() }
        #expect(await eventually { signingIn.withLock { $0 } })
        await client.disconnect()
        released.withLock { $0 = true }

        await expectCode(.unauthenticated) { _ = try await call.value }
        #expect(await server.requests.allSatisfy { $0.code != CommandCode.getStreams.rawValue })
    }

    @Test func aDisconnectDuringAStalledReconnectEndsTheRequestUnauthenticated() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let connector = GatedConnector()
        connector.released.withLock { $0 = true }
        let client = makeClient(server, requestTimeout: shortTimeout, connector: connector)
        defer { Task { await client.shutdown() } }
        try await client.connect()
        connector.released.withLock { $0 = false }
        await client.core.disconnectTransport()
        let call = Task { try await client.ping() }
        #expect(await eventually { await client.state == .connecting })
        await client.disconnect()

        // The dial is still held when the deadline ends the wait for it.
        await expectCode(.unauthenticated) { try await call.value }
        connector.released.withLock { $0 = true }
        #expect(await pingCount(server) == 0)
    }

    @Test func loginBindsTheSessionAndSequencesRequests() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = makeClient(server)
        try await client.connect()
        #expect(await client.state == .authenticated)
        try await client.ping()
        try await client.deleteStream(1)
        try await client.deleteStream(2)
        try await client.ping()
        let requests = await server.requests
        #expect(requests.map(\.operation) == [.register, .nonReplicated, .nonReplicated, .deleteStream, .deleteStream, .nonReplicated])
        #expect(requests.map(\.requestID) == Array(0...5))
        #expect(requests[0].session == 0 && requests[0].requestID == 0 && requests[0].checksum != 0)
        #expect(requests[1].code == CommandCode.getClusterMetadata.rawValue)
        #expect(requests[2].code == CommandCode.ping.rawValue && requests[2].session == 1)
        #expect(requests[3].session == 1 && requests[3].checksum != XXH3.hash64(requests[3].payload))
        #expect(requests[3].checksum != requests[4].checksum)
        #expect(requests[5].checksum == 0)
        #expect(requests.allSatisfy { $0.clientID == requests[0].clientID })
        #expect(await client.serverAddress == server.address)
        #expect(await client.clientAddress?.hasPrefix("127.0.0.1:") == true)
        await client.shutdown()
    }

    @Test func manualLoginWithoutAutoLogin() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = makeClient(server, autoLogin: nil)
        try await client.connect()
        #expect(await client.state == .connected)
        try await client.ping()
        await expectCode(.unauthenticated) { _ = try await client.getStreams() }
        let identity = try await client.login(username: "iggy", password: "iggy")
        #expect(identity.userID == 0)
        #expect(await client.state == .authenticated)
        // A re-login on a bound session logs out first, then registers under
        // a fresh client id.
        let before = await server.requests.last!.clientID
        _ = try await client.login(username: "iggy", password: "iggy")
        let ops = await operations(server)
        #expect(ops.suffix(3) == [.logout, .register, .nonReplicated])
        #expect(await server.requests.last!.clientID != before)
        try await client.logout()
        #expect(await client.state == .connected)
        await client.shutdown()
    }

    @Test(
        arguments: [
            (FakeVSRServer.Reply.status(IggyErrorCode.invalidCredentials.rawValue), IggyErrorCode.invalidCredentials),
            (.status(IggyErrorCode.invalidPersonalAccessToken.rawValue), .invalidPersonalAccessToken),
            (.status(IggyErrorCode.personalAccessTokenExpired.rawValue), .personalAccessTokenExpired),
            (.status(IggyErrorCode.userInactive.rawValue), .userInactive),
            (.stall, .transientNotAccepted),
        ], [false, true])
    func aFailedExplicitSignInDisablesAutomaticFallback(_ failure: (FakeVSRServer.Reply, IggyErrorCode), token: Bool) async throws {
        let (reply, code) = failure
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = makeClient(server, requestTimeout: shortTimeout)
        defer { Task { await client.shutdown() } }
        try await client.connect()
        await client.core.disconnectTransport()
        await server.respond { request in
            request.operation == .register ? reply : FakeVSRServer.defaultReply(request, address: server.address)
        }

        await expectCode(code) {
            if token {
                _ = try await client.login(personalAccessToken: "caller-token")
            } else {
                _ = try await client.login(username: "other", password: "secret")
            }
        }
        let before = await server.requests.filter { $0.operation == .register }
        #expect(before.count == 2)
        #expect(before.last.flatMap { Self.username(in: $0.payload) } == (token ? "caller-token" : "other"))
        if !token { #expect(before.last.flatMap { Self.password(in: $0.payload) } == "secret") }
        await server.respond { FakeVSRServer.defaultReply($0, address: server.address) }
        await client.core.disconnectTransport()

        try await client.connect()
        await expectCode(.unauthenticated) { _ = try await client.getStreams() }
        #expect(await server.requests.filter { $0.operation == .register }.count == before.count)
        await client.disconnect()
        try await client.connect()
        await expectCode(.unauthenticated) { _ = try await client.getStreams() }
        #expect(await server.requests.filter { $0.operation == .register }.count == before.count)
        _ = try await client.login(username: "recovered", password: "secret")
        await Self.forgetSessions(on: server)
        await client.core.disconnectTransport()
        try await client.ping()
        let after = await server.requests.filter { $0.operation == .register }
        #expect(after.suffix(2).map { Self.username(in: $0.payload) } == ["recovered", "recovered"])
    }

    @Test func aValidationFailureWithdrawsConfiguredAndRememberedSignIn() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = makeClient(server)
        defer { Task { await client.shutdown() } }
        try await client.connect()
        _ = try await client.login(username: "remembered", password: "secret")

        await expectCode(.invalidUsername) { _ = try await client.login(username: "", password: "secret") }
        await client.core.disconnectTransport()
        try await client.connect()

        await expectCode(.unauthenticated) { _ = try await client.getStreams() }
        #expect(await server.requests.filter { $0.operation == .register }.count == 2)
    }

    @Test func anExplicitSignInSupersedesAnAutomaticIdentity() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let received = Locked(false)
        let released = Locked(false)
        let bound = Locked<[Int: UInt32]>([:])
        let explicitUser: UInt32 = 7
        await server.respond { request in
            if request.operation == .logout {
                bound.withLock { $0[request.connection] = nil }
            }
            if request.code == CommandCode.bindSession.rawValue { return .status(IggyErrorCode.unauthenticated.rawValue) }
            if request.operation == .register {
                let automatic = Self.username(in: request.payload) == "iggy"
                if automatic {
                    received.withLock { $0 = true }
                    while !released.withLock({ $0 }) { try? await Task.sleep(for: .milliseconds(2)) }
                }
                let user = bound.withLock { users in
                    if let user = users[request.connection] { return user }
                    let user: UInt32 = automatic ? 0 : explicitUser
                    users[request.connection] = user
                    return user
                }
                return .ok(FakeVSRServer.registerBody(userID: user))
            }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = makeClient(server)
        defer { Task { await client.shutdown() } }
        let automatic = Task { try await client.connect() }
        #expect(await eventually { received.withLock { $0 } })
        let started = await client.core.lineage
        let explicit = Task { try await client.login(username: "other", password: "secret") }
        #expect(await eventually { await client.core.lineage != started })
        released.withLock { $0 = true }

        await expectCode(.unauthenticated) { try await automatic.value }
        #expect(try await explicit.value.userID == explicitUser)
        await client.core.disconnectTransport()
        try await client.ping()
        let registers = await server.requests.filter { $0.operation == .register }
        #expect(registers.map { Self.username(in: $0.payload) } == ["iggy", "other", "other"])
    }

    @Test(arguments: [false, true])
    func olderAndQueuedSignInsCannotRestoreCredentialsAfterANewerFailure(automatic: Bool) async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = makeClient(server, autoLogin: automatic ? rootLogin : nil)
        defer { Task { await client.shutdown() } }
        if !automatic { try await client.connect() }
        let received = Locked(false)
        let released = Locked(false)
        await server.respond { request in
            if request.operation == .register {
                let username = Self.username(in: request.payload)
                if username == "iggy" || username == "older" {
                    received.withLock { $0 = true }
                    while !released.withLock({ $0 }) { try? await Task.sleep(for: .milliseconds(2)) }
                }
            }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        let older = Task {
            if automatic { try await client.connect() } else { _ = try await client.login(username: "older", password: "secret") }
        }
        #expect(await eventually { received.withLock { $0 } })
        let started = await client.core.lineage
        let queued = Task { _ = try await client.login(username: "queued", password: "secret") }
        #expect(await eventually { await client.core.lineage != started })

        await expectCode(.invalidPassword) { _ = try await client.login(username: "newest", password: "") }
        released.withLock { $0 = true }
        await expectCode(.unauthenticated) { try await older.value }
        await expectCode(.unauthenticated) { try await queued.value }
        await client.core.disconnectTransport()
        try await client.connect()

        await expectCode(.unauthenticated) { _ = try await client.getStreams() }
        let registers = await server.requests.filter { $0.operation == .register }
        #expect(registers.count == 1)
        #expect(!registers.contains { Self.username(in: $0.payload) == "queued" })
    }

    @Test(
        arguments: [
            (FakeVSRServer.Reply.status(IggyErrorCode.invalidCredentials.rawValue), Optional(IggyErrorCode.invalidCredentials)),
            (.close, .disconnected),
            (.stall, .disconnected),
            (.stall, nil),
        ])
    func aFailedExplicitLeaderSettlementDiscardsTheRegisteredSession(_ failure: (FakeVSRServer.Reply, IggyErrorCode?)) async throws {
        let (reply, code) = failure
        let follower = try await FakeVSRServer.started()
        let leader = try await FakeVSRServer.started()
        defer {
            Task {
                await follower.stop()
                await leader.stop()
            }
        }
        let binding = Locked(false)
        let roster = FakeVSRServer.rosterBody([follower.address, leader.address], leaderIndex: 1)
        for server in [follower, leader] {
            await server.respond { request in
                if request.code == CommandCode.bindSession.rawValue {
                    binding.withLock { $0 = true }
                    return reply
                }
                if request.code == CommandCode.getClusterMetadata.rawValue { return .ok(roster) }
                return FakeVSRServer.defaultReply(request, address: server.address)
            }
        }
        let client = makeClient(follower, autoLogin: nil, reconnection: false, requestTimeout: shortTimeout)
        defer { Task { await client.shutdown() } }
        try await client.connect()
        let login = Task { try await client.login(username: "caller", password: "secret") }
        #expect(await eventually { binding.withLock { $0 } })
        if let code {
            await expectCode(code) { _ = try await login.value }
        } else {
            login.cancel()
            await #expect(throws: CancellationError.self) { _ = try await login.value }
        }
        #expect(await client.core.sessionIdentity(at: 0) == nil)
        #expect(await eventually { await follower.openConnectionCount + leader.openConnectionCount == 0 })
        let before = await follower.requests + leader.requests
        #expect(before.filter { $0.operation == .register }.count == 1)
        #expect(before.contains { $0.code == CommandCode.bindSession.rawValue })
        for server in [follower, leader] {
            await server.respond { FakeVSRServer.defaultReply($0, address: server.address) }
        }

        try await client.connect()
        await expectCode(.unauthenticated) { _ = try await client.getStreams() }
        try await client.ping()
        let after = await follower.requests + leader.requests
        #expect(after.filter { $0.operation == .register }.count == 1)
        #expect(after.filter { $0.code == CommandCode.bindSession.rawValue }.count == before.filter { $0.code == CommandCode.bindSession.rawValue }.count)
        #expect(after.filter { $0.code == CommandCode.ping.rawValue }.allSatisfy { $0.session == 0 })
        _ = try await client.login(username: "recovered", password: "secret")
        try await client.deleteStream(1)
    }

    @Test func aFailedExplicitSignInClosesTheOldSessionWithoutClaimingAnInflightWriteFailed() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = makeClient(server)
        defer { Task { await client.shutdown() } }
        try await client.connect()
        await server.respond { request in
            request.operation == .deleteStream ? .stall : FakeVSRServer.defaultReply(request, address: server.address)
        }
        let write = Task { try await client.deleteStream(1) }
        #expect(await eventually { await operations(server).contains(.deleteStream) })

        await expectCode(.invalidPassword) { _ = try await client.login(username: "other", password: "") }
        #expect(await client.core.sessionIdentity(at: 0) == nil)
        #expect(await eventually { await server.openConnectionCount == 0 })
        await expectCode(.transientNotCommitted) { try await write.value }
        try await client.connect()
        await expectCode(.unauthenticated) { _ = try await client.getStreams() }
        #expect(await operations(server).filter { $0 == .register }.count == 1)
        #expect(await codes(server).contains(CommandCode.bindSession.rawValue) == false)
    }

    @Test func anOlderExplicitFailureCannotDiscardANewerSuccessfulSession() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let connector = GatedCloseConnector()
        defer { connector.released.withLock { $0 = true } }
        let client = makeClient(server, connector: connector)
        defer { Task { await client.shutdown() } }
        try await client.connect()
        let failed = Task { try await client.login(username: "invalid", password: "") }
        #expect(await eventually { connector.closed.withLock { $0 } })

        _ = try await client.login(username: "newer", password: "secret")
        let newer = await client.core.sessionIdentity(at: 0)
        #expect(newer != nil)
        connector.released.withLock { $0 = true }
        await expectCode(.invalidPassword) { _ = try await failed.value }
        #expect(await client.state == .authenticated)
        #expect(await client.core.sessionIdentity(at: 0)?.clientID == newer?.clientID)
        try await client.deleteStream(1)
        #expect(await server.requests.last?.clientID == newer?.clientID)
        #expect(await server.requests.filter { $0.operation == .register }.last.flatMap { Self.username(in: $0.payload) } == "newer")
    }

    @Test func aBestEffortRosterRefusalStillLeavesExplicitSignInSuccessful() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await server.respond { request in
            request.code == CommandCode.getClusterMetadata.rawValue
                ? .status(IggyErrorCode.featureUnavailable.rawValue) : FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = makeClient(server, autoLogin: nil)
        defer { Task { await client.shutdown() } }
        try await client.connect()
        _ = try await client.login(username: "caller", password: "secret")
        #expect(await client.state == .authenticated)
        #expect(await client.core.sessionIdentity(at: 0) != nil)
        try await client.deleteStream(1)
        #expect(await operations(server).filter { $0 == .register }.count == 1)
    }

    @Test func rejectedCredentialsAreNotRetried() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await server.respond { request in
            request.operation == .register ? .eviction(.invalidCredentials) : .ok([])
        }
        let client = makeClient(server, autoLogin: nil)
        try await client.connect()
        await expectCode(.invalidCredentials) { _ = try await client.login(username: "iggy", password: "wrong") }
        // The eviction ended the connection, so the client is back to
        // disconnected rather than half signed in.
        #expect(await client.state != .authenticated)
        #expect(await operations(server).filter { $0 == .register }.count == 1)
        await client.shutdown()
    }

    @Test func committedRegisterRejectionIsTyped() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await server.respond { request in
            request.operation == .register ? .committedRejection(IggyErrorCode.userInactive.rawValue) : .ok([])
        }
        let client = makeClient(server, autoLogin: nil)
        try await client.connect()
        await expectCode(.userInactive) { _ = try await client.login(username: "iggy", password: "iggy") }
        await client.shutdown()
    }

    @Test func preCommitDenialsAreTyped() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await server.respond { request in
            switch request.operation {
            case .register: .ok(FakeVSRServer.registerBody())
            case .createStream: .status(IggyErrorCode.streamNameAlreadyExists.rawValue)
            case .nonReplicated where request.code == CommandCode.getStats.rawValue: .committedRejection(0)
            default: .ok([])
            }
        }
        let client = makeClient(server)
        try await client.connect()
        await expectCode(.streamNameAlreadyExists) { _ = try await client.createStream(name: "dup") }
        // The session survives a denial: the next request still goes out.
        try await client.ping()
        #expect(await client.state == .authenticated)
        await client.shutdown()
    }

    /// A read carries the newest metadata commit the client saw, as
    /// `read_context` in the Rust SDK stamps it, so a backup that lags holds
    /// the read until it applied the client's own write.
    @Test func aReadCarriesTheMetadataWatermark() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let commit: UInt64 = 42
        await server.respond { request in
            request.operation == .deleteStream ? .committed([], commit: commit) : FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = makeClient(server)
        try await client.connect()
        _ = try await client.getStreams()
        try await client.deleteStream(1)
        _ = try await client.getStreams()
        let reads = await server.requests.filter { $0.code == CommandCode.getStreams.rawValue }
        #expect(reads.map(\.context.metadataOp) == [0, commit])
        #expect(await server.requests.first { $0.operation == .deleteStream }?.context == .empty)
        await client.shutdown()
    }

    @Test func anUncommittedMetadataWriteFollowsTheNewPrimaryWithItsFrame() async throws {
        let first = try await FakeVSRServer.started()
        let second = try await FakeVSRServer.started()
        defer {
            Task {
                await first.stop(); await second.stop()
            }
        }
        let attempted = Locked(false)
        let moved = Locked(false)
        let addresses = [first.address, second.address]
        await first.respond { request in
            if request.code == CommandCode.getClusterMetadata.rawValue {
                return .ok(FakeVSRServer.rosterBody(addresses, leaderIndex: moved.withLock { $0 } ? 1 : 0))
            }
            if request.operation == .deleteStream {
                if !attempted.exchange(true) { return .status(IggyErrorCode.transientNotCommitted.rawValue) }
                moved.withLock { $0 = true }
                return .status(IggyErrorCode.notPrimary.rawValue)
            }
            return FakeVSRServer.defaultReply(request, address: first.address)
        }
        await second.respond { request in
            request.code == CommandCode.getClusterMetadata.rawValue
                ? .ok(FakeVSRServer.rosterBody(addresses, leaderIndex: 1))
                : FakeVSRServer.defaultReply(request, address: second.address)
        }
        let client = makeClient(first, requestTimeout: .seconds(2))
        defer { Task { await client.shutdown() } }
        try await client.connect()

        try await client.deleteStream(1)

        let previous = await first.requests.filter { $0.operation == .deleteStream }
        let next = await second.requests.filter { $0.operation == .deleteStream }
        #expect(previous.count == 2)
        #expect(next.count == 1)
        #expect(Set((previous + next).map(\.requestID)).count == 1)
        #expect(Set((previous + next).map(\.clientID)).count == 1)
        #expect(Set((previous + next).map(\.session)).count == 1)
        #expect(Set((previous + next).map(\.checksum)).count == 1)
    }

    @Test func internalSessionAndRosterFramesKeepTheirEmptyContext() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let commit: UInt64 = 42
        await server.respond { request in
            request.operation == .deleteStream ? .committed([], commit: commit) : FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = makeClient(server)
        defer { Task { await client.shutdown() } }
        try await client.connect()
        try await client.deleteStream(1)
        await client.core.disconnectTransport()
        try await client.ping()
        _ = try await client.getStreams()

        let frames = await server.requests
        let internalFrames = frames.filter { $0.code == CommandCode.bindSession.rawValue || $0.code == CommandCode.getClusterMetadata.rawValue }
        #expect(internalFrames.contains { $0.code == CommandCode.bindSession.rawValue })
        #expect(internalFrames.allSatisfy { $0.context == .empty })
        #expect(frames.last { $0.code == CommandCode.getStreams.rawValue }?.context.metadataOp == commit)
    }

    @Test(arguments: [CommandCode.ping, .getStreams])
    func aCallerInterruptedSignInIsNotReportedAsAServerRefusal(_ waiting: CommandCode) async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = makeClient(server)
        defer { Task { await client.shutdown() } }
        try await client.connect()
        await client.core.disconnectTransport()
        let signingIn = Locked(false)
        let released = Locked(false)
        await server.respond { request in
            if request.code == CommandCode.bindSession.rawValue { return .status(IggyErrorCode.unauthenticated.rawValue) }
            if request.operation == .register, Self.username(in: request.payload) == "iggy" {
                signingIn.withLock { $0 = true }
                while !released.withLock({ $0 }) { try? await Task.sleep(for: .milliseconds(2)) }
                return .status(IggyErrorCode.transientNotAccepted.rawValue)
            }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        let lineage = await client.core.lineage
        let call = Task {
            if waiting == .ping { try await client.ping() } else { _ = try await client.getStreams() }
        }
        #expect(await eventually { signingIn.withLock { $0 } })
        let login = Task { _ = try await client.login(username: "other", password: "secret") }
        #expect(await eventually { await client.core.lineage != lineage })
        released.withLock { $0 = true }
        do {
            try await call.value
            Issue.record("the call continued after the caller signed in")
        } catch let error as IggyError {
            #expect(error.code == .unauthenticated)
            #expect(error.context?.contains("automatic sign-in was refused") == false)
            #expect(error.context?.contains("caller signed in or out") == true)
        }
        try await login.value
    }

    @Test func aRepeatedlyDetachedReadWaitsBetweenReconnects() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let attempts = Locked<[ContinuousClock.Instant]>([])
        await server.respond { request in
            if request.code == CommandCode.getStreams.rawValue {
                let count = attempts.withLock {
                    $0.append(.now); return $0.count
                }
                if count < 3 { return .eviction(.noSession) }
            }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = makeClient(server)
        defer { Task { await client.shutdown() } }
        try await client.connect()

        _ = try await client.getStreams()

        let times = attempts.withLock { $0 }
        #expect(times.count == 3)
        #expect(times[1] - times[0] >= PollRouter.retryInterval)
        #expect(times[2] - times[1] >= PollRouter.retryInterval * 2)
    }

    @Test func transientNotCommittedReplaysTheSameRequestID() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let counter = Locked(0)
        await server.respond { request in
            if let reply = FakeVSRServer.sessionReply(request) { return reply }
            if request.operation == .deleteStream {
                let seen = counter.withLock {
                    $0 += 1; return $0
                }
                return seen < 3 ? .status(IggyErrorCode.transientNotCommitted.rawValue) : .ok([])
            }
            return .ok([])
        }
        let client = makeClient(server)
        try await client.connect()
        try await client.deleteStream(1)
        let creates = await server.requests.filter { $0.operation == .deleteStream }
        #expect(creates.count == 3)
        #expect(Set(creates.map(\.requestID)).count == 1)
        #expect(Set(creates.map(\.connection)).count == 1)
        #expect(await server.connectionCount == 1)
        await client.shutdown()
    }

    /// A write answered "not committed" may still commit. When a replay then
    /// loses its connection, the client reconnects, binds the same session,
    /// and replays the write under its own request id until the deadline, as
    /// the Rust SDK does, instead of ending it as unknown at once.
    @Test(arguments: [1, 2])
    func anUncommittedWriteReplaysAcrossLostConnections(_ losses: Int) async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let attempts = Locked(0)
        await server.respond { request in
            guard request.operation == .deleteStream else {
                return FakeVSRServer.defaultReply(request, address: server.address)
            }
            let attempt = attempts.withLock {
                $0 += 1; return $0
            }
            if attempt == 1 {
                return .status(IggyErrorCode.transientNotCommitted.rawValue)
            }
            return attempt <= losses + 1 ? .close : .ok([])
        }
        let client = makeClient(server)
        try await client.connect()

        try await client.deleteStream(1)

        let deletes = await server.requests.filter { $0.operation == .deleteStream }
        #expect(deletes.count == losses + 2)
        #expect(Set(deletes.map(\.requestID)).count == 1)
        #expect(Set(deletes.map(\.clientID)).count == 1)
        #expect(await codes(server).filter { $0 == CommandCode.bindSession.rawValue }.count == losses)
        #expect(await operations(server).filter { $0 == .register }.count == 1)
        await client.shutdown()
    }

    @Test func transientNotAcceptedReplaysOnASingleNode() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let counter = Locked(0)
        await server.respond { request in
            if let reply = FakeVSRServer.sessionReply(request) { return reply }
            if request.operation == .nonReplicated, request.code == CommandCode.getClusterMetadata.rawValue {
                return .ok(FakeVSRServer.rosterBody([server.address]))
            }
            if request.operation == .deleteSegments {
                let seen = counter.withLock {
                    $0 += 1; return $0
                }
                return seen < 4 ? .status(IggyErrorCode.transientNotAccepted.rawValue) : .ok([])
            }
            return .ok([])
        }
        let client = makeClient(server)
        try await client.connect()
        try await client.deleteSegments(streamID: 1, topicID: 1, partitionID: 0, count: 1)
        let sends = await server.requests.filter { $0.operation == .deleteSegments }
        #expect(sends.count == 4)
        #expect(Set(sends.map(\.requestID)).count == 1)
        #expect(await server.connectionCount == 1)
        await client.shutdown()
    }

    /// The replay after the failover check is stamped afresh. The refused
    /// request frees the exchange lock for that check, a caller queued behind
    /// it spends the next request id, and the server refuses an id older than
    /// one it already applied.
    @Test func replayAfterTheFailoverCheckTakesANewRequestID() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let applied = Locked<UInt64>(0)
        await server.respond { request in
            if let reply = FakeVSRServer.sessionReply(request) { return reply }
            if request.operation == .nonReplicated, request.code == CommandCode.getClusterMetadata.rawValue {
                return .ok(FakeVSRServer.rosterBody([server.address]))
            }
            switch request.operation {
            case .deleteStream:
                applied.withLock { $0 = max($0, request.requestID) }
                return .ok([])
            case .deleteTopic:
                let highest = applied.withLock { $0 }
                if highest == 0 {
                    return .status(IggyErrorCode.transientNotAccepted.rawValue)
                }
                return request.requestID < highest ? .status(IggyErrorCode.requestTooOld.rawValue) : .ok([])
            default:
                return .ok([])
            }
        }
        let client = makeClient(server)
        try await client.connect()
        let refused = Task { try await client.deleteTopic(streamID: 1, topicID: 1) }
        #expect(await eventually { await operations(server).contains(.deleteTopic) })
        try await client.deleteStream(1)
        try await refused.value
        let attempts = await server.requests.filter { $0.operation == .deleteTopic }.map(\.requestID)
        let queued = await server.requests.first { $0.operation == .deleteStream }?.requestID ?? 0
        #expect(attempts.first.map { $0 < queued } == true)
        #expect(attempts.last.map { $0 > queued } == true)
        #expect(await server.connectionCount == 1)
        await client.shutdown()
    }

    /// After a view change a backup refuses a replicated write with "not
    /// primary". The write was never admitted, so it goes to the new primary
    /// at once instead of being replayed on the backup.
    @Test func notPrimaryMovesTheRequestToTheNewPrimary() async throws {
        let first = try await FakeVSRServer.started()
        let second = try await FakeVSRServer.started()
        defer {
            Task {
                await first.stop()
                await second.stop()
            }
        }
        let addresses = [first.address, second.address]
        let viewChanged = Locked(false)
        await first.respond { request in
            if let reply = FakeVSRServer.sessionReply(request) { return reply }
            if request.code == CommandCode.getClusterMetadata.rawValue {
                return .ok(FakeVSRServer.rosterBody(addresses, leaderIndex: viewChanged.withLock { $0 } ? 1 : 0))
            }
            guard request.operation == .deleteStream else { return .ok([]) }
            viewChanged.withLock { $0 = true }
            return .status(IggyErrorCode.notPrimary.rawValue)
        }
        await second.respond { request in
            if let reply = FakeVSRServer.sessionReply(request) { return reply }
            if request.code == CommandCode.getClusterMetadata.rawValue { return .ok(FakeVSRServer.rosterBody(addresses, leaderIndex: 1)) }
            return .ok([])
        }
        let client = makeClient(first)
        try await client.connect()
        let clock = ContinuousClock()
        let elapsed = try await clock.measure { try await client.deleteStream(1) }
        #expect(elapsed < ClientCore.transientFailoverCheckInterval)
        #expect(await first.requests.filter { $0.operation == .deleteStream }.count == 1)
        #expect(await second.requests.filter { $0.operation == .deleteStream }.count == 1)
        #expect(await client.serverAddress == second.address)
        await client.shutdown()
    }

    /// A write whose first attempt may have committed stays uncertain when
    /// the failover after a later "not primary" fails: every node refusing
    /// the sign-in mid view change must not turn it into "not accepted",
    /// which a caller may resend under a new request id.
    @Test func aFailedFailoverKeepsAnUncertainWriteUncertain() async throws {
        let first = try await FakeVSRServer.started()
        let second = try await FakeVSRServer.started()
        defer {
            Task {
                await first.stop()
                await second.stop()
            }
        }
        let addresses = [first.address, second.address]
        let viewChanging = Locked(false)
        let attempts = Locked(0)
        let refuseSignIn: @Sendable (FakeVSRServer.Request) -> FakeVSRServer.Reply? = { request in
            guard request.operation == .register || request.code == CommandCode.bindSession.rawValue else { return nil }
            return viewChanging.withLock { $0 } ? .status(IggyErrorCode.transientNotAccepted.rawValue) : FakeVSRServer.sessionReply(request)
        }
        await first.respond { request in
            if let reply = refuseSignIn(request) { return reply }
            if request.code == CommandCode.getClusterMetadata.rawValue {
                return .ok(FakeVSRServer.rosterBody(addresses, leaderIndex: viewChanging.withLock { $0 } ? 1 : 0))
            }
            guard request.operation == .deleteStream else { return .ok([]) }
            let attempt = attempts.withLock {
                $0 += 1; return $0
            }
            if attempt == 1 {
                return .status(IggyErrorCode.transientNotCommitted.rawValue)
            }
            viewChanging.withLock { $0 = true }
            return .status(IggyErrorCode.notPrimary.rawValue)
        }
        await second.respond { request in
            if let reply = refuseSignIn(request) { return reply }
            if request.code == CommandCode.getClusterMetadata.rawValue { return .ok(FakeVSRServer.rosterBody(addresses, leaderIndex: 1)) }
            return .ok([])
        }
        // One reconnect attempt, so the sweep gives up before the deadline.
        let client = makeClient(first, maxRetries: 1)
        try await client.connect()
        await expectCode(.transientNotCommitted) { try await client.deleteStream(1) }
        #expect(await first.requests.filter { $0.operation == .deleteStream }.count >= 2)
        #expect(await second.requests.filter { $0.operation == .deleteStream }.isEmpty)
        await client.shutdown()
    }

    /// With nowhere to move, "not primary" is reported like "not accepted":
    /// neither was admitted.
    @Test func notPrimaryWithoutAnotherNodeIsReportedAsNotAccepted() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await server.respond { request in
            if let reply = FakeVSRServer.sessionReply(request) { return reply }
            if request.code == CommandCode.getClusterMetadata.rawValue { return .ok(FakeVSRServer.rosterBody([server.address])) }
            return request.operation == .deleteStream ? .status(IggyErrorCode.notPrimary.rawValue) : .ok([])
        }
        let client = makeClient(server, requestTimeout: shortTimeout)
        try await client.connect()
        await expectCode(.transientNotAccepted) { try await client.deleteStream(1) }
        let attempts = await server.requests.filter { $0.operation == .deleteStream }
        #expect(attempts.count > 1)
        #expect(Set(attempts.map(\.requestID)).count == attempts.count)
        #expect(await client.state == .authenticated)
        await client.shutdown()
    }

    @Test func transientReplayGivesUpAtTheDeadline() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await server.respond { request in
            request.operation == .register
                ? .ok(FakeVSRServer.registerBody()) : request.operation == .createStream ? .status(IggyErrorCode.transientNotCommitted.rawValue) : .ok([])
        }
        let client = makeClient(server, requestTimeout: shortTimeout)
        try await client.connect()
        let clock = ContinuousClock()
        let elapsed = await clock.measure {
            await expectCode(.transientNotCommitted) { _ = try await client.createStream(name: "x") }
        }
        #expect(elapsed >= .milliseconds(450) && elapsed < .seconds(10))
        await client.shutdown()
    }

    /// A reply that never comes spends the whole deadline, so the request
    /// fails without waiting for a reconnect. The stream is at an unknown
    /// boundary: the connection is dropped and the next request dials again.
    @Test func requestTimeoutClosesTheConnectionAndTheNextRequestReconnects() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let stallOnce = Locked(true)
        await server.respond { request in
            if let reply = FakeVSRServer.sessionReply(request) { return reply }
            if request.operation == .nonReplicated, request.code == CommandCode.getStats.rawValue,
                stallOnce.withLock({
                    let stall = $0; $0 = false; return stall
                })
            {
                return .stall
            }
            return .ok([])
        }
        let client = makeClient(server, requestTimeout: shortTimeout)
        try await client.connect()
        let clock = ContinuousClock()
        let elapsed = await clock.measure {
            await expectCode(.transientNotAccepted) { _ = try await client.getStats() }
        }
        #expect(elapsed >= .milliseconds(450) && elapsed < .seconds(10))
        #expect(await client.state == .disconnected)
        #expect(await server.connectionCount == 1)
        try await client.ping()
        #expect(await server.connectionCount == 2)
        // The new connection resumes the session instead of registering one.
        #expect(await operations(server).filter { $0 == .register }.count == 1)
        #expect(await codes(server).filter { $0 == CommandCode.bindSession.rawValue }.count == 1)
        #expect(await client.state == .authenticated)
        await client.shutdown()
    }

    /// With unlimited retries a reconnect to a server that is gone never
    /// ends. The request that started it still has to return at its deadline,
    /// refused as not accepted since it was never sent.
    @Test func requestDeadlineCoversTheReconnect() async throws {
        let server = try await FakeVSRServer.started()
        let client = makeClient(server, requestTimeout: shortTimeout)
        try await client.connect()
        await server.stop()
        let clock = ContinuousClock()
        let elapsed = await clock.measure {
            // The server closed the connection first, so the ping was never
            // written.
            await expectCode(.transientNotAccepted) { try await client.ping() }
        }
        #expect(elapsed >= .milliseconds(450) && elapsed < .seconds(10))
        // No caller waits for the sweep any more, so it ends too.
        #expect(await eventually { await client.state == .disconnected })
        await client.shutdown()
    }

    /// An attempt answered with "not committed" may have committed. A later
    /// "not accepted" for the same request does not prove otherwise, so it
    /// must not send the request to the next node under a new session, where
    /// the server cannot recognise it as a repeat.
    @Test func uncertainOutcomeIsNotReissuedOnAnotherNode() async throws {
        let first = try await FakeVSRServer.started()
        let second = try await FakeVSRServer.started()
        defer {
            Task {
                await first.stop()
                await second.stop()
            }
        }
        let roster = FakeVSRServer.rosterBody([first.address, second.address], leaderIndex: 0)
        let seen = Locked(0)
        await first.respond { request in
            if let reply = FakeVSRServer.sessionReply(request) { return reply }
            if request.code == CommandCode.getClusterMetadata.rawValue { return .ok(roster) }
            guard request.operation == .createStream else { return .ok([]) }
            let count = seen.withLock {
                $0 += 1; return $0
            }
            return .status((count == 1 ? IggyErrorCode.transientNotCommitted : .transientNotAccepted).rawValue)
        }
        await second.respond { request in
            if let reply = FakeVSRServer.sessionReply(request) { return reply }
            if request.code == CommandCode.getClusterMetadata.rawValue { return .ok(roster) }
            return .ok([])
        }
        let client = makeClient(first, requestTimeout: ClientCore.transientFailoverCheckInterval + .seconds(1))
        try await client.connect()
        await #expect(throws: IggyError.self) { _ = try await client.createStream(name: "once") }
        let attempts = await first.requests.filter { $0.operation == .createStream }
        #expect(attempts.count > 1)
        #expect(Set(attempts.map(\.requestID)).count == 1)
        #expect(Set(attempts.map(\.clientID)).count == 1)
        #expect(await second.requests.filter { $0.operation == .createStream }.isEmpty)
        await client.shutdown()
    }

    @Test func misroutedReplyDesyncsAndReconnects() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let once = Locked(true)
        await server.respond { request in
            if let reply = FakeVSRServer.sessionReply(request) { return reply }
            if request.code == CommandCode.ping.rawValue,
                once.withLock({
                    let first = $0; $0 = false; return first
                })
            {
                return .wrongRequestID(request.requestID + 7)
            }
            return .ok([])
        }
        let client = makeClient(server)
        try await client.connect()
        try await client.ping()
        #expect(await server.connectionCount == 2)
        #expect(await codes(server).filter { $0 == CommandCode.ping.rawValue }.count == 2)
        await client.shutdown()
    }

    /// A write can commit before an eviction ends its session, as the Rust
    /// SDK assumes. When the session cannot be bound again, a new session
    /// cannot replay the write safely: the call ends with the unknown outcome
    /// after one send, and the next write signs in again.
    @Test(arguments: FakeVSRServer.sessionEndingEvictions, replicatedWrites)
    func aWriteEvictedWithItsSessionIsSentOnceAndStaysUnknown(_ reason: VSRFrame.EvictionReason, _ write: CommandCode) async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let evicted = Locked(false)
        await server.respond { request in
            if request.code == CommandCode.bindSession.rawValue { return .status(IggyErrorCode.unauthenticated.rawValue) }
            if request.code == write.rawValue, !evicted.exchange(true) {
                return .eviction(reason)
            }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = makeClient(server)
        try await client.connect()

        await expectCode(.transientNotCommitted) { try await Self.issue(write, on: client) }

        #expect(await server.requests.filter { $0.code == write.rawValue }.count == 1)
        try await Self.issue(write, on: client)
        #expect(await operations(server).filter { $0 == .register }.count == 2)
        await client.shutdown()
    }

    /// The session of an evicted write that the client binds again answers
    /// the same request from its reply cache, so the write goes again as it
    /// was stamped, as the Rust SDK replays it after a rebind.
    @Test func aWriteEvictedWithItsSessionIsReplayedWhenTheSessionIsBoundAgain() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let evicted = Locked(false)
        await server.respond { request in
            if request.operation == .deleteStream, !evicted.exchange(true) {
                return .eviction(.noSession)
            }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = makeClient(server)
        try await client.connect()

        try await client.deleteStream(1)

        let deletes = await server.requests.filter { $0.operation == .deleteStream }
        #expect(deletes.count == 2)
        #expect(Set(deletes.map(\.requestID)).count == 1)
        #expect(Set(deletes.map(\.clientID)).count == 1)
        #expect(await operations(server).filter { $0 == .register }.count == 1)
        await client.shutdown()
    }

    /// A call made before the caller signs out and in as another user
    /// belongs to the old session. Between two refused attempts it learns
    /// that the caller did so: it ends unauthenticated and never reaches the
    /// server under the new session.
    @Test(arguments: [CommandCode.getStreams, .pollMessages])
    func aCallParkedWhileTheCallerSignsInAsAnotherUserEndsUnauthenticated(_ parked: CommandCode) async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let other = Locked<UInt128Value?>(nil)
        await server.respond { request in
            if request.operation == .register, Self.username(in: request.payload) == "other" {
                other.withLock { $0 = request.clientID }
            }
            if request.code == parked.rawValue, request.clientID != other.withLock({ $0 }) {
                return .status(IggyErrorCode.notPrimary.rawValue)
            }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = makeClient(server)
        try await client.connect()
        let call = Task {
            if parked == .pollMessages {
                _ = try await client.pollMessages(streamID: 1, topicID: 2, partitionID: 0, strategy: .first, count: 1, autoCommit: false)
            } else {
                _ = try await client.getStreams()
            }
        }
        #expect(await eventually { await server.requests.contains { $0.code == parked.rawValue } })

        try await client.logout()
        _ = try await client.login(username: "other", password: "secret")

        await expectCode(.unauthenticated) { try await call.value }
        let otherClientID = try #require(other.withLock { $0 })
        let underOther = await server.requests.filter {
            $0.clientID == otherClientID && ($0.code == parked.rawValue || $0.code == CommandCode.getPartitionRoute.rawValue)
        }
        #expect(underOther.isEmpty)
        await client.shutdown()
    }

    @Test func staleClientEvictionReconnects() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let once = Locked(true)
        await server.respond { request in
            if let reply = FakeVSRServer.sessionReply(request) { return reply }
            if request.code == CommandCode.ping.rawValue,
                once.withLock({
                    let first = $0; $0 = false; return first
                })
            {
                return .eviction(.staleClient)
            }
            return .ok([])
        }
        let client = makeClient(server)
        try await client.connect()
        try await client.ping()
        #expect(await server.connectionCount == 2)
        #expect(await operations(server).filter { $0 == .register }.count == 1)
        #expect(await codes(server).filter { $0 == CommandCode.bindSession.rawValue }.count == 1)
        await client.shutdown()
    }

    @Test func incompatibleProtocolIsTerminal() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await server.respond { request in
            request.operation == .register ? .eviction(.incompatibleProtocol) : .ok([])
        }
        let client = makeClient(server, autoLogin: nil)
        try await client.connect()
        await expectCode(.incompatibleProtocolVersion) { _ = try await client.login(username: "iggy", password: "iggy") }
        await client.shutdown()
    }

    /// A resumed session keeps the client table entry, so a replicated write
    /// lost with its connection goes again under its own request id, and the
    /// server answers it from its reply cache if it committed.
    @Test func replicatedRequestIsReplayedUnderTheResumedSession() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let once = Locked(true)
        await server.respond { request in
            if let reply = FakeVSRServer.sessionReply(request) { return reply }
            if request.operation == .deleteStream, once.exchange(false) {
                return .close
            }
            return .ok([])
        }
        let client = makeClient(server)
        try await client.connect()
        try await client.deleteStream(1)
        let writes = await server.requests.filter { $0.operation == .deleteStream }
        #expect(writes.count == 2)
        #expect(Set(writes.map(\.requestID)).count == 1)
        #expect(Set(writes.map(\.clientID)).count == 1)
        #expect(await operations(server).filter { $0 == .register }.count == 1)
        #expect(await codes(server).filter { $0 == CommandCode.bindSession.rawValue }.count == 1)
        await client.shutdown()
    }

    /// The first attempt was written before its connection went, so a replay
    /// the resumed session keeps refusing may still have committed: it is
    /// reported as unknown, never stamped afresh under a new request id.
    @Test func aRefusedResumedReplayIsNotStampedAfresh() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let once = Locked(true)
        await server.respond { request in
            if let reply = FakeVSRServer.sessionReply(request) { return reply }
            if request.operation == .deleteStream {
                return once.exchange(false) ? .close : .status(IggyErrorCode.transientNotAccepted.rawValue)
            }
            return .ok([])
        }
        let client = makeClient(server, requestTimeout: ClientCore.transientFailoverCheckInterval + .seconds(1))
        try await client.connect()
        await expectCode(.transientNotCommitted) { try await client.deleteStream(1) }
        let attempts = await server.requests.filter { $0.operation == .deleteStream }.map(\.requestID)
        #expect(attempts.count > 2)
        #expect(Set(attempts).count == 1)
        #expect(await codes(server).filter { $0 == CommandCode.bindSession.rawValue }.count == 1)
        await client.shutdown()
    }

    @Test func anUnprovenNamespaceDenialAfterALostWriteStaysUnknown() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let once = Locked(true)
        await server.respond { request in
            if let reply = FakeVSRServer.sessionReply(request) { return reply }
            if request.operation == .deleteStream {
                return once.exchange(false) ? .close : .status(IggyErrorCode.streamIdNotFound.rawValue)
            }
            return .ok([])
        }
        let client = makeClient(server, requestTimeout: .seconds(1))
        try await client.connect()

        await expectCode(.transientNotCommitted) { try await client.deleteStream(1) }

        let attempts = await server.requests.filter { $0.operation == .deleteStream }
        #expect(attempts.count > 1)
        #expect(Set(attempts.map(\.requestID)).count == 1)
        await client.shutdown()
    }

    @Test func aCommittedRejectionAfterALostWriteProvesItsOutcome() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let once = Locked(true)
        await server.respond { request in
            if let reply = FakeVSRServer.sessionReply(request) { return reply }
            if request.operation == .deleteStream {
                return once.exchange(false) ? .close : .committedRejection(IggyErrorCode.streamIdNotFound.rawValue)
            }
            return .ok([])
        }
        let client = makeClient(server, requestTimeout: .seconds(1))
        try await client.connect()

        await expectCode(.streamIdNotFound) { try await client.deleteStream(1) }

        #expect(await server.requests.filter { $0.operation == .deleteStream }.count == 2)
        await client.shutdown()
    }

    @Test func anUncommittedSuccessAfterALostWriteStaysUnknown() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let once = Locked(true)
        await server.respond { request in
            if let reply = FakeVSRServer.sessionReply(request) { return reply }
            if request.operation == .deleteStream {
                return once.exchange(false) ? .close : .uncommitted([])
            }
            return .ok([])
        }
        let client = makeClient(server, requestTimeout: .seconds(1))
        try await client.connect()

        await expectCode(.transientNotCommitted) { try await client.deleteStream(1) }

        let attempts = await server.requests.filter { $0.operation == .deleteStream }
        #expect(Set(attempts.map(\.requestID)).count == 1)
        await client.shutdown()
    }

    /// A send whose reply was lost may have committed. A replay that finds
    /// the session ended, refused as a stale client or evicted, proves
    /// nothing about the lost attempt: the outcome stays unknown, and no new
    /// session sends the batch again. No replay can reach the ended session,
    /// so the call ends at once instead of replaying until its deadline.
    @Test(arguments: sessionEnds)
    func aLostSendStaysUnknownWhenItsReplayFindsTheSessionEnded(_ sessionEnd: FakeVSRServer.Reply) async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let lost = Locked(false)
        let ended = Locked(false)
        await server.respond { request in
            if request.code == CommandCode.bindSession.rawValue, ended.withLock({ $0 }) {
                return .status(IggyErrorCode.unauthenticated.rawValue)
            }
            if request.code == CommandCode.sendMessages.rawValue {
                guard lost.exchange(true) else {
                    return .close
                }
                ended.withLock { $0 = true }
                return sessionEnd
            }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = makeClient(server, requestTimeout: .seconds(1))
        try await client.connect()

        await expectCode(.transientNotCommitted) {
            try await client.sendBatch(streamID: 1, topicID: 2, partitioning: .partition(1), messages: ["a"])
        }

        let sends = await server.requests.filter { $0.code == CommandCode.sendMessages.rawValue }
        #expect(sends.count == 2)
        #expect(Set(sends.map(\.requestID)).count == 1)
        #expect(Set(sends.map(\.clientID)).count == 1)
        await client.shutdown()
    }

    /// A send answered "not committed" may still commit. Only another "not
    /// committed" keeps its replay going on the connection, as in the Rust
    /// SDK: a replay that finds the session ended ends the call at once with
    /// the unknown outcome.
    @Test(arguments: sessionEnds)
    func anUncommittedSendStaysUnknownWhenItsReplayFindsTheSessionEnded(_ sessionEnd: FakeVSRServer.Reply) async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let uncommitted = Locked(true)
        let ended = Locked(false)
        await server.respond { request in
            if request.code == CommandCode.bindSession.rawValue, ended.withLock({ $0 }) {
                return .status(IggyErrorCode.unauthenticated.rawValue)
            }
            if request.code == CommandCode.sendMessages.rawValue {
                if uncommitted.exchange(false) {
                    return .status(IggyErrorCode.transientNotCommitted.rawValue)
                }
                ended.withLock { $0 = true }
                return sessionEnd
            }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = makeClient(server, requestTimeout: .seconds(1))
        try await client.connect()

        await expectCode(.transientNotCommitted) {
            try await client.sendBatch(streamID: 1, topicID: 2, partitioning: .partition(1), messages: ["a"])
        }

        let sends = await server.requests.filter { $0.code == CommandCode.sendMessages.rawValue }
        #expect(sends.count == 2)
        #expect(Set(sends.map(\.requestID)).count == 1)
        await client.shutdown()
    }

    /// Once a send was answered "not committed", only a transient answer
    /// keeps its replay going until the deadline, as in the Rust SDK: "not
    /// committed", "not accepted" or "not primary". Any other refusal ends
    /// the call at once with the unknown outcome, since every replay of the
    /// same request id gets the same answer.
    @Test(arguments: [
        (IggyErrorCode.transientNotAccepted, true), (.notPrimary, true), (.requestTooOld, false), (.lifecycleBusy, false),
        (.historyUnavailable, false), (.consumerGroupPartitionNotOwned, false), (.invalidCredentials, false),
    ])
    func anUncommittedSendReplaysOnlyWhileItsReplayIsAnsweredTransiently(_ refusal: IggyErrorCode, _ keepsReplaying: Bool) async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let uncommitted = Locked(true)
        await server.respond { request in
            if request.code == CommandCode.sendMessages.rawValue {
                return .status(uncommitted.exchange(false) ? IggyErrorCode.transientNotCommitted.rawValue : refusal.rawValue)
            }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = makeClient(server, requestTimeout: .seconds(1))
        try await client.connect()

        await expectCode(.transientNotCommitted) {
            try await client.sendBatch(streamID: 1, topicID: 2, partitioning: .partition(1), messages: ["a"])
        }

        let sends = await server.requests.filter { $0.code == CommandCode.sendMessages.rawValue }
        #expect(keepsReplaying ? sends.count > 2 : sends.count == 2, "\(sends.count) sends")
        #expect(Set(sends.map(\.requestID)).count == 1)
        await client.shutdown()
    }

    /// A connection whose session ended serves no request any more. The
    /// client closes it, so the next request dials again and signs in under
    /// a new session, as the Rust SDK signs in again after such an answer.
    @Test(arguments: sessionEnds)
    func aSessionEndAfterALostSendClosesTheConnectionAndTheNextRequestSignsInAgain(_ sessionEnd: FakeVSRServer.Reply) async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let sends = Locked(0)
        await server.respond { request in
            if request.code == CommandCode.bindSession.rawValue, sends.withLock({ $0 }) > 1 {
                return .status(IggyErrorCode.unauthenticated.rawValue)
            }
            if request.code == CommandCode.sendMessages.rawValue {
                let sent = sends.withLock {
                    $0 += 1; return $0
                }
                return sent == 1 ? .close : sessionEnd
            }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = makeClient(server, requestTimeout: .seconds(1))
        try await client.connect()
        await expectCode(.transientNotCommitted) {
            try await client.sendBatch(streamID: 1, topicID: 2, partitioning: .partition(1), messages: ["a"])
        }

        try await client.ping()

        #expect(await server.connectionCount == 3)
        #expect(await operations(server).filter { $0 == .register }.count == 2)
        await client.shutdown()
    }

    @Test(arguments: [false, true])
    func aStaleClientAnswerAfter57BindsTheRetainedSession(evicted: Bool) async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let attempts = Locked(0)
        await server.respond { request in
            if request.code == CommandCode.sendMessages.rawValue {
                switch attempts.withLock({
                    $0 += 1; return $0
                }) {
                case 1: return .status(IggyErrorCode.transientNotCommitted.rawValue)
                case 2: return evicted ? .eviction(.staleClient) : .status(IggyErrorCode.staleClient.rawValue)
                default: break
                }
            }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = makeClient(server)
        defer { Task { await client.shutdown() } }
        try await client.connect()
        let send = { _ = try await client.sendBatch(streamID: 1, topicID: 2, partitioning: .partition(1), messages: ["a"]) }

        if evicted {
            try await send()
        } else {
            await expectCode(.transientNotCommitted) { try await send() }
            try await send()
        }

        let sends = await server.requests.filter { $0.code == CommandCode.sendMessages.rawValue }
        #expect(sends.count == 3)
        #expect(Set(sends.map(\.clientID)).count == 1)
        #expect(Set(sends.map(\.session)).count == 1)
        #expect(Set(sends.prefix(2).map(\.requestID)).count == 1)
        if evicted { #expect(Set(sends.map(\.requestID)).count == 1) }
        #expect(await operations(server).filter { $0 == .register }.count == 1)
        #expect(await codes(server).filter { $0 == CommandCode.bindSession.rawValue }.count == 1)
    }

    /// A reply that refuses the first attempt of a send as a stale client
    /// proves the send never committed. As in the Rust SDK, the call ends
    /// with that refusal, not with an unknown outcome, and the next request
    /// binds the same session again.
    @Test func aStaleClientRefusalOfAFirstSendIsFinalAndTheNextSendBindsAgain() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let refused = Locked(false)
        await server.respond { request in
            if request.code == CommandCode.sendMessages.rawValue, !refused.exchange(true) {
                return .status(IggyErrorCode.staleClient.rawValue)
            }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = makeClient(server)
        try await client.connect()

        await expectCode(.staleClient) {
            try await client.sendBatch(streamID: 1, topicID: 2, partitioning: .partition(1), messages: ["a"])
        }
        #expect(await server.requests.filter { $0.code == CommandCode.sendMessages.rawValue }.count == 1)

        try await client.sendBatch(streamID: 1, topicID: 2, partitioning: .partition(1), messages: ["b"])
        #expect(await server.requests.filter { $0.code == CommandCode.sendMessages.rawValue }.count == 2)
        #expect(await operations(server).filter { $0 == .register }.count == 1)
        let sends = await server.requests.filter { $0.code == CommandCode.sendMessages.rawValue }
        #expect(Set(sends.map(\.clientID)).count == 1)
        #expect(Set(sends.map(\.session)).count == 1)
        #expect(await codes(server).filter { $0 == CommandCode.bindSession.rawValue }.count == 1)
        await client.shutdown()
    }

    /// A stale client refusal ends a read too, as the Rust SDK ends any call
    /// with it, instead of reconnecting and reading again. The server
    /// attaches the connection to no session any more, so the client reports
    /// itself disconnected, and the next call signs in again.
    @Test func aStaleClientRefusalOfAFirstReadEndsTheRead() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let refused = Locked(false)
        await server.respond { request in
            if request.code == CommandCode.bindSession.rawValue {
                return .status(IggyErrorCode.unauthenticated.rawValue)
            }
            if request.code == CommandCode.getStreams.rawValue, !refused.exchange(true) {
                return .status(IggyErrorCode.staleClient.rawValue)
            }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = makeClient(server)
        let events = await client.events
        try await client.connect()

        await expectCode(.staleClient) { _ = try await client.getStreams() }
        #expect(await codes(server).filter { $0 == CommandCode.getStreams.rawValue }.count == 1)
        #expect(await client.state == .disconnected)

        _ = try await client.getStreams()
        #expect(await operations(server).filter { $0 == .register }.count == 2)
        await client.shutdown()
        var seen: [DiagnosticEvent] = []
        for await event in events {
            seen.append(event)
        }
        #expect(seen == [.connected, .signedIn, .disconnected, .connected, .signedIn, .shutdown])
    }

    /// A stale-client eviction ends only the connection, as the Rust SDK
    /// reads it: the session goes on over a new connection, where the send
    /// goes again under its own request id.
    @Test func aStaleClientEvictionOfASendResumesTheSessionAndReplaysTheSend() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let evicted = Locked(false)
        await server.respond { request in
            if request.code == CommandCode.sendMessages.rawValue, !evicted.exchange(true) {
                return .eviction(.staleClient)
            }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = makeClient(server)
        try await client.connect()

        try await client.sendBatch(streamID: 1, topicID: 2, partitioning: .partition(1), messages: ["a"])

        let sends = await server.requests.filter { $0.code == CommandCode.sendMessages.rawValue }
        #expect(sends.count == 2)
        #expect(Set(sends.map(\.requestID)).count == 1)
        #expect(await operations(server).filter { $0 == .register }.count == 1)
        await client.shutdown()
    }

    /// A reconnect that fails after the request was written leaves its
    /// outcome unknown; the reconnect's own error must not hide that. The
    /// server evicts the sign-in of a deleted or deactivated user: with a
    /// password as invalid credentials, with a token as an invalid token or
    /// an inactive user.
    @Test(arguments: [
        (rootLogin, FakeVSRServer.Reply.status(IggyErrorCode.invalidCredentials.rawValue)), (rootLogin, .eviction(.invalidCredentials)),
        (tokenLogin, .eviction(.invalidToken)), (tokenLogin, .eviction(.userInactive)),
    ])
    func aFailedReconnectKeepsTheUnknownOutcome(_ credentials: Credentials, _ signInRefusal: FakeVSRServer.Reply) async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let registers = Locked(0)
        await server.respond { request in
            if request.code == CommandCode.bindSession.rawValue { return .status(IggyErrorCode.unauthenticated.rawValue) }
            if request.operation == .register {
                let first = registers.withLock {
                    $0 += 1; return $0 == 1
                }
                return first ? .ok(FakeVSRServer.registerBody()) : signInRefusal
            }
            return request.operation == .deleteStream ? .close : .ok([])
        }
        let client = makeClient(server, autoLogin: credentials)
        try await client.connect()
        await expectCode(.transientNotCommitted) { try await client.deleteStream(1) }
        #expect(await server.requests.filter { $0.operation == .deleteStream }.count == 1)
        #expect(registers.withLock { $0 } == 2)
        await client.shutdown()
    }

    @Test func replicatedRequestIsNotReplayedUnderANewSession() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let once = Locked(true)
        await server.respond { request in
            if request.code == CommandCode.bindSession.rawValue { return .status(IggyErrorCode.unauthenticated.rawValue) }
            if let reply = FakeVSRServer.sessionReply(request) { return reply }
            if request.operation == .createStream, once.exchange(false) {
                return .close
            }
            return .ok([])
        }
        let client = makeClient(server)
        try await client.connect()
        // The server no longer holds the session, so the client signs in
        // under a new one. The outcome is unknown: the client reports the
        // write as possibly committed instead of applying it twice, so it is
        // not mistaken for a request that was never sent.
        await expectCode(.transientNotCommitted) { _ = try await client.createStream(name: "x") }
        #expect(await operations(server).filter { $0 == .createStream }.count == 1)
        #expect(await operations(server).filter { $0 == .register }.count == 2)
        #expect(await client.state == .authenticated)
        #expect(await server.connectionCount == 2)
        try await client.ping()
        await client.shutdown()
    }

    /// The body of a login is built once, with the bind secret of the session
    /// that registers. A retry on a new connection has to arrive under that
    /// same client id, or the server binds the secret to the wrong session.
    @Test func loginRetriedAfterAConnectionDropKeepsItsIdentityAndSecret() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let once = Locked(true)
        await server.respond { request in
            guard request.operation == .register else { return .ok([]) }
            let first = once.withLock {
                let first = $0; $0 = false; return first
            }
            return first ? .close : .ok(FakeVSRServer.registerBody())
        }
        let client = makeClient(server, autoLogin: nil)
        try await client.connect()
        _ = try await client.login(username: "iggy", password: "iggy")
        #expect(await client.state == .authenticated)
        let registers = await server.requests.filter { $0.operation == .register }
        #expect(registers.count == 2)
        #expect(registers[0].connection != registers[1].connection)
        #expect(registers[0].clientID == registers[1].clientID)
        let secrets = registers.compactMap { Self.bindSecret(in: $0.payload) }
        #expect(secrets.count == 2)
        #expect(secrets[0] == secrets[1])
        await client.shutdown()
    }

    @Test func nonReplicatedRequestIsReplayedAfterAConnectionDrop() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let once = Locked(true)
        await server.respond { request in
            if let reply = FakeVSRServer.sessionReply(request) { return reply }
            if request.code == CommandCode.getStats.rawValue,
                once.withLock({
                    let first = $0; $0 = false; return first
                })
            {
                return .close
            }
            return .ok([UInt8](repeating: 0, count: 200))
        }
        let client = makeClient(server)
        try await client.connect()
        _ = try? await client.getStats()
        #expect(await codes(server).filter { $0 == CommandCode.getStats.rawValue }.count == 2)
        #expect(await server.connectionCount == 2)
        await client.shutdown()
    }

    @Test func reconnectionDisabledSurfacesTheDrop() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await server.respond { request in
            request.operation == .register ? .ok(FakeVSRServer.registerBody()) : request.code == CommandCode.ping.rawValue ? .close : .ok([])
        }
        let client = makeClient(server, reconnection: false)
        try await client.connect()
        await expectCode(.disconnected) { try await client.ping() }
        #expect(await client.state == .disconnected)
        #expect(await server.connectionCount == 1)
        await expectCode(.disconnected) { try await client.ping() }
        await client.shutdown()
    }

    @Test func reconnectionDisabledReportsAnUnknownOutcome() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await server.respond { request in
            request.operation == .register ? .ok(FakeVSRServer.registerBody()) : request.operation == .deleteStream ? .close : .ok([])
        }
        let client = makeClient(server, reconnection: false)
        try await client.connect()
        await expectCode(.transientNotCommitted) { try await client.deleteStream(1) }
        // A typed call that never reaches the wire still says so.
        await expectCode(.disconnected) { try await client.deleteStream(1) }
        #expect(await operations(server).filter { $0 == .deleteStream }.count == 1)
        await client.shutdown()
    }

    @Test func withoutCredentialsNothingIsReplayed() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await server.respond { request in
            request.code == CommandCode.ping.rawValue ? .close : .ok([])
        }
        let client = makeClient(server, autoLogin: nil)
        try await client.connect()
        await expectCode(.disconnected) { try await client.ping() }
        #expect(await server.connectionCount == 1)
        await client.shutdown()
    }

    /// A teardown, such as a failover moving the shared connection, waits for
    /// the reply of a frame another caller already wrote instead of cutting
    /// it, which would leave the write's outcome unknown and replay it.
    @Test func aTeardownWaitsForTheExchangeInFlight() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await server.respond { request in
            if let reply = FakeVSRServer.sessionReply(request) { return reply }
            if request.operation == .deleteStream {
                try? await Task.sleep(for: .milliseconds(300))
            }
            return .ok([])
        }
        let client = makeClient(server)
        try await client.connect()
        let write = Task { try await client.deleteStream(1) }
        #expect(await eventually { await operations(server).contains(.deleteStream) })
        await client.core.disconnectTransport()
        try await write.value
        #expect(await operations(server).filter { $0 == .deleteStream }.count == 1)
        #expect(await client.state == .disconnected)
        try await client.ping()
        #expect(await server.connectionCount == 2)
        await client.shutdown()
    }

    /// An explicit disconnect ends the session, so the next connect signs in
    /// under a new one instead of resuming it.
    @Test func anExplicitDisconnectIsNotResumed() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = makeClient(server)
        try await client.connect()
        await client.disconnect()
        try await client.connect()
        try await client.ping()
        let registers = await server.requests.filter { $0.operation == .register }
        #expect(registers.count == 2)
        #expect(Set(registers.map(\.clientID)).count == 2)
        #expect(await codes(server).filter { $0 == CommandCode.bindSession.rawValue }.isEmpty)
        await client.shutdown()
    }

    @Test func serverInitiatedCloseIsRepairedByTheNextRequest() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = makeClient(server)
        let events = await client.events
        try await client.connect()
        await server.closeAllConnections()
        try await Task.sleep(for: .milliseconds(20))
        try await client.ping()
        #expect(await server.connectionCount == 2)
        #expect(await operations(server).filter { $0 == .register }.count == 1)
        #expect(await codes(server).filter { $0 == CommandCode.bindSession.rawValue }.count == 1)
        await client.shutdown()
        var seen: [DiagnosticEvent] = []
        for await event in events {
            seen.append(event)
        }
        #expect(seen == [.connected, .signedIn, .disconnected, .connected, .signedIn, .shutdown])
    }

    /// After the caller logs out, the client does not sign in by itself, not
    /// even with the configured auto-login, until the caller signs in again.
    @Test func aReconnectAfterALogoutDoesNotSignIn() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = makeClient(server)
        try await client.connect()
        try await client.logout()
        await server.closeAllConnections()
        try await Task.sleep(for: .milliseconds(20))

        await #expect(throws: IggyError.self) { try await client.ping() }

        #expect(await operations(server).filter { $0 == .register }.count == 1)
        #expect(await client.state != .authenticated)
        _ = try await client.login(username: "iggy", password: "iggy")
        #expect(await operations(server).filter { $0 == .register }.count == 2)
        await client.shutdown()
    }

    /// A logout after a lost connection finds no session to end, yet the
    /// client signs out: no reconnect binds the old session or signs in.
    @Test func aLogoutWhileDisconnectedSignsOut() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await server.respond { request in
            if let reply = FakeVSRServer.sessionReply(request) { return reply }
            return request.code == CommandCode.getStats.rawValue ? .stall : .ok([])
        }
        let client = makeClient(server, requestTimeout: shortTimeout)
        try await client.connect()
        _ = try? await client.getStats()
        #expect(await client.state == .disconnected)

        await expectCode(.unauthenticated) { try await client.logout() }

        await #expect(throws: IggyError.self) { try await client.ping() }
        #expect(await codes(server).filter { $0 == CommandCode.bindSession.rawValue }.isEmpty)
        #expect(await operations(server).filter { $0 == .register }.count == 1)
        await client.shutdown()
    }

    /// A call that waits for a reconnect whose sign-in the server refuses
    /// ends unauthenticated, with the refusal as its cause, whether it waited
    /// in the exchange or before a typed call. The client forgets the refused
    /// sign-in and does not fall back to the configured auto-login, since
    /// the caller signed in as another user.
    @Test(arguments: [CommandCode.ping, .getStreams])
    func aRefusedAutomaticSignInEndsTheWaitingCallUnauthenticated(_ waiting: CommandCode) async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let refusing = Locked(false)
        await server.respond { request in
            if request.code == CommandCode.bindSession.rawValue { return .status(IggyErrorCode.unauthenticated.rawValue) }
            if request.operation == .register, refusing.withLock({ $0 }) {
                return .status(IggyErrorCode.invalidCredentials.rawValue)
            }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = makeClient(server)
        try await client.connect()
        _ = try await client.login(username: "other", password: "secret")
        refusing.withLock { $0 = true }
        await server.closeAllConnections()
        try await Task.sleep(for: .milliseconds(20))

        do {
            if waiting == .ping {
                try await client.ping()
            } else {
                _ = try await client.getStreams()
            }
            Issue.record("the call ran although the sign-in was refused")
        } catch let error as IggyError {
            #expect(error.code == .unauthenticated, "\(error)")
            #expect(error.context?.contains(IggyError(.invalidCredentials).description) == true, "\(error)")
        }

        await server.closeAllConnections()
        try await Task.sleep(for: .milliseconds(20))
        try? await client.ping()
        let registers = await server.requests.filter { $0.operation == .register }
        #expect(registers.map { Self.username(in: $0.payload) } == ["iggy", "other", "other"])
        #expect(await client.state != .authenticated)
        await client.shutdown()
    }

    /// A sign-in refusal that the Rust SDK treats as lasting, such as an
    /// invalid or expired personal access token, makes the client forget the
    /// token: the waiting call ends unauthenticated with the refusal as its
    /// cause, and no later reconnect signs in with that token again.
    @Test(
        arguments: [IggyErrorCode.invalidPersonalAccessToken, .personalAccessTokenExpired, .userInactive, .invalidFormat],
        [CommandCode.ping, .getStreams])
    func aTokenRefusedForGoodIsNotSentAgain(_ refusal: IggyErrorCode, _ waiting: CommandCode) async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let refusing = Locked(false)
        await server.respond { request in
            if request.code == CommandCode.bindSession.rawValue { return .status(IggyErrorCode.unauthenticated.rawValue) }
            if request.operation == .register, refusing.withLock({ $0 }) {
                return .status(refusal.rawValue)
            }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        let client = makeClient(server, autoLogin: tokenLogin)
        try await client.connect()
        refusing.withLock { $0 = true }
        await server.closeAllConnections()
        try await Task.sleep(for: .milliseconds(20))

        do {
            if waiting == .ping {
                try await client.ping()
            } else {
                _ = try await client.getStreams()
            }
            Issue.record("the call ran although the sign-in was refused")
        } catch let error as IggyError {
            #expect(error.code == .unauthenticated, "\(error)")
            #expect(error.context?.contains(IggyError(refusal).description) == true, "\(error)")
        }

        await server.closeAllConnections()
        try await Task.sleep(for: .milliseconds(20))
        try? await client.ping()
        #expect(await operations(server).filter { $0 == .register }.count == 2)
        #expect(await client.state != .authenticated)
        await client.shutdown()
    }

    @Test func connectRetriesUntilTheServerIsUp() async throws {
        let server = FakeVSRServer()
        try await server.start()
        await server.state.setAddress(server.address)
        let address = server.address
        await server.stop()
        let client = makeClient(server, maxRetries: 2)
        let clock = ContinuousClock()
        let elapsed = await clock.measure {
            await expectCode(.cannotEstablishConnection) { try await client.connect() }
        }
        // Three dials with two 20 ms pauses between them.
        #expect(elapsed >= .milliseconds(40) && elapsed < .seconds(10))
        #expect(await client.state == .disconnected)

        // Bring a server back on the same port and connect again.
        let revived = FakeVSRServer()
        let port = try Endpoint(parsing: address).port
        try await revived.startOn(port: port)
        defer { Task { await revived.stop() } }
        try await client.connect()
        #expect(await client.state == .authenticated)
        await client.shutdown()
    }

    /// A server that lost every session, as after a restart, so a reconnect
    /// signs in again instead of resuming.
    private static func forgetSessions(on server: FakeVSRServer) async {
        await server.respond { request in
            if request.code == CommandCode.bindSession.rawValue { return .status(IggyErrorCode.unauthenticated.rawValue) }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
    }

    @Test(arguments: [false, true])
    func aQueuedPasswordChangeCannotCommitDuringAutomaticSignIn(eviction: Bool) async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = makeClient(server)
        defer { Task { await client.shutdown() } }
        try await client.connect()
        let registered = Locked(false)
        let released = Locked(false)
        defer { released.withLock { $0 = true } }
        await server.respond { request in
            if request.code == CommandCode.bindSession.rawValue { return .status(IggyErrorCode.unauthenticated.rawValue) }
            if request.operation == .register {
                registered.withLock { $0 = true }
                while !released.withLock({ $0 }) { try? await Task.sleep(for: .milliseconds(2)) }
                return eviction ? .eviction(.invalidCredentials) : .status(IggyErrorCode.invalidCredentials.rawValue)
            }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        await server.closeAllConnections()
        let recovering = Task { try await client.ping() }
        try #require(await eventually { registered.withLock { $0 } })
        let entered = Locked(false)
        let changing = Task {
            entered.withLock { $0 = true }
            try await client.changePassword("iggy", currentPassword: "iggy", newPassword: "new-secret")
        }
        try #require(await eventually { entered.withLock { $0 } })
        #expect(await operations(server).contains(.changePassword) == false)
        released.withLock { $0 = true }
        await expectCode(.unauthenticated) { try await recovering.value }
        await expectCode(.unauthenticated) { try await changing.value }
        try await client.connect()
        await expectCode(.unauthenticated) { _ = try await client.getStreams() }
        #expect(await operations(server).contains(.changePassword) == false)
        #expect(await operations(server).filter { $0 == .register }.count == 2)
    }

    @Test(arguments: [false, true])
    func aPasswordChangeAfterAutomaticSignInKeepsItsCredentials(roster: Bool) async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = makeClient(server)
        defer { Task { await client.shutdown() } }
        try await client.connect()
        let held = Locked(false)
        let released = Locked(false)
        defer { released.withLock { $0 = true } }
        await server.respond { request in
            if request.code == CommandCode.bindSession.rawValue { return .status(IggyErrorCode.unauthenticated.rawValue) }
            if !released.withLock({ $0 }), roster ? request.code == CommandCode.getClusterMetadata.rawValue : request.operation == .register {
                held.withLock { $0 = true }
                while !released.withLock({ $0 }) { try? await Task.sleep(for: .milliseconds(2)) }
            }
            if request.operation == .changePassword { return .committed([], commit: 10) }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        await server.closeAllConnections()
        let recovering = Task { try await client.ping() }
        try #require(await eventually { held.withLock { $0 } })
        let entered = Locked(false)
        let changing = Task {
            entered.withLock { $0 = true }
            try await client.changePassword("iggy", currentPassword: "iggy", newPassword: "new-secret")
        }
        try #require(await eventually { entered.withLock { $0 } })
        #expect(await operations(server).contains(.changePassword) == false)
        released.withLock { $0 = true }
        try await changing.value
        try await recovering.value
        await server.closeAllConnections()
        try await client.ping()
        let registers = await server.requests.filter { $0.operation == .register }
        #expect(registers.map { Self.password(in: $0.payload) } == ["iggy", "iggy", "new-secret"])
    }

    @Test func aCommittedPasswordReplyPrecedesQueuedAutomaticRecovery() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = makeClient(server)
        defer { Task { await client.shutdown() } }
        try await client.connect()
        let changing = Locked(false)
        let releaseChange = Locked(false)
        let registered = Locked(false)
        let releaseRegister = Locked(false)
        let evicted = Locked(false)
        defer {
            releaseChange.withLock { $0 = true }
            releaseRegister.withLock { $0 = true }
        }
        await server.respond { request in
            if request.operation == .changePassword {
                changing.withLock { $0 = true }
                while !releaseChange.withLock({ $0 }) { try? await Task.sleep(for: .milliseconds(2)) }
                return .committed([], commit: 10)
            }
            if request.code == CommandCode.ping.rawValue, !evicted.exchange(true) { return .eviction(.sessionError) }
            if request.code == CommandCode.bindSession.rawValue { return .status(IggyErrorCode.unauthenticated.rawValue) }
            if request.operation == .register {
                registered.withLock { $0 = true }
                while !releaseRegister.withLock({ $0 }) { try? await Task.sleep(for: .milliseconds(2)) }
            }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        let change = Task { try await client.changePassword("iggy", currentPassword: "iggy", newPassword: "new-secret") }
        try #require(await eventually { changing.withLock { $0 } })
        let entered = Locked(false)
        let recovering = Task {
            entered.withLock { $0 = true }
            try await client.ping()
        }
        try #require(await eventually { entered.withLock { $0 } })
        #expect(registered.withLock { $0 } == false)
        releaseChange.withLock { $0 = true }
        try await change.value
        try #require(await eventually { registered.withLock { $0 } })
        #expect(await server.requests.filter { $0.operation == .register }.map { Self.password(in: $0.payload) } == ["iggy", "new-secret"])
        releaseRegister.withLock { $0 = true }
        try await recovering.value
        await server.closeAllConnections()
        try await client.ping()
        #expect(await server.requests.filter { $0.operation == .register }.map { Self.password(in: $0.payload) } == ["iggy", "new-secret", "new-secret"])
    }

    @Test func anOlderPasswordReplyCannotChangeANewerExplicitIdentity() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = makeClient(server)
        defer { Task { await client.shutdown() } }
        try await client.connect()
        let changing = Locked(false)
        let released = Locked(false)
        defer { released.withLock { $0 = true } }
        await server.respond { request in
            if request.operation == .changePassword {
                changing.withLock { $0 = true }
                while !released.withLock({ $0 }) { try? await Task.sleep(for: .milliseconds(2)) }
                return .committed([], commit: 10)
            }
            if request.code == CommandCode.bindSession.rawValue { return .status(IggyErrorCode.unauthenticated.rawValue) }
            return FakeVSRServer.defaultReply(request, address: server.address)
        }
        let change = Task { try await client.changePassword("iggy", currentPassword: "iggy", newPassword: "new-secret") }
        try #require(await eventually { changing.withLock { $0 } })
        let lineage = await client.core.lineage
        let signIn = Task { try await client.login(username: "other", password: "own-secret") }
        try #require(await eventually { await client.core.lineage != lineage })
        #expect(await operations(server).filter { $0 == .register }.count == 1)
        released.withLock { $0 = true }
        try await change.value
        _ = try await signIn.value
        await server.closeAllConnections()
        try await client.ping()
        let registers = await server.requests.filter { $0.operation == .register }
        #expect(registers.map { Self.username(in: $0.payload) } == ["iggy", "other", "other"])
        #expect(registers.map { Self.password(in: $0.payload) } == ["iggy", "own-secret", "own-secret"])
    }

    @Test func passwordChangeUpdatesTheReconnectCredentials() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await Self.forgetSessions(on: server)
        let client = makeClient(server)
        try await client.connect()
        try await client.changePassword("iggy", currentPassword: "iggy", newPassword: "new-secret")
        await server.closeAllConnections()
        try await Task.sleep(for: .milliseconds(20))
        try await client.ping()
        let registers = await server.requests.filter { $0.operation == .register }
        #expect(registers.count == 2)
        #expect(Self.password(in: registers[0].payload) == "iggy")
        #expect(Self.password(in: registers[1].payload) == "new-secret")
        // Each sign-in binds its own session, so the secret is fresh and never the zero default.
        let secrets = registers.compactMap { Self.bindSecret(in: $0.payload) }
        #expect(secrets.count == 2)
        #expect(secrets[0] != secrets[1])
        #expect(!secrets.contains([UInt8](repeating: 0, count: Requests.bindSecretSize)))
        await client.shutdown()
    }

    @Test func userRenameUpdatesTheReconnectCredentials() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await Self.forgetSessions(on: server)
        let client = makeClient(server)
        try await client.connect()
        try await client.updateUser("iggy", options: UserUpdateOptions(username: "renamed"))
        await server.closeAllConnections()
        try await Task.sleep(for: .milliseconds(20))
        try await client.ping()
        let registers = await server.requests.filter { $0.operation == .register }
        #expect(registers.count == 2)
        #expect(Self.username(in: registers[0].payload) == "iggy")
        #expect(Self.username(in: registers[1].payload) == "renamed")
        // A later password change for the renamed user still lands on the
        // credentials the reconnect uses.
        try await client.changePassword("renamed", currentPassword: "iggy", newPassword: "new-secret")
        await server.closeAllConnections()
        try await Task.sleep(for: .milliseconds(20))
        try await client.ping()
        let latest = await server.requests.filter { $0.operation == .register }.last!
        #expect(Self.username(in: latest.payload) == "renamed")
        #expect(Self.password(in: latest.payload) == "new-secret")
        await client.shutdown()
    }

    @Test func rawUsernameRenameUpdatesTheReconnectCredentials() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await Self.forgetSessions(on: server)
        let client = makeClient(server)
        try await client.connect()
        try await client.updateUser("iggy", options: UserUpdateOptions(raw: [UserOptionKey.username.rawValue: "renamed"]))
        await server.closeAllConnections()
        try await Task.sleep(for: .milliseconds(20))
        try await client.ping()
        let registers = await server.requests.filter { $0.operation == .register }
        #expect(registers.count == 2)
        #expect(Self.username(in: registers[1].payload) == "renamed")
        await client.shutdown()
    }

    @Test func rawPasswordChangeUpdatesTheReconnectCredentials() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await Self.forgetSessions(on: server)
        let client = makeClient(server)
        try await client.connect()
        let change = try Requests.changePassword(userID: "iggy", currentPassword: "iggy", newPassword: "new-secret")
        _ = try await client.sendRawRequest(code: CommandCode.changePassword.rawValue, payload: change)
        await server.closeAllConnections()
        try await Task.sleep(for: .milliseconds(20))
        try await client.ping()
        let registers = await server.requests.filter { $0.operation == .register }
        #expect(registers.count == 2)
        #expect(registers.map { Self.password(in: $0.payload) } == ["iggy", "new-secret"])
        await client.shutdown()
    }

    @Test func rawUserRenameUpdatesTheReconnectCredentials() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await Self.forgetSessions(on: server)
        let client = makeClient(server)
        try await client.connect()
        let rename = try Requests.updateResource(.user("iggy"), try UserUpdateOptions(username: "renamed").toPatch())
        _ = try await client.sendRawRequest(code: CommandCode.updateResource.rawValue, payload: rename)
        await server.closeAllConnections()
        try await Task.sleep(for: .milliseconds(20))
        try await client.ping()
        let registers = await server.requests.filter { $0.operation == .register }
        #expect(registers.count == 2)
        #expect(registers.map { Self.username(in: $0.payload) } == ["iggy", "renamed"])
        await client.shutdown()
    }

    @Test func largeRepliesArriveInPieces() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let body = [UInt8](repeating: 7, count: 300_000)
        await server.respond { request in
            request.operation == .register ? .ok(FakeVSRServer.registerBody()) : request.code == CommandCode.getStats.rawValue ? .okInChunks(body) : .ok([])
        }
        let client = makeClient(server)
        try await client.connect()
        let raw = try await client.sendRawRequest(code: CommandCode.getStats.rawValue, payload: [])
        #expect(raw == body)
        await client.shutdown()
    }

    @Test func leaderRedirectionMovesTheClient() async throws {
        let leader = try await FakeVSRServer.started()
        let follower = try await FakeVSRServer.started()
        defer {
            Task {
                await leader.stop()
                await follower.stop()
            }
        }
        let roster = FakeVSRServer.rosterBody([follower.address, leader.address], leaderIndex: 1)
        for server in [leader, follower] {
            await server.respond { request in
                if let reply = FakeVSRServer.sessionReply(request) { return reply }
                if request.code == CommandCode.getClusterMetadata.rawValue { return .ok(roster) }
                return .ok([])
            }
        }
        let client = makeClient(follower)
        try await client.connect()
        #expect(await client.serverAddress == leader.address)
        #expect(await client.state == .authenticated)
        #expect(await follower.connectionCount == 1)
        #expect(await leader.connectionCount == 1)
        try await client.ping()
        #expect(await codes(leader).contains(CommandCode.ping.rawValue))
        // A failover after the leader drops the connection dials the roster.
        await leader.stop()
        try await Task.sleep(for: .milliseconds(20))
        try await client.ping()
        #expect(await client.serverAddress == follower.address)
        await client.shutdown()
    }

    /// An outage longer than the request timeout leaves the client
    /// disconnected. Once the server is back the next typed call reconnects and
    /// resumes the session, with no ``IggyClient/connect()`` of its own.
    @Test func typedCallReconnectsAfterAnOutage() async throws {
        let server = try await FakeVSRServer.started()
        let port = try Endpoint(parsing: server.address).port
        let client = makeClient(server, requestTimeout: shortTimeout)
        try await client.connect()
        await server.stop()
        await expectCode(.transientNotAccepted) { try await client.ping() }
        #expect(await eventually { await client.state == .disconnected })
        // A typed call waits for the reconnect up to its timeout first.
        await expectCode(.transientNotAccepted) { _ = try await client.getStreams() }

        let revived = FakeVSRServer()
        try await revived.startOn(port: port)
        defer { Task { await revived.stop() } }
        #expect(try await client.getStreams().isEmpty)
        #expect(await client.state == .authenticated)
        #expect(await codes(revived).filter { $0 == CommandCode.bindSession.rawValue }.count == 1)
        await client.shutdown()
    }

    /// The heartbeat keeps pinging a client that lost its connection, which
    /// brings it back once the server returns, without any call.
    @Test func heartbeatReconnectsAfterAnOutage() async throws {
        let server = try await FakeVSRServer.started()
        let port = try Endpoint(parsing: server.address).port
        let client = makeClient(server, requestTimeout: shortTimeout, heartbeat: .milliseconds(30))
        try await client.connect()
        await server.stop()
        #expect(await eventually { await client.state == .disconnected })

        let revived = FakeVSRServer()
        try await revived.startOn(port: port)
        defer { Task { await revived.stop() } }
        #expect(await eventually { await pingCount(revived) >= 1 })
        #expect(await client.state == .authenticated)
        #expect(await codes(revived).filter { $0 == CommandCode.bindSession.rawValue }.count == 1)
        await client.shutdown()
    }

    /// `connect()` on a connected client leaves the heartbeat alone:
    /// cancelling it would abort a ping in flight and the connection with it.
    @Test func connectingAgainKeepsTheHeartbeatAndItsPing() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await server.respond { request in
            if let reply = FakeVSRServer.sessionReply(request) { return reply }
            if request.code == CommandCode.ping.rawValue {
                try? await Task.sleep(for: .milliseconds(300))
            }
            return .ok([])
        }
        let client = makeClient(server, heartbeat: .milliseconds(20))
        try await client.connect()
        #expect(await eventually { await pingCount(server) >= 1 })
        try await client.connect()
        #expect(await eventually { await pingCount(server) >= 2 })
        #expect(await client.state == .authenticated)
        #expect(await server.connectionCount == 1)
        await client.shutdown()
    }

    @Test func heartbeatPingsPeriodically() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        var configuration = ClientConfiguration(address: server.address, autoLogin: rootLogin)
        configuration.heartbeatInterval = .milliseconds(30)
        let client = IggyClient(configuration: configuration, logger: testLogger)
        try await client.connect()
        // Waits for the pings rather than for a fixed time: a loaded machine
        // stretches the interval, and only their arrival is being tested.
        #expect(await eventually { await pingCount(server) >= 3 })
        await client.shutdown()
        // One ping may already be on the wire when the heartbeat stops.
        let stopped = await pingCount(server)
        try await Task.sleep(for: .milliseconds(150))
        #expect(await pingCount(server) <= stopped + 1)
    }

    @Test func concurrentCallersShareOneConnectionInLockstep() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await server.respond { request in
            if let reply = FakeVSRServer.sessionReply(request) { return reply }
            try? await Task.sleep(for: .milliseconds(2))
            return .ok([])
        }
        let client = makeClient(server)
        try await client.connect()
        try await withThrowingTaskGroup(of: Void.self) { group in
            for index in 0..<20 {
                group.addTask {
                    if index.isMultiple(of: 2) {
                        try await client.ping()
                    } else {
                        try await client.deleteStream(Identifier(numeric: UInt32(index)))
                    }
                }
            }
            try await group.waitForAll()
        }
        let requests = await server.requests
        #expect(requests.filter { $0.operation == .deleteStream }.count == 10)
        #expect(requests.map(\.requestID) == Array(0...21))
        #expect(await server.connectionCount == 1)
        await client.shutdown()
    }

    /// Every caller that was waiting on a dropped connection fails on it, but
    /// only the first may tear it down: the rest must reuse the connection
    /// that caller established instead of closing it under its replay.
    @Test func concurrentCallersReconnectOnceAfterADrop() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let once = Locked(true)
        await server.respond { request in
            if let reply = FakeVSRServer.sessionReply(request) { return reply }
            guard request.code == CommandCode.getStats.rawValue else { return .ok([]) }
            let first = once.withLock {
                let first = $0; $0 = false; return first
            }
            if first {
                // Lets the other callers queue up behind this exchange.
                try? await Task.sleep(for: .milliseconds(50))
                return .close
            }
            return .ok([UInt8](repeating: 0, count: 200))
        }
        let client = makeClient(server)
        try await client.connect()
        try await withThrowingTaskGroup(of: Void.self) { group in
            for _ in 0..<12 {
                group.addTask { _ = try await client.getStats() }
            }
            try await group.waitForAll()
        }
        #expect(await server.connectionCount == 2)
        #expect(await operations(server).filter { $0 == .register }.count == 1)
        #expect(await codes(server).filter { $0 == CommandCode.bindSession.rawValue }.count == 1)
        await client.shutdown()
    }

    /// The replay after a reconnect can itself be refused for longer than the
    /// failover check interval, which sends it to the routing lock the
    /// reconnect just held. It must be free by then.
    @Test func replayAfterAReconnectCanStillFailOver() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let seen = Locked(0)
        let refuseUntil = Locked<ContinuousClock.Instant?>(nil)
        await server.respond { request in
            if let reply = FakeVSRServer.sessionReply(request) { return reply }
            if request.code == CommandCode.getClusterMetadata.rawValue { return .ok(FakeVSRServer.rosterBody([server.address])) }
            guard request.code == CommandCode.getStats.rawValue else { return .ok([]) }
            let count = seen.withLock {
                $0 += 1; return $0
            }
            if count == 1 { return .close }
            let deadline = refuseUntil.withLock { stored -> ContinuousClock.Instant in
                let deadline = stored ?? ContinuousClock.now + ClientCore.transientFailoverCheckInterval + .milliseconds(200)
                stored = deadline
                return deadline
            }
            return ContinuousClock.now < deadline ? .status(IggyErrorCode.transientNotAccepted.rawValue) : .ok([UInt8](repeating: 0, count: 200))
        }
        var configuration = ClientConfiguration(address: server.address, autoLogin: rootLogin)
        configuration.reconnection = ReconnectionOptions(interval: .milliseconds(20), reestablishAfter: .zero)
        configuration.requestTimeout = ClientCore.transientFailoverCheckInterval + .seconds(3)
        configuration.heartbeatInterval = .seconds(60)
        let client = IggyClient(configuration: configuration, logger: testLogger)
        try await client.connect()
        _ = try await client.getStats()
        #expect(await server.connectionCount == 2)
        await client.shutdown()
    }

    /// The reply of an abandoned request could arrive later and be read as
    /// the answer to the next one, so the connection goes with the request.
    @Test func cancellingARequestInFlightClosesItsConnection() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let once = Locked(true)
        await server.respond { request in
            if let reply = FakeVSRServer.sessionReply(request) { return reply }
            guard request.code == CommandCode.getStats.rawValue else { return .ok([]) }
            let first = once.withLock {
                let first = $0; $0 = false; return first
            }
            return first ? .stall : .ok([UInt8](repeating: 0, count: 200))
        }
        var configuration = ClientConfiguration(address: server.address, autoLogin: rootLogin)
        configuration.reconnection = ReconnectionOptions(interval: .milliseconds(20), reestablishAfter: .zero)
        configuration.heartbeatInterval = .seconds(60)
        let client = IggyClient(configuration: configuration, logger: testLogger)
        try await client.connect()
        let request = Task { try await client.getStats() }
        #expect(await eventually { await codes(server).contains(CommandCode.getStats.rawValue) })
        let clock = ContinuousClock()
        request.cancel()
        let elapsed = await clock.measure {
            await #expect(throws: CancellationError.self) { _ = try await request.value }
        }
        // Far below the 30 second request timeout the stalled reply would hit.
        #expect(elapsed < .seconds(5))
        #expect(await server.connectionCount == 1)
        _ = try await client.getStats()
        #expect(await server.connectionCount == 2)
        await client.shutdown()
    }

    /// A request cancelled before it is written costs nothing.
    @Test func cancelledCallerDoesNotTouchTheConnection() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = makeClient(server)
        try await client.connect()
        let before = await server.requests.count
        let request = Task {
            withUnsafeCurrentTask { $0?.cancel() }
            try await client.ping()
        }
        await #expect(throws: CancellationError.self) { try await request.value }
        #expect(await server.requests.count == before)
        try await client.ping()
        #expect(await server.connectionCount == 1)
        await client.shutdown()
    }

    /// An explicit disconnect is caller intent: neither a later request nor a
    /// sweep that was already retrying may reconnect behind the caller's back.
    @Test func explicitDisconnectStaysDisconnectedUntilConnect() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = makeClient(server)
        try await client.connect()
        await client.disconnect()
        #expect(await client.state == .disconnected)
        await expectCode(.notConnected) { try await client.ping() }
        #expect(await server.connectionCount == 1)
        try await client.connect()
        try await client.ping()
        #expect(await client.state == .authenticated)
        #expect(await server.connectionCount == 2)
        await client.shutdown()
    }

    @Test func explicitDisconnectEndsAConnectSweep() async throws {
        let server = FakeVSRServer()
        try await server.start()
        await server.stop()
        // Nothing listens on the address, and the retries never run out.
        let client = makeClient(server)
        let connecting = Task { try await client.connect() }
        #expect(await eventually { await client.state == .connecting })
        await client.disconnect()
        await expectCode(.notConnected) { try await connecting.value }
        #expect(await client.state == .disconnected)
        await client.shutdown()
    }

    @Test func shutdownIsFinal() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = makeClient(server)
        try await client.connect()
        await client.shutdown()
        // A teardown after the shutdown must not put the client back into a
        // state it can connect from.
        await client.disconnect()
        #expect(await client.state == .shutdown)
        await expectCode(.clientShutdown) { try await client.connect() }
        await expectCode(.clientShutdown) { try await client.ping() }
        #expect(await client.state == .shutdown)
        #expect(await server.connectionCount == 1)
        // Nothing is published after shutdown, so a late subscription ends
        // at once instead of waiting for a first event.
        var events = await client.events.makeAsyncIterator()
        #expect(await events.next() == nil)
    }

    @Test func shutdownDuringADialDoesNotReviveTheClient() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let connector = GatedConnector()
        let client = makeClient(server, connector: connector)
        let connecting = Task { try await client.connect() }
        #expect(await eventually { await client.state == .connecting })
        await client.shutdown()
        connector.released.withLock { $0 = true }
        await expectCode(.clientShutdown) { try await connecting.value }
        #expect(await client.state == .shutdown)
        // The dial that finished late was dropped before any sign-in.
        #expect(await eventually { await server.openConnectionCount == 0 })
        #expect(await server.requests.isEmpty)
    }

    /// With unlimited retries a connect to a server that is gone never ends
    /// by itself; cancelling the caller has to end it.
    @Test func cancelledConnectEndsItsSweep() async throws {
        let server = FakeVSRServer()
        try await server.start()
        let port = try Endpoint(parsing: server.address).port
        await server.stop()
        let client = makeClient(server)
        let connecting = Task { try await client.connect() }
        #expect(await eventually { await client.state == .connecting })
        connecting.cancel()
        await #expect(throws: CancellationError.self) { try await connecting.value }
        #expect(await eventually { await client.state == .disconnected })
        // The sweep left nothing behind: a later connect dials again.
        try await server.startOn(port: port)
        defer { Task { await server.stop() } }
        try await client.connect()
        #expect(await client.state == .authenticated)
        await client.shutdown()
    }

    @Test func cancelledConnectDuringTheSignInLeavesNoHalfOpenState() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let stallOnce = Locked(true)
        await server.respond { request in
            guard request.operation == .register else { return .ok([]) }
            let stall = stallOnce.withLock {
                let stall = $0; $0 = false; return stall
            }
            return stall ? .stall : .ok(FakeVSRServer.registerBody())
        }
        let client = makeClient(server)
        let connecting = Task { try await client.connect() }
        #expect(await eventually { await operations(server).contains(.register) })
        connecting.cancel()
        await #expect(throws: CancellationError.self) { try await connecting.value }
        #expect(await eventually { await client.state == .disconnected })
        try await client.connect()
        #expect(await client.state == .authenticated)
        try await client.ping()
        await client.shutdown()
    }

    /// A client dropped without a shutdown must not keep its socket or its
    /// heartbeat.
    @Test func droppedClientClosesItsConnection() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        do {
            var configuration = ClientConfiguration(address: server.address, autoLogin: rootLogin)
            configuration.heartbeatInterval = .milliseconds(20)
            let client = IggyClient(configuration: configuration, logger: testLogger)
            try await client.connect()
            #expect(await eventually { await pingCount(server) >= 1 })
        }
        #expect(await eventually { await server.openConnectionCount == 0 })
        let stopped = await pingCount(server)
        try await Task.sleep(for: .milliseconds(150))
        #expect(await pingCount(server) <= stopped + 1)
    }

    @Test func sessionControlCodesAreRefusedRaw() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = makeClient(server)
        try await client.connect()
        await expectCode(.invalidCommand) { _ = try await client.sendRawRequest(code: CommandCode.loginRegister.rawValue, payload: []) }
        await expectCode(.invalidCommand) { _ = try await client.sendRawRequest(code: CommandCode.logoutUser.rawValue, payload: []) }
        await client.shutdown()
    }

    /// Issues one of the replicated writes through its typed call.
    private static func issue(_ write: CommandCode, on client: IggyClient) async throws {
        switch write {
        case .deleteStream:
            try await client.deleteStream(1)
        case .sendMessages:
            try await client.sendBatch(streamID: 1, topicID: 2, partitioning: .partition(1), messages: ["a"])
        case .storeConsumerOffset:
            try await client.storeConsumerOffset(streamID: 1, topicID: 2, partitionID: 1, offset: 5)
        case .deleteSegments:
            try await client.deleteSegments(streamID: 1, topicID: 2, partitionID: 1, count: 1)
        default:
            Issue.record("no typed call issues command \(write)")
        }
    }

    /// A reader placed after the version info of a register payload, with the
    /// bind secret that follows it.
    private static func registerBody(_ payload: [UInt8]) -> (bindSecret: [UInt8], reader: ByteReader)? {
        var reader = ByteReader(payload[...])
        guard (try? reader.readUInt32()) != nil, (try? reader.readName()) != nil, (try? reader.readName()) != nil,
            let bindSecret = try? reader.readBytes(Requests.bindSecretSize)
        else {
            return nil
        }
        return (Array(bindSecret), reader)
    }

    private static func bindSecret(in payload: [UInt8]) -> [UInt8]? {
        registerBody(payload)?.bindSecret
    }

    /// The username field of a register payload, after the bind secret.
    private static func username(in payload: [UInt8]) -> String? {
        guard var reader = registerBody(payload)?.reader else {
            return nil
        }
        return try? reader.readName()
    }

    /// The password field of a register payload, after the username.
    private static func password(in payload: [UInt8]) -> String? {
        guard var reader = registerBody(payload)?.reader else {
            return nil
        }
        _ = try? reader.readName()
        return try? reader.readName()
    }
}
