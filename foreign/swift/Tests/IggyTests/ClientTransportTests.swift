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

/// A client on the fake server with fast reconnection and a short request
/// timeout, so a stalled test fails in well under a second.
private func makeClient(_ server: FakeVSRServer, autoLogin: Credentials? = rootLogin, reconnection: Bool = true, maxRetries: UInt32? = nil) -> IggyClient {
    var configuration = ClientConfiguration(address: server.address, autoLogin: autoLogin)
    configuration.reconnection = ReconnectionOptions(enabled: reconnection, maxRetries: maxRetries, interval: .milliseconds(20), reestablishAfter: .zero)
    configuration.requestTimeout = .milliseconds(500)
    configuration.connectTimeout = .milliseconds(500)
    configuration.heartbeatInterval = .seconds(60)
    return IggyClient(configuration: configuration, logger: testLogger)
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
        // The register runs unbound with request id 0; the roster read that
        // follows is non-replicated and never advances the watermark, which
        // the replicated creates then consume as 1 and 2.
        #expect(requests[0].session == 0 && requests[0].requestID == 0 && requests[0].checksum != 0)
        #expect(requests[1].code == CommandCode.getClusterMetadata.rawValue)
        #expect(requests[2].code == CommandCode.ping.rawValue && requests[2].session == 1 && requests[2].requestID == 1)
        #expect(requests[3].requestID == 1 && requests[3].session == 1 && requests[3].checksum == XXH3.hash64(requests[3].payload))
        #expect(requests[4].requestID == 2)
        #expect(requests[5].requestID == 3 && requests[5].checksum == 0)
        #expect(requests.allSatisfy { $0.clientID == requests[0].clientID })
        #expect(await client.serverAddress == server.address)
        #expect(await client.clientAddress?.hasPrefix("127.0.0.1:") == true)
        try await client.shutdown()
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
        try await client.shutdown()
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
        try await client.shutdown()
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
        try await client.shutdown()
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
        try await client.shutdown()
    }

    @Test func transientNotCommittedReplaysTheSameRequestID() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let counter = Locked(0)
        await server.respond { request in
            if request.operation == .register { return .ok(FakeVSRServer.registerBody()) }
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
        try await client.shutdown()
    }

    @Test func transientNotAcceptedReplaysOnASingleNode() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let counter = Locked(0)
        await server.respond { request in
            if request.operation == .register { return .ok(FakeVSRServer.registerBody()) }
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
        try await client.shutdown()
    }

    @Test func transientReplayGivesUpAtTheDeadline() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await server.respond { request in
            request.operation == .register
                ? .ok(FakeVSRServer.registerBody()) : request.operation == .createStream ? .status(IggyErrorCode.transientNotCommitted.rawValue) : .ok([])
        }
        let client = makeClient(server)
        try await client.connect()
        let clock = ContinuousClock()
        let elapsed = await clock.measure {
            await expectCode(.disconnected) { _ = try await client.createStream(name: "x") }
        }
        #expect(elapsed >= .milliseconds(450) && elapsed < .seconds(10))
        try await client.shutdown()
    }

    @Test func requestTimeoutClosesTheConnectionAndReconnects() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let stallOnce = Locked(true)
        await server.respond { request in
            if request.operation == .register { return .ok(FakeVSRServer.registerBody()) }
            if request.operation == .nonReplicated, request.code == CommandCode.getStats.rawValue,
                stallOnce.withLock({
                    let stall = $0; $0 = false; return stall
                })
            {
                return .stall
            }
            return .ok([])
        }
        let client = makeClient(server)
        try await client.connect()
        // The timeout leaves the stream at an unknown boundary; a stats read
        // is non-replicated, so it is replayed over a fresh connection.
        let clock = ContinuousClock()
        let elapsed = await clock.measure {
            _ = try? await client.getStats()
        }
        #expect(elapsed >= .milliseconds(450))
        #expect(await server.connectionCount == 2)
        #expect(await operations(server).filter { $0 == .register }.count == 2)
        #expect(await client.state == .authenticated)
        try await client.shutdown()
    }

    @Test func misroutedReplyDesyncsAndReconnects() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let once = Locked(true)
        await server.respond { request in
            if request.operation == .register { return .ok(FakeVSRServer.registerBody()) }
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
        try await client.shutdown()
    }

    @Test func evictionRestoresTheSessionAndReplaysSafeRequests() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let once = Locked(true)
        await server.respond { request in
            if request.operation == .register { return .ok(FakeVSRServer.registerBody()) }
            if request.operation == .deleteStream,
                once.withLock({
                    let first = $0; $0 = false; return first
                })
            {
                return .eviction(.noSession)
            }
            return .ok([])
        }
        let client = makeClient(server)
        try await client.connect()
        // An unauthenticated eviction means the request never reached the
        // log, so the delete is replayed under the new session.
        try await client.deleteStream(1)
        let ops = await operations(server)
        #expect(ops.filter { $0 == .register }.count == 2)
        #expect(ops.filter { $0 == .deleteStream }.count == 2)
        #expect(await server.connectionCount == 2)
        try await client.shutdown()
    }

    @Test func staleClientEvictionReconnects() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let once = Locked(true)
        await server.respond { request in
            if request.operation == .register { return .ok(FakeVSRServer.registerBody()) }
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
        try await client.shutdown()
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
        try await client.shutdown()
    }

    @Test func replicatedRequestIsNotReplayedAfterAConnectionDrop() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let once = Locked(true)
        await server.respond { request in
            if request.operation == .register { return .ok(FakeVSRServer.registerBody()) }
            if request.operation == .createStream,
                once.withLock({
                    let first = $0; $0 = false; return first
                })
            {
                return .close
            }
            return .ok([])
        }
        let client = makeClient(server)
        try await client.connect()
        // The outcome is unknown: the client reconnects for later requests
        // but returns the original error instead of applying it twice.
        await expectCode(.disconnected) { _ = try await client.createStream(name: "x") }
        #expect(await operations(server).filter { $0 == .createStream }.count == 1)
        #expect(await client.state == .authenticated)
        #expect(await server.connectionCount == 2)
        try await client.ping()
        try await client.shutdown()
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
        try await client.shutdown()
    }

    @Test func nonReplicatedRequestIsReplayedAfterAConnectionDrop() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let once = Locked(true)
        await server.respond { request in
            if request.operation == .register { return .ok(FakeVSRServer.registerBody()) }
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
        try await client.shutdown()
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
        try await client.shutdown()
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
        try await client.shutdown()
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
        #expect(await operations(server).filter { $0 == .register }.count == 2)
        try await client.shutdown()
        var seen: [DiagnosticEvent] = []
        for await event in events {
            seen.append(event)
        }
        #expect(seen == [.connected, .signedIn, .disconnected, .connected, .signedIn, .shutdown])
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
        try await client.shutdown()
    }

    @Test func passwordChangeUpdatesTheReconnectCredentials() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
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
        try await client.shutdown()
    }

    @Test func userRenameUpdatesTheReconnectCredentials() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = makeClient(server)
        try await client.connect()
        try await client.updateUser("iggy", username: "renamed")
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
        try await client.shutdown()
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
        try await client.shutdown()
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
                if request.operation == .register { return .ok(FakeVSRServer.registerBody()) }
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
        try await client.shutdown()
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
        try await client.shutdown()
        // One ping may already be on the wire when the heartbeat stops.
        let stopped = await pingCount(server)
        try await Task.sleep(for: .milliseconds(150))
        #expect(await pingCount(server) <= stopped + 1)
    }

    @Test func concurrentCallersShareOneConnectionInLockstep() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        await server.respond { request in
            if request.operation == .register { return .ok(FakeVSRServer.registerBody()) }
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
        let creates = await server.requests.filter { $0.operation == .deleteStream }.map(\.requestID)
        #expect(creates.sorted() == Array(1...10))
        #expect(await server.connectionCount == 1)
        try await client.shutdown()
    }

    /// Every caller that was waiting on a dropped connection fails on it, but
    /// only the first may tear it down: the rest must reuse the connection
    /// that caller established instead of closing it under its replay.
    @Test func concurrentCallersReconnectOnceAfterADrop() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let once = Locked(true)
        await server.respond { request in
            if request.operation == .register { return .ok(FakeVSRServer.registerBody()) }
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
        #expect(await operations(server).filter { $0 == .register }.count == 2)
        try await client.shutdown()
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
            if request.operation == .register { return .ok(FakeVSRServer.registerBody()) }
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
        try await client.shutdown()
    }

    /// The reply of an abandoned request could arrive later and be read as
    /// the answer to the next one, so the connection goes with the request.
    @Test func cancellingARequestInFlightClosesItsConnection() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let once = Locked(true)
        await server.respond { request in
            if request.operation == .register { return .ok(FakeVSRServer.registerBody()) }
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
        try await client.shutdown()
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
        try await client.shutdown()
    }

    /// An explicit disconnect is caller intent: neither a later request nor a
    /// sweep that was already retrying may reconnect behind the caller's back.
    @Test func explicitDisconnectStaysDisconnectedUntilConnect() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = makeClient(server)
        try await client.connect()
        try await client.disconnect()
        #expect(await client.state == .disconnected)
        await expectCode(.notConnected) { try await client.ping() }
        #expect(await server.connectionCount == 1)
        try await client.connect()
        try await client.ping()
        #expect(await client.state == .authenticated)
        #expect(await server.connectionCount == 2)
        try await client.shutdown()
    }

    @Test func explicitDisconnectEndsAConnectSweep() async throws {
        let server = FakeVSRServer()
        try await server.start()
        await server.stop()
        // Nothing listens on the address, and the retries never run out.
        let client = makeClient(server)
        let connecting = Task { try await client.connect() }
        #expect(await eventually { await client.state == .connecting })
        try await client.disconnect()
        await expectCode(.notConnected) { try await connecting.value }
        #expect(await client.state == .disconnected)
        try await client.shutdown()
    }

    @Test func sessionControlCodesAreRefusedRaw() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = makeClient(server)
        try await client.connect()
        await expectCode(.invalidCommand) { _ = try await client.sendRawRequest(code: CommandCode.loginRegister.rawValue, payload: []) }
        await expectCode(.invalidCommand) { _ = try await client.sendRawRequest(code: CommandCode.logoutUser.rawValue, payload: []) }
        try await client.shutdown()
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
