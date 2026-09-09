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
            if request.operation == .storeConsumerOffset {
                let seen = counter.withLock {
                    $0 += 1; return $0
                }
                return seen < 4 ? .status(IggyErrorCode.transientNotAccepted.rawValue) : .ok([])
            }
            return .ok([])
        }
        let client = makeClient(server)
        try await client.connect()
        try await client.storeConsumerOffset(consumer: .default, streamID: 1, topicID: 1, partitionID: 0, offset: 3)
        let sends = await server.requests.filter { $0.operation == .storeConsumerOffset }
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
        #expect(elapsed >= .milliseconds(450) && elapsed < .seconds(3))
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
        #expect(elapsed >= .milliseconds(40) && elapsed < .seconds(2))
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
        try await Task.sleep(for: .milliseconds(150))
        let pings = await codes(server).filter { $0 == CommandCode.ping.rawValue }.count
        #expect(pings >= 3)
        try await client.shutdown()
        try await Task.sleep(for: .milliseconds(60))
        #expect(await codes(server).filter { $0 == CommandCode.ping.rawValue }.count <= pings + 1)
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

    @Test func sessionControlCodesAreRefusedRaw() async throws {
        let server = try await FakeVSRServer.started()
        defer { Task { await server.stop() } }
        let client = makeClient(server)
        try await client.connect()
        await expectCode(.invalidCommand) { _ = try await client.sendRawRequest(code: CommandCode.loginRegister.rawValue, payload: []) }
        await expectCode(.invalidCommand) { _ = try await client.sendRawRequest(code: CommandCode.logoutUser.rawValue, payload: []) }
        try await client.shutdown()
    }

    /// The password field of a register payload: version info, then username
    /// and password as length-prefixed names.
    private static func password(in payload: [UInt8]) -> String? {
        var reader = ByteReader(payload[...])
        _ = try? reader.readUInt32()
        _ = try? reader.readName()
        _ = try? reader.readName()
        _ = try? reader.readName()
        return try? reader.readName()
    }
}
