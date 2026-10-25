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

private let leaderReply: [UInt8] = Array("leader".utf8)
private let backupReply: [UInt8] = Array("backup".utf8)
private let metadataCommit: UInt64 = 7

/// Raw requests that name the replica to answer them, on a fake leader
/// (replica 0, the coordinator) and a fake backup (replica 1). Mirrors the
/// replica request tests of `core/sdk/src/driver.rs`.
@Suite("Replica requests", .serialized)
struct ReplicaRequestTests {
    @Test func aRawRequestReachesTheChosenReplicaWithTheSessionWatermark() async throws {
        let cluster = try await TwoReplicas.started { _ in .ok(backupReply) }
        defer { cluster.stop() }
        let client = try await cluster.signedInClient()
        _ = try await client.sendRawRequest(code: CommandCode.createStream.rawValue, payload: [])

        let backup = try await client.sendRawRequest(code: CommandCode.getStats.rawValue, payload: [1], replica: 1)
        let leader = try await client.sendRawRequest(code: CommandCode.getStats.rawValue, payload: [0], replica: 0)

        #expect(backup == backupReply)
        #expect(leader == leaderReply)
        let backupStats = await cluster.stats(on: cluster.backup)
        let leaderStats = await cluster.stats(on: cluster.leader)
        #expect(backupStats.map(\.payload) == [[1]])
        #expect(backupStats.map(\.context.metadataOp) == [metadataCommit])
        #expect(leaderStats.map(\.payload) == [[0]])
        #expect(leaderStats.map(\.context.metadataOp) == [metadataCommit])
        await client.shutdown()
    }

    @Test func notPrimaryFromTheChosenReplicaIsReturnedWithoutTryingAnother() async throws {
        let cluster = try await TwoReplicas.started { _ in .status(IggyErrorCode.notPrimary.rawValue) }
        defer { cluster.stop() }
        let client = try await cluster.signedInClient()

        await expectCode(.notPrimary) { _ = try await client.sendRawRequest(code: CommandCode.getStats.rawValue, payload: [], replica: 1) }

        #expect(await cluster.stats(on: cluster.backup).count == 1)
        #expect(await cluster.stats(on: cluster.leader).isEmpty)
        await client.shutdown()
    }

    @Test func aRefusalAndALostConnectionAskTheSameReplicaAgain() async throws {
        let cluster = try await TwoReplicas.started { attempt in
            switch attempt {
            case 1: .status(IggyErrorCode.transientNotAccepted.rawValue)
            case 2: .close
            default: .ok(backupReply)
            }
        }
        defer { cluster.stop() }
        let client = try await cluster.signedInClient()

        let backup = try await client.sendRawRequest(code: CommandCode.getStats.rawValue, payload: [], replica: 1)

        #expect(backup == backupReply)
        #expect(await cluster.stats(on: cluster.backup).count == 3)
        #expect(await cluster.stats(on: cluster.leader).isEmpty)
        await client.shutdown()
    }

    @Test func aReplicaTheRosterDoesNotListFailsAfterOneRosterRead() async throws {
        let cluster = try await TwoReplicas.started { _ in .ok(backupReply) }
        defer { cluster.stop() }
        let client = try await cluster.signedInClient()
        _ = try await client.sendRawRequest(code: CommandCode.getStats.rawValue, payload: [], replica: 1)
        let rosterReads = await cluster.rosterReads()

        await expectCode(.invalidIdentifier) { _ = try await client.sendRawRequest(code: CommandCode.getStats.rawValue, payload: [], replica: 2) }

        #expect(await cluster.rosterReads() == rosterReads + 1)
        #expect(await cluster.stats(on: cluster.backup).count == 1)
        await client.shutdown()
    }

    @Test func replicatedPartitionReadAndSessionCommandsAreRefusedBeforeAnythingIsSent() async throws {
        let cluster = try await TwoReplicas.started { _ in .ok(backupReply) }
        defer { cluster.stop() }
        let client = try await cluster.signedInClient()
        let requests = await cluster.leader.requests.count + cluster.backup.requests.count
        let refused: [CommandCode] = [.createStream, .pollMessages, .getConsumerOffset, .loginUser, .logoutUser]

        for code in refused {
            await expectCode(.invalidCommand) { _ = try await client.sendRawRequest(code: code.rawValue, payload: [], replica: 1) }
        }

        #expect(await cluster.leader.requests.count + cluster.backup.requests.count == requests)
        #expect(try await client.sendRawRequest(code: CommandCode.getStats.rawValue, payload: [], replica: 1) == backupReply)
        await client.shutdown()
    }
}

/// A leader that commits metadata writes at `metadataCommit` and a backup
/// that answers its stats requests as the test scripts each attempt,
/// numbered from 1.
private struct TwoReplicas {
    let leader: FakeVSRServer
    let backup: FakeVSRServer

    static func started(backupStats: @escaping @Sendable (Int) -> FakeVSRServer.Reply) async throws -> TwoReplicas {
        let leader = try await FakeVSRServer.started()
        let backup = try await FakeVSRServer.started()
        let roster = FakeVSRServer.rosterBody([leader.address, backup.address])
        let attempts = Locked(0)
        await leader.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .getClusterMetadata:
                return .ok(roster)
            case .createStream:
                return .committed([], commit: metadataCommit)
            case .getStats:
                return .ok(leaderReply)
            default:
                return FakeVSRServer.defaultReply(request, address: leader.address)
            }
        }
        await backup.respond { request in
            switch request.code.flatMap(CommandCode.init(rawValue:)) {
            case .getClusterMetadata:
                return .ok(roster)
            case .getStats:
                return backupStats(
                    attempts.withLock { attempt in
                        attempt += 1
                        return attempt
                    })
            default:
                return FakeVSRServer.defaultReply(request, address: backup.address)
            }
        }
        return TwoReplicas(leader: leader, backup: backup)
    }

    func signedInClient() async throws -> IggyClient {
        var configuration = ClientConfiguration(address: leader.address, autoLogin: .usernamePassword(username: "iggy", password: "iggy"))
        configuration.reconnection = ReconnectionOptions(maxRetries: nil, interval: .milliseconds(20), reestablishAfter: .zero)
        configuration.requestTimeout = .seconds(10)
        configuration.heartbeatInterval = .seconds(60)
        let client = IggyClient(configuration: configuration, connector: NIOConnector(), logger: testLogger)
        try await client.connect()
        return client
    }

    func stats(on node: FakeVSRServer) async -> [FakeVSRServer.Request] {
        await node.requests.filter { $0.code == CommandCode.getStats.rawValue }
    }

    func rosterReads() async -> Int {
        let reads = { (node: FakeVSRServer) async in await node.requests.filter { $0.code == CommandCode.getClusterMetadata.rawValue }.count }
        return await reads(leader) + reads(backup)
    }

    func stop() {
        Task {
            await leader.stop()
            await backup.stop()
        }
    }
}
