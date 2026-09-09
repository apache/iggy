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
import Iggy
import Testing

/// The end-to-end suite runs against a live server named by
/// `IGGY_TCP_ADDRESS` and is skipped when that variable is unset. Start one
/// from a checkout of the repository:
///
/// ```text
/// cargo build --bin iggy-server
/// IGGY_SYSTEM_PATH=/tmp/iggy-swift IGGY_TCP_ADDRESS=127.0.0.1:8090 \
/// IGGY_HTTP_ENABLED=false IGGY_QUIC_ENABLED=false IGGY_WEBSOCKET_ENABLED=false \
/// IGGY_ROOT_USERNAME=iggy IGGY_ROOT_PASSWORD=iggy target/debug/iggy-server
///
/// IGGY_TCP_ADDRESS=127.0.0.1:8090 swift test --filter IggyE2ETests
/// ```
///
/// Set `IGGY_TCP_TLS_ENABLED=true` to run against a server started with
/// `IGGY_TCP_TLS_ENABLED=true` and the certificate pair in `core/certs`.
enum E2EEnvironment {
    static let rootUsername = ProcessInfo.processInfo.environment["IGGY_ROOT_USERNAME"] ?? "iggy"
    static let rootPassword = ProcessInfo.processInfo.environment["IGGY_ROOT_PASSWORD"] ?? "iggy"

    static var address: String? {
        ProcessInfo.processInfo.environment["IGGY_TCP_ADDRESS"]
    }

    static var isAvailable: Bool { address != nil }

    static var tlsEnabled: Bool {
        ProcessInfo.processInfo.environment["IGGY_TCP_TLS_ENABLED"] == "true"
    }

    /// Path of a file in `core/certs`, resolved from this source file so the
    /// suite works from any working directory.
    static func certificatePath(_ name: String) -> String {
        let root = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()  // E2EEnvironment.swift
            .deletingLastPathComponent()  // Support
            .deletingLastPathComponent()  // IggyE2ETests
            .deletingLastPathComponent()  // Tests
            .deletingLastPathComponent()  // swift
            .deletingLastPathComponent()  // foreign
        return root.appendingPathComponent("core/certs/\(name)").path
    }

    static var tlsOptions: TLSOptions? {
        guard tlsEnabled else { return nil }
        return TLSOptions(domain: "localhost", caFile: certificatePath("iggy_ca_cert.pem"))
    }

    static func configuration(autoLogin: Credentials? = nil) -> ClientConfiguration {
        var configuration = ClientConfiguration(address: address ?? "127.0.0.1:8090", tls: tlsOptions, autoLogin: autoLogin)
        configuration.reconnection.interval = .milliseconds(200)
        configuration.reconnection.reestablishAfter = .zero
        return configuration
    }

    /// A connected client without a session.
    static func newClient(autoLogin: Credentials? = nil) async throws -> IggyClient {
        let client = IggyClient(configuration: configuration(autoLogin: autoLogin))
        try await client.connect()
        return client
    }

    /// A connected client signed in as root.
    static func connect() async throws -> IggyClient {
        let client = try await newClient()
        _ = try await client.login(username: rootUsername, password: rootPassword)
        return client
    }

    /// Runs `body` with a connected client and tears everything down in order
    /// afterwards: scratch streams are deleted, secondary clients and then the
    /// primary client are shut down. Teardown runs even when `body` throws.
    static func withSession(login: Bool = true, autoLogin: Credentials? = nil, _ body: (TestSession) async throws -> Void) async throws {
        let client = try await newClient(autoLogin: autoLogin)
        if login, autoLogin == nil {
            _ = try await client.login(username: rootUsername, password: rootPassword)
        }
        let session = TestSession(client: client)
        do {
            try await body(session)
        } catch {
            await session.tearDown()
            throw error
        }
        await session.tearDown()
    }

    /// A unique name per test, within the 255-byte bound.
    static func uniqueName(_ prefix: String) -> String {
        let name = "\(prefix)-\(UInt64(Date().timeIntervalSince1970 * 1_000_000))-\(UInt32.random(in: 0..<UInt32.max))"
        return String(name.prefix(255))
    }

    static func messages(_ count: Int, prefix: String = "message") throws -> [IggyMessage] {
        try (0..<count).map { try IggyMessage("\(prefix) \($0)") }
    }
}

/// One test's clients and the server-side resources it created.
actor TestSession {
    struct ScratchTopic: Sendable {
        let stream: Identifier
        let topic: Identifier
        let streamID: UInt32
        let topicID: UInt32
    }

    nonisolated let client: IggyClient
    private var streams: [Identifier] = []
    private var secondaries: [IggyClient] = []

    init(client: IggyClient) {
        self.client = client
    }

    /// Deletes the stream once the test finishes.
    func track(stream: Identifier) {
        streams.append(stream)
    }

    /// A second connected, signed-out client, shut down with the session.
    func secondaryClient() async throws -> IggyClient {
        let other = try await E2EEnvironment.newClient()
        secondaries.append(other)
        return other
    }

    /// A stream and topic scoped to the test.
    func scratchTopic(partitions: UInt32) async throws -> ScratchTopic {
        let name = E2EEnvironment.uniqueName("swift-e2e")
        let stream = try await client.createStream(name: name)
        let streamID = Identifier(numeric: stream.id)
        // Tracked by name: the server reuses freed numeric ids, so deleting
        // by id after a test's own delete could hit another test's stream.
        streams.append(try Identifier(named: name))
        let topic = try await client.createTopic(streamID: streamID, name: name, options: TopicCreateOptions(partitionsCount: partitions))
        return ScratchTopic(stream: streamID, topic: Identifier(numeric: topic.id), streamID: stream.id, topicID: topic.id)
    }

    func tearDown() async {
        if await client.state == .authenticated {
            for stream in streams {
                try? await client.deleteStream(stream)
            }
        }
        for other in secondaries {
            try? await other.shutdown()
        }
        try? await client.shutdown()
    }
}

/// Skips a test when no server is configured.
let requiresServer: any SuiteTrait = .enabled(if: E2EEnvironment.isAvailable, "set IGGY_TCP_ADDRESS to run the end-to-end suite against a server")

/// Asserts that `body` throws an ``IggyError`` with the given code, whatever
/// context the error carries.
func expectIggyError(_ code: IggyErrorCode, sourceLocation: SourceLocation = #_sourceLocation, _ body: () async throws -> Void) async {
    do {
        try await body()
        Issue.record("expected \(code) but nothing was thrown", sourceLocation: sourceLocation)
    } catch let error as IggyError {
        #expect(error.code == code, "expected \(code), got \(error)", sourceLocation: sourceLocation)
    } catch {
        Issue.record("expected \(code), got \(error)", sourceLocation: sourceLocation)
    }
}
