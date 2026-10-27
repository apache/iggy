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
import Testing

@testable import Iggy

let testLogger = Logger(label: "org.apache.iggy.tests")

/// A value behind a lock, for counters shared with `@Sendable` closures.
final class Locked<Value>: @unchecked Sendable {
    private let lock = NSLock()
    private var value: Value

    init(_ value: Value) {
        self.value = value
    }

    func withLock<T>(_ body: (inout Value) throws -> T) rethrows -> T {
        lock.lock()
        defer { lock.unlock() }
        return try body(&value)
    }

    /// Stores `newValue` and returns the value it replaced.
    func exchange(_ newValue: Value) -> Value {
        withLock { value in
            let previous = value
            value = newValue
            return previous
        }
    }
}

/// An address where nothing listens, so a dial is refused at once. Port 1
/// is privileged and below the ephemeral range, so no fake server can take
/// it, unlike the port of a stopped server, which another one can bind.
let unreachableAddress = "127.0.0.1:1"

/// A dialer that records every endpoint before it dials it. The fake server
/// counts a connection only some time after the client's connect returned,
/// so a check for a wrong dial reads this record instead.
struct RecordingConnector: IggyConnector {
    let dialed = Locked<[Endpoint]>([])

    func connect(to endpoint: Endpoint, tls: TLSOptions?, timeout: Duration, noDelay: Bool) async throws -> any IggyConnection {
        dialed.withLock { $0.append(endpoint) }
        return try await NIOConnector().connect(to: endpoint, tls: tls, timeout: timeout, noDelay: noDelay)
    }

    func hasDialed(_ server: FakeVSRServer) throws -> Bool {
        let endpoint = try Endpoint(parsing: server.address)
        return dialed.withLock { $0.contains(endpoint) }
    }
}

/// Polls `condition` until it holds or `timeout` passes.
func eventually(timeout: Duration = .seconds(5), _ condition: @Sendable () async -> Bool) async -> Bool {
    let deadline = ContinuousClock.now + timeout
    while ContinuousClock.now < deadline {
        if await condition() {
            return true
        }
        try? await Task.sleep(for: .milliseconds(5))
    }
    return await condition()
}

/// Asserts that `body` throws an ``IggyError`` with one of `codes`.
func expectCode(_ codes: IggyErrorCode..., sourceLocation: SourceLocation = #_sourceLocation, _ body: () async throws -> Void) async {
    do {
        try await body()
        Issue.record("expected \(codes) but nothing was thrown", sourceLocation: sourceLocation)
    } catch let error as IggyError {
        #expect(codes.contains(error.code), "expected \(codes), got \(error)", sourceLocation: sourceLocation)
    } catch {
        Issue.record("expected \(codes), got \(error)", sourceLocation: sourceLocation)
    }
}

extension IggyClient {
    /// ``sendMessages(streamID:topicID:partitioning:messages:)`` for a batch
    /// the test does not read back, so the call can take a literal.
    @discardableResult
    func sendBatch(
        streamID: Identifier, topicID: Identifier, partitioning: Partitioning = .balanced, messages: [IggyMessage]
    ) async throws -> SendMessagesResponse {
        var batch = messages
        return try await sendMessages(streamID: streamID, topicID: topicID, partitioning: partitioning, messages: &batch)
    }
}
