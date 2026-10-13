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

/// Consensus-level session state, a port of `core/sdk/src/session.rs`.
///
/// Each logical login owns one session: an ephemeral random client
/// identifier and a bind secret. After login the server registers the client
/// through consensus and returns a session number, the fence every later
/// request echoes. Replicated requests carry a monotonically increasing
/// request id the server deduplicates on.
///
/// The identity, the secret, and the request counter outlive a transport
/// reconnect, so a registration retry recovers the session the server already
/// committed. Only an explicit logout starts a new one, through ``reset()``.
///
/// Not thread-safe: the connection owns one and serializes access through
/// the lockstep exchange.
struct ConsensusSession: Sendable {
    /// Register carries request id 0. Application requests start at 1.
    static let registerRequestID: UInt64 = 0

    let clientID: UInt128Value
    private(set) var session: UInt64?
    /// Bearer proof that binds connections to this logical session, sent with
    /// every login. Generated per session and never logged.
    let bindSecret: [UInt8]
    private var requestCounter: UInt64

    init(clientID: UInt128Value = .random()) {
        var generator = SystemRandomNumberGenerator()
        self.clientID = clientID
        self.bindSecret = (0..<Requests.bindSecretSize).map { _ in UInt8.random(in: UInt8.min...UInt8.max, using: &generator) }
        self.session = nil
        self.requestCounter = 1
    }

    var isBound: Bool { session != nil }

    /// Records the session fence the server assigned when Register committed.
    /// A registration replayed after a reconnect returns the same fence and is
    /// accepted; a different one cannot replace a live session.
    mutating func bind(_ session: UInt64) throws {
        if session == 0 {
            throw IggyError(.invalidSession, context: "session must be greater than zero")
        }
        if let bound = self.session, bound != session {
            throw IggyError(.sessionMismatch, context: "session is bound to \(bound), the server returned \(session)")
        }
        self.session = session
    }

    /// The next replicated request id, advancing the watermark: 1, 2, 3, ...
    mutating func nextRequestID() throws -> UInt64 {
        guard isBound else {
            throw IggyError(.unauthenticated, context: "request id taken before the session is bound")
        }
        if requestCounter == UInt64.max {
            throw IggyError(.requestIdExhausted)
        }
        let id = requestCounter
        requestCounter += 1
        return id
    }

    /// The watermark without advancing it. Non-replicated requests use it
    /// because the server never consults the client table for them.
    var currentRequestID: UInt64 { requestCounter }

    /// Ends the logical session and starts a new one under a fresh client
    /// identifier and bind secret. An explicit logout retires the server-side
    /// session, so the next Register must not be mistaken for a retry of it.
    mutating func reset() {
        self = ConsensusSession()
    }
}
