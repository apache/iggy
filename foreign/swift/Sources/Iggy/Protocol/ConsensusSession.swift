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
/// Each client generates an ephemeral random client identifier. After login
/// the server registers the client through consensus and returns a session
/// number, the fence every later request echoes. Replicated requests carry a
/// monotonically increasing request id the server deduplicates on.
///
/// Not thread-safe: the connection owns one and serializes access through
/// the lockstep exchange.
struct ConsensusSession: Sendable {
    private(set) var clientID: UInt128Value
    private(set) var session: UInt64?
    private var requestCounter: UInt64
    private var registerConsumed: Bool

    init(clientID: UInt128Value = .random()) {
        self.clientID = clientID
        self.session = nil
        self.requestCounter = 1
        self.registerConsumed = false
    }

    var isBound: Bool { session != nil }

    /// Whether the session registered, bound, or issued a replicated request.
    var hasActivity: Bool { registerConsumed || isBound || requestCounter > 1 }

    /// Arms the session for a Register and returns its request id, always 0.
    /// A session that already registered or bound is replaced wholesale with a
    /// fresh client identifier, so a re-login encodes a clean Register.
    mutating func beginRegister() -> UInt64 {
        if registerConsumed || isBound {
            self = ConsensusSession()
        }
        registerConsumed = true
        return 0
    }

    /// Records the session fence the server assigned when Register committed.
    mutating func bind(_ session: UInt64) throws {
        if isBound {
            throw IggyError(.alreadyAuthenticated)
        }
        if session == 0 {
            throw IggyError(.invalidSession, context: "session must be greater than zero")
        }
        self.session = session
    }

    /// The next replicated request id, advancing the watermark: 1, 2, 3, ...
    mutating func nextRequestID() throws -> UInt64 {
        guard isBound else {
            throw IggyError(.unauthenticated, context: "request id taken before the session is bound")
        }
        if requestCounter == UInt64.max {
            throw IggyError(.invalidConfiguration, context: "request counter exhausted")
        }
        let id = requestCounter
        requestCounter += 1
        return id
    }

    /// The watermark without advancing it. Non-replicated requests use it
    /// because the server never consults the client table for them.
    var currentRequestID: UInt64 { requestCounter }

    /// Drops the session and mints a fresh client identifier. A disconnect
    /// invalidates the server-side fence, so the next Register must arrive
    /// under a new identity.
    mutating func reset() {
        self = ConsensusSession()
    }
}
