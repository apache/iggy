/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! Consensus-level session state for the SDK.
//!
//! Each SDK client instance generates an ephemeral random `client_id: u128`
//! on construction. After login, the server registers this client through
//! consensus and returns a `session` number (the commit op number).
//!
//! The SDK tracks the `(client_id, session)` pair and a monotonically
//! increasing `request_id` counter. These values populate the consensus
//! headers (`RequestHeader.client`, `.session`, `.request`) when the
//! transport sends requests through server-ng.
//!
//! ## Lifecycle
//!
//! ```text
//! new()                  - fresh client_id generated, session = None
//! register_request_id()  - returns 0 (for the register operation, once only)
//! bind(session)          - session assigned by server after register commits
//! next_request_id()      - returns 1, 2, 3, ... (application requests)
//! drop + new()           - on disconnect/crash, create a fresh session
//! ```
//!
//! A `ConsensusSession` is **not reusable** across connections. On disconnect
//! or crash, drop it and create a new one. This generates a fresh `client_id`
//! and avoids ambiguous re-register semantics (TigerBeetle requires a fresh
//! client_id per process). The old session stays in the server's `ClientTable`
//! until evicted.

/// Consensus-level session state.
///
/// Single-threaded: owned by whatever drives the request loop (connection
/// handler or SDK transport). All methods take `&mut self`. If shared
/// access is needed, wrap in `Arc<Mutex<_>>` at the call site.
#[derive(Debug)]
pub struct ConsensusSession {
    /// Ephemeral random client identifier. Generated once per process,
    /// never persisted. Each SDK instance gets a unique value.
    client_id: u128,
    /// Session number assigned by the server after register commits
    /// through consensus. `None` until bound.
    session: Option<u64>,
    /// Monotonically increasing request counter for application requests.
    /// Starts at 1 after registration. Register itself always uses request=0.
    request_counter: u64,
    /// Whether `register_request_id()` has been called.
    register_consumed: bool,
}

impl ConsensusSession {
    /// Create a new session with a random `client_id`.
    #[must_use]
    pub fn new() -> Self {
        Self::with_client_id(generate_client_id())
    }

    /// Create a session with a specific `client_id` (for testing).
    #[must_use]
    pub fn with_client_id(client_id: u128) -> Self {
        Self {
            client_id,
            session: None,
            request_counter: 1,
            register_consumed: false,
        }
    }

    /// The ephemeral client identifier for this session.
    #[must_use]
    pub fn client_id(&self) -> u128 {
        self.client_id
    }

    /// The session number, if registered.
    #[must_use]
    pub fn session(&self) -> Option<u64> {
        self.session
    }

    /// Whether the session is bound (register committed).
    #[must_use]
    pub fn is_bound(&self) -> bool {
        self.session.is_some()
    }

    /// Bind the session after register commits through consensus.
    /// The `session` value is the commit op number from the server's reply.
    ///
    /// # Panics
    /// Panics if already bound (drop and create a new session instead).
    pub fn bind(&mut self, session: u64) {
        assert!(
            self.session.is_none(),
            "session already bound (session={})",
            self.session.unwrap()
        );
        assert!(session > 0, "session must be > 0");
        self.session = Some(session);
    }

    /// Returns the request ID for the register operation (always 0).
    ///
    /// Must be called exactly once, before [`bind`](Self::bind).
    ///
    /// # Panics
    /// Panics if called more than once or after the session is bound.
    pub fn register_request_id(&mut self) -> u64 {
        assert!(
            !self.register_consumed,
            "register_request_id already called"
        );
        assert!(!self.is_bound(), "register_request_id called after bind");
        self.register_consumed = true;
        0
    }

    /// Get the next application request ID and advance the counter.
    ///
    /// Returns 1, 2, 3, ... (request 0 is reserved for register).
    ///
    /// # Panics
    /// Panics if the session is not bound.
    pub fn next_request_id(&mut self) -> u64 {
        assert!(self.is_bound(), "next_request_id called before bind");
        let id = self.request_counter;
        self.request_counter = self
            .request_counter
            .checked_add(1)
            .expect("request counter overflow (u64::MAX requests on a single session)");
        id
    }

    /// Current request counter value (the next ID that will be returned).
    #[must_use]
    pub fn current_request_id(&self) -> u64 {
        self.request_counter
    }
}

impl Default for ConsensusSession {
    fn default() -> Self {
        Self::new()
    }
}

/// Generate an ephemeral random u128 client ID using UUID v4.
///
/// Non-zero by construction (UUID v4 has fixed bits that prevent all-zeros).
fn generate_client_id() -> u128 {
    iggy_common::random_id::get_uuid()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_session_is_unbound() {
        let session = ConsensusSession::new();
        assert!(!session.is_bound());
        assert!(session.session().is_none());
        assert_ne!(session.client_id(), 0);
    }

    #[test]
    fn client_id_is_unique() {
        let s1 = ConsensusSession::new();
        let s2 = ConsensusSession::new();
        assert_ne!(s1.client_id(), s2.client_id());
    }

    #[test]
    fn bind_sets_session() {
        let mut session = ConsensusSession::with_client_id(42);
        session.bind(100);
        assert!(session.is_bound());
        assert_eq!(session.session(), Some(100));
    }

    #[test]
    fn register_request_id_returns_zero() {
        let mut session = ConsensusSession::with_client_id(1);
        assert_eq!(session.register_request_id(), 0);
    }

    #[test]
    fn request_ids_are_monotonic_after_bind() {
        let mut session = ConsensusSession::with_client_id(1);
        let _ = session.register_request_id();
        session.bind(10);
        assert_eq!(session.next_request_id(), 1);
        assert_eq!(session.next_request_id(), 2);
        assert_eq!(session.next_request_id(), 3);
        assert_eq!(session.current_request_id(), 4);
    }

    #[test]
    #[should_panic(expected = "register_request_id already called")]
    fn double_register_request_id_panics() {
        let mut session = ConsensusSession::with_client_id(1);
        let _ = session.register_request_id();
        let _ = session.register_request_id();
    }

    #[test]
    #[should_panic(expected = "next_request_id called before bind")]
    fn next_request_id_before_bind_panics() {
        let mut session = ConsensusSession::with_client_id(1);
        let _ = session.next_request_id();
    }

    #[test]
    #[should_panic(expected = "already bound")]
    fn double_bind_panics() {
        let mut session = ConsensusSession::with_client_id(1);
        session.bind(10);
        session.bind(20);
    }

    #[test]
    fn reconnect_uses_fresh_session() {
        let s1 = ConsensusSession::new();
        let s2 = ConsensusSession::new();
        // Each new session gets a different client_id. No reuse.
        assert_ne!(s1.client_id(), s2.client_id());
        assert!(!s1.is_bound());
        assert!(!s2.is_bound());
    }

    #[test]
    fn with_client_id_deterministic() {
        let session = ConsensusSession::with_client_id(0xDEAD_BEEF);
        assert_eq!(session.client_id(), 0xDEAD_BEEF);
    }
}
