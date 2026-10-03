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

//! A logical session keeps its identity, bind secret, and request counter
//! across transport reconnects. Registration retries recover the original
//! committed session. Explicit logout starts a new logical session.
//!
//! # Lifecycle
//!
//! Create one session per logical login. `begin_register` preserves its client
//! identity and secret when registration is retried. `bind` accepts only a
//! nonzero epoch matching any previous binding. Application request IDs are
//! available only after binding; exhaustion fails instead of wrapping.
//! Both `bind` and `next_request_id` return `Result` and callers must handle
//! failure. Explicit disconnect clears sign-in and requires a new login.

use iggy_binary_protocol::requests::users::login_register::BindSecret;
use iggy_common::IggyError;

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
    bind_secret: BindSecret,
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
            bind_secret: BindSecret::new(Box::new(rand::random())),
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

    /// Registration proof retained across reconnects and session bindings.
    pub fn bind_secret(&self) -> BindSecret {
        self.bind_secret.clone()
    }

    /// Accept the original registration, including its replay after reconnect.
    pub fn bind(&mut self, session: u64) -> Result<(), IggyError> {
        if session == 0 {
            return Err(IggyError::InvalidSession(session));
        }
        if let Some(bound) = self.session
            && bound != session
        {
            return Err(IggyError::SessionMismatch(bound, session));
        }
        self.session = Some(session);
        Ok(())
    }

    /// Returns the request ID for the register operation (always 0).
    ///
    pub const fn register_request_id(&self) -> u64 {
        0
    }

    /// Start or retry registration without resetting the identity or counter.
    pub const fn begin_register(&self) -> u64 {
        self.register_request_id()
    }

    /// Get the next application request ID and advance the counter.
    ///
    /// Returns 1, 2, 3, ... (request 0 is reserved for register).
    ///
    pub fn next_request_id(&mut self) -> Result<u64, IggyError> {
        if !self.is_bound() {
            return Err(IggyError::Unauthenticated);
        }
        let id = self.request_counter;
        self.request_counter = self
            .request_counter
            .checked_add(1)
            .ok_or(IggyError::RequestIdExhausted)?;
        Ok(id)
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
    use secrecy::ExposeSecret;

    #[test]
    fn new_session_is_unbound_and_has_unique_credentials() {
        let first = ConsensusSession::new();
        let second = ConsensusSession::new();
        assert!(!first.is_bound());
        assert_ne!(first.client_id(), 0);
        assert_ne!(first.client_id(), second.client_id());
        assert_ne!(
            first.bind_secret().expose_secret(),
            second.bind_secret().expose_secret()
        );
    }

    #[test]
    fn lost_registration_reply_preserves_identity_and_secret() {
        let session = ConsensusSession::with_client_id(7);
        let secret = session.bind_secret();
        assert_eq!(session.begin_register(), 0);
        assert_eq!(session.begin_register(), 0);
        assert_eq!(session.client_id(), 7);
        assert_eq!(
            session.bind_secret().expose_secret(),
            secret.expose_secret()
        );
        assert!(!session.is_bound());
    }

    #[test]
    fn reconnect_preserves_binding_and_request_counter() {
        let mut session = ConsensusSession::with_client_id(7);
        session.bind(42).unwrap();
        assert_eq!(session.next_request_id().unwrap(), 1);
        assert_eq!(session.begin_register(), 0);
        session.bind(42).unwrap();
        assert_eq!(session.client_id(), 7);
        assert_eq!(session.session(), Some(42));
        assert_eq!(session.next_request_id().unwrap(), 2);
    }

    #[test]
    fn conflicting_registration_cannot_replace_a_live_session() {
        let mut session = ConsensusSession::with_client_id(7);
        assert_eq!(session.bind(0), Err(IggyError::InvalidSession(0)));
        session.bind(42).unwrap();
        assert!(matches!(
            session.bind(43),
            Err(IggyError::SessionMismatch(42, 43))
        ));
        assert_eq!(session.session(), Some(42));
    }

    #[test]
    fn unbound_session_cannot_number_a_mutation() {
        let mut session = ConsensusSession::with_client_id(7);
        assert_eq!(session.next_request_id(), Err(IggyError::Unauthenticated));
        assert_eq!(session.current_request_id(), 1);
    }

    #[test]
    fn exhausted_counter_cannot_wrap_or_issue_another_request() {
        let mut session = ConsensusSession::with_client_id(7);
        session.bind(42).unwrap();
        session.request_counter = u64::MAX;
        assert_eq!(
            session.next_request_id(),
            Err(IggyError::RequestIdExhausted)
        );
        assert_eq!(session.current_request_id(), u64::MAX);
    }
}
