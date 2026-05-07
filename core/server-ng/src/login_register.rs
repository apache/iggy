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

//! Combined login + register handler.
//!
//! One client command, two phases:
//! 1. Local credential verify (Argon2). Not consensus.
//! 2. `Operation::Register` through consensus -> `ClientTable` entry on all replicas.
//!
//! Trait-based for mocking.

use crate::session_manager::{SessionError, SessionManager};
use iggy_binary_protocol::EvictionReason;
use iggy_binary_protocol::requests::users::{LoginRegisterRequest, LoginRegisterWithPatRequest};
use iggy_binary_protocol::responses::users::LoginRegisterResponse;
use metadata::RegisterSubmitError;
use secrecy::ExposeSecret;

/// Credential verifier. Real impl: metadata user store + Argon2.
pub trait CredentialVerifier {
    /// Verify username/password. Returns `user_id`.
    ///
    /// # Errors
    /// `LoginRegisterError` on invalid credentials.
    fn verify(&self, username: &str, password: &str) -> Result<u32, LoginRegisterError>;
}

/// PAT verifier. Real impl: hash lookup + expiry check.
pub trait TokenVerifier {
    /// Verify PAT. Returns `user_id`.
    ///
    /// # Errors
    /// `LoginRegisterError` on invalid/expired token.
    fn verify_token(&self, token: &str) -> Result<u32, LoginRegisterError>;
}

/// Submit `Register` through consensus.
///
/// # Runtime
/// `Future` is **not `Send`**. Production
/// ([`crate::register_submitter::IggyRegisterSubmitter`]) wraps
/// `IggyMetadata::submit_register_in_process`, whose state is `RefCell`/`Cell`
/// on single-threaded `compio`. Multi-threaded embedders need a shim or
/// custom transport; constraining `Send` would tax every call site.
///
/// # Failures
/// Transient (pipeline-full, view-change cancel, primary-not-caught-up,
/// in-flight register) MUST eventually be absorbed via bounded retry. Until
/// then they surface as [`LoginRegisterError::Transient`] → `NotEvictable`,
/// so network can't ship a wire-terminal `Eviction` for a recoverable
/// failure. SDK read-timeout replays.
///
/// Terminal → [`EvictionReason`] in `EvictionHeader`; SDK invokes its
/// eviction callback and stops.
pub trait RegisterSubmitter {
    /// Submit register for `client_id`, await commit. Returns session number.
    ///
    /// # Errors
    /// Only genuinely terminal failures. Transients absorbed silently per contract.
    fn submit_register(
        &self,
        client_id: u128,
    ) -> impl std::future::Future<Output = Result<u64, LoginRegisterError>> + '_;
}

/// Handle login + register (username/password).
/// Validate -> local credential verify → Register through consensus →
/// `user_id` + `session`.
///
/// # Errors
/// Auth, consensus, or session state error.
#[allow(clippy::future_not_send)]
pub async fn handle_login_register<V: CredentialVerifier, R: RegisterSubmitter>(
    request: &LoginRegisterRequest,
    verifier: &V,
    submitter: &R,
    session_manager: &mut SessionManager,
    connection_id: u64,
) -> Result<LoginRegisterResponse, LoginRegisterError> {
    if request.client_id == 0 {
        return Err(LoginRegisterError::InvalidClientId);
    }

    // Phase 1: local verify (not replicated).
    let user_id = verifier.verify(request.username.as_str(), request.password.expose_secret())?;

    // Phase 2: Register through consensus.
    complete_register(
        request.client_id,
        user_id,
        submitter,
        session_manager,
        connection_id,
    )
    .await
}

/// PAT variant of [`handle_login_register`]; Phase 1 verifies token.
///
/// # Errors
/// Token, consensus, or session state error.
#[allow(clippy::future_not_send)]
pub async fn handle_login_register_with_pat<T: TokenVerifier, R: RegisterSubmitter>(
    request: &LoginRegisterWithPatRequest,
    token_verifier: &T,
    submitter: &R,
    session_manager: &mut SessionManager,
    connection_id: u64,
) -> Result<LoginRegisterResponse, LoginRegisterError> {
    if request.client_id == 0 {
        return Err(LoginRegisterError::InvalidClientId);
    }

    // Phase 1: token verify (local, not replicated).
    let user_id = token_verifier.verify_token(request.token.expose_secret())?;

    // Phase 2: Register through consensus.
    complete_register(
        request.client_id,
        user_id,
        submitter,
        session_manager,
        connection_id,
    )
    .await
}

/// Phase 2: transition session state, submit Register. Shared by password + PAT handlers.
#[allow(clippy::future_not_send)]
async fn complete_register<R: RegisterSubmitter>(
    client_id: u128,
    user_id: u32,
    submitter: &R,
    session_manager: &mut SessionManager,
    connection_id: u64,
) -> Result<LoginRegisterResponse, LoginRegisterError> {
    // Transition: Connected -> Authenticated.
    session_manager
        .login(connection_id, user_id)
        .map_err(LoginRegisterError::Session)?;

    // Submit Register.
    let session = match submitter.submit_register(client_id).await {
        Ok(session) => session,
        Err(e) => {
            // Rollback Authenticated -> Connected so client can retry full flow.
            let _ = session_manager.reset_to_connected(connection_id);
            return Err(e);
        }
    };

    // Transition: Authenticated -> Bound.
    session_manager
        .bind_session(connection_id, client_id, session)
        .map_err(LoginRegisterError::Session)?;

    Ok(LoginRegisterResponse { user_id, session })
}

/// Login/register failure.
///
/// Most variants -> [`EvictionReason`] (via [`TryFrom<&LoginRegisterError>`]).
/// Two variants are `NotEvictable`:
/// - [`LoginRegisterError::InvalidClientId`], see variant doc.
/// - [`LoginRegisterError::Transient`], recoverable; SDK read-timeout replays.
///
/// `#[non_exhaustive]`: external matchers need a wildcard arm.
#[derive(Debug)]
#[non_exhaustive]
pub enum LoginRegisterError {
    /// `client_id == 0` (reserved sentinel). No frame can address it
    /// ([`EvictionHeader::validate`] rejects); silent close.
    /// `TryFrom` -> `Err(NotEvictable)`.
    ///
    /// [`EvictionHeader::validate`]: iggy_binary_protocol::EvictionHeader
    InvalidClientId,
    InvalidCredentials,
    InvalidToken,
    UserInactive,
    Session(SessionError),
    /// Recoverable consensus failure. Caller rolls back to `Connected`;
    /// SDK read-timeout replays. `TryFrom` -> `Err(NotEvictable)`.
    Transient(RegisterSubmitError),
}

impl std::fmt::Display for LoginRegisterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidClientId => write!(f, "client_id must be non-zero"),
            Self::InvalidCredentials => write!(f, "invalid username or password"),
            Self::InvalidToken => write!(f, "invalid or expired personal access token"),
            Self::UserInactive => write!(f, "user account is inactive"),
            Self::Session(e) => write!(f, "session error: {e}"),
            Self::Transient(e) => write!(f, "transient consensus failure: {e}"),
        }
    }
}

impl std::error::Error for LoginRegisterError {}

/// `TryFrom<&LoginRegisterError>` for [`EvictionReason`] returns this when
/// no wire mapping exists. Caller closes silently / lets client retry.
///
/// - `InvalidClientId`: `client_id == 0`, no addressable client.
/// - `Transient`: wire-terminal Eviction would contradict the absorb-silently
///   contract; failure is recoverable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotEvictable;

impl std::fmt::Display for NotEvictable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("error has no wireable EvictionReason; close connection silently")
    }
}

impl std::error::Error for NotEvictable {}

/// **No production caller yet**, pins the contract for the eventual
/// wire-eviction dispatcher: `try_from(&err)` -> on `Ok(reason)` build
/// [`iggy_binary_protocol::EvictionHeader`] via
/// [`consensus::build_eviction_message`] and ship+close; on `NotEvictable`
/// close silently.
///
/// `#[non_exhaustive]` + this exhaustive match force every future
/// `LoginRegisterError` through compile-time triage.
impl TryFrom<&LoginRegisterError> for EvictionReason {
    type Error = NotEvictable;

    // Per-variant arms (not `A | B`), so each carries its own NotEvictable
    // reason. Distinct cases (no addressable client_id vs recoverable);
    // merging would lose docs.
    #[allow(clippy::match_same_arms)]
    fn try_from(err: &LoginRegisterError) -> Result<Self, Self::Error> {
        Ok(match err {
            // No client_id to address,  close silently. See variant doc.
            LoginRegisterError::InvalidClientId => return Err(NotEvictable),
            // Recoverable: caller rolls back to Connected; SDK retries.
            // Wire Eviction would erase a recoverable session.
            LoginRegisterError::Transient(_) => return Err(NotEvictable),
            LoginRegisterError::InvalidCredentials => Self::InvalidCredentials,
            LoginRegisterError::InvalidToken => Self::InvalidToken,
            LoginRegisterError::UserInactive => Self::UserInactive,
            LoginRegisterError::Session(_) => Self::SessionError,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session_manager::SessionManager;
    use consensus::{EvictionContext, build_eviction_message};
    use iggy_binary_protocol::{Command2, ConsensusHeader, HEADER_SIZE};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    struct MockVerifier {
        result: Result<u32, LoginRegisterError>,
    }

    impl CredentialVerifier for MockVerifier {
        fn verify(&self, _username: &str, _password: &str) -> Result<u32, LoginRegisterError> {
            match &self.result {
                Ok(uid) => Ok(*uid),
                Err(LoginRegisterError::InvalidCredentials) => {
                    Err(LoginRegisterError::InvalidCredentials)
                }
                Err(LoginRegisterError::UserInactive) => Err(LoginRegisterError::UserInactive),
                _ => Err(LoginRegisterError::InvalidCredentials),
            }
        }
    }

    struct MockSubmitter {
        session: Result<u64, LoginRegisterError>,
    }

    impl RegisterSubmitter for MockSubmitter {
        async fn submit_register(&self, _client_id: u128) -> Result<u64, LoginRegisterError> {
            match &self.session {
                Ok(s) => Ok(*s),
                Err(LoginRegisterError::Transient(e)) => {
                    Err(LoginRegisterError::Transient(e.clone()))
                }
                _ => Err(LoginRegisterError::Transient(
                    RegisterSubmitError::PipelineFull,
                )),
            }
        }
    }

    fn make_request(client_id: u128) -> LoginRegisterRequest {
        LoginRegisterRequest {
            client_id,
            username: iggy_binary_protocol::WireName::new("admin").unwrap(),
            password: secrecy::SecretString::from("secret"),
            version: None,
            client_context: None,
        }
    }

    macro_rules! block_on {
        ($e:expr) => {
            futures::executor::block_on($e)
        };
    }

    #[test]
    fn happy_path() {
        block_on!(async {
            let mut mgr = SessionManager::new();
            let conn = mgr.add_connection(addr(5000));
            let verifier = MockVerifier { result: Ok(42) };
            let submitter = MockSubmitter { session: Ok(100) };
            let req = make_request(0xDEAD);

            let resp = handle_login_register(&req, &verifier, &submitter, &mut mgr, conn)
                .await
                .unwrap();

            assert_eq!(resp.user_id, 42);
            assert_eq!(resp.session, 100);
            assert_eq!(mgr.get_session(conn), Some((0xDEAD, 100)));
            assert_eq!(mgr.bound_count(), 1);
        });
    }

    #[test]
    fn auth_failure_stays_connected() {
        block_on!(async {
            let mut mgr = SessionManager::new();
            let conn = mgr.add_connection(addr(5000));
            let verifier = MockVerifier {
                result: Err(LoginRegisterError::InvalidCredentials),
            };
            let submitter = MockSubmitter { session: Ok(100) };
            let req = make_request(0xDEAD);

            let err = handle_login_register(&req, &verifier, &submitter, &mut mgr, conn)
                .await
                .unwrap_err();

            assert!(matches!(err, LoginRegisterError::InvalidCredentials));
            assert!(mgr.get_session(conn).is_none());
        });
    }

    #[test]
    fn consensus_failure_rolls_back_to_connected() {
        block_on!(async {
            let mut mgr = SessionManager::new();
            let conn = mgr.add_connection(addr(5000));
            let verifier = MockVerifier { result: Ok(42) };
            let submitter = MockSubmitter {
                session: Err(LoginRegisterError::Transient(
                    RegisterSubmitError::PipelineFull,
                )),
            };
            let req = make_request(0xDEAD);

            let err = handle_login_register(&req, &verifier, &submitter, &mut mgr, conn)
                .await
                .unwrap_err();

            assert!(matches!(err, LoginRegisterError::Transient(_)));
            assert!(mgr.get_session(conn).is_none());

            // Rolled back to Connected; retry succeeds.
            let submitter_ok = MockSubmitter { session: Ok(100) };
            let resp = handle_login_register(&req, &verifier, &submitter_ok, &mut mgr, conn)
                .await
                .unwrap();
            assert_eq!(resp.user_id, 42);
            assert_eq!(resp.session, 100);
            assert_eq!(mgr.get_session(conn), Some((0xDEAD, 100)));
        });
    }

    #[test]
    fn zero_client_id_rejected() {
        block_on!(async {
            let mut mgr = SessionManager::new();
            let conn = mgr.add_connection(addr(5000));
            let verifier = MockVerifier { result: Ok(42) };
            let submitter = MockSubmitter { session: Ok(100) };
            let req = make_request(0);

            let err = handle_login_register(&req, &verifier, &submitter, &mut mgr, conn)
                .await
                .unwrap_err();

            assert!(matches!(err, LoginRegisterError::InvalidClientId));
        });
    }

    #[test]
    fn login_register_error_maps_to_eviction_reason() {
        let cases: &[(LoginRegisterError, EvictionReason)] = &[
            (
                LoginRegisterError::InvalidCredentials,
                EvictionReason::InvalidCredentials,
            ),
            (
                LoginRegisterError::InvalidToken,
                EvictionReason::InvalidToken,
            ),
            (
                LoginRegisterError::UserInactive,
                EvictionReason::UserInactive,
            ),
            (
                LoginRegisterError::Session(SessionError::ConnectionNotFound(0)),
                EvictionReason::SessionError,
            ),
        ];
        for (err, expected) in cases {
            let mapped = EvictionReason::try_from(err).expect("variant should be wireable");
            assert_eq!(mapped, *expected, "mapping for {err:?}");
        }
    }

    // Transient must NOT be wireable, point of the variant. Callers rely
    // on NotEvictable to roll back instead of shipping a terminal frame.
    #[test]
    fn transient_is_not_evictable() {
        let err = LoginRegisterError::Transient(RegisterSubmitError::PipelineFull);
        let mapped = EvictionReason::try_from(&err);
        assert_eq!(mapped, Err(NotEvictable));
    }

    // InvalidClientId: client_id == 0 cannot address a frame, caller
    // must handle Err path (silent close).
    #[test]
    fn invalid_client_id_is_not_evictable() {
        let err = LoginRegisterError::InvalidClientId;
        let mapped = EvictionReason::try_from(&err);
        assert_eq!(mapped, Err(NotEvictable));
    }

    #[test]
    fn build_eviction_message_carries_typed_reason() {
        let ctx = EvictionContext {
            cluster: 7,
            view: 3,
            replica: 1,
        };
        let msg = build_eviction_message(ctx, 0xCAFE, EvictionReason::SessionError);
        let header = msg.header();
        assert_eq!(header.command, Command2::Eviction);
        assert_eq!(header.cluster, 7);
        assert_eq!(header.view, 3);
        assert_eq!(header.replica, 1);
        assert_eq!(header.client, 0xCAFE);
        assert_eq!(header.reason, EvictionReason::SessionError);
        assert_eq!(header.size as usize, HEADER_SIZE);
        // validate accepts.
        assert!(header.validate().is_ok());
    }

    // PAT tests

    struct MockTokenVerifier {
        result: Result<u32, LoginRegisterError>,
    }

    impl TokenVerifier for MockTokenVerifier {
        fn verify_token(&self, _token: &str) -> Result<u32, LoginRegisterError> {
            match &self.result {
                Ok(uid) => Ok(*uid),
                Err(LoginRegisterError::UserInactive) => Err(LoginRegisterError::UserInactive),
                _ => Err(LoginRegisterError::InvalidToken),
            }
        }
    }

    fn make_pat_request(client_id: u128) -> LoginRegisterWithPatRequest {
        LoginRegisterWithPatRequest {
            client_id,
            token: secrecy::SecretString::from("test-pat-token"),
            version: None,
            client_context: None,
        }
    }

    #[test]
    fn pat_happy_path() {
        block_on!(async {
            let mut mgr = SessionManager::new();
            let conn = mgr.add_connection(addr(5000));
            let verifier = MockTokenVerifier { result: Ok(42) };
            let submitter = MockSubmitter { session: Ok(100) };
            let req = make_pat_request(0xDEAD);

            let resp = handle_login_register_with_pat(&req, &verifier, &submitter, &mut mgr, conn)
                .await
                .unwrap();

            assert_eq!(resp.user_id, 42);
            assert_eq!(resp.session, 100);
            assert_eq!(mgr.get_session(conn), Some((0xDEAD, 100)));
            assert_eq!(mgr.bound_count(), 1);
        });
    }

    #[test]
    fn pat_auth_failure_stays_connected() {
        block_on!(async {
            let mut mgr = SessionManager::new();
            let conn = mgr.add_connection(addr(5000));
            let verifier = MockTokenVerifier {
                result: Err(LoginRegisterError::InvalidToken),
            };
            let submitter = MockSubmitter { session: Ok(100) };
            let req = make_pat_request(0xDEAD);

            let err = handle_login_register_with_pat(&req, &verifier, &submitter, &mut mgr, conn)
                .await
                .unwrap_err();

            assert!(matches!(err, LoginRegisterError::InvalidToken));
            assert!(mgr.get_session(conn).is_none());
        });
    }

    #[test]
    fn pat_consensus_failure_rolls_back_to_connected() {
        block_on!(async {
            let mut mgr = SessionManager::new();
            let conn = mgr.add_connection(addr(5000));
            let verifier = MockTokenVerifier { result: Ok(42) };
            let submitter = MockSubmitter {
                session: Err(LoginRegisterError::Transient(
                    RegisterSubmitError::PipelineFull,
                )),
            };
            let req = make_pat_request(0xDEAD);

            let err = handle_login_register_with_pat(&req, &verifier, &submitter, &mut mgr, conn)
                .await
                .unwrap_err();

            assert!(matches!(err, LoginRegisterError::Transient(_)));
            assert!(mgr.get_session(conn).is_none());

            // Rolled back to Connected; retry succeeds.
            let submitter_ok = MockSubmitter { session: Ok(100) };
            let resp =
                handle_login_register_with_pat(&req, &verifier, &submitter_ok, &mut mgr, conn)
                    .await
                    .unwrap();
            assert_eq!(resp.user_id, 42);
            assert_eq!(resp.session, 100);
            assert_eq!(mgr.get_session(conn), Some((0xDEAD, 100)));
        });
    }

    #[test]
    fn pat_zero_client_id_rejected() {
        block_on!(async {
            let mut mgr = SessionManager::new();
            let conn = mgr.add_connection(addr(5000));
            let verifier = MockTokenVerifier { result: Ok(42) };
            let submitter = MockSubmitter { session: Ok(100) };
            let req = make_pat_request(0);

            let err = handle_login_register_with_pat(&req, &verifier, &submitter, &mut mgr, conn)
                .await
                .unwrap_err();

            assert!(matches!(err, LoginRegisterError::InvalidClientId));
        });
    }
}
