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

//! Combined login + register handler for server-ng.
//!
//! One client-facing command, two internal phases:
//! 1. Verify credentials locally (Argon2). NOT through consensus
//! 2. Submit `Operation::Register` through consensus. All replicas create `ClientTable` entry
//!
//! The handler is trait-based so it can be tested via mocking.

use crate::session_manager::{SessionError, SessionManager};
use iggy_binary_protocol::requests::users::{LoginRegisterRequest, LoginRegisterWithPatRequest};
use iggy_binary_protocol::responses::users::LoginRegisterResponse;
use secrecy::ExposeSecret;

/// Credential verification abstraction.
///
/// The real implementation delegates to the shard's metadata user store
/// and Argon2 password hashing. Test implementations return fixed results.
pub trait CredentialVerifier {
    /// Verify username/password. Returns `user_id` on success.
    ///
    /// # Errors
    /// Returns `LoginRegisterError` if credentials are invalid.
    fn verify(&self, username: &str, password: &str) -> Result<u32, LoginRegisterError>;
}

/// Personal access token verification abstraction.
///
/// The real implementation looks up the PAT by hash in the user store,
/// checks expiry, and returns the owning user's ID.
pub trait TokenVerifier {
    /// Verify a personal access token. Returns `user_id` on success.
    ///
    /// # Errors
    /// Returns `LoginRegisterError` if the token is invalid or expired.
    fn verify_token(&self, token: &str) -> Result<u32, LoginRegisterError>;
}

/// Consensus register submission abstraction.
///
/// The real implementation builds a `RequestHeader { operation: Register }`,
/// calls `check_register` on the `ClientTable`, submits through the consensus
/// pipeline, and awaits the `Notify` for commit. Returns the session number
/// (commit op number).
pub trait RegisterSubmitter {
    /// Submit a register for `client_id` through consensus and await commit.
    /// Returns the session number.
    ///
    /// # Errors
    /// Returns `LoginRegisterError` if consensus fails or pipeline is full.
    fn submit_register(
        &self,
        client_id: u128,
    ) -> impl std::future::Future<Output = Result<u64, LoginRegisterError>> + '_;
}

/// Handle a combined login + register request (username/password).
///
/// 1. Validates input
/// 2. Verifies credentials locally (no consensus)
/// 3. Submits `Register` through consensus.
/// 4. Returns `user_id` + `session`
///
/// # Errors
/// Returns `LoginRegisterError` on auth failure, consensus failure, or
/// session state errors.
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

    // Phase 1: Local credential verification (NOT replicated).
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

/// Handle a combined login + register request (personal access token).
///
/// Same two-phase flow as [`handle_login_register`], but Phase 1 verifies
/// a personal access token instead of username/password.
///
/// # Errors
/// Returns `LoginRegisterError` on token failure, consensus failure, or
/// session state errors.
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

    // Phase 1: Token verification (local, not replicated).
    let user_id = token_verifier.verify_token(request.token.expose_secret())?;

    // Phase 2: Register through consensus (shared).
    complete_register(
        request.client_id,
        user_id,
        submitter,
        session_manager,
        connection_id,
    )
    .await
}

/// Phase 2 - transition session state and register through consensus.
///
/// Called by both password and PAT handlers after their respective Phase 1
/// credential verification succeeds.
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

    // Submit Register through consensus.
    let session = match submitter.submit_register(client_id).await {
        Ok(session) => session,
        Err(e) => {
            // Rollback: Authenticated -> Connected so the client can retry
            // the full login+register on the same connection.
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

#[derive(Debug)]
pub enum LoginRegisterError {
    InvalidClientId,
    InvalidCredentials,
    InvalidToken,
    UserInactive,
    Session(SessionError),
    PipelineFull,
    ConsensusFailed(String),
}

impl std::fmt::Display for LoginRegisterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidClientId => write!(f, "client_id must be non-zero"),
            Self::InvalidCredentials => write!(f, "invalid username or password"),
            Self::InvalidToken => write!(f, "invalid or expired personal access token"),
            Self::UserInactive => write!(f, "user account is inactive"),
            Self::Session(e) => write!(f, "session error: {e}"),
            Self::PipelineFull => write!(f, "consensus pipeline full, try again later"),
            Self::ConsensusFailed(msg) => write!(f, "consensus failed: {msg}"),
        }
    }
}

impl std::error::Error for LoginRegisterError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session_manager::SessionManager;
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
                Err(LoginRegisterError::PipelineFull) => Err(LoginRegisterError::PipelineFull),
                Err(LoginRegisterError::ConsensusFailed(msg)) => {
                    Err(LoginRegisterError::ConsensusFailed(msg.clone()))
                }
                _ => Err(LoginRegisterError::ConsensusFailed(
                    "mock error".to_string(),
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
                session: Err(LoginRegisterError::PipelineFull),
            };
            let req = make_request(0xDEAD);

            let err = handle_login_register(&req, &verifier, &submitter, &mut mgr, conn)
                .await
                .unwrap_err();

            assert!(matches!(err, LoginRegisterError::PipelineFull));
            assert!(mgr.get_session(conn).is_none());

            // Connection rolled back to Connected. Retry.
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
                session: Err(LoginRegisterError::PipelineFull),
            };
            let req = make_pat_request(0xDEAD);

            let err = handle_login_register_with_pat(&req, &verifier, &submitter, &mut mgr, conn)
                .await
                .unwrap_err();

            assert!(matches!(err, LoginRegisterError::PipelineFull));
            assert!(mgr.get_session(conn).is_none());

            // Connection rolled back to Connected. Retry.
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
