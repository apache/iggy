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
use consensus::VsrConsensus;
use iggy_binary_protocol::{
    EvictionReason, Message, PrepareHeader,
    requests::users::{LoginRegisterRequest, LoginRegisterWithPatRequest},
    responses::users::LoginRegisterResponse,
};
use journal::{Journal, JournalHandle};
use message_bus::MessageBus;
use metadata::{
    IggyMetadata, RegisterSubmitError, impls::metadata::StreamsFrontend, stm::StateMachine,
};
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

/// Bounds for [`IggyMetadata`] threaded through the login/register handlers.
///
/// Re-stated rather than aliased so the public handler signatures stay
/// readable at call sites that already type their metadata instance.
type LoginMetadata<'a, B, J, S, M> = IggyMetadata<VsrConsensus<B>, J, S, M>;

/// Handle login + register (username/password).
/// Validate -> local credential verify → Register through consensus →
/// `user_id` + `session`.
///
/// # Errors
/// Auth, consensus, or session state error.
#[allow(clippy::future_not_send)]
pub async fn handle_login_register<V, B, J, S, M>(
    request: &LoginRegisterRequest,
    verifier: &V,
    metadata: &LoginMetadata<'_, B, J, S, M>,
    session_manager: &mut SessionManager,
    connection_id: u64,
) -> Result<LoginRegisterResponse, LoginRegisterError>
where
    V: CredentialVerifier,
    B: MessageBus,
    J: JournalHandle,
    J::Target: Journal<J::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    M: StreamsFrontend
        + StateMachine<
            Input = Message<PrepareHeader>,
            Output = bytes::Bytes,
            Error = iggy_common::IggyError,
        >,
{
    if request.client_id == 0 {
        return Err(LoginRegisterError::InvalidClientId);
    }

    // Phase 1: local verify (not replicated).
    let user_id = verifier.verify(request.username.as_str(), request.password.expose_secret())?;

    // Phase 2: Register through consensus.
    complete_register(
        request.client_id,
        user_id,
        metadata,
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
pub async fn handle_login_register_with_pat<T, B, J, S, M>(
    request: &LoginRegisterWithPatRequest,
    token_verifier: &T,
    metadata: &LoginMetadata<'_, B, J, S, M>,
    session_manager: &mut SessionManager,
    connection_id: u64,
) -> Result<LoginRegisterResponse, LoginRegisterError>
where
    T: TokenVerifier,
    B: MessageBus,
    J: JournalHandle,
    J::Target: Journal<J::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    M: StreamsFrontend
        + StateMachine<
            Input = Message<PrepareHeader>,
            Output = bytes::Bytes,
            Error = iggy_common::IggyError,
        >,
{
    if request.client_id == 0 {
        return Err(LoginRegisterError::InvalidClientId);
    }

    // Phase 1: token verify (local, not replicated).
    let user_id = token_verifier.verify_token(request.token.expose_secret())?;

    // Phase 2: Register through consensus.
    complete_register(
        request.client_id,
        user_id,
        metadata,
        session_manager,
        connection_id,
    )
    .await
}

/// Phase 2: transition session state, submit Register. Shared by password + PAT handlers.
#[allow(clippy::future_not_send)]
async fn complete_register<B, J, S, M>(
    client_id: u128,
    user_id: u32,
    metadata: &LoginMetadata<'_, B, J, S, M>,
    session_manager: &mut SessionManager,
    connection_id: u64,
) -> Result<LoginRegisterResponse, LoginRegisterError>
where
    B: MessageBus,
    J: JournalHandle,
    J::Target: Journal<J::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    M: StreamsFrontend
        + StateMachine<
            Input = Message<PrepareHeader>,
            Output = bytes::Bytes,
            Error = iggy_common::IggyError,
        >,
{
    // Transition: Connected -> Authenticated.
    session_manager
        .login(connection_id, user_id)
        .map_err(LoginRegisterError::Session)?;

    // Submit Register. All RegisterSubmitError variants are transient by
    // contract (see [`RegisterSubmitError`] docs); wrap into Transient so
    // TryFrom<&LoginRegisterError> for EvictionReason returns NotEvictable
    // and the network layer can't ship a wire-terminal Eviction for a
    // recoverable failure. SDK read-timeout replays.
    let session = match metadata.submit_register_in_process(client_id).await {
        Ok(session) => session,
        Err(e) => {
            triage_submit_error(&e);
            // Rollback Authenticated -> Connected so client can retry full flow.
            let _ = session_manager.reset_to_connected(connection_id);
            return Err(LoginRegisterError::Transient(e));
        }
    };

    // Transition: Authenticated -> Bound.
    session_manager
        .bind_session(connection_id, client_id, session)
        .map_err(LoginRegisterError::Session)?;

    Ok(LoginRegisterResponse { user_id, session })
}

/// Surface unclassified [`RegisterSubmitError`] variants in debug builds.
///
/// `RegisterSubmitError` is `#[non_exhaustive]`; the wildcard arm guards
/// against a new variant slipping through without an explicit decision
/// about wire-eviction safety. Today every variant is transient, so the
/// wildcard never fires; if metadata adds a terminal variant the
/// `debug_assert` flags it so the dev wires a proper `EvictionReason`.
//
// TODO(absorb-silently): trait wants transient absorbed via bounded
// retry; today they surface as Transient; arms will collapse once
// retry lands.
fn triage_submit_error(e: &RegisterSubmitError) {
    match e {
        RegisterSubmitError::NotPrimary
        | RegisterSubmitError::NotCaughtUp
        | RegisterSubmitError::PipelineFull
        | RegisterSubmitError::InProgress
        | RegisterSubmitError::Canceled => {}
        other => {
            tracing::warn!(
                error = ?other,
                "triage_submit_error: unclassified RegisterSubmitError; default Transient, add explicit arm"
            );
            debug_assert!(
                false,
                "triage_submit_error: unclassified RegisterSubmitError {other:?}"
            );
        }
    }
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
    use consensus::{EvictionContext, build_eviction_message};
    use iggy_binary_protocol::{Command2, ConsensusHeader, HEADER_SIZE};

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

    // triage_submit_error must not debug_assert on any classified variant.
    // If metadata adds a new RegisterSubmitError variant, the wildcard arm
    // fires here and the dev wires it through explicitly.
    #[test]
    fn triage_submit_error_accepts_all_known_variants() {
        let known = [
            RegisterSubmitError::NotPrimary,
            RegisterSubmitError::NotCaughtUp,
            RegisterSubmitError::PipelineFull,
            RegisterSubmitError::InProgress,
            RegisterSubmitError::Canceled,
        ];
        for variant in &known {
            triage_submit_error(variant);
        }
    }
}
