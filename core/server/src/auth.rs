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

//! Local credential verification and login/register completion.
//!
//! Verifies password + PAT credentials locally, then runs the consensus
//! `Register` proposal on the metadata owner; terminal failures are
//! surfaced as typed `Eviction` frames, transient ones as result-framed
//! replay hints.

use crate::bootstrap::{ShellBus, ShellShard};
use crate::dispatch::{send_login_eviction, submit_register_on_owner};
use crate::login_register::LoginRegisterError;
use crate::responses::{build_login_register_reply, current_metadata_commit};
use crate::session_manager::{ClientSdkInfo, SessionManager};
use consensus::{MetadataHandle, build_result_rejection_reply};
use iggy_binary_protocol::PrepareHeader;
use iggy_binary_protocol::{ClientVersionInfo, EvictionReason, RoutedRequestHeader};
use iggy_common::defaults::{
    MAX_PASSWORD_LENGTH, MAX_USERNAME_LENGTH, MIN_PASSWORD_LENGTH, MIN_USERNAME_LENGTH,
};
use iggy_common::{IggyError, IggyTimestamp, PersonalAccessToken, UserStatus};
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use metadata::MetadataSubmitError;
use metadata::impls::metadata::StreamsFrontend;
use server_common::Message;
use server_common::crypto;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::LazyLock;
use tracing::warn;

/// A well-formed Argon2 hash to verify against on the unknown-user login
/// branch, so a missing username costs the same single `verify_password` a real
/// user's wrong-password branch costs. Closes the username-existence timing
/// oracle without changing the returned error. On the unknown-username branch
/// the verify result is discarded, so even a request presenting the exact dummy
/// plaintext cannot authenticate; the literal only needs to be a fixed input
/// hashed by the same Argon2 hasher real users use, so the dummy verify runs an
/// identical Argon2 KDF.
static DUMMY_PASSWORD_HASH: LazyLock<String> =
    LazyLock::new(|| crypto::hash_password("http-login-timing-guard"));

/// Pay the one-time Argon2 cost of [`DUMMY_PASSWORD_HASH`] at boot instead of
/// inside the first unknown-username login request.
pub(crate) fn warm_dummy_password_hash() {
    LazyLock::force(&DUMMY_PASSWORD_HASH);
}

pub(crate) fn verify_login_credentials<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    username: &str,
    password: &str,
) -> Result<u32, LoginRegisterError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    // Same bounds the legacy server enforces before any lookup or hashing;
    // also keeps arbitrary-length input out of the password hash. Collapsed
    // to InvalidCredentials on purpose (legacy: InvalidUsername /
    // InvalidPassword): don't leak which field failed.
    if !(MIN_USERNAME_LENGTH..=MAX_USERNAME_LENGTH).contains(&username.len())
        || !(MIN_PASSWORD_LENGTH..=MAX_PASSWORD_LENGTH).contains(&password.len())
    {
        return Err(LoginRegisterError::InvalidCredentials);
    }
    shard.plane.metadata().mux_stm.users().read(|users| {
        let user = users
            .index
            .get(username)
            .copied()
            .and_then(|user_id| users.items.get(user_id as usize));
        let Some(user) = user else {
            // Constant-cost path: verify against a dummy hash so a missing
            // username is indistinguishable by response timing from a wrong
            // password (both return InvalidCredentials).
            let _ = crypto::verify_password(password, DUMMY_PASSWORD_HASH.as_str());
            return Err(LoginRegisterError::InvalidCredentials);
        };
        // Verify before the status check and collapse inactive to
        // InvalidCredentials: an inactive account must answer exactly like a
        // wrong password (same error, same Argon2 cost), or login could probe
        // which accounts exist but are disabled.
        if !crypto::verify_password(password, user.password_hash.as_ref())
            || user.status != UserStatus::Active
        {
            return Err(LoginRegisterError::InvalidCredentials);
        }
        Ok(user.id)
    })
}

pub(crate) fn verify_pat_credentials<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    token: &str,
) -> Result<u32, LoginRegisterError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    verify_pat_credentials_with_expiry(shard, token).map(|(user_id, _)| user_id)
}

/// Like [`verify_pat_credentials`] but also surfaces the token's expiry (unix
/// seconds, `u64::MAX` when the PAT never expires). The HTTP extractor keys a
/// per-token VSR session table on this expiry for lazy eviction; the wire and
/// login paths only need the user id and go through [`verify_pat_credentials`].
pub(crate) fn verify_pat_credentials_with_expiry<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    token: &str,
) -> Result<(u32, u64), LoginRegisterError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let token_hash = PersonalAccessToken::hash_token(token);
    // PAT expiry gates the login accept/reject, and that outcome folds into
    // the reply, so read the environment-injected bus clock (seed-derived
    // under the simulator), not the wall clock, or a replayed login diverges.
    // The two sibling wall-clock reads in `dispatch` stay direct because both
    // are off the reply path (diagnostic-only). The bus seam exists on every
    // shard, so this holds even when login lands on an entry shard that does
    // not own the metadata consensus.
    let now = IggyTimestamp::from(shard.bus.realtime_micros());
    shard.plane.metadata().mux_stm.users().read(|users| {
        let Some((user_id, token_name)) =
            users.personal_access_token_index.get(token_hash.as_str())
        else {
            return Err(LoginRegisterError::InvalidToken);
        };
        let Some(pat) = users
            .personal_access_tokens
            .get(user_id)
            .and_then(|tokens| tokens.get(token_name))
        else {
            return Err(LoginRegisterError::InvalidToken);
        };
        if pat.is_expired(now) {
            return Err(LoginRegisterError::InvalidToken);
        }
        let Some(user) = users.items.get(*user_id as usize) else {
            return Err(LoginRegisterError::InvalidToken);
        };
        if user.status != UserStatus::Active {
            return Err(LoginRegisterError::UserInactive);
        }
        // `expiry_at == None` is a never-expiring PAT; map it to `u64::MAX` so
        // the HTTP session table never expiry-evicts its entry.
        let expiry = pat
            .expiry_at
            .map_or(u64::MAX, |expiry_at| expiry_at.to_secs());
        Ok((user.id, expiry))
    })
}

#[allow(clippy::future_not_send)]
pub(crate) async fn complete_login_register<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
    vsr_client_id: u128,
    request_header: &RoutedRequestHeader,
    user_id: u32,
    client_version: &ClientVersionInfo,
) -> Result<(), LoginRegisterError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let sdk_info = ClientSdkInfo {
        sdk_name: client_version.sdk_name.as_str().to_owned(),
        sdk_version: client_version.sdk_version.as_str().to_owned(),
        protocol_version: client_version.protocol_version,
    };
    let existing_session = {
        let sessions = sessions.borrow();
        sessions
            .get_session(transport_client_id)
            .map(|(_, session)| session)
    };
    if let Some(session) = existing_session {
        // Re-login on a bound connection: refresh the recorded SDK info
        // (a reconnecting client may have been upgraded) and replay.
        sessions
            .borrow_mut()
            .record_sdk_info(transport_client_id, sdk_info);
        // A lagging backup's commit_max can sit below the epoch this session
        // already bound; never advertise a commit behind the session itself.
        let commit = current_metadata_commit(shard).max(session);
        let reply =
            build_login_register_reply(request_header, vsr_client_id, session, commit, user_id);
        let _ = shard
            .bus
            .send_to_client(transport_client_id, reply.into_generic().into_frozen())
            .await;
        return Ok(());
    }

    // Submit Register and await the commit. The SessionManager is left
    // untouched until the op commits cluster-wide (post-quorum): there is no
    // optimistic Authenticated transition, so a transient submit failure
    // needs no rollback -- the connection stays Connected and the SDK
    // read-timeout replays.
    let session = match submit_register_on_owner(shard, vsr_client_id, user_id).await {
        // The wire reply carries only the fence epoch; the SDK numbers its
        // own requests, so the bind watermark is not surfaced (see the
        // BoundSession doc for who does consume it).
        Ok(bound) => bound.epoch,
        Err(error) => {
            return Err(LoginRegisterError::Transient(error));
        }
    };

    // Post-commit: Connected -> Authenticated -> Bound in a single borrow with
    // no await in between, so the intermediate Authenticated state is never
    // observable to a concurrent request on this connection.
    {
        let mut sessions = sessions.borrow_mut();
        sessions
            .login(transport_client_id, user_id)
            .map_err(LoginRegisterError::Session)?;
        sessions.record_sdk_info(transport_client_id, sdk_info);
        if let Err(error) = sessions.bind_session(transport_client_id, vsr_client_id, session) {
            // No local rollback: `submit_register_in_process` above has
            // already committed cluster-wide. A local-only
            // `remove_client_session` here would diverge peers (they retain
            // the slot until they evict the client themselves). The
            // transport-disconnect callback owns local cleanup once the
            // socket closes.
            return Err(LoginRegisterError::Session(error));
        }
    }

    // `session` IS the register's commit op, and on a backup that forwarded
    // the proposal the local applied commit still lags it. Reporting the
    // lower number would make one frame contradict itself.
    let commit = current_metadata_commit(shard).max(session);
    let reply = build_login_register_reply(request_header, vsr_client_id, session, commit, user_id);
    let send_result = shard
        .bus
        .send_to_client(transport_client_id, reply.into_generic().into_frozen())
        .await;
    if let Err(error) = send_result {
        warn!(
            transport_client_id,
            error = %error,
            "failed to send login/register reply"
        );
    }

    Ok(())
}

/// Decide whether a failed login/register gets a terminal eviction or a
/// transient replay hint.
///
/// A transient consensus failure ([`LoginRegisterError::is_terminal`] is
/// `false`) means the cluster could not commit *right now* (a freshly booted
/// primary still catching up, or a cross-shard submit canceled). Those get a
/// result-framed replay hint instead of silence, so the SDK replays at once
/// rather than waiting out its read-timeout; replying empty would surface as
/// a hard `InvalidFormat` decode failure and break the replay.
///
/// Terminal auth errors (`InvalidCredentials` / `InvalidToken` /
/// `UserInactive` / `Session`) fast-fail with a typed `Eviction` frame so the
/// SDK surfaces the real reason (every frame transport decodes
/// `Command2::Eviction`) instead of a decode error or a timeout.
#[allow(clippy::future_not_send)]
pub(crate) async fn surface_login_failure<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    transport_client_id: u128,
    request_header: &RoutedRequestHeader,
    error: &LoginRegisterError,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    if error.is_terminal() {
        send_login_eviction(
            shard,
            transport_client_id,
            request_header.client,
            eviction_reason_for(error),
        )
        .await;
    } else {
        // Which code the hint carries is what tells the client whether the
        // replay may move to another node: see `transient_login_code`.
        send_login_transient_reply(
            shard,
            transport_client_id,
            request_header,
            transient_login_code(error),
        )
        .await;
    }
}

/// Result-framed transient Reply on a non-terminal failed Register. The SDK
/// decodes the nonzero result code and replays the same login on the same
/// connection. Only call for transient errors -- see
/// [`surface_login_failure`].
#[allow(clippy::future_not_send)]
async fn send_login_transient_reply<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    transport_client_id: u128,
    request_header: &RoutedRequestHeader,
    code: IggyError,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let commit = current_metadata_commit(shard);
    let reply = build_result_rejection_reply(request_header, commit, code.as_code());
    if let Err(error) = shard
        .bus
        .send_to_client(transport_client_id, reply.into_generic().into_frozen())
        .await
    {
        warn!(
            transport_client_id,
            error = %error,
            "failed to send login transient reply"
        );
    }
}

/// Wire code for a transient (non-terminal) login/register failure.
///
/// `TransientNotAccepted` asserts nothing was committed: the register never
/// entered a pipeline (not primary / not caught up / pipeline full) or never
/// left this node (primary unreachable). The client may re-issue it anywhere,
/// including under a fresh identity after failing over to another node.
///
/// A forward timeout is the one transient whose outcome is UNKNOWN, so it
/// cannot ride that assertion. The primary absorbs a register it cannot serve
/// yet with no deadline of its own, so a slow but healthy primary still
/// commits it after this node has stopped waiting. `TransientNotCommitted`
/// pins the replay to this connection and its client id, where a register
/// that did commit rebinds its own client-table entry; re-issuing under a
/// freshly minted id would instead orphan that entry until capacity eviction
/// reclaims it.
const fn transient_login_code(error: &LoginRegisterError) -> IggyError {
    match error {
        LoginRegisterError::Transient(MetadataSubmitError::ForwardTimedOut) => {
            IggyError::TransientNotCommitted
        }
        _ => IggyError::TransientNotAccepted,
    }
}

/// Wire reason for a terminal login/register failure. Session-level
/// rejections (including the non-retryable submit refusal, where the
/// presented client id belongs to another user) collapse to
/// `SessionError`; the SDK maps it to `Unauthenticated`.
const fn eviction_reason_for(error: &LoginRegisterError) -> EvictionReason {
    match error {
        LoginRegisterError::InvalidCredentials => EvictionReason::InvalidCredentials,
        LoginRegisterError::InvalidToken => EvictionReason::InvalidToken,
        LoginRegisterError::UserInactive => EvictionReason::UserInactive,
        _ => EvictionReason::SessionError,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_common::{IggyError, eviction_reason_to_error};

    /// The reasons this module emits must map back to the credential
    /// errors the SDK is expected to surface (the shared
    /// `eviction_reason_to_error` grading both ends).
    #[test]
    fn terminal_login_errors_map_to_typed_sdk_errors() {
        let cases = [
            (
                eviction_reason_for(&LoginRegisterError::InvalidCredentials),
                IggyError::InvalidCredentials,
            ),
            (
                eviction_reason_for(&LoginRegisterError::InvalidToken),
                IggyError::InvalidPersonalAccessToken,
            ),
            (
                eviction_reason_for(&LoginRegisterError::UserInactive),
                IggyError::Unauthenticated,
            ),
        ];
        for (reason, expected) in cases {
            let error = eviction_reason_to_error(reason, 0, 0);
            assert_eq!(
                error.as_code(),
                expected.as_code(),
                "reason {reason:?} must surface as {expected:?}"
            );
        }
    }
}
