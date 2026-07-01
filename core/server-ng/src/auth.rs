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
//! `Register` proposal on the metadata owner; also builds the terminal
//! login-failure replies.

use crate::bootstrap::ServerNgShard;
use crate::dispatch::submit_register_on_owner;
use crate::login_register::LoginRegisterError;
use crate::responses::{build_empty_reply, build_login_register_reply, current_metadata_commit};
use crate::session_manager::{ClientSdkInfo, SessionManager};
use consensus::MetadataHandle;
use iggy_binary_protocol::{ClientVersionInfo, RequestHeader};
use iggy_common::defaults::{
    MAX_PASSWORD_LENGTH, MAX_USERNAME_LENGTH, MIN_PASSWORD_LENGTH, MIN_USERNAME_LENGTH,
};
use iggy_common::{IggyTimestamp, PersonalAccessToken, UserStatus};
use message_bus::MessageBus;
use metadata::impls::metadata::StreamsFrontend;
use server::streaming::utils::crypto;
use std::cell::RefCell;
use std::rc::Rc;
use tracing::warn;

pub(crate) fn verify_login_credentials(
    shard: &Rc<ServerNgShard>,
    username: &str,
    password: &str,
) -> Result<u32, LoginRegisterError> {
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
        let Some(user_id) = users.index.get(username).copied() else {
            return Err(LoginRegisterError::InvalidCredentials);
        };
        let Some(user) = users.items.get(user_id as usize) else {
            return Err(LoginRegisterError::InvalidCredentials);
        };
        if user.status != UserStatus::Active {
            return Err(LoginRegisterError::UserInactive);
        }
        if !crypto::verify_password(password, user.password_hash.as_ref()) {
            return Err(LoginRegisterError::InvalidCredentials);
        }
        Ok(user.id)
    })
}

pub(crate) fn verify_pat_credentials(
    shard: &Rc<ServerNgShard>,
    token: &str,
) -> Result<u32, LoginRegisterError> {
    let token_hash = PersonalAccessToken::hash_token(token);
    let now = IggyTimestamp::now();
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
        Ok(user.id)
    })
}

#[allow(clippy::future_not_send)]
pub(crate) async fn complete_login_register(
    shard: &Rc<ServerNgShard>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
    vsr_client_id: u128,
    request_header: &RequestHeader,
    user_id: u32,
    client_version: &ClientVersionInfo,
) -> Result<(), LoginRegisterError> {
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
        let commit = current_metadata_commit(shard);
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
    let session = match submit_register_on_owner(shard, vsr_client_id).await {
        Ok(session) => session,
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

    let commit = current_metadata_commit(shard);
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

/// Decide whether a failed login/register gets a terminal reply or silence.
///
/// A transient consensus failure ([`LoginRegisterError::is_terminal`] is
/// `false`) means the cluster could not commit *right now* (a freshly booted
/// primary still catching up, or a cross-shard submit canceled). Staying
/// silent lets the SDK read-timeout replay once the primary is caught up;
/// replying empty would surface as a hard `InvalidFormat` decode failure and
/// break the replay.
///
/// Terminal auth errors (`InvalidCredentials` / `InvalidToken` /
/// `UserInactive` / `Session`) fast-fail with an empty reply rather than make
/// the client wait for a timeout. (TODO: ship a typed `Eviction` frame once
/// the SDK eviction decoder lands on every transport.)
#[allow(clippy::future_not_send)]
pub(crate) async fn surface_login_failure(
    shard: &Rc<ServerNgShard>,
    transport_client_id: u128,
    request_header: &RequestHeader,
    error: &LoginRegisterError,
) {
    if error.is_terminal() {
        send_login_failure_reply(shard, transport_client_id, request_header).await;
    }
}

/// Empty Reply on a terminal failed Register. The SDK only decodes
/// `Command2::Reply`; an empty body fails `LoginRegisterResponse` decoding
/// fast instead of hanging until the socket read timeout. Only call for
/// terminal errors -- see [`surface_login_failure`].
#[allow(clippy::future_not_send)]
pub(crate) async fn send_login_failure_reply(
    shard: &Rc<ServerNgShard>,
    transport_client_id: u128,
    request_header: &RequestHeader,
) {
    let commit = current_metadata_commit(shard);
    let reply = build_empty_reply(request_header, transport_client_id, 0, commit);
    if let Err(error) = shard
        .bus
        .send_to_client(transport_client_id, reply.into_generic().into_frozen())
        .await
    {
        warn!(
            transport_client_id,
            error = %error,
            "failed to send login-failure reply"
        );
    }
}
