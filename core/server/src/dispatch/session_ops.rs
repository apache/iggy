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

//! Session lifecycle: login/register, logout, evictions, and their
//! replication plumbing.
//!
//! Credentials (password + PAT) verify locally against replicated state on
//! whichever node the client dialed; only the consensus `Register` / `Logout`
//! proposal runs on the metadata owner, forwarded at most one hop to the
//! current primary (`submit_*_local_or_forward`, shard 0 only) with both ends
//! of that replica protocol on this page. Terminal failures surface as typed
//! `Eviction` frames, transient ones as result-framed replay hints.
//!
//! Deliberate asymmetry (the two session stores): the per-shard
//! `SessionManager` owns transport sessions (connection -> user binding,
//! heartbeats, SDK info) and is never replicated; the consensus `ClientTable`
//! owns replicated VSR sessions and their dedup watermarks. Login binds the
//! two together, so logout and eviction must release BOTH -- every teardown
//! path below pairs `remove_connection` with a replicated `Logout`.

use crate::dispatch::login_error::LoginRegisterError;
use crate::responses::{
    build_deny_reply, build_empty_reply, build_login_register_reply, current_metadata_commit,
};
use crate::session_manager::{ClientSdkInfo, SessionManager};
use crate::shell::{ShellBus, ShellShard};
use crate::wire::request_body;
use consensus::{
    Consensus, DISCONNECT_LOGOUT_REQUEST_ID, EvictionContext, MetadataHandle,
    build_eviction_message, build_incompatible_protocol_eviction_message,
    build_result_rejection_reply,
};
use iggy_binary_protocol::PrepareHeader;
use iggy_binary_protocol::requests::users::{LoginRegisterRequest, LoginRegisterWithPatRequest};
use iggy_binary_protocol::{
    ClientVersionInfo, Command, ConsensusHeader, EvictionReason, ForwardLogoutHeader,
    ForwardLogoutOutcome, ForwardLogoutResultHeader, ForwardRegisterHeader, ForwardRegisterOutcome,
    ForwardRegisterResultHeader, HEADER_SIZE, ProtocolVersion, RoutedRequestHeader, WireDecode,
    is_protocol_compatible,
};
use iggy_common::defaults::{
    MAX_PASSWORD_LENGTH, MAX_USERNAME_LENGTH, MIN_PASSWORD_LENGTH, MIN_USERNAME_LENGTH,
};
use iggy_common::{IggyError, IggyTimestamp, PersonalAccessToken, UserStatus};
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use metadata::MetadataSubmitError;
use metadata::impls::metadata::{BoundSession, StreamsFrontend};
use secrecy::ExposeSecret;
use server_common::Message;
use server_common::crypto;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::LazyLock;
use std::time::Duration;
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
pub fn warm_dummy_password_hash() {
    LazyLock::force(&DUMMY_PASSWORD_HASH);
}

pub fn verify_login_credentials<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    username: &str,
    password: &str,
) -> Result<u32, LoginRegisterError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
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

pub fn verify_pat_credentials<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    token: &str,
) -> Result<u32, LoginRegisterError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    verify_pat_credentials_with_expiry(shard, token).map(|(user_id, _)| user_id)
}

/// Like [`verify_pat_credentials`] but also surfaces the token's expiry (unix
/// seconds, `u64::MAX` when the PAT never expires). The HTTP extractor keys a
/// per-token VSR session table on this expiry for lazy eviction; the wire and
/// login paths only need the user id and go through [`verify_pat_credentials`].
pub fn verify_pat_credentials_with_expiry<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    token: &str,
) -> Result<(u32, u64), LoginRegisterError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
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
async fn complete_login_register<B, MJ, S, SB>(
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
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
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
/// `Command::Eviction`) instead of a decode error or a timeout.
#[allow(clippy::future_not_send)]
async fn surface_login_failure<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    transport_client_id: u128,
    request_header: &RoutedRequestHeader,
    error: &LoginRegisterError,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
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
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
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
/// A forward timeout, an in-progress proposal, or a canceled proposal has an
/// UNKNOWN outcome, so none can ride that assertion. `TransientNotCommitted`
/// pins the replay to this connection and its client id, where a register that
/// did commit rebinds its own client-table entry. Re-issuing under a freshly
/// minted id would instead orphan that entry until capacity eviction reclaims
/// it.
const fn transient_login_code(error: &LoginRegisterError) -> IggyError {
    match error {
        LoginRegisterError::Transient(
            MetadataSubmitError::ForwardTimedOut
            | MetadataSubmitError::InProgress
            | MetadataSubmitError::Canceled,
        ) => IggyError::TransientNotCommitted,
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

/// Reject a replicated request from an unbound transport with a typed
/// `Eviction(NoSession)` frame: the session the client believes it has is
/// gone, so it must register again. Pre-auth non-replicated reads get a
/// deny Reply instead (no session exists, so nothing is evicted).
///
/// The SDK's reply decoder maps eviction reasons to typed errors
/// (`NoSession` -> `Unauthenticated`), so clients fail fast with the same
/// error the legacy server returns instead of a body-decode failure. The
/// eviction context is best-effort off the metadata consensus (peer shards
/// have none; zeroes are cosmetic -- the SDK only reads the reason).
#[allow(clippy::future_not_send)]
pub(in crate::dispatch) async fn send_unauthenticated_eviction<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    transport_client_id: u128,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let ctx = shard.plane.metadata().consensus.as_ref().map_or(
        consensus::EvictionContext {
            cluster: 0,
            view: 0,
            replica: 0,
        },
        consensus::EvictionContext::from_consensus,
    );
    let eviction = consensus::build_eviction_message(
        ctx,
        transport_client_id,
        iggy_binary_protocol::EvictionReason::NoSession,
    );
    if let Err(error) = shard
        .bus
        .send_to_client(transport_client_id, eviction.into_generic().into_frozen())
        .await
    {
        warn!(
            transport_client_id,
            error = %error,
            "failed to send unauthenticated eviction"
        );
    }
}

/// Per-shard heartbeat verifier: evict connections that have not pinged within
/// `1.2 x interval`. Mirrors the legacy `verify_heartbeats` periodic task.
/// Eviction reuses the disconnect path (drops the client from its consumer
/// groups + rebalances via the replicated `Logout`) and sends a session-
/// terminal `Eviction(StaleClient)` so the client fails fast and can reconnect.
#[allow(clippy::future_not_send)]
pub async fn run_heartbeat_verifier<B, MJ, S, SB>(
    shard: Rc<ShellShard<B, MJ, S, SB>>,
    sessions: Rc<RefCell<SessionManager>>,
    interval: std::time::Duration,
    stop_rx: shard::Receiver<()>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    // Legacy `MAX_THRESHOLD`: a client is stale once it misses 1.2 intervals.
    // Integer 6/5 rather than `mul_f64`, which panics on an absurd interval.
    let max_age = interval.saturating_mul(6) / 5;
    loop {
        // `Ok(_)`: stop signalled -> exit. `Err(_)`: interval elapsed -> pass.
        // Waiting on the stop channel rather than sleeping past it keeps this
        // task inside the shutdown drain budget, which is shorter than the
        // heartbeat interval.
        let stop_signal = compio::time::timeout(interval, stop_rx.recv()).await;
        if stop_signal.is_ok() {
            break;
        }
        // Production-only wall clock: the heartbeat verifier is spawned solely
        // by `build_shard_for_thread`, never by the simulator's
        // `wire_shell_handlers`, so neither the interval wait above nor this
        // read is on a deterministic path. Driving this task under the
        // deterministic executor means routing both through the injected clock.
        let stale = sessions
            .borrow()
            .collect_stale(max_age, std::time::Instant::now());
        for transport_client_id in stale {
            // The heartbeat verifier exists to release a dead client's
            // consumer-group membership (so the group rebalances off it). A
            // connection that holds no membership has nothing for the eviction
            // to clean up; reaping it would only drop a still-usable session
            // (e.g. an idle admin connection that polls between long gaps),
            // which the legacy server tolerates. The real transport-disconnect
            // path still reaps it on socket close. So only evict a stale
            // connection that is actually a group member.
            let is_group_member = sessions
                .borrow()
                .bound_client_id(transport_client_id)
                .is_some_and(|vsr_client_id| {
                    !shard
                        .plane
                        .metadata()
                        .mux_stm
                        .streams()
                        .consumer_group_memberships(vsr_client_id)
                        .is_empty()
                });
            if is_group_member {
                evict_stale_client(&shard, &sessions, transport_client_id).await;
            }
        }
    }
}

/// Evict one stale connection: drop its session (releasing consumer-group
/// membership through a replicated `Logout`) and notify the client with a
/// session-terminal `Eviction(StaleClient)`.
#[allow(clippy::future_not_send)]
async fn evict_stale_client<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let bound = sessions.borrow_mut().remove_connection(transport_client_id);
    if let Some((vsr_client_id, session)) = bound {
        submit_disconnect_logout(Rc::clone(shard), vsr_client_id, session);
    }
    let ctx = shard.plane.metadata().consensus.as_ref().map_or(
        consensus::EvictionContext {
            cluster: 0,
            view: 0,
            replica: 0,
        },
        consensus::EvictionContext::from_consensus,
    );
    let eviction = consensus::build_eviction_message(
        ctx,
        transport_client_id,
        iggy_binary_protocol::EvictionReason::StaleClient,
    );
    if let Err(error) = shard
        .bus
        .send_to_client(transport_client_id, eviction.into_generic().into_frozen())
        .await
    {
        warn!(
            transport_client_id,
            error = %error,
            "failed to send stale-client eviction"
        );
    } else {
        warn!(
            transport_client_id,
            "evicted stale client (missed heartbeat)"
        );
    }
}

/// Answer a backup's forwarded `Register` from the node it named primary.
///
/// Proposes in process, never through [`submit_register_local_or_forward`]:
/// that is what bounds a forward at one hop. A node that has since lost
/// primaryship answers `NotPrimary`, and the origin's client replays against
/// whichever node it names next.
#[allow(clippy::future_not_send)]
pub(in crate::dispatch) async fn answer_forwarded_register<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    vsr_client_id: u128,
    user_id: u32,
    nonce: u128,
    origin_replica: u8,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let Some((cluster, view, replica)) = shard
        .plane
        .metadata()
        .consensus
        .as_ref()
        .map(|consensus| (consensus.cluster(), consensus.view(), consensus.replica()))
    else {
        warn!("ForwardedRegister submit reached a shard without metadata consensus");
        return;
    };
    let bound = shard
        .plane
        .metadata()
        .submit_register_in_process(vsr_client_id, user_id)
        .await;
    // `view` predates the await above, which parks with no deadline, so the
    // sealed value can be stale by send time. The origin routes the result by
    // `(nonce, client)` alone; this field must never become a freshness fence.
    let result =
        build_forward_register_result_message(cluster, view, replica, vsr_client_id, nonce, &bound);
    if let Err(error) = shard
        .bus
        .send_to_replica(origin_replica, result.into_generic().into_frozen())
        .await
    {
        warn!(
            origin_replica,
            error = %error,
            "failed to answer a forwarded register"
        );
    }
}

/// How long a login waits for the primary's verdict on a forwarded register.
///
/// Expiry does NOT prove the peer or the frame was lost. The primary answers
/// only once the proposal resolves, and its own submit parks with no deadline:
/// a primary that is not caught up, or whose pipeline is full, absorbs the
/// register into its request queue and answers when that drains. So a slow but
/// healthy primary commits the register after this node has stopped waiting,
/// which is why expiry surfaces as `TransientNotCommitted` rather than the
/// not-accepted flavor.
///
/// The budget stays well under the SDK's response-read timeout on purpose: the
/// client only replays a login while it is still reading, so a longer wait
/// here turns a transient into a torn-down socket.
const FORWARD_SUBMIT_TIMEOUT: Duration = Duration::from_secs(5);

/// Run the `Register` proposal for a login this node has already
/// authenticated, wherever the metadata primary currently is. Shard 0 only.
///
/// A client may dial any node in the cluster. Credentials verify against the
/// replicated users table, which every node holds, so the whole login except
/// the consensus proposal already works on a backup. Only the verified
/// identity crosses the replica interconnect -- never the client's frame and
/// never its credentials -- and the session bind, the reply, and the
/// connection all stay on the node the client dialed.
///
/// The hop does not move any credential decision:
/// - `verify_login_credentials` reads the backup's applied replicated user
///   state.
/// - `verify_pat_credentials` reads the same state, so a PAT minted on the
///   primary that has not replicated here yet is refused until it does.
///   Fail-closed on purpose, the same parity the HTTP forward keeps: it too
///   answers 401 until replication catches up rather than relaying an
///   unverified bearer.
/// - `ClientIdOwnedByAnotherUser` stays a decision of the caught-up primary
///   and round-trips as a terminal refusal.
///
/// Verification is point-in-time on the backup. A password change, PAT
/// revocation, or user deactivation committed on the primary but not yet
/// applied on the backup can therefore admit a login during the backup's apply
/// lag. The forward cannot complete while the backup is partitioned from the
/// primary, which bounds this to a connected replica's replication lag. This
/// is the same stale-read window as the existing HTTP forward.
///
/// The session binds here before this node applies the commit locally. That
/// is the window a primary-side login already has against every other node's
/// apply lag, not a new one.
#[allow(clippy::future_not_send)]
pub(in crate::dispatch) async fn submit_register_local_or_forward<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    vsr_client_id: u128,
    user_id: u32,
) -> Result<BoundSession, MetadataSubmitError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let Some(consensus) = shard.plane.metadata().consensus.as_ref() else {
        return Err(MetadataSubmitError::NotPrimary);
    };
    let (cluster, view, self_replica) =
        (consensus.cluster(), consensus.view(), consensus.replica());
    let target = consensus.primary_index(view);
    // Forward only as a healthy backup. Everything else answers locally: the
    // in-process submit proposes when this node is the serving primary and
    // re-derives `NotPrimary` otherwise -- mid view change there is nobody to
    // forward to (the node the view names has not finished taking over, the
    // SDK replays once it settles), and the view's own primary under state
    // transfer has nowhere to forward to and nothing to commit yet.
    if target == self_replica || !consensus.is_normal() {
        return shard
            .plane
            .metadata()
            .submit_register_in_process(vsr_client_id, user_id)
            .await;
    }

    let nonce = shard.next_forward_nonce(self_replica);
    let (reply, outcome) = shard::channel::<ForwardRegisterResultHeader>(1);
    shard.park_register_forward(nonce, vsr_client_id, reply);
    let forward =
        build_forward_register_message(cluster, view, self_replica, vsr_client_id, nonce, user_id);
    if let Err(error) = shard
        .bus
        .send_to_replica(target, forward.into_generic().into_frozen())
        .await
    {
        shard.cancel_register_forward(nonce, vsr_client_id);
        warn!(
            target,
            error = %error,
            "failed to forward register to the metadata primary"
        );
        return Err(MetadataSubmitError::PrimaryUnreachable);
    }

    match shard::bus_timeout(&shard.bus, FORWARD_SUBMIT_TIMEOUT, outcome.recv()).await {
        Some(Ok(result)) => forward_register_result(&result),
        // Shard-0 teardown dropped the sender without answering.
        Some(Err(_)) => Err(MetadataSubmitError::Canceled),
        None => {
            shard.cancel_register_forward(nonce, vsr_client_id);
            warn!(target, "forwarded register timed out");
            Err(MetadataSubmitError::ForwardTimedOut)
        }
    }
}

/// The primary's verdict, back in the vocabulary the login path speaks.
const fn forward_register_result(
    result: &ForwardRegisterResultHeader,
) -> Result<BoundSession, MetadataSubmitError> {
    match result.outcome {
        ForwardRegisterOutcome::Ok => Ok(BoundSession {
            epoch: result.epoch,
            watermark: result.watermark,
        }),
        ForwardRegisterOutcome::NotPrimary => Err(MetadataSubmitError::NotPrimary),
        ForwardRegisterOutcome::NotCaughtUp => Err(MetadataSubmitError::NotCaughtUp),
        ForwardRegisterOutcome::PipelineFull => Err(MetadataSubmitError::PipelineFull),
        ForwardRegisterOutcome::InProgress => Err(MetadataSubmitError::InProgress),
        ForwardRegisterOutcome::Canceled => Err(MetadataSubmitError::Canceled),
        ForwardRegisterOutcome::ClientIdOwnedByAnotherUser => {
            Err(MetadataSubmitError::ClientIdOwnedByAnotherUser)
        }
    }
}

/// Inverse of [`forward_register_result`], for the answering primary.
const fn forward_register_outcome(
    bound: &Result<BoundSession, MetadataSubmitError>,
) -> (BoundSession, ForwardRegisterOutcome) {
    let zero = BoundSession {
        epoch: 0,
        watermark: 0,
    };
    match bound {
        Ok(bound) => (*bound, ForwardRegisterOutcome::Ok),
        Err(MetadataSubmitError::NotPrimary) => (zero, ForwardRegisterOutcome::NotPrimary),
        Err(MetadataSubmitError::NotCaughtUp) => (zero, ForwardRegisterOutcome::NotCaughtUp),
        Err(MetadataSubmitError::PipelineFull) => (zero, ForwardRegisterOutcome::PipelineFull),
        Err(MetadataSubmitError::InProgress) => (zero, ForwardRegisterOutcome::InProgress),
        Err(MetadataSubmitError::ClientIdOwnedByAnotherUser) => {
            (zero, ForwardRegisterOutcome::ClientIdOwnedByAnotherUser)
        }
        // `MetadataSubmitError` is `#[non_exhaustive]`. Every variant but the
        // ownership refusal is transient by contract, and `Canceled` is the
        // transient answer that claims nothing beyond "retry".
        Err(_) => (zero, ForwardRegisterOutcome::Canceled),
    }
}

#[allow(clippy::cast_possible_truncation)]
fn build_forward_register_message(
    cluster: u128,
    view: u32,
    replica: u8,
    client: u128,
    nonce: u128,
    user_id: u32,
) -> Message<ForwardRegisterHeader> {
    Message::<ForwardRegisterHeader>::new(HEADER_SIZE).transmute_header(
        |_, header: &mut ForwardRegisterHeader| {
            header.command = Command::ForwardRegister;
            header.cluster = cluster;
            header.view = view;
            header.replica = replica;
            header.client = client;
            header.nonce = nonce;
            header.user_id = user_id;
            header.size = HEADER_SIZE as u32;
            header.seal();
        },
    )
}

#[allow(clippy::cast_possible_truncation)]
fn build_forward_register_result_message(
    cluster: u128,
    view: u32,
    replica: u8,
    client: u128,
    nonce: u128,
    bound: &Result<BoundSession, MetadataSubmitError>,
) -> Message<ForwardRegisterResultHeader> {
    let (session, outcome) = forward_register_outcome(bound);
    Message::<ForwardRegisterResultHeader>::new(HEADER_SIZE).transmute_header(
        |_, header: &mut ForwardRegisterResultHeader| {
            header.command = Command::ForwardRegisterResult;
            header.cluster = cluster;
            header.view = view;
            header.replica = replica;
            header.client = client;
            header.nonce = nonce;
            header.epoch = session.epoch;
            header.watermark = session.watermark;
            header.outcome = outcome;
            header.size = HEADER_SIZE as u32;
            header.seal();
        },
    )
}

/// Answer a backup's forwarded Logout from the node it named primary.
#[allow(clippy::future_not_send)]
pub(in crate::dispatch) async fn answer_forwarded_logout<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    vsr_client_id: u128,
    session: u64,
    request: u64,
    nonce: u128,
    origin_replica: u8,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let Some((cluster, view, replica)) = shard
        .plane
        .metadata()
        .consensus
        .as_ref()
        .map(|consensus| (consensus.cluster(), consensus.view(), consensus.replica()))
    else {
        warn!("ForwardedLogout submit reached a shard without metadata consensus");
        return;
    };
    let outcome = shard
        .plane
        .metadata()
        .submit_logout_in_process(vsr_client_id, session, request)
        .await;
    let result =
        build_forward_logout_result_message(cluster, view, replica, vsr_client_id, nonce, &outcome);
    if let Err(error) = shard
        .bus
        .send_to_replica(origin_replica, result.into_generic().into_frozen())
        .await
    {
        warn!(
            origin_replica,
            error = %error,
            "failed to answer a forwarded logout"
        );
    }
}

/// Commit a Logout locally when this node is primary, otherwise forward it
/// once to the primary named by the current normal view.
#[allow(clippy::future_not_send)]
pub(in crate::dispatch) async fn submit_logout_local_or_forward<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    vsr_client_id: u128,
    session: u64,
    request: u64,
) -> Result<u64, MetadataSubmitError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let Some(consensus) = shard.plane.metadata().consensus.as_ref() else {
        return Err(MetadataSubmitError::NotPrimary);
    };
    let (cluster, view, self_replica) =
        (consensus.cluster(), consensus.view(), consensus.replica());
    let target = consensus.primary_index(view);
    if target == self_replica || !consensus.is_normal() {
        return shard
            .plane
            .metadata()
            .submit_logout_in_process(vsr_client_id, session, request)
            .await;
    }

    let nonce = shard.next_forward_nonce(self_replica);
    let (reply, outcome) = shard::channel::<ForwardLogoutResultHeader>(1);
    shard.park_logout_forward(nonce, vsr_client_id, reply);
    let forward = build_forward_logout_message(
        cluster,
        view,
        self_replica,
        vsr_client_id,
        nonce,
        session,
        request,
    );
    if let Err(error) = shard
        .bus
        .send_to_replica(target, forward.into_generic().into_frozen())
        .await
    {
        shard.cancel_logout_forward(nonce, vsr_client_id);
        warn!(
            target,
            error = %error,
            "failed to forward logout to the metadata primary"
        );
        return Err(MetadataSubmitError::PrimaryUnreachable);
    }

    match shard::bus_timeout(&shard.bus, FORWARD_SUBMIT_TIMEOUT, outcome.recv()).await {
        Some(Ok(result)) => forward_logout_result(&result),
        Some(Err(_)) => Err(MetadataSubmitError::Canceled),
        None => {
            shard.cancel_logout_forward(nonce, vsr_client_id);
            warn!(target, "forwarded logout timed out");
            Err(MetadataSubmitError::ForwardTimedOut)
        }
    }
}

const fn forward_logout_result(
    result: &ForwardLogoutResultHeader,
) -> Result<u64, MetadataSubmitError> {
    match result.outcome {
        ForwardLogoutOutcome::Ok => Ok(result.commit),
        ForwardLogoutOutcome::NotPrimary => Err(MetadataSubmitError::NotPrimary),
        ForwardLogoutOutcome::PipelineFull => Err(MetadataSubmitError::PipelineFull),
        ForwardLogoutOutcome::InProgress => Err(MetadataSubmitError::InProgress),
        ForwardLogoutOutcome::Canceled => Err(MetadataSubmitError::Canceled),
    }
}

const fn forward_logout_outcome(
    outcome: &Result<u64, MetadataSubmitError>,
) -> (u64, ForwardLogoutOutcome) {
    match outcome {
        Ok(commit) => (*commit, ForwardLogoutOutcome::Ok),
        Err(MetadataSubmitError::NotPrimary) => (0, ForwardLogoutOutcome::NotPrimary),
        Err(MetadataSubmitError::PipelineFull) => (0, ForwardLogoutOutcome::PipelineFull),
        Err(MetadataSubmitError::InProgress) => (0, ForwardLogoutOutcome::InProgress),
        Err(_) => (0, ForwardLogoutOutcome::Canceled),
    }
}

#[allow(clippy::cast_possible_truncation, clippy::too_many_arguments)]
fn build_forward_logout_message(
    cluster: u128,
    view: u32,
    replica: u8,
    client: u128,
    nonce: u128,
    session: u64,
    request: u64,
) -> Message<ForwardLogoutHeader> {
    Message::<ForwardLogoutHeader>::new(HEADER_SIZE).transmute_header(
        |_, header: &mut ForwardLogoutHeader| {
            header.command = Command::ForwardLogout;
            header.cluster = cluster;
            header.view = view;
            header.replica = replica;
            header.client = client;
            header.nonce = nonce;
            header.session = session;
            header.request = request;
            header.size = HEADER_SIZE as u32;
            header.seal();
        },
    )
}

#[allow(clippy::cast_possible_truncation)]
fn build_forward_logout_result_message(
    cluster: u128,
    view: u32,
    replica: u8,
    client: u128,
    nonce: u128,
    result: &Result<u64, MetadataSubmitError>,
) -> Message<ForwardLogoutResultHeader> {
    let (commit, outcome) = forward_logout_outcome(result);
    Message::<ForwardLogoutResultHeader>::new(HEADER_SIZE).transmute_header(
        |_, header: &mut ForwardLogoutResultHeader| {
            header.command = Command::ForwardLogoutResult;
            header.cluster = cluster;
            header.view = view;
            header.replica = replica;
            header.client = client;
            header.nonce = nonce;
            header.commit = commit;
            header.outcome = outcome;
            header.size = HEADER_SIZE as u32;
            header.seal();
        },
    )
}

/// Run the consensus `Register` proposal on the metadata owner (shard 0)
/// and return the committed session.
///
/// Credential verification and session binding stay on the calling (home)
/// shard -- only this consensus step must execute where the metadata
/// consensus group lives. On shard 0 it goes straight to
/// [`submit_register_local_or_forward`]; on a peer it forwards a
/// [`shard::MetadataSubmit`] to shard 0 and awaits the committed op. A dropped
/// reply (shard-0 inbox full / shutdown) maps to a transient `Canceled`, which
/// the caller wraps so the SDK replays.
#[allow(clippy::future_not_send)]
pub async fn submit_register_on_owner<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    vsr_client_id: u128,
    user_id: u32,
) -> Result<BoundSession, MetadataSubmitError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    if shard.id == 0 {
        return submit_register_local_or_forward(shard, vsr_client_id, user_id).await;
    }
    let (reply, rx) = shard::channel::<Result<BoundSession, MetadataSubmitError>>(1);
    shard.forward_metadata_submit(shard::MetadataSubmit::Register {
        vsr_client_id,
        user_id,
        reply,
    });
    // The owner's outcome, verbatim in both directions. `Canceled` is only for a
    // dropped channel, where nothing came back to classify.
    rx.recv()
        .await
        .unwrap_or(Err(MetadataSubmitError::Canceled))
}

/// Logout counterpart of [`submit_register_on_owner`].
#[allow(clippy::future_not_send)]
pub async fn submit_logout_on_owner<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    vsr_client_id: u128,
    session: u64,
    request: u64,
) -> Result<u64, MetadataSubmitError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    if shard.id == 0 {
        return submit_logout_local_or_forward(shard, vsr_client_id, session, request).await;
    }
    let (reply, rx) = shard::channel::<Result<u64, MetadataSubmitError>>(1);
    shard.forward_metadata_submit(shard::MetadataSubmit::Logout {
        vsr_client_id,
        session,
        request,
        reply,
    });
    rx.recv()
        .await
        .unwrap_or(Err(MetadataSubmitError::Canceled))
}

/// Release the client-table slot for a disconnected transport, cluster-wide.
///
/// The local `SessionManager` connection is already dropped by the caller;
/// this is what drops the replicated entry, so a peer replica does not keep an
/// orphaned session until it evicts one under capacity pressure.
///
/// Unconditional, and deliberately so. Holding the slot open for a grace
/// window would let a reconnecting client resume onto its entry with its
/// watermark and reply ring intact, but nothing in tree re-presents a
/// `client_id` after a disconnect (the Rust SDK mints a fresh one on
/// re-login), so the window buys nothing today and the slot it holds is not
/// free: the client table's eviction point moves from concurrent connections
/// to CUMULATIVE connects, and every capacity eviction silently erases a
/// dedup watermark.
///
/// A resume window becomes worth having once SDK-side identity stability
/// lands, at which point it needs a timer of its own -- riding the heartbeat
/// verifier would tie the grace period to heartbeat configuration, since
/// `collect_stale` keys off `heartbeat.interval` and the verifier does not run
/// at all when `heartbeat.enabled` is false.
/// Deliberately does NOT drop the local `ClientTable` slot first:
/// `submit_logout_*` short-circuits when the slot is already gone, so a
/// pre-emptive local removal would suppress the `Logout` and leave peer
/// replicas with an orphaned session until they evict it themselves -- the
/// exact divergence this avoids. `submit_logout_on_owner` runs in-process on
/// shard 0 and forwards for peer-homed connections; its session guard drops a
/// stale logout for a reused client id.
#[allow(clippy::future_not_send)]
pub(in crate::dispatch) fn submit_disconnect_logout<B, MJ, S, SB>(
    shard: Rc<ShellShard<B, MJ, S, SB>>,
    vsr_client_id: u128,
    session: u64,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    // The sentinel request id is what the apply path reads to keep, rather
    // than drop, the session's dedup fence: the client may be reconnecting
    // under the same key, and its retry must still be answered.
    let bus = shard.bus.clone();
    bus.spawn(async move {
        if let Err(error) =
            submit_logout_on_owner(&shard, vsr_client_id, session, DISCONNECT_LOGOUT_REQUEST_ID)
                .await
        {
            warn!(
                vsr_client_id,
                ?error,
                "disconnect logout submit failed; peer slots may linger until eviction"
            );
        }
    });
}

#[allow(clippy::future_not_send)]
pub(in crate::dispatch) async fn handle_logout_request<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
    request: Message<RoutedRequestHeader>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let Some((vsr_client_id, session)) = sessions.borrow().get_session(transport_client_id) else {
        // Logout on an unbound transport: the desired state already holds,
        // so answer ok. A silent drop would wedge the lockstep SDK on this
        // connection until its socket read timeout, and the SDK routinely
        // sends a logout before each re-login.
        warn!(
            transport_client_id,
            "logout for unbound VSR session; answering ok"
        );
        let commit = current_metadata_commit(shard);
        let reply = build_empty_reply(request.header(), transport_client_id, 0, commit);
        if let Err(error) = shard
            .bus
            .send_to_client(transport_client_id, reply.into_generic().into_frozen())
            .await
        {
            warn!(
                transport_client_id,
                error = %error,
                "failed to send unbound logout reply"
            );
        }
        return;
    };

    let request_id = request.header().request;
    let commit = match submit_logout_on_owner(shard, vsr_client_id, session, request_id).await {
        Ok(commit) => commit,
        Err(error) => {
            // Deny as transient instead of dropping the frame: the submit
            // usually fails because this replica is not the metadata owner
            // right now, and the SDK replays a transient rejection.
            warn!(transport_client_id, error = %error, "logout/unregister failed; denying transient");
            let commit = current_metadata_commit(shard);
            let reply = build_deny_reply(
                request.header(),
                vsr_client_id,
                session,
                commit,
                transient_logout_code(&error).as_code(),
            );
            if let Err(send_error) = shard
                .bus
                .send_to_client(transport_client_id, reply.into_generic().into_frozen())
                .await
            {
                warn!(
                    transport_client_id,
                    error = %send_error,
                    "failed to send logout deny reply"
                );
            }
            return;
        }
    };

    sessions.borrow_mut().remove_connection(transport_client_id);

    let reply = build_empty_reply(request.header(), vsr_client_id, session, commit);
    if let Err(error) = shard
        .bus
        .send_to_client(transport_client_id, reply.into_generic().into_frozen())
        .await
    {
        warn!(
            transport_client_id,
            error = %error,
            "failed to send logout reply"
        );
    }
}

/// Preserve the client identity when a Logout may already have entered the
/// primary's pipeline. Moving an unknown-outcome replay to another connection
/// could race a later Register and obscure whether the old epoch was removed.
const fn transient_logout_code(error: &MetadataSubmitError) -> IggyError {
    match error {
        MetadataSubmitError::ForwardTimedOut
        | MetadataSubmitError::InProgress
        | MetadataSubmitError::Canceled => IggyError::TransientNotCommitted,
        _ => IggyError::TransientNotAccepted,
    }
}

#[allow(clippy::future_not_send, clippy::too_many_lines)]
pub(in crate::dispatch) async fn handle_login_register_request<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
    request: Message<RoutedRequestHeader>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let body = request_body(&request);
    let vsr_client_id = request.header().client;

    // Both login-register shapes share the ClientVersionInfo prefix, so the
    // protocol gate decodes it once and runs before any credential work; the
    // body shapes below parse from past the prefix. Only VSR clients reach
    // this gate -- legacy SDKs use LOGIN_USER_CODE, a separate path. A
    // pre-versioning VSR client sends the old prefix-less body, which fails
    // ClientVersionInfo::decode (-> MalformedLogin) or the version gate
    // (-> IncompatibleProtocol) right here, not dropped earlier.
    let Ok((version_info, prefix_len)) = ClientVersionInfo::decode(body) else {
        warn!(
            transport_client_id,
            "rejecting login: body has no decodable version prefix"
        );
        send_login_eviction(
            shard,
            transport_client_id,
            vsr_client_id,
            EvictionReason::MalformedLogin,
        )
        .await;
        return;
    };
    if !is_protocol_compatible(version_info.protocol_version) {
        warn!(
            transport_client_id,
            client_protocol_version = %ProtocolVersion(version_info.protocol_version),
            sdk_name = %version_info.sdk_name,
            sdk_version = %version_info.sdk_version,
            "rejecting login: incompatible protocol version"
        );
        send_login_eviction(
            shard,
            transport_client_id,
            vsr_client_id,
            EvictionReason::IncompatibleProtocol,
        )
        .await;
        return;
    }

    let body_tail = &body[prefix_len..];
    let mut credentials_rejected = false;
    if let Ok((wire_request, _)) =
        LoginRegisterRequest::decode_after_prefix(version_info.clone(), body_tail)
    {
        match verify_login_credentials(
            shard,
            wire_request.username.as_str(),
            wire_request.password.expose_secret(),
        ) {
            Ok(user_id) => {
                if let Err(error) = complete_login_register(
                    shard,
                    sessions,
                    transport_client_id,
                    vsr_client_id,
                    request.header(),
                    user_id,
                    &wire_request.version_info,
                )
                .await
                {
                    warn!(transport_client_id, error = %error, "login/register failed");
                    surface_login_failure(shard, transport_client_id, request.header(), &error)
                        .await;
                }
                return;
            }
            Err(LoginRegisterError::InvalidCredentials) => {
                // Fall through to PAT attempt so a credential payload that
                // collides with a valid PAT payload shape still gets a
                // chance. A password-shaped body rarely parses as a PAT
                // body, so remember the rejection: the final fall-through
                // must surface InvalidCredentials, not MalformedLogin.
                credentials_rejected = true;
            }
            Err(error) => {
                warn!(transport_client_id, error = %error, "login/register failed");
                surface_login_failure(shard, transport_client_id, request.header(), &error).await;
                return;
            }
        }
    }

    if let Ok((wire_request, _)) =
        LoginRegisterWithPatRequest::decode_after_prefix(version_info, body_tail)
    {
        match verify_pat_credentials(shard, wire_request.token.expose_secret()) {
            Ok(user_id) => {
                if let Err(error) = complete_login_register(
                    shard,
                    sessions,
                    transport_client_id,
                    vsr_client_id,
                    request.header(),
                    user_id,
                    &wire_request.version_info,
                )
                .await
                {
                    warn!(
                        transport_client_id,
                        error = %error,
                        "login/register with PAT failed"
                    );
                    surface_login_failure(shard, transport_client_id, request.header(), &error)
                        .await;
                }
                return;
            }
            Err(error) => {
                warn!(
                    transport_client_id,
                    error = %error,
                    "login/register with PAT failed"
                );
                surface_login_failure(shard, transport_client_id, request.header(), &error).await;
                return;
            }
        }
    }

    if credentials_rejected {
        warn!(
            transport_client_id,
            "rejecting register request: invalid credentials"
        );
        send_login_eviction(
            shard,
            transport_client_id,
            request.header().client,
            EvictionReason::InvalidCredentials,
        )
        .await;
        return;
    }

    warn!(
        transport_client_id,
        "rejecting register request with unsupported payload shape"
    );
    send_login_eviction(
        shard,
        transport_client_id,
        request.header().client,
        EvictionReason::MalformedLogin,
    )
    .await;
}

/// Best-effort login-rejection eviction. Terminal one-way frame; a gone
/// connection has nothing to recover, so the send error is logged and
/// dropped. Consensus context (cluster/view/replica) is stamped on the
/// metadata shard and zeroed elsewhere -- the SDK only reads the reason,
/// plus the protocol window on `IncompatibleProtocol`.
#[allow(clippy::future_not_send)]
pub(in crate::dispatch) async fn send_login_eviction<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    transport_client_id: u128,
    vsr_client_id: u128,
    reason: EvictionReason,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let ctx = shard.plane.metadata().consensus.as_ref().map_or(
        EvictionContext {
            cluster: 0,
            view: 0,
            replica: 0,
        },
        EvictionContext::from_consensus,
    );
    let eviction = match reason {
        EvictionReason::IncompatibleProtocol => {
            build_incompatible_protocol_eviction_message(ctx, vsr_client_id)
        }
        _ => build_eviction_message(ctx, vsr_client_id, reason),
    };
    if let Err(error) = shard
        .bus
        .send_to_client(transport_client_id, eviction.into_generic().into_frozen())
        .await
    {
        warn!(
            transport_client_id,
            error = %error,
            reason = ?reason,
            "failed to send login eviction"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::test_support::{
        FIRST_BOOT, SECOND_BOOT, SpyBus, TestMux, TestShard, prepare_message, register_reply,
        request_message, test_shard,
    };
    use crate::session_manager::SessionError;
    use consensus::{LocalPipeline, Plane as _, PlaneKind, VsrConsensus};
    use iggy_binary_protocol::Operation;
    use iggy_binary_protocol::requests::streams::CreateStreamRequest;
    use iggy_binary_protocol::{PrepareOkHeader, ReplyHeader, WireEncode, WireOptions};
    use iggy_common::eviction_reason_to_error;
    use journal::prepare_journal::PrepareJournal;
    use message_bus::installer::conn_info::ClientTransportKind;
    use metadata::IggyMetadata;
    use metadata::impls::metadata::IggySnapshot;
    use partitions::{IggyPartitions, PartitionPathLayout, PartitionsConfig};
    use server_common::MessageBag;
    use server_common::sharding::ShardId;
    use shard::shards_table::PapayaShardsTable;
    use shard::{PartitionConsensusConfig, ReplicaTopology, ShardIdentity};
    use std::future::Future;
    use std::mem::size_of;

    #[test]
    fn terminal_vs_transient() {
        assert!(LoginRegisterError::InvalidCredentials.is_terminal());
        assert!(LoginRegisterError::InvalidToken.is_terminal());
        assert!(LoginRegisterError::UserInactive.is_terminal());
        assert!(LoginRegisterError::Session(SessionError::ConnectionNotFound(0)).is_terminal());
        // Transient is the only recoverable variant: never terminal.
        assert!(!LoginRegisterError::Transient(MetadataSubmitError::PipelineFull).is_terminal());
    }

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

    #[test]
    fn unknown_register_outcomes_pin_the_client_identity() {
        for error in [
            MetadataSubmitError::ForwardTimedOut,
            MetadataSubmitError::InProgress,
            MetadataSubmitError::Canceled,
        ] {
            assert_eq!(
                transient_login_code(&LoginRegisterError::Transient(error)),
                IggyError::TransientNotCommitted,
            );
        }
        for error in [
            MetadataSubmitError::NotPrimary,
            MetadataSubmitError::NotCaughtUp,
            MetadataSubmitError::PipelineFull,
            MetadataSubmitError::PrimaryUnreachable,
        ] {
            assert_eq!(
                transient_login_code(&LoginRegisterError::Transient(error)),
                IggyError::TransientNotAccepted,
            );
        }
    }

    /// Regression test for the production failure chain "CLI stream
    /// create succeeded, logout failed: Disconnected".
    ///
    /// Why the logout of a CLI invocation used to fail during ITS OWN
    /// successful `stream create`: the catch-up gate was GLOBAL. The suite
    /// runs many CLI invocations against one shared single-node server;
    /// each one is three replicated ops (Register, work, Logout). When
    /// THIS client's logout frame arrived, some SIBLING client's op was
    /// regularly sitting between quorum-ack (`commit_max` advanced inside
    /// `on_ack`) and apply (`commit_min` still behind, driver parked at
    /// the journal read). `submit_logout_in_process` then rejected
    /// `NotCaughtUp`, and `handle_logout_request` swallowed the error: no
    /// reply frame, session left bound. A one-shot CLI saw only a dead
    /// connection — "Problem with server logout / Disconnected" — and
    /// exited non-zero although its create committed; the harness retry
    /// then tripped "already exists".
    ///
    /// This test rebuilds that interleaving deterministically (client B =
    /// the sibling parked mid-commit; client A = the CLI logging out) and
    /// pins the contract that fixed it (non-register ops carry no
    /// catch-up gate, see `submit_logout_in_process`):
    ///
    ///   a client-initiated logout must always produce a reply frame and
    ///   unbind the transport session, even while a sibling's commit is
    ///   in flight — the logout simply pipelines behind it.
    #[compio::test]
    async fn logout_rejected_by_closed_gate_must_still_reply_to_client() {
        const CLIENT_A: u128 = 1;
        const CLIENT_B: u128 = 2;
        const SESSION: u64 = 1;
        const ACTING_USER: u32 = 7;
        const TRANSPORT_A: u128 = 77;

        let dir = tempfile::tempdir().unwrap();
        let journal = PrepareJournal::open(&dir.path().join("journal.wal"), 0)
            .await
            .unwrap();
        let bus = SpyBus::default();
        let consensus = VsrConsensus::new(
            1,
            0,
            1,
            server_common::sharding::METADATA_GROUP,
            bus.clone(),
            LocalPipeline::new(),
        );
        consensus.init();
        let metadata: IggyMetadata<_, PrepareJournal, IggySnapshot, TestMux> = IggyMetadata::new(
            Some(consensus),
            Some(journal),
            None,
            None,
            TestMux::default(),
            None,
        );
        let partitions = IggyPartitions::new(
            ShardId::new(0),
            PartitionsConfig {
                messages_required_to_save: 1,
                size_of_messages_required_to_save: iggy_common::IggyByteSize::from(1024_u64),
                enforce_fsync: false,
                validate_checksum: true,
                segment_size: iggy_common::IggyByteSize::from(1_048_576_u64),
                preallocate_segments: false,
                encryptor: None,
                path_layout: PartitionPathLayout::default(),
            },
        );
        let shard = Rc::new(TestShard::without_inbox(
            ShardIdentity::new(0, "logout-window-test".to_string()),
            bus.clone(),
            metadata,
            partitions,
            PapayaShardsTable::new(),
            PartitionConsensusConfig::new(1, ReplicaTopology::new(0, 1), bus.clone()),
        ));
        let md = shard.plane.metadata();
        let consensus = md.consensus.as_ref().unwrap();

        // A and B hold committed sessions (as after their CLI logins).
        for client in [CLIENT_A, CLIENT_B] {
            md.client_table.borrow_mut().commit_register(
                client,
                ACTING_USER,
                register_reply(client, SESSION),
            );
        }
        // A's transport connection, authenticated + bound — the state a
        // CLI connection is in right after its create-stream reply.
        let sessions = Rc::new(RefCell::new(SessionManager::new()));
        sessions.borrow_mut().ensure_connection(
            TRANSPORT_A,
            "127.0.0.1:34567".parse().unwrap(),
            ClientTransportKind::Tcp,
        );
        sessions
            .borrow_mut()
            .login(TRANSPORT_A, ACTING_USER)
            .unwrap();
        sessions
            .borrow_mut()
            .bind_session(TRANSPORT_A, CLIENT_A, SESSION)
            .unwrap();

        // Sibling B's op: prepared, journaled, self-acked through the real
        // replicate path. (The public submit API cannot be used to open
        // the window: `dispatch_prepare_and_await` pumps its own loopback
        // inline, committing before it returns. Production's window is a
        // sibling submit task parked INSIDE `on_ack`'s awaits — modeled
        // below by driving `on_ack` by hand.)
        let create_body = CreateStreamRequest {
            name: iggy_binary_protocol::primitives::identifier::WireName::new("s1").unwrap(),
            options: WireOptions::empty(),
        }
        .to_bytes();
        let prepare = prepare_message(Operation::CreateStream, CLIENT_B, 1, &create_body);
        consensus.pipeline_message(PlaneKind::Metadata, &prepare);
        md.on_replicate(prepare).await;
        let mut loopback = Vec::new();
        consensus.drain_loopback_into(&mut loopback);
        let ack = loopback
            .pop()
            .expect("one self-ack per replicated prepare")
            .try_into_typed::<PrepareOkHeader>()
            .expect("loopback holds self PrepareOks");

        // Open the window: first poll of `on_ack` advances commit_max at
        // quorum, then parks at the journal read — commit_min unchanged.
        // Every production NotCaughtUp logout was submitted exactly here.
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        let mut driver = Box::pin(md.on_ack(ack));
        assert!(
            driver.as_mut().poll(&mut cx).is_pending(),
            "driver must park mid-commit at the journal read"
        );
        assert_eq!(consensus.commit_max(), 1);
        assert_eq!(consensus.commit_min(), 0);

        // A's logout lands in the window, through the real dispatch path.
        let logout = request_message(Operation::Logout, CLIENT_A, SESSION, 2, &[]);
        handle_logout_request(&shard, &sessions, TRANSPORT_A, logout).await;

        // DESIRED CONTRACT (red on current code): the client must never be
        // left in silence — that silence is what a one-shot CLI reports as
        // "Problem with server logout / Disconnected".
        assert!(
            bus.client_replies
                .borrow()
                .iter()
                .any(|(client, _)| *client == TRANSPORT_A),
            "logout must produce a reply frame to the client even while the \
             catch-up gate is closed (silence = CLI 'Disconnected', exit 1)"
        );
        assert_eq!(
            sessions.borrow().get_session(TRANSPORT_A),
            None,
            "transport session must be unbound by a client-initiated logout; \
             the VSR slot may lapse to the eviction sweep"
        );
    }

    /// A backup's login: it forwards the register it authenticated to the
    /// view's primary and completes on the primary's verdict, with the whole
    /// round trip going through the real shard ingest arm.
    #[compio::test]
    async fn backup_forwards_register_and_completes_on_the_primary_verdict() {
        const CLIENT: u128 = 0xCAFE;
        const USER: u32 = 7;
        const EPOCH: u64 = 41;
        const WATERMARK: u64 = 9;

        let bus = SpyBus::default();
        // Replica 1 of 3, view 0: `primary_index(0)` is replica 0.
        let shard = Rc::new(test_shard(&bus, 1, 3, FIRST_BOOT));
        let login = {
            let shard = Rc::clone(&shard);
            compio::runtime::spawn(async move {
                submit_register_local_or_forward(&shard, CLIENT, USER).await
            })
        };
        await_forward(&bus).await;
        let (target, forward) = bus.sole_replica_send::<ForwardRegisterHeader>();
        assert_eq!(target, 0, "forward must address the view's primary");
        assert_eq!(forward.command, Command::ForwardRegister);
        assert_eq!(forward.client, CLIENT);
        assert_eq!(
            forward.user_id, USER,
            "the forwarded identity is the payload"
        );
        assert_eq!(forward.replica, 1, "the origin names itself for the answer");
        assert_ne!(forward.nonce, 0);
        assert_eq!(forward.verify_frame(), Ok(()), "the frame must be sealed");
        assert_eq!(forward.validate(), Ok(()));

        shard
            .on_message(forward_register_result(
                &forward,
                ForwardRegisterOutcome::Ok,
                EPOCH,
                WATERMARK,
            ))
            .await;
        assert_eq!(
            login.await.expect("the login task ran to completion"),
            Ok(BoundSession {
                epoch: EPOCH,
                watermark: WATERMARK,
            })
        );
    }

    #[compio::test]
    async fn backup_forwards_logout_and_completes_on_the_primary_verdict() {
        const CLIENT: u128 = 0xCAFE;
        const SESSION: u64 = 41;
        const REQUEST: u64 = 9;
        const COMMIT: u64 = 42;

        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, 1, 3, FIRST_BOOT));
        let logout = {
            let shard = Rc::clone(&shard);
            compio::runtime::spawn(async move {
                submit_logout_local_or_forward(&shard, CLIENT, SESSION, REQUEST).await
            })
        };
        await_forward(&bus).await;
        let (target, forward) = bus.sole_replica_send::<ForwardLogoutHeader>();
        assert_eq!(target, 0, "forward must address the view's primary");
        assert_eq!(forward.command, Command::ForwardLogout);
        assert_eq!(forward.client, CLIENT);
        assert_eq!(forward.session, SESSION);
        assert_eq!(forward.request, REQUEST);
        assert_eq!(forward.replica, 1);
        assert_ne!(forward.nonce, 0);
        assert_eq!(forward.verify_frame(), Ok(()));
        assert_eq!(forward.validate(), Ok(()));

        shard
            .on_message(forward_logout_result_message(&forward, &Ok(COMMIT)))
            .await;
        assert_eq!(
            logout.await.expect("the logout task ran to completion"),
            Ok(COMMIT)
        );
    }

    #[compio::test]
    async fn unanswered_logout_forward_times_out_and_clears_the_waiter() {
        let bus = SpyBus::default();
        bus.instant_timers.set(true);
        let shard = Rc::new(test_shard(&bus, 1, 3, FIRST_BOOT));

        let outcome = submit_logout_local_or_forward(&shard, 0xCAFE, 41, 9).await;
        assert_eq!(outcome, Err(MetadataSubmitError::ForwardTimedOut));

        let (_, forward) = bus.sole_replica_send::<ForwardLogoutHeader>();
        shard
            .on_message(forward_logout_result_message(&forward, &Ok(42)))
            .await;
    }

    #[test]
    fn unknown_logout_outcomes_pin_the_session() {
        for error in [
            MetadataSubmitError::ForwardTimedOut,
            MetadataSubmitError::InProgress,
            MetadataSubmitError::Canceled,
        ] {
            assert_eq!(
                transient_logout_code(&error),
                IggyError::TransientNotCommitted
            );
        }
        for error in [
            MetadataSubmitError::NotPrimary,
            MetadataSubmitError::PipelineFull,
            MetadataSubmitError::PrimaryUnreachable,
        ] {
            assert_eq!(
                transient_logout_code(&error),
                IggyError::TransientNotAccepted
            );
        }
    }

    /// The ownership refusal is the one terminal verdict, and it has to stay
    /// terminal across the hop or the SDK replays a login that cannot succeed.
    #[compio::test]
    async fn forwarded_register_keeps_the_ownership_refusal_terminal() {
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, 1, 3, FIRST_BOOT));
        let login = {
            let shard = Rc::clone(&shard);
            compio::runtime::spawn(async move {
                submit_register_local_or_forward(&shard, 0xCAFE, 7).await
            })
        };
        await_forward(&bus).await;
        let (_, forward) = bus.sole_replica_send::<ForwardRegisterHeader>();
        shard
            .on_message(forward_register_result(
                &forward,
                ForwardRegisterOutcome::ClientIdOwnedByAnotherUser,
                0,
                0,
            ))
            .await;
        let error = login
            .await
            .expect("the login task ran to completion")
            .expect_err("the refusal must surface");
        assert_eq!(error, MetadataSubmitError::ClientIdOwnedByAnotherUser);
        assert!(!error.is_transient(), "the refusal must stay terminal");
    }

    /// A primary that never answers must not strand the login or leak its
    /// parked entry; the client gets a transient failure and replays.
    #[compio::test]
    async fn unanswered_forward_times_out_and_clears_the_parked_login() {
        let bus = SpyBus::default();
        bus.instant_timers.set(true);
        let shard = Rc::new(test_shard(&bus, 1, 3, FIRST_BOOT));

        let outcome = submit_register_local_or_forward(&shard, 0xCAFE, 7).await;
        assert_eq!(outcome, Err(MetadataSubmitError::ForwardTimedOut));
        assert!(
            outcome.unwrap_err().is_transient(),
            "a lost answer is replayable"
        );

        // The parked entry is gone: the answer that arrives late finds nothing
        // and is dropped rather than completing a login nobody is waiting on.
        let (_, forward) = bus.sole_replica_send::<ForwardRegisterHeader>();
        shard
            .on_message(forward_register_result(
                &forward,
                ForwardRegisterOutcome::Ok,
                41,
                0,
            ))
            .await;
    }

    /// The reply frame is where an unknown outcome has to be told apart from a
    /// refusal: a forward that timed out may still commit, so the client must
    /// replay under the same client id instead of failing over under a fresh
    /// one. A verdict that refused the register carries no such doubt.
    #[compio::test]
    async fn transient_login_reply_marks_a_timed_out_forward_not_committed() {
        const TRANSPORT: u128 = 91;
        const VSR_CLIENT: u128 = 0xCAFE;
        const RESULT_OFFSET: usize = size_of::<ReplyHeader>() + 8;

        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, 1, 3, FIRST_BOOT));
        let request = request_message(Operation::Register, VSR_CLIENT, 0, 0, &[]);

        for (submit_error, expected) in [
            (
                MetadataSubmitError::ForwardTimedOut,
                IggyError::TransientNotCommitted,
            ),
            (
                MetadataSubmitError::NotPrimary,
                IggyError::TransientNotAccepted,
            ),
        ] {
            let error = LoginRegisterError::Transient(submit_error);
            surface_login_failure(&shard, TRANSPORT, request.header(), &error).await;

            let replies = bus.client_replies.borrow();
            assert_eq!(replies.len(), 1, "a transient login must answer a frame");
            let (client, frame) = &replies[0];
            assert_eq!(*client, TRANSPORT, "reply must target the transport id");
            let result =
                u32::from_le_bytes(frame[RESULT_OFFSET..RESULT_OFFSET + 4].try_into().unwrap());
            assert_eq!(result, expected.as_code(), "{error} must reply {expected}");
            drop(replies);
            bus.client_replies.borrow_mut().clear();
        }
    }

    /// A restart must not re-mint the nonce sequence of the boot before it. The
    /// nonce is never persisted, and an answer to a pre-restart forward can
    /// still be in flight: routed by a repeated nonce it would confirm a login
    /// the cluster never committed, with another client's epoch.
    #[compio::test]
    async fn a_restart_moves_the_forward_nonce_sequence() {
        assert_ne!(
            first_forward_nonce(FIRST_BOOT).await,
            first_forward_nonce(SECOND_BOOT).await,
            "each boot must start its nonce sequence somewhere the other did not"
        );
    }

    /// Seeding the counter from the incarnation means it can start one step
    /// short of wrapping, and a zero nonce is a frame every replica rejects.
    #[compio::test]
    async fn wrapping_forward_nonce_counter_skips_zero() {
        let nonce = first_forward_nonce(u128::from(u64::MAX)).await;
        assert_ne!(
            nonce & u128::from(u64::MAX),
            0,
            "a counter that wrapped must not contribute a zero nonce half"
        );
    }

    /// An answer echoing a client the nonce was never parked for must neither
    /// complete that login nor evict it, since a repeated nonce is exactly what
    /// a late cross-boot answer carries.
    #[compio::test]
    async fn forward_result_for_another_client_leaves_the_login_parked() {
        const CLIENT: u128 = 0xCAFE;
        const EPOCH: u64 = 41;
        const WATERMARK: u64 = 9;
        const FOREIGN_EPOCH: u64 = 77;

        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, 1, 3, FIRST_BOOT));
        let login = {
            let shard = Rc::clone(&shard);
            compio::runtime::spawn(async move {
                submit_register_local_or_forward(&shard, CLIENT, 7).await
            })
        };
        await_forward(&bus).await;
        let (_, forward) = bus.sole_replica_send::<ForwardRegisterHeader>();

        let mut foreign = forward;
        foreign.client = CLIENT + 1;
        shard
            .on_message(forward_register_result(
                &foreign,
                ForwardRegisterOutcome::Ok,
                FOREIGN_EPOCH,
                0,
            ))
            .await;
        shard
            .on_message(forward_register_result(
                &forward,
                ForwardRegisterOutcome::Ok,
                EPOCH,
                WATERMARK,
            ))
            .await;
        assert_eq!(
            login.await.expect("the login task ran to completion"),
            Ok(BoundSession {
                epoch: EPOCH,
                watermark: WATERMARK,
            }),
            "the login must bind the epoch addressed to it, and must still be \
             parked to receive it"
        );
    }

    /// A node that is primary itself never forwards -- that is what bounds a
    /// forward at one hop.
    #[compio::test]
    async fn primary_proposes_locally_instead_of_forwarding() {
        let bus = SpyBus::default();
        // Replica 0 of 3, view 0: this node IS the primary.
        let shard = Rc::new(test_shard(&bus, 0, 3, FIRST_BOOT));

        // No journal on the test shard, so the proposal cannot commit; what
        // matters is that nothing left over the interconnect.
        let _ = compio::time::timeout(
            Duration::from_millis(50),
            submit_register_local_or_forward(&shard, 0xCAFE, 7),
        )
        .await;
        assert!(
            bus.replica_sends.borrow().is_empty(),
            "a primary must propose in process"
        );
    }

    /// The nonce a shard booted at `incarnation` stamps on its first forward.
    /// Nobody answers, so the login abandons on the instant timer; the frame it
    /// left on the bus is what the caller is after.
    async fn first_forward_nonce(incarnation: u128) -> u128 {
        let bus = SpyBus::default();
        bus.instant_timers.set(true);
        let shard = Rc::new(test_shard(&bus, 1, 3, incarnation));
        let outcome = submit_register_local_or_forward(&shard, 0xCAFE, 7).await;
        assert_eq!(outcome, Err(MetadataSubmitError::ForwardTimedOut));
        bus.sole_replica_send::<ForwardRegisterHeader>().1.nonce
    }

    /// Let a spawned login run until it has parked on the primary's answer.
    async fn await_forward(bus: &SpyBus) {
        for _ in 0..1000 {
            if !bus.replica_sends.borrow().is_empty() {
                return;
            }
            compio::time::sleep(Duration::from_millis(1)).await;
        }
        panic!("the login never forwarded a register");
    }

    /// A sealed `ForwardRegisterResult` addressed to `forward`'s nonce.
    fn forward_register_result(
        forward: &ForwardRegisterHeader,
        outcome: ForwardRegisterOutcome,
        epoch: u64,
        watermark: u64,
    ) -> MessageBag {
        let bound = match outcome {
            ForwardRegisterOutcome::Ok => Ok(BoundSession { epoch, watermark }),
            ForwardRegisterOutcome::ClientIdOwnedByAnotherUser => {
                Err(MetadataSubmitError::ClientIdOwnedByAnotherUser)
            }
            _ => Err(MetadataSubmitError::NotPrimary),
        };
        MessageBag::ForwardRegisterResult(build_forward_register_result_message(
            forward.cluster,
            forward.view,
            0,
            forward.client,
            forward.nonce,
            &bound,
        ))
    }

    fn forward_logout_result_message(
        forward: &ForwardLogoutHeader,
        outcome: &Result<u64, MetadataSubmitError>,
    ) -> MessageBag {
        MessageBag::ForwardLogoutResult(build_forward_logout_result_message(
            forward.cluster,
            forward.view,
            0,
            forward.client,
            forward.nonce,
            outcome,
        ))
    }
}
