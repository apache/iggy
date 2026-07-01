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

//! Shard-0 HTTP/REST listener: state bridge, router, and the auth-issuance
//! half (login + bearer extraction). Management and data-plane routes land
//! later; this module owns only health, login, and the authenticated probe.

mod extractor;
mod jwt;

use std::cell::RefCell;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::rc::Rc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use bytes::Bytes;
use configs::http::HttpJwtConfig;
use iggy_binary_protocol::consensus::{
    Command2, EvictionHeader, EvictionReason, HEADER_SIZE, result_code, result_section_len,
};
use iggy_binary_protocol::requests::streams::{
    CreateStreamRequest, DeleteStreamRequest, PurgeStreamRequest, UpdateStreamRequest,
};
use iggy_binary_protocol::requests::topics::{
    CreateTopicRequest, DeleteTopicRequest, PurgeTopicRequest, UpdateTopicRequest,
};
use iggy_binary_protocol::responses::streams::get_stream::GetStreamResponse;
use iggy_binary_protocol::responses::topics::get_topic::GetTopicResponse;
use iggy_binary_protocol::version::IGGY_PROTOCOL_VERSION;
use iggy_binary_protocol::{
    GenericHeader, Operation, RequestHeader, WireDecode, WireEncode, WireName,
};
use iggy_common::create_stream::CreateStream;
use iggy_common::create_topic::CreateTopic;
use iggy_common::defaults::DEFAULT_ROOT_USER_ID;
use iggy_common::login_user::LoginUser;
use iggy_common::login_with_personal_access_token::LoginWithPersonalAccessToken;
use iggy_common::update_stream::UpdateStream;
use iggy_common::update_topic::UpdateTopic;
use iggy_common::wire_conversions::identifier_to_wire;
use iggy_common::{
    Identifier, IdentityInfo, IggyError, IggyTimestamp, StreamDetails, TokenInfo, TopicDetails,
    Validatable,
};
use message_bus::client_listener;
use secrecy::ExposeSecret;
use send_wrapper::SendWrapper;
use server::http::error::{CustomError, ErrorResponse};
use server_common::Message;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

use crate::auth::{verify_login_credentials, verify_pat_credentials};
use crate::bootstrap::ServerNgShard;
use crate::dispatch::{submit_client_request_on_owner, submit_register_on_owner};
use crate::http::extractor::Authenticated;
use crate::http::jwt::JwtManager;
use crate::login_register::LoginRegisterError;
use crate::server_error::ServerNgError;

/// `GET /ping` response body, matching the legacy HTTP server's health probe.
const PONG: &str = "pong";

/// Hard cap on live per-credential sessions. A leak-guard, not a tuning knob:
/// reaching it means this many distinct live tokens are in flight at once
/// (pathological at P1 volume). New sessions past the cap are refused with a
/// transient 503 rather than evicting a live one; the client retries. Expired
/// entries are dropped first, so the cap only bites on live oversubscription.
const MAX_HTTP_SESSIONS: usize = 100_000;

/// First per-session request id the write path hands out. VSR request numbers
/// are 1-based and strictly increasing within a session.
const FIRST_REQUEST_ID: u64 = 1;

/// One VSR session established for a single login credential (a JWT `jti` or a
/// PAT). Shared via `Rc` by every concurrent request bearing that credential,
/// so the session granularity is per-login.
struct HttpSession {
    /// Shard-0 client id minted for this credential; its top 16 bits are 0, so
    /// it shares the shard-0 id space with TCP virtual clients without
    /// colliding. Fills `RequestHeader.client` on every write.
    client_id: u128,
    /// Cluster session number returned by the VSR `Register` commit. Fills
    /// `RequestHeader.session` on every write.
    session: u64,
    /// User the credential authenticated as. Consumed by the write path for
    /// authorization.
    user_id: u32,
    /// Credential expiry in unix seconds (`u64::MAX` = never). Drives lazy
    /// eviction of stale table entries.
    expiry: u64,
    /// Serializes this session's writes: the guarded value is the NEXT request
    /// id. A `tokio::sync::Mutex` because the write path holds it across the
    /// submit `.await` so each session's request numbers reach the primary in
    /// order and stay gap-free for the depth-1 consensus dedup.
    gate: Mutex<u64>,
}

/// Rejection for protected routes.
///
/// Two failure classes get two statuses: a missing, invalid, or expired
/// credential is the caller's fault (401, rendered exactly like the legacy
/// server so SDK error bodies match), while a VSR `Register` that cannot commit
/// right now is a transient server condition (503) and must never masquerade as
/// an auth failure.
pub enum AuthError {
    Unauthenticated(IggyError),
    SessionUnavailable,
}

impl From<IggyError> for AuthError {
    fn from(error: IggyError) -> Self {
        Self::Unauthenticated(error)
    }
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        match self {
            // Reuse the legacy `IggyError -> CustomError` rendering so 401
            // bodies stay byte-identical to what the SDKs are tested against.
            Self::Unauthenticated(error) => CustomError::from(error).into_response(),
            // Register could not commit (no caught-up primary, pipeline full, or
            // a view-change cancel). Transient server condition -> 503, retryable.
            Self::SessionUnavailable => service_unavailable(),
        }
    }
}

/// Rejection for an authenticated control-plane write (`POST /streams` and the
/// writes that follow it).
///
/// Same two-class split as [`AuthError`], for the same reasons: a caller-side
/// validation failure or a committed business rejection (e.g. a duplicate
/// stream name) renders through the legacy `IggyError -> CustomError` map so
/// SDK error bodies stay byte-identical, while a write that cannot commit right
/// now is a transient server condition (503) and must never surface as a
/// business error or, worse, a 200 with a stale body.
enum WriteError {
    Rejected(IggyError),
    Unavailable,
}

impl IntoResponse for WriteError {
    fn into_response(self) -> Response {
        match self {
            Self::Rejected(error) => CustomError::from(error).into_response(),
            Self::Unavailable => service_unavailable(),
        }
    }
}

/// The shared 503 body for a request that could not commit right now: no
/// caught-up primary, a full pipeline, or a view-change cancel. Retryable, and
/// rendered with the `CannotEstablishConnection` code the SDKs treat as a
/// connection-level retry rather than a terminal error.
fn service_unavailable() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(ErrorResponse::from_error(
            IggyError::CannotEstablishConnection,
        )),
    )
        .into_response()
}

/// Shared shard-0 HTTP state.
///
/// Groups the shard handle, the JWT issuer/verifier, and the per-credential VSR
/// session table so every handler and the [`Authenticated`] extractor reach
/// them through one axum `State`.
struct HttpInner {
    shard: Rc<ServerNgShard>,
    jwt: JwtManager,
    /// Per-credential VSR sessions keyed by JWT `jti` / PAT hash. `RefCell` is
    /// sound here - shard 0 is single-threaded and the `SendWrapper` state
    /// bridge tolerates the `!Sync` interior - but the guard must never be held
    /// across an `.await` (see [`HttpInner::resolve_session`]).
    sessions: RefCell<HashMap<String, Rc<HttpSession>>>,
}

impl HttpInner {
    /// Resolve the VSR session for `key`, minting and Registering one on first
    /// use. Every later request bearing the same credential reuses it.
    ///
    /// Borrow discipline: the `RefCell` table guard is taken, read, and dropped
    /// WITHOUT crossing the `.await`. Holding it across the Register suspend
    /// would panic the moment a sibling shard-0 task borrowed the table while
    /// this one is parked (single-threaded `RefCell` + cooperative scheduling).
    async fn resolve_session(
        &self,
        key: String,
        user_id: u32,
        expiry: u64,
    ) -> Result<Rc<HttpSession>, AuthError> {
        let now = IggyTimestamp::now().to_secs();
        if let Some(session) = self.live_session(&key, now) {
            return Ok(session);
        }

        // Miss: mint + Register with no borrow held (an async VSR commit).
        let fresh = self.register_session(user_id, expiry).await?;

        let mut table = self.sessions.borrow_mut();
        // A concurrent first request for the same key may have Registered and
        // inserted while we were parked. Last-writer-wins: reuse the installed
        // entry and drop `fresh`, whose client id is then orphaned on the peers
        // until they evict it - an accepted rare cost at P1 volume.
        if let Some(session) = live_entry(&table, &key, now) {
            return Ok(session);
        }
        table.retain(|_, session| session.expiry > now);
        if table.len() >= MAX_HTTP_SESSIONS {
            // Still full after dropping expired entries: too many genuinely live
            // sessions. Refuse rather than evict a live one; the client retries.
            return Err(AuthError::SessionUnavailable);
        }
        table.insert(key, Rc::clone(&fresh));
        Ok(fresh)
    }

    /// Clone the live (non-expired) entry for `key`, if present. Confines the
    /// shared `RefCell` borrow to this call so it can never span an `.await`.
    fn live_session(&self, key: &str, now_secs: u64) -> Option<Rc<HttpSession>> {
        live_entry(&self.sessions.borrow(), key, now_secs)
    }

    /// Mint a shard-0 client id and run the VSR `Register` for a fresh session.
    /// Holds no table borrow; the caller inserts the result.
    async fn register_session(
        &self,
        user_id: u32,
        expiry: u64,
    ) -> Result<Rc<HttpSession>, AuthError> {
        let coordinator = self
            .shard
            .coordinator()
            .ok_or(AuthError::SessionUnavailable)?;
        // Reuse the TCP accept path's minter: it draws from the same shard-0
        // `client_seq`, so an HTTP session id can never collide with a TCP
        // virtual client's and the shard-0 tag (top 16 bits == 0) is preserved.
        let client_id = coordinator.mint_shard_zero_client_id();
        // The minter seeds at 1, so 0 is only reachable after a 2^112 wrap.
        // Guard anyway: `submit_register_in_process` asserts `client_id != 0`,
        // and an assert on this request path would be a panic.
        if client_id == 0 {
            return Err(AuthError::SessionUnavailable);
        }
        // Shared Register entry point; on shard 0 (always, for HTTP) it runs
        // `submit_register_in_process` directly on the metadata owner.
        let session = submit_register_on_owner(&self.shard, client_id)
            .await
            .map_err(|error| {
                warn!(?error, "server-ng HTTP: VSR Register submit failed");
                AuthError::SessionUnavailable
            })?;
        Ok(Rc::new(HttpSession {
            client_id,
            session,
            user_id,
            expiry,
            gate: Mutex::new(FIRST_REQUEST_ID),
        }))
    }
}

/// Borrow-and-clone a live table entry, or `None` if missing or expired. Shared
/// by the fast path and the post-Register re-check so neither leaks a guard.
fn live_entry(
    table: &HashMap<String, Rc<HttpSession>>,
    key: &str,
    now_secs: u64,
) -> Option<Rc<HttpSession>> {
    table
        .get(key)
        .filter(|session| session.expiry > now_secs)
        .map(Rc::clone)
}

/// Axum router state: shard-0's [`HttpInner`] behind an `Rc`, `!Send` yet
/// bridged into axum's `Send + Sync` requirement by `SendWrapper`. Sound
/// because the listener and every handler run on shard 0's compio thread - the
/// same thread that builds this state. Never touch it off that thread.
type HttpState = SendWrapper<Rc<HttpInner>>;

/// Bind the shard-0 HTTP listener and spawn the `cyper-axum` serve loop as a
/// background task on shard 0's compio runtime.
///
/// The caller gates this to shard 0 and to `http.enabled`; the listener stops
/// when the bus shutdown token fires.
///
/// # Errors
///
/// Returns [`ServerNgError`] if the JWT manager cannot be built from
/// `jwt_config` or the listener cannot bind to `addr`.
pub async fn start(
    shard: &Rc<ServerNgShard>,
    addr: SocketAddr,
    jwt_config: &HttpJwtConfig,
) -> Result<(), ServerNgError> {
    let jwt = JwtManager::build(jwt_config)?;
    let (listener, bound_addr) = client_listener::tcp::bind(addr).await?;
    info!(address = %bound_addr, "server-ng HTTP listener started");

    let state: HttpState = SendWrapper::new(Rc::new(HttpInner {
        shard: Rc::clone(shard),
        jwt,
        sessions: RefCell::new(HashMap::new()),
    }));
    let router = router(state);

    let shutdown = shard.bus.token();
    let handle = compio::runtime::spawn(async move {
        if let Err(error) = cyper_axum::serve(listener, router)
            .with_graceful_shutdown(async move { shutdown.wait().await })
            .await
        {
            error!(%error, "server-ng HTTP listener terminated with error");
        }
    });
    shard.bus.track_background(handle);

    Ok(())
}

/// Assemble the shard-0 router: unauthenticated health + login routes, plus
/// one authenticated probe that exercises the [`Authenticated`] extractor.
fn router(state: HttpState) -> Router {
    Router::new()
        .route("/ping", get(ping))
        .route("/users/login", post(login_user))
        .route(
            "/personal-access-tokens/login",
            post(login_with_personal_access_token),
        )
        .route("/users/me", get(get_me))
        .route("/streams", post(create_stream))
        .route(
            "/streams/{stream_id}",
            put(update_stream).delete(delete_stream),
        )
        .route("/streams/{stream_id}/purge", delete(purge_stream))
        .route("/streams/{stream_id}/topics", post(create_topic))
        .route(
            "/streams/{stream_id}/topics/{topic_id}",
            put(update_topic).delete(delete_topic),
        )
        .route(
            "/streams/{stream_id}/topics/{topic_id}/purge",
            delete(purge_topic),
        )
        .with_state(state)
}

/// Extracting the state here proves at compile time that the `!Send` state
/// bridges into axum's `Send + Sync` router state on shard 0's compio thread.
/// Ping needs no state, so it is discarded.
async fn ping(State(_state): State<HttpState>) -> &'static str {
    PONG
}

async fn login_user(
    State(state): State<HttpState>,
    Json(command): Json<LoginUser>,
) -> Result<Json<IdentityInfo>, CustomError> {
    let user_id = verify_login_credentials(
        &state.shard,
        &command.username,
        command.password.expose_secret(),
    )
    .map_err(|error| login_error_to_iggy(&error))?;
    issue_identity(&state, user_id)
}

async fn login_with_personal_access_token(
    State(state): State<HttpState>,
    Json(command): Json<LoginWithPersonalAccessToken>,
) -> Result<Json<IdentityInfo>, CustomError> {
    let user_id = verify_pat_credentials(&state.shard, command.token.expose_secret())
        .map_err(|error| login_error_to_iggy(&error))?;
    issue_identity(&state, user_id)
}

/// Interim authenticated probe: echoes the caller's resolved `user_id` so the
/// [`Authenticated`] extractor is covered end to end. Iggy exposes no "current
/// identity" route to reuse. This path also shadows a future
/// `GET /users/{user_id}` and must be reconciled when that route is added.
async fn get_me(identity: Authenticated) -> Json<IdentityInfo> {
    Json(IdentityInfo {
        user_id: identity.user_id,
        access_token: None,
    })
}

/// `POST /streams`: create a stream and render the committed reply as the same
/// `StreamDetails` JSON the legacy server returns.
///
/// The accepted body is name-only (`{"name": ...}`), matching the legacy
/// request; server-ng's wire `CreateStreamRequest` is likewise name-only and
/// auto-assigns the id, so there is no client-supplied stream id to honor.
async fn create_stream(
    State(state): State<HttpState>,
    identity: Authenticated,
    Json(command): Json<CreateStream>,
) -> Result<Json<StreamDetails>, WriteError> {
    let request = CreateStreamRequest {
        name: WireName::new(command.name)
            .map_err(|_| WriteError::Rejected(IggyError::InvalidStreamName))?,
    };
    let body = request.to_bytes();
    let payload = SendWrapper::new(submit_write(
        &state,
        &identity.session,
        Operation::CreateStream,
        &body,
    ))
    .await?;
    Ok(Json(decode_stream_details(&payload)?))
}

/// `PUT /streams/{stream_id}`: rename a stream. A committed write returns 204
/// with no body, matching the legacy server.
async fn update_stream(
    State(state): State<HttpState>,
    identity: Authenticated,
    Path(stream_id): Path<String>,
    Json(command): Json<UpdateStream>,
) -> Result<StatusCode, WriteError> {
    let stream_id = Identifier::from_str_value(&stream_id).map_err(WriteError::Rejected)?;
    let request = UpdateStreamRequest {
        stream_id: identifier_to_wire(&stream_id).map_err(WriteError::Rejected)?,
        name: WireName::new(command.name)
            .map_err(|_| WriteError::Rejected(IggyError::InvalidStreamName))?,
    };
    let body = request.to_bytes();
    SendWrapper::new(submit_write(
        &state,
        &identity.session,
        Operation::UpdateStream,
        &body,
    ))
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `DELETE /streams/{stream_id}`: delete a stream. Returns 204 on commit.
async fn delete_stream(
    State(state): State<HttpState>,
    identity: Authenticated,
    Path(stream_id): Path<String>,
) -> Result<StatusCode, WriteError> {
    let stream_id = Identifier::from_str_value(&stream_id).map_err(WriteError::Rejected)?;
    let request = DeleteStreamRequest {
        stream_id: identifier_to_wire(&stream_id).map_err(WriteError::Rejected)?,
    };
    let body = request.to_bytes();
    SendWrapper::new(submit_write(
        &state,
        &identity.session,
        Operation::DeleteStream,
        &body,
    ))
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `DELETE /streams/{stream_id}/purge`: drop a stream's messages. Returns 204.
async fn purge_stream(
    State(state): State<HttpState>,
    identity: Authenticated,
    Path(stream_id): Path<String>,
) -> Result<StatusCode, WriteError> {
    let stream_id = Identifier::from_str_value(&stream_id).map_err(WriteError::Rejected)?;
    let request = PurgeStreamRequest {
        stream_id: identifier_to_wire(&stream_id).map_err(WriteError::Rejected)?,
    };
    let body = request.to_bytes();
    SendWrapper::new(submit_write(
        &state,
        &identity.session,
        Operation::PurgeStream,
        &body,
    ))
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /streams/{stream_id}/topics`: create a topic under a stream and render
/// the committed reply as the same `TopicDetails` JSON the legacy server returns.
///
/// The stream comes from the path; the JSON body carries the remaining fields.
/// The submitted op is a plain `CreateTopic`; the metadata owner allocates the
/// consensus group ids and rewrites it to `CreateTopicWithAssignments` before
/// replication, so this handler stays a pure submit-and-decode.
async fn create_topic(
    State(state): State<HttpState>,
    identity: Authenticated,
    Path(stream_id): Path<String>,
    Json(command): Json<CreateTopic>,
) -> Result<Json<TopicDetails>, WriteError> {
    let stream_id = Identifier::from_str_value(&stream_id).map_err(WriteError::Rejected)?;
    // Rejects empty/oversized name, partitions_count > MAX, replication_factor == Some(0).
    command.validate().map_err(WriteError::Rejected)?;
    let request = CreateTopicRequest {
        stream_id: identifier_to_wire(&stream_id).map_err(WriteError::Rejected)?,
        partitions_count: command.partitions_count,
        compression_algorithm: command.compression_algorithm.as_code(),
        message_expiry: command.message_expiry.into(),
        max_topic_size: command.max_topic_size.into(),
        replication_factor: command.replication_factor.unwrap_or(0),
        name: WireName::new(command.name)
            .map_err(|_| WriteError::Rejected(IggyError::InvalidTopicName))?,
    };
    let body = request.to_bytes();
    let payload = SendWrapper::new(submit_write(
        &state,
        &identity.session,
        Operation::CreateTopic,
        &body,
    ))
    .await?;
    Ok(Json(decode_topic_details(&payload)?))
}

/// `PUT /streams/{stream_id}/topics/{topic_id}`: update a topic. Returns 204.
async fn update_topic(
    State(state): State<HttpState>,
    identity: Authenticated,
    Path((stream_id, topic_id)): Path<(String, String)>,
    Json(command): Json<UpdateTopic>,
) -> Result<StatusCode, WriteError> {
    let stream_id = Identifier::from_str_value(&stream_id).map_err(WriteError::Rejected)?;
    let topic_id = Identifier::from_str_value(&topic_id).map_err(WriteError::Rejected)?;
    // Also rejects replication_factor == Some(0), which `WireName` cannot see.
    command.validate().map_err(WriteError::Rejected)?;
    let request = UpdateTopicRequest {
        stream_id: identifier_to_wire(&stream_id).map_err(WriteError::Rejected)?,
        topic_id: identifier_to_wire(&topic_id).map_err(WriteError::Rejected)?,
        compression_algorithm: command.compression_algorithm.as_code(),
        message_expiry: command.message_expiry.into(),
        max_topic_size: command.max_topic_size.into(),
        replication_factor: command.replication_factor.unwrap_or(0),
        name: WireName::new(command.name)
            .map_err(|_| WriteError::Rejected(IggyError::InvalidTopicName))?,
    };
    let body = request.to_bytes();
    SendWrapper::new(submit_write(
        &state,
        &identity.session,
        Operation::UpdateTopic,
        &body,
    ))
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `DELETE /streams/{stream_id}/topics/{topic_id}`: delete a topic. Returns 204.
async fn delete_topic(
    State(state): State<HttpState>,
    identity: Authenticated,
    Path((stream_id, topic_id)): Path<(String, String)>,
) -> Result<StatusCode, WriteError> {
    let stream_id = Identifier::from_str_value(&stream_id).map_err(WriteError::Rejected)?;
    let topic_id = Identifier::from_str_value(&topic_id).map_err(WriteError::Rejected)?;
    let request = DeleteTopicRequest {
        stream_id: identifier_to_wire(&stream_id).map_err(WriteError::Rejected)?,
        topic_id: identifier_to_wire(&topic_id).map_err(WriteError::Rejected)?,
    };
    let body = request.to_bytes();
    SendWrapper::new(submit_write(
        &state,
        &identity.session,
        Operation::DeleteTopic,
        &body,
    ))
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `DELETE /streams/{stream_id}/topics/{topic_id}/purge`: drop a topic's
/// messages. Returns 204.
async fn purge_topic(
    State(state): State<HttpState>,
    identity: Authenticated,
    Path((stream_id, topic_id)): Path<(String, String)>,
) -> Result<StatusCode, WriteError> {
    let stream_id = Identifier::from_str_value(&stream_id).map_err(WriteError::Rejected)?;
    let topic_id = Identifier::from_str_value(&topic_id).map_err(WriteError::Rejected)?;
    let request = PurgeTopicRequest {
        stream_id: identifier_to_wire(&stream_id).map_err(WriteError::Rejected)?,
        topic_id: identifier_to_wire(&topic_id).map_err(WriteError::Rejected)?,
    };
    let body = request.to_bytes();
    SendWrapper::new(submit_write(
        &state,
        &identity.session,
        Operation::PurgeTopic,
        &body,
    ))
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Run one authenticated control-plane write end to end and return the committed
/// reply's typed payload. Shared by every write route: `create_stream` decodes
/// the payload into an entity, the update/delete/purge routes ignore it (it is
/// empty) and answer 204.
///
/// Serializes this session's writes behind its gate and holds it across the
/// submit so request ids reach the primary strictly in order. The gate advances
/// only on a committed `Reply`: a transient failure or an eviction leaves the id
/// free for the caller's retry, which the depth-1 dedup requires (the next
/// accepted request must be `committed + 1`, so a consumed-but-uncommitted id
/// would wedge the session on `RequestGap`).
///
/// Reply discrimination mirrors the SDK's `split_metadata_result`: the body
/// leads with a result section (`Some(0)` is success followed by the typed
/// payload; a nonzero first result is a committed business rejection carrying an
/// `IggyError` code). An eviction frame carries no result section and means the
/// session is dead (re-authenticate); a missing or short result section is a
/// malformed committed reply, mapped to an error rather than a false success.
async fn submit_write(
    state: &HttpInner,
    session: &HttpSession,
    operation: Operation,
    body: &[u8],
) -> Result<Bytes, WriteError> {
    // Interim authorization: until server-ng has an RBAC permissioner, every
    // control-plane write is root-only. A non-root credential is authenticated
    // but unprivileged, rejected before any consensus work is spent.
    if session.user_id != DEFAULT_ROOT_USER_ID {
        return Err(WriteError::Rejected(IggyError::Unauthorized));
    }
    let mut next_request_id = session.gate.lock().await;
    let message = build_request_message(
        operation,
        session.client_id,
        session.session,
        *next_request_id,
        body,
    );
    let reply = submit_client_request_on_owner(&state.shard, message).await;
    let Some(reply) = reply else {
        return Err(WriteError::Unavailable);
    };

    match reply.header().command {
        Command2::Reply => {
            *next_request_id += 1;
            drop(next_request_id);
            let size = reply.header().size as usize;
            let reply_body = reply.as_slice().get(HEADER_SIZE..size).unwrap_or_default();
            match result_code(reply_body) {
                Some(0) => {
                    let payload_start = result_section_len(reply_body)
                        .ok_or(WriteError::Rejected(IggyError::InvalidCommand))?;
                    let payload = reply_body
                        .get(payload_start..)
                        .ok_or(WriteError::Rejected(IggyError::InvalidCommand))?;
                    Ok(Bytes::copy_from_slice(payload))
                }
                Some(code) => Err(WriteError::Rejected(IggyError::from_code(code))),
                None => Err(WriteError::Rejected(IggyError::InvalidCommand)),
            }
        }
        Command2::Eviction => Err(WriteError::Rejected(eviction_error(&reply))),
        _ => Err(WriteError::Rejected(IggyError::InvalidCommand)),
    }
}

/// Build a `Message<RequestHeader>` for a control-plane write by filling a zeroed
/// `#[repr(C)]` header, mirroring `wire::rewrite_request_body` and the partition
/// reconciler's prepare builder. `body` is the already-encoded wire request,
/// copied in after the header.
fn build_request_message(
    operation: Operation,
    client_id: u128,
    session_id: u64,
    request_id: u64,
    body: &[u8],
) -> Message<RequestHeader> {
    let total = HEADER_SIZE + body.len();
    let mut message = Message::<RequestHeader>::new(total);
    message.as_mut_slice()[HEADER_SIZE..].copy_from_slice(body);
    let header = bytemuck::checked::try_from_bytes_mut::<RequestHeader>(
        &mut message.as_mut_slice()[..HEADER_SIZE],
    )
    .expect("zeroed bytes form a valid RequestHeader");
    header.command = Command2::Request;
    header.operation = operation;
    header.client = client_id;
    header.session = session_id;
    header.request = request_id;
    header.size = u32::try_from(total).expect("control-plane message size fits u32");
    message
}

/// Decode the `GetStreamResponse` payload of a committed create-stream reply into
/// `StreamDetails`. `payload` is the slice past the result section that
/// [`submit_write`] already validated as a success.
fn decode_stream_details(payload: &[u8]) -> Result<StreamDetails, WriteError> {
    let response = GetStreamResponse::decode_from(payload)
        .map_err(|_| WriteError::Rejected(IggyError::InvalidCommand))?;
    StreamDetails::try_from(response).map_err(WriteError::Rejected)
}

/// Decode the `GetTopicResponse` payload of a committed create-topic reply into
/// `TopicDetails`. `payload` is the slice past the result section that
/// [`submit_write`] already validated as a success.
fn decode_topic_details(payload: &[u8]) -> Result<TopicDetails, WriteError> {
    let response = GetTopicResponse::decode_from(payload)
        .map_err(|_| WriteError::Rejected(IggyError::InvalidCommand))?;
    TopicDetails::try_from(response).map_err(WriteError::Rejected)
}

/// Map an eviction frame to the same typed [`IggyError`] the SDK's
/// `decode_eviction` produces, so an HTTP caller sees the identical status a TCP
/// caller would (session-terminal reasons render as 401 -> re-authenticate).
/// Reuses the shared [`EvictionHeader`] primitive rather than hand-decoding
/// offsets; an unreadable frame falls back to re-authentication.
fn eviction_error(reply: &Message<GenericHeader>) -> IggyError {
    let Some(eviction) = reply
        .as_slice()
        .get(..HEADER_SIZE)
        .and_then(|bytes| bytemuck::checked::try_from_bytes::<EvictionHeader>(bytes).ok())
    else {
        return IggyError::Unauthenticated;
    };
    match eviction.reason {
        EvictionReason::InvalidCredentials => IggyError::InvalidCredentials,
        EvictionReason::InvalidToken => IggyError::InvalidPersonalAccessToken,
        EvictionReason::UserInactive
        | EvictionReason::SessionError
        | EvictionReason::NoSession
        | EvictionReason::SessionTooLow
        | EvictionReason::SessionReleaseMismatch => IggyError::Unauthenticated,
        EvictionReason::StaleClient => IggyError::StaleClient,
        EvictionReason::IncompatibleProtocol => {
            let server_max = eviction.server_protocol_version;
            let server_min = eviction.server_protocol_version_min;
            if server_min == 0 || server_max < server_min {
                IggyError::Unauthenticated
            } else {
                IggyError::IncompatibleProtocolVersion(
                    IGGY_PROTOCOL_VERSION,
                    server_min,
                    server_max,
                )
            }
        }
        EvictionReason::MalformedLogin => IggyError::InvalidFormat,
        _ => IggyError::InvalidCommand,
    }
}

/// Issue a fresh access token for `user_id` and wrap it in the exact
/// `IdentityInfo` shape the SDKs pin: numeric `user_id` plus an `access_token`
/// carrying the token string and its unix-seconds expiry.
fn issue_identity(inner: &HttpInner, user_id: u32) -> Result<Json<IdentityInfo>, CustomError> {
    let generated = inner.jwt.generate(user_id)?;
    Ok(Json(IdentityInfo {
        user_id: generated.user_id,
        access_token: Some(TokenInfo {
            token: generated.access_token,
            expiry: generated.access_token_expiry,
        }),
    }))
}

/// Grade a credential-verification failure onto the `IggyError` the legacy
/// HTTP error map already maps to a status + body, so `CustomError` renders
/// what the SDKs are tested against. `verify_*` only yield the first three
/// variants; the tail is unreachable but kept terminal (401) for the
/// `#[non_exhaustive]` enum.
const fn login_error_to_iggy(error: &LoginRegisterError) -> IggyError {
    match error {
        LoginRegisterError::InvalidCredentials => IggyError::InvalidCredentials,
        LoginRegisterError::InvalidToken => IggyError::InvalidPersonalAccessToken,
        LoginRegisterError::UserInactive => IggyError::UserInactive,
        _ => IggyError::Unauthenticated,
    }
}
