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

use axum::extract::{Path, Query, State};
use axum::http::{HeaderName, HeaderValue, StatusCode};
use axum::middleware::map_response;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use bytes::Bytes;
use configs::cluster::{ClusterConfig, ClusterNodeConfig};
use configs::http::HttpJwtConfig;
use consensus::{MetadataHandle, VsrConsensus};
use iggy_binary_protocol::codes::{
    GET_CONSUMER_GROUP_CODE, GET_CONSUMER_GROUPS_CODE, GET_STATS_CODE, GET_STREAM_CODE,
    GET_STREAMS_CODE, GET_TOPIC_CODE, GET_TOPICS_CODE, GET_USER_CODE, GET_USERS_CODE,
};
use iggy_binary_protocol::consensus::{
    Command2, EvictionHeader, EvictionReason, HEADER_SIZE, result_code, result_section_len,
};
use iggy_binary_protocol::requests::consumer_groups::{
    CreateConsumerGroupRequest, DeleteConsumerGroupRequest, GetConsumerGroupRequest,
    GetConsumerGroupsRequest,
};
use iggy_binary_protocol::requests::personal_access_tokens::{
    CreatePersonalAccessTokenRequest, DeletePersonalAccessTokenRequest,
};
use iggy_binary_protocol::requests::streams::{
    CreateStreamRequest, DeleteStreamRequest, GetStreamRequest, GetStreamsRequest,
    PurgeStreamRequest, UpdateStreamRequest,
};
use iggy_binary_protocol::requests::system::GetStatsRequest;
use iggy_binary_protocol::requests::topics::{
    CreateTopicRequest, DeleteTopicRequest, GetTopicRequest, GetTopicsRequest, PurgeTopicRequest,
    UpdateTopicRequest,
};
use iggy_binary_protocol::requests::users::{
    ChangePasswordRequest, CreateUserRequest, DeleteUserRequest, GetUserRequest, GetUsersRequest,
    UpdatePermissionsRequest, UpdateUserRequest,
};
use iggy_binary_protocol::responses::clients::client_response::ConsumerGroupInfoResponse;
use iggy_binary_protocol::responses::clients::get_client::ClientDetailsResponse;
use iggy_binary_protocol::responses::clients::get_clients::GetClientsResponse;
use iggy_binary_protocol::responses::consumer_groups::get_consumer_group::ConsumerGroupDetailsResponse;
use iggy_binary_protocol::responses::consumer_groups::get_consumer_groups::GetConsumerGroupsResponse;
use iggy_binary_protocol::responses::personal_access_tokens::RawPersonalAccessTokenResponse;
use iggy_binary_protocol::responses::streams::get_stream::GetStreamResponse;
use iggy_binary_protocol::responses::streams::get_streams::GetStreamsResponse;
use iggy_binary_protocol::responses::system::get_stats::StatsResponse;
use iggy_binary_protocol::responses::topics::get_topic::GetTopicResponse;
use iggy_binary_protocol::responses::topics::get_topics::GetTopicsResponse;
use iggy_binary_protocol::responses::users::get_user::UserDetailsResponse;
use iggy_binary_protocol::responses::users::get_users::GetUsersResponse;
use iggy_binary_protocol::version::IGGY_PROTOCOL_VERSION;
use iggy_binary_protocol::{
    GenericHeader, Operation, RequestHeader, WireDecode, WireEncode, WireName,
};
use iggy_common::change_password::ChangePassword;
use iggy_common::create_consumer_group::CreateConsumerGroup;
use iggy_common::create_personal_access_token::CreatePersonalAccessToken;
use iggy_common::create_stream::CreateStream;
use iggy_common::create_topic::CreateTopic;
use iggy_common::create_user::CreateUser;
use iggy_common::defaults::DEFAULT_ROOT_USER_ID;
use iggy_common::login_user::LoginUser;
use iggy_common::login_with_personal_access_token::LoginWithPersonalAccessToken;
use iggy_common::update_permissions::UpdatePermissions;
use iggy_common::update_stream::UpdateStream;
use iggy_common::update_topic::UpdateTopic;
use iggy_common::update_user::UpdateUser;
use iggy_common::wire_conversions::{
    clients_from_wire, consumer_groups_from_wire, identifier_to_wire, permissions_to_wire,
    streams_from_wire, topics_from_wire, users_from_wire,
};
use iggy_common::{
    ClientInfo, ClientInfoDetails, ClusterMetadata, ClusterNode, ClusterNodeRole,
    ClusterNodeStatus, ConsumerGroup, ConsumerGroupDetails, Identifier, IdentityInfo, IggyError,
    IggyTimestamp, RawPersonalAccessToken, Stats, Stream, StreamDetails, TokenInfo, Topic,
    TopicDetails, TransportEndpoints, UserInfo, UserInfoDetails, Validatable,
};
use message_bus::client_listener;
use metadata::impls::metadata::StreamsFrontend;
use secrecy::ExposeSecret;
use send_wrapper::SendWrapper;
use serde::Deserialize;
use server::http::error::{CustomError, ErrorResponse};
use server_common::Message;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

use crate::auth::{verify_login_credentials, verify_pat_credentials};
use crate::bootstrap::ServerNgShard;
use crate::dispatch::{submit_client_request_on_owner, submit_register_on_owner};
use crate::http::extractor::{Authenticated, Identity};
use crate::http::jwt::JwtManager;
use crate::login_register::LoginRegisterError;
use crate::pat::rewrite_pat_request_for_user;
use crate::responses::{
    NonReplicatedResponse, build_non_replicated_response, build_raw_pat_reply,
    connected_client_to_response,
};
use crate::server_error::ServerNgError;
use crate::users::maybe_rewrite_user_password_request;

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

/// Node name reported for the synthesized single self-node, matching the legacy
/// server's single-node synthesis.
const SELF_NODE_NAME: &str = "iggy-node";

/// Cluster name reported when no roster is configured, matching the legacy
/// server's single-node synthesis.
const SINGLE_NODE_CLUSTER_NAME: &str = "single-node";

/// Response header carrying the current VSR view number, set on every response
/// while this shard-0 node has live consensus.
const VIEW_HEADER: HeaderName = HeaderName::from_static("x-iggy-view");

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

/// Read consistency selected by the `?consistency=` query param.
///
/// `serializable` (the default) serves from this node's local metadata STM:
/// correct and consensus-free, but may trail the primary by the replication
/// delay. `linearizable` demands the freshest committed state and is honored
/// only on the primary; a follower answers 503 (see [`read_local`]).
#[derive(Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Consistency {
    #[default]
    Serializable,
    Linearizable,
}

/// `?consistency=` query wrapper. An absent param defaults to
/// [`Consistency::Serializable`]; an unrecognized value is a 400 (axum `Query`).
#[derive(Default, Deserialize)]
struct ConsistencyQuery {
    #[serde(default)]
    consistency: Consistency,
}

/// Rejection for an authenticated read route (`GET /streams`,
/// `GET /streams/{id}`, and the reads that follow). Three classes, three
/// renderings, mirroring [`WriteError`]'s split so error bodies stay
/// SDK-identical:
enum ReadError {
    /// Caller-side or STM rejection (bad identifier, unsupported op, or the
    /// interim non-root authz denial) graded through the legacy
    /// `IggyError -> status` map so SDK error bodies stay byte-identical.
    Rejected(IggyError),
    /// Requested entity is absent -> 404 with the legacy not-found body.
    NotFound,
    /// A linearizable read reached a follower. Transient from the caller's view
    /// -> 503, retryable; becomes a 307-to-leader once the roster + redirect
    /// land (see [`not_primary_response`]).
    NotPrimary,
}

impl IntoResponse for ReadError {
    fn into_response(self) -> Response {
        match self {
            Self::Rejected(error) => CustomError::from(error).into_response(),
            // Reuse the legacy 404 body so a missing stream renders exactly as
            // the legacy server's `CustomError::ResourceNotFound` does.
            Self::NotFound => CustomError::ResourceNotFound.into_response(),
            Self::NotPrimary => not_primary_response(),
        }
    }
}

/// The 503 body for a linearizable read that reached a follower. No node roster
/// exists at the HTTP layer yet (F7), so this node cannot 307-redirect to the
/// primary nor emit a `Location` (F3); the caller must retry, ideally against
/// the leader. Once both land this branch becomes a 307. Rendered as an
/// `ErrorResponse` so the body shape matches every other HTTP error.
fn not_primary_response() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(ErrorResponse {
            id: StatusCode::SERVICE_UNAVAILABLE.as_u16().into(),
            code: "not_primary".to_owned(),
            reason: "linearizable read requires the primary; retry against the leader".to_owned(),
            field: None,
        }),
    )
        .into_response()
}

/// Owned snapshot of the cluster topology `GET /cluster/metadata` reports.
///
/// The roster lives only in `ServerNgConfig` and is not reachable from
/// [`ServerNgShard`], so [`start`] copies the minimal pieces here at listener
/// start. Owned so the handler stays synchronous and never borrows config.
struct ClusterRoster {
    enabled: bool,
    name: String,
    nodes: Vec<ClusterNodeConfig>,
    /// This node's bound HTTP address, used to synthesize the sole self-node
    /// when no roster is configured (the only transport port known here).
    http_addr: SocketAddr,
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
    roster: ClusterRoster,
}

impl HttpInner {
    /// True when this shard-0 node is the current VSR metadata primary, i.e.
    /// `primary_index(current_view) == own_replica_id` - the check consensus
    /// `is_primary` already encapsulates over the live view and this replica's
    /// id. Absent consensus (never on shard 0 under VSR, only a no-replica
    /// build) is treated as not-primary so a linearizable read fails closed
    /// rather than serving possibly-stale local state as authoritative.
    fn is_metadata_primary(&self) -> bool {
        self.shard
            .plane
            .metadata()
            .consensus
            .as_ref()
            .is_some_and(VsrConsensus::is_primary)
    }

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
    cluster: &ClusterConfig,
) -> Result<(), ServerNgError> {
    let jwt = JwtManager::build(jwt_config)?;
    let (listener, bound_addr) = client_listener::tcp::bind(addr).await?;
    info!(address = %bound_addr, "server-ng HTTP listener started");

    let state: HttpState = SendWrapper::new(Rc::new(HttpInner {
        shard: Rc::clone(shard),
        jwt,
        sessions: RefCell::new(HashMap::new()),
        roster: ClusterRoster {
            enabled: cluster.enabled,
            name: cluster.name.clone(),
            nodes: cluster.nodes.clone(),
            http_addr: bound_addr,
        },
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
    // Cloned for the response layer so `X-Iggy-View` reads the live view per
    // response; the original `state` is moved into `with_state` below.
    let view_source = state.clone();
    Router::new()
        .route("/ping", get(ping))
        .route("/users/login", post(login_user))
        .route(
            "/personal-access-tokens/login",
            post(login_with_personal_access_token),
        )
        .route("/users/me", get(get_me))
        .route("/users", get(get_users).post(create_user))
        .route(
            "/users/{user_id}",
            get(get_user).put(update_user).delete(delete_user),
        )
        .route("/users/{user_id}/password", put(change_password))
        .route("/users/{user_id}/permissions", put(update_permissions))
        .route("/streams", get(get_streams).post(create_stream))
        .route(
            "/streams/{stream_id}",
            get(get_stream).put(update_stream).delete(delete_stream),
        )
        .route("/streams/{stream_id}/purge", delete(purge_stream))
        .route(
            "/streams/{stream_id}/topics",
            get(get_topics).post(create_topic),
        )
        .route(
            "/streams/{stream_id}/topics/{topic_id}",
            get(get_topic).put(update_topic).delete(delete_topic),
        )
        .route(
            "/streams/{stream_id}/topics/{topic_id}/purge",
            delete(purge_topic),
        )
        .route(
            "/streams/{stream_id}/topics/{topic_id}/consumer-groups",
            get(get_cgs).post(create_cg),
        )
        .route(
            "/streams/{stream_id}/topics/{topic_id}/consumer-groups/{group_id}",
            get(get_cg).delete(delete_cg),
        )
        // TODO(hubcio): GET /personal-access-tokens (list) deferred - needs a server-ng STM
        // list accessor (PATs sit behind the "Never iterate" apply invariant; TCP-ng lacks it too).
        .route("/personal-access-tokens", post(create_pat))
        .route("/personal-access-tokens/{name}", delete(delete_pat))
        .route("/stats", get(get_stats))
        .route("/cluster/metadata", get(get_cluster_metadata))
        .route("/clients", get(get_clients))
        .route("/clients/{client_id}", get(get_client))
        .with_state(state)
        .layer(map_response(move |response: Response| {
            let view_source = view_source.clone();
            async move { insert_view_header(&view_source, response) }
        }))
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

/// `GET /streams`: list every stream as the same `Vec<Stream>` JSON the legacy
/// server returns. A consensus-free local STM read via [`read_local`].
async fn get_streams(
    State(state): State<HttpState>,
    identity: Identity,
    Query(query): Query<ConsistencyQuery>,
) -> Result<Json<Vec<Stream>>, ReadError> {
    let body = GetStreamsRequest.to_bytes();
    let bytes = read_local(
        &state,
        identity.user_id,
        query.consistency,
        GET_STREAMS_CODE,
        &body,
    )?;
    let response = GetStreamsResponse::decode_from(&bytes)
        .map_err(|_| ReadError::Rejected(IggyError::InvalidCommand))?;
    Ok(Json(streams_from_wire(response)))
}

/// `GET /streams/{stream_id}`: fetch one stream by numeric id or name as the
/// same `StreamDetails` JSON the legacy server returns; 404 when absent. A
/// consensus-free local STM read via [`read_local`].
async fn get_stream(
    State(state): State<HttpState>,
    identity: Identity,
    Path(stream_id): Path<String>,
    Query(query): Query<ConsistencyQuery>,
) -> Result<Json<StreamDetails>, ReadError> {
    let stream_id = Identifier::from_str_value(&stream_id).map_err(ReadError::Rejected)?;
    let request = GetStreamRequest {
        stream_id: identifier_to_wire(&stream_id).map_err(ReadError::Rejected)?,
    };
    let body = request.to_bytes();
    let bytes = read_local(
        &state,
        identity.user_id,
        query.consistency,
        GET_STREAM_CODE,
        &body,
    )?;
    let response = GetStreamResponse::decode_from(&bytes)
        .map_err(|_| ReadError::Rejected(IggyError::InvalidCommand))?;
    Ok(Json(
        StreamDetails::try_from(response).map_err(ReadError::Rejected)?,
    ))
}

/// `GET /streams/{stream_id}/topics`: list a stream's topics as the same
/// `Vec<Topic>` JSON the legacy server returns. A consensus-free local STM read
/// via [`read_local`].
async fn get_topics(
    State(state): State<HttpState>,
    identity: Identity,
    Path(stream_id): Path<String>,
    Query(query): Query<ConsistencyQuery>,
) -> Result<Json<Vec<Topic>>, ReadError> {
    let stream_id = Identifier::from_str_value(&stream_id).map_err(ReadError::Rejected)?;
    let request = GetTopicsRequest {
        stream_id: identifier_to_wire(&stream_id).map_err(ReadError::Rejected)?,
    };
    let body = request.to_bytes();
    let bytes = read_local(
        &state,
        identity.user_id,
        query.consistency,
        GET_TOPICS_CODE,
        &body,
    )?;
    let response = GetTopicsResponse::decode_from(&bytes)
        .map_err(|_| ReadError::Rejected(IggyError::InvalidCommand))?;
    Ok(Json(
        topics_from_wire(response).map_err(ReadError::Rejected)?,
    ))
}

/// `GET /streams/{stream_id}/topics/{topic_id}`: fetch one topic by numeric id
/// or name as the same `TopicDetails` JSON the legacy server returns; 404 when
/// absent. A consensus-free local STM read via [`read_local`].
async fn get_topic(
    State(state): State<HttpState>,
    identity: Identity,
    Path((stream_id, topic_id)): Path<(String, String)>,
    Query(query): Query<ConsistencyQuery>,
) -> Result<Json<TopicDetails>, ReadError> {
    let stream_id = Identifier::from_str_value(&stream_id).map_err(ReadError::Rejected)?;
    let topic_id = Identifier::from_str_value(&topic_id).map_err(ReadError::Rejected)?;
    let request = GetTopicRequest {
        stream_id: identifier_to_wire(&stream_id).map_err(ReadError::Rejected)?,
        topic_id: identifier_to_wire(&topic_id).map_err(ReadError::Rejected)?,
    };
    let body = request.to_bytes();
    let bytes = read_local(
        &state,
        identity.user_id,
        query.consistency,
        GET_TOPIC_CODE,
        &body,
    )?;
    let response = GetTopicResponse::decode_from(&bytes)
        .map_err(|_| ReadError::Rejected(IggyError::InvalidCommand))?;
    Ok(Json(
        TopicDetails::try_from(response).map_err(ReadError::Rejected)?,
    ))
}

/// `GET /users`: list every user as the same `Vec<UserInfo>` JSON the legacy
/// server returns. A consensus-free local STM read via [`read_local`].
async fn get_users(
    State(state): State<HttpState>,
    identity: Identity,
    Query(query): Query<ConsistencyQuery>,
) -> Result<Json<Vec<UserInfo>>, ReadError> {
    let body = GetUsersRequest.to_bytes();
    let bytes = read_local(
        &state,
        identity.user_id,
        query.consistency,
        GET_USERS_CODE,
        &body,
    )?;
    let response = GetUsersResponse::decode_from(&bytes)
        .map_err(|_| ReadError::Rejected(IggyError::InvalidCommand))?;
    Ok(Json(
        users_from_wire(response).map_err(ReadError::Rejected)?,
    ))
}

/// `GET /users/{user_id}`: fetch one user by numeric id or name as the same
/// `UserInfoDetails` JSON the legacy server returns; 404 when absent. A
/// consensus-free local STM read via [`read_local`].
async fn get_user(
    State(state): State<HttpState>,
    identity: Identity,
    Path(user_id): Path<String>,
    Query(query): Query<ConsistencyQuery>,
) -> Result<Json<UserInfoDetails>, ReadError> {
    let user_id = Identifier::from_str_value(&user_id).map_err(ReadError::Rejected)?;
    let request = GetUserRequest {
        user_id: identifier_to_wire(&user_id).map_err(ReadError::Rejected)?,
    };
    let body = request.to_bytes();
    let bytes = read_local(
        &state,
        identity.user_id,
        query.consistency,
        GET_USER_CODE,
        &body,
    )?;
    let response = UserDetailsResponse::decode_from(&bytes)
        .map_err(|_| ReadError::Rejected(IggyError::InvalidCommand))?;
    Ok(Json(
        UserInfoDetails::try_from(response).map_err(ReadError::Rejected)?,
    ))
}

/// `GET /streams/{stream_id}/topics/{topic_id}/consumer-groups`: list a topic's
/// consumer groups as the same `Vec<ConsumerGroup>` JSON the legacy server
/// returns. A consensus-free local STM read via [`read_local`].
async fn get_cgs(
    State(state): State<HttpState>,
    identity: Identity,
    Path((stream_id, topic_id)): Path<(String, String)>,
    Query(query): Query<ConsistencyQuery>,
) -> Result<Json<Vec<ConsumerGroup>>, ReadError> {
    let stream_id = Identifier::from_str_value(&stream_id).map_err(ReadError::Rejected)?;
    let topic_id = Identifier::from_str_value(&topic_id).map_err(ReadError::Rejected)?;
    let request = GetConsumerGroupsRequest {
        stream_id: identifier_to_wire(&stream_id).map_err(ReadError::Rejected)?,
        topic_id: identifier_to_wire(&topic_id).map_err(ReadError::Rejected)?,
    };
    let body = request.to_bytes();
    let bytes = read_local(
        &state,
        identity.user_id,
        query.consistency,
        GET_CONSUMER_GROUPS_CODE,
        &body,
    )?;
    let response = GetConsumerGroupsResponse::decode_from(&bytes)
        .map_err(|_| ReadError::Rejected(IggyError::InvalidCommand))?;
    Ok(Json(consumer_groups_from_wire(response)))
}

/// `GET /streams/{stream_id}/topics/{topic_id}/consumer-groups/{group_id}`:
/// fetch one consumer group by numeric id or name as the same
/// `ConsumerGroupDetails` JSON the legacy server returns; 404 when absent. The
/// wire-to-domain conversion is infallible.
async fn get_cg(
    State(state): State<HttpState>,
    identity: Identity,
    Path((stream_id, topic_id, group_id)): Path<(String, String, String)>,
    Query(query): Query<ConsistencyQuery>,
) -> Result<Json<ConsumerGroupDetails>, ReadError> {
    let stream_id = Identifier::from_str_value(&stream_id).map_err(ReadError::Rejected)?;
    let topic_id = Identifier::from_str_value(&topic_id).map_err(ReadError::Rejected)?;
    let group_id = Identifier::from_str_value(&group_id).map_err(ReadError::Rejected)?;
    let request = GetConsumerGroupRequest {
        stream_id: identifier_to_wire(&stream_id).map_err(ReadError::Rejected)?,
        topic_id: identifier_to_wire(&topic_id).map_err(ReadError::Rejected)?,
        group_id: identifier_to_wire(&group_id).map_err(ReadError::Rejected)?,
    };
    let body = request.to_bytes();
    let bytes = read_local(
        &state,
        identity.user_id,
        query.consistency,
        GET_CONSUMER_GROUP_CODE,
        &body,
    )?;
    let response = ConsumerGroupDetailsResponse::decode_from(&bytes)
        .map_err(|_| ReadError::Rejected(IggyError::InvalidCommand))?;
    Ok(Json(ConsumerGroupDetails::from(response)))
}

/// `GET /stats`: server + storage counters as the same `Stats` JSON the legacy
/// server returns. `GET_STATS` is served by `build_non_replicated_response` like
/// the entity reads, so it flows through [`read_local`] unchanged rather than a
/// dedicated builder. No entity can be missing, so no 404 branch.
async fn get_stats(
    State(state): State<HttpState>,
    identity: Identity,
    Query(query): Query<ConsistencyQuery>,
) -> Result<Json<Stats>, ReadError> {
    let body = GetStatsRequest.to_bytes();
    let bytes = read_local(
        &state,
        identity.user_id,
        query.consistency,
        GET_STATS_CODE,
        &body,
    )?;
    let response = StatsResponse::decode_from(&bytes)
        .map_err(|_| ReadError::Rejected(IggyError::InvalidCommand))?;
    Ok(Json(Stats::from(response)))
}

/// `GET /cluster/metadata`: report the live cluster topology as the same
/// `ClusterMetadata` JSON the legacy server returns.
///
/// Auth-only: any valid token serves. Unlike the entity reads it bypasses both
/// the root gate and the consistency gate, and serves from the roster captured
/// at listener start plus the sync consensus getters, so it never touches the
/// metadata STM, consensus, or a VSR session and stays fully synchronous.
async fn get_cluster_metadata(
    State(state): State<HttpState>,
    _identity: Identity,
) -> Json<ClusterMetadata> {
    Json(build_cluster_metadata(&state))
}

/// `GET /clients`: list every connected client across all shards as the same
/// `Vec<ClientInfo>` JSON the legacy server returns.
///
/// Unlike the entity reads, connections live in each shard's session manager,
/// not the metadata STM, so this scatter-gathers over the shard mesh
/// (`list_all_clients`) instead of going through [`read_local`]. It still runs
/// the identical root-only + consistency gate via [`authorize_read`], so its
/// authorization matches every metadata read. The gather future is `!Send`,
/// bridged onto shard 0's thread by `SendWrapper` exactly as the write path
/// bridges its submit.
async fn get_clients(
    State(state): State<HttpState>,
    identity: Identity,
    Query(query): Query<ConsistencyQuery>,
) -> Result<Json<Vec<ClientInfo>>, ReadError> {
    authorize_read(&state, identity.user_id, query.consistency)?;
    let infos = SendWrapper::new(state.shard.list_all_clients()).await;
    let response = GetClientsResponse {
        clients: infos
            .iter()
            .map(|info| connected_client_to_response(&state.shard, info))
            .collect(),
    };
    Ok(Json(clients_from_wire(response)))
}

/// `GET /clients/{client_id}`: fetch one connected client as the same
/// `ClientInfoDetails` JSON the legacy server returns; 404 when absent.
///
/// The path id is the `u32` wire client id (the seq tail of the u128 transport
/// id), matching the legacy route's `Path<u32>`. There is no reverse map from
/// that id to a home shard, so this gathers every shard's clients and filters -
/// the same fan-out-and-filter as [`get_clients`] and the TCP `get_client`
/// dispatch. The wire-to-domain conversion is infallible.
async fn get_client(
    State(state): State<HttpState>,
    identity: Identity,
    Path(client_id): Path<u32>,
    Query(query): Query<ConsistencyQuery>,
) -> Result<Json<ClientInfoDetails>, ReadError> {
    authorize_read(&state, identity.user_id, query.consistency)?;
    let infos = SendWrapper::new(state.shard.list_all_clients()).await;
    // The wire client id is the u32 seq tail of the u128 transport id.
    #[allow(clippy::cast_possible_truncation)]
    let info = infos
        .iter()
        .find(|info| info.client_id as u32 == client_id)
        .ok_or(ReadError::NotFound)?;
    let consumer_groups = info.vsr_client_id.map_or_else(Vec::new, |vsr_client_id| {
        state
            .shard
            .plane
            .metadata()
            .mux_stm
            .streams()
            .consumer_group_memberships(vsr_client_id)
            .into_iter()
            .map(
                |(stream_id, topic_id, group_id)| ConsumerGroupInfoResponse {
                    stream_id,
                    topic_id,
                    group_id,
                },
            )
            .collect()
    });
    let response = ClientDetailsResponse {
        client: connected_client_to_response(&state.shard, info),
        consumer_groups,
    };
    Ok(Json(ClientInfoDetails::from(response)))
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

/// `POST /streams/{stream_id}/topics/{topic_id}/consumer-groups`: create a
/// consumer group under a topic and render the committed reply as the same
/// `ConsumerGroupDetails` JSON the legacy server returns.
///
/// The stream and topic come from the path; the JSON body is name-only
/// (`{"name": ...}`), matching the legacy request. The submitted op is a plain
/// `CreateConsumerGroup`; the metadata owner assigns the group id, so this
/// handler never allocates one itself.
async fn create_cg(
    State(state): State<HttpState>,
    identity: Authenticated,
    Path((stream_id, topic_id)): Path<(String, String)>,
    Json(command): Json<CreateConsumerGroup>,
) -> Result<Json<ConsumerGroupDetails>, WriteError> {
    let stream_id = Identifier::from_str_value(&stream_id).map_err(WriteError::Rejected)?;
    let topic_id = Identifier::from_str_value(&topic_id).map_err(WriteError::Rejected)?;
    let request = CreateConsumerGroupRequest {
        stream_id: identifier_to_wire(&stream_id).map_err(WriteError::Rejected)?,
        topic_id: identifier_to_wire(&topic_id).map_err(WriteError::Rejected)?,
        name: WireName::new(command.name)
            .map_err(|_| WriteError::Rejected(IggyError::InvalidConsumerGroupName))?,
    };
    let body = request.to_bytes();
    let payload = SendWrapper::new(submit_write(
        &state,
        &identity.session,
        Operation::CreateConsumerGroup,
        &body,
    ))
    .await?;
    Ok(Json(decode_consumer_group_details(&payload)?))
}

/// `DELETE /streams/{stream_id}/topics/{topic_id}/consumer-groups/{group_id}`:
/// delete a consumer group. Returns 204 on commit.
async fn delete_cg(
    State(state): State<HttpState>,
    identity: Authenticated,
    Path((stream_id, topic_id, group_id)): Path<(String, String, String)>,
) -> Result<StatusCode, WriteError> {
    let stream_id = Identifier::from_str_value(&stream_id).map_err(WriteError::Rejected)?;
    let topic_id = Identifier::from_str_value(&topic_id).map_err(WriteError::Rejected)?;
    let group_id = Identifier::from_str_value(&group_id).map_err(WriteError::Rejected)?;
    let request = DeleteConsumerGroupRequest {
        stream_id: identifier_to_wire(&stream_id).map_err(WriteError::Rejected)?,
        topic_id: identifier_to_wire(&topic_id).map_err(WriteError::Rejected)?,
        group_id: identifier_to_wire(&group_id).map_err(WriteError::Rejected)?,
    };
    let body = request.to_bytes();
    SendWrapper::new(submit_write(
        &state,
        &identity.session,
        Operation::DeleteConsumerGroup,
        &body,
    ))
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /users`: create a user and render the committed reply as the same
/// `UserInfoDetails` JSON the legacy server returns.
///
/// The plaintext password rides the JSON body; [`submit_write`] hashes it on
/// shard 0 before the request enters consensus (see
/// [`maybe_rewrite_user_password_request`]), so no plaintext is ever replicated.
async fn create_user(
    State(state): State<HttpState>,
    identity: Authenticated,
    Json(command): Json<CreateUser>,
) -> Result<Json<UserInfoDetails>, WriteError> {
    // Rejects empty/oversized username or password before any consensus work.
    command.validate().map_err(WriteError::Rejected)?;
    let request = CreateUserRequest {
        username: WireName::new(&command.username)
            .map_err(|_| WriteError::Rejected(IggyError::InvalidUsername))?,
        password: command.password.expose_secret().to_string(),
        status: command.status.as_code(),
        permissions: command.permissions.as_ref().map(permissions_to_wire),
    };
    let body = request.to_bytes();
    let payload = SendWrapper::new(submit_write(
        &state,
        &identity.session,
        Operation::CreateUser,
        &body,
    ))
    .await?;
    Ok(Json(decode_user_details(&payload)?))
}

/// `PUT /users/{user_id}`: update a user's username and/or status. Returns 204.
async fn update_user(
    State(state): State<HttpState>,
    identity: Authenticated,
    Path(user_id): Path<String>,
    Json(command): Json<UpdateUser>,
) -> Result<StatusCode, WriteError> {
    let user_id = Identifier::from_str_value(&user_id).map_err(WriteError::Rejected)?;
    // Rejects an oversized replacement username; a no-op when username is absent.
    command.validate().map_err(WriteError::Rejected)?;
    let request = UpdateUserRequest {
        user_id: identifier_to_wire(&user_id).map_err(WriteError::Rejected)?,
        username: command
            .username
            .as_deref()
            .map(WireName::new)
            .transpose()
            .map_err(|_| WriteError::Rejected(IggyError::InvalidUsername))?,
        status: command.status.map(|status| status.as_code()),
    };
    let body = request.to_bytes();
    SendWrapper::new(submit_write(
        &state,
        &identity.session,
        Operation::UpdateUser,
        &body,
    ))
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `DELETE /users/{user_id}`: delete a user. Returns 204.
async fn delete_user(
    State(state): State<HttpState>,
    identity: Authenticated,
    Path(user_id): Path<String>,
) -> Result<StatusCode, WriteError> {
    let user_id = Identifier::from_str_value(&user_id).map_err(WriteError::Rejected)?;
    let request = DeleteUserRequest {
        user_id: identifier_to_wire(&user_id).map_err(WriteError::Rejected)?,
    };
    let body = request.to_bytes();
    SendWrapper::new(submit_write(
        &state,
        &identity.session,
        Operation::DeleteUser,
        &body,
    ))
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `PUT /users/{user_id}/password`: change a user's password. Returns 204.
///
/// Like [`create_user`], the new password rides the JSON body in plaintext and
/// is hashed by [`submit_write`] on shard 0 before replication.
async fn change_password(
    State(state): State<HttpState>,
    identity: Authenticated,
    Path(user_id): Path<String>,
    Json(command): Json<ChangePassword>,
) -> Result<StatusCode, WriteError> {
    let user_id = Identifier::from_str_value(&user_id).map_err(WriteError::Rejected)?;
    // Rejects empty/oversized current or new password before any consensus work.
    command.validate().map_err(WriteError::Rejected)?;
    let request = ChangePasswordRequest {
        user_id: identifier_to_wire(&user_id).map_err(WriteError::Rejected)?,
        current_password: command.current_password.expose_secret().to_string(),
        new_password: command.new_password.expose_secret().to_string(),
    };
    let body = request.to_bytes();
    SendWrapper::new(submit_write(
        &state,
        &identity.session,
        Operation::ChangePassword,
        &body,
    ))
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `PUT /users/{user_id}/permissions`: replace a user's permissions. Returns 204.
async fn update_permissions(
    State(state): State<HttpState>,
    identity: Authenticated,
    Path(user_id): Path<String>,
    Json(command): Json<UpdatePermissions>,
) -> Result<StatusCode, WriteError> {
    let user_id = Identifier::from_str_value(&user_id).map_err(WriteError::Rejected)?;
    command.validate().map_err(WriteError::Rejected)?;
    let request = UpdatePermissionsRequest {
        user_id: identifier_to_wire(&user_id).map_err(WriteError::Rejected)?,
        permissions: command.permissions.as_ref().map(permissions_to_wire),
    };
    let body = request.to_bytes();
    SendWrapper::new(submit_write(
        &state,
        &identity.session,
        Operation::UpdatePermissions,
        &body,
    ))
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /personal-access-tokens`: mint a personal access token for the caller
/// and return its one-time raw secret as the same `{"token": ...}` JSON the
/// legacy server returns, with HTTP 200.
///
/// The raw token is non-deterministic and secret, so it must never enter
/// consensus: [`rewrite_pat_request_for_user`] (invoked inside
/// [`submit_committed`]) mints it on shard 0 and replicates only its hash, so a
/// successful committed reply body is empty. [`build_raw_pat_reply`] then splices
/// the raw secret back into that reply locally, using the confirmed commit
/// position. The token is surfaced only after the write commits; a malformed
/// splice fails closed rather than emitting a blank token.
///
/// A committed create can still carry a business rejection (duplicate name,
/// invalid expiry), so the result code is honored via [`committed_payload`] -
/// exactly as the mechanical routes do - BEFORE the secret is spliced. Only a
/// genuine success gets a token; a rejection renders the legacy error instead of
/// a bogus 200 + token.
async fn create_pat(
    State(state): State<HttpState>,
    identity: Authenticated,
    Json(command): Json<CreatePersonalAccessToken>,
) -> Result<Json<RawPersonalAccessToken>, WriteError> {
    // Rejects an empty/oversized token name before any consensus work.
    command.validate().map_err(WriteError::Rejected)?;
    let request = CreatePersonalAccessTokenRequest {
        name: WireName::new(&command.name)
            .map_err(|_| WriteError::Rejected(IggyError::InvalidPersonalAccessTokenName))?,
        expiry: command.expiry.into(),
    };
    let body = request.to_bytes();
    let (request_header, committed, raw_token) = SendWrapper::new(submit_committed(
        &state,
        &identity.session,
        Operation::CreatePersonalAccessToken,
        &body,
    ))
    .await?;
    // Reject a committed business error before splicing the secret; the success
    // payload is empty, so the returned slice is discarded.
    committed_payload(&committed)?;
    let reply =
        build_raw_pat_reply(&request_header, committed, raw_token).map_err(WriteError::Rejected)?;
    Ok(Json(RawPersonalAccessToken {
        token: decode_raw_pat_token(&reply)?,
    }))
}

/// `DELETE /personal-access-tokens/{name}`: delete one of the caller's tokens by
/// name. Returns 204 on commit. Inherits the root-only gate via [`submit_write`].
async fn delete_pat(
    State(state): State<HttpState>,
    identity: Authenticated,
    Path(name): Path<String>,
) -> Result<StatusCode, WriteError> {
    let request = DeletePersonalAccessTokenRequest {
        name: WireName::new(&name)
            .map_err(|_| WriteError::Rejected(IggyError::InvalidPersonalAccessTokenName))?,
    };
    let body = request.to_bytes();
    SendWrapper::new(submit_write(
        &state,
        &identity.session,
        Operation::DeletePersonalAccessToken,
        &body,
    ))
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Run one authenticated control-plane write to commit and hand back the
/// committed reply `Message`, the request header, and any raw PAT token minted
/// along the way. Shared core of every HTTP write: [`submit_write`] decodes the
/// reply body for the stream/topic/user routes, while [`create_pat`] needs the
/// raw `Message` + request header to substitute the one-time token.
///
/// Enforces the root-only admin gate, then serializes this session's writes
/// behind its gate and holds it across the submit so request ids reach the
/// primary strictly in order. The gate advances only on a committed `Reply`: a
/// transient failure or an eviction leaves the id free for the caller's retry,
/// which the depth-1 dedup requires (the next accepted request must be
/// `committed + 1`, so a consumed-but-uncommitted id would wedge the session on
/// `RequestGap`). An eviction means the session is dead, mapped to 401.
///
/// Both shard-0 request rewrites run here before consensus, mirroring the TCP
/// dispatch path: the PAT rewrite mints a raw token and replicates only its hash
/// (`CreatePersonalAccessToken`), and the user-password rewrite hashes plaintext
/// (`CreateUser` / `ChangePassword`). Both are no-ops for every other operation,
/// so plaintext secrets never enter consensus on any write. On either rewrite's
/// decode failure the gate is released without advancing, leaving the id free.
async fn submit_committed(
    state: &HttpInner,
    session: &HttpSession,
    operation: Operation,
    body: &[u8],
) -> Result<(RequestHeader, Message<GenericHeader>, Option<String>), WriteError> {
    // Interim authorization: until server-ng has an RBAC permissioner, every
    // control-plane write is root-only. A non-root credential is authenticated
    // but unprivileged, rejected before any consensus work is spent.
    if session.user_id != DEFAULT_ROOT_USER_ID {
        return Err(WriteError::Rejected(IggyError::Unauthorized));
    }
    // Per-session gate: serialize this session's request ids across the commit
    // await, advancing the id only after an observed Reply so a transient
    // failure leaves it free to retry.
    //
    // TODO(hubcio): cancellation-unsafe. If the client disconnects while parked
    // on the await below, this handler is dropped without advancing the id, yet
    // the prepare still commits pump-driven. The next write on this session then
    // reuses the id, gets the prior op's cached reply (Duplicate), silently loses
    // its own write, and returns a foreign success. Fix by moving the submit into
    // a detached shard-0 task that owns the id advance and signals the handler
    // via a oneshot, so the commit is recorded regardless of client liveness.
    let mut next_request_id = session.gate.lock().await;
    let message = build_request_message(
        operation,
        session.client_id,
        session.session,
        *next_request_id,
        body,
    );
    let (message, raw_token) =
        rewrite_pat_request_for_user(session.user_id, message).map_err(WriteError::Rejected)?;
    let message = maybe_rewrite_user_password_request(message).map_err(WriteError::Rejected)?;
    let request_header = *message.header();
    let reply = submit_client_request_on_owner(&state.shard, message).await;
    let Some(reply) = reply else {
        return Err(WriteError::Unavailable);
    };

    match reply.header().command {
        Command2::Reply => {
            *next_request_id += 1;
            drop(next_request_id);
            Ok((request_header, reply, raw_token))
        }
        Command2::Eviction => Err(WriteError::Rejected(eviction_error(&reply))),
        _ => Err(WriteError::Rejected(IggyError::InvalidCommand)),
    }
}

/// Run one authenticated control-plane write end to end and return the committed
/// reply's typed payload. Wraps [`submit_committed`] and decodes the reply body
/// via [`committed_payload`]: `create_stream` decodes the payload into an entity,
/// the update/delete/purge routes ignore it (it is empty) and answer 204.
async fn submit_write(
    state: &HttpInner,
    session: &HttpSession,
    operation: Operation,
    body: &[u8],
) -> Result<Bytes, WriteError> {
    let (_request_header, reply, _raw_token) =
        submit_committed(state, session, operation, body).await?;
    Ok(Bytes::copy_from_slice(committed_payload(&reply)?))
}

/// The two cross-cutting gates every authenticated read enforces before it
/// touches state. Factored out of [`read_local`] so the cross-shard client
/// reads (`get_clients` / `get_client`) - which serve from the shard session
/// managers, not the local STM, and so cannot use [`read_local`] - still pass
/// the identical gate. Keeping it in one place is what guarantees no read route
/// can silently skip authz or answer a linearizable request on a follower.
///
/// Root-only: until server-ng has an RBAC permissioner, every read is root-only,
/// mirroring the write gate in [`submit_committed`]. A non-root credential is
/// authenticated but unprivileged -> 403. Linearizable reads must come from the
/// primary; on a follower we cannot yet 307-redirect (no roster: F7) nor emit
/// the Location (F3), so 503 and let the client retry. This becomes a
/// 307-to-leader once both land.
fn authorize_read(
    state: &HttpInner,
    user_id: u32,
    consistency: Consistency,
) -> Result<(), ReadError> {
    if user_id != DEFAULT_ROOT_USER_ID {
        return Err(ReadError::Rejected(IggyError::Unauthorized));
    }
    if consistency == Consistency::Linearizable && !state.is_metadata_primary() {
        return Err(ReadError::NotPrimary);
    }
    Ok(())
}

/// Serve one authenticated read from the local metadata STM and hand back the
/// wire response body. Shared chokepoint for every read route whose data lives
/// in the metadata STM: it runs the shared [`authorize_read`] gate, then
/// delegates to [`build_non_replicated_response`], the SAME local-read entry the
/// TCP dispatch spine uses (`handle_default_non_replicated`), so an HTTP read and
/// a TCP read of the same entity return byte-identical bodies.
///
/// Reads never touch consensus or a VSR session: `build_non_replicated_response`
/// is a pure STM read. It is synchronous, so this helper is too - no submit
/// await, no gate, no `SendWrapper`. An absent entity surfaces as
/// [`NonReplicatedResponse::Empty`], mapped to 404 here because every REST read
/// whose entity can be missing shares that not-found shape.
fn read_local(
    state: &HttpInner,
    user_id: u32,
    consistency: Consistency,
    code: u32,
    body: &[u8],
) -> Result<Bytes, ReadError> {
    authorize_read(state, user_id, consistency)?;
    match build_non_replicated_response(&state.shard, code, body).map_err(ReadError::Rejected)? {
        NonReplicatedResponse::Empty => Err(ReadError::NotFound),
        NonReplicatedResponse::Bytes(bytes) => Ok(bytes),
    }
}

/// Build the live [`ClusterMetadata`] for `GET /cluster/metadata`.
///
/// Mirrors the legacy `get_cluster_metadata`: with a configured, non-empty
/// roster and live consensus, emit one node per roster entry and derive each
/// role from the current VSR view - the node at `primary_index(view)` leads, the
/// rest follow. Otherwise (cluster disabled, empty roster, or absent consensus)
/// fail closed to a synthesized self-node as the sole leader.
fn build_cluster_metadata(state: &HttpInner) -> ClusterMetadata {
    let roster = &state.roster;
    match state.shard.plane.metadata().consensus.as_ref() {
        Some(consensus) if roster.enabled && !roster.nodes.is_empty() => {
            let primary = consensus.primary_index(consensus.view());
            let nodes = roster
                .nodes
                .iter()
                .map(|node| ClusterNode {
                    name: node.name.clone(),
                    ip: node.ip.clone(),
                    endpoints: TransportEndpoints::new(
                        node.ports.tcp.unwrap_or(0),
                        node.ports.quic.unwrap_or(0),
                        node.ports.http.unwrap_or(0),
                        node.ports.websocket.unwrap_or(0),
                    ),
                    role: if node.replica_id == primary {
                        ClusterNodeRole::Leader
                    } else {
                        ClusterNodeRole::Follower
                    },
                    status: ClusterNodeStatus::Healthy,
                })
                .collect();
            ClusterMetadata {
                name: roster.name.clone(),
                nodes,
            }
        }
        _ => self_node_metadata(roster),
    }
}

/// Synthesize a single self-node [`ClusterMetadata`], mirroring the legacy
/// single-node path. Only this node's bound HTTP port is known at the HTTP layer
/// (the roster is not threaded onto the shard), so the other transports report 0.
fn self_node_metadata(roster: &ClusterRoster) -> ClusterMetadata {
    ClusterMetadata {
        name: SINGLE_NODE_CLUSTER_NAME.to_owned(),
        nodes: vec![ClusterNode {
            name: SELF_NODE_NAME.to_owned(),
            ip: roster.http_addr.ip().to_string(),
            endpoints: TransportEndpoints::new(0, 0, roster.http_addr.port(), 0),
            role: ClusterNodeRole::Leader,
            status: ClusterNodeStatus::Healthy,
        }],
    }
}

/// Set the [`VIEW_HEADER`] to the current VSR view on `response`. Omits the
/// header when this node has no live consensus: a missing header is
/// unambiguous, whereas a fabricated view number would mislead.
fn insert_view_header(state: &HttpInner, mut response: Response) -> Response {
    if let Some(consensus) = state.shard.plane.metadata().consensus.as_ref() {
        response
            .headers_mut()
            .insert(VIEW_HEADER, HeaderValue::from(consensus.view()));
    }
    response
}

/// Classify a committed reply's leading result section and return the typed
/// payload slice on success. Mirrors the SDK's `split_metadata_result`:
/// `Some(0)` is success and the payload follows the result section; a nonzero
/// first result is a committed business rejection carrying an `IggyError` code
/// (e.g. a duplicate token name); a missing or short section is a malformed
/// committed reply, mapped to an error rather than a false success. Shared by
/// [`submit_write`] and [`create_pat`] so a committed rejection can never render
/// as a 2xx.
fn committed_payload(reply: &Message<GenericHeader>) -> Result<&[u8], WriteError> {
    let size = reply.header().size as usize;
    let reply_body = reply.as_slice().get(HEADER_SIZE..size).unwrap_or_default();
    match result_code(reply_body) {
        Some(0) => {
            let payload_start = result_section_len(reply_body)
                .ok_or(WriteError::Rejected(IggyError::InvalidCommand))?;
            reply_body
                .get(payload_start..)
                .ok_or(WriteError::Rejected(IggyError::InvalidCommand))
        }
        Some(code) => Err(WriteError::Rejected(IggyError::from_code(code))),
        None => Err(WriteError::Rejected(IggyError::InvalidCommand)),
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

/// Decode the `UserDetailsResponse` payload of a committed create-user reply into
/// `UserInfoDetails`. `payload` is the slice past the result section that
/// [`submit_write`] already validated as a success.
fn decode_user_details(payload: &[u8]) -> Result<UserInfoDetails, WriteError> {
    let response = UserDetailsResponse::decode_from(payload)
        .map_err(|_| WriteError::Rejected(IggyError::InvalidCommand))?;
    UserInfoDetails::try_from(response).map_err(WriteError::Rejected)
}

/// Decode the `ConsumerGroupDetailsResponse` payload of a committed
/// create-consumer-group reply into `ConsumerGroupDetails`. `payload` is the
/// slice past the result section that [`submit_write`] already validated as a
/// success. The wire-to-domain conversion is infallible.
fn decode_consumer_group_details(payload: &[u8]) -> Result<ConsumerGroupDetails, WriteError> {
    let response = ConsumerGroupDetailsResponse::decode_from(payload)
        .map_err(|_| WriteError::Rejected(IggyError::InvalidCommand))?;
    Ok(ConsumerGroupDetails::from(response))
}

/// Extract the raw one-time token from a [`build_raw_pat_reply`] output. That
/// reply's body is a bare `RawPersonalAccessTokenResponse` (no leading result
/// section, unlike a committed metadata reply), so decode it straight past the
/// header.
fn decode_raw_pat_token(reply: &Message<GenericHeader>) -> Result<String, WriteError> {
    let size = reply.header().size as usize;
    let body = reply.as_slice().get(HEADER_SIZE..size).unwrap_or_default();
    let response = RawPersonalAccessTokenResponse::decode_from(body)
        .map_err(|_| WriteError::Rejected(IggyError::InvalidCommand))?;
    Ok(response.token.as_str().to_string())
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
