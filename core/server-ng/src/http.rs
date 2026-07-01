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

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::rc::Rc;
use std::time::Duration;

use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::header::LOCATION;
use axum::http::{HeaderName, HeaderValue, StatusCode};
use axum::middleware::map_response;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use bytes::{Bytes, BytesMut};
use configs::cluster::{ClusterConfig, ClusterNodeConfig};
use configs::http::HttpConfig;
use consensus::{MetadataHandle, VsrConsensus};
use iggy_binary_protocol::codes::{
    GET_CONSUMER_GROUP_CODE, GET_CONSUMER_GROUPS_CODE, GET_STATS_CODE, GET_STREAM_CODE,
    GET_STREAMS_CODE, GET_TOPIC_CODE, GET_TOPICS_CODE, GET_USER_CODE, GET_USERS_CODE,
};
use iggy_binary_protocol::consensus::{
    Command2, EvictionHeader, EvictionReason, HEADER_SIZE, result_code, result_section_len,
};
use iggy_binary_protocol::primitives::consumer::WireConsumer;
use iggy_binary_protocol::primitives::polling_strategy::WirePollingStrategy;
use iggy_binary_protocol::requests::consumer_groups::{
    CreateConsumerGroupRequest, DeleteConsumerGroupRequest, GetConsumerGroupRequest,
    GetConsumerGroupsRequest,
};
use iggy_binary_protocol::requests::consumer_offsets::{
    DeleteConsumerOffset2Request, GetConsumerOffsetRequest, StoreConsumerOffset2Request,
};
use iggy_binary_protocol::requests::messages::{
    PollMessagesRequest, RawMessage, SendMessagesEncoder,
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
    AckLevel, GenericHeader, Operation, ReplyHeader, RequestHeader, WireDecode, WireEncode,
    WireName,
};
use iggy_common::change_password::ChangePassword;
use iggy_common::create_consumer_group::CreateConsumerGroup;
use iggy_common::create_personal_access_token::CreatePersonalAccessToken;
use iggy_common::create_stream::CreateStream;
use iggy_common::create_topic::CreateTopic;
use iggy_common::create_user::CreateUser;
use iggy_common::defaults::DEFAULT_ROOT_USER_ID;
use iggy_common::delete_consumer_offset::DeleteConsumerOffset;
use iggy_common::get_consumer_offset::GetConsumerOffset;
use iggy_common::login_user::LoginUser;
use iggy_common::login_with_personal_access_token::LoginWithPersonalAccessToken;
use iggy_common::poll_messages::DEFAULT_PARTITION_ID;
use iggy_common::store_consumer_offset::StoreConsumerOffset;
use iggy_common::update_permissions::UpdatePermissions;
use iggy_common::update_stream::UpdateStream;
use iggy_common::update_topic::UpdateTopic;
use iggy_common::update_user::UpdateUser;
use iggy_common::wire_conversions::{
    clients_from_wire, consumer_groups_from_wire, consumer_to_wire, identifier_to_wire,
    partitioning_to_wire, permissions_to_wire, streams_from_wire, topics_from_wire,
    users_from_wire,
};
use iggy_common::{
    ClientInfo, ClientInfoDetails, ClusterMetadata, ClusterNode, ClusterNodeRole,
    ClusterNodeStatus, Consumer, ConsumerGroup, ConsumerGroupDetails, ConsumerOffsetInfo,
    Identifier, IdentityInfo, IggyError, IggyMessageView, IggyTimestamp, PollMessages,
    PolledMessages, RESYNC_REQUIRED_PARTITION_SENTINEL, RawPersonalAccessToken, SendMessages,
    Stats, Stream, StreamDetails, TokenInfo, Topic, TopicDetails, TransportEndpoints, UserInfo,
    UserInfoDetails, Validatable,
};
use message_bus::{BusMessage, InstanceToken, client_listener};
use metadata::impls::metadata::StreamsFrontend;
use secrecy::ExposeSecret;
use send_wrapper::SendWrapper;
use serde::Deserialize;
use server::http::error::{CustomError, ErrorResponse};
use server_common::Message;
use shard::{PartitionRead, PartitionReadReply};
use tokio::sync::Mutex;
use tracing::{error, info, warn};

use crate::auth::{verify_login_credentials, verify_pat_credentials};
use crate::bootstrap::ServerNgShard;
use crate::dispatch::{
    dispatch_partition_request, resolve_consumer_offset_request, resolve_poll_request,
    submit_client_request_on_owner, submit_register_on_owner,
};
use crate::http::extractor::{Authenticated, Identity};
use crate::http::jwt::JwtManager;
use crate::login_register::LoginRegisterError;
use crate::pat::rewrite_pat_request_for_user;
use crate::responses::{
    NonReplicatedResponse, build_non_replicated_response, build_polled_messages_body,
    build_raw_pat_reply, connected_client_to_response,
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

/// Bound on a partition write's (produce / consumer-offset write) wait for its
/// committed reply. Long enough to ride out a view change (plus the dispatch
/// gates' own routable-wait budget), short enough not to pin HTTP connections
/// behind a dead consensus group. On expiry the caller gets 504 and must treat
/// the outcome as unknown: the partition plane is at-least-once and the prepare
/// may still commit after the wait gave up, so the server never retries on the
/// caller's behalf.
const PARTITION_WRITE_REPLY_TIMEOUT: Duration = Duration::from_secs(10);

/// Per-session cap on concurrently awaited partition writes (produce /
/// consumer-offset). Bounds how much of [`MAX_IN_FLIGHT_WRITES_GLOBAL`] one
/// credential can occupy, so a session that outruns its own commits saturates
/// itself (429, its own backpressure signal) before it can starve every other
/// session out of the shared budget.
const MAX_IN_FLIGHT_WRITES_PER_SESSION: u32 = 32;

/// Shard-0-global budget for concurrently awaited partition writes, across all
/// sessions. Every admitted write parks a handler for up to
/// [`PARTITION_WRITE_REPLY_TIMEOUT`] while pinning its buffered body, and its
/// decode/encode/HS256 CPU runs on the same single-threaded core that pumps
/// consensus. The budget therefore bounds both starvation terms: budget x
/// `max_request_size` bounds the worst-case buffered bytes, and budget x
/// per-request CPU bounds how far admitted HTTP work can delay the consensus
/// pump. `?ack=none` produces sit outside the budget by design: they never
/// park a handler, so their whole cost is the synchronous handler body itself,
/// which admission could not shed anyway.
const MAX_IN_FLIGHT_WRITES_GLOBAL: u32 = 128;

/// VSR client id stamped on HTTP data-plane reads (poll / consumer-offset).
/// HTTP reads never Register a VSR client, and shard-0 client ids are minted
/// from 1, so 0 can never name a live consumer-group member: a group-kind
/// poll fences closed with `ConsumerGroupPartitionNotOwned` and answers the
/// re-sync sentinel empty poll - the same failure a stale TCP member sees.
/// Legacy HTTP polls with client id 0 for the same reason (no persistent
/// sessions, so no group membership).
const HTTP_READ_CLIENT_ID: u128 = 0;

/// Response header attesting what durability a produce response proves:
/// [`DURABILITY_REPLICATED_MEMORY`] after an awaited quorum commit,
/// [`DURABILITY_NONE`] for a `?ack=none` fire-and-forget.
const DURABILITY_HEADER: HeaderName = HeaderName::from_static("x-iggy-durability");
const DURABILITY_REPLICATED_MEMORY: &str = "replicated-memory";
const DURABILITY_NONE: &str = "none";

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
    /// Next data-plane request id. A separate, gate-free counter: partition ops
    /// are at-least-once with no consensus dedup, so the id only correlates the
    /// in-process reply slot and concurrent produces on one session are legal.
    /// A plain `Cell` suffices on single-threaded shard 0; ids are minted
    /// monotonically and never reused, which the slot-guard contract requires.
    data_request: Cell<u64>,
    /// Registry token of this session's lazily-installed in-process reply
    /// target (`None` until the first awaited partition write). Stored so
    /// session eviction can tear the registry entry down fenced by the same
    /// token.
    registry_token: Cell<Option<InstanceToken>>,
    /// Awaited partition writes currently in flight on this session, gated by
    /// [`MAX_IN_FLIGHT_WRITES_PER_SESSION`]. Only [`InFlightWriteGuard`]
    /// touches it, so every admission is paired with exactly one release.
    in_flight_writes: Cell<u32>,
}

impl HttpSession {
    /// Mint the next data-plane request id. Also consumed by the `?ack=none`
    /// path, which installs no slot: sharing one counter keeps a shed reply's
    /// id from ever colliding with a live awaited slot on this session.
    fn next_data_request_id(&self) -> u64 {
        let id = self.data_request.get();
        self.data_request.set(id + 1);
        id
    }
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

/// Rejection for a data-plane partition write (`POST .../messages` produce and
/// the `PUT`/`DELETE .../consumer-offsets` writes).
///
/// Split differently from [`WriteError`] because the partition plane replies
/// carry no committed error code: a pre-dispatch gate failure is an empty
/// reply distinguishable only by header (see [`classify_partition_reply`]),
/// and an unanswered write is a distinct outcome the caller must treat as
/// unknown rather than failed.
#[derive(Debug)]
enum PartitionWriteError {
    /// Caller-side rejection (bad identifier, oversized batch, the interim
    /// non-root denial) or a malformed reply frame, rendered through the
    /// legacy `IggyError -> status` map for SDK-identical bodies.
    Rejected(IggyError),
    /// The dispatch gates could not route the write: the stream, topic, or
    /// partition does not resolve (or never materialised within the routable
    /// budget). Rendered as the legacy 404 body.
    NotFound,
    /// The in-process reply slot could not be installed. Transient server
    /// condition -> the shared 503, retryable.
    Unavailable,
    /// This session is already at [`MAX_IN_FLIGHT_WRITES_PER_SESSION`]
    /// awaited writes. 429: the caller's own concurrency is the problem, so
    /// it must drain its outstanding writes before submitting more.
    TooManyInFlight,
    /// Shard 0 is already at [`MAX_IN_FLIGHT_WRITES_GLOBAL`] awaited writes
    /// across all sessions. 503 with its own code (distinct from the shared
    /// consensus-unavailable body) so an operator can tell admission shedding
    /// from a consensus outage.
    ServerBusy,
    /// No committed reply within [`PARTITION_WRITE_REPLY_TIMEOUT`], or the
    /// session's reply target was torn down mid-wait. 504: the commit may
    /// still land (at-least-once), so this is a hard "outcome unknown", not a
    /// failure the server may transparently retry. Carries the write's
    /// operation so the 504 body names which write kind timed out.
    Timeout(Operation),
}

impl IntoResponse for PartitionWriteError {
    fn into_response(self) -> Response {
        match self {
            Self::Rejected(error) => CustomError::from(error).into_response(),
            Self::NotFound => CustomError::ResourceNotFound.into_response(),
            Self::Unavailable => service_unavailable(),
            Self::TooManyInFlight => too_many_in_flight_response(),
            Self::ServerBusy => server_busy_response(),
            Self::Timeout(operation) => partition_write_timeout_response(operation),
        }
    }
}

/// 504 body for a partition write whose commit outcome is unknown, coded per
/// write kind so a caller can tell a produce timeout from an offset-write
/// timeout. Shaped like every other HTTP error (`ErrorResponse`) so clients
/// parse one error schema.
fn partition_write_timeout_response(operation: Operation) -> Response {
    let (code, reason) = match operation {
        Operation::SendMessages => (
            "produce_timeout",
            "produce was not acknowledged in time; the write may still commit",
        ),
        _ => (
            "offset_write_timeout",
            "consumer-offset write was not acknowledged in time; the write may still commit",
        ),
    };
    gateway_timeout_response(code, reason)
}

/// Shared 504 rendering for an in-band request the partition plane did not
/// answer in time, shaped like every other HTTP error (`ErrorResponse`) so
/// clients parse one error schema. Consumed by the partition-write reply wait
/// and the partition reads ([`ReadError::Timeout`]).
fn gateway_timeout_response(code: &str, reason: &str) -> Response {
    (
        StatusCode::GATEWAY_TIMEOUT,
        Json(ErrorResponse {
            id: StatusCode::GATEWAY_TIMEOUT.as_u16().into(),
            code: code.to_owned(),
            reason: reason.to_owned(),
            field: None,
        }),
    )
        .into_response()
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

/// 429 for a session at [`MAX_IN_FLIGHT_WRITES_PER_SESSION`] awaited partition
/// writes. Shaped like every other HTTP error (`ErrorResponse`) so clients
/// parse one error schema; the remedy is the caller's own: let outstanding
/// writes finish, then retry.
fn too_many_in_flight_response() -> Response {
    (
        StatusCode::TOO_MANY_REQUESTS,
        Json(ErrorResponse {
            id: StatusCode::TOO_MANY_REQUESTS.as_u16().into(),
            code: "too_many_in_flight_writes".to_owned(),
            reason: "session reached its in-flight write cap; await outstanding writes and retry"
                .to_owned(),
            field: None,
        }),
    )
        .into_response()
}

/// 503 for shard 0 at [`MAX_IN_FLIGHT_WRITES_GLOBAL`] awaited partition writes
/// across all sessions. A distinct `server_busy` code (unlike the shared
/// consensus-unavailable 503) so admission shedding is tellable from a
/// consensus outage; retry with backoff.
fn server_busy_response() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(ErrorResponse {
            id: StatusCode::SERVICE_UNAVAILABLE.as_u16().into(),
            code: "server_busy".to_owned(),
            reason: "shard is at its in-flight write budget; retry with backoff".to_owned(),
            field: None,
        }),
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

/// Produce acknowledgement selected by the `?ack=` query param.
///
/// `replicated` (the default) answers 201 only after the partition group's
/// quorum commit. `none` is fire-and-forget: the request is validated,
/// dispatched, and answered 202 immediately; the commit still happens, but its
/// reply is shed at the bus (no reply slot is installed).
#[derive(Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
enum ProduceAck {
    #[default]
    Replicated,
    None,
}

/// `?ack=` query wrapper. An absent param defaults to
/// [`ProduceAck::Replicated`]; an unrecognized value is a 400 (axum `Query`).
#[derive(Default, Deserialize)]
struct ProduceQuery {
    #[serde(default)]
    ack: ProduceAck,
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
    /// A linearizable read reached a follower and the primary's HTTP address was
    /// not resolvable from the roster. Fail-closed 503, retryable against the
    /// leader (see [`not_primary_response`]).
    NotPrimary,
    /// A linearizable read reached a follower and the current VSR primary's HTTP
    /// address resolved: 307 to that address carrying the original path and
    /// query, so the caller re-issues the read against the leader (see
    /// [`primary_redirect_response`]).
    RedirectToPrimary(String),
    /// A partition read (poll / consumer-offset) got no reply from the owning
    /// shard within the mesh budget. 504 like a produce timeout: the outcome is
    /// unknown (the abandoned read may still be running), so the caller retries.
    Timeout,
}

impl IntoResponse for ReadError {
    fn into_response(self) -> Response {
        match self {
            Self::Rejected(error) => CustomError::from(error).into_response(),
            // Reuse the legacy 404 body so a missing stream renders exactly as
            // the legacy server's `CustomError::ResourceNotFound` does.
            Self::NotFound => CustomError::ResourceNotFound.into_response(),
            Self::NotPrimary => not_primary_response(),
            Self::RedirectToPrimary(location) => primary_redirect_response(&location),
            Self::Timeout => gateway_timeout_response(
                "partition_read_timeout",
                "the partition owner did not answer the read in time; retry",
            ),
        }
    }
}

/// The 503 fail-closed body for a linearizable read that reached a follower
/// whose primary HTTP address could not be resolved (absent consensus, a roster
/// with no node at the primary index, or a port-less node). The resolvable case
/// is a 307 via [`primary_redirect_response`] instead. Rendered as an
/// `ErrorResponse` so the body shape matches every other HTTP error; the caller
/// retries against the leader.
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

/// 307 Temporary Redirect to the current VSR primary for a linearizable read
/// that reached a follower. `Location` is the primary's HTTP base plus the
/// original path and query, so the caller re-issues the identical read against
/// the leader. Dormant on a single node (always primary) and followed by no SDK
/// yet. A `Location` that is not a valid header value falls back to the 503.
fn primary_redirect_response(location: &str) -> Response {
    HeaderValue::from_str(location).map_or_else(
        |_| not_primary_response(),
        |value| {
            let mut response = StatusCode::TEMPORARY_REDIRECT.into_response();
            response.headers_mut().insert(LOCATION, value);
            response
        },
    )
}

/// Build the `Location` for a 307 redirect of a linearizable read to the VSR
/// primary: `http://<host>:<http-port><path_and_query>`. `None` when the roster
/// has no node at `primary_index`, that node exposes no HTTP port, or its `ip`
/// is not a valid address, so the caller fails closed to a 503 rather than
/// pointing at an unreachable target. Formats through [`SocketAddr`] so an IPv6
/// host is bracketed (`http://[::1]:8080/...`) rather than left ambiguous. Pure
/// (no consensus or axum dependency) so the redirect target is unit-tested in
/// isolation.
fn primary_redirect_location(
    roster: &ClusterRoster,
    primary_index: u8,
    path_and_query: &str,
) -> Option<String> {
    let node = roster
        .nodes
        .iter()
        .find(|node| node.replica_id == primary_index)?;
    let http_port = node.ports.http?;
    let ip = node.ip.parse::<IpAddr>().ok()?;
    let socket = SocketAddr::new(ip, http_port);
    Some(format!("http://{socket}{path_and_query}"))
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
    /// Awaited partition writes currently in flight across all sessions, gated
    /// by [`MAX_IN_FLIGHT_WRITES_GLOBAL`]. Only [`InFlightWriteGuard`] touches
    /// it, so every admission is paired with exactly one release.
    in_flight_writes: Cell<u32>,
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

    /// Grade a linearizable read that reached a follower: redirect (307) to the
    /// current VSR primary's HTTP address when it resolves from the roster, else
    /// fail closed to the 503. The target is the roster node whose `replica_id`
    /// equals `primary_index(view)`; an absent consensus, an unmatched id, or a
    /// port-less node all fall back to [`ReadError::NotPrimary`].
    fn not_primary_read_error(&self, path_and_query: &str) -> ReadError {
        let location = self
            .shard
            .plane
            .metadata()
            .consensus
            .as_ref()
            .and_then(|consensus| {
                let primary_index = consensus.primary_index(consensus.view());
                primary_redirect_location(&self.roster, primary_index, path_and_query)
            });
        location.map_or(ReadError::NotPrimary, ReadError::RedirectToPrimary)
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
        table.retain(|_, session| {
            if session.expiry > now {
                return true;
            }
            // Evicting the session also tears down its in-process reply
            // target (if a partition write ever installed one), cancelling any
            // still-parked reply waiters. Token-fenced so a stale sweep can
            // never remove a later occupant of the key.
            if let Some(token) = session.registry_token.get() {
                self.shard
                    .bus
                    .clients()
                    .remove_if_token_matches(session.client_id, token);
            }
            false
        });
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
            data_request: Cell::new(FIRST_REQUEST_ID),
            registry_token: Cell::new(None),
            in_flight_writes: Cell::new(0),
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
/// `http_config.jwt` or the listener cannot bind to `addr`.
pub async fn start(
    shard: &Rc<ServerNgShard>,
    addr: SocketAddr,
    http_config: &HttpConfig,
    cluster: &ClusterConfig,
) -> Result<(), ServerNgError> {
    let jwt = JwtManager::build(&http_config.jwt)?;
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
        in_flight_writes: Cell::new(0),
    }));
    // Saturating: a configured limit past the pointer width (32-bit target,
    // >4 GiB value) clamps to the largest enforceable cap instead of wrapping.
    let max_request_size =
        usize::try_from(http_config.max_request_size.as_bytes_u64()).unwrap_or(usize::MAX);
    let router = router(state, max_request_size);

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
///
/// `max_request_size` becomes the router-wide `DefaultBodyLimit` (413 past
/// it), exactly like the legacy server: it bounds the per-request term of the
/// admission math - what one body may cost in bytes and decode CPU - while
/// the in-flight caps bound the multiplier.
fn router(state: HttpState, max_request_size: usize) -> Router {
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
            "/streams/{stream_id}/topics/{topic_id}/messages",
            get(poll_messages).post(send_messages),
        )
        .route(
            "/streams/{stream_id}/topics/{topic_id}/consumer-offsets",
            get(get_consumer_offset).put(store_consumer_offset),
        )
        .route(
            "/streams/{stream_id}/topics/{topic_id}/consumer-offsets/{consumer_id}",
            delete(delete_consumer_offset),
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
        .layer(DefaultBodyLimit::max(max_request_size))
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
        &identity,
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
    let bytes = read_local(&state, &identity, query.consistency, GET_STREAM_CODE, &body)?;
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
    let bytes = read_local(&state, &identity, query.consistency, GET_TOPICS_CODE, &body)?;
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
    let bytes = read_local(&state, &identity, query.consistency, GET_TOPIC_CODE, &body)?;
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
    let bytes = read_local(&state, &identity, query.consistency, GET_USERS_CODE, &body)?;
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
    let bytes = read_local(&state, &identity, query.consistency, GET_USER_CODE, &body)?;
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
        &identity,
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
        &identity,
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
    let bytes = read_local(&state, &identity, query.consistency, GET_STATS_CODE, &body)?;
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
    authorize_read(&state, &identity, query.consistency)?;
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
    authorize_read(&state, &identity, query.consistency)?;
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

/// `GET /streams/{stream_id}/topics/{topic_id}/messages`: poll a batch of
/// messages as the same `PolledMessages` JSON the legacy server returns. The
/// query is the same flattened `PollMessages` shape the legacy server accepts
/// (`consumer_id`, `partition_id`, strategy `kind`+`value`, `count`,
/// `auto_commit`); stream and topic come from the path.
///
/// A non-replicated read served in band: the same resolution the TCP dispatch
/// runs ([`resolve_poll_request`]), then a mesh read on the owning shard and a
/// re-encode of the stored batches into the legacy wire body, decoded here by
/// the SDK's own [`PolledMessages::from_bytes`] so the JSON is field-identical
/// to a TCP poll. `auto_commit` rides [`resolve_poll_request`]'s args and is
/// honored by the owning shard's poll plan; no extra HTTP work.
async fn poll_messages(
    State(state): State<HttpState>,
    identity: Identity,
    Path((stream_id, topic_id)): Path<(String, String)>,
    Query(query): Query<PollMessages>,
    Query(consistency): Query<ConsistencyQuery>,
) -> Result<Json<PolledMessages>, ReadError> {
    authorize_read(&state, &identity, consistency.consistency)?;
    let stream_id = Identifier::from_str_value(&stream_id).map_err(ReadError::Rejected)?;
    let topic_id = Identifier::from_str_value(&topic_id).map_err(ReadError::Rejected)?;
    let wire = poll_wire_request(&stream_id, &topic_id, &query).map_err(ReadError::Rejected)?;
    let (namespace, partition_id, consumer, args) =
        match resolve_poll_request(&state.shard, &wire, HTTP_READ_CLIENT_ID) {
            Ok(decoded) => decoded,
            // TCP parity: a fenced group poll answers 200 with the re-sync
            // sentinel partition, not an error (see [`HTTP_READ_CLIENT_ID`]).
            Err(IggyError::ConsumerGroupPartitionNotOwned(..)) => {
                return Ok(Json(resync_required_polled_messages()));
            }
            // The remaining resolver failures are STM lookups that came up
            // empty (unknown stream, topic, partition, or consumer group), so
            // they render as the legacy 404 body.
            Err(_) => return Err(ReadError::NotFound),
        };
    let reply = SendWrapper::new(
        state
            .shard
            .partition_read(namespace, PartitionRead::Poll { consumer, args }),
    )
    .await;
    match reply {
        Some(PartitionReadReply::Poll {
            fragments,
            current_offset,
        }) => {
            let body = build_polled_messages_body(partition_id, current_offset, fragments)
                .map_err(ReadError::Rejected)?;
            Ok(Json(
                PolledMessages::from_bytes(body).map_err(ReadError::Rejected)?,
            ))
        }
        Some(PartitionReadReply::NotFound) => Err(ReadError::NotFound),
        Some(_) => Err(ReadError::Rejected(IggyError::InvalidCommand)),
        None => Err(ReadError::Timeout),
    }
}

/// `GET /streams/{stream_id}/topics/{topic_id}/consumer-offsets`: fetch a
/// consumer's stored offset as the same `ConsumerOffsetInfo` JSON the legacy
/// server returns. The query is the same flattened `GetConsumerOffset` shape
/// the legacy server accepts (`consumer_id`, optional `partition_id`).
///
/// A non-replicated read served in band, mirroring [`poll_messages`]. A
/// missing offset (never stored, or the partition unknown to its owner) is
/// the legacy 404: the TCP path replies an empty body the SDK decodes as
/// `None`, and the legacy HTTP server renders that `None` as
/// `CustomError::ResourceNotFound`.
async fn get_consumer_offset(
    State(state): State<HttpState>,
    identity: Identity,
    Path((stream_id, topic_id)): Path<(String, String)>,
    Query(query): Query<GetConsumerOffset>,
    Query(consistency): Query<ConsistencyQuery>,
) -> Result<Json<ConsumerOffsetInfo>, ReadError> {
    authorize_read(&state, &identity, consistency.consistency)?;
    let stream_id = Identifier::from_str_value(&stream_id).map_err(ReadError::Rejected)?;
    let topic_id = Identifier::from_str_value(&topic_id).map_err(ReadError::Rejected)?;
    let wire =
        consumer_offset_wire_request(&stream_id, &topic_id, &query).map_err(ReadError::Rejected)?;
    let (namespace, partition_id, consumer) =
        resolve_consumer_offset_request(&state.shard, &wire).map_err(|_| ReadError::NotFound)?;
    let reply = SendWrapper::new(
        state
            .shard
            .partition_read(namespace, PartitionRead::ConsumerOffset { consumer }),
    )
    .await;
    match reply {
        Some(PartitionReadReply::ConsumerOffset {
            stored: Some(stored_offset),
            current_offset,
        }) => Ok(Json(ConsumerOffsetInfo {
            partition_id,
            current_offset,
            stored_offset,
        })),
        Some(
            PartitionReadReply::ConsumerOffset { stored: None, .. } | PartitionReadReply::NotFound,
        ) => Err(ReadError::NotFound),
        Some(_) => Err(ReadError::Rejected(IggyError::InvalidCommand)),
        None => Err(ReadError::Timeout),
    }
}

/// `POST /streams/{stream_id}/topics/{topic_id}/messages`: produce a batch of
/// messages to a topic. The JSON body is the same `SendMessages` shape the
/// legacy server accepts (partitioning + base64 messages); stream and topic
/// come from the path.
///
/// Data plane, not control plane: the batch rides the partition group's own
/// consensus (at-least-once, no dedup, no session gate - concurrent produces
/// on one credential are legal), and the committed reply comes back through
/// the session's in-process reply slot rather than a submit return value.
/// The default answers 201 + `X-Iggy-Durability: replicated-memory` only
/// after the quorum commit; `?ack=none` answers 202 + `X-Iggy-Durability:
/// none` immediately after dispatch.
async fn send_messages(
    State(state): State<HttpState>,
    identity: Authenticated,
    Path((stream_id, topic_id)): Path<(String, String)>,
    Query(query): Query<ProduceQuery>,
    Json(command): Json<SendMessages>,
) -> Result<Response, PartitionWriteError> {
    // Interim authorization: root-only until server-ng has an RBAC
    // permissioner, mirroring the control-plane gate in `submit_committed`.
    if identity.session.user_id != DEFAULT_ROOT_USER_ID {
        return Err(PartitionWriteError::Rejected(IggyError::Unauthorized));
    }
    let stream_id =
        Identifier::from_str_value(&stream_id).map_err(PartitionWriteError::Rejected)?;
    let topic_id = Identifier::from_str_value(&topic_id).map_err(PartitionWriteError::Rejected)?;
    // Rejects an oversized partitioning key and an empty or oversized batch.
    command.validate().map_err(PartitionWriteError::Rejected)?;
    let body = encode_send_messages(&stream_id, &topic_id, &command)
        .map_err(PartitionWriteError::Rejected)?;
    match query.ack {
        ProduceAck::Replicated => {
            SendWrapper::new(partition_write_replicated(
                &state,
                &identity.session,
                Operation::SendMessages,
                &body,
            ))
            .await?;
            Ok((
                StatusCode::CREATED,
                [(
                    DURABILITY_HEADER,
                    HeaderValue::from_static(DURABILITY_REPLICATED_MEMORY),
                )],
            )
                .into_response())
        }
        ProduceAck::None => {
            SendWrapper::new(produce_unacked(&state, &identity.session, &body)).await;
            Ok((
                StatusCode::ACCEPTED,
                [(DURABILITY_HEADER, HeaderValue::from_static(DURABILITY_NONE))],
            )
                .into_response())
        }
    }
}

/// `PUT /streams/{stream_id}/topics/{topic_id}/consumer-offsets`: store a
/// consumer's offset. The JSON body is the same `StoreConsumerOffset` shape the
/// legacy server accepts (flattened `consumer_id`, optional `partition_id`,
/// `offset`); stream and topic come from the path. Returns 204 on commit,
/// matching the legacy server.
///
/// Data plane like a produce: the offset write is a replicated op on the
/// partition group's own consensus, awaited through the session's in-process
/// reply slot ([`partition_write_replicated`]). The v2 wire op is pinned to
/// `ack = Quorum` - `?ack=none` is a produce-only surface. The consumer
/// identifier passes through on the wire; the dispatch resolvers hash named
/// consumers and rewrite group ids server-side, identically to TCP.
async fn store_consumer_offset(
    State(state): State<HttpState>,
    identity: Authenticated,
    Path((stream_id, topic_id)): Path<(String, String)>,
    Json(command): Json<StoreConsumerOffset>,
) -> Result<StatusCode, PartitionWriteError> {
    // Interim authorization: root-only until server-ng has an RBAC
    // permissioner, mirroring the control-plane gate in `submit_committed`.
    if identity.session.user_id != DEFAULT_ROOT_USER_ID {
        return Err(PartitionWriteError::Rejected(IggyError::Unauthorized));
    }
    let stream_id =
        Identifier::from_str_value(&stream_id).map_err(PartitionWriteError::Rejected)?;
    let topic_id = Identifier::from_str_value(&topic_id).map_err(PartitionWriteError::Rejected)?;
    let request = store_offset_wire_request(&stream_id, &topic_id, &command)
        .map_err(PartitionWriteError::Rejected)?;
    let body = request.to_bytes();
    SendWrapper::new(partition_write_replicated(
        &state,
        &identity.session,
        Operation::StoreConsumerOffset2,
        &body,
    ))
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `DELETE /streams/{stream_id}/topics/{topic_id}/consumer-offsets/{consumer_id}`:
/// delete a consumer's stored offset. The consumer comes from the path and the
/// optional `partition_id` from the query, the same `DeleteConsumerOffset`
/// shape the legacy server accepts. Returns 204 on commit, matching the legacy
/// server. Same replicated partition write as [`store_consumer_offset`].
async fn delete_consumer_offset(
    State(state): State<HttpState>,
    identity: Authenticated,
    Path((stream_id, topic_id, consumer_id)): Path<(String, String, String)>,
    Query(query): Query<DeleteConsumerOffset>,
) -> Result<StatusCode, PartitionWriteError> {
    // Interim authorization: root-only until server-ng has an RBAC
    // permissioner, mirroring the control-plane gate in `submit_committed`.
    if identity.session.user_id != DEFAULT_ROOT_USER_ID {
        return Err(PartitionWriteError::Rejected(IggyError::Unauthorized));
    }
    let stream_id =
        Identifier::from_str_value(&stream_id).map_err(PartitionWriteError::Rejected)?;
    let topic_id = Identifier::from_str_value(&topic_id).map_err(PartitionWriteError::Rejected)?;
    // `Consumer::new` fixes the kind to `Consumer`, exactly as the legacy
    // handler does; HTTP cannot express a group-kind offset op.
    let consumer = Consumer::new(
        Identifier::from_str_value(&consumer_id).map_err(PartitionWriteError::Rejected)?,
    );
    let request = delete_offset_wire_request(&stream_id, &topic_id, &consumer, query.partition_id)
        .map_err(PartitionWriteError::Rejected)?;
    let body = request.to_bytes();
    SendWrapper::new(partition_write_replicated(
        &state,
        &identity.session,
        Operation::DeleteConsumerOffset2,
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

/// Encode a validated HTTP `SendMessages` into the `SendMessagesRequest` wire
/// body, mirroring the SDK's TCP produce encode: identifier + partitioning
/// wire conversion, then `RawMessage` borrows into [`SendMessagesEncoder`].
/// Balanced / messages-key partitioning passes through untouched; the
/// dispatch gates resolve it to a concrete partition server-side.
fn encode_send_messages(
    stream_id: &Identifier,
    topic_id: &Identifier,
    command: &SendMessages,
) -> Result<Bytes, IggyError> {
    let wire_stream_id = identifier_to_wire(stream_id)?;
    let wire_topic_id = identifier_to_wire(topic_id)?;
    let wire_partitioning = partitioning_to_wire(&command.partitioning)?;
    // Two passes because the view accessors' return borrows are tied to the
    // view value, not the batch buffer, so the views must outlive the borrows.
    let views: Vec<IggyMessageView<'_>> = command.batch.iter().collect();
    let raw_messages: Vec<RawMessage<'_>> = views
        .iter()
        .map(|view| RawMessage {
            id: view.header().id(),
            origin_timestamp: view.header().origin_timestamp(),
            headers: view.user_headers(),
            payload: view.payload(),
        })
        .collect();
    let size = SendMessagesEncoder::encoded_size(
        &wire_stream_id,
        &wire_topic_id,
        &wire_partitioning,
        &raw_messages,
    );
    let mut buf = BytesMut::with_capacity(size);
    SendMessagesEncoder::encode(
        &mut buf,
        &wire_stream_id,
        &wire_topic_id,
        &wire_partitioning,
        &raw_messages,
    );
    Ok(buf.freeze())
}

/// Map a validated HTTP poll query onto the wire `PollMessagesRequest` the
/// shared TCP resolver consumes, so both transports resolve one request shape.
/// The query's consumer kind is structurally always `Consumer`
/// (`Consumer::kind` is `#[serde(skip)]` - the flattened `kind` param names
/// the polling strategy), matching the legacy HTTP server.
fn poll_wire_request(
    stream_id: &Identifier,
    topic_id: &Identifier,
    query: &PollMessages,
) -> Result<PollMessagesRequest, IggyError> {
    Ok(PollMessagesRequest {
        consumer: WireConsumer {
            kind: query.consumer.kind.as_code(),
            id: identifier_to_wire(&query.consumer.id)?,
        },
        stream_id: identifier_to_wire(stream_id)?,
        topic_id: identifier_to_wire(topic_id)?,
        partition_id: query.partition_id,
        strategy: WirePollingStrategy {
            kind: query.strategy.kind.as_code(),
            value: query.strategy.value,
        },
        count: query.count,
        auto_commit: query.auto_commit,
    })
}

/// Map a validated HTTP consumer-offset query onto the wire
/// `GetConsumerOffsetRequest` the shared TCP resolver consumes. An omitted
/// `partition_id` defaults to partition 0, matching the legacy server's
/// `resolve_consumer_with_partition_id` (`partition_id.unwrap_or(0)`).
fn consumer_offset_wire_request(
    stream_id: &Identifier,
    topic_id: &Identifier,
    query: &GetConsumerOffset,
) -> Result<GetConsumerOffsetRequest, IggyError> {
    Ok(GetConsumerOffsetRequest {
        consumer: WireConsumer {
            kind: query.consumer.kind.as_code(),
            id: identifier_to_wire(&query.consumer.id)?,
        },
        stream_id: identifier_to_wire(stream_id)?,
        topic_id: identifier_to_wire(topic_id)?,
        partition_id: Some(query.partition_id.unwrap_or(DEFAULT_PARTITION_ID)),
    })
}

/// Map a validated HTTP store-offset body onto the v2 wire request
/// (`StoreConsumerOffset2Request`), `ack` pinned to `Quorum` so the route can
/// await the committed reply. The body's consumer kind is structurally always
/// `Consumer` (`Consumer::kind` is `#[serde(skip)]`), matching the legacy HTTP
/// server; `partition_id` passes through as the wire `Option` (flag byte +
/// u32) for the server-side resolvers to ground.
fn store_offset_wire_request(
    stream_id: &Identifier,
    topic_id: &Identifier,
    command: &StoreConsumerOffset,
) -> Result<StoreConsumerOffset2Request, IggyError> {
    Ok(StoreConsumerOffset2Request {
        consumer: consumer_to_wire(&command.consumer)?,
        stream_id: identifier_to_wire(stream_id)?,
        topic_id: identifier_to_wire(topic_id)?,
        partition_id: command.partition_id,
        offset: command.offset,
        ack: AckLevel::Quorum,
    })
}

/// Map a validated HTTP delete-offset request onto the v2 wire request
/// (`DeleteConsumerOffset2Request`), `ack` pinned to `Quorum` like
/// [`store_offset_wire_request`].
fn delete_offset_wire_request(
    stream_id: &Identifier,
    topic_id: &Identifier,
    consumer: &Consumer,
    partition_id: Option<u32>,
) -> Result<DeleteConsumerOffset2Request, IggyError> {
    Ok(DeleteConsumerOffset2Request {
        consumer: consumer_to_wire(consumer)?,
        stream_id: identifier_to_wire(stream_id)?,
        topic_id: identifier_to_wire(topic_id)?,
        partition_id,
        ack: AckLevel::Quorum,
    })
}

/// The empty `PolledMessages` a fenced consumer-group poll answers, carrying
/// the re-sync sentinel in `partition_id` exactly as the TCP dispatch replies
/// it, so an SDK re-syncs its assignment instead of reading end-of-partition.
const fn resync_required_polled_messages() -> PolledMessages {
    PolledMessages {
        partition_id: RESYNC_REQUIRED_PARTITION_SENTINEL,
        current_offset: 0,
        count: 0,
        messages: Vec::new(),
    }
}

/// In-flight admission token for one awaited partition write. One guard owns
/// both releases (session + global) so success, every error return, the reply
/// timeout, and handler cancellation (the client hanging up mid-await drops
/// this future) all decrement through the same `Drop`.
struct InFlightWriteGuard<'a> {
    session_in_flight: &'a Cell<u32>,
    global_in_flight: &'a Cell<u32>,
}

impl Drop for InFlightWriteGuard<'_> {
    fn drop(&mut self) {
        self.session_in_flight.set(self.session_in_flight.get() - 1);
        self.global_in_flight.set(self.global_in_flight.get() - 1);
    }
}

/// Admit one awaited partition write against the per-session cap and the
/// shard-0 global budget, incrementing both counters only when both pass. The
/// session cap is checked first so a session that saturates itself reads as
/// its own 429 rather than as server-wide pressure.
fn admit_partition_write<'a>(
    session_in_flight: &'a Cell<u32>,
    global_in_flight: &'a Cell<u32>,
) -> Result<InFlightWriteGuard<'a>, PartitionWriteError> {
    if session_in_flight.get() >= MAX_IN_FLIGHT_WRITES_PER_SESSION {
        return Err(PartitionWriteError::TooManyInFlight);
    }
    if global_in_flight.get() >= MAX_IN_FLIGHT_WRITES_GLOBAL {
        return Err(PartitionWriteError::ServerBusy);
    }
    session_in_flight.set(session_in_flight.get() + 1);
    global_in_flight.set(global_in_flight.get() + 1);
    Ok(InFlightWriteGuard {
        session_in_flight,
        global_in_flight,
    })
}

/// Run one awaited partition write (produce / consumer-offset write) end to
/// end: admit against the in-flight caps, install the reply slot, dispatch
/// into the partition plane, and wait (bounded) for the committed reply.
///
/// Slot-before-dispatch is load-bearing: every pre-dispatch gate failure
/// inside [`dispatch_partition_request`] replies through `send_to_client`,
/// which fires an installed slot, so one slot catches every exit. The slot
/// guard borrows the registry, which is why this whole future runs inside
/// the caller's `SendWrapper` on shard 0.
async fn partition_write_replicated(
    state: &HttpInner,
    session: &HttpSession,
    operation: Operation,
    body: &[u8],
) -> Result<(), PartitionWriteError> {
    // Admission sits here rather than before body decode: axum's extractors
    // already buffered and deserialized the body (bounded by the router-wide
    // `DefaultBodyLimit`) before the handler ran, so the caps gate what is
    // actually unbounded - the slot install, the dispatch, and the parked
    // reply await that pins this request's buffers for up to the reply
    // timeout. Held across every exit below; released by `Drop`.
    let _in_flight = admit_partition_write(&session.in_flight_writes, &state.in_flight_writes)?;
    ensure_in_process_reply_target(state, session);
    let request_id = session.next_data_request_id();
    let message = build_request_message(
        operation,
        session.client_id,
        session.session,
        request_id,
        body,
    );
    let (guard, receiver) = state
        .shard
        .bus
        .clients()
        .install_reply_slot(session.client_id, request_id)
        .map_err(|error| {
            warn!(
                ?error,
                ?operation,
                "server-ng HTTP: partition write reply slot install failed"
            );
            PartitionWriteError::Unavailable
        })?;
    dispatch_partition_request(
        &state.shard,
        message,
        session.client_id,
        session.session,
        session.client_id,
    )
    .await;
    let outcome = compio::time::timeout(PARTITION_WRITE_REPLY_TIMEOUT, receiver).await;
    // Removes the slot unless the reply already fired, so a late commit
    // reply after a timeout sheds at the bus instead of leaking a waiter.
    drop(guard);
    match outcome {
        Ok(Ok(reply)) => classify_partition_reply(&reply),
        // Cancelled (reply target torn down by session eviction mid-wait) or
        // elapsed: same caller contract either way - outcome unknown, 504.
        Ok(Err(_)) | Err(_) => Err(PartitionWriteError::Timeout(operation)),
    }
}

/// Fire-and-forget produce (`?ack=none`): no reply slot, no wait. The commit
/// still happens; its reply (and any gate-failure reply) targets a request id
/// with no slot installed and is shed at the bus by design.
async fn produce_unacked(state: &HttpInner, session: &HttpSession, body: &[u8]) {
    let request_id = session.next_data_request_id();
    let message = build_request_message(
        Operation::SendMessages,
        session.client_id,
        session.session,
        request_id,
        body,
    );
    dispatch_partition_request(
        &state.shard,
        message,
        session.client_id,
        session.session,
        session.client_id,
    )
    .await;
}

/// Install this session's in-process reply target on first data-plane use.
///
/// The registry key is the session's shard-0 client id - the same id stamped
/// into `RequestHeader.client` - so a partition reply routed through
/// `send_to_client` lands on this entry and resolves the request-keyed slot.
/// `None` from the registry means the key is already occupied; treat it as
/// installed but leave the token unset so this session never tears down an
/// entry it does not own.
fn ensure_in_process_reply_target(state: &HttpInner, session: &HttpSession) {
    if session.registry_token.get().is_some() {
        return;
    }
    if let Some(token) = state
        .shard
        .bus
        .clients()
        .insert_in_process(session.client_id)
    {
        session.registry_token.set(Some(token));
    }
}

/// Discriminate a partition write reply. Partition replies carry no result
/// section and no error code (success and gate failure are both empty-bodied),
/// so the only wire discriminator is the reply's `op`: a committed reply is
/// built from its prepare header, whose op (the partition group's commit
/// number) is always >= 1, while the pre-dispatch gate failures reply through
/// `build_empty_reply` with 0 in that field. The gate reply cannot name which
/// entity was missing, hence the generic legacy 404 body.
fn classify_partition_reply(reply: &BusMessage) -> Result<(), PartitionWriteError> {
    let header = reply
        .as_slice()
        .get(..HEADER_SIZE)
        .and_then(|bytes| bytemuck::checked::try_from_bytes::<ReplyHeader>(bytes).ok())
        .ok_or(PartitionWriteError::Rejected(IggyError::InvalidCommand))?;
    if header.command != Command2::Reply {
        return Err(PartitionWriteError::Rejected(IggyError::InvalidCommand));
    }
    if header.op == 0 {
        return Err(PartitionWriteError::NotFound);
    }
    Ok(())
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
/// authenticated but unprivileged -> 403. A linearizable read must come from the
/// primary; on a follower it redirects (307) to the primary's HTTP address when
/// resolvable, else fails closed to a 503 (see
/// [`HttpInner::not_primary_read_error`]).
fn authorize_read(
    state: &HttpInner,
    identity: &Identity,
    consistency: Consistency,
) -> Result<(), ReadError> {
    if identity.user_id != DEFAULT_ROOT_USER_ID {
        return Err(ReadError::Rejected(IggyError::Unauthorized));
    }
    if consistency == Consistency::Linearizable && !state.is_metadata_primary() {
        return Err(state.not_primary_read_error(&identity.path_and_query));
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
    identity: &Identity,
    consistency: Consistency,
    code: u32,
    body: &[u8],
) -> Result<Bytes, ReadError> {
    authorize_read(state, identity, consistency)?;
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

#[cfg(test)]
mod tests {
    use axum::http::Uri;
    use configs::cluster::TransportPorts;
    use iggy_binary_protocol::PrepareHeader;
    use iggy_common::{
        Consumer, ConsumerKind, IggyMessage, IggyMessagesBatch, Partitioning, PartitioningKind,
        PollingKind, PollingStrategy,
    };
    use partitions::{Fragment, PollFragments};
    use server_common::MESSAGE_ALIGN;
    use server_common::iobuf::Owned;
    use server_common::send_messages2::{
        COMMAND_HEADER_SIZE, IggyMessage2, IggyMessage2Header, IggyMessages2, SendMessages2Header,
        SendMessages2Owned,
    };

    use super::*;
    use crate::responses::build_empty_reply;

    const READ_PATH: &str = "/streams?consistency=linearizable";

    fn node(replica_id: u8, ip: &str, http: Option<u16>) -> ClusterNodeConfig {
        ClusterNodeConfig {
            name: format!("node-{replica_id}"),
            ip: ip.to_owned(),
            replica_id,
            ports: TransportPorts {
                tcp: None,
                quic: None,
                http,
                websocket: None,
                tcp_replica: None,
            },
        }
    }

    fn roster(nodes: Vec<ClusterNodeConfig>) -> ClusterRoster {
        ClusterRoster {
            enabled: true,
            name: "test-cluster".to_owned(),
            nodes,
            http_addr: "127.0.0.1:3000".parse().expect("valid socket addr"),
        }
    }

    #[test]
    fn primary_redirect_location_targets_primary_http_addr_with_path_passthrough() {
        let roster = roster(vec![
            node(0, "10.0.0.1", Some(8080)),
            node(1, "10.0.0.2", Some(8090)),
        ]);
        assert_eq!(
            primary_redirect_location(&roster, 1, READ_PATH),
            Some("http://10.0.0.2:8090/streams?consistency=linearizable".to_owned())
        );
    }

    #[test]
    fn primary_redirect_location_is_none_when_no_node_matches_primary_index() {
        let roster = roster(vec![node(0, "10.0.0.1", Some(8080))]);
        assert_eq!(primary_redirect_location(&roster, 2, READ_PATH), None);
    }

    #[test]
    fn primary_redirect_location_is_none_when_primary_has_no_http_port() {
        let roster = roster(vec![node(0, "10.0.0.1", None)]);
        assert_eq!(primary_redirect_location(&roster, 0, READ_PATH), None);
    }

    #[test]
    fn primary_redirect_location_is_none_for_empty_roster() {
        let roster = roster(Vec::new());
        assert_eq!(primary_redirect_location(&roster, 0, READ_PATH), None);
    }

    #[test]
    fn primary_redirect_location_brackets_ipv6_host() {
        let roster = roster(vec![node(0, "::1", Some(8080))]);
        assert_eq!(
            primary_redirect_location(&roster, 0, READ_PATH),
            Some("http://[::1]:8080/streams?consistency=linearizable".to_owned())
        );
    }

    // -- encode_send_messages --

    fn produce_command(partitioning: Partitioning) -> SendMessages {
        let first = IggyMessage::builder()
            .id(7)
            .payload(Bytes::from_static(b"first"))
            .build()
            .expect("valid message");
        // Raw pre-encoded user headers, mirroring the HTTP deserializer's
        // base64 branch.
        let mut second = IggyMessage::builder()
            .id(8)
            .payload(Bytes::from_static(b"second"))
            .build()
            .expect("valid message");
        let raw_headers = Bytes::from_static(b"raw-header-bytes");
        second.header.user_headers_length =
            u32::try_from(raw_headers.len()).expect("test headers fit u32");
        second.user_headers = Some(raw_headers);
        let messages = vec![first, second];
        SendMessages {
            partitioning,
            batch: IggyMessagesBatch::from(&messages),
            ..Default::default()
        }
    }

    #[test]
    fn encode_send_messages_round_trips_through_wire_decoders() {
        use iggy_binary_protocol::WireMessageIterator;
        use iggy_binary_protocol::message_layout::WIRE_MESSAGE_INDEX_SIZE;
        use iggy_binary_protocol::requests::messages::SendMessagesHeader;

        let stream_id = Identifier::from_str_value("1").expect("valid stream id");
        let topic_id = Identifier::from_str_value("orders").expect("valid topic id");
        let command = produce_command(Partitioning::partition_id(3));
        let origin_timestamps: Vec<u64> = command
            .batch
            .iter()
            .map(|view| view.header().origin_timestamp())
            .collect();

        let bytes = encode_send_messages(&stream_id, &topic_id, &command).expect("encodes");

        let metadata_length =
            u32::from_le_bytes(bytes[..4].try_into().expect("length prefix")) as usize;
        let (header, consumed) =
            SendMessagesHeader::decode(&bytes[4..4 + metadata_length]).expect("valid metadata");
        assert_eq!(consumed, metadata_length);
        assert_eq!(header.stream_id, identifier_to_wire(&stream_id).unwrap());
        assert_eq!(header.topic_id, identifier_to_wire(&topic_id).unwrap());
        assert_eq!(
            header.partitioning,
            partitioning_to_wire(&command.partitioning).unwrap()
        );
        assert_eq!(header.messages_count, 2);

        let data_offset = 4 + metadata_length + 2 * WIRE_MESSAGE_INDEX_SIZE;
        let views: Vec<_> = WireMessageIterator::new(&bytes[data_offset..], 2)
            .collect::<Result<Vec<_>, _>>()
            .expect("valid message frames");
        assert_eq!(views[0].id(), 7);
        assert_eq!(views[0].payload(), b"first");
        assert_eq!(views[0].user_headers(), b"");
        assert_eq!(views[0].origin_timestamp(), origin_timestamps[0]);
        assert_eq!(views[1].id(), 8);
        assert_eq!(views[1].payload(), b"second");
        assert_eq!(views[1].user_headers(), b"raw-header-bytes");
        assert_eq!(views[1].origin_timestamp(), origin_timestamps[1]);
    }

    #[test]
    fn send_messages_validate_rejects_oversized_partitioning_key() {
        let command = produce_command(Partitioning {
            kind: PartitioningKind::MessagesKey,
            length: 0,
            value: vec![0u8; 256],
        });
        assert!(command.validate().is_err());
    }

    // -- poll / consumer-offset query parsing and wire mapping --

    #[test]
    fn poll_query_parses_with_documented_defaults() {
        let uri: Uri = "/streams/1/topics/1/messages?consumer_id=42"
            .parse()
            .expect("valid uri");
        let Query(query) = Query::<PollMessages>::try_from_uri(&uri).expect("parses");
        assert_eq!(query.consumer.kind, ConsumerKind::Consumer);
        assert_eq!(
            query.consumer.id,
            Identifier::numeric(42).expect("valid id")
        );
        assert_eq!(query.partition_id, Some(0));
        assert_eq!(query.strategy, PollingStrategy::offset(0));
        assert_eq!(query.count, 10);
        assert!(!query.auto_commit);
    }

    #[test]
    fn poll_query_parses_explicit_strategy_count_and_tolerates_consistency_param() {
        let uri: Uri = "/x?consumer_id=app&partition_id=3&kind=timestamp&value=42&count=5&auto_commit=true&consistency=linearizable"
            .parse()
            .expect("valid uri");
        let Query(query) = Query::<PollMessages>::try_from_uri(&uri).expect("parses");
        assert_eq!(
            query.consumer.id,
            Identifier::named("app").expect("valid id")
        );
        assert_eq!(query.partition_id, Some(3));
        assert_eq!(
            query.strategy,
            PollingStrategy::timestamp(IggyTimestamp::from(42))
        );
        assert_eq!(query.count, 5);
        assert!(query.auto_commit);
        let Query(consistency) = Query::<ConsistencyQuery>::try_from_uri(&uri).expect("parses");
        assert!(consistency.consistency == Consistency::Linearizable);
    }

    #[test]
    fn poll_wire_request_maps_query_onto_tcp_request_shape() {
        let stream_id = Identifier::from_str_value("orders").expect("valid stream id");
        let topic_id = Identifier::from_str_value("1").expect("valid topic id");
        let query = PollMessages {
            consumer: Consumer {
                kind: ConsumerKind::Consumer,
                id: Identifier::numeric(9).expect("valid id"),
            },
            partition_id: Some(4),
            strategy: PollingStrategy::next(),
            count: 25,
            auto_commit: true,
            ..Default::default()
        };
        let wire = poll_wire_request(&stream_id, &topic_id, &query).expect("maps");
        assert_eq!(wire.consumer.kind, 1);
        assert_eq!(
            wire.consumer.id,
            identifier_to_wire(&query.consumer.id).unwrap()
        );
        assert_eq!(wire.stream_id, identifier_to_wire(&stream_id).unwrap());
        assert_eq!(wire.topic_id, identifier_to_wire(&topic_id).unwrap());
        assert_eq!(wire.partition_id, Some(4));
        assert_eq!(wire.strategy.kind, PollingKind::Next.as_code());
        assert_eq!(wire.strategy.value, 0);
        assert_eq!(wire.count, 25);
        assert!(wire.auto_commit);
    }

    #[test]
    fn consumer_offset_wire_request_defaults_omitted_partition_to_zero() {
        let stream_id = Identifier::numeric(1).expect("valid stream id");
        let topic_id = Identifier::numeric(1).expect("valid topic id");
        let query = GetConsumerOffset {
            consumer: Consumer::new(Identifier::numeric(7).expect("valid id")),
            partition_id: None,
        };
        let wire = consumer_offset_wire_request(&stream_id, &topic_id, &query).expect("maps");
        assert_eq!(wire.partition_id, Some(DEFAULT_PARTITION_ID));
        assert_eq!(wire.consumer.kind, 1);
    }

    // -- consumer-offset write wire mapping --

    #[test]
    fn store_offset_wire_request_round_trips_with_quorum_ack() {
        let stream_id = Identifier::numeric(1).expect("valid stream id");
        let topic_id = Identifier::named("orders").expect("valid topic id");
        let command = StoreConsumerOffset {
            consumer: Consumer::new(Identifier::named("c1").expect("valid id")),
            partition_id: Some(1),
            offset: 42,
        };
        let request = store_offset_wire_request(&stream_id, &topic_id, &command).expect("maps");
        let bytes = request.to_bytes();
        assert_eq!(*bytes.last().expect("non-empty"), AckLevel::Quorum.as_u8());
        let (decoded, consumed) =
            StoreConsumerOffset2Request::decode(&bytes).expect("decodes as the server does");
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, request);
        assert_eq!(decoded.consumer.kind, 1);
        assert_eq!(decoded.partition_id, Some(1));
        assert_eq!(decoded.offset, 42);
        assert_eq!(decoded.ack, AckLevel::Quorum);
    }

    #[test]
    fn store_offset_wire_request_passes_omitted_partition_through() {
        let stream_id = Identifier::numeric(1).expect("valid stream id");
        let topic_id = Identifier::numeric(2).expect("valid topic id");
        let command = StoreConsumerOffset {
            consumer: Consumer::new(Identifier::numeric(7).expect("valid id")),
            partition_id: None,
            offset: u64::MAX,
        };
        let request = store_offset_wire_request(&stream_id, &topic_id, &command).expect("maps");
        let bytes = request.to_bytes();
        let (decoded, consumed) =
            StoreConsumerOffset2Request::decode(&bytes).expect("decodes as the server does");
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded.partition_id, None);
        assert_eq!(decoded.offset, u64::MAX);
        assert_eq!(decoded.ack, AckLevel::Quorum);
    }

    #[test]
    fn delete_offset_wire_request_round_trips_partition_variants() {
        let stream_id = Identifier::named("stream-1").expect("valid stream id");
        let topic_id = Identifier::numeric(2).expect("valid topic id");
        for partition_id in [Some(1), None] {
            let request = delete_offset_wire_request(
                &stream_id,
                &topic_id,
                &Consumer::new(Identifier::named("c1").expect("valid id")),
                partition_id,
            )
            .expect("maps");
            let bytes = request.to_bytes();
            assert_eq!(*bytes.last().expect("non-empty"), AckLevel::Quorum.as_u8());
            let (decoded, consumed) =
                DeleteConsumerOffset2Request::decode(&bytes).expect("decodes as the server does");
            assert_eq!(consumed, bytes.len());
            assert_eq!(decoded, request);
            assert_eq!(decoded.consumer.kind, 1);
            assert_eq!(decoded.partition_id, partition_id);
            assert_eq!(decoded.ack, AckLevel::Quorum);
        }
    }

    #[test]
    fn delete_offset_query_parses_optional_partition_id() {
        let uri: Uri = "/x?partition_id=3".parse().expect("valid uri");
        let Query(query) = Query::<DeleteConsumerOffset>::try_from_uri(&uri).expect("parses");
        assert_eq!(query.partition_id, Some(3));
        let bare: Uri = "/x".parse().expect("valid uri");
        let Query(query) = Query::<DeleteConsumerOffset>::try_from_uri(&bare).expect("parses");
        assert_eq!(query.partition_id, None);
    }

    // -- polled messages body decode --

    /// Wrap one stored `SendMessages2` batch (`[256B header][blob]`) as the
    /// poll fragment the owning shard replies, the shape
    /// `build_polled_messages_body` consumes.
    fn fragment_from_stored_batch(header: &SendMessages2Header, blob: &[u8]) -> PollFragments {
        let mut buffer = Owned::<MESSAGE_ALIGN>::zeroed(COMMAND_HEADER_SIZE + blob.len());
        header.encode_into(buffer.as_mut_slice());
        buffer.as_mut_slice()[COMMAND_HEADER_SIZE..].copy_from_slice(blob);
        let mut fragments = PollFragments::new();
        fragments.push(Fragment::whole(buffer.into()));
        fragments
    }

    /// Round-trip the poll route's encode/decode seam: the store's own batch
    /// writer (`SendMessages2Owned::from_messages`) is the encoder oracle,
    /// `build_polled_messages_body` re-encodes to the legacy wire body, and
    /// the SDK's `PolledMessages::from_bytes` must read back every field.
    #[test]
    fn polled_messages_body_decodes_into_common_polled_messages() {
        let mut messages = IggyMessages2::with_capacity(2);
        messages.push(IggyMessage2 {
            header: IggyMessage2Header {
                id: 7,
                origin_timestamp: 1_000,
                ..Default::default()
            },
            payload: Bytes::from_static(b"first"),
            user_headers: None,
        });
        messages.push(IggyMessage2 {
            header: IggyMessage2Header {
                id: 8,
                origin_timestamp: 1_050,
                ..Default::default()
            },
            payload: Bytes::from_static(b"second"),
            user_headers: Some(Bytes::from_static(b"raw-header-bytes")),
        });
        let namespace = server_common::sharding::IggyNamespace::new(0, 0, 3);
        let stored =
            SendMessages2Owned::from_messages(namespace, &messages).expect("encodes stored batch");
        // The store stamps these on append; `from_messages` leaves them zero.
        let mut header = stored.header;
        header.base_offset = 41;
        header.base_timestamp = 999_999;

        let body =
            build_polled_messages_body(3, 42, fragment_from_stored_batch(&header, &stored.blob))
                .expect("re-encodes wire body");
        let polled = PolledMessages::from_bytes(body).expect("decodes as the SDK does");

        assert_eq!(polled.partition_id, 3);
        assert_eq!(polled.current_offset, 42);
        assert_eq!(polled.count, 2);
        assert_eq!(polled.messages.len(), 2);
        assert_eq!(polled.messages[0].header.id, 7);
        assert_eq!(polled.messages[0].header.offset, 41);
        assert_eq!(polled.messages[0].header.timestamp, 999_999);
        assert_eq!(polled.messages[0].header.origin_timestamp, 1_000);
        assert_eq!(polled.messages[0].payload.as_ref(), b"first");
        assert!(polled.messages[0].user_headers.is_none());
        assert_eq!(polled.messages[1].header.id, 8);
        assert_eq!(polled.messages[1].header.offset, 42);
        assert_eq!(polled.messages[1].header.origin_timestamp, 1_050);
        assert_eq!(polled.messages[1].payload.as_ref(), b"second");
        assert_eq!(
            polled.messages[1].user_headers.as_deref(),
            Some(b"raw-header-bytes".as_ref())
        );
    }

    #[test]
    fn resync_required_polled_messages_carries_sentinel_partition() {
        let polled = resync_required_polled_messages();
        assert_eq!(polled.partition_id, RESYNC_REQUIRED_PARTITION_SENTINEL);
        assert_eq!(polled.count, 0);
        assert!(polled.messages.is_empty());
    }

    // -- classify_partition_reply --

    fn frozen(reply: Message<iggy_binary_protocol::ReplyHeader>) -> BusMessage {
        reply.into_generic().into_frozen()
    }

    #[test]
    fn committed_partition_reply_classifies_as_success() {
        let prepare = PrepareHeader {
            command: Command2::Prepare,
            operation: Operation::SendMessages,
            client: 42,
            op: 1,
            request: 1,
            ..Default::default()
        };
        let reply = frozen(consensus::build_reply_message(&prepare, &Bytes::new()));
        assert!(classify_partition_reply(&reply).is_ok());
    }

    /// The 204 path of the offset write routes: a committed
    /// `StoreConsumerOffset2` reply (op >= 1) classifies as success.
    #[test]
    fn committed_offset_write_reply_classifies_as_success() {
        let prepare = PrepareHeader {
            command: Command2::Prepare,
            operation: Operation::StoreConsumerOffset2,
            client: 42,
            op: 3,
            request: 2,
            ..Default::default()
        };
        let reply = frozen(consensus::build_reply_message(&prepare, &Bytes::new()));
        assert!(classify_partition_reply(&reply).is_ok());
    }

    #[test]
    fn gate_failure_empty_reply_classifies_as_not_found() {
        let request = build_request_message(Operation::SendMessages, 42, 7, 1, &[]);
        let reply = frozen(build_empty_reply(request.header(), 42, 0, 9));
        assert!(matches!(
            classify_partition_reply(&reply),
            Err(PartitionWriteError::NotFound)
        ));
    }

    #[test]
    fn non_reply_frame_classifies_as_rejected() {
        let request = build_request_message(Operation::SendMessages, 42, 7, 1, &[]);
        assert!(matches!(
            classify_partition_reply(&request.into_generic().into_frozen()),
            Err(PartitionWriteError::Rejected(IggyError::InvalidCommand))
        ));
    }

    #[test]
    fn in_flight_write_guard_decrements_both_counters_on_drop() {
        let session = Cell::new(0);
        let global = Cell::new(0);
        let guard = admit_partition_write(&session, &global).expect("below both caps");
        assert_eq!(session.get(), 1);
        assert_eq!(global.get(), 1);
        drop(guard);
        assert_eq!(session.get(), 0);
        assert_eq!(global.get(), 0);
    }

    #[test]
    fn admission_at_session_cap_rejects_with_too_many_in_flight() {
        let session = Cell::new(MAX_IN_FLIGHT_WRITES_PER_SESSION);
        let global = Cell::new(0);
        assert!(matches!(
            admit_partition_write(&session, &global),
            Err(PartitionWriteError::TooManyInFlight)
        ));
        // A refusal must not leak a partial increment on either counter.
        assert_eq!(session.get(), MAX_IN_FLIGHT_WRITES_PER_SESSION);
        assert_eq!(global.get(), 0);
    }

    #[test]
    fn admission_at_global_budget_rejects_with_server_busy() {
        let session = Cell::new(0);
        let global = Cell::new(MAX_IN_FLIGHT_WRITES_GLOBAL);
        assert!(matches!(
            admit_partition_write(&session, &global),
            Err(PartitionWriteError::ServerBusy)
        ));
        assert_eq!(session.get(), 0);
        assert_eq!(global.get(), MAX_IN_FLIGHT_WRITES_GLOBAL);
    }

    #[test]
    fn interleaved_admission_reopens_exactly_released_session_slots() {
        let session = Cell::new(0);
        let global = Cell::new(0);
        let mut guards = Vec::new();
        for _ in 0..MAX_IN_FLIGHT_WRITES_PER_SESSION {
            guards.push(admit_partition_write(&session, &global).expect("below both caps"));
        }
        assert!(matches!(
            admit_partition_write(&session, &global),
            Err(PartitionWriteError::TooManyInFlight)
        ));
        let released = 3;
        guards.truncate((MAX_IN_FLIGHT_WRITES_PER_SESSION - released) as usize);
        assert_eq!(session.get(), MAX_IN_FLIGHT_WRITES_PER_SESSION - released);
        for _ in 0..released {
            guards.push(admit_partition_write(&session, &global).expect("released slots"));
        }
        assert!(matches!(
            admit_partition_write(&session, &global),
            Err(PartitionWriteError::TooManyInFlight)
        ));
        drop(guards);
        assert_eq!(session.get(), 0);
        assert_eq!(global.get(), 0);
    }

    #[test]
    fn global_budget_spans_sessions_and_reopens_after_release() {
        let global = Cell::new(0);
        let session_count =
            MAX_IN_FLIGHT_WRITES_GLOBAL.div_ceil(MAX_IN_FLIGHT_WRITES_PER_SESSION) as usize;
        let sessions: Vec<Cell<u32>> = (0..session_count).map(|_| Cell::new(0)).collect();
        let mut guards = Vec::new();
        'fill: for session in &sessions {
            for _ in 0..MAX_IN_FLIGHT_WRITES_PER_SESSION {
                match admit_partition_write(session, &global) {
                    Ok(guard) => guards.push(guard),
                    Err(PartitionWriteError::ServerBusy) => break 'fill,
                    Err(other) => {
                        panic!("only the global budget may refuse this fill, got {other:?}")
                    }
                }
            }
        }
        assert_eq!(global.get(), MAX_IN_FLIGHT_WRITES_GLOBAL);
        // A fresh session is refused on the shared budget, not its own cap.
        let fresh = Cell::new(0);
        assert!(matches!(
            admit_partition_write(&fresh, &global),
            Err(PartitionWriteError::ServerBusy)
        ));
        drop(guards.pop());
        let readmitted = admit_partition_write(&fresh, &global).expect("budget slot released");
        assert_eq!(fresh.get(), 1);
        assert_eq!(global.get(), MAX_IN_FLIGHT_WRITES_GLOBAL);
        drop(readmitted);
    }
}
