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

//! HTTP rejection types and hand-built error responses: the auth / write /
//! read / partition-write error enums, their `IntoResponse` renderings, the
//! `?consistency=` and `?ack=` query DTOs, and the primary-redirect helpers.

use std::net::{IpAddr, SocketAddr};

use axum::Json;
use axum::http::header::{LOCATION, RETRY_AFTER};
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use iggy_binary_protocol::Operation;
use iggy_common::IggyError;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::error;

use crate::cluster_meta::ClusterRoster;

#[derive(Debug, Error)]
pub(in crate::http) enum CustomError {
    #[error(transparent)]
    Error(#[from] IggyError),
    #[error("Resource not found")]
    ResourceNotFound,
}

#[derive(Debug, Serialize)]
pub(in crate::http) struct ErrorResponse {
    /// Two conventions by construction: the `IggyError` numeric code
    /// (`IggyError::as_code`) when the error wraps one (via [`Self::from_error`]),
    /// or the HTTP status code for the hand-built HTTP-layer errors (429/503/504
    /// and the 404 not-found fallback) that carry no underlying `IggyError`.
    pub id: u32,
    pub code: String,
    pub reason: String,
    pub field: Option<String>,
}

impl IntoResponse for CustomError {
    fn into_response(self) -> Response {
        match self {
            Self::Error(error) => {
                error!("There was an error: {error}");
                let status_code = match error {
                    IggyError::StreamIdNotFound(_)
                    | IggyError::TopicIdNotFound(_, _)
                    | IggyError::PartitionNotFound(_, _, _)
                    | IggyError::SegmentNotFound
                    | IggyError::ClientNotFound(_)
                    | IggyError::ConsumerGroupIdNotFound(_, _)
                    | IggyError::ConsumerGroupNameNotFound(_, _)
                    | IggyError::ConsumerGroupMemberNotFound(_, _, _)
                    | IggyError::ConsumerOffsetNotFound(_)
                    | IggyError::ResourceNotFound(_) => StatusCode::NOT_FOUND,
                    IggyError::Unauthenticated
                    | IggyError::AccessTokenMissing
                    | IggyError::InvalidAccessToken
                    | IggyError::InvalidPersonalAccessToken => StatusCode::UNAUTHORIZED,
                    IggyError::Unauthorized => StatusCode::FORBIDDEN,
                    // The pre-consensus retry frame: reaching this render
                    // means the write path's replay budget is exhausted and
                    // the op never committed - a transient server condition,
                    // retryable like the other cannot-commit-right-now 503s
                    // (see `service_unavailable`), never a caller error.
                    IggyError::TransientNotCommitted | IggyError::TransientNotAccepted => {
                        StatusCode::SERVICE_UNAVAILABLE
                    }
                    _ => StatusCode::BAD_REQUEST,
                };
                (status_code, Json(ErrorResponse::from_error(&error)))
            }
            Self::ResourceNotFound => (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    id: 404,
                    code: "not_found".to_string(),
                    reason: "Resource not found".to_string(),
                    field: None,
                }),
            ),
        }
        .into_response()
    }
}

impl ErrorResponse {
    pub fn from_error(error: &IggyError) -> Self {
        Self {
            id: error.as_code(),
            code: error.as_string().to_string(),
            reason: error.to_string(),
            field: match error {
                IggyError::StreamIdNotFound(_) | IggyError::InvalidStreamId => {
                    Some("stream_id".to_string())
                }
                IggyError::TopicIdNotFound(_, _) | IggyError::InvalidTopicId => {
                    Some("topic_id".to_string())
                }
                IggyError::PartitionNotFound(_, _, _) => Some("partition_id".to_string()),
                IggyError::SegmentNotFound => Some("segment_id".to_string()),
                IggyError::ClientNotFound(_) => Some("client_id".to_string()),
                IggyError::InvalidStreamName
                | IggyError::StreamNameAlreadyExists(_)
                | IggyError::InvalidTopicName
                | IggyError::TopicNameAlreadyExists(_, _)
                | IggyError::ConsumerGroupNameAlreadyExists(_, _)
                | IggyError::PersonalAccessTokenAlreadyExists(_, _) => Some("name".to_string()),
                IggyError::InvalidOffset(_) => Some("offset".to_string()),
                IggyError::InvalidConsumerGroupId => Some("consumer_group_id".to_string()),
                IggyError::UserAlreadyExists => Some("username".to_string()),
                _ => None,
            },
        }
    }
}

/// Rejection for protected routes.
///
/// Two failure classes get two statuses: a missing, invalid, or expired
/// credential is the caller's fault (401, rendered as the JSON `ErrorResponse`
/// body every other route error uses), while a VSR session that cannot be
/// established right now is a transient server condition (503) and must never
/// masquerade as an auth failure.
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
            // Render 401 through the shared `IggyError -> CustomError` map so it
            // carries the same JSON `ErrorResponse` body as every other ng error.
            // The legacy server's protected-route 401 comes from a bare-status
            // JWT middleware (empty body), so this is deliberately richer, not
            // byte-identical to legacy.
            Self::Unauthenticated(error) => CustomError::from(error).into_response(),
            // A fresh session could not be established: the Register did not
            // commit (no caught-up primary, pipeline full, or a view-change
            // cancel), or the session table is at `MAX_HTTP_SESSIONS` and
            // refused the fresh registration. Transient server condition -> 503,
            // retryable.
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
pub(in crate::http) enum WriteError {
    Rejected(IggyError),
    /// The VSR session was evicted (its client slot was reclaimed cluster-side,
    /// e.g. LRU-evicted from the full client table). Renders identically to a
    /// terminal `Rejected` (401 -> re-authenticate), but is a distinct variant
    /// so the submit path can drop the dead session entry and let the caller's
    /// next request re-register cleanly instead of 401-looping on it.
    Evicted(IggyError),
    Unavailable,
}

impl IntoResponse for WriteError {
    fn into_response(self) -> Response {
        match self {
            Self::Rejected(error) | Self::Evicted(error) => {
                CustomError::from(error).into_response()
            }
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
pub(in crate::http) enum PartitionWriteError {
    /// Caller-side rejection (bad identifier, oversized batch, an authorization
    /// denial), a typed pre-commit deny from the partition plane
    /// (`ReplyHeader.status`), or a malformed reply frame, rendered through the
    /// legacy `IggyError -> status` map for SDK-identical bodies.
    Rejected(IggyError),
    /// The dispatch gates could not route the write: the stream, topic, or
    /// partition does not resolve (or never materialised within the routable
    /// budget). Rendered as the legacy 404 body.
    NotFound,
    /// The in-process reply slot could not be installed. Transient server
    /// condition -> the shared 503, retryable.
    Unavailable,
    /// This session is already at its `http_admission.max_in_flight_writes_per_session`
    /// awaited writes. 429: the caller's own concurrency is the problem, so
    /// it must drain its outstanding writes before submitting more.
    TooManyInFlight,
    /// Shard 0 is already at its `http_admission.max_in_flight_writes` awaited
    /// writes across all sessions. 503 with its own code (distinct from the
    /// shared consensus-unavailable body) so an operator can tell admission
    /// shedding from a consensus outage.
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

/// Advisory `Retry-After` seconds for the shed / transient 429 and 503
/// responses. One second: admission shedding, a briefly unavailable consensus
/// group, and a linearizable-read-on-follower all typically clear well within
/// it, and a small hint keeps a backing-off client responsive.
const RETRY_AFTER_SECONDS: u64 = 1;

/// Attach the advisory [`RETRY_AFTER_SECONDS`] hint to a retryable 429/503.
fn with_retry_after(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(RETRY_AFTER, HeaderValue::from(RETRY_AFTER_SECONDS));
    response
}

/// Render an `ErrorResponse` body for `status`, tagged with `code` / `reason`
/// and no field, so every hand-built HTTP error the routes return parses as the
/// one error schema clients already handle.
fn error_response(status: StatusCode, code: &str, reason: &str) -> Response {
    (
        status,
        Json(ErrorResponse {
            id: status.as_u16().into(),
            code: code.to_owned(),
            reason: reason.to_owned(),
            field: None,
        }),
    )
        .into_response()
}

/// Shared 504 rendering for an in-band request the partition plane did not
/// answer in time, shaped like every other HTTP error (`ErrorResponse`) so
/// clients parse one error schema. Consumed by the partition-write reply wait
/// and the partition reads ([`ReadError::Timeout`]).
fn gateway_timeout_response(code: &str, reason: &str) -> Response {
    error_response(StatusCode::GATEWAY_TIMEOUT, code, reason)
}

/// The shared 503 body for a request that could not commit right now: no
/// caught-up primary, a full pipeline, or a view-change cancel. Retryable, and
/// rendered with the `CannotEstablishConnection` code the SDKs treat as a
/// connection-level retry rather than a terminal error.
fn service_unavailable() -> Response {
    with_retry_after(
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse::from_error(
                &IggyError::CannotEstablishConnection,
            )),
        )
            .into_response(),
    )
}

/// 429 for a session at its `http_admission.max_in_flight_writes_per_session`
/// awaited partition writes. Shaped like every other HTTP error
/// (`ErrorResponse`) so clients parse one error schema; the remedy is the
/// caller's own: let outstanding writes finish, then retry.
fn too_many_in_flight_response() -> Response {
    with_retry_after(error_response(
        StatusCode::TOO_MANY_REQUESTS,
        "too_many_in_flight_writes",
        "session reached its in-flight write cap; await outstanding writes and retry",
    ))
}

/// 503 for shard 0 at its `http_admission.max_in_flight_writes` awaited partition
/// writes across all sessions. A distinct `server_busy` code (unlike the shared
/// consensus-unavailable 503) so admission shedding is tellable from a
/// consensus outage; retry with backoff.
fn server_busy_response() -> Response {
    with_retry_after(error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "server_busy",
        "shard is at its in-flight write budget; retry with backoff",
    ))
}

/// Read consistency selected by the `?consistency=` query param.
///
/// `serializable` (the default) serves from this node's local metadata STM:
/// correct and consensus-free, but may trail the primary by the replication
/// delay. `linearizable` demands the freshest committed state and is honored
/// only on the primary; a follower redirects (307) to the primary when its HTTP
/// address resolves from the roster, else fails closed to 503 (see
/// [`read_local`]).
#[derive(Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(in crate::http) enum Consistency {
    #[default]
    Serializable,
    Linearizable,
}

/// `?consistency=` query wrapper. An absent param defaults to
/// [`Consistency::Serializable`]; an unrecognized value is a 400 (axum `Query`).
#[derive(Default, Deserialize)]
pub(in crate::http) struct ConsistencyQuery {
    #[serde(default)]
    pub(in crate::http) consistency: Consistency,
}

/// Produce acknowledgement selected by the `?ack=` query param.
///
/// `replicated` (the default) answers 201 only after the partition group's
/// quorum commit. `none` is fire-and-forget: the request is validated,
/// dispatched, and answered 202 immediately; the commit still happens, but its
/// reply is shed at the bus (no reply slot is installed).
#[derive(Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(in crate::http) enum ProduceAck {
    #[default]
    Replicated,
    None,
}

/// `?ack=` query wrapper. An absent param defaults to
/// [`ProduceAck::Replicated`]; an unrecognized value is a 400 (axum `Query`).
#[derive(Default, Deserialize)]
pub(in crate::http) struct ProduceQuery {
    #[serde(default)]
    pub(in crate::http) ack: ProduceAck,
}

/// Rejection for an authenticated read route (`GET /streams`,
/// `GET /streams/{id}`, and the reads that follow).
pub(in crate::http) enum ReadError {
    /// Caller-side or STM rejection (bad identifier, unsupported op, or an
    /// authorization denial) graded through the legacy `IggyError -> status`
    /// map so SDK error bodies stay byte-identical.
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
    with_retry_after(error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "not_primary",
        "linearizable read requires the primary; retry against the leader",
    ))
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
pub(in crate::http) fn primary_redirect_location(
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

#[cfg(test)]
mod tests {
    use super::*;

    use configs::ng_cluster::{ClusterNodeConfig, TransportPorts};

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
            self_ip: "127.0.0.1".to_owned(),
            self_ports: TransportPorts::default(),
            metadata_view: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(
                crate::cluster_meta::METADATA_VIEW_UNKNOWN,
            )),
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
}
