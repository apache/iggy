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

//! Follower-side forwarding of consensus-needing requests to the VSR
//! primary that serves them.
//!
//! A load balancer that round-robins HTTP across the cluster lands writes and
//! linearizable reads on followers, which cannot serve them: a follower can
//! neither Register a VSR session nor commit a control-plane op. Instead of
//! failing those requests with a transient 503, the middleware here re-issues
//! them against the current primary's HTTP listener and relays the primary's
//! response on the original connection, so any node answers any request.
//!
//! Control-plane routes share the metadata primary as their forward target.
//! Partition routes find the primary of their partition before dispatch. The
//! contacted node chooses the partition and its context once, and relays the
//! request with both in the `iggy-partition-route` header when another replica
//! is the primary. The hop target serves exactly that partition or refuses
//! with `NotPrimary`, which proves nothing was admitted, and never relays.
//!
//! Safety model, in order:
//! - The bearer is verified locally (verify-only, no session mint) before any
//!   bytes leave this node, so an unauthenticated caller cannot make a
//!   follower relay junk to the primary. A PAT is checked against this node's
//!   local metadata view, so a PAT minted on the primary and not yet
//!   replicated here answers 401 until replication catches up - fail-closed
//!   on purpose, the price of keeping the pre-forward auth gate.
//! - The primary re-authenticates and re-authorizes the forwarded request
//!   through its ordinary extractor stack; the forward marker header is a loop
//!   guard only and never a trust input.
//! - The route header, like the marker, carries only values a caller can send
//!   itself, so the hop target authenticates and authorizes the request again.
//! - A forwarded write is retried only when it provably never entered the
//!   primary's pipeline: a connect-phase failure, and for a metadata relay a
//!   503 whose body carries the `TransientNotAccepted` or `NotPrimary` code or
//!   a 307 from a stale target, for a partition relay a 503 carrying
//!   `NotPrimary`. Every other outcome, including `TransientNotCommitted`, is
//!   relayed as-is: its op may still commit, and re-issuing it under a fresh
//!   session would defeat the consensus dedup and double-apply the op.
//! - A partition read changes no state, so a transport failure after its send
//!   is retried too: the resend cannot apply anything twice.
//! - The target is always resolved from the local roster + consensus view,
//!   never from a response `Location`, and the client follows no redirects.

use std::cell::Cell;
use std::fmt;
use std::net::SocketAddr;
use std::path::Path;
use std::rc::Rc;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::{Body, to_bytes};
use axum::extract::{Request, State};
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE, RETRY_AFTER};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use configs::http::HttpTlsConfig;
use consensus::MetadataHandle;
use futures::{Stream, StreamExt};
use iggy_binary_protocol::primitives::partition_history::PartitionContext;
use iggy_common::IggyError;
use message_bus::transports::tls::load_pem;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, WebPkiSupportedAlgorithms};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{CertificateError, DigitallySignedStruct, SignatureScheme};
use send_wrapper::SendWrapper;
use serde::Deserialize;
use server_common::sharding::IggyNamespace;
use shard::{PartitionRead, PartitionReadReply};
use tracing::{debug, warn};

use crate::dispatch::reads::hold_for_frontier;
use crate::http::HttpState;
use crate::http::error::{
    CustomError, error_response, gateway_timeout_response, primary_http_socket, with_retry_after,
};
use crate::http::extractor::{bearer_token, resolve_credential};
use crate::http::state::{
    APPLIED_OP_HEADER, DURABILITY_HEADER, ForwardState, HttpInner, VIEW_HEADER,
};
use crate::server_error::ServerError;

/// Marker stamped on every forwarded request. Loop guard only: a node that is
/// not primary and sees it answers the transient 503 instead of forwarding
/// again, so a stale view can never chain hops. It is client-spoofable by
/// design - spoofing it at a follower is a self-inflicted 503, and the primary
/// ignores it - and it must never gate auth, authz, or admission.
const FORWARDED_HEADER: HeaderName = HeaderName::from_static("iggy-forwarded");
const FORWARDED_VALUE: HeaderValue = HeaderValue::from_static("1");

/// The relay inputs a contacted node chose for a partition request, as
/// `<partition_id>,<incarnation>,<owner_generation>,<metadata_op>,<budget_ms>`.
const PARTITION_ROUTE_HEADER: HeaderName = HeaderName::from_static("iggy-partition-route");

/// One bound for a partition request on the node that received it: the primary
/// lookup, every relay attempt and a relayed poll body, the hop target's
/// metadata hold, and the local lane and reply waits.
pub(in crate::http) const PARTITION_REQUEST_DEADLINE: Duration = Duration::from_secs(10);

/// Keeps the hop target's deadline before the contacted node's, so the hop
/// target answers while the contacted node still waits for it.
const RELAY_MARGIN: Duration = Duration::from_millis(100);

/// Wall-clock bound on one metadata forward attempt, including its buffered
/// reply. Above the primary's own 30s in-flight transient replay budget, so a
/// legitimately slow commit is answered rather than cut mid-flight; without
/// this cap a hung primary would park the connection for the whole retry
/// budget with no per-attempt bound (the HTTP client itself has no timeout).
/// A partition relay attempt ends at the request deadline instead.
const FORWARD_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(35);

/// Cadence between retryable attempts, mirroring the binary SDKs' in-client
/// replay loop and the local submit path's transient replay interval.
const FORWARD_RETRY_INTERVAL: Duration = Duration::from_millis(50);

/// Budget across retryable attempts. Sized to ride out a full view change
/// (detection is up to the heartbeat timeout, default 5s, plus election
/// rounds) a few times over; on exhaustion the caller gets a retryable 503.
const FORWARD_RETRY_DEADLINE: Duration = Duration::from_secs(30);

/// Cap on concurrent forwards held by this node. Deliberately its own budget:
/// a forward parks no reply slot and touches none of the shard bus machinery
/// the partition-write caps protect, and counting forwards against those caps
/// would let a slow primary starve this node's own direct clients.
const MAX_IN_FLIGHT_FORWARDS: u32 = 128;

/// Bound on a relayed response body, enforced twice: against a declared
/// `content-length` before the read, and as a running cap on the streamed
/// bytes so a length-less reply is bounded too. Successful poll responses
/// stream separately because their automatic commit may already have advanced
/// progress, so rejecting their total size would discard acknowledged data.
const RESPONSE_BODY_LIMIT: usize = 64 * 1024 * 1024;

/// Pre-allocation hint cap for a relayed body: honest control-plane replies
/// fit well under this, and a mis-declared content-length must not reserve
/// the full [`RESPONSE_BODY_LIMIT`] up front.
const RESPONSE_CAPACITY_HINT: usize = 64 * 1024;

/// Response headers copied from the primary's reply. Everything else is
/// dropped, which subsumes the RFC 7230 hop-by-hop set: the relayed response
/// uses a new body, so upstream `connection` / `transfer-encoding`
/// semantics cannot leak to the client. `iggy-view` and `iggy-applied-op` are
/// included so the relayed response carries the serving primary's view and
/// applied op, not this follower's (the response layer only fills either when
/// absent); the applied op is also what this node records as the caller's
/// read-your-writes floor, so dropping it here would reopen the stale read.
/// `iggy-durability` preserves the primary's acknowledged completion policy
/// for writes.
const RELAYED_RESPONSE_HEADERS: [HeaderName; 5] = [
    CONTENT_TYPE,
    RETRY_AFTER,
    VIEW_HEADER,
    APPLIED_OP_HEADER,
    DURABILITY_HEADER,
];

/// What a contacted node chose for a relayed partition request. The hop target
/// serves exactly this partition under this context, so a keyed or balanced
/// send is never placed twice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::http) struct PartitionRoute {
    pub(in crate::http) partition_id: u32,
    pub(in crate::http) context: PartitionContext,
    budget: Duration,
}

impl fmt::Display for PartitionRoute {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{},{},{},{},{}",
            self.partition_id,
            self.context.incarnation,
            self.context.owner_generation,
            self.context.metadata_op,
            self.budget.as_millis()
        )
    }
}

impl FromStr for PartitionRoute {
    type Err = IggyError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        fn number<T: FromStr>(field: Option<&str>) -> Result<T, IggyError> {
            field
                .and_then(|field| field.parse().ok())
                .ok_or(IggyError::InvalidCommand)
        }
        let mut fields = value.split(',');
        let route = Self {
            partition_id: number(fields.next())?,
            context: PartitionContext {
                incarnation: number(fields.next())?,
                owner_generation: number(fields.next())?,
                metadata_op: number(fields.next())?,
            },
            budget: Duration::from_millis(number(fields.next())?),
        };
        if fields.next().is_some() {
            return Err(IggyError::InvalidCommand);
        }
        Ok(route)
    }
}

/// The place of a partition request in its single relay hop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::http) enum PartitionHop {
    /// No relay marker: this node chooses the partition and finds its primary.
    Contacted,
    /// Relayed with a route: this node serves that partition or refuses.
    Target(PartitionRoute),
    /// Relayed without a route, by an older build or by a caller that set the
    /// marker: served here with this node's own choices and never relayed.
    Unrouted,
}

/// Whether a partition request can change state on its primary, which decides
/// what a relay may resend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::http) enum PartitionEffect {
    /// An offset read, or a poll without `auto_commit`.
    Read,
    /// A send, an offset store or delete, or a poll with `auto_commit`.
    Write,
}

/// A partition request as the route layer buffered it, so that its handler can
/// relay it once the partition and context are fixed.
#[derive(Debug)]
pub(in crate::http) struct PartitionRequest {
    method: Method,
    /// Already the forwardable subset, with the relay marker.
    headers: HeaderMap,
    path_and_query: String,
    body: Bytes,
    pub(in crate::http) hop: PartitionHop,
    pub(in crate::http) deadline: Instant,
}

impl PartitionRequest {
    pub(in crate::http) const fn route(&self) -> Option<PartitionRoute> {
        match self.hop {
            PartitionHop::Target(route) => Some(route),
            PartitionHop::Contacted | PartitionHop::Unrouted => None,
        }
    }
}

/// Build the [`ForwardState`] at listener startup.
///
/// With `http.tls.enabled` the forward hop dials `https` and verifies the peer
/// against this node's OWN certificate chain (exact-DER pin): cluster nodes
/// are expected to share the HTTP certificate, and a fixed-roster deployment
/// makes pinning strictly stronger than name-based verification against
/// config-listed IPs. Per-node distinct certificates fail closed at the
/// handshake. Plaintext deployments dial plain `http`, which puts the relayed
/// bearer on the node-to-node link exactly as exposed as it already is on the
/// client-to-node link - TLS is the remedy for both.
///
/// # Errors
///
/// [`ServerError::ListenerCredentials`] when TLS is enabled but the PEM
/// files cannot be loaded, [`ServerError::HttpForwardClient`] when the
/// outbound client cannot be built.
pub(in crate::http) fn build_forward_state(
    tls: &HttpTlsConfig,
    body_limit: usize,
    active: bool,
) -> Result<ForwardState, ServerError> {
    let builder = cyper::Client::builder()
        // The retry loop re-resolves the primary from the local roster; a
        // followed `Location` would let the peer steer the bearer anywhere.
        .redirect(cyper::redirect::Policy::none());
    // `bootstrap()` installs the process-level provider before any shard
    // thread exists; both `ClientConfig` builders below panic without one, so
    // fail the boot instead. A unit test building this state installs it
    // itself, as http/tls.rs does.
    let provider = CryptoProvider::get_default().ok_or_else(|| ServerError::HttpForwardClient {
        reason: "no process-level rustls CryptoProvider installed".to_string(),
    })?;
    let (builder, scheme) = if tls.enabled {
        let credentials =
            load_pem(Path::new(&tls.cert_file), Path::new(&tls.key_file)).map_err(|source| {
                ServerError::ListenerCredentials {
                    transport: "http.tls",
                    source,
                }
            })?;
        // load_pem guarantees a non-empty chain; this is the no-panic
        // path for the unreachable empty case.
        let pinned = credentials.cert_chain.into_iter().next().ok_or_else(|| {
            ServerError::HttpForwardClient {
                reason: "TLS certificate chain is empty".to_string(),
            }
        })?;
        let algorithms = provider.signature_verification_algorithms;
        let config = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(PinnedCertVerifier { pinned, algorithms }))
            .with_no_client_auth();
        (builder.use_rustls(Arc::new(config)), "https")
    } else {
        // Explicit config even for plain http: cyper's implicit rustls
        // backend eagerly loads the system CA store at build() and fails
        // boot on CA-less hosts (minimal container images), although this
        // client never dials https. Empty roots skip that load and keep an
        // accidental https dial fail-closed.
        let config = rustls::ClientConfig::builder()
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_no_client_auth();
        (builder.use_rustls(Arc::new(config)), "http")
    };
    let client = builder
        .build()
        .map_err(|source| ServerError::HttpForwardClient {
            reason: source.to_string(),
        })?;
    Ok(ForwardState {
        active,
        client,
        scheme,
        body_limit,
        in_flight: Rc::new(Cell::new(0)),
    })
}

/// Route-layer middleware for the control-plane routes: pass through on the
/// primary and for local (non-linearizable) reads, otherwise forward to the
/// primary and relay its response.
///
/// The `!Send` internals (roster/consensus reads, the compio-bound HTTP
/// client) are bridged with `SendWrapper` exactly like every handler: sound
/// because the listener pins all of this to shard 0's thread.
pub(in crate::http) async fn forward_to_primary(
    State(state): State<HttpState>,
    request: Request,
    next: Next,
) -> Response {
    SendWrapper::new(forward_or_pass(state, request, next)).await
}

/// Route layer of the partition routes: buffer the request once and attach it
/// as a [`PartitionRequest`], so its handler can relay it to the partition
/// primary with [`route_partition`]. No attempt runs here.
pub(in crate::http) async fn forward_partition_request(
    State(state): State<HttpState>,
    request: Request,
    next: Next,
) -> Response {
    SendWrapper::new(attach_partition_request(state, request, next)).await
}

/// Decide where a partition request runs once its partition and context are
/// fixed. `None` serves it on this node. `Some` is the final answer: the reply
/// of the partition primary, or a refusal that proves nothing was admitted.
///
/// Until the deadline, every hop waits for a view with a primary, because a
/// new partition starts on each replica at its own pace. A hop target never
/// relays: it refuses with `NotPrimary` when another replica is the primary,
/// so a stale view never chains hops. The contacted node starts another turn
/// only after such a proven non-admission, a failed connect, or a read that
/// failed after its send.
pub(in crate::http) async fn route_partition(
    state: &HttpInner,
    request: &PartitionRequest,
    namespace: IggyNamespace,
    context: PartitionContext,
    effect: PartitionEffect,
) -> Option<Response> {
    if request.hop == PartitionHop::Unrouted {
        return None;
    }
    let Ok(partition_id) = u32::try_from(namespace.partition_id()) else {
        return Some(CustomError::from(IggyError::InvalidCommand).into_response());
    };
    let mut guard = None;
    loop {
        if let Some(primary) = partition_primary(state, namespace).await {
            if is_local_replica(state, primary) {
                return None;
            }
            if matches!(request.hop, PartitionHop::Target(_)) {
                return Some(CustomError::from(IggyError::NotPrimary).into_response());
            }
            if !state.forward.active {
                return Some(not_admitted());
            }
            if let Some(socket) = primary_http_socket(&state.roster, primary) {
                let remaining = request.deadline.saturating_duration_since(Instant::now());
                // A target without time would dispatch a write it cannot wait
                // for and answer an unknown outcome.
                if remaining <= RELAY_MARGIN {
                    return Some(not_admitted());
                }
                if guard.is_none() {
                    guard = ForwardGuard::admit(&state.forward.in_flight);
                    if guard.is_none() {
                        return Some(forward_busy());
                    }
                }
                let route = PartitionRoute {
                    partition_id,
                    context,
                    budget: remaining.saturating_sub(RELAY_MARGIN),
                };
                let Ok(route) = HeaderValue::try_from(route.to_string()) else {
                    return Some(not_admitted());
                };
                let mut headers = request.headers.clone();
                headers.insert(PARTITION_ROUTE_HEADER, route);
                let url = format!(
                    "{}://{socket}{}",
                    state.forward.scheme, request.path_and_query
                );
                let outcome = attempt(
                    state,
                    &request.method,
                    headers,
                    &request.body,
                    &url,
                    RelayKind::Partition(effect),
                    remaining,
                )
                .await;
                if let AttemptOutcome::Relay(response) = outcome {
                    return Some(match guard {
                        Some(guard)
                            if request.method == Method::GET && response.status().is_success() =>
                        {
                            retain_forward_guard(response, guard, FORWARD_ATTEMPT_TIMEOUT)
                        }
                        _ => response,
                    });
                }
            }
        }
        if request.deadline.saturating_duration_since(Instant::now()) <= FORWARD_RETRY_INTERVAL {
            return Some(not_admitted());
        }
        compio::time::sleep(FORWARD_RETRY_INTERVAL).await;
    }
}

async fn forward_or_pass(state: HttpState, request: Request, next: Next) -> Response {
    if !state.forward.active || state.is_metadata_primary() {
        return next.run(request).await;
    }
    // Reads default to the local STM and stay on this node; only an explicit
    // linearizable read must reach the primary. An encoded or malformed
    // `consistency` value falls through to the handler, whose own gate still
    // answers 307/503, so a miss here degrades, never breaks.
    if request.method() == Method::GET && !wants_linearizable(request.uri().query()) {
        return next.run(request).await;
    }
    if request.headers().contains_key(FORWARDED_HEADER) {
        // One hop max. The peer that forwarded here re-resolves the primary
        // and retries; the transient body code tells it the request never
        // entered any pipeline.
        return CustomError::from(IggyError::TransientNotAccepted).into_response();
    }
    // Verify-only auth gate (no VSR session mint): a garbage bearer dies here
    // instead of being buffered and relayed, so unauthenticated traffic cannot
    // use followers to amplify load onto the primary. The primary still runs
    // its full extractor on what arrives.
    let bearer = match bearer_token(request.headers()) {
        Ok(bearer) => bearer,
        Err(error) => return CustomError::from(error).into_response(),
    };
    // The user id is kept, not discarded: the relayed answer carries the
    // primary's applied op, and this node has to record it as this caller's
    // read-your-writes floor (see `record_relayed_floor`).
    let user_id = match resolve_credential(&state, bearer).await {
        Ok((_key, user_id, _user_created_at, _expiry)) => user_id,
        Err(rejection) => return rejection.into_response(),
    };
    let Some(_guard) = ForwardGuard::admit(&state.forward.in_flight) else {
        return forward_busy();
    };
    let response = forward(&state, request).await;
    record_relayed_floor(&state, user_id, &response);
    response
}

/// Record the serving primary's applied op as `user_id`'s read-your-writes
/// floor on THIS node.
///
/// The relayed write ran on the primary, so the local write path never saw it
/// and left no floor behind, while the caller's next unqualified GET stays
/// local: without this, a `POST` followed by a `GET` through the same follower
/// can answer from before the write. Only a relayed SUCCESS counts - a 503 or a
/// 4xx promises the caller nothing - and the floor is monotone, so a slow relay
/// landing after a faster one cannot lower it.
///
/// A missing or unparsable header is a no-op rather than a failure: it means
/// the peer is an older build, and a floor this node never learns is the
/// pre-existing behavior, not a new hazard.
fn record_relayed_floor(state: &HttpInner, user_id: u32, response: &Response) {
    if !response.status().is_success() {
        return;
    }
    let Some(applied) = response
        .headers()
        .get(APPLIED_OP_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
    else {
        debug!(
            user_id,
            "relayed response carried no applied op; the caller's floor stays where it was"
        );
        return;
    };
    state.metadata_watermarks.record(user_id, applied);
}

async fn attach_partition_request(state: HttpState, request: Request, next: Next) -> Response {
    let received = Instant::now();
    let hop = match partition_hop(request.headers()) {
        Ok(hop) => hop,
        Err(error) => return CustomError::from(error).into_response(),
    };
    // Verify-only, like the metadata relay: an unauthenticated caller gets no
    // buffered body and no relay. The handler authenticates again.
    let bearer = match bearer_token(request.headers()) {
        Ok(bearer) => bearer,
        Err(error) => return CustomError::from(error).into_response(),
    };
    if let Err(rejection) = resolve_credential(&state, bearer).await {
        return rejection.into_response();
    }
    let _guard = if matches!(hop, PartitionHop::Target(_)) {
        let Some(guard) = ForwardGuard::admit(&state.forward.in_flight) else {
            return forward_busy();
        };
        Some(guard)
    } else {
        None
    };
    let (mut parts, request_body) = request.into_parts();
    let Ok(body) = to_bytes(request_body, state.forward.body_limit).await else {
        return error_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "payload_too_large",
            "request body exceeds http.max_request_size",
        );
    };
    let deadline = received
        + match hop {
            PartitionHop::Target(route) => route.budget.min(PARTITION_REQUEST_DEADLINE),
            PartitionHop::Contacted | PartitionHop::Unrouted => PARTITION_REQUEST_DEADLINE,
        };
    // The contacted node chose the partition from metadata at this op. Before
    // that op is applied here, the topic or the partition may not exist yet,
    // and the handler would answer 404 for a request it can serve.
    if let PartitionHop::Target(route) = hop {
        let frontier = state.shard.plane.metadata().applied_frontier();
        let budget = compio::time::sleep(deadline.saturating_duration_since(Instant::now()));
        if hold_for_frontier(frontier, route.context.metadata_op, budget)
            .await
            .is_err()
        {
            return not_admitted();
        }
    }
    let partition_request = PartitionRequest {
        method: parts.method.clone(),
        headers: forwarded_headers(&parts.headers),
        path_and_query: parts
            .uri
            .path_and_query()
            .map_or("/", |path_and_query| path_and_query.as_str())
            .to_owned(),
        body: body.clone(),
        hop,
        deadline,
    };
    parts.extensions.insert(Arc::new(partition_request));
    next.run(Request::from_parts(parts, Body::from(body))).await
}

/// Buffer the request and drive forward attempts until one yields a relayable
/// outcome or the retry budget runs out.
async fn forward(state: &HttpInner, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    // The router-wide `DefaultBodyLimit` only annotates the request; it is
    // enforced by whoever consumes the body, so the bound is passed explicitly
    // here or the buffer would be unbounded.
    let Ok(body) = to_bytes(body, state.forward.body_limit).await else {
        return error_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "payload_too_large",
            "request body exceeds http.max_request_size",
        );
    };
    let path_and_query = parts
        .uri
        .path_and_query()
        .map_or("/", |path_and_query| path_and_query.as_str());
    let deadline = Instant::now() + FORWARD_RETRY_DEADLINE;
    loop {
        let outcome = match primary_socket(state) {
            // No resolvable primary (mid-election, or a roster hole): count it
            // as a retryable attempt so a completing election is picked up.
            None => AttemptOutcome::Retry,
            Some(socket) => {
                let url = format!("{}://{socket}{path_and_query}", state.forward.scheme);
                attempt(
                    state,
                    &parts.method,
                    forwarded_headers(&parts.headers),
                    &body,
                    &url,
                    RelayKind::Metadata,
                    FORWARD_ATTEMPT_TIMEOUT,
                )
                .await
            }
        };
        match outcome {
            AttemptOutcome::Relay(response) => return response,
            AttemptOutcome::Retry => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    warn!(
                        path = parts.uri.path(),
                        "forward retry budget exhausted without a reachable primary"
                    );
                    return with_retry_after(error_response(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "no_reachable_primary",
                        "no primary accepted the request within the forward budget; retry",
                    ));
                }
                compio::time::sleep(FORWARD_RETRY_INTERVAL.min(remaining)).await;
            }
        }
    }
}

enum AttemptOutcome {
    /// Terminal: hand this response to the client.
    Relay(Response),
    /// A resend cannot apply anything twice: either nothing was admitted, or
    /// the request is a read. Re-resolve and retry.
    Retry,
}

/// What a relay may retry on besides a failed connect: a reply that proves
/// nothing was admitted, or a transport failure of a partition read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RelayKind {
    /// The target was not the metadata primary: a 307, or a 503 whose op never
    /// entered a pipeline.
    Metadata,
    /// The target was not the partition primary: a 503 carrying `NotPrimary`.
    /// Any other 503 goes back to the caller, because the target was the
    /// primary and refused for a reason another turn cannot change. A read
    /// also retries a transport failure after its send.
    Partition(PartitionEffect),
}

/// Bound connect, send and buffered replies by `timeout`. Successful partition
/// polls return after their headers and have a separate body deadline.
async fn attempt(
    state: &HttpInner,
    method: &Method,
    headers: HeaderMap,
    body: &Bytes,
    url: &str,
    kind: RelayKind,
    timeout: Duration,
) -> AttemptOutcome {
    let builder = match state.forward.client.request(method.clone(), url) {
        Ok(builder) => builder,
        Err(error) => {
            warn!(%error, "forward request build failed");
            return AttemptOutcome::Relay(bad_gateway());
        }
    };
    let request = builder.headers(headers).body(body.clone()).build();
    let attempt = async {
        let response = match state.forward.client.execute(request).await {
            Ok(response) => response,
            Err(error) => return classify_transport_error(&error, kind),
        };
        classify_forwarded_reply(response, method, kind).await
    };
    match compio::time::timeout(timeout, attempt).await {
        Err(_elapsed) => AttemptOutcome::Relay(forward_timeout(kind)),
        Ok(outcome) => outcome,
    }
}

fn forward_timeout(kind: RelayKind) -> Response {
    if kind == RelayKind::Partition(PartitionEffect::Read) {
        return with_retry_after(error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "forward_timeout",
            "the primary did not answer the forwarded read in time; retry",
        ));
    }
    gateway_timeout_response(
        "forward_timeout",
        "the primary did not answer the forwarded request in time; the outcome is unknown",
    )
}

async fn classify_forwarded_reply(
    response: cyper::Response,
    method: &Method,
    kind: RelayKind,
) -> AttemptOutcome {
    let status = response.status();
    // Only the relayed subset survives; the response is consumed by the
    // body stream below, so the values are pulled out first.
    let relayed_headers: Vec<(HeaderName, HeaderValue)> = RELAYED_RESPONSE_HEADERS
        .into_iter()
        .filter_map(|name| {
            let value = response.headers().get(&name)?.clone();
            Some((name, value))
        })
        .collect();
    if method == Method::GET && matches!(kind, RelayKind::Partition(_)) && status.is_success() {
        let mut response = Response::new(stream_poll_body(response.bytes_stream()));
        *response.status_mut() = status;
        for (name, value) in relayed_headers {
            response.headers_mut().insert(name, value);
        }
        return AttemptOutcome::Relay(response);
    }
    let declared = response.content_length();
    if declared.is_some_and(|length| length > RESPONSE_BODY_LIMIT as u64) {
        warn!(?declared, "relayed response exceeds the body bound");
        return AttemptOutcome::Relay(bad_gateway());
    }
    // Streamed with a running cap so a length-less reply is bounded by
    // the limit, not merely by the attempt timeout. The capacity hint is
    // clamped to RESPONSE_CAPACITY_HINT so a mis-declared content-length
    // cannot pre-reserve the full bound. The running cap still bounds the
    // real total.
    let mut body = Vec::with_capacity(
        declared
            .and_then(|length| usize::try_from(length).ok())
            .unwrap_or(0)
            .min(RESPONSE_CAPACITY_HINT),
    );
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(error) => return classify_transport_error(&error, kind),
        };
        if body.len() + chunk.len() > RESPONSE_BODY_LIMIT {
            warn!(
                received = body.len() + chunk.len(),
                "relayed response exceeds the body bound"
            );
            return AttemptOutcome::Relay(bad_gateway());
        }
        body.extend_from_slice(&chunk);
    }
    classify_reply(status, relayed_headers, Bytes::from(body), kind)
}

fn stream_poll_body(stream: impl Stream<Item = Result<Bytes, cyper::Error>> + 'static) -> Body {
    let stream = futures::stream::try_unfold(Box::pin(stream), |mut stream| async move {
        match compio::time::timeout(FORWARD_ATTEMPT_TIMEOUT, stream.next()).await {
            Ok(Some(Ok(bytes))) => Ok(Some((bytes, stream))),
            Ok(None) => Ok(None),
            Ok(Some(Err(error))) => Err(std::io::Error::other(error.to_string())),
            Err(_) => Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "forwarded poll response stalled",
            )),
        }
    });
    Body::from_stream(SendWrapper::new(stream))
}

fn retain_forward_guard(
    response: Response,
    guard: ForwardGuard,
    idle_timeout: Duration,
) -> Response {
    let (parts, body) = response.into_parts();
    let (sender, receiver) = async_channel::bounded(1);
    // The timer must run even when the downstream connection stops polling its
    // body. One queued chunk bounds read-ahead; dropping the body cancels the task.
    let task = compio::runtime::spawn(async move {
        let mut stream = body.into_data_stream();
        loop {
            let relay_chunk = async {
                match stream.next().await {
                    Some(chunk) => sender
                        .send(chunk.map_err(std::io::Error::other))
                        .await
                        .is_ok(),
                    None => false,
                }
            };
            match compio::time::timeout(idle_timeout, relay_chunk).await {
                Ok(true) => {}
                Ok(false) => break,
                Err(_) => {
                    drop(stream);
                    drop(guard);
                    // Discard a queued chunk so a stalled reader still receives
                    // an explicit body error instead of mistaking timeout for EOF.
                    let _ = sender.force_send(Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "forwarded poll response stalled",
                    )));
                    break;
                }
            }
        }
    });
    let stream = futures::stream::unfold((receiver, task), |(receiver, task)| async move {
        receiver
            .recv()
            .await
            .ok()
            .map(|chunk| (chunk, (receiver, task)))
    });
    Response::from_parts(parts, Body::from_stream(SendWrapper::new(stream)))
}

/// Copy the forwardable request headers: the bearer (the primary
/// re-authenticates it) and the content type. Everything else - including any
/// client-supplied forward marker or partition route - is dropped, then the
/// loop-guard marker is stamped fresh.
fn forwarded_headers(request_headers: &HeaderMap) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for name in [AUTHORIZATION, CONTENT_TYPE] {
        if let Some(value) = request_headers.get(&name) {
            headers.insert(name, value.clone());
        }
    }
    headers.insert(FORWARDED_HEADER, FORWARDED_VALUE);
    headers
}

/// Grade a transport-level failure. A connect-phase error - the request was
/// never written - may retry, and so may any failure of a partition read,
/// which changes no state. For any other request a later failure (reset
/// mid-request, a broken body read) leaves the outcome unknown and must
/// surface, because the primary may have committed the op and a re-issue
/// would double-apply it. Such a request on a pooled connection that dies
/// before the request is written also lands in the relay arm (hyper exposes
/// no sound never-sent predicate): that spurious 502 is the safe side, and
/// hyper's own canceled-request retry for buffered bodies absorbs most of it.
fn classify_transport_error(error: &cyper::Error, kind: RelayKind) -> AttemptOutcome {
    if let cyper::Error::HyperClient(client_error) = error
        && client_error.is_connect()
    {
        debug!(%error, "forward connect failed; re-resolving primary");
        return AttemptOutcome::Retry;
    }
    if kind == RelayKind::Partition(PartitionEffect::Read) {
        debug!(%error, "relayed read failed after its send; re-resolving primary");
        return AttemptOutcome::Retry;
    }
    warn!(%error, "forward transport error after connect; outcome unknown");
    AttemptOutcome::Relay(bad_gateway())
}

/// Grade a complete reply from the target.
///
/// A 307 means the target itself was not the metadata primary and knows a
/// better one; the retry re-resolves from the LOCAL view instead of trusting
/// the `Location`. A 503 is retried only when its body proves the target was
/// not the primary for this [`RelayKind`] - a `TransientNotCommitted` 503 may
/// still commit and is relayed untouched. Everything else is the primary's
/// answer and is relayed.
fn classify_reply(
    status: StatusCode,
    relayed_headers: Vec<(HeaderName, HeaderValue)>,
    body: Bytes,
    kind: RelayKind,
) -> AttemptOutcome {
    let not_primary = match kind {
        RelayKind::Metadata => {
            status == StatusCode::TEMPORARY_REDIRECT
                || (status == StatusCode::SERVICE_UNAVAILABLE
                    && error_code(&body).is_some_and(|code| {
                        code == IggyError::TransientNotAccepted.as_code()
                            || code == IggyError::NotPrimary.as_code()
                    }))
        }
        RelayKind::Partition(_) => {
            status == StatusCode::SERVICE_UNAVAILABLE
                && error_code(&body) == Some(IggyError::NotPrimary.as_code())
        }
    };
    if not_primary {
        return AttemptOutcome::Retry;
    }
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    for (name, value) in relayed_headers {
        response.headers_mut().insert(name, value);
    }
    AttemptOutcome::Relay(response)
}

/// The `id` of a JSON `ErrorResponse` body. Unparsable or foreign bodies have
/// none, so when in doubt a reply is relayed, never retried.
fn error_code(body: &[u8]) -> Option<u32> {
    #[derive(Deserialize)]
    struct ErrorId {
        id: u32,
    }
    serde_json::from_slice::<ErrorId>(body)
        .ok()
        .map(|error| error.id)
}

/// HTTP socket of the current metadata primary, from the live consensus view
/// and the static roster. `None` mid-election or when the roster has no HTTP
/// address for the primary.
fn primary_socket(state: &HttpInner) -> Option<SocketAddr> {
    let consensus = state.shard.plane.metadata().consensus.as_ref()?;
    let primary_index = consensus.primary_index(consensus.view());
    primary_http_socket(&state.roster, primary_index)
}

/// Read the relay marker and the partition route of a request. Only a marked
/// request carries a route, and a malformed one is the caller's error.
fn partition_hop(headers: &HeaderMap) -> Result<PartitionHop, IggyError> {
    if !headers.contains_key(FORWARDED_HEADER) {
        return Ok(PartitionHop::Contacted);
    }
    let Some(route) = headers.get(PARTITION_ROUTE_HEADER) else {
        return Ok(PartitionHop::Unrouted);
    };
    route
        .to_str()
        .map_err(|_| IggyError::InvalidCommand)?
        .parse()
        .map(PartitionHop::Target)
}

/// The primary replica of the partition group in this replica's view. `None`
/// while the group has no usable view here: view change, state transfer or a
/// partition that is not materialized yet.
async fn partition_primary(state: &HttpInner, namespace: IggyNamespace) -> Option<u8> {
    match state
        .shard
        .partition_read(namespace, PartitionRead::Primary)
        .await?
    {
        PartitionReadReply::Primary { primary, .. } => Some(primary),
        _ => None,
    }
}

fn is_local_replica(state: &HttpInner, replica: u8) -> bool {
    state
        .shard
        .plane
        .metadata()
        .consensus
        .as_ref()
        .is_none_or(|consensus| consensus.replica() == replica)
}

/// The refusal that proves nothing was admitted, safe to re-issue anywhere.
fn not_admitted() -> Response {
    CustomError::from(IggyError::TransientNotAccepted).into_response()
}

fn forward_busy() -> Response {
    with_retry_after(error_response(
        StatusCode::SERVICE_UNAVAILABLE,
        "forward_busy",
        "node is at its forward budget; retry with backoff",
    ))
}

fn wants_linearizable(query: Option<&str>) -> bool {
    query.is_some_and(|query| {
        query
            .split('&')
            .any(|pair| pair == "consistency=linearizable")
    })
}

fn bad_gateway() -> Response {
    error_response(
        StatusCode::BAD_GATEWAY,
        "forward_failed",
        "forwarding to the primary failed after the request was sent; the outcome is unknown",
    )
}

/// RAII admission against [`MAX_IN_FLIGHT_FORWARDS`]; releases on drop, so a
/// client disconnect mid-forward frees the slot.
struct ForwardGuard {
    in_flight: Rc<Cell<u32>>,
}

impl ForwardGuard {
    fn admit(in_flight: &Rc<Cell<u32>>) -> Option<Self> {
        if in_flight.get() >= MAX_IN_FLIGHT_FORWARDS {
            return None;
        }
        in_flight.set(in_flight.get() + 1);
        Some(Self {
            in_flight: Rc::clone(in_flight),
        })
    }
}

impl Drop for ForwardGuard {
    fn drop(&mut self) {
        self.in_flight.set(self.in_flight.get() - 1);
    }
}

/// Exact-DER pin against this node's own end-entity certificate. Presented-leaf
/// equality replaces chain building and name checks on purpose (config-listed
/// IPs rarely appear as SANs in operator certs); handshake signatures are still
/// verified with the provider's algorithms, so possession of the pinned
/// certificate's private key remains required.
#[derive(Debug)]
struct PinnedCertVerifier {
    pinned: CertificateDer<'static>,
    algorithms: WebPkiSupportedAlgorithms,
}

impl ServerCertVerifier for PinnedCertVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        if self.pinned == *end_entity {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::InvalidCertificate(
                CertificateError::UnknownIssuer,
            ))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::{Read, Write};

    #[test]
    fn given_relay_timeout_when_request_is_read_only_should_report_retryable_unavailability() {
        let response = forward_timeout(RelayKind::Partition(PartitionEffect::Read));
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(response.headers().contains_key(RETRY_AFTER));
        for kind in [
            RelayKind::Metadata,
            RelayKind::Partition(PartitionEffect::Write),
        ] {
            assert_eq!(forward_timeout(kind).status(), StatusCode::GATEWAY_TIMEOUT);
        }
    }

    #[test]
    fn linearizable_query_detected_only_on_exact_pair() {
        assert!(wants_linearizable(Some("consistency=linearizable")));
        assert!(wants_linearizable(Some("foo=bar&consistency=linearizable")));
        assert!(!wants_linearizable(Some("consistency=serializable")));
        assert!(!wants_linearizable(Some("consistency=LINEARIZABLE")));
        assert!(!wants_linearizable(None));
    }

    #[test]
    fn partition_route_header_round_trips() {
        let route = PartitionRoute {
            partition_id: 3,
            context: PartitionContext {
                incarnation: 7,
                owner_generation: 0,
                metadata_op: 41,
            },
            budget: Duration::from_millis(9_900),
        };
        assert_eq!(route.to_string(), "3,7,0,41,9900");
        assert_eq!(route.to_string().parse::<PartitionRoute>(), Ok(route));
    }

    #[test]
    fn partition_route_header_refuses_malformed_values() {
        for malformed in [
            "",
            "3,7,0,41",
            "3,7,0,41,9900,1",
            "3,7,0,41,soon",
            "-3,7,0,41,9900",
            "3,,0,41,9900",
            "4294967296,7,0,41,9900",
        ] {
            assert_eq!(
                malformed.parse::<PartitionRoute>(),
                Err(IggyError::InvalidCommand),
                "{malformed:?} must be refused"
            );
        }
    }

    #[test]
    fn only_a_marked_request_reads_its_partition_route() {
        let mut headers = HeaderMap::new();
        headers.insert(
            PARTITION_ROUTE_HEADER,
            HeaderValue::from_static("3,7,0,41,9900"),
        );
        assert_eq!(partition_hop(&headers), Ok(PartitionHop::Contacted));

        headers.insert(FORWARDED_HEADER, FORWARDED_VALUE);
        let Ok(PartitionHop::Target(parsed)) = partition_hop(&headers) else {
            panic!("a marked request must carry its route")
        };
        assert_eq!(parsed.partition_id, 3);
        assert_eq!(parsed.context.metadata_op, 41);

        headers.remove(PARTITION_ROUTE_HEADER);
        assert_eq!(partition_hop(&headers), Ok(PartitionHop::Unrouted));

        headers.insert(PARTITION_ROUTE_HEADER, HeaderValue::from_static("3,7"));
        assert_eq!(partition_hop(&headers), Err(IggyError::InvalidCommand));
    }

    #[test]
    fn each_relay_kind_retries_only_its_proof_of_non_admission() {
        fn reply(status: StatusCode, code: Option<u32>, kind: RelayKind) -> bool {
            let body = code.map_or_else(Bytes::new, |code| {
                Bytes::from(format!(
                    r#"{{"id":{code},"code":"x","reason":"x","field":null}}"#
                ))
            });
            matches!(
                classify_reply(status, Vec::new(), body, kind),
                AttemptOutcome::Retry
            )
        }
        let unavailable = StatusCode::SERVICE_UNAVAILABLE;
        let redirect = StatusCode::TEMPORARY_REDIRECT;
        let not_primary = Some(IggyError::NotPrimary.as_code());
        let not_accepted = Some(IggyError::TransientNotAccepted.as_code());
        let not_committed = Some(IggyError::TransientNotCommitted.as_code());

        for effect in [PartitionEffect::Read, PartitionEffect::Write] {
            let kind = RelayKind::Partition(effect);
            assert!(reply(unavailable, not_primary, kind));
            assert!(!reply(unavailable, not_accepted, kind));
            assert!(!reply(unavailable, not_committed, kind));
            assert!(!reply(redirect, None, kind));
            assert!(!reply(StatusCode::OK, None, kind));
        }

        assert!(reply(unavailable, not_accepted, RelayKind::Metadata));
        assert!(reply(unavailable, not_primary, RelayKind::Metadata));
        assert!(reply(redirect, None, RelayKind::Metadata));
        assert!(!reply(unavailable, not_committed, RelayKind::Metadata));
        assert!(!reply(unavailable, None, RelayKind::Metadata));
    }

    #[compio::test]
    async fn only_a_partition_read_retries_a_failure_after_its_send() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let forward =
            build_forward_state(&HttpTlsConfig::default(), 1024, true).expect("forward client");
        let kinds = [
            (RelayKind::Partition(PartitionEffect::Read), true),
            (RelayKind::Partition(PartitionEffect::Write), false),
            (RelayKind::Metadata, false),
        ];
        let connections = kinds.len();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("HTTP listener");
        let address = listener.local_addr().expect("listener address");
        // Each connection closes once its request arrived, as a primary that
        // dies under a relayed request does.
        let server = std::thread::spawn(move || {
            for _ in 0..connections {
                let (mut socket, _) = listener.accept().expect("client connection");
                socket
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .expect("read timeout");
                let mut request = Vec::new();
                let mut bytes = [0; 1024];
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    let read = socket.read(&mut bytes).expect("request bytes");
                    assert_ne!(read, 0);
                    request.extend_from_slice(&bytes[..read]);
                }
            }
        });
        for (kind, retries) in kinds {
            let error = forward
                .client
                .get(format!("http://{address}/consumer-offsets"))
                .expect("offset request")
                .send()
                .await
                .expect_err("the target closed without a reply");
            assert_eq!(
                matches!(
                    classify_transport_error(&error, kind),
                    AttemptOutcome::Retry
                ),
                retries,
                "{kind:?} after {error}"
            );
        }
        server.join().expect("HTTP server finished");
    }

    #[test]
    fn forward_guard_caps_and_releases() {
        let in_flight = Rc::new(Cell::new(0));
        let guards: Vec<_> = (0..MAX_IN_FLIGHT_FORWARDS)
            .map(|_| ForwardGuard::admit(&in_flight).expect("under cap"))
            .collect();
        assert!(ForwardGuard::admit(&in_flight).is_none());
        drop(guards);
        assert_eq!(in_flight.get(), 0);
        assert!(ForwardGuard::admit(&in_flight).is_some());
    }

    #[compio::test]
    async fn forwarded_poll_streams_large_body_and_retains_admission() {
        let in_flight = Rc::new(Cell::new(0));
        let guard = ForwardGuard::admit(&in_flight).expect("forward admitted");
        let polled = Rc::new(Cell::new(0));
        let observed = Rc::clone(&polled);
        let chunk = Bytes::from(vec![0; 1024 * 1024]);
        let stream = futures::stream::iter((0..65).map(move |_| {
            observed.set(observed.get() + 1);
            Ok::<_, cyper::Error>(chunk.clone())
        }));
        let response = retain_forward_guard(
            Response::new(stream_poll_body(stream)),
            guard,
            FORWARD_ATTEMPT_TIMEOUT,
        );
        assert_eq!(polled.get(), 0);
        assert_eq!(in_flight.get(), 1);
        let mut stream = response.into_body().into_data_stream();
        let mut received = 0;
        while let Some(chunk) = stream.next().await {
            received += chunk.expect("successful chunk").len();
        }
        assert_eq!(received, 65 * 1024 * 1024);
        assert_eq!(in_flight.get(), 0);
    }

    #[compio::test]
    async fn dropping_forwarded_poll_body_releases_admission() {
        const CANCEL_TURN: Duration = Duration::from_millis(1);
        let in_flight = Rc::new(Cell::new(0));
        let guard = ForwardGuard::admit(&in_flight).expect("forward admitted");
        let response = retain_forward_guard(
            Response::new(stream_poll_body(futures::stream::pending())),
            guard,
            FORWARD_ATTEMPT_TIMEOUT,
        );
        assert_eq!(in_flight.get(), 1);
        drop(response);
        compio::time::sleep(CANCEL_TURN).await;
        assert_eq!(in_flight.get(), 0);
    }

    #[compio::test]
    async fn a_progressing_forwarded_poll_outlives_the_idle_timeout() {
        const IDLE_TIMEOUT: Duration = Duration::from_millis(200);
        const CHUNK_DELAY: Duration = Duration::from_millis(50);
        const CHUNKS: usize = 8;
        let in_flight = Rc::new(Cell::new(0));
        let guard = ForwardGuard::admit(&in_flight).expect("forward admitted");
        let stream = futures::stream::iter(0..CHUNKS).then(|_| async {
            compio::time::sleep(CHUNK_DELAY).await;
            Ok::<_, cyper::Error>(Bytes::from_static(b"chunk"))
        });
        let response =
            retain_forward_guard(Response::new(stream_poll_body(stream)), guard, IDLE_TIMEOUT);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("progressing body must not expire");
        assert_eq!(body.as_ref(), b"chunk".repeat(CHUNKS));
        assert_eq!(in_flight.get(), 0);
    }

    #[compio::test]
    async fn an_unread_forwarded_poll_expires_and_releases_upstream() {
        const BODY_TIMEOUT: Duration = Duration::from_millis(10);
        let in_flight = Rc::new(Cell::new(0));
        let guard = ForwardGuard::admit(&in_flight).expect("forward admitted");
        let upstream = Rc::new(());
        let held = Rc::clone(&upstream);
        let stream = futures::stream::unfold(held, |held| async move {
            Some((Ok::<_, cyper::Error>(Bytes::from_static(b"chunk")), held))
        });
        let response =
            retain_forward_guard(Response::new(stream_poll_body(stream)), guard, BODY_TIMEOUT);
        compio::time::sleep(BODY_TIMEOUT * 3).await;
        assert_eq!(in_flight.get(), 0, "unread bodies must release admission");
        assert_eq!(Rc::strong_count(&upstream), 1, "upstream must be dropped");
        assert!(to_bytes(response.into_body(), usize::MAX).await.is_err());
    }

    #[compio::test]
    async fn forwarded_poll_accepts_large_http_content_length() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let forward =
            build_forward_state(&HttpTlsConfig::default(), 1024, true).expect("forward client");
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("HTTP listener");
        let address = listener.local_addr().expect("listener address");
        let server = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("client connection");
            socket
                .set_read_timeout(Some(Duration::from_secs(10)))
                .expect("read timeout");
            socket
                .set_write_timeout(Some(Duration::from_secs(10)))
                .expect("write timeout");
            let mut request = Vec::new();
            let mut bytes = [0; 1024];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = socket.read(&mut bytes).expect("request bytes");
                assert_ne!(read, 0);
                request.extend_from_slice(&bytes[..read]);
            }
            write!(socket, "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n", 65 * 1024 * 1024)
                .expect("response headers");
            let chunk = vec![0; 1024 * 1024];
            for _ in 0..65 {
                socket.write_all(&chunk).expect("response chunk");
            }
        });
        let response = forward
            .client
            .get(format!("http://{address}/messages"))
            .expect("poll request")
            .send()
            .await
            .expect("poll response");
        assert_eq!(response.content_length(), Some(65 * 1024 * 1024));
        let AttemptOutcome::Relay(response) = classify_forwarded_reply(
            response,
            &Method::GET,
            RelayKind::Partition(PartitionEffect::Read),
        )
        .await
        else {
            panic!("successful poll must not be retried")
        };
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[CONTENT_TYPE], "application/json");
        let mut stream = response.into_body().into_data_stream();
        let mut received = 0;
        while let Some(chunk) = stream.next().await {
            received += chunk.expect("response chunk").len();
        }
        assert_eq!(received, 65 * 1024 * 1024);
        server.join().expect("HTTP server finished");
    }
}
