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
//! Partition routes run on the node that received them first, through the
//! primary gate of the partition's owner. The contacted node chooses the
//! partition and its context once, and relays the request with both in the
//! `iggy-partition-route` header only after the owner refused it without
//! admitting it and the partition's view names another primary. The hop target
//! serves exactly that partition or refuses with `NotPrimary`, which proves
//! nothing was admitted, and never relays. A write whose caller holds no
//! session on the contacted node looks the primary up first instead, because
//! serving it there would first Register a session. A hop target's
//! `NotPrimary` names the primary of its own view of the group, so a contacted
//! node whose view lags reaches that primary in one more hop instead of
//! retrying the stale one until its own view catches up.
//!
//! A linearizable partition read on a node that is not the metadata primary
//! first holds until this node applied the metadata primary's commit floor
//! ([`metadata_floor`]), so it is served here or relayed once instead of
//! redirected to the metadata primary.
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
//! - The route header carries what a hop target acts on: the partition, its
//!   context and the metadata op it holds the request for. The contacted node
//!   signs it ([`super::relay_proof`]) and the hop target refuses a route
//!   without a valid, current proof, so a client cannot pose as a relay. Its
//!   403 names why, so a contacted node whose own proof was refused answers its
//!   caller the retryable `relay_refused` 503 instead of a 403 that reads as
//!   the caller's. The contacted node signs only a metadata op it applied, so
//!   the op a caller names in its context cannot hold the hop target either.
//!   The proof vouches for the relay fields, not the caller: the hop target
//!   authenticates and authorizes the request again, once it applied that op,
//!   because the caller's account can be as new as the op.
//! - A forwarded write is retried only when it provably never entered the
//!   primary's pipeline: a connect-phase failure, and for a metadata relay a
//!   503 whose body carries the `TransientNotAccepted` or `NotPrimary` code or
//!   a 307 from a stale target, for a partition relay a 503 carrying
//!   `NotPrimary`. Every other outcome, including `TransientNotCommitted`, is
//!   relayed as-is: its op may still commit, and re-issuing it under a fresh
//!   session would defeat the consensus dedup and double-apply the op.
//! - A partition read changes no state, so a transport failure after its send
//!   is retried too: the resend cannot apply anything twice.
//! - The target is always resolved from the local roster, by a replica index
//!   from the local consensus view or from a refusing hop target's newer view,
//!   never from a response `Location`, and the client follows no redirects.

use std::cell::Cell;
use std::fmt;
use std::net::SocketAddr;
use std::ops::ControlFlow;
use std::path::Path;
use std::rc::Rc;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::{Body, to_bytes};
use axum::extract::{Request, State};
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE, RETRY_AFTER};
use axum::http::request::Parts;
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use configs::http::HttpTlsConfig;
use consensus::{MetadataHandle, is_settled_primary};
use cyper::IntoUrl;
use futures::{Stream, StreamExt};
use iggy_binary_protocol::primitives::partition_history::PartitionContext;
use iggy_common::{IggyError, IggyTimestamp};
use message_bus::MessageBus;
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
use crate::http::admission::{RelayTargetRefusal, RelayTargets};
use crate::http::error::{
    CustomError, PartitionWriteError, ReadError, error_response, forward_busy,
    gateway_timeout_response, primary_http_socket, relay_refused, with_retry_after,
};
use crate::http::extractor::{bearer_token, credential_key, resolve_credential};
use crate::http::relay_proof::{RELAY_PROOF_LIFETIME, RelayKey, UnprovenRelay, parse_decimal};
use crate::http::state::{
    APPLIED_OP_HEADER, DURABILITY_HEADER, FloorReads, ForwardState, HttpInner, VIEW_HEADER,
};
use crate::http::{HttpState, close_connection};
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

/// The contacted node's proof of the route, the method, the path and query,
/// and its own expiry ([`RelayKey::prove`]).
const RELAY_PROOF_HEADER: HeaderName = HeaderName::from_static("iggy-relay-proof");

/// Why a node refused a relay proof ([`UnprovenRelay::as_str`]), on its 403.
/// The node that made the proof finds it on the answer to its own relay, so
/// it can tell a cluster misconfiguration from the caller's own 403.
const RELAY_REFUSAL_HEADER: HeaderName = HeaderName::from_static("iggy-relay-refusal");

/// The view and primary of a partition group as a hop target that refused a
/// relay with `NotPrimary` sees them, as `<view>,<replica>`.
const PARTITION_PRIMARY_HEADER: HeaderName = HeaderName::from_static("iggy-partition-primary");

/// Internal route on which the metadata primary answers its commit floor to
/// another cluster node ([`metadata_floor`]).
pub(in crate::http) const METADATA_FLOOR_PATH: &str = "/cluster/metadata-floor";

/// The route a metadata floor request proves: it relays no partition request.
const NO_PARTITION_ROUTE: HeaderValue = HeaderValue::from_static("");

/// One bound for a partition request on the node that received it: the
/// metadata floor read and hold, every local attempt, primary lookup and relay
/// attempt, a relayed poll body, the hop target's metadata hold, and the local
/// lane and reply waits. A relay proof lives as long, so no relay attempt
/// outlives its proof.
pub(in crate::http) const PARTITION_REQUEST_DEADLINE: Duration = RELAY_PROOF_LIFETIME;

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

/// Budget of a metadata relay, from its body read across retryable attempts.
/// Sized to ride out a full view change (detection is up to the heartbeat
/// timeout, default 5s, plus election rounds) a few times over; on exhaustion
/// the caller gets a retryable 503.
const FORWARD_RETRY_DEADLINE: Duration = Duration::from_secs(30);

/// Cap on concurrent forwards held by this node: metadata forwards and
/// partition relays. Deliberately its own budget: a forward
/// parks no reply slot and touches none of the shard bus machinery the
/// partition-write caps protect, and counting forwards against those caps
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
                .and_then(parse_decimal)
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

/// The primary of one view of a partition group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PartitionPrimary {
    view: u32,
    replica: u8,
}

impl PartitionPrimary {
    /// `hint` when it names a newer view than this one.
    fn newer(self, hint: Option<Self>) -> Option<Self> {
        hint.filter(|hint| hint.view > self.view)
    }
}

impl fmt::Display for PartitionPrimary {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{},{}", self.view, self.replica)
    }
}

impl FromStr for PartitionPrimary {
    type Err = IggyError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (view, replica) = value.split_once(',').ok_or(IggyError::InvalidCommand)?;
        Ok(Self {
            view: parse_decimal(view).ok_or(IggyError::InvalidCommand)?,
            replica: parse_decimal(replica).ok_or(IggyError::InvalidCommand)?,
        })
    }
}

/// The place of a partition request in its single relay hop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::http) enum PartitionHop {
    /// No relay marker: this node chooses the partition, serves it, and relays
    /// it when its owner refuses it and another replica is the primary.
    Contacted,
    /// Relayed with a route: this node serves that partition or refuses.
    Target(PartitionRoute),
    /// Relayed without a route, by an older build or by a caller that set the
    /// marker: served here with this node's own choices and never relayed.
    Unrouted,
}

/// Why a hop target refused the relay fields of a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HopRefusal {
    /// No valid proof, or no key on this node to check one with.
    Unproven(UnprovenRelay),
    /// A route under a valid proof that does not parse.
    MalformedRoute,
}

impl IntoResponse for HopRefusal {
    fn into_response(self) -> Response {
        match self {
            Self::Unproven(unproven) => unproven_relay(unproven),
            Self::MalformedRoute => CustomError::from(IggyError::InvalidCommand).into_response(),
        }
    }
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

/// What a contacted node runs first in each turn of a partition request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::http) enum FirstAttempt {
    /// The owner's primary gate here, which costs no more than a lookup.
    Local,
    /// The primary lookup, because serving here would first Register a
    /// session, a metadata commit that a relay wastes.
    Lookup,
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

/// The error of a partition route handler, for the code it was rejected with.
/// [`serve_partition`] acts on two codes of the owner's primary gate, both of
/// which prove nothing was admitted: `NotPrimary` and `TransientNotAccepted`.
pub(in crate::http) trait Rejection {
    fn rejection(&self) -> Option<&IggyError>;
}

impl Rejection for ReadError {
    fn rejection(&self) -> Option<&IggyError> {
        match self {
            Self::Rejected(error) => Some(error),
            _ => None,
        }
    }
}

impl Rejection for PartitionWriteError {
    fn rejection(&self) -> Option<&IggyError> {
        match self {
            Self::Rejected(error) => Some(error),
            _ => None,
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
/// `key_material` is the cluster-wide bearer key material; the relay proof key
/// derives from it, and without it forwarding stays off.
///
/// # Errors
///
/// [`ServerError::ListenerCredentials`] when TLS is enabled but the PEM
/// files cannot be loaded, [`ServerError::HttpForwardClient`] when the
/// outbound client cannot be built.
pub(in crate::http) fn build_forward_state(
    tls: &HttpTlsConfig,
    body_limit: usize,
    key_material: Option<&str>,
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
        relay_key: key_material.map(RelayKey::derive),
        client,
        scheme,
        body_limit,
        in_flight: Rc::new(Cell::new(0)),
        relay_targets: RelayTargets::default(),
        floor_reads: FloorReads::default(),
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
/// primary with [`serve_partition`]. No attempt runs here.
pub(in crate::http) async fn forward_partition_request(
    State(state): State<HttpState>,
    request: Request,
    next: Next,
) -> Response {
    SendWrapper::new(attach_partition_request(state, request, next)).await
}

/// Serve a partition request on this node through its owner's primary gate,
/// and look up the partition primary only after the owner refused it. The
/// partition and context are fixed by then, so a relay carries the choice
/// this node made.
///
/// `serve_locally` must answer `NotPrimary` or `TransientNotAccepted` only for
/// a request it did not admit. Every other outcome is the answer, so a request
/// that may have been admitted, or whose reply went missing, never runs twice.
///
/// After such a refusal the lookup decides. The primary's own
/// `TransientNotAccepted` is the answer, another primary takes the relay, and
/// a view that moved here runs the request here again. A group in a view
/// change or a state transfer, or a partition this replica has not
/// materialized, names no primary, so the lookup waits for one until the
/// deadline instead of running the request into a replica that refuses it
/// again. A hop target never relays: it answers `NotPrimary` instead, so a
/// stale view never chains hops. The refusal names the primary of the target's
/// own view, and the contacted node relays once more to it when that view is
/// newer than the one it relayed by. Otherwise the contacted node starts
/// another turn after such a refusal, a failed connect, or a read that failed
/// after its send.
///
/// With [`FirstAttempt::Lookup`] a contacted node runs the lookup first and
/// serves here only once it names this replica. A hop target always serves
/// first, because the contacted node looked the primary up already.
pub(in crate::http) async fn serve_partition<E: Rejection>(
    state: &HttpInner,
    request: &PartitionRequest,
    namespace: IggyNamespace,
    context: PartitionContext,
    effect: PartitionEffect,
    first: FirstAttempt,
    mut serve_locally: impl AsyncFnMut() -> Result<Response, E>,
) -> Result<Response, E> {
    if request.hop == PartitionHop::Unrouted {
        return serve_locally().await;
    }
    let target = matches!(request.hop, PartitionHop::Target(_));
    let mut local_first = target || first == FirstAttempt::Local;
    let out_of_time =
        || request.deadline.saturating_duration_since(Instant::now()) <= FORWARD_RETRY_INTERVAL;
    let mut guard = None;
    loop {
        let refused = if local_first {
            let served = serve_locally().await;
            let not_accepted = match served.as_ref().err().and_then(Rejection::rejection) {
                Some(IggyError::NotPrimary) if target => {
                    return Ok(not_primary(partition_primary(state, namespace).await));
                }
                Some(IggyError::NotPrimary) => false,
                Some(IggyError::TransientNotAccepted) => true,
                _ => return served,
            };
            Some((served, not_accepted))
        } else {
            None
        };
        let mut waited_for_view = false;
        let primary = loop {
            if let Some(primary) = partition_primary(state, namespace).await {
                break primary;
            }
            if out_of_time() {
                return Ok(not_admitted());
            }
            waited_for_view = true;
            compio::time::sleep(FORWARD_RETRY_INTERVAL).await;
        };
        if is_local_replica(state, primary.replica) {
            match refused {
                // A refusal from before the group had a primary says nothing
                // about the primary it has now.
                Some((served, true)) if !waited_for_view => return served,
                Some(_) => {}
                None => {
                    local_first = true;
                    continue;
                }
            }
        } else if target {
            return Ok(not_primary(Some(primary)));
        } else {
            let mut primary = primary;
            loop {
                let relayed = relay_to_primary(
                    state,
                    request,
                    namespace,
                    primary.replica,
                    context,
                    effect,
                    &mut guard,
                )
                .await;
                match relayed {
                    AttemptOutcome::Relay(response) => return Ok(response),
                    // A target that applied a newer view names its primary,
                    // which one more hop reaches before this replica's view.
                    AttemptOutcome::Retry(hint) => match primary.newer(hint) {
                        Some(newer) if !is_local_replica(state, newer.replica) => primary = newer,
                        _ => break,
                    },
                }
            }
        }
        if out_of_time() {
            return Ok(not_admitted());
        }
        compio::time::sleep(FORWARD_RETRY_INTERVAL).await;
    }
}

/// Refuse a request unless this node is the primary of its partition group in
/// this replica's view. Only `?ack=none` needs it: it answers once the
/// partition plane takes the write, before the owner's gate can refuse it, so
/// the gate's refusal never reaches it.
pub(in crate::http) async fn require_local_primary(
    state: &HttpInner,
    namespace: IggyNamespace,
) -> Result<(), IggyError> {
    match partition_primary(state, namespace).await {
        Some(primary) if is_local_replica(state, primary.replica) => Ok(()),
        Some(_) => Err(IggyError::NotPrimary),
        None => Err(IggyError::TransientNotAccepted),
    }
}

/// Relay a request the owner here refused to `primary`, the partition primary
/// of this replica's view. [`AttemptOutcome::Retry`] when the roster has no
/// HTTP address for it, or when the target proved it admitted nothing: the
/// caller then serves the request here again, or relays it to the primary of
/// the newer view the target named.
async fn relay_to_primary(
    state: &HttpInner,
    request: &PartitionRequest,
    namespace: IggyNamespace,
    primary: u8,
    context: PartitionContext,
    effect: PartitionEffect,
    guard: &mut Option<ForwardGuard>,
) -> AttemptOutcome {
    let Some(relay_key) = &state.forward.relay_key else {
        return AttemptOutcome::Relay(not_admitted());
    };
    let Some(socket) = primary_http_socket(&state.roster, primary) else {
        return AttemptOutcome::Retry(None);
    };
    let Ok(partition_id) = u32::try_from(namespace.partition_id()) else {
        return AttemptOutcome::Relay(CustomError::from(IggyError::InvalidCommand).into_response());
    };
    // The hop target holds a relay until it applied the metadata op of its
    // route, and a caller can name any op in the context of its request. This
    // node vouches only for an op it applied, so the wait for an op the
    // cluster may never reach parks here, without a slot of the hop target or
    // of this node's relays.
    let frontier = state.shard.plane.metadata().applied_frontier();
    let budget = compio::time::sleep(request.deadline.saturating_duration_since(Instant::now()));
    if hold_for_frontier(frontier, context.metadata_op, budget)
        .await
        .is_err()
    {
        return AttemptOutcome::Relay(not_admitted());
    }
    let remaining = request.deadline.saturating_duration_since(Instant::now());
    // A target without time would dispatch a write it cannot wait for and
    // answer an unknown outcome.
    if remaining <= RELAY_MARGIN {
        return AttemptOutcome::Relay(not_admitted());
    }
    if guard.is_none() {
        *guard = ForwardGuard::admit(&state.forward.in_flight);
        if guard.is_none() {
            return AttemptOutcome::Relay(forward_busy());
        }
    }
    let route = PartitionRoute {
        partition_id,
        context,
        budget: remaining.saturating_sub(RELAY_MARGIN),
    };
    let url = format!(
        "{}://{socket}{}",
        state.forward.scheme, request.path_and_query
    );
    let Some(headers) = relay_headers(relay_key, request, route, &url) else {
        return AttemptOutcome::Relay(not_admitted());
    };
    state.metrics.partition_relays.inc();
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
    match outcome {
        AttemptOutcome::Relay(response) => AttemptOutcome::Relay(match guard.take() {
            Some(guard) if request.method == Method::GET && response.status().is_success() => {
                retain_forward_guard(response, guard, FORWARD_ATTEMPT_TIMEOUT)
            }
            _ => response,
        }),
        retry @ AttemptOutcome::Retry(_) => retry,
    }
}

/// The metadata primary's commit floor, read before `deadline`: an op at or
/// above every metadata op acknowledged before this call. Refusals preserve
/// whether forwarding is unavailable or the primary rejected this node's
/// proof.
///
/// The reads are single flight ([`FloorReads`]): a burst of linearizable reads
/// sends one read per round trip to the metadata primary, and one per
/// [`FORWARD_RETRY_INTERVAL`] while it is not settled. That is their own
/// budget, so they hold no forward slot ([`MAX_IN_FLIGHT_FORWARDS`]) and
/// relays cannot starve them.
pub(in crate::http) async fn metadata_floor(
    state: &HttpInner,
    deadline: Instant,
) -> Result<u64, ReadError> {
    let relay_key = state
        .forward
        .relay_key
        .as_ref()
        .ok_or(ReadError::NotPrimary)?;
    loop {
        let read = state.forward.floor_reads.read(|| async {
            let Some(socket) = primary_socket(state) else {
                return ControlFlow::Continue(());
            };
            state.metrics.metadata_floor_reads.inc();
            read_metadata_floor(state, relay_key, socket).await
        });
        let remaining = deadline.saturating_duration_since(Instant::now());
        if let Ok(ControlFlow::Break(floor)) = compio::time::timeout(remaining, read).await {
            return floor.ok_or(ReadError::RelayRefused);
        }
        if deadline.saturating_duration_since(Instant::now()) <= FORWARD_RETRY_INTERVAL {
            return Err(ReadError::NotPrimary);
        }
        compio::time::sleep(FORWARD_RETRY_INTERVAL).await;
    }
}

/// One read of the floor from the metadata primary at `socket`. It breaks
/// with the floor, or with `None` when the primary refused this node's proof,
/// which another read cannot change, and continues on any other failure.
async fn read_metadata_floor(
    state: &HttpInner,
    relay_key: &RelayKey,
    socket: SocketAddr,
) -> ControlFlow<Option<u64>> {
    let Ok(proof) = relay_key.prove(
        &Method::GET,
        METADATA_FLOOR_PATH,
        &NO_PARTITION_ROUTE,
        IggyTimestamp::now(),
    ) else {
        return ControlFlow::Continue(());
    };
    let url = format!("{}://{socket}{METADATA_FLOOR_PATH}", state.forward.scheme);
    let sent = async {
        state
            .forward
            .client
            .get(url)?
            .header(RELAY_PROOF_HEADER, proof)?
            .send()
            .await
    };
    let response = match sent.await {
        Ok(response) => response,
        Err(error) => {
            debug!(%error, "metadata floor read failed; retrying");
            return ControlFlow::Continue(());
        }
    };
    if proof_refused(response.status(), response.headers(), socket) {
        return ControlFlow::Break(None);
    }
    if response.status() != StatusCode::NO_CONTENT {
        debug!(status = %response.status(), "metadata primary did not answer its floor; retrying");
        return ControlFlow::Continue(());
    }
    response
        .headers()
        .get(APPLIED_OP_HEADER)
        .and_then(|floor| parse_decimal(floor.to_str().ok()?))
        .map_or(ControlFlow::Continue(()), |floor| {
            ControlFlow::Break(Some(floor))
        })
}

/// Answer another cluster node's [`metadata_floor`] read: 204 with the floor
/// in [`APPLIED_OP_HEADER`]. A request without a valid relay proof is refused,
/// so only a cluster node learns the floor, and a node that cannot vouch for
/// its floor answers the retryable not-admitted 503.
pub(in crate::http) async fn answer_metadata_floor(
    State(state): State<HttpState>,
    request: Request,
) -> Response {
    let path_and_query = request
        .uri()
        .path_and_query()
        .map_or("/", |path_and_query| path_and_query.as_str());
    let Some(relay_key) = &state.forward.relay_key else {
        return unproven_relay(UnprovenRelay::Invalid);
    };
    if let Err(unproven) = relay_key.verify(
        request.method(),
        path_and_query,
        &NO_PARTITION_ROUTE,
        request.headers().get(RELAY_PROOF_HEADER),
        IggyTimestamp::now(),
    ) {
        return unproven_relay(unproven);
    }
    metadata_commit_floor(&state).map_or_else(not_admitted, |floor| {
        (
            StatusCode::NO_CONTENT,
            [(APPLIED_OP_HEADER, HeaderValue::from(floor))],
        )
            .into_response()
    })
}

/// This node's applied metadata op, when no metadata op acknowledged anywhere
/// can be above it: this node is the settled primary of a view whose start it
/// applied, with any recovered suffix applied too. A newer primary's merged
/// log can hold ops an older view acknowledged above its commit, so its
/// applied op becomes a floor only once it applied that log.
///
/// The floor also needs this node's own partition admission: a primary cut
/// off from its quorum stops vouching when its quorum evidence expires, as
/// its own partition requests stop. That bounds the stale window by the
/// freshness lease, not by a quorum round, so it is not strictly
/// linearizable.
fn metadata_commit_floor(state: &HttpInner) -> Option<u64> {
    let consensus = state.shard.plane.metadata().consensus.as_ref()?;
    let frontier = state.shard.plane.metadata().applied_frontier();
    let settled = is_settled_primary(consensus)
        && consensus.applied_view_start()
        && consensus.commit_min() >= consensus.recovery_barrier()
        && frontier.partition_requests_allowed(state.shard.bus.monotonic_now());
    settled.then(|| frontier.get())
}

async fn forward_or_pass(state: HttpState, request: Request, next: Next) -> Response {
    if !state.forward.active() || state.is_metadata_primary() {
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
        Ok(verified) => verified.user_id,
        Err(rejection) => return rejection.into_response(),
    };
    let deadline = Instant::now() + FORWARD_RETRY_DEADLINE;
    let (parts, body) = request.into_parts();
    // Buffered before a slot is taken: the slots also carry this node's
    // partition relays, so a stalled upload must end with its request instead
    // of keeping a slot for as long as it stalls. The router-wide
    // `DefaultBodyLimit` only annotates the request, so the bound is passed
    // here or the buffer would be unbounded.
    let read = compio::time::timeout(
        deadline.saturating_duration_since(Instant::now()),
        to_bytes(body, state.forward.body_limit),
    )
    .await;
    let Ok(read) = read else {
        return close_connection(parts.version, not_admitted());
    };
    let Ok(body) = read else {
        return error_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "payload_too_large",
            "request body exceeds http.max_request_size",
        );
    };
    let Some(_guard) = ForwardGuard::admit(&state.forward.in_flight) else {
        return forward_busy();
    };
    let response = forward(&state, &parts, &body, deadline).await;
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
        .and_then(parse_decimal)
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
    let hop = match partition_hop(
        &request,
        state.forward.relay_key.as_ref(),
        IggyTimestamp::now(),
    ) {
        Ok(hop) => hop,
        Err(refusal) => return refusal.into_response(),
    };
    // Verify-only, like the metadata relay: an unauthenticated caller gets no
    // buffered body and no relay. The handler's extractor reuses the proof.
    let bearer = match bearer_token(request.headers()) {
        Ok(bearer) => bearer,
        Err(error) => return CustomError::from(error).into_response(),
    };
    let deadline = received
        + match hop {
            PartitionHop::Target(route) => route.budget.min(PARTITION_REQUEST_DEADLINE),
            PartitionHop::Contacted | PartitionHop::Unrouted => PARTITION_REQUEST_DEADLINE,
        };
    // The contacted node chose the partition and authenticated the caller
    // from metadata at this op. Before that op is applied here, the topic, the
    // partition or the caller's account may not exist yet, and this node would
    // refuse a request it can serve.
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
    let verified = match resolve_credential(&state, bearer).await {
        Ok(verified) => verified,
        Err(rejection) => return rejection.into_response(),
    };
    let target = if matches!(hop, PartitionHop::Target(_)) {
        let credential = credential_key(
            bearer,
            verified.jwt_id.as_deref(),
            verified.user_id,
            verified.user_created_at,
        );
        match state.forward.relay_targets.admit(credential) {
            Ok(guard) => Some(guard),
            Err(refusal) => return relay_target_refused(refusal),
        }
    } else {
        None
    };
    let (mut parts, request_body) = request.into_parts();
    let read = to_bytes(request_body, state.forward.body_limit);
    // A hop target holds its slot from here on, so a stalled upload must end
    // with the request instead of keeping the slot for as long as it stalls.
    let read = if target.is_some() {
        match compio::time::timeout(deadline.saturating_duration_since(Instant::now()), read).await
        {
            Ok(read) => read,
            Err(_elapsed) => return close_connection(parts.version, not_admitted()),
        }
    } else {
        read.await
    };
    let Ok(body) = read else {
        return error_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "payload_too_large",
            "request body exceeds http.max_request_size",
        );
    };
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
    parts.extensions.insert(verified);
    next.run(Request::from_parts(parts, Body::from(body))).await
}

/// Drive forward attempts of a buffered request until one yields a relayable
/// outcome or `deadline` passes.
async fn forward(state: &HttpInner, parts: &Parts, body: &Bytes, deadline: Instant) -> Response {
    let path_and_query = parts
        .uri
        .path_and_query()
        .map_or("/", |path_and_query| path_and_query.as_str());
    loop {
        let outcome = match primary_socket(state) {
            // No resolvable primary (mid-election, or a roster hole): count it
            // as a retryable attempt so a completing election is picked up.
            None => AttemptOutcome::Retry(None),
            Some(socket) => {
                let url = format!("{}://{socket}{path_and_query}", state.forward.scheme);
                attempt(
                    state,
                    &parts.method,
                    forwarded_headers(&parts.headers),
                    body,
                    &url,
                    RelayKind::Metadata,
                    FORWARD_ATTEMPT_TIMEOUT,
                )
                .await
            }
        };
        match outcome {
            AttemptOutcome::Relay(response) => return response,
            AttemptOutcome::Retry(_) => {
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
    /// the request is a read. Re-resolve and retry, by the primary of a newer
    /// view when the refusing partition target named one.
    Retry(Option<PartitionPrimary>),
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
    if proof_refused(
        response.status(),
        response.headers(),
        response.url().authority(),
    ) {
        return AttemptOutcome::Relay(relay_refused());
    }
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
    let hint = response
        .headers()
        .get(PARTITION_PRIMARY_HEADER)
        .and_then(|value| value.to_str().ok()?.parse().ok());
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
    classify_reply(status, relayed_headers, Bytes::from(body), kind, hint)
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

/// The headers of one partition relay attempt: the forwardable ones, the route
/// and its proof. The proof covers the path and query as the URL serializes
/// them, which may percent-encode what the caller sent raw, because those are
/// the bytes the hop target receives and checks.
fn relay_headers(
    key: &RelayKey,
    request: &PartitionRequest,
    route: PartitionRoute,
    url: &str,
) -> Option<HeaderMap> {
    let route = HeaderValue::try_from(route.to_string()).ok()?;
    let url = url.into_url().ok()?;
    let path_and_query = url.query().map_or_else(
        || url.path().to_owned(),
        |query| format!("{}?{query}", url.path()),
    );
    let proof = key
        .prove(
            &request.method,
            &path_and_query,
            &route,
            IggyTimestamp::now(),
        )
        .ok()?;
    let mut headers = request.headers.clone();
    headers.insert(PARTITION_ROUTE_HEADER, route);
    headers.insert(RELAY_PROOF_HEADER, proof);
    Some(headers)
}

/// Copy the forwardable request headers: the bearer (the primary
/// re-authenticates it) and the content type. Everything else - including any
/// client-supplied forward marker, partition route or relay proof - is
/// dropped, then the loop-guard marker is stamped fresh.
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
        return AttemptOutcome::Retry(None);
    }
    if kind == RelayKind::Partition(PartitionEffect::Read) {
        debug!(%error, "relayed read failed after its send; re-resolving primary");
        return AttemptOutcome::Retry(None);
    }
    warn!(%error, "forward transport error after connect; outcome unknown");
    AttemptOutcome::Relay(bad_gateway())
}

/// Grade a complete reply from the target.
///
/// A 307 means the target itself was not the metadata primary and knows a
/// better one; the retry re-resolves from the LOCAL view instead of trusting
/// the `Location`. A 503 is retried only when its body proves the target was
/// not the primary for this [`RelayKind`]. Everything else is the primary's
/// answer and is relayed, such as a `TransientNotCommitted` 504 whose op may
/// still commit. A retry carries `hint`, the primary a refusing
/// target named.
fn classify_reply(
    status: StatusCode,
    relayed_headers: Vec<(HeaderName, HeaderValue)>,
    body: Bytes,
    kind: RelayKind,
    hint: Option<PartitionPrimary>,
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
        return AttemptOutcome::Retry(hint);
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
/// request carries a route, and only with a cluster node's proof, valid at
/// `now`: a client can neither take a hop-target slot nor choose the metadata
/// op a hold waits for. A malformed route under a valid proof is the caller's
/// error.
fn partition_hop(
    request: &Request,
    relay_key: Option<&RelayKey>,
    now: IggyTimestamp,
) -> Result<PartitionHop, HopRefusal> {
    let headers = request.headers();
    if !headers.contains_key(FORWARDED_HEADER) {
        return Ok(PartitionHop::Contacted);
    }
    let Some(route) = headers.get(PARTITION_ROUTE_HEADER) else {
        return Ok(PartitionHop::Unrouted);
    };
    let path_and_query = request
        .uri()
        .path_and_query()
        .map_or("/", |path_and_query| path_and_query.as_str());
    let proven = relay_key.map_or(Err(UnprovenRelay::Invalid), |key| {
        key.verify(
            request.method(),
            path_and_query,
            route,
            headers.get(RELAY_PROOF_HEADER),
            now,
        )
    });
    if let Err(unproven) = proven {
        match unproven {
            UnprovenRelay::Expired(late) => warn!(
                ?late,
                "refused a relay whose proof expired; node clocks disagree or the relay stalled"
            ),
            UnprovenRelay::Early(ahead) => warn!(
                ?ahead,
                "refused a relay whose proof was made ahead of this node's clock; node clocks disagree"
            ),
            UnprovenRelay::Invalid => debug!("refused relay fields without a valid proof"),
        }
        return Err(HopRefusal::Unproven(unproven));
    }
    route
        .to_str()
        .ok()
        .and_then(|route| route.parse().ok())
        .map(PartitionHop::Target)
        .ok_or(HopRefusal::MalformedRoute)
}

/// The primary of the partition group in this replica's view. `None` while the
/// group has no usable view here: view change, state transfer or a partition
/// that is not materialized yet.
async fn partition_primary(
    state: &HttpInner,
    namespace: IggyNamespace,
) -> Option<PartitionPrimary> {
    state.metrics.partition_primary_probes.inc();
    match state
        .shard
        .partition_read(namespace, PartitionRead::Primary)
        .await?
    {
        PartitionReadReply::Primary { view, primary } => Some(PartitionPrimary {
            view,
            replica: primary,
        }),
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

/// A hop target's refusal, naming the primary of its own view of the group
/// when it has one.
fn not_primary(primary: Option<PartitionPrimary>) -> Response {
    let mut response = CustomError::from(IggyError::NotPrimary).into_response();
    if let Some(value) = primary.and_then(|primary| HeaderValue::try_from(primary.to_string()).ok())
    {
        response
            .headers_mut()
            .insert(PARTITION_PRIMARY_HEADER, value);
    }
    response
}

/// The contacted node relays this answer to its caller as-is: a 429 when the
/// caller's credential saturated its own share, a 503 under node-wide pressure.
fn relay_target_refused(refusal: RelayTargetRefusal) -> Response {
    with_retry_after(match refusal {
        RelayTargetRefusal::CredentialSaturated => error_response(
            StatusCode::TOO_MANY_REQUESTS,
            "too_many_relayed_requests",
            "credential reached its relayed-request cap on the partition primary; await outstanding requests and retry",
        ),
        RelayTargetRefusal::BudgetSaturated => error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "relay_target_busy",
            "partition primary is at its relay-target budget; retry with backoff",
        ),
    })
}

/// The 403 for a relay proof that is not valid here, the answer a client that
/// sets relay fields itself gets, named for the node that made the proof.
fn unproven_relay(unproven: UnprovenRelay) -> Response {
    let mut response = CustomError::from(IggyError::Unauthorized).into_response();
    response.headers_mut().insert(
        RELAY_REFUSAL_HEADER,
        HeaderValue::from_static(unproven.as_str()),
    );
    response
}

/// Whether `target` refused this node's own relay proof: its 403 names why.
/// The cause lies in the cluster, not in the caller, so it is logged here,
/// where the caller's request failed.
fn proof_refused(status: StatusCode, headers: &HeaderMap, target: impl fmt::Display) -> bool {
    let Some(refusal) = headers
        .get(RELAY_REFUSAL_HEADER)
        .filter(|_| status == StatusCode::FORBIDDEN)
    else {
        return false;
    };
    warn!(
        %target,
        ?refusal,
        "a node refused this node's relay proof; invalid means the cluster key material differs, expired or early that node clocks disagree"
    );
    true
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
    use std::pin::pin;

    use configs::cluster::{ClusterNodeConfig, ResolvedClusterNode, TransportPorts};
    use futures::poll;
    use hyper::service::Service as _;
    use hyper_util::service::TowerToHyperService;
    use iggy_binary_protocol::requests::users::UpdatePermissionsRequest;
    use iggy_binary_protocol::{Operation, WireEncode, WireIdentifier};
    use iggy_common::IggyDuration;
    use metadata::impls::metadata::StreamsFrontend;
    use server_common::sharding::{PartitionLocation, ShardId};
    use shard::shards_table::ShardsTable;
    use shard::{LifecycleFrame, ShardFrame, shard_channel};

    use crate::dispatch::test_support::{create_one_partition_in, create_reader_request};
    use crate::http::handlers::HTTP_UNREGISTERED_CLIENT_ID;
    use crate::http::reads::tests::{
        READER_CREATED_AT, READER_ID, commit_user_request, read_state, read_state_on,
        read_state_with_frontier,
    };
    use crate::http::router;

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
            "+3,7,0,41,9900",
            "3,7,0,+41,9900",
            "03,7,0,41,9900",
            "3,7,0,41,09900",
        ] {
            assert_eq!(
                malformed.parse::<PartitionRoute>(),
                Err(IggyError::InvalidCommand),
                "{malformed:?} must be refused"
            );
        }
    }

    const KEY_MATERIAL: &str = "0123456789abcdef0123456789abcdef";
    const SEND_PATH: &str = "/streams/1/topics/1/messages";

    fn received(method: Method, path_and_query: &str, headers: HeaderMap) -> Request {
        let mut request = Request::builder()
            .method(method)
            .uri(path_and_query)
            .body(Body::empty())
            .expect("a valid request");
        *request.headers_mut() = headers;
        request
    }

    fn marked(route: &HeaderValue, proof: Option<&HeaderValue>) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(FORWARDED_HEADER, FORWARDED_VALUE);
        headers.insert(PARTITION_ROUTE_HEADER, route.clone());
        if let Some(proof) = proof {
            headers.insert(RELAY_PROOF_HEADER, proof.clone());
        }
        headers
    }

    fn send_proof(key: &RelayKey, route: &HeaderValue, made_at: IggyTimestamp) -> HeaderValue {
        key.prove(&Method::POST, SEND_PATH, route, made_at)
            .expect("a proof is a valid header value")
    }

    #[compio::test]
    async fn given_expired_metadata_when_contacted_or_relayed_partition_requests_arrive_should_refuse_before_namespace_and_grants()
     {
        const SEND_BODY: &str = r#"{"partitioning":{"kind":"balanced","value":""},"messages":[{"id":1,"payload":"eA=="}]}"#;
        const STORE_BODY: &str = r#"{"consumer_id":1,"partition_id":0,"offset":0}"#;
        let epoch = Instant::now().checked_sub(Duration::from_secs(9)).unwrap();
        let frontier = Arc::new(
            metadata::AppliedFrontier::default()
                .with_partition_freshness(epoch, Duration::from_secs(5)),
        );
        frontier.credit_partition_freshness(epoch, Duration::ZERO);
        let (sender, inbox, _replies) = shard_channel(0, 1, 1);
        let state = read_state_with_frontier(Some(sender), Some(KEY_MATERIAL), frontier);
        let namespace = create_one_partition_in(&state.shard.plane.metadata().mux_stm);
        commit_user_request(
            &state,
            READER_CREATED_AT,
            Operation::CreateUser,
            &create_reader_request().to_bytes(),
        );
        let revoke = UpdatePermissionsRequest {
            user_id: WireIdentifier::numeric(READER_ID),
            permissions: None,
        };
        commit_user_request(
            &state,
            READER_CREATED_AT + 1,
            Operation::UpdatePermissions,
            &revoke.to_bytes(),
        );
        let token = state
            .jwt
            .generate(READER_ID, IggyTimestamp::from(READER_CREATED_AT))
            .unwrap();
        let route = HeaderValue::try_from(
            PartitionRoute {
                partition_id: 0,
                context: PartitionContext {
                    incarnation: state
                        .shard
                        .plane
                        .metadata()
                        .mux_stm
                        .streams()
                        .created_revision_for_namespace(namespace)
                        .unwrap(),
                    owner_generation: 0,
                    metadata_op: 0,
                },
                budget: PARTITION_REQUEST_DEADLINE,
            }
            .to_string(),
        )
        .unwrap();
        let service = TowerToHyperService::new(router(
            state.clone(),
            state.forward.body_limit,
            None,
            None,
            false,
        ));
        for relayed in [false, true] {
            for stream in ["stream", "missing"] {
                for (method, suffix, body) in [
                    (
                        Method::GET,
                        "messages?consumer_id=1&partition_id=0&kind=offset&value=0&count=1",
                        "",
                    ),
                    (
                        Method::GET,
                        "consumer-offsets?consumer_id=1&partition_id=0",
                        "",
                    ),
                    (Method::POST, "messages", SEND_BODY),
                    (Method::PUT, "consumer-offsets", STORE_BODY),
                    (Method::DELETE, "consumer-offsets/1?partition_id=0", ""),
                ] {
                    let path = format!("/streams/{stream}/topics/topic/{suffix}");
                    let mut headers = if relayed {
                        let proof = RelayKey::derive(KEY_MATERIAL)
                            .prove(&method, &path, &route, IggyTimestamp::now())
                            .unwrap();
                        marked(&route, Some(&proof))
                    } else {
                        HeaderMap::new()
                    };
                    headers.insert(
                        AUTHORIZATION,
                        HeaderValue::try_from(format!("Bearer {}", token.access_token)).unwrap(),
                    );
                    headers.insert(
                        axum::http::header::CONTENT_TYPE,
                        HeaderValue::from_static("application/json"),
                    );
                    let mut request = received(method.clone(), &path, headers);
                    *request.body_mut() = Body::from(body);
                    let response = service.call(request).await.unwrap();
                    assert_eq!(
                        response.status(),
                        StatusCode::SERVICE_UNAVAILABLE,
                        "{method} {path}, relayed={relayed}"
                    );
                    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                        .await
                        .unwrap();
                    let error: serde_json::Value = serde_json::from_slice(&body).unwrap();
                    assert_eq!(
                        error["code"], "transient_not_accepted",
                        "{method} {path}: {error}"
                    );
                    assert!(
                        inbox.try_recv().is_err(),
                        "expired request must not reach a partition"
                    );
                }
            }
        }
    }

    #[test]
    fn only_a_marked_and_proven_request_reads_its_partition_route() {
        let key = RelayKey::derive(KEY_MATERIAL);
        let now = IggyTimestamp::now();
        let hop = |headers: &HeaderMap| {
            partition_hop(
                &received(Method::POST, SEND_PATH, headers.clone()),
                Some(&key),
                now,
            )
        };
        let route = HeaderValue::from_static("3,7,0,41,9900");
        let mut headers = marked(&route, Some(&send_proof(&key, &route, now)));
        headers.remove(FORWARDED_HEADER);
        assert_eq!(hop(&headers), Ok(PartitionHop::Contacted));

        headers.insert(FORWARDED_HEADER, FORWARDED_VALUE);
        let Ok(PartitionHop::Target(parsed)) = hop(&headers) else {
            panic!("a marked request must carry its route")
        };
        assert_eq!(parsed.partition_id, 3);
        assert_eq!(parsed.context.metadata_op, 41);

        headers.remove(PARTITION_ROUTE_HEADER);
        assert_eq!(hop(&headers), Ok(PartitionHop::Unrouted));

        let malformed = HeaderValue::from_static("3,7");
        let headers = marked(&malformed, Some(&send_proof(&key, &malformed, now)));
        assert_eq!(hop(&headers), Err(HopRefusal::MalformedRoute));
    }

    #[test]
    fn given_relay_fields_without_a_valid_proof_when_reading_the_hop_should_refuse_them() {
        let key = RelayKey::derive(KEY_MATERIAL);
        let now = IggyTimestamp::now();
        let later = now + IggyDuration::from(RELAY_PROOF_LIFETIME + Duration::from_secs(1));
        let genuine = HeaderValue::from_static("3,7,0,41,9900");
        let unreachable_frontier = HeaderValue::from_static("3,7,0,18446744073709551615,9900");
        let proof = send_proof(&key, &genuine, now);
        let early = send_proof(&key, &genuine, later);
        for (case, method, headers, relay_key, at, refusal) in [
            (
                "no proof",
                Method::POST,
                marked(&genuine, None),
                Some(&key),
                now,
                UnprovenRelay::Invalid,
            ),
            (
                "forged frontier",
                Method::POST,
                marked(&unreachable_frontier, Some(&proof)),
                Some(&key),
                now,
                UnprovenRelay::Invalid,
            ),
            (
                "another method",
                Method::GET,
                marked(&genuine, Some(&proof)),
                Some(&key),
                now,
                UnprovenRelay::Invalid,
            ),
            (
                "expired proof",
                Method::POST,
                marked(&genuine, Some(&proof)),
                Some(&key),
                later,
                UnprovenRelay::Expired(Duration::from_secs(1)),
            ),
            (
                "proof made ahead of this node's clock",
                Method::POST,
                marked(&genuine, Some(&early)),
                Some(&key),
                now,
                UnprovenRelay::Early(RELAY_PROOF_LIFETIME + Duration::from_secs(1)),
            ),
            (
                "no cluster key material",
                Method::POST,
                marked(&genuine, Some(&proof)),
                None,
                now,
                UnprovenRelay::Invalid,
            ),
        ] {
            assert_eq!(
                partition_hop(&received(method, SEND_PATH, headers), relay_key, at),
                Err(HopRefusal::Unproven(refusal)),
                "{case}"
            );
        }
    }

    #[test]
    fn given_relay_headers_of_an_attempt_when_the_hop_target_reads_them_should_prove_the_route() {
        let key = RelayKey::derive(KEY_MATERIAL);
        let request = PartitionRequest {
            method: Method::GET,
            headers: forwarded_headers(&HeaderMap::new()),
            path_and_query: format!("{SEND_PATH}?consumer_id=o'brien"),
            body: Bytes::new(),
            hop: PartitionHop::Contacted,
            deadline: Instant::now(),
        };
        let route = PartitionRoute {
            partition_id: 3,
            context: PartitionContext {
                incarnation: 7,
                owner_generation: 0,
                metadata_op: 41,
            },
            budget: Duration::from_millis(9_900),
        };
        let url = format!("http://127.0.0.1:3000{}", request.path_and_query);
        let headers = relay_headers(&key, &request, route, &url).expect("relay headers");
        // The URL percent-encodes the quote the caller sent raw, and the hop
        // target receives the encoded form.
        let target = received(
            Method::GET,
            &format!("{SEND_PATH}?consumer_id=o%27brien"),
            headers.clone(),
        );
        assert_eq!(
            partition_hop(&target, Some(&key), IggyTimestamp::now()),
            Ok(PartitionHop::Target(route))
        );
        let moved = received(
            Method::GET,
            &format!("{SEND_PATH}?consumer_id=other"),
            headers,
        );
        assert_eq!(
            partition_hop(&moved, Some(&key), IggyTimestamp::now()),
            Err(HopRefusal::Unproven(UnprovenRelay::Invalid))
        );
    }

    #[compio::test]
    async fn given_relay_fields_without_a_valid_proof_when_answering_should_name_the_refusal() {
        let state = read_state_on(None, Some(KEY_MATERIAL));
        let service = TowerToHyperService::new(router(
            state.clone(),
            state.forward.body_limit,
            None,
            None,
            false,
        ));
        let route = HeaderValue::from_static("3,7,0,41,9900");
        for request in [
            received(Method::POST, SEND_PATH, marked(&route, None)),
            received(Method::GET, METADATA_FLOOR_PATH, HeaderMap::new()),
        ] {
            let path = request.uri().path().to_owned();
            let response = service.call(request).await.expect("the node answers");
            assert_eq!(response.status(), StatusCode::FORBIDDEN, "{path}");
            assert_eq!(
                response.headers().get(RELAY_REFUSAL_HEADER),
                Some(&HeaderValue::from_static(UnprovenRelay::Invalid.as_str())),
                "{path}"
            );
        }
    }

    #[compio::test]
    async fn given_expired_partition_admission_when_answering_the_metadata_floor_should_refuse() {
        let epoch = Instant::now().checked_sub(Duration::from_secs(9)).unwrap();
        for (credited_at, expected) in [
            (Instant::now(), StatusCode::NO_CONTENT),
            (epoch, StatusCode::SERVICE_UNAVAILABLE),
        ] {
            let frontier = Arc::new(
                metadata::AppliedFrontier::default()
                    .with_partition_freshness(epoch, Duration::from_secs(5)),
            );
            frontier.credit_partition_freshness(credited_at, Duration::ZERO);
            let state = read_state_with_frontier(None, Some(KEY_MATERIAL), frontier);
            let proof = RelayKey::derive(KEY_MATERIAL)
                .prove(
                    &Method::GET,
                    METADATA_FLOOR_PATH,
                    &NO_PARTITION_ROUTE,
                    IggyTimestamp::now(),
                )
                .expect("a proof is a valid header value");
            let mut headers = HeaderMap::new();
            headers.insert(RELAY_PROOF_HEADER, proof);

            let response = answer_metadata_floor(
                State(state),
                received(Method::GET, METADATA_FLOOR_PATH, headers),
            )
            .await;

            assert_eq!(response.status(), expected);
        }
    }

    #[compio::test]
    async fn given_a_refusal_of_this_nodes_relay_proof_when_classifying_should_answer_it_as_no_403()
    {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let forward =
            build_forward_state(&HttpTlsConfig::default(), 1024, None).expect("forward client");
        let (address, server) = answer_with(vec![
            refused_proof_reply(UnprovenRelay::Expired(Duration::from_secs(1))),
            plain_reply(StatusCode::FORBIDDEN),
        ]);
        let mut answers = Vec::new();
        for _ in 0..2 {
            let response = forward
                .client
                .get(format!("http://{address}/consumer-offsets"))
                .expect("offset request")
                .send()
                .await
                .expect("the target answers");
            let AttemptOutcome::Relay(answer) = classify_forwarded_reply(
                response,
                &Method::GET,
                RelayKind::Partition(PartitionEffect::Read),
            )
            .await
            else {
                panic!("a 403 is never retried")
            };
            let status = answer.status();
            let body = to_bytes(answer.into_body(), usize::MAX)
                .await
                .expect("answer body");
            let code = serde_json::from_slice::<serde_json::Value>(&body)
                .ok()
                .and_then(|error| error["code"].as_str().map(str::to_owned));
            answers.push((status, code));
        }
        assert_eq!(
            answers,
            [
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Some("relay_refused".to_owned())
                ),
                (StatusCode::FORBIDDEN, None),
            ]
        );
        server.join().expect("HTTP server finished");
    }

    #[compio::test]
    async fn given_a_refusal_of_this_nodes_proof_when_reading_the_metadata_floor_should_stop() {
        const FLOOR: u64 = 41;
        let state = read_state_on(None, Some(KEY_MATERIAL));
        let relay_key = state.forward.relay_key.as_ref().expect("a relay key");
        let (address, server) = answer_with(vec![
            refused_proof_reply(UnprovenRelay::Invalid),
            plain_reply(StatusCode::SERVICE_UNAVAILABLE),
            format!(
                "HTTP/1.1 204 No Content\r\n{APPLIED_OP_HEADER}: {FLOOR}\r\nconnection: close\r\n\r\n"
            ),
        ]);
        for read in [
            ControlFlow::Break(None),
            ControlFlow::Continue(()),
            ControlFlow::Break(Some(FLOOR)),
        ] {
            assert_eq!(read_metadata_floor(&state, relay_key, address).await, read);
        }
        server.join().expect("HTTP server finished");
    }

    #[compio::test]
    async fn given_a_rejected_proof_when_awaiting_the_metadata_floor_should_report_relay_refused() {
        let (address, server) = answer_with(vec![refused_proof_reply(UnprovenRelay::Invalid)]);
        let state = state_with_metadata_primary_at(address);
        let refusal = crate::http::reads::await_metadata_floor(
            &state,
            Instant::now() + PARTITION_REQUEST_DEADLINE,
        )
        .await
        .expect_err("a refused proof must stop the read");
        assert!(matches!(refusal, ReadError::RelayRefused));
        let response = refusal.into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["code"], "relay_refused");
        assert_eq!(state.forward.in_flight.get(), 0);
        server.join().unwrap();
    }

    #[compio::test]
    async fn given_noncanonical_applied_op_when_reading_metadata_floor_should_retry() {
        let state = read_state_on(None, Some(KEY_MATERIAL));
        let relay_key = state.forward.relay_key.as_ref().unwrap();
        let floors = ["+41", "041", "41"];
        let (address, server) = answer_with(floors.iter().map(|floor| format!(
            "HTTP/1.1 204 No Content\r\n{APPLIED_OP_HEADER}: {floor}\r\nconnection: close\r\n\r\n"
        )).collect());
        let mut outcomes = Vec::new();
        for _ in floors {
            outcomes.push(read_metadata_floor(&state, relay_key, address).await);
        }
        server.join().unwrap();
        assert_eq!(
            outcomes,
            [
                ControlFlow::Continue(()),
                ControlFlow::Continue(()),
                ControlFlow::Break(Some(41))
            ]
        );
    }

    #[test]
    fn given_noncanonical_applied_op_when_recording_relay_should_preserve_floor() {
        let state = read_state_on(None, Some(KEY_MATERIAL));
        for value in ["+41", "041", "41"] {
            let response = (StatusCode::NO_CONTENT, [(APPLIED_OP_HEADER, value)]).into_response();
            record_relayed_floor(&state, READER_ID, &response);
            assert_eq!(
                state.metadata_watermarks.get(READER_ID),
                if value == "41" { 41 } else { 0 }
            );
        }
    }

    /// Answer each of `replies` on a connection of its own once that
    /// connection's request head arrived, as a node that answers exactly so.
    fn answer_with(replies: Vec<String>) -> (SocketAddr, std::thread::JoinHandle<()>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("HTTP listener");
        let address = listener.local_addr().expect("listener address");
        let server = std::thread::spawn(move || {
            for reply in replies {
                let (mut socket, _) = listener.accept().expect("client connection");
                read_request_head(&mut socket);
                socket.write_all(reply.as_bytes()).expect("reply");
            }
        });
        (address, server)
    }

    fn read_request_head(socket: &mut std::net::TcpStream) {
        const READ_TIMEOUT: Duration = Duration::from_secs(10);
        socket
            .set_read_timeout(Some(READ_TIMEOUT))
            .expect("read timeout");
        let mut request = Vec::new();
        let mut bytes = [0; 1024];
        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
            let read = socket.read(&mut bytes).expect("request bytes");
            assert_ne!(read, 0);
            request.extend_from_slice(&bytes[..read]);
        }
    }

    /// A state whose metadata primary, replica 0, serves HTTP at `address`.
    fn state_with_metadata_primary_at(address: SocketAddr) -> HttpState {
        let mut state = read_state_on(None, Some(KEY_MATERIAL));
        let roster = Rc::get_mut(&mut Rc::get_mut(&mut state).expect("one state owner").roster)
            .expect("one roster owner");
        roster.enabled = true;
        roster.nodes.push(
            ResolvedClusterNode::try_from(ClusterNodeConfig {
                name: "primary".to_owned(),
                ip: address.ip().to_string(),
                advertised_address: None,
                advertised_addresses: Vec::new(),
                replica_id: 0,
                ports: TransportPorts {
                    http: Some(address.port()),
                    ..TransportPorts::default()
                },
            })
            .expect("a valid roster node"),
        );
        state
    }

    fn floor_reply(floor: u64) -> String {
        format!(
            "HTTP/1.1 204 No Content\r\n{APPLIED_OP_HEADER}: {floor}\r\nconnection: close\r\n\r\n"
        )
    }

    fn refused_proof_reply(unproven: UnprovenRelay) -> String {
        format!(
            "HTTP/1.1 403 Forbidden\r\n{RELAY_REFUSAL_HEADER}: {}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
            unproven.as_str()
        )
    }

    fn plain_reply(status: StatusCode) -> String {
        format!("HTTP/1.1 {status}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
    }

    /// A contacted node relays with the metadata op it chose the partition
    /// at, and the account of the caller can be as new as that op. The hop
    /// target holds the relay until it applied the op and only then checks the
    /// bearer and the grants, so it serves an account it did not know yet when
    /// the relay arrived.
    #[compio::test]
    async fn given_a_relay_from_an_account_its_op_created_when_the_hop_target_lags_should_serve_it_once_applied()
     {
        const POLL_PATH: &str = "/streams/stream/topics/topic/messages?consumer_id=1&partition_id=0&kind=offset&value=0&count=1";
        let (sender, shard_inbox, _shard_replies) = shard_channel(0, 1, 1);
        let state = read_state_on(Some(sender), Some(KEY_MATERIAL));
        let metadata = state.shard.plane.metadata();
        let namespace = create_one_partition_in(&metadata.mux_stm);
        state
            .shard
            .shards_table()
            .insert(namespace, PartitionLocation::new(ShardId::new(0), 0));
        let frontier = metadata.applied_frontier();
        assert!(
            frontier.get() < READER_CREATED_AT,
            "the hop target has not applied the op that creates the reader"
        );
        let route = PartitionRoute {
            partition_id: 0,
            context: metadata
                .mux_stm
                .streams()
                .poll_metadata(namespace, None, HTTP_UNREGISTERED_CLIENT_ID)
                .expect("the partition resolves")
                .context(READER_CREATED_AT),
            budget: PARTITION_REQUEST_DEADLINE,
        };
        let route = HeaderValue::try_from(route.to_string()).expect("a route is a header value");
        let proof = RelayKey::derive(KEY_MATERIAL)
            .prove(&Method::GET, POLL_PATH, &route, IggyTimestamp::now())
            .expect("a proof is a header value");
        let token = state
            .jwt
            .generate(READER_ID, IggyTimestamp::from(READER_CREATED_AT))
            .expect("a token for the reader");
        let mut headers = marked(&route, Some(&proof));
        headers.insert(
            AUTHORIZATION,
            HeaderValue::try_from(format!("Bearer {}", token.access_token))
                .expect("a bearer is a header value"),
        );
        let service = TowerToHyperService::new(router(
            state.clone(),
            state.forward.body_limit,
            None,
            None,
            false,
        ));
        let mut response = pin!(service.call(received(Method::GET, POLL_PATH, headers)));
        assert!(
            poll!(response.as_mut()).is_pending(),
            "the hop target must hold the relay for its op"
        );

        commit_user_request(
            &state,
            READER_CREATED_AT,
            Operation::CreateUser,
            &create_reader_request().to_bytes(),
        );
        frontier.advance(READER_CREATED_AT);
        assert!(
            poll!(response.as_mut()).is_pending(),
            "the poll waits for its partition"
        );
        let Ok(ShardFrame::Lifecycle(LifecycleFrame::PartitionRead {
            read: PartitionRead::UnboundPollOnPrimary { metadata, .. },
            reply,
            ..
        })) = shard_inbox.try_recv()
        else {
            panic!("the hop target must serve the relay once it applied its op");
        };
        reply
            .try_send(PartitionReadReply::Poll {
                context: metadata.context(READER_CREATED_AT),
                fragments: Vec::new().into(),
                current_offset: 0,
            })
            .expect("the poll waits for its reply");
        assert_eq!(
            response.await.expect("the hop target answers").status(),
            StatusCode::OK
        );
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
                classify_reply(status, Vec::new(), body, kind, None),
                AttemptOutcome::Retry(None)
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
            build_forward_state(&HttpTlsConfig::default(), 1024, None).expect("forward client");
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
                read_request_head(&mut socket);
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
                    AttemptOutcome::Retry(None)
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

    /// A waiter whose deadline drops the read it sent must not strand the
    /// waiters behind it: the next one sends its own read.
    #[compio::test]
    async fn given_a_dropped_floor_read_when_waiters_remain_should_send_the_next_read() {
        const FLOOR: u64 = 7;
        let reads = FloorReads::default();
        let mut dropped = Box::pin(reads.read(std::future::pending));
        assert!(poll!(dropped.as_mut()).is_pending());
        let mut waiting = Box::pin(reads.read(|| async { ControlFlow::Break(Some(FLOOR)) }));
        assert!(poll!(waiting.as_mut()).is_pending());
        drop(dropped);
        assert_eq!(waiting.await, ControlFlow::Break(Some(FLOOR)));
    }

    /// Floor reads have their own budget, so relays that hold every forward
    /// slot do not refuse a linearizable read its floor.
    #[compio::test]
    async fn given_a_full_forward_budget_when_reading_the_metadata_floor_should_still_wait_for_it()
    {
        let state = read_state_on(None, Some(KEY_MATERIAL));
        state.forward.in_flight.set(MAX_IN_FLIGHT_FORWARDS);
        let mut waiting = Box::pin(metadata_floor(
            &state,
            Instant::now() + PARTITION_REQUEST_DEADLINE,
        ));
        assert!(
            poll!(waiting.as_mut()).is_pending(),
            "a floor read must not take a forward slot"
        );
        assert_eq!(state.forward.in_flight.get(), MAX_IN_FLIGHT_FORWARDS);
    }

    /// A floor read already sent may miss ops acknowledged after it left, so
    /// it serves only the waiter that sent it. The two waiters that arrive
    /// while it is in flight share the next read, and the metadata primary
    /// answers two reads for three waiters.
    #[compio::test]
    async fn given_concurrent_floor_waiters_when_a_read_is_in_flight_should_share_the_next_read() {
        const FIRST_FLOOR: u64 = 41;
        const NEXT_FLOOR: u64 = 42;
        let (address, server) =
            answer_with(vec![floor_reply(FIRST_FLOOR), floor_reply(NEXT_FLOOR)]);
        let state = state_with_metadata_primary_at(address);
        let deadline = Instant::now() + Duration::from_secs(2);
        let floors = futures::join!(
            metadata_floor(&state, deadline),
            metadata_floor(&state, deadline),
            metadata_floor(&state, deadline),
        );
        server.join().expect("HTTP server finished");
        assert_eq!(
            [floors.0.ok(), floors.1.ok(), floors.2.ok()],
            [Some(FIRST_FLOOR), Some(NEXT_FLOOR), Some(NEXT_FLOOR)]
        );
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
            build_forward_state(&HttpTlsConfig::default(), 1024, None).expect("forward client");
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("HTTP listener");
        let address = listener.local_addr().expect("listener address");
        let server = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().expect("client connection");
            socket
                .set_write_timeout(Some(Duration::from_secs(10)))
                .expect("write timeout");
            read_request_head(&mut socket);
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

    const BUDGET: Duration = Duration::from_secs(5);
    const SHORT_BUDGET: Duration = Duration::from_millis(300);
    const NO_CONTEXT: PartitionContext = PartitionContext {
        incarnation: 0,
        owner_generation: 0,
        metadata_op: 0,
    };

    fn partition_request(hop: PartitionHop, budget: Duration) -> PartitionRequest {
        PartitionRequest {
            method: Method::POST,
            headers: HeaderMap::new(),
            path_and_query: SEND_PATH.to_owned(),
            body: Bytes::new(),
            hop,
            deadline: Instant::now() + budget,
        }
    }

    fn target() -> PartitionHop {
        PartitionHop::Target(PartitionRoute {
            partition_id: 0,
            context: NO_CONTEXT,
            budget: BUDGET,
        })
    }

    /// Serve `request` on a node that holds no partition, with a local attempt
    /// that answers `outcome` every time. Returns the answer, the local
    /// attempts and the primary lookups.
    async fn serve(
        request: &PartitionRequest,
        first: FirstAttempt,
        outcome: impl Fn() -> Result<Response, PartitionWriteError>,
    ) -> (Result<Response, PartitionWriteError>, u64, u64) {
        let state = read_state();
        let attempts = Cell::new(0);
        let served = serve_partition(
            &state,
            request,
            IggyNamespace::new(1, 1, 0),
            NO_CONTEXT,
            PartitionEffect::Write,
            first,
            async || {
                attempts.set(attempts.get() + 1);
                outcome()
            },
        )
        .await;
        let probes = state.metrics.partition_primary_probes.get();
        (served, attempts.get(), probes)
    }

    #[compio::test]
    async fn given_a_request_its_owner_serves_when_routing_should_take_no_primary_lookup() {
        for hop in [PartitionHop::Contacted, target(), PartitionHop::Unrouted] {
            let (served, attempts, probes) =
                serve(&partition_request(hop, BUDGET), FirstAttempt::Local, || {
                    Ok(StatusCode::CREATED.into_response())
                })
                .await;
            assert_eq!(
                served.ok().map(|response| response.status()),
                Some(StatusCode::CREATED),
                "{hop:?}"
            );
            assert_eq!((attempts, probes), (1, 0), "{hop:?}");
        }
    }

    #[compio::test]
    async fn given_an_outcome_that_is_not_a_gate_refusal_when_routing_should_answer_it_once() {
        let missing_reply = || Err(PartitionWriteError::Timeout(Operation::SendMessages));
        let (served, attempts, probes) = serve(
            &partition_request(PartitionHop::Contacted, BUDGET),
            FirstAttempt::Local,
            missing_reply,
        )
        .await;
        assert!(matches!(served, Err(PartitionWriteError::Timeout(_))));
        assert_eq!((attempts, probes), (1, 0));

        let maybe_committed = || {
            Err(PartitionWriteError::Rejected(
                IggyError::TransientNotCommitted,
            ))
        };
        let (served, attempts, probes) = serve(
            &partition_request(PartitionHop::Contacted, BUDGET),
            FirstAttempt::Local,
            maybe_committed,
        )
        .await;
        assert!(matches!(
            served,
            Err(PartitionWriteError::Rejected(
                IggyError::TransientNotCommitted
            ))
        ));
        assert_eq!((attempts, probes), (1, 0));

        let refused = || Err(PartitionWriteError::Rejected(IggyError::NotPrimary));
        let (served, attempts, probes) = serve(
            &partition_request(target(), BUDGET),
            FirstAttempt::Local,
            refused,
        )
        .await;
        let Ok(response) = served else {
            panic!("a hop target answers its owner's refusal itself");
        };
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            !response.headers().contains_key(PARTITION_PRIMARY_HEADER),
            "a target without a primary in view names none"
        );
        assert_eq!(
            (attempts, probes),
            (1, 1),
            "a hop target looks its view up for the refusal and never relays"
        );
    }

    #[test]
    fn partition_primary_header_round_trips_and_refuses_malformed_values() {
        let primary = PartitionPrimary {
            view: 7,
            replica: 2,
        };
        let refusal = not_primary(Some(primary));
        let header = refusal
            .headers()
            .get(PARTITION_PRIMARY_HEADER)
            .expect("the refusal names its primary");
        assert_eq!(header, "7,2");
        assert_eq!(header.to_str().unwrap().parse(), Ok(primary));
        assert!(
            !not_primary(None)
                .headers()
                .contains_key(PARTITION_PRIMARY_HEADER)
        );
        for malformed in [
            "", "7", "7,", ",2", "7,2,1", "7,256", "-1,2", "x,2", "+7,2", "7,+2", "07,2",
        ] {
            assert_eq!(
                malformed.parse::<PartitionPrimary>(),
                Err(IggyError::InvalidCommand),
                "{malformed:?}"
            );
        }
    }

    #[test]
    fn given_a_refusal_hint_when_choosing_the_next_hop_should_follow_only_a_newer_view() {
        let relayed_by = PartitionPrimary {
            view: 5,
            replica: 1,
        };
        let at = |view| PartitionPrimary { view, replica: 2 };
        assert_eq!(relayed_by.newer(Some(at(6))), Some(at(6)));
        assert_eq!(relayed_by.newer(Some(at(5))), None);
        assert_eq!(relayed_by.newer(Some(at(4))), None);
        assert_eq!(relayed_by.newer(None), None);
    }

    #[test]
    fn given_a_not_primary_refusal_with_a_hint_when_classifying_should_retry_by_it() {
        let hint = PartitionPrimary {
            view: 9,
            replica: 0,
        };
        let body = Bytes::from(format!(
            r#"{{"id":{},"code":"x","reason":"x","field":null}}"#,
            IggyError::NotPrimary.as_code()
        ));
        let outcome = classify_reply(
            StatusCode::SERVICE_UNAVAILABLE,
            Vec::new(),
            body,
            RelayKind::Partition(PartitionEffect::Write),
            Some(hint),
        );
        assert!(matches!(outcome, AttemptOutcome::Retry(Some(retry)) if retry == hint));
        let outcome = classify_reply(
            StatusCode::CREATED,
            Vec::new(),
            Bytes::new(),
            RelayKind::Partition(PartitionEffect::Write),
            Some(hint),
        );
        assert!(
            matches!(outcome, AttemptOutcome::Relay(_)),
            "a hint never turns an answer into a retry"
        );
    }

    #[compio::test]
    async fn given_a_gate_refusal_without_a_primary_in_view_when_routing_should_wait_for_one_until_the_deadline()
     {
        for refusal in [IggyError::NotPrimary, IggyError::TransientNotAccepted] {
            let refused = || Err(PartitionWriteError::Rejected(refusal.clone()));
            let (served, attempts, probes) = serve(
                &partition_request(PartitionHop::Contacted, SHORT_BUDGET),
                FirstAttempt::Local,
                refused,
            )
            .await;
            let Ok(response) = served else {
                panic!("the deadline answers a {refusal:?} refusal");
            };
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert!(response.headers().contains_key(RETRY_AFTER));
            assert_eq!(
                attempts, 1,
                "a {refusal:?} refusal must not run again before a primary is in view"
            );
            assert!(
                probes > 1,
                "the lookup must wait for a view, looked {probes} times"
            );
        }
    }

    #[compio::test]
    async fn given_a_write_that_would_register_a_session_here_when_routing_should_look_the_primary_up_first()
     {
        let created = || Ok(StatusCode::CREATED.into_response());
        let (served, attempts, probes) = serve(
            &partition_request(PartitionHop::Contacted, SHORT_BUDGET),
            FirstAttempt::Lookup,
            created,
        )
        .await;
        let Ok(response) = served else {
            panic!("the deadline answers a write that found no primary");
        };
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            attempts, 0,
            "the write must not run here before a lookup names this replica"
        );
        assert!(
            probes > 1,
            "the lookup must wait for a view, looked {probes} times"
        );

        for hop in [target(), PartitionHop::Unrouted] {
            let (served, attempts, probes) = serve(
                &partition_request(hop, BUDGET),
                FirstAttempt::Lookup,
                created,
            )
            .await;
            assert_eq!(
                served.ok().map(|response| response.status()),
                Some(StatusCode::CREATED),
                "{hop:?}"
            );
            assert_eq!((attempts, probes), (1, 0), "{hop:?} serves first");
        }
    }
}
