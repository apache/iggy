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
//! metadata primary.
//!
//! A load balancer that round-robins HTTP across the cluster lands writes and
//! linearizable reads on followers, which cannot serve them: a follower can
//! neither Register a VSR session nor commit a control-plane op. Instead of
//! failing those requests with a transient 503, the middleware here re-issues
//! them against the current primary's HTTP listener and relays the primary's
//! response on the original connection, so any node answers any request.
//!
//! Control-plane routes share the metadata primary as their forward target.
//! Partition-write routes use a separate fallback: after a typed
//! `TransientNotAccepted` response, walk the roster until the retry deadline.
//! That denial proves the operation never entered a partition pipeline.
//! Partition primaries can differ from the metadata primary, so this fallback
//! cannot use the metadata leader as its sole target.
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
//! - A forwarded request is retried only when it provably never entered the
//!   primary's pipeline: a connect-phase failure, a 503 whose body carries the
//!   `TransientNotAccepted` code, or a 307 from a stale target. Every other
//!   outcome - including a 503 carrying `TransientNotCommitted`, whose op may
//!   still commit - is relayed as-is, because re-issuing it under a fresh
//!   session would defeat the consensus dedup and double-apply the op.
//! - The target is always resolved from the local roster + consensus view,
//!   never from a response `Location`, and the client follows no redirects.

use std::cell::Cell;
use std::net::SocketAddr;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::{Body, to_bytes};
use axum::extract::{Query, Request, State};
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE, RETRY_AFTER};
use axum::http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use configs::http::HttpTlsConfig;
use consensus::MetadataHandle;
use futures::{Stream, StreamExt};
use iggy_common::IggyError;
use message_bus::transports::tls::load_pem;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, WebPkiSupportedAlgorithms};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{CertificateError, DigitallySignedStruct, SignatureScheme};
use send_wrapper::SendWrapper;
use serde::Deserialize;
use tracing::{debug, warn};

use crate::http::HttpState;
use crate::http::error::{
    CustomError, error_response, gateway_timeout_response, primary_http_socket, with_retry_after,
};
use crate::http::extractor::{bearer_token, resolve_credential};
use crate::http::handlers::{DURABILITY_HEADER, DeferredPollQuery};
use crate::http::state::{APPLIED_OP_HEADER, ForwardState, HttpInner, VIEW_HEADER};
use crate::server_error::ServerError;

/// Marker stamped on every forwarded request. Loop guard only: a node that is
/// not primary and sees it answers the transient 503 instead of forwarding
/// again, so a stale view can never chain hops. It is client-spoofable by
/// design - spoofing it at a follower is a self-inflicted 503, and the primary
/// ignores it - and it must never gate auth, authz, or admission.
const FORWARDED_HEADER: HeaderName = HeaderName::from_static("iggy-forwarded");
const FORWARDED_VALUE: HeaderValue = HeaderValue::from_static("1");

/// Wall-clock bound on one forward attempt, including buffered replies but
/// only up to the headers for a streamed poll. Above the primary's own 30s
/// in-flight transient replay budget, so a
/// legitimately slow commit is answered rather than cut mid-flight; without
/// this cap a hung primary would park the connection for the whole retry
/// budget with no per-attempt bound (the HTTP client itself has no timeout).
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

/// Route-layer fallback for acknowledged partition writes over HTTP.
///
/// HTTP has no persistent leader-aware connection to retarget. Execute on the
/// contacted node first, then retry across the configured HTTP nodes within
/// the deadline after a typed `TransientNotAccepted` denial. That
/// denial proves the write never entered a partition pipeline. Every ambiguous
/// outcome is returned without replay.
pub(in crate::http) async fn forward_partition_write(
    State(state): State<HttpState>,
    request: Request,
    next: Next,
) -> Response {
    SendWrapper::new(forward_partition_or_pass(state, request, next)).await
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
        Ok((_key, user_id, _expiry)) => user_id,
        Err(rejection) => return rejection.into_response(),
    };
    let Some(_guard) = ForwardGuard::admit(&state.forward.in_flight) else {
        return with_retry_after(error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "forward_busy",
            "node is at its forward budget; retry with backoff",
        ));
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

async fn forward_partition_or_pass(state: HttpState, request: Request, next: Next) -> Response {
    if !state.forward.active || request.headers().contains_key(FORWARDED_HEADER) {
        return next.run(request).await;
    }
    // A read that does not move consumer progress is answered by whichever
    // replica the caller reached, which is the point of reading from one. Only
    // an automatic commit needs a replica that can originate the offset
    // operation, so only that poll pays for the buffering and the credential
    // resolution below.
    if request.method() == Method::GET && !wants_auto_commit(request.uri().query()) {
        return next.run(request).await;
    }
    let deferred_deadline = deferred_poll_deadline(&request);
    let bearer = match bearer_token(request.headers()) {
        Ok(bearer) => bearer,
        Err(error) => return CustomError::from(error).into_response(),
    };
    if let Err(rejection) = resolve_credential(&state, bearer).await {
        return rejection.into_response();
    }

    let (parts, request_body) = request.into_parts();
    let Ok(body) = to_bytes(request_body, state.forward.body_limit).await else {
        return error_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            "payload_too_large",
            "request body exceeds http.max_request_size",
        );
    };
    let method = parts.method.clone();
    let request_headers = parts.headers.clone();
    let path_and_query = parts
        .uri
        .path_and_query()
        .map_or("/", |path_and_query| path_and_query.as_str())
        .to_owned();
    let local = next
        .run(Request::from_parts(parts, Body::from(body.clone())))
        .await;
    if let AttemptOutcome::Relay(response) = classify_local_partition_reply(local).await {
        return response;
    }

    let Some(guard) = ForwardGuard::admit(&state.forward.in_flight) else {
        return with_retry_after(error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "forward_busy",
            "node is at its forward budget; retry with backoff",
        ));
    };
    let self_id = state
        .shard
        .plane
        .metadata()
        .consensus
        .as_ref()
        .map(consensus::VsrConsensus::replica);
    let deadline = deferred_deadline.map_or_else(
        || Instant::now() + FORWARD_RETRY_DEADLINE,
        |deadline| deadline.request,
    );
    let mut skip_node = self_id;
    loop {
        for socket in partition_http_sockets(&state.roster, skip_node) {
            if Instant::now() >= deadline {
                break;
            }
            let url = format!("{}://{socket}{path_and_query}", state.forward.scheme);
            match attempt(
                &state,
                &method,
                &request_headers,
                &body,
                &url,
                false,
                deferred_deadline,
            )
            .await
            {
                AttemptOutcome::Relay(response) => {
                    return if method == Method::GET && response.status().is_success() {
                        retain_forward_guard(
                            response,
                            guard,
                            deferred_deadline.map_or(FORWARD_ATTEMPT_TIMEOUT, |deadline| {
                                deadline.request.saturating_duration_since(Instant::now())
                            }),
                        )
                    } else {
                        response
                    };
                }
                AttemptOutcome::Retry => {}
            }
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining <= FORWARD_RETRY_INTERVAL {
            break;
        }
        // The local replica may become primary during a view change. Its
        // forwarded marker prevents another roster walk on the loopback hop.
        skip_node = None;
        compio::time::sleep(FORWARD_RETRY_INTERVAL).await;
    }

    with_retry_after(CustomError::from(IggyError::TransientNotAccepted).into_response())
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
                    &parts.headers,
                    &body,
                    &url,
                    true,
                    None,
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
    /// The request provably never entered a pipeline; re-resolve and retry.
    Retry,
}

/// Bound connect, send and buffered replies by [`FORWARD_ATTEMPT_TIMEOUT`].
/// Successful polls return after their headers and have a separate body deadline.
async fn attempt(
    state: &HttpInner,
    method: &Method,
    request_headers: &HeaderMap,
    body: &Bytes,
    url: &str,
    retry_redirect: bool,
    deferred_deadline: Option<DeferredPollDeadline>,
) -> AttemptOutcome {
    let builder = match state.forward.client.request(method.clone(), url) {
        Ok(builder) => builder,
        Err(error) => {
            warn!(%error, "forward request build failed");
            return AttemptOutcome::Relay(bad_gateway());
        }
    };
    let mut request = builder
        .headers(forwarded_headers(request_headers))
        .body(body.clone())
        .build();
    let timeout = match deferred_deadline {
        Some(deadline) => {
            let Some(remaining) = reduce_deferred_wait(&mut request, deadline) else {
                return AttemptOutcome::Retry;
            };
            remaining
        }
        None => FORWARD_ATTEMPT_TIMEOUT,
    };
    let attempt = async {
        let response = match state.forward.client.execute(request).await {
            Ok(response) => response,
            Err(error) => return classify_transport_error(&error),
        };
        classify_forwarded_reply(
            response,
            method,
            retry_redirect,
            deferred_deadline.map(|deadline| deadline.request),
        )
        .await
    };
    match compio::time::timeout(timeout, attempt).await {
        // Elapsed: the request may be mid-commit on the primary. Outcome
        // unknown, so never retried - 504, same contract as a local commit
        // wait that timed out.
        Err(_elapsed) => AttemptOutcome::Relay(gateway_timeout_response(
            "forward_timeout",
            "the primary did not answer the forwarded request in time; the outcome is unknown",
        )),
        Ok(outcome) => outcome,
    }
}

#[derive(Clone, Copy)]
struct DeferredPollDeadline {
    wait: Instant,
    request: Instant,
}

fn deferred_poll_deadline(request: &Request) -> Option<DeferredPollDeadline> {
    if request.method() != Method::GET || !request.uri().path().ends_with("/messages/deferred") {
        return None;
    }
    let Query(query) = Query::<DeferredPollQuery>::try_from_uri(request.uri()).ok()?;
    let options = iggy_common::DeferredPollOptions::from(query);
    options.validate(options.min_count).ok()?;
    let now = Instant::now();
    Some(DeferredPollDeadline {
        wait: now.checked_add(options.max_wait.get_duration())?,
        request: now.checked_add(options.request_timeout.get_duration())?,
    })
}

fn reduce_deferred_wait(
    request: &mut cyper::Request,
    deadline: DeferredPollDeadline,
) -> Option<Duration> {
    let now = Instant::now();
    let remaining = deadline.request.checked_duration_since(now)?;
    let wait_us = deadline.wait.saturating_duration_since(now).as_micros();
    if remaining.as_micros() == 0 {
        return None;
    }
    let query: Vec<_> = request
        .url()
        .query_pairs()
        .filter(|(key, _)| key != "wait_us" && key != "request_timeout_us")
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();
    request
        .url_mut()
        .query_pairs_mut()
        .clear()
        .extend_pairs(query)
        .append_pair("wait_us", &wait_us.to_string())
        .append_pair("request_timeout_us", &remaining.as_micros().to_string());
    Some(remaining)
}

async fn classify_forwarded_reply(
    response: cyper::Response,
    method: &Method,
    retry_redirect: bool,
    deadline: Option<Instant>,
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
    if method == Method::GET && !retry_redirect && status.is_success() {
        let mut response = Response::new(stream_poll_body(response.bytes_stream(), deadline));
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
            Err(error) => {
                warn!(%error, "forward response body read failed; outcome unknown");
                return AttemptOutcome::Relay(bad_gateway());
            }
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
    classify_reply(status, relayed_headers, Bytes::from(body), retry_redirect)
}

fn stream_poll_body(
    stream: impl Stream<Item = Result<Bytes, cyper::Error>> + 'static,
    deadline: Option<Instant>,
) -> Body {
    let stream = futures::stream::try_unfold(Box::pin(stream), move |mut stream| async move {
        let timeout = deadline.map_or(FORWARD_ATTEMPT_TIMEOUT, |deadline| {
            deadline.saturating_duration_since(Instant::now())
        });
        match compio::time::timeout(timeout, stream.next()).await {
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

fn retain_forward_guard(response: Response, guard: ForwardGuard, timeout: Duration) -> Response {
    let (parts, body) = response.into_parts();
    let (sender, receiver) = async_channel::bounded(1);
    // The timer must run even when the downstream connection stops polling its
    // body. One queued chunk bounds read-ahead; dropping the body cancels the task.
    let task = compio::runtime::spawn(async move {
        let mut stream = body.into_data_stream();
        let relay = async {
            while let Some(chunk) = stream.next().await {
                if sender
                    .send(chunk.map_err(std::io::Error::other))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        };
        if compio::time::timeout(timeout, relay).await.is_err() {
            drop(stream);
            drop(guard);
            // Discard a queued chunk so a stalled reader still receives an
            // explicit body error instead of mistaking the timeout for EOF.
            let _ = sender.force_send(Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "forwarded poll response deadline exceeded",
            )));
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
/// client-supplied forward marker, which `forward_or_pass` already bounced -
/// is dropped, then the loop-guard marker is stamped fresh.
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

/// Grade a transport-level failure. Only a connect-phase error - the request
/// was never written - may retry; anything later (reset mid-request, a broken
/// body read) leaves the outcome unknown and must surface, because the
/// primary may have committed the op and a re-issue would double-apply it.
/// A pooled connection that dies before the request is written also lands in
/// the relay arm (hyper exposes no sound never-sent predicate): that spurious
/// 502 is the safe side, and hyper's own canceled-request retry for buffered
/// bodies absorbs most of it.
fn classify_transport_error(error: &cyper::Error) -> AttemptOutcome {
    if let cyper::Error::HyperClient(client_error) = error
        && client_error.is_connect()
    {
        debug!(%error, "forward connect failed; re-resolving primary");
        return AttemptOutcome::Retry;
    }
    warn!(%error, "forward transport error after connect; outcome unknown");
    AttemptOutcome::Relay(bad_gateway())
}

/// Grade a complete reply from the target.
///
/// A 307 means the target itself was not primary and knows a better one; the
/// retry re-resolves from the LOCAL view instead of trusting the `Location`.
/// A 503 is retried only when its body carries the `TransientNotAccepted`
/// code (never entered a pipeline; also what the hop guard answers) - a
/// `TransientNotCommitted` 503 may still commit and is relayed untouched.
/// Everything else is the primary's answer and is relayed.
fn classify_reply(
    status: StatusCode,
    relayed_headers: Vec<(HeaderName, HeaderValue)>,
    body: Bytes,
    retry_redirect: bool,
) -> AttemptOutcome {
    if retry_redirect && status == StatusCode::TEMPORARY_REDIRECT {
        return AttemptOutcome::Retry;
    }
    if status == StatusCode::SERVICE_UNAVAILABLE && is_transient_not_accepted_body(&body) {
        return AttemptOutcome::Retry;
    }
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    for (name, value) in relayed_headers {
        response.headers_mut().insert(name, value);
    }
    AttemptOutcome::Relay(response)
}

/// Inspect a response produced on this node without changing any terminal
/// response. Only the typed never-admitted denial opens the roster fallback.
async fn classify_local_partition_reply(response: Response) -> AttemptOutcome {
    if response.status() != StatusCode::SERVICE_UNAVAILABLE {
        return AttemptOutcome::Relay(response);
    }
    let (parts, body) = response.into_parts();
    let body = match to_bytes(body, RESPONSE_BODY_LIMIT).await {
        Ok(body) => body,
        Err(error) => {
            warn!(%error, "local partition response body read failed; outcome unknown");
            return AttemptOutcome::Relay(bad_gateway());
        }
    };
    if is_transient_not_accepted_body(&body) {
        return AttemptOutcome::Retry;
    }
    AttemptOutcome::Relay(Response::from_parts(parts, Body::from(body)))
}

/// True when a 503 body is the JSON `ErrorResponse` whose `id` is the
/// `TransientNotAccepted` code. Unparsable or foreign bodies are NOT
/// transient: when in doubt the reply is relayed, never retried.
fn is_transient_not_accepted_body(body: &[u8]) -> bool {
    #[derive(Deserialize)]
    struct ErrorId {
        id: u32,
    }
    serde_json::from_slice::<ErrorId>(body)
        .is_ok_and(|error| error.id == IggyError::TransientNotAccepted.as_code())
}

/// HTTP socket of the current metadata primary, from the live consensus view
/// and the static roster. `None` mid-election or when the roster has no HTTP
/// address for the primary.
fn primary_socket(state: &HttpInner) -> Option<SocketAddr> {
    let consensus = state.shard.plane.metadata().consensus.as_ref()?;
    let primary_index = consensus.primary_index(consensus.view());
    primary_http_socket(&state.roster, primary_index)
}

/// Private HTTP sockets for every other configured replica, in stable roster
/// order. The caller tries each once. HTTP-disabled entries are skipped
/// because they cannot accept the forwarded request.
fn partition_http_sockets(
    roster: &crate::cluster_meta::ClusterRoster,
    self_id: Option<u8>,
) -> Vec<SocketAddr> {
    roster
        .nodes
        .iter()
        .filter(|node| Some(node.config().replica_id) != self_id)
        .filter_map(|node| {
            Some(SocketAddr::new(
                node.replica_ip(),
                node.config().ports.http?,
            ))
        })
        .collect()
}

/// Whether a poll asks the server to store its offset after serving it.
///
/// Match the handler's URL decoding and boolean parsing. An invalid query
/// reaches the handler through the forwarding layer for its normal rejection.
fn wants_auto_commit(query: Option<&str>) -> bool {
    #[derive(Default, Deserialize)]
    struct AutoCommitQuery {
        #[serde(default)]
        auto_commit: bool,
    }
    query.is_some_and(|query| {
        let Ok(uri) = format!("/?{query}").parse() else {
            return true;
        };
        Query::<AutoCommitQuery>::try_from_uri(&uri).map_or(true, |Query(query)| query.auto_commit)
    })
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

    use configs::cluster::{ClusterNodeConfig, ResolvedClusterNode, TransportPorts};

    fn node(replica_id: u8, ip: &str, http: Option<u16>) -> ClusterNodeConfig {
        ClusterNodeConfig {
            name: format!("node-{replica_id}"),
            ip: ip.to_owned(),
            advertised_address: None,
            advertised_addresses: Vec::new(),
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

    fn roster(nodes: Vec<ClusterNodeConfig>) -> crate::cluster_meta::ClusterRoster {
        crate::cluster_meta::ClusterRoster {
            enabled: true,
            name: "test-cluster".to_owned(),
            nodes: nodes
                .into_iter()
                .map(|node| ResolvedClusterNode::try_from(node).expect("valid roster node"))
                .collect(),
            self_advertised: "127.0.0.1".to_owned(),
            configured_ports: TransportPorts::default(),
            bound_ports: Arc::default(),
            metadata_view: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(
                crate::cluster_meta::METADATA_VIEW_UNKNOWN,
            )),
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
    fn auto_commit_query_matches_handler_decoding() {
        assert!(wants_auto_commit(Some("auto_commit=true")));
        assert!(wants_auto_commit(Some("auto_commit=1")));
        assert!(wants_auto_commit(Some("count=10&auto_commit=true")));
        assert!(wants_auto_commit(Some("auto_commit=yes")));
        assert!(!wants_auto_commit(Some("auto_commit=false")));
        assert!(wants_auto_commit(Some("auto_commit=0")));
        assert!(!wants_auto_commit(Some("count=10")));
        assert!(!wants_auto_commit(None));
        assert!(wants_auto_commit(Some("%61uto_commit=true")));
        assert!(wants_auto_commit(Some("auto_commit=%74rue")));
        assert!(!wants_auto_commit(Some("%61uto_commit=%66alse")));
    }

    #[test]
    fn deferred_forward_keeps_one_wait_budget_and_preserves_other_query_fields() {
        let mut request = cyper::Request::new(
            Method::GET,
            "http://localhost/streams/0/topics/0/messages/deferred?%77ait_us=2000000&auto_commit=true&consistency=linearizable"
                .parse()
                .unwrap(),
        );
        let remaining = reduce_deferred_wait(
            &mut request,
            DeferredPollDeadline {
                wait: Instant::now() + Duration::from_secs(1),
                request: Instant::now() + Duration::from_secs(1),
            },
        )
        .unwrap();
        let query: Vec<_> = request.url().query_pairs().collect();
        let waits: Vec<_> = query.iter().filter(|(key, _)| key == "wait_us").collect();
        assert_eq!(waits.len(), 1);
        assert!(waits[0].1.parse::<u128>().unwrap() <= remaining.as_micros());
        assert!(remaining.as_micros() > 0 && remaining <= Duration::from_secs(1));
        assert!(
            query
                .iter()
                .any(|(key, value)| key == "auto_commit" && value == "true")
        );
        assert!(
            query
                .iter()
                .any(|(key, value)| key == "consistency" && value == "linearizable")
        );
        let previous = request.url().clone();
        assert!(
            reduce_deferred_wait(
                &mut request,
                DeferredPollDeadline {
                    wait: Instant::now(),
                    request: Instant::now()
                }
            )
            .is_none()
        );
        assert_eq!(
            request.url(),
            &previous,
            "request timeout must stop forwarding"
        );
    }

    #[test]
    fn forwarding_extends_only_valid_deferred_poll_requests() {
        for (path, deferred) in [
            (
                "/streams/0/topics/0/messages/deferred?wait_us=1000000",
                true,
            ),
            ("/streams/0/topics/0/messages?wait_us=1000000", false),
            ("/streams/0/topics/0/messages/deferred", true),
            ("/streams/0/topics/0/messages/deferred?wait_us=0", true),
            (
                "/streams/0/topics/0/messages/deferred?wait_us=600000001",
                false,
            ),
        ] {
            let request = Request::builder().uri(path).body(Body::empty()).unwrap();
            assert_eq!(
                deferred_poll_deadline(&request).is_some(),
                deferred,
                "{path}"
            );
        }
    }

    #[test]
    fn transient_not_accepted_body_matches_only_its_code() {
        let accepted = format!(
            r#"{{"id":{},"code":"transient_not_accepted","reason":"x","field":null}}"#,
            IggyError::TransientNotAccepted.as_code()
        );
        let committed = format!(
            r#"{{"id":{},"code":"transient_not_committed","reason":"x","field":null}}"#,
            IggyError::TransientNotCommitted.as_code()
        );
        assert!(is_transient_not_accepted_body(accepted.as_bytes()));
        assert!(!is_transient_not_accepted_body(committed.as_bytes()));
        assert!(!is_transient_not_accepted_body(b"not json"));
        assert!(!is_transient_not_accepted_body(b"{}"));
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

    #[test]
    fn partition_roster_walk_skips_self_and_undialable_nodes_once() {
        let roster = roster(vec![
            node(0, "10.0.0.1", Some(8080)),
            node(1, "10.0.0.2", Some(8081)),
            node(2, "10.0.0.3", None),
        ]);

        assert_eq!(
            partition_http_sockets(&roster, Some(0)),
            vec!["10.0.0.2:8081".parse().expect("valid socket")]
        );
    }

    #[compio::test]
    async fn partition_fallback_opens_only_for_typed_never_admitted_reply() {
        let retry = classify_local_partition_reply(
            CustomError::from(IggyError::TransientNotAccepted).into_response(),
        )
        .await;
        assert!(matches!(retry, AttemptOutcome::Retry));

        let terminal = classify_local_partition_reply(
            CustomError::from(IggyError::TransientNotCommitted).into_response(),
        )
        .await;
        let AttemptOutcome::Relay(terminal) = terminal else {
            panic!("an ambiguous commit outcome must never be retried")
        };
        assert_eq!(terminal.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[compio::test]
    async fn successful_local_poll_body_is_not_collected_or_size_limited() {
        let polled = Rc::new(Cell::new(0));
        let observed = Rc::clone(&polled);
        let chunk = Bytes::from(vec![0; 1024 * 1024]);
        let stream = futures::stream::iter((0..65).map(move |_| {
            observed.set(observed.get() + 1);
            Ok::<_, std::io::Error>(chunk.clone())
        }));
        let response = Response::new(Body::from_stream(SendWrapper::new(stream)));
        let AttemptOutcome::Relay(response) = classify_local_partition_reply(response).await else {
            panic!("successful response must not be retried")
        };
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(polled.get(), 0);
        let mut stream = response.into_body().into_data_stream();
        let mut received = 0;
        while let Some(chunk) = stream.next().await {
            received += chunk.expect("successful chunk").len();
        }
        assert_eq!(received, 65 * 1024 * 1024);
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
            Response::new(stream_poll_body(stream, None)),
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
            Response::new(stream_poll_body(futures::stream::pending(), None)),
            guard,
            FORWARD_ATTEMPT_TIMEOUT,
        );
        assert_eq!(in_flight.get(), 1);
        drop(response);
        compio::time::sleep(CANCEL_TURN).await;
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
        let response = retain_forward_guard(
            Response::new(stream_poll_body(stream, None)),
            guard,
            BODY_TIMEOUT,
        );
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
        let AttemptOutcome::Relay(response) =
            classify_forwarded_reply(response, &Method::GET, false, None).await
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
