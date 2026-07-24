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
//! Scope: the middleware is attached (via `route_layer`) only to the
//! control-plane routes, whose ops all commit through the metadata consensus
//! group and therefore share one forward target. Partition-plane writes
//! (produce, consumer-offset writes) are excluded: each partition is its own
//! consensus group whose primary can diverge from the metadata primary, so
//! forwarding them needs per-group target resolution.
//!
//! Safety model, in order:
//! - The bearer is verified locally (verify-only, no session mint) before any
//!   bytes leave this node, so an unauthenticated caller cannot make a
//!   follower relay junk to the primary.
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
use std::net::{IpAddr, SocketAddr};
use std::path::Path;
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
use consensus::MetadataHandle;
use iggy_common::IggyError;
use message_bus::transports::tls::{install_default_crypto_provider, load_pem};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, WebPkiSupportedAlgorithms};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{CertificateError, DigitallySignedStruct, SignatureScheme};
use send_wrapper::SendWrapper;
use serde::Deserialize;
use tracing::{debug, warn};

use crate::http::HttpState;
use crate::http::error::{CustomError, error_response, gateway_timeout_response, with_retry_after};
use crate::http::extractor::{bearer_token, resolve_credential};
use crate::http::state::{HttpInner, VIEW_HEADER};
use crate::server_error::ServerNgError;

/// Marker stamped on every forwarded request. Loop guard only: a node that is
/// not primary and sees it answers the transient 503 instead of forwarding
/// again, so a stale view can never chain hops. It is client-spoofable by
/// design - spoofing it at a follower is a self-inflicted 503, and the primary
/// ignores it - and it must never gate auth, authz, or admission.
const FORWARDED_HEADER: HeaderName = HeaderName::from_static("x-iggy-forwarded");
const FORWARDED_VALUE: HeaderValue = HeaderValue::from_static("1");

/// Wall-clock bound on one forward attempt (send + primary processing + body
/// read). Above the primary's own 30s in-flight transient replay budget, so a
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

/// Bound on a relayed response body. Forwarded routes answer entity JSON, not
/// message batches (poll and snapshot are served locally), so this is a
/// backstop against buffering an unbounded body, not a working limit.
const RESPONSE_BODY_LIMIT: usize = 64 * 1024 * 1024;

/// Response headers copied from the primary's reply. Everything else is
/// dropped, which subsumes the RFC 7230 hop-by-hop set: the relayed response
/// is rebuilt, never streamed, so upstream `connection` / `transfer-encoding`
/// semantics cannot leak to the client. `x-iggy-view` is included so the
/// relayed response carries the serving primary's view, not this follower's
/// (the view layer only fills the header when absent).
const RELAYED_RESPONSE_HEADERS: [HeaderName; 3] = [CONTENT_TYPE, RETRY_AFTER, VIEW_HEADER];

/// Per-node forwarding context hung off `HttpInner`: the outbound client
/// (pinned-cert TLS when the listener serves HTTPS), the scheme it dials, the
/// request-body buffer bound, and the in-flight budget.
pub(in crate::http) struct ForwardState {
    client: cyper::Client,
    scheme: &'static str,
    body_limit: usize,
    in_flight: Cell<u32>,
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
/// [`ServerNgError::ListenerCredentials`] when TLS is enabled but the PEM
/// files cannot be loaded, [`ServerNgError::HttpForwardClient`] when the
/// outbound client cannot be built.
pub(in crate::http) fn build_forward_state(
    tls: &HttpTlsConfig,
    body_limit: usize,
) -> Result<ForwardState, ServerNgError> {
    // Unconditional: cyper's rustls connector resolves the process default
    // provider even when the client only ever dials plain http.
    install_default_crypto_provider();
    let builder = cyper::Client::builder()
        // The retry loop re-resolves the primary from the local roster; a
        // followed `Location` would let the peer steer the bearer anywhere.
        .redirect(cyper::redirect::Policy::none());
    let (builder, scheme) =
        if tls.enabled {
            let credentials = load_pem(Path::new(&tls.cert_file), Path::new(&tls.key_file))
                .map_err(|source| ServerNgError::ListenerCredentials {
                    transport: "http.tls",
                    source,
                })?;
            let algorithms = CryptoProvider::get_default()
                // Installed above; absence is unreachable.
                .expect("default crypto provider installed")
                .signature_verification_algorithms;
            let config = rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(PinnedCertVerifier {
                    pinned: credentials.cert_chain,
                    algorithms,
                }))
                .with_no_client_auth();
            (builder.use_rustls(Arc::new(config)), "https")
        } else {
            (builder, "http")
        };
    let client = builder
        .build()
        .map_err(|source| ServerNgError::HttpForwardClient {
            reason: source.to_string(),
        })?;
    Ok(ForwardState {
        client,
        scheme,
        body_limit,
        in_flight: Cell::new(0),
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

async fn forward_or_pass(state: HttpState, request: Request, next: Next) -> Response {
    if state.is_metadata_primary() {
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
    if let Err(rejection) = resolve_credential(&state, bearer).await {
        return rejection.into_response();
    }
    let Some(_guard) = ForwardGuard::admit(&state.forward.in_flight) else {
        return with_retry_after(error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "forward_busy",
            "node is at its forward budget; retry with backoff",
        ));
    };
    forward(&state, request).await
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
        let outcome = match primary_base(state) {
            // No resolvable primary (mid-election, or a roster hole): count it
            // as a retryable attempt so a completing election is picked up.
            None => AttemptOutcome::Retry,
            Some(base) => {
                let url = format!("{base}{path_and_query}");
                attempt(state, &parts, &body, &url).await
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

/// Run one forward attempt end to end (connect, send, read the full reply)
/// under [`FORWARD_ATTEMPT_TIMEOUT`].
async fn attempt(state: &HttpInner, parts: &Parts, body: &Bytes, url: &str) -> AttemptOutcome {
    let builder = match state.forward.client.request(parts.method.clone(), url) {
        Ok(builder) => builder,
        Err(error) => {
            warn!(%error, "forward request build failed");
            return AttemptOutcome::Relay(bad_gateway());
        }
    };
    let request = builder
        .headers(forwarded_headers(&parts.headers))
        .body(body.clone())
        .build();
    let attempt = async {
        let response = match state.forward.client.execute(request).await {
            Ok(response) => response,
            Err(error) => return classify_transport_error(&error),
        };
        let status = response.status();
        let headers = response.headers().clone();
        // Declared-length backstop only: the peer is an authenticated cluster
        // node serving sized JSON bodies, so a length-less runaway body is
        // bounded by the attempt timeout rather than a streaming limit.
        if response
            .content_length()
            .is_some_and(|length| length > RESPONSE_BODY_LIMIT as u64)
        {
            warn!(declared = ?response.content_length(), "relayed response exceeds the body bound");
            return AttemptOutcome::Relay(bad_gateway());
        }
        match response.bytes().await {
            Ok(body) => classify_reply(status, &headers, body),
            Err(error) => {
                warn!(%error, "forward response body read failed; outcome unknown");
                AttemptOutcome::Relay(bad_gateway())
            }
        }
    };
    match compio::time::timeout(FORWARD_ATTEMPT_TIMEOUT, attempt).await {
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
fn classify_reply(status: StatusCode, headers: &HeaderMap, body: Bytes) -> AttemptOutcome {
    if status == StatusCode::TEMPORARY_REDIRECT {
        return AttemptOutcome::Retry;
    }
    if status == StatusCode::SERVICE_UNAVAILABLE && is_transient_not_accepted_body(&body) {
        return AttemptOutcome::Retry;
    }
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    for name in RELAYED_RESPONSE_HEADERS {
        if let Some(value) = headers.get(&name) {
            response.headers_mut().insert(name, value.clone());
        }
    }
    AttemptOutcome::Relay(response)
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

/// `scheme://ip:http_port` of the current metadata primary, from the live
/// consensus view and the static roster. `None` mid-election or when the
/// roster has no HTTP address for the primary.
fn primary_base(state: &HttpInner) -> Option<String> {
    let consensus = state.shard.plane.metadata().consensus.as_ref()?;
    let primary_index = consensus.primary_index(consensus.view());
    let node = state
        .roster
        .nodes
        .iter()
        .find(|node| node.replica_id == primary_index)?;
    let http_port = node.ports.http?;
    let ip = node.ip.parse::<IpAddr>().ok()?;
    let socket = SocketAddr::new(ip, http_port);
    Some(format!("{}://{socket}", state.forward.scheme))
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
struct ForwardGuard<'a> {
    in_flight: &'a Cell<u32>,
}

impl<'a> ForwardGuard<'a> {
    fn admit(in_flight: &'a Cell<u32>) -> Option<Self> {
        if in_flight.get() >= MAX_IN_FLIGHT_FORWARDS {
            return None;
        }
        in_flight.set(in_flight.get() + 1);
        Some(Self { in_flight })
    }
}

impl Drop for ForwardGuard<'_> {
    fn drop(&mut self) {
        self.in_flight.set(self.in_flight.get() - 1);
    }
}

/// Exact-DER pin against this node's own certificate chain. Presented-leaf
/// equality replaces chain building and name checks on purpose (config-listed
/// IPs rarely appear as SANs in operator certs); handshake signatures are
/// still verified with the provider's algorithms, so possession of the pinned
/// certificate's private key remains required.
#[derive(Debug)]
struct PinnedCertVerifier {
    pinned: Vec<CertificateDer<'static>>,
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
        if self.pinned.iter().any(|pinned| pinned == end_entity) {
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

    #[test]
    fn linearizable_query_detected_only_on_exact_pair() {
        assert!(wants_linearizable(Some("consistency=linearizable")));
        assert!(wants_linearizable(Some("foo=bar&consistency=linearizable")));
        assert!(!wants_linearizable(Some("consistency=serializable")));
        assert!(!wants_linearizable(Some("consistency=LINEARIZABLE")));
        assert!(!wants_linearizable(None));
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
        let in_flight = Cell::new(0);
        let guards: Vec<_> = (0..MAX_IN_FLIGHT_FORWARDS)
            .map(|_| ForwardGuard::admit(&in_flight).expect("under cap"))
            .collect();
        assert!(ForwardGuard::admit(&in_flight).is_none());
        drop(guards);
        assert_eq!(in_flight.get(), 0);
        assert!(ForwardGuard::admit(&in_flight).is_some());
    }
}
