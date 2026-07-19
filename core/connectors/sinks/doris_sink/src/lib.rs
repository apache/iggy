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

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose};
use bytes::Bytes;
use humantime::Duration as HumanDuration;
use iggy_connector_sdk::retry::{exponential_backoff, jitter};
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Payload, Sink, TopicMetadata, sink_connector,
};
use reqwest::{Method, StatusCode, header};
use secrecy::zeroize::Zeroizing;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::time::Duration;
use tracing::{debug, error, info, warn};

sink_connector!(DorisSink);

const DEFAULT_LABEL_PREFIX: &str = "iggy";
const DEFAULT_BATCH_SIZE: u32 = 1000;
// Total per-request budget. Human-readable (e.g. "30s") to match the sibling
// web sinks (http_sink, influxdb_sink).
const DEFAULT_TIMEOUT: &str = "30s";
// Bounded TCP handshake timeout so an unreachable FE fails fast instead of
// burning the whole request-timeout budget on connect alone.
const DEFAULT_CONNECT_TIMEOUT: &str = "5s";
// Doris's FE Stream Load 307s to a BE, and reqwest strips Authorization on
// cross-host redirects, so we follow them manually. This caps a looping cluster.
const MAX_REDIRECTS: u8 = 5;
// Doris Stream Load labels must be 1..=128 chars of `[A-Za-z0-9_-]`. These caps
// keep the worst-case label well under that limit.
const MAX_LABEL_PREFIX_LEN: usize = 16;
const MAX_LABEL_NAME_LEN: usize = 16;
// A single 64-bit (16-hex) joint hash over the raw
// (prefix, table, stream, topic) identity. 64 bits keeps the
// adversarial birthday bound high enough that a
// multi-tenant namer can't cheaply force the label collisions that Doris's
// server-side dedupe would turn into silent data loss. One joint hash (not one
// per segment) buys that for the same length budget, leaving the sanitized names
// full-length.
const LABEL_HASH_HEX_LEN: usize = 16;
// Cap the response-body slice kept for logs/errors so a misbehaving proxy that
// returns a giant body can't flood the logs. Bounds only what we *log*, not peak
// memory — `response.text()` already buffers the full body first.
const MAX_RESPONSE_LOG_BYTES: usize = 4096;
// In-request retry budget for *transient* Stream Load failures (5xx/408/429,
// transport errors, and duplicate labels whose existing job is RUNNING or
// CANCELLED).
// The runtime commits the consumer
// offset at poll time before consume() runs, so a transient failure we don't
// retry here is lost — an in-request re-PUT under the same label (which Doris
// dedupes) is the connector's only redelivery lever. Keep the worst-case budget
// well inside Doris's label_keep_max_second so a retry still dedupes.
const DEFAULT_MAX_RETRIES: u32 = 3;
// Higher values are still honored, but warn because the retry loop runs inside
// an uncancellable consume() call and can substantially delay graceful shutdown.
const MAX_RETRIES_WARNING_THRESHOLD: u32 = 10;
const DEFAULT_RETRY_DELAY: &str = "200ms";
const DEFAULT_MAX_RETRY_DELAY: &str = "5s";

#[derive(Debug)]
pub struct DorisSink {
    id: u32,
    config: DorisSinkConfig,
    // Precomputed in `new()` and marked sensitive so reqwest keeps it out of any
    // debug/trace output (and never HPACK-indexes it on HTTP/2).
    auth_header: header::HeaderValue,
    // Set in `open()`, `None` until then. Holds the HTTP client, the parsed
    // Stream Load URL (which doubles as the redirect-validation baseline), the
    // precomputed/validated optional headers, and the resolved redirect policy.
    connected: Option<Connected>,
}

#[derive(Debug)]
struct Connected {
    client: reqwest::Client,
    base_url: reqwest::Url,
    // Optional Stream Load headers, validated once at `open()` so a bad byte
    // fails fast at startup rather than on every batch.
    max_filter_ratio_header: Option<header::HeaderValue>,
    columns_header: Option<header::HeaderValue>,
    where_header: Option<header::HeaderValue>,
    // Redirect policy resolved once at `open()` so `validate_redirect` reads it
    // off `self` instead of threading it through every call.
    allow_insecure_redirect: bool,
    allowed_redirect_hosts: Option<Vec<String>>,
    // In-request retry policy for transient Stream Load failures, resolved once
    // at `open()`. `max_retries` is the total attempt count (1 = no retry).
    max_retries: u32,
    retry_delay: Duration,
    max_retry_delay: Duration,
}

#[derive(Debug, Deserialize)]
pub struct DorisSinkConfig {
    pub fe_url: String,
    pub database: String,
    pub table: String,
    pub username: String,
    pub password: SecretString,
    pub label_prefix: Option<String>,
    pub max_filter_ratio: Option<f64>,
    pub columns: Option<String>,
    #[serde(rename = "where")]
    pub where_clause: Option<String>,
    /// Total per-request HTTP timeout as a human-readable duration, e.g. "30s"
    /// (default 30s). Matches the `timeout` field on the http/influxdb sinks.
    pub timeout: Option<String>,
    /// TCP connect timeout as a human-readable duration, e.g. "5s". Independent
    /// of `timeout` (the total request budget). Defaults to 5s; raise it for
    /// cross-region or cold-start FEs that are slow to accept the connection.
    pub connect_timeout: Option<String>,
    pub batch_size: Option<u32>,
    /// Total number of Stream Load attempts per batch on a *transient* failure
    /// (HTTP 5xx/408/429, a transport error, or a duplicate label whose existing
    /// job is `RUNNING` or `CANCELLED`). `0` or `1` disables retries. Each retry
    /// re-PUTs under the same label — which Doris dedupes — so an ambiguous
    /// success (e.g. a 2xx with a missing or unreadable body) is absorbed rather
    /// than doubled. Default 3. Values above 10 are honored but emit a startup
    /// warning because they can substantially delay graceful shutdown.
    pub max_retries: Option<u32>,
    /// Base backoff before the first retry, as a human-readable duration (e.g.
    /// "200ms"). Doubles each attempt up to `max_retry_delay`, with ±20% jitter.
    pub retry_delay: Option<String>,
    /// Strict upper bound on a single retry backoff, including jitter, as a
    /// human-readable duration (e.g. "5s").
    pub max_retry_delay: Option<String>,
    /// Permit a redirect that downgrades the scheme (e.g. `https://` FE ->
    /// `http://` BE). Off by default: a downgrade would push Basic-auth
    /// credentials onto a cleartext hop, so we refuse it unless the operator
    /// explicitly opts in for a known-insecure FE -> BE topology.
    pub allow_insecure_redirect: Option<bool>,
    /// Optional allowlist of hosts a Stream Load redirect may target. Each entry
    /// is `host` or `host:port`; a bare host pins only the host (any port), while
    /// `host:port` pins the exact endpoint. When set and non-empty, a redirect to
    /// any other target is refused — a hard lockdown against a compromised/MITM'd
    /// FE exfiltrating credentials via `Location`. When unset, cross-host
    /// redirects are allowed (required for the normal FE -> BE topology) subject
    /// only to the scheme-downgrade rule above.
    pub allowed_redirect_hosts: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct StreamLoadResponse {
    #[serde(rename = "Status")]
    status: String,
    #[serde(rename = "Message")]
    #[serde(default)]
    message: String,
    #[serde(rename = "NumberLoadedRows")]
    #[serde(default)]
    number_loaded_rows: u64,
    #[serde(rename = "NumberFilteredRows")]
    #[serde(default)]
    number_filtered_rows: u64,
    #[serde(rename = "ExistingJobStatus")]
    #[serde(default)]
    existing_job_status: Option<String>,
}

impl DorisSink {
    pub fn new(id: u32, config: DorisSinkConfig) -> Self {
        let credential = Zeroizing::new(format!(
            "{}:{}",
            config.username,
            config.password.expose_secret()
        ));
        let encoded = Zeroizing::new(general_purpose::STANDARD.encode(credential.as_bytes()));
        // `Basic <base64>` is always visible ASCII, so this conversion cannot fail.
        let auth_value = Zeroizing::new(format!("Basic {}", encoded.as_str()));
        let mut auth_header = header::HeaderValue::from_str(&auth_value)
            .expect("Basic auth header is always valid ASCII");
        auth_header.set_sensitive(true);

        DorisSink {
            id,
            config,
            auth_header,
            connected: None,
        }
    }

    fn build_client(&self) -> Result<reqwest::Client, Error> {
        let timeout = parse_request_duration(self.config.timeout.as_deref(), DEFAULT_TIMEOUT);
        let connect_timeout = parse_request_duration(
            self.config.connect_timeout.as_deref(),
            DEFAULT_CONNECT_TIMEOUT,
        );
        // `Policy::none()` so we follow redirects manually and keep Authorization
        // alive across the FE -> BE hop (reqwest strips it on cross-host 307s).
        reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(timeout)
            .connect_timeout(connect_timeout)
            .build()
            .map_err(|e| Error::InitError(format!("Failed to build Doris HTTP client: {e}")))
    }

    async fn send_stream_load(
        &self,
        connected: &Connected,
        label: &str,
        body: Bytes,
    ) -> Result<StreamLoadResponse, Error> {
        // `base_url` is the redirect-validation baseline (original FE scheme/host)
        // and the parsed first-hop target.
        let mut url = connected.base_url.clone();
        let mut redirects = 0u8;

        loop {
            let mut request = connected
                .client
                .request(Method::PUT, url.clone())
                .header(header::AUTHORIZATION, self.auth_header.clone())
                .header(header::EXPECT, "100-continue")
                .header("format", "json")
                .header("strip_outer_array", "true")
                .header("label", label)
                .body(body.clone());

            // Headers were validated and built once in `open()`.
            if let Some(value) = &connected.max_filter_ratio_header {
                request = request.header("max_filter_ratio", value.clone());
            }
            if let Some(value) = &connected.columns_header {
                request = request.header("columns", value.clone());
            }
            if let Some(value) = &connected.where_header {
                request = request.header("where", value.clone());
            }

            let response = request.send().await.map_err(|e| {
                error!("Doris sink ID {} HTTP request failed: {e}", self.id);
                Error::HttpRequestFailed(e.to_string())
            })?;

            let status = response.status();
            if matches!(
                status,
                StatusCode::TEMPORARY_REDIRECT | StatusCode::PERMANENT_REDIRECT
            ) {
                redirects += 1;
                if redirects > MAX_REDIRECTS {
                    // A redirect loop is permanent, not transient: retrying just
                    // re-walks the same loop, so surface it as such.
                    return Err(Error::PermanentHttpError(format!(
                        "Doris sink ID {} exceeded max redirects ({MAX_REDIRECTS})",
                        self.id
                    )));
                }
                let Some(location) = response
                    .headers()
                    .get(header::LOCATION)
                    .and_then(|v| v.to_str().ok())
                else {
                    // A redirect with no usable Location is malformed; retrying
                    // won't produce one.
                    return Err(Error::PermanentHttpError(format!(
                        "Doris sink ID {} got {status} with no Location header",
                        self.id
                    )));
                };
                // Doris always emits an *absolute* Location (the BE endpoint).
                // A relative one is outside that contract; resolving it against
                // the current URL would silently target a sibling path (a near-
                // certain 404) and give a false sense of safety, so reject it.
                let target = reqwest::Url::parse(location).map_err(|e| {
                    Error::PermanentHttpError(format!(
                        "Doris sink ID {} got {status} with non-absolute or unparsable Location '{location}': {e}",
                        self.id
                    ))
                })?;
                connected.validate_redirect(&target, self.id)?;
                debug!("Doris sink ID {} following redirect to {target}", self.id);
                url = target;
                continue;
            }

            let is_success = status.is_success();
            let response_text = match response.text().await {
                Ok(text) => text,
                Err(e) if is_success => {
                    // 2xx but the body never fully arrived (mid-stream TCP reset,
                    // decompression error, body-read timeout). Doris almost
                    // certainly persisted the load, but we can't read the row
                    // counts to confirm. Classify transient — not a fabricated
                    // parse failure — so a retry re-PUTs under the same label and
                    // Doris's dedupe reveals the real outcome instead of
                    // surfacing a committed load as a permanent failure.
                    warn!(
                        "Doris sink ID {} failed to read 2xx response body: {e}; treating as retryable",
                        self.id
                    );
                    return Err(Error::CannotStoreData(format!(
                        "Doris sink ID {} could not read 2xx Stream Load response body: {e}",
                        self.id
                    )));
                }
                Err(e) => {
                    // Non-2xx with an unreadable body: log it, then fall back to
                    // an empty body so the status-based handling below still
                    // lets the HTTP status mapping below determine whether the
                    // outcome is transient or permanent.
                    warn!(
                        "Doris sink ID {} failed to read response body: {e}",
                        self.id
                    );
                    String::new()
                }
            };
            let response_for_log = truncate_for_log(&response_text, MAX_RESPONSE_LOG_BYTES);

            if !is_success {
                let msg = format!(
                    "Doris sink ID {} stream load returned HTTP {status}: {response_for_log}",
                    self.id
                );
                error!("{msg}");
                // 408/429 are 4xx but transient, so include them in the bounded
                // in-request retry path.
                return Err(match status {
                    StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_MANY_REQUESTS => {
                        Error::CannotStoreData(msg)
                    }
                    s if s.is_server_error() => Error::CannotStoreData(msg),
                    _ => Error::PermanentHttpError(msg),
                });
            }

            return parse_stream_load_response(&response_text);
        }
    }

    /// Load one batch with bounded in-request retry. `send_stream_load` performs
    /// a single full FE -> BE attempt; this wraps it (plus status
    /// classification) so a *transient* failure re-PUTs under the same `label`.
    /// The runtime commits the consumer offset before consume() runs, so this is
    /// the connector's only redelivery path on a transient outage; the shared
    /// label lets Doris dedupe a prior attempt that actually landed (e.g. a 2xx
    /// with a missing or unreadable body). A `PermanentHttpError` returns
    /// immediately — retrying bad data would just hammer the FE.
    async fn load_batch(&self, label: &str, body: Bytes) -> Result<StreamLoadResponse, Error> {
        let connected = self.connected.as_ref().ok_or_else(|| {
            Error::InitError(format!(
                "Doris sink ID {} called before open() — not connected",
                self.id
            ))
        })?;

        let mut attempt = 0u32;
        loop {
            let error = match self
                .send_stream_load(connected, label, body.clone())
                .await
                .and_then(|response| classify_status(self.id, &response).map(|()| response))
            {
                Ok(response) => return Ok(response),
                Err(error) => error,
            };

            attempt += 1;
            if attempt >= connected.max_retries || !is_transient_error(&error) {
                return Err(error);
            }

            // `attempt` counts completed attempts. Subtract one so the first
            // retry waits exactly the configured base delay (base * 2^0).
            let delay = jitter(exponential_backoff(
                connected.retry_delay,
                attempt - 1,
                connected.max_retry_delay,
            ))
            .min(connected.max_retry_delay);
            warn!(
                "Doris sink ID {} transient Stream Load failure on attempt {attempt}/{} (label={label}): {error}; retrying in {delay:?}",
                self.id, connected.max_retries
            );
            tokio::time::sleep(delay).await;
        }
    }
}

impl Connected {
    /// Validate a Stream Load redirect target before re-attaching credentials.
    ///
    /// Doris's FE legitimately redirects (307) to a BE on a *different host*, so
    /// we can't require same-host. Instead we enforce three rules that close the
    /// credential-exfiltration vector a compromised/MITM'd FE would otherwise have:
    ///
    ///   0. The target scheme must be `http` or `https`. A non-HTTP scheme
    ///      (`ftp`, `file`, ...) would slip past the downgrade rule when the FE
    ///      itself is `http`, so reject it before re-attaching credentials.
    ///   1. No scheme downgrade (`https` -> `http`) unless `allow_insecure_redirect`
    ///      is set — a downgrade would push Basic-auth creds onto a cleartext hop.
    ///   2. If `allowed_redirect_hosts` is non-empty, the target must match an
    ///      entry. A bare-host entry pins only the host; a `host:port` entry pins
    ///      the exact endpoint, refusing an allowlisted host on an attacker port.
    fn validate_redirect(&self, target: &reqwest::Url, id: u32) -> Result<(), Error> {
        // Only http(s) targets ever get credentials re-attached. Preventing something like ftp://.
        let scheme = target.scheme();
        if !scheme.eq_ignore_ascii_case("http") && !scheme.eq_ignore_ascii_case("https") {
            return Err(Error::PermanentHttpError(format!(
                "Doris sink ID {id}: refusing redirect to non-HTTP(S) scheme '{scheme}'"
            )));
        }

        let downgraded = self.base_url.scheme().eq_ignore_ascii_case("https")
            && !target.scheme().eq_ignore_ascii_case("https");
        if downgraded && !self.allow_insecure_redirect {
            return Err(Error::PermanentHttpError(format!(
                "Doris sink ID {id}: refusing redirect that downgrades {} -> {} \
                 (would leak credentials in cleartext; set allow_insecure_redirect=true \
                 to permit a known-insecure FE -> BE topology)",
                self.base_url.scheme(),
                target.scheme(),
            )));
        }

        if let Some(allowed) = self.allowed_redirect_hosts.as_deref()
            && !allowed.is_empty()
            && !redirect_target_allowed(allowed, target)
        {
            return Err(Error::PermanentHttpError(format!(
                "Doris sink ID {id}: redirect target '{}:{}' is not in allowed_redirect_hosts",
                target.host_str().unwrap_or(""),
                target
                    .port_or_known_default()
                    .map(|p| p.to_string())
                    .unwrap_or_default(),
            )));
        }

        Ok(())
    }
}

/// Match a redirect target against the allowlist. An entry of `host` matches any
/// port on that host; an entry of `host:port` pins the exact endpoint. DNS names,
/// IPv4 literals, and IPv6 literals (bare `::1` or bracketed `[::1]`/`[::1]:8040`)
/// all split cleanly.
fn redirect_target_allowed(allowed: &[String], target: &reqwest::Url) -> bool {
    let raw_host = target.host_str().unwrap_or("");
    // `host_str()` brackets IPv6 literals (`[::1]`); strip them so a bare (`::1`)
    // or bracketed (`[::1]`) allowlist entry both compare equal.
    let host = strip_brackets(raw_host);
    let port = target.port_or_known_default();
    allowed.iter().any(|entry| match split_host_port(entry) {
        (entry_host, Some(entry_port)) => {
            entry_host.eq_ignore_ascii_case(host) && Some(entry_port) == port
        }
        (entry_host, None) => entry_host.eq_ignore_ascii_case(host),
    })
}

/// Strip a single pair of surrounding `[ ]` brackets from an IPv6 literal, so a
/// bracketed host compares equal to its bare form.
fn strip_brackets(host: &str) -> &str {
    host.strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host)
}

/// Split an allowlist entry into `(host, optional port)`, with the host returned
/// *unbracketed* so it compares against a bracket-stripped `host_str()`.
///
/// - `[host]` / `[host]:port` — bracketed IPv6: the bracketed host splits from an
///   optional trailing `:<port>`.
/// - A bare entry with more than one `:` is an unbracketed IPv6 literal (`::1`,
///   `fe80::1`); a port suffix on it would be ambiguous, so it is host-only. Pin a
///   port on an IPv6 host by bracketing it (`[::1]:8040`).
/// - Otherwise a trailing `:<digits>` is the port; anything else is host-only.
fn split_host_port(entry: &str) -> (&str, Option<u16>) {
    if let Some(rest) = entry.strip_prefix('[') {
        // Bracketed IPv6: `[host]` or `[host]:port`.
        if let Some((host, after)) = rest.split_once(']') {
            let port = after.strip_prefix(':').and_then(|p| p.parse::<u16>().ok());
            return (host, port);
        }
        // No closing `]`: malformed, treat the whole thing as host-only.
        return (entry, None);
    }
    // A bare multi-colon entry is an unbracketed IPv6 literal: host-only.
    if entry.matches(':').count() > 1 {
        return (entry, None);
    }
    if let Some((host, port)) = entry.rsplit_once(':')
        && !port.is_empty()
        && let Ok(port) = port.parse::<u16>()
    {
        (host, Some(port))
    } else {
        (entry, None)
    }
}

/// Parse a human-readable duration (e.g. "30s"), falling back to `default` with
/// a warning when malformed. Zero is valid for retry backoff configuration.
fn parse_duration(input: Option<&str>, default: &str) -> Duration {
    let raw = input.unwrap_or(default);
    HumanDuration::from_str(raw)
        .map(|d| *d)
        .unwrap_or_else(|e| {
            warn!("Invalid duration '{raw}': {e}, using default '{default}'");
            *HumanDuration::from_str(default).expect("default duration must be valid")
        })
}

/// Parse a reqwest request timeout. Unlike retry delays, a zero request timeout
/// is degenerate because every request expires immediately.
fn parse_request_duration(input: Option<&str>, default: &str) -> Duration {
    let raw = input.unwrap_or(default);
    let parsed = parse_duration(input, default);
    if parsed.is_zero() {
        warn!(
            "Duration '{raw}' is zero, which would time out every request immediately; \
             using default '{default}'"
        );
        return *HumanDuration::from_str(default).expect("default duration must be valid");
    }
    parsed
}

/// Build the Stream Load URL from `fe_url` and the (already identifier-checked)
/// `database`/`table`. Replaces the path wholesale so a trailing slash or stray
/// path on `fe_url` can't double up the path.
fn build_stream_load_url(
    id: u32,
    fe_url: &str,
    database: &str,
    table: &str,
) -> Result<reqwest::Url, Error> {
    let mut url = reqwest::Url::parse(fe_url).map_err(|e| {
        Error::InvalidConfigValue(format!(
            "Doris sink ID {id} has invalid fe_url '{fe_url}': {e}"
        ))
    })?;
    // Stream Load speaks HTTP. A `file://`/`ftp://`/... base parses fine but would
    // only fail later, per-batch, at `send()`; reject it here at startup instead.
    let scheme = url.scheme();
    if scheme != "http" && scheme != "https" {
        return Err(Error::InvalidConfigValue(format!(
            "Doris sink ID {id} fe_url '{fe_url}' must use http or https, got '{scheme}'"
        )));
    }
    url.set_path(&format!("/api/{database}/{table}/_stream_load"));
    Ok(url)
}

/// Effective batch size: the configured value floored at 1 so `chunks()` is
/// never handed a 0 (which would panic).
fn effective_batch_size(configured: Option<u32>) -> usize {
    configured.unwrap_or(DEFAULT_BATCH_SIZE).max(1) as usize
}

/// Total Stream Load attempts per batch. An unset value uses the default;
/// configured `0` or `1` both mean one attempt with no retry.
fn effective_max_retries(configured: Option<u32>) -> u32 {
    configured.unwrap_or(DEFAULT_MAX_RETRIES).max(1)
}

fn should_warn_for_retry_count(max_retries: u32) -> bool {
    max_retries > MAX_RETRIES_WARNING_THRESHOLD
}

/// Build a validated Stream Load header value, surfacing a bad byte (CR/LF,
/// non-visible-ASCII) as a startup-time `InvalidConfigValue` instead of a
/// per-batch `HttpRequestFailed` (reqwest defers `HeaderValue::try_from` to
/// `.send()`, so an invalid `columns`/`where` would otherwise fail every batch).
fn validated_header(field: &str, value: &str, id: u32) -> Result<header::HeaderValue, Error> {
    header::HeaderValue::from_str(value).map_err(|e| {
        Error::InvalidConfigValue(format!(
            "Doris sink ID {id}: '{field}' header value is invalid (must be visible ASCII, no CR/LF): {e}"
        ))
    })
}

/// Replace Doris-label-illegal characters with `_` and cap the result at
/// `max_len` chars. Doris labels allow `[A-Za-z0-9_-]` only.
fn sanitize_segment(value: &str, max_len: usize) -> String {
    value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .take(max_len)
        .collect()
}

/// A single blake3 fingerprint over the *raw* (unsanitized) `prefix`, target
/// table, `stream`, and `topic`, truncated to `LABEL_HASH_HEX_LEN` hex
/// chars. This disambiguates
/// identities that sanitize+truncate to the same string (e.g. `events.v1` vs
/// `events_v1`, or prefixes `prod_events_us_east_1` vs `..._2`), which would
/// otherwise produce identical labels and cause silent data loss via Doris's
/// database-scoped label dedupe. Inputs are length-prefixed so distinct tuples
/// cannot alias into one digest.
fn identity_hash(prefix: &str, table: &str, stream: &str, topic: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    for part in [prefix, table, stream, topic] {
        hasher.update(&(part.len() as u64).to_le_bytes());
        hasher.update(part.as_bytes());
    }
    let hash = hasher.finalize().to_hex();
    hash.as_str()[..LABEL_HASH_HEX_LEN].to_string()
}

/// Pure label builder. Format:
/// `{prefix_san}-{stream_san}-{topic_san}-{hash16}-{partition}-{first}-{last}`.
///
/// The segment caps bound the total under Doris's 128-char label limit (worst
/// case 120), and the joint `hash16` over the raw source and target identity
/// keeps labels distinct even when the sanitized segments collide. The target
/// table must participate because Doris labels are scoped to a database rather
/// than to an individual table.
///
/// `#[doc(hidden)]`: `pub` only so the integration test harness can reproduce
/// labels; not part of the connector's supported API.
#[doc(hidden)]
pub fn build_label(
    prefix: &str,
    table: &str,
    stream: &str,
    topic: &str,
    partition_id: u32,
    first_offset: u64,
    last_offset: u64,
) -> String {
    format!(
        "{}-{}-{}-{}-{}-{}-{}",
        sanitize_segment(prefix, MAX_LABEL_PREFIX_LEN),
        sanitize_segment(stream, MAX_LABEL_NAME_LEN),
        sanitize_segment(topic, MAX_LABEL_NAME_LEN),
        identity_hash(prefix, table, stream, topic),
        partition_id,
        first_offset,
        last_offset,
    )
}

/// Truncate `s` at the largest char boundary `<= max_bytes` and append a marker
/// recording the original size. Bounds the portion of an HTTP response body that
/// lands in logs or error variants.
fn truncate_for_log(s: &str, max_bytes: usize) -> String {
    if s.len() <= max_bytes {
        return s.to_string();
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...(truncated, total {} bytes)", &s[..end], s.len())
}

fn serialize_json_batch<T>(batch: &T) -> Result<Bytes, Error>
where
    T: Serialize + ?Sized,
{
    simd_json::to_vec(batch)
        .map(Bytes::from)
        .map_err(|e| Error::Serialization(format!("Failed to serialize batch for Doris: {e}")))
}

fn parse_stream_load_response(body: &str) -> Result<StreamLoadResponse, Error> {
    if body.is_empty() {
        // A readable but empty 2xx body leaves the commit outcome ambiguous in
        // exactly the same way as a body-read failure. Retrying the identical
        // request under the same label lets Doris reveal or dedupe the outcome.
        return Err(Error::CannotStoreData(
            "Doris Stream Load returned an empty 2xx response body".to_string(),
        ));
    }

    // A non-empty, unparsable 2xx body (Doris bug, proxy-injected HTML, future
    // schema change) isn't cured by retrying the same bytes, so classify it as
    // permanent and surface it without spending the retry budget.
    serde_json::from_str(body).map_err(|e| {
        Error::PermanentHttpError(format!(
            "Failed to parse Doris stream load response: {e}. Body: {}",
            truncate_for_log(body, MAX_RESPONSE_LOG_BYTES)
        ))
    })
}

fn validate_identifier(name: &str, field: &str, id: u32) -> Result<(), Error> {
    if name.is_empty() {
        return Err(Error::InvalidConfigValue(format!(
            "Doris sink ID {id}: {field} must not be empty"
        )));
    }
    if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(Error::InvalidConfigValue(format!(
            "Doris sink ID {id}: {field} '{name}' must match [A-Za-z0-9_]+ (iggy's stricter subset of Doris identifiers, used as a path-traversal guard)"
        )));
    }
    Ok(())
}

/// Transient Stream Load failures worth an in-request retry: HTTP 5xx/408/429,
/// transport errors, and duplicate labels whose existing Doris job is `RUNNING`
/// or `CANCELLED`. `PermanentHttpError` (4xx, "Fail", schema/redirect problems,
/// non-empty unparsable body) is never retried — re-PUTing bad data just hammers
/// the FE.
fn is_transient_error(error: &Error) -> bool {
    matches!(
        error,
        Error::CannotStoreData(_) | Error::HttpRequestFailed(_)
    )
}

fn classify_status(id: u32, response: &StreamLoadResponse) -> Result<(), Error> {
    match response.status.as_str() {
        "Success" => Ok(()),
        "Label Already Exists" => match response.existing_job_status.as_deref() {
            Some("FINISHED") => {
                info!(
                    "Doris sink ID {id} confirmed duplicate label belongs to a FINISHED job; treating as success"
                );
                Ok(())
            }
            Some(existing_status @ ("RUNNING" | "CANCELLED")) => {
                Err(Error::CannotStoreData(format!(
                    "Doris sink ID {id} found duplicate label with retryable existing job status '{}': {}",
                    existing_status, response.message
                )))
            }
            Some(existing_status) => Err(Error::PermanentHttpError(format!(
                "Doris sink ID {id} found duplicate label with unsupported existing job status '{existing_status}': {}",
                response.message
            ))),
            None => Err(Error::PermanentHttpError(format!(
                "Doris sink ID {id} found duplicate label without ExistingJobStatus: {}",
                response.message
            ))),
        },
        "Publish Timeout" => {
            warn!(
                "Doris sink ID {id} stream load committed but publish visibility timed out; treating as success: {}",
                response.message
            );
            Ok(())
        }
        "Fail" => Err(Error::PermanentHttpError(format!(
            "Doris sink ID {id} stream load failed: {}",
            response.message
        ))),
        // Default unknown statuses to permanent: surfacing an unrecognized
        // failure (e.g. a future Doris error variant) beats silently retrying it
        // against the FE until the in-request budget is exhausted.
        other => Err(Error::PermanentHttpError(format!(
            "Doris sink ID {id} stream load returned unexpected status '{other}': {}",
            response.message
        ))),
    }
}

#[async_trait]
impl Sink for DorisSink {
    async fn open(&mut self) -> Result<(), Error> {
        // Constrain database/table BEFORE building the URL — they flow into the
        // path, so [A-Za-z0-9_]+ (narrower than Doris's own identifier rules)
        // blocks path traversal in `/api/{db}/{table}/_stream_load`.
        validate_identifier(&self.config.database, "database", self.id)?;
        validate_identifier(&self.config.table, "table", self.id)?;

        let base_url = build_stream_load_url(
            self.id,
            &self.config.fe_url,
            &self.config.database,
            &self.config.table,
        )?;

        // Doris permits passwordless users (e.g. a fresh `root`), so an empty
        // password is valid — but almost always a misconfiguration. Warn, don't
        // fail, so local/dev setups still work.
        if self.config.password.expose_secret().is_empty() {
            warn!(
                "Doris sink ID {} is configured with an empty password for user '{}'; \
                 this is accepted but is usually a misconfiguration.",
                self.id, self.config.username
            );
        }

        // Warn when credentials would travel in cleartext. The FE -> BE 307 hop
        // itself is guarded by `validate_redirect` (scheme downgrade + host
        // allowlist); this covers the case where the FE itself is plain http.
        if base_url.scheme().eq_ignore_ascii_case("http") {
            let host = base_url.host_str().unwrap_or("");
            // `host_str()` brackets IPv6 literals (`[::1]`); strip them and parse
            // as `IpAddr` to catch `127.0.0.1`, `::1`, and any loopback spelling.
            let is_loopback = host == "localhost"
                || host
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback());
            if !is_loopback {
                warn!(
                    "Doris sink ID {} is configured with http:// to non-loopback host '{}'; \
                     credentials and message data will be transmitted in cleartext. \
                     Use https:// in production.",
                    self.id, host
                );
            }
        }

        // Validate + precompute the optional Stream Load headers once. A bad byte
        // in `columns`/`where` fails here at startup, not silently per batch.
        let max_filter_ratio_header = match self.config.max_filter_ratio {
            Some(ratio) => {
                // Doris's max_filter_ratio is a fraction in [0.0, 1.0]. A NaN/inf or
                // out-of-range value formats to a header-valid string (so the ASCII
                // check below would pass) but Doris rejects it on every batch —
                // catch it here at startup instead.
                if !ratio.is_finite() || !(0.0..=1.0).contains(&ratio) {
                    return Err(Error::InvalidConfigValue(format!(
                        "Doris sink ID {}: max_filter_ratio must be a finite value in [0.0, 1.0], got {ratio}",
                        self.id
                    )));
                }
                Some(validated_header(
                    "max_filter_ratio",
                    &ratio.to_string(),
                    self.id,
                )?)
            }
            None => None,
        };
        let columns_header = match self.config.columns.as_deref() {
            Some(columns) => Some(validated_header("columns", columns, self.id)?),
            None => None,
        };
        let where_header = match self.config.where_clause.as_deref() {
            Some(where_clause) => Some(validated_header("where", where_clause, self.id)?),
            None => None,
        };

        let retry_delay = parse_duration(self.config.retry_delay.as_deref(), DEFAULT_RETRY_DELAY);
        let max_retry_delay = parse_duration(
            self.config.max_retry_delay.as_deref(),
            DEFAULT_MAX_RETRY_DELAY,
        );
        let max_retries = effective_max_retries(self.config.max_retries);
        if should_warn_for_retry_count(max_retries) {
            warn!(
                "Doris sink ID {} configured max_retries={max_retries}, above the warning threshold {MAX_RETRIES_WARNING_THRESHOLD}; the value is honored, but an unavailable FE can keep each chunk in consume() for tens of minutes or hours and delay graceful shutdown",
                self.id
            );
        }
        // `exponential_backoff` already caps at the max, but a base above the cap
        // is a config mistake worth surfacing rather than silently flattening.
        let (retry_delay, max_retry_delay) = if retry_delay > max_retry_delay {
            warn!(
                "Doris sink ID {}: retry_delay ({retry_delay:?}) exceeds max_retry_delay ({max_retry_delay:?}); clamping base to the cap",
                self.id
            );
            (max_retry_delay, max_retry_delay)
        } else {
            (retry_delay, max_retry_delay)
        };

        self.connected = Some(Connected {
            client: self.build_client()?,
            base_url,
            max_filter_ratio_header,
            columns_header,
            where_header,
            allow_insecure_redirect: self.config.allow_insecure_redirect.unwrap_or(false),
            allowed_redirect_hosts: self.config.allowed_redirect_hosts.clone(),
            max_retries,
            retry_delay,
            max_retry_delay,
        });

        info!(
            "Opened Doris sink ID {} for {}.{} at {}",
            self.id, self.config.database, self.config.table, self.config.fe_url
        );
        Ok(())
    }

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        if messages.is_empty() {
            return Ok(());
        }

        let total = messages.len();
        debug!(
            "Doris sink ID {} received {total} messages for {}.{}",
            self.id, self.config.database, self.config.table
        );

        let batch_size = effective_batch_size(self.config.batch_size);
        let label_prefix = self
            .config
            .label_prefix
            .as_deref()
            .unwrap_or(DEFAULT_LABEL_PREFIX);
        let mut first_error: Option<Error> = None;

        // Best-effort across chunks: on a per-chunk serialize/HTTP/status failure
        // we log it, keep the first error, and `continue` so later chunks still
        // land. The runtime commits this poll's consumer offset before consume()
        // runs, so returning early would drop the remaining chunks rather than
        // replay them. The returned error is mapped to a 0/1 status at the FFI
        // boundary (severity isn't propagated), and every chunk error is already
        // logged individually below — so first-error is sufficient.
        //
        // The lone hard-abort is a non-JSON payload (via `?`): a stream-wide
        // schema-contract violation, not a transient chunk failure. Under the
        // documented `schema = "json"` config the SDK drops non-JSON before
        // consume() is called, so this stands as a defensive guard.
        for chunk in messages.chunks(batch_size) {
            let json_values: Vec<&simd_json::OwnedValue> = chunk
                .iter()
                .map(|m| match &m.payload {
                    Payload::Json(value) => Ok(value),
                    _ => {
                        error!(
                            "Doris sink ID {} received non-JSON payload (schema={}); aborting poll",
                            self.id, messages_metadata.schema
                        );
                        Err(Error::InvalidPayloadType)
                    }
                })
                .collect::<Result<_, _>>()?;

            // `chunks()` never yields an empty slice, so first/last are present.
            // Use `zip` + `continue` (not `.expect`) so that if a future refactor
            // ever breaks that invariant we neither fabricate offset 0 (which
            // would alias the real offset-0 label and break idempotency) nor
            // panic across the `extern "C"` FFI boundary (UB per the nomicon).
            let Some((first_msg, last_msg)) = chunk.first().zip(chunk.last()) else {
                continue;
            };

            let body = match serialize_json_batch(&json_values) {
                Ok(body) => body,
                Err(error) => {
                    error!(
                        "Doris sink ID {} failed to serialize batch: {error}",
                        self.id
                    );
                    first_error.get_or_insert(error);
                    continue;
                }
            };

            let label = build_label(
                label_prefix,
                &self.config.table,
                &topic_metadata.stream,
                &topic_metadata.topic,
                messages_metadata.partition_id,
                first_msg.offset,
                last_msg.offset,
            );

            match self.load_batch(&label, body).await {
                Ok(response) => {
                    if response.number_filtered_rows > 0 {
                        // Filtered rows usually mean schema drift upstream.
                        // Surface above debug so operators can alert on it.
                        warn!(
                            "Doris sink ID {} loaded {} rows but FILTERED {} rows for {}.{} (label={label}); \
                             likely schema drift upstream",
                            self.id,
                            response.number_loaded_rows,
                            response.number_filtered_rows,
                            self.config.database,
                            self.config.table,
                        );
                    } else {
                        debug!(
                            "Doris sink ID {} loaded {} rows into {}.{} (label={label})",
                            self.id,
                            response.number_loaded_rows,
                            self.config.database,
                            self.config.table,
                        );
                    }
                }
                Err(error) => {
                    error!(
                        "Doris sink ID {} batch failed (label={label}): {error}",
                        self.id
                    );
                    first_error.get_or_insert(error);
                }
            }
        }

        if let Some(err) = first_error {
            return Err(err);
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        info!("Doris sink ID {} closed.", self.id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config() -> DorisSinkConfig {
        DorisSinkConfig {
            fe_url: "http://localhost:8030".into(),
            database: "test_db".into(),
            table: "test_tbl".into(),
            username: "root".into(),
            password: SecretString::from("pw"),
            label_prefix: None,
            max_filter_ratio: None,
            columns: None,
            where_clause: None,
            timeout: None,
            connect_timeout: None,
            batch_size: None,
            allow_insecure_redirect: None,
            allowed_redirect_hosts: None,
            max_retries: None,
            retry_delay: None,
            max_retry_delay: None,
        }
    }

    fn stream_load_response(status: &str, existing_job_status: Option<&str>) -> StreamLoadResponse {
        StreamLoadResponse {
            status: status.into(),
            message: String::new(),
            number_loaded_rows: 0,
            number_filtered_rows: 0,
            existing_job_status: existing_job_status.map(String::from),
        }
    }

    #[test]
    fn stream_load_url_is_well_formed() {
        let url = build_stream_load_url(1, "http://localhost:8030", "test_db", "test_tbl").unwrap();
        assert_eq!(
            url.as_str(),
            "http://localhost:8030/api/test_db/test_tbl/_stream_load"
        );
    }

    #[test]
    fn stream_load_url_handles_trailing_slash() {
        let url =
            build_stream_load_url(1, "http://localhost:8030/", "test_db", "test_tbl").unwrap();
        assert_eq!(
            url.as_str(),
            "http://localhost:8030/api/test_db/test_tbl/_stream_load"
        );
    }

    #[test]
    fn stream_load_url_rejects_garbage_fe_url() {
        assert!(matches!(
            build_stream_load_url(1, "not a url", "db", "tbl"),
            Err(Error::InvalidConfigValue(_))
        ));
    }

    #[test]
    fn stream_load_url_rejects_non_http_scheme() {
        // A parseable but non-HTTP base would only fail later, per-batch, at send().
        for fe_url in ["file:///etc", "ftp://host/path", "ws://host:8030"] {
            assert!(
                matches!(
                    build_stream_load_url(1, fe_url, "db", "tbl"),
                    Err(Error::InvalidConfigValue(_))
                ),
                "expected {fe_url} to be rejected at startup",
            );
        }
    }

    #[test]
    fn label_is_deterministic() {
        let a = build_label("iggy", "test_tbl", "events", "orders", 7, 100, 199);
        let b = build_label("iggy", "test_tbl", "events", "orders", 7, 100, 199);
        assert_eq!(a, b);
        // Format: {prefix}-{stream_san}-{topic_san}-{hash16}-{partition}-{first}-{last}
        let parts: Vec<&str> = a.split('-').collect();
        assert_eq!(parts.len(), 7);
        assert_eq!(parts[0], "iggy");
        assert_eq!(parts[1], "events");
        assert_eq!(parts[2], "orders");
        assert_eq!(parts[3].len(), LABEL_HASH_HEX_LEN);
        assert!(parts[3].chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(parts[4], "7");
        assert_eq!(parts[5], "100");
        assert_eq!(parts[6], "199");
    }

    #[test]
    fn label_sanitizes_illegal_chars() {
        let label = build_label("iggy", "test_tbl", "events.v1", "orders/inbound", 0, 0, 0);
        // dots and slashes are not allowed in Doris labels.
        assert!(!label.contains('.'));
        assert!(!label.contains('/'));
    }

    #[test]
    fn label_disambiguates_names_that_sanitize_identically() {
        // The whole point of the joint hash: `events.v1` and `events_v1`
        // collapse to the same sanitized form, but the hash is over the raw
        // names so the final labels differ. Without this, two streams could
        // silently dedupe against each other in Doris.
        assert_ne!(
            build_label("iggy", "test_tbl", "events.v1", "orders", 0, 0, 0),
            build_label("iggy", "test_tbl", "events_v1", "orders", 0, 0, 0),
            "labels must NOT collide for names that sanitize to the same string"
        );
    }

    #[test]
    fn label_disambiguates_prefixes_that_sanitize_identically() {
        // Two connectors writing the same stream/topic/partition/offset range
        // but with prefixes that collapse to the same sanitized+truncated
        // segment must still get distinct labels — otherwise Doris's label
        // dedupe silently drops the second tenant's batch.
        let a = build_label(
            "prod_events_us_east_1",
            "test_tbl",
            "events",
            "orders",
            0,
            0,
            0,
        );
        let b = build_label(
            "prod_events_us_east_2",
            "test_tbl",
            "events",
            "orders",
            0,
            0,
            0,
        );
        // Precondition: the sanitized prefix segments collide (both truncate to
        // the same 16 chars).
        assert_eq!(
            a.split('-').next(),
            b.split('-').next(),
            "precondition: sanitized prefixes should collide at 16 chars"
        );
        // ...but the full labels differ because the raw prefix is folded into
        // the hash.
        assert_ne!(
            a, b,
            "labels must NOT collide for prefixes that sanitize to the same string"
        );
    }

    #[test]
    fn label_disambiguates_target_tables_in_same_database() {
        let first = build_label("iggy", "orders", "events", "created", 0, 0, 99);
        let second = build_label("iggy", "orders_archive", "events", "created", 0, 0, 99);

        assert_ne!(
            first, second,
            "Doris labels are database-scoped, so the target table must affect the label"
        );
    }

    #[test]
    fn identity_hash_is_not_aliased_by_boundary_shift() {
        // The joint hash is length-prefixed so shifting any boundary cannot
        // produce the same digest: distinct source/target identity tuples must
        // map to distinct hashes, otherwise two identities could share a label
        // and silently dedupe in Doris.
        assert_ne!(
            identity_hash("iggy", "test_tbl", "ab", "c"),
            identity_hash("iggy", "test_tbl", "a", "bc")
        );
        assert_ne!(
            identity_hash("iggy", "test_tbl", "events", "orders"),
            identity_hash("iggy", "test_tbl", "event", "sorders")
        );
        // The prefix participates too: shifting the prefix/stream boundary must
        // not alias.
        assert_ne!(
            identity_hash("ab", "test_tbl", "c", "topic"),
            identity_hash("a", "test_tbl", "bc", "topic")
        );
    }

    #[test]
    fn label_stays_under_doris_128_char_cap() {
        // Doris caps Stream Load labels at 128 chars. Build the worst case
        // permitted by the connector: 100-char prefix/stream/topic (all of
        // which get truncated), u64::MAX offsets, u32::MAX partition.
        let prefix = "p".repeat(100);
        let stream = "s".repeat(100);
        let topic = "t".repeat(100);
        let label = build_label(
            &prefix,
            "test_tbl",
            &stream,
            &topic,
            u32::MAX,
            u64::MAX,
            u64::MAX,
        );
        assert!(
            label.len() <= 128,
            "label exceeds Doris's 128-char cap: {} chars: {label}",
            label.len()
        );
    }

    #[test]
    fn effective_batch_size_floors_at_one() {
        assert_eq!(effective_batch_size(Some(0)), 1);
        assert_eq!(effective_batch_size(None), DEFAULT_BATCH_SIZE as usize);
        assert_eq!(effective_batch_size(Some(500)), 500);
    }

    #[test]
    fn effective_max_retries_uses_default_and_floors_at_one() {
        assert_eq!(effective_max_retries(None), DEFAULT_MAX_RETRIES);
        assert_eq!(effective_max_retries(Some(0)), 1);
        assert_eq!(effective_max_retries(Some(1)), 1);
        assert_eq!(effective_max_retries(Some(5)), 5);
        assert_eq!(
            effective_max_retries(Some(MAX_RETRIES_WARNING_THRESHOLD + 1)),
            MAX_RETRIES_WARNING_THRESHOLD + 1
        );
    }

    #[test]
    fn retry_count_warning_starts_above_threshold() {
        assert!(!should_warn_for_retry_count(MAX_RETRIES_WARNING_THRESHOLD));
        assert!(should_warn_for_retry_count(
            MAX_RETRIES_WARNING_THRESHOLD + 1
        ));
    }

    #[test]
    fn classify_success_returns_ok() {
        let mut response = stream_load_response("Success", None);
        response.number_loaded_rows = 10;
        assert!(classify_status(1, &response).is_ok());
    }

    #[test]
    fn classify_finished_duplicate_returns_ok() {
        let response = stream_load_response("Label Already Exists", Some("FINISHED"));
        assert!(classify_status(1, &response).is_ok());
    }

    #[test]
    fn classify_running_or_cancelled_duplicate_is_transient() {
        for existing_status in ["RUNNING", "CANCELLED"] {
            let response = stream_load_response("Label Already Exists", Some(existing_status));
            assert!(matches!(
                classify_status(1, &response),
                Err(Error::CannotStoreData(_))
            ));
        }
    }

    #[test]
    fn classify_unconfirmed_duplicate_is_permanent() {
        for existing_status in [None, Some(""), Some("PRECOMMITTED"), Some("UNKNOWN")] {
            let response = stream_load_response("Label Already Exists", existing_status);
            assert!(matches!(
                classify_status(1, &response),
                Err(Error::PermanentHttpError(_))
            ));
        }
    }

    #[test]
    fn classify_publish_timeout_returns_ok() {
        let mut response = stream_load_response("Publish Timeout", None);
        response.message = "publish visibility delayed".into();
        assert!(classify_status(1, &response).is_ok());
    }

    #[test]
    fn classify_fail_is_permanent() {
        let mut response = stream_load_response("Fail", None);
        response.message = "schema mismatch".into();
        assert!(matches!(
            classify_status(1, &response).unwrap_err(),
            Error::PermanentHttpError(_)
        ));
    }

    #[test]
    fn classify_unknown_status_is_permanent() {
        let response = stream_load_response("Future Doris Status", None);
        assert!(matches!(
            classify_status(1, &response),
            Err(Error::PermanentHttpError(_))
        ));
    }

    #[test]
    fn parse_stream_load_response_handles_minimal_json() {
        let body = r#"{"Status":"Success"}"#;
        let r = parse_stream_load_response(body).unwrap();
        assert_eq!(r.status, "Success");
        assert_eq!(r.number_loaded_rows, 0);
    }

    #[test]
    fn parse_stream_load_response_treats_empty_body_as_transient() {
        assert!(matches!(
            parse_stream_load_response("").unwrap_err(),
            Error::CannotStoreData(_)
        ));
    }

    #[test]
    fn parse_stream_load_response_rejects_nonempty_garbage_as_permanent() {
        // An unparsable body must surface as PermanentHttpError instead of
        // retrying the same garbage for the whole in-request budget.
        let body = "not json";
        assert!(matches!(
            parse_stream_load_response(body).unwrap_err(),
            Error::PermanentHttpError(_)
        ));
    }

    #[test]
    fn serialize_json_batch_maps_local_failure_to_serialization_error() {
        let invalid_json_map = std::collections::BTreeMap::from([(true, 1)]);
        let error = serialize_json_batch(&invalid_json_map).unwrap_err();

        assert!(matches!(&error, Error::Serialization(_)));
        assert!(!is_transient_error(&error));
    }

    #[test]
    fn validate_identifier_rejects_path_traversal() {
        assert!(validate_identifier("../admin", "database", 1).is_err());
        assert!(validate_identifier("foo/bar", "table", 1).is_err());
        assert!(validate_identifier("", "database", 1).is_err());
        assert!(validate_identifier("ok_name_1", "database", 1).is_ok());
    }

    #[test]
    fn truncate_for_log_caps_long_input() {
        let long = "x".repeat(10_000);
        let truncated = truncate_for_log(&long, 100);
        assert!(truncated.len() <= 100 + "...(truncated, total 10000 bytes)".len());
        assert!(truncated.contains("(truncated"));
    }

    #[test]
    fn truncate_for_log_passes_short_input_through() {
        let short = "hello";
        assert_eq!(truncate_for_log(short, 100), "hello");
    }

    #[test]
    fn parse_duration_parses_zero_and_falls_back_for_invalid_input() {
        assert_eq!(parse_duration(Some("10s"), "30s"), Duration::from_secs(10));
        assert_eq!(parse_duration(None, "30s"), Duration::from_secs(30));
        assert_eq!(
            parse_duration(Some("not_a_duration"), "30s"),
            Duration::from_secs(30)
        );
        assert_eq!(parse_duration(Some("0ms"), "5s"), Duration::ZERO);
    }

    #[test]
    fn parse_request_duration_rejects_zero() {
        assert_eq!(
            parse_request_duration(Some("0s"), "30s"),
            Duration::from_secs(30)
        );
    }

    #[tokio::test]
    async fn open_rejects_out_of_range_max_filter_ratio() {
        for ratio in [1.5_f64, -0.1_f64, f64::INFINITY, f64::NAN] {
            let mut cfg = make_config();
            cfg.max_filter_ratio = Some(ratio);
            let mut sink = DorisSink::new(1, cfg);
            assert!(
                matches!(sink.open().await, Err(Error::InvalidConfigValue(_))),
                "expected InvalidConfigValue for max_filter_ratio={ratio}",
            );
        }
    }

    #[tokio::test]
    async fn open_accepts_in_range_max_filter_ratio() {
        for ratio in [0.0_f64, 0.5_f64, 1.0_f64] {
            let mut cfg = make_config();
            cfg.max_filter_ratio = Some(ratio);
            let mut sink = DorisSink::new(1, cfg);
            assert!(
                sink.open().await.is_ok(),
                "expected open() to accept max_filter_ratio={ratio}",
            );
        }
    }

    fn url(s: &str) -> reqwest::Url {
        reqwest::Url::parse(s).unwrap()
    }

    fn opened_connection(sink: &DorisSink) -> &Connected {
        sink.connected.as_ref().expect("sink should be open")
    }

    /// Build a `Connected` for redirect-validation tests: a throwaway client and
    /// no precomputed headers, with the redirect policy under test.
    fn connected(
        base: &str,
        allow_insecure: bool,
        allowed_hosts: Option<Vec<String>>,
    ) -> Connected {
        Connected {
            client: reqwest::Client::new(),
            base_url: url(base),
            max_filter_ratio_header: None,
            columns_header: None,
            where_header: None,
            allow_insecure_redirect: allow_insecure,
            allowed_redirect_hosts: allowed_hosts,
            max_retries: DEFAULT_MAX_RETRIES,
            retry_delay: Duration::from_millis(1),
            max_retry_delay: Duration::from_millis(5),
        }
    }

    #[test]
    fn redirect_refuses_https_to_http_downgrade_by_default() {
        // A compromised FE redirecting https -> http would leak Basic creds in
        // cleartext. Refuse it unless explicitly opted in.
        let err = connected("https://fe.doris:8030", false, None)
            .validate_redirect(&url("http://attacker.evil/"), 1);
        assert!(matches!(err, Err(Error::PermanentHttpError(_))));
    }

    #[test]
    fn redirect_allows_downgrade_when_opted_in() {
        // Known-insecure FE -> BE topology: operator accepts the risk.
        assert!(
            connected("https://fe.doris:8030", true, None)
                .validate_redirect(&url("http://be.doris:8040/"), 1)
                .is_ok()
        );
    }

    #[test]
    fn redirect_allows_cross_host_same_scheme() {
        // The normal FE -> BE hop: different host, same scheme, no allowlist.
        assert!(
            connected("https://fe.doris:8030", false, None)
                .validate_redirect(&url("https://be.doris:8040/"), 1)
                .is_ok()
        );
        // http -> http is not a downgrade.
        assert!(
            connected("http://fe.doris:8030", false, None)
                .validate_redirect(&url("http://be.doris:8040/"), 1)
                .is_ok()
        );
    }

    #[test]
    fn redirect_refuses_non_http_scheme() {
        // An http FE redirecting to a non-HTTP scheme slips past the downgrade
        // check (the base isn't https) and, with no allowlist, the host check is
        // skipped — so it must be rejected by the scheme gate before creds are
        // re-attached. Covers the default (no allowlist) path.
        for target in [
            "ftp://be.doris/",
            "file:///etc/passwd",
            "gopher://be.doris/",
        ] {
            assert!(
                matches!(
                    connected("http://fe.doris:8030", false, None)
                        .validate_redirect(&url(target), 1),
                    Err(Error::PermanentHttpError(_))
                ),
                "scheme of {target} should be refused"
            );
        }
    }

    #[test]
    fn redirect_enforces_host_allowlist_when_set() {
        let allowed = vec!["be1.doris".to_string(), "be2.doris".to_string()];
        // Target host not in the allowlist is refused.
        assert!(matches!(
            connected("http://fe.doris:8030", false, Some(allowed.clone()))
                .validate_redirect(&url("http://attacker.evil:8040/"), 1),
            Err(Error::PermanentHttpError(_))
        ));
        // Target host in the allowlist passes (bare host pins host only).
        assert!(
            connected("http://fe.doris:8030", false, Some(allowed))
                .validate_redirect(&url("http://be2.doris:8040/"), 1)
                .is_ok()
        );
    }

    #[test]
    fn redirect_allowlist_matches_ipv6_targets() {
        // `host_str()` brackets IPv6 (`[::1]`), so a naive `rsplit_once(':')` on a
        // bare `::1` entry used to misparse to host ":" / port 1 and refuse a
        // legitimate IPv6 BE redirect. Bare, bracketed, and port-pinned entries
        // must all match the same `http://[::1]:8040` target.
        let target = "http://[::1]:8040/api/db/tbl/_stream_load";
        for entry in ["::1", "[::1]", "[::1]:8040"] {
            assert!(
                connected("http://fe.doris:8030", false, Some(vec![entry.to_string()]))
                    .validate_redirect(&url(target), 1)
                    .is_ok(),
                "IPv6 allowlist entry {entry:?} should match {target}"
            );
        }
        // A port-pinned IPv6 entry still refuses the wrong port.
        assert!(matches!(
            connected(
                "http://fe.doris:8030",
                false,
                Some(vec!["[::1]:8040".to_string()])
            )
            .validate_redirect(&url("http://[::1]:6379/exfil"), 1),
            Err(Error::PermanentHttpError(_))
        ));
        // A different IPv6 host is refused.
        assert!(matches!(
            connected("http://fe.doris:8030", false, Some(vec!["::1".to_string()]))
                .validate_redirect(&url("http://[fe80::1]:8040/"), 1),
            Err(Error::PermanentHttpError(_))
        ));
    }

    #[test]
    fn redirect_allowlist_pins_port_when_specified() {
        // A `host:port` entry pins the endpoint — an allowlisted host on a
        // different (attacker) port is refused, closing the exfiltration vector.
        let allowed = vec!["be.doris:8040".to_string()];
        assert!(
            connected("http://fe.doris:8030", false, Some(allowed.clone()))
                .validate_redirect(&url("http://be.doris:8040/"), 1)
                .is_ok()
        );
        assert!(matches!(
            connected("http://fe.doris:8030", false, Some(allowed))
                .validate_redirect(&url("http://be.doris:6379/exfil"), 1),
            Err(Error::PermanentHttpError(_))
        ));
    }

    #[test]
    fn auth_header_is_basic_b64() {
        let sink = DorisSink::new(1, make_config());
        // base64("root:pw") = cm9vdDpwdw==
        assert_eq!(sink.auth_header.to_str().unwrap(), "Basic cm9vdDpwdw==");
        // Marked sensitive so reqwest keeps it out of debug/trace output.
        assert!(sink.auth_header.is_sensitive());
    }

    fn text_msg(offset: u64) -> ConsumedMessage {
        ConsumedMessage {
            id: offset as u128,
            offset,
            checksum: 0,
            timestamp: 0,
            origin_timestamp: 0,
            headers: None,
            payload: Payload::Text("not json".into()),
        }
    }

    fn json_msg(offset: u64) -> ConsumedMessage {
        let mut bytes = br#"{"k":1}"#.to_vec();
        let value = simd_json::to_owned_value(&mut bytes).unwrap();
        ConsumedMessage {
            id: offset as u128,
            offset,
            checksum: 0,
            timestamp: 0,
            origin_timestamp: 0,
            headers: None,
            payload: Payload::Json(value),
        }
    }

    fn topic_meta() -> TopicMetadata {
        TopicMetadata {
            stream: "events".into(),
            topic: "orders".into(),
        }
    }

    fn messages_meta() -> MessagesMetadata {
        MessagesMetadata {
            partition_id: 0,
            current_offset: 0,
            schema: iggy_connector_sdk::Schema::Json,
        }
    }

    #[tokio::test]
    async fn consume_aborts_on_first_non_json_payload() {
        let sink = DorisSink::new(1, make_config());
        let result = sink
            .consume(&topic_meta(), messages_meta(), vec![text_msg(0)])
            .await;
        assert!(
            matches!(result, Err(Error::InvalidPayloadType)),
            "expected InvalidPayloadType, got {result:?}",
        );
    }

    #[tokio::test]
    async fn consume_aborts_on_non_json_in_mixed_batch() {
        let sink = DorisSink::new(1, make_config());
        let result = sink
            .consume(
                &topic_meta(),
                messages_meta(),
                vec![json_msg(0), text_msg(1)],
            )
            .await;
        assert!(
            matches!(result, Err(Error::InvalidPayloadType)),
            "expected InvalidPayloadType, got {result:?}",
        );
    }

    #[tokio::test]
    async fn open_rejects_columns_header_with_control_chars() {
        // A CR/LF in `columns` is an invalid HeaderValue. reqwest would defer the
        // failure to every `.send()`; we must fail fast at open() instead.
        let mut cfg = make_config();
        cfg.columns = Some("c1,\nc2".into());
        let mut sink = DorisSink::new(1, cfg);
        assert!(
            matches!(sink.open().await, Err(Error::InvalidConfigValue(_))),
            "expected InvalidConfigValue for a columns header with a newline",
        );
    }

    /// Drives a real 307 FE -> BE redirect through `send_stream_load` and
    /// asserts the connector rebuilds the *full* Stream Load request on the
    /// redirected hop. The BE mock only matches when every header is present
    /// — crucially the `Authorization` header, which reqwest would otherwise
    /// strip on a cross-host redirect — so a regression that drops a header
    /// makes the BE mock miss, yielding a 404 and a failed assertion.
    #[tokio::test]
    async fn redirect_rebuilds_full_request_on_be() {
        use wiremock::matchers::{header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let expected_auth = format!("Basic {}", general_purpose::STANDARD.encode("root:pw"));
        // Same host + scheme as the FE, so `validate_redirect` permits it —
        // this is the normal Doris FE -> BE topology.
        let be_url = format!("{}/be/_stream_load", server.uri());

        Mock::given(method("PUT"))
            .and(path("/api/test_db/test_tbl/_stream_load"))
            .respond_with(ResponseTemplate::new(307).insert_header("Location", be_url.as_str()))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("PUT"))
            .and(path("/be/_stream_load"))
            .and(header("authorization", expected_auth.as_str()))
            .and(header("format", "json"))
            .and(header("strip_outer_array", "true"))
            .and(header("expect", "100-continue"))
            .and(header("label", "iggy-test-label"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Status": "Success",
                "Message": "OK",
                "NumberLoadedRows": 1,
                "NumberFilteredRows": 0,
            })))
            .expect(1)
            .mount(&server)
            .await;

        let mut cfg = make_config();
        cfg.fe_url = server.uri();
        let mut sink = DorisSink::new(1, cfg);
        sink.open().await.expect("open should succeed");

        let result = sink
            .send_stream_load(
                opened_connection(&sink),
                "iggy-test-label",
                Bytes::from_static(b"[{\"a\":1}]"),
            )
            .await;

        assert!(
            matches!(&result, Ok(r) if r.status == "Success"),
            "expected Ok(Success) after redirect, got {result:?}",
        );
    }

    /// A redirect loop must surface as a *permanent* error: retrying just
    /// re-walks the loop. (Regression guard for the HttpRequestFailed ->
    /// PermanentHttpError reclassification.)
    #[tokio::test]
    async fn redirect_loop_is_permanent_error() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let mut cfg = make_config();
        cfg.fe_url = server.uri();
        // Self-redirect: same host + scheme, so validate_redirect permits it,
        // and the connector loops until MAX_REDIRECTS is exceeded.
        let self_url = format!("{}/api/test_db/test_tbl/_stream_load", server.uri());

        Mock::given(method("PUT"))
            .and(path("/api/test_db/test_tbl/_stream_load"))
            .respond_with(ResponseTemplate::new(307).insert_header("Location", self_url.as_str()))
            .mount(&server)
            .await;

        let mut sink = DorisSink::new(1, cfg);
        sink.open().await.expect("open should succeed");
        let result = sink
            .send_stream_load(
                opened_connection(&sink),
                "iggy-test-label",
                Bytes::from_static(b"[{\"a\":1}]"),
            )
            .await;

        assert!(
            matches!(&result, Err(Error::PermanentHttpError(_))),
            "expected PermanentHttpError on redirect loop, got {result:?}",
        );
    }

    /// A redirect with no usable `Location` is malformed and permanent.
    #[tokio::test]
    async fn redirect_without_location_is_permanent_error() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let mut cfg = make_config();
        cfg.fe_url = server.uri();

        Mock::given(method("PUT"))
            .and(path("/api/test_db/test_tbl/_stream_load"))
            .respond_with(ResponseTemplate::new(307)) // no Location header
            .mount(&server)
            .await;

        let mut sink = DorisSink::new(1, cfg);
        sink.open().await.expect("open should succeed");
        let result = sink
            .send_stream_load(
                opened_connection(&sink),
                "iggy-test-label",
                Bytes::from_static(b"[{\"a\":1}]"),
            )
            .await;

        assert!(
            matches!(&result, Err(Error::PermanentHttpError(_))),
            "expected PermanentHttpError on missing Location, got {result:?}",
        );
    }

    /// A relative `Location` is outside Doris's absolute-Location contract and
    /// must be rejected as permanent rather than silently joined.
    #[tokio::test]
    async fn redirect_with_relative_location_is_permanent_error() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let mut cfg = make_config();
        cfg.fe_url = server.uri();

        Mock::given(method("PUT"))
            .and(path("/api/test_db/test_tbl/_stream_load"))
            .respond_with(ResponseTemplate::new(307).insert_header("Location", "be_endpoint"))
            .mount(&server)
            .await;

        let mut sink = DorisSink::new(1, cfg);
        sink.open().await.expect("open should succeed");
        let result = sink
            .send_stream_load(
                opened_connection(&sink),
                "iggy-test-label",
                Bytes::from_static(b"[{\"a\":1}]"),
            )
            .await;

        assert!(
            matches!(&result, Err(Error::PermanentHttpError(_))),
            "expected PermanentHttpError on relative Location, got {result:?}",
        );
    }

    /// A transient failure (HTTP 503) is retried through the public `consume`
    /// path. Both mocks match the generated label and serialized body, proving
    /// the retry re-PUTs the same batch under the same idempotency key.
    #[tokio::test]
    async fn transient_failure_is_retried_then_succeeds() {
        use wiremock::matchers::{body_json, header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let mut cfg = make_config();
        cfg.fe_url = server.uri();
        cfg.max_retries = Some(3);
        cfg.retry_delay = Some("1ms".into());
        cfg.max_retry_delay = Some("5ms".into());
        let expected_label = build_label("iggy", "test_tbl", "events", "orders", 0, 0, 0);
        let expected_body = serde_json::json!([{"k": 1}]);

        // wiremock serves the first matching mock in mount order, so mount the
        // single-shot 503 first: it serves attempt 1, then — capped at one
        // response — stops matching, and attempt 2 falls through to the success.
        Mock::given(method("PUT"))
            .and(path("/api/test_db/test_tbl/_stream_load"))
            .and(header("label", expected_label.as_str()))
            .and(body_json(expected_body.clone()))
            .respond_with(ResponseTemplate::new(503))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/api/test_db/test_tbl/_stream_load"))
            .and(header("label", expected_label.as_str()))
            .and(body_json(expected_body))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"Status": "Success", "NumberLoadedRows": 1})),
            )
            .expect(1)
            .mount(&server)
            .await;

        let mut sink = DorisSink::new(1, cfg);
        sink.open().await.expect("open should succeed");
        let result = sink
            .consume(&topic_meta(), messages_meta(), vec![json_msg(0)])
            .await;

        assert!(
            result.is_ok(),
            "expected consume() to succeed after one retry, got {result:?}",
        );
    }

    /// A readable but empty 2xx response leaves the commit outcome unknown.
    /// Retry the identical request and let Doris's label state confirm that the
    /// first attempt finished, without loading the batch twice.
    #[test]
    fn empty_success_body_is_retried_under_same_label() {
        use wiremock::matchers::{body_json, header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let runtime = tokio::runtime::Runtime::new().expect("test runtime should build");
        runtime.block_on(async {
            let server = MockServer::start().await;
            let mut cfg = make_config();
            cfg.fe_url = server.uri();
            cfg.max_retries = Some(3);
            cfg.retry_delay = Some("1ms".into());
            cfg.max_retry_delay = Some("5ms".into());

            let label = "iggy-test-label";
            let body = serde_json::json!([{"a": 1}]);
            Mock::given(method("PUT"))
                .and(path("/api/test_db/test_tbl/_stream_load"))
                .and(header("label", label))
                .and(body_json(body.clone()))
                .respond_with(ResponseTemplate::new(200))
                .up_to_n_times(1)
                .expect(1)
                .mount(&server)
                .await;
            Mock::given(method("PUT"))
                .and(path("/api/test_db/test_tbl/_stream_load"))
                .and(header("label", label))
                .and(body_json(body))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "Status": "Label Already Exists",
                    "ExistingJobStatus": "FINISHED",
                    "Message": "job finished",
                })))
                .expect(1)
                .mount(&server)
                .await;

            let mut sink = DorisSink::new(1, cfg);
            sink.open().await.expect("open should succeed");
            let result = sink
                .load_batch(label, Bytes::from_static(b"[{\"a\":1}]"))
                .await;

            assert!(
                matches!(&result, Ok(response) if response.existing_job_status.as_deref() == Some("FINISHED")),
                "expected the retry to confirm the first attempt, got {result:?}",
            );
        });
    }

    /// A non-empty malformed 2xx body is a protocol failure, not an ambiguous
    /// missing response. It must remain permanent and consume only one attempt.
    #[test]
    fn nonempty_malformed_success_body_is_not_retried() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let runtime = tokio::runtime::Runtime::new().expect("test runtime should build");
        runtime.block_on(async {
            let server = MockServer::start().await;
            let mut cfg = make_config();
            cfg.fe_url = server.uri();
            cfg.max_retries = Some(3);
            cfg.retry_delay = Some("1ms".into());
            cfg.max_retry_delay = Some("5ms".into());

            Mock::given(method("PUT"))
                .and(path("/api/test_db/test_tbl/_stream_load"))
                .respond_with(
                    ResponseTemplate::new(200).set_body_string("<html>proxy error</html>"),
                )
                .expect(1)
                .mount(&server)
                .await;

            let mut sink = DorisSink::new(1, cfg);
            sink.open().await.expect("open should succeed");
            let result = sink
                .load_batch("iggy-test-label", Bytes::from_static(b"[{\"a\":1}]"))
                .await;

            assert!(
                matches!(&result, Err(Error::PermanentHttpError(_))),
                "expected non-empty malformed response to stay permanent, got {result:?}",
            );
        });
    }

    /// An unfinished duplicate label means Doris may still be completing an
    /// earlier ambiguous attempt. Retry the same request until Doris confirms
    /// that job is FINISHED, then accept it without issuing a third request.
    #[tokio::test]
    async fn running_duplicate_is_retried_until_finished() {
        use wiremock::matchers::{body_json, header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let mut cfg = make_config();
        cfg.fe_url = server.uri();
        cfg.max_retries = Some(3);
        cfg.retry_delay = Some("1ms".into());
        cfg.max_retry_delay = Some("5ms".into());

        let label = "iggy-test-label";
        let body = serde_json::json!([{"a": 1}]);
        Mock::given(method("PUT"))
            .and(path("/api/test_db/test_tbl/_stream_load"))
            .and(header("label", label))
            .and(body_json(body.clone()))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Status": "Label Already Exists",
                "ExistingJobStatus": "RUNNING",
                "Message": "job is still running",
            })))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/api/test_db/test_tbl/_stream_load"))
            .and(header("label", label))
            .and(body_json(body))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Status": "Label Already Exists",
                "ExistingJobStatus": "FINISHED",
                "Message": "job finished",
            })))
            .expect(1)
            .mount(&server)
            .await;

        let mut sink = DorisSink::new(1, cfg);
        sink.open().await.expect("open should succeed");
        let result = sink
            .load_batch(label, Bytes::from_static(b"[{\"a\":1}]"))
            .await;

        assert!(
            matches!(&result, Ok(response) if response.existing_job_status.as_deref() == Some("FINISHED")),
            "expected FINISHED duplicate after one retry, got {result:?}",
        );
    }

    /// Doris documents Publish Timeout as a committed transaction whose data
    /// may not yet be visible. Treat it as success and do not retry the payload.
    #[tokio::test]
    async fn publish_timeout_is_not_retried() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let mut cfg = make_config();
        cfg.fe_url = server.uri();
        cfg.max_retries = Some(3);
        cfg.retry_delay = Some("1ms".into());
        cfg.max_retry_delay = Some("5ms".into());

        Mock::given(method("PUT"))
            .and(path("/api/test_db/test_tbl/_stream_load"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "Status": "Publish Timeout",
                "Message": "transaction committed; publish is delayed",
            })))
            .expect(1)
            .mount(&server)
            .await;

        let mut sink = DorisSink::new(1, cfg);
        sink.open().await.expect("open should succeed");
        let result = sink
            .load_batch("iggy-test-label", Bytes::from_static(b"[{\"a\":1}]"))
            .await;

        assert!(
            matches!(&result, Ok(response) if response.status == "Publish Timeout"),
            "expected Publish Timeout to be accepted without a retry, got {result:?}",
        );
    }

    /// When every attempt fails transiently, the budget is exhausted and the
    /// last transient error is surfaced. `.expect(2)` pins the attempt count to
    /// exactly `max_retries` (1 initial + 1 retry) — no over- or under-retry.
    #[tokio::test]
    async fn transient_failure_exhausts_retries_and_returns_error() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let mut cfg = make_config();
        cfg.fe_url = server.uri();
        cfg.max_retries = Some(2);
        cfg.retry_delay = Some("1ms".into());
        cfg.max_retry_delay = Some("5ms".into());

        Mock::given(method("PUT"))
            .and(path("/api/test_db/test_tbl/_stream_load"))
            .respond_with(ResponseTemplate::new(503))
            .expect(2)
            .mount(&server)
            .await;

        let mut sink = DorisSink::new(1, cfg);
        sink.open().await.expect("open should succeed");
        let result = sink
            .load_batch("iggy-test-label", Bytes::from_static(b"[{\"a\":1}]"))
            .await;

        assert!(
            matches!(&result, Err(Error::CannotStoreData(_))),
            "expected CannotStoreData after exhausting retries, got {result:?}",
        );
    }

    /// A permanent failure (HTTP 400) returns on the first attempt with no
    /// retry, even with `max_retries` high. `.expect(1)` pins it to one attempt.
    #[tokio::test]
    async fn permanent_failure_is_not_retried() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let mut cfg = make_config();
        cfg.fe_url = server.uri();
        cfg.max_retries = Some(5);
        cfg.retry_delay = Some("1ms".into());
        cfg.max_retry_delay = Some("5ms".into());

        Mock::given(method("PUT"))
            .and(path("/api/test_db/test_tbl/_stream_load"))
            .respond_with(ResponseTemplate::new(400))
            .expect(1)
            .mount(&server)
            .await;

        let mut sink = DorisSink::new(1, cfg);
        sink.open().await.expect("open should succeed");
        let result = sink
            .load_batch("iggy-test-label", Bytes::from_static(b"[{\"a\":1}]"))
            .await;

        assert!(
            matches!(&result, Err(Error::PermanentHttpError(_))),
            "expected PermanentHttpError with no retry, got {result:?}",
        );
    }
}
