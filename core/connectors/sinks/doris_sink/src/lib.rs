/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose};
use bytes::Bytes;
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Payload, Sink, TopicMetadata, sink_connector,
};
use reqwest::{Method, StatusCode, header};
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use std::time::Duration;
use tracing::{debug, error, info, warn};

sink_connector!(DorisSink);

const DEFAULT_LABEL_PREFIX: &str = "iggy";
const DEFAULT_BATCH_SIZE: u32 = 1000;
const DEFAULT_TIMEOUT_SECS: u64 = 30;
// A bounded TCP handshake timeout so an unreachable FE fails fast instead of
// eating the entire request-timeout budget on connect alone.
const CONNECT_TIMEOUT_SECS: u64 = 5;
// Doris's FE Stream Load returns 307 to a BE; reqwest's default policy strips
// the Authorization header on cross-host redirects, so we follow redirects
// manually. This cap protects against a misbehaving cluster looping.
const MAX_REDIRECTS: u8 = 5;
// Per Doris docs, Stream Load labels must be 1..=128 chars matching
// `[A-Za-z0-9_-]`. These caps keep the worst-case label well under that limit.
const MAX_LABEL_PREFIX_LEN: usize = 16;
const MAX_LABEL_NAME_LEN: usize = 16;
// A single 16-hex-char (64-bit) hash over the raw (stream, topic) pair. 64 bits
// puts the adversarial birthday bound at ~5B, so a multi-tenant namer can't
// cheaply force the label collisions that Doris's server-side dedupe would
// otherwise turn into silent data loss. One joint hash (not one per segment)
// buys that entropy for the same budget, leaving the names full-length.
const LABEL_HASH_HEX_LEN: usize = 16;
// Doris Stream Load responses are normally tiny JSON blobs (~200 B). Cap the
// portion we keep for logs/errors so a misbehaving proxy that returns a giant
// body cannot OOM the connector or flood logs.
const MAX_RESPONSE_LOG_BYTES: usize = 4096;

#[derive(Debug)]
pub struct DorisSink {
    id: u32,
    config: DorisSinkConfig,
    client: Option<reqwest::Client>,
    // Precomputed once in `new()` and marked sensitive so reqwest keeps it out
    // of any debug/trace output (and never HPACK-indexes it on HTTP/2).
    auth_header: header::HeaderValue,
    // Precomputed once in `new()` — the target only depends on fe_url/database/
    // table, so there's no need to re-format it on every batch.
    stream_load_url: String,
    // Precomputed once in `new()`: `max_filter_ratio` is config-static, so its
    // header string is formatted up front rather than via `to_string()` on
    // every PUT (and every redirect hop). `columns`/`where` are already stored
    // as `String` in the config and borrowed directly, so they need no cache.
    max_filter_ratio_header: Option<String>,
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
    pub timeout_secs: Option<u64>,
    /// TCP connect timeout. Independent of `timeout_secs` (the total request
    /// budget). Defaults to 5s; raise it for cross-region or cold-start FEs
    /// that are slow to accept the connection.
    pub connect_timeout_secs: Option<u64>,
    pub batch_size: Option<u32>,
    /// Permit a redirect that downgrades the scheme (e.g. `https://` FE ->
    /// `http://` BE). Off by default: a downgrade would push Basic-auth
    /// credentials onto a cleartext hop, so we refuse it unless the operator
    /// explicitly opts in for a known-insecure FE -> BE topology.
    pub allow_insecure_redirect: Option<bool>,
    /// Optional allowlist of hosts a Stream Load redirect may target. When set
    /// and non-empty, a redirect to any other host is refused — a hard lockdown
    /// against a compromised/MITM'd FE exfiltrating credentials via `Location`.
    /// When unset, cross-host redirects are allowed (required for the normal
    /// FE -> BE topology) subject only to the scheme-downgrade rule above.
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
}

impl DorisSink {
    pub fn new(id: u32, config: DorisSinkConfig) -> Self {
        let credential = format!("{}:{}", config.username, config.password.expose_secret());
        // base64 over `Basic <...>` is always within the visible-ASCII set that
        // HeaderValue accepts, so this conversion cannot fail.
        let mut auth_header = header::HeaderValue::from_str(&format!(
            "Basic {}",
            general_purpose::STANDARD.encode(credential)
        ))
        .expect("Basic auth header is always valid ASCII");
        auth_header.set_sensitive(true);

        let stream_load_url = format!(
            "{}/api/{}/{}/_stream_load",
            config.fe_url.trim_end_matches('/'),
            config.database,
            config.table,
        );

        let max_filter_ratio_header = config.max_filter_ratio.map(|ratio| ratio.to_string());

        DorisSink {
            id,
            config,
            client: None,
            auth_header,
            stream_load_url,
            max_filter_ratio_header,
        }
    }

    fn build_client(&self) -> Result<reqwest::Client, Error> {
        let timeout = Duration::from_secs(self.config.timeout_secs.unwrap_or(DEFAULT_TIMEOUT_SECS));
        // We follow redirects manually to keep the Authorization header alive
        // across the FE -> BE hop. reqwest strips Authorization on cross-host
        // 307s by design.
        let connect_timeout = Duration::from_secs(
            self.config
                .connect_timeout_secs
                .unwrap_or(CONNECT_TIMEOUT_SECS),
        );
        reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(timeout)
            .connect_timeout(connect_timeout)
            .build()
            .map_err(|e| Error::InitError(format!("Failed to build Doris HTTP client: {e}")))
    }

    fn stream_load_url(&self) -> &str {
        &self.stream_load_url
    }

    fn label_prefix(&self) -> &str {
        self.config
            .label_prefix
            .as_deref()
            .unwrap_or(DEFAULT_LABEL_PREFIX)
    }

    fn batch_size(&self) -> usize {
        self.config.batch_size.unwrap_or(DEFAULT_BATCH_SIZE).max(1) as usize
    }

    /// Build the deterministic label for one batch. Delegates to the pure
    /// `build_label` free function so integration tests can reproduce the
    /// exact same label without instantiating a `DorisSink`.
    fn build_label(
        &self,
        topic_metadata: &TopicMetadata,
        partition_id: u32,
        first_offset: u64,
        last_offset: u64,
    ) -> String {
        build_label(
            self.label_prefix(),
            &topic_metadata.stream,
            &topic_metadata.topic,
            partition_id,
            first_offset,
            last_offset,
        )
    }

    async fn send_stream_load(
        &self,
        label: &str,
        body: Bytes,
    ) -> Result<StreamLoadResponse, Error> {
        let client = self.client.as_ref().ok_or_else(|| {
            Error::InitError(format!(
                "Doris sink ID {} called before open() — client not initialized",
                self.id
            ))
        })?;
        let mut url = self.stream_load_url().to_string();
        // Baseline for redirect validation: every redirect target is checked
        // against the originally configured FE scheme/host before we re-attach
        // credentials, so a compromised FE cannot exfiltrate them via Location.
        let base_url = reqwest::Url::parse(&url).map_err(|e| {
            Error::PermanentHttpError(format!(
                "Doris sink ID {} has unparsable stream load URL '{url}': {e}",
                self.id
            ))
        })?;
        let mut redirects = 0u8;

        loop {
            let mut request = client
                .request(Method::PUT, &url)
                .header(header::AUTHORIZATION, self.auth_header.clone())
                .header(header::EXPECT, "100-continue")
                .header("format", "json")
                .header("strip_outer_array", "true")
                .header("label", label)
                .body(body.clone());

            if let Some(ratio) = &self.max_filter_ratio_header {
                request = request.header("max_filter_ratio", ratio);
            }
            if let Some(columns) = &self.config.columns {
                request = request.header("columns", columns);
            }
            if let Some(where_clause) = &self.config.where_clause {
                request = request.header("where", where_clause);
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
                    // A redirect loop is a permanent protocol violation, not a
                    // transient connectivity failure — retrying re-walks the
                    // same loop. Surface it as permanent so it's not retried.
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
                    // won't produce one. Permanent, matching the unparsable-
                    // Location cases below.
                    return Err(Error::PermanentHttpError(format!(
                        "Doris sink ID {} got {status} with no Location header",
                        self.id
                    )));
                };
                // Resolve Location (Doris returns absolute; fall back to joining
                // a relative one onto the current URL) and validate the target
                // before trusting it with credentials on the next iteration.
                let current = reqwest::Url::parse(&url).map_err(|e| {
                    Error::PermanentHttpError(format!(
                        "Doris sink ID {} has unparsable redirect source '{url}': {e}",
                        self.id
                    ))
                })?;
                let target = reqwest::Url::parse(location)
                    .or_else(|_| current.join(location))
                    .map_err(|e| {
                        Error::PermanentHttpError(format!(
                            "Doris sink ID {} got {status} with unparsable Location '{location}': {e}",
                            self.id
                        ))
                    })?;
                validate_redirect(
                    &base_url,
                    &target,
                    self.config.allow_insecure_redirect.unwrap_or(false),
                    self.config.allowed_redirect_hosts.as_deref(),
                    self.id,
                )?;
                debug!("Doris sink ID {} following redirect to {target}", self.id);
                url = target.to_string();
                continue;
            }

            let response_text = response.text().await.unwrap_or_else(|e| {
                // A body-read failure shouldn't be silently swallowed: log it,
                // then fall back to an empty body so the status-based handling
                // below still classifies the outcome (an unparsable/empty body
                // on a non-2xx becomes a permanent error, which is correct).
                warn!(
                    "Doris sink ID {} failed to read response body: {e}",
                    self.id
                );
                String::new()
            });
            let response_for_log = truncate_for_log(&response_text, MAX_RESPONSE_LOG_BYTES);

            if !status.is_success() {
                let msg = format!(
                    "Doris sink ID {} stream load returned HTTP {status}: {response_for_log}",
                    self.id
                );
                error!("{msg}");
                // 408 (Request Timeout) and 429 (Too Many Requests) are 4xx
                // but transient — they should be retried, not DLQ'd.
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
}

/// Validate a Stream Load redirect target before re-attaching credentials.
///
/// Doris's FE legitimately redirects (307) to a BE on a *different host*, so we
/// cannot require same-host. Instead we enforce two rules that close the
/// credential-exfiltration vector a compromised/MITM'd FE would otherwise have:
///
///   1. Scheme must not be downgraded (`https` -> `http`) unless `allow_insecure`
///      is set — a downgrade would push Basic-auth creds onto a cleartext hop.
///   2. If `allowed_hosts` is non-empty, the target host must be in it — a hard
///      lockdown for operators who want to pin redirects to known BE hosts.
fn validate_redirect(
    base: &reqwest::Url,
    target: &reqwest::Url,
    allow_insecure: bool,
    allowed_hosts: Option<&[String]>,
    id: u32,
) -> Result<(), Error> {
    let downgraded = base.scheme().eq_ignore_ascii_case("https")
        && !target.scheme().eq_ignore_ascii_case("https");
    if downgraded && !allow_insecure {
        return Err(Error::PermanentHttpError(format!(
            "Doris sink ID {id}: refusing redirect that downgrades {} -> {} \
             (would leak credentials in cleartext; set allow_insecure_redirect=true \
             to permit a known-insecure FE -> BE topology)",
            base.scheme(),
            target.scheme(),
        )));
    }

    if let Some(allowed) = allowed_hosts
        && !allowed.is_empty()
    {
        let host = target.host_str().unwrap_or("");
        if !allowed.iter().any(|h| h.eq_ignore_ascii_case(host)) {
            return Err(Error::PermanentHttpError(format!(
                "Doris sink ID {id}: redirect target host '{host}' is not in \
                 allowed_redirect_hosts",
            )));
        }
    }

    Ok(())
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

/// A single blake3 fingerprint over the *raw* (unsanitized) `stream` and
/// `topic`, truncated to `LABEL_HASH_HEX_LEN` hex chars. This disambiguates
/// identities that sanitize to the same string (e.g. `events.v1` vs
/// `events_v1`), which would otherwise produce identical labels and cause
/// silent data loss via Doris's server-side label dedupe.
///
/// Hashing the pair jointly (rather than one hash per segment) yields 64-bit
/// collision resistance over the full identity while spending the budget of a
/// single segment, so the sanitized names stay full-length for readability.
/// Inputs are length-prefixed so distinct pairs can't alias into one digest
/// (e.g. `("ab", "c")` vs `("a", "bc")`).
fn identity_hash(stream: &str, topic: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&(stream.len() as u64).to_le_bytes());
    hasher.update(stream.as_bytes());
    hasher.update(&(topic.len() as u64).to_le_bytes());
    hasher.update(topic.as_bytes());
    let hash = hasher.finalize().to_hex();
    hash.as_str()[..LABEL_HASH_HEX_LEN].to_string()
}

/// Pure label builder. Format:
/// `{prefix}-{stream_san}-{topic_san}-{hash16}-{partition}-{first}-{last}`.
///
/// Exposed as a `pub` free function so integration tests can produce the
/// exact same label without instantiating a `DorisSink`. The format is
/// engineered to:
///   1. Bound the total length under Doris's 128-char label cap regardless
///      of input name length (worst case 120 chars).
///   2. Eliminate the silent-collision risk between identities that sanitize
///      to the same string — the joint `hash16` is over the raw names, so it
///      stays distinct even when both sanitized segments collide.
///
/// `#[doc(hidden)]`: this is `pub` only so the integration test harness can
/// call it; it is not part of the connector's supported API.
#[doc(hidden)]
pub fn build_label(
    prefix: &str,
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
        identity_hash(stream, topic),
        partition_id,
        first_offset,
        last_offset,
    )
}

/// Truncate `s` at the largest char boundary `<= max_bytes` and append a
/// marker that records the original size. Used to bound the portion of an
/// HTTP response body that lands in logs or error variants.
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

fn parse_stream_load_response(body: &str) -> Result<StreamLoadResponse, Error> {
    // An unparsable 200-OK body is almost always a Doris bug, a proxy that
    // injected HTML, or a future schema change — none of those are cured by
    // retrying the same bytes. Default to permanent so the runtime DLQs the
    // batch instead of looping forever.
    serde_json::from_str(body).map_err(|e| {
        Error::PermanentHttpError(format!(
            "Failed to parse Doris stream load response: {e}. Body: {}",
            truncate_for_log(body, MAX_RESPONSE_LOG_BYTES)
        ))
    })
}

/// True for errors the runtime must NOT retry (schema mismatch, bad config,
/// permanent HTTP error, etc.). Used by `consume` to pick the most-severe
/// error across chunks: a transient error from chunk N must NOT shadow a
/// permanent error from chunk M, or the runtime would retry forever instead
/// of routing the batch to its DLQ path.
fn is_permanent_error(e: &Error) -> bool {
    matches!(
        e,
        Error::PermanentHttpError(_) | Error::InvalidConfigValue(_) | Error::InvalidPayloadType
    )
}

/// Replace `slot` with `e` either when it's still empty or when `e` is
/// permanent and the existing entry is transient.
fn record_error(slot: &mut Option<Error>, e: Error) {
    let replace = match slot.as_ref() {
        None => true,
        Some(existing) => is_permanent_error(&e) && !is_permanent_error(existing),
    };
    if replace {
        *slot = Some(e);
    }
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

fn classify_status(response: &StreamLoadResponse) -> Result<(), Error> {
    match response.status.as_str() {
        "Success" => Ok(()),
        "Label Already Exists" => {
            // Idempotent replay — the data was already loaded with this label.
            // Treat as success so the runtime advances the consumer offset.
            info!(
                "Doris reported 'Label Already Exists' (loaded={}, filtered={}); treating as success.",
                response.number_loaded_rows, response.number_filtered_rows
            );
            Ok(())
        }
        "Publish Timeout" => Err(Error::CannotStoreData(format!(
            "Doris stream load Publish Timeout: {}",
            response.message
        ))),
        "Fail" => Err(Error::PermanentHttpError(format!(
            "Doris stream load failed: {}",
            response.message
        ))),
        // Default unknown statuses to permanent: silently retrying an
        // unrecognized failure (e.g. a future Doris error variant) is worse
        // than surfacing it. The runtime will route the batch to its DLQ
        // path instead of hammering the FE forever.
        other => Err(Error::PermanentHttpError(format!(
            "Doris stream load returned unexpected status '{other}': {}",
            response.message
        ))),
    }
}

#[async_trait]
impl Sink for DorisSink {
    async fn open(&mut self) -> Result<(), Error> {
        // Validate the URL up front so a bad config fails at startup rather
        // than on the first batch.
        let parsed_url = reqwest::Url::parse(self.stream_load_url()).map_err(|e| {
            Error::InvalidConfigValue(format!(
                "Doris sink ID {} has invalid fe_url '{}': {e}",
                self.id, self.config.fe_url
            ))
        })?;

        // Constrain `database`/`table` to [A-Za-z0-9_]+ — a stricter subset
        // than Doris's actual identifier rules (which permit `$` and, when
        // backtick-quoted, almost anything). We're deliberately narrower to
        // prevent path traversal in the constructed
        // `/api/{db}/{table}/_stream_load` URL.
        validate_identifier(&self.config.database, "database", self.id)?;
        validate_identifier(&self.config.table, "table", self.id)?;

        // Doris permits passwordless users (e.g. a fresh `root`), so an empty
        // password is valid — but it's almost always a misconfiguration in a
        // real deployment. Warn rather than fail so local/dev setups still work.
        if self.config.password.expose_secret().is_empty() {
            warn!(
                "Doris sink ID {} is configured with an empty password for user '{}'; \
                 this is accepted but is usually a misconfiguration.",
                self.id, self.config.username
            );
        }

        // Warn when credentials would travel in cleartext.
        //
        // NOTE on trust model: this connector intentionally preserves the
        // Authorization header across the FE -> BE 307 redirect (reqwest
        // would otherwise strip it on cross-host redirects). A compromised or
        // MITM'd FE could try to exfiltrate credentials via `Location:
        // http://attacker/` — `validate_redirect` enforces the trust boundary
        // by refusing scheme downgrades (and honoring `allowed_redirect_hosts`)
        // before re-attaching credentials. This warning remains for the case
        // where the FE itself is plain http; deploy Doris over TLS in prod.
        if parsed_url.scheme().eq_ignore_ascii_case("http") {
            let host = parsed_url.host_str().unwrap_or("");
            let is_loopback = host == "localhost" || host == "127.0.0.1" || host == "::1";
            if !is_loopback {
                warn!(
                    "Doris sink ID {} is configured with http:// to non-loopback host '{}'; \
                     credentials and message data will be transmitted in cleartext. \
                     Use https:// in production.",
                    self.id, host
                );
            }
        }

        self.client = Some(self.build_client()?);

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

        let batch_size = self.batch_size();
        let mut worst_error: Option<Error> = None;

        // Best-effort across chunks: on a chunk failure we record the error and
        // `continue` rather than `break`, so a later chunk still gets a chance to
        // land. Under the runtime's AutoCommit-at-poll semantics the consumer
        // offset for this whole poll is already committed before consume() runs,
        // so returning early would *not* replay the unprocessed chunks anyway —
        // it would only drop them. Pushing as much data as possible and then
        // surfacing the worst error (so the runtime's DLQ/alerting still fires)
        // is therefore strictly better than bailing on the first failure.
        for chunk in messages.chunks(batch_size) {
            let json_values: Vec<&simd_json::OwnedValue> = chunk
                .iter()
                .map(|m| match &m.payload {
                    Payload::Json(value) => Ok(value),
                    _ => {
                        error!(
                            "Doris sink ID {} received non-JSON payload (schema={}); aborting batch",
                            self.id, messages_metadata.schema
                        );
                        Err(Error::InvalidPayloadType)
                    }
                })
                .collect::<Result<_, _>>()?;

            if json_values.is_empty() {
                continue;
            }

            let body = match simd_json::to_vec(&json_values) {
                Ok(b) => Bytes::from(b),
                Err(e) => {
                    record_error(
                        &mut worst_error,
                        Error::CannotStoreData(format!("Failed to serialize batch for Doris: {e}")),
                    );
                    continue;
                }
            };

            // `chunks()` never yields an empty slice, so first/last are always
            // present. Use `.expect` rather than `unwrap_or(0)`: a fabricated
            // offset 0 here would alias the real offset-0 label and break the
            // idempotency contract, so a broken invariant must fail loud.
            let first_offset = chunk
                .first()
                .map(|m| m.offset)
                .expect("chunks() never yields an empty chunk");
            let last_offset = chunk
                .last()
                .map(|m| m.offset)
                .expect("chunks() never yields an empty chunk");
            let label = self.build_label(
                topic_metadata,
                messages_metadata.partition_id,
                first_offset,
                last_offset,
            );

            match self.send_stream_load(&label, body).await {
                Ok(response) => match classify_status(&response) {
                    Ok(()) => {
                        if response.number_filtered_rows > 0 {
                            // Filtered rows usually indicate schema drift in
                            // upstream messages. Surface above debug so
                            // operators can alert on it.
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
                    Err(e) => {
                        error!("Doris sink ID {} batch failed: {e}", self.id);
                        record_error(&mut worst_error, e);
                    }
                },
                Err(e) => {
                    error!("Doris sink ID {} HTTP failed: {e}", self.id);
                    record_error(&mut worst_error, e);
                }
            }
        }

        if let Some(err) = worst_error {
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
            timeout_secs: None,
            connect_timeout_secs: None,
            batch_size: None,
            allow_insecure_redirect: None,
            allowed_redirect_hosts: None,
        }
    }

    #[test]
    fn stream_load_url_is_well_formed() {
        let sink = DorisSink::new(1, make_config());
        assert_eq!(
            sink.stream_load_url(),
            "http://localhost:8030/api/test_db/test_tbl/_stream_load"
        );
    }

    #[test]
    fn stream_load_url_strips_trailing_slash() {
        let mut cfg = make_config();
        cfg.fe_url = "http://localhost:8030/".into();
        let sink = DorisSink::new(1, cfg);
        assert!(!sink.stream_load_url().contains("//api"));
    }

    #[test]
    fn label_is_deterministic() {
        let sink = DorisSink::new(1, make_config());
        let topic = TopicMetadata {
            stream: "events".into(),
            topic: "orders".into(),
        };
        let a = sink.build_label(&topic, 7, 100, 199);
        let b = sink.build_label(&topic, 7, 100, 199);
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
        let sink = DorisSink::new(1, make_config());
        let topic = TopicMetadata {
            stream: "events.v1".into(),
            topic: "orders/inbound".into(),
        };
        let label = sink.build_label(&topic, 0, 0, 0);
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
        let sink = DorisSink::new(1, make_config());
        let dotted = TopicMetadata {
            stream: "events.v1".into(),
            topic: "orders".into(),
        };
        let underscored = TopicMetadata {
            stream: "events_v1".into(),
            topic: "orders".into(),
        };
        assert_ne!(
            sink.build_label(&dotted, 0, 0, 0),
            sink.build_label(&underscored, 0, 0, 0),
            "labels must NOT collide for names that sanitize to the same string"
        );
    }

    #[test]
    fn identity_hash_is_not_aliased_by_boundary_shift() {
        // The joint hash is length-prefixed so shifting the stream/topic
        // boundary cannot produce the same digest: `("ab", "c")` must differ
        // from `("a", "bc")`, otherwise two distinct identities could share a
        // label and silently dedupe in Doris.
        assert_ne!(identity_hash("ab", "c"), identity_hash("a", "bc"));
        assert_ne!(
            identity_hash("events", "orders"),
            identity_hash("event", "sorders")
        );
    }

    #[test]
    fn label_stays_under_doris_128_char_cap() {
        // Doris caps Stream Load labels at 128 chars. Build the worst case
        // permitted by the connector: 100-char prefix/stream/topic (all of
        // which get truncated), u64::MAX offsets, u32::MAX partition.
        let mut cfg = make_config();
        cfg.label_prefix = Some("p".repeat(100));
        let sink = DorisSink::new(1, cfg);
        let topic = TopicMetadata {
            stream: "s".repeat(100),
            topic: "t".repeat(100),
        };
        let label = sink.build_label(&topic, u32::MAX, u64::MAX, u64::MAX);
        assert!(
            label.len() <= 128,
            "label exceeds Doris's 128-char cap: {} chars: {label}",
            label.len()
        );
    }

    #[test]
    fn label_prefix_default_is_iggy() {
        let sink = DorisSink::new(1, make_config());
        assert_eq!(sink.label_prefix(), "iggy");
    }

    #[test]
    fn label_prefix_uses_config_value() {
        let mut cfg = make_config();
        cfg.label_prefix = Some("prod".into());
        let sink = DorisSink::new(1, cfg);
        assert_eq!(sink.label_prefix(), "prod");
    }

    #[test]
    fn batch_size_floors_at_one() {
        let mut cfg = make_config();
        cfg.batch_size = Some(0);
        let sink = DorisSink::new(1, cfg);
        assert_eq!(sink.batch_size(), 1);
    }

    #[test]
    fn classify_success_returns_ok() {
        let r = StreamLoadResponse {
            status: "Success".into(),
            message: String::new(),
            number_loaded_rows: 10,
            number_filtered_rows: 0,
        };
        assert!(classify_status(&r).is_ok());
    }

    #[test]
    fn classify_label_already_exists_returns_ok() {
        let r = StreamLoadResponse {
            status: "Label Already Exists".into(),
            message: String::new(),
            number_loaded_rows: 0,
            number_filtered_rows: 0,
        };
        assert!(classify_status(&r).is_ok());
    }

    #[test]
    fn classify_publish_timeout_is_transient() {
        let r = StreamLoadResponse {
            status: "Publish Timeout".into(),
            message: "be unreachable".into(),
            number_loaded_rows: 0,
            number_filtered_rows: 0,
        };
        assert!(matches!(
            classify_status(&r).unwrap_err(),
            Error::CannotStoreData(_)
        ));
    }

    #[test]
    fn classify_fail_is_permanent() {
        let r = StreamLoadResponse {
            status: "Fail".into(),
            message: "schema mismatch".into(),
            number_loaded_rows: 0,
            number_filtered_rows: 0,
        };
        assert!(matches!(
            classify_status(&r).unwrap_err(),
            Error::PermanentHttpError(_)
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
    fn parse_stream_load_response_rejects_garbage_as_permanent() {
        // An unparsable body must surface as PermanentHttpError so the
        // runtime DLQs the batch instead of retrying the same garbage forever.
        let body = "not json";
        assert!(matches!(
            parse_stream_load_response(body).unwrap_err(),
            Error::PermanentHttpError(_)
        ));
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
    fn record_error_prefers_permanent_over_transient() {
        let mut slot: Option<Error> = None;
        record_error(&mut slot, Error::CannotStoreData("transient".into()));
        record_error(&mut slot, Error::PermanentHttpError("permanent".into()));
        // Permanent must shadow transient — otherwise the runtime retries
        // forever instead of routing to DLQ.
        assert!(matches!(slot, Some(Error::PermanentHttpError(_))));
    }

    #[test]
    fn record_error_keeps_permanent_when_transient_arrives_later() {
        let mut slot: Option<Error> = None;
        record_error(&mut slot, Error::PermanentHttpError("p".into()));
        record_error(&mut slot, Error::CannotStoreData("t".into()));
        assert!(matches!(slot, Some(Error::PermanentHttpError(_))));
    }

    fn url(s: &str) -> reqwest::Url {
        reqwest::Url::parse(s).unwrap()
    }

    #[test]
    fn redirect_refuses_https_to_http_downgrade_by_default() {
        // A compromised FE redirecting https -> http would leak Basic creds in
        // cleartext. Refuse it unless explicitly opted in.
        let err = validate_redirect(
            &url("https://fe.doris:8030"),
            &url("http://attacker.evil/"),
            false,
            None,
            1,
        );
        assert!(matches!(err, Err(Error::PermanentHttpError(_))));
    }

    #[test]
    fn redirect_allows_downgrade_when_opted_in() {
        // Known-insecure FE -> BE topology: operator accepts the risk.
        assert!(
            validate_redirect(
                &url("https://fe.doris:8030"),
                &url("http://be.doris:8040/"),
                true,
                None,
                1,
            )
            .is_ok()
        );
    }

    #[test]
    fn redirect_allows_cross_host_same_scheme() {
        // The normal FE -> BE hop: different host, same scheme, no allowlist.
        assert!(
            validate_redirect(
                &url("https://fe.doris:8030"),
                &url("https://be.doris:8040/"),
                false,
                None,
                1,
            )
            .is_ok()
        );
        // http -> http is not a downgrade.
        assert!(
            validate_redirect(
                &url("http://fe.doris:8030"),
                &url("http://be.doris:8040/"),
                false,
                None,
                1,
            )
            .is_ok()
        );
    }

    #[test]
    fn redirect_enforces_host_allowlist_when_set() {
        let allowed = vec!["be1.doris".to_string(), "be2.doris".to_string()];
        // Target host not in the allowlist is refused.
        assert!(matches!(
            validate_redirect(
                &url("http://fe.doris:8030"),
                &url("http://attacker.evil:8040/"),
                false,
                Some(&allowed),
                1,
            ),
            Err(Error::PermanentHttpError(_))
        ));
        // Target host in the allowlist passes.
        assert!(
            validate_redirect(
                &url("http://fe.doris:8030"),
                &url("http://be2.doris:8040/"),
                false,
                Some(&allowed),
                1,
            )
            .is_ok()
        );
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
            .send_stream_load("iggy-test-label", Bytes::from_static(b"[{\"a\":1}]"))
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
            .send_stream_load("iggy-test-label", Bytes::from_static(b"[{\"a\":1}]"))
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
            .send_stream_load("iggy-test-label", Bytes::from_static(b"[{\"a\":1}]"))
            .await;

        assert!(
            matches!(&result, Err(Error::PermanentHttpError(_))),
            "expected PermanentHttpError on missing Location, got {result:?}",
        );
    }
}
