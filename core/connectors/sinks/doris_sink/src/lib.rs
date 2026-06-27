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
use serde::Deserialize;
use simd_json::{OwnedValue, StaticNode};
use std::io::Write as _;
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
// A single 64-bit (16-hex) joint hash over the raw (prefix, stream, topic)
// triple. 64 bits keeps the adversarial birthday bound high enough that a
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
// transport error, Doris "Publish Timeout"). The runtime commits the consumer
// offset at poll time before consume() runs, so a transient failure we don't
// retry here is lost — an in-request re-PUT under the same label (which Doris
// dedupes) is the connector's only redelivery lever. Keep the worst-case budget
// well inside Doris's label_keep_max_second so a retry still dedupes.
const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_RETRY_DELAY: &str = "200ms";
const DEFAULT_RETRY_MAX_DELAY: &str = "5s";
// Doris CSV framing. Control-char separators keep field/row boundaries
// collision-improbable; `enclose`+`escape` protect any value that still contains
// them. Doris escapes with a prefix char (`\"`), NOT RFC-4180 quote-doubling,
// which is why we hand-roll the encoder instead of using the `csv` crate. The
// four bytes are sent to Doris as the header forms below (it decodes the `\xNN`
// prefix); all are visible ASCII so `validated_header` accepts them.
const CSV_COLUMN_SEPARATOR: u8 = 0x01; // SOH
const CSV_LINE_DELIMITER: u8 = 0x02; // STX
const CSV_ENCLOSE: u8 = b'"';
const CSV_ESCAPE: u8 = b'\\';
const CSV_NULL_MARKER: &[u8] = b"\\N";
const CSV_COLUMN_SEPARATOR_HEADER: &str = "\\x01";
const CSV_LINE_DELIMITER_HEADER: &str = "\\x02";
const CSV_ENCLOSE_HEADER: &str = "\"";
const CSV_ESCAPE_HEADER: &str = "\\";

/// Stream Load output serialization. `Json` (default) is name-mapped and handles
/// embedded separators/quotes/newlines natively; `Csv` is opt-in for throughput
/// and is positional, so it requires the `columns` config to pin column order.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Format {
    #[default]
    Json,
    Csv,
}

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
    retry_max_delay: Duration,
    // Output format resolved once at `open()`. For `Csv`, `csv_columns` holds the
    // positional column order (the leading bare names from the `columns` config);
    // it is empty for `Json`.
    format: Format,
    csv_columns: Vec<String>,
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
    /// Stream Load output format: `"json"` (default) or `"csv"`. CSV is opt-in
    /// for throughput; because Doris CSV is positional (JSON is name-mapped), it
    /// requires `columns` to pin the column order, else `open()` fails. Named
    /// `output_format` (not `format`) so the env override
    /// `..._PLUGIN_CONFIG_OUTPUT_FORMAT` doesn't collide with the runtime's
    /// top-level `plugin_config_format` (both would flatten to `..._FORMAT`).
    pub output_format: Option<Format>,
    /// Total per-request HTTP timeout as a human-readable duration, e.g. "30s"
    /// (default 30s). Matches the `timeout` field on the http/influxdb sinks.
    pub timeout: Option<String>,
    /// TCP connect timeout as a human-readable duration, e.g. "5s". Independent
    /// of `timeout` (the total request budget). Defaults to 5s; raise it for
    /// cross-region or cold-start FEs that are slow to accept the connection.
    pub connect_timeout: Option<String>,
    pub batch_size: Option<u32>,
    /// Total number of Stream Load attempts per batch on a *transient* failure
    /// (HTTP 5xx/408/429, a transport error, or Doris "Publish Timeout"). `1`
    /// disables retries. Each retry re-PUTs under the same label — which Doris
    /// dedupes — so an ambiguous success (e.g. a 2xx whose body we couldn't
    /// read) is absorbed rather than doubled. Default 3.
    pub max_retries: Option<u32>,
    /// Base backoff before the first retry, as a human-readable duration (e.g.
    /// "200ms"). Doubles each attempt up to `max_retry_delay`, with ±20% jitter.
    pub retry_delay: Option<String>,
    /// Upper bound on a single retry backoff, as a human-readable duration (e.g.
    /// "5s"). Keep `max_retries * max_retry_delay` inside Doris's
    /// `label_keep_max_second` so a retry still dedupes instead of re-loading.
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
        let timeout = parse_duration(self.config.timeout.as_deref(), DEFAULT_TIMEOUT);
        let connect_timeout = parse_duration(
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
        label: &str,
        body: Bytes,
    ) -> Result<StreamLoadResponse, Error> {
        let connected = self.connected.as_ref().ok_or_else(|| {
            Error::InitError(format!(
                "Doris sink ID {} called before open() — not connected",
                self.id
            ))
        })?;
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
                .header("label", label)
                .body(body.clone());

            // Format-specific Stream Load headers. JSON is name-mapped and uses
            // `strip_outer_array` (the body is a JSON array); CSV is positional
            // with control-char framing + enclose/escape (no strip_outer_array).
            request = match connected.format {
                Format::Json => request
                    .header("format", "json")
                    .header("strip_outer_array", "true"),
                Format::Csv => request
                    .header("format", "csv")
                    .header("column_separator", CSV_COLUMN_SEPARATOR_HEADER)
                    .header("line_delimiter", CSV_LINE_DELIMITER_HEADER)
                    .header("enclose", CSV_ENCLOSE_HEADER)
                    .header("escape", CSV_ESCAPE_HEADER),
            };

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
                    // Doris's dedupe reveals the real outcome instead of DLQing a
                    // success.
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
                    // classifies the outcome (empty body on a non-2xx => permanent).
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
                // 408/429 are 4xx but transient — retry them, don't DLQ.
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
    /// whose body we couldn't read). A `PermanentHttpError` returns immediately —
    /// retrying bad data would just hammer the FE.
    async fn load_batch(&self, label: &str, body: Bytes) -> Result<StreamLoadResponse, Error> {
        let connected = self.connected.as_ref().ok_or_else(|| {
            Error::InitError(format!(
                "Doris sink ID {} called before open() — not connected",
                self.id
            ))
        })?;

        let mut attempt = 0u32;
        loop {
            let result = match self.send_stream_load(label, body.clone()).await {
                Ok(response) => classify_status(&response).map(|()| response),
                Err(error) => Err(error),
            };
            let error = match result {
                Ok(response) => return Ok(response),
                Err(error) => error,
            };

            attempt += 1;
            if attempt >= connected.max_retries || !is_transient_error(&error) {
                return Err(error);
            }

            let delay = jitter(exponential_backoff(
                connected.retry_delay,
                attempt - 1,
                connected.retry_max_delay,
            ));
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
/// a warning on a malformed *or zero* value. Mirrors the http/influxdb sinks.
///
/// A zero duration parses fine but is degenerate: reqwest treats a zero
/// timeout/connect-timeout as an immediate deadline, so every request fails with
/// a `TimedOut` error before it can complete. Treat it like a malformed value.
fn parse_duration(input: Option<&str>, default: &str) -> Duration {
    let raw = input.unwrap_or(default);
    let fallback = || *HumanDuration::from_str(default).expect("default duration must be valid");
    let parsed = HumanDuration::from_str(raw)
        .map(|d| *d)
        .unwrap_or_else(|e| {
            warn!("Invalid duration '{raw}': {e}, using default '{default}'");
            fallback()
        });
    if parsed.is_zero() {
        warn!(
            "Duration '{raw}' is zero, which would time out every request immediately; \
             using default '{default}'"
        );
        return fallback();
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

/// Total Stream Load attempts per batch: the configured value floored at 1 so
/// `0` (or an unset value) collapses to a single attempt with no retry.
fn effective_max_retries(configured: Option<u32>) -> u32 {
    configured.unwrap_or(DEFAULT_MAX_RETRIES).max(1)
}

/// The leading positional column names from a Doris `columns` header: bare names
/// in order, stopping at the first derived `name = expr` entry (Doris computes
/// those server-side and they consume no CSV field). Empty if there are none.
fn csv_column_names(columns: &str) -> Vec<String> {
    columns
        .split(',')
        .map(str::trim)
        .take_while(|name| !name.is_empty() && !name.contains('='))
        .map(str::to_string)
        .collect()
}

/// Serialize a chunk into the request body for the configured Stream Load format.
fn build_request_body(
    format: Format,
    rows: &[&OwnedValue],
    csv_columns: &[String],
) -> Result<Bytes, Error> {
    match format {
        Format::Json => simd_json::to_vec(&rows).map(Bytes::from).map_err(|e| {
            Error::CannotStoreData(format!("Failed to serialize JSON batch for Doris: {e}"))
        }),
        Format::Csv => build_csv_body(rows, csv_columns).map(Bytes::from),
    }
}

/// Serialize JSON object rows into Doris CSV: one `enclose`-quoted/escaped field
/// per column in `columns` order, control-char separated. A missing key or JSON
/// `null` becomes the unenclosed `\N` NULL marker; an empty string becomes the
/// enclosed `""` (the distinction Doris draws between NULL and empty). Numbers
/// and bools are emitted bare; nested values are compact-JSON-stringified.
fn build_csv_body(rows: &[&OwnedValue], columns: &[String]) -> Result<Vec<u8>, Error> {
    let mut out = Vec::with_capacity(rows.len() * columns.len() * 16);
    for &row in rows {
        let OwnedValue::Object(object) = row else {
            return Err(Error::InvalidRecordValue(
                "Doris CSV format requires each message to be a JSON object".to_string(),
            ));
        };
        for (index, column) in columns.iter().enumerate() {
            if index > 0 {
                out.push(CSV_COLUMN_SEPARATOR);
            }
            match object.get(column.as_str()) {
                None => out.extend_from_slice(CSV_NULL_MARKER),
                Some(value) => encode_csv_field(value, &mut out),
            }
        }
        out.push(CSV_LINE_DELIMITER);
    }
    Ok(out)
}

/// Append one JSON value as a Doris CSV field (see `build_csv_body`).
fn encode_csv_field(value: &OwnedValue, out: &mut Vec<u8>) {
    match value {
        OwnedValue::Static(StaticNode::Null) => out.extend_from_slice(CSV_NULL_MARKER),
        OwnedValue::Static(StaticNode::Bool(flag)) => {
            out.extend_from_slice(if *flag { b"true" } else { b"false" });
        }
        OwnedValue::Static(StaticNode::I64(number)) => {
            let _ = write!(out, "{number}");
        }
        OwnedValue::Static(StaticNode::U64(number)) => {
            let _ = write!(out, "{number}");
        }
        OwnedValue::Static(StaticNode::F64(number)) => {
            let _ = write!(out, "{number}");
        }
        OwnedValue::String(text) => encode_csv_enclosed(text.as_bytes(), out),
        // Nested array/object: stringify to compact JSON and enclose as text. The
        // target Doris column must accept it (STRING/JSON/VARIANT).
        nested => match simd_json::to_string(nested) {
            Ok(json) => encode_csv_enclosed(json.as_bytes(), out),
            Err(_) => out.extend_from_slice(CSV_NULL_MARKER),
        },
    }
}

/// Append `bytes` as an `enclose`-quoted CSV field, prefix-escaping any embedded
/// escape or enclose byte. The escape char is handled by the same branch, so a
/// trailing backslash cannot leak past the closing quote.
fn encode_csv_enclosed(bytes: &[u8], out: &mut Vec<u8>) {
    out.push(CSV_ENCLOSE);
    for &byte in bytes {
        if byte == CSV_ESCAPE || byte == CSV_ENCLOSE {
            out.push(CSV_ESCAPE);
        }
        out.push(byte);
    }
    out.push(CSV_ENCLOSE);
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

/// A single blake3 fingerprint over the *raw* (unsanitized) `prefix`, `stream`,
/// and `topic`, truncated to `LABEL_HASH_HEX_LEN` hex chars. This disambiguates
/// identities that sanitize+truncate to the same string (e.g. `events.v1` vs
/// `events_v1`, or prefixes `prod_events_us_east_1` vs `..._2`), which would
/// otherwise produce identical labels and cause silent data loss via Doris's
/// server-side label dedupe. Inputs are length-prefixed so distinct triples
/// can't alias into one digest (e.g. `("ab","c",..)` vs `("a","bc",..)`).
fn identity_hash(prefix: &str, stream: &str, topic: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    for part in [prefix, stream, topic] {
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
/// case 120), and the joint `hash16` over the raw (prefix, stream, topic) keeps
/// labels distinct even when the sanitized segments collide.
///
/// `#[doc(hidden)]`: `pub` only so the integration test harness can reproduce
/// labels; not part of the connector's supported API.
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
        identity_hash(prefix, stream, topic),
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

fn parse_stream_load_response(body: &str) -> Result<StreamLoadResponse, Error> {
    // An unparsable 200-OK body (Doris bug, proxy-injected HTML, future schema
    // change) isn't cured by retrying the same bytes — default to permanent so
    // the runtime DLQs the batch instead of looping.
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

/// Transient Stream Load failures worth an in-request retry: HTTP 5xx/408/429
/// and transport errors (`CannotStoreData` / `HttpRequestFailed`), plus Doris's
/// "Publish Timeout" (also `CannotStoreData`). `PermanentHttpError` (4xx, "Fail",
/// schema/redirect problems, unparsable body) is never retried — re-PUTing bad
/// data just hammers the FE.
fn is_transient_error(error: &Error) -> bool {
    matches!(
        error,
        Error::CannotStoreData(_) | Error::HttpRequestFailed(_)
    )
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
        // Default unknown statuses to permanent: surfacing an unrecognized
        // failure (e.g. a future Doris error variant) and letting the runtime DLQ
        // it beats silently retrying it against the FE forever.
        other => Err(Error::PermanentHttpError(format!(
            "Doris stream load returned unexpected status '{other}': {}",
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
        let retry_max_delay = parse_duration(
            self.config.max_retry_delay.as_deref(),
            DEFAULT_RETRY_MAX_DELAY,
        );
        // `exponential_backoff` already caps at the max, but a base above the cap
        // is a config mistake worth surfacing rather than silently flattening.
        let (retry_delay, retry_max_delay) = if retry_delay > retry_max_delay {
            warn!(
                "Doris sink ID {}: retry_delay ({retry_delay:?}) exceeds max_retry_delay ({retry_max_delay:?}); clamping base to the cap",
                self.id
            );
            (retry_max_delay, retry_max_delay)
        } else {
            (retry_delay, retry_max_delay)
        };

        let format = self.config.output_format.unwrap_or_default();
        // CSV is positional, so it needs the explicit column order; JSON is
        // name-mapped and ignores it. Resolve (and validate) once at open().
        let csv_columns = match format {
            Format::Json => Vec::new(),
            Format::Csv => {
                let columns = self
                    .config
                    .columns
                    .as_deref()
                    .map(csv_column_names)
                    .unwrap_or_default();
                if columns.is_empty() {
                    return Err(Error::InvalidConfigValue(format!(
                        "Doris sink ID {}: format=\"csv\" requires a non-empty `columns` listing \
                         the source columns in order (CSV is positional, unlike name-mapped JSON)",
                        self.id
                    )));
                }
                columns
            }
        };

        self.connected = Some(Connected {
            client: self.build_client()?,
            base_url,
            max_filter_ratio_header,
            columns_header,
            where_header,
            allow_insecure_redirect: self.config.allow_insecure_redirect.unwrap_or(false),
            allowed_redirect_hosts: self.config.allowed_redirect_hosts.clone(),
            max_retries: effective_max_retries(self.config.max_retries),
            retry_delay,
            retry_max_delay,
            format,
            csv_columns,
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
        let connected = self.connected.as_ref().ok_or_else(|| {
            Error::InitError(format!(
                "Doris sink ID {} called before open() — not connected",
                self.id
            ))
        })?;
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

            let body =
                match build_request_body(connected.format, &json_values, &connected.csv_columns) {
                    Ok(body) => body,
                    Err(e) => {
                        error!("Doris sink ID {} failed to serialize batch: {e}", self.id);
                        first_error.get_or_insert(e);
                        continue;
                    }
                };

            let label = build_label(
                label_prefix,
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
            output_format: None,
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
        let a = build_label("iggy", "events", "orders", 7, 100, 199);
        let b = build_label("iggy", "events", "orders", 7, 100, 199);
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
        let label = build_label("iggy", "events.v1", "orders/inbound", 0, 0, 0);
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
            build_label("iggy", "events.v1", "orders", 0, 0, 0),
            build_label("iggy", "events_v1", "orders", 0, 0, 0),
            "labels must NOT collide for names that sanitize to the same string"
        );
    }

    #[test]
    fn label_disambiguates_prefixes_that_sanitize_identically() {
        // Two connectors writing the same stream/topic/partition/offset range
        // but with prefixes that collapse to the same sanitized+truncated
        // segment must still get distinct labels — otherwise Doris's label
        // dedupe silently drops the second tenant's batch.
        let a = build_label("prod_events_us_east_1", "events", "orders", 0, 0, 0);
        let b = build_label("prod_events_us_east_2", "events", "orders", 0, 0, 0);
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
    fn identity_hash_is_not_aliased_by_boundary_shift() {
        // The joint hash is length-prefixed so shifting any boundary cannot
        // produce the same digest: distinct (prefix, stream, topic) triples must
        // map to distinct hashes, otherwise two identities could share a label
        // and silently dedupe in Doris.
        assert_ne!(
            identity_hash("iggy", "ab", "c"),
            identity_hash("iggy", "a", "bc")
        );
        assert_ne!(
            identity_hash("iggy", "events", "orders"),
            identity_hash("iggy", "event", "sorders")
        );
        // The prefix participates too: shifting the prefix/stream boundary must
        // not alias.
        assert_ne!(
            identity_hash("ab", "c", "topic"),
            identity_hash("a", "bc", "topic")
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
        let label = build_label(&prefix, &stream, &topic, u32::MAX, u64::MAX, u64::MAX);
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
    fn parse_duration_parses_and_falls_back() {
        assert_eq!(parse_duration(Some("10s"), "30s"), Duration::from_secs(10));
        assert_eq!(parse_duration(None, "30s"), Duration::from_secs(30));
        // A malformed value falls back to the default rather than erroring.
        assert_eq!(
            parse_duration(Some("not_a_duration"), "30s"),
            Duration::from_secs(30)
        );
        // A zero duration is degenerate (reqwest times out every request
        // immediately) and falls back to the default.
        assert_eq!(parse_duration(Some("0s"), "30s"), Duration::from_secs(30));
        assert_eq!(parse_duration(Some("0ms"), "5s"), Duration::from_secs(5));
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
            retry_max_delay: Duration::from_millis(5),
            format: Format::Json,
            csv_columns: Vec::new(),
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
        let mut sink = DorisSink::new(1, make_config());
        sink.open().await.expect("open should succeed");
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
        let mut sink = DorisSink::new(1, make_config());
        sink.open().await.expect("open should succeed");
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
            .send_stream_load("iggy-test-label", Bytes::from_static(b"[{\"a\":1}]"))
            .await;

        assert!(
            matches!(&result, Err(Error::PermanentHttpError(_))),
            "expected PermanentHttpError on relative Location, got {result:?}",
        );
    }

    /// A transient failure (HTTP 503) is retried in-request, and the next
    /// attempt's success is returned. The `.expect(1)` on the 503 mock proves
    /// the retry actually re-PUT (a single attempt would have surfaced the
    /// transient error instead of `Ok(Success)`).
    #[tokio::test]
    async fn transient_failure_is_retried_then_succeeds() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let mut cfg = make_config();
        cfg.fe_url = server.uri();
        cfg.max_retries = Some(3);
        cfg.retry_delay = Some("1ms".into());
        cfg.max_retry_delay = Some("5ms".into());

        // wiremock serves the first matching mock in mount order, so mount the
        // single-shot 503 first: it serves attempt 1, then — capped at one
        // response — stops matching, and attempt 2 falls through to the success.
        Mock::given(method("PUT"))
            .and(path("/api/test_db/test_tbl/_stream_load"))
            .respond_with(ResponseTemplate::new(503))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/api/test_db/test_tbl/_stream_load"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"Status": "Success", "NumberLoadedRows": 1})),
            )
            .mount(&server)
            .await;

        let mut sink = DorisSink::new(1, cfg);
        sink.open().await.expect("open should succeed");
        let result = sink
            .load_batch("iggy-test-label", Bytes::from_static(b"[{\"a\":1}]"))
            .await;

        assert!(
            matches!(&result, Ok(r) if r.status == "Success"),
            "expected Ok(Success) after one retry, got {result:?}",
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

    fn owned(json: &str) -> OwnedValue {
        let mut bytes = json.as_bytes().to_vec();
        simd_json::to_owned_value(&mut bytes).expect("valid JSON")
    }

    #[test]
    fn format_deserializes_from_lowercase() {
        assert_eq!(
            serde_json::from_str::<Format>("\"json\"").unwrap(),
            Format::Json
        );
        assert_eq!(
            serde_json::from_str::<Format>("\"csv\"").unwrap(),
            Format::Csv
        );
        assert!(serde_json::from_str::<Format>("\"xml\"").is_err());
        assert_eq!(Format::default(), Format::Json);
    }

    #[test]
    fn csv_column_names_takes_leading_bare_names() {
        assert_eq!(csv_column_names("id, name, count"), ["id", "name", "count"]);
        // A derived `name = expr` column (and anything after) is server-side.
        assert_eq!(
            csv_column_names("id, name, calc = count + 1"),
            ["id", "name"]
        );
        assert_eq!(csv_column_names(" a ,b, c "), ["a", "b", "c"]);
        assert!(csv_column_names("").is_empty());
        assert!(csv_column_names("calc = x").is_empty());
    }

    #[test]
    fn build_csv_body_encodes_scalars_nulls_and_missing() {
        let row = owned(r#"{"i":-5,"u":10,"f":2.5,"b":false,"nul":null,"empty":""}"#);
        let columns = ["i", "u", "f", "b", "nul", "missing", "empty"].map(String::from);
        let body = build_csv_body(&[&row], &columns).unwrap();
        assert_eq!(*body.last().unwrap(), CSV_LINE_DELIMITER);
        let fields: Vec<&[u8]> = body[..body.len() - 1]
            .split(|&byte| byte == CSV_COLUMN_SEPARATOR)
            .collect();
        assert_eq!(
            fields,
            vec![
                &b"-5"[..],  // i64
                &b"10"[..],  // u64
                &b"2.5"[..], // f64
                &b"false"[..],
                &b"\\N"[..],  // json null
                &b"\\N"[..],  // missing key
                &b"\"\""[..], // empty string -> enclosed empty (distinct from null)
            ],
        );
    }

    #[test]
    fn build_csv_body_prefix_escapes_enclose_and_escape_bytes() {
        // The value contains a quote and a backslash; both must be prefix-escaped
        // inside the enclosure (escape the escape char too, in one left-to-right
        // pass, so a trailing backslash can't leak past the closing quote).
        let row = owned(r#"{"v":"a\"b\\c"}"#);
        let columns = ["v"].map(String::from);
        let body = build_csv_body(&[&row], &columns).unwrap();
        let expected: &[u8] = &[
            b'"',
            b'a',
            b'\\',
            b'"',
            b'b',
            b'\\',
            b'\\',
            b'c',
            b'"',
            CSV_LINE_DELIMITER,
        ];
        assert_eq!(body, expected);
    }

    #[test]
    fn build_csv_body_keeps_embedded_separator_and_newline_inside_enclosure() {
        // Raw separator (0x01), line delimiter (0x02), and a newline live inside
        // the value; the enclosure protects them, so they are emitted literally
        // (Doris reads them as data, not structure) and are NOT escaped.
        let row = owned("{\"v\":\"a\\u0001b\\u0002c\\nd\"}");
        let columns = ["v"].map(String::from);
        let body = build_csv_body(&[&row], &columns).unwrap();
        let expected: &[u8] = &[
            b'"',
            b'a',
            0x01,
            b'b',
            0x02,
            b'c',
            b'\n',
            b'd',
            b'"',
            CSV_LINE_DELIMITER,
        ];
        assert_eq!(body, expected);
    }

    #[test]
    fn build_csv_body_stringifies_nested_values() {
        let row = owned(r#"{"o":{"k":1}}"#);
        let columns = ["o"].map(String::from);
        let body = build_csv_body(&[&row], &columns).unwrap();
        // {"k":1} enclosed, with its inner quotes prefix-escaped.
        let expected: &[u8] = &[
            b'"',
            b'{',
            b'\\',
            b'"',
            b'k',
            b'\\',
            b'"',
            b':',
            b'1',
            b'}',
            b'"',
            CSV_LINE_DELIMITER,
        ];
        assert_eq!(body, expected);
    }

    #[test]
    fn build_csv_body_rejects_non_object_row() {
        let row = owned("[1, 2, 3]");
        let columns = ["v"].map(String::from);
        assert!(matches!(
            build_csv_body(&[&row], &columns),
            Err(Error::InvalidRecordValue(_))
        ));
    }

    #[tokio::test]
    async fn open_rejects_csv_format_without_columns() {
        let mut cfg = make_config();
        cfg.output_format = Some(Format::Csv);
        cfg.columns = None;
        let mut sink = DorisSink::new(1, cfg);
        assert!(matches!(
            sink.open().await,
            Err(Error::InvalidConfigValue(_))
        ));
    }

    #[tokio::test]
    async fn open_resolves_csv_columns_dropping_derived() {
        let mut cfg = make_config();
        cfg.output_format = Some(Format::Csv);
        cfg.columns = Some("id, name, calc = id + 1".into());
        let mut sink = DorisSink::new(1, cfg);
        sink.open().await.expect("open should succeed");
        let connected = sink.connected.as_ref().unwrap();
        assert_eq!(connected.format, Format::Csv);
        assert_eq!(connected.csv_columns, ["id", "name"]);
    }
}
