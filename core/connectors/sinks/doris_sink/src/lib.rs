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
const LABEL_HASH_HEX_LEN: usize = 8;
// Doris Stream Load responses are normally tiny JSON blobs (~200 B). Cap the
// portion we keep for logs/errors so a misbehaving proxy that returns a giant
// body cannot OOM the connector or flood logs.
const MAX_RESPONSE_LOG_BYTES: usize = 4096;

#[derive(Debug)]
pub struct DorisSink {
    id: u32,
    config: DorisSinkConfig,
    client: Option<reqwest::Client>,
    auth_header: SecretString,
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
    pub batch_size: Option<u32>,
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
        let auth_header = SecretString::from(format!(
            "Basic {}",
            general_purpose::STANDARD.encode(credential)
        ));

        DorisSink {
            id,
            config,
            client: None,
            auth_header,
        }
    }

    fn build_client(&self) -> Result<reqwest::Client, Error> {
        let timeout = Duration::from_secs(self.config.timeout_secs.unwrap_or(DEFAULT_TIMEOUT_SECS));
        // We follow redirects manually to keep the Authorization header alive
        // across the FE -> BE hop. reqwest strips Authorization on cross-host
        // 307s by design.
        reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(timeout)
            .connect_timeout(Duration::from_secs(CONNECT_TIMEOUT_SECS))
            .build()
            .map_err(|e| Error::InitError(format!("Failed to build Doris HTTP client: {e}")))
    }

    fn stream_load_url(&self) -> String {
        format!(
            "{}/api/{}/{}/_stream_load",
            self.config.fe_url.trim_end_matches('/'),
            self.config.database,
            self.config.table,
        )
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
        let mut url = self.stream_load_url();
        let mut redirects = 0u8;

        loop {
            let mut request = client
                .request(Method::PUT, &url)
                .header(header::AUTHORIZATION, self.auth_header.expose_secret())
                .header(header::EXPECT, "100-continue")
                .header("format", "json")
                .header("strip_outer_array", "true")
                .header("label", label)
                .body(body.clone());

            if let Some(ratio) = self.config.max_filter_ratio {
                request = request.header("max_filter_ratio", ratio.to_string());
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
                    return Err(Error::HttpRequestFailed(format!(
                        "Doris sink ID {} exceeded max redirects ({MAX_REDIRECTS})",
                        self.id
                    )));
                }
                let Some(location) = response
                    .headers()
                    .get(header::LOCATION)
                    .and_then(|v| v.to_str().ok())
                else {
                    return Err(Error::HttpRequestFailed(format!(
                        "Doris sink ID {} got {status} with no Location header",
                        self.id
                    )));
                };
                debug!("Doris sink ID {} following redirect to {location}", self.id);
                url = location.to_string();
                continue;
            }

            let response_text = response.text().await.unwrap_or_default();
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

/// Sanitize + truncate a stream/topic name and append a short blake3 hash
/// of the *raw* (unsanitized) name. The hash disambiguates names that
/// sanitize to the same string (e.g. `events.v1` vs `events_v1`), which
/// would otherwise produce identical labels and cause silent data loss
/// via Doris's server-side label dedupe.
fn label_segment_with_hash(value: &str) -> String {
    let san = sanitize_segment(value, MAX_LABEL_NAME_LEN);
    let hash = blake3::hash(value.as_bytes()).to_hex();
    format!("{san}_{}", &hash.as_str()[..LABEL_HASH_HEX_LEN])
}

/// Pure label builder. Format:
/// `{prefix}-{stream_san}_{hash8}-{topic_san}_{hash8}-{partition}-{first}-{last}`.
///
/// Exposed as a `pub` free function so integration tests can produce the
/// exact same label without instantiating a `DorisSink`. The format is
/// engineered to:
///   1. Bound the total length under Doris's 128-char label cap regardless
///      of input name length.
///   2. Eliminate the silent-collision risk between names that sanitize to
///      the same string (the hash is over the raw name).
pub fn build_label(
    prefix: &str,
    stream: &str,
    topic: &str,
    partition_id: u32,
    first_offset: u64,
    last_offset: u64,
) -> String {
    format!(
        "{}-{}-{}-{}-{}-{}",
        sanitize_segment(prefix, MAX_LABEL_PREFIX_LEN),
        label_segment_with_hash(stream),
        label_segment_with_hash(topic),
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
    // An unparseable 200-OK body is almost always a Doris bug, a proxy that
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
            "Doris sink ID {id}: {field} '{name}' must match [A-Za-z0-9_]+ (Doris identifier rules)"
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
        let parsed_url = reqwest::Url::parse(&self.stream_load_url()).map_err(|e| {
            Error::InvalidConfigValue(format!(
                "Doris sink ID {} has invalid fe_url '{}': {e}",
                self.id, self.config.fe_url
            ))
        })?;

        // Reject `database`/`table` outside Doris's identifier alphabet.
        // Doris would reject them server-side anyway, but rejecting here
        // also prevents path traversal in the constructed
        // `/api/{db}/{table}/_stream_load` URL.
        validate_identifier(&self.config.database, "database", self.id)?;
        validate_identifier(&self.config.table, "table", self.id)?;

        // Warn when credentials would travel in cleartext.
        //
        // NOTE on trust model: this connector intentionally preserves the
        // Authorization header across the FE -> BE 307 redirect (reqwest
        // would otherwise strip it on cross-host redirects). That means a
        // compromised or MITM'd FE could exfiltrate credentials by responding
        // with `Location: http://attacker/`. The cleartext warning is the
        // primary mitigation; for hostile networks, deploy Doris over TLS.
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

        for chunk in messages.chunks(batch_size) {
            let json_values: Vec<&simd_json::OwnedValue> = chunk
                .iter()
                .filter_map(|m| match &m.payload {
                    Payload::Json(value) => Some(value),
                    _ => {
                        warn!(
                            "Doris sink ID {} dropping non-JSON payload (schema={})",
                            self.id, messages_metadata.schema
                        );
                        None
                    }
                })
                .collect();

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

            let first_offset = chunk.first().map(|m| m.offset).unwrap_or(0);
            let last_offset = chunk.last().map(|m| m.offset).unwrap_or(0);
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
            batch_size: None,
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
        // Format: {prefix}-{stream_san}_{hash8}-{topic_san}_{hash8}-{partition}-{first}-{last}
        let parts: Vec<&str> = a.split('-').collect();
        assert_eq!(parts.len(), 6);
        assert_eq!(parts[0], "iggy");
        assert!(parts[1].starts_with("events_"));
        assert_eq!(parts[1].len(), "events_".len() + LABEL_HASH_HEX_LEN);
        assert!(parts[2].starts_with("orders_"));
        assert_eq!(parts[2].len(), "orders_".len() + LABEL_HASH_HEX_LEN);
        assert_eq!(parts[3], "7");
        assert_eq!(parts[4], "100");
        assert_eq!(parts[5], "199");
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
        // The whole point of the hash suffix: `events.v1` and `events_v1`
        // collapse to the same sanitized form, but the hash is over the raw
        // name so the final labels differ. Without this, two streams could
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
        // An unparseable body must surface as PermanentHttpError so the
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

    #[test]
    fn auth_header_is_basic_b64() {
        let sink = DorisSink::new(1, make_config());
        // base64("root:pw") = cm9vdDpwdw==
        assert_eq!(sink.auth_header.expose_secret(), "Basic cm9vdDpwdw==");
    }
}
