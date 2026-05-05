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
// Doris's FE Stream Load returns 307 to a BE; reqwest's default policy strips
// the Authorization header on cross-host redirects, so we follow redirects
// manually. This cap protects against a misbehaving cluster looping.
const MAX_REDIRECTS: u8 = 5;

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

    /// Build the deterministic label for one batch. Same inputs always produce
    /// the same label, so a replayed batch (after restart, retry, etc.) gets
    /// deduplicated by Doris within its label-keep window.
    fn build_label(
        &self,
        topic_metadata: &TopicMetadata,
        partition_id: u32,
        first_offset: u64,
        last_offset: u64,
    ) -> String {
        format!(
            "{}-{}-{}-{}-{}-{}",
            sanitize_label(self.label_prefix()),
            sanitize_label(&topic_metadata.stream),
            sanitize_label(&topic_metadata.topic),
            partition_id,
            first_offset,
            last_offset,
        )
    }

    async fn send_stream_load(
        &self,
        label: &str,
        body: Vec<u8>,
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

            if !status.is_success() {
                let msg = format!(
                    "Doris sink ID {} stream load returned HTTP {status}: {response_text}",
                    self.id
                );
                error!("{msg}");
                return Err(if status.is_server_error() {
                    Error::CannotStoreData(msg)
                } else {
                    Error::PermanentHttpError(msg)
                });
            }

            return parse_stream_load_response(&response_text);
        }
    }
}

/// Replace Doris-label-illegal characters with `_`. Doris allows
/// [A-Za-z0-9_-]. Anything else -> `_`.
fn sanitize_label(value: &str) -> String {
    value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn parse_stream_load_response(body: &str) -> Result<StreamLoadResponse, Error> {
    let parsed: StreamLoadResponse = serde_json::from_str(body).map_err(|e| {
        Error::CannotStoreData(format!(
            "Failed to parse Doris stream load response: {e}. Body: {body}"
        ))
    })?;
    Ok(parsed)
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
        reqwest::Url::parse(&self.stream_load_url()).map_err(|e| {
            Error::InvalidConfigValue(format!(
                "Doris sink ID {} has invalid fe_url '{}': {e}",
                self.id, self.config.fe_url
            ))
        })?;

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
        let mut first_error: Option<Error> = None;

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
                Ok(b) => b,
                Err(e) => {
                    let err =
                        Error::CannotStoreData(format!("Failed to serialize batch for Doris: {e}"));
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
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
                        info!(
                            "Doris sink ID {} loaded {} rows (filtered={}) into {}.{} (label={label})",
                            self.id,
                            response.number_loaded_rows,
                            response.number_filtered_rows,
                            self.config.database,
                            self.config.table,
                        );
                    }
                    Err(e) => {
                        error!("Doris sink ID {} batch failed: {e}", self.id);
                        if first_error.is_none() {
                            first_error = Some(e);
                        }
                    }
                },
                Err(e) => {
                    error!("Doris sink ID {} HTTP failed: {e}", self.id);
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
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
        assert_eq!(a, "iggy-events-orders-7-100-199");
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
        assert_eq!(label, "iggy-events_v1-orders_inbound-0-0-0");
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
    fn parse_stream_load_response_rejects_garbage() {
        let body = "not json";
        assert!(parse_stream_load_response(body).is_err());
    }

    #[test]
    fn auth_header_is_basic_b64() {
        let sink = DorisSink::new(1, make_config());
        // base64("root:pw") = cm9vdDpwdw==
        assert_eq!(sink.auth_header.expose_secret(), "Basic cm9vdDpwdw==");
    }
}
