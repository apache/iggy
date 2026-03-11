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
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata, sink_connector,
};
use serde::{Deserialize, Serialize};
use std::sync::atomic::AtomicU64;
use tracing::info;

sink_connector!(HttpSink);

/// HTTP method enum — validated at deserialization, prevents invalid values like "DELET" or "GETS".
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum HttpMethod {
    Get,
    Head,
    #[default]
    Post,
    Put,
    Patch,
    Delete,
}

/// Payload formatting mode for HTTP requests.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BatchMode {
    /// One HTTP request per message (default). Note: with batch_length=50, this produces 50
    /// sequential HTTP round trips per poll cycle. Use ndjson or json_array for higher throughput.
    #[default]
    Individual,
    /// All messages in one request, newline-delimited JSON.
    Ndjson,
    /// All messages as a single JSON array.
    JsonArray,
    /// Raw bytes, one request per message (for non-JSON payloads).
    Raw,
}

/// Configuration for the HTTP sink connector, deserialized from [plugin_config] in config.toml.
#[derive(Debug, Serialize, Deserialize)]
pub struct HttpSinkConfig {
    /// Target URL for HTTP requests (required).
    pub url: String,
    /// HTTP method (default: POST).
    pub method: Option<HttpMethod>,
    /// Request timeout as a human-readable duration string, e.g. "30s" (default: 30s).
    pub timeout: Option<String>,
    /// Maximum HTTP body size in bytes (default: 10MB). Set to 0 to disable.
    pub max_payload_size_bytes: Option<u64>,
    /// Custom HTTP headers.
    pub headers: Option<std::collections::HashMap<String, String>>,
    /// Payload formatting mode (default: individual).
    pub batch_mode: Option<BatchMode>,
    /// Include Iggy metadata envelope in payload (default: true).
    pub include_metadata: Option<bool>,
    /// Include message checksum in metadata (default: false).
    pub include_checksum: Option<bool>,
    /// Include origin timestamp in metadata (default: false).
    pub include_origin_timestamp: Option<bool>,
    /// Enable health check request in open() (default: false).
    pub health_check_enabled: Option<bool>,
    /// HTTP method for health check (default: HEAD).
    pub health_check_method: Option<HttpMethod>,
    /// Maximum number of retries for transient errors (default: 3).
    pub max_retries: Option<u32>,
    /// Retry delay as a human-readable duration string, e.g. "1s" (default: 1s).
    pub retry_delay: Option<String>,
    /// Backoff multiplier for exponential retry delay (default: 2.0).
    pub retry_backoff_multiplier: Option<f64>,
    /// Maximum retry delay cap as a human-readable duration string (default: 30s).
    pub max_retry_delay: Option<String>,
    /// HTTP status codes considered successful (default: [200, 201, 202, 204]).
    pub success_status_codes: Option<Vec<u16>>,
    /// Accept invalid TLS certificates (default: false). Named to signal danger.
    pub tls_danger_accept_invalid_certs: Option<bool>,
    /// Maximum idle connections per host (default: 10).
    pub max_connections: Option<usize>,
    /// Enable verbose request/response logging (default: false).
    pub verbose_logging: Option<bool>,
}

/// HTTP sink connector that delivers consumed messages to any HTTP endpoint.
///
/// Lifecycle: `new()` → `open()` → `consume()` (repeated) → `close()`.
/// The `reqwest::Client` is built in `open()` (not `new()`) so that config-derived
/// settings (timeout, TLS, connection pool) are applied. This matches the
/// MongoDB/Elasticsearch/PostgreSQL sink initialization pattern.
#[derive(Debug)]
#[allow(dead_code)] // Fields used incrementally as consume()/close() are implemented.
pub struct HttpSink {
    id: u32,
    config: HttpSinkConfig,
    /// Initialized in `open()` with config-derived settings. `None` before `open()` is called.
    client: Option<reqwest::Client>,
    requests_sent: AtomicU64,
    messages_delivered: AtomicU64,
    errors_count: AtomicU64,
    retries_count: AtomicU64,
    last_success_timestamp: AtomicU64,
}

impl HttpSink {
    pub fn new(id: u32, config: HttpSinkConfig) -> Self {
        HttpSink {
            id,
            config,
            client: None,
            requests_sent: AtomicU64::new(0),
            messages_delivered: AtomicU64::new(0),
            errors_count: AtomicU64::new(0),
            retries_count: AtomicU64::new(0),
            last_success_timestamp: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl Sink for HttpSink {
    async fn open(&mut self) -> Result<(), Error> {
        // TODO(Commit 2): Build reqwest::Client here with config-derived settings:
        //   - timeout from self.config.timeout (humantime parse)
        //   - tls_danger_accept_invalid_certs
        //   - max_connections (pool_max_idle_per_host)
        //   - optional health check request
        self.client = Some(reqwest::Client::new());
        info!(
            "Opened HTTP sink connector with ID: {} for URL: {}",
            self.id, self.config.url
        );
        Ok(())
    }

    /// Deliver messages to the configured HTTP endpoint.
    ///
    /// **Runtime note**: The connector runtime (`sink.rs:585`) currently discards the `Result`
    /// returned by `consume()`. All retry logic must live inside this method — returning `Err`
    /// does not trigger a runtime-level retry. This is a known upstream issue.
    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        info!(
            "HTTP sink with ID: {} received: {} messages, schema: {}, stream: {}, topic: {}",
            self.id,
            messages.len(),
            messages_metadata.schema,
            topic_metadata.stream,
            topic_metadata.topic,
        );
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        info!("HTTP sink connector with ID: {} is closed.", self.id);
        Ok(())
    }
}
