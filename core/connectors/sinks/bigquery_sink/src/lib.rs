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

//! Google BigQuery sink connector.
//!
//! Writes Iggy messages into a BigQuery table through the Storage Write API
//! `_default` stream, with rows encoded as Arrow record batches.
//!
//! Module map:
//! - `schema`: BigQuery table schema (from `tables.get`) to Arrow schema.
//! - `encode`: `ConsumedMessage`s to Arrow record batches, split by size.
//! - `client`: credentials, `tables.get`, and `AppendRows` calls.
//! - `error`: gRPC status classification into retryable and permanent.
//! - `sink`: the `Sink` trait implementation.

use humantime::Duration as HumanDuration;
use iggy_connector_sdk::{Error, sink_connector};
use secrecy::SecretString;
use serde::Deserialize;
use std::str::FromStr;
use std::sync::atomic::AtomicU64;
use std::time::Duration;
use tracing::warn;

mod client;
mod encode;
mod error;
mod schema;
mod sink;

sink_connector!(BigQuerySink);

const DEFAULT_PAYLOAD_COLUMN: &str = "payload";
const DEFAULT_MAX_REQUEST_BYTES: usize = 8 * 1024 * 1024;
/// Hard ceiling below the 10 MB `AppendRows` limit, leaving room for the
/// request envelope (stream name, writer schema, protobuf framing).
const MAX_REQUEST_BYTES_CEILING: usize = 9 * 1024 * 1024;
/// Smallest accepted request budget. Anything lower makes most real rows
/// unwritable.
const MIN_REQUEST_BYTES: usize = 64 * 1024;
const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_RETRY_DELAY: &str = "1s";
const DEFAULT_MAX_RETRY_DELAY: &str = "30s";
const DEFAULT_TIMEOUT: &str = "30s";

/// Plugin configuration, deserialized from `[plugin_config]`.
///
/// `Deserialize` only. Nothing re-serializes a plugin config, and leaving
/// `Serialize` off is what keeps `credentials_json` unserializable.
#[derive(Debug, Clone, Deserialize)]
pub struct BigQuerySinkConfig {
    pub project_id: String,
    pub dataset: String,
    pub table: String,
    /// `mapped` (default) or `raw`.
    pub mode: Option<WriteMode>,
    /// Path to a service account key file. Mutually exclusive with
    /// `credentials_json`. When neither is set, Application Default
    /// Credentials are used.
    pub credentials_path: Option<String>,
    /// Inline service account key JSON.
    pub credentials_json: Option<SecretString>,
    /// Column holding the payload in `raw` mode (default `payload`).
    pub payload_column: Option<String>,
    /// Write `iggy_stream`, `iggy_topic`, `iggy_partition_id`, `iggy_offset`,
    /// `iggy_timestamp` and `iggy_id` (default `true`).
    pub include_metadata: Option<bool>,
    /// Write message headers into an `iggy_headers` column (default `false`).
    pub include_headers: Option<bool>,
    /// How BigQuery fills columns absent from a request: `default` (column
    /// default value, else NULL) or `null`.
    pub missing_value: Option<MissingValue>,
    /// Upper bound for one `AppendRows` request (default 8 MiB).
    pub max_request_bytes: Option<usize>,
    /// Total attempts per append, including the first (default 3).
    pub max_retries: Option<u32>,
    /// Base retry delay, e.g. `"1s"` (default `1s`).
    pub retry_delay: Option<String>,
    /// Upper bound for a single retry delay (default `30s`).
    pub max_retry_delay: Option<String>,
    /// Per-request timeout for `tables.get` and `AppendRows` (default `30s`).
    pub timeout: Option<String>,
    /// Log every batch at info level instead of debug (default `false`).
    pub verbose_logging: Option<bool>,
    /// REST endpoint override, e.g. `http://127.0.0.1:9050`. Must be set
    /// together with `grpc_endpoint`; when both are set no credentials are
    /// used. Intended for local fakes and emulators.
    pub endpoint: Option<String>,
    /// gRPC `host:port` override for the Storage Write API.
    pub grpc_endpoint: Option<String>,
}

/// How message payloads map onto table columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteMode {
    /// Top-level JSON fields map to table columns by name.
    #[default]
    Mapped,
    /// The whole payload goes into one column.
    Raw,
}

/// Maps to `AppendRowsRequest.default_missing_value_interpretation`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissingValue {
    #[default]
    Default,
    Null,
}

impl MissingValue {
    /// Wire value of `AppendRowsRequest.MissingValueInterpretation`.
    pub(crate) fn as_proto(self) -> i32 {
        match self {
            MissingValue::Null => 1,
            MissingValue::Default => 2,
        }
    }
}

/// Resolved, validated runtime settings. Built once in `new()`.
#[derive(Debug, Clone)]
pub(crate) struct Settings {
    pub mode: WriteMode,
    pub payload_column: String,
    pub include_metadata: bool,
    pub include_headers: bool,
    pub missing_value: MissingValue,
    pub max_request_bytes: usize,
    pub max_retries: u32,
    pub retry_delay: Duration,
    pub max_retry_delay: Duration,
    pub timeout: Duration,
    pub verbose: bool,
}

/// Counters updated on the hot path without taking a lock.
#[derive(Debug, Default)]
pub(crate) struct Counters {
    pub rows_written: AtomicU64,
    pub rows_rejected: AtomicU64,
    pub rows_failed: AtomicU64,
}

#[derive(Debug)]
struct OpenState {
    client: client::BigQueryClient,
    layout: schema::TableLayout,
}

#[derive(Debug)]
pub struct BigQuerySink {
    id: u32,
    config: BigQuerySinkConfig,
    settings: Settings,
    /// `project.dataset.table`, safe to log.
    target: String,
    state: Option<OpenState>,
    counters: Counters,
}

impl BigQuerySink {
    pub fn new(id: u32, config: BigQuerySinkConfig) -> Self {
        let settings = Settings::from_config(&config);
        let target = format!("{}.{}.{}", config.project_id, config.dataset, config.table);
        BigQuerySink {
            id,
            config,
            settings,
            target,
            state: None,
            counters: Counters::default(),
        }
    }
}

impl Settings {
    fn from_config(config: &BigQuerySinkConfig) -> Self {
        let mut retry_delay = parse_duration(config.retry_delay.as_deref(), DEFAULT_RETRY_DELAY);
        let mut max_retry_delay =
            parse_duration(config.max_retry_delay.as_deref(), DEFAULT_MAX_RETRY_DELAY);
        if retry_delay > max_retry_delay {
            warn!(
                "BigQuery sink: retry_delay ({retry_delay:?}) is greater than max_retry_delay ({max_retry_delay:?}), swapping them"
            );
            std::mem::swap(&mut retry_delay, &mut max_retry_delay);
        }

        let requested = config
            .max_request_bytes
            .unwrap_or(DEFAULT_MAX_REQUEST_BYTES);
        let max_request_bytes = requested.clamp(MIN_REQUEST_BYTES, MAX_REQUEST_BYTES_CEILING);
        if max_request_bytes != requested {
            warn!(
                "BigQuery sink: max_request_bytes {requested} is outside [{MIN_REQUEST_BYTES}, {MAX_REQUEST_BYTES_CEILING}], using {max_request_bytes}"
            );
        }

        Settings {
            mode: config.mode.unwrap_or_default(),
            payload_column: config
                .payload_column
                .clone()
                .unwrap_or_else(|| DEFAULT_PAYLOAD_COLUMN.to_owned()),
            include_metadata: config.include_metadata.unwrap_or(true),
            include_headers: config.include_headers.unwrap_or(false),
            missing_value: config.missing_value.unwrap_or_default(),
            max_request_bytes,
            max_retries: config.max_retries.unwrap_or(DEFAULT_MAX_RETRIES),
            retry_delay,
            max_retry_delay,
            timeout: parse_duration(config.timeout.as_deref(), DEFAULT_TIMEOUT),
            verbose: config.verbose_logging.unwrap_or(false),
        }
    }
}

impl BigQuerySinkConfig {
    /// Structural checks that need no network. Called from `open()` because
    /// `new()` cannot fail.
    pub(crate) fn validate(&self) -> Result<(), Error> {
        for (name, value) in [
            ("project_id", &self.project_id),
            ("dataset", &self.dataset),
            ("table", &self.table),
        ] {
            if value.trim().is_empty() {
                return Err(Error::InvalidConfigValue(format!(
                    "{name} must not be empty"
                )));
            }
        }
        if self.credentials_path.is_some() && self.credentials_json.is_some() {
            return Err(Error::InvalidConfigValue(
                "set either credentials_path or credentials_json, not both".into(),
            ));
        }
        if self.endpoint.is_some() != self.grpc_endpoint.is_some() {
            return Err(Error::InvalidConfigValue(
                "endpoint and grpc_endpoint must be set together".into(),
            ));
        }
        if let Some(column) = &self.payload_column
            && column.trim().is_empty()
        {
            return Err(Error::InvalidConfigValue(
                "payload_column must not be empty".into(),
            ));
        }
        Ok(())
    }
}

fn parse_duration(input: Option<&str>, default: &str) -> Duration {
    let raw = input.unwrap_or(default);
    HumanDuration::from_str(raw)
        .map(|d| *d)
        .unwrap_or_else(|e| {
            warn!("BigQuery sink: invalid duration '{raw}': {e}, using default '{default}'");
            HumanDuration::from_str(default)
                .map(|d| *d)
                .unwrap_or(Duration::from_secs(1))
        })
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;

    pub(crate) fn config() -> BigQuerySinkConfig {
        BigQuerySinkConfig {
            project_id: "proj".into(),
            dataset: "ds".into(),
            table: "events".into(),
            mode: None,
            credentials_path: None,
            credentials_json: None,
            payload_column: None,
            include_metadata: None,
            include_headers: None,
            missing_value: None,
            max_request_bytes: None,
            max_retries: None,
            retry_delay: None,
            max_retry_delay: None,
            timeout: None,
            verbose_logging: None,
            endpoint: None,
            grpc_endpoint: None,
        }
    }

    pub(crate) fn settings(mode: WriteMode, include_metadata: bool) -> Settings {
        let mut config = config();
        config.mode = Some(mode);
        config.include_metadata = Some(include_metadata);
        Settings::from_config(&config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::config as test_config;

    #[test]
    fn given_minimal_config_should_apply_defaults() {
        let settings = Settings::from_config(&test_config());
        assert_eq!(settings.mode, WriteMode::Mapped);
        assert_eq!(settings.payload_column, "payload");
        assert!(settings.include_metadata);
        assert!(!settings.include_headers);
        assert_eq!(settings.missing_value, MissingValue::Default);
        assert_eq!(settings.max_request_bytes, DEFAULT_MAX_REQUEST_BYTES);
        assert_eq!(settings.max_retries, 3);
        assert_eq!(settings.retry_delay, Duration::from_secs(1));
        assert_eq!(settings.max_retry_delay, Duration::from_secs(30));
        assert_eq!(settings.timeout, Duration::from_secs(30));
        assert!(!settings.verbose);
    }

    #[test]
    fn given_minimal_toml_should_deserialize() {
        let config: BigQuerySinkConfig = toml::from_str(
            r#"
            project_id = "proj"
            dataset = "ds"
            table = "events"
            "#,
        )
        .expect("minimal config should parse");
        assert!(config.validate().is_ok());
    }

    #[test]
    fn given_raw_mode_and_null_missing_value_should_deserialize() {
        let config: BigQuerySinkConfig = toml::from_str(
            r#"
            project_id = "proj"
            dataset = "ds"
            table = "events"
            mode = "raw"
            missing_value = "null"
            "#,
        )
        .expect("config should parse");
        assert_eq!(config.mode, Some(WriteMode::Raw));
        assert_eq!(config.missing_value, Some(MissingValue::Null));
    }

    #[test]
    fn given_unknown_mode_should_fail_to_deserialize() {
        let result = toml::from_str::<BigQuerySinkConfig>(
            r#"
            project_id = "proj"
            dataset = "ds"
            table = "events"
            mode = "upsert"
            "#,
        );
        assert!(result.is_err());
    }

    #[test]
    fn given_missing_table_should_fail_to_deserialize() {
        let result = toml::from_str::<BigQuerySinkConfig>(
            r#"
            project_id = "proj"
            dataset = "ds"
            "#,
        );
        assert!(result.is_err());
    }

    #[test]
    fn given_both_credential_sources_should_fail_validation() {
        let mut config = test_config();
        config.credentials_path = Some("/tmp/key.json".into());
        config.credentials_json = Some(SecretString::from("{}"));
        assert!(matches!(
            config.validate(),
            Err(Error::InvalidConfigValue(_))
        ));
    }

    #[test]
    fn given_only_rest_endpoint_should_fail_validation() {
        let mut config = test_config();
        config.endpoint = Some("http://127.0.0.1:9050".into());
        assert!(matches!(
            config.validate(),
            Err(Error::InvalidConfigValue(_))
        ));
    }

    #[test]
    fn given_empty_table_should_fail_validation() {
        let mut config = test_config();
        config.table = " ".into();
        assert!(matches!(
            config.validate(),
            Err(Error::InvalidConfigValue(_))
        ));
    }

    #[test]
    fn given_reversed_retry_delays_should_swap_them() {
        let mut config = test_config();
        config.retry_delay = Some("10s".into());
        config.max_retry_delay = Some("2s".into());
        let settings = Settings::from_config(&config);
        assert_eq!(settings.retry_delay, Duration::from_secs(2));
        assert_eq!(settings.max_retry_delay, Duration::from_secs(10));
    }

    #[test]
    fn given_oversized_request_budget_should_clamp_below_api_limit() {
        let mut config = test_config();
        config.max_request_bytes = Some(50 * 1024 * 1024);
        let settings = Settings::from_config(&config);
        assert_eq!(settings.max_request_bytes, MAX_REQUEST_BYTES_CEILING);
    }

    #[test]
    fn given_invalid_duration_should_fall_back_to_default() {
        let mut config = test_config();
        config.timeout = Some("soon".into());
        let settings = Settings::from_config(&config);
        assert_eq!(settings.timeout, Duration::from_secs(30));
    }

    #[test]
    fn given_debug_format_should_not_leak_inline_credentials() {
        let mut config = test_config();
        config.credentials_json = Some(SecretString::from("super-secret-key"));
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("super-secret-key"));
    }

    #[test]
    fn given_missing_value_should_map_to_proto_enum() {
        assert_eq!(MissingValue::Null.as_proto(), 1);
        assert_eq!(MissingValue::Default.as_proto(), 2);
    }
}
