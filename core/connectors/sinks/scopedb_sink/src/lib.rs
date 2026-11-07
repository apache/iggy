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

//! Appends Iggy messages to an existing ScopeDB table using the Rust SDK.

use std::fmt;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use humantime::Duration as HumanDuration;
use iggy_connector_sdk::{Error, sink_connector};
use scopedb_client::{AppendWriter, reqwest::Url};
use secrecy::SecretString;
use serde::Deserialize;
use tracing::warn;

mod encode;
mod schema;
mod sink;

sink_connector!(ScopeDBSink);

const MAX_REQUEST_BYTES: usize = 8 * 1024 * 1024;
const MAX_BATCH_ROWS: usize = 200_000;

/// Deserialized once from the connector's plugin configuration.
#[derive(Clone, Deserialize)]
pub struct ScopeDBSinkConfig {
    pub endpoint: String,
    pub table: String,
    pub api_key: Option<SecretString>,
    pub database: Option<String>,
    pub schema: Option<String>,
    pub mode: Option<WriteMode>,
    pub payload_column: Option<String>,
    pub include_metadata: Option<bool>,
    pub include_headers: Option<bool>,
    pub batch_size: Option<usize>,
    pub max_request_bytes: Option<usize>,
    pub max_concurrent_requests: Option<usize>,
    /// Total attempts, including the first, capped at three.
    pub max_retries: Option<u32>,
    pub retry_delay: Option<String>,
    pub max_retry_delay: Option<String>,
    pub retry_ambiguous: Option<bool>,
    pub timeout: Option<String>,
    pub verbose_logging: Option<bool>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WriteMode {
    #[default]
    Mapped,
    Raw,
}

#[derive(Debug)]
struct Settings {
    mode: WriteMode,
    payload_column: String,
    include_metadata: bool,
    include_headers: bool,
    batch_size: usize,
    max_request_bytes: usize,
    max_concurrent_requests: usize,
    max_attempts: u32,
    retry_delay: Duration,
    max_retry_delay: Duration,
    retry_ambiguous: bool,
    timeout: Duration,
    verbose: bool,
}

#[derive(Debug)]
struct OpenState {
    writer: AppendWriter,
    layout: schema::Layout,
}

#[derive(Debug, Default)]
struct Counters {
    written: AtomicU64,
    rejected: AtomicU64,
    failed: AtomicU64,
    unconfirmed: AtomicU64,
}

/// Logical row counts, including partial writes from failed consume calls.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ScopeDBSinkStats {
    pub written: u64,
    pub rejected: u64,
    pub failed: u64,
    pub unconfirmed: u64,
}

#[derive(Debug)]
pub struct ScopeDBSink {
    id: u32,
    config: ScopeDBSinkConfig,
    settings: Settings,
    state: Option<OpenState>,
    counters: Counters,
}

impl ScopeDBSink {
    pub fn new(id: u32, config: ScopeDBSinkConfig) -> Self {
        let settings = Settings::from_config(id, &config);
        Self {
            id,
            config,
            settings,
            state: None,
            counters: Counters::default(),
        }
    }

    pub fn stats(&self) -> ScopeDBSinkStats {
        ScopeDBSinkStats {
            written: self.counters.written.load(Ordering::Relaxed),
            rejected: self.counters.rejected.load(Ordering::Relaxed),
            failed: self.counters.failed.load(Ordering::Relaxed),
            unconfirmed: self.counters.unconfirmed.load(Ordering::Relaxed),
        }
    }
}

impl Settings {
    fn from_config(id: u32, config: &ScopeDBSinkConfig) -> Self {
        let mut retry_delay = duration(id, config.retry_delay.as_deref(), "1s");
        let mut max_retry_delay = duration(id, config.max_retry_delay.as_deref(), "30s");
        if retry_delay > max_retry_delay {
            warn!("ScopeDB sink connector ID: {id}: swapping reversed retry delays");
            std::mem::swap(&mut retry_delay, &mut max_retry_delay);
        }
        let requested_attempts = config.max_retries.unwrap_or(3);
        let max_attempts = requested_attempts.clamp(1, 3);
        if requested_attempts != max_attempts {
            warn!(
                "ScopeDB sink connector ID: {id}: max_retries must be in 1..=3, using {max_attempts}"
            );
        }
        Self {
            mode: config.mode.unwrap_or_default(),
            payload_column: config
                .payload_column
                .clone()
                .unwrap_or_else(|| "payload".into()),
            include_metadata: config.include_metadata.unwrap_or(true),
            include_headers: config.include_headers.unwrap_or(false),
            batch_size: config.batch_size.unwrap_or(100),
            max_request_bytes: config.max_request_bytes.unwrap_or(MAX_REQUEST_BYTES),
            max_concurrent_requests: config.max_concurrent_requests.unwrap_or(1),
            max_attempts,
            retry_delay,
            max_retry_delay,
            retry_ambiguous: config.retry_ambiguous.unwrap_or(false),
            timeout: duration(id, config.timeout.as_deref(), "30s"),
            verbose: config.verbose_logging.unwrap_or(false),
        }
    }

    fn validate(&self) -> Result<(), Error> {
        if self.payload_column.trim().is_empty() {
            return Err(Error::InvalidConfigValue(
                "payload_column must not be empty".into(),
            ));
        }
        if self.batch_size == 0 || self.batch_size > MAX_BATCH_ROWS {
            return Err(Error::InvalidConfigValue(
                "batch_size must be in 1..=200000".into(),
            ));
        }
        if self.max_request_bytes == 0 || self.max_request_bytes > MAX_REQUEST_BYTES {
            return Err(Error::InvalidConfigValue(
                "max_request_bytes must be in 1..=8388608".into(),
            ));
        }
        if self.max_concurrent_requests == 0 {
            return Err(Error::InvalidConfigValue(
                "max_concurrent_requests must be greater than zero".into(),
            ));
        }
        if self.timeout.is_zero() {
            return Err(Error::InvalidConfigValue(
                "timeout must be greater than zero".into(),
            ));
        }
        Ok(())
    }
}

impl ScopeDBSinkConfig {
    fn validate(&self) -> Result<(), Error> {
        for (field, value) in [
            ("table", self.table.as_str()),
            ("database", self.database.as_deref().unwrap_or("scopedb")),
            ("schema", self.schema.as_deref().unwrap_or("public")),
        ] {
            if value.trim().is_empty() {
                return Err(Error::InvalidConfigValue(format!(
                    "{field} must not be empty"
                )));
            }
        }
        let endpoint = Url::parse(&self.endpoint)
            .map_err(|_| Error::InvalidConfigValue("endpoint must be an HTTP URL".into()))?;
        if !matches!(endpoint.scheme(), "http" | "https")
            || endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(Error::InvalidConfigValue(
                "endpoint must be HTTP(S) without credentials, a query or a fragment".into(),
            ));
        }
        Ok(())
    }
}

impl fmt::Debug for ScopeDBSinkConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScopeDBSinkConfig")
            .field("endpoint", &"[REDACTED]")
            .field("api_key", &self.api_key)
            .field("table", &self.table)
            .field("database", &self.database)
            .field("schema", &self.schema)
            .field("mode", &self.mode)
            .finish_non_exhaustive()
    }
}

fn duration(id: u32, value: Option<&str>, default: &str) -> Duration {
    HumanDuration::from_str(value.unwrap_or(default))
        .map(|value| *value)
        .unwrap_or_else(|_| {
            warn!("ScopeDB sink connector ID: {id}: invalid duration, using {default}");
            HumanDuration::from_str(default)
                .map(|value| *value)
                .unwrap_or(Duration::from_secs(1))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> ScopeDBSinkConfig {
        toml::from_str("endpoint = 'http://localhost:6543'\ntable = 'events'")
            .expect("valid config")
    }

    #[test]
    fn given_credentials_when_formatted_should_redact_config() {
        let mut config = test_config();
        config.api_key = Some(SecretString::from("secret-api-key"));
        config.endpoint = "https://user:password@example.com/?token=private".into();
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("secret-api-key"));
        assert!(!rendered.contains("password"));
        assert!(!rendered.contains("private"));
        assert!(matches!(
            config.validate(),
            Err(Error::InvalidConfigValue(_))
        ));
    }

    #[test]
    fn given_reversed_delays_when_resolved_should_swap_and_cap_attempts() {
        let mut config = test_config();
        config.retry_delay = Some("10s".into());
        config.max_retry_delay = Some("1s".into());
        config.max_retries = Some(99);
        let settings = Settings::from_config(1, &config);
        assert_eq!(settings.retry_delay, Duration::from_secs(1));
        assert_eq!(settings.max_retry_delay, Duration::from_secs(10));
        assert_eq!(settings.max_attempts, 3);
    }

    #[test]
    fn given_invalid_duration_when_resolved_should_use_default() {
        let mut config = test_config();
        config.timeout = Some("later".into());
        assert_eq!(
            Settings::from_config(1, &config).timeout,
            Duration::from_secs(30)
        );
    }

    #[test]
    fn given_invalid_limits_when_validated_should_reject_config() {
        let mut config = test_config();
        config.batch_size = Some(0);
        assert!(matches!(
            Settings::from_config(1, &config).validate(),
            Err(Error::InvalidConfigValue(_))
        ));

        let mut config = test_config();
        config.max_concurrent_requests = Some(0);
        assert!(matches!(
            Settings::from_config(1, &config).validate(),
            Err(Error::InvalidConfigValue(_))
        ));

        let mut config = test_config();
        config.timeout = Some("0s".into());
        assert!(matches!(
            Settings::from_config(1, &config).validate(),
            Err(Error::InvalidConfigValue(_))
        ));
    }
}
