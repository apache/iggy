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

use crate::router::Router;
use humantime::Duration as HumanDuration;
use iggy_connector_sdk::retry::RetryPolicy;
use iggy_connector_sdk::{Error, sink_connector};
use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::time::Duration;
use strum::Display as StrumDisplay;
use tracing::warn;

mod catalog;
mod props;
mod router;
mod sink;

const DEFAULT_MAX_RETRIES: u32 = 5;
const DEFAULT_RETRY_DELAY: &str = "1s";
const DEFAULT_MAX_RETRY_DELAY: &str = "30s";

/// What the sink does with a row that does not match the table schema.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, StrumDisplay)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum InvalidRows {
    /// Fail the whole batch.
    #[default]
    Fail,
    /// Write the rows that match, and log each skipped row with its offset.
    Skip,
}

#[derive(Debug, Serialize, Deserialize, StrumDisplay)]
#[serde(rename_all = "lowercase")]
pub enum IcebergSinkTypes {
    REST,
}

#[derive(Debug, Serialize, Deserialize, StrumDisplay)]
#[serde(rename_all = "lowercase")]
pub enum IcebergSinkStoreClass {
    S3,
    FS,
    GCS,
    AZDLS,
    OSS,
}

sink_connector!(IcebergSink);

#[derive(Debug)]
pub struct IcebergSink {
    id: u32,
    config: IcebergSinkConfig,
    commit_retry: RetryPolicy,
    router: Option<Box<dyn Router>>,
}

// `Deserialize` only: nothing re-serializes the config, and leaving `Serialize` off keeps the
// store credentials unserializable.
#[derive(Debug, Deserialize)]
pub struct IcebergSinkConfig {
    #[serde(default)]
    pub tables: Vec<String>,
    pub catalog_type: IcebergSinkTypes,
    pub warehouse: String,
    pub uri: String,
    pub dynamic_routing: bool,
    #[serde(default)]
    pub dynamic_route_field: String,
    pub store_url: String,
    pub store_access_key_id: Option<SecretString>,
    pub store_secret_access_key: Option<SecretString>,
    pub store_region: String,
    pub store_class: IcebergSinkStoreClass,
    /// Path-style S3 addressing (`http://host/bucket/key`). MinIO-style endpoints
    /// need it; iceberg 0.10 defaults to the virtual-host style otherwise.
    #[serde(default = "default_true")]
    pub store_path_style_access: bool,
    #[serde(default)]
    pub invalid_rows: Option<InvalidRows>,
    /// Total attempts per catalog commit, including the first (default 5).
    pub max_retries: Option<u32>,
    /// Base delay before a commit retry, e.g. `"1s"` (default `1s`).
    pub retry_delay: Option<String>,
    /// Upper bound for one delay before a commit retry (default `30s`).
    pub max_retry_delay: Option<String>,
}

fn default_true() -> bool {
    true
}

fn slice_user_table(table: &str) -> Vec<String> {
    table.split('.').map(|s| s.to_string()).collect()
}

impl IcebergSink {
    pub fn new(id: u32, config: IcebergSinkConfig) -> Self {
        let commit_retry = commit_retry_policy(id, &config);
        IcebergSink {
            id,
            config,
            commit_retry,
            router: None,
        }
    }
}

fn commit_retry_policy(id: u32, config: &IcebergSinkConfig) -> RetryPolicy {
    let mut base_delay = parse_duration(id, config.retry_delay.as_deref(), DEFAULT_RETRY_DELAY);
    let mut max_delay = parse_duration(
        id,
        config.max_retry_delay.as_deref(),
        DEFAULT_MAX_RETRY_DELAY,
    );
    if base_delay > max_delay {
        warn!(
            "Iceberg sink ID: {id}: retry_delay ({base_delay:?}) is greater than max_retry_delay ({max_delay:?}), swapping them"
        );
        std::mem::swap(&mut base_delay, &mut max_delay);
    }
    RetryPolicy {
        max_attempts: config.max_retries.unwrap_or(DEFAULT_MAX_RETRIES),
        base_delay,
        max_delay,
    }
}

fn parse_duration(id: u32, input: Option<&str>, default: &str) -> Duration {
    let raw = input.unwrap_or(default);
    HumanDuration::from_str(raw)
        .map(|duration| *duration)
        .unwrap_or_else(|error| {
            warn!(
                "Iceberg sink ID: {id}: invalid duration '{raw}': {error}, using default '{default}'"
            );
            HumanDuration::from_str(default)
                .map(|duration| *duration)
                .unwrap_or(Duration::from_secs(1))
        })
}

#[cfg(test)]
mod tests {
    use iggy_connector_sdk::Sink;

    use super::*;

    #[tokio::test]
    async fn omitted_routing_fields_deserialize_but_the_active_mode_requires_configuration() {
        for dynamic_routing in [false, true] {
            let mut json = simd_json::to_vec(&simd_json::json!({
                "catalog_type": "rest",
                "warehouse": "warehouse",
                "uri": "http://localhost:1",
                "dynamic_routing": dynamic_routing,
                "store_url": "http://localhost:1",
                "store_region": "us-east-1",
                "store_class": "s3"
            }))
            .unwrap();
            let config: IcebergSinkConfig =
                simd_json::from_slice(&mut json).expect("Unused routing fields must be optional");
            assert!(config.tables.is_empty());
            assert!(config.dynamic_route_field.is_empty());
            let mut sink = IcebergSink::new(1, config);
            let error = sink
                .open()
                .await
                .expect_err("The active routing mode needs a destination");
            assert!(matches!(error, Error::InvalidConfigValue(_)), "{error}");
            let field = if dynamic_routing {
                "dynamic_route_field"
            } else {
                "tables"
            };
            assert!(error.to_string().contains(field), "{error}");
        }
    }

    fn retry_config(
        max_retries: Option<u32>,
        retry_delay: Option<&str>,
        max_retry_delay: Option<&str>,
    ) -> IcebergSinkConfig {
        let mut json = simd_json::to_vec(&simd_json::json!({
            "catalog_type": "rest",
            "warehouse": "warehouse",
            "uri": "http://localhost:1",
            "dynamic_routing": false,
            "store_url": "http://localhost:1",
            "store_region": "us-east-1",
            "store_class": "s3",
            "max_retries": max_retries,
            "retry_delay": retry_delay,
            "max_retry_delay": max_retry_delay
        }))
        .unwrap();
        simd_json::from_slice(&mut json).expect("Failed to parse config")
    }

    #[test]
    fn given_no_retry_settings_should_retry_commits_with_the_defaults() {
        let policy = commit_retry_policy(1, &retry_config(None, None, None));

        assert_eq!(policy.max_attempts, DEFAULT_MAX_RETRIES);
        assert_eq!(policy.base_delay, Duration::from_secs(1));
        assert_eq!(policy.max_delay, Duration::from_secs(30));
    }

    #[test]
    fn given_retry_settings_should_retry_commits_with_them() {
        let policy = commit_retry_policy(1, &retry_config(Some(10), Some("200ms"), Some("2s")));

        assert_eq!(policy.max_attempts, 10);
        assert_eq!(policy.base_delay, Duration::from_millis(200));
        assert_eq!(policy.max_delay, Duration::from_secs(2));
    }

    #[test]
    fn given_reversed_retry_delays_should_swap_them() {
        let policy = commit_retry_policy(1, &retry_config(None, Some("10s"), Some("2s")));

        assert_eq!(policy.base_delay, Duration::from_secs(2));
        assert_eq!(policy.max_delay, Duration::from_secs(10));
    }
}
