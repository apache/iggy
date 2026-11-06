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

pub mod http_provider;
pub(crate) mod local_provider;

use crate::configs::connectors::http_provider::HttpConnectorsConfigProvider;
use crate::configs::connectors::local_provider::LocalConnectorsConfigProvider;
use crate::configs::runtime::ConnectorsConfig as RuntimeConnectorsConfig;
use crate::error::RuntimeError;
use async_trait::async_trait;
use configs_derive::ConfigEnv;
use iggy_common::{DateTime, Utc};
use iggy_connector_sdk::Schema;
use iggy_connector_sdk::transforms::TransformType;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Formatter;
use std::num::NonZeroU32;
use std::ops::Deref;
use std::path::PathBuf;
use std::str::FromStr;
use strum::Display;

#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize, Display,
)]
#[serde(rename_all = "lowercase")]
pub enum ConfigFormat {
    #[strum(to_string = "json")]
    Json,
    #[strum(to_string = "yaml")]
    Yaml,
    #[default]
    #[strum(to_string = "toml")]
    Toml,
    #[strum(to_string = "text")]
    Text,
}

/// What a sink promises about each message it reads.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Display)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryGuarantee {
    /// The offset is stored when a message is read, so a failed write loses the message.
    #[default]
    #[strum(to_string = "at_most_once")]
    AtMostOnce,
    /// The offset is stored once the plugin proved the message delivered, so a message can be
    /// delivered again but is not lost while Iggy retains it.
    #[strum(to_string = "at_least_once")]
    AtLeastOnce,
}

/// What an at-least-once sink does with a message that the runtime cannot decode, transform or
/// serialize.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Display)]
#[serde(rename_all = "snake_case")]
pub enum InvalidMessages {
    /// Stop the delivery at the message, so that it is not lost.
    #[default]
    #[strum(to_string = "stop")]
    Stop,
    /// Skip the message, counting an error and logging its offset.
    #[strum(to_string = "skip")]
    Skip,
}

/// The `[delivery]` table of a sink. Absent, the sink keeps the at-most-once behaviour.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ConfigEnv)]
#[serde(default)]
pub struct SinkDeliveryConfig {
    #[config_env(leaf)]
    pub guarantee: DeliveryGuarantee,
    /// Attempts of a batch that fails transiently before the delivery stops. Unset retries until
    /// the destination recovers.
    #[config_env(leaf)]
    pub retry_max_attempts: Option<NonZeroU32>,
    pub retry_initial_backoff: String,
    pub retry_max_backoff: String,
    /// The longest that accepted messages wait for a flush. Unset flushes after every batch.
    pub flush_interval: Option<String>,
    /// The most messages that a partition holds accepted but unflushed. Reaching it flushes at
    /// once, before `flush_interval` runs out.
    #[config_env(leaf)]
    pub flush_max_messages: NonZeroU32,
    #[config_env(leaf)]
    pub invalid_messages: InvalidMessages,
}

impl Default for SinkDeliveryConfig {
    fn default() -> Self {
        Self {
            guarantee: DeliveryGuarantee::default(),
            retry_max_attempts: None,
            retry_initial_backoff: "1s".to_owned(),
            retry_max_backoff: "30s".to_owned(),
            flush_interval: None,
            flush_max_messages: DEFAULT_FLUSH_MAX_MESSAGES,
            invalid_messages: InvalidMessages::default(),
        }
    }
}

const DEFAULT_FLUSH_MAX_MESSAGES: NonZeroU32 = NonZeroU32::new(10_000).unwrap();

/// A connector key becomes part of a filename under the local provider's
/// `config_dir` and of a URL under the HTTP provider, so it must stay a single
/// path component. Requiring a leading letter or digit is what rules out `.`,
/// `..` and hidden-file names outright, instead of relying on the `sink_` /
/// `source_` filename prefix to neutralize them.
#[derive(Debug)]
pub struct ConnectorKey(String);

impl ConnectorKey {
    /// Leaves room for the `source_` prefix, the version suffix and the
    /// `.toml` extension inside a 255-byte filename limit.
    pub const MAX_LENGTH: usize = 128;

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn is_valid(key: &str) -> bool {
        key.len() <= Self::MAX_LENGTH
            && key.as_bytes().split_first().is_some_and(|(first, rest)| {
                first.is_ascii_alphanumeric()
                    && rest.iter().all(|byte| {
                        byte.is_ascii_alphanumeric() || matches!(*byte, b'-' | b'_' | b'.')
                    })
            })
    }
}

impl TryFrom<String> for ConnectorKey {
    type Error = RuntimeError;

    fn try_from(key: String) -> Result<Self, Self::Error> {
        if Self::is_valid(&key) {
            Ok(Self(key))
        } else {
            Err(RuntimeError::InvalidConnectorKey(key))
        }
    }
}

impl FromStr for ConnectorKey {
    type Err = RuntimeError;

    fn from_str(key: &str) -> Result<Self, Self::Err> {
        Self::try_from(key.to_owned())
    }
}

impl Deref for ConnectorKey {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<ConnectorKey> for String {
    fn from(key: ConnectorKey) -> Self {
        key.0
    }
}

impl std::fmt::Display for ConnectorKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ConnectorConfig {
    Sink(SinkConfig),
    Source(SourceConfig),
}

impl Default for ConnectorConfig {
    fn default() -> Self {
        Self::Sink(SinkConfig::default())
    }
}

impl ConnectorConfig {
    fn version(&self) -> u64 {
        match self {
            ConnectorConfig::Sink(config) => config.version,
            ConnectorConfig::Source(config) => config.version,
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct CreateSinkConfig {
    pub enabled: bool,
    pub name: String,
    pub path: String,
    pub transforms: Option<TransformsConfig>,
    pub streams: Vec<StreamConsumerConfig>,
    pub plugin_config_format: Option<ConfigFormat>,
    pub plugin_config: Option<serde_json::Value>,
    #[serde(default)]
    pub verbose: bool,
    #[serde(default)]
    pub benchmark: bool,
    #[serde(default)]
    pub delivery: SinkDeliveryConfig,
}

impl CreateSinkConfig {
    fn into_sink_config(self, key: &ConnectorKey, version: u64) -> SinkConfig {
        SinkConfig {
            key: key.to_string(),
            enabled: self.enabled,
            version,
            name: self.name,
            path: self.path,
            transforms: self.transforms,
            streams: self.streams,
            plugin_config_format: self.plugin_config_format,
            plugin_config: self.plugin_config,
            verbose: self.verbose,
            benchmark: self.benchmark,
            delivery: self.delivery,
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, ConfigEnv)]
pub struct SinkConfig {
    // Identity comes from the file: an env KEY would rename the sink and move its consumer group.
    #[config_env(skip)]
    pub key: String,
    pub enabled: bool,
    #[config_env(skip)]
    pub version: u64,
    pub name: String,
    pub path: String,
    #[config_env(skip)]
    pub transforms: Option<TransformsConfig>,
    pub streams: Vec<StreamConsumerConfig>,
    #[config_env(leaf)]
    pub plugin_config_format: Option<ConfigFormat>,
    #[config_env(skip)]
    pub plugin_config: Option<serde_json::Value>,
    #[serde(default)]
    pub verbose: bool,
    #[serde(default)]
    pub benchmark: bool,
    #[serde(default)]
    pub delivery: SinkDeliveryConfig,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct CreateSourceConfig {
    pub enabled: bool,
    pub name: String,
    pub path: String,
    pub transforms: Option<TransformsConfig>,
    pub streams: Vec<StreamProducerConfig>,
    pub plugin_config_format: Option<ConfigFormat>,
    pub plugin_config: Option<serde_json::Value>,
    #[serde(default)]
    pub verbose: bool,
    #[serde(default)]
    pub benchmark: bool,
    #[serde(default)]
    pub exactly_once: bool,
}

impl CreateSourceConfig {
    fn into_source_config(self, key: &ConnectorKey, version: u64) -> SourceConfig {
        SourceConfig {
            key: key.to_string(),
            enabled: self.enabled,
            version,
            name: self.name,
            path: self.path,
            transforms: self.transforms,
            streams: self.streams,
            plugin_config_format: self.plugin_config_format,
            plugin_config: self.plugin_config,
            verbose: self.verbose,
            benchmark: self.benchmark,
            exactly_once: self.exactly_once,
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, ConfigEnv)]
pub struct SourceConfig {
    // Identity comes from the file: an env KEY would orphan saved state.
    #[config_env(skip)]
    pub key: String,
    pub enabled: bool,
    #[config_env(skip)]
    pub version: u64,
    pub name: String,
    pub path: String,
    #[config_env(skip)]
    pub transforms: Option<TransformsConfig>,
    pub streams: Vec<StreamProducerConfig>,
    #[config_env(leaf)]
    pub plugin_config_format: Option<ConfigFormat>,
    #[config_env(skip)]
    pub plugin_config: Option<serde_json::Value>,
    #[serde(default)]
    pub verbose: bool,
    #[serde(default)]
    pub benchmark: bool,
    /// Each batch goes to one partition with the source's producer, and lands once, also after a
    /// crash of the runtime. Needs a plugin that produces the same batch again from its state.
    #[serde(default)]
    pub exactly_once: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TransformsConfig {
    #[serde(flatten)]
    pub transforms: HashMap<TransformType, serde_json::Value>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, ConfigEnv)]
pub struct StreamConsumerConfig {
    pub stream: String,
    pub topics: Vec<String>,
    #[config_env(leaf)]
    pub schema: Schema,
    pub avro_schema_json: Option<String>,
    #[config_env(leaf)]
    pub avro_schema_path: Option<PathBuf>,
    pub batch_length: Option<u32>,
    /// Replaced by `max_wait` and `min_count`. Kept so that a sink that still
    /// sets it fails to start: without the field, serde ignores the setting.
    pub poll_interval: Option<String>,
    pub max_wait: Option<String>,
    pub min_count: Option<u32>,
    pub consumer_group: Option<String>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, ConfigEnv)]
pub struct StreamProducerConfig {
    pub stream: String,
    pub topic: String,
    #[config_env(leaf)]
    pub schema: Schema,
    pub avro_schema_json: Option<String>,
    #[config_env(leaf)]
    pub avro_schema_path: Option<PathBuf>,
    pub batch_length: Option<u32>,
    pub linger_time: Option<String>,
    /// The partition every message goes to. An exactly-once source uses 0 when it is unset.
    pub partition_id: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorConfigVersionInfo {
    pub version: u64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ConnectorConfigVersions {
    pub sinks: HashMap<String, ConnectorConfigVersionInfo>,
    pub sources: HashMap<String, ConnectorConfigVersionInfo>,
}

/// Only the two `create_*` methods take a parsed key: they are where the local
/// provider turns the key into a filename, so the type carries the proof that
/// the API boundary already validated it. The other methods only compare keys.
#[async_trait]
pub trait ConnectorsConfigProvider: Send + Sync {
    async fn create_sink_config(
        &self,
        key: &ConnectorKey,
        config: CreateSinkConfig,
    ) -> Result<SinkConfig, RuntimeError>;
    async fn create_source_config(
        &self,
        key: &ConnectorKey,
        config: CreateSourceConfig,
    ) -> Result<SourceConfig, RuntimeError>;
    async fn get_active_configs(&self) -> Result<ConnectorsConfig, RuntimeError>;
    #[allow(dead_code)]
    async fn get_active_configs_versions(&self) -> Result<ConnectorConfigVersions, RuntimeError>;
    async fn set_active_sink_version(&self, key: &str, version: u64) -> Result<(), RuntimeError>;
    async fn set_active_source_version(&self, key: &str, version: u64) -> Result<(), RuntimeError>;
    async fn get_sink_configs(&self, key: &str) -> Result<Vec<SinkConfig>, RuntimeError>;
    async fn get_sink_config(
        &self,
        key: &str,
        version: Option<u64>,
    ) -> Result<Option<SinkConfig>, RuntimeError>;
    async fn get_source_configs(&self, key: &str) -> Result<Vec<SourceConfig>, RuntimeError>;
    async fn get_source_config(
        &self,
        key: &str,
        version: Option<u64>,
    ) -> Result<Option<SourceConfig>, RuntimeError>;
    async fn delete_sink_config(&self, key: &str, version: Option<u64>)
    -> Result<(), RuntimeError>;
    async fn delete_source_config(
        &self,
        key: &str,
        version: Option<u64>,
    ) -> Result<(), RuntimeError>;
}

pub async fn create_connectors_config_provider(
    config: &RuntimeConnectorsConfig,
) -> Result<Box<dyn ConnectorsConfigProvider>, RuntimeError> {
    match config {
        RuntimeConnectorsConfig::Local(config) => {
            let provider = LocalConnectorsConfigProvider::new(&config.config_dir);
            let provider = provider.init().await?;
            Ok(Box::new(provider))
        }
        RuntimeConnectorsConfig::Http(config) => {
            let provider = HttpConnectorsConfigProvider::new(
                &config.base_url,
                config.timeout.get_duration(),
                &config.request_headers,
                &config.url_templates,
                &config.response,
                &config.retry,
            )?;
            Ok(Box::new(provider))
        }
    }
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ConnectorsConfig {
    sinks: HashMap<String, SinkConfig>,
    sources: HashMap<String, SourceConfig>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SharedTransformConfig {
    pub enabled: bool,
}

impl std::fmt::Display for ConnectorConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectorConfig::Sink(config) => {
                write!(f, "sink {config}")
            }
            ConnectorConfig::Source(config) => {
                write!(f, "source {config}",)
            }
        }
    }
}

impl std::fmt::Display for SinkConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, name: {}, path: {}, transforms: {:?}, streams: [{}], plugin_config_format: {:?}, verbose: {}, benchmark: {} }}",
            self.enabled,
            self.name,
            self.path,
            self.transforms,
            self.streams
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<String>>()
                .join(", "),
            self.plugin_config_format,
            self.verbose,
            self.benchmark,
        )
    }
}

impl std::fmt::Display for SourceConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, name: {}, path: {}, transforms: {:?}, streams: [{}], plugin_config_format: {:?}, verbose: {}, benchmark: {}, exactly_once: {} }}",
            self.enabled,
            self.name,
            self.path,
            self.transforms,
            self.streams
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<String>>()
                .join(", "),
            self.plugin_config_format,
            self.verbose,
            self.benchmark,
            self.exactly_once,
        )
    }
}

impl std::fmt::Display for TransformsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let transforms: Vec<String> = self
            .transforms
            .iter()
            .map(|(k, v)| format!("{}: {}", k, v))
            .collect();
        write!(f, "{{ {} }}", transforms.join(", "))
    }
}

impl std::fmt::Display for StreamConsumerConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ stream: {}, topics: {}, schema: {:?}, avro_schema_json: {:?}, avro_schema_path: {:?}, batch_length: {:?}, poll_interval: {:?}, max_wait: {:?}, min_count: {:?}, consumer_group: {:?} }}",
            self.stream,
            self.topics
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<&str>>()
                .join(", "),
            self.schema,
            self.avro_schema_json,
            self.avro_schema_path,
            self.batch_length,
            self.poll_interval,
            self.max_wait,
            self.min_count,
            self.consumer_group
        )
    }
}

impl std::fmt::Display for StreamProducerConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ stream: {}, topic: {}, schema: {:?}, avro_schema_json: {:?}, avro_schema_path: {:?}, batch_length: {:?}, linger_time: {:?}, partition_id: {:?} }}",
            self.stream,
            self.topic,
            self.schema,
            self.avro_schema_json,
            self.avro_schema_path,
            self.batch_length,
            self.linger_time,
            self.partition_id
        )
    }
}

impl ConnectorsConfig {
    pub fn new(sinks: HashMap<String, SinkConfig>, sources: HashMap<String, SourceConfig>) -> Self {
        Self { sinks, sources }
    }

    pub fn sinks(&self) -> &HashMap<String, SinkConfig> {
        &self.sinks
    }

    pub fn sources(&self) -> &HashMap<String, SourceConfig> {
        &self.sources
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use configs::ConfigEnvMappings;

    #[test]
    fn given_sink_and_source_configs_when_mapped_should_exclude_key_and_version() {
        for (name, mappings) in [
            ("sink", SinkConfig::env_mappings()),
            ("source", SourceConfig::env_mappings()),
        ] {
            assert!(
                mappings.iter().any(|mapping| mapping.env_name == "ENABLED"),
                "{name} should map ENABLED from the environment"
            );
            assert!(
                mappings
                    .iter()
                    .all(|mapping| !matches!(mapping.env_name, "KEY" | "VERSION")),
                "{name} maps key or version from the environment"
            );
        }
    }

    #[test]
    fn given_a_sink_without_a_delivery_table_when_parsed_should_deliver_at_most_once() {
        let config: SinkConfig = toml::from_str(SINK_TOML).expect("the sink config should parse");

        assert_eq!(config.delivery, SinkDeliveryConfig::default());
        assert_eq!(config.delivery.guarantee, DeliveryGuarantee::AtMostOnce);
    }

    #[test]
    fn given_a_delivery_table_when_parsed_should_read_every_setting() {
        let toml = format!(
            r#"{SINK_TOML}
[delivery]
guarantee = "at_least_once"
retry_max_attempts = 5
retry_max_backoff = "1m"
flush_interval = "10s"
flush_max_messages = 500
invalid_messages = "skip"
"#
        );

        let config: SinkConfig = toml::from_str(&toml).expect("the sink config should parse");

        assert_eq!(
            config.delivery,
            SinkDeliveryConfig {
                guarantee: DeliveryGuarantee::AtLeastOnce,
                retry_max_attempts: NonZeroU32::new(5),
                retry_max_backoff: "1m".to_owned(),
                flush_interval: Some("10s".to_owned()),
                flush_max_messages: NonZeroU32::new(500).expect("500 is not zero"),
                invalid_messages: InvalidMessages::Skip,
                ..SinkDeliveryConfig::default()
            }
        );
    }

    #[test]
    fn given_the_sink_env_mappings_when_listed_should_reach_the_delivery_guarantee() {
        assert!(
            SinkConfig::env_mappings()
                .iter()
                .any(|mapping| mapping.env_name == "DELIVERY_GUARANTEE"),
            "the delivery guarantee should be settable from the environment"
        );
    }

    #[test]
    fn given_a_zero_attempt_limit_or_message_cap_when_parsed_should_refuse_it() {
        for setting in ["retry_max_attempts = 0", "flush_max_messages = 0"] {
            let toml = format!("{SINK_TOML}\n[delivery]\n{setting}\n");

            assert!(
                toml::from_str::<SinkConfig>(&toml).is_err(),
                "{setting} must be refused"
            );
        }
    }

    const SINK_TOML: &str = r#"
key = "test"
enabled = true
version = 0
name = "Test sink"
path = "target/debug/libtest_sink"

[[streams]]
stream = "test_stream"
topics = ["test_topic"]
schema = "json"
"#;

    #[test]
    fn given_single_component_key_when_parsed_should_succeed() {
        for key in ["postgres", "es-sink.v2_1", "A1", "9lives", "a.b-c_d"] {
            let parsed: ConnectorKey = key
                .parse()
                .unwrap_or_else(|error| panic!("key {key:?} should be accepted, got: {error}"));
            assert_eq!(parsed.as_str(), key);
            assert_eq!(parsed.to_string(), key);
        }
    }

    #[test]
    fn given_key_at_the_length_limit_when_parsed_should_succeed() {
        let key = "k".repeat(ConnectorKey::MAX_LENGTH);
        assert_eq!(key.parse::<ConnectorKey>().unwrap().as_str(), key);
    }

    #[test]
    fn given_key_over_the_length_limit_when_parsed_should_fail() {
        assert_rejected(&"k".repeat(ConnectorKey::MAX_LENGTH + 1));
    }

    #[test]
    fn given_key_with_path_separator_when_parsed_should_fail() {
        for key in ["../../pwned", "x/../../../tmp/pwn", "a/b", "a\\b", "/abs"] {
            assert_rejected(key);
        }
    }

    #[test]
    fn given_key_that_is_a_dot_segment_or_hidden_name_when_parsed_should_fail() {
        for key in [".", "..", "..evil", ".hidden"] {
            assert_rejected(key);
        }
    }

    #[test]
    fn given_key_with_characters_outside_the_charset_when_parsed_should_fail() {
        for key in [
            "",
            "-leading-dash",
            "_leading_underscore",
            "with space",
            "k\0ey",
            "k\ney",
            "ключ",
            "a#b",
        ] {
            assert_rejected(key);
        }
    }

    #[test]
    fn given_owned_key_when_converted_should_apply_the_same_rule() {
        let accepted = ConnectorKey::try_from("random".to_owned()).unwrap();
        assert_eq!(accepted.as_str(), "random");

        let rejected = ConnectorKey::try_from("../pwned".to_owned()).unwrap_err();
        assert!(
            matches!(&rejected, RuntimeError::InvalidConnectorKey(key) if key == "../pwned"),
            "unexpected error: {rejected}"
        );
    }

    fn assert_rejected(key: &str) {
        let result = key.parse::<ConnectorKey>();
        assert!(
            matches!(&result, Err(RuntimeError::InvalidConnectorKey(rejected)) if rejected == key),
            "key {key:?} should be rejected, got: {result:?}"
        );
    }
}
