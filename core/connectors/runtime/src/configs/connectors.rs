/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

mod local_provider;

use crate::configs::connectors::local_provider::LocalConnectorsConfigProvider;
use crate::configs::runtime::ConnectorsConfig as RuntimeConnectorsConfig;
use crate::error::RuntimeError;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use iggy_connector_sdk::Schema;
use iggy_connector_sdk::transforms::TransformType;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Formatter;
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
}

impl CreateSinkConfig {
    fn to_sink_config(&self, key: &str, version: u64) -> SinkConfig {
        SinkConfig {
            key: key.to_owned(),
            enabled: self.enabled,
            version,
            name: self.name.clone(),
            path: self.path.clone(),
            transforms: self.transforms.clone(),
            streams: self.streams.clone(),
            plugin_config_format: self.plugin_config_format,
            plugin_config: self.plugin_config.clone(),
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SinkConfig {
    pub key: String,
    pub enabled: bool,
    pub version: u64,
    pub name: String,
    pub path: String,
    pub transforms: Option<TransformsConfig>,
    pub streams: Vec<StreamConsumerConfig>,
    pub plugin_config_format: Option<ConfigFormat>,
    pub plugin_config: Option<serde_json::Value>,
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
}

impl CreateSourceConfig {
    fn to_source_config(&self, key: &str, version: u64) -> SourceConfig {
        SourceConfig {
            key: key.to_owned(),
            enabled: self.enabled,
            version,
            name: self.name.clone(),
            path: self.path.clone(),
            transforms: self.transforms.clone(),
            streams: self.streams.clone(),
            plugin_config_format: self.plugin_config_format,
            plugin_config: self.plugin_config.clone(),
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SourceConfig {
    pub key: String,
    pub enabled: bool,
    pub version: u64,
    pub name: String,
    pub path: String,
    pub transforms: Option<TransformsConfig>,
    pub streams: Vec<StreamProducerConfig>,
    pub plugin_config_format: Option<ConfigFormat>,
    pub plugin_config: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TransformsConfig {
    #[serde(flatten)]
    pub transforms: HashMap<TransformType, serde_json::Value>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct StreamConsumerConfig {
    pub stream: String,
    pub topics: Vec<String>,
    pub schema: Schema,
    pub batch_length: Option<u32>,
    pub poll_interval: Option<String>,
    pub consumer_group: Option<String>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct StreamProducerConfig {
    pub stream: String,
    pub topic: String,
    pub schema: Schema,
    pub batch_length: Option<u32>,
    pub linger_time: Option<String>,
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

#[async_trait]
pub trait ConnectorsConfigProvider: Send + Sync {
    async fn create_sink_config(
        &self,
        key: &str,
        config: CreateSinkConfig,
    ) -> Result<SinkConfig, RuntimeError>;
    async fn create_source_config(
        &self,
        key: &str,
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
            "{{ enabled: {}, name: {}, path: {}, transforms: {:?}, streams: [{}], plugin_config_format: {:?} }}",
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
        )
    }
}

impl std::fmt::Display for SourceConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, name: {}, path: {}, transforms: {:?}, streams: [{}], plugin_config_format: {:?} }}",
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
            "{{ stream: {}, topics: {}, schema: {:?}, batch_length: {:?}, poll_interval: {:?}, consumer_group: {:?} }}",
            self.stream,
            self.topics
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<&str>>()
                .join(", "),
            self.schema,
            self.batch_length,
            self.poll_interval,
            self.consumer_group
        )
    }
}

impl std::fmt::Display for StreamProducerConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ stream: {}, topic: {}, schema: {:?}, batch_length: {:?}, linger_time: {:?} }}",
            self.stream, self.topic, self.schema, self.batch_length, self.linger_time
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
