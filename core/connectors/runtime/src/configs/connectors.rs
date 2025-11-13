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

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SinkConfig {
    pub id: String,
    pub enabled: bool,
    pub version: u64,
    pub name: String,
    pub path: String,
    pub transforms: Option<TransformsConfig>,
    pub streams: Vec<StreamConsumerConfig>,
    pub config_format: Option<ConfigFormat>,
    pub config: Option<serde_json::Value>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SourceConfig {
    pub id: String,
    pub enabled: bool,
    pub version: u64,
    pub name: String,
    pub path: String,
    pub transforms: Option<TransformsConfig>,
    pub streams: Vec<StreamProducerConfig>,
    pub config_format: Option<ConfigFormat>,
    pub config: Option<serde_json::Value>,
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

#[async_trait]
pub trait ConnectorsConfigProvider {
    async fn load_configs(&self) -> Result<ConnectorsConfig, RuntimeError>;
}

impl From<RuntimeConnectorsConfig> for Box<dyn ConnectorsConfigProvider> {
    fn from(value: RuntimeConnectorsConfig) -> Self {
        match value {
            RuntimeConnectorsConfig::Local(config) => {
                Box::new(LocalConnectorsConfigProvider::new(&config.config_dir))
            }
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
            "{{ enabled: {}, name: {}, path: {}, transforms: {:?}, streams: [{}], config_format: {:?} }}",
            self.enabled,
            self.name,
            self.path,
            self.transforms,
            self.streams
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<String>>()
                .join(", "),
            self.config_format,
        )
    }
}

impl std::fmt::Display for SourceConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ enabled: {}, name: {}, path: {}, transforms: {:?}, streams: [{}], config_format: {:?} }}",
            self.enabled,
            self.name,
            self.path,
            self.transforms,
            self.streams
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<String>>()
                .join(", "),
            self.config_format,
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
    pub fn sinks(&self) -> HashMap<String, SinkConfig> {
        self.sinks.clone()
    }

    pub fn sources(&self) -> HashMap<String, SourceConfig> {
        self.sources.clone()
    }
}
