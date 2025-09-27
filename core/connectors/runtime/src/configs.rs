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

use crate::api::config::HttpConfig;
use figment::{
    Metadata, Profile, Provider,
    providers::{Format, Toml},
    value::Dict,
};
use iggy::prelude::{DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME};
use iggy_common::{CustomEnvProvider, FileConfigProvider};
use iggy_connector_sdk::{Schema, transforms::TransformType};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Formatter};
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

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ConnectorsConfig {
    pub http: HttpConfig,
    pub iggy: IggyConfig,
    pub sinks: HashMap<String, SinkConfig>,
    pub sources: HashMap<String, SourceConfig>,
    pub state: StateConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IggyConfig {
    pub address: String,
    pub username: String,
    pub password: String,
    pub token: String,
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

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SinkConfig {
    pub enabled: bool,
    pub name: String,
    pub path: String,
    pub transforms: Option<TransformsConfig>,
    pub streams: Vec<StreamConsumerConfig>,
    pub config_format: Option<ConfigFormat>,
    pub config: Option<serde_json::Value>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SourceConfig {
    pub enabled: bool,
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
pub struct SharedTransformConfig {
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateConfig {
    pub path: String,
}

impl std::fmt::Display for StateConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{ path: {} }}", self.path)
    }
}

impl std::fmt::Display for ConnectorsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ http: {}, iggy: {}, sinks: {:?}, sources: {:?}, state: {:} }}",
            self.http, self.iggy, self.sinks, self.sources, self.state
        )
    }
}

impl std::fmt::Display for IggyConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ address: {}, username: {}, password: {}, token: {} }}",
            self.address,
            self.username,
            if !self.password.is_empty() {
                "****"
            } else {
                ""
            },
            if !self.token.is_empty() { "****" } else { "" },
        )
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

impl Default for StateConfig {
    fn default() -> Self {
        Self {
            path: "local_state".to_owned(),
        }
    }
}

impl Default for IggyConfig {
    fn default() -> Self {
        Self {
            address: "localhost:8090".to_owned(),
            username: DEFAULT_ROOT_USERNAME.to_owned(),
            password: DEFAULT_ROOT_PASSWORD.to_owned(),
            token: "".to_owned(),
        }
    }
}

impl ConnectorsConfig {
    pub fn config_provider(path: String) -> FileConfigProvider<ConnectorsEnvProvider> {
        let default_config = Toml::string(include_str!("../../../connectors/runtime/config.toml"));
        FileConfigProvider::new(
            path,
            ConnectorsEnvProvider::default(),
            true,
            Some(default_config),
        )
    }
}

#[derive(Debug, Clone)]
pub struct ConnectorsEnvProvider {
    provider: CustomEnvProvider<ConnectorsConfig>,
}

impl Default for ConnectorsEnvProvider {
    fn default() -> Self {
        Self {
            provider: CustomEnvProvider::new("IGGY_CONNECTORS_", &[]),
        }
    }
}

impl Provider for ConnectorsEnvProvider {
    fn metadata(&self) -> Metadata {
        Metadata::named("iggy-connectors-config")
    }

    fn data(&self) -> Result<figment::value::Map<Profile, Dict>, figment::Error> {
        self.provider.deserialize().map_err(|_| {
            figment::Error::from("Cannot deserialize environment variables for connectors config")
        })
    }
}
