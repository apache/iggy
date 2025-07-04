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

use crate::api::config::HttpApiConfig;
use iggy_connector_sdk::{Schema, transforms::TransformType};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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
    #[strum(to_string = "toml")]
    Toml,
    #[default]
    #[strum(to_string = "text")]
    Text,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RuntimeConfig {
    pub http_api: HttpApiConfig,
    pub iggy: IggyConfig,
    pub sinks: HashMap<String, SinkConfig>,
    pub sources: HashMap<String, SourceConfig>,
    pub state: StateConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IggyConfig {
    pub address: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SinkConfig {
    pub enabled: bool,
    pub name: String,
    pub path: String,
    pub transforms: Option<TransformsConfig>,
    pub streams: Vec<StreamConsumerConfig>,
    pub config_format: Option<ConfigFormat>,
    pub config: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamConsumerConfig {
    pub stream: String,
    pub topics: Vec<String>,
    pub schema: Schema,
    pub batch_length: Option<u32>,
    pub poll_interval: Option<String>,
    pub consumer_group: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamProducerConfig {
    pub stream: String,
    pub topic: String,
    pub schema: Schema,
    pub batch_length: Option<u32>,
    pub linger_time: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SharedTransformConfig {
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateConfig {
    pub path: String,
}
