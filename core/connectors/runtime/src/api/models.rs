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

use crate::configs::connectors::{
    ConfigFormat, SinkConfig, SourceConfig, StreamConsumerConfig, StreamProducerConfig,
};
use crate::manager::{sink::SinkInfo, source::SourceInfo};
use iggy_connector_sdk::transforms::TransformType;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct SinkInfoResponse {
    pub id: u32,
    pub key: String,
    pub name: String,
    pub path: String,
    pub enabled: bool,
    pub running: bool,
    pub plugin_config_format: Option<ConfigFormat>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SinkDetailsResponse {
    #[serde(flatten)]
    pub info: SinkInfoResponse,
    pub streams: Vec<StreamConsumerConfig>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SinkConfigResponse {
    #[serde(flatten)]
    pub config: SinkConfig,
    pub active: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SourceInfoResponse {
    pub id: u32,
    pub key: String,
    pub name: String,
    pub path: String,
    pub enabled: bool,
    pub running: bool,
    pub plugin_config_format: Option<ConfigFormat>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SourceDetailsResponse {
    #[serde(flatten)]
    pub info: SourceInfoResponse,
    pub streams: Vec<StreamProducerConfig>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SourceConfigResponse {
    #[serde(flatten)]
    pub config: SourceConfig,
    pub active: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TransformResponse {
    pub r#type: TransformType,
    pub config: serde_json::Value,
}

impl From<SinkInfo> for SinkInfoResponse {
    fn from(sink: SinkInfo) -> Self {
        SinkInfoResponse {
            id: sink.id,
            key: sink.key,
            name: sink.name,
            path: sink.path,
            enabled: sink.enabled,
            running: sink.running,
            plugin_config_format: sink.plugin_config_format,
        }
    }
}

impl From<SourceInfo> for SourceInfoResponse {
    fn from(source: SourceInfo) -> Self {
        SourceInfoResponse {
            id: source.id,
            key: source.key,
            name: source.name,
            path: source.path,
            enabled: source.enabled,
            running: source.running,
            plugin_config_format: source.plugin_config_format,
        }
    }
}
