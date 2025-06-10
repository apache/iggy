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

use std::sync::Arc;

use iggy_connector_sdk::transforms::TransformType;
use serde::{Deserialize, Serialize};

use crate::{
    configs::{ConfigFormat, StreamConsumerConfig, StreamProducerConfig},
    manager::{
        sink::{SinkDetails, SinkInfo},
        source::{SourceDetails, SourceInfo},
    },
};

#[derive(Debug, Serialize, Deserialize)]
pub struct SinkInfoResponse {
    pub id: u32,
    pub key: String,
    pub name: String,
    pub path: String,
    pub enabled: bool,
    pub running: bool,
    pub config_format: Option<ConfigFormat>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SinkDetailsResponse {
    #[serde(flatten)]
    pub info: SinkInfoResponse,
    pub streams: Arc<Vec<StreamConsumerConfig>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SourceInfoResponse {
    pub id: u32,
    pub key: String,
    pub name: String,
    pub path: String,
    pub enabled: bool,
    pub running: bool,
    pub config_format: Option<ConfigFormat>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SourceDetailsResponse {
    #[serde(flatten)]
    pub info: SourceInfoResponse,
    pub streams: Arc<Vec<StreamProducerConfig>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TransformResponse {
    pub r#type: TransformType,
    pub config: Arc<serde_json::Value>,
}

impl From<&SinkInfo> for SinkInfoResponse {
    fn from(sink: &SinkInfo) -> Self {
        SinkInfoResponse {
            id: sink.id,
            key: sink.key.to_owned(),
            name: sink.name.to_owned(),
            path: sink.path.to_owned(),
            enabled: sink.enabled,
            running: sink.running,
            config_format: sink.config_format,
        }
    }
}

impl From<&SinkDetails> for SinkDetailsResponse {
    fn from(sink: &SinkDetails) -> Self {
        let info = &sink.info;
        SinkDetailsResponse {
            info: info.into(),
            streams: sink.streams.clone(),
        }
    }
}

impl From<&SourceInfo> for SourceInfoResponse {
    fn from(source: &SourceInfo) -> Self {
        SourceInfoResponse {
            id: source.id,
            key: source.key.to_owned(),
            name: source.name.to_owned(),
            path: source.path.to_owned(),
            enabled: source.enabled,
            running: source.running,
            config_format: source.config_format,
        }
    }
}

impl From<&SourceDetails> for SourceDetailsResponse {
    fn from(source: &SourceDetails) -> Self {
        let info = &source.info;
        SourceDetailsResponse {
            info: info.into(),
            streams: source.streams.clone(),
        }
    }
}
