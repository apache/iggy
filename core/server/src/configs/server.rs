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

use crate::configs::COMPONENT;
use crate::configs::cluster::ClusterConfig;
use crate::configs::config_provider::ConfigProviderKind;
use crate::configs::http::HttpConfig;
use crate::configs::quic::QuicConfig;
use crate::configs::system::SystemConfig;
use crate::configs::tcp::TcpConfig;
use crate::configs::websocket::WebSocketConfig;
use crate::server_error::ConfigError;
use derive_more::Display;
use err_trail::ErrContext;
use iggy_common::IggyDuration;
use iggy_common::Validatable;
use serde::{Deserialize, Serialize};
use serde_with::DisplayFromStr;
use serde_with::serde_as;
use std::str::FromStr;
use std::sync::Arc;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ServerConfig {
    pub data_maintenance: DataMaintenanceConfig,
    pub message_saver: MessageSaverConfig,
    pub personal_access_token: PersonalAccessTokenConfig,
    pub heartbeat: HeartbeatConfig,
    pub system: Arc<SystemConfig>,
    pub quic: QuicConfig,
    pub tcp: TcpConfig,
    pub http: HttpConfig,
    pub websocket: WebSocketConfig,
    pub telemetry: TelemetryConfig,
    pub cluster: ClusterConfig,
}

#[serde_as]
#[derive(Debug, Default, Deserialize, Serialize, Clone)]
pub struct DataMaintenanceConfig {
    pub messages: MessagesMaintenanceConfig,
    pub state: StateMaintenanceConfig,
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MessagesMaintenanceConfig {
    pub archiver_enabled: bool,
    pub cleaner_enabled: bool,
    #[serde_as(as = "DisplayFromStr")]
    pub interval: IggyDuration,
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct StateMaintenanceConfig {
    pub archiver_enabled: bool,
    pub overwrite: bool,
    #[serde_as(as = "DisplayFromStr")]
    pub interval: IggyDuration,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DiskArchiverConfig {
    pub path: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct S3ArchiverConfig {
    pub key_id: String,
    pub key_secret: String,
    pub bucket: String,
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub tmp_upload_dir: String,
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MessageSaverConfig {
    pub enabled: bool,
    pub enforce_fsync: bool,
    #[serde_as(as = "DisplayFromStr")]
    pub interval: IggyDuration,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PersonalAccessTokenConfig {
    pub max_tokens_per_user: u32,
    pub cleaner: PersonalAccessTokenCleanerConfig,
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PersonalAccessTokenCleanerConfig {
    pub enabled: bool,
    #[serde_as(as = "DisplayFromStr")]
    pub interval: IggyDuration,
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct HeartbeatConfig {
    pub enabled: bool,
    #[serde_as(as = "DisplayFromStr")]
    pub interval: IggyDuration,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TelemetryConfig {
    pub enabled: bool,
    pub service_name: String,
    pub logs: TelemetryLogsConfig,
    pub traces: TelemetryTracesConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TelemetryLogsConfig {
    pub transport: TelemetryTransport,
    pub endpoint: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TelemetryTracesConfig {
    pub transport: TelemetryTransport,
    pub endpoint: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Display, Copy, Clone)]
#[serde(rename_all = "lowercase")]
pub enum TelemetryTransport {
    #[display("grpc")]
    GRPC,
    #[display("http")]
    HTTP,
}

impl FromStr for TelemetryTransport {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "grpc" => Ok(TelemetryTransport::GRPC),
            "http" => Ok(TelemetryTransport::HTTP),
            _ => Err(format!("Invalid telemetry transport: {s}")),
        }
    }
}

impl ServerConfig {
    pub async fn load(config_provider: &ConfigProviderKind) -> Result<ServerConfig, ConfigError> {
        let server_config = config_provider.load_config().await.with_error(|error| {
            format!("{COMPONENT} (error: {error}) - failed to load config provider config")
        })?;
        server_config.validate().with_error(|error| {
            format!("{COMPONENT} (error: {error}) - failed to validate server config")
        })?;
        Ok(server_config)
    }
}
