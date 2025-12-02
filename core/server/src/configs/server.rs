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
use crate::IGGY_ROOT_PASSWORD_ENV;
use crate::configs::COMPONENT;
use crate::configs::cluster::ClusterConfig;
use crate::configs::http::HttpConfig;
use crate::configs::quic::QuicConfig;
use crate::configs::system::SystemConfig;
use crate::configs::tcp::TcpConfig;
use crate::configs::websocket::WebSocketConfig;
use crate::server_error::ConfigurationError;
use derive_more::Display;
use err_trail::ErrContext;
use figment::providers::{Format, Toml};
use figment::value::Dict;
use figment::{Metadata, Profile, Provider};
use iggy_common::{ConfigProvider, IggyByteSize, MemoryPoolConfigOther, Validatable};
use iggy_common::{CustomEnvProvider, FileConfigProvider, IggyDuration};
use serde::{Deserialize, Serialize};
use serde_with::DisplayFromStr;
use serde_with::serde_as;
use std::env;
use std::str::FromStr;
use std::sync::Arc;

const DEFAULT_CONFIG_PATH: &str = "configs/server.toml";
const SECRET_KEYS: [&str; 6] = [
    IGGY_ROOT_PASSWORD_ENV,
    "IGGY_DATA_MAINTENANCE_ARCHIVER_S3_KEY_SECRET",
    "IGGY_HTTP_JWT_ENCODING_SECRET",
    "IGGY_HTTP_JWT_DECODING_SECRET",
    "IGGY_TCP_TLS_PASSWORD",
    "IGGY_SYSTEM_ENCRYPTION_KEY",
];

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

/// Configuration for the memory pool.
#[derive(Debug, Deserialize, Serialize)]
pub struct MemoryPoolConfig {
    /// Whether the pool is enabled.
    pub enabled: bool,
    /// Maximum size of the pool.
    pub size: IggyByteSize,
    /// Maximum number of buffers per bucket.
    pub bucket_capacity: u32,
}

// Hack around the fact that we define our config inside of the `server`  crate, but `memory_pool` is in `common`.
impl MemoryPoolConfig {
    pub fn into_other(&self) -> MemoryPoolConfigOther {
        MemoryPoolConfigOther {
            enabled: self.enabled,
            size: self.size,
            bucket_capacity: self.bucket_capacity,
        }
    }
}

#[serde_as]
#[derive(Debug, Default, Deserialize, Serialize, Clone)]
pub struct DataMaintenanceConfig {
    pub messages: MessagesMaintenanceConfig,
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MessagesMaintenanceConfig {
    pub cleaner_enabled: bool,
    #[serde_as(as = "DisplayFromStr")]
    pub interval: IggyDuration,
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
    pub async fn load() -> Result<ServerConfig, ConfigurationError> {
        let config_path =
            env::var("IGGY_CONFIG_PATH").unwrap_or_else(|_| DEFAULT_CONFIG_PATH.to_string());
        let config_provider = ServerConfig::config_provider(&config_path);
        let server_config: ServerConfig =
            config_provider.load_config().await.with_error(|error| {
                format!("{COMPONENT} (error: {error}) - failed to load config provider config")
            })?;
        server_config.validate().with_error(|error| {
            format!("{COMPONENT} (error: {error}) - failed to validate server config")
        })?;
        Ok(server_config)
    }

    pub fn config_provider(config_path: &str) -> FileConfigProvider<ServerConfigEnvProvider> {
        let default_config = Toml::string(include_str!("../../../configs/server.toml"));
        FileConfigProvider::new(
            config_path.to_string(),
            ServerConfigEnvProvider::default(),
            true,
            Some(default_config),
        )
    }
}

#[derive(Debug, Clone)]
pub struct ServerConfigEnvProvider {
    provider: CustomEnvProvider<ServerConfig>,
}

impl Default for ServerConfigEnvProvider {
    fn default() -> Self {
        Self {
            provider: CustomEnvProvider::new("IGGY_", &SECRET_KEYS),
        }
    }
}

impl Provider for ServerConfigEnvProvider {
    fn metadata(&self) -> Metadata {
        Metadata::named("iggy-server config")
    }

    fn data(&self) -> Result<figment::value::Map<Profile, Dict>, figment::Error> {
        self.provider.deserialize().map_err(|_| {
            figment::Error::from("Cannot deserialize environment variables for server config")
        })
    }
}
