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
use super::COMPONENT;
use super::cluster::ClusterConfig;
use super::http::HttpConfig;
use super::quic::QuicConfig;
use super::system::SystemConfig;
use super::tcp::TcpConfig;
use super::websocket::WebSocketConfig;
use crate::ConfigurationError;
use configs::{ConfigEnv, ConfigEnvMappings, ConfigProvider, FileConfigProvider, TypedEnvProvider};
use derive_more::Display;
use err_trail::ErrContext;
use figment::providers::{Format, Toml};
use figment::value::Dict;
use figment::{Metadata, Profile, Provider};
use iggy_common::{IggyByteSize, IggyDuration, MemoryPoolConfigOther, Validatable};
use serde::{Deserialize, Serialize};
use serde_with::DisplayFromStr;
use serde_with::serde_as;
use std::env;
use std::str::FromStr;
use std::sync::Arc;

const DEFAULT_CONFIG_PATH: &str = "core/server/config.toml";

#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
#[config_env(prefix = "IGGY_", name = "iggy-server-config")]
pub struct ServerConfig {
    pub consumer_group: ConsumerGroupConfig,
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
#[derive(Debug, Deserialize, Serialize, ConfigEnv)]
pub struct MemoryPoolConfig {
    pub enabled: bool,
    #[config_env(leaf)]
    pub size: IggyByteSize,
    pub bucket_capacity: u32,
}

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
#[derive(Debug, Default, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct DataMaintenanceConfig {
    pub messages: MessagesMaintenanceConfig,
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct MessagesMaintenanceConfig {
    pub cleaner_enabled: bool,
    #[config_env(leaf)]
    #[serde_as(as = "DisplayFromStr")]
    pub interval: IggyDuration,
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct MessageSaverConfig {
    pub enabled: bool,
    pub enforce_fsync: bool,
    #[config_env(leaf)]
    #[serde_as(as = "DisplayFromStr")]
    pub interval: IggyDuration,
}

#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct PersonalAccessTokenConfig {
    pub max_tokens_per_user: u32,
    pub cleaner: PersonalAccessTokenCleanerConfig,
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct PersonalAccessTokenCleanerConfig {
    pub enabled: bool,
    #[config_env(leaf)]
    #[serde_as(as = "DisplayFromStr")]
    pub interval: IggyDuration,
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct HeartbeatConfig {
    pub enabled: bool,
    #[config_env(leaf)]
    #[serde_as(as = "DisplayFromStr")]
    pub interval: IggyDuration,
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct ConsumerGroupConfig {
    #[config_env(leaf)]
    #[serde_as(as = "DisplayFromStr")]
    pub rebalancing_timeout: IggyDuration,
    #[config_env(leaf)]
    #[serde_as(as = "DisplayFromStr")]
    pub rebalancing_check_interval: IggyDuration,
}

#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct TelemetryConfig {
    pub enabled: bool,
    pub service_name: String,
    pub logs: TelemetryLogsConfig,
    pub traces: TelemetryTracesConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct TelemetryLogsConfig {
    #[config_env(leaf)]
    pub transport: TelemetryTransport,
    pub endpoint: String,
}

#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct TelemetryTracesConfig {
    #[config_env(leaf)]
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
    /// Load server configuration from file and environment variables.
    ///
    /// Uses compile-time generated env var mappings for unambiguous resolution.
    pub async fn load() -> Result<ServerConfig, ConfigurationError> {
        let config_path =
            env::var("IGGY_CONFIG_PATH").unwrap_or_else(|_| DEFAULT_CONFIG_PATH.to_string());
        let config_provider = ServerConfig::config_provider(&config_path);
        let server_config: ServerConfig =
            config_provider
                .load_config()
                .await
                .error(|e: &configs::ConfigurationError| {
                    format!("{COMPONENT} (error: {e}) - failed to load config")
                })?;
        server_config
            .validate()
            .error(|e: &configs::ConfigurationError| {
                format!("{COMPONENT} (error: {e}) - failed to validate server config")
            })?;
        Ok(server_config)
    }

    /// Create a config provider using compile-time generated env var mappings.
    pub fn config_provider(config_path: &str) -> FileConfigProvider<ServerConfigEnvProvider> {
        let default_config = Toml::string(include_str!("../../../server/config.toml"));
        FileConfigProvider::new(
            config_path.to_string(),
            ServerConfigEnvProvider::default(),
            true,
            Some(default_config),
        )
    }

    /// Returns all valid environment variable names for ServerConfig.
    pub fn all_env_var_names() -> Vec<&'static str> {
        <ServerConfig as ConfigEnvMappings>::all_env_var_names()
    }
}

/// Type-safe environment provider using compile-time generated mappings.
///
/// Uses the `ConfigEnvMappings` trait generated by `#[derive(ConfigEnv)]`
/// to directly look up known environment variable names, eliminating path ambiguity.
#[derive(Debug, Clone)]
pub struct ServerConfigEnvProvider {
    provider: TypedEnvProvider<ServerConfig>,
}

impl Default for ServerConfigEnvProvider {
    fn default() -> Self {
        Self {
            provider: TypedEnvProvider::from_config(ServerConfig::ENV_PREFIX),
        }
    }
}

impl Provider for ServerConfigEnvProvider {
    fn metadata(&self) -> Metadata {
        Metadata::named(ServerConfig::ENV_PROVIDER_NAME)
    }

    fn data(&self) -> Result<figment::value::Map<Profile, Dict>, figment::Error> {
        self.provider.deserialize().map_err(|e| {
            figment::Error::from(format!(
                "Cannot deserialize environment variables for server config: {e}"
            ))
        })
    }
}
