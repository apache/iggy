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

use super::COMPONENT;
use super::cluster::ClusterConfig;
use super::http::HttpConfig;
use super::quic::QuicConfig;
use super::system::SystemConfig;
use super::tcp::TcpConfig;
use super::websocket::WebSocketConfig;
use crate::ConfigurationError;
use configs::{ConfigEnv, ConfigEnvMappings, ConfigProvider, FileConfigProvider, TypedEnvProvider};
use err_trail::ErrContext;
use figment::providers::{Format, Toml};
use figment::value::Dict;
use figment::{Metadata, Profile, Provider};
use iggy_common::{IggyByteSize, IggyDuration, Validatable};
use serde::{Deserialize, Serialize};
use serde_with::DisplayFromStr;
use serde_with::serde_as;
use server_common::MemoryPoolConfigOther;
use server_common::log::{TelemetryEndpointSettings, TelemetrySettings};
use std::env;
use std::sync::Arc;

pub use server_common::log::TelemetryTransport;

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

impl From<&TelemetryConfig> for TelemetrySettings {
    fn from(config: &TelemetryConfig) -> Self {
        Self {
            enabled: config.enabled,
            service_name: config.service_name.clone(),
            logs: TelemetryEndpointSettings {
                transport: config.logs.transport,
                endpoint: config.logs.endpoint.clone(),
            },
            traces: TelemetryEndpointSettings {
                transport: config.traces.transport,
                endpoint: config.traces.endpoint.clone(),
            },
        }
    }
}

impl ServerConfig {
    /// Load server configuration from file and environment variables.
    ///
    /// Uses compile-time generated env var mappings for unambiguous resolution.
    pub async fn load() -> Result<ServerConfig, ConfigurationError> {
        Self::load_with_path(
            DEFAULT_CONFIG_PATH,
            include_str!("../../../server/config.toml"),
        )
        .await
    }

    pub async fn load_with_path(
        default_config_path: &str,
        default_config: &'static str,
    ) -> Result<ServerConfig, ConfigurationError> {
        let config_path =
            env::var("IGGY_CONFIG_PATH").unwrap_or_else(|_| default_config_path.to_string());
        let config_provider =
            ServerConfig::config_provider_with_default(&config_path, default_config);
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
        Self::config_provider_with_default(config_path, include_str!("../../../server/config.toml"))
    }

    /// Create a config provider using compile-time generated env var mappings.
    pub fn config_provider_with_default(
        config_path: &str,
        default_config: &'static str,
    ) -> FileConfigProvider<ServerConfigEnvProvider> {
        let default_config = Toml::string(default_config);
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
