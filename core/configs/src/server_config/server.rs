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
use super::message_bus::MessageBusConfig;
use super::metadata::MetadataConfig;
use super::partition::PartitionConfig;
use super::quic::QuicConfig;
use super::tcp::TcpConfig;
use super::websocket::WebSocketConfig;
use crate::ConfigurationError;
use crate::common::http::HttpConfig;
use crate::common::system::SystemConfig;
use configs::{ConfigEnv, ConfigEnvMappings, ConfigProvider, FileConfigProvider, TypedEnvProvider};
use err_trail::ErrContext;
use figment::providers::{Format, Toml};
use figment::value::Dict;
use figment::{Metadata, Profile, Provider};
use iggy_common::Validatable;
use serde::{Deserialize, Serialize};
use server_common::sharding::{MAX_PARTITIONS, MAX_STREAMS, MAX_TOPICS};
use std::env;
use std::sync::Arc;

pub use crate::common::server::{
    ConsumerGroupConfig, DataMaintenanceConfig, HeartbeatConfig, MemoryPoolConfig,
    MessageSaverConfig, MessagesMaintenanceConfig, PersonalAccessTokenCleanerConfig,
    PersonalAccessTokenConfig, TelemetryConfig, TelemetryLogsConfig, TelemetryTracesConfig,
    TelemetryTransport,
};

const DEFAULT_CONFIG_PATH: &str = "core/server/config.toml";

/// [`SystemConfig`] bound to this crate's own
/// [`super::sharding::ShardingConfig`]. `core/server` names this alias
/// wherever it refers to the system config.
pub type ServerSystemConfig = SystemConfig<super::sharding::ShardingConfig>;

/// Top-level on-disk config schema for the `iggy-server` binary.
///
/// Composes the shared section types from [`crate::common`] with the
/// transport, cluster, metadata and [`MessageBusConfig`] sections owned
/// by [`super`].
#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
#[config_env(prefix = "IGGY_", name = "iggy-server-config")]
pub struct ServerConfig {
    pub consumer_group: ConsumerGroupConfig,
    pub data_maintenance: DataMaintenanceConfig,
    #[serde(default)]
    pub extra: ExtraConfig,
    pub message_saver: MessageSaverConfig,
    pub personal_access_token: PersonalAccessTokenConfig,
    pub heartbeat: HeartbeatConfig,
    pub system: Arc<ServerSystemConfig>,
    pub quic: QuicConfig,
    pub tcp: TcpConfig,
    pub http: HttpConfig,
    pub websocket: WebSocketConfig,
    pub telemetry: TelemetryConfig,
    pub cluster: ClusterConfig,
    pub metadata: MetadataConfig,
    pub partition: PartitionConfig,
    pub message_bus: MessageBusConfig,
}

#[derive(Debug, Default, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct ExtraConfig {
    pub namespace: NamespaceConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct NamespaceConfig {
    pub max_streams: usize,
    pub max_topics: usize,
    pub max_partitions: usize,
}

impl Default for NamespaceConfig {
    fn default() -> Self {
        Self {
            max_streams: MAX_STREAMS,
            max_topics: MAX_TOPICS,
            max_partitions: MAX_PARTITIONS,
        }
    }
}

impl ServerConfig {
    /// Load server configuration from file and environment variables.
    ///
    /// The path comes from `IGGY_CONFIG_PATH` or defaults to
    /// `core/server/config.toml`; missing on-disk paths fall through
    /// to the embedded default TOML; env-var overrides flow through the
    /// [`ServerConfigEnvProvider`]; the result is validated before
    /// returning.
    ///
    /// # Errors
    /// Returns [`ConfigurationError`] when the config cannot be parsed
    /// from the configured source(s) or fails [`Validatable::validate`].
    pub async fn load() -> Result<ServerConfig, ConfigurationError> {
        let config_path =
            env::var("IGGY_CONFIG_PATH").unwrap_or_else(|_| DEFAULT_CONFIG_PATH.to_string());
        let provider = ServerConfig::config_provider(&config_path);
        let cfg: ServerConfig =
            provider
                .load_config()
                .await
                .error(|e: &configs::ConfigurationError| {
                    format!("{COMPONENT} (error: {e}) - failed to load server config")
                })?;
        cfg.validate().error(|e: &configs::ConfigurationError| {
            format!("{COMPONENT} (error: {e}) - failed to validate server config")
        })?;
        Ok(cfg)
    }

    /// Build the file-backed config provider with the embedded default
    /// TOML and the type-safe env-var provider attached.
    pub fn config_provider(config_path: &str) -> FileConfigProvider<ServerConfigEnvProvider> {
        let default_config = Toml::string(include_str!("../../../server/config.toml"));
        FileConfigProvider::new(
            config_path.to_string(),
            ServerConfigEnvProvider::default(),
            true,
            Some(default_config),
        )
    }

    /// All recognised env var names for [`ServerConfig`].
    pub fn all_env_var_names() -> Vec<&'static str> {
        <ServerConfig as ConfigEnvMappings>::all_env_var_names()
    }
}

/// Type-safe environment provider for [`ServerConfig`].
///
/// Uses the [`ConfigEnvMappings`] trait generated by `#[derive(ConfigEnv)]`
/// to look up known env var names directly, eliminating path ambiguity.
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

#[cfg(test)]
mod tests {
    use super::*;
    use figment::Figment;

    /// The embedded default TOML deserializes into a fully populated
    /// [`ServerConfig`] and passes validation. Exercises the
    /// `include_str!` resolution and the deserialization of every
    /// section without depending on an async runtime in `dev-deps`.
    #[test]
    fn embedded_default_toml_deserializes_and_validates() {
        let toml_str = include_str!("../../../server/config.toml");
        let cfg: ServerConfig = Figment::new()
            .merge(Toml::string(toml_str))
            .extract()
            .expect("embedded TOML deserializes");
        cfg.validate().expect("embedded default validates");

        // Spot-check: defaults match the runtime crate's invariants.
        assert_eq!(cfg.message_bus.max_batch, 256);
        assert_eq!(cfg.message_bus.peer_queue_capacity, 256);
    }

    #[test]
    fn default_impl_validates() {
        let cfg = ServerConfig::default();
        cfg.validate().expect("Default impl validates");
    }

    #[test]
    fn env_prefix_is_iggy() {
        assert_eq!(ServerConfig::ENV_PREFIX, "IGGY_");
    }

    #[test]
    fn all_env_var_names_include_message_bus_section() {
        let names = ServerConfig::all_env_var_names();
        assert!(
            names.iter().any(|n| n.starts_with("IGGY_MESSAGE_BUS_")),
            "expected at least one IGGY_MESSAGE_BUS_* env var, got: {names:?}"
        );
    }
}
