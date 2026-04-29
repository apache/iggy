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

use super::COMPONENT_NG;
use super::message_bus::MessageBusConfig;
use crate::ConfigurationError;
use crate::server_config::cluster::ClusterConfig;
use crate::server_config::http::HttpConfig;
use crate::server_config::quic::QuicConfig;
use crate::server_config::server::{
    ConsumerGroupConfig, DataMaintenanceConfig, HeartbeatConfig, MessageSaverConfig,
    PersonalAccessTokenConfig, TelemetryConfig,
};
use crate::server_config::system::SystemConfig;
use crate::server_config::tcp::TcpConfig;
use crate::server_config::websocket::WebSocketConfig;
use configs::{ConfigEnv, ConfigEnvMappings, ConfigProvider, FileConfigProvider, TypedEnvProvider};
use err_trail::ErrContext;
use figment::providers::{Format, Toml};
use figment::value::Dict;
use figment::{Metadata, Profile, Provider};
use iggy_common::Validatable;
use serde::{Deserialize, Serialize};
use std::env;
use std::sync::Arc;

const DEFAULT_CONFIG_PATH: &str = "core/server-ng/config.toml";

/// Top-level on-disk config schema for the `server-ng` binary.
///
/// Mirrors the legacy [`crate::server::ServerConfig`] section surface
/// verbatim (operator-facing schema is unchanged) and adds a
/// [`MessageBusConfig`] section for inter-shard / inter-replica bus
/// tunables.
///
/// Section types are reused directly from the legacy server-config
/// modules; only [`MessageBusConfig`] and this composer are net-new
/// code at the time of introduction.
///
/// At the time of introduction this type is NOT consumed by
/// `core/server-ng`'s bootstrap. The wiring PR is a separate change.
#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
#[config_env(prefix = "IGGY_", name = "iggy-server-ng-config")]
pub struct ServerNgConfig {
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
    pub message_bus: MessageBusConfig,
}

impl ServerNgConfig {
    /// Load server-ng configuration from file and environment variables.
    ///
    /// Mirrors [`crate::server::ServerConfig::load`]: the path comes
    /// from `IGGY_CONFIG_PATH` or defaults to
    /// `core/server-ng/config.toml`; missing on-disk paths fall through
    /// to the embedded default TOML; env-var overrides flow through the
    /// [`ServerNgConfigEnvProvider`]; the result is validated before
    /// returning.
    ///
    /// # Errors
    /// Returns [`ConfigurationError`] when the config cannot be parsed
    /// from the configured source(s) or fails [`Validatable::validate`].
    pub async fn load() -> Result<ServerNgConfig, ConfigurationError> {
        let config_path =
            env::var("IGGY_CONFIG_PATH").unwrap_or_else(|_| DEFAULT_CONFIG_PATH.to_string());
        let provider = ServerNgConfig::config_provider(&config_path);
        let cfg: ServerNgConfig =
            provider
                .load_config()
                .await
                .error(|e: &configs::ConfigurationError| {
                    format!("{COMPONENT_NG} (error: {e}) - failed to load server-ng config")
                })?;
        cfg.validate().error(|e: &configs::ConfigurationError| {
            format!("{COMPONENT_NG} (error: {e}) - failed to validate server-ng config")
        })?;
        Ok(cfg)
    }

    /// Build the file-backed config provider with the embedded default
    /// TOML and the type-safe env-var provider attached.
    pub fn config_provider(config_path: &str) -> FileConfigProvider<ServerNgConfigEnvProvider> {
        let default_config = Toml::string(include_str!("../../../server-ng/config.toml"));
        FileConfigProvider::new(
            config_path.to_string(),
            ServerNgConfigEnvProvider::default(),
            true,
            Some(default_config),
        )
    }

    /// All recognised env var names for [`ServerNgConfig`].
    pub fn all_env_var_names() -> Vec<&'static str> {
        <ServerNgConfig as ConfigEnvMappings>::all_env_var_names()
    }
}

/// Type-safe environment provider for [`ServerNgConfig`].
///
/// Uses the [`ConfigEnvMappings`] trait generated by `#[derive(ConfigEnv)]`
/// to look up known env var names directly, eliminating path ambiguity.
#[derive(Debug, Clone)]
pub struct ServerNgConfigEnvProvider {
    provider: TypedEnvProvider<ServerNgConfig>,
}

impl Default for ServerNgConfigEnvProvider {
    fn default() -> Self {
        Self {
            provider: TypedEnvProvider::from_config(ServerNgConfig::ENV_PREFIX),
        }
    }
}

impl Provider for ServerNgConfigEnvProvider {
    fn metadata(&self) -> Metadata {
        Metadata::named(ServerNgConfig::ENV_PROVIDER_NAME)
    }

    fn data(&self) -> Result<figment::value::Map<Profile, Dict>, figment::Error> {
        self.provider.deserialize().map_err(|e| {
            figment::Error::from(format!(
                "Cannot deserialize environment variables for server-ng config: {e}"
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use figment::Figment;

    /// The embedded default TOML deserializes into a fully populated
    /// [`ServerNgConfig`] and passes validation. Exercises the
    /// `include_str!` resolution and the deserialization of every
    /// section without depending on an async runtime in `dev-deps`.
    #[test]
    fn embedded_default_toml_deserializes_and_validates() {
        let toml_str = include_str!("../../../server-ng/config.toml");
        let cfg: ServerNgConfig = Figment::new()
            .merge(Toml::string(toml_str))
            .extract()
            .expect("embedded TOML deserializes");
        cfg.validate().expect("embedded default validates");

        // Spot-check: defaults match the runtime crate's invariants.
        assert_eq!(cfg.message_bus.max_batch, 256);
        assert_eq!(cfg.message_bus.peer_queue_capacity, 256);
        assert_eq!(cfg.message_bus.keepalive_retries, 3);
        assert!(cfg.message_bus.tcp_tls_listen_addr.is_none());
        assert!(cfg.message_bus.wss_listen_addr.is_none());
    }

    #[test]
    fn default_impl_validates() {
        let cfg = ServerNgConfig::default();
        cfg.validate().expect("Default impl validates");
    }

    #[test]
    fn env_prefix_is_iggy() {
        assert_eq!(ServerNgConfig::ENV_PREFIX, "IGGY_");
    }

    #[test]
    fn all_env_var_names_include_message_bus_section() {
        let names = ServerNgConfig::all_env_var_names();
        assert!(
            names.iter().any(|n| n.starts_with("IGGY_MESSAGE_BUS_")),
            "expected at least one IGGY_MESSAGE_BUS_* env var, got: {names:?}"
        );
    }
}
