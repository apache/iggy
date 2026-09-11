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
use super::node::NodeConfig;
use super::partition::PartitionConfig;
use super::quic::QuicConfig;
use super::sharding::ShardingConfig;
use super::tcp::TcpConfig;
use super::websocket::WebSocketConfig;
use crate::ConfigurationError;
use crate::common::http::HttpConfig;
use crate::common::system::{
    EncryptionConfig, INDEX_EXTENSION, LOG_EXTENSION, LoggingConfig, RuntimeConfig,
};
use configs::{
    ConfigEnv, ConfigEnvMappings, ConfigProvider, FileConfigProvider, RelocatedKey,
    RelocatedTarget, TypedEnvProvider,
};
use err_trail::ErrContext;
use figment::providers::{Format, Toml};
use figment::value::Dict;
use figment::{Metadata, Profile, Provider};
use iggy_common::Validatable;
use serde::{Deserialize, Serialize};
use server_common::bootstrap::SystemPaths;
use std::env;

pub use crate::common::server::{
    ConsumerGroupConfig, DataMaintenanceConfig, HeartbeatConfig, MemoryPoolConfig,
    MessagesMaintenanceConfig, PersonalAccessTokenCleanerConfig, PersonalAccessTokenConfig,
    TelemetryConfig, TelemetryLogsConfig, TelemetryTracesConfig, TelemetryTransport,
};

pub const SERVER_PROCESS_ENV_VARS: &[&str] = &[
    "IGGY_CONFIG_PATH",
    "IGGY_ENV_PATH",
    "IGGY_DISPLAY_CONFIG",
    "IGGY_ROOT_USERNAME",
    "IGGY_ROOT_PASSWORD",
    "IGGY_TEST_VERBOSE",
    "IGGY_TEST_CLUSTER_NODES",
    "IGGY_TEST_CLEANUP_DISABLED",
    "IGGY_SHARD_RUNTIME_CAPACITY",
    "IGGY_SHARD_EVENT_INTERVAL",
    "IGGY_CI_BUILD",
];

const DEFAULT_CONFIG_PATH: &str = "core/server/config.toml";

/// Server config keys that became per-topic options, or went away with the
/// feature they configured.
///
/// The provider refuses to boot while any of them is still set, in the config
/// file or in the environment. See [`RelocatedKey`] for why a warning is not
/// enough. The partition knobs matter most: they are create-only options now,
/// so a topic that boots without one can never be given it afterwards.
const RELOCATED_CONFIG_KEYS: &[RelocatedKey] = &[
    // Refuse obsolete layout overrides instead of silently reading another directory.
    RelocatedKey {
        path: "partition.path",
        replacement: RelocatedTarget::Removed,
    },
    RelocatedKey {
        path: "system.path",
        replacement: RelocatedTarget::MovedTo("path"),
    },
    RelocatedKey {
        path: "system.runtime",
        replacement: RelocatedTarget::MovedTo("runtime"),
    },
    RelocatedKey {
        path: "system.logging",
        replacement: RelocatedTarget::MovedTo("logging"),
    },
    RelocatedKey {
        path: "system.encryption",
        replacement: RelocatedTarget::MovedTo("encryption"),
    },
    RelocatedKey {
        path: "system.partition",
        replacement: RelocatedTarget::MovedTo("partition"),
    },
    RelocatedKey {
        path: "system.sharding",
        replacement: RelocatedTarget::MovedTo("sharding"),
    },
    RelocatedKey {
        path: "system.memory_pool",
        replacement: RelocatedTarget::MovedTo("memory_pool"),
    },
    RelocatedKey {
        path: "stream",
        replacement: RelocatedTarget::Removed,
    },
    RelocatedKey {
        path: "topic",
        replacement: RelocatedTarget::Removed,
    },
    // Reject the removed table and every former environment mapping beneath it.
    RelocatedKey {
        path: "system",
        replacement: RelocatedTarget::Removed,
    },
    RelocatedKey {
        path: "partition.consumer_offset_enforce_fsync",
        replacement: RelocatedTarget::TopicOption("consumer_offset_durability"),
    },
    // The whole table, not just its leaves. Caps are compile-time constants
    // enforced at admission, so a per-node value could only diverge from them.
    RelocatedKey {
        path: "extra",
        replacement: RelocatedTarget::Removed,
    },
];

/// Top-level on-disk config schema for the `iggy-server` binary.
///
/// Composes the shared section types from `crate::common` with the
/// transport, cluster, metadata and [`MessageBusConfig`] sections owned
/// by `super`.
#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
#[config_env(prefix = "IGGY_", name = "iggy-server-config")]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    pub consumer_group: ConsumerGroupConfig,
    pub data_maintenance: DataMaintenanceConfig,
    #[serde(default)]
    pub node: NodeConfig,
    #[serde(default)]
    pub personal_access_token: PersonalAccessTokenConfig,
    pub heartbeat: HeartbeatConfig,
    pub path: String,
    pub runtime: RuntimeConfig,
    pub logging: LoggingConfig,
    pub encryption: EncryptionConfig,
    pub memory_pool: MemoryPoolConfig,
    pub sharding: ShardingConfig,
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

/// One client-facing listener, as the client-facing address derivation and
/// boot validation see it: the config key naming its bind address, that
/// address as written, and whether the listener is switched on.
pub struct ClientListener<'a> {
    pub key: &'static str,
    pub address: &'a str,
    pub enabled: bool,
}

impl ServerConfig {
    /// The client-facing listeners, in the order the derived client-facing
    /// address prefers them. TCP leads: it is the binary protocol every SDK
    /// speaks, so it is the listener an address derived for clients should
    /// describe whenever it is running.
    #[must_use]
    pub fn client_listeners(&self) -> [ClientListener<'_>; 4] {
        [
            ClientListener {
                key: "tcp.address",
                address: &self.tcp.address,
                enabled: self.tcp.enabled,
            },
            ClientListener {
                key: "websocket.address",
                address: &self.websocket.address,
                enabled: self.websocket.enabled,
            },
            ClientListener {
                key: "quic.address",
                address: &self.quic.address,
                enabled: self.quic.enabled,
            },
            ClientListener {
                key: "http.address",
                address: &self.http.address,
                enabled: self.http.enabled,
            },
        ]
    }

    /// The listener whose bind address cluster metadata derives this node's
    /// client-facing address from when `node.advertised_address` is unset:
    /// the first enabled one. `None` when every client-facing listener is
    /// off, which leaves no address for a client to dial and nothing to
    /// publish.
    #[must_use]
    pub fn derived_address_listener(&self) -> Option<ClientListener<'_>> {
        self.client_listeners()
            .into_iter()
            .find(|listener| listener.enabled)
    }

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
        .with_relocated_keys(ServerConfig::ENV_PREFIX, RELOCATED_CONFIG_KEYS)
        .with_known_env_names(
            Self::all_env_var_names()
                .into_iter()
                .chain(SERVER_PROCESS_ENV_VARS.iter().copied())
                .collect(),
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

impl ServerConfig {
    pub fn get_system_path(&self) -> String {
        self.path.to_string()
    }

    pub fn get_state_path(&self) -> String {
        format!("{}/state", self.get_system_path())
    }

    pub fn get_state_messages_file_path(&self) -> String {
        format!("{}/log", self.get_state_path())
    }

    pub fn get_state_info_path(&self) -> String {
        format!("{}/info", self.get_state_path())
    }
    pub fn get_state_tokens_path(&self) -> String {
        format!("{}/tokens", self.get_state_path())
    }

    pub fn get_runtime_path(&self) -> String {
        format!("{}/{}", self.get_system_path(), self.runtime.path)
    }

    pub fn get_streams_path(&self) -> String {
        format!("{}/streams", self.get_system_path())
    }

    pub fn get_stream_path(&self, stream_id: usize) -> String {
        format!("{}/{}", self.get_streams_path(), stream_id)
    }

    pub fn get_topics_path(&self, stream_id: usize) -> String {
        format!("{}/topics", self.get_stream_path(stream_id))
    }

    pub fn get_topic_path(&self, stream_id: usize, topic_id: usize) -> String {
        format!("{}/{}", self.get_topics_path(stream_id), topic_id)
    }

    pub fn get_partitions_path(&self, stream_id: usize, topic_id: usize) -> String {
        format!("{}/partitions", self.get_topic_path(stream_id, topic_id))
    }

    pub fn get_partition_path(
        &self,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
    ) -> String {
        format!(
            "{}/{}",
            self.get_partitions_path(stream_id, topic_id),
            partition_id
        )
    }

    pub fn get_offsets_path(
        &self,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
    ) -> String {
        format!(
            "{}/offsets",
            self.get_partition_path(stream_id, topic_id, partition_id)
        )
    }

    pub fn get_consumer_offsets_path(
        &self,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
    ) -> String {
        format!(
            "{}/consumers",
            self.get_offsets_path(stream_id, topic_id, partition_id)
        )
    }

    pub fn get_consumer_group_offsets_path(
        &self,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
    ) -> String {
        format!(
            "{}/groups",
            self.get_offsets_path(stream_id, topic_id, partition_id)
        )
    }

    pub fn get_segment_path(
        &self,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
        start_offset: u64,
    ) -> String {
        format!(
            "{}/{:0>20}",
            self.get_partition_path(stream_id, topic_id, partition_id),
            start_offset
        )
    }

    pub fn get_messages_file_path(
        &self,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
        start_offset: u64,
    ) -> String {
        let path = self.get_segment_path(stream_id, topic_id, partition_id, start_offset);
        format!("{path}.{LOG_EXTENSION}")
    }

    pub fn get_index_path(
        &self,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
        start_offset: u64,
    ) -> String {
        let path = self.get_segment_path(stream_id, topic_id, partition_id, start_offset);
        format!("{path}.{INDEX_EXTENSION}")
    }
}

impl SystemPaths for ServerConfig {
    fn get_system_path(&self) -> String {
        ServerConfig::get_system_path(self)
    }

    fn get_state_path(&self) -> String {
        ServerConfig::get_state_path(self)
    }

    fn get_state_messages_file_path(&self) -> String {
        ServerConfig::get_state_messages_file_path(self)
    }

    fn get_streams_path(&self) -> String {
        ServerConfig::get_streams_path(self)
    }

    fn get_runtime_path(&self) -> String {
        ServerConfig::get_runtime_path(self)
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
    fn data_root_uses_fixed_stream_topic_and_partition_directories() {
        let config = ServerConfig {
            path: "/var/lib/iggy".to_owned(),
            ..ServerConfig::default()
        };
        assert_eq!(
            config.get_partition_path(1, 2, 3),
            "/var/lib/iggy/streams/1/topics/2/partitions/3"
        );
        assert!(!ServerConfig::all_env_var_names().contains(&"IGGY_PARTITION_PATH"));
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
