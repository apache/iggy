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
use super::external_auth::ExternalAuthConfig;
use super::geo::GeoConfig;
use super::message_bus::MessageBusConfig;
use super::metadata::MetadataConfig;
use super::node::NodeConfig;
use super::partition::PartitionConfig;
use super::quic::QuicConfig;
use super::sharding::ShardingConfig;
use super::tcp::TcpConfig;
use super::tiered_storage::TieredStorageConfig;
use super::websocket::WebSocketConfig;
use crate::ConfigurationError;
use crate::common::http::HttpConfig;
use crate::common::system::{EncryptionConfig, LoggingConfig, RuntimeConfig};
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

/// Vars used by sibling binaries (iggy CLI) or test/CI-only. These suppress
/// unknown-name warnings but are not advertised by the server.
const SERVER_SCAN_ONLY_ENV_VARS: &[&str] = &[
    "IGGY_TEST_VERBOSE",
    "IGGY_TEST_CLUSTER_NODES",
    "IGGY_TEST_CLEANUP_DISABLED",
    "IGGY_CI_BUILD",
    "IGGY_HOME",
    "IGGY_USERNAME",
    "IGGY_PASSWORD",
    "IGGY_TOKEN",
];

/// Non-config env vars supported by the server and advertised to operators.
const SERVER_RUNTIME_ENV_VARS: &[&str] = &[
    "IGGY_CONFIG_PATH",
    "IGGY_ENV_PATH",
    crate::configs_impl::DISPLAY_CONFIG_ENV,
    "IGGY_ROOT_USERNAME",
    "IGGY_ROOT_PASSWORD",
    "IGGY_SHARD_RUNTIME_CAPACITY",
    "IGGY_SHARD_EVENT_INTERVAL",
];

/// Non-config vars advertised by `iggy-server --list-config-env-vars`.
pub fn server_runtime_env_vars() -> impl Iterator<Item = &'static str> {
    SERVER_RUNTIME_ENV_VARS.iter().copied()
}

/// All non-config vars accepted by the server's unknown-name scan.
pub fn server_process_env_vars() -> impl Iterator<Item = &'static str> {
    SERVER_RUNTIME_ENV_VARS
        .iter()
        .chain(SERVER_SCAN_ONLY_ENV_VARS)
        .copied()
}

pub(crate) const SERVER_ALLOWED_ENV_PREFIXES: &[&str] =
    &["IGGY_CONNECTORS_", "IGGY_KAFKA_", "IGGY_MCP_", "IGGY_TEST_"];

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
    // Refusing the old key keeps an operator's timeout from changing without
    // notice.
    RelocatedKey {
        path: "consumer_group.session_timeout",
        replacement: RelocatedTarget::Superseded(
            "the session timeout is now 1.2 x heartbeat.interval",
        ),
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
    #[serde(default)]
    pub external_auth: ExternalAuthConfig,
    #[serde(default)]
    pub tiered_storage: TieredStorageConfig,
    #[serde(default)]
    pub geo: GeoConfig,
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
                .chain(server_process_env_vars())
                .collect(),
        )
        .with_allowed_env_prefixes(SERVER_ALLOWED_ENV_PREFIXES)
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
            // `ServerConfig::config_provider` checks every `IGGY_` name before
            // this provider runs.
            provider: TypedEnvProvider::from_config(ServerConfig::ENV_PREFIX)
                .without_unknown_env_var_check(),
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
    use super::super::geo::GeoPeerConfig;
    use super::super::tiered_storage::{GcsAuth, S3UrlStyle, TieredStorageBackend};
    use super::*;
    use figment::Figment;
    use figment::providers::Serialized;
    use iggy_common::IggyByteSize;
    use std::time::Duration;

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
        assert_eq!(cfg.message_bus.peer_queue_capacity, 4096);
        assert_eq!(cfg.message_bus.connections_max, None);
        assert_eq!(ServerConfig::default().message_bus.connections_max, None);
    }

    #[test]
    #[serial_test::serial]
    fn given_zero_connections_max_env_var_when_loading_should_disable_the_cap() {
        const NAME: &str = "IGGY_MESSAGE_BUS_CONNECTIONS_MAX";
        assert!(ServerConfig::all_env_var_names().contains(&NAME));
        // SAFETY: `serial_test::serial` keeps other tests off the environment.
        unsafe { env::set_var(NAME, "0") };

        let cfg: Result<ServerConfig, _> = Figment::new()
            .merge(Toml::string(include_str!("../../../server/config.toml")))
            .merge(ServerConfigEnvProvider::default())
            .extract();

        // SAFETY: paired with the set above.
        unsafe { env::remove_var(NAME) };
        let cfg = cfg.expect("config with the env override deserializes");
        assert_eq!(cfg.message_bus.connections_max, Some(0));
    }

    #[test]
    #[serial_test::serial]
    fn given_change_log_capacity_env_var_when_loading_should_override_the_default() {
        const NAME: &str = "IGGY_METADATA_CHANGE_LOG_ACTIVITY_CAPACITY";
        assert!(ServerConfig::all_env_var_names().contains(&NAME));
        // SAFETY: `serial_test::serial` keeps other tests off the environment.
        unsafe { env::set_var(NAME, "4096") };

        let cfg: Result<ServerConfig, _> = Figment::new()
            .merge(Toml::string(include_str!("../../../server/config.toml")))
            .merge(ServerConfigEnvProvider::default())
            .extract();

        // SAFETY: paired with the set above.
        unsafe { env::remove_var(NAME) };
        let cfg = cfg.expect("config with the env override deserializes");
        assert_eq!(cfg.metadata.change_log_activity_capacity, 4096);
    }

    /// The config file documents `retention = "0"` as turning deletion by age
    /// off, so the same digits from the environment have to load the same way.
    #[test]
    #[serial_test::serial]
    fn given_a_bare_zero_for_a_duration_env_var_when_loading_should_read_zero() {
        let cfg = load_with_env_vars(&[("IGGY_LOGGING_RETENTION", "0")])
            .expect("config with a bare 0 duration from the environment deserializes");

        assert!(cfg.logging.retention.is_zero());
    }

    #[test]
    #[serial_test::serial]
    fn given_a_bare_byte_count_for_a_size_env_var_when_loading_should_read_bytes() {
        let cfg = load_with_env_vars(&[("IGGY_WEBSOCKET_READ_BUFFER_SIZE", "131072")])
            .expect("config with a bare byte count from the environment deserializes");

        assert_eq!(
            cfg.websocket.read_buffer_size,
            Some(IggyByteSize::from(131_072_u64))
        );
    }

    /// The environment provider hands every all-digit value over as a number,
    /// so a setting that parses the same digits from text has to take the
    /// number too. Settings that take any text are plain strings and exempt.
    #[test]
    #[serial_test::serial]
    fn given_digits_from_env_for_a_setting_parsing_them_as_text_when_loading_should_accept_them() {
        const ANY_TEXT: &str = "any text";
        let file = Figment::new().merge(Toml::string(include_str!("../../../server/config.toml")));
        let leaves = ServerConfig::env_templates()
            .iter()
            .filter(|template| template.max_elements.is_empty());

        let mut refused = Vec::new();
        for template in leaves {
            for digits in ["0", "1"] {
                if load_with_env_vars(&[(template.env_name, digits)]).is_err()
                    && loads_as_text(&file, template.config_path, digits)
                    && !loads_as_text(&file, template.config_path, ANY_TEXT)
                {
                    refused.push(format!("{}={digits}", template.env_name));
                }
            }
        }

        assert!(
            refused.is_empty(),
            "these settings parse the digits as text but refuse them from the environment: {refused:?}"
        );
    }

    #[test]
    fn given_a_config_file_without_the_partition_skip_key_when_loading_should_place_partitions_on_every_shard()
     {
        const KEY_LINE: &str = "skip_shard_zero_for_partitions = false\n";
        let shipped = include_str!("../../../server/config.toml");
        assert!(shipped.contains(KEY_LINE), "the shipped file sets the key");

        let cfg: ServerConfig = Figment::new()
            .merge(Toml::string(&shipped.replace(KEY_LINE, "")))
            .extract()
            .expect("TOML without the key deserializes");

        assert!(!cfg.cluster.coordinator.skip_shard_zero_for_partitions);
        assert!(
            !ServerConfig::default()
                .cluster
                .coordinator
                .skip_shard_zero_for_partitions
        );
    }

    #[test]
    #[serial_test::serial]
    fn given_partition_skip_env_var_when_loading_should_override_the_file() {
        const NAME: &str = "IGGY_CLUSTER_COORDINATOR_SKIP_SHARD_ZERO_FOR_PARTITIONS";
        let cfg = load_with_env_vars(&[(NAME, "true")])
            .expect("config with the env override deserializes");

        assert!(cfg.cluster.coordinator.skip_shard_zero_for_partitions);
    }

    /// The shipped file, like every file written before tiered storage, has
    /// no `[tiered_storage]` table.
    #[test]
    fn given_a_config_file_without_tiered_storage_when_loading_should_turn_tiering_off() {
        let cfg: ServerConfig = Figment::new()
            .merge(Toml::string(include_str!("../../../server/config.toml")))
            .extract()
            .expect("embedded TOML deserializes");

        assert_eq!(cfg.tiered_storage.backend, TieredStorageBackend::None);
        cfg.validate()
            .expect("a config without tiered storage validates");
    }

    #[test]
    #[serial_test::serial]
    fn given_tiered_storage_env_vars_when_loading_should_set_the_section() {
        let vars = [
            ("IGGY_TIERED_STORAGE_BACKEND", "s3"),
            ("IGGY_TIERED_STORAGE_BUCKET", "segments"),
            ("IGGY_TIERED_STORAGE_REGION", "eu-central-1"),
            ("IGGY_TIERED_STORAGE_ACCESS_KEY_ID", "AKIDEXAMPLE"),
            ("IGGY_TIERED_STORAGE_SECRET_ACCESS_KEY", "wJalrXUtnFEMI"),
            ("IGGY_TIERED_STORAGE_PROGRESS_TIMEOUT", "1 m"),
            ("IGGY_TIERED_STORAGE_JOBS_UPLOADS", "5"),
            ("IGGY_TIERED_STORAGE_JOBS_UPLOAD_BYTES", "4 GiB"),
            ("IGGY_TIERED_STORAGE_JOBS_FRESH_UPLOADS", "8"),
            ("IGGY_TIERED_STORAGE_FRESH_UPLOAD_INTERVAL", "25 ms"),
            ("IGGY_TIERED_STORAGE_LONG_TERM_BACKLOG_LIMIT", "512 MiB"),
            ("IGGY_TIERED_STORAGE_FRESH_PENDING_BYTES", "96 MiB"),
            (
                "IGGY_TIERED_STORAGE_FRESH_PARTITION_PENDING_BYTES",
                "24 MiB",
            ),
        ];
        let known = ServerConfig::all_env_var_names();
        for (name, value) in vars {
            assert!(known.contains(&name), "{name}");
            // SAFETY: `serial_test::serial` keeps other tests off the environment.
            unsafe { env::set_var(name, value) };
        }

        let cfg: Result<ServerConfig, _> = Figment::new()
            .merge(Toml::string(include_str!("../../../server/config.toml")))
            .merge(ServerConfigEnvProvider::default())
            .extract();

        for (name, _) in vars {
            // SAFETY: paired with the set above.
            unsafe { env::remove_var(name) };
        }
        let cfg = cfg.expect("config with the env overrides deserializes");
        let tiered = &cfg.tiered_storage;
        assert_eq!(tiered.backend, TieredStorageBackend::S3);
        assert_eq!(tiered.bucket, "segments");
        assert_eq!(tiered.region, "eu-central-1");
        assert_eq!(tiered.access_key_id, "AKIDEXAMPLE");
        assert_eq!(tiered.secret_access_key, "wJalrXUtnFEMI");
        assert_eq!(tiered.progress_timeout.get_duration().as_secs(), 60);
        assert_eq!(tiered.jobs.uploads, 5);
        assert_eq!(tiered.jobs.fresh_uploads, 8);
        assert_eq!(tiered.fresh_upload_interval.get_duration().as_millis(), 25);
        assert_eq!(
            tiered.long_term_backlog_limit.as_bytes_u64(),
            512 * 1024 * 1024
        );
        assert_eq!(tiered.fresh_pending_bytes.as_bytes_u64(), 96 * 1024 * 1024);
        assert_eq!(
            tiered.fresh_partition_pending_bytes.as_bytes_u64(),
            24 * 1024 * 1024
        );
        assert_eq!(
            tiered.jobs.upload_bytes.as_bytes_u64(),
            4 * 1024 * 1024 * 1024
        );
        cfg.validate()
            .expect("an s3 section from the environment validates");
        assert!(!cfg.to_string().contains("wJalrXUtnFEMI"));
    }

    #[test]
    fn given_fresh_pending_bytes_in_the_file_when_loading_should_take_them() {
        let load = |shard: &str, partition: &str| -> ServerConfig {
            Figment::new()
                .merge(Toml::string(include_str!("../../../server/config.toml")))
                .merge(Toml::string(&format!(
                    "[tiered_storage]\nbackend = \"fs\"\npath = \"/var/lib/iggy-tiered\"\n\
                     fresh_pending_bytes = \"{shard}\"\n\
                     fresh_partition_pending_bytes = \"{partition}\"\n"
                )))
                .extract()
                .expect("TOML with fresh pending bytes deserializes")
        };

        let tuned = load("64 MiB", "32 MiB");
        let share_above_cap = load("16 MiB", "32 MiB");

        let tiered = &tuned.tiered_storage;
        assert_eq!(tiered.fresh_pending_bytes.as_bytes_u64(), 64 * 1024 * 1024);
        assert_eq!(
            tiered.fresh_partition_pending_bytes.as_bytes_u64(),
            32 * 1024 * 1024
        );
        tuned
            .validate()
            .expect("a share of 32 MiB under a cap of 64 MiB validates");
        assert!(share_above_cap.validate().is_err());
    }

    #[test]
    #[serial_test::serial]
    fn given_mock_s3_env_vars_when_loading_should_set_the_backend_and_its_latency() {
        let vars = [
            ("IGGY_TIERED_STORAGE_BACKEND", "mock_s3"),
            ("IGGY_TIERED_STORAGE_MOCK_S3_PUT_LATENCY", "30 ms"),
            ("IGGY_TIERED_STORAGE_MOCK_S3_GET_LATENCY", "15 ms"),
            ("IGGY_TIERED_STORAGE_MOCK_S3_OTHER_LATENCY", "5 ms"),
            ("IGGY_TIERED_STORAGE_MOCK_S3_BYTES_PER_SECOND", "100 MiB"),
        ];
        let known = ServerConfig::all_env_var_names();
        for (name, value) in vars {
            assert!(known.contains(&name), "{name}");
            // SAFETY: `serial_test::serial` keeps other tests off the environment.
            unsafe { env::set_var(name, value) };
        }

        let cfg: Result<ServerConfig, _> = Figment::new()
            .merge(Toml::string(include_str!("../../../server/config.toml")))
            .merge(ServerConfigEnvProvider::default())
            .extract();

        for (name, _) in vars {
            // SAFETY: paired with the set above.
            unsafe { env::remove_var(name) };
        }
        let cfg = cfg.expect("config with the env overrides deserializes");
        let tiered = &cfg.tiered_storage;
        assert_eq!(tiered.backend, TieredStorageBackend::MockS3);
        assert_eq!(tiered.mock_s3.put_latency.get_duration().as_millis(), 30);
        assert_eq!(tiered.mock_s3.get_latency.get_duration().as_millis(), 15);
        assert_eq!(tiered.mock_s3.other_latency.get_duration().as_millis(), 5);
        assert_eq!(
            tiered.mock_s3.bytes_per_second.as_bytes_u64(),
            100 * 1024 * 1024
        );
        cfg.validate()
            .expect("a mock_s3 section from the environment validates");
    }

    #[test]
    #[serial_test::serial]
    fn given_gcs_env_vars_when_loading_should_set_the_section_and_a_peer() {
        let vars = [
            ("IGGY_TIERED_STORAGE_BACKEND", "gcs"),
            ("IGGY_TIERED_STORAGE_GCS_BUCKET", "segments"),
            ("IGGY_TIERED_STORAGE_GCS_ENDPOINT", "http://localhost:4443"),
            ("IGGY_TIERED_STORAGE_GCS_AUTH", "none"),
            (
                "IGGY_TIERED_STORAGE_GCS_SERVICE_ACCOUNT_KEY",
                "/etc/iggy/key.json",
            ),
            ("IGGY_GEO_ORIGIN", "dc-a"),
            ("IGGY_GEO_PEERS_0_ORIGIN", "dc-b"),
            ("IGGY_GEO_PEERS_0_BACKEND", "gcs"),
            ("IGGY_GEO_PEERS_0_GCS_BUCKET", "iggy-dc-b"),
        ];
        let known = ServerConfig::all_env_var_names();
        for (name, value) in vars {
            assert!(known.contains(&name), "{name}");
            // SAFETY: `serial_test::serial` keeps other tests off the environment.
            unsafe { env::set_var(name, value) };
        }

        let cfg: Result<ServerConfig, _> = Figment::new()
            .merge(Toml::string(include_str!("../../../server/config.toml")))
            .merge(ServerConfigEnvProvider::default())
            .extract();

        for (name, _) in vars {
            // SAFETY: paired with the set above.
            unsafe { env::remove_var(name) };
        }
        let cfg = cfg.expect("config with the env overrides deserializes");
        let gcs = &cfg.tiered_storage.gcs;
        assert_eq!(cfg.tiered_storage.backend, TieredStorageBackend::Gcs);
        assert_eq!(gcs.bucket, "segments");
        assert_eq!(gcs.endpoint, "http://localhost:4443");
        assert_eq!(gcs.auth, GcsAuth::None);
        assert_eq!(gcs.service_account_key, "/etc/iggy/key.json");
        let [peer] = cfg.geo.peers.as_slice() else {
            panic!("one peer: {}", cfg.geo);
        };
        assert_eq!(peer.backend, TieredStorageBackend::Gcs);
        assert_eq!(peer.gcs.bucket, "iggy-dc-b");
        assert_eq!(peer.gcs.auth, GcsAuth::Auto);
        cfg.validate()
            .expect("a gcs section from the environment validates");
    }

    /// Every file written before geo topics has no `[geo]` table.
    #[test]
    fn given_a_config_file_without_geo_when_loading_should_have_no_origin_or_peers() {
        let cfg: ServerConfig = Figment::new()
            .merge(Toml::string(include_str!("../../../server/config.toml")))
            .extract()
            .expect("embedded TOML deserializes");

        assert!(cfg.geo.origin.is_empty());
        assert!(cfg.geo.peers.is_empty());
        assert_eq!(cfg.geo.tombstone_grace.get_duration().as_secs(), 24 * 3600);
        assert_eq!(
            cfg.geo.long_term_rewrite_max_interval.get_duration(),
            Duration::from_secs(10)
        );
        cfg.validate().expect("a config without geo validates");
    }

    #[test]
    fn given_a_long_term_rewrite_max_interval_in_the_file_when_loading_should_take_it() {
        let load = |value: &str| -> ServerConfig {
            Figment::new()
                .merge(Toml::string(include_str!("../../../server/config.toml")))
                .merge(Toml::string(&format!(
                    "[geo]\nlong_term_rewrite_max_interval = \"{value}\"\n"
                )))
                .extract()
                .expect("TOML with a rewrite ceiling deserializes")
        };

        let tuned = load("2 s");
        let too_short = load("500 ms");

        assert_eq!(
            tuned.geo.long_term_rewrite_max_interval.get_duration(),
            Duration::from_secs(2)
        );
        tuned.validate().expect("a ceiling of 2 s validates");
        assert!(too_short.validate().is_err());
    }

    #[test]
    #[serial_test::serial]
    fn given_geo_env_vars_when_loading_should_set_the_origin_and_a_peer() {
        let vars = [
            ("IGGY_GEO_ORIGIN", "dc-a"),
            ("IGGY_GEO_TOMBSTONE_GRACE", "2 h"),
            ("IGGY_GEO_LONG_TERM_REWRITE_MAX_INTERVAL", "3 s"),
            ("IGGY_GEO_PEERS_0_ORIGIN", "dc-b"),
            ("IGGY_GEO_PEERS_0_BACKEND", "s3"),
            ("IGGY_GEO_PEERS_0_BUCKET", "iggy-dc-b"),
            ("IGGY_GEO_PEERS_0_REGION", "us-east-1"),
            ("IGGY_GEO_PEERS_0_URL_STYLE", "path"),
            ("IGGY_GEO_PEERS_0_ACCESS_KEY_ID", "AKIDPEEREXAMPLE"),
            ("IGGY_GEO_PEERS_0_SECRET_ACCESS_KEY", "peerSecretKeyExample"),
        ];
        let known = ServerConfig::all_env_var_names();
        for (name, value) in vars {
            assert!(known.contains(&name), "{name}");
            // SAFETY: `serial_test::serial` keeps other tests off the environment.
            unsafe { env::set_var(name, value) };
        }

        let cfg: Result<ServerConfig, _> = Figment::new()
            .merge(Toml::string(include_str!("../../../server/config.toml")))
            .merge(ServerConfigEnvProvider::default())
            .extract();

        for (name, _) in vars {
            // SAFETY: paired with the set above.
            unsafe { env::remove_var(name) };
        }
        let cfg = cfg.expect("config with the env overrides deserializes");
        assert_eq!(cfg.geo.origin, "dc-a");
        assert_eq!(cfg.geo.tombstone_grace.get_duration().as_secs(), 2 * 3600);
        assert_eq!(
            cfg.geo.long_term_rewrite_max_interval.get_duration(),
            Duration::from_secs(3)
        );
        let [peer] = cfg.geo.peers.as_slice() else {
            panic!("one peer: {}", cfg.geo);
        };
        assert_eq!(peer.origin, "dc-b");
        assert_eq!(peer.backend, TieredStorageBackend::S3);
        assert_eq!(peer.bucket, "iggy-dc-b");
        assert_eq!(peer.region, "us-east-1");
        assert_eq!(peer.prefix, "iggy", "the default prefix");
        assert_eq!(peer.url_style, S3UrlStyle::Path);
        assert_eq!(peer.access_key_id, "AKIDPEEREXAMPLE");
        assert_eq!(peer.secret_access_key, "peerSecretKeyExample");
        cfg.validate()
            .expect("a geo section from the environment validates");
        assert!(!cfg.to_string().contains("peerSecretKeyExample"));
        assert!(
            !known.contains(&"IGGY_GEO_PEERS_16_ORIGIN"),
            "env overrides stop at geo::MAX_PEERS"
        );
    }

    #[test]
    fn given_peers_and_bad_tuning_without_a_backend_when_validating_should_refuse() {
        let mut cfg = ServerConfig::default();
        cfg.tiered_storage.max_attempts = 0;
        assert!(cfg.validate().is_ok(), "no peer takes the tuning");

        cfg.geo.origin = "dc-a".to_owned();
        cfg.geo.peers.push(GeoPeerConfig {
            origin: "dc-b".to_owned(),
            backend: TieredStorageBackend::Fs,
            path: "/mnt/dc-b".to_owned(),
            ..GeoPeerConfig::default()
        });

        assert!(cfg.validate().is_err());
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

    #[test]
    #[serial_test::serial]
    fn env_provider_accepts_server_process_env_vars() {
        for name in server_process_env_vars() {
            // SAFETY: the race is process-wide, not per key: `set_var` is unsound
            // against any concurrent environment access. `serial_test::serial` on
            // this test is what prevents that.
            unsafe { env::set_var(name, "1") };
        }

        let data = ServerConfigEnvProvider::default().data();

        for name in server_process_env_vars() {
            // SAFETY: paired with the set above.
            unsafe { env::remove_var(name) };
        }

        // The provider holds no scan of its own, so the typed provider's
        // debug_assert! stays quiet. A panic above is one failure this test
        // guards, and one of these names reaching the map is the other.
        let data = data.expect("the server env provider must accept every process variable");
        let profile = data.get(&Profile::default()).expect("no default profile");
        assert!(
            profile.is_empty(),
            "none of these variables is a config value, so none of them may reach the map: {profile:?}"
        );
    }

    #[test]
    fn server_process_env_vars_include_every_scan_only_name() {
        let process_names = server_process_env_vars().collect::<std::collections::HashSet<_>>();
        for name in [
            "IGGY_TEST_VERBOSE",
            "IGGY_TEST_CLUSTER_NODES",
            "IGGY_TEST_CLEANUP_DISABLED",
            "IGGY_CI_BUILD",
            "IGGY_HOME",
            "IGGY_USERNAME",
            "IGGY_PASSWORD",
        ] {
            assert!(
                process_names.contains(name),
                "missing scan-only name {name}"
            );
        }
        assert!(
            SERVER_RUNTIME_ENV_VARS
                .iter()
                .all(|name| !SERVER_SCAN_ONLY_ENV_VARS.contains(name)),
            "advertised and scan-only env vars must stay disjoint"
        );
    }

    /// Loads the embedded config with `vars` set. Callers must be
    /// `serial_test::serial`.
    fn load_with_env_vars(vars: &[(&str, &str)]) -> Result<ServerConfig, Box<figment::Error>> {
        let known = ServerConfig::all_env_var_names();
        for (name, value) in vars {
            assert!(known.contains(name), "{name}");
            // SAFETY: `serial_test::serial` on every caller keeps other tests
            // off the environment.
            unsafe { env::set_var(name, value) };
        }

        let cfg = Figment::new()
            .merge(Toml::string(include_str!("../../../server/config.toml")))
            .merge(ServerConfigEnvProvider::default())
            .extract()
            .map_err(Box::new);

        for (name, _) in vars {
            // SAFETY: paired with the set above.
            unsafe { env::remove_var(name) };
        }
        cfg
    }

    /// Whether `file` deserializes with `text` written as a string at `path`,
    /// the form the environment cannot produce for digits.
    fn loads_as_text(file: &Figment, path: &str, text: &str) -> bool {
        file.clone()
            .merge(Serialized::default(path, text))
            .extract::<ServerConfig>()
            .is_ok()
    }
}
