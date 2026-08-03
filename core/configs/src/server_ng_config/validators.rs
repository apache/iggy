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

//! [`Validatable`] for [`ServerNgConfig`].
//!
//! Mirrors the section-by-section delegation of
//! [`crate::validators`]'s `impl Validatable for ServerConfig`, plus a
//! call into [`super::message_bus::MessageBusConfig::validate`] for the
//! new section. The cross-section invariants (topic vs segment sizing,
//! JWT gating when HTTP is enabled, server-default expiry sanity) are
//! mirrored exactly so server-ng inherits the same boot-time safety
//! net.

use super::COMPONENT_NG;
use super::cluster::STATE_CHUNK_HEADER_LEN;
use super::server_ng::{ExtraConfig, NamespaceConfig, ServerNgConfig};
use crate::ConfigurationError;
use err_trail::ErrContext;
use iggy_common::{IggyExpiry, MaxTopicSize, Validatable};
use server_common::sharding::IggyNamespace;
use tracing::warn;

/// compio-ws (tungstenite 0.29) `write_buffer_size` default. Used to
/// evaluate the `max_write_buffer_size > write_buffer_size` invariant
/// when the operator leaves `write_buffer_size` unset; keep in sync
/// with the defaults documented in the shipped config.toml.
const WS_DEFAULT_WRITE_BUFFER_SIZE: u64 = 128 * 1024;

impl Validatable<ConfigurationError> for ServerNgConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        self.system
            .memory_pool
            .validate()
            .error(|e: &ConfigurationError| {
                format!("{COMPONENT_NG} (error: {e}) - failed to validate memory pool config")
            })?;
        self.data_maintenance
            .validate()
            .error(|e: &ConfigurationError| {
                format!("{COMPONENT_NG} (error: {e}) - failed to validate data maintenance config")
            })?;
        self.personal_access_token
            .validate()
            .error(|e: &ConfigurationError| {
                format!(
                    "{COMPONENT_NG} (error: {e}) - failed to validate personal access token config"
                )
            })?;
        self.extra.validate().error(|e: &ConfigurationError| {
            format!("{COMPONENT_NG} (error: {e}) - failed to validate extra config")
        })?;
        self.system
            .segment
            .validate()
            .error(|e: &ConfigurationError| {
                format!("{COMPONENT_NG} (error: {e}) - failed to validate segment config")
            })?;
        self.system
            .compression
            .validate()
            .error(|e: &ConfigurationError| {
                format!("{COMPONENT_NG} (error: {e}) - failed to validate compression config")
            })?;
        self.telemetry.validate().error(|e: &ConfigurationError| {
            format!("{COMPONENT_NG} (error: {e}) - failed to validate telemetry config")
        })?;
        self.system
            .sharding
            .validate()
            .error(|e: &ConfigurationError| {
                format!("{COMPONENT_NG} (error: {e}) - failed to validate sharding config")
            })?;
        self.cluster.validate().error(|e: &ConfigurationError| {
            format!("{COMPONENT_NG} (error: {e}) - failed to validate cluster config")
        })?;
        self.metadata.validate().error(|e: &ConfigurationError| {
            format!("{COMPONENT_NG} (error: {e}) - failed to validate metadata config")
        })?;
        self.partition.validate().error(|e: &ConfigurationError| {
            format!("{COMPONENT_NG} (error: {e}) - failed to validate partition config")
        })?;
        self.system
            .logging
            .validate()
            .error(|e: &ConfigurationError| {
                format!("{COMPONENT_NG} (error: {e}) - failed to validate logging config")
            })?;
        self.message_saver
            .validate()
            .error(|e: &ConfigurationError| {
                format!("{COMPONENT_NG} (error: {e}) - failed to validate message saver config")
            })?;

        let topic_size = match self.system.topic.max_size {
            MaxTopicSize::Custom(size) => Ok(size.as_bytes_u64()),
            MaxTopicSize::Unlimited => Ok(u64::MAX),
            MaxTopicSize::ServerDefault => {
                eprintln!("system.topic.max_size cannot be ServerDefault in server-ng config");
                Err(ConfigurationError::InvalidConfigurationValue)
            }
        }?;

        if let IggyExpiry::ServerDefault = self.system.topic.message_expiry {
            eprintln!("system.topic.message_expiry cannot be ServerDefault in server-ng config");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        // A zero duration encodes to wire value 0, the same value the wire uses
        // for ServerDefault, so it would silently collide with that sentinel.
        if let IggyExpiry::ExpireDuration(duration) = self.system.topic.message_expiry
            && duration.as_micros() == 0
        {
            eprintln!(
                "system.topic.message_expiry is a zero duration, which collides with the server-default sentinel on the wire; use \"none\" to never expire or a positive duration"
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        if self.http.enabled
            && let IggyExpiry::ServerDefault = self.http.jwt.access_token_expiry
        {
            eprintln!("http.jwt.access_token_expiry cannot be ServerDefault when HTTP is enabled");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        if self.http.enabled
            && self.http.tls.enabled
            && (self.http.tls.cert_file.is_empty() || self.http.tls.key_file.is_empty())
        {
            eprintln!(
                "http.tls.enabled=true requires non-empty http.tls.cert_file and http.tls.key_file"
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        // Cluster mode has no port fallbacks: the roster is the single source
        // of listener ports, so every enabled transport needs an explicit
        // per-node port. Falling back to the port of a transport's top-level
        // `address` would hand two same-host nodes the same socket and fail
        // only at bind time, and a portless node would silently degrade every
        // follower-to-primary HTTP forward through it to a fail-closed 503.
        if self.cluster.enabled {
            for node in &self.cluster.nodes {
                let required_ports = [
                    ("tcp", true, node.ports.tcp),
                    ("quic", self.quic.enabled, node.ports.quic),
                    ("http", self.http.enabled, node.ports.http),
                    ("websocket", self.websocket.enabled, node.ports.websocket),
                    ("tcp_replica", true, node.ports.tcp_replica),
                ];
                for (transport, enabled, port) in required_ports {
                    if enabled && port.is_none() {
                        eprintln!(
                            "cluster node '{}' has no ports.{transport}; cluster mode requires an explicit roster port for every enabled transport",
                            node.name
                        );
                        return Err(ConfigurationError::InvalidConfigurationValue);
                    }
                }
            }
        }

        if topic_size < self.system.segment.size.as_bytes_u64() {
            eprintln!(
                "system.topic.max_size ({} B) must be >= system.segment.size ({} B)",
                topic_size,
                self.system.segment.size.as_bytes_u64()
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        self.message_bus
            .validate()
            .error(|e: &ConfigurationError| {
                format!("{COMPONENT_NG} (error: {e}) - failed to validate message_bus config")
            })?;

        // Repair frames ride the bounded per-peer message-bus queue. A repair
        // round of cluster.repair_chunk_max frames that meets or overruns
        // message_bus.peer_queue_capacity drops its own tail silently, wedging
        // the repair loop into slow retries. Keep the chunk strictly below the
        // queue; this also floors peer_queue_capacity, which is otherwise only
        // checked for > 0.
        if self.cluster.repair_chunk_max >= self.message_bus.peer_queue_capacity {
            eprintln!(
                "{COMPONENT_NG} cluster.repair_chunk_max ({}) must be < message_bus.peer_queue_capacity ({}): repair frames ride the per-peer bus queue, so a chunk that fills or overruns it drops frames and wedges repair",
                self.cluster.repair_chunk_max, self.message_bus.peer_queue_capacity
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        // State-transfer chunks ride the same bus. A cap that cannot carry one
        // header plus a byte of payload makes every rejoin that needs a
        // transfer impossible, and the failure surfaces only as a replica
        // connection tearing down when the frame is rejected on the read side.
        let bus_cap = self.message_bus.max_message_size.as_bytes_u64();
        if bus_cap <= STATE_CHUNK_HEADER_LEN {
            eprintln!(
                "{COMPONENT_NG} message_bus.max_message_size ({bus_cap}) must exceed the {STATE_CHUNK_HEADER_LEN}-byte state-chunk header: state transfer serves artifact chunks over this bus, and a frame above the cap is rejected by the receiving transport, which tears down the whole replica connection"
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        // WS frame chain: websocket.max_frame_size <= websocket.max_message_size
        // <= message_bus.max_message_size. The bus's WS / WSS install path takes
        // its frame tuning from [websocket], so a WS ceiling above the bus's own
        // frame cap would admit messages the bus read-side validator then tears
        // the connection down over. An absent knob defers to the compio-ws
        // default (16 MiB frame / 64 MiB message), which satisfies the chain
        // against the shipped bus cap in practice.
        let bus_max_message_size = self.message_bus.max_message_size.as_bytes_u64();
        if let (Some(frame), Some(message)) = (
            self.websocket.max_frame_size,
            self.websocket.max_message_size,
        ) && frame.as_bytes_u64() > message.as_bytes_u64()
        {
            eprintln!(
                "{COMPONENT_NG} websocket.max_frame_size ({}) exceeds websocket.max_message_size ({})",
                frame.as_bytes_u64(),
                message.as_bytes_u64()
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if let Some(message) = self.websocket.max_message_size
            && message.as_bytes_u64() > bus_max_message_size
        {
            eprintln!(
                "{COMPONENT_NG} websocket.max_message_size ({}) exceeds message_bus.max_message_size ({})",
                message.as_bytes_u64(),
                bus_max_message_size
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if let Some(frame) = self.websocket.max_frame_size
            && frame.as_bytes_u64() > bus_max_message_size
        {
            eprintln!(
                "{COMPONENT_NG} websocket.max_frame_size ({}) exceeds message_bus.max_message_size ({})",
                frame.as_bytes_u64(),
                bus_max_message_size
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        // "0", "unlimited" and "none" all parse to a zero IggyByteSize. A
        // zero WS tunable is never usable: zero message or frame ceilings
        // reject every inbound frame, and zero buffers starve the
        // compio-ws pipeline. Reject at boot instead of shipping a
        // listener that cannot serve a single message.
        for (key, size) in [
            ("read_buffer_size", self.websocket.read_buffer_size),
            ("write_buffer_size", self.websocket.write_buffer_size),
            (
                "max_write_buffer_size",
                self.websocket.max_write_buffer_size,
            ),
            ("max_message_size", self.websocket.max_message_size),
            ("max_frame_size", self.websocket.max_frame_size),
        ] {
            if let Some(size) = size
                && size.as_bytes_u64() == 0
            {
                eprintln!(
                    "{COMPONENT_NG} websocket.{key} must be non-zero (\"0\", \"unlimited\" and \"none\" all parse to zero)"
                );
                return Err(ConfigurationError::InvalidConfigurationValue);
            }
        }

        // tungstenite asserts `max_write_buffer_size > write_buffer_size`
        // during connection setup, so a violating pair panics on every
        // accepted socket. Enforce the invariant at boot; an unset
        // write_buffer_size runs at the compio-ws default.
        if let Some(max_write) = self.websocket.max_write_buffer_size {
            let write_buffer_size = self
                .websocket
                .write_buffer_size
                .map_or(WS_DEFAULT_WRITE_BUFFER_SIZE, |size| size.as_bytes_u64());
            if max_write.as_bytes_u64() <= write_buffer_size {
                eprintln!(
                    "{COMPONENT_NG} websocket.max_write_buffer_size ({}) must exceed websocket.write_buffer_size ({write_buffer_size})",
                    max_write.as_bytes_u64()
                );
                return Err(ConfigurationError::InvalidConfigurationValue);
            }
        }

        self.quic.validate().error(|e: &ConfigurationError| {
            format!("{COMPONENT_NG} (error: {e}) - failed to validate quic config")
        })?;

        // Both knobs below are live in server-ng but sit on structs the legacy
        // server shares, so the rejects live here instead of in a `Validatable`
        // impl that would tighten legacy boots too. `0` / `disabled` /
        // `unlimited` all parse to the same zero duration.
        if self
            .consumer_group
            .rebalancing_timeout
            .get_duration()
            .is_zero()
        {
            eprintln!(
                "{COMPONENT_NG} consumer_group.rebalancing_timeout must be nonzero: it is the deadline after which a pending revocation completes without the source client committing what it was served, so zero force-transfers every partition on the next reconciler tick and reopens the duplicate-delivery window"
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.heartbeat.enabled && self.heartbeat.interval.get_duration().is_zero() {
            eprintln!(
                "{COMPONENT_NG} heartbeat.interval must be nonzero when heartbeat.enabled: it sizes both the verifier's sleep and the staleness window, so zero spins the verifier and reaps every live session on its first pass"
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        reject_unsupported_and_warn_inert(self)?;

        Ok(())
    }
}

/// server-ng parses the whole legacy config surface but does not yet honor
/// every knob. Make the still-inert ones loud at boot: reject the unsupported
/// features (all off by default, so only a deliberate opt-in trips this) and
/// warn once for tuning knobs server-ng silently ignores. Warnings fire only
/// when a knob deviates from its [`ServerNgConfig::default`] baseline, so a
/// pristine config.toml boots without noise. Baseline caveat: only the tcp/quic
/// fork sections take that default from the ng config.toml. The reused legacy
/// sections (`system.*`, `consumer_group.*`, `message_saver.*`) take theirs from
/// the legacy server config.toml; those match ng's shipped values today but are
/// not schema-locked, so editing such a knob in the ng config.toml could surface
/// a spurious warn. The guard test below pins the compared knobs against drift.
fn reject_unsupported_and_warn_inert(config: &ServerNgConfig) -> Result<(), ConfigurationError> {
    if config.system.message_deduplication.enabled {
        eprintln!("system.message_deduplication.enabled is not supported in server-ng");
        return Err(ConfigurationError::InvalidConfigurationValue);
    }
    if config.system.segment.archive_expired {
        eprintln!("system.segment.archive_expired is not supported in server-ng");
        return Err(ConfigurationError::InvalidConfigurationValue);
    }
    if config.system.recovery.recreate_missing_state {
        eprintln!("system.recovery.recreate_missing_state is not supported in server-ng");
        return Err(ConfigurationError::InvalidConfigurationValue);
    }

    let defaults = ServerNgConfig::default();

    if config.tcp.socket.override_defaults {
        warn!("tcp.socket tuning is set but not applied in server-ng");
    }
    if config.quic.socket.override_defaults {
        warn!("quic.socket tuning is set but not applied in server-ng");
    }
    if config.tcp.ipv6 {
        warn!(
            "tcp.ipv6 is ignored in server-ng; IPv4 vs IPv6 is decided by the tcp.address string"
        );
    }
    if config.tcp.socket_migration != defaults.tcp.socket_migration {
        warn!("tcp.socket_migration is not implemented in server-ng");
    }
    if config.system.partition.validate_checksum != defaults.system.partition.validate_checksum {
        warn!(
            "system.partition.validate_checksum is not applied in server-ng; nothing verifies checksums on load"
        );
    }
    if config.system.segment.cache_indexes != defaults.system.segment.cache_indexes {
        warn!("system.segment.cache_indexes is not applied in server-ng");
    }
    if config.system.logging.sysinfo_print_interval
        != defaults.system.logging.sysinfo_print_interval
    {
        warn!("system.logging.sysinfo_print_interval is not applied in server-ng");
    }
    if config.system.backup.path != defaults.system.backup.path
        || config.system.backup.compatibility.path != defaults.system.backup.compatibility.path
    {
        warn!("backup is not supported in server-ng");
    }
    // default_algorithm deviation is already warned by the delegated legacy
    // CompressionConfig::validate; only allow_override needs a signal here.
    if config.system.compression.allow_override != defaults.system.compression.allow_override {
        warn!(
            "system.compression.allow_override is inert in server-ng; live compression is per-topic from the request"
        );
    }
    if config.system.state.enforce_fsync != defaults.system.state.enforce_fsync
        || config.system.state.max_file_operation_retries
            != defaults.system.state.max_file_operation_retries
        || config.system.state.retry_delay != defaults.system.state.retry_delay
    {
        warn!(
            "system.state tuning (enforce_fsync, max_file_operation_retries, retry_delay) is not applied in server-ng"
        );
    }
    if config.consumer_group.rebalancing_check_interval
        != defaults.consumer_group.rebalancing_check_interval
    {
        warn!(
            "consumer_group.rebalancing_check_interval is not applied in server-ng; rebalancing cadence uses system.sharding.reconcile_periodic_interval"
        );
    }
    if config.message_saver.interval != defaults.message_saver.interval
        || config.message_saver.enforce_fsync != defaults.message_saver.enforce_fsync
    {
        warn!(
            "periodic message_saver is not implemented in server-ng; only shutdown-flush is active"
        );
    }

    Ok(())
}

impl Validatable<ConfigurationError> for ExtraConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        self.namespace.validate().error(|e: &ConfigurationError| {
            format!("{COMPONENT_NG} (error: {e}) - failed to validate namespace config")
        })?;
        Ok(())
    }
}

impl Validatable<ConfigurationError> for NamespaceConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        IggyNamespace::validate_capacity(self.max_streams, self.max_topics, self.max_partitions)
            .map_err(|error| {
                eprintln!("extra.namespace is invalid: {error}");
                ConfigurationError::InvalidConfigurationValue
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::cluster::{ClusterNodeConfig, TransportPorts};
    use super::*;
    use figment::Figment;
    use figment::providers::{Format, Toml};

    const DEFAULT_CONFIG: &str = include_str!("../../../server-ng/config.toml");

    /// Deep-merge a partial override over the shipped default, mirroring the
    /// file-over-embedded layering the runtime loader performs.
    fn config_with_override(override_toml: &str) -> ServerNgConfig {
        Figment::new()
            .merge(Toml::string(DEFAULT_CONFIG))
            .merge(Toml::string(override_toml))
            .extract()
            .expect("config deserializes")
    }

    #[test]
    fn given_shipped_default_config_when_validating_should_pass() {
        let config: ServerNgConfig = Figment::new()
            .merge(Toml::string(DEFAULT_CONFIG))
            .extract()
            .expect("default config deserializes");
        config
            .validate()
            .expect("pristine server-ng config must validate");
    }

    #[test]
    fn given_message_deduplication_enabled_when_validating_should_reject() {
        let config = config_with_override("[system.message_deduplication]\nenabled = true\n");
        assert!(config.validate().is_err());
    }

    #[test]
    fn given_web_ui_enabled_when_validating_should_pass() {
        let config = config_with_override("[http]\nweb_ui = true\n");
        config
            .validate()
            .expect("web_ui is now served by server-ng and must validate");
    }

    #[test]
    fn given_archive_expired_enabled_when_validating_should_reject() {
        let config = config_with_override("[system.segment]\narchive_expired = true\n");
        assert!(config.validate().is_err());
    }

    #[test]
    fn given_recreate_missing_state_enabled_when_validating_should_reject() {
        let config = config_with_override("[system.recovery]\nrecreate_missing_state = true\n");
        assert!(config.validate().is_err());
    }

    #[test]
    fn given_zero_message_expiry_when_validating_should_reject() {
        let config = config_with_override("[system.topic]\nmessage_expiry = \"0s\"\n");
        assert!(config.validate().is_err());
    }

    #[test]
    fn given_peer_queue_capacity_not_above_repair_chunk_max_when_validating_should_reject() {
        // The default repair_chunk_max (128) must stay strictly below
        // peer_queue_capacity; shrinking the queue to the chunk size is the
        // silent wedged-repair footgun this cross-section guard closes.
        let config = config_with_override("[message_bus]\npeer_queue_capacity = 128\n");
        assert!(config.validate().is_err());
    }

    #[test]
    fn given_repair_chunk_max_at_peer_queue_capacity_when_validating_should_reject() {
        let config = config_with_override("[cluster]\nrepair_chunk_max = 256\n");
        assert!(config.validate().is_err());
    }

    #[test]
    fn given_repair_chunk_max_below_peer_queue_capacity_when_validating_should_pass() {
        let config = config_with_override("[cluster]\nrepair_chunk_max = 255\n");
        config
            .validate()
            .expect("a chunk below the peer queue capacity must validate");
    }

    #[test]
    fn given_ws_frame_size_above_ws_message_size_when_validating_should_reject() {
        let config = config_with_override(
            "[websocket]\nmax_message_size = \"1 MiB\"\nmax_frame_size = \"2 MiB\"\n",
        );
        assert!(config.validate().is_err());
    }

    // The shipped bus cap is 64 MiB, so a 128 MiB WS ceiling breaks the chain.
    #[test]
    fn given_ws_message_size_above_bus_max_message_size_when_validating_should_reject() {
        let config = config_with_override("[websocket]\nmax_message_size = \"128 MiB\"\n");
        assert!(config.validate().is_err());
    }

    #[test]
    fn given_ws_frame_size_above_bus_max_message_size_when_validating_should_reject() {
        let config = config_with_override("[websocket]\nmax_frame_size = \"128 MiB\"\n");
        assert!(config.validate().is_err());
    }

    #[test]
    fn given_ws_frame_chain_in_ascending_order_when_validating_should_pass() {
        let config = config_with_override(
            "[websocket]\nmax_message_size = \"32 MiB\"\nmax_frame_size = \"16 MiB\"\n",
        );
        config
            .validate()
            .expect("frame <= message <= bus cap must validate");
    }

    // "unlimited" is not a supported sentinel for the WS size knobs: it
    // parses to zero, which as a cap would reject every message.
    #[test]
    fn given_zero_ws_size_when_validating_should_reject() {
        let config = config_with_override("[websocket]\nmax_message_size = \"unlimited\"\n");
        assert!(config.validate().is_err());
    }

    // tungstenite panics on this pair at connection setup; boot must
    // reject it first.
    #[test]
    fn given_max_write_buffer_at_write_buffer_when_validating_should_reject() {
        let config = config_with_override(
            "[websocket]\nwrite_buffer_size = \"256 KiB\"\nmax_write_buffer_size = \"256 KiB\"\n",
        );
        assert!(config.validate().is_err());
    }

    #[test]
    fn given_max_write_buffer_below_default_write_buffer_when_validating_should_reject() {
        let config = config_with_override("[websocket]\nmax_write_buffer_size = \"64 KiB\"\n");
        assert!(config.validate().is_err());
    }

    #[test]
    fn given_max_write_buffer_above_write_buffer_when_validating_should_pass() {
        let config = config_with_override(
            "[websocket]\nwrite_buffer_size = \"128 KiB\"\nmax_write_buffer_size = \"1 MiB\"\n",
        );
        config
            .validate()
            .expect("max write buffer above write buffer must validate");
    }

    // The size knobs are strictly typed; a malformed string must fail
    // deserialization at load rather than degrade to the compio-ws default.
    #[test]
    fn given_malformed_ws_size_string_when_deserializing_should_reject() {
        let result: Result<ServerNgConfig, _> = Figment::new()
            .merge(Toml::string(DEFAULT_CONFIG))
            .merge(Toml::string(
                "[websocket]\nmax_message_size = \"not-a-size\"\n",
            ))
            .extract();
        assert!(
            result.is_err(),
            "malformed websocket.max_message_size must fail config load"
        );
    }

    // The shipped config is single-node (cluster.enabled = false), where the
    // cross-section rule above is the only repair_chunk_max check that used to
    // run; its structural bounds have to hold there too.
    #[test]
    fn given_single_node_zero_repair_chunk_max_when_validating_should_reject() {
        let config = config_with_override("[cluster]\nrepair_chunk_max = 0\n");
        assert!(config.validate().is_err());
    }

    #[test]
    fn given_single_node_repair_chunk_max_above_ceiling_when_validating_should_reject() {
        // Queue widened past the chunk so the cross-section rule passes and
        // only the structural ceiling can reject.
        let config = config_with_override(
            "[cluster]\nrepair_chunk_max = 2000\n\n[message_bus]\npeer_queue_capacity = 4096\n",
        );
        assert!(config.validate().is_err());
    }

    #[test]
    fn given_zero_rebalancing_timeout_when_validating_should_reject() {
        let config = config_with_override("[consumer_group]\nrebalancing_timeout = \"0\"\n");
        assert!(config.validate().is_err());
    }

    #[test]
    fn given_disabled_rebalancing_timeout_when_validating_should_reject() {
        // "disabled" reads like an opt-out but parses to the same zero
        // duration, which force-transfers every revocation instead.
        let config = config_with_override("[consumer_group]\nrebalancing_timeout = \"disabled\"\n");
        assert!(config.validate().is_err());
    }

    #[test]
    fn given_zero_heartbeat_interval_when_heartbeat_enabled_should_reject() {
        let config = config_with_override("[heartbeat]\nenabled = true\ninterval = \"0\"\n");
        assert!(config.validate().is_err());
    }

    #[test]
    fn given_zero_heartbeat_interval_when_heartbeat_disabled_should_pass() {
        let config = config_with_override("[heartbeat]\nenabled = false\ninterval = \"0\"\n");
        config
            .validate()
            .expect("a disabled heartbeat never reads its interval");
    }

    /// The warn-helper baseline is [`ServerNgConfig::default`], but the reused
    /// legacy sections source that default from the legacy server config.toml,
    /// not this NG file. Pin the knobs the helper compares so any drift between
    /// the two config.toml files fails here instead of as a spurious boot warn.
    #[test]
    fn given_shipped_ng_config_when_compared_to_default_should_match_warned_knobs() {
        let shipped: ServerNgConfig = Figment::new()
            .merge(Toml::string(DEFAULT_CONFIG))
            .extract()
            .expect("default config deserializes");
        let defaults = ServerNgConfig::default();

        assert_eq!(shipped.tcp.socket_migration, defaults.tcp.socket_migration);
        assert_eq!(
            shipped.system.partition.validate_checksum,
            defaults.system.partition.validate_checksum
        );
        assert_eq!(
            shipped.system.segment.cache_indexes,
            defaults.system.segment.cache_indexes
        );
        assert_eq!(
            shipped.system.logging.sysinfo_print_interval,
            defaults.system.logging.sysinfo_print_interval
        );
        assert_eq!(shipped.system.backup.path, defaults.system.backup.path);
        assert_eq!(
            shipped.system.backup.compatibility.path,
            defaults.system.backup.compatibility.path
        );
        assert_eq!(
            shipped.system.compression.allow_override,
            defaults.system.compression.allow_override
        );
        assert_eq!(
            shipped.system.state.enforce_fsync,
            defaults.system.state.enforce_fsync
        );
        assert_eq!(
            shipped.system.state.max_file_operation_retries,
            defaults.system.state.max_file_operation_retries
        );
        assert_eq!(
            shipped.system.state.retry_delay,
            defaults.system.state.retry_delay
        );
        assert_eq!(
            shipped.consumer_group.rebalancing_check_interval,
            defaults.consumer_group.rebalancing_check_interval
        );
        assert_eq!(
            shipped.message_saver.interval,
            defaults.message_saver.interval
        );
        assert_eq!(
            shipped.message_saver.enforce_fsync,
            defaults.message_saver.enforce_fsync
        );
    }

    // http.enabled needs a non-ServerDefault JWT expiry to clear the sibling
    // check above; ServerNgConfig::default() already satisfies that.
    fn https_config(cert_file: &str, key_file: &str) -> ServerNgConfig {
        let mut cfg = ServerNgConfig::default();
        cfg.http.enabled = true;
        cfg.http.tls.enabled = true;
        cfg.http.tls.cert_file = cert_file.to_string();
        cfg.http.tls.key_file = key_file.to_string();
        cfg
    }

    #[test]
    fn validate_rejects_tls_enabled_with_empty_cert_file() {
        let cfg = https_config("", "key.pem");
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_accepts_tls_enabled_with_both_files_set() {
        let cfg = https_config("cert.pem", "key.pem");
        assert!(cfg.validate().is_ok());
    }

    fn cluster_node(replica_id: u8, http: Option<u16>) -> ClusterNodeConfig {
        ClusterNodeConfig {
            name: format!("node-{replica_id}"),
            ip: "127.0.0.1".to_string(),
            advertised_address: None,
            replica_id,
            ports: TransportPorts {
                tcp: Some(8090 + u16::from(replica_id)),
                quic: Some(8080 + u16::from(replica_id)),
                http,
                websocket: Some(8070 + u16::from(replica_id)),
                tcp_replica: Some(9090 + u16::from(replica_id)),
            },
        }
    }

    fn clustered_http_config(nodes: Vec<ClusterNodeConfig>) -> ServerNgConfig {
        let mut cfg = ServerNgConfig::default();
        cfg.http.enabled = true;
        cfg.cluster.enabled = true;
        cfg.cluster.name = "test-cluster".to_string();
        cfg.cluster.nodes = nodes;
        cfg
    }

    // Keyless cluster+http boots: forwarding degrades to off instead of
    // failing the whole server.
    #[test]
    fn validate_accepts_cluster_http_without_jwt_secret_or_cluster_auth() {
        let cfg = clustered_http_config(vec![
            cluster_node(0, Some(3000)),
            cluster_node(1, Some(3001)),
        ]);
        assert!(cfg.validate().is_ok());
    }

    // Cluster mode has no port fallbacks, so a portless roster node is
    // invalid even when forwarding is off (keyless).
    #[test]
    fn validate_rejects_keyless_cluster_http_with_portless_roster_node() {
        let cfg = clustered_http_config(vec![cluster_node(0, Some(3000)), cluster_node(1, None)]);
        assert!(cfg.validate().is_err());
    }

    // The explicit-port rule covers every enabled transport, not just http.
    #[test]
    fn validate_rejects_cluster_node_without_port_for_enabled_quic() {
        let mut cfg = clustered_http_config(vec![
            cluster_node(0, Some(3000)),
            cluster_node(1, Some(3001)),
        ]);
        cfg.quic.enabled = true;
        cfg.cluster.nodes[1].ports.quic = None;
        assert!(cfg.validate().is_err());
    }

    // A disabled transport never binds, so its roster port may stay unset.
    #[test]
    fn validate_accepts_cluster_node_without_port_for_disabled_quic() {
        let mut cfg = clustered_http_config(vec![
            cluster_node(0, Some(3000)),
            cluster_node(1, Some(3001)),
        ]);
        cfg.quic.enabled = false;
        cfg.cluster.nodes[1].ports.quic = None;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn validate_accepts_cluster_http_with_configured_jwt_secrets() {
        let mut cfg = clustered_http_config(vec![
            cluster_node(0, Some(3000)),
            cluster_node(1, Some(3001)),
        ]);
        cfg.http.jwt.encoding_secret = "0123456789abcdef0123456789abcdef".to_string();
        cfg.http.jwt.decoding_secret = "0123456789abcdef0123456789abcdef".to_string();
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn validate_accepts_cluster_http_with_cluster_auth_as_jwt_key_source() {
        let mut cfg = clustered_http_config(vec![
            cluster_node(0, Some(3000)),
            cluster_node(1, Some(3001)),
        ]);
        cfg.cluster.auth.enabled = true;
        cfg.cluster.auth.shared_secret = "0123456789abcdef0123456789abcdef".to_string();
        assert!(cfg.validate().is_ok());
    }
}
