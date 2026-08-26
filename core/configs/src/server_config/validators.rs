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

//! [`Validatable`] for [`ServerConfig`].
//!
//! Delegates section by section, including
//! [`super::message_bus::MessageBusConfig::validate`], then applies the
//! cross-section invariants: JWT gating when HTTP is enabled and
//! server-default expiry sanity.

use super::COMPONENT;
use super::cluster::STATE_CHUNK_HEADER_LEN;
use super::partition::{CONCURRENT_SERVED_SEGMENTS, SEGMENT_SIZE_OVERSHOOT_BYTES};
use super::server::ServerConfig;
use crate::ConfigurationError;
use crate::common::http::HMAC_JWT_ALGORITHMS;
use crate::common::validators::SEGMENT_MAX_SIZE_BYTES;
use err_trail::ErrContext;
use iggy_common::{IggyExpiry, MAX_MESSAGE_SIZE_UPPER_BYTES, Validatable};

/// compio-ws (tungstenite 0.29) `write_buffer_size` default. Used to
/// evaluate the `max_write_buffer_size > write_buffer_size` invariant
/// when the operator leaves `write_buffer_size` unset; keep in sync
/// with the defaults documented in the shipped config.toml.
const WS_DEFAULT_WRITE_BUFFER_SIZE: u64 = 128 * 1024;

impl Validatable<ConfigurationError> for ServerConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        self.system
            .memory_pool
            .validate()
            .error(|e: &ConfigurationError| {
                format!("{COMPONENT} (error: {e}) - failed to validate memory pool config")
            })?;
        self.data_maintenance
            .validate()
            .error(|e: &ConfigurationError| {
                format!("{COMPONENT} (error: {e}) - failed to validate data maintenance config")
            })?;
        self.personal_access_token
            .validate()
            .error(|e: &ConfigurationError| {
                format!(
                    "{COMPONENT} (error: {e}) - failed to validate personal access token config"
                )
            })?;
        self.system
            .segment
            .validate()
            .error(|e: &ConfigurationError| {
                format!("{COMPONENT} (error: {e}) - failed to validate segment config")
            })?;
        self.telemetry.validate().error(|e: &ConfigurationError| {
            format!("{COMPONENT} (error: {e}) - failed to validate telemetry config")
        })?;
        self.system
            .sharding
            .validate()
            .error(|e: &ConfigurationError| {
                format!("{COMPONENT} (error: {e}) - failed to validate sharding config")
            })?;
        self.cluster.validate().error(|e: &ConfigurationError| {
            format!("{COMPONENT} (error: {e}) - failed to validate cluster config")
        })?;
        self.metadata.validate().error(|e: &ConfigurationError| {
            format!("{COMPONENT} (error: {e}) - failed to validate metadata config")
        })?;
        self.partition.validate().error(|e: &ConfigurationError| {
            format!("{COMPONENT} (error: {e}) - failed to validate partition config")
        })?;
        self.system
            .logging
            .validate()
            .error(|e: &ConfigurationError| {
                format!("{COMPONENT} (error: {e}) - failed to validate logging config")
            })?;

        if self.http.enabled
            && let IggyExpiry::ServerDefault = self.http.jwt.access_token_expiry
        {
            eprintln!("http.jwt.access_token_expiry cannot be ServerDefault when HTTP is enabled");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        // The signing key is always HMAC (built from a shared secret; there is
        // no PEM loading path), and jsonwebtoken refuses a header algorithm
        // whose family does not match the key. An asymmetric algorithm builds
        // the manager fine and then fails every login at token generation, so
        // the server would serve HTTP that nobody can authenticate against.
        if self.http.enabled && self.http.jwt.get_algorithm().is_err() {
            let supported = HMAC_JWT_ALGORITHMS.map(|(name, _)| name).join(", ");
            eprintln!(
                "http.jwt.algorithm '{}' is not supported: the server signs with a shared \
                 secret, so only {supported} can be used. This field governs self-issued \
                 tokens only; [[http.jwt.trusted_issuers]] is unaffected, because an issuer's \
                 token is verified with the algorithm named in its own header and the matching \
                 JWKS key, never with this field",
                self.http.jwt.algorithm
            );
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

        // Validate the bus knobs BEFORE any cross-section bound derived from
        // them: `max_message_size` has a frozen ceiling in there, and an
        // operator who raised it past the ceiling must meet that error first
        // -- not the artifact-floor error below, which would send them off
        // to raise `transfer_artifact_bytes_max` and only then learn the
        // edit was impossible.
        self.message_bus
            .validate()
            .error(|e: &ConfigurationError| {
                format!("{COMPONENT} (error: {e}) - failed to validate message_bus config")
            })?;

        // The HTTP produce path builds its bus message in-process, so the
        // framing decoder's `max_message_size` cap never runs on it: the
        // request body is the only bound on the widest batch record that
        // path can persist. A produce request carries at most one batch and
        // base64 leaves ~25% slack, so bounding the body above the frozen
        // recovery ceiling would let a legally admitted, checksum-valid
        // batch be refused as implausible by boot-time segment recovery.
        if self.http.enabled
            && self.http.max_request_size.as_bytes_u64() > MAX_MESSAGE_SIZE_UPPER_BYTES
        {
            eprintln!(
                "{COMPONENT} http.max_request_size ({}) exceeds the frozen ceiling of \
                 {MAX_MESSAGE_SIZE_UPPER_BYTES} bytes: the HTTP produce path is not framed by \
                 the message bus, so its body size is what bounds the widest persistable batch \
                 record, and records above the ceiling are refused as implausible by boot-time \
                 segment recovery",
                self.http.max_request_size.as_bytes_u64()
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        // A received segment artifact can be one whole batch larger than the
        // segment cap (rotation checks the cap AFTER appending), and the real
        // batch bound is the BUS frame cap -- the server never enforces
        // `MAX_PAYLOAD_SIZE`. An artifact ceiling under that floor refuses a
        // legal segment, and the manifest check is all-or-nothing, so the
        // partition livelocks re-requesting the same segment from every peer at
        // the backoff ceiling. Caught here so it is a boot error rather than one
        // partition that silently never rejoins.
        // Segment size is per topic now, so the floor is the LARGEST segment
        // any topic may legally be created with, not a configured value.
        let artifact_floor =
            SEGMENT_MAX_SIZE_BYTES.saturating_add(self.message_bus.max_message_size.as_bytes_u64());
        if self.partition.transfer_artifact_bytes_max.as_bytes_u64() < artifact_floor {
            eprintln!(
                "{COMPONENT} partition.transfer_artifact_bytes_max ({} B) must be at least the \
                 largest legal segment ({SEGMENT_MAX_SIZE_BYTES} B) + \
                 message_bus.max_message_size ({} B) = {artifact_floor} B: a segment may close \
                 one whole batch past its cap, and an artifact ceiling below that refuses a \
                 legal segment and livelocks the partition's rejoin",
                self.partition.transfer_artifact_bytes_max.as_bytes_u64(),
                self.message_bus.max_message_size.as_bytes_u64(),
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        // Raising the artifact ceiling (or the segment size) without raising the
        // served-payload budget silently serialises rejoins: a whole-node rejoin
        // then walks its partitions one at a time. Warn rather than reject,
        // because running under the budget costs re-reads, not failures, and an
        // operator deliberately trading serving concurrency for memory still
        // boots.
        let (resident_len, served_slots) = served_transfer_slots(self);
        if served_slots < CONCURRENT_SERVED_SEGMENTS {
            let served_cache = self
                .partition
                .transfer_served_cache_bytes_max
                .as_bytes_u64();
            eprintln!(
                "{COMPONENT} partition.transfer_served_cache_bytes_max ({served_cache} B) holds \
                 only {served_slots} state-transfer slot(s) of {resident_len} B (the larger of \
                 partition.transfer_artifact_bytes_max and the built-in segment ceiling of \
                 {SEGMENT_MAX_SIZE_BYTES} B + {SEGMENT_SIZE_OVERSHOOT_BYTES} B of overshoot), \
                 below the {CONCURRENT_SERVED_SEGMENTS} this shard is sized to serve at once, so \
                 rejoins serialise and every cache miss re-reads and re-hashes a whole segment to \
                 serve one chunk. The segment ceiling is a compile-time constant, so the only \
                 knobs here are these two: raise transfer_served_cache_bytes_max to {} B, or \
                 lower transfer_artifact_bytes_max",
                resident_len.saturating_mul(CONCURRENT_SERVED_SEGMENTS)
            );
        }

        // Repair frames ride the bounded per-peer message-bus queue. A repair
        // round of cluster.repair_chunk_max frames that meets or overruns
        // message_bus.peer_queue_capacity drops its own tail silently, wedging
        // the repair loop into slow retries. Keep the chunk strictly below the
        // queue; this also floors peer_queue_capacity, which is otherwise only
        // checked for > 0.
        if self.cluster.repair_chunk_max >= self.message_bus.peer_queue_capacity {
            eprintln!(
                "{COMPONENT} cluster.repair_chunk_max ({}) must be < message_bus.peer_queue_capacity ({}): repair frames ride the per-peer bus queue, so a chunk that fills or overruns it drops frames and wedges repair",
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
                "{COMPONENT} message_bus.max_message_size ({bus_cap}) must exceed the {STATE_CHUNK_HEADER_LEN}-byte state-chunk header: state transfer serves artifact chunks over this bus, and a frame above the cap is rejected by the receiving transport, which tears down the whole replica connection"
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
                "{COMPONENT} websocket.max_frame_size ({}) exceeds websocket.max_message_size ({})",
                frame.as_bytes_u64(),
                message.as_bytes_u64()
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if let Some(message) = self.websocket.max_message_size
            && message.as_bytes_u64() > bus_max_message_size
        {
            eprintln!(
                "{COMPONENT} websocket.max_message_size ({}) exceeds message_bus.max_message_size ({})",
                message.as_bytes_u64(),
                bus_max_message_size
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if let Some(frame) = self.websocket.max_frame_size
            && frame.as_bytes_u64() > bus_max_message_size
        {
            eprintln!(
                "{COMPONENT} websocket.max_frame_size ({}) exceeds message_bus.max_message_size ({})",
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
                    "{COMPONENT} websocket.{key} must be non-zero (\"0\", \"unlimited\" and \"none\" all parse to zero)"
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
                    "{COMPONENT} websocket.max_write_buffer_size ({}) must exceed websocket.write_buffer_size ({write_buffer_size})",
                    max_write.as_bytes_u64()
                );
                return Err(ConfigurationError::InvalidConfigurationValue);
            }
        }

        self.quic.validate().error(|e: &ConfigurationError| {
            format!("{COMPONENT} (error: {e}) - failed to validate quic config")
        })?;

        // Both knobs below sit on shared section structs, so the rejects live
        // here rather than in those types' own `Validatable` impls. `0` /
        // `disabled` / `unlimited` all parse to the same zero duration.
        if self
            .consumer_group
            .rebalancing_timeout
            .get_duration()
            .is_zero()
        {
            eprintln!(
                "{COMPONENT} consumer_group.rebalancing_timeout must be nonzero: it is the deadline after which a pending revocation completes without the source client committing what it was served, so zero force-transfers every partition on the next reconciler tick and reopens the duplicate-delivery window"
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.heartbeat.enabled && self.heartbeat.interval.get_duration().is_zero() {
            eprintln!(
                "{COMPONENT} heartbeat.interval must be nonzero when heartbeat.enabled: it sizes both the verifier's sleep and the staleness window, so zero spins the verifier and reaps every live session on its first pass"
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        reject_unsupported(self)?;

        Ok(())
    }
}

/// `(resident_len, slots)`: the size a SEALED segment actually reaches, and how
/// many of them the served-payload budget holds at once.
///
/// Reproduces what `IggyShard::partition_transfer_admission_cap` computes at the
/// shipped defaults, which is what really decides how many rejoins one shard
/// serves concurrently. The shard divides by the CONFIGURED segment size plus
/// overshoot; this divides by the compile-time segment ceiling, and the two
/// coincide only because bootstrap pins the deployed segment size to
/// `DEFAULT_SEGMENT_SIZE`. Where they diverge this side is the conservative one,
/// so the warning it drives can over-fire but never under-fire.
///
/// The divisor is the sealed size rather than the configured target because
/// rotation checks the cap after appending. Segment size is per topic now, so
/// the sealed bound is the LARGEST segment any topic may legally declare, not a
/// configured value. The `max` floor is never zero, so the division always
/// holds, and the quotient carries the shard's own one-slot clamp so the warning
/// never claims a slot count the runtime would not admit.
fn served_transfer_slots(config: &ServerConfig) -> (u64, u64) {
    let resident_len = config
        .partition
        .transfer_artifact_bytes_max
        .as_bytes_u64()
        .max(SEGMENT_MAX_SIZE_BYTES.saturating_add(SEGMENT_SIZE_OVERSHOOT_BYTES));
    let slots = (config
        .partition
        .transfer_served_cache_bytes_max
        .as_bytes_u64()
        / resident_len)
        .max(1);
    (resident_len, slots)
}

/// The server parses the whole config surface but does not yet honor every
/// knob. Make the still-inert ones loud at boot rather than silently ignored.
/// All are off by default, so only a deliberate opt-in trips this.
fn reject_unsupported(config: &ServerConfig) -> Result<(), ConfigurationError> {
    if config.system.segment.archive_expired {
        eprintln!("system.segment.archive_expired is not supported");
        return Err(ConfigurationError::InvalidConfigurationValue);
    }
    if config.system.recovery.recreate_missing_state {
        eprintln!("system.recovery.recreate_missing_state is not supported");
        return Err(ConfigurationError::InvalidConfigurationValue);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::cluster::{ClusterNodeConfig, TransportPorts};
    use super::*;
    use figment::Figment;
    use figment::providers::{Format, Toml};

    const DEFAULT_CONFIG: &str = include_str!("../../../server/config.toml");

    /// Deep-merge a partial override over the shipped default, mirroring the
    /// file-over-embedded layering the runtime loader performs.
    fn config_with_override(override_toml: &str) -> ServerConfig {
        Figment::new()
            .merge(Toml::string(DEFAULT_CONFIG))
            .merge(Toml::string(override_toml))
            .extract()
            .expect("config deserializes")
    }

    /// The per-topic `segment_size` ceiling lives in `iggy_common`, which cannot
    /// import this crate. Admission reads it from there; boot validation reads
    /// the constant here. This is the only place both are visible.
    #[test]
    fn given_segment_maximum_when_compared_to_the_option_ceiling_should_match() {
        assert_eq!(
            SEGMENT_MAX_SIZE_BYTES,
            iggy_common::MAX_TOPIC_SEGMENT_SIZE,
            "the option ceiling and the segment maximum must move together"
        );
    }

    /// Admission may cap a topic's `segment_size` at
    /// [`iggy_common::MAX_TOPIC_SEGMENT_SIZE`] flat only while boot refuses any
    /// config whose artifact budget cannot carry a segment that large plus one
    /// bus frame. Without that refusal the ceiling would have to be per node.
    #[test]
    fn given_artifact_budget_below_the_segment_ceiling_when_validating_should_reject() {
        let config = config_with_override(
            "[partition]\ntransfer_artifact_bytes_max = \"1 GiB\"\n[message_bus]\nmax_message_size = \"1 MiB\"\n",
        );
        assert!(
            config.validate().is_err(),
            "an artifact budget under segment maximum + one frame must refuse boot"
        );
    }

    #[test]
    fn given_shipped_default_config_when_validating_should_pass() {
        let config: ServerConfig = Figment::new()
            .merge(Toml::string(DEFAULT_CONFIG))
            .extract()
            .expect("default config deserializes");
        config.validate().expect("pristine config must validate");
    }

    #[test]
    fn given_web_ui_enabled_when_validating_should_pass() {
        let config = config_with_override("[http]\nweb_ui = true\n");
        config
            .validate()
            .expect("web_ui is served by the server and must validate");
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
    fn given_asymmetric_jwt_algorithm_when_validating_should_reject() {
        // Boots clean today and then fails every login: the key is HMAC and
        // jsonwebtoken refuses a mismatched algorithm family at sign time.
        for algorithm in ["RS256", "RS384", "RS512", "ES256", "not-an-algorithm"] {
            let config =
                config_with_override(&format!("[http.jwt]\nalgorithm = \"{algorithm}\"\n"));
            assert!(
                config.validate().is_err(),
                "{algorithm} has no key material and must not boot"
            );
        }
    }

    #[test]
    fn given_hmac_jwt_algorithm_when_validating_should_pass() {
        for (algorithm, _) in HMAC_JWT_ALGORITHMS {
            let config =
                config_with_override(&format!("[http.jwt]\nalgorithm = \"{algorithm}\"\n"));
            config
                .validate()
                .unwrap_or_else(|_| panic!("{algorithm} is signable and must validate"));
        }
    }

    #[test]
    fn given_asymmetric_jwt_algorithm_when_http_disabled_should_pass() {
        // No listener means no token is ever issued, so a stale algorithm is
        // inert rather than a broken deployment.
        let config =
            config_with_override("[http]\nenabled = false\n\n[http.jwt]\nalgorithm = \"RS256\"\n");
        config
            .validate()
            .expect("a disabled HTTP listener never signs a token");
    }

    // The shipped budget is exactly two artifact ceilings, so the pristine
    // config sits on the boundary and a single MiB past it starves a slot.
    #[test]
    fn given_artifact_ceiling_starving_transfer_slots_when_validating_should_warn_only() {
        let config =
            config_with_override("[partition]\ntransfer_artifact_bytes_max = \"1089 MiB\"\n");
        assert_eq!(served_transfer_slots(&config).1, 1);
        config
            .validate()
            .expect("a starved served cache warns but still boots");
    }

    #[test]
    fn given_artifact_ceiling_at_transfer_slot_boundary_when_sizing_should_keep_full_slot_count() {
        let config = config_with_override("");
        assert_eq!(
            served_transfer_slots(&config).1,
            CONCURRENT_SERVED_SEGMENTS,
            "the shipped defaults must serve the concurrency they are sized for"
        );
    }

    #[test]
    fn given_served_cache_raised_with_artifact_ceiling_when_validating_should_keep_transfer_slots()
    {
        let config = config_with_override(
            "[partition]\ntransfer_artifact_bytes_max = \"2 GiB\"\ntransfer_served_cache_bytes_max = \"4 GiB\"\n",
        );
        assert_eq!(served_transfer_slots(&config).1, CONCURRENT_SERVED_SEGMENTS);
        config.validate().expect("budget raised in step must boot");
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

    // The HTTP produce path is not bus-framed, so its body cap is the only
    // bound on the widest record that path can persist.
    #[test]
    fn given_http_max_request_size_above_recovery_ceiling_when_validating_should_reject() {
        let config = config_with_override("[http]\nmax_request_size = \"257 MiB\"\n");
        assert!(config.validate().is_err());
    }

    #[test]
    fn given_http_max_request_size_at_recovery_ceiling_when_validating_should_pass() {
        let config = config_with_override("[http]\nmax_request_size = \"256 MiB\"\n");
        assert!(config.validate().is_ok());
    }

    #[test]
    fn given_http_disabled_when_max_request_size_above_ceiling_should_pass() {
        let config =
            config_with_override("[http]\nenabled = false\nmax_request_size = \"257 MiB\"\n");
        assert!(config.validate().is_ok());
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
        let result: Result<ServerConfig, _> = Figment::new()
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

    // http.enabled needs a non-ServerDefault JWT expiry to clear the sibling
    // check above; ServerConfig::default() already satisfies that.
    fn https_config(cert_file: &str, key_file: &str) -> ServerConfig {
        let mut cfg = ServerConfig::default();
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
            advertised_addresses: Vec::new(),
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

    fn clustered_http_config(nodes: Vec<ClusterNodeConfig>) -> ServerConfig {
        let mut cfg = ServerConfig::default();
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
