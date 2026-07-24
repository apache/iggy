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

//! Server-ng cluster schema.
//!
//! Field shape mirrors the legacy [`crate::cluster::ClusterConfig`]
//! plus the ng-only `heartbeat_timeout` knob; the type is forked into
//! `server_ng_config` so server-ng can evolve its cluster surface
//! (VSR consensus tunables) independently of the legacy server.

use super::defaults::SERVER_NG_CONFIG;
use crate::ConfigurationError;
use configs::ConfigEnv;
use iggy_common::{IggyDuration, Validatable};
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};
use std::time::Duration;

/// Absolute floor for the backup liveness window, independent of the
/// commit-broadcast rate. The primary signals liveness through its commit
/// broadcast (`commit_broadcast_interval`, 500ms by default); 2s spans several
/// broadcasts, so a single delayed one never elects. The per-config
/// `MIN_HEARTBEAT_TO_COMMIT_BROADCAST_RATIO` check scales the same headroom
/// when the broadcast interval is retuned.
pub const MIN_CLUSTER_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(2);

/// The backup liveness window (`heartbeat_timeout`) must span at least this
/// many commit broadcasts (`commit_broadcast_interval`), so one dropped or
/// delayed broadcast never trips a view change on a healthy primary.
const MIN_HEARTBEAT_TO_COMMIT_BROADCAST_RATIO: u32 = 4;

/// Length floor for the replica-auth PSK, in raw bytes. The 32-byte MAC key
/// is KDF-derived from these bytes at use-site, so any encoding clearing this
/// length is accepted.
const MIN_SHARED_SECRET_LEN: usize = 32;

/// serde fallback for configs written before the field existed; the value
/// itself lives in `core/server-ng/config.toml` like every other default.
fn default_heartbeat_timeout() -> IggyDuration {
    SERVER_NG_CONFIG.cluster.heartbeat_timeout.parse().unwrap()
}

/// serde fallback for configs written before the field existed; the value
/// itself lives in `core/server-ng/config.toml` like every other default.
fn default_commit_broadcast_interval() -> IggyDuration {
    SERVER_NG_CONFIG
        .cluster
        .commit_broadcast_interval
        .parse()
        .unwrap()
}

/// serde fallback for configs written before the field existed; the value
/// itself lives in `core/server-ng/config.toml` like every other default.
fn default_prepare_retransmit_interval() -> IggyDuration {
    SERVER_NG_CONFIG
        .cluster
        .prepare_retransmit_interval
        .parse()
        .unwrap()
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    pub enabled: bool,
    pub name: String,
    /// Backup-side liveness window for a plane's primary. A replica that
    /// sees no primary traffic for this long starts a view change
    /// (`normal_heartbeat_timeout`). Raise it on oversubscribed hosts where
    /// scheduling stalls fake primary death; sub-`MIN_CLUSTER_HEARTBEAT_TIMEOUT`
    /// values (including the `0` / `disabled` / `unlimited` sentinels, which
    /// all parse to zero) are rejected at boot.
    #[serde(default = "default_heartbeat_timeout")]
    #[serde_as(as = "DisplayFromStr")]
    #[config_env(leaf)]
    pub heartbeat_timeout: IggyDuration,
    /// How often the primary broadcasts its commit point to every backup, the
    /// cluster's primary-liveness signal. Each broadcast resets the backups'
    /// `heartbeat_timeout` window, so that window must span several broadcasts:
    /// boot rejects `heartbeat_timeout < MIN_HEARTBEAT_TO_COMMIT_BROADCAST_RATIO
    /// * commit_broadcast_interval`. Sizes the consensus `CommitMessage` timer.
    /// Zero (and the `0` / `disabled` / `unlimited` sentinels, which all parse
    /// to zero) is rejected at boot.
    #[serde(default = "default_commit_broadcast_interval")]
    #[serde_as(as = "DisplayFromStr")]
    #[config_env(leaf)]
    pub commit_broadcast_interval: IggyDuration,
    /// How often the primary retransmits prepares a backup has not yet acked.
    /// Lower recovers faster from a dropped prepare at the cost of replica
    /// traffic. Sizes the consensus `Prepare` timer. Zero (and the `0` /
    /// `disabled` / `unlimited` sentinels, which all parse to zero) is rejected
    /// at boot.
    #[serde(default = "default_prepare_retransmit_interval")]
    #[serde_as(as = "DisplayFromStr")]
    #[config_env(leaf)]
    pub prepare_retransmit_interval: IggyDuration,
    /// Full roster of cluster members. Intended to be byte-identical across
    /// every node so operators ship one config. The running node's identity
    /// is supplied out-of-band via the `--replica-id` CLI flag, which
    /// selects the entry in this list that describes the current node.
    #[serde(default)]
    pub nodes: Vec<ClusterNodeConfig>,
    /// Replica-to-replica authentication settings (PSK + BLAKE3 handshake).
    #[serde(default)]
    pub auth: ClusterAuthConfig,
    /// Replica-to-replica TLS settings for the consensus (`tcp_replica`) port.
    #[serde(default)]
    pub tls: ClusterTlsConfig,
}

/// Replica-to-replica authentication for the consensus (`tcp_replica`) port.
#[derive(Debug, Default, Deserialize, Serialize, Clone, ConfigEnv)]
#[serde(deny_unknown_fields)]
pub struct ClusterAuthConfig {
    /// When true, every replica peer must complete the authenticated handshake
    /// or be rejected, and [`Self::shared_secret`] is mandatory. When false
    /// (default) the replica handshake stays in legacy unauthenticated mode and
    /// `shared_secret` is not used for authentication. A configured non-empty
    /// `shared_secret` must still meet the 32-byte minimum whenever the cluster
    /// is enabled (a short value fails boot even with auth off).
    ///
    /// Enabling auth is a coordinated-restart change, and not the only one: the
    /// consensus `cluster_id` is derived from `ClusterConfig::name`
    /// unconditionally, so a mixed-version roster fails to connect regardless of
    /// this flag. Flip every node in one restart.
    #[serde(default)]
    pub enabled: bool,
    /// Cluster-wide pre-shared key for replica-to-replica authentication.
    ///
    /// At least 32 bytes of CSPRNG output, byte-identical across every node.
    /// Provisioned out-of-band, normally via `IGGY_CLUSTER_AUTH_SHARED_SECRET`
    /// rather than the on-disk config.
    // skip_serializing keeps the PSK out of the runtime `current_config.toml`
    // (and the `ServerConfig` diagnostic snapshot that cats it). The live
    // secret is read from env / on-disk config at boot, never from the
    // snapshot, so it must never be persisted there. Deserialize is retained.
    #[serde(default, skip_serializing)]
    #[config_env(secret)]
    pub shared_secret: String,
}

/// Replica-to-replica TLS for the consensus (`tcp_replica`) port.
///
/// Mirrors the legacy [`crate::tcp::TcpTlsConfig`] shape plus `ca_file`:
/// the replica plane DIALS its peers (a TLS client role the
/// client-facing server plane never has), so the dialer needs a trust
/// anchor to verify the acceptor's certificate against.
#[derive(Debug, Default, Deserialize, Serialize, Clone, ConfigEnv)]
#[serde(deny_unknown_fields)]
pub struct ClusterTlsConfig {
    /// When true every replica connection is wrapped in TLS (1.3 only)
    /// before the replica handshake runs. Requires `cluster.auth.enabled`:
    /// TLS carries no client certificates, so it authenticates the
    /// acceptor only; the PSK handshake authenticates the peer while TLS
    /// supplies confidentiality. Enabling is a coordinated-restart
    /// change: a TLS dialer cannot talk to a plaintext acceptor or vice
    /// versa. Flip every node in one restart.
    #[serde(default)]
    pub enabled: bool,
    /// When true the node auto-generates a self-signed certificate at
    /// boot and the dialer accepts ANY peer certificate. With the
    /// default `false`, `cert_file` / `key_file` / `ca_file` are all
    /// required.
    #[serde(default)]
    pub self_signed: bool,
    /// PEM certificate chain presented by this node's acceptor side.
    #[serde(default)]
    pub cert_file: String,
    /// PEM private key matching `cert_file`.
    #[serde(default)]
    pub key_file: String,
    /// PEM trust anchor(s) the dialer verifies peer certificates
    /// against. Unused when `self_signed` is true.
    #[serde(default)]
    pub ca_file: String,
}

#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct ClusterNodeConfig {
    pub name: String,
    pub ip: String,
    /// Numeric replica ID for VSR consensus (0-based).
    ///
    /// Must be unique across [`ClusterConfig::nodes`] and strictly less than
    /// `nodes.len()`. Validated by [`ClusterConfig::validate`].
    pub replica_id: u8,
    pub ports: TransportPorts,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default, ConfigEnv)]
pub struct TransportPorts {
    pub tcp: Option<u16>,
    pub quic: Option<u16>,
    pub http: Option<u16>,
    pub websocket: Option<u16>,
    /// Dedicated port for replica-to-replica consensus traffic.
    pub tcp_replica: Option<u16>,
}

impl Validatable<ConfigurationError> for ClusterConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        if !self.enabled {
            return Ok(());
        }

        if self.name.trim().is_empty() {
            eprintln!("Invalid cluster configuration: cluster name cannot be empty");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        // `0` / `disabled` / `unlimited` all parse to a zero duration and
        // land here too: there is no way to switch the liveness window off.
        if self.heartbeat_timeout.get_duration() < MIN_CLUSTER_HEARTBEAT_TIMEOUT {
            eprintln!(
                "Invalid cluster configuration: cluster.heartbeat_timeout '{}' must be at least {}s \
                 (the primary signals liveness through its commit broadcast; a shorter window \
                 elects on every scheduling hiccup)",
                self.heartbeat_timeout,
                MIN_CLUSTER_HEARTBEAT_TIMEOUT.as_secs()
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        // The commit broadcast is the cluster's liveness feed and the prepare
        // retransmit its recovery timer; both size consensus timers that have
        // to advance. `0` / `disabled` / `unlimited` all collapse to a zero
        // duration, which would stall the timer - reject them.
        if self.commit_broadcast_interval.get_duration().is_zero() {
            eprintln!(
                "Invalid cluster configuration: cluster.commit_broadcast_interval must be nonzero \
                 (it drives the primary's liveness broadcast)"
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.prepare_retransmit_interval.get_duration().is_zero() {
            eprintln!(
                "Invalid cluster configuration: cluster.prepare_retransmit_interval must be \
                 nonzero (it drives prepare retransmission)"
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        // The liveness window must span several commit broadcasts so a single
        // delayed broadcast never trips a view change on a healthy primary.
        let min_heartbeat = self
            .commit_broadcast_interval
            .get_duration()
            .saturating_mul(MIN_HEARTBEAT_TO_COMMIT_BROADCAST_RATIO);
        if self.heartbeat_timeout.get_duration() < min_heartbeat {
            eprintln!(
                "Invalid cluster configuration: cluster.heartbeat_timeout '{}' must be at least \
                 {}x cluster.commit_broadcast_interval '{}' so the liveness window spans several \
                 broadcasts",
                self.heartbeat_timeout,
                MIN_HEARTBEAT_TO_COMMIT_BROADCAST_RATIO,
                self.commit_broadcast_interval
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        if self.nodes.is_empty() {
            eprintln!(
                "Invalid cluster configuration: cluster.nodes must contain at least one entry when cluster is enabled"
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        // VSR needs every replica to have a stable, unique id strictly
        // less than the total replica count. Duplicate ids would split the
        // cluster into two replicas claiming the same slot; out-of-range
        // ids never win a primary election. Both are unrecoverable at
        // runtime - fail fast at startup.
        let total_replicas = u8::try_from(self.nodes.len()).map_err(|_| {
            eprintln!("Invalid cluster configuration: more than 255 replicas is unsupported");
            ConfigurationError::InvalidConfigurationValue
        })?;

        let mut seen_ids = std::collections::HashSet::new();
        let mut seen_names = std::collections::HashSet::new();
        let mut used_endpoints = std::collections::HashSet::new();

        for node in &self.nodes {
            if node.name.trim().is_empty() {
                eprintln!("Invalid cluster configuration: node name cannot be empty");
                return Err(ConfigurationError::InvalidConfigurationValue);
            }

            if node.ip.trim().is_empty() {
                eprintln!(
                    "Invalid cluster configuration: IP cannot be empty for node '{}'",
                    node.name
                );
                return Err(ConfigurationError::InvalidConfigurationValue);
            }

            if !seen_names.insert(node.name.clone()) {
                eprintln!(
                    "Invalid cluster configuration: duplicate node name '{}' found",
                    node.name
                );
                return Err(ConfigurationError::InvalidConfigurationValue);
            }

            if node.replica_id >= total_replicas {
                eprintln!(
                    "Invalid cluster configuration: replica_id {} for node '{}' must be < total replica count {total_replicas}",
                    node.replica_id, node.name
                );
                return Err(ConfigurationError::InvalidConfigurationValue);
            }

            if !seen_ids.insert(node.replica_id) {
                eprintln!(
                    "Invalid cluster configuration: duplicate replica_id {} (two nodes claim the same slot)",
                    node.replica_id
                );
                return Err(ConfigurationError::InvalidConfigurationValue);
            }

            let port_list = [
                ("TCP", node.ports.tcp),
                ("QUIC", node.ports.quic),
                ("HTTP", node.ports.http),
                ("WebSocket", node.ports.websocket),
                ("TCP_REPLICA", node.ports.tcp_replica),
            ];

            for (name, port_opt) in &port_list {
                if let Some(port) = port_opt {
                    if *port == 0 {
                        eprintln!(
                            "Invalid cluster configuration: {} port cannot be 0 for node '{}'",
                            name, node.name
                        );
                        return Err(ConfigurationError::InvalidConfigurationValue);
                    }

                    let endpoint = format!("{}:{}", node.ip, port);
                    if !used_endpoints.insert(endpoint.clone()) {
                        eprintln!(
                            "Invalid cluster configuration: port conflict - {endpoint} is already bound (node '{}', transport {name})",
                            node.name
                        );
                        return Err(ConfigurationError::InvalidConfigurationValue);
                    }
                }
            }
        }

        // Replica-auth PSK (only reached when the cluster is enabled; the early
        // return above skips these while it is disabled). When auth is enabled
        // the key is mandatory; any configured key must clear the length floor -
        // a typo guard that fires with auth off too, though only while the
        // cluster itself is enabled.
        let secret_len = self.auth.shared_secret.len();
        if self.auth.enabled && self.auth.shared_secret.is_empty() {
            eprintln!(
                "Invalid cluster configuration: cluster.auth.shared_secret must be set when cluster.auth.enabled is true"
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if !self.auth.shared_secret.is_empty() && secret_len < MIN_SHARED_SECRET_LEN {
            eprintln!(
                "Invalid cluster configuration: cluster.auth.shared_secret must be >= {MIN_SHARED_SECRET_LEN} bytes"
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        // Replica TLS. Both cert modes run one-directional TLS (no client
        // certificate anywhere), so TLS only authenticates the acceptor to
        // the dialer; peer authentication comes solely from the PSK
        // handshake. Without it any TLS-capable host could register as a
        // replica - require auth in both modes. CA mode (the default)
        // additionally needs all three PEM paths: cert/key for this node's
        // acceptor side, ca_file as the dialer's trust anchor.
        if self.tls.enabled {
            if !self.auth.enabled {
                eprintln!(
                    "Invalid cluster configuration: cluster.tls.enabled = true requires cluster.auth.enabled = true (TLS authenticates the acceptor only; the PSK handshake authenticates the peer)"
                );
                return Err(ConfigurationError::InvalidConfigurationValue);
            }
            if !self.tls.self_signed {
                for (field, value) in [
                    ("cert_file", &self.tls.cert_file),
                    ("key_file", &self.tls.key_file),
                    ("ca_file", &self.tls.ca_file),
                ] {
                    if value.trim().is_empty() {
                        eprintln!(
                            "Invalid cluster configuration: cluster.tls.{field} must be set when cluster.tls.enabled = true and self_signed = false"
                        );
                        return Err(ConfigurationError::InvalidConfigurationValue);
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_secret_is_never_serialized() {
        // Regression guard: the runtime current_config.toml (and the
        // ServerConfig diagnostic snapshot that cats it) are produced by
        // serializing this struct, so the PSK must not survive serialize.
        // skip_serializing is format-agnostic, so a JSON dump proves the toml
        // path too.
        let config = ClusterConfig {
            enabled: true,
            name: "iggy-cluster".to_owned(),
            heartbeat_timeout: default_heartbeat_timeout(),
            commit_broadcast_interval: default_commit_broadcast_interval(),
            prepare_retransmit_interval: default_prepare_retransmit_interval(),
            nodes: Vec::new(),
            auth: ClusterAuthConfig {
                enabled: true,
                shared_secret: "current-psk-MUST-NOT-be-persisted".to_owned(),
            },
            tls: ClusterTlsConfig::default(),
        };
        let serialized = serde_json::to_string(&config).expect("serialize cluster config");
        assert!(
            !serialized.contains("MUST-NOT-be-persisted"),
            "PSK leaked into serialized config: {serialized}"
        );
        assert!(
            !serialized.contains("shared_secret"),
            "shared_secret field present in serialized config: {serialized}"
        );
    }
}

#[cfg(test)]
mod cluster_validate_tests {
    use super::*;

    fn node(name: &str, id: u8) -> ClusterNodeConfig {
        ClusterNodeConfig {
            name: name.to_string(),
            ip: "127.0.0.1".to_string(),
            replica_id: id,
            ports: TransportPorts::default(),
        }
    }

    fn cfg(nodes: Vec<ClusterNodeConfig>) -> ClusterConfig {
        ClusterConfig {
            enabled: true,
            name: "iggy-cluster".to_string(),
            heartbeat_timeout: default_heartbeat_timeout(),
            commit_broadcast_interval: default_commit_broadcast_interval(),
            prepare_retransmit_interval: default_prepare_retransmit_interval(),
            nodes,
            auth: ClusterAuthConfig::default(),
            tls: ClusterTlsConfig::default(),
        }
    }

    #[test]
    fn validate_rejects_sub_minimum_heartbeat_timeout() {
        let mut c = cfg(vec![node("n1", 0), node("n2", 1)]);
        c.heartbeat_timeout = IggyDuration::new(Duration::from_millis(500));
        assert!(c.validate().is_err());
        // The "disabled" / "unlimited" sentinels collapse to zero and must
        // be rejected the same way.
        c.heartbeat_timeout = IggyDuration::new(Duration::ZERO);
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_zero_commit_broadcast_interval() {
        // `0` / `disabled` / `unlimited` all collapse to zero and stall the
        // liveness broadcast.
        let mut c = cfg(vec![node("n1", 0), node("n2", 1)]);
        c.commit_broadcast_interval = IggyDuration::new(Duration::ZERO);
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_zero_prepare_retransmit_interval() {
        let mut c = cfg(vec![node("n1", 0), node("n2", 1)]);
        c.prepare_retransmit_interval = IggyDuration::new(Duration::ZERO);
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_heartbeat_below_commit_broadcast_ratio() {
        // 3s clears the absolute 2s floor but is still < 4x the 1s broadcast,
        // so the ratio rule is what rejects here, not the floor.
        let mut c = cfg(vec![node("n1", 0), node("n2", 1)]);
        c.heartbeat_timeout = IggyDuration::new(Duration::from_secs(3));
        c.commit_broadcast_interval = IggyDuration::new(Duration::from_secs(1));
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_accepts_heartbeat_at_commit_broadcast_ratio() {
        // Exactly 4x the broadcast (and above the 2s floor) must pass.
        let mut c = cfg(vec![node("n1", 0), node("n2", 1)]);
        c.heartbeat_timeout = IggyDuration::new(Duration::from_secs(4));
        c.commit_broadcast_interval = IggyDuration::new(Duration::from_secs(1));
        assert!(c.validate().is_ok());
    }

    #[test]
    fn validate_rejects_empty_nodes() {
        let c = cfg(vec![]);
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_duplicate_replica_ids() {
        let c = cfg(vec![node("n1", 0), node("n2", 0)]);
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_duplicate_names() {
        let c = cfg(vec![node("n1", 0), node("n1", 1)]);
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_out_of_range_replica_id() {
        // 2 nodes total, so id 2 is out of range.
        let c = cfg(vec![node("n1", 0), node("n2", 2)]);
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_accepts_unique_contiguous_replica_ids() {
        let c = cfg(vec![node("n1", 0), node("n2", 1), node("n3", 2)]);
        assert!(c.validate().is_ok());
    }

    #[test]
    fn validate_skips_checks_when_disabled() {
        let mut c = cfg(vec![]);
        c.enabled = false;
        assert!(c.validate().is_ok());
    }

    #[test]
    fn validate_rejects_duplicate_tcp_replica_port() {
        let ports = TransportPorts {
            tcp: None,
            quic: None,
            http: None,
            websocket: None,
            tcp_replica: Some(9090),
        };
        let mut n1 = node("n1", 0);
        n1.ports = ports.clone();
        let mut n2 = node("n2", 1);
        n2.ports = ports;
        let c = cfg(vec![n1, n2]);
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_cross_transport_port_reuse() {
        let mut n1 = node("n1", 0);
        n1.ports = TransportPorts {
            tcp: Some(8090),
            quic: None,
            http: Some(8090),
            websocket: None,
            tcp_replica: None,
        };
        let c = cfg(vec![n1]);
        assert!(
            c.validate().is_err(),
            "same port on TCP and HTTP of the same node must be rejected"
        );
    }

    #[test]
    fn validate_accepts_same_port_on_different_ips() {
        let mut n1 = node("n1", 0);
        n1.ip = "127.0.0.1".to_string();
        n1.ports = TransportPorts {
            tcp: Some(8090),
            quic: None,
            http: None,
            websocket: None,
            tcp_replica: None,
        };
        let mut n2 = node("n2", 1);
        n2.ip = "127.0.0.2".to_string();
        n2.ports = TransportPorts {
            tcp: Some(8090),
            quic: None,
            http: None,
            websocket: None,
            tcp_replica: None,
        };
        let c = cfg(vec![n1, n2]);
        assert!(c.validate().is_ok());
    }

    #[test]
    fn validate_rejects_zero_tcp_replica_port() {
        let ports = TransportPorts {
            tcp: None,
            quic: None,
            http: None,
            websocket: None,
            tcp_replica: Some(0),
        };
        let mut n1 = node("n1", 0);
        n1.ports = ports;
        let c = cfg(vec![n1]);
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_accepts_empty_secret_when_auth_disabled() {
        // Default: no secret, auth off -> legacy mode, must pass.
        let c = cfg(vec![node("n1", 0), node("n2", 1)]);
        assert!(c.validate().is_ok());
    }

    #[test]
    fn validate_rejects_missing_secret_when_auth_enabled() {
        let mut c = cfg(vec![node("n1", 0), node("n2", 1)]);
        c.auth.enabled = true;
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_short_secret_when_auth_enabled() {
        let mut c = cfg(vec![node("n1", 0), node("n2", 1)]);
        c.auth.enabled = true;
        c.auth.shared_secret = "a".repeat(MIN_SHARED_SECRET_LEN - 1);
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_short_secret_even_when_auth_disabled() {
        // Typo guard: a configured-but-short key fails even with auth off.
        let mut c = cfg(vec![node("n1", 0), node("n2", 1)]);
        c.auth.shared_secret = "a".repeat(MIN_SHARED_SECRET_LEN - 1);
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_accepts_valid_secret_when_auth_enabled() {
        let mut c = cfg(vec![node("n1", 0), node("n2", 1)]);
        c.auth.enabled = true;
        c.auth.shared_secret = "a".repeat(MIN_SHARED_SECRET_LEN);
        assert!(c.validate().is_ok());
    }

    fn tls_files() -> ClusterTlsConfig {
        ClusterTlsConfig {
            enabled: true,
            self_signed: false,
            cert_file: "cert.pem".to_string(),
            key_file: "key.pem".to_string(),
            ca_file: "ca.pem".to_string(),
        }
    }

    #[test]
    fn validate_rejects_tls_ca_mode_with_missing_files() {
        // Auth on so the failure exercises the file check, not the auth gate.
        for missing in ["cert_file", "key_file", "ca_file"] {
            let mut c = cfg(vec![node("n1", 0), node("n2", 1)]);
            c.auth.enabled = true;
            c.auth.shared_secret = "a".repeat(MIN_SHARED_SECRET_LEN);
            c.tls = tls_files();
            match missing {
                "cert_file" => c.tls.cert_file.clear(),
                "key_file" => c.tls.key_file.clear(),
                _ => c.tls.ca_file.clear(),
            }
            assert!(c.validate().is_err(), "missing {missing} must be rejected");
        }
    }

    #[test]
    fn validate_rejects_tls_self_signed_without_auth() {
        // Accept-any certificate without the PSK handshake = MITM-able.
        let mut c = cfg(vec![node("n1", 0), node("n2", 1)]);
        c.tls = ClusterTlsConfig {
            enabled: true,
            self_signed: true,
            ..ClusterTlsConfig::default()
        };
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_accepts_tls_self_signed_with_auth() {
        let mut c = cfg(vec![node("n1", 0), node("n2", 1)]);
        c.auth.enabled = true;
        c.auth.shared_secret = "a".repeat(MIN_SHARED_SECRET_LEN);
        c.tls = ClusterTlsConfig {
            enabled: true,
            self_signed: true,
            ..ClusterTlsConfig::default()
        };
        assert!(c.validate().is_ok());
    }

    #[test]
    fn validate_rejects_tls_ca_mode_without_auth() {
        // TLS never authenticates the dialer (no client certificates);
        // only the PSK handshake does, so it is mandatory with TLS on.
        let mut c = cfg(vec![node("n1", 0), node("n2", 1)]);
        c.tls = tls_files();
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_accepts_tls_ca_mode_with_auth() {
        let mut c = cfg(vec![node("n1", 0), node("n2", 1)]);
        c.auth.enabled = true;
        c.auth.shared_secret = "a".repeat(MIN_SHARED_SECRET_LEN);
        c.tls = tls_files();
        assert!(c.validate().is_ok());
    }
}
