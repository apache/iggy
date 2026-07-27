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
use crate::http::HttpJwtConfig;
use configs::ConfigEnv;
use iggy_common::{IggyDuration, Validatable};
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};
use std::fmt;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::str::FromStr;
use std::time::Duration;

/// The primary heartbeats roughly every second (`PING_TICKS`); a window at
/// or below one ping interval would elect on every scheduling hiccup.
pub const MIN_CLUSTER_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(2);

/// Length floor for the replica-auth PSK, in raw bytes. The 32-byte MAC key
/// is KDF-derived from these bytes at use-site, so any encoding clearing this
/// length is accepted.
const MIN_SHARED_SECRET_LEN: usize = 32;

/// DNS caps a full name at 255 octets on the wire, which leaves 253
/// characters of presentation text (RFC 1035).
const MAX_HOSTNAME_LEN: usize = 253;

/// Per-label limit from RFC 1035.
const MAX_HOSTNAME_LABEL_LEN: usize = 63;

/// serde fallback for configs written before the field existed; the value
/// itself lives in `core/server-ng/config.toml` like every other default.
fn default_heartbeat_timeout() -> IggyDuration {
    SERVER_NG_CONFIG.cluster.heartbeat_timeout.parse().unwrap()
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
#[serde(deny_unknown_fields)]
pub struct ClusterNodeConfig {
    pub name: String,
    pub ip: String,
    /// Optional client-facing address: a literal IP or a DNS hostname,
    /// validated as [`AdvertisedAddress`] at boot. Replica traffic continues
    /// to use [`Self::ip`].
    #[serde(default)]
    pub advertised_address: Option<String>,
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

/// A validated client-facing node address: a literal IP or a DNS hostname.
///
/// Hostnames follow RFC 1123: ASCII letters, digits and hyphens in labels of
/// 1-63 characters that do not start or end with a hyphen, at most
/// [`MAX_HOSTNAME_LEN`] characters total, no port and no trailing dot. Names
/// consisting solely of digits and dots are rejected as malformed IPv4 rather
/// than accepted as hostnames, so `10.0.0.256` fails loudly instead of being
/// handed to DNS. Hostnames normalize to lowercase and IPs to their canonical
/// form ([`IpAddr`]), so textual variants of one address (`Broker.Example.COM`,
/// `2001:DB8::1`, `[2001:db8::1]`) compare equal.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AdvertisedAddress {
    Ip(IpAddr),
    Hostname(String),
}

impl AdvertisedAddress {
    /// Render `host:port` for a URL or endpoint listing, bracketing IPv6
    /// hosts (`[::1]:8080`) so the port separator stays unambiguous.
    pub fn authority(&self, port: u16) -> String {
        match self {
            Self::Ip(ip) => SocketAddr::new(*ip, port).to_string(),
            Self::Hostname(hostname) => format!("{hostname}:{port}"),
        }
    }
}

impl FromStr for AdvertisedAddress {
    type Err = AdvertisedAddressError;

    fn from_str(address: &str) -> Result<Self, Self::Err> {
        if address.is_empty() {
            return Err(AdvertisedAddressError::Empty);
        }
        if let Ok(ip) = address.parse::<IpAddr>() {
            return Ok(Self::Ip(ip));
        }
        // URL-style bracketed IPv6 (`[2001:db8::1]`) is unambiguous; accept
        // it and store the inner address.
        if let Some(inner) = address
            .strip_prefix('[')
            .and_then(|rest| rest.strip_suffix(']'))
            && let Ok(ip) = inner.parse::<Ipv6Addr>()
        {
            return Ok(Self::Ip(IpAddr::V6(ip)));
        }
        if let Some((host, port)) = address.rsplit_once(':') {
            // `host:port` and `[v6]:port` are the common misconfigurations;
            // anything else with a colon can only be a broken IPv6 literal,
            // since ':' never appears in a hostname.
            let bracketed_host = host.starts_with('[') && host.ends_with(']');
            if !port.is_empty()
                && port.bytes().all(|byte| byte.is_ascii_digit())
                && (bracketed_host || !host.contains(':'))
            {
                return Err(AdvertisedAddressError::PortNotAllowed);
            }
            return Err(AdvertisedAddressError::MalformedIpv6);
        }
        if address.len() > MAX_HOSTNAME_LEN {
            return Err(AdvertisedAddressError::HostnameTooLong {
                length: address.len(),
            });
        }
        let mut all_labels_numeric = true;
        for label in address.split('.') {
            if label.is_empty() {
                return Err(AdvertisedAddressError::EmptyLabel);
            }
            if label.len() > MAX_HOSTNAME_LABEL_LEN {
                return Err(AdvertisedAddressError::LabelTooLong {
                    label: label.to_owned(),
                });
            }
            if label.starts_with('-') || label.ends_with('-') {
                return Err(AdvertisedAddressError::LabelHyphen {
                    label: label.to_owned(),
                });
            }
            if let Some(character) = label
                .chars()
                .find(|character| !character.is_ascii_alphanumeric() && *character != '-')
            {
                return Err(AdvertisedAddressError::InvalidCharacter { character });
            }
            all_labels_numeric &= label.bytes().all(|byte| byte.is_ascii_digit());
        }
        if all_labels_numeric {
            return Err(AdvertisedAddressError::MalformedIpv4);
        }
        // DNS resolution is case-insensitive; normalizing here makes equality
        // (and thus endpoint-conflict detection) case-insensitive too.
        Ok(Self::Hostname(address.to_ascii_lowercase()))
    }
}

impl fmt::Display for AdvertisedAddress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ip(ip) => write!(formatter, "{ip}"),
            Self::Hostname(hostname) => write!(formatter, "{hostname}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdvertisedAddressError {
    Empty,
    PortNotAllowed,
    MalformedIpv4,
    MalformedIpv6,
    HostnameTooLong { length: usize },
    EmptyLabel,
    LabelTooLong { label: String },
    LabelHyphen { label: String },
    InvalidCharacter { character: char },
}

impl fmt::Display for AdvertisedAddressError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(formatter, "address cannot be empty"),
            Self::PortNotAllowed => write!(
                formatter,
                "address must not include a port; ports are configured in cluster.nodes.ports"
            ),
            Self::MalformedIpv4 => write!(
                formatter,
                "address consists only of digits and dots but is not a valid IPv4 address"
            ),
            Self::MalformedIpv6 => write!(
                formatter,
                "address contains ':' but is not a valid IPv6 address, and ':' cannot appear in a hostname"
            ),
            Self::HostnameTooLong { length } => write!(
                formatter,
                "hostname is {length} characters long; the limit is {MAX_HOSTNAME_LEN}"
            ),
            Self::EmptyLabel => write!(
                formatter,
                "hostname contains an empty label (leading, trailing, or doubled dot)"
            ),
            Self::LabelTooLong { label } => write!(
                formatter,
                "hostname label '{label}' exceeds {MAX_HOSTNAME_LABEL_LEN} characters"
            ),
            Self::LabelHyphen { label } => write!(
                formatter,
                "hostname label '{label}' cannot start or end with a hyphen"
            ),
            Self::InvalidCharacter { character } => write!(
                formatter,
                "character '{character}' is not allowed in a hostname (allowed: ASCII letters, digits, '-', '.')"
            ),
        }
    }
}

impl std::error::Error for AdvertisedAddressError {}

/// Whether cluster-wide JWT key material exists: a configured `http.jwt`
/// secret, or the signing key derived from the cluster PSK. When it does, a
/// bearer minted on any node verifies on every node - the invariant
/// follower-to-primary HTTP forwarding depends on. Callers gate `http.enabled`
/// themselves; this covers only the key material.
///
/// Single source for both the boot-time config validator and the server-ng
/// runtime forwarding gate. If the two ever disagree the validator's roster
/// http-port guarantee is silently bypassed: forwarding would activate against
/// a node the validator never required to expose an http port, and every
/// forward through it fails closed with a 503.
pub fn http_forwarding_key_material(jwt: &HttpJwtConfig, cluster: &ClusterConfig) -> bool {
    cluster.enabled
        && ((cluster.auth.enabled && !cluster.auth.shared_secret.is_empty())
            || !jwt.encoding_secret.is_empty()
            || !jwt.decoding_secret.is_empty())
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
                 (the primary heartbeats every second; a shorter window elects on every hiccup)",
                self.heartbeat_timeout,
                MIN_CLUSTER_HEARTBEAT_TIMEOUT.as_secs()
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
        let mut used_advertised_endpoints = std::collections::HashSet::new();
        let mut used_raw_advertised_endpoints = std::collections::HashSet::new();

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

            let client_ports = [
                ("TCP", node.ports.tcp),
                ("QUIC", node.ports.quic),
                ("HTTP", node.ports.http),
                ("WebSocket", node.ports.websocket),
            ];
            let replica_port = ("TCP_REPLICA", node.ports.tcp_replica);

            for (name, port_opt) in client_ports.into_iter().chain([replica_port]) {
                if let Some(port) = port_opt {
                    if port == 0 {
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

            // An advertised address must parse strictly (IP or RFC 1123
            // hostname): the value is handed verbatim to every client via
            // cluster metadata and redirect URLs, so a bad one poisons them
            // all. The roster `ip` predates this check and is only validated
            // as non-empty (Docker service names with underscores exist in
            // the wild), so when it backs the client endpoints an unparsable
            // value falls back to raw-string comparison instead of failing
            // boot.
            let client_address = match node.advertised_address.as_deref() {
                Some(advertised_address) => match advertised_address.parse::<AdvertisedAddress>() {
                    Ok(address) => Some(address),
                    Err(error) => {
                        eprintln!(
                            "Invalid cluster configuration: advertised_address '{advertised_address}' for node '{}': {error}",
                            node.name
                        );
                        return Err(ConfigurationError::InvalidConfigurationValue);
                    }
                },
                None => node.ip.parse::<AdvertisedAddress>().ok(),
            };

            for (name, port) in &client_ports {
                if let Some(port) = port {
                    let (endpoint, inserted) = match &client_address {
                        Some(address) => (
                            address.authority(*port),
                            used_advertised_endpoints.insert((address.clone(), *port)),
                        ),
                        None => {
                            let endpoint = format!("{}:{port}", node.ip);
                            let inserted = used_raw_advertised_endpoints.insert(endpoint.clone());
                            (endpoint, inserted)
                        }
                    };
                    if !inserted {
                        eprintln!(
                            "Invalid cluster configuration: advertised client endpoint conflict - {endpoint} is already used (node '{}', transport {name})",
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

    #[test]
    fn cluster_node_rejects_unknown_fields() {
        let error = serde_json::from_str::<ClusterNodeConfig>(
            r#"{
                "name": "node-0",
                "ip": "10.0.0.1",
                "advertise_address": "203.0.113.1",
                "replica_id": 0,
                "ports": {}
            }"#,
        )
        .expect_err("misspelled advertised_address must be rejected");

        assert!(
            error
                .to_string()
                .contains("unknown field `advertise_address`"),
            "unexpected deserialization error: {error}"
        );
    }
}

#[cfg(test)]
mod advertised_address_tests {
    use super::*;

    #[test]
    fn parses_ip_literals_to_canonical_form() {
        assert_eq!(
            "203.0.113.1".parse::<AdvertisedAddress>(),
            Ok(AdvertisedAddress::Ip("203.0.113.1".parse().unwrap()))
        );
        for equivalent_address in ["2001:DB8::1", "2001:db8:0:0:0:0:0:1", "[2001:db8::1]"] {
            assert_eq!(
                equivalent_address.parse::<AdvertisedAddress>(),
                Ok(AdvertisedAddress::Ip("2001:db8::1".parse().unwrap())),
                "'{equivalent_address}' must parse to canonical 2001:db8::1"
            );
        }
    }

    #[test]
    fn normalizes_hostname_to_lowercase() {
        let address = "Broker-1.Example.COM".parse::<AdvertisedAddress>();
        assert_eq!(
            address,
            Ok(AdvertisedAddress::Hostname(
                "broker-1.example.com".to_owned()
            ))
        );
    }

    #[test]
    fn authority_brackets_ipv6_hosts_only() {
        let cases = [
            ("203.0.113.1", "203.0.113.1:8090"),
            ("2001:db8::1", "[2001:db8::1]:8090"),
            ("broker-1.example.com", "broker-1.example.com:8090"),
        ];
        for (host, expected_authority) in cases {
            let address = host.parse::<AdvertisedAddress>().expect("valid address");
            assert_eq!(address.authority(8090), expected_authority);
        }
    }

    #[test]
    fn rejects_port_suffixes() {
        for address_with_port in ["example.com:8090", "10.0.0.1:8090", "[2001:db8::1]:8090"] {
            assert_eq!(
                address_with_port.parse::<AdvertisedAddress>(),
                Err(AdvertisedAddressError::PortNotAllowed),
                "'{address_with_port}' must be rejected as host:port"
            );
        }
    }

    #[test]
    fn rejects_dotted_numeric_strings_as_malformed_ipv4() {
        for malformed_ip in ["10.0.0.256", "192.168.1", "12345"] {
            assert_eq!(
                malformed_ip.parse::<AdvertisedAddress>(),
                Err(AdvertisedAddressError::MalformedIpv4),
                "'{malformed_ip}' must not pass as a hostname"
            );
        }
    }

    #[test]
    fn rejects_broken_ipv6_literals() {
        for broken_ipv6 in ["2001:db8:::1", "[2001:db8::zz]", "::1::2"] {
            assert_eq!(
                broken_ipv6.parse::<AdvertisedAddress>(),
                Err(AdvertisedAddressError::MalformedIpv6),
                "'{broken_ipv6}' must be rejected as malformed IPv6"
            );
        }
    }
}

#[cfg(test)]
mod cluster_validate_tests {
    use super::*;

    fn node(name: &str, id: u8) -> ClusterNodeConfig {
        ClusterNodeConfig {
            name: name.to_string(),
            ip: "127.0.0.1".to_string(),
            advertised_address: None,
            replica_id: id,
            ports: TransportPorts::default(),
        }
    }

    fn cfg(nodes: Vec<ClusterNodeConfig>) -> ClusterConfig {
        ClusterConfig {
            enabled: true,
            name: "iggy-cluster".to_string(),
            heartbeat_timeout: default_heartbeat_timeout(),
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
    fn validate_rejects_duplicate_advertised_client_endpoint() {
        let mut n1 = node("n1", 0);
        n1.ip = "10.0.0.1".to_owned();
        n1.advertised_address = Some("203.0.113.1".to_owned());
        n1.ports.tcp = Some(8090);
        let mut n2 = node("n2", 1);
        n2.ip = "10.0.0.2".to_owned();
        n2.advertised_address = n1.advertised_address.clone();
        n2.ports.tcp = Some(8090);

        assert!(cfg(vec![n1, n2]).validate().is_err());
    }

    #[test]
    fn validate_rejects_equivalent_ipv6_advertised_client_endpoints() {
        for equivalent_address in ["2001:DB8::1", "2001:db8:0:0:0:0:0:1", "[2001:db8::1]"] {
            let mut n1 = node("n1", 0);
            n1.ip = "10.0.0.1".to_owned();
            n1.advertised_address = Some("2001:db8::1".to_owned());
            n1.ports.tcp = Some(8090);
            let mut n2 = node("n2", 1);
            n2.ip = "10.0.0.2".to_owned();
            n2.advertised_address = Some(equivalent_address.to_owned());
            n2.ports.tcp = Some(8090);

            assert!(
                cfg(vec![n1, n2]).validate().is_err(),
                "{equivalent_address} must conflict with 2001:db8::1"
            );
        }
    }

    #[test]
    fn validate_rejects_equivalent_ipv6_client_endpoints_from_node_ip() {
        let mut n1 = node("n1", 0);
        n1.ip = "2001:db8::1".to_owned();
        n1.ports.tcp = Some(8090);
        let mut n2 = node("n2", 1);
        n2.ip = "2001:db8:0:0:0:0:0:1".to_owned();
        n2.ports.tcp = Some(8090);

        assert!(cfg(vec![n1, n2]).validate().is_err());
    }

    #[test]
    fn validate_accepts_distinct_advertised_client_endpoints() {
        let mut n1 = node("n1", 0);
        n1.ip = "10.0.0.1".to_owned();
        n1.advertised_address = Some("203.0.113.1".to_owned());
        n1.ports.tcp = Some(8090);
        let mut n2 = node("n2", 1);
        n2.ip = "10.0.0.2".to_owned();
        n2.advertised_address = Some("203.0.113.2".to_owned());
        n2.ports.tcp = Some(8090);

        assert!(cfg(vec![n1, n2]).validate().is_ok());
    }

    #[test]
    fn validate_accepts_hostname_advertised_address() {
        let mut n1 = node("n1", 0);
        n1.advertised_address = Some("iggy-node-1.example.com".to_owned());
        n1.ports.tcp = Some(8090);

        assert!(cfg(vec![n1, node("n2", 1)]).validate().is_ok());
    }

    #[test]
    fn validate_rejects_malformed_advertised_addresses() {
        let oversized_label = format!("{}.example.com", "a".repeat(64));
        let oversized_hostname = format!("{}example.com", "a.".repeat(130));
        for advertised_address in [
            "",
            " 203.0.113.1",
            "10.0.0.256",
            "192.168.1",
            "example.com:8090",
            "[2001:db8::1]:8090",
            "2001:db8:::1",
            "iggy_node.example.com",
            "-node.example.com",
            "node-.example.com",
            ".example.com",
            "example..com",
            "example.com.",
            "ex\u{e4}mple.com",
            oversized_label.as_str(),
            oversized_hostname.as_str(),
        ] {
            let mut n1 = node("n1", 0);
            n1.advertised_address = Some(advertised_address.to_owned());

            assert!(
                cfg(vec![n1, node("n2", 1)]).validate().is_err(),
                "'{advertised_address}' must be rejected"
            );
        }
    }

    #[test]
    fn validate_rejects_case_variant_hostname_advertised_endpoints() {
        let mut n1 = node("n1", 0);
        n1.ip = "10.0.0.1".to_owned();
        n1.advertised_address = Some("broker.example.com".to_owned());
        n1.ports.tcp = Some(8090);
        let mut n2 = node("n2", 1);
        n2.ip = "10.0.0.2".to_owned();
        n2.advertised_address = Some("Broker.Example.COM".to_owned());
        n2.ports.tcp = Some(8090);

        assert!(cfg(vec![n1, n2]).validate().is_err());
    }

    #[test]
    fn validate_rejects_node_ip_hostname_clashing_with_advertised_hostname() {
        let mut n1 = node("n1", 0);
        n1.ip = "10.0.0.1".to_owned();
        n1.advertised_address = Some("broker.example.com".to_owned());
        n1.ports.tcp = Some(8090);
        let mut n2 = node("n2", 1);
        n2.ip = "broker.example.com".to_owned();
        n2.ports.tcp = Some(8090);

        assert!(cfg(vec![n1, n2]).validate().is_err());
    }

    #[test]
    fn validate_accepts_distinct_hostname_advertised_endpoints() {
        let mut n1 = node("n1", 0);
        n1.ip = "10.0.0.1".to_owned();
        n1.advertised_address = Some("broker-1.example.com".to_owned());
        n1.ports.tcp = Some(8090);
        let mut n2 = node("n2", 1);
        n2.ip = "10.0.0.2".to_owned();
        n2.advertised_address = Some("broker-2.example.com".to_owned());
        n2.ports.tcp = Some(8090);

        assert!(cfg(vec![n1, n2]).validate().is_ok());
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
