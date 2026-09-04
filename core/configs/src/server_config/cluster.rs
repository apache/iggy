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

//! Cluster schema: node topology plus the VSR consensus tunables.

use super::defaults::SERVER_CONFIG;
use crate::ConfigurationError;
use crate::http::HttpJwtConfig;
use configs::ConfigEnv;
use iggy_common::{IggyDuration, Validatable};
use ipnet::{IpNet, Ipv4Net};
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};
use std::cmp::Reverse;
use std::fmt;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::str::FromStr;
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

/// The view-change status backstop (`view_change_status_timeout`) must span at
/// least this many retransmit intervals (`view_change_retransmit_interval`), so
/// a few dropped `StartViewChange` / `DoViewChange` messages retransmit rather
/// than escalating a progressing view change into a fresh cluster-wide election.
const MIN_STATUS_TO_RETRANSMIT_RATIO: u32 = 4;

/// Floor for a nonzero `superblock_wedged_fatal_timeout`: a shorter window
/// would fail-stop the process on a transient disk hiccup instead of a wedge.
const MIN_SUPERBLOCK_WEDGED_FATAL_TIMEOUT: Duration = Duration::from_secs(30);

/// Default recovering-replica probe-attempt ceiling. Duplicated here rather
/// than imported so `core/configs` keeps off a build-time edge onto
/// `core/consensus` (mirroring [`super::partition`]); `core/server`'s
/// bootstrap static-asserts it equal to `consensus::PROBE_ATTEMPTS_MAX`.
pub const DEFAULT_VIEW_PROBE_ATTEMPTS_MAX: u32 = 5;

/// Upper bound on `view_probe_attempts_max`. A recovering replica probes once
/// per `request_start_view_retransmit_interval`, so hundreds of attempts would
/// stall the election fallback for minutes on a full-cluster restart; this is a
/// typo guard, not a sizing endorsement.
const MAX_VIEW_PROBE_ATTEMPTS: u32 = 100;

/// Default per-round repair-serving chunk. Duplicated here rather than imported
/// so `core/configs` keeps off a build-time edge onto `core/shard` (mirroring
/// [`DEFAULT_VIEW_PROBE_ATTEMPTS_MAX`]); `core/server`'s bootstrap
/// static-asserts it equal to `shard::REPAIR_CHUNK_MAX`.
pub const DEFAULT_REPAIR_CHUNK_MAX: usize = 128;

/// `size_of::<StateChunkHeader>()`. Duplicated here for the same reason as
/// [`DEFAULT_REPAIR_CHUNK_MAX`]; `core/server`'s bootstrap static-asserts it
/// against the real header. Used to reject a `[message_bus] max_message_size`
/// too small to carry a single state-transfer chunk.
pub const STATE_CHUNK_HEADER_LEN: u64 = 256;

/// Upper bound on `repair_chunk_max`. A chunk rides the per-peer bus queue, so
/// the load-bearing rule is `repair_chunk_max < message_bus.peer_queue_capacity`
/// (enforced at the top level); this standalone ceiling is a typo guard.
const MAX_REPAIR_CHUNK_MAX: usize = 1024;

/// Upper bound on per-node `advertised_addresses` selectors. Must mirror the
/// `#[config_env(max_elements = 16)]` cap on the field: env overrides above
/// the cap do not exist, so a TOML roster exceeding it could never be
/// replicated through the env path. Also bounds the cross-node conflict scan
/// (quadratic in pooled entries) and the per-request longest-prefix walk.
const MAX_ADVERTISED_SELECTORS: usize = 16;

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
/// itself lives in `core/server/config.toml` like every other default.
fn default_heartbeat_timeout() -> IggyDuration {
    SERVER_CONFIG.cluster.heartbeat_timeout.parse().unwrap()
}

/// serde fallback for configs written before the field existed; the value
/// itself lives in `core/server/config.toml` like every other default.
fn default_commit_broadcast_interval() -> IggyDuration {
    SERVER_CONFIG
        .cluster
        .commit_broadcast_interval
        .parse()
        .unwrap()
}

/// serde fallback for configs written before the field existed; the value
/// itself lives in `core/server/config.toml` like every other default.
fn default_prepare_retransmit_interval() -> IggyDuration {
    SERVER_CONFIG
        .cluster
        .prepare_retransmit_interval
        .parse()
        .unwrap()
}

/// serde fallback for configs written before the field existed; the value
/// itself lives in `core/server/config.toml` like every other default.
fn default_view_change_retransmit_interval() -> IggyDuration {
    SERVER_CONFIG
        .cluster
        .view_change_retransmit_interval
        .parse()
        .unwrap()
}

/// serde fallback for configs written before the field existed; the value
/// itself lives in `core/server/config.toml` like every other default.
fn default_view_change_status_timeout() -> IggyDuration {
    SERVER_CONFIG
        .cluster
        .view_change_status_timeout
        .parse()
        .unwrap()
}

/// serde fallback for configs written before the field existed; the value
/// itself lives in `core/server/config.toml` like every other default.
fn default_request_start_view_retransmit_interval() -> IggyDuration {
    SERVER_CONFIG
        .cluster
        .request_start_view_retransmit_interval
        .parse()
        .unwrap()
}

/// serde fallback for configs written before the field existed; the value
/// itself lives in `core/server/config.toml` like every other default.
fn default_view_probe_attempts_max() -> u32 {
    SERVER_CONFIG.cluster.view_probe_attempts_max as u32
}

/// serde fallback for configs written before the field existed; the value
/// itself lives in `core/server/config.toml` like every other default.
fn default_repair_retry_interval() -> IggyDuration {
    SERVER_CONFIG.cluster.repair_retry_interval.parse().unwrap()
}

/// serde fallback for configs written before the field existed; the value
/// itself lives in `core/server/config.toml` like every other default.
fn default_repair_chunk_max() -> usize {
    SERVER_CONFIG.cluster.repair_chunk_max as usize
}

/// serde fallback for configs written before the field existed; the value
/// itself lives in `core/server/config.toml` like every other default.
fn default_superblock_wedged_fatal_timeout() -> IggyDuration {
    SERVER_CONFIG
        .cluster
        .superblock_wedged_fatal_timeout
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
    /// How often a plane retransmits its `StartViewChange` / `DoViewChange`
    /// while a view change is running. Lower converges a healthy election
    /// faster at the cost of replica traffic. Sizes both consensus view-change
    /// retransmit timers, which are deliberately equal. Zero (and the `0` /
    /// `disabled` / `unlimited` sentinels, which all parse to zero) is rejected
    /// at boot.
    #[serde(default = "default_view_change_retransmit_interval")]
    #[serde_as(as = "DisplayFromStr")]
    #[config_env(leaf)]
    pub view_change_retransmit_interval: IggyDuration,
    /// Backstop for a stalled view change: one that does not conclude within
    /// this window escalates to a fresh cluster-wide election. Must span
    /// several `view_change_retransmit_interval`s so a few dropped view-change
    /// messages retransmit rather than escalate: boot rejects
    /// `view_change_status_timeout < MIN_STATUS_TO_RETRANSMIT_RATIO *
    /// view_change_retransmit_interval`. Zero (and the `0` / `disabled` /
    /// `unlimited` sentinels, which all parse to zero) is rejected at boot.
    #[serde(default = "default_view_change_status_timeout")]
    #[serde_as(as = "DisplayFromStr")]
    #[config_env(leaf)]
    pub view_change_status_timeout: IggyDuration,
    /// How often a recovering or view-change backup re-requests the current
    /// view's `StartView` from its primary (`RequestStartView`). Sizes the
    /// consensus `RequestStartView` timer. Zero (and the `0` / `disabled` /
    /// `unlimited` sentinels, which all parse to zero) is rejected at boot.
    #[serde(default = "default_request_start_view_retransmit_interval")]
    #[serde_as(as = "DisplayFromStr")]
    #[config_env(leaf)]
    pub request_start_view_retransmit_interval: IggyDuration,
    /// How many consecutive unanswered `RequestStartView` probes a recovering
    /// replica tolerates before it falls back to an election (a full-cluster
    /// restart leaves nobody settled to answer). Must be >= 1 and <=
    /// `MAX_VIEW_PROBE_ATTEMPTS`.
    #[serde(default = "default_view_probe_attempts_max")]
    pub view_probe_attempts_max: u32,
    /// How long a stalled journal-repair stream waits before re-requesting its
    /// remaining window from the serving peer. Repair frames are
    /// fire-and-forget over the lossy bus, so a session with no retry wedges
    /// forever on a single dropped frame. Paces both the metadata and
    /// partition repair loops. Sizes the retry threshold in consensus ticks.
    /// Zero (and the `0` / `disabled` / `unlimited` sentinels, which all parse
    /// to zero) is rejected at boot.
    ///
    /// It also sizes the partition sweep's gap debounce: how long a backup must
    /// hold committed ops it cannot walk to before the shard OPENS a repair
    /// session for it. That second use is floored at
    /// `PARTITION_GAP_DEBOUNCE_TICKS_MIN` consensus ticks, so shortening the
    /// interval below the floor does not arm repair any sooner - one tick of
    /// lag is ordinary pipelining. Gap recovery therefore starts after
    /// `max(this, the floor)`, while the stalled-stream retry follows the
    /// configured value directly. The shard crate owns the floor; `config.toml`
    /// states its value.
    #[serde(default = "default_repair_retry_interval")]
    #[serde_as(as = "DisplayFromStr")]
    #[config_env(leaf)]
    pub repair_retry_interval: IggyDuration,
    /// Prepares a peer serves per repair round before the requester walks to
    /// the next chunk. Each frame rides the per-peer message-bus queue, so this
    /// must stay below `message_bus.peer_queue_capacity` or a full round
    /// overruns the queue and silently drops frames (enforced at the top
    /// level). Applies to both the metadata and partition repair planes. Must
    /// be > 0 and <= `MAX_REPAIR_CHUNK_MAX`.
    #[serde(default = "default_repair_chunk_max")]
    pub repair_chunk_max: usize,
    /// How long a superblock may stay unwritable before the replica fail-stops.
    /// Applies per plane: the metadata superblock, and each PARTITION's own. While
    /// wedged the group is already fenced quorum-invisible and peers elect around
    /// it; this converts the log-only limp into a distinct exit status a
    /// supervisor can act on. Zero (and the `0` /
    /// `disabled` / `unlimited` sentinels, which all parse to zero) disables
    /// the fail-stop; nonzero values below
    /// `MIN_SUPERBLOCK_WEDGED_FATAL_TIMEOUT` are rejected at boot.
    #[serde(default = "default_superblock_wedged_fatal_timeout")]
    #[serde_as(as = "DisplayFromStr")]
    #[config_env(leaf)]
    pub superblock_wedged_fatal_timeout: IggyDuration,
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
    /// Shard-0 coordinator placement tunables.
    #[serde(default)]
    pub coordinator: ClusterCoordinatorConfig,
}

/// Placement tunables for the shard-0 coordinator, converted into the shard
/// crate's `CoordinatorConfig` at bootstrap (the domain type lives there
/// because `configs` and `shard` share no dependency edge).
#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
#[serde(deny_unknown_fields)]
pub struct ClusterCoordinatorConfig {
    /// When `total_shards > 1`, exclude shard 0 from replica placement.
    /// Shard 0 already hosts the coordinator, the metadata writer, and both
    /// listeners; replicas are long-lived steady flows, so offload them to
    /// peer shards by default.
    #[serde(default = "default_skip_shard_zero_for_replicas")]
    pub skip_shard_zero_for_replicas: bool,
    /// When `total_shards > 1`, exclude shard 0 from client placement.
    /// Default false: client connections are short-lived and benefit from
    /// shard-0 parallelism more than replicas do.
    #[serde(default)]
    pub skip_shard_zero_for_clients: bool,
}

fn default_skip_shard_zero_for_replicas() -> bool {
    true
}

impl Default for ClusterCoordinatorConfig {
    fn default() -> Self {
        Self {
            skip_shard_zero_for_replicas: default_skip_shard_zero_for_replicas(),
            skip_shard_zero_for_clients: false,
        }
    }
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
    /// Retiring pre-shared key, accepted for VERIFICATION only during a key
    /// rotation window; every MAC this node produces uses [`Self::shared_secret`].
    ///
    /// Enables rolling PSK rotation without an auth outage, three rolls:
    /// 1. every node gets `shared_secret = old, previous_shared_secret = new`;
    /// 2. every node gets `shared_secret = new, previous_shared_secret = old`;
    /// 3. every node gets `shared_secret = new` alone, closing the window.
    ///
    /// Leave empty (default) outside a rotation. Same 32-byte minimum and
    /// provisioning rules as `shared_secret`
    /// (`IGGY_CLUSTER_AUTH_PREVIOUS_SHARED_SECRET`).
    #[serde(default, skip_serializing)]
    #[config_env(secret)]
    pub previous_shared_secret: String,
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
    /// Replica-plane address, dialed verbatim by every peer, a literal IP.
    pub ip: String,
    /// Optional client-facing address: a literal IP or a DNS hostname,
    /// validated as [`AdvertisedAddress`] at boot. Replica traffic continues
    /// to use [`Self::ip`].
    #[serde(default)]
    pub advertised_address: Option<String>,
    /// Client-network-scoped overrides of [`Self::advertised_address`],
    /// resolved by longest-prefix match over the client's IP (see
    /// [`AdvertisedAddressSelector`]). Empty by default, so existing configs
    /// keep the single catch-all address. At most `MAX_ADVERTISED_SELECTORS`
    /// per node (validated), matching the env-override cap below.
    #[serde(default)]
    #[config_env(max_elements = 16)]
    pub advertised_addresses: Vec<AdvertisedAddressSelector>,
    /// Numeric replica ID for VSR consensus (0-based).
    ///
    /// Must be unique across [`ClusterConfig::nodes`] and strictly less than
    /// `nodes.len()`. Validated by [`ClusterConfig::validate`].
    pub replica_id: u8,
    pub ports: TransportPorts,
}

/// One client-network-scoped advertised address: clients whose IP falls
/// inside `client_cidr` are told `address` instead of the node's catch-all
/// [`ClusterNodeConfig::advertised_address`].
///
/// Typical split-network case: the roster `ip` is VPC-private and
/// `advertised_address` is public; a selector with the VPC CIDR keeps
/// in-VPC clients on the private address while everyone else stays on the
/// public one. Selection is longest-prefix match across a node's selectors.
/// Selection sees the transport-level peer address, so clients arriving
/// through a proxy or load balancer match the proxy's network, not their
/// own.
#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
#[serde(deny_unknown_fields)]
pub struct AdvertisedAddressSelector {
    /// Client network this selector matches, in CIDR notation
    /// (`10.0.0.0/16`, `2001:db8::/32`). Must parse at boot; duplicate
    /// networks within one node are rejected. A v4-mapped v6 network
    /// (`::ffff:10.0.0.0/104`) canonicalizes to its v4 form (`10.0.0.0/8`),
    /// matching how client IPs canonicalize before matching.
    pub client_cidr: String,
    /// Address advertised to matching clients: a literal IP or a DNS
    /// hostname, validated as [`AdvertisedAddress`] at boot (no port; ports
    /// come from [`ClusterNodeConfig::ports`]).
    pub address: String,
}

/// A roster node with its advertised-address selectors and catch-all parsed
/// once, built wherever a roster is assembled for serving clients
/// (listener/shard start). Per-request resolution never re-parses config
/// strings: everything is snapshotted here, so mutating the source config
/// after conversion has no effect on what clients are told.
#[derive(Debug, Clone)]
pub struct ResolvedClusterNode {
    config: ClusterNodeConfig,
    /// Truncated, canonicalized selector networks with their parsed
    /// addresses, in declaration order.
    selectors: Vec<(IpNet, AdvertisedAddress)>,
    /// Parsed catch-all: [`ClusterNodeConfig::advertised_address`], else the
    /// roster [`ClusterNodeConfig::ip`]. A set `advertised_address` never
    /// falls through to the private roster ip.
    catch_all: AdvertisedAddress,
    /// Parsed roster [`ClusterNodeConfig::ip`], the replica-plane dial
    /// address.
    replica_ip: IpAddr,
}

impl TryFrom<ClusterNodeConfig> for ResolvedClusterNode {
    type Error = ConfigurationError;

    fn try_from(config: ClusterNodeConfig) -> Result<Self, Self::Error> {
        let replica_ip = config.ip.parse::<IpAddr>().map_err(|error| {
            eprintln!(
                "Invalid cluster configuration: IP '{}' for node '{}' is not a literal IP \
                 address: {error}; set cluster.nodes[*].advertised_address for the name clients \
                 dial",
                config.ip, config.name
            );
            ConfigurationError::InvalidConfigurationValue
        })?;

        if replica_ip.to_canonical().is_unspecified() {
            eprintln!(
                "Invalid cluster configuration: IP '{}' for node '{}' is the unspecified \
                 address; declare the address peers and clients reach this node at",
                config.ip, config.name
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        let catch_all =
            match config.advertised_address.as_deref() {
                Some(advertised_address) => advertised_address
                    .parse::<AdvertisedAddress>()
                    .map_err(|error| {
                        eprintln!(
                            "Invalid cluster configuration: advertised_address \
                         '{advertised_address}' for node '{}': {error}",
                            config.name
                        );
                        ConfigurationError::InvalidConfigurationValue
                    })?,
                None => AdvertisedAddress::Ip(replica_ip.to_canonical()),
            };

        let mut selectors = Vec::with_capacity(config.advertised_addresses.len());
        for selector in &config.advertised_addresses {
            let network = selector.client_cidr.parse::<IpNet>().map_err(|error| {
                eprintln!(
                    "Invalid cluster configuration: advertised_addresses client_cidr '{}' for \
                     node '{}': {error}",
                    selector.client_cidr, config.name
                );
                ConfigurationError::InvalidConfigurationValue
            })?;
            let address = selector
                .address
                .parse::<AdvertisedAddress>()
                .map_err(|error| {
                    eprintln!(
                        "Invalid cluster configuration: advertised_addresses address '{}' for \
                         node '{}': {error}",
                        selector.address, config.name
                    );
                    ConfigurationError::InvalidConfigurationValue
                })?;
            selectors.push((canonical_ip_net(network.trunc()), address));
        }

        Ok(Self {
            config,
            selectors,
            catch_all,
            replica_ip,
        })
    }
}

impl ResolvedClusterNode {
    /// The roster entry this node was built from. Read-only: resolution runs
    /// on the boot-parsed snapshot, never on the config strings.
    #[must_use]
    pub fn config(&self) -> &ClusterNodeConfig {
        &self.config
    }

    /// The roster `ip` as a dialable address for the replica plane and
    /// internal request forwarding. Never routed through the advertised
    /// ladder: this is what servers dial, not what clients are told.
    #[must_use]
    pub fn replica_ip(&self) -> IpAddr {
        self.replica_ip
    }

    /// The client-facing address for a client connecting from `client_ip`:
    /// longest-prefix match over the selector networks, then the catch-all.
    /// Always an address, since construction refused a node whose sources did
    /// not parse.
    #[must_use]
    pub fn advertised_for(&self, client_ip: Option<IpAddr>) -> &AdvertisedAddress {
        client_ip
            .and_then(|client_ip| self.selector_address(client_ip))
            .unwrap_or(&self.catch_all)
    }

    /// Longest-prefix match over the boot-parsed selector networks. The
    /// client IP is canonicalized first so a v4-mapped v6 peer
    /// (`::ffff:10.0.0.7`, the shape a dual-stack listener reports) matches
    /// v4 networks. `min_by_key` keeps the first of equal-length matches, so
    /// resolution stays declaration-order deterministic even though a
    /// validated config cannot produce two matching networks of equal length
    /// (equal-length distinct networks are disjoint, duplicates are
    /// rejected).
    fn selector_address(&self, client_ip: IpAddr) -> Option<&AdvertisedAddress> {
        let client_ip = client_ip.to_canonical();
        self.selectors
            .iter()
            .filter(|(network, _)| network.contains(&client_ip))
            .min_by_key(|(network, _)| Reverse(network.prefix_len()))
            .map(|(_, address)| address)
    }
}

/// Network-side mirror of the `IpAddr::to_canonical` applied to client IPs
/// before matching: a selector network written in v4-mapped v6 form
/// (`::ffff:10.0.0.0/104`) becomes its v4 equivalent (`10.0.0.0/8`), since a
/// canonicalized client could never match the v6 spelling. Prefixes shorter
/// than 96 bits cannot drop the `::ffff:` mapping and stay v6 (they match
/// native v6 clients only).
fn canonical_ip_net(network: IpNet) -> IpNet {
    if let IpNet::V6(v6_network) = network
        && v6_network.prefix_len() >= 96
        && let IpAddr::V4(v4_address) = v6_network.addr().to_canonical()
        && let Ok(v4_network) = Ipv4Net::new(v4_address, v6_network.prefix_len() - 96)
    {
        return IpNet::V4(v4_network);
    }
    network
}

/// Per-node listener ports advertised in the cluster roster. In cluster mode
/// the roster is the single source of ports: every enabled transport needs
/// an explicit per-node port (validated at startup, no fallback to the
/// transport's top-level `address` port). The roster entry's `ip` is the
/// advertised address only: tcp/ws/quic/http bind the interface from their own
/// `address` config, and followers forward HTTP requests to the primary at
/// `ip:http`.
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
/// `MAX_HOSTNAME_LEN` characters total, no port and no trailing dot. Names
/// consisting solely of digits and dots are rejected as malformed IPv4 rather
/// than accepted as hostnames, so `10.0.0.256` fails loudly instead of being
/// handed to DNS. Hostnames normalize to lowercase and IPs to their canonical
/// form ([`IpAddr::to_canonical`]), so textual variants of one address
/// (`Broker.Example.COM`, `2001:DB8::1`, `[2001:db8::1]`, `::ffff:10.0.0.1`)
/// compare equal.
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

    /// Canonicalize first, so the v4-mapped spellings of one address
    /// (`::ffff:10.0.0.1`, `::ffff:0.0.0.0`) are the same value as their v4
    /// form for both the comparison and the wildcard refusal.
    fn from_ip(ip: IpAddr) -> Result<Self, AdvertisedAddressError> {
        let ip = ip.to_canonical();
        if ip.is_unspecified() {
            return Err(AdvertisedAddressError::Unspecified);
        }
        Ok(Self::Ip(ip))
    }
}

impl FromStr for AdvertisedAddress {
    type Err = AdvertisedAddressError;

    fn from_str(address: &str) -> Result<Self, Self::Err> {
        if address.is_empty() {
            return Err(AdvertisedAddressError::Empty);
        }
        if let Ok(ip) = address.parse::<IpAddr>() {
            return Self::from_ip(ip);
        }
        // URL-style bracketed IPv6 (`[2001:db8::1]`) is unambiguous; accept
        // it and store the inner address.
        if let Some(inner) = address
            .strip_prefix('[')
            .and_then(|rest| rest.strip_suffix(']'))
            && let Ok(ip) = inner.parse::<Ipv6Addr>()
        {
            return Self::from_ip(IpAddr::V6(ip));
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
    Unspecified,
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
            Self::Unspecified => write!(
                formatter,
                "address is the unspecified address, which tells a client which interfaces this \
                 node accepts on rather than where to reach it; declare a routable address"
            ),
            Self::PortNotAllowed => write!(
                formatter,
                "address must not include a port; each transport's port comes from its own \
                 listener address in single-node mode and from cluster.nodes.ports in a cluster"
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
/// Forwarding targets resolve from the roster (`ip:ports.http`); the config
/// validator unconditionally requires a roster port for every enabled
/// transport, so a forward never dials a node without a declared http port.
pub fn http_forwarding_key_material(jwt: &HttpJwtConfig, cluster: &ClusterConfig) -> bool {
    cluster.enabled
        && ((cluster.auth.enabled && !cluster.auth.shared_secret.is_empty())
            || !jwt.encoding_secret.is_empty()
            || !jwt.decoding_secret.is_empty())
}

impl Validatable<ConfigurationError> for ClusterConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        // Ahead of the enabled gate: the top-level rule against
        // message_bus.peer_queue_capacity holds repair_chunk_max unconditionally,
        // so a single-node config skipping these would be bound by the
        // cross-section rule while its own floor and ceiling went unchecked.
        if self.repair_chunk_max == 0 {
            eprintln!("Invalid cluster configuration: cluster.repair_chunk_max must be > 0");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.repair_chunk_max > MAX_REPAIR_CHUNK_MAX {
            eprintln!(
                "Invalid cluster configuration: cluster.repair_chunk_max ({}) exceeds the maximum \
                 ({MAX_REPAIR_CHUNK_MAX})",
                self.repair_chunk_max
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        // Also ahead of the enabled gate: a solo replica persists the
        // superblock too, so the fail-stop window applies regardless.
        let superblock_fatal_window = self.superblock_wedged_fatal_timeout.get_duration();
        if !superblock_fatal_window.is_zero()
            && superblock_fatal_window < MIN_SUPERBLOCK_WEDGED_FATAL_TIMEOUT
        {
            eprintln!(
                "Invalid cluster configuration: cluster.superblock_wedged_fatal_timeout '{}' must \
                 be zero (disabled) or at least {}s (a shorter window fail-stops the process on a \
                 transient disk hiccup)",
                self.superblock_wedged_fatal_timeout,
                MIN_SUPERBLOCK_WEDGED_FATAL_TIMEOUT.as_secs()
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

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

        // The three view-change timers each size a consensus timer that has to
        // advance; `0` / `disabled` / `unlimited` all collapse to zero and
        // would stall it - reject them.
        if self
            .view_change_retransmit_interval
            .get_duration()
            .is_zero()
        {
            eprintln!(
                "Invalid cluster configuration: cluster.view_change_retransmit_interval must be \
                 nonzero (it drives StartViewChange / DoViewChange retransmission)"
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.view_change_status_timeout.get_duration().is_zero() {
            eprintln!(
                "Invalid cluster configuration: cluster.view_change_status_timeout must be nonzero \
                 (it backstops a stalled view change)"
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self
            .request_start_view_retransmit_interval
            .get_duration()
            .is_zero()
        {
            eprintln!(
                "Invalid cluster configuration: cluster.request_start_view_retransmit_interval \
                 must be nonzero (it drives RequestStartView retransmission)"
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        // The status backstop must span several retransmits so a few dropped
        // view-change messages retransmit rather than escalating a progressing
        // view change into a fresh cluster-wide election.
        let min_status = self
            .view_change_retransmit_interval
            .get_duration()
            .saturating_mul(MIN_STATUS_TO_RETRANSMIT_RATIO);
        if self.view_change_status_timeout.get_duration() < min_status {
            eprintln!(
                "Invalid cluster configuration: cluster.view_change_status_timeout '{}' must be at \
                 least {}x cluster.view_change_retransmit_interval '{}' so a stalled view change \
                 retransmits before it escalates to an election",
                self.view_change_status_timeout,
                MIN_STATUS_TO_RETRANSMIT_RATIO,
                self.view_change_retransmit_interval
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        // A recovering replica needs at least one probe before it may give up
        // and elect; the ceiling is a typo guard (see MAX_VIEW_PROBE_ATTEMPTS).
        if self.view_probe_attempts_max == 0 {
            eprintln!(
                "Invalid cluster configuration: cluster.view_probe_attempts_max must be >= 1"
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.view_probe_attempts_max > MAX_VIEW_PROBE_ATTEMPTS {
            eprintln!(
                "Invalid cluster configuration: cluster.view_probe_attempts_max ({}) exceeds the \
                 maximum ({MAX_VIEW_PROBE_ATTEMPTS})",
                self.view_probe_attempts_max
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        // The repair retry interval sizes a tick threshold that has to advance;
        // `0` / `disabled` / `unlimited` all collapse to zero and would wedge
        // every stalled repair stream - reject them.
        if self.repair_retry_interval.get_duration().is_zero() {
            eprintln!(
                "Invalid cluster configuration: cluster.repair_retry_interval must be nonzero \
                 (it paces stalled-repair retries)"
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
        let mut advertised_endpoints: Vec<AdvertisedEndpoint> = Vec::new();

        for node in &self.nodes {
            if node.name.trim().is_empty() {
                eprintln!("Invalid cluster configuration: node name cannot be empty");
                return Err(ConfigurationError::InvalidConfigurationValue);
            }

            let resolved = ResolvedClusterNode::try_from(node.clone())?;

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

                    // Keyed on the canonical form, and rendered from it, so
                    // two spellings of one address ('::ffff:10.0.0.1' and
                    // '10.0.0.1') cannot claim the same port twice and an IPv6
                    // endpoint reads back with its port separable.
                    let endpoint = SocketAddr::new(resolved.replica_ip().to_canonical(), port);
                    if !used_endpoints.insert(endpoint) {
                        eprintln!(
                            "Invalid cluster configuration: port conflict - {endpoint} is already bound (node '{}', transport {name})",
                            node.name
                        );
                        return Err(ConfigurationError::InvalidConfigurationValue);
                    }
                }
            }

            if node.advertised_addresses.len() > MAX_ADVERTISED_SELECTORS {
                eprintln!(
                    "Invalid cluster configuration: node '{}' declares {} advertised_addresses \
                     selectors, exceeding the maximum ({MAX_ADVERTISED_SELECTORS})",
                    node.name,
                    node.advertised_addresses.len()
                );
                return Err(ConfigurationError::InvalidConfigurationValue);
            }

            // Networks are compared truncated (`10.0.1.0/16` ==
            // `10.0.0.0/16`) and canonicalized (`::ffff:10.0.0.0/104` ==
            // `10.0.0.0/8`), the form resolution matched them in, so two
            // spellings of one network cannot both be declared.
            let selectors = &resolved.selectors;
            let mut seen_selector_cidrs = std::collections::HashSet::new();
            for ((client_cidr, _), declared) in selectors.iter().zip(&node.advertised_addresses) {
                if !seen_selector_cidrs.insert(*client_cidr) {
                    eprintln!(
                        "Invalid cluster configuration: duplicate advertised_addresses client_cidr '{}' for node '{}'",
                        declared.client_cidr, node.name
                    );
                    return Err(ConfigurationError::InvalidConfigurationValue);
                }
            }
            let selector_ranges: Vec<ClientAddressRange> = selectors
                .iter()
                .map(|(network, _)| ClientAddressRange::from(network))
                .collect();

            // Endpoint conflicts are checked across every node's selectors
            // and catch-all on effective client sets - the clients an entry
            // actually wins after this node's longest-prefix match. Two
            // nodes may reuse one host:port as long as the winning sets stay
            // disjoint (that is the feature, and it includes a per-subnet
            // override shadowing the same node's wider selector); a conflict
            // means some client wins both entries and would resolve both
            // nodes to one endpoint. The catch-all is an implicit
            // match-everything-else selector, so it pools the same way.
            let catch_all_clients = EffectiveClients::for_catch_all(&selector_ranges);
            for (name, port) in &client_ports {
                if let Some(port) = port {
                    insert_advertised_endpoint(
                        &mut advertised_endpoints,
                        AdvertisedEndpoint {
                            node_name: &node.name,
                            transport: name,
                            network: None,
                            clients: catch_all_clients.clone(),
                            host: resolved.catch_all.clone(),
                            port: *port,
                        },
                    )?;
                }
            }

            for (selector_index, (client_cidr, address)) in selectors.iter().enumerate() {
                let sibling_ranges: Vec<ClientAddressRange> = selector_ranges
                    .iter()
                    .enumerate()
                    .filter(|(other_index, _)| *other_index != selector_index)
                    .map(|(_, range)| *range)
                    .collect();
                let clients = EffectiveClients::for_selector(client_cidr, &sibling_ranges);
                for (name, port) in &client_ports {
                    if let Some(port) = port {
                        insert_advertised_endpoint(
                            &mut advertised_endpoints,
                            AdvertisedEndpoint {
                                node_name: &node.name,
                                transport: name,
                                network: Some(*client_cidr),
                                clients: clients.clone(),
                                host: address.clone(),
                                port: *port,
                            },
                        )?;
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
        // Rotation window key: same typo guard as the primary, plus a
        // distinctness check - a window equal to the primary means the
        // operator rolled the config without changing the key, so the
        // "rotation" would silently be a no-op.
        if !self.auth.previous_shared_secret.is_empty() {
            if self.auth.previous_shared_secret.len() < MIN_SHARED_SECRET_LEN {
                eprintln!(
                    "Invalid cluster configuration: cluster.auth.previous_shared_secret must be >= {MIN_SHARED_SECRET_LEN} bytes"
                );
                return Err(ConfigurationError::InvalidConfigurationValue);
            }
            if self.auth.previous_shared_secret == self.auth.shared_secret {
                eprintln!(
                    "Invalid cluster configuration: cluster.auth.previous_shared_secret must differ from cluster.auth.shared_secret (an identical window is a no-op rotation)"
                );
                return Err(ConfigurationError::InvalidConfigurationValue);
            }
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

/// One advertised client endpoint and the clients it wins, pooled by
/// [`ClusterConfig::validate`] so selectors and catch-all conflict-check
/// against each other. `network: None` is the catch-all (`advertised_address`,
/// or the roster `ip` as fallback); `clients` is the entry's effective set
/// after its node's longest-prefix shadowing.
struct AdvertisedEndpoint<'roster> {
    node_name: &'roster str,
    transport: &'static str,
    network: Option<IpNet>,
    clients: EffectiveClients,
    host: AdvertisedAddress,
    port: u16,
}

impl AdvertisedEndpoint<'_> {
    /// True when some client would resolve both entries to one host:port on
    /// two different nodes. Effective client sets already encode each node's
    /// longest-prefix shadowing, so nested networks conflict only where the
    /// wider entry still wins some client that the other node's entry also
    /// wins. Entries of one node never conflict: their effective sets are
    /// disjoint by construction.
    fn conflicts_with(&self, other: &Self) -> bool {
        self.node_name != other.node_name
            && self.port == other.port
            && self.host == other.host
            && self.clients.overlaps(&other.clients)
    }

    fn authority(&self) -> String {
        self.host.authority(self.port)
    }

    fn network_description(&self) -> String {
        match self.network {
            Some(network) => format!("client_cidr {network}"),
            None => "every client network (catch-all)".to_owned(),
        }
    }
}

/// The clients an advertised entry actually wins under its node's
/// longest-prefix match, built by [`ClusterConfig::validate`] for the
/// cross-node conflict scan.
#[derive(Clone)]
struct EffectiveClients {
    /// Sorted disjoint ranges of winning client addresses.
    ranges: Vec<ClientAddressRange>,
    /// The catch-all also wins clients whose peer address the transport
    /// could not produce ([`ResolvedClusterNode::advertised_for`] with no
    /// client IP), so two catch-all overlap even when selectors cover both
    /// address families.
    serves_unknown_peers: bool,
}

impl EffectiveClients {
    /// A selector wins its network minus the sibling networks nested inside
    /// it (longer prefixes take the node's LPM). `sibling_ranges` must
    /// exclude the selector's own network.
    fn for_selector(network: &IpNet, sibling_ranges: &[ClientAddressRange]) -> Self {
        Self {
            ranges: ClientAddressRange::from(network).subtract_nested(sibling_ranges),
            serves_unknown_peers: false,
        }
    }

    /// The catch-all wins every client no selector matches, in both address
    /// families, plus unknown-peer clients.
    fn for_catch_all(selector_ranges: &[ClientAddressRange]) -> Self {
        let mut ranges = ClientAddressRange::FULL_IPV4.subtract_nested(selector_ranges);
        ranges.extend(ClientAddressRange::FULL_IPV6.subtract_nested(selector_ranges));
        Self {
            ranges,
            serves_unknown_peers: true,
        }
    }

    fn overlaps(&self, other: &Self) -> bool {
        if self.serves_unknown_peers && other.serves_unknown_peers {
            return true;
        }
        self.ranges.iter().any(|range| {
            other.ranges.iter().any(|other_range| {
                range.is_ipv4 == other_range.is_ipv4
                    && range.first <= other_range.last
                    && other_range.first <= range.last
            })
        })
    }
}

/// Inclusive range of client addresses within one family. Client IPs
/// canonicalize to v4 before matching, so v4 and v6 networks match disjoint
/// client populations and a range never spans families.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ClientAddressRange {
    is_ipv4: bool,
    first: u128,
    last: u128,
}

impl ClientAddressRange {
    const FULL_IPV4: Self = Self {
        is_ipv4: true,
        first: 0,
        last: u32::MAX as u128,
    };
    const FULL_IPV6: Self = Self {
        is_ipv4: false,
        first: 0,
        last: u128::MAX,
    };

    /// `self` minus every range nested inside it, as sorted disjoint
    /// leftovers. CIDR networks are nested or disjoint, never partially
    /// overlapping, so a range outside `self` is either disjoint from it
    /// (subtracts nothing) or contains it (a shorter prefix, which loses
    /// LPM and also subtracts nothing).
    fn subtract_nested(self, ranges: &[Self]) -> Vec<Self> {
        let mut nested: Vec<Self> = ranges
            .iter()
            .filter(|range| {
                range.is_ipv4 == self.is_ipv4
                    && range.first >= self.first
                    && range.last <= self.last
            })
            .copied()
            .collect();
        nested.sort_unstable();
        let mut remaining = Vec::new();
        let mut cursor = Some(self.first);
        for nested_range in nested {
            let Some(next_free) = cursor else { break };
            if nested_range.first > next_free {
                remaining.push(Self {
                    is_ipv4: self.is_ipv4,
                    first: next_free,
                    last: nested_range.first - 1,
                });
            }
            cursor = nested_range
                .last
                .checked_add(1)
                .map(|after| after.max(next_free));
        }
        if let Some(next_free) = cursor
            && next_free <= self.last
        {
            remaining.push(Self {
                is_ipv4: self.is_ipv4,
                first: next_free,
                last: self.last,
            });
        }
        remaining
    }
}

impl From<&IpNet> for ClientAddressRange {
    fn from(network: &IpNet) -> Self {
        match network {
            IpNet::V4(network) => Self {
                is_ipv4: true,
                first: u128::from(u32::from(network.network())),
                last: u128::from(u32::from(network.broadcast())),
            },
            IpNet::V6(network) => Self {
                is_ipv4: false,
                first: u128::from(network.network()),
                last: u128::from(network.broadcast()),
            },
        }
    }
}

fn insert_advertised_endpoint<'roster>(
    advertised_endpoints: &mut Vec<AdvertisedEndpoint<'roster>>,
    endpoint: AdvertisedEndpoint<'roster>,
) -> Result<(), ConfigurationError> {
    if let Some(existing) = advertised_endpoints
        .iter()
        .find(|existing| existing.conflicts_with(&endpoint))
    {
        eprintln!(
            "Invalid cluster configuration: advertised client endpoint conflict - {} is advertised for {} (node '{}', transport {}) and for {} (node '{}', transport {}); their effective client sets overlap after longest-prefix shadowing, so a client in the overlap would resolve both nodes to one endpoint",
            endpoint.authority(),
            endpoint.network_description(),
            endpoint.node_name,
            endpoint.transport,
            existing.network_description(),
            existing.node_name,
            existing.transport,
        );
        return Err(ConfigurationError::InvalidConfigurationValue);
    }
    advertised_endpoints.push(endpoint);
    Ok(())
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
            view_change_retransmit_interval: default_view_change_retransmit_interval(),
            view_change_status_timeout: default_view_change_status_timeout(),
            request_start_view_retransmit_interval: default_request_start_view_retransmit_interval(
            ),
            view_probe_attempts_max: default_view_probe_attempts_max(),
            repair_retry_interval: default_repair_retry_interval(),
            repair_chunk_max: default_repair_chunk_max(),
            superblock_wedged_fatal_timeout: default_superblock_wedged_fatal_timeout(),
            nodes: Vec::new(),
            auth: ClusterAuthConfig {
                enabled: true,
                shared_secret: "current-psk-MUST-NOT-be-persisted".to_owned(),
                previous_shared_secret: "retiring-psk-MUST-NOT-be-persisted".to_owned(),
            },
            tls: ClusterTlsConfig::default(),
            coordinator: ClusterCoordinatorConfig::default(),
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

    #[test]
    fn advertised_addresses_env_expansion_is_capped() {
        // The selectors Vec nests inside the nodes Vec, so the derive's
        // index ceilings multiply; without the field's max_elements cap the
        // default of 256x256 adds ~131k leaked mappings to every boot.
        let mappings = <ClusterConfig as configs::ConfigEnvMappings>::env_mappings();
        assert!(
            mappings
                .iter()
                .any(|mapping| mapping.env_name.contains("ADVERTISED_ADDRESSES_15_")),
            "selector index 15 must stay reachable by env override"
        );
        assert!(
            !mappings
                .iter()
                .any(|mapping| mapping.env_name.contains("ADVERTISED_ADDRESSES_16_")),
            "selector env expansion must stop at max_elements = 16"
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
    fn rejects_every_spelling_of_the_unspecified_address() {
        for wildcard in [
            "0.0.0.0",
            "::",
            "::ffff:0.0.0.0",
            "[::]",
            "[::ffff:0.0.0.0]",
        ] {
            assert_eq!(
                wildcard.parse::<AdvertisedAddress>(),
                Err(AdvertisedAddressError::Unspecified),
                "'{wildcard}' names interfaces, not a dial target"
            );
        }
        for routable in ["203.0.113.1", "2001:db8::1", "::ffff:203.0.113.1", "broker"] {
            assert!(
                routable.parse::<AdvertisedAddress>().is_ok(),
                "'{routable}' is dialable"
            );
        }
    }

    /// A v4-mapped IPv6 address and its v4 form are one address, so they must
    /// be one value: the conflict pool compares advertised endpoints for
    /// equality, and two spellings would let one host:port slip past it twice.
    #[test]
    fn canonicalizes_a_v4_mapped_address_to_its_v4_form() {
        assert_eq!(
            "::ffff:10.0.0.1".parse::<AdvertisedAddress>(),
            "10.0.0.1".parse::<AdvertisedAddress>()
        );
        assert_eq!(
            "::ffff:10.0.0.1"
                .parse::<AdvertisedAddress>()
                .unwrap()
                .authority(8090),
            "10.0.0.1:8090"
        );
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
mod advertised_for_tests {
    use super::*;

    fn node_with_selectors(selectors: Vec<AdvertisedAddressSelector>) -> ClusterNodeConfig {
        ClusterNodeConfig {
            name: "node-0".to_owned(),
            ip: "10.0.1.5".to_owned(),
            advertised_address: Some("203.0.113.10".to_owned()),
            advertised_addresses: selectors,
            replica_id: 0,
            ports: TransportPorts::default(),
        }
    }

    fn selector(client_cidr: &str, address: &str) -> AdvertisedAddressSelector {
        AdvertisedAddressSelector {
            client_cidr: client_cidr.to_owned(),
            address: address.to_owned(),
        }
    }

    fn resolved(node: ClusterNodeConfig) -> ResolvedClusterNode {
        ResolvedClusterNode::try_from(node).expect("a roster node the validator would accept")
    }

    fn ip(address: &str) -> IpAddr {
        address.parse().unwrap()
    }

    #[test]
    fn falls_back_to_advertised_address_without_selectors() {
        let node = node_with_selectors(Vec::new());
        assert_eq!(
            resolved(node).advertised_for(Some(ip("10.0.0.7"))),
            &AdvertisedAddress::Ip(ip("203.0.113.10"))
        );
    }

    #[test]
    fn falls_back_to_roster_ip_without_advertised_address() {
        let mut node = node_with_selectors(Vec::new());
        node.advertised_address = None;
        assert_eq!(
            resolved(node).advertised_for(Some(ip("10.0.0.7"))),
            &AdvertisedAddress::Ip(ip("10.0.1.5"))
        );
    }

    #[test]
    fn refuses_a_node_whose_ip_is_not_an_address() {
        // Nothing downstream carries a fallback for an unparsable source, so
        // the conversion is where such a node has to stop.
        let mut node = node_with_selectors(Vec::new());
        node.advertised_address = None;
        node.ip = "iggy_node".to_owned();
        assert!(ResolvedClusterNode::try_from(node).is_err());
    }

    #[test]
    fn refuses_a_node_advertising_the_unspecified_address() {
        // The conversion is reachable without the validator, so the "every
        // resolved node is dialable" invariant has to hold here rather than
        // rely on validation having run first.
        for wildcard in ["0.0.0.0", "::", "::ffff:0.0.0.0"] {
            let mut roster_ip = node_with_selectors(Vec::new());
            roster_ip.advertised_address = None;
            roster_ip.ip = wildcard.to_owned();
            assert!(
                ResolvedClusterNode::try_from(roster_ip).is_err(),
                "roster ip '{wildcard}' is not dialable"
            );

            let mut catch_all = node_with_selectors(Vec::new());
            catch_all.advertised_address = Some(wildcard.to_owned());
            assert!(
                ResolvedClusterNode::try_from(catch_all).is_err(),
                "advertised_address '{wildcard}' is not dialable"
            );

            let selectors = node_with_selectors(vec![selector("10.0.0.0/16", wildcard)]);
            assert!(
                ResolvedClusterNode::try_from(selectors).is_err(),
                "selector address '{wildcard}' is not dialable"
            );
        }
    }

    #[test]
    fn matching_selector_beats_advertised_address() {
        let node = node_with_selectors(vec![selector("10.0.0.0/16", "10.0.1.5")]);
        assert_eq!(
            resolved(node).advertised_for(Some(ip("10.0.200.7"))),
            &AdvertisedAddress::Ip(ip("10.0.1.5"))
        );
    }

    #[test]
    fn unmatched_client_falls_back_to_advertised_address() {
        let node = node_with_selectors(vec![selector("10.0.0.0/16", "10.0.1.5")]);
        assert_eq!(
            resolved(node).advertised_for(Some(ip("192.168.0.7"))),
            &AdvertisedAddress::Ip(ip("203.0.113.10"))
        );
    }

    #[test]
    fn unknown_client_ip_falls_back_to_advertised_address() {
        let node = node_with_selectors(vec![selector("10.0.0.0/16", "10.0.1.5")]);
        assert_eq!(
            resolved(node).advertised_for(None),
            &AdvertisedAddress::Ip(ip("203.0.113.10"))
        );
    }

    #[test]
    fn longest_prefix_wins_regardless_of_declaration_order() {
        let node = resolved(node_with_selectors(vec![
            selector("10.0.0.0/8", "10.255.255.1"),
            selector("10.0.0.0/16", "10.0.1.5"),
        ]));
        assert_eq!(
            node.advertised_for(Some(ip("10.0.200.7"))),
            &AdvertisedAddress::Ip(ip("10.0.1.5")),
            "the /16 must win over the /8 even though it is declared second"
        );
        assert_eq!(
            node.advertised_for(Some(ip("10.9.0.7"))),
            &AdvertisedAddress::Ip(ip("10.255.255.1")),
            "a client outside the /16 but inside the /8 must match the /8"
        );
    }

    #[test]
    fn equal_prefix_matches_resolve_deterministically_to_first_declared() {
        // No validated config reaches this state: these networks truncate to
        // one /16, which validation rejects as a duplicate. Pinned anyway so
        // a future relaxation of that rule cannot make resolution
        // order-dependent.
        let node = node_with_selectors(vec![
            selector("10.0.1.0/16", "10.0.1.5"),
            selector("10.0.2.0/16", "10.0.2.5"),
        ]);
        assert_eq!(
            resolved(node).advertised_for(Some(ip("10.0.200.7"))),
            &AdvertisedAddress::Ip(ip("10.0.1.5"))
        );
    }

    #[test]
    fn v4_mapped_v6_client_matches_v4_cidr() {
        // A dual-stack listener reports v4 peers as `::ffff:a.b.c.d`.
        let node = node_with_selectors(vec![selector("10.0.0.0/16", "10.0.1.5")]);
        assert_eq!(
            resolved(node).advertised_for(Some(ip("::ffff:10.0.0.7"))),
            &AdvertisedAddress::Ip(ip("10.0.1.5"))
        );
    }

    #[test]
    fn v4_mapped_v6_selector_cidr_matches_v4_client() {
        // The mirror case: the CIDR side canonicalizes at build, so
        // `::ffff:10.0.0.0/104` matches like `10.0.0.0/8` instead of being
        // a silently dead selector.
        let node = node_with_selectors(vec![selector("::ffff:10.0.0.0/104", "10.0.1.5")]);
        assert_eq!(
            resolved(node).advertised_for(Some(ip("10.0.0.7"))),
            &AdvertisedAddress::Ip(ip("10.0.1.5"))
        );
    }

    #[test]
    fn v6_selector_matches_v6_client() {
        let node = node_with_selectors(vec![selector("2001:db8::/32", "2001:db8::1")]);
        assert_eq!(
            resolved(node).advertised_for(Some(ip("2001:db8::7"))),
            &AdvertisedAddress::Ip(ip("2001:db8::1"))
        );
    }

    #[test]
    fn selector_address_may_be_a_hostname() {
        let node = node_with_selectors(vec![selector("10.0.0.0/16", "Broker.Internal.Example")]);
        assert_eq!(
            resolved(node).advertised_for(Some(ip("10.0.0.7"))),
            &AdvertisedAddress::Hostname("broker.internal.example".to_owned())
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
            advertised_address: None,
            advertised_addresses: Vec::new(),
            replica_id: id,
            ports: TransportPorts::default(),
        }
    }

    fn selector(client_cidr: &str, address: &str) -> AdvertisedAddressSelector {
        AdvertisedAddressSelector {
            client_cidr: client_cidr.to_owned(),
            address: address.to_owned(),
        }
    }

    fn cfg(nodes: Vec<ClusterNodeConfig>) -> ClusterConfig {
        ClusterConfig {
            enabled: true,
            name: "iggy-cluster".to_string(),
            heartbeat_timeout: default_heartbeat_timeout(),
            commit_broadcast_interval: default_commit_broadcast_interval(),
            prepare_retransmit_interval: default_prepare_retransmit_interval(),
            view_change_retransmit_interval: default_view_change_retransmit_interval(),
            view_change_status_timeout: default_view_change_status_timeout(),
            request_start_view_retransmit_interval: default_request_start_view_retransmit_interval(
            ),
            view_probe_attempts_max: default_view_probe_attempts_max(),
            repair_retry_interval: default_repair_retry_interval(),
            repair_chunk_max: default_repair_chunk_max(),
            superblock_wedged_fatal_timeout: default_superblock_wedged_fatal_timeout(),
            nodes,
            auth: ClusterAuthConfig::default(),
            tls: ClusterTlsConfig::default(),
            coordinator: ClusterCoordinatorConfig::default(),
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
    fn validate_rejects_zero_view_change_retransmit_interval() {
        // `0` / `disabled` / `unlimited` all collapse to zero and stall the
        // view-change retransmit timers.
        let mut c = cfg(vec![node("n1", 0), node("n2", 1)]);
        c.view_change_retransmit_interval = IggyDuration::new(Duration::ZERO);
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_zero_view_change_status_timeout() {
        let mut c = cfg(vec![node("n1", 0), node("n2", 1)]);
        c.view_change_status_timeout = IggyDuration::new(Duration::ZERO);
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_zero_request_start_view_retransmit_interval() {
        let mut c = cfg(vec![node("n1", 0), node("n2", 1)]);
        c.request_start_view_retransmit_interval = IggyDuration::new(Duration::ZERO);
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_view_change_status_below_retransmit_ratio() {
        // 3s is nonzero but still < 4x the 1s retransmit, so the ratio rule is
        // what rejects here, not the zero check.
        let mut c = cfg(vec![node("n1", 0), node("n2", 1)]);
        c.view_change_retransmit_interval = IggyDuration::new(Duration::from_secs(1));
        c.view_change_status_timeout = IggyDuration::new(Duration::from_secs(3));
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_accepts_view_change_status_at_retransmit_ratio() {
        // Exactly 4x the retransmit interval must pass.
        let mut c = cfg(vec![node("n1", 0), node("n2", 1)]);
        c.view_change_retransmit_interval = IggyDuration::new(Duration::from_secs(1));
        c.view_change_status_timeout = IggyDuration::new(Duration::from_secs(4));
        assert!(c.validate().is_ok());
    }

    #[test]
    fn validate_rejects_zero_view_probe_attempts_max() {
        let mut c = cfg(vec![node("n1", 0), node("n2", 1)]);
        c.view_probe_attempts_max = 0;
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_view_probe_attempts_above_ceiling() {
        let mut c = cfg(vec![node("n1", 0), node("n2", 1)]);
        c.view_probe_attempts_max = MAX_VIEW_PROBE_ATTEMPTS + 1;
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_accepts_view_probe_attempts_at_ceiling() {
        let mut c = cfg(vec![node("n1", 0), node("n2", 1)]);
        c.view_probe_attempts_max = MAX_VIEW_PROBE_ATTEMPTS;
        assert!(c.validate().is_ok());
    }

    #[test]
    fn validate_rejects_zero_repair_retry_interval() {
        // `0` / `disabled` / `unlimited` all collapse to zero and would wedge
        // stalled repair streams.
        let mut c = cfg(vec![node("n1", 0), node("n2", 1)]);
        c.repair_retry_interval = IggyDuration::new(Duration::ZERO);
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_zero_repair_chunk_max() {
        let mut c = cfg(vec![node("n1", 0), node("n2", 1)]);
        c.repair_chunk_max = 0;
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_repair_chunk_max_above_ceiling() {
        let mut c = cfg(vec![node("n1", 0), node("n2", 1)]);
        c.repair_chunk_max = MAX_REPAIR_CHUNK_MAX + 1;
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_accepts_repair_chunk_max_at_ceiling() {
        // Section-level validate only; the cross-section rule against
        // message_bus.peer_queue_capacity lives in the top-level validate.
        let mut c = cfg(vec![node("n1", 0), node("n2", 1)]);
        c.repair_chunk_max = MAX_REPAIR_CHUNK_MAX;
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
    fn validate_rejects_an_unspecified_node_ip() {
        for wildcard in ["0.0.0.0", "::", "::ffff:0.0.0.0"] {
            let mut nodes = vec![node("n1", 0), node("n2", 1)];
            nodes[0].ip = wildcard.to_owned();
            assert!(cfg(nodes).validate().is_err(), "{wildcard} is not dialable");
        }
    }

    #[test]
    fn validate_rejects_a_hostname_node_ip() {
        for hostname in ["iggy_leader", "node-1.example.com"] {
            let mut nodes = vec![node("n1", 0), node("n2", 1)];
            nodes[0].ip = hostname.to_owned();
            assert!(
                cfg(nodes).validate().is_err(),
                "'{hostname}' must not pass as a roster ip"
            );
        }
    }

    #[test]
    fn validate_rejects_an_unspecified_advertised_address() {
        for wildcard in ["0.0.0.0", "::", "::ffff:0.0.0.0"] {
            let mut nodes = vec![node("n1", 0), node("n2", 1)];
            nodes[0].advertised_address = Some(wildcard.to_owned());
            assert!(cfg(nodes).validate().is_err(), "{wildcard} is not dialable");
        }
    }

    #[test]
    fn validate_rejects_an_unspecified_selector_address() {
        for wildcard in ["0.0.0.0", "::", "::ffff:0.0.0.0"] {
            let mut nodes = vec![node("n1", 0), node("n2", 1)];
            nodes[0].advertised_addresses = vec![selector("10.0.0.0/16", wildcard)];
            assert!(cfg(nodes).validate().is_err(), "{wildcard} is not dialable");
        }
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

    // repair_chunk_max is also read by the unconditional top-level check
    // against message_bus.peer_queue_capacity, so its own bounds apply with
    // the cluster off too.
    #[test]
    fn validate_rejects_zero_repair_chunk_max_when_disabled() {
        let mut c = cfg(vec![]);
        c.enabled = false;
        c.repair_chunk_max = 0;
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_repair_chunk_max_above_ceiling_when_disabled() {
        let mut c = cfg(vec![]);
        c.enabled = false;
        c.repair_chunk_max = MAX_REPAIR_CHUNK_MAX + 1;
        assert!(c.validate().is_err());
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
    fn validate_accepts_selectors_with_distinct_cidrs() {
        let mut n1 = node("n1", 0);
        n1.ip = "10.0.0.1".to_owned();
        n1.advertised_address = Some("203.0.113.1".to_owned());
        n1.advertised_addresses = vec![
            selector("10.0.0.0/16", "10.0.0.1"),
            selector("10.0.0.0/8", "broker-1.internal.example"),
        ];
        n1.ports.tcp = Some(8090);

        assert!(cfg(vec![n1, node("n2", 1)]).validate().is_ok());
    }

    #[test]
    fn validate_rejects_malformed_selector_cidr() {
        for client_cidr in ["10.0.0.0", "10.0.0.0/33", "not-a-cidr", ""] {
            let mut n1 = node("n1", 0);
            n1.advertised_addresses = vec![selector(client_cidr, "10.0.0.1")];

            assert!(
                cfg(vec![n1, node("n2", 1)]).validate().is_err(),
                "client_cidr '{client_cidr}' must be rejected"
            );
        }
    }

    #[test]
    fn validate_rejects_malformed_selector_address() {
        for address in ["", "10.0.0.1:8090", "10.0.0.256", "iggy_node"] {
            let mut n1 = node("n1", 0);
            n1.advertised_addresses = vec![selector("10.0.0.0/16", address)];

            assert!(
                cfg(vec![n1, node("n2", 1)]).validate().is_err(),
                "selector address '{address}' must be rejected"
            );
        }
    }

    #[test]
    fn validate_rejects_duplicate_selector_cidr_within_a_node() {
        // `10.0.1.0/16` truncates to `10.0.0.0/16`: the two selectors match
        // the identical client set, so the second could never win LPM.
        let mut n1 = node("n1", 0);
        n1.advertised_addresses = vec![
            selector("10.0.0.0/16", "10.0.0.1"),
            selector("10.0.1.0/16", "10.0.0.2"),
        ];

        assert!(cfg(vec![n1, node("n2", 1)]).validate().is_err());
    }

    #[test]
    fn validate_accepts_selector_count_at_the_cap() {
        let mut n1 = node("n1", 0);
        n1.advertised_addresses = (0..MAX_ADVERTISED_SELECTORS)
            .map(|index| selector(&format!("10.{index}.0.0/16"), &format!("192.0.2.{index}")))
            .collect();

        assert!(cfg(vec![n1, node("n2", 1)]).validate().is_ok());
    }

    #[test]
    fn validate_rejects_selector_count_above_the_cap() {
        // The env-override path stops expanding selector indices at the same
        // ceiling, so a TOML roster exceeding it could never be replicated
        // byte-identically through env vars.
        let mut n1 = node("n1", 0);
        n1.advertised_addresses = (0..=MAX_ADVERTISED_SELECTORS)
            .map(|index| selector(&format!("10.{index}.0.0/16"), &format!("192.0.2.{index}")))
            .collect();

        assert!(cfg(vec![n1, node("n2", 1)]).validate().is_err());
    }

    #[test]
    fn validate_rejects_selector_endpoint_conflict_within_one_cidr() {
        let mut n1 = node("n1", 0);
        n1.ip = "10.0.0.1".to_owned();
        n1.advertised_addresses = vec![selector("10.0.0.0/16", "10.0.7.7")];
        n1.ports.tcp = Some(8090);
        let mut n2 = node("n2", 1);
        n2.ip = "10.0.0.2".to_owned();
        n2.advertised_addresses = vec![selector("10.0.0.0/16", "10.0.7.7")];
        n2.ports.tcp = Some(8090);

        assert!(cfg(vec![n1, n2]).validate().is_err());
    }

    #[test]
    fn validate_accepts_identical_selector_endpoint_across_different_cidrs() {
        // Reusing one host:port across DIFFERENT client networks is the
        // feature (e.g. each network NATs the address to its local node).
        let mut n1 = node("n1", 0);
        n1.ip = "10.0.0.1".to_owned();
        n1.advertised_addresses = vec![selector("10.1.0.0/16", "192.0.2.10")];
        n1.ports.tcp = Some(8090);
        let mut n2 = node("n2", 1);
        n2.ip = "10.0.0.2".to_owned();
        n2.advertised_addresses = vec![selector("10.2.0.0/16", "192.0.2.10")];
        n2.ports.tcp = Some(8090);

        assert!(cfg(vec![n1, n2]).validate().is_ok());
    }

    #[test]
    fn validate_rejects_v4_mapped_v6_selector_cidr_duplicating_its_v4_form() {
        // `::ffff:10.0.0.0/104` canonicalizes to `10.0.0.0/8` (matching how
        // client IPs canonicalize before LPM), so these two selectors match
        // the identical client set.
        let mut n1 = node("n1", 0);
        n1.advertised_addresses = vec![
            selector("10.0.0.0/8", "10.0.0.1"),
            selector("::ffff:10.0.0.0/104", "10.0.0.2"),
        ];

        assert!(cfg(vec![n1, node("n2", 1)]).validate().is_err());
    }

    #[test]
    fn validate_rejects_selector_endpoint_clashing_with_another_nodes_catch_all() {
        // The catch-all matches every client, so a 10.0.0.0/16 client would
        // resolve both nodes to 192.0.2.10:8090.
        let mut n1 = node("n1", 0);
        n1.ip = "10.0.0.1".to_owned();
        n1.advertised_addresses = vec![selector("10.0.0.0/16", "192.0.2.10")];
        n1.ports.tcp = Some(8090);
        let mut n2 = node("n2", 1);
        n2.ip = "10.0.0.2".to_owned();
        n2.advertised_address = Some("192.0.2.10".to_owned());
        n2.ports.tcp = Some(8090);

        assert!(cfg(vec![n1, n2]).validate().is_err());
    }

    #[test]
    fn validate_rejects_selector_endpoint_clashing_with_another_nodes_roster_ip() {
        // Without an advertised_address the roster ip backs the catch-all,
        // so the same cross-set conflict applies to it.
        let mut n1 = node("n1", 0);
        n1.ip = "10.0.0.1".to_owned();
        n1.advertised_addresses = vec![selector("10.0.0.0/16", "10.0.0.2")];
        n1.ports.tcp = Some(8090);
        let mut n2 = node("n2", 1);
        n2.ip = "10.0.0.2".to_owned();
        n2.ports.tcp = Some(8090);

        assert!(cfg(vec![n1, n2]).validate().is_err());
    }

    #[test]
    fn validate_rejects_identical_selector_endpoint_across_nested_cidrs() {
        // LPM runs per node, not cluster-wide: n1 has no longer prefix of
        // its own shadowing the /16 overlap, so a 10.0.0.0/16 client wins
        // n1's /8 and n2's /16, resolving both to 192.0.2.10:8090.
        let mut n1 = node("n1", 0);
        n1.ip = "10.0.0.1".to_owned();
        n1.advertised_addresses = vec![selector("10.0.0.0/8", "192.0.2.10")];
        n1.ports.tcp = Some(8090);
        let mut n2 = node("n2", 1);
        n2.ip = "10.0.0.2".to_owned();
        n2.advertised_addresses = vec![selector("10.0.0.0/16", "192.0.2.10")];
        n2.ports.tcp = Some(8090);

        assert!(cfg(vec![n1, n2]).validate().is_err());
    }

    #[test]
    fn validate_accepts_nested_cidr_reuse_shadowed_by_same_node_longer_prefix() {
        // n1's /16 selector shadows its /8 within 10.0.0.0/16, so n1's /8
        // entry wins only 10.0.0.0/8 minus 10.0.0.0/16 - disjoint from n2's
        // /16. No client resolves both nodes to 192.0.2.10:8090.
        let mut n1 = node("n1", 0);
        n1.ip = "10.0.0.1".to_owned();
        n1.advertised_addresses = vec![
            selector("10.0.0.0/8", "192.0.2.10"),
            selector("10.0.0.0/16", "192.0.2.20"),
        ];
        n1.ports.tcp = Some(8090);
        let mut n2 = node("n2", 1);
        n2.ip = "10.0.0.2".to_owned();
        n2.advertised_addresses = vec![selector("10.0.0.0/16", "192.0.2.10")];
        n2.ports.tcp = Some(8090);

        assert!(cfg(vec![n1, n2]).validate().is_ok());
    }

    #[test]
    fn validate_rejects_partially_shadowed_nested_cidr_reuse() {
        // n1's /24 shadow carves only part of the /16 overlap: a client in
        // 10.0.0.0/16 outside 10.0.0.0/24 still wins n1's /8 entry and n2's
        // /16 entry, resolving both nodes to 192.0.2.10:8090.
        let mut n1 = node("n1", 0);
        n1.ip = "10.0.0.1".to_owned();
        n1.advertised_addresses = vec![
            selector("10.0.0.0/8", "192.0.2.10"),
            selector("10.0.0.0/24", "192.0.2.20"),
        ];
        n1.ports.tcp = Some(8090);
        let mut n2 = node("n2", 1);
        n2.ip = "10.0.0.2".to_owned();
        n2.advertised_addresses = vec![selector("10.0.0.0/16", "192.0.2.10")];
        n2.ports.tcp = Some(8090);

        assert!(cfg(vec![n1, n2]).validate().is_err());
    }

    #[test]
    fn validate_accepts_catch_all_reuse_shadowed_by_same_node_selector() {
        // n1's /16 selector shadows its catch-all within 10.0.0.0/16, so
        // the catch-all never wins a client inside n2's /24. Without the
        // shadow the same pair conflicts (see
        // validate_rejects_selector_endpoint_clashing_with_another_nodes_catch_all).
        let mut n1 = node("n1", 0);
        n1.ip = "10.0.0.1".to_owned();
        n1.advertised_address = Some("192.0.2.10".to_owned());
        n1.advertised_addresses = vec![selector("10.0.0.0/16", "192.0.2.20")];
        n1.ports.tcp = Some(8090);
        let mut n2 = node("n2", 1);
        n2.ip = "10.0.0.2".to_owned();
        n2.advertised_addresses = vec![selector("10.0.0.0/24", "192.0.2.10")];
        n2.ports.tcp = Some(8090);

        assert!(cfg(vec![n1, n2]).validate().is_ok());
    }

    #[test]
    fn validate_accepts_selector_reusing_a_fully_shadowed_catch_all_address() {
        // n1's selectors cover both address families, so its catch-all wins
        // known peers nowhere; only unknown-peer clients reach it, and they
        // never match n2's selector.
        let mut n1 = node("n1", 0);
        n1.ip = "10.0.0.1".to_owned();
        n1.advertised_address = Some("192.0.2.10".to_owned());
        n1.advertised_addresses = vec![
            selector("0.0.0.0/0", "192.0.2.20"),
            selector("::/0", "192.0.2.30"),
        ];
        n1.ports.tcp = Some(8090);
        let mut n2 = node("n2", 1);
        n2.ip = "10.0.0.2".to_owned();
        n2.advertised_addresses = vec![selector("10.0.0.0/16", "192.0.2.10")];
        n2.ports.tcp = Some(8090);

        assert!(cfg(vec![n1, n2]).validate().is_ok());
    }

    #[test]
    fn validate_accepts_catch_all_spelling_another_nodes_selector_address_when_self_shadowed() {
        // Split-network NAT roster: n2's catch-all spells n1's 10/8 selector
        // address, but n2's own 10/8 selector shadows its catch-all inside
        // 10/8 (outside it n1 serves its own catch-all), so no client
        // resolves both nodes to 192.0.2.10:8090.
        let mut n1 = node("n1", 0);
        n1.ip = "10.0.0.1".to_owned();
        n1.advertised_addresses = vec![selector("10.0.0.0/8", "192.0.2.10")];
        n1.ports.tcp = Some(8090);
        let mut n2 = node("n2", 1);
        n2.ip = "10.0.0.2".to_owned();
        n2.advertised_address = Some("192.0.2.10".to_owned());
        n2.advertised_addresses = vec![selector("10.0.0.0/8", "192.0.2.20")];
        n2.ports.tcp = Some(8090);

        assert!(cfg(vec![n1, n2]).validate().is_ok());
    }

    #[test]
    fn validate_rejects_duplicate_catch_all_even_when_fully_shadowed() {
        // A client whose peer address the transport cannot produce always
        // falls to the catch-all, so duplicate catch-all conflict even when
        // selectors cover every known network.
        let mut n1 = node("n1", 0);
        n1.ip = "10.0.0.1".to_owned();
        n1.advertised_address = Some("192.0.2.10".to_owned());
        n1.advertised_addresses = vec![
            selector("0.0.0.0/0", "192.0.2.20"),
            selector("::/0", "192.0.2.30"),
        ];
        n1.ports.tcp = Some(8090);
        let mut n2 = node("n2", 1);
        n2.ip = "10.0.0.2".to_owned();
        n2.advertised_address = Some("192.0.2.10".to_owned());
        n2.advertised_addresses = vec![
            selector("0.0.0.0/0", "192.0.2.40"),
            selector("::/0", "192.0.2.50"),
        ];
        n2.ports.tcp = Some(8090);

        assert!(cfg(vec![n1, n2]).validate().is_err());
    }

    #[test]
    fn validate_accepts_selector_reusing_its_own_nodes_catch_all_address() {
        // Redundant but harmless: within one node the selector and the
        // catch-all cannot resolve a client to two different nodes.
        let mut n1 = node("n1", 0);
        n1.ip = "10.0.0.1".to_owned();
        n1.advertised_address = Some("192.0.2.10".to_owned());
        n1.advertised_addresses = vec![selector("10.0.0.0/16", "192.0.2.10")];
        n1.ports.tcp = Some(8090);
        let mut n2 = node("n2", 1);
        n2.ip = "10.0.0.2".to_owned();
        n2.ports.tcp = Some(8091);

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

    #[test]
    fn validate_accepts_valid_rotation_window() {
        let mut c = cfg(vec![node("n1", 0), node("n2", 1)]);
        c.auth.enabled = true;
        c.auth.shared_secret = "a".repeat(MIN_SHARED_SECRET_LEN);
        c.auth.previous_shared_secret = "b".repeat(MIN_SHARED_SECRET_LEN);
        assert!(c.validate().is_ok());
    }

    #[test]
    fn validate_rejects_short_previous_secret() {
        // Same typo guard as the primary key.
        let mut c = cfg(vec![node("n1", 0), node("n2", 1)]);
        c.auth.enabled = true;
        c.auth.shared_secret = "a".repeat(MIN_SHARED_SECRET_LEN);
        c.auth.previous_shared_secret = "b".repeat(MIN_SHARED_SECRET_LEN - 1);
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_rotation_window_equal_to_primary() {
        // An identical window is a no-op rotation: the operator rolled the
        // config without changing the key.
        let mut c = cfg(vec![node("n1", 0), node("n2", 1)]);
        c.auth.enabled = true;
        c.auth.shared_secret = "a".repeat(MIN_SHARED_SECRET_LEN);
        c.auth.previous_shared_secret = "a".repeat(MIN_SHARED_SECRET_LEN);
        assert!(c.validate().is_err());
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
