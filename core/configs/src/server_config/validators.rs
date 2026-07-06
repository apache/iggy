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
use super::server::{
    DataMaintenanceConfig, MessageSaverConfig, MessagesMaintenanceConfig, TelemetryConfig,
};
use super::server::{MemoryPoolConfig, PersonalAccessTokenConfig, ServerConfig};
use super::sharding::{
    CpuAllocation, INBOX_CAPACITY_MAX, RECONCILE_PERIODIC_INTERVAL_MAX, SHUTDOWN_DRAIN_TIMEOUT_MAX,
    SHUTDOWN_POLL_INTERVAL_MAX, ShardingConfig,
};
use super::system::SegmentConfig;
use super::system::{CompressionConfig, LoggingConfig, PartitionConfig};
use crate::ConfigurationError;
use cpu_allocation::allowed_cpus;
use err_trail::ErrContext;
use iggy_common::CompressionAlgorithm;
use iggy_common::IggyExpiry;
use iggy_common::MaxTopicSize;
use iggy_common::Validatable;
use std::thread::available_parallelism;
use tracing::warn;

/// 1 GiB max segment size. Canonical definition; re-exported by core/server streaming.
pub const SEGMENT_MAX_SIZE_BYTES: u64 = 1024 * 1024 * 1024;

/// Return `Err(reason)` when `alloc` would yield a host-dependent shard
/// count, disqualifying it for cluster mode where every node must derive
/// the same count from its byte-identical config.
///
/// Deterministic variants (`Count(n)`, `Range(s, e)`, explicit
/// `NumaAware`) return `Ok`.
fn host_dependent_cpu_allocation(alloc: &CpuAllocation) -> Result<(), &'static str> {
    match alloc {
        CpuAllocation::All => Err("'all' (follows host CPU count)"),
        CpuAllocation::NumaAware(numa) if numa.nodes.is_empty() && numa.cores_per_node == 0 => {
            Err("'numa:auto' (follows host NUMA topology)")
        }
        CpuAllocation::Count(_) | CpuAllocation::Range(_, _) | CpuAllocation::NumaAware(_) => {
            Ok(())
        }
    }
}

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
        self.system
            .compression
            .validate()
            .error(|e: &ConfigurationError| {
                format!("{COMPONENT} (error: {e}) - failed to validate compression config")
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

        // Cluster consensus routing (`calculate_shard_from_consensus_ns`)
        // hashes namespaces modulo the local shard count. Every node must
        // agree on that count or control-plane messages (StartViewChange,
        // DoViewChange, StartView, Commit) route to different shards on
        // different nodes, splitting the view-change quorum.
        //
        // Because `cluster.nodes` is byte-identical across every host, the
        // only way shard counts can drift is if `system.sharding.cpu_allocation`
        // depends on host topology. We can't prove divergence from a single
        // node at load time (peers may be homogeneous), so this is a warning
        // rather than a hard error; operators running heterogeneous hardware
        // must pin a deterministic value themselves.
        if self.cluster.enabled
            && let Err(reason) = host_dependent_cpu_allocation(&self.system.sharding.cpu_allocation)
        {
            warn!(
                "cluster.enabled = true with host-dependent system.sharding.cpu_allocation ({reason}); \
                 if peers resolve this to different shard counts, view-change quorum will split. \
                 Pin a deterministic value (count, explicit range, or explicit numa) on heterogeneous hardware."
            );
        }

        self.system
            .logging
            .validate()
            .error(|e: &ConfigurationError| {
                format!("{COMPONENT} (error: {e}) - failed to validate logging config")
            })?;

        let topic_size = match self.system.topic.max_size {
            MaxTopicSize::Custom(size) => Ok(size.as_bytes_u64()),
            MaxTopicSize::Unlimited => Ok(u64::MAX),
            MaxTopicSize::ServerDefault => {
                eprintln!("system.topic.max_size cannot be ServerDefault in server config");
                Err(ConfigurationError::InvalidConfigurationValue)
            }
        }?;

        if let IggyExpiry::ServerDefault = self.system.topic.message_expiry {
            eprintln!("system.topic.message_expiry cannot be ServerDefault in server config");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        if self.http.enabled
            && let IggyExpiry::ServerDefault = self.http.jwt.access_token_expiry
        {
            eprintln!("http.jwt.access_token_expiry cannot be ServerDefault when HTTP is enabled");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        if topic_size < self.system.segment.size.as_bytes_u64() {
            eprintln!(
                "system.topic.max_size ({} B) must be >= system.segment.size ({} B)",
                topic_size,
                self.system.segment.size.as_bytes_u64()
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        Ok(())
    }
}

impl Validatable<ConfigurationError> for CompressionConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        let compression_alg = &self.default_algorithm;
        if *compression_alg != CompressionAlgorithm::None {
            // TODO(numinex): Change this message once server side compression is fully developed.
            warn!(
                "Server started with server-side compression enabled, using algorithm: {compression_alg}, this feature is not implemented yet!"
            );
        }

        Ok(())
    }
}

impl Validatable<ConfigurationError> for TelemetryConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        if !self.enabled {
            return Ok(());
        }

        if self.service_name.trim().is_empty() {
            eprintln!("telemetry.service_name cannot be empty when telemetry is enabled");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        if self.logs.endpoint.is_empty() {
            eprintln!("telemetry.logs.endpoint cannot be empty when telemetry is enabled");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        if self.traces.endpoint.is_empty() {
            eprintln!("telemetry.traces.endpoint cannot be empty when telemetry is enabled");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        Ok(())
    }
}

impl Validatable<ConfigurationError> for PartitionConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        if self.messages_required_to_save == 0 {
            eprintln!("Configured system.partition.messages_required_to_save cannot be 0");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        Ok(())
    }
}

impl Validatable<ConfigurationError> for SegmentConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        if self.size > SEGMENT_MAX_SIZE_BYTES {
            eprintln!(
                "Configured system.segment.size {} B is greater than maximum {} B",
                self.size.as_bytes_u64(),
                SEGMENT_MAX_SIZE_BYTES
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        if !self.size.as_bytes_u64().is_multiple_of(512) {
            eprintln!(
                "Configured system.segment.size {} B is not a multiple of 512 B",
                self.size.as_bytes_u64()
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        Ok(())
    }
}

impl Validatable<ConfigurationError> for MessageSaverConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        if self.enabled && self.interval.is_zero() {
            eprintln!("message_saver.interval cannot be zero when message_saver is enabled");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        Ok(())
    }
}

impl Validatable<ConfigurationError> for DataMaintenanceConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        self.messages.validate().error(|e: &ConfigurationError| {
            format!("{COMPONENT} (error: {e}) - failed to validate messages maintenance config")
        })?;
        Ok(())
    }
}

impl Validatable<ConfigurationError> for MessagesMaintenanceConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        if self.cleaner_enabled && self.interval.is_zero() {
            eprintln!("data_maintenance.messages.interval cannot be zero when cleaner is enabled");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        Ok(())
    }
}

impl Validatable<ConfigurationError> for PersonalAccessTokenConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        if self.max_tokens_per_user == 0 {
            eprintln!("personal_access_token.max_tokens_per_user cannot be 0");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        if self.cleaner.enabled && self.cleaner.interval.is_zero() {
            eprintln!(
                "personal_access_token.cleaner.interval cannot be zero when cleaner is enabled"
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        Ok(())
    }
}

impl Validatable<ConfigurationError> for LoggingConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        if self.level.is_empty() {
            eprintln!("system.logging.level is supposed be configured");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        if self.retention.as_secs() < 1 {
            eprintln!(
                "Configured system.logging.retention {} is less than minimum 1 second",
                self.retention
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        if self.rotation_check_interval.as_secs() < 1 {
            eprintln!(
                "Configured system.logging.rotation_check_interval {} is less than minimum 1 second",
                self.rotation_check_interval
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        let max_total_size_unlimited = self.max_total_size.as_bytes_u64() == 0;
        if !max_total_size_unlimited
            && self.max_file_size.as_bytes_u64() > self.max_total_size.as_bytes_u64()
        {
            eprintln!(
                "Configured system.logging.max_total_size {} is less than system.logging.max_file_size {}",
                self.max_total_size, self.max_file_size
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        Ok(())
    }
}

impl Validatable<ConfigurationError> for MemoryPoolConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        if self.enabled && self.size == 0 {
            eprintln!(
                "Configured system.memory_pool.enabled is true and system.memory_pool.size is 0"
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        const MIN_POOL_SIZE: u64 = 512 * 1024 * 1024; // 512 MiB
        const MIN_BUCKET_CAPACITY: u32 = 128;
        const DEFAULT_PAGE_SIZE: u64 = 4096;

        if self.enabled && self.size < MIN_POOL_SIZE {
            eprintln!(
                "Configured system.memory_pool.size {} B ({} MiB) is less than minimum {} B, ({} MiB)",
                self.size.as_bytes_u64(),
                self.size.as_bytes_u64() / (1024 * 1024),
                MIN_POOL_SIZE,
                MIN_POOL_SIZE / (1024 * 1024),
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        if self.enabled && !self.size.as_bytes_u64().is_multiple_of(DEFAULT_PAGE_SIZE) {
            eprintln!(
                "Configured system.memory_pool.size {} B is not a multiple of default page size {} B",
                self.size.as_bytes_u64(),
                DEFAULT_PAGE_SIZE
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        if self.enabled && self.bucket_capacity < MIN_BUCKET_CAPACITY {
            eprintln!(
                "Configured system.memory_pool.buffers {} is less than minimum {}",
                self.bucket_capacity, MIN_BUCKET_CAPACITY
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        if self.enabled && !self.bucket_capacity.is_power_of_two() {
            eprintln!(
                "Configured system.memory_pool.buffers {} is not a power of 2",
                self.bucket_capacity
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        Ok(())
    }
}

impl Validatable<ConfigurationError> for ShardingConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        if self.inbox_capacity == 0 {
            eprintln!(
                "Invalid sharding configuration: inbox_capacity must be > 0 (crossfire silently \
                 rounds 0 to 1, masking config errors)"
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.inbox_capacity > INBOX_CAPACITY_MAX {
            eprintln!(
                "Invalid sharding configuration: inbox_capacity {} exceeds the {} cap (each \
                 shard preallocates a channel of this size; oversizing here OOMs the process at \
                 boot)",
                self.inbox_capacity, INBOX_CAPACITY_MAX
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        let drain = self.shutdown_drain_timeout.get_duration();
        if drain.is_zero() {
            eprintln!(
                "Invalid sharding configuration: shutdown_drain_timeout must be > 0 (a zero \
                 budget force-tears the bus mid-WAL-fsync on every shutdown)"
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if drain > SHUTDOWN_DRAIN_TIMEOUT_MAX {
            eprintln!(
                "Invalid sharding configuration: shutdown_drain_timeout {:?} exceeds the {:?} \
                 cap (an unbounded drain wedges process exit on bus stall)",
                drain, SHUTDOWN_DRAIN_TIMEOUT_MAX
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        let poll = self.shutdown_poll_interval.get_duration();
        if poll.is_zero() {
            eprintln!(
                "Invalid sharding configuration: shutdown_poll_interval must be > 0 (a zero \
                 cadence busy-loops every shard's watchdog and metadata-handoff poller)"
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if poll > SHUTDOWN_POLL_INTERVAL_MAX {
            eprintln!(
                "Invalid sharding configuration: shutdown_poll_interval {:?} exceeds the {:?} \
                 cap (a coarse cadence stalls Ctrl-C handling and metadata handoff abort)",
                poll, SHUTDOWN_POLL_INTERVAL_MAX
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if poll > drain {
            eprintln!(
                "Invalid sharding configuration: shutdown_poll_interval {:?} must be <= \
                 shutdown_drain_timeout {:?} (a poll cadence coarser than the drain budget makes \
                 the shutdown flag effectively unobservable)",
                poll, drain
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        let reconcile = self.reconcile_periodic_interval.get_duration();
        if reconcile.is_zero() {
            eprintln!(
                "Invalid sharding configuration: reconcile_periodic_interval resolves to zero. \
                 Note that \"0\", \"none\", \"unlimited\", and \"disabled\" all parse to zero. The \
                 periodic reconcile tick is a safety net for dropped commit-wakes and cannot be \
                 turned off; set a positive duration (default \"1s\", max {RECONCILE_PERIODIC_INTERVAL_MAX:?})."
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if reconcile > RECONCILE_PERIODIC_INTERVAL_MAX {
            eprintln!(
                "Invalid sharding configuration: reconcile_periodic_interval {:?} exceeds the \
                 {:?} cap (a long tick makes post-failure convergence latency operator-visible)",
                reconcile, RECONCILE_PERIODIC_INTERVAL_MAX
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        let available_cpus = available_parallelism()
            .map_err(|_| {
                eprintln!("Failed to detect available CPU cores");
                ConfigurationError::InvalidConfigurationValue
            })?
            .get();

        match &self.cpu_allocation {
            CpuAllocation::All => Ok(()),
            CpuAllocation::Count(count) => {
                if *count == 0 {
                    eprintln!("Invalid sharding configuration: cpu_allocation count cannot be 0");
                    return Err(ConfigurationError::InvalidConfigurationValue);
                }
                if *count > available_cpus {
                    eprintln!(
                        "Invalid sharding configuration: cpu_allocation count {count} exceeds available CPU cores {available_cpus}"
                    );
                    return Err(ConfigurationError::InvalidConfigurationValue);
                }
                Ok(())
            }
            CpuAllocation::Range(start, end) => {
                if start >= end {
                    eprintln!(
                        "Invalid sharding configuration: cpu_allocation range {start}..{end} is invalid (start must be less than end)"
                    );
                    return Err(ConfigurationError::InvalidConfigurationValue);
                }
                if !self.pin_cores {
                    if *end - *start > available_cpus {
                        eprintln!(
                            "Invalid sharding configuration: cpu_allocation range {start}..{end} yields {} shards, exceeding available CPU cores {available_cpus}",
                            *end - *start
                        );
                        return Err(ConfigurationError::InvalidConfigurationValue);
                    }
                    return Ok(());
                }
                if *end - *start > available_cpus {
                    eprintln!(
                        "Invalid sharding configuration: cpu_allocation range {start}..{end} yields {} shards, exceeding available CPU cores {available_cpus}",
                        *end - *start
                    );
                    return Err(ConfigurationError::InvalidConfigurationValue);
                }
                let allowed = allowed_cpus();
                if let Some(cpu) = (*start..*end).find(|cpu| !allowed.contains(cpu)) {
                    eprintln!(
                        "Invalid sharding configuration: cpu_allocation range {start}..{end} includes CPU {cpu}, which is outside the set of cores allowed for this process (affinity/cpuset mask)"
                    );
                    return Err(ConfigurationError::InvalidConfigurationValue);
                }
                Ok(())
            }
            // NUMA topology validation requires hwlocality (runtime dep).
            // Full NUMA validation happens in shard_allocator at startup.
            CpuAllocation::NumaAware(_) => Ok(()),
        }
    }
}

/// Length floor for the replica-auth PSK, in raw bytes. The 32-byte MAC key
/// is KDF-derived from these bytes at use-site, so any encoding clearing this
/// length is accepted.
const MIN_SHARED_SECRET_LEN: usize = 32;

impl Validatable<ConfigurationError> for ClusterConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        if !self.enabled {
            return Ok(());
        }

        if self.name.trim().is_empty() {
            eprintln!("Invalid cluster configuration: cluster name cannot be empty");
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
mod cluster_validate_tests {
    use super::*;
    use crate::server_config::cluster::{
        ClusterAuthConfig, ClusterConfig, ClusterNodeConfig, ClusterTlsConfig, TransportPorts,
    };

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
            nodes,
            auth: ClusterAuthConfig::default(),
            tls: ClusterTlsConfig::default(),
        }
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

#[cfg(test)]
mod cluster_shards_count_determinism_tests {
    use super::*;
    use crate::server_config::sharding::NumaConfig;

    #[test]
    fn all_is_rejected() {
        let err = host_dependent_cpu_allocation(&CpuAllocation::All).unwrap_err();
        assert!(err.contains("all"));
    }

    #[test]
    fn numa_auto_is_rejected() {
        let err = host_dependent_cpu_allocation(&CpuAllocation::NumaAware(NumaConfig::default()))
            .unwrap_err();
        assert!(err.contains("numa:auto"));
    }

    #[test]
    fn count_is_accepted() {
        assert!(host_dependent_cpu_allocation(&CpuAllocation::Count(4)).is_ok());
    }

    #[test]
    fn range_is_accepted() {
        assert!(host_dependent_cpu_allocation(&CpuAllocation::Range(0, 4)).is_ok());
    }

    #[test]
    fn explicit_numa_is_accepted() {
        let numa = NumaConfig {
            nodes: vec![0, 1],
            cores_per_node: 4,
            avoid_hyperthread: true,
        };
        assert!(host_dependent_cpu_allocation(&CpuAllocation::NumaAware(numa)).is_ok());
    }
}

#[cfg(test)]
mod sharding_shutdown_knob_tests {
    use super::*;
    use crate::server_config::sharding::{SHUTDOWN_DRAIN_TIMEOUT_MAX, SHUTDOWN_POLL_INTERVAL_MAX};
    use iggy_common::IggyDuration;
    use std::time::Duration;

    #[test]
    fn defaults_validate() {
        assert!(ShardingConfig::default().validate().is_ok());
    }

    #[test]
    fn zero_drain_is_rejected() {
        let cfg = ShardingConfig {
            shutdown_drain_timeout: IggyDuration::new(Duration::ZERO),
            ..ShardingConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn over_cap_drain_is_rejected() {
        let cfg = ShardingConfig {
            shutdown_drain_timeout: IggyDuration::new(
                SHUTDOWN_DRAIN_TIMEOUT_MAX + Duration::from_secs(1),
            ),
            ..ShardingConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn zero_poll_is_rejected() {
        let cfg = ShardingConfig {
            shutdown_poll_interval: IggyDuration::new(Duration::ZERO),
            ..ShardingConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn over_cap_poll_is_rejected() {
        let cfg = ShardingConfig {
            shutdown_poll_interval: IggyDuration::new(
                SHUTDOWN_POLL_INTERVAL_MAX + Duration::from_secs(1),
            ),
            ..ShardingConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn poll_greater_than_drain_is_rejected() {
        let cfg = ShardingConfig {
            shutdown_drain_timeout: IggyDuration::new(Duration::from_millis(20)),
            shutdown_poll_interval: IggyDuration::new(Duration::from_millis(50)),
            ..ShardingConfig::default()
        };
        assert!(cfg.validate().is_err());
    }
}

#[cfg(test)]
mod sharding_cpu_range_tests {
    use super::*;

    #[test]
    fn inverted_range_is_rejected() {
        let cfg = ShardingConfig {
            cpu_allocation: CpuAllocation::Range(2, 2),
            ..ShardingConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn pinned_range_within_allowed_set_is_accepted() {
        let first = allowed_cpus()[0];
        let cfg = ShardingConfig {
            cpu_allocation: CpuAllocation::Range(first, first + 1),
            ..ShardingConfig::default()
        };
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn pinned_range_outside_allowed_set_is_rejected() {
        let past_last = allowed_cpus().last().copied().unwrap() + 1;
        let cfg = ShardingConfig {
            cpu_allocation: CpuAllocation::Range(past_last, past_last + 1),
            ..ShardingConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn pinned_range_wider_than_parallelism_is_rejected() {
        // Under a cgroup CPU quota the affinity mask stays full while
        // `available_parallelism` shrinks, so membership alone would
        // accept this; the shard-count cap must reject it.
        let first = allowed_cpus()[0];
        let available = available_parallelism().unwrap().get();
        let cfg = ShardingConfig {
            cpu_allocation: CpuAllocation::Range(first, first + available + 1),
            ..ShardingConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn unpinned_range_is_capped_by_shard_count_not_core_ids() {
        // Core ids outside the machine are fine unpinned; only the
        // resulting shard count matters.
        let cfg = ShardingConfig {
            cpu_allocation: CpuAllocation::Range(1 << 20, (1 << 20) + 1),
            pin_cores: false,
            ..ShardingConfig::default()
        };
        assert!(cfg.validate().is_ok());

        let available = available_parallelism().unwrap().get();
        let cfg = ShardingConfig {
            cpu_allocation: CpuAllocation::Range(0, available + 1),
            pin_cores: false,
            ..ShardingConfig::default()
        };
        assert!(cfg.validate().is_err());
    }
}
