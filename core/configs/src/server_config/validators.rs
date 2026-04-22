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

use super::COMPONENT;
use super::cluster::ClusterConfig;
use super::server::{
    DataMaintenanceConfig, MessageSaverConfig, MessagesMaintenanceConfig, TelemetryConfig,
};
use super::server::{MemoryPoolConfig, PersonalAccessTokenConfig, ServerConfig};
use super::sharding::{CpuAllocation, ShardingConfig};
use super::system::SegmentConfig;
use super::system::{CompressionConfig, LoggingConfig, PartitionConfig};
use crate::ConfigurationError;
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
        // depends on host topology. Reject those variants in cluster mode.
        if self.cluster.enabled
            && let Err(reason) = host_dependent_cpu_allocation(&self.system.sharding.cpu_allocation)
        {
            eprintln!(
                "cluster.enabled = true requires a deterministic system.sharding.cpu_allocation (count or explicit range/numa); \
                 got {reason}. Cluster nodes must agree on shard count; host-dependent allocation splits the view-change quorum."
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
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
                if *end > available_cpus {
                    eprintln!(
                        "Invalid sharding configuration: cpu_allocation range {start}..{end} exceeds available CPU cores (max: {available_cpus})"
                    );
                    return Err(ConfigurationError::InvalidConfigurationValue);
                }
                Ok(())
            }
            // NUMA topology validation requires hwlocality (runtime dep).
            // Full NUMA validation happens in server::shard_allocator at startup.
            CpuAllocation::NumaAware(_) => Ok(()),
        }
    }
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

        Ok(())
    }
}

#[cfg(test)]
mod cluster_validate_tests {
    use super::*;
    use crate::server_config::cluster::{ClusterConfig, ClusterNodeConfig, TransportPorts};

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
