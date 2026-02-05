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

use super::cluster::ClusterConfig;
use super::server::{
    DataMaintenanceConfig, MessageSaverConfig, MessagesMaintenanceConfig, TelemetryConfig,
};
use super::sharding::{CpuAllocation, ShardingConfig};
use super::system::{CompressionConfig, LoggingConfig, PartitionConfig};
use crate::configs::COMPONENT;
use crate::configs::server::{MemoryPoolConfig, PersonalAccessTokenConfig, ServerConfig};
use crate::configs::sharding::NumaTopology;
use crate::configs::system::SegmentConfig;
use crate::streaming::segments::*;
use configs::ConfigurationError;
use err_trail::ErrContext;
use iggy_common::CompressionAlgorithm;
use iggy_common::IggyExpiry;
use iggy_common::MaxTopicSize;
use iggy_common::Validatable;
use std::thread::available_parallelism;
use tracing::warn;

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
            .expect("Failed to get number of CPU cores")
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
            CpuAllocation::NumaAware(numa_config) => match NumaTopology::detect() {
                // TODO: dry the validation, already validate it from the shard allocation
                Ok(topology) => numa_config.validate(&topology).map_err(|e| {
                    eprintln!("Invalid NUMA configuration: {}", e);
                    ConfigurationError::InvalidConfigurationValue
                }),
                Err(e) => {
                    eprintln!("Failed to detect NUMA topology: {}", e);
                    eprintln!("NUMA allocation requested but system doesn't support it");
                    Err(ConfigurationError::InvalidConfigurationValue)
                }
            },
        }
    }
}

impl Validatable<ConfigurationError> for ClusterConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        if !self.enabled {
            return Ok(());
        }

        // Validate cluster name is not empty
        if self.name.trim().is_empty() {
            eprintln!("Invalid cluster configuration: cluster name cannot be empty");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        // Validate current node name is not empty
        if self.node.current.name.trim().is_empty() {
            eprintln!("Invalid cluster configuration: current node name cannot be empty");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        // Check for duplicate node names among other nodes
        let mut node_names = std::collections::HashSet::new();
        node_names.insert(self.node.current.name.clone());

        for node in &self.node.others {
            if !node_names.insert(node.name.clone()) {
                eprintln!(
                    "Invalid cluster configuration: duplicate node name '{}' found",
                    node.name
                );
                return Err(ConfigurationError::InvalidConfigurationValue);
            }
        }

        // Validate each other node configuration
        let mut used_endpoints = std::collections::HashSet::new();
        for node in &self.node.others {
            // Validate node name is not empty
            if node.name.trim().is_empty() {
                eprintln!("Invalid cluster configuration: node name cannot be empty");
                return Err(ConfigurationError::InvalidConfigurationValue);
            }

            // Validate IP is not empty
            if node.ip.trim().is_empty() {
                eprintln!(
                    "Invalid cluster configuration: IP cannot be empty for node '{}'",
                    node.name
                );
                return Err(ConfigurationError::InvalidConfigurationValue);
            }

            // Validate transport ports if provided
            let port_list = [
                ("TCP", node.ports.tcp),
                ("QUIC", node.ports.quic),
                ("HTTP", node.ports.http),
                ("WebSocket", node.ports.websocket),
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

                    // Check for port conflicts across nodes on the same IP
                    let endpoint = format!("{}:{}:{}", node.ip, name, port);
                    if !used_endpoints.insert(endpoint.clone()) {
                        eprintln!(
                            "Invalid cluster configuration: port conflict - {}:{} is already used",
                            node.ip, port
                        );
                        return Err(ConfigurationError::InvalidConfigurationValue);
                    }
                }
            }
        }

        Ok(())
    }
}
