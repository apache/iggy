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
use super::system::{CompressionConfig, PartitionConfig};
use crate::configs::COMPONENT;
use crate::configs::server::{MemoryPoolConfig, PersonalAccessTokenConfig, ServerConfig};
use crate::configs::sharding::NumaTopology;
use crate::configs::system::SegmentConfig;
use crate::streaming::segments::*;
use err_trail::ErrContext;
use iggy_common::IggyExpiry;
use iggy_common::MaxTopicSize;
use iggy_common::Validatable;
use iggy_common::{CompressionAlgorithm, ConfigurationError};
use std::thread::available_parallelism;
use tracing::{error, warn};

impl Validatable<ConfigurationError> for ServerConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        self.system.memory_pool.validate().with_error(|error| {
            format!("{COMPONENT} (error: {error}) - failed to validate memory pool config")
        })?;
        self.data_maintenance.validate().with_error(|error| {
            format!("{COMPONENT} (error: {error}) - failed to validate data maintenance config")
        })?;
        self.personal_access_token.validate().with_error(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to validate personal access token config"
            )
        })?;
        self.system.segment.validate().with_error(|error| {
            format!("{COMPONENT} (error: {error}) - failed to validate segment config")
        })?;
        self.system.compression.validate().with_error(|error| {
            format!("{COMPONENT} (error: {error}) - failed to validate compression config")
        })?;
        self.telemetry.validate().with_error(|error| {
            format!("{COMPONENT} (error: {error}) - failed to validate telemetry config")
        })?;
        self.system.sharding.validate().with_error(|error| {
            format!("{COMPONENT} (error: {error}) - failed to validate sharding config")
        })?;
        self.cluster.validate().with_error(|error| {
            format!("{COMPONENT} (error: {error}) - failed to validate cluster config")
        })?;

        let topic_size = match self.system.topic.max_size {
            MaxTopicSize::Custom(size) => Ok(size.as_bytes_u64()),
            MaxTopicSize::Unlimited => Ok(u64::MAX),
            MaxTopicSize::ServerDefault => Err(ConfigurationError::InvalidConfigurationValue),
        }?;

        if let IggyExpiry::ServerDefault = self.system.segment.message_expiry {
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        if self.http.enabled
            && let IggyExpiry::ServerDefault = self.http.jwt.access_token_expiry
        {
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        if topic_size < self.system.segment.size.as_bytes_u64() {
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
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        if self.logs.endpoint.is_empty() {
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        if self.traces.endpoint.is_empty() {
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        Ok(())
    }
}

impl Validatable<ConfigurationError> for PartitionConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        if self.messages_required_to_save < 32 {
            error!(
                "Configured system.partition.messages_required_to_save {} is less than minimum {}",
                self.messages_required_to_save, 32
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        if !self.messages_required_to_save.is_multiple_of(32) {
            error!(
                "Configured system.partition.messages_required_to_save {} is not a multiple of 32",
                self.messages_required_to_save
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        if self.size_of_messages_required_to_save < 512 {
            error!(
                "Configured system.partition.size_of_messages_required_to_save {} is less than minimum {}",
                self.size_of_messages_required_to_save, 512
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        if !self
            .size_of_messages_required_to_save
            .as_bytes_u64()
            .is_multiple_of(512)
        {
            error!(
                "Configured system.partition.size_of_messages_required_to_save {} is not a multiple of 512 B",
                self.size_of_messages_required_to_save
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        Ok(())
    }
}

impl Validatable<ConfigurationError> for SegmentConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        if self.size > SEGMENT_MAX_SIZE_BYTES {
            error!(
                "Configured system.segment.size {} B is greater than maximum {} B",
                self.size.as_bytes_u64(),
                SEGMENT_MAX_SIZE_BYTES
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        if !self.size.as_bytes_u64().is_multiple_of(512) {
            error!(
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
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        Ok(())
    }
}

impl Validatable<ConfigurationError> for DataMaintenanceConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        self.messages.validate().with_error(|error| {
            format!("{COMPONENT} (error: {error}) - failed to validate messages maintenance config")
        })?;
        Ok(())
    }
}

impl Validatable<ConfigurationError> for MessagesMaintenanceConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        if self.cleaner_enabled && self.interval.is_zero() {
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        Ok(())
    }
}

impl Validatable<ConfigurationError> for PersonalAccessTokenConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        if self.max_tokens_per_user == 0 {
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        if self.cleaner.enabled && self.cleaner.interval.is_zero() {
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        Ok(())
    }
}

impl Validatable<ConfigurationError> for MemoryPoolConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        if self.enabled && self.size == 0 {
            error!(
                "Configured system.memory_pool.enabled is true and system.memory_pool.size is 0"
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        const MIN_POOL_SIZE: u64 = 512 * 1024 * 1024; // 512 MiB
        const MIN_BUCKET_CAPACITY: u32 = 128;
        const DEFAULT_PAGE_SIZE: u64 = 4096;

        if self.enabled && self.size < MIN_POOL_SIZE {
            error!(
                "Configured system.memory_pool.size {} B ({} MiB) is less than minimum {} B, ({} MiB)",
                self.size.as_bytes_u64(),
                self.size.as_bytes_u64() / (1024 * 1024),
                MIN_POOL_SIZE,
                MIN_POOL_SIZE / (1024 * 1024),
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        if self.enabled && !self.size.as_bytes_u64().is_multiple_of(DEFAULT_PAGE_SIZE) {
            error!(
                "Configured system.memory_pool.size {} B is not a multiple of default page size {} B",
                self.size.as_bytes_u64(),
                DEFAULT_PAGE_SIZE
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        if self.enabled && self.bucket_capacity < MIN_BUCKET_CAPACITY {
            error!(
                "Configured system.memory_pool.buffers {} is less than minimum {}",
                self.bucket_capacity, MIN_BUCKET_CAPACITY
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        if self.enabled && !self.bucket_capacity.is_power_of_two() {
            error!(
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
                    error!("Invalid sharding configuration: cpu_allocation count cannot be 0");
                    return Err(ConfigurationError::InvalidConfigurationValue);
                }
                if *count > available_cpus {
                    error!(
                        "Invalid sharding configuration: cpu_allocation count {count} exceeds available CPU cores {available_cpus}"
                    );
                    return Err(ConfigurationError::InvalidConfigurationValue);
                }
                Ok(())
            }
            CpuAllocation::Range(start, end) => {
                if start >= end {
                    error!(
                        "Invalid sharding configuration: cpu_allocation range {start}..{end} is invalid (start must be less than end)"
                    );
                    return Err(ConfigurationError::InvalidConfigurationValue);
                }
                if *end > available_cpus {
                    error!(
                        "Invalid sharding configuration: cpu_allocation range {start}..{end} exceeds available CPU cores (max: {available_cpus})"
                    );
                    return Err(ConfigurationError::InvalidConfigurationValue);
                }
                Ok(())
            }
            CpuAllocation::NumaAware(numa_config) => match NumaTopology::detect() {
                // TODO: dry the validation, already validate it from the shard allocation
                Ok(topology) => numa_config.validate(&topology).map_err(|e| {
                    error!("Invalid NUMA configuration: {}", e);
                    ConfigurationError::InvalidConfigurationValue
                }),
                Err(e) => {
                    error!("Failed to detect NUMA topology: {}", e);
                    error!("NUMA allocation requested but system doesn't support it");
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
            error!("Invalid cluster configuration: cluster name cannot be empty");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        // Validate current node name is not empty
        if self.node.current.name.trim().is_empty() {
            error!("Invalid cluster configuration: current node name cannot be empty");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        // Check for duplicate node names among other nodes
        let mut node_names = std::collections::HashSet::new();
        node_names.insert(self.node.current.name.clone());

        for node in &self.node.others {
            if !node_names.insert(node.name.clone()) {
                error!(
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
                error!("Invalid cluster configuration: node name cannot be empty");
                return Err(ConfigurationError::InvalidConfigurationValue);
            }

            // Validate IP is not empty
            if node.ip.trim().is_empty() {
                error!(
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
                        error!(
                            "Invalid cluster configuration: {} port cannot be 0 for node '{}'",
                            name, node.name
                        );
                        return Err(ConfigurationError::InvalidConfigurationValue);
                    }

                    // Check for port conflicts across nodes on the same IP
                    let endpoint = format!("{}:{}:{}", node.ip, name, port);
                    if !used_endpoints.insert(endpoint.clone()) {
                        error!(
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
