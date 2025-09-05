/* Licensed to the Apache Software Foundation (ASF) under one
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

extern crate sysinfo;

use super::server::{
    DataMaintenanceConfig, MessageSaverConfig, MessagesMaintenanceConfig, StateMaintenanceConfig,
    TelemetryConfig,
};
use super::sharding::{CpuAllocation, ShardingConfig};
use super::system::{CompressionConfig, MemoryPoolConfig, PartitionConfig};
use crate::configs::COMPONENT;
use crate::configs::server::{PersonalAccessTokenConfig, ServerConfig};
use crate::configs::system::SegmentConfig;
use crate::server_error::ConfigError;
use crate::streaming::segments::*;
use error_set::ErrContext;
use iggy_common::CompressionAlgorithm;
use iggy_common::IggyExpiry;
use iggy_common::MaxTopicSize;
use iggy_common::Validatable;
use std::thread::available_parallelism;
use tracing::error;

impl Validatable<ConfigError> for ServerConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        self.system
            .memory_pool
            .validate()
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to validate memory pool config")
            })?;
        self.data_maintenance
            .validate()
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to validate data maintenance config")
            })?;
        self.personal_access_token
            .validate()
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to validate personal access token config"
                )
            })?;
        self.system.segment.validate().with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to validate segment config")
        })?;
        self.system
            .compression
            .validate()
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to validate compression config")
            })?;
        self.telemetry.validate().with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to validate telemetry config")
        })?;
        self.system
            .sharding
            .validate()
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to validate sharding config")
            })?;

        let topic_size = match self.system.topic.max_size {
            MaxTopicSize::Custom(size) => Ok(size.as_bytes_u64()),
            MaxTopicSize::Unlimited => Ok(u64::MAX),
            MaxTopicSize::ServerDefault => Err(ConfigError::InvalidConfiguration),
        }?;

        if let IggyExpiry::ServerDefault = self.system.segment.message_expiry {
            return Err(ConfigError::InvalidConfiguration);
        }

        if self.http.enabled
            && let IggyExpiry::ServerDefault = self.http.jwt.access_token_expiry
        {
            return Err(ConfigError::InvalidConfiguration);
        }

        if topic_size < self.system.segment.size.as_bytes_u64() {
            return Err(ConfigError::InvalidConfiguration);
        }

        Ok(())
    }
}

impl Validatable<ConfigError> for CompressionConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        let compression_alg = &self.default_algorithm;
        if *compression_alg != CompressionAlgorithm::None {
            // TODO(numinex): Change this message once server side compression is fully developed.
            println!(
                "Server started with server-side compression enabled, using algorithm: {compression_alg}, this feature is not implemented yet!"
            );
        }

        Ok(())
    }
}

impl Validatable<ConfigError> for TelemetryConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if !self.enabled {
            return Ok(());
        }

        if self.service_name.trim().is_empty() {
            return Err(ConfigError::InvalidConfiguration);
        }

        if self.logs.endpoint.is_empty() {
            return Err(ConfigError::InvalidConfiguration);
        }

        if self.traces.endpoint.is_empty() {
            return Err(ConfigError::InvalidConfiguration);
        }

        Ok(())
    }
}

impl Validatable<ConfigError> for PartitionConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.messages_required_to_save < 32 {
            eprintln!(
                "Configured system.partition.messages_required_to_save {} is less than minimum {}",
                self.messages_required_to_save, 32
            );
            return Err(ConfigError::InvalidConfiguration);
        }

        if self.messages_required_to_save % 32 != 0 {
            eprintln!(
                "Configured system.partition.messages_required_to_save {} is not a multiple of 32",
                self.messages_required_to_save
            );
            return Err(ConfigError::InvalidConfiguration);
        }

        if self.size_of_messages_required_to_save < 512 {
            eprintln!(
                "Configured system.partition.size_of_messages_required_to_save {} is less than minimum {}",
                self.size_of_messages_required_to_save, 512
            );
            return Err(ConfigError::InvalidConfiguration);
        }

        if self.size_of_messages_required_to_save.as_bytes_u64() % 512 != 0 {
            eprintln!(
                "Configured system.partition.size_of_messages_required_to_save {} is not a multiple of 512 B",
                self.size_of_messages_required_to_save
            );
            return Err(ConfigError::InvalidConfiguration);
        }

        Ok(())
    }
}

impl Validatable<ConfigError> for SegmentConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.size > SEGMENT_MAX_SIZE_BYTES {
            eprintln!(
                "Configured system.segment.size {} B is greater than maximum {} B",
                self.size.as_bytes_u64(),
                SEGMENT_MAX_SIZE_BYTES
            );
            return Err(ConfigError::InvalidConfiguration);
        }

        if self.size.as_bytes_u64() % 512 != 0 {
            eprintln!(
                "Configured system.segment.size {} B is not a multiple of 512 B",
                self.size.as_bytes_u64()
            );
            return Err(ConfigError::InvalidConfiguration);
        }

        Ok(())
    }
}

impl Validatable<ConfigError> for MessageSaverConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.enabled && self.interval.is_zero() {
            return Err(ConfigError::InvalidConfiguration);
        }

        Ok(())
    }
}

impl Validatable<ConfigError> for DataMaintenanceConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        self.messages.validate().with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to validate messages maintenance config")
        })?;
        self.state.validate().with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to validate state maintenance config")
        })?;
        Ok(())
    }
}

impl Validatable<ConfigError> for MessagesMaintenanceConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.archiver_enabled && self.interval.is_zero() {
            return Err(ConfigError::InvalidConfiguration);
        }

        Ok(())
    }
}

impl Validatable<ConfigError> for StateMaintenanceConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.archiver_enabled && self.interval.is_zero() {
            return Err(ConfigError::InvalidConfiguration);
        }

        Ok(())
    }
}

impl Validatable<ConfigError> for PersonalAccessTokenConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.max_tokens_per_user == 0 {
            return Err(ConfigError::InvalidConfiguration);
        }

        if self.cleaner.enabled && self.cleaner.interval.is_zero() {
            return Err(ConfigError::InvalidConfiguration);
        }

        Ok(())
    }
}

impl Validatable<ConfigError> for MemoryPoolConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.enabled && self.size == 0 {
            error!(
                "Configured system.memory_pool.enabled is true and system.memory_pool.size is 0"
            );
            return Err(ConfigError::InvalidConfiguration);
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
            return Err(ConfigError::InvalidConfiguration);
        }

        if self.enabled && self.size.as_bytes_u64() % DEFAULT_PAGE_SIZE != 0 {
            error!(
                "Configured system.memory_pool.size {} B is not a multiple of default page size {} B",
                self.size.as_bytes_u64(),
                DEFAULT_PAGE_SIZE
            );
            return Err(ConfigError::InvalidConfiguration);
        }

        if self.enabled && self.bucket_capacity < MIN_BUCKET_CAPACITY {
            error!(
                "Configured system.memory_pool.buffers {} is less than minimum {}",
                self.bucket_capacity, MIN_BUCKET_CAPACITY
            );
            return Err(ConfigError::InvalidConfiguration);
        }

        if self.enabled && !self.bucket_capacity.is_power_of_two() {
            error!(
                "Configured system.memory_pool.buffers {} is not a power of 2",
                self.bucket_capacity
            );
            return Err(ConfigError::InvalidConfiguration);
        }

        Ok(())
    }
}

impl Validatable<ConfigError> for ShardingConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        let available_cpus = available_parallelism()
            .expect("Failed to get number of CPU cores")
            .get();

        match &self.cpu_allocation {
            CpuAllocation::All => Ok(()),
            CpuAllocation::Count(count) => {
                if *count == 0 {
                    eprintln!("Invalid sharding configuration: cpu_allocation count cannot be 0");
                    return Err(ConfigError::InvalidConfiguration);
                }
                if *count > available_cpus {
                    eprintln!(
                        "Invalid sharding configuration: cpu_allocation count {count} exceeds available CPU cores {available_cpus}"
                    );
                    return Err(ConfigError::InvalidConfiguration);
                }
                Ok(())
            }
            CpuAllocation::Range(start, end) => {
                if start >= end {
                    eprintln!(
                        "Invalid sharding configuration: cpu_allocation range {start}..{end} is invalid (start must be less than end)"
                    );
                    return Err(ConfigError::InvalidConfiguration);
                }
                if *end > available_cpus {
                    eprintln!(
                        "Invalid sharding configuration: cpu_allocation range {start}..{end} exceeds available CPU cores (max: {available_cpus})"
                    );
                    return Err(ConfigError::InvalidConfiguration);
                }
                Ok(())
            }
        }
    }
}
