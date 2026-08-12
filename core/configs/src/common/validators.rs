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
use super::server::{
    DataMaintenanceConfig, MessageSaverConfig, MessagesMaintenanceConfig, TelemetryConfig,
};
use super::server::{MemoryPoolConfig, PersonalAccessTokenConfig};
use super::system::SegmentConfig;
use super::system::{CompressionConfig, LoggingConfig, PartitionConfig};
use crate::ConfigurationError;
use cpu_allocation::{CpuAllocation, allowed_cpus};
use err_trail::ErrContext;
use iggy_common::CompressionAlgorithm;
use iggy_common::Validatable;
use std::thread::available_parallelism;
use tracing::warn;

/// 1 GiB max segment size.
pub const SEGMENT_MAX_SIZE_BYTES: u64 = 1024 * 1024 * 1024;

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

/// Validate a [`CpuAllocation`] against the machine's available parallelism
/// and, when pinning, the process affinity mask.
pub(crate) fn validate_cpu_allocation(
    cpu_allocation: &CpuAllocation,
    pin_cores: bool,
) -> Result<(), ConfigurationError> {
    let available_cpus = available_parallelism()
        .map_err(|_| {
            eprintln!("Failed to detect available CPU cores");
            ConfigurationError::InvalidConfigurationValue
        })?
        .get();

    match cpu_allocation {
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
            if *end - *start > available_cpus {
                eprintln!(
                    "Invalid sharding configuration: cpu_allocation range {start}..{end} yields {} shards, exceeding available CPU cores {available_cpus}",
                    *end - *start
                );
                return Err(ConfigurationError::InvalidConfigurationValue);
            }
            if !pin_cores {
                return Ok(());
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

#[cfg(test)]
mod cpu_allocation_tests {
    use super::*;

    #[test]
    fn inverted_range_is_rejected() {
        assert!(validate_cpu_allocation(&CpuAllocation::Range(2, 2), true).is_err());
    }

    #[test]
    fn pinned_range_within_allowed_set_is_accepted() {
        let first = allowed_cpus()[0];
        assert!(validate_cpu_allocation(&CpuAllocation::Range(first, first + 1), true).is_ok());
    }

    #[test]
    fn pinned_range_outside_allowed_set_is_rejected() {
        let past_last = allowed_cpus().last().copied().unwrap() + 1;
        assert!(
            validate_cpu_allocation(&CpuAllocation::Range(past_last, past_last + 1), true).is_err()
        );
    }

    #[test]
    fn pinned_range_wider_than_parallelism_is_rejected() {
        // Under a cgroup CPU quota the affinity mask stays full while
        // `available_parallelism` shrinks, so membership alone would
        // accept this; the shard-count cap must reject it.
        let first = allowed_cpus()[0];
        let available = available_parallelism().unwrap().get();
        assert!(
            validate_cpu_allocation(&CpuAllocation::Range(first, first + available + 1), true)
                .is_err()
        );
    }

    #[test]
    fn unpinned_range_is_capped_by_shard_count_not_core_ids() {
        // Core ids outside the machine are fine unpinned; only the
        // resulting shard count matters.
        let outside = 1 << 20;
        assert!(
            validate_cpu_allocation(&CpuAllocation::Range(outside, outside + 1), false).is_ok()
        );

        let available = available_parallelism().unwrap().get();
        assert!(validate_cpu_allocation(&CpuAllocation::Range(0, available + 1), false).is_err());
    }
}
