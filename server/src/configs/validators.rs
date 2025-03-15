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
    ArchiverConfig, DataMaintenanceConfig, MessageSaverConfig, MessagesMaintenanceConfig,
    StateMaintenanceConfig, TelemetryConfig,
};
use super::system::CompressionConfig;
use crate::archiver::ArchiverKindType;
use crate::configs::server::{PersonalAccessTokenConfig, ServerConfig};
use crate::configs::system::{CacheConfig, SegmentConfig};
use crate::configs::COMPONENT;
use crate::server_error::ConfigError;
use crate::streaming::segments::*;
use error_set::ErrContext;
use iggy::compression::compression_algorithm::CompressionAlgorithm;
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::expiry::IggyExpiry;
use iggy::utils::topic_size::MaxTopicSize;
use iggy::validatable::Validatable;
use sysinfo::{Pid, ProcessesToUpdate, System};

impl Validatable<ConfigError> for ServerConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        self.data_maintenance
            .validate()
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to validate data maintenance config")
            })?;
        self.personal_access_token
            .validate()
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to validate personal access token config")
            })?;
        self.system.segment.validate().with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to validate segment config")
        })?;
        self.system.cache.validate().with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to validate cache config")
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

        let topic_size = match self.system.topic.max_size {
            MaxTopicSize::Custom(size) => Ok(size.as_bytes_u64()),
            MaxTopicSize::Unlimited => Ok(u64::MAX),
            MaxTopicSize::ServerDefault => Err(ConfigError::InvalidConfiguration),
        }?;

        if let IggyExpiry::ServerDefault = self.system.segment.message_expiry {
            return Err(ConfigError::InvalidConfiguration);
        }

        if self.http.enabled {
            if let IggyExpiry::ServerDefault = self.http.jwt.access_token_expiry {
                return Err(ConfigError::InvalidConfiguration);
            }
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
                "Server started with server-side compression enabled, using algorithm: {}, this feature is not implemented yet!",
                compression_alg
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

impl Validatable<ConfigError> for CacheConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if !self.enabled {
            println!("Cache configuration -> cache is disabled.");
            return Ok(());
        }

        let limit_bytes = self.size.clone().into();
        let mut sys = System::new_all();
        sys.refresh_all();
        sys.refresh_processes(
            ProcessesToUpdate::Some(&[Pid::from_u32(std::process::id())]),
            true,
        );
        let total_memory = sys.total_memory();
        let free_memory = sys.free_memory();
        let cache_percentage = (limit_bytes.as_bytes_u64() as f64 / total_memory as f64) * 100.0;

        let pretty_cache_limit = limit_bytes.as_human_string();
        let pretty_total_memory = IggyByteSize::from(total_memory).as_human_string();
        let pretty_free_memory = IggyByteSize::from(free_memory).as_human_string();

        if limit_bytes > total_memory {
            return Err(ConfigError::CacheConfigValidationFailure);
        }

        if limit_bytes > (total_memory as f64 * 0.75) as u64 {
            println!(
                "Cache configuration -> cache size exceeds 75% of total memory. Set to: {} ({:.2}% of total memory: {}).",
                pretty_cache_limit, cache_percentage, pretty_total_memory
            );
        }

        println!(
            "Cache configuration -> cache size set to {} ({:.2}% of total memory: {}, free memory: {}).",
            pretty_cache_limit, cache_percentage, pretty_total_memory, pretty_free_memory
        );

        Ok(())
    }
}

impl Validatable<ConfigError> for SegmentConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.size > SEGMENT_MAX_SIZE_BYTES {
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
        self.archiver.validate().with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to validate archiver config")
        })?;
        self.messages.validate().with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to validate messages maintenance config")
        })?;
        self.state.validate().with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to validate state maintenance config")
        })?;
        Ok(())
    }
}

impl Validatable<ConfigError> for ArchiverConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if !self.enabled {
            return Ok(());
        }

        match self.kind {
            ArchiverKindType::Disk => {
                if self.disk.is_none() {
                    return Err(ConfigError::InvalidConfiguration);
                }

                let disk = self.disk.as_ref().unwrap();
                if disk.path.is_empty() {
                    return Err(ConfigError::InvalidConfiguration);
                }
                Ok(())
            }
            ArchiverKindType::S3 => {
                if self.s3.is_none() {
                    return Err(ConfigError::InvalidConfiguration);
                }

                let s3 = self.s3.as_ref().unwrap();
                if s3.key_id.is_empty() {
                    return Err(ConfigError::InvalidConfiguration);
                }

                if s3.key_secret.is_empty() {
                    return Err(ConfigError::InvalidConfiguration);
                }

                if s3.endpoint.is_none() && s3.region.is_none() {
                    return Err(ConfigError::InvalidConfiguration);
                }

                if s3.endpoint.as_deref().unwrap_or_default().is_empty()
                    && s3.region.as_deref().unwrap_or_default().is_empty()
                {
                    return Err(ConfigError::InvalidConfiguration);
                }

                if s3.bucket.is_empty() {
                    return Err(ConfigError::InvalidConfiguration);
                }
                Ok(())
            }
        }
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
