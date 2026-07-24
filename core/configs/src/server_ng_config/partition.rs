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

//! On-disk schema for the per-partition consensus plane.
//!
//! One capacity knob previously hardcoded in the runtime crates:
//!
//! - `prepare_queue_depth` -> `consensus::PIPELINE_PREPARE_QUEUE_MAX`
//!   (the pipeline's in-flight prepare bound; submits beyond it bounce
//!   with the transient prepare-queue-full path the SDK retries)
//!
//! Distinct from `[metadata]` (a single, shard-0-global VSR plane) because
//! partition pipelines exist PER PARTITION. The default mirrors the runtime
//! constant so a default deployment is byte-identical; the ceiling is far
//! below metadata's because the request queue (`depth * 2` slots) pins full
//! inbound produce batches, so pinned memory scales with the partition count
//! (see [`MAX_PARTITION_PREPARE_QUEUE_DEPTH`]).
//!
//! The default is a duplicated literal rather than an import so
//! `core/configs` does not grow a build-time edge onto `core/consensus`
//! (mirroring [`super::metadata`]). `core/server-ng`'s bootstrap pins the
//! literal against the runtime constant with a static assert.

use super::COMPONENT_NG;
use crate::ConfigurationError;
use configs::ConfigEnv;
use iggy_common::Validatable;
use serde::{Deserialize, Serialize};

/// Mirrors `consensus::PIPELINE_PREPARE_QUEUE_MAX`.
pub const DEFAULT_PARTITION_PREPARE_QUEUE_DEPTH: usize = 32;

/// Upper bound on `prepare_queue_depth`. Unlike the single metadata pipeline,
/// a pipeline exists per partition, and each queued request pins a full
/// inbound produce batch (a 4 KiB floor up to megabytes). Worst-case pinned
/// memory therefore scales as `depth * 2 * partition_count * batch_size`, so
/// this ceiling sits far below metadata's 4096: it is a typo guard, not a
/// sizing endorsement.
pub const MAX_PARTITION_PREPARE_QUEUE_DEPTH: usize = 256;

/// Capacity tunables for the per-partition consensus plane.
#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct PartitionConfig {
    /// Depth of a partition's prepare queue: how many uncommitted produce /
    /// consumer-offset ops may be in flight at once for that partition.
    /// Submits beyond it are rejected with the transient prepare-queue-full
    /// path the SDK retries. Applies to every partition; raising it multiplies
    /// pinned request-buffer memory by the partition count.
    pub prepare_queue_depth: usize,
}

impl Validatable<ConfigurationError> for PartitionConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        if self.prepare_queue_depth == 0 {
            eprintln!("{COMPONENT_NG} partition.prepare_queue_depth must be > 0");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.prepare_queue_depth > MAX_PARTITION_PREPARE_QUEUE_DEPTH {
            eprintln!(
                "{COMPONENT_NG} partition.prepare_queue_depth ({}) exceeds the maximum ({MAX_PARTITION_PREPARE_QUEUE_DEPTH})",
                self.prepare_queue_depth
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_impl_validates() {
        // `Default` reads the shipped config.toml; the pristine deployment
        // must validate.
        assert!(PartitionConfig::default().validate().is_ok());
    }

    #[test]
    fn rejects_zero() {
        let config = PartitionConfig {
            prepare_queue_depth: 0,
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_above_ceiling() {
        let config = PartitionConfig {
            prepare_queue_depth: MAX_PARTITION_PREPARE_QUEUE_DEPTH + 1,
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn accepts_at_ceiling() {
        let config = PartitionConfig {
            prepare_queue_depth: MAX_PARTITION_PREPARE_QUEUE_DEPTH,
        };
        assert!(config.validate().is_ok());
    }
}
