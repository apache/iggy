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
//! Capacity knobs previously hardcoded in the runtime crates:
//!
//! - `prepare_queue_depth` -> `consensus::PIPELINE_PREPARE_QUEUE_MAX`
//!   (the pipeline's in-flight prepare bound; submits beyond it bounce
//!   with the transient prepare-queue-full path the SDK retries)
//! - `evicted_ring_capacity` -> `partitions::EVICTED_RING_CAPACITY` and
//!   `evicted_ring_bytes_max` -> `partitions::EVICTED_RING_BYTES_MAX`
//!   (the per-partition journal-repair retention ring's dual ceilings)
//!
//! Distinct from `[metadata]` (a single, shard-0-global VSR plane) because
//! partition pipelines exist PER PARTITION: the request queue (`depth * 2` slots)
//! pins full inbound produce batches, so pinned memory scales with the partition
//! count. The default mirrors the runtime constant; the ceiling matches metadata's,
//! since both planes ship the same `DoViewChange` suffix over the same bitsets (see
//! [`MAX_PARTITION_PREPARE_QUEUE_DEPTH`]).
//!
//! The default is a duplicated literal rather than an import so
//! `core/configs` does not grow a build-time edge onto `core/consensus`
//! (mirroring [`super::metadata`]). `core/server`'s bootstrap pins the
//! literal against the runtime constant with a static assert.

use super::COMPONENT;
use crate::ConfigurationError;
use crate::common::validators::SEGMENT_MAX_SIZE_BYTES;
use configs::ConfigEnv;
use iggy_common::{IggyByteSize, Validatable};
use serde::{Deserialize, Serialize};

/// Mirrors `consensus::PIPELINE_PREPARE_QUEUE_MAX`.
pub const DEFAULT_PARTITION_PREPARE_QUEUE_DEPTH: usize = 32;

/// Upper bound on `prepare_queue_depth`.
///
/// Pinned by the view-change wire format, and equal to
/// [`super::metadata::MAX_METADATA_PREPARE_QUEUE_DEPTH`] for that reason: a
/// `DoViewChange` carries the sender's uncommitted suffix spanning `commit..=op`
/// with one nack bit and one present bit per entry, each bitset a single `u128`
/// (`consensus::DVC_HEADERS_MAX` = 128). This depth bounds `op - commit`, so a
/// deeper queue produces entries the new primary can neither adopt nor prove dead.
/// The reserved head slot leaves room for the head op.
///
/// The memory bound (`depth * 2 * partition_count * batch_size` of pinned produce
/// batches) still holds and is looser, so the wire is what decides.
pub const MAX_PARTITION_PREPARE_QUEUE_DEPTH: usize = 127;

/// Most one segment can overshoot its size cap: rotation checks the cap AFTER
/// appending, so a sealed segment runs up to one maximum batch past the
/// per-topic `segment_size`, whose own ceiling is the compile-time
/// [`SEGMENT_MAX_SIZE_BYTES`]. Tracks the shipped
/// `message_bus.max_message_size`, which is what actually bounds an appendable
/// batch.
///
/// Mirrors `shard::SEGMENT_SIZE_OVERSHOOT_BYTES`, which the serving side uses as
/// the floor of its admission divisor.
pub const SEGMENT_SIZE_OVERSHOOT_BYTES: u64 = 64 * 1024 * 1024;

/// Distinct max-size segments the served-payload budget is sized to hold at
/// once, and so the concurrent state transfers a shard admits.
///
/// Mirrors `shard::CONCURRENT_SERVED_SEGMENTS`.
pub const CONCURRENT_SERVED_SEGMENTS: u64 = 2;

/// Mirrors the free const `shard::PARTITION_ARTIFACT_LEN_DEFAULT` (segment
/// ceiling plus the one whole batch a segment may close past it).
pub const DEFAULT_TRANSFER_ARTIFACT_BYTES_MAX: u64 =
    SEGMENT_MAX_SIZE_BYTES + SEGMENT_SIZE_OVERSHOOT_BYTES;

/// Mirrors the free const `shard::SERVED_SEGMENT_CACHE_BYTES_DEFAULT`: room for
/// [`CONCURRENT_SERVED_SEGMENTS`] served segments at the size a SEALED one
/// actually reaches, which is the artifact ceiling above, not the configured
/// segment target. Sized off the target instead, two admitted pulls would not
/// both fit and would evict each other on every chunk.
pub const DEFAULT_TRANSFER_SERVED_CACHE_BYTES_MAX: u64 =
    CONCURRENT_SERVED_SEGMENTS * DEFAULT_TRANSFER_ARTIFACT_BYTES_MAX;

/// Upper bound on the two state-transfer byte knobs. A typo guard, not a sizing
/// endorsement: both are PER SHARD, so a slipped digit multiplies by the core
/// count.
pub const MAX_TRANSFER_BYTES: u64 = 64 * 1024 * 1024 * 1024;

/// Mirrors `partitions::EVICTED_RING_CAPACITY`.
pub const DEFAULT_EVICTED_RING_CAPACITY: usize = 4096;

/// Upper bound on `evicted_ring_capacity`. The ring exists per multi-replica
/// partition and each retained entry pins a full committed batch, so
/// worst-case pinned memory scales with the partition count; a typo guard,
/// not a sizing endorsement.
pub const MAX_EVICTED_RING_CAPACITY: usize = 65536;

/// Mirrors `partitions::EVICTED_RING_BYTES_MAX`.
pub const DEFAULT_EVICTED_RING_BYTES_MAX: u64 = 16 * 1024 * 1024;

/// Upper bound on `evicted_ring_bytes_max`, per partition. Whichever ring cap
/// trips first evicts; this byte ceiling is the second typo guard.
pub const MAX_EVICTED_RING_BYTES: u64 = 256 * 1024 * 1024;

/// Shipped default for [`PartitionConfig::dedup_clients_max`]; pinned against
/// the runtime constant by a bootstrap assert.
pub const PARTITION_DEDUP_CLIENTS_DEFAULT: usize = 4096;

/// Ceiling for [`PartitionConfig::dedup_clients_max`]. A per-group budget, so
/// the ceiling bounds worst-case memory at roughly `partitions * this * 130
/// bytes`: a 96-byte slot entry plus its index-map slot.
pub const PARTITION_DEDUP_CLIENTS_CEILING: usize = 1 << 16;

/// Capacity tunables for the per-partition consensus plane.
#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct PartitionConfig {
    /// Depth of a partition's prepare queue: how many uncommitted produce /
    /// consumer-offset ops may be in flight at once for that partition.
    /// Submits beyond it are rejected with the transient prepare-queue-full
    /// path the SDK retries. Applies to every partition; raising it multiplies
    /// pinned request-buffer memory by the partition count.
    pub prepare_queue_depth: usize,

    /// Distinct clients each partition group tracks request watermarks for,
    /// deduplicating retried produces and consumer-offset writes. At capacity
    /// the entry whose newest commit is oldest is evicted, which costs dedup
    /// coverage for that client (its next replay re-executes, exactly as it
    /// would have before dedup existed) and never correctness. Must be > 0 and
    /// <= [`PARTITION_DEDUP_CLIENTS_CEILING`].
    ///
    /// Unlike `[metadata] clients_table_max`, this budget is PER GROUP, so the
    /// worst case scales with partition count: size it to the producers a
    /// single partition actually sees, not the node's client total.
    pub dedup_clients_max: usize,

    /// Entries the evicted ring retains per multi-replica partition for
    /// journal repair after a peer rejoins. Larger widens the window a
    /// restarting peer can be served from the ring before falling back to
    /// bulk sync, at the cost of pinned memory per partition. Must be > 0 and
    /// <= [`MAX_EVICTED_RING_CAPACITY`]. Single-replica partitions retain
    /// nothing regardless.
    pub evicted_ring_capacity: usize,

    /// Byte ceiling for the evicted ring per partition; whichever ring cap
    /// (this or [`Self::evicted_ring_capacity`]) trips first evicts. Bounds
    /// the ring memory a burst of large batches can pin. Must be > 0 and <=
    /// [`MAX_EVICTED_RING_BYTES`].
    #[config_env(leaf)]
    pub evicted_ring_bytes_max: IggyByteSize,

    /// Byte budget for segment payloads a SERVING shard keeps resident to
    /// answer state-transfer chunk requests, per shard (so the process-wide
    /// bound is this times the shard count).
    ///
    /// Sized for concurrent pulls, not one: at exactly one maximum segment a
    /// single receiver arming several transfers thrashes the cache by itself,
    /// and every miss re-reads and re-hashes a whole segment to serve one
    /// chunk. Must be > 0.
    #[config_env(leaf)]
    pub transfer_served_cache_bytes_max: IggyByteSize,

    /// Alloc ceiling for ONE received state-transfer artifact, per shard.
    ///
    /// A receiver holds the whole artifact resident through verify, walk and
    /// staging write, so the in-flight cap multiplies this. It must stay above
    /// the largest legal segment (`segment.size` plus the one batch a segment
    /// may overshoot it by) or legal segments are rejected deterministically.
    /// Must be > 0.
    #[config_env(leaf)]
    pub transfer_artifact_bytes_max: IggyByteSize,
}

impl Validatable<ConfigurationError> for PartitionConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        if self.prepare_queue_depth == 0 {
            eprintln!("{COMPONENT} partition.prepare_queue_depth must be > 0");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.prepare_queue_depth > MAX_PARTITION_PREPARE_QUEUE_DEPTH {
            eprintln!(
                "{COMPONENT} partition.prepare_queue_depth ({}) exceeds the maximum \
                 ({MAX_PARTITION_PREPARE_QUEUE_DEPTH}). The ceiling is the view-change wire, not memory: \
                 a DoViewChange describes the uncommitted suffix with one bit per op in a u128 \
                 bitset, and this depth bounds that suffix. Deeper produces entries a new \
                 primary can neither adopt nor prove dead. Lowered from 256; not raisable.",
                self.prepare_queue_depth
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.dedup_clients_max == 0 || self.dedup_clients_max > PARTITION_DEDUP_CLIENTS_CEILING {
            eprintln!(
                "{COMPONENT} partition.dedup_clients_max ({}) must be > 0 and <= \
                 {PARTITION_DEDUP_CLIENTS_CEILING}",
                self.dedup_clients_max
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.evicted_ring_capacity == 0 {
            eprintln!("{COMPONENT} partition.evicted_ring_capacity must be > 0");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.evicted_ring_capacity > MAX_EVICTED_RING_CAPACITY {
            eprintln!(
                "{COMPONENT} partition.evicted_ring_capacity ({}) exceeds the maximum ({MAX_EVICTED_RING_CAPACITY})",
                self.evicted_ring_capacity
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        // The FLOOR on `transfer_artifact_bytes_max` cannot live here (it needs
        // `system.segment.size` and the bus cap); it is enforced in the
        // `ServerConfig` validator, which is what turns that misconfiguration
        // into a boot error instead of a silent per-partition rejoin livelock.
        let served_cache = self.transfer_served_cache_bytes_max.as_bytes_u64();
        if served_cache == 0 || served_cache > MAX_TRANSFER_BYTES {
            eprintln!(
                "{COMPONENT} partition.transfer_served_cache_bytes_max ({served_cache} bytes) \
                 must be > 0 and <= {MAX_TRANSFER_BYTES} bytes"
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        let artifact_bytes = self.transfer_artifact_bytes_max.as_bytes_u64();
        if artifact_bytes == 0 || artifact_bytes > MAX_TRANSFER_BYTES {
            eprintln!(
                "{COMPONENT} partition.transfer_artifact_bytes_max ({artifact_bytes} bytes) \
                 must be > 0 and <= {MAX_TRANSFER_BYTES} bytes"
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        let ring_bytes = self.evicted_ring_bytes_max.as_bytes_u64();
        if ring_bytes == 0 {
            eprintln!("{COMPONENT} partition.evicted_ring_bytes_max must be > 0");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if ring_bytes > MAX_EVICTED_RING_BYTES {
            eprintln!(
                "{COMPONENT} partition.evicted_ring_bytes_max ({ring_bytes} bytes) exceeds the maximum ({MAX_EVICTED_RING_BYTES} bytes)"
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

    /// The shipped TOML strings are the only thing an operator sees, and nothing
    /// else ties them to the constants the code sizes itself against -- a
    /// decimal/binary slip ("1088 MB" for 1088 MiB) parses fine and ships a cap
    /// BELOW the largest legal segment, which livelocks a rejoin per partition.
    #[test]
    fn shipped_transfer_defaults_match_the_runtime_constants() {
        let config = PartitionConfig::default();
        assert_eq!(
            config.transfer_artifact_bytes_max.as_bytes_u64(),
            DEFAULT_TRANSFER_ARTIFACT_BYTES_MAX,
            "config.toml transfer_artifact_bytes_max drifted from the runtime default"
        );
        assert_eq!(
            config.transfer_served_cache_bytes_max.as_bytes_u64(),
            DEFAULT_TRANSFER_SERVED_CACHE_BYTES_MAX,
            "config.toml transfer_served_cache_bytes_max drifted from the runtime default"
        );
    }

    #[test]
    fn shipped_dedup_default_matches_the_runtime_constant() {
        assert_eq!(
            PartitionConfig::default().dedup_clients_max,
            PARTITION_DEDUP_CLIENTS_DEFAULT,
            "config.toml dedup_clients_max drifted from the runtime default"
        );
    }

    #[test]
    fn rejects_out_of_range_dedup_clients_max() {
        for value in [0, PARTITION_DEDUP_CLIENTS_CEILING + 1] {
            let config = PartitionConfig {
                dedup_clients_max: value,
                ..PartitionConfig::default()
            };
            assert!(
                config.validate().is_err(),
                "dedup_clients_max {value} must be rejected"
            );
        }
    }

    #[test]
    fn rejects_zero_prepare_queue_depth() {
        let config = PartitionConfig {
            prepare_queue_depth: 0,
            ..PartitionConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_prepare_queue_depth_above_ceiling() {
        let config = PartitionConfig {
            prepare_queue_depth: MAX_PARTITION_PREPARE_QUEUE_DEPTH + 1,
            ..PartitionConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn accepts_prepare_queue_depth_at_ceiling() {
        let config = PartitionConfig {
            prepare_queue_depth: MAX_PARTITION_PREPARE_QUEUE_DEPTH,
            ..PartitionConfig::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn rejects_zero_evicted_ring_capacity() {
        let config = PartitionConfig {
            evicted_ring_capacity: 0,
            ..PartitionConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_evicted_ring_capacity_above_ceiling() {
        let config = PartitionConfig {
            evicted_ring_capacity: MAX_EVICTED_RING_CAPACITY + 1,
            ..PartitionConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_zero_evicted_ring_bytes_max() {
        let config = PartitionConfig {
            evicted_ring_bytes_max: IggyByteSize::from(0_u64),
            ..PartitionConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_evicted_ring_bytes_max_above_ceiling() {
        let config = PartitionConfig {
            evicted_ring_bytes_max: IggyByteSize::from(MAX_EVICTED_RING_BYTES + 1),
            ..PartitionConfig::default()
        };
        assert!(config.validate().is_err());
    }
}
