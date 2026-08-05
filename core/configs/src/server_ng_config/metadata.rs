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

//! On-disk schema for the metadata consensus plane (shard 0's VSR
//! replica: users, streams, topics, sessions).
//!
//! Three capacity knobs previously hardcoded in the runtime crates:
//!
//! - `prepare_queue_depth` -> `consensus::PIPELINE_PREPARE_QUEUE_MAX`
//!   (the pipeline's in-flight prepare bound; submits beyond it bounce
//!   with the transient "metadata prepare queue is full")
//! - `journal_slots` -> `journal::prepare_journal::DEFAULT_SLOT_COUNT`
//!   (the WAL's in-memory index; committed-but-unsnapshotted headroom
//!   between forced checkpoints)
//! - `clients_table_max` -> `consensus::CLIENTS_TABLE_MAX` (the VSR
//!   client-table slot count; independent of the two above). The
//!   server-ng HTTP session cap tracks it at half.
//!
//! The first two interlock through the forced-checkpoint margin
//! (`max(64, prepare_queue_depth)` at bootstrap): while a checkpoint
//! runs, up to a full prepare queue of already-pipelined ops appends
//! into that margin, and `validate` keeps `journal_slots` far enough
//! above it that checkpoints stay rare instead of back-to-back.
//!
//! The defaults are duplicated literals rather than imports so
//! `core/configs` does not grow build-time edges onto `core/consensus`
//! and `core/journal` (the runtime crates are the consumers of this
//! config, mirroring the `IOV_MAX_LIMIT_NG` precedent in
//! [`super::message_bus`]). `core/server-ng`'s bootstrap pins these
//! literals against the runtime constants with static asserts.

use super::COMPONENT_NG;
use crate::ConfigurationError;
use configs::ConfigEnv;
use iggy_common::Validatable;
use serde::{Deserialize, Serialize};

/// Mirrors `consensus::PIPELINE_PREPARE_QUEUE_MAX`.
pub const DEFAULT_METADATA_PREPARE_QUEUE_DEPTH: usize = 32;

/// Mirrors `journal::prepare_journal::DEFAULT_SLOT_COUNT`.
pub const DEFAULT_METADATA_JOURNAL_SLOTS: usize = 1024;

/// Floor of the forced-checkpoint margin
/// (`metadata::SnapshotCoordinator::CHECKPOINT_MARGIN`); the effective
/// margin is `max(this, prepare_queue_depth)`.
pub const METADATA_CHECKPOINT_MARGIN_FLOOR: usize = 64;

/// Upper bound on `prepare_queue_depth`. Every queued prepare pins a
/// full message buffer; four thousand in-flight metadata ops is far past
/// any sane deployment and a likely unit typo.
pub const MAX_METADATA_PREPARE_QUEUE_DEPTH: usize = 4096;

/// Upper bound on `journal_slots`. Each slot costs index memory and every
/// checkpoint rewrites the live WAL suffix; a million slots is the sanity
/// ceiling, not a tuning target.
pub const MAX_METADATA_JOURNAL_SLOTS: usize = 1 << 20;

/// Mirrors `consensus::CLIENTS_TABLE_MAX`, the VSR client-table slot count.
pub const DEFAULT_METADATA_CLIENTS_TABLE_MAX: usize = 8192;

/// Floor on `clients_table_max`. The server-ng HTTP session cap derives as
/// `clients_table_max / 2`; below two that floors to zero and HTTP could
/// register no sessions at all.
pub const MIN_METADATA_CLIENTS_TABLE_MAX: usize = 2;

/// Upper bound on `clients_table_max`. Every slot is preallocated for the
/// table's whole lifetime whether or not it holds a live client, so this caps
/// fixed per-shard table memory; 8x the default is far past any real client
/// population and a likely unit typo.
pub const MAX_METADATA_CLIENTS_TABLE_MAX: usize = 1 << 16;

/// Capacity tunables for the metadata consensus plane.
#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct MetadataConfig {
    /// Depth of the metadata prepare queue: how many uncommitted metadata
    /// ops may be in flight at once. Submits beyond it are rejected with
    /// the transient "metadata prepare queue is full" (SDK retries).
    pub prepare_queue_depth: usize,

    /// Size of the metadata WAL's in-memory index, in slots (one
    /// committed-but-unsnapshotted op per slot). Headroom between forced
    /// checkpoints; more slots = rarer checkpoints, more memory, larger
    /// per-checkpoint WAL rewrites.
    pub journal_slots: usize,

    /// Slot count of the VSR client table: how many distinct clients
    /// (TCP/QUIC/WS virtual clients and HTTP sessions together) hold live
    /// session state before the oldest-committed entry is evicted. The
    /// server-ng HTTP session cap tracks this at half, so raising it lifts
    /// both.
    pub clients_table_max: usize,
}

impl MetadataConfig {
    /// The forced-checkpoint margin bootstrap installs for this config:
    /// the built-in floor, raised to the configured prepare-queue depth.
    #[must_use]
    pub const fn checkpoint_margin(&self) -> usize {
        if self.prepare_queue_depth > METADATA_CHECKPOINT_MARGIN_FLOOR {
            self.prepare_queue_depth
        } else {
            METADATA_CHECKPOINT_MARGIN_FLOOR
        }
    }
}

impl Validatable<ConfigurationError> for MetadataConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        if self.prepare_queue_depth == 0 {
            eprintln!("{COMPONENT_NG} metadata.prepare_queue_depth must be > 0");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.prepare_queue_depth > MAX_METADATA_PREPARE_QUEUE_DEPTH {
            eprintln!(
                "{COMPONENT_NG} metadata.prepare_queue_depth ({}) exceeds the maximum ({MAX_METADATA_PREPARE_QUEUE_DEPTH})",
                self.prepare_queue_depth
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.journal_slots > MAX_METADATA_JOURNAL_SLOTS {
            eprintln!(
                "{COMPONENT_NG} metadata.journal_slots ({}) exceeds the maximum ({MAX_METADATA_JOURNAL_SLOTS})",
                self.journal_slots
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        // The journal must comfortably out-size the checkpoint margin:
        // at `journal_slots == margin` every single prepare would force a
        // checkpoint, and below it the journal could wrap. 4x keeps
        // checkpoints amortized over at least 3/4 of the journal.
        let min_slots = 4 * self.checkpoint_margin();
        if self.journal_slots < min_slots {
            eprintln!(
                "{COMPONENT_NG} metadata.journal_slots ({}) must be >= 4 * max({METADATA_CHECKPOINT_MARGIN_FLOOR}, prepare_queue_depth) = {min_slots}",
                self.journal_slots
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.clients_table_max < MIN_METADATA_CLIENTS_TABLE_MAX {
            eprintln!(
                "{COMPONENT_NG} metadata.clients_table_max ({}) must be >= {MIN_METADATA_CLIENTS_TABLE_MAX}",
                self.clients_table_max
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.clients_table_max > MAX_METADATA_CLIENTS_TABLE_MAX {
            eprintln!(
                "{COMPONENT_NG} metadata.clients_table_max ({}) exceeds the maximum ({MAX_METADATA_CLIENTS_TABLE_MAX})",
                self.clients_table_max
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
    fn defaults_are_valid() {
        let config = MetadataConfig {
            prepare_queue_depth: DEFAULT_METADATA_PREPARE_QUEUE_DEPTH,
            journal_slots: DEFAULT_METADATA_JOURNAL_SLOTS,
            clients_table_max: DEFAULT_METADATA_CLIENTS_TABLE_MAX,
        };
        assert!(config.validate().is_ok());
        assert_eq!(config.checkpoint_margin(), METADATA_CHECKPOINT_MARGIN_FLOOR);
    }

    #[test]
    fn margin_tracks_deep_prepare_queue() {
        let config = MetadataConfig {
            prepare_queue_depth: 256,
            journal_slots: 4096,
            clients_table_max: DEFAULT_METADATA_CLIENTS_TABLE_MAX,
        };
        assert!(config.validate().is_ok());
        assert_eq!(config.checkpoint_margin(), 256);
    }

    #[test]
    fn journal_must_outsize_margin() {
        // Deep queue, journal kept at the old default: margin becomes 256,
        // 4 * 256 = 1024 == journal_slots, boundary accepted...
        let boundary = MetadataConfig {
            prepare_queue_depth: 256,
            journal_slots: 1024,
            clients_table_max: DEFAULT_METADATA_CLIENTS_TABLE_MAX,
        };
        assert!(boundary.validate().is_ok());
        // ...one slot fewer is refused.
        let starved = MetadataConfig {
            prepare_queue_depth: 256,
            journal_slots: 1023,
            clients_table_max: DEFAULT_METADATA_CLIENTS_TABLE_MAX,
        };
        assert!(starved.validate().is_err());
    }

    #[test]
    fn zero_depth_is_refused() {
        let config = MetadataConfig {
            prepare_queue_depth: 0,
            journal_slots: DEFAULT_METADATA_JOURNAL_SLOTS,
            clients_table_max: DEFAULT_METADATA_CLIENTS_TABLE_MAX,
        };
        assert!(config.validate().is_err());
    }

    // The shipped config.toml default is the canonical slot count, so a
    // pristine deployment sizes the table exactly as the consensus constant.
    #[test]
    fn embedded_default_matches_canonical_clients_table_max() {
        assert_eq!(
            MetadataConfig::default().clients_table_max,
            DEFAULT_METADATA_CLIENTS_TABLE_MAX
        );
    }

    #[test]
    fn clients_table_max_below_floor_is_refused() {
        let config = MetadataConfig {
            prepare_queue_depth: DEFAULT_METADATA_PREPARE_QUEUE_DEPTH,
            journal_slots: DEFAULT_METADATA_JOURNAL_SLOTS,
            clients_table_max: MIN_METADATA_CLIENTS_TABLE_MAX - 1,
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn clients_table_max_at_floor_is_accepted() {
        let config = MetadataConfig {
            prepare_queue_depth: DEFAULT_METADATA_PREPARE_QUEUE_DEPTH,
            journal_slots: DEFAULT_METADATA_JOURNAL_SLOTS,
            clients_table_max: MIN_METADATA_CLIENTS_TABLE_MAX,
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn clients_table_max_above_ceiling_is_refused() {
        let config = MetadataConfig {
            prepare_queue_depth: DEFAULT_METADATA_PREPARE_QUEUE_DEPTH,
            journal_slots: DEFAULT_METADATA_JOURNAL_SLOTS,
            clients_table_max: MAX_METADATA_CLIENTS_TABLE_MAX + 1,
        };
        assert!(config.validate().is_err());
    }
}
