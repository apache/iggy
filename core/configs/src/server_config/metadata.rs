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
//! Six capacity knobs. The first three were hardcoded in the runtime
//! crates before:
//!
//! - `prepare_queue_depth` -> `consensus::PIPELINE_PREPARE_QUEUE_MAX`
//!   (the pipeline's in-flight prepare bound; submits beyond it bounce
//!   with the transient "metadata prepare queue is full")
//! - `journal_slots` -> `journal::prepare_journal::DEFAULT_SLOT_COUNT`
//!   (the WAL's in-memory index; committed-but-unsnapshotted headroom
//!   between forced checkpoints)
//! - `clients_table_max` -> `consensus::CLIENTS_TABLE_MAX` (the VSR
//!   client-table slot count; independent of the two above). The
//!   HTTP session cap tracks it at half.
//! - `partitions_max` (the partitions of all streams and topics, checked
//!   by the metadata primary when it admits a create; zero is no cap)
//! - `change_log_resource_capacity` and `change_log_activity_capacity`
//!   (records the metadata change log retains per retention class)
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
//! config, mirroring the `IOV_MAX_LIMIT` precedent in
//! [`super::message_bus`]). `core/server`'s bootstrap pins these
//! literals against the runtime constants with static asserts.

use super::COMPONENT;
use super::defaults::SERVER_CONFIG;
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

/// Upper bound on `prepare_queue_depth`.
///
/// Pinned by the view-change wire format, not by memory: a `DoViewChange` carries
/// the sender's uncommitted suffix plus one nack bit and one present bit per entry,
/// each bitset a single `u128` (`consensus::DVC_HEADERS_MAX` = 128). The suffix
/// spans `commit_max..=op`, which this depth bounds, so a deeper queue produces
/// entries the new primary can neither adopt nor prove dead. The reserved head slot
/// leaves room for the head op.
pub const MAX_METADATA_PREPARE_QUEUE_DEPTH: usize = 127;

/// Upper bound on `journal_slots`. Each slot costs index memory and every
/// checkpoint rewrites the live WAL suffix; a million slots is the sanity
/// ceiling, not a tuning target.
pub const MAX_METADATA_JOURNAL_SLOTS: usize = 1 << 20;

/// Mirrors `consensus::CLIENTS_TABLE_MAX`, the VSR client-table slot count.
pub const DEFAULT_METADATA_CLIENTS_TABLE_MAX: usize = 8192;

/// Floor on `clients_table_max`. The HTTP session cap derives as
/// `clients_table_max / 2`; below two that floors to zero and HTTP could
/// register no sessions at all.
pub const MIN_METADATA_CLIENTS_TABLE_MAX: usize = 2;

/// Upper bound on `clients_table_max`. Every slot is preallocated for the
/// table's whole lifetime whether or not it holds a live client, so this caps
/// fixed per-shard table memory; 8x the default is far past any real client
/// population and a likely unit typo.
pub const MAX_METADATA_CLIENTS_TABLE_MAX: usize = 1 << 16;

/// Mirrors `metadata::change_log::DEFAULT_RESOURCE_CAPACITY`, the records of
/// streams, topics, partitions, consumer groups, users and tokens retained.
pub const DEFAULT_METADATA_CHANGE_LOG_RESOURCE_CAPACITY: usize = 65_536;

/// Mirrors `metadata::change_log::DEFAULT_ACTIVITY_CAPACITY`, the records of
/// sessions and consumer group membership retained.
pub const DEFAULT_METADATA_CHANGE_LOG_ACTIVITY_CAPACITY: usize = 16_384;

/// Mirrors `metadata::change_log::MAX_CAPACITY`. A snapshot holding a larger
/// ring is refused, so a capacity past it could never be restored.
pub const MAX_METADATA_CHANGE_LOG_CAPACITY: usize = 1 << 20;

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
    /// per-checkpoint WAL rewrites. Reducing this for an existing data directory
    /// is refused at boot when the live WAL suffix cannot fit without collisions;
    /// restore the previous value to recover.
    pub journal_slots: usize,

    /// Initial capacity of the metadata session registry, shared by binary
    /// clients and HTTP sessions. Live and unretired sessions occupy slots;
    /// a full registry refuses new sessions instead of evicting retry protection.
    /// The first committed operation fixes this capacity. Changing configuration
    /// does not resize an existing registry, including after restart or transfer.
    /// The HTTP cache cap uses half the configured value, but cannot raise the
    /// committed registry limit.
    pub clients_table_max: usize,

    /// Cap on the partitions of all streams and topics. A CreateTopic or
    /// CreatePartitions that would exceed it is rejected with
    /// `PartitionsLimitReached` before it enters consensus. Zero is no cap.
    ///
    /// The metadata primary checks it with its own value, so every node needs
    /// the same value. The check counts the committed partitions and those of
    /// the creates still in flight, so creates never take the count past it.
    #[serde(default)]
    pub partitions_max: u32,

    /// Change log records kept for streams, topics, partitions, consumer
    /// groups, users and personal access tokens. The oldest record goes once
    /// the count passes it. Node-local: replicas may keep different depths.
    #[serde(default = "default_change_log_resource_capacity")]
    pub change_log_resource_capacity: usize,

    /// Change log records kept for sessions and consumer group membership,
    /// separate so a login or membership storm cannot evict resource history.
    #[serde(default = "default_change_log_activity_capacity")]
    pub change_log_activity_capacity: usize,
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
            eprintln!("{COMPONENT} metadata.prepare_queue_depth must be > 0");
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.prepare_queue_depth > MAX_METADATA_PREPARE_QUEUE_DEPTH {
            eprintln!(
                "{COMPONENT} metadata.prepare_queue_depth ({}) exceeds the maximum \
                 ({MAX_METADATA_PREPARE_QUEUE_DEPTH}). The ceiling is the view-change wire, not memory: \
                 a DoViewChange describes the uncommitted suffix with one bit per op in a u128 \
                 bitset, and this depth bounds that suffix. Deeper produces entries a new \
                 primary can neither adopt nor prove dead. Lowered from 256; not raisable.",
                self.prepare_queue_depth
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.journal_slots > MAX_METADATA_JOURNAL_SLOTS {
            eprintln!(
                "{COMPONENT} metadata.journal_slots ({}) exceeds the maximum ({MAX_METADATA_JOURNAL_SLOTS})",
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
                "{COMPONENT} metadata.journal_slots ({}) must be >= 4 * max({METADATA_CHECKPOINT_MARGIN_FLOOR}, prepare_queue_depth) = {min_slots}",
                self.journal_slots
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.clients_table_max < MIN_METADATA_CLIENTS_TABLE_MAX {
            eprintln!(
                "{COMPONENT} metadata.clients_table_max ({}) must be >= {MIN_METADATA_CLIENTS_TABLE_MAX}",
                self.clients_table_max
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.clients_table_max > MAX_METADATA_CLIENTS_TABLE_MAX {
            eprintln!(
                "{COMPONENT} metadata.clients_table_max ({}) exceeds the maximum ({MAX_METADATA_CLIENTS_TABLE_MAX})",
                self.clients_table_max
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        for (name, capacity) in [
            (
                "change_log_resource_capacity",
                self.change_log_resource_capacity,
            ),
            (
                "change_log_activity_capacity",
                self.change_log_activity_capacity,
            ),
        ] {
            if !(1..=MAX_METADATA_CHANGE_LOG_CAPACITY).contains(&capacity) {
                eprintln!(
                    "{COMPONENT} metadata.{name} ({capacity}) must be between 1 and {MAX_METADATA_CHANGE_LOG_CAPACITY}"
                );
                return Err(ConfigurationError::InvalidConfigurationValue);
            }
        }
        Ok(())
    }
}

/// Zero fails validation, so the value comes from the embedded `config.toml`.
fn default_change_log_resource_capacity() -> usize {
    SERVER_CONFIG.metadata.change_log_resource_capacity as usize
}

fn default_change_log_activity_capacity() -> usize {
    SERVER_CONFIG.metadata.change_log_activity_capacity as usize
}

#[cfg(test)]
mod tests {
    use super::*;
    use figment::Figment;
    use figment::providers::{Format, Toml};

    #[test]
    fn defaults_are_valid() {
        let config = MetadataConfig {
            prepare_queue_depth: DEFAULT_METADATA_PREPARE_QUEUE_DEPTH,
            journal_slots: DEFAULT_METADATA_JOURNAL_SLOTS,
            clients_table_max: DEFAULT_METADATA_CLIENTS_TABLE_MAX,
            partitions_max: 0,
            change_log_resource_capacity: DEFAULT_METADATA_CHANGE_LOG_RESOURCE_CAPACITY,
            change_log_activity_capacity: DEFAULT_METADATA_CHANGE_LOG_ACTIVITY_CAPACITY,
        };
        assert!(config.validate().is_ok());
        assert_eq!(config.checkpoint_margin(), METADATA_CHECKPOINT_MARGIN_FLOOR);
    }

    #[test]
    fn margin_tracks_deep_prepare_queue() {
        let config = MetadataConfig {
            prepare_queue_depth: MAX_METADATA_PREPARE_QUEUE_DEPTH,
            journal_slots: 4096,
            clients_table_max: DEFAULT_METADATA_CLIENTS_TABLE_MAX,
            partitions_max: 0,
            change_log_resource_capacity: DEFAULT_METADATA_CHANGE_LOG_RESOURCE_CAPACITY,
            change_log_activity_capacity: DEFAULT_METADATA_CHANGE_LOG_ACTIVITY_CAPACITY,
        };
        assert!(config.validate().is_ok());
        assert_eq!(config.checkpoint_margin(), MAX_METADATA_PREPARE_QUEUE_DEPTH);
    }

    #[test]
    fn journal_must_outsize_margin() {
        // Deepest permitted queue: margin becomes the depth, and the journal
        // must hold 4x that. At exactly 4x the boundary is accepted...
        let min_slots = 4 * MAX_METADATA_PREPARE_QUEUE_DEPTH;
        let boundary = MetadataConfig {
            prepare_queue_depth: MAX_METADATA_PREPARE_QUEUE_DEPTH,
            journal_slots: min_slots,
            clients_table_max: DEFAULT_METADATA_CLIENTS_TABLE_MAX,
            partitions_max: 0,
            change_log_resource_capacity: DEFAULT_METADATA_CHANGE_LOG_RESOURCE_CAPACITY,
            change_log_activity_capacity: DEFAULT_METADATA_CHANGE_LOG_ACTIVITY_CAPACITY,
        };
        assert!(boundary.validate().is_ok());
        // ...one slot fewer is refused.
        let starved = MetadataConfig {
            prepare_queue_depth: MAX_METADATA_PREPARE_QUEUE_DEPTH,
            journal_slots: min_slots - 1,
            clients_table_max: DEFAULT_METADATA_CLIENTS_TABLE_MAX,
            partitions_max: 0,
            change_log_resource_capacity: DEFAULT_METADATA_CHANGE_LOG_RESOURCE_CAPACITY,
            change_log_activity_capacity: DEFAULT_METADATA_CHANGE_LOG_ACTIVITY_CAPACITY,
        };
        assert!(starved.validate().is_err());
    }

    #[test]
    fn prepare_queue_depth_capped_by_view_change_bitset_width() {
        // Not a memory guard: it keeps every uncommitted suffix entry addressable by
        // a `u128` bitset in a `DoViewChange`. One past it must be refused, or a view
        // change meets an entry it can neither adopt nor prove dead.
        let over = MetadataConfig {
            prepare_queue_depth: MAX_METADATA_PREPARE_QUEUE_DEPTH + 1,
            journal_slots: MAX_METADATA_JOURNAL_SLOTS,
            clients_table_max: DEFAULT_METADATA_CLIENTS_TABLE_MAX,
            partitions_max: 0,
            change_log_resource_capacity: DEFAULT_METADATA_CHANGE_LOG_RESOURCE_CAPACITY,
            change_log_activity_capacity: DEFAULT_METADATA_CHANGE_LOG_ACTIVITY_CAPACITY,
        };
        assert!(over.validate().is_err());
        assert_eq!(
            MAX_METADATA_PREPARE_QUEUE_DEPTH + 1,
            128,
            "cap must leave the head op a slot inside the 128-bit bitset"
        );
    }

    #[test]
    fn zero_depth_is_refused() {
        let config = MetadataConfig {
            prepare_queue_depth: 0,
            journal_slots: DEFAULT_METADATA_JOURNAL_SLOTS,
            clients_table_max: DEFAULT_METADATA_CLIENTS_TABLE_MAX,
            partitions_max: 0,
            change_log_resource_capacity: DEFAULT_METADATA_CHANGE_LOG_RESOURCE_CAPACITY,
            change_log_activity_capacity: DEFAULT_METADATA_CHANGE_LOG_ACTIVITY_CAPACITY,
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
            partitions_max: 0,
            change_log_resource_capacity: DEFAULT_METADATA_CHANGE_LOG_RESOURCE_CAPACITY,
            change_log_activity_capacity: DEFAULT_METADATA_CHANGE_LOG_ACTIVITY_CAPACITY,
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn clients_table_max_at_floor_is_accepted() {
        let config = MetadataConfig {
            prepare_queue_depth: DEFAULT_METADATA_PREPARE_QUEUE_DEPTH,
            journal_slots: DEFAULT_METADATA_JOURNAL_SLOTS,
            clients_table_max: MIN_METADATA_CLIENTS_TABLE_MAX,
            partitions_max: 0,
            change_log_resource_capacity: DEFAULT_METADATA_CHANGE_LOG_RESOURCE_CAPACITY,
            change_log_activity_capacity: DEFAULT_METADATA_CHANGE_LOG_ACTIVITY_CAPACITY,
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn embedded_defaults_match_canonical_change_log_capacities() {
        let config = MetadataConfig::default();
        assert_eq!(
            config.change_log_resource_capacity,
            DEFAULT_METADATA_CHANGE_LOG_RESOURCE_CAPACITY
        );
        assert_eq!(
            config.change_log_activity_capacity,
            DEFAULT_METADATA_CHANGE_LOG_ACTIVITY_CAPACITY
        );
    }

    #[test]
    fn change_log_capacities_absent_from_toml_take_the_defaults() {
        let config: MetadataConfig = Figment::new()
            .merge(Toml::string(
                "prepare_queue_depth = 32\njournal_slots = 1024\nclients_table_max = 8192\n",
            ))
            .extract()
            .expect("config without change log capacities deserializes");
        assert_eq!(
            config.change_log_resource_capacity,
            DEFAULT_METADATA_CHANGE_LOG_RESOURCE_CAPACITY
        );
        assert_eq!(
            config.change_log_activity_capacity,
            DEFAULT_METADATA_CHANGE_LOG_ACTIVITY_CAPACITY
        );
    }

    #[test]
    fn change_log_capacity_out_of_bounds_is_refused() {
        for capacity in [0, MAX_METADATA_CHANGE_LOG_CAPACITY + 1] {
            for resource in [true, false] {
                let mut config = MetadataConfig::default();
                if resource {
                    config.change_log_resource_capacity = capacity;
                } else {
                    config.change_log_activity_capacity = capacity;
                }
                assert!(config.validate().is_err(), "{capacity} must be refused");
            }
        }
    }

    #[test]
    fn change_log_capacity_at_bounds_is_accepted() {
        for capacity in [1, MAX_METADATA_CHANGE_LOG_CAPACITY] {
            let config = MetadataConfig {
                change_log_resource_capacity: capacity,
                change_log_activity_capacity: capacity,
                ..MetadataConfig::default()
            };
            assert!(config.validate().is_ok(), "{capacity} must be accepted");
        }
    }

    #[test]
    fn clients_table_max_above_ceiling_is_refused() {
        let config = MetadataConfig {
            prepare_queue_depth: DEFAULT_METADATA_PREPARE_QUEUE_DEPTH,
            journal_slots: DEFAULT_METADATA_JOURNAL_SLOTS,
            clients_table_max: MAX_METADATA_CLIENTS_TABLE_MAX + 1,
            partitions_max: 0,
            change_log_resource_capacity: DEFAULT_METADATA_CHANGE_LOG_RESOURCE_CAPACITY,
            change_log_activity_capacity: DEFAULT_METADATA_CHANGE_LOG_ACTIVITY_CAPACITY,
        };
        assert!(config.validate().is_err());
    }
}
