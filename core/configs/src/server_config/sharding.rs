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

//! Sharding config: the full thread-per-core + bus surface, with
//! defaults read from the embedded `core/server/config.toml`.

use iggy_common::IggyDuration;
use iggy_common::Validatable;
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};
use std::time::Duration;

use super::defaults::SERVER_CONFIG;
use crate::ConfigurationError;
use crate::common::validators::validate_cpu_allocation;
use configs::ConfigEnv;

// Re-exported so callers reach these through `configs::sharding::*`
// alongside the rest of the section.
pub use cpu_allocation::{CpuAllocation, NumaConfig};

/// Maximum permitted per-shard inbox depth. The channel is allocated
/// up-front per shard, so a runaway value here OOMs the process at boot.
/// `1 << 20` (~1M frames) is several orders of magnitude above any
/// realistic backpressure target and still fits comfortably in process
/// address space.
pub const INBOX_CAPACITY_MAX: usize = 1 << 20;

/// Hard upper bound on `shutdown_drain_timeout`. A drain that never
/// completes wedges process exit; capping at 10 minutes guarantees the
/// watchdog eventually force-tears the bus even with a pathological
/// config typo.
pub const SHUTDOWN_DRAIN_TIMEOUT_MAX: Duration = Duration::from_secs(600);

/// Hard upper bound on `shutdown_poll_interval`. A poll interval longer
/// than the drain timeout makes the flag effectively unobservable; cap
/// at 5s so Ctrl-C latency stays bounded regardless of config.
pub const SHUTDOWN_POLL_INTERVAL_MAX: Duration = Duration::from_secs(5);

/// Hard upper bound on `shutdown_join_timeout`. Comfortably above the
/// drain cap so a full drain always fits inside the join budget, while
/// still guaranteeing process exit against a pathological config typo.
pub const SHUTDOWN_JOIN_TIMEOUT_MAX: Duration = Duration::from_secs(900);

/// Hard upper bound on `reconcile_periodic_interval`. A tick longer
/// than ~30s makes post-failure recovery latency operator-visible; the
/// cap reins in pathological typos without disturbing reasonable
/// production values.
pub const RECONCILE_PERIODIC_INTERVAL_MAX: Duration = Duration::from_secs(30);

// Every omitted field falls back to the frozen `Default`, so a partial
// `[system.sharding]` table resolves each key independently instead of
// failing on the first missing one (parity with the legacy type).
#[serde_as]
#[derive(Debug, Deserialize, Serialize, ConfigEnv)]
#[serde(default)]
pub struct ShardingConfig {
    #[serde(default)]
    #[config_env(leaf)]
    pub cpu_allocation: CpuAllocation,
    /// Whether shard threads are pinned to dedicated CPU cores
    /// (`sched_setaffinity`). Pinning maximizes cache locality when this
    /// server owns its cores (dedicated host, `numa:` allocations). Set to
    /// `false` when the server shares cores with other workloads — e.g. a
    /// multi-tenant host slicing CPU via cgroup quotas — where every process
    /// pinning to the same low-numbered cores would pile onto one core while
    /// the rest sit idle; unpinned shards let the kernel scheduler place
    /// threads freely within the allowed set. With a NUMA-aware allocation,
    /// `false` drops both the CPU and memory-node bindings (and logs a
    /// warning, since NUMA placement without pinning is meaningless).
    pub pin_cores: bool,
    /// Per-shard inter-shard inbox channel capacity (the main lane:
    /// consensus frames, connection setup, reconcile wakes). Bounded by
    /// design; drops of consensus frames on a full inbox are recovered by
    /// VSR retransmit. Size against the consensus working set per shard.
    pub inbox_capacity: usize,
    /// Capacity of the reply lane: the separate bounded channel carrying
    /// cross-shard client `Reply` forwards, whose drops are terminal (no
    /// in-protocol retransmit; the client never receives the reply). Split
    /// from [`Self::inbox_capacity`] so a consensus burst cannot evict
    /// reply forwards and each lane is sized for its own worst case: this
    /// one against peak client-reply fan-out per shard.
    pub reply_inbox_capacity: usize,
    /// Wall-clock budget for a single shard's bus drain on shutdown.
    /// Drives `IggyMessageBus::shutdown(..)` from the per-shard watchdog
    /// and the parallel-join survivor path. Sized larger than typical
    /// TCP RTT times in-flight write-batch so writers receive their full
    /// last `write_vectored_all` budget before the connection registry
    /// force-tears the bus. Slow-fsync hosts may need to extend this past
    /// the default; the cap is `SHUTDOWN_DRAIN_TIMEOUT_MAX` so a config
    /// typo cannot wedge process exit.
    #[serde_as(as = "DisplayFromStr")]
    #[config_env(leaf)]
    pub shutdown_drain_timeout: IggyDuration,
    /// Poll cadence for the cross-thread shutdown flag and for the
    /// `await_metadata_bundle` / `broadcast_metadata_bundle` poll loops.
    /// Trades off Ctrl-C latency against idle wakeup cost; the default
    /// keeps shutdown observably prompt without measurable scheduler
    /// overhead. Capped at `SHUTDOWN_POLL_INTERVAL_MAX` so the flag
    /// remains effectively observable regardless of config.
    #[serde_as(as = "DisplayFromStr")]
    #[config_env(leaf)]
    pub shutdown_poll_interval: IggyDuration,
    /// Hard wall-clock deadline for joining shard threads at process
    /// exit. A shard whose pump or listener wedges past this budget is
    /// abandoned with an error log instead of blocking exit forever.
    /// Must be at least `shutdown_drain_timeout` (abandoning a shard
    /// mid-drain would interrupt its WAL fsync / replica drain) and at
    /// most [`SHUTDOWN_JOIN_TIMEOUT_MAX`].
    #[serde_as(as = "DisplayFromStr")]
    #[config_env(leaf)]
    pub shutdown_join_timeout: IggyDuration,
    /// Safety-tick cadence for the partition reconciliation loop; the
    /// reconciler also wakes immediately on every
    /// `LifecycleFrame::MetadataCommitTick` from shard 0. This periodic
    /// fallback covers dropped wake-ups (the wake channel is capacity-1)
    /// and the initial post-bootstrap convergence window. Values above
    /// [`RECONCILE_PERIODIC_INTERVAL_MAX`] are rejected by the validator.
    #[serde_as(as = "DisplayFromStr")]
    #[config_env(leaf)]
    pub reconcile_periodic_interval: IggyDuration,
}

impl Default for ShardingConfig {
    fn default() -> Self {
        Self {
            cpu_allocation: CpuAllocation::default(),
            pin_cores: SERVER_CONFIG.system.sharding.pin_cores,
            inbox_capacity: SERVER_CONFIG.system.sharding.inbox_capacity as usize,
            reply_inbox_capacity: SERVER_CONFIG.system.sharding.reply_inbox_capacity as usize,
            shutdown_drain_timeout: SERVER_CONFIG
                .system
                .sharding
                .shutdown_drain_timeout
                .parse()
                .unwrap(),
            shutdown_poll_interval: SERVER_CONFIG
                .system
                .sharding
                .shutdown_poll_interval
                .parse()
                .unwrap(),
            shutdown_join_timeout: SERVER_CONFIG
                .system
                .sharding
                .shutdown_join_timeout
                .parse()
                .unwrap(),
            reconcile_periodic_interval: SERVER_CONFIG
                .system
                .sharding
                .reconcile_periodic_interval
                .parse()
                .unwrap(),
        }
    }
}

impl Validatable<ConfigurationError> for ShardingConfig {
    fn validate(&self) -> Result<(), ConfigurationError> {
        if self.inbox_capacity == 0 {
            eprintln!(
                "Invalid sharding configuration: inbox_capacity must be > 0 (crossfire silently \
                 rounds 0 to 1, masking config errors)"
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.inbox_capacity > INBOX_CAPACITY_MAX {
            eprintln!(
                "Invalid sharding configuration: inbox_capacity {} exceeds the {} cap (each \
                 shard preallocates a channel of this size; oversizing here OOMs the process at \
                 boot)",
                self.inbox_capacity, INBOX_CAPACITY_MAX
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.reply_inbox_capacity == 0 {
            eprintln!(
                "Invalid sharding configuration: reply_inbox_capacity must be > 0 (crossfire \
                 silently rounds 0 to 1, masking config errors)"
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if self.reply_inbox_capacity > INBOX_CAPACITY_MAX {
            eprintln!(
                "Invalid sharding configuration: reply_inbox_capacity {} exceeds the {} cap \
                 (each shard preallocates a channel of this size; oversizing here OOMs the \
                 process at boot)",
                self.reply_inbox_capacity, INBOX_CAPACITY_MAX
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        let drain = self.shutdown_drain_timeout.get_duration();
        if drain.is_zero() {
            eprintln!(
                "Invalid sharding configuration: shutdown_drain_timeout must be > 0 (a zero \
                 budget force-tears the bus mid-WAL-fsync on every shutdown)"
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if drain > SHUTDOWN_DRAIN_TIMEOUT_MAX {
            eprintln!(
                "Invalid sharding configuration: shutdown_drain_timeout {:?} exceeds the {:?} \
                 cap (an unbounded drain wedges process exit on bus stall)",
                drain, SHUTDOWN_DRAIN_TIMEOUT_MAX
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        let poll = self.shutdown_poll_interval.get_duration();
        if poll.is_zero() {
            eprintln!(
                "Invalid sharding configuration: shutdown_poll_interval must be > 0 (a zero \
                 cadence busy-loops every shard's watchdog and metadata-handoff poller)"
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if poll > SHUTDOWN_POLL_INTERVAL_MAX {
            eprintln!(
                "Invalid sharding configuration: shutdown_poll_interval {:?} exceeds the {:?} \
                 cap (a coarse cadence stalls Ctrl-C handling and metadata handoff abort)",
                poll, SHUTDOWN_POLL_INTERVAL_MAX
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if poll > drain {
            eprintln!(
                "Invalid sharding configuration: shutdown_poll_interval {:?} must be <= \
                 shutdown_drain_timeout {:?} (a poll cadence coarser than the drain budget makes \
                 the shutdown flag effectively unobservable)",
                poll, drain
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        let join = self.shutdown_join_timeout.get_duration();
        if join < drain {
            eprintln!(
                "Invalid sharding configuration: shutdown_join_timeout {:?} must be >= \
                 shutdown_drain_timeout {:?} (a join budget shorter than the drain abandons \
                 shards mid-drain, interrupting the WAL fsync / replica drain)",
                join, drain
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if join > SHUTDOWN_JOIN_TIMEOUT_MAX {
            eprintln!(
                "Invalid sharding configuration: shutdown_join_timeout {:?} exceeds the {:?} \
                 cap (an unbounded join budget wedges process exit on a stuck shard)",
                join, SHUTDOWN_JOIN_TIMEOUT_MAX
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        let reconcile = self.reconcile_periodic_interval.get_duration();
        if reconcile.is_zero() {
            eprintln!(
                "Invalid sharding configuration: reconcile_periodic_interval resolves to zero. \
                 Note that \"0\", \"none\", \"unlimited\", and \"disabled\" all parse to zero. The \
                 periodic reconcile tick is a safety net for dropped commit-wakes and cannot be \
                 turned off; set a positive duration (default \"1s\", max {RECONCILE_PERIODIC_INTERVAL_MAX:?})."
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }
        if reconcile > RECONCILE_PERIODIC_INTERVAL_MAX {
            eprintln!(
                "Invalid sharding configuration: reconcile_periodic_interval {:?} exceeds the \
                 {:?} cap (a long tick makes post-failure convergence latency operator-visible)",
                reconcile, RECONCILE_PERIODIC_INTERVAL_MAX
            );
            return Err(ConfigurationError::InvalidConfigurationValue);
        }

        validate_cpu_allocation(&self.cpu_allocation, self.pin_cores)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server_config::server::ServerConfig;
    use figment::Figment;
    use figment::providers::{Format, Toml};

    #[test]
    fn defaults_validate() {
        assert!(ShardingConfig::default().validate().is_ok());
    }

    #[test]
    fn zero_drain_is_rejected() {
        let cfg = ShardingConfig {
            shutdown_drain_timeout: IggyDuration::new(Duration::ZERO),
            ..ShardingConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn over_cap_drain_is_rejected() {
        let cfg = ShardingConfig {
            shutdown_drain_timeout: IggyDuration::new(
                SHUTDOWN_DRAIN_TIMEOUT_MAX + Duration::from_secs(1),
            ),
            ..ShardingConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn zero_poll_is_rejected() {
        let cfg = ShardingConfig {
            shutdown_poll_interval: IggyDuration::new(Duration::ZERO),
            ..ShardingConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn over_cap_poll_is_rejected() {
        let cfg = ShardingConfig {
            shutdown_poll_interval: IggyDuration::new(
                SHUTDOWN_POLL_INTERVAL_MAX + Duration::from_secs(1),
            ),
            ..ShardingConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn poll_greater_than_drain_is_rejected() {
        let cfg = ShardingConfig {
            shutdown_drain_timeout: IggyDuration::new(Duration::from_millis(20)),
            shutdown_poll_interval: IggyDuration::new(Duration::from_millis(50)),
            ..ShardingConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn join_shorter_than_drain_is_rejected() {
        // A join budget under the drain would abandon shards mid-drain.
        let cfg = ShardingConfig {
            shutdown_drain_timeout: IggyDuration::new(Duration::from_secs(10)),
            shutdown_join_timeout: IggyDuration::new(Duration::from_secs(5)),
            ..ShardingConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn over_cap_join_is_rejected() {
        let cfg = ShardingConfig {
            shutdown_join_timeout: IggyDuration::new(
                SHUTDOWN_JOIN_TIMEOUT_MAX + Duration::from_secs(1),
            ),
            ..ShardingConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn join_equal_to_drain_is_accepted() {
        let cfg = ShardingConfig {
            shutdown_drain_timeout: IggyDuration::new(Duration::from_secs(10)),
            shutdown_join_timeout: IggyDuration::new(Duration::from_secs(10)),
            ..ShardingConfig::default()
        };
        assert!(cfg.validate().is_ok());
    }

    // Guards the single source of truth: the sharding defaults resolve
    // from the embedded TOML, not hard-coded Rust values.
    #[test]
    fn embedded_toml_resolves_sharding_defaults() {
        let toml_str = include_str!("../../../server/config.toml");
        let config: ServerConfig = Figment::new()
            .merge(Toml::string(toml_str))
            .extract()
            .expect("embedded TOML deserializes");
        config.validate().expect("embedded config validates");

        let sharding = &config.system.sharding;
        assert!(sharding.pin_cores);
        assert_eq!(sharding.inbox_capacity, 1024);
        assert_eq!(sharding.reply_inbox_capacity, 1024);
        assert_eq!(sharding.shutdown_drain_timeout, "10 s".parse().unwrap());
        assert_eq!(sharding.shutdown_poll_interval, "50 ms".parse().unwrap());
        assert_eq!(sharding.shutdown_join_timeout, "30 s".parse().unwrap());
        assert_eq!(sharding.reconcile_periodic_interval, "1 s".parse().unwrap());
    }

    // Extract straight from a raw table (no embedded base layer) so the
    // struct-level `#[serde(default)]` is what fills the gaps, not the
    // provider's embedded-TOML fallback.
    #[test]
    fn partial_table_fills_missing_fields_with_frozen_defaults() {
        let sharding: ShardingConfig = Figment::new()
            .merge(Toml::string("pin_cores = false"))
            .extract()
            .expect("partial sharding table deserializes");

        assert!(!sharding.pin_cores);
        assert_eq!(sharding.inbox_capacity, 1024);
        assert_eq!(sharding.reply_inbox_capacity, 1024);
        assert_eq!(sharding.shutdown_drain_timeout, "10 s".parse().unwrap());
        assert_eq!(sharding.shutdown_poll_interval, "50 ms".parse().unwrap());
        assert_eq!(sharding.shutdown_join_timeout, "30 s".parse().unwrap());
        assert_eq!(sharding.reconcile_periodic_interval, "1 s".parse().unwrap());
    }

    #[test]
    fn empty_table_yields_all_frozen_defaults() {
        let sharding: ShardingConfig = Figment::new()
            .merge(Toml::string(""))
            .extract()
            .expect("empty sharding table deserializes");

        assert!(sharding.pin_cores);
        assert_eq!(sharding.inbox_capacity, 1024);
        assert_eq!(sharding.reply_inbox_capacity, 1024);
        assert_eq!(sharding.shutdown_drain_timeout, "10 s".parse().unwrap());
        assert_eq!(sharding.shutdown_poll_interval, "50 ms".parse().unwrap());
        assert_eq!(sharding.shutdown_join_timeout, "30 s".parse().unwrap());
        assert_eq!(sharding.reconcile_periodic_interval, "1 s".parse().unwrap());
    }
}
