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

use iggy_common::IggyDuration;
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};
use std::time::Duration;

use configs::ConfigEnv;

// `CpuAllocation`/`NumaConfig` are pure config types and live in their own
// leaf crate so both `configs` and `shard_allocator` can share them without
// pulling each other's heavier dependency trees. Re-exported here to keep the
// `configs::sharding::*` path stable for existing callers.
pub use cpu_allocation::{CpuAllocation, NumaConfig};

/// Default capacity of the per-shard inter-shard inbox channel. Sized
/// comfortably above the consensus working set, which is roughly
/// `PIPELINE_PREPARE_QUEUE_MAX (= 8) * replica_count * directions`
/// frames in flight per shard, without allowing a runaway producer to
/// eat unbounded memory. Tunable via `[system.sharding] inbox_capacity`
/// in TOML.
///
/// The capacity must also absorb the worst-case cross-shard client
/// Reply burst. Unlike consensus frames, client Replies have no VSR
/// retransmit path: a Reply lost on full inbox is gone and the client
/// times out. A reasonable lower bound is
/// `max_inflight_client_requests / num_shards` (assuming requests are
/// distributed evenly across owning shards) plus the consensus
/// headroom above.
///
/// Consensus frames and client-reply forwards share this one channel,
/// so the two headrooms are not independent: a consensus burst or
/// retransmit storm can fill the inbox with consensus frames exactly
/// when a client Reply needs the space. A single `inbox_capacity` knob
/// cannot isolate the two frame classes - size it for the sum of both
/// worst cases occurring together. Watch the drop-site `tracing` logs
/// (and, once a per-shard exporter lands, the `frame_drops_total`
/// `{variant="forward_client_send"}` counter) to detect when the bound
/// is too low in production.
pub const DEFAULT_INBOX_CAPACITY: usize = 1024;

/// Maximum permitted per-shard inbox depth. The channel is allocated
/// up-front per shard, so a runaway value here OOMs the process at boot.
/// `1 << 20` (~1M frames) is several orders of magnitude above any
/// realistic backpressure target and still fits comfortably in process
/// address space.
pub const INBOX_CAPACITY_MAX: usize = 1 << 20;

/// Default bus shutdown drain timeout. Sized larger than typical TCP RTT
/// times in-flight write-batch so writers receive their full last
/// `write_vectored_all` budget before the connection registry kicks in.
pub const DEFAULT_SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// Default watchdog poll cadence for the cross-thread shutdown flag.
/// 50ms keeps Ctrl-C latency operator-visible without measurable wakeup
/// overhead.
pub const DEFAULT_SHUTDOWN_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Hard upper bound on `shutdown_drain_timeout`. A drain that never
/// completes wedges process exit; capping at 10 minutes guarantees the
/// watchdog eventually force-tears the bus even with a pathological
/// config typo.
pub const SHUTDOWN_DRAIN_TIMEOUT_MAX: Duration = Duration::from_secs(600);

/// Hard upper bound on `shutdown_poll_interval`. A pollerinterval longer
/// than the drain timeout makes the flag effectively unobservable; cap
/// at 5s so Ctrl-C latency stays bounded regardless of config.
pub const SHUTDOWN_POLL_INTERVAL_MAX: Duration = Duration::from_secs(5);

/// Default safety-tick cadence for the partition reconciliation loop.
/// The reconciler also wakes on every `LifecycleFrame::MetadataCommitTick`
/// broadcast by shard 0; this fallback covers dropped wake-ups (the wake
/// channel is intentionally capacity-1) and the initial post-bootstrap
/// convergence window before shard 0's first tick. One second is
/// invisible to operators yet keeps idle clusters from burning CPU
/// re-reading the same target snapshot.
pub const DEFAULT_RECONCILE_PERIODIC_INTERVAL: Duration = Duration::from_secs(1);

/// Hard upper bound on `reconcile_periodic_interval`. A tick longer
/// than ~30s makes post-failure recovery latency operator-visible; the
/// cap reins in pathological typos without disturbing reasonable
/// production values.
pub const RECONCILE_PERIODIC_INTERVAL_MAX: Duration = Duration::from_secs(30);

const fn default_inbox_capacity() -> usize {
    DEFAULT_INBOX_CAPACITY
}

fn default_shutdown_drain_timeout() -> IggyDuration {
    IggyDuration::new(DEFAULT_SHUTDOWN_DRAIN_TIMEOUT)
}

fn default_shutdown_poll_interval() -> IggyDuration {
    IggyDuration::new(DEFAULT_SHUTDOWN_POLL_INTERVAL)
}

fn default_reconcile_periodic_interval() -> IggyDuration {
    IggyDuration::new(DEFAULT_RECONCILE_PERIODIC_INTERVAL)
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize, ConfigEnv)]
pub struct ShardingConfig {
    #[serde(default)]
    #[config_env(leaf)]
    pub cpu_allocation: CpuAllocation,
    /// Per-shard inter-shard inbox channel capacity. Bounded by design.
    /// Drops on full inbox of consensus frames are recovered by VSR
    /// retransmit. Drops of cross-shard client Reply frames are terminal:
    /// the client never receives the reply (no in-protocol retransmit).
    /// Both frame classes share this one channel, so a consensus burst
    /// can starve client-reply forwards: size against the worst-case sum
    /// of consensus working set + peak client-reply fan-out per shard
    /// occurring together; see `DEFAULT_INBOX_CAPACITY` for the
    /// rationale. Used by `core/server-ng`; the legacy server uses its
    /// own hard-coded inbox sizing.
    ///
    // TODO(hubcio): split into two priority lanes - one bounded queue for
    // consensus frames (drops recovered by VSR retransmit) and one for
    // client `Reply` frames (drops terminal, must be sized for worst-case
    // fan-out). Current single-channel design is the minimum-viable
    // wiring so `frame_drops_total{variant,reason}` surfaces under load
    // and yields real numbers to size the split against.
    #[serde(default = "default_inbox_capacity")]
    pub inbox_capacity: usize,
    /// Wall-clock budget for a single shard's bus drain on shutdown.
    /// Drives `IggyMessageBus::shutdown(..)` from the per-shard watchdog
    /// and the parallel-join survivor path. Sized larger than typical
    /// TCP RTT times in-flight write-batch so writers receive their full
    /// last `write_vectored_all` budget before the connection registry
    /// force-tears the bus. Slow-fsync hosts may need to extend this past
    /// the default; the cap is `SHUTDOWN_DRAIN_TIMEOUT_MAX` so a config
    /// typo cannot wedge process exit.
    #[serde(default = "default_shutdown_drain_timeout")]
    #[serde_as(as = "DisplayFromStr")]
    #[config_env(leaf)]
    pub shutdown_drain_timeout: IggyDuration,
    /// Poll cadence for the cross-thread shutdown flag and for the
    /// `await_metadata_bundle` / `broadcast_metadata_bundle` poll loops.
    /// Trades off Ctrl-C latency against idle wakeup cost; the default
    /// keeps shutdown observably prompt without measurable scheduler
    /// overhead. Capped at `SHUTDOWN_POLL_INTERVAL_MAX` so the flag
    /// remains effectively observable regardless of config.
    #[serde(default = "default_shutdown_poll_interval")]
    #[serde_as(as = "DisplayFromStr")]
    #[config_env(leaf)]
    pub shutdown_poll_interval: IggyDuration,
    /// Safety-tick cadence for the partition reconciliation loop; the
    /// reconciler also wakes immediately on every
    /// `LifecycleFrame::MetadataCommitTick` from shard 0. See
    /// [`DEFAULT_RECONCILE_PERIODIC_INTERVAL`] for the rationale; values
    /// above [`RECONCILE_PERIODIC_INTERVAL_MAX`] are rejected by the
    /// validator.
    #[serde(default = "default_reconcile_periodic_interval")]
    #[serde_as(as = "DisplayFromStr")]
    #[config_env(leaf)]
    pub reconcile_periodic_interval: IggyDuration,
}

impl Default for ShardingConfig {
    fn default() -> Self {
        Self {
            cpu_allocation: CpuAllocation::default(),
            inbox_capacity: DEFAULT_INBOX_CAPACITY,
            shutdown_drain_timeout: default_shutdown_drain_timeout(),
            shutdown_poll_interval: default_shutdown_poll_interval(),
            reconcile_periodic_interval: default_reconcile_periodic_interval(),
        }
    }
}
