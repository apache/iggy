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

use serde::{Deserialize, Serialize};

use super::defaults::SERVER_CONFIG;
use configs::ConfigEnv;

<<<<<<< Updated upstream
// `CpuAllocation`/`NumaConfig` are pure config types and live in their own
// leaf crate so both `configs` and `shard_allocator` can share them without
// pulling each other's heavier dependency trees. Re-exported here to keep the
// `configs::sharding::*` path stable for existing callers.
pub use cpu_allocation::{CpuAllocation, NumaConfig};
=======
/// Default capacity of the per-shard inter-shard inbox channel. Sized
/// comfortably above the consensus working set, which is roughly
/// `PIPELINE_PREPARE_QUEUE_MAX (= 32) * replica_count * directions`
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
>>>>>>> Stashed changes

/// Sharding config for the legacy `core/server`. That server consumes only
/// `cpu_allocation` and `pin_cores`; the bus / shutdown / reconcile knobs are
/// server-ng concepts and live in [`crate::server_ng_config::sharding`].
#[derive(Debug, Deserialize, Serialize, ConfigEnv)]
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
}

impl Default for ShardingConfig {
    fn default() -> Self {
        Self {
            cpu_allocation: CpuAllocation::default(),
            pin_cores: SERVER_CONFIG.system.sharding.pin_cores,
        }
    }
}
