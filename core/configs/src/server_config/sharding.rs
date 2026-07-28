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

// `CpuAllocation`/`NumaConfig` are pure config types and live in their own
// leaf crate so both `configs` and `shard_allocator` can share them without
// pulling each other's heavier dependency trees. Re-exported here to keep the
// `configs::sharding::*` path stable for existing callers.
pub use cpu_allocation::{CpuAllocation, NumaConfig};

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
