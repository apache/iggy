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

//! Runtime tunables for the shard-0 coordinator.
//!
//! The serde-facing section lives in `core/configs` as
//! `[cluster.coordinator]` (`ClusterCoordinatorConfig`); the server's
//! bootstrap converts it into this domain type. The split exists because
//! `configs` and `shard` share no dependency edge.

/// Tunables for [`crate::coordinator::ShardZeroCoordinator`].
#[derive(Debug, Clone)]
pub struct CoordinatorConfig {
    /// When `total_shards > 1`, exclude shard 0 from replica placement.
    /// Shard 0 already hosts the coordinator, the metadata writer, and
    /// both listeners; replicas are long-lived steady flows, so offload
    /// them to peer shards by default.
    pub skip_shard_zero_for_replicas: bool,

    /// When `total_shards > 1`, exclude shard 0 from client placement.
    /// Default false: shard 0 continues to serve client traffic because
    /// client connections are short-lived and benefit from shard-0
    /// parallelism more than replicas do.
    pub skip_shard_zero_for_clients: bool,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            skip_shard_zero_for_replicas: true,
            skip_shard_zero_for_clients: false,
        }
    }
}

/// Owner-local deferred poll limits. Running reads retain their byte reservation
/// until I/O and completion delivery end, even if their caller disconnects.
#[derive(Debug, Clone, Copy)]
pub struct DeferredPollConfig {
    pub max_wait_us: u64,
    pub max_pending: usize,
    pub max_pending_per_session: usize,
    pub max_read_bytes: usize,
    pub max_inflight_bytes: usize,
}

impl Default for DeferredPollConfig {
    fn default() -> Self {
        Self {
            max_wait_us: 30_000_000,
            max_pending: 1024,
            max_pending_per_session: 64,
            max_read_bytes: 16 * 1024 * 1024,
            max_inflight_bytes: 64 * 1024 * 1024,
        }
    }
}
