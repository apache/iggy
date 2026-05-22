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
//! TODO: move this module into `core/configs` (as a `CoordinatorConfig`
//! section nested under `ClusterConfig`) once downstream bootstrap
//! wiring that constructs [`crate::coordinator::ShardZeroCoordinator`]
//! from `ServerConfig` lands. Kept in-crate for now to avoid churning
//! the configs crate ahead of that wiring.

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
