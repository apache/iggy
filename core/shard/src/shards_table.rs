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

use hash32::{Hasher, Murmur3Hasher};
use iggy_common::sharding::{IggyNamespace, PartitionLocation};
use std::hash::Hasher as _;

/// Lookup table that maps partition namespaces to their owning shard.
///
/// Implementations can be:
/// - A shared concurrent map (`DashMap`, papaya, etc.) referenced by all shards.
/// - A per-shard local `HashMap` replica, updated via a
///   broadcast when partitions are created, deleted, or moved.
pub trait ShardsTable {
    /// Returns the shard id that owns `namespace`, or `None` if the
    /// namespace is not yet registered (partition not created or update
    /// hasn't propagated).
    fn shard_for(&self, namespace: IggyNamespace) -> Option<u16>;
}

/// No-op shards table for single-shard setups.
impl ShardsTable for () {
    fn shard_for(&self, _namespace: IggyNamespace) -> Option<u16> {
        None
    }
}

/// Lock-free shards table backed by [`papaya::HashMap`].
pub struct PapayaShardsTable {
    inner: papaya::HashMap<IggyNamespace, PartitionLocation>,
}

impl Default for PapayaShardsTable {
    fn default() -> Self {
        Self::new()
    }
}

impl PapayaShardsTable {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: papaya::HashMap::new(),
        }
    }

    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: papaya::HashMap::with_capacity(capacity),
        }
    }

    pub fn insert(&self, namespace: IggyNamespace, location: PartitionLocation) {
        self.inner.pin().insert(namespace, location);
    }

    pub fn remove(&self, namespace: &IggyNamespace) -> Option<PartitionLocation> {
        let guard = self.inner.guard();
        self.inner.remove(namespace, &guard).copied()
    }
}

impl ShardsTable for PapayaShardsTable {
    fn shard_for(&self, namespace: IggyNamespace) -> Option<u16> {
        let guard = self.inner.guard();
        self.inner.get(&namespace, &guard).map(|loc| *loc.shard_id)
    }
}

/// Deterministic partition-to-shard assignment using Murmur3 hash.
///
/// Given a packed `IggyNamespace` and the total number of shards, returns the
/// shard id that should own the partition.  The upper bits of the Murmur3 hash
/// are used to avoid the weak lower bits for small integer inputs.
#[must_use]
pub fn calculate_shard_assignment(ns: &IggyNamespace, shard_count: u32) -> u16 {
    let mut hasher = Murmur3Hasher::default();
    hasher.write_u64(ns.inner());
    let hash = hasher.finish32();
    ((hash >> 16) % shard_count) as u16
}
