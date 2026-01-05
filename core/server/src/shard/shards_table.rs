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

//! Shard routing table mapping partitions to shard IDs.
//!
//! Uses ArcSwap + imbl::HashMap for lock-free reads and structural sharing
//! on updates. Each shard can read the table without locks, and updates
//! (which happen only during partition creation/deletion) use RCU pattern.

use arc_swap::{ArcSwap, Guard};
use iggy_common::sharding::{IggyNamespace, PartitionLocation};
use imbl::HashMap as ImHashMap;
use std::sync::Arc;

/// Thread-safe shard routing table.
///
/// Maps `IggyNamespace` (stream_id + topic_id + partition_id) to `PartitionLocation`.
/// Uses ArcSwap for lock-free reads and imbl::HashMap for structural sharing.
#[derive(Debug)]
pub struct ShardsTable {
    inner: ArcSwap<ImHashMap<IggyNamespace, PartitionLocation>>,
}

impl Default for ShardsTable {
    fn default() -> Self {
        Self::new()
    }
}

impl ShardsTable {
    /// Create a new empty shards table.
    pub fn new() -> Self {
        Self {
            inner: ArcSwap::from_pointee(ImHashMap::new()),
        }
    }

    /// Create a shards table from an existing map.
    pub fn from_map(map: ImHashMap<IggyNamespace, PartitionLocation>) -> Self {
        Self {
            inner: ArcSwap::from_pointee(map),
        }
    }

    /// Load the current snapshot. Lock-free read.
    pub fn load(&self) -> Guard<Arc<ImHashMap<IggyNamespace, PartitionLocation>>> {
        self.inner.load()
    }

    /// Get the partition location for a namespace.
    pub fn get(&self, ns: &IggyNamespace) -> Option<PartitionLocation> {
        self.inner.load().get(ns).copied()
    }

    /// Check if a namespace exists in the table.
    pub fn contains(&self, ns: &IggyNamespace) -> bool {
        self.inner.load().contains_key(ns)
    }

    /// Insert a namespace -> location mapping.
    ///
    /// Returns the previous location if the namespace already existed.
    pub fn insert(
        &self,
        ns: IggyNamespace,
        location: PartitionLocation,
    ) -> Option<PartitionLocation> {
        let mut result = None;
        self.inner.rcu(|current| {
            result = current.get(&ns).copied();
            Arc::new(current.update(ns, location))
        });
        result
    }

    /// Remove a namespace from the table.
    ///
    /// Returns the removed (namespace, location) pair if it existed.
    pub fn remove(&self, ns: &IggyNamespace) -> Option<(IggyNamespace, PartitionLocation)> {
        let mut result = None;
        self.inner.rcu(|current| {
            if let Some(&location) = current.get(ns) {
                result = Some((*ns, location));
                Arc::new(current.without(ns))
            } else {
                Arc::clone(current)
            }
        });
        result
    }

    /// Get the number of entries in the table.
    pub fn len(&self) -> usize {
        self.inner.load().len()
    }

    /// Check if the table is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.load().is_empty()
    }

    /// Iterate over all entries.
    ///
    /// Returns a snapshot iterator - changes during iteration won't be seen.
    /// Uses imbl's structural sharing so clone is O(1).
    pub fn iter(&self) -> impl Iterator<Item = (IggyNamespace, PartitionLocation)> {
        let snapshot = (**self.inner.load()).clone();
        snapshot.into_iter()
    }

    /// Get all namespaces assigned to a specific shard.
    pub fn namespaces_for_shard(&self, shard_id: u16) -> Vec<IggyNamespace> {
        self.inner
            .load()
            .iter()
            .filter_map(|(ns, location)| {
                if *location.shard_id == shard_id {
                    Some(*ns)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Remove all namespaces for a given stream.
    ///
    /// Returns the removed entries.
    pub fn remove_stream(&self, stream_id: usize) -> Vec<(IggyNamespace, PartitionLocation)> {
        let mut removed = Vec::new();
        self.inner.rcu(|current| {
            let mut new_map = (**current).clone();
            let to_remove: Vec<_> = new_map
                .keys()
                .filter(|ns| ns.stream_id() == stream_id)
                .cloned()
                .collect();

            for ns in to_remove {
                if let Some(location) = new_map.remove(&ns) {
                    removed.push((ns, location));
                }
            }
            Arc::new(new_map)
        });
        removed
    }

    /// Remove all namespaces for a given topic.
    ///
    /// Returns the removed entries.
    pub fn remove_topic(
        &self,
        stream_id: usize,
        topic_id: usize,
    ) -> Vec<(IggyNamespace, PartitionLocation)> {
        let mut removed = Vec::new();
        self.inner.rcu(|current| {
            let mut new_map = (**current).clone();
            let to_remove: Vec<_> = new_map
                .keys()
                .filter(|ns| ns.stream_id() == stream_id && ns.topic_id() == topic_id)
                .cloned()
                .collect();

            for ns in to_remove {
                if let Some(location) = new_map.remove(&ns) {
                    removed.push((ns, location));
                }
            }
            Arc::new(new_map)
        });
        removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_common::sharding::{LocalIdx, ShardId};

    fn make_location(shard: u16) -> PartitionLocation {
        PartitionLocation::new(ShardId::new(shard), LocalIdx::new(0))
    }

    #[test]
    fn test_insert_and_get() {
        let table = ShardsTable::new();
        let ns = IggyNamespace::new(1, 2, 3);
        let location = make_location(5);

        assert!(table.get(&ns).is_none());

        table.insert(ns, location);
        assert_eq!(table.get(&ns), Some(location));
    }

    #[test]
    fn test_remove() {
        let table = ShardsTable::new();
        let ns = IggyNamespace::new(1, 2, 3);
        let location = make_location(5);

        table.insert(ns, location);
        let removed = table.remove(&ns);

        assert_eq!(removed, Some((ns, location)));
        assert!(table.get(&ns).is_none());
    }

    #[test]
    fn test_iter() {
        let table = ShardsTable::new();
        table.insert(IggyNamespace::new(1, 1, 0), make_location(0));
        table.insert(IggyNamespace::new(1, 1, 1), make_location(1));
        table.insert(IggyNamespace::new(1, 1, 2), make_location(0));

        let entries: Vec<_> = table.iter().collect();
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn test_namespaces_for_shard() {
        let table = ShardsTable::new();
        table.insert(IggyNamespace::new(1, 1, 0), make_location(0));
        table.insert(IggyNamespace::new(1, 1, 1), make_location(1));
        table.insert(IggyNamespace::new(1, 1, 2), make_location(0));

        let shard0_ns = table.namespaces_for_shard(0);
        assert_eq!(shard0_ns.len(), 2);

        let shard1_ns = table.namespaces_for_shard(1);
        assert_eq!(shard1_ns.len(), 1);
    }
}
