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

use super::PartitionData;
use iggy_common::sharding::IggyNamespace;
use std::{cell::RefCell, collections::HashMap, rc::Rc};

/// Rc-wrapped partition data with interior mutability.
///
/// This type allows:
/// - Cloning the Rc synchronously to extract a reference
/// - Using the reference across async boundaries
/// - Mutable access via `borrow_mut()` for append operations
pub type RcPartitionData = Rc<RefCell<PartitionData>>;

/// Per-shard storage for partition data.
///
/// This store holds only the partitions owned by this shard. Since each shard
/// runs on a single thread (compio runtime), no synchronization primitives are
/// needed - it's a plain `HashMap`.
///
/// Partition data is wrapped in `Rc<RefCell<_>>` to allow:
/// - Cloning references synchronously and using them across async boundaries
/// - Mutable access for append operations (via RefCell)
///
/// Partition ownership is determined by consistent hashing of `IggyNamespace`
/// (stream_id + topic_id + partition_id) to shard IDs.
#[derive(Debug, Default)]
pub struct PartitionDataStore {
    partitions: HashMap<IggyNamespace, RcPartitionData>,
}

impl PartitionDataStore {
    /// Create a new empty partition data store.
    pub fn new() -> Self {
        Self {
            partitions: HashMap::new(),
        }
    }

    /// Create a store with pre-allocated capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            partitions: HashMap::with_capacity(capacity),
        }
    }

    /// Get a reference to the Rc-wrapped partition data if it exists.
    pub fn get(&self, ns: &IggyNamespace) -> Option<&RcPartitionData> {
        self.partitions.get(ns)
    }

    /// Clone the Rc to partition data if it exists.
    ///
    /// This is the primary way to access partition data for async operations.
    /// Clone the Rc synchronously (while holding the outer store borrow), then
    /// drop the store borrow and use the Rc across await points.
    ///
    /// For mutable access: `data.borrow_mut()` (synchronous operations only)
    /// For read access: `data.borrow()` (can be used in async after cloning Rc)
    pub fn get_rc(&self, ns: &IggyNamespace) -> Option<RcPartitionData> {
        self.partitions.get(ns).cloned()
    }

    /// Get a mutable reference to the Rc if it exists.
    pub fn get_mut(&mut self, ns: &IggyNamespace) -> Option<&mut RcPartitionData> {
        self.partitions.get_mut(ns)
    }

    /// Check if a partition exists in the store.
    pub fn contains(&self, ns: &IggyNamespace) -> bool {
        self.partitions.contains_key(ns)
    }

    /// Insert partition data into the store.
    ///
    /// The data is wrapped in `Rc<RefCell<_>>` automatically.
    /// Returns the previous data if it existed.
    pub fn insert(&mut self, ns: IggyNamespace, data: PartitionData) -> Option<RcPartitionData> {
        self.partitions.insert(ns, Rc::new(RefCell::new(data)))
    }

    /// Insert an already-wrapped partition data into the store.
    ///
    /// Returns the previous data if it existed.
    pub fn insert_rc(
        &mut self,
        ns: IggyNamespace,
        data: RcPartitionData,
    ) -> Option<RcPartitionData> {
        self.partitions.insert(ns, data)
    }

    /// Remove partition data from the store.
    ///
    /// Returns the removed data if it existed.
    pub fn remove(&mut self, ns: &IggyNamespace) -> Option<RcPartitionData> {
        self.partitions.remove(ns)
    }

    /// Get or insert partition data into the store.
    ///
    /// If the partition already exists, returns a reference to it.
    /// If not, inserts the provided data wrapped in `Rc<RefCell<_>>`.
    pub fn get_or_insert(&mut self, ns: IggyNamespace, data: PartitionData) -> &RcPartitionData {
        self.partitions
            .entry(ns)
            .or_insert_with(|| Rc::new(RefCell::new(data)))
    }

    /// Get partition data or insert using a closure.
    ///
    /// This is useful when the data is expensive to create and you only
    /// want to create it if the partition doesn't already exist.
    pub fn get_or_insert_with<F>(&mut self, ns: IggyNamespace, f: F) -> &RcPartitionData
    where
        F: FnOnce() -> PartitionData,
    {
        self.partitions
            .entry(ns)
            .or_insert_with(|| Rc::new(RefCell::new(f())))
    }

    /// Get the number of partitions in the store.
    pub fn len(&self) -> usize {
        self.partitions.len()
    }

    /// Check if the store is empty.
    pub fn is_empty(&self) -> bool {
        self.partitions.is_empty()
    }

    /// Iterate over all partition data.
    pub fn iter(&self) -> impl Iterator<Item = (&IggyNamespace, &RcPartitionData)> {
        self.partitions.iter()
    }

    /// Iterate over all partition data mutably.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&IggyNamespace, &mut RcPartitionData)> {
        self.partitions.iter_mut()
    }

    /// Remove all partitions belonging to a stream.
    ///
    /// Called when a stream is deleted.
    pub fn remove_stream_partitions(
        &mut self,
        stream_id: usize,
    ) -> Vec<(IggyNamespace, RcPartitionData)> {
        let to_remove: Vec<_> = self
            .partitions
            .keys()
            .filter(|ns| ns.stream_id() == stream_id)
            .cloned()
            .collect();

        to_remove
            .into_iter()
            .filter_map(|ns| self.partitions.remove(&ns).map(|data| (ns, data)))
            .collect()
    }

    /// Remove all partitions belonging to a topic.
    ///
    /// Called when a topic is deleted.
    pub fn remove_topic_partitions(
        &mut self,
        stream_id: usize,
        topic_id: usize,
    ) -> Vec<(IggyNamespace, RcPartitionData)> {
        let to_remove: Vec<_> = self
            .partitions
            .keys()
            .filter(|ns| ns.stream_id() == stream_id && ns.topic_id() == topic_id)
            .cloned()
            .collect();

        to_remove
            .into_iter()
            .filter_map(|ns| self.partitions.remove(&ns).map(|data| (ns, data)))
            .collect()
    }

    /// Purge all partitions belonging to a topic.
    ///
    /// This resets each partition to empty state (offset=0, no segments, etc.)
    /// without removing them from the store. Called by purge_topic.
    pub fn purge_topic_partitions(&mut self, stream_id: usize, topic_id: usize) {
        for (ns, data) in self.partitions.iter_mut() {
            if ns.stream_id() == stream_id && ns.topic_id() == topic_id {
                data.borrow_mut().purge();
            }
        }
    }

    /// Get all namespaces in the store.
    pub fn namespaces(&self) -> impl Iterator<Item = &IggyNamespace> {
        self.partitions.keys()
    }

    /// Retain only partitions that satisfy the predicate.
    pub fn retain<F>(&mut self, f: F)
    where
        F: FnMut(&IggyNamespace, &mut RcPartitionData) -> bool,
    {
        self.partitions.retain(f);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_store_basic_operations() {
        let store = PartitionDataStore::new();
        assert!(store.is_empty());

        let ns = IggyNamespace::new(1, 2, 3);
        assert!(!store.contains(&ns));
    }
}
