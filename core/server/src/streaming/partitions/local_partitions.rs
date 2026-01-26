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

//! Per-shard partition storage.
//!
//! Single-threaded (compio runtime) - NO synchronization needed!

use super::local_partition::LocalPartition;
use iggy_common::sharding::IggyNamespace;
use std::collections::HashMap;

/// Per-shard partition storage.
/// Single-threaded (compio runtime) - NO synchronization needed!
#[derive(Debug, Default)]
pub struct LocalPartitions {
    partitions: HashMap<IggyNamespace, LocalPartition>,
}

impl LocalPartitions {
    pub fn new() -> Self {
        Self {
            partitions: HashMap::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            partitions: HashMap::with_capacity(capacity),
        }
    }

    #[inline]
    pub fn get(&self, ns: &IggyNamespace) -> Option<&LocalPartition> {
        self.partitions.get(ns)
    }

    #[inline]
    pub fn get_mut(&mut self, ns: &IggyNamespace) -> Option<&mut LocalPartition> {
        self.partitions.get_mut(ns)
    }

    #[inline]
    pub fn insert(&mut self, ns: IggyNamespace, data: LocalPartition) {
        self.partitions.insert(ns, data);
    }

    #[inline]
    pub fn remove(&mut self, ns: &IggyNamespace) -> Option<LocalPartition> {
        self.partitions.remove(ns)
    }

    #[inline]
    pub fn contains(&self, ns: &IggyNamespace) -> bool {
        self.partitions.contains_key(ns)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.partitions.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.partitions.is_empty()
    }

    /// Iterate over all namespaces owned by this shard.
    pub fn namespaces(&self) -> impl Iterator<Item = &IggyNamespace> {
        self.partitions.keys()
    }

    /// Iterate over all partition data.
    pub fn iter(&self) -> impl Iterator<Item = (&IggyNamespace, &LocalPartition)> {
        self.partitions.iter()
    }

    /// Iterate over all partition data mutably.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&IggyNamespace, &mut LocalPartition)> {
        self.partitions.iter_mut()
    }

    /// Remove multiple partitions at once.
    pub fn remove_many(&mut self, namespaces: &[IggyNamespace]) -> Vec<LocalPartition> {
        namespaces
            .iter()
            .filter_map(|ns| self.partitions.remove(ns))
            .collect()
    }

    /// Get partition data, initializing if not present.
    pub fn get_or_init<F>(&mut self, ns: IggyNamespace, init: F) -> &mut LocalPartition
    where
        F: FnOnce() -> LocalPartition,
    {
        self.partitions.entry(ns).or_insert_with(init)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming::{
        partitions::{
            consumer_group_offsets::ConsumerGroupOffsets, consumer_offsets::ConsumerOffsets,
        },
        stats::{PartitionStats, StreamStats, TopicStats},
    };
    use iggy_common::IggyTimestamp;
    use std::sync::{Arc, atomic::AtomicU64};

    fn create_test_partition() -> LocalPartition {
        let stream_stats = Arc::new(StreamStats::default());
        let topic_stats = Arc::new(TopicStats::new(stream_stats));
        let partition_stats = Arc::new(PartitionStats::new(topic_stats));

        LocalPartition::new(
            partition_stats,
            Arc::new(AtomicU64::new(0)),
            Arc::new(ConsumerOffsets::with_capacity(10)),
            Arc::new(ConsumerGroupOffsets::with_capacity(10)),
            None,
            IggyTimestamp::now(),
            1,
            true,
        )
    }

    #[test]
    fn test_basic_operations() {
        let mut partitions = LocalPartitions::new();
        let ns = IggyNamespace::new(1, 1, 0);

        assert!(!partitions.contains(&ns));
        assert!(partitions.is_empty());

        partitions.insert(ns, create_test_partition());

        assert!(partitions.contains(&ns));
        assert_eq!(partitions.len(), 1);
        assert!(partitions.get(&ns).is_some());
        assert!(partitions.get_mut(&ns).is_some());

        let removed = partitions.remove(&ns);
        assert!(removed.is_some());
        assert!(!partitions.contains(&ns));
        assert!(partitions.is_empty());
    }

    #[test]
    fn test_iteration() {
        let mut partitions = LocalPartitions::new();
        let ns1 = IggyNamespace::new(1, 1, 0);
        let ns2 = IggyNamespace::new(1, 1, 1);
        let ns3 = IggyNamespace::new(1, 2, 0);

        partitions.insert(ns1, create_test_partition());
        partitions.insert(ns2, create_test_partition());
        partitions.insert(ns3, create_test_partition());

        let namespaces: Vec<_> = partitions.namespaces().collect();
        assert_eq!(namespaces.len(), 3);

        let pairs: Vec<_> = partitions.iter().collect();
        assert_eq!(pairs.len(), 3);
    }

    #[test]
    fn test_remove_many() {
        let mut partitions = LocalPartitions::new();
        let ns1 = IggyNamespace::new(1, 1, 0);
        let ns2 = IggyNamespace::new(1, 1, 1);
        let ns3 = IggyNamespace::new(1, 2, 0);

        partitions.insert(ns1, create_test_partition());
        partitions.insert(ns2, create_test_partition());
        partitions.insert(ns3, create_test_partition());

        let removed = partitions.remove_many(&[ns1, ns2]);
        assert_eq!(removed.len(), 2);
        assert!(!partitions.contains(&ns1));
        assert!(!partitions.contains(&ns2));
        assert!(partitions.contains(&ns3));
    }

    #[test]
    fn test_get_or_init() {
        let mut partitions = LocalPartitions::new();
        let ns = IggyNamespace::new(1, 1, 0);

        assert!(!partitions.contains(&ns));

        let _ = partitions.get_or_init(ns, create_test_partition);
        assert!(partitions.contains(&ns));

        // Second call should not reinitialize
        let data = partitions.get_or_init(ns, || panic!("Should not be called"));
        assert!(data.should_increment_offset);
    }
}
