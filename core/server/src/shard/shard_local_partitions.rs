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

//! Per-shard partition data storage.
//!
//! Each shard runs on a single-threaded compio runtime, so no synchronization
//! is needed. Plain HashMap provides maximum performance.

use crate::streaming::{
    deduplication::message_deduplicator::MessageDeduplicator,
    partitions::{
        journal::MemoryMessageJournal,
        log::SegmentedLog,
        partition::{ConsumerGroupOffsets, ConsumerOffsets},
    },
    stats::PartitionStats,
};
use iggy_common::IggyTimestamp;
use iggy_common::sharding::IggyNamespace;
use std::{
    collections::HashMap,
    sync::{Arc, atomic::AtomicU64},
};

#[derive(Debug)]
pub struct PartitionData {
    pub log: SegmentedLog<MemoryMessageJournal>,
    pub offset: Arc<AtomicU64>,
    pub consumer_offsets: Arc<ConsumerOffsets>,
    pub consumer_group_offsets: Arc<ConsumerGroupOffsets>,
    pub message_deduplicator: Option<Arc<MessageDeduplicator>>,
    pub stats: Arc<PartitionStats>,
    pub created_at: IggyTimestamp,
    pub revision_id: u64,
    pub should_increment_offset: bool,
}

impl PartitionData {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        stats: Arc<PartitionStats>,
        offset: Arc<AtomicU64>,
        consumer_offsets: Arc<ConsumerOffsets>,
        consumer_group_offsets: Arc<ConsumerGroupOffsets>,
        message_deduplicator: Option<Arc<MessageDeduplicator>>,
        created_at: IggyTimestamp,
        revision_id: u64,
        should_increment_offset: bool,
    ) -> Self {
        Self {
            log: SegmentedLog::default(),
            offset,
            consumer_offsets,
            consumer_group_offsets,
            message_deduplicator,
            stats,
            created_at,
            revision_id,
            should_increment_offset,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn with_log(
        log: SegmentedLog<MemoryMessageJournal>,
        stats: Arc<PartitionStats>,
        offset: Arc<AtomicU64>,
        consumer_offsets: Arc<ConsumerOffsets>,
        consumer_group_offsets: Arc<ConsumerGroupOffsets>,
        message_deduplicator: Option<Arc<MessageDeduplicator>>,
        created_at: IggyTimestamp,
        revision_id: u64,
        should_increment_offset: bool,
    ) -> Self {
        Self {
            log,
            offset,
            consumer_offsets,
            consumer_group_offsets,
            message_deduplicator,
            stats,
            created_at,
            revision_id,
            should_increment_offset,
        }
    }
}

/// Per-shard partition data storage.
/// Single-threaded (compio runtime) - NO synchronization needed!
#[derive(Debug, Default)]
pub struct ShardLocalPartitions {
    partitions: HashMap<IggyNamespace, PartitionData>,
}

impl ShardLocalPartitions {
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
    pub fn get(&self, ns: &IggyNamespace) -> Option<&PartitionData> {
        self.partitions.get(ns)
    }

    #[inline]
    pub fn get_mut(&mut self, ns: &IggyNamespace) -> Option<&mut PartitionData> {
        self.partitions.get_mut(ns)
    }

    #[inline]
    pub fn insert(&mut self, ns: IggyNamespace, data: PartitionData) {
        self.partitions.insert(ns, data);
    }

    #[inline]
    pub fn remove(&mut self, ns: &IggyNamespace) -> Option<PartitionData> {
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
    pub fn iter(&self) -> impl Iterator<Item = (&IggyNamespace, &PartitionData)> {
        self.partitions.iter()
    }

    /// Iterate over all partition data mutably.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&IggyNamespace, &mut PartitionData)> {
        self.partitions.iter_mut()
    }

    /// Remove multiple partitions at once.
    pub fn remove_many(&mut self, namespaces: &[IggyNamespace]) -> Vec<PartitionData> {
        namespaces
            .iter()
            .filter_map(|ns| self.partitions.remove(ns))
            .collect()
    }

    /// Get partition data, initializing if not present.
    /// Returns None if initialization fails.
    pub fn get_or_init<F>(&mut self, ns: IggyNamespace, init: F) -> &mut PartitionData
    where
        F: FnOnce() -> PartitionData,
    {
        self.partitions.entry(ns).or_insert_with(init)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming::stats::{StreamStats, TopicStats};

    fn create_test_partition_data() -> PartitionData {
        let stream_stats = Arc::new(StreamStats::default());
        let topic_stats = Arc::new(TopicStats::new(stream_stats));
        let partition_stats = Arc::new(PartitionStats::new(topic_stats));

        PartitionData::new(
            partition_stats,
            Arc::new(AtomicU64::new(0)),
            Arc::new(ConsumerOffsets::with_capacity(10)),
            Arc::new(ConsumerGroupOffsets::with_capacity(10)),
            None,
            IggyTimestamp::now(),
            1, // revision_id
            true,
        )
    }

    #[test]
    fn test_basic_operations() {
        let mut store = ShardLocalPartitions::new();
        let ns = IggyNamespace::new(1, 1, 0);

        assert!(!store.contains(&ns));
        assert!(store.is_empty());

        store.insert(ns, create_test_partition_data());

        assert!(store.contains(&ns));
        assert_eq!(store.len(), 1);
        assert!(store.get(&ns).is_some());
        assert!(store.get_mut(&ns).is_some());

        let removed = store.remove(&ns);
        assert!(removed.is_some());
        assert!(!store.contains(&ns));
        assert!(store.is_empty());
    }

    #[test]
    fn test_iteration() {
        let mut store = ShardLocalPartitions::new();
        let ns1 = IggyNamespace::new(1, 1, 0);
        let ns2 = IggyNamespace::new(1, 1, 1);
        let ns3 = IggyNamespace::new(1, 2, 0);

        store.insert(ns1, create_test_partition_data());
        store.insert(ns2, create_test_partition_data());
        store.insert(ns3, create_test_partition_data());

        let namespaces: Vec<_> = store.namespaces().collect();
        assert_eq!(namespaces.len(), 3);

        let pairs: Vec<_> = store.iter().collect();
        assert_eq!(pairs.len(), 3);
    }

    #[test]
    fn test_remove_many() {
        let mut store = ShardLocalPartitions::new();
        let ns1 = IggyNamespace::new(1, 1, 0);
        let ns2 = IggyNamespace::new(1, 1, 1);
        let ns3 = IggyNamespace::new(1, 2, 0);

        store.insert(ns1, create_test_partition_data());
        store.insert(ns2, create_test_partition_data());
        store.insert(ns3, create_test_partition_data());

        let removed = store.remove_many(&[ns1, ns2]);
        assert_eq!(removed.len(), 2);
        assert!(!store.contains(&ns1));
        assert!(!store.contains(&ns2));
        assert!(store.contains(&ns3));
    }

    #[test]
    fn test_get_or_init() {
        let mut store = ShardLocalPartitions::new();
        let ns = IggyNamespace::new(1, 1, 0);

        assert!(!store.contains(&ns));

        let _ = store.get_or_init(ns, create_test_partition_data);
        assert!(store.contains(&ns));

        // Second call should not reinitialize
        let data = store.get_or_init(ns, || panic!("Should not be called"));
        assert!(data.should_increment_offset);
    }
}
