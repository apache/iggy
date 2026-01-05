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

//! Shared statistics store for cross-shard stats visibility.
//!
//! When shard 0 creates a stream/topic/partition, it registers the stats Arc here.
//! When other shards do lazy init, they retrieve the same Arc so updates are visible.
//!
//! Note: Partition offsets are stored in SharedMetadata's PartitionMeta, not here.

use crate::streaming::stats::{PartitionStats, StreamStats, TopicStats};
use dashmap::DashMap;
use std::sync::Arc;

/// Thread-safe store for sharing stats Arcs across shards.
/// Uses DashMap for lock-free concurrent access.
#[derive(Debug, Default)]
pub struct SharedStatsStore {
    /// Stream stats indexed by stream_id
    stream_stats: DashMap<usize, Arc<StreamStats>>,

    /// Topic stats indexed by (stream_id, topic_id)
    topic_stats: DashMap<(usize, usize), Arc<TopicStats>>,

    /// Partition stats indexed by (stream_id, topic_id, partition_id)
    partition_stats: DashMap<(usize, usize, usize), Arc<PartitionStats>>,
}

impl SharedStatsStore {
    pub fn new() -> Self {
        Self {
            stream_stats: DashMap::new(),
            topic_stats: DashMap::new(),
            partition_stats: DashMap::new(),
        }
    }

    // Stream stats

    pub fn initialize_stream(&self, stream_id: usize) {
        self.stream_stats
            .insert(stream_id, Arc::new(StreamStats::default()));
    }

    pub fn register_stream_stats(&self, stream_id: usize, stats: Arc<StreamStats>) {
        self.stream_stats.insert(stream_id, stats);
    }

    pub fn get_stream_stats(&self, stream_id: usize) -> Option<Arc<StreamStats>> {
        self.stream_stats.get(&stream_id).map(|r| Arc::clone(&r))
    }

    pub fn remove_stream_stats(&self, stream_id: usize) {
        self.stream_stats.remove(&stream_id);
    }

    // Topic stats

    pub fn initialize_topic(&self, stream_id: usize, topic_id: usize) {
        // Get parent stream stats to link them
        let parent_stats = self
            .stream_stats
            .get(&stream_id)
            .map(|r| Arc::clone(&r))
            .unwrap_or_else(|| Arc::new(StreamStats::default()));
        self.topic_stats.insert(
            (stream_id, topic_id),
            Arc::new(TopicStats::new(parent_stats)),
        );
    }

    pub fn register_topic_stats(&self, stream_id: usize, topic_id: usize, stats: Arc<TopicStats>) {
        self.topic_stats.insert((stream_id, topic_id), stats);
    }

    pub fn get_topic_stats(&self, stream_id: usize, topic_id: usize) -> Option<Arc<TopicStats>> {
        self.topic_stats
            .get(&(stream_id, topic_id))
            .map(|r| Arc::clone(&r))
    }

    pub fn remove_topic_stats(&self, stream_id: usize, topic_id: usize) {
        // Decrement stream stats before removing topic stats
        if let Some((_, topic_stats)) = self.topic_stats.remove(&(stream_id, topic_id)) {
            if let Some(stream_stats) = self.get_stream_stats(stream_id) {
                stream_stats.decrement_size_bytes(topic_stats.size_bytes_inconsistent());
                stream_stats.decrement_messages_count(topic_stats.messages_count_inconsistent());
                stream_stats.decrement_segments_count(topic_stats.segments_count_inconsistent());
            }
        }
    }

    // Partition stats

    pub fn initialize_partition(&self, stream_id: usize, topic_id: usize, partition_id: usize) {
        // Get parent topic stats to link them
        let parent_stats = self
            .topic_stats
            .get(&(stream_id, topic_id))
            .map(|r| Arc::clone(&r))
            .unwrap_or_else(|| {
                let stream_stats = Arc::new(StreamStats::default());
                Arc::new(TopicStats::new(stream_stats))
            });
        self.partition_stats.insert(
            (stream_id, topic_id, partition_id),
            Arc::new(PartitionStats::new(parent_stats)),
        );
    }

    pub fn register_partition_stats(
        &self,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
        stats: Arc<PartitionStats>,
    ) {
        self.partition_stats
            .insert((stream_id, topic_id, partition_id), stats);
    }

    pub fn get_partition_stats(
        &self,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
    ) -> Option<Arc<PartitionStats>> {
        self.partition_stats
            .get(&(stream_id, topic_id, partition_id))
            .map(|r| Arc::clone(&r))
    }

    pub fn remove_partition_stats(&self, stream_id: usize, topic_id: usize, partition_id: usize) {
        self.partition_stats
            .remove(&(stream_id, topic_id, partition_id));
    }

    /// Zero out all partition stats for a topic.
    /// Called during purge_topic to reset partition stats across all shards.
    /// Note: Partition offsets are zeroed separately via SharedMetadata.
    pub fn zero_out_topic_partition_stats(&self, stream_id: usize, topic_id: usize) {
        for entry in self.partition_stats.iter() {
            let (sid, tid, _pid) = entry.key();
            if *sid == stream_id && *tid == topic_id {
                entry.value().zero_out_all();
            }
        }
    }
}
