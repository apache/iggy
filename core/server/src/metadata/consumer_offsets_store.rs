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

//! Shared consumer offsets store for cross-shard offset visibility.
//!
//! When shard 0 creates a partition, it registers the ConsumerOffsets/ConsumerGroupOffsets Arcs here.
//! When other shards do lazy init, they retrieve the same Arcs so offset updates are visible.

use crate::streaming::partitions::partition::{ConsumerGroupOffsets, ConsumerOffsets};
use dashmap::DashMap;
use std::sync::Arc;

/// Thread-safe store for sharing consumer offsets Arcs across shards.
/// Uses DashMap for lock-free concurrent access.
#[derive(Debug, Default)]
pub struct SharedConsumerOffsetsStore {
    /// Consumer offsets indexed by (stream_id, topic_id, partition_id)
    consumer_offsets: DashMap<(usize, usize, usize), Arc<ConsumerOffsets>>,

    /// Consumer group offsets indexed by (stream_id, topic_id, partition_id)
    consumer_group_offsets: DashMap<(usize, usize, usize), Arc<ConsumerGroupOffsets>>,
}

impl SharedConsumerOffsetsStore {
    pub fn new() -> Self {
        Self {
            consumer_offsets: DashMap::new(),
            consumer_group_offsets: DashMap::new(),
        }
    }

    // Consumer offsets

    pub fn register_consumer_offsets(
        &self,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
        offsets: Arc<ConsumerOffsets>,
    ) {
        self.consumer_offsets
            .insert((stream_id, topic_id, partition_id), offsets);
    }

    pub fn get_consumer_offsets(
        &self,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
    ) -> Option<Arc<ConsumerOffsets>> {
        self.consumer_offsets
            .get(&(stream_id, topic_id, partition_id))
            .map(|r| Arc::clone(&r))
    }

    pub fn remove_consumer_offsets(&self, stream_id: usize, topic_id: usize, partition_id: usize) {
        self.consumer_offsets
            .remove(&(stream_id, topic_id, partition_id));
    }

    // Consumer group offsets

    pub fn register_consumer_group_offsets(
        &self,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
        offsets: Arc<ConsumerGroupOffsets>,
    ) {
        self.consumer_group_offsets
            .insert((stream_id, topic_id, partition_id), offsets);
    }

    pub fn get_consumer_group_offsets(
        &self,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
    ) -> Option<Arc<ConsumerGroupOffsets>> {
        self.consumer_group_offsets
            .get(&(stream_id, topic_id, partition_id))
            .map(|r| Arc::clone(&r))
    }

    pub fn remove_consumer_group_offsets(
        &self,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
    ) {
        self.consumer_group_offsets
            .remove(&(stream_id, topic_id, partition_id));
    }
}
