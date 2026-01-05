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

use crate::metadata::{ConsumerGroupMeta, PartitionMeta};
use iggy_common::{CompressionAlgorithm, IggyExpiry, IggyTimestamp, MaxTopicSize};
use imbl::HashMap as ImHashMap;

/// Topic metadata stored in the shared snapshot.
#[derive(Debug, Clone)]
pub struct TopicMeta {
    pub id: usize,
    pub name: String,
    pub created_at: IggyTimestamp,
    pub replication_factor: u8,
    pub message_expiry: IggyExpiry,
    pub compression_algorithm: CompressionAlgorithm,
    pub max_topic_size: MaxTopicSize,

    /// Partition metadata indexed by partition ID
    pub partitions: ImHashMap<usize, PartitionMeta>,

    /// Consumer group metadata indexed by group ID
    pub consumer_groups: ImHashMap<usize, ConsumerGroupMeta>,

    /// Consumer group name to ID index
    pub consumer_group_index: ImHashMap<String, usize>,

    /// Next partition ID for round-robin assignment (not cloned, use atomic)
    next_partition_id: usize,
}

impl TopicMeta {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: usize,
        name: String,
        created_at: IggyTimestamp,
        replication_factor: u8,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
    ) -> Self {
        Self {
            id,
            name,
            created_at,
            replication_factor,
            message_expiry,
            compression_algorithm,
            max_topic_size,
            partitions: ImHashMap::new(),
            consumer_groups: ImHashMap::new(),
            consumer_group_index: ImHashMap::new(),
            next_partition_id: 0,
        }
    }

    pub fn partitions_count(&self) -> u32 {
        self.partitions.len() as u32
    }

    pub fn add_partition(&mut self, partition: PartitionMeta) {
        self.partitions.insert(partition.id, partition);
    }

    pub fn remove_partition(&mut self, partition_id: usize) -> Option<PartitionMeta> {
        self.partitions.remove(&partition_id)
    }

    pub fn add_consumer_group(&mut self, group: ConsumerGroupMeta) {
        self.consumer_group_index
            .insert(group.name.clone(), group.id);
        self.consumer_groups.insert(group.id, group);
    }

    pub fn remove_consumer_group(&mut self, group_id: usize) -> Option<ConsumerGroupMeta> {
        if let Some(group) = self.consumer_groups.remove(&group_id) {
            self.consumer_group_index.remove(&group.name);
            Some(group)
        } else {
            None
        }
    }

    pub fn get_consumer_group_id_by_name(&self, name: &str) -> Option<usize> {
        self.consumer_group_index.get(name).copied()
    }

    pub fn get_next_partition_id(&self, upperbound: usize) -> usize {
        if upperbound == 0 {
            return 0;
        }
        self.next_partition_id % upperbound
    }

    pub fn increment_partition_counter(&mut self) {
        self.next_partition_id = self.next_partition_id.wrapping_add(1);
    }
}
