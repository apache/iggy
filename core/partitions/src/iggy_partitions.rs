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

#![allow(dead_code)]

use crate::IggyPartition;
use iggy_common::sharding::{IggyNamespace, LocalIdx, ShardId};
use std::collections::HashMap;

/// Per-shard collection of all partitions.
///
/// This struct manages ALL partitions assigned to a single shard, regardless
/// of which stream/topic they belong to.
///
/// Note: The partition_id within IggyNamespace may NOT equal the Vec index.
/// For example, shard 0 might have partition_ids [0, 2, 4] while shard 1
/// has partition_ids [1, 3, 5]. The `LocalIdx` provides the actual index
/// into the `partitions` Vec.
pub struct IggyPartitions {
    shard_id: ShardId,
    partitions: Vec<IggyPartition>,
    namespace_to_local: HashMap<IggyNamespace, LocalIdx>,
}

impl IggyPartitions {
    pub fn new(shard_id: ShardId) -> Self {
        Self {
            shard_id,
            partitions: Vec::new(),
            namespace_to_local: HashMap::new(),
        }
    }

    pub fn with_capacity(shard_id: ShardId, capacity: usize) -> Self {
        Self {
            shard_id,
            partitions: Vec::with_capacity(capacity),
            namespace_to_local: HashMap::with_capacity(capacity),
        }
    }

    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    pub fn len(&self) -> usize {
        self.partitions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.partitions.is_empty()
    }

    /// Get partition by local index.
    pub fn get(&self, local_idx: LocalIdx) -> Option<&IggyPartition> {
        self.partitions.get(*local_idx)
    }

    /// Get mutable partition by local index.
    pub fn get_mut(&mut self, local_idx: LocalIdx) -> Option<&mut IggyPartition> {
        self.partitions.get_mut(*local_idx)
    }

    /// Lookup local index by namespace.
    pub fn local_idx(&self, namespace: &IggyNamespace) -> Option<LocalIdx> {
        self.namespace_to_local.get(namespace).copied()
    }

    /// Insert a new partition and return its local index.
    pub fn insert(&mut self, namespace: IggyNamespace, partition: IggyPartition) -> LocalIdx {
        let local_idx = LocalIdx::new(self.partitions.len());
        self.partitions.push(partition);
        self.namespace_to_local.insert(namespace, local_idx);
        local_idx
    }

    /// Remove a partition by namespace. Returns the removed partition if found.
    pub fn remove(&mut self, namespace: &IggyNamespace) -> Option<IggyPartition> {
        // TODO(hubcio): consider adding reverse map `LocalIdx → IggyNamespace` for O(1)
        // updates, or use a different data structure (e.g., slotmap) if removal is frequent.

        let local_idx = self.namespace_to_local.remove(namespace)?;
        let idx = *local_idx;

        if idx >= self.partitions.len() {
            return None;
        }

        // Swap-remove for O(1) deletion
        let partition = self.partitions.swap_remove(idx);

        // If we swapped an element, update its index in the map
        if idx < self.partitions.len() {
            // Find the namespace that was at the last position (now at idx)
            for (_ns, lidx) in self.namespace_to_local.iter_mut() {
                if **lidx == self.partitions.len() {
                    *lidx = LocalIdx::new(idx);
                    break;
                }
            }
        }

        Some(partition)
    }
}
