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

use iggy_common::IggyTimestamp;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

/// Partition metadata stored in the shared snapshot.
/// The actual partition data (segments, journals) is stored per-shard.
///
/// The offset and should_increment_offset Arcs are shared across all shards -
/// when cloned via ArcSwap, all shards get the same underlying atomics.
#[derive(Debug, Clone)]
pub struct PartitionMeta {
    pub id: usize,
    pub created_at: IggyTimestamp,
    /// Current message offset for this partition.
    /// Shared across all shards via Arc - all shards read/write the same AtomicU64.
    pub offset: Arc<AtomicU64>,
    /// Whether offset should be incremented on next append (false for empty partitions).
    /// Shared across all shards via Arc.
    pub should_increment_offset: Arc<AtomicBool>,
}

impl PartitionMeta {
    pub fn new(id: usize, created_at: IggyTimestamp) -> Self {
        Self {
            id,
            created_at,
            offset: Arc::new(AtomicU64::new(0)),
            should_increment_offset: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Create with existing offset/should_increment Arcs (used when loading from disk).
    pub fn with_offset(
        id: usize,
        created_at: IggyTimestamp,
        offset: Arc<AtomicU64>,
        should_increment_offset: Arc<AtomicBool>,
    ) -> Self {
        Self {
            id,
            created_at,
            offset,
            should_increment_offset,
        }
    }

    /// Get current offset value.
    #[inline]
    pub fn current_offset(&self) -> u64 {
        self.offset.load(Ordering::Relaxed)
    }

    /// Calculate the next offset for appending messages.
    #[inline]
    pub fn next_offset(&self) -> u64 {
        if self.should_increment_offset.load(Ordering::Relaxed) {
            self.offset.load(Ordering::Relaxed) + 1
        } else {
            0
        }
    }

    /// Store the new offset after appending messages.
    #[inline]
    pub fn store_offset(&self, offset: u64) {
        self.offset.store(offset, Ordering::Relaxed);
        // After first store, always increment
        self.should_increment_offset.store(true, Ordering::Relaxed);
    }
}
