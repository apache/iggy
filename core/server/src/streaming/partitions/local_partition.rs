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

//! Per-shard partition data.
//!
//! Each shard runs on a single-threaded compio runtime, so per-shard data
//! needs NO synchronization.

use super::{
    consumer_group_offsets::ConsumerGroupOffsets, consumer_offsets::ConsumerOffsets,
    journal::MemoryMessageJournal, log::SegmentedLog,
};
use crate::streaming::{deduplication::MessageDeduplicator, stats::PartitionStats};
use iggy_common::IggyTimestamp;
use std::sync::{Arc, atomic::AtomicU64};
use tokio::sync::Mutex as TokioMutex;

/// Per-shard partition data - mutable, single-threaded access.
#[derive(Debug)]
pub struct LocalPartition {
    pub log: SegmentedLog<MemoryMessageJournal>,
    pub offset: Arc<AtomicU64>,
    pub consumer_offsets: Arc<ConsumerOffsets>,
    pub consumer_group_offsets: Arc<ConsumerGroupOffsets>,
    pub message_deduplicator: Option<Arc<MessageDeduplicator>>,
    pub stats: Arc<PartitionStats>,
    pub created_at: IggyTimestamp,
    pub revision_id: u64,
    pub should_increment_offset: bool,
    pub write_lock: Arc<TokioMutex<()>>,
}

impl LocalPartition {
    /// Create new partition data with default log.
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
            write_lock: Arc::new(TokioMutex::new(())),
        }
    }

    /// Create partition data with existing log (e.g., loaded from disk).
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
            write_lock: Arc::new(TokioMutex::new(())),
        }
    }
}
