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

use iggy_binary_protocol::consensus::iobuf::Frozen;
use iggy_common::{IggyByteSize, PollingStrategy};
use smallvec::SmallVec;

#[derive(Debug, Clone)]
pub struct Fragment<const ALIGN: usize = 4096> {
    source: Frozen<ALIGN>,
    start: usize,
    end: usize,
}

impl<const ALIGN: usize> Fragment<ALIGN> {
    #[must_use]
    pub fn whole(source: Frozen<ALIGN>) -> Self {
        let end = source.len();
        Self {
            source,
            start: 0,
            end,
        }
    }

    #[must_use]
    /// # Panics
    ///
    /// Panics if `start > end` or if `end` is past the end of `source`.
    pub fn slice(source: Frozen<ALIGN>, start: usize, end: usize) -> Self {
        assert!(start <= end);
        assert!(end <= source.len());
        Self { source, start, end }
    }

    #[must_use]
    pub fn into_frozen(self) -> Frozen<ALIGN> {
        if self.start == 0 && self.end == self.source.len() {
            self.source
        } else {
            self.source.slice(self.start..self.end)
        }
    }
}

/// Arguments for polling messages from a partition.
#[derive(Debug, Clone)]
pub struct PollingArgs {
    pub strategy: PollingStrategy,
    pub count: u32,
    pub auto_commit: bool,
}

pub type PollFragments<const ALIGN: usize = 4096> = SmallVec<[Fragment<ALIGN>; 4]>;
pub type PollQueryResult<const ALIGN: usize = 4096> = (PollFragments<ALIGN>, Option<u64>);

impl PollingArgs {
    #[must_use]
    pub const fn new(strategy: PollingStrategy, count: u32, auto_commit: bool) -> Self {
        Self {
            strategy,
            count,
            auto_commit,
        }
    }
}

/// Result of sending messages.
#[derive(Debug)]
pub struct SendMessagesResult {
    pub messages_count: u32,
}

/// Consumer identification for offset operations.
// TODO(hubcio): unify with server's `PollingConsumer` in `streaming/polling_consumer.rs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollingConsumer {
    /// Regular consumer with (`consumer_id`, `partition_id`)
    Consumer(usize, usize),
    /// Consumer group with (`group_id`, `member_id`)
    ConsumerGroup(usize, usize),
}

/// Result of appending messages during the prepare phase.
///
/// Indicates the offset range assigned to the appended messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendResult {
    /// First offset assigned to the batch.
    pub start_offset: u64,
    /// Last offset assigned to the batch (inclusive).
    pub end_offset: u64,
    /// Number of messages in the batch.
    pub messages_count: u32,
}

impl AppendResult {
    #[must_use]
    pub const fn new(start_offset: u64, end_offset: u64, messages_count: u32) -> Self {
        Self {
            start_offset,
            end_offset,
            messages_count,
        }
    }

    /// Returns the number of offsets in the range.
    #[inline]
    #[must_use]
    pub const fn offset_count(&self) -> u64 {
        self.end_offset - self.start_offset + 1
    }
}

/// Current offset state of a partition.
///
/// Tracks both the durable offset (highest persisted message) and write offset
/// (highest assigned message offset). These may differ when there are prepared
/// messages that still only live in the in-memory journal.
///
/// ```text
/// Segment: [msg0][msg1][msg2][msg3][msg4][msg5][msg6][msg7]
///                                     ▲              ▲
///                              durable_offset   write_offset
///                                   (4)             (7)
///
/// - Messages 0-4: durably persisted
/// - Messages 5-7: prepared, but still buffered in memory
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionOffsets {
    /// Highest durably persisted offset.
    pub commit_offset: u64,

    /// Highest offset assigned to the partition.
    ///
    /// This may be greater than `commit_offset` when there are prepared
    /// messages buffered in the in-memory journal.
    ///
    /// Invariant: `write_offset >= commit_offset`
    pub write_offset: u64,
}

impl PartitionOffsets {
    #[must_use]
    pub fn new(commit_offset: u64, write_offset: u64) -> Self {
        debug_assert!(
            write_offset >= commit_offset,
            "write_offset ({write_offset}) must be >= commit_offset ({commit_offset})",
        );
        Self {
            commit_offset,
            write_offset,
        }
    }

    /// Create offsets for an empty partition.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            commit_offset: 0,
            write_offset: 0,
        }
    }

    /// Returns true if there are uncommitted (prepared) messages.
    #[must_use]
    pub const fn has_uncommitted(&self) -> bool {
        self.write_offset > self.commit_offset
    }

    /// Returns the number of uncommitted messages.
    #[must_use]
    pub const fn uncommitted_count(&self) -> u64 {
        self.write_offset - self.commit_offset
    }

    /// Returns true if commit and write offsets are equal.
    #[must_use]
    pub const fn is_fully_committed(&self) -> bool {
        self.write_offset == self.commit_offset
    }
}

impl Default for PartitionOffsets {
    fn default() -> Self {
        Self::empty()
    }
}

/// Configuration for partition operations.
///
/// Mirrors the relevant fields from the server's `PartitionConfig` and
/// `SegmentConfig` (`core/server/src/configs/system.rs`).
#[derive(Debug, Clone)]
pub struct PartitionsConfig {
    /// Flush journal to disk when it accumulates this many messages.
    pub messages_required_to_save: u32,
    /// Flush journal to disk when it accumulates this many bytes.
    pub size_of_messages_required_to_save: IggyByteSize,
    /// Whether to enforce fsync after writes.
    pub enforce_fsync: bool,
    /// Maximum size of a single segment before rotation.
    pub segment_size: IggyByteSize,
}

impl PartitionsConfig {
    #[must_use]
    pub fn get_partition_path(
        &self,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
    ) -> String {
        format!("/tmp/iggy_stub/streams/{stream_id}/topics/{topic_id}/partitions/{partition_id}",)
    }

    /// Constructs the file path for segment messages.
    ///
    /// TODO: This is a stub waiting for completion of issue to move server config
    /// to shared module. Real implementation should use:
    /// `{base_path}/{streams_path}/{stream_id}/{topics_path}/{topic_id}/{partitions_path}/{partition_id}/{start_offset:0>20}.log`
    #[must_use]
    pub fn get_messages_path(
        &self,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
        start_offset: u64,
    ) -> String {
        format!(
            "{}/{start_offset:0>20}.log",
            self.get_partition_path(stream_id, topic_id, partition_id)
        )
    }

    /// Constructs the file path for segment indexes.
    ///
    /// TODO: This is a stub waiting for completion of issue to move server config
    /// to shared module. Real implementation should use:
    /// `{base_path}/{streams_path}/{stream_id}/{topics_path}/{topic_id}/{partitions_path}/{partition_id}/{start_offset:0>20}.index`
    #[must_use]
    pub fn get_index_path(
        &self,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
        start_offset: u64,
    ) -> String {
        format!(
            "{}/{start_offset:0>20}.index",
            self.get_partition_path(stream_id, topic_id, partition_id)
        )
    }

    #[must_use]
    pub fn get_offsets_path(
        &self,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
    ) -> String {
        format!(
            "{}/offsets",
            self.get_partition_path(stream_id, topic_id, partition_id)
        )
    }

    #[must_use]
    pub fn get_consumer_offsets_path(
        &self,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
    ) -> String {
        format!(
            "{}/consumers",
            self.get_offsets_path(stream_id, topic_id, partition_id)
        )
    }

    #[must_use]
    pub fn get_consumer_group_offsets_path(
        &self,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
    ) -> String {
        format!(
            "{}/groups",
            self.get_offsets_path(stream_id, topic_id, partition_id)
        )
    }
}
