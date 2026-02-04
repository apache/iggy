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

use iggy_common::PollingStrategy;

/// Arguments for polling messages from a partition.
#[derive(Debug, Clone)]
pub struct PollingArgs {
    pub strategy: PollingStrategy,
    pub count: u32,
    pub auto_commit: bool,
}

impl PollingArgs {
    pub fn new(strategy: PollingStrategy, count: u32, auto_commit: bool) -> Self {
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
    /// Regular consumer with (consumer_id, partition_id)
    Consumer(usize, usize),
    /// Consumer group with (group_id, member_id)
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
    pub fn new(start_offset: u64, end_offset: u64, messages_count: u32) -> Self {
        Self {
            start_offset,
            end_offset,
            messages_count,
        }
    }

    /// Returns the number of offsets in the range.
    #[inline]
    pub fn offset_count(&self) -> u64 {
        self.end_offset - self.start_offset + 1
    }
}

/// Current offset state of a partition.
///
/// Tracks both the commit offset (visibility boundary) and write offset
/// (highest written message). These may differ when there are prepared
/// but uncommitted messages.
///
/// ```text
/// Segment: [msg0][msg1][msg2][msg3][msg4][msg5][msg6][msg7]
///                                     ▲              ▲
///                               commit_offset   write_offset
///                                   (4)             (7)
///
/// - Messages 0-4: COMMITTED (visible to consumers)
/// - Messages 5-7: PREPARED but not committed (invisible)
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionOffsets {
    /// Highest offset visible to consumers.
    ///
    /// All messages with `offset <= commit_offset` can be read via
    /// `read_committed()` or `poll_messages()`.
    pub commit_offset: u64,

    /// Highest offset written to storage.
    ///
    /// This may be greater than `commit_offset` when there are prepared
    /// but uncommitted messages (during the window between prepare and
    /// commit in VSR).
    ///
    /// Invariant: `write_offset >= commit_offset`
    pub write_offset: u64,
}

impl PartitionOffsets {
    pub fn new(commit_offset: u64, write_offset: u64) -> Self {
        debug_assert!(
            write_offset >= commit_offset,
            "write_offset ({}) must be >= commit_offset ({})",
            write_offset,
            commit_offset
        );
        Self {
            commit_offset,
            write_offset,
        }
    }

    /// Create offsets for an empty partition.
    pub fn empty() -> Self {
        Self {
            commit_offset: 0,
            write_offset: 0,
        }
    }

    /// Returns true if there are uncommitted (prepared) messages.
    pub fn has_uncommitted(&self) -> bool {
        self.write_offset > self.commit_offset
    }

    /// Returns the number of uncommitted messages.
    pub fn uncommitted_count(&self) -> u64 {
        self.write_offset - self.commit_offset
    }

    /// Returns true if commit and write offsets are equal.
    pub fn is_fully_committed(&self) -> bool {
        self.write_offset == self.commit_offset
    }
}

impl Default for PartitionOffsets {
    fn default() -> Self {
        Self::empty()
    }
}
