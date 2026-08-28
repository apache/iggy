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

use iggy_common::{EncryptorKind, IggyByteSize, PollingStrategy};
use server_common::iobuf::Frozen;
use smallvec::SmallVec;
use std::sync::Arc;

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

/// Ticks of a stalled repair stream tolerated before a re-request.
///
/// Partition group ticks are ~10ms, so ~1s. Repair frames are
/// fire-and-forget over a lossy bus; a session with no retry wedges
/// forever on a single dropped frame. The remaining window is re-requested
/// from the serving peer.
pub const REPAIR_RETRY_TICKS: u32 = 100;

/// One in-flight journal-repair stream for a partition group.
#[derive(Debug, Clone, Copy)]
pub struct RepairSession {
    /// Fences range replies from an earlier attempt. Repair bodies carry the
    /// stored prepare header instead, so [`Self::view`] and canonical suffix
    /// checks fence their ingest.
    pub nonce: u128,
    /// Consensus view in which this session was armed. A later view discards
    /// the session before any delayed repair body can enter its journal.
    pub view: u32,
    /// Committed frontier this repair must make locally walkable. Floor
    /// completeness and session completion are bounded here.
    pub commit_to_op: u64,
    /// Highest op requested from the peer. This may extend above
    /// [`Self::commit_to_op`] only for the canonical suffix carried by the
    /// adopted `StartView`.
    pub fetch_to_op: u64,
    /// Commit floor learned from `RangeEvicted { retained_from }`:
    /// `retained_from - 1`. `None` until (unless) the serving peer reports a
    /// truncated prefix.
    pub floor: Option<u64>,
    /// The peer serving this stream (re-request target on stall).
    pub peer: u8,
    /// Lowest `base_offset` among the repaired `SendMessages` batches:
    /// where the served window begins in offset space. Compared against the
    /// boot-recovered durable end when a commit floor arrives -- a window
    /// starting above `recovered_durable_offset + 1` means ops below the
    /// floor are neither locally durable nor repaired (state-transfer
    /// territory), and the floor must be refused.
    pub first_batch_offset: Option<u64>,
    /// Ticks since the stream last made progress; at
    /// [`REPAIR_RETRY_TICKS`] the remaining window is re-requested.
    pub idle_ticks: u32,
}

/// How a repair-window commit walk concluded, decided by
/// `IggyPartition::complete_repair`.
///
/// `#[must_use]` because `FloorRefused` is the partition plane's
/// state-transfer trigger: repair proved the gap below the floor is neither
/// locally durable nor repairable, so ignoring it wedges the replica
/// gap-stopped forever.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairConclusion {
    /// The walk fell short of `to_op`; the session stays armed and the stall
    /// retry re-requests the remains.
    InProgress,
    /// The walk reached the requested frontier; the session was dropped.
    Done,
    /// The floor's continuity check failed: ops below it are neither locally
    /// durable nor repaired. The session was dropped here -- state transfer
    /// supersedes repair -- and the caller arms the transfer.
    FloorRefused { floor: u64, to_op: u64 },
}

/// Where partition directories live on disk, mirroring the server's
/// `SystemConfig` path scheme so segment files created by the partition plane
/// land next to the ones the server bootstrap created.
#[derive(Debug, Clone)]
pub struct PartitionPathLayout {
    /// `{system.path}/{stream.path}`: the directory holding per-stream dirs.
    pub streams_root: String,
    /// Directory name of the per-topic level (`topic.path`).
    pub topics_dir: String,
    /// Directory name of the per-partition level (`partition.path`).
    pub partitions_dir: String,
}

/// Synthetic layout for tests and the simulator, where paths only key the
/// sim storage and never touch a real filesystem. The server always wires
/// the real layout from its `SystemConfig`.
impl Default for PartitionPathLayout {
    fn default() -> Self {
        Self {
            streams_root: "/tmp/iggy_stub/streams".to_string(),
            topics_dir: "topics".to_string(),
            partitions_dir: "partitions".to_string(),
        }
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
    /// Whether a disk poll verifies each batch's `batch_checksum` against the bytes
    /// it just read.
    ///
    /// Detection only: a mismatch fails the poll closed and is reported, with no
    /// attempt to repair. The alternative is serving bytes provably not the ones
    /// written, which reads to a consumer as ordinary data.
    pub validate_checksum: bool,
    /// Maximum size of a single segment before rotation.
    pub segment_size: IggyByteSize,
    /// Whether local message files reserve the configured segment size on open.
    pub preallocate_segments: bool,
    /// Server-side at-rest encryption. Applied ONCE, on the primary at
    /// ingestion, so the ciphertext replicates verbatim: every replica
    /// journals, acks, and persists identical bytes (checksums and the
    /// deterministic segment rolls both depend on that), and the poll path
    /// decrypts uniformly whether a fragment came from the resident journal
    /// or from disk.
    pub encryptor: Option<Arc<EncryptorKind>>,
    /// On-disk location scheme for partition directories.
    pub path_layout: PartitionPathLayout,
}

impl PartitionsConfig {
    #[must_use]
    pub fn get_partition_path(
        &self,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
    ) -> String {
        format!(
            "{}/{stream_id}/{}/{topic_id}/{}/{partition_id}",
            self.path_layout.streams_root,
            self.path_layout.topics_dir,
            self.path_layout.partitions_dir,
        )
    }

    /// Constructs the file path for segment messages.
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
}
