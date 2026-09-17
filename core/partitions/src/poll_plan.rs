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

//! Owned poll reads without authority to change consumer progress.
//!
//! The pump snapshots file handles and resident fragments before a read can
//! yield. Execution returns facts about that snapshot. Only the partition
//! owner validates its history and accepts progress.

use crate::iggy_index::{IGGY_INDEX_SIZE, IggyIndexCache};
use crate::iggy_index_reader::IggyIndexReader;
use crate::journal::{
    MessageLookup, push_selected_batch_fragments, select_batch_slice, unpin_sparse_source,
};
use crate::{Fragment, PollFragments, PollingConsumer};
use compio::io::AsyncReadAtExt;
use iggy_binary_protocol::batch::{BATCH_HEADER_SIZE, BatchHeader, BatchRef};
use iggy_binary_protocol::responses::messages::poll_messages::POLL_RESPONSE_HEADER_SIZE;
use iggy_binary_protocol::{WireError, batch};
use iggy_common::{ConsumerKind, IggyError};
use server_common::iobuf::{Frozen, Owned};
use server_common::poll::PollHistoryId;
use server_common::send_messages::{BatchIntegrity, COMMAND_HEADER_SIZE, frozen_batch_header};
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use tracing::{error, warn};

/// Byte cap for materializing a sealed segment's sparse index into its shared
/// read-state handle. Index density is one entry per flush, never per message:
/// at the default cadence a 1 GiB segment yields ~24 KiB of index, but
/// `messages_required_to_save = 1` flushes once per produced batch and the same
/// segment yields tens of MB (hundreds only when producers send one message per
/// batch), which a single poll must never read (or pin resident) in one go.
/// At or under the cap the whole file loads once and is
/// cached; above it every poll binary-searches the file with single-entry
/// preads instead, so resident index bytes per partition stay bounded by the
/// sealed-LRU capacity times this cap.
pub const SEALED_INDEX_RESIDENT_MAX_BYTES: u64 = 512 * 1024;

/// Where the disk tier's segment files live, resolved at plan-build time.
/// The two dir-less cases must stay distinct: only a genuinely file-less
/// partition may fall forward to the journal tier, while file-backed data
/// behind an unresolvable dir must fail closed (see [`DiskReadOutcome`]).
pub enum PartitionDirResolution {
    /// The canonical partition directory to open segment files from.
    Resolved(String),
    /// No file-backed storage exists (simulated in-memory persistence):
    /// there are no files to read and the journal tier is the only tier.
    NoFiles,
    /// File-backed storage exists but no directory was resolvable right now
    /// (a live partition mid-rotation whose sealed segments dropped their
    /// writer). Disk-resident data may be temporarily hidden, so the read
    /// fails closed instead of letting the journal-forward skip it.
    Unresolvable,
}

/// Per-sealed-segment read state, shared as a cheap `Rc` handle between the
/// owning partition and the off-borrow [`DiskReadPlan`] (a plain `Rc`, never a
/// partition reference, so the read runs off the pump). Both slots fill lazily
/// on the first sealed poll and are reused after. On retention retirement the
/// pump just drops its handle: the state frees once any in-flight poll holding
/// a clone finishes, and a cached fd meanwhile reads the unlinked inode, which
/// is consistent because retired paths are never recreated. A purge instead
/// wipes the slots in place (`SegmentedLog::invalidate_sealed_read_state`):
/// it recreates the same paths, so a clone surviving in a suspended walk must
/// re-open by path and observe the fresh files rather than serve purged data.
/// The active segment uses the [`Self::fd`] slot only: it grows under the
/// reader, so a size-derived memo on it would go stale, and its slot sits
/// outside the sealed LRU (`SegmentedLog::reset_read_state` drops it wherever
/// a segment changes sealed-ness).
#[derive(Debug, Default)]
pub struct SealedSegmentReadState {
    /// Read-only descriptor; compio `File` clones share the kernel fd, so a hit
    /// avoids the per-poll `openat` (an `io_uring` op prone to io-wq punts) and
    /// preserves kernel readahead. `None` until the first poll opens it.
    pub(crate) fd: RefCell<Option<compio::fs::File>>,
    /// Sparse offset/timestamp index reloaded from the `.index` file the
    /// segment dropped at rotation, so a poll resolves the start byte in
    /// O(log n) instead of scanning the whole segment from byte 0 (the stall).
    /// `None` until the first sealed poll loads it.
    pub(crate) index: RefCell<Option<IggyIndexCache>>,
    /// Whether the owning partition's sealed LRU currently tracks this handle.
    /// Gates the fd store-back in `resolve_segment_file` for a SEALED segment
    /// (the active segment's slot is outside the LRU, so it always fills): a
    /// walk crosses every
    /// sealed segment from the poll's start onward, but only the start segment
    /// is LRU-touched, so an untracked fill would retain a descriptor the
    /// `SEALED_READ_STATE_CAP` budget never counts. Set on touch, cleared on
    /// evict; plain `Cell`, all access is same-thread (`Rc` handle).
    pub(crate) tracked: Cell<bool>,
    /// One-slot memo of the last file-backed offset resolution, so a
    /// sequentially advancing consumer re-polling the same segment resolves
    /// inside `[offset, valid_until)` with zero index-file reads. Only the
    /// too-large-to-materialize index path consults it (a resident
    /// [`Self::index`] already resolves in memory). Sealed segments are
    /// immutable, so the memo cannot go stale; the one exception is a purge
    /// recreating the same paths, which wipes this slot with the others.
    /// Timestamp polls bypass it.
    pub(crate) offset_cursor: Cell<Option<SealedOffsetCursor>>,
}

/// See [`SealedSegmentReadState::offset_cursor`].
#[derive(Debug, Clone, Copy)]
pub struct SealedOffsetCursor {
    /// Offset of the resolved index entry (interval floor, inclusive).
    pub(crate) offset: u64,
    /// The successor index entry's offset (interval ceiling, exclusive);
    /// `u64::MAX` when the resolved entry is the segment's last.
    pub(crate) valid_until: u64,
    /// Start byte the whole interval resolves to.
    pub(crate) position: u64,
}

pub type SealedSegmentHandle = Rc<SealedSegmentReadState>;

/// Owned, borrow-free inputs for the disk tier of a poll (see module docs). A
/// sealed segment reuses its cached [`SealedSegmentReadState`] (read fd + sparse
/// index); the active segment reuses the cached fd but resolves from its
/// resident index, because sealed segments drop that index at rotation.
pub struct DiskReadPlan {
    pub(crate) partition_dir: PartitionDirResolution,
    /// Segments to walk, snapshotted from the poll's starting segment onward
    /// (see `build_poll_plan`); `start_position` is the byte offset into the
    /// first one.
    pub(crate) segments: Vec<DiskSegment>,
    pub(crate) start_position: u64,
    pub(crate) start_index_offset: Option<u64>,
    pub(crate) namespace_raw: u64,
    /// Whether to verify each batch's `batch_checksum` against the bytes read.
    /// Detection only; a mismatch fails the poll closed and repairs nothing.
    pub(crate) validate_checksum: bool,
    /// Mean encoded bytes per message on this partition, or `None` before it
    /// has committed anything. Sizes the disk walk's reads, and is never a
    /// bound on what a read may return, since messages vary in size within a
    /// partition.
    pub(crate) bytes_per_message: Option<u32>,
    /// Widest batch this partition has committed, which is the smallest read
    /// that is guaranteed to contain a whole one. The walk cannot advance on a
    /// chunk holding no complete batch, so a count-derived estimate below this
    /// buys nothing and costs the re-read it triggers.
    pub(crate) widest_batch_bytes: u64,
}

pub struct DiskSegment {
    pub(crate) start_offset: u64,
    pub(crate) persisted: u64,
    /// Shared read state, cloned from the owning partition at plan time. See
    /// [`SealedSegmentReadState`].
    pub(crate) read_state: SealedSegmentHandle,
    /// Whether the segment was sealed when the plan was built. Only a sealed
    /// segment resolves its start byte from the shared sparse index; the active
    /// one grows under the reader and uses its resident index instead.
    pub(crate) sealed: bool,
}

/// Admission inputs carried by a read without access to consumer progress maps.
#[derive(Debug)]
pub struct PollContext {
    /// History captured at planning, which must still match at completion.
    pub(crate) history: PollHistoryId,
    /// Consumer or group whose progress the owner may update after admission.
    pub(crate) consumer: PollingConsumer,
    /// For nonempty results, whether to advance the stored offset locally.
    /// Group `last_polled` progress also advances when this is false.
    pub(crate) auto_commit: bool,
}

/// An owned read result awaiting validation by the partition owner.
/// Finishing I/O does not authorize a successful reply or a progress update.
#[derive(Debug)]
pub struct PollReadResult {
    pub(crate) context: PollContext,
    /// Selected message bytes, which remain unaccepted until owner validation.
    pub(crate) fragments: PollFragments,
    /// Partition message frontier captured at planning, not consumer progress.
    pub(crate) commit_offset: u64,
    /// Inclusive offset of the last selected message, or `None` for no match.
    pub(crate) last_matching_offset: Option<u64>,
    pub(crate) message_count: u32,
    pub(crate) read_error: Option<IggyError>,
}

impl PollReadResult {
    /// Cap the encoded selection before the owner accepts its final offset.
    /// A sliced batch retains its original offset base and receives a new checksum.
    ///
    /// # Errors
    /// Returns `InvalidSizeBytes` when the first message cannot fit, or
    /// `CannotReadMessage` if the selected batch framing is invalid.
    pub fn limit_bytes(mut self, max_bytes: usize) -> Result<(Self, bool), IggyError> {
        let mut remaining = max_bytes
            .checked_sub(POLL_RESPONSE_HEADER_SIZE)
            .ok_or(IggyError::InvalidSizeBytes)?;
        let mut fragments = std::mem::take(&mut self.fragments).into_iter();
        self.message_count = 0;
        self.last_matching_offset = None;
        let mut limited = false;
        while let Some(fragment) = fragments.next() {
            let source = fragment.into_frozen();
            let mut header =
                BatchHeader::decode(&source).map_err(|_| IggyError::CannotReadMessage)?;
            let body = if source.len() == BATCH_HEADER_SIZE {
                fragments
                    .next()
                    .ok_or(IggyError::CannotReadMessage)?
                    .into_frozen()
            } else {
                source.slice(BATCH_HEADER_SIZE..source.len())
            };
            if body.len()
                != header
                    .blob_len()
                    .map_err(|_| IggyError::CannotReadMessage)?
            {
                return Err(IggyError::CannotReadMessage);
            }
            let batch = BatchRef::new(header, &body);
            let mut selected = 0;
            let mut end = 0;
            for record in batch.iter_with_offsets() {
                if BATCH_HEADER_SIZE.saturating_add(record.end) > remaining {
                    limited = true;
                    break;
                }
                selected += 1;
                end = record.end;
                self.last_matching_offset = Some(
                    header
                        .base_offset
                        .checked_add(u64::from(record.message.header.offset_delta))
                        .ok_or(IggyError::CannotReadMessage)?,
                );
            }
            if selected == 0 {
                if self.message_count == 0 {
                    return Err(IggyError::InvalidSizeBytes);
                }
                break;
            }
            self.message_count += selected;
            remaining -= BATCH_HEADER_SIZE + end;
            limited |= remaining == 0;
            if selected == header.message_count && source.len() != BATCH_HEADER_SIZE {
                self.fragments.push(Fragment::whole(source));
            } else {
                header.message_count = selected;
                header.batch_length = (BATCH_HEADER_SIZE + end) as u64;
                header.batch_checksum = header.checksum_for_blob(&body[..end]);
                self.fragments
                    .push(Fragment::whole(frozen_batch_header(&header)));
                self.fragments.push(Fragment::whole(body.slice(..end)));
            }
            if limited {
                break;
            }
        }
        Ok((self, limited))
    }

    /// Memory this result keeps alive, counting every backing allocation once
    /// however many fragments slice it.
    #[must_use]
    pub fn retained_bytes(&self) -> usize {
        let mut bytes = self
            .fragments
            .capacity()
            .saturating_mul(size_of::<crate::Fragment>());
        for (index, fragment) in self.fragments.iter().enumerate() {
            if !self.fragments[..index]
                .iter()
                .any(|earlier| earlier.shares_allocation_with(fragment))
            {
                bytes = bytes.saturating_add(fragment.allocation_bytes());
            }
        }
        bytes
    }

    /// Copy the selection into one exact-size allocation and drop the borrowed
    /// buffers. A reply that slices a few bytes out of segment-sized journal
    /// buffers pins all of them, so the copy trades one pass over the reply for
    /// a charge that tracks the reply itself. Fragment boundaries survive, which
    /// keeps batch framing intact.
    #[must_use]
    pub fn compacted(mut self) -> Self {
        let total: usize = self.fragments.iter().map(Fragment::len).sum();
        let mut buffer = Owned::with_capacity(total);
        for fragment in &self.fragments {
            buffer.extend_from_slice(fragment.as_slice());
        }
        let compacted = Frozen::from(buffer);
        let mut start = 0;
        self.fragments = self
            .fragments
            .iter()
            .map(|fragment| {
                let end = start + fragment.len();
                let piece = Fragment::slice(compacted.clone(), start, end);
                start = end;
                piece
            })
            .collect();
        self
    }

    #[must_use]
    pub const fn message_count(&self) -> u32 {
        self.message_count
    }

    /// Reject incomplete reads before a deferred poll can cache or accept them.
    /// Reject a read whose valid prefix was followed by an error.
    ///
    /// # Errors
    /// Returns the underlying read or allocation-limit failure.
    pub fn checked(mut self) -> Result<Self, IggyError> {
        self.read_error.take().map_or_else(|| Ok(self), Err)
    }

    #[must_use]
    pub const fn consumer_kind(&self) -> ConsumerKind {
        match self.context.consumer {
            PollingConsumer::Consumer(..) => ConsumerKind::Consumer,
            PollingConsumer::ConsumerGroup(..) => ConsumerKind::ConsumerGroup,
        }
    }
}

/// Owned, point-in-time snapshot of the resident journal tail for the disk-tier
/// straddle. `entries` are op-ascending `Frozen` clones (refcount bumps).
pub struct ResidentTailSnapshot {
    pub(crate) oldest_resident: Option<u64>,
    pub(crate) entries: Vec<Frozen<4096>>,
}

impl ResidentTailSnapshot {
    /// Offset query to continue a disk match into the resident tail, or `None`
    /// when the tail cannot contiguously extend it. The snapshot is
    /// point-in-time, so the gate (`oldest_resident <= last + 1`) is race-free:
    /// a commit after the snapshot cannot have evicted the run. Without it,
    /// splicing the next resident op over an evicted run silently skips offsets.
    fn straddle_continuation(
        &self,
        last_offset: u64,
        remaining: u32,
        ceiling: u64,
    ) -> Option<MessageLookup> {
        (remaining > 0
            && self
                .oldest_resident
                .is_some_and(|oldest| oldest <= last_offset + 1))
        .then_some(MessageLookup::Offset {
            offset: last_offset + 1,
            count: remaining,
            ceiling,
        })
    }
}

/// Owned read snapshot that may outlive the partition history it captured.
///
/// Execution yields a [`PollReadResult`] that the owner must accept through
/// [`crate::IggyPartitions::complete_poll`] before replying or updating progress.
pub struct PollPlan {
    /// Monotone high-water snapshot taken before the disk read, so it may lag a
    /// concurrent producer by the poll duration and self-corrects next poll.
    pub(crate) commit_offset: u64,
    pub(crate) context: PollContext,
    pub(crate) tier: PollTier,
}

impl PollPlan {
    /// Whether this snapshot needs disk I/O on a detached read task.
    /// Resident reads execute and complete synchronously on the owner.
    #[must_use]
    pub const fn needs_off_pump_io(&self) -> bool {
        matches!(self.tier, PollTier::Disk { .. })
    }

    /// Read the captured snapshot without changing consumer progress.
    /// The owner decides whether legacy prefix results or strict errors apply.
    pub async fn execute(self) -> PollReadResult {
        self.execute_with_limit(usize::MAX).await
    }

    pub async fn execute_with_limit(self, max_bytes: usize) -> PollReadResult {
        let mut read_error = None;
        let (fragments, last_matching_offset, message_count) = match self.tier {
            PollTier::Empty => (PollFragments::new(), None, 0),
            PollTier::Resident {
                fragments,
                last_matching_offset,
                message_count,
            } => (fragments, last_matching_offset, message_count),
            PollTier::Disk {
                disk,
                query,
                resident_tail,
            } => match disk.read_disk_with_limit(query, max_bytes).await {
                DiskReadOutcome::Empty => {
                    crate::journal::select_resident(&resident_tail.entries, query)
                        .unwrap_or_else(|| (PollFragments::new(), None, 0))
                }
                DiskReadOutcome::Limited => {
                    read_error = Some(IggyError::InvalidSizeBytes);
                    (PollFragments::new(), None, 0)
                }
                DiskReadOutcome::Faulted => {
                    read_error = Some(IggyError::CannotReadMessage);
                    (PollFragments::new(), None, 0)
                }
                DiskReadOutcome::Matched {
                    mut fragments,
                    last_matching_offset,
                    matched,
                    faulted,
                } => {
                    read_error = faulted.then_some(IggyError::CannotReadMessage);
                    let remaining = query.count().saturating_sub(matched);
                    let continuation = last_matching_offset
                        .and_then(|last_offset| {
                            resident_tail.straddle_continuation(
                                last_offset,
                                remaining,
                                query.ceiling(),
                            )
                        })
                        .and_then(|query| {
                            crate::journal::select_resident(&resident_tail.entries, query)
                        });
                    match continuation {
                        Some((journal_fragments, journal_last, journal_count)) => {
                            fragments.extend(journal_fragments);
                            (
                                fragments,
                                journal_last.or(last_matching_offset),
                                matched + journal_count,
                            )
                        }
                        None => (fragments, last_matching_offset, matched),
                    }
                }
            },
        };
        PollReadResult {
            context: self.context,
            commit_offset: self.commit_offset,
            fragments,
            last_matching_offset,
            message_count,
            read_error,
        }
    }

    /// Read a resident snapshot synchronously on the pump.
    /// The returned result requires the same owner validation as a disk read.
    ///
    /// # Panics
    /// Panics if [`Self::needs_off_pump_io`] is true.
    #[must_use]
    pub fn execute_resident(self) -> PollReadResult {
        let (fragments, last_matching_offset, message_count) = match self.tier {
            PollTier::Empty => (PollFragments::new(), None, 0),
            PollTier::Resident {
                fragments,
                last_matching_offset,
                message_count,
            } => (fragments, last_matching_offset, message_count),
            PollTier::Disk { .. } => {
                unreachable!("execute_resident on Disk tier; needs_off_pump_io guards this")
            }
        };
        PollReadResult {
            context: self.context,
            commit_offset: self.commit_offset,
            fragments,
            last_matching_offset,
            message_count,
            read_error: None,
        }
    }
}

pub enum PollTier {
    Empty,
    Resident {
        fragments: PollFragments<4096>,
        last_matching_offset: Option<u64>,
        message_count: u32,
    },
    Disk {
        disk: DiskReadPlan,
        query: MessageLookup,
        /// Resident journal tail snapshot for the straddle continuation,
        /// captured at plan time so the splice runs off the partition borrow.
        resident_tail: ResidentTailSnapshot,
    },
}

/// Outcome of [`DiskReadPlan::read_disk`], distinguishing a benign empty walk
/// from an IO fault so the caller can fail-closed.
///
/// A faulted segment may hold data that is present-but-unreadable right now;
/// the disk walk stops at the fault (never advancing to later segments) so a
/// poll cannot return a gap. The caller must NOT fall the journal forward over
/// a `Faulted` result, or it would splice the next resident op over the
/// unreadable run and silently skip live messages.
pub enum DiskReadOutcome {
    /// Walk produced matches (possibly a partial prefix if a fault stopped it).
    Matched {
        fragments: PollFragments<4096>,
        last_matching_offset: Option<u64>,
        matched: u32,
        faulted: bool,
    },
    /// Walk completed with no fault and matched nothing. The query offset is
    /// below disk retention too, so the caller may serve the journal forward
    /// (retention-recovery) without skipping anything.
    Empty,
    /// Walk stopped on an IO fault before matching anything. Fail-closed: the
    /// caller returns an empty poll so the consumer cursor does not advance
    /// past data that may still be present-but-unreadable.
    Faulted,
    Limited,
}

/// Ceiling for ordinary disk reads. An incomplete batch may require one
/// larger re-read, without widening subsequent chunks or segments.
const READ_ALLOCATION_FACTOR: usize = 4;

const DISK_POLL_CHUNK_MAX: u64 = 1 << 20;

/// Smallest first read of a disk poll. Below this the syscall and the segment
/// walk cost more than the bytes the smaller read saves, and a poll for a
/// handful of messages would issue a read per batch.
const DISK_POLL_CHUNK_MIN: u64 = 64 << 10;

/// How the chunk loop over one segment ended.
enum SegmentWalk {
    /// The segment is exhausted or the requested count is filled. The walk
    /// may continue into the next segment.
    Done,
    /// Fail-closed: the segment may hold present-but-unreadable or corrupt
    /// data, so no later segment may be served over it.
    Faulted,
}

/// The state one disk walk carries across its segments.
struct DiskWalk {
    max_bytes: usize,
    retained_bytes: usize,
    limited: bool,
    /// Byte offset into the segment being walked; reset at each boundary.
    position: u64,
    /// Messages between the resolved index entry and the requested offset,
    /// which the first read has to cover on top of what the poll asked for.
    /// Cleared once anything matches, since the walk is then at the target.
    skipped: u32,
    matched: u32,
    fragments: PollFragments<4096>,
    last_matching_offset: Option<u64>,
    /// Batch width learned from an incomplete read, capped at the chunk ceiling.
    batch_read_floor: u64,
    #[cfg(feature = "poll-diagnostics")]
    requested_bytes: u64,
    #[cfg(feature = "poll-diagnostics")]
    chunk_reads: u32,
}

impl DiskWalk {
    /// Messages the next read has to cover: what the poll still wants, plus
    /// the run the sparse index left in front of the first match.
    const fn remaining_to_read(&self, count: u32) -> u32 {
        (count - self.matched).saturating_add(self.skipped)
    }

    fn starting_at(position: u64, skipped: u32) -> Self {
        Self {
            position,
            skipped,
            matched: 0,
            fragments: PollFragments::new(),
            last_matching_offset: None,
            batch_read_floor: 0,
            max_bytes: usize::MAX,
            retained_bytes: 0,
            limited: false,
            #[cfg(feature = "poll-diagnostics")]
            requested_bytes: 0,
            #[cfg(feature = "poll-diagnostics")]
            chunk_reads: 0,
        }
    }
}

impl DiskReadPlan {
    /// Bytes to read for the next `remaining` messages.
    ///
    /// A poll asks for a message count and the walk reads bytes, so the two are
    /// bridged by the partition's own mean encoded size. Reading a fixed
    /// megabyte instead costs a poll for a thousand hundred-byte messages ten
    /// times the bytes it returns, and the sparse-selection copy that follows
    /// scales with the chunk rather than with the selection.
    ///
    /// The count-derived estimate alone is not enough, because a batch is the
    /// unit the walk can consume. A poll for fewer messages than a producer put
    /// in one batch estimates below that batch, decodes nothing, and pays the
    /// quadrupling re-read below: at one message short of a full batch that is
    /// five times the bytes a flat megabyte read would have taken. So the
    /// estimate is floored at the widest batch this partition has committed,
    /// which is the smallest read guaranteed to hold a whole one.
    ///
    /// The result is still not a bound. Messages vary in size, the starting
    /// offset can sit inside a batch the index resolved before it, and a batch
    /// wider than the ceiling still grows through the re-read path. The ceiling
    /// is what every poll read before it was sized at all, so no poll reads
    /// more than it used to.
    fn chunk_len(&self, remaining: u32) -> u64 {
        let Some(bytes_per_message) = self.bytes_per_message else {
            return DISK_POLL_CHUNK_MAX;
        };
        u64::from(bytes_per_message)
            .saturating_mul(u64::from(remaining))
            .saturating_add(COMMAND_HEADER_SIZE as u64)
            .max(self.widest_batch_bytes)
            .clamp(DISK_POLL_CHUNK_MIN, DISK_POLL_CHUNK_MAX)
    }

    /// Serve a poll from the on-disk segment files, off the partition borrow.
    /// Reads from owned descriptors so no partition reference is held across
    /// the file IO. Walks stamped `[256B BatchHeader][blob]` batches in
    /// chunked reads, re-reading a batch split across a chunk boundary in the
    /// next chunk.
    #[cfg(test)]
    pub(crate) async fn read_disk(self, query: MessageLookup) -> DiskReadOutcome {
        self.read_disk_with_limit(query, usize::MAX).await
    }

    async fn read_disk_with_limit(self, query: MessageLookup, max_bytes: usize) -> DiskReadOutcome {
        let count = query.count();
        if count == 0 || self.segments.is_empty() {
            return DiskReadOutcome::Empty;
        }
        let partition_dir = match &self.partition_dir {
            PartitionDirResolution::Resolved(dir) => dir.as_str(),
            // Simulated in-memory persistence: no files exist, so this is not
            // an IO fault on present data. `Empty` lets the caller serve the
            // resident journal tier (the sim's only tier) without skipping
            // anything.
            PartitionDirResolution::NoFiles => return DiskReadOutcome::Empty,
            // File-backed data exists but the dir was unresolvable at plan
            // time (mid-rotation). Fail-closed like an IO fault: the
            // journal-forward would splice resident ops over the hidden
            // disk-resident offsets. A later poll resolves the dir again.
            PartitionDirResolution::Unresolvable => {
                warn!(
                    target: "iggy.partitions.diag",
                    plane = "partitions",
                    namespace_raw = self.namespace_raw,
                    segment_count = self.segments.len(),
                    "disk poll: file-backed partition has no resolvable dir; failing closed"
                );
                return DiskReadOutcome::Faulted;
            }
        };

        // `start_position` applies to the first snapshotted segment; each later
        // segment is walked from byte 0 (reset at the end of every iteration).
        //
        // A sealed first segment dropped its resident index at rotation, so
        // `disk_poll_start` fell back to byte 0. Reload the sparse index (once,
        // then cached) and resolve the start byte so the walk skips straight to
        // the target instead of scanning the whole segment - the poll stall. A
        // miss or load failure keeps `start_position` (the pre-existing
        // full-scan fallback). An active first segment keeps its
        // resident-index-resolved `start_position` untouched.
        let resolved = match self.segments.first() {
            Some(first) => {
                self.resolve_sealed_start(first, query, partition_dir, max_bytes)
                    .await
            }
            None => None,
        };
        let position = resolved.map_or(self.start_position, |(position, _)| position);
        // The index is sparse, so the entry it resolved can sit a whole flush
        // group before the requested offset. The walk has to read that run to
        // reach the first match, and sizing the read from the requested count
        // alone would cross it in floor-sized reads.
        let entry_offset = resolved
            .map(|(_, offset)| offset)
            .or(self.start_index_offset);
        let skipped = match (entry_offset, query) {
            (Some(entry_offset), MessageLookup::Offset { offset, .. }) => {
                u32::try_from(offset.saturating_sub(entry_offset)).unwrap_or(u32::MAX)
            }
            _ => 0,
        };
        let mut walk = DiskWalk::starting_at(position, skipped);
        walk.max_bytes = max_bytes;
        // Set when an open/read retry exhausts. The walk breaks immediately so
        // later segments are never read into the result (which would leave a
        // gap at the faulted segment). Pre-fault matches are still served.
        let mut faulted = false;

        for segment in &self.segments {
            if walk.matched >= count {
                break;
            }
            let persisted = segment.persisted;
            if persisted == 0 || walk.position >= persisted {
                // Benign skip: nothing persisted for this segment yet, or the
                // start position is already past it. Not a fault.
                walk.position = 0;
                continue;
            }
            let path = format!("{partition_dir}/{:0>20}.log", segment.start_offset);
            let Some(file) = self.resolve_segment_file(segment, &path).await else {
                // Open exhausted retries: the segment may hold present-but-
                // unreadable data. Stop here rather than walking past it.
                faulted = true;
                break;
            };

            if matches!(
                self.walk_segment(&file, query, count, persisted, &mut walk)
                    .await,
                SegmentWalk::Faulted
            ) {
                faulted = true;
                break;
            }
            walk.position = 0;
        }

        // The three ratios a read-sizing change is judged on: bytes asked of
        // the file API, bytes actually served, and the reads it took to get
        // them. Per poll, so a short run answers whether a sized first read
        // pays for itself before anything becomes a permanent counter.
        #[cfg(feature = "poll-diagnostics")]
        tracing::debug!(
            target: "iggy.partitions.poll_diagnostics",
            namespace_raw = self.namespace_raw,
            requested_bytes = walk.requested_bytes,
            served_bytes = walk.fragments.iter().map(|fragment| fragment.len() as u64).sum::<u64>(),
            chunk_reads = walk.chunk_reads,
            requested_count = count,
            matched = walk.matched,
            "disk poll read accounting"
        );

        if walk.limited {
            return DiskReadOutcome::Limited;
        }
        if walk.matched > 0 {
            // Pre-fault matches are always a contiguous prefix (the walk stops
            // at the first fault), so a partial result carries no gap.
            DiskReadOutcome::Matched {
                fragments: walk.fragments,
                last_matching_offset: walk.last_matching_offset,
                matched: walk.matched,
                faulted,
            }
        } else if faulted {
            DiskReadOutcome::Faulted
        } else {
            DiskReadOutcome::Empty
        }
    }

    /// Read one segment from `walk.position` until the count is filled, the
    /// segment is exhausted, or the walk must fail closed.
    ///
    /// The read length is the chunk clipped to the segment's persisted bytes,
    /// so narrowing it to a `usize` cannot truncate. The chunk itself is not
    /// bounded by `DISK_POLL_CHUNK_MAX`: a batch wider than the ceiling grows
    /// past it below.
    #[allow(clippy::cast_possible_truncation)]
    async fn walk_segment(
        &self,
        file: &compio::fs::File,
        query: MessageLookup,
        count: u32,
        persisted: u64,
        walk: &mut DiskWalk,
    ) -> SegmentWalk {
        let mut chunk_len = self
            .chunk_len(walk.remaining_to_read(count))
            .max(walk.batch_read_floor);
        while walk.matched < count && walk.position < persisted {
            let len = (persisted - walk.position).min(chunk_len) as usize;
            // One source buffer plus sparse copies, fragment headers and descriptors.
            if len
                .saturating_mul(READ_ALLOCATION_FACTOR)
                .saturating_add(walk.retained_bytes)
                > walk.max_bytes
            {
                walk.limited = true;
                return SegmentWalk::Faulted;
            }
            let Some(chunk) = self.read_chunk_with_retry(file, len, walk).await else {
                // Chunk read exhausted retries: same fail-closed reason as
                // a failed open.
                return SegmentWalk::Faulted;
            };
            let fragments_before_chunk = walk.fragments.len();
            let ChunkWalk {
                consumed,
                needed,
                corrupt,
            } = walk_disk_chunk(
                &chunk,
                query,
                count,
                &mut walk.matched,
                &mut walk.fragments,
                &mut walk.last_matching_offset,
                if self.validate_checksum {
                    BatchIntegrity::Verify
                } else {
                    BatchIntegrity::LayoutOnly
                },
                self.namespace_raw,
            );
            // Detached from the pump, so the ratio alone bounds the copy.
            unpin_sparse_source(
                &mut walk.fragments,
                fragments_before_chunk,
                &chunk,
                usize::MAX,
            );
            for fragment in &walk.fragments[fragments_before_chunk..] {
                walk.retained_bytes = walk
                    .retained_bytes
                    .saturating_add(fragment.allocation_bytes())
                    .saturating_add(size_of::<crate::Fragment>() * 2);
            }
            if walk.retained_bytes > walk.max_bytes {
                walk.limited = true;
                return SegmentWalk::Faulted;
            }
            if corrupt {
                // A batch that does not match its own checksum. Fail closed like
                // an IO fault: serving it hands a consumer data provably not what
                // was written, and skipping ahead punches a silent gap.
                return SegmentWalk::Faulted;
            }
            if consumed == 0 {
                if (len as u64) >= persisted - walk.position {
                    // The whole remainder fit yet no complete batch decoded: a
                    // corrupt batch in this segment. Fail-closed like an IO
                    // fault so a later segment is never served over the corrupt
                    // run, which would punch a silent gap into the poll.
                    return SegmentWalk::Faulted;
                }
                // A single batch larger than the chunk. Its own header says
                // how wide it is, so re-read exactly that; only a header this
                // read could not reach leaves the old quadrupling.
                if needed as u64 > persisted - walk.position {
                    return SegmentWalk::Faulted;
                }
                chunk_len = if needed > len {
                    walk.batch_read_floor = walk
                        .batch_read_floor
                        .max((needed as u64).min(DISK_POLL_CHUNK_MAX));
                    needed as u64
                } else {
                    chunk_len.saturating_mul(4)
                };
                continue;
            }
            if walk.matched > 0 {
                walk.skipped = 0;
            }
            chunk_len = self
                .chunk_len(walk.remaining_to_read(count))
                .max(walk.batch_read_floor);
            walk.position += consumed as u64;
        }
        SegmentWalk::Done
    }

    /// Resolve the read-only descriptor for `segment`'s file. A hit clones the
    /// cached fd (sharing the kernel fd, no syscall); a miss opens by path and
    /// stores the fd back so later polls skip the `openat`. Returns `None` only
    /// when the open exhausts its retries (the caller fails closed).
    async fn resolve_segment_file(
        &self,
        segment: &DiskSegment,
        path: &str,
    ) -> Option<compio::fs::File> {
        let handle = &segment.read_state;
        // Borrow only to clone the `Option<File>` out, never across the await.
        if let Some(cached) = handle.fd.borrow().clone() {
            return Some(cached);
        }
        let file = self.open_segment_with_retry(path).await?;
        // A sealed segment stores back only while the pump tracks its handle;
        // an untracked fill (walk-through segment, or a slot evicted mid-poll)
        // would pin an fd outside the LRU budget, so it opens transiently
        // instead. The active segment's slot is not LRU-budgeted (one per
        // partition, dropped when it seals), so it always fills. Benign race: a
        // concurrent poll of the same segment may have filled the slot while
        // this open was in flight; overwriting with an equivalent fd (same
        // inode) is harmless, as is filling a slot the pump orphaned mid-poll.
        if !segment.sealed || handle.tracked.get() {
            *handle.fd.borrow_mut() = Some(file.clone());
        }
        Some(file)
    }

    /// Resolve the start byte for the poll's target segment from its sparse
    /// index. An index at or under [`SEALED_INDEX_RESIDENT_MAX_BYTES`] loads
    /// whole on the first sealed poll and is cached on the shared handle; a
    /// larger one is binary-searched on file every poll and never materialized
    /// (see the constant). Returns `None` (keep the byte-0 fallback) for the
    /// active segment, a below-range query, or an IO failure.
    async fn resolve_sealed_start(
        &self,
        segment: &DiskSegment,
        query: MessageLookup,
        partition_dir: &str,
        max_bytes: usize,
    ) -> Option<(u64, u64)> {
        // The active segment grows under the reader, so neither the shared
        // sparse index nor the offset memo can describe it; its own resident
        // index already resolved `start_position`.
        if !segment.sealed {
            return None;
        }
        let handle = &segment.read_state;
        // Cache hit: resolve under a short borrow, never across the await.
        let cached = handle
            .index
            .borrow()
            .as_ref()
            .map(|index| resolve_index_position(index, query));
        if let Some(resolved) = cached {
            return resolved;
        }
        // No resident index, so this is the file-backed path a sequential
        // consumer would otherwise binary-search on disk every poll: answer
        // from the memoized interval when the query lands inside it.
        if let (MessageLookup::Offset { offset, .. }, Some(cursor)) =
            (query, handle.offset_cursor.get())
            && offset >= cursor.offset
            && offset < cursor.valid_until
        {
            return Some((cursor.position, cursor.offset));
        }
        let path = format!("{partition_dir}/{:0>20}.index", segment.start_offset);
        let reader = match IggyIndexReader::new(&path).await {
            Ok(reader) => reader,
            Err(error) => {
                self.warn_sparse_index_fallback(&path, "open", &error);
                return None;
            }
        };
        let entry_count = match reader.entry_count().await {
            Ok(entry_count) => entry_count,
            Err(error) => {
                self.warn_sparse_index_fallback(&path, "entry_count", &error);
                return None;
            }
        };
        if entry_count.saturating_mul(IGGY_INDEX_SIZE as u64) <= SEALED_INDEX_RESIDENT_MAX_BYTES
            && entry_count
                .saturating_mul(IGGY_INDEX_SIZE as u64)
                .saturating_mul(2)
                <= max_bytes as u64
        {
            let index = match reader.load_all().await {
                Ok(index) => index,
                Err(error) => {
                    self.warn_sparse_index_fallback(&path, "load", &error);
                    return None;
                }
            };
            let resolved = resolve_index_position(&index, query);
            *handle.index.borrow_mut() = Some(index);
            return resolved;
        }
        let looked_up = match query {
            MessageLookup::Offset { offset, .. } => {
                match reader
                    .offset_lower_bound_with_successor(entry_count, offset)
                    .await
                {
                    Ok(resolved) => Ok(resolved.map(|(entry, successor_offset)| {
                        handle.offset_cursor.set(Some(SealedOffsetCursor {
                            offset: entry.offset,
                            valid_until: successor_offset.unwrap_or(u64::MAX),
                            position: entry.position,
                        }));
                        entry
                    })),
                    Err(error) => Err(error),
                }
            }
            MessageLookup::Timestamp { timestamp, .. } => {
                reader.timestamp_lower_bound(entry_count, timestamp).await
            }
        };
        match looked_up {
            Ok(entry) => entry.map(|entry| (entry.position, entry.offset)),
            Err(error) => {
                self.warn_sparse_index_fallback(&path, "lower_bound", &error);
                None
            }
        }
    }

    /// The sparse index is unavailable or unreadable; the caller falls back to
    /// a byte-0 scan (the pre-existing behavior) and retries on the next poll.
    fn warn_sparse_index_fallback(&self, path: &str, stage: &str, error: &IggyError) {
        warn!(
            target: "iggy.partitions.diag",
            plane = "partitions",
            namespace_raw = self.namespace_raw,
            path,
            stage,
            %error,
            "disk poll: sparse index unavailable; scanning from segment start"
        );
    }

    /// Open a segment file for a disk poll, retrying transient IO failures (fd
    /// pressure under heavy parallel load) so one failed syscall does not
    /// silently collapse the poll into an empty result.
    async fn open_segment_with_retry(&self, path: &str) -> Option<compio::fs::File> {
        for attempt in 0..3u8 {
            match compio::fs::File::open(path).await {
                Ok(file) => return Some(file),
                Err(error) => {
                    warn!(
                        target: "iggy.partitions.diag",
                        plane = "partitions",
                        namespace_raw = self.namespace_raw,
                        path,
                        attempt,
                        %error,
                        "disk poll: failed to open segment file"
                    );
                    compio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            }
        }
        None
    }

    /// Read one chunk for a disk poll, retrying transient IO failures.
    #[cfg_attr(
        not(feature = "poll-diagnostics"),
        allow(
            clippy::needless_pass_by_ref_mut,
            reason = "The mutable reference is needed to update counters when poll diagnostics are enabled"
        )
    )]
    async fn read_chunk_with_retry(
        &self,
        file: &compio::fs::File,
        len: usize,
        walk: &mut DiskWalk,
    ) -> Option<Frozen<4096>> {
        let position = walk.position;
        for attempt in 0..3u8 {
            #[cfg(feature = "poll-diagnostics")]
            {
                walk.requested_bytes += len as u64;
                walk.chunk_reads += 1;
            }
            // `with_capacity` (len == 0, capacity == len) instead of `zeroed`:
            // `read_exact_at` fills the whole capacity in place and advances the
            // length via `SetLen`, so the `zeroed` memset of up to 1MiB per
            // chunk was pure waste - every byte is overwritten by the read.
            let buffer = Owned::<4096>::with_capacity(len);
            let compio::BufResult(read, buffer) = file.read_exact_at(buffer, position).await;
            match read {
                Ok(()) => return Some(Frozen::from(buffer)),
                Err(error) => {
                    warn!(
                        target: "iggy.partitions.diag",
                        plane = "partitions",
                        namespace_raw = self.namespace_raw,
                        position,
                        attempt,
                        %error,
                        "disk poll: segment read failed"
                    );
                    compio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            }
        }
        None
    }
}

/// Byte position of the sparse-index entry at or below the query's offset /
/// timestamp, or `None` when the query is below the first indexed entry (the
/// caller then scans from the segment start). Mirrors `disk_poll_start`'s
/// resident-index resolution for the sealed, off-pump path.
/// Start byte for `query`, and the offset of the index entry it resolved to.
/// The entry sits at or before the requested offset, so the difference is the
/// run the walk has to skip before it can match anything.
fn resolve_index_position(index: &IggyIndexCache, query: MessageLookup) -> Option<(u64, u64)> {
    match query {
        MessageLookup::Offset { offset, .. } => index.offset_lower_bound(offset),
        MessageLookup::Timestamp { timestamp, .. } => index.timestamp_lower_bound(timestamp),
    }
    .map(|entry| (entry.position, entry.offset))
}

/// Walk stamped `[256B BatchHeader][blob]` batches in one disk
/// chunk, pushing matching fragments. Returns bytes consumed: the start
/// of the first batch that did not fully fit in the chunk (the caller
/// re-reads from there), or the chunk end when everything decoded.
#[allow(clippy::too_many_arguments)]
fn walk_disk_chunk(
    chunk: &Frozen<4096>,
    query: MessageLookup,
    count: u32,
    matched: &mut u32,
    fragments: &mut PollFragments<4096>,
    last_matching_offset: &mut Option<u64>,
    integrity: BatchIntegrity,
    namespace_raw: u64,
) -> ChunkWalk {
    let bytes: &[u8] = chunk;
    let mut cursor = 0usize;
    let mut needed = 0usize;

    while *matched < count && cursor + COMMAND_HEADER_SIZE <= bytes.len() {
        let batch = match batch::decode_batch_slice_with(&bytes[cursor..], integrity) {
            Ok(batch) => batch,
            Err(WireError::InvalidBatchChecksum {
                stored: found,
                computed: expected,
                base_offset,
            }) => {
                // Distinguished from the incomplete-tail case below: this batch is
                // entirely present and fails its own checksum, so it is damaged at rest.
                error!(
                    target: "iggy.partitions.diag",
                    plane = "partitions",
                    namespace_raw,
                    base_offset,
                    expected,
                    found,
                    position = cursor,
                    "disk poll: batch checksum mismatch; segment is corrupt at rest"
                );
                return ChunkWalk {
                    consumed: cursor.min(bytes.len()),
                    needed: 0,
                    corrupt: true,
                };
            }
            Err(WireError::UnexpectedEof { need, .. })
                if need <= journal::partition_journal::PREPARE_BYTES_MAX =>
            {
                needed = need;
                break;
            }
            Err(error) => {
                error!(namespace_raw, position = cursor, %error, "invalid disk batch");
                return ChunkWalk {
                    consumed: cursor,
                    needed: 0,
                    corrupt: true,
                };
            }
        };
        let total_size = batch.header.total_size();

        if let Some(selection) = select_batch_slice(&batch, query, *matched) {
            // On disk a batch is the bare `[256B header][blob]`, so the batch
            // base is the chunk cursor (no preceding prepare header).
            push_selected_batch_fragments(
                fragments,
                last_matching_offset,
                matched,
                chunk,
                cursor,
                &batch,
                selection,
            );
        }

        cursor += total_size;
    }

    ChunkWalk {
        consumed: cursor.min(bytes.len()),
        needed,
        corrupt: false,
    }
}

/// How far [`walk_disk_chunk`] got, and whether it stopped on corruption rather
/// than on a batch that simply did not fit in the chunk.
struct ChunkWalk {
    consumed: usize,
    /// Bytes the batch that did not fit needs in full, from its own header, or
    /// zero when that header could not be read. Lets the caller re-read
    /// exactly the batch instead of doubling its way up to it.
    needed: usize,
    corrupt: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iggy_index::IggyIndex;
    use bytes::Bytes;
    use compio::io::AsyncWriteAtExt;
    use server_common::iobuf::Owned;
    use server_common::send_messages::{
        IggyMessage, IggyMessageHeader, IggyMessages, SendMessagesOwned,
    };
    use server_common::sharding::IggyNamespace;

    #[test]
    fn byte_limit_rewrites_whole_and_split_batches_with_valid_checksums() {
        const PAYLOAD: &[u8] = b"payload";
        let mut messages = IggyMessages::with_capacity(2);
        for _ in 0..2 {
            messages.push(IggyMessage {
                header: IggyMessageHeader {
                    payload_length: u32::try_from(PAYLOAD.len()).unwrap(),
                    ..Default::default()
                },
                payload: Bytes::from_static(PAYLOAD),
                user_headers: None,
            });
        }
        let mut batch =
            SendMessagesOwned::from_messages(IggyNamespace::new(1, 1, 0), &messages).unwrap();
        batch.header.base_offset = 42;
        batch.header.batch_checksum = batch.header.checksum_for_blob(&batch.blob);
        let mut bytes = vec![0; batch.header.total_size()];
        batch.header.encode_into(&mut bytes);
        bytes[BATCH_HEADER_SIZE..].copy_from_slice(&batch.blob);
        let max_bytes = POLL_RESPONSE_HEADER_SIZE
            + BATCH_HEADER_SIZE
            + batch::BATCH_MESSAGE_HEADER_SIZE
            + PAYLOAD.len();
        for split in [false, true] {
            let source: Frozen<4096> = Owned::copy_from_slice(&bytes).into();
            let mut fragments = PollFragments::new();
            if split {
                fragments.push(Fragment::whole(source.slice(..BATCH_HEADER_SIZE)));
                fragments.push(Fragment::whole(source.slice(BATCH_HEADER_SIZE..)));
            } else {
                fragments.push(Fragment::whole(source));
            }
            let result = PollPlan {
                context: PollContext {
                    history: PollHistoryId::default(),
                    consumer: PollingConsumer::Consumer(0, 0),
                    auto_commit: true,
                },
                commit_offset: 99,
                tier: PollTier::Resident {
                    fragments,
                    last_matching_offset: Some(43),
                    message_count: 2,
                },
            }
            .execute_resident();
            let (result, limited) = result.limit_bytes(max_bytes).unwrap();
            assert!(limited);
            assert_eq!(result.message_count(), 1);
            assert_eq!(result.last_matching_offset, Some(42));
            assert_eq!(result.commit_offset, 99);
            let bytes: Vec<u8> = result
                .fragments
                .into_iter()
                .flat_map(|fragment| fragment.into_frozen().to_vec())
                .collect();
            assert_eq!(bytes.len() + POLL_RESPONSE_HEADER_SIZE, max_bytes);
            let header = BatchHeader::decode(&bytes).unwrap();
            assert_eq!(header.base_offset, 42);
            assert_eq!(header.message_count, 1);
            assert_eq!(header.total_size(), bytes.len());
            assert_eq!(
                header.batch_checksum,
                header.checksum_for_blob(&bytes[BATCH_HEADER_SIZE..])
            );
        }
    }

    /// Write a sealed-segment index file too large to materialize
    /// (`entry_count * IGGY_INDEX_SIZE > SEALED_INDEX_RESIDENT_MAX_BYTES`), so
    /// `resolve_sealed_start` takes the file-backed lookup path the offset
    /// cursor memoizes. Entry `i` maps offset `i * 10` to position `i * 100`.
    async fn write_oversized_index(dir: &std::path::Path, start_offset: u64) -> u64 {
        let entry_count =
            SEALED_INDEX_RESIDENT_MAX_BYTES / crate::iggy_index::IGGY_INDEX_SIZE as u64 + 1;
        let mut bytes = Vec::with_capacity(
            usize::try_from(entry_count).unwrap() * crate::iggy_index::IGGY_INDEX_SIZE,
        );
        for i in 0..entry_count {
            bytes.extend_from_slice(&crate::iggy_index::IggyIndexCache::serialize(
                &IggyIndex::new(i * 10, i + 1, i * 100),
            ));
        }
        let path = format!("{}/{:0>20}.index", dir.display(), start_offset);
        let mut file = compio::fs::File::create(&path).await.expect("create index");
        let (written, _) = file.write_all_at(bytes, 0).await.into();
        written.expect("write index");
        file.sync_all().await.expect("sync index");
        entry_count
    }

    fn sizing_plan(bytes_per_message: Option<u32>, widest_batch_bytes: u64) -> DiskReadPlan {
        DiskReadPlan {
            partition_dir: PartitionDirResolution::NoFiles,
            bytes_per_message,
            widest_batch_bytes,
            segments: Vec::new(),
            start_position: 0,
            start_index_offset: None,
            namespace_raw: 0,
            validate_checksum: false,
        }
    }

    /// A batch is the unit the walk can consume, so a count-derived estimate
    /// that lands under one decodes nothing and pays the quadrupling re-read.
    /// A poll one message short of a producer's batch was the worst case,
    /// reading about five times what a flat megabyte would have.
    #[test]
    fn chunk_len_never_lands_under_a_whole_batch() {
        let batch = COMMAND_HEADER_SIZE as u64 + 1000 * 1000;
        let mean = u32::try_from(batch / 1000).expect("mean fits");
        let plan = sizing_plan(Some(mean), batch);

        assert!(plan.chunk_len(999) >= batch);
        assert!(plan.chunk_len(500) >= batch);
        assert!(plan.chunk_len(1) >= batch);
        // The floor never pushes a read above what an unsized poll would take.
        assert_eq!(
            sizing_plan(Some(mean), 4 << 20).chunk_len(1),
            DISK_POLL_CHUNK_MAX
        );
        // A count wide enough to matter still wins over the floor.
        assert_eq!(
            sizing_plan(Some(10), 2048).chunk_len(1000),
            DISK_POLL_CHUNK_MIN
        );
    }

    #[test]
    fn chunk_len_sizes_the_first_read_from_the_requested_count() {
        let plan = |bytes_per_message| sizing_plan(bytes_per_message, 0);

        // Nothing committed yet, so nothing bridges a count to bytes.
        assert_eq!(plan(None).chunk_len(1000), DISK_POLL_CHUNK_MAX);
        // A thousand small messages used to read a megabyte to return 150 KB.
        assert_eq!(
            plan(Some(150)).chunk_len(1000),
            150 * 1000 + COMMAND_HEADER_SIZE as u64
        );
        // The floor keeps a poll for a few messages off a read per batch, the
        // ceiling is what every poll read before it was sized at all.
        assert_eq!(plan(Some(150)).chunk_len(1), DISK_POLL_CHUNK_MIN);
        assert_eq!(plan(Some(64 << 10)).chunk_len(1000), DISK_POLL_CHUNK_MAX);
        // Wide messages and a wide count must clamp, never wrap.
        assert_eq!(
            plan(Some(u32::MAX)).chunk_len(u32::MAX),
            DISK_POLL_CHUNK_MAX
        );
    }

    #[cfg(feature = "poll-diagnostics")]
    #[compio::test]
    async fn read_accounting_includes_failed_retry_attempts() {
        let directory = tempfile::tempdir().unwrap();
        let file = compio::fs::File::create(directory.path().join("empty.log"))
            .await
            .unwrap();
        let plan = DiskReadPlan {
            partition_dir: PartitionDirResolution::NoFiles,
            bytes_per_message: None,
            widest_batch_bytes: 0,
            segments: Vec::new(),
            start_position: 0,
            start_index_offset: None,
            namespace_raw: 0,
            validate_checksum: true,
        };
        let mut walk = DiskWalk::starting_at(0, 0);
        assert!(
            plan.read_chunk_with_retry(&file, 64, &mut walk)
                .await
                .is_none()
        );
        assert_eq!(walk.chunk_reads, 3);
        assert_eq!(walk.requested_bytes, 192);
        assert_eq!(walk.matched, 0);
    }

    #[cfg(feature = "poll-diagnostics")]
    #[compio::test]
    async fn incomplete_batch_reread_keeps_its_exact_length_for_the_walk() {
        const BATCH_COUNT: u32 = 4;
        let directory = tempfile::tempdir().unwrap();
        let length = disk_batch(128 << 10, 0).len();
        let mut records = Vec::with_capacity(length * BATCH_COUNT as usize);
        for offset in 0..BATCH_COUNT {
            records.extend_from_slice(&disk_batch(128 << 10, u64::from(offset)));
        }
        let mut file = compio::fs::File::create(directory.path().join("batches.log"))
            .await
            .unwrap();
        let (written, _) = file.write_all_at(records, 0).await.into();
        written.unwrap();
        let file = compio::fs::File::open(directory.path().join("batches.log"))
            .await
            .unwrap();
        let plan = sizing_plan(Some(1), 0);
        let mut walk = DiskWalk::starting_at(0, 0);
        assert!(matches!(
            plan.walk_segment(
                &file,
                MessageLookup::Offset {
                    offset: 0,
                    count: BATCH_COUNT,
                    ceiling: u64::MAX
                },
                BATCH_COUNT,
                length as u64 * u64::from(BATCH_COUNT),
                &mut walk
            )
            .await,
            SegmentWalk::Done
        ));
        assert_eq!(walk.matched, BATCH_COUNT);
        assert_eq!(walk.chunk_reads, BATCH_COUNT + 1);
        assert_eq!(
            walk.requested_bytes,
            DISK_POLL_CHUNK_MIN + length as u64 * u64::from(BATCH_COUNT)
        );
    }

    #[cfg(feature = "poll-diagnostics")]
    #[compio::test]
    async fn oversized_batch_does_not_widen_later_reads_or_the_next_segment() {
        const SMALL_BATCHES: u64 = 16;
        for separate_segments in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let mut wide = disk_batch(3 << 20, 0);
            let wide_length = wide.len() as u64;
            let mut tail = Vec::new();
            for offset in 1..=SMALL_BATCHES {
                tail.extend_from_slice(&disk_batch(128 << 10, offset));
            }
            let segments = if separate_segments {
                vec![wide, tail]
            } else {
                wide.extend_from_slice(&tail);
                vec![wide]
            };
            let plan = sizing_plan(Some(1), 0);
            let mut walk = DiskWalk::starting_at(0, 0);
            for (index, records) in segments.into_iter().enumerate() {
                let path = directory.path().join(format!("{index}.log"));
                std::fs::write(&path, &records).unwrap();
                let file = compio::fs::File::open(path).await.unwrap();
                walk.position = 0;
                assert!(matches!(
                    plan.walk_segment(
                        &file,
                        MessageLookup::Offset {
                            offset: 0,
                            count: 2,
                            ceiling: u64::MAX
                        },
                        2,
                        records.len() as u64,
                        &mut walk,
                    )
                    .await,
                    SegmentWalk::Done
                ));
            }
            assert_eq!(walk.matched, 2);
            assert_eq!(walk.chunk_reads, 3);
            assert_eq!(
                walk.requested_bytes,
                DISK_POLL_CHUNK_MIN + wide_length + DISK_POLL_CHUNK_MAX,
                "one oversized batch must not raise subsequent reads above the chunk ceiling"
            );
        }
    }

    fn disk_batch(payload_length: u32, offset: u64) -> Vec<u8> {
        let mut messages = IggyMessages::with_capacity(1);
        messages.push(IggyMessage {
            header: IggyMessageHeader {
                payload_length,
                ..Default::default()
            },
            payload: Bytes::from(vec![1; usize::try_from(payload_length).unwrap()]),
            user_headers: None,
        });
        let mut batch =
            SendMessagesOwned::from_messages(IggyNamespace::new(1, 1, 0), &messages).unwrap();
        batch.header.base_offset = offset;
        batch.header.batch_checksum = batch.header.checksum_for_blob(&batch.blob);
        let mut record = vec![0; batch.header.total_size()];
        batch.header.encode_into(&mut record);
        record[COMMAND_HEADER_SIZE..].copy_from_slice(&batch.blob);
        record
    }

    fn offset_query(offset: u64) -> MessageLookup {
        MessageLookup::Offset {
            offset,
            count: 1,
            ceiling: u64::MAX,
        }
    }

    #[compio::test]
    async fn sealed_offset_cursor_answers_in_interval_polls_without_the_index_file() {
        let dir = std::env::temp_dir().join(format!(
            "iggy-poll-cursor-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after epoch")
                .as_nanos(),
        ));
        compio::fs::create_dir_all(&dir).await.expect("create dir");
        write_oversized_index(&dir, 0).await;

        let handle: SealedSegmentHandle = Rc::new(SealedSegmentReadState::default());
        let segment = DiskSegment {
            start_offset: 0,
            persisted: u64::MAX,
            read_state: Rc::clone(&handle),
            sealed: true,
        };
        let plan = DiskReadPlan {
            partition_dir: PartitionDirResolution::Resolved(dir.display().to_string()),
            bytes_per_message: None,
            widest_batch_bytes: 0,
            segments: Vec::new(),
            start_position: 0,
            start_index_offset: None,
            namespace_raw: 0,
            validate_checksum: false,
        };
        let partition_dir = dir.display().to_string();

        // First poll pays the on-file lookup and memoizes entry 2's interval
        // [20, 30): offset 25 resolves to entry 2 (offset 20 -> position 200).
        let first = plan
            .resolve_sealed_start(&segment, offset_query(25), &partition_dir, usize::MAX)
            .await;
        // The entry offset rides along so the caller can size its first read
        // to cover the run between that entry and the requested offset.
        assert_eq!(first, Some((200, 20)));
        let cursor = handle.offset_cursor.get().expect("cursor memoized");
        assert_eq!(
            (cursor.offset, cursor.valid_until, cursor.position),
            (20, 30, 200),
        );

        // Delete the index file: an in-interval re-poll must still resolve
        // (proof the cursor answered with zero index-file reads)...
        std::fs::remove_dir_all(&dir).expect("remove dir");
        let in_interval = plan
            .resolve_sealed_start(&segment, offset_query(29), &partition_dir, usize::MAX)
            .await;
        assert_eq!(in_interval, Some((200, 20)));

        // ...while an offset past the interval misses the cursor, reaches for
        // the (now gone) file, and falls back to the byte-0 scan.
        let past_interval = plan
            .resolve_sealed_start(&segment, offset_query(30), &partition_dir, usize::MAX)
            .await;
        assert_eq!(past_interval, None);
    }

    // Resident execution passes already selected bytes through unchanged.
    // The snapshot test needs a nonempty payload but does not decode messages.
    fn placeholder_fragments() -> PollFragments<4096> {
        let mut fragments = PollFragments::new();
        fragments.push(crate::types::Fragment::whole(
            Owned::<4096>::zeroed(8).into(),
        ));
        fragments
    }

    #[test]
    fn resident_read_returns_snapshot_facts() {
        let snapshot_history = PollHistoryId::default();
        let partition_commit_offset = 42;
        let last_selected_offset = 5;
        let consumer_id = 7;
        let partition_id = 0;
        let resident_plan = PollPlan {
            commit_offset: partition_commit_offset,
            context: PollContext {
                history: snapshot_history,
                consumer: PollingConsumer::Consumer(consumer_id, partition_id),
                auto_commit: true,
            },
            tier: PollTier::Resident {
                fragments: placeholder_fragments(),
                last_matching_offset: Some(last_selected_offset),
                message_count: 1,
            },
        };
        assert!(!resident_plan.needs_off_pump_io());

        // Reading preserves both the partition frontier and the last selected
        // message offset. These are snapshot facts awaiting owner acceptance.
        let read_result = resident_plan.execute_resident();
        assert_eq!(read_result.context.history, snapshot_history);
        assert_eq!(read_result.commit_offset, partition_commit_offset);
        assert_eq!(read_result.last_matching_offset, Some(last_selected_offset));
        assert!(!read_result.fragments.is_empty());
    }

    #[test]
    fn empty_read_returns_no_progress() {
        let consumer_id = 7;
        let partition_id = 0;
        let empty_plan = PollPlan {
            commit_offset: 9,
            context: PollContext {
                history: PollHistoryId::default(),
                consumer: PollingConsumer::Consumer(consumer_id, partition_id),
                auto_commit: true,
            },
            tier: PollTier::Empty,
        };

        // Automatic commits are enabled, but no matching message means there
        // is no selected offset for the owner to apply as consumer progress.
        let read_result = empty_plan.execute_resident();
        assert!(read_result.fragments.is_empty());
        assert_eq!(read_result.last_matching_offset, None);
    }
    #[compio::test]
    async fn deferred_disk_result_enforces_byte_limits_and_rejects_corruption() {
        for corrupt_tail in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let mut bytes = disk_batch(10, 0);
            let first_batch_bytes = bytes.len();
            let mut tail = disk_batch(10, 9);
            if corrupt_tail {
                *tail.last_mut().unwrap() ^= 1;
            }
            bytes.extend(tail);
            std::fs::write(directory.path().join("00000000000000000000.log"), &bytes).unwrap();
            let plan = PollPlan {
                context: PollContext {
                    history: PollHistoryId::default(),
                    consumer: PollingConsumer::Consumer(0, 0),
                    auto_commit: true,
                },
                commit_offset: 9,
                tier: PollTier::Disk {
                    query: MessageLookup::Offset {
                        offset: 0,
                        count: 2,
                        ceiling: 9,
                    },
                    resident_tail: ResidentTailSnapshot {
                        entries: Vec::new(),
                        oldest_resident: None,
                    },
                    disk: DiskReadPlan {
                        partition_dir: PartitionDirResolution::Resolved(
                            directory.path().display().to_string(),
                        ),
                        bytes_per_message: None,
                        widest_batch_bytes: 0,
                        segments: vec![DiskSegment {
                            start_offset: 0,
                            persisted: bytes.len() as u64,
                            read_state: Rc::default(),
                            sealed: false,
                        }],
                        start_position: 0,
                        start_index_offset: None,
                        namespace_raw: 0,
                        validate_checksum: true,
                    },
                },
            };
            let result = plan.execute_with_limit(1 << 20).await;
            if corrupt_tail {
                assert_eq!(
                    result.message_count(),
                    1,
                    "a valid prefix must not hide corruption"
                );
                assert!(matches!(
                    result.checked(),
                    Err(IggyError::CannotReadMessage)
                ));
            } else {
                assert_eq!(
                    result.message_count(),
                    2,
                    "sparse offsets count as two messages"
                );
                let (result, limited) = result
                    .checked()
                    .unwrap()
                    .limit_bytes(POLL_RESPONSE_HEADER_SIZE + first_batch_bytes)
                    .unwrap();
                assert!(limited);
                assert_eq!(result.message_count(), 1);
                assert_eq!(result.last_matching_offset, Some(0));
                assert_eq!(result.commit_offset, 9);
            }
        }
    }

    #[compio::test]
    async fn deferred_disk_budget_rejects_oversized_batch_before_growing_buffer() {
        let directory = tempfile::tempdir().unwrap();
        let bytes = disk_batch(2 << 20, 0);
        let path = directory.path().join("batch.log");
        std::fs::write(&path, &bytes).unwrap();
        let file = compio::fs::File::open(path).await.unwrap();
        let plan = sizing_plan(Some(1), 0);
        let mut walk = DiskWalk::starting_at(0, 0);
        walk.max_bytes = 1 << 20;
        assert!(matches!(
            plan.walk_segment(&file, offset_query(0), 1, bytes.len() as u64, &mut walk)
                .await,
            SegmentWalk::Faulted
        ));
        assert!(walk.limited);
        assert_eq!(walk.matched, 0);
        assert!(walk.fragments.is_empty());
    }

    fn resident(fragments: PollFragments) -> PollReadResult {
        PollPlan {
            context: PollContext {
                history: PollHistoryId::default(),
                consumer: PollingConsumer::Consumer(0, 0),
                auto_commit: true,
            },
            commit_offset: 0,
            tier: PollTier::Resident {
                fragments,
                last_matching_offset: None,
                message_count: 0,
            },
        }
        .execute_resident()
    }

    fn windows(source: &Frozen<4096>, count: usize) -> PollFragments {
        (0..count)
            .map(|window| Fragment::slice(source.clone(), window * 4096, window * 4096 + 512))
            .collect()
    }

    #[test]
    fn retained_bytes_counts_one_allocation_once_however_many_slices_borrow_it() {
        let source: Frozen<4096> = Owned::copy_from_slice(&vec![7; 1 << 20]).into();
        let one = resident(windows(&source, 1)).retained_bytes();
        let four = resident(windows(&source, 4)).retained_bytes();
        assert_eq!(one, four);
        assert!(four < 2 * source.allocation_bytes());
    }

    #[test]
    fn compacting_releases_the_borrowed_allocation_and_preserves_the_selection() {
        let source: Frozen<4096> = Owned::copy_from_slice(&vec![7; 1 << 20]).into();
        let borrowed = resident(windows(&source, 4));
        let selection: Vec<Vec<u8>> = borrowed
            .fragments
            .iter()
            .map(|fragment| fragment.as_slice().to_vec())
            .collect();
        let charge = borrowed.retained_bytes();
        let compacted = borrowed.compacted();
        assert!(compacted.retained_bytes() < charge);
        let kept: Vec<Vec<u8>> = compacted
            .fragments
            .iter()
            .map(|fragment| fragment.as_slice().to_vec())
            .collect();
        assert_eq!(kept, selection);
    }
}
