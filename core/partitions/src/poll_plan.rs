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

//! Owned, borrow-free poll execution.
//!
//! A poll must not hold a partition reference across an `.await`: the shard pump
//! can reallocate the partitions `Vec` (`ReconcileOp::InsertOwned`) or take a
//! `&mut` to the same namespace while a poll is parked, dangling the reference.
//! So `IggyPartition::build_poll_plan` captures everything a poll needs
//! synchronously under the borrow into the owned types here, drops the borrow,
//! then [`PollPlan::execute`] runs the disk read + the in-memory auto-commit
//! apply on owned data alone: consumer offsets are already `Arc`, the journal
//! tail is a point-in-time `Frozen` snapshot, and each sealed segment carries a
//! shared [`SealedSegmentReadState`] handle (a plain `Rc`, not a partition
//! reference) whose read fd + sparse index the read reuses or fills on a miss.
//! No value in this module holds a partition reference, so executing a plan is
//! sound on a detached task concurrently with the pump's own writes.

use crate::PollFragments;
use crate::iggy_index::IggyIndexCache;
use crate::iggy_index_reader::IggyIndexReader;
use crate::journal::{MessageLookup, push_selected_batch_fragments, select_batch_slice};
use compio::io::AsyncReadAtExt;
use iggy_common::{
    ConsumerGroupId, ConsumerGroupOffsets, ConsumerKind, ConsumerOffset, ConsumerOffsets,
};
use server_common::iobuf::{Frozen, Owned};
use server_common::send_messages2::{COMMAND_HEADER_SIZE, decode_batch_slice};
use std::cell::RefCell;
use std::hash::Hash;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tracing::warn;

/// Per-sealed-segment read state, shared as a cheap `Rc` handle between the
/// owning partition and the off-borrow [`DiskReadPlan`] (a plain `Rc`, never a
/// partition reference, so the read runs off the pump). Both slots fill lazily
/// on the first sealed poll and are reused after. The pump drops its handle
/// when the segment is retired, so the state frees once any in-flight poll
/// holding a clone finishes (a cached fd meanwhile reads the unlinked inode,
/// which is fine). The active segment is never cached.
#[derive(Debug, Default)]
pub struct SealedSegmentReadState {
    /// Read-only descriptor; compio `File` clones share the kernel fd, so a hit
    /// avoids the per-poll `openat` (an `io_uring` op prone to io-wq punts) and
    /// preserves kernel readahead. `None` until the first sealed poll opens it.
    pub(crate) fd: RefCell<Option<compio::fs::File>>,
    /// Sparse offset/timestamp index reloaded from the `.index` file the
    /// segment dropped at rotation, so a poll resolves the start byte in
    /// O(log n) instead of scanning the whole segment from byte 0 (the stall).
    /// `None` until the first sealed poll loads it.
    pub(crate) index: RefCell<Option<IggyIndexCache>>,
}

pub type SealedSegmentHandle = Rc<SealedSegmentReadState>;

/// Owned, borrow-free inputs for the disk tier of a poll (see module docs). A
/// sealed segment reuses its cached [`SealedSegmentReadState`] (read fd + sparse
/// index); the active segment (and any cache miss) opens by path and resolves
/// from its resident index, because sealed segments drop both at rotation.
pub struct DiskReadPlan {
    pub(crate) partition_dir: Option<String>,
    /// Segments to walk, snapshotted from the poll's starting segment onward
    /// (see `build_poll_plan`); `start_position` is the byte offset into the
    /// first one.
    pub(crate) segments: Vec<DiskSegment>,
    pub(crate) start_position: u64,
    pub(crate) namespace_raw: u64,
}

pub struct DiskSegment {
    pub(crate) start_offset: u64,
    pub(crate) persisted: u64,
    /// Shared read state, cloned from the owning partition at plan time for a
    /// SEALED segment; `None` for the active segment, which always opens fresh
    /// and resolves from its resident index. See [`SealedSegmentReadState`].
    pub(crate) read_state: Option<SealedSegmentHandle>,
}

/// Owned auto-commit input, applied off the partition borrow after a poll (see
/// module docs). Only the in-memory apply happens here; durability is the
/// replicated [`crate::iggy_partition::IggyPartition::apply_staged_consumer_offset_commit`]
/// path's job on every node, driven by the `StoreConsumerOffset2` op the serving
/// shard submits from [`AutoCommitApplied`]. A poll-local disk write would be
/// node-local only and diverge on failover.
pub struct AutoCommitCtx {
    pub(crate) target: AutoCommitTarget,
}

/// The offset an `auto_commit` poll applied in memory, surfaced for replication.
///
/// The serving shard replicates it through the partition consensus (the only
/// cross-node durable path); `kind` + `consumer_id` are the offset key the
/// submitted `StoreConsumerOffset2` op must carry.
pub struct AutoCommitApplied {
    pub kind: ConsumerKind,
    pub consumer_id: u32,
    pub offset: u64,
}

/// The lock-free offset map this auto-commit updates, captured as an owned
/// `Arc` so the apply needs no partition borrow. `create_path` builds the
/// `ConsumerOffset` entry on first commit for a consumer that has none yet.
pub enum AutoCommitTarget {
    Consumer {
        offsets: Arc<ConsumerOffsets>,
        consumer_id: u32,
        create_path: Option<String>,
    },
    ConsumerGroup {
        offsets: Arc<ConsumerGroupOffsets>,
        group_id: u32,
        create_path: Option<String>,
    },
}

/// Owned cooperative-rebalance input: a group's lock-free `last_polled` map
/// (captured as an `Arc`) plus its id, so the highest offset served to the group
/// is recorded off the partition borrow after the poll completes (the served
/// offset is unknown until then). See [`PollPlan::execute`].
pub struct LastPolledCtx {
    pub(crate) offsets: Arc<ConsumerGroupOffsets>,
    pub(crate) group_id: usize,
}

impl LastPolledCtx {
    /// Bump the group's recorded high-water served offset (monotone via
    /// `fetch_max`). Lock-free `papaya` on an owned `Arc`, so sound off the pump.
    #[allow(clippy::cast_possible_truncation)]
    fn record(&self, last_offset: u64) {
        let guard = self.offsets.pin();
        let key = ConsumerGroupId(self.group_id);
        if let Some(existing) = guard.get(&key) {
            existing.offset.fetch_max(last_offset, Ordering::Relaxed);
        } else {
            let created = ConsumerOffset::new(
                ConsumerKind::ConsumerGroup,
                u32::try_from(self.group_id).unwrap_or(u32::MAX),
                last_offset,
                String::new(),
            );
            guard.insert(key, created);
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

/// Everything a poll needs, captured by `IggyPartition::build_poll_plan` (see
/// module docs for the borrow contract).
pub struct PollPlan {
    /// Monotone high-water snapshot taken before the disk read, so it may lag a
    /// concurrent producer by the poll duration and self-corrects next poll.
    pub(crate) commit_offset: u64,
    pub(crate) auto_commit: Option<AutoCommitCtx>,
    pub(crate) last_polled: Option<LastPolledCtx>,
    pub(crate) tier: PollTier,
}

impl PollPlan {
    /// Whether executing this plan needs off-pump IO: only a `Disk` tier read.
    /// When `false` the result is fully resident and the caller runs
    /// [`Self::execute_resident`] + replies on the pump; when `true` it must
    /// spawn [`Self::execute`] so the pump is not blocked on file IO.
    ///
    /// Auto-commit no longer forces a detached task: its in-memory apply is
    /// synchronous and its durability rides consensus off the serving shard
    /// (no poll-local disk write), so a fully-resident `auto_commit` poll still
    /// replies inline.
    #[must_use]
    pub const fn needs_off_pump_io(&self) -> bool {
        matches!(self.tier, PollTier::Disk { .. })
    }

    /// Execute this plan off the partition borrow: disk read (if any), straddle
    /// splice into the owned resident-tail snapshot, then apply the auto-commit
    /// to the owned `Arc` offset map. Holds no partition reference (see module
    /// docs), so it is safe on a detached task. Returns the served fragments,
    /// the poll's high-water offset, and the auto-committed offset (if any) for
    /// the serving shard to replicate through consensus.
    pub async fn execute(self) -> (PollFragments<4096>, u64, Option<AutoCommitApplied>) {
        let commit_offset = self.commit_offset;
        let (fragments, last_matching_offset) = match self.tier {
            PollTier::Empty => (PollFragments::new(), None),
            PollTier::Resident {
                fragments,
                last_matching_offset,
            } => (fragments, last_matching_offset),
            PollTier::Disk {
                disk,
                query,
                resident_tail,
            } => match disk.read_disk(query).await {
                // Disk walked cleanly and matched nothing: the query offset is
                // below disk retention too, so the match (if any) is journal-
                // resident. Serve the journal forward (retention-recovery) from
                // the resident-tail snapshot with the ORIGINAL query (offset or
                // timestamp); no contiguity gate, this is not a straddle.
                DiskReadOutcome::Empty => {
                    crate::journal::select_resident(&resident_tail.entries, query)
                        .unwrap_or_else(|| (PollFragments::new(), None))
                }
                // Disk read stopped on an IO fault. Fail-closed: return an empty
                // poll WITHOUT the journal-forward fallback. Falling forward
                // here would splice the next resident op over the unreadable run
                // and silently skip live data; the fault instead surfaces as a
                // visibly stuck consumer that recovers on a later poll once the
                // segment reads again.
                DiskReadOutcome::Faulted => (PollFragments::new(), None),
                // Straddle: continue past the last disk match into the resident
                // tail (gate + race argument live on `straddle_continuation`).
                DiskReadOutcome::Matched {
                    mut fragments,
                    last_matching_offset,
                    matched,
                } => {
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
                        Some((journal_fragments, journal_last)) => {
                            fragments.extend(journal_fragments);
                            (fragments, journal_last.or(last_matching_offset))
                        }
                        None => (fragments, last_matching_offset),
                    }
                }
            },
        };

        finish(
            self.last_polled.as_ref(),
            self.auto_commit,
            commit_offset,
            fragments,
            last_matching_offset,
        )
    }

    /// Synchronous fast path for a fully-resident poll
    /// ([`Self::needs_off_pump_io`] is `false`): no disk read, so the pump
    /// applies the auto-commit in memory and replies inline without spawning.
    /// The auto-committed offset is returned for the serving shard to replicate.
    #[must_use]
    pub fn execute_resident(self) -> (PollFragments<4096>, u64, Option<AutoCommitApplied>) {
        let commit_offset = self.commit_offset;
        let (fragments, last_matching_offset) = match self.tier {
            PollTier::Empty => (PollFragments::new(), None),
            PollTier::Resident {
                fragments,
                last_matching_offset,
            } => (fragments, last_matching_offset),
            // `needs_off_pump_io` is true for every Disk tier, so the dispatch
            // gate never routes one here.
            PollTier::Disk { .. } => {
                unreachable!("execute_resident on Disk tier; needs_off_pump_io guards this")
            }
        };
        finish(
            self.last_polled.as_ref(),
            self.auto_commit,
            commit_offset,
            fragments,
            last_matching_offset,
        )
    }
}

/// Common tail of [`PollPlan::execute`] and [`PollPlan::execute_resident`],
/// factored out so the high-water record, auto-commit, and returned triple
/// stay identical across both.
fn finish(
    last_polled: Option<&LastPolledCtx>,
    auto_commit: Option<AutoCommitCtx>,
    commit_offset: u64,
    fragments: PollFragments<4096>,
    last_matching_offset: Option<u64>,
) -> (PollFragments<4096>, u64, Option<AutoCommitApplied>) {
    if let (Some(last_polled), Some(last_offset)) = (last_polled, last_matching_offset) {
        last_polled.record(last_offset);
    }
    let auto_commit_applied = apply_auto_commit(auto_commit, &fragments, last_matching_offset);
    (fragments, commit_offset, auto_commit_applied)
}

/// Apply an `auto_commit` to the in-memory offset map (monotone) and surface
/// the committed offset so the serving shard can replicate it through
/// consensus. `None` when the poll served nothing (empty fragments) or no
/// auto-commit was requested. Shared by [`PollPlan::execute`] and
/// [`PollPlan::execute_resident`] so both apply identically.
///
/// The eager in-memory apply preserves read-your-own-poll for a tight
/// `Consumer::Next` loop that reads before the replicated commit lands; the
/// commit's apply is an idempotent monotone set, so the double-apply converges.
fn apply_auto_commit(
    auto_commit: Option<AutoCommitCtx>,
    fragments: &PollFragments<4096>,
    last_matching_offset: Option<u64>,
) -> Option<AutoCommitApplied> {
    let auto_commit = auto_commit?;
    if fragments.is_empty() {
        return None;
    }
    let last_offset = last_matching_offset?;
    auto_commit.apply(last_offset);
    let (kind, consumer_id) = auto_commit.kind_and_id();
    Some(AutoCommitApplied {
        kind,
        consumer_id,
        offset: last_offset,
    })
}

pub enum PollTier {
    Empty,
    Resident {
        fragments: PollFragments<4096>,
        last_matching_offset: Option<u64>,
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
    },
    /// Walk completed with no fault and matched nothing. The query offset is
    /// below disk retention too, so the caller may serve the journal forward
    /// (retention-recovery) without skipping anything.
    Empty,
    /// Walk stopped on an IO fault before matching anything. Fail-closed: the
    /// caller returns an empty poll so the consumer cursor does not advance
    /// past data that may still be present-but-unreadable.
    Faulted,
}

impl DiskReadPlan {
    /// Serve a poll from the on-disk segment files, off the partition borrow.
    /// Reads from owned descriptors so no partition reference is held across
    /// the file IO. Walks stamped `[256B SendMessages2Header][blob]` batches in
    /// chunked reads, re-reading a batch split across a chunk boundary in the
    /// next chunk.
    #[allow(clippy::cast_possible_truncation)]
    pub(crate) async fn read_disk(self, query: MessageLookup) -> DiskReadOutcome {
        const DISK_POLL_CHUNK: u64 = 1 << 20;

        let count = query.count();
        if count == 0 || self.segments.is_empty() {
            return DiskReadOutcome::Empty;
        }
        let Some(partition_dir) = self.partition_dir.as_deref() else {
            // Simulated in-memory persistence, or no writer was resolvable
            // (e.g. mid-rotation): no files to read. This is not an IO fault on
            // present data, so it is `Empty`: the caller serves the resident
            // journal tier (the sim's only tier) without skipping anything.
            // TODO(hubcio): a live partition mid-rotation can also land here
            // with disk-resident-but-unresolvable data; the journal-forward
            // could then skip those offsets. Distinguish sim/no-files (Empty)
            // from a transiently-unresolvable writer (Faulted, fail-closed).
            warn!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                namespace_raw = self.namespace_raw,
                segment_count = self.segments.len(),
                "disk poll: no partition dir to resolve segment files; serving journal tier"
            );
            return DiskReadOutcome::Empty;
        };

        // `start_position` applies to the first snapshotted segment; each later
        // segment is walked from byte 0 (reset at the end of every iteration).
        //
        // A sealed first segment dropped its resident index at rotation, so
        // `disk_poll_start` fell back to byte 0. Reload the sparse index (once,
        // then cached) and resolve the start byte so the walk skips straight to
        // the target instead of scanning the whole segment - the poll stall. A
        // miss or load failure keeps `start_position` (the pre-existing
        // full-scan fallback). The active segment carries no read state, so its
        // resident-index-resolved `start_position` is left untouched.
        let mut position = match self.segments.first() {
            Some(first) => self
                .resolve_sealed_start(first, query, partition_dir)
                .await
                .unwrap_or(self.start_position),
            None => self.start_position,
        };
        let mut fragments = PollFragments::new();
        let mut last_matching_offset = None;
        let mut matched: u32 = 0;
        // Set when an open/read retry exhausts. The walk breaks immediately so
        // later segments are never read into the result (which would leave a
        // gap at the faulted segment). Pre-fault matches are still served.
        let mut faulted = false;

        'walk: for segment in &self.segments {
            if matched >= count {
                break;
            }
            let persisted = segment.persisted;
            if persisted == 0 || position >= persisted {
                // Benign skip: nothing persisted for this segment yet, or the
                // start position is already past it. Not a fault.
                position = 0;
                continue;
            }
            let path = format!("{partition_dir}/{:0>20}.log", segment.start_offset);
            let Some(file) = self.resolve_segment_file(segment, &path).await else {
                // Open exhausted retries: the segment may hold present-but-
                // unreadable data. Stop here rather than walking past it.
                faulted = true;
                break 'walk;
            };

            let mut chunk_len = DISK_POLL_CHUNK;
            while matched < count && position < persisted {
                let len = (persisted - position).min(chunk_len) as usize;
                let Some(chunk) = self.read_chunk_with_retry(&file, position, len).await else {
                    // Chunk read exhausted retries: same fail-closed reason as
                    // a failed open.
                    faulted = true;
                    break 'walk;
                };
                let consumed = walk_disk_chunk(
                    &chunk,
                    query,
                    count,
                    &mut matched,
                    &mut fragments,
                    &mut last_matching_offset,
                );
                if consumed == 0 {
                    if (len as u64) >= persisted - position {
                        // The whole remainder fit yet no complete batch
                        // decoded: a corrupt batch in this segment. Fail-closed
                        // like an IO fault (set `faulted`, stop the walk) so a
                        // later segment is never served over the corrupt run,
                        // which would punch a silent gap into the poll.
                        faulted = true;
                        break 'walk;
                    }
                    // A single batch larger than the chunk: grow and re-read.
                    chunk_len = chunk_len.saturating_mul(4);
                    continue;
                }
                chunk_len = DISK_POLL_CHUNK;
                position += consumed as u64;
            }
            position = 0;
        }

        if matched > 0 {
            // Pre-fault matches are always a contiguous prefix (the walk stops
            // at the first fault), so a partial result carries no gap.
            DiskReadOutcome::Matched {
                fragments,
                last_matching_offset,
                matched,
            }
        } else if faulted {
            DiskReadOutcome::Faulted
        } else {
            DiskReadOutcome::Empty
        }
    }

    /// Resolve the read-only descriptor for `segment`'s file. A sealed segment
    /// clones its cached fd on a hit (sharing the kernel fd, no syscall) and, on
    /// a miss, opens by path and stores the fd back so later polls skip the
    /// `openat`. The active segment (no cache slot) always opens fresh. Returns
    /// `None` only when the open exhausts its retries (the caller fails closed).
    async fn resolve_segment_file(
        &self,
        segment: &DiskSegment,
        path: &str,
    ) -> Option<compio::fs::File> {
        let Some(handle) = &segment.read_state else {
            return self.open_segment_with_retry(path).await;
        };
        // Borrow only to clone the `Option<File>` out, never across the await.
        if let Some(cached) = handle.fd.borrow().clone() {
            return Some(cached);
        }
        let file = self.open_segment_with_retry(path).await?;
        // Benign race: a concurrent poll of the same segment may have filled the
        // slot while this open was in flight; overwriting with an equivalent fd
        // (same inode) is harmless.
        *handle.fd.borrow_mut() = Some(file.clone());
        Some(file)
    }

    /// Resolve the start byte for the poll's target segment from its sparse
    /// index, loading the `.index` file on the first sealed poll and caching it
    /// on the shared handle. Returns `None` (keep the byte-0 fallback) for the
    /// active segment (no handle), a below-range query, or a load failure.
    async fn resolve_sealed_start(
        &self,
        segment: &DiskSegment,
        query: MessageLookup,
        partition_dir: &str,
    ) -> Option<u64> {
        // TODO: a per-consumer cursor hint (the previous sealed poll's resolved
        // position) could seed this so a sequentially advancing consumer skips
        // the sparse-index lookup on repeated polls of the same segment.
        let handle = segment.read_state.as_ref()?;
        // Cache hit: resolve under a short borrow, never across the await.
        let cached = handle
            .index
            .borrow()
            .as_ref()
            .map(|index| resolve_index_position(index, query));
        if let Some(resolved) = cached {
            return resolved;
        }
        let path = format!("{partition_dir}/{:0>20}.index", segment.start_offset);
        let index = self.load_sealed_index(&path).await?;
        let resolved = resolve_index_position(&index, query);
        *handle.index.borrow_mut() = Some(index);
        resolved
    }

    /// Load a sealed segment's sparse index from its `.index` file. `None` on a
    /// missing/unreadable file so the caller falls back to a byte-0 scan (the
    /// pre-existing behavior); the load is retried on the next poll.
    async fn load_sealed_index(&self, path: &str) -> Option<IggyIndexCache> {
        match IggyIndexReader::new(path).await {
            Ok(reader) => match reader.load_all().await {
                Ok(index) => Some(index),
                Err(error) => {
                    warn!(
                        target: "iggy.partitions.diag",
                        plane = "partitions",
                        namespace_raw = self.namespace_raw,
                        path,
                        %error,
                        "disk poll: failed to read sparse index; scanning from segment start"
                    );
                    None
                }
            },
            Err(error) => {
                warn!(
                    target: "iggy.partitions.diag",
                    plane = "partitions",
                    namespace_raw = self.namespace_raw,
                    path,
                    %error,
                    "disk poll: failed to open sparse index; scanning from segment start"
                );
                None
            }
        }
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
    async fn read_chunk_with_retry(
        &self,
        file: &compio::fs::File,
        position: u64,
        len: usize,
    ) -> Option<Frozen<4096>> {
        for attempt in 0..3u8 {
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
fn resolve_index_position(index: &IggyIndexCache, query: MessageLookup) -> Option<u64> {
    match query {
        MessageLookup::Offset { offset, .. } => index.offset_lower_bound(offset),
        MessageLookup::Timestamp { timestamp, .. } => index.timestamp_lower_bound(timestamp),
    }
    .map(|entry| entry.position)
}

impl AutoCommitCtx {
    /// The offset key (kind + numeric id) this auto-commit targets, for the
    /// replicated `StoreConsumerOffset2` op the serving shard submits.
    pub(crate) const fn kind_and_id(&self) -> (ConsumerKind, u32) {
        match &self.target {
            AutoCommitTarget::Consumer { consumer_id, .. } => {
                (ConsumerKind::Consumer, *consumer_id)
            }
            AutoCommitTarget::ConsumerGroup { group_id, .. } => {
                (ConsumerKind::ConsumerGroup, *group_id)
            }
        }
    }

    /// Apply the committed offset to the in-memory map on the owned `Arc`
    /// handle, with NO partition reference. Uses the monotone
    /// [`upsert_offset_max`] so a stale off-pump auto-commit cannot rewind a
    /// newer explicit store; the maps are lock-free (`papaya`), so this is
    /// sound off the pump task.
    #[allow(clippy::cast_possible_truncation)]
    pub(crate) fn apply(&self, offset: u64) {
        match &self.target {
            AutoCommitTarget::Consumer {
                offsets,
                consumer_id,
                create_path,
            } => {
                let consumer_id = *consumer_id;
                let map: &ConsumerOffsets = offsets;
                upsert_offset_max(map, consumer_id as usize, offset, || {
                    create_path.as_deref().map_or_else(
                        || {
                            ConsumerOffset::new(
                                ConsumerKind::Consumer,
                                consumer_id,
                                0,
                                String::new(),
                            )
                        },
                        |path| ConsumerOffset::default_for_consumer(consumer_id, path),
                    )
                });
            }
            AutoCommitTarget::ConsumerGroup {
                offsets,
                group_id,
                create_path,
            } => {
                let group_id = *group_id;
                let key = ConsumerGroupId(group_id as usize);
                let map: &ConsumerGroupOffsets = offsets;
                upsert_offset_max(map, key, offset, || {
                    create_path.as_deref().map_or_else(
                        || {
                            ConsumerOffset::new(
                                ConsumerKind::ConsumerGroup,
                                group_id,
                                0,
                                String::new(),
                            )
                        },
                        |path| ConsumerOffset::default_for_consumer_group(key, path),
                    )
                });
            }
        }
    }
}

/// Upsert a committed offset into a lock-free `papaya` offset map: bump an
/// existing entry in place, or build one via `create_on_miss` on first commit
/// for a consumer/group that has none yet. Shared by the pump's
/// [`IggyPartition::apply_consumer_offset_commit`] and the off-pump
/// [`AutoCommitCtx::apply`] so both store offsets identically.
pub fn upsert_offset<K>(
    map: &papaya::HashMap<K, ConsumerOffset>,
    key: K,
    offset: u64,
    create_on_miss: impl FnOnce() -> ConsumerOffset,
) where
    K: Hash + Eq + Clone + Send + Sync,
{
    let guard = map.pin();
    if let Some(existing) = guard.get(&key) {
        existing.offset.store(offset, Ordering::Relaxed);
    } else {
        let created = create_on_miss();
        created.offset.store(offset, Ordering::Relaxed);
        guard.insert(key, created);
    }
}

/// Monotone variant of [`upsert_offset`] for the off-pump auto-commit: an
/// existing entry is bumped via `fetch_max` so a stale auto-commit racing a
/// newer explicit `StoreConsumerOffset` cannot rewind it backward. The
/// on-miss create branch is identical. The explicit pump path keeps
/// [`upsert_offset`] (`store`), since an explicit store may legitimately rewind.
///
/// Also used by the replicated commit-apply for a server auto-commit op
/// ([`crate::iggy_partition::IggyPartition::apply_consumer_offset_commit`]): its
/// offset was already advanced in memory by the eager poll-path apply, and this
/// commit can land behind a newer poll, so it must not `store` (rewind) it.
pub fn upsert_offset_max<K>(
    map: &papaya::HashMap<K, ConsumerOffset>,
    key: K,
    offset: u64,
    create_on_miss: impl FnOnce() -> ConsumerOffset,
) where
    K: Hash + Eq + Clone + Send + Sync,
{
    let guard = map.pin();
    if let Some(existing) = guard.get(&key) {
        existing.offset.fetch_max(offset, Ordering::Relaxed);
    } else {
        let created = create_on_miss();
        created.offset.store(offset, Ordering::Relaxed);
        guard.insert(key, created);
    }
}

/// Walk stamped `[256B SendMessages2Header][blob]` batches in one disk
/// chunk, pushing matching fragments. Returns bytes consumed: the start
/// of the first batch that did not fully fit in the chunk (the caller
/// re-reads from there), or the chunk end when everything decoded.
fn walk_disk_chunk(
    chunk: &Frozen<4096>,
    query: MessageLookup,
    count: u32,
    matched: &mut u32,
    fragments: &mut PollFragments<4096>,
    last_matching_offset: &mut Option<u64>,
) -> usize {
    let bytes: &[u8] = chunk;
    let mut cursor = 0usize;

    while *matched < count && cursor + COMMAND_HEADER_SIZE <= bytes.len() {
        let Ok(batch) = decode_batch_slice(&bytes[cursor..]) else {
            // Incomplete tail batch (or corrupt data): hand the position
            // back so the caller can re-read or bail.
            break;
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

    cursor.min(bytes.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use server_common::iobuf::Owned;

    fn non_empty_fragments() -> PollFragments<4096> {
        let mut fragments = PollFragments::new();
        fragments.push(crate::types::Fragment::whole(
            Owned::<4096>::zeroed(8).into(),
        ));
        fragments
    }

    fn consumer_auto_commit(offsets: Arc<ConsumerOffsets>, consumer_id: u32) -> AutoCommitCtx {
        AutoCommitCtx {
            target: AutoCommitTarget::Consumer {
                offsets,
                consumer_id,
                create_path: None,
            },
        }
    }

    #[test]
    fn resident_auto_commit_applies_in_memory_and_surfaces_offset() {
        // A resident auto_commit poll stays on the inline fast path (no detached
        // task since the poll no longer persists), applies the committed offset
        // to the in-memory map for read-your-own-poll, AND surfaces it so the
        // serving shard replicates it through consensus.
        let offsets = Arc::new(ConsumerOffsets::with_capacity(1));
        let plan = PollPlan {
            commit_offset: 42,
            auto_commit: Some(consumer_auto_commit(offsets.clone(), 7)),
            last_polled: None,
            tier: PollTier::Resident {
                fragments: non_empty_fragments(),
                last_matching_offset: Some(5),
            },
        };

        assert!(
            !plan.needs_off_pump_io(),
            "a resident auto_commit no longer persists on the poll path; the pump must not spawn",
        );

        let (fragments, commit_offset, applied) = plan.execute_resident();
        assert!(!fragments.is_empty(), "resident fragments must be returned");
        assert_eq!(commit_offset, 42, "commit offset is forwarded verbatim");

        let applied = applied.expect("auto_commit must surface the applied offset for replication");
        assert!(matches!(applied.kind, ConsumerKind::Consumer));
        assert_eq!(applied.consumer_id, 7);
        assert_eq!(applied.offset, 5);

        let stored = offsets
            .pin()
            .get(&7usize)
            .map(|entry| entry.offset.load(Ordering::Relaxed));
        assert_eq!(
            stored,
            Some(5),
            "the in-memory auto-commit must be applied on the resident path",
        );
    }

    #[test]
    fn empty_resident_poll_surfaces_no_auto_commit() {
        // Nothing served -> nothing to commit: no offset is surfaced and the
        // in-memory map stays untouched.
        let offsets = Arc::new(ConsumerOffsets::with_capacity(1));
        let plan = PollPlan {
            commit_offset: 9,
            auto_commit: Some(consumer_auto_commit(offsets.clone(), 7)),
            last_polled: None,
            tier: PollTier::Empty,
        };
        let (fragments, _commit_offset, applied) = plan.execute_resident();
        assert!(fragments.is_empty());
        assert!(
            applied.is_none(),
            "empty poll must not surface an auto-commit"
        );
        assert!(
            offsets.pin().get(&7usize).is_none(),
            "an empty poll must not touch the offset map",
        );
    }

    #[test]
    fn auto_commit_apply_is_monotone_but_explicit_store_rewinds() {
        // Auto-commit must never rewind a newer offset (anti-rewind via
        // fetch_max); an explicit StoreConsumerOffset may legitimately rewind.
        let offsets = Arc::new(ConsumerOffsets::with_capacity(1));
        let auto_commit = consumer_auto_commit(offsets.clone(), 7);

        auto_commit.apply(10);
        let after_high = offsets
            .pin()
            .get(&7usize)
            .map(|entry| entry.offset.load(Ordering::Relaxed));
        assert_eq!(after_high, Some(10));

        // A stale auto-commit with a smaller offset must not rewind.
        auto_commit.apply(4);
        let after_stale = offsets
            .pin()
            .get(&7usize)
            .map(|entry| entry.offset.load(Ordering::Relaxed));
        assert_eq!(after_stale, Some(10), "auto-commit fetch_max must hold");

        // The explicit pump path (store-semantics) still rewinds to 4.
        upsert_offset(&offsets, 7usize, 4, || {
            ConsumerOffset::new(ConsumerKind::Consumer, 7, 0, String::new())
        });
        let after_explicit = offsets
            .pin()
            .get(&7usize)
            .map(|entry| entry.offset.load(Ordering::Relaxed));
        assert_eq!(
            after_explicit,
            Some(4),
            "explicit store may rewind below the auto-committed offset",
        );
    }
}
