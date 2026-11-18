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

//! The rewrite of a placing partition's committed batches into a long-term
//! segment, by its primary.
//!
//! A rewrite reads the batches above the long-term floor the way a poll
//! does, with the messages their placements name. It puts them back to back
//! as a `.log`, with an `.index` beside it, in the layout of a tiered
//! segment, and stores both from memory under the partition's own prefix. A
//! reader of the bucket then needs no placement and no fresh object to read
//! the partition.
//!
//! The segment ends where the rewrite stops: at the commit point, or once
//! it holds [`LONG_TERM_SEGMENT_BYTES`]. Another primary can have stored a
//! segment from the same offset that ends elsewhere, just before it lost
//! primacy. The store refuses to replace it, and the rewrite then reports
//! where that one ends, so the next attempt ends there too and writes the
//! same bytes.

use std::future;
use std::rc::Rc;

use bytes::Bytes;
use futures::FutureExt;
use futures::future::LocalBoxFuture;
use iggy_binary_protocol::batch::BATCH_HEADER_SIZE;
use iggy_binary_protocol::placement::PlacedBatch;
use iggy_object_store::{
    KeyLayout, ManifestSegment, ObjectPrefix, ObjectStore, SegmentKey, SegmentMetadata,
    SegmentObject, TierExecutor, TierJobCancelled, TierJobClass, TierJobRequest, TierScope,
    TierScopeToken,
};
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use tracing::{debug, warn};

use crate::IggyPartition;
use crate::Partition;
use crate::iggy_index::IGGY_INDEX_SIZE;
use crate::journal::MessageLookup;
use crate::long_term::LONG_TERM_SEGMENTS_MAX;
use crate::placed_read::{BatchPiece, PlacedReadPlan};
use crate::poll_plan::DiskFault;
use crate::segment_chain::{HashSource, SegmentHasher, SegmentRecord};
use crate::segment_upload::{UploadError, memory_body, store_segment};
use crate::state_transfer::ChunkHasher;

/// Bytes of batches at which a rewrite ends its segment. The rewrite holds
/// the batches of one segment in memory while it writes them.
pub const LONG_TERM_SEGMENT_BYTES: usize = 64 * 1024 * 1024;

/// Bytes of a segment between two entries of its index.
const INDEX_STRIDE_BYTES: u64 = 64 * 1024;

/// One rewrite, as the partition's owner captured it.
pub struct SegmentRewrite {
    pub prefix: ObjectPrefix,
    /// The first offset of the segment: the long-term floor plus one.
    pub start_offset: u64,
    /// The last offset the segment may hold. The commit point, or where a
    /// segment another primary stored from `start_offset` ends.
    pub up_to: u64,
    /// Whether the segment must end exactly at `up_to`.
    pub ends_at_up_to: bool,
    pub(crate) read: PlacedReadPlan,
}

#[derive(Debug, thiserror::Error)]
pub enum RewriteError {
    /// A batch could not be read, from the disk or from the object store.
    #[error("cannot read the batches to rewrite: {0:?}")]
    Read(DiskFault),
    /// The batches read are not the run of offsets the segment needs.
    #[error("the batches read from offset {expected} continue at {found:?}")]
    Gap { expected: u64, found: Option<u64> },
    /// The store holds a segment from the same offset that ends at
    /// `end_offset`, which another primary stored.
    #[error("the store holds a segment from the same offset that ends at {end_offset}")]
    EndsElsewhere { end_offset: u64 },
    #[error(transparent)]
    Upload(UploadError),
}

/// What a rewrite ended as: the segment it stored, or why it stored none,
/// or a cancellation before it finished.
pub type RewriteOutcome = Result<Result<ManifestSegment, RewriteError>, TierJobCancelled>;

/// Runs the rewrites of a shard, behind a pointer, so the shard does not
/// carry the executor's type.
pub trait LongTermWriter {
    /// Run `rewrite` under `scope`. The rewrite stops when the scope is
    /// cancelled, as when its partition loses primacy.
    fn write(
        &self,
        rewrite: SegmentRewrite,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, RewriteOutcome>;
}

/// Rewrites through a tier executor, in its upload class.
pub struct ExecutorLongTermWriter<E> {
    executor: E,
    layout: KeyLayout,
}

impl<E> ExecutorLongTermWriter<E> {
    #[must_use]
    pub const fn new(executor: E, layout: KeyLayout) -> Self {
        Self { executor, layout }
    }

    /// The executor the rewrites go through.
    #[must_use]
    pub const fn executor(&self) -> &E {
        &self.executor
    }
}

impl<E: TierExecutor> LongTermWriter for ExecutorLongTermWriter<E> {
    fn write(
        &self,
        rewrite: SegmentRewrite,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, RewriteOutcome> {
        let layout = self.layout.clone();
        let request = TierJobRequest::new(TierJobClass::Upload, scope)
            .with_bytes(LONG_TERM_SEGMENT_BYTES as u64);
        self.executor
            .submit(request, move |store: Rc<E::Store>| async move {
                rewrite.run(&*store, &layout).await
            })
            .boxed_local()
    }
}

impl SegmentRewrite {
    /// Read the batches into the segment's bytes, summing them as they are
    /// copied, and store the segment and its index from memory.
    ///
    /// Nothing is written to the disk. A stored segment whose command is
    /// lost is found again in the store by the rewrite that repeats it, so
    /// a local copy would guard nothing.
    async fn run<S: ObjectStore>(
        self,
        store: &S,
        layout: &KeyLayout,
    ) -> Result<ManifestSegment, RewriteError> {
        let query = MessageLookup::Offset {
            offset: self.start_offset,
            count: u32::MAX,
            ceiling: self.up_to,
        };
        let placed = self
            .read
            .placed_batches(query, LONG_TERM_SEGMENT_BYTES)
            .await
            .map_err(RewriteError::Read)?;
        let built = BuiltSegment::of(self.start_offset, &placed)?;
        if self.ends_at_up_to && built.end_offset != self.up_to {
            return Err(RewriteError::Gap {
                expected: self.up_to.saturating_add(1),
                found: Some(built.end_offset.saturating_add(1)),
            });
        }
        let mut log = SegmentBytes::<SegmentHasher>::sized(built.size)?;
        self.read
            .read_blobs(&placed, |piece| log.push(piece))
            .await
            .map_err(RewriteError::Read)?;
        drop(placed);
        if log.bytes.len() as u64 != built.size {
            return Err(RewriteError::Read(DiskFault::Unreadable));
        }
        let hashes = log.hasher.finalize();
        let record = SegmentRecord {
            start_offset: self.start_offset,
            end_offset: built.end_offset,
            start_timestamp: built.start_timestamp,
            end_timestamp: built.end_timestamp,
            max_timestamp: built.max_timestamp,
            size: built.size,
            incarnation: 0,
            hash: hashes.blake3,
            crc32c: hashes.crc32c,
            source: HashSource::CommitPath,
        };
        let log = Bytes::from(log.bytes);
        let index = Bytes::from(built.index);
        let stored = store_segment(
            store,
            layout,
            self.prefix,
            &record,
            future::ready(Ok(memory_body(&log))),
            future::ready(Ok(memory_body(&index))),
            crc32c::crc32c(&index),
        )
        .await;
        match stored {
            Ok(()) => Ok(ManifestSegment {
                start_offset: record.start_offset,
                end_offset: record.end_offset,
                start_timestamp: record.start_timestamp,
                end_timestamp: record.end_timestamp,
                max_timestamp: record.max_timestamp,
                size: record.size,
                crc32c: record.crc32c,
                hash: record.hash,
            }),
            Err(UploadError::Diverged { key }) => {
                Err(self.ends_elsewhere(store, layout, key).await)
            }
            Err(error) => Err(RewriteError::Upload(error)),
        }
    }

    /// Why the store refused the segment: where the one it holds ends, when
    /// its metadata says so, and the refusal itself otherwise.
    async fn ends_elsewhere<S: ObjectStore>(
        &self,
        store: &S,
        layout: &KeyLayout,
        key: String,
    ) -> RewriteError {
        let log_key = layout.segment_key(&SegmentKey {
            prefix: self.prefix,
            start_offset: self.start_offset,
            object: SegmentObject::Log,
        });
        let stored = store
            .head(&log_key)
            .await
            .ok()
            .and_then(|meta| SegmentMetadata::try_from(&meta.metadata).ok())
            .filter(|stored| stored.start_offset == self.start_offset);
        stored.map_or(
            RewriteError::Upload(UploadError::Diverged { key }),
            |stored| RewriteError::EndsElsewhere {
                end_offset: stored.end_offset,
            },
        )
    }
}

/// What a run of whole batches makes as one segment.
struct BuiltSegment {
    end_offset: u64,
    start_timestamp: u64,
    end_timestamp: u64,
    max_timestamp: u64,
    size: u64,
    index: Vec<u8>,
}

impl BuiltSegment {
    /// The segment of `batches`, which must hold every offset from
    /// `start_offset` on without a gap.
    fn of(start_offset: u64, batches: &[PlacedBatch]) -> Result<Self, RewriteError> {
        let mut next_offset = start_offset;
        let mut built: Option<Self> = None;
        let mut position = 0u64;
        let mut indexed_at: Option<u64> = None;
        let mut index = Vec::new();
        for batch in batches {
            let header = &batch.header;
            // The bytes of the segment are sized by the headers and filled
            // by the placements, so the two have to agree.
            if header.batch_length != BATCH_HEADER_SIZE as u64 + batch.placement.length {
                return Err(RewriteError::Read(DiskFault::Unreadable));
            }
            if header.base_offset != next_offset || header.message_count == 0 {
                return Err(RewriteError::Gap {
                    expected: next_offset,
                    found: Some(header.base_offset),
                });
            }
            if indexed_at.is_none_or(|indexed| position - indexed >= INDEX_STRIDE_BYTES) {
                indexed_at = Some(position);
                for field in [header.base_offset, header.base_timestamp, position] {
                    index.extend_from_slice(&field.to_le_bytes());
                }
            }
            let end_offset = header.base_offset + u64::from(header.message_count) - 1;
            let timestamp = header.base_timestamp;
            position += header.batch_length;
            next_offset = end_offset + 1;
            built = Some(built.map_or(
                Self {
                    end_offset,
                    start_timestamp: timestamp,
                    end_timestamp: timestamp,
                    max_timestamp: timestamp,
                    size: position,
                    index: Vec::new(),
                },
                |previous| Self {
                    end_offset,
                    end_timestamp: timestamp,
                    max_timestamp: previous.max_timestamp.max(timestamp),
                    size: position,
                    ..previous
                },
            ));
        }
        debug_assert_eq!(index.len() % IGGY_INDEX_SIZE, 0);
        built
            .map(|built| Self { index, ..built })
            .ok_or(RewriteError::Gap {
                expected: start_offset,
                found: None,
            })
    }
}

/// The bytes of a segment as its batches are put back together, and the
/// sums of those bytes, taken from each piece as it is copied in.
struct SegmentBytes<H> {
    bytes: Vec<u8>,
    hasher: H,
}

impl<H: ChunkHasher + Default> SegmentBytes<H> {
    /// An empty segment with room for `size` bytes, so that no batch moves
    /// the ones before it.
    fn sized(size: u64) -> Result<Self, RewriteError> {
        let size = usize::try_from(size).map_err(|_| RewriteError::Read(DiskFault::Unreadable))?;
        Ok(Self {
            bytes: Vec::with_capacity(size),
            hasher: H::default(),
        })
    }

    /// Append `piece` of the next batch.
    fn push(&mut self, piece: BatchPiece<'_>) {
        let start = self.bytes.len();
        match piece {
            BatchPiece::Header(header) => {
                self.bytes.resize(start + BATCH_HEADER_SIZE, 0);
                header.encode_into(&mut self.bytes[start..]);
            }
            BatchPiece::Messages(messages) => self.bytes.extend_from_slice(messages),
        }
        self.hasher.update(&self.bytes[start..]);
    }
}

/// What a replica keeps of its rewrites while it is the primary of a
/// partition that places its batches. Nothing here is persisted.
#[derive(Default)]
pub struct LongTermRewrites {
    /// The view this state belongs to, `None` until the replica acts as the
    /// primary.
    view: Option<u32>,
    /// The rewrite in flight: its number and the scope whose end stops it.
    running: Option<(u64, TierScope)>,
    /// A segment the store holds that the long-term log does not yet. Its
    /// command is sent again until the log ends with it.
    stored: Option<ManifestSegment>,
    /// Where the segment from the floor on must end: where the one another
    /// primary stored ends.
    ends_at: Option<u64>,
    /// The commit point the last rewrite started at.
    committed_at_start: u64,
    /// Number of the last rewrite this partition started.
    jobs: u64,
}

/// Bytes of the batches a partition committed above its long-term floor,
/// as their headers state them: what its rewrites have yet to store.
///
/// A walk of the log above the floor counts them once. From then on the
/// commits add to the count and the floor advances take from it, on a
/// backup as on the primary, so a new primary has the count already.
#[derive(Debug, Default)]
pub struct LongTermBacklog {
    /// `None` until a walk counted the backlog.
    bytes: Option<u64>,
    /// What commits added and floor advances took since the running walk
    /// took its snapshot.
    walk: Option<BacklogChanges>,
    /// Number of the last walk, so the count of one started before a reset
    /// is dropped.
    walks: u64,
}

#[derive(Debug, Default, Clone, Copy)]
struct BacklogChanges {
    added: u64,
    taken: u64,
}

impl LongTermBacklog {
    /// A batch of `bytes` committed.
    pub(crate) const fn add(&mut self, bytes: u64) {
        if let Some(backlog) = &mut self.bytes {
            *backlog = backlog.saturating_add(bytes);
        } else if let Some(walk) = &mut self.walk {
            walk.added = walk.added.saturating_add(bytes);
        }
    }

    /// The floor moved past a segment of `bytes`.
    const fn take(&mut self, bytes: u64) {
        if let Some(backlog) = &mut self.bytes {
            *backlog = backlog.saturating_sub(bytes);
        } else if let Some(walk) = &mut self.walk {
            walk.taken = walk.taken.saturating_add(bytes);
        }
    }

    /// Drop the count and the walk that runs, so the next walk counts again.
    pub(crate) const fn forget(&mut self) {
        self.bytes = None;
        self.walk = None;
    }
}

/// A count of the bytes of a partition's committed batches above its
/// long-term floor, as the partition's owner captured it.
pub struct BacklogWalk {
    from: u64,
    up_to: u64,
    read: PlacedReadPlan,
}

impl BacklogWalk {
    /// Read the headers of the batches and add up their lengths. `None` when
    /// the log could not be read.
    pub async fn run(self) -> Option<u64> {
        self.read
            .batch_bytes(self.from, self.up_to)
            .await
            .inspect_err(|fault| {
                debug!(
                    from = self.from,
                    up_to = self.up_to,
                    ?fault,
                    "counting the long-term backlog failed and is tried again"
                );
            })
            .ok()
    }
}

impl<B, SB> IggyPartition<B, SB>
where
    B: MessageBus,
    SB: SuperblockStore,
{
    /// Start the rewrite of the committed batches above the long-term
    /// floor, when this replica is the primary of a partition that places
    /// its batches, none is in flight, and a batch is there to rewrite. The
    /// caller runs it under the returned scope and hands the result to
    /// [`Self::finish_segment_rewrite`] with the returned number.
    ///
    /// A replica that is not the primary cancels the rewrite it started, so
    /// a pass over a demoted replica is what stops it.
    pub fn begin_segment_rewrite(&mut self) -> Option<(u64, SegmentRewrite, TierScopeToken)> {
        if !self.places_batches() || !self.state_machines_current() {
            self.long_term_rewrites = LongTermRewrites::default();
            return None;
        }
        // The log refuses a segment past this, and a stored segment whose
        // command is refused holds back every later rewrite.
        if self.long_term_segments().len() >= LONG_TERM_SEGMENTS_MAX {
            return None;
        }
        let floor = self.long_term_floor();
        let offsets = self.offsets();
        let committed = self
            .holds_committed_messages()
            .then_some(offsets.commit_offset);
        let rewrites = self.long_term_rewrites_of_this_view();
        if rewrites
            .stored
            .is_some_and(|stored| floor >= Some(stored.end_offset))
        {
            rewrites.stored = None;
        }
        if rewrites.running.is_some() || rewrites.stored.is_some() {
            return None;
        }
        let start_offset = floor.map_or(0, |floor| floor.saturating_add(1));
        let committed = committed.filter(|committed| *committed >= start_offset)?;
        let (up_to, ends_at_up_to) = match rewrites.ends_at {
            // The other primary rewrote batches this replica has yet to
            // commit, which it will.
            Some(end) if end > committed => return None,
            Some(end) if end >= start_offset => (end, true),
            // The log moved past that segment since.
            Some(_) | None => (committed, false),
        };
        let scope = TierScope::new();
        let token = scope.token();
        rewrites.jobs += 1;
        let id = rewrites.jobs;
        rewrites.running = Some((id, scope));
        rewrites.committed_at_start = committed;
        let rewrite = SegmentRewrite {
            prefix: crate::object_prefix(self.namespace(), self.created_revision()),
            start_offset,
            up_to,
            ends_at_up_to,
            read: self.placed_read_plan(None),
        };
        Some((id, rewrite, token))
    }

    /// Take the result of the rewrite numbered `id`, and return the segment
    /// it stored, which the caller commits to the long-term log. A result of
    /// a rewrite this partition does not wait for, or one that arrives after
    /// primacy moved, changes nothing. A rewrite that failed is repeated by
    /// a later pass.
    pub fn finish_segment_rewrite(
        &mut self,
        id: u64,
        outcome: RewriteOutcome,
    ) -> Option<ManifestSegment> {
        let awaited = self
            .long_term_rewrites
            .running
            .as_ref()
            .is_some_and(|(running, _)| *running == id);
        if !awaited {
            return None;
        }
        if !self.state_machines_current()
            || self.long_term_rewrites.view != Some(self.consensus().view())
        {
            self.long_term_rewrites = LongTermRewrites::default();
            return None;
        }
        let namespace_raw = self.namespace().inner();
        let rewrites = &mut self.long_term_rewrites;
        rewrites.running = None;
        match outcome {
            Ok(Ok(segment)) => {
                rewrites.stored = Some(segment);
                rewrites.ends_at = None;
                debug!(
                    namespace_raw,
                    start_offset = segment.start_offset,
                    end_offset = segment.end_offset,
                    size = segment.size,
                    "stored a long-term segment of the partition"
                );
                Some(segment)
            }
            Ok(Err(RewriteError::EndsElsewhere { end_offset })) => {
                rewrites.ends_at = Some(end_offset);
                None
            }
            Ok(Err(error)) => {
                warn!(
                    namespace_raw,
                    %error,
                    "rewriting the partition's batches into a long-term segment failed and is tried again"
                );
                None
            }
            Err(TierJobCancelled) => None,
        }
    }

    /// The segment the store holds that the long-term log does not end with
    /// yet, whose command the primary sends until it does.
    #[must_use]
    pub fn unpublished_segment(&self) -> Option<ManifestSegment> {
        let rewrites = &self.long_term_rewrites;
        rewrites.stored.filter(|stored| {
            rewrites.view == Some(self.consensus().view())
                && self.state_machines_current()
                && self.long_term_floor() < Some(stored.end_offset)
        })
    }

    /// Whether the segment the last rewrite stored holds every message that
    /// was committed when the rewrite started. A segment that ended earlier
    /// has more behind it.
    #[must_use]
    pub fn stored_segment_reached_commit(&self) -> bool {
        let rewrites = &self.long_term_rewrites;
        rewrites
            .stored
            .is_none_or(|stored| stored.end_offset >= rewrites.committed_at_start)
    }

    /// Whether a rewrite this partition started has yet to hand its result
    /// back.
    #[must_use]
    pub const fn segment_rewrite_running(&self) -> bool {
        self.long_term_rewrites.running.is_some()
    }

    /// Messages this partition committed that no long-term segment holds.
    /// Zero on a replica that is not the primary of a partition that places
    /// its batches: the primary is the one that rewrites them.
    #[must_use]
    pub fn long_term_lag(&self) -> u64 {
        if !self.places_batches()
            || !self.consensus().is_primary()
            || !self.holds_committed_messages()
        {
            return 0;
        }
        let committed = self.offsets().commit_offset.saturating_add(1);
        let rewritten = self
            .long_term_floor()
            .map_or(0, |floor| floor.saturating_add(1));
        committed.saturating_sub(rewritten)
    }

    /// Bytes of the batches this partition committed above its long-term
    /// floor, `None` until a walk counted them.
    #[must_use]
    pub fn long_term_backlog(&self) -> Option<u64> {
        self.long_term_backlog.bytes
    }

    /// Start counting [`Self::long_term_backlog`] on a partition that places
    /// its batches and has no count: a walk of the committed batches above
    /// the floor, which the caller runs off the pump and hands back to
    /// [`Self::finish_backlog_walk`] with the returned number. What commits
    /// and floor advances change meanwhile is added to the walk's count.
    pub fn begin_backlog_walk(&mut self) -> Option<(u64, BacklogWalk)> {
        let backlog = &self.long_term_backlog;
        if !self.places_batches() || backlog.bytes.is_some() || backlog.walk.is_some() {
            return None;
        }
        let from = self
            .long_term_floor()
            .map_or(0, |floor| floor.saturating_add(1));
        let committed = self
            .holds_committed_messages()
            .then_some(self.offsets().commit_offset)
            .filter(|committed| *committed >= from);
        let Some(up_to) = committed else {
            self.long_term_backlog.bytes = Some(0);
            return None;
        };
        let read = self.placed_read_plan(None);
        let backlog = &mut self.long_term_backlog;
        backlog.walks += 1;
        backlog.walk = Some(BacklogChanges::default());
        Some((backlog.walks, BacklogWalk { from, up_to, read }))
    }

    /// Take the count of the walk numbered `id`: `None` for a walk that could
    /// not read the log, which a later pass starts again. The count of a
    /// walk this partition does not wait for changes nothing.
    pub fn finish_backlog_walk(&mut self, id: u64, counted: Option<u64>) {
        let backlog = &mut self.long_term_backlog;
        if id != backlog.walks {
            return;
        }
        let Some(changes) = backlog.walk.take() else {
            return;
        };
        backlog.bytes = counted.map(|counted| {
            counted
                .saturating_add(changes.added)
                .saturating_sub(changes.taken)
        });
    }

    /// A command applied to the long-term log, which ended at `floor`
    /// before it. When the floor moved, the bytes of the segment it moved
    /// past leave the backlog.
    pub(crate) fn note_long_term_advance(&mut self, floor: Option<u64>) {
        if self.long_term_floor() <= floor {
            return;
        }
        if let Some(size) = self.long_term_segments().last().map(|segment| segment.size) {
            self.long_term_backlog.take(size);
        }
    }

    /// The rewrite state of the current view. What an earlier view left is
    /// dropped, and its rewrite cancelled.
    fn long_term_rewrites_of_this_view(&mut self) -> &mut LongTermRewrites {
        let view = self.consensus().view();
        if self.long_term_rewrites.view != Some(view) {
            self.long_term_rewrites = LongTermRewrites {
                view: Some(view),
                ..LongTermRewrites::default()
            };
        }
        &mut self.long_term_rewrites
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_binary_protocol::batch::BatchHeader;
    use iggy_binary_protocol::placement::Placement;

    /// Counts the bytes it is fed.
    #[derive(Default)]
    struct FedBytes(usize);

    impl ChunkHasher for FedBytes {
        fn update(&mut self, bytes: &[u8]) {
            self.0 += bytes.len();
        }
    }

    fn batch(base_offset: u64, blob: &[u8]) -> PlacedBatch {
        let mut header = BatchHeader::new(0, 0, (BATCH_HEADER_SIZE + blob.len()) as u64, 1);
        header.base_offset = base_offset;
        PlacedBatch::new(header, Placement::of(1, 0, blob))
    }

    #[test]
    fn given_batches_when_putting_a_segment_together_should_sum_each_byte_once_and_move_none() {
        let blobs = [vec![1_u8; 700], vec![2; 3], vec![3; 4096]];
        let placed: Vec<_> = (0..)
            .zip(&blobs)
            .map(|(at, blob)| batch(at, blob))
            .collect();
        let built = BuiltSegment::of(0, &placed).unwrap();
        let mut log = SegmentBytes::<FedBytes>::sized(built.size).unwrap();
        let (start, capacity) = (log.bytes.as_ptr(), log.bytes.capacity());

        for (placed, blob) in placed.iter().zip(&blobs) {
            log.push(BatchPiece::Header(&placed.header));
            for messages in blob.chunks(500) {
                log.push(BatchPiece::Messages(messages));
            }
        }

        assert_eq!(log.bytes.len() as u64, built.size);
        assert_eq!(log.hasher.0, log.bytes.len());
        assert_eq!(
            (log.bytes.as_ptr(), log.bytes.capacity()),
            (start, capacity)
        );
        let mut expected = Vec::new();
        for (placed, blob) in placed.iter().zip(&blobs) {
            let mut header = [0; BATCH_HEADER_SIZE];
            placed.header.encode_into(&mut header);
            expected.extend_from_slice(&header);
            expected.extend_from_slice(blob);
        }
        assert_eq!(log.bytes, expected);
    }

    #[test]
    fn given_a_placement_of_another_length_than_its_header_when_building_should_refuse_it() {
        let mut placed = batch(0, &[7; 100]);
        placed.placement.length += 1;

        assert!(matches!(
            BuiltSegment::of(0, &[placed]),
            Err(RewriteError::Read(DiskFault::Unreadable))
        ));
    }
}
