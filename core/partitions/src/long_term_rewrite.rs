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
//! it holds [`LONG_TERM_SEGMENT_BYTES`]. Its key names both its offsets, so
//! a segment another primary stored from the same offset, just before it
//! lost primacy, is another object when it ends elsewhere. The long-term log
//! lists the one whose command commits. One that ends at the same offset
//! holds the same bytes, and the rewrite takes it as its own.
//!
//! The long-term log holds the registration of a segment before its object
//! is stored. A plan reads the headers of the batches to find where the
//! segment ends, the primary commits the registration of those offsets, and
//! the rewrite that follows stores exactly them. The object of a primary
//! that lost primacy before its command committed is then deleted once its
//! registration expires, whatever the next primary stores.

use std::cell::Cell;
use std::future;
use std::io;
use std::rc::Rc;

use bytes::Bytes;
use compio::io::AsyncWriteAtExt;
use futures::FutureExt;
use futures::future::LocalBoxFuture;
use iggy_binary_protocol::batch::BATCH_HEADER_SIZE;
use iggy_binary_protocol::placement::PlacedBatch;
use iggy_common::IggyTimestamp;
use iggy_object_store::{
    KeyLayout, LongTermKey, ManifestSegment, ObjectPrefix, ObjectStore, SegmentObject,
    TierExecutor, TierJobCancelled, TierJobClass, TierJobRequest, TierScope, TierScopeToken,
};
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use tracing::{debug, warn};

use crate::IggyPartition;
use crate::Partition;
use crate::iggy_index::IGGY_INDEX_SIZE;
use crate::journal::MessageLookup;
use crate::long_term::{LONG_TERM_SEGMENTS_MAX, LongTermLog, ObjectRegistration, REGISTER_OBJECT};
use crate::placed_read::{BatchPiece, PlacedReadPlan};
use crate::poll_plan::DiskFault;
use crate::segment_chain::{HashSource, SegmentHasher, SegmentRecord};
use crate::segment_upload::{UploadError, memory_body, store_segment};
use crate::state_machine::CommandKind;
use crate::state_transfer::ChunkHasher;

/// Bytes of batches at which a rewrite ends its segment. The rewrite holds
/// the batches of one segment in memory while it writes them.
pub const LONG_TERM_SEGMENT_BYTES: usize = 64 * 1024 * 1024;

/// Bytes of a segment between two entries of its index.
pub const INDEX_STRIDE_BYTES: u64 = 64 * 1024;

/// One rewrite, as the partition's owner captured it.
pub struct SegmentRewrite {
    pub prefix: ObjectPrefix,
    /// The first offset of the segment: the long-term floor plus one.
    pub start_offset: u64,
    /// The last offset of the segment, where its plan found it ends.
    pub up_to: u64,
    pub(crate) read: PlacedReadPlan,
}

/// The plan of a rewrite, as the partition's owner captured it: where the
/// segment ends, found from the headers of its batches alone.
pub struct SegmentPlan {
    /// The first offset of the segment: the long-term floor plus one.
    pub start_offset: u64,
    /// The last offset the segment may hold: the commit point.
    pub up_to: u64,
    pub(crate) read: PlacedReadPlan,
}

/// The offsets of the segment a plan found, which name its object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlannedSegment {
    pub start_offset: u64,
    pub end_offset: u64,
}

/// What a rewrite pass over a partition starts.
pub enum RewriteStart {
    /// Run this plan off the pump, and hand its result to
    /// [`IggyPartition::finish_segment_plan`] with the number.
    Plan(u64, SegmentPlan),
    /// Commit this registration of the segment the rewrite stores. The pass
    /// after it commits starts the rewrite.
    Register(ObjectRegistration),
    /// Run this rewrite under the scope of the token, and hand its result to
    /// [`IggyPartition::finish_segment_rewrite`] with the number.
    Rewrite(u64, SegmentRewrite, TierScopeToken),
}

#[derive(Debug, thiserror::Error)]
pub enum RewriteError {
    /// A batch could not be read, from the disk or from the object store.
    #[error("cannot read the batches to rewrite: {0:?}")]
    Read(DiskFault),
    /// The batches read are not the run of offsets the segment needs.
    #[error("the batches read from offset {expected} continue at {found:?}")]
    Gap { expected: u64, found: Option<u64> },
    /// The batches end elsewhere than the registered segment does, so
    /// nothing is stored under offsets the log may never list.
    #[error("the batches end at offset {end_offset}, not where the registered segment ends")]
    Unregistered { end_offset: u64 },
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
        let placed = placed_batches(&self.read, self.start_offset, self.up_to).await?;
        let built = BuiltSegment::of(self.start_offset, &placed)?;
        if built.end_offset != self.up_to {
            return Err(RewriteError::Unregistered {
                end_offset: built.end_offset,
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
        let key = |object| {
            layout.long_term_key(&LongTermKey {
                prefix: self.prefix,
                start_offset: record.start_offset,
                end_offset: record.end_offset,
                object,
            })
        };
        store_segment(
            store,
            &record,
            key(SegmentObject::Log),
            &key(SegmentObject::Index),
            future::ready(Ok(memory_body(&log))),
            future::ready(Ok(memory_body(&index))),
            crc32c::crc32c(&index),
        )
        .await
        .map_err(RewriteError::Upload)?;
        Ok(ManifestSegment {
            start_offset: record.start_offset,
            end_offset: record.end_offset,
            start_timestamp: record.start_timestamp,
            end_timestamp: record.end_timestamp,
            max_timestamp: record.max_timestamp,
            size: record.size,
            crc32c: record.crc32c,
            hash: record.hash,
        })
    }
}

impl SegmentPlan {
    /// Read the headers of the batches from the long-term floor on, and
    /// find where the segment of the rewrite ends. The messages are not
    /// read.
    ///
    /// # Errors
    /// Returns an error when the batches cannot be read, or leave a gap.
    pub async fn run(self) -> Result<PlannedSegment, RewriteError> {
        let placed = placed_batches(&self.read, self.start_offset, self.up_to).await?;
        let built = BuiltSegment::of(self.start_offset, &placed)?;
        Ok(PlannedSegment {
            start_offset: self.start_offset,
            end_offset: built.end_offset,
        })
    }
}

/// The batches of a segment from `start_offset` on, up to `up_to` or
/// [`LONG_TERM_SEGMENT_BYTES`], whichever comes first. The same batches for
/// the plan and for the rewrite of the offsets it found.
async fn placed_batches(
    read: &PlacedReadPlan,
    start_offset: u64,
    up_to: u64,
) -> Result<Vec<PlacedBatch>, RewriteError> {
    let query = MessageLookup::Offset {
        offset: start_offset,
        count: u32::MAX,
        ceiling: up_to,
    };
    read.placed_batches(query, LONG_TERM_SEGMENT_BYTES)
        .await
        .map_err(RewriteError::Read)
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
        let mut index = SegmentIndex::default();
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
            index.push(
                header.base_offset,
                header.base_timestamp,
                header.batch_length,
            );
            let end_offset = header.base_offset + u64::from(header.message_count) - 1;
            let timestamp = header.base_timestamp;
            let size = index.position();
            next_offset = end_offset + 1;
            built = Some(built.map_or(
                Self {
                    end_offset,
                    start_timestamp: timestamp,
                    end_timestamp: timestamp,
                    max_timestamp: timestamp,
                    size,
                    index: Vec::new(),
                },
                |previous| Self {
                    end_offset,
                    end_timestamp: timestamp,
                    max_timestamp: previous.max_timestamp.max(timestamp),
                    size,
                    ..previous
                },
            ));
        }
        let index = index.into_bytes();
        debug_assert_eq!(index.len() % IGGY_INDEX_SIZE, 0);
        built
            .map(|built| Self { index, ..built })
            .ok_or(RewriteError::Gap {
                expected: start_offset,
                found: None,
            })
    }
}

/// The index of a long-term segment, built from its batches in the order
/// the segment holds them. It depends on nothing but the bytes of the
/// segment, so every writer of the same segment builds the same index.
#[derive(Default)]
pub struct SegmentIndex {
    /// The bytes of the segment before the next batch.
    position: u64,
    /// Where the batch of the last entry starts.
    indexed_at: Option<u64>,
    entries: Vec<u8>,
}

impl SegmentIndex {
    /// Take the next batch of the segment. The first batch gets an entry,
    /// and so does each batch that starts [`INDEX_STRIDE_BYTES`] or more
    /// after the batch of the entry before.
    pub fn push(&mut self, base_offset: u64, base_timestamp: u64, batch_length: u64) {
        if self
            .indexed_at
            .is_none_or(|indexed| self.position - indexed >= INDEX_STRIDE_BYTES)
        {
            self.indexed_at = Some(self.position);
            for field in [base_offset, base_timestamp, self.position] {
                self.entries.extend_from_slice(&field.to_le_bytes());
            }
        }
        self.position += batch_length;
    }

    /// The bytes of the batches taken so far.
    pub const fn position(&self) -> u64 {
        self.position
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.entries
    }
}

pub async fn write_file(path: &str, bytes: Vec<u8>) -> io::Result<()> {
    let mut file = compio::fs::File::create(path).await?;
    let compio::BufResult(written, _) = file.write_all_at(bytes, 0).await;
    written?;
    file.sync_all().await
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
    /// The segment this view's last plan found, registered at the time it
    /// carries. Its rewrite stores it once the log holds the registration.
    planned: Option<ObjectRegistration>,
    /// The commit point the last plan started at.
    committed_at_start: u64,
    /// Whether a registration committed on this primary since the last
    /// call to [`IggyPartition::take_long_term_registered`].
    registered: Cell<bool>,
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
    /// Take the next step of the rewrite of the committed batches above the
    /// long-term floor, when this replica is the primary of a partition that
    /// places its batches, none is in flight, and a batch is there to
    /// rewrite.
    ///
    /// The first step is a plan, which finds where the segment ends. Its
    /// registration is the next, and the caller commits it. The rewrite
    /// starts once the log holds it, and stores exactly the offsets the plan
    /// found. A log that takes no more registrations gets the rewrite without
    /// one, so the floor still moves.
    ///
    /// A replica that is not the primary cancels the rewrite it started, so
    /// a pass over a demoted replica is what stops it.
    pub fn begin_segment_rewrite(&mut self) -> Option<RewriteStart> {
        if !self.places_batches() || !self.state_machines_current() {
            *self.long_term_rewrites = LongTermRewrites::default();
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
        // A plan from below the floor named a segment that was published.
        rewrites.planned = rewrites
            .planned
            .filter(|planned| planned.start_offset == start_offset);
        let Some(planned) = rewrites.planned else {
            let (id, _token) = self.start_rewrite_job(Some(committed));
            let plan = SegmentPlan {
                start_offset,
                up_to: committed,
                read: self.placed_read_plan(None),
            };
            return Some(RewriteStart::Plan(id, plan));
        };
        let log = self.long_term_log()?;
        let stores = log
            .registration(planned.start_offset, planned.end_offset)
            .map_or_else(
                || log.registrations_full(),
                // A registration of an earlier view keeps the later time.
                |held| held.registered_at >= planned.registered_at,
            );
        if !stores {
            return Some(RewriteStart::Register(planned));
        }
        let (id, token) = self.start_rewrite_job(None);
        let rewrite = SegmentRewrite {
            prefix: crate::object_prefix(self.namespace(), self.created_revision()),
            start_offset,
            up_to: planned.end_offset,
            read: self.placed_read_plan(None),
        };
        Some(RewriteStart::Rewrite(id, rewrite, token))
    }

    /// Take the result of the plan numbered `id`, which ended at `now`, and
    /// return the registration of the segment it found, which the caller
    /// commits to the long-term log. A result of a plan this partition does
    /// not wait for, or one that arrives after primacy moved, changes
    /// nothing. A plan that failed is repeated by a later pass.
    pub fn finish_segment_plan(
        &mut self,
        id: u64,
        outcome: Result<PlannedSegment, RewriteError>,
        now: IggyTimestamp,
    ) -> Option<ObjectRegistration> {
        if !self.awaits_rewrite_job(id) {
            return None;
        }
        let next_start = self.long_term_log().map_or(0, LongTermLog::next_start);
        let namespace_raw = self.namespace().inner();
        let rewrites = &mut self.long_term_rewrites;
        rewrites.running = None;
        match outcome {
            Ok(planned) if planned.start_offset == next_start => {
                let registration = ObjectRegistration {
                    start_offset: planned.start_offset,
                    end_offset: planned.end_offset,
                    registered_at: now.as_micros(),
                };
                rewrites.planned = Some(registration);
                Some(registration)
            }
            Ok(_) => None,
            Err(error) => {
                warn!(
                    namespace_raw,
                    %error,
                    "finding where the partition's next long-term segment ends failed and is tried again"
                );
                None
            }
        }
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
        if !self.awaits_rewrite_job(id) {
            return None;
        }
        let namespace_raw = self.namespace().inner();
        let rewrites = &mut self.long_term_rewrites;
        rewrites.running = None;
        match outcome {
            Ok(Ok(segment)) => {
                rewrites.stored = Some(segment);
                debug!(
                    namespace_raw,
                    start_offset = segment.start_offset,
                    end_offset = segment.end_offset,
                    size = segment.size,
                    "stored a long-term segment of the partition"
                );
                Some(segment)
            }
            Ok(Err(error)) => {
                // The next pass plans again rather than repeat a rewrite
                // that cannot store what was registered.
                if matches!(error, RewriteError::Unregistered { .. }) {
                    rewrites.planned = None;
                }
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

    /// Whether a registration committed on this primary since the last
    /// call. The rewrite or the merge that waited for it is due.
    pub fn take_long_term_registered(&self) -> bool {
        self.long_term_rewrites.registered.take()
    }

    /// A command of `kind` changed the long-term log. A registration leaves
    /// the manifest as it is, so only this says that it committed.
    pub(crate) fn note_long_term_registration(&self, kind: CommandKind) {
        if kind == REGISTER_OBJECT && self.consensus().is_primary() {
            self.long_term_rewrites.registered.set(true);
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

    /// Whether a plan or a rewrite this partition started has yet to hand
    /// its result back.
    #[must_use]
    pub fn segment_rewrite_running(&self) -> bool {
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
            *self.long_term_rewrites = LongTermRewrites {
                view: Some(view),
                ..LongTermRewrites::default()
            };
        }
        &mut self.long_term_rewrites
    }

    /// Number the next plan or rewrite and take it as in flight. A plan
    /// also notes the commit point it starts at. The numbers go on across
    /// views, so the late result of an earlier view's plan or rewrite is
    /// never taken for the one in flight.
    fn start_rewrite_job(&mut self, committed: Option<u64>) -> (u64, TierScopeToken) {
        let scope = TierScope::new();
        let token = scope.token();
        self.long_term_rewrite_jobs += 1;
        let id = self.long_term_rewrite_jobs;
        let rewrites = &mut self.long_term_rewrites;
        rewrites.running = Some((id, scope));
        if let Some(committed) = committed {
            rewrites.committed_at_start = committed;
        }
        (id, token)
    }

    /// Whether the plan or the rewrite numbered `id` is the one in flight,
    /// in the view that started it. A result that arrives after primacy
    /// moved drops what the view kept.
    fn awaits_rewrite_job(&mut self, id: u64) -> bool {
        let awaited = self
            .long_term_rewrites
            .running
            .as_ref()
            .is_some_and(|(running, _)| *running == id);
        if !awaited {
            return false;
        }
        if !self.state_machines_current()
            || self.long_term_rewrites.view != Some(self.consensus().view())
        {
            *self.long_term_rewrites = LongTermRewrites::default();
            return false;
        }
        true
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
        PlacedBatch::new(header, Placement::of(1, 0, blob, 0))
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
