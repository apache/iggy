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

//! The segment chain of a tiered partition whose `remote.chain` file is gone.
//!
//! An evicted segment is left only in the store and in the chain, so without
//! the chain a replica cannot tell an evicted front from one that retention
//! removed. The store may extend the local log backwards to meet it, and
//! never says where the log ends: a partition without a local `.log` is never
//! rebuilt from it.
//!
//! [`chain_loss`] decides from local facts alone whether anything can be
//! missing. [`rebuild_front`] walks what the store holds from the first offset
//! retention kept up to the local front, and returns the records of a chain
//! that describes it, or the first range it cannot account for. Neither does
//! any I/O. [`ExecutorStoreViewReader`] reads what the walk needs.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::rc::Rc;

use bytes::Bytes;
use futures::future::LocalBoxFuture;
use futures::{FutureExt, StreamExt, stream};
use iggy_object_store::{
    KeyLayout, ManifestError, ObjectPrefix, ObjectStore, ObjectStoreError, PartitionManifest,
    SegmentKey, SegmentMetadata, SegmentMetadataError, SegmentObject, TierExecutor,
    TierJobCancelled, TierJobClass, TierJobRequest, TierScopeToken,
};

use crate::iggy_index::IGGY_INDEX_SIZE;
use crate::object_delete::{ListedSegment, list_incarnation};
use crate::segment_anchor::SegmentAnchor;
use crate::segment_chain::{ChainRecord, HashSource, SegmentRecord};
use crate::tiered_floor::AdvanceTieredFloor;

#[cfg(test)]
mod tests;

/// The most bytes of the manifest a rebuild reads. A larger manifest is not
/// decoded, and the rebuild goes on without the cross-check.
pub const MANIFEST_CROSS_CHECK_BYTES_MAX: u64 = 16 * 1024 * 1024;

/// HEADs of `.log` objects one rebuild has in flight at once.
const STORE_VIEW_HEADS: usize = 16;

/// What a partition whose chain is gone knows of itself.
#[derive(Debug, Clone, Copy)]
pub struct LocalFront<'a> {
    /// The first offset of the first `.log` on disk, `None` without one.
    pub front: Option<u64>,
    /// The truncation watermark metadata committed: the last offset
    /// retention removed, 0 while it removed nothing.
    pub watermark: u64,
    /// The tiered floor, `None` before the first advance.
    pub floor: Option<u64>,
    /// The start offsets of the `.index` and `.anchor` files below the front
    /// that have no `.log`, ascending.
    pub stems_without_log: &'a [u64],
}

impl LocalFront<'_> {
    /// The first offset retention kept.
    #[must_use]
    pub const fn first_kept(&self) -> u64 {
        first_kept(self.watermark)
    }
}

/// The first offset retention kept under the truncation `watermark`.
const fn first_kept(watermark: u64) -> u64 {
    if watermark == 0 {
        0
    } else {
        watermark.saturating_add(1)
    }
}

/// What a partition whose chain is gone does about it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChainLoss {
    /// Nothing can be missing: tiering creates a fresh chain, as on a first
    /// enable.
    Fresh,
    /// Something below the local front may have left the disk. The store
    /// says what.
    Rebuild,
    /// A range below the local front is not accounted for.
    Refuse(ChainLost),
}

/// A range below the local front that a partition without its chain cannot
/// account for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChainLost {
    pub from: u64,
    pub to: u64,
    pub reason: ChainLostReason,
}

/// Why a range below the local front is not accounted for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChainLostReason {
    /// No `.log` is left on disk.
    NoLocalLog,
    /// The store holds no object that covers the range, and no anchor on
    /// disk makes it a gap.
    MissingObject,
    /// The object of the segment starting at `start` has no `.index`.
    MissingIndex { start: u64 },
    /// The `.index` of the segment starting at `start` is empty or not a
    /// whole number of entries.
    IndexSize { start: u64, size: u64 },
    /// The metadata of the `.log` object starting at `start` does not decode.
    UnreadableMetadata { start: u64 },
    /// The `.log` object keyed `start` records another first offset.
    MisplacedObject { start: u64, recorded: u64 },
    /// The `.log` object starting at `start` is not the size its metadata
    /// records.
    SizeMismatch {
        start: u64,
        listed: u64,
        recorded: u64,
    },
    /// The `.log` object starting at `start` holds no bytes.
    EmptyObject { start: u64 },
    /// The object starting at `start` reaches the segment that the WAL
    /// checkpoint names, which recovery keeps on disk.
    AtCheckpoint { start: u64, checkpoint_start: u64 },
    /// The range reaches past the tiered floor, so no advance confirmed it.
    AboveFloor { floor: Option<u64> },
    /// The object starting at `start` reaches the local front.
    OverlapsFront { start: u64, end: u64 },
    /// Neither the store nor the manifest gives the CRC32C of the object
    /// starting at `start`.
    NoChecksum { start: u64 },
    /// The manifest records the segment starting at `start` differently.
    ManifestDisagrees { start: u64 },
    /// The manifest lists a segment starting at `start` that the walk passed.
    ManifestSkipped { start: u64 },
    /// The manifest names another partition or incarnation.
    ForeignManifest,
    /// An `.index` or `.anchor` starting at `start` stays on disk for a
    /// segment the walk did not find.
    StrayFile { start: u64 },
}

impl fmt::Display for ChainLostReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoLocalLog => f.write_str("no segment log is left on disk"),
            Self::MissingObject => {
                f.write_str("the store holds no segment for them, and no anchor makes them a gap")
            }
            Self::MissingIndex { start } => {
                write!(f, "the segment {start} in the store has no index")
            }
            Self::IndexSize { start, size } => write!(
                f,
                "the index of the segment {start} in the store holds {size} bytes, not one or \
                 more whole entries"
            ),
            Self::UnreadableMetadata { start } => {
                write!(
                    f,
                    "the metadata of the segment {start} in the store does not decode"
                )
            }
            Self::MisplacedObject { start, recorded } => write!(
                f,
                "the segment keyed {start} in the store records {recorded} as its first offset"
            ),
            Self::SizeMismatch {
                start,
                listed,
                recorded,
            } => write!(
                f,
                "the segment {start} in the store holds {listed} bytes, and its metadata records \
                 {recorded}"
            ),
            Self::EmptyObject { start } => {
                write!(f, "the segment {start} in the store holds no bytes")
            }
            Self::AtCheckpoint {
                start,
                checkpoint_start,
            } => write!(
                f,
                "the segment {start} in the store reaches the segment {checkpoint_start} that \
                 the WAL checkpoint keeps on disk"
            ),
            Self::AboveFloor { floor: Some(floor) } => write!(
                f,
                "they reach above the tiered floor {floor}, so no advance confirmed them"
            ),
            Self::AboveFloor { floor: None } => {
                f.write_str("no advance of the tiered floor confirmed them")
            }
            Self::OverlapsFront { start, end } => write!(
                f,
                "the segment {start} in the store ends at {end}, inside the local log"
            ),
            Self::NoChecksum { start } => write!(
                f,
                "neither the store nor the manifest gives the CRC32C of the segment {start}"
            ),
            Self::ManifestDisagrees { start } => write!(
                f,
                "the manifest records the segment {start} differently from the store"
            ),
            Self::ManifestSkipped { start } => write!(
                f,
                "the manifest lists a segment at {start} that the store does not chain"
            ),
            Self::ForeignManifest => {
                f.write_str("the manifest names another partition or incarnation")
            }
            Self::StrayFile { start } => write!(
                f,
                "an index or anchor of a segment at {start} is on disk, and the store does not \
                 chain that segment"
            ),
        }
    }
}

/// Decide what a partition of a tiered topic does when it finds no chain.
///
/// An eviction needs an applied floor advance, and leaves the `.index` and
/// the `.anchor` of the segment behind, so a front that starts above the
/// first offset retention kept with neither a floor nor such a file was
/// never evicted. Retention that ran before tiering took effect commits no
/// watermark and leaves such a front, so the partition recovers as it would
/// untiered, and its new chain records that front.
#[must_use]
pub fn chain_loss(local: &LocalFront<'_>) -> ChainLoss {
    let from = local.first_kept();
    let Some(front) = local.front else {
        return ChainLoss::Refuse(ChainLost {
            from,
            to: u64::MAX,
            reason: ChainLostReason::NoLocalLog,
        });
    };
    if front <= from {
        return ChainLoss::Fresh;
    }
    if local.floor.is_some() || local.stems_without_log.iter().any(|stem| *stem >= from) {
        return ChainLoss::Rebuild;
    }
    ChainLoss::Fresh
}

/// What a rebuild asks of the store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreViewRequest {
    pub prefix: ObjectPrefix,
    /// The first offset retention kept, where the walk starts.
    pub from: u64,
    /// The first offset of the first `.log` on disk, where the walk stops.
    pub front: u64,
    /// The start offsets of the local sealed segments at or below the floor,
    /// whose objects the rebuilt chain lets the replica confirm again.
    pub local_sealed: Vec<u64>,
}

impl StoreViewRequest {
    /// Whether the partition of incarnation `created_revision`, trimmed
    /// through `watermark`, may still ask this. Another incarnation or trim
    /// asks another range, or nothing of the store at all.
    #[must_use]
    pub const fn is_for(&self, created_revision: u64, watermark: u64) -> bool {
        self.prefix.created_revision == created_revision && self.from == first_kept(watermark)
    }
}

/// What the store holds of one incarnation, as far as a rebuild reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreView {
    /// The request this view answers.
    pub request: StoreViewRequest,
    /// Every segment keyed by its start that the store lists, by start.
    pub listed: BTreeMap<u64, ListedSegment>,
    /// What a HEAD found of each `.log` object the walk may take, by start.
    pub logs: BTreeMap<u64, StoredLog>,
    pub manifest: ManifestRead,
}

/// What a HEAD found of a listed `.log` object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoredLog {
    Found {
        size: u64,
        /// What the store computed, `None` when it keeps no CRC32C.
        crc32c: Option<u32>,
        metadata: Result<SegmentMetadata, SegmentMetadataError>,
    },
    /// Deleted between the listing and the HEAD.
    Gone,
    /// The HEAD failed with this error. Only a walk that needs the object
    /// fails on it.
    Failed(String),
}

/// What a rebuild read of the incarnation's manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestRead {
    Absent,
    Read(PartitionManifest),
    /// Larger than [`MANIFEST_CROSS_CHECK_BYTES_MAX`], so not decoded.
    TooLarge {
        size: u64,
    },
    Undecodable(ManifestError),
}

/// Local facts the walk needs beside [`LocalFront`].
#[derive(Debug, Clone, Copy)]
pub struct RebuildInput<'a> {
    pub local: LocalFront<'a>,
    /// The incarnation every rebuilt record names.
    pub created_revision: u64,
    /// The anchors on disk below or at the front, by the start offset of the
    /// segment each stands beside.
    pub anchors: &'a BTreeMap<u64, SegmentAnchor>,
    /// The first and the last offset of every local sealed segment at or
    /// below the floor.
    pub local_sealed: &'a [(u64, u64)],
    /// The first offset of the segment the WAL checkpoint names, which
    /// recovery keeps on disk.
    pub checkpoint_start: Option<u64>,
}

/// Why a rebuild wrote no chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RebuildFailure {
    /// A range the view does not account for.
    Lost(ChainLost),
    /// The HEAD of the object starting at `start`, which the walk needs,
    /// failed, so the view says nothing of it yet.
    Unread { start: u64, error: String },
}

impl From<ChainLost> for RebuildFailure {
    fn from(lost: ChainLost) -> Self {
        Self::Lost(lost)
    }
}

/// The chain a rebuild writes, and what it holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RebuiltFront {
    pub records: Vec<ChainRecord>,
    /// The segments the chain holds only remotely.
    pub remote_segments: usize,
    /// The local segments the chain lets the replica confirm again.
    pub confirmed_local: usize,
    pub trim_point: Option<u64>,
}

/// Walk what `view` holds from the first offset retention kept, following
/// each segment's end, up to the local front, and return the chain that
/// describes it.
///
/// The walk stays at or below the floor and stops exactly at the front. A gap
/// is taken only where an anchor on disk names the segment before it, or,
/// before the first segment, where an anchor names a segment that ended at or
/// below the watermark. The manifest cross-checks what the walk took, and is
/// never its source: every segment it lists in the walked range must be one
/// the walk took, as the walk took it.
///
/// The trim point is the committed watermark, whatever the store or the
/// manifest hold below it, or the offset before the first segment when an
/// anchor proves the offsets between them a gap. Recovery refuses a chain
/// whose first segment starts above the offset after its trim point, so the
/// chain it writes opens by the same rules as any other.
///
/// # Errors
/// The first range the walk cannot account for, or the failed read of an
/// object it needs.
pub fn rebuild_front(
    view: &StoreView,
    input: &RebuildInput<'_>,
) -> Result<RebuiltFront, RebuildFailure> {
    let local = input.local;
    let from = local.first_kept();
    let Some(front) = local.front else {
        return Err(ChainLost {
            from,
            to: u64::MAX,
            reason: ChainLostReason::NoLocalLog,
        }
        .into());
    };
    let walk = Walk {
        view,
        anchors: input.anchors,
        front,
        floor: local.floor,
        checkpoint_start: input.checkpoint_start,
    };
    let mut walked = walk.objects(from)?;
    walk.cross_check(&mut walked, from)?;
    walk.check_stems(&walked, from, local.stems_without_log)?;

    let first_start = walked
        .first()
        .map_or(front, |object| object.metadata.start_offset);
    let trim_point = if first_start > from {
        Some(first_start - 1)
    } else {
        (local.watermark > 0).then_some(local.watermark)
    };
    let mut records = Vec::with_capacity(walked.len() + input.local_sealed.len() + 1);
    records.extend(trim_point.map(ChainRecord::TrimPoint));
    for object in &walked {
        let metadata = &object.metadata;
        let crc32c = object.crc32c.ok_or_else(|| ChainLost {
            from: metadata.start_offset.max(from),
            to: metadata.end_offset,
            reason: ChainLostReason::NoChecksum {
                start: metadata.start_offset,
            },
        })?;
        records.push(ChainRecord::Remote(SegmentRecord {
            start_offset: metadata.start_offset,
            end_offset: metadata.end_offset,
            start_timestamp: metadata.start_timestamp,
            end_timestamp: metadata.end_timestamp,
            max_timestamp: metadata.max_timestamp,
            size: metadata.size,
            incarnation: input.created_revision,
            hash: metadata.hash,
            crc32c,
            source: HashSource::Disk,
        }));
    }
    let remote_end = records.len();
    records.extend(
        input
            .local_sealed
            .iter()
            .filter_map(|&(start, end)| walk.stored_end(start, end))
            .map(ChainRecord::Advanced),
    );
    Ok(RebuiltFront {
        confirmed_local: records.len() - remote_end,
        records,
        remote_segments: walked.len(),
        trim_point,
    })
}

/// Reads what a rebuild needs of an incarnation, behind a pointer, so the
/// shard does not carry the executor's type.
pub trait StoreViewReader {
    /// Read what `request` asks for under `scope`.
    fn read(
        &self,
        request: StoreViewRequest,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, StoreViewOutcome>;
}

/// The result of a read: the store's answer, or that the job's scope was
/// cancelled first.
pub type StoreViewOutcome = Result<Result<StoreView, ObjectStoreError>, TierJobCancelled>;

/// Reads through a tier executor, in jobs of its demand fetch class: a
/// partition waits for them before it serves anything. One job lists the
/// incarnation and reads the metadata of its objects, and holds no bytes of
/// the class. A second reads the manifest, and holds the bytes it may read
/// only while it runs. The jobs never go through the read cache.
pub struct ExecutorStoreViewReader<E> {
    executor: Rc<E>,
    layout: KeyLayout,
}

impl<E> ExecutorStoreViewReader<E> {
    #[must_use]
    pub fn new(executor: E, layout: KeyLayout) -> Self {
        Self {
            executor: Rc::new(executor),
            layout,
        }
    }
}

impl<E: TierExecutor + 'static> StoreViewReader for ExecutorStoreViewReader<E> {
    fn read(
        &self,
        request: StoreViewRequest,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, StoreViewOutcome> {
        let executor = Rc::clone(&self.executor);
        let layout = self.layout.clone();
        async move {
            let listing = {
                let layout = layout.clone();
                let request = request.clone();
                executor
                    .submit(
                        TierJobRequest::new(TierJobClass::DemandFetch, scope.clone()),
                        move |store| async move { read_listing(&*store, &layout, &request).await },
                    )
                    .await
            };
            let (listed, logs) = match listing? {
                Ok(listing) => listing,
                Err(error) => return Ok(Err(error)),
            };
            let prefix = request.prefix;
            let manifest = executor
                .submit(
                    TierJobRequest::new(TierJobClass::DemandFetch, scope)
                        .with_bytes(MANIFEST_CROSS_CHECK_BYTES_MAX),
                    move |store| async move { read_manifest(&*store, &layout, &prefix).await },
                )
                .await?;
            Ok(manifest.map(|manifest| StoreView {
                request,
                listed,
                logs,
                manifest,
            }))
        }
        .boxed_local()
    }
}

/// Read what `request` asks for in one go: the listing and the HEADs of
/// [`read_listing`], then one read of the manifest.
///
/// # Errors
/// As [`read_listing`] and [`read_manifest`].
#[cfg(test)]
async fn read_store_view<S: ObjectStore>(
    store: &S,
    layout: &KeyLayout,
    request: StoreViewRequest,
) -> Result<StoreView, ObjectStoreError> {
    let (listed, logs) = read_listing(store, layout, &request).await?;
    let manifest = read_manifest(store, layout, &request.prefix).await?;
    Ok(StoreView {
        request,
        listed,
        logs,
        manifest,
    })
}

/// What the store lists of an incarnation, by start, and what a HEAD finds of
/// each `.log` object from the one that holds `request.from` up to the front
/// and of each local sealed segment, up to [`STORE_VIEW_HEADS`] at once.
///
/// # Errors
/// The error of the listing. A HEAD that failed is kept as
/// [`StoredLog::Failed`], and fails the rebuild only if the walk needs it.
async fn read_listing<S: ObjectStore>(
    store: &S,
    layout: &KeyLayout,
    request: &StoreViewRequest,
) -> Result<(BTreeMap<u64, ListedSegment>, BTreeMap<u64, StoredLog>), ObjectStoreError> {
    let listed = list_incarnation(store, layout, &request.prefix)
        .await?
        .segments;
    let first = listed
        .range(..=request.from)
        .next_back()
        .map_or(request.from, |(start, _)| *start);
    let wanted: BTreeSet<u64> = listed
        .range(first..request.front)
        .map(|(start, _)| *start)
        .chain(request.local_sealed.iter().copied())
        .filter(|start| {
            listed
                .get(start)
                .is_some_and(|segment| segment.log_size.is_some())
        })
        .collect();
    let prefix = request.prefix;
    let logs = stream::iter(wanted)
        .map(|start| head_log(store, layout, prefix, start))
        .buffered(STORE_VIEW_HEADS)
        .collect()
        .await;
    Ok((listed, logs))
}

/// What a HEAD finds of the `.log` object of the segment keyed `start`.
async fn head_log<S: ObjectStore>(
    store: &S,
    layout: &KeyLayout,
    prefix: ObjectPrefix,
    start: u64,
) -> (u64, StoredLog) {
    let key = layout.segment_key(&SegmentKey {
        prefix,
        start_offset: start,
        object: SegmentObject::Log,
    });
    let stored = match store.head(&key).await {
        Ok(meta) => StoredLog::Found {
            size: meta.size,
            crc32c: meta.crc32c,
            metadata: SegmentMetadata::try_from(&meta.metadata),
        },
        Err(ObjectStoreError::NotFound { .. }) => StoredLog::Gone,
        Err(error) => StoredLog::Failed(error.to_string()),
    };
    (start, stored)
}

async fn read_manifest<S: ObjectStore>(
    store: &S,
    layout: &KeyLayout,
    prefix: &ObjectPrefix,
) -> Result<ManifestRead, ObjectStoreError> {
    let key = layout.manifest_key(prefix);
    // No head first: the primary replaces the manifest under this key, so a
    // size read before the bytes can be of an older, shorter manifest. The
    // read reports the size of the object its bytes come from.
    let bytes = match store
        .get_range(&key, 0..MANIFEST_CROSS_CHECK_BYTES_MAX)
        .await
    {
        Ok(read) if read.size > MANIFEST_CROSS_CHECK_BYTES_MAX => {
            return Ok(ManifestRead::TooLarge { size: read.size });
        }
        Ok(read) => read.bytes,
        Err(ObjectStoreError::NotFound { .. }) => return Ok(ManifestRead::Absent),
        // An empty object has no readable range.
        Err(ObjectStoreError::InvalidRange { start: 0, .. }) => Bytes::new(),
        Err(error) => return Err(error),
    };
    Ok(
        PartitionManifest::decode(&bytes)
            .map_or_else(ManifestRead::Undecodable, ManifestRead::Read),
    )
}

/// One object the walk took.
#[derive(Debug, Clone, Copy)]
struct Walked {
    metadata: SegmentMetadata,
    crc32c: Option<u32>,
}

struct Walk<'a> {
    view: &'a StoreView,
    anchors: &'a BTreeMap<u64, SegmentAnchor>,
    front: u64,
    floor: Option<u64>,
    checkpoint_start: Option<u64>,
}

impl Walk<'_> {
    /// The objects from the one that holds `from` up to the front, each
    /// starting right after the one before it or past a gap an anchor names.
    fn objects(&self, from: u64) -> Result<Vec<Walked>, RebuildFailure> {
        let mut walked: Vec<Walked> = Vec::new();
        let mut next = from;
        while next < self.front {
            let start = match walked.last() {
                None => match self.leading_gap(next) {
                    Some(planted) if planted == self.front => break,
                    Some(planted) => {
                        next = planted;
                        self.view.listed.contains_key(&planted).then_some(planted)
                    }
                    None => self
                        .view
                        .listed
                        .range(..=next)
                        .next_back()
                        .map(|(start, _)| *start),
                },
                Some(_) if self.view.listed.contains_key(&next) => Some(next),
                Some(previous) => match self.anchored_gap(&previous.metadata) {
                    Some(planted) if planted == self.front => break,
                    Some(planted) if planted > next && planted < self.front => {
                        next = planted;
                        self.view.listed.contains_key(&planted).then_some(planted)
                    }
                    _ => None,
                },
            };
            if self.floor.is_none_or(|floor| next > floor) {
                return Err(self
                    .lost(
                        next,
                        self.last_below_front(),
                        ChainLostReason::AboveFloor { floor: self.floor },
                    )
                    .into());
            }
            let Some(start) = start else {
                return Err(self
                    .lost(next, self.gap_end(next), ChainLostReason::MissingObject)
                    .into());
            };
            let object = self.object(start, next)?;
            next = object.metadata.end_offset.saturating_add(1);
            walked.push(object);
        }
        Ok(walked)
    }

    /// The object keyed `start`, which must hold `covers`, end at or below
    /// the floor, and end before the front.
    fn object(&self, start: u64, covers: u64) -> Result<Walked, RebuildFailure> {
        let unknown_end = self.gap_end(covers);
        let listed = self.view.listed.get(&start).copied().unwrap_or_default();
        if let (Some(_), Some(StoredLog::Failed(error))) =
            (listed.log_size, self.view.logs.get(&start))
        {
            return Err(RebuildFailure::Unread {
                start,
                error: error.clone(),
            });
        }
        Ok(self.found_object(start, covers, unknown_end, listed)?)
    }

    /// [`Self::object`] once the store answered the HEAD of the object.
    fn found_object(
        &self,
        start: u64,
        covers: u64,
        unknown_end: u64,
        listed: ListedSegment,
    ) -> Result<Walked, ChainLost> {
        let (
            Some(log_size),
            Some(StoredLog::Found {
                size,
                crc32c,
                metadata,
            }),
        ) = (listed.log_size, self.view.logs.get(&start))
        else {
            return Err(self.lost(covers, unknown_end, ChainLostReason::MissingObject));
        };
        let Ok(metadata) = metadata else {
            return Err(self.lost(
                covers,
                unknown_end,
                ChainLostReason::UnreadableMetadata { start },
            ));
        };
        if metadata.start_offset != start {
            return Err(self.lost(
                covers,
                unknown_end,
                ChainLostReason::MisplacedObject {
                    start,
                    recorded: metadata.start_offset,
                },
            ));
        }
        let end = metadata.end_offset.min(self.last_below_front());
        if metadata.end_offset < covers {
            return Err(self.lost(covers, unknown_end, ChainLostReason::MissingObject));
        }
        if *size != log_size || metadata.size != log_size {
            return Err(self.lost(
                covers,
                end,
                ChainLostReason::SizeMismatch {
                    start,
                    listed: log_size,
                    recorded: metadata.size,
                },
            ));
        }
        if log_size == 0 {
            return Err(self.lost(covers, end, ChainLostReason::EmptyObject { start }));
        }
        if let Some(reason) = index_fault(start, &listed) {
            return Err(self.lost(covers, end, reason));
        }
        if self.floor.is_none_or(|floor| metadata.end_offset > floor) {
            return Err(self.lost(
                covers,
                self.last_below_front(),
                ChainLostReason::AboveFloor { floor: self.floor },
            ));
        }
        if metadata.end_offset >= self.front {
            return Err(self.lost(
                covers,
                end,
                ChainLostReason::OverlapsFront {
                    start,
                    end: metadata.end_offset,
                },
            ));
        }
        if let Some(checkpoint_start) = self
            .checkpoint_start
            .filter(|checkpoint_start| metadata.end_offset >= *checkpoint_start)
        {
            return Err(self.lost(
                covers,
                end,
                ChainLostReason::AtCheckpoint {
                    start,
                    checkpoint_start,
                },
            ));
        }
        Ok(Walked {
            metadata: *metadata,
            crc32c: *crc32c,
        })
    }

    /// Where the first segment above `from` stands, when an anchor beside it
    /// names a segment that ended below `from` and the store lists nothing
    /// between them: retention removed that segment, and the offsets up to
    /// the anchored one were never written.
    fn leading_gap(&self, from: u64) -> Option<u64> {
        let (planted, anchor) = self
            .anchors
            .range(from.saturating_add(1)..=self.front)
            .next()?;
        let proves = anchor.planted_start == *planted
            && anchor.sealed_start <= anchor.sealed_end
            && anchor.sealed_end < from
            && self
                .view
                .listed
                .range(anchor.sealed_end.saturating_add(1)..*planted)
                .next()
                .is_none();
        proves.then_some(*planted)
    }

    /// Where the anchor that names `previous` as the segment before its gap
    /// stands.
    fn anchored_gap(&self, previous: &SegmentMetadata) -> Option<u64> {
        self.anchors
            .iter()
            .find(|(planted, anchor)| {
                anchor.covers(**planted, previous.start_offset, previous.end_offset)
            })
            .map(|(planted, _)| *planted)
    }

    /// Check what the walk took against the manifest, and take each CRC32C
    /// the store did not give from it.
    fn cross_check(&self, walked: &mut [Walked], from: u64) -> Result<(), ChainLost> {
        let ManifestRead::Read(manifest) = &self.view.manifest else {
            return Ok(());
        };
        if manifest.prefix != self.view.request.prefix {
            return Err(self.lost(
                from,
                self.last_below_front(),
                ChainLostReason::ForeignManifest,
            ));
        }
        for segment in manifest
            .segments
            .iter()
            .filter(|segment| segment.end_offset >= from && segment.start_offset < self.front)
        {
            let start = segment.start_offset;
            let range = (
                start.max(from),
                segment.end_offset.min(self.last_below_front()),
            );
            let Ok(at) = walked.binary_search_by_key(&start, |object| object.metadata.start_offset)
            else {
                return Err(self.lost(
                    range.0,
                    range.1,
                    ChainLostReason::ManifestSkipped { start },
                ));
            };
            let object = &mut walked[at];
            let metadata = &object.metadata;
            let agrees = (
                metadata.end_offset,
                metadata.start_timestamp,
                metadata.end_timestamp,
                metadata.max_timestamp,
                metadata.size,
                metadata.hash,
            ) == (
                segment.end_offset,
                segment.start_timestamp,
                segment.end_timestamp,
                segment.max_timestamp,
                segment.size,
                segment.hash,
            ) && object.crc32c.is_none_or(|crc32c| crc32c == segment.crc32c);
            if !agrees {
                return Err(self.lost(
                    range.0,
                    range.1,
                    ChainLostReason::ManifestDisagrees { start },
                ));
            }
            object.crc32c.get_or_insert(segment.crc32c);
        }
        Ok(())
    }

    /// Every `.index` or `.anchor` left on disk from the first walked segment
    /// or from `from`, whichever is lower, belongs to a segment the walk
    /// took. One inside a leading gap contradicts the anchor of that gap.
    fn check_stems(&self, walked: &[Walked], from: u64, stems: &[u64]) -> Result<(), ChainLost> {
        let first = walked
            .first()
            .map_or(from, |object| object.metadata.start_offset)
            .min(from);
        for &stem in stems.iter().filter(|stem| **stem >= first) {
            let at = walked.partition_point(|object| object.metadata.start_offset < stem);
            if walked
                .get(at)
                .is_some_and(|object| object.metadata.start_offset == stem)
            {
                continue;
            }
            let to = walked
                .get(at)
                .map_or(self.front, |object| object.metadata.start_offset)
                .saturating_sub(1);
            return Err(self.lost(stem, to, ChainLostReason::StrayFile { start: stem }));
        }
        Ok(())
    }

    /// The advance that confirms the local sealed segment from `start` to
    /// `end` again, when the store holds its object as it ends and an index a
    /// reader can use.
    fn stored_end(&self, start: u64, end: u64) -> Option<AdvanceTieredFloor> {
        let listed = self.view.listed.get(&start)?;
        let Some(StoredLog::Found {
            size,
            metadata: Ok(metadata),
            ..
        }) = self.view.logs.get(&start)
        else {
            return None;
        };
        let stored = listed.log_size == Some(*size)
            && index_fault(start, listed).is_none()
            && metadata.start_offset == start
            && metadata.end_offset == end
            && metadata.size == *size
            && self.floor.is_some_and(|floor| end <= floor);
        stored.then_some(AdvanceTieredFloor {
            start,
            up_to: end,
            hash: metadata.hash,
        })
    }

    /// The last offset before the next listed start past `at`, or before the
    /// front.
    fn gap_end(&self, at: u64) -> u64 {
        let next = self
            .view
            .listed
            .range(at.saturating_add(1)..)
            .next()
            .map_or(self.front, |(start, _)| (*start).min(self.front));
        next.saturating_sub(1).max(at)
    }

    /// The last offset below the front.
    const fn last_below_front(&self) -> u64 {
        self.front.saturating_sub(1)
    }

    fn lost(&self, from: u64, to: u64, reason: ChainLostReason) -> ChainLost {
        ChainLost {
            from,
            to: to.min(self.last_below_front()).max(from),
            reason,
        }
    }
}

/// Why the `.index` listed for the segment keyed `start` is of no use to a
/// reader, `None` when it holds one or more whole entries.
fn index_fault(start: u64, listed: &ListedSegment) -> Option<ChainLostReason> {
    match listed.index_size {
        None => Some(ChainLostReason::MissingIndex { start }),
        Some(size) if size == 0 || size % IGGY_INDEX_SIZE as u64 != 0 => {
            Some(ChainLostReason::IndexSize { start, size })
        }
        Some(_) => None,
    }
}
