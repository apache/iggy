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

//! The merge of small long-term segments that follow each other into one,
//! by the partition's primary.
//!
//! A rewrite stores what was committed when it ran, so a partition with
//! little load gets many small segments. A merge reads a run of them from
//! the store, writes their bytes back to back as one segment, stores it, and
//! commits a replace, which lists the one in the place of the many. The
//! objects of the segments it replaced are deleted as any other object the
//! long-term log no longer lists.
//!
//! [`MergeRun::best`] picks the run from the long-term log alone, so every
//! primary picks the same one. A primary that takes over after the merged
//! segment was stored and before its command committed stores the same
//! bytes under the same key, finds them there, and commits the command.
//!
//! The long-term log holds the registration of a merged segment before its
//! object is stored, and the replace clears it. An object whose replace
//! never commits, as when the next primary merged other segments, is
//! deleted once its registration expires.
//!
//! A merge never trusts the store for what a segment holds: the bytes of
//! every source must match the size and the hashes the log committed for
//! it. A source that does not is left where it is and merged by nobody
//! while this replica stays the primary, since a merge would give its bytes
//! a new hash that nothing contradicts.
//!
//! Merges run beside the rewrites and wait for none of them: a replace and
//! the publish of the next segment commute.
//!
//! A log of segments that are all large enough is never merged, and still
//! takes no segment beyond [`LONG_TERM_SEGMENTS_MAX`]. From
//! [`LONG_TERM_SEGMENTS_PRESSURE`] segments on, a partition merges larger
//! ones into larger ones, and no longer waits for the shard to be idle.

use std::collections::BTreeSet;
use std::io;
use std::rc::Rc;

use compio::io::AsyncWriteAtExt;
use futures::FutureExt;
use futures::future::LocalBoxFuture;
use iggy_binary_protocol::batch::{BATCH_HEADER_SIZE, BatchHeader};
use iggy_common::IggyTimestamp;
use iggy_object_store::{
    KeyLayout, LongTermKey, ManifestSegment, ObjectPrefix, ObjectStore, ObjectStoreError,
    SegmentObject, TierExecutor, TierJobCancelled, TierJobClass, TierJobRequest, TierScope,
    TierScopeToken,
};
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use tracing::{debug, error, warn};

use crate::IggyPartition;
use crate::iggy_index::IGGY_INDEX_SIZE;
use crate::long_term::{
    LONG_TERM_REGISTRATION_TTL, LONG_TERM_SEGMENTS_MAX, LongTermLog, ObjectRegistration,
    REPLACE_SOURCES_MAX, concatenated,
};
use crate::long_term_delete::LongTermObject;
use crate::long_term_rewrite::{
    INDEX_STRIDE_BYTES, LONG_TERM_SEGMENT_BYTES, SegmentIndex, write_file,
};
use crate::placed_read::LONG_TERM_INDEX_BYTES;
use crate::segment_chain::{HashSource, SegmentHasher, SegmentHashes, SegmentRecord};
use crate::segment_upload::{SegmentUpload, UploadError, upload_segment_under};
use crate::state_transfer::{ChunkHasher, STAGING_SUFFIX, hash_segment_range};

/// Bytes below which a long-term segment is small enough to merge: half of
/// what a segment holds at most, so two of them always fit in one.
pub const LONG_TERM_MERGE_MIN_BYTES: u64 = LONG_TERM_SEGMENT_BYTES as u64 / 2;

/// Bytes of a source that a merge reads from the store, and holds in
/// memory, at once.
pub const LONG_TERM_MERGE_READ_BYTES: u64 = 8 * 1024 * 1024;

/// Segments from which a long-term log is near its limit: three quarters of
/// [`LONG_TERM_SEGMENTS_MAX`].
///
/// A partition that holds this many merges whatever makes its log shorter,
/// so that it does not stop publishing at the limit.
pub const LONG_TERM_SEGMENTS_PRESSURE: usize = LONG_TERM_SEGMENTS_MAX / 4 * 3;

/// Bytes a merged segment holds at most when its log is near the limit.
pub const LONG_TERM_MERGE_PRESSURE_BYTES: u64 = 1024 * 1024 * 1024;

// A poll reads no more than `LONG_TERM_INDEX_BYTES` of an index, so the
// index of the largest merged segment has to fit in them.
const _: () = assert!(
    LONG_TERM_MERGE_PRESSURE_BYTES.div_ceil(INDEX_STRIDE_BYTES) * IGGY_INDEX_SIZE as u64
        <= LONG_TERM_INDEX_BYTES
);

/// Which segments a merge reads, and how large the one it writes may be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MergeLimits {
    /// Bytes below which a segment is small enough to merge.
    pub small_below: u64,
    /// Bytes the merged segment holds at most.
    pub output_max: u64,
}

impl MergeLimits {
    /// The limits of a log that is not near its limit: two small segments
    /// always fit in one.
    pub const NORMAL: Self = Self {
        small_below: LONG_TERM_MERGE_MIN_BYTES,
        output_max: LONG_TERM_SEGMENT_BYTES as u64,
    };

    /// Twice these limits. `None` when the merged segment would hold more
    /// than [`LONG_TERM_MERGE_PRESSURE_BYTES`].
    #[must_use]
    pub const fn doubled(self) -> Option<Self> {
        let output_max = self.output_max.saturating_mul(2);
        if output_max > LONG_TERM_MERGE_PRESSURE_BYTES {
            return None;
        }
        Some(Self {
            small_below: self.small_below.saturating_mul(2),
            output_max,
        })
    }
}

/// The segments one merge replaces: `count` segments of the long-term log
/// that follow each other, from the one at `first`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MergeRun {
    /// Where the first segment is in the log.
    pub first: usize,
    pub count: usize,
    /// The first offset of the first segment.
    pub start_offset: u64,
    /// The bytes of the segments together.
    pub bytes: u64,
}

impl MergeRun {
    /// How many segments shorter the merge makes the log.
    #[must_use]
    pub const fn reclaim(&self) -> usize {
        self.count - 1
    }

    /// One merge for every run of small segments in `segments`, the
    /// long-term log in offset order.
    ///
    /// A segment below `limits.small_below` is small, and a run is every
    /// small segment between two that are not. A merge takes the segments
    /// from the start of its run for as long as their bytes fit in
    /// `limits.output_max` and a replace takes that many. A segment that is
    /// not small is not read: merging it would cost its bytes and make the
    /// log no shorter. A small segment alone in its run stays.
    #[must_use]
    pub fn all(segments: &[ManifestSegment], limits: MergeLimits) -> Vec<Self> {
        let small = |segment: &&ManifestSegment| segment.size < limits.small_below;
        let mut merges = Vec::new();
        let mut first = 0;
        while first < segments.len() {
            let run = segments[first..].iter().take_while(small).count();
            let mut merge = Self {
                first,
                count: 0,
                start_offset: segments[first].start_offset,
                bytes: 0,
            };
            for segment in &segments[first..first + run] {
                if merge.count == REPLACE_SOURCES_MAX
                    || merge.bytes + segment.size > limits.output_max
                {
                    break;
                }
                merge.count += 1;
                merge.bytes += segment.size;
            }
            if merge.count >= 2 {
                merges.push(merge);
            }
            first += run.max(1);
        }
        merges
    }

    /// The merge of `segments` within `limits` that makes the log shortest,
    /// and of several that do, the one at the lowest offset. A merge whose
    /// sources `skipped` names is left out.
    #[must_use]
    pub fn best(
        segments: &[ManifestSegment],
        limits: MergeLimits,
        skipped: impl Fn(&[ManifestSegment]) -> bool,
    ) -> Option<Self> {
        Self::all(segments, limits)
            .into_iter()
            .filter(|merge| !skipped(merge.sources(segments)))
            // The first of equal ones is the one at the lowest offset.
            .reduce(|best, merge| {
                if merge.reclaim() > best.reclaim() {
                    merge
                } else {
                    best
                }
            })
    }

    /// The best merge of a log near its limit: the one within
    /// [`MergeLimits::NORMAL`], and when there is none, the one within the
    /// first doubling of them that has one. The cheapest merge that makes
    /// the log shorter is taken, and none writes a segment of more than
    /// [`LONG_TERM_MERGE_PRESSURE_BYTES`].
    #[must_use]
    pub fn relieving(
        segments: &[ManifestSegment],
        skipped: impl Fn(&[ManifestSegment]) -> bool,
    ) -> Option<Self> {
        std::iter::successors(Some(MergeLimits::NORMAL), |limits| limits.doubled())
            .find_map(|limits| Self::best(segments, limits, &skipped))
    }

    fn sources<'a>(&self, segments: &'a [ManifestSegment]) -> &'a [ManifestSegment] {
        &segments[self.first..self.first + self.count]
    }

    /// The merge of the segments of `segments` from the one that starts at
    /// `start_offset` to the one that ends at `end_offset`, when those are
    /// two segments or more.
    fn spanning(
        segments: &[ManifestSegment],
        (start_offset, end_offset): (u64, u64),
    ) -> Option<Self> {
        let first = segments
            .binary_search_by_key(&start_offset, |segment| segment.start_offset)
            .ok()?;
        let last = segments
            .binary_search_by_key(&end_offset, |segment| segment.end_offset)
            .ok()?;
        (last > first).then(|| Self {
            first,
            count: last - first + 1,
            start_offset,
            bytes: segments[first..=last]
                .iter()
                .map(|segment| segment.size)
                .sum(),
        })
    }
}

/// One merge, as the partition's owner captured it.
pub struct SegmentMerge {
    pub prefix: ObjectPrefix,
    /// The segments to merge as the long-term log lists them, in offset
    /// order.
    pub sources: Vec<ManifestSegment>,
    pub partition_dir: String,
    /// The class the merge runs in: [`TierJobClass::Housekeeping`], or
    /// [`TierJobClass::Upload`] for a log near its limit.
    pub class: TierJobClass,
}

#[derive(Debug, thiserror::Error)]
pub enum MergeError {
    /// The store does not hold the bytes the long-term log committed for
    /// the segment at `start_offset`.
    #[error(
        "the object of the long-term segment at offset {start_offset} is not what was committed"
    )]
    SourceDiverged { start_offset: u64 },
    #[error("cannot read a segment to merge: {0}")]
    Store(ObjectStoreError),
    /// The bytes of the sources together are not one segment.
    #[error("the merged segment from offset {start_offset} is not a segment: {reason}")]
    Malformed {
        start_offset: u64,
        reason: &'static str,
    },
    #[error("cannot write the merged segment: {0}")]
    Io(#[from] io::Error),
    #[error(transparent)]
    Upload(UploadError),
}

/// What a merge ended as: the segment it stored, or why it stored none, or
/// a cancellation before it finished.
pub type MergeOutcome = Result<Result<ManifestSegment, MergeError>, TierJobCancelled>;

/// Runs the merges of a shard, behind a pointer, so the shard does not
/// carry the executor's type.
pub trait LongTermMerger {
    /// Run `merge` under `scope`. The merge stops when the scope is
    /// cancelled, as when its partition loses primacy.
    fn merge(
        &self,
        merge: SegmentMerge,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, MergeOutcome>;
}

/// Merges through a tier executor, in the class each merge names.
///
/// In the housekeeping class a merge starts only when no read and no upload
/// waits. A log near its limit cannot wait for that, and its merge takes
/// the place of an upload.
pub struct ExecutorLongTermMerger<E> {
    executor: E,
    layout: KeyLayout,
}

impl<E> ExecutorLongTermMerger<E> {
    #[must_use]
    pub const fn new(executor: E, layout: KeyLayout) -> Self {
        Self { executor, layout }
    }

    /// The executor the merges go through.
    #[must_use]
    pub const fn executor(&self) -> &E {
        &self.executor
    }
}

impl<E: TierExecutor> LongTermMerger for ExecutorLongTermMerger<E> {
    fn merge(
        &self,
        merge: SegmentMerge,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, MergeOutcome> {
        let layout = self.layout.clone();
        let bytes = merge.sources.iter().map(|source| source.size).sum();
        let request = TierJobRequest::new(merge.class, scope).with_bytes(bytes);
        self.executor
            .submit(request, move |store: Rc<E::Store>| async move {
                merge.run(&*store, &layout).await
            })
            .boxed_local()
    }
}

impl SegmentMerge {
    /// Copy the sources into one file beside the partition's own, build its
    /// index, store both, and remove the files again.
    async fn run<S: ObjectStore>(
        self,
        store: &S,
        layout: &KeyLayout,
    ) -> Result<ManifestSegment, MergeError> {
        let start_offset = self.sources.first().map_or(0, |first| first.start_offset);
        let stem = format!("{}/{start_offset:0>20}.merge", self.partition_dir);
        let log_path = format!("{stem}.log{STAGING_SUFFIX}");
        let index_path = format!("{stem}.index{STAGING_SUFFIX}");
        let stored = self
            .store(store, layout, start_offset, &log_path, &index_path)
            .await;
        // Left behind, the files are swept at the next boot.
        let _ = compio::fs::remove_file(&log_path).await;
        let _ = compio::fs::remove_file(&index_path).await;
        stored
    }

    async fn store<S: ObjectStore>(
        &self,
        store: &S,
        layout: &KeyLayout,
        start_offset: u64,
        log_path: &str,
        index_path: &str,
    ) -> Result<ManifestSegment, MergeError> {
        let malformed = |reason| MergeError::Malformed {
            start_offset,
            reason,
        };
        let key = |start_offset, end_offset, object| {
            layout.long_term_key(&LongTermKey {
                prefix: self.prefix,
                start_offset,
                end_offset,
                object,
            })
        };
        let mut file = compio::fs::File::create(log_path).await?;
        let mut size = 0u64;
        for source in &self.sources {
            let source_key = key(source.start_offset, source.end_offset, SegmentObject::Log);
            copy_source(store, &source_key, source, &mut file, size).await?;
            size += source.size;
        }
        file.sync_all().await?;

        // The index comes from the bytes of the file and from nothing else,
        // so it is the one a rewrite of the same offsets builds.
        let mut walk = LogWalk::new(start_offset);
        hash_segment_range(log_path, 0, size, &mut walk, None).await?;
        let walked = walk.finish().map_err(malformed)?;
        let merged = concatenated(&self.sources, walked.hashes.blake3)
            .ok_or_else(|| malformed("it has no source"))?;
        // A replace is refused unless the segment is what its sources add
        // up to.
        if (merged.size, merged.crc32c, merged.end_offset)
            != (size, walked.hashes.crc32c, walked.end_offset)
        {
            return Err(malformed("it is not what its sources add up to"));
        }
        write_file(index_path, walked.index).await?;

        let upload = SegmentUpload {
            prefix: self.prefix,
            record: SegmentRecord {
                start_offset: merged.start_offset,
                end_offset: merged.end_offset,
                start_timestamp: merged.start_timestamp,
                end_timestamp: merged.end_timestamp,
                max_timestamp: merged.max_timestamp,
                size: merged.size,
                incarnation: 0,
                hash: merged.hash,
                crc32c: merged.crc32c,
                source: HashSource::Disk,
            },
            log_path: log_path.to_owned(),
            index_path: index_path.to_owned(),
        };
        upload_segment_under(
            store,
            &upload,
            key(merged.start_offset, merged.end_offset, SegmentObject::Log),
            &key(merged.start_offset, merged.end_offset, SegmentObject::Index),
        )
        .await
        .map_err(MergeError::Upload)?;
        Ok(merged)
    }
}

/// Append the object under `key` to `file` at `position`, a piece at a
/// time, and fail unless it is the bytes the long-term log committed as
/// `source`.
async fn copy_source<S: ObjectStore>(
    store: &S,
    key: &str,
    source: &ManifestSegment,
    file: &mut compio::fs::File,
    position: u64,
) -> Result<(), MergeError> {
    let diverged = || MergeError::SourceDiverged {
        start_offset: source.start_offset,
    };
    let mut hasher = SegmentHasher::default();
    let mut read = 0u64;
    while read < source.size {
        let end = source.size.min(read + LONG_TERM_MERGE_READ_BYTES);
        let piece = store
            .get_range(key, read..end)
            .await
            .map_err(MergeError::Store)?;
        if piece.size != source.size || piece.bytes.len() as u64 != end - read {
            return Err(diverged());
        }
        hasher.update(&piece.bytes);
        let compio::BufResult(written, _) = file
            .write_all_at(Vec::from(piece.bytes), position + read)
            .await;
        written?;
        read = end;
    }
    let found = hasher.finalize();
    if (found.blake3, found.crc32c) != (source.hash, source.crc32c) {
        return Err(diverged());
    }
    Ok(())
}

/// What a walk over the bytes of a segment found.
struct WalkedLog {
    hashes: SegmentHashes,
    index: Vec<u8>,
    end_offset: u64,
}

/// Takes a `.log` in order: its hashes, its index, and whether its batches
/// hold every offset from the first one on.
struct LogWalk {
    hasher: SegmentHasher,
    index: SegmentIndex,
    /// The bytes of the batch header that the chunks so far have not
    /// completed.
    header: Vec<u8>,
    /// The bytes left of the batch whose header was read.
    body_left: u64,
    start_offset: u64,
    next_offset: u64,
    fault: Option<&'static str>,
}

impl LogWalk {
    fn new(start_offset: u64) -> Self {
        Self {
            hasher: SegmentHasher::default(),
            index: SegmentIndex::default(),
            header: Vec::with_capacity(BATCH_HEADER_SIZE),
            body_left: 0,
            start_offset,
            next_offset: start_offset,
            fault: None,
        }
    }

    fn finish(self) -> Result<WalkedLog, &'static str> {
        if let Some(fault) = self.fault {
            return Err(fault);
        }
        if self.body_left > 0 || !self.header.is_empty() {
            return Err("it ends inside a batch");
        }
        if self.next_offset == self.start_offset {
            return Err("it holds no batch");
        }
        Ok(WalkedLog {
            hashes: self.hasher.finalize(),
            index: self.index.into_bytes(),
            end_offset: self.next_offset - 1,
        })
    }
}

impl ChunkHasher for LogWalk {
    fn update(&mut self, bytes: &[u8]) {
        self.hasher.update(bytes);
        let mut rest = bytes;
        while self.fault.is_none() && !rest.is_empty() {
            let body =
                usize::try_from(self.body_left).map_or(rest.len(), |left| left.min(rest.len()));
            self.body_left -= body as u64;
            rest = &rest[body..];
            let wanted = (BATCH_HEADER_SIZE - self.header.len()).min(rest.len());
            self.header.extend_from_slice(&rest[..wanted]);
            rest = &rest[wanted..];
            if self.header.len() < BATCH_HEADER_SIZE {
                break;
            }
            let Ok(header) = BatchHeader::decode(&self.header) else {
                self.fault = Some("a batch header does not decode");
                break;
            };
            if header.base_offset != self.next_offset || header.message_count == 0 {
                self.fault = Some("its batches do not follow each other offset by offset");
                break;
            }
            self.index.push(
                header.base_offset,
                header.base_timestamp,
                header.batch_length,
            );
            self.next_offset = header.base_offset + u64::from(header.message_count);
            self.body_left = header.batch_length - BATCH_HEADER_SIZE as u64;
            self.header.clear();
        }
    }
}

/// Which pass over a partition starts a merge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergePass {
    /// The pass a shard makes over its partitions on its interval.
    Scan,
    /// The pass over the partition whose merge just settled, so a long run
    /// of small segments is merged without a wait between its merges.
    Drain,
}

/// What a pass over a partition starts of its next merge.
pub enum MergeStart {
    /// Commit this registration of what the merge stores. A pass after it
    /// commits starts the merge.
    Register(ObjectRegistration),
    /// Run this merge under the scope of the token, and hand its result to
    /// [`IggyPartition::finish_segment_merge`] with the number.
    Merge(u64, SegmentMerge, TierScopeToken),
}

/// Where the registration of a merge stands in the long-term log.
enum Registered {
    /// The log holds it, from this view: the merge of `MergeRun` may store.
    Ready(MergeRun),
    /// Its command has not committed yet.
    Pending(ObjectRegistration),
    /// The log can no longer take it, or none was made.
    Gone,
}

/// What became of the merged segment a partition stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeSettled {
    /// The partition waits for no merged segment.
    Nothing,
    /// The long-term log does not list the segment yet, and still can: its
    /// command is sent again.
    Waiting(ManifestSegment),
    /// The long-term log lists the segment in the place of `replaced`
    /// others.
    Committed { replaced: usize },
    /// A trim removed a segment the merged one starts or ends with, so no
    /// command can list it, and its object waits for its delete.
    Retired,
}

/// The merge in flight.
struct RunningMerge {
    id: u64,
    /// How many segments it merges.
    sources: usize,
    /// The scope whose end stops it.
    _scope: TierScope,
}

/// What a replica keeps of its merges while it is the primary of a
/// partition that places its batches. Nothing here is persisted.
#[derive(Default)]
pub struct LongTermMerges {
    /// The view this state belongs to, `None` until the replica acts as the
    /// primary.
    view: Option<u32>,
    running: Option<RunningMerge>,
    /// A merged segment the store holds that the long-term log does not
    /// list yet, and how many segments it replaces. Its command is sent
    /// again until the log lists it or no longer can.
    stored: Option<(ManifestSegment, usize)>,
    /// The first offsets of the segments whose objects were not the bytes
    /// the log committed. No merge of this view reads them again.
    quarantined: BTreeSet<u64>,
    /// The first offset of the last merged segment of this view, when it
    /// came out small. Only a shard's scan merges it again, so a run that
    /// grows by a segment at a time is not rewritten once for each. A log
    /// near its limit does not wait for the scan.
    resting: Option<u64>,
    /// What this view registered for the merge it starts next, runs, or
    /// waits on the command of. No merge stores an object before the long-term
    /// log holds its registration.
    registration: Option<ObjectRegistration>,
    /// Number of the last merge this partition started. Kept across views,
    /// so the result of a merge of an earlier view is never taken for
    /// another's.
    jobs: u64,
}

impl LongTermMerges {
    fn skips(&self, sources: &[ManifestSegment]) -> bool {
        sources.iter().any(|segment| {
            self.quarantined.contains(&segment.start_offset)
                || self.resting == Some(segment.start_offset)
        })
    }

    /// Forget what this view learned and cancel the merge in flight.
    fn clear(&mut self) {
        *self = Self {
            jobs: self.jobs,
            ..Self::default()
        };
    }
}

impl<B, SB> IggyPartition<B, SB>
where
    B: MessageBus,
    SB: SuperblockStore,
{
    /// How many segments shorter the best merge of this partition makes its
    /// long-term log, and the offset that merge starts at. `None` when this
    /// replica merges nothing: it is not the primary of a partition that
    /// places its batches, a merge of its own is not settled yet, or no
    /// merge makes its log shorter.
    #[must_use]
    pub fn segment_merge_reclaim(&self) -> Option<(usize, u64)> {
        let lane = &self.long_term_merges;
        if !self.merges_long_term_segments() || lane.running.is_some() || lane.stored.is_some() {
            return None;
        }
        // The scan that asks merges what rests, and another view's
        // quarantine is not this one's.
        let quarantined = (lane.view == Some(self.consensus().view())).then_some(&lane.quarantined);
        self.best_segment_merge(|sources| {
            quarantined.is_some_and(|quarantined| {
                sources
                    .iter()
                    .any(|segment| quarantined.contains(&segment.start_offset))
            })
        })
        .map(|merge| (merge.reclaim(), merge.start_offset))
    }

    /// Whether this replica merges the partition's long-term segments and
    /// its log holds [`LONG_TERM_SEGMENTS_PRESSURE`] of them or more. Its
    /// next merge is then due at once.
    #[must_use]
    pub fn merges_under_pressure(&self) -> bool {
        self.merges_long_term_segments() && self.long_term_log_near_limit()
    }

    /// Take the next step of this partition's merges, when this replica is
    /// the primary of a partition that places its batches, no merge is in
    /// flight, and none waits for its command.
    ///
    /// A merge stores its segment only once the long-term log holds the
    /// registration of it from this view: until then the step is the
    /// registration, made at `now`, for the caller to commit. Once it is
    /// held, the step is the merge, which the caller runs under the
    /// returned scope and whose result it hands to
    /// [`Self::finish_segment_merge`] with the returned number.
    ///
    /// A merged segment whose command has not committed is settled by
    /// [`Self::settle_segment_merge`], which the caller asks first.
    ///
    /// A replica that is not the primary cancels the merge it started, so a
    /// pass over a demoted replica is what stops it.
    pub fn begin_segment_merge(
        &mut self,
        pass: MergePass,
        now: IggyTimestamp,
    ) -> Option<MergeStart> {
        if !self.merges_long_term_segments() {
            self.long_term_merges.clear();
            return None;
        }
        let partition_dir = self.partition_dir()?;
        let near_limit = self.long_term_log_near_limit();
        let lane = self.long_term_merges_of_this_view();
        if pass == MergePass::Scan || near_limit {
            lane.resting = None;
        }
        if lane.running.is_some() || lane.stored.is_some() {
            return None;
        }
        let run = match self.registered_merge() {
            Registered::Ready(run) => run,
            Registered::Pending(registration) => return Some(MergeStart::Register(registration)),
            Registered::Gone => {
                self.long_term_merges.registration = None;
                let registration = self.next_merge_registration(now)?;
                self.long_term_merges.registration = Some(registration);
                return Some(MergeStart::Register(registration));
            }
        };
        let sources = run.sources(self.long_term_segments()).to_vec();
        let prefix = crate::object_prefix(self.namespace(), self.created_revision());
        let scope = TierScope::new();
        let token = scope.token();
        let lane = &mut self.long_term_merges;
        lane.jobs += 1;
        let id = lane.jobs;
        lane.running = Some(RunningMerge {
            id,
            sources: sources.len(),
            _scope: scope,
        });
        let merge = SegmentMerge {
            prefix,
            sources,
            partition_dir,
            class: if near_limit {
                TierJobClass::Upload
            } else {
                TierJobClass::Housekeeping
            },
        };
        Some(MergeStart::Merge(id, merge, token))
    }

    /// Take the result of the merge numbered `id`, and return the segment
    /// it stored, which the caller commits to the long-term log as a
    /// replace. A result of a merge this partition does not wait for, or
    /// one that arrives after primacy moved, changes nothing.
    ///
    /// A merge that found a source the store holds other bytes for is not
    /// started again while this replica stays the primary. Any other
    /// failure is repeated by a later scan, under the same registration.
    pub fn finish_segment_merge(
        &mut self,
        id: u64,
        outcome: MergeOutcome,
    ) -> Option<ManifestSegment> {
        let awaited = self
            .long_term_merges
            .running
            .as_ref()
            .is_some_and(|running| running.id == id);
        if !awaited {
            return None;
        }
        if !self.merges_long_term_segments()
            || self.long_term_merges.view != Some(self.consensus().view())
        {
            self.long_term_merges.clear();
            return None;
        }
        let namespace_raw = self.namespace().inner();
        let lane = &mut self.long_term_merges;
        let running = lane.running.take()?;
        match outcome {
            Ok(Ok(merged)) => {
                lane.stored = Some((merged, running.sources));
                debug!(
                    namespace_raw,
                    start_offset = merged.start_offset,
                    end_offset = merged.end_offset,
                    size = merged.size,
                    sources = running.sources,
                    "stored a merged long-term segment of the partition"
                );
                Some(merged)
            }
            Ok(Err(MergeError::SourceDiverged { start_offset })) => {
                lane.quarantined.insert(start_offset);
                lane.registration = None;
                error!(
                    namespace_raw,
                    start_offset,
                    "the object of a long-term segment is not what the partition committed, \
                     and the segments around it are no longer merged"
                );
                None
            }
            Ok(Err(error)) => {
                warn!(
                    namespace_raw,
                    %error,
                    "merging long-term segments of the partition failed and is tried again"
                );
                None
            }
            Err(TierJobCancelled) => None,
        }
    }

    /// Say what became of the merged segment this partition stored, and
    /// stop waiting for one the long-term log lists or can no longer list.
    pub fn settle_segment_merge(&mut self) -> MergeSettled {
        if !self.merges_long_term_segments() {
            self.long_term_merges.clear();
            return MergeSettled::Nothing;
        }
        let Some((merged, sources)) = self.long_term_merges_of_this_view().stored else {
            return MergeSettled::Nothing;
        };
        match self.long_term_object(merged.start_offset, merged.end_offset) {
            LongTermObject::Registered | LongTermObject::Unannounced => {
                MergeSettled::Waiting(merged)
            }
            LongTermObject::Listed => {
                let lane = &mut self.long_term_merges;
                lane.stored = None;
                lane.registration = None;
                lane.resting =
                    (merged.size < LONG_TERM_MERGE_MIN_BYTES).then_some(merged.start_offset);
                MergeSettled::Committed { replaced: sources }
            }
            LongTermObject::Dead => {
                let lane = &mut self.long_term_merges;
                lane.stored = None;
                lane.registration = None;
                self.queue_dead_long_term_objects(&[(merged.start_offset, merged.end_offset)]);
                MergeSettled::Retired
            }
        }
    }

    /// Whether a merge this partition started has yet to hand its result
    /// back.
    #[must_use]
    pub fn segment_merge_running(&self) -> bool {
        self.long_term_merges.running.is_some()
    }

    /// The time before which the registrations of the long-term log expire
    /// at `now`, when this replica is the primary that merges and the log
    /// holds one it would expire. The registration of this view's own merge
    /// never expires while the merge may still store or list its object,
    /// and that of its rewrite names the next segment, which no expiry
    /// takes.
    #[must_use]
    pub fn long_term_registrations_expiry(&self, now: IggyTimestamp) -> Option<u64> {
        if !self.merges_long_term_segments() {
            return None;
        }
        let log = self.long_term_log()?;
        let lane = &self.long_term_merges;
        let held = lane
            .registration
            .filter(|_| lane.view == Some(self.consensus().view()))
            .and_then(|held| log.registration(held.start_offset, held.end_offset));
        let ttl = u64::try_from(LONG_TERM_REGISTRATION_TTL.as_micros()).unwrap_or(u64::MAX);
        let limit = now.as_micros().saturating_sub(ttl);
        let before = held.map_or(limit, |held| limit.min(held.registered_at));
        log.expires_any(before).then_some(before)
    }

    /// Whether this replica is the one that merges the partition's
    /// long-term segments: the settled primary of a partition that places
    /// its batches.
    fn merges_long_term_segments(&self) -> bool {
        self.places_batches() && self.state_machines_current()
    }

    fn long_term_log_near_limit(&self) -> bool {
        self.long_term_segments().len() >= LONG_TERM_SEGMENTS_PRESSURE
    }

    /// Where the registration of this view's next merge stands.
    fn registered_merge(&self) -> Registered {
        let (Some(registration), Some(log)) =
            (self.long_term_merges.registration, self.long_term_log())
        else {
            return Registered::Gone;
        };
        let (start_offset, end_offset) = registration.key();
        let held = match log.classify(start_offset, end_offset) {
            // A registration from an earlier view is not this one's: the
            // later time is what keeps its object from expiring.
            LongTermObject::Registered => log
                .registration(start_offset, end_offset)
                .is_some_and(|held| held.registered_at >= registration.registered_at),
            LongTermObject::Unannounced if !log.registrations_full() => false,
            _ => return Registered::Gone,
        };
        if !held {
            return Registered::Pending(registration);
        }
        MergeRun::spanning(log.segments(), registration.key())
            .map_or(Registered::Gone, Registered::Ready)
    }

    /// The registration of the best merge that skips nothing this view
    /// skips, at `now`. `None` when there is no merge, or the log takes no
    /// registration of it.
    fn next_merge_registration(&self, now: IggyTimestamp) -> Option<ObjectRegistration> {
        let lane = &self.long_term_merges;
        let run = self.best_segment_merge(|sources| lane.skips(sources))?;
        let last = run.sources(self.long_term_segments()).last()?;
        let registration = ObjectRegistration {
            start_offset: run.start_offset,
            end_offset: last.end_offset,
            registered_at: now.as_micros(),
        };
        let log = self.long_term_log()?;
        let (start_offset, end_offset) = registration.key();
        (!log.registrations_full() || log.registration(start_offset, end_offset).is_some())
            .then_some(registration)
    }

    /// The best merge of the partition's long-term segments whose sources
    /// `skipped` does not name. A log near its limit takes larger segments
    /// when it has no small ones to merge. A merge whose registration
    /// expired is left out: the log refuses its replace.
    fn best_segment_merge(&self, skipped: impl Fn(&[ManifestSegment]) -> bool) -> Option<MergeRun> {
        let segments = self.long_term_segments();
        let expired = self.long_term_log().map_or(&[][..], LongTermLog::expired);
        let skipped = |sources: &[ManifestSegment]| {
            skipped(sources)
                || sources
                    .first()
                    .zip(sources.last())
                    .is_some_and(|(first, last)| {
                        expired
                            .binary_search(&(first.start_offset, last.end_offset))
                            .is_ok()
                    })
        };
        if self.long_term_log_near_limit() {
            MergeRun::relieving(segments, skipped)
        } else {
            MergeRun::best(segments, MergeLimits::NORMAL, skipped)
        }
    }

    /// The merge state of the current view. What an earlier view left is
    /// dropped, and its merge cancelled.
    fn long_term_merges_of_this_view(&mut self) -> &mut LongTermMerges {
        let view = self.consensus().view();
        if self.long_term_merges.view != Some(view) {
            self.long_term_merges.clear();
            self.long_term_merges.view = Some(view);
        }
        &mut self.long_term_merges
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_object_store::SEGMENT_HASH_LEN;
    use std::cell::RefCell;
    use std::future::Future;

    const MIB: u64 = 1024 * 1024;

    /// A log of segments of `sizes`, one offset each.
    fn log(sizes: &[u64]) -> Vec<ManifestSegment> {
        (0..)
            .zip(sizes)
            .map(|(offset, size)| ManifestSegment {
                start_offset: offset,
                end_offset: offset,
                start_timestamp: 0,
                end_timestamp: 0,
                max_timestamp: 0,
                size: *size,
                crc32c: 0,
                hash: [0; SEGMENT_HASH_LEN],
            })
            .collect()
    }

    fn merges(sizes: &[u64]) -> Vec<(usize, usize)> {
        MergeRun::all(&log(sizes), MergeLimits::NORMAL)
            .iter()
            .map(|merge| (merge.first, merge.count))
            .collect()
    }

    fn best(sizes: &[u64]) -> Option<(usize, usize)> {
        MergeRun::best(&log(sizes), MergeLimits::NORMAL, |_| false)
            .map(|merge| (merge.first, merge.count))
    }

    #[test]
    fn given_no_two_small_segments_in_a_row_when_selecting_should_find_no_merge() {
        assert_eq!(merges(&[]), []);
        assert_eq!(merges(&[MIB]), []);
        assert_eq!(merges(&[40 * MIB, 64 * MIB]), []);
        assert_eq!(merges(&[MIB, 32 * MIB, MIB, 40 * MIB, MIB]), []);
        assert_eq!(best(&[MIB, 32 * MIB, MIB]), None);
    }

    #[test]
    fn given_a_segment_at_the_small_limit_when_selecting_should_not_merge_it() {
        assert_eq!(merges(&[MIB, LONG_TERM_MERGE_MIN_BYTES]), []);
        assert_eq!(merges(&[MIB, LONG_TERM_MERGE_MIN_BYTES - 1]), [(0, 2)]);
    }

    #[test]
    fn given_a_run_broken_by_a_healthy_segment_when_selecting_should_merge_each_side_alone() {
        let sizes = [MIB, MIB, 48 * MIB, MIB, MIB, MIB];

        let found = MergeRun::all(&log(&sizes), MergeLimits::NORMAL);

        assert_eq!(
            found,
            [
                MergeRun {
                    first: 0,
                    count: 2,
                    start_offset: 0,
                    bytes: 2 * MIB
                },
                MergeRun {
                    first: 3,
                    count: 3,
                    start_offset: 3,
                    bytes: 3 * MIB
                },
            ]
        );
    }

    #[test]
    fn given_a_run_larger_than_a_segment_when_selecting_should_stop_at_the_byte_cap() {
        // 20 + 20 + 24 fills a segment to the byte, and one more would not.
        assert_eq!(merges(&[20 * MIB, 20 * MIB, 24 * MIB, MIB, MIB]), [(0, 3)]);
        assert_eq!(merges(&[20 * MIB, 20 * MIB, 24 * MIB + 1, MIB]), [(0, 2)]);
        let found = MergeRun::all(&log(&[31 * MIB, 31 * MIB, 31 * MIB]), MergeLimits::NORMAL);
        assert_eq!(found[0].bytes, 62 * MIB);
        assert_eq!(found[0].reclaim(), 1);
    }

    #[test]
    fn given_a_run_of_more_segments_than_a_replace_takes_when_selecting_should_stop_at_the_limit() {
        let sizes = vec![1; REPLACE_SOURCES_MAX + 5];

        assert_eq!(merges(&sizes), [(0, REPLACE_SOURCES_MAX)]);
    }

    #[test]
    fn given_several_runs_when_selecting_should_take_the_one_that_shortens_the_log_most() {
        let sizes = [MIB, MIB, 48 * MIB, MIB, MIB, MIB, 48 * MIB, MIB, MIB];

        assert_eq!(best(&sizes), Some((3, 3)));
    }

    #[test]
    fn given_runs_that_shorten_the_log_alike_when_selecting_should_take_the_lowest_offset() {
        let sizes = [48 * MIB, MIB, MIB, 48 * MIB, MIB, MIB];

        assert_eq!(best(&sizes), Some((1, 2)));
    }

    #[test]
    fn given_a_skipped_segment_when_selecting_should_leave_out_the_merge_that_reads_it() {
        let sizes = [MIB, MIB, 48 * MIB, MIB, MIB, MIB];
        let segments = log(&sizes);

        let found = MergeRun::best(&segments, MergeLimits::NORMAL, |sources| {
            sources.iter().any(|segment| segment.start_offset == 4)
        });

        assert_eq!(found.map(|merge| (merge.first, merge.count)), Some((0, 2)));
    }

    fn relieving(sizes: &[u64]) -> Option<(usize, usize)> {
        MergeRun::relieving(&log(sizes), |_| false).map(|merge| (merge.first, merge.count))
    }

    #[test]
    fn given_the_normal_limits_when_doubling_should_stop_at_the_pressure_output() {
        let doublings: Vec<MergeLimits> =
            std::iter::successors(Some(MergeLimits::NORMAL), |limits| limits.doubled()).collect();

        let outputs: Vec<u64> = doublings.iter().map(|limits| limits.output_max).collect();
        assert_eq!(
            outputs,
            [64 * MIB, 128 * MIB, 256 * MIB, 512 * MIB, 1024 * MIB]
        );
        assert_eq!(outputs.last(), Some(&LONG_TERM_MERGE_PRESSURE_BYTES));
        assert!(
            doublings
                .iter()
                .all(|limits| limits.small_below * 2 == limits.output_max),
            "two small segments always fit in one"
        );
    }

    #[test]
    fn given_limits_when_selecting_should_call_small_and_fill_what_they_say() {
        let sizes = [40 * MIB, 40 * MIB, 40 * MIB, 40 * MIB, 64 * MIB, MIB, MIB];
        let limits = MergeLimits {
            small_below: 64 * MIB,
            output_max: 128 * MIB,
        };

        let found: Vec<(usize, usize)> = MergeRun::all(&log(&sizes), limits)
            .iter()
            .map(|merge| (merge.first, merge.count))
            .collect();

        assert_eq!(found, [(0, 3), (5, 2)]);
        assert_eq!(merges(&sizes), [(5, 2)]);
    }

    #[test]
    fn given_small_segments_when_relieving_should_merge_them_within_the_normal_limits() {
        let sizes = [40 * MIB, 40 * MIB, 40 * MIB, MIB, MIB];

        assert_eq!(relieving(&sizes), Some((3, 2)));
        assert_eq!(relieving(&sizes), best(&sizes));
    }

    #[test]
    fn given_only_healthy_segments_when_relieving_should_merge_within_the_first_doubling_that_can()
    {
        assert_eq!(best(&[40 * MIB, 40 * MIB, 40 * MIB, 40 * MIB]), None);
        assert_eq!(
            relieving(&[40 * MIB, 40 * MIB, 40 * MIB, 40 * MIB]),
            Some((0, 3)),
            "three fit in 128 MiB, and a fourth does not"
        );
        assert_eq!(
            relieving(&[64 * MIB, 64 * MIB, 64 * MIB, 64 * MIB, 64 * MIB]),
            Some((0, 4)),
            "a segment of 64 MiB is small from the second doubling on"
        );
        assert_eq!(
            relieving(&[300 * MIB, 300 * MIB, 300 * MIB, 300 * MIB]),
            Some((0, 3))
        );
    }

    #[test]
    fn given_segments_of_half_the_pressure_output_when_relieving_should_find_no_merge() {
        let half = LONG_TERM_MERGE_PRESSURE_BYTES / 2;

        assert_eq!(relieving(&[half, half, half]), None);
        assert_eq!(relieving(&[half - 1, half - 1, half - 1]), Some((0, 2)));
        assert_eq!(relieving(&[half - 1, half + 1, half - 1]), None);
        let merged = MergeRun::relieving(&log(&[half - 1, half - 1, half - 1]), |_| false).unwrap();
        assert!(merged.bytes <= LONG_TERM_MERGE_PRESSURE_BYTES);
    }

    #[test]
    fn given_a_skipped_segment_when_relieving_should_read_it_within_no_limits() {
        let sizes = [MIB, MIB, 40 * MIB, 40 * MIB];
        let segments = log(&sizes);

        let found = MergeRun::relieving(&segments, |sources| {
            sources.iter().any(|segment| segment.start_offset == 1)
        });

        assert_eq!(found, None);
        assert_eq!(relieving(&sizes), Some((0, 2)));
    }

    /// An executor that runs each job at once and keeps what it was asked
    /// for.
    struct Recording {
        store: Rc<iggy_object_store::MemoryStore>,
        requests: RefCell<Vec<(TierJobClass, u64)>>,
    }

    impl TierExecutor for Recording {
        type Store = iggy_object_store::MemoryStore;

        fn submit<T, F, Fut>(
            &self,
            request: TierJobRequest,
            job: F,
        ) -> iggy_object_store::TierJobHandle<T>
        where
            T: 'static,
            F: FnOnce(Rc<Self::Store>) -> Fut + 'static,
            Fut: Future<Output = T> + 'static,
        {
            self.requests
                .borrow_mut()
                .push((request.class(), request.bytes()));
            let job = job(Rc::clone(&self.store));
            iggy_object_store::TierJobHandle::new(async move { Ok(job.await) })
        }
    }

    /// The class decides whether the merge waits for an idle shard, and the
    /// bytes count against what the uploads of the shard hold at once.
    #[compio::test]
    async fn given_a_merge_when_running_it_should_ask_for_its_class_and_the_bytes_of_its_sources() {
        let merger = ExecutorLongTermMerger::new(
            Recording {
                store: Rc::new(iggy_object_store::MemoryStore::new()),
                requests: RefCell::default(),
            },
            KeyLayout::new("root").unwrap(),
        );
        let directory = tempfile::tempdir().unwrap();

        for class in [TierJobClass::Housekeeping, TierJobClass::Upload] {
            let merge = SegmentMerge {
                prefix: ObjectPrefix {
                    stream_id: 1,
                    topic_id: 2,
                    partition_id: 3,
                    created_revision: 4,
                },
                sources: log(&[3, 4]),
                partition_dir: directory.path().to_string_lossy().into_owned(),
                class,
            };
            let outcome = merger.merge(merge, TierScope::new().token()).await;
            assert!(
                matches!(outcome, Ok(Err(MergeError::Store(_)))),
                "the store holds no source"
            );
        }

        assert_eq!(
            merger.executor().requests.take(),
            [(TierJobClass::Housekeeping, 7), (TierJobClass::Upload, 7)]
        );
    }

    /// Copy an object of `bytes` that the log committed as `source`.
    async fn copied(bytes: &[u8], source: &ManifestSegment) -> Result<Vec<u8>, MergeError> {
        let store = iggy_object_store::MemoryStore::new();
        store
            .put(
                "source",
                iggy_object_store::PutBody::from_bytes(bytes.to_vec()),
                &iggy_object_store::PutOptions::default(),
            )
            .await
            .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("copy");
        let mut file = compio::fs::File::create(&path).await.unwrap();
        copy_source(&store, "source", source, &mut file, 0).await?;
        Ok(std::fs::read(path).unwrap())
    }

    /// Each of the three is checked on its own: a size, a CRC32C and a hash
    /// that the object does not have each refuse it.
    #[compio::test]
    async fn given_an_object_when_copying_a_source_should_take_it_only_with_the_committed_size_and_hashes()
     {
        let bytes = vec![7u8; 1000];
        let committed = ManifestSegment {
            start_offset: 4,
            end_offset: 9,
            start_timestamp: 0,
            end_timestamp: 0,
            max_timestamp: 0,
            size: bytes.len() as u64,
            crc32c: crc32c::crc32c(&bytes),
            hash: *blake3::hash(&bytes).as_bytes(),
        };
        let diverged = |copied: Result<Vec<u8>, MergeError>| {
            matches!(copied, Err(MergeError::SourceDiverged { start_offset: 4 }))
        };

        assert_eq!(copied(&bytes, &committed).await.unwrap(), bytes);
        let other_crc = ManifestSegment {
            crc32c: committed.crc32c ^ 1,
            ..committed
        };
        assert!(diverged(copied(&bytes, &other_crc).await));
        let other_hash = ManifestSegment {
            hash: [0; SEGMENT_HASH_LEN],
            ..committed
        };
        assert!(diverged(copied(&bytes, &other_hash).await));
        let longer = ManifestSegment {
            size: committed.size + 1,
            ..committed
        };
        assert!(diverged(copied(&bytes, &longer).await));
        let shorter = ManifestSegment {
            size: committed.size - 1,
            ..committed
        };
        assert!(diverged(copied(&bytes, &shorter).await));
    }

    fn batch(base_offset: u64, message_count: u32, body: usize) -> Vec<u8> {
        let mut header = BatchHeader::new(0, 0, (BATCH_HEADER_SIZE + body) as u64, message_count);
        header.base_offset = base_offset;
        header.base_timestamp = 1000 + base_offset;
        let mut bytes = vec![0; BATCH_HEADER_SIZE + body];
        header.encode_into(&mut bytes[..BATCH_HEADER_SIZE]);
        bytes
    }

    fn walked(start_offset: u64, log: &[u8], piece: usize) -> Result<WalkedLog, &'static str> {
        let mut walk = LogWalk::new(start_offset);
        for bytes in log.chunks(piece) {
            walk.update(bytes);
        }
        walk.finish()
    }

    #[test]
    fn given_a_log_in_pieces_of_any_size_when_walking_should_find_the_same_index_and_end() {
        let log = [batch(5, 2, 40_000), batch(7, 1, 40_000), batch(8, 3, 10)].concat();
        let mut index = SegmentIndex::default();
        for (base_offset, length) in [(5, 40_256), (7, 40_256), (8, 266)] {
            index.push(base_offset, 1000 + base_offset, length);
        }
        let index = index.into_bytes();
        assert_eq!(index.len(), 2 * crate::iggy_index::IGGY_INDEX_SIZE);

        for piece in [1, 7, 255, 256, 257, 40_000, log.len()] {
            let found = walked(5, &log, piece).unwrap();
            assert_eq!(found.end_offset, 10, "pieces of {piece}");
            assert_eq!(found.index, index, "pieces of {piece}");
            assert_eq!(found.hashes.crc32c, crc32c::crc32c(&log));
        }
    }

    #[test]
    fn given_a_log_that_is_not_a_run_of_whole_batches_when_walking_should_refuse_it() {
        let log = [batch(5, 2, 100), batch(7, 1, 100)].concat();

        assert!(walked(5, &log, 64).is_ok());
        assert!(walked(4, &log, 64).is_err(), "starts elsewhere");
        assert!(walked(5, &log[..log.len() - 1], 64).is_err(), "torn batch");
        assert!(walked(5, &log[..300], 64).is_err(), "torn header");
        assert!(walked(5, &[], 64).is_err(), "no batch");
        let gap = [batch(5, 2, 100), batch(8, 1, 100)].concat();
        assert!(walked(5, &gap, 64).is_err(), "a missing offset");
    }
}
