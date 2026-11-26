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

//! Reads of segments whose `.log` left the disk, and of fresh objects.
//!
//! A poll plan names a remote segment by an object and a byte range in it,
//! and the walk reads that range with ranged GETs from the store, through
//! the shard's read cache when it has one (see [`ReadCache`]). The cache
//! also holds the fresh objects the shard uploaded, until the long-term
//! floors of their partitions pass them.
//! The segment's sparse index is the local `.index` while one is usable, and
//! otherwise the `.index` object, fetched once and kept on disk. The fetch
//! runs in a task of its own (see [`IndexFetch`]), apart from the polls that
//! wait for it.
//!
//! The shard hands its partitions one [`RemoteSegments`] over the tier
//! service, see [`ExecutorObjects`].

use std::cell::{Cell, RefCell};
use std::ops::{Range, RangeInclusive};
use std::rc::Rc;
use std::time::Duration;

use bytes::Bytes;
use compio::io::AsyncWriteAtExt;
use futures::FutureExt;
use futures::channel::oneshot;
use futures::future::{self, Either, LocalBoxFuture, Shared};
use iggy_object_store::{
    ErrorClass, KeyLayout, LongTermKey, ObjectPrefix, ObjectRange, ObjectStore, ObjectStoreError,
    SegmentKey, SegmentObject, TierExecutor, TierJobClass, TierJobRequest, TierScope,
};
use server_common::fatal::NoteDescriptorExhaustion;
use server_common::iobuf::{Frozen, Owned};
use server_common::poll::PollHistoryId;
use server_common::sharding::IggyNamespace;
use tracing::{error, warn};

use crate::iggy_index::IGGY_INDEX_SIZE;
use crate::io::PartitionIoQuiescence;
use crate::poll_plan::DiskFault;
use crate::remote_blocks::{
    PartitionReadSettings, ReadCache, ReadGate, Reading, RemoteBlocks, RemoteReadMetrics, StoreSlot,
};
use crate::state_transfer::{STAGING_SUFFIX, fsync_dir};

/// Bytes of an index object that one GET asks for: a whole number of entries,
/// so every response can be checked on its own.
const INDEX_FETCH_CHUNK: u64 = (IGGY_INDEX_SIZE as u64) * 32 * 1024;

/// How long the store may take to answer one GET of an index object.
///
/// No poll's deadline ends an index fetch, so without a limit of its own a
/// store that stopped answering would hold the fetch, and every poll that
/// joins it, for good.
const INDEX_GET_TIMEOUT: Duration = Duration::from_secs(5);

/// The ranged read of an [`ObjectStore`], behind a pointer, so a partition
/// does not carry the provider's type.
pub trait RemoteObjects {
    /// See [`ObjectStore::get_range`].
    fn get_range<'a>(
        &'a self,
        key: &'a str,
        range: Range<u64>,
    ) -> LocalBoxFuture<'a, Result<ObjectRange, ObjectStoreError>>;

    /// As [`Self::get_range`], if `wanted` still says so when the read
    /// starts. `None` when it does not, and the store is not asked.
    fn get_range_if<'a>(
        &'a self,
        key: &'a str,
        range: Range<u64>,
        wanted: Box<dyn FnOnce() -> bool>,
    ) -> LocalBoxFuture<'a, Result<Option<ObjectRange>, ObjectStoreError>> {
        async move {
            if !wanted() {
                return Ok(None);
            }
            self.get_range(key, range).await.map(Some)
        }
        .boxed_local()
    }
}

impl<S: ObjectStore> RemoteObjects for S {
    fn get_range<'a>(
        &'a self,
        key: &'a str,
        range: Range<u64>,
    ) -> LocalBoxFuture<'a, Result<ObjectRange, ObjectStoreError>> {
        ObjectStore::get_range(self, key, range).boxed_local()
    }
}

/// Ranged reads through a tier executor, in its demand fetch class, so its
/// caps bound how many ranges, and how many bytes of them, the polls of a
/// shard hold in memory at once.
pub struct ExecutorObjects<E> {
    executor: E,
    /// Never cancelled. A read belongs to a poll, and the poll's own deadline
    /// ends it, whoever is the primary by then.
    scope: TierScope,
    class: TierJobClass,
}

impl<E> ExecutorObjects<E> {
    /// Reads that a waiting poll needs, in [`TierJobClass::DemandFetch`].
    #[must_use]
    pub fn new(executor: E) -> Self {
        Self::with_class(executor, TierJobClass::DemandFetch)
    }

    /// Reads in `class`, for work that no poll waits for.
    #[must_use]
    pub fn with_class(executor: E, class: TierJobClass) -> Self {
        Self {
            executor,
            scope: TierScope::new(),
            class,
        }
    }
}

impl<E: TierExecutor> RemoteObjects for ExecutorObjects<E> {
    fn get_range<'a>(
        &'a self,
        key: &'a str,
        range: Range<u64>,
    ) -> LocalBoxFuture<'a, Result<ObjectRange, ObjectStoreError>> {
        let request = self.request(&range);
        let key = key.to_owned();
        let job = self.executor.submit(request, move |store| async move {
            ObjectStore::get_range(&*store, &key, range).await
        });
        async move {
            job.await
                .unwrap_or_else(|cancelled| Err(ObjectStoreError::Transport(cancelled.to_string())))
        }
        .boxed_local()
    }

    /// `wanted` is asked when the job starts, which can be long after it
    /// queued.
    fn get_range_if<'a>(
        &'a self,
        key: &'a str,
        range: Range<u64>,
        wanted: Box<dyn FnOnce() -> bool>,
    ) -> LocalBoxFuture<'a, Result<Option<ObjectRange>, ObjectStoreError>> {
        let request = self.request(&range);
        let key = key.to_owned();
        let job = self.executor.submit(request, move |store| async move {
            if !wanted() {
                return Ok(None);
            }
            ObjectStore::get_range(&*store, &key, range).await.map(Some)
        });
        async move {
            job.await
                .unwrap_or_else(|cancelled| Err(ObjectStoreError::Transport(cancelled.to_string())))
        }
        .boxed_local()
    }
}

impl<E> ExecutorObjects<E> {
    fn request(&self, range: &Range<u64>) -> TierJobRequest {
        TierJobRequest::new(self.class, self.scope.token())
            .with_bytes(range.end.saturating_sub(range.start))
    }
}

/// The object that holds a segment's bytes and where they sit in it.
///
/// A whole-segment object holds them at `0..size`. The range is part of the
/// source so that data stored in objects other than whole segments reads
/// through the same plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteRange {
    pub key: SegmentKey,
    /// The segment's last offset when the keys of its objects name it after
    /// the first one, as the manifest of the cluster that stored them says.
    /// `None` for objects keyed by the first offset alone.
    pub keyed_end_offset: Option<u64>,
    pub range: Range<u64>,
}

impl RemoteRange {
    /// The name of the file a fetched `.index` of the segment takes in the
    /// partition directory.
    ///
    /// A segment keyed by both offsets carries both here too. One that takes
    /// the place of several starts where the first of them did, and its
    /// index must not pass for the index of that one.
    #[must_use]
    pub fn index_file_name(&self) -> String {
        index_file_name(self.key.start_offset, self.keyed_end_offset)
    }

    /// Bytes of segment data the range holds.
    #[must_use]
    pub const fn len(&self) -> u64 {
        self.range.end - self.range.start
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.range.end == self.range.start
    }
}

/// See [`RemoteRange::index_file_name`].
pub fn index_file_name(start_offset: u64, keyed_end_offset: Option<u64>) -> String {
    keyed_end_offset.map_or_else(
        || format!("{start_offset:0>20}.index"),
        |end_offset| format!("{start_offset:0>20}-{end_offset:0>20}.index"),
    )
}

/// Bytes between two blobs of one fresh object that a read fetches along
/// with them. A request to the store costs more than these bytes do.
pub const FRESH_READ_GAP_BYTES: u64 = 256 * 1024;

/// The most bytes one read of a fresh object asks for when it covers the
/// blobs of several batches. A single blob is read whatever its size.
pub const FRESH_READ_BYTES_MAX: u64 = 8 * 1024 * 1024;

/// `range` of an object out of `bytes`, which start at `position` of it.
fn slice_at(bytes: &Bytes, position: u64, range: &Range<u64>) -> Option<Bytes> {
    let start = usize::try_from(range.start.checked_sub(position)?).ok()?;
    let end = usize::try_from(range.end.checked_sub(position)?).ok()?;
    (end <= bytes.len()).then(|| bytes.slice(start..end))
}

/// The reads that fetch `ranges` of one object, in the order of their
/// positions: ranges within [`FRESH_READ_GAP_BYTES`] of each other in one
/// read of at most [`FRESH_READ_BYTES_MAX`], and a single range whatever its
/// size.
fn coalesced(mut ranges: Vec<Range<u64>>) -> Vec<Range<u64>> {
    ranges.sort_unstable_by_key(|range| range.start);
    let mut reads: Vec<Range<u64>> = Vec::new();
    for range in ranges {
        match reads.last_mut() {
            Some(read)
                if range.start <= read.end.saturating_add(FRESH_READ_GAP_BYTES)
                    && range.end.max(read.end) - read.start <= FRESH_READ_BYTES_MAX =>
            {
                read.end = read.end.max(range.end);
            }
            _ => reads.push(range),
        }
    }
    reads
}

/// What one fetch read of a fresh object.
#[derive(Default)]
pub struct FreshReads {
    object_id: u128,
    /// Each read with the position it starts at, in the order of the
    /// positions. No read lies inside another.
    reads: Vec<(u64, Bytes)>,
}

impl FreshReads {
    /// `range` of the object `object_id`, when one read holds all of it.
    pub(crate) fn get(&self, object_id: u128, range: &Range<u64>) -> Option<Bytes> {
        if object_id != self.object_id {
            return None;
        }
        let after = self
            .reads
            .partition_point(|(position, _)| *position <= range.start);
        let (position, bytes) = self.reads.get(after.checked_sub(1)?)?;
        slice_at(bytes, *position, range)
    }
}

/// Where the shard reads remote segments from.
pub struct RemoteSegments {
    objects: Rc<dyn RemoteObjects>,
    layout: KeyLayout,
    /// See [`INDEX_GET_TIMEOUT`].
    pub(crate) index_get_timeout: Duration,
    /// The reads through the shard's cache, `None` for reads that go to the
    /// store for exactly their range.
    pub(crate) blocks: Option<RemoteBlocks>,
}

impl RemoteSegments {
    #[must_use]
    pub fn new(objects: Rc<dyn RemoteObjects>, layout: KeyLayout) -> Self {
        Self {
            objects,
            layout,
            index_get_timeout: INDEX_GET_TIMEOUT,
            blocks: None,
        }
    }

    /// Reads of the node's own store through the tier service `executor`,
    /// which keep the fresh objects the shard uploads in `cache`, and what
    /// they fetch when the cache keeps that, fetching ahead of a poll that
    /// reads a segment in order.
    #[must_use]
    pub fn cached<E>(executor: E, layout: KeyLayout, cache: &Rc<ReadCache>) -> Self
    where
        E: TierExecutor + Clone + 'static,
    {
        let ahead = Rc::new(ExecutorObjects::with_class(
            executor.clone(),
            TierJobClass::SpeculativeFetch,
        ));
        Self::new(Rc::new(ExecutorObjects::new(executor)), layout).with_cache(ahead, cache)
    }

    /// Reads of the store of the geo peer in `store` through the shard's
    /// `cache`, once `gate` admits them: through `demand` for what a read
    /// waits for, and through `ahead` for readahead.
    #[must_use]
    pub fn through_cache(
        demand: Rc<dyn RemoteObjects>,
        ahead: Rc<dyn RemoteObjects>,
        layout: KeyLayout,
        cache: &Rc<ReadCache>,
        store: StoreSlot,
        gate: Rc<dyn ReadGate>,
    ) -> Self {
        let mut remote = Self::new(Rc::clone(&demand), layout);
        remote.blocks = Some(RemoteBlocks::new(
            Rc::clone(cache),
            store,
            gate,
            demand,
            ahead,
        ));
        remote
    }

    /// Read the node's own store through `cache`, and fetch ahead through
    /// `ahead`.
    #[must_use]
    pub fn with_cache(mut self, ahead: Rc<dyn RemoteObjects>, cache: &Rc<ReadCache>) -> Self {
        self.blocks = Some(RemoteBlocks::own(
            Rc::clone(cache),
            Rc::clone(&self.objects),
            ahead,
        ));
        self
    }

    /// The reads through the shard's cache when it keeps what they fetch.
    fn fetched_into_cache(&self) -> Option<&RemoteBlocks> {
        self.blocks
            .as_ref()
            .filter(|blocks| blocks.cache().keeps_fetched())
    }

    /// Whether the cache holds all `len` bytes of the segment in `source`
    /// from byte `position`, for the partition that reads them with
    /// `settings`, so that a read of them asks the store nothing.
    pub(crate) fn holds(
        &self,
        source: &RemoteRange,
        position: u64,
        len: usize,
        settings: PartitionReadSettings,
    ) -> bool {
        let start = source.range.start + position;
        let end = start + len as u64;
        end <= source.range.end
            && self
                .reading(source.key.prefix, settings)
                .is_some_and(|(blocks, _)| blocks.holds_segment(source, &(start..end)))
    }

    /// Drop what the cache holds of the `len` bytes of the segment in
    /// `source` from byte `position`, which are not the batches that the
    /// store holds there, so that the next read fetches them again.
    pub(crate) fn forget_cached(&self, source: &RemoteRange, position: u64, len: usize) {
        if let Some(blocks) = self.fetched_into_cache() {
            let start = source.range.start + position;
            blocks.forget_segment(source, &(start..start + len as u64));
        }
    }

    /// What the cache that these reads go through did since the last take,
    /// `None` without one.
    #[must_use]
    pub fn take_read_metrics(&self) -> Option<RemoteReadMetrics> {
        self.blocks
            .as_ref()
            .map(|blocks| blocks.cache().take_metrics())
    }

    /// Hold the whole fresh object `object_id`, which this shard has just
    /// stored, so that polls of its batches need no fetch until the
    /// long-term floors of `partitions`, the partitions with a batch in it,
    /// pass it. Bytes that the shard built and no longer shares are held
    /// without a copy, and count as the capacity of their buffer. Reads
    /// without a cache hold nothing.
    pub fn cache_fresh_object(
        &self,
        object_id: u128,
        bytes: Bytes,
        partitions: impl IntoIterator<Item = IggyNamespace>,
    ) {
        if let Some(blocks) = &self.blocks {
            blocks.hold_fresh(object_id, bytes, partitions);
        }
    }

    /// Every send of the fresh object `object_id` was answered.
    pub fn settle_fresh_object(&self, object_id: u128) {
        if let Some(blocks) = &self.blocks {
            blocks.cache().settle_fresh(object_id);
        }
    }

    /// A batch of `namespace` in the fresh object `object_id` committed, its
    /// last message at `end_offset`.
    pub(crate) fn note_fresh_commit(
        &self,
        namespace: IggyNamespace,
        object_id: u128,
        end_offset: u64,
    ) {
        if let Some(blocks) = &self.blocks {
            blocks
                .cache()
                .note_fresh_commit(namespace, object_id, end_offset);
        }
    }

    /// The long-term floor of `namespace` moved to `floor`.
    pub(crate) fn note_long_term_floor(&self, namespace: IggyNamespace, floor: u64) {
        if let Some(blocks) = &self.blocks {
            blocks.cache().passed_fresh(namespace, floor);
        }
    }

    /// Keep the end of the long-term segment that a rewrite just stored, its
    /// `.log` bytes `log` read by `segment`, and its `index`, when the cache
    /// keeps what reads fetch.
    pub(crate) fn carry_long_term(&self, segment: &RemoteRange, log: &[u8], index: &[u8]) {
        if let Some(blocks) = self.fetched_into_cache() {
            blocks.carry_segment(segment, log, index);
        }
    }

    /// Whether the fresh object `object_id` is still held for the polls of
    /// its partitions.
    #[cfg(test)]
    pub(crate) fn holds_fresh_object(&self, object_id: u128) -> bool {
        self.blocks
            .as_ref()
            .is_some_and(|blocks| blocks.cache().holds_fresh(object_id))
    }

    /// `namespace` left the shard.
    pub(crate) fn forget_partition(&self, namespace: IggyNamespace) {
        if let Some(blocks) = &self.blocks {
            blocks.cache().forget_fresh(namespace);
        }
    }

    /// The partition whose incarnation `prefix` names in this store left the
    /// shard: what its reads keep goes, and what its GETs in flight fetch is
    /// not kept.
    pub(crate) fn forget_reader(&self, prefix: ObjectPrefix) {
        if let Some(blocks) = &self.blocks {
            blocks.forget_reader(prefix);
        }
    }

    /// How the partition `prefix` names reads through the cache with
    /// `settings`, `None` without a cache that keeps what it fetches for it.
    fn reading(
        &self,
        prefix: ObjectPrefix,
        settings: PartitionReadSettings,
    ) -> Option<(&RemoteBlocks, Reading)> {
        let blocks = self.fetched_into_cache()?;
        Some((blocks, blocks.reading(prefix, settings)?))
    }

    /// The bytes at `range` of the fresh object `object_id`: the blob of one
    /// batch, from memory when this shard holds them and from the store
    /// otherwise.
    ///
    /// The reader is the partition whose incarnation `prefix` names, reading
    /// with `settings`.
    ///
    /// An error the store may not repeat is retryable. Anything else is not:
    /// a missing object, a refusal, or fewer bytes than the range.
    pub(crate) async fn read_fresh(
        &self,
        object_id: u128,
        range: Range<u64>,
        prefix: ObjectPrefix,
        settings: PartitionReadSettings,
    ) -> Result<Bytes, DiskFault> {
        if let Some(blob) = self
            .blocks
            .as_ref()
            .and_then(|blocks| blocks.held_fresh(object_id))
            .and_then(|object| slice_at(&object, 0, &range))
        {
            return Ok(blob);
        }
        if let Some((blocks, reading)) = self.reading(prefix, settings) {
            let key_of = || self.layout.fresh_key(object_id);
            return blocks.read_fresh(reading, object_id, &key_of, range).await;
        }
        self.read_fresh_from_store(object_id, range).await
    }

    /// `range` of the fresh object `object_id` when this shard holds it in
    /// memory, not counted as a read of it: a rewrite reads each blob once,
    /// and what it read is in its long-term segment from then on.
    pub(crate) fn cached_fresh(&self, object_id: u128, range: &Range<u64>) -> Option<Bytes> {
        self.blocks.as_ref()?.peek_fresh(object_id, range)
    }

    /// The bytes at `range` of the fresh object `object_id` from the store,
    /// which this shard does not keep.
    ///
    /// A rewrite that fell behind reads what no consumer at the tail asks
    /// for. Kept, those bytes would push out the ones the tail reads.
    ///
    /// # Errors
    /// As [`Self::read_fresh`].
    pub(crate) async fn read_fresh_from_store(
        &self,
        object_id: u128,
        range: Range<u64>,
    ) -> Result<Bytes, DiskFault> {
        let key = self.layout.fresh_key(object_id);
        let object = self
            .objects
            .get_range(&key, range.clone())
            .await
            .map_err(|error| fault(&key, &error))?;
        if object.bytes.len() as u64 != range.end - range.start {
            error!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                key,
                start = range.start,
                end = range.end,
                returned = object.bytes.len(),
                "poll of a placed batch: the fresh object is shorter than its placement"
            );
            return Err(DiskFault::Tiered);
        }
        Ok(object.bytes)
    }

    /// The `.index` object of the long-term segment whose `.log` `log`
    /// reads, whole and at most `cap` bytes. The index of a long-term
    /// segment never changes once stored, so a cache may keep it.
    ///
    /// An error the store may not repeat is retryable. Anything else is not:
    /// a missing object or a refusal.
    pub(crate) async fn read_long_term_index(
        &self,
        log: &RemoteRange,
        cap: u64,
        settings: PartitionReadSettings,
    ) -> Result<Bytes, DiskFault> {
        if let Some((blocks, reading)) = self.reading(log.key.prefix, settings) {
            let key_of = || self.object_key(log, SegmentObject::Index);
            return blocks.read_segment_index(reading, log, &key_of, cap).await;
        }
        let key = self.object_key(log, SegmentObject::Index);
        self.objects
            .get_range(&key, 0..cap)
            .await
            .map(|object| object.bytes)
            .map_err(|error| fault(&key, &error))
    }

    /// Fetch the blobs at `ranges` of the fresh object `object_id`, and
    /// return what was read, so a poll of many batches of one object asks
    /// the store once and not once a batch. An object this shard holds is
    /// read from memory. Otherwise blobs that sit within
    /// [`FRESH_READ_GAP_BYTES`] of each other come in one read of at most
    /// [`FRESH_READ_BYTES_MAX`], in the blocks of a cache that keeps what it
    /// fetches. The poll takes its blobs from the returned reads, which the
    /// cache may have had no room to keep. The reader is as in
    /// [`Self::read_fresh`].
    ///
    /// # Errors
    /// As [`Self::read_fresh`].
    pub(crate) async fn fetch_fresh(
        &self,
        object_id: u128,
        ranges: &[Range<u64>],
        prefix: ObjectPrefix,
        settings: PartitionReadSettings,
    ) -> Result<FreshReads, DiskFault> {
        if let Some(object) = self
            .blocks
            .as_ref()
            .and_then(|blocks| blocks.held_fresh(object_id))
        {
            return Ok(FreshReads {
                object_id,
                reads: vec![(0, object)],
            });
        }
        let missing = ranges
            .iter()
            .filter(|range| range.start < range.end)
            .cloned()
            .collect();
        let reads = coalesced(missing);
        let through_cache = self.reading(prefix, settings);
        let key_of = || self.layout.fresh_key(object_id);
        let mut fetched = FreshReads {
            object_id,
            reads: Vec::with_capacity(reads.len()),
        };
        for read in reads {
            let position = read.start;
            let bytes = match through_cache {
                Some((blocks, reading)) => {
                    blocks.read_fresh(reading, object_id, &key_of, read).await?
                }
                None => self.read_fresh_from_store(object_id, read).await?,
            };
            fetched.reads.push((position, bytes));
        }
        Ok(fetched)
    }

    /// The key of the `object` of the segment that `source` names, in the
    /// scheme its objects are keyed by.
    fn object_key(&self, source: &RemoteRange, object: SegmentObject) -> String {
        let SegmentKey {
            prefix,
            start_offset,
            ..
        } = source.key;
        source.keyed_end_offset.map_or_else(
            || {
                self.layout.segment_key(&SegmentKey {
                    prefix,
                    start_offset,
                    object,
                })
            },
            |end_offset| {
                self.layout.long_term_key(&LongTermKey {
                    prefix,
                    start_offset,
                    end_offset,
                    object,
                })
            },
        )
    }

    /// Exactly `len` bytes of the segment in `source`, from byte `position`
    /// of the segment, for the partition that reads it with `settings` and a
    /// poll that reads `poll_offset` next when it knows that offset. `next`
    /// is the segment the reader goes on to, if it is in the same store,
    /// which readahead may run on into.
    ///
    /// An error the store may not repeat is retryable. Anything else is not:
    /// a missing object, a refusal, or fewer bytes than the segment holds.
    pub(crate) async fn read(
        &self,
        source: &RemoteRange,
        position: u64,
        len: usize,
        next: Option<&RemoteRange>,
        poll_offset: Option<u64>,
        settings: PartitionReadSettings,
    ) -> Result<Frozen<4096>, DiskFault> {
        let start = source.range.start + position;
        let end = start + len as u64;
        if end > source.range.end {
            return Err(DiskFault::Tiered);
        }
        if let Some((blocks, reading)) = self.reading(source.key.prefix, settings) {
            let key_of = |source: &RemoteRange| self.object_key(source, source.key.object);
            return blocks
                .read_segment(reading, source, &key_of, start..end, next, poll_offset)
                .await;
        }
        let key = self.object_key(source, source.key.object);
        let object = self
            .objects
            .get_range(&key, start..end)
            .await
            .map_err(|error| fault(&key, &error))?;
        if object.bytes.len() != len {
            error!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                key,
                start,
                asked = len,
                returned = object.bytes.len(),
                "remote poll: the object is shorter than its segment"
            );
            return Err(DiskFault::Tiered);
        }
        Ok(Frozen::from(Owned::<4096>::copy_from_slice(&object.bytes)))
    }

    /// Fetch the `.index` object next to the `.log` in `log` and put it at
    /// `index_path`, whole or not at all.
    ///
    /// The object's own size says how long the index is. Every entry has to
    /// sit inside the segment and none may step back, so an index that would
    /// send a walk outside the segment's bytes is refused and nothing is
    /// written.
    ///
    /// Nothing is kept either once `fetch` no longer stands for the segment
    /// the plan saw: the path then belongs to another segment or to nothing.
    pub(crate) async fn fetch_index(
        &self,
        log: &RemoteRange,
        start_offset: u64,
        partition_dir: &str,
        index_path: &str,
        fetch: &IndexFetchTask,
    ) -> Result<(), DiskFault> {
        let key = self.object_key(log, SegmentObject::Index);
        let staging = format!("{index_path}.fetch{STAGING_SUFFIX}");
        let written = self
            .stage_index(&key, log.len(), start_offset, &staging)
            .await;
        if written.is_err() {
            // Best effort: the staging sweeps remove what this leaves behind.
            let _ = compio::fs::remove_file(&staging).await;
            return written;
        }
        if !fetch.may_publish() {
            let _ = compio::fs::remove_file(&staging).await;
            return Err(DiskFault::Retryable);
        }
        let renamed = compio::fs::rename(&staging, index_path).await;
        if renamed.is_ok() && fetch.segment_retired() {
            // Retention unlinked the segment's files while the rename was in
            // flight, and may have looked for this one before it existed.
            let _ = compio::fs::remove_file(index_path).await;
            return Err(DiskFault::Retryable);
        }
        let renamed = match renamed {
            Ok(()) => fsync_dir(partition_dir).await,
            Err(error) => Err(error),
        };
        renamed.map_err(|error| {
            warn!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                index_path,
                %error,
                "remote poll: could not keep the fetched index"
            );
            DiskFault::Retryable
        })
    }

    async fn stage_index(
        &self,
        key: &str,
        segment_size: u64,
        start_offset: u64,
        staging: &str,
    ) -> Result<(), DiskFault> {
        let mut file = compio::fs::File::create(staging)
            .await
            .note_descriptor_exhaustion(|| format!("creating {staging}"))
            .map_err(|error| local_fault(staging, &error))?;
        let mut check = IndexCheck::new(start_offset, segment_size);
        let mut at = 0;
        let mut whole: Option<(String, u64)> = None;
        loop {
            let get = self.objects.get_range(key, at..at + INDEX_FETCH_CHUNK);
            let object = compio::time::timeout(self.index_get_timeout, get)
                .await
                .map_err(|_| {
                    warn!(
                        target: "iggy.partitions.diag",
                        plane = "partitions",
                        key,
                        at,
                        "remote poll: the store did not answer a read of an index in time"
                    );
                    DiskFault::Retryable
                })?
                .map_err(|error| fault(key, &error))?;
            let (etag, size) = whole.get_or_insert_with(|| (object.etag.clone(), object.size));
            if *etag != object.etag || *size != object.size {
                // Another uploader replaced the object between two GETs. The
                // next poll fetches the new one whole.
                return Err(DiskFault::Retryable);
            }
            let size = *size;
            let expected = (size - at).min(INDEX_FETCH_CHUNK);
            if object.bytes.len() as u64 != expected || !check.accept(&object.bytes) {
                error!(
                    target: "iggy.partitions.diag",
                    plane = "partitions",
                    key,
                    size,
                    at,
                    "remote poll: refusing an index object that does not describe its segment"
                );
                return Err(DiskFault::Tiered);
            }
            let compio::BufResult(result, _) = file.write_all_at(object.bytes, at).await;
            result.map_err(|error| local_fault(staging, &error))?;
            at += expected;
            if at == size {
                break;
            }
        }
        file.sync_all()
            .await
            .map_err(|error| local_fault(staging, &error))
    }
}

/// Checks index entries as they arrive: offsets and positions never step
/// back, no offset is before the segment and no position is past its bytes.
pub struct IndexCheck {
    segment_size: u64,
    last_offset: u64,
    last_position: u64,
}

impl IndexCheck {
    pub(crate) const fn new(start_offset: u64, segment_size: u64) -> Self {
        Self {
            segment_size,
            last_offset: start_offset,
            last_position: 0,
        }
    }

    pub(crate) fn accept(&mut self, entries: &[u8]) -> bool {
        let (entries, rest) = entries.as_chunks::<IGGY_INDEX_SIZE>();
        if !rest.is_empty() {
            return false;
        }
        entries.iter().all(|entry| {
            let (offset, position) = (field(entry, 0), field(entry, 2));
            let ascends = offset >= self.last_offset
                && position >= self.last_position
                && position < self.segment_size;
            self.last_offset = offset;
            self.last_position = position;
            ascends
        })
    }
}

/// The `slot`th little-endian `u64` of an index entry.
fn field(entry: &[u8; IGGY_INDEX_SIZE], slot: usize) -> u64 {
    let at = slot * size_of::<u64>();
    let mut bytes = [0; size_of::<u64>()];
    bytes.copy_from_slice(&entry[at..at + size_of::<u64>()]);
    u64::from_le_bytes(bytes)
}

/// What a store that failed a read of `key` with `error` means for the poll,
/// logged: retryable for a transient error, tiered for any other.
pub fn fault(key: &str, error: &ObjectStoreError) -> DiskFault {
    warn!(
        target: "iggy.partitions.diag",
        plane = "partitions",
        key,
        %error,
        "remote poll: the store failed a read"
    );
    match error.class() {
        ErrorClass::Transient => DiskFault::Retryable,
        ErrorClass::NotFound | ErrorClass::PreconditionFailed | ErrorClass::Permanent => {
            DiskFault::Tiered
        }
    }
}

fn local_fault(path: &str, error: &std::io::Error) -> DiskFault {
    warn!(
        target: "iggy.partitions.diag",
        plane = "partitions",
        path,
        %error,
        "remote poll: could not write the fetched index"
    );
    DiskFault::Retryable
}

/// The index fetches of one partition incarnation.
///
/// A fetch runs in a task of its own and ends by putting a file in the
/// partition directory. While it runs the directory can pass to another
/// incarnation, the history can be replaced and the segment can retire, and
/// each of them reuses or frees the path the fetch writes. So teardown waits
/// for the tasks in flight, and a task starts and publishes only while its
/// segment is still the one its plan saw.
pub struct IndexFetches {
    io: Rc<PartitionIoQuiescence>,
    history: Cell<PollHistoryId>,
    /// End offset of the newest segment that retention retired from
    /// `history`.
    retired_through: Cell<Option<u64>>,
    /// How many times one segment took the place of several in `history`.
    replaces: Cell<u64>,
    /// The segments whose fetch task has not ended, by the offsets that
    /// name their index file (see [`RemoteRange::index_file_name`]), so no
    /// second task writes the same file under the first.
    running: RefCell<Vec<(u64, Option<u64>)>>,
    /// The fetches of `history` that polls can join, by start offset. They
    /// live here and not in a segment's read state, which the read cache
    /// replaces while a fetch still runs.
    joinable: RefCell<Vec<(u64, IndexFetch)>>,
}

impl IndexFetches {
    pub(crate) fn new(io: Rc<PartitionIoQuiescence>, history: PollHistoryId) -> Self {
        Self {
            io,
            history: Cell::new(history),
            retired_through: Cell::new(None),
            replaces: Cell::new(0),
            running: RefCell::default(),
            joinable: RefCell::default(),
        }
    }

    /// What a plan built now holds of the fetches.
    pub(crate) fn fence(self: &Rc<Self>) -> IndexFetchFence {
        IndexFetchFence {
            fetches: Rc::clone(self),
            history: self.history.get(),
            replaces: self.replaces.get(),
        }
    }

    /// The partition serves another history from here on, whose segments can
    /// reuse the paths of the ones before it. The fetches of the old one are
    /// cancelled, and the polls that wait for them learn that at once.
    pub(crate) fn replace_history(&self, history: PollHistoryId) {
        self.history.set(history);
        self.retired_through.set(None);
        self.replaces.set(0);
        self.joinable.borrow_mut().clear();
    }

    /// Retention retired the segments that end at or below `end_offset`.
    pub(crate) fn retire_through(&self, end_offset: u64) {
        self.retired_through
            .set(self.retired_through.get().max(Some(end_offset)));
    }

    /// One segment took the place of the segments that start in `starts`.
    ///
    /// The new segment starts where the first of them did, so a fetch that
    /// a poll could join under that offset is cancelled here. A plan built
    /// before this cannot tell which of its segments are gone, so it starts
    /// and publishes no fetch from here on. Its poll is retried, and the
    /// retry plans against the segments that are left.
    pub(crate) fn replace_segments(&self, starts: RangeInclusive<u64>) {
        self.replaces.set(self.replaces.get() + 1);
        self.joinable
            .borrow_mut()
            .retain(|(start_offset, _)| !starts.contains(start_offset));
    }
}

/// The index fetches of a plan's partition, with the history the plan was
/// built in.
#[derive(Clone)]
pub struct IndexFetchFence {
    fetches: Rc<IndexFetches>,
    history: PollHistoryId,
    /// See [`IndexFetches::replace_segments`].
    replaces: u64,
}

impl IndexFetchFence {
    /// Wait for the fetch of the index of the segment at `start_offset`,
    /// which `start` begins unless one runs. `keyed_end_offset` is that of
    /// the segment's [`RemoteRange`].
    ///
    /// `None` once the plan's partition is retiring or no longer serves the
    /// segment, and while a cancelled fetch of the same file still unwinds.
    pub(crate) fn join<Start: FnOnce(IndexFetchTask) -> IndexFetch>(
        &self,
        start_offset: u64,
        keyed_end_offset: Option<u64>,
        start: Start,
    ) -> Option<impl Future<Output = Option<Result<(), DiskFault>>> + 'static + use<Start>> {
        if !self.serves(start_offset) {
            return None;
        }
        let mut joinable = self.fetches.joinable.borrow_mut();
        // A fetch that every poll stopped waiting for leaves its entry here.
        // One that failed keeps it for the next poll of its segment to learn
        // the failure from.
        joinable.retain(|(joined, fetch)| *joined == start_offset || !fetch.succeeded());
        if let Some((_, fetch)) = joinable.iter().find(|(joined, _)| *joined == start_offset) {
            return Some(fetch.wait());
        }
        let fetch = start(self.begin_keyed(start_offset, keyed_end_offset)?);
        let wait = fetch.wait();
        joinable.push((start_offset, fetch));
        Some(wait)
    }

    /// Forget the fetch of the segment at `start_offset` once it is over, so
    /// the next poll that needs the index starts another.
    pub(crate) fn forget_finished(&self, start_offset: u64) {
        // The fetches of a later history are not this plan's to forget.
        if self.fetches.history.get() == self.history {
            self.fetches
                .joinable
                .borrow_mut()
                .retain(|(joined, fetch)| *joined != start_offset || !fetch.is_over());
        }
    }

    /// [`Self::begin_keyed`] for a segment keyed by its first offset alone.
    #[cfg(test)]
    pub(crate) fn begin(&self, start_offset: u64) -> Option<IndexFetchTask> {
        self.begin_keyed(start_offset, None)
    }

    /// The right to run the task that fetches the index of the segment at
    /// `start_offset`, whose [`RemoteRange`] has `keyed_end_offset`.
    ///
    /// `None` while another task fetches it, and once the plan's partition is
    /// retiring or no longer serves the segment.
    pub(crate) fn begin_keyed(
        &self,
        start_offset: u64,
        keyed_end_offset: Option<u64>,
    ) -> Option<IndexFetchTask> {
        let fetches = &*self.fetches;
        let file = (start_offset, keyed_end_offset);
        if !self.serves(start_offset)
            || fetches.running.borrow().contains(&file)
            || !fetches.io.begin_local_work()
        {
            return None;
        }
        fetches.running.borrow_mut().push(file);
        Some(IndexFetchTask {
            fence: self.clone(),
            start_offset,
            keyed_end_offset,
            state: Rc::default(),
        })
    }

    fn serves(&self, start_offset: u64) -> bool {
        !self.fetches.io.is_retiring() && !self.retired(start_offset)
    }

    /// The plan's history is gone, retention took the segment from it, or
    /// segments of it were replaced since the plan was built.
    fn retired(&self, start_offset: u64) -> bool {
        self.fetches.history.get() != self.history
            || self.fetches.replaces.get() != self.replaces
            || self
                .fetches
                .retired_through
                .get()
                .is_some_and(|through| start_offset <= through)
    }

    /// A fence no partition holds, for a plan built by hand.
    #[cfg(test)]
    pub(crate) fn detached() -> Self {
        Rc::new(IndexFetches::new(Rc::default(), PollHistoryId::default())).fence()
    }
}

/// The task of one fetch of a segment's index, which its partition's
/// teardown waits for.
pub struct IndexFetchTask {
    fence: IndexFetchFence,
    start_offset: u64,
    keyed_end_offset: Option<u64>,
    state: Rc<IndexFetchState>,
}

/// What an [`IndexFetch`] and its task tell each other.
#[derive(Default)]
struct IndexFetchState {
    /// The fetch was dropped, so its task publishes nothing.
    cancelled: Cell<bool>,
    /// The task put the index in place.
    succeeded: Cell<bool>,
}

impl IndexFetchTask {
    /// Whether the fetched index may still take its place in the partition
    /// directory.
    fn may_publish(&self) -> bool {
        !self.state.cancelled.get() && self.fence.serves(self.start_offset)
    }

    /// Whether retention retired the segment from the history the plan saw.
    /// A replaced history says nothing about who owns the path now.
    fn segment_retired(&self) -> bool {
        self.fence.fetches.history.get() == self.fence.history
            && self.fence.retired(self.start_offset)
    }
}

impl Drop for IndexFetchTask {
    fn drop(&mut self) {
        let fetches = &self.fence.fetches;
        fetches
            .running
            .borrow_mut()
            .retain(|file| *file != (self.start_offset, self.keyed_end_offset));
        fetches.io.settle_local_work();
    }
}

/// A segment's `.index` object on its way to the disk, in a task of its own.
///
/// The polls that need the index wait for the task, each up to its own
/// deadline, and the task goes on without them. So an index that takes longer
/// to fetch than one poll may wait still arrives, and a later poll finds it on
/// disk. Dropping the fetch cancels it: the polls still waiting learn that at
/// once, and the task publishes nothing.
///
/// The task is not aborted, because a rename it has in flight would then land
/// after its partition's teardown stopped waiting for it. It ends on its own
/// at the next check of its [`IndexFetchTask`].
pub struct IndexFetch {
    fetched: Shared<oneshot::Receiver<Result<(), DiskFault>>>,
    /// Ends when this fetch is dropped.
    dropped: Shared<oneshot::Receiver<()>>,
    _dropping: oneshot::Sender<()>,
    state: Rc<IndexFetchState>,
}

impl IndexFetch {
    /// Start [`RemoteSegments::fetch_index`] with the same arguments.
    pub(crate) fn start(
        task: IndexFetchTask,
        remote: Rc<RemoteSegments>,
        log: RemoteRange,
        partition_dir: String,
        index_path: String,
    ) -> Self {
        let (done, fetched) = oneshot::channel();
        let (dropping, dropped) = oneshot::channel();
        let state = Rc::clone(&task.state);
        compio::runtime::spawn(async move {
            let result = remote
                .fetch_index(&log, task.start_offset, &partition_dir, &index_path, &task)
                .await;
            task.state.succeeded.set(result.is_ok());
            // Teardown may go on as soon as the waiters hear of the end.
            drop(task);
            // Every poll that waited may have reached its deadline.
            let _ = done.send(result);
        })
        .detach();
        Self {
            fetched: fetched.shared(),
            dropped: dropped.shared(),
            _dropping: dropping,
            state,
        }
    }

    /// How the fetch ended, or `None` when it was cancelled.
    pub(crate) fn wait(
        &self,
    ) -> impl Future<Output = Option<Result<(), DiskFault>>> + 'static + use<> {
        let (fetched, dropped) = (self.fetched.clone(), self.dropped.clone());
        async move {
            match future::select(fetched, dropped).await {
                Either::Left((fetched, _)) => fetched.ok(),
                Either::Right(_) => None,
            }
        }
    }

    /// Whether a poll has seen the fetch end.
    pub(crate) fn is_over(&self) -> bool {
        self.fetched.peek().is_some()
    }

    fn succeeded(&self) -> bool {
        self.state.succeeded.get()
    }
}

impl Drop for IndexFetch {
    fn drop(&mut self) {
        self.state.cancelled.set(true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use iggy_object_store::{InlineTierExecutor, MemoryStore, ObjectPrefix, PutBody, PutOptions};
    use std::cell::RefCell;

    const START_OFFSET: u64 = 100;
    /// Enough entries that the index takes three GETs.
    const ENTRIES: u64 = 2 * INDEX_FETCH_CHUNK / IGGY_INDEX_SIZE as u64 + 7;
    const SEGMENT_SIZE: u64 = ENTRIES * 10;

    /// Serves one index object and stamps each answer with the next etag.
    struct Scripted {
        index: Bytes,
        etags: RefCell<Vec<&'static str>>,
        asked: RefCell<Vec<Range<u64>>>,
    }

    impl RemoteObjects for Scripted {
        fn get_range<'a>(
            &'a self,
            _key: &'a str,
            range: Range<u64>,
        ) -> LocalBoxFuture<'a, Result<ObjectRange, ObjectStoreError>> {
            self.asked.borrow_mut().push(range.clone());
            let size = self.index.len() as u64;
            let end = range.end.min(size);
            let object = ObjectRange {
                bytes: self
                    .index
                    .slice(usize::try_from(range.start).unwrap()..usize::try_from(end).unwrap()),
                etag: self.etags.borrow_mut().remove(0).to_owned(),
                size,
            };
            async move { Ok(object) }.boxed_local()
        }
    }

    fn index() -> Bytes {
        (0..ENTRIES)
            .flat_map(|entry| [START_OFFSET + entry, 1000 + entry, entry * 10])
            .flat_map(u64::to_le_bytes)
            .collect::<Vec<u8>>()
            .into()
    }

    fn log() -> RemoteRange {
        RemoteRange {
            key: SegmentKey {
                prefix: ObjectPrefix {
                    stream_id: 1,
                    topic_id: 2,
                    partition_id: 3,
                    created_revision: 4,
                },
                start_offset: START_OFFSET,
                object: SegmentObject::Log,
            },
            keyed_end_offset: None,
            range: 0..SEGMENT_SIZE,
        }
    }

    async fn fetch(
        etags: [&'static str; 3],
    ) -> (Result<(), DiskFault>, Option<Vec<u8>>, Vec<Range<u64>>) {
        let directory = tempfile::tempdir().unwrap();
        let partition_dir = directory.path().to_string_lossy().into_owned();
        let index_path = format!("{partition_dir}/{START_OFFSET:020}.index");
        let objects = Rc::new(Scripted {
            index: index(),
            etags: RefCell::new(etags.to_vec()),
            asked: RefCell::default(),
        });
        let remote = RemoteSegments::new(objects.clone(), KeyLayout::new("iggy").unwrap());
        let fence = IndexFetchFence::detached();
        let fetch = fence.begin(START_OFFSET).unwrap();
        let fetched = remote
            .fetch_index(&log(), START_OFFSET, &partition_dir, &index_path, &fetch)
            .await;
        let left = std::fs::read_dir(directory.path()).unwrap().count();
        let written = std::fs::read(&index_path).ok();
        assert_eq!(left, usize::from(written.is_some()));
        (fetched, written, objects.asked.take())
    }

    #[compio::test]
    async fn given_an_index_wider_than_one_get_when_fetching_should_write_every_entry() {
        let (fetched, written, asked) = fetch(["a", "a", "a"]).await;

        assert_eq!(fetched, Ok(()));
        assert_eq!(written.unwrap(), index());
        assert_eq!(
            asked,
            [0, 1, 2].map(|chunk| chunk * INDEX_FETCH_CHUNK..(chunk + 1) * INDEX_FETCH_CHUNK)
        );
    }

    #[compio::test]
    async fn given_an_index_replaced_between_gets_when_fetching_should_write_nothing() {
        let (fetched, written, _asked) = fetch(["a", "a", "b"]).await;

        assert_eq!(fetched, Err(DiskFault::Retryable));
        assert_eq!(written, None);
    }

    #[compio::test]
    async fn given_a_segment_retired_under_the_rename_when_fetching_should_remove_the_index() {
        let directory = tempfile::tempdir().unwrap();
        let partition_dir = directory.path().to_string_lossy().into_owned();
        let index_path = format!("{partition_dir}/{START_OFFSET:020}.index");
        let objects = Rc::new(Scripted {
            index: index(),
            etags: RefCell::new(vec!["a"; 3]),
            asked: RefCell::default(),
        });
        let remote = RemoteSegments::new(objects, KeyLayout::new("iggy").unwrap());
        let fetches = Rc::new(IndexFetches::new(Rc::default(), PollHistoryId::default()));
        let fence = fetches.fence();
        let fetch = fence.begin(START_OFFSET).unwrap();
        let log = log();
        let mut fetching =
            Box::pin(remote.fetch_index(&log, START_OFFSET, &partition_dir, &index_path, &fetch));

        // The file is in place before the fetch learns that its rename ended,
        // which is where retention can find nothing to unlink and move on.
        while !std::fs::exists(&index_path).unwrap() {
            assert!(futures::poll!(fetching.as_mut()).is_pending());
            compio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        fetches.retire_through(START_OFFSET);

        assert_eq!(fetching.await, Err(DiskFault::Retryable));
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }

    fn entries(entries: &[[u64; 3]]) -> Vec<u8> {
        entries
            .iter()
            .flatten()
            .flat_map(|field| field.to_le_bytes())
            .collect()
    }

    #[test]
    fn given_index_entries_when_checking_should_accept_only_ones_that_ascend_inside_the_segment() {
        let accepts = |index: &[[u64; 3]]| IndexCheck::new(100, 50).accept(&entries(index));

        assert!(accepts(&[[100, 7, 0], [100, 7, 0], [105, 3, 49]]));
        assert!(!accepts(&[[99, 7, 0]]));
        assert!(!accepts(&[[100, 7, 0], [104, 8, 10], [103, 9, 20]]));
        assert!(!accepts(&[[100, 7, 10], [104, 8, 9]]));
        assert!(!accepts(&[[100, 7, 50]]));
        assert!(!IndexCheck::new(100, 50).accept(&entries(&[[100, 7, 0]])[1..]));
        // The check carries over from one GET's entries to the next.
        let mut check = IndexCheck::new(100, 50);
        assert!(check.accept(&entries(&[[104, 7, 10]])));
        assert!(!check.accept(&entries(&[[103, 8, 20]])));
    }

    #[test]
    fn given_a_fetch_in_flight_when_a_second_begins_should_wait_until_the_first_ends() {
        let fence = IndexFetchFence::detached();
        let first = fence.begin(START_OFFSET).unwrap();
        assert!(fence.begin(START_OFFSET).is_none());
        assert!(fence.begin(START_OFFSET).is_none());
        // Another segment's index is another file.
        assert!(fence.begin(START_OFFSET + 1).is_some());
        drop(first);
        assert!(fence.begin(START_OFFSET).is_some());
    }

    #[test]
    fn given_a_plan_built_before_segments_were_replaced_when_fetching_should_neither_begin_nor_publish()
     {
        let fetches = Rc::new(IndexFetches::new(Rc::default(), PollHistoryId::default()));
        let before = fetches.fence();
        let running = before
            .begin_keyed(START_OFFSET, Some(START_OFFSET + 1))
            .unwrap();
        assert!(running.may_publish());

        fetches.replace_segments(START_OFFSET..=START_OFFSET + 3);

        assert!(!running.may_publish());
        assert!(
            before
                .begin_keyed(START_OFFSET + 2, Some(START_OFFSET + 3))
                .is_none(),
            "the plan cannot tell whether the segment is one of the replaced"
        );
        let after = fetches.fence();
        assert!(
            after
                .begin_keyed(START_OFFSET, Some(START_OFFSET + 1))
                .is_none(),
            "the task of the replaced segment still writes its file"
        );
        assert!(
            after
                .begin_keyed(START_OFFSET, Some(START_OFFSET + 3))
                .is_some(),
            "the segment that took their place starts where the first did"
        );
        drop(running);
        assert!(
            after
                .begin_keyed(START_OFFSET, Some(START_OFFSET + 1))
                .is_some()
        );
    }

    #[test]
    fn given_a_plan_whose_segment_is_gone_when_fetching_should_neither_begin_nor_publish() {
        let io = Rc::<PartitionIoQuiescence>::default();
        let history = PollHistoryId::default();
        let fetches = Rc::new(IndexFetches::new(Rc::clone(&io), history));
        let fence = fetches.fence();

        let fetch = fence.begin(START_OFFSET).unwrap();
        assert!(fetch.may_publish());
        fetches.retire_through(START_OFFSET - 1);
        assert!(fetch.may_publish() && !fetch.segment_retired());
        fetches.retire_through(START_OFFSET);
        assert!(!fetch.may_publish() && fetch.segment_retired());
        drop(fetch);
        assert!(fence.begin(START_OFFSET).is_none());
        assert!(fence.begin(START_OFFSET + 1).is_some());

        let fetch = fence.begin(START_OFFSET + 1).unwrap();
        fetches.replace_history(PollHistoryId::default());
        // The new history may hold another segment at the path, so the fetch
        // must not unlink it either.
        assert!(!fetch.may_publish() && !fetch.segment_retired());
        drop(fetch);
        assert!(fence.begin(START_OFFSET + 1).is_none());
        assert!(fetches.fence().begin(START_OFFSET).is_some());

        let fence = fetches.fence();
        let fetch = fence.begin(START_OFFSET).unwrap();
        io.retire();
        assert!(!fetch.may_publish() && !fetch.segment_retired());
        drop(fetch);
        assert!(fence.begin(START_OFFSET).is_none());
    }

    #[compio::test]
    async fn given_a_tier_executor_when_reading_a_range_should_read_it_from_the_executor_s_store() {
        let executor = InlineTierExecutor::new(MemoryStore::new());
        executor
            .store()
            .put(
                "iggy/1/1/0/0/00000000000000000000.log",
                PutBody::from_bytes(Bytes::from_static(b"0123456789")),
                &PutOptions::default(),
            )
            .await
            .unwrap();
        let objects = ExecutorObjects::new(executor);

        let read = objects
            .get_range("iggy/1/1/0/0/00000000000000000000.log", 2..6)
            .await
            .unwrap();
        let missing = objects.get_range("iggy/1/1/0/0/missing.log", 0..1).await;

        assert_eq!(read.bytes.as_ref(), b"2345");
        assert_eq!(read.size, 10);
        assert!(matches!(missing, Err(ObjectStoreError::NotFound { .. })));
    }

    /// One object, and the ranges it was asked for.
    struct Asked {
        object: Bytes,
        ranges: RefCell<Vec<Range<u64>>>,
    }

    impl Asked {
        fn holding(len: usize) -> Rc<Self> {
            let object: Vec<u8> = (0..len)
                .map(|byte| u8::try_from(byte % 251).unwrap())
                .collect();
            Rc::new(Self {
                object: Bytes::from(object),
                ranges: RefCell::default(),
            })
        }
    }

    impl RemoteObjects for Asked {
        fn get_range<'a>(
            &'a self,
            _key: &'a str,
            range: Range<u64>,
        ) -> LocalBoxFuture<'a, Result<ObjectRange, ObjectStoreError>> {
            self.ranges.borrow_mut().push(range.clone());
            let end = usize::try_from(range.end).unwrap().min(self.object.len());
            let object = ObjectRange {
                bytes: self
                    .object
                    .slice(usize::try_from(range.start).unwrap()..end),
                etag: String::new(),
                size: self.object.len() as u64,
            };
            async move { Ok(object) }.boxed_local()
        }
    }

    /// Without a cache, a poll's blobs of one object still cost one read
    /// when they are close together, and the poll takes them from what it
    /// read.
    #[compio::test]
    async fn given_blobs_close_together_and_no_cache_when_fetching_them_should_ask_the_store_once_and_hand_back_what_it_read()
     {
        let store = Asked::holding(4096);
        let remote = RemoteSegments::new(store.clone(), KeyLayout::new("iggy").unwrap());
        let blobs = [100..200, 200..300, 1000..1100];

        let fetched = remote
            .fetch_fresh(7, &blobs, log().key.prefix, PartitionReadSettings::SHARD)
            .await
            .unwrap();

        assert_eq!(store.ranges.borrow().len(), 1);
        assert_eq!(store.ranges.borrow()[0], 100..1100);
        for blob in blobs {
            let expected = store
                .object
                .slice(usize::try_from(blob.start).unwrap()..usize::try_from(blob.end).unwrap());
            assert_eq!(fetched.get(7, &blob).unwrap(), expected);
        }
        // Another object, and a range the read does not hold.
        assert_eq!(fetched.get(8, &(100..200)), None);
        assert_eq!(fetched.get(7, &(50..150)), None);
    }

    #[compio::test]
    async fn given_blobs_far_apart_or_too_many_bytes_when_fetching_should_ask_for_each_run() {
        let gap = FRESH_READ_GAP_BYTES;
        let far = 100 + gap + 1;
        let store = Asked::holding(usize::try_from(far + FRESH_READ_BYTES_MAX + 200).unwrap());
        let remote = RemoteSegments::new(store.clone(), KeyLayout::new("iggy").unwrap());
        // The third blob would make the second read longer than a read may be.
        let third = far + FRESH_READ_BYTES_MAX;
        let blobs = [third..third + 100, 0..100, far..far + 100];

        remote
            .fetch_fresh(7, &blobs, log().key.prefix, PartitionReadSettings::SHARD)
            .await
            .unwrap();

        assert_eq!(
            *store.ranges.borrow(),
            [0..100, far..far + 100, third..third + 100]
        );
    }

    #[compio::test]
    async fn given_an_object_shorter_than_its_blobs_when_fetching_should_fail_the_read() {
        let store = Asked::holding(250);
        let remote = RemoteSegments::new(store, KeyLayout::new("iggy").unwrap());

        assert_eq!(
            remote
                .fetch_fresh(
                    7,
                    &[100..200, 200..300],
                    log().key.prefix,
                    PartitionReadSettings::SHARD
                )
                .await
                .err(),
            Some(DiskFault::Tiered)
        );
    }

    /// A store that never answers.
    struct Silent;

    impl RemoteObjects for Silent {
        fn get_range<'a>(
            &'a self,
            _key: &'a str,
            _range: Range<u64>,
        ) -> LocalBoxFuture<'a, Result<ObjectRange, ObjectStoreError>> {
            futures::future::pending().boxed_local()
        }
    }

    #[compio::test]
    async fn given_a_store_that_never_answers_when_fetching_should_give_up_and_write_nothing() {
        let directory = tempfile::tempdir().unwrap();
        let partition_dir = directory.path().to_string_lossy().into_owned();
        let index_path = format!("{partition_dir}/{START_OFFSET:020}.index");
        let mut remote = RemoteSegments::new(Rc::new(Silent), KeyLayout::new("iggy").unwrap());
        remote.index_get_timeout = Duration::from_millis(20);

        let fence = IndexFetchFence::detached();
        let fetch = fence.begin(START_OFFSET).unwrap();
        let fetched = remote
            .fetch_index(&log(), START_OFFSET, &partition_dir, &index_path, &fetch)
            .await;

        assert_eq!(fetched, Err(DiskFault::Retryable));
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }

    /// A fetch of the scripted index, with its directory and where it writes.
    fn start_fetch() -> (tempfile::TempDir, String, IndexFetch) {
        let directory = tempfile::tempdir().unwrap();
        let partition_dir = directory.path().to_string_lossy().into_owned();
        let index_path = format!("{partition_dir}/{START_OFFSET:020}.index");
        let objects = Rc::new(Scripted {
            index: index(),
            etags: RefCell::new(vec!["a"; 3]),
            asked: RefCell::default(),
        });
        let remote = RemoteSegments::new(objects, KeyLayout::new("iggy").unwrap());
        let fetch = IndexFetch::start(
            IndexFetchFence::detached().begin(START_OFFSET).unwrap(),
            Rc::new(remote),
            log(),
            partition_dir,
            index_path.clone(),
        );
        (directory, index_path, fetch)
    }

    #[compio::test]
    async fn given_a_started_fetch_when_no_poll_waits_should_still_write_the_index() {
        let (_directory, index_path, fetch) = start_fetch();
        let abandoned = fetch.wait();
        drop(abandoned);
        assert!(!fetch.is_over());

        // The task is not polled through any waiter, only by the runtime.
        for _turn in 0..5_000 {
            if std::path::Path::new(&index_path).exists() {
                break;
            }
            compio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(std::fs::read(&index_path).unwrap(), index());
        assert_eq!(fetch.wait().await, Some(Ok(())));
        assert_eq!(fetch.wait().await, Some(Ok(())));
        assert!(fetch.is_over());
    }

    #[compio::test]
    async fn given_a_started_fetch_when_it_is_dropped_should_cancel_and_tell_its_waiters() {
        let (_directory, index_path, fetch) = start_fetch();
        let waiter = fetch.wait();
        drop(fetch);

        assert_eq!(waiter.await, None);
        compio::time::sleep(Duration::from_millis(50)).await;
        assert!(!std::path::Path::new(&index_path).exists());
    }
}
