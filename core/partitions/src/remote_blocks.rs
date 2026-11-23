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

//! The `.log` objects of remote segments in blocks, kept in memory by the
//! shard that read them, with readahead in front of the polls that read a
//! segment in order.
//!
//! A poll reads a segment in chunks, and each chunk starts again at the batch
//! that the one before it cut. A poll of a later offset can walk again from
//! the same index entry. In blocks, the bytes read again come from memory,
//! and the polls that miss the same block at once share one GET.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, VecDeque};
use std::ops::{Range, RangeInclusive};
use std::rc::Rc;

use bytes::Bytes;
use futures::FutureExt;
use futures::channel::oneshot;
use futures::future::Shared;
use iggy_object_store::{Admission, ByteCache, Lookup, SegmentKey};
use server_common::iobuf::{Frozen, Owned};
use tracing::error;

use crate::poll_plan::DiskFault;
use crate::remote_segments::{RemoteObjects, RemoteRange, fault};

/// Blocks that the first readahead in front of a reader fetches. Each
/// sequential read after it doubles them, up to
/// [`ReadCacheSettings::read_ahead`].
pub const READ_AHEAD_START_BLOCKS: u64 = 2;

/// Readers that a shard follows through their objects. Past it, the one that
/// read the longest ago is forgotten.
const READERS_MAX: usize = 256;

/// Readahead fetches that one shard has in flight at most. The tier service
/// starts the fetches of a class in the order they came, and one queued
/// behind many others can start after its reader went past its blocks. The
/// default of `tiered_storage.jobs.speculative_fetches`, so at the defaults
/// none of them queues.
const READ_AHEAD_FETCHES_MAX: usize = 4;

/// How a shard keeps the `.log` objects that it reads from its own store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadCacheSettings {
    /// Bytes of blocks that the shard keeps at most. With 0 it keeps none,
    /// and each read asks the store for exactly its range.
    pub cache_bytes: usize,
    /// Bytes of one block. The blocks of an object start at its byte 0, and
    /// its last block is shorter.
    pub block_bytes: u64,
    /// Bytes that readahead fetches in front of a sequential reader at most,
    /// in whole blocks. 0 turns readahead off.
    pub read_ahead: u64,
}

/// What the block cache of a shard did since the last
/// [`crate::RemoteSegments::take_read_metrics`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RemoteReadMetrics {
    /// Blocks that reads took from the cache.
    pub block_hits: u64,
    /// Blocks that reads fetched, or waited for while another read fetched
    /// them.
    pub block_misses: u64,
    /// First reads of blocks that readahead fetched.
    pub read_ahead_hits: u64,
    /// Blocks that readahead fetched.
    pub blocks_fetched_ahead: u64,
    /// Bytes of the GETs that reads waited for.
    pub demand_bytes: u64,
    /// Bytes of the GETs of readahead.
    pub ahead_bytes: u64,
    /// Bytes that the cache holds now.
    pub cached_bytes: u64,
}

/// The blocks of the `.log` objects that one shard read from its own store.
pub struct RemoteBlocks {
    demand: Rc<dyn RemoteObjects>,
    /// Reads for readahead, which no read may wait for: their class starts
    /// only while no demand fetch waits.
    ahead: Rc<dyn RemoteObjects>,
    cache: RefCell<ByteCache<BlockKey>>,
    /// The GETs that reads wait for, under each block they fetch.
    fetching: RefCell<BTreeMap<BlockKey, RunFetch>>,
    /// The GETs of readahead, under each block they fetch.
    fetching_ahead: RefCell<BTreeMap<BlockKey, RunFetch>>,
    ahead_fetches: Cell<usize>,
    block_bytes: u64,
    /// Blocks that readahead keeps in front of a reader at most.
    ahead_blocks_max: u64,
    readers: RefCell<Readers>,
    counted: Cell<RemoteReadMetrics>,
}

impl RemoteBlocks {
    /// Blocks fetched through `demand`, and readahead through `ahead`, as
    /// `settings` say.
    pub(crate) fn new(
        demand: Rc<dyn RemoteObjects>,
        ahead: Rc<dyn RemoteObjects>,
        settings: ReadCacheSettings,
    ) -> Self {
        let block_bytes = settings.block_bytes.max(1);
        Self {
            demand,
            ahead,
            cache: RefCell::new(ByteCache::new(settings.cache_bytes)),
            fetching: RefCell::default(),
            fetching_ahead: RefCell::default(),
            ahead_fetches: Cell::new(0),
            block_bytes,
            ahead_blocks_max: settings.read_ahead / block_bytes,
            readers: RefCell::default(),
            counted: Cell::default(),
        }
    }

    /// Exactly `range` of the object that `source` reads and `key` names:
    /// the blocks it covers from the cache, and the others with one GET for
    /// each run of adjacent ones.
    pub(crate) async fn read(
        self: &Rc<Self>,
        source: &RemoteRange,
        key: &str,
        range: Range<u64>,
    ) -> Result<Frozen<4096>, DiskFault> {
        let object = ObjectId::new(source);
        let Some(blocks) = object.blocks_of(&range, self.block_bytes) else {
            return Ok(Frozen::from(Owned::<4096>::with_capacity(0)));
        };
        let first = *blocks.start();
        let last = *blocks.end();
        let follows = self.readers.borrow_mut().follow(object, &range);
        let (mut found, sequential) = self.look_up(object, blocks, follows.as_ref());
        let window = if sequential {
            self.next_window(follows.map_or(0, |last_read| last_read.window))
        } else {
            0
        };
        self.readers.borrow_mut().record(Expectation {
            object,
            read: range.clone(),
            window,
        });
        let waits = self.fetch_missing(object, key, first, &found);
        self.read_ahead(object, key, last, window);
        for (slot, fetch) in waits {
            found[slot] = Some(fetch.block(first + slot as u64).await?);
        }
        assemble(&found, first, self.block_bytes, &range)
    }

    /// What the cache did since the last take, and the bytes it holds.
    pub(crate) fn take_metrics(&self) -> RemoteReadMetrics {
        RemoteReadMetrics {
            cached_bytes: self.cache.borrow().held() as u64,
            ..self.counted.take()
        }
    }

    /// The cached ones of `blocks`, `None` for the others, and whether the
    /// read goes on where a reader was expected to.
    fn look_up(
        &self,
        object: ObjectId,
        blocks: RangeInclusive<u64>,
        follows: Option<&Expectation>,
    ) -> (Vec<Option<Bytes>>, bool) {
        let first = *blocks.start();
        // The reader's last read counted these. Counted again, every block
        // of one pass would look like one that readers came back to.
        let read_through = follows.map(|last_read| last_read.last_block(self.block_bytes));
        let mut sequential = follows.is_some();
        let mut found = Vec::with_capacity(usize::try_from(blocks.end() - first + 1).unwrap_or(0));
        let mut counted = self.counted.get();
        let mut cache = self.cache.borrow_mut();
        for index in blocks {
            let key = BlockKey { object, index };
            let bytes = if read_through.is_some_and(|through| index <= through) {
                cache.peek(&key)
            } else {
                match cache.read(&key) {
                    Lookup::Hit(bytes) => Some(bytes),
                    Lookup::AheadHit(bytes) => {
                        counted.read_ahead_hits += 1;
                        sequential |= index == first;
                        Some(bytes)
                    }
                    Lookup::Miss => None,
                }
            };
            if bytes.is_some() {
                counted.block_hits += 1;
            } else {
                counted.block_misses += 1;
            }
            found.push(bytes);
        }
        self.counted.set(counted);
        (found, sequential)
    }

    /// The blocks that readahead keeps in front of a sequential reader whose
    /// last readahead kept `last_window` there.
    fn next_window(&self, last_window: u64) -> u64 {
        let window = if last_window == 0 {
            READ_AHEAD_START_BLOCKS
        } else {
            last_window.saturating_mul(2)
        };
        window.min(self.ahead_blocks_max)
    }

    /// Start the GETs of the missing blocks of `found`, which starts at block
    /// `first`, or join the ones that fetch them already. Returns where each
    /// missing block goes in `found` and the GET it comes with.
    fn fetch_missing(
        self: &Rc<Self>,
        object: ObjectId,
        key: &str,
        first: u64,
        found: &[Option<Bytes>],
    ) -> Vec<(usize, RunFetch)> {
        let mut waits = Vec::new();
        let mut unfetched: Vec<(usize, u64)> = Vec::new();
        {
            let fetching = self.fetching.borrow();
            let missing = (first..)
                .zip(found)
                .enumerate()
                .filter(|(_, (_, bytes))| bytes.is_none());
            for (slot, (index, _)) in missing {
                match fetching.get(&BlockKey { object, index }) {
                    Some(fetch) => waits.push((slot, fetch.clone())),
                    None => unfetched.push((slot, index)),
                }
            }
        }
        for run in unfetched.chunk_by(|(_, before), (_, after)| before + 1 == *after) {
            let (Some((_, start)), Some((_, end))) = (run.first(), run.last()) else {
                continue;
            };
            let fetch = self.start_fetch(FetchKind::Demand, object, key, *start..end + 1);
            waits.extend(run.iter().map(|(slot, _)| (*slot, fetch.clone())));
        }
        waits
    }

    /// Fetch the `window` blocks after block `last` of `object` that no
    /// cache or GET holds, unless too many readahead fetches are in flight.
    fn read_ahead(self: &Rc<Self>, object: ObjectId, key: &str, last: u64, window: u64) {
        let through = last
            .saturating_add(window)
            .min(object.last_block(self.block_bytes));
        let missing: Vec<u64> = {
            let cache = self.cache.borrow();
            let fetching = self.fetching.borrow();
            let fetching_ahead = self.fetching_ahead.borrow();
            (last + 1..=through)
                .filter(|index| {
                    let key = BlockKey {
                        object,
                        index: *index,
                    };
                    !cache.holds(&key)
                        && !fetching.contains_key(&key)
                        && !fetching_ahead.contains_key(&key)
                })
                .collect()
        };
        for run in missing.chunk_by(|before, after| before + 1 == *after) {
            let (Some(start), Some(end)) = (run.first(), run.last()) else {
                continue;
            };
            if self.ahead_fetches.get() >= READ_AHEAD_FETCHES_MAX {
                return;
            }
            self.start_fetch(FetchKind::Ahead, object, key, *start..end + 1);
        }
    }

    /// Start one GET of the blocks `run` in a task of its own, which keeps
    /// what it fetched whether anyone still waits for it or not.
    fn start_fetch(
        self: &Rc<Self>,
        kind: FetchKind,
        object: ObjectId,
        key: &str,
        run: Range<u64>,
    ) -> RunFetch {
        let (done, fetched) = oneshot::channel();
        let fetch = RunFetch {
            first: run.start,
            fetched: fetched.shared(),
        };
        let flight = Flight::begin(Rc::clone(self), kind, object, run, &fetch);
        let key = key.to_owned();
        compio::runtime::spawn(async move {
            let outcome = flight.fetch(&key).await;
            // Every read that waited may have reached its deadline.
            let _ = done.send(outcome);
        })
        .detach();
        fetch
    }

    const fn flights(&self, kind: FetchKind) -> &RefCell<BTreeMap<BlockKey, RunFetch>> {
        match kind {
            FetchKind::Demand => &self.fetching,
            FetchKind::Ahead => &self.fetching_ahead,
        }
    }

    /// Keep the blocks `run` of `object` that a GET of `kind` fetched.
    fn keep(&self, kind: FetchKind, object: ObjectId, run: Range<u64>, blocks: &[Bytes]) {
        let mut cache = self.cache.borrow_mut();
        for (index, bytes) in run.zip(blocks) {
            let key = BlockKey { object, index };
            match kind {
                // Readahead fetched the block meanwhile. The read that this
                // GET was for is its first.
                FetchKind::Demand if cache.holds(&key) => {
                    cache.read(&key);
                }
                FetchKind::Demand => {
                    cache.insert(key, bytes.clone(), bytes.len(), Admission::Read);
                }
                FetchKind::Ahead => {
                    cache.insert(key, bytes.clone(), bytes.len(), Admission::Ahead);
                }
            }
        }
        drop(cache);
        let fetched: u64 = blocks.iter().map(|bytes| bytes.len() as u64).sum();
        let mut counted = self.counted.get();
        match kind {
            FetchKind::Demand => counted.demand_bytes += fetched,
            FetchKind::Ahead => {
                counted.blocks_fetched_ahead += blocks.len() as u64;
                counted.ahead_bytes += fetched;
            }
        }
        self.counted.set(counted);
    }
}

/// A segment object as the cache names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ObjectId {
    key: SegmentKey,
    keyed_end_offset: Option<u64>,
    /// Where the bytes that reads take end, which is where the last block
    /// ends.
    end: u64,
}

impl ObjectId {
    const fn new(source: &RemoteRange) -> Self {
        Self {
            key: source.key,
            keyed_end_offset: source.keyed_end_offset,
            end: source.range.end,
        }
    }

    /// The blocks of `block_bytes` that hold `range`, `None` for an empty
    /// one.
    fn blocks_of(&self, range: &Range<u64>, block_bytes: u64) -> Option<RangeInclusive<u64>> {
        (range.start < range.end.min(self.end))
            .then(|| range.start / block_bytes..=(range.end - 1) / block_bytes)
    }

    const fn last_block(&self, block_bytes: u64) -> u64 {
        self.end.saturating_sub(1) / block_bytes
    }

    /// The bytes of the blocks `run` of `block_bytes`.
    fn bytes_of(&self, run: &Range<u64>, block_bytes: u64) -> Range<u64> {
        let start = run.start.saturating_mul(block_bytes);
        start..run.end.saturating_mul(block_bytes).min(self.end)
    }
}

/// One block of an object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct BlockKey {
    object: ObjectId,
    index: u64,
}

/// How a GET ended: the blocks it fetched, the first one first.
type RunOutcome = Result<Rc<[Bytes]>, DiskFault>;

/// A GET of a run of adjacent blocks, in a task of its own.
#[derive(Clone)]
struct RunFetch {
    /// The first block of the run.
    first: u64,
    fetched: Shared<oneshot::Receiver<RunOutcome>>,
}

impl RunFetch {
    /// Block `index` of the run, once the GET ends.
    async fn block(self, index: u64) -> Result<Bytes, DiskFault> {
        // The sender goes only with a task that its runtime dropped.
        let blocks = self.fetched.await.map_err(|_| DiskFault::Retryable)??;
        index
            .checked_sub(self.first)
            .and_then(|at| usize::try_from(at).ok())
            .and_then(|at| blocks.get(at).cloned())
            .ok_or(DiskFault::Retryable)
    }
}

/// Who a GET is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FetchKind {
    /// Reads that wait for it.
    Demand,
    /// Readahead, which no read waits for.
    Ahead,
}

/// One GET, listed under each block it fetches until its task ends.
struct Flight {
    blocks: Rc<RemoteBlocks>,
    kind: FetchKind,
    object: ObjectId,
    run: Range<u64>,
}

impl Flight {
    fn begin(
        blocks: Rc<RemoteBlocks>,
        kind: FetchKind,
        object: ObjectId,
        run: Range<u64>,
        fetch: &RunFetch,
    ) -> Self {
        {
            let mut flights = blocks.flights(kind).borrow_mut();
            for index in run.clone() {
                flights.insert(BlockKey { object, index }, fetch.clone());
            }
        }
        if kind == FetchKind::Ahead {
            blocks.ahead_fetches.set(blocks.ahead_fetches.get() + 1);
        }
        Self {
            blocks,
            kind,
            object,
            run,
        }
    }

    /// GET the run from the object `key` and keep its blocks. Nothing of a
    /// failed or short GET is kept.
    async fn fetch(self, key: &str) -> RunOutcome {
        let block_bytes = self.blocks.block_bytes;
        let range = self.object.bytes_of(&self.run, block_bytes);
        let objects = match self.kind {
            FetchKind::Demand => &self.blocks.demand,
            FetchKind::Ahead => &self.blocks.ahead,
        };
        let fetched = match objects.get_range(key, range.clone()).await {
            Ok(fetched) => fetched.bytes,
            Err(error) if self.kind == FetchKind::Demand => return Err(fault(key, &error)),
            Err(_) => return Err(DiskFault::Retryable),
        };
        if fetched.len() as u64 != range.end - range.start {
            if self.kind == FetchKind::Demand {
                error!(
                    target: "iggy.partitions.diag",
                    plane = "partitions",
                    key,
                    start = range.start,
                    asked = range.end - range.start,
                    returned = fetched.len(),
                    "remote poll: the object is shorter than its segment"
                );
            }
            return Err(DiskFault::Tiered);
        }
        // One allocation for each block, so evicting a block frees its bytes.
        let block_len = usize::try_from(block_bytes).unwrap_or(usize::MAX);
        let blocks: Rc<[Bytes]> = fetched
            .chunks(block_len)
            .map(Bytes::copy_from_slice)
            .collect();
        self.blocks
            .keep(self.kind, self.object, self.run.clone(), &blocks);
        Ok(blocks)
    }
}

impl Drop for Flight {
    fn drop(&mut self) {
        let mut flights = self.blocks.flights(self.kind).borrow_mut();
        for index in self.run.clone() {
            flights.remove(&BlockKey {
                object: self.object,
                index,
            });
        }
        if self.kind == FetchKind::Ahead {
            self.blocks
                .ahead_fetches
                .set(self.blocks.ahead_fetches.get().saturating_sub(1));
        }
    }
}

/// The last read of a reader, which its next read goes on from when it reads
/// in order.
#[derive(Debug, Clone)]
struct Expectation {
    object: ObjectId,
    read: Range<u64>,
    /// Blocks that readahead keeps in front of the reader, 0 for none.
    window: u64,
}

impl Expectation {
    /// The next read of a sequential reader starts no earlier than this one
    /// did and no later than where it ended, and starts later or ends later.
    /// A poll that goes on reads again the batch this one cut, and a poll of
    /// a later offset walks again from the same index entry.
    fn continued_by(&self, object: ObjectId, read: &Range<u64>) -> bool {
        self.object == object
            && (self.read.start..=self.read.end).contains(&read.start)
            && (read.start > self.read.start || read.end > self.read.end)
    }

    const fn last_block(&self, block_bytes: u64) -> u64 {
        self.read.end.saturating_sub(1) / block_bytes
    }
}

/// The readers expected to read on, the one that read last at the back.
#[derive(Default)]
struct Readers {
    expected: VecDeque<Expectation>,
}

impl Readers {
    /// The expectation that a read of `read` of `object` meets, which leaves
    /// the table.
    fn follow(&mut self, object: ObjectId, read: &Range<u64>) -> Option<Expectation> {
        let at = self
            .expected
            .iter()
            .rposition(|expectation| expectation.continued_by(object, read))?;
        self.expected.remove(at)
    }

    fn record(&mut self, expectation: Expectation) {
        if self.expected.len() >= READERS_MAX {
            self.expected.pop_front();
        }
        self.expected.push_back(expectation);
    }
}

/// `range` of an object out of its blocks of `block_bytes` in `found`, the
/// first of which is block `first`.
fn assemble(
    found: &[Option<Bytes>],
    first: u64,
    block_bytes: u64,
    range: &Range<u64>,
) -> Result<Frozen<4096>, DiskFault> {
    let len = usize::try_from(range.end - range.start).map_err(|_| DiskFault::Tiered)?;
    let mut owned = Owned::<4096>::with_capacity(len);
    for (index, block) in (first..).zip(found) {
        let block = block.as_ref().ok_or(DiskFault::Retryable)?;
        let piece = piece(block, index * block_bytes, range).ok_or(DiskFault::Tiered)?;
        owned.extend_from_slice(piece);
    }
    if owned.as_slice().len() != len {
        return Err(DiskFault::Tiered);
    }
    Ok(Frozen::from(owned))
}

/// The part of `range` of an object that `block`, which starts at byte `at`
/// of it, holds.
fn piece<'a>(block: &'a [u8], at: u64, range: &Range<u64>) -> Option<&'a [u8]> {
    let from = range.start.max(at) - at;
    let to = range.end.min(at + block.len() as u64).saturating_sub(at);
    block.get(usize::try_from(from).ok()?..usize::try_from(to).ok()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    use futures::future::{self, LocalBoxFuture};
    use iggy_object_store::{
        KeyLayout, MemoryStore, ObjectPrefix, ObjectRange, ObjectStore, ObjectStoreError, PutBody,
        PutOptions, SegmentObject, TierExecutor, TierJobClass, TierJobHandle, TierJobRequest,
    };

    use crate::RemoteSegments;

    const BLOCK_LEN: usize = 256 * 1024;
    const BLOCK: u64 = BLOCK_LEN as u64;
    /// Five whole blocks and a short one.
    const OBJECT_LEN: u64 = 5 * BLOCK + 1000;
    const CACHE_BYTES: usize = 64 * 1024 * 1024;
    const READ_AHEAD: u64 = 8 * BLOCK;

    fn layout() -> KeyLayout {
        KeyLayout::new("iggy").unwrap()
    }

    fn source_of(start_offset: u64, len: u64) -> RemoteRange {
        RemoteRange {
            key: SegmentKey {
                prefix: ObjectPrefix {
                    stream_id: 1,
                    topic_id: 2,
                    partition_id: 3,
                    created_revision: 4,
                },
                start_offset,
                object: SegmentObject::Log,
            },
            keyed_end_offset: None,
            range: 0..len,
        }
    }

    fn source() -> RemoteRange {
        source_of(0, OBJECT_LEN)
    }

    fn object(len: u64) -> Bytes {
        (0..len)
            .map(|byte| u8::try_from(byte % 251).unwrap())
            .collect::<Vec<u8>>()
            .into()
    }

    fn expected(range: Range<u64>) -> Vec<u8> {
        object(range.end)
            .slice(usize::try_from(range.start).unwrap()..)
            .to_vec()
    }

    /// One object, served under every key. It keeps the ranges it was asked
    /// for, and while it has a gate, each GET waits for the gate to open.
    struct Store {
        object: Bytes,
        asked: RefCell<Vec<Range<u64>>>,
        gate: RefCell<Option<Shared<oneshot::Receiver<()>>>>,
        waiting: Cell<usize>,
        /// GETs that fail before the next one answers.
        failing: Cell<usize>,
    }

    impl Store {
        fn holding(len: u64) -> Rc<Self> {
            Rc::new(Self {
                object: object(len),
                asked: RefCell::default(),
                gate: RefCell::default(),
                waiting: Cell::new(0),
                failing: Cell::new(0),
            })
        }

        /// Close the gate. Sending on what this returns opens it.
        fn close(&self) -> oneshot::Sender<()> {
            let (open, gate) = oneshot::channel();
            *self.gate.borrow_mut() = Some(gate.shared());
            open
        }

        /// The ranges asked for, as their bounds.
        fn asked(&self) -> Vec<(u64, u64)> {
            self.asked
                .borrow()
                .iter()
                .map(|range| (range.start, range.end))
                .collect()
        }
    }

    impl RemoteObjects for Store {
        fn get_range<'a>(
            &'a self,
            _key: &'a str,
            range: Range<u64>,
        ) -> LocalBoxFuture<'a, Result<ObjectRange, ObjectStoreError>> {
            self.asked.borrow_mut().push(range.clone());
            let gate = self.gate.borrow().clone();
            async move {
                if let Some(gate) = gate {
                    self.waiting.set(self.waiting.get() + 1);
                    let _ = gate.await;
                    self.waiting.set(self.waiting.get() - 1);
                }
                if self.failing.get() > 0 {
                    self.failing.set(self.failing.get() - 1);
                    return Err(ObjectStoreError::Transport("refused".to_owned()));
                }
                let end = usize::try_from(range.end).unwrap().min(self.object.len());
                Ok(ObjectRange {
                    bytes: self
                        .object
                        .slice(usize::try_from(range.start).unwrap()..end),
                    etag: String::new(),
                    size: self.object.len() as u64,
                })
            }
            .boxed_local()
        }
    }

    /// A store that never answers.
    struct Silent;

    impl RemoteObjects for Silent {
        fn get_range<'a>(
            &'a self,
            _key: &'a str,
            _range: Range<u64>,
        ) -> LocalBoxFuture<'a, Result<ObjectRange, ObjectStoreError>> {
            future::pending().boxed_local()
        }
    }

    /// A tier executor that keeps the class and the bytes of each job and
    /// runs it at once, or never runs speculative jobs, as the tier service
    /// does while demand fetches wait.
    #[derive(Clone)]
    struct Recording {
        store: Rc<MemoryStore>,
        requests: Rc<RefCell<Vec<(TierJobClass, u64)>>>,
        starve_speculative: bool,
    }

    impl Recording {
        /// An executor over a store that holds the object of [`source`].
        async fn holding(starve_speculative: bool) -> Self {
            let store = MemoryStore::new();
            store
                .put(
                    &layout().segment_key(&source().key),
                    PutBody::from_bytes(object(OBJECT_LEN)),
                    &PutOptions::default(),
                )
                .await
                .unwrap();
            Self {
                store: Rc::new(store),
                requests: Rc::default(),
                starve_speculative,
            }
        }

        fn requests(&self) -> Vec<(TierJobClass, u64)> {
            self.requests.borrow().clone()
        }
    }

    impl TierExecutor for Recording {
        type Store = MemoryStore;

        fn submit<T, F, Fut>(&self, request: TierJobRequest, job: F) -> TierJobHandle<T>
        where
            T: 'static,
            F: FnOnce(Rc<Self::Store>) -> Fut + 'static,
            Fut: Future<Output = T> + 'static,
        {
            self.requests
                .borrow_mut()
                .push((request.class(), request.bytes()));
            if self.starve_speculative && request.class() == TierJobClass::SpeculativeFetch {
                return TierJobHandle::new(future::pending());
            }
            let job = job(Rc::clone(&self.store));
            TierJobHandle::new(async move { Ok(job.await) })
        }
    }

    /// Blocks of [`BLOCK`].
    const fn settings(cache_bytes: usize, read_ahead: u64) -> ReadCacheSettings {
        ReadCacheSettings {
            cache_bytes,
            block_bytes: BLOCK,
            read_ahead,
        }
    }

    fn remote(
        store: &Rc<Store>,
        ahead: Rc<dyn RemoteObjects>,
        cache_bytes: usize,
        read_ahead: u64,
    ) -> RemoteSegments {
        remote_with(store, ahead, settings(cache_bytes, read_ahead))
    }

    fn remote_with(
        store: &Rc<Store>,
        ahead: Rc<dyn RemoteObjects>,
        settings: ReadCacheSettings,
    ) -> RemoteSegments {
        let demand: Rc<dyn RemoteObjects> = Rc::clone(store) as _;
        RemoteSegments::new(demand, layout()).with_blocks(ahead, settings)
    }

    fn blocks(remote: &RemoteSegments) -> &Rc<RemoteBlocks> {
        remote.blocks.as_ref().unwrap()
    }

    async fn read_of(
        remote: &RemoteSegments,
        source: &RemoteRange,
        range: Range<u64>,
    ) -> Result<Vec<u8>, DiskFault> {
        let len = usize::try_from(range.end - range.start).unwrap();
        remote
            .read(source, range.start, len)
            .await
            .map(|read| read.as_slice().to_vec())
    }

    async fn read(remote: &RemoteSegments, range: Range<u64>) -> Result<Vec<u8>, DiskFault> {
        read_of(remote, &source(), range).await
    }

    /// Let the tasks of the GETs run.
    async fn settle() {
        for _ in 0..3 {
            compio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    #[compio::test]
    async fn given_a_range_read_once_when_reading_it_or_an_overlapping_one_should_fetch_each_block_once()
     {
        let store = Store::holding(OBJECT_LEN);
        let remote = remote(&store, Rc::new(Silent), CACHE_BYTES, 0);

        assert_eq!(
            read(&remote, 100..300_000).await,
            Ok(expected(100..300_000))
        );
        assert_eq!(
            read(&remote, 100..300_000).await,
            Ok(expected(100..300_000))
        );
        assert_eq!(store.asked(), [(0, 2 * BLOCK)]);

        assert_eq!(
            read(&remote, 200_000..600_000).await,
            Ok(expected(200_000..600_000))
        );
        assert_eq!(store.asked(), [(0, 2 * BLOCK), (2 * BLOCK, 3 * BLOCK)]);
    }

    #[compio::test]
    async fn given_ranges_inside_across_and_at_the_end_of_blocks_when_reading_should_return_the_object_s_bytes()
     {
        let ranges = [
            10..20,
            BLOCK - 5..BLOCK + 5,
            BLOCK..2 * BLOCK,
            1000..4 * BLOCK + 7,
            OBJECT_LEN - 10..OBJECT_LEN,
            3 * BLOCK + 1..OBJECT_LEN,
            0..OBJECT_LEN,
        ];
        for range in ranges {
            let store = Store::holding(OBJECT_LEN);
            let remote = remote(&store, Rc::new(Silent), CACHE_BYTES, 0);

            assert_eq!(
                read(&remote, range.clone()).await,
                Ok(expected(range.clone()))
            );
            assert_eq!(
                read(&remote, range.clone()).await,
                Ok(expected(range.clone()))
            );
            let asked = store.asked();
            assert_eq!(asked.len(), 1, "{range:?}");
            assert_eq!(asked[0].0, range.start / BLOCK * BLOCK, "{range:?}");
            assert_eq!(
                asked[0].1,
                range
                    .end
                    .div_ceil(BLOCK)
                    .saturating_mul(BLOCK)
                    .min(OBJECT_LEN),
                "{range:?}"
            );
        }
        let store = Store::holding(OBJECT_LEN);
        let remote = remote(&store, Rc::new(Silent), CACHE_BYTES, 0);
        assert_eq!(read(&remote, 7..7).await, Ok(Vec::new()));
        assert_eq!(store.asked(), []);
    }

    /// Reads at random places, many of them going on from the one before,
    /// in a cache that holds three blocks, so blocks are evicted all along.
    #[compio::test]
    async fn given_random_ranges_and_a_tiny_budget_when_reading_should_always_return_the_object_s_bytes()
     {
        for block_bytes in [1000, BLOCK] {
            Box::pin(read_random_ranges(block_bytes)).await;
        }
    }

    async fn read_random_ranges(block_bytes: u64) {
        let len = 23 * block_bytes + block_bytes / 3;
        let budget = 3 * usize::try_from(block_bytes).unwrap();
        let store = Store::holding(len);
        let ahead = Store::holding(len);
        let remote = remote_with(
            &store,
            ahead,
            ReadCacheSettings {
                cache_bytes: budget,
                block_bytes,
                read_ahead: 2 * block_bytes,
            },
        );
        let source = source_of(0, len);
        let reference = object(len);
        let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut random = |bound: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % bound
        };
        let mut last = 0..1;

        for step in 0..3_000 {
            let start = if random(2) == 0 && last.end < len {
                last.end - random(last.end - last.start).min(block_bytes / 8)
            } else {
                random(len)
            };
            let range = start..start + 1 + random((len - start).min(3 * block_bytes));

            let read = read_of(&remote, &source, range.clone()).await;

            let at = usize::try_from(start).unwrap()..usize::try_from(range.end).unwrap();
            assert_eq!(
                read.as_deref(),
                Ok(&reference[at]),
                "blocks of {block_bytes}, step {step}: {range:?}"
            );
            assert!(blocks(&remote).cache.borrow().held() <= budget);
            if step % 100 == 0 {
                settle().await;
            }
            last = range;
        }
        settle().await;
        assert!(blocks(&remote).fetching.borrow().is_empty());
        assert!(blocks(&remote).fetching_ahead.borrow().is_empty());
        assert_eq!(blocks(&remote).ahead_fetches.get(), 0);
        assert!(remote.take_read_metrics().unwrap().blocks_fetched_ahead > 0);
    }

    /// A reader that reads less than a block at a time reads each block
    /// several times, and that is still one pass: whether each read goes on
    /// from the batch the last one cut, or walks again from the start and
    /// reads further.
    #[compio::test]
    async fn given_one_pass_in_reads_smaller_than_a_block_when_much_is_read_once_after_should_not_keep_its_blocks()
     {
        let chunk = BLOCK / 4;
        let onward: Vec<Range<u64>> = (0..3 * BLOCK)
            .step_by(usize::try_from(chunk - 100).unwrap())
            .map(|start| start..start + chunk)
            .take_while(|range| range.end <= 3 * BLOCK)
            .collect();
        let from_the_start: Vec<Range<u64>> = (chunk..=3 * BLOCK)
            .step_by(usize::try_from(chunk).unwrap())
            .map(|end| 0..end)
            .collect();
        for pass in [onward, from_the_start] {
            let store = Store::holding(OBJECT_LEN);
            let remote = remote(&store, Rc::new(Silent), 10 * BLOCK_LEN, 0);
            for range in pass {
                assert_eq!(read(&remote, range.clone()).await, Ok(expected(range)));
            }
            for start_offset in 1..=40 {
                let other = source_of(start_offset * 1000, OBJECT_LEN);
                assert_eq!(read_of(&remote, &other, 0..10).await, Ok(expected(0..10)));
            }
            let asked = store.asked().len();

            assert_eq!(read(&remote, 0..10).await, Ok(expected(0..10)));

            assert_eq!(store.asked().len(), asked + 1);
        }
    }

    /// A poll of each later offset walks again from the index entry at the
    /// start of the segment, and reads one batch further than the last.
    #[compio::test]
    async fn given_reads_from_one_place_that_reach_further_each_time_when_reading_should_fetch_ahead_of_them()
     {
        let store = Store::holding(OBJECT_LEN);
        let ahead = Store::holding(OBJECT_LEN);
        let remote = remote(&store, ahead.clone(), CACHE_BYTES, READ_AHEAD);

        for end in (100_000..=OBJECT_LEN).step_by(100_000) {
            assert_eq!(read(&remote, 0..end).await, Ok(expected(0..end)));
            settle().await;
        }

        assert_eq!(store.asked(), [(0, BLOCK)]);
        assert_eq!(ahead.asked(), [(BLOCK, 3 * BLOCK), (3 * BLOCK, OBJECT_LEN)]);
    }

    #[compio::test]
    async fn given_two_reads_of_a_missing_block_at_once_when_both_wait_should_ask_the_store_once() {
        let store = Store::holding(OBJECT_LEN);
        let remote = remote(&store, Rc::new(Silent), CACHE_BYTES, 0);
        let open = store.close();
        let mut first = Box::pin(read(&remote, 100..200));
        let mut second = Box::pin(read(&remote, 150..BLOCK));

        while store.waiting.get() == 0 {
            assert!(futures::poll!(first.as_mut()).is_pending());
            settle().await;
        }
        assert!(futures::poll!(second.as_mut()).is_pending());
        settle().await;
        assert_eq!(store.asked(), [(0, BLOCK)]);

        open.send(()).unwrap();
        assert_eq!(first.await, Ok(expected(100..200)));
        assert_eq!(second.await, Ok(expected(150..BLOCK)));
        assert_eq!(store.asked(), [(0, BLOCK)]);
        let metrics = remote.take_read_metrics().unwrap();
        assert_eq!((metrics.block_hits, metrics.block_misses), (0, 2));
    }

    #[compio::test]
    async fn given_a_fetch_whose_only_reader_was_dropped_when_it_ends_should_still_keep_its_blocks()
    {
        let store = Store::holding(OBJECT_LEN);
        let remote = remote(&store, Rc::new(Silent), CACHE_BYTES, 0);
        let open = store.close();
        let mut abandoned = Box::pin(read(&remote, 100..200));
        while store.waiting.get() == 0 {
            assert!(futures::poll!(abandoned.as_mut()).is_pending());
            settle().await;
        }

        drop(abandoned);
        open.send(()).unwrap();
        settle().await;

        assert!(blocks(&remote).fetching.borrow().is_empty());
        assert_eq!(read(&remote, 100..200).await, Ok(expected(100..200)));
        assert_eq!(store.asked(), [(0, BLOCK)]);
    }

    #[compio::test]
    async fn given_a_failed_get_when_reading_again_should_ask_the_store_again() {
        let store = Store::holding(OBJECT_LEN);
        let remote = remote(&store, Rc::new(Silent), CACHE_BYTES, 0);
        store.failing.set(1);

        assert_eq!(read(&remote, 100..200).await, Err(DiskFault::Retryable));
        assert!(blocks(&remote).fetching.borrow().is_empty());
        assert_eq!(remote.take_read_metrics().unwrap().cached_bytes, 0);

        assert_eq!(read(&remote, 100..200).await, Ok(expected(100..200)));
        assert_eq!(store.asked(), [(0, BLOCK), (0, BLOCK)]);
    }

    #[compio::test]
    async fn given_an_object_shorter_than_its_segment_when_reading_its_end_should_fail_and_keep_nothing()
     {
        let store = Store::holding(OBJECT_LEN - 100);
        let remote = remote(&store, Rc::new(Silent), CACHE_BYTES, 0);

        assert_eq!(
            read(&remote, 4 * BLOCK..OBJECT_LEN).await,
            Err(DiskFault::Tiered)
        );

        assert!(blocks(&remote).fetching.borrow().is_empty());
        assert_eq!(remote.take_read_metrics().unwrap().cached_bytes, 0);
        assert_eq!(
            read(&remote, 4 * BLOCK..4 * BLOCK + 10).await,
            Ok(expected(4 * BLOCK..4 * BLOCK + 10))
        );
        assert_eq!(
            store.asked(),
            [(4 * BLOCK, OBJECT_LEN), (4 * BLOCK, 5 * BLOCK)]
        );
    }

    #[compio::test]
    async fn given_no_cache_budget_when_reading_should_ask_the_store_for_exactly_each_range() {
        let store = Store::holding(OBJECT_LEN);
        let remote = remote(&store, Rc::new(Silent), 0, READ_AHEAD);

        for _ in 0..2 {
            assert_eq!(
                read(&remote, 100..300_000).await,
                Ok(expected(100..300_000))
            );
        }
        assert_eq!(
            read(&remote, 200_000..600_000).await,
            Ok(expected(200_000..600_000))
        );

        assert!(remote.blocks.is_none());
        assert_eq!(remote.take_read_metrics(), None);
        assert_eq!(
            store.asked(),
            [(100, 300_000), (100, 300_000), (200_000, 600_000)]
        );
    }

    #[compio::test]
    async fn given_a_second_sequential_read_when_reading_should_fetch_the_next_blocks_as_speculative()
     {
        let executor = Recording::holding(false).await;
        let remote = RemoteSegments::cached(
            executor.clone(),
            layout(),
            settings(CACHE_BYTES, READ_AHEAD),
        );

        assert_eq!(read(&remote, 0..100_000).await, Ok(expected(0..100_000)));
        settle().await;
        assert_eq!(executor.requests(), [(TierJobClass::DemandFetch, BLOCK)]);
        assert_eq!(
            read(&remote, 90_000..200_000).await,
            Ok(expected(90_000..200_000))
        );
        settle().await;
        assert_eq!(
            executor.requests(),
            [
                (TierJobClass::DemandFetch, BLOCK),
                (TierJobClass::SpeculativeFetch, 2 * BLOCK)
            ]
        );

        // Both blocks come from memory, and the window doubles up to the end
        // of the object.
        assert_eq!(
            read(&remote, 190_000..3 * BLOCK).await,
            Ok(expected(190_000..3 * BLOCK))
        );
        settle().await;
        assert_eq!(
            executor.requests(),
            [
                (TierJobClass::DemandFetch, BLOCK),
                (TierJobClass::SpeculativeFetch, 2 * BLOCK),
                (TierJobClass::SpeculativeFetch, OBJECT_LEN - 3 * BLOCK)
            ]
        );
        let metrics = remote.take_read_metrics().unwrap();
        assert_eq!(metrics.read_ahead_hits, 2);
        assert_eq!(metrics.blocks_fetched_ahead, 5);
        assert_eq!(metrics.ahead_bytes, OBJECT_LEN - BLOCK);
        assert_eq!(metrics.demand_bytes, BLOCK);
    }

    /// Each read takes one block more, from the last byte of the one before.
    #[compio::test]
    async fn given_a_sequential_reader_when_it_reads_on_should_double_the_window_up_to_the_setting_and_stop_at_the_end()
     {
        const LONG: u64 = 13 * BLOCK + 1000;
        let store = Store::holding(LONG);
        let ahead = Store::holding(LONG);
        let remote = remote(&store, ahead.clone(), CACHE_BYTES, READ_AHEAD);
        let source = source_of(0, LONG);

        for block in 0..8 {
            let range = (block * BLOCK).saturating_sub(1)..(block + 1) * BLOCK;
            assert_eq!(
                read_of(&remote, &source, range.clone()).await,
                Ok(expected(range))
            );
            settle().await;
        }

        assert_eq!(store.asked(), [(0, BLOCK), (BLOCK, 2 * BLOCK)]);
        assert_eq!(
            ahead.asked(),
            [
                (2 * BLOCK, 4 * BLOCK),
                (4 * BLOCK, 7 * BLOCK),
                (7 * BLOCK, 12 * BLOCK),
                (12 * BLOCK, 13 * BLOCK),
                (13 * BLOCK, LONG)
            ]
        );
    }

    #[compio::test]
    async fn given_reads_at_random_places_when_reading_should_fetch_nothing_ahead() {
        let store = Store::holding(OBJECT_LEN);
        let ahead = Store::holding(OBJECT_LEN);
        let remote = remote(&store, ahead.clone(), CACHE_BYTES, READ_AHEAD);

        for block in [4, 1, 3, 0, 5, 2] {
            let range = block * BLOCK + 10..block * BLOCK + 500;
            assert_eq!(read(&remote, range.clone()).await, Ok(expected(range)));
        }
        settle().await;

        assert_eq!(ahead.asked(), []);
    }

    #[compio::test]
    async fn given_readahead_turned_off_or_below_a_block_when_reading_in_order_should_fetch_nothing_ahead()
     {
        for read_ahead in [0, BLOCK - 1] {
            let store = Store::holding(OBJECT_LEN);
            let ahead = Store::holding(OBJECT_LEN);
            let remote = remote(&store, ahead.clone(), CACHE_BYTES, read_ahead);

            for block in 0..5 {
                let range = (block * BLOCK).saturating_sub(1)..(block + 1) * BLOCK;
                assert_eq!(read(&remote, range.clone()).await, Ok(expected(range)));
            }
            settle().await;

            assert_eq!(ahead.asked(), []);
            assert_eq!(store.asked().len(), 5);
        }
    }

    /// The table of readers forgot this one, and the block that readahead
    /// fetched for it tells that it reads on.
    #[compio::test]
    async fn given_a_block_fetched_ahead_when_its_reader_reads_it_should_take_it_from_memory_and_read_further_ahead()
     {
        let store = Store::holding(OBJECT_LEN);
        let ahead = Store::holding(OBJECT_LEN);
        let remote = remote(&store, ahead.clone(), CACHE_BYTES, READ_AHEAD);
        assert_eq!(read(&remote, 0..BLOCK).await, Ok(expected(0..BLOCK)));
        assert_eq!(
            read(&remote, BLOCK - 1..2 * BLOCK).await,
            Ok(expected(BLOCK - 1..2 * BLOCK))
        );
        settle().await;
        assert_eq!(ahead.asked(), [(2 * BLOCK, 4 * BLOCK)]);
        for start_offset in 1..=READERS_MAX as u64 {
            let other = source_of(start_offset * 1000, OBJECT_LEN);
            assert_eq!(read_of(&remote, &other, 0..10).await, Ok(expected(0..10)));
        }
        let asked = store.asked().len();

        assert_eq!(
            read(&remote, 2 * BLOCK..2 * BLOCK + 10).await,
            Ok(expected(2 * BLOCK..2 * BLOCK + 10))
        );
        settle().await;

        assert_eq!(store.asked().len(), asked);
        assert_eq!(
            ahead.asked(),
            [(2 * BLOCK, 4 * BLOCK), (4 * BLOCK, 5 * BLOCK)]
        );
        assert_eq!(remote.take_read_metrics().unwrap().read_ahead_hits, 1);
    }

    /// The tier service starts no speculative fetch while a demand fetch
    /// waits, so a read that waited for one would wait for itself.
    #[compio::test]
    async fn given_readahead_that_never_starts_when_a_read_needs_its_block_should_fetch_it_on_demand()
     {
        let executor = Recording::holding(true).await;
        let remote = RemoteSegments::cached(
            executor.clone(),
            layout(),
            settings(CACHE_BYTES, READ_AHEAD),
        );
        assert_eq!(read(&remote, 0..BLOCK).await, Ok(expected(0..BLOCK)));
        assert_eq!(
            read(&remote, BLOCK - 1..2 * BLOCK).await,
            Ok(expected(BLOCK - 1..2 * BLOCK))
        );
        assert!(
            executor
                .requests()
                .contains(&(TierJobClass::SpeculativeFetch, 2 * BLOCK))
        );

        let read = compio::time::timeout(
            Duration::from_secs(1),
            read(&remote, 2 * BLOCK - 1..3 * BLOCK),
        )
        .await;

        assert_eq!(
            read.expect("the read waited for readahead"),
            Ok(expected(2 * BLOCK - 1..3 * BLOCK))
        );
        let demand = executor
            .requests()
            .into_iter()
            .filter(|(class, _)| *class == TierJobClass::DemandFetch)
            .count();
        assert_eq!(demand, 3);
    }

    #[compio::test]
    async fn given_reads_when_taking_the_metrics_should_count_since_the_last_take() {
        let store = Store::holding(OBJECT_LEN);
        let remote = remote(&store, Rc::new(Silent), CACHE_BYTES, 0);
        read(&remote, 100..300_000).await.unwrap();
        read(&remote, 100..300_000).await.unwrap();

        let first = remote.take_read_metrics().unwrap();
        let second = remote.take_read_metrics().unwrap();

        assert_eq!(
            first,
            RemoteReadMetrics {
                block_hits: 2,
                block_misses: 2,
                demand_bytes: 2 * BLOCK,
                cached_bytes: 2 * BLOCK,
                ..RemoteReadMetrics::default()
            }
        );
        assert_eq!(
            second,
            RemoteReadMetrics {
                cached_bytes: 2 * BLOCK,
                ..RemoteReadMetrics::default()
            }
        );
    }
}
