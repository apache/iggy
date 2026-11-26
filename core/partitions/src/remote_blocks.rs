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

//! What a shard reads from object stores, kept in memory: from the node's own
//! store and from the stores of its geo peers, under one budget.
//!
//! Objects are kept in blocks. The blocks a read misses come with one GET for
//! each run of adjacent ones, and the reads that miss the same block at once
//! share its GET. A poll reads a segment in chunks, and each chunk starts
//! again at the batch that the one before it cut. A poll of a later offset
//! can walk again from the same index entry. In blocks, the bytes read again
//! come from memory. A reader that goes on from its last read gets the next
//! blocks fetched ahead, and on into the object after it when the read names
//! one.
//!
//! The budget is the shard's, and every partition reads within it. What a
//! partition fetches is its own, and a partition can have a limit on it, see
//! [`PartitionReadSettings`]: a limit and not a reservation, which the
//! partition over it meets by evicting its own blocks.
//!
//! The fresh objects that the shard uploads are held whole, apart from what
//! reads fetch, until the long-term floor of every partition with a batch in
//! one passes it. A rewrite hands the end of each long-term segment it
//! stores to the cache. The bytes of a geo peer's store are served only
//! while the peer may be read, and only under the identity of the cluster
//! that owned its root when they were read.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ops::{Range, RangeInclusive};
use std::rc::Rc;

use bytes::{Bytes, BytesMut};
use futures::FutureExt;
use futures::channel::oneshot;
use futures::future::Shared;
use iggy_object_store::{
    AHEAD_SHARE, Admission, ByteCache, Lookup, ObjectPrefix, ObjectRange, SegmentKey, SegmentObject,
};
use server_common::iobuf::{Frozen, Owned};
use server_common::sharding::IggyNamespace;
use server_common::yield_to_reactor;
use tracing::{error, info};

use crate::placed_read::REWRITE_YIELD_BYTES;
use crate::poll_plan::DiskFault;
use crate::remote_segments::{RemoteObjects, RemoteRange, fault};

/// Blocks that the first readahead in front of a reader fetches. Each
/// sequential read after it doubles them, up to
/// [`ReadCacheSettings::read_ahead`].
pub const READ_AHEAD_START_BLOCKS: u64 = 2;

/// Where the node's own store is among the stores a shard's cache keeps.
pub const OWN_STORE: StoreSlot = StoreSlot(0);

/// Bytes of the fresh objects a shard uploaded that its cache holds when it
/// keeps nothing it fetched.
///
/// Not a setting: a shard always kept this much of what it uploaded, so that
/// a consumer at the tail of a geo topic reads it from memory however the
/// cache is set.
pub const FRESH_CACHE_BYTES: usize = 64 * 1024 * 1024;

/// Readers that a shard follows through their objects. Past it, the one that
/// read the longest ago is forgotten.
const READERS_MAX: usize = 256;

/// Blocks below which a partition's limit keeps nothing it fetches: the
/// least a shard's whole cache may be, so that readahead has a block of it.
pub const LIMIT_BLOCKS_MIN: u64 = AHEAD_SHARE as u64;

/// How a shard keeps what it reads from object stores.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadCacheSettings {
    /// Bytes that the shard keeps at most. With 0 it keeps nothing that
    /// reads fetch, and each read asks the store for exactly its range. The
    /// fresh objects it uploads are held then in [`FRESH_CACHE_BYTES`].
    pub cache_bytes: usize,
    /// Bytes of one block. The blocks of an object start at its byte 0, and
    /// its last block is shorter.
    pub block_bytes: u64,
    /// Bytes that readahead fetches in front of a sequential reader at most,
    /// in whole blocks. 0 turns readahead off.
    pub read_ahead: u64,
    /// Readahead fetches that the shard has in flight at most, the slots of
    /// its speculative fetches. The tier service starts the fetches of a
    /// class in the order they came, and one queued behind many others can
    /// start after its reader went past its blocks.
    pub read_ahead_fetches: usize,
    /// Bytes of one readahead fetch at most, the byte cap of its speculative
    /// fetches, past which the tier service would start a fetch alone.
    /// Below one block, nothing is fetched ahead.
    pub read_ahead_fetch_bytes: u64,
}

/// What a partition's topic sets for the partition's reads through the
/// shard's cache. `None` leaves a value to the shard.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PartitionReadSettings {
    /// The most bytes of the shard's cache that the blocks the partition
    /// fetched hold: a limit, not a reservation, so what it leaves unused is
    /// there for the others. Below [`LIMIT_BLOCKS_MIN`] blocks the partition
    /// keeps nothing it fetches, and each of its reads asks the store for
    /// exactly its range.
    pub limit: Option<usize>,
    /// Bytes that readahead fetches in front of the partition's sequential
    /// readers at most, and never more than an eighth of its limit or of the
    /// shard's budget, whichever is smaller. Below
    /// [`READ_AHEAD_START_BLOCKS`] blocks, none.
    pub read_ahead: Option<u64>,
    /// Whether the partition left the shard, `None` for reads that no
    /// partition makes. The polls and rewrites that outlive it carry it.
    pub(crate) left: Option<PartitionLeft>,
}

impl PartitionReadSettings {
    /// Every value left to the shard.
    pub const SHARD: Self = Self {
        limit: None,
        read_ahead: None,
        left: None,
    };

    /// What a topic sets: `limit` and `read_ahead`, `None` to leave either
    /// to the shard.
    #[must_use]
    pub const fn new(limit: Option<usize>, read_ahead: Option<u64>) -> Self {
        Self {
            limit,
            read_ahead,
            left: None,
        }
    }

    /// Whether the partition that reads with these left the shard: it keeps
    /// nothing in the cache from then on, and starts no GET.
    pub(crate) fn left_shard(&self) -> bool {
        self.left.as_ref().is_some_and(PartitionLeft::is_set)
    }
}

/// Whether a partition left the shard, which the partition shares with the
/// snapshots of its polls and rewrites.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PartitionLeft(Rc<Cell<bool>>);

impl PartitionLeft {
    pub(crate) fn set(&self) {
        self.0.set(true);
    }

    fn is_set(&self) -> bool {
        self.0.get()
    }
}

/// What the cache of a shard did since the last [`ReadCache::take_metrics`].
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
    /// Bytes of the fresh objects the shard uploaded that the cache holds
    /// now for the polls of their partitions.
    pub held_bytes: u64,
    /// Fresh objects that the cache stopped holding: once the long-term
    /// floors of their partitions passed them, once every send in them was
    /// answered and none committed, or once their partitions left the shard.
    pub fresh_releases: u64,
    /// Bytes of long-term segments that rewrites handed to the cache as they
    /// stored them.
    pub carried_bytes: u64,
    /// Blocks and objects that left the cache because their partition was
    /// over its limit.
    pub limit_evictions: u64,
}

/// One of the stores whose bytes a shard's cache keeps: the node's own, or
/// the store of a geo peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct StoreSlot(pub u8);

/// Whether a store may be read now. Every read asks, whether the cache
/// holds its bytes or not.
pub trait ReadGate {
    /// The identity of the cluster that owns the store, for a read that
    /// starts now, or `None` while nothing of the store may be read. Bytes
    /// read under one identity are never served under another.
    fn admit(&self) -> Option<u128>;
}

/// The gate of the node's own store, which is always open.
struct OwnStoreGate;

impl ReadGate for OwnStoreGate {
    fn admit(&self) -> Option<u128> {
        Some(0)
    }
}

/// The bytes that one shard read from object stores, which every store it
/// reads shares.
pub struct ReadCache {
    cache: RefCell<ByteCache<CacheKey, ReadOwner>>,
    /// Whether it keeps what reads fetch, or only the fresh objects the
    /// shard uploads.
    keeps_fetched: bool,
    /// The GETs that reads wait for, under each block or object they fetch.
    fetching: RefCell<BTreeMap<CacheKey, RunFetch>>,
    /// The blocks that GETs of readahead fetch. No read waits for them.
    fetching_ahead: RefCell<BTreeSet<CacheKey>>,
    ahead_fetches: Cell<usize>,
    /// See [`ReadCacheSettings::read_ahead_fetches`].
    ahead_fetches_max: usize,
    block_bytes: u64,
    /// Blocks that readahead keeps in front of a reader at most.
    ahead_blocks_max: u64,
    /// Blocks of one GET of readahead at most.
    ahead_run_blocks_max: usize,
    readers: RefCell<Readers>,
    counted: Cell<RemoteReadMetrics>,
    held: RefCell<HeldFresh>,
    /// The blocks and objects that rewrites handed over and nobody read yet.
    /// They wait in the part of readahead, and their first reads are no
    /// readahead hits.
    carried: RefCell<BTreeSet<CacheKey>>,
}

impl ReadCache {
    /// The cache that `settings` describe. With no bytes for it, it keeps
    /// nothing that reads fetch and holds only the fresh objects the shard
    /// uploads, in [`FRESH_CACHE_BYTES`].
    #[must_use]
    pub fn new(settings: ReadCacheSettings) -> Rc<Self> {
        let keeps_fetched = settings.cache_bytes > 0;
        let budget = if keeps_fetched {
            settings.cache_bytes
        } else {
            FRESH_CACHE_BYTES
        };
        let block_bytes = settings.block_bytes.max(1);
        Rc::new(Self {
            cache: RefCell::new(ByteCache::new(budget)),
            keeps_fetched,
            fetching: RefCell::default(),
            fetching_ahead: RefCell::default(),
            ahead_fetches: Cell::new(0),
            ahead_fetches_max: settings.read_ahead_fetches,
            block_bytes,
            ahead_blocks_max: settings.read_ahead / block_bytes,
            ahead_run_blocks_max: usize::try_from(settings.read_ahead_fetch_bytes / block_bytes)
                .unwrap_or(usize::MAX),
            readers: RefCell::default(),
            counted: Cell::default(),
            held: RefCell::default(),
            carried: RefCell::default(),
        })
    }

    /// Whether it keeps what reads fetch from the store.
    #[must_use]
    pub const fn keeps_fetched(&self) -> bool {
        self.keeps_fetched
    }

    /// What the cache did since the last take, and the bytes it holds.
    #[must_use]
    pub fn take_metrics(&self) -> RemoteReadMetrics {
        let mut cache = self.cache.borrow_mut();
        RemoteReadMetrics {
            cached_bytes: cache.held() as u64,
            held_bytes: cache.held_apart() as u64,
            limit_evictions: cache.take_limit_evictions(),
            ..self.counted.take()
        }
    }

    /// How `owner` reads with `settings`: its limit goes to the cache when
    /// it changed, and `None` says that it keeps nothing it fetches.
    fn reading(&self, owner: ReadOwner, settings: &PartitionReadSettings) -> Option<Reading> {
        let kept_at_least = LIMIT_BLOCKS_MIN.saturating_mul(self.block_bytes);
        let limit = settings.limit.map(|limit| {
            if (limit as u64) < kept_at_least {
                0
            } else {
                limit
            }
        });
        let budget = {
            let mut cache = self.cache.borrow_mut();
            if cache.limit(&owner) != limit {
                // Logged when the effective limit becomes zero, not on each
                // update: a share that changes while it stays below the
                // minimum keeps the limit at zero and logs nothing again.
                if let (Some(0), Some(share @ 1..)) = (limit, settings.limit) {
                    info!(
                        prefix = ?owner.prefix,
                        share,
                        kept_at_least,
                        "a partition's share of its topic's read_cache_size is below \
                         {LIMIT_BLOCKS_MIN} blocks of this node's read_cache_block_size, so it \
                         keeps nothing that its reads fetch"
                    );
                }
                cache.set_limit(owner, limit);
            }
            cache.budget()
        };
        if limit == Some(0) {
            return None;
        }
        // The shard's own readahead passed the checks of its settings.
        let ahead_blocks = if settings.read_ahead.is_none() && limit.is_none() {
            self.ahead_blocks_max
        } else {
            let wanted = settings
                .read_ahead
                .map_or(self.ahead_blocks_max, |bytes| bytes / self.block_bytes);
            let room = limit.map_or(budget, |limit| limit.min(budget));
            let within = wanted.min(room as u64 / AHEAD_SHARE as u64 / self.block_bytes);
            if within < READ_AHEAD_START_BLOCKS {
                0
            } else {
                within
            }
        };
        Some(Reading {
            owner,
            ahead_blocks,
            left: settings.left.clone(),
        })
    }

    /// Drop every block and object kept of the store in `store`.
    fn forget_store(&self, store: StoreSlot) {
        self.cache.borrow_mut().retain(|key| key.store() != store);
    }

    /// A batch of `namespace` in the fresh object `object_id` committed, its
    /// last message at `end_offset`.
    pub(crate) fn note_fresh_commit(
        &self,
        namespace: IggyNamespace,
        object_id: u128,
        end_offset: u64,
    ) {
        let mut held = self.held.borrow_mut();
        if held.objects.is_empty() {
            return;
        }
        held.commit(namespace, object_id, end_offset);
    }

    /// Every send of the fresh object `object_id` was answered. A partition
    /// that committed none of its batches in it will never read it.
    pub(crate) fn settle_fresh(&self, object_id: u128) {
        let released = {
            let mut held = self.held.borrow_mut();
            let Some(partitions) = held.objects.get_mut(&object_id) else {
                return;
            };
            partitions.retain(|_, end_offset| end_offset.is_some());
            partitions.is_empty()
        };
        if released {
            self.held.borrow_mut().objects.remove(&object_id);
            self.release_fresh(object_id);
        }
    }

    /// The long-term floor of `namespace` reached `floor`, so its polls of
    /// the offsets up to it read long-term segments.
    pub(crate) fn passed_fresh(&self, namespace: IggyNamespace, floor: u64) {
        let released = {
            let mut held = self.held.borrow_mut();
            if held.by_end.is_empty() {
                return;
            }
            held.passed(namespace, floor)
        };
        for object_id in released {
            self.release_fresh(object_id);
        }
    }

    /// `namespace` left the shard, and reads none of what the shard holds.
    pub(crate) fn forget_fresh(&self, namespace: IggyNamespace) {
        let released = self.held.borrow_mut().forget(namespace);
        for object_id in released {
            self.release_fresh(object_id);
        }
    }

    /// Whether the fresh object `object_id` of the node's own store is still
    /// held for the polls of its partitions.
    #[cfg(test)]
    pub(crate) fn holds_fresh(&self, object_id: u128) -> bool {
        self.cache
            .borrow()
            .is_held(&CacheKey::Whole(ObjectId::own_fresh(object_id)))
    }

    /// Hold the fresh object of `key`, which the partitions `partitions`
    /// read until their long-term floors pass it.
    fn hold_fresh(
        &self,
        key: CacheKey,
        object_id: u128,
        bytes: Bytes,
        partitions: impl IntoIterator<Item = IggyNamespace>,
    ) {
        let mut cache = self.cache.borrow_mut();
        let Some((bytes, taken)) = held_within(bytes, cache.held_limit()) else {
            return;
        };
        cache.insert(key, bytes, taken, Admission::Held);
        let mut held = self.held.borrow_mut();
        // Uploads end in any order, so the objects the cache let go of are
        // not the ones with the lowest ids.
        for let_go in cache.take_let_go() {
            if let CacheKey::Whole(ObjectId {
                name: ObjectName::Fresh(object_id),
                ..
            }) = let_go
            {
                held.drop_object(object_id);
            }
        }
        if cache.is_held(&key) {
            held.objects.insert(
                object_id,
                partitions
                    .into_iter()
                    .map(|namespace| (namespace, None))
                    .collect(),
            );
        }
    }

    /// The fresh object `object_id` leaves the held ones. With nothing else
    /// cached it leaves the cache too, and otherwise it is evicted as bytes
    /// nobody read. One that the cache released on its own counts once.
    fn release_fresh(&self, object_id: u128) {
        let key = CacheKey::Whole(ObjectId::own_fresh(object_id));
        let mut cache = self.cache.borrow_mut();
        let held = cache.is_held(&key);
        if self.keeps_fetched {
            cache.release(&key);
        } else {
            cache.remove(&key);
        }
        if held {
            let mut counted = self.counted.get();
            counted.fresh_releases += 1;
            self.counted.set(counted);
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
        let read_through = follows.and_then(|last_read| last_read.last_block(self.block_bytes));
        let mut sequential = follows.is_some();
        let mut found = Vec::with_capacity(usize::try_from(blocks.end() - first + 1).unwrap_or(0));
        let mut counted = self.counted.get();
        let mut cache = self.cache.borrow_mut();
        for index in blocks {
            let key = CacheKey::Block { object, index };
            let bytes = if read_through.is_some_and(|through| index <= through) {
                cache.peek(&key)
            } else {
                match cache.read(&key) {
                    Lookup::Hit(bytes) => Some(bytes),
                    Lookup::AheadHit(bytes) => {
                        if !self.first_carried_read(&key) {
                            counted.read_ahead_hits += 1;
                        }
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
    /// last readahead kept `last_window` there, at most `most`.
    fn next_window(last_window: u64, most: u64) -> u64 {
        let window = if last_window == 0 {
            READ_AHEAD_START_BLOCKS
        } else {
            last_window.saturating_mul(2)
        };
        window.min(most)
    }

    /// Whether a GET of readahead for `keys` would fetch anything that the
    /// cache or a GET that reads wait for does not hold already.
    fn wants_ahead(&self, mut keys: impl Iterator<Item = CacheKey>) -> bool {
        let cache = self.cache.borrow();
        let fetching = self.fetching.borrow();
        keys.any(|key| !cache.holds(&key) && !fetching.contains_key(&key))
    }

    /// Whether `key` is a first read of what a rewrite handed over, which
    /// readahead did not fetch. Its next reads are none.
    fn first_carried_read(&self, key: &CacheKey) -> bool {
        self.carried.borrow_mut().remove(key)
    }

    /// Keep what a GET of `kind` for `reading` fetched of `object`, unless
    /// its partition left the shard since.
    fn keep(
        &self,
        kind: FetchKind,
        reading: &Reading,
        object: ObjectId,
        fetched: &Fetched,
        blocks: &[Bytes],
    ) {
        let admission = match kind {
            FetchKind::Demand => Admission::Read,
            FetchKind::Ahead => Admission::Ahead,
        };
        if !reading.left_shard() {
            let owner = reading.owner;
            let mut cache = self.cache.borrow_mut();
            let mut carried = self.carried.borrow_mut();
            for (key, bytes) in fetched.keys(object).zip(blocks) {
                // Readahead may have fetched it meanwhile, and then the read
                // that this GET was for is its first.
                if kind == FetchKind::Ahead || matches!(cache.read(&key), Lookup::Miss) {
                    carried.remove(&key);
                    cache.insert_owned(key, bytes.clone(), bytes.len(), admission, owner);
                }
            }
        }
        let bytes: u64 = blocks.iter().map(|bytes| bytes.len() as u64).sum();
        let mut counted = self.counted.get();
        match kind {
            FetchKind::Demand => counted.demand_bytes += bytes,
            FetchKind::Ahead => {
                counted.blocks_fetched_ahead += blocks.len() as u64;
                counted.ahead_bytes += bytes;
            }
        }
        self.counted.set(counted);
    }
}

/// The reads of one store through a shard's [`ReadCache`].
pub struct RemoteBlocks {
    cache: Rc<ReadCache>,
    store: StoreSlot,
    gate: Rc<dyn ReadGate>,
    demand: Rc<dyn RemoteObjects>,
    /// Reads for readahead, which no read may wait for: their class starts
    /// only while no demand fetch waits.
    ahead: Rc<dyn RemoteObjects>,
    /// The identity that the gate admitted last, `None` before it admitted
    /// one or once it refused.
    admitted: Cell<Option<u128>>,
}

impl RemoteBlocks {
    /// The reads of the store in `store`, through `demand` for what a read
    /// waits for and `ahead` for readahead, once `gate` admits them.
    pub(crate) fn new(
        cache: Rc<ReadCache>,
        store: StoreSlot,
        gate: Rc<dyn ReadGate>,
        demand: Rc<dyn RemoteObjects>,
        ahead: Rc<dyn RemoteObjects>,
    ) -> Self {
        Self {
            cache,
            store,
            gate,
            demand,
            ahead,
            admitted: Cell::new(None),
        }
    }

    /// The reads of the node's own store.
    pub(crate) fn own(
        cache: Rc<ReadCache>,
        demand: Rc<dyn RemoteObjects>,
        ahead: Rc<dyn RemoteObjects>,
    ) -> Self {
        Self::new(cache, OWN_STORE, Rc::new(OwnStoreGate), demand, ahead)
    }

    pub(crate) const fn cache(&self) -> &Rc<ReadCache> {
        &self.cache
    }

    /// How the partition whose incarnation `prefix` names in this store reads
    /// with `settings`, `None` for one that keeps nothing it fetches. Its
    /// limit takes effect here, at its first read after it changed.
    pub(crate) fn reading(
        &self,
        prefix: ObjectPrefix,
        settings: &PartitionReadSettings,
    ) -> Option<Reading> {
        self.cache.reading(self.owner(prefix), settings)
    }

    /// The partition whose incarnation `prefix` names in this store left the
    /// shard: what it holds goes.
    pub(crate) fn forget_reader(&self, prefix: ObjectPrefix) {
        self.cache
            .cache
            .borrow_mut()
            .remove_owner(&self.owner(prefix));
    }

    const fn owner(&self, prefix: ObjectPrefix) -> ReadOwner {
        ReadOwner {
            store: self.store,
            prefix,
        }
    }

    /// Exactly `range` of the segment object that `source` reads: the blocks
    /// it covers from the cache, and the others with one GET for each run of
    /// adjacent ones. `key_of` names the object of a segment. Readahead goes
    /// on into `next`, the segment after it. `poll_offset` is the offset that
    /// the poll reads next, when it knows one.
    pub(crate) async fn read_segment(
        &self,
        reading: Reading,
        source: &RemoteRange,
        key_of: &dyn Fn(&RemoteRange) -> String,
        range: Range<u64>,
        next: Option<&RemoteRange>,
        poll_offset: Option<u64>,
    ) -> Result<Frozen<4096>, DiskFault> {
        let identity = self.admit()?;
        let object = ObjectId::segment(self.store, identity, source);
        let object_key = || key_of(source);
        let next_key = next.map(|next| move || key_of(next));
        let next = next.zip(next_key.as_ref()).map(|(next, next_key)| {
            let next_key: &dyn Fn() -> String = next_key;
            (ObjectId::segment(self.store, identity, next), next_key)
        });
        let (first, found) = self
            .blocks(reading, object, &object_key, &range, next, poll_offset)
            .await?;
        assemble(&found, first, self.cache.block_bytes, &range)
    }

    /// Whether the cache holds every block of `range` of the segment object
    /// that `source` reads, so that a read of it asks the store nothing.
    pub(crate) fn holds_segment(&self, source: &RemoteRange, range: &Range<u64>) -> bool {
        let Ok(identity) = self.admit() else {
            return false;
        };
        let object = ObjectId::segment(self.store, identity, source);
        let cache = self.cache.cache.borrow();
        object
            .blocks_of(range, self.cache.block_bytes)
            .is_none_or(|mut blocks| {
                blocks.all(|index| cache.holds(&CacheKey::Block { object, index }))
            })
    }

    /// Drop the blocks of `range` of the segment object that `source` reads,
    /// whose bytes are not what the store holds, so that the next read
    /// fetches them again.
    pub(crate) fn forget_segment(&self, source: &RemoteRange, range: &Range<u64>) {
        let Ok(identity) = self.admit() else {
            return;
        };
        let object = ObjectId::segment(self.store, identity, source);
        let Some(blocks) = object.blocks_of(range, self.cache.block_bytes) else {
            return;
        };
        let mut cache = self.cache.cache.borrow_mut();
        for index in blocks {
            cache.remove(&CacheKey::Block { object, index });
        }
    }

    /// The `.index` object of the segment that `source` reads, which
    /// `key_of` names, whole and at most `cap` bytes. Its size is what the
    /// store says, and the reads that miss it at once share one GET.
    pub(crate) async fn read_segment_index(
        &self,
        reading: Reading,
        source: &RemoteRange,
        key_of: &dyn Fn() -> String,
        cap: u64,
    ) -> Result<Bytes, DiskFault> {
        let identity = self.admit()?;
        let object = ObjectId {
            store: self.store,
            owner: identity,
            name: ObjectName::Segment {
                key: SegmentKey {
                    object: SegmentObject::Index,
                    ..source.key
                },
                keyed_end_offset: source.keyed_end_offset,
            },
            end: None,
        };
        self.read_whole(&reading, object, key_of, cap).await
    }

    /// Hold the whole fresh object `object_id`, which this shard has just
    /// uploaded, until the long-term floors of `partitions` pass it. Only the
    /// node's own store holds what it uploads.
    pub(crate) fn hold_fresh(
        &self,
        object_id: u128,
        bytes: Bytes,
        partitions: impl IntoIterator<Item = IggyNamespace>,
    ) {
        if self.store != OWN_STORE {
            return;
        }
        let key = CacheKey::Whole(ObjectId::own_fresh(object_id));
        self.cache.hold_fresh(key, object_id, bytes, partitions);
    }

    /// The whole fresh object `object_id` while this shard holds it. Not
    /// counted as a read of it: once its data is in a long-term segment,
    /// nothing reads it, however often it was read before.
    pub(crate) fn held_fresh(&self, object_id: u128) -> Option<Bytes> {
        let owner = self.admit().ok()?;
        let object = self
            .cache
            .cache
            .borrow()
            .peek(&CacheKey::Whole(self.fresh(owner, object_id)))?;
        let mut counted = self.cache.counted.get();
        counted.block_hits += 1;
        self.cache.counted.set(counted);
        Some(object)
    }

    /// `range` of the fresh object `object_id`, which `key_of` names, in
    /// blocks of the cache. The store tells where the object ends. The
    /// blocks are the reader's, whoever else reads them later.
    pub(crate) async fn read_fresh(
        &self,
        reading: Reading,
        object_id: u128,
        key_of: &dyn Fn() -> String,
        range: Range<u64>,
    ) -> Result<Bytes, DiskFault> {
        let identity = self.admit()?;
        let object = self.fresh(identity, object_id);
        let (first, found) = self
            .blocks(reading, object, key_of, &range, None, None)
            .await?;
        assemble_bytes(&found, first, self.cache.block_bytes, &range)
    }

    /// `range` of the fresh object `object_id` when the cache holds it,
    /// whole or in blocks, without counting a read of it.
    pub(crate) fn peek_fresh(&self, object_id: u128, range: &Range<u64>) -> Option<Bytes> {
        let owner = self.admit().ok()?;
        let object = self.fresh(owner, object_id);
        let cache = self.cache.cache.borrow();
        if let Some(whole) = cache.peek(&CacheKey::Whole(object)) {
            return slice_of(&whole, 0, range);
        }
        let blocks = object.blocks_of(range, self.cache.block_bytes)?;
        let first = *blocks.start();
        let found = blocks
            .map(|index| cache.peek(&CacheKey::Block { object, index }))
            .collect::<Option<Vec<_>>>()?;
        assemble_bytes(&found, first, self.cache.block_bytes, range).ok()
    }

    const fn fresh(&self, owner: u128, object_id: u128) -> ObjectId {
        ObjectId {
            store: self.store,
            owner,
            name: ObjectName::Fresh(object_id),
            end: None,
        }
    }

    /// Keep what a reader just behind a rewrite reads next of the long-term
    /// segment the rewrite stored: its whole `index`, and the end of its
    /// `log`, as much as waits for a reader in the part of the cache that
    /// readahead takes beside the index. `source` reads the segment's
    /// `.log`.
    ///
    /// They count towards the limit of the segment's partition, as of its
    /// last read, and take at most an eighth of it.
    ///
    /// The tail is copied [`REWRITE_YIELD_BYTES`] at a time, with a yield to
    /// the shard's other tasks between two, and the cache free for them. The
    /// index goes in last, so it is the newest of what waits there and the
    /// last of it to go. Once the partition that reads with `settings`
    /// leaves the shard, nothing more of it goes in.
    pub(crate) async fn carry_segment(
        &self,
        source: &RemoteRange,
        log: &[u8],
        index: &[u8],
        settings: &PartitionReadSettings,
    ) {
        let Ok(identity) = self.admit() else {
            return;
        };
        let owner = self.owner(source.key.prefix);
        let object = ObjectId::segment(self.store, identity, source);
        let index_object = ObjectId {
            name: ObjectName::Segment {
                key: SegmentKey {
                    object: SegmentObject::Index,
                    ..source.key
                },
                keyed_end_offset: source.keyed_end_offset,
            },
            end: None,
            ..object
        };
        let block_bytes = self.cache.block_bytes;
        let room = {
            let cache = self.cache.cache.borrow();
            let budget = cache.budget();
            cache
                .limit(&owner)
                .map_or(budget, |limit| limit.min(budget))
        };
        if (room as u64) < LIMIT_BLOCKS_MIN.saturating_mul(block_bytes) {
            return;
        }
        let share = (room / AHEAD_SHARE) as u64;
        // The index waits in the same share, and the oldest of it goes first.
        // Alone past the share, it would wait there all the same.
        let Some(tail_room) = share.checked_sub(index.len() as u64) else {
            return;
        };
        let first = (log.len() as u64)
            .saturating_sub(tail_room)
            .div_ceil(block_bytes);
        let Some(tail) = usize::try_from(first * block_bytes)
            .ok()
            .and_then(|from| log.get(from..))
        else {
            return;
        };
        let block_len = usize::try_from(block_bytes).unwrap_or(usize::MAX);
        let mut blocks = (first..).zip(tail.chunks(block_len)).peekable();
        while blocks.peek().is_some() && !settings.left_shard() {
            {
                let mut cache = self.cache.cache.borrow_mut();
                let mut carried = self.cache.carried.borrow_mut();
                let mut copied = 0;
                for (index, block) in blocks.by_ref() {
                    let key = CacheKey::Block { object, index };
                    carried.insert(key);
                    cache.insert_owned(
                        key,
                        Bytes::copy_from_slice(block),
                        block.len(),
                        Admission::Ahead,
                        owner,
                    );
                    copied += block.len();
                    if copied >= REWRITE_YIELD_BYTES {
                        break;
                    }
                }
            }
            if blocks.peek().is_some() {
                yield_to_reactor().await;
            }
        }
        let left = settings.left_shard();
        let mut cache = self.cache.cache.borrow_mut();
        let mut carried = self.cache.carried.borrow_mut();
        if !left {
            let index_key = CacheKey::Whole(index_object);
            carried.insert(index_key);
            cache.insert_owned(
                index_key,
                Bytes::copy_from_slice(index),
                index.len(),
                Admission::Ahead,
                owner,
            );
        }
        // Only what the cache still holds may be read as handed over.
        carried.retain(|key| cache.holds(key));
        drop((cache, carried));
        if left {
            return;
        }
        let mut counted = self.cache.counted.get();
        counted.carried_bytes += (index.len() + tail.len()) as u64;
        self.cache.counted.set(counted);
    }

    /// The identity under which the store is read now. Once it moves, no
    /// read reaches the blocks kept under the last one, so they go.
    fn admit(&self) -> Result<u128, DiskFault> {
        let admitted = self.gate.admit();
        if let Some(last) = self.admitted.replace(admitted)
            && admitted != Some(last)
        {
            self.cache.forget_store(self.store);
        }
        // A store that refuses reads now fails them as a store that does not
        // answer, which a later poll can get past.
        admitted.ok_or(DiskFault::Retryable)
    }

    /// The blocks of `object` that hold `range`, from the first of them on,
    /// for the reader that `reading` describes.
    async fn blocks(
        &self,
        reading: Reading,
        object: ObjectId,
        key_of: &dyn Fn() -> String,
        range: &Range<u64>,
        next: Option<(ObjectId, &dyn Fn() -> String)>,
        poll_offset: Option<u64>,
    ) -> Result<(u64, Vec<Bytes>), DiskFault> {
        // A poll that outlives its partition reads a block at a time, and
        // starts no GET for it.
        if reading.left_shard() {
            return Err(DiskFault::Retryable);
        }
        let cache = &self.cache;
        let Some(blocks) = object.blocks_of(range, cache.block_bytes) else {
            return Ok((0, Vec::new()));
        };
        let first = *blocks.start();
        let last = *blocks.end();
        let follows = cache
            .readers
            .borrow_mut()
            .follow(object, range, poll_offset);
        let (mut found, sequential) = cache.look_up(object, blocks, follows.as_ref());
        let window = if sequential {
            ReadCache::next_window(
                follows.map_or(0, |last_read| last_read.window),
                reading.ahead_blocks,
            )
        } else {
            0
        };
        cache.readers.borrow_mut().record(Expectation {
            object,
            read: range.clone(),
            poll_offset,
            window,
        });
        let waits = self.fetch_missing(&reading, object, key_of, first, &found);
        self.read_ahead(&reading, object, key_of, last, window, next, poll_offset);
        for (slot, fetch) in waits {
            found[slot] = Some(fetch.block(first + slot as u64).await?);
        }
        let found = found.into_iter().collect::<Option<Vec<_>>>();
        Ok((first, found.ok_or(DiskFault::Retryable)?))
    }

    /// The whole `object`, of at most `cap` bytes, for `reading`.
    async fn read_whole(
        &self,
        reading: &Reading,
        object: ObjectId,
        key_of: &dyn Fn() -> String,
        cap: u64,
    ) -> Result<Bytes, DiskFault> {
        if reading.left_shard() {
            return Err(DiskFault::Retryable);
        }
        let whole = CacheKey::Whole(object);
        let cached = self.cache.cache.borrow_mut().read(&whole);
        let mut counted = self.cache.counted.get();
        let found = match cached {
            Lookup::Hit(bytes) => Some(bytes),
            Lookup::AheadHit(bytes) => {
                if !self.cache.first_carried_read(&whole) {
                    counted.read_ahead_hits += 1;
                }
                Some(bytes)
            }
            Lookup::Miss => None,
        };
        if found.is_some() {
            counted.block_hits += 1;
        } else {
            counted.block_misses += 1;
        }
        self.cache.counted.set(counted);
        if let Some(bytes) = found {
            return Ok(bytes);
        }
        let joined = self.cache.fetching.borrow().get(&whole).cloned();
        let fetch = joined.unwrap_or_else(|| {
            self.start_fetch(
                FetchKind::Demand,
                reading,
                object,
                key_of,
                Fetched::Whole { cap },
            )
        });
        fetch.block(0).await
    }

    /// Start the GETs of the missing blocks of `found`, which starts at block
    /// `first`, or join the ones that fetch them already. Returns where each
    /// missing block goes in `found` and the GET it comes with.
    fn fetch_missing(
        &self,
        reading: &Reading,
        object: ObjectId,
        key_of: &dyn Fn() -> String,
        first: u64,
        found: &[Option<Bytes>],
    ) -> Vec<(usize, RunFetch)> {
        let mut waits = Vec::new();
        let mut unfetched: Vec<(usize, u64)> = Vec::new();
        {
            let fetching = self.cache.fetching.borrow();
            let missing = (first..)
                .zip(found)
                .enumerate()
                .filter(|(_, (_, bytes))| bytes.is_none());
            for (slot, (index, _)) in missing {
                match fetching.get(&CacheKey::Block { object, index }) {
                    Some(fetch) => waits.push((slot, fetch.clone())),
                    None => unfetched.push((slot, index)),
                }
            }
        }
        for run in unfetched.chunk_by(|(_, before), (_, after)| before + 1 == *after) {
            let (Some((_, start)), Some((_, end))) = (run.first(), run.last()) else {
                continue;
            };
            let fetched = Fetched::Blocks(*start..end + 1);
            let fetch = self.start_fetch(FetchKind::Demand, reading, object, key_of, fetched);
            waits.extend(run.iter().map(|(slot, _)| (*slot, fetch.clone())));
        }
        waits
    }

    /// Fetch the `window` blocks after block `last` of `object` that no
    /// cache or GET holds, and past its end the first ones of `next`, for a
    /// poll that reads `poll_offset` next.
    #[allow(clippy::too_many_arguments)]
    fn read_ahead(
        &self,
        reading: &Reading,
        object: ObjectId,
        key_of: &dyn Fn() -> String,
        last: u64,
        window: u64,
        next: Option<(ObjectId, &dyn Fn() -> String)>,
        poll_offset: Option<u64>,
    ) {
        let block_bytes = self.cache.block_bytes;
        let Some(last_block) = object.last_block(block_bytes) else {
            return;
        };
        let reach = last.saturating_add(window);
        let through = reach.min(last_block);
        self.fetch_ahead(reading, object, key_of, last + 1..=through);
        // A reader that reached the end of the object, or whose readahead
        // did, goes on in `next`, even one that takes each object in one
        // read and so never had a window.
        let Some((next, next_key_of)) = next.filter(|_| reach >= last_block) else {
            return;
        };
        let Some(next_last) = next.last_block(block_bytes) else {
            return;
        };
        // The reader's first read of `next` goes on with this window.
        self.cache.readers.borrow_mut().record(Expectation {
            object: next,
            read: 0..0,
            poll_offset,
            window,
        });
        if reach > through {
            self.fetch_ahead(
                reading,
                next,
                next_key_of,
                0..=(reach - through - 1).min(next_last),
            );
        }
    }

    /// Fetch the ones of `blocks` of `object` that no cache or GET holds,
    /// unless too many readahead fetches are in flight. No GET of readahead
    /// is longer than its byte cap allows.
    fn fetch_ahead(
        &self,
        reading: &Reading,
        object: ObjectId,
        key_of: &dyn Fn() -> String,
        blocks: RangeInclusive<u64>,
    ) {
        let read_cache = &self.cache;
        if read_cache.ahead_fetches.get() >= read_cache.ahead_fetches_max
            || read_cache.ahead_run_blocks_max == 0
        {
            return;
        }
        let missing: Vec<u64> = {
            let cache = self.cache.cache.borrow();
            let fetching = self.cache.fetching.borrow();
            let fetching_ahead = self.cache.fetching_ahead.borrow();
            blocks
                .filter(|index| {
                    let key = CacheKey::Block {
                        object,
                        index: *index,
                    };
                    !cache.holds(&key)
                        && !fetching.contains_key(&key)
                        && !fetching_ahead.contains(&key)
                })
                .collect()
        };
        let runs = missing
            .chunk_by(|before, after| before + 1 == *after)
            .flat_map(|run| run.chunks(read_cache.ahead_run_blocks_max));
        for run in runs {
            let (Some(start), Some(end)) = (run.first(), run.last()) else {
                continue;
            };
            if read_cache.ahead_fetches.get() >= read_cache.ahead_fetches_max {
                return;
            }
            self.start_fetch(
                FetchKind::Ahead,
                reading,
                object,
                key_of,
                Fetched::Blocks(*start..end + 1),
            );
        }
    }

    /// Start one GET of `fetched` of `object` for `reading` in a task of its
    /// own, which keeps what it fetched whether anyone still waits for it or
    /// not.
    fn start_fetch(
        &self,
        kind: FetchKind,
        reading: &Reading,
        object: ObjectId,
        key_of: &dyn Fn() -> String,
        fetched: Fetched,
    ) -> RunFetch {
        let (done, outcome) = oneshot::channel();
        let fetch = RunFetch {
            first: fetched.first(),
            fetched: outcome.shared(),
        };
        let objects = match kind {
            FetchKind::Demand => Rc::clone(&self.demand),
            FetchKind::Ahead => Rc::clone(&self.ahead),
        };
        let flight = Flight::begin(
            Rc::clone(&self.cache),
            objects,
            kind,
            reading.clone(),
            object,
            fetched,
            &fetch,
        );
        let key = key_of();
        compio::runtime::spawn(async move {
            let outcome = flight.fetch(&key).await;
            // Every read that waited may have reached its deadline.
            let _ = done.send(outcome);
        })
        .detach();
        fetch
    }
}

/// The fresh objects that a shard uploaded and holds, with the partitions
/// that may still read each.
#[derive(Default)]
struct HeldFresh {
    /// Each partition with a batch in an object, and the last offset of its
    /// batches there once one committed.
    objects: BTreeMap<u128, BTreeMap<IggyNamespace, Option<u64>>>,
    /// The last committed offset of each partition in each object, so that a
    /// floor finds the objects it passed without a walk of the others.
    by_end: BTreeSet<(IggyNamespace, u64, u128)>,
}

impl HeldFresh {
    fn commit(&mut self, namespace: IggyNamespace, object_id: u128, end_offset: u64) {
        let Some(last) = self
            .objects
            .get_mut(&object_id)
            .and_then(|partitions| partitions.get_mut(&namespace))
        else {
            return;
        };
        if let Some(before) = *last {
            if before >= end_offset {
                return;
            }
            self.by_end.remove(&(namespace, before, object_id));
        }
        *last = Some(end_offset);
        self.by_end.insert((namespace, end_offset, object_id));
    }

    /// Drop `namespace` from the objects whose batches of it end at or below
    /// `floor`, and return the objects no partition reads any more.
    fn passed(&mut self, namespace: IggyNamespace, floor: u64) -> Vec<u128> {
        let passed: Vec<(IggyNamespace, u64, u128)> = self
            .by_end
            .range((namespace, 0, 0)..=(namespace, floor, u128::MAX))
            .copied()
            .collect();
        let mut released = Vec::new();
        for row in passed {
            self.by_end.remove(&row);
            let (_, _, object_id) = row;
            if let Some(partitions) = self.objects.get_mut(&object_id) {
                partitions.remove(&namespace);
                if partitions.is_empty() {
                    self.objects.remove(&object_id);
                    released.push(object_id);
                }
            }
        }
        released
    }

    /// Drop `namespace` from every object, and return the objects no
    /// partition reads any more.
    fn forget(&mut self, namespace: IggyNamespace) -> Vec<u128> {
        let mut released = Vec::new();
        for (object_id, partitions) in &mut self.objects {
            if let Some(Some(end_offset)) = partitions.remove(&namespace) {
                self.by_end.remove(&(namespace, end_offset, *object_id));
            }
            if partitions.is_empty() {
                released.push(*object_id);
            }
        }
        for object_id in &released {
            self.objects.remove(object_id);
        }
        released
    }

    fn drop_object(&mut self, object_id: u128) {
        let Some(partitions) = self.objects.remove(&object_id) else {
            return;
        };
        for (namespace, end_offset) in partitions {
            if let Some(end_offset) = end_offset {
                self.by_end.remove(&(namespace, end_offset, object_id));
            }
        }
    }
}

/// The partition that reads an entry, as the cache counts what each one
/// holds: its incarnation's prefix in the store in `store`. A prefix names one
/// incarnation of one partition in a store, so a partition that comes back
/// after it left is another owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ReadOwner {
    store: StoreSlot,
    prefix: ObjectPrefix,
}

/// How one partition reads through the cache.
#[derive(Debug, Clone)]
pub struct Reading {
    owner: ReadOwner,
    /// Blocks that readahead keeps in front of its readers at most.
    ahead_blocks: u64,
    /// See [`PartitionReadSettings::left`].
    left: Option<PartitionLeft>,
}

impl Reading {
    fn left_shard(&self) -> bool {
        self.left.as_ref().is_some_and(PartitionLeft::is_set)
    }
}

/// An object as the cache names it: in which store, under whose identity,
/// which object, and where it ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ObjectId {
    store: StoreSlot,
    /// The identity of the cluster that owned the store when the bytes
    /// were read.
    owner: u128,
    name: ObjectName,
    /// Where the bytes that reads take end, which is where the last block
    /// ends. `None` for an object whose end the store tells.
    end: Option<u64>,
}

/// Which object of a store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ObjectName {
    Segment {
        key: SegmentKey,
        keyed_end_offset: Option<u64>,
    },
    /// A fresh object, by its id.
    Fresh(u128),
}

impl ObjectId {
    /// The fresh object `object_id` of the node's own store.
    const fn own_fresh(object_id: u128) -> Self {
        Self {
            store: OWN_STORE,
            owner: 0,
            name: ObjectName::Fresh(object_id),
            end: None,
        }
    }

    const fn segment(store: StoreSlot, owner: u128, source: &RemoteRange) -> Self {
        Self {
            store,
            owner,
            name: ObjectName::Segment {
                key: source.key,
                keyed_end_offset: source.keyed_end_offset,
            },
            end: Some(source.range.end),
        }
    }

    /// The blocks of `block_bytes` that hold `range`, `None` for an empty
    /// one.
    fn blocks_of(&self, range: &Range<u64>, block_bytes: u64) -> Option<RangeInclusive<u64>> {
        let end = self.end.map_or(range.end, |end| range.end.min(end));
        (range.start < end).then(|| range.start / block_bytes..=(range.end - 1) / block_bytes)
    }

    /// The last block, `None` while the end of the object is unknown.
    fn last_block(&self, block_bytes: u64) -> Option<u64> {
        self.end.map(|end| end.saturating_sub(1) / block_bytes)
    }

    /// The bytes of the blocks `run` of `block_bytes`.
    fn bytes_of(&self, run: &Range<u64>, block_bytes: u64) -> Range<u64> {
        let start = run.start.saturating_mul(block_bytes);
        let end = run.end.saturating_mul(block_bytes);
        start..self.end.map_or(end, |object_end| end.min(object_end))
    }
}

/// What the cache keeps: one block of an object, or a whole object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum CacheKey {
    Block { object: ObjectId, index: u64 },
    Whole(ObjectId),
}

impl CacheKey {
    const fn store(&self) -> StoreSlot {
        match self {
            Self::Block { object, .. } | Self::Whole(object) => object.store,
        }
    }
}

/// What one GET fetches of an object.
#[derive(Debug, Clone)]
enum Fetched {
    /// A run of adjacent blocks.
    Blocks(Range<u64>),
    /// The whole object, which is at most `cap` bytes.
    Whole { cap: u64 },
}

impl Fetched {
    /// The block a [`RunFetch`] of this starts at.
    const fn first(&self) -> u64 {
        match self {
            Self::Blocks(run) => run.start,
            Self::Whole { .. } => 0,
        }
    }

    /// The key of every block or object this fetches of `object`, in the
    /// order of the bytes a GET of it returns.
    fn keys(&self, object: ObjectId) -> impl Iterator<Item = CacheKey> + use<> {
        let (run, whole) = match self {
            Self::Blocks(run) => (run.clone(), None),
            Self::Whole { .. } => (0..0, Some(CacheKey::Whole(object))),
        };
        run.map(move |index| CacheKey::Block { object, index })
            .chain(whole)
    }
}

/// How a GET ended: the blocks it fetched, the first one first, or the
/// whole object alone.
type RunOutcome = Result<Rc<[Bytes]>, DiskFault>;

/// A GET of a run of adjacent blocks, or of a whole object, in a task of its
/// own.
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
        // A run can end early only where its object does, so a block past
        // the run's end is past the object's end.
        index
            .checked_sub(self.first)
            .and_then(|at| usize::try_from(at).ok())
            .and_then(|at| blocks.get(at).cloned())
            .ok_or(DiskFault::Tiered)
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

/// One GET, listed under each block or object it fetches until its task
/// ends.
struct Flight {
    cache: Rc<ReadCache>,
    objects: Rc<dyn RemoteObjects>,
    kind: FetchKind,
    reading: Reading,
    object: ObjectId,
    fetched: Fetched,
}

impl Flight {
    fn begin(
        cache: Rc<ReadCache>,
        objects: Rc<dyn RemoteObjects>,
        kind: FetchKind,
        reading: Reading,
        object: ObjectId,
        fetched: Fetched,
        fetch: &RunFetch,
    ) -> Self {
        let keys = fetched.keys(object);
        match kind {
            FetchKind::Demand => cache
                .fetching
                .borrow_mut()
                .extend(keys.map(|key| (key, fetch.clone()))),
            FetchKind::Ahead => {
                cache.fetching_ahead.borrow_mut().extend(keys);
                cache.ahead_fetches.set(cache.ahead_fetches.get() + 1);
            }
        }
        Self {
            cache,
            objects,
            kind,
            reading,
            object,
            fetched,
        }
    }

    /// GET what the flight fetches from the object `key` and keep it.
    /// Nothing of a failed or short GET is kept. Readahead that waited for
    /// its slot until reads fetched its blocks asks the store nothing.
    async fn fetch(self, key: &str) -> RunOutcome {
        let block_bytes = self.cache.block_bytes;
        let range = match &self.fetched {
            Fetched::Blocks(run) => self.object.bytes_of(run, block_bytes),
            Fetched::Whole { cap } => 0..*cap,
        };
        let fetched = match self.kind {
            FetchKind::Demand => match self.objects.get_range(key, range.clone()).await {
                Ok(fetched) => fetched,
                Err(error) => return Err(fault(key, &error)),
            },
            FetchKind::Ahead => {
                let cache = Rc::clone(&self.cache);
                let keys = self.fetched.keys(self.object);
                let wanted = Box::new(move || cache.wants_ahead(keys));
                match self.objects.get_range_if(key, range.clone(), wanted).await {
                    Ok(Some(fetched)) => fetched,
                    Ok(None) | Err(_) => return Err(DiskFault::Retryable),
                }
            }
        };
        let bytes = self.checked(key, &range, fetched)?;
        let blocks: Rc<[Bytes]> = match &self.fetched {
            // One allocation for each block, so evicting a block frees its
            // bytes.
            Fetched::Blocks(_) => bytes
                .chunks(usize::try_from(block_bytes).unwrap_or(usize::MAX))
                .map(Bytes::copy_from_slice)
                .collect(),
            Fetched::Whole { .. } => Rc::from([Bytes::copy_from_slice(&bytes)]),
        };
        self.cache.keep(
            self.kind,
            &self.reading,
            self.object,
            &self.fetched,
            &blocks,
        );
        Ok(blocks)
    }

    /// The bytes of `fetched` when they are all of `range` that the object
    /// holds: exactly the range, or for an object whose end the store tells,
    /// the range cut inside its last block, or anywhere for a whole object.
    fn checked(
        &self,
        key: &str,
        range: &Range<u64>,
        fetched: ObjectRange,
    ) -> Result<Bytes, DiskFault> {
        let asked = range.end - range.start;
        let returned = fetched.bytes.len() as u64;
        let ends_inside = match &self.fetched {
            Fetched::Blocks(_) => returned + self.cache.block_bytes > asked,
            Fetched::Whole { .. } => true,
        };
        let complete = returned == asked
            || (self.object.end.is_none()
                && returned < asked
                && ends_inside
                && range.start + returned == fetched.size);
        if complete {
            return Ok(fetched.bytes);
        }
        if self.kind == FetchKind::Demand {
            error!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                key,
                start = range.start,
                asked,
                returned,
                size = fetched.size,
                "remote poll: the object is shorter than its reader expects"
            );
        }
        Err(DiskFault::Tiered)
    }
}

impl Drop for Flight {
    fn drop(&mut self) {
        let keys = self.fetched.keys(self.object);
        match self.kind {
            FetchKind::Demand => {
                let mut fetching = self.cache.fetching.borrow_mut();
                for key in keys {
                    fetching.remove(&key);
                }
            }
            FetchKind::Ahead => {
                let mut fetching_ahead = self.cache.fetching_ahead.borrow_mut();
                for key in keys {
                    fetching_ahead.remove(&key);
                }
                self.cache
                    .ahead_fetches
                    .set(self.cache.ahead_fetches.get().saturating_sub(1));
            }
        }
    }
}

/// The last read of a reader, which its next read goes on from when it reads
/// in order.
#[derive(Debug, Clone)]
struct Expectation {
    object: ObjectId,
    read: Range<u64>,
    /// The offset that the poll which read was to read next, when it knew
    /// one.
    poll_offset: Option<u64>,
    /// Blocks that readahead keeps in front of the reader, 0 for none.
    window: u64,
}

impl Expectation {
    /// The next read of a sequential reader starts no earlier than this one
    /// did and no later than where it ended, and starts later, ends later or
    /// is for a later offset. A poll that goes on reads again the batch this
    /// one cut, and a poll of a later offset walks again from the same index
    /// entry, as far as this one or further. The same bytes for the same
    /// offset are read again.
    fn continued_by(&self, object: ObjectId, read: &Range<u64>, poll_offset: Option<u64>) -> bool {
        let later_offset = matches!(
            (self.poll_offset, poll_offset),
            (Some(last), Some(next)) if next > last
        );
        self.object == object
            && (self.read.start..=self.read.end).contains(&read.start)
            && (read.start > self.read.start || read.end > self.read.end || later_offset)
    }

    /// The last block the read took, `None` for a reader that took none of
    /// the object yet.
    fn last_block(&self, block_bytes: u64) -> Option<u64> {
        (!self.read.is_empty()).then(|| (self.read.end - 1) / block_bytes)
    }
}

/// The readers expected to read on, the one that read last at the back.
#[derive(Default)]
struct Readers {
    expected: VecDeque<Expectation>,
}

impl Readers {
    /// The expectation that a read of `read` of `object` for `poll_offset`
    /// meets, which leaves the table.
    fn follow(
        &mut self,
        object: ObjectId,
        read: &Range<u64>,
        poll_offset: Option<u64>,
    ) -> Option<Expectation> {
        let at = self
            .expected
            .iter()
            .rposition(|expectation| expectation.continued_by(object, read, poll_offset))?;
        self.expected.remove(at)
    }

    fn record(&mut self, expectation: Expectation) {
        // Readahead names the start of the next object on each read near the
        // end of one, and one row is enough.
        if let Some(at) = self.expected.iter().position(|recorded| {
            recorded.object == expectation.object && recorded.read == expectation.read
        }) {
            self.expected.remove(at);
        }
        if self.expected.len() >= READERS_MAX {
            self.expected.pop_front();
        }
        self.expected.push_back(expectation);
    }
}

/// `range` of an object out of its adjacent blocks of `block_bytes` in
/// `found`, the first of which is block `first`.
fn assemble(
    found: &[Bytes],
    first: u64,
    block_bytes: u64,
    range: &Range<u64>,
) -> Result<Frozen<4096>, DiskFault> {
    let len = usize::try_from(range.end - range.start).map_err(|_| DiskFault::Tiered)?;
    let mut owned = Owned::<4096>::with_capacity(len);
    for (index, block) in (first..).zip(found) {
        let piece = piece(block, index * block_bytes, range).ok_or(DiskFault::Tiered)?;
        owned.extend_from_slice(piece);
    }
    if owned.as_slice().len() != len {
        return Err(DiskFault::Tiered);
    }
    Ok(Frozen::from(owned))
}

/// `range` of an object out of its adjacent blocks of `block_bytes` in
/// `found`, the first of which is block `first`. A range inside one block is
/// a view of it.
fn assemble_bytes(
    found: &[Bytes],
    first: u64,
    block_bytes: u64,
    range: &Range<u64>,
) -> Result<Bytes, DiskFault> {
    if let [block] = found {
        return slice_of(block, first * block_bytes, range).ok_or(DiskFault::Tiered);
    }
    let len = usize::try_from(range.end - range.start).map_err(|_| DiskFault::Tiered)?;
    let mut bytes = BytesMut::with_capacity(len);
    for (index, block) in (first..).zip(found) {
        bytes.extend_from_slice(piece(block, index * block_bytes, range).ok_or(DiskFault::Tiered)?);
    }
    if bytes.len() != len {
        return Err(DiskFault::Tiered);
    }
    Ok(bytes.freeze())
}

/// `range` of an object out of `bytes`, which start at byte `at` of it,
/// when they hold all of it.
fn slice_of(bytes: &Bytes, at: u64, range: &Range<u64>) -> Option<Bytes> {
    let start = usize::try_from(range.start.checked_sub(at)?).ok()?;
    let end = usize::try_from(range.end.checked_sub(at)?).ok()?;
    (start <= end && end <= bytes.len()).then(|| bytes.slice(start..end))
}

/// `bytes` as the cache holds them, and the memory they take, or `None`
/// for bytes longer than `limit`, which the cache would refuse, so they are
/// not copied first.
///
/// A buffer that grew by doubling takes up to twice its length. One with no
/// other owner is kept as it is while its capacity is within `limit`, so
/// holding it copies nothing. Any other buffer is copied to its length: one
/// that is shared can take more memory than this owner can measure.
fn held_within(bytes: Bytes, limit: usize) -> Option<(Bytes, usize)> {
    if bytes.len() > limit {
        return None;
    }
    let exact = |bytes: &[u8]| (Bytes::copy_from_slice(bytes), bytes.len());
    Some(match bytes.try_into_mut() {
        Ok(unique) if unique.capacity() <= limit => {
            let held = unique.capacity();
            (unique.freeze(), held)
        }
        Ok(unique) => exact(&unique),
        Err(shared) => exact(&shared),
    })
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
        KeyLayout, LongTermKey, MemoryStore, ObjectPrefix, ObjectStore, ObjectStoreError, PutBody,
        PutOptions, TierExecutor, TierJobClass, TierJobHandle, TierJobRequest, UNHELD_SHARE,
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

    /// The partition that the tests read fresh objects as.
    fn reader() -> ObjectPrefix {
        source().key.prefix
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
        asked_keys: RefCell<Vec<String>>,
        gate: RefCell<Option<Shared<oneshot::Receiver<()>>>>,
        waiting: Cell<usize>,
        /// GETs that fail before the next one answers.
        failing: Cell<usize>,
    }

    impl Store {
        fn holding(len: u64) -> Rc<Self> {
            Self::with(object(len))
        }

        fn with(object: Bytes) -> Rc<Self> {
            Rc::new(Self {
                object,
                asked: RefCell::default(),
                asked_keys: RefCell::default(),
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
            key: &'a str,
            range: Range<u64>,
        ) -> LocalBoxFuture<'a, Result<ObjectRange, ObjectStoreError>> {
            self.asked.borrow_mut().push(range.clone());
            self.asked_keys.borrow_mut().push(key.to_owned());
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
                if range.start >= self.object.len() as u64 {
                    return Err(ObjectStoreError::InvalidRange {
                        key: key.to_owned(),
                        start: range.start,
                        end: range.end,
                    });
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

    /// A tier executor over a store that holds the object of [`source`],
    /// which starts speculative jobs only once its gate opens, as the tier
    /// service does once no demand fetch waits.
    #[derive(Clone)]
    struct Queueing {
        store: Rc<MemoryStore>,
        gate: Shared<oneshot::Receiver<()>>,
    }

    impl TierExecutor for Queueing {
        type Store = MemoryStore;

        fn submit<T, F, Fut>(&self, request: TierJobRequest, job: F) -> TierJobHandle<T>
        where
            T: 'static,
            F: FnOnce(Rc<Self::Store>) -> Fut + 'static,
            Fut: Future<Output = T> + 'static,
        {
            let store = Rc::clone(&self.store);
            let gate =
                (request.class() == TierJobClass::SpeculativeFetch).then(|| self.gate.clone());
            TierJobHandle::new(async move {
                if let Some(gate) = gate {
                    let _ = gate.await;
                }
                Ok(job(store).await)
            })
        }
    }

    /// Blocks of [`BLOCK`], and readahead fetches at the caps of the
    /// speculative fetches by default.
    const fn settings(cache_bytes: usize, read_ahead: u64) -> ReadCacheSettings {
        ReadCacheSettings {
            cache_bytes,
            block_bytes: BLOCK,
            read_ahead,
            read_ahead_fetches: 4,
            read_ahead_fetch_bytes: 32 * 1024 * 1024,
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
        RemoteSegments::new(demand, layout()).with_cache(ahead, &ReadCache::new(settings))
    }

    fn read_cache(remote: &RemoteSegments) -> &ReadCache {
        remote.blocks.as_ref().unwrap().cache()
    }

    async fn read_of(
        remote: &RemoteSegments,
        source: &RemoteRange,
        range: Range<u64>,
    ) -> Result<Vec<u8>, DiskFault> {
        read_for(remote, source, range, None).await
    }

    async fn read(remote: &RemoteSegments, range: Range<u64>) -> Result<Vec<u8>, DiskFault> {
        read_of(remote, &source(), range).await
    }

    /// [`read_of`] by a poll that reads `poll_offset` next.
    async fn read_for(
        remote: &RemoteSegments,
        source: &RemoteRange,
        range: Range<u64>,
        poll_offset: Option<u64>,
    ) -> Result<Vec<u8>, DiskFault> {
        let len = usize::try_from(range.end - range.start).unwrap();
        remote
            .read(
                source,
                range.start,
                len,
                None,
                poll_offset,
                &PartitionReadSettings::SHARD,
            )
            .await
            .map(|read| read.as_slice().to_vec())
    }

    /// [`read_of`] by a reader that goes on to `next` after `source`.
    async fn read_on(
        remote: &RemoteSegments,
        source: &RemoteRange,
        range: Range<u64>,
        next: &RemoteRange,
    ) -> Result<Vec<u8>, DiskFault> {
        let len = usize::try_from(range.end - range.start).unwrap();
        remote
            .read(
                source,
                range.start,
                len,
                Some(next),
                None,
                &PartitionReadSettings::SHARD,
            )
            .await
            .map(|read| read.as_slice().to_vec())
    }

    /// A gate that a test opens and closes, under an identity it sets.
    struct Gate(Cell<Option<u128>>);

    impl ReadGate for Gate {
        fn admit(&self) -> Option<u128> {
            self.0.get()
        }
    }

    /// The reads of a geo peer's store `store` through `cache`, in slot 1.
    fn peer(store: &Rc<Store>, cache: &Rc<ReadCache>, gate: &Rc<Gate>) -> RemoteSegments {
        let demand: Rc<dyn RemoteObjects> = Rc::clone(store) as _;
        RemoteSegments::through_cache(
            Rc::clone(&demand),
            demand,
            layout(),
            cache,
            StoreSlot(1),
            Rc::clone(gate) as Rc<dyn ReadGate>,
        )
    }

    /// The bytes of another object than [`object`], of `len` bytes.
    fn other_object(len: u64) -> Bytes {
        (0..len)
            .map(|byte| u8::try_from(250 - byte % 251).unwrap())
            .collect::<Vec<u8>>()
            .into()
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
                read_ahead_fetches: 4,
                read_ahead_fetch_bytes: 2 * block_bytes,
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
            assert!(read_cache(&remote).cache.borrow().held() <= budget);
            if step % 100 == 0 {
                settle().await;
            }
            last = range;
        }
        settle().await;
        assert!(read_cache(&remote).fetching.borrow().is_empty());
        assert!(read_cache(&remote).fetching_ahead.borrow().is_empty());
        assert_eq!(read_cache(&remote).ahead_fetches.get(), 0);
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

    /// Polls of later offsets that walk from the same index entry and fit
    /// one read read the same bytes, and are still one pass. Polls of the
    /// same offset read them again.
    #[compio::test]
    async fn given_polls_of_later_offsets_over_the_same_bytes_when_much_is_read_once_after_should_not_keep_their_blocks()
     {
        for (poll_offsets, kept) in [([0, 5, 10], false), ([0, 0, 0], true)] {
            let store = Store::holding(OBJECT_LEN);
            let remote = remote(&store, Rc::new(Silent), 10 * BLOCK_LEN, 0);
            for poll_offset in poll_offsets {
                assert_eq!(
                    read_for(&remote, &source(), 0..BLOCK / 2, Some(poll_offset)).await,
                    Ok(expected(0..BLOCK / 2))
                );
            }
            for start_offset in 1..=40 {
                let other = source_of(start_offset * 1000, OBJECT_LEN);
                assert_eq!(read_of(&remote, &other, 0..10).await, Ok(expected(0..10)));
            }
            let asked = store.asked().len();

            assert_eq!(read(&remote, 0..10).await, Ok(expected(0..10)));

            assert_eq!(store.asked().len() == asked, kept, "{poll_offsets:?}");
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

        assert!(read_cache(&remote).fetching.borrow().is_empty());
        assert_eq!(read(&remote, 100..200).await, Ok(expected(100..200)));
        assert_eq!(store.asked(), [(0, BLOCK)]);
    }

    #[compio::test]
    async fn given_a_failed_get_when_reading_again_should_ask_the_store_again() {
        let store = Store::holding(OBJECT_LEN);
        let remote = remote(&store, Rc::new(Silent), CACHE_BYTES, 0);
        store.failing.set(1);

        assert_eq!(read(&remote, 100..200).await, Err(DiskFault::Retryable));
        assert!(read_cache(&remote).fetching.borrow().is_empty());
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

        assert!(read_cache(&remote).fetching.borrow().is_empty());
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

        assert_eq!(remote.take_read_metrics().unwrap().cached_bytes, 0);
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
            &ReadCache::new(settings(CACHE_BYTES, READ_AHEAD)),
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
    async fn given_a_byte_cap_on_readahead_fetches_when_reading_in_order_should_fetch_ahead_in_pieces_that_fit_it()
     {
        for (fetch_bytes, fetched) in [
            (
                BLOCK + BLOCK / 2,
                vec![(2 * BLOCK, 3 * BLOCK), (3 * BLOCK, 4 * BLOCK)],
            ),
            (BLOCK - 1, vec![]),
        ] {
            let store = Store::holding(OBJECT_LEN);
            let ahead = Store::holding(OBJECT_LEN);
            let remote = remote_with(
                &store,
                ahead.clone(),
                ReadCacheSettings {
                    read_ahead_fetch_bytes: fetch_bytes,
                    ..settings(CACHE_BYTES, READ_AHEAD)
                },
            );

            assert_eq!(read(&remote, 0..BLOCK).await, Ok(expected(0..BLOCK)));
            assert_eq!(
                read(&remote, BLOCK - 1..2 * BLOCK).await,
                Ok(expected(BLOCK - 1..2 * BLOCK))
            );
            settle().await;

            assert_eq!(ahead.asked(), fetched, "{fetch_bytes}");
        }
    }

    /// Each read goes on from the last and doubles the window, while no
    /// readahead fetch ends.
    #[compio::test]
    async fn given_readahead_fetches_that_do_not_end_when_reading_in_order_should_keep_as_many_in_flight_as_the_setting()
     {
        const LONG: u64 = 16 * BLOCK;
        for fetches in [1, 6] {
            let store = Store::holding(LONG);
            let ahead = Store::holding(LONG);
            let _open = ahead.close();
            let remote = remote_with(
                &store,
                ahead.clone(),
                ReadCacheSettings {
                    read_ahead_fetches: fetches,
                    read_ahead_fetch_bytes: BLOCK,
                    ..settings(CACHE_BYTES, READ_AHEAD)
                },
            );
            let source = source_of(0, LONG);

            for block in 0..4 {
                let range = (block * BLOCK).saturating_sub(1)..(block + 1) * BLOCK;
                assert_eq!(
                    read_of(&remote, &source, range.clone()).await,
                    Ok(expected(range))
                );
                settle().await;
            }

            assert_eq!(ahead.asked().len(), fetches);
            assert_eq!(read_cache(&remote).ahead_fetches.get(), fetches);
        }
    }

    #[compio::test]
    async fn given_readahead_queued_until_reads_fetched_its_blocks_when_it_starts_should_not_ask_the_store()
     {
        let (open, gate) = oneshot::channel();
        let executor = Queueing {
            store: Recording::holding(false).await.store,
            gate: gate.shared(),
        };
        let remote = RemoteSegments::cached(
            executor,
            layout(),
            &ReadCache::new(settings(CACHE_BYTES, READ_AHEAD)),
        );
        assert_eq!(read(&remote, 0..BLOCK).await, Ok(expected(0..BLOCK)));
        assert_eq!(
            read(&remote, BLOCK - 1..2 * BLOCK).await,
            Ok(expected(BLOCK - 1..2 * BLOCK))
        );
        settle().await;
        assert_eq!(read_cache(&remote).ahead_fetches.get(), 1);
        // Not where the last read ended, so it fetches the blocks itself.
        let range = 2 * BLOCK + 10..4 * BLOCK - 10;
        assert_eq!(read(&remote, range.clone()).await, Ok(expected(range)));

        open.send(()).unwrap();
        settle().await;

        assert_eq!(read_cache(&remote).ahead_fetches.get(), 0);
        let metrics = remote.take_read_metrics().unwrap();
        assert_eq!(metrics.ahead_bytes, 0);
        assert_eq!(metrics.blocks_fetched_ahead, 0);
        assert_eq!(metrics.demand_bytes, 4 * BLOCK);
    }

    /// A rewrite hands the index of a long-term segment to the share of
    /// readahead beside the end of its log, which leaves the index room.
    #[compio::test]
    async fn given_a_carried_segment_longer_than_the_share_of_readahead_when_reading_its_index_should_take_it_from_memory()
     {
        const LOG_LEN: u64 = 5 * BLOCK;
        let store = Store::holding(LOG_LEN);
        let remote = remote(&store, Rc::new(Silent), 8 * BLOCK_LEN, 0);
        let source = RemoteRange {
            keyed_end_offset: Some(9),
            ..source_of(0, LOG_LEN)
        };
        let index = vec![7_u8; 4096];

        remote
            .carry_long_term(
                &source,
                &object(LOG_LEN),
                &index,
                &PartitionReadSettings::SHARD,
            )
            .await;

        assert_eq!(
            remote
                .read_long_term_index(&source, 4096, &PartitionReadSettings::SHARD)
                .await,
            Ok(Bytes::from(index))
        );
        assert_eq!(store.asked(), []);
    }

    /// A rewrite hands a long tail over a stride at a time, and the cache is
    /// free for the shard's other tasks between two.
    #[compio::test]
    async fn given_a_tail_of_three_strides_when_carrying_it_should_yield_between_them_with_the_cache_free()
     {
        const STRIDES: usize = 3;
        let store = Store::holding(OBJECT_LEN);
        let remote = remote(&store, Rc::new(Silent), CACHE_BYTES, 0);
        let log_len = (STRIDES * REWRITE_YIELD_BYTES) as u64;
        let source = RemoteRange {
            keyed_end_offset: Some(9),
            ..source_of(0, log_len)
        };
        let log = object(log_len);
        let mut carrying = std::pin::pin!(remote.carry_long_term(
            &source,
            &log,
            b"index",
            &PartitionReadSettings::SHARD
        ));

        let mut polls = 0;
        loop {
            polls += 1;
            if futures::poll!(carrying.as_mut()).is_ready() {
                break;
            }
            // Another task of the shard reads the cache meanwhile.
            assert_eq!(read_cache(&remote).take_metrics().carried_bytes, 0);
            compio::time::sleep(Duration::from_millis(5)).await;
        }

        assert_eq!(polls, STRIDES);
        assert_eq!(
            read_cache(&remote).take_metrics().carried_bytes,
            log_len + 5
        );
    }

    /// What a rewrite handed over is no readahead, so its first reads count
    /// as plain hits.
    #[compio::test]
    async fn given_a_carried_segment_when_reading_it_should_count_no_readahead_hits() {
        let store = Store::holding(OBJECT_LEN);
        let remote = remote(&store, Rc::new(Silent), CACHE_BYTES, 0);
        let source = RemoteRange {
            keyed_end_offset: Some(9),
            ..source()
        };
        remote
            .carry_long_term(
                &source,
                &object(OBJECT_LEN),
                b"index",
                &PartitionReadSettings::SHARD,
            )
            .await;
        let _ = read_cache(&remote).take_metrics();

        assert_eq!(
            read_of(&remote, &source, 0..OBJECT_LEN).await,
            Ok(expected(0..OBJECT_LEN))
        );
        assert_eq!(
            remote
                .read_long_term_index(&source, 5, &PartitionReadSettings::SHARD)
                .await,
            Ok(Bytes::from_static(b"index"))
        );

        let metrics = read_cache(&remote).take_metrics();
        assert_eq!(metrics.read_ahead_hits, 0);
        assert_eq!(metrics.block_hits, 7);
        assert_eq!(store.asked(), []);
    }

    /// A reader that takes each object in one read reaches the end of each,
    /// and goes on into the next with readahead.
    #[compio::test]
    async fn given_a_reader_that_reads_each_object_whole_when_it_names_the_next_should_fetch_ahead_into_the_one_after()
     {
        let store = Store::holding(OBJECT_LEN);
        let ahead = Store::holding(OBJECT_LEN);
        let remote = remote(&store, ahead.clone(), CACHE_BYTES, READ_AHEAD);
        let first = source_of(0, 2 * BLOCK);
        let second = source_of(1000, 2 * BLOCK);
        let third = source_of(2000, 4 * BLOCK);

        for (source, next) in [(&first, &second), (&second, &third)] {
            assert_eq!(
                read_on(&remote, source, 0..2 * BLOCK, next).await,
                Ok(expected(0..2 * BLOCK))
            );
        }
        settle().await;

        assert_eq!(
            *ahead.asked_keys.borrow(),
            [layout().segment_key(&third.key)]
        );
        assert_eq!(ahead.asked(), [(0, 2 * BLOCK)]);
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
            &ReadCache::new(settings(CACHE_BYTES, READ_AHEAD)),
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
    async fn given_two_stores_that_name_one_key_when_both_read_through_one_cache_should_each_get_their_own_bytes()
     {
        let cache = ReadCache::new(settings(CACHE_BYTES, 0));
        let own_store = Store::holding(OBJECT_LEN);
        let peer_store = Store::with(other_object(OBJECT_LEN));
        let own = RemoteSegments::new(Rc::clone(&own_store) as _, layout())
            .with_cache(Rc::new(Silent), &cache);
        let gate = Rc::new(Gate(Cell::new(Some(7))));
        let peer = peer(&peer_store, &cache, &gate);
        let range = 100..300_000;
        let theirs = other_object(OBJECT_LEN).slice(100..300_000).to_vec();

        for _ in 0..2 {
            assert_eq!(read(&own, range.clone()).await, Ok(expected(range.clone())));
            assert_eq!(read(&peer, range.clone()).await, Ok(theirs.clone()));
        }

        assert_eq!(own_store.asked(), [(0, 2 * BLOCK)]);
        assert_eq!(peer_store.asked(), [(0, 2 * BLOCK)]);
    }

    /// Readahead that passes the end of an object goes on into the one the
    /// reader names next, and the reader's first read there goes on with it.
    #[compio::test]
    async fn given_a_reader_at_the_end_of_an_object_when_it_names_the_next_should_fetch_the_start_of_the_next_ahead()
     {
        let store = Store::holding(OBJECT_LEN);
        let ahead = Store::holding(OBJECT_LEN);
        let remote = remote(&store, ahead.clone(), CACHE_BYTES, READ_AHEAD);
        let first = source_of(0, 2 * BLOCK);
        let next = source_of(1000, 4 * BLOCK);

        for range in [0..BLOCK, BLOCK - 1..2 * BLOCK] {
            assert_eq!(
                read_on(&remote, &first, range.clone(), &next).await,
                Ok(expected(range))
            );
        }
        settle().await;
        let next_key = layout().segment_key(&next.key);
        assert_eq!(*ahead.asked_keys.borrow(), std::slice::from_ref(&next_key));
        assert_eq!(ahead.asked(), [(0, 2 * BLOCK)]);
        let demand = store.asked().len();

        assert_eq!(read_of(&remote, &next, 0..10).await, Ok(expected(0..10)));
        settle().await;

        assert_eq!(store.asked().len(), demand);
        assert_eq!(*ahead.asked_keys.borrow(), [next_key.clone(), next_key]);
        assert_eq!(ahead.asked(), [(0, 2 * BLOCK), (2 * BLOCK, 4 * BLOCK)]);
    }

    /// What readahead of one store fetched never answers a read of another
    /// store, even under the same key.
    #[compio::test]
    async fn given_readahead_of_a_peer_into_the_next_object_when_the_own_store_reads_that_object_should_fetch_its_own_bytes()
     {
        let cache = ReadCache::new(settings(CACHE_BYTES, READ_AHEAD));
        let own_store = Store::holding(OBJECT_LEN);
        let peer_store = Store::with(other_object(4 * BLOCK));
        let own = RemoteSegments::new(Rc::clone(&own_store) as _, layout())
            .with_cache(Rc::new(Silent), &cache);
        let gate = Rc::new(Gate(Cell::new(Some(7))));
        let peer = peer(&peer_store, &cache, &gate);
        let first = source_of(0, 2 * BLOCK);
        let next = source_of(1000, 4 * BLOCK);
        for range in [0..BLOCK, BLOCK - 1..2 * BLOCK] {
            read_on(&peer, &first, range, &next).await.unwrap();
        }
        settle().await;
        let peer_asked = peer_store.asked().len();

        assert_eq!(read_of(&own, &next, 0..10).await, Ok(expected(0..10)));
        assert_eq!(
            read_of(&peer, &next, 0..10).await,
            Ok(other_object(10).to_vec())
        );

        assert_eq!(own_store.asked(), [(0, BLOCK)]);
        assert_eq!(peer_store.asked().len(), peer_asked);
    }

    #[compio::test]
    async fn given_a_long_term_index_read_twice_when_reading_should_ask_the_store_once_for_the_whole_object()
     {
        let store = Store::holding(30_000);
        let remote = remote(&store, Rc::new(Silent), CACHE_BYTES, 0);
        let log = RemoteRange {
            keyed_end_offset: Some(99),
            ..source()
        };

        for _ in 0..2 {
            let index = remote
                .read_long_term_index(&log, 4 * BLOCK, &PartitionReadSettings::SHARD)
                .await;
            assert_eq!(index.as_deref(), Ok(&expected(0..30_000)[..]));
        }

        assert_eq!(store.asked(), [(0, 4 * BLOCK)]);
        let key = layout().long_term_key(&LongTermKey {
            prefix: log.key.prefix,
            start_offset: log.key.start_offset,
            end_offset: 99,
            object: SegmentObject::Index,
        });
        assert_eq!(*store.asked_keys.borrow(), [key]);
    }

    #[compio::test]
    async fn given_a_long_term_index_missed_by_two_reads_at_once_when_both_wait_should_ask_the_store_once()
     {
        let store = Store::holding(30_000);
        let remote = remote(&store, Rc::new(Silent), CACHE_BYTES, 0);
        let log = RemoteRange {
            keyed_end_offset: Some(99),
            ..source()
        };
        let open = store.close();
        let mut first =
            Box::pin(remote.read_long_term_index(&log, 4 * BLOCK, &PartitionReadSettings::SHARD));
        let mut second =
            Box::pin(remote.read_long_term_index(&log, 4 * BLOCK, &PartitionReadSettings::SHARD));
        while store.waiting.get() == 0 {
            assert!(futures::poll!(first.as_mut()).is_pending());
            settle().await;
        }
        assert!(futures::poll!(second.as_mut()).is_pending());

        open.send(()).unwrap();

        assert_eq!(first.await.unwrap().len(), 30_000);
        assert_eq!(second.await.unwrap().len(), 30_000);
        assert_eq!(store.asked().len(), 1);
    }

    /// With no bytes for the cache, what reads fetch is not kept, so each
    /// read asks the store as it did before the cache.
    #[compio::test]
    async fn given_no_cache_budget_when_reading_an_index_or_a_log_twice_should_ask_the_store_each_time()
     {
        let store = Store::holding(OBJECT_LEN);
        let remote = remote(&store, Rc::new(Silent), 0, READ_AHEAD);
        let log = RemoteRange {
            keyed_end_offset: Some(99),
            ..source()
        };

        for _ in 0..2 {
            remote
                .read_long_term_index(&log, 1000, &PartitionReadSettings::SHARD)
                .await
                .unwrap();
            assert_eq!(read_of(&remote, &log, 10..20).await, Ok(expected(10..20)));
        }

        assert_eq!(store.asked(), [(0, 1000), (10, 20), (0, 1000), (10, 20)]);
        assert_eq!(remote.take_read_metrics().unwrap().cached_bytes, 0);
    }

    #[compio::test]
    async fn given_a_fresh_range_read_twice_through_a_cache_when_reading_should_ask_the_store_once_and_return_the_object_s_bytes()
     {
        let store = Store::holding(OBJECT_LEN);
        let remote = remote(&store, Rc::new(Silent), CACHE_BYTES, READ_AHEAD);
        let ranges = [
            100..200,
            BLOCK - 5..BLOCK + 5,
            1000..3 * BLOCK + 7,
            OBJECT_LEN - 10..OBJECT_LEN,
        ];

        for _ in 0..2 {
            for range in ranges.clone() {
                let read = remote
                    .read_fresh(7, range.clone(), reader(), &PartitionReadSettings::SHARD)
                    .await;
                assert_eq!(read.as_deref(), Ok(&expected(range)[..]));
            }
        }

        // Each block once, the last one cut where the object ends.
        assert_eq!(
            store.asked(),
            [
                (0, BLOCK),
                (BLOCK, 2 * BLOCK),
                (2 * BLOCK, 4 * BLOCK),
                (5 * BLOCK, 6 * BLOCK)
            ]
        );
    }

    /// The store says where the object ends, so its last block is whole as
    /// it is kept. A block past the end fails its GET and is not kept.
    #[compio::test]
    async fn given_a_fresh_object_shorter_than_its_placement_when_reading_should_fail_and_keep_only_its_own_bytes()
     {
        let store = Store::holding(BLOCK + 100);
        let remote = remote(&store, Rc::new(Silent), CACHE_BYTES, 0);

        assert_eq!(
            remote
                .read_fresh(
                    7,
                    BLOCK + 50..BLOCK + 200,
                    reader(),
                    &PartitionReadSettings::SHARD
                )
                .await,
            Err(DiskFault::Tiered)
        );
        assert_eq!(
            remote
                .read_fresh(
                    7,
                    2 * BLOCK + 1..2 * BLOCK + 9,
                    reader(),
                    &PartitionReadSettings::SHARD
                )
                .await,
            Err(DiskFault::Tiered)
        );

        assert_eq!(remote.take_read_metrics().unwrap().cached_bytes, 100);
        assert!(read_cache(&remote).fetching.borrow().is_empty());
    }

    #[compio::test]
    async fn given_no_cache_budget_when_reading_a_fresh_range_twice_should_ask_the_store_each_time()
    {
        let store = Store::holding(OBJECT_LEN);
        let remote = remote(&store, Rc::new(Silent), 0, READ_AHEAD);

        for _ in 0..2 {
            let read = remote
                .read_fresh(7, 100..200, reader(), &PartitionReadSettings::SHARD)
                .await;
            assert_eq!(read.as_deref(), Ok(&expected(100..200)[..]));
        }

        assert_eq!(store.asked(), [(100, 200), (100, 200)]);
        assert_eq!(remote.take_read_metrics().unwrap().cached_bytes, 0);
    }

    /// What the shard uploaded serves its polls whatever the cache keeps of
    /// what it fetches.
    #[compio::test]
    async fn given_a_fresh_object_this_shard_uploaded_when_polls_read_its_blobs_should_not_ask_the_store()
     {
        for cache_bytes in [0, CACHE_BYTES] {
            let store = Store::holding(OBJECT_LEN);
            let remote = remote(&store, Rc::new(Silent), cache_bytes, 0);
            remote.cache_fresh_object(7, object(OBJECT_LEN), std::iter::empty());

            let blob = remote
                .read_fresh(7, 100..200, reader(), &PartitionReadSettings::SHARD)
                .await;
            let fetched = remote
                .fetch_fresh(
                    7,
                    [300..400, 400..500],
                    reader(),
                    &PartitionReadSettings::SHARD,
                )
                .await
                .unwrap();

            assert_eq!(blob.as_deref(), Ok(&expected(100..200)[..]));
            assert_eq!(fetched.get(7, &(400..500)).unwrap(), expected(400..500));
            assert_eq!(store.asked(), [], "a cache of {cache_bytes} bytes");
        }
    }

    /// A poll of batches far apart in a fresh object fetches the blocks of
    /// those batches, not the messages of other partitions between them.
    #[compio::test]
    async fn given_blobs_far_apart_in_a_fresh_object_when_fetching_them_through_the_cache_should_fetch_only_their_blocks()
     {
        let store = Store::holding(OBJECT_LEN);
        let remote = remote(&store, Rc::new(Silent), CACHE_BYTES, 0);
        let far = 4 * BLOCK + 10..4 * BLOCK + 20;

        let fetched = remote
            .fetch_fresh(
                7,
                [far.clone(), 10..20],
                reader(),
                &PartitionReadSettings::SHARD,
            )
            .await
            .unwrap();

        assert_eq!(fetched.get(7, &(10..20)).unwrap(), expected(10..20));
        assert_eq!(fetched.get(7, &far).unwrap(), expected(far));
        assert_eq!(store.asked(), [(0, BLOCK), (4 * BLOCK, 5 * BLOCK)]);
    }

    /// Bytes the cache would refuse are not copied for it first.
    #[test]
    fn given_bytes_longer_than_the_cache_when_holding_them_should_take_nothing() {
        assert_eq!(held_within(Bytes::from(vec![0; 11]), 10), None);
        let shared = Bytes::from(vec![0; 10]);
        let _other_owner = shared.clone();
        assert_eq!(held_within(shared, 10).map(|(_, taken)| taken), Some(10));
    }

    /// Uploads end in any order, so the object the cache lets go of past
    /// the held share can be any of those it holds. Its rows go with it.
    #[compio::test]
    async fn given_objects_held_out_of_id_order_when_the_cache_lets_one_go_should_drop_its_rows() {
        let store = Store::holding(OBJECT_LEN);
        let remote = remote(&store, Rc::new(Silent), 8 * BLOCK_LEN, 0);
        for object_id in [10, 3, 4, 5, 6, 7, 8] {
            remote.cache_fresh_object(object_id, object(BLOCK), [partition(1)]);
        }

        remote.cache_fresh_object(9, object(BLOCK), [partition(1)]);

        let rows: Vec<u128> = read_cache(&remote)
            .held
            .borrow()
            .objects
            .keys()
            .copied()
            .collect();
        assert_eq!(rows, [3, 4, 5, 6, 7, 8, 9]);
        assert!(!remote.holds_fresh_object(10));
    }

    #[compio::test]
    async fn given_an_uploaded_object_when_much_is_fetched_after_it_should_still_hold_it() {
        let store = Store::holding(OBJECT_LEN);
        let remote = remote(&store, Rc::new(Silent), 8 * BLOCK_LEN, 0);
        remote.cache_fresh_object(7, object(BLOCK), std::iter::empty());

        for other in 100..140 {
            remote
                .read_fresh(other, 0..BLOCK, reader(), &PartitionReadSettings::SHARD)
                .await
                .unwrap();
        }
        let asked = store.asked().len();

        assert_eq!(
            remote
                .read_fresh(7, 10..20, reader(), &PartitionReadSettings::SHARD)
                .await
                .as_deref(),
            Ok(&expected(10..20)[..])
        );
        assert_eq!(store.asked().len(), asked);
    }

    /// A buffer the shard built and no longer shares is held as it is, and
    /// counts as its capacity. A shared one is copied to its length: its
    /// owner cannot tell what else it holds.
    #[compio::test]
    async fn given_uploaded_buffers_when_held_should_count_the_memory_they_take() {
        let store = Store::holding(OBJECT_LEN);
        let remote = remote(&store, Rc::new(Silent), CACHE_BYTES, 0);
        let mut grown = bytes::BytesMut::with_capacity(1200);
        grown.resize(600, b'x');
        let mut shared = bytes::BytesMut::with_capacity(1200);
        shared.resize(100, b'y');
        let shared = shared.freeze();

        remote.cache_fresh_object(1, grown.freeze(), std::iter::empty());
        assert_eq!(remote.take_read_metrics().unwrap().cached_bytes, 1200);
        remote.cache_fresh_object(2, shared.clone(), std::iter::empty());
        assert_eq!(remote.take_read_metrics().unwrap().cached_bytes, 1300);
        assert_eq!(
            remote
                .read_fresh(2, 0..100, reader(), &PartitionReadSettings::SHARD)
                .await
                .unwrap(),
            shared
        );
    }

    fn partition(id: usize) -> IggyNamespace {
        IggyNamespace::new(1, 1, id)
    }

    /// Whether the cache holds the fresh object `object_id` apart, as one
    /// that the rewrite has yet to release.
    fn holds_apart(remote: &RemoteSegments, object_id: u128) -> bool {
        read_cache(remote)
            .cache
            .borrow()
            .is_held(&CacheKey::Whole(ObjectId::own_fresh(object_id)))
    }

    /// An object holds the batches of several partitions, and the polls of
    /// each read it until its own long-term floor passes it.
    #[compio::test]
    async fn given_an_uploaded_object_of_two_partitions_when_their_floors_pass_it_should_release_it_after_the_last()
     {
        for cache_bytes in [0, CACHE_BYTES] {
            let store = Store::holding(OBJECT_LEN);
            let remote = remote(&store, Rc::new(Silent), cache_bytes, 0);
            remote.cache_fresh_object(7, object(BLOCK), [partition(1), partition(2)]);
            remote.note_fresh_commit(partition(1), 7, 10);
            remote.note_fresh_commit(partition(2), 7, 40);
            remote.note_fresh_commit(partition(1), 7, 12);
            remote.settle_fresh_object(7);

            remote.note_long_term_floor(partition(1), 11);
            remote.note_long_term_floor(partition(2), 50);
            assert!(holds_apart(&remote, 7), "a cache of {cache_bytes} bytes");
            remote.note_long_term_floor(partition(1), 12);

            assert!(!holds_apart(&remote, 7), "a cache of {cache_bytes} bytes");
            let fresh = read_cache(&remote).held.borrow();
            assert!(fresh.objects.is_empty() && fresh.by_end.is_empty());
        }
    }

    /// With nothing else to keep, an object the rewrites released leaves.
    #[compio::test]
    async fn given_no_cache_budget_when_an_uploaded_object_is_released_should_drop_it() {
        let store = Store::holding(OBJECT_LEN);
        let remote = remote(&store, Rc::new(Silent), 0, 0);
        remote.cache_fresh_object(7, object(BLOCK), [partition(1)]);
        remote.note_fresh_commit(partition(1), 7, 10);

        remote.note_long_term_floor(partition(1), 10);

        assert_eq!(remote.take_read_metrics().unwrap().cached_bytes, 0);
        assert_eq!(read_fresh_asked(&remote, &store, 7).await, 1);
    }

    /// How many GETs a read of the fresh object `object_id` asks for.
    async fn read_fresh_asked(remote: &RemoteSegments, store: &Store, object_id: u128) -> usize {
        let before = store.asked().len();
        remote
            .read_fresh(object_id, 10..20, reader(), &PartitionReadSettings::SHARD)
            .await
            .unwrap();
        store.asked().len() - before
    }

    /// A send the shard answered as not accepted, or one whose partition
    /// left, never makes its partition read the object.
    #[compio::test]
    async fn given_a_partition_that_committed_nothing_in_an_uploaded_object_when_its_sends_settle_should_not_keep_it_held()
     {
        let store = Store::holding(OBJECT_LEN);
        let remote = remote(&store, Rc::new(Silent), CACHE_BYTES, 0);
        remote.cache_fresh_object(7, object(BLOCK), [partition(1), partition(2), partition(3)]);
        remote.note_fresh_commit(partition(1), 7, 10);
        remote.note_fresh_commit(partition(3), 7, 10);

        remote.settle_fresh_object(7);
        remote.forget_partition(partition(3));
        assert!(holds_apart(&remote, 7));
        remote.note_long_term_floor(partition(1), 10);

        assert!(!holds_apart(&remote, 7));
        assert_eq!(read_fresh_asked(&remote, &store, 7).await, 0, "still kept");
    }

    /// A floor that stops moving never lets held objects take more than
    /// their share, and what the cache released on its own is released once.
    #[compio::test]
    async fn given_a_floor_that_never_moves_when_the_shard_uploads_on_should_hold_no_more_than_the_held_share()
     {
        let budget = 16 * BLOCK_LEN;
        let store = Store::holding(OBJECT_LEN);
        let remote = remote(&store, Rc::new(Silent), budget, 0);

        for object_id in 0..40 {
            remote.cache_fresh_object(object_id, object(BLOCK), [partition(1)]);
            remote.note_fresh_commit(partition(1), object_id, u64::try_from(object_id).unwrap());
            let cache = read_cache(&remote).cache.borrow();
            assert!(cache.held_apart() <= budget - budget / UNHELD_SHARE);
        }
        let rows = read_cache(&remote).held.borrow().objects.len();
        assert!(rows <= 16, "{rows} rows for objects the cache let go");

        remote.note_long_term_floor(partition(1), 39);

        let fresh = read_cache(&remote).held.borrow();
        assert!(fresh.objects.is_empty() && fresh.by_end.is_empty());
        assert_eq!(read_cache(&remote).cache.borrow().held_apart(), 0);
    }

    /// With no bytes for the cache, what the shard uploads is held in a
    /// bound of its own.
    #[compio::test]
    async fn given_no_cache_budget_when_the_shard_uploads_more_than_the_fresh_bound_should_hold_no_more_than_it()
     {
        let store = Store::holding(OBJECT_LEN);
        let remote = remote(&store, Rc::new(Silent), 0, 0);
        let upload = Bytes::from(vec![7; 4 * 1024 * 1024]);

        for object_id in 0..24 {
            remote.cache_fresh_object(object_id, upload.clone(), [partition(1)]);
            let held = remote.take_read_metrics().unwrap().cached_bytes;
            assert!(held <= FRESH_CACHE_BYTES as u64, "{held}");
        }

        assert!(holds_apart(&remote, 23));
        assert_eq!(read_fresh_asked(&remote, &store, 23).await, 0);
    }

    /// A peer that may not be read now gets no request, and its bytes in
    /// the cache are not served either.
    #[compio::test]
    async fn given_a_peer_that_stopped_admitting_reads_when_its_bytes_are_cached_should_refuse_them_without_a_get()
     {
        let cache = ReadCache::new(settings(CACHE_BYTES, 0));
        let store = Store::holding(OBJECT_LEN);
        let gate = Rc::new(Gate(Cell::new(Some(7))));
        let peer = peer(&store, &cache, &gate);
        assert_eq!(read(&peer, 100..200).await, Ok(expected(100..200)));

        gate.0.set(None);

        assert_eq!(read(&peer, 100..200).await, Err(DiskFault::Retryable));
        assert_eq!(store.asked(), [(0, BLOCK)]);
    }

    /// No read reaches what a peer's blocks were kept under once its gate
    /// moves, so they leave the cache, and the blocks of other stores stay.
    #[compio::test]
    async fn given_a_peer_whose_gate_moved_when_read_again_should_drop_its_blocks_and_keep_the_others()
     {
        let cache = ReadCache::new(settings(CACHE_BYTES, 0));
        let own = RemoteSegments::new(Store::holding(OBJECT_LEN) as _, layout())
            .with_cache(Rc::new(Silent), &cache);
        let gate = Rc::new(Gate(Cell::new(Some(7))));
        let peer = peer(&Store::holding(OBJECT_LEN), &cache, &gate);
        assert_eq!(read(&own, 100..200).await, Ok(expected(100..200)));
        assert_eq!(read(&peer, 100..200).await, Ok(expected(100..200)));
        assert_eq!(cache.take_metrics().cached_bytes, 2 * BLOCK);

        gate.0.set(None);

        assert_eq!(read(&peer, 100..200).await, Err(DiskFault::Retryable));
        assert_eq!(
            cache.take_metrics().cached_bytes,
            BLOCK,
            "the own store's block"
        );
    }

    /// A root that another cluster owns now can name other bytes under the
    /// same key, so the bytes read under the old owner are not served.
    #[compio::test]
    async fn given_a_peer_whose_owner_changed_when_reading_bytes_cached_under_the_old_one_should_fetch_them_again()
     {
        let cache = ReadCache::new(settings(CACHE_BYTES, 0));
        let store = Store::holding(OBJECT_LEN);
        let gate = Rc::new(Gate(Cell::new(Some(7))));
        let peer = peer(&store, &cache, &gate);
        assert_eq!(read(&peer, 100..200).await, Ok(expected(100..200)));

        gate.0.set(Some(8));

        assert_eq!(read(&peer, 100..200).await, Ok(expected(100..200)));
        assert_eq!(store.asked(), [(0, BLOCK), (0, BLOCK)]);
    }

    /// Every store a shard reads takes from one budget.
    #[compio::test]
    async fn given_reads_of_the_own_store_and_of_peers_when_cached_should_keep_within_one_budget() {
        let budget = 8 * BLOCK_LEN;
        let cache = ReadCache::new(settings(budget, 0));
        let own_store = Store::holding(16 * BLOCK);
        let own = RemoteSegments::new(Rc::clone(&own_store) as _, layout())
            .with_cache(Rc::new(Silent), &cache);
        let gate = Rc::new(Gate(Cell::new(Some(7))));
        let peers: Vec<RemoteSegments> = (1..=3)
            .map(|slot| {
                let store = Store::holding(16 * BLOCK);
                let demand: Rc<dyn RemoteObjects> = store as _;
                RemoteSegments::through_cache(
                    Rc::clone(&demand),
                    demand,
                    layout(),
                    &cache,
                    StoreSlot(slot),
                    Rc::clone(&gate) as Rc<dyn ReadGate>,
                )
            })
            .collect();
        let source = source_of(0, 16 * BLOCK);

        for remote in std::iter::once(&own).chain(&peers) {
            for block in 0..16 {
                read_of(remote, &source, block * BLOCK..block * BLOCK + 10)
                    .await
                    .unwrap();
            }
        }

        assert!(cache.take_metrics().cached_bytes <= budget as u64);
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

    /// The segment of `len` bytes of partition `partition_id` of a topic of
    /// this cluster, which the cache counts apart from the others.
    fn partition_source(partition_id: u64, len: u64) -> RemoteRange {
        let mut source = source_of(0, len);
        source.key.prefix.partition_id = partition_id;
        source
    }

    /// The settings of a partition whose limit is `blocks` blocks.
    const fn limited(blocks: usize) -> PartitionReadSettings {
        PartitionReadSettings {
            limit: Some(blocks * BLOCK_LEN),
            read_ahead: None,
            left: None,
        }
    }

    async fn read_as(
        remote: &RemoteSegments,
        source: &RemoteRange,
        range: Range<u64>,
        settings: &PartitionReadSettings,
    ) -> Result<Vec<u8>, DiskFault> {
        let len = usize::try_from(range.end - range.start).unwrap();
        remote
            .read(source, range.start, len, None, None, settings)
            .await
            .map(|read| read.as_slice().to_vec())
    }

    /// Blocks of `source` of the node's own store that the cache holds.
    fn cached_blocks(remote: &RemoteSegments, source: &RemoteRange) -> u64 {
        let object = ObjectId::segment(OWN_STORE, 0, source);
        let last = object.last_block(BLOCK).unwrap();
        let cache = read_cache(remote).cache.borrow();
        (0..=last)
            .filter(|index| {
                cache.holds(&CacheKey::Block {
                    object,
                    index: *index,
                })
            })
            .count() as u64
    }

    /// A partition over its limit evicts its own blocks, so a long history
    /// it reads once leaves what the others hold where it was.
    #[compio::test]
    async fn given_a_partition_with_a_small_limit_reading_a_long_history_when_another_holds_blocks_should_keep_them()
     {
        let store = Store::holding(40 * BLOCK);
        let remote = remote(&store, Rc::new(Silent), CACHE_BYTES, 0);
        let (shared, long) = (
            partition_source(1, 40 * BLOCK),
            partition_source(2, 40 * BLOCK),
        );
        read_as(
            &remote,
            &shared,
            0..4 * BLOCK,
            &PartitionReadSettings::SHARD,
        )
        .await
        .unwrap();

        for block in 0..40 {
            let range = block * BLOCK..(block + 1) * BLOCK;
            assert_eq!(
                read_as(&remote, &long, range.clone(), &limited(8)).await,
                Ok(expected(range))
            );
        }

        assert_eq!(cached_blocks(&remote, &shared), 4);
        assert_eq!(cached_blocks(&remote, &long), 8);
        assert_eq!(read_cache(&remote).take_metrics().limit_evictions, 32);
    }

    /// A limit below eight blocks keeps nothing, as a cache of no bytes does:
    /// each read asks the store for exactly its range. Lifted, the reads go
    /// through blocks again.
    #[compio::test]
    async fn given_a_limit_under_eight_blocks_when_reading_should_ask_the_store_for_exactly_each_range_and_keep_nothing()
     {
        let store = Store::holding(OBJECT_LEN);
        let remote = remote(&store, Rc::new(Silent), CACHE_BYTES, READ_AHEAD);
        let nothing = PartitionReadSettings {
            limit: Some(0),
            read_ahead: None,
            left: None,
        };

        let under = limited(usize::try_from(LIMIT_BLOCKS_MIN).unwrap() - 1);
        for settings in [under, nothing] {
            for _ in 0..2 {
                assert_eq!(
                    read_as(&remote, &source(), 100..300, &settings).await,
                    Ok(expected(100..300))
                );
            }
        }
        settle().await;

        assert_eq!(store.asked(), [(100, 300); 4]);
        assert_eq!(read_cache(&remote).take_metrics().cached_bytes, 0);
        read_as(&remote, &source(), 100..300, &PartitionReadSettings::SHARD)
            .await
            .unwrap();
        assert_eq!(store.asked()[4], (0, BLOCK));
    }

    /// Readahead keeps at most an eighth of a partition's limit in front of
    /// its readers, or of the shard's budget when that is smaller, and none
    /// below two blocks.
    #[compio::test]
    async fn given_partition_limits_and_readahead_when_reading_in_order_should_fetch_ahead_within_them()
     {
        // (row, the shard's budget, settings, the most blocks fetched ahead
        // of the reader)
        for (row, cache_bytes, settings, blocks_ahead) in [
            (
                "the shard's readahead",
                CACHE_BYTES,
                PartitionReadSettings::SHARD,
                8,
            ),
            ("an eighth of the limit", CACHE_BYTES, limited(32), 4),
            (
                "an eighth of the limit is one block",
                CACHE_BYTES,
                limited(8),
                0,
            ),
            (
                "the partition's own readahead",
                CACHE_BYTES,
                PartitionReadSettings {
                    limit: None,
                    read_ahead: Some(2 * BLOCK),
                    left: None,
                },
                2,
            ),
            (
                "the partition's readahead is one block",
                CACHE_BYTES,
                PartitionReadSettings {
                    limit: None,
                    read_ahead: Some(BLOCK),
                    left: None,
                },
                0,
            ),
            (
                "the partition's readahead past an eighth of the budget",
                32 * BLOCK_LEN,
                PartitionReadSettings {
                    limit: None,
                    read_ahead: Some(16 * BLOCK),
                    left: None,
                },
                4,
            ),
            (
                "the partition's readahead past an eighth of a limit above the budget",
                32 * BLOCK_LEN,
                PartitionReadSettings {
                    limit: Some(64 * BLOCK_LEN),
                    read_ahead: Some(16 * BLOCK),
                    left: None,
                },
                4,
            ),
        ] {
            let len = 40 * BLOCK;
            let (store, ahead) = (Store::holding(len), Store::holding(len));
            let remote = remote(&store, Rc::clone(&ahead) as _, cache_bytes, READ_AHEAD);
            let source = partition_source(1, len);
            for block in 0..12 {
                read_as(
                    &remote,
                    &source,
                    block * BLOCK..(block + 1) * BLOCK,
                    &settings,
                )
                .await
                .unwrap();
                settle().await;
            }

            let reach = ahead.asked().iter().map(|(_, end)| *end).max().unwrap_or(0);
            assert_eq!(
                reach.div_ceil(BLOCK).saturating_sub(12),
                blocks_ahead,
                "{row}"
            );
        }
    }

    /// A partition's own readahead keeps within an eighth of the shard's
    /// cache when its limit is unset or above that cache.
    #[compio::test]
    async fn given_a_partition_readahead_above_an_eighth_of_the_shard_cache_when_reading_in_order_should_fetch_ahead_within_it()
     {
        const SHARD_CACHE_BYTES: usize = 32 * BLOCK_LEN;
        for (row, limit) in [
            ("no limit", None),
            ("a limit above the cache", Some(2 * SHARD_CACHE_BYTES)),
        ] {
            let len = 40 * BLOCK;
            let (store, ahead) = (Store::holding(len), Store::holding(len));
            let remote = remote(&store, Rc::clone(&ahead) as _, SHARD_CACHE_BYTES, 2 * BLOCK);
            let source = partition_source(1, len);
            let settings = PartitionReadSettings {
                limit,
                read_ahead: Some(16 * BLOCK),
                left: None,
            };
            for block in 0..12 {
                read_as(
                    &remote,
                    &source,
                    block * BLOCK..(block + 1) * BLOCK,
                    &settings,
                )
                .await
                .unwrap();
                settle().await;
            }

            let reach = ahead.asked().iter().map(|(_, end)| *end).max().unwrap_or(0);
            let an_eighth_of_the_cache = (SHARD_CACHE_BYTES / AHEAD_SHARE / BLOCK_LEN) as u64;
            assert_eq!(
                reach.div_ceil(BLOCK).saturating_sub(12),
                an_eighth_of_the_cache,
                "{row}"
            );
        }
    }

    /// The settings of a partition that can leave the shard, and its flag.
    fn leaving() -> (PartitionReadSettings, PartitionLeft) {
        let left = PartitionLeft::default();
        let settings = PartitionReadSettings {
            left: Some(left.clone()),
            ..PartitionReadSettings::SHARD
        };
        (settings, left)
    }

    /// What a GET in flight for a partition that left the shard fetches is
    /// not kept, and nothing else of the partition is either.
    #[compio::test]
    async fn given_a_partition_that_left_the_shard_when_its_get_in_flight_ends_should_keep_nothing()
    {
        let store = Store::holding(OBJECT_LEN);
        let remote = remote(&store, Rc::new(Silent), CACHE_BYTES, 0);
        let (settings, left) = leaving();
        read_as(&remote, &source(), 0..BLOCK, &settings)
            .await
            .unwrap();
        let open = store.close();
        let source = source();
        let mut in_flight = Box::pin(read_as(&remote, &source, BLOCK..2 * BLOCK, &settings));
        assert!(futures::poll!(in_flight.as_mut()).is_pending());

        left.set();
        remote.forget_reader(source.key.prefix);
        open.send(()).unwrap();

        assert_eq!(in_flight.await, Ok(expected(BLOCK..2 * BLOCK)));
        assert_eq!(cached_blocks(&remote, &source), 0);
        assert_eq!(read_cache(&remote).take_metrics().cached_bytes, 0);
        read_as(
            &remote,
            &source,
            BLOCK..2 * BLOCK,
            &PartitionReadSettings::SHARD,
        )
        .await
        .unwrap();
        assert_eq!(cached_blocks(&remote, &source), 1, "a new read keeps again");
    }

    /// A poll that outlives its partition, after the last GET it had in
    /// flight ended, starts no GET for it and leaves nothing in the cache,
    /// through the cache or past it.
    #[compio::test]
    async fn given_a_partition_that_left_the_shard_when_its_poll_reads_on_after_its_gets_ended_should_start_no_get_and_keep_nothing()
     {
        let store = Store::holding(OBJECT_LEN);
        let remote = remote(&store, Rc::new(Silent), CACHE_BYTES, 0);
        let (settings, left) = leaving();
        read_as(&remote, &source(), 0..BLOCK, &settings)
            .await
            .unwrap();

        left.set();
        remote.forget_reader(source().key.prefix);
        let under = PartitionReadSettings {
            limit: Some(BLOCK_LEN),
            ..settings.clone()
        };

        assert_eq!(
            read_as(&remote, &source(), BLOCK..2 * BLOCK, &settings).await,
            Err(DiskFault::Retryable)
        );
        assert_eq!(
            read_as(&remote, &source(), 0..100, &under).await,
            Err(DiskFault::Retryable),
            "nor past the cache"
        );
        assert_eq!(
            remote.read_long_term_index(&source(), 5, &settings).await,
            Err(DiskFault::Retryable)
        );
        settle().await;
        assert_eq!(store.asked(), [(0, BLOCK)]);
        assert_eq!(read_cache(&remote).take_metrics().cached_bytes, 0);
        let owner = ReadOwner {
            store: OWN_STORE,
            prefix: source().key.prefix,
        };
        assert_eq!(read_cache(&remote).cache.borrow().limit(&owner), None);
    }

    /// A rewrite that hands a segment over a stride at a time stops once its
    /// partition leaves the shard, and what it handed over before goes.
    #[compio::test]
    async fn given_a_partition_that_leaves_the_shard_while_a_rewrite_carries_its_segment_should_keep_nothing_of_it()
     {
        let store = Store::holding(OBJECT_LEN);
        let remote = remote(&store, Rc::new(Silent), CACHE_BYTES, 0);
        let log_len = (3 * REWRITE_YIELD_BYTES) as u64;
        let source = RemoteRange {
            keyed_end_offset: Some(9),
            ..source_of(0, log_len)
        };
        let log = object(log_len);
        let (settings, left) = leaving();
        let mut carrying =
            std::pin::pin!(remote.carry_long_term(&source, &log, b"index", &settings));
        assert!(futures::poll!(carrying.as_mut()).is_pending());
        assert!(read_cache(&remote).take_metrics().cached_bytes > 0);

        left.set();
        remote.forget_reader(source.key.prefix);
        carrying.await;

        let metrics = read_cache(&remote).take_metrics();
        assert_eq!(metrics.cached_bytes, 0);
        assert_eq!(metrics.carried_bytes, 0);
        assert!(read_cache(&remote).carried.borrow().is_empty());
    }

    /// A peer's partition and one of the node's own read within one budget,
    /// and the peer's over its limit evicts only its own blocks.
    #[compio::test]
    async fn given_a_peer_partition_over_its_limit_and_an_own_one_when_reading_should_share_the_budget()
     {
        let budget = 16 * BLOCK_LEN;
        let cache = ReadCache::new(settings(budget, 0));
        let own_store = Store::holding(40 * BLOCK);
        let own = RemoteSegments::new(Rc::clone(&own_store) as _, layout())
            .with_cache(Rc::new(Silent), &cache);
        let gate = Rc::new(Gate(Cell::new(Some(7))));
        let peer = peer(&Store::holding(40 * BLOCK), &cache, &gate);
        let source = partition_source(1, 40 * BLOCK);
        read_as(&own, &source, 0..4 * BLOCK, &PartitionReadSettings::SHARD)
            .await
            .unwrap();

        for block in 0..40 {
            let range = block * BLOCK..(block + 1) * BLOCK;
            read_as(&peer, &source, range, &limited(8)).await.unwrap();
        }
        assert_eq!(cached_blocks(&own, &source), 4);
        for block in 4..40 {
            let range = block * BLOCK..(block + 1) * BLOCK;
            read_as(&own, &source, range, &PartitionReadSettings::SHARD)
                .await
                .unwrap();
        }

        let metrics = cache.take_metrics();
        assert_eq!(metrics.cached_bytes, budget as u64);
        assert_eq!(metrics.limit_evictions, 32);
    }

    /// More partitions on a shard than the default budget has room for at
    /// the least cache each: without limits each keeps what it reads while
    /// the budget has room, as before limits. With a limit each holds at
    /// most it, and the budget binds them all, whatever the limits add up
    /// to.
    #[compio::test]
    async fn given_forty_partitions_reading_at_the_default_budget_when_with_or_without_limits_should_cache_within_each_bound()
     {
        let partitions: Vec<RemoteRange> =
            (0..40).map(|id| partition_source(id, 40 * BLOCK)).collect();
        for (settings, blocks_each) in [(PartitionReadSettings::SHARD, 4), (limited(8), 16)] {
            let store = Store::holding(40 * BLOCK);
            let remote = remote(&store, Rc::new(Silent), CACHE_BYTES, 0);
            for source in &partitions {
                for block in 0..blocks_each {
                    let range = block * BLOCK..(block + 1) * BLOCK;
                    read_as(&remote, source, range, &settings).await.unwrap();
                }
            }

            let cached: Vec<u64> = partitions
                .iter()
                .map(|source| cached_blocks(&remote, source))
                .collect();
            if settings.limit.is_none() {
                assert_eq!(
                    cached, [blocks_each; 40],
                    "every partition keeps what it read"
                );
            } else {
                assert!(cached.iter().all(|blocks| *blocks <= 8), "{cached:?}");
            }
            assert!(read_cache(&remote).take_metrics().cached_bytes <= CACHE_BYTES as u64);
        }
    }

    /// An index past an eighth of the partition's limit would take more than
    /// the share on its own, so a rewrite hands over nothing of the segment.
    #[compio::test]
    async fn given_an_index_larger_than_an_eighth_of_the_partition_limit_when_a_rewrite_carries_its_segment_should_keep_none_of_it()
     {
        let store = Store::holding(OBJECT_LEN);
        let remote = remote(&store, Rc::new(Silent), CACHE_BYTES, 0);
        let segment = source_of(0, 40 * BLOCK);
        let log = object(40 * BLOCK);
        read_as(&remote, &segment, 0..10, &limited(16))
            .await
            .unwrap();
        let held = read_cache(&remote).take_metrics().cached_bytes;
        let index = vec![7_u8; usize::try_from(2 * BLOCK + 1).unwrap()];

        remote
            .carry_long_term(&segment, &log, &index, &PartitionReadSettings::SHARD)
            .await;

        let metrics = read_cache(&remote).take_metrics();
        assert_eq!(metrics.carried_bytes, 0);
        assert_eq!(metrics.cached_bytes, held);
    }

    /// A rewrite hands the cache no more of a segment than an eighth of the
    /// limit of its partition, and nothing under eight blocks of it.
    #[compio::test]
    async fn given_a_partition_limit_when_a_rewrite_carries_its_segment_should_keep_an_eighth_of_it()
     {
        let store = Store::holding(OBJECT_LEN);
        let remote = remote(&store, Rc::new(Silent), CACHE_BYTES, 0);
        let segment = source_of(0, 40 * BLOCK);
        let log = object(40 * BLOCK);

        remote
            .carry_long_term(&segment, &log, b"index", &PartitionReadSettings::SHARD)
            .await;
        let unlimited = read_cache(&remote).take_metrics().carried_bytes;
        read_as(&remote, &segment, 0..10, &limited(16))
            .await
            .unwrap();
        remote
            .carry_long_term(&segment, &log, b"index", &PartitionReadSettings::SHARD)
            .await;
        let within_limit = read_cache(&remote).take_metrics().carried_bytes;
        read_as(&remote, &segment, 0..10, &limited(7))
            .await
            .unwrap();
        remote
            .carry_long_term(&segment, &log, b"index", &PartitionReadSettings::SHARD)
            .await;

        // The index takes its room in the eighth, a block of the tail.
        assert_eq!(unlimited, CACHE_BYTES as u64 / 8 - BLOCK + 5);
        assert_eq!(within_limit, BLOCK + 5);
        assert_eq!(read_cache(&remote).take_metrics().carried_bytes, 0);
    }
}
