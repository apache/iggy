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

//! Deletes of fresh objects that no placed batch names any more.
//!
//! A fresh object holds the messages of batches from many partitions. A
//! partition needs it for as long as it holds a placed batch above its
//! long-term floor that names it: the rewrite into long-term segments reads
//! the object for those batches, and so does a poll. Below the floor the
//! long-term segment holds the messages, and nothing reads the object for
//! them again. An object whose sends never committed is named by no batch at
//! all.
//!
//! So an object can go once no partition names it above its floor. Only the
//! replica that wrote the object decides that, from what its own partitions
//! hold. It led every partition whose send it put into the object, so it
//! holds each of them, and the floor it reads is committed. A replica that
//! is behind reads a lower floor and keeps the object for longer.
//!
//! A replica stops holding a partition it led without a delete of it: its
//! copy is fenced and rebuilt after it diverged or its files were damaged,
//! and a state transfer replaces what the copy holds. The group still holds
//! the placed batches then, and the copy that comes back reads them again.
//! Until it does, the replica cannot tell which objects it needs, and it
//! deletes none.
//!
//! An object that nothing names waits out a grace period before it is
//! deleted. A poll that planned a read of it before the floor passed it
//! ends in that time, and so does a batch that other replicas hold and this
//! one is still repairing.
//!
//! An object can also hold the proof rows of batches that other objects
//! hold, and a reader of the store alone needs a row for as long as no
//! long-term segment holds its batch. So before an object that is due goes,
//! the delete reads its index, and keeps the object while one of its rows
//! is of a batch above the long-term floor of its partition on this replica.
//! A kept object is due again at the next sweep, which reads it again.

use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::time::{Duration, SystemTime};

use consensus::Consensus;
use futures::FutureExt;
use futures::future::LocalBoxFuture;
use iggy_common::IggyTimestamp;
use iggy_object_store::{
    ErrorClass, FreshIndex, FreshObjectError, FreshObjectId, KeyLayout, ListRequest, ObjectStore,
    ObjectStoreError, TierExecutor, TierJobCancelled, TierJobClass, TierJobRequest, TierScopeToken,
};
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use server_common::sharding::IggyNamespace;
use tracing::warn;

use crate::object_delete::delete_objects;
use crate::placed_read::PlacedReadPlan;
use crate::{IggyPartition, IggyPartitions};

/// How long a fresh object that no placed batch names stays in the store.
pub const FRESH_OBJECT_GRACE: Duration = Duration::from_mins(10);

/// Keys of fresh objects that the listings of one sweep take together. Each
/// scope gets an equal share, and the next sweep goes on where a scope
/// stopped.
pub const FRESH_OBJECTS_PER_SWEEP: usize = 65_536;

/// Fresh objects one delete job takes. A sweep with more to delete runs one
/// job after another.
pub const FRESH_DELETES_PER_BATCH: usize = 4096;

/// Bytes of an object that the delete reads first for its index. An index
/// that is longer takes one more read, of the length its header names.
const INDEX_READ_BYTES: u64 = 16 * 1024;

/// A part of a replica's fresh objects that a sweep lists on its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum FreshListScope {
    /// The objects one shard wrote.
    Shard(u16),
    /// The objects of every shard. It reaches the objects of a shard that
    /// this node no longer runs, which no [`Self::Shard`] lists.
    Replica,
}

/// Where the listing of a scope stopped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FreshListCursor {
    /// What the store gave for the page that follows.
    pub continuation: String,
    /// The last object the listing reached, `None` when it reached none.
    pub after: Option<u128>,
}

/// One scope for a sweep to list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FreshListRequest {
    pub scope: FreshListScope,
    /// Where the last sweep stopped, `None` to list from the first key.
    pub resume: Option<FreshListCursor>,
    /// The most keys to take.
    pub keys: usize,
}

/// How the listing of a scope ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FreshListEnd {
    /// It reached the last key of the scope.
    Complete,
    /// It took its share of keys. Holds what the store gave for the page
    /// that follows.
    Truncated(String),
    /// The store failed a page.
    Failed,
}

/// The objects of one scope that a listing found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FreshListedScope {
    pub scope: FreshListScope,
    /// The object the listing went on after, `None` when it started at the
    /// first key.
    pub from: Option<u128>,
    /// The ids of the objects, in the order of their keys: by the shard
    /// that wrote them, then ascending.
    pub objects: Vec<u128>,
    pub end: FreshListEnd,
}

impl FreshListedScope {
    /// Whether the listing went past where the key of `id` sorts, so that it
    /// holds the object if the store does.
    fn covers(&self, id: u128) -> bool {
        if let FreshListScope::Shard(shard) = self.scope
            && FreshObjectId::from_raw(id).shard() != shard
        {
            return false;
        }
        let key = key_order(id);
        if self.from.is_some_and(|from| key <= key_order(from)) {
            return false;
        }
        self.end == FreshListEnd::Complete
            || self
                .objects
                .last()
                .is_some_and(|last| key <= key_order(*last))
    }
}

/// The fresh objects of one replica that a sweep listed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FreshListing {
    pub scopes: Vec<FreshListedScope>,
    /// Bytes the listed objects hold together.
    pub bytes: u64,
    /// When the oldest of them was stored.
    pub oldest: Option<SystemTime>,
}

impl FreshListing {
    /// The ids of the listed objects, each once.
    #[must_use]
    pub fn objects(&self) -> BTreeSet<u128> {
        self.scopes
            .iter()
            .flat_map(|scope| scope.objects.iter().copied())
            .collect()
    }

    /// Scopes whose listing did not reach their last key.
    #[must_use]
    pub fn truncated(&self) -> usize {
        self.scopes
            .iter()
            .filter(|scope| scope.end != FreshListEnd::Complete)
            .count()
    }
}

/// What a listing ended as: what it found, or a cancellation before it
/// finished.
pub type FreshListOutcome = Result<FreshListing, TierJobCancelled>;

/// The objects a delete left in the store.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FreshDeleted {
    /// Those the store failed to delete.
    pub failed: Vec<u128>,
    /// Those kept for their proof rows, or whose index could not be read.
    pub kept: Vec<u128>,
}

/// What a delete ended as, or a cancellation before it finished.
pub type FreshDeleteOutcome = Result<FreshDeleted, TierJobCancelled>;

/// The long-term floor of each placing partition of a replica, by its
/// namespace and its incarnation, as a sweep found them. A proof row of a
/// batch above the floor of its partition keeps its object.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProofFloors(BTreeMap<(u64, u64), Option<u64>>);

impl ProofFloors {
    /// The partition of `namespace` and `created_revision` has its long-term
    /// floor at `floor`, `None` before its first long-term segment.
    pub fn insert(&mut self, namespace: IggyNamespace, created_revision: u64, floor: Option<u64>) {
        self.0.insert((namespace.inner(), created_revision), floor);
    }

    /// Take the floors `other` found as well.
    pub fn extend(&mut self, other: Self) {
        self.0.extend(other.0);
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Whether `index` holds a proof row of a batch that no long-term
    /// segment of a partition here holds yet. A row of a partition this
    /// replica does not hold, or of another incarnation of it, keeps
    /// nothing.
    #[must_use]
    pub fn needs(&self, index: &FreshIndex) -> bool {
        index.directory().iter().any(|row| {
            self.0
                .get(&(row.namespace, row.created_revision))
                .is_some_and(|floor| {
                    let held_below = floor.map_or(0, |floor| floor.saturating_add(1));
                    index
                        .proofs_of(row)
                        .iter()
                        .any(|proof| proof.end_offset().is_none_or(|end| end > held_below))
                })
        })
    }
}

/// Lists and deletes the fresh objects of a replica, behind a pointer, so
/// the shard does not carry the executor's type.
pub trait FreshObjectSweeper {
    /// List the fresh objects that `replica` wrote, scope by scope as
    /// `requests` say.
    fn list(
        &self,
        replica: u8,
        requests: Vec<FreshListRequest>,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, FreshListOutcome>;

    /// Delete the fresh objects `objects`, but those whose proof rows
    /// `floors` still needs.
    fn delete(
        &self,
        objects: Vec<u128>,
        floors: Rc<ProofFloors>,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, FreshDeleteOutcome>;
}

/// Sweeps through a tier executor. A listing goes in its housekeeping class
/// and a delete in its delete class, so neither takes a slot from an upload
/// or a read.
pub struct ExecutorFreshSweeper<E> {
    executor: E,
    layout: KeyLayout,
}

impl<E> ExecutorFreshSweeper<E> {
    #[must_use]
    pub const fn new(executor: E, layout: KeyLayout) -> Self {
        Self { executor, layout }
    }

    /// The executor the jobs go through.
    #[must_use]
    pub const fn executor(&self) -> &E {
        &self.executor
    }
}

impl<E: TierExecutor> FreshObjectSweeper for ExecutorFreshSweeper<E> {
    fn list(
        &self,
        replica: u8,
        requests: Vec<FreshListRequest>,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, FreshListOutcome> {
        let layout = self.layout.clone();
        self.executor
            .submit(
                TierJobRequest::new(TierJobClass::Housekeeping, scope),
                move |store| async move {
                    list_fresh_objects(&*store, &layout, replica, &requests).await
                },
            )
            .boxed_local()
    }

    fn delete(
        &self,
        objects: Vec<u128>,
        floors: Rc<ProofFloors>,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, FreshDeleteOutcome> {
        let layout = self.layout.clone();
        self.executor
            .submit(
                TierJobRequest::new(TierJobClass::Delete, scope),
                move |store| async move {
                    let mut kept = Vec::new();
                    let mut keys = Vec::with_capacity(objects.len());
                    for id in objects {
                        match needs_proofs(&*store, &layout, id, &floors).await {
                            Ok(false) => keys.push(layout.fresh_key(id)),
                            Ok(true) | Err(_) => kept.push(id),
                        }
                    }
                    let failed = delete_objects(&*store, &keys)
                        .await
                        .iter()
                        .filter_map(|key| layout.parse_fresh_key(key))
                        .collect();
                    FreshDeleted { failed, kept }
                },
            )
            .boxed_local()
    }
}

/// Whether the fresh object `id` holds a proof row that `floors` needs, from
/// a read of its index. An object that is gone, or that holds no index this
/// build reads, needs nothing.
///
/// # Errors
/// A transient error of the store, after which the object is kept until a
/// later sweep reads it.
async fn needs_proofs<S: ObjectStore>(
    store: &S,
    layout: &KeyLayout,
    id: u128,
    floors: &ProofFloors,
) -> Result<bool, ObjectStoreError> {
    if floors.is_empty() {
        return Ok(false);
    }
    let key = layout.fresh_key(id);
    let mut read = INDEX_READ_BYTES;
    loop {
        let prefix = match store.get_range(&key, 0..read).await {
            Ok(range) => range.bytes,
            Err(error) if error.class() == ErrorClass::Transient => return Err(error),
            Err(_) => return Ok(false),
        };
        match FreshIndex::decode(&prefix, FreshObjectId::from_raw(id)) {
            Ok(index) => return Ok(floors.needs(&index)),
            // Read again once, for the length the header names, unless the
            // object ends before it.
            Err(FreshObjectError::Incomplete { have, need }) if have == read && need > read => {
                read = need;
            }
            Err(_) => return Ok(false),
        }
    }
}

/// List the fresh objects under `layout` that `replica` wrote, scope by
/// scope as `requests` say.
///
/// A key that names no fresh object of the replica is skipped: this build
/// did not write it and does not delete it. A scope whose page the store
/// fails ends there, and the other scopes are still listed.
pub async fn list_fresh_objects<S: ObjectStore>(
    store: &S,
    layout: &KeyLayout,
    replica: u8,
    requests: &[FreshListRequest],
) -> FreshListing {
    let mut listing = FreshListing::default();
    let mut counted = BTreeSet::new();
    for request in requests {
        let prefix = match request.scope {
            FreshListScope::Shard(shard) => layout.fresh_shard_prefix(replica, shard),
            FreshListScope::Replica => layout.fresh_replica_prefix(replica),
        };
        let (mut continuation, from) = request.resume.clone().map_or((None, None), |cursor| {
            (Some(cursor.continuation), cursor.after)
        });
        let mut objects = Vec::new();
        let mut keys_left = request.keys.max(1);
        let end = loop {
            // A page never holds more keys than are left, so the listing
            // stops at the end of a page, where the store says how to go on.
            let page = ListRequest::new(&prefix).with_max_keys(keys_left);
            let page = continuation
                .as_deref()
                .map_or(page, |continuation| page.after(continuation));
            let page = match store.list(&page).await {
                Ok(page) => page,
                Err(error) => {
                    warn!(%error, scope = ?request.scope, "listing fresh objects failed");
                    break FreshListEnd::Failed;
                }
            };
            keys_left = keys_left.saturating_sub(page.objects.len().max(1));
            for object in page.objects {
                let Some(id) = layout.parse_fresh_key(&object.key) else {
                    continue;
                };
                if FreshObjectId::from_raw(id).replica() != replica {
                    continue;
                }
                objects.push(id);
                if counted.insert(id) {
                    listing.bytes = listing.bytes.saturating_add(object.size);
                    listing.oldest = Some(listing.oldest.map_or(object.last_modified, |oldest| {
                        oldest.min(object.last_modified)
                    }));
                }
            }
            match page.next {
                None => break FreshListEnd::Complete,
                Some(next) if keys_left == 0 => break FreshListEnd::Truncated(next),
                Some(next) => continuation = Some(next),
            }
        };
        listing.scopes.push(FreshListedScope {
            scope: request.scope,
            from,
            objects,
            end,
        });
    }
    listing
}

/// What [`FreshSweep::settle`] found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FreshSettled {
    /// The objects to delete now, oldest first.
    pub due: Vec<u128>,
    /// Objects this sweep first found unnamed.
    pub found_unnamed: usize,
}

/// What the sweeps of a replica keep between them: the fresh objects that
/// nothing names, each with when a sweep first found it so, and where the
/// listing of each scope stopped.
///
/// A sweep lists only a part of a scope that holds more keys than its share.
/// The objects of the other parts stay here as they were, and their grace
/// period runs on.
#[derive(Debug, Default)]
pub struct FreshSweep {
    unnamed: BTreeMap<u128, IggyTimestamp>,
    cursors: BTreeMap<FreshListScope, FreshListCursor>,
}

impl FreshSweep {
    /// What the next sweep lists on a node of `shards` shards: every shard
    /// and the replica as a whole, each from where its last listing stopped
    /// and with an equal share of `keys`. So a shard with few objects is
    /// listed to its end however many another one holds.
    #[must_use]
    pub fn plan(&self, shards: u16, keys: usize) -> Vec<FreshListRequest> {
        let share = (keys / (usize::from(shards) + 1)).max(1);
        (0..shards)
            .map(FreshListScope::Shard)
            .chain([FreshListScope::Replica])
            .map(|scope| FreshListRequest {
                scope,
                resume: self.cursors.get(&scope).cloned(),
                keys: share,
            })
            .collect()
    }

    /// Take in one sweep: `listing` is what it listed, and `in_use` every
    /// object that a placed batch of this replica names above its
    /// partition's long-term floor or that a send still waits on, read after
    /// the listing ended.
    ///
    /// An object is deleted once every sweep since one first listed it
    /// found it unnamed for `grace`, whether those sweeps listed it again or
    /// not. One that a sweep finds named again starts over.
    pub fn settle(
        &mut self,
        listing: &FreshListing,
        in_use: &BTreeSet<u128>,
        now: IggyTimestamp,
        grace: Duration,
    ) -> FreshSettled {
        for listed in &listing.scopes {
            match &listed.end {
                FreshListEnd::Truncated(continuation) => {
                    self.cursors.insert(
                        listed.scope,
                        FreshListCursor {
                            continuation: continuation.clone(),
                            after: listed.objects.last().copied().or(listed.from),
                        },
                    );
                }
                // A scope that failed starts over: what the store gave for
                // its next page may be what it refuses.
                FreshListEnd::Complete | FreshListEnd::Failed => {
                    self.cursors.remove(&listed.scope);
                }
            }
        }
        let objects = listing.objects();
        // An object that left the store has nothing left to delete. A
        // listing says so only about the keys it went past.
        let scopes: BTreeMap<_, _> = listing
            .scopes
            .iter()
            .map(|listed| (listed.scope, listed))
            .collect();
        self.unnamed.retain(|id, _| {
            if in_use.contains(id) {
                return false;
            }
            let shard = FreshListScope::Shard(FreshObjectId::from_raw(*id).shard());
            objects.contains(id)
                || ![shard, FreshListScope::Replica]
                    .iter()
                    .filter_map(|scope| scopes.get(scope))
                    .any(|listed| listed.covers(*id))
        });
        let mut found_unnamed = 0;
        for id in objects.difference(in_use) {
            self.unnamed.entry(*id).or_insert_with(|| {
                found_unnamed += 1;
                now
            });
        }
        let grace = u64::try_from(grace.as_micros()).unwrap_or(u64::MAX);
        let due = self
            .unnamed
            .iter()
            .filter(|(_, since)| now.as_micros().saturating_sub(since.as_micros()) >= grace)
            .map(|(id, _)| *id)
            .collect();
        FreshSettled { due, found_unnamed }
    }

    /// Forget the objects a delete removed.
    pub fn forget(&mut self, deleted: &[u128]) {
        for id in deleted {
            self.unnamed.remove(id);
        }
    }

    /// Objects that nothing names and that are still in the store as far as
    /// the sweeps know.
    #[must_use]
    pub fn len(&self) -> usize {
        self.unnamed.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.unnamed.is_empty()
    }
}

/// Where the key of the fresh object `id` sorts among the keys of its
/// replica: under the directory of the shard that wrote it.
const fn key_order(id: u128) -> (u16, u128) {
    (FreshObjectId::from_raw(id).shard(), id)
}

/// The read of the fresh objects a partition names above its long-term
/// floor, captured under the partition borrow so it runs without.
pub struct FreshObjectsInUse {
    plan: PlacedReadPlan,
    floor: Option<u64>,
}

/// A read of a partition's placed batches that failed, so the sweep it was
/// for deletes nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FreshScanFailed;

impl FreshObjectsInUse {
    /// The ids of the objects.
    ///
    /// # Errors
    /// Returns [`FreshScanFailed`] when a segment or a journal entry of the
    /// partition could not be read.
    pub async fn read(self) -> Result<BTreeSet<u128>, FreshScanFailed> {
        self.plan
            .fresh_objects_above(self.floor)
            .await
            .map_err(|_| FreshScanFailed)
    }
}

impl<B, SB> IggyPartition<B, SB>
where
    B: MessageBus,
    SB: SuperblockStore,
{
    /// The read of the fresh objects this partition names above its
    /// long-term floor. `None` for a partition that does not place its
    /// batches, which names none.
    #[must_use]
    pub fn fresh_objects_in_use(&self) -> Option<FreshObjectsInUse> {
        self.places_batches().then(|| FreshObjectsInUse {
            plan: self.placed_read_plan(None),
            floor: self.long_term_floor(),
        })
    }

    /// Whether this copy holds every placed batch that its group committed
    /// and that names an object of this replica. A copy outside normal
    /// status may miss some, and so may one that a state transfer is
    /// replacing or that lost its files and waits for one.
    fn holds_its_placed_batches(&self) -> bool {
        let consensus = self.consensus();
        consensus.is_normal() && !consensus.is_transferring() && !self.requires_state_transfer()
    }
}

impl<B, SB> IggyPartitions<B, SB>
where
    B: MessageBus,
    SB: SuperblockStore,
{
    /// The long-term floor of each placing partition of this shard, which
    /// the proof rows of its fresh objects are checked against.
    #[must_use]
    pub fn proof_floors(&self) -> ProofFloors {
        let mut floors = ProofFloors::default();
        for (namespace, partition) in self.stored() {
            if partition.places_batches() && !self.is_tombstoned(namespace) {
                floors.insert(
                    *namespace,
                    partition.created_revision(),
                    partition.long_term_floor(),
                );
            }
        }
        floors
    }

    /// The reads of the fresh objects that the placing partitions of this
    /// shard name above their long-term floors. `None` when the shard cannot
    /// tell: a copy is fenced or misses placed batches of its group while
    /// `committed` says its namespace is still committed. A copy whose
    /// namespace is not committed was deleted and holds nothing back.
    #[must_use]
    pub fn fresh_objects_in_use(
        &self,
        committed: impl Fn(&IggyNamespace) -> bool,
    ) -> Option<Vec<FreshObjectsInUse>> {
        let mut readable = Vec::new();
        // Asked of every partition before any read is planned: one that
        // cannot tell makes all the plans useless.
        for (namespace, partition) in self.stored() {
            if !partition.places_batches() {
                continue;
            }
            if !self.is_tombstoned(namespace) && partition.holds_its_placed_batches() {
                readable.push(partition);
            } else if committed(namespace) {
                return None;
            }
        }
        Some(
            readable
                .into_iter()
                .filter_map(IggyPartition::fresh_objects_in_use)
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PartitionsConfig;
    use bytes::Bytes;
    use consensus::{LocalPipeline, VsrConsensus};
    use iggy_common::{IggyByteSize, PartitionStats};
    use iggy_object_store::{InlineTierExecutor, MemoryStore, PutBody, PutOptions, TierScope};
    use message_bus::IggyMessageBus;
    use server_common::sharding::ShardId;
    use std::sync::Arc;

    const REPLICA: u8 = 1;

    /// No object to delete.
    const NONE: [u128; 0] = [];

    fn layout() -> KeyLayout {
        KeyLayout::new("iggy").unwrap()
    }

    fn id(replica: u8, sequence: u64) -> u128 {
        FreshObjectId::new(replica, 0, 7, sequence).unwrap().get()
    }

    /// An object that `shard` of [`REPLICA`] wrote in the life it started at
    /// `started_at`.
    fn id_of_shard(shard: u16, started_at: u64) -> u128 {
        FreshObjectId::new(REPLICA, shard, started_at, 0)
            .unwrap()
            .get()
    }

    fn sweeper() -> ExecutorFreshSweeper<InlineTierExecutor<MemoryStore>> {
        ExecutorFreshSweeper::new(InlineTierExecutor::new(MemoryStore::new()), layout())
    }

    async fn store_object(store: &MemoryStore, key: &str, bytes: &'static [u8]) {
        store
            .put(
                key,
                PutBody::from_bytes(Bytes::from_static(bytes)),
                &PutOptions::default(),
            )
            .await
            .unwrap();
    }

    /// Every key of the replica, in one scope.
    fn whole_replica() -> Vec<FreshListRequest> {
        vec![FreshListRequest {
            scope: FreshListScope::Replica,
            resume: None,
            keys: FRESH_OBJECTS_PER_SWEEP,
        }]
    }

    /// A listing of the whole replica that found `objects` and reached the
    /// last key.
    fn listing(objects: &[u128]) -> FreshListing {
        listing_of(
            FreshListScope::Replica,
            None,
            objects,
            FreshListEnd::Complete,
        )
    }

    /// A listing of one scope.
    fn listing_of(
        scope: FreshListScope,
        from: Option<u128>,
        objects: &[u128],
        end: FreshListEnd,
    ) -> FreshListing {
        FreshListing {
            scopes: vec![FreshListedScope {
                scope,
                from,
                objects: objects.to_vec(),
                end,
            }],
            ..FreshListing::default()
        }
    }

    fn truncated() -> FreshListEnd {
        FreshListEnd::Truncated("next".to_owned())
    }

    fn at(seconds: u64) -> IggyTimestamp {
        IggyTimestamp::from(seconds * 1_000_000)
    }

    /// The objects that are due at `now` when the sweep takes in `listing`
    /// and nothing is named.
    fn due(sweep: &mut FreshSweep, listing: &FreshListing, now: IggyTimestamp) -> Vec<u128> {
        sweep
            .settle(listing, &BTreeSet::new(), now, FRESH_OBJECT_GRACE)
            .due
    }

    #[compio::test]
    async fn given_objects_of_two_replicas_when_listing_should_return_those_of_the_replica() {
        let sweeper = sweeper();
        let store = sweeper.executor().store();
        store_object(store, &layout().fresh_key(id(REPLICA, 2)), b"second").await;
        store_object(store, &layout().fresh_key(id(REPLICA, 1)), b"first").await;
        store_object(store, &layout().fresh_key(id(2, 1)), b"another replica").await;
        store_object(store, "iggy/meta/fresh/01/not-an-id", b"another writer").await;

        let listed = sweeper
            .list(REPLICA, whole_replica(), TierScope::new().token())
            .await
            .unwrap();

        assert_eq!(listed.scopes.len(), 1);
        assert_eq!(
            listed.scopes[0].objects,
            vec![id(REPLICA, 1), id(REPLICA, 2)]
        );
        assert_eq!(listed.scopes[0].end, FreshListEnd::Complete);
        assert_eq!(listed.bytes, 11);
        assert!(listed.oldest.is_some());
        assert_eq!(listed.truncated(), 0);
    }

    /// A key holds the shard ahead of the id, and an id holds the start of
    /// its shard ahead of the shard. So the keys of two shards do not sort
    /// as their ids do.
    #[compio::test]
    async fn given_objects_of_two_shards_when_listing_should_return_them_shard_by_shard() {
        let sweeper = sweeper();
        let store = sweeper.executor().store();
        let older_of_later_shard = id_of_shard(1, 7);
        let newer_of_first_shard = id_of_shard(0, 9);
        assert!(older_of_later_shard < newer_of_first_shard);
        store_object(store, &layout().fresh_key(older_of_later_shard), b"one").await;
        store_object(store, &layout().fresh_key(newer_of_first_shard), b"two").await;

        let listed = sweeper
            .list(REPLICA, whole_replica(), TierScope::new().token())
            .await
            .unwrap();

        assert_eq!(
            listed.scopes[0].objects,
            vec![newer_of_first_shard, older_of_later_shard]
        );
        assert_eq!(listed.scopes[0].end, FreshListEnd::Complete);

        let deleted = sweeper
            .delete(
                listed.objects().into_iter().collect(),
                Rc::default(),
                TierScope::new().token(),
            )
            .await
            .unwrap();

        assert_eq!(deleted, FreshDeleted::default());
        assert_eq!(store.keys(), Vec::<String>::new());
    }

    /// Each shard has its own share of the keys a sweep lists. The first
    /// shard holds more than its share here, and the objects of the second
    /// one and of a shard this node no longer runs are listed all the same.
    #[compio::test]
    async fn given_a_shard_with_more_objects_than_its_share_when_listing_should_list_the_other_shards_to_their_end()
     {
        let sweeper = sweeper();
        let store = sweeper.executor().store();
        let crowded: Vec<u128> = (0..7)
            .map(|sequence| FreshObjectId::new(REPLICA, 0, 7, sequence).unwrap().get())
            .collect();
        let quiet = id_of_shard(1, 7);
        for object in crowded.iter().chain([&quiet]) {
            store_object(store, &layout().fresh_key(*object), b"x").await;
        }
        let requests = FreshSweep::default().plan(2, 9);
        assert_eq!(
            requests
                .iter()
                .map(|request| (request.scope, request.keys))
                .collect::<Vec<_>>(),
            [
                (FreshListScope::Shard(0), 3),
                (FreshListScope::Shard(1), 3),
                (FreshListScope::Replica, 3)
            ]
        );

        let listed = sweeper
            .list(REPLICA, requests, TierScope::new().token())
            .await
            .unwrap();

        assert_eq!(listed.scopes[0].objects, crowded[..3]);
        assert!(matches!(listed.scopes[0].end, FreshListEnd::Truncated(_)));
        assert_eq!(listed.scopes[1].objects, vec![quiet]);
        assert_eq!(listed.scopes[1].end, FreshListEnd::Complete);
        assert_eq!(listed.scopes[2].objects, crowded[..3]);
        assert_eq!(listed.truncated(), 2);
        assert_eq!(listed.objects().len(), 4);
        assert_eq!(listed.bytes, 4, "an object two scopes list counts once");
    }

    /// Sweep after sweep over a shard that holds more objects than a sweep
    /// lists: each one goes on where the last stopped, and the objects it
    /// does not list keep the time a sweep first found them unnamed.
    #[compio::test]
    async fn given_a_truncated_listing_when_later_sweeps_list_should_resume_where_it_stopped_and_restart_no_grace()
     {
        let sweeper = sweeper();
        let store = sweeper.executor().store();
        let objects: Vec<u128> = (0..5)
            .map(|sequence| FreshObjectId::new(REPLICA, 0, 7, sequence).unwrap().get())
            .collect();
        for object in &objects {
            store_object(store, &layout().fresh_key(*object), b"x").await;
        }
        let mut sweep = FreshSweep::default();
        let mut listed_by_shard_scope = Vec::new();
        let mut unnamed_after = Vec::new();

        // Two keys for the shard and two for the replica as a whole.
        for seconds in [100, 200, 300, 400] {
            let listed = sweeper
                .list(REPLICA, sweep.plan(1, 4), TierScope::new().token())
                .await
                .unwrap();
            listed_by_shard_scope.push(listed.scopes[0].objects.clone());
            assert_eq!(due(&mut sweep, &listed, at(seconds)), NONE);
            unnamed_after.push(sweep.len());
        }

        assert_eq!(
            listed_by_shard_scope,
            [
                objects[..2].to_vec(),
                objects[2..4].to_vec(),
                objects[4..].to_vec(),
                objects[..2].to_vec()
            ],
            "the fourth sweep starts over after the third reached the end"
        );
        assert_eq!(unnamed_after, [2, 4, 5, 5]);
        let listed = sweeper
            .list(REPLICA, sweep.plan(1, 4), TierScope::new().token())
            .await
            .unwrap();
        assert_eq!(listed.scopes[0].objects, objects[2..4]);
        assert_eq!(
            due(&mut sweep, &listed, at(700)),
            objects[..2],
            "ten minutes after the first sweep found them, though this one did not list them"
        );
        assert_eq!(due(&mut sweep, &listed, at(800)), objects[..4]);
        assert_eq!(due(&mut sweep, &listed, at(900)), objects);
    }

    #[compio::test]
    async fn given_fresh_objects_when_deleting_some_should_leave_the_others() {
        let sweeper = sweeper();
        let store = sweeper.executor().store();
        store_object(store, &layout().fresh_key(id(REPLICA, 1)), b"first").await;
        store_object(store, &layout().fresh_key(id(REPLICA, 2)), b"second").await;

        let deleted = sweeper
            .delete(
                vec![id(REPLICA, 1), id(REPLICA, 9)],
                Rc::default(),
                TierScope::new().token(),
            )
            .await
            .unwrap();

        assert_eq!(deleted, FreshDeleted::default());
        assert_eq!(store.keys(), vec![layout().fresh_key(id(REPLICA, 2))]);
    }

    #[test]
    fn given_an_unnamed_object_when_its_grace_has_not_passed_should_keep_it() {
        let mut sweep = FreshSweep::default();
        let listing = listing(&[id(REPLICA, 1)]);

        assert_eq!(due(&mut sweep, &listing, at(100)), NONE);
        assert_eq!(due(&mut sweep, &listing, at(699)), NONE);
        assert_eq!(due(&mut sweep, &listing, at(700)), vec![id(REPLICA, 1)]);
    }

    #[test]
    fn given_an_object_named_again_when_settling_should_start_its_grace_over() {
        let mut sweep = FreshSweep::default();
        let listing = listing(&[id(REPLICA, 1)]);
        let named = BTreeSet::from([id(REPLICA, 1)]);

        assert_eq!(due(&mut sweep, &listing, at(100)), NONE);
        assert_eq!(
            sweep
                .settle(&listing, &named, at(400), FRESH_OBJECT_GRACE)
                .due,
            NONE
        );
        assert!(sweep.is_empty());
        assert_eq!(due(&mut sweep, &listing, at(800)), NONE);
        assert_eq!(due(&mut sweep, &listing, at(1400)), vec![id(REPLICA, 1)]);
    }

    /// The shards name what they need in every sweep, whatever part of the
    /// store the sweep lists. So an object named again starts over even when
    /// the sweep that hears of it did not list it.
    #[test]
    fn given_an_object_a_sweep_did_not_list_when_it_is_named_again_should_start_its_grace_over() {
        let mut sweep = FreshSweep::default();
        let first = id(REPLICA, 1);
        let second = id(REPLICA, 2);
        let scope = FreshListScope::Shard(0);
        let start = listing_of(scope, None, &[first], truncated());
        let rest = listing_of(scope, Some(first), &[second], FreshListEnd::Complete);
        assert_eq!(due(&mut sweep, &start, at(100)), NONE);

        let settled = sweep.settle(&rest, &BTreeSet::from([first]), at(200), FRESH_OBJECT_GRACE);

        assert_eq!((settled.found_unnamed, sweep.len()), (1, 1));
        assert_eq!(due(&mut sweep, &start, at(700)), NONE);
        assert_eq!(due(&mut sweep, &rest, at(800)), vec![second]);
        assert_eq!(due(&mut sweep, &rest, at(1300)), vec![first, second]);
    }

    #[test]
    fn given_a_named_object_when_settling_should_never_return_it() {
        let mut sweep = FreshSweep::default();
        let listing = listing(&[id(REPLICA, 1), id(REPLICA, 2)]);
        let in_use = BTreeSet::from([id(REPLICA, 2)]);

        sweep.settle(&listing, &in_use, at(100), FRESH_OBJECT_GRACE);
        let settled = sweep.settle(&listing, &in_use, at(10_000), FRESH_OBJECT_GRACE);

        assert_eq!(settled.due, vec![id(REPLICA, 1)]);
    }

    #[test]
    fn given_an_object_that_left_the_store_when_settling_should_forget_it() {
        let mut sweep = FreshSweep::default();
        due(
            &mut sweep,
            &listing(&[id(REPLICA, 1), id(REPLICA, 2)]),
            at(100),
        );
        assert_eq!(sweep.len(), 2);

        due(&mut sweep, &listing(&[id(REPLICA, 2)]), at(200));

        assert_eq!(sweep.len(), 1);
    }

    /// A listing that went on after one object and stopped at another says
    /// that an object between them left the store, and nothing about the
    /// ones before and after.
    #[test]
    fn given_a_listing_of_a_part_of_a_shard_when_settling_should_forget_only_the_objects_it_went_past()
     {
        let mut sweep = FreshSweep::default();
        let objects: Vec<u128> = (1..=5).map(|sequence| id(REPLICA, sequence)).collect();
        due(&mut sweep, &listing(&objects), at(100));
        let part = listing_of(
            FreshListScope::Shard(0),
            Some(objects[1]),
            &[objects[3]],
            truncated(),
        );

        assert_eq!(due(&mut sweep, &part, at(200)), NONE);

        assert_eq!(
            due(&mut sweep, &FreshListing::default(), at(700)),
            [objects[0], objects[1], objects[3], objects[4]],
            "the third one was between the two and was not listed"
        );
    }

    #[test]
    fn given_a_listing_that_stopped_early_when_settling_should_keep_the_objects_past_it() {
        let mut sweep = FreshSweep::default();
        due(
            &mut sweep,
            &listing(&[id(REPLICA, 1), id(REPLICA, 2), id(REPLICA, 3)]),
            at(100),
        );
        let partial = listing_of(
            FreshListScope::Replica,
            None,
            &[id(REPLICA, 2)],
            truncated(),
        );

        due(&mut sweep, &partial, at(200));

        // The first is gone from the store, the third was not listed.
        assert_eq!(sweep.len(), 2);
        assert_eq!(
            due(
                &mut sweep,
                &listing(&[id(REPLICA, 2), id(REPLICA, 3)]),
                at(700)
            ),
            vec![id(REPLICA, 2), id(REPLICA, 3)]
        );
    }

    /// The listing stops in the directory of the first shard. The object of
    /// the later shard has the lower id, and its key is still past the stop.
    #[test]
    fn given_a_listing_that_stopped_in_an_earlier_shard_when_settling_should_keep_the_objects_of_later_shards()
     {
        let mut sweep = FreshSweep::default();
        let of_first_shard = id_of_shard(0, 9);
        let of_later_shard = id_of_shard(1, 7);
        let both = listing(&[of_first_shard, of_later_shard]);
        due(&mut sweep, &both, at(100));
        let partial = listing_of(
            FreshListScope::Replica,
            None,
            &[of_first_shard],
            truncated(),
        );

        due(&mut sweep, &partial, at(200));

        assert_eq!(sweep.len(), 2);
        assert_eq!(
            due(&mut sweep, &both, at(700)),
            vec![of_later_shard, of_first_shard],
            "the grace of the object past the stop ran on"
        );
    }

    /// What the store gave for the next page of a listing that then failed
    /// may be what it refuses, so the scope is listed from its first key
    /// again. The objects found so far keep their time.
    #[test]
    fn given_a_scope_whose_listing_failed_when_planning_the_next_sweep_should_start_it_over() {
        let mut sweep = FreshSweep::default();
        let scope = FreshListScope::Shard(0);
        let first = id(REPLICA, 1);
        due(
            &mut sweep,
            &listing_of(scope, None, &[first], truncated()),
            at(100),
        );
        assert_eq!(
            sweep.plan(1, 2)[0].resume,
            Some(FreshListCursor {
                continuation: "next".to_owned(),
                after: Some(first),
            })
        );

        due(
            &mut sweep,
            &listing_of(scope, Some(first), &[], FreshListEnd::Failed),
            at(200),
        );

        assert_eq!(sweep.plan(1, 2)[0].resume, None);
        assert_eq!(sweep.len(), 1);
        assert_eq!(
            due(&mut sweep, &FreshListing::default(), at(700)),
            vec![first]
        );
    }

    #[test]
    fn given_deleted_objects_when_forgetting_should_drop_them() {
        let mut sweep = FreshSweep::default();
        due(
            &mut sweep,
            &listing(&[id(REPLICA, 1), id(REPLICA, 2)]),
            at(100),
        );

        sweep.forget(&[id(REPLICA, 1)]);

        assert_eq!(sweep.len(), 1);
    }

    fn placing() -> IggyNamespace {
        IggyNamespace::new(1, 1, 0)
    }

    /// A shard whose one partition [`placing`] places its batches, a
    /// single-replica primary in normal status.
    fn shard_of_one_placing_partition() -> IggyPartitions<IggyMessageBus> {
        let size = IggyByteSize::from(1024 * 1024);
        let partitions = IggyPartitions::new(
            ShardId::new(0),
            PartitionsConfig {
                messages_required_to_save: 1,
                size_of_messages_required_to_save: size,
                validate_checksum: false,
                segment_size: size,
                preallocate_segments: false,
                key_context: Arc::default(),
                path_layout: crate::PartitionPathLayout::default(),
            },
        );
        let consensus = VsrConsensus::new(
            1,
            0,
            1,
            placing().inner(),
            IggyMessageBus::new(0),
            LocalPipeline::new(),
        );
        consensus.init();
        let mut partition = IggyPartition::with_in_memory_storage(
            Arc::new(PartitionStats::default()),
            consensus,
            size,
        );
        partition.set_places_batches(true);
        partitions.insert(placing(), partition);
        partitions
    }

    /// The reads the shard plans while [`placing`] is committed, and once its
    /// delete committed.
    fn planned_reads(
        partitions: &IggyPartitions<IggyMessageBus>,
    ) -> (Option<usize>, Option<usize>) {
        (
            partitions
                .fresh_objects_in_use(|_| true)
                .map(|reads| reads.len()),
            partitions
                .fresh_objects_in_use(|_| false)
                .map(|reads| reads.len()),
        )
    }

    /// A rebuilt copy whose state transfer waits for a free slot is back in
    /// the idle stage and normal status, and holds none of its group's
    /// batches yet.
    #[test]
    fn given_a_rebuilt_copy_waiting_for_a_transfer_slot_when_planning_reads_should_not_tell_until_its_delete_commits()
     {
        let partitions = shard_of_one_placing_partition();
        assert_eq!(planned_reads(&partitions), (Some(1), Some(1)));

        let copy = partitions.get_mut_by_ns(&placing()).unwrap();
        copy.materialization_missing = true;
        assert!(copy.consensus().is_normal() && !copy.consensus().is_transferring());

        assert_eq!(planned_reads(&partitions), (None, Some(0)));
    }

    /// The partition whose proof rows the objects below carry.
    fn proving() -> IggyNamespace {
        IggyNamespace::new(1, 1, 0)
    }

    const REVISION: u64 = 11;

    /// An object of proof rows alone, of `count` batches of one message of
    /// [`proving`] from `first` on, which the object `source` holds.
    fn proving_object(sequence: u64, source: u64, first: u64, count: u64) -> (u128, Bytes) {
        use iggy_object_store::{
            FreshObjectHeader, FreshPartition, FreshProofRow, SealedFreshObject,
        };

        let object_id = FreshObjectId::from_raw(id(REPLICA, sequence));
        let proofs = (first..first + count)
            .map(|offset| FreshProofRow {
                object_id: FreshObjectId::from_raw(id(REPLICA, source)),
                position: 1_000 + offset,
                base_offset: offset,
                base_timestamp: offset,
                message_count: 1,
                placed_checksum: offset,
            })
            .collect();
        let sealed = SealedFreshObject::seal::<&[u8]>(
            FreshObjectHeader {
                object_id,
                cluster_uid: 1,
                sealed_at: 1,
            },
            &[FreshPartition {
                namespace: proving().inner(),
                created_revision: REVISION,
                view: 0,
                claimed_first_offset: 0,
                proven_below: first,
                batches: Vec::new(),
                proofs,
            }],
        )
        .unwrap();
        (object_id.get(), sealed.bytes)
    }

    async fn store_bytes(store: &MemoryStore, id: u128, bytes: Bytes) {
        store
            .put(
                &layout().fresh_key(id),
                PutBody::from_bytes(bytes),
                &PutOptions::default(),
            )
            .await
            .unwrap();
    }

    fn floors(floor: Option<u64>) -> Rc<ProofFloors> {
        let mut floors = ProofFloors::default();
        floors.insert(proving(), REVISION, floor);
        Rc::new(floors)
    }

    /// An object that proves a batch above the long-term floor stays, and
    /// goes once the floor passes the last batch it proves. One whose index
    /// is longer than the first read is read again whole.
    #[compio::test]
    async fn given_objects_that_prove_batches_above_the_floor_when_deleting_should_keep_them_until_the_floor_passes()
     {
        let sweeper = sweeper();
        let store = sweeper.executor().store();
        let (small, small_bytes) = proving_object(5, 1, 10, 2);
        let (large, large_bytes) = proving_object(6, 2, 100, 1_000);
        assert!(large_bytes.len() as u64 > INDEX_READ_BYTES);
        store_bytes(store, small, small_bytes).await;
        store_bytes(store, large, large_bytes).await;
        let delete =
            |floor| sweeper.delete(vec![small, large], floors(floor), TierScope::new().token());

        let deleted = delete(Some(9)).await.unwrap();
        assert_eq!(deleted.kept, [small, large]);
        assert_eq!(deleted.failed, NONE);
        let deleted = delete(Some(11)).await.unwrap();
        assert_eq!(
            deleted.kept,
            [large],
            "the floor holds both batches of the first"
        );
        assert_eq!(store.keys(), vec![layout().fresh_key(large)]);
        let deleted = delete(Some(1_098)).await.unwrap();
        assert_eq!(deleted.kept, [large], "its last batch is above the floor");
        let deleted = delete(Some(1_099)).await.unwrap();
        assert_eq!(deleted, FreshDeleted::default());
        assert_eq!(store.keys(), Vec::<String>::new());
    }

    /// Proof rows keep an object only for a partition this replica holds, in
    /// the incarnation that wrote them.
    #[compio::test]
    async fn given_objects_whose_rows_no_partition_here_needs_when_deleting_should_delete_them() {
        let sweeper = sweeper();
        let store = sweeper.executor().store();
        let (proof, proof_bytes) = proving_object(5, 1, 10, 2);
        store_bytes(store, proof, proof_bytes.clone()).await;
        store_object(
            store,
            &layout().fresh_key(id(REPLICA, 7)),
            b"IGGYFRS1 of an earlier build",
        )
        .await;
        let mut recreated = ProofFloors::default();
        recreated.insert(proving(), REVISION + 1, None);
        let mut elsewhere = ProofFloors::default();
        elsewhere.insert(IggyNamespace::new(1, 1, 1), REVISION, None);

        for floors in [recreated, elsewhere, ProofFloors::default()] {
            store_bytes(store, proof, proof_bytes.clone()).await;
            let deleted = sweeper
                .delete(
                    vec![proof, id(REPLICA, 7)],
                    Rc::new(floors),
                    TierScope::new().token(),
                )
                .await
                .unwrap();
            assert_eq!(deleted, FreshDeleted::default());
            assert_eq!(store.keys(), Vec::<String>::new());
        }
    }
}
