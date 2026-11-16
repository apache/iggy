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
//! An object that nothing names waits out a grace period before it is
//! deleted. A poll that planned a read of it before the floor passed it
//! ends in that time, and so does a batch that other replicas hold and this
//! one is still repairing.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, SystemTime};

use futures::FutureExt;
use futures::future::LocalBoxFuture;
use iggy_common::IggyTimestamp;
use iggy_object_store::{
    FreshObjectId, KeyLayout, ListRequest, ObjectStore, ObjectStoreError, TierExecutor,
    TierJobCancelled, TierJobClass, TierJobRequest, TierScopeToken,
};
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;

use crate::IggyPartition;
use crate::object_delete::delete_objects;
use crate::placed_read::PlacedReadPlan;

/// How long a fresh object that no placed batch names stays in the store.
pub const FRESH_OBJECT_GRACE: Duration = Duration::from_mins(10);

/// Fresh objects of this replica that one listing takes, oldest first. The
/// ones past it wait for a later sweep, which finds the oldest gone.
pub const FRESH_OBJECTS_PER_SWEEP: usize = 65_536;

/// Fresh objects one sweep deletes.
pub const FRESH_DELETES_PER_SWEEP: usize = 4096;

/// The fresh objects of one replica that a listing found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FreshListing {
    /// The ids of the objects, ascending.
    pub objects: Vec<u128>,
    /// Bytes the objects hold together.
    pub bytes: u64,
    /// When the oldest of them was stored.
    pub oldest: Option<SystemTime>,
    /// Whether the listing reached the end of the fresh objects. One that
    /// did not says nothing about the ids after its last.
    pub complete: bool,
}

/// What a listing ended as: its own result, or a cancellation before it
/// finished.
pub type FreshListOutcome = Result<Result<FreshListing, ObjectStoreError>, TierJobCancelled>;

/// What a delete ended as: the ids whose objects are still there, or a
/// cancellation before it finished.
pub type FreshDeleteOutcome = Result<Vec<u128>, TierJobCancelled>;

/// Lists and deletes the fresh objects of a replica, behind a pointer, so
/// the shard does not carry the executor's type.
pub trait FreshObjectSweeper {
    /// List the fresh objects that `replica` wrote, oldest first, at most
    /// [`FRESH_OBJECTS_PER_SWEEP`] of them.
    fn list(&self, replica: u8, scope: TierScopeToken)
    -> LocalBoxFuture<'static, FreshListOutcome>;

    /// Delete the fresh objects `objects`.
    fn delete(
        &self,
        objects: Vec<u128>,
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
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, FreshListOutcome> {
        let layout = self.layout.clone();
        self.executor
            .submit(
                TierJobRequest::new(TierJobClass::Housekeeping, scope),
                move |store| async move { list_fresh_objects(&*store, &layout, replica).await },
            )
            .boxed_local()
    }

    fn delete(
        &self,
        objects: Vec<u128>,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, FreshDeleteOutcome> {
        let layout = self.layout.clone();
        self.executor
            .submit(
                TierJobRequest::new(TierJobClass::Delete, scope),
                move |store| async move {
                    let keys: Vec<_> = objects.iter().map(|id| layout.fresh_key(*id)).collect();
                    delete_objects(&*store, &keys)
                        .await
                        .iter()
                        .filter_map(|key| layout.parse_fresh_key(key))
                        .collect()
                },
            )
            .boxed_local()
    }
}

/// List the fresh objects under `layout` that `replica` wrote, oldest first,
/// at most [`FRESH_OBJECTS_PER_SWEEP`] of them.
///
/// A key under the prefix that names no fresh object is skipped: this build
/// did not write it and does not delete it.
///
/// # Errors
/// Returns the error of the store call that failed.
pub async fn list_fresh_objects<S: ObjectStore>(
    store: &S,
    layout: &KeyLayout,
    replica: u8,
) -> Result<FreshListing, ObjectStoreError> {
    let prefix = layout.fresh_prefix();
    let mut listing = FreshListing::default();
    let mut continuation: Option<String> = None;
    loop {
        let request = ListRequest::new(&prefix);
        let request = continuation
            .as_deref()
            .map_or(request, |continuation| request.after(continuation));
        let page = store.list(&request).await?;
        for object in page.objects {
            let Some(id) = layout.parse_fresh_key(&object.key) else {
                continue;
            };
            if FreshObjectId::from_raw(id).replica() != replica {
                continue;
            }
            if listing.objects.len() == FRESH_OBJECTS_PER_SWEEP {
                return Ok(listing);
            }
            listing.objects.push(id);
            listing.bytes = listing.bytes.saturating_add(object.size);
            listing.oldest = Some(listing.oldest.map_or(object.last_modified, |oldest| {
                oldest.min(object.last_modified)
            }));
        }
        let Some(next) = page.next else {
            listing.complete = true;
            return Ok(listing);
        };
        continuation = Some(next);
    }
}

/// The fresh objects of a replica that nothing names, each with when a sweep
/// first found it so.
#[derive(Debug, Default)]
pub struct FreshSweep {
    unnamed: BTreeMap<u128, IggyTimestamp>,
}

impl FreshSweep {
    /// Take in one sweep: `listing` is what the store holds, and `in_use`
    /// every object that a placed batch of this replica names above its
    /// partition's long-term floor or that a send still waits on, read after
    /// the listing ended. Returns the objects to delete now, oldest first,
    /// at most [`FRESH_DELETES_PER_SWEEP`].
    ///
    /// An object is deleted once every sweep found it unnamed for `grace`.
    /// One that a sweep finds named again starts over.
    pub fn settle(
        &mut self,
        listing: &FreshListing,
        in_use: &BTreeSet<u128>,
        now: IggyTimestamp,
        grace: Duration,
    ) -> Vec<u128> {
        let listed: BTreeSet<u128> = listing.objects.iter().copied().collect();
        // An object that left the store has nothing left to delete. A
        // listing that stopped early says nothing about the ids past it.
        let last = listing.objects.last().copied();
        self.unnamed.retain(|id, _| {
            listed.contains(id) || (!listing.complete && last.is_some_and(|last| *id > last))
        });
        let grace = u64::try_from(grace.as_micros()).unwrap_or(u64::MAX);
        let mut due = Vec::new();
        for id in &listing.objects {
            if in_use.contains(id) {
                self.unnamed.remove(id);
                continue;
            }
            let since = *self.unnamed.entry(*id).or_insert(now);
            if due.len() < FRESH_DELETES_PER_SWEEP
                && now.as_micros().saturating_sub(since.as_micros()) >= grace
            {
                due.push(*id);
            }
        }
        due
    }

    /// Forget the objects a delete removed.
    pub fn forget(&mut self, deleted: &[u128]) {
        for id in deleted {
            self.unnamed.remove(id);
        }
    }

    /// Objects that nothing names and that wait out their grace period.
    #[must_use]
    pub fn len(&self) -> usize {
        self.unnamed.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.unnamed.is_empty()
    }
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use iggy_object_store::{InlineTierExecutor, MemoryStore, PutBody, PutOptions, TierScope};

    const REPLICA: u8 = 1;

    /// No object to delete.
    const NONE: [u128; 0] = [];

    fn layout() -> KeyLayout {
        KeyLayout::new("iggy").unwrap()
    }

    fn id(replica: u8, sequence: u64) -> u128 {
        FreshObjectId::new(replica, 0, 7, sequence).unwrap().get()
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

    fn listing(objects: &[u128]) -> FreshListing {
        FreshListing {
            objects: objects.to_vec(),
            complete: true,
            ..FreshListing::default()
        }
    }

    fn at(seconds: u64) -> IggyTimestamp {
        IggyTimestamp::from(seconds * 1_000_000)
    }

    #[compio::test]
    async fn given_objects_of_two_replicas_when_listing_should_return_those_of_the_replica() {
        let sweeper = sweeper();
        let store = sweeper.executor().store();
        store_object(store, &layout().fresh_key(id(REPLICA, 2)), b"second").await;
        store_object(store, &layout().fresh_key(id(REPLICA, 1)), b"first").await;
        store_object(store, &layout().fresh_key(id(2, 1)), b"another replica").await;
        store_object(store, "iggy/meta/fresh/not-an-id", b"another writer").await;

        let listed = sweeper
            .list(REPLICA, TierScope::new().token())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(listed.objects, vec![id(REPLICA, 1), id(REPLICA, 2)]);
        assert_eq!(listed.bytes, 11);
        assert!(listed.oldest.is_some());
        assert!(listed.complete);
    }

    #[compio::test]
    async fn given_fresh_objects_when_deleting_some_should_leave_the_others() {
        let sweeper = sweeper();
        let store = sweeper.executor().store();
        store_object(store, &layout().fresh_key(id(REPLICA, 1)), b"first").await;
        store_object(store, &layout().fresh_key(id(REPLICA, 2)), b"second").await;

        let failed = sweeper
            .delete(
                vec![id(REPLICA, 1), id(REPLICA, 9)],
                TierScope::new().token(),
            )
            .await
            .unwrap();

        assert!(failed.is_empty(), "{failed:?}");
        assert_eq!(store.keys(), vec![layout().fresh_key(id(REPLICA, 2))]);
    }

    #[test]
    fn given_an_unnamed_object_when_its_grace_has_not_passed_should_keep_it() {
        let mut sweep = FreshSweep::default();
        let listing = listing(&[id(REPLICA, 1)]);
        let in_use = BTreeSet::new();

        assert_eq!(
            sweep.settle(&listing, &in_use, at(100), FRESH_OBJECT_GRACE),
            NONE
        );
        assert_eq!(
            sweep.settle(&listing, &in_use, at(699), FRESH_OBJECT_GRACE),
            NONE
        );
        assert_eq!(
            sweep.settle(&listing, &in_use, at(700), FRESH_OBJECT_GRACE),
            vec![id(REPLICA, 1)]
        );
    }

    #[test]
    fn given_an_object_named_again_when_settling_should_start_its_grace_over() {
        let mut sweep = FreshSweep::default();
        let listing = listing(&[id(REPLICA, 1)]);
        let unnamed = BTreeSet::new();
        let named = BTreeSet::from([id(REPLICA, 1)]);

        assert_eq!(
            sweep.settle(&listing, &unnamed, at(100), FRESH_OBJECT_GRACE),
            NONE
        );
        assert_eq!(
            sweep.settle(&listing, &named, at(400), FRESH_OBJECT_GRACE),
            NONE
        );
        assert!(sweep.is_empty());
        assert_eq!(
            sweep.settle(&listing, &unnamed, at(800), FRESH_OBJECT_GRACE),
            NONE
        );
        assert_eq!(
            sweep.settle(&listing, &unnamed, at(1400), FRESH_OBJECT_GRACE),
            vec![id(REPLICA, 1)]
        );
    }

    #[test]
    fn given_a_named_object_when_settling_should_never_return_it() {
        let mut sweep = FreshSweep::default();
        let listing = listing(&[id(REPLICA, 1), id(REPLICA, 2)]);
        let in_use = BTreeSet::from([id(REPLICA, 2)]);

        sweep.settle(&listing, &in_use, at(100), FRESH_OBJECT_GRACE);
        let due = sweep.settle(&listing, &in_use, at(10_000), FRESH_OBJECT_GRACE);

        assert_eq!(due, vec![id(REPLICA, 1)]);
    }

    #[test]
    fn given_an_object_that_left_the_store_when_settling_should_forget_it() {
        let mut sweep = FreshSweep::default();
        let in_use = BTreeSet::new();
        sweep.settle(
            &listing(&[id(REPLICA, 1), id(REPLICA, 2)]),
            &in_use,
            at(100),
            FRESH_OBJECT_GRACE,
        );
        assert_eq!(sweep.len(), 2);

        sweep.settle(
            &listing(&[id(REPLICA, 2)]),
            &in_use,
            at(200),
            FRESH_OBJECT_GRACE,
        );

        assert_eq!(sweep.len(), 1);
    }

    #[test]
    fn given_a_listing_that_stopped_early_when_settling_should_keep_the_objects_past_it() {
        let mut sweep = FreshSweep::default();
        let in_use = BTreeSet::new();
        sweep.settle(
            &listing(&[id(REPLICA, 1), id(REPLICA, 2), id(REPLICA, 3)]),
            &in_use,
            at(100),
            FRESH_OBJECT_GRACE,
        );
        let partial = FreshListing {
            objects: vec![id(REPLICA, 2)],
            complete: false,
            ..FreshListing::default()
        };

        sweep.settle(&partial, &in_use, at(200), FRESH_OBJECT_GRACE);

        // The first is gone from the store, the third was not listed.
        assert_eq!(sweep.len(), 2);
        assert_eq!(
            sweep.settle(
                &listing(&[id(REPLICA, 2), id(REPLICA, 3)]),
                &in_use,
                at(700),
                FRESH_OBJECT_GRACE
            ),
            vec![id(REPLICA, 2), id(REPLICA, 3)]
        );
    }

    #[test]
    fn given_deleted_objects_when_forgetting_should_drop_them() {
        let mut sweep = FreshSweep::default();
        sweep.settle(
            &listing(&[id(REPLICA, 1), id(REPLICA, 2)]),
            &BTreeSet::new(),
            at(100),
            FRESH_OBJECT_GRACE,
        );

        sweep.forget(&[id(REPLICA, 1)]);

        assert_eq!(sweep.len(), 1);
    }
}
