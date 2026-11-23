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

//! The sweep of the fresh objects this replica wrote.
//!
//! The first shard runs it, for the objects of every shard of the replica.
//! The shard that wrote an object is not the one to ask whether it is still
//! needed: a node that restarts with another number of shards moves its
//! partitions between them, and the object keeps the number of the shard
//! that wrote it.
//!
//! The first shard starts a sweep from its pump's tick, every
//! [`FRESH_SWEEP_INTERVAL`] for as long as it has a sweeper. A sweep that
//! left work it could have done starts the next one at once: objects that
//! are due after it deleted some, or a listing that stopped at its share of
//! keys after it found or deleted something.
//!
//! A sweep lists the objects, and then asks every shard which objects its
//! partitions name above their long-term floors or a send still waits on,
//! and where those floors are. Each shard reads that after the listing
//! ended, so an object the listing holds and no answer names was already
//! unnamed when it was listed. Such an object is deleted once the sweeps
//! found it so for a grace period, unless it holds a proof row of a batch
//! above its partition's floor, which the delete job reads from its index.
//! The sweep deletes all that are due, one delete job after another, and
//! each job's result passes through the pump's inbox before the next
//! starts.
//!
//! A shard answers that it cannot tell while a placing partition that is
//! still committed may be missing batches its group holds: one outside
//! normal status, one a state transfer is replacing or waits to replace,
//! one fenced for a rebuild, and one this shard should hold and does not.
//! The sweep then deletes nothing, and the first shard reports it held until
//! a sweep settles. A partition whose delete committed holds no sweep back.
//!
//! Every step reports back as a frame, and a frame the inbox refused loses
//! the sweep it belongs to and nothing else. The next sweep lists again.

use crate::shards_table::ShardsTable;
use crate::{IggyShard, LifecycleFrame, ShardFrame};
use consensus::{MetadataHandle, PartitionsHandle};
use iggy_common::IggyTimestamp;
use iggy_object_store::TierScope;
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use metadata::impls::metadata::StreamsFrontend;
use partitions::{
    FRESH_DELETES_PER_BATCH, FRESH_OBJECT_GRACE, FRESH_OBJECTS_PER_SWEEP, FreshDeleteOutcome,
    FreshDeleted, FreshListOutcome, FreshListing, FreshObjectSweeper, FreshSweep, ProofFloors,
};
use server_common::sharding::IggyNamespace;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeSet, VecDeque};
use std::rc::Rc;
use std::time::Duration;

/// How often the first shard starts a sweep.
///
/// A tenth of the grace period, so an object goes that much later than its
/// grace at most. Every sweep makes each shard read its placed batches
/// above the long-term floor and costs one listing call per shard, so a
/// shorter interval buys little: a sweep that left work starts the next one
/// at once.
pub const FRESH_SWEEP_INTERVAL: Duration = Duration::from_mins(1);

/// Sweep starts that a sweep may wait through for the answers of the shards
/// before the next one replaces it.
const SWEEP_TICKS_MAX: u32 = 8;

/// What the task listing the fresh objects hands back to the pump.
pub struct FinishedFreshListing {
    pub sweep: u64,
    pub outcome: FreshListOutcome,
}

/// The fresh objects the partitions of one shard still need.
pub struct FreshObjectsReport {
    pub sweep: u64,
    pub shard: u16,
    /// `None` when the shard cannot tell.
    pub in_use: Option<BTreeSet<u128>>,
    /// The long-term floors of its placing partitions.
    pub floors: ProofFloors,
}

/// What the task deleting fresh objects hands back to the pump.
pub struct FinishedFreshDelete {
    /// The objects the delete was for.
    pub objects: Vec<u128>,
    pub outcome: FreshDeleteOutcome,
}

/// A sweep between its listing and the last shard's answer.
struct Sweep {
    id: u64,
    now: IggyTimestamp,
    /// `None` until the listing ended.
    listing: Option<FreshListing>,
    /// The answer of each shard, by its id.
    reports: Vec<Option<BTreeSet<u128>>>,
    /// The floors the answers gave.
    floors: ProofFloors,
    /// Timer ticks since it started.
    ticks: u32,
}

/// A sweep after the last shard's answer, while it deletes.
struct Deleting {
    /// The objects that are due and in no delete job yet.
    due: VecDeque<u128>,
    /// What keeps a due object for its proof rows.
    floors: Rc<ProofFloors>,
    /// Objects of this sweep that a delete job left in the store.
    failed: usize,
    /// Objects of this sweep kept for their proof rows.
    kept: usize,
    deleted: usize,
    /// Scopes whose listing did not reach their last key.
    truncated: usize,
    /// Objects this sweep first found unnamed.
    found_unnamed: usize,
}

/// Sweeps in a row that a shard which could not tell held back.
#[derive(Debug, Clone, Copy)]
struct HeldSweeps {
    /// The time of the first of them.
    since: IggyTimestamp,
    warned: bool,
}

impl HeldSweeps {
    const fn starting_at(now: IggyTimestamp) -> Self {
        Self {
            since: now,
            warned: false,
        }
    }

    /// Take one more held sweep, at `now`. Returns how long the sweeps have
    /// been held, the first time that is longer than `grace`.
    fn held_at(&mut self, now: IggyTimestamp, grace: Duration) -> Option<Duration> {
        let held_for =
            Duration::from_micros(now.as_micros().saturating_sub(self.since.as_micros()));
        if self.warned || held_for <= grace {
            return None;
        }
        self.warned = true;
        Some(held_for)
    }
}

/// What the first shard keeps for the sweep of fresh objects.
pub struct FreshSweeps {
    sweeper: RefCell<Option<Rc<dyn FreshObjectSweeper>>>,
    state: RefCell<FreshSweep>,
    sweep: RefCell<Option<Sweep>>,
    /// Boxed, as the shard is held by value in the futures of its tests.
    deleting: RefCell<Option<Box<Deleting>>>,
    interval_micros: Cell<u64>,
    keys_per_sweep: Cell<usize>,
    /// When the tick starts the next sweep, on the bus's monotonic clock.
    /// `None` until the first tick with a sweeper.
    next_sweep_at: Cell<Option<u64>>,
    sweeps: Cell<u64>,
    /// Sweeps that took the answer of every shard.
    settled: Cell<u64>,
    /// Whether a listing or a delete is in flight. The task that runs it
    /// clears it, so a result whose frame was refused does not hold the
    /// next sweep back.
    running: Rc<Cell<bool>>,
    /// Never cancelled: a sweep belongs to no partition.
    scope: TierScope,
    /// The sweeps held back since the last one that settled.
    held: Cell<Option<HeldSweeps>>,
}

impl Default for FreshSweeps {
    fn default() -> Self {
        Self {
            sweeper: RefCell::new(None),
            state: RefCell::default(),
            sweep: RefCell::new(None),
            deleting: RefCell::new(None),
            interval_micros: Cell::new(micros(FRESH_SWEEP_INTERVAL)),
            keys_per_sweep: Cell::new(FRESH_OBJECTS_PER_SWEEP),
            next_sweep_at: Cell::new(None),
            sweeps: Cell::new(0),
            settled: Cell::new(0),
            running: Rc::default(),
            scope: TierScope::new(),
            held: Cell::new(None),
        }
    }
}

impl<B, MJ, S, M, T, SB> IggyShard<B, MJ, S, M, T, SB>
where
    B: MessageBus + 'static,
    T: ShardsTable,
    SB: SuperblockStore,
{
    /// Give this shard the sweeper of the replica's fresh objects, or take
    /// it away with `None`. A job already in flight ends on the sweeper that
    /// started it.
    pub fn set_fresh_sweeper(&self, sweeper: Option<Rc<dyn FreshObjectSweeper>>) {
        *self.fresh_sweeps.sweeper.borrow_mut() = sweeper;
    }

    /// The sweeper of the replica's fresh objects. `None` on a shard that
    /// was given none.
    #[must_use]
    pub fn fresh_sweeper(&self) -> Option<Rc<dyn FreshObjectSweeper>> {
        self.fresh_sweeps.sweeper.borrow().clone()
    }

    /// Sweeps of fresh objects that this shard ran to the answer of every
    /// shard.
    #[must_use]
    pub const fn fresh_sweeps_settled(&self) -> u64 {
        self.fresh_sweeps.settled.get()
    }

    /// Set how often this shard starts a sweep, and how many keys the
    /// listings of one sweep take together.
    pub fn set_fresh_sweep_pace(&self, interval: Duration, keys_per_sweep: usize) {
        self.fresh_sweeps.interval_micros.set(micros(interval));
        self.fresh_sweeps.keys_per_sweep.set(keys_per_sweep);
        self.fresh_sweeps.next_sweep_at.set(None);
    }

    /// Stage a sweep of the replica's fresh objects on this shard's pump,
    /// apart from the ones its tick starts. Only the first shard sweeps, so
    /// any other stages nothing, and a sweep staged while one runs is
    /// dropped.
    pub fn request_sweep_fresh_objects(&self, now: IggyTimestamp) {
        if self.id != 0 {
            return;
        }
        let Some(sender) = self.senders.get(self.id as usize) else {
            return;
        };
        let _ = sender.try_send(ShardFrame::lifecycle(LifecycleFrame::SweepFreshObjects {
            now,
        }));
    }

    /// The pump's tick: start a sweep when one is due. The first sweep is
    /// due one interval after the shard got its sweeper.
    pub(crate) fn tick_fresh_sweeps(&self) {
        let sweeps = &self.fresh_sweeps;
        if self.id != 0 || sweeps.sweeper.borrow().is_none() {
            return;
        }
        let now = self.bus.monotonic_micros();
        let next = now.saturating_add(sweeps.interval_micros.get());
        let Some(due_at) = sweeps.next_sweep_at.get() else {
            sweeps.next_sweep_at.set(Some(next));
            return;
        };
        if now < due_at {
            return;
        }
        sweeps.next_sweep_at.set(Some(next));
        self.on_sweep_fresh_objects(IggyTimestamp::from(self.bus.realtime_micros()));
    }

    pub(crate) fn on_sweep_fresh_objects(&self, now: IggyTimestamp) {
        let sweeps = &self.fresh_sweeps;
        let Some(sweeper) = sweeps.sweeper.borrow().clone() else {
            return;
        };
        if self.id != 0 || sweeps.running.get() {
            return;
        }
        // No job runs, so the frame of a delete was refused. What that sweep
        // left is due again in this one.
        *sweeps.deleting.borrow_mut() = None;
        {
            let mut sweep = sweeps.sweep.borrow_mut();
            if let Some(waiting) = sweep.as_mut() {
                waiting.ticks += 1;
                if waiting.ticks < SWEEP_TICKS_MAX {
                    return;
                }
            }
            let id = sweeps.sweeps.get() + 1;
            sweeps.sweeps.set(id);
            *sweep = Some(Sweep {
                id,
                now,
                listing: None,
                reports: vec![None; self.senders.len()],
                floors: ProofFloors::default(),
                ticks: 0,
            });
        }
        let Some(sender) = self.senders.get(self.id as usize).cloned() else {
            return;
        };
        let sweep = sweeps.sweeps.get();
        let replica = self.partition_consensus.self_replica_id;
        let shards = u16::try_from(self.senders.len()).unwrap_or(u16::MAX);
        let requests = sweeps
            .state
            .borrow()
            .plan(shards, sweeps.keys_per_sweep.get());
        let scope = sweeps.scope.token();
        let running = Rc::clone(&sweeps.running);
        running.set(true);
        // Through the bus, not the runtime directly: the simulator supplies
        // its own executor.
        self.bus.spawn(async move {
            let outcome = sweeper.list(replica, requests, scope).await;
            running.set(false);
            let finished = Box::new(FinishedFreshListing { sweep, outcome });
            let _ = sender.try_send(ShardFrame::lifecycle(LifecycleFrame::FreshObjectsListed(
                finished,
            )));
        });
    }

    /// Take the listing of a sweep, and ask every shard what its partitions
    /// still need.
    pub(crate) fn on_fresh_objects_listed(&self, finished: FinishedFreshListing) {
        let sweeps = &self.fresh_sweeps;
        let mut slot = sweeps.sweep.borrow_mut();
        let Some(sweep) = slot.as_mut().filter(|sweep| sweep.id == finished.sweep) else {
            return;
        };
        let Ok(listing) = finished.outcome else {
            *slot = None;
            return;
        };
        if listing.scopes.iter().all(|scope| scope.objects.is_empty())
            && sweeps.state.borrow().is_empty()
        {
            // Nothing to ask the shards about.
            let now = sweep.now;
            *slot = None;
            drop(slot);
            self.settle_fresh_sweep(&listing, &BTreeSet::new(), ProofFloors::default(), now);
            return;
        }
        sweep.listing = Some(listing);
        let id = sweep.id;
        drop(slot);
        for sender in &self.senders {
            let frame = ShardFrame::lifecycle(LifecycleFrame::ReportFreshObjects { sweep: id });
            if sender.try_send(frame).is_err() {
                // A shard that was not asked never answers.
                *sweeps.sweep.borrow_mut() = None;
                return;
            }
        }
    }

    /// Answer the first shard with the fresh objects the partitions of this
    /// shard still need.
    pub(crate) fn on_report_fresh_objects(&self, sweep: u64)
    where
        M: StreamsFrontend,
    {
        let Some(sender) = self.senders.first().cloned() else {
            return;
        };
        let shard = self.id;
        // Taken together with the partitions' plans, under one borrow of the
        // pump: a send that is answered after this is in its partition's
        // journal by then or names nothing.
        let awaited = self.fresh_writes.awaited_objects();
        let streams = self.plane.metadata().mux_stm.streams();
        let reads = if self.holds_committed_placing_partitions() {
            self.plane.partitions().fresh_objects_in_use(|namespace| {
                streams.created_revision_for_namespace(*namespace).is_some()
            })
        } else {
            None
        };
        let floors = self.plane.partitions().proof_floors();
        let certain = reads.is_some();
        let reads = reads.unwrap_or_default();
        self.bus.spawn(async move {
            let mut in_use = certain.then_some(awaited);
            for read in reads {
                let Some(objects) = in_use.as_mut() else {
                    break;
                };
                match read.read().await {
                    Ok(named) => objects.extend(named),
                    Err(_) => in_use = None,
                }
            }
            let report = Box::new(FreshObjectsReport {
                sweep,
                shard,
                in_use,
                floors,
            });
            let _ = sender.try_send(ShardFrame::lifecycle(LifecycleFrame::FreshObjectsReported(
                report,
            )));
        });
    }

    /// Whether this shard holds a copy, fenced or not, of every committed
    /// placing partition that this node stores and that belongs on this
    /// shard. The group of one it lacks may hold placed batches that name
    /// this replica's objects.
    fn holds_committed_placing_partitions(&self) -> bool
    where
        M: StreamsFrontend,
    {
        let partitions = self.plane.partitions();
        let replica = self.partition_consensus.self_replica_id;
        let replicas = self.partition_consensus.replica_count;
        self.plane.metadata().mux_stm.streams().read(|inner| {
            inner.items.iter().all(|(_, stream)| {
                stream
                    .topics
                    .iter()
                    .filter(|(_, topic)| topic.resolved.runtime.places_batches)
                    .all(|(topic_id, topic)| {
                        topic.partitions.iter().all(|partition| {
                            let namespace = IggyNamespace::new(stream.id, topic_id, partition.id);
                            partition.replica_set(replicas).member(replica).is_none()
                                || self.partition_shards.shard_for(&namespace) != self.id
                                || partitions.contains(&namespace)
                        })
                    })
            })
        })
    }

    /// Take a shard's answer. With the last one, delete the objects that
    /// nothing named for long enough.
    pub(crate) fn on_fresh_objects_reported(&self, report: FreshObjectsReport) {
        let sweeps = &self.fresh_sweeps;
        let mut slot = sweeps.sweep.borrow_mut();
        let Some(sweep) = slot.as_mut().filter(|sweep| sweep.id == report.sweep) else {
            return;
        };
        let Some(in_use) = report.in_use else {
            let now = sweep.now;
            *slot = None;
            drop(slot);
            self.hold_fresh_sweep(report.shard, now);
            return;
        };
        let Some(answer) = sweep.reports.get_mut(report.shard as usize) else {
            return;
        };
        *answer = Some(in_use);
        sweep.floors.extend(report.floors);
        if sweep.reports.iter().any(Option::is_none) {
            return;
        }
        let Some(sweep) = slot.take() else {
            return;
        };
        drop(slot);
        let Some(listing) = sweep.listing else {
            return;
        };
        let in_use: BTreeSet<u128> = sweep.reports.into_iter().flatten().flatten().collect();
        self.settle_fresh_sweep(&listing, &in_use, sweep.floors, sweep.now);
    }

    /// `shard` cannot tell which fresh objects it needs, so the sweep at
    /// `now` deletes nothing. Warns once the sweeps have been held for
    /// longer than the grace of an object, once per run of held sweeps.
    fn hold_fresh_sweep(&self, shard: u16, now: IggyTimestamp) {
        let sweeps = &self.fresh_sweeps;
        let mut held = sweeps
            .held
            .get()
            .unwrap_or_else(|| HeldSweeps::starting_at(now));
        if let Some(held_for) = held.held_at(now, FRESH_OBJECT_GRACE) {
            tracing::warn!(
                shard,
                held_for_secs = held_for.as_secs(),
                "no fresh object was deleted for longer than their grace: a shard cannot \
                 tell which ones its partitions need"
            );
        } else {
            tracing::debug!(
                shard,
                "a shard cannot tell which fresh objects it needs; the sweep deletes none"
            );
        }
        sweeps.held.set(Some(held));
        self.metrics.set_fresh_sweeps_held(true);
    }

    /// Take a sweep into the state the sweeps keep, and start deleting what
    /// is due and `floors` does not keep for its proof rows.
    fn settle_fresh_sweep(
        &self,
        listing: &FreshListing,
        in_use: &BTreeSet<u128>,
        floors: ProofFloors,
        now: IggyTimestamp,
    ) {
        let sweeps = &self.fresh_sweeps;
        let (settled, unnamed) = {
            let mut state = sweeps.state.borrow_mut();
            let settled = state.settle(listing, in_use, now, FRESH_OBJECT_GRACE);
            (settled, state.len())
        };
        let oldest = listing.oldest.map_or(Duration::ZERO, |oldest| {
            Duration::from_micros(
                now.as_micros()
                    .saturating_sub(IggyTimestamp::from(oldest).as_micros()),
            )
        });
        self.metrics
            .set_fresh_objects(listing.objects().len(), listing.bytes, oldest, unnamed);
        sweeps.held.set(None);
        self.metrics.set_fresh_sweeps_held(false);
        sweeps.settled.set(sweeps.settled.get() + 1);
        *sweeps.deleting.borrow_mut() = Some(Box::new(Deleting {
            due: settled.due.into(),
            floors: Rc::new(floors),
            failed: 0,
            kept: 0,
            deleted: 0,
            truncated: listing.truncated(),
            found_unnamed: settled.found_unnamed,
        }));
        self.delete_due_fresh_objects();
    }

    /// Start the next delete job of the sweep, or end the sweep when nothing
    /// is left for one.
    fn delete_due_fresh_objects(&self) {
        let sweeps = &self.fresh_sweeps;
        let (batch, floors): (Vec<u128>, _) = {
            let mut deleting = sweeps.deleting.borrow_mut();
            let Some(deleting) = deleting.as_mut() else {
                return;
            };
            let batch = deleting.due.len().min(FRESH_DELETES_PER_BATCH);
            (
                deleting.due.drain(..batch).collect(),
                Rc::clone(&deleting.floors),
            )
        };
        let sweeper = sweeps.sweeper.borrow().clone();
        let sender = self.senders.get(self.id as usize).cloned();
        let (Some(sweeper), Some(sender)) = (sweeper, sender) else {
            self.end_fresh_sweep(batch.len());
            return;
        };
        if batch.is_empty() || sweeps.running.get() {
            self.end_fresh_sweep(batch.len());
            return;
        }
        let scope = sweeps.scope.token();
        let running = Rc::clone(&sweeps.running);
        running.set(true);
        self.bus.spawn(async move {
            let outcome = sweeper.delete(batch.clone(), floors, scope).await;
            running.set(false);
            let finished = Box::new(FinishedFreshDelete {
                objects: batch,
                outcome,
            });
            let _ = sender.try_send(ShardFrame::lifecycle(LifecycleFrame::FreshObjectsDeleted(
                finished,
            )));
        });
    }

    /// Forget the objects a delete removed, and go on with the next job of
    /// the sweep. One that is still there is found by the next sweep, as is
    /// one whose result never arrived and one kept for its proof rows.
    pub(crate) fn on_fresh_objects_deleted(&self, finished: FinishedFreshDelete) {
        let sweeps = &self.fresh_sweeps;
        let sent = finished.objects.len();
        let FreshDeleted { failed, kept } = finished.outcome.unwrap_or_else(|_| FreshDeleted {
            failed: finished.objects.clone(),
            kept: Vec::new(),
        });
        let deleted: Vec<u128> = finished
            .objects
            .into_iter()
            .filter(|object| !failed.contains(object) && !kept.contains(object))
            .collect();
        if !failed.is_empty() {
            tracing::warn!(
                shard = self.id,
                failed = failed.len(),
                "some fresh objects were not deleted; the next sweep sends them again"
            );
        }
        sweeps.state.borrow_mut().forget(&deleted);
        self.metrics.record_fresh_objects_deleted(deleted.len());
        if let Some(deleting) = sweeps.deleting.borrow_mut().as_mut() {
            deleting.deleted += deleted.len();
            deleting.kept += kept.len();
            deleting.failed += sent - deleted.len() - kept.len();
        }
        if deleted.is_empty() && kept.len() < sent {
            // A store that deleted nothing of a whole job is not asked for
            // the next one.
            self.end_fresh_sweep(0);
        } else {
            self.delete_due_fresh_objects();
        }
    }

    /// The sweep is over: `unsent` objects of a job that did not start are
    /// still due, with the ones no job took and the ones a job left.
    fn end_fresh_sweep(&self, unsent: usize) {
        let sweeps = &self.fresh_sweeps;
        let Some(deleting) = sweeps.deleting.borrow_mut().take() else {
            return;
        };
        let due = deleting.due.len() + deleting.failed + unsent;
        self.metrics
            .set_fresh_sweep_backlog(deleting.truncated, due, deleting.kept);
        // Only a sweep that got something done is followed at once, so a
        // store that refuses deletes is not asked without a pause.
        let more_to_delete = due > 0 && deleting.deleted > 0;
        let more_to_list =
            deleting.truncated > 0 && (deleting.found_unnamed > 0 || deleting.deleted > 0);
        if more_to_delete || more_to_list {
            sweeps.next_sweep_at.set(Some(0));
        }
    }
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    const START: Duration = Duration::from_secs(1_000);

    fn at(elapsed: Duration) -> IggyTimestamp {
        IggyTimestamp::from(micros(START + elapsed))
    }

    #[test]
    fn given_sweeps_held_for_no_longer_than_the_grace_when_one_more_is_held_should_not_warn() {
        let mut held = HeldSweeps::starting_at(at(Duration::ZERO));

        assert_eq!(held.held_at(at(Duration::ZERO), FRESH_OBJECT_GRACE), None);
        assert_eq!(
            held.held_at(at(FRESH_OBJECT_GRACE), FRESH_OBJECT_GRACE),
            None
        );
    }

    #[test]
    fn given_sweeps_held_past_the_grace_when_more_are_held_should_warn_once() {
        let past_grace = FRESH_OBJECT_GRACE + Duration::from_secs(1);
        let mut held = HeldSweeps::starting_at(at(Duration::ZERO));

        assert_eq!(
            held.held_at(at(past_grace), FRESH_OBJECT_GRACE),
            Some(past_grace)
        );
        assert_eq!(held.held_at(at(past_grace * 2), FRESH_OBJECT_GRACE), None);
        assert_eq!(held.held_at(at(past_grace * 9), FRESH_OBJECT_GRACE), None);
    }
}
