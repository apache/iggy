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
//! A sweep lists the objects, and then asks every shard which objects its
//! partitions name above their long-term floors or a send still waits on.
//! Each shard reads that after the listing ended, so an object the listing
//! holds and no answer names was already unnamed when it was listed. Such
//! an object is deleted once the sweeps found it so for a grace period.
//!
//! A shard answers that it cannot tell while one of its placing partitions
//! is not in normal status, such as during a view change or a state
//! transfer: that partition may be missing batches its group holds. The
//! sweep then deletes nothing.
//!
//! Every step reports back as a frame, and a frame the inbox refused loses
//! the sweep it belongs to and nothing else. The next sweep lists again.

use crate::shards_table::ShardsTable;
use crate::{IggyShard, LifecycleFrame, ShardFrame};
use consensus::{Consensus, PartitionsHandle};
use iggy_common::IggyTimestamp;
use iggy_object_store::TierScope;
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use partitions::{
    FRESH_OBJECT_GRACE, FreshDeleteOutcome, FreshListOutcome, FreshListing, FreshObjectSweeper,
    FreshSweep,
};
use std::cell::{Cell, RefCell};
use std::collections::BTreeSet;
use std::rc::Rc;
use std::time::SystemTime;

/// Timer ticks a sweep may wait for the answers of the shards before the
/// next one replaces it.
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
    /// Timer ticks since it started.
    ticks: u32,
}

/// What the first shard keeps for the sweep of fresh objects.
pub struct FreshSweeps {
    sweeper: RefCell<Option<Rc<dyn FreshObjectSweeper>>>,
    state: RefCell<FreshSweep>,
    sweep: RefCell<Option<Sweep>>,
    sweeps: Cell<u64>,
    /// Sweeps that took the answer of every shard.
    settled: Cell<u64>,
    /// Whether a listing or a delete is in flight. The task that runs it
    /// clears it, so a result whose frame was refused does not hold the
    /// next sweep back.
    running: Rc<Cell<bool>>,
    /// Never cancelled: a sweep belongs to no partition.
    scope: TierScope,
}

impl Default for FreshSweeps {
    fn default() -> Self {
        Self {
            sweeper: RefCell::new(None),
            state: RefCell::default(),
            sweep: RefCell::new(None),
            sweeps: Cell::new(0),
            settled: Cell::new(0),
            running: Rc::default(),
            scope: TierScope::new(),
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

    /// Stage a sweep of the replica's fresh objects on this shard's pump.
    /// Only the first shard sweeps, so any other stages nothing. A refused
    /// frame is picked up by the timer's next tick.
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

    pub(crate) fn on_sweep_fresh_objects(&self, now: IggyTimestamp) {
        let sweeps = &self.fresh_sweeps;
        let Some(sweeper) = sweeps.sweeper.borrow().clone() else {
            return;
        };
        if self.id != 0 || sweeps.running.get() {
            return;
        }
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
                ticks: 0,
            });
        }
        let Some(sender) = self.senders.get(self.id as usize).cloned() else {
            return;
        };
        let sweep = sweeps.sweeps.get();
        let replica = self.partition_consensus.self_replica_id;
        let scope = sweeps.scope.token();
        let running = Rc::clone(&sweeps.running);
        running.set(true);
        // Through the bus, not the runtime directly: the simulator supplies
        // its own executor.
        self.bus.spawn(async move {
            let outcome = sweeper.list(replica, scope).await;
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
        let listing = match finished.outcome {
            Ok(Ok(listing)) => listing,
            Ok(Err(error)) => {
                tracing::warn!(shard = self.id, %error, "listing the fresh objects failed");
                *slot = None;
                return;
            }
            Err(_) => {
                *slot = None;
                return;
            }
        };
        if listing.objects.is_empty() {
            // Nothing to ask the shards about.
            let now = sweep.now;
            *slot = None;
            drop(slot);
            sweeps
                .state
                .borrow_mut()
                .settle(&listing, &BTreeSet::new(), now, FRESH_OBJECT_GRACE);
            self.metrics
                .set_fresh_objects(0, 0, std::time::Duration::ZERO, 0);
            sweeps.settled.set(sweeps.settled.get() + 1);
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
    pub(crate) fn on_report_fresh_objects(&self, sweep: u64) {
        let Some(sender) = self.senders.first().cloned() else {
            return;
        };
        let shard = self.id;
        // Taken together with the partitions' plans, under one borrow of the
        // pump: a send that is answered after this is in its partition's
        // journal by then or names nothing.
        let awaited = self.fresh_writes.awaited_objects();
        let partitions = self.plane.partitions();
        let mut reads = Vec::new();
        let mut certain = true;
        for namespace in partitions.namespaces() {
            let Some(partition) = partitions.get_by_ns(namespace) else {
                continue;
            };
            let Some(read) = partition.fresh_objects_in_use() else {
                continue;
            };
            if !partition.consensus().is_normal() {
                certain = false;
                break;
            }
            reads.push(read);
        }
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
            });
            let _ = sender.try_send(ShardFrame::lifecycle(LifecycleFrame::FreshObjectsReported(
                report,
            )));
        });
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
            tracing::debug!(
                shard = report.shard,
                "a shard cannot tell which fresh objects it needs; the sweep deletes none"
            );
            *slot = None;
            return;
        };
        let Some(answer) = sweep.reports.get_mut(report.shard as usize) else {
            return;
        };
        *answer = Some(in_use);
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
        let (due, unnamed) = {
            let mut state = sweeps.state.borrow_mut();
            let due = state.settle(&listing, &in_use, sweep.now, FRESH_OBJECT_GRACE);
            (due, state.len())
        };
        let oldest = listing
            .oldest
            .and_then(|oldest| SystemTime::now().duration_since(oldest).ok())
            .unwrap_or_default();
        self.metrics
            .set_fresh_objects(listing.objects.len(), listing.bytes, oldest, unnamed);
        sweeps.settled.set(sweeps.settled.get() + 1);
        if due.is_empty() || sweeps.running.get() {
            return;
        }
        let Some(sweeper) = sweeps.sweeper.borrow().clone() else {
            return;
        };
        let Some(sender) = self.senders.get(self.id as usize).cloned() else {
            return;
        };
        let scope = sweeps.scope.token();
        let running = Rc::clone(&sweeps.running);
        running.set(true);
        self.bus.spawn(async move {
            let outcome = sweeper.delete(due.clone(), scope).await;
            running.set(false);
            let finished = Box::new(FinishedFreshDelete {
                objects: due,
                outcome,
            });
            let _ = sender.try_send(ShardFrame::lifecycle(LifecycleFrame::FreshObjectsDeleted(
                finished,
            )));
        });
    }

    /// Forget the objects a delete removed. One that is still there is found
    /// by the next sweep, as is one whose result never arrived.
    pub(crate) fn on_fresh_objects_deleted(&self, finished: FinishedFreshDelete) {
        let Ok(failed) = finished.outcome else {
            return;
        };
        let deleted: Vec<u128> = finished
            .objects
            .into_iter()
            .filter(|object| !failed.contains(object))
            .collect();
        if !failed.is_empty() {
            tracing::warn!(
                shard = self.id,
                failed = failed.len(),
                "some fresh objects were not deleted; the next sweep sends them again"
            );
        }
        self.fresh_sweeps.state.borrow_mut().forget(&deleted);
        self.metrics.record_fresh_objects_deleted(deleted.len());
    }
}
