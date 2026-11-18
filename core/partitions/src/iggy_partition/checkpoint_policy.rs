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

//! When a partition comes due for a WAL checkpoint.
//!
//! A partition comes due at min(last op + idle delay, first op since its
//! last checkpoint + max delay). A quiet partition checkpoints once after its
//! burst, and a busy one at least once per max delay. Without this rule a
//! partition that stops below the WAL size trigger never checkpoints, and a
//! boot loads all of its ops again.
//!
//! At the deadline the shard puts the partition into its checkpoint queue,
//! which paces the starts (see [`crate::IggyPartitions`]), and the partition
//! sleeps until the queue reaches it.
//!
//! Invariant: a partition with a committed op above its checkpoint has
//! stamps, a queue entry, a checkpoint request or a running checkpoint. The
//! stamps settle when a checkpoint starts, not when it ends, because commits
//! continue while it runs.

use super::IggyPartition;
use consensus::TICK_INTERVAL;
use journal::Journal as _;
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use std::cell::Cell;
use std::time::Duration;

/// The two delays of the checkpoint rule, in shard ticks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CheckpointDelays {
    idle_ticks: u64,
    max_ticks: u64,
}

impl CheckpointDelays {
    /// Rounds each delay up to whole ticks. The configuration validator
    /// rejects a zero delay and an idle delay above the max delay.
    #[must_use]
    pub fn new(idle: Duration, max: Duration) -> Self {
        debug_assert!(
            !idle.is_zero() && idle <= max,
            "checkpoint delays {idle:?} and {max:?} passed no validation"
        );
        Self {
            idle_ticks: whole_ticks(idle),
            max_ticks: whole_ticks(max),
        }
    }
}

/// Why a checkpoint started.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CheckpointTrigger {
    /// No op for the idle delay.
    Idle,
    /// The max delay passed since the first op above the checkpoint.
    Age,
    /// The graceful stop served the partition.
    Stop,
    /// The WAL size or dirty file trigger, or a direct request: the command
    /// caps, retention, tiered eviction or the offset re-anchor at boot.
    Demand,
    /// The WAL bytes above the checkpoints of the shard passed its share of
    /// the reload budget.
    Reload,
}

impl CheckpointTrigger {
    /// The number of triggers.
    pub const COUNT: usize = 5;

    /// Every trigger, in [`CheckpointTrigger::index`] order.
    pub const ALL: [Self; Self::COUNT] = [
        Self::Idle,
        Self::Age,
        Self::Stop,
        Self::Demand,
        Self::Reload,
    ];

    /// The position of the trigger in [`CheckpointTrigger::ALL`], for arrays
    /// with one slot per trigger.
    #[must_use]
    pub const fn index(self) -> usize {
        match self {
            Self::Idle => 0,
            Self::Age => 1,
            Self::Stop => 2,
            Self::Demand => 3,
            Self::Reload => 4,
        }
    }

    /// The name of the trigger, as a static string for labels.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Age => "age",
            Self::Stop => "stop",
            Self::Demand => "demand",
            Self::Reload => "reload",
        }
    }
}

/// The checkpoint state of one partition. The shard owns the delays, the
/// queue and the stop deadline. Every partition holds this state, so it
/// stays small.
#[derive(Debug, Default)]
pub struct CheckpointPolicy {
    /// Whether the partition holds an op above its checkpoint that a
    /// deadline still has to cover. The two ticks are valid only while it
    /// does.
    stamped: Cell<bool>,
    /// The shard tick of the first op since the last checkpoint start.
    first_op: Cell<u64>,
    /// The shard tick of the last op.
    last_op: Cell<u64>,
    /// Whether the shard checkpoint queue holds this partition. One entry per
    /// partition.
    queued: Cell<bool>,
    /// The queue trigger of a request that waits for its start.
    requested: Cell<Option<CheckpointTrigger>>,
    /// The graceful stop waits before it drains the writers, until the
    /// checkpoint starts or the shard ends the hold.
    stop_hold: Cell<bool>,
}

impl CheckpointPolicy {
    /// Restarts both stamps at `now`, or clears them.
    fn restart_stamps(&self, stamped: bool, now: u64) {
        self.stamped.set(stamped);
        self.first_op.set(now);
        self.last_op.set(now);
    }

    /// The tick at which the partition comes due, and which rule fires first.
    const fn deadline(&self, delays: CheckpointDelays) -> Option<(u64, CheckpointTrigger)> {
        if !self.stamped.get() {
            return None;
        }
        let idle = self.last_op.get().saturating_add(delays.idle_ticks);
        let age = self.first_op.get().saturating_add(delays.max_ticks);
        Some(if idle <= age {
            (idle, CheckpointTrigger::Idle)
        } else {
            (age, CheckpointTrigger::Age)
        })
    }
}

impl<B, SB> IggyPartition<B, SB>
where
    B: MessageBus,
    SB: SuperblockStore,
{
    /// Stamps the ops that a boot reloaded above the checkpoint. A clean
    /// partition gets no stamps, so it does not wake one idle delay after
    /// the boot.
    ///
    /// Runs after the shard clock is installed. It replaces the stamps of the
    /// boot, which count on the private clock of the consensus.
    pub(crate) fn reset_checkpoint_stamps(&self) {
        let reloaded = self
            .persistence
            .as_ref()
            .is_some_and(|persistence| persistence.head() > persistence.checkpoint_op());
        self.checkpoint_policy
            .restart_stamps(reloaded, self.consensus.tick_clock().now());
    }

    /// Records a journaled op. Only a partition with a WAL keeps stamps.
    pub(super) fn stamp_checkpoint_op(&self) {
        if self.persistence.is_none() {
            return;
        }
        let policy = &self.checkpoint_policy;
        let now = self.consensus.tick_clock().now();
        if !policy.stamped.replace(true) {
            policy.first_op.set(now);
        }
        policy.last_op.set(now);
    }

    /// The tick at which this partition comes due for a checkpoint.
    #[must_use]
    pub fn checkpoint_deadline(&self, delays: CheckpointDelays) -> Option<u64> {
        self.checkpoint_policy
            .deadline(delays)
            .map(|(deadline, _)| deadline)
    }

    /// The tick of the first op since the last checkpoint start. The stop
    /// serves partitions outside the queue in this order.
    pub(crate) fn checkpoint_first_op_tick(&self) -> Option<u64> {
        let policy = &self.checkpoint_policy;
        policy.stamped.get().then(|| policy.first_op.get())
    }

    /// Handles a deadline at or before `now`. Returns the trigger when the
    /// partition goes into the shard queue, which clears its stamps.
    pub fn take_due_checkpoint(
        &self,
        now: u64,
        delays: CheckpointDelays,
    ) -> Option<CheckpointTrigger> {
        let (deadline, trigger) = self.checkpoint_policy.deadline(delays)?;
        if deadline > now {
            return None;
        }
        self.queue_stamped_checkpoint(now, trigger)
    }

    /// The entry rule of the shard queue for a stamped partition, which the
    /// deadline and the reload budget share. Returns `trigger` when the
    /// partition goes into the queue, which clears its stamps.
    ///
    /// A running checkpoint and uncommitted ops arm the stamps again at `now`:
    /// the queue entry of a running checkpoint would request nothing, and an
    /// uncommitted op has nothing to cover yet.
    pub(crate) fn queue_stamped_checkpoint(
        &self,
        now: u64,
        trigger: CheckpointTrigger,
    ) -> Option<CheckpointTrigger> {
        let policy = &self.checkpoint_policy;
        if self.fatal.is_some() {
            policy.stamped.set(false);
            return None;
        }
        if self.checkpoint_in_progress() {
            policy.restart_stamps(true, now);
            return None;
        }
        if !self.has_uncheckpointed_commits() {
            let uncommitted = self.last_journaled_op() > self.consensus.commit_min();
            policy.restart_stamps(uncommitted, now);
            return None;
        }
        policy.stamped.set(false);
        (!policy.queued.replace(true)).then_some(trigger)
    }

    /// Takes this partition off the shard queue. Returns whether it now waits
    /// for a checkpoint start, which the shard counts against its limit of
    /// running checkpoints.
    ///
    /// A checkpoint in progress requests nothing: it settled the stamps at
    /// its start, and the request would stop the run-ahead after it ends. A
    /// pop that requests nothing ends a stop hold, which would wait for a
    /// start that does not come.
    pub(crate) fn request_queued_checkpoint(&self, trigger: CheckpointTrigger) -> bool {
        self.checkpoint_policy.queued.set(false);
        let requested = self.fatal.is_none()
            && !self.checkpoint_in_progress()
            && self.has_uncheckpointed_commits();
        let Some(persistence) = self.persistence.as_ref().filter(|_| requested) else {
            self.release_checkpoint_hold();
            return false;
        };
        self.checkpoint_policy.requested.set(Some(trigger));
        persistence.request_checkpoint();
        true
    }

    /// Whether the shard queue holds this partition.
    pub(crate) const fn in_checkpoint_queue(&self) -> bool {
        self.checkpoint_policy.queued.get()
    }

    /// Marks a partition that the graceful stop puts at the back of the
    /// queue.
    pub(crate) fn join_checkpoint_queue(&self) {
        self.checkpoint_policy.queued.set(true);
    }

    /// Whether the graceful stop has a checkpoint to serve here: a committed
    /// op above the checkpoint and none in progress.
    pub(crate) fn needs_stop_checkpoint(&self) -> bool {
        self.fatal.is_none() && !self.checkpoint_in_progress() && self.has_uncheckpointed_commits()
    }

    /// Whether a checkpoint that the queue requested still waits for its
    /// start or runs. A fenced partition starts none. A request with nothing
    /// left to cover, after an install for example, fires at the next commit
    /// and no longer counts.
    pub(crate) fn queued_checkpoint_in_flight(&self) -> bool {
        self.fatal.is_none()
            && (self.checkpoint_in_progress()
                || (self.checkpoint_policy.requested.get().is_some()
                    && self.has_uncheckpointed_commits()))
    }

    /// Whether a checkpoint started now would cover a committed op.
    pub(crate) fn has_uncheckpointed_commits(&self) -> bool {
        self.persistence
            .as_ref()
            .is_some_and(|persistence| self.consensus.commit_min() > persistence.checkpoint_op())
    }

    /// Settles the policy at a checkpoint start that covers `through_op`, and
    /// returns why the checkpoint started. Ops above `through_op` keep a
    /// deadline from this tick on.
    pub(super) fn settle_checkpoint_start(&self, through_op: u64) -> CheckpointTrigger {
        let policy = &self.checkpoint_policy;
        let trigger = policy.requested.take().unwrap_or(CheckpointTrigger::Demand);
        policy.restart_stamps(
            self.last_journaled_op() > through_op,
            self.consensus.tick_clock().now(),
        );
        policy.stop_hold.set(false);
        trigger
    }

    /// Holds the graceful stop of this partition before it drains its
    /// writers, until its checkpoint starts or the shard ends the hold.
    /// Without the hold the partition can complete first, and a checkpoint
    /// that starts later can be cut by the exit.
    pub(crate) fn hold_shutdown_for_checkpoint(&self) {
        self.checkpoint_policy.stop_hold.set(true);
    }

    /// Ends the stop hold. Returns whether the partition still held, so its
    /// checkpoint did not start.
    pub(crate) fn release_checkpoint_hold(&self) -> bool {
        let held = self.checkpoint_policy.stop_hold.replace(false);
        if held {
            self.notify_io();
        }
        held
    }

    pub(super) const fn holds_shutdown_for_checkpoint(&self) -> bool {
        self.checkpoint_policy.stop_hold.get()
    }

    /// The highest op in the journal or the WAL.
    fn last_journaled_op(&self) -> u64 {
        let journal = self.log.journal().inner.last_op().unwrap_or(0);
        self.persistence
            .as_ref()
            .map_or(journal, |persistence| journal.max(persistence.head()))
    }

    pub(crate) fn checkpoint_in_progress(&self) -> bool {
        self.maintenance.checkpoint.is_some() || self.persistence_checkpoint_pending()
    }
}

fn whole_ticks(delay: Duration) -> u64 {
    u64::try_from(delay.as_nanos().div_ceil(TICK_INTERVAL.as_nanos())).unwrap_or(u64::MAX)
}
