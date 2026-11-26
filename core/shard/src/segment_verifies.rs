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

//! The verify passes over the segments tiered partitions stored, run by
//! their primary, see `partitions::SegmentVerifies`.
//!
//! A pass over a partition starts one job in a detached task, as a delete
//! pass does, from the same timed request, so the passes add no frame of
//! their own to the inbox. The task holds no partition reference: it sends
//! its result back as a frame, and the pump, the single writer of partition
//! state, settles it. The findings reach the shard's gauges through each
//! partition's `partitions::GaugeShare`.
//!
//! A shard runs at most [`SEGMENT_VERIFY_JOBS_MAX`] jobs at once, so the
//! passes never queue more than that in the tier service. A partition due
//! for a job that finds them all taken waits its turn behind the ones that
//! found them taken before it.

use std::collections::VecDeque;
use std::rc::Rc;

use consensus::PartitionsHandle;
use iggy_common::IggyTimestamp;
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use partitions::{SegmentVerifier, VerifyBegin};
use server_common::sharding::IggyNamespace;

use crate::shards_table::ShardsTable;
use crate::{FinishedSegmentVerify, IggyShard, LifecycleFrame, ShardFrame};

/// The most verify jobs one shard runs at once.
const SEGMENT_VERIFY_JOBS_MAX: usize = 4;

/// The jobs a shard's verify passes run, and the partitions waiting for one.
#[derive(Debug, Default)]
pub struct VerifySlots {
    /// Jobs started whose result the pump has not settled.
    running: usize,
    /// The partitions, by namespace and incarnation, that were due for a job
    /// and found every slot taken, in the order they found them taken.
    waiting: VecDeque<(IggyNamespace, u64)>,
    /// Changes each time the shard's verifier is given or taken away. A job
    /// started under another grant counts for nothing.
    grant: u64,
}

impl VerifySlots {
    /// Whether the partition `key`, which is due for a job when `due` says
    /// so, starts one now. A partition that is due and must wait queues. The
    /// waiting partitions that `still_due` no longer finds due leave the
    /// head of the queue first, so none blocks the others.
    fn take_turn(
        &mut self,
        key: (IggyNamespace, u64),
        due: bool,
        still_due: impl Fn(&(IggyNamespace, u64)) -> bool,
    ) -> bool {
        while self
            .waiting
            .front()
            .is_some_and(|front| *front != key && !still_due(front))
        {
            self.waiting.pop_front();
        }
        let queued = self.waiting.iter().position(|waiting| *waiting == key);
        if !due {
            if let Some(at) = queued {
                self.waiting.remove(at);
            }
            return false;
        }
        let first = queued.map_or(self.waiting.is_empty(), |at| at == 0);
        if self.running < SEGMENT_VERIFY_JOBS_MAX && first {
            if queued.is_some() {
                self.waiting.pop_front();
            }
            return true;
        }
        if queued.is_none() {
            self.waiting.push_back(key);
        }
        false
    }

    /// Settle the result of a job started under `grant`. Returns whether it
    /// counts.
    const fn settle(&mut self, grant: u64) -> bool {
        self.running = self.running.saturating_sub(1);
        grant == self.grant
    }
}

impl<B, MJ, S, M, T, SB> IggyShard<B, MJ, S, M, T, SB>
where
    B: MessageBus + 'static,
    T: ShardsTable,
    SB: SuperblockStore,
{
    /// Give this shard the verifier its verify passes run, or take it away
    /// with `None`. The results of the jobs a verifier that was taken away
    /// started count for nothing, and each partition starts its pass over
    /// once a verifier is back.
    pub fn set_segment_verifier(&self, verifier: Option<Rc<dyn SegmentVerifier>>) {
        let mut held = self.segment_verifier.borrow_mut();
        let same = match (&*held, &verifier) {
            (Some(held), Some(verifier)) => Rc::ptr_eq(held, verifier),
            (None, None) => true,
            _ => false,
        };
        if !same {
            let mut slots = self.segment_verify_slots.borrow_mut();
            slots.grant = slots.grant.wrapping_add(1);
        }
        *held = verifier;
    }

    /// The verifier this shard's verify passes run. `None` on a shard that
    /// was given none.
    #[must_use]
    pub fn segment_verifier(&self) -> Option<Rc<dyn SegmentVerifier>> {
        self.segment_verifier.borrow().clone()
    }

    /// Start the next job of the verify pass over `namespace` on the pump,
    /// for the incarnation `created_revision`, when it is due and its turn
    /// came. Run with each delete pass. Does nothing on a shard without a
    /// verifier.
    pub(crate) fn on_verify_segments(
        &self,
        namespace: IggyNamespace,
        created_revision: u64,
        now: IggyTimestamp,
    ) {
        // Every pass settles all of them, whatever partition it is for, so a
        // result is not left waiting for a pass over a deleted partition.
        let undelivered = std::mem::take(&mut *self.undelivered_segment_verifies.borrow_mut());
        for finished in &undelivered {
            self.on_segment_verify_finished(finished);
        }

        let Some(verifier) = self.segment_verifier.borrow().clone() else {
            return;
        };
        // Looked up first: a job that cannot report back is not started.
        let Some(sender) = self.senders.get(self.id as usize).cloned() else {
            return;
        };
        let grant = self.segment_verify_slots.borrow().grant;
        let partitions = self.plane.partitions();
        let due = |(namespace, created_revision): &(IggyNamespace, u64)| {
            partitions.get_by_ns(namespace).is_some_and(|partition| {
                partition.created_revision() == *created_revision
                    && partition.segment_verify_due(now, grant)
            })
        };
        let key = (namespace, created_revision);
        let due_now = due(&key);
        let turn = self
            .segment_verify_slots
            .borrow_mut()
            .take_turn(key, due_now, due);
        if due_now && !turn {
            return;
        }
        let Some(partition) = partitions
            .get_mut_by_ns(&namespace)
            .filter(|partition| partition.created_revision() == created_revision)
        else {
            return;
        };
        // Asked of a partition that is not due too: one that stopped
        // verifying drops what it found.
        let (id, job, scope) = match partition.begin_segment_verify(now, grant) {
            VerifyBegin::Idle => return,
            VerifyBegin::PassEnded => {
                self.metrics.record_segment_verify_pass();
                return;
            }
            VerifyBegin::Job { id, job, scope } => (id, job, scope),
        };
        self.segment_verify_slots.borrow_mut().running += 1;
        let undelivered = Rc::clone(&self.undelivered_segment_verifies);
        // Through the bus, not the runtime directly: the simulator supplies
        // its own executor.
        self.bus.spawn(async move {
            let finished = Box::new(FinishedSegmentVerify {
                namespace,
                created_revision,
                grant,
                id,
                outcome: verifier.verify(job, scope).await,
            });
            let frame = ShardFrame::lifecycle(LifecycleFrame::SegmentVerifyFinished(finished));
            // The partition starts no other job until it sees this result,
            // so a refused frame must not lose it.
            if let Err(
                crossfire::TrySendError::Full(frame) | crossfire::TrySendError::Disconnected(frame),
            ) = sender.try_send(frame)
                && let ShardFrame::Lifecycle(LifecycleFrame::SegmentVerifyFinished(finished)) =
                    frame
            {
                undelivered.borrow_mut().push(*finished);
            }
        });
    }

    pub(crate) fn on_segment_verify_finished(&self, finished: &FinishedSegmentVerify) {
        if !self
            .segment_verify_slots
            .borrow_mut()
            .settle(finished.grant)
        {
            return;
        }
        if let Some(partition) = self
            .plane
            .partitions()
            .get_mut_by_ns(&finished.namespace)
            .filter(|partition| partition.created_revision() == finished.created_revision)
            && let Some(tally) = partition.finish_segment_verify(finished.id, &finished.outcome)
        {
            self.metrics.record_stored_segments_verified(tally.checked);
            if tally.unanswered {
                self.metrics.record_segment_verify_unanswered();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(partition_id: usize) -> (IggyNamespace, u64) {
        (IggyNamespace::new(1, 1, partition_id), 1)
    }

    /// Fill every slot with partitions `100..`.
    fn full() -> VerifySlots {
        let mut slots = VerifySlots::default();
        for partition_id in 100..100 + SEGMENT_VERIFY_JOBS_MAX {
            assert!(slots.take_turn(key(partition_id), true, |_| true));
            slots.running += 1;
        }
        slots
    }

    #[test]
    fn given_every_slot_taken_when_a_partition_is_due_should_start_no_job_until_one_frees() {
        let mut slots = full();

        assert!(!slots.take_turn(key(0), true, |_| true));
        assert!(!slots.take_turn(key(0), true, |_| true));
        assert_eq!(slots.waiting.len(), 1, "queued once");

        assert!(slots.settle(slots.grant));
        assert!(slots.take_turn(key(0), true, |_| true));
        assert!(slots.waiting.is_empty());
    }

    #[test]
    fn given_partitions_that_waited_when_a_slot_frees_should_give_it_to_the_first_that_waited() {
        let mut slots = full();
        assert!(!slots.take_turn(key(1), true, |_| true));
        assert!(!slots.take_turn(key(2), true, |_| true));
        slots.settle(slots.grant);

        assert!(
            !slots.take_turn(key(3), true, |_| true),
            "queues behind them"
        );
        assert!(!slots.take_turn(key(2), true, |_| true), "not the first");
        assert!(slots.take_turn(key(1), true, |_| true));
        slots.running += 1;
        slots.settle(slots.grant);
        assert!(slots.take_turn(key(2), true, |_| true));
        slots.running += 1;
        slots.settle(slots.grant);
        assert!(slots.take_turn(key(3), true, |_| true));
    }

    #[test]
    fn given_a_waiting_partition_that_is_no_longer_due_when_another_asks_should_drop_it() {
        let mut slots = full();
        assert!(!slots.take_turn(key(1), true, |_| true));
        slots.settle(slots.grant);

        assert!(slots.take_turn(key(2), true, |waiting| *waiting != key(1)));
        assert!(slots.waiting.is_empty());
    }

    #[test]
    fn given_a_partition_that_stopped_being_due_when_it_asks_should_leave_the_queue() {
        let mut slots = full();
        assert!(!slots.take_turn(key(1), true, |_| true));

        assert!(!slots.take_turn(key(1), false, |_| true));
        assert!(slots.waiting.is_empty());
    }

    #[test]
    fn given_a_job_of_a_revoked_grant_when_it_settles_should_free_its_slot_and_not_count() {
        let mut slots = full();
        let started_under = slots.grant;
        slots.grant += 1;

        assert!(!slots.settle(started_under));
        assert_eq!(slots.running, SEGMENT_VERIFY_JOBS_MAX - 1);
    }
}
