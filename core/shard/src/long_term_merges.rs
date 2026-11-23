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

//! Merges of small long-term segments by the partition primaries of a
//! shard.
//!
//! A shard runs one merge at a time. Every [`LONG_TERM_MERGE_INTERVAL`] it
//! asks each partition it rewrites how much shorter a merge would make its
//! long-term log, and starts the merge of the one that gains most. It then
//! stays with that partition: when the merged segment's command commits,
//! the partition's next merge starts at once, so a long run of small
//! segments is merged without a wait in between. The shard looks at the
//! other partitions again once that partition has nothing left to merge
//! and the interval is over.
//!
//! The interval bounds what merging costs a partition that grows slowly: a
//! segment that a merge left small is merged again with what follows it by
//! a scan, and not by the pass after a merge.
//!
//! A partition whose long-term log is near its limit does not wait for the
//! scan: the commit that leaves it there makes a pass over it due as soon
//! as the shard merges for no other partition.
//!
//! A merge runs in a detached task and reports back as a frame, as a
//! rewrite does. Nothing here is shared with the rewrites, so a rewrite
//! never waits for a merge.

use crate::rewrite_pacing::{REWRITE_INTERVAL_MIN, RewriteKey};
use crate::shards_table::ShardsTable;
use crate::{
    FinishedSegmentMerge, IggyShard, LifecycleFrame, RestorableMetadataStm, ShardFrame, channel,
};
use consensus::PartitionsHandle;
use iggy_binary_protocol::{PrepareHeader, RoutedRequestHeader};
use iggy_common::IggyTimestamp;
use iggy_object_store::{ManifestSegment, TierScopeToken};
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use message_bus::MessageBus;
use partitions::long_term::ObjectRegistration;
use partitions::state_machine::CommandEncodeError;
use partitions::{
    IggyPartition, LongTermMerger, MergePass, MergeSettled, MergeStart, SegmentMerge,
};
use server_common::Message;
use server_common::sharding::IggyNamespace;
use std::cell::RefCell;
use std::cmp::Reverse;
use std::collections::BTreeSet;
use std::rc::Rc;
use std::time::Duration;

/// How long after a scan of its partitions for segments to merge a shard
/// scans them again.
pub const LONG_TERM_MERGE_INTERVAL: Duration = Duration::from_mins(5);

/// When a shard looks for a merge, and the partition whose merge it runs.
///
/// Every decision takes the time it is made at, so nothing here reads a
/// clock.
#[derive(Debug, Default)]
pub struct MergeSchedule {
    /// When the next scan is due, in the caller's microseconds.
    scan_due_at: u64,
    /// The partition this shard merges for, and when its next pass is due.
    active: Option<(RewriteKey, u64)>,
    /// The partitions whose long-term log changed while it was near its
    /// limit, and that the shard has not passed over since.
    pressed: BTreeSet<RewriteKey>,
}

impl MergeSchedule {
    /// The partition this shard merges for, when a pass over it is due at
    /// `now`.
    #[must_use]
    pub fn due_pass(&self, now: u64) -> Option<RewriteKey> {
        self.active
            .filter(|(_, due_at)| *due_at <= now)
            .map(|(key, _)| key)
    }

    /// Whether a scan is due at `now`. It is not while the shard merges for
    /// a partition. A scan that is due is taken, and the next one is due an
    /// interval later.
    pub fn take_scan(&mut self, now: u64) -> bool {
        let due = self.active.is_none() && self.scan_due_at <= now;
        if due {
            self.scan_due_at = now.saturating_add(micros(LONG_TERM_MERGE_INTERVAL));
        }
        due
    }

    /// The long-term log of `key` changed and is near its limit, so a pass
    /// over the partition is due without a scan. The partition the shard
    /// merges for gets its passes already.
    pub fn press(&mut self, key: RewriteKey) {
        if self.active.is_none_or(|(active, _)| active != key) {
            self.pressed.insert(key);
        }
    }

    /// A partition whose log is near its limit, to pass over now. `None`
    /// while the shard merges for a partition. A partition is handed out
    /// once for every time its log changed, so one that has nothing to
    /// merge is not passed over again and again.
    pub fn take_pressed(&mut self) -> Option<RewriteKey> {
        if self.active.is_some() {
            return None;
        }
        self.pressed.pop_first()
    }

    /// Merge for `key` and for no other partition, and pass over it again
    /// at `due_at`.
    pub fn hold(&mut self, key: RewriteKey, due_at: u64) {
        self.pressed.remove(&key);
        self.active = Some((key, due_at));
    }

    /// Stop merging for `key`, so the next scan picks a partition.
    pub fn release(&mut self, key: &RewriteKey) {
        if self.active.is_some_and(|(active, _)| active == *key) {
            self.active = None;
        }
    }

    /// A command of the long-term log of `key` committed at `now`. When the
    /// shard merges for it, the pass that settles the merge is due.
    pub fn committed(&mut self, key: &RewriteKey, now: u64) {
        if let Some((active, due_at)) = &mut self.active
            && active == key
        {
            *due_at = now;
        }
    }

    /// Make the next scan due at `now`.
    #[cfg(test)]
    pub(crate) const fn scan_at(&mut self, now: u64) {
        self.scan_due_at = now;
    }
}

/// What a shard keeps of the merges of its partitions.
#[derive(Default)]
pub struct ShardMerges {
    /// Late-bound merger of long-term segments. `None` on a shard without
    /// an object store, where nothing is merged.
    pub(crate) merger: RefCell<Option<Rc<dyn LongTermMerger>>>,
    /// Results of merges whose frame the inbox refused. The partition
    /// counts such a merge as in flight until it sees the result, so the
    /// next tick takes them from here.
    pub(crate) undelivered: Rc<RefCell<Vec<FinishedSegmentMerge>>>,
    pub(crate) schedule: RefCell<MergeSchedule>,
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

/// Where the best merge of a partition stands among those of the shard's
/// other partitions, the highest first. A log near its limit stops taking
/// segments soon, so its merge goes before any that only saves more
/// segments.
fn merge_rank(
    pressed: bool,
    reclaim: usize,
    start_offset: u64,
    namespace: IggyNamespace,
) -> (bool, usize, Reverse<u64>, Reverse<u64>) {
    (
        pressed,
        reclaim,
        Reverse(start_offset),
        Reverse(namespace.inner()),
    )
}

/// What a pass over a partition found to do.
enum MergeStep {
    /// Send the command of the merged segment again.
    Resend(ManifestSegment),
    /// Commit the registration of what the next merge stores, which a later
    /// pass starts.
    Register(ObjectRegistration),
    Start((u64, SegmentMerge, TierScopeToken)),
    /// A merge runs.
    Wait,
    /// Nothing is left to merge.
    Idle,
}

impl<B, MJ, S, M, T, SB> IggyShard<B, MJ, S, M, T, SB>
where
    B: MessageBus + 'static,
    T: ShardsTable,
    SB: SuperblockStore,
{
    /// Give this shard the merger its merges run on, or take it away with
    /// `None`. A merge already in flight ends on the merger that started
    /// it.
    pub fn set_long_term_merger(&self, merger: Option<Rc<dyn LongTermMerger>>) {
        *self.long_term_merges.merger.borrow_mut() = merger;
    }

    /// The merger this shard's merges run on. `None` on a shard that was
    /// given none.
    #[must_use]
    pub fn long_term_merger(&self) -> Option<Rc<dyn LongTermMerger>> {
        self.long_term_merges.merger.borrow().clone()
    }

    /// A command changed the long-term log of `key`. `pressed` says that
    /// the log is near its limit and this shard merges it.
    pub(crate) fn long_term_merge_committed(&self, key: &RewriteKey, pressed: bool) {
        let mut schedule = self.long_term_merges.schedule.borrow_mut();
        schedule.committed(key, self.bus.monotonic_micros());
        if pressed {
            schedule.press(*key);
        }
    }

    /// The pump's tick: settle the merges whose frame the inbox refused,
    /// pass over the partition this shard merges for when that is due, and
    /// when the shard merges for none, pass over a partition whose log is
    /// near its limit, or scan for the next one.
    #[allow(clippy::future_not_send)]
    pub(crate) async fn tick_long_term_merges(&self)
    where
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        let undelivered = std::mem::take(&mut *self.long_term_merges.undelivered.borrow_mut());
        for finished in undelivered {
            self.settle_segment_merge(finished).await;
        }
        // A scan taken without a merger would cost the shard an interval.
        if self.long_term_merges.merger.borrow().is_none() {
            return;
        }
        let now = self.bus.monotonic_micros();
        let due = self.long_term_merges.schedule.borrow().due_pass(now);
        if let Some(key) = due {
            self.run_merge_pass(key, MergePass::Drain).await;
            return;
        }
        let pressed = self.long_term_merges.schedule.borrow_mut().take_pressed();
        if let Some(key) = pressed {
            self.run_merge_pass(key, MergePass::Drain).await;
            return;
        }
        if !self.long_term_merges.schedule.borrow_mut().take_scan(now) {
            return;
        }
        if let Some(key) = self.best_merge() {
            self.run_merge_pass(key, MergePass::Scan).await;
        }
    }

    #[allow(clippy::future_not_send)]
    pub(crate) async fn on_segment_merge_finished(&self, finished: FinishedSegmentMerge)
    where
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        self.settle_segment_merge(finished).await;
    }

    /// The partition whose best merge makes its long-term log shortest, of
    /// the partitions this shard rewrites. Of several that gain alike, the
    /// one whose merge starts at the lowest offset. A partition whose log
    /// is near its limit goes before any that is not.
    fn best_merge(&self) -> Option<RewriteKey> {
        let partitions = self.plane.partitions();
        self.rewrite_pacing
            .borrow()
            .paced()
            .filter_map(|key| {
                let (namespace, created_revision) = key;
                let partition = partitions
                    .get_by_ns(namespace)
                    .filter(|partition| partition.created_revision() == *created_revision)?;
                let (reclaim, start_offset) = partition.segment_merge_reclaim()?;
                let pressed = partition.merges_under_pressure();
                Some((merge_rank(pressed, reclaim, start_offset, *namespace), *key))
            })
            .max_by_key(|(rank, _)| *rank)
            .map(|(_, key)| key)
    }

    /// Pass over the partition `key`: settle the merged segment it stored,
    /// send its command again while the long-term log can still list it,
    /// and start the partition's next merge otherwise. The shard stays with
    /// the partition until it has nothing left to merge.
    #[allow(clippy::future_not_send)]
    async fn run_merge_pass(&self, key: RewriteKey, pass: MergePass)
    where
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        let (namespace, created_revision) = key;
        let merger = self.long_term_merges.merger.borrow().clone();
        // Looked up first: a merge that cannot report back is not started.
        let sender = self.senders.get(self.id as usize).cloned();
        let (Some(merger), Some(sender)) = (merger, sender) else {
            self.long_term_merges.schedule.borrow_mut().release(&key);
            return;
        };
        let step = {
            let partition = self
                .plane
                .partitions()
                .get_mut_by_ns(&namespace)
                .filter(|partition| partition.created_revision() == created_revision);
            let Some(partition) = partition else {
                self.long_term_merges.schedule.borrow_mut().release(&key);
                return;
            };
            match partition.settle_segment_merge() {
                MergeSettled::Waiting(stored) => MergeStep::Resend(stored),
                settled => {
                    if let MergeSettled::Committed { replaced } = settled {
                        self.metrics.record_long_term_merge(replaced);
                    }
                    match partition
                        .begin_segment_merge(pass, IggyTimestamp::from(self.bus.realtime_micros()))
                    {
                        Some(MergeStart::Merge(id, merge, scope)) => {
                            MergeStep::Start((id, merge, scope))
                        }
                        Some(MergeStart::Register(registration)) => {
                            MergeStep::Register(registration)
                        }
                        None if partition.segment_merge_running() => MergeStep::Wait,
                        None => MergeStep::Idle,
                    }
                }
            }
        };
        // The commit of a command and the result of a merge both make the
        // pass due before this. The wait covers a command that got lost and
        // a partition that stopped being the primary.
        let next_pass = self
            .bus
            .monotonic_micros()
            .saturating_add(micros(REWRITE_INTERVAL_MIN));
        let (id, merge, scope) = match step {
            MergeStep::Idle => {
                self.long_term_merges.schedule.borrow_mut().release(&key);
                return;
            }
            MergeStep::Wait => {
                self.long_term_merges
                    .schedule
                    .borrow_mut()
                    .hold(key, next_pass);
                return;
            }
            MergeStep::Resend(stored) => {
                self.long_term_merges
                    .schedule
                    .borrow_mut()
                    .hold(key, next_pass);
                self.replace_segments(namespace, &stored).await;
                return;
            }
            MergeStep::Register(registration) => {
                self.long_term_merges
                    .schedule
                    .borrow_mut()
                    .hold(key, next_pass);
                self.submit_long_term_command(namespace, |partition, number| {
                    partition.register_object_request(&registration, number)
                })
                .await;
                return;
            }
            MergeStep::Start(started) => started,
        };
        self.long_term_merges
            .schedule
            .borrow_mut()
            .hold(key, next_pass);
        let undelivered = Rc::clone(&self.long_term_merges.undelivered);
        // Through the bus, not the runtime directly: the simulator supplies
        // its own executor.
        self.bus.spawn(async move {
            let finished = Box::new(FinishedSegmentMerge {
                namespace,
                created_revision,
                id,
                outcome: merger.merge(merge, scope).await,
            });
            let frame = ShardFrame::lifecycle(LifecycleFrame::SegmentMergeFinished(finished));
            // The partition starts no other merge until it sees this
            // result, so a refused frame must not lose it.
            if let Err(
                crossfire::TrySendError::Full(frame) | crossfire::TrySendError::Disconnected(frame),
            ) = sender.try_send(frame)
                && let ShardFrame::Lifecycle(LifecycleFrame::SegmentMergeFinished(finished)) = frame
            {
                undelivered.borrow_mut().push(*finished);
            }
        });
    }

    /// Hand `finished` to the partition that started it, and commit the
    /// segment it stored. A merge that stored none frees the shard for the
    /// next scan, so a store that refuses is asked again an interval later
    /// and not in a loop.
    #[allow(clippy::future_not_send)]
    async fn settle_segment_merge(&self, finished: FinishedSegmentMerge)
    where
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        let key = (finished.namespace, finished.created_revision);
        let stored = {
            let partition = self
                .plane
                .partitions()
                .get_mut_by_ns(&finished.namespace)
                .filter(|partition| partition.created_revision() == finished.created_revision);
            let Some(partition) = partition else {
                self.long_term_merges.schedule.borrow_mut().release(&key);
                return;
            };
            let stored = partition.finish_segment_merge(finished.id, finished.outcome);
            // A result of an earlier view can arrive while the partition's
            // next merge runs, and the shard stays with that one.
            if stored.is_none() && !partition.segment_merge_running() {
                self.long_term_merges.schedule.borrow_mut().release(&key);
            }
            stored
        };
        if let Some(merged) = stored {
            self.replace_segments(finished.namespace, &merged).await;
        }
    }

    /// Commit the replace of the long-term segments that `merged` holds the
    /// offsets of, the way a client write is committed. Nothing waits for
    /// the reply: a command that is refused or lost leaves the merged
    /// segment stored, and the next pass sends it again.
    #[allow(clippy::future_not_send)]
    async fn replace_segments(&self, namespace: IggyNamespace, merged: &ManifestSegment)
    where
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        let number = self.floor_advance_requests.get().wrapping_add(1);
        self.floor_advance_requests.set(number);
        let request = self
            .plane
            .partitions()
            .get_by_ns(&namespace)
            .and_then(|partition| {
                partition
                    .replace_segments_request(merged, number)
                    .inspect_err(|error| {
                        tracing::error!(
                            shard = self.id,
                            namespace_raw = namespace.inner(),
                            %error,
                            "a merged long-term segment cannot be encoded as a command"
                        );
                    })
                    .ok()
            });
        let Some(request) = request else {
            return;
        };
        let (reply, _unread) = channel(1);
        self.dispatch_partition_submit(request, reply, None, None)
            .await;
    }

    /// Commit the command of the long-term log of `namespace` that `request`
    /// builds under the number it is given, the way a client write is
    /// committed. Nothing waits for the reply: the pass that sent it reads
    /// what came of it from the log, and sends it again if need be.
    #[allow(clippy::future_not_send)]
    pub(crate) async fn submit_long_term_command(
        &self,
        namespace: IggyNamespace,
        request: impl FnOnce(
            &IggyPartition<B, SB>,
            u64,
        ) -> Result<Message<RoutedRequestHeader>, CommandEncodeError>,
    ) where
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        let number = self.floor_advance_requests.get().wrapping_add(1);
        self.floor_advance_requests.set(number);
        let request = self
            .plane
            .partitions()
            .get_by_ns(&namespace)
            .and_then(|partition| {
                request(partition, number)
                    .inspect_err(|error| {
                        tracing::error!(
                            shard = self.id,
                            namespace_raw = namespace.inner(),
                            %error,
                            "a long-term command cannot be encoded"
                        );
                    })
                    .ok()
            });
        let Some(request) = request else {
            return;
        };
        let (reply, _unread) = channel(1);
        self.dispatch_partition_submit(request, reply, None, None)
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(partition: u32) -> RewriteKey {
        (IggyNamespace::new(1, 1, partition as usize), 0)
    }

    const INTERVAL: u64 = 5 * 60 * 1_000_000;

    #[test]
    fn given_a_new_schedule_when_ticking_should_scan_once_an_interval() {
        let mut schedule = MergeSchedule::default();

        assert!(schedule.take_scan(10));
        assert!(!schedule.take_scan(11));
        assert!(!schedule.take_scan(10 + INTERVAL - 1));
        assert!(schedule.take_scan(10 + INTERVAL));
        assert!(!schedule.take_scan(10 + INTERVAL + 1));
    }

    #[test]
    fn given_a_partition_the_shard_merges_for_when_a_scan_is_due_should_not_scan() {
        let mut schedule = MergeSchedule::default();
        schedule.hold(key(0), 500);

        assert!(!schedule.take_scan(INTERVAL));
        assert_eq!(schedule.due_pass(499), None);
        assert_eq!(schedule.due_pass(500), Some(key(0)));

        schedule.release(&key(1));
        assert!(!schedule.take_scan(INTERVAL), "another partition's release");
        schedule.release(&key(0));
        assert_eq!(schedule.due_pass(INTERVAL), None);
        assert!(
            schedule.take_scan(INTERVAL),
            "a scan that was due while the shard merged is due once it stops"
        );
    }

    #[test]
    fn given_merges_of_several_partitions_when_ranking_should_put_a_log_near_its_limit_first() {
        let (first, second) = (key(0).0, key(1).0);

        assert!(merge_rank(true, 1, 9, second) > merge_rank(false, 1000, 0, first));
        assert!(merge_rank(true, 2, 9, second) > merge_rank(true, 1, 0, first));
        assert!(merge_rank(false, 2, 9, second) > merge_rank(false, 1, 0, first));
        assert!(merge_rank(false, 1, 0, second) > merge_rank(false, 1, 9, first));
        assert!(merge_rank(false, 1, 0, first) > merge_rank(false, 1, 0, second));
    }

    #[test]
    fn given_a_pressed_partition_when_the_shard_merges_for_none_should_pass_over_it_once() {
        let mut schedule = MergeSchedule::default();
        assert!(schedule.take_scan(10));
        assert_eq!(schedule.take_pressed(), None);

        schedule.press(key(3));
        schedule.press(key(3));
        schedule.press(key(2));

        assert_eq!(schedule.take_pressed(), Some(key(2)));
        assert_eq!(schedule.take_pressed(), Some(key(3)));
        assert_eq!(schedule.take_pressed(), None, "once for every change");
        assert!(!schedule.take_scan(11), "a pass is not a scan");
    }

    #[test]
    fn given_a_partition_the_shard_merges_for_when_another_is_pressed_should_pass_over_it_after_the_release()
     {
        let mut schedule = MergeSchedule::default();
        schedule.hold(key(0), 500);

        schedule.press(key(1));
        schedule.press(key(0));

        assert_eq!(schedule.take_pressed(), None, "one merge at a time");
        schedule.release(&key(0));
        assert_eq!(schedule.take_pressed(), Some(key(1)));
        assert_eq!(
            schedule.take_pressed(),
            None,
            "the partition the shard merged for got its passes"
        );
    }

    #[test]
    fn given_a_pressed_partition_when_the_shard_starts_to_merge_for_it_should_not_pass_over_it_again()
     {
        let mut schedule = MergeSchedule::default();
        schedule.press(key(0));

        schedule.hold(key(0), 500);
        schedule.release(&key(0));

        assert_eq!(schedule.take_pressed(), None);
    }

    #[test]
    fn given_a_partition_the_shard_merges_for_when_its_command_commits_should_pass_at_once() {
        let mut schedule = MergeSchedule::default();
        schedule.hold(key(0), 500);

        schedule.committed(&key(1), 100);
        assert_eq!(schedule.due_pass(100), None);

        schedule.committed(&key(0), 100);
        assert_eq!(schedule.due_pass(100), Some(key(0)));
    }
}
