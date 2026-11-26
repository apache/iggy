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

//! Rewrites of placed batches into long-term segments by the partition
//! primary.
//!
//! A pass over a partition starts one rewrite in a detached task, as an
//! upload pass does. The task holds no partition reference: it sends its
//! result back as a frame, and the pump settles it and commits the stored
//! segment to the partition's long-term log through the partition-submit
//! path every other request takes.
//!
//! The shard gives a partition its wait when the pump's tick finds this
//! replica leading it with committed messages to rewrite, so nothing outside
//! the shard has to ask for the first pass. The segment cleaner stages one
//! too, which does nothing for a partition that has a wait. The pump's tick
//! starts each pass when the partition's wait is over, and
//! the wait follows how full the stored segments come out. A segment that
//! ended at its byte cap has more behind it, and the pass for that starts
//! when the segment's command commits.
//!
//! A commit to the long-term log also stages a publish of the partition's
//! manifest, so a reader of the bucket finds the segment without waiting
//! for the segment cleaner. The commits within one
//! [`partitions::MANIFEST_STORE_INTERVAL`] share one publish.
//!
//! A segment is stored before its command commits. A pass that finds such
//! a segment sends its command again and starts no rewrite, so a command
//! that was refused or lost costs a pass and nothing else.
//!
//! A segment is registered before it is stored. A pass without a plan
//! starts one in a detached task, which reads the headers of the batches to
//! find where the segment ends. The tick hands that to the partition and
//! commits the registration, and the pass that its commit makes due starts
//! the rewrite.
//!
//! A pass over a partition whose backlog has no count yet, as after a boot
//! or an install, also starts a walk of its log that counts it, in a
//! detached task whose count the tick hands back. The pass of a partition
//! past its backlog limit does not wait out its interval.

use crate::metrics::LongTermLag;
use crate::rewrite_pacing::{BacklogCount, FinishedPlan, RewriteKey, RewriteResult};
use crate::shards_table::ShardsTable;
use crate::{
    FinishedSegmentRewrite, IggyShard, LifecycleFrame, RestorableMetadataStm, ShardFrame, channel,
};
use consensus::PartitionsHandle;
use iggy_binary_protocol::PrepareHeader;
use iggy_common::IggyTimestamp;
use iggy_object_store::ManifestSegment;
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use message_bus::MessageBus;
use partitions::{IggyPartition, LONG_TERM_SEGMENT_BYTES, LongTermWriter, RewriteStart};
use server_common::Message;
use server_common::sharding::IggyNamespace;
use std::rc::Rc;
use std::time::Duration;

impl<B, MJ, S, M, T, SB> IggyShard<B, MJ, S, M, T, SB>
where
    B: MessageBus,
    SB: SuperblockStore,
{
    /// Give `partition` a wait, so the pump's tick runs its rewrite passes,
    /// when this replica leads it in a normal view, it places its batches,
    /// messages it committed wait for a rewrite, and this shard has a
    /// writer. A partition that has a wait keeps it.
    ///
    /// The shard tick calls this for every partition it visited.
    pub(crate) fn pace_placing_primary(
        &self,
        namespace: IggyNamespace,
        partition: &IggyPartition<B, SB>,
    ) {
        if partition.long_term_lag() == 0
            || !partition.state_machines_current()
            || self.long_term_writer.borrow().is_none()
        {
            return;
        }
        let key = (namespace, partition.created_revision());
        let mut pacing = self.rewrite_pacing.borrow_mut();
        // A pass over a tombstoned partition drops its wait again.
        if pacing.paces(&key) || self.plane.partitions().is_tombstoned(&namespace) {
            return;
        }
        pacing.enter(key, self.bus.monotonic_micros());
    }
}

impl<B, MJ, S, M, T, SB> IggyShard<B, MJ, S, M, T, SB>
where
    B: MessageBus + 'static,
    T: ShardsTable,
    SB: SuperblockStore,
{
    /// Give this shard the writer its rewrite passes run, or take it away
    /// with `None`. A rewrite already in flight ends on the writer that
    /// started it.
    pub fn set_long_term_writer(&self, writer: Option<Rc<dyn LongTermWriter>>) {
        // A partition that slept through the time without a writer has no
        // wait, and no tick visits it.
        if writer.is_some() {
            self.rewrite_pacing.borrow_mut().want_sweep();
        }
        *self.long_term_writer.borrow_mut() = writer;
    }

    /// The writer this shard's rewrite passes run. `None` on a shard that
    /// was given none.
    #[must_use]
    pub fn long_term_writer(&self) -> Option<Rc<dyn LongTermWriter>> {
        self.long_term_writer.borrow().clone()
    }

    /// Set the backlog past which the sends to a partition this shard leads
    /// are delayed, in bytes of messages it committed that no long-term
    /// segment holds. Zero turns the delay off.
    pub fn set_long_term_backlog_limit(&self, bytes: u64) {
        self.rewrite_pacing.borrow_mut().set_backlog_limit(bytes);
    }

    /// Set the longest wait between two rewrites of a partition this shard
    /// leads, which a partition with little load waits. A wait below
    /// [`crate::rewrite_pacing::REWRITE_INTERVAL_MIN`] counts as it.
    pub fn set_long_term_rewrite_max_interval(&self, interval: Duration) {
        self.rewrite_pacing.borrow_mut().set_max_interval(interval);
    }

    /// Stage the first rewrite pass for `namespace` on this shard's pump,
    /// for the incarnation `created_revision`. The pump's tick starts the
    /// passes that follow, so this does nothing for a partition that had
    /// one. A refused frame is picked up by the timer's next tick.
    pub fn request_rewrite_segments(&self, namespace: IggyNamespace, created_revision: u64) {
        let Some(sender) = self.senders.get(self.id as usize) else {
            return;
        };
        let _ = sender.try_send(ShardFrame::lifecycle(LifecycleFrame::RewriteSegments {
            namespace,
            created_revision,
        }));
    }

    /// The pass the segment cleaner staged: the first one over `namespace`.
    /// A partition that had a pass starts the next one from the tick.
    #[allow(clippy::future_not_send)]
    pub(crate) async fn on_rewrite_segments(&self, namespace: IggyNamespace, created_revision: u64)
    where
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        if self
            .rewrite_pacing
            .borrow()
            .paces(&(namespace, created_revision))
        {
            return;
        }
        self.run_rewrite_pass(namespace, created_revision).await;
    }

    /// Act on a command that changed the long-term log of `partition`, if
    /// one did since the last call: stage a publish of its manifest, and
    /// make the rewrite that waited for the command due.
    pub(crate) fn stage_long_term_change(
        &self,
        namespace: IggyNamespace,
        partition: &IggyPartition<B, SB>,
    ) {
        if partition.take_long_term_changed() {
            self.long_term_changed(
                namespace,
                partition.created_revision(),
                partition.merges_under_pressure(),
            );
        }
        if partition.take_long_term_registered() {
            self.long_term_registered(&(namespace, partition.created_revision()));
        }
    }

    fn long_term_changed(&self, namespace: IggyNamespace, created_revision: u64, pressed: bool) {
        self.rewrite_pacing
            .borrow_mut()
            .committed(&(namespace, created_revision), self.bus.monotonic_micros());
        self.long_term_merge_committed(&(namespace, created_revision), pressed);
        if self.manifest_publisher.borrow().is_some() {
            self.request_publish_manifest(
                namespace,
                created_revision,
                IggyTimestamp::from(self.bus.realtime_micros()),
            );
        }
    }

    /// A registration committed to the long-term log of `key`, which leaves
    /// its manifest as it is. The rewrite or the merge that waited for it
    /// is due.
    fn long_term_registered(&self, key: &RewriteKey) {
        self.rewrite_pacing
            .borrow_mut()
            .committed(key, self.bus.monotonic_micros());
        self.long_term_merge_committed(key, false);
    }

    /// The pump's tick: settle the rewrites whose frame the inbox refused,
    /// look over every partition once after the shard got a writer, hand
    /// the partitions the backlogs their walks counted, act on the
    /// commands that committed outside a partition I/O step, stage the
    /// manifest passes that no commit asks for, make the pass of a partition
    /// past its backlog limit due, start the passes that are due, and report
    /// how far the rewrites are behind, per partition and at worst, the
    /// largest backlog, how long the longest long-term log is, and how many
    /// objects no long-term log accounts for or wait for their delete.
    #[allow(clippy::future_not_send)]
    pub(crate) async fn tick_long_term_rewrites(&self)
    where
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        let undelivered = std::mem::take(&mut *self.undelivered_segment_rewrites.borrow_mut());
        for finished in undelivered {
            self.settle_segment_rewrite(finished).await;
        }
        let plans = self.rewrite_pacing.borrow().take_plans();
        for plan in plans {
            self.settle_segment_plan(plan).await;
        }
        let manifest_now = IggyTimestamp::from(self.bus.realtime_micros());
        self.stage_held_manifest_passes(manifest_now);
        let partitions = self.plane.partitions();
        if self.rewrite_pacing.borrow_mut().take_sweep_wanted() {
            for namespace in partitions.namespaces() {
                if let Some(partition) = partitions.get_by_ns(namespace) {
                    self.pace_placing_primary(*namespace, partition);
                    self.stage_manifest_pass(*namespace, partition, manifest_now);
                }
            }
        }
        let counts = self.rewrite_pacing.borrow().take_backlog_counts();
        for BacklogCount { key, walk, bytes } in counts {
            if let Some(partition) = partitions
                .get_mut_by_ns(&key.0)
                .filter(|partition| partition.created_revision() == key.1)
            {
                partition.finish_backlog_walk(walk, bytes);
            }
        }
        let limit = self.rewrite_pacing.borrow().backlog_limit();
        let mut unannounced_objects = 0;
        let mut dead_objects = 0;
        let mut most_segments = 0;
        let mut changed = Vec::new();
        let mut registered = Vec::new();
        let mut past_limit = Vec::new();
        let mut largest_backlog = 0;
        let mut lags = self.metrics.long_term_lags_to_fill();
        for key in self.rewrite_pacing.borrow().paced() {
            let (namespace, created_revision) = key;
            let Some(partition) = partitions
                .get_by_ns(namespace)
                .filter(|partition| partition.created_revision() == *created_revision)
            else {
                continue;
            };
            unannounced_objects += partition.long_term_unannounced_objects();
            dead_objects += partition.queued_dead_long_term_objects();
            most_segments = most_segments.max(partition.long_term_segments().len());
            let long_term_changed = partition.take_long_term_changed();
            if long_term_changed {
                changed.push((*key, partition.merges_under_pressure()));
            }
            if partition.take_long_term_registered() {
                registered.push(*key);
            }
            if !partition.consensus().is_primary() {
                continue;
            }
            // A change stages its own pass below.
            if !long_term_changed {
                self.stage_manifest_pass(key.0, partition, manifest_now);
            }
            lags.push(LongTermLag {
                namespace: key.0,
                messages: partition.long_term_lag(),
                bytes: partition.long_term_backlog(),
            });
            let backlog = partition.long_term_backlog().unwrap_or(0);
            largest_backlog = largest_backlog.max(backlog);
            if limit > 0 && backlog > limit {
                past_limit.push(*key);
            }
        }
        self.metrics.set_long_term_lags(lags);
        self.metrics
            .set_long_term_unannounced_objects(unannounced_objects);
        self.metrics.set_long_term_dead_objects(dead_objects);
        self.metrics.set_long_term_segments(most_segments);
        for ((namespace, created_revision), pressed) in changed {
            self.long_term_changed(namespace, created_revision, pressed);
        }
        for key in registered {
            self.long_term_registered(&key);
        }
        let now = self.bus.monotonic_micros();
        let due = {
            let mut pacing = self.rewrite_pacing.borrow_mut();
            for key in &past_limit {
                pacing.hurry(key, now);
            }
            pacing.take_due(now)
        };
        for (namespace, created_revision) in due {
            self.run_rewrite_pass(namespace, created_revision).await;
        }
        self.metrics
            .set_long_term_rewrite_lag(self.rewrite_pacing.borrow().worst_lag());
        self.metrics.set_long_term_backlog(largest_backlog);
    }

    /// Run a rewrite pass over `namespace` on the pump, and set when its
    /// next one is due. A shard without a writer, and a partition that is
    /// gone, drop out of the passes the tick starts.
    #[allow(clippy::future_not_send)]
    async fn run_rewrite_pass(&self, namespace: IggyNamespace, created_revision: u64)
    where
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        let key = (namespace, created_revision);
        let Some(writer) = self.long_term_writer.borrow().clone() else {
            self.rewrite_pacing.borrow_mut().forget(&key);
            return;
        };
        // Looked up first: a rewrite that cannot report back is not started.
        let Some(sender) = self.senders.get(self.id as usize).cloned() else {
            return;
        };
        let now = self.bus.monotonic_micros();
        // Read at each pass, so a changed ceiling applies from the next one.
        let ceiling = self.rewrite_ceiling(namespace);
        let (started, walk) = {
            // The rewrite reads the messages of the batches from the store.
            let partition = self
                .plane
                .partitions()
                .get_mut_with_remote(&namespace)
                .filter(|partition| partition.created_revision() == created_revision);
            let Some(partition) = partition else {
                self.rewrite_pacing.borrow_mut().forget(&key);
                return;
            };
            // On a backup too, so that a new primary has the count.
            let walk = partition.begin_backlog_walk();
            let lag = partition.long_term_lag();
            let started = partition
                .unpublished_segment()
                .map_or_else(|| Ok(partition.begin_segment_rewrite()), Err);
            let mut pacing = self.rewrite_pacing.borrow_mut();
            pacing.set_ceiling(key, ceiling);
            match &started {
                Ok(Some(RewriteStart::Register(_))) => {
                    pacing.set_lag(key, lag);
                    pacing.settle(key, RewriteResult::Registering, now);
                }
                Ok(Some(_)) => pacing.started(key, lag),
                // Its result sets when the next pass is due.
                Ok(None) if partition.segment_rewrite_running() => pacing.started(key, lag),
                Ok(None) => {
                    pacing.settle(key, RewriteResult::Idle, now);
                    // A full long-term log starts no rewrite and still lags.
                    pacing.set_lag(key, lag);
                }
                Err(_) => {
                    pacing.set_lag(key, lag);
                    pacing.settle(key, RewriteResult::Resent, now);
                }
            }
            (started, walk)
        };
        if let Some((walk_id, walk)) = walk {
            let counts = self.rewrite_pacing.borrow().backlog_counts();
            // Off the pump: the walk reads the partition's segment files.
            self.bus.spawn(async move {
                let bytes = walk.run().await;
                counts.borrow_mut().push(BacklogCount {
                    key,
                    walk: walk_id,
                    bytes,
                });
            });
        }
        let (id, rewrite, scope) = match started {
            Err(stored) => {
                self.publish_segment(namespace, &stored).await;
                return;
            }
            Ok(None) => return,
            Ok(Some(RewriteStart::Register(registration))) => {
                self.submit_long_term_command(namespace, |partition, number| {
                    partition.register_object_request(&registration, number)
                })
                .await;
                return;
            }
            Ok(Some(RewriteStart::Plan(id, plan))) => {
                let plans = self.rewrite_pacing.borrow().plans();
                // Off the pump: the plan reads the partition's segment files.
                self.bus.spawn(async move {
                    let outcome = plan.run().await;
                    plans.borrow_mut().push(FinishedPlan { key, id, outcome });
                });
                return;
            }
            Ok(Some(RewriteStart::Rewrite(id, rewrite, scope))) => (id, rewrite, scope),
        };
        let undelivered = Rc::clone(&self.undelivered_segment_rewrites);
        // Through the bus, not the runtime directly: the simulator supplies
        // its own executor.
        self.bus.spawn(async move {
            let finished = Box::new(FinishedSegmentRewrite {
                namespace,
                created_revision,
                id,
                outcome: writer.write(rewrite, scope).await,
            });
            let frame = ShardFrame::lifecycle(LifecycleFrame::SegmentRewriteFinished(finished));
            // The partition starts no other rewrite until it sees this
            // result, so a refused frame must not lose it.
            if let Err(
                crossfire::TrySendError::Full(frame) | crossfire::TrySendError::Disconnected(frame),
            ) = sender.try_send(frame)
                && let ShardFrame::Lifecycle(LifecycleFrame::SegmentRewriteFinished(finished)) =
                    frame
            {
                undelivered.borrow_mut().push(*finished);
            }
        });
    }

    /// The longest wait between two rewrites that the topic of `namespace`
    /// sets for itself, `None` to follow the shard's.
    fn rewrite_ceiling(&self, namespace: IggyNamespace) -> Option<Duration>
    where
        M: RestorableMetadataStm,
    {
        self.geo_durations(namespace)
            .long_term_rewrite_max_interval
            .map(|ceiling| ceiling.get_duration())
    }

    #[allow(clippy::future_not_send)]
    pub(crate) async fn on_segment_rewrite_finished(&self, finished: FinishedSegmentRewrite)
    where
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        self.settle_segment_rewrite(finished).await;
    }

    /// Hand `finished` to the partition that started it, commit the segment
    /// it stored, and set when the partition's next pass is due. A segment
    /// that ended before the commit point has a backlog behind it, whose
    /// pass starts when the segment's command commits. A failure waits, so
    /// a store that refuses is not asked in a loop.
    #[allow(clippy::future_not_send)]
    async fn settle_segment_rewrite(&self, finished: FinishedSegmentRewrite)
    where
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        let key = (finished.namespace, finished.created_revision);
        let Some(partition) = self
            .plane
            .partitions()
            .get_mut_by_ns(&finished.namespace)
            .filter(|partition| partition.created_revision() == finished.created_revision)
        else {
            self.rewrite_pacing.borrow_mut().forget(&key);
            return;
        };
        let stored = partition.finish_segment_rewrite(finished.id, finished.outcome);
        let result = stored.map_or(RewriteResult::Unchanged, |stored| RewriteResult::Stored {
            bytes: stored.size,
            cap: LONG_TERM_SEGMENT_BYTES as u64,
            reached_commit: partition.stored_segment_reached_commit(),
        });
        self.rewrite_pacing
            .borrow_mut()
            .settle(key, result, self.bus.monotonic_micros());
        if let Some(stored) = stored {
            self.publish_segment(finished.namespace, &stored).await;
        }
    }

    /// Hand `plan` to the partition that started it, and commit the
    /// registration of the segment it found. The pass after the
    /// registration commits starts the rewrite. A plan that failed waits,
    /// as a failed rewrite does.
    #[allow(clippy::future_not_send)]
    async fn settle_segment_plan(&self, plan: FinishedPlan)
    where
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        let (namespace, created_revision) = plan.key;
        let Some(partition) = self
            .plane
            .partitions()
            .get_mut_by_ns(&namespace)
            .filter(|partition| partition.created_revision() == created_revision)
        else {
            self.rewrite_pacing.borrow_mut().forget(&plan.key);
            return;
        };
        let registration = partition.finish_segment_plan(
            plan.id,
            plan.outcome,
            IggyTimestamp::from(self.bus.realtime_micros()),
        );
        let result = if registration.is_some() {
            RewriteResult::Registering
        } else {
            RewriteResult::Unchanged
        };
        self.rewrite_pacing
            .borrow_mut()
            .settle(plan.key, result, self.bus.monotonic_micros());
        if let Some(registration) = registration {
            self.submit_long_term_command(namespace, |partition, number| {
                partition.register_object_request(&registration, number)
            })
            .await;
        }
    }

    /// Commit `segment` to the long-term log of `namespace` the way a client
    /// write is committed. Nothing waits for the reply: a command that is
    /// refused or lost leaves the segment unpublished, and the next pass
    /// sends it again.
    #[allow(clippy::future_not_send)]
    async fn publish_segment(&self, namespace: IggyNamespace, segment: &ManifestSegment)
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
                    .publish_segment_request(segment, number)
                    .inspect_err(|error| {
                        tracing::error!(
                            shard = self.id,
                            namespace_raw = namespace.inner(),
                            %error,
                            "a long-term segment cannot be encoded as a command"
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
