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

//! Uploads of sealed segments by the partition primary.
//!
//! A pass over a partition starts one upload in a detached task. The task
//! holds no partition reference: it sends its result back as a frame, and the
//! pump, the single writer of partition state, settles it and commits the
//! floor advance through the partition-submit path every other request takes.
//!
//! A pass comes from the cleaner's timer, from a segment that sealed, or from
//! an upload that stored. Whether it starts an upload is up to the shard's
//! byte budget and the partition's wait after a failure.
//!
//! Only the timed pass carries the truncation watermark metadata holds. A
//! primary whose own retention removed offsets that nothing committed as
//! deleted uploads nothing until a timed pass has had that truncation
//! committed, since its floor advance would declare those offsets stored.

use crate::shards_table::ShardsTable;
use crate::upload_pacing::{LagChange, UPLOAD_LAG_ALARM};
use crate::{
    FinishedSegmentUpload, IggyShard, LifecycleFrame, MetadataSubmit, RestorableMetadataStm,
    ShardFrame, channel,
};
use consensus::PartitionsHandle;
use iggy_binary_protocol::{PrepareHeader, RoutedRequestHeader};
use iggy_common::IggyTimestamp;
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use message_bus::MessageBus;
use partitions::tiered_floor::AdvanceTieredFloor;
use partitions::{IggyPartition, SegmentUploader};
use server_common::Message;
use server_common::sharding::IggyNamespace;
use std::rc::Rc;
use std::time::{Duration, Instant};

impl<B, MJ, S, M, T, SB> IggyShard<B, MJ, S, M, T, SB>
where
    B: MessageBus + 'static,
    T: ShardsTable,
    SB: SuperblockStore,
{
    /// Give this shard the uploader its upload passes run, or take it away
    /// with `None`. An upload already in flight ends on the uploader that
    /// started it.
    pub fn set_segment_uploader(&self, uploader: Option<Rc<dyn SegmentUploader>>) {
        *self.segment_uploader.borrow_mut() = uploader;
    }

    /// The uploader this shard's upload passes run. `None` on a shard that
    /// was given none.
    #[must_use]
    pub fn segment_uploader(&self) -> Option<Rc<dyn SegmentUploader>> {
        self.segment_uploader.borrow().clone()
    }

    /// Stage an upload pass for the partition `namespace` names now on this
    /// shard's pump. A refused frame is picked up by the cleaner's next tick.
    pub fn request_upload_segments(&self, namespace: IggyNamespace) {
        let partitions = self.plane.partitions();
        if let Some(partition) = partitions.get_by_ns(&namespace) {
            self.stage_upload_segments(namespace, partition.created_revision(), true, None);
        }
    }

    /// Stage the timed upload passes of `passes`, the tiered partitions this
    /// shard hosts, each as `(namespace, created_revision, committed_up_to)`:
    /// the incarnation metadata holds and its truncation watermark. Republish
    /// how far their uploads are behind at `now`.
    ///
    /// The partitions whose local eviction waits for an upload are staged
    /// first: the shard's byte budget goes to the passes that run first, and
    /// those partitions hold disk they are configured to give back.
    pub fn request_upload_passes(&self, passes: &[(IggyNamespace, u64, u64)], now: IggyTimestamp) {
        let partitions = self.plane.partitions();
        let namespaces: Vec<_> = passes.iter().map(|(namespace, ..)| *namespace).collect();
        let mut pacing = self.upload_pacing.borrow_mut();
        pacing.retain(&namespaces);
        let mut oldest = Duration::ZERO;
        let mut unhurried = Vec::with_capacity(passes.len());
        for &(namespace, created_revision, committed_up_to) in passes {
            let Some(partition) = partitions.get_by_ns(&namespace) else {
                unhurried.push((namespace, created_revision, committed_up_to));
                continue;
            };
            let lag = partition.upload_lag(now).unwrap_or_default();
            oldest = oldest.max(lag);
            match pacing.observe_lag(namespace, lag) {
                Some(LagChange::Raised) => tracing::warn!(
                    shard = self.id,
                    namespace_raw = namespace.inner(),
                    lag_secs = lag.as_secs(),
                    alarm_secs = UPLOAD_LAG_ALARM.as_secs(),
                    "segment uploads are behind"
                ),
                Some(LagChange::Recovered) => tracing::info!(
                    shard = self.id,
                    namespace_raw = namespace.inner(),
                    lag_secs = lag.as_secs(),
                    "segment uploads caught up"
                ),
                None => {}
            }
            if partition.eviction_awaits_upload(now) {
                self.stage_upload_segments(
                    namespace,
                    created_revision,
                    true,
                    Some(committed_up_to),
                );
            } else {
                unhurried.push((namespace, created_revision, committed_up_to));
            }
        }
        drop(pacing);
        for (namespace, created_revision, committed_up_to) in unhurried {
            self.stage_upload_segments(namespace, created_revision, true, Some(committed_up_to));
        }
        self.metrics.set_segment_upload_lag(oldest);
    }

    /// Stage the pass that a newly uploadable segment of `partition` asked
    /// for, so its upload does not wait for the timer. The pass starts the
    /// oldest segment, which is another one while a backlog stands.
    pub(crate) fn stage_wanted_upload(
        &self,
        namespace: IggyNamespace,
        partition: &IggyPartition<B, SB>,
    ) {
        if partition.take_upload_wanted() && self.segment_uploader.borrow().is_some() {
            self.stage_upload_segments(namespace, partition.created_revision(), false, None);
        }
    }

    fn stage_upload_segments(
        &self,
        namespace: IggyNamespace,
        created_revision: u64,
        resend_advance: bool,
        committed_up_to: Option<u64>,
    ) {
        let Some(sender) = self.senders.get(self.id as usize) else {
            return;
        };
        let _ = sender.try_send(ShardFrame::lifecycle(LifecycleFrame::UploadSegments {
            namespace,
            created_revision,
            resend_advance,
            committed_up_to,
        }));
    }

    /// Run an upload pass over `namespace` on the pump, when its partition is
    /// the incarnation `created_revision` the pass was staged for.
    #[allow(clippy::future_not_send)]
    pub(crate) async fn on_upload_segments(
        &self,
        namespace: IggyNamespace,
        created_revision: u64,
        resend_advance: bool,
        committed_up_to: Option<u64>,
    ) where
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        let now = Instant::now();
        // The watermark belongs to `created_revision`. Noted into any other
        // incarnation it would let a floor advance pass over offsets nothing
        // committed as deleted there. The finished uploads are still settled:
        // they wait for a pass, whatever partition it is for.
        let staged_for_this_incarnation = self
            .plane
            .partitions()
            .get_by_ns(&namespace)
            .is_some_and(|partition| partition.created_revision() == created_revision);
        if !staged_for_this_incarnation {
            self.settle_undelivered_uploads(namespace, now).await;
            return;
        }
        if let Some(committed_up_to) = committed_up_to {
            self.commit_trim_point(namespace, committed_up_to);
        }
        self.upload_segments_at(namespace, resend_advance, now)
            .await;
    }

    /// Hand the partition of `namespace` the truncation watermark metadata
    /// holds, and when it is a primary whose trim point is above both that
    /// and its tiered floor, hand the trim point to shard 0 to commit, the
    /// way a retention decision is. Does nothing on a shard without an
    /// uploader, which has no advance to make honest.
    ///
    /// The same offset is handed over on every timed pass until a pass
    /// comes with a watermark that covers it.
    fn commit_trim_point(&self, namespace: IggyNamespace, committed_up_to: u64) {
        if self.segment_uploader.borrow().is_none() {
            return;
        }
        let Some(partition) = self.plane.partitions().get_mut_by_ns(&namespace) else {
            return;
        };
        partition.note_committed_truncation(committed_up_to);
        if let Some(up_to_offset) = partition.trim_point_to_commit(committed_up_to) {
            self.forward_metadata_submit(MetadataSubmit::RetentionTruncate {
                namespace,
                created_revision: partition.created_revision(),
                up_to_offset,
            });
        }
    }

    /// Settle the uploads that finished and could not report it. Every pass
    /// settles all of them, whatever partition it is for, so a result is not
    /// left waiting for a pass over a deleted partition. Returns whether one
    /// of them sent the floor advance of `namespace`.
    #[allow(clippy::future_not_send)]
    async fn settle_undelivered_uploads(&self, namespace: IggyNamespace, now: Instant) -> bool
    where
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        let undelivered = std::mem::take(&mut *self.undelivered_segment_uploads.borrow_mut());
        let mut advance_sent = false;
        for finished in undelivered {
            let settled = finished.namespace;
            advance_sent |= self.settle_segment_upload(finished, now).await && settled == namespace;
        }
        advance_sent
    }

    /// [`Self::on_upload_segments`] at a given time.
    #[allow(clippy::future_not_send)]
    pub(crate) async fn upload_segments_at(
        &self,
        namespace: IggyNamespace,
        resend_advance: bool,
        now: Instant,
    ) where
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        let Some(uploader) = self.segment_uploader.borrow().clone() else {
            return;
        };
        let advance_sent = self.settle_undelivered_uploads(namespace, now).await;

        let partitions = self.plane.partitions();
        let owed = partitions
            .get_by_ns(&namespace)
            .filter(|_| resend_advance && !advance_sent)
            .and_then(|partition| {
                let advance = partition.pending_floor_advance()?;
                self.floor_advance_request(namespace, partition, &advance)
            });
        if let Some(request) = owed {
            self.submit_floor_advance(request).await;
        }

        // Looked up first: an upload that cannot report back is not started.
        let Some(sender) = self.senders.get(self.id as usize).cloned() else {
            return;
        };
        let Some(partition) = partitions.get_mut_by_ns(&namespace) else {
            return;
        };
        let created_revision = partition.created_revision();
        // A refusal starts nothing and stages nothing: the next pass asks
        // again, so an empty budget or a failing store is not polled.
        let admit = |bytes| {
            self.upload_pacing
                .borrow_mut()
                .admit(namespace, created_revision, bytes, now)
        };
        let Some((upload, scope, attempt)) = partition.begin_segment_upload_if(admit) else {
            return;
        };
        let record = upload.record;
        let undelivered = Rc::clone(&self.undelivered_segment_uploads);
        // Through the bus, not the runtime directly: the simulator supplies
        // its own executor.
        self.bus.spawn(async move {
            let finished = Box::new(FinishedSegmentUpload {
                namespace,
                created_revision,
                attempt,
                record,
                outcome: uploader.upload(upload, scope).await,
            });
            let frame = ShardFrame::lifecycle(LifecycleFrame::SegmentUploadFinished(finished));
            // The partition starts no other upload until it sees this result,
            // so a refused frame must not lose it.
            if let Err(
                crossfire::TrySendError::Full(frame) | crossfire::TrySendError::Disconnected(frame),
            ) = sender.try_send(frame)
                && let ShardFrame::Lifecycle(LifecycleFrame::SegmentUploadFinished(finished)) =
                    frame
            {
                undelivered.borrow_mut().push(*finished);
            }
        });
    }

    #[allow(clippy::future_not_send)]
    pub(crate) async fn on_segment_upload_finished(&self, finished: FinishedSegmentUpload)
    where
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        self.segment_upload_finished_at(finished, Instant::now())
            .await;
    }

    /// [`Self::on_segment_upload_finished`] at a given time.
    #[allow(clippy::future_not_send)]
    pub(crate) async fn segment_upload_finished_at(
        &self,
        finished: FinishedSegmentUpload,
        now: Instant,
    ) where
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        let namespace = finished.namespace;
        let created_revision = finished.created_revision;
        let stored = matches!(finished.outcome, Ok(Ok(())));
        self.settle_segment_upload(finished, now).await;
        // A backlog drains without waiting for the timer. A failure waits for
        // it, so a store that refuses every upload is not asked in a loop.
        if stored {
            self.stage_upload_segments(namespace, created_revision, false, None);
        }
    }

    /// Hand `finished` to the partition that started it, and send the floor
    /// advance it yields. Returns whether an advance was sent.
    ///
    /// The outcome also sets how long the partition waits before its next
    /// attempt, counted from `now`. A cancelled upload says nothing about the
    /// store, so it changes no wait. Neither does the result of an attempt
    /// the partition gave up: the attempt that replaced it is the one whose
    /// outcome counts.
    #[allow(clippy::future_not_send)]
    async fn settle_segment_upload(&self, finished: FinishedSegmentUpload, now: Instant) -> bool
    where
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        let request = self
            .plane
            .partitions()
            .get_mut_by_ns(&finished.namespace)
            .filter(|partition| {
                partition.created_revision() == finished.created_revision
                    && partition.segment_upload_attempt() == Some(finished.attempt)
            })
            .and_then(|partition| {
                let mut pacing = self.upload_pacing.borrow_mut();
                match &finished.outcome {
                    Ok(Ok(())) => pacing.upload_stored(finished.namespace),
                    Ok(Err(failure)) => pacing.upload_failed(
                        finished.namespace,
                        finished.created_revision,
                        failure.is_transient(),
                        now,
                    ),
                    Err(_) => {}
                }
                drop(pacing);
                let advance = partition.finish_segment_upload(
                    finished.attempt,
                    &finished.record,
                    &finished.outcome,
                )?;
                self.floor_advance_request(finished.namespace, partition, &advance)
            });
        let Some(request) = request else {
            return false;
        };
        self.submit_floor_advance(request).await;
        true
    }

    fn floor_advance_request(
        &self,
        namespace: IggyNamespace,
        partition: &IggyPartition<B, SB>,
        advance: &AdvanceTieredFloor,
    ) -> Option<Message<RoutedRequestHeader>> {
        let request = self.floor_advance_requests.get().wrapping_add(1);
        self.floor_advance_requests.set(request);
        partition
            .advance_tiered_floor_request(advance, request)
            .inspect_err(|error| {
                tracing::error!(
                    shard = self.id,
                    namespace_raw = namespace.inner(),
                    up_to = advance.up_to,
                    %error,
                    "tiered floor advance cannot be encoded"
                );
            })
            .ok()
    }

    /// Commit a floor advance the way a client write is committed. Nothing
    /// waits for the reply: an advance that is refused or lost leaves its
    /// segment above the floor, and the next timed pass sends it again.
    #[allow(clippy::future_not_send)]
    async fn submit_floor_advance(&self, request: Message<RoutedRequestHeader>)
    where
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        let (reply, _unread) = channel(1);
        self.dispatch_partition_submit(request, reply, None, None)
            .await;
    }
}
