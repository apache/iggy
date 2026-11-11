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

use crate::shards_table::ShardsTable;
use crate::{
    FinishedSegmentUpload, IggyShard, LifecycleFrame, RestorableMetadataStm, ShardFrame, channel,
};
use consensus::PartitionsHandle;
use iggy_binary_protocol::{PrepareHeader, RoutedRequestHeader};
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use message_bus::MessageBus;
use partitions::tiered_floor::AdvanceTieredFloor;
use partitions::{IggyPartition, SegmentUploader};
use server_common::Message;
use server_common::sharding::IggyNamespace;
use std::rc::Rc;

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

    /// Stage an upload pass for `namespace` on this shard's pump. A refused
    /// frame is picked up by the cleaner's next tick.
    pub fn request_upload_segments(&self, namespace: IggyNamespace) {
        self.stage_upload_segments(namespace, true);
    }

    fn stage_upload_segments(&self, namespace: IggyNamespace, resend_advance: bool) {
        let Some(sender) = self.senders.get(self.id as usize) else {
            return;
        };
        let _ = sender.try_send(ShardFrame::lifecycle(LifecycleFrame::UploadSegments {
            namespace,
            resend_advance,
        }));
    }

    #[allow(clippy::future_not_send)]
    pub(crate) async fn on_upload_segments(&self, namespace: IggyNamespace, resend_advance: bool)
    where
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        let Some(uploader) = self.segment_uploader.borrow().clone() else {
            return;
        };
        // Every pass settles all of them, whatever partition it is for, so a
        // result is not left waiting for a pass over a deleted partition.
        let undelivered = std::mem::take(&mut *self.undelivered_segment_uploads.borrow_mut());
        let mut advance_sent = false;
        for finished in undelivered {
            let settled = finished.namespace;
            advance_sent |= self.settle_segment_upload(finished).await && settled == namespace;
        }

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
        let Some((upload, scope)) = partition.begin_segment_upload() else {
            return;
        };
        let created_revision = partition.created_revision();
        let record = upload.record;
        let undelivered = Rc::clone(&self.undelivered_segment_uploads);
        // Through the bus, not the runtime directly: the simulator supplies
        // its own executor.
        self.bus.spawn(async move {
            let finished = Box::new(FinishedSegmentUpload {
                namespace,
                created_revision,
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
        let namespace = finished.namespace;
        let stored = matches!(finished.outcome, Ok(Ok(())));
        self.settle_segment_upload(finished).await;
        // A backlog drains without waiting for the timer. A failure waits for
        // it, so a store that refuses every upload is not asked in a loop.
        if stored {
            self.stage_upload_segments(namespace, false);
        }
    }

    /// Hand `finished` to the partition that started it, and send the floor
    /// advance it yields. Returns whether an advance was sent.
    #[allow(clippy::future_not_send)]
    async fn settle_segment_upload(&self, finished: FinishedSegmentUpload) -> bool
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
            .filter(|partition| partition.created_revision() == finished.created_revision)
            .and_then(|partition| {
                let advance =
                    partition.finish_segment_upload(&finished.record, &finished.outcome)?;
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
