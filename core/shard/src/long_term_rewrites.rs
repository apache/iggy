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
//! A segment is stored before its command commits. A pass that finds such
//! a segment sends its command again and starts no rewrite, so a command
//! that was refused or lost costs a pass and nothing else.

use crate::shards_table::ShardsTable;
use crate::{
    FinishedSegmentRewrite, IggyShard, LifecycleFrame, RestorableMetadataStm, ShardFrame, channel,
};
use consensus::PartitionsHandle;
use iggy_binary_protocol::PrepareHeader;
use iggy_object_store::ManifestSegment;
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use message_bus::MessageBus;
use partitions::LongTermWriter;
use server_common::Message;
use server_common::sharding::IggyNamespace;
use std::rc::Rc;

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
        *self.long_term_writer.borrow_mut() = writer;
    }

    /// The writer this shard's rewrite passes run. `None` on a shard that
    /// was given none.
    #[must_use]
    pub fn long_term_writer(&self) -> Option<Rc<dyn LongTermWriter>> {
        self.long_term_writer.borrow().clone()
    }

    /// Stage a rewrite pass for `namespace` on this shard's pump, for the
    /// incarnation `created_revision`. A refused frame is picked up by the
    /// timer's next tick.
    pub fn request_rewrite_segments(&self, namespace: IggyNamespace, created_revision: u64) {
        let Some(sender) = self.senders.get(self.id as usize) else {
            return;
        };
        let _ = sender.try_send(ShardFrame::lifecycle(LifecycleFrame::RewriteSegments {
            namespace,
            created_revision,
        }));
    }

    /// Run a rewrite pass over `namespace` on the pump. Does nothing on a
    /// shard without a writer.
    #[allow(clippy::future_not_send)]
    pub(crate) async fn on_rewrite_segments(&self, namespace: IggyNamespace, created_revision: u64)
    where
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        let Some(writer) = self.long_term_writer.borrow().clone() else {
            return;
        };
        // Every pass settles all of them, whatever partition it is for, so a
        // result is not left waiting for a pass over a deleted partition.
        let undelivered = std::mem::take(&mut *self.undelivered_segment_rewrites.borrow_mut());
        for finished in undelivered {
            self.settle_segment_rewrite(finished).await;
        }

        // Looked up first: a rewrite that cannot report back is not started.
        let Some(sender) = self.senders.get(self.id as usize).cloned() else {
            return;
        };
        let started = {
            // The rewrite reads the messages of the batches from the store.
            let Some(partition) = self.plane.partitions().get_mut_with_remote(&namespace) else {
                return;
            };
            if partition.created_revision() != created_revision {
                return;
            }
            partition
                .unpublished_segment()
                .map_or_else(|| Ok(partition.begin_segment_rewrite()), Err)
        };
        let (id, rewrite, scope) = match started {
            Err(stored) => {
                self.publish_segment(namespace, &stored).await;
                return;
            }
            Ok(None) => return,
            Ok(Some(started)) => started,
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

    #[allow(clippy::future_not_send)]
    pub(crate) async fn on_segment_rewrite_finished(&self, finished: FinishedSegmentRewrite)
    where
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        let (namespace, created_revision) = (finished.namespace, finished.created_revision);
        if self.settle_segment_rewrite(finished).await {
            // A backlog drains without waiting for the timer. A failure
            // waits for it, so a store that refuses is not asked in a loop.
            self.request_rewrite_segments(namespace, created_revision);
        }
    }

    /// Hand `finished` to the partition that started it, and commit the
    /// segment it stored. Returns whether it stored one.
    #[allow(clippy::future_not_send)]
    async fn settle_segment_rewrite(&self, finished: FinishedSegmentRewrite) -> bool
    where
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        let stored = self
            .plane
            .partitions()
            .get_mut_by_ns(&finished.namespace)
            .filter(|partition| partition.created_revision() == finished.created_revision)
            .and_then(|partition| partition.finish_segment_rewrite(finished.id, finished.outcome));
        let Some(stored) = stored else {
            return false;
        };
        self.publish_segment(finished.namespace, &stored).await;
        true
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
