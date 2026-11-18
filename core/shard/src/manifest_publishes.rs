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

//! Publishes of the manifests of tiered partitions by their primary.
//!
//! A pass over a partition starts one publish in a detached task, as a
//! delete pass does. The task holds no partition reference: it sends its
//! result back as a frame, and the pump, the single writer of partition
//! state, settles it.

use crate::shards_table::ShardsTable;
use crate::{FinishedManifestPublish, IggyShard, LifecycleFrame, ShardFrame};
use consensus::PartitionsHandle;
use iggy_common::IggyTimestamp;
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use partitions::{IggyPartition, ManifestPublisher};
use server_common::sharding::IggyNamespace;
use std::rc::Rc;

impl<B, MJ, S, M, T, SB> IggyShard<B, MJ, S, M, T, SB>
where
    B: MessageBus,
    SB: SuperblockStore,
{
    /// Stage a manifest pass for `partition` when this replica leads it in a
    /// normal view that had no pass yet, it places its batches, and this
    /// shard has a publisher. Nothing else asks for the manifest of a new
    /// primary that has no segment left to commit.
    ///
    /// The shard tick calls this for every partition it visited.
    pub(crate) fn stage_manifest_of_new_primary(
        &self,
        namespace: IggyNamespace,
        partition: &IggyPartition<B, SB>,
    ) {
        // A pass over a tombstoned partition starts no publish, so it would
        // be staged on every tick.
        if !partition.manifest_awaits_first_pass()
            || self.manifest_publisher.borrow().is_none()
            || self.plane.partitions().is_tombstoned(&namespace)
        {
            return;
        }
        let Some(sender) = self.senders.get(self.id as usize) else {
            return;
        };
        // A refused frame is staged again by the next tick.
        let _ = sender.try_send(ShardFrame::lifecycle(LifecycleFrame::PublishManifest {
            namespace,
            created_revision: partition.created_revision(),
            now: IggyTimestamp::now(),
        }));
    }
}

impl<B, MJ, S, M, T, SB> IggyShard<B, MJ, S, M, T, SB>
where
    B: MessageBus + 'static,
    T: ShardsTable,
    SB: SuperblockStore,
{
    /// Give this shard the publisher its manifest passes run, or take it
    /// away with `None`. A publish already in flight ends on the publisher
    /// that started it.
    pub fn set_manifest_publisher(&self, publisher: Option<Rc<dyn ManifestPublisher>>) {
        *self.manifest_publisher.borrow_mut() = publisher;
    }

    /// The publisher this shard's manifest passes run. `None` on a shard that
    /// was given none.
    #[must_use]
    pub fn manifest_publisher(&self) -> Option<Rc<dyn ManifestPublisher>> {
        self.manifest_publisher.borrow().clone()
    }

    /// Stage a manifest pass for `namespace` on this shard's pump, for the
    /// incarnation `created_revision`. A refused frame is picked up by the
    /// timer's next tick.
    pub fn request_publish_manifest(
        &self,
        namespace: IggyNamespace,
        created_revision: u64,
        now: IggyTimestamp,
    ) {
        let Some(sender) = self.senders.get(self.id as usize) else {
            return;
        };
        let _ = sender.try_send(ShardFrame::lifecycle(LifecycleFrame::PublishManifest {
            namespace,
            created_revision,
            now,
        }));
    }

    /// Run a manifest pass over `namespace` on the pump. Does nothing on a
    /// shard without a publisher.
    pub(crate) fn on_publish_manifest(
        &self,
        namespace: IggyNamespace,
        created_revision: u64,
        now: IggyTimestamp,
    ) {
        let Some(publisher) = self.manifest_publisher.borrow().clone() else {
            return;
        };
        // Every pass settles all of them, whatever partition it is for, so a
        // result is not left waiting for a pass over a deleted partition.
        let undelivered = std::mem::take(&mut *self.undelivered_manifest_publishes.borrow_mut());
        for finished in &undelivered {
            self.on_manifest_publish_finished(finished);
        }

        // Looked up first: a publish that cannot report back is not started.
        let Some(sender) = self.senders.get(self.id as usize).cloned() else {
            return;
        };
        let Some(partition) = self.plane.partitions().get_mut_by_ns(&namespace) else {
            return;
        };
        if partition.created_revision() != created_revision {
            return;
        }
        let Some((id, manifest, scope)) = partition.begin_manifest_publish(now) else {
            return;
        };
        let undelivered = Rc::clone(&self.undelivered_manifest_publishes);
        // Through the bus, not the runtime directly: the simulator supplies
        // its own executor.
        self.bus.spawn(async move {
            let finished = Box::new(FinishedManifestPublish {
                namespace,
                created_revision,
                id,
                outcome: publisher.publish(manifest, scope).await,
            });
            let frame = ShardFrame::lifecycle(LifecycleFrame::ManifestPublishFinished(finished));
            // The partition starts no other publish until it sees this
            // result, so a refused frame must not lose it.
            if let Err(
                crossfire::TrySendError::Full(frame) | crossfire::TrySendError::Disconnected(frame),
            ) = sender.try_send(frame)
                && let ShardFrame::Lifecycle(LifecycleFrame::ManifestPublishFinished(finished)) =
                    frame
            {
                undelivered.borrow_mut().push(*finished);
            }
        });
    }

    pub(crate) fn on_manifest_publish_finished(&self, finished: &FinishedManifestPublish) {
        if let Some(partition) = self
            .plane
            .partitions()
            .get_mut_by_ns(&finished.namespace)
            .filter(|partition| partition.created_revision() == finished.created_revision)
        {
            let now = IggyTimestamp::now();
            if partition.finish_manifest_publish(finished.id, &finished.outcome, now) {
                self.request_publish_manifest(finished.namespace, finished.created_revision, now);
            }
        }
    }
}
