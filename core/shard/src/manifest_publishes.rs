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
//!
//! A partition that places its batches stores its manifest at most once a
//! [`partitions::MANIFEST_STORE_INTERVAL`]. A pass that the interval holds
//! back leaves the partition among the held ones, and the pump's tick stages
//! its pass once the interval passed, so the last change before a quiet
//! spell is stored too.

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
    /// Stage a manifest pass for `partition` when this replica leads it, it
    /// places its batches, this shard has a publisher, and the pass is one
    /// that nothing else asks for: the first of a normal view, or the one
    /// [`IggyPartition::manifest_pass_due_at`] owes by `now`. A new primary
    /// with no segment left to commit, and a publish that failed on an idle
    /// partition, get no pass otherwise.
    ///
    /// The shard tick calls this for every partition it visited, and the
    /// pump's tick for every partition it paces, awake or not.
    pub(crate) fn stage_manifest_pass(
        &self,
        namespace: IggyNamespace,
        partition: &IggyPartition<B, SB>,
        now: IggyTimestamp,
    ) {
        let due = partition.manifest_awaits_first_pass()
            || partition
                .manifest_pass_due_at()
                .is_some_and(|due_at| now.as_micros() >= due_at.as_micros());
        // A pass over a tombstoned partition starts no publish, so it would
        // be staged on every tick.
        if !due
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
            now,
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
            if partition.manifest_store_held(now) {
                self.manifests_held
                    .borrow_mut()
                    .insert((namespace, created_revision));
            }
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

    /// The pump's tick: stage the pass of each held partition whose
    /// interval passed by `now`, and forget the ones that are gone.
    pub(crate) fn stage_held_manifest_passes(&self, now: IggyTimestamp) {
        if self.manifests_held.borrow().is_empty() {
            return;
        }
        let partitions = self.plane.partitions();
        let held: Vec<_> = self.manifests_held.borrow().iter().copied().collect();
        for (namespace, created_revision) in held {
            let partition = partitions
                .get_by_ns(&namespace)
                .filter(|partition| partition.created_revision() == created_revision);
            if let Some(partition) = partition {
                if partition.manifest_store_held(now) {
                    continue;
                }
                self.stage_manifest_pass(namespace, partition, now);
            }
            self.manifests_held
                .borrow_mut()
                .remove(&(namespace, created_revision));
        }
    }

    pub(crate) fn on_manifest_publish_finished(&self, finished: &FinishedManifestPublish) {
        if let Some(partition) = self
            .plane
            .partitions()
            .get_mut_by_ns(&finished.namespace)
            .filter(|partition| partition.created_revision() == finished.created_revision)
        {
            let now = IggyTimestamp::from(self.bus.realtime_micros());
            if partition.finish_manifest_publish(finished.id, &finished.outcome, now) {
                self.request_publish_manifest(finished.namespace, finished.created_revision, now);
            }
        }
    }
}
