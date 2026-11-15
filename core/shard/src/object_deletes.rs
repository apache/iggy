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

//! Deletes of the objects of removed segments by the partition primary.
//!
//! A pass over a partition starts one job in a detached task, as an upload
//! pass does. The task holds no partition reference: it sends its result back
//! as a frame, and the pump, the single writer of partition state, settles
//! it.
//!
//! The objects of an incarnation a committed delete removed have no
//! partition left to delete them. Their owner takes the shard's deleter with
//! [`IggyShard::object_deleter`] and runs those jobs itself.

use crate::shards_table::ShardsTable;
use crate::{FinishedObjectDelete, IggyShard, LifecycleFrame, ShardFrame};
use consensus::PartitionsHandle;
use iggy_common::IggyTimestamp;
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use partitions::ObjectDeleter;
use server_common::sharding::IggyNamespace;
use std::rc::Rc;

impl<B, MJ, S, M, T, SB> IggyShard<B, MJ, S, M, T, SB>
where
    B: MessageBus + 'static,
    T: ShardsTable,
    SB: SuperblockStore,
{
    /// Give this shard the deleter its delete passes run, or take it away
    /// with `None`. A job already in flight ends on the deleter that started
    /// it.
    pub fn set_object_deleter(&self, deleter: Option<Rc<dyn ObjectDeleter>>) {
        *self.object_deleter.borrow_mut() = deleter;
    }

    /// The deleter this shard's delete passes run, for the jobs that belong
    /// to no partition of the shard. `None` on a shard that was given none.
    #[must_use]
    pub fn object_deleter(&self) -> Option<Rc<dyn ObjectDeleter>> {
        self.object_deleter.borrow().clone()
    }

    /// Stage a delete pass for `namespace` on this shard's pump.
    /// `committed_up_to` is the truncation watermark metadata holds for the
    /// incarnation `created_revision`. A refused frame is picked up by the
    /// cleaner's next tick.
    pub fn request_delete_objects(
        &self,
        namespace: IggyNamespace,
        created_revision: u64,
        now: IggyTimestamp,
        committed_up_to: u64,
    ) {
        let Some(sender) = self.senders.get(self.id as usize) else {
            return;
        };
        let _ = sender.try_send(ShardFrame::lifecycle(LifecycleFrame::DeleteObjects {
            namespace,
            created_revision,
            now,
            committed_up_to,
        }));
    }

    pub(crate) fn on_delete_objects(
        &self,
        namespace: IggyNamespace,
        created_revision: u64,
        now: IggyTimestamp,
        committed_up_to: u64,
    ) {
        let Some(deleter) = self.object_deleter.borrow().clone() else {
            return;
        };
        // Every pass settles all of them, whatever partition it is for, so a
        // result is not left waiting for a pass over a deleted partition.
        let undelivered = std::mem::take(&mut *self.undelivered_object_deletes.borrow_mut());
        for finished in undelivered {
            self.on_object_delete_finished(finished);
        }

        // Looked up first: a job that cannot report back is not started.
        let Some(sender) = self.senders.get(self.id as usize).cloned() else {
            return;
        };
        let Some(partition) = self.plane.partitions().get_mut_by_ns(&namespace) else {
            return;
        };
        // The watermark belongs to `created_revision`. Handed to any other
        // incarnation it would let a sweep delete objects below an offset
        // nothing committed as deleted there.
        if partition.created_revision() != created_revision {
            return;
        }
        let Some((job, scope)) = partition.begin_object_delete(now, committed_up_to) else {
            return;
        };
        let id = job.id;
        let undelivered = Rc::clone(&self.undelivered_object_deletes);
        // Through the bus, not the runtime directly: the simulator supplies
        // its own executor.
        self.bus.spawn(async move {
            let finished = Box::new(FinishedObjectDelete {
                namespace,
                created_revision,
                id,
                outcome: deleter.run(job, scope).await,
            });
            let frame = ShardFrame::lifecycle(LifecycleFrame::ObjectDeleteFinished(finished));
            // The partition starts no other job until it sees this result,
            // so a refused frame must not lose it.
            if let Err(
                crossfire::TrySendError::Full(frame) | crossfire::TrySendError::Disconnected(frame),
            ) = sender.try_send(frame)
                && let ShardFrame::Lifecycle(LifecycleFrame::ObjectDeleteFinished(finished)) = frame
            {
                undelivered.borrow_mut().push(*finished);
            }
        });
    }

    /// Hand `finished` to the partition that started it. The segments a
    /// listing queues wait their grace period from the moment it is settled.
    pub(crate) fn on_object_delete_finished(&self, finished: FinishedObjectDelete) {
        if let Some(partition) = self
            .plane
            .partitions()
            .get_mut_by_ns(&finished.namespace)
            .filter(|partition| partition.created_revision() == finished.created_revision)
        {
            partition.finish_object_delete(finished.id, finished.outcome, IggyTimestamp::now());
        }
    }
}
