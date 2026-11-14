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

//! Deletes of the objects of partition incarnations a committed delete
//! removed.
//!
//! A deleted partition has no primary left to delete what it uploaded, so
//! the metadata primary does it: it is the one node that is sure to see every
//! delete commit. Work comes only from the `Removed` partition changes of
//! committed metadata, which a delete records when it completes. A local
//! teardown queues nothing, since a replica also tears down an incarnation
//! that moved away or that it rebuilds.
//!
//! A `Removed` change does not say whether the topic was tiered, and the
//! topic is gone from metadata by then, so every removed incarnation is
//! queued and the listing of one that never uploaded finds nothing.
//!
//! The queue is memory only and holds what this node saw commit while it was
//! the metadata primary. A restart, a primary change, a change feed that
//! overflowed and a full queue each leave objects in the store, for a sweep
//! of unowned prefixes to find. Nothing is queued on a shard without an
//! object deleter, and a shard holds one only while its node passes the
//! store identity check of [`crate::store_identity`].

use crate::shell::ServerShard;
use consensus::MetadataHandle;
use iggy_common::IggyTimestamp;
use message_bus::MessageBus;
use metadata::impls::metadata::StreamsFrontend;
use metadata::stm::stream::{PartitionChange, PartitionChangeKind};
use partitions::{DeletedIncarnations, ObjectDeleteStart, ObjectDeleter, object_prefix};
use server_common::sharding::IggyNamespace;
use std::cell::RefCell;
use std::rc::Rc;
use tracing::warn;

/// The deleted incarnations of one shard and the task that deletes them.
#[derive(Default)]
pub struct IncarnationDeletes {
    /// Shared with the delete task, which settles each job it ran.
    queue: Rc<RefCell<DeletedIncarnations>>,
}

impl IncarnationDeletes {
    /// Queue the incarnations that `changes` removed, to wait their grace
    /// period from `now`. `changes` must come from committed metadata.
    pub fn observe(&self, shard: &ServerShard, changes: &[PartitionChange], now: IggyTimestamp) {
        if !shard.plane.metadata().is_settled_primary() || shard.object_deleter().is_none() {
            return;
        }
        let mut queue = self.queue.borrow_mut();
        let mut left_out = 0_usize;
        for change in changes {
            if change.kind == PartitionChangeKind::Removed
                && !queue.queue(
                    object_prefix(change.namespace, change.created_revision),
                    now,
                )
            {
                left_out += 1;
            }
        }
        if left_out > 0 {
            warn!(
                shard = shard.id,
                left_out,
                queued = queue.len(),
                "too many deleted partitions wait for the delete of their objects; the objects of the rest stay in the store"
            );
        }
    }

    /// Delete the objects of every incarnation that waited long enough by
    /// `now`, one job at a time in a detached task. A pass while that task
    /// runs starts nothing.
    pub fn pass(&self, shard: &Rc<ServerShard>, now: IggyTimestamp) {
        let Some(first) = next_job(shard, &self.queue, now) else {
            return;
        };
        let queue = Rc::clone(&self.queue);
        let owner = Rc::clone(shard);
        // Through the bus, not the runtime directly: the simulator supplies
        // its own executor.
        shard.bus.spawn(async move {
            let mut next = Some(first);
            while let Some((deleter, (job, scope))) = next {
                let id = job.id;
                let outcome = deleter.run(job, scope).await;
                queue.borrow_mut().finish(id, outcome, now);
                // A job that did not finish waits past `now`, so the task
                // ends once it tried everything that was due.
                next = next_job(&owner, &queue, now);
            }
        });
    }

    /// Forget every incarnation and cancel the job in flight.
    pub fn stop(&self) {
        self.queue.borrow_mut().clear();
    }

    /// How many incarnations wait for their delete.
    #[cfg(test)]
    pub fn queued(&self) -> usize {
        self.queue.borrow().len()
    }

    /// Whether the incarnation `created_revision` of `namespace` waits for
    /// its delete.
    #[cfg(test)]
    pub fn is_queued(&self, namespace: IggyNamespace, created_revision: u64) -> bool {
        self.queue
            .borrow()
            .contains(&object_prefix(namespace, created_revision))
    }
}

/// The next due job and the deleter to run it on. A node that is not the
/// metadata primary drops its queue instead: the primary that took over
/// queues what it sees commit, and two nodes must not both hold the work.
fn next_job(
    shard: &ServerShard,
    queue: &RefCell<DeletedIncarnations>,
    now: IggyTimestamp,
) -> Option<(Rc<dyn ObjectDeleter>, ObjectDeleteStart)> {
    let mut queue = queue.borrow_mut();
    if !shard.plane.metadata().is_settled_primary() {
        queue.clear();
        return None;
    }
    let deleter = shard.object_deleter()?;
    let streams = shard.plane.metadata().mux_stm.streams();
    let start = queue.begin(now, |prefix| {
        // An id that names no namespace cannot be checked, so it counts as
        // live and is never deleted.
        let (Ok(stream_id), Ok(topic_id), Ok(partition_id)) = (
            usize::try_from(prefix.stream_id),
            usize::try_from(prefix.topic_id),
            usize::try_from(prefix.partition_id),
        ) else {
            return true;
        };
        IggyNamespace::try_new(stream_id, topic_id, partition_id).is_none_or(|namespace| {
            streams.created_revision_for_namespace(namespace) == Some(prefix.created_revision)
        })
    })?;
    Some((deleter, start))
}
