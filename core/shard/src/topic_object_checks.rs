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

//! The reads of geo topic objects by the primaries of the topics'
//! partitions, see `partitions::TopicObjects`.
//!
//! A pass over the shard starts one read in a detached task for each
//! partition that waits for its topic's object, as a manifest pass does for
//! a publish. The task holds no partition reference: it sends its result
//! back as a frame, and the pump, the single writer of partition state,
//! settles it.

use crate::shards_table::ShardsTable;
use crate::{FinishedTopicObjectCheck, IggyShard, LifecycleFrame, ShardFrame};
use consensus::PartitionsHandle;
use iggy_common::IggyTimestamp;
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use partitions::TopicObjects;
use std::cell::RefCell;
use std::rc::Rc;

/// What a shard reads geo topic objects with.
#[derive(Default)]
pub struct TopicObjectChecks {
    /// Late-bound jobs on the geo topic objects of this cluster's store.
    /// `None` on a shard without an object store, where no geo topic's
    /// partition takes messages.
    objects: RefCell<Option<Rc<dyn TopicObjects>>>,
    /// Results of reads whose frame the inbox refused. The partition counts
    /// such a read as in flight until it sees the result, so the next pass
    /// takes them from here.
    undelivered: Rc<RefCell<Vec<FinishedTopicObjectCheck>>>,
}

impl<B, MJ, S, M, T, SB> IggyShard<B, MJ, S, M, T, SB>
where
    B: MessageBus + 'static,
    T: ShardsTable,
    SB: SuperblockStore,
{
    /// Give this shard the jobs on the topic objects of its store, or take
    /// them away with `None`. A read already in flight ends on the jobs that
    /// started it.
    pub fn set_topic_objects(&self, topic_objects: Option<Rc<dyn TopicObjects>>) {
        *self.topic_object_checks.objects.borrow_mut() = topic_objects;
    }

    /// The jobs on the topic objects of this shard's store. `None` on a shard
    /// that was given none.
    #[must_use]
    pub fn topic_objects(&self) -> Option<Rc<dyn TopicObjects>> {
        self.topic_object_checks.objects.borrow().clone()
    }

    /// Stage a pass over the partitions of geo topics on this shard's pump.
    /// A refused frame is picked up by the timer's next tick.
    pub fn request_check_topic_objects(&self, now: IggyTimestamp) {
        let Some(sender) = self.senders.get(self.id as usize) else {
            return;
        };
        let _ = sender.try_send(ShardFrame::lifecycle(LifecycleFrame::CheckTopicObjects {
            now,
        }));
    }

    /// Run a pass over the partitions of geo topics on the pump. Does
    /// nothing on a shard without topic objects.
    pub(crate) fn on_check_topic_objects(&self, now: IggyTimestamp) {
        let Some(topic_objects) = self.topic_objects() else {
            return;
        };
        // Every pass settles all of them, whatever partition they are for, so
        // a result is not left waiting for a partition that is gone.
        let undelivered = std::mem::take(&mut *self.topic_object_checks.undelivered.borrow_mut());
        for finished in &undelivered {
            self.on_topic_object_check_finished(finished);
        }

        // Looked up first: a read that cannot report back is not started.
        let Some(sender) = self.senders.get(self.id as usize).cloned() else {
            return;
        };
        let partitions = self.plane.partitions();
        let namespaces: Vec<_> = partitions.namespaces().copied().collect();
        for namespace in namespaces {
            let Some(partition) = partitions.get_mut_by_ns(&namespace) else {
                continue;
            };
            let Some((id, key, scope)) = partition.begin_topic_object_check(now) else {
                continue;
            };
            let created_revision = partition.created_revision();
            let read = topic_objects.read(key, scope);
            let sender = sender.clone();
            let undelivered = Rc::clone(&self.topic_object_checks.undelivered);
            // Through the bus, not the runtime directly: the simulator
            // supplies its own executor.
            self.bus.spawn(async move {
                let finished = Box::new(FinishedTopicObjectCheck {
                    namespace,
                    created_revision,
                    id,
                    outcome: read.await,
                });
                let frame =
                    ShardFrame::lifecycle(LifecycleFrame::TopicObjectCheckFinished(finished));
                // The partition starts no other read until it sees this
                // result, so a refused frame must not lose it.
                if let Err(
                    crossfire::TrySendError::Full(frame)
                    | crossfire::TrySendError::Disconnected(frame),
                ) = sender.try_send(frame)
                    && let ShardFrame::Lifecycle(LifecycleFrame::TopicObjectCheckFinished(
                        finished,
                    )) = frame
                {
                    undelivered.borrow_mut().push(*finished);
                }
            });
        }
    }

    pub(crate) fn on_topic_object_check_finished(&self, finished: &FinishedTopicObjectCheck) {
        if let Some(partition) = self
            .plane
            .partitions()
            .get_mut_by_ns(&finished.namespace)
            .filter(|partition| partition.created_revision() == finished.created_revision)
        {
            partition.finish_topic_object_check(
                finished.id,
                &finished.outcome,
                IggyTimestamp::from(self.bus.realtime_micros()),
            );
        }
    }
}
