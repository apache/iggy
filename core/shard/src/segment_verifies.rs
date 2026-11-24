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

//! The verify passes over the segments tiered partitions stored, run by
//! their primary, see `partitions::SegmentVerifies`.
//!
//! A pass over a partition starts one job in a detached task, as a delete
//! pass does. The task holds no partition reference: it sends its result back
//! as a frame, and the pump, the single writer of partition state, settles
//! it. The findings reach the shard's gauges through each partition's
//! `partitions::GaugeShare`.

use crate::shards_table::ShardsTable;
use crate::{FinishedSegmentVerify, IggyShard, LifecycleFrame, ShardFrame};
use consensus::PartitionsHandle;
use iggy_common::IggyTimestamp;
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use partitions::{SegmentVerifier, VerifyBegin};
use server_common::sharding::IggyNamespace;
use std::rc::Rc;

impl<B, MJ, S, M, T, SB> IggyShard<B, MJ, S, M, T, SB>
where
    B: MessageBus + 'static,
    T: ShardsTable,
    SB: SuperblockStore,
{
    /// Give this shard the verifier its verify passes run, or take it away
    /// with `None`. A job already in flight ends on the verifier that
    /// started it.
    pub fn set_segment_verifier(&self, verifier: Option<Rc<dyn SegmentVerifier>>) {
        *self.segment_verifier.borrow_mut() = verifier;
    }

    /// The verifier this shard's verify passes run. `None` on a shard that
    /// was given none.
    #[must_use]
    pub fn segment_verifier(&self) -> Option<Rc<dyn SegmentVerifier>> {
        self.segment_verifier.borrow().clone()
    }

    /// Stage a verify pass for `namespace` on this shard's pump, for the
    /// incarnation `created_revision`. A refused frame is picked up by the
    /// cleaner's next tick.
    pub fn request_verify_segments(
        &self,
        namespace: IggyNamespace,
        created_revision: u64,
        now: IggyTimestamp,
    ) {
        let Some(sender) = self.senders.get(self.id as usize) else {
            return;
        };
        let _ = sender.try_send(ShardFrame::lifecycle(LifecycleFrame::VerifySegments {
            namespace,
            created_revision,
            now,
        }));
    }

    /// Run a verify pass over `namespace` on the pump. Does nothing on a
    /// shard without a verifier.
    pub(crate) fn on_verify_segments(
        &self,
        namespace: IggyNamespace,
        created_revision: u64,
        now: IggyTimestamp,
    ) {
        let Some(verifier) = self.segment_verifier.borrow().clone() else {
            return;
        };
        // Every pass settles all of them, whatever partition it is for, so a
        // result is not left waiting for a pass over a deleted partition.
        let undelivered = std::mem::take(&mut *self.undelivered_segment_verifies.borrow_mut());
        for finished in &undelivered {
            self.on_segment_verify_finished(finished);
        }

        // Looked up first: a job that cannot report back is not started.
        let Some(sender) = self.senders.get(self.id as usize).cloned() else {
            return;
        };
        let Some(partition) = self.plane.partitions().get_mut_by_ns(&namespace) else {
            return;
        };
        if partition.created_revision() != created_revision {
            return;
        }
        let (id, job, scope) = match partition.begin_segment_verify(now) {
            VerifyBegin::Idle => return,
            VerifyBegin::PassEnded => {
                self.metrics.record_segment_verify_pass();
                return;
            }
            VerifyBegin::Job { id, job, scope } => (id, job, scope),
        };
        let undelivered = Rc::clone(&self.undelivered_segment_verifies);
        // Through the bus, not the runtime directly: the simulator supplies
        // its own executor.
        self.bus.spawn(async move {
            let finished = Box::new(FinishedSegmentVerify {
                namespace,
                created_revision,
                id,
                outcome: verifier.verify(job, scope).await,
            });
            let frame = ShardFrame::lifecycle(LifecycleFrame::SegmentVerifyFinished(finished));
            // The partition starts no other job until it sees this result,
            // so a refused frame must not lose it.
            if let Err(
                crossfire::TrySendError::Full(frame) | crossfire::TrySendError::Disconnected(frame),
            ) = sender.try_send(frame)
                && let ShardFrame::Lifecycle(LifecycleFrame::SegmentVerifyFinished(finished)) =
                    frame
            {
                undelivered.borrow_mut().push(*finished);
            }
        });
    }

    pub(crate) fn on_segment_verify_finished(&self, finished: &FinishedSegmentVerify) {
        if let Some(partition) = self
            .plane
            .partitions()
            .get_mut_by_ns(&finished.namespace)
            .filter(|partition| partition.created_revision() == finished.created_revision)
            && let Some(checked) = partition.finish_segment_verify(finished.id, &finished.outcome)
        {
            self.metrics.record_stored_segments_verified(checked);
        }
    }
}
