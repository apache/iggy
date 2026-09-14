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

//! Capacity belongs to a disk read from dispatch until its result is dequeued.
//! Only a reservation can enqueue a completion, so ordinary work cannot occupy
//! its space and a finished read never waits for space. The pump still decides
//! whether the result may advance progress or produce a successful reply.
//!
//! For capacity N, running reads plus queued results never exceed N. A requester
//! timeout does not cancel detached I/O, so its reservation remains occupied.
//! Dropping the read, dequeuing its result, or discarding it releases the slot.
//!
//! Reservation, delivery, and closure run on the owner's shard. Delivery never
//! yields between checking closure and enqueueing, so the pump cannot close and
//! drain the lane between those steps. The atomic flag alone would not provide
//! that ordering if delivery moved to another thread.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use consensus::client_table::SessionAttachment;
use crossfire::{RecvError, TryRecvError, TrySendError};
use iggy_common::IggyError;
use partitions::PollReadResult;
use server_common::sharding::IggyNamespace;

use super::PollCompleted;
use crate::coordinator::classify_try_send_err;
use crate::metrics::{FrameDropMetrics, ShardMetrics, frame_drop_reason, frame_drop_variant};
use crate::{PartitionReadReply, Receiver, Sender, channel};

/// A private bounded lane, with capacity shared by pending reads and results.
/// It lives on the owner; its raw sender cannot bypass reservation accounting.
pub struct PollCompletionLane {
    sender: Sender<QueuedCompletion>,
    receiver: Receiver<QueuedCompletion>,
    state: Arc<LaneState>,
}

impl PollCompletionLane {
    /// Create a lane with the same limit for reservations and queued results.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` is zero.
    pub(crate) fn new(capacity: usize, metrics: &ShardMetrics) -> Self {
        assert!(capacity > 0, "poll completion capacity must be nonzero");
        let (sender, receiver) = channel(capacity);
        Self {
            sender,
            receiver,
            state: Arc::new(LaneState {
                capacity,
                reserved: AtomicUsize::new(0),
                closed: AtomicBool::new(false),
                metrics: metrics.frame_drop_metrics().clone(),
            }),
        }
    }

    /// Reserve before starting I/O. Exhaustion or shutdown rejects the poll
    /// immediately, without reading data or waiting on the owner's pump.
    pub(crate) fn try_reserve(
        &self,
        namespace: IggyNamespace,
        reply: Sender<PartitionReadReply>,
        attachment: Option<SessionAttachment>,
    ) -> Option<PollCompletionSender> {
        if self.state.closed.load(Ordering::Relaxed) || self.sender.is_disconnected() {
            reject(&reply, &self.state.metrics, frame_drop_reason::DISCONNECTED);
            return None;
        }
        if self
            .state
            .reserved
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |reserved| {
                (reserved < self.state.capacity).then_some(reserved + 1)
            })
            .is_err()
        {
            reject(&reply, &self.state.metrics, frame_drop_reason::FULL);
            return None;
        }
        Some(PollCompletionSender {
            inbox: self.sender.clone(),
            slot: CompletionSlot {
                state: self.state.clone(),
            },
            namespace,
            reply,
            attachment,
        })
    }

    /// Dequeueing frees capacity before owner validation can await replication.
    #[allow(clippy::future_not_send)]
    pub(crate) async fn recv(&self) -> Result<Box<PollCompleted>, RecvError> {
        self.receiver
            .recv()
            .await
            .map(QueuedCompletion::into_result)
    }

    pub(crate) fn try_recv(&self) -> Result<Box<PollCompleted>, TryRecvError> {
        self.receiver.try_recv().map(QueuedCompletion::into_result)
    }

    #[cfg(any(test, feature = "simulator"))]
    pub(crate) fn len(&self) -> usize {
        self.receiver.len()
    }

    /// Stop admission and late delivery on the owning shard while leaving
    /// queued results available for its graceful drain. Closing never accepts
    /// consumer progress.
    pub(crate) fn close(&self) {
        self.state.closed.store(true, Ordering::Relaxed);
    }

    /// Close on cancellation too: the shard can outlive its borrowed pump.
    pub(crate) const fn pump_guard(&self) -> CompletionPumpGuard<'_> {
        CompletionPumpGuard { lane: self }
    }
}

impl Drop for PollCompletionLane {
    fn drop(&mut self) {
        self.close();
        // Crossfire senders may outlive the receiver. Drain explicitly so
        // queued permits and reply senders do not wait for those tasks to end.
        while self.receiver.try_recv().is_ok() {}
    }
}

/// A detached read can reject delivery, but cannot authorize a successful poll.
/// Its slot follows the result into the queue. Caller cancellation retains
/// capacity until the owner dequeues and discards the result. Dropping this
/// sender instead abandons the read and releases its reservation.
pub struct PollCompletionSender {
    inbox: Sender<QueuedCompletion>,
    slot: CompletionSlot,
    namespace: IggyNamespace,
    reply: Sender<PartitionReadReply>,
    attachment: Option<SessionAttachment>,
}

impl PollCompletionSender {
    /// Transfer the reservation into the completion queue without waiting.
    /// Must run on the owning shard, like admission and closure. A closed
    /// owner rejects the result and releases the reservation.
    pub(crate) fn complete(self, result: PollReadResult) {
        if self.slot.state.closed.load(Ordering::Relaxed) || self.inbox.is_disconnected() {
            reject(
                &self.reply,
                &self.slot.state.metrics,
                frame_drop_reason::DISCONNECTED,
            );
            return;
        }
        let completion = QueuedCompletion {
            result: Box::new(PollCompleted {
                namespace: self.namespace,
                result,
                reply: self.reply,
                attachment: self.attachment,
                #[cfg(feature = "poll-diagnostics")]
                queued_at: Some(std::time::Instant::now()),
            }),
            slot: self.slot,
        };
        if let Err(error) = self.inbox.try_send(completion) {
            let reason = classify_try_send_err(&error);
            let completion = match error {
                TrySendError::Full(completion) => {
                    debug_assert!(false, "reserved poll completion slot was unavailable");
                    completion
                }
                TrySendError::Disconnected(completion) => completion,
            };
            reject(
                &completion.result.reply,
                &completion.slot.state.metrics,
                reason,
            );
        }
    }
}

/// Closes admission and abandons any undrained results when the pump exits,
/// including when its future is canceled while the shard remains alive.
pub struct CompletionPumpGuard<'lane> {
    lane: &'lane PollCompletionLane,
}

impl Drop for CompletionPumpGuard<'_> {
    fn drop(&mut self) {
        self.lane.close();
        // Graceful shutdown already delivered its queue. Cancellation or a
        // fatal commit instead abandons results without accepting progress.
        while self.lane.receiver.try_recv().is_ok() {}
    }
}

/// Report a completion route or admission failure without changing progress.
pub(super) fn reject(
    reply: &Sender<PartitionReadReply>,
    metrics: &FrameDropMetrics,
    reason: &'static str,
) {
    metrics.record(frame_drop_variant::PARTITION_POLL_COMPLETION, reason);
    let _ = reply.try_send(PartitionReadReply::Rejected(
        IggyError::TransientNotAccepted,
    ));
}

struct LaneState {
    capacity: usize,
    /// Includes slots held by running reads and by results awaiting dequeue.
    reserved: AtomicUsize,
    /// Closing stops both new reservations and delivery by existing reads.
    closed: AtomicBool,
    /// Share the registered family and cache through the existing slot handle.
    /// Disk reads need no additional metric handle clones at dispatch.
    metrics: FrameDropMetrics,
}

/// Unique ownership of one slot, transferred from read to queued completion.
struct CompletionSlot {
    state: Arc<LaneState>,
}

impl Drop for CompletionSlot {
    fn drop(&mut self) {
        let previous = self.state.reserved.fetch_sub(1, Ordering::Relaxed);
        debug_assert!(previous > 0, "poll completion reservation underflow");
    }
}

struct QueuedCompletion {
    result: Box<PollCompleted>,
    /// Kept until dequeue, even after the disk task has returned.
    slot: CompletionSlot,
}

impl QueuedCompletion {
    fn into_result(self) -> Box<PollCompleted> {
        self.result
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use consensus::{LocalPipeline, VsrConsensus};
    use iggy_common::{IggyByteSize, IggyError, PartitionStats, PollingStrategy};
    use message_bus::IggyMessageBus;
    use partitions::{
        IggyPartition, IggyPartitions, PartitionPathLayout, PartitionsConfig, PollReadResult,
        PollingArgs, PollingConsumer,
    };
    use prometheus_client::encoding::text::encode;
    use prometheus_client::registry::Registry;
    use server_common::sharding::{IggyNamespace, ShardId};

    use super::{PollCompletionLane, PollCompletionSender};
    use crate::metrics::{ShardMetrics, frame_drop_reason, frame_drop_variant};
    use crate::{PartitionReadReply, Receiver, channel};

    #[test]
    fn given_pending_read_when_capacity_is_reserved_should_reject_another_reservation() {
        let metrics = ShardMetrics::for_shard();
        let lane = PollCompletionLane::new(1, &metrics);
        let (pending_read, _pending_replies) = reserve_read(&lane);
        let (reply, rejected_replies) = channel(1);

        assert!(lane.try_reserve(namespace(), reply, None).is_none());
        assert!(matches!(
            rejected_replies.try_recv(),
            Ok(PartitionReadReply::Rejected(
                IggyError::TransientNotAccepted
            ))
        ));
        assert_eq!(metrics.frame_drops_value(), 1);
        assert_eq!(lane.len(), 0, "the first read still owns the empty slot");

        // Canceling the detached read releases capacity without a result.
        drop(pending_read);
        let (_next_read, _next_replies) = reserve_read(&lane);
    }

    #[test]
    fn given_queued_result_when_read_finishes_should_keep_reservation_until_dequeue() {
        let metrics = ShardMetrics::for_shard();
        let lane = PollCompletionLane::new(1, &metrics);
        let (read, replies) = reserve_read(&lane);
        read.complete(read_empty_partition());
        assert_eq!(lane.len(), 1);
        assert!(matches!(
            replies.try_recv(),
            Err(crossfire::TryRecvError::Empty)
        ));

        let (next_reply, _next_replies) = channel(1);
        assert!(lane.try_reserve(namespace(), next_reply, None).is_none());

        // Dequeue frees capacity even while owner validation still holds bytes.
        let _result_for_owner = lane.try_recv().expect("result reaches owner");
        let (_next_read, _next_replies) = reserve_read(&lane);
        assert!(matches!(
            replies.try_recv(),
            Err(crossfire::TryRecvError::Empty)
        ));
    }

    #[test]
    fn given_closed_caller_when_read_is_pending_should_retain_reservation() {
        let metrics = ShardMetrics::for_shard();
        let lane = PollCompletionLane::new(1, &metrics);
        let (pending_read, replies) = reserve_read(&lane);
        drop(replies);
        let (next_reply, _next_replies) = channel(1);
        assert!(lane.try_reserve(namespace(), next_reply, None).is_none());

        pending_read.complete(read_empty_partition());
        assert_eq!(
            lane.len(),
            1,
            "caller cancellation retains capacity until the owner discards the result"
        );
        let _result_for_owner = lane
            .try_recv()
            .expect("owner dequeues the late result before discarding it");
        let (_next_read, _next_replies) = reserve_read(&lane);
    }

    #[test]
    fn given_closed_lane_when_reserved_read_finishes_should_reject_without_enqueue() {
        let metrics = ShardMetrics::for_shard();
        let lane = PollCompletionLane::new(1, &metrics);
        let (pending_read, replies) = reserve_read(&lane);
        lane.close();
        pending_read.complete(read_empty_partition());

        assert!(matches!(
            replies.try_recv(),
            Ok(PartitionReadReply::Rejected(
                IggyError::TransientNotAccepted
            ))
        ));
        assert_eq!(lane.len(), 0);
        assert_eq!(lane.state.reserved.load(Ordering::Relaxed), 0);
        let (next_reply, next_replies) = channel(1);
        assert!(lane.try_reserve(namespace(), next_reply, None).is_none());
        assert!(matches!(
            next_replies.try_recv(),
            Ok(PartitionReadReply::Rejected(
                IggyError::TransientNotAccepted
            ))
        ));
    }

    #[test]
    fn given_dropped_lane_when_read_is_pending_should_release_queued_and_pending_slots() {
        let metrics = ShardMetrics::for_shard();
        let mut registry = Registry::default();
        metrics.register(&mut registry);
        let lane = PollCompletionLane::new(2, &metrics);
        let state = lane.state.clone();
        let (queued_read, queued_replies) = reserve_read(&lane);
        let (pending_read, pending_replies) = reserve_read(&lane);
        queued_read.complete(read_empty_partition());

        let mut scrape = String::new();
        encode(&mut scrape, &registry).expect("metrics are encodable");
        assert!(
            !scrape.contains(frame_drop_variant::PARTITION_POLL_COMPLETION),
            "reserving and delivering reads must not create unused drop series",
        );
        drop(lane);

        assert_eq!(state.reserved.load(Ordering::Relaxed), 1);
        assert!(matches!(
            queued_replies.try_recv(),
            Err(crossfire::TryRecvError::Disconnected)
        ));
        pending_read.complete(read_empty_partition());
        assert!(matches!(
            pending_replies.try_recv(),
            Ok(PartitionReadReply::Rejected(
                IggyError::TransientNotAccepted
            ))
        ));
        assert_eq!(state.reserved.load(Ordering::Relaxed), 0);
        assert_eq!(
            metrics.frame_drop_count(
                frame_drop_variant::PARTITION_POLL_COMPLETION,
                frame_drop_reason::DISCONNECTED,
            ),
            1,
            "the late read must record its drop in the original shard metrics",
        );
        scrape.clear();
        encode(&mut scrape, &registry).expect("metrics are encodable");
        assert!(
            scrape.lines().any(|line| {
                line.starts_with("frame_drops_total{")
                    && line.contains("variant=\"partition_poll_completion\"")
                    && line.contains("reason=\"disconnected\"")
                    && line.ends_with(" 1")
            }),
            "the registered family must observe the late read's drop"
        );
    }

    #[test]
    fn given_canceled_pump_when_completions_exist_should_abandon_without_acceptance() {
        let metrics = ShardMetrics::for_shard();
        let lane = PollCompletionLane::new(2, &metrics);
        let pump = lane.pump_guard();
        let (queued_read, queued_replies) = reserve_read(&lane);
        let (pending_read, pending_replies) = reserve_read(&lane);
        queued_read.complete(read_empty_partition());
        drop(pump);

        assert_eq!(lane.len(), 0);
        assert!(matches!(
            queued_replies.try_recv(),
            Err(crossfire::TryRecvError::Disconnected)
        ));
        pending_read.complete(read_empty_partition());
        assert!(matches!(
            pending_replies.try_recv(),
            Ok(PartitionReadReply::Rejected(
                IggyError::TransientNotAccepted
            ))
        ));
        assert_eq!(lane.state.reserved.load(Ordering::Relaxed), 0);
    }

    fn reserve_read(
        lane: &PollCompletionLane,
    ) -> (PollCompletionSender, Receiver<PartitionReadReply>) {
        let (reply, replies) = channel(1);
        let reservation = lane
            .try_reserve(namespace(), reply, None)
            .expect("scenario has capacity for this read");
        (reservation, replies)
    }

    fn namespace() -> IggyNamespace {
        IggyNamespace::new(1, 1, 0)
    }

    /// Read an empty partition without invoking owner acceptance. These tests
    /// exercise completion delivery, so no messages or disk I/O are needed.
    fn read_empty_partition() -> PollReadResult {
        let partitions = IggyPartitions::<IggyMessageBus>::new(
            ShardId::new(0),
            PartitionsConfig {
                messages_required_to_save: 1,
                size_of_messages_required_to_save: IggyByteSize::from(1024_u64),
                validate_checksum: true,
                segment_size: IggyByteSize::from(1_048_576_u64),
                preallocate_segments: false,
                encryptor: None,
                path_layout: PartitionPathLayout::default(),
            },
        );
        let consensus = VsrConsensus::new(
            1,
            0,
            1,
            namespace().inner(),
            IggyMessageBus::new(0),
            LocalPipeline::new(),
        );
        partitions.insert(
            namespace(),
            IggyPartition::new(Arc::new(PartitionStats::default()), consensus),
        );
        let consumer_id = 7;
        let partition_id = 0;
        partitions
            .build_poll_snapshot(
                &namespace(),
                PollingConsumer::Consumer(consumer_id, partition_id),
                &PollingArgs {
                    strategy: PollingStrategy::next(),
                    count: 0,
                    auto_commit: true,
                },
            )
            .expect("partition has a read snapshot")
            .execute_resident()
    }
}
