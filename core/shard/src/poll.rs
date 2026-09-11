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

//! Poll reads complete under the partition owner's control.
//!
//! 1. The owner snapshots the history identity and read resources.
//! 2. Resident reads complete inline. Disk reads run in a detached task and
//!    return their result through the owner's inbox.
//! 3. The owner checks the history and recovery state, admits any automatic
//!    commit, and updates consumer progress before releasing the reply.
//!
//! A disk read can yield while purge or state transfer replaces the history,
//! even on the same shard thread. The detached task therefore cannot advance
//! progress or authorize a successful reply. Completion validation and progress
//! updates run synchronously on the owner, before any replication wait.

use crate::shards_table::ShardsTable;
use crate::{IggyShard, PartitionRead, PartitionReadReply, Sender};
use consensus::PartitionsHandle;
use iggy_common::IggyError;
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use partitions::{PollCompletion, PollPlan, PollReadResult};
use server_common::sharding::IggyNamespace;

/// A read result awaiting acceptance by its partition owner.
/// Disk tasks send it through the inbox. Resident reads pass it directly to
/// the same completion handler.
pub struct PollCompleted {
    /// Namespace whose current partition must validate the captured history.
    namespace: IggyNamespace,
    /// Snapshot facts that have not yet been accepted as consumer progress.
    result: PollReadResult,
    /// Return path for the accepted read or a rejection.
    reply: Sender<PartitionReadReply>,
    /// Inbox enqueue time for disk diagnostics, or `None` for resident completion.
    #[cfg(feature = "poll-diagnostics")]
    queued_at: Option<std::time::Instant>,
}

impl<B, MJ, S, M, T, SB> IggyShard<B, MJ, S, M, T, SB>
where
    B: MessageBus + 'static,
    T: ShardsTable,
    SB: SuperblockStore,
{
    /// Execute a routed read on the partition owner's pump.
    /// Partitions missing materialized data reject the read. Resident polls finish
    /// inline. Disk polls return to the pump for acceptance after detached I/O.
    #[allow(clippy::future_not_send)]
    pub(crate) async fn on_partition_read(
        &self,
        namespace: IggyNamespace,
        read: PartitionRead,
        reply: Sender<PartitionReadReply>,
    ) {
        let partitions = self.plane.partitions();
        if partitions.with_partition(
            &namespace,
            partitions::IggyPartition::requires_state_transfer,
        ) == Some(true)
        {
            let _ = reply.try_send(PartitionReadReply::Rejected(
                IggyError::TransientNotAccepted,
            ));
            return;
        }
        let result = match read {
            PartitionRead::Poll { consumer, args } => {
                match partitions.build_poll_snapshot(&namespace, consumer, &args) {
                    None => PartitionReadReply::NotFound,
                    Some(plan) if plan.needs_off_pump_io() => {
                        #[cfg(feature = "poll-diagnostics")]
                        tracing::debug!(
                            target: "iggy.shard.poll_diagnostics",
                            namespace_raw = namespace.inner(),
                            phase = "dispatch",
                            tier = "disk",
                            "partition poll dispatch"
                        );
                        let completion = completion::PollCompletionSender::new(
                            self.senders.get(usize::from(self.id)).cloned(),
                            namespace,
                            reply,
                            self.metrics.clone(),
                        );
                        self.bus.spawn(read_poll(namespace, plan, completion));
                        return;
                    }
                    Some(plan) => {
                        #[cfg(feature = "poll-diagnostics")]
                        tracing::debug!(
                            target: "iggy.shard.poll_diagnostics",
                            namespace_raw = namespace.inner(),
                            phase = "dispatch",
                            tier = "resident",
                            "partition poll dispatch"
                        );
                        self.on_poll_completed(PollCompleted {
                            namespace,
                            result: plan.execute_resident(),
                            reply,
                            #[cfg(feature = "poll-diagnostics")]
                            queued_at: None,
                        })
                        .await;
                        return;
                    }
                }
            }
            PartitionRead::ConsumerOffset { consumer } => partitions
                .consumer_offset_read(&namespace, consumer)
                .map_or(PartitionReadReply::NotFound, |(stored, current_offset)| {
                    PartitionReadReply::ConsumerOffset {
                        stored,
                        current_offset,
                    }
                }),
            PartitionRead::GroupOffsetState { group_id } => partitions
                .group_offset_state(&namespace, group_id)
                .map_or(PartitionReadReply::NotFound, |(last_polled, committed)| {
                    PartitionReadReply::GroupOffsetState {
                        last_polled,
                        committed,
                    }
                }),
            PartitionRead::ClearGroupLastPolled { group_id } => partitions
                .clear_group_last_polled(&namespace, group_id)
                .map_or(PartitionReadReply::NotFound, |()| PartitionReadReply::Ack),
            PartitionRead::ResolveSegmentDeleteOffset { count } => partitions
                .segment_delete_resolution(&namespace, count)
                .map_or(PartitionReadReply::NotFound, |(up_to_offset, lagging)| {
                    PartitionReadReply::SegmentDeleteOffset {
                        up_to_offset,
                        lagging,
                    }
                }),
        };
        let _ = reply.try_send(result);
    }

    /// Accept a read on the owner's pump, then attempt the reply before replication.
    /// Replication may suspend. A closed reply channel does not cancel an
    /// automatic commit that the owner has already admitted.
    #[allow(clippy::future_not_send)]
    pub(crate) async fn on_poll_completed(&self, completion: PollCompleted) {
        let PollCompleted {
            namespace,
            result,
            reply,
            #[cfg(feature = "poll-diagnostics")]
            queued_at,
        } = completion;
        #[cfg(feature = "poll-diagnostics")]
        if let Some(queued_at) = queued_at {
            tracing::debug!(
                target: "iggy.shard.poll_diagnostics",
                namespace_raw = namespace.inner(),
                phase = "completion",
                tier = "disk",
                queue_wait_us = u64::try_from(queued_at.elapsed().as_micros()).unwrap_or(u64::MAX),
                "partition poll completion"
            );
        }
        let partitions = self.plane.partitions();
        let consumer_kind = result.consumer_kind();
        match partitions.complete_poll(&namespace, result) {
            Ok(PollCompletion {
                fragments,
                current_offset,
                replication,
            }) => {
                // The owner has admitted progress, but the offset may still be
                // queued. Release the reply before waiting for replication.
                // A poll reply does not acknowledge a durable offset commit.
                let _ = reply.try_send(PartitionReadReply::Poll {
                    fragments,
                    current_offset,
                });
                if let Some(replication) = replication {
                    partitions
                        .replicate_poll_completion(&namespace, replication)
                        .await;
                }
            }
            Err(error) => {
                if matches!(error, IggyError::TooManyConsumerOffsets) {
                    self.metrics.record_consumer_offset_denied(consumer_kind);
                }
                let _ = reply.try_send(PartitionReadReply::Rejected(error));
            }
        }
    }
}

/// Read an owned snapshot without borrowing the partition or changing progress.
/// The completion sender returns the result to the owner for acceptance.
#[allow(clippy::future_not_send)]
async fn read_poll(
    namespace: IggyNamespace,
    plan: PollPlan,
    completion: completion::PollCompletionSender,
) {
    let poll_started = std::time::Instant::now();
    let result = plan.execute().await;
    let elapsed = poll_started.elapsed();
    if elapsed > std::time::Duration::from_secs(1) {
        tracing::warn!(
            namespace_raw = namespace.inner(),
            elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
            "slow partition poll; gather side may have timed out"
        );
    }
    completion.complete(result);
}

mod completion {
    use super::PollCompleted;
    use crate::coordinator::classify_try_send_err;
    use crate::metrics::{ShardMetrics, frame_drop_reason, frame_drop_variant};
    use crate::{LifecycleFrame, PartitionReadReply, Sender, ShardFrame, TaggedSender};
    use iggy_common::IggyError;
    use partitions::PollReadResult;
    use server_common::sharding::IggyNamespace;

    /// The read task can return bytes to the owner or reject a lost completion.
    /// The reply sender stays private so it cannot authorize successful reads.
    pub(super) struct PollCompletionSender {
        /// Originating owner's inbox, whose absence causes a rejection.
        inbox: Option<TaggedSender>,
        namespace: IggyNamespace,
        /// May be used here only to reject a result that cannot reach its owner.
        reply: Sender<PartitionReadReply>,
        metrics: ShardMetrics,
    }

    impl PollCompletionSender {
        /// Bind the result and reply route to the shard that built the snapshot.
        pub(super) const fn new(
            inbox: Option<TaggedSender>,
            namespace: IggyNamespace,
            reply: Sender<PartitionReadReply>,
            metrics: ShardMetrics,
        ) -> Self {
            Self {
                inbox,
                namespace,
                reply,
                metrics,
            }
        }

        /// Return the result to the owner without waiting for inbox capacity.
        /// A missing, full, or disconnected inbox records a dropped completion
        /// and attempts a `TransientNotAccepted` reply. Successful enqueueing
        /// leaves the reply to the owner.
        pub(super) fn complete(self, result: PollReadResult) {
            let Some(inbox) = self.inbox else {
                self.metrics.record_frame_drop(
                    frame_drop_variant::PARTITION_POLL_COMPLETION,
                    frame_drop_reason::UNROUTABLE,
                );
                let _ = self.reply.try_send(PartitionReadReply::Rejected(
                    IggyError::TransientNotAccepted,
                ));
                return;
            };
            let frame =
                ShardFrame::lifecycle(LifecycleFrame::PollCompleted(Box::new(PollCompleted {
                    namespace: self.namespace,
                    result,
                    reply: self.reply,
                    #[cfg(feature = "poll-diagnostics")]
                    queued_at: Some(std::time::Instant::now()),
                })));
            if let Err(error) = inbox.try_send(frame) {
                self.metrics.record_frame_drop(
                    frame_drop_variant::PARTITION_POLL_COMPLETION,
                    classify_try_send_err(&error),
                );
                let frame = match error {
                    crossfire::TrySendError::Full(frame)
                    | crossfire::TrySendError::Disconnected(frame) => frame,
                };
                if let ShardFrame::Lifecycle(LifecycleFrame::PollCompleted(completion)) = frame {
                    let _ = completion.reply.try_send(PartitionReadReply::Rejected(
                        IggyError::TransientNotAccepted,
                    ));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::completion::PollCompletionSender;
    use crate::metrics::ShardMetrics;
    use crate::{LifecycleFrame, PartitionReadReply, ShardFrame, channel, shard_channel};
    use consensus::{LocalPipeline, VsrConsensus};
    use iggy_common::{IggyByteSize, IggyError, PartitionStats, PollingStrategy};
    use message_bus::IggyMessageBus;
    use partitions::{
        IggyPartition, IggyPartitions, PartitionPathLayout, PartitionsConfig, PollReadResult,
        PollingArgs, PollingConsumer,
    };
    use server_common::sharding::{IggyNamespace, ShardId};
    use std::sync::Arc;

    #[test]
    fn given_full_owner_inbox_when_poll_completes_should_reject_without_displacing_work() {
        let (sender, receiver, _reply_inbox) = shard_channel(0, 1, 1);
        assert!(
            sender
                .try_send(ShardFrame::lifecycle(LifecycleFrame::ReconcileApply))
                .is_ok()
        );
        let (reply, replies) = channel(1);
        let metrics = ShardMetrics::for_shard();
        PollCompletionSender::new(Some(sender), namespace(), reply, metrics.clone())
            .complete(empty_poll());

        assert!(matches!(
            replies.try_recv(),
            Ok(PartitionReadReply::Rejected(
                IggyError::TransientNotAccepted
            ))
        ));
        assert!(matches!(
            receiver.try_recv(),
            Ok(ShardFrame::Lifecycle(LifecycleFrame::ReconcileApply))
        ));
        assert_eq!(metrics.frame_drops_value(), 1);
    }

    #[test]
    fn given_closed_owner_inbox_when_poll_completes_should_reject() {
        let (sender, receiver, _reply_inbox) = shard_channel(0, 1, 1);
        drop(receiver);
        let (reply, replies) = channel(1);
        let metrics = ShardMetrics::for_shard();
        PollCompletionSender::new(Some(sender), namespace(), reply, metrics.clone())
            .complete(empty_poll());

        assert!(matches!(
            replies.try_recv(),
            Ok(PartitionReadReply::Rejected(
                IggyError::TransientNotAccepted
            ))
        ));
        assert_eq!(metrics.frame_drops_value(), 1);
    }

    #[test]
    fn given_available_owner_inbox_when_poll_completes_should_leave_reply_to_owner() {
        let (sender, receiver, _reply_inbox) = shard_channel(0, 1, 1);
        let (reply, replies) = channel(1);
        PollCompletionSender::new(Some(sender), namespace(), reply, ShardMetrics::for_shard())
            .complete(empty_poll());

        assert!(replies.try_recv().is_err());
        assert!(matches!(
            receiver.try_recv(),
            Ok(ShardFrame::Lifecycle(LifecycleFrame::PollCompleted(_)))
        ));
    }

    fn namespace() -> IggyNamespace {
        IggyNamespace::new(1, 1, 0)
    }

    fn empty_poll() -> PollReadResult {
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
        partitions
            .build_poll_snapshot(
                &namespace(),
                PollingConsumer::Consumer(7, 0),
                &PollingArgs::new(PollingStrategy::next(), 0, true),
            )
            .expect("partition has a read snapshot")
            .execute_resident()
    }
}
