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
//! 2. Resident reads complete inline. Disk reads reserve completion capacity
//!    before detached I/O and return through the owner's completion lane.
//! 3. The owner checks the reply connection, history, and recovery state, admits
//!    any automatic commit, and updates progress before releasing the reply.
//!
//! A disk read can yield while purge or state transfer replaces the history,
//! even on the same shard thread. The detached task therefore cannot advance
//! progress or authorize a successful reply. Completion validation and progress
//! updates run synchronously on the owner, before any replication wait.

use crate::shards_table::ShardsTable;
use crate::{IggyShard, PartitionRead, PartitionReadReply, Sender};
use consensus::client_table::SessionAttachment;
use consensus::{Consensus, MetadataHandle, PartitionsHandle};
use iggy_common::IggyError;
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use metadata::impls::metadata::StreamsFrontend;
use metadata::stm::stream::PollMetadata;
use partitions::{PollCompletion, PollPlan, PollReadResult};
use server_common::sharding::IggyNamespace;
use std::sync::Arc;

pub mod completion;
#[cfg(test)]
mod completion_tests;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod timeout_tests;

/// Session lifetime and relevant metadata must both survive detached poll I/O.
#[derive(Debug)]
pub struct PollAttachment {
    pub session: Arc<SessionAttachment>,
    pub metadata: PollMetadata,
}

/// A read result awaiting acceptance by its partition owner.
/// Disk tasks send it through the reserved completion lane. Resident reads
/// pass it directly to the same completion handler.
///
/// The channel requires `Send` even for messages addressed to this same shard.
/// Completion capacity is released on dequeue. Consumer offset capacity is
/// reserved separately during owner acceptance; those guards stay in the
/// owner's local request queue or replication state, outside this channel.
pub struct PollCompleted {
    /// Namespace whose current partition must validate the captured history.
    namespace: IggyNamespace,
    /// Snapshot facts that have not yet been accepted as consumer progress.
    result: PollReadResult,
    /// Return path for the accepted read or a rejection.
    reply: Sender<PartitionReadReply>,
    attachment: Option<PollAttachment>,
    /// Inbox enqueue time for disk diagnostics, or `None` for resident completion.
    #[cfg(feature = "poll-diagnostics")]
    queued_at: Option<std::time::Instant>,
}

impl<B, MJ, S, M, T, SB> IggyShard<B, MJ, S, M, T, SB>
where
    B: MessageBus + 'static,
    T: ShardsTable,
    M: StreamsFrontend,
    SB: SuperblockStore,
{
    /// Execute a routed read on the partition owner's pump.
    /// Partitions missing materialized data reject the read. Resident polls finish
    /// inline. Disk polls return to the pump for acceptance after detached I/O.
    #[allow(clippy::future_not_send, clippy::too_many_lines)]
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
        if matches!(read, PartitionRead::PollOnPrimary { .. })
            && partitions.with_partition(&namespace, |partition| {
                let consensus = partition.consensus();
                consensus.is_primary() && consensus.is_normal() && !consensus.is_transferring()
            }) != Some(true)
        {
            let _ = reply.try_send(PartitionReadReply::Rejected(
                IggyError::TransientNotAccepted,
            ));
            return;
        }
        let (read, attachment) = match read {
            PartitionRead::PollOnPrimary {
                consumer,
                args,
                attachment,
            } => (PartitionRead::Poll { consumer, args }, Some(attachment)),
            read => (read, None),
        };
        if attachment.as_ref().is_some_and(|attachment| {
            partitions.with_partition(&namespace, |partition| {
                attachment.metadata.matches_partition(
                    self.shards_table.epoch_for(namespace),
                    partition.applied_purge_generation(),
                )
            }) != Some(true)
        }) {
            let _ = reply.try_send(PartitionReadReply::Rejected(
                IggyError::TransientNotAccepted,
            ));
            return;
        }
        let result = match read {
            PartitionRead::Primary => partitions
                .with_partition(&namespace, |partition| {
                    let consensus = partition.consensus();
                    if consensus.is_normal()
                        && !consensus.is_transferring()
                        && !(consensus.has_ceded_primaryship()
                            && consensus.primary_index(consensus.view()) == consensus.replica())
                    {
                        PartitionReadReply::Primary(consensus.primary_index(consensus.view()))
                    } else {
                        PartitionReadReply::Rejected(IggyError::TransientNotAccepted)
                    }
                })
                .unwrap_or(PartitionReadReply::NotFound),
            PartitionRead::Poll { consumer, args }
            | PartitionRead::PollOnPrimary { consumer, args, .. } => {
                match partitions.build_poll_snapshot(&namespace, consumer, &args) {
                    None => PartitionReadReply::NotFound,
                    Some(plan) if plan.needs_off_pump_io() => {
                        let route_failure = self.senders.get(usize::from(self.id)).map_or(
                            Some(crate::metrics::frame_drop_reason::UNROUTABLE),
                            |sender| {
                                sender
                                    .is_disconnected()
                                    .then_some(crate::metrics::frame_drop_reason::DISCONNECTED)
                            },
                        );
                        if let Some(reason) = route_failure {
                            completion::reject(&reply, self.metrics.frame_drop_metrics(), reason);
                            return;
                        }
                        let Some(completion) = self
                            .poll_completions
                            .try_reserve(namespace, reply, attachment)
                        else {
                            return;
                        };
                        #[cfg(feature = "poll-diagnostics")]
                        tracing::debug!(
                            target: "iggy.shard.poll_diagnostics",
                            namespace_raw = namespace.inner(),
                            phase = "dispatch",
                            tier = "disk",
                            "partition poll dispatch"
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
                            attachment,
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

    /// Discard a read if its reply receiver is already disconnected at the check.
    /// Otherwise accept it on the owner's pump and attempt the reply before
    /// replication, which may suspend. Another shard can disconnect after the
    /// check, so delivery is not guaranteed and admitted progress is not rolled
    /// back if the reply fails.
    #[allow(clippy::future_not_send)]
    pub(crate) async fn on_poll_completed(&self, completion: PollCompleted) {
        let PollCompleted {
            namespace,
            result,
            reply,
            attachment,
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
        if reply.is_disconnected() {
            return;
        }
        if attachment.is_some_and(|attachment| {
            !attachment.session.is_valid()
                || !attachment
                    .metadata
                    .is_valid(self.plane.metadata().mux_stm.streams(), namespace)
        }) {
            let _ = reply.try_send(PartitionReadReply::Rejected(
                IggyError::TransientNotAccepted,
            ));
            return;
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
