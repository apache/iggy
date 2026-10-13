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
//! A disk read can yield while a delete and re-create or a state transfer
//! replaces the history, even on the same shard thread. The detached task
//! therefore cannot advance progress or authorize a successful reply.
//! Completion validation and progress updates run synchronously on the owner,
//! before any replication wait.

use crate::shards_table::ShardsTable;
use crate::{IggyShard, PartitionRead, PartitionReadReply, Sender};
use consensus::client_table::SessionAttachment;
use consensus::{Consensus, MetadataHandle, PartitionsHandle, is_partition_receipt_operation};
use iggy_binary_protocol::primitives::partition_history::ConsumerGroupOwner;
use iggy_binary_protocol::{Operation, RoutedRequestHeader};
use iggy_common::IggyError;
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use metadata::impls::metadata::StreamsFrontend;
use metadata::stm::stream::PollMetadata;
use partitions::{PollCompletion, PollPlan, PollReadResult, PollingConsumer};
use server_common::Message;
use server_common::sharding::IggyNamespace;

pub mod completion;
#[cfg(test)]
mod completion_tests;
#[cfg(test)]
mod test_support;
#[cfg(test)]
pub mod timeout_tests;

/// Parent session and metadata identity checked by the partition owner before
/// accepting consumer progress, including after detached poll I/O.
#[derive(Debug)]
pub struct ConsumerAttachment {
    pub session: SessionAttachment,
    pub metadata: PollMetadata,
}

impl ConsumerAttachment {
    pub(crate) fn validate_write(&self, operation: Operation) -> Result<(), IggyError> {
        if !is_partition_receipt_operation(operation) {
            return Err(IggyError::InvalidCommand);
        }
        if !self.session.is_valid() {
            return Err(IggyError::StaleClient);
        }
        Ok(())
    }
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
    attachment: Option<ConsumerAttachment>,
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
    pub(crate) fn validate_partition_attachment(
        &self,
        request: &Message<RoutedRequestHeader>,
        attachment: &ConsumerAttachment,
    ) -> Result<(), IggyError> {
        attachment.validate_write(request.header().operation)?;
        let namespace = IggyNamespace::from_raw(request.header().group);
        if !attachment
            .metadata
            .is_valid_for_offset(self.plane.metadata().mux_stm.streams(), namespace)
        {
            return Err(IggyError::TransientNotAccepted);
        }
        let admissible = self
            .plane
            .partitions()
            .with_partition(&namespace, |partition| {
                let consensus = partition.consensus();
                !partition.requires_state_transfer()
                    && consensus.is_primary()
                    && consensus.is_normal()
                    && !consensus.is_transferring()
                    && attachment
                        .metadata
                        .matches_partition(self.shards_table.epoch_for(namespace))
            })
            .unwrap_or(false);
        if !admissible {
            return Err(IggyError::TransientNotAccepted);
        }
        Ok(())
    }

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
        if matches!(read, PartitionRead::SessionRetired { .. }) {
            let failed_revision = partitions.failed_revision(&namespace).or_else(|| {
                partitions
                    .with_partition(&namespace, |partition| {
                        partition.fatal().map(|_| partition.created_revision())
                    })
                    .flatten()
            });
            if let Some(created_revision) = failed_revision {
                let _ = reply
                    .try_send(PartitionReadReply::SessionRetirementFailed { created_revision });
                return;
            }
        }
        let rejection = partitions
            .with_partition(&namespace, |partition| {
                if partition.requires_state_transfer() || partition.read_history_is_changing() {
                    return Some(IggyError::TransientNotAccepted);
                }
                if let PartitionRead::PollOnPrimary {
                    consumer,
                    attachment,
                    ..
                } = &read
                {
                    let consensus = partition.consensus();
                    if !consensus.is_primary()
                        || !consensus.is_normal()
                        || consensus.is_transferring()
                        || !attachment
                            .metadata
                            .matches_partition(self.shards_table.epoch_for(namespace))
                    {
                        return Some(IggyError::TransientNotAccepted);
                    }
                    // This node's metadata can lag a delete fence the partition
                    // already applied, so it still authorizes the old incarnation.
                    if partition.history_deleted() {
                        return Some(IggyError::HistoryUnavailable);
                    }
                    // The plan captures this owner in the same turn, and
                    // completion refuses an owner that changed since.
                    return match *consumer {
                        PollingConsumer::ConsumerGroup(group_id, _) => group_owner_rejection(
                            namespace,
                            group_id as u64,
                            partition.consumer_group_owner(group_id as u64),
                            &attachment.metadata,
                        ),
                        PollingConsumer::Consumer(..) => None,
                    };
                }
                if let PartitionRead::Poll {
                    metadata: Some(metadata),
                    ..
                } = &read
                    && (!metadata.is_valid(self.plane.metadata().mux_stm.streams(), namespace)
                        || !metadata.matches_partition(self.shards_table.epoch_for(namespace)))
                {
                    return Some(IggyError::TransientNotAccepted);
                }
                (matches!(read, PartitionRead::Poll { .. }) && partition.history_deleted())
                    .then_some(IggyError::HistoryUnavailable)
            })
            .unwrap_or_else(|| {
                matches!(read, PartitionRead::PollOnPrimary { .. })
                    .then_some(IggyError::TransientNotAccepted)
            });
        if let Some(error) = rejection {
            let _ = reply.try_send(PartitionReadReply::Rejected(error));
            return;
        }
        let (read, attachment) = match read {
            PartitionRead::PollOnPrimary {
                consumer,
                args,
                attachment,
            } => (
                PartitionRead::Poll {
                    consumer,
                    args,
                    metadata: None,
                },
                Some(attachment),
            ),
            read => (read, None),
        };
        let result = match read {
            PartitionRead::SessionRetired { identity } => partitions
                .with_partition(&namespace, |partition| {
                    PartitionReadReply::SessionRetired(partition.session_retired(identity))
                })
                .unwrap_or(PartitionReadReply::NotFound),
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
            PartitionRead::Poll { consumer, args, .. }
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
            PartitionRead::ExternalGroupOffset { group_id } => partitions
                .external_group_offset_read(&namespace, group_id)
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
            PartitionRead::ResolveSegmentDeleteOffset { count } => partitions
                .segment_delete_resolution(&namespace, count)
                .map_or(
                    PartitionReadReply::NotFound,
                    |(up_to_offset, lagging, created_revision)| {
                        PartitionReadReply::SegmentDeleteOffset {
                            up_to_offset,
                            lagging,
                            created_revision,
                        }
                    },
                ),
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
                context,
                fragments,
                current_offset,
                replication,
            }) => {
                // The owner has admitted progress, but the offset may still be
                // queued. Release the reply before waiting for replication.
                // A poll reply does not acknowledge a durable offset commit.
                let _ = reply.try_send(PartitionReadReply::Poll {
                    context,
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

/// Serve a group read only to the partition's installed owner. The serving
/// node's metadata can lag an install the partition already applied, for
/// example on a new primary after a view change, and the partition commits a
/// read's progress under the installed owner.
fn group_owner_rejection(
    namespace: IggyNamespace,
    group_id: u64,
    installed: Option<ConsumerGroupOwner>,
    metadata: &PollMetadata,
) -> Option<IggyError> {
    if installed.is_some_and(|owner| metadata.is_installed_owner(group_id, owner)) {
        return None;
    }
    // Metadata activates an owner only after its install committed, so an
    // older or missing install means this replica has not applied it yet.
    let attached_generation = metadata.context(0).owner_generation;
    if installed.is_none_or(|owner| owner.generation < attached_generation) {
        return Some(IggyError::TransientNotAccepted);
    }
    Some(IggyError::ConsumerGroupPartitionNotOwned(
        u32::try_from(group_id).unwrap_or(u32::MAX),
        u32::try_from(namespace.partition_id()).unwrap_or(u32::MAX),
    ))
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
