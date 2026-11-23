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

use std::collections::BTreeMap;

use crate::shards_table::ShardsTable;
use crate::{
    COLD_POLL_BUDGET, ConsumerOffsetRead, ExternalGroupRead, IggyShard, LifecycleFrame,
    MAX_HELD_READS, MAX_HELD_READS_PER_NAMESPACE, PARTITION_BUILD_HOLD, PartitionRead,
    PartitionReadReply, ReadHold, Sender, ShardFrame,
};
use consensus::client_table::SessionAttachment;
use consensus::{Consensus, MetadataHandle, PartitionsHandle, is_partition_receipt_operation};
use iggy_binary_protocol::primitives::partition_history::ConsumerGroupOwner;
use iggy_binary_protocol::requests::messages::FETCH_REPLY_MARGIN_US;
use iggy_binary_protocol::requests::users::login_register::CredentialProof;
use iggy_binary_protocol::{Operation, RoutedRequestHeader};
use iggy_common::{IggyError, IggyTimestamp};
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use metadata::impls::metadata::StreamsFrontend;
use metadata::stm::stream::PollMetadata;
use partitions::{IggyPartition, PollCompletion, PollPlan, PollReadResult, PollingConsumer};
use server_common::Message;
use server_common::sharding::IggyNamespace;

pub mod completion;
#[cfg(test)]
mod completion_tests;
pub mod deferred;
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

#[derive(Debug)]
pub enum PollAttachment {
    Bound(ConsumerAttachment),
    Unbound {
        user_id: u32,
        credential: CredentialProof,
        metadata: PollMetadata,
    },
}

impl ConsumerAttachment {
    pub(crate) fn validate_write(
        &self,
        operation: Operation,
        now_micros: u64,
    ) -> Result<(), IggyError> {
        if !is_partition_receipt_operation(operation) {
            return Err(IggyError::InvalidCommand);
        }
        if !self.session.is_valid_at(now_micros) {
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
    target: PollTarget,
    /// Inbox enqueue time for disk diagnostics, or `None` for resident completion.
    #[cfg(feature = "poll-diagnostics")]
    queued_at: Option<std::time::Instant>,
}

pub enum PollTarget {
    Immediate {
        reply: Sender<PartitionReadReply>,
        attachment: Option<PollAttachment>,
    },
    Deferred {
        request_id: u64,
        _reservation: deferred::ReadReservation,
    },
}

/// A read that waits on the owner shard until its partition can serve it.
struct HeldRead {
    read: PartitionRead,
    reply: Sender<PartitionReadReply>,
    /// Committed `created_revision` when the read arrived. A partition built
    /// at another revision is another incarnation and does not answer it.
    created_revision: u64,
    /// Bus-clock micros at which the hold ends.
    deadline: u64,
}

impl HeldRead {
    /// What this read takes of [`MAX_HELD_READS`]: a batch stores each of its
    /// offset reads, and the end of its hold answers each of them.
    fn cost(&self) -> usize {
        match &self.read {
            PartitionRead::ConsumerOffsetsOnPrimary { reads, .. } => reads.len().max(1),
            _ => 1,
        }
    }
}

/// The held reads of a shard by namespace.
///
/// [`BTreeMap`], not `HashMap`: expiry and shutdown answer in map order, and
/// a per-process hash seed would change that order between simulator runs of
/// one seed.
#[derive(Default)]
pub struct HeldReads {
    by_namespace: BTreeMap<IggyNamespace, Vec<HeldRead>>,
    /// The [`HeldRead::cost`] of every held read. Carried, not summed again:
    /// the shard cap reads it for every arriving read.
    cost: usize,
}

impl HeldReads {
    #[cfg(any(test, feature = "simulator"))]
    pub fn count(&self) -> usize {
        self.by_namespace.values().map(Vec::len).sum()
    }

    /// Stores `read`, or hands it back when a cap is spent.
    fn hold(&mut self, namespace: IggyNamespace, read: HeldRead) -> Option<HeldRead> {
        let cost = read.cost();
        if self.cost.saturating_add(cost) > MAX_HELD_READS {
            return Some(read);
        }
        // A new entry is below the namespace cap, so a refusal leaves no
        // empty entry behind.
        let reads = self.by_namespace.entry(namespace).or_default();
        if reads.len() >= MAX_HELD_READS_PER_NAMESPACE {
            return Some(read);
        }
        reads.push(read);
        self.cost += cost;
        None
    }

    fn take(&mut self, namespace: IggyNamespace) -> Vec<HeldRead> {
        let reads = self.by_namespace.remove(&namespace).unwrap_or_default();
        let cost = reads.iter().map(HeldRead::cost).sum();
        self.cost = self.cost.saturating_sub(cost);
        reads
    }

    /// Removes the reads whose hold ended by `now`. A read whose requester
    /// stopped waiting leaves without being returned.
    fn take_expired(&mut self, now: u64) -> Vec<(IggyNamespace, HeldRead)> {
        let mut expired = Vec::new();
        self.by_namespace.retain(|namespace, reads| {
            reads.retain(|read| !read.reply.is_disconnected());
            expired.extend(
                reads
                    .extract_if(.., |read| read.deadline <= now)
                    .map(|read| (*namespace, read)),
            );
            !reads.is_empty()
        });
        self.cost = self
            .by_namespace
            .values()
            .flatten()
            .map(HeldRead::cost)
            .sum();
        expired
    }

    fn take_all(&mut self) -> BTreeMap<IggyNamespace, Vec<HeldRead>> {
        self.cost = 0;
        std::mem::take(&mut self.by_namespace)
    }
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
        attachment.validate_write(request.header().operation, self.bus.realtime_micros())?;
        let namespace = IggyNamespace::from_raw(request.header().group);
        if !attachment
            .metadata
            .is_valid_for_offset(self.plane.metadata().mux_stm.streams(), namespace)
        {
            return Err(IggyError::TransientNotAccepted);
        }
        self.plane
            .partitions()
            .with_partition(&namespace, |partition| {
                let consensus = partition.consensus();
                if consensus.is_normal_backup() {
                    return Err(IggyError::NotPrimary);
                }
                let admissible = !partition.requires_state_transfer()
                    && consensus.is_primary()
                    && consensus.is_normal()
                    && !consensus.is_transferring()
                    && attachment
                        .metadata
                        .matches_partition(self.shards_table.epoch_for(namespace));
                if admissible {
                    Ok(())
                } else {
                    Err(IggyError::TransientNotAccepted)
                }
            })
            .unwrap_or(Err(IggyError::TransientNotAccepted))
    }

    /// Execute a routed read on the partition owner's pump.
    /// Partitions missing materialized data reject the read. Resident polls finish
    /// inline. Disk polls return to the pump for acceptance after detached I/O.
    /// A lookup of the primary, or a read of messages or offsets, gets no
    /// answer here while its partition cannot serve yet: see
    /// [`Self::read_hold`]. It is held until the partition can, and not past
    /// the deadline of `hold`, the end that its requester or an earlier hold
    /// of the read set. A read that an earlier hold handed back to the inbox
    /// runs only on the incarnation that the hold waited for.
    #[allow(clippy::future_not_send, clippy::too_many_lines)]
    pub(crate) async fn on_partition_read(
        &self,
        namespace: IggyNamespace,
        read: PartitionRead,
        reply: Sender<PartitionReadReply>,
        hold: Option<ReadHold>,
    ) {
        let awaited = self.read_hold(namespace, &read);
        // The pump can remove the partition that a hold ended for, and build
        // the next incarnation of its ids, before the read that the hold
        // handed back runs. Not every read carries a fence of its own.
        if let Some(waited_for) = hold.and_then(|hold| hold.created_revision) {
            let incarnation = awaited
                .map(|(_, created_revision)| created_revision)
                .or_else(|| {
                    self.plane
                        .partitions()
                        .with_partition(&namespace, IggyPartition::created_revision)
                });
            if incarnation.is_some_and(|incarnation| incarnation != waited_for) {
                let _ = reply.try_send(PartitionReadReply::Rejected(
                    IggyError::TransientNotAccepted,
                ));
                return;
            }
        }
        if let Some((awaited, created_revision)) = awaited {
            self.hold_read(
                awaited,
                created_revision,
                read,
                reply,
                hold.map(|hold| hold.deadline),
            );
            return;
        }
        if let PartitionRead::DeferredPoll(request) = read {
            self.admit_deferred_poll(namespace, *request, &reply);
            return;
        }
        if let PartitionRead::ConsumerOffsetsOnPrimary { reads, .. } = read {
            let _ = reply.try_send(self.consumer_offsets_reply(reads));
            return;
        }
        if matches!(read, PartitionRead::Primary | PartitionRead::PrimaryNow) {
            // The caller of `PrimaryNow` serves at the primary that the answer
            // names. A primary here that still starts refuses that read, and
            // so does the owner for an older incarnation of the ids.
            let cannot_serve = matches!(read, PartitionRead::PrimaryNow)
                && self
                    .plane
                    .partitions()
                    .with_partition(&namespace, |partition| {
                        self.superseding_revision(namespace, partition)
                            .or_else(|| Self::starting_revision(partition))
                    })
                    .flatten()
                    .is_some();
            let answer = if cannot_serve {
                PartitionReadReply::Rejected(IggyError::TransientNotAccepted)
            } else {
                self.primary_read_reply(namespace)
                    .unwrap_or(PartitionReadReply::NotFound)
            };
            let _ = reply.try_send(answer);
            return;
        }
        let partitions = self.plane.partitions();
        if matches!(read, PartitionRead::SessionsRetired { .. }) {
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
        let rejection = self
            .partition_read_rejection(namespace, &read)
            .or_else(|| self.geo_read_refusal(namespace, &read))
            .or_else(|| {
                // Metadata lists the partition and it is not here: not built yet,
                // or behind a tombstone. `NotFound` would read as a permanent
                // miss on HTTP. A read that needs the primary was refused above
                // when its partition is not here.
                (read.reads_messages_or_offset()
                    && !read.requires_primary()
                    && !self.plane.partitions().contains(&namespace)
                    && self.stored_partition_revision(namespace).is_some())
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
                Some(PollAttachment::Bound(attachment)),
            ),
            PartitionRead::UnboundPollOnPrimary {
                consumer,
                args,
                user_id,
                credential,
                metadata,
            } => (
                PartitionRead::Poll {
                    consumer,
                    args,
                    metadata: None,
                },
                Some(PollAttachment::Unbound {
                    user_id,
                    credential,
                    metadata,
                }),
            ),
            read => (read, None),
        };
        let result = match read {
            PartitionRead::SessionsRetired {
                ended,
                retired_through,
                created_revision,
            } => partitions
                .with_partition(&namespace, |partition| {
                    (partition.created_revision() == created_revision).then(|| {
                        PartitionReadReply::SessionsRetired(
                            partition.sessions_retired(&ended, retired_through),
                        )
                    })
                })
                .flatten()
                .unwrap_or(PartitionReadReply::NotFound),
            PartitionRead::RetirementDebt {
                created_revision,
                debt_op,
            } => partitions
                .with_partition(&namespace, |partition| {
                    (partition.created_revision() == created_revision).then(|| {
                        PartitionReadReply::RetirementDebt(
                            partition.observe_retirement_debt(debt_op),
                        )
                    })
                })
                .flatten()
                .unwrap_or(PartitionReadReply::NotFound),
            PartitionRead::DeferredPoll(_) => unreachable!("deferred admission handled above"),
            PartitionRead::Primary | PartitionRead::PrimaryNow => {
                unreachable!("primary read answered above")
            }
            // Answered above, entry by entry.
            PartitionRead::ConsumerOffsetsOnPrimary { .. } => {
                PartitionReadReply::Rejected(IggyError::InvalidCommand)
            }
            PartitionRead::Poll { consumer, args, .. }
            | PartitionRead::PollOnPrimary { consumer, args, .. }
            | PartitionRead::UnboundPollOnPrimary { consumer, args, .. } => {
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
                        let Ok(cold) = self.poll_completions.admit(plan.reads_remote()) else {
                            completion::reject(
                                &reply,
                                self.metrics.frame_drop_metrics(),
                                crate::metrics::frame_drop_reason::FULL,
                            );
                            return;
                        };
                        let plan = plan.with_deadline(self.bus.monotonic_now() + COLD_POLL_BUDGET);
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
                        self.bus.spawn(read_poll(namespace, plan, completion, cold));
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
                            target: PollTarget::Immediate { reply, attachment },
                            #[cfg(feature = "poll-diagnostics")]
                            queued_at: None,
                        })
                        .await;
                        return;
                    }
                }
            }
            PartitionRead::ConsumerOffset { consumer }
            | PartitionRead::ConsumerOffsetOnPrimary {
                consumer: Some(consumer),
                ..
            } => partitions
                .consumer_offset_read(&namespace, consumer)
                .map_or(PartitionReadReply::NotFound, |(stored, current_offset)| {
                    PartitionReadReply::ConsumerOffset {
                        stored,
                        current_offset,
                    }
                }),
            PartitionRead::ConsumerOffsetOnPrimary { consumer: None, .. } => {
                PartitionReadReply::ConsumerOffset {
                    stored: None,
                    current_offset: 0,
                }
            }
            PartitionRead::ExternalGroupOffsetOnPrimary { group, .. } => match group {
                ExternalGroupRead::Group(group_id) => {
                    partitions.external_group_offset_read(&namespace, group_id)
                }
                ExternalGroupRead::Missing(name) => {
                    partitions.external_position_read(&namespace, name)
                }
            }
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

    /// Ends the holds of `namespace` when its partition can serve: after the
    /// insert or the removal of a partition, and on the tick. The holds of a
    /// primary that still starts stay until the tick finds it started. Those
    /// of an older incarnation stay until the insert of the committed one.
    ///
    /// Every insert and removal of a partition passes here, so the committed
    /// lookup runs only for a namespace with held reads.
    pub(crate) fn release_settled_reads(&self, namespace: IggyNamespace) {
        if !self
            .held_reads
            .borrow()
            .by_namespace
            .contains_key(&namespace)
        {
            return;
        }
        let waits = self.awaits_start(namespace)
            || self
                .plane
                .partitions()
                .with_partition(&namespace, |partition| {
                    self.superseding_revision(namespace, partition)
                })
                .flatten()
                .is_some();
        if !waits {
            self.release_held_reads(namespace);
        }
    }

    /// Whether the partition of `namespace` cannot serve yet and will
    /// without a new request: this shard is due to build it, or it is a
    /// primary that still starts.
    pub(crate) fn awaits_start(&self, namespace: IggyNamespace) -> bool {
        // One lookup for a partition that is here: a fetch asks it for each
        // of its targets on this shard.
        self.plane
            .partitions()
            .with_partition(&namespace, |partition| {
                Self::starting_revision(partition).is_some()
            })
            .unwrap_or_else(|| self.awaited_build(namespace).is_some())
    }

    /// Ends the holds of `namespace`. A lookup gets its answer, and every
    /// other read runs again, unless its hold ended or another incarnation
    /// took the place of the one it waited for: not every read carries a
    /// fence of its own.
    ///
    /// A build can land after the end of a hold and before the sweep that
    /// expires it. The requester of that read no longer waits long enough
    /// for a run to reach it, and a poll that runs then can commit an offset
    /// whose messages nobody receives.
    fn release_held_reads(&self, namespace: IggyNamespace) {
        let held = self.held_reads.borrow_mut().take(namespace);
        let built = self
            .plane
            .partitions()
            .with_partition(&namespace, IggyPartition::created_revision);
        let now = self.bus.monotonic_micros();
        for held in held {
            if matches!(held.read, PartitionRead::Primary) {
                let answer = self.lookup_answer(namespace, held.created_revision);
                let _ = held.reply.try_send(answer);
            } else if held.deadline <= now {
                self.metrics.record_partition_held_read_expired();
                self.answer_unheld(namespace, held);
            } else if built.is_some_and(|built| built != held.created_revision) {
                self.answer_unheld(namespace, held);
            } else {
                self.resubmit_held_read(namespace, held);
            }
        }
    }

    /// Hands a held read back to the inbox of this shard, where it runs as a
    /// read that arrives now, except that a new hold ends where this one does
    /// and that it runs only on the incarnation that this hold waited for.
    /// A node whose metadata is no longer fresh refuses it.
    fn resubmit_held_read(&self, namespace: IggyNamespace, held: HeldRead) {
        let HeldRead {
            read,
            reply,
            created_revision,
            deadline,
        } = held;
        // A read whose requester left must not run: it can commit an offset
        // that nobody receives.
        if reply.is_disconnected() {
            return;
        }
        let refused = reply.clone();
        // A batch waits for one partition after the other, and each of its
        // reads has a fence of its own.
        let created_revision = (!matches!(read, PartitionRead::ConsumerOffsetsOnPrimary { .. }))
            .then_some(created_revision);
        let frame = ShardFrame::lifecycle(LifecycleFrame::PartitionRead {
            namespace,
            read,
            reply,
            hold: Some(ReadHold {
                deadline,
                created_revision,
            }),
        });
        let sent = self.partition_requests_allowed()
            && self
                .senders
                .get(self.id as usize)
                .is_some_and(|sender| sender.try_send(frame).is_ok());
        if !sent {
            let _ = refused.try_send(PartitionReadReply::Rejected(
                IggyError::TransientNotAccepted,
            ));
        }
    }

    /// Whether this node still has proof that its metadata is fresh. Dispatch
    /// tests that before a read reaches this shard, and a hold can end after
    /// the proof does. Session and permission tests on stale metadata prove
    /// nothing, so a held read must not serve then.
    fn partition_requests_allowed(&self) -> bool {
        self.plane
            .metadata()
            .applied_frontier()
            .partition_requests_allowed(self.bus.monotonic_now())
    }

    /// Ends the holds whose partition can serve now, and those that reached
    /// their deadline. On the bus clock, so the virtual clock of the
    /// simulator governs it.
    pub(crate) fn expire_held_reads(&self) {
        let cost = self.held_reads.borrow().cost;
        self.metrics.set_partition_held_reads(cost);
        if cost == 0 {
            return;
        }
        let held: Vec<IggyNamespace> = self
            .held_reads
            .borrow()
            .by_namespace
            .keys()
            .copied()
            .collect();
        for namespace in held {
            self.release_settled_reads(namespace);
        }
        let expired = self
            .held_reads
            .borrow_mut()
            .take_expired(self.bus.monotonic_micros());
        for (namespace, read) in expired {
            self.metrics.record_partition_held_read_expired();
            self.answer_unheld(namespace, read);
        }
    }

    /// Answers every held read when the shard stops, as the deferred polls
    /// are answered: no build lands after the stop.
    pub(crate) fn reject_held_reads(&self) {
        let held = self.held_reads.borrow_mut().take_all();
        for read in held.into_values().flatten() {
            let _ = read.reply.try_send(PartitionReadReply::Rejected(
                IggyError::TransientNotAccepted,
            ));
        }
    }

    /// The answer to a [`PartitionRead::Primary`] of `namespace`, or `None`
    /// when this shard has no partition to answer from.
    fn primary_read_reply(&self, namespace: IggyNamespace) -> Option<PartitionReadReply> {
        if let Some(error) = self.partition_read_rejection(namespace, &PartitionRead::Primary) {
            return Some(PartitionReadReply::Rejected(error));
        }
        self.plane
            .partitions()
            .with_partition(&namespace, |partition| {
                let consensus = partition.consensus();
                let view = consensus.view();
                if consensus.is_normal()
                    && !consensus.is_transferring()
                    && !(consensus.has_ceded_primaryship()
                        && consensus.primary_index(view) == consensus.replica())
                {
                    PartitionReadReply::Primary {
                        view,
                        primary: consensus.primary_node(view),
                    }
                } else {
                    PartitionReadReply::Rejected(IggyError::TransientNotAccepted)
                }
            })
    }

    /// The namespace and `created_revision` to hold `read` under, when its
    /// partition cannot serve it yet and will without a new request.
    ///
    /// A client reaches this node through the lookup that another member
    /// answered after its own start, so a read can arrive before the build
    /// here, or before the primary here can serve. A refusal would reach a
    /// client that has no reason to expect one right after a create. A lookup
    /// of the primary also waits out an older incarnation, see
    /// [`Self::superseding_revision`].
    fn read_hold(
        &self,
        namespace: IggyNamespace,
        read: &PartitionRead,
    ) -> Option<(IggyNamespace, u64)> {
        if self.shutting_down.get() {
            return None;
        }
        let lookup = matches!(read, PartitionRead::Primary);
        let hold = |namespace: IggyNamespace, needs_primary: bool| {
            // One lookup for a partition that is here, which every read
            // finds but the first ones after a create. A lookup of the
            // primary also reads committed metadata.
            self.plane
                .partitions()
                .with_partition(&namespace, |partition| {
                    // Before the start test, which holds under the revision
                    // of this partition: the insert of the committed one
                    // would answer that hold with `NotFound`.
                    lookup
                        .then(|| self.superseding_revision(namespace, partition))
                        .flatten()
                        .or_else(|| {
                            needs_primary
                                .then(|| Self::starting_revision(partition))
                                .flatten()
                        })
                })
                .unwrap_or_else(|| self.awaited_build(namespace))
                .map(|created_revision| (namespace, created_revision))
        };
        match read {
            PartitionRead::Primary => hold(namespace, true),
            PartitionRead::ConsumerOffsetsOnPrimary { reads, hold_until } => {
                // A batch past its hold end is answered at once, so its
                // entries need no lookup.
                if *hold_until <= self.bus.monotonic_micros() {
                    return None;
                }
                reads.iter().find_map(|read| hold(read.namespace, true))
            }
            PartitionRead::DeferredPoll(request) => request
                .holdable
                .then(|| hold(namespace, request.context.primary))
                .flatten(),
            read if read.reads_messages_or_offset() => hold(namespace, read.requires_primary()),
            _ => None,
        }
    }

    /// Committed `created_revision` of the partition of `namespace` when this
    /// shard is due to build it: local metadata holds it as committed, this
    /// node stores it and it hashes here.
    ///
    /// A tombstone counts where the reconciler is seen to build the committed
    /// partition after it: a partition still here, in teardown or quarantine,
    /// or the fence of an older incarnation without a partition, whose files
    /// it removes first. A fence of the committed incarnation or a newer one
    /// stays. Nor does a fenced partition that is still here count: the
    /// reconciler waits for a removal that only a teardown sends. Another
    /// tombstone without a partition does not count either: only the records
    /// of the reconciler tell whether a build follows it.
    fn awaited_build(&self, namespace: IggyNamespace) -> Option<u64> {
        let partitions = self.plane.partitions();
        let built = partitions.contains(&namespace);
        let tombstoned = partitions.is_tombstoned(&namespace);
        if (built && !tombstoned) || self.partition_shards.shard_for(&namespace) != self.id {
            return None;
        }
        let committed = self.stored_partition_revision(namespace)?;
        if !tombstoned {
            return Some(committed);
        }
        partitions
            .failed_revision(&namespace)
            .map_or(built, |failed| !built && failed < committed)
            .then_some(committed)
    }

    /// Committed `created_revision` of `namespace` when `partition` is an
    /// older incarnation, which the reconciler of this owner shard replaces.
    /// A delete and a create reuse the ids of a partition, and a lookup must
    /// not get the primary of the group that is torn down. Revisions only
    /// grow, so a partition above local metadata is newer and stays.
    fn superseding_revision(
        &self,
        namespace: IggyNamespace,
        partition: &IggyPartition<B, SB>,
    ) -> Option<u64> {
        if self.partition_shards.shard_for(&namespace) != self.id {
            return None;
        }
        self.stored_partition_revision(namespace)
            .filter(|committed| partition.created_revision() < *committed)
    }

    /// `created_revision` of `partition` when it is the primary of a normal
    /// view that refuses reads until its view write and its view start land.
    /// A partition created in a view above 0 starts so.
    fn starting_revision(partition: &IggyPartition<B, SB>) -> Option<u64> {
        let consensus = partition.consensus();
        (consensus.is_primary()
            && consensus.is_normal()
            && !consensus.is_transferring()
            && (consensus.needs_superblock_persist() || !consensus.applied_view_start()))
        .then(|| partition.created_revision())
    }

    /// Holds `read` until the partition of `namespace` can serve it, or
    /// answers it at once when a cap is spent or its requester left no time
    /// for a hold. A read with a `hold_deadline` keeps it: the end that its
    /// requester set, or the end of the hold before this one.
    fn hold_read(
        &self,
        namespace: IggyNamespace,
        created_revision: u64,
        read: PartitionRead,
        reply: Sender<PartitionReadReply>,
        hold_deadline: Option<u64>,
    ) {
        let hold = u64::try_from(PARTITION_BUILD_HOLD.as_micros()).unwrap_or(u64::MAX);
        let now = self.bus.monotonic_micros();
        let mut deadline = hold_deadline.unwrap_or_else(|| now.saturating_add(hold));
        // The requester of these reads stops its wait at a moment of its own,
        // and it must get the answer of the owner before that.
        match &read {
            PartitionRead::ConsumerOffsetsOnPrimary { hold_until, .. } => {
                deadline = deadline.min(*hold_until);
            }
            PartitionRead::DeferredPoll(request) => {
                deadline = deadline.min(
                    request
                        .request_deadline
                        .saturating_sub(FETCH_REPLY_MARGIN_US),
                );
            }
            _ => {}
        }
        let read = HeldRead {
            read,
            reply,
            created_revision,
            deadline,
        };
        if deadline <= now {
            self.answer_unheld(namespace, read);
            return;
        }
        let refused = self.held_reads.borrow_mut().hold(namespace, read);
        if let Some(read) = refused {
            self.metrics.record_partition_held_read_refused();
            tracing::debug!(
                shard = self.id,
                namespace_raw = namespace.inner(),
                "held partition reads at capacity; answering without the wait"
            );
            self.answer_unheld(namespace, read);
        }
    }

    /// Answers a read whose wait ended without a partition that can serve.
    /// A lookup gets what the partition says now. A batch of offset reads is
    /// answered entry by entry, so the partition that cannot serve refuses
    /// only its own entry. Every other read is refused as transient, never
    /// `NotFound`: HTTP maps the `NotFound` of those reads to a permanent 404.
    /// So is a batch on a node whose metadata is no longer fresh.
    fn answer_unheld(&self, namespace: IggyNamespace, held: HeldRead) {
        let answer = match held.read {
            PartitionRead::Primary => self.lookup_answer(namespace, held.created_revision),
            PartitionRead::ConsumerOffsetsOnPrimary { reads, .. }
                if self.partition_requests_allowed() =>
            {
                self.consumer_offsets_reply(reads)
            }
            _ => PartitionReadReply::Rejected(IggyError::TransientNotAccepted),
        };
        let _ = held.reply.try_send(answer);
    }

    /// The answer to a held lookup: that of the partition that is here when
    /// it is the incarnation the lookup waited for, and `NotFound` otherwise.
    fn lookup_answer(&self, namespace: IggyNamespace, created_revision: u64) -> PartitionReadReply {
        let built_revision = self
            .plane
            .partitions()
            .with_partition(&namespace, IggyPartition::created_revision);
        (built_revision == Some(created_revision))
            .then(|| self.primary_read_reply(namespace))
            .flatten()
            .unwrap_or(PartitionReadReply::NotFound)
    }

    /// The condition of this shard that refuses `read` of `namespace` before
    /// it runs, if any.
    #[allow(clippy::too_many_lines)]
    fn partition_read_rejection(
        &self,
        namespace: IggyNamespace,
        read: &PartitionRead,
    ) -> Option<IggyError> {
        let partitions = self.plane.partitions();
        partitions
            .with_partition(&namespace, |partition| {
                if read.requires_primary() {
                    let consensus = partition.consensus();
                    if consensus.is_normal_backup() {
                        return Some(IggyError::NotPrimary);
                    }
                    // The quorum check of a primary waits for its view write, so
                    // until the write lands nothing bounds how long a primary cut
                    // off from a newer view serves.
                    if !consensus.is_primary()
                        || !consensus.is_normal()
                        || consensus.is_transferring()
                        || consensus.needs_superblock_persist()
                    {
                        return Some(IggyError::TransientNotAccepted);
                    }
                }
                if partition.requires_state_transfer() || partition.read_history_is_changing() {
                    return Some(IggyError::TransientNotAccepted);
                }
                // The merged log of a new view can hold a message append or an
                // offset store that the old view acknowledged. The new primary
                // applies it only after the view starts, so until then a poll
                // can miss acknowledged messages and a stored offset can be
                // older. The head is current only on the primary that started
                // the view.
                if read.reads_messages_or_offset()
                    && partition.consensus().is_primary()
                    && !partition.consensus().applied_view_start()
                {
                    return Some(IggyError::TransientNotAccepted);
                }
                if let PartitionRead::ConsumerOffsetOnPrimary { metadata, .. }
                | PartitionRead::ExternalGroupOffsetOnPrimary { metadata, .. } = &read
                {
                    if !metadata
                        .is_valid_for_offset(self.plane.metadata().mux_stm.streams(), namespace)
                        || !metadata.matches_partition(self.shards_table.epoch_for(namespace))
                    {
                        return Some(IggyError::TransientNotAccepted);
                    }
                    if partition.history_deleted() {
                        return Some(IggyError::HistoryUnavailable);
                    }
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
                }
                | PartitionRead::UnboundPollOnPrimary { metadata, .. } = &read
                    && (!metadata.is_valid(self.plane.metadata().mux_stm.streams(), namespace)
                        || !metadata.matches_partition(self.shards_table.epoch_for(namespace)))
                {
                    return Some(IggyError::TransientNotAccepted);
                }
                (matches!(
                    read,
                    PartitionRead::Poll { .. } | PartitionRead::UnboundPollOnPrimary { .. }
                ) && partition.history_deleted())
                .then_some(IggyError::HistoryUnavailable)
            })
            .unwrap_or_else(|| {
                read.requires_primary()
                    .then_some(IggyError::TransientNotAccepted)
            })
    }

    /// A [`PartitionRead::ConsumerOffsetOnPrimary`] of a partition that this
    /// shard owns, run in place.
    pub(crate) fn consumer_offset_on_primary(
        &self,
        ConsumerOffsetRead {
            namespace,
            consumer,
            metadata,
        }: ConsumerOffsetRead,
    ) -> PartitionReadReply {
        let read = PartitionRead::ConsumerOffsetOnPrimary { consumer, metadata };
        if let Some(error) = self.partition_read_rejection(namespace, &read) {
            return PartitionReadReply::Rejected(error);
        }
        let Some(consumer) = consumer else {
            return PartitionReadReply::ConsumerOffset {
                stored: None,
                current_offset: 0,
            };
        };
        self.plane
            .partitions()
            .consumer_offset_read(&namespace, consumer)
            .map_or(PartitionReadReply::NotFound, |(stored, current_offset)| {
                PartitionReadReply::ConsumerOffset {
                    stored,
                    current_offset,
                }
            })
    }

    /// The reply to [`PartitionRead::ConsumerOffsetsOnPrimary`]: each read
    /// answered alone, in order.
    fn consumer_offsets_reply(&self, reads: Vec<ConsumerOffsetRead>) -> PartitionReadReply {
        PartitionReadReply::ConsumerOffsets(
            reads
                .into_iter()
                .map(|read| self.consumer_offset_on_primary(read))
                .collect(),
        )
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
            target,
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
        let (reply, attachment) = match target {
            PollTarget::Immediate { reply, attachment } => (reply, attachment),
            PollTarget::Deferred {
                request_id,
                _reservation,
            } => {
                self.on_deferred_poll_completed(request_id, result).await;
                return;
            }
        };
        if reply.is_disconnected() {
            return;
        }
        if let Some(attachment) = &attachment {
            let now_micros = self.bus.realtime_micros();
            let authorization = self.plane.metadata().mux_stm.users().read(|users| {
                let (user_id, metadata, account_valid) = match attachment {
                    PollAttachment::Bound(attachment) => (
                        attachment.session.user_id(),
                        &attachment.metadata,
                        attachment.session.is_valid_at(now_micros),
                    ),
                    PollAttachment::Unbound {
                        user_id,
                        credential,
                        metadata,
                    } => (
                        *user_id,
                        metadata,
                        users.accepts_credential_at(
                            *user_id,
                            credential,
                            IggyTimestamp::from(now_micros),
                        ),
                    ),
                };
                if !account_valid {
                    Err(match attachment {
                        PollAttachment::Bound(_) => IggyError::TransientNotAccepted,
                        PollAttachment::Unbound { .. } => IggyError::Unauthenticated,
                    })
                } else if !metadata.is_valid(self.plane.metadata().mux_stm.streams(), namespace) {
                    Err(IggyError::TransientNotAccepted)
                } else {
                    users.authorize(|permissioner| {
                        permissioner.poll_messages(
                            user_id,
                            namespace.stream_id(),
                            namespace.topic_id(),
                        )
                    })
                }
            });
            if let Err(error) = authorization {
                let _ = reply.try_send(PartitionReadReply::Rejected(error));
                return;
            }
        }
        self.accept_poll_result(namespace, result, reply, None)
            .await;
    }

    #[allow(clippy::future_not_send)]
    async fn accept_poll_result(
        &self,
        namespace: IggyNamespace,
        result: PollReadResult,
        reply: Sender<PartitionReadReply>,
        next_offset: Option<u64>,
    ) {
        let partitions = self.plane.partitions();
        let consumer_kind = result.consumer_kind();
        match partitions.complete_poll(&namespace, result) {
            Ok(PollCompletion {
                context,
                fragments,
                current_offset,
                replication,
                encryptor,
            }) => {
                // The owner has admitted progress, but the offset may still be
                // queued. Release the reply before waiting for replication.
                // A poll reply does not acknowledge a durable offset commit.
                let _ = reply.try_send(PartitionReadReply::Poll {
                    context,
                    fragments,
                    current_offset,
                    encryptor,
                    next_offset,
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
/// read's progress under the installed owner. `TransientNotAccepted` means
/// only that the partition has not applied the install yet, and a readiness
/// mirror reads it as not ready.
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
        metadata.client_id(),
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
    cold: Option<completion::ColdPollPermit>,
) {
    let poll_started = std::time::Instant::now();
    let result = plan.execute().await;
    drop(cold);
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
