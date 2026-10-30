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

//! `FetchMessages` (109): one request names partitions of one topic, waits
//! until they hold messages, reads them and answers with one section per
//! target.

use crate::dispatch::authz::{
    authorize_partition_read, authorize_served_incarnation, authorize_served_topic_read,
};
use crate::dispatch::partition::{fence_poll_context, polling_consumer_from_wire};
use crate::namespace::resolve_partition_namespace;
use crate::reply_frame::{
    build_fetch_messages_reply, current_metadata_commit, fetch_section_records,
};
use crate::session_manager::{FetchReadGuard, SessionManager};
use crate::shell::{ShellBus, ShellShard};
use ahash::AHashMap;
use consensus::MetadataHandle;
use consensus::client_table::SessionAttachment;
use futures::future::{Either, join_all, select};
use futures::stream::{FuturesUnordered, StreamExt};
use iggy_binary_protocol::primitives::partition_history::PartitionContext;
use iggy_binary_protocol::primitives::polling_strategy::WirePollingStrategy;
use iggy_binary_protocol::requests::messages::{
    FETCH_REPLY_MARGIN_US, FetchMessagesRequest, MIN_DEFERRED_POLL_MAX_BYTES,
};
use iggy_binary_protocol::responses::messages::FetchMessagesSectionHeader;
use iggy_binary_protocol::{
    KIND_CONSUMER_GROUP, KIND_EXTERNAL_GROUP, PrepareHeader, RoutedRequestHeader, STATUS_OK,
};
use iggy_common::{DeferredPollOptions, EncryptorKind, IggyError, PollingStrategy};
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use message_bus::BusMessage;
use metadata::impls::metadata::StreamsFrontend;
use metadata::permissioner::Permissioner;
use metadata::stm::stream::{PollMetadata, PollTarget};
use partitions::{Fragment, PollFragments, PollingArgs, PollingConsumer};
use server_common::iobuf::Frozen;
use server_common::sharding::IggyNamespace;
use server_common::{MESSAGE_ALIGN, Message};
use shard::metrics::FetchRecord;
use shard::shards_table::ShardsTable;
use shard::{
    CommitGate, DeferredPollContext, DeferredPollRequest, KNOWN_BYTES_SLACK, OversizedTurn,
    PartitionRead, PartitionReadReply, PollQuotaIdentity, ReadinessTarget, ReadyTarget,
};
use std::cell::RefCell;
use std::iter;
use std::pin::pin;
use std::rc::Rc;
use std::time::Duration;
use tracing::error;

#[allow(clippy::cast_possible_truncation)]
const MIN_READ_BYTES: u32 = MIN_DEFERRED_POLL_MAX_BYTES as u32;

/// The partition ids of the targets that a condition of their own refuses,
/// with that condition.
type RefusedTargets = Vec<(u32, IggyError)>;

type Section = (FetchMessagesSectionHeader, Vec<Frozen<MESSAGE_ALIGN>>);

/// Answers a fetch in four steps: resolve the targets once (D9), resolve the
/// start of each `Next` target from its stored offset (D4), wait until the
/// targets hold `min_count` messages, then read the ready targets that the
/// budgets allot (D3, D10, D22).
///
/// The request timeout `T` starts with this future. Each read must commit
/// before `T - 2 x FETCH_REPLY_MARGIN_US`, and the reply leaves at the latest
/// at `T - FETCH_REPLY_MARGIN_US` with the sections that arrived, so a read
/// that committed reaches the client (D6). Only a condition of the caller
/// fails the request (see [`is_caller_condition`]). A condition of one target
/// is its error section, and the other targets go on (D7). A wait that a
/// target condition ends reads every target, so each read reports its own.
#[allow(clippy::future_not_send)]
pub(super) async fn fetch_messages<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    sessions: &Rc<RefCell<SessionManager>>,
    request: &Message<RoutedRequestHeader>,
    (wire, commit_gate): (&FetchMessagesRequest, Option<CommitGate>),
    (consumer_client_id, attachment, user_id): (u128, &SessionAttachment, u32),
) -> Result<(BusMessage, Vec<FetchReadGuard>), IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let started = shard.bus.monotonic_micros();
    let timeout = started.saturating_add(wire.request_timeout_us);
    let (mut targets, mut refused) =
        resolve_fetch(shard, wire, request.header(), consumer_client_id, user_id)?;
    refused.extend(resolve_next_starts(shard, &mut targets).await?);
    let caller = DeferredPollContext {
        session: Some(attachment.clone()),
        user_id,
        user_created_at: None,
        quota: PollQuotaIdentity::Session(consumer_client_id),
        primary: true,
    };
    // The wait ends where the request asked, whatever the stored-offset reads
    // took, and never after the read deadline.
    let wait_end = started
        .saturating_add(shard.deferred_wait_us(wire.wait_us))
        .min(timeout.saturating_sub(2 * FETCH_REPLY_MARGIN_US));
    let ready_targets = if targets.is_empty() {
        Vec::new()
    } else {
        match shard
            .wait_for_messages(
                caller.clone(),
                targets.iter().map(FetchTarget::readiness).collect(),
                Duration::from_micros(wait_end.saturating_sub(shard.bus.monotonic_micros())),
                wire.min_count,
            )
            .await
        {
            Ok(ready) => ready,
            Err(error) if is_caller_condition(&error) => return Err(error),
            Err(_) => targets
                .iter()
                .map(|target| ReadyTarget {
                    partition_id: target.partition_id,
                    available: u64::from(wire.count),
                    known_bytes: None,
                })
                .collect(),
        }
    };
    let fired = shard.bus.monotonic_micros();
    let (allotted, budget_waits) = allot(
        shard,
        sessions,
        &targets,
        &ready_targets,
        wire,
        consumer_client_id,
    );
    // An empty reply would bring the next fetch back at once while the quota is still full.
    if allotted.is_empty() && budget_waits > 0 {
        shard.metrics().record_fetch(&FetchRecord {
            budget_waits,
            ..FetchRecord::default()
        });
        return Err(IggyError::DeferredPollCapacityExceeded);
    }
    let (replies, held_reads) = read_allotted(
        shard,
        &targets,
        allotted,
        (caller, wire.auto_commit, commit_gate),
        timeout,
    )
    .await;
    let served = targets.first().map_or(Ok(()), |target| {
        authorize_served_topic_read(
            shard,
            Some(attachment),
            target.namespace,
            Permissioner::poll_messages,
        )
    });
    let commit = current_metadata_commit(shard);
    let mut fetch = FetchRecord {
        budget_waits,
        ..FetchRecord::default()
    };
    let sections = served_sections(
        fetch_sections(shard, &targets, replies, commit, &mut fetch),
        served,
        wire.auto_commit,
    )?;
    if fetch.bytes == 0
        && fetch.quota_refusals > 0
        && sections
            .iter()
            .all(|(header, _)| header.status == STATUS_OK)
    {
        shard.metrics().record_fetch(&fetch);
        return Err(IggyError::DeferredPollCapacityExceeded);
    }
    let sections = in_request_order(wire, sections, &refused);
    let reply = build_fetch_messages_reply(request.header(), commit, sections)?;
    fetch.fire_to_reply = Duration::from_micros(shard.bus.monotonic_micros().saturating_sub(fired));
    shard.metrics().record_fetch(&fetch);
    Ok((reply, held_reads))
}

/// The sections of a fetch whose caller check after the reads ended with
/// `served`. A session or permission lost during the reads fails the fetch
/// only while no read stored an offset. Otherwise the stored sections reach
/// the client and the others report the loss (D15).
fn served_sections(
    sections: Vec<Section>,
    served: Result<(), IggyError>,
    auto_commit: bool,
) -> Result<Vec<Section>, IggyError> {
    let Err(error) = served else {
        return Ok(sections);
    };
    if !auto_commit
        || sections
            .iter()
            .all(|(header, _)| header.messages_count == 0)
    {
        return Err(error);
    }
    Ok(sections
        .into_iter()
        .map(|section| {
            if section.0.status == STATUS_OK && section.0.messages_count > 0 {
                section
            } else {
                error_section(section.0.partition_id, &error)
            }
        })
        .collect())
}

/// Runs the allotted reads at once and collects the replies that arrive
/// before the reply deadline, by target, with the session room of each
/// answered read settled to the bytes it returned. The reads that did not
/// answer by then are dropped.
#[allow(clippy::future_not_send)]
async fn read_allotted<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    targets: &[FetchTarget],
    allotted: Vec<Read>,
    (caller, auto_commit, commit_gate): (DeferredPollContext, bool, Option<CommitGate>),
    timeout: u64,
) -> (Vec<Option<PartitionReadReply>>, Vec<FetchReadGuard>)
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let read_deadline = timeout.saturating_sub(2 * FETCH_REPLY_MARGIN_US);
    let reply_at = timeout.saturating_sub(FETCH_REPLY_MARGIN_US);
    let mut replies: Vec<Option<PartitionReadReply>> =
        iter::repeat_with(|| None).take(targets.len()).collect();
    let mut oversized_turn = OversizedTurn::default();
    let mut reads = allotted.into_iter();
    let start_read =
        |read: Read, oversized_turn: &OversizedTurn, replies: &mut [Option<PartitionReadReply>]| {
            replies[read.index] = Some(PartitionReadReply::Rejected(
                IggyError::ShardCommunicationError,
            ));
            let target = &targets[read.index];
            // The owner resolves a `Next` start again when it admits the read and refuses a
            // commit once another session stored an offset, as for a deferred poll.
            let strategy = if target.next {
                PollingStrategy::next()
            } else {
                PollingStrategy::offset(target.start)
            };
            let mut poll = DeferredPollRequest::new(
                target.consumer,
                PollingArgs::new(strategy, read.count, auto_commit),
                target.metadata,
                caller.clone(),
                DeferredPollOptions {
                    max_bytes: read.max_bytes,
                    ..Default::default()
                },
            );
            poll.commit_gate.clone_from(&commit_gate);
            poll.oversized_turn = Some(oversized_turn.clone());
            async move {
                let reply = shard
                    .deferred_read(target.namespace, poll, read_deadline)
                    .await;
                (read.index, read.max_bytes, reply, read.reservation)
            }
        };
    let mut pending = FuturesUnordered::new();
    if let Some(read) = reads.next() {
        pending.push(start_read(read, &oversized_turn, &mut replies));
    }
    let mut first_nonempty = false;
    let mut reply_timer = pin!(shard.bus.sleep(Duration::from_micros(
        reply_at.saturating_sub(shard.bus.monotonic_micros())
    )));
    let mut held_reads = Vec::new();
    while let Either::Left((Some((index, max_bytes, reply, reservation)), _)) =
        select(pending.next(), reply_timer.as_mut()).await
    {
        let bytes = match &reply {
            Some(PartitionReadReply::Poll { fragments, .. }) => {
                fragments.iter().map(Fragment::len).sum()
            }
            _ => 0,
        };
        let uncommitted = match &reply {
            Some(PartitionReadReply::Rejected(_)) => true,
            Some(PartitionReadReply::Poll { fragments, .. }) => fragments.is_empty(),
            _ => false,
        };
        if let Some(mut reservation) = reservation {
            reservation.settle(bytes);
            held_reads.push(reservation);
        }
        if let Some(reply) = reply {
            replies[index] = Some(reply);
        }
        if !first_nonempty {
            if bytes > (max_bytes as usize).saturating_sub(KNOWN_BYTES_SLACK as usize) {
                break;
            }
            if bytes > 0 {
                first_nonempty = true;
                pending.extend(
                    reads
                        .by_ref()
                        .map(|read| start_read(read, &oversized_turn, &mut replies)),
                );
            } else if let Some(read) = reads.next() {
                if uncommitted {
                    oversized_turn = OversizedTurn::default();
                }
                pending.push(start_read(read, &oversized_turn, &mut replies));
            }
        }
    }
    (replies, held_reads)
}

/// The sections of a fetch in request order, from the read replies (D4, D7).
/// A served section must still hold the incarnation that its target named.
fn fetch_sections<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    targets: &[FetchTarget],
    replies: Vec<Option<PartitionReadReply>>,
    commit: u64,
    fetch: &mut FetchRecord,
) -> Vec<Section>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let sections: Vec<Section> = targets
        .iter()
        .zip(replies)
        .filter_map(|(target, reply)| {
            let section = match reply {
                Some(PartitionReadReply::Poll {
                    context,
                    fragments,
                    current_offset,
                    encryptor,
                    next_offset,
                }) => target.read_section(
                    PartitionContext {
                        metadata_op: context.metadata_op.max(commit),
                        ..context
                    },
                    fragments,
                    encryptor.as_deref(),
                    current_offset,
                    next_offset,
                ),
                Some(PartitionReadReply::Rejected(IggyError::DeferredPollCapacityExceeded)) => {
                    fetch.quota_refusals += 1;
                    Ok(target.start_section(commit)?)
                }
                Some(PartitionReadReply::Rejected(error)) => Err(error),
                Some(PartitionReadReply::NotFound) => Err(IggyError::TransientNotAccepted),
                Some(_) => Err(IggyError::ShardCommunicationError),
                None => Ok(target.start_section(commit)?),
            };
            let section = section.and_then(|section| {
                authorize_served_incarnation(shard, target.namespace, target.incarnation)
                    .map(|()| section)
            });
            Some(section.unwrap_or_else(|error| error_section(target.partition_id, &error)))
        })
        .collect();
    fetch.sections = sections.len() as u64;
    fetch.bytes = sections
        .iter()
        .map(|(header, _)| u64::from(header.records_len))
        .sum();
    sections
}

/// One target of a fetch, resolved once per request.
struct FetchTarget {
    partition_id: u32,
    namespace: IggyNamespace,
    consumer: PollingConsumer,
    metadata: PollMetadata,
    /// The incarnation that the request names, which a served section must
    /// still hold.
    incarnation: u64,
    max_bytes: u32,
    next: bool,
    start: u64,
    /// The current offset that the stored-offset read of a `Next` target
    /// returned, for the section that carries its start.
    resolved: Option<u64>,
}

impl FetchTarget {
    fn readiness(&self) -> ReadinessTarget {
        ReadinessTarget {
            namespace: self.namespace,
            partition_id: self.partition_id,
            consumer: self.consumer,
            strategy: PollingStrategy::offset(self.start),
            metadata: self.metadata,
        }
    }

    fn read_section(
        &self,
        context: PartitionContext,
        fragments: PollFragments,
        encryptor: Option<&EncryptorKind>,
        current_offset: u64,
        next_offset: Option<u64>,
    ) -> Result<Section, IggyError> {
        // An offset read always reports where the next one starts.
        let Some(next_offset) = next_offset else {
            error!(
                partition_id = self.partition_id,
                "fetch: an offset read returned no next offset"
            );
            return Err(IggyError::ShardCommunicationError);
        };
        let (records, messages_count) = fetch_section_records(fragments, encryptor)?;
        let records_len = u32::try_from(records.iter().map(Frozen::len).sum::<usize>())
            .map_err(|_| IggyError::InvalidCommand)?;
        Ok((
            FetchMessagesSectionHeader {
                partition_id: self.partition_id,
                status: STATUS_OK,
                current_offset,
                messages_count,
                context,
                next_offset,
                records_len,
            },
            records,
        ))
    }

    /// The section of a `Next` target that got no other one: no records, and
    /// its resolved start as the next offset, so the client sends it as an
    /// offset from now on (D4).
    fn start_section(&self, commit: u64) -> Option<Section> {
        Some((
            FetchMessagesSectionHeader {
                partition_id: self.partition_id,
                status: STATUS_OK,
                current_offset: self.resolved?,
                messages_count: 0,
                context: self.metadata.context(commit),
                next_offset: self.start,
                records_len: 0,
            },
            Vec::new(),
        ))
    }
}

/// A read that [`allot`] gave a target.
struct Read {
    index: usize,
    count: u32,
    max_bytes: u32,
    /// `None` for a target that no shard owns, which `deferred_read` refuses.
    reservation: Option<FetchReadGuard>,
}

/// The targets of a fetch that it can read, and those that a condition of
/// their own refuses, with that condition.
#[allow(clippy::cast_possible_truncation)]
fn resolve_fetch<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    wire: &FetchMessagesRequest,
    header: &RoutedRequestHeader,
    consumer_client_id: u128,
    user_id: u32,
) -> Result<(Vec<FetchTarget>, RefusedTargets), IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    if let Some(status) = authorize_partition_read(
        shard,
        &wire.stream_id,
        &wire.topic_id,
        Some(user_id),
        |permissioner, user_id, stream_id, topic_id| {
            permissioner.poll_messages(user_id, stream_id, topic_id)
        },
    ) {
        return Err(IggyError::from_code(status));
    }
    if wire.consumer.kind == KIND_EXTERNAL_GROUP {
        return Err(IggyError::FeatureUnavailable);
    }
    let group = (wire.consumer.kind == KIND_CONSUMER_GROUP).then_some(&wire.consumer.id);
    let consumer = match group {
        Some(_) => None,
        None => Some(polling_consumer_from_wire(&wire.consumer, 0)?),
    };
    let partition_ids: Vec<u32> = wire
        .targets
        .iter()
        .map(|target| target.partition_id)
        .collect();
    let (group_id, resolved) = shard.plane.metadata().mux_stm.streams().poll_targets(
        &wire.stream_id,
        &wire.topic_id,
        group,
        consumer_client_id,
        &partition_ids,
    );
    let commit = current_metadata_commit(shard);
    let mut targets = Vec::with_capacity(wire.targets.len());
    let mut refused = Vec::new();
    for (target, resolved) in wire.targets.iter().zip(resolved) {
        let partition_id = target.partition_id;
        let target = (|| {
            let not_owned =
                IggyError::ConsumerGroupPartitionNotOwned(consumer_client_id, partition_id);
            let (namespace, metadata) = match resolved {
                PollTarget::Pollable {
                    namespace,
                    metadata,
                } => (namespace, metadata),
                PollTarget::NotOwned => return Err(not_owned),
                // The typed error names the level that is missing. The
                // partition can appear between the two reads.
                PollTarget::NotFound => {
                    return Err(resolve_partition_namespace(
                        shard,
                        &wire.stream_id,
                        &wire.topic_id,
                        Some(partition_id),
                    )
                    .map_or_else(|error| error, |_| IggyError::TransientNotAccepted));
                }
                PollTarget::Unavailable => return Err(IggyError::TransientNotAccepted),
            };
            let local = metadata.context(commit);
            fence_poll_context(
                PartitionContext {
                    incarnation: target.incarnation,
                    owner_generation: if target.owner_generation == 0 {
                        local.owner_generation
                    } else {
                        target.owner_generation
                    },
                    metadata_op: header.minimum_metadata_op,
                },
                local,
                consumer_client_id,
                partition_id,
            )?;
            let consumer = match (consumer, group_id) {
                (Some(PollingConsumer::Consumer(id, _)), _) => {
                    PollingConsumer::Consumer(id, partition_id as usize)
                }
                (None, Some(group_id)) => {
                    PollingConsumer::ConsumerGroup(group_id as usize, partition_id as usize)
                }
                // A group that does not resolve owns no partition.
                _ => return Err(not_owned),
            };
            let next = target.strategy.kind == WirePollingStrategy::next().kind;
            Ok(FetchTarget {
                partition_id,
                namespace,
                consumer,
                metadata,
                incarnation: target.incarnation,
                max_bytes: target.max_bytes,
                next,
                start: if next { 0 } else { target.strategy.value },
                resolved: None,
            })
        })();
        match target {
            Ok(target) => targets.push(target),
            Err(error) if is_caller_condition(&error) => return Err(error),
            Err(error) => refused.push((partition_id, error)),
        }
    }
    Ok((targets, refused))
}

/// Resolves each `Next` target to the offset after its stored one, with one
/// stored-offset read per target, all at once (D4). A target whose read is
/// refused leaves `targets` and comes back with its refusal.
#[allow(clippy::future_not_send)]
async fn resolve_next_starts<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    targets: &mut Vec<FetchTarget>,
) -> Result<RefusedTargets, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let replies = join_all(targets.iter().filter(|target| target.next).map(|target| {
        shard.partition_read(
            target.namespace,
            PartitionRead::ConsumerOffsetOnPrimary {
                consumer: Some(target.consumer),
                metadata: target.metadata,
            },
        )
    }))
    .await;
    let mut refused = Vec::new();
    for (target, reply) in targets.iter_mut().filter(|target| target.next).zip(replies) {
        let start = match reply {
            Some(PartitionReadReply::ConsumerOffset {
                stored,
                current_offset,
            }) => stored
                .map_or(Ok(0), |offset| {
                    offset
                        .checked_add(1)
                        .ok_or(IggyError::InvalidOffset(offset))
                })
                .map(|start| (start, current_offset)),
            Some(PartitionReadReply::Rejected(error)) => Err(error),
            Some(PartitionReadReply::NotFound) => Err(IggyError::TransientNotAccepted),
            _ => Err(IggyError::ShardCommunicationError),
        };
        match start {
            Ok((start, current_offset)) => {
                target.start = start;
                target.resolved = Some(current_offset);
            }
            Err(error) if is_caller_condition(&error) => return Err(error),
            Err(error) => refused.push((target.partition_id, error)),
        }
    }
    targets.retain(|target| {
        !refused
            .iter()
            .any(|(partition_id, _)| *partition_id == target.partition_id)
    });
    Ok(refused)
}

/// The reads of a fetch, and the ready targets that wait for a later fetch,
/// in request order (D3, D22, D10).
///
/// [`split_fetch`] gives each ready target its messages and bytes, out of at
/// most `max_read_bytes`, the reply cap of a deferred poll. A target wants its
/// known bytes plus `KNOWN_BYTES_SLACK`, else its cap. The session must also
/// have room for the read's bytes in what this shard holds for it at the
/// target's owner.
fn allot<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    sessions: &Rc<RefCell<SessionManager>>,
    targets: &[FetchTarget],
    ready_targets: &[ReadyTarget],
    wire: &FetchMessagesRequest,
    consumer_client_id: u128,
) -> (Vec<Read>, u64)
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let ready: AHashMap<u32, &ReadyTarget> = ready_targets
        .iter()
        .map(|target| (target.partition_id, target))
        .collect();
    let wants: Vec<ReadWant> = targets
        .iter()
        .enumerate()
        .filter_map(|(index, target)| {
            let readiness = ready.get(&target.partition_id)?;
            let bytes = readiness.known_bytes.map_or(target.max_bytes, |known| {
                u32::try_from(known)
                    .unwrap_or(u32::MAX)
                    .saturating_add(KNOWN_BYTES_SLACK)
                    .min(target.max_bytes)
                    .max(MIN_READ_BYTES)
            });
            Some(ReadWant {
                index,
                available: u32::try_from(readiness.available).unwrap_or(u32::MAX),
                bytes,
            })
        })
        .collect();
    let limits = shard.deferred_poll_config();
    let reply_cap = u32::try_from(limits.max_read_bytes).unwrap_or(u32::MAX);
    let planned = split_fetch(&wants, wire.count, wire.max_bytes.min(reply_cap));
    let mut budget_waits = (wants.len() - planned.len()) as u64;
    let mut allotted = Vec::with_capacity(planned.len());
    for read in planned {
        let target = &targets[read.index];
        let reservation =
            shard
                .shards_table()
                .shard_for(target.namespace)
                .map_or(Some(None), |owner| {
                    SessionManager::reserve_fetch_read(
                        sessions,
                        (consumer_client_id, owner),
                        read.max_bytes as usize,
                        &limits,
                    )
                    .map(Some)
                });
        let Some(reservation) = reservation else {
            budget_waits += 1;
            continue;
        };
        allotted.push(Read {
            index: read.index,
            count: read.count,
            max_bytes: read.max_bytes,
            reservation,
        });
    }
    (allotted, budget_waits)
}

/// What one ready target of a fetch can read: its index among the targets, the
/// messages it holds and the bytes it wants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReadWant {
    index: usize,
    available: u32,
    bytes: u32,
}

/// The messages and bytes that [`split_fetch`] gives one target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PlannedRead {
    index: usize,
    count: u32,
    max_bytes: u32,
}

/// Splits `count` messages and `max_bytes` bytes over `wants`, in request
/// order. First each target gets an equal share of both, at most what it holds
/// or wants and at least [`MIN_READ_BYTES`], so a backlog in one target cannot
/// take the whole fetch. Then the first targets take what is left. A target
/// that the bytes left cannot read waits for a later fetch.
fn split_fetch(wants: &[ReadWant], count: u32, max_bytes: u32) -> Vec<PlannedRead> {
    let Ok(targets) = u32::try_from(wants.len()) else {
        return Vec::new();
    };
    if targets == 0 {
        return Vec::new();
    }
    let count_share = count.div_ceil(targets);
    let bytes_share = (max_bytes / targets).max(MIN_READ_BYTES);
    let mut count_left = count;
    let mut bytes_left = max_bytes;
    let mut planned = Vec::with_capacity(wants.len());
    for want in wants {
        let bytes = want.bytes.min(bytes_share).min(bytes_left);
        if count_left == 0 || bytes < MIN_READ_BYTES {
            continue;
        }
        let read_count = want.available.min(count_share).min(count_left);
        count_left -= read_count;
        bytes_left -= bytes;
        planned.push((
            *want,
            PlannedRead {
                index: want.index,
                count: read_count,
                max_bytes: bytes,
            },
        ));
    }
    planned
        .into_iter()
        .map(|(want, mut read)| {
            let more_count = (want.available - read.count).min(count_left);
            let more_bytes = (want.bytes - read.max_bytes).min(bytes_left);
            count_left -= more_count;
            bytes_left -= more_bytes;
            read.count += more_count;
            read.max_bytes += more_bytes;
            read
        })
        .collect()
}

/// Whether `error` concerns the caller of a fetch rather than one of its
/// targets, so it fails the whole request.
const fn is_caller_condition(error: &IggyError) -> bool {
    matches!(
        error,
        IggyError::Unauthenticated
            | IggyError::StaleClient
            | IggyError::Unauthorized
            | IggyError::InvalidCommand
            | IggyError::FeatureUnavailable
            | IggyError::StreamIdNotFound(..)
            | IggyError::StreamNameNotFound(..)
            | IggyError::TopicIdNotFound(..)
            | IggyError::TopicNameNotFound(..)
    )
}

/// The sections of `served` and the error sections of `refused`, in the order
/// of the targets of `wire`.
fn in_request_order(
    wire: &FetchMessagesRequest,
    served: Vec<Section>,
    refused: &[(u32, IggyError)],
) -> Vec<Section> {
    if refused.is_empty() {
        return served;
    }
    let mut sections: AHashMap<u32, Section> = served
        .into_iter()
        .map(|section| (section.0.partition_id, section))
        .chain(
            refused
                .iter()
                .map(|(partition_id, error)| (*partition_id, error_section(*partition_id, error))),
        )
        .collect();
    wire.targets
        .iter()
        .filter_map(|target| sections.remove(&target.partition_id))
        .collect()
}

fn error_section(partition_id: u32, error: &IggyError) -> Section {
    (
        FetchMessagesSectionHeader {
            partition_id,
            status: error.as_code(),
            current_offset: 0,
            messages_count: 0,
            context: PartitionContext::default(),
            next_offset: 0,
            records_len: 0,
        },
        Vec::new(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::failure::ClientOrigin;
    use crate::dispatch::reads::handle_non_replicated_request;
    use crate::dispatch::test_support::{
        FIRST_BOOT, SpyBus, TestShard, attach_root_session, recreate_topic_in, request_message,
        route_partitions, stored_batch_record, test_shard,
    };
    use crate::reply_frame::build_polled_messages_body;
    use crate::session_manager::ConnectionContext;
    use bytes::Bytes;
    use configs::server::ServerConfig;
    use futures::channel::oneshot;
    use iggy_binary_protocol::codes::FETCH_MESSAGES_CODE;
    use iggy_binary_protocol::requests::messages::{
        FetchMessagesTarget, MIN_DEFERRED_POLL_REPLY_MARGIN_US,
    };
    use iggy_binary_protocol::responses::messages::poll_messages::POLL_RESPONSE_HEADER_SIZE;
    use iggy_binary_protocol::responses::messages::{FetchMessagesResponse, FetchMessagesSection};
    use iggy_binary_protocol::{Operation, ReplyHeader, WireConsumer, WireEncode, WireIdentifier};
    use iggy_common::defaults::DEFAULT_ROOT_USER_ID;
    use iggy_common::{Aes256GcmEncryptor, PollingKind};
    use message_bus::MessageBus;
    use message_bus::installer::conn_info::ClientTransportKind;
    use partitions::Fragment;
    use prometheus_client::encoding::text::encode;
    use prometheus_client::registry::Registry;
    use server_common::iobuf::Owned;
    use server_common::send_messages::{
        COMMAND_HEADER_SIZE, IggyMessage, IggyMessageHeader, IggyMessages, SendMessagesOwned,
    };
    use shard::config::DeferredPollConfig;
    use shard::{LifecycleFrame, ReadinessWait, Receiver, Sender, ShardFrame, shard_channel};
    use std::collections::BTreeMap;
    use std::future::Future;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::Poll;
    use std::time::Instant;

    const CLIENT: u128 = 1;
    const SESSION: u64 = 1;
    const REQUEST: u64 = 1;
    /// The connection that sends a fetch on the deferred path.
    const TRANSPORT: u128 = 91;
    const PEER: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0));
    const CONSUMER_ID: u32 = 1;
    /// Room for every frame that one fetch sends to the owner at once.
    const INBOX_CAPACITY: usize = 16;
    const WAIT_US: u64 = 1_000_000;
    const REQUEST_TIMEOUT_US: u64 = WAIT_US + MIN_DEFERRED_POLL_REPLY_MARGIN_US;
    const MIN_COUNT: u32 = 3;
    const COUNT: u32 = 100;
    const MAX_BYTES: u32 = 1024 * 1024;
    /// Small enough that the request bytes hold a read of every target.
    const TARGET_MAX_BYTES: u32 = 64 * 1024;
    /// The applied metadata commit of the shard.
    const COMMIT: u64 = 17;
    /// The current offset that the owner reports with a served read.
    const CURRENT_OFFSET: u64 = 99;
    /// The messages of [`stored_batch_record`], at offsets 0 and 1.
    const STORED_BATCH_MESSAGES: u32 = 2;

    #[test]
    fn given_ready_targets_with_backlogs_when_split_should_share_count_and_bytes_first() {
        // A backlog target wanted the whole byte limit, so each fetch read one
        // partition while the others waited.
        let mib = 1024 * 1024;
        let wants = [0, 1, 2].map(|index| ReadWant {
            index,
            available: 10_000,
            bytes: 4 * mib,
        });

        let planned = split_fetch(&wants, 900, 4 * mib);

        assert_eq!(
            planned,
            [0, 1, 2]
                .map(|index| PlannedRead {
                    index,
                    count: 300,
                    max_bytes: 4 * mib / 3 + u32::from(index == 0) * (4 * mib % 3),
                })
                .to_vec()
        );
    }

    #[test]
    fn given_a_small_target_beside_a_backlog_when_split_should_give_the_rest_in_request_order() {
        let mib = 1024 * 1024;
        let wants = [
            ReadWant {
                index: 0,
                available: 5,
                bytes: MIN_READ_BYTES,
            },
            ReadWant {
                index: 1,
                available: 10_000,
                bytes: 4 * mib,
            },
        ];

        let planned = split_fetch(&wants, 1000, 4 * mib);

        assert_eq!(
            planned,
            vec![
                PlannedRead {
                    index: 0,
                    count: 5,
                    max_bytes: MIN_READ_BYTES,
                },
                PlannedRead {
                    index: 1,
                    count: 995,
                    max_bytes: 4 * mib - MIN_READ_BYTES,
                },
            ]
        );
    }

    #[test]
    fn given_more_targets_than_bytes_when_split_should_leave_the_last_for_a_later_fetch() {
        let wants = [0, 1, 2].map(|index| ReadWant {
            index,
            available: 10,
            bytes: 2 * MIN_READ_BYTES,
        });

        let planned = split_fetch(&wants, 30, 2 * MIN_READ_BYTES);

        assert_eq!(
            planned.iter().map(|read| read.index).collect::<Vec<_>>(),
            [0, 1],
            "a target that the bytes left cannot read waits"
        );
        assert!(planned.iter().all(|read| read.count == 10));
    }

    #[compio::test]
    async fn given_a_fetch_that_does_not_decode_when_served_should_deny_invalid_command() {
        let fixture = Fixture::new(2);
        let valid = fetch_of(vec![
            fixture.target(0, WirePollingStrategy::offset(0)),
            fixture.target(1, WirePollingStrategy::offset(0)),
        ]);
        let mut truncated = valid.to_bytes().to_vec();
        truncated.pop();
        let mut first = valid.clone();
        first.targets[0].strategy = WirePollingStrategy::first();
        let mut repeated = valid;
        repeated.targets[1].partition_id = repeated.targets[0].partition_id;
        for (body, case) in [
            (truncated, "a truncated body"),
            (first.to_bytes().to_vec(), "a First target"),
            (repeated.to_bytes().to_vec(), "a repeated partition"),
        ] {
            fixture.bus.client_replies.borrow_mut().clear();
            fixture.serve(&body).await;
            let replies = fixture.bus.client_replies.borrow();
            assert_eq!(replies.len(), 1, "{case} gets one reply");
            let (client, frame) = &replies[0];
            assert_eq!(*client, TRANSPORT);
            assert_eq!(
                reply_status(frame),
                IggyError::InvalidCommand.as_code(),
                "{case} is denied as an invalid command"
            );
        }
        assert!(
            fixture.owner_frames().is_empty(),
            "a fetch that does not decode reaches no owner"
        );
    }

    #[compio::test]
    async fn given_a_committing_fetch_queued_past_its_timeout_when_served_should_refuse_it_unread()
    {
        // The timeout ran from the start of the handler, so a frame that waited
        // in the drain after its client gave up still waited, read and stored
        // offsets for a reply nobody took.
        const ARRIVED: u64 = 1_000_000;
        let fixture = Fixture::new(1);
        fixture.sessions.borrow_mut().ensure_connection(
            TRANSPORT,
            PEER,
            ClientTransportKind::Tcp,
            Instant::now(),
        );
        fixture.bus.monotonic.set(Some(ARRIVED));
        let origin = ClientOrigin::capture(&fixture.bus, TRANSPORT);
        fixture
            .bus
            .monotonic
            .set(Some(ARRIVED + REQUEST_TIMEOUT_US));
        let wire = FetchMessagesRequest {
            auto_commit: true,
            ..fetch_of(vec![fixture.target(0, WirePollingStrategy::next())])
        };

        fixture.serve_from(origin, &wire.to_bytes()).await;

        let replies = fixture.bus.client_replies.borrow();
        assert_eq!(replies.len(), 1);
        assert_eq!(
            reply_status(&replies[0].1),
            IggyError::TransientNotAccepted.as_code()
        );
        assert!(
            fixture.owner_frames().is_empty(),
            "a request its client gave up on reaches no owner"
        );
    }

    #[compio::test]
    async fn given_a_fetch_when_served_on_the_deferred_path_should_reply_with_its_sections() {
        let fixture = Fixture::new(1);
        fixture.sessions.borrow_mut().ensure_connection(
            TRANSPORT,
            PEER,
            ClientTransportKind::Tcp,
            Instant::now(),
        );
        let record = stored_batch_record(None);
        let body = fetch_of(vec![fixture.target(0, WirePollingStrategy::offset(0))]).to_bytes();
        let served_request = fixture.serve(&body);
        let mut served_request = pin!(served_request);
        assert!(futures::poll!(served_request.as_mut()).is_pending());
        fixture.answer_wait(vec![ready(0, u64::from(STORED_BATCH_MESSAGES))]);
        assert!(futures::poll!(served_request.as_mut()).is_pending());
        fixture
            .reads()
            .remove(&0)
            .expect("the ready target is read")
            .answer(served(fixture.context(0), whole(&record)));
        served_request.await;

        let replies = fixture.bus.client_replies.borrow();
        assert_eq!(replies.len(), 1, "the fetch gets one reply");
        let (client, frame) = &replies[0];
        assert_eq!(*client, TRANSPORT);
        assert_eq!(reply_status(frame), STATUS_OK);
        let sections = sections(frame);
        let headers: Vec<_> = sections.iter().map(|section| section.header).collect();
        assert_eq!(headers, [fixture.served_header(0, record.len())]);
        assert_eq!(sections[0].records, record.as_slice());
    }

    #[compio::test]
    async fn given_a_target_that_does_not_resolve_when_fetching_should_answer_its_error_section() {
        const MISSING_PARTITION: u32 = 5;
        let fixture = Fixture::new(2);
        let target = |partition_id| fixture.target(partition_id, WirePollingStrategy::offset(0));
        let missing = FetchMessagesTarget {
            partition_id: MISSING_PARTITION,
            ..target(1)
        };
        let wire = fetch_of(vec![target(0), missing]);
        let fetch = fixture.fetch(&wire);
        let mut fetch = pin!(fetch);
        assert!(futures::poll!(fetch.as_mut()).is_pending());

        let wait = fixture.answer_wait(Vec::new());

        assert_eq!(
            wait.targets
                .iter()
                .map(|target| target.partition_id)
                .collect::<Vec<_>>(),
            [0],
            "the other target still waits"
        );
        let Poll::Ready(reply) = futures::poll!(fetch.as_mut()) else {
            panic!("a fetch without ready targets answers at the end of its wait");
        };
        let reply = reply.expect("the fetch succeeds").into_contiguous();
        let statuses: Vec<_> = sections(reply.as_slice())
            .iter()
            .map(|section| section.header.partition_id)
            .collect();
        assert_eq!(
            statuses,
            [MISSING_PARTITION],
            "a missing partition is its own section"
        );

        let stale = FetchMessagesTarget {
            incarnation: fixture.context(1).incarnation + 1,
            ..target(1)
        };
        let wire = fetch_of(vec![target(0), stale]);
        let fetch = fixture.fetch(&wire);
        let mut fetch = pin!(fetch);
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        fixture.answer_wait(Vec::new());
        let Poll::Ready(reply) = futures::poll!(fetch.as_mut()) else {
            panic!("a fetch without ready targets answers at the end of its wait");
        };
        let reply = reply.expect("the fetch succeeds").into_contiguous();
        let statuses: Vec<_> = sections(reply.as_slice())
            .iter()
            .map(|section| (section.header.partition_id, section.header.status))
            .collect();
        assert_eq!(
            statuses,
            [(1, IggyError::HistoryUnavailable.as_code())],
            "a target fenced to another incarnation is its own error section"
        );

        let external = FetchMessagesRequest {
            consumer: WireConsumer::external_group(WireIdentifier::numeric(CONSUMER_ID)),
            ..fetch_of(vec![target(0)])
        };
        assert_eq!(
            fixture.fetch(&external).await.err(),
            Some(IggyError::FeatureUnavailable),
            "an external group reads no partition"
        );
        assert!(
            fixture.owner_frames().is_empty(),
            "a fetch that does not resolve reaches no owner"
        );
    }

    #[compio::test]
    async fn given_a_ready_next_target_when_read_should_let_the_owner_resolve_and_fence_its_start()
    {
        // Every read went out as an offset, so the owner never checked the
        // stored offset again and a committing read overwrote an offset that
        // another session stored during the wait.
        const STORED: u64 = 41;
        let fixture = Fixture::new(1);
        let wire = FetchMessagesRequest {
            auto_commit: true,
            ..fetch_of(vec![fixture.target(0, WirePollingStrategy::next())])
        };
        let fetch = fixture.fetch(&wire);
        let mut fetch = pin!(fetch);
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        fixture.answer_stored_offsets(BTreeMap::from([(
            0,
            PartitionReadReply::ConsumerOffset {
                stored: Some(STORED),
                current_offset: STORED + 10,
            },
        )]));
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        fixture.answer_wait(vec![ready(0, 10)]);
        assert!(futures::poll!(fetch.as_mut()).is_pending());

        let reads = fixture.reads();

        assert_eq!(reads[&0].request.args.kind, PollingKind::Next);
        assert!(reads[&0].request.args.auto_commit);
    }

    #[compio::test]
    async fn given_next_targets_when_no_target_is_ready_should_answer_each_at_its_resolved_start() {
        const STORED: u64 = 41;
        const STORED_CURRENT: u64 = 50;
        const UNSTORED_CURRENT: u64 = 7;
        const OFFSET_START: u64 = 3;
        let fixture = Fixture::new(3);
        let wire = fetch_of(vec![
            fixture.target(0, WirePollingStrategy::next()),
            fixture.target(1, WirePollingStrategy::next()),
            fixture.target(2, WirePollingStrategy::offset(OFFSET_START)),
        ]);
        let fetch = fixture.fetch(&wire);
        let mut fetch = pin!(fetch);
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        fixture.answer_stored_offsets(BTreeMap::from([
            (
                0,
                PartitionReadReply::ConsumerOffset {
                    stored: Some(STORED),
                    current_offset: STORED_CURRENT,
                },
            ),
            (
                1,
                PartitionReadReply::ConsumerOffset {
                    stored: None,
                    current_offset: UNSTORED_CURRENT,
                },
            ),
        ]));
        assert!(futures::poll!(fetch.as_mut()).is_pending());

        let wait = fixture.answer_wait(Vec::new());
        let starts: Vec<_> = wait
            .targets
            .iter()
            .map(|target| (target.partition_id, target.strategy))
            .collect();
        assert_eq!(
            starts,
            [
                (0, PollingStrategy::offset(STORED + 1)),
                (1, PollingStrategy::offset(0)),
                (2, PollingStrategy::offset(OFFSET_START)),
            ],
            "the wait names each next target at the offset after its stored one"
        );
        assert_eq!(wait.min_count, MIN_COUNT);
        let Poll::Ready(reply) = futures::poll!(fetch.as_mut()) else {
            panic!("a fetch without ready targets answers at the end of its wait");
        };
        let reply = reply.expect("the fetch succeeds").into_contiguous();
        let start = |partition_id, current_offset, next_offset| FetchMessagesSectionHeader {
            partition_id,
            status: STATUS_OK,
            current_offset,
            messages_count: 0,
            context: fixture.context(partition_id),
            next_offset,
            records_len: 0,
        };
        let headers: Vec<_> = sections(reply.as_slice())
            .iter()
            .map(|section| section.header)
            .collect();
        assert_eq!(
            headers,
            [
                start(0, STORED_CURRENT, STORED + 1),
                start(1, UNSTORED_CURRENT, 0)
            ],
            "each next target gets a section at its resolved start, an offset target none"
        );
        assert!(
            fixture.owner_frames().is_empty(),
            "a target that is not ready gets no read"
        );
    }

    #[compio::test]
    async fn given_ready_targets_when_the_count_is_short_should_share_it_before_the_rest_in_order()
    {
        const FETCH_COUNT: u32 = 5;
        const FIRST_AVAILABLE: u32 = 3;
        const STARTS: [u64; 3] = [10, 20, 30];
        let fixture = Fixture::new(3);
        let wire = FetchMessagesRequest {
            count: FETCH_COUNT,
            auto_commit: true,
            ..fetch_of(vec![
                fixture.target(0, WirePollingStrategy::offset(STARTS[0])),
                fixture.target(1, WirePollingStrategy::offset(STARTS[1])),
                fixture.target(2, WirePollingStrategy::offset(STARTS[2])),
            ])
        };
        let fetch = fixture.fetch(&wire);
        let mut fetch = pin!(fetch);
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        fixture.answer_wait(vec![
            ready(0, u64::from(FIRST_AVAILABLE)),
            // More messages than the count of one read can name.
            ready(1, u64::from(u32::MAX) + 1),
            ready(2, 1),
        ]);
        assert!(futures::poll!(fetch.as_mut()).is_pending());

        let reads = fixture.refuse_reads(fetch.as_mut(), 3).await;
        let allotments: BTreeMap<_, _> = reads
            .iter()
            .map(|(partition_id, read)| (*partition_id, read.allotment()))
            .collect();
        assert_eq!(
            allotments,
            BTreeMap::from([
                (0, (2, STARTS[0], TARGET_MAX_BYTES)),
                (1, (2, STARTS[1], TARGET_MAX_BYTES)),
                (2, (1, STARTS[2], TARGET_MAX_BYTES)),
            ]),
            "each ready target gets an equal share of the count, at most what it holds"
        );
        for (partition_id, read) in &reads {
            assert_eq!(
                read.request.args.kind,
                PollingKind::Offset,
                "a read starts at its target offset"
            );
            assert!(
                read.request.args.auto_commit,
                "a read commits as the fetch asks"
            );
            assert_eq!(
                read.request.consumer,
                offset_key(*partition_id),
                "a read commits under the offset key of its target"
            );
            assert_eq!(
                read.request.context.quota,
                PollQuotaIdentity::Session(CLIENT),
                "a read is charged to the session"
            );
        }
        drop(reads);
        let Poll::Ready(reply) = futures::poll!(fetch.as_mut()) else {
            panic!("the fetch answers once its reads ended");
        };
        assert!(reply.is_ok());
        assert_scraped(&fixture.shard, "fetch_budget_waits_total 0");
    }

    #[compio::test]
    async fn given_ready_targets_when_reserving_bytes_should_share_them_before_the_rest_in_order() {
        const CAP: u32 = 2048;
        const NARROW_CAP: u32 = 1024;
        // Below the smallest read, also with the slack.
        const SMALL_KNOWN: u64 = 100;
        // Above `NARROW_CAP`.
        const LARGE_KNOWN: u64 = 10_000;
        const FITTING_KNOWN: u32 = 500;
        let fitting = FITTING_KNOWN + KNOWN_BYTES_SLACK;
        let fixture = Fixture::new(5);
        let capped = |partition_id: u32, max_bytes: u32| FetchMessagesTarget {
            max_bytes,
            ..fixture.target(partition_id, WirePollingStrategy::offset(0))
        };
        let max_bytes = MIN_READ_BYTES + NARROW_CAP + CAP + fitting;
        let share = max_bytes / 5;
        let wire = FetchMessagesRequest {
            max_bytes,
            ..fetch_of(vec![
                capped(0, CAP),
                capped(1, NARROW_CAP),
                capped(2, CAP),
                capped(3, CAP),
                capped(4, CAP),
            ])
        };
        let known = |partition_id, known_bytes| ReadyTarget {
            partition_id,
            available: 1,
            known_bytes: Some(known_bytes),
        };
        let fetch = fixture.fetch(&wire);
        let mut fetch = pin!(fetch);
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        fixture.answer_wait(vec![
            known(0, SMALL_KNOWN),
            known(1, LARGE_KNOWN),
            ready(2, 1),
            ready(3, 1),
            known(4, u64::from(FITTING_KNOWN)),
        ]);
        assert!(futures::poll!(fetch.as_mut()).is_pending());

        let caps: BTreeMap<_, _> = fixture
            .refuse_reads(fetch.as_mut(), 5)
            .await
            .iter()
            .map(|(partition_id, read)| (*partition_id, read.request.options.max_bytes))
            .collect();
        // Each target gets at most an equal share first: known bytes and the
        // slack within the target cap and above the smallest read, other
        // targets their cap. The first ones in request order take the rest.
        let left = max_bytes - (MIN_READ_BYTES + 3 * share + fitting);
        let narrow_more = NARROW_CAP - share;
        assert_eq!(
            caps,
            BTreeMap::from([
                (0, MIN_READ_BYTES),
                (1, NARROW_CAP),
                (2, share + left - narrow_more),
                (3, share),
                (4, fitting)
            ]),
        );
        let Poll::Ready(reply) = futures::poll!(fetch.as_mut()) else {
            panic!("the fetch answers once its reads ended");
        };
        assert!(reply.is_ok());
        assert_scraped(&fixture.shard, "fetch_budget_waits_total 0");
    }

    #[compio::test]
    async fn given_a_committing_fetch_whose_session_ends_during_its_reads_should_keep_the_stored_sections()
     {
        // The check after the reads failed the whole fetch, so a section whose
        // offset its read already stored never reached the client.
        let fixture = Fixture::new(2);
        let record = stored_batch_record(None);
        let wire = FetchMessagesRequest {
            auto_commit: true,
            ..fetch_of(vec![
                fixture.target(0, WirePollingStrategy::offset(0)),
                fixture.target(1, WirePollingStrategy::offset(0)),
            ])
        };
        let fetch = fixture.fetch(&wire);
        let mut fetch = pin!(fetch);
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        fixture.answer_wait(vec![ready(0, 1), ready(1, 1)]);
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        let mut reads = fixture.reads();
        reads
            .remove(&0)
            .expect("the ready target is read")
            .answer(served(fixture.context(0), whole(&record)));
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        reads.extend(fixture.reads());
        reads
            .remove(&1)
            .expect("the ready target is read")
            .answer(served(fixture.context(1), PollFragments::new()));
        assert!(
            fixture
                .shard
                .plane
                .metadata()
                .client_table
                .borrow_mut()
                .end_session(CLIENT, DEFAULT_ROOT_USER_ID, SESSION, SESSION + 1)
        );

        let Poll::Ready(reply) = futures::poll!(fetch.as_mut()) else {
            panic!("the fetch answers once its reads answered");
        };
        let reply = reply
            .expect("a fetch whose reads stored offsets answers with them")
            .into_contiguous();
        let statuses: Vec<_> = sections(reply.as_slice())
            .iter()
            .map(|section| {
                (
                    section.header.partition_id,
                    section.header.status,
                    section.header.messages_count,
                )
            })
            .collect();
        assert_eq!(
            statuses,
            [
                (0, STATUS_OK, STORED_BATCH_MESSAGES),
                (1, IggyError::StaleClient.as_code(), 0),
            ]
        );
    }

    #[compio::test]
    async fn given_a_plain_fetch_whose_session_ends_during_its_reads_should_fail_whole() {
        let fixture = Fixture::new(1);
        let record = stored_batch_record(None);
        let wire = fetch_of(vec![fixture.target(0, WirePollingStrategy::offset(0))]);
        let fetch = fixture.fetch(&wire);
        let mut fetch = pin!(fetch);
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        fixture.answer_wait(vec![ready(0, 1)]);
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        fixture
            .reads()
            .remove(&0)
            .expect("the ready target is read")
            .answer(served(fixture.context(0), whole(&record)));
        assert!(
            fixture
                .shard
                .plane
                .metadata()
                .client_table
                .borrow_mut()
                .end_session(CLIENT, DEFAULT_ROOT_USER_ID, SESSION, SESSION + 1)
        );

        let Poll::Ready(reply) = futures::poll!(fetch.as_mut()) else {
            panic!("the fetch answers once its read answered");
        };
        assert_eq!(
            reply.err(),
            Some(IggyError::StaleClient),
            "a read that stored nothing needs no reply"
        );
    }

    #[compio::test]
    async fn given_slow_stored_offset_reads_when_waiting_should_end_the_wait_when_the_request_wait_ends()
     {
        // The wait got its whole time after the stored-offset reads, so a slow
        // owner pushed its end past the read deadline.
        const STARTED: u64 = 1_000_000;
        const READS_TOOK: u64 = 300_000;
        let fixture = Fixture::new(1);
        fixture.bus.monotonic.set(Some(STARTED));
        let wire = fetch_of(vec![fixture.target(0, WirePollingStrategy::next())]);
        let fetch = fixture.fetch(&wire);
        let mut fetch = pin!(fetch);
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        fixture.bus.monotonic.set(Some(STARTED + READS_TOOK));
        fixture.answer_stored_offsets(BTreeMap::from([(
            0,
            PartitionReadReply::ConsumerOffset {
                stored: Some(0),
                current_offset: 0,
            },
        )]));
        assert!(futures::poll!(fetch.as_mut()).is_pending());

        let wait = fixture.answer_wait(Vec::new());
        assert_eq!(wait.deadline, STARTED + WAIT_US);
    }

    #[compio::test]
    async fn given_a_delayed_first_section_when_fetching_should_resolve_it_before_later_reads() {
        let fixture = Fixture::new(4);
        let record = stored_batch_record(None);
        let wire = fetch_of(
            (0..4)
                .map(|partition| fixture.target(partition, WirePollingStrategy::offset(0)))
                .collect(),
        );
        let fetch = fixture.fetch(&wire);
        let mut fetch = pin!(fetch);
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        fixture.answer_wait((0..4).map(|partition| ready(partition, 1)).collect());
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        let mut first = fixture.reads();
        assert_eq!(first.keys().copied().collect::<Vec<_>>(), [0]);
        first
            .remove(&0)
            .unwrap()
            .answer(served(fixture.context(0), PollFragments::new()));

        assert!(futures::poll!(fetch.as_mut()).is_pending());
        let mut second = fixture.reads();
        assert_eq!(second.keys().copied().collect::<Vec<_>>(), [1]);
        second
            .remove(&1)
            .unwrap()
            .answer(served(fixture.context(1), whole(&record)));

        assert!(futures::poll!(fetch.as_mut()).is_pending());
        let remaining = fixture.reads();
        assert_eq!(remaining.keys().copied().collect::<Vec<_>>(), [2, 3]);
        for (partition, read) in remaining {
            read.answer(served(
                fixture.context(u32::try_from(partition).unwrap()),
                whole(&record),
            ));
        }
        let Poll::Ready(reply) = futures::poll!(fetch.as_mut()) else {
            panic!("all reads answered");
        };
        let reply = reply.unwrap().into_contiguous();
        let count: u32 = sections(reply.as_slice())
            .iter()
            .map(|section| section.header.messages_count)
            .sum();
        assert_eq!(count, 3 * STORED_BATCH_MESSAGES);
    }

    #[compio::test]
    async fn given_a_missing_first_reply_after_claiming_its_turn_when_fetching_should_keep_the_exception_closed()
     {
        let fixture = Fixture::new(2);
        let wire = fetch_of(vec![
            fixture.target(0, WirePollingStrategy::offset(0)),
            fixture.target(1, WirePollingStrategy::offset(0)),
        ]);
        let fetch = fixture.fetch(&wire);
        let mut fetch = pin!(fetch);
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        fixture.answer_wait(vec![ready(0, 1), ready(1, 1)]);
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        let first = fixture.reads().remove(&0).unwrap();
        assert!(first.request.oversized_turn.as_ref().unwrap().take());
        drop(first);

        assert!(futures::poll!(fetch.as_mut()).is_pending());
        let next = fixture.reads().remove(&1).unwrap();
        assert!(!next.request.oversized_turn.as_ref().unwrap().take());
        next.answer(PartitionReadReply::Rejected(
            IggyError::DeferredPollCapacityExceeded,
        ));
        let Poll::Ready(reply) = futures::poll!(fetch.as_mut()) else {
            panic!("both reads ended");
        };
        let reply = reply
            .expect("capacity must not hide an uncertain read")
            .into_contiguous();
        assert_eq!(
            sections(reply.as_slice())[0].header.status,
            IggyError::ShardCommunicationError.as_code()
        );
    }

    #[compio::test]
    async fn given_a_missing_owner_reply_when_fetching_should_report_uncertainty_only_for_the_started_target()
     {
        for timeout in [false, true] {
            for auto_commit in [false, true] {
                for next in [false, true] {
                    let fixture = Fixture::new(3);
                    fixture.bus.monotonic.set(Some(1_000_000));
                    let strategy = if next {
                        WirePollingStrategy::next()
                    } else {
                        WirePollingStrategy::offset(0)
                    };
                    let wire = FetchMessagesRequest {
                        auto_commit,
                        ..fetch_of(vec![
                            fixture.target(0, WirePollingStrategy::offset(0)),
                            fixture.target(1, strategy),
                            fixture.target(2, WirePollingStrategy::next()),
                        ])
                    };
                    let fetch = fixture.fetch(&wire);
                    let mut fetch = pin!(fetch);
                    assert!(futures::poll!(fetch.as_mut()).is_pending());
                    let mut stored = BTreeMap::from([(
                        2,
                        PartitionReadReply::ConsumerOffset {
                            stored: Some(2),
                            current_offset: 9,
                        },
                    )]);
                    if next {
                        stored.insert(
                            1,
                            PartitionReadReply::ConsumerOffset {
                                stored: Some(2),
                                current_offset: 9,
                            },
                        );
                    }
                    fixture.answer_stored_offsets(stored);
                    assert!(futures::poll!(fetch.as_mut()).is_pending());
                    fixture.answer_wait(vec![ready(0, 2), ready(1, 1)]);
                    let (deadline, held) = oneshot::channel();
                    *fixture.bus.held_timer.borrow_mut() = Some(held);
                    assert!(futures::poll!(fetch.as_mut()).is_pending());
                    fixture.reads().remove(&0).unwrap().answer(served(
                        fixture.context(0),
                        whole(&stored_batch_record(None)),
                    ));
                    assert!(futures::poll!(fetch.as_mut()).is_pending());
                    let missing = fixture.reads().remove(&1).unwrap();
                    let held_read = if timeout {
                        deadline.send(()).unwrap();
                        Some(missing)
                    } else {
                        drop(missing);
                        None
                    };
                    let Poll::Ready(reply) = futures::poll!(fetch.as_mut()) else {
                        panic!("the missing reply ends or reaches its deadline");
                    };
                    let reply = reply.unwrap().into_contiguous();
                    let sections = sections(reply.as_slice());
                    let statuses: Vec<_> = sections
                        .iter()
                        .map(|section| {
                            (
                                section.header.partition_id,
                                section.header.status,
                                section.header.messages_count,
                            )
                        })
                        .collect();
                    assert_eq!(
                        statuses,
                        [
                            (0, STATUS_OK, STORED_BATCH_MESSAGES),
                            (1, IggyError::ShardCommunicationError.as_code(), 0),
                            (2, STATUS_OK, 0)
                        ],
                        "timeout={timeout}, auto_commit={auto_commit}, next={next}"
                    );
                    assert_eq!(sections[2].header.next_offset, 3);
                    assert!(fixture.owner_frames().is_empty());
                    drop(held_read);
                }
            }
        }
    }

    #[compio::test]
    async fn given_a_first_read_refused_after_claiming_its_turn_when_fetching_should_preserve_the_exception()
     {
        let fixture = Fixture::new(2);
        let wire = fetch_of(vec![
            fixture.target(0, WirePollingStrategy::offset(0)),
            fixture.target(1, WirePollingStrategy::offset(0)),
        ]);
        let fetch = fixture.fetch(&wire);
        let mut fetch = pin!(fetch);
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        fixture.answer_wait(vec![ready(0, 1), ready(1, 1)]);
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        let first = fixture.reads().remove(&0).unwrap();
        assert!(first.request.oversized_turn.as_ref().unwrap().take());
        first.answer(PartitionReadReply::Rejected(IggyError::NotPrimary));

        assert!(futures::poll!(fetch.as_mut()).is_pending());
        let next = fixture.reads().remove(&1).unwrap();
        assert!(next.request.oversized_turn.as_ref().unwrap().take());
        next.answer(served(
            fixture.context(1),
            whole(&stored_batch_record(None)),
        ));
        let Poll::Ready(reply) = futures::poll!(fetch.as_mut()) else {
            panic!("both reads answered");
        };
        assert!(reply.is_ok());
    }

    #[compio::test]
    async fn given_an_oversized_first_section_when_fetching_should_leave_later_targets_unread() {
        let fixture = Fixture::new(3);
        let cap = 1024;
        fixture.shard.set_deferred_poll_config(DeferredPollConfig {
            max_read_bytes: cap as usize,
            ..Default::default()
        });
        let mut messages = IggyMessages::with_capacity(1);
        messages.push(IggyMessage {
            header: IggyMessageHeader::default(),
            payload: Bytes::from(vec![42; 4096]),
            user_headers: None,
        });
        let batch = SendMessagesOwned::from_messages(fixture.namespaces[0], &messages).unwrap();
        let mut bytes = vec![0; batch.header.total_size()];
        batch.header.encode_into(&mut bytes);
        bytes[COMMAND_HEADER_SIZE..].copy_from_slice(&batch.blob);
        let record: Frozen<MESSAGE_ALIGN> = Owned::copy_from_slice(&bytes).into();
        let wire = FetchMessagesRequest {
            max_bytes: cap,
            auto_commit: true,
            ..fetch_of(vec![
                fixture.target(0, WirePollingStrategy::offset(0)),
                fixture.target(1, WirePollingStrategy::next()),
                fixture.target(2, WirePollingStrategy::offset(0)),
            ])
        };
        let fetch = fixture.fetch_holding(&wire);
        let mut fetch = pin!(fetch);
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        fixture.answer_stored_offsets(BTreeMap::from([(
            1,
            PartitionReadReply::ConsumerOffset {
                stored: Some(4),
                current_offset: 9,
            },
        )]));
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        fixture.answer_wait((0..3).map(|partition| ready(partition, 1)).collect());
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        let mut reads = fixture.reads();
        assert_eq!(reads.keys().copied().collect::<Vec<_>>(), [0]);
        reads
            .remove(&0)
            .unwrap()
            .answer(served(fixture.context(0), whole(&record)));
        let Poll::Ready(reply) = futures::poll!(fetch.as_mut()) else {
            panic!("the oversized record completes the fetch alone");
        };
        let (reply, held) = reply.unwrap();
        let reply = reply.into_contiguous();
        let sections = sections(reply.as_slice());
        assert_eq!(sections.len(), 2);
        assert_eq!(sections[0].header.messages_count, 1);
        assert_eq!(sections[0].records.len(), record.len());
        assert!(record.len() > cap as usize);
        assert_eq!(sections[1].header.messages_count, 0);
        assert_eq!(sections[1].header.next_offset, 5);
        assert_eq!(held.len(), 1);
        assert!(fixture.owner_frames().is_empty());
    }

    #[compio::test]
    async fn given_a_session_read_cap_when_targets_share_an_owner_should_read_up_to_the_cap() {
        assert_one_read_under(|| DeferredPollConfig {
            max_pending_per_session: 1,
            ..DeferredPollConfig::default()
        })
        .await;
    }

    #[compio::test]
    async fn given_a_session_byte_budget_when_targets_share_an_owner_should_read_what_it_holds() {
        assert_one_read_under(|| DeferredPollConfig {
            max_inflight_bytes: TARGET_MAX_BYTES as usize,
            ..DeferredPollConfig::default()
        })
        .await;
    }

    #[compio::test]
    async fn given_a_fetch_above_the_reply_cap_when_split_should_read_at_most_the_cap() {
        // A 109 reply held up to the request bytes, while a 106 reply stops at
        // the server read cap.
        let fixture = Fixture::new(2);
        fixture.shard.set_deferred_poll_config(DeferredPollConfig {
            max_read_bytes: TARGET_MAX_BYTES as usize,
            ..DeferredPollConfig::default()
        });
        let wire = fetch_of(vec![
            fixture.target(0, WirePollingStrategy::offset(0)),
            fixture.target(1, WirePollingStrategy::offset(0)),
        ]);
        let fetch = fixture.fetch(&wire);
        let mut fetch = pin!(fetch);
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        fixture.answer_wait(vec![ready(0, 1), ready(1, 1)]);
        assert!(futures::poll!(fetch.as_mut()).is_pending());

        let bytes: Vec<u32> = fixture
            .refuse_reads(fetch.as_mut(), 2)
            .await
            .values()
            .map(|read| read.allotment().2)
            .collect();
        assert_eq!(bytes, [TARGET_MAX_BYTES / 2, TARGET_MAX_BYTES / 2]);
    }

    #[compio::test]
    async fn given_an_answered_read_when_its_reply_is_held_should_charge_what_it_returned_until_dropped()
     {
        // A read was charged the cost of a disk read, so a session room of one
        // reply could not hold even a small resident read.
        const OWNER: u16 = 0;
        let fixture = Fixture::new(1);
        let limits = DeferredPollConfig {
            max_inflight_bytes: TARGET_MAX_BYTES as usize,
            ..DeferredPollConfig::default()
        };
        fixture.shard.set_deferred_poll_config(limits);
        let record = stored_batch_record(None);
        let wire = fetch_of(vec![fixture.target(0, WirePollingStrategy::offset(0))]);
        let fetch = fixture.fetch_holding(&wire);
        let mut fetch = pin!(fetch);
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        fixture.answer_wait(vec![ready(0, 1)]);
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        fixture
            .reads()
            .remove(&0)
            .expect("the ready target is read")
            .answer(served(fixture.context(0), whole(&record)));
        let Poll::Ready(reply) = futures::poll!(fetch.as_mut()) else {
            panic!("the fetch answers once its read answered");
        };
        let (_reply, held_reads) = reply.expect("the fetch succeeds");

        let room = TARGET_MAX_BYTES as usize - record.len();
        let reserve = |cost| {
            SessionManager::reserve_fetch_read(&fixture.sessions, (CLIENT, OWNER), cost, &limits)
        };
        assert!(
            reserve(room + 1).is_none(),
            "the held reply keeps the bytes it returned"
        );
        assert!(
            reserve(room).is_some(),
            "the held reply keeps only the bytes it returned"
        );
        drop(held_reads);
        assert!(
            reserve(TARGET_MAX_BYTES as usize).is_some(),
            "a written reply gives its room back"
        );
    }

    /// Two ready targets on one owner, under `limits` that hold one read of
    /// the session there. The second target waits for a later fetch, and the
    /// read gives its room back when it ends.
    async fn assert_one_read_under(limits: impl FnOnce() -> DeferredPollConfig) {
        const OWNER: u16 = 0;
        let fixture = Fixture::new(2);
        let limits = limits();
        fixture.shard.set_deferred_poll_config(limits);
        let cost = TARGET_MAX_BYTES as usize;
        let wire = fetch_of(vec![
            fixture.target(0, WirePollingStrategy::offset(0)),
            fixture.target(1, WirePollingStrategy::offset(0)),
        ]);
        let fetch = fixture.fetch(&wire);
        let mut fetch = pin!(fetch);
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        fixture.answer_wait(vec![ready(0, 1), ready(1, 1)]);
        assert!(futures::poll!(fetch.as_mut()).is_pending());

        let reads = fixture.reads();
        assert_eq!(
            reads.keys().copied().collect::<Vec<_>>(),
            [0],
            "the first target in request order takes the only read"
        );
        assert!(
            SessionManager::reserve_fetch_read(&fixture.sessions, (CLIENT, OWNER), cost, &limits)
                .is_none(),
            "the running read holds the room"
        );
        drop(reads);
        let Poll::Ready(reply) = futures::poll!(fetch.as_mut()) else {
            panic!("the fetch answers once its read ended");
        };
        assert!(reply.is_ok());
        assert!(
            SessionManager::reserve_fetch_read(&fixture.sessions, (CLIENT, OWNER), cost, &limits)
                .is_some(),
            "an ended read gives its room back"
        );
        assert_scraped(&fixture.shard, "fetch_budget_waits_total 1");
    }

    #[test]
    fn given_fetch_reads_of_a_session_when_reserving_should_stop_at_the_read_cap_and_the_byte_budget()
     {
        const COST: usize = 1024;
        const BUDGET: usize = 3 * COST;
        const READ_CAP: usize = 2;
        const OWNER: u16 = 0;
        const OTHER_OWNER: u16 = 1;
        const OTHER_CLIENT: u128 = CLIENT + 1;
        let sessions = Rc::new(RefCell::new(SessionManager::new()));
        let limits = DeferredPollConfig {
            max_pending_per_session: READ_CAP,
            max_inflight_bytes: BUDGET,
            ..DeferredPollConfig::default()
        };
        let reserve = |key, cost| SessionManager::reserve_fetch_read(&sessions, key, cost, &limits);

        let first = reserve((CLIENT, OWNER), COST).expect("a first read fits");
        let second = reserve((CLIENT, OWNER), COST).expect("a second read fits");
        assert!(
            reserve((CLIENT, OWNER), COST).is_none(),
            "the read cap of the session on its owner is reached"
        );
        assert!(
            reserve((CLIENT, OTHER_OWNER), COST).is_some(),
            "another owner counts its own reads"
        );
        assert!(
            reserve((OTHER_CLIENT, OWNER), COST).is_some(),
            "another session counts its own reads"
        );

        drop(first);
        assert!(
            reserve((CLIENT, OWNER), BUDGET - COST + 1).is_none(),
            "a read past the byte budget is refused below the read cap"
        );
        assert!(
            reserve((CLIENT, OWNER), usize::MAX).is_none(),
            "a cost that overflows the running bytes is refused"
        );
        let third =
            reserve((CLIENT, OWNER), BUDGET - COST).expect("a read that fills the budget fits");
        drop((second, third));
        assert!(
            reserve((CLIENT, OWNER), BUDGET).is_some(),
            "ended reads give the whole budget back"
        );
    }

    #[compio::test]
    async fn given_quota_refusals_of_every_read_when_reading_should_answer_capacity_exceeded() {
        // An empty reply brought the next fetch back at once while the owner
        // quota was still full.
        const STORED: u64 = 4;
        const STORED_CURRENT: u64 = 9;
        let fixture = Fixture::new(2);
        let wire = fetch_of(vec![
            fixture.target(0, WirePollingStrategy::offset(0)),
            fixture.target(1, WirePollingStrategy::next()),
        ]);
        let fetch = fixture.fetch(&wire);
        let mut fetch = pin!(fetch);
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        fixture.answer_stored_offsets(BTreeMap::from([(
            1,
            PartitionReadReply::ConsumerOffset {
                stored: Some(STORED),
                current_offset: STORED_CURRENT,
            },
        )]));
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        fixture.answer_wait(vec![ready(0, 1), ready(1, 1)]);
        assert!(futures::poll!(fetch.as_mut()).is_pending());

        for partition in 0..2 {
            let mut reads = fixture.reads();
            assert_eq!(reads.len(), 1);
            reads
                .remove(&partition)
                .unwrap()
                .answer(PartitionReadReply::Rejected(
                    IggyError::DeferredPollCapacityExceeded,
                ));
            if partition == 0 {
                assert!(futures::poll!(fetch.as_mut()).is_pending());
            }
        }
        let Poll::Ready(reply) = futures::poll!(fetch.as_mut()) else {
            panic!("the fetch answers once its reads answered");
        };
        assert_eq!(
            reply.err(),
            Some(IggyError::DeferredPollCapacityExceeded),
            "a fetch that read nothing for quota tells the client to back off"
        );
        assert_scraped(&fixture.shard, "fetch_quota_refusals_total 2");
    }

    #[compio::test]
    async fn given_a_refused_stored_offset_read_when_resolving_next_should_refuse_only_that_target()
    {
        // One target that moved or vanished failed the whole fetch, so every
        // other partition of the group lost its turn too.
        for (answer, expected) in [
            (
                PartitionReadReply::Rejected(IggyError::NotPrimary),
                IggyError::NotPrimary,
            ),
            (
                PartitionReadReply::NotFound,
                IggyError::TransientNotAccepted,
            ),
        ] {
            let fixture = Fixture::new(2);
            let wire = fetch_of(vec![
                fixture.target(0, WirePollingStrategy::offset(0)),
                fixture.target(1, WirePollingStrategy::next()),
            ]);
            let fetch = fixture.fetch(&wire);
            let mut fetch = pin!(fetch);
            assert!(futures::poll!(fetch.as_mut()).is_pending());
            fixture.answer_stored_offsets(BTreeMap::from([(1, answer)]));
            assert!(futures::poll!(fetch.as_mut()).is_pending());

            let wait = fixture.answer_wait(Vec::new());

            assert_eq!(
                wait.targets
                    .iter()
                    .map(|target| target.partition_id)
                    .collect::<Vec<_>>(),
                [0],
                "the other target still waits"
            );
            let Poll::Ready(reply) = futures::poll!(fetch.as_mut()) else {
                panic!("a fetch without ready targets answers at the end of its wait");
            };
            let reply = reply.expect("the fetch succeeds").into_contiguous();
            let statuses: Vec<_> = sections(reply.as_slice())
                .iter()
                .map(|section| (section.header.partition_id, section.header.status))
                .collect();
            assert_eq!(statuses, [(1, expected.as_code())]);
        }
    }

    #[compio::test]
    async fn given_a_wait_refused_for_a_target_when_waiting_should_read_every_target() {
        // A target that changed its history failed the wait of all of them,
        // and the fetch failed without a partition, so no client could tell
        // which one to move on.
        let fixture = Fixture::new(2);
        let wire = fetch_of(vec![
            fixture.target(0, WirePollingStrategy::offset(0)),
            fixture.target(1, WirePollingStrategy::offset(0)),
        ]);
        let fetch = fixture.fetch(&wire);
        let mut fetch = pin!(fetch);
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        fixture.answer_wait_with(PartitionReadReply::Rejected(IggyError::HistoryUnavailable));
        assert!(futures::poll!(fetch.as_mut()).is_pending());

        for partition_id in 0..2 {
            let mut reads = fixture.reads();
            assert_eq!(reads.len(), 1);
            let read = reads.remove(&partition_id).unwrap();
            read.answer(if partition_id == 0 {
                PartitionReadReply::Rejected(IggyError::HistoryUnavailable)
            } else {
                PartitionReadReply::NotFound
            });
            if partition_id == 0 {
                assert!(futures::poll!(fetch.as_mut()).is_pending());
            }
        }
        let Poll::Ready(reply) = futures::poll!(fetch.as_mut()) else {
            panic!("the fetch answers once its reads did");
        };
        let reply = reply.expect("the fetch succeeds").into_contiguous();
        let statuses: Vec<_> = sections(reply.as_slice())
            .iter()
            .map(|section| (section.header.partition_id, section.header.status))
            .collect();
        assert_eq!(
            statuses,
            [
                (0, IggyError::HistoryUnavailable.as_code()),
                (1, IggyError::TransientNotAccepted.as_code())
            ]
        );
    }

    #[compio::test]
    async fn given_a_wait_refused_for_the_caller_when_waiting_should_fail_the_fetch_before_any_read()
     {
        let fixture = Fixture::new(2);
        let wire = fetch_of(vec![
            fixture.target(0, WirePollingStrategy::offset(0)),
            fixture.target(1, WirePollingStrategy::offset(0)),
        ]);
        let fetch = fixture.fetch(&wire);
        let mut fetch = pin!(fetch);
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        fixture.answer_wait_with(PartitionReadReply::Rejected(IggyError::StaleClient));
        let Poll::Ready(result) = futures::poll!(fetch.as_mut()) else {
            panic!("a wait refused for the caller ends the fetch");
        };
        assert_eq!(result.err(), Some(IggyError::StaleClient));
        assert!(
            fixture.owner_frames().is_empty(),
            "a fetch whose caller lost its session reads nothing"
        );
    }

    #[compio::test]
    async fn given_refused_reads_when_serving_should_answer_error_sections_beside_the_served_one() {
        const PARTITIONS: u32 = 5;
        let fixture = Fixture::new(PARTITIONS);
        let record = stored_batch_record(None);
        let wire = fetch_of(
            (0..PARTITIONS)
                .map(|partition_id| fixture.target(partition_id, WirePollingStrategy::offset(0)))
                .collect(),
        );
        let fetch = fixture.fetch(&wire);
        let mut fetch = pin!(fetch);
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        fixture.answer_wait(
            (0..PARTITIONS)
                .map(|partition_id| ready(partition_id, u64::from(STORED_BATCH_MESSAGES)))
                .collect(),
        );
        assert!(futures::poll!(fetch.as_mut()).is_pending());

        let mut reads = fixture.reads();
        reads
            .remove(&0)
            .unwrap()
            .answer(served(fixture.context(0), whole(&record)));
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        reads.extend(fixture.reads());
        let mut answer = |partition_id: usize, reply: PartitionReadReply| {
            reads
                .remove(&partition_id)
                .expect("each ready target is read")
                .answer(reply);
        };
        answer(1, PartitionReadReply::Rejected(IggyError::NotPrimary));
        answer(2, PartitionReadReply::NotFound);
        // A reply of another read kind.
        answer(
            3,
            PartitionReadReply::Primary {
                view: 0,
                primary: 0,
            },
        );
        answer(
            4,
            PartitionReadReply::Poll {
                context: fixture.context(4),
                fragments: whole(&record),
                current_offset: CURRENT_OFFSET,
                encryptor: None,
                next_offset: None,
            },
        );
        let Poll::Ready(reply) = futures::poll!(fetch.as_mut()) else {
            panic!("the fetch answers once its reads answered");
        };
        let reply = reply
            .expect("an error after the reads is a section")
            .into_contiguous();
        let sections = sections(reply.as_slice());
        let headers: Vec<_> = sections.iter().map(|section| section.header).collect();
        assert_eq!(
            headers,
            [
                fixture.served_header(0, record.len()),
                error_header(1, &IggyError::NotPrimary),
                error_header(2, &IggyError::TransientNotAccepted),
                error_header(3, &IggyError::ShardCommunicationError),
                error_header(4, &IggyError::ShardCommunicationError),
            ]
        );
        assert_eq!(sections[0].records, record.as_slice());
        assert!(
            sections[1..]
                .iter()
                .all(|section| section.records.is_empty()),
            "an error section holds no records"
        );
    }

    #[compio::test]
    async fn given_a_session_that_ends_during_the_reads_when_serving_should_fail_the_fetch() {
        let fixture = Fixture::new(1);
        let wire = fetch_of(vec![fixture.target(0, WirePollingStrategy::offset(0))]);
        let fetch = fixture.fetch(&wire);
        let mut fetch = pin!(fetch);
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        fixture.answer_wait(vec![ready(0, u64::from(STORED_BATCH_MESSAGES))]);
        assert!(futures::poll!(fetch.as_mut()).is_pending());

        let mut reads = fixture.reads();
        assert!(
            fixture
                .shard
                .plane
                .metadata()
                .client_table
                .borrow_mut()
                .end_session(CLIENT, DEFAULT_ROOT_USER_ID, SESSION, COMMIT),
            "the session ends while its read runs"
        );
        reads
            .remove(&0)
            .expect("the ready target is read")
            .answer(served(
                fixture.context(0),
                whole(&stored_batch_record(None)),
            ));
        let Poll::Ready(result) = futures::poll!(fetch.as_mut()) else {
            panic!("the fetch answers once its read answered");
        };
        assert_eq!(
            result.err(),
            Some(IggyError::StaleClient),
            "a session that ended fails the whole fetch"
        );
    }

    #[compio::test]
    async fn given_a_partition_recreated_during_its_read_when_serving_should_answer_not_accepted() {
        let fixture = Fixture::new(1);
        let context = fixture.context(0);
        let wire = fetch_of(vec![fixture.target(0, WirePollingStrategy::offset(0))]);
        let fetch = fixture.fetch(&wire);
        let mut fetch = pin!(fetch);
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        fixture.answer_wait(vec![ready(0, u64::from(STORED_BATCH_MESSAGES))]);
        assert!(futures::poll!(fetch.as_mut()).is_pending());

        let mut reads = fixture.reads();
        recreate_topic_in(
            &fixture.shard.plane.metadata().mux_stm,
            fixture.namespaces[0],
        );
        reads
            .remove(&0)
            .expect("the ready target is read")
            .answer(served(context, whole(&stored_batch_record(None))));
        let Poll::Ready(reply) = futures::poll!(fetch.as_mut()) else {
            panic!("the fetch answers once its read answered");
        };
        let reply = reply
            .expect("a section of an old incarnation does not fail the fetch")
            .into_contiguous();
        let headers: Vec<_> = sections(reply.as_slice())
            .iter()
            .map(|section| section.header)
            .collect();
        assert_eq!(
            headers,
            [error_header(0, &IggyError::TransientNotAccepted)],
            "records of the incarnation that the target named are not served from another"
        );
    }

    #[compio::test]
    async fn given_records_split_across_fragments_when_serving_should_answer_them_whole_without_a_copy()
     {
        // More than the stored batch holds, so the count of a section can
        // only come from its batch headers.
        const AVAILABLE: u64 = 5;
        // The owner served the batch after a retention cut, so its next
        // offset is not the start plus the count.
        const NEXT_OFFSET: u64 = 9;
        const AHEAD: u64 = 5;
        let fixture = Fixture::new(2);
        let record = stored_batch_record(None);
        // Inside the batch header, so that the count reads a header that
        // spans both fragments.
        let split = COMMAND_HEADER_SIZE / 2;
        let split_fragments = PollFragments::from_iter([
            Fragment::slice(record.clone(), 0, split),
            Fragment::slice(record.clone(), split, record.len()),
        ]);
        let stored_fragments = split_fragments.len() + 1;
        let wire = fetch_of(vec![
            fixture.target(0, WirePollingStrategy::offset(0)),
            fixture.target(1, WirePollingStrategy::offset(0)),
        ]);
        let fetch = fixture.fetch(&wire);
        let mut fetch = pin!(fetch);
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        fixture.answer_wait(vec![ready(0, AVAILABLE), ready(1, AVAILABLE)]);
        assert!(futures::poll!(fetch.as_mut()).is_pending());

        let mut reads = fixture.reads();
        reads
            .remove(&0)
            .expect("the ready target is read")
            .answer(PartitionReadReply::Poll {
                context: PartitionContext {
                    metadata_op: 0,
                    ..fixture.context(0)
                },
                fragments: split_fragments,
                current_offset: CURRENT_OFFSET,
                encryptor: None,
                next_offset: Some(NEXT_OFFSET),
            });
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        reads.extend(fixture.reads());
        let ahead = PartitionContext {
            metadata_op: COMMIT + AHEAD,
            ..fixture.context(1)
        };
        reads
            .remove(&1)
            .expect("the ready target is read")
            .answer(served(ahead, whole(&record)));
        let Poll::Ready(reply) = futures::poll!(fetch.as_mut()) else {
            panic!("the fetch answers once its reads answered");
        };
        let reply = reply.expect("the fetch succeeds");
        assert_eq!(
            reply.fragments().len(),
            1 + stored_fragments,
            "the reply is its head, then every stored fragment as it is"
        );
        let reply = reply.into_contiguous();
        let sections = sections(reply.as_slice());
        let headers: Vec<_> = sections.iter().map(|section| section.header).collect();
        assert_eq!(
            headers,
            [
                FetchMessagesSectionHeader {
                    next_offset: NEXT_OFFSET,
                    ..fixture.served_header(0, record.len())
                },
                FetchMessagesSectionHeader {
                    context: ahead,
                    ..fixture.served_header(1, record.len())
                },
            ],
            "a section counts the messages of its records, names at least the applied \
             commit, and the offsets of its owner"
        );
        assert!(
            sections
                .iter()
                .all(|section| section.records == record.as_slice()),
            "a record split across fragments arrives whole"
        );
        assert_scraped(&fixture.shard, "fetch_sections_total 2");
        assert_scraped(
            &fixture.shard,
            &format!("fetch_bytes_total {}", headers.len() * record.len()),
        );
    }

    #[compio::test]
    async fn given_a_read_that_never_answers_when_the_reply_deadline_passes_should_reply_with_the_sections_that_arrived()
     {
        let fixture = Fixture::new(2);
        let record = stored_batch_record(None);
        let wire = fetch_of(vec![
            fixture.target(0, WirePollingStrategy::offset(0)),
            fixture.target(1, WirePollingStrategy::offset(0)),
        ]);
        let started = fixture.bus.monotonic_micros();
        let fetch = fixture.fetch(&wire);
        let mut fetch = pin!(fetch);
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        fixture.answer_wait(vec![
            ready(0, u64::from(STORED_BATCH_MESSAGES)),
            ready(1, u64::from(STORED_BATCH_MESSAGES)),
        ]);
        // The next timer of the fetch is its reply deadline, which runs while
        // the reads do.
        let (deadline, held) = oneshot::channel();
        *fixture.bus.held_timer.borrow_mut() = Some(held);
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        let elapsed = fixture.bus.monotonic_micros() - started;

        let read_budget = REQUEST_TIMEOUT_US - 2 * FETCH_REPLY_MARGIN_US;
        let mut reads = fixture.reads();
        assert_eq!(reads.len(), 1, "the first ready target is read");
        for read in reads.values() {
            let budget = read.request.options.request_timeout.as_micros();
            assert!(
                (read_budget - elapsed..=read_budget).contains(&budget),
                "a read must commit two margins before the request timeout, not after {budget} us"
            );
            assert_eq!(
                read.request.options.max_wait.as_micros(),
                0,
                "a read never parks"
            );
        }
        let reply_budget = REQUEST_TIMEOUT_US - FETCH_REPLY_MARGIN_US;
        let reply_timer = fixture
            .bus
            .sleeps
            .borrow()
            .last()
            .copied()
            .expect("the reply deadline is armed");
        assert!(
            (Duration::from_micros(reply_budget - elapsed)..=Duration::from_micros(reply_budget))
                .contains(&reply_timer),
            "the reply leaves one margin before the request timeout, not after {reply_timer:?}"
        );

        reads
            .remove(&0)
            .expect("the ready target is read")
            .answer(served(fixture.context(0), whole(&record)));
        assert!(
            futures::poll!(fetch.as_mut()).is_pending(),
            "the fetch waits for its other read until the reply deadline"
        );
        reads.extend(fixture.reads());
        assert_eq!(reads.len(), 1, "the remaining target is now in flight");
        deadline.send(()).expect("the reply deadline is held");
        let Poll::Ready(reply) = futures::poll!(fetch.as_mut()) else {
            panic!("the reply deadline ends the fetch");
        };
        let reply = reply.expect("the fetch succeeds").into_contiguous();
        let headers: Vec<_> = sections(reply.as_slice())
            .iter()
            .map(|section| section.header)
            .collect();
        assert_eq!(
            headers,
            [
                fixture.served_header(0, record.len()),
                error_header(1, &IggyError::ShardCommunicationError),
            ],
            "the read that answered before the deadline is served, the other one is dropped"
        );
        drop(reads);
    }

    #[compio::test]
    async fn given_committed_reads_when_replies_arrive_after_read_deadline_should_preserve_them_beside_refused_reads()
     {
        const STARTED: u64 = 1_000_000;
        const PARTITIONS: u32 = 3;
        let fixture = Fixture::new(PARTITIONS);
        fixture.bus.monotonic.set(Some(STARTED));
        let record = stored_batch_record(None);
        let wire = FetchMessagesRequest {
            auto_commit: true,
            ..fetch_of(
                (0..PARTITIONS)
                    .map(|partition| fixture.target(partition, WirePollingStrategy::offset(0)))
                    .collect(),
            )
        };
        let fetch = fixture.fetch(&wire);
        let mut fetch = pin!(fetch);
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        fixture.answer_wait(
            (0..PARTITIONS)
                .map(|partition| ready(partition, u64::from(STORED_BATCH_MESSAGES)))
                .collect(),
        );
        let (_deadline, held) = oneshot::channel();
        *fixture.bus.held_timer.borrow_mut() = Some(held);
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        let mut reads = fixture.reads();
        assert_eq!(reads.len(), 1);
        let read_deadline = STARTED + REQUEST_TIMEOUT_US - 2 * FETCH_REPLY_MARGIN_US;
        for read in reads.values() {
            assert!(read.request.args.auto_commit);
            assert_eq!(
                read.request.options.request_timeout.as_micros(),
                read_deadline - STARTED,
            );
        }
        reads
            .remove(&0)
            .unwrap()
            .answer(served(fixture.context(0), whole(&record)));
        assert!(futures::poll!(fetch.as_mut()).is_pending());

        reads.extend(fixture.reads());
        assert_eq!(reads.len(), 2);

        // A commit before the owner's deadline still has one reply margin
        // to cross the shard channel. The connection must keep that data.
        fixture
            .bus
            .monotonic
            .set(Some(read_deadline + FETCH_REPLY_MARGIN_US / 2));
        reads
            .remove(&1)
            .unwrap()
            .answer(served(fixture.context(1), whole(&record)));
        reads
            .remove(&2)
            .unwrap()
            .answer(PartitionReadReply::Rejected(
                IggyError::TransientNotAccepted,
            ));
        let Poll::Ready(reply) = futures::poll!(fetch.as_mut()) else {
            panic!("all owner replies arrived before the reply deadline");
        };
        let reply = reply.unwrap().into_contiguous();
        let sections = sections(reply.as_slice());
        assert_eq!(sections.len(), 3);
        for (partition, section) in sections[..2].iter().enumerate() {
            assert_eq!(
                section.header,
                fixture.served_header(u32::try_from(partition).unwrap(), record.len())
            );
            assert_eq!(section.records, record.as_slice());
        }
        assert_eq!(
            sections[2].header.status,
            IggyError::TransientNotAccepted.as_code()
        );
        assert_eq!(sections[2].records, b"");
    }

    #[compio::test]
    async fn given_an_encrypted_partition_when_serving_should_answer_plaintext_and_an_error_section_for_a_bad_record()
     {
        const KEY: [u8; 32] = [7; 32];
        let encryptor = Arc::new(EncryptorKind::Aes256Gcm(
            Aes256GcmEncryptor::new(&KEY).expect("a valid key"),
        ));
        let fixture = Fixture::new(2);
        let encrypted = whole(&stored_batch_record(Some(&encryptor)));
        let plaintext = build_polled_messages_body(
            0,
            CURRENT_OFFSET,
            fixture.context(0),
            encrypted.clone(),
            Some(&encryptor),
        )
        .expect("the record decrypts");
        let plaintext = &plaintext[POLL_RESPONSE_HEADER_SIZE..];
        let wire = fetch_of(vec![
            fixture.target(0, WirePollingStrategy::offset(0)),
            fixture.target(1, WirePollingStrategy::offset(0)),
        ]);
        let fetch = fixture.fetch(&wire);
        let mut fetch = pin!(fetch);
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        fixture.answer_wait(vec![
            ready(0, u64::from(STORED_BATCH_MESSAGES)),
            ready(1, u64::from(STORED_BATCH_MESSAGES)),
        ]);
        assert!(futures::poll!(fetch.as_mut()).is_pending());

        let mut reads = fixture.reads();
        let answer = |read: OwnerRead, partition_id: usize, fragments: PollFragments| {
            let partition = u32::try_from(partition_id).expect("the partition id fits u32");
            read.answer(PartitionReadReply::Poll {
                context: fixture.context(partition),
                fragments,
                current_offset: CURRENT_OFFSET,
                encryptor: Some(Arc::clone(&encryptor)),
                next_offset: Some(u64::from(STORED_BATCH_MESSAGES)),
            });
        };
        answer(reads.remove(&0).unwrap(), 0, encrypted);
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        reads.extend(fixture.reads());
        // A record stored without the key of the partition.
        answer(
            reads.remove(&1).unwrap(),
            1,
            whole(&stored_batch_record(None)),
        );
        let Poll::Ready(reply) = futures::poll!(fetch.as_mut()) else {
            panic!("the fetch answers once its reads answered");
        };
        let reply = reply.expect("a record that does not decrypt is a section");
        assert_eq!(
            reply.fragments().len(),
            2,
            "the decrypted records are one copied frame after the head"
        );
        let reply = reply.into_contiguous();
        let sections = sections(reply.as_slice());
        let headers: Vec<_> = sections.iter().map(|section| section.header).collect();
        assert_eq!(
            headers,
            [
                fixture.served_header(0, plaintext.len()),
                error_header(1, &IggyError::CannotDecryptData),
            ]
        );
        assert_eq!(
            sections[0].records, plaintext,
            "a fetch serves the records that a poll serves"
        );
    }

    /// One shard that owns `partitions` partitions of one topic, with a bound
    /// root session. The frames that the shard sends to the owner of a
    /// partition land in `owner_inbox`, where the test answers as the owner.
    struct Fixture {
        bus: SpyBus,
        shard: Rc<TestShard>,
        sessions: Rc<RefCell<SessionManager>>,
        attachment: SessionAttachment,
        namespaces: Vec<IggyNamespace>,
        owner_inbox: Receiver<ShardFrame>,
        _owner_replies: Receiver<ShardFrame>,
    }

    impl Fixture {
        fn new(partitions: u32) -> Self {
            let bus = SpyBus::default();
            let mut shard = test_shard(&bus, 0, 1, FIRST_BOOT);
            let (sender, owner_inbox, owner_replies) = shard_channel(0, INBOX_CAPACITY, 1);
            shard.attach_senders(vec![sender]);
            let shard = Rc::new(shard);
            let namespaces = route_partitions(&shard, partitions);
            shard.plane.metadata().applied_frontier().advance(COMMIT);
            let attachment = attach_root_session(&shard, CLIENT, SESSION);
            Self {
                bus,
                shard,
                sessions: Rc::new(RefCell::new(SessionManager::new())),
                attachment,
                namespaces,
                owner_inbox,
                _owner_replies: owner_replies,
            }
        }

        /// A target of `partition_id` that names its incarnation.
        fn target(&self, partition_id: u32, strategy: WirePollingStrategy) -> FetchMessagesTarget {
            FetchMessagesTarget {
                partition_id,
                strategy,
                max_bytes: TARGET_MAX_BYTES,
                incarnation: self.context(partition_id).incarnation,
                owner_generation: 0,
            }
        }

        /// The context of `partition_id` at the applied commit.
        fn context(&self, partition_id: u32) -> PartitionContext {
            let namespace = self.namespaces[partition_id as usize];
            PartitionContext {
                incarnation: self
                    .shard
                    .plane
                    .metadata()
                    .mux_stm
                    .streams()
                    .created_revision_for_namespace(namespace)
                    .expect("the partition exists"),
                owner_generation: 0,
                metadata_op: COMMIT,
            }
        }

        /// The table entry of a section that [`served`] a record of
        /// `records_len` bytes.
        fn served_header(
            &self,
            partition_id: u32,
            records_len: usize,
        ) -> FetchMessagesSectionHeader {
            FetchMessagesSectionHeader {
                partition_id,
                status: STATUS_OK,
                current_offset: CURRENT_OFFSET,
                messages_count: STORED_BATCH_MESSAGES,
                context: self.context(partition_id),
                next_offset: u64::from(STORED_BATCH_MESSAGES),
                records_len: u32::try_from(records_len).expect("a record fits u32"),
            }
        }

        async fn fetch(&self, wire: &FetchMessagesRequest) -> Result<BusMessage, IggyError> {
            self.fetch_holding(wire).await.map(|(reply, _)| reply)
        }

        /// [`Self::fetch`] with the session room that its reply holds.
        async fn fetch_holding(
            &self,
            wire: &FetchMessagesRequest,
        ) -> Result<(BusMessage, Vec<FetchReadGuard>), IggyError> {
            let request = fetch_request(&wire.to_bytes());
            fetch_messages(
                &self.shard,
                &self.sessions,
                &request,
                (wire, None),
                (CLIENT, &self.attachment, DEFAULT_ROOT_USER_ID),
            )
            .await
        }

        /// Serves `body` as a fetch of the root session on the connection
        /// [`TRANSPORT`]. The reply lands in the client replies of the bus.
        async fn serve(&self, body: &[u8]) {
            self.serve_from(ClientOrigin::capture(&self.bus, TRANSPORT), body)
                .await;
        }

        /// [`Self::serve`] for a frame that arrived at `origin`.
        async fn serve_from(&self, origin: ClientOrigin, body: &[u8]) {
            let config = Arc::new(ServerConfig::default());
            handle_non_replicated_request(
                &self.shard,
                &self.sessions,
                &config,
                origin,
                fetch_request(body),
                ConnectionContext {
                    bound: Some((CLIENT, SESSION)),
                    attachment: Some(self.attachment.clone()),
                    user_id: Some(DEFAULT_ROOT_USER_ID),
                    address: None,
                    metadata_watermark: 0,
                },
            )
            .await;
        }

        /// The frames that the shard sent to the owner since the last call.
        fn owner_frames(&self) -> Vec<LifecycleFrame> {
            iter::from_fn(|| self.owner_inbox.try_recv().ok())
                .map(|frame| {
                    let ShardFrame::Lifecycle(frame) = frame else {
                        panic!("a fetch sends its owner lifecycle frames only");
                    };
                    frame
                })
                .collect()
        }

        /// Answers the stored-offset read of each `Next` target with the
        /// entry of `answers` for its partition.
        fn answer_stored_offsets(&self, mut answers: BTreeMap<usize, PartitionReadReply>) {
            for frame in self.owner_frames() {
                let LifecycleFrame::PartitionRead {
                    namespace,
                    read:
                        PartitionRead::ConsumerOffsetOnPrimary {
                            consumer: stored_key,
                            ..
                        },
                    reply,
                } = frame
                else {
                    panic!("a fetch reads the stored offsets of its next targets first");
                };
                let partition_id = namespace.partition_id();
                assert_eq!(
                    stored_key,
                    Some(offset_key(partition_id)),
                    "the read names the offset key of the target"
                );
                let answer = answers
                    .remove(&partition_id)
                    .expect("only a next target reads its stored offset");
                reply
                    .try_send(answer)
                    .expect("the fetch awaits the stored offset");
            }
            assert!(
                answers.is_empty(),
                "each next target reads its stored offset"
            );
        }

        /// Answers the readiness wait of the fetch with `ready`, and returns
        /// the wait.
        fn answer_wait(&self, ready: Vec<ReadyTarget>) -> ReadinessWait {
            self.answer_wait_with(PartitionReadReply::Ready {
                shard: 0,
                parked: None,
                partitions: ready,
            })
        }

        fn answer_wait_with(&self, answer: PartitionReadReply) -> ReadinessWait {
            let mut frames = self.owner_frames();
            assert_eq!(
                frames.len(),
                1,
                "the fetch waits for its targets in one frame"
            );
            let Some(LifecycleFrame::ReadinessWait { wait, reply, .. }) = frames.pop() else {
                panic!("the fetch must wait for its targets");
            };
            reply.try_send(answer).expect("the fetch awaits its wait");
            *wait
        }

        async fn refuse_reads(
            &self,
            mut fetch: Pin<&mut impl Future>,
            count: usize,
        ) -> BTreeMap<usize, OwnerRead> {
            let mut reads = BTreeMap::new();
            for index in 0..count {
                let current = self.reads();
                assert_eq!(current.len(), 1);
                for read in current.values() {
                    read.reply
                        .try_send(PartitionReadReply::Rejected(IggyError::NotPrimary))
                        .unwrap();
                }
                reads.extend(current);
                if index + 1 < count {
                    assert!(futures::poll!(fetch.as_mut()).is_pending());
                }
            }
            reads
        }

        /// The reads that the shard sent to the owner, by partition id.
        fn reads(&self) -> BTreeMap<usize, OwnerRead> {
            self.owner_frames()
                .into_iter()
                .map(|frame| match frame {
                    LifecycleFrame::PartitionRead {
                        namespace,
                        read: PartitionRead::DeferredPoll(request),
                        reply,
                    } => (namespace.partition_id(), OwnerRead { request, reply }),
                    _ => panic!("after its wait a fetch sends reads only"),
                })
                .collect()
        }
    }

    /// One read of a fetch, as its owner receives it.
    struct OwnerRead {
        request: Box<DeferredPollRequest>,
        reply: Sender<PartitionReadReply>,
    }

    impl OwnerRead {
        /// The count, the start offset and the byte cap of the read.
        fn allotment(&self) -> (u32, u64, u32) {
            (
                self.request.args.count,
                self.request.args.value,
                self.request.options.max_bytes,
            )
        }

        fn answer(self, reply: PartitionReadReply) {
            self.reply
                .try_send(reply)
                .expect("the fetch awaits its read");
        }
    }

    /// `body` as a fetch request of the root session.
    fn fetch_request(body: &[u8]) -> Message<RoutedRequestHeader> {
        request_message(Operation::NonReplicated, CLIENT, SESSION, REQUEST, body).transmute_header(
            |old, header: &mut RoutedRequestHeader| {
                *header = old;
                let code = FETCH_MESSAGES_CODE.to_le_bytes();
                header.reserved[..code.len()].copy_from_slice(&code);
            },
        )
    }

    /// A fetch of `targets` by the plain consumer [`CONSUMER_ID`].
    fn fetch_of(targets: Vec<FetchMessagesTarget>) -> FetchMessagesRequest {
        FetchMessagesRequest {
            consumer: WireConsumer::consumer(WireIdentifier::numeric(CONSUMER_ID)),
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            wait_us: WAIT_US,
            request_timeout_us: REQUEST_TIMEOUT_US,
            min_count: MIN_COUNT,
            count: COUNT,
            max_bytes: MAX_BYTES,
            auto_commit: false,
            targets,
        }
    }

    /// The offset key of [`CONSUMER_ID`] on `partition_id`.
    fn offset_key(partition_id: usize) -> PollingConsumer {
        let consumer = usize::try_from(CONSUMER_ID).expect("the consumer id fits usize");
        PollingConsumer::Consumer(consumer, partition_id)
    }

    /// A ready target that holds `available` messages, without known bytes.
    const fn ready(partition_id: u32, available: u64) -> ReadyTarget {
        ReadyTarget {
            partition_id,
            available,
            known_bytes: None,
        }
    }

    /// The owner reply of a read that served `fragments`, which hold
    /// [`stored_batch_record`].
    fn served(context: PartitionContext, fragments: PollFragments) -> PartitionReadReply {
        PartitionReadReply::Poll {
            context,
            fragments,
            current_offset: CURRENT_OFFSET,
            encryptor: None,
            next_offset: Some(u64::from(STORED_BATCH_MESSAGES)),
        }
    }

    fn whole(record: &Frozen<MESSAGE_ALIGN>) -> PollFragments {
        PollFragments::from_iter([Fragment::whole(record.clone())])
    }

    /// The sections of the fetch reply `frame`.
    fn sections(frame: &[u8]) -> Vec<FetchMessagesSection<'_>> {
        FetchMessagesResponse::decode(&frame[size_of::<ReplyHeader>()..])
            .expect("the reply decodes")
            .sections
    }

    /// The table entry of a section whose target failed with `error`.
    fn error_header(partition_id: u32, error: &IggyError) -> FetchMessagesSectionHeader {
        FetchMessagesSectionHeader {
            partition_id,
            status: error.as_code(),
            current_offset: 0,
            messages_count: 0,
            context: PartitionContext::default(),
            next_offset: 0,
            records_len: 0,
        }
    }

    fn reply_status(frame: &[u8]) -> u32 {
        const STATUS: usize = std::mem::offset_of!(ReplyHeader, status);
        u32::from_le_bytes(
            frame[STATUS..STATUS + size_of::<u32>()]
                .try_into()
                .expect("the reply holds a status"),
        )
    }

    /// Asserts that a scrape of the metrics of `shard` holds the line `series`.
    fn assert_scraped(shard: &TestShard, series: &str) {
        let mut registry = Registry::default();
        shard.metrics().register(&mut registry);
        let mut scrape = String::new();
        encode(&mut scrape, &registry).expect("the scrape encodes");
        assert!(
            scrape.lines().any(|line| line == series),
            "{series} is not in the scrape:\n{scrape}"
        );
    }
}
