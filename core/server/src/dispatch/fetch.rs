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
use partitions::{PollFragments, PollingArgs, PollingConsumer};
use server_common::iobuf::Frozen;
use server_common::sharding::IggyNamespace;
use server_common::{MESSAGE_ALIGN, Message};
use shard::metrics::FetchRecord;
use shard::shards_table::ShardsTable;
use shard::{
    DeferredPollContext, DeferredPollRequest, KNOWN_BYTES_SLACK, PartitionRead, PartitionReadReply,
    PollQuotaIdentity, ReadinessTarget, ReadyTarget,
};
use std::cell::RefCell;
use std::iter;
use std::pin::pin;
use std::rc::Rc;
use std::time::Duration;
use tracing::error;

#[allow(clippy::cast_possible_truncation)]
const MIN_READ_BYTES: u32 = MIN_DEFERRED_POLL_MAX_BYTES as u32;

type Section = (FetchMessagesSectionHeader, Vec<Frozen<MESSAGE_ALIGN>>);

/// Answers a fetch in four steps: resolve the targets once (D9), resolve the
/// start of each `Next` target from its stored offset (D4), wait until the
/// targets hold `min_count` messages, then read the ready targets that the
/// budgets allot (D3, D10, D22).
///
/// The request timeout `T` starts with this future. Each read must commit
/// before `T - 2 x FETCH_REPLY_MARGIN_US`, and the reply leaves at the latest
/// at `T - FETCH_REPLY_MARGIN_US` with the sections that arrived, so a read
/// that committed reaches the client (D6). An error before the reads fails the
/// request. After them, only a session or permission that is gone fails it,
/// and any other error of a target is its section (D7).
#[allow(clippy::future_not_send)]
pub(super) async fn fetch_messages<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    sessions: &Rc<RefCell<SessionManager>>,
    request: &Message<RoutedRequestHeader>,
    wire: &FetchMessagesRequest,
    (consumer_client_id, attachment, user_id): (u128, &SessionAttachment, u32),
) -> Result<BusMessage, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let started = shard.bus.monotonic_micros();
    let timeout = started.saturating_add(wire.request_timeout_us);
    let mut targets = resolve_fetch(shard, wire, request.header(), consumer_client_id, user_id)?;
    resolve_next_starts(shard, &mut targets).await?;
    let caller = DeferredPollContext {
        session: Some(attachment.clone()),
        user_id,
        user_created_at: None,
        quota: PollQuotaIdentity::Session(consumer_client_id),
        primary: true,
    };
    let ready_targets = shard
        .wait_for_messages(
            caller.clone(),
            targets.iter().map(FetchTarget::readiness).collect(),
            Duration::from_micros(shard.deferred_wait_us(wire.wait_us)),
            wire.min_count,
        )
        .await?;
    let fired = shard.bus.monotonic_micros();
    let (allotted, budget_waits) = allot(
        shard,
        sessions,
        &targets,
        &ready_targets,
        wire,
        consumer_client_id,
    );
    let replies = read_allotted(
        shard,
        &targets,
        allotted,
        (caller, wire.auto_commit),
        timeout,
    )
    .await;
    if let Some(target) = targets.first() {
        authorize_served_topic_read(
            shard,
            Some(attachment),
            target.namespace,
            Permissioner::poll_messages,
        )?;
    }
    let commit = current_metadata_commit(shard);
    let mut fetch = FetchRecord {
        budget_waits,
        ..FetchRecord::default()
    };
    let sections = fetch_sections(shard, &targets, replies, commit, &mut fetch);
    let reply = build_fetch_messages_reply(request.header(), commit, sections)?;
    fetch.fire_to_reply = Duration::from_micros(shard.bus.monotonic_micros().saturating_sub(fired));
    shard.metrics().record_fetch(&fetch);
    Ok(reply)
}

/// Runs the allotted reads at once and collects the replies that arrive
/// before the reply deadline, by target. The reads that did not answer by
/// then are dropped.
#[allow(clippy::future_not_send)]
async fn read_allotted<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    targets: &[FetchTarget],
    allotted: Vec<Read>,
    (caller, auto_commit): (DeferredPollContext, bool),
    timeout: u64,
) -> Vec<Option<PartitionReadReply>>
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
    let mut pending: FuturesUnordered<_> = allotted
        .into_iter()
        .map(|read| {
            let target = &targets[read.index];
            let poll = DeferredPollRequest::new(
                target.consumer,
                PollingArgs::new(
                    PollingStrategy::offset(target.start),
                    read.count,
                    auto_commit,
                ),
                target.metadata,
                caller.clone(),
                DeferredPollOptions {
                    max_bytes: read.max_bytes,
                    ..Default::default()
                },
            );
            async move {
                let _reservation = read.reservation;
                let reply = shard
                    .deferred_read(target.namespace, poll, read_deadline)
                    .await;
                (read.index, reply)
            }
        })
        .collect();
    let mut reply_timer = pin!(shard.bus.sleep(Duration::from_micros(
        reply_at.saturating_sub(shard.bus.monotonic_micros())
    )));
    while let Either::Left((Some((index, reply)), _)) =
        select(pending.next(), reply_timer.as_mut()).await
    {
        replies[index] = reply;
    }
    replies
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

#[allow(clippy::cast_possible_truncation)]
fn resolve_fetch<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    wire: &FetchMessagesRequest,
    header: &RoutedRequestHeader,
    consumer_client_id: u128,
    user_id: u32,
) -> Result<Vec<FetchTarget>, IggyError>
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
    wire.targets
        .iter()
        .zip(resolved)
        .map(|(target, resolved)| {
            let partition_id = target.partition_id;
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
        })
        .collect()
}

/// Resolves each `Next` target to the offset after its stored one, with one
/// stored-offset read per target, all at once (D4).
#[allow(clippy::future_not_send)]
async fn resolve_next_starts<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    targets: &mut [FetchTarget],
) -> Result<(), IggyError>
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
    for (target, reply) in targets.iter_mut().filter(|target| target.next).zip(replies) {
        let (stored, current_offset) = match reply {
            Some(PartitionReadReply::ConsumerOffset {
                stored,
                current_offset,
            }) => (stored, current_offset),
            Some(PartitionReadReply::Rejected(error)) => return Err(error),
            Some(PartitionReadReply::NotFound) => return Err(IggyError::TransientNotAccepted),
            _ => return Err(IggyError::ShardCommunicationError),
        };
        target.start = stored.map_or(Ok(0), |offset| {
            offset
                .checked_add(1)
                .ok_or(IggyError::InvalidOffset(offset))
        })?;
        target.resolved = Some(current_offset);
    }
    Ok(())
}

/// The reads of a fetch, and the ready targets that wait for a later fetch,
/// in request order (D3, D22, D10).
///
/// A ready target reads `min(available, the count left)` messages. It
/// reserves its known bytes plus `KNOWN_BYTES_SLACK`, else its cap, from the
/// bytes left, all or nothing. Its owner must also have room in the reads
/// that this shard runs there for the session.
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
    let limits = shard.deferred_poll_config();
    let mut count = wire.count;
    let mut bytes = wire.max_bytes;
    let mut allotted = Vec::with_capacity(ready.len());
    let mut budget_waits = 0;
    for (index, target) in targets.iter().enumerate() {
        let Some(readiness) = ready.get(&target.partition_id) else {
            continue;
        };
        let max_bytes = readiness.known_bytes.map_or(target.max_bytes, |known| {
            u32::try_from(known)
                .unwrap_or(u32::MAX)
                .saturating_add(KNOWN_BYTES_SLACK)
                .min(target.max_bytes)
                .max(MIN_READ_BYTES)
        });
        let reservation = if count == 0 || max_bytes > bytes {
            None
        } else {
            shard
                .shards_table()
                .shard_for(target.namespace)
                .map_or(Some(None), |owner| {
                    SessionManager::reserve_fetch_read(
                        sessions,
                        (consumer_client_id, owner),
                        shard.deferred_read_cost(max_bytes),
                        &limits,
                    )
                    .map(Some)
                })
        };
        let Some(reservation) = reservation else {
            budget_waits += 1;
            continue;
        };
        let read_count =
            u32::try_from(readiness.available).map_or(count, |available| available.min(count));
        count -= read_count;
        bytes -= max_bytes;
        allotted.push(Read {
            index,
            count: read_count,
            max_bytes,
            reservation,
        });
    }
    (allotted, budget_waits)
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
    use server_common::send_messages::COMMAND_HEADER_SIZE;
    use shard::config::DeferredPollConfig;
    use shard::{LifecycleFrame, ReadinessWait, Receiver, Sender, ShardFrame, shard_channel};
    use std::collections::BTreeMap;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
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
    async fn given_a_target_that_does_not_resolve_when_fetching_should_fail_before_any_owner_frame()
    {
        const MISSING_PARTITION: u32 = 5;
        let fixture = Fixture::new(2);
        let target = |partition_id| fixture.target(partition_id, WirePollingStrategy::offset(0));
        let missing = FetchMessagesTarget {
            partition_id: MISSING_PARTITION,
            ..target(1)
        };
        let result = fixture.fetch(&fetch_of(vec![target(0), missing])).await;
        assert!(
            matches!(
                result.err(),
                Some(IggyError::PartitionNotFound(partition_id, ..))
                    if partition_id == MISSING_PARTITION as usize
            ),
            "a missing partition fails the whole fetch"
        );

        let stale = FetchMessagesTarget {
            incarnation: fixture.context(1).incarnation + 1,
            ..target(1)
        };
        let result = fixture.fetch(&fetch_of(vec![target(0), stale])).await;
        assert_eq!(
            result.err(),
            Some(IggyError::HistoryUnavailable),
            "a target fenced to another incarnation fails the whole fetch"
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
    async fn given_ready_targets_when_the_count_runs_out_should_read_only_the_targets_before_it() {
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

        let reads = fixture.reads();
        let allotments: BTreeMap<_, _> = reads
            .iter()
            .map(|(partition_id, read)| (*partition_id, read.allotment()))
            .collect();
        assert_eq!(
            allotments,
            BTreeMap::from([
                (0, (FIRST_AVAILABLE, STARTS[0], TARGET_MAX_BYTES)),
                (
                    1,
                    (FETCH_COUNT - FIRST_AVAILABLE, STARTS[1], TARGET_MAX_BYTES)
                ),
            ]),
            "a read takes what its target holds out of the count left, in request order"
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
        assert_scraped(&fixture.shard, "fetch_budget_waits_total 1");
    }

    #[compio::test]
    async fn given_ready_targets_when_reserving_bytes_should_read_each_target_whose_whole_reservation_fits()
     {
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
        let wire = FetchMessagesRequest {
            // After the first three reads, the cap of the fourth target does
            // not fit, and the known bytes of the fifth fit exactly.
            max_bytes: MIN_READ_BYTES + NARROW_CAP + CAP + fitting,
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
            .reads()
            .iter()
            .map(|(partition_id, read)| (*partition_id, read.request.options.max_bytes))
            .collect();
        assert_eq!(
            caps,
            BTreeMap::from([(0, MIN_READ_BYTES), (1, NARROW_CAP), (2, CAP), (4, fitting)]),
            "known bytes reserve themselves and the slack within the target cap and above \
             the smallest read, other targets their cap, and a reservation that does not fit \
             gets no read"
        );
        let Poll::Ready(reply) = futures::poll!(fetch.as_mut()) else {
            panic!("the fetch answers once its reads ended");
        };
        assert!(reply.is_ok());
        assert_scraped(&fixture.shard, "fetch_budget_waits_total 1");
    }

    #[compio::test]
    async fn given_a_session_read_cap_when_targets_share_an_owner_should_read_up_to_the_cap() {
        assert_one_read_under(|_| DeferredPollConfig {
            max_pending_per_session: 1,
            ..DeferredPollConfig::default()
        })
        .await;
    }

    #[compio::test]
    async fn given_a_session_byte_budget_when_targets_share_an_owner_should_read_what_it_holds() {
        assert_one_read_under(|shard| DeferredPollConfig {
            max_inflight_bytes: shard.deferred_read_cost(TARGET_MAX_BYTES),
            ..DeferredPollConfig::default()
        })
        .await;
    }

    /// Two ready targets on one owner, under `limits` that hold one read of
    /// the session there. The second target waits for a later fetch, and the
    /// read gives its room back when it ends.
    async fn assert_one_read_under(limits: impl FnOnce(&TestShard) -> DeferredPollConfig) {
        const OWNER: u16 = 0;
        let fixture = Fixture::new(2);
        let limits = limits(&fixture.shard);
        fixture.shard.set_deferred_poll_config(limits);
        let cost = fixture.shard.deferred_read_cost(TARGET_MAX_BYTES);
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
    async fn given_a_quota_refusal_when_reading_should_drop_an_offset_section_and_keep_a_next_start()
     {
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

        let reads = fixture.reads();
        assert_eq!(reads.len(), 2, "each ready target is read");
        for read in reads.into_values() {
            read.answer(PartitionReadReply::Rejected(
                IggyError::DeferredPollCapacityExceeded,
            ));
        }
        let Poll::Ready(reply) = futures::poll!(fetch.as_mut()) else {
            panic!("the fetch answers once its reads answered");
        };
        let reply = reply
            .expect("a quota refusal does not fail the fetch")
            .into_contiguous();
        let headers: Vec<_> = sections(reply.as_slice())
            .iter()
            .map(|section| section.header)
            .collect();
        assert_eq!(
            headers,
            [FetchMessagesSectionHeader {
                partition_id: 1,
                status: STATUS_OK,
                current_offset: STORED_CURRENT,
                messages_count: 0,
                context: fixture.context(1),
                next_offset: STORED + 1,
                records_len: 0,
            }],
            "a refused offset target keeps its position without a section, and a next \
             target still learns its start"
        );
        assert_scraped(&fixture.shard, "fetch_quota_refusals_total 2");
    }

    #[compio::test]
    async fn given_a_refused_stored_offset_read_when_resolving_next_should_fail_the_fetch() {
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
            let Poll::Ready(result) = futures::poll!(fetch.as_mut()) else {
                panic!("a refused stored offset ends the fetch");
            };
            assert_eq!(result.err(), Some(expected));
            assert!(
                fixture.owner_frames().is_empty(),
                "a fetch that fails before its wait sends no wait"
            );
        }
    }

    #[compio::test]
    async fn given_a_refused_wait_when_waiting_should_fail_the_fetch_before_any_read() {
        let fixture = Fixture::new(2);
        let wire = fetch_of(vec![
            fixture.target(0, WirePollingStrategy::offset(0)),
            fixture.target(1, WirePollingStrategy::offset(0)),
        ]);
        let fetch = fixture.fetch(&wire);
        let mut fetch = pin!(fetch);
        assert!(futures::poll!(fetch.as_mut()).is_pending());
        fixture.answer_wait_with(PartitionReadReply::Rejected(IggyError::HistoryUnavailable));
        let Poll::Ready(result) = futures::poll!(fetch.as_mut()) else {
            panic!("a refused wait ends the fetch");
        };
        assert_eq!(result.err(), Some(IggyError::HistoryUnavailable));
        assert!(
            fixture.owner_frames().is_empty(),
            "a fetch whose wait failed reads nothing"
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
        let mut answer = |partition_id: usize, reply: PartitionReadReply| {
            reads
                .remove(&partition_id)
                .expect("each ready target is read")
                .answer(reply);
        };
        answer(0, served(fixture.context(0), whole(&record)));
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
        assert_eq!(reads.len(), 2, "each ready target is read");
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
            [fixture.served_header(0, record.len())],
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
        assert_eq!(reads.len(), 3);
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
        let mut answer = |partition_id: usize, fragments: PollFragments| {
            let partition = u32::try_from(partition_id).expect("the partition id fits u32");
            reads
                .remove(&partition_id)
                .expect("each ready target is read")
                .answer(PartitionReadReply::Poll {
                    context: fixture.context(partition),
                    fragments,
                    current_offset: CURRENT_OFFSET,
                    encryptor: Some(Arc::clone(&encryptor)),
                    next_offset: Some(u64::from(STORED_BATCH_MESSAGES)),
                });
        };
        answer(0, encrypted);
        // A record stored without the key of the partition.
        answer(1, whole(&stored_batch_record(None)));
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
            let request = fetch_request(&wire.to_bytes());
            fetch_messages(
                &self.shard,
                &self.sessions,
                &request,
                wire,
                (CLIENT, &self.attachment, DEFAULT_ROOT_USER_ID),
            )
            .await
        }

        /// Serves `body` as a fetch of the root session on the connection
        /// [`TRANSPORT`]. The reply lands in the client replies of the bus.
        async fn serve(&self, body: &[u8]) {
            let config = Arc::new(ServerConfig::default());
            handle_non_replicated_request(
                &self.shard,
                &self.sessions,
                &config,
                ClientOrigin::capture(&self.bus, TRANSPORT),
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
