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

//! The shard-0 metadata-submit RPC, both ends on one page.
//!
//! The metadata consensus group lives on shard 0, but connections live on
//! their home shards. Peer shards send a [`shard::MetadataSubmit`] and await
//! the committed outcome; [`handle_metadata_submit`] is what shard 0 runs
//! for those frames. The session-lifecycle arms (register / logout and
//! their replica forwards) delegate to `session_ops`, which owns that
//! machinery. A backup relays partition completions to the metadata primary
//! here, because only the primary proposes and it may hold no replica of the
//! partition. The retention decision of a tiered partition's primary takes the
//! same relay, because that primary can sit on a metadata backup.

use crate::consumer_group::lease::ConsumerGroupLiveness;
use crate::dispatch::session_ops::{
    answer_forwarded_logout, answer_forwarded_register, submit_logout_local_or_forward,
    submit_register_local_or_forward,
};
use crate::reply_frame::committed_reply_header;
use crate::shell::{ShellBus, ShellShard};
use ahash::AHashMap;
use consensus::{Consensus, MetadataHandle};
use iggy_binary_protocol::{
    Command, ConsensusHeader, ForwardCompletionHeader, GenericHeader, HEADER_SIZE, Operation,
    PrepareHeader, RoutedRequestHeader, WireDecode, WireEncode, WireIdentifier,
};
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use metadata::MetadataSubmitError;
use metadata::impls::metadata::StreamsFrontend;
use metadata::stm::consumer_group::CompleteConsumerGroupRevocationRequest;
use metadata::stm::lifecycle::CompleteLifecycleRequest;
use metadata::stm::stream::{Streams, TruncatePartitionRequest};
use secrecy::ExposeSecret;
use server_common::Message;
use server_common::sharding::IggyNamespace;
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;
use tracing::{trace, warn};

/// In normal operation the primary commits a completion itself, and a backup
/// sees it pending for up to one commit broadcast, or two when the metadata
/// primary does not lead the partition. A backup relays only after that and
/// [`COMPLETION_RELAY_MARGIN`], so a relay stays rare at any legal broadcast.
const COMPLETION_RELAY_BROADCASTS: u32 = 2;

/// Covers the commit of the completion and the delivery of the broadcasts.
const COMPLETION_RELAY_MARGIN: Duration = Duration::from_millis(500);

/// Shard 0's end of the RPC, run for an inbound [`shard::MetadataSubmit`]:
/// a peer shard has verified credentials and owns the session locally, and
/// asks shard 0 (the metadata consensus owner) to run only the consensus
/// proposal. Spawns a task so the awaiting peer is woken once the op
/// commits. Submit failures are returned verbatim so the peer can preserve
/// unknown-outcome retry semantics. Called only from
/// `ServerHost::on_metadata_submit`; `pub(in crate::dispatch)` is the
/// tightest visibility Rust can express, so this doc carries the constraint.
#[allow(clippy::too_many_lines)]
pub(in crate::dispatch) fn handle_metadata_submit<B, MJ, S, SB>(
    shard: Rc<ShellShard<B, MJ, S, SB>>,
    submit: shard::MetadataSubmit,
    liveness: &Rc<RefCell<ConsumerGroupLiveness>>,
    session_timeout: Duration,
    relays: &Rc<RefCell<CompletionRelays>>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let liveness = Rc::clone(liveness);
    let relays = Rc::clone(relays);
    let bus = shard.bus.clone();
    bus.spawn(async move {
        match submit {
            shard::MetadataSubmit::ResolveSegmentDelete {
                namespace, count, created_revision, minimum_metadata_op, reply,
            } => {
                let result = super::segment_delete::resolve(&shard, namespace, count, created_revision, minimum_metadata_op).await;
                let _ = reply.try_send(result);
            }
            shard::MetadataSubmit::ForwardedSegmentDelete(query) => {
                super::segment_delete::answer(&shard, *query.header()).await;
            }
            shard::MetadataSubmit::RetentionTruncate {
                namespace,
                created_revision,
                up_to_offset,
            } => {
                if relay_retention_truncate(&shard, namespace, created_revision, up_to_offset)
                    .await
                {
                    return;
                }
                let committed =
                    commit_retention_truncate(&shard, namespace, created_revision, up_to_offset)
                        .await;
                if let Some(Err(error)) = committed {
                    trace!(
                        ?error,
                        namespace_raw = namespace.inner(),
                        up_to_offset,
                        "retention truncation not committed; the next pass decides again"
                    );
                }
            }

            shard::MetadataSubmit::ConsumerSessionHeartbeat(message) => {
                let metadata = shard.plane.metadata();
                if let Some(consensus) = metadata.consensus.as_ref() {
                    if message.header().replica >= consensus.replica_count() {
                        return;
                    }
                    liveness.borrow_mut().receive(
                        consensus.cluster(),
                        (consensus.is_primary()
                            && consensus.is_normal()
                            && !consensus.is_transferring())
                        .then_some(consensus.view()),
                        &message,
                        shard.bus.monotonic_now(),
                        session_timeout,
                    );
                }
            }
            shard::MetadataSubmit::BindSession {
                vsr_client_id,
                session,
                secret,
                reply,
            } => {
                let attached = shard
                    .plane
                    .metadata()
                    .client_table
                    .borrow_mut()
                    .bind_session(vsr_client_id, session, secret.expose_secret());
                let _ = reply.try_send(attached);
            }
            shard::MetadataSubmit::MetadataCurrentState {
                query,
                user_id,
                reply,
            } => {
                let page = super::metadata_changes::current_state_page(&shard, &query, user_id);
                let _ = reply.try_send(page);
            }
            shard::MetadataSubmit::Register {
                vsr_client_id,
                user_id,
                registration,
                reply,
            } => {
                let bound =
                    submit_register_local_or_forward(&shard, vsr_client_id, user_id, registration)
                        .await;
                let _ = reply.try_send(bound);
            }
            shard::MetadataSubmit::ForwardedRegister {
                vsr_client_id,
                user_id,
                registration,
                nonce,
                origin_replica,
            } => {
                answer_forwarded_register(
                    &shard,
                    vsr_client_id,
                    user_id,
                    registration,
                    nonce,
                    origin_replica,
                )
                .await;
            }
            shard::MetadataSubmit::ForwardedLogout {
                vsr_client_id,
                session,
                request,
                nonce,
                origin_replica,
            } => {
                answer_forwarded_logout(
                    &shard,
                    vsr_client_id,
                    session,
                    request,
                    nonce,
                    origin_replica,
                )
                .await;
            }
            shard::MetadataSubmit::Logout {
                vsr_client_id,
                session,
                request,
                reply,
            } => {
                let outcome =
                    submit_logout_local_or_forward(&shard, vsr_client_id, session, request).await;
                let _ = reply.try_send(outcome);
            }
            shard::MetadataSubmit::ClientRequest { request, reply } => {
                let committed = match request.try_into_typed::<RoutedRequestHeader>() {
                    Ok(typed) => shard
                        .plane
                        .metadata()
                        .submit_request_in_process(typed)
                        .await
                        .ok(),
                    Err(error) => {
                        warn!(?error, "ClientRequest submit: undecodable request header");
                        None
                    }
                };
                let _ = reply.try_send(committed);
            }
            shard::MetadataSubmit::CompleteLifecycle {
                request,
                reporter,
                reply,
            } => {
                let key = CompletionKey::Lifecycle {
                    reporter,
                    metadata_op: request.metadata_op,
                };
                let submitted = if relay_completion(&shard, &relays, key, &request).await {
                    None
                } else {
                    Some(
                        shard
                            .plane
                            .metadata()
                            .submit_complete_lifecycle_in_process(request, reporter)
                            .await,
                    )
                };
                let interval = relays.borrow().interval;
                let report_again = match submitted {
                    Some(Ok(applied)) => applied,
                    // Nothing commits here for a relay, or for a batch that a
                    // view change refused or canceled, so no commit wakes the
                    // reporter, and its periodic pass can be far off. The
                    // answer wakes it after one relay interval.
                    None
                    | Some(Err(MetadataSubmitError::NotPrimary | MetadataSubmitError::Canceled)) => {
                        shard.bus.sleep(interval).await;
                        true
                    }
                    // The commit that ends a short lag finds this batch in
                    // flight and wakes nothing, so the answer follows the
                    // catch-up instead.
                    Some(Err(MetadataSubmitError::NotCaughtUp)) => {
                        let mut left = interval;
                        while !left.is_zero() {
                            let tick = left.min(shard::CONSENSUS_TICK_INTERVAL);
                            shard.bus.sleep(tick).await;
                            left -= tick;
                            if shard.plane.metadata().is_caught_up_primary() {
                                break;
                            }
                        }
                        true
                    }
                    Some(Err(_)) => false,
                };
                let _ = reply.try_send(report_again);
            }
            shard::MetadataSubmit::CompleteRevocation {
                stream_id,
                topic_id,
                partition_id,
                installation,
                partition_op,
                reply,
            } => {
                let namespace = IggyNamespace::new(
                    stream_id as usize,
                    topic_id as usize,
                    partition_id as usize,
                );
                let request = CompleteConsumerGroupRevocationRequest {
                    stream_id: WireIdentifier::numeric(stream_id),
                    topic_id: WireIdentifier::numeric(topic_id),
                    partition_id,
                    installation,
                    partition_op,
                };
                let key = CompletionKey::Revocation {
                    namespace,
                    group_id: installation.group_id,
                };
                let commit = if relay_completion(&shard, &relays, key, &request).await {
                    None
                } else {
                    shard
                        .plane
                        .metadata()
                        .submit_complete_revocation_in_process(request, namespace)
                        .await
                        .ok()
                };
                let _ = reply.try_send(commit);
            }
            shard::MetadataSubmit::ForwardedCompletion(message) => {
                propose_relayed_completion(&shard, &message).await;
            }
            shard::MetadataSubmit::IsMetadataPrimary { reply } => {
                let primary = shard
                    .plane
                    .metadata()
                    .consensus
                    .as_ref()
                    .is_some_and(|c| c.is_normal() && c.is_primary());
                let _ = reply.try_send(primary);
            }
        }
    });
}

/// Submit a replicated client request to the metadata owner (shard 0) and
/// return the committed reply.
///
/// The metadata consensus group lives on shard 0, but the connection lives
/// on the home shard (this shard). Run consensus where it belongs and bring
/// the committed reply back here so the caller can write it to the
/// originating socket -- shard 0 cannot route the reply by the consensus
/// `client` id (it's the VSR id, not the transport/home-shard-encoding id).
/// `None` = no verdict, for example after shard 0 refused the forward. The
/// outcome is unknown, so callers answer `TransientNotCommitted`, which
/// licenses only a replay of the same request id.
#[allow(clippy::future_not_send)]
pub async fn submit_client_request_on_owner<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    request: Message<RoutedRequestHeader>,
) -> Option<Message<GenericHeader>>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    if shard.id == 0 {
        return shard
            .plane
            .metadata()
            .submit_request_in_process(request)
            .await
            .ok();
    }
    let (reply, rx) = shard::channel::<Option<Message<GenericHeader>>>(1);
    shard.forward_metadata_submit(shard::MetadataSubmit::ClientRequest {
        request: request.into_generic(),
        reply,
    });
    rx.recv().await.ok().flatten()
}

/// Commit the retention decision of a tiered partition's primary as a
/// `TruncatePartition`, which every replica's reconciler then enforces.
///
/// `None` when nothing is proposed: the decision is of another incarnation,
/// or the committed watermark already covers it. `Some(Ok(true))` when the
/// truncation committed and applied.
///
/// Only the metadata primary proposes, so every other node gets `NotPrimary`
/// here. A backup hands its decision to the primary with
/// [`relay_retention_truncate`] instead, and the primary runs a relayed
/// decision through this same function, so both origins pass the same checks
/// against the committed state of the node that proposes.
#[allow(clippy::future_not_send)]
async fn commit_retention_truncate<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    namespace: IggyNamespace,
    created_revision: u64,
    up_to_offset: u64,
) -> Option<Result<bool, MetadataSubmitError>>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let metadata = shard.plane.metadata();
    let request = retention_truncate_request(
        metadata.mux_stm.streams(),
        namespace,
        created_revision,
        up_to_offset,
    )?;
    Some(
        metadata
            .submit_truncate_partition_in_process(&request)
            .await,
    )
}

/// The truncation that raises the committed watermark of `namespace` to
/// `up_to_offset`. `None` when the partition is gone or is another
/// incarnation than `created_revision`, and when the watermark is there
/// already: the frame that carried the decision can be older than a commit.
#[allow(clippy::cast_possible_truncation)]
fn retention_truncate_request(
    streams: &Streams,
    namespace: IggyNamespace,
    created_revision: u64,
    up_to_offset: u64,
) -> Option<TruncatePartitionRequest> {
    let committed_up_to = streams
        .with_committed_partition(namespace, |partition| {
            (partition.created_revision == created_revision)
                .then_some(partition.deleted_up_to_offset)
        })
        .flatten()?;
    (up_to_offset > committed_up_to).then(|| TruncatePartitionRequest {
        stream_id: WireIdentifier::numeric(namespace.stream_id() as u32),
        topic_id: WireIdentifier::numeric(namespace.topic_id() as u32),
        partition_id: namespace.partition_id() as u32,
        up_to_offset,
        expected_history: Some(created_revision),
    })
}

/// Relays the retention decision to the metadata primary when this node is a
/// backup that serves its view. `true` for such a backup, relayed or not:
/// only the primary proposes, and the next timed pass decides again, so a
/// relay that cannot be sent is not kept. `false` in every other state, where
/// the caller proposes in process: a primary commits, and a node in a view
/// change refuses.
///
/// A decision that this node's committed state already rules out is not sent.
/// The primary checks it again against its own state, which can be ahead.
#[allow(clippy::future_not_send)]
async fn relay_retention_truncate<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    namespace: IggyNamespace,
    created_revision: u64,
    up_to_offset: u64,
) -> bool
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let metadata = shard.plane.metadata();
    let Some(consensus) = metadata.consensus.as_ref() else {
        return false;
    };
    if !consensus.is_normal_backup() {
        return false;
    }
    let Some(request) = retention_truncate_request(
        metadata.mux_stm.streams(),
        namespace,
        created_revision,
        up_to_offset,
    ) else {
        return true;
    };
    let view = consensus.view();
    let message = relay_message(
        consensus.cluster(),
        view,
        consensus.replica(),
        Operation::TruncatePartition,
        0,
        &request.to_bytes(),
    );
    if let Err(error) = shard
        .bus
        .send_to_replica(
            consensus.primary_index(view),
            message.into_generic().into_frozen(),
        )
        .await
    {
        trace!(
            ?error,
            namespace_raw = namespace.inner(),
            up_to_offset,
            "retention truncation relay not sent; the next pass decides again"
        );
    }
    true
}

/// The commit position a SUCCESSFULLY COMMITTED metadata reply carries, or
/// `None` when the frame promises the caller nothing.
///
/// Only a success promises. Every other frame on this path stamps `commit` with
/// the primary's `commit_max`, an op the caller was never told committed and,
/// on a backup-homed caller, one its own reads would then wait for:
///
/// - an eviction is an `EvictionHeader` whose bytes would cast cleanly as a
///   reply, so the command is checked first (same guard as
///   `build_raw_pat_reply`);
/// - a request-level denial names itself in `ReplyHeader.status`, the channel
///   the SDK peeks before body decode (see `build_deny_reply`);
/// - a transient rejection did not commit and will be replayed;
/// - a TERMINAL pre-consensus rejection (`PreflightOutcome::Reject`, e.g. a
///   fenced session) is a result section carrying a non-transient code, which
///   is byte-identical to a COMMITTED business rejection (duplicate name, bad
///   expiry). Neither is separable here, and neither has to be: a rejection
///   mutated nothing, so the caller has nothing to read back from it, and
///   grading both as no-promise is the only reading that cannot make a read
///   wait for an op that never committed.
///
/// The grading itself is [`committed_reply_header`], shared with the raw-PAT
/// splice, which must admit exactly the same frames.
///
/// Shared with the HTTP write path, which grades the same frames off the same
/// submit entry point ([`submit_client_request_on_owner`]); one classifier is
/// what keeps the two planes' watermarks meaning the same thing.
pub fn committed_reply_commit(reply: &Message<GenericHeader>) -> Option<u64> {
    committed_reply_header(reply).map(|header| header.commit)
}

/// The completion that a relay carries, so that unrelated completions keep
/// separate schedules.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum CompletionKey {
    Lifecycle {
        reporter: u16,
        metadata_op: u64,
    },
    Revocation {
        namespace: IggyNamespace,
        group_id: u64,
    },
}

impl CompletionKey {
    fn is_pending(self, streams: &Streams) -> bool {
        match self {
            Self::Lifecycle { metadata_op, .. } => streams.lifecycle_pending(metadata_op),
            Self::Revocation {
                namespace,
                group_id,
            } => streams
                .consumer_group_pending_revocation(namespace, group_id)
                .is_some(),
        }
    }
}

/// When a backup's shard 0 first saw, or last relayed, each completion. A
/// completion relays once it stayed pending for one interval, and then at most
/// once per interval.
pub(in crate::dispatch) struct CompletionRelays {
    interval: Duration,
    since: AHashMap<CompletionKey, u64>,
    pruned_at: u64,
}

impl CompletionRelays {
    /// The interval is [`COMPLETION_RELAY_BROADCASTS`] periods of
    /// `commit_broadcast_interval` plus [`COMPLETION_RELAY_MARGIN`], so 1.5 s
    /// at the default 500 ms broadcast.
    pub(in crate::dispatch) fn new(commit_broadcast_interval: Duration) -> Self {
        Self {
            interval: commit_broadcast_interval
                .saturating_mul(COMPLETION_RELAY_BROADCASTS)
                .saturating_add(COMPLETION_RELAY_MARGIN),
            since: AHashMap::default(),
            pruned_at: 0,
        }
    }

    /// `true` when `key` relays at `now`, which starts its next interval. A
    /// first sight only records `now`. Once per interval, the completions that
    /// `streams` no longer shows pending go. Age alone cannot prune, because
    /// reconcile passes can be further apart than any interval.
    fn admit(&mut self, key: CompletionKey, now: u64, streams: &Streams) -> bool {
        if !within_relay_interval(self.pruned_at, now, self.interval) {
            self.since.retain(|key, _| key.is_pending(streams));
            self.pruned_at = now;
        }
        let Some(since) = self.since.get_mut(&key) else {
            self.since.insert(key, now);
            return false;
        };
        if within_relay_interval(*since, now, self.interval) {
            return false;
        }
        *since = now;
        true
    }
}

/// A clock that went back counts as due.
fn within_relay_interval(since: u64, now: u64, interval: Duration) -> bool {
    now.checked_sub(since)
        .is_some_and(|elapsed| Duration::from_micros(elapsed) < interval)
}

/// Relays `request` to the metadata primary when this node is a backup that
/// serves its view. `true` for such a backup, relayed or not: only the
/// primary proposes, so nothing applies here, and the reconciler resends on
/// a later wake. `false` in every other state, where the caller proposes in
/// process: a primary commits, and a node in a view change refuses.
#[allow(clippy::future_not_send)]
async fn relay_completion<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    relays: &RefCell<CompletionRelays>,
    key: CompletionKey,
    request: &impl WireEncode,
) -> bool
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let metadata = shard.plane.metadata();
    let Some(consensus) = metadata.consensus.as_ref() else {
        return false;
    };
    if !consensus.is_normal_backup() {
        return false;
    }
    let due =
        relays
            .borrow_mut()
            .admit(key, shard.bus.realtime_micros(), metadata.mux_stm.streams());
    if due {
        let view = consensus.view();
        let message = forward_completion_message(
            consensus.cluster(),
            view,
            consensus.replica(),
            key,
            &request.to_bytes(),
        );
        if let Err(error) = shard
            .bus
            .send_to_replica(
                consensus.primary_index(view),
                message.into_generic().into_frozen(),
            )
            .await
        {
            trace!(?error, ?key, "partition completion relay deferred");
        }
    }
    true
}

/// Proposes a relayed completion in process and never relays it again, so a
/// relay takes one hop. A node that lost primaryship refuses, and the backup
/// relays to the primary of its own view on a later wake. A completion that
/// the applied state no longer shows pending would apply as a no-op, so it is
/// dropped before it takes a prepare. A refusal while this primary catches up,
/// or while the reporter's own batch is in its pipeline, also drops the frame,
/// and the backup relays it again one relay interval later.
///
/// A relayed retention truncation is checked against the committed state of
/// this node the way a local decision is, so a relay of another incarnation,
/// or one that the watermark covers, takes no prepare either.
#[allow(clippy::future_not_send)]
async fn propose_relayed_completion<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    message: &Message<ForwardCompletionHeader>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let header = *message.header();
    let metadata = shard.plane.metadata();
    let streams = metadata.mux_stm.streams();
    // `validate` admits only the two completion operations and the truncation.
    if header.operation == Operation::TruncatePartition {
        let Some((namespace, created_revision, up_to_offset)) =
            decode_relayed_truncation(message.body())
        else {
            trace!(
                header.replica,
                "dropping an undecodable relayed retention truncation"
            );
            return;
        };
        let committed =
            commit_retention_truncate(shard, namespace, created_revision, up_to_offset).await;
        if !matches!(committed, Some(Ok(true))) {
            trace!(
                ?committed,
                header.replica,
                namespace_raw = namespace.inner(),
                up_to_offset,
                "relayed retention truncation not committed"
            );
        }
    } else if header.operation == Operation::CompleteLifecycle {
        let Ok(request) = CompleteLifecycleRequest::decode_from(message.body()) else {
            trace!(
                header.replica,
                "dropping an undecodable relayed lifecycle batch"
            );
            return;
        };
        if !streams.lifecycle_pending(request.metadata_op) {
            trace!(
                header.replica,
                "dropping a relayed batch of a resolved intent"
            );
            return;
        }
        let applied = metadata
            .submit_complete_lifecycle_in_process(request, header.reporter)
            .await;
        if !matches!(applied, Ok(true)) {
            trace!(?applied, header.replica, "relayed lifecycle batch refused");
        }
    } else {
        let Some((request, namespace)) = decode_relayed_revocation(message.body()) else {
            trace!(header.replica, "dropping an undecodable relayed revocation");
            return;
        };
        if streams
            .consumer_group_pending_revocation(namespace, request.installation.group_id)
            .is_none_or(|pending| pending.installation != request.installation)
        {
            trace!(
                header.replica,
                "dropping a relayed revocation that is not pending"
            );
            return;
        }
        if let Err(error) = metadata
            .submit_complete_revocation_in_process(request, namespace)
            .await
        {
            trace!(?error, header.replica, "relayed revocation refused");
        }
    }
}

/// A relayed revocation and the partition it completes. `None` when the body
/// does not decode or names no partition by number.
fn decode_relayed_revocation(
    body: &[u8],
) -> Option<(CompleteConsumerGroupRevocationRequest, IggyNamespace)> {
    let request = CompleteConsumerGroupRevocationRequest::decode_from(body).ok()?;
    let namespace = IggyNamespace::try_new(
        request.stream_id.as_u32()? as usize,
        request.topic_id.as_u32()? as usize,
        request.partition_id as usize,
    )?;
    Some((request, namespace))
}

/// The partition, the incarnation and the offset of a relayed retention
/// truncation. `None` when the body does not decode, names no partition by
/// number, or names no incarnation: without one the truncation could not be
/// bound to the partition the sender decided for.
fn decode_relayed_truncation(body: &[u8]) -> Option<(IggyNamespace, u64, u64)> {
    let request = TruncatePartitionRequest::decode_from(body).ok()?;
    let namespace = IggyNamespace::try_new(
        request.stream_id.as_u32()? as usize,
        request.topic_id.as_u32()? as usize,
        request.partition_id as usize,
    )?;
    Some((namespace, request.expected_history?, request.up_to_offset))
}

fn forward_completion_message(
    cluster: u128,
    view: u32,
    replica: u8,
    key: CompletionKey,
    body: &[u8],
) -> Message<ForwardCompletionHeader> {
    let (operation, reporter) = match key {
        CompletionKey::Lifecycle { reporter, .. } => (Operation::CompleteLifecycle, reporter),
        CompletionKey::Revocation { .. } => (Operation::CompleteConsumerGroupRevocation, 0),
    };
    relay_message(cluster, view, replica, operation, reporter, body)
}

#[allow(clippy::cast_possible_truncation)]
fn relay_message(
    cluster: u128,
    view: u32,
    replica: u8,
    operation: Operation,
    reporter: u16,
    body: &[u8],
) -> Message<ForwardCompletionHeader> {
    let size = HEADER_SIZE + body.len();
    let mut message = Message::<ForwardCompletionHeader>::new(size);
    message.as_mut_slice()[HEADER_SIZE..size].copy_from_slice(body);
    message.transmute_header(|_, header: &mut ForwardCompletionHeader| {
        header.command = Command::ForwardCompletion;
        header.cluster = cluster;
        header.view = view;
        header.replica = replica;
        header.reporter = reporter;
        header.operation = operation;
        header.size = size as u32;
        header.checksum_body = u128::from(iggy_common::calculate_checksum(body));
        header.seal();
    })
}

#[cfg(test)]
mod tests {
    use super::{
        CompletionKey, CompletionRelays, commit_retention_truncate, committed_reply_commit,
        decode_relayed_truncation, forward_completion_message, handle_metadata_submit,
        relay_message, retention_truncate_request,
    };
    use crate::dispatch::host::ServerHost;
    use crate::dispatch::test_support::{
        FIRST_BOOT, SpyBus, TestShard, create_one_partition, prepare_message, request_message,
        test_shard,
    };
    use crate::reply_frame::{build_deny_reply, build_reply_from_bytes};
    use bytes::Bytes;
    use configs::server::ServerConfig;
    use configs::sharding::RECONCILE_PERIODIC_INTERVAL_MAX;
    use consensus::{
        Consensus as _, MetadataHandle, PIPELINE_PREPARE_QUEUE_MAX, PlaneKind, TimeoutManager,
    };
    use futures::FutureExt as _;
    use futures::channel::oneshot;
    use iggy_binary_protocol::requests::consumer_groups::CreateConsumerGroupRequest;
    use iggy_binary_protocol::requests::topics::DeleteTopicRequest;
    use iggy_binary_protocol::{
        Command, ConsensusHeader, ForwardCompletionHeader, GenericHeader, HEADER_SIZE, Operation,
        StartViewChangeHeader, WireEncode, WireIdentifier, WireName,
    };
    use iggy_common::{IggyDuration, IggyError};
    use journal::prepare_journal::PrepareJournal;
    use metadata::MetadataSubmitError;
    use metadata::impls::metadata::StreamsFrontend;
    use metadata::stm::StateMachine as _;
    use metadata::stm::consumer_group::{
        CompleteConsumerGroupRevocationRequest, ConsumerGroupOwnershipTransition,
        JoinConsumerGroupRequest, LeaveConsumerGroupRequest,
    };
    use metadata::stm::lifecycle::{
        CompleteLifecycleRequest, LIFECYCLE_COMPLETIONS_MAX, LifecycleCompletion,
    };
    use metadata::stm::stream::TruncatePartitionRequest;
    use server_common::iobuf::Owned;
    use server_common::sharding::IggyNamespace;
    use server_common::{MESSAGE_ALIGN, Message};
    use shard::ShardHost as _;
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::time::Duration;

    /// Replica 1 of 3 in view 0, so replica 0 is the primary.
    const BACKUP: u8 = 1;
    const REPLICAS: u8 = 3;
    const PRIMARY: u8 = 0;
    const REPORTER: u16 = 3;
    /// The completion arms do not read it.
    const SESSION_TIMEOUT: Duration = Duration::from_secs(30);
    /// The default `cluster.commit_broadcast_interval`.
    const BROADCAST: Duration = Duration::from_millis(500);
    /// Two broadcasts of [`BROADCAST`] and the 500 ms margin.
    const RELAY_INTERVAL_MICROS: u64 = 1_500_000;
    /// The bus time at which a backup first sees a completion.
    const FIRST_SIGHT: u64 = 1_000_000;
    /// The partition op that installed the fence or the owner.
    const PARTITION_OP: u64 = 9;

    /// Commit position of the frames below. Above zero on purpose: `0` is the
    /// "promised nothing" answer, so a fixture at zero could not tell a
    /// classified success from a rejected frame.
    const COMMIT: u64 = 9;

    /// A result-framed body: `[count][index][result]`, then the payload.
    fn result_body(code: u32, payload: &[u8]) -> Bytes {
        let mut body = Vec::new();
        let count = u32::from(code != 0);
        body.extend_from_slice(&count.to_le_bytes());
        if count == 1 {
            body.extend_from_slice(&0u32.to_le_bytes());
            body.extend_from_slice(&code.to_le_bytes());
        }
        body.extend_from_slice(payload);
        Bytes::from(body)
    }

    /// The whole classification in one table: only a successful commit hands
    /// the read gate a floor. Everything else stamps the primary's
    /// `commit_max` into a frame that promised the caller nothing, and a floor
    /// taken from one of those parks the caller's next read on a backup until
    /// the budget expires.
    #[test]
    fn given_a_metadata_reply_when_classified_should_promise_only_a_committed_success() {
        let request = request_message(Operation::CreateStream, 42, 7, 3, &[]);

        let committed =
            build_reply_from_bytes(request.header(), 42, 7, COMMIT, &result_body(0, b"payload"))
                .into_generic();
        assert_eq!(
            committed_reply_commit(&committed),
            Some(COMMIT),
            "a committed success is the one frame that promises the caller its op"
        );

        for code in [
            IggyError::TransientNotCommitted.as_code(),
            IggyError::TransientNotAccepted.as_code(),
            IggyError::UserAlreadyExists.as_code(),
        ] {
            let rejected =
                build_reply_from_bytes(request.header(), 42, 7, COMMIT, &result_body(code, &[]))
                    .into_generic();
            assert_eq!(
                committed_reply_commit(&rejected),
                None,
                "result code {code} mutated nothing, so it promises no read floor"
            );
        }

        let denied = build_deny_reply(
            request.header(),
            42,
            7,
            COMMIT,
            IggyError::Unauthorized.as_code(),
        )
        .into_generic();
        assert_eq!(
            committed_reply_commit(&denied),
            None,
            "a request-level denial names itself in `status` and commits nothing"
        );

        // Any non-`Reply` command stands in for the eviction frame, whose bytes
        // would otherwise cast cleanly as a `ReplyHeader`.
        assert_eq!(
            committed_reply_commit(&request.into_generic()),
            None,
            "only a `Reply` carries a commit position"
        );
    }

    fn lifecycle_batch(metadata_op: u64) -> CompleteLifecycleRequest {
        CompleteLifecycleRequest::new(
            metadata_op,
            0,
            vec![LifecycleCompletion {
                topic_id: 0,
                partition_id: 0,
                partition_op: PARTITION_OP,
            }],
        )
        .expect("one completion with an op is a well-formed batch")
    }

    fn revocation(
        transition: &ConsumerGroupOwnershipTransition,
    ) -> CompleteConsumerGroupRevocationRequest {
        CompleteConsumerGroupRevocationRequest {
            stream_id: WireIdentifier::numeric(transition.stream_id),
            topic_id: WireIdentifier::numeric(transition.topic_id),
            partition_id: transition.partition_id,
            installation: transition.installation,
            partition_op: PARTITION_OP,
        }
    }

    /// Deletes the topic of [`create_one_partition`], so that a lifecycle
    /// intent waits for its partition, and returns the op of that intent.
    fn pending_delete_intent(shard: &TestShard) -> u64 {
        create_one_partition(shard);
        let mux = &shard.plane.metadata().mux_stm;
        let delete = DeleteTopicRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
        };
        let applied = mux
            .update(prepare_message(
                Operation::DeleteTopic,
                1,
                3,
                &delete.to_bytes(),
            ))
            .expect("the delete applies");
        assert_eq!(applied.code, 0);
        let intents = mux.streams().pending_lifecycles();
        assert_eq!(intents.len(), 1, "the delete waits for its partition");
        intents[0].context.metadata_op
    }

    /// Two groups on the only partition of [`create_one_partition`], each
    /// waiting for its first owner.
    fn two_pending_revocations(shard: &TestShard) -> Vec<ConsumerGroupOwnershipTransition> {
        create_one_partition(shard);
        let mux = &shard.plane.metadata().mux_stm;
        for (group_id, client_id) in [(0, 41), (1, 42)] {
            let create = CreateConsumerGroupRequest {
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(0),
                name: WireName::new(format!("group-{group_id}")).expect("a short name"),
            };
            let join = JoinConsumerGroupRequest {
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(0),
                group_id: WireIdentifier::numeric(group_id),
                client_id,
                session: 1,
            };
            for prepare in [
                prepare_message(
                    Operation::CreateConsumerGroup,
                    client_id,
                    1,
                    &create.to_bytes(),
                ),
                prepare_message(Operation::JoinConsumerGroup, client_id, 2, &join.to_bytes()),
            ] {
                assert_eq!(mux.update(prepare).expect("the group op applies").code, 0);
            }
        }
        let pending = mux.streams().consumer_group_pending_revocations();
        assert_eq!(pending.len(), 2, "each group waits for its first owner");
        pending
    }

    /// The relay map of a node at [`BROADCAST`].
    fn relay_map() -> Rc<RefCell<CompletionRelays>> {
        Rc::new(RefCell::new(CompletionRelays::new(BROADCAST)))
    }

    /// Hands shard 0 a reconciler's lifecycle submit at bus time `now`, and
    /// returns the receiver of its answer.
    fn start_lifecycle_submit(
        shard: &Rc<TestShard>,
        bus: &SpyBus,
        completion_relays: &Rc<RefCell<CompletionRelays>>,
        now: u64,
        request: &CompleteLifecycleRequest,
    ) -> shard::Receiver<bool> {
        bus.realtime.set(Some(now));
        let (reply, answer) = shard::channel::<bool>(1);
        handle_metadata_submit(
            Rc::clone(shard),
            shard::MetadataSubmit::CompleteLifecycle {
                request: request.clone(),
                reporter: REPORTER,
                reply,
            },
            &Rc::default(),
            SESSION_TIMEOUT,
            completion_relays,
        );
        answer
    }

    /// Runs a reconciler's lifecycle submit on shard 0 at bus time `now`. Every
    /// wait before the answer resolves at once.
    async fn submit_lifecycle(
        shard: &Rc<TestShard>,
        bus: &SpyBus,
        completion_relays: &Rc<RefCell<CompletionRelays>>,
        now: u64,
        request: &CompleteLifecycleRequest,
    ) -> bool {
        bus.instant_timers.set(true);
        start_lifecycle_submit(shard, bus, completion_relays, now, request)
            .recv()
            .await
            .expect("the submit answers")
    }

    /// Runs a reconciler's lifecycle submit on shard 0 at bus time `now`, and
    /// checks that `case` waits and sends no answer until the test ends the
    /// wait.
    async fn submit_lifecycle_and_end_wait(
        shard: &Rc<TestShard>,
        bus: &SpyBus,
        completion_relays: &Rc<RefCell<CompletionRelays>>,
        now: u64,
        request: &CompleteLifecycleRequest,
        case: &str,
    ) -> bool {
        let end_wait = hold_next_sleep(bus);
        let answer = start_lifecycle_submit(shard, bus, completion_relays, now, request);
        assert_waits(bus, &answer, case).await;
        let _ = end_wait.send(());
        answer.recv().await.expect("the submit answers")
    }

    /// Holds the next sleep on `bus` until the returned sender fires.
    fn hold_next_sleep(bus: &SpyBus) -> oneshot::Sender<()> {
        let (end_wait, wait) = oneshot::channel();
        bus.held_timer.replace(Some(wait));
        end_wait
    }

    /// Lets a started submit run, and checks that `case` sleeps on the held
    /// timer and sent no answer yet. A submit that did not run yet fails the
    /// second check.
    async fn assert_waits(bus: &SpyBus, answer: &shard::Receiver<bool>, case: &str) {
        compio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(
            answer.try_recv().ok(),
            None,
            "{case} sends no answer before the wait ends"
        );
        assert!(bus.held_timer.borrow().is_none(), "{case} waits");
    }

    /// Checks that `case`, a primary that stays behind, refuses the batch and
    /// answers after one relay interval, checked every consensus tick.
    async fn assert_stays_behind_for_one_interval(
        shard: &Rc<TestShard>,
        bus: &SpyBus,
        request: &CompleteLifecycleRequest,
        case: &str,
    ) {
        assert_eq!(
            shard
                .plane
                .metadata()
                .submit_complete_lifecycle_in_process(request.clone(), REPORTER)
                .await,
            Err(MetadataSubmitError::NotCaughtUp),
            "{case} is not caught up"
        );
        assert!(
            submit_lifecycle(shard, bus, &relay_map(), FIRST_SIGHT, request).await,
            "{case} asks the reporter for its next report"
        );
        let sleeps = bus.sleeps.take();
        assert!(
            sleeps
                .iter()
                .all(|sleep| *sleep <= shard::CONSENSUS_TICK_INTERVAL),
            "{case} is checked every consensus tick, slept {sleeps:?}"
        );
        assert_eq!(
            sleeps.iter().sum::<Duration>(),
            Duration::from_micros(RELAY_INTERVAL_MICROS),
            "{case} answers after one relay interval"
        );
    }

    /// Runs a reconciler's revocation submit on shard 0 at bus time `now`.
    async fn submit_revocation(
        shard: &Rc<TestShard>,
        bus: &SpyBus,
        completion_relays: &Rc<RefCell<CompletionRelays>>,
        now: u64,
        transition: &ConsumerGroupOwnershipTransition,
    ) -> Option<u64> {
        bus.realtime.set(Some(now));
        let (reply, commit) = shard::channel::<Option<u64>>(1);
        handle_metadata_submit(
            Rc::clone(shard),
            shard::MetadataSubmit::CompleteRevocation {
                stream_id: transition.stream_id,
                topic_id: transition.topic_id,
                partition_id: transition.partition_id,
                installation: transition.installation,
                partition_op: PARTITION_OP,
                reply,
            },
            &Rc::default(),
            SESSION_TIMEOUT,
            completion_relays,
        );
        commit.recv().await.expect("the submit answers")
    }

    /// Hands shard 0 the relay of a backup at bus time `now`, and lets the arm
    /// run to its first suspension.
    async fn receive_relay(
        shard: &Rc<TestShard>,
        bus: &SpyBus,
        completion_relays: &Rc<RefCell<CompletionRelays>>,
        now: u64,
        key: CompletionKey,
        body: &[u8],
    ) {
        bus.realtime.set(Some(now));
        handle_metadata_submit(
            Rc::clone(shard),
            shard::MetadataSubmit::ForwardedCompletion(forward_completion_message(
                1, 0, BACKUP, key, body,
            )),
            &Rc::default(),
            SESSION_TIMEOUT,
            completion_relays,
        );
        compio::time::sleep(Duration::from_millis(1)).await;
    }

    /// Every frame the backup sent, parsed the way the primary's router parses it.
    fn relays(bus: &SpyBus) -> Vec<(u8, Message<ForwardCompletionHeader>)> {
        bus.replica_sends
            .borrow()
            .iter()
            .map(|(target, frame)| {
                let mut owned = Owned::<MESSAGE_ALIGN>::zeroed(frame.len());
                owned.as_mut_slice().copy_from_slice(frame);
                let relay = Message::<GenericHeader>::try_from(owned)
                    .and_then(Message::try_into_typed::<ForwardCompletionHeader>)
                    .expect("a relay is a sealed and valid forward completion");
                (*target, relay)
            })
            .collect()
    }

    #[compio::test]
    async fn given_normal_backup_when_completion_stays_pending_should_relay_it_once_per_interval() {
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, BACKUP, REPLICAS, FIRST_BOOT));
        let request = lifecycle_batch(pending_delete_intent(&shard));
        let completion_relays = relay_map();
        // The last submit runs on a clock that went back, which counts as due.
        for (now, relayed) in [
            (FIRST_SIGHT, 0),
            (FIRST_SIGHT + RELAY_INTERVAL_MICROS - 1, 0),
            (FIRST_SIGHT + RELAY_INTERVAL_MICROS, 1),
            (FIRST_SIGHT + 2 * RELAY_INTERVAL_MICROS - 1, 1),
            (FIRST_SIGHT + 2 * RELAY_INTERVAL_MICROS, 2),
            (FIRST_SIGHT, 3),
        ] {
            submit_lifecycle(&shard, &bus, &completion_relays, now, &request).await;
            assert_eq!(
                bus.replica_sends.borrow().len(),
                relayed,
                "relays sent by {now} us"
            );
        }
        for (target, relay) in relays(&bus) {
            assert_eq!(target, PRIMARY, "a relay goes to the primary of the view");
            assert_eq!(relay.header().operation, Operation::CompleteLifecycle);
            assert_eq!(relay.header().reporter, REPORTER);
            assert_eq!(relay.header().replica, BACKUP);
            assert_eq!(
                relay.body(),
                &request.to_bytes()[..],
                "the body is the op body"
            );
        }
    }

    /// A relay commits nothing, so only the answer wakes the reporter before
    /// its periodic pass. A backup answers once the batch may relay again. An
    /// earlier answer would make the reporter and shard 0 spin.
    #[compio::test]
    async fn given_normal_backup_when_lifecycle_batch_is_submitted_should_wake_the_reporter_once_its_relay_is_due()
     {
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, BACKUP, REPLICAS, FIRST_BOOT));
        let request = lifecycle_batch(pending_delete_intent(&shard));
        let completion_relays = relay_map();
        for (now, relayed) in [(FIRST_SIGHT, 0), (FIRST_SIGHT + RELAY_INTERVAL_MICROS, 1)] {
            let case = format!("the submit at {now} us");
            assert!(
                submit_lifecycle_and_end_wait(
                    &shard,
                    &bus,
                    &completion_relays,
                    now,
                    &request,
                    &case
                )
                .await,
                "{case} asks the reporter for its next report"
            );
            assert_eq!(
                bus.replica_sends.borrow().len(),
                relayed,
                "relays sent by {now} us"
            );
        }
        assert_eq!(
            *bus.sleeps.borrow(),
            [Duration::from_micros(RELAY_INTERVAL_MICROS); 2],
            "each answer waits until the batch may relay again"
        );
    }

    /// Nothing wakes the reporter when a view change ends, so a node in a view
    /// change answers after one relay interval.
    #[compio::test]
    async fn given_node_in_view_change_when_lifecycle_batch_is_submitted_should_wake_the_reporter_after_one_interval()
     {
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, BACKUP, REPLICAS, FIRST_BOOT));
        let request = lifecycle_batch(pending_delete_intent(&shard));
        let metadata = shard.plane.metadata();
        let consensus = metadata
            .consensus
            .as_ref()
            .expect("shard 0 holds the metadata consensus");
        for _ in 0..TimeoutManager::NORMAL_HEARTBEAT_TICKS {
            consensus.tick(PlaneKind::Metadata);
        }
        let vote = Message::<StartViewChangeHeader>::new(HEADER_SIZE).transmute_header(
            |_, header: &mut StartViewChangeHeader| {
                header.command = Command::StartViewChange;
                header.cluster = consensus.cluster();
                header.group = consensus.group();
                header.view = 1;
                header.replica = PRIMARY;
                header.size = u32::try_from(HEADER_SIZE).unwrap();
                header.seal();
            },
        );
        consensus.handle_start_view_change(PlaneKind::Metadata, vote.header());
        assert!(
            !consensus.is_normal(),
            "a quorum of votes starts a view change"
        );
        assert_eq!(
            metadata
                .submit_complete_lifecycle_in_process(request.clone(), REPORTER)
                .await,
            Err(MetadataSubmitError::NotPrimary)
        );
        let case = "a NotPrimary refusal";
        assert!(
            submit_lifecycle_and_end_wait(&shard, &bus, &relay_map(), FIRST_SIGHT, &request, case)
                .await,
            "{case} asks the reporter for its next report"
        );
        assert_eq!(
            *bus.sleeps.borrow(),
            [Duration::from_micros(RELAY_INTERVAL_MICROS)],
            "{case} waits one relay interval"
        );
    }

    /// A view change cancels the batch in the pipeline of the old primary. The
    /// batch may never commit, and then no commit wakes the reporter.
    #[compio::test]
    async fn given_primary_when_view_change_cancels_lifecycle_batch_should_wake_the_reporter_after_one_interval()
     {
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, PRIMARY, REPLICAS, FIRST_BOOT));
        let request = lifecycle_batch(pending_delete_intent(&shard));
        let consensus = shard
            .plane
            .metadata()
            .consensus
            .as_ref()
            .expect("shard 0 holds the metadata consensus");
        let end_wait = hold_next_sleep(&bus);
        let answer = start_lifecycle_submit(&shard, &bus, &relay_map(), FIRST_SIGHT, &request);
        compio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(
            consensus.pipeline_len(),
            1,
            "the test shard has no journal, so the batch waits for its quorum"
        );
        // Drops the reply sender of the batch, as a view change does.
        consensus.clear_pipeline();
        let case = "a canceled batch";
        assert_waits(&bus, &answer, case).await;
        let _ = end_wait.send(());
        assert!(
            answer.recv().await.expect("the submit answers"),
            "{case} asks the reporter for its next report"
        );
        assert_eq!(
            *bus.sleeps.borrow(),
            [Duration::from_micros(RELAY_INTERVAL_MICROS)],
            "{case} waits one relay interval"
        );
    }

    /// A restarted primary lags until its recovered suffix commits again, and
    /// a primary lags while it applies ops that it knows committed. The commit
    /// that ends the lag finds the batch in flight, so only the answer can
    /// wake the reporter.
    #[compio::test]
    async fn given_lagging_primary_when_lifecycle_batch_is_submitted_should_wake_the_reporter_once_it_caught_up()
     {
        const LAGGING_TICKS: usize = 3;
        let tick = shard::CONSENSUS_TICK_INTERVAL;
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, PRIMARY, REPLICAS, FIRST_BOOT));
        let request = lifecycle_batch(pending_delete_intent(&shard));
        let consensus = shard
            .plane
            .metadata()
            .consensus
            .as_ref()
            .expect("shard 0 holds the metadata consensus");
        let lagging_op = consensus.commit_min() + 1;
        consensus.set_recovery_barrier(lagging_op);
        assert_eq!(
            consensus.commit_min(),
            consensus.commit_max(),
            "only the recovery barrier holds the primary back"
        );
        assert_stays_behind_for_one_interval(
            &shard,
            &bus,
            &request,
            "a primary behind its recovery barrier",
        )
        .await;
        consensus.advance_commit_max(lagging_op);
        assert_stays_behind_for_one_interval(
            &shard,
            &bus,
            &request,
            "a primary that applies committed ops",
        )
        .await;

        let mut end_tick = hold_next_sleep(&bus);
        let answer = start_lifecycle_submit(&shard, &bus, &relay_map(), FIRST_SIGHT, &request);
        for lagged in 0..=LAGGING_TICKS {
            assert_waits(
                &bus,
                &answer,
                &format!("a primary that lagged for {lagged} ticks"),
            )
            .await;
            if lagged == LAGGING_TICKS {
                consensus.advance_commit_min(lagging_op);
            }
            let _ = end_tick.send(());
            end_tick = hold_next_sleep(&bus);
        }
        compio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(
            answer.try_recv().ok(),
            Some(true),
            "the answer comes one tick after the primary caught up"
        );
        assert_eq!(
            *bus.sleeps.borrow(),
            [tick; LAGGING_TICKS + 1],
            "a lagging primary is checked once per tick until it caught up"
        );
    }

    /// While a batch of the reporter waits in the pipeline, the primary
    /// refuses its next one, and the commit of the first wakes the reporter.
    /// A `true` answer would make the reporter submit into the same refusal.
    #[compio::test]
    async fn given_reporter_batch_in_primary_pipeline_when_reporter_submits_again_should_not_wake_the_reporter()
     {
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, PRIMARY, REPLICAS, FIRST_BOOT));
        let request = lifecycle_batch(pending_delete_intent(&shard));
        start_lifecycle_submit(&shard, &bus, &relay_map(), FIRST_SIGHT, &request);
        compio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(
            shard
                .plane
                .metadata()
                .submit_complete_lifecycle_in_process(request.clone(), REPORTER)
                .await,
            Err(MetadataSubmitError::InProgress)
        );

        let answer = start_lifecycle_submit(&shard, &bus, &relay_map(), FIRST_SIGHT, &request);
        compio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(
            answer.try_recv().ok(),
            Some(false),
            "an InProgress refusal answers false at once"
        );
        assert!(
            bus.sleeps.borrow().is_empty(),
            "an InProgress refusal does not wait"
        );
    }

    #[compio::test]
    async fn given_invalid_lifecycle_batch_when_committed_should_not_wake_the_reporter() {
        const SINGLE_REPLICA: u8 = 1;
        const WRONG_STREAM: u32 = 1;
        let directory = tempfile::tempdir().expect("create the metadata journal directory");
        let bus = SpyBus::default();
        let mut shard = test_shard(&bus, PRIMARY, SINGLE_REPLICA, FIRST_BOOT);
        shard.plane.metadata_mut().journal = Some(
            PrepareJournal::open(&directory.path().join("journal.wal"), 0)
                .await
                .expect("open the metadata journal"),
        );
        let shard = Rc::new(shard);
        let metadata_op = pending_delete_intent(&shard);
        let mut request = lifecycle_batch(metadata_op);
        request.stream_id = WRONG_STREAM;
        let metadata = shard.plane.metadata();
        let consensus = metadata
            .consensus
            .as_ref()
            .expect("shard 0 holds the metadata consensus");
        let before_commit = consensus.commit_min();

        let answer = start_lifecycle_submit(&shard, &bus, &relay_map(), FIRST_SIGHT, &request);
        assert!(
            !answer.recv().await.expect("the committed batch answers"),
            "a committed batch rejected during apply must not wake the reporter"
        );
        assert_eq!(consensus.commit_min(), before_commit + 1);
        assert_eq!(consensus.pipeline_len(), 0);
        assert!(metadata.mux_stm.streams().lifecycle_pending(metadata_op));
        assert!(bus.sleeps.borrow().is_empty());
    }

    /// The committed truncation watermark of `namespace`, next to the
    /// incarnation that holds it.
    fn committed_truncation(shard: &TestShard, namespace: IggyNamespace) -> (u64, u64) {
        shard
            .plane
            .metadata()
            .mux_stm
            .streams()
            .with_committed_partition(namespace, |partition| {
                (partition.created_revision, partition.deleted_up_to_offset)
            })
            .expect("the partition is committed")
    }

    #[compio::test]
    async fn given_a_retention_decision_when_this_node_leads_metadata_should_commit_each_offset_once()
     {
        const SINGLE_REPLICA: u8 = 1;
        const DECIDED: u64 = 9;
        let directory = tempfile::tempdir().expect("create the metadata journal directory");
        let bus = SpyBus::default();
        let mut shard = test_shard(&bus, PRIMARY, SINGLE_REPLICA, FIRST_BOOT);
        shard.plane.metadata_mut().journal = Some(
            PrepareJournal::open(&directory.path().join("journal.wal"), 0)
                .await
                .expect("open the metadata journal"),
        );
        let shard = Rc::new(shard);
        let namespace = create_one_partition(&shard);
        let (created_revision, untrimmed) = committed_truncation(&shard, namespace);
        assert_eq!(untrimmed, 0);
        let consensus = shard
            .plane
            .metadata()
            .consensus
            .as_ref()
            .expect("shard 0 holds the metadata consensus");
        let before_commit = consensus.commit_min();

        handle_metadata_submit(
            Rc::clone(&shard),
            shard::MetadataSubmit::RetentionTruncate {
                namespace,
                created_revision,
                up_to_offset: DECIDED,
            },
            &Rc::default(),
            SESSION_TIMEOUT,
            &relay_map(),
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while committed_truncation(&shard, namespace).1 != DECIDED {
            assert!(
                std::time::Instant::now() < deadline,
                "the truncation did not commit"
            );
            compio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(consensus.commit_min(), before_commit + 1);

        // What the watermark covers, and a decision read from another
        // incarnation, propose nothing.
        for (revision, up_to_offset) in [
            (created_revision, DECIDED),
            (created_revision, DECIDED - 1),
            (created_revision + 1, DECIDED + 1),
        ] {
            assert_eq!(
                commit_retention_truncate(&shard, namespace, revision, up_to_offset).await,
                None
            );
        }
        assert_eq!(consensus.commit_min(), before_commit + 1);
        assert_eq!(committed_truncation(&shard, namespace).1, DECIDED);

        assert_eq!(
            commit_retention_truncate(&shard, namespace, created_revision, DECIDED + 1).await,
            Some(Ok(true))
        );
        assert_eq!(committed_truncation(&shard, namespace).1, DECIDED + 1);
        assert_eq!(consensus.commit_min(), before_commit + 2);

        // A truncation that commits as a rejection is not reported as applied.
        let other_incarnation = TruncatePartitionRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            partition_id: 0,
            up_to_offset: DECIDED + 2,
            expected_history: Some(created_revision + 1),
        };
        assert_eq!(
            shard
                .plane
                .metadata()
                .submit_truncate_partition_in_process(&other_incarnation)
                .await,
            Ok(false)
        );
        assert_eq!(committed_truncation(&shard, namespace).1, DECIDED + 1);
    }

    #[compio::test]
    async fn given_a_retention_decision_when_building_its_truncation_should_bind_it_to_the_incarnation()
     {
        let bus = SpyBus::default();
        let shard = test_shard(&bus, PRIMARY, REPLICAS, FIRST_BOOT);
        let namespace = create_one_partition(&shard);
        let (created_revision, _) = committed_truncation(&shard, namespace);

        let request = retention_truncate_request(
            shard.plane.metadata().mux_stm.streams(),
            namespace,
            created_revision,
            9,
        )
        .expect("the watermark is below the decision");

        assert_eq!(request.up_to_offset, 9);
        assert_eq!(request.partition_id as usize, namespace.partition_id());
        assert_eq!(
            request.expected_history,
            Some(created_revision),
            "a partition recreated before the commit must not be truncated"
        );
    }

    #[compio::test]
    async fn given_a_full_primary_pipeline_when_a_retention_decision_arrives_should_refuse_it() {
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, PRIMARY, REPLICAS, FIRST_BOOT));
        let namespace = create_one_partition(&shard);
        let (created_revision, _) = committed_truncation(&shard, namespace);
        let metadata = shard.plane.metadata();
        let consensus = metadata
            .consensus
            .as_ref()
            .expect("shard 0 holds the metadata consensus");
        for up_to_offset in (1..).take(PIPELINE_PREPARE_QUEUE_MAX) {
            assert!(
                commit_retention_truncate(&shard, namespace, created_revision, up_to_offset)
                    .now_or_never()
                    .is_none(),
                "the truncation stays in the pipeline without a journal or quorum"
            );
        }
        assert!(consensus.pipeline_is_full());

        assert_eq!(
            commit_retention_truncate(&shard, namespace, created_revision, u64::MAX).await,
            Some(Err(MetadataSubmitError::PipelineFull))
        );
        assert_eq!(consensus.pipeline_len(), PIPELINE_PREPARE_QUEUE_MAX);
    }

    #[compio::test]
    async fn given_a_retention_decision_when_this_node_is_a_metadata_backup_should_commit_nothing()
    {
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, BACKUP, REPLICAS, FIRST_BOOT));
        let namespace = create_one_partition(&shard);
        let (created_revision, _) = committed_truncation(&shard, namespace);

        assert_eq!(
            commit_retention_truncate(&shard, namespace, created_revision, 9).await,
            Some(Err(MetadataSubmitError::NotPrimary))
        );

        assert_eq!(committed_truncation(&shard, namespace).1, 0);
        assert!(
            bus.replica_sends.borrow().is_empty(),
            "a refused proposal relays nothing, so a relayed decision takes one hop"
        );
    }

    /// The body a backup relays for a retention decision.
    fn retention_truncation(
        namespace: IggyNamespace,
        created_revision: u64,
        up_to_offset: u64,
    ) -> TruncatePartitionRequest {
        TruncatePartitionRequest {
            stream_id: WireIdentifier::numeric(u32::try_from(namespace.stream_id()).unwrap()),
            topic_id: WireIdentifier::numeric(u32::try_from(namespace.topic_id()).unwrap()),
            partition_id: u32::try_from(namespace.partition_id()).unwrap(),
            up_to_offset,
            expected_history: Some(created_revision),
        }
    }

    /// Hands shard 0 the retention decision of a partition primary on this
    /// node, and lets the arm run to its first suspension.
    async fn submit_retention_decision(
        shard: &Rc<TestShard>,
        namespace: IggyNamespace,
        created_revision: u64,
        up_to_offset: u64,
    ) {
        handle_metadata_submit(
            Rc::clone(shard),
            shard::MetadataSubmit::RetentionTruncate {
                namespace,
                created_revision,
                up_to_offset,
            },
            &Rc::default(),
            SESSION_TIMEOUT,
            &relay_map(),
        );
        compio::time::sleep(Duration::from_millis(1)).await;
    }

    /// Hands shard 0 the retention truncation that a backup relayed, and lets
    /// the arm run to its first suspension.
    async fn receive_retention_relay(shard: &Rc<TestShard>, request: &TruncatePartitionRequest) {
        handle_metadata_submit(
            Rc::clone(shard),
            shard::MetadataSubmit::ForwardedCompletion(relay_message(
                1,
                0,
                BACKUP,
                Operation::TruncatePartition,
                0,
                &request.to_bytes(),
            )),
            &Rc::default(),
            SESSION_TIMEOUT,
            &relay_map(),
        );
        compio::time::sleep(Duration::from_millis(1)).await;
    }

    /// Applies `request` to the committed state of `shard`, as replication
    /// from the primary does.
    fn apply_truncation(shard: &TestShard, request: &TruncatePartitionRequest) {
        let applied = shard
            .plane
            .metadata()
            .mux_stm
            .update(prepare_message(
                Operation::TruncatePartition,
                0,
                1,
                &request.to_bytes(),
            ))
            .expect("the truncation applies");
        assert_eq!(applied.code, 0);
    }

    /// How many of the frames this node sent to replicas are relays.
    fn relays_sent(bus: &SpyBus) -> usize {
        bus.replica_sends
            .borrow()
            .iter()
            .filter(|(_, frame)| {
                let mut owned = Owned::<MESSAGE_ALIGN>::zeroed(frame.len());
                owned.as_mut_slice().copy_from_slice(frame);
                Message::<GenericHeader>::try_from(owned)
                    .is_ok_and(|message| message.header().command == Command::ForwardCompletion)
            })
            .count()
    }

    fn pipeline_len(shard: &TestShard) -> usize {
        shard
            .plane
            .metadata()
            .consensus
            .as_ref()
            .expect("shard 0 holds the metadata consensus")
            .pipeline_len()
    }

    #[compio::test]
    async fn given_a_retention_decision_when_this_node_is_a_metadata_backup_should_relay_it_to_the_primary()
     {
        const DECIDED: u64 = 9;
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, BACKUP, REPLICAS, FIRST_BOOT));
        let namespace = create_one_partition(&shard);
        let (created_revision, _) = committed_truncation(&shard, namespace);

        submit_retention_decision(&shard, namespace, created_revision, DECIDED).await;

        let relays = relays(&bus);
        assert_eq!(relays.len(), 1, "one decision is one relay");
        let (target, relay) = &relays[0];
        assert_eq!(*target, PRIMARY, "a relay goes to the primary of the view");
        assert_eq!(relay.header().operation, Operation::TruncatePartition);
        assert_eq!(relay.header().reporter, 0);
        assert_eq!(relay.header().replica, BACKUP);
        assert_eq!(
            relay.body(),
            &retention_truncation(namespace, created_revision, DECIDED).to_bytes()[..],
            "the body is the truncation bound to the incarnation"
        );
        assert_eq!(
            decode_relayed_truncation(relay.body()),
            Some((namespace, created_revision, DECIDED)),
            "the primary reads back what the backup decided"
        );
        assert_eq!(pipeline_len(&shard), 0, "a backup proposes nothing");
        assert_eq!(committed_truncation(&shard, namespace).1, 0);
    }

    /// The backup's committed state can only be behind the primary's, so what
    /// it rules out the primary would drop too.
    #[compio::test]
    async fn given_a_ruled_out_retention_decision_when_this_node_is_a_metadata_backup_should_relay_nothing()
     {
        const COMMITTED: u64 = 9;
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, BACKUP, REPLICAS, FIRST_BOOT));
        let namespace = create_one_partition(&shard);
        let (created_revision, _) = committed_truncation(&shard, namespace);
        apply_truncation(
            &shard,
            &retention_truncation(namespace, created_revision, COMMITTED),
        );
        assert_eq!(committed_truncation(&shard, namespace).1, COMMITTED);

        for (revision, up_to_offset) in [
            (created_revision, COMMITTED),
            (created_revision, COMMITTED - 1),
            (created_revision + 1, COMMITTED + 1),
        ] {
            submit_retention_decision(&shard, namespace, revision, up_to_offset).await;
            assert!(
                bus.replica_sends.borrow().is_empty(),
                "offset {up_to_offset} of incarnation {revision} is not relayed"
            );
        }

        submit_retention_decision(&shard, namespace, created_revision, COMMITTED + 1).await;
        assert_eq!(
            relays(&bus).len(),
            1,
            "a decision above the watermark relays"
        );
    }

    /// The test shard has no journal, so the prepare stays in the pipeline.
    #[compio::test]
    async fn given_a_retention_decision_when_this_node_leads_metadata_should_propose_it_and_relay_nothing()
     {
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, PRIMARY, REPLICAS, FIRST_BOOT));
        let namespace = create_one_partition(&shard);
        let (created_revision, _) = committed_truncation(&shard, namespace);

        submit_retention_decision(&shard, namespace, created_revision, 9).await;

        assert_eq!(pipeline_len(&shard), 1, "the primary proposes in process");
        assert_eq!(relays_sent(&bus), 0, "the primary has no one to relay to");
    }

    #[compio::test]
    async fn given_a_relayed_retention_truncation_when_this_node_leads_metadata_should_commit_it() {
        const SINGLE_REPLICA: u8 = 1;
        const DECIDED: u64 = 9;
        let directory = tempfile::tempdir().expect("create the metadata journal directory");
        let bus = SpyBus::default();
        let mut shard = test_shard(&bus, PRIMARY, SINGLE_REPLICA, FIRST_BOOT);
        shard.plane.metadata_mut().journal = Some(
            PrepareJournal::open(&directory.path().join("journal.wal"), 0)
                .await
                .expect("open the metadata journal"),
        );
        let shard = Rc::new(shard);
        let namespace = create_one_partition(&shard);
        let (created_revision, _) = committed_truncation(&shard, namespace);
        let consensus = shard
            .plane
            .metadata()
            .consensus
            .as_ref()
            .expect("shard 0 holds the metadata consensus");
        let before_commit = consensus.commit_min();

        receive_retention_relay(
            &shard,
            &retention_truncation(namespace, created_revision, DECIDED),
        )
        .await;

        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while committed_truncation(&shard, namespace).1 != DECIDED {
            assert!(
                std::time::Instant::now() < deadline,
                "the relayed truncation did not commit"
            );
            compio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(consensus.commit_min(), before_commit + 1);
        assert_eq!(relays_sent(&bus), 0, "a relay takes one hop");
    }

    /// A relay can be older than a commit, and slab ids are reused after a
    /// delete, so the primary checks a relay against its own committed state.
    /// The test shard has no journal, so a proposed relay stays in the pipeline.
    #[compio::test]
    async fn given_a_relayed_retention_truncation_when_the_committed_state_rules_it_out_should_propose_nothing()
     {
        const COMMITTED: u64 = 9;
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, PRIMARY, REPLICAS, FIRST_BOOT));
        let namespace = create_one_partition(&shard);
        let (created_revision, _) = committed_truncation(&shard, namespace);
        apply_truncation(
            &shard,
            &retention_truncation(namespace, created_revision, COMMITTED),
        );

        for (revision, up_to_offset) in [
            (created_revision, COMMITTED),
            (created_revision, COMMITTED - 1),
            (created_revision + 1, COMMITTED + 1),
        ] {
            receive_retention_relay(
                &shard,
                &retention_truncation(namespace, revision, up_to_offset),
            )
            .await;
            assert_eq!(
                pipeline_len(&shard),
                0,
                "offset {up_to_offset} of incarnation {revision} takes no prepare"
            );
        }
        let gone = IggyNamespace::new(0, 0, 1);
        receive_retention_relay(
            &shard,
            &retention_truncation(gone, created_revision, COMMITTED + 1),
        )
        .await;
        assert_eq!(pipeline_len(&shard), 0, "an unknown partition takes none");

        receive_retention_relay(
            &shard,
            &retention_truncation(namespace, created_revision, COMMITTED + 1),
        )
        .await;
        assert_eq!(
            pipeline_len(&shard),
            1,
            "a relay above the watermark of the same incarnation is proposed"
        );
        assert_eq!(committed_truncation(&shard, namespace).1, COMMITTED);
    }

    #[compio::test]
    async fn given_a_relayed_retention_truncation_when_receiver_is_a_backup_should_neither_propose_nor_relay_again()
     {
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, BACKUP, REPLICAS, FIRST_BOOT));
        let namespace = create_one_partition(&shard);
        let (created_revision, _) = committed_truncation(&shard, namespace);

        receive_retention_relay(
            &shard,
            &retention_truncation(namespace, created_revision, 9),
        )
        .await;

        assert_eq!(pipeline_len(&shard), 0, "only the primary proposes");
        assert!(
            bus.replica_sends.borrow().is_empty(),
            "a relay received by a backup is dropped, not relayed again"
        );
        assert_eq!(committed_truncation(&shard, namespace).1, 0);
    }

    /// Both bodies have the size the frame admits for a truncation, or decode
    /// as one, and neither names a partition incarnation by number.
    #[test]
    fn given_a_relayed_truncation_body_when_it_is_not_bound_to_a_numbered_incarnation_should_not_decode()
     {
        let namespace = IggyNamespace::new(0, 0, 0);
        let bound = retention_truncation(namespace, 7, 9);
        assert_eq!(
            decode_relayed_truncation(&bound.to_bytes()),
            Some((namespace, 7, 9))
        );

        let unbound = TruncatePartitionRequest {
            expected_history: None,
            ..bound.clone()
        };
        assert_eq!(decode_relayed_truncation(&unbound.to_bytes()), None);

        // A four-byte name encodes to the size of a numeric id.
        let by_stream_name = TruncatePartitionRequest {
            stream_id: WireIdentifier::named("abcd").expect("a short name"),
            ..bound.clone()
        };
        assert_eq!(by_stream_name.to_bytes().len(), bound.to_bytes().len());
        assert_eq!(decode_relayed_truncation(&by_stream_name.to_bytes()), None);
        let by_topic_name = TruncatePartitionRequest {
            topic_id: WireIdentifier::named("abcd").expect("a short name"),
            ..bound
        };
        assert_eq!(decode_relayed_truncation(&by_topic_name.to_bytes()), None);
        assert_eq!(decode_relayed_truncation(&[]), None);
    }

    #[compio::test]
    async fn given_full_primary_pipeline_when_lifecycle_batch_is_submitted_should_not_wake_the_reporter()
     {
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, PRIMARY, REPLICAS, FIRST_BOOT));
        let request = lifecycle_batch(pending_delete_intent(&shard));
        let metadata = shard.plane.metadata();
        let consensus = metadata
            .consensus
            .as_ref()
            .expect("shard 0 holds the metadata consensus");
        for reporter in (0..=u16::MAX)
            .filter(|reporter| *reporter != REPORTER)
            .take(PIPELINE_PREPARE_QUEUE_MAX)
        {
            assert!(
                metadata
                    .submit_complete_lifecycle_in_process(request.clone(), reporter)
                    .now_or_never()
                    .is_none(),
                "the batch stays in the pipeline without a journal or quorum"
            );
        }
        assert!(consensus.pipeline_is_full());
        assert_eq!(
            metadata
                .submit_complete_lifecycle_in_process(request.clone(), REPORTER)
                .await,
            Err(MetadataSubmitError::PipelineFull)
        );

        let answer = start_lifecycle_submit(&shard, &bus, &relay_map(), FIRST_SIGHT, &request);
        assert!(
            !answer.recv().await.expect("the refused batch answers"),
            "a PipelineFull refusal must not wake the reporter"
        );
        assert_eq!(consensus.pipeline_len(), PIPELINE_PREPARE_QUEUE_MAX);
        assert!(
            bus.sleeps.borrow().is_empty(),
            "a PipelineFull refusal does not wait"
        );
    }

    /// The relay interval follows `[cluster] commit_broadcast_interval`.
    #[compio::test]
    async fn given_configured_commit_broadcast_when_backup_answers_lifecycle_batch_should_wait_two_broadcasts_and_the_margin()
     {
        let mut server_config = ServerConfig::default();
        server_config.cluster.commit_broadcast_interval =
            IggyDuration::from(Duration::from_secs(1));
        let bus = SpyBus::default();
        bus.instant_timers.set(true);
        let shard = Rc::new(test_shard(&bus, BACKUP, REPLICAS, FIRST_BOOT));
        let shard_handle = Rc::new(RefCell::new(Some(Rc::downgrade(&shard))));
        let host = ServerHost::new(&bus, &shard_handle, Arc::new(server_config));
        let (reply, answer) = shard::channel::<bool>(1);
        host.on_metadata_submit(shard::MetadataSubmit::CompleteLifecycle {
            request: lifecycle_batch(pending_delete_intent(&shard)),
            reporter: REPORTER,
            reply,
        });
        assert!(answer.recv().await.expect("the submit answers"));
        assert_eq!(
            *bus.sleeps.borrow(),
            [Duration::from_millis(2_500)],
            "two broadcasts of 1 s and the 500 ms margin"
        );
    }

    /// The largest batch must pass `validate`, or sealing its relay panics the
    /// shard 0 task of the backup.
    #[test]
    fn given_largest_lifecycle_batch_when_relayed_should_parse_as_forward_completion() {
        let completions = (0..LIFECYCLE_COMPLETIONS_MAX)
            .map(|partition_id| LifecycleCompletion {
                topic_id: 0,
                partition_id: u32::try_from(partition_id).expect("a batch index fits u32"),
                partition_op: PARTITION_OP,
            })
            .collect();
        let request =
            CompleteLifecycleRequest::new(1, 0, completions).expect("a full batch is well formed");
        let key = CompletionKey::Lifecycle {
            reporter: REPORTER,
            metadata_op: request.metadata_op,
        };
        let relay = forward_completion_message(1, 0, BACKUP, key, &request.to_bytes())
            .into_generic()
            .try_into_typed::<ForwardCompletionHeader>()
            .expect("the largest batch is a valid forward completion");
        assert_eq!(relay.body(), &request.to_bytes()[..]);
    }

    /// Reconcile passes can be as far apart as the longest periodic interval,
    /// so only a resolved completion may lose its record.
    #[compio::test]
    async fn given_passes_further_apart_than_two_intervals_when_completion_stays_pending_should_relay_on_each_pass()
     {
        let gap = u64::try_from(RECONCILE_PERIODIC_INTERVAL_MAX.as_micros())
            .expect("the longest periodic interval fits u64");
        assert!(gap > 2 * RELAY_INTERVAL_MICROS);
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, BACKUP, REPLICAS, FIRST_BOOT));
        let request = lifecycle_batch(pending_delete_intent(&shard));
        let completion_relays = relay_map();
        for (pass, relayed) in [(0, 0), (1, 1), (2, 2)] {
            submit_lifecycle(
                &shard,
                &bus,
                &completion_relays,
                FIRST_SIGHT + pass * gap,
                &request,
            )
            .await;
            assert_eq!(
                bus.replica_sends.borrow().len(),
                relayed,
                "relays sent by pass {pass}"
            );
        }
        let mux = &shard.plane.metadata().mux_stm;
        let completed = mux
            .update(prepare_message(
                Operation::CompleteLifecycle,
                1,
                4,
                &request.to_bytes(),
            ))
            .expect("the completion applies");
        assert_eq!(completed.code, 0);
        assert!(!mux.streams().lifecycle_pending(request.metadata_op));
        submit_lifecycle(
            &shard,
            &bus,
            &completion_relays,
            FIRST_SIGHT + 3 * gap,
            &request,
        )
        .await;
        assert_eq!(
            bus.replica_sends.borrow().len(),
            2,
            "a resolved completion loses its record, so it starts over as a first sight"
        );
    }

    /// Two groups that revoke one partition are separate completions, so
    /// neither waits behind the other. A revocation submits on every tick
    /// while it stays pending, so only the relay map paces its relay, here at
    /// a broadcast other than the default.
    #[compio::test]
    async fn given_two_groups_revoking_one_partition_when_both_stay_pending_should_relay_both() {
        /// The largest broadcast that the default heartbeat timeout allows.
        const SLOW_BROADCAST: Duration = Duration::from_millis(1_250);
        /// Two broadcasts of [`SLOW_BROADCAST`] and the 500 ms margin.
        const SLOW_RELAY_INTERVAL_MICROS: u64 = 3_000_000;
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, BACKUP, REPLICAS, FIRST_BOOT));
        let pending = two_pending_revocations(&shard);
        let completion_relays = Rc::new(RefCell::new(CompletionRelays::new(SLOW_BROADCAST)));
        for (now, relayed) in [
            (FIRST_SIGHT, 0),
            (FIRST_SIGHT + RELAY_INTERVAL_MICROS, 0),
            (FIRST_SIGHT + SLOW_RELAY_INTERVAL_MICROS - 1, 0),
            (FIRST_SIGHT + SLOW_RELAY_INTERVAL_MICROS, 2),
        ] {
            for transition in &pending {
                assert_eq!(
                    submit_revocation(&shard, &bus, &completion_relays, now, transition).await,
                    None,
                    "a backup commits nothing itself"
                );
            }
            assert_eq!(
                bus.replica_sends.borrow().len(),
                relayed,
                "relays sent by {now} us"
            );
        }
        for ((target, relay), transition) in relays(&bus).iter().zip(&pending) {
            assert_eq!(*target, PRIMARY, "a relay goes to the primary of the view");
            assert_eq!(
                relay.header().operation,
                Operation::CompleteConsumerGroupRevocation
            );
            assert_eq!(
                relay.body(),
                &revocation(transition).to_bytes()[..],
                "each relay carries the revocation of its own group"
            );
        }
    }

    /// Each relay is still pending on the receiver, so it passes the pre-check.
    /// The receiver keeps one relay map, so a resend through it would go out at
    /// the second receipt, one interval after the first.
    #[compio::test]
    async fn given_relayed_completion_when_receiver_is_a_backup_should_not_relay_again() {
        let bus = SpyBus::default();
        let lifecycle_shard = Rc::new(test_shard(&bus, BACKUP, REPLICAS, FIRST_BOOT));
        let request = lifecycle_batch(pending_delete_intent(&lifecycle_shard));
        let key = CompletionKey::Lifecycle {
            reporter: REPORTER,
            metadata_op: request.metadata_op,
        };
        let completion_relays = relay_map();
        for now in [FIRST_SIGHT, FIRST_SIGHT + RELAY_INTERVAL_MICROS] {
            receive_relay(
                &lifecycle_shard,
                &bus,
                &completion_relays,
                now,
                key,
                &request.to_bytes(),
            )
            .await;
            assert!(
                bus.replica_sends.borrow().is_empty(),
                "a lifecycle relay received at {now} us is proposed in process and never relayed again"
            );
        }
        let revocation_shard = Rc::new(test_shard(&bus, BACKUP, REPLICAS, FIRST_BOOT));
        let transition = &two_pending_revocations(&revocation_shard)[0];
        let key = CompletionKey::Revocation {
            namespace: IggyNamespace::new(0, 0, 0),
            group_id: transition.installation.group_id,
        };
        let completion_relays = relay_map();
        for now in [FIRST_SIGHT, FIRST_SIGHT + RELAY_INTERVAL_MICROS] {
            receive_relay(
                &revocation_shard,
                &bus,
                &completion_relays,
                now,
                key,
                &revocation(transition).to_bytes(),
            )
            .await;
            assert!(
                bus.replica_sends.borrow().is_empty(),
                "a revocation relay received at {now} us is proposed in process and never relayed again"
            );
        }
    }

    /// The primary drops a relay that its applied state already resolved,
    /// because it would commit as a no-op. A pending one takes a prepare. The
    /// test shard has no journal, so each prepare stays in the pipeline.
    #[compio::test]
    async fn given_relayed_lifecycle_batch_when_primary_resolved_its_intent_should_propose_nothing()
    {
        // Another reporter has its own internal client, so its relay does not
        // wait behind the prepare of the first one.
        const OTHER_REPORTER: u16 = REPORTER + 1;
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, PRIMARY, REPLICAS, FIRST_BOOT));
        let request = lifecycle_batch(pending_delete_intent(&shard));
        let metadata = shard.plane.metadata();
        let consensus = metadata
            .consensus
            .as_ref()
            .expect("shard 0 holds the metadata consensus");
        let relay_from = |reporter| CompletionKey::Lifecycle {
            reporter,
            metadata_op: request.metadata_op,
        };
        receive_relay(
            &shard,
            &bus,
            &relay_map(),
            FIRST_SIGHT,
            relay_from(REPORTER),
            &request.to_bytes(),
        )
        .await;
        assert_eq!(
            consensus
                .pipeline_head_header()
                .map(|header| header.operation),
            Some(Operation::CompleteLifecycle),
            "a relay of a pending intent takes a prepare"
        );
        let completed = metadata
            .mux_stm
            .update(prepare_message(
                Operation::CompleteLifecycle,
                1,
                4,
                &request.to_bytes(),
            ))
            .expect("the completion applies");
        assert_eq!(completed.code, 0);
        assert!(
            !metadata
                .mux_stm
                .streams()
                .lifecycle_pending(request.metadata_op)
        );
        receive_relay(
            &shard,
            &bus,
            &relay_map(),
            FIRST_SIGHT,
            relay_from(OTHER_REPORTER),
            &request.to_bytes(),
        )
        .await;
        assert_eq!(
            consensus.pipeline_len(),
            1,
            "a relay of a resolved intent takes no prepare"
        );
    }

    /// A pending revocation of the same group with another installation does
    /// not count: the apply matches the exact installation.
    #[compio::test]
    async fn given_relayed_revocation_when_primary_resolved_it_should_propose_nothing() {
        const REPLACEMENT_CLIENT: u128 = 43;
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, PRIMARY, REPLICAS, FIRST_BOOT));
        let pending = two_pending_revocations(&shard);
        let (completed, left) = (&pending[0], &pending[1]);
        let metadata = shard.plane.metadata();
        let mux = &metadata.mux_stm;
        let group_id = WireIdentifier::numeric(
            u32::try_from(left.installation.group_id).expect("a test group id fits u32"),
        );
        let join = JoinConsumerGroupRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            group_id: group_id.clone(),
            client_id: REPLACEMENT_CLIENT,
            session: 1,
        };
        let leave = LeaveConsumerGroupRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            group_id,
            client_id: left.installation.owner.client_id,
        };
        for prepare in [
            prepare_message(
                Operation::JoinConsumerGroup,
                REPLACEMENT_CLIENT,
                1,
                &join.to_bytes(),
            ),
            prepare_message(
                Operation::LeaveConsumerGroup,
                left.installation.owner.client_id,
                3,
                &leave.to_bytes(),
            ),
            prepare_message(
                Operation::CompleteConsumerGroupRevocation,
                1,
                3,
                &revocation(completed).to_bytes(),
            ),
        ] {
            assert_eq!(mux.update(prepare).expect("the op applies").code, 0);
        }
        let replacement = mux
            .streams()
            .consumer_group_pending_revocation(
                IggyNamespace::new(0, 0, 0),
                left.installation.group_id,
            )
            .expect("the group waits for the member that stayed");
        assert_eq!(replacement.installation.owner.client_id, REPLACEMENT_CLIENT);
        let consensus = metadata
            .consensus
            .as_ref()
            .expect("shard 0 holds the metadata consensus");
        for (transition, proposed, case) in [
            (completed, None, "a completed revocation"),
            (
                left,
                None,
                "an owner that left before it took the partition",
            ),
            (
                &replacement,
                Some(Operation::CompleteConsumerGroupRevocation),
                "the waiting revocation",
            ),
        ] {
            let key = CompletionKey::Revocation {
                namespace: IggyNamespace::new(0, 0, 0),
                group_id: transition.installation.group_id,
            };
            receive_relay(
                &shard,
                &bus,
                &relay_map(),
                FIRST_SIGHT,
                key,
                &revocation(transition).to_bytes(),
            )
            .await;
            assert_eq!(
                consensus
                    .pipeline_head_header()
                    .map(|header| header.operation),
                proposed,
                "the relay of {case}"
            );
        }
    }
}
