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

//! The partition data plane, both sides of the mesh on one page.
//!
//! Server side: [`make_partition_read_handler`] answers `PartitionRead`
//! frames on the owning shard (poll snapshots, consumer offsets, segment
//! deletes). Client side: the funnel routes partition writes through
//! [`dispatch_partition_request`], and the non-replicated read arms call
//! [`handle_poll_messages`] / [`handle_get_consumer_offset`], which read via
//! the shard mesh.
//!
//! Deliberate asymmetry (the plane-split reply trio): the partitions engine
//! replies to committed writes itself, straight from the owning shard, while
//! everything the host builds here -- read bodies, denies, empty-poll shapes
//! -- goes out on the connection's home shard. The reply path is therefore
//! split by plane, not unified, and the deny helpers in `authz` are the
//! third leg (typed status replies for requests that never reach a plane).

use crate::consumer_group::maybe_rewrite_consumer_offset_request;
use crate::dispatch::authz::{
    authorize_partition_op, authorize_partition_read, send_deny_reply, send_non_replicated_deny,
};
use crate::dispatch::submit::submit_client_request_on_owner;
use crate::dispatch::{send_non_replicated_bytes, send_reply_frame, upgrade_shard_handle};
use crate::responses::{
    build_consumer_offset_body, build_empty_reply, build_polled_messages_reply,
    current_metadata_commit, resolve_partition_namespace, resolve_partition_request_namespace,
};
use crate::shell::{ShellBus, ShellShard, ShellShardHandle};
use crate::wire::{request_body, usize_to_u32};
use bytes::Bytes;
use consensus::{Consensus, MetadataHandle, PartitionsHandle, build_result_rejection_reply};
use iggy_binary_protocol::PrepareHeader;
use iggy_binary_protocol::primitives::consumer::WireConsumer;
use iggy_binary_protocol::primitives::polling_strategy::WirePollingStrategy;
use iggy_binary_protocol::requests::consumer_offsets::{
    GetConsumerOffsetRequest, StoreConsumerOffsetRequest,
};
use iggy_binary_protocol::requests::messages::PollMessagesRequest;
use iggy_binary_protocol::requests::segments::DeleteSegmentsRequest;
use iggy_binary_protocol::{
    AckLevel, Command, KIND_CONSUMER_GROUP, Operation, RoutedRequestHeader, WireDecode, WireEncode,
    WireIdentifier,
};
use iggy_common::{IggyError, Permissions, PollingStrategy};
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use message_bus::AUTO_COMMIT_CLIENT_ID;
use metadata::impls::metadata::{
    StreamsFrontend, build_truncate_partition_client_message,
    build_truncate_partition_client_message_with_identifiers,
};
use partitions::{AutoCommitApplied, PollPlan, PollingArgs, PollingConsumer};
use server_common::Message;
use server_common::sharding::IggyNamespace;
use shard::shards_table::ShardsTable;
use shard::{PartitionRead, PartitionReadHandler, PartitionReadReply};
use std::rc::Rc;
use tracing::{debug, warn};

/// Build the per-shard [`PartitionReadHandler`]: on a `PartitionRead` frame
/// (this shard owns the namespace), run the poll / consumer-offset lookup
/// against the local partitions plane and push the result back over the
/// carried reply sender. The requesting shard bounds the wait with a
/// timeout, so a dropped reply degrades to a client-visible read failure.
pub fn make_partition_read_handler<B, MJ, S, SB>(
    shard_handle: &ShellShardHandle<B, MJ, S, SB>,
) -> PartitionReadHandler
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let shard_handle = Rc::clone(shard_handle);
    // Runs synchronously on the shard pump (see `process_lifecycle` ->
    // `on_partition_read`). `build_poll_snapshot` takes a pump-only `&mut`
    // partition borrow (synchronous, so no sibling task can realloc under it) and
    // returns an owned `PollPlan`; only owned data crosses into `spawn_poll_io`. A
    // fully-resident poll replies here without spawning. See the `poll_plan` module docs.
    Rc::new(move |namespace, read, reply| {
        let Some(shard) = upgrade_shard_handle(&shard_handle) else {
            return;
        };
        let partitions = shard.plane.partitions();
        match read {
            PartitionRead::Poll { consumer, args } => {
                match partitions.build_poll_snapshot(&namespace, consumer, &args) {
                    None => {
                        let _ = reply.try_send(PartitionReadReply::NotFound);
                    }
                    Some(plan) if plan.needs_off_pump_io() => {
                        spawn_poll_io(Rc::clone(&shard), namespace, plan, reply);
                    }
                    Some(plan) => {
                        let (fragments, current_offset, auto_commit) = plan.execute_resident();
                        if let Some(applied) = auto_commit {
                            submit_auto_commit(&shard, namespace, &applied);
                        }
                        let _ = reply.try_send(PartitionReadReply::Poll {
                            fragments,
                            current_offset,
                        });
                    }
                }
            }
            PartitionRead::ConsumerOffset { consumer } => {
                let result = match partitions.consumer_offset_read(&namespace, consumer) {
                    Some((stored, current_offset)) => PartitionReadReply::ConsumerOffset {
                        stored,
                        current_offset,
                    },
                    None => PartitionReadReply::NotFound,
                };
                let _ = reply.try_send(result);
            }
            PartitionRead::GroupOffsetState { group_id } => {
                let result = match partitions.group_offset_state(&namespace, group_id) {
                    Some((last_polled, committed)) => PartitionReadReply::GroupOffsetState {
                        last_polled,
                        committed,
                    },
                    None => PartitionReadReply::NotFound,
                };
                let _ = reply.try_send(result);
            }
            PartitionRead::ClearGroupLastPolled { group_id } => {
                let result = match partitions.clear_group_last_polled(&namespace, group_id) {
                    Some(()) => PartitionReadReply::Ack,
                    None => PartitionReadReply::NotFound,
                };
                let _ = reply.try_send(result);
            }
            PartitionRead::ResolveSegmentDeleteOffset { count } => {
                let result = partitions
                    .segment_delete_resolution(&namespace, count)
                    .map_or_else(
                        || PartitionReadReply::NotFound,
                        |(up_to_offset, lagging)| PartitionReadReply::SegmentDeleteOffset {
                            up_to_offset,
                            lagging,
                        },
                    );
                let _ = reply.try_send(result);
            }
        }
    })
}

/// Spawn the off-pump leg of a partition poll: disk read + auto-commit apply on
/// the OWNED plan (disk descriptors, resident-tail `Frozen` clones, `Arc` offset
/// map), then replicate the auto-committed offset and send the reply. Holds no
/// partition reference across the IO, so it is sound concurrently with the
/// pump's `&mut` writes; the auto-commit submit re-borrows synchronously after.
fn spawn_poll_io<B, MJ, S, SB>(
    shard: Rc<ShellShard<B, MJ, S, SB>>,
    namespace: IggyNamespace,
    plan: PollPlan,
    reply: shard::Sender<PartitionReadReply>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let bus = shard.bus.clone();
    bus.spawn(async move {
        // Diagnostic-only wall clock: `elapsed` gates the slow-poll `warn!`
        // below and is never folded into a reply or the deterministic schedule,
        // so it stays sound under the simulator's virtual clock (there it just
        // measures near-zero real time and never fires). Do not derive any
        // replicated or reply value from it, or replay determinism breaks.
        let poll_started = std::time::Instant::now();
        let (fragments, current_offset, auto_commit) = plan.execute().await;
        let elapsed = poll_started.elapsed();
        if elapsed > std::time::Duration::from_secs(1) {
            warn!(
                namespace_raw = namespace.inner(),
                elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
                "slow partition poll; gather side may have timed out"
            );
        }
        // Fire-and-forget: the poll reply is not gated on the offset commit.
        if let Some(applied) = auto_commit {
            submit_auto_commit(&shard, namespace, &applied);
        }
        let _ = reply.try_send(PartitionReadReply::Poll {
            fragments,
            current_offset,
        });
    });
}

/// Replicate a poll's auto-committed offset through the partition consensus so
/// it survives failover, mirroring the explicit `StoreConsumerOffset` path: the
/// same op code, submitted onto the owning shard's own pipeline. Best-effort and
/// fire-and-forget -- the poll reply never waits on it, and a full inbox drops
/// the op at WARN rather than backpressuring the reply.
///
/// The partition plane admits writes on the primary only (it asserts so), and a
/// poll is served on whichever node owns the namespace locally, which may be a
/// backup. So gate on primary status here and drop at WARN otherwise; auto-commit
/// is server-managed best-effort (at-least-once delivery), so a follower-served
/// poll simply does not advance the durable offset.
///
/// Coalescing: an offset the partition's committed high-water already covers is
/// dropped without a consensus op (the steady state for a re-poll of committed
/// data, hence no log). The gate reads committed state only, so an offset that
/// merely sits in flight keeps resubmitting until its covering op commits -- a
/// dropped op self-heals on the next poll instead of being suppressed forever.
fn submit_auto_commit<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    namespace: IggyNamespace,
    applied: &AutoCommitApplied,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    enum AutoCommitGate {
        Submit,
        Covered,
        NotPrimary,
    }
    let gate = shard
        .plane
        .partitions()
        .with_partition(&namespace, |partition| {
            let consensus = partition.consensus();
            if !(consensus.is_primary() && consensus.is_normal() && !consensus.is_transferring()) {
                AutoCommitGate::NotPrimary
            } else if partition.is_auto_commit_offset_covered(
                applied.kind,
                applied.consumer_id,
                applied.offset,
            ) {
                AutoCommitGate::Covered
            } else {
                AutoCommitGate::Submit
            }
        });
    match gate {
        Some(AutoCommitGate::Submit) => {}
        Some(AutoCommitGate::Covered) => return,
        Some(AutoCommitGate::NotPrimary) | None => {
            warn!(
                namespace_raw = namespace.inner(),
                "auto-commit offset not replicated: partition not primary on this node (best-effort)"
            );
            return;
        }
    }
    let message = match build_auto_commit_request(namespace, applied) {
        Ok(message) => message,
        Err(error) => {
            warn!(
                namespace_raw = namespace.inner(),
                error = %error,
                "failed to build auto-commit store-offset request"
            );
            return;
        }
    };
    // Routes by namespace to this same (owning, primary) shard's inbox; the pump
    // admits it next turn exactly like a client store. `dispatch` never blocks.
    shard.dispatch(message.into_generic());
}

/// Build the synthetic `StoreConsumerOffset` request for an auto-commit, keyed
/// to the resolved numeric consumer/group id and stamped with the reserved
/// [`AUTO_COMMIT_CLIENT_ID`] so the commit path skips the (unwaited) reply. The
/// wire stream/topic ids are cosmetic here -- admission and apply key off the
/// header namespace and the consumer id -- but are set from the namespace for a
/// well-formed body. `ack` is `Quorum` so the offset actually replicates.
fn build_auto_commit_request(
    namespace: IggyNamespace,
    applied: &AutoCommitApplied,
) -> Result<Message<RoutedRequestHeader>, IggyError> {
    let request = StoreConsumerOffsetRequest {
        consumer: WireConsumer {
            kind: applied.kind.as_code(),
            id: WireIdentifier::Numeric(applied.consumer_id),
        },
        stream_id: WireIdentifier::Numeric(usize_to_u32(namespace.stream_id())?),
        topic_id: WireIdentifier::Numeric(usize_to_u32(namespace.topic_id())?),
        partition_id: Some(usize_to_u32(namespace.partition_id())?),
        offset: applied.offset,
        ack: AckLevel::Quorum,
    };
    let body = request.to_bytes();
    let header_size = std::mem::size_of::<RoutedRequestHeader>();
    let total_size = header_size + body.len();
    let size = u32::try_from(total_size).map_err(|_| IggyError::InvalidConfiguration)?;
    let mut message = Message::<RoutedRequestHeader>::new(total_size);
    message.as_mut_slice()[header_size..].copy_from_slice(&body);
    Ok(
        message.transmute_header(|_, header: &mut RoutedRequestHeader| {
            *header = RoutedRequestHeader {
                command: Command::Request,
                operation: Operation::StoreConsumerOffset,
                size,
                client: AUTO_COMMIT_CLIENT_ID,
                // The reserved sentinel client is never deduped and never
                // replied to; a nonzero session + request just satisfy the wire
                // header validation.
                session: 1,
                request: 1,
                group: namespace.inner(),
                ..Default::default()
            };
        }),
    )
}

/// Route a partition data-plane op (`SendMessages` / consumer-offset writes)
/// through the shard mesh by namespace: the op belongs to the partition's
/// own consensus group, not the metadata group. The owning shard's
/// partitions plane dedups the request against its group's slice, so
/// `header.client` carries the VSR consensus id (the dedup key) rather than
/// the transport id.
///
/// How the committed reply gets back depends on whether the bus can route that
/// id. HTTP registers each session under its own shard-0 transport id, so the
/// two are equal and the plane's `send_to_client` fires the session's
/// in-process reply slot directly; the request is dispatched and forgotten
/// here (`?ack=none` relies on exactly that: nothing listening, reply shed at
/// the bus). Every other transport registers under a client-chosen id the bus
/// cannot route, so the request is submitted with an in-process channel and
/// the reply is relayed to the socket this shard holds.
///
/// Callers must have authenticated the transport already: `vsr_client_id` /
/// `bound_session` come from its bound VSR session. Every failure before
/// dispatch replies with a nonzero status -- unresolvable namespace,
/// authorization denial, exhausted routable wait -- so the client fails fast
/// instead of wedging on a silent drop or reading a status-0 frame as a
/// committed write.
///
/// `vsr_client_id` keys the consumer-group offset fence (the member id),
/// not the transport id stamped into the partition-op header.
#[allow(clippy::future_not_send)]
pub async fn dispatch_partition_request<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    request: Message<RoutedRequestHeader>,
    vsr_client_id: u128,
    bound_session: u64,
    transport_client_id: u128,
    acting_user_id: Option<u32>,
    session_perms: Option<&Permissions>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let header = *request.header();
    let namespace = match resolve_partition_request_namespace(
        shard,
        header.operation,
        request_body(&request),
        vsr_client_id,
    ) {
        Ok(namespace) => namespace,
        Err(error) => {
            // A partition op against a stream/topic that no longer resolves
            // (e.g. a consumer's trailing auto-commit racing a `delete_stream`,
            // or an explicit partition id that skipped the client-side
            // resolve). The op never reached the partition plane, so a status-0
            // reply would read as a committed ack for work that never happened.
            // A silent drop is no better: the SDK connection processes replies
            // in lockstep and would wedge forever.
            warn!(
                transport_client_id,
                error = %error,
                operation = ?header.operation,
                "partition request with unresolved namespace; replying denied"
            );
            send_deny_reply(
                shard,
                transport_client_id,
                &header,
                IggyError::ResourceNotFound(String::new()).as_code(),
            )
            .await;
            return;
        }
    };
    // Dispatch-time RBAC. The partition plane is not replicated through the
    // metadata STM, so the in-apply gate cannot cover it; authorize here, on
    // the connection's own shard, before burning the routable wait or touching
    // the plane. The namespace resolved above, so its stream/topic are the
    // committed slab ids the permissioner keys on directly. A denial replies
    // the op's frame with an empty body and a nonzero `status` the SDK peeks.
    //
    // Consistency: this reads THIS shard's local committed permissioner. On a
    // peer shard that is a replicated read-mirror, so a permission revocation
    // takes effect on the partition plane only once this shard applies the
    // revoking commit -- an apply-lag window bounded by replication lag.
    // Control-plane ops are exact (gated in-apply, in the same committed order
    // on every replica); this local-read relaxation on the data plane is the
    // accepted trade for keeping partition ops off the metadata consensus.
    let scope = IggyNamespace::from_raw(namespace);
    if let Some(status) = authorize_partition_op(
        shard,
        header.operation,
        acting_user_id,
        scope.stream_id(),
        scope.topic_id(),
        session_perms,
    ) {
        warn!(
            transport_client_id,
            status,
            operation = ?header.operation,
            "partition request denied by authorization; replying with status"
        );
        send_deny_reply(shard, transport_client_id, &header, status).await;
        return;
    }
    // Convergence wait: a CreateTopic commit returns to the client before the
    // per-shard reconcilers seed routing rows and materialise the partition
    // (next wake/periodic tick). An op arriving inside that window is not lost
    // if it skips this wait -- `router::route_typed` falls back to the hash
    // assignment, and the owning shard parks it -- so this is an admission
    // courtesy that keeps the steady state off that park buffer, not a
    // correctness gate. See `wait_for_partition_routable`, which spells out why
    // there is no owner-readiness probe here any more.
    if !wait_for_partition_routable(shard, IggyNamespace::from_raw(namespace)).await {
        // The op never reached the partition plane, so it is safe to re-issue
        // anywhere -- the same contract the plane itself answers for a
        // non-primary routing artifact. A status-0 empty reply here would
        // fabricate a success ack for a write that hit no partition at all.
        warn!(
            transport_client_id,
            namespace,
            operation = ?header.operation,
            "partition request not routable within budget; replying transient"
        );
        send_deny_reply(
            shard,
            transport_client_id,
            &header,
            IggyError::TransientNotAccepted.as_code(),
        )
        .await;
        return;
    }
    // A group consumer-offset op carries the group NAME on the wire; the
    // partition plane keys the offset by the group's monotonic id (the same
    // key the poll path auto-commits under and the read path resolves), so
    // rewrite the consumer id before replication -- the apply layer has no
    // metadata access to resolve it.
    let request = match maybe_rewrite_consumer_offset_request(shard, request) {
        Ok(rewritten) => rewritten,
        Err(error) => {
            warn!(
                transport_client_id,
                error = %error,
                operation = ?header.operation,
                "failed to rewrite consumer-offset request; replying empty"
            );
            send_empty_partition_reply(shard, transport_client_id, &header).await;
            return;
        }
    };
    let request = request.transmute_header(|header, new_header: &mut RoutedRequestHeader| {
        *new_header = header;
        new_header.group = namespace;
        // The VSR consensus id, exactly as metadata ops are stamped: it is the
        // dedup key every replica keys its slice by, and unlike the transport
        // id it stays valid across nodes. Replies therefore cannot be routed
        // by this field -- they ride the submit's channel back to this shard,
        // which owns the socket.
        new_header.client = vsr_client_id;
        // Header validation requires `session > 0` for non-register ops. The
        // slices mint no epoch of their own, so the bound VSR session only
        // satisfies validation here.
        new_header.session = bound_session;
        // The session's owner as this shard resolved it, never the
        // client-supplied value: the dedup slice keys on it to tell a re-minted
        // id's next holder apart from its previous one, so it has to be
        // trustworthy on every replica. Same rule as the metadata plane. The
        // gate above failed closed on `None`, so `0` (unattributed) is only a
        // type-level fallback here.
        new_header.user_id = acting_user_id.unwrap_or(0);
    });
    if vsr_client_id == transport_client_id {
        shard.dispatch(request.into_generic());
        return;
    }
    relay_partition_reply(
        shard,
        IggyNamespace::from_raw(namespace),
        request,
        transport_client_id,
        &header,
    )
    .await;
}

/// Submit a partition write whose reply the bus cannot route (the routed
/// header's `client` is the VSR consensus id, whose bits encode no home shard)
/// and relay the committed reply to the socket this shard holds.
///
/// Only the submit runs on the caller's drain loop: it is what fixes the order
/// two writes from one connection reach the owning shard in. The wait for the
/// commit is spawned, so a connection keeps draining its queued polls and
/// metadata ops instead of holding them behind one replication round trip.
#[allow(clippy::future_not_send)]
async fn relay_partition_reply<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    namespace: IggyNamespace,
    request: Message<RoutedRequestHeader>,
    transport_client_id: u128,
    header: &RoutedRequestHeader,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let Ok(ticket) = shard.partition_submit(namespace, request) else {
        // `PartitionSubmitRefused`: the frame never reached the owning shard,
        // so this is a known outcome and the client can be told now rather
        // than after its read-timeout. Same transient the plane itself answers
        // for a request it could not admit.
        send_deny_reply(
            shard,
            transport_client_id,
            header,
            IggyError::TransientNotAccepted.as_code(),
        )
        .await;
        return;
    };
    let operation = header.operation;
    let shard = Rc::clone(shard);
    // Through the bus, not the runtime directly: the simulator supplies its
    // own executor and virtual clock.
    shard.bus.clone().spawn(async move {
        let Some(reply) = shard.await_partition_submit(ticket).await else {
            // Abandoned or expired. Deliberately silent: the outcome is
            // unknown, and a synthesized failure could contradict a write that
            // commits moments later. The client's read-timeout is the recovery.
            return;
        };
        if let Err(error) = shard
            .bus
            .send_to_client(transport_client_id, reply.into_frozen())
            .await
        {
            warn!(
                transport_client_id,
                operation = ?operation,
                error = %error,
                "failed to forward committed partition reply to its socket"
            );
        }
    });
}

/// Serve `poll_messages`: resolve the partition namespace, run the read on
/// the owning shard ([`shard::IggyShard::partition_read`]), and re-encode
/// the stored batches into the legacy wire `PolledMessages` body.
///
/// Failures reply with an empty body so the SDK fails fast on decode
/// instead of hanging until its read timeout.
#[allow(clippy::future_not_send)]
pub(in crate::dispatch) async fn handle_poll_messages<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    transport_client_id: u128,
    request: &Message<RoutedRequestHeader>,
    user_id: Option<u32>,
    session_perms: Option<&Permissions>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let Ok(wire) = PollMessagesRequest::decode_from(request_body(request)) else {
        // Undecodable poll: keep the fail-fast empty-poll shape.
        send_non_replicated_bytes(
            shard,
            request,
            transport_client_id,
            empty_polled_messages_body(0),
            "poll_messages",
        )
        .await;
        return;
    };
    // Gate on (stream, topic) before touching the partition plane. A resolution
    // miss falls through to the resolve path below (empty-poll / not-found); a
    // denial replies status!=0 with an empty body, distinct from the empty-poll
    // "0 messages" shape.
    if let Some(status) = authorize_partition_read(
        shard,
        &wire.stream_id,
        &wire.topic_id,
        user_id,
        |permissioner, uid, stream_id, topic_id| {
            permissioner.poll_messages(uid, stream_id, topic_id)
        },
        session_perms,
    ) {
        send_non_replicated_deny(shard, request, transport_client_id, status).await;
        return;
    }
    let body = match resolve_poll_request(shard, &wire, request.header().client) {
        Ok((namespace, partition_id, consumer, args)) => {
            match shard
                .partition_read(namespace, PartitionRead::Poll { consumer, args })
                .await
            {
                Some(PartitionReadReply::Poll {
                    fragments,
                    current_offset,
                }) => match build_polled_messages_reply(
                    request.header(),
                    current_metadata_commit(shard),
                    partition_id,
                    current_offset,
                    fragments,
                    shard.plane.partitions().config().encryptor.as_deref(),
                ) {
                    Ok(reply) => {
                        send_reply_frame(shard, transport_client_id, reply, "poll_messages").await;
                        return;
                    }
                    Err(error) => {
                        warn!(
                            transport_client_id,
                            error = %error,
                            "failed to re-encode polled batches; replying empty poll"
                        );
                        empty_polled_messages_body(partition_id)
                    }
                },
                other => {
                    warn!(
                        transport_client_id,
                        namespace = namespace.inner(),
                        reply_was_none = other.is_none(),
                        "partition read failed; replying empty poll"
                    );
                    empty_polled_messages_body(partition_id)
                }
            }
        }
        Err(error) => {
            // A stream, topic, or partition id that does not resolve is a
            // client addressing error and must surface as a typed rejection,
            // not an empty poll a consumer would read as end-of-partition.
            if matches!(
                error,
                IggyError::PartitionNotFound(..)
                    | IggyError::StreamIdNotFound(_)
                    | IggyError::TopicIdNotFound(..)
            ) {
                warn!(
                    transport_client_id,
                    error = %error,
                    "poll_messages rejected: target not found"
                );
                send_non_replicated_deny(shard, request, transport_client_id, error.as_code())
                    .await;
                return;
            }
            // A zero-byte body would panic the SDK's `PolledMessages`
            // decoder; reply the 16-byte empty-poll shape instead. A generation
            // fence (the client's cached assignment is stale after a rebalance)
            // carries the re-sync sentinel so the SDK re-syncs and retries
            // rather than treating the empty poll as end-of-partition.
            warn!(
                transport_client_id,
                error = %error,
                "poll_messages request rejected; replying empty poll"
            );
            let partition_id = if matches!(error, IggyError::ConsumerGroupPartitionNotOwned(..)) {
                iggy_common::RESYNC_REQUIRED_PARTITION_SENTINEL
            } else {
                0
            };
            empty_polled_messages_body(partition_id)
        }
    };
    send_non_replicated_bytes(shard, request, transport_client_id, body, "poll_messages").await;
}

/// Serve `get_consumer_offset`. An empty body decodes as `None` on the SDK
/// side (no offset stored / partition unknown).
// TODO(hubcio): plain local partition_read with no primary gate, so a
// follower answers from its own (possibly lagging) offset state. Needs the
// same is-caught-up-primary gate the auto-commit path has, or an explicit
// read-from-follower contract.
#[allow(clippy::future_not_send)]
pub(in crate::dispatch) async fn handle_get_consumer_offset<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    transport_client_id: u128,
    request: &Message<RoutedRequestHeader>,
    user_id: Option<u32>,
    session_perms: Option<&Permissions>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let Ok(wire) = GetConsumerOffsetRequest::decode_from(request_body(request)) else {
        // Undecodable: an empty body decodes as None (no offset) on the SDK.
        send_non_replicated_bytes(
            shard,
            request,
            transport_client_id,
            Bytes::new(),
            "get_consumer_offset",
        )
        .await;
        return;
    };
    if let Some(status) = authorize_partition_read(
        shard,
        &wire.stream_id,
        &wire.topic_id,
        user_id,
        |permissioner, uid, stream_id, topic_id| {
            permissioner.get_consumer_offset(uid, stream_id, topic_id)
        },
        session_perms,
    ) {
        send_non_replicated_deny(shard, request, transport_client_id, status).await;
        return;
    }
    let body = match resolve_consumer_offset_request(shard, &wire) {
        Ok((namespace, partition_id, consumer)) => {
            match shard
                .partition_read(namespace, PartitionRead::ConsumerOffset { consumer })
                .await
            {
                Some(PartitionReadReply::ConsumerOffset {
                    stored: Some(stored_offset),
                    current_offset,
                }) => build_consumer_offset_body(partition_id, current_offset, stored_offset),
                _ => Bytes::new(),
            }
        }
        // A partition id that does not exist in a resolvable topic is a client
        // addressing error, the same one the poll path denies typed. An empty
        // body decodes as `None` -- indistinguishable from "this consumer has
        // no stored offset yet" -- so the caller cannot tell a typo from a
        // fresh consumer.
        Err(error @ IggyError::PartitionNotFound(..)) => {
            warn!(
                transport_client_id,
                error = %error,
                "get_consumer_offset rejected: partition not found"
            );
            send_non_replicated_deny(shard, request, transport_client_id, error.as_code()).await;
            return;
        }
        Err(error) => {
            warn!(
                transport_client_id,
                error = %error,
                "get_consumer_offset request rejected; replying empty"
            );
            Bytes::new()
        }
    };
    send_non_replicated_bytes(
        shard,
        request,
        transport_client_id,
        body,
        "get_consumer_offset",
    )
    .await;
}

/// Ack a consumer-offset op whose body could not be rewritten for the
/// partition plane with an empty Reply. The SDK connection processes replies
/// in lockstep, so a silent drop wedges every subsequent request on that
/// connection.
#[allow(clippy::future_not_send)]
async fn send_empty_partition_reply<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    transport_client_id: u128,
    request_header: &RoutedRequestHeader,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let commit = current_metadata_commit(shard);
    let reply = build_empty_reply(request_header, transport_client_id, 0, commit);
    if let Err(error) = shard
        .bus
        .send_to_client(transport_client_id, reply.into_generic().into_frozen())
        .await
    {
        warn!(
            transport_client_id,
            error = %error,
            operation = ?request_header.operation,
            "failed to surface empty partition reply"
        );
    }
}

/// Wait (bounded) until this shard holds a routing row for `namespace`. Fast
/// path: row already present -> no wait.
///
/// Covers the post-`CreateTopic` convergence window where the metadata commit
/// has returned to the client but the per-shard reconcilers have not yet seeded
/// routing rows. This is an admission courtesy, not a correctness gate: the row
/// is a cache of the deterministic hash assignment and may exist before the
/// owner has materialised anything, so its presence proves only where the
/// partition belongs. What makes an early arrival safe is the owning shard
/// itself - `park_if_unmaterialised` holds the frame until its partition lands,
/// and `serves_committed_incarnation` refuses to serve a mismatched
/// incarnation. Waiting here simply keeps the steady state off that park
/// buffer, whose overflow is the one path that still sheds a request without
/// replying (`frame_drops_total{variant=partition,reason=park_overflow}`).
///
/// Deliberately no owner-readiness probe. One used to run here, on the theory
/// that the table could not be trusted; it could not close the window either,
/// because the fast path above skipped it in exactly the case it was meant to
/// cover - a row seeded from the hash by a shard that owns nothing. Readiness
/// belongs to the owner, which is where it is now enforced.
#[allow(clippy::future_not_send)]
async fn wait_for_partition_routable<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    namespace: IggyNamespace,
) -> bool
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    const ATTEMPT_DELAY: std::time::Duration = std::time::Duration::from_millis(50);
    // 3s budget at 50ms per attempt. Counting attempts, not reading a
    // wall-clock deadline, keeps the wait virtual under the simulator: the
    // bus sleep advances virtual time, whereas `Instant::now` would not.
    const MAX_ATTEMPTS: u32 = 60;

    let mut attempts = 0u32;
    while shard.shards_table().shard_for(namespace).is_none() {
        if attempts >= MAX_ATTEMPTS {
            return false;
        }
        attempts += 1;
        shard.bus.sleep(ATTEMPT_DELAY).await;
    }
    true
}

/// The 16-byte `PolledMessages` body with zero messages
/// (`[partition_id:4][current_offset:8][count:4]`). The SDK decoder
/// requires at least this header, so failure paths must never reply a
/// zero-byte body.
fn empty_polled_messages_body(partition_id: u32) -> Bytes {
    let mut body = Vec::with_capacity(16);
    body.extend_from_slice(&partition_id.to_le_bytes());
    body.extend_from_slice(&0u64.to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes());
    Bytes::from(body)
}

type DecodedPollRequest = (IggyNamespace, u32, PollingConsumer, PollingArgs);

/// Resolve a decoded poll request into its owning-shard read: namespace,
/// partition, polling consumer, and args. Shared by the TCP dispatch (client
/// id = the connection's bound VSR client) and the HTTP route (client id 0,
/// which fences group polls closed).
#[allow(clippy::cast_possible_truncation)]
pub fn resolve_poll_request<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    wire: &PollMessagesRequest,
    client_id: u128,
) -> Result<DecodedPollRequest, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let strategy = polling_strategy_from_wire(&wire.strategy)?;
    let args = PollingArgs::new(strategy, wire.count, wire.auto_commit);

    // Consumer-group poll: the client selects which of its assigned partitions
    // to read and sends it explicitly. The coordinator FENCES ownership (a stale
    // client whose partition was reassigned is rejected with
    // `ConsumerGroupPartitionNotOwned`, prompting a re-sync) and resolves the
    // group's monotonic id -- the offset key the store rewrite and read path
    // both use, so `next()` reads back the offset it just committed.
    if wire.consumer.kind == KIND_CONSUMER_GROUP {
        let partition_id = wire.partition_id.ok_or(IggyError::InvalidIdentifier)?;
        let group_id = shard
            .plane
            .metadata()
            .mux_stm
            .streams()
            .consumer_group_fence(
                &wire.stream_id,
                &wire.topic_id,
                &wire.consumer.id,
                client_id,
                partition_id,
                // Poll fence: reject a pending-revoked partition so the source
                // re-syncs and skips it (it still commits it via the offset fence).
                true,
            )
            .ok_or(IggyError::ConsumerGroupPartitionNotOwned(
                client_id as u32,
                partition_id,
            ))?;
        let namespace = resolve_partition_namespace(
            shard,
            &wire.stream_id,
            &wire.topic_id,
            Some(partition_id),
        )?;
        #[allow(clippy::cast_possible_truncation)]
        let consumer = PollingConsumer::ConsumerGroup(group_id as usize, partition_id as usize);
        return Ok((namespace, partition_id, consumer, args));
    }

    // Plain-consumer poll: an omitted partition selects partition 0, matching
    // the legacy resolver (`resolve_consumer_with_partition_id` uses
    // `unwrap_or(0)` for `ConsumerKind::Consumer`).
    let partition_id = wire.partition_id.unwrap_or(0);
    let namespace =
        resolve_partition_namespace(shard, &wire.stream_id, &wire.topic_id, Some(partition_id))?;
    let consumer = polling_consumer_from_wire(&wire.consumer, partition_id)?;
    Ok((namespace, partition_id, consumer, args))
}

/// Resolve a decoded consumer-offset read into its owning-shard read:
/// namespace, partition, and polling consumer. Shared by the TCP dispatch and
/// the HTTP route; needs no client id because offset reads are not fenced
/// (any client may read a group's offset, member or not).
pub fn resolve_consumer_offset_request<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    wire: &GetConsumerOffsetRequest,
) -> Result<(IggyNamespace, u32, PollingConsumer), IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    // Omitted partition reads partition 0, matching the legacy resolver for
    // both consumer kinds (`unwrap_or(0)`).
    let partition_id = wire.partition_id.unwrap_or(0);
    let namespace =
        resolve_partition_namespace(shard, &wire.stream_id, &wire.topic_id, Some(partition_id))?;
    // A group offset is keyed by the group's monotonic id (any client may read
    // it, member or not), the same key the write path is rewritten to. An
    // unresolved group (e.g. deleted) has no offset, so the read reports None.
    let consumer = if wire.consumer.kind == KIND_CONSUMER_GROUP {
        let group_id = shard
            .plane
            .metadata()
            .mux_stm
            .streams()
            .resolve_consumer_group_id(&wire.stream_id, &wire.topic_id, &wire.consumer.id)
            .ok_or(IggyError::InvalidIdentifier)?;
        #[allow(clippy::cast_possible_truncation)]
        PollingConsumer::ConsumerGroup(group_id as usize, partition_id as usize)
    } else {
        polling_consumer_from_wire(&wire.consumer, partition_id)?
    };
    Ok((namespace, partition_id, consumer))
}

fn polling_consumer_from_wire(
    consumer: &WireConsumer,
    partition_id: u32,
) -> Result<PollingConsumer, IggyError> {
    // Mirrors the legacy server's `PollingConsumer::resolve_consumer_id`:
    // numeric ids pass through, named consumers hash to a stable u32 so
    // reads derive the same offset-table key the write path stores under.
    let consumer_id = match &consumer.id {
        iggy_binary_protocol::WireIdentifier::Numeric(id) => *id,
        iggy_binary_protocol::WireIdentifier::String(name) => {
            iggy_common::calculate_32(name.as_str().as_bytes())
        }
    } as usize;
    match consumer.kind {
        1 => Ok(PollingConsumer::Consumer(
            consumer_id,
            partition_id as usize,
        )),
        KIND_CONSUMER_GROUP => Ok(PollingConsumer::ConsumerGroup(
            consumer_id,
            partition_id as usize,
        )),
        _ => Err(IggyError::InvalidCommand),
    }
}

fn polling_strategy_from_wire(
    strategy: &WirePollingStrategy,
) -> Result<PollingStrategy, IggyError> {
    let mut mapped = match strategy.kind {
        1 => PollingStrategy::offset(0),
        2 => PollingStrategy::timestamp(iggy_common::IggyTimestamp::from(strategy.value)),
        3 => PollingStrategy::first(),
        4 => PollingStrategy::last(),
        5 => PollingStrategy::next(),
        _ => return Err(IggyError::InvalidCommand),
    };
    mapped.set_value(strategy.value);
    Ok(mapped)
}

/// Handle a client `DeleteSegments`: resolve the requested count to an offset
/// on the owning shard, replicate a `TruncatePartition` through metadata so
/// every replica trims to the same watermark, then ack the client. The local
/// deletion happens later, when each replica's reconciler observes the commit.
///
/// The consensus reply is forwarded verbatim: nothing-to-delete commits a
/// no-op `TruncatePartition(0)` and acks, while a not-primary rejection
/// reaches the client as `TransientNotCommitted` so the SDK replays instead
/// of mistaking a dropped delete for success. Only a malformed / unresolvable
/// request is acked empty without a commit.
#[allow(clippy::future_not_send)]
pub(in crate::dispatch) async fn handle_delete_segments_request<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    transport_client_id: u128,
    bound: Option<(u128, u64)>,
    request: &Message<RoutedRequestHeader>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let header = *request.header();
    let body = request_body(request);

    // An unbound transport cannot be attributed a VSR request sequence; the
    // outer handler already short-circuits these, so this is defensive.
    let Some((vsr_client_id, session)) = bound else {
        return;
    };

    // The client numbers DeleteSegments in the same monotonic request sequence
    // as every other metadata op. So resolve the requested count to a concrete
    // offset on the owning shard, then replicate a `TruncatePartition(offset)`
    // AS the client's own request through the standard owner path: the commit
    // records (client, session, request) in the `ClientTable` on every replica,
    // advancing the watermark. Skipping the commit (or attributing it to an
    // internal id) leaves this request id unrecorded, so the SDK's own retry
    // of it would re-execute instead of deduping. A no-op delete still
    // commits `up_to_offset = 0` (monotonic apply) for the same reason.
    let truncate = match resolve_delete_segments_truncate(
        shard,
        &header,
        vsr_client_id,
        session,
        body,
    )
    .await
    {
        Ok(truncate) => Some(truncate),
        // The owning partition has not converged on the committed log yet, so
        // the delete cannot be resolved to a watermark. Reply with the
        // result-framed transient rejection (under the TruncatePartition
        // operation, which the SDK decodes) so the client replays the same
        // request once the partition catches up. Nothing was submitted, hence
        // the re-issuable-anywhere flavor.
        Err(IggyError::TransientNotAccepted) => {
            let template = build_truncate_partition_client_message(
                &header,
                vsr_client_id,
                session,
                0,
                0,
                0,
                0,
            );
            let reply = build_result_rejection_reply(
                template.header(),
                current_metadata_commit(shard),
                IggyError::TransientNotAccepted.as_code(),
            );
            if let Err(error) = shard
                .bus
                .send_to_client(transport_client_id, reply.into_generic().into_frozen())
                .await
            {
                warn!(
                    transport_client_id,
                    error = %error,
                    "delete_segments: failed to send transient rejection"
                );
            }
            return;
        }
        Err(_) => None,
    };

    let reply = if let Some(truncate) = truncate {
        // Forward the consensus reply verbatim, exactly like the generic
        // metadata path: a committed success acks the delete, and a
        // result-framed `TransientNotCommitted` rejection makes the SDK
        // replay the request. Acking unconditionally here would swallow a
        // not-primary rejection and drop the delete on the floor while the
        // client believes it succeeded.
        let Some(reply) = submit_client_request_on_owner(shard, truncate).await else {
            // Transient submit failure (not primary / view change). Stay
            // silent; the SDK read-timeout replays the same request id,
            // which re-resolves and commits. Acking here would advance the
            // client past an unrecorded request and gap the next metadata
            // op.
            warn!(
                transport_client_id,
                "delete_segments: transient submit; client will replay"
            );
            return;
        };
        reply
    } else {
        // Undecodable body (never produced by the SDK): ack empty so the
        // lockstep stream stays framed; the typed decoder surfaces the
        // failure client-side. Unresolvable-but-well-formed targets commit a
        // typed rejection instead (see the resolve), so only a wire-corrupt
        // request can gap the sequence here.
        let commit = current_metadata_commit(shard);
        build_empty_reply(&header, transport_client_id, session, commit).into_generic()
    };
    if let Err(error) = shard
        .bus
        .send_to_client(transport_client_id, reply.into_frozen())
        .await
    {
        warn!(
            transport_client_id,
            error = %error,
            "delete_segments: failed to send reply"
        );
    }
}

/// Resolve a client `DeleteSegments` to the `TruncatePartition` that commits the
/// trim. Shared by the TCP dispatch and the HTTP listener so both resolve the
/// requested segment count to a concrete watermark identically.
///
/// `template` supplies the wire `cluster` / `view` / `release` and the client's
/// `request` number; `client_id` / `session` are the bound VSR identity the
/// truncate commits under. A resolvable namespace with nothing sealed to delete
/// still yields a `TruncatePartition(up_to_offset = 0)` so the metadata request
/// sequence stays contiguous. `Err` on a malformed body or an unresolved
/// namespace: the TCP caller drops it to a silent replay, the HTTP caller renders
/// the error.
#[allow(clippy::future_not_send)]
#[allow(clippy::cast_possible_truncation)]
pub async fn resolve_delete_segments_truncate<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    template: &RoutedRequestHeader,
    client_id: u128,
    session: u64,
    body: &[u8],
) -> Result<Message<RoutedRequestHeader>, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let parsed = DeleteSegmentsRequest::decode_from(body).map_err(|_| IggyError::InvalidCommand)?;
    let namespace_raw = match resolve_partition_request_namespace(
        shard,
        Operation::DeleteSegments,
        body,
        client_id,
    ) {
        Ok(namespace_raw) => namespace_raw,
        // Unresolvable stream/topic: still commit the truncate, against the
        // client's raw identifiers -- the apply rejects it as a committed
        // result, so the failure is recorded against the client's request id
        // and its retry dedups, while the client gets the typed error an
        // empty ack would swallow.
        Err(error) => {
            debug!(
                client_id,
                %error,
                "delete_segments: unresolved target; committing typed rejection"
            );
            return Ok(build_truncate_partition_client_message_with_identifiers(
                template,
                client_id,
                session,
                parsed.stream_id,
                parsed.topic_id,
                parsed.partition_id,
                0,
            ));
        }
    };
    let namespace = IggyNamespace::from_raw(namespace_raw);
    let up_to_offset = match shard
        .partition_read(
            namespace,
            PartitionRead::ResolveSegmentDeleteOffset {
                count: parsed.segments_count,
            },
        )
        .await
    {
        Some(PartitionReadReply::SegmentDeleteOffset {
            up_to_offset: Some(offset),
            ..
        }) => offset,
        // Nothing sealed to delete on a replica that has not converged on the
        // replicated log (a backup behind the commit frontier may be missing
        // whole sealed segments). Answering now would commit a no-op truncate
        // and silently drop the delete, so surface a transient and let the
        // client replay once the partition catches up. A converged primary
        // whose resident tail is merely unflushed settles as a no-op below.
        Some(PartitionReadReply::SegmentDeleteOffset {
            up_to_offset: None,
            lagging: true,
        }) => {
            debug!(
                client_id,
                namespace_raw, "delete_segments: partition not converged; transient"
            );
            return Err(IggyError::TransientNotAccepted);
        }
        other => {
            debug!(
                client_id,
                namespace_raw,
                reply = ?other,
                "delete_segments: nothing to delete; committing no-op truncate"
            );
            0
        }
    };
    Ok(build_truncate_partition_client_message(
        template,
        client_id,
        session,
        namespace.stream_id() as u32,
        namespace.topic_id() as u32,
        namespace.partition_id() as u32,
        up_to_offset,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::test_support::{
        SpyBus, TestMux, TestShard, prepare_message, request_message,
    };
    use iggy_binary_protocol::ReplyHeader;
    use iggy_binary_protocol::primitives::partition_assignment::CreatedPartitionAssignment;
    use iggy_binary_protocol::requests::messages::SendMessagesHeader;
    use iggy_binary_protocol::requests::streams::CreateStreamRequest;
    use iggy_binary_protocol::requests::topics::{
        CreateTopicRequest, CreateTopicWithAssignmentsRequest,
    };
    use iggy_binary_protocol::{WireName, WireOptions, WirePartitioning};
    use iggy_common::defaults::DEFAULT_ROOT_USER_ID;
    use metadata::IggyMetadata;
    use metadata::stm::StateMachine as _;
    use partitions::{IggyPartitions, PartitionPathLayout, PartitionsConfig};
    use server_common::MessageBag;
    use server_common::sharding::ShardId;
    use shard::metrics::ShardMetrics;
    use shard::shards_table::PapayaShardsTable;
    use shard::{
        LifecycleFrame, PartitionConsensusConfig, ReconcileOp, ReplicaTopology, ShardFrame,
        ShardIdentity, shard_channel,
    };

    /// A partition write whose routable wait exhausts (namespace committed,
    /// but no reconciler ever seeds this shard's routing row -- the state a
    /// teardown/rematerialise churn leaves behind) must answer a nonzero
    /// retriable status. A status-0 empty reply is a fabricated success: the
    /// SDK grades the send as acknowledged while zero bytes reached any
    /// partition.
    #[compio::test]
    async fn unroutable_partition_send_must_reply_transient_error_not_success() {
        const VSR_CLIENT: u128 = 1;
        const SESSION: u64 = 1;
        const TRANSPORT: u128 = 91;
        const STATUS_OFFSET: usize = std::mem::offset_of!(ReplyHeader, status);

        let bus = SpyBus::default();
        let metadata = IggyMetadata::new(None, None, None, None, TestMux::default(), None);
        let partitions = IggyPartitions::new(
            ShardId::new(0),
            PartitionsConfig {
                messages_required_to_save: 1,
                size_of_messages_required_to_save: iggy_common::IggyByteSize::from(1024_u64),
                enforce_fsync: false,
                validate_checksum: true,
                segment_size: iggy_common::IggyByteSize::from(1_048_576_u64),
                preallocate_segments: false,
                encryptor: None,
                path_layout: PartitionPathLayout::default(),
            },
        );
        let shard = Rc::new(TestShard::without_inbox(
            ShardIdentity::new(0, "unroutable-send-test".to_string()),
            bus.clone(),
            metadata,
            partitions,
            PapayaShardsTable::new(),
            PartitionConsensusConfig::new(1, ReplicaTopology::new(0, 1), bus.clone()),
        ));
        let md = shard.plane.metadata();

        // Committed stream 0 / topic 0 / partition 0, applied straight into
        // the STM: the namespace resolves and root authorizes, but no
        // reconciler runs, so the shards table never gains a routing row and
        // the routable wait exhausts its budget.
        md.mux_stm.users().ensure_root_user("iggy", "hash");
        let create_stream = CreateStreamRequest {
            name: WireName::new("stream").unwrap(),
            options: WireOptions::empty(),
        };
        md.mux_stm
            .update(prepare_message(
                Operation::CreateStream,
                VSR_CLIENT,
                1,
                &create_stream.to_bytes(),
            ))
            .unwrap();
        let create_topic = CreateTopicWithAssignmentsRequest {
            request: CreateTopicRequest {
                stream_id: WireIdentifier::numeric(0),
                partitions_count: 1,
                name: WireName::new("topic").unwrap(),
                options: WireOptions::empty(),
            },
            derived_options: WireOptions::empty(),
            partitions: vec![CreatedPartitionAssignment {
                partition_id: 0,
                consensus_group_id: 1,
            }],
            created_view: 0,
        };
        md.mux_stm
            .update(prepare_message(
                Operation::CreateTopicWithAssignments,
                VSR_CLIENT,
                2,
                &create_topic.to_bytes(),
            ))
            .unwrap();
        assert!(
            md.mux_stm
                .streams()
                .namespace_from_partition(
                    &WireIdentifier::numeric(0),
                    &WireIdentifier::numeric(0),
                    0
                )
                .is_some(),
            "seeded namespace must resolve, or the unresolved-namespace path \
             would reply instead of the exhausted routable wait"
        );

        let send_header = SendMessagesHeader {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            partitioning: WirePartitioning::PartitionId(0),
            messages_count: 1,
        };
        let send_metadata = send_header.to_bytes();
        let mut send_body = Vec::with_capacity(4 + send_metadata.len());
        send_body.extend_from_slice(&u32::try_from(send_metadata.len()).unwrap().to_le_bytes());
        send_body.extend_from_slice(&send_metadata);
        let request = request_message(Operation::SendMessages, VSR_CLIENT, SESSION, 1, &send_body);

        dispatch_partition_request(
            &shard,
            request,
            VSR_CLIENT,
            SESSION,
            TRANSPORT,
            Some(DEFAULT_ROOT_USER_ID),
            None,
        )
        .await;

        let replies = bus.client_replies.borrow();
        assert_eq!(replies.len(), 1, "one reply frame for the failed send");
        let (client, frame) = &replies[0];
        assert_eq!(*client, TRANSPORT, "reply must target the transport id");
        let status =
            u32::from_le_bytes(frame[STATUS_OFFSET..STATUS_OFFSET + 4].try_into().unwrap());
        assert_eq!(
            status,
            IggyError::TransientNotAccepted.as_code(),
            "an unroutable partition write must surface the retriable \
             transient status; status 0 with an empty body grades as a \
             successfully acknowledged send"
        );
    }

    /// A send that reaches the owning shard while its namespace is
    /// tombstoned (the teardown fence a delete/recreate churn sets before
    /// the disk delete) must answer the retriable transient status. The
    /// partition plane's own tombstone guard drops the frame without any
    /// reply; the transports decode replies in lockstep, so that silence
    /// wedges the connection until the SDK's response read-timeout.
    #[compio::test]
    async fn tombstoned_partition_send_must_reply_transient_error_not_silence() {
        const TRANSPORT: u128 = 91;
        const SESSION: u64 = 1;
        const STATUS_OFFSET: usize = std::mem::offset_of!(ReplyHeader, status);

        let bus = SpyBus::default();
        let metadata = IggyMetadata::new(None, None, None, None, TestMux::default(), None);
        let partitions = IggyPartitions::new(
            ShardId::new(0),
            PartitionsConfig {
                messages_required_to_save: 1,
                size_of_messages_required_to_save: iggy_common::IggyByteSize::from(1024_u64),
                enforce_fsync: false,
                validate_checksum: true,
                segment_size: iggy_common::IggyByteSize::from(1_048_576_u64),
                preallocate_segments: false,
                encryptor: None,
                path_layout: PartitionPathLayout::default(),
            },
        );
        let shard = Rc::new(TestShard::without_inbox(
            ShardIdentity::new(0, "tombstoned-send-test".to_string()),
            bus.clone(),
            metadata,
            partitions,
            PapayaShardsTable::new(),
            PartitionConsensusConfig::new(1, ReplicaTopology::new(0, 1), bus.clone()),
        ));

        let namespace = IggyNamespace::new(0, 0, 0);
        shard.plane.partitions().tombstone(namespace);

        let request = request_message(Operation::SendMessages, TRANSPORT, SESSION, 1, &[])
            .transmute_header(|header, new_header: &mut RoutedRequestHeader| {
                *new_header = header;
                new_header.group = namespace.inner();
            });
        shard.on_message(MessageBag::Request(request)).await;

        let replies = bus.client_replies.borrow();
        assert_eq!(
            replies.len(),
            1,
            "a send into a tombstoned namespace must produce a reply frame; \
             silence wedges the connection's lockstep decode"
        );
        let (client, frame) = &replies[0];
        assert_eq!(*client, TRANSPORT, "reply must target the request's client");
        let status =
            u32::from_le_bytes(frame[STATUS_OFFSET..STATUS_OFFSET + 4].try_into().unwrap());
        assert_eq!(
            status,
            IggyError::TransientNotAccepted.as_code(),
            "a tombstoned-namespace send must surface the retriable transient \
             status so the SDK replays it after the partition rematerialises"
        );
    }

    /// A send parked for a namespace that is torn down before materialising
    /// (create -> delete before the reconciler's `InsertOwned`) is discarded
    /// on `ConfirmRemove`. The discard must stage the same retriable
    /// transient deny toward the client -- through the shard's own pump as a
    /// `ForwardClientSend` -- instead of dropping the request without any
    /// reply.
    #[compio::test]
    async fn discarded_parked_partition_send_must_reply_transient_error_not_silence() {
        const TRANSPORT: u128 = 91;
        const SESSION: u64 = 1;
        const STATUS_OFFSET: usize = std::mem::offset_of!(ReplyHeader, status);

        let bus = SpyBus::default();
        let metadata = IggyMetadata::new(None, None, None, None, TestMux::default(), None);
        let partitions = IggyPartitions::new(
            ShardId::new(0),
            PartitionsConfig {
                messages_required_to_save: 1,
                size_of_messages_required_to_save: iggy_common::IggyByteSize::from(1024_u64),
                enforce_fsync: false,
                validate_checksum: true,
                segment_size: iggy_common::IggyByteSize::from(1_048_576_u64),
                preallocate_segments: false,
                encryptor: None,
                path_layout: PartitionPathLayout::default(),
            },
        );
        // Real sender ring so the staged deny is observable: the test holds
        // the receiving ends of this shard's own lanes. The deny is a client
        // Reply forward, so it lands on the REPLY lane.
        let (sender, _pump_rx, reply_rx) = shard_channel(0, 16, 16);
        let (_inbox_tx, inbox_rx, reply_inbox_rx) = shard_channel(0, 1, 1);
        let shard = TestShard::new(
            ShardIdentity::new(0, "discarded-parked-send-test".to_string()),
            bus.clone(),
            Rc::new(|_, _| {}),
            Rc::new(|_, _| {}),
            Rc::new(|_| {}),
            Rc::new(|_| {}),
            Rc::new(|_, _, _| {}),
            metadata,
            partitions,
            vec![sender],
            inbox_rx,
            reply_inbox_rx,
            PapayaShardsTable::new(),
            PartitionConsensusConfig::new(1, ReplicaTopology::new(0, 1), bus.clone()),
            None,
            ShardMetrics::for_shard(),
        )
        .expect("single-sender ring is canonically ordered");

        let namespace = IggyNamespace::new(0, 0, 0);
        let request = request_message(Operation::SendMessages, TRANSPORT, SESSION, 1, &[])
            .transmute_header(|header, new_header: &mut RoutedRequestHeader| {
                *new_header = header;
                new_header.group = namespace.inner();
            });
        // Namespace neither materialised nor tombstoned: the frame parks.
        shard.on_message(MessageBag::Request(request)).await;

        shard.enqueue_reconcile_op(ReconcileOp::ConfirmRemove { namespace });
        shard.apply_reconcile_ops();

        let mut denies = Vec::new();
        while let Ok(frame) = reply_rx.try_recv() {
            if let ShardFrame::Lifecycle(LifecycleFrame::ForwardClientSend { client_id, msg }) =
                frame
            {
                denies.push((client_id, msg.into_contiguous().as_slice().to_vec()));
            }
        }
        assert_eq!(
            denies.len(),
            1,
            "discarding a parked client request must stage exactly one deny \
             reply; silence wedges the connection's lockstep decode"
        );
        let (client, frame) = &denies[0];
        assert_eq!(*client, TRANSPORT, "deny must target the request's client");
        let status =
            u32::from_le_bytes(frame[STATUS_OFFSET..STATUS_OFFSET + 4].try_into().unwrap());
        assert_eq!(
            status,
            IggyError::TransientNotAccepted.as_code(),
            "a discarded parked send must surface the retriable transient \
             status so the SDK replays it instead of timing out"
        );
    }
}
