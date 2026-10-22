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

//! Partition requests and replies through the shard mesh.
//!
//! Writes enter through [`dispatch_partition_request`]. Reads enter through
//! [`handle_poll_messages`] and [`handle_get_consumer_offset`], then execute
//! on the owning shard. The partition engine sends replies for committed writes;
//! this module sends read replies and admission errors through the connection's
//! home shard.

use crate::consumer_group::maybe_rewrite_consumer_offset_request;
use crate::dispatch::authz::{
    authorize_attached, authorize_partition_op, authorize_partition_read,
    authorize_served_partition_read,
};
use crate::dispatch::failure::{
    ClientOrigin, FrameChannel, send_deny_reply, send_host_frame, send_non_replicated_bytes,
    send_non_replicated_deny, send_result_rejection,
};
use crate::dispatch::submit::submit_client_request_on_owner;
use crate::dispatch::{ConsumerProgress, InFlight};
use crate::namespace::{resolve_partition_namespace, resolve_partition_request_namespace};
use crate::reply_frame::{
    build_consumer_offset_body, build_polled_messages_reply, current_metadata_commit,
};
use crate::shell::{ShellBus, ShellShard};
use crate::wire::request_body;
use bytes::Bytes;
use consensus::client_table::SessionAttachment;
use consensus::{MetadataHandle, PartitionsHandle};
use iggy_binary_protocol::PrepareHeader;
use iggy_binary_protocol::codes::{GET_CONSUMER_OFFSET_CODE, POLL_MESSAGES_CODE};
use iggy_binary_protocol::primitives::consumer::WireConsumer;
use iggy_binary_protocol::primitives::partition_history::PartitionContext;
use iggy_binary_protocol::primitives::polling_strategy::WirePollingStrategy;
use iggy_binary_protocol::requests::consumer_offsets::{
    DeleteConsumerOffsetRequest, GetConsumerOffsetRequest, StoreConsumerOffsetRequest,
};
use iggy_binary_protocol::requests::messages::PollMessagesRequest;
use iggy_binary_protocol::requests::segments::DeleteSegmentsRequest;
use iggy_binary_protocol::responses::messages::poll_messages::PollMessagesResponseHeader;
use iggy_binary_protocol::{
    GenericHeader, KIND_CONSUMER, KIND_CONSUMER_GROUP, KIND_EXTERNAL_GROUP, Operation,
    RequestHeader, RoutedRequestHeader, WireDecode, WireEncode, WireIdentifier,
};
use iggy_common::{ConsumerKind, IggyError, PollingStrategy, RESYNC_REQUIRED_PARTITION_SENTINEL};
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use message_bus::BusMessage;
use metadata::impls::metadata::{
    StreamsFrontend, build_truncate_partition_client_message,
    build_truncate_partition_client_message_with_identifiers,
};
use metadata::permissioner::Permissioner;
use metadata::stm::stream::{PollMetadata, TruncatePartitionRequest};
use partitions::{PollingArgs, PollingConsumer};
use server_common::Message;
use server_common::sharding::IggyNamespace;
use shard::shards_table::ShardsTable;
use shard::{ConsumerAttachment, PartitionRead, PartitionReadReply};
use std::rc::Rc;
use tracing::{debug, warn};

/// Whether [`dispatch_partition_request`] handed its request to the partition
/// plane. After `Denied`, the deny reply has already gone to the transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionAdmission {
    Admitted,
    Denied,
}

/// Route a partition data-plane op (`SendMessages` / consumer-offset writes)
/// through the shard mesh by namespace: the op belongs to the partition's
/// own consensus group, not the metadata group. The owning shard's
/// partitions plane dedups the request against its group's slice, so
/// `header.client` carries the VSR consensus id (the dedup key) rather than
/// the transport id.
///
/// Every request relays: the owning shard returns the committed reply over an
/// in-process channel, and this shard sends it to `origin`, the connection the
/// request arrived on. For HTTP that is the reply slot installed for its
/// session, including detached `?ack=none` writes. Attached requests also keep
/// their session fence through the owning shard's materialization queue.
///
/// Callers must have authenticated the transport already: `vsr_client_id` /
/// `bound_session` come from its bound VSR session. Every failure before
/// dispatch replies with a nonzero status -- unresolvable namespace,
/// authorization denial, exhausted routable wait -- so the client fails fast
/// instead of wedging on a silent drop or reading a status-0 frame as a
/// committed write. Such a failure returns [`PartitionAdmission::Denied`].
///
/// `vsr_client_id` keys the consumer-group offset fence (the member id),
/// not the transport id stamped into the partition-op header.
#[allow(
    clippy::future_not_send,
    clippy::too_many_arguments,
    clippy::too_many_lines
)]
pub async fn dispatch_partition_request<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    request: Message<RoutedRequestHeader>,
    vsr_client_id: u128,
    bound_session: u64,
    origin: ClientOrigin,
    acting_user_id: Option<u32>,
    consumer_session: Option<(u128, SessionAttachment)>,
    resolved_namespace: Option<u64>,
    in_flight: Option<InFlight>,
) -> PartitionAdmission
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let transport_client_id = origin.id;
    let header = *request.header();
    if !shard
        .plane
        .metadata()
        .applied_frontier()
        .partition_requests_allowed(shard.bus.monotonic_now())
    {
        send_deny_reply(
            shard,
            origin,
            &header,
            IggyError::TransientNotAccepted.as_code(),
        )
        .await;
        return PartitionAdmission::Denied;
    }
    let metadata_watermark = shard.plane.metadata().applied_frontier().get();
    if consumer_session
        .as_ref()
        .is_some_and(|(_, attachment)| !attachment.is_valid())
    {
        send_deny_reply(shard, origin, &header, IggyError::StaleClient.as_code()).await;
        return PartitionAdmission::Denied;
    }
    // A newer route can name group membership this replica has not applied yet.
    if header.minimum_metadata_op > metadata_watermark {
        send_deny_reply(
            shard,
            origin,
            &header,
            IggyError::TransientNotAccepted.as_code(),
        )
        .await;
        return PartitionAdmission::Denied;
    }
    let namespace = match resolved_namespace.map_or_else(
        || {
            resolve_partition_request_namespace(
                shard,
                header.operation,
                request_body(&request),
                consumer_session
                    .as_ref()
                    .map_or(vsr_client_id, |(parent, _)| *parent),
            )
        },
        Ok,
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
                minimum_metadata_op = header.minimum_metadata_op,
                metadata_watermark,
                error = %error,
                operation = ?header.operation,
                "partition request with unresolved namespace; replying denied"
            );
            send_deny_reply(shard, origin, &header, error.as_code()).await;
            return PartitionAdmission::Denied;
        }
    };
    // Dispatch-time RBAC. The partition plane is not replicated through the
    // metadata STM, so the in-apply gate cannot cover it; authorize here, on
    // the connection's own shard, before burning the routable wait or touching
    // the plane. The namespace resolved above, so its stream/topic are the
    // committed slab ids the permissioner keys on directly. A denial replies
    // the op's frame with an empty body and a nonzero `status` the SDK peeks.
    //
    // Authorization uses this node's committed metadata. Admission requires
    // quorum evidence younger than heartbeat_timeout + 3s, on a backup too,
    // because it ages the evidence from its previous probe. This limits stale
    // grants while keeping partition traffic independent of metadata consensus.
    let scope = IggyNamespace::from_raw(namespace);
    if let Some(status) = authorize_partition_op(
        shard,
        header.operation,
        acting_user_id,
        scope.stream_id(),
        scope.topic_id(),
    ) {
        warn!(
            transport_client_id,
            status,
            operation = ?header.operation,
            "partition request denied by authorization; replying with status"
        );
        send_deny_reply(shard, origin, &header, status).await;
        return PartitionAdmission::Denied;
    }
    let attachment = consumer_session
        .map(|(parent, session)| {
            capture_offset_attachment(
                shard.plane.metadata().mux_stm.streams(),
                scope,
                request_body(&request),
                parent,
                session,
                header.operation,
            )
        })
        .transpose();
    let attachment = match attachment {
        Ok(attachment) => attachment,
        Err(error) => {
            send_deny_reply(shard, origin, &header, error.as_code()).await;
            return PartitionAdmission::Denied;
        }
    };
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
            origin,
            &header,
            IggyError::TransientNotAccepted.as_code(),
        )
        .await;
        return PartitionAdmission::Denied;
    }
    // A group consumer-offset op carries the group NAME on the wire; the
    // partition plane keys the offset by the group's monotonic id (the same
    // key the poll path auto-commits under and the read path resolves), so
    // rewrite the consumer id before replication -- the apply layer has no
    // metadata access to resolve it.
    let request = match maybe_rewrite_consumer_offset_request(shard, request) {
        Ok(rewritten) => rewritten,
        // Metadata can change during the routing wait, so a previously valid
        // group may be missing now. Preserve the typed failure before submit.
        Err(error) => {
            warn!(
                transport_client_id,
                error = %error,
                operation = ?header.operation,
                "failed to rewrite consumer-offset request; replying denied"
            );
            send_deny_reply(shard, origin, &header, error.as_code()).await;
            return PartitionAdmission::Denied;
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
        new_header.metadata_watermark = metadata_watermark;
    });
    relay_partition_reply(
        shard,
        IggyNamespace::from_raw(namespace),
        request,
        origin,
        &header,
        attachment,
        in_flight,
    )
    .await
}

/// Submit a partition write whose reply the bus cannot route (the routed
/// header's `client` is the VSR consensus id, whose bits encode no home shard)
/// and relay the committed reply to the connection the request arrived on.
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
    origin: ClientOrigin,
    header: &RoutedRequestHeader,
    attachment: Option<ConsumerAttachment>,
    in_flight: Option<InFlight>,
) -> PartitionAdmission
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let consumer_kind = consumer_offset_kind(&request);
    let Ok(ticket) = shard.partition_submit_attached(namespace, request, attachment) else {
        // `PartitionSubmitRefused`: the frame never reached the owning shard,
        // so this is a known outcome and the client can be told now rather
        // than after its read-timeout. Same transient the plane itself answers
        // for a request it could not admit.
        send_deny_reply(
            shard,
            origin,
            header,
            IggyError::TransientNotAccepted.as_code(),
        )
        .await;
        return PartitionAdmission::Denied;
    };
    let header = *header;
    let shard = Rc::clone(shard);
    // Through the bus, not the runtime directly: the simulator supplies its
    // own executor and virtual clock.
    shard.bus.clone().spawn(async move {
        let _in_flight = in_flight;
        let Some(reply) = shard.await_partition_submit(ticket).await else {
            // Abandoned or expired: the outcome is unknown. A typed failure
            // could contradict a write that commits moments later, and silence
            // would cost the client its whole deadline. `TransientNotCommitted`
            // licenses only a replay of this request id, which returns the
            // receipt once the write commits. Releasing the in-flight guard
            // does not cancel that write or fence it against later progress.
            send_deny_reply(
                &shard,
                origin,
                &header,
                IggyError::TransientNotCommitted.as_code(),
            )
            .await;
            return;
        };
        if let Some(kind) = consumer_kind
            && reply
                .as_slice()
                .get(..size_of::<iggy_binary_protocol::ReplyHeader>())
                .and_then(|bytes| {
                    bytemuck::checked::try_from_bytes::<iggy_binary_protocol::ReplyHeader>(bytes)
                        .ok()
                })
                .is_some_and(|header| header.status == IggyError::TooManyConsumerOffsets.as_code())
        {
            shard.metrics().record_consumer_offset_denied(kind);
        }
        send_host_frame(
            &shard,
            origin,
            reply.into_frozen(),
            FrameChannel::Reply,
            "partition_reply",
        )
        .await;
    });
    PartitionAdmission::Admitted
}

/// The offset key shared by stored-offset reads and progress writes on a connection.
/// Failed resolution is left to the normal handler's authorization and error path.
pub(super) fn progress_key<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    message: &Message<GenericHeader>,
) -> Option<ConsumerProgress>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let request = message.try_as_typed::<RequestHeader>().ok()?;
    let wire = match request.header().operation {
        Operation::NonReplicated
            if request.header().reserved[..4] == GET_CONSUMER_OFFSET_CODE.to_le_bytes() =>
        {
            GetConsumerOffsetRequest::decode_from(request.body()).ok()?
        }
        Operation::NonReplicated
            if request.header().reserved[..4] == POLL_MESSAGES_CODE.to_le_bytes() =>
        {
            let (strategy, auto_commit) =
                PollMessagesRequest::peek_progress(request.body()).ok()?;
            if !auto_commit && strategy.kind != WirePollingStrategy::next().kind {
                return None;
            }
            let poll = PollMessagesRequest::decode_from(request.body()).ok()?;
            GetConsumerOffsetRequest {
                consumer: poll.consumer,
                stream_id: poll.stream_id,
                topic_id: poll.topic_id,
                partition_id: poll.partition_id,
            }
        }
        Operation::StoreConsumerOffset => {
            let store = StoreConsumerOffsetRequest::decode_from(request.body()).ok()?;
            GetConsumerOffsetRequest {
                consumer: store.consumer,
                stream_id: store.stream_id,
                topic_id: store.topic_id,
                partition_id: store.partition_id,
            }
        }
        Operation::DeleteConsumerOffset => {
            let delete = DeleteConsumerOffsetRequest::decode_from(request.body()).ok()?;
            GetConsumerOffsetRequest {
                consumer: delete.consumer,
                stream_id: delete.stream_id,
                topic_id: delete.topic_id,
                partition_id: delete.partition_id,
            }
        }
        _ => return None,
    };
    let (namespace, _, target) = resolve_consumer_offset_request(shard, &wire).ok()?;
    let (kind, consumer_id) = match target {
        ConsumerOffsetTarget::Consumer(PollingConsumer::Consumer(id, _)) => (KIND_CONSUMER, id),
        ConsumerOffsetTarget::Consumer(PollingConsumer::ConsumerGroup(id, _)) => {
            (KIND_CONSUMER_GROUP, id)
        }
        ConsumerOffsetTarget::ExternalGroup(group_id) => (KIND_EXTERNAL_GROUP, group_id as usize),
        ConsumerOffsetTarget::MissingGroup => return None,
    };
    Some((namespace.inner(), kind, consumer_id))
}

pub fn capture_offset_attachment(
    streams: &metadata::stm::stream::Streams,
    namespace: IggyNamespace,
    body: &[u8],
    parent: u128,
    session: SessionAttachment,
    operation: Operation,
) -> Result<ConsumerAttachment, IggyError> {
    if !session.is_valid() {
        return Err(IggyError::StaleClient);
    }
    let metadata = partition_write_metadata(streams, namespace, body, parent, operation)?;
    Ok(ConsumerAttachment { session, metadata })
}

/// The metadata a partition write of `parent` is fenced with: its partition's,
/// and for a consumer-group offset the generation `parent` holds it under.
pub fn partition_write_metadata(
    streams: &metadata::stm::stream::Streams,
    namespace: IggyNamespace,
    body: &[u8],
    parent: u128,
    operation: Operation,
) -> Result<PollMetadata, IggyError> {
    let wire = if operation == Operation::SendMessages {
        None
    } else {
        Some(
            GetConsumerOffsetRequest::decode(body)
                .map_err(|_| IggyError::InvalidCommand)?
                .0,
        )
    };
    let group = if let Some(wire) = wire.filter(|wire| wire.consumer.kind == KIND_CONSUMER_GROUP) {
        Some(crate::namespace::resolve_offset_group_id(
            streams,
            &wire.stream_id,
            &wire.topic_id,
            &wire.consumer.id,
        )?)
    } else {
        None
    };
    streams
        .consumer_offset_metadata(namespace, group, parent)
        .ok_or(IggyError::TransientNotAccepted)
}

fn consumer_offset_kind(request: &Message<RoutedRequestHeader>) -> Option<ConsumerKind> {
    if request.header().operation != Operation::StoreConsumerOffset {
        return None;
    }
    // WireConsumer starts with its kind byte. The dispatch path has already
    // decoded and validated the complete request.
    ConsumerKind::from_code(*request_body(request).first()?).ok()
}

/// Serve `poll_messages`: resolve the partition namespace, run the read on
/// the owning shard ([`shard::IggyShard::partition_read`]), and re-encode
/// the stored batches into the legacy wire `PolledMessages` body.
///
/// Owner rejections and missing replies return a nonzero status. A missing
/// reply leaves acceptance unknown: the owner may still advance progress, so
/// it cannot carry a retry hint that promises the poll was not accepted.
/// A consumer group poll fenced by the coordinator carries the resync sentinel.
/// Other unexpected owner replies and encoding failures retain the empty poll
/// fallback; these do not establish that the partition has no messages.
#[allow(clippy::future_not_send)]
pub(in crate::dispatch) async fn handle_poll_messages<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    origin: ClientOrigin,
    request: &Message<RoutedRequestHeader>,
    user_id: Option<u32>,
    consumer_client_id: u128,
    attachment: &SessionAttachment,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    if !shard
        .plane
        .metadata()
        .applied_frontier()
        .partition_requests_allowed(shard.bus.monotonic_now())
    {
        send_non_replicated_deny(
            shard,
            request,
            origin,
            IggyError::TransientNotAccepted.as_code(),
        )
        .await;
        return;
    }
    let transport_client_id = origin.id;
    let Ok(wire) = PollMessagesRequest::decode_from(request_body(request)) else {
        // A permanent client error, so it must not borrow the empty-poll
        // shape: that body decodes as a successful 0-message poll, and a
        // consumer looping on it never learns why. The server already sends
        // nonzero-status poll replies (authz, unresolved target).
        send_non_replicated_deny(shard, request, origin, IggyError::InvalidCommand.as_code()).await;
        return;
    };
    if !attachment.is_valid() {
        send_non_replicated_deny(shard, request, origin, IggyError::StaleClient.as_code()).await;
        return;
    }
    // Gate on (stream, topic) before touching the partition plane. A resolution
    // miss denies typed on the resolve path below; a denial here replies
    // status!=0 with an empty body, distinct from the empty-poll "0 messages"
    // shape.
    if let Some(status) = authorize_partition_read(
        shard,
        &wire.stream_id,
        &wire.topic_id,
        user_id,
        |permissioner, uid, stream_id, topic_id| {
            permissioner.poll_messages(uid, stream_id, topic_id)
        },
    ) {
        send_non_replicated_deny(shard, request, origin, status).await;
        return;
    }
    // A context from a node ahead of this one can name a member, an owner or
    // a partition this node has not applied yet. Refuse it as not accepted
    // before the group fence or the resolve can call it gone.
    if request.header().minimum_metadata_op > current_metadata_commit(shard) {
        let status = IggyError::TransientNotAccepted.as_code();
        send_non_replicated_deny(shard, request, origin, status).await;
        return;
    }
    let (body, channel) = match resolve_poll_request(shard, &wire, consumer_client_id) {
        Ok(resolved) => {
            match read_polled_messages(
                shard,
                origin,
                request,
                resolved,
                attachment,
                consumer_client_id,
            )
            .await
            {
                Ok(reply) => {
                    send_host_frame(shard, origin, reply, FrameChannel::Reply, "poll_messages")
                        .await;
                    return;
                }
                Err(ReadPolledMessagesError::Fallback(fallback)) => fallback,
                Err(ReadPolledMessagesError::Rejected(error)) => {
                    send_non_replicated_deny(shard, request, origin, error.as_code()).await;
                    return;
                }
            }
        }
        // A generation fence: the client's cached assignment went stale after a
        // rebalance. The empty poll carries the re-sync sentinel so the SDK
        // re-syncs and retries instead of reading end-of-partition.
        Err(error @ IggyError::ConsumerGroupPartitionNotOwned(..)) => {
            warn!(
                transport_client_id,
                error = %error,
                "poll_messages fenced; replying re-sync sentinel"
            );
            empty_poll_fallback(RESYNC_REQUIRED_PARTITION_SENTINEL)
        }
        // Everything else the resolve rejects is a permanent client error: an
        // unresolved stream, topic, or partition, a polling strategy or
        // consumer kind outside the wire enum, or a group poll that omitted
        // its partition id. All must surface typed -- the empty poll is a
        // successful 0-message read, and a consumer looping on it never learns
        // why.
        Err(error) => {
            warn!(
                transport_client_id,
                error = %error,
                "poll_messages rejected; replying denied"
            );
            send_non_replicated_deny(shard, request, origin, error.as_code()).await;
            return;
        }
    };
    send_non_replicated_bytes(shard, request, origin, body, channel, "poll_messages").await;
}

/// Run the resolved poll on the owning shard and re-encode the stored
/// batches into the wire `PolledMessages` reply. Owner rejections preserve
/// their error; a missing reply reports a communication error without claiming
/// that the owner rejected the read. Unexpected replies and encoding failures
/// retain the empty poll fallback.
#[allow(clippy::future_not_send)]
async fn read_polled_messages<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    origin: ClientOrigin,
    request: &Message<RoutedRequestHeader>,
    (namespace, partition_id, consumer, args): DecodedPollRequest,
    attachment: &SessionAttachment,
    consumer_client_id: u128,
) -> Result<BusMessage, ReadPolledMessagesError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let transport_client_id = origin.id;
    let group_id = match consumer {
        PollingConsumer::ConsumerGroup(group, _) => Some(group as u64),
        PollingConsumer::Consumer(..) => None,
    };
    let metadata = shard
        .plane
        .metadata()
        .mux_stm
        .streams()
        .poll_metadata(namespace, group_id, consumer_client_id)
        .ok_or(ReadPolledMessagesError::Rejected(
            IggyError::TransientNotAccepted,
        ))?;
    let header = request.header();
    let local = metadata.context(current_metadata_commit(shard));
    // A group poll with owner 0 captured no owner. The primary then serves
    // only the owner it installed for the caller, and the reply names it.
    let owner_generation = if group_id.is_some() && header.owner_generation == 0 {
        local.owner_generation
    } else {
        header.owner_generation
    };
    fence_poll_context(
        PartitionContext {
            incarnation: header.partition_incarnation,
            owner_generation,
            metadata_op: header.minimum_metadata_op,
        },
        local,
        consumer_client_id,
        partition_id,
    )
    .map_err(ReadPolledMessagesError::Rejected)?;
    let read = PartitionRead::PollOnPrimary {
        consumer,
        args,
        attachment: ConsumerAttachment {
            session: attachment.clone(),
            metadata,
        },
    };
    let reply = shard.partition_read(namespace, read).await;
    // Auto-commit is at-most-once: denial here can discard an already committed
    // batch. Delivery must still stop when the session or grant was revoked, or
    // the partition was deleted.
    authorize_served_partition_read(
        shard,
        Some(attachment),
        namespace,
        header.partition_incarnation,
        Permissioner::poll_messages,
    )
    .map_err(ReadPolledMessagesError::Rejected)?;
    match reply {
        Some(PartitionReadReply::Poll {
            context,
            fragments,
            current_offset,
        }) => build_polled_messages_reply(
            request.header(),
            current_metadata_commit(shard),
            partition_id,
            current_offset,
            context,
            fragments,
            shard.plane.partitions().config().encryptor.as_deref(),
        )
        .map_err(|error| {
            warn!(
                transport_client_id,
                error = %error,
                "failed to re-encode polled batches; replying empty poll"
            );
            ReadPolledMessagesError::Fallback(empty_poll_fallback(partition_id))
        }),
        Some(PartitionReadReply::Rejected(error)) => Err(ReadPolledMessagesError::Rejected(error)),
        None => {
            // The owner rejects a disconnected receiver before poll admission,
            // but timeout can race with that check. Missing replies therefore
            // do not prove rejection and must not be reported as empty success.
            warn!(
                transport_client_id,
                namespace = namespace.inner(),
                "partition read reply unavailable; acceptance unknown"
            );
            Err(ReadPolledMessagesError::Rejected(
                IggyError::ShardCommunicationError,
            ))
        }
        Some(other) => {
            warn!(
                transport_client_id,
                namespace = namespace.inner(),
                reply = ?other,
                "unexpected partition poll reply; replying empty poll"
            );
            Err(ReadPolledMessagesError::Fallback(empty_poll_fallback(
                partition_id,
            )))
        }
    }
}

/// The poll gate shared by TCP and HTTP. A caller context from a node ahead of
/// this one can name an incarnation or owner this node has not applied yet, so
/// a mismatch is final only once this node has applied the caller's metadata.
/// Until then the poll is not accepted, which keeps the consumer's position,
/// where `HistoryUnavailable` would drop it.
pub const fn fence_poll_context(
    requested: PartitionContext,
    local: PartitionContext,
    client_id: u128,
    partition_id: u32,
) -> Result<(), IggyError> {
    if requested.metadata_op > local.metadata_op {
        return Err(IggyError::TransientNotAccepted);
    }
    if requested.incarnation != local.incarnation {
        return Err(IggyError::HistoryUnavailable);
    }
    if requested.owner_generation != local.owner_generation {
        return Err(IggyError::ConsumerGroupPartitionNotOwned(
            client_id,
            partition_id,
        ));
    }
    Ok(())
}

enum ReadPolledMessagesError {
    Fallback((Bytes, FrameChannel)),
    Rejected(IggyError),
}

/// The fail-fast poll reply for a partition that could not answer: the
/// 40-byte empty poll ([`PollMessagesResponseHeader`] with a zero context)
/// for `partition_id`, riding the re-sync sentinel channel when the id is the
/// sentinel and the empty-frame channel otherwise.
pub(super) fn empty_poll_fallback(partition_id: u32) -> (Bytes, FrameChannel) {
    let channel = if partition_id == RESYNC_REQUIRED_PARTITION_SENTINEL {
        FrameChannel::ResyncSentinel
    } else {
        FrameChannel::EmptyFrame
    };
    (empty_polled_messages_body(partition_id), channel)
}

/// Serve `get_consumer_offset`. An empty status-0 body decodes as `None` on
/// the SDK side, so it is reserved for "this consumer has no stored offset" --
/// an unresolved consumer group included, since a deleted group has no offset
/// to report. A malformed request or an unresolved stream, topic, or partition
/// denies with a nonzero status instead, so an addressing typo cannot read
/// back as a fresh consumer. A read that its owner does not answer in time, or
/// whose owner does not hold the partition, denies with `TransientNotAccepted`.
/// Primary reads also return `NotPrimary` (89) for a moved primary,
/// `HistoryUnavailable` (87) for a missing incarnation, or
/// `TransientNotAccepted` (58) when the context is ahead of local metadata.
/// The gate runs again after the owner answers, on the namespace the read
/// resolved ([`authorize_served_partition_read`]).
#[allow(clippy::future_not_send, clippy::too_many_lines)]
pub(in crate::dispatch) async fn handle_get_consumer_offset<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    origin: ClientOrigin,
    request: &Message<RoutedRequestHeader>,
    user_id: Option<u32>,
    attachment: Option<&SessionAttachment>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    if !shard
        .plane
        .metadata()
        .applied_frontier()
        .partition_requests_allowed(shard.bus.monotonic_now())
    {
        send_non_replicated_deny(
            shard,
            request,
            origin,
            IggyError::TransientNotAccepted.as_code(),
        )
        .await;
        return;
    }
    let transport_client_id = origin.id;
    let Ok(wire) = GetConsumerOffsetRequest::decode_from(request_body(request)) else {
        // Same rule as the poll above: an empty body is the legitimate
        // "no offset stored" answer, so a malformed request must not send
        // one. Byte-identical frames on the same channel with the same
        // context would also be indistinguishable in the send-failure log.
        send_non_replicated_deny(shard, request, origin, IggyError::InvalidCommand.as_code()).await;
        return;
    };
    if let Err(error) = authorize_attached(attachment, || {
        authorize_partition_read(
            shard,
            &wire.stream_id,
            &wire.topic_id,
            user_id,
            |permissioner, uid, stream_id, topic_id| {
                permissioner.get_consumer_offset(uid, stream_id, topic_id)
            },
        )
        .map_or(Ok(()), |status| Err(IggyError::from_code(status)))
    }) {
        send_non_replicated_deny(shard, request, origin, error.as_code()).await;
        return;
    }
    let context = PartitionContext {
        incarnation: request.header().partition_incarnation,
        owner_generation: 0,
        metadata_op: request.header().minimum_metadata_op,
    };
    let body = match resolve_primary_consumer_offset_request(shard, &wire, context) {
        Ok((namespace, partition_id, read)) => {
            let reply = shard.partition_read(namespace, read).await;
            if let Err(error) = authorize_served_partition_read(
                shard,
                attachment,
                namespace,
                context.incarnation,
                Permissioner::get_consumer_offset,
            ) {
                send_non_replicated_deny(shard, request, origin, error.as_code()).await;
                return;
            }
            match reply {
                Some(PartitionReadReply::ConsumerOffset {
                    stored: Some(stored_offset),
                    current_offset,
                }) => build_consumer_offset_body(partition_id, current_offset, stored_offset),
                Some(PartitionReadReply::ConsumerOffset { stored: None, .. }) => Bytes::new(),
                Some(PartitionReadReply::Rejected(error)) => {
                    send_non_replicated_deny(shard, request, origin, error.as_code()).await;
                    return;
                }
                // No reply, or an owner that does not hold the partition yet,
                // says nothing about the offset, so it must not read as "no
                // stored offset". A read moves no progress, so the client may
                // retry it. The SDK retries every 50 ms, so the not-yet-held
                // case logs at debug.
                reply => {
                    if matches!(reply, Some(PartitionReadReply::NotFound)) {
                        debug!(
                            transport_client_id,
                            namespace = namespace.inner(),
                            "consumer offset partition not held by owner; replying not accepted"
                        );
                    } else {
                        warn!(
                            transport_client_id,
                            namespace = namespace.inner(),
                            ?reply,
                            "consumer offset read unanswered; replying not accepted"
                        );
                    }
                    send_non_replicated_deny(
                        shard,
                        request,
                        origin,
                        IggyError::TransientNotAccepted.as_code(),
                    )
                    .await;
                    return;
                }
            }
        }
        // Everything the resolve rejects is a client error: an unresolved
        // stream, topic, or partition, or `resolve_partition_namespace`'s
        // generic rejection. All must surface typed -- an empty body decodes as
        // `None`, indistinguishable from "no stored offset yet", so a typo'd or
        // deleted target would read back as a fresh consumer and resume from
        // its configured default with no error anywhere. The same read over
        // REST 404s.
        Err(error) => {
            warn!(
                transport_client_id,
                error = %error,
                "get_consumer_offset rejected; replying denied"
            );
            send_non_replicated_deny(shard, request, origin, error.as_code()).await;
            return;
        }
    };
    send_non_replicated_bytes(
        shard,
        request,
        origin,
        body,
        FrameChannel::Reply,
        "get_consumer_offset",
    )
    .await;
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

/// A `PolledMessages` body with zero messages and a zero context: only the
/// 40-byte [`PollMessagesResponseHeader`]. The SDK decoder requires at least
/// this header, so failure paths must never reply a zero-byte body.
fn empty_polled_messages_body(partition_id: u32) -> Bytes {
    PollMessagesResponseHeader {
        partition_id,
        current_offset: 0,
        messages_count: 0,
        context: iggy_binary_protocol::primitives::partition_history::PartitionContext::default(),
    }
    .to_bytes()
}

type DecodedPollRequest = (IggyNamespace, u32, PollingConsumer, PollingArgs);

/// Resolve a decoded poll request into its owning-shard read: namespace,
/// partition, polling consumer, and args. Shared by the TCP dispatch (client
/// id = the connection's bound VSR client) and the HTTP route (client id 0,
/// which fences group polls closed).
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
    // An external group only holds offsets. Nothing in Iggy may poll with it.
    if wire.consumer.kind == KIND_EXTERNAL_GROUP {
        return Err(IggyError::FeatureUnavailable);
    }
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
                client_id,
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

/// A stored offset has no membership fence, but it still belongs to one
/// partition history. The captured metadata is checked again on the owner.
pub fn resolve_primary_consumer_offset_request<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    wire: &GetConsumerOffsetRequest,
    context: PartitionContext,
) -> Result<(IggyNamespace, u32, PartitionRead), IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let metadata_op = current_metadata_commit(shard);
    if context.metadata_op > metadata_op {
        return Err(IggyError::TransientNotAccepted);
    }
    let (namespace, partition_id, target) = resolve_consumer_offset_request(shard, wire)?;
    let metadata = shard
        .plane
        .metadata()
        .mux_stm
        .streams()
        .consumer_offset_metadata(namespace, None, 0)
        .ok_or(IggyError::TransientNotAccepted)?;
    let mut local = metadata.context(metadata_op);
    local.owner_generation = 0;
    fence_poll_context(
        PartitionContext {
            owner_generation: 0,
            ..context
        },
        local,
        0,
        partition_id,
    )?;
    let read = match target {
        ConsumerOffsetTarget::Consumer(consumer) => PartitionRead::ConsumerOffsetOnPrimary {
            consumer: Some(consumer),
            metadata,
        },
        ConsumerOffsetTarget::ExternalGroup(group_id) => {
            PartitionRead::ExternalGroupOffsetOnPrimary { group_id, metadata }
        }
        ConsumerOffsetTarget::MissingGroup => PartitionRead::ConsumerOffsetOnPrimary {
            consumer: None,
            metadata,
        },
    };
    Ok((namespace, partition_id, read))
}

/// Whose stored offset a consumer-offset read returns.
pub enum ConsumerOffsetTarget {
    Consumer(PollingConsumer),
    /// An external group's monotonic id. Not a `PollingConsumer`: the kind is
    /// never polled.
    ExternalGroup(u32),
    /// The named group does not exist, so no offset exists to report.
    MissingGroup,
}

/// Resolve a decoded consumer-offset read into its namespace, partition, and
/// target. Shared by the TCP dispatch and the HTTP route; needs no client id
/// because offset reads are not fenced (any client may read a group's offset,
/// member or not).
///
/// [`ConsumerOffsetTarget::MissingGroup`] is the one outcome that means "no
/// offset exists to report". It is a target rather than an error code because
/// [`resolve_partition_namespace`] also rejects an addressing miss with
/// `InvalidIdentifier`, and the caller must deny that one typed.
pub fn resolve_consumer_offset_request<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    wire: &GetConsumerOffsetRequest,
) -> Result<(IggyNamespace, u32, ConsumerOffsetTarget), IggyError>
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
    if wire.consumer.kind != KIND_CONSUMER_GROUP && wire.consumer.kind != KIND_EXTERNAL_GROUP {
        let consumer = polling_consumer_from_wire(&wire.consumer, partition_id)?;
        return Ok((
            namespace,
            partition_id,
            ConsumerOffsetTarget::Consumer(consumer),
        ));
    }
    // A group offset is keyed by the group's monotonic id (any client may read
    // it, member or not), the same key the write path is rewritten to. An
    // unresolved group (e.g. deleted) has no offset to report.
    let Some(group_id) = shard
        .plane
        .metadata()
        .mux_stm
        .streams()
        .resolve_consumer_group_id(&wire.stream_id, &wire.topic_id, &wire.consumer.id)
    else {
        return Ok((namespace, partition_id, ConsumerOffsetTarget::MissingGroup));
    };
    #[allow(clippy::cast_possible_truncation)]
    let target = if wire.consumer.kind == KIND_EXTERNAL_GROUP {
        ConsumerOffsetTarget::ExternalGroup(u32::try_from(group_id).unwrap_or(u32::MAX))
    } else {
        ConsumerOffsetTarget::Consumer(PollingConsumer::ConsumerGroup(
            group_id as usize,
            partition_id as usize,
        ))
    };
    Ok((namespace, partition_id, target))
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
        KIND_CONSUMER => Ok(PollingConsumer::Consumer(
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
/// no-op `TruncatePartition(0)` and acks, while a not-primary refusal reaches
/// the client result-framed (`NotPrimary` from a normal backup,
/// `TransientNotAccepted` mid view change) so the SDK replays instead of
/// mistaking a dropped delete for success. Nothing is acked empty: a
/// malformed body denies typed, and an unresolvable namespace commits a typed
/// rejection against the client's raw identifiers so its retry dedups.
#[allow(clippy::future_not_send)]
pub(in crate::dispatch) async fn handle_delete_segments_request<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    origin: ClientOrigin,
    bound: Option<(u128, u64)>,
    request: &Message<RoutedRequestHeader>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let transport_client_id = origin.id;
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
        Ok(truncate) => truncate,
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
                &TruncatePartitionRequest {
                    stream_id: WireIdentifier::numeric(0),
                    topic_id: WireIdentifier::numeric(0),
                    partition_id: 0,
                    up_to_offset: 0,
                    expected_history: None,
                },
            );
            send_result_rejection(
                shard,
                origin,
                template.header(),
                &IggyError::TransientNotAccepted,
                "delete_segments_transient_rejection",
            )
            .await;
            return;
        }
        // Undecodable body (never produced by the SDK): deny typed rather
        // than ack empty. Both keep the lockstep stream framed, but a
        // status-0 ack reads as a completed trim. Unresolvable-but-well-formed
        // targets commit a typed rejection instead (see the resolve).
        Err(error) => {
            send_deny_reply(shard, origin, &header, error.as_code()).await;
            return;
        }
    };

    // Forward the consensus reply verbatim, exactly like the generic metadata
    // path: a committed success acks the delete, and a result-framed transient
    // refusal makes the SDK replay the request.
    // Acking unconditionally here would swallow a not-primary rejection and
    // drop the delete on the floor while the client believes it succeeded.
    let Some(reply) = submit_client_request_on_owner(shard, truncate).await else {
        // No verdict came back, so the outcome is unknown. The transient deny
        // makes the SDK replay the same request id, which re-resolves and
        // commits. An ack here advances the client past an unrecorded request
        // and gaps the next metadata op.
        warn!(
            transport_client_id,
            "delete_segments: no metadata verdict; client replays"
        );
        send_deny_reply(
            shard,
            origin,
            &header,
            IggyError::TransientNotCommitted.as_code(),
        )
        .await;
        return;
    };
    send_host_frame(
        shard,
        origin,
        reply.into_frozen(),
        FrameChannel::Reply,
        "delete_segments_reply",
    )
    .await;
}

/// Resolve a client `DeleteSegments` to the `TruncatePartition` that commits the
/// trim. Shared by the TCP dispatch and the HTTP listener so both resolve the
/// requested segment count to a concrete watermark identically.
///
/// `template` supplies the wire `cluster` / `view` / `release` and the client's
/// `request` number; `client_id` / `session` are the bound VSR identity the
/// truncate commits under. A resolvable namespace with nothing sealed to delete
/// still yields a `TruncatePartition(up_to_offset = 0)` so the metadata request
/// sequence stays contiguous. Both deletion and no-op resolution require a
/// normal partition primary that applied the view it started, and retain their
/// history guard. A no-op also waits for the commits that primary has not
/// executed yet, since they can seal a segment.
/// An UNRESOLVABLE namespace yields the truncate
/// against the client's raw identifiers (the apply rejects it as a committed
/// result). `Err` is only `InvalidCommand` for a malformed body and
/// `TransientNotAccepted` for a resolution that must wait or a changed committed
/// history: both transports surface that transient rejection.
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
    // A delete and re-create after offset resolution must keep its committed
    // history-change result.
    let committed_history = shard
        .plane
        .metadata()
        .mux_stm
        .streams()
        .consumer_offset_metadata(namespace, None, client_id)
        .ok_or(IggyError::TransientNotAccepted)?;
    let context = committed_history.context(current_metadata_commit(shard));
    let up_to_offset = super::segment_delete::resolve(
        shard,
        namespace,
        parsed.segments_count,
        context.incarnation,
        context.metadata_op,
    )
    .await?;
    let expected_history = Some(context.incarnation);
    Ok(build_truncate_partition_client_message(
        template,
        client_id,
        session,
        &TruncatePartitionRequest {
            stream_id: WireIdentifier::numeric(namespace.stream_id() as u32),
            topic_id: WireIdentifier::numeric(namespace.topic_id() as u32),
            partition_id: namespace.partition_id() as u32,
            up_to_offset,
            expected_history,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::test_support::{
        SpyBus, TestMux, TestShard, create_consumer_group, create_one_partition,
        create_reader_request, prepare_message, register_reply, request_message, test_shard,
        test_shard_with_frontier,
    };
    use crate::dispatch::tests::non_replicated_request;
    use crate::dispatch::{
        ActiveClientRequests, Admission, ClientDrain, ClientRequestQueues, enqueue_client_request,
    };
    use crate::reply_frame::build_deny_reply;
    use crate::session_manager::SessionManager;
    use crate::shell::ShellShardHandle;
    use bytes::BytesMut;
    use configs::server::ServerConfig;
    #[cfg(target_os = "linux")]
    use consensus::Sequencer;
    use iggy_binary_protocol::codes::PING_CODE;
    use iggy_binary_protocol::primitives::partition_assignment::CreatedPartitionAssignment;
    use iggy_binary_protocol::requests::consumer_groups::CreateConsumerGroupRequest;
    use iggy_binary_protocol::requests::consumer_offsets::DeleteConsumerOffsetRequest;
    use iggy_binary_protocol::requests::consumer_offsets::StoreConsumerOffsetRequest;
    use iggy_binary_protocol::requests::messages::{
        RawMessage, SendMessagesEncoder, SendMessagesHeader,
    };
    use iggy_binary_protocol::requests::streams::CreateStreamRequest;
    use iggy_binary_protocol::requests::topics::{
        CreateTopicRequest, CreateTopicWithAssignmentsRequest, DeleteTopicRequest,
    };
    use iggy_binary_protocol::{
        Command, ConsensusHeader, GenericHeader, PrepareOkHeader, ReplyHeader, SegmentDeleteHeader,
        SegmentDeleteKind, WireEncode, WireIdentifier, WireName, WireOptions, WirePartitioning,
    };
    use iggy_common::Identifier;
    use iggy_common::defaults::DEFAULT_ROOT_USER_ID;
    use journal::prepare_journal::PrepareJournal;
    use message_bus::{BusReceiver, IggyMessageBus, Shutdown, ShutdownToken};
    use metadata::IggyMetadata;
    use metadata::impls::metadata::IggySnapshot;
    use metadata::stm::StateMachine as _;
    use metadata::stm::consumer_group::{
        CompleteConsumerGroupRevocationRequest, JoinConsumerGroupRequest,
    };
    use metadata::stm::lifecycle::{CompleteLifecycleRequest, LifecycleCompletion};
    use partitions::{IggyPartitions, PartitionPathLayout, PartitionsConfig};
    use server_common::MessageBag;
    use server_common::sharding::{PartitionLocation, ShardId};
    use shard::metrics::{ShardMetrics, frame_drop_reason, frame_drop_variant};
    use shard::shards_table::PapayaShardsTable;
    use shard::{
        IggyShard, LifecycleFrame, NoopHost, PartitionConsensusConfig, Receiver, ReconcileOp,
        ReplicaTopology, ShardFrame, ShardIdentity, shard_channel,
    };
    use std::cell::RefCell;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    #[compio::test]
    async fn given_unapplied_history_when_resolving_segment_deletes_should_retry() {
        const CLIENT: u128 = 1;
        let bus = SpyBus::default();
        let mut shard = test_shard(&bus, 0, 1, 1);
        let (namespace, created_revision) = create_segment_delete_topic(&shard);
        let (sender, owner_inbox, _owner_replies) = shard_channel(0, 1, 1);
        shard.attach_senders(vec![sender]);
        let shard = Rc::new(shard);
        let body = DeleteSegmentsRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            partition_id: 0,
            segments_count: 1,
        }
        .to_bytes();
        let request = request_message(Operation::DeleteSegments, CLIENT, 1, 1, &body);
        let stale_revision = created_revision.wrapping_sub(1);
        for (up_to_offset, revision, lagging) in [
            (Some(7), stale_revision, false),
            (None, stale_revision, false),
            (Some(7), created_revision, true),
            (None, created_revision, true),
        ] {
            let owner = async {
                answer_segment_delete_primary(&owner_inbox, 0).await;
                let ShardFrame::Lifecycle(LifecycleFrame::PartitionRead {
                    namespace: resolved,
                    read: PartitionRead::ResolveSegmentDeleteOffset { count: 1 },
                    reply,
                }) = owner_inbox.recv().await.unwrap()
                else {
                    panic!("segment deletion must read its owner");
                };
                assert_eq!(resolved, namespace);
                reply
                    .try_send(PartitionReadReply::SegmentDeleteOffset {
                        up_to_offset,
                        lagging,
                        created_revision: revision,
                    })
                    .unwrap();
            };
            let (result, ()) = futures::join!(
                resolve_delete_segments_truncate(&shard, request.header(), CLIENT, 1, &body),
                owner,
            );
            assert!(
                matches!(result, Err(IggyError::TransientNotAccepted)),
                "offset {up_to_offset:?} from revision {stale_revision} must retry before submission"
            );
        }
    }

    #[compio::test]
    async fn given_split_primaries_when_deleting_segments_should_resolve_on_partition_primary() {
        let bus = SpyBus::default();
        let mut shard = test_shard(&bus, 0, 3, 1);
        let (namespace, created_revision) = create_segment_delete_topic(&shard);
        let (sender, inbox, _owner_replies) = shard_channel(0, 1, 1);
        shard.attach_senders(vec![sender]);
        let shard = Rc::new(shard);
        let body = DeleteSegmentsRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            partition_id: 0,
            segments_count: 2,
        }
        .to_bytes();
        let request = request_message(Operation::DeleteSegments, 1, 1, 1, &body);
        let peer = async {
            answer_segment_delete_primary(&inbox, 1).await;
            while bus.replica_sends.borrow().is_empty() {
                compio::time::sleep(Duration::from_millis(1)).await;
            }
            let (target, query) = bus.sole_replica_send::<SegmentDeleteHeader>();
            assert_eq!(target, 1);
            assert_eq!(query.namespace, namespace.inner());
            assert_eq!(query.created_revision, created_revision);
            assert_eq!(query.segments_count, 2);
            query.validate().unwrap();
            query.verify_frame().unwrap();
            for revision in [created_revision + 1, created_revision] {
                let reply = Message::<SegmentDeleteHeader>::new(size_of::<SegmentDeleteHeader>())
                    .transmute_header(|_, header| {
                        *header = SegmentDeleteHeader {
                            replica: 1,
                            kind: SegmentDeleteKind::Resolved,
                            created_revision: revision,
                            up_to_offset: 17,
                            ..query
                        };
                        header.seal();
                    });
                let reply = MessageBag::try_from(reply.into_generic()).unwrap();
                assert_eq!(reply.routing().1, server_common::sharding::METADATA_GROUP);
                shard.on_message(reply).await;
            }
        };
        let (resolved, ()) = futures::join!(
            resolve_delete_segments_truncate(&shard, request.header(), 1, 1, &body),
            peer,
        );
        let resolved = resolved.unwrap();
        let truncate = TruncatePartitionRequest::decode_from(request_body(&resolved)).unwrap();
        assert_eq!(truncate.up_to_offset, 17);
        assert_eq!(truncate.expected_history, Some(created_revision));
    }

    /// A segment delete resolves its offset on the owning shard before it
    /// replies. A connection that took the id during that round trip must
    /// not get the reply.
    #[compio::test]
    async fn given_a_segment_delete_resolving_on_its_owner_when_the_connection_is_replaced_should_not_answer_the_new_one()
     {
        const CLIENT: u128 = 1;
        const TRANSPORT: u128 = 2;
        const SESSION: u64 = 1;
        let bus = SpyBus::default();
        let mut shard = test_shard(&bus, 0, 1, 1);
        let (namespace, _) = create_segment_delete_topic(&shard);
        let (sender, owner_inbox, _owner_replies) = shard_channel(0, 1, 1);
        shard.attach_senders(vec![sender]);
        let shard = Rc::new(shard);
        let body = DeleteSegmentsRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            partition_id: 0,
            segments_count: 1,
        }
        .to_bytes();
        let request = request_message(Operation::DeleteSegments, CLIENT, SESSION, 1, &body);
        let created_revision = shard
            .plane
            .metadata()
            .mux_stm
            .streams()
            .created_revision_for_namespace(namespace)
            .unwrap();
        let owner = async {
            answer_segment_delete_primary(&owner_inbox, 0).await;
            let ShardFrame::Lifecycle(LifecycleFrame::PartitionRead { reply, .. }) =
                owner_inbox.recv().await.unwrap()
            else {
                panic!("segment deletion must read its owner");
            };
            bus.reconnect_client(TRANSPORT);
            // A stale revision makes the delete answer a retryable refusal.
            reply
                .try_send(PartitionReadReply::SegmentDeleteOffset {
                    up_to_offset: None,
                    lagging: false,
                    created_revision: created_revision.wrapping_sub(1),
                })
                .unwrap();
        };
        futures::join!(
            handle_delete_segments_request(
                &shard,
                ClientOrigin::capture(&shard.bus, TRANSPORT),
                Some((CLIENT, SESSION)),
                &request
            ),
            owner,
        );

        assert!(
            bus.client_replies
                .borrow()
                .iter()
                .all(|(target, _)| *target != TRANSPORT),
            "a reply for a replaced connection must not reach the connection that took its id"
        );
    }

    #[compio::test]
    async fn given_recreate_after_segment_resolution_when_committing_should_report_history_changed()
    {
        const CLIENT: u128 = 1;
        const REQUEST_CHECKSUM: u128 = (1 << 80) | 0x002A;
        let bus = SpyBus::default();
        let mut shard = test_shard(&bus, 0, 1, 1);
        let (namespace, _) = create_segment_delete_topic(&shard);
        let (sender, owner_inbox, _owner_replies) = shard_channel(0, 1, 1);
        shard.attach_senders(vec![sender]);
        let shard = Rc::new(shard);
        let body = DeleteSegmentsRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            partition_id: 0,
            segments_count: 1,
        }
        .to_bytes();
        let request = request_message(Operation::DeleteSegments, CLIENT, 1, 1, &body);
        let header = RoutedRequestHeader {
            request_checksum: REQUEST_CHECKSUM,
            ..*request.header()
        };
        for up_to_offset in [Some(7), None] {
            let created_revision = shard
                .plane
                .metadata()
                .mux_stm
                .streams()
                .created_revision_for_namespace(namespace)
                .unwrap();
            let owner = async {
                answer_segment_delete_primary(&owner_inbox, 0).await;
                let ShardFrame::Lifecycle(LifecycleFrame::PartitionRead { reply, .. }) =
                    owner_inbox.recv().await.unwrap()
                else {
                    panic!("segment deletion must read its owner");
                };
                reply
                    .try_send(PartitionReadReply::SegmentDeleteOffset {
                        up_to_offset,
                        lagging: false,
                        created_revision,
                    })
                    .unwrap();
                let delete = prepare_message(
                    Operation::DeleteTopic,
                    CLIENT,
                    1,
                    &DeleteTopicRequest {
                        stream_id: WireIdentifier::numeric(0),
                        topic_id: WireIdentifier::numeric(0),
                    }
                    .to_bytes(),
                );
                assert_eq!(
                    shard.plane.metadata().mux_stm.update(delete).unwrap().code,
                    0
                );
                complete_segment_delete_lifecycle(&shard);
                assert_eq!(
                    shard
                        .plane
                        .metadata()
                        .mux_stm
                        .update(create_topic_prepare())
                        .unwrap()
                        .code,
                    0
                );
            };
            let (resolved, ()) = futures::join!(
                resolve_delete_segments_truncate(&shard, &header, CLIENT, 1, &body),
                owner,
            );
            let resolved = resolved.unwrap();
            assert_eq!(resolved.header().request_checksum, REQUEST_CHECKSUM);
            let truncate = TruncatePartitionRequest::decode(request_body(&resolved))
                .unwrap()
                .0;
            assert_eq!(truncate.up_to_offset, up_to_offset.unwrap_or(0));
            assert_eq!(truncate.expected_history, Some(created_revision));
            let reply = shard
                .plane
                .metadata()
                .mux_stm
                .update(prepare_message(
                    Operation::TruncatePartition,
                    CLIENT,
                    1,
                    &truncate.to_bytes(),
                ))
                .unwrap();
            assert_eq!(reply.code, IggyError::PartitionHistoryChanged.as_code());
            assert_eq!(
                shard
                    .plane
                    .metadata()
                    .mux_stm
                    .streams()
                    .partition_delete_watermark(0, 0, 0),
                0
            );
        }
    }

    #[cfg(target_os = "linux")]
    #[compio::test]
    async fn checkpoint_index_failure_is_returned_by_the_same_partition_tick() {
        let root = tempfile::tempdir().unwrap();
        let bus = SpyBus::default();
        let shard = test_shard(&bus, 0, 3, 1);
        let namespace = IggyNamespace::new(1, 1, 0);
        let consensus = consensus::VsrConsensus::new(
            1,
            0,
            3,
            namespace.inner(),
            bus,
            consensus::LocalPipeline::new(),
        );
        consensus.init();
        let mut partition = partitions::IggyPartition::with_in_memory_storage(
            std::sync::Arc::new(iggy_common::PartitionStats::default()),
            consensus,
            shard.plane.partitions().config().segment_size,
        );
        partition.set_runtime_options(iggy_common::TopicRuntimeOptions {
            durability: iggy_common::Durability::Persisted,
            preallocate_segments: Some(false),
            ..Default::default()
        });
        partition.set_partition_dir(root.path().to_string_lossy().into_owned());
        let capacity = journal::partition_journal::PARTITION_WAL_BYTES_MAX;
        let (persistence, prepares) = partitions::PartitionPersistence::open_with_capacity(
            &root.path().join("prepares-0"),
            namespace.inner(),
            0,
            partitions::WalStorage::default(),
            capacity,
            false,
        )
        .await
        .unwrap();
        partition
            .open_persistence_with_recovered(
                capacity,
                std::time::Duration::ZERO,
                Some((Rc::clone(&persistence), prepares)),
            )
            .await
            .unwrap();

        let mut messages = server_common::send_messages::IggyMessages::with_capacity(1);
        messages.push(server_common::send_messages::IggyMessage {
            header: server_common::send_messages::IggyMessageHeader::default(),
            payload: Bytes::from_static(b"checkpoint"),
            user_headers: None,
        });
        let batch =
            server_common::send_messages::SendMessagesOwned::from_messages(namespace, &messages)
                .unwrap();
        let mut body = vec![0; batch.header.total_size()];
        batch.header.encode_into(&mut body);
        body[iggy_binary_protocol::batch::BATCH_HEADER_SIZE..].copy_from_slice(&batch.blob);
        let prepare = prepare_message(Operation::SendMessages, 1, 1, &body).transmute_header(
            |original, header: &mut PrepareHeader| {
                *header = original;
                header.cluster = 1;
                header.group = namespace.inner();
                header.checksum = header.identity_checksum();
            },
        );
        let checksum = prepare.header().checksum;
        persistence
            .append(prepare.clone().into_frozen(), true)
            .unwrap();
        assert!(persistence.start());
        Rc::clone(&persistence).run().await;
        partition
            .log
            .journal()
            .inner
            .append(prepare.into_frozen())
            .await
            .unwrap();
        partition.log.journal_mut().info.messages_count = 1;
        partition.log.journal_mut().info.size = iggy_common::IggyByteSize::from(body.len() as u64);
        partition.consensus().sequencer().set_sequence(1);
        partition.consensus().set_last_prepare_checksum(checksum);
        partition.consensus().restore_commit_state(1, 1);
        partition.log.index_writers_mut()[0] = Some(Rc::new(
            partitions::IggyIndexWriter::new(
                "/dev/full",
                Rc::new(std::sync::atomic::AtomicU64::new(0)),
                false,
                false,
            )
            .await
            .unwrap(),
        ));
        persistence.request_checkpoint();
        assert!(partition.needs_persistence_checkpoint());
        assert!(partition.fatal().is_none());
        shard.plane.partitions().insert(namespace, partition);

        let fault = shard
            .tick_partitions(&mut Vec::new())
            .await
            .expect("the checkpoint fault must be returned in its originating sweep");
        assert_eq!(fault.namespace_raw, namespace.inner());
        assert_eq!(fault.op, 1);
        assert_eq!(fault.operation, None);
        assert_eq!(persistence.checkpoint_op(), 0);
    }

    #[compio::test]
    async fn pre_resolved_balanced_write_keeps_its_namespace_and_admission_frontier() {
        const CLIENT: u128 = 1;
        const ADMISSION_FRONTIER: u64 = 10;
        const LATER_FRONTIER: u64 = 20;
        let bus = SpyBus::default();
        let mut shard = test_shard(&bus, 0, 1, 1);
        let (sender, owner_inbox, _owner_replies) = shard_channel(0, 1, 1);
        shard.attach_senders(vec![sender]);
        let shard = Rc::new(shard);
        let metadata = shard.plane.metadata();
        metadata.mux_stm.users().ensure_root_user("iggy", "hash");
        metadata
            .mux_stm
            .update(prepare_message(
                Operation::CreateStream,
                CLIENT,
                1,
                &CreateStreamRequest {
                    name: WireName::new("stream").unwrap(),
                    options: WireOptions::empty(),
                }
                .to_bytes(),
            ))
            .unwrap();
        metadata
            .mux_stm
            .update(prepare_message(
                Operation::CreateTopicWithAssignments,
                CLIENT,
                2,
                &CreateTopicWithAssignmentsRequest {
                    request: CreateTopicRequest {
                        stream_id: WireIdentifier::numeric(0),
                        partitions_count: 2,
                        name: WireName::new("topic").unwrap(),
                        options: WireOptions::empty(),
                    },
                    derived_options: WireOptions::empty(),
                    partitions: vec![
                        CreatedPartitionAssignment {
                            initial_view: 0,
                            partition_id: 0,
                            consensus_group_id: 1,
                        },
                        CreatedPartitionAssignment {
                            initial_view: 0,
                            partition_id: 1,
                            consensus_group_id: 2,
                        },
                    ],
                }
                .to_bytes(),
            ))
            .unwrap();
        metadata.applied_frontier().advance(ADMISSION_FRONTIER);
        let wire_header = SendMessagesHeader {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            partitioning: WirePartitioning::Balanced,
            messages_count: 1,
        }
        .to_bytes();
        let mut body = Vec::new();
        body.extend_from_slice(&u32::try_from(wire_header.len()).unwrap().to_le_bytes());
        body.extend_from_slice(&wire_header);
        let namespace =
            resolve_partition_request_namespace(&shard, Operation::SendMessages, &body, CLIENT)
                .unwrap();
        let delayed_shard = Rc::clone(&shard);
        let updater = compio::runtime::spawn(async move {
            compio::time::sleep(std::time::Duration::from_millis(1)).await;
            delayed_shard
                .plane
                .metadata()
                .applied_frontier()
                .advance(LATER_FRONTIER);
            for partition_id in 0..2 {
                let namespace = delayed_shard
                    .plane
                    .metadata()
                    .mux_stm
                    .streams()
                    .namespace_from_partition(
                        &WireIdentifier::numeric(0),
                        &WireIdentifier::numeric(0),
                        partition_id,
                    )
                    .unwrap();
                delayed_shard
                    .shards_table()
                    .insert(namespace, PartitionLocation::new(ShardId::new(0), 0));
            }
        });
        dispatch_partition_request(
            &shard,
            request_message(Operation::SendMessages, CLIENT, 1, 1, &body),
            CLIENT,
            1,
            ClientOrigin::capture(&shard.bus, CLIENT),
            Some(DEFAULT_ROOT_USER_ID),
            None,
            Some(namespace),
            None,
        )
        .await;
        let _ = updater.await;
        let ShardFrame::Lifecycle(LifecycleFrame::PartitionSubmit { request, .. }) =
            owner_inbox.try_recv().unwrap()
        else {
            panic!("the dispatcher must submit one partition write");
        };
        assert_eq!(
            request.header().group,
            namespace,
            "Balanced must advance once for the gate and write"
        );
        assert_eq!(
            request.header().metadata_watermark,
            ADMISSION_FRONTIER,
            "routing waits cannot promote an old binding past Logout"
        );
        assert_eq!(metadata.applied_frontier().get(), LATER_FRONTIER);
    }

    #[compio::test]
    async fn given_poll_strategies_when_resolving_progress_should_only_order_stored_offset_dependencies()
     {
        const CONSUMER: u32 = 1;
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, 0, 1, 1));
        let namespace = create_one_partition(&shard);
        let expected = Some((namespace.inner(), KIND_CONSUMER, CONSUMER as usize));
        for (strategy, reads_stored) in [
            (WirePollingStrategy::offset(0), false),
            (WirePollingStrategy::timestamp(1), false),
            (WirePollingStrategy::first(), false),
            (WirePollingStrategy::last(), false),
            (WirePollingStrategy::next(), true),
        ] {
            for auto_commit in [false, true] {
                let body = PollMessagesRequest {
                    consumer: WireConsumer::consumer(WireIdentifier::numeric(CONSUMER)),
                    stream_id: WireIdentifier::numeric(0),
                    topic_id: WireIdentifier::numeric(0),
                    partition_id: Some(0),
                    strategy: strategy.clone(),
                    count: 1,
                    auto_commit,
                }
                .to_bytes();
                let mut request = request_message(Operation::NonReplicated, 1, 1, 1, &body);
                bytemuck::checked::from_bytes_mut::<RequestHeader>(
                    &mut request.as_mut_slice()[..size_of::<RequestHeader>()],
                )
                .reserved[..4]
                    .copy_from_slice(&POLL_MESSAGES_CODE.to_le_bytes());
                assert_eq!(
                    progress_key(&shard, &request.into_generic()),
                    if reads_stored || auto_commit {
                        expected
                    } else {
                        None
                    },
                    "strategy {strategy:?}, auto_commit {auto_commit}",
                );
            }
        }
        let requests = [
            request_message(
                Operation::StoreConsumerOffset,
                1,
                1,
                1,
                &StoreConsumerOffsetRequest {
                    consumer: WireConsumer::consumer(WireIdentifier::numeric(CONSUMER)),
                    stream_id: WireIdentifier::numeric(0),
                    topic_id: WireIdentifier::numeric(0),
                    partition_id: Some(0),
                    offset: 1,
                    ack: iggy_binary_protocol::AckLevel::Quorum,
                }
                .to_bytes(),
            ),
            request_message(
                Operation::DeleteConsumerOffset,
                1,
                1,
                2,
                &DeleteConsumerOffsetRequest {
                    consumer: WireConsumer::consumer(WireIdentifier::numeric(CONSUMER)),
                    stream_id: WireIdentifier::numeric(0),
                    topic_id: WireIdentifier::numeric(0),
                    partition_id: Some(0),
                    ack: iggy_binary_protocol::AckLevel::Quorum,
                }
                .to_bytes(),
            ),
        ];
        for request in requests {
            assert_eq!(
                progress_key(&shard, &request.into_generic()),
                expected,
                "explicit writes must retain the same progress lane"
            );
        }
        let mut read = request_message(
            Operation::NonReplicated,
            1,
            1,
            3,
            &GetConsumerOffsetRequest {
                consumer: WireConsumer::consumer(WireIdentifier::numeric(CONSUMER)),
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(0),
                partition_id: Some(0),
            }
            .to_bytes(),
        );
        bytemuck::checked::from_bytes_mut::<RequestHeader>(
            &mut read.as_mut_slice()[..size_of::<RequestHeader>()],
        )
        .reserved[..4]
            .copy_from_slice(&GET_CONSUMER_OFFSET_CODE.to_le_bytes());
        assert_eq!(
            progress_key(&shard, &read.into_generic()),
            expected,
            "stored-offset reads must still wait for explicit progress"
        );
    }

    #[compio::test]
    async fn given_expired_metadata_when_partition_writes_name_revoked_or_missing_topics_should_refuse_before_authorization()
     {
        const READER: u32 = 1;
        let epoch = Instant::now().checked_sub(Duration::from_secs(9)).unwrap();
        let frontier = Arc::new(
            metadata::AppliedFrontier::default()
                .with_partition_freshness(epoch, Duration::from_secs(5)),
        );
        frontier.credit_partition_freshness(epoch, Duration::ZERO);
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard_with_frontier(&bus, 0, 3, 1, frontier));
        create_one_partition(&shard);
        let mut reader = create_reader_request();
        reader.permissions = None;
        assert_eq!(
            shard
                .plane
                .metadata()
                .mux_stm
                .update(prepare_message(
                    Operation::CreateUser,
                    0,
                    3,
                    &reader.to_bytes()
                ))
                .unwrap()
                .code,
            0
        );
        let mut count = 0;
        for name in ["stream", "missing"] {
            let stream = WireIdentifier::named(name).unwrap();
            let topic = WireIdentifier::named("topic").unwrap();
            let raw = [RawMessage {
                id: 1,
                origin_timestamp: 0,
                headers: None,
                payload: b"expired",
            }];
            let mut send = BytesMut::new();
            SendMessagesEncoder::encode(
                &mut send,
                &stream,
                &topic,
                &WirePartitioning::PartitionId(0),
                &raw,
            )
            .unwrap();
            let store = StoreConsumerOffsetRequest {
                consumer: WireConsumer::consumer(WireIdentifier::numeric(7)),
                stream_id: stream.clone(),
                topic_id: topic.clone(),
                partition_id: Some(0),
                offset: 0,
                ack: iggy_binary_protocol::AckLevel::Quorum,
            }
            .to_bytes();
            let delete = DeleteConsumerOffsetRequest {
                consumer: WireConsumer::consumer(WireIdentifier::numeric(7)),
                stream_id: stream,
                topic_id: topic,
                partition_id: Some(0),
                ack: iggy_binary_protocol::AckLevel::Quorum,
            }
            .to_bytes();
            for (operation, body) in [
                (Operation::SendMessages, send.freeze()),
                (Operation::StoreConsumerOffset, store),
                (Operation::DeleteConsumerOffset, delete),
            ] {
                count += 1;
                let admission = dispatch_partition_request(
                    &shard,
                    request_message(operation, 1, 1, count, &body),
                    1,
                    1,
                    ClientOrigin::capture(&shard.bus, 91),
                    Some(READER),
                    None,
                    None,
                    None,
                )
                .await;
                assert!(matches!(admission, PartitionAdmission::Denied));
                let replies = bus.client_replies.borrow();
                let frame = &replies.last().unwrap().1;
                let status = std::mem::offset_of!(ReplyHeader, status);
                assert_eq!(
                    u32::from_le_bytes(frame[status..status + 4].try_into().unwrap()),
                    IggyError::TransientNotAccepted.as_code(),
                    "{name} {operation:?}"
                );
            }
        }
    }

    #[compio::test]
    async fn given_invalid_partition_writes_when_resolving_should_answer_typed_error_codes() {
        const VSR_CLIENT: u128 = 1;
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, 0, 1, 1));
        // Stream 0 / topic 0 / partition 0 committed straight into the STM, so a
        // group op resolves its namespace and reaches the group fence, which
        // runs before the routable wait.
        let md = shard.plane.metadata();
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
                initial_view: 0,
                partition_id: 0,
                consensus_group_id: 1,
            }],
        };
        md.mux_stm
            .update(prepare_message(
                Operation::CreateTopicWithAssignments,
                VSR_CLIENT,
                2,
                &create_topic.to_bytes(),
            ))
            .unwrap();
        // A balanced pick on a topic without partitions finds nothing to resolve.
        let create_empty_topic = CreateTopicWithAssignmentsRequest {
            request: CreateTopicRequest {
                stream_id: WireIdentifier::numeric(0),
                partitions_count: 0,
                name: WireName::new("empty").unwrap(),
                options: WireOptions::empty(),
            },
            derived_options: WireOptions::empty(),
            partitions: Vec::new(),
        };
        md.mux_stm
            .update(prepare_message(
                Operation::CreateTopicWithAssignments,
                VSR_CLIENT,
                3,
                &create_empty_topic.to_bytes(),
            ))
            .unwrap();

        let offset_body = |operation: Operation,
                           consumer: WireConsumer,
                           stream_id: WireIdentifier,
                           topic_id: WireIdentifier| {
            if operation == Operation::StoreConsumerOffset {
                StoreConsumerOffsetRequest {
                    consumer,
                    stream_id,
                    topic_id,
                    partition_id: Some(0),
                    offset: 0,
                    ack: iggy_binary_protocol::AckLevel::Quorum,
                }
                .to_bytes()
            } else {
                DeleteConsumerOffsetRequest {
                    consumer,
                    stream_id,
                    topic_id,
                    partition_id: Some(0),
                    ack: iggy_binary_protocol::AckLevel::Quorum,
                }
                .to_bytes()
            }
        };
        let mut cases = Vec::new();
        for operation in [
            Operation::StoreConsumerOffset,
            Operation::DeleteConsumerOffset,
        ] {
            cases.push((operation, vec![1], IggyError::InvalidCommand));
            cases.push((
                operation,
                offset_body(
                    operation,
                    WireConsumer::consumer(WireIdentifier::Numeric(1)),
                    WireIdentifier::Numeric(404),
                    WireIdentifier::Numeric(1),
                )
                .to_vec(),
                IggyError::StreamIdNotFound(Identifier::numeric(404).unwrap()),
            ));
            // The group fence: an unknown group on a known topic answers the
            // group's own not-found code, numeric and named, not a bare
            // ResourceNotFound.
            cases.push((
                operation,
                offset_body(
                    operation,
                    WireConsumer::consumer_group(WireIdentifier::Numeric(999)),
                    WireIdentifier::Numeric(0),
                    WireIdentifier::Numeric(0),
                )
                .to_vec(),
                IggyError::ConsumerGroupIdNotFound(
                    Identifier::numeric(999).unwrap(),
                    Identifier::numeric(0).unwrap(),
                ),
            ));
            cases.push((
                operation,
                offset_body(
                    operation,
                    WireConsumer::consumer_group(WireIdentifier::named("ghost").unwrap()),
                    WireIdentifier::Numeric(0),
                    WireIdentifier::Numeric(0),
                )
                .to_vec(),
                IggyError::ConsumerGroupNameNotFound(
                    "ghost".to_owned(),
                    Identifier::numeric(0).unwrap(),
                ),
            ));
        }
        let send_body = |stream_id: WireIdentifier,
                         topic_id: WireIdentifier,
                         partitioning: WirePartitioning| {
            let metadata = SendMessagesHeader {
                stream_id,
                topic_id,
                partitioning,
                messages_count: 1,
            }
            .to_bytes();
            let mut body = Vec::with_capacity(4 + metadata.len());
            body.extend_from_slice(&u32::try_from(metadata.len()).unwrap().to_le_bytes());
            body.extend_from_slice(&metadata);
            body
        };
        let missing_stream = || IggyError::StreamIdNotFound(Identifier::numeric(404).unwrap());
        let missing_topic = || {
            IggyError::TopicIdNotFound(
                Identifier::numeric(404).unwrap(),
                Identifier::numeric(0).unwrap(),
            )
        };
        let missing_partition = || {
            IggyError::PartitionNotFound(
                99,
                Identifier::numeric(0).unwrap(),
                Identifier::numeric(0).unwrap(),
            )
        };
        cases.push((Operation::SendMessages, vec![1], IggyError::InvalidCommand));
        for (stream_id, topic_id, partitioning, expected) in [
            (404, 0, WirePartitioning::PartitionId(0), missing_stream()),
            (0, 404, WirePartitioning::PartitionId(0), missing_topic()),
            (0, 0, WirePartitioning::PartitionId(99), missing_partition()),
            (404, 0, WirePartitioning::Balanced, missing_stream()),
            (
                0,
                404,
                WirePartitioning::MessagesKey(b"key".to_vec()),
                missing_topic(),
            ),
        ] {
            cases.push((
                Operation::SendMessages,
                send_body(
                    WireIdentifier::Numeric(stream_id),
                    WireIdentifier::Numeric(topic_id),
                    partitioning,
                ),
                expected,
            ));
        }
        cases.push((
            Operation::SendMessages,
            send_body(
                WireIdentifier::Numeric(0),
                WireIdentifier::named("empty").unwrap(),
                WirePartitioning::Balanced,
            ),
            IggyError::InvalidIdentifier,
        ));
        cases.push((
            Operation::DeleteSegments,
            DeleteSegmentsRequest {
                stream_id: WireIdentifier::Numeric(0),
                topic_id: WireIdentifier::Numeric(0),
                partition_id: 99,
                segments_count: 1,
            }
            .to_bytes()
            .to_vec(),
            missing_partition(),
        ));
        for (index, (operation, body, expected)) in cases.into_iter().enumerate() {
            let request = request_message(operation, 1, 1, index as u64 + 1, &body);
            dispatch_partition_request(
                &shard,
                request,
                1,
                1,
                ClientOrigin::capture(&shard.bus, 91),
                Some(DEFAULT_ROOT_USER_ID),
                None,
                None,
                None,
            )
            .await;
            let replies = bus.client_replies.borrow();
            assert_eq!(
                replies.len(),
                index + 1,
                "{operation:?} must reply exactly once"
            );
            let (_, frame) = replies.last().unwrap();
            let start = std::mem::offset_of!(ReplyHeader, status);
            let status = u32::from_le_bytes(frame[start..start + 4].try_into().unwrap());
            assert_eq!(status, expected.as_code(), "{operation:?}");
        }
    }

    /// An undecodable request body is a PERMANENT client error, so every read
    /// on this path must answer a nonzero status. The fail-fast shapes these
    /// used to borrow all decode as success: the empty poll reads as a
    /// 0-message poll, an empty consumer-offset body as "no offset stored",
    /// and a status-0 `delete_segments` ack as a completed trim. A client on a
    /// skewed protocol would loop on those forever with no error to surface.
    #[compio::test]
    async fn undecodable_read_bodies_must_deny_typed_not_fabricate_success() {
        const TRANSPORT: u128 = 91;
        const VSR_CLIENT: u128 = 1;
        const SESSION: u64 = 1;
        const STATUS_OFFSET: usize = std::mem::offset_of!(ReplyHeader, status);
        // Shorter than any of the three request encodings, so each decoder
        // fails on length before it can interpret a field.
        const TRUNCATED_BODY: &[u8] = &[0x01];

        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, 0, 1, 1));
        let attachment = attach_root_session(&shard, VSR_CLIENT, SESSION);

        let poll = request_message(
            Operation::NonReplicated,
            VSR_CLIENT,
            SESSION,
            1,
            TRUNCATED_BODY,
        );
        handle_poll_messages(
            &shard,
            ClientOrigin::capture(&shard.bus, TRANSPORT),
            &poll,
            Some(DEFAULT_ROOT_USER_ID),
            poll.header().client,
            &attachment,
        )
        .await;

        let offset = request_message(
            Operation::NonReplicated,
            VSR_CLIENT,
            SESSION,
            2,
            TRUNCATED_BODY,
        );
        handle_get_consumer_offset(
            &shard,
            ClientOrigin::capture(&shard.bus, TRANSPORT),
            &offset,
            Some(DEFAULT_ROOT_USER_ID),
            Some(&attachment),
        )
        .await;

        let delete = request_message(
            Operation::DeleteSegments,
            VSR_CLIENT,
            SESSION,
            3,
            TRUNCATED_BODY,
        );
        handle_delete_segments_request(
            &shard,
            ClientOrigin::capture(&shard.bus, TRANSPORT),
            Some((VSR_CLIENT, SESSION)),
            &delete,
        )
        .await;

        let replies = bus.client_replies.borrow();
        assert_eq!(
            replies.len(),
            3,
            "one deny frame per undecodable request, none of them silent"
        );
        for (label, (client, frame)) in ["poll_messages", "get_consumer_offset", "delete_segments"]
            .into_iter()
            .zip(replies.iter())
        {
            assert_eq!(*client, TRANSPORT, "{label} deny must target the transport");
            let status =
                u32::from_le_bytes(frame[STATUS_OFFSET..STATUS_OFFSET + 4].try_into().unwrap());
            assert_eq!(
                status,
                IggyError::InvalidCommand.as_code(),
                "{label} with an undecodable body must carry a nonzero status"
            );
        }
    }

    /// A WELL-FORMED read against a stream that does not exist is a permanent
    /// addressing error, and it reaches the server through a public SDK method
    /// (a typo'd or deleted stream). Both fail-fast shapes decode as success:
    /// the empty poll as a 0-message read, and an empty consumer-offset body
    /// as `None`, which is indistinguishable from a fresh consumer -- so the
    /// caller resumes from its configured default and silently reprocesses.
    /// The same lookup 404s over REST.
    #[compio::test]
    async fn unresolved_read_targets_must_deny_typed_not_read_as_empty() {
        const TRANSPORT: u128 = 91;
        const VSR_CLIENT: u128 = 1;
        const SESSION: u64 = 1;
        const STATUS_OFFSET: usize = std::mem::offset_of!(ReplyHeader, status);
        const MISSING_STREAM: u32 = 404;

        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, 0, 1, 1));
        let attachment = attach_root_session(&shard, VSR_CLIENT, SESSION);

        let poll_body = PollMessagesRequest {
            consumer: WireConsumer::consumer(WireIdentifier::Numeric(1)),
            stream_id: WireIdentifier::Numeric(MISSING_STREAM),
            topic_id: WireIdentifier::Numeric(1),
            partition_id: Some(1),
            strategy: WirePollingStrategy::offset(0),
            count: 10,
            auto_commit: false,
        }
        .to_bytes();
        let poll = request_message(Operation::NonReplicated, VSR_CLIENT, SESSION, 1, &poll_body);
        handle_poll_messages(
            &shard,
            ClientOrigin::capture(&shard.bus, TRANSPORT),
            &poll,
            Some(DEFAULT_ROOT_USER_ID),
            poll.header().client,
            &attachment,
        )
        .await;

        let offset_body = GetConsumerOffsetRequest {
            consumer: WireConsumer::consumer(WireIdentifier::Numeric(1)),
            stream_id: WireIdentifier::Numeric(MISSING_STREAM),
            topic_id: WireIdentifier::Numeric(1),
            partition_id: Some(1),
        }
        .to_bytes();
        let offset = request_message(
            Operation::NonReplicated,
            VSR_CLIENT,
            SESSION,
            2,
            &offset_body,
        );
        handle_get_consumer_offset(
            &shard,
            ClientOrigin::capture(&shard.bus, TRANSPORT),
            &offset,
            Some(DEFAULT_ROOT_USER_ID),
            Some(&attachment),
        )
        .await;

        let replies = bus.client_replies.borrow();
        assert_eq!(
            replies.len(),
            2,
            "one deny frame per unresolved read, none of them silent"
        );
        for (label, (client, frame)) in ["poll_messages", "get_consumer_offset"]
            .into_iter()
            .zip(replies.iter())
        {
            assert_eq!(*client, TRANSPORT, "{label} deny must target the transport");
            let status =
                u32::from_le_bytes(frame[STATUS_OFFSET..STATUS_OFFSET + 4].try_into().unwrap());
            assert_eq!(
                status,
                IggyError::StreamIdNotFound(
                    Identifier::numeric(MISSING_STREAM).expect("a nonzero numeric identifier")
                )
                .as_code(),
                "{label} against a missing stream must carry the not-found status"
            );
        }
    }

    #[compio::test]
    async fn given_unroutable_partition_when_polling_should_report_not_accepted() {
        let bus = SpyBus::default();
        let mut shard = test_shard(&bus, 0, 1, 1);
        let (sender, owner_inbox, _owner_replies) = shard_channel(0, 1, 1);
        shard.attach_senders(vec![sender]);
        let shard = Rc::new(shard);
        let namespace = create_one_partition(&shard);

        // The owner has a live inbox, but no route identifies it for this partition.
        assert_eq!(
            poll_and_expect_error(&shard, namespace).await,
            IggyError::TransientNotAccepted
        );
        assert!(
            owner_inbox.try_recv().is_err(),
            "a poll rejected during routing must never reach the owner"
        );
        shard
            .shards_table()
            .insert(namespace, PartitionLocation::new(ShardId::new(0), 0));
        assert_poll_reaches_owner(&shard, namespace, &owner_inbox).await;
    }

    #[compio::test]
    async fn given_missing_owner_sender_when_polling_should_report_not_accepted() {
        const OWNER_WITHOUT_SENDER: u16 = 1;
        let bus = SpyBus::default();
        let mut shard = test_shard(&bus, 0, 1, 1);
        let (sender, owner_inbox, _owner_replies) = shard_channel(0, 1, 1);
        shard.attach_senders(vec![sender]);
        let shard = Rc::new(shard);
        let namespace = create_one_partition(&shard);
        shard.shards_table().insert(
            namespace,
            PartitionLocation::new(ShardId::new(OWNER_WITHOUT_SENDER), 0),
        );

        // A route exists, but no sender was attached to submit work to its owner.
        assert_eq!(
            poll_and_expect_error(&shard, namespace).await,
            IggyError::TransientNotAccepted
        );
        shard
            .shards_table()
            .insert(namespace, PartitionLocation::new(ShardId::new(0), 0));
        assert_poll_reaches_owner(&shard, namespace, &owner_inbox).await;
    }

    #[compio::test]
    async fn given_full_owner_inbox_when_polling_should_report_not_accepted() {
        let bus = SpyBus::default();
        let mut shard = test_shard(&bus, 0, 1, 1);
        let inbox_capacity = 1;
        let (sender, owner_inbox, _owner_replies) = shard_channel(0, inbox_capacity, 1);
        // Occupy the only slot before polling; no task drains this inbox.
        sender
            .try_send(ShardFrame::lifecycle(LifecycleFrame::MetadataCommitTick))
            .unwrap();
        shard.attach_senders(vec![sender]);
        let shard = Rc::new(shard);
        let namespace = route_one_partition(&shard);

        assert_eq!(
            poll_and_expect_error(&shard, namespace).await,
            IggyError::TransientNotAccepted
        );
        assert!(matches!(
            owner_inbox.try_recv().unwrap(),
            ShardFrame::Lifecycle(LifecycleFrame::MetadataCommitTick)
        ));
        assert!(
            owner_inbox.try_recv().is_err(),
            "only the filler frame may be queued; the poll was never submitted"
        );
        assert_poll_reaches_owner(&shard, namespace, &owner_inbox).await;
    }

    #[compio::test]
    async fn given_closed_owner_inbox_when_polling_should_report_not_accepted() {
        let bus = SpyBus::default();
        let mut shard = test_shard(&bus, 0, 1, 1);
        let (sender, owner_inbox, _owner_replies) = shard_channel(0, 1, 1);
        shard.attach_senders(vec![sender]);
        let shard = Rc::new(shard);
        let namespace = route_one_partition(&shard);
        assert_poll_reaches_owner(&shard, namespace, &owner_inbox).await;
        // Closing the destination leaves its sender attached, so submission fails.
        drop(owner_inbox);

        assert_eq!(
            poll_and_expect_error(&shard, namespace).await,
            IggyError::TransientNotAccepted
        );
    }

    #[compio::test]
    async fn given_submitted_poll_when_owner_drops_reply_should_report_unknown_acceptance() {
        let bus = SpyBus::default();
        let mut shard = test_shard(&bus, 0, 1, 1);
        let (sender, owner_inbox, _owner_replies) = shard_channel(0, 1, 1);
        shard.attach_senders(vec![sender]);
        let shard = Rc::new(shard);
        let namespace = route_one_partition(&shard);

        // Receive the request before dropping its reply. Submission succeeded,
        // so the caller cannot infer whether the owner accepted any progress.
        let owner_task = compio::runtime::spawn(async move {
            let ShardFrame::Lifecycle(LifecycleFrame::PartitionRead { reply, .. }) =
                owner_inbox.recv().await.unwrap()
            else {
                panic!("the poll must have reached the owner");
            };
            drop(reply);
        });

        assert_eq!(
            poll_and_expect_error(&shard, namespace).await,
            IggyError::ShardCommunicationError
        );
        owner_task.await.expect("the owner task must finish");
    }

    #[compio::test]
    async fn given_pending_poll_when_owner_reply_times_out_should_report_unknown_acceptance() {
        const TRANSPORT_CLIENT_ID: u128 = 91;
        const VSR_CLIENT_ID: u128 = 1;
        let bus = SpyBus::default();
        // Expire the reply timeout deterministically, without a wall clock wait.
        bus.instant_timers.set(true);
        let mut shard = test_shard(&bus, 0, 1, 1);
        let (sender, owner_inbox, _owner_replies) = shard_channel(0, 1, 1);
        shard.attach_senders(vec![sender]);
        let shard = Rc::new(shard);
        let namespace = route_one_partition(&shard);
        let attachment = attach_root_session(&shard, VSR_CLIENT_ID, 1);
        let poll_body = PollMessagesRequest {
            consumer: WireConsumer::consumer(WireIdentifier::numeric(1)),
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            partition_id: Some(0),
            strategy: WirePollingStrategy::offset(0),
            count: 10,
            auto_commit: true,
        }
        .to_bytes();
        let request = with_partition_incarnation(
            &shard,
            namespace,
            request_message(Operation::NonReplicated, VSR_CLIENT_ID, 1, 1, &poll_body),
        );

        // Keep the owner request queued so its live reply sender forces the
        // timeout path. The owner can still receive the request after that timeout.
        handle_poll_messages(
            &shard,
            ClientOrigin::capture(&shard.bus, TRANSPORT_CLIENT_ID),
            &request,
            Some(DEFAULT_ROOT_USER_ID),
            request.header().client,
            &attachment,
        )
        .await;

        // The client receives one error header, with no successful empty poll body.
        let replies = bus.client_replies.borrow();
        assert_eq!(replies.len(), 1);
        let (client_id, frame) = &replies[0];
        assert_eq!(*client_id, TRANSPORT_CLIENT_ID);
        assert_eq!(frame.len(), std::mem::size_of::<ReplyHeader>());
        let status_start = std::mem::offset_of!(ReplyHeader, status);
        let status = u32::from_le_bytes(frame[status_start..status_start + 4].try_into().unwrap());
        assert_eq!(status, IggyError::ShardCommunicationError.as_code());
        drop(replies);

        // Timing out abandons the reply receiver; a late owner reply cannot
        // replace the communication error or produce a second client response.
        let ShardFrame::Lifecycle(LifecycleFrame::PartitionRead { reply, .. }) =
            owner_inbox.try_recv().unwrap()
        else {
            panic!("the timed out poll must remain queued for its owner");
        };
        assert!(
            reply
                .try_send(PartitionReadReply::Rejected(
                    IggyError::TransientNotAccepted
                ))
                .is_err(),
            "the timeout must have dropped the caller's reply receiver"
        );
        assert_eq!(bus.client_replies.borrow().len(), 1);
    }

    /// A relayed write whose owner drops the reply channel (a view-change
    /// reset, a park eviction) has an unknown outcome. The client must hear
    /// that at once under its own request id, so it replays the id instead of
    /// waiting out its whole deadline.
    #[compio::test]
    async fn given_relayed_write_when_owner_drops_reply_should_answer_not_committed() {
        let bus = SpyBus::default();
        let owner_inbox = relay_partition_write(&bus).await;
        let ShardFrame::Lifecycle(LifecycleFrame::PartitionSubmit { reply, .. }) =
            owner_inbox.try_recv().unwrap()
        else {
            panic!("the write must reach its owner");
        };
        drop(reply);

        assert_relayed_not_committed(&bus).await;
    }

    /// An owner that does not answer within the submit budget leaves the
    /// outcome unknown too. The answer must come while the client can still
    /// replay, so the budget stays below the SDK's operation deadline.
    #[compio::test]
    async fn given_relayed_write_when_owner_reply_expires_should_answer_not_committed() {
        let bus = SpyBus::default();
        bus.instant_timers.set(true);
        // Held, so the reply sender stays alive and only the budget can expire.
        let _owner_inbox = relay_partition_write(&bus).await;

        assert_relayed_not_committed(&bus).await;
        let budget = *bus
            .sleeps
            .borrow()
            .last()
            .expect("the wait arms its budget");
        assert!(
            budget < iggy_common::BINARY_OPERATION_TIMEOUT,
            "the expiry answer must reach a client that can still replay, budget {budget:?}"
        );
    }

    const RELAYED_TRANSPORT: u128 = 91;
    const RELAYED_REQUEST: u64 = 7;

    /// Dispatch a partition write from a transport whose id differs from its
    /// VSR client id, so the committed reply is relayed. Returns the owner
    /// inbox holding the submitted write and its reply sender.
    async fn relay_partition_write(bus: &SpyBus) -> Receiver<ShardFrame> {
        const VSR_CLIENT: u128 = 1;
        const SESSION: u64 = 1;
        let mut shard = test_shard(bus, 0, 1, 1);
        let (sender, owner_inbox, _owner_replies) = shard_channel(0, 1, 1);
        shard.attach_senders(vec![sender]);
        let shard = Rc::new(shard);
        let namespace = route_one_partition(&shard);
        let admission = dispatch_partition_request(
            &shard,
            request_message(
                Operation::SendMessages,
                VSR_CLIENT,
                SESSION,
                RELAYED_REQUEST,
                &[],
            ),
            VSR_CLIENT,
            SESSION,
            ClientOrigin::capture(&shard.bus, RELAYED_TRANSPORT),
            Some(DEFAULT_ROOT_USER_ID),
            None,
            Some(namespace.inner()),
            None,
        )
        .await;
        assert!(matches!(admission, PartitionAdmission::Admitted));
        owner_inbox
    }

    /// Wait for the relay task to answer, then check that its one frame
    /// carries `TransientNotCommitted` for the relayed request.
    async fn assert_relayed_not_committed(bus: &SpyBus) {
        const POLL: std::time::Duration = std::time::Duration::from_millis(1);
        const MAX_POLLS: usize = 1_000;
        const STATUS_OFFSET: usize = std::mem::offset_of!(ReplyHeader, status);
        const REQUEST_OFFSET: usize = std::mem::offset_of!(ReplyHeader, request);
        for _ in 0..MAX_POLLS {
            if !bus.client_replies.borrow().is_empty() {
                break;
            }
            compio::time::sleep(POLL).await;
        }
        let replies = bus.client_replies.borrow();
        assert_eq!(replies.len(), 1, "an unknown outcome must not be silent");
        let (client, frame) = &replies[0];
        assert_eq!(*client, RELAYED_TRANSPORT);
        let status =
            u32::from_le_bytes(frame[STATUS_OFFSET..STATUS_OFFSET + 4].try_into().unwrap());
        assert_eq!(status, IggyError::TransientNotCommitted.as_code());
        let request = u64::from_le_bytes(
            frame[REQUEST_OFFSET..REQUEST_OFFSET + 8]
                .try_into()
                .unwrap(),
        );
        assert_eq!(request, RELAYED_REQUEST, "the replay must reuse this id");
    }

    /// A consumer drops its position on `HistoryUnavailable`. A caller context
    /// from a node ahead of this one can name an incarnation this node has not
    /// applied yet, so only metadata this node has applied can prove it gone.
    #[compio::test]
    async fn given_context_ahead_of_local_metadata_when_polling_should_report_not_accepted() {
        const TRANSPORT_CLIENT_ID: u128 = 91;
        const VSR_CLIENT_ID: u128 = 1;
        const SESSION: u64 = 1;
        const LOCAL_FRONTIER: u64 = 10;
        const STATUS_OFFSET: usize = std::mem::offset_of!(ReplyHeader, status);
        let bus = SpyBus::default();
        // A poll the gate wrongly admits fails on the owner reply timeout
        // instead of hanging the test.
        bus.instant_timers.set(true);
        let mut shard = test_shard(&bus, 0, 1, 1);
        let (sender, owner_inbox, _owner_replies) = shard_channel(0, 1, 1);
        shard.attach_senders(vec![sender]);
        let shard = Rc::new(shard);
        route_one_partition(&shard);
        let metadata = shard.plane.metadata();
        metadata.applied_frontier().advance(LOCAL_FRONTIER);
        let attachment = attach_root_session(&shard, VSR_CLIENT_ID, SESSION);
        let streams = metadata.mux_stm.streams();
        let namespace = streams
            .namespace_from_partition(&WireIdentifier::numeric(0), &WireIdentifier::numeric(0), 0)
            .unwrap();
        let incarnation = streams.created_revision_for_namespace(namespace).unwrap();
        let poll_body = PollMessagesRequest {
            consumer: WireConsumer::consumer(WireIdentifier::numeric(1)),
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            partition_id: Some(0),
            strategy: WirePollingStrategy::offset(0),
            count: 10,
            auto_commit: false,
        }
        .to_bytes();

        for (minimum_metadata_op, expected) in [
            (LOCAL_FRONTIER + 1, IggyError::TransientNotAccepted),
            (LOCAL_FRONTIER, IggyError::HistoryUnavailable),
        ] {
            let request = request_message(
                Operation::NonReplicated,
                VSR_CLIENT_ID,
                SESSION,
                1,
                &poll_body,
            )
            .transmute_header(|old, header: &mut RoutedRequestHeader| {
                *header = old;
                header.partition_incarnation = incarnation + 1;
                header.minimum_metadata_op = minimum_metadata_op;
            });
            handle_poll_messages(
                &shard,
                ClientOrigin::capture(&shard.bus, TRANSPORT_CLIENT_ID),
                &request,
                Some(DEFAULT_ROOT_USER_ID),
                VSR_CLIENT_ID,
                &attachment,
            )
            .await;

            let (client_id, frame) = bus.client_replies.borrow_mut().pop().unwrap();
            assert_eq!(client_id, TRANSPORT_CLIENT_ID);
            let status =
                u32::from_le_bytes(frame[STATUS_OFFSET..STATUS_OFFSET + 4].try_into().unwrap());
            assert_eq!(
                IggyError::from_code(status),
                expected,
                "a context requiring metadata op {minimum_metadata_op} at local frontier {LOCAL_FRONTIER}"
            );
        }
        assert!(
            owner_inbox.try_recv().is_err(),
            "a refused poll must never reach the owner"
        );
    }

    /// The re-sync sentinel tells a group consumer that its assignment is
    /// stale. A caller context from a node ahead of this one can name a member
    /// this node has not applied yet, so the group fence may judge it only
    /// once this node has applied that metadata.
    #[compio::test]
    async fn given_group_poll_ahead_of_local_metadata_when_member_is_unknown_should_report_not_accepted()
     {
        const TRANSPORT_CLIENT_ID: u128 = 91;
        const VSR_CLIENT_ID: u128 = 1;
        const SESSION: u64 = 1;
        const LOCAL_FRONTIER: u64 = 10;
        const UNKNOWN_GROUP_ID: u32 = 1;
        const STATUS_OFFSET: usize = std::mem::offset_of!(ReplyHeader, status);
        const BODY_OFFSET: usize = std::mem::size_of::<ReplyHeader>();
        let bus = SpyBus::default();
        // A poll the gate wrongly admits fails on the owner reply timeout
        // instead of hanging the test.
        bus.instant_timers.set(true);
        let mut shard = test_shard(&bus, 0, 1, 1);
        let (sender, owner_inbox, _owner_replies) = shard_channel(0, 1, 1);
        shard.attach_senders(vec![sender]);
        let shard = Rc::new(shard);
        route_one_partition(&shard);
        shard
            .plane
            .metadata()
            .applied_frontier()
            .advance(LOCAL_FRONTIER);
        let attachment = attach_root_session(&shard, VSR_CLIENT_ID, SESSION);
        let poll_body = PollMessagesRequest {
            consumer: WireConsumer::consumer_group(WireIdentifier::numeric(UNKNOWN_GROUP_ID)),
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            partition_id: Some(0),
            strategy: WirePollingStrategy::offset(0),
            count: 10,
            auto_commit: false,
        }
        .to_bytes();

        // A refusal carries no body. The fence answers a status-0 empty poll
        // whose partition id is the sentinel.
        for (minimum_metadata_op, expected) in [
            (
                LOCAL_FRONTIER + 1,
                (IggyError::TransientNotAccepted.as_code(), None),
            ),
            (
                LOCAL_FRONTIER,
                (0, Some(RESYNC_REQUIRED_PARTITION_SENTINEL)),
            ),
        ] {
            let request = request_message(
                Operation::NonReplicated,
                VSR_CLIENT_ID,
                SESSION,
                1,
                &poll_body,
            )
            .transmute_header(|old, header: &mut RoutedRequestHeader| {
                *header = old;
                header.minimum_metadata_op = minimum_metadata_op;
            });
            handle_poll_messages(
                &shard,
                ClientOrigin::capture(&shard.bus, TRANSPORT_CLIENT_ID),
                &request,
                Some(DEFAULT_ROOT_USER_ID),
                VSR_CLIENT_ID,
                &attachment,
            )
            .await;

            let (client_id, frame) = bus.client_replies.borrow_mut().pop().unwrap();
            assert_eq!(client_id, TRANSPORT_CLIENT_ID);
            let status =
                u32::from_le_bytes(frame[STATUS_OFFSET..STATUS_OFFSET + 4].try_into().unwrap());
            let polled_partition_id = frame
                .get(BODY_OFFSET..BODY_OFFSET + 4)
                .map(|bytes| u32::from_le_bytes(bytes.try_into().unwrap()));
            assert_eq!(
                (status, polled_partition_id),
                expected,
                "a group poll requiring metadata op {minimum_metadata_op} at local frontier {LOCAL_FRONTIER}"
            );
        }
        assert!(
            owner_inbox.try_recv().is_err(),
            "a refused or fenced poll must never reach the owner"
        );
    }

    #[compio::test]
    async fn given_group_offset_ahead_of_local_metadata_when_group_is_unknown_should_report_not_accepted()
     {
        const TRANSPORT: u128 = 91;
        const CLIENT: u128 = 1;
        const SESSION: u64 = 1;
        const LOCAL_FRONTIER: u64 = 10;
        const UNKNOWN_GROUP: u32 = 1;
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, 0, 1, 1));
        route_one_partition(&shard);
        shard
            .plane
            .metadata()
            .applied_frontier()
            .advance(LOCAL_FRONTIER);
        let attachment = attach_root_session(&shard, CLIENT, SESSION);
        for operation in [
            Operation::StoreConsumerOffset,
            Operation::DeleteConsumerOffset,
        ] {
            let offset = GetConsumerOffsetRequest {
                consumer: WireConsumer::consumer_group(WireIdentifier::numeric(UNKNOWN_GROUP)),
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(0),
                partition_id: Some(0),
            };
            let body = if operation == Operation::StoreConsumerOffset {
                StoreConsumerOffsetRequest {
                    consumer: offset.consumer,
                    stream_id: offset.stream_id,
                    topic_id: offset.topic_id,
                    partition_id: offset.partition_id,
                    offset: 0,
                    ack: iggy_binary_protocol::AckLevel::Quorum,
                }
                .to_bytes()
            } else {
                DeleteConsumerOffsetRequest {
                    consumer: offset.consumer,
                    stream_id: offset.stream_id,
                    topic_id: offset.topic_id,
                    partition_id: offset.partition_id,
                    ack: iggy_binary_protocol::AckLevel::Quorum,
                }
                .to_bytes()
            };
            for (minimum_metadata_op, expected) in [
                (
                    LOCAL_FRONTIER + 1,
                    IggyError::TransientNotAccepted.as_code(),
                ),
                (
                    LOCAL_FRONTIER,
                    IggyError::ConsumerGroupIdNotFound(
                        Identifier::numeric(UNKNOWN_GROUP).unwrap(),
                        Identifier::numeric(0).unwrap(),
                    )
                    .as_code(),
                ),
            ] {
                let request = request_message(operation, CLIENT, SESSION, 1, &body)
                    .transmute_header(|old, header: &mut RoutedRequestHeader| {
                        *header = old;
                        header.minimum_metadata_op = minimum_metadata_op;
                    });
                dispatch_partition_request(
                    &shard,
                    request,
                    CLIENT,
                    SESSION,
                    ClientOrigin::capture(&shard.bus, TRANSPORT),
                    Some(DEFAULT_ROOT_USER_ID),
                    Some((CLIENT, attachment.clone())),
                    None,
                    None,
                )
                .await;
                let (client, reply) = bus.client_replies.borrow_mut().pop().unwrap();
                assert_eq!(client, TRANSPORT);
                let reply = bytemuck::checked::try_pod_read_unaligned::<ReplyHeader>(
                    &reply[..size_of::<ReplyHeader>()],
                )
                .unwrap();
                assert_eq!(
                    reply.status, expected,
                    "{operation:?} at {minimum_metadata_op}"
                );
            }
        }
    }

    /// HTTP polls share this gate with TCP. A context from a node ahead of this
    /// one is refused only as not accepted, whatever incarnation or owner it
    /// names. Once this node has applied that metadata, each mismatch keeps its
    /// own refusal.
    #[test]
    fn given_context_ahead_of_local_metadata_when_fencing_poll_should_report_not_accepted() {
        const CLIENT_ID: u128 = (1 << 64) + 3;
        const PARTITION_ID: u32 = 0;
        let local = PartitionContext {
            incarnation: 7,
            owner_generation: 2,
            metadata_op: 10,
        };
        let another_incarnation = PartitionContext {
            incarnation: 8,
            ..local
        };
        let another_owner = PartitionContext {
            owner_generation: 3,
            ..local
        };
        for requested in [another_incarnation, another_owner] {
            let ahead = PartitionContext {
                metadata_op: local.metadata_op + 1,
                ..requested
            };
            let refusal = fence_poll_context(ahead, local, CLIENT_ID, PARTITION_ID);
            assert!(
                matches!(refusal, Err(IggyError::TransientNotAccepted)),
                "{ahead:?} against {local:?} answered {refusal:?}"
            );
        }

        assert!(matches!(
            fence_poll_context(another_incarnation, local, CLIENT_ID, PARTITION_ID),
            Err(IggyError::HistoryUnavailable)
        ));
        assert!(matches!(
            fence_poll_context(another_owner, local, CLIENT_ID, PARTITION_ID),
            Err(IggyError::ConsumerGroupPartitionNotOwned(
                CLIENT_ID,
                PARTITION_ID
            ))
        ));
        assert!(fence_poll_context(local, local, CLIENT_ID, PARTITION_ID).is_ok());
    }

    /// A member poll whose context names an owner generation that this node
    /// did not install for the member is refused as not owned. The refusal
    /// names the member client, as the resolve fence does, not the group.
    #[compio::test]
    async fn given_other_owner_generation_when_member_polls_should_name_the_member_client() {
        const TRANSPORT_CLIENT_ID: u128 = 91;
        // Wider than u64, so a refusal that truncated it would name another client.
        const MEMBER_CLIENT_ID: u128 = (1 << 64) + 41;
        const SESSION: u64 = 1;
        const GROUP_ID: u32 = 0;
        const PARTITION_ID: u32 = 0;
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, 0, 1, 1));
        let namespace = create_one_partition(&shard);
        let attachment = attach_root_session(&shard, MEMBER_CLIENT_ID, SESSION);
        let mux = &shard.plane.metadata().mux_stm;
        mux.update(prepare_message(
            Operation::CreateConsumerGroup,
            MEMBER_CLIENT_ID,
            3,
            &CreateConsumerGroupRequest {
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(0),
                name: WireName::new("group").unwrap(),
            }
            .to_bytes(),
        ))
        .unwrap();
        mux.update(prepare_message(
            Operation::JoinConsumerGroup,
            MEMBER_CLIENT_ID,
            4,
            &JoinConsumerGroupRequest {
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(0),
                group_id: WireIdentifier::numeric(GROUP_ID),
                client_id: MEMBER_CLIENT_ID,
                session: SESSION,
            }
            .to_bytes(),
        ))
        .unwrap();
        for transition in mux.streams().consumer_group_pending_revocations() {
            mux.update(prepare_message(
                Operation::CompleteConsumerGroupRevocation,
                MEMBER_CLIENT_ID,
                5,
                &CompleteConsumerGroupRevocationRequest {
                    stream_id: WireIdentifier::numeric(0),
                    topic_id: WireIdentifier::numeric(0),
                    partition_id: transition.partition_id,
                    installation: transition.installation,
                    partition_op: 1,
                }
                .to_bytes(),
            ))
            .unwrap();
        }
        let installed = mux
            .streams()
            .poll_metadata(namespace, Some(u64::from(GROUP_ID)), MEMBER_CLIENT_ID)
            .unwrap()
            .context(0);
        let request = request_message(Operation::NonReplicated, MEMBER_CLIENT_ID, SESSION, 1, &[])
            .transmute_header(|old, header: &mut RoutedRequestHeader| {
                *header = old;
                header.partition_incarnation = installed.incarnation;
                header.owner_generation = installed.owner_generation + 1;
            });
        let poll = (
            namespace,
            PARTITION_ID,
            PollingConsumer::ConsumerGroup(GROUP_ID as usize, PARTITION_ID as usize),
            PollingArgs::new(PollingStrategy::offset(0), 1, false),
        );

        let Err(ReadPolledMessagesError::Rejected(refusal)) = read_polled_messages(
            &shard,
            ClientOrigin::capture(&shard.bus, TRANSPORT_CLIENT_ID),
            &request,
            poll,
            &attachment,
            MEMBER_CLIENT_ID,
        )
        .await
        else {
            panic!("a poll that names another owner generation must be refused");
        };
        // `IggyError` equality compares codes only, so match the payload.
        assert!(
            matches!(
                refusal,
                IggyError::ConsumerGroupPartitionNotOwned(MEMBER_CLIENT_ID, PARTITION_ID)
            ),
            "the refusal must name member client {MEMBER_CLIENT_ID}, not group {GROUP_ID}, got {refusal:?}"
        );
    }

    /// A group poll from a client that owns no partition of the group is
    /// refused before it reaches the owner, and the refusal names that client.
    #[test]
    fn given_non_member_when_resolving_group_poll_should_name_the_full_client_id() {
        // Wider than u64, so a refusal that truncated it would name another client.
        const CLIENT_ID: u128 = (1 << 64) + 41;
        const GROUP_ID: u32 = 0;
        const PARTITION_ID: u32 = 0;
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, 0, 1, 1));
        create_one_partition(&shard);
        create_consumer_group(&shard);
        let wire = PollMessagesRequest {
            consumer: WireConsumer::consumer_group(WireIdentifier::numeric(GROUP_ID)),
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            partition_id: Some(PARTITION_ID),
            strategy: WirePollingStrategy::offset(0),
            count: 1,
            auto_commit: false,
        };

        let refusal = resolve_poll_request(&shard, &wire, CLIENT_ID).err();

        assert!(
            matches!(
                refusal,
                Some(IggyError::ConsumerGroupPartitionNotOwned(
                    CLIENT_ID,
                    PARTITION_ID
                ))
            ),
            "{refusal:?}"
        );
    }

    /// An empty body reads as "no stored offset", and a consumer that reads it
    /// resets its position. A read with no reply must deny instead.
    #[compio::test]
    async fn given_pending_offset_read_when_owner_reply_times_out_should_deny_not_accepted() {
        const TRANSPORT_CLIENT_ID: u128 = 91;
        let bus = SpyBus::default();
        bus.instant_timers.set(true);
        let mut shard = test_shard(&bus, 0, 1, 1);
        let (sender, owner_inbox, _owner_replies) = shard_channel(0, 1, 1);
        shard.attach_senders(vec![sender]);
        let shard = Rc::new(shard);
        let namespace = route_one_partition(&shard);
        let offset_body = GetConsumerOffsetRequest {
            consumer: WireConsumer::consumer(WireIdentifier::numeric(1)),
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            partition_id: Some(0),
        }
        .to_bytes();
        let request = with_partition_incarnation(
            &shard,
            namespace,
            request_message(Operation::NonReplicated, 1, 1, 1, &offset_body),
        );
        let attachment = attach_root_session(&shard, 1, 1);

        handle_get_consumer_offset(
            &shard,
            ClientOrigin::capture(&shard.bus, TRANSPORT_CLIENT_ID),
            &request,
            Some(DEFAULT_ROOT_USER_ID),
            Some(&attachment),
        )
        .await;

        let replies = bus.client_replies.borrow();
        assert_eq!(replies.len(), 1);
        let (client_id, frame) = &replies[0];
        assert_eq!(*client_id, TRANSPORT_CLIENT_ID);
        assert_eq!(frame.len(), std::mem::size_of::<ReplyHeader>());
        let status_start = std::mem::offset_of!(ReplyHeader, status);
        let status = u32::from_le_bytes(frame[status_start..status_start + 4].try_into().unwrap());
        assert_eq!(status, IggyError::TransientNotAccepted.as_code());
        assert!(
            owner_inbox.try_recv().is_ok(),
            "the read must have reached its owner before the timeout"
        );
    }

    /// Only the owner's own "no stored offset" may go back as the empty body.
    /// An owner that does not hold the partition says nothing about the offset.
    #[compio::test]
    async fn given_owner_reply_when_reading_offset_should_send_empty_body_only_for_no_offset() {
        const TRANSPORT_CLIENT_ID: u128 = 91;
        for (owner_reply, expected_status) in [
            (
                PartitionReadReply::NotFound,
                IggyError::TransientNotAccepted.as_code(),
            ),
            (
                PartitionReadReply::Rejected(IggyError::NotPrimary),
                IggyError::NotPrimary.as_code(),
            ),
            (
                PartitionReadReply::Rejected(IggyError::HistoryUnavailable),
                IggyError::HistoryUnavailable.as_code(),
            ),
            (
                PartitionReadReply::ConsumerOffset {
                    stored: None,
                    current_offset: 0,
                },
                0,
            ),
        ] {
            let bus = SpyBus::default();
            let mut shard = test_shard(&bus, 0, 1, 1);
            let (sender, owner_inbox, _owner_replies) = shard_channel(0, 1, 1);
            shard.attach_senders(vec![sender]);
            let shard = Rc::new(shard);
            let namespace = route_one_partition(&shard);
            let owner_task = compio::runtime::spawn(async move {
                let ShardFrame::Lifecycle(LifecycleFrame::PartitionRead { read, reply, .. }) =
                    owner_inbox.recv().await.unwrap()
                else {
                    panic!("the read must have reached the owner");
                };
                assert!(matches!(
                    read,
                    PartitionRead::ConsumerOffsetOnPrimary { .. }
                ));
                reply.try_send(owner_reply).unwrap();
            });
            let offset_body = GetConsumerOffsetRequest {
                consumer: WireConsumer::consumer(WireIdentifier::numeric(1)),
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(0),
                partition_id: Some(0),
            }
            .to_bytes();
            let request = with_partition_incarnation(
                &shard,
                namespace,
                request_message(Operation::NonReplicated, 1, 1, 1, &offset_body),
            );
            let attachment = attach_root_session(&shard, 1, 1);

            handle_get_consumer_offset(
                &shard,
                ClientOrigin::capture(&shard.bus, TRANSPORT_CLIENT_ID),
                &request,
                Some(DEFAULT_ROOT_USER_ID),
                Some(&attachment),
            )
            .await;
            owner_task.await.expect("the owner task must finish");

            let replies = bus.client_replies.borrow();
            assert_eq!(replies.len(), 1);
            let (_, frame) = &replies[0];
            assert_eq!(frame.len(), std::mem::size_of::<ReplyHeader>());
            let status_start = std::mem::offset_of!(ReplyHeader, status);
            let status =
                u32::from_le_bytes(frame[status_start..status_start + 4].try_into().unwrap());
            assert_eq!(status, expected_status);
        }
    }

    #[compio::test]
    async fn given_an_offset_read_when_history_is_stale_should_reject_before_owner_dispatch() {
        for consumer in [
            WireConsumer::consumer(WireIdentifier::numeric(1)),
            WireConsumer::consumer_group(WireIdentifier::numeric(999)),
            WireConsumer::external_group(WireIdentifier::numeric(999)),
        ] {
            for (ahead, expected) in [
                (false, IggyError::HistoryUnavailable),
                (true, IggyError::TransientNotAccepted),
            ] {
                let bus = SpyBus::default();
                let mut shard = test_shard(&bus, 0, 1, 1);
                let (sender, owner_inbox, _owner_replies) = shard_channel(0, 1, 1);
                shard.attach_senders(vec![sender]);
                let shard = Rc::new(shard);
                let namespace = route_one_partition(&shard);
                let wire = GetConsumerOffsetRequest {
                    consumer: consumer.clone(),
                    stream_id: WireIdentifier::numeric(0),
                    topic_id: WireIdentifier::numeric(0),
                    partition_id: Some(0),
                };
                let request = with_partition_incarnation(
                    &shard,
                    namespace,
                    request_message(Operation::NonReplicated, 1, 1, 1, &wire.to_bytes()),
                )
                .transmute_header(|old, header: &mut RoutedRequestHeader| {
                    *header = old;
                    if ahead {
                        header.minimum_metadata_op = current_metadata_commit(&shard) + 1;
                    } else {
                        header.partition_incarnation = 0;
                    }
                });
                let attachment = attach_root_session(&shard, 1, 1);

                handle_get_consumer_offset(
                    &shard,
                    ClientOrigin::capture(&shard.bus, 91),
                    &request,
                    Some(DEFAULT_ROOT_USER_ID),
                    Some(&attachment),
                )
                .await;

                let replies = bus.client_replies.borrow();
                assert_eq!(replies.len(), 1);
                let frame = &replies[0].1;
                let status_start = std::mem::offset_of!(ReplyHeader, status);
                let status =
                    u32::from_le_bytes(frame[status_start..status_start + 4].try_into().unwrap());
                assert_eq!(status, expected.as_code(), "consumer {consumer:?}");
                assert!(owner_inbox.try_recv().is_err());
            }
        }
    }

    #[test]
    fn given_a_missing_group_when_resolving_offset_should_still_require_its_partition_primary() {
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, 0, 1, 1));
        let namespace = route_one_partition(&shard);
        let context = shard
            .plane
            .metadata()
            .mux_stm
            .streams()
            .consumer_offset_metadata(namespace, None, 0)
            .unwrap()
            .context(current_metadata_commit(&shard));
        for consumer in [
            WireConsumer::consumer_group(WireIdentifier::numeric(999)),
            WireConsumer::external_group(WireIdentifier::numeric(999)),
        ] {
            let (resolved, partition, read) = resolve_primary_consumer_offset_request(
                &shard,
                &GetConsumerOffsetRequest {
                    consumer,
                    stream_id: WireIdentifier::numeric(0),
                    topic_id: WireIdentifier::numeric(0),
                    partition_id: None,
                },
                context,
            )
            .unwrap();
            assert_eq!(resolved, namespace);
            assert_eq!(partition, 0);
            assert!(matches!(
                read,
                PartitionRead::ConsumerOffsetOnPrimary { consumer: None, .. }
            ));
        }
    }

    #[test]
    fn given_an_offset_read_ahead_of_metadata_when_namespace_is_missing_should_report_not_accepted()
    {
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, 0, 1, 1));
        route_one_partition(&shard);
        let local_metadata_op = current_metadata_commit(&shard);
        for (stream_id, topic_id, partition_id, expected) in [
            (
                1,
                0,
                0,
                IggyError::StreamIdNotFound(Identifier::numeric(1).unwrap()),
            ),
            (
                0,
                1,
                0,
                IggyError::TopicIdNotFound(
                    Identifier::numeric(1).unwrap(),
                    Identifier::numeric(0).unwrap(),
                ),
            ),
            (
                0,
                0,
                1,
                IggyError::PartitionNotFound(
                    1,
                    Identifier::numeric(0).unwrap(),
                    Identifier::numeric(0).unwrap(),
                ),
            ),
        ] {
            let wire = GetConsumerOffsetRequest {
                consumer: WireConsumer::consumer(WireIdentifier::numeric(1)),
                stream_id: WireIdentifier::numeric(stream_id),
                topic_id: WireIdentifier::numeric(topic_id),
                partition_id: Some(partition_id),
            };
            for (metadata_op, expected) in [
                (local_metadata_op + 1, IggyError::TransientNotAccepted),
                (local_metadata_op, expected),
            ] {
                let result = resolve_primary_consumer_offset_request(
                    &shard,
                    &wire,
                    PartitionContext {
                        metadata_op,
                        ..PartitionContext::default()
                    },
                );
                assert_eq!(
                    result.err(),
                    Some(expected),
                    "stream={stream_id}, topic={topic_id}, partition={partition_id}, metadata_op={metadata_op}"
                );
            }
        }
    }

    /// [`create_one_partition`] with the partition routed to shard 0, so
    /// authorization and resolution let a read reach the owner inbox.
    fn route_one_partition(shard: &TestShard) -> IggyNamespace {
        let namespace = create_one_partition(shard);
        shard
            .shards_table()
            .insert(namespace, PartitionLocation::new(ShardId::new(0), 0));
        namespace
    }

    /// `request` as a client that has seen the partition create sends it: it
    /// names the incarnation of `namespace`.
    fn with_partition_incarnation(
        shard: &TestShard,
        namespace: IggyNamespace,
        request: Message<RoutedRequestHeader>,
    ) -> Message<RoutedRequestHeader> {
        let incarnation = shard
            .plane
            .metadata()
            .mux_stm
            .streams()
            .created_revision_for_namespace(namespace)
            .unwrap();
        request.transmute_header(|old, header: &mut RoutedRequestHeader| {
            *header = old;
            header.partition_incarnation = incarnation;
        })
    }

    /// Registers `client_id` as the root user and attaches `session`, the
    /// state of a bound TCP connection.
    fn attach_root_session(shard: &TestShard, client_id: u128, session: u64) -> SessionAttachment {
        let mut table = shard.plane.metadata().client_table.borrow_mut();
        table
            .commit_register(
                client_id,
                DEFAULT_ROOT_USER_ID,
                [0x5a; 32],
                register_reply(client_id, session),
            )
            .unwrap();
        table
            .attach_session(client_id, session, DEFAULT_ROOT_USER_ID)
            .unwrap()
    }

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
                initial_view: 0,
                partition_id: 0,
                consensus_group_id: 1,
            }],
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
            ClientOrigin::capture(&shard.bus, TRANSPORT),
            Some(DEFAULT_ROOT_USER_ID),
            None,
            None,
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

    #[compio::test]
    async fn tombstoned_partition_send_without_owner_must_not_reach_client() {
        const TRANSPORT: u128 = 91;
        const SESSION: u64 = 1;

        let bus = SpyBus::default();
        let metadata = IggyMetadata::new(None, None, None, None, TestMux::default(), None);
        let partitions = IggyPartitions::new(
            ShardId::new(0),
            PartitionsConfig {
                messages_required_to_save: 1,
                size_of_messages_required_to_save: iggy_common::IggyByteSize::from(1024_u64),
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

        assert!(
            bus.client_replies.borrow().is_empty(),
            "a consensus client id cannot identify this request's reply owner"
        );
    }

    #[compio::test]
    async fn unroutable_operation_without_owner_must_not_reach_client() {
        const TRANSPORT: u128 = 91;
        const SESSION: u64 = 1;

        let bus = SpyBus::default();
        let metadata = IggyMetadata::new(None, None, None, None, TestMux::default(), None);
        let partitions = IggyPartitions::new(
            ShardId::new(0),
            PartitionsConfig {
                messages_required_to_save: 1,
                size_of_messages_required_to_save: iggy_common::IggyByteSize::from(1024_u64),
                validate_checksum: true,
                segment_size: iggy_common::IggyByteSize::from(1_048_576_u64),
                preallocate_segments: false,
                encryptor: None,
                path_layout: PartitionPathLayout::default(),
            },
        );
        let shard = Rc::new(TestShard::without_inbox(
            ShardIdentity::new(0, "unroutable-operation-test".to_string()),
            bus.clone(),
            metadata,
            partitions,
            PapayaShardsTable::new(),
            PartitionConsensusConfig::new(1, ReplicaTopology::new(0, 1), bus.clone()),
        ));

        let request = request_message(Operation::DeleteSegments, TRANSPORT, SESSION, 1, &[]);
        shard.on_message(MessageBag::Request(request)).await;

        assert!(
            bus.client_replies.borrow().is_empty(),
            "a consensus client id cannot identify this request's reply owner"
        );
    }

    /// The replicated halves of the same hole. A prepare or an ack whose
    /// operation no plane claims reaches the same terminator, and there is no
    /// client to answer, so the counter and the log are the only record. Dropping
    /// it is the only option: nothing journals or acks an operation no plane
    /// owns.
    #[compio::test]
    async fn unroutable_replicated_frames_must_be_dropped_and_counted() {
        const TRANSPORT: u128 = 91;

        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, 0, 1, 1));
        let unroutable_drops = || {
            shard
                .metrics()
                .frame_drop_count(frame_drop_variant::CONSENSUS, frame_drop_reason::UNROUTABLE)
        };

        let prepare = prepare_message(Operation::DeleteSegments, TRANSPORT, 1, &[]);
        shard.on_message(MessageBag::Prepare(prepare)).await;
        assert_eq!(
            unroutable_drops(),
            1,
            "a prepare no plane claims must be counted, not absorbed by the chain"
        );

        shard
            .on_message(MessageBag::PrepareOk(prepare_ok_message(
                Operation::DeleteSegments,
                1,
            )))
            .await;
        assert_eq!(
            unroutable_drops(),
            2,
            "an ack no plane claims must be counted, not absorbed by the chain"
        );

        assert!(
            bus.client_replies.borrow().is_empty(),
            "replicated frames answer nobody on this node"
        );
    }

    /// Bare `PrepareOk` for the routing guard: only `operation`, `group` and `op`
    /// are read before the plane chain, so the rest stays zeroed.
    fn prepare_ok_message(operation: Operation, op: u64) -> Message<PrepareOkHeader> {
        let header_size = size_of::<PrepareOkHeader>();
        let mut msg = Message::<PrepareOkHeader>::new(header_size);
        let header = bytemuck::checked::try_from_bytes_mut::<PrepareOkHeader>(
            &mut msg.as_mut_slice()[..header_size],
        )
        .expect("zeroed bytes form a valid PrepareOkHeader");
        header.command = Command::PrepareOk;
        header.size = u32::try_from(header_size).expect("ack size fits u32");
        header.operation = operation;
        header.op = op;
        header.group = server_common::sharding::METADATA_GROUP;
        msg
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
        // This test does not dispatch disk polls, so keep that lane minimal.
        const POLL_COMPLETION_CAPACITY: usize = 1;

        let bus = SpyBus::default();
        let metadata = IggyMetadata::new(None, None, None, None, TestMux::default(), None);
        let partitions = IggyPartitions::new(
            ShardId::new(0),
            PartitionsConfig {
                messages_required_to_save: 1,
                size_of_messages_required_to_save: iggy_common::IggyByteSize::from(1024_u64),
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
            Rc::new(NoopHost),
            metadata,
            partitions,
            vec![sender],
            inbox_rx,
            reply_inbox_rx,
            POLL_COMPLETION_CAPACITY,
            None,
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

    async fn answer_segment_delete_primary(inbox: &Receiver<ShardFrame>, primary: u8) {
        let ShardFrame::Lifecycle(LifecycleFrame::PartitionRead {
            read: PartitionRead::Primary,
            reply,
            ..
        }) = inbox.recv().await.unwrap()
        else {
            panic!("segment resolution must locate the partition primary");
        };
        reply
            .try_send(PartitionReadReply::Primary { view: 0, primary })
            .unwrap();
    }

    fn complete_segment_delete_lifecycle(shard: &TestShard) {
        let metadata = shard.plane.metadata();
        let intents = metadata.mux_stm.streams().pending_lifecycles();
        assert_eq!(intents.len(), 1);
        let intent = &intents[0];
        assert_eq!(intent.partitions.len(), 1);
        let partition = &intent.partitions[0];
        let reply = metadata
            .mux_stm
            .update(prepare_message(
                Operation::CompleteLifecycle,
                1,
                1,
                &CompleteLifecycleRequest::new(
                    intent.context.metadata_op,
                    intent.stream_id,
                    vec![LifecycleCompletion {
                        topic_id: partition.topic_id,
                        partition_id: partition.partition_id,
                        partition_op: 1,
                    }],
                )
                .unwrap()
                .to_bytes(),
            ))
            .unwrap();
        assert_eq!(reply.code, 0);
        assert!(!metadata.mux_stm.streams().has_pending_lifecycles());
    }

    fn create_segment_delete_topic(shard: &TestShard) -> (IggyNamespace, u64) {
        let metadata = shard.plane.metadata();
        metadata.mux_stm.users().ensure_root_user("iggy", "hash");
        for prepare in [
            prepare_message(
                Operation::CreateStream,
                1,
                1,
                &CreateStreamRequest {
                    name: WireName::new("stream").unwrap(),
                    options: WireOptions::empty(),
                }
                .to_bytes(),
            ),
            create_topic_prepare(),
        ] {
            assert_eq!(metadata.mux_stm.update(prepare).unwrap().code, 0);
        }
        let namespace = IggyNamespace::new(0, 0, 0);
        shard
            .shards_table()
            .insert(namespace, PartitionLocation::new(ShardId::new(0), 0));
        let created_revision = metadata
            .mux_stm
            .streams()
            .created_revision_for_namespace(namespace)
            .unwrap();
        (namespace, created_revision)
    }

    /// Applied again after a topic delete, it re-creates partition 0 under a
    /// new `created_revision`.
    fn create_topic_prepare() -> Message<PrepareHeader> {
        prepare_message(
            Operation::CreateTopicWithAssignments,
            1,
            2,
            &CreateTopicWithAssignmentsRequest {
                request: CreateTopicRequest {
                    stream_id: WireIdentifier::numeric(0),
                    partitions_count: 1,
                    name: WireName::new("topic").unwrap(),
                    options: WireOptions::empty(),
                },
                derived_options: WireOptions::empty(),
                partitions: vec![CreatedPartitionAssignment {
                    initial_view: 0,
                    partition_id: 0,
                    consensus_group_id: 1,
                }],
            }
            .to_bytes(),
        )
    }

    /// Attempt an already resolved poll of an attached root session and
    /// require an error, including when no owner reply arrives. Bypassing the
    /// request resolve isolates owner routing and reply handling from
    /// authentication and request decoding. The poll names the incarnation of
    /// `namespace`, so the poll metadata gate and the context fence pass for
    /// an existing partition.
    async fn poll_and_expect_error(shard: &Rc<TestShard>, namespace: IggyNamespace) -> IggyError {
        let vsr_client_id = 1;
        let session = 1;
        let transport_client_id = 91;
        let consumer_id = 1;
        let partition_id = 0;
        let attachment = attach_root_session(shard, vsr_client_id, session);
        let request = with_partition_incarnation(
            shard,
            namespace,
            request_message(Operation::NonReplicated, vsr_client_id, session, 1, &[]),
        );
        let resolved_poll = (
            namespace,
            partition_id,
            PollingConsumer::Consumer(consumer_id, usize::try_from(partition_id).unwrap()),
            PollingArgs::new(PollingStrategy::offset(0), 1, true),
        );
        match read_polled_messages(
            shard,
            ClientOrigin::capture(&shard.bus, transport_client_id),
            &request,
            resolved_poll,
            &attachment,
            transport_client_id,
        )
        .await
        {
            Err(ReadPolledMessagesError::Rejected(error)) => error,
            Err(ReadPolledMessagesError::Fallback(_)) => {
                panic!("poll must not return an empty reply")
            }
            Ok(_) => panic!("poll must not succeed without an owner reply"),
        }
    }

    /// The control of a routing refusal: the same poll reaches the owner
    /// inbox once routing can deliver it, so the refusal came from routing
    /// and not from a gate before it. Nothing answers in the inbox, so the
    /// bus expires the reply timeout at once.
    async fn assert_poll_reaches_owner(
        shard: &Rc<TestShard>,
        namespace: IggyNamespace,
        owner_inbox: &Receiver<ShardFrame>,
    ) {
        shard.bus.instant_timers.set(true);
        assert_eq!(
            poll_and_expect_error(shard, namespace).await,
            IggyError::ShardCommunicationError
        );
        assert!(
            matches!(
                owner_inbox.try_recv(),
                Ok(ShardFrame::Lifecycle(LifecycleFrame::PartitionRead {
                    read: PartitionRead::PollOnPrimary { .. },
                    ..
                }))
            ),
            "the poll must reach the owner once routing can deliver it"
        );
    }

    type BusShard =
        IggyShard<Rc<IggyMessageBus>, PrepareJournal, IggySnapshot, TestMux, PapayaShardsTable>;

    const RELAY_SESSION: u64 = 1;
    const RELAY_REQUEST: u64 = 1;
    const RELAY_POLL_INTERVAL: Duration = Duration::from_millis(1);
    const RELAY_BUDGET: Duration = Duration::from_secs(5);

    /// The owning shard commits a write and its reply returns to the shard
    /// holding the socket. A full reply queue makes the reply wait for the
    /// writer instead of closing the connection, and the write keeps its
    /// place under the in-flight cap until its reply is queued.
    #[compio::test]
    async fn given_full_reply_queue_when_owner_shard_replies_should_wait_for_the_writer() {
        const CLIENT: u128 = 0x41;
        let (bus, shard, owner_inbox) = origin_shard_over_bus("relay-full-queue-test");
        let (queue, conn_token) = install_stalled_client(&bus, CLIENT);
        bus.send_to_client_instance(
            CLIENT,
            bus.client_instance(CLIENT),
            committed_reply(CLIENT).into_frozen().into(),
        )
        .expect("the first reply fits the queue");

        let drain = relay_write(&shard, ClientOrigin::capture(&shard.bus, CLIENT).queued()).await;
        answer_submit(&owner_inbox);
        // A reply that waits for room holds a sender of the queue.
        let deadline = Instant::now() + RELAY_BUDGET;
        while queue.sender_count() == 1 && !conn_token.is_triggered() {
            assert!(
                Instant::now() < deadline,
                "the relay must take the committed reply"
            );
            compio::time::sleep(RELAY_POLL_INTERVAL).await;
        }
        assert!(
            !conn_token.is_triggered(),
            "a full queue must not close the connection a queued request came on"
        );
        assert_eq!(
            drain.in_flight.get(),
            1,
            "the write keeps its permit while its reply waits"
        );

        queue
            .recv()
            .await
            .expect("the writer takes the first reply");
        wait_for_relay(&drain).await;
        assert_eq!(queue.len(), 1, "the waiting reply takes the freed slot");
        assert!(!conn_token.is_triggered());
    }

    #[compio::test]
    async fn given_waiting_reply_when_connection_disconnects_should_release_its_permit() {
        const CLIENT: u128 = 0x46;
        let (bus, shard, owner_inbox) = origin_shard_over_bus("waiting-reply-disconnect");
        let (queue, conn_token) = install_stalled_client(&bus, CLIENT);
        bus.send_to_client_instance(
            CLIENT,
            bus.client_instance(CLIENT),
            committed_reply(CLIENT).into_frozen().into(),
        )
        .unwrap();
        let drain = relay_write(&shard, ClientOrigin::capture(&shard.bus, CLIENT).queued()).await;
        answer_submit(&owner_inbox);
        let deadline = Instant::now() + RELAY_BUDGET;
        while queue.sender_count() == 1 {
            assert!(Instant::now() < deadline);
            compio::time::sleep(RELAY_POLL_INTERVAL).await;
        }
        assert_eq!(drain.in_flight.get(), 1);
        assert!(!conn_token.is_triggered());
        bus.clients()
            .close_peer_if_token_matches(
                CLIENT,
                bus.client_instance(CLIENT).unwrap(),
                std::time::Duration::ZERO,
            )
            .await;
        wait_for_relay(&drain).await;
        assert_eq!(drain.in_flight.get(), 0);
        assert!(queue.is_closed());
    }

    /// A reply that outlived its connection answers a request the connection
    /// reusing the id never sent: it must neither reach that connection nor
    /// close it when its queue is full.
    #[compio::test]
    async fn given_reused_connection_id_when_owner_shard_replies_should_not_reach_new_connection() {
        const CLIENT: u128 = 0x43;
        let (bus, shard, owner_inbox) = origin_shard_over_bus("relay-reused-id-test");
        let _replaced = install_stalled_client(&bus, CLIENT);
        let drain = relay_write(&shard, ClientOrigin::capture(&shard.bus, CLIENT).queued()).await;
        assert!(bus.clients().remove(CLIENT));
        let (reused_queue, reused_token) = install_stalled_client(&bus, CLIENT);
        bus.send_to_client_instance(
            CLIENT,
            bus.client_instance(CLIENT),
            committed_reply(CLIENT).into_frozen().into(),
        )
        .expect("the reused connection takes its own reply");

        answer_submit(&owner_inbox);
        wait_for_relay(&drain).await;

        assert!(
            !reused_token.is_triggered(),
            "a stale reply must not close the connection that reuses the id"
        );
        assert_eq!(
            reused_queue.len(),
            1,
            "a stale reply must not reach the connection that reuses the id"
        );
    }

    /// HTTP has no socket: it waits on a reply slot of its in-process
    /// installation, so the relay must reach that slot through the
    /// installation it captured, now that a reply by id reaches no socket.
    #[compio::test]
    async fn given_in_process_session_when_owner_shard_replies_should_fill_its_reply_slot() {
        const CLIENT: u128 = 0x44;
        let (bus, shard, owner_inbox) = origin_shard_over_bus("relay-in-process-test");
        bus.clients()
            .insert_in_process(CLIENT)
            .expect("the client id is free");
        let (_slot, mut reply) = bus
            .clients()
            .install_reply_slot(CLIENT, RELAY_REQUEST)
            .expect("the reply slot installs");

        let drain = relay_write(&shard, ClientOrigin::capture(&shard.bus, CLIENT)).await;
        answer_submit(&owner_inbox);
        wait_for_relay(&drain).await;

        assert!(
            matches!(reply.try_recv(), Ok(Some(_))),
            "the committed reply must fill the session's reply slot"
        );
    }

    /// A client can have more requests outstanding than its reply queue
    /// holds, and their replies can land in one burst before its writer runs.
    /// A client that takes each reply as it comes must keep its connection.
    #[compio::test]
    async fn given_more_requests_than_the_reply_queue_holds_when_the_client_reads_promptly_should_keep_the_connection()
     {
        const CLIENT: u128 = 0x45;
        const REQUESTS: usize = 8;
        let (bus, shard, _owner_inbox) = origin_shard_over_bus("prompt-reader-test");
        // The test takes the replies off the queue in place of the writer.
        let (queue, conn_token) = install_stalled_client(&bus, CLIENT);
        let shard_handle: ShellShardHandle<Rc<IggyMessageBus>, PrepareJournal, IggySnapshot> =
            Rc::new(RefCell::new(Some(Rc::downgrade(&shard))));
        let sessions = Rc::new(RefCell::new(SessionManager::new()));
        let server_config = Arc::new(ServerConfig::default());
        let queues = ClientRequestQueues::default();
        let active = ActiveClientRequests::default();
        for _ in 0..REQUESTS {
            enqueue_client_request(
                &bus,
                &shard_handle,
                &sessions,
                &server_config,
                &queues,
                &active,
                CLIENT,
                non_replicated_request(CLIENT, PING_CODE),
            );
        }

        for _ in 0..REQUESTS {
            compio::time::timeout(RELAY_BUDGET, queue.recv())
                .await
                .expect("every request must be answered")
                .expect("the connection must stay open");
        }
        assert!(
            !conn_token.is_triggered(),
            "a client that reads its replies must keep its connection"
        );
    }

    /// Shard 0 of a two-shard node over a real bus, so replies reach the
    /// connection registry. Shard 1 owns the partition: the test holds its
    /// inbox and answers its submits the way the owning plane does.
    fn origin_shard_over_bus(
        name: &str,
    ) -> (Rc<IggyMessageBus>, Rc<BusShard>, Receiver<ShardFrame>) {
        const POLL_COMPLETION_CAPACITY: usize = 1;
        const INBOX_CAPACITY: usize = 4;
        const OWNER_SHARD: u16 = 1;
        let bus = Rc::new(IggyMessageBus::new(0));
        let partitions = IggyPartitions::new(
            ShardId::new(0),
            PartitionsConfig {
                messages_required_to_save: 1,
                size_of_messages_required_to_save: iggy_common::IggyByteSize::from(1024_u64),
                validate_checksum: true,
                segment_size: iggy_common::IggyByteSize::from(1_048_576_u64),
                preallocate_segments: false,
                encryptor: None,
                path_layout: PartitionPathLayout::default(),
            },
        );
        let (origin_sender, origin_inbox, origin_replies) =
            shard_channel(0, INBOX_CAPACITY, INBOX_CAPACITY);
        let (owner_sender, owner_inbox, _owner_replies) =
            shard_channel(OWNER_SHARD, INBOX_CAPACITY, INBOX_CAPACITY);
        let shard = BusShard::new(
            ShardIdentity::new(0, name.to_string()),
            Rc::clone(&bus),
            Rc::new(NoopHost),
            IggyMetadata::new(None, None, None, None, TestMux::default(), None),
            partitions,
            vec![origin_sender, owner_sender],
            origin_inbox,
            origin_replies,
            POLL_COMPLETION_CAPACITY,
            None,
            PapayaShardsTable::new(),
            PartitionConsensusConfig::new(1, ReplicaTopology::new(0, 1), Rc::clone(&bus)),
            None,
            ShardMetrics::for_shard(),
        )
        .expect("the two-sender ring is canonically ordered");
        shard.shards_table().insert(
            relay_namespace(),
            PartitionLocation::new(ShardId::new(OWNER_SHARD), 0),
        );
        (bus, Rc::new(shard), owner_inbox)
    }

    fn relay_namespace() -> IggyNamespace {
        IggyNamespace::new(0, 0, 0)
    }

    /// A socket connection whose writer stopped draining, as when its client
    /// stopped reading: its queue holds one reply. Returns the queue and the
    /// connection's shutdown token.
    fn install_stalled_client(
        bus: &IggyMessageBus,
        client_id: u128,
    ) -> (BusReceiver, ShutdownToken) {
        const STALLED_QUEUE_CAPACITY: usize = 1;
        let (sender, queue) = async_channel::bounded(STALLED_QUEUE_CAPACITY);
        let (conn_shutdown, conn_token) = Shutdown::new();
        let writer_token = conn_token.clone();
        let writer = compio::runtime::spawn(async move { writer_token.wait().await });
        let reader_token = conn_token.clone();
        let reader = compio::runtime::spawn(async move { reader_token.wait().await });
        bus.clients()
            .insert(client_id, sender, writer, reader, conn_shutdown)
            .expect("the client id is free");
        (queue, conn_token)
    }

    /// Relay a write from `origin` to the owning shard. The returned drain
    /// counts the relay task until it has answered the client.
    async fn relay_write(shard: &Rc<BusShard>, origin: ClientOrigin) -> Rc<ClientDrain> {
        let namespace = relay_namespace();
        let request = request_message(
            Operation::SendMessages,
            origin.id,
            RELAY_SESSION,
            RELAY_REQUEST,
            &[],
        )
        .transmute_header(|header, routed: &mut RoutedRequestHeader| {
            *routed = header;
            routed.group = namespace.inner();
        });
        let header = *request.header();
        let drain = Rc::new(ClientDrain::new(&ServerConfig::default()));
        let in_flight = drain.admit(Admission::Cap).await;
        let admission = relay_partition_reply(
            shard,
            namespace,
            request,
            origin,
            &header,
            None,
            Some(in_flight),
        )
        .await;
        assert_eq!(admission, PartitionAdmission::Admitted);
        drain
    }

    /// Commit the write the owning shard received: its reply rides the
    /// submit's own channel back to the shard holding the socket.
    fn answer_submit(owner_inbox: &Receiver<ShardFrame>) {
        let Ok(ShardFrame::Lifecycle(LifecycleFrame::PartitionSubmit { request, reply, .. })) =
            owner_inbox.try_recv()
        else {
            panic!("the write must reach the owning shard as a submit");
        };
        let client = request.header().client;
        assert!(
            reply
                .try_send(Some(committed_reply_to(request.header(), client)))
                .is_ok(),
            "the relay waits for the committed reply"
        );
    }

    fn committed_reply(client: u128) -> Message<GenericHeader> {
        let request = request_message(
            Operation::SendMessages,
            client,
            RELAY_SESSION,
            RELAY_REQUEST,
            &[],
        );
        committed_reply_to(request.header(), client)
    }

    fn committed_reply_to(header: &RoutedRequestHeader, client: u128) -> Message<GenericHeader> {
        const COMMITTED: u32 = 0;
        build_deny_reply(header, client, 0, 0, COMMITTED).into_generic()
    }

    async fn wait_for_relay(drain: &ClientDrain) {
        let deadline = Instant::now() + RELAY_BUDGET;
        while drain.in_flight.get() > 0 {
            assert!(
                Instant::now() < deadline,
                "the relay must answer once the owning shard replies"
            );
            compio::time::sleep(RELAY_POLL_INTERVAL).await;
        }
    }
}
