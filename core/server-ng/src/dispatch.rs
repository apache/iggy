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

//! Per-shard request dispatch.
//!
//! Client-request queue plumbing, the transport / replica /
//! metadata-submit handler factories, the owner-forwarding helpers that
//! run consensus on shard 0, and the login/register, logout, and
//! non-replicated request handlers.

use crate::auth::{
    complete_login_register, send_login_failure_reply, surface_login_failure,
    verify_login_credentials, verify_pat_credentials,
};
use crate::bootstrap::{ServerNgShard, ServerNgShardHandle};
use crate::consumer_group::{
    maybe_rewrite_consumer_group_request, maybe_rewrite_consumer_offset_request,
};
use crate::login_register::LoginRegisterError;
use crate::pat::maybe_rewrite_pat_request;
use crate::responses::{
    NonReplicatedResponse, build_consumer_offset_body, build_empty_reply, build_get_me_response,
    build_get_personal_access_tokens_response, build_non_replicated_response,
    build_polled_messages_body, build_raw_pat_reply, connected_client_to_response,
    current_metadata_commit, resolve_partition_namespace, resolve_partition_request_namespace,
};
use crate::session_manager::SessionManager;
use crate::users::maybe_rewrite_user_password_request;
use crate::wire::request_body;
use bytes::Bytes;
use consensus::{
    EvictionContext, MetadataHandle, PartitionsHandle, build_eviction_message,
    build_incompatible_protocol_eviction_message,
};
use iggy_binary_protocol::codes::{
    GET_CLIENT_CODE, GET_CLIENTS_CODE, GET_CLUSTER_METADATA_CODE, GET_CONSUMER_OFFSET_CODE,
    GET_ME_CODE, GET_PERSONAL_ACCESS_TOKENS_CODE, PING_CODE, POLL_MESSAGES_CODE,
    SYNC_CONSUMER_GROUP_CODE,
};
use iggy_binary_protocol::primitives::consumer::WireConsumer;
use iggy_binary_protocol::primitives::polling_strategy::WirePollingStrategy;
use iggy_binary_protocol::requests::consumer_groups::SyncConsumerGroupRequest;
use iggy_binary_protocol::requests::consumer_offsets::GetConsumerOffsetRequest;
use iggy_binary_protocol::requests::messages::PollMessagesRequest;
use iggy_binary_protocol::requests::segments::DeleteSegmentsRequest;
use iggy_binary_protocol::requests::system::get_client::GetClientRequest;
use iggy_binary_protocol::requests::users::{LoginRegisterRequest, LoginRegisterWithPatRequest};
use iggy_binary_protocol::responses::clients::client_response::ConsumerGroupInfoResponse;
use iggy_binary_protocol::responses::clients::get_client::ClientDetailsResponse;
use iggy_binary_protocol::responses::clients::get_clients::GetClientsResponse;
use iggy_binary_protocol::responses::consumer_groups::SyncConsumerGroupResponse;
use iggy_binary_protocol::{
    ClientVersionInfo, EvictionReason, GenericHeader, KIND_CONSUMER_GROUP, Operation,
    ProtocolVersion, RequestHeader, WireDecode, WireEncode, is_protocol_compatible,
};
use iggy_common::{IggyError, PollingStrategy};
use message_bus::client_listener::RequestHandler;
use message_bus::replica::listener::MessageHandler;
use message_bus::{IggyMessageBus, MessageBus};
use metadata::impls::metadata::{
    MetadataSubmitError, StreamsFrontend, build_truncate_partition_client_message,
};
use partitions::{PollPlan, PollingArgs, PollingConsumer};
use secrecy::ExposeSecret;
use server_common::Message;
use server_common::sharding::IggyNamespace;
use shard::shards_table::ShardsTable;
use shard::{
    ConnectedClientInfo, ListClientsHandler, PartitionRead, PartitionReadHandler,
    PartitionReadReply,
};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::rc::Rc;
use tracing::warn;

pub(crate) type ClientRequestQueues = Rc<RefCell<HashMap<u128, VecDeque<Message<GenericHeader>>>>>;
pub(crate) type ActiveClientRequests = Rc<RefCell<HashSet<u128>>>;

pub(crate) fn make_client_request_handler(
    shard: &Rc<ServerNgShard>,
    sessions: &Rc<RefCell<SessionManager>>,
) -> RequestHandler {
    let shard = Rc::clone(shard);
    let sessions = Rc::clone(sessions);
    let queues: ClientRequestQueues = Rc::new(RefCell::new(HashMap::new()));
    let active: ActiveClientRequests = Rc::new(RefCell::new(HashSet::new()));
    let sessions_for_disconnect = Rc::clone(&sessions);
    let shard_for_disconnect = Rc::clone(&shard);
    shard
        .bus
        .set_client_connection_lost_fn(Rc::new(move |client_id| {
            if let Some((vsr_client_id, session)) = sessions_for_disconnect
                .borrow_mut()
                .remove_connection(client_id)
            {
                submit_disconnect_logout(Rc::clone(&shard_for_disconnect), vsr_client_id, session);
            }
        }));
    Rc::new(move |client_id, message| {
        enqueue_client_request(
            Rc::clone(&shard),
            Rc::clone(&sessions),
            Rc::clone(&queues),
            Rc::clone(&active),
            client_id,
            message,
        );
    })
}

/// Build the per-shard [`ListClientsHandler`]: on a `ListClients`
/// broadcast, serialize this shard's locally-homed connected clients from
/// its `SessionManager` and push them back over the reply sender. The
/// aggregation across all shards happens in
/// [`shard::IggyShard::list_all_clients`].
pub(crate) fn make_list_clients_handler(
    sessions: &Rc<RefCell<SessionManager>>,
) -> ListClientsHandler {
    let sessions = Rc::clone(sessions);
    Rc::new(move |reply| {
        let clients: Vec<ConnectedClientInfo> = sessions.borrow().iter_clients().collect();
        // Best-effort: the gather side bounds itself by count + timeout, so
        // a dropped reply (receiver gone) just means this shard is omitted.
        let _ = reply.try_send(clients);
    })
}

/// Build the per-shard [`PartitionReadHandler`]: on a `PartitionRead` frame
/// (this shard owns the namespace), run the poll / consumer-offset lookup
/// against the local partitions plane and push the result back over the
/// carried reply sender. The requesting shard bounds the wait with a
/// timeout, so a dropped reply degrades to a client-visible read failure.
pub(crate) fn make_partition_read_handler(
    shard_handle: &ServerNgShardHandle,
) -> PartitionReadHandler {
    let shard_handle = Rc::clone(shard_handle);
    // Runs synchronously on the shard pump (see `process_lifecycle` ->
    // `on_partition_read`). `build_poll_snapshot` takes the partition borrow via
    // `with_partition` (closure-scoped, debug `BorrowGuard`) and returns an owned
    // `PollPlan`; only owned data crosses into `spawn_poll_io`. A fully-resident
    // poll replies here without spawning. See the `poll_plan` module docs.
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
                        spawn_poll_io(namespace, plan, reply);
                    }
                    Some(plan) => {
                        let (fragments, current_offset) = plan.execute_resident();
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
                    .nth_oldest_sealed_end_offset(&namespace, count)
                    .map_or_else(
                        || PartitionReadReply::NotFound,
                        |up_to_offset| PartitionReadReply::SegmentDeleteOffset { up_to_offset },
                    );
                let _ = reply.try_send(result);
            }
        }
    })
}

/// Spawn the off-pump leg of a partition poll: disk read + auto-commit
/// persist/apply on the OWNED plan (disk descriptors, resident-tail `Frozen`
/// clones, `Arc` offset map), then send the reply. Holds no partition
/// reference, so it is sound concurrently with the pump's `&mut` writes.
fn spawn_poll_io(
    namespace: IggyNamespace,
    plan: PollPlan,
    reply: shard::Sender<PartitionReadReply>,
) {
    compio::runtime::spawn(async move {
        let poll_started = std::time::Instant::now();
        let (fragments, current_offset) = plan.execute().await;
        let elapsed = poll_started.elapsed();
        if elapsed > std::time::Duration::from_secs(1) {
            warn!(
                namespace_raw = namespace.inner(),
                elapsed_ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
                "slow partition poll; gather side may have timed out"
            );
        }
        let _ = reply.try_send(PartitionReadReply::Poll {
            fragments,
            current_offset,
        });
    })
    .detach();
}

pub(crate) fn make_deferred_replica_message_handler(
    shard_handle: &ServerNgShardHandle,
) -> MessageHandler {
    let shard_handle = Rc::clone(shard_handle);
    Rc::new(move |_replica_id, message| {
        if let Some(shard) = upgrade_shard_handle(&shard_handle) {
            shard.dispatch(message);
        }
    })
}

pub(crate) fn make_deferred_client_request_handler(
    bus: &Rc<IggyMessageBus>,
    shard_handle: &ServerNgShardHandle,
    sessions: &Rc<RefCell<SessionManager>>,
) -> RequestHandler {
    let shard_handle = Rc::clone(shard_handle);
    let sessions = Rc::clone(sessions);
    let queues: ClientRequestQueues = Rc::new(RefCell::new(HashMap::new()));
    let active: ActiveClientRequests = Rc::new(RefCell::new(HashSet::new()));
    let sessions_for_disconnect = Rc::clone(&sessions);
    let shard_handle_for_disconnect = Rc::clone(&shard_handle);
    bus.set_client_connection_lost_fn(Rc::new(move |client_id| {
        if let Some((vsr_client_id, session)) = sessions_for_disconnect
            .borrow_mut()
            .remove_connection(client_id)
            && let Some(shard) = upgrade_shard_handle(&shard_handle_for_disconnect)
        {
            submit_disconnect_logout(shard, vsr_client_id, session);
        }
    }));
    Rc::new(move |client_id, message| {
        let shard_handle = Rc::clone(&shard_handle);
        let sessions = Rc::clone(&sessions);
        let queues = Rc::clone(&queues);
        let active = Rc::clone(&active);
        queues
            .borrow_mut()
            .entry(client_id)
            .or_default()
            .push_back(message);
        if !active.borrow_mut().insert(client_id) {
            return;
        }
        compio::runtime::spawn(async move {
            let Some(shard) = upgrade_shard_handle(&shard_handle) else {
                active.borrow_mut().remove(&client_id);
                return;
            };
            drain_client_requests(shard, sessions, queues, active, client_id).await;
        })
        .detach();
    })
}

/// Handler shard 0 runs for an inbound [`shard::MetadataSubmit`]: a peer
/// shard has verified credentials and owns the session locally, and asks
/// shard 0 (the metadata consensus owner) to run only the consensus
/// proposal. Spawns a task so the awaiting peer is woken once the op
/// commits; replies `None` on transient submit failure so the peer never
/// blocks forever.
pub(crate) fn make_metadata_submit_handler(
    shard_handle: &ServerNgShardHandle,
) -> shard::MetadataSubmitHandler {
    let shard_handle = Rc::clone(shard_handle);
    Rc::new(move |submit| {
        let shard_handle = Rc::clone(&shard_handle);
        compio::runtime::spawn(async move {
            let Some(shard) = upgrade_shard_handle(&shard_handle) else {
                return;
            };
            match submit {
                shard::MetadataSubmit::Register {
                    vsr_client_id,
                    reply,
                } => {
                    let session = shard
                        .plane
                        .metadata()
                        .submit_register_in_process(vsr_client_id)
                        .await
                        .ok();
                    let _ = reply.try_send(session);
                }
                shard::MetadataSubmit::Logout {
                    vsr_client_id,
                    session,
                    request,
                    reply,
                } => {
                    let commit = shard
                        .plane
                        .metadata()
                        .submit_logout_in_process(vsr_client_id, session, request)
                        .await
                        .ok();
                    let _ = reply.try_send(commit);
                }
                shard::MetadataSubmit::ClientRequest { request, reply } => {
                    let committed = match request.try_into_typed::<RequestHeader>() {
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
                shard::MetadataSubmit::CompleteRevocation {
                    stream_id,
                    topic_id,
                    group_id,
                    source_client_id,
                    partition_id,
                    reply,
                } => {
                    let commit = shard
                        .plane
                        .metadata()
                        .submit_complete_revocation_in_process(
                            stream_id,
                            topic_id,
                            group_id,
                            source_client_id,
                            partition_id,
                        )
                        .await
                        .ok();
                    let _ = reply.try_send(commit);
                }
            }
        })
        .detach();
    })
}

fn enqueue_client_request(
    shard: Rc<ServerNgShard>,
    sessions: Rc<RefCell<SessionManager>>,
    queues: ClientRequestQueues,
    active: ActiveClientRequests,
    client_id: u128,
    message: Message<GenericHeader>,
) {
    queues
        .borrow_mut()
        .entry(client_id)
        .or_default()
        .push_back(message);
    if !active.borrow_mut().insert(client_id) {
        return;
    }

    compio::runtime::spawn(async move {
        drain_client_requests(shard, sessions, queues, active, client_id).await;
    })
    .detach();
}

#[allow(clippy::future_not_send)]
async fn drain_client_requests(
    shard: Rc<ServerNgShard>,
    sessions: Rc<RefCell<SessionManager>>,
    queues: ClientRequestQueues,
    active: ActiveClientRequests,
    client_id: u128,
) {
    loop {
        let Some(message) = pop_next_client_request(&queues, &active, client_id) else {
            return;
        };
        handle_client_request(&shard, &sessions, client_id, message).await;
    }
}

fn pop_next_client_request(
    queues: &ClientRequestQueues,
    active: &ActiveClientRequests,
    client_id: u128,
) -> Option<Message<GenericHeader>> {
    let mut queues = queues.borrow_mut();
    let Some(queue) = queues.get_mut(&client_id) else {
        active.borrow_mut().remove(&client_id);
        return None;
    };
    let message = queue.pop_front();
    if queue.is_empty() {
        queues.remove(&client_id);
    }
    if message.is_none() {
        active.borrow_mut().remove(&client_id);
    }
    message
}

#[allow(clippy::future_not_send, clippy::too_many_lines)]
async fn handle_client_request(
    shard: &Rc<ServerNgShard>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
    message: Message<iggy_binary_protocol::GenericHeader>,
) {
    let request = match message.try_into_typed::<RequestHeader>() {
        Ok(request) => request,
        Err(error) => {
            warn!(
                transport_client_id,
                error = %error,
                "dropping client request with invalid header"
            );
            return;
        }
    };

    ensure_transport_connection(shard, sessions, transport_client_id);

    // Any request is liveness proof, not just PING: an idle-but-active client
    // (e.g. an admin issuing reads between long sleeps) must not be evicted by
    // the heartbeat verifier. A genuinely dead connection sends nothing, so the
    // intended stale-client eviction still fires. No-ops for an unbound client.
    sessions.borrow_mut().record_heartbeat(transport_client_id);

    let header = *request.header();
    if header.operation == Operation::NonReplicated {
        // Auth bypass guard: only `PING` and `GET_CLUSTER_METADATA` are
        // legitimately pre-auth (liveness probe + connection bootstrap
        // metadata). Every other non-replicated code (`GET_STREAM*`,
        // `GET_TOPIC*`, `GET_STATS`, `POLL_MESSAGES`) reads live state and
        // MUST go through Register first, since server-ng has no
        // per-resource authz layer.
        let nr_code = u32::from_le_bytes(request.header().reserved[..4].try_into().unwrap());
        let allowed_pre_auth = matches!(nr_code, PING_CODE | GET_CLUSTER_METADATA_CODE);
        if !allowed_pre_auth && sessions.borrow().get_session(transport_client_id).is_none() {
            warn!(
                transport_client_id,
                code = nr_code,
                "rejecting pre-auth non-replicated read with Eviction(NoSession)"
            );
            send_unauthenticated_eviction(shard, transport_client_id).await;
            return;
        }
        handle_non_replicated_request(shard, sessions, transport_client_id, request).await;
        return;
    }

    if header.operation == Operation::Register && header.session == 0 && header.request == 0 {
        handle_login_register_request(shard, sessions, transport_client_id, request).await;
        return;
    }

    if header.operation == Operation::Logout {
        handle_logout_request(shard, sessions, transport_client_id, request).await;
        return;
    }

    let bound = sessions.borrow().get_session(transport_client_id);
    if bound.is_none() {
        // Replicated request on an unbound transport. Without this short-
        // circuit, the rewrite below overwrites `header.client` with
        // `transport_client_id` and dispatches; the request_preflight then
        // rejects with `NoSession`/`SessionMismatch` and the failure either
        // disappears silently or emits an Eviction the SDK previously
        // could not decode. Either way the SDK blocked until socket
        // timeout. Emit an empty Reply so the SDK fails fast: the typed
        // decoder downstream rejects the empty body with `InvalidCommand`
        // instead of hanging.
        let commit = current_metadata_commit(shard);
        let reply = build_empty_reply(&header, transport_client_id, 0, commit);
        if let Err(error) = shard
            .bus
            .send_to_client(transport_client_id, reply.into_generic().into_frozen())
            .await
        {
            warn!(
                transport_client_id,
                error = %error,
                operation = ?header.operation,
                "failed to surface unbound-session reply"
            );
        } else {
            warn!(
                transport_client_id,
                operation = ?header.operation,
                "dropping replicated request from unbound transport; replied empty"
            );
        }
        return;
    }

    // DeleteSegments is neither a partition nor a metadata consensus op: the
    // owning shard resolves the requested count to a concrete offset, then a
    // `TruncatePartition` is replicated through metadata (Option A). Each
    // replica's reconciler trims to the committed watermark. Handle it here,
    // ahead of the partition/metadata routing below.
    if header.operation == Operation::DeleteSegments {
        handle_delete_segments_request(shard, transport_client_id, bound, &request).await;
        return;
    }

    // Partition data-plane op (SendMessages / consumer-offset writes): the
    // op belongs to the partition's own consensus group, not the metadata
    // group. Route it through the shard mesh by namespace; the owning
    // shard's partitions plane runs at-least-once consensus and replies
    // directly via `send_to_client`. `header.client` therefore stays the
    // TRANSPORT id (home-shard routing bits), not the VSR session id --
    // partition ops are sessionless ("session lifecycle is metadata-only").
    if header.operation.is_partition() {
        // The consumer-group offset fence keys on the bound VSR client id (the
        // member id), not the transport id stamped into the partition-op
        // header. `bound` is Some here (unbound transports returned above).
        let vsr_client_id = bound.map_or(0, |(client_id, _)| client_id);
        let namespace = match resolve_partition_request_namespace(
            shard,
            header.operation,
            request_body(&request),
            vsr_client_id,
        ) {
            Ok(namespace) => namespace,
            Err(error) => {
                // A partition op against a stream/topic that no longer
                // resolves (e.g. a consumer's trailing auto-commit racing a
                // `delete_stream`). A silent drop wedges the SDK connection
                // forever; reply empty so the client fails fast instead.
                warn!(
                    transport_client_id,
                    error = %error,
                    operation = ?header.operation,
                    "partition request with unresolved namespace; replying empty"
                );
                send_empty_partition_reply(shard, transport_client_id, &header).await;
                return;
            }
        };
        // Convergence wait: a CreateTopic commit returns to the client
        // before the per-shard reconcilers seed routing rows and
        // materialise the partition (next wake/periodic tick). The SDK
        // does not replay sends, so an immediately-following partition op
        // would be dropped as unroutable. Absorb that window here with a
        // bounded wait; steady-state sends (row present, partition probed
        // once) skip it entirely.
        if !wait_for_partition_routable(shard, IggyNamespace::from_raw(namespace)).await {
            warn!(
                transport_client_id,
                namespace,
                operation = ?header.operation,
                "partition request not routable within budget; replying empty"
            );
            send_empty_partition_reply(shard, transport_client_id, &header).await;
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
        let request = request.transmute_header(|header, new_header: &mut RequestHeader| {
            *new_header = header;
            new_header.namespace = namespace;
            new_header.client = transport_client_id;
            // Header validation requires `session > 0 && request > 0` for
            // non-register ops. The partition plane itself is sessionless
            // (at-least-once, no `ClientTable` dedup), so the bound VSR
            // session merely satisfies validation, and a zero request id
            // (the SDK does not number data-plane ops) is normalized.
            if let Some((_, bound_session)) = bound {
                new_header.session = bound_session;
            }
            new_header.request = new_header.request.max(1);
        });
        shard.dispatch(request.into_generic());
        return;
    }

    let request = request.transmute_header(|header, new_header: &mut RequestHeader| {
        *new_header = header;
        // `bound` is always Some here (unbound transports early-return above);
        // this sets the consensus client id + session for the replicated op.
        if let Some((bound_client_id, bound_session)) = bound {
            new_header.client = bound_client_id;
            new_header.session = bound_session;
        }
    });
    let (request, raw_pat_token) =
        match maybe_rewrite_pat_request(sessions, transport_client_id, request) {
            Ok(rewritten) => rewritten,
            Err(error) => {
                warn!(
                    transport_client_id,
                    error = %error,
                    operation = ?header.operation,
                    "dropping request with invalid PAT replication context"
                );
                return;
            }
        };
    // Hash raw passwords on the primary before replication (CreateUser /
    // ChangePassword); see `crate::users`. Replicas store the hash directly.
    let request = match maybe_rewrite_user_password_request(request) {
        Ok(rewritten) => rewritten,
        Err(error) => {
            warn!(
                transport_client_id,
                error = %error,
                operation = ?header.operation,
                "dropping user request with invalid password payload"
            );
            return;
        }
    };
    // Enrich consumer-group Join/Leave with the client's VSR id (+ topic
    // partition count for Join) before replication; see `crate::consumer_group`.
    let request = match maybe_rewrite_consumer_group_request(shard, request).await {
        Ok(rewritten) => rewritten,
        Err(error) => {
            warn!(
                transport_client_id,
                error = %error,
                operation = ?header.operation,
                "dropping consumer-group request with invalid payload"
            );
            return;
        }
    };
    let request_header = *request.header();
    // Replicated request: run consensus on the metadata owner (shard 0) and
    // bring the committed reply back here. This shard owns the connection,
    // so it writes the reply to the socket via the transport client id --
    // shard 0 can't route by the consensus client id (no home-shard bits).
    match submit_client_request_on_owner(shard, request).await {
        Some(reply) => {
            // The raw PAT token never enters consensus (it is non-deterministic
            // and secret), so the committed reply body is empty. Substitute the
            // raw-token response here, on the minting client's home shard, using
            // the confirmed commit position from the committed reply.
            let reply = match build_raw_pat_reply(&request_header, reply, raw_pat_token) {
                Ok(reply) => reply,
                Err(error) => {
                    warn!(
                        transport_client_id,
                        error = %error,
                        "failed to build raw PAT reply"
                    );
                    return;
                }
            };
            if let Err(error) = shard
                .bus
                .send_to_client(transport_client_id, reply.into_frozen())
                .await
            {
                warn!(
                    transport_client_id,
                    error = %error,
                    operation = ?header.operation,
                    "failed to deliver committed reply to client"
                );
            }
        }
        None => {
            // Transient submit failure (not primary / not caught up / dedup
            // absorbed). Stay silent; the SDK read-timeout replays.
            warn!(
                transport_client_id,
                operation = ?header.operation,
                "replicated request not committed (transient); client will replay"
            );
        }
    }
}

/// Per-user PATs, resolved from this shard's session (like `get_me`) and read
/// out of the Users STM. Built here rather than in `build_non_replicated_response`
/// which has no session context.
#[allow(clippy::future_not_send)]
async fn handle_get_personal_access_tokens(
    shard: &Rc<ServerNgShard>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
    request: &Message<RequestHeader>,
) {
    let response = build_get_personal_access_tokens_response(shard, sessions, transport_client_id);
    send_non_replicated_bytes(
        shard,
        request,
        transport_client_id,
        response.to_bytes(),
        "get_personal_access_tokens",
    )
    .await;
}

/// The requesting connection's own identity, sourced from this shard's
/// `SessionManager` (not `IggyMetadata`), so built here rather than in
/// `build_non_replicated_response`.
#[allow(clippy::future_not_send)]
async fn handle_get_me(
    shard: &Rc<ServerNgShard>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
    request: &Message<RequestHeader>,
) {
    let response = build_get_me_response(shard, sessions, transport_client_id);
    send_non_replicated_bytes(
        shard,
        request,
        transport_client_id,
        response.to_bytes(),
        "get_me",
    )
    .await;
}

#[allow(clippy::future_not_send)]
async fn handle_non_replicated_request(
    shard: &Rc<ServerNgShard>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
    request: Message<RequestHeader>,
) {
    const CODE_RANGE: std::ops::Range<usize> = 0..4;
    let code = u32::from_le_bytes(request.header().reserved[CODE_RANGE].try_into().unwrap());
    match code {
        PING_CODE => {
            // A ping is the client's liveness proof; reset its staleness clock
            // so the heartbeat verifier doesn't evict an active connection.
            sessions.borrow_mut().record_heartbeat(transport_client_id);
            let commit = current_metadata_commit(shard);
            let reply = build_empty_reply(
                request.header(),
                request.header().client,
                request.header().session,
                commit,
            );
            if let Err(error) = shard
                .bus
                .send_to_client(transport_client_id, reply.into_generic().into_frozen())
                .await
            {
                warn!(
                    transport_client_id,
                    error = %error,
                    "failed to send non-replicated ping reply"
                );
            }
        }
        GET_ME_CODE => {
            handle_get_me(shard, sessions, transport_client_id, &request).await;
        }
        GET_PERSONAL_ACCESS_TOKENS_CODE => {
            handle_get_personal_access_tokens(shard, sessions, transport_client_id, &request).await;
        }
        GET_CLIENTS_CODE => {
            // Shared-nothing: each shard knows only its own connections, so
            // gather across all shards (scatter-gather over the mesh).
            let infos = shard.list_all_clients().await;
            let response = GetClientsResponse {
                clients: infos
                    .iter()
                    .map(|info| connected_client_to_response(shard, info))
                    .collect(),
            };
            send_non_replicated_bytes(
                shard,
                &request,
                transport_client_id,
                response.to_bytes(),
                "get_clients",
            )
            .await;
        }
        GET_CLIENT_CODE => {
            // No reverse map from the wire u32 id to a u128 transport id /
            // home shard (the u32 is just the seq tail), so gather all and
            // filter -- same fan-out as `get_clients`.
            let target = GetClientRequest::decode_from(request_body(&request))
                .ok()
                .map(|req| req.client_id);
            let infos = shard.list_all_clients().await;
            #[allow(clippy::cast_possible_truncation)]
            let found = target.and_then(|id| infos.iter().find(|info| info.client_id as u32 == id));
            // The SDK decodes an empty body as `None` (client not found).
            let bytes = found.map_or_else(Bytes::new, |info| {
                let consumer_groups = info.vsr_client_id.map_or_else(Vec::new, |vsr_client_id| {
                    shard
                        .plane
                        .metadata()
                        .mux_stm
                        .streams()
                        .consumer_group_memberships(vsr_client_id)
                        .into_iter()
                        .map(
                            |(stream_id, topic_id, group_id)| ConsumerGroupInfoResponse {
                                stream_id,
                                topic_id,
                                group_id,
                            },
                        )
                        .collect()
                });
                ClientDetailsResponse {
                    client: connected_client_to_response(shard, info),
                    consumer_groups,
                }
                .to_bytes()
            });
            send_non_replicated_bytes(shard, &request, transport_client_id, bytes, "get_client")
                .await;
        }
        POLL_MESSAGES_CODE => {
            handle_poll_messages(shard, transport_client_id, &request).await;
        }
        GET_CONSUMER_OFFSET_CODE => {
            handle_get_consumer_offset(shard, transport_client_id, &request).await;
        }
        SYNC_CONSUMER_GROUP_CODE => {
            handle_sync_consumer_group(shard, transport_client_id, &request).await;
        }
        _ => handle_default_non_replicated(shard, transport_client_id, code, &request).await,
    }
}

#[allow(clippy::future_not_send)]
async fn handle_default_non_replicated(
    shard: &Rc<ServerNgShard>,
    transport_client_id: u128,
    code: u32,
    request: &Message<RequestHeader>,
) {
    match build_non_replicated_response(shard, code, request_body(request)) {
        Ok(response) => {
            let commit = current_metadata_commit(shard);
            let reply = response.into_reply(
                request.header(),
                request.header().client,
                request.header().session,
                commit,
            );
            if let Err(error) = shard
                .bus
                .send_to_client(transport_client_id, reply.into_generic().into_frozen())
                .await
            {
                warn!(
                    transport_client_id,
                    code,
                    error = %error,
                    "failed to send non-replicated VSR reply"
                );
            }
        }
        Err(error) => {
            warn!(
                transport_client_id,
                code,
                error = %error,
                "dropping unsupported non-replicated VSR request"
            );
        }
    }
}

/// Send a non-replicated reply body to a client, stamping the current
/// metadata commit. Shared by the `get_me` / `get_clients` / `get_client`
/// arms.
#[allow(clippy::future_not_send)]
async fn send_non_replicated_bytes(
    shard: &Rc<ServerNgShard>,
    request: &Message<RequestHeader>,
    transport_client_id: u128,
    bytes: Bytes,
    label: &'static str,
) {
    let commit = current_metadata_commit(shard);
    let reply = NonReplicatedResponse::Bytes(bytes).into_reply(
        request.header(),
        request.header().client,
        request.header().session,
        commit,
    );
    if let Err(error) = shard
        .bus
        .send_to_client(transport_client_id, reply.into_generic().into_frozen())
        .await
    {
        warn!(transport_client_id, label, error = %error, "failed to send non-replicated reply");
    }
}

/// Reject a pre-auth request with a typed `Eviction(NoSession)` frame.
///
/// The SDK's reply decoder maps eviction reasons to typed errors
/// (`NoSession` -> `Unauthenticated`), so clients fail fast with the same
/// error the legacy server returns instead of a body-decode failure. The
/// eviction context is best-effort off the metadata consensus (peer shards
/// have none; zeroes are cosmetic -- the SDK only reads the reason).
#[allow(clippy::future_not_send)]
async fn send_unauthenticated_eviction(shard: &Rc<ServerNgShard>, transport_client_id: u128) {
    let ctx = shard.plane.metadata().consensus.as_ref().map_or(
        consensus::EvictionContext {
            cluster: 0,
            view: 0,
            replica: 0,
        },
        consensus::EvictionContext::from_consensus,
    );
    let eviction = consensus::build_eviction_message(
        ctx,
        transport_client_id,
        iggy_binary_protocol::EvictionReason::NoSession,
    );
    if let Err(error) = shard
        .bus
        .send_to_client(transport_client_id, eviction.into_generic().into_frozen())
        .await
    {
        warn!(
            transport_client_id,
            error = %error,
            "failed to send unauthenticated eviction"
        );
    }
}

/// Per-shard heartbeat verifier: evict connections that have not pinged within
/// `1.2 x interval`. Mirrors the legacy `verify_heartbeats` periodic task.
/// Eviction reuses the disconnect path (drops the client from its consumer
/// groups + rebalances via the replicated `Logout`) and sends a session-
/// terminal `Eviction(StaleClient)` so the client fails fast and can reconnect.
#[allow(clippy::future_not_send)]
pub(crate) async fn run_heartbeat_verifier(
    shard: Rc<ServerNgShard>,
    sessions: Rc<RefCell<SessionManager>>,
    interval: std::time::Duration,
    stop_rx: shard::Receiver<()>,
) {
    // Legacy `MAX_THRESHOLD`: a client is stale once it misses ~1.2 intervals.
    let max_age = interval.mul_f64(1.2);
    loop {
        if stop_rx.try_recv().is_ok() {
            break;
        }
        let stale = sessions
            .borrow()
            .collect_stale(max_age, std::time::Instant::now());
        for transport_client_id in stale {
            // The heartbeat verifier exists to release a dead client's
            // consumer-group membership (so the group rebalances off it). A
            // connection that holds no membership has nothing for the eviction
            // to clean up; reaping it would only drop a still-usable session
            // (e.g. an idle admin connection that polls between long gaps),
            // which the legacy server tolerates. The real transport-disconnect
            // path still reaps it on socket close. So only evict a stale
            // connection that is actually a group member.
            let is_group_member = sessions
                .borrow()
                .bound_client_id(transport_client_id)
                .is_some_and(|vsr_client_id| {
                    !shard
                        .plane
                        .metadata()
                        .mux_stm
                        .streams()
                        .consumer_group_memberships(vsr_client_id)
                        .is_empty()
                });
            if is_group_member {
                evict_stale_client(&shard, &sessions, transport_client_id).await;
            }
        }
        compio::time::sleep(interval).await;
    }
}

/// Evict one stale connection: drop its session (releasing consumer-group
/// membership through a replicated `Logout`) and notify the client with a
/// session-terminal `Eviction(StaleClient)`.
#[allow(clippy::future_not_send)]
async fn evict_stale_client(
    shard: &Rc<ServerNgShard>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
) {
    let bound = sessions.borrow_mut().remove_connection(transport_client_id);
    if let Some((vsr_client_id, session)) = bound {
        submit_disconnect_logout(Rc::clone(shard), vsr_client_id, session);
    }
    let ctx = shard.plane.metadata().consensus.as_ref().map_or(
        consensus::EvictionContext {
            cluster: 0,
            view: 0,
            replica: 0,
        },
        consensus::EvictionContext::from_consensus,
    );
    let eviction = consensus::build_eviction_message(
        ctx,
        transport_client_id,
        iggy_binary_protocol::EvictionReason::StaleClient,
    );
    if let Err(error) = shard
        .bus
        .send_to_client(transport_client_id, eviction.into_generic().into_frozen())
        .await
    {
        warn!(
            transport_client_id,
            error = %error,
            "failed to send stale-client eviction"
        );
    } else {
        warn!(
            transport_client_id,
            "evicted stale client (missed heartbeat)"
        );
    }
}

/// Serve `poll_messages`: resolve the partition namespace, run the read on
/// the owning shard ([`shard::IggyShard::partition_read`]), and re-encode
/// the stored batches into the legacy wire `PolledMessages` body.
///
/// Failures reply with an empty body so the SDK fails fast on decode
/// instead of hanging until its read timeout.
#[allow(clippy::future_not_send)]
async fn handle_poll_messages(
    shard: &Rc<ServerNgShard>,
    transport_client_id: u128,
    request: &Message<RequestHeader>,
) {
    let body = match decode_poll_request(shard, request) {
        Ok((namespace, partition_id, consumer, args)) => {
            match shard
                .partition_read(namespace, PartitionRead::Poll { consumer, args })
                .await
            {
                Some(PartitionReadReply::Poll {
                    fragments,
                    current_offset,
                }) => build_polled_messages_body(partition_id, current_offset, fragments)
                    .unwrap_or_else(|error| {
                        warn!(
                            transport_client_id,
                            error = %error,
                            "failed to re-encode polled batches; replying empty poll"
                        );
                        empty_polled_messages_body(partition_id)
                    }),
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
#[allow(clippy::future_not_send)]
async fn handle_get_consumer_offset(
    shard: &Rc<ServerNgShard>,
    transport_client_id: u128,
    request: &Message<RequestHeader>,
) {
    let body = match decode_consumer_offset_request(shard, request) {
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

/// Serve `SyncConsumerGroup`: return the requesting member's current partition
/// assignment + group generation so the client can select partitions locally.
/// The member is keyed by the connection's bound VSR client id
/// (`header().client`). An empty body decodes as "no assignment" on the SDK.
#[allow(clippy::future_not_send)]
async fn handle_sync_consumer_group(
    shard: &Rc<ServerNgShard>,
    transport_client_id: u128,
    request: &Message<RequestHeader>,
) {
    let body = match SyncConsumerGroupRequest::decode_from(request_body(request)) {
        Ok(wire) => shard
            .plane
            .metadata()
            .mux_stm
            .streams()
            .consumer_group_member_assignment(
                &wire.stream_id,
                &wire.topic_id,
                &wire.group_id,
                request.header().client,
            )
            .map_or_else(Bytes::new, |(generation, partitions)| {
                SyncConsumerGroupResponse {
                    generation,
                    partitions,
                }
                .to_bytes()
            }),
        Err(error) => {
            warn!(
                transport_client_id,
                error = %error,
                "sync_consumer_group request rejected; replying empty"
            );
            Bytes::new()
        }
    };
    send_non_replicated_bytes(
        shard,
        request,
        transport_client_id,
        body,
        "sync_consumer_group",
    )
    .await;
}

/// Ack a partition op that cannot be routed (unresolved or never-
/// materialised namespace) with an empty Reply. The SDK connection
/// processes replies in lockstep, so a silent drop wedges every
/// subsequent request on that connection.
#[allow(clippy::future_not_send)]
async fn send_empty_partition_reply(
    shard: &Rc<ServerNgShard>,
    transport_client_id: u128,
    request_header: &RequestHeader,
) {
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

/// Wait (bounded) until `namespace` is routable: this shard's routing row
/// exists and the owning shard answers a probe read (partition
/// materialised). Fast path: row already present -> no probe, no wait.
///
/// Covers the post-`CreateTopic` convergence window where the metadata
/// commit has returned to the client but the per-shard reconcilers have
/// not yet seeded routing rows / materialised partitions.
#[allow(clippy::future_not_send)]
async fn wait_for_partition_routable(shard: &Rc<ServerNgShard>, namespace: IggyNamespace) -> bool {
    const ATTEMPT_DELAY: std::time::Duration = std::time::Duration::from_millis(50);
    const BUDGET: std::time::Duration = std::time::Duration::from_secs(3);

    if shard.shards_table().shard_for(namespace).is_some() {
        return true;
    }
    let deadline = std::time::Instant::now() + BUDGET;
    while shard.shards_table().shard_for(namespace).is_none() {
        if std::time::Instant::now() >= deadline {
            return false;
        }
        compio::time::sleep(ATTEMPT_DELAY).await;
    }
    // The local row is seeded by THIS shard's reconciler; the owner
    // materialises the partition on its own pass. Probe with a cheap read
    // until the owner answers, so the write below normally clears the
    // owner's "partition not initialized" guard. Not a hard guarantee: the
    // partition can de-materialise between this probe and the dispatch, but
    // the park/tombstone path re-checks and the client retries.
    while std::time::Instant::now() < deadline {
        match shard
            .partition_read(
                namespace,
                PartitionRead::ConsumerOffset {
                    consumer: PollingConsumer::Consumer(0, 0),
                },
            )
            .await
        {
            Some(PartitionReadReply::NotFound) | None => {
                compio::time::sleep(ATTEMPT_DELAY).await;
            }
            Some(_) => return true,
        }
    }
    false
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

#[allow(clippy::cast_possible_truncation)]
fn decode_poll_request(
    shard: &Rc<ServerNgShard>,
    request: &Message<RequestHeader>,
) -> Result<DecodedPollRequest, IggyError> {
    let wire = PollMessagesRequest::decode_from(request_body(request))
        .map_err(|_| IggyError::InvalidCommand)?;
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
        let client_id = request.header().client;
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

    let partition_id = wire.partition_id.ok_or(IggyError::InvalidIdentifier)?;
    let namespace =
        resolve_partition_namespace(shard, &wire.stream_id, &wire.topic_id, Some(partition_id))?;
    let consumer = polling_consumer_from_wire(&wire.consumer, partition_id)?;
    Ok((namespace, partition_id, consumer, args))
}

fn decode_consumer_offset_request(
    shard: &Rc<ServerNgShard>,
    request: &Message<RequestHeader>,
) -> Result<(IggyNamespace, u32, PollingConsumer), IggyError> {
    let wire = GetConsumerOffsetRequest::decode_from(request_body(request))
        .map_err(|_| IggyError::InvalidCommand)?;
    let partition_id = wire.partition_id.ok_or(IggyError::InvalidIdentifier)?;
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

/// Run the consensus `Register` proposal on the metadata owner (shard 0)
/// and return the committed session.
///
/// Credential verification and session binding stay on the calling (home)
/// shard -- only this consensus step must execute where the metadata
/// consensus group lives. On shard 0 it calls in-process directly; on a
/// peer it forwards a [`shard::MetadataSubmit`] to shard 0 and awaits the
/// committed op. A dropped reply (shard-0 inbox full / shutdown) maps to a
/// transient `Canceled`, which the caller wraps so the SDK replays.
#[allow(clippy::future_not_send)]
pub(crate) async fn submit_register_on_owner(
    shard: &Rc<ServerNgShard>,
    vsr_client_id: u128,
) -> Result<u64, MetadataSubmitError> {
    if shard.id == 0 {
        return shard
            .plane
            .metadata()
            .submit_register_in_process(vsr_client_id)
            .await;
    }
    let (reply, rx) = shard::channel::<Option<u64>>(1);
    shard.forward_metadata_submit(shard::MetadataSubmit::Register {
        vsr_client_id,
        reply,
    });
    match rx.recv().await {
        Ok(Some(session)) => Ok(session),
        _ => Err(MetadataSubmitError::Canceled),
    }
}

/// Logout counterpart of [`submit_register_on_owner`].
#[allow(clippy::future_not_send)]
async fn submit_logout_on_owner(
    shard: &Rc<ServerNgShard>,
    vsr_client_id: u128,
    session: u64,
    request: u64,
) -> Result<u64, MetadataSubmitError> {
    if shard.id == 0 {
        return shard
            .plane
            .metadata()
            .submit_logout_in_process(vsr_client_id, session, request)
            .await;
    }
    let (reply, rx) = shard::channel::<Option<u64>>(1);
    shard.forward_metadata_submit(shard::MetadataSubmit::Logout {
        vsr_client_id,
        session,
        request,
        reply,
    });
    match rx.recv().await {
        Ok(Some(commit)) => Ok(commit),
        _ => Err(MetadataSubmitError::Canceled),
    }
}

/// Handle a client `DeleteSegments`: resolve the requested count to an offset
/// on the owning shard, replicate a `TruncatePartition` through metadata so
/// every replica trims to the same watermark, then ack the client. The local
/// deletion happens later, when each replica's reconciler observes the commit.
///
/// Best-effort space management: any failure (bad request, unknown namespace,
/// nothing sealed to delete, transient submit error) is logged and still acks,
/// so the client never blocks on it.
#[allow(clippy::future_not_send)]
async fn handle_delete_segments_request(
    shard: &Rc<ServerNgShard>,
    transport_client_id: u128,
    bound: Option<(u128, u64)>,
    request: &Message<RequestHeader>,
) {
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
    // keeping the sequence contiguous. Skipping the commit (or attributing it
    // to an internal id) leaves a hole that fails the next metadata op's
    // `request == committed + 1` preflight -> RequestGap -> silent drop -> the
    // SDK blocks until timeout. A no-op delete still commits `up_to_offset = 0`
    // (monotonic apply) for the same reason.
    #[allow(clippy::cast_possible_truncation)]
    let truncate = match DeleteSegmentsRequest::decode_from(body) {
        Ok(parsed) => match resolve_partition_request_namespace(
            shard,
            Operation::DeleteSegments,
            body,
            transport_client_id,
        ) {
            Ok(namespace_raw) => {
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
                    }) => offset,
                    _ => 0,
                };
                Some(build_truncate_partition_client_message(
                    &header,
                    vsr_client_id,
                    session,
                    namespace.stream_id() as u32,
                    namespace.topic_id() as u32,
                    namespace.partition_id() as u32,
                    up_to_offset,
                ))
            }
            Err(error) => {
                warn!(
                    transport_client_id,
                    error = %error,
                    "delete_segments: unresolved namespace"
                );
                None
            }
        },
        Err(_) => None,
    };

    if let Some(truncate) = truncate
        && submit_client_request_on_owner(shard, truncate)
            .await
            .is_none()
    {
        // Transient submit failure (not primary / view change). Stay silent;
        // the SDK read-timeout replays the same request id, which re-resolves
        // and commits. Acking here would advance the client past an unrecorded
        // request and gap the next metadata op.
        warn!(
            transport_client_id,
            "delete_segments: transient submit; client will replay"
        );
        return;
    }

    let commit = current_metadata_commit(shard);
    let reply = build_empty_reply(&header, transport_client_id, session, commit);
    if let Err(error) = shard
        .bus
        .send_to_client(transport_client_id, reply.into_generic().into_frozen())
        .await
    {
        warn!(
            transport_client_id,
            error = %error,
            "delete_segments: failed to send reply"
        );
    }
}

/// Disconnect cleanup: the local `SessionManager` connection is already
/// dropped by the caller; this submits a session-matched `Logout` so the
/// committed apply releases the `ClientTable` slot on every replica (shard 0
/// included, since shard 0 is itself a replica).
///
/// Deliberately does NOT drop the local `ClientTable` slot first:
/// `submit_logout_*` short-circuits when the slot is already gone, so a
/// pre-emptive local removal would suppress the `Logout` and leave peer
/// replicas with an orphaned session until they evict it themselves -- the
/// exact divergence this avoids. `submit_logout_on_owner` runs in-process on
/// shard 0 and forwards for peer-homed connections; its session guard drops a
/// stale logout for a reused client id.
#[allow(clippy::future_not_send)]
fn submit_disconnect_logout(shard: Rc<ServerNgShard>, vsr_client_id: u128, session: u64) {
    // Synthetic request id: header validation rejects `request == 0` for
    // non-register ops, and a disconnect has no client-issued request id.
    // The logout apply keys on (client, session) only, so any non-zero id
    // is valid here.
    const DISCONNECT_LOGOUT_REQUEST_ID: u64 = u64::MAX;
    compio::runtime::spawn(async move {
        if let Err(error) =
            submit_logout_on_owner(&shard, vsr_client_id, session, DISCONNECT_LOGOUT_REQUEST_ID)
                .await
        {
            warn!(
                vsr_client_id,
                ?error,
                "disconnect logout submit failed; peer slots may linger until eviction"
            );
        }
    })
    .detach();
}

/// Submit a replicated client request to the metadata owner (shard 0) and
/// return the committed reply.
///
/// The metadata consensus group lives on shard 0, but the connection lives
/// on the home shard (this shard). Run consensus where it belongs and bring
/// the committed reply back here so the caller can write it to the
/// originating socket -- shard 0 cannot route the reply by the consensus
/// `client` id (it's the VSR id, not the transport/home-shard-encoding id).
/// `None` = transient submit failure (SDK read-timeout replays).
#[allow(clippy::future_not_send)]
async fn submit_client_request_on_owner(
    shard: &Rc<ServerNgShard>,
    request: Message<RequestHeader>,
) -> Option<Message<GenericHeader>> {
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

#[allow(clippy::future_not_send)]
async fn handle_logout_request(
    shard: &Rc<ServerNgShard>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
    request: Message<RequestHeader>,
) {
    let Some((vsr_client_id, session)) = sessions.borrow().get_session(transport_client_id) else {
        warn!(
            transport_client_id,
            "dropping logout for unbound VSR session"
        );
        return;
    };

    let request_id = request.header().request;
    let commit = match submit_logout_on_owner(shard, vsr_client_id, session, request_id).await {
        Ok(commit) => commit,
        Err(error) => {
            warn!(transport_client_id, error = %error, "logout/unregister failed");
            return;
        }
    };

    sessions.borrow_mut().remove_connection(transport_client_id);

    let reply = build_empty_reply(request.header(), vsr_client_id, session, commit);
    if let Err(error) = shard
        .bus
        .send_to_client(transport_client_id, reply.into_generic().into_frozen())
        .await
    {
        warn!(
            transport_client_id,
            error = %error,
            "failed to send logout reply"
        );
    }
}

fn ensure_transport_connection(
    shard: &Rc<ServerNgShard>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
) {
    let Some(meta) = shard.bus.client_meta(transport_client_id) else {
        return;
    };
    sessions
        .borrow_mut()
        .ensure_connection(transport_client_id, meta.peer_addr, meta.transport);
}

#[allow(clippy::future_not_send, clippy::too_many_lines)]
async fn handle_login_register_request(
    shard: &Rc<ServerNgShard>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
    request: Message<RequestHeader>,
) {
    let body = request_body(&request);
    let vsr_client_id = request.header().client;

    // Both login-register shapes share the ClientVersionInfo prefix, so the
    // protocol gate decodes it once and runs before any credential work; the
    // body shapes below parse from past the prefix. Only VSR clients reach
    // this gate -- legacy SDKs use LOGIN_USER_CODE, a separate path. A
    // pre-versioning VSR client sends the old prefix-less body, which fails
    // ClientVersionInfo::decode (-> MalformedLogin) or the version gate
    // (-> IncompatibleProtocol) right here, not dropped earlier.
    let Ok((version_info, prefix_len)) = ClientVersionInfo::decode(body) else {
        warn!(
            transport_client_id,
            "rejecting login: body has no decodable version prefix"
        );
        send_login_eviction(
            shard,
            transport_client_id,
            vsr_client_id,
            EvictionReason::MalformedLogin,
        )
        .await;
        return;
    };
    if !is_protocol_compatible(version_info.protocol_version) {
        warn!(
            transport_client_id,
            client_protocol_version = %ProtocolVersion(version_info.protocol_version),
            sdk_name = %version_info.sdk_name,
            sdk_version = %version_info.sdk_version,
            "rejecting login: incompatible protocol version"
        );
        send_login_eviction(
            shard,
            transport_client_id,
            vsr_client_id,
            EvictionReason::IncompatibleProtocol,
        )
        .await;
        return;
    }

    let body_tail = &body[prefix_len..];
    if let Ok((wire_request, _)) =
        LoginRegisterRequest::decode_after_prefix(version_info.clone(), body_tail)
    {
        match verify_login_credentials(
            shard,
            wire_request.username.as_str(),
            wire_request.password.expose_secret(),
        ) {
            Ok(user_id) => {
                if let Err(error) = complete_login_register(
                    shard,
                    sessions,
                    transport_client_id,
                    vsr_client_id,
                    request.header(),
                    user_id,
                    &wire_request.version_info,
                )
                .await
                {
                    warn!(transport_client_id, error = %error, "login/register failed");
                    surface_login_failure(shard, transport_client_id, request.header(), &error)
                        .await;
                }
                return;
            }
            Err(LoginRegisterError::InvalidCredentials) => {
                // Fall through to PAT attempt so a credential payload that
                // collides with a valid PAT payload shape still gets a
                // chance; if PAT also rejects, the final fall-through emits
                // the empty-reply failure path below.
            }
            Err(error) => {
                warn!(transport_client_id, error = %error, "login/register failed");
                surface_login_failure(shard, transport_client_id, request.header(), &error).await;
                return;
            }
        }
    }

    if let Ok((wire_request, _)) =
        LoginRegisterWithPatRequest::decode_after_prefix(version_info, body_tail)
    {
        match verify_pat_credentials(shard, wire_request.token.expose_secret()) {
            Ok(user_id) => {
                if let Err(error) = complete_login_register(
                    shard,
                    sessions,
                    transport_client_id,
                    vsr_client_id,
                    request.header(),
                    user_id,
                    &wire_request.version_info,
                )
                .await
                {
                    warn!(
                        transport_client_id,
                        error = %error,
                        "login/register with PAT failed"
                    );
                    surface_login_failure(shard, transport_client_id, request.header(), &error)
                        .await;
                }
                return;
            }
            Err(error) => {
                warn!(
                    transport_client_id,
                    error = %error,
                    "login/register with PAT failed"
                );
                surface_login_failure(shard, transport_client_id, request.header(), &error).await;
                return;
            }
        }
    }

    warn!(
        transport_client_id,
        "dropping register request with unsupported payload shape"
    );
    send_login_failure_reply(shard, transport_client_id, request.header()).await;
}

/// Best-effort login-rejection eviction. Terminal one-way frame; a gone
/// connection has nothing to recover, so the send error is logged and
/// dropped. Consensus context (cluster/view/replica) is stamped on the
/// metadata shard and zeroed elsewhere -- the SDK only reads the reason,
/// plus the protocol window on `IncompatibleProtocol`.
#[allow(clippy::future_not_send)]
async fn send_login_eviction(
    shard: &Rc<ServerNgShard>,
    transport_client_id: u128,
    vsr_client_id: u128,
    reason: EvictionReason,
) {
    let ctx = shard.plane.metadata().consensus.as_ref().map_or(
        EvictionContext {
            cluster: 0,
            view: 0,
            replica: 0,
        },
        EvictionContext::from_consensus,
    );
    let eviction = match reason {
        EvictionReason::IncompatibleProtocol => {
            build_incompatible_protocol_eviction_message(ctx, vsr_client_id)
        }
        _ => build_eviction_message(ctx, vsr_client_id, reason),
    };
    if let Err(error) = shard
        .bus
        .send_to_client(transport_client_id, eviction.into_generic().into_frozen())
        .await
    {
        warn!(
            transport_client_id,
            error = %error,
            reason = ?reason,
            "failed to send login eviction"
        );
    }
}

pub(crate) fn upgrade_shard_handle(
    shard_handle: &ServerNgShardHandle,
) -> Option<Rc<ServerNgShard>> {
    shard_handle
        .borrow()
        .as_ref()
        .and_then(std::rc::Weak::upgrade)
}
