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

mod authz;

use crate::auth::{
    complete_login_register, send_login_failure_reply, surface_login_failure,
    verify_login_credentials, verify_pat_credentials,
};
use crate::bootstrap::{ShellBus, ShellShard, ShellShardHandle};
use crate::cluster_meta::ClusterRoster;
use crate::consumer_group::{
    maybe_rewrite_consumer_group_request, maybe_rewrite_consumer_offset_request,
};
use crate::dispatch::authz::{
    authorize_default_read, authorize_partition_op, authorize_partition_read, authorize_uid,
    send_non_replicated_deny, send_partition_deny_reply,
};
use crate::login_register::LoginRegisterError;
use crate::pat::maybe_rewrite_pat_request;
use crate::responses::{
    NonReplicatedResponse, build_consumer_offset_body, build_deny_reply, build_empty_reply,
    build_get_me_response, build_get_personal_access_tokens_response,
    build_non_replicated_response, build_polled_messages_body, build_raw_pat_reply,
    connected_client_to_response, current_metadata_commit, resolve_partition_namespace,
    resolve_partition_request_namespace,
};
use crate::session_manager::SessionManager;
use crate::snapshot;
use crate::users::maybe_rewrite_user_password_request;
use crate::wire::{request_body, usize_to_u32};
use bytes::Bytes;
use configs::server_ng::NgSystemConfig;
use consensus::{
    Consensus, EvictionContext, MetadataHandle, PartitionsHandle, build_eviction_message,
    build_incompatible_protocol_eviction_message, build_result_rejection_reply,
};
use iggy_binary_protocol::PrepareHeader;
use iggy_binary_protocol::codes::{
    GET_CLIENT_CODE, GET_CLIENTS_CODE, GET_CLUSTER_METADATA_CODE, GET_CONSUMER_OFFSET_CODE,
    GET_ME_CODE, GET_PERSONAL_ACCESS_TOKENS_CODE, GET_SNAPSHOT_FILE_CODE, LOGIN_USER_CODE,
    LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE, PING_CODE, POLL_MESSAGES_CODE, SYNC_CONSUMER_GROUP_CODE,
};
use iggy_binary_protocol::primitives::consumer::WireConsumer;
use iggy_binary_protocol::primitives::polling_strategy::WirePollingStrategy;
use iggy_binary_protocol::requests::consumer_groups::SyncConsumerGroupRequest;
use iggy_binary_protocol::requests::consumer_offsets::{
    GetConsumerOffsetRequest, StoreConsumerOffset2Request,
};
use iggy_binary_protocol::requests::messages::PollMessagesRequest;
use iggy_binary_protocol::requests::segments::DeleteSegmentsRequest;
use iggy_binary_protocol::requests::system::get_client::GetClientRequest;
use iggy_binary_protocol::requests::system::get_snapshot::GetSnapshotRequest;
use iggy_binary_protocol::requests::users::{LoginRegisterRequest, LoginRegisterWithPatRequest};
use iggy_binary_protocol::responses::clients::client_response::ConsumerGroupInfoResponse;
use iggy_binary_protocol::responses::clients::get_client::ClientDetailsResponse;
use iggy_binary_protocol::responses::clients::get_clients::GetClientsResponse;
use iggy_binary_protocol::responses::consumer_groups::SyncConsumerGroupResponse;
use iggy_binary_protocol::responses::system::get_snapshot::GetSnapshotResponse;
use iggy_binary_protocol::{
    AckLevel, ClientVersionInfo, Command2, EvictionReason, GenericHeader, HEADER_SIZE,
    KIND_CONSUMER_GROUP, Operation, ProtocolVersion, RequestHeader, WireDecode, WireEncode,
    WireIdentifier, is_protocol_compatible,
};
use iggy_common::{IggyError, PollingStrategy, SnapshotCompression, SystemSnapshotType};
use journal::{Journal, JournalHandle};
use message_bus::AUTO_COMMIT_CLIENT_ID;
use message_bus::client_listener::RequestHandler;
use message_bus::framing::MAX_MESSAGE_SIZE;
use message_bus::replica::listener::MessageHandler;
use metadata::impls::metadata::{
    MetadataSubmitError, StreamsFrontend, build_truncate_partition_client_message,
    build_truncate_partition_client_message_with_identifiers,
};
use metadata::permissioner::Permissioner;
use partitions::{AutoCommitApplied, PollPlan, PollingArgs, PollingConsumer};
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
use std::sync::Arc;
use tracing::{debug, warn};

pub(crate) type ClientRequestQueues = Rc<RefCell<HashMap<u128, VecDeque<Message<GenericHeader>>>>>;
pub(crate) type ActiveClientRequests = Rc<RefCell<HashSet<u128>>>;

pub(crate) fn make_client_request_handler<B, MJ, S>(
    shard: &Rc<ShellShard<B, MJ, S>>,
    sessions: &Rc<RefCell<SessionManager>>,
    system_config: Arc<NgSystemConfig>,
) -> RequestHandler
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
{
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
            Arc::clone(&system_config),
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
pub(crate) fn make_partition_read_handler<B, MJ, S>(
    shard_handle: &ShellShardHandle<B, MJ, S>,
) -> PartitionReadHandler
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
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
fn spawn_poll_io<B, MJ, S>(
    shard: Rc<ShellShard<B, MJ, S>>,
    namespace: IggyNamespace,
    plan: PollPlan,
    reply: shard::Sender<PartitionReadReply>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
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
fn submit_auto_commit<B, MJ, S>(
    shard: &Rc<ShellShard<B, MJ, S>>,
    namespace: IggyNamespace,
    applied: &AutoCommitApplied,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
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
            if !(consensus.is_primary() && consensus.is_normal() && !consensus.is_syncing()) {
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

/// Build the synthetic `StoreConsumerOffset2` request for an auto-commit, keyed
/// to the resolved numeric consumer/group id and stamped with the reserved
/// [`AUTO_COMMIT_CLIENT_ID`] so the commit path skips the (unwaited) reply. The
/// wire stream/topic ids are cosmetic here -- admission and apply key off the
/// header namespace and the consumer id -- but are set from the namespace for a
/// well-formed body. `ack` is `Quorum` so the offset actually replicates.
fn build_auto_commit_request(
    namespace: IggyNamespace,
    applied: &AutoCommitApplied,
) -> Result<Message<RequestHeader>, IggyError> {
    let request = StoreConsumerOffset2Request {
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
    let header_size = std::mem::size_of::<RequestHeader>();
    let total_size = header_size + body.len();
    let size = u32::try_from(total_size).map_err(|_| IggyError::InvalidConfiguration)?;
    let mut message = Message::<RequestHeader>::new(total_size);
    message.as_mut_slice()[header_size..].copy_from_slice(&body);
    Ok(message.transmute_header(|_, header: &mut RequestHeader| {
        *header = RequestHeader {
            command: Command2::Request,
            operation: Operation::StoreConsumerOffset2,
            size,
            client: AUTO_COMMIT_CLIENT_ID,
            // The partition plane is sessionless (no `ClientTable` dedup); a
            // nonzero session + request just satisfy the wire header
            // validation.
            session: 1,
            request: 1,
            namespace: namespace.inner(),
            ..Default::default()
        };
    }))
}

pub(crate) fn make_deferred_replica_message_handler<B, MJ, S>(
    shard_handle: &ShellShardHandle<B, MJ, S>,
) -> MessageHandler
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
{
    let shard_handle = Rc::clone(shard_handle);
    Rc::new(move |_replica_id, message| {
        if let Some(shard) = upgrade_shard_handle(&shard_handle) {
            shard.dispatch(message);
        }
    })
}

pub(crate) fn make_deferred_client_request_handler<B, MJ, S>(
    bus: &B,
    shard_handle: &ShellShardHandle<B, MJ, S>,
    sessions: &Rc<RefCell<SessionManager>>,
    system_config: Arc<NgSystemConfig>,
) -> RequestHandler
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
{
    let shard_handle = Rc::clone(shard_handle);
    let sessions = Rc::clone(sessions);
    let queues: ClientRequestQueues = Rc::new(RefCell::new(HashMap::new()));
    let active: ActiveClientRequests = Rc::new(RefCell::new(HashSet::new()));
    let sessions_for_disconnect = Rc::clone(&sessions);
    let shard_handle_for_disconnect = Rc::clone(&shard_handle);
    let bus_for_spawn = (*bus).clone();
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
        let system_config = Arc::clone(&system_config);
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
        bus_for_spawn.spawn(async move {
            let Some(shard) = upgrade_shard_handle(&shard_handle) else {
                active.borrow_mut().remove(&client_id);
                return;
            };
            drain_client_requests(shard, sessions, system_config, queues, active, client_id).await;
        });
    })
}

/// Handler shard 0 runs for an inbound [`shard::MetadataSubmit`]: a peer
/// shard has verified credentials and owns the session locally, and asks
/// shard 0 (the metadata consensus owner) to run only the consensus
/// proposal. Spawns a task so the awaiting peer is woken once the op
/// commits; replies `None` on transient submit failure so the peer never
/// blocks forever.
pub(crate) fn make_metadata_submit_handler<B, MJ, S>(
    shard_handle: &ShellShardHandle<B, MJ, S>,
) -> shard::MetadataSubmitHandler
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
{
    let shard_handle = Rc::clone(shard_handle);
    Rc::new(move |submit| {
        let Some(shard) = upgrade_shard_handle(&shard_handle) else {
            return;
        };
        let bus = shard.bus.clone();
        bus.spawn(async move {
            match submit {
                shard::MetadataSubmit::Register {
                    vsr_client_id,
                    user_id,
                    reply,
                } => {
                    let session = shard
                        .plane
                        .metadata()
                        .submit_register_in_process(vsr_client_id, user_id)
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
        });
    })
}

fn enqueue_client_request<B, MJ, S>(
    shard: Rc<ShellShard<B, MJ, S>>,
    sessions: Rc<RefCell<SessionManager>>,
    system_config: Arc<NgSystemConfig>,
    queues: ClientRequestQueues,
    active: ActiveClientRequests,
    client_id: u128,
    message: Message<GenericHeader>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
{
    queues
        .borrow_mut()
        .entry(client_id)
        .or_default()
        .push_back(message);
    if !active.borrow_mut().insert(client_id) {
        return;
    }

    let bus = shard.bus.clone();
    bus.spawn(async move {
        drain_client_requests(shard, sessions, system_config, queues, active, client_id).await;
    });
}

#[allow(clippy::future_not_send)]
async fn drain_client_requests<B, MJ, S>(
    shard: Rc<ShellShard<B, MJ, S>>,
    sessions: Rc<RefCell<SessionManager>>,
    system_config: Arc<NgSystemConfig>,
    queues: ClientRequestQueues,
    active: ActiveClientRequests,
    client_id: u128,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
{
    loop {
        let Some(message) = pop_next_client_request(&queues, &active, client_id) else {
            return;
        };
        handle_client_request(&shard, &sessions, &system_config, client_id, message).await;
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
async fn handle_client_request<B, MJ, S>(
    shard: &Rc<ShellShard<B, MJ, S>>,
    sessions: &Rc<RefCell<SessionManager>>,
    system_config: &Arc<NgSystemConfig>,
    transport_client_id: u128,
    message: Message<iggy_binary_protocol::GenericHeader>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
{
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
        // MUST go through Register first, which binds the acting user the
        // per-op authz gates resolve.
        let nr_code = u32::from_le_bytes(request.header().reserved[..4].try_into().unwrap());
        // Legacy (pre-register) login codes. server-ng authenticates only via
        // the Register handshake (LOGIN_REGISTER / LOGIN_REGISTER_WITH_PAT,
        // Operation::Register); the vsr SDK funnels both logins there and never
        // emits these. Reject them uniformly with a typed MalformedLogin (the
        // SDK maps it to InvalidFormat) before the session gate, so a legacy or
        // foreign client fails fast instead of getting the misleading
        // NoSession eviction the pre-auth guard would send unbound, or the
        // silent empty-ok Reply the bound non-replicated path would send.
        if matches!(
            nr_code,
            LOGIN_USER_CODE | LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE
        ) {
            warn!(
                transport_client_id,
                code = nr_code,
                "rejecting legacy login code; server-ng requires the register handshake"
            );
            send_login_eviction(
                shard,
                transport_client_id,
                header.client,
                EvictionReason::MalformedLogin,
            )
            .await;
            return;
        }
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
        handle_non_replicated_request(shard, sessions, system_config, transport_client_id, request)
            .await;
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

    if header.operation.is_partition() {
        // `bound` is Some here (unbound transports returned above).
        let (vsr_client_id, bound_session) = bound.unwrap_or((0, 0));
        // `get_session` discards the acting user id the partition gate needs;
        // resolve it from the same bound connection. A bound transport always
        // has one, but the gate fails closed on `None` rather than trust that.
        let acting_user_id = sessions.borrow().get_user_id(transport_client_id);
        dispatch_partition_request(
            shard,
            request,
            vsr_client_id,
            bound_session,
            transport_client_id,
            acting_user_id,
        )
        .await;
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
    // Hash raw passwords and, for ChangePassword, verify the current password
    // on the primary before replication; see `crate::users`. Replicas store the
    // hash directly. A wrong current password is not denied here: it rides
    // consensus and applies as a committed InvalidCredentials no-op, so the only
    // Err returned is a malformed body.
    let request = match maybe_rewrite_user_password_request(shard, request) {
        Ok(rewritten) => rewritten,
        Err(error) => {
            // Malformed body: deny fast with InvalidCommand. A silent drop
            // would wedge every later request on the connection until the
            // socket read timeout.
            warn!(
                transport_client_id,
                error = %error,
                operation = ?header.operation,
                "denying user password request"
            );
            let commit = current_metadata_commit(shard);
            let reply = build_deny_reply(&header, transport_client_id, 0, commit, error.as_code());
            if let Err(send_error) = shard
                .bus
                .send_to_client(transport_client_id, reply.into_generic().into_frozen())
                .await
            {
                warn!(
                    transport_client_id,
                    error = %send_error,
                    "failed to send password-deny reply"
                );
            }
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
async fn handle_get_personal_access_tokens<B, MJ, S>(
    shard: &Rc<ShellShard<B, MJ, S>>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
    request: &Message<RequestHeader>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
{
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
async fn handle_get_me<B, MJ, S>(
    shard: &Rc<ShellShard<B, MJ, S>>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
    request: &Message<RequestHeader>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
{
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

/// Route a partition data-plane op (`SendMessages` / consumer-offset writes)
/// through the shard mesh by namespace: the op belongs to the partition's
/// own consensus group, not the metadata group. The owning shard's
/// partitions plane runs at-least-once consensus and replies directly via
/// `send_to_client`. `header.client` therefore stays the TRANSPORT id
/// (home-shard routing bits), not the VSR session id -- partition ops are
/// sessionless ("session lifecycle is metadata-only").
///
/// Callers must have authenticated the transport already: `vsr_client_id` /
/// `bound_session` come from its bound VSR session. Every failure before
/// dispatch replies empty so the client fails fast instead of wedging on a
/// silent drop.
///
/// `vsr_client_id` keys the consumer-group offset fence (the member id),
/// not the transport id stamped into the partition-op header.
#[allow(clippy::future_not_send)]
pub(crate) async fn dispatch_partition_request<B, MJ, S>(
    shard: &Rc<ShellShard<B, MJ, S>>,
    request: Message<RequestHeader>,
    vsr_client_id: u128,
    bound_session: u64,
    transport_client_id: u128,
    acting_user_id: Option<u32>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
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
    ) {
        warn!(
            transport_client_id,
            status,
            operation = ?header.operation,
            "partition request denied by authorization; replying with status"
        );
        send_partition_deny_reply(shard, transport_client_id, &header, status).await;
        return;
    }
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
        new_header.session = bound_session;
        new_header.request = new_header.request.max(1);
    });
    shard.dispatch(request.into_generic());
}

#[allow(clippy::future_not_send, clippy::too_many_lines)]
async fn handle_non_replicated_request<B, MJ, S>(
    shard: &Rc<ShellShard<B, MJ, S>>,
    sessions: &Rc<RefCell<SessionManager>>,
    system_config: &Arc<NgSystemConfig>,
    transport_client_id: u128,
    request: Message<RequestHeader>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
{
    const CODE_RANGE: std::ops::Range<usize> = 0..4;
    let code = u32::from_le_bytes(request.header().reserved[CODE_RANGE].try_into().unwrap());
    // Acting user for the read gates below, resolved once. `None` only on the
    // pre-auth path (PING / GET_CLUSTER_METADATA), which serves ungated codes;
    // the gated arms fail closed on it.
    let user_id = sessions.borrow().get_user_id(transport_client_id);
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
            if let Err(error) = authorize_uid(shard, user_id, Permissioner::get_clients) {
                send_non_replicated_deny(shard, &request, transport_client_id, error.as_code())
                    .await;
                return;
            }
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
            if let Err(error) = authorize_uid(shard, user_id, Permissioner::get_client) {
                send_non_replicated_deny(shard, &request, transport_client_id, error.as_code())
                    .await;
                return;
            }
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
        GET_SNAPSHOT_FILE_CODE => {
            handle_get_snapshot(shard, system_config, transport_client_id, &request, user_id).await;
        }
        POLL_MESSAGES_CODE => {
            handle_poll_messages(shard, transport_client_id, &request, user_id).await;
        }
        GET_CONSUMER_OFFSET_CODE => {
            handle_get_consumer_offset(shard, transport_client_id, &request, user_id).await;
        }
        SYNC_CONSUMER_GROUP_CODE => {
            // Self-scoped: serves the caller's own assignment keyed by the
            // header client id, so it carries no permissioner rule.
            handle_sync_consumer_group(shard, transport_client_id, &request).await;
        }
        _ => {
            let roster = sessions.borrow().cluster_roster();
            handle_default_non_replicated(
                shard,
                transport_client_id,
                code,
                &request,
                user_id,
                &roster,
            )
            .await;
        }
    }
}

#[allow(clippy::future_not_send)]
async fn handle_default_non_replicated<B, MJ, S>(
    shard: &Rc<ShellShard<B, MJ, S>>,
    transport_client_id: u128,
    code: u32,
    request: &Message<RequestHeader>,
    user_id: Option<u32>,
    roster: &ClusterRoster,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
{
    // Gate by command code before the shared builder runs. The builder stays
    // authz-free (it is byte-shared with the HTTP read path, which gates
    // separately); a denial replies status!=0 with an empty body.
    if let Err(error) = authorize_default_read(shard, code, request_body(request), user_id) {
        send_non_replicated_deny(shard, request, transport_client_id, error.as_code()).await;
        return;
    }
    match build_non_replicated_response(shard, code, request_body(request), user_id, roster) {
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
            // Surface the builder's typed error (unsupported op, undecodable
            // body, or a not-found parity read) on the same deny channel the
            // authz gate uses; a silent drop would wedge the client until its
            // read timeout.
            warn!(
                transport_client_id,
                code,
                error = %error,
                "denying non-replicated VSR request"
            );
            send_non_replicated_deny(shard, request, transport_client_id, error.as_code()).await;
        }
    }
}

/// Serve `GET_SNAPSHOT_FILE`: gate on the snapshot rule (`read_servers ||
/// manage_servers`, the legacy gate - the archive dumps host diagnostics, so
/// plain authentication must not suffice), then await the off-thread
/// collection (see `snapshot::collect`) and reply with the raw ZIP bytes.
#[allow(clippy::future_not_send)]
async fn handle_get_snapshot<B, MJ, S>(
    shard: &Rc<ShellShard<B, MJ, S>>,
    system_config: &Arc<NgSystemConfig>,
    transport_client_id: u128,
    request: &Message<RequestHeader>,
    user_id: Option<u32>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
{
    if let Err(error) = authorize_uid(shard, user_id, Permissioner::get_snapshot) {
        send_non_replicated_deny(shard, request, transport_client_id, error.as_code()).await;
        return;
    }
    let result = match decode_get_snapshot(request_body(request)) {
        Ok((compression, snapshot_types)) => {
            snapshot::collect(Arc::clone(system_config), compression, snapshot_types).await
        }
        Err(error) => Err(error),
    };
    match result {
        Ok(archive) => {
            // The reply frames as `[256-byte header][archive]`. The client's
            // `message_bus::read_message` rejects any frame past `MAX_MESSAGE_SIZE`
            // (64 MiB) by tearing the connection down untyped, and a frame past
            // `u32::MAX` would panic `build_reply_with_body`. The archive is the
            // only unbounded non-replicated body, so refuse an oversized one with a
            // typed error the SDK decodes. The HTTP path streams via `Body` (not
            // this framing), so it stays uncapped.
            let frame_size = HEADER_SIZE + archive.len();
            if frame_size > MAX_MESSAGE_SIZE {
                warn!(
                    transport_client_id,
                    frame_size,
                    max = MAX_MESSAGE_SIZE,
                    "snapshot archive exceeds the client frame limit; refusing to send"
                );
                send_non_replicated_deny(
                    shard,
                    request,
                    transport_client_id,
                    IggyError::SnapshotFileCompletionFailed.as_code(),
                )
                .await;
                return;
            }
            send_non_replicated_bytes(
                shard,
                request,
                transport_client_id,
                GetSnapshotResponse { data: archive }.to_bytes(),
                "get_snapshot",
            )
            .await;
        }
        Err(error) => {
            warn!(transport_client_id, error = %error, "denying snapshot request");
            send_non_replicated_deny(shard, request, transport_client_id, error.as_code()).await;
        }
    }
}

fn decode_get_snapshot(
    body: &[u8],
) -> Result<(SnapshotCompression, Vec<SystemSnapshotType>), IggyError> {
    let request = GetSnapshotRequest::decode_from(body).map_err(|_| IggyError::InvalidCommand)?;
    let compression = SnapshotCompression::from_code(request.compression)?;
    let snapshot_types = request
        .snapshot_types
        .iter()
        .map(|&code| SystemSnapshotType::from_code(code))
        .collect::<Result<Vec<_>, _>>()?;
    Ok((compression, snapshot_types))
}

/// Send a non-replicated reply body to a client, stamping the current
/// metadata commit. Shared by the `get_me` / `get_clients` / `get_client`
/// arms.
#[allow(clippy::future_not_send)]
async fn send_non_replicated_bytes<B, MJ, S>(
    shard: &Rc<ShellShard<B, MJ, S>>,
    request: &Message<RequestHeader>,
    transport_client_id: u128,
    bytes: Bytes,
    label: &'static str,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
{
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
async fn send_unauthenticated_eviction<B, MJ, S>(
    shard: &Rc<ShellShard<B, MJ, S>>,
    transport_client_id: u128,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
{
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
pub(crate) async fn run_heartbeat_verifier<B, MJ, S>(
    shard: Rc<ShellShard<B, MJ, S>>,
    sessions: Rc<RefCell<SessionManager>>,
    interval: std::time::Duration,
    stop_rx: shard::Receiver<()>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
{
    // Legacy `MAX_THRESHOLD`: a client is stale once it misses ~1.2 intervals.
    let max_age = interval.mul_f64(1.2);
    loop {
        if stop_rx.try_recv().is_ok() {
            break;
        }
        // Production-only wall clock: the heartbeat verifier is spawned solely
        // by `build_shard_for_thread`, never by the simulator's
        // `wire_shell_handlers`, so this read is off the deterministic path. If
        // it is ever driven under the deterministic executor, route it through
        // the bus sleep / injected clock (as the pump tick is) to stay virtual.
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
        shard.bus.sleep(interval).await;
    }
}

/// Evict one stale connection: drop its session (releasing consumer-group
/// membership through a replicated `Logout`) and notify the client with a
/// session-terminal `Eviction(StaleClient)`.
#[allow(clippy::future_not_send)]
async fn evict_stale_client<B, MJ, S>(
    shard: &Rc<ShellShard<B, MJ, S>>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
{
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
async fn handle_poll_messages<B, MJ, S>(
    shard: &Rc<ShellShard<B, MJ, S>>,
    transport_client_id: u128,
    request: &Message<RequestHeader>,
    user_id: Option<u32>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
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
                }) => build_polled_messages_body(
                    partition_id,
                    current_offset,
                    fragments,
                    shard.plane.partitions().config().encryptor.as_deref(),
                )
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
async fn handle_get_consumer_offset<B, MJ, S>(
    shard: &Rc<ShellShard<B, MJ, S>>,
    transport_client_id: u128,
    request: &Message<RequestHeader>,
    user_id: Option<u32>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
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
async fn handle_sync_consumer_group<B, MJ, S>(
    shard: &Rc<ShellShard<B, MJ, S>>,
    transport_client_id: u128,
    request: &Message<RequestHeader>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
{
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
async fn send_empty_partition_reply<B, MJ, S>(
    shard: &Rc<ShellShard<B, MJ, S>>,
    transport_client_id: u128,
    request_header: &RequestHeader,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
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

/// Wait (bounded) until `namespace` is routable: this shard's routing row
/// exists and the owning shard answers a probe read (partition
/// materialised). Fast path: row already present -> no probe, no wait.
///
/// Covers the post-`CreateTopic` convergence window where the metadata
/// commit has returned to the client but the per-shard reconcilers have
/// not yet seeded routing rows / materialised partitions.
#[allow(clippy::future_not_send)]
async fn wait_for_partition_routable<B, MJ, S>(
    shard: &Rc<ShellShard<B, MJ, S>>,
    namespace: IggyNamespace,
) -> bool
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
{
    const ATTEMPT_DELAY: std::time::Duration = std::time::Duration::from_millis(50);
    // 3s budget at 50ms per attempt. Counting attempts, not reading a
    // wall-clock deadline, keeps the wait virtual under the simulator: the
    // bus sleep advances virtual time, whereas `Instant::now` would not.
    const MAX_ATTEMPTS: u32 = 60;

    if shard.shards_table().shard_for(namespace).is_some() {
        return true;
    }
    let mut attempts = 0u32;
    while shard.shards_table().shard_for(namespace).is_none() {
        if attempts >= MAX_ATTEMPTS {
            return false;
        }
        attempts += 1;
        shard.bus.sleep(ATTEMPT_DELAY).await;
    }
    // The local row is seeded by THIS shard's reconciler; the owner
    // materialises the partition on its own pass. Probe with a cheap read
    // until the owner answers, so the write below normally clears the
    // owner's "partition not initialized" guard. Not a hard guarantee: the
    // partition can de-materialise between this probe and the dispatch, but
    // the park/tombstone path re-checks and the client retries.
    while attempts < MAX_ATTEMPTS {
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
                attempts += 1;
                shard.bus.sleep(ATTEMPT_DELAY).await;
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

pub(crate) type DecodedPollRequest = (IggyNamespace, u32, PollingConsumer, PollingArgs);

/// Resolve a decoded poll request into its owning-shard read: namespace,
/// partition, polling consumer, and args. Shared by the TCP dispatch (client
/// id = the connection's bound VSR client) and the HTTP route (client id 0,
/// which fences group polls closed).
#[allow(clippy::cast_possible_truncation)]
pub(crate) fn resolve_poll_request<B, MJ, S>(
    shard: &Rc<ShellShard<B, MJ, S>>,
    wire: &PollMessagesRequest,
    client_id: u128,
) -> Result<DecodedPollRequest, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
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
pub(crate) fn resolve_consumer_offset_request<B, MJ, S>(
    shard: &Rc<ShellShard<B, MJ, S>>,
    wire: &GetConsumerOffsetRequest,
) -> Result<(IggyNamespace, u32, PollingConsumer), IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
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
pub(crate) async fn submit_register_on_owner<B, MJ, S>(
    shard: &Rc<ShellShard<B, MJ, S>>,
    vsr_client_id: u128,
    user_id: u32,
) -> Result<u64, MetadataSubmitError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
{
    if shard.id == 0 {
        return shard
            .plane
            .metadata()
            .submit_register_in_process(vsr_client_id, user_id)
            .await;
    }
    let (reply, rx) = shard::channel::<Option<u64>>(1);
    shard.forward_metadata_submit(shard::MetadataSubmit::Register {
        vsr_client_id,
        user_id,
        reply,
    });
    match rx.recv().await {
        Ok(Some(session)) => Ok(session),
        _ => Err(MetadataSubmitError::Canceled),
    }
}

/// Logout counterpart of [`submit_register_on_owner`].
#[allow(clippy::future_not_send)]
pub(crate) async fn submit_logout_on_owner<B, MJ, S>(
    shard: &Rc<ShellShard<B, MJ, S>>,
    vsr_client_id: u128,
    session: u64,
    request: u64,
) -> Result<u64, MetadataSubmitError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
{
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
/// The consensus reply is forwarded verbatim: nothing-to-delete commits a
/// no-op `TruncatePartition(0)` and acks, while a not-primary rejection
/// reaches the client as `TransientNotCommitted` so the SDK replays instead
/// of mistaking a dropped delete for success. Only a malformed / unresolvable
/// request is acked empty without a commit.
#[allow(clippy::future_not_send)]
async fn handle_delete_segments_request<B, MJ, S>(
    shard: &Rc<ShellShard<B, MJ, S>>,
    transport_client_id: u128,
    bound: Option<(u128, u64)>,
    request: &Message<RequestHeader>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
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
    // keeping the sequence contiguous. Skipping the commit (or attributing it
    // to an internal id) leaves a hole that fails the next metadata op's
    // `request == committed + 1` preflight -> RequestGap -> silent drop -> the
    // SDK blocks until timeout. A no-op delete still commits `up_to_offset = 0`
    // (monotonic apply) for the same reason.
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
pub(crate) async fn resolve_delete_segments_truncate<B, MJ, S>(
    shard: &Rc<ShellShard<B, MJ, S>>,
    template: &RequestHeader,
    client_id: u128,
    session: u64,
    body: &[u8],
) -> Result<Message<RequestHeader>, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
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
        // result. That keeps the request sequence contiguous (an empty ack
        // without a commit gaps `request == committed + 1` and silently
        // drops the NEXT metadata op) while the client gets the typed error.
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
fn submit_disconnect_logout<B, MJ, S>(
    shard: Rc<ShellShard<B, MJ, S>>,
    vsr_client_id: u128,
    session: u64,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
{
    // Synthetic request id: header validation rejects `request == 0` for
    // non-register ops, and a disconnect has no client-issued request id.
    // The logout apply keys on (client, session) only, so any non-zero id
    // is valid here.
    const DISCONNECT_LOGOUT_REQUEST_ID: u64 = u64::MAX;
    let bus = shard.bus.clone();
    bus.spawn(async move {
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
/// `None` = transient submit failure (SDK read-timeout replays).
#[allow(clippy::future_not_send)]
pub(crate) async fn submit_client_request_on_owner<B, MJ, S>(
    shard: &Rc<ShellShard<B, MJ, S>>,
    request: Message<RequestHeader>,
) -> Option<Message<GenericHeader>>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
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

#[allow(clippy::future_not_send)]
async fn handle_logout_request<B, MJ, S>(
    shard: &Rc<ShellShard<B, MJ, S>>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
    request: Message<RequestHeader>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
{
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

fn ensure_transport_connection<B, MJ, S>(
    shard: &Rc<ShellShard<B, MJ, S>>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
{
    let Some(meta) = shard.bus.client_meta(transport_client_id) else {
        return;
    };
    sessions
        .borrow_mut()
        .ensure_connection(transport_client_id, meta.peer_addr, meta.transport);
}

#[allow(clippy::future_not_send, clippy::too_many_lines)]
async fn handle_login_register_request<B, MJ, S>(
    shard: &Rc<ShellShard<B, MJ, S>>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
    request: Message<RequestHeader>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
{
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
async fn send_login_eviction<B, MJ, S>(
    shard: &Rc<ShellShard<B, MJ, S>>,
    transport_client_id: u128,
    vsr_client_id: u128,
    reason: EvictionReason,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
{
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

pub(crate) fn upgrade_shard_handle<B, MJ, S>(
    shard_handle: &ShellShardHandle<B, MJ, S>,
) -> Option<Rc<ShellShard<B, MJ, S>>>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
{
    shard_handle
        .borrow()
        .as_ref()
        .and_then(std::rc::Weak::upgrade)
}

#[cfg(test)]
mod tests {
    use super::*;
    use consensus::{LocalPipeline, Plane as _, PlaneKind, VsrConsensus};
    use iggy_binary_protocol::requests::streams::CreateStreamRequest;
    use iggy_binary_protocol::{PrepareOkHeader, ReplyHeader};
    use iggy_common::variadic;
    use journal::prepare_journal::PrepareJournal;
    use message_bus::client_listener::RequestHandler;
    use message_bus::fd_transfer::DupedFd;
    use message_bus::installer::ConnectionInstaller;
    use message_bus::installer::conn_info::{ClientConnMeta, ClientTransportKind};
    use message_bus::replica::listener::MessageHandler;
    use message_bus::{
        ClientConnectionLostFn, ClientForwardFn, ConnectionLostFn, JoinHandle, MessageBus,
        ReplicaForwardFn, ReplicaHandshakeDoneFn, SendError,
    };
    use metadata::impls::metadata::IggySnapshot;
    use metadata::stm::stream::Streams;
    use metadata::stm::user::Users;
    use metadata::{IggyMetadata, MuxStateMachine};
    use partitions::{IggyPartitions, PartitionsConfig};
    use server_common::iobuf::Frozen;
    use server_common::sharding::ShardId;
    use server_common::{MESSAGE_ALIGN, Message};
    use shard::shards_table::PapayaShardsTable;
    use shard::{IggyShard, PartitionConsensusConfig, ReplicaTopology, ShardIdentity};
    use std::cell::RefCell;
    use std::future::Future;
    use std::mem::size_of;
    use std::rc::Rc;

    type TestMux = MuxStateMachine<variadic!(Users, Streams)>;
    type TestShard = IggyShard<SpyBus, PrepareJournal, IggySnapshot, TestMux, PapayaShardsTable>;

    /// Records every client-bound reply instead of writing to a socket;
    /// everything else is a no-op. The two `ShellBus` halves are stubbed.
    #[derive(Debug, Clone, Default)]
    struct SpyBus {
        client_replies: Rc<RefCell<Vec<u128>>>,
    }

    #[allow(clippy::future_not_send)]
    impl MessageBus for SpyBus {
        fn track_background(&self, _handle: JoinHandle<()>) {}
        async fn send_to_client(
            &self,
            client_id: u128,
            _data: Frozen<MESSAGE_ALIGN>,
        ) -> Result<(), SendError> {
            self.client_replies.borrow_mut().push(client_id);
            Ok(())
        }
        async fn send_to_replica(
            &self,
            _replica: u8,
            _data: Frozen<MESSAGE_ALIGN>,
        ) -> Result<(), SendError> {
            Ok(())
        }
        fn set_connection_lost_fn(&self, _f: ConnectionLostFn) {}
        fn set_replica_forward_fn(&self, _f: ReplicaForwardFn) {}
        fn set_client_forward_fn(&self, _f: ClientForwardFn) {}
    }

    impl ConnectionInstaller for SpyBus {
        fn install_replica_inbound_fd(
            &self,
            _fd: DupedFd,
            _on_message: MessageHandler,
            _on_done: ReplicaHandshakeDoneFn,
        ) {
        }
        fn install_replica_outbound_fd(
            &self,
            _fd: DupedFd,
            _replica_id: u8,
            _on_message: MessageHandler,
            _on_done: ReplicaHandshakeDoneFn,
        ) {
        }
        fn release_replica_handshake_slot(&self, _slot: u64) {}
        fn clear_replica_dial_pending(&self, _replica_id: u8) {}
        fn install_client_fd(
            &self,
            _fd: DupedFd,
            _meta: ClientConnMeta,
            _on_request: RequestHandler,
        ) {
        }
        fn install_client_ws_fd(
            &self,
            _fd: DupedFd,
            _meta: ClientConnMeta,
            _on_request: RequestHandler,
        ) {
        }
        fn client_meta(&self, _client_id: u128) -> Option<Rc<ClientConnMeta>> {
            None
        }
        fn set_client_connection_lost_fn(&self, _f: ClientConnectionLostFn) {}
    }

    /// Minimal committed `Register` reply for `ClientTable::commit_register`
    /// (reads only `client` and `commit`).
    fn register_reply(client: u128, session: u64) -> Message<ReplyHeader> {
        let header_size = size_of::<ReplyHeader>();
        let mut reply = Message::<ReplyHeader>::new(header_size);
        let header = bytemuck::checked::try_from_bytes_mut::<ReplyHeader>(
            &mut reply.as_mut_slice()[..header_size],
        )
        .expect("zeroed bytes are a valid ReplyHeader");
        *header = ReplyHeader {
            client,
            request: 0,
            commit: session,
            command: Command2::Reply,
            operation: Operation::Register,
            ..Default::default()
        };
        reply
    }

    fn request_message(
        operation: Operation,
        client: u128,
        session: u64,
        request: u64,
        body: &[u8],
    ) -> Message<RequestHeader> {
        let header_size = size_of::<RequestHeader>();
        let total = header_size + body.len();
        let mut message = Message::<RequestHeader>::new(total);
        {
            let slice = message.as_mut_slice();
            slice[header_size..total].copy_from_slice(body);
            let header =
                bytemuck::checked::from_bytes_mut::<RequestHeader>(&mut slice[..header_size]);
            *header = RequestHeader {
                command: Command2::Request,
                operation,
                size: u32::try_from(total).expect("test request fits u32"),
                client,
                session,
                request,
                user_id: 0,
                namespace: server_common::sharding::METADATA_CONSENSUS_NAMESPACE,
                ..Default::default()
            };
        }
        message
    }

    /// Raw prepare for the sibling op, standing in for the crate-private
    /// `prepare_request` projection. `user_id` 0 skips the in-apply RBAC
    /// gate (server-originated convention), so the op applies cleanly.
    fn prepare_message(
        operation: Operation,
        client: u128,
        request: u64,
        body: &[u8],
    ) -> Message<PrepareHeader> {
        let header_size = size_of::<PrepareHeader>();
        let total = header_size + body.len();
        let mut message = Message::<PrepareHeader>::new(total);
        {
            let slice = message.as_mut_slice();
            slice[header_size..total].copy_from_slice(body);
            let header =
                bytemuck::checked::from_bytes_mut::<PrepareHeader>(&mut slice[..header_size]);
            *header = PrepareHeader {
                command: Command2::Prepare,
                operation,
                size: u32::try_from(total).expect("test prepare fits u32"),
                op: 1,
                view: 0,
                client,
                request,
                user_id: 0,
                checksum: 42,
                namespace: server_common::sharding::METADATA_CONSENSUS_NAMESPACE,
                ..Default::default()
            };
        }
        message
    }

    /// Regression test for the production failure chain "CLI stream
    /// create succeeded, logout failed: Disconnected".
    ///
    /// Why the logout of a CLI invocation used to fail during ITS OWN
    /// successful `stream create`: the catch-up gate was GLOBAL. The suite
    /// runs many CLI invocations against one shared single-node server;
    /// each one is three replicated ops (Register, work, Logout). When
    /// THIS client's logout frame arrived, some SIBLING client's op was
    /// regularly sitting between quorum-ack (`commit_max` advanced inside
    /// `on_ack`) and apply (`commit_min` still behind, driver parked at
    /// the journal read). `submit_logout_in_process` then rejected
    /// `NotCaughtUp`, and `handle_logout_request` swallowed the error: no
    /// reply frame, session left bound. A one-shot CLI saw only a dead
    /// connection — "Problem with server logout / Disconnected" — and
    /// exited non-zero although its create committed; the harness retry
    /// then tripped "already exists".
    ///
    /// This test rebuilds that interleaving deterministically (client B =
    /// the sibling parked mid-commit; client A = the CLI logging out) and
    /// pins the contract that fixed it (non-register ops carry no
    /// catch-up gate, see `submit_logout_in_process`):
    ///
    ///   a client-initiated logout must always produce a reply frame and
    ///   unbind the transport session, even while a sibling's commit is
    ///   in flight — the logout simply pipelines behind it.
    #[compio::test]
    async fn logout_rejected_by_closed_gate_must_still_reply_to_client() {
        const CLIENT_A: u128 = 1;
        const CLIENT_B: u128 = 2;
        const SESSION: u64 = 1;
        const ACTING_USER: u32 = 7;
        const TRANSPORT_A: u128 = 77;

        let dir = tempfile::tempdir().unwrap();
        let journal = PrepareJournal::open(&dir.path().join("journal.wal"), 0)
            .await
            .unwrap();
        let bus = SpyBus::default();
        let consensus = VsrConsensus::new(
            1,
            0,
            1,
            server_common::sharding::METADATA_CONSENSUS_NAMESPACE,
            bus.clone(),
            LocalPipeline::new(),
        );
        consensus.init();
        let metadata: IggyMetadata<_, PrepareJournal, IggySnapshot, TestMux> = IggyMetadata::new(
            Some(consensus),
            Some(journal),
            None,
            TestMux::default(),
            None,
        );
        let partitions = IggyPartitions::new(
            ShardId::new(0),
            PartitionsConfig {
                messages_required_to_save: 1,
                size_of_messages_required_to_save: iggy_common::IggyByteSize::from(1024_u64),
                enforce_fsync: false,
                segment_size: iggy_common::IggyByteSize::from(1_048_576_u64),
                encryptor: None,
            },
        );
        let shard = Rc::new(TestShard::without_inbox(
            ShardIdentity::new(0, "logout-window-test".to_string()),
            bus.clone(),
            metadata,
            partitions,
            PapayaShardsTable::new(),
            PartitionConsensusConfig::new(1, ReplicaTopology::new(0, 1), bus.clone()),
        ));
        let md = shard.plane.metadata();
        let consensus = md.consensus.as_ref().unwrap();

        // A and B hold committed sessions (as after their CLI logins).
        for client in [CLIENT_A, CLIENT_B] {
            md.client_table.borrow_mut().commit_register(
                client,
                ACTING_USER,
                register_reply(client, SESSION),
                |_| false,
            );
        }
        // A's transport connection, authenticated + bound — the state a
        // CLI connection is in right after its create-stream reply.
        let sessions = Rc::new(RefCell::new(SessionManager::new()));
        sessions.borrow_mut().ensure_connection(
            TRANSPORT_A,
            "127.0.0.1:34567".parse().unwrap(),
            ClientTransportKind::Tcp,
        );
        sessions
            .borrow_mut()
            .login(TRANSPORT_A, ACTING_USER)
            .unwrap();
        sessions
            .borrow_mut()
            .bind_session(TRANSPORT_A, CLIENT_A, SESSION)
            .unwrap();

        // Sibling B's op: prepared, journaled, self-acked through the real
        // replicate path. (The public submit API cannot be used to open
        // the window: `dispatch_prepare_and_await` pumps its own loopback
        // inline, committing before it returns. Production's window is a
        // sibling submit task parked INSIDE `on_ack`'s awaits — modeled
        // below by driving `on_ack` by hand.)
        let create_body = CreateStreamRequest {
            name: iggy_binary_protocol::primitives::identifier::WireName::new("s1").unwrap(),
        }
        .to_bytes();
        let prepare = prepare_message(Operation::CreateStream, CLIENT_B, 1, &create_body);
        consensus.pipeline_message(PlaneKind::Metadata, &prepare);
        md.on_replicate(prepare).await;
        let mut loopback = Vec::new();
        consensus.drain_loopback_into(&mut loopback);
        let ack = loopback
            .pop()
            .expect("one self-ack per replicated prepare")
            .try_into_typed::<PrepareOkHeader>()
            .expect("loopback holds self PrepareOks");

        // Open the window: first poll of `on_ack` advances commit_max at
        // quorum, then parks at the journal read — commit_min unchanged.
        // Every production NotCaughtUp logout was submitted exactly here.
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        let mut driver = Box::pin(md.on_ack(ack));
        assert!(
            driver.as_mut().poll(&mut cx).is_pending(),
            "driver must park mid-commit at the journal read"
        );
        assert_eq!(consensus.commit_max(), 1);
        assert_eq!(consensus.commit_min(), 0);

        // A's logout lands in the window, through the real dispatch path.
        let logout = request_message(Operation::Logout, CLIENT_A, SESSION, 2, &[]);
        handle_logout_request(&shard, &sessions, TRANSPORT_A, logout).await;

        // DESIRED CONTRACT (red on current code): the client must never be
        // left in silence — that silence is what a one-shot CLI reports as
        // "Problem with server logout / Disconnected".
        assert!(
            bus.client_replies.borrow().contains(&TRANSPORT_A),
            "logout must produce a reply frame to the client even while the \
             catch-up gate is closed (silence = CLI 'Disconnected', exit 1)"
        );
        assert_eq!(
            sessions.borrow().get_session(TRANSPORT_A),
            None,
            "transport session must be unbound by a client-initiated logout; \
             the VSR slot may lapse to the eviction sweep"
        );
    }
}
