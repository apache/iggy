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
    complete_login_register, surface_login_failure, verify_login_credentials,
    verify_pat_credentials,
};
use crate::bootstrap::{ShellBus, ShellShard, ShellShardHandle};
use crate::cluster_meta::ClusterRoster;
use crate::consumer_group::{
    maybe_rewrite_consumer_group_request, maybe_rewrite_consumer_offset_request,
};
use crate::dispatch::authz::{
    authorize_default_read, authorize_partition_op, authorize_partition_read, authorize_uid,
    send_deny_reply, send_non_replicated_deny, send_unbound_deny_reply,
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
use crate::segment_cleaner::UNENFORCEABLE_TOPIC_SIZE_WARN;
use crate::session_manager::SessionManager;
use crate::snapshot;
use crate::users::maybe_rewrite_user_password_request;
use crate::wire::{request_body, usize_to_u32, verify_request_checksum};
use bytes::Bytes;
use configs::server::ServerSystemConfig;
use consensus::{
    Consensus, DISCONNECT_LOGOUT_REQUEST_ID, EvictionContext, MetadataHandle, PartitionsHandle,
    build_eviction_message, build_incompatible_protocol_eviction_message,
    build_result_rejection_reply,
};
use iggy_binary_protocol::PrepareHeader;
use iggy_binary_protocol::codes::{
    GET_CLIENT_CODE, GET_CLIENTS_CODE, GET_CLUSTER_METADATA_CODE, GET_CONSUMER_OFFSET_CODE,
    GET_ME_CODE, GET_PERSONAL_ACCESS_TOKENS_CODE, GET_SNAPSHOT_FILE_CODE, GET_STATS_CODE,
    LOGIN_USER_CODE, LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE, PING_CODE, POLL_MESSAGES_CODE,
    SYNC_CONSUMER_GROUP_CODE,
};
use iggy_binary_protocol::primitives::consumer::WireConsumer;
use iggy_binary_protocol::primitives::polling_strategy::WirePollingStrategy;
use iggy_binary_protocol::requests::consumer_groups::SyncConsumerGroupRequest;
use iggy_binary_protocol::requests::consumer_offsets::{
    GetConsumerOffsetRequest, StoreConsumerOffsetRequest,
};
use iggy_binary_protocol::requests::messages::PollMessagesRequest;
use iggy_binary_protocol::requests::partitions::{
    CreatePartitionsRequest, DeletePartitionsRequest,
};
use iggy_binary_protocol::requests::segments::DeleteSegmentsRequest;
use iggy_binary_protocol::requests::streams::{CreateStreamRequest, UpdateStreamRequest};
use iggy_binary_protocol::requests::system::get_client::GetClientRequest;
use iggy_binary_protocol::requests::system::get_snapshot::GetSnapshotRequest;
use iggy_binary_protocol::requests::topics::{CreateTopicRequest, UpdateTopicRequest};
use iggy_binary_protocol::requests::users::{
    CreateUserRequest, LoginRegisterRequest, LoginRegisterWithPatRequest, UpdateUserRequest,
};
use iggy_binary_protocol::responses::clients::client_response::ConsumerGroupInfoResponse;
use iggy_binary_protocol::responses::clients::get_client::ClientDetailsResponse;
use iggy_binary_protocol::responses::clients::get_clients::GetClientsResponse;
use iggy_binary_protocol::responses::consumer_groups::SyncConsumerGroupResponse;
use iggy_binary_protocol::responses::system::get_snapshot::GetSnapshotResponse;
use iggy_binary_protocol::{
    AckLevel, ClientVersionInfo, Command, ConsensusHeader, EvictionReason, ForwardLogoutHeader,
    ForwardLogoutOutcome, ForwardLogoutResultHeader, ForwardRegisterHeader, ForwardRegisterOutcome,
    ForwardRegisterResultHeader, GenericHeader, HEADER_SIZE, KIND_CONSUMER_GROUP,
    MAX_PARTITIONS_PER_REQUEST, Operation, ProtocolVersion, RequestHeader, RoutedRequestHeader,
    WireDecode, WireEncode, WireIdentifier, WireOptions, is_protocol_compatible,
};
use iggy_common::{
    IggyByteSize, IggyError, MaxTopicSize, PollingStrategy, SnapshotCompression,
    SystemSnapshotType, TopicCreateOptions, UPDATABLE_STREAM_OPTION_KEYS,
    UPDATABLE_TOPIC_OPTION_KEYS, UPDATABLE_USER_OPTION_KEYS, validate_preallocated_topic_bytes,
    validate_topic_segment_size,
};
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use message_bus::AUTO_COMMIT_CLIENT_ID;
use message_bus::client_listener::RequestHandler;
use message_bus::framing::MAX_MESSAGE_SIZE;
use message_bus::replica::listener::MessageHandler;
use metadata::impls::metadata::{
    BoundSession, MetadataSubmitError, StreamsFrontend, build_truncate_partition_client_message,
    build_truncate_partition_client_message_with_identifiers,
};
use metadata::permissioner::Permissioner;
use metadata::stm::stream::Streams;
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
use std::net::IpAddr;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

pub(crate) type ClientRequestQueues = Rc<RefCell<HashMap<u128, VecDeque<Message<GenericHeader>>>>>;
pub(crate) type ActiveClientRequests = Rc<RefCell<HashSet<u128>>>;

pub(crate) fn make_client_request_handler<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    sessions: &Rc<RefCell<SessionManager>>,
    system_config: Arc<ServerSystemConfig>,
    max_tokens_per_user: u32,
) -> RequestHandler
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
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
            max_tokens_per_user,
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
pub(crate) fn make_partition_read_handler<B, MJ, S, SB>(
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
                // The partition plane is sessionless (no `ClientTable` dedup); a
                // nonzero session + request just satisfy the wire header
                // validation.
                session: 1,
                request: 1,
                group: namespace.inner(),
                ..Default::default()
            };
        }),
    )
}

pub(crate) fn make_deferred_replica_message_handler<B, MJ, S, SB>(
    shard_handle: &ShellShardHandle<B, MJ, S, SB>,
) -> MessageHandler
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let shard_handle = Rc::clone(shard_handle);
    Rc::new(move |_replica_id, message| {
        if let Some(shard) = upgrade_shard_handle(&shard_handle) {
            shard.dispatch(message);
        }
    })
}

pub(crate) fn make_deferred_client_request_handler<B, MJ, S, SB>(
    bus: &B,
    shard_handle: &ShellShardHandle<B, MJ, S, SB>,
    sessions: &Rc<RefCell<SessionManager>>,
    system_config: Arc<ServerSystemConfig>,
    max_tokens_per_user: u32,
) -> RequestHandler
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
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
            drain_client_requests(
                shard,
                sessions,
                system_config,
                max_tokens_per_user,
                queues,
                active,
                client_id,
            )
            .await;
        });
    })
}

/// Handler shard 0 runs for an inbound [`shard::MetadataSubmit`]: a peer
/// shard has verified credentials and owns the session locally, and asks
/// shard 0 (the metadata consensus owner) to run only the consensus
/// proposal. Spawns a task so the awaiting peer is woken once the op
/// commits. Submit failures are returned verbatim so the peer can preserve
/// unknown-outcome retry semantics.
pub(crate) fn make_metadata_submit_handler<B, MJ, S, SB>(
    shard_handle: &ShellShardHandle<B, MJ, S, SB>,
) -> shard::MetadataSubmitHandler
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
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
                    let bound =
                        submit_register_local_or_forward(&shard, vsr_client_id, user_id).await;
                    let _ = reply.try_send(bound);
                }
                shard::MetadataSubmit::ForwardedRegister {
                    vsr_client_id,
                    user_id,
                    nonce,
                    origin_replica,
                } => {
                    answer_forwarded_register(
                        &shard,
                        vsr_client_id,
                        user_id,
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
                        submit_logout_local_or_forward(&shard, vsr_client_id, session, request)
                            .await;
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

// Session resume is performed BY THE LOGIN PATH, not by a separate
// credential-free rebind.
//
// A reconnecting client re-authenticates on the new connection and presents
// its previous `client_id` in the login frame; `submit_register_in_process`
// finds the existing table entry, verifies the authenticated user owns it,
// and returns its epoch, so `bind_session` binds the new transport to the
// old entry with its watermark and reply ring intact. That IS the resume.
//
// An earlier revision instead rebound an *unbound* transport straight from
// the table whenever a replicated frame carried a matching
// `(client, session)`, treating that pair as a bearer token. That was wrong
// in four ways, and the combination was a pre-auth session takeover:
//
//   - it called `SessionManager::login` itself, so no credential was ever
//     presented, and the connection was logged in as the entry's cached
//     `user_id`; authority for replicated ops then resolves from the table
//     (`resolve_acting_user_id`) and for partition ops from the session
//     manager, so BOTH planes ran as the original registrant;
//   - the pair carries far less entropy than "client-generated random
//     u128" implies: HTTP mints `client_id` from the shard-0 sequential
//     counter (`mint_shard_zero_client_id`, seeded at 1 per process) and no
//     live path ever bumps an epoch past 1, so the token was `client=N,
//     session=1` for small N;
//   - `ClientEntry` carries no transport or plane tag, so a raw TCP peer
//     could bind an HTTP-originated session;
//   - `bind_session` demotes the evicted holder to `Connected`, the one
//     state `login` accepts, so the loser's next replicated frame
//     re-resumed and stole the session back, unbounded and with no eviction
//     frame either way.
//
// Routing resume through login also restores the checks that path owns:
// password / PAT verification, `UserStatus::Active`, PAT expiry, the
// protocol-version gate, and SDK-info recording.
//
// An unbound transport sending a replicated frame therefore gets the typed
// `Eviction(NoSession)` fail-fast below and must log in.

#[allow(clippy::too_many_arguments)]
fn enqueue_client_request<B, MJ, S, SB>(
    shard: Rc<ShellShard<B, MJ, S, SB>>,
    sessions: Rc<RefCell<SessionManager>>,
    system_config: Arc<ServerSystemConfig>,
    max_tokens_per_user: u32,
    queues: ClientRequestQueues,
    active: ActiveClientRequests,
    client_id: u128,
    message: Message<GenericHeader>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
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
        drain_client_requests(
            shard,
            sessions,
            system_config,
            max_tokens_per_user,
            queues,
            active,
            client_id,
        )
        .await;
    });
}

#[allow(clippy::future_not_send)]
async fn drain_client_requests<B, MJ, S, SB>(
    shard: Rc<ShellShard<B, MJ, S, SB>>,
    sessions: Rc<RefCell<SessionManager>>,
    system_config: Arc<ServerSystemConfig>,
    max_tokens_per_user: u32,
    queues: ClientRequestQueues,
    active: ActiveClientRequests,
    client_id: u128,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    loop {
        let Some(message) = pop_next_client_request(&queues, &active, client_id) else {
            return;
        };
        handle_client_request(
            &shard,
            &sessions,
            &system_config,
            max_tokens_per_user,
            client_id,
            message,
        )
        .await;
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

/// Per-request partitions-count cap, shared by create-topic, create-partitions
/// and delete-partitions admission. Runs pre-consensus like
/// [`validate_topic_bounds`]: an oversized count must not burn a replicated
/// log entry (create-partitions admission would also allocate that many
/// consensus-group ids before replicating).
///
/// Zero passes here because a zero-partition TOPIC is legal (legacy
/// `create_topic` admits `0..=MAX`); the add/remove requests reject it in
/// [`validate_partitions_change_count`].
pub(crate) const fn validate_partitions_count(partitions_count: u32) -> Result<(), IggyError> {
    if partitions_count > MAX_PARTITIONS_PER_REQUEST {
        return Err(IggyError::TooManyPartitions);
    }
    Ok(())
}

/// [`validate_partitions_count`] plus the zero rejection that create-partitions
/// and delete-partitions carry: adding or removing zero partitions is a no-op
/// that would still burn a replicated log entry, bump `Streams::revision` and
/// force every shard through a rebalance pass. Legacy rejects it with
/// `TooManyPartitions` in both handlers (`1..=MAX` on create, `== 0` on
/// delete), so the code matches rather than inventing a new one.
pub(crate) const fn validate_partitions_change_count(
    partitions_count: u32,
) -> Result<(), IggyError> {
    if partitions_count == 0 {
        return Err(IggyError::TooManyPartitions);
    }
    validate_partitions_count(partitions_count)
}

/// Static create-topic bounds shared by the TCP and HTTP ingresses. Runs
/// pre-consensus: a rejected request must not burn a replicated log entry,
/// and `prepare_request` errors evict the session instead of denying typed.
/// `ServerDefault` is exempt from the size floor (it resolves against server
/// config at admission, matching legacy); `Unlimited` passes numerically.
/// `segment_size_bytes` is the topic's RESOLVED segment size (explicit
/// option, else this node's default), so a per-topic segment above the
/// global default still floors the topic cap.
pub(crate) fn validate_topic_bounds(
    partitions_count: u32,
    max_topic_size: MaxTopicSize,
    segment_size_bytes: u64,
) -> Result<(), IggyError> {
    validate_partitions_count(partitions_count)?;
    validate_topic_size_floor(max_topic_size, segment_size_bytes)
}

/// A topic cap below one segment can never be enforced: the first segment
/// already exceeds it. Split out of [`validate_topic_bounds`] because update
/// admission checks the cap without a partitions count to check.
pub(crate) fn validate_topic_size_floor(
    max_topic_size: MaxTopicSize,
    segment_size_bytes: u64,
) -> Result<(), IggyError> {
    if !matches!(max_topic_size, MaxTopicSize::ServerDefault)
        && max_topic_size.as_bytes_u64() < segment_size_bytes
    {
        return Err(IggyError::InvalidTopicSize(
            max_topic_size,
            IggyByteSize::from(segment_size_bytes),
        ));
    }
    Ok(())
}

/// Announce an accepted `max_topic_size` the server cannot enforce as written.
///
/// [`validate_topic_size_floor`] admits any cap of one segment or more, but
/// retention runs PER PARTITION and floors each partition's share at one SEALED
/// segment, which reaches up to one maximum bus frame past `segment_size`. A cap
/// between the two is stored and echoed back verbatim while the server actually
/// keeps `(segment_size + max_message_size) * partitions_count`, so the only
/// moment an operator can be told is the one where they set it.
///
/// Warns rather than rejects: which caps are accepted is client-visible wire
/// behavior, and tightening it would break topics that already exist.
pub(crate) fn warn_unenforceable_topic_size(
    max_topic_size: MaxTopicSize,
    segment_size_bytes: u64,
    max_message_size_bytes: usize,
    partitions_count: u32,
) {
    let MaxTopicSize::Custom(configured) = max_topic_size else {
        return;
    };
    let max_message_size_bytes = u64::try_from(max_message_size_bytes).unwrap_or(u64::MAX);
    let per_partition_floor = segment_size_bytes.saturating_add(max_message_size_bytes);
    let topic_floor = per_partition_floor.saturating_mul(u64::from(partitions_count));
    if configured.as_bytes_u64() >= topic_floor {
        return;
    }
    warn!(
        max_topic_size = configured.as_bytes_u64(),
        partitions_count,
        segment_size = segment_size_bytes,
        enforced_per_partition = per_partition_floor,
        "{UNENFORCEABLE_TOPIC_SIZE_WARN}"
    );
}

/// Announce the same unenforceable cap when partitions are ADDED to a topic.
///
/// The cap is topic-wide but enforcement is per partition, so every added
/// partition shrinks the share: a cap that cleared the floor when the topic was
/// created can stop clearing it here. The request carries only the delta, so
/// the stored cap, segment size and current partition count come from metadata.
pub(crate) fn warn_unenforceable_topic_size_on_partition_add(
    streams: &Streams,
    stream_id: &WireIdentifier,
    topic_id: &WireIdentifier,
    max_message_size_bytes: usize,
    added_partitions_count: u32,
) {
    let Some(((stream_slab, topic_slab), _)) = streams.partition_count_context(stream_id, topic_id)
    else {
        return;
    };
    let Some((_, max_topic_size, partitions_count, segment_size)) =
        streams.topic_retention_config(stream_slab, topic_slab)
    else {
        return;
    };
    warn_unenforceable_topic_size(
        max_topic_size,
        segment_size.map_or(iggy_common::DEFAULT_SEGMENT_SIZE, |segment_size| {
            segment_size.as_bytes_u64()
        }),
        max_message_size_bytes,
        u32::try_from(partitions_count)
            .unwrap_or(u32::MAX)
            .saturating_add(added_partitions_count),
    );
}

/// Reject option keys outside the resource's catalog, pre-consensus. Unknown
/// keys are rejected rather than skipped: a silently ignored knob would hand
/// the client server defaults without it ever learning. Streams and users
/// have no catalog keys yet, so `known` is empty for both until one lands.
pub(crate) fn validate_option_keys(options: &WireOptions, known: &[&str]) -> Result<(), IggyError> {
    for entry in options {
        // Wire validation already enforced UTF-8 string keys.
        let key = String::from_utf8_lossy(entry.key);
        if !known.contains(&key.as_ref()) {
            return Err(IggyError::UnsupportedOptionKey(key.into_owned()));
        }
    }
    Ok(())
}

/// Reject a request before it reaches consensus: warn, then send the typed
/// deny reply. A silent drop would wedge every later request on the
/// connection until the socket read timeout. `context` labels the rejection
/// site in both log lines.
#[allow(clippy::future_not_send)]
async fn send_pre_consensus_deny<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    header: &RoutedRequestHeader,
    transport_client_id: u128,
    error: &IggyError,
    context: &'static str,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    warn!(
        transport_client_id,
        error = %error,
        operation = ?header.operation,
        context,
        "denying request pre-consensus"
    );
    let commit = current_metadata_commit(shard);
    let reply = build_deny_reply(header, transport_client_id, 0, commit, error.as_code());
    if let Err(send_error) = shard
        .bus
        .send_to_client(transport_client_id, reply.into_generic().into_frozen())
        .await
    {
        warn!(
            transport_client_id,
            error = %send_error,
            context,
            "failed to send pre-consensus deny reply"
        );
    }
}

#[allow(clippy::future_not_send, clippy::too_many_lines)]
async fn handle_client_request<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    sessions: &Rc<RefCell<SessionManager>>,
    system_config: &Arc<ServerSystemConfig>,
    max_tokens_per_user: u32,
    transport_client_id: u128,
    message: Message<iggy_binary_protocol::GenericHeader>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
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
    // Promote to the server-internal routed shape at the boundary: the
    // client wire carries no group (it is derived -- plane from `operation`,
    // partition target from the payload), so it starts unset here and the
    // resolution sites below stamp it before anything routes on it.
    let request = request.into_routed();

    // The last point that still sees the body the CLIENT sent; every rewrite below
    // substitutes server-chosen bytes and carries the stamp through unchanged.
    if let Err(error) = verify_request_checksum(&request) {
        warn!(
            transport_client_id,
            operation = ?request.header().operation,
            request = request.header().request,
            "dropping client request whose body does not match its own checksum"
        );
        send_deny_reply(
            shard,
            transport_client_id,
            request.header(),
            error.as_code(),
        )
        .await;
        return;
    }

    ensure_transport_connection(shard, sessions, transport_client_id);

    // Any request is liveness proof, not just PING: an idle-but-active client
    // (e.g. an admin issuing reads between long sleeps) must not be evicted by
    // the heartbeat verifier. A genuinely dead connection sends nothing, so the
    // intended stale-client eviction still fires. No-ops for an unbound client.
    sessions.borrow_mut().record_heartbeat(transport_client_id);

    let header = *request.header();
    if header.operation == Operation::NonReplicated {
        // Auth bypass guard: `PING`, the liveness probe, is the only pre-auth
        // code, on every roster shape. `GET_CLUSTER_METADATA` describes the
        // private replica network and is not something an unauthenticated
        // caller gets to read; a client that dialed a backup no longer needs
        // it to find the leader, because the backup authenticates the login
        // locally and forwards only the consensus proposal
        // (`submit_register_local_or_forward`). Every other non-replicated
        // code MUST go through Register first, which binds the acting user
        // the per-op authz gates resolve.
        let nr_code = u32::from_le_bytes(request.header().reserved[..4].try_into().unwrap());
        // Legacy (pre-register) login codes. The server authenticates only via
        // the Register handshake (LOGIN_REGISTER / LOGIN_REGISTER_WITH_PAT,
        // Operation::Register); the vsr SDK funnels both logins there and never
        // emits these. Reject them uniformly with a typed MalformedLogin (the
        // SDK maps it to InvalidFormat) before the session gate, so a legacy or
        // foreign client fails fast instead of getting the generic
        // Unauthenticated deny the pre-auth guard would send unbound, or the
        // silent empty-ok Reply the bound non-replicated path would send.
        if matches!(
            nr_code,
            LOGIN_USER_CODE | LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE
        ) {
            warn!(
                transport_client_id,
                code = nr_code,
                "rejecting legacy login code; server requires the register handshake"
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
        let allowed_pre_auth = nr_code == PING_CODE;
        if !allowed_pre_auth && sessions.borrow().get_session(transport_client_id).is_none() {
            // Foreign SDKs still probe `GET_CLUSTER_METADATA` before login
            // until they are fixed, so that rejection is routine traffic and
            // logs at debug rather than warn.
            if nr_code == GET_CLUSTER_METADATA_CODE {
                debug!(
                    transport_client_id,
                    "denying pre-auth cluster-metadata read with Unauthenticated"
                );
            } else {
                warn!(
                    transport_client_id,
                    code = nr_code,
                    "denying pre-auth non-replicated read with Unauthenticated"
                );
            }
            // A plain deny Reply, not an Eviction: there is no session to
            // evict, and an Eviction is session-terminal by wire contract,
            // so SDKs would tear down the very connection their login is
            // about to use. The status channel carries the error the same
            // way the request-checksum denial above does.
            send_unbound_deny_reply(
                shard,
                transport_client_id,
                request.header(),
                IggyError::Unauthenticated.as_code(),
            )
            .await;
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
        // rejects with `NoSession`/`Fenced` and the failure disappears
        // silently, wedging the SDK until the socket timeout. A typed
        // `Eviction(NoSession)` is right here, unlike the pre-auth read
        // guard above: a replicated request implies the client believes it
        // has a session, and that session is gone, so it must register
        // again. An empty status-0 Reply is not safe here, because
        // SendMessages is the one replicated operation without a result
        // section, and its decoder would read the empty body as a
        // successful send.
        warn!(
            transport_client_id,
            operation = ?header.operation,
            "rejecting replicated request from unbound transport with Eviction(NoSession)"
        );
        send_unauthenticated_eviction(shard, transport_client_id).await;
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

    let request = request.transmute_header(|header, new_header: &mut RoutedRequestHeader| {
        *new_header = header;
        // Metadata-plane ops route by operation: stamp the sentinel group.
        new_header.group = server_common::sharding::METADATA_GROUP;
        // `bound` is always Some here (unbound transports early-return above);
        // this sets the consensus client id + session for the replicated op.
        if let Some((bound_client_id, bound_session)) = bound {
            new_header.client = bound_client_id;
            new_header.session = bound_session;
        }
    });
    let (request, raw_pat_token) = match maybe_rewrite_pat_request(
        sessions,
        transport_client_id,
        max_tokens_per_user,
        |user_id| {
            shard
                .plane
                .metadata()
                .mux_stm
                .users()
                .read(|users| users.pat_count_of(user_id))
        },
        request,
    ) {
        Ok(rewritten) => rewritten,
        Err(error) => {
            // Token cap reached, malformed body, or a lost session binding.
            send_pre_consensus_deny(
                shard,
                &header,
                transport_client_id,
                &error,
                "personal-access-token",
            )
            .await;
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
            // Malformed body: deny fast with InvalidCommand.
            send_pre_consensus_deny(shard, &header, transport_client_id, &error, "user-password")
                .await;
            return;
        }
    };
    // Static bounds run pre-consensus so a rejected request burns no
    // replicated log entry; HTTP covers the same bounds via
    // `command.validate()`. A body that fails to decode denies typed too
    // (`InvalidCommand`), instead of riding consensus just to fail there.
    let bounds = match header.operation {
        Operation::CreateTopic => CreateTopicRequest::decode_from(request_body(&request))
            .map_err(|_| IggyError::InvalidCommand)
            .and_then(|create_topic| {
                // `parse` doubles as the catalog gate: an unknown key or a
                // malformed value denies typed here, pre-consensus.
                let options = TopicCreateOptions::parse(&create_topic.options)?;
                if let Some(segment_size) = options.segment_size {
                    validate_topic_segment_size(
                        segment_size.as_bytes_u64(),
                        iggy_common::MAX_TOPIC_SEGMENT_SIZE,
                    )?;
                }
                let segment_size = options.segment_size.map_or_else(
                    || iggy_common::DEFAULT_SEGMENT_SIZE,
                    |segment_size| segment_size.as_bytes_u64(),
                );
                if options
                    .preallocate_segments
                    .unwrap_or(iggy_common::DEFAULT_PREALLOCATE_SEGMENTS)
                {
                    validate_preallocated_topic_bytes(segment_size, create_topic.partitions_count)?;
                }
                let max_topic_size = options
                    .max_topic_size
                    .unwrap_or(MaxTopicSize::ServerDefault);
                validate_topic_bounds(create_topic.partitions_count, max_topic_size, segment_size)?;
                warn_unenforceable_topic_size(
                    max_topic_size,
                    segment_size,
                    shard.bus_max_message_size(),
                    create_topic.partitions_count,
                );
                Ok(())
            }),
        Operation::CreatePartitions => CreatePartitionsRequest::decode_from(request_body(&request))
            .map_err(|_| IggyError::InvalidCommand)
            .and_then(|create_partitions| {
                validate_partitions_change_count(create_partitions.partitions_count)?;
                let metadata = shard.plane.metadata();
                warn_unenforceable_topic_size_on_partition_add(
                    metadata.mux_stm.streams(),
                    &create_partitions.stream_id,
                    &create_partitions.topic_id,
                    shard.bus_max_message_size(),
                    create_partitions.partitions_count,
                );
                Ok(())
            }),
        Operation::DeletePartitions => DeletePartitionsRequest::decode_from(request_body(&request))
            .map_err(|_| IggyError::InvalidCommand)
            .and_then(|delete_partitions| {
                validate_partitions_change_count(delete_partitions.partitions_count)
            }),
        // Only the updatable subset: the create-time knobs are pushed to
        // partitions when the topic is built and nothing re-pushes them, so
        // accepting one here would store a value no partition ever sees.
        Operation::UpdateTopic => UpdateTopicRequest::decode_from(request_body(&request))
            .map_err(|_| IggyError::InvalidCommand)
            .and_then(|update_topic| {
                validate_option_keys(&update_topic.options, UPDATABLE_TOPIC_OPTION_KEYS)?;
                let options = TopicCreateOptions::parse(&update_topic.options)?;
                let Some(max_topic_size) = options.max_topic_size else {
                    return Ok(());
                };
                // An update can lower the cap below one segment just as a
                // create can, and the stored map would then report a size the
                // topic can never enforce. The floor is this topic's own
                // segment size, since that key is create-only.
                let metadata = shard.plane.metadata();
                let streams = metadata.mux_stm.streams();
                let segment_size = streams
                    .topic_segment_size(&update_topic.stream_id, &update_topic.topic_id)
                    .map_or_else(
                        || iggy_common::DEFAULT_SEGMENT_SIZE,
                        |segment_size| segment_size.as_bytes_u64(),
                    );
                validate_topic_size_floor(max_topic_size, segment_size)?;
                let partitions_count = streams
                    .topic_partitions_count(&update_topic.stream_id, &update_topic.topic_id)
                    .unwrap_or(0);
                warn_unenforceable_topic_size(
                    max_topic_size,
                    segment_size,
                    shard.bus_max_message_size(),
                    u32::try_from(partitions_count).unwrap_or(u32::MAX),
                );
                Ok(())
            }),
        Operation::UpdateStream => UpdateStreamRequest::decode_from(request_body(&request))
            .map_err(|_| IggyError::InvalidCommand)
            .and_then(|update_stream| {
                validate_option_keys(&update_stream.options, UPDATABLE_STREAM_OPTION_KEYS)
            }),
        Operation::UpdateUser => UpdateUserRequest::decode_from(request_body(&request))
            .map_err(|_| IggyError::InvalidCommand)
            .and_then(|update_user| {
                validate_option_keys(&update_user.options, UPDATABLE_USER_OPTION_KEYS)
            }),
        Operation::CreateStream => CreateStreamRequest::decode_from(request_body(&request))
            .map_err(|_| IggyError::InvalidCommand)
            .and_then(|create_stream| validate_option_keys(&create_stream.options, &[])),
        Operation::CreateUser => CreateUserRequest::decode_from(request_body(&request))
            .map_err(|_| IggyError::InvalidCommand)
            .and_then(|create_user| validate_option_keys(&create_user.options, &[])),
        _ => Ok(()),
    };
    if let Err(error) = bounds {
        send_pre_consensus_deny(shard, &header, transport_client_id, &error, "static-bounds").await;
        return;
    }
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
async fn handle_get_personal_access_tokens<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
    request: &Message<RoutedRequestHeader>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
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
async fn handle_get_me<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
    request: &Message<RoutedRequestHeader>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
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
/// dispatch replies with a nonzero status -- unresolvable namespace,
/// authorization denial, exhausted routable wait -- so the client fails fast
/// instead of wedging on a silent drop or reading a status-0 frame as a
/// committed write.
///
/// `vsr_client_id` keys the consumer-group offset fence (the member id),
/// not the transport id stamped into the partition-op header.
#[allow(clippy::future_not_send)]
pub(crate) async fn dispatch_partition_request<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    request: Message<RoutedRequestHeader>,
    vsr_client_id: u128,
    bound_session: u64,
    transport_client_id: u128,
    acting_user_id: Option<u32>,
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
async fn handle_non_replicated_request<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    sessions: &Rc<RefCell<SessionManager>>,
    system_config: &Arc<ServerSystemConfig>,
    transport_client_id: u128,
    request: Message<RoutedRequestHeader>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    const CODE_RANGE: std::ops::Range<usize> = 0..4;
    let code = u32::from_le_bytes(request.header().reserved[CODE_RANGE].try_into().unwrap());
    // Acting user and peer address for the read gates below, resolved in one
    // connection lookup. `user_id` is `None` only on the pre-auth path
    // (PING), which serves ungated codes; the gated arms fail closed on it.
    let (user_id, client_address) = sessions.borrow().read_context(transport_client_id);
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
            let client_ip = client_address.map(|address| address.ip());
            if client_ip.is_none() {
                debug!(
                    transport_client_id,
                    code,
                    "no peer address recorded; advertised-address resolution degrades to the catch-all"
                );
            }
            handle_default_non_replicated(
                shard,
                transport_client_id,
                code,
                &request,
                user_id,
                &roster,
                client_ip,
            )
            .await;
        }
    }
}

#[allow(clippy::future_not_send, clippy::too_many_arguments)]
async fn handle_default_non_replicated<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    transport_client_id: u128,
    code: u32,
    request: &Message<RoutedRequestHeader>,
    user_id: Option<u32>,
    roster: &ClusterRoster,
    client_ip: Option<IpAddr>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    // Gate by command code before the shared builder runs. The builder stays
    // authz-free (it is byte-shared with the HTTP read path, which gates
    // separately); a denial replies status!=0 with an empty body.
    if let Err(error) = authorize_default_read(shard, code, request_body(request), user_id) {
        send_non_replicated_deny(shard, request, transport_client_id, error.as_code()).await;
        return;
    }
    // Stats is the one default read with an async input: the cross-shard
    // connected-client gather. Run it here so the shared builder stays sync.
    let clients_count = if code == GET_STATS_CODE {
        u32::try_from(shard.list_all_clients().await.len()).unwrap_or(u32::MAX)
    } else {
        0
    };
    match build_non_replicated_response(
        shard,
        code,
        request_body(request),
        user_id,
        roster,
        client_ip,
        clients_count,
    ) {
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
async fn handle_get_snapshot<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    system_config: &Arc<ServerSystemConfig>,
    transport_client_id: u128,
    request: &Message<RoutedRequestHeader>,
    user_id: Option<u32>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
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
async fn send_non_replicated_bytes<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    request: &Message<RoutedRequestHeader>,
    transport_client_id: u128,
    bytes: Bytes,
    label: &'static str,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
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

/// Reject a replicated request from an unbound transport with a typed
/// `Eviction(NoSession)` frame: the session the client believes it has is
/// gone, so it must register again. Pre-auth non-replicated reads get a
/// deny Reply instead (no session exists, so nothing is evicted).
///
/// The SDK's reply decoder maps eviction reasons to typed errors
/// (`NoSession` -> `Unauthenticated`), so clients fail fast with the same
/// error the legacy server returns instead of a body-decode failure. The
/// eviction context is best-effort off the metadata consensus (peer shards
/// have none; zeroes are cosmetic -- the SDK only reads the reason).
#[allow(clippy::future_not_send)]
async fn send_unauthenticated_eviction<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    transport_client_id: u128,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
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
pub(crate) async fn run_heartbeat_verifier<B, MJ, S, SB>(
    shard: Rc<ShellShard<B, MJ, S, SB>>,
    sessions: Rc<RefCell<SessionManager>>,
    interval: std::time::Duration,
    stop_rx: shard::Receiver<()>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    // Legacy `MAX_THRESHOLD`: a client is stale once it misses 1.2 intervals.
    // Integer 6/5 rather than `mul_f64`, which panics on an absurd interval.
    let max_age = interval.saturating_mul(6) / 5;
    loop {
        // `Ok(_)`: stop signalled -> exit. `Err(_)`: interval elapsed -> pass.
        // Waiting on the stop channel rather than sleeping past it keeps this
        // task inside the shutdown drain budget, which is shorter than the
        // heartbeat interval.
        let stop_signal = compio::time::timeout(interval, stop_rx.recv()).await;
        if stop_signal.is_ok() {
            break;
        }
        // Production-only wall clock: the heartbeat verifier is spawned solely
        // by `build_shard_for_thread`, never by the simulator's
        // `wire_shell_handlers`, so neither the interval wait above nor this
        // read is on a deterministic path. Driving this task under the
        // deterministic executor means routing both through the injected clock.
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
    }
}

/// Evict one stale connection: drop its session (releasing consumer-group
/// membership through a replicated `Logout`) and notify the client with a
/// session-terminal `Eviction(StaleClient)`.
#[allow(clippy::future_not_send)]
async fn evict_stale_client<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
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
async fn handle_poll_messages<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    transport_client_id: u128,
    request: &Message<RoutedRequestHeader>,
    user_id: Option<u32>,
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
async fn handle_get_consumer_offset<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    transport_client_id: u128,
    request: &Message<RoutedRequestHeader>,
    user_id: Option<u32>,
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

/// Serve `SyncConsumerGroup`: return the requesting member's current partition
/// assignment + group generation so the client can select partitions locally.
/// The member is keyed by the connection's bound VSR client id
/// (`header().client`). An empty body decodes as "no assignment" on the SDK.
#[allow(clippy::future_not_send)]
async fn handle_sync_consumer_group<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    transport_client_id: u128,
    request: &Message<RoutedRequestHeader>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
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

pub(crate) type DecodedPollRequest = (IggyNamespace, u32, PollingConsumer, PollingArgs);

/// Resolve a decoded poll request into its owning-shard read: namespace,
/// partition, polling consumer, and args. Shared by the TCP dispatch (client
/// id = the connection's bound VSR client) and the HTTP route (client id 0,
/// which fences group polls closed).
#[allow(clippy::cast_possible_truncation)]
pub(crate) fn resolve_poll_request<B, MJ, S, SB>(
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
pub(crate) fn resolve_consumer_offset_request<B, MJ, S, SB>(
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

/// Answer a backup's forwarded `Register` from the node it named primary.
///
/// Proposes in process, never through [`submit_register_local_or_forward`]:
/// that is what bounds a forward at one hop. A node that has since lost
/// primaryship answers `NotPrimary`, and the origin's client replays against
/// whichever node it names next.
#[allow(clippy::future_not_send)]
async fn answer_forwarded_register<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    vsr_client_id: u128,
    user_id: u32,
    nonce: u128,
    origin_replica: u8,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let Some((cluster, view, replica)) = shard
        .plane
        .metadata()
        .consensus
        .as_ref()
        .map(|consensus| (consensus.cluster(), consensus.view(), consensus.replica()))
    else {
        warn!("ForwardedRegister submit reached a shard without metadata consensus");
        return;
    };
    let bound = shard
        .plane
        .metadata()
        .submit_register_in_process(vsr_client_id, user_id)
        .await;
    // `view` predates the await above, which parks with no deadline, so the
    // sealed value can be stale by send time. The origin routes the result by
    // `(nonce, client)` alone; this field must never become a freshness fence.
    let result =
        build_forward_register_result_message(cluster, view, replica, vsr_client_id, nonce, &bound);
    if let Err(error) = shard
        .bus
        .send_to_replica(origin_replica, result.into_generic().into_frozen())
        .await
    {
        warn!(
            origin_replica,
            error = %error,
            "failed to answer a forwarded register"
        );
    }
}

/// How long a login waits for the primary's verdict on a forwarded register.
///
/// Expiry does NOT prove the peer or the frame was lost. The primary answers
/// only once the proposal resolves, and its own submit parks with no deadline:
/// a primary that is not caught up, or whose pipeline is full, absorbs the
/// register into its request queue and answers when that drains. So a slow but
/// healthy primary commits the register after this node has stopped waiting,
/// which is why expiry surfaces as `TransientNotCommitted` rather than the
/// not-accepted flavor.
///
/// The budget stays well under the SDK's response-read timeout on purpose: the
/// client only replays a login while it is still reading, so a longer wait
/// here turns a transient into a torn-down socket.
const FORWARD_SUBMIT_TIMEOUT: Duration = Duration::from_secs(5);

/// Run the `Register` proposal for a login this node has already
/// authenticated, wherever the metadata primary currently is. Shard 0 only.
///
/// A client may dial any node in the cluster. Credentials verify against the
/// replicated users table, which every node holds, so the whole login except
/// the consensus proposal already works on a backup. Only the verified
/// identity crosses the replica interconnect -- never the client's frame and
/// never its credentials -- and the session bind, the reply, and the
/// connection all stay on the node the client dialed.
///
/// The hop does not move any credential decision:
/// - `verify_login_credentials` reads the backup's applied replicated user
///   state.
/// - `verify_pat_credentials` reads the same state, so a PAT minted on the
///   primary that has not replicated here yet is refused until it does.
///   Fail-closed on purpose, the same parity the HTTP forward keeps: it too
///   answers 401 until replication catches up rather than relaying an
///   unverified bearer.
/// - `ClientIdOwnedByAnotherUser` stays a decision of the caught-up primary
///   and round-trips as a terminal refusal.
///
/// Verification is point-in-time on the backup. A password change, PAT
/// revocation, or user deactivation committed on the primary but not yet
/// applied on the backup can therefore admit a login during the backup's apply
/// lag. The forward cannot complete while the backup is partitioned from the
/// primary, which bounds this to a connected replica's replication lag. This
/// is the same stale-read window as the existing HTTP forward.
///
/// The session binds here before this node applies the commit locally. That
/// is the window a primary-side login already has against every other node's
/// apply lag, not a new one.
#[allow(clippy::future_not_send)]
async fn submit_register_local_or_forward<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    vsr_client_id: u128,
    user_id: u32,
) -> Result<BoundSession, MetadataSubmitError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let Some(consensus) = shard.plane.metadata().consensus.as_ref() else {
        return Err(MetadataSubmitError::NotPrimary);
    };
    let (cluster, view, self_replica) =
        (consensus.cluster(), consensus.view(), consensus.replica());
    let target = consensus.primary_index(view);
    // Forward only as a healthy backup. Everything else answers locally: the
    // in-process submit proposes when this node is the serving primary and
    // re-derives `NotPrimary` otherwise -- mid view change there is nobody to
    // forward to (the node the view names has not finished taking over, the
    // SDK replays once it settles), and the view's own primary under state
    // transfer has nowhere to forward to and nothing to commit yet.
    if target == self_replica || !consensus.is_normal() {
        return shard
            .plane
            .metadata()
            .submit_register_in_process(vsr_client_id, user_id)
            .await;
    }

    let nonce = shard.next_forward_nonce(self_replica);
    let (reply, outcome) = shard::channel::<ForwardRegisterResultHeader>(1);
    shard.park_register_forward(nonce, vsr_client_id, reply);
    let forward =
        build_forward_register_message(cluster, view, self_replica, vsr_client_id, nonce, user_id);
    if let Err(error) = shard
        .bus
        .send_to_replica(target, forward.into_generic().into_frozen())
        .await
    {
        shard.cancel_register_forward(nonce, vsr_client_id);
        warn!(
            target,
            error = %error,
            "failed to forward register to the metadata primary"
        );
        return Err(MetadataSubmitError::PrimaryUnreachable);
    }

    match shard::bus_timeout(&shard.bus, FORWARD_SUBMIT_TIMEOUT, outcome.recv()).await {
        Some(Ok(result)) => forward_register_result(&result),
        // Shard-0 teardown dropped the sender without answering.
        Some(Err(_)) => Err(MetadataSubmitError::Canceled),
        None => {
            shard.cancel_register_forward(nonce, vsr_client_id);
            warn!(target, "forwarded register timed out");
            Err(MetadataSubmitError::ForwardTimedOut)
        }
    }
}

/// The primary's verdict, back in the vocabulary the login path speaks.
const fn forward_register_result(
    result: &ForwardRegisterResultHeader,
) -> Result<BoundSession, MetadataSubmitError> {
    match result.outcome {
        ForwardRegisterOutcome::Ok => Ok(BoundSession {
            epoch: result.epoch,
            watermark: result.watermark,
        }),
        ForwardRegisterOutcome::NotPrimary => Err(MetadataSubmitError::NotPrimary),
        ForwardRegisterOutcome::NotCaughtUp => Err(MetadataSubmitError::NotCaughtUp),
        ForwardRegisterOutcome::PipelineFull => Err(MetadataSubmitError::PipelineFull),
        ForwardRegisterOutcome::InProgress => Err(MetadataSubmitError::InProgress),
        ForwardRegisterOutcome::Canceled => Err(MetadataSubmitError::Canceled),
        ForwardRegisterOutcome::ClientIdOwnedByAnotherUser => {
            Err(MetadataSubmitError::ClientIdOwnedByAnotherUser)
        }
    }
}

/// Inverse of [`forward_register_result`], for the answering primary.
const fn forward_register_outcome(
    bound: &Result<BoundSession, MetadataSubmitError>,
) -> (BoundSession, ForwardRegisterOutcome) {
    let zero = BoundSession {
        epoch: 0,
        watermark: 0,
    };
    match bound {
        Ok(bound) => (*bound, ForwardRegisterOutcome::Ok),
        Err(MetadataSubmitError::NotPrimary) => (zero, ForwardRegisterOutcome::NotPrimary),
        Err(MetadataSubmitError::NotCaughtUp) => (zero, ForwardRegisterOutcome::NotCaughtUp),
        Err(MetadataSubmitError::PipelineFull) => (zero, ForwardRegisterOutcome::PipelineFull),
        Err(MetadataSubmitError::InProgress) => (zero, ForwardRegisterOutcome::InProgress),
        Err(MetadataSubmitError::ClientIdOwnedByAnotherUser) => {
            (zero, ForwardRegisterOutcome::ClientIdOwnedByAnotherUser)
        }
        // `MetadataSubmitError` is `#[non_exhaustive]`. Every variant but the
        // ownership refusal is transient by contract, and `Canceled` is the
        // transient answer that claims nothing beyond "retry".
        Err(_) => (zero, ForwardRegisterOutcome::Canceled),
    }
}

#[allow(clippy::cast_possible_truncation)]
fn build_forward_register_message(
    cluster: u128,
    view: u32,
    replica: u8,
    client: u128,
    nonce: u128,
    user_id: u32,
) -> Message<ForwardRegisterHeader> {
    Message::<ForwardRegisterHeader>::new(HEADER_SIZE).transmute_header(
        |_, header: &mut ForwardRegisterHeader| {
            header.command = Command::ForwardRegister;
            header.cluster = cluster;
            header.view = view;
            header.replica = replica;
            header.client = client;
            header.nonce = nonce;
            header.user_id = user_id;
            header.size = HEADER_SIZE as u32;
            header.seal();
        },
    )
}

#[allow(clippy::cast_possible_truncation)]
fn build_forward_register_result_message(
    cluster: u128,
    view: u32,
    replica: u8,
    client: u128,
    nonce: u128,
    bound: &Result<BoundSession, MetadataSubmitError>,
) -> Message<ForwardRegisterResultHeader> {
    let (session, outcome) = forward_register_outcome(bound);
    Message::<ForwardRegisterResultHeader>::new(HEADER_SIZE).transmute_header(
        |_, header: &mut ForwardRegisterResultHeader| {
            header.command = Command::ForwardRegisterResult;
            header.cluster = cluster;
            header.view = view;
            header.replica = replica;
            header.client = client;
            header.nonce = nonce;
            header.epoch = session.epoch;
            header.watermark = session.watermark;
            header.outcome = outcome;
            header.size = HEADER_SIZE as u32;
            header.seal();
        },
    )
}

/// Answer a backup's forwarded Logout from the node it named primary.
#[allow(clippy::future_not_send)]
async fn answer_forwarded_logout<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    vsr_client_id: u128,
    session: u64,
    request: u64,
    nonce: u128,
    origin_replica: u8,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let Some((cluster, view, replica)) = shard
        .plane
        .metadata()
        .consensus
        .as_ref()
        .map(|consensus| (consensus.cluster(), consensus.view(), consensus.replica()))
    else {
        warn!("ForwardedLogout submit reached a shard without metadata consensus");
        return;
    };
    let outcome = shard
        .plane
        .metadata()
        .submit_logout_in_process(vsr_client_id, session, request)
        .await;
    let result =
        build_forward_logout_result_message(cluster, view, replica, vsr_client_id, nonce, &outcome);
    if let Err(error) = shard
        .bus
        .send_to_replica(origin_replica, result.into_generic().into_frozen())
        .await
    {
        warn!(
            origin_replica,
            error = %error,
            "failed to answer a forwarded logout"
        );
    }
}

/// Commit a Logout locally when this node is primary, otherwise forward it
/// once to the primary named by the current normal view.
#[allow(clippy::future_not_send)]
async fn submit_logout_local_or_forward<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    vsr_client_id: u128,
    session: u64,
    request: u64,
) -> Result<u64, MetadataSubmitError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let Some(consensus) = shard.plane.metadata().consensus.as_ref() else {
        return Err(MetadataSubmitError::NotPrimary);
    };
    let (cluster, view, self_replica) =
        (consensus.cluster(), consensus.view(), consensus.replica());
    let target = consensus.primary_index(view);
    if target == self_replica || !consensus.is_normal() {
        return shard
            .plane
            .metadata()
            .submit_logout_in_process(vsr_client_id, session, request)
            .await;
    }

    let nonce = shard.next_forward_nonce(self_replica);
    let (reply, outcome) = shard::channel::<ForwardLogoutResultHeader>(1);
    shard.park_logout_forward(nonce, vsr_client_id, reply);
    let forward = build_forward_logout_message(
        cluster,
        view,
        self_replica,
        vsr_client_id,
        nonce,
        session,
        request,
    );
    if let Err(error) = shard
        .bus
        .send_to_replica(target, forward.into_generic().into_frozen())
        .await
    {
        shard.cancel_logout_forward(nonce, vsr_client_id);
        warn!(
            target,
            error = %error,
            "failed to forward logout to the metadata primary"
        );
        return Err(MetadataSubmitError::PrimaryUnreachable);
    }

    match shard::bus_timeout(&shard.bus, FORWARD_SUBMIT_TIMEOUT, outcome.recv()).await {
        Some(Ok(result)) => forward_logout_result(&result),
        Some(Err(_)) => Err(MetadataSubmitError::Canceled),
        None => {
            shard.cancel_logout_forward(nonce, vsr_client_id);
            warn!(target, "forwarded logout timed out");
            Err(MetadataSubmitError::ForwardTimedOut)
        }
    }
}

const fn forward_logout_result(
    result: &ForwardLogoutResultHeader,
) -> Result<u64, MetadataSubmitError> {
    match result.outcome {
        ForwardLogoutOutcome::Ok => Ok(result.commit),
        ForwardLogoutOutcome::NotPrimary => Err(MetadataSubmitError::NotPrimary),
        ForwardLogoutOutcome::PipelineFull => Err(MetadataSubmitError::PipelineFull),
        ForwardLogoutOutcome::InProgress => Err(MetadataSubmitError::InProgress),
        ForwardLogoutOutcome::Canceled => Err(MetadataSubmitError::Canceled),
    }
}

const fn forward_logout_outcome(
    outcome: &Result<u64, MetadataSubmitError>,
) -> (u64, ForwardLogoutOutcome) {
    match outcome {
        Ok(commit) => (*commit, ForwardLogoutOutcome::Ok),
        Err(MetadataSubmitError::NotPrimary) => (0, ForwardLogoutOutcome::NotPrimary),
        Err(MetadataSubmitError::PipelineFull) => (0, ForwardLogoutOutcome::PipelineFull),
        Err(MetadataSubmitError::InProgress) => (0, ForwardLogoutOutcome::InProgress),
        Err(_) => (0, ForwardLogoutOutcome::Canceled),
    }
}

#[allow(clippy::cast_possible_truncation, clippy::too_many_arguments)]
fn build_forward_logout_message(
    cluster: u128,
    view: u32,
    replica: u8,
    client: u128,
    nonce: u128,
    session: u64,
    request: u64,
) -> Message<ForwardLogoutHeader> {
    Message::<ForwardLogoutHeader>::new(HEADER_SIZE).transmute_header(
        |_, header: &mut ForwardLogoutHeader| {
            header.command = Command::ForwardLogout;
            header.cluster = cluster;
            header.view = view;
            header.replica = replica;
            header.client = client;
            header.nonce = nonce;
            header.session = session;
            header.request = request;
            header.size = HEADER_SIZE as u32;
            header.seal();
        },
    )
}

#[allow(clippy::cast_possible_truncation)]
fn build_forward_logout_result_message(
    cluster: u128,
    view: u32,
    replica: u8,
    client: u128,
    nonce: u128,
    result: &Result<u64, MetadataSubmitError>,
) -> Message<ForwardLogoutResultHeader> {
    let (commit, outcome) = forward_logout_outcome(result);
    Message::<ForwardLogoutResultHeader>::new(HEADER_SIZE).transmute_header(
        |_, header: &mut ForwardLogoutResultHeader| {
            header.command = Command::ForwardLogoutResult;
            header.cluster = cluster;
            header.view = view;
            header.replica = replica;
            header.client = client;
            header.nonce = nonce;
            header.commit = commit;
            header.outcome = outcome;
            header.size = HEADER_SIZE as u32;
            header.seal();
        },
    )
}

/// Run the consensus `Register` proposal on the metadata owner (shard 0)
/// and return the committed session.
///
/// Credential verification and session binding stay on the calling (home)
/// shard -- only this consensus step must execute where the metadata
/// consensus group lives. On shard 0 it goes straight to
/// [`submit_register_local_or_forward`]; on a peer it forwards a
/// [`shard::MetadataSubmit`] to shard 0 and awaits the committed op. A dropped
/// reply (shard-0 inbox full / shutdown) maps to a transient `Canceled`, which
/// the caller wraps so the SDK replays.
#[allow(clippy::future_not_send)]
pub(crate) async fn submit_register_on_owner<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    vsr_client_id: u128,
    user_id: u32,
) -> Result<BoundSession, MetadataSubmitError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    if shard.id == 0 {
        return submit_register_local_or_forward(shard, vsr_client_id, user_id).await;
    }
    let (reply, rx) = shard::channel::<Result<BoundSession, MetadataSubmitError>>(1);
    shard.forward_metadata_submit(shard::MetadataSubmit::Register {
        vsr_client_id,
        user_id,
        reply,
    });
    // The owner's outcome, verbatim in both directions. `Canceled` is only for a
    // dropped channel, where nothing came back to classify.
    rx.recv()
        .await
        .unwrap_or(Err(MetadataSubmitError::Canceled))
}

/// Logout counterpart of [`submit_register_on_owner`].
#[allow(clippy::future_not_send)]
pub(crate) async fn submit_logout_on_owner<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    vsr_client_id: u128,
    session: u64,
    request: u64,
) -> Result<u64, MetadataSubmitError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    if shard.id == 0 {
        return submit_logout_local_or_forward(shard, vsr_client_id, session, request).await;
    }
    let (reply, rx) = shard::channel::<Result<u64, MetadataSubmitError>>(1);
    shard.forward_metadata_submit(shard::MetadataSubmit::Logout {
        vsr_client_id,
        session,
        request,
        reply,
    });
    rx.recv()
        .await
        .unwrap_or(Err(MetadataSubmitError::Canceled))
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
async fn handle_delete_segments_request<B, MJ, S, SB>(
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
pub(crate) async fn resolve_delete_segments_truncate<B, MJ, S, SB>(
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

/// Release the client-table slot for a disconnected transport, cluster-wide.
///
/// The local `SessionManager` connection is already dropped by the caller;
/// this is what drops the replicated entry, so a peer replica does not keep an
/// orphaned session until it evicts one under capacity pressure.
///
/// Unconditional, and deliberately so. Holding the slot open for a grace
/// window would let a reconnecting client resume onto its entry with its
/// watermark and reply ring intact, but nothing in tree re-presents a
/// `client_id` after a disconnect (the Rust SDK mints a fresh one on
/// re-login), so the window buys nothing today and the slot it holds is not
/// free: the client table's eviction point moves from concurrent connections
/// to CUMULATIVE connects, and every capacity eviction silently erases a
/// dedup watermark.
///
/// A resume window becomes worth having once SDK-side identity stability
/// lands, at which point it needs a timer of its own -- riding the heartbeat
/// verifier would tie the grace period to heartbeat configuration, since
/// `collect_stale` keys off `heartbeat.interval` and the verifier does not run
/// at all when `heartbeat.enabled` is false.
/// Deliberately does NOT drop the local `ClientTable` slot first:
/// `submit_logout_*` short-circuits when the slot is already gone, so a
/// pre-emptive local removal would suppress the `Logout` and leave peer
/// replicas with an orphaned session until they evict it themselves -- the
/// exact divergence this avoids. `submit_logout_on_owner` runs in-process on
/// shard 0 and forwards for peer-homed connections; its session guard drops a
/// stale logout for a reused client id.
#[allow(clippy::future_not_send)]
fn submit_disconnect_logout<B, MJ, S, SB>(
    shard: Rc<ShellShard<B, MJ, S, SB>>,
    vsr_client_id: u128,
    session: u64,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    // The sentinel request id is what the apply path reads to keep, rather
    // than drop, the session's dedup fence: the client may be reconnecting
    // under the same key, and its retry must still be answered.
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
pub(crate) async fn submit_client_request_on_owner<B, MJ, S, SB>(
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

#[allow(clippy::future_not_send)]
async fn handle_logout_request<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
    request: Message<RoutedRequestHeader>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let Some((vsr_client_id, session)) = sessions.borrow().get_session(transport_client_id) else {
        // Logout on an unbound transport: the desired state already holds,
        // so answer ok. A silent drop would wedge the lockstep SDK on this
        // connection until its socket read timeout, and the SDK routinely
        // sends a logout before each re-login.
        warn!(
            transport_client_id,
            "logout for unbound VSR session; answering ok"
        );
        let commit = current_metadata_commit(shard);
        let reply = build_empty_reply(request.header(), transport_client_id, 0, commit);
        if let Err(error) = shard
            .bus
            .send_to_client(transport_client_id, reply.into_generic().into_frozen())
            .await
        {
            warn!(
                transport_client_id,
                error = %error,
                "failed to send unbound logout reply"
            );
        }
        return;
    };

    let request_id = request.header().request;
    let commit = match submit_logout_on_owner(shard, vsr_client_id, session, request_id).await {
        Ok(commit) => commit,
        Err(error) => {
            // Deny as transient instead of dropping the frame: the submit
            // usually fails because this replica is not the metadata owner
            // right now, and the SDK replays a transient rejection.
            warn!(transport_client_id, error = %error, "logout/unregister failed; denying transient");
            let commit = current_metadata_commit(shard);
            let reply = build_deny_reply(
                request.header(),
                vsr_client_id,
                session,
                commit,
                transient_logout_code(&error).as_code(),
            );
            if let Err(send_error) = shard
                .bus
                .send_to_client(transport_client_id, reply.into_generic().into_frozen())
                .await
            {
                warn!(
                    transport_client_id,
                    error = %send_error,
                    "failed to send logout deny reply"
                );
            }
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

/// Preserve the client identity when a Logout may already have entered the
/// primary's pipeline. Moving an unknown-outcome replay to another connection
/// could race a later Register and obscure whether the old epoch was removed.
const fn transient_logout_code(error: &MetadataSubmitError) -> IggyError {
    match error {
        MetadataSubmitError::ForwardTimedOut
        | MetadataSubmitError::InProgress
        | MetadataSubmitError::Canceled => IggyError::TransientNotCommitted,
        _ => IggyError::TransientNotAccepted,
    }
}

fn ensure_transport_connection<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let Some(meta) = shard.bus.client_meta(transport_client_id) else {
        return;
    };
    sessions
        .borrow_mut()
        .ensure_connection(transport_client_id, meta.peer_addr, meta.transport);
}

#[allow(clippy::future_not_send, clippy::too_many_lines)]
async fn handle_login_register_request<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
    request: Message<RoutedRequestHeader>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
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
    let mut credentials_rejected = false;
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
                // chance. A password-shaped body rarely parses as a PAT
                // body, so remember the rejection: the final fall-through
                // must surface InvalidCredentials, not MalformedLogin.
                credentials_rejected = true;
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

    if credentials_rejected {
        warn!(
            transport_client_id,
            "rejecting register request: invalid credentials"
        );
        send_login_eviction(
            shard,
            transport_client_id,
            request.header().client,
            EvictionReason::InvalidCredentials,
        )
        .await;
        return;
    }

    warn!(
        transport_client_id,
        "rejecting register request with unsupported payload shape"
    );
    send_login_eviction(
        shard,
        transport_client_id,
        request.header().client,
        EvictionReason::MalformedLogin,
    )
    .await;
}

/// Best-effort login-rejection eviction. Terminal one-way frame; a gone
/// connection has nothing to recover, so the send error is logged and
/// dropped. Consensus context (cluster/view/replica) is stamped on the
/// metadata shard and zeroed elsewhere -- the SDK only reads the reason,
/// plus the protocol window on `IncompatibleProtocol`.
#[allow(clippy::future_not_send)]
pub(crate) async fn send_login_eviction<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    transport_client_id: u128,
    vsr_client_id: u128,
    reason: EvictionReason,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
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

pub(crate) fn upgrade_shard_handle<B, MJ, S, SB>(
    shard_handle: &ShellShardHandle<B, MJ, S, SB>,
) -> Option<Rc<ShellShard<B, MJ, S, SB>>>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
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
    use iggy_binary_protocol::primitives::partition_assignment::CreatedPartitionAssignment;
    use iggy_binary_protocol::requests::messages::SendMessagesHeader;
    use iggy_binary_protocol::requests::streams::CreateStreamRequest;
    use iggy_binary_protocol::requests::topics::CreateTopicWithAssignmentsRequest;
    use iggy_binary_protocol::{PrepareOkHeader, ReplyHeader, WireName, WirePartitioning};
    use iggy_common::defaults::DEFAULT_ROOT_USER_ID;
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
    use metadata::stm::StateMachine as _;
    use metadata::stm::stream::Streams;
    use metadata::stm::user::Users;
    use metadata::{IggyMetadata, MuxStateMachine};
    use partitions::{IggyPartitions, PartitionPathLayout, PartitionsConfig};
    use server_common::iobuf::Frozen;
    use server_common::sharding::ShardId;
    use server_common::{MESSAGE_ALIGN, Message, MessageBag};
    use shard::metrics::ShardMetrics;
    use shard::shards_table::PapayaShardsTable;
    use shard::{
        IggyShard, LifecycleFrame, PartitionConsensusConfig, ReconcileOp, ReplicaTopology,
        ShardFrame, ShardIdentity, shard_channel,
    };
    use std::cell::{Cell, RefCell};
    use std::future::Future;
    use std::mem::size_of;
    use std::rc::Rc;

    type TestMux = MuxStateMachine<variadic!(Users, Streams)>;
    type TestShard = IggyShard<SpyBus, PrepareJournal, IggySnapshot, TestMux, PapayaShardsTable>;
    /// `(target client id, reply frame bytes)` per `send_to_client` call.
    type RecordedReplies = Rc<RefCell<Vec<(u128, Vec<u8>)>>>;
    /// `(target replica id, frame bytes)` per `send_to_replica` call.
    type RecordedReplicaSends = Rc<RefCell<Vec<(u8, Vec<u8>)>>>;

    /// Records every client-bound reply and replica-bound frame (target +
    /// bytes) instead of writing to a socket; everything else is a no-op. The
    /// two `ShellBus` halves are stubbed.
    #[derive(Debug, Clone, Default)]
    struct SpyBus {
        client_replies: RecordedReplies,
        replica_sends: RecordedReplicaSends,
        /// Resolve [`MessageBus::sleep`] immediately instead of arming a real
        /// timer. The register forward is the only path here that races a
        /// timer, and its budget is five seconds -- too long to wait for in a
        /// unit test, and too long to shorten in production for one.
        instant_timers: Rc<Cell<bool>>,
    }

    impl SpyBus {
        /// Decode the single frame this bus sent to a replica.
        fn sole_replica_send<H: iggy_binary_protocol::ConsensusHeader>(&self) -> (u8, H) {
            let sends = self.replica_sends.borrow();
            assert_eq!(sends.len(), 1, "expected exactly one replica-bound frame");
            let (target, frame) = &sends[0];
            let mut aligned = server_common::iobuf::Owned::<MESSAGE_ALIGN>::zeroed(frame.len());
            aligned.as_mut_slice().copy_from_slice(frame);
            let header =
                *bytemuck::checked::try_from_bytes::<H>(&aligned.as_slice()[..size_of::<H>()])
                    .expect("replica frame decodes into the expected header");
            (*target, header)
        }
    }

    #[allow(clippy::future_not_send)]
    impl MessageBus for SpyBus {
        fn track_background(&self, _handle: JoinHandle<()>) {}
        async fn send_to_client(
            &self,
            client_id: u128,
            data: Frozen<MESSAGE_ALIGN>,
        ) -> Result<(), SendError> {
            self.client_replies
                .borrow_mut()
                .push((client_id, data.as_slice().to_vec()));
            Ok(())
        }
        async fn send_to_replica(
            &self,
            replica: u8,
            data: Frozen<MESSAGE_ALIGN>,
        ) -> Result<(), SendError> {
            self.replica_sends
                .borrow_mut()
                .push((replica, data.as_slice().to_vec()));
            Ok(())
        }
        async fn sleep(&self, duration: std::time::Duration) {
            if !self.instant_timers.get() {
                compio::time::sleep(duration).await;
            }
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

    /// Consensus incarnations standing for two successive boots of one node, as
    /// far apart as the random draw at bootstrap makes them.
    const FIRST_BOOT: u128 = 0x5EED_0001;
    const SECOND_BOOT: u128 = 0x9E37_79B9_7F4A_7C15;

    /// Shard 0 carrying a metadata consensus group of `replica_count`
    /// replicas in which this node is `replica`. No journal: every test using
    /// it either never proposes, or is a backup that cannot.
    ///
    /// `incarnation` stands for one boot of this node: the shard seeds its
    /// forward-nonce counter from it, so passing a different value models a
    /// restart.
    fn test_shard(bus: &SpyBus, replica: u8, replica_count: u8, incarnation: u128) -> TestShard {
        let consensus = VsrConsensus::new(
            1,
            replica,
            replica_count,
            server_common::sharding::METADATA_GROUP,
            bus.clone(),
            LocalPipeline::new(),
        );
        consensus.set_incarnation(incarnation);
        consensus.init();
        let metadata: IggyMetadata<_, PrepareJournal, IggySnapshot, TestMux> =
            IggyMetadata::new(Some(consensus), None, None, None, TestMux::default(), None);
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
        TestShard::without_inbox(
            ShardIdentity::new(0, "dispatch-test".to_string()),
            bus.clone(),
            metadata,
            partitions,
            PapayaShardsTable::new(),
            PartitionConsensusConfig::new(
                1,
                ReplicaTopology::new(replica, replica_count),
                bus.clone(),
            ),
        )
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
            command: Command::Reply,
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
    ) -> Message<RoutedRequestHeader> {
        let header_size = size_of::<RoutedRequestHeader>();
        let total = header_size + body.len();
        let mut message = Message::<RoutedRequestHeader>::new(total);
        {
            let slice = message.as_mut_slice();
            slice[header_size..total].copy_from_slice(body);
            let header =
                bytemuck::checked::from_bytes_mut::<RoutedRequestHeader>(&mut slice[..header_size]);
            *header = RoutedRequestHeader {
                command: Command::Request,
                operation,
                size: u32::try_from(total).expect("test request fits u32"),
                client,
                session,
                request,
                user_id: 0,
                group: server_common::sharding::METADATA_GROUP,
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
                command: Command::Prepare,
                operation,
                size: u32::try_from(total).expect("test prepare fits u32"),
                op: 1,
                view: 0,
                client,
                request,
                user_id: 0,
                group: server_common::sharding::METADATA_GROUP,
                ..Default::default()
            };
        }
        // A real identity, not a placeholder: `on_replicate` recomputes it before the
        // prepare reaches the WAL, so an arbitrary value reads as transit corruption.
        consensus::seal_prepare_checksum(message)
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
            server_common::sharding::METADATA_GROUP,
            bus.clone(),
            LocalPipeline::new(),
        );
        consensus.init();
        let metadata: IggyMetadata<_, PrepareJournal, IggySnapshot, TestMux> = IggyMetadata::new(
            Some(consensus),
            Some(journal),
            None,
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
                validate_checksum: true,
                segment_size: iggy_common::IggyByteSize::from(1_048_576_u64),
                preallocate_segments: false,
                encryptor: None,
                path_layout: PartitionPathLayout::default(),
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
            options: WireOptions::empty(),
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
            bus.client_replies
                .borrow()
                .iter()
                .any(|(client, _)| *client == TRANSPORT_A),
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

    /// A test shard wired to its own lanes (the held sender feeds them),
    /// for the reply-lane pump tests below.
    fn reply_lane_test_shard(name: &str) -> (SpyBus, shard::TaggedSender, Rc<TestShard>) {
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
        let (sender, inbox_rx, reply_inbox_rx) = shard_channel(0, 16, 16);
        let lane_sender = sender.clone();
        let shard = TestShard::new(
            ShardIdentity::new(0, name.to_string()),
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
        (bus, lane_sender, Rc::new(shard))
    }

    fn reply_lane_forward(client_id: u128) -> ShardFrame {
        ShardFrame::lifecycle(LifecycleFrame::ForwardClientSend {
            client_id,
            msg: server_common::iobuf::Owned::<MESSAGE_ALIGN>::zeroed(64).into(),
        })
    }

    /// A frame on the reply lane must reach the client through the RUNNING
    /// pump's reply arm: the lane split moved `ForwardClientSend` off the
    /// main inbox, so a pump that forgot to service the new lane would
    /// strand every cross-shard reply while the send sites happily report
    /// success.
    #[compio::test]
    async fn pump_live_arm_delivers_reply_lane_forwards() {
        const TRANSPORT: u128 = 92;
        let (bus, lane_sender, shard) = reply_lane_test_shard("reply-lane-live-arm-test");

        let (stop_tx, stop_rx) = shard::channel::<()>(1);
        let pump_shard = Rc::clone(&shard);
        let pump = compio::runtime::spawn(async move {
            pump_shard.run_message_pump(stop_rx).await;
        });

        lane_sender
            .reply_sender()
            .try_send(reply_lane_forward(TRANSPORT))
            .expect("reply lane has capacity");

        // The pump is idle on the main lane, so its bottom reply arm must
        // serve the frame without any main-lane traffic or shutdown drain.
        let mut delivered = false;
        for _ in 0..500 {
            if !bus.client_replies.borrow().is_empty() {
                delivered = true;
                break;
            }
            compio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        stop_tx.try_send(()).expect("stop channel has capacity");
        let _ = pump.await;

        assert!(
            delivered,
            "the live reply arm must deliver a forward while the pump runs"
        );
        let replies = bus.client_replies.borrow();
        assert_eq!(replies[0].0, TRANSPORT, "forward must reach its client");
    }

    /// The shutdown path must ALSO deliver reply-lane frames: a forward
    /// already accepted by the lane when the stop signal wins the biased
    /// select would otherwise be silently destroyed at teardown.
    #[compio::test]
    async fn pump_shutdown_drain_delivers_reply_lane_forwards() {
        const TRANSPORT: u128 = 93;
        let (bus, lane_sender, shard) = reply_lane_test_shard("reply-lane-drain-test");

        lane_sender
            .reply_sender()
            .try_send(reply_lane_forward(TRANSPORT))
            .expect("reply lane has capacity");

        // Pre-armed stop: the pump exits through the biased stop arm and the
        // post-loop drain must still deliver the reply-lane frame.
        let (stop_tx, stop_rx) = shard::channel::<()>(1);
        stop_tx.try_send(()).expect("stop channel has capacity");
        shard.run_message_pump(stop_rx).await;

        let replies = bus.client_replies.borrow();
        assert_eq!(
            replies.len(),
            1,
            "the pump's reply-lane drain must deliver the forwarded reply"
        );
        assert_eq!(
            replies[0].0, TRANSPORT,
            "the forward must reach the client it was addressed to"
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
                denies.push((client_id, msg.as_slice().to_vec()));
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

    /// A backup's login: it forwards the register it authenticated to the
    /// view's primary and completes on the primary's verdict, with the whole
    /// round trip going through the real shard ingest arm.
    #[compio::test]
    async fn backup_forwards_register_and_completes_on_the_primary_verdict() {
        const CLIENT: u128 = 0xCAFE;
        const USER: u32 = 7;
        const EPOCH: u64 = 41;
        const WATERMARK: u64 = 9;

        let bus = SpyBus::default();
        // Replica 1 of 3, view 0: `primary_index(0)` is replica 0.
        let shard = Rc::new(test_shard(&bus, 1, 3, FIRST_BOOT));
        let login = {
            let shard = Rc::clone(&shard);
            compio::runtime::spawn(async move {
                submit_register_local_or_forward(&shard, CLIENT, USER).await
            })
        };
        await_forward(&bus).await;
        let (target, forward) = bus.sole_replica_send::<ForwardRegisterHeader>();
        assert_eq!(target, 0, "forward must address the view's primary");
        assert_eq!(forward.command, Command::ForwardRegister);
        assert_eq!(forward.client, CLIENT);
        assert_eq!(
            forward.user_id, USER,
            "the forwarded identity is the payload"
        );
        assert_eq!(forward.replica, 1, "the origin names itself for the answer");
        assert_ne!(forward.nonce, 0);
        assert_eq!(forward.verify_frame(), Ok(()), "the frame must be sealed");
        assert_eq!(forward.validate(), Ok(()));

        shard
            .on_message(forward_register_result(
                &forward,
                ForwardRegisterOutcome::Ok,
                EPOCH,
                WATERMARK,
            ))
            .await;
        assert_eq!(
            login.await.expect("the login task ran to completion"),
            Ok(BoundSession {
                epoch: EPOCH,
                watermark: WATERMARK,
            })
        );
    }

    #[compio::test]
    async fn backup_forwards_logout_and_completes_on_the_primary_verdict() {
        const CLIENT: u128 = 0xCAFE;
        const SESSION: u64 = 41;
        const REQUEST: u64 = 9;
        const COMMIT: u64 = 42;

        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, 1, 3, FIRST_BOOT));
        let logout = {
            let shard = Rc::clone(&shard);
            compio::runtime::spawn(async move {
                submit_logout_local_or_forward(&shard, CLIENT, SESSION, REQUEST).await
            })
        };
        await_forward(&bus).await;
        let (target, forward) = bus.sole_replica_send::<ForwardLogoutHeader>();
        assert_eq!(target, 0, "forward must address the view's primary");
        assert_eq!(forward.command, Command::ForwardLogout);
        assert_eq!(forward.client, CLIENT);
        assert_eq!(forward.session, SESSION);
        assert_eq!(forward.request, REQUEST);
        assert_eq!(forward.replica, 1);
        assert_ne!(forward.nonce, 0);
        assert_eq!(forward.verify_frame(), Ok(()));
        assert_eq!(forward.validate(), Ok(()));

        shard
            .on_message(forward_logout_result_message(&forward, &Ok(COMMIT)))
            .await;
        assert_eq!(
            logout.await.expect("the logout task ran to completion"),
            Ok(COMMIT)
        );
    }

    #[compio::test]
    async fn unanswered_logout_forward_times_out_and_clears_the_waiter() {
        let bus = SpyBus::default();
        bus.instant_timers.set(true);
        let shard = Rc::new(test_shard(&bus, 1, 3, FIRST_BOOT));

        let outcome = submit_logout_local_or_forward(&shard, 0xCAFE, 41, 9).await;
        assert_eq!(outcome, Err(MetadataSubmitError::ForwardTimedOut));

        let (_, forward) = bus.sole_replica_send::<ForwardLogoutHeader>();
        shard
            .on_message(forward_logout_result_message(&forward, &Ok(42)))
            .await;
    }

    #[test]
    fn unknown_logout_outcomes_pin_the_session() {
        for error in [
            MetadataSubmitError::ForwardTimedOut,
            MetadataSubmitError::InProgress,
            MetadataSubmitError::Canceled,
        ] {
            assert_eq!(
                transient_logout_code(&error),
                IggyError::TransientNotCommitted
            );
        }
        for error in [
            MetadataSubmitError::NotPrimary,
            MetadataSubmitError::PipelineFull,
            MetadataSubmitError::PrimaryUnreachable,
        ] {
            assert_eq!(
                transient_logout_code(&error),
                IggyError::TransientNotAccepted
            );
        }
    }

    /// The ownership refusal is the one terminal verdict, and it has to stay
    /// terminal across the hop or the SDK replays a login that cannot succeed.
    #[compio::test]
    async fn forwarded_register_keeps_the_ownership_refusal_terminal() {
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, 1, 3, FIRST_BOOT));
        let login = {
            let shard = Rc::clone(&shard);
            compio::runtime::spawn(async move {
                submit_register_local_or_forward(&shard, 0xCAFE, 7).await
            })
        };
        await_forward(&bus).await;
        let (_, forward) = bus.sole_replica_send::<ForwardRegisterHeader>();
        shard
            .on_message(forward_register_result(
                &forward,
                ForwardRegisterOutcome::ClientIdOwnedByAnotherUser,
                0,
                0,
            ))
            .await;
        let error = login
            .await
            .expect("the login task ran to completion")
            .expect_err("the refusal must surface");
        assert_eq!(error, MetadataSubmitError::ClientIdOwnedByAnotherUser);
        assert!(!error.is_transient(), "the refusal must stay terminal");
    }

    /// A primary that never answers must not strand the login or leak its
    /// parked entry; the client gets a transient failure and replays.
    #[compio::test]
    async fn unanswered_forward_times_out_and_clears_the_parked_login() {
        let bus = SpyBus::default();
        bus.instant_timers.set(true);
        let shard = Rc::new(test_shard(&bus, 1, 3, FIRST_BOOT));

        let outcome = submit_register_local_or_forward(&shard, 0xCAFE, 7).await;
        assert_eq!(outcome, Err(MetadataSubmitError::ForwardTimedOut));
        assert!(
            outcome.unwrap_err().is_transient(),
            "a lost answer is replayable"
        );

        // The parked entry is gone: the answer that arrives late finds nothing
        // and is dropped rather than completing a login nobody is waiting on.
        let (_, forward) = bus.sole_replica_send::<ForwardRegisterHeader>();
        shard
            .on_message(forward_register_result(
                &forward,
                ForwardRegisterOutcome::Ok,
                41,
                0,
            ))
            .await;
    }

    /// The reply frame is where an unknown outcome has to be told apart from a
    /// refusal: a forward that timed out may still commit, so the client must
    /// replay under the same client id instead of failing over under a fresh
    /// one. A verdict that refused the register carries no such doubt.
    #[compio::test]
    async fn transient_login_reply_marks_a_timed_out_forward_not_committed() {
        const TRANSPORT: u128 = 91;
        const VSR_CLIENT: u128 = 0xCAFE;
        const RESULT_OFFSET: usize = size_of::<ReplyHeader>() + 8;

        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, 1, 3, FIRST_BOOT));
        let request = request_message(Operation::Register, VSR_CLIENT, 0, 0, &[]);

        for (submit_error, expected) in [
            (
                MetadataSubmitError::ForwardTimedOut,
                IggyError::TransientNotCommitted,
            ),
            (
                MetadataSubmitError::NotPrimary,
                IggyError::TransientNotAccepted,
            ),
        ] {
            let error = LoginRegisterError::Transient(submit_error);
            surface_login_failure(&shard, TRANSPORT, request.header(), &error).await;

            let replies = bus.client_replies.borrow();
            assert_eq!(replies.len(), 1, "a transient login must answer a frame");
            let (client, frame) = &replies[0];
            assert_eq!(*client, TRANSPORT, "reply must target the transport id");
            let result =
                u32::from_le_bytes(frame[RESULT_OFFSET..RESULT_OFFSET + 4].try_into().unwrap());
            assert_eq!(result, expected.as_code(), "{error} must reply {expected}");
            drop(replies);
            bus.client_replies.borrow_mut().clear();
        }
    }

    /// A restart must not re-mint the nonce sequence of the boot before it. The
    /// nonce is never persisted, and an answer to a pre-restart forward can
    /// still be in flight: routed by a repeated nonce it would confirm a login
    /// the cluster never committed, with another client's epoch.
    #[compio::test]
    async fn a_restart_moves_the_forward_nonce_sequence() {
        assert_ne!(
            first_forward_nonce(FIRST_BOOT).await,
            first_forward_nonce(SECOND_BOOT).await,
            "each boot must start its nonce sequence somewhere the other did not"
        );
    }

    /// Seeding the counter from the incarnation means it can start one step
    /// short of wrapping, and a zero nonce is a frame every replica rejects.
    #[compio::test]
    async fn wrapping_forward_nonce_counter_skips_zero() {
        let nonce = first_forward_nonce(u128::from(u64::MAX)).await;
        assert_ne!(
            nonce & u128::from(u64::MAX),
            0,
            "a counter that wrapped must not contribute a zero nonce half"
        );
    }

    /// An answer echoing a client the nonce was never parked for must neither
    /// complete that login nor evict it, since a repeated nonce is exactly what
    /// a late cross-boot answer carries.
    #[compio::test]
    async fn forward_result_for_another_client_leaves_the_login_parked() {
        const CLIENT: u128 = 0xCAFE;
        const EPOCH: u64 = 41;
        const WATERMARK: u64 = 9;
        const FOREIGN_EPOCH: u64 = 77;

        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, 1, 3, FIRST_BOOT));
        let login = {
            let shard = Rc::clone(&shard);
            compio::runtime::spawn(async move {
                submit_register_local_or_forward(&shard, CLIENT, 7).await
            })
        };
        await_forward(&bus).await;
        let (_, forward) = bus.sole_replica_send::<ForwardRegisterHeader>();

        let mut foreign = forward;
        foreign.client = CLIENT + 1;
        shard
            .on_message(forward_register_result(
                &foreign,
                ForwardRegisterOutcome::Ok,
                FOREIGN_EPOCH,
                0,
            ))
            .await;
        shard
            .on_message(forward_register_result(
                &forward,
                ForwardRegisterOutcome::Ok,
                EPOCH,
                WATERMARK,
            ))
            .await;
        assert_eq!(
            login.await.expect("the login task ran to completion"),
            Ok(BoundSession {
                epoch: EPOCH,
                watermark: WATERMARK,
            }),
            "the login must bind the epoch addressed to it, and must still be \
             parked to receive it"
        );
    }

    /// A node that is primary itself never forwards -- that is what bounds a
    /// forward at one hop.
    #[compio::test]
    async fn primary_proposes_locally_instead_of_forwarding() {
        let bus = SpyBus::default();
        // Replica 0 of 3, view 0: this node IS the primary.
        let shard = Rc::new(test_shard(&bus, 0, 3, FIRST_BOOT));

        // No journal on the test shard, so the proposal cannot commit; what
        // matters is that nothing left over the interconnect.
        let _ = compio::time::timeout(
            Duration::from_millis(50),
            submit_register_local_or_forward(&shard, 0xCAFE, 7),
        )
        .await;
        assert!(
            bus.replica_sends.borrow().is_empty(),
            "a primary must propose in process"
        );
    }

    /// The nonce a shard booted at `incarnation` stamps on its first forward.
    /// Nobody answers, so the login abandons on the instant timer; the frame it
    /// left on the bus is what the caller is after.
    async fn first_forward_nonce(incarnation: u128) -> u128 {
        let bus = SpyBus::default();
        bus.instant_timers.set(true);
        let shard = Rc::new(test_shard(&bus, 1, 3, incarnation));
        let outcome = submit_register_local_or_forward(&shard, 0xCAFE, 7).await;
        assert_eq!(outcome, Err(MetadataSubmitError::ForwardTimedOut));
        bus.sole_replica_send::<ForwardRegisterHeader>().1.nonce
    }

    /// Let a spawned login run until it has parked on the primary's answer.
    async fn await_forward(bus: &SpyBus) {
        for _ in 0..1000 {
            if !bus.replica_sends.borrow().is_empty() {
                return;
            }
            compio::time::sleep(Duration::from_millis(1)).await;
        }
        panic!("the login never forwarded a register");
    }

    /// A sealed `ForwardRegisterResult` addressed to `forward`'s nonce.
    fn forward_register_result(
        forward: &ForwardRegisterHeader,
        outcome: ForwardRegisterOutcome,
        epoch: u64,
        watermark: u64,
    ) -> MessageBag {
        let bound = match outcome {
            ForwardRegisterOutcome::Ok => Ok(BoundSession { epoch, watermark }),
            ForwardRegisterOutcome::ClientIdOwnedByAnotherUser => {
                Err(MetadataSubmitError::ClientIdOwnedByAnotherUser)
            }
            _ => Err(MetadataSubmitError::NotPrimary),
        };
        MessageBag::ForwardRegisterResult(build_forward_register_result_message(
            forward.cluster,
            forward.view,
            0,
            forward.client,
            forward.nonce,
            &bound,
        ))
    }

    fn forward_logout_result_message(
        forward: &ForwardLogoutHeader,
        outcome: &Result<u64, MetadataSubmitError>,
    ) -> MessageBag {
        MessageBag::ForwardLogoutResult(build_forward_logout_result_message(
            forward.cluster,
            forward.view,
            0,
            forward.client,
            forward.nonce,
            outcome,
        ))
    }

    /// The `GET_CLUSTER_METADATA` auth gate holds on every roster shape: it
    /// describes the private replica network, and a client that dialed a
    /// backup reaches the cluster by logging in there (the backup forwards
    /// the register), not by reading the topology first.
    ///
    /// The denial must be a plain Reply on the status channel, not an
    /// Eviction: no session exists yet, and a session-terminal frame makes
    /// SDKs drop the connection their login is about to use.
    #[compio::test]
    async fn pre_auth_cluster_metadata_denied_on_every_roster() {
        use configs::cluster::{ClusterNodeConfig, TransportPorts};
        use iggy_binary_protocol::codes::GET_CLUSTER_METADATA_CODE;
        use iggy_binary_protocol::{GenericHeader, ReplyHeader};

        const TRANSPORT: u128 = 91;
        const COMMAND_OFFSET: usize = std::mem::offset_of!(GenericHeader, command);
        const STATUS_OFFSET: usize = std::mem::offset_of!(ReplyHeader, status);
        const OP_OFFSET: usize = std::mem::offset_of!(ReplyHeader, op);
        const COMMIT_OFFSET: usize = std::mem::offset_of!(ReplyHeader, commit);

        fn metadata_read() -> Message<GenericHeader> {
            let header_size = size_of::<RequestHeader>();
            let mut message = Message::<RequestHeader>::new(header_size);
            {
                let header = bytemuck::checked::from_bytes_mut::<RequestHeader>(
                    &mut message.as_mut_slice()[..header_size],
                );
                *header = RequestHeader {
                    command: Command::Request,
                    operation: Operation::NonReplicated,
                    size: u32::try_from(header_size).expect("header fits u32"),
                    client: TRANSPORT,
                    ..Default::default()
                };
                header.reserved[..4].copy_from_slice(&GET_CLUSTER_METADATA_CODE.to_le_bytes());
            }
            message.into_generic()
        }

        fn roster_node(name: &str) -> ClusterNodeConfig {
            ClusterNodeConfig {
                name: name.to_owned(),
                ip: "127.0.0.1".to_owned(),
                advertised_address: None,
                advertised_addresses: Vec::new(),
                replica_id: 0,
                ports: TransportPorts::default(),
            }
        }

        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, 0, 1, FIRST_BOOT));
        let sessions = Rc::new(RefCell::new(SessionManager::new()));
        let system_config = Arc::new(ServerSystemConfig::default());

        let multi_node = Rc::new(ClusterRoster {
            enabled: true,
            name: "test-cluster".to_owned(),
            nodes: vec![roster_node("node-0").into(), roster_node("node-1").into()],
            self_ip: "127.0.0.1".to_owned(),
            self_ports: TransportPorts::default(),
            metadata_view: Arc::new(std::sync::atomic::AtomicU64::new(
                crate::cluster_meta::METADATA_VIEW_UNKNOWN,
            )),
        });
        // Default roster is disabled / single node; the installed one is a
        // real cluster. Neither serves an unbound caller.
        for roster in [None, Some(multi_node)] {
            if let Some(roster) = roster {
                sessions.borrow_mut().set_cluster_roster(roster);
            }
            handle_client_request(
                &shard,
                &sessions,
                &system_config,
                1,
                TRANSPORT,
                metadata_read(),
            )
            .await;
            let replies = bus.client_replies.borrow();
            assert_eq!(replies.len(), 1, "gated read must still produce a frame");
            let (client, frame) = &replies[0];
            assert_eq!(*client, TRANSPORT);
            assert_eq!(
                frame[COMMAND_OFFSET],
                Command::Reply as u8,
                "an unbound cluster-metadata read must be denied with a Reply, not evicted"
            );
            let status =
                u32::from_le_bytes(frame[STATUS_OFFSET..STATUS_OFFSET + 4].try_into().unwrap());
            assert_eq!(
                status,
                IggyError::Unauthenticated.as_code(),
                "deny reply status must be Unauthenticated"
            );
            let op = u64::from_le_bytes(frame[OP_OFFSET..OP_OFFSET + 8].try_into().unwrap());
            assert_eq!(op, 0, "pre-auth deny carries no session, so op must be 0");
            let commit =
                u64::from_le_bytes(frame[COMMIT_OFFSET..COMMIT_OFFSET + 8].try_into().unwrap());
            assert_eq!(commit, 0, "pre-auth deny must not disclose commit activity");
            drop(replies);
            bus.client_replies.borrow_mut().clear();
        }
    }

    #[test]
    fn create_topic_bounds_deny_pre_consensus() {
        let segment_size = iggy_common::DEFAULT_SEGMENT_SIZE;
        assert!(segment_size > 0, "default segment size must be nonzero");

        assert!(
            validate_topic_bounds(
                MAX_PARTITIONS_PER_REQUEST,
                MaxTopicSize::ServerDefault,
                segment_size
            )
            .is_ok(),
            "the partition cap itself is admissible"
        );
        assert!(
            matches!(
                validate_topic_bounds(
                    MAX_PARTITIONS_PER_REQUEST + 1,
                    MaxTopicSize::ServerDefault,
                    segment_size
                ),
                Err(IggyError::TooManyPartitions)
            ),
            "one past the partition cap must deny"
        );
        // ServerDefault is numerically 0 yet exempt from the segment-size
        // floor: it resolves against server config, matching legacy.
        assert!(validate_topic_bounds(1, MaxTopicSize::ServerDefault, segment_size).is_ok());
        assert!(validate_topic_bounds(1, MaxTopicSize::Unlimited, segment_size).is_ok());
        let below_floor = MaxTopicSize::Custom((segment_size - 1).into());
        assert!(
            matches!(
                validate_topic_bounds(1, below_floor, segment_size),
                Err(IggyError::InvalidTopicSize(size, floor))
                    if size == below_floor && floor == IggyByteSize::from(segment_size)
            ),
            "custom size below the segment size must deny with the bounds"
        );
        let at_floor = MaxTopicSize::Custom(IggyByteSize::from(segment_size));
        assert!(
            validate_topic_bounds(1, at_floor, segment_size).is_ok(),
            "a topic exactly one segment large is admissible"
        );
    }

    #[test]
    fn partitions_count_cap_denies_pre_consensus() {
        assert!(
            validate_partitions_count(MAX_PARTITIONS_PER_REQUEST).is_ok(),
            "the cap itself is admissible"
        );
        assert!(
            matches!(
                validate_partitions_count(MAX_PARTITIONS_PER_REQUEST + 1),
                Err(IggyError::TooManyPartitions)
            ),
            "one past the cap must deny"
        );
        // Zero passes the shared cap because a zero-partition TOPIC is legal
        // (legacy `create_topic` admits `0..=MAX`).
        assert!(validate_partitions_count(0).is_ok());
    }

    #[test]
    fn zero_partitions_change_denies_pre_consensus() {
        // Adding or removing zero partitions is a no-op that would still burn
        // a replicated log entry and force a rebalance. Legacy rejects it with
        // `TooManyPartitions` in both handlers, so the code matches.
        assert!(
            matches!(
                validate_partitions_change_count(0),
                Err(IggyError::TooManyPartitions)
            ),
            "adding or removing zero partitions must deny"
        );
        assert!(validate_partitions_change_count(1).is_ok());
        assert!(validate_partitions_change_count(MAX_PARTITIONS_PER_REQUEST).is_ok());
        assert!(
            matches!(
                validate_partitions_change_count(MAX_PARTITIONS_PER_REQUEST + 1),
                Err(IggyError::TooManyPartitions)
            ),
            "the cap still applies"
        );
    }
}
