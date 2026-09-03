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

//! Per-shard request dispatch: queue plumbing and the request funnel.
//!
//! The tree: [`session_ops`] (login/register/logout and their replica
//! forwards), [`partition`] (the partition data plane, both mesh ends),
//! [`reads`] (the non-replicated read router), [`submit`] (the shard-0
//! metadata-submit RPC), `authz` (the wire-path authorization gates).
//!
//! Deliberate asymmetry (the two authz gates): replicated metadata ops are
//! authorized in-apply by the STM, in committed order on every replica;
//! partition and non-replicated ops never enter the metadata log, so `authz`
//! gates them pre-dispatch against this shard's applied permissioner. The
//! HTTP spine keeps its own equivalent gates (see `crate::http`) because its
//! error contract (404-before-403) is pinned client-visible behavior.

mod authz;
pub mod login_error;
pub mod partition;
pub mod reads;
pub mod session_ops;
pub mod submit;
#[cfg(test)]
mod test_support;

use crate::consumer_group::maybe_rewrite_consumer_group_request;
use crate::dispatch::authz::{send_deny_reply, send_unbound_deny_reply};
use crate::dispatch::partition::{dispatch_partition_request, handle_delete_segments_request};
use crate::dispatch::reads::handle_non_replicated_request;
use crate::dispatch::session_ops::{
    handle_login_register_request, handle_logout_request, send_login_eviction,
    send_unauthenticated_eviction, submit_disconnect_logout,
};
use crate::dispatch::submit::{committed_reply_commit, submit_client_request_on_owner};
use crate::pat::maybe_rewrite_pat_request;
use crate::responses::{
    NonReplicatedResponse, build_deny_reply, build_raw_pat_reply, current_metadata_commit,
};
use crate::segment_cleaner::UNENFORCEABLE_TOPIC_SIZE_WARN;
use crate::session_manager::SessionManager;
use crate::shell::{ShellBus, ShellShard, ShellShardHandle};
use crate::users::maybe_rewrite_user_password_request;
use crate::wire::{request_body, verify_request_checksum};
use bytes::Bytes;
use configs::server::ServerSystemConfig;
use consensus::MetadataHandle;
use iggy_binary_protocol::PrepareHeader;
use iggy_binary_protocol::codes::{
    GET_CLUSTER_METADATA_CODE, LOGIN_USER_CODE, LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE, PING_CODE,
};
use iggy_binary_protocol::requests::partitions::{
    CreatePartitionsRequest, DeletePartitionsRequest,
};
use iggy_binary_protocol::requests::streams::{CreateStreamRequest, UpdateStreamRequest};
use iggy_binary_protocol::requests::topics::{CreateTopicRequest, UpdateTopicRequest};
use iggy_binary_protocol::requests::users::{CreateUserRequest, UpdateUserRequest};
use iggy_binary_protocol::{
    EvictionReason, GenericHeader, MAX_PARTITIONS_PER_REQUEST, Operation, RequestHeader,
    RoutedRequestHeader, WireDecode, WireIdentifier, WireOptions,
};
use iggy_common::{
    IggyByteSize, IggyError, MaxTopicSize, TopicCreateOptions, UPDATABLE_STREAM_OPTION_KEYS,
    UPDATABLE_TOPIC_OPTION_KEYS, UPDATABLE_USER_OPTION_KEYS, validate_preallocated_topic_bytes,
    validate_topic_segment_size,
};
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use message_bus::BusMessage;
use message_bus::client_listener::RequestHandler;
use message_bus::replica::listener::MessageHandler;
use metadata::impls::metadata::StreamsFrontend;
use metadata::stm::stream::Streams;
use server_common::Message;
use shard::{ConnectedClientInfo, ListClientsHandler};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::rc::Rc;
use std::sync::Arc;
use tracing::{debug, warn};

type ClientRequestQueues = Rc<RefCell<HashMap<u128, VecDeque<Message<GenericHeader>>>>>;
type ActiveClientRequests = Rc<RefCell<HashSet<u128>>>;

pub fn make_client_request_handler<B, MJ, S, SB>(
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
pub fn make_list_clients_handler(sessions: &Rc<RefCell<SessionManager>>) -> ListClientsHandler {
    let sessions = Rc::clone(sessions);
    Rc::new(move |reply| {
        let clients: Vec<ConnectedClientInfo> = sessions.borrow().iter_clients().collect();
        // Best-effort: the gather side bounds itself by count + timeout, so
        // a dropped reply (receiver gone) just means this shard is omitted.
        let _ = reply.try_send(clients);
    })
}

pub fn make_deferred_replica_message_handler<B, MJ, S, SB>(
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

pub fn make_deferred_client_request_handler<B, MJ, S, SB>(
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
const fn validate_partitions_count(partitions_count: u32) -> Result<(), IggyError> {
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
const fn validate_partitions_change_count(partitions_count: u32) -> Result<(), IggyError> {
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
pub fn validate_topic_bounds(
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
pub fn validate_topic_size_floor(
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
pub fn warn_unenforceable_topic_size(
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
pub fn warn_unenforceable_topic_size_on_partition_add(
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
pub fn validate_option_keys(options: &WireOptions, known: &[&str]) -> Result<(), IggyError> {
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
            // Recorded before the reply reaches the socket, so a read the client
            // sends the instant it decodes this frame already sees the mark.
            if let Some(commit) = committed_reply_commit(&reply) {
                sessions
                    .borrow_mut()
                    .record_metadata_watermark(transport_client_id, commit);
            }
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
    send_reply_frame(
        shard,
        transport_client_id,
        reply.into_generic().into_frozen(),
        label,
    )
    .await;
}

/// Hand a built reply frame to the bus for `transport_client_id`.
#[allow(clippy::future_not_send)]
async fn send_reply_frame<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    transport_client_id: u128,
    frame: impl Into<BusMessage>,
    label: &'static str,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    if let Err(error) = shard.bus.send_to_client(transport_client_id, frame).await {
        warn!(transport_client_id, label, error = %error, "failed to send non-replicated reply");
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

pub(in crate::dispatch) fn upgrade_shard_handle<B, MJ, S, SB>(
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
    use crate::cluster_meta::ClusterRoster;
    use crate::dispatch::test_support::{FIRST_BOOT, SpyBus, TestMux, TestShard, test_shard};
    use iggy_binary_protocol::Command;
    use metadata::IggyMetadata;
    use partitions::{IggyPartitions, PartitionPathLayout, PartitionsConfig};
    use server_common::MESSAGE_ALIGN;
    use server_common::sharding::ShardId;
    use shard::metrics::ShardMetrics;
    use shard::shards_table::PapayaShardsTable;
    use shard::{
        LifecycleFrame, PartitionConsensusConfig, ReplicaTopology, ShardFrame, ShardIdentity,
        shard_channel,
    };
    use std::mem::size_of;
    use std::sync::atomic::AtomicBool;

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
            msg: server_common::iobuf::Frozen::from(
                server_common::iobuf::Owned::<MESSAGE_ALIGN>::zeroed(64),
            )
            .into(),
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
            pump_shard
                .run_message_pump(stop_rx, Arc::new(AtomicBool::new(false)))
                .await;
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
        shard
            .run_message_pump(stop_rx, Arc::new(AtomicBool::new(false)))
            .await;

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
            nodes: ["node-0", "node-1"]
                .map(|name| {
                    configs::cluster::ResolvedClusterNode::try_from(roster_node(name))
                        .expect("valid roster node")
                })
                .to_vec(),
            self_advertised: "127.0.0.1".to_owned(),
            configured_ports: TransportPorts::default(),
            bound_ports: Arc::default(),
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
