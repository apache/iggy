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
//! metadata-submit RPC), `authz` (the wire-path authorization gates),
//! `failure` (the wire failure channels and the one send exit for
//! host-built frames).
//!
//! Deliberate asymmetry (the two authz gates): replicated metadata ops are
//! authorized in-apply by the STM, in committed order on every replica;
//! partition and non-replicated ops never enter the metadata log, so `authz`
//! gates them pre-dispatch against this shard's applied permissioner. The
//! HTTP spine keeps its own equivalent gates (see `crate::http`) because its
//! error contract (404-before-403) is pinned client-visible behavior.

mod authz;
mod failure;
pub mod login_error;
pub mod partition;
pub mod reads;
pub mod session_ops;
pub mod submit;
#[cfg(test)]
mod test_support;

use crate::consumer_group::maybe_rewrite_consumer_group_request;
use crate::dispatch::failure::{
    FrameChannel, send_deny_reply, send_eviction, send_host_frame, send_pre_consensus_deny,
    send_unbound_deny_reply,
};
use crate::dispatch::partition::{dispatch_partition_request, handle_delete_segments_request};
use crate::dispatch::reads::handle_non_replicated_request;
use crate::dispatch::session_ops::{
    handle_login_register_request, handle_logout_request, submit_disconnect_logout,
};
use crate::dispatch::submit::{committed_reply_commit, submit_client_request_on_owner};
use crate::responses::build_raw_pat_reply;
use crate::rewrite::{RewriteDeny, RewriteStage, tcp_chain};
use crate::session_manager::{ConnectionContext, SessionManager};
use crate::shell::{ShellBus, ShellShard, ShellShardHandle};
use crate::wire::verify_request_checksum;
use ahash::{AHashMap, AHashSet};
use configs::server::ServerSystemConfig;
use iggy_binary_protocol::PrepareHeader;
use iggy_binary_protocol::codes::{
    LOGIN_USER_CODE, LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE, PING_CODE,
};
use iggy_binary_protocol::{
    EvictionReason, GenericHeader, Operation, RequestHeader, RoutedRequestHeader,
};
use iggy_common::IggyError;
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use message_bus::client_listener::RequestHandler;
use message_bus::replica::listener::MessageHandler;
use server_common::Message;
use shard::{ConnectedClientInfo, ListClientsHandler};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::Arc;
use tracing::{debug, error, warn};

type ClientRequestQueues = Rc<RefCell<AHashMap<u128, VecDeque<Message<GenericHeader>>>>>;

/// Requests one client may have queued behind a request this shard has not
/// answered yet.
///
/// The drain loop below serves one frame per client at a time, and a frame can
/// legitimately hold it for a while: a metadata write awaits consensus, and a
/// read can be HELD for the read-your-writes budget (see
/// `crate::dispatch::reads`). Without a cap, a client that keeps pipelining
/// through such a stall grows its queue - and this node's memory - unbounded.
///
/// Overflow is ANSWERED, not dropped: `TransientNotAccepted` is the honest
/// code, since the frame provably never entered any pipeline, so the SDK may
/// re-issue it anywhere, including here once the queue drains. Sized far above
/// any SDK's in-flight window, so it only ever fires under a genuine stall.
const MAX_QUEUED_CLIENT_REQUESTS: usize = 1024;
type ActiveClientRequests = Rc<RefCell<AHashSet<u128>>>;

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

/// Build the shard's one client-request handler: per-client FIFO queues
/// drained one task per client, and the bus connection-lost hook that
/// logs a dropped connection out. Every transport on the shard must
/// share the instance (shard 0 hands it to its local QUIC, TCP-TLS and
/// WSS listeners as well), or a client's ordering guarantee and the
/// disconnect hook split by transport.
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
    let queues: ClientRequestQueues = Rc::new(RefCell::new(AHashMap::new()));
    let active: ActiveClientRequests = Rc::new(RefCell::new(AHashSet::new()));
    let queues_for_disconnect = Rc::clone(&queues);
    let sessions_for_disconnect = Rc::clone(&sessions);
    let shard_handle_for_disconnect = Rc::clone(&shard_handle);
    let bus_for_spawn = (*bus).clone();
    bus.set_client_connection_lost_fn(Rc::new(move |client_id| {
        // The socket is gone, so nothing will drain what a live drain task
        // left queued. The active slot is NOT released here: the transport
        // task runs this hook while a drain may be suspended at an `.await`,
        // and clearing the slot would let a frame the dispatch task still has
        // buffered spawn a second drain over the same queue. The drain task's
        // own guard covers every exit, the panic compio catches included.
        queues_for_disconnect.borrow_mut().remove(&client_id);
        // Upgrade FIRST: `remove_connection` strips the `SessionManager`
        // entry, so running it ahead of a failed upgrade would drop the
        // binding without ever submitting the replicated `Logout`, leaking
        // the `ClientTable` entry and its consumer-group memberships. The
        // window is pre-build / post-runtime-drop only.
        let Some(shard) = upgrade_shard_handle(&shard_handle_for_disconnect) else {
            // Nothing reaps what stays behind: the heartbeat verifier is
            // optional and only collects `Bound` / `Authenticated` sessions,
            // so a `Connected` row survives to process exit.
            error!(
                client_id,
                "client connection lost with no live shard; session and client-table entries \
                 leak until process exit"
            );
            return;
        };
        if let Some((vsr_client_id, session)) = sessions_for_disconnect
            .borrow_mut()
            .remove_connection(client_id)
        {
            submit_disconnect_logout(shard, vsr_client_id, session);
        }
    }));
    Rc::new(move |client_id, message| {
        enqueue_client_request(
            &bus_for_spawn,
            &shard_handle,
            &sessions,
            &system_config,
            max_tokens_per_user,
            &queues,
            &active,
            client_id,
            message,
        );
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
    bus: &B,
    shard_handle: &ShellShardHandle<B, MJ, S, SB>,
    sessions: &Rc<RefCell<SessionManager>>,
    system_config: &Arc<ServerSystemConfig>,
    max_tokens_per_user: u32,
    queues: &ClientRequestQueues,
    active: &ActiveClientRequests,
    client_id: u128,
    message: Message<GenericHeader>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    {
        let mut queues = queues.borrow_mut();
        let queue = queues.entry(client_id).or_default();
        if queue.len() >= MAX_QUEUED_CLIENT_REQUESTS {
            // Borrow released before the deny, which spawns onto this same
            // task and would otherwise re-enter the table.
            drop(queues);
            deny_overflowing_client_request(shard_handle, client_id, message);
            return;
        }
        queue.push_back(message);
    }
    if !active.borrow_mut().insert(client_id) {
        return;
    }

    let shard_handle = Rc::clone(shard_handle);
    let sessions = Rc::clone(sessions);
    let system_config = Arc::clone(system_config);
    let queues = Rc::clone(queues);
    let active = Rc::clone(active);
    bus.spawn(async move {
        let _slot = ActiveDrainSlot { active, client_id };
        // The handle is set once the shard is built. A frame that beats it
        // stays queued and the client's next frame drains both, which needs
        // the slot released on this path too - the guard above does it.
        let Some(shard) = upgrade_shard_handle(&shard_handle) else {
            return;
        };
        drain_client_requests(
            shard,
            sessions,
            system_config,
            max_tokens_per_user,
            queues,
            client_id,
        )
        .await;
    });
}

/// Answer a request that arrived with this client's queue already at
/// [`MAX_QUEUED_CLIENT_REQUESTS`] with the retryable transient denial.
///
/// Spawned rather than awaited: the enqueue path is sync (it runs straight off
/// frame arrival) and the reply goes out on the bus. Two shapes are dropped
/// rather than answered: a frame whose header will not even cast, exactly as
/// the drain loop drops it, and a frame that arrives before the shard is
/// built, which leaves nothing to render a reply from.
fn deny_overflowing_client_request<B, MJ, S, SB>(
    shard_handle: &ShellShardHandle<B, MJ, S, SB>,
    transport_client_id: u128,
    message: Message<GenericHeader>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    // Unreachable in practice: a listener binds only after the build backfills
    // the weak self-reference, which `crate::boot` pins with a `debug_assert`.
    // Dropped rather than admitted past the cap -- the cap exists to refuse
    // this frame, and the deny itself needs the shard for its metric and reply.
    let Some(shard) = upgrade_shard_handle(shard_handle) else {
        warn!(
            transport_client_id,
            "dropping over-queue client request received before the shard was built"
        );
        return;
    };
    shard.metrics().record_client_request_denied_queue_full();
    let Ok(request) = message.try_into_typed::<RequestHeader>() else {
        warn!(
            transport_client_id,
            "dropping over-queue client request with invalid header"
        );
        return;
    };
    let request = request.into_routed();
    debug!(
        transport_client_id,
        operation = ?request.header().operation,
        queued = MAX_QUEUED_CLIENT_REQUESTS,
        "denying client request retryable: this connection's request queue is full"
    );
    let bus = shard.bus.clone();
    bus.spawn(async move {
        send_deny_reply(
            &shard,
            transport_client_id,
            request.header(),
            IggyError::TransientNotAccepted.as_code(),
        )
        .await;
    });
}

/// Holds a client's one drain slot for as long as its drain task lives.
///
/// Releasing from the task's own `Drop` covers every exit, the panic compio
/// catches included: a slot left taken with no live drain queues every later
/// frame for that client forever. It also keeps the release out of the
/// connection-lost hook, which fires from the transport task and could
/// otherwise clear the slot under a drain suspended at an `.await`.
struct ActiveDrainSlot {
    active: ActiveClientRequests,
    client_id: u128,
}

impl Drop for ActiveDrainSlot {
    fn drop(&mut self) {
        self.active.borrow_mut().remove(&self.client_id);
    }
}

#[allow(clippy::future_not_send)]
async fn drain_client_requests<B, MJ, S, SB>(
    shard: Rc<ShellShard<B, MJ, S, SB>>,
    sessions: Rc<RefCell<SessionManager>>,
    system_config: Arc<ServerSystemConfig>,
    max_tokens_per_user: u32,
    queues: ClientRequestQueues,
    client_id: u128,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    loop {
        let Some(message) = pop_next_client_request(&queues, client_id) else {
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
    client_id: u128,
) -> Option<Message<GenericHeader>> {
    // Freed as soon as it drains empty. Keeping it would cost the connection
    // one `VecDeque` (at its peak depth, since `pop_front` never gives
    // capacity back), and the connection-lost hook cannot be the sole owner:
    // the dispatch task can still deliver a buffered frame after the hook
    // ran, and `client_id` is never reused, so that entry would live until
    // process exit.
    let mut queues = queues.borrow_mut();
    let queue = queues.get_mut(&client_id)?;
    let message = queue.pop_front();
    if queue.is_empty() {
        queues.remove(&client_id);
    }
    message
}

/// Where the funnel routes a client request. Derived by [`classify`], which
/// IS the routing: [`handle_client_request`] matches on its result. The
/// variant ORDER mirrors the order of the checks inside [`classify`], and
/// that order is semantics (documented there).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::dispatch) enum RequestClass {
    /// Legacy pre-register login code: rejected with a typed
    /// `MalformedLogin` eviction before the session gate.
    LegacyLogin,
    /// Non-replicated code other than PING on an unbound transport:
    /// denied `Unauthenticated` (plain deny reply, never an eviction).
    UnauthenticatedRead,
    /// Non-replicated read for the reads router.
    NonReplicatedRead,
    /// The register handshake (`session == 0 && request == 0`).
    LoginRegister,
    Logout,
    /// Replicated operation on an unbound transport: `Eviction(NoSession)`.
    UnboundReplicated,
    /// Neither a partition nor a metadata consensus op: resolved to a
    /// replicated `TruncatePartition` by the owning shard.
    DeleteSegments,
    /// Partition-plane operation.
    Partition,
    /// Everything else: replicated metadata consensus.
    ReplicatedMetadata,
}

/// Route a client request: [`handle_client_request`] matches on the result,
/// so this function IS the routing and the order of the checks below is the
/// semantics. Pure: `bound` stands for
/// `sessions.get_session(transport_client_id).is_some()`, the only session
/// fact the checks consult.
///
/// The pins, in check order:
/// - the legacy-login rejection precedes the session gate: a legacy code
///   must get the typed `MalformedLogin` eviction, not the generic
///   unauthenticated deny;
/// - the pre-auth allowlist is PING only; `GET_CLUSTER_METADATA` is
///   deliberately NOT pre-auth (see the funnel's auth-bypass guard);
/// - poll-messages and consumer-offset reads are non-replicated CODES, not
///   partition operations: they classify
///   [`RequestClass::NonReplicatedRead`] and route inside the reads router;
/// - a `Register` with `session != 0` or `request != 0` falls through to
///   the default. Those rows are classify-only: `RequestHeader::validate`
///   rejects such a header before the funnel sees it, so no mid-session
///   register ever reaches consensus;
/// - `DeleteSegments` is neither a partition nor a metadata op, so its
///   check sits before `is_partition`;
/// - the checksum and heartbeat pre-gates run BEFORE classification in the
///   funnel.
pub(in crate::dispatch) fn classify(header: &RoutedRequestHeader, bound: bool) -> RequestClass {
    if header.operation == Operation::NonReplicated {
        let nr_code = non_replicated_code(header);
        if matches!(
            nr_code,
            LOGIN_USER_CODE | LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE
        ) {
            return RequestClass::LegacyLogin;
        }
        if nr_code != PING_CODE && !bound {
            return RequestClass::UnauthenticatedRead;
        }
        return RequestClass::NonReplicatedRead;
    }
    if header.operation == Operation::Register && header.session == 0 && header.request == 0 {
        return RequestClass::LoginRegister;
    }
    if header.operation == Operation::Logout {
        return RequestClass::Logout;
    }
    if !bound {
        return RequestClass::UnboundReplicated;
    }
    if header.operation == Operation::DeleteSegments {
        return RequestClass::DeleteSegments;
    }
    if header.operation.is_partition() {
        return RequestClass::Partition;
    }
    RequestClass::ReplicatedMetadata
}

/// The command code a `NonReplicated` header carries in its first four
/// reserved bytes.
fn non_replicated_code(header: &RoutedRequestHeader) -> u32 {
    u32::from_le_bytes(header.reserved[..4].try_into().unwrap())
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

    // ONE `connections` walk for the whole prologue. Any request is liveness
    // proof, not just PING: an idle-but-active client (e.g. an admin issuing
    // reads between long sleeps) must not be evicted by the heartbeat
    // verifier. A genuinely dead connection sends nothing, so the intended
    // stale-client eviction still fires.
    // Bound, not matched: a `match` on `borrow_mut()` would hold the guard
    // across the arm that borrows again.
    let mut touched = sessions.borrow_mut().touch_connection(transport_client_id);
    if touched.is_none() {
        // A transport's first frame: the peer address and transport kind live
        // on the bus, so only this path pays that lookup.
        ensure_transport_connection(shard, sessions, transport_client_id);
        touched = sessions.borrow_mut().touch_connection(transport_client_id);
    }
    let ConnectionContext {
        bound,
        user_id,
        address: client_address,
        metadata_watermark,
    } = touched.unwrap_or_default();

    // Borrowed, not copied: the 256-byte header is only worth a by-value
    // snapshot where an arm rewrites it (`ReplicatedMetadata`) and still has
    // to echo the client's original fields on a deny.
    let header = request.header();
    match classify(header, bound.is_some()) {
        RequestClass::LegacyLogin => {
            // Legacy (pre-register) login codes. The server authenticates only via
            // the Register handshake (LOGIN_REGISTER / LOGIN_REGISTER_WITH_PAT,
            // Operation::Register); the vsr SDK funnels both logins there and never
            // emits these. Reject them uniformly with a typed MalformedLogin (the
            // SDK maps it to InvalidFormat) before the session gate, so a legacy or
            // foreign client fails fast instead of getting the generic
            // Unauthenticated deny the pre-auth guard would send unbound, or the
            // silent empty-ok Reply the bound non-replicated path would send.
            let nr_code = non_replicated_code(header);
            warn!(
                transport_client_id,
                code = nr_code,
                "rejecting legacy login code; server requires the register handshake"
            );
            send_eviction(
                shard,
                transport_client_id,
                header.client,
                EvictionReason::MalformedLogin,
                "legacy_login_rejection",
            )
            .await;
        }
        RequestClass::UnauthenticatedRead => {
            let nr_code = non_replicated_code(header);
            // No per-code exemption: every in-tree SDK reads the roster only
            // after login, so an unauthenticated roster read is a real event
            // and not something to hide at debug.
            warn!(
                transport_client_id,
                code = nr_code,
                "denying pre-auth non-replicated read with Unauthenticated"
            );
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
        }
        RequestClass::NonReplicatedRead => {
            // The auth-bypass guard is `classify`'s `UnauthenticatedRead` class:
            // `PING`, the liveness probe, is the only pre-auth code, on every
            // roster shape. `GET_CLUSTER_METADATA` describes the private replica
            // network and is not something an unauthenticated caller gets to
            // read; a client that dialed a backup no longer needs it to find the
            // leader, because the backup authenticates the login locally and
            // forwards only the consensus proposal
            // (`submit_register_local_or_forward`). Every other non-replicated
            // code MUST go through Register first, which binds the acting user
            // the per-op authz gates resolve.
            handle_non_replicated_request(
                shard,
                sessions,
                system_config,
                transport_client_id,
                request,
                (user_id, client_address, metadata_watermark),
            )
            .await;
        }
        RequestClass::LoginRegister => {
            handle_login_register_request(shard, sessions, transport_client_id, request).await;
        }
        RequestClass::Logout => {
            handle_logout_request(shard, sessions, transport_client_id, request).await;
        }
        RequestClass::UnboundReplicated => {
            // Replicated request on an unbound transport. Without this short-
            // circuit, the rewrite below overwrites `header.client` with
            // `transport_client_id` and dispatches; the request_preflight then
            // rejects with `NoSession`/`Fenced` and the failure disappears
            // silently, wedging the SDK until the socket timeout. A typed
            // `Eviction(NoSession)` is right here, unlike the plain deny of
            // `UnauthenticatedRead`: a replicated request implies the client
            // believes it has a session, and that session is gone, so it must
            // register again. An empty status-0 Reply is not safe here, because
            // SendMessages is the one replicated operation without a result
            // section, and its decoder would read the empty body as a
            // successful send.
            warn!(
                transport_client_id,
                operation = ?header.operation,
                "rejecting replicated request from unbound transport with Eviction(NoSession)"
            );
            // The eviction context is best-effort off the metadata consensus
            // (peer shards have none; zeroes are cosmetic -- the SDK only
            // reads the reason), and the evicted id is the transport id: no
            // VSR session exists to name.
            send_eviction(
                shard,
                transport_client_id,
                transport_client_id,
                EvictionReason::NoSession,
                "unbound_replicated_request",
            )
            .await;
        }
        RequestClass::DeleteSegments => {
            // DeleteSegments is neither a partition nor a metadata consensus op: the
            // owning shard resolves the requested count to a concrete offset, then a
            // `TruncatePartition` is replicated through metadata (Option A). Each
            // replica's reconciler trims to the committed watermark. `classify`
            // names it ahead of the partition and metadata classes.
            handle_delete_segments_request(shard, transport_client_id, bound, &request).await;
        }
        RequestClass::Partition => {
            // `bound` is Some here: `classify` sends unbound transports to
            // `UnboundReplicated`.
            let (vsr_client_id, bound_session) = bound.unwrap_or((0, 0));
            // The acting user comes from the prologue's lookup. A bound
            // transport always has one, but the gate below fails closed on
            // `None` rather than trust that.
            dispatch_partition_request(
                shard,
                request,
                vsr_client_id,
                bound_session,
                transport_client_id,
                user_id,
            )
            .await;
        }
        RequestClass::ReplicatedMetadata => {
            // The one arm that needs the by-value copy: the rewrite below
            // stamps the consensus client / session / group over the header,
            // and a pre-consensus deny still has to echo what the client sent.
            let header = *header;
            let request =
                request.transmute_header(|header, new_header: &mut RoutedRequestHeader| {
                    *new_header = header;
                    // Metadata-plane ops route by operation: stamp the sentinel group.
                    new_header.group = server_common::sharding::METADATA_GROUP;
                    // `bound` is always Some here (`classify` sends unbound transports to
                    // `UnboundReplicated`); this sets the consensus client id + session
                    // for the replicated op.
                    if let Some((bound_client_id, bound_session)) = bound {
                        new_header.client = bound_client_id;
                        new_header.session = bound_session;
                    }
                });
            let (request, raw_pat_token) = match tcp_chain(
                shard,
                sessions,
                transport_client_id,
                max_tokens_per_user,
                request,
            ) {
                Ok(rewritten) => rewritten,
                Err(RewriteDeny { stage, error }) => {
                    send_pre_consensus_deny(shard, transport_client_id, &header, &error, stage)
                        .await;
                    return;
                }
            };
            // Enrich consumer-group Join/Leave with the client's VSR id (+ topic
            // partition count for Join) before replication; see `crate::consumer_group`.
            let request = match maybe_rewrite_consumer_group_request(shard, request).await {
                Ok(rewritten) => rewritten,
                Err(error) => {
                    // Both of the rewrite's own failures are `InvalidCommand`
                    // decode errors, so a replay cannot help: deny typed
                    // instead of leaving the lockstep connection to its read
                    // timeout. (Its third error path needs a body past
                    // `u32::MAX` against a 64 MiB message cap, so no client
                    // frame reaches it; the deny is correct there too.)
                    send_pre_consensus_deny(
                        shard,
                        transport_client_id,
                        &header,
                        &error,
                        RewriteStage::ConsumerGroup,
                    )
                    .await;
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
                            // The op COMMITTED; only the reply could not be
                            // rendered. A typed deny is still the right frame:
                            // silence wedges the lockstep connection on a
                            // request that succeeded server-side. The raw
                            // token is unrecoverable either way -- it lives in
                            // that one reply and a retry dedups to the empty
                            // committed body -- so the caller must delete the
                            // token and mint a new one.
                            send_deny_reply(shard, transport_client_id, &header, error.as_code())
                                .await;
                            return;
                        }
                    };
                    send_host_frame(
                        &shard.bus,
                        transport_client_id,
                        reply.into_frozen(),
                        FrameChannel::Reply,
                        "committed_reply",
                    )
                    .await;
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
    use iggy_binary_protocol::codes::{
        GET_CLUSTER_METADATA_CODE, GET_CONSUMER_OFFSET_CODE, POLL_MESSAGES_CODE,
    };
    use iggy_binary_protocol::{EvictionHeader, ReplyHeader};
    use journal::prepare_journal::PrepareJournal;
    use metadata::IggyMetadata;
    use metadata::impls::metadata::IggySnapshot;
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

    /// Client-wire frame for the funnel tests, shaped like the SDK sends it
    /// (the funnel promotes it to the routed shape itself).
    fn wire_request(
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
                command: Command::Request,
                operation,
                size: u32::try_from(total).expect("test request fits u32"),
                client,
                session,
                request,
                ..Default::default()
            };
        }
        message
    }

    fn non_replicated_request(client: u128, nr_code: u32) -> Message<GenericHeader> {
        let mut message = wire_request(Operation::NonReplicated, client, 0, 0, &[]);
        {
            let header_size = size_of::<RequestHeader>();
            let header = bytemuck::checked::from_bytes_mut::<RequestHeader>(
                &mut message.as_mut_slice()[..header_size],
            );
            header.reserved[..4].copy_from_slice(&nr_code.to_le_bytes());
        }
        message.into_generic()
    }

    const fn frame_command(frame: &[u8]) -> u8 {
        frame[std::mem::offset_of!(GenericHeader, command)]
    }

    const fn eviction_reason_byte(frame: &[u8]) -> u8 {
        frame[std::mem::offset_of!(EvictionHeader, reason)]
    }

    fn reply_status(frame: &[u8]) -> u32 {
        const STATUS_OFFSET: usize = std::mem::offset_of!(ReplyHeader, status);
        u32::from_le_bytes(frame[STATUS_OFFSET..STATUS_OFFSET + 4].try_into().unwrap())
    }

    /// The classifier is the funnel's probe chain as data: every row pins
    /// one routing decision, and the labeled rows pin the probe ORDERINGS
    /// the funnel relies on.
    #[allow(clippy::too_many_lines)]
    #[test]
    fn classify_pins_probe_order() {
        fn routed(operation: Operation, session: u64, request: u64) -> RoutedRequestHeader {
            RoutedRequestHeader {
                command: Command::Request,
                operation,
                client: 7,
                session,
                request,
                ..Default::default()
            }
        }
        fn nr(nr_code: u32) -> RoutedRequestHeader {
            let mut header = routed(Operation::NonReplicated, 0, 0);
            header.reserved[..4].copy_from_slice(&nr_code.to_le_bytes());
            header
        }
        let table = [
            (
                "legacy login beats the session gate (unbound)",
                nr(LOGIN_USER_CODE),
                false,
                RequestClass::LegacyLogin,
            ),
            (
                "legacy login beats the bound reads route",
                nr(LOGIN_USER_CODE),
                true,
                RequestClass::LegacyLogin,
            ),
            (
                "legacy PAT login beats the session gate (unbound)",
                nr(LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE),
                false,
                RequestClass::LegacyLogin,
            ),
            (
                "legacy PAT login beats the bound reads route",
                nr(LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE),
                true,
                RequestClass::LegacyLogin,
            ),
            (
                "ping is the only pre-auth code",
                nr(PING_CODE),
                false,
                RequestClass::NonReplicatedRead,
            ),
            (
                "cluster metadata is not pre-auth",
                nr(GET_CLUSTER_METADATA_CODE),
                false,
                RequestClass::UnauthenticatedRead,
            ),
            (
                "poll-messages is a non-replicated code, not a partition op",
                nr(POLL_MESSAGES_CODE),
                true,
                RequestClass::NonReplicatedRead,
            ),
            (
                "consumer-offset read is a non-replicated code, not a partition op",
                nr(GET_CONSUMER_OFFSET_CODE),
                true,
                RequestClass::NonReplicatedRead,
            ),
            (
                "the register handshake",
                routed(Operation::Register, 0, 0),
                false,
                RequestClass::LoginRegister,
            ),
            (
                "register with a session falls to the default",
                routed(Operation::Register, 1, 0),
                true,
                RequestClass::ReplicatedMetadata,
            ),
            (
                "register with a request number falls to the default",
                routed(Operation::Register, 0, 1),
                true,
                RequestClass::ReplicatedMetadata,
            ),
            (
                "logout, bound",
                routed(Operation::Logout, 1, 1),
                true,
                RequestClass::Logout,
            ),
            (
                "logout beats the session gate",
                routed(Operation::Logout, 1, 1),
                false,
                RequestClass::Logout,
            ),
            (
                "unbound metadata op",
                routed(Operation::CreateStream, 1, 1),
                false,
                RequestClass::UnboundReplicated,
            ),
            (
                "the session gate beats the delete-segments probe",
                routed(Operation::DeleteSegments, 1, 1),
                false,
                RequestClass::UnboundReplicated,
            ),
            (
                "the session gate beats the partition probe",
                routed(Operation::SendMessages, 1, 1),
                false,
                RequestClass::UnboundReplicated,
            ),
            (
                "delete-segments is neither partition nor metadata",
                routed(Operation::DeleteSegments, 1, 1),
                true,
                RequestClass::DeleteSegments,
            ),
            (
                "send-messages is partition-plane",
                routed(Operation::SendMessages, 1, 1),
                true,
                RequestClass::Partition,
            ),
            (
                "store-consumer-offset is partition-plane",
                routed(Operation::StoreConsumerOffset, 1, 1),
                true,
                RequestClass::Partition,
            ),
            (
                "create-topic is replicated metadata",
                routed(Operation::CreateTopic, 1, 1),
                true,
                RequestClass::ReplicatedMetadata,
            ),
        ];
        for (label, header, bound, expected) in table {
            assert_eq!(classify(&header, bound), expected, "{label}");
        }
    }

    /// A legacy login code must get the typed `MalformedLogin` eviction
    /// BEFORE the session gate runs: an unbound sender must not fall into
    /// the generic Unauthenticated deny the pre-auth guard sends for other
    /// codes.
    #[compio::test]
    async fn legacy_login_codes_evicted_before_session_gate() {
        const TRANSPORT: u128 = 94;
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, 0, 1, FIRST_BOOT));
        let sessions = Rc::new(RefCell::new(SessionManager::new()));
        let system_config = Arc::new(ServerSystemConfig::default());

        for code in [LOGIN_USER_CODE, LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE] {
            handle_client_request(
                &shard,
                &sessions,
                &system_config,
                1,
                TRANSPORT,
                non_replicated_request(TRANSPORT, code),
            )
            .await;
            let replies = bus.client_replies.borrow();
            assert_eq!(replies.len(), 1, "code {code} must produce one frame");
            let (client, frame) = &replies[0];
            assert_eq!(*client, TRANSPORT);
            assert_eq!(
                frame_command(frame),
                Command::Eviction as u8,
                "legacy code {code} must be evicted, not denied"
            );
            assert_eq!(
                eviction_reason_byte(frame),
                EvictionReason::MalformedLogin as u8,
                "legacy code {code} must carry MalformedLogin"
            );
            drop(replies);
            bus.client_replies.borrow_mut().clear();
        }
    }

    /// A replicated op from an unbound transport must get the typed
    /// `Eviction(NoSession)`: the client believes it has a session and that
    /// session is gone, so a silent drop or an empty Reply would wedge or
    /// mislead it.
    #[compio::test]
    async fn unbound_replicated_request_gets_no_session_eviction() {
        const TRANSPORT: u128 = 95;
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, 0, 1, FIRST_BOOT));
        let sessions = Rc::new(RefCell::new(SessionManager::new()));
        let system_config = Arc::new(ServerSystemConfig::default());

        handle_client_request(
            &shard,
            &sessions,
            &system_config,
            1,
            TRANSPORT,
            wire_request(Operation::CreateStream, TRANSPORT, 1, 1, &[]).into_generic(),
        )
        .await;
        let replies = bus.client_replies.borrow();
        assert_eq!(replies.len(), 1, "unbound replicated op must be answered");
        let (client, frame) = &replies[0];
        assert_eq!(*client, TRANSPORT);
        assert_eq!(
            frame_command(frame),
            Command::Eviction as u8,
            "unbound replicated op must be evicted, not denied or dropped"
        );
        assert_eq!(
            eviction_reason_byte(frame),
            EvictionReason::NoSession as u8,
            "the eviction must carry NoSession"
        );
    }

    /// PING is the one pre-auth code: an unbound transport's ping must get a
    /// normal status-0 Reply, not an eviction and not a deny.
    #[compio::test]
    async fn pre_auth_ping_allowed() {
        const TRANSPORT: u128 = 96;
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, 0, 1, FIRST_BOOT));
        let sessions = Rc::new(RefCell::new(SessionManager::new()));
        let system_config = Arc::new(ServerSystemConfig::default());

        handle_client_request(
            &shard,
            &sessions,
            &system_config,
            1,
            TRANSPORT,
            non_replicated_request(TRANSPORT, PING_CODE),
        )
        .await;
        let replies = bus.client_replies.borrow();
        assert_eq!(replies.len(), 1, "pre-auth ping must be answered");
        let (client, frame) = &replies[0];
        assert_eq!(*client, TRANSPORT);
        assert_eq!(
            frame_command(frame),
            Command::Reply as u8,
            "pre-auth ping must get a Reply, not an eviction"
        );
        assert_eq!(reply_status(frame), 0, "pre-auth ping must succeed");
    }

    /// The request-checksum gate runs before every probe: a replicated op
    /// from an UNBOUND transport with a bad stamp must get the checksum deny
    /// Reply, not the `Eviction(NoSession)` the session gate would send.
    #[compio::test]
    async fn checksum_mismatch_denies_before_everything() {
        const TRANSPORT: u128 = 97;
        const BODY: &[u8] = b"stream-body";
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, 0, 1, FIRST_BOOT));
        let sessions = Rc::new(RefCell::new(SessionManager::new()));
        let system_config = Arc::new(ServerSystemConfig::default());

        let mut message = wire_request(Operation::CreateStream, TRANSPORT, 1, 1, BODY);
        {
            let header_size = size_of::<RequestHeader>();
            let header = bytemuck::checked::from_bytes_mut::<RequestHeader>(
                &mut message.as_mut_slice()[..header_size],
            );
            // Nonzero (zero means unstamped and skips the check) and never
            // the body's real checksum.
            header.request_checksum = u128::from(iggy_common::calculate_checksum(BODY)) + 1;
        }
        handle_client_request(
            &shard,
            &sessions,
            &system_config,
            1,
            TRANSPORT,
            message.into_generic(),
        )
        .await;
        let replies = bus.client_replies.borrow();
        assert_eq!(replies.len(), 1, "bad stamp must be answered");
        let (client, frame) = &replies[0];
        assert_eq!(*client, TRANSPORT);
        assert_eq!(
            frame_command(frame),
            Command::Reply as u8,
            "a bad stamp must get the checksum deny, not the NoSession eviction"
        );
        assert_eq!(
            reply_status(frame),
            IggyError::InvalidFormat.as_code(),
            "the deny status must carry the checksum error"
        );
    }

    /// The late-bound self-reference as the shard build creates it: unset
    /// until the shard exists.
    fn unset_shard_handle() -> ShellShardHandle<SpyBus, PrepareJournal, IggySnapshot> {
        Rc::new(RefCell::new(None))
    }

    /// Yield so the drain task the enqueue spawned on the bus can run.
    async fn run_spawned_tasks() {
        compio::time::sleep(std::time::Duration::from_millis(1)).await;
    }

    /// One handler per shard means one connection-lost hook on its bus: a
    /// second install would overwrite the first and orphan the sessions
    /// the first one bound.
    #[test]
    fn deferred_handler_installs_one_connection_lost_hook() {
        let bus = SpyBus::default();
        let _handler = make_deferred_client_request_handler(
            &bus,
            &unset_shard_handle(),
            &Rc::new(RefCell::new(SessionManager::new())),
            Arc::new(ServerSystemConfig::default()),
            1,
        );
        assert_eq!(
            bus.connection_lost_hooks.get(),
            1,
            "one factory call must install exactly one connection-lost hook"
        );
    }

    /// The handler is built before the shard it serves. A frame that
    /// arrives while the self-reference is still unset stays queued, and
    /// the enqueue must release the client's active slot: otherwise every
    /// later frame for that client finds the slot taken and nothing is
    /// ever drained, the stranded frame included.
    #[compio::test]
    async fn deferred_handler_drains_after_the_shard_handle_is_set() {
        const TRANSPORT: u128 = 98;
        let bus = SpyBus::default();
        let shard_handle = unset_shard_handle();
        let handler = make_deferred_client_request_handler(
            &bus,
            &shard_handle,
            &Rc::new(RefCell::new(SessionManager::new())),
            Arc::new(ServerSystemConfig::default()),
            1,
        );

        handler(TRANSPORT, non_replicated_request(TRANSPORT, PING_CODE));
        run_spawned_tasks().await;
        assert!(
            bus.client_replies.borrow().is_empty(),
            "nothing can be served before the shard exists"
        );

        let shard = Rc::new(test_shard(&bus, 0, 1, FIRST_BOOT));
        *shard_handle.borrow_mut() = Some(Rc::downgrade(&shard));
        handler(TRANSPORT, non_replicated_request(TRANSPORT, PING_CODE));
        for _ in 0..500 {
            if bus.client_replies.borrow().len() == 2 {
                break;
            }
            run_spawned_tasks().await;
        }

        let replies = bus.client_replies.borrow();
        assert_eq!(
            replies.len(),
            2,
            "the stranded ping and the new one must both be served once the shard exists"
        );
        for (client, frame) in replies.iter() {
            assert_eq!(*client, TRANSPORT);
            assert_eq!(frame_command(frame), Command::Reply as u8);
            assert_eq!(reply_status(frame), 0, "a served ping succeeds");
        }
    }

    /// The per-client queue entry goes with its last frame. The
    /// connection-lost hook cannot be its only owner: the dispatch task still
    /// delivers frames it had buffered when the socket closed, which re-create
    /// the entry after the hook ran, and `client_id` is monotonic - so an
    /// entry nothing frees again lives until process exit.
    #[test]
    fn draining_a_client_queue_frees_its_entry() {
        const CLIENT: u128 = 7;
        let queues: ClientRequestQueues = Rc::new(RefCell::new(AHashMap::new()));
        queues
            .borrow_mut()
            .entry(CLIENT)
            .or_default()
            .push_back(non_replicated_request(CLIENT, PING_CODE));

        assert!(pop_next_client_request(&queues, CLIENT).is_some());
        assert!(
            queues.borrow().is_empty(),
            "the entry must be freed as soon as it drains empty"
        );
        assert!(
            pop_next_client_request(&queues, CLIENT).is_none(),
            "a drained client has nothing left to pop"
        );
    }
}
