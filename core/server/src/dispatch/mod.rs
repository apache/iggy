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
//! The tree: [`host`] (the shard host: per-client queues, session manager,
//! connection-lost hook), [`session_ops`] (login/register/logout and their
//! replica forwards), [`partition`] (the partition data plane, both mesh ends),
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
pub mod host;
pub mod login_error;
pub mod partition;
pub mod reads;
pub mod session_ops;
pub mod submit;
#[cfg(test)]
pub mod test_support;

use crate::dispatch::failure::{
    FrameChannel, send_deny_reply, send_eviction, send_host_frame, send_pre_consensus_deny,
    send_unbound_deny_reply, send_undeclared_operation_deny,
};
use crate::dispatch::partition::{
    dispatch_partition_request, handle_delete_segments_request, progress_key,
};
use crate::dispatch::reads::handle_non_replicated_request;
use crate::dispatch::session_ops::{handle_login_register_request, handle_logout_request};
use crate::dispatch::submit::{committed_reply_commit, submit_client_request_on_owner};
use crate::reply_frame::build_raw_pat_reply;
use crate::rewrite::{RewriteDeny, tcp_chain};
use crate::session_manager::{ConnectionContext, SessionManager};
use crate::shell::{ShellBus, ShellShard, ShellShardHandle};
use crate::wire::verify_request_checksum;
use ahash::{AHashMap, AHashSet};
use configs::server::ServerConfig;
use consensus::MetadataHandle;
use futures::future::{Either, select};
use iggy_binary_protocol::PrepareHeader;
use iggy_binary_protocol::codes::{
    BIND_SESSION_CODE, LOGIN_USER_CODE, LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE, PING_CODE,
};
use iggy_binary_protocol::requests::system::SessionIdentity;
use iggy_binary_protocol::requests::users::login_register::BindSecret;
use iggy_binary_protocol::{
    EvictionReason, GenericHeader, HEADER_SIZE, Operation, RequestHeader, RoutedRequestHeader,
};
use iggy_common::{IggyError, UserStatus};
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use metadata::impls::metadata::StreamsFrontend;
use secrecy::ExposeSecret;
use server_common::Message;
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::poll_fn;
use std::mem::offset_of;
use std::pin::pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Poll, Waker};
use tracing::{debug, warn};

type ClientRequestQueues = Rc<RefCell<AHashMap<u128, VecDeque<QueuedClientRequest>>>>;

struct QueuedClientRequest {
    message: Message<GenericHeader>,
    // A queued metadata barrier can change the namespace resolved by this frame.
    progress: Option<(u64, Option<ConsumerProgress>)>,
}

impl From<Message<GenericHeader>> for QueuedClientRequest {
    fn from(message: Message<GenericHeader>) -> Self {
        Self {
            message,
            progress: None,
        }
    }
}

impl QueuedClientRequest {
    fn progress_key(
        &mut self,
        metadata_frontier: u64,
        resolve: impl FnOnce(&Message<GenericHeader>) -> Option<ConsumerProgress>,
    ) -> Option<ConsumerProgress> {
        if let Some((frontier, key)) = self.progress
            && frontier == metadata_frontier
        {
            return key;
        }
        let key = resolve(&self.message);
        self.progress = Some((metadata_frontier, key));
        key
    }
}

/// Requests one client may have queued behind a request this shard has not
/// answered yet.
///
/// The drain loop below serves the frames of one client in arrival order, and
/// a frame it must finish before the next one can hold it for a while: a login
/// or a logout awaits consensus, a `BIND_SESSION` can be HELD for the
/// read-your-writes budget (see `crate::dispatch::reads`), and any frame can
/// wait for room under the in-flight cap or for the metadata lane of
/// [`ClientDrain`]. Without a cap, a client that keeps pipelining through such
/// a stall grows its queue - and this node's memory - unbounded.
///
/// Overflow is ANSWERED, not dropped: `TransientNotAccepted` is the honest
/// code, since the frame provably never entered any pipeline, so the SDK may
/// re-issue it anywhere, including here once the queue drains. Sized far above
/// any SDK's in-flight window, so it only ever fires under a genuine stall.
const MAX_QUEUED_CLIENT_REQUESTS: usize = 1024;
type ActiveClientRequests = Rc<RefCell<AHashMap<u128, Rc<ClientDrain>>>>;
type ConsumerProgress = (u64, u8, usize);

// BindSession authenticates with the registered secret, never with the
// public client id and epoch alone. It retains the original retry identity;
// unbound replicated frames still fail with Eviction(NoSession).

#[allow(clippy::too_many_arguments)]
fn enqueue_client_request<B, MJ, S, SB>(
    bus: &B,
    shard_handle: &ShellShardHandle<B, MJ, S, SB>,
    sessions: &Rc<RefCell<SessionManager>>,
    server_config: &Arc<ServerConfig>,
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
        queue.push_back(message.into());
    }
    let running = active.borrow().get(&client_id).cloned();
    if let Some(drain) = running {
        // A drain with requests in flight parks on an empty queue.
        drain.wake();
        return;
    }
    let drain = Rc::new(ClientDrain::new(server_config));
    active.borrow_mut().insert(client_id, Rc::clone(&drain));

    let shard_handle = Rc::clone(shard_handle);
    let sessions = Rc::clone(sessions);
    let server_config = Arc::clone(server_config);
    let queues = Rc::clone(queues);
    let active = Rc::clone(active);
    bus.spawn(async move {
        let _slot = ActiveDrainSlot { active, client_id };
        // The handle is set once the shard is built. A frame that beats it
        // stays queued and the client's next frame drains both, which needs
        // the entry removed on this path too - the guard above does it.
        let Some(shard) = upgrade_shard_handle(&shard_handle) else {
            return;
        };
        drain_client_requests(
            shard,
            sessions,
            server_config,
            max_tokens_per_user,
            queues,
            &drain,
            client_id,
        )
        .await;
    });
}

/// Answer a request that arrived with this client's queue already at
/// [`MAX_QUEUED_CLIENT_REQUESTS`] with the retryable transient denial.
///
/// Spawned rather than awaited: the enqueue path is sync (it runs straight off
/// frame arrival) and the reply goes out on the bus. The header decodes as in
/// the drain loop: an undeclared operation byte gets the `InvalidCommand` deny
/// instead of the transient one, because no retry of that frame can succeed,
/// and any other header that will not cast is dropped. A frame that arrives
/// before the shard is built is dropped too: nothing exists yet to render a
/// reply from.
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
    let bus = shard.bus.clone();
    bus.spawn(async move {
        let Some(request) =
            decode_or_deny_client_request(&shard, transport_client_id, message).await
        else {
            return;
        };
        let request = request.into_routed();
        debug!(
            transport_client_id,
            operation = ?request.header().operation,
            queued = MAX_QUEUED_CLIENT_REQUESTS,
            "denying client request retryable: this connection's request queue is full"
        );
        send_deny_reply(
            &shard,
            transport_client_id,
            request.header(),
            IggyError::TransientNotAccepted.as_code(),
        )
        .await;
    });
}

/// Holds a client's one drain entry for as long as its drain task lives.
///
/// Removing it from the task's own `Drop` covers every exit, the panic compio
/// catches included: an entry left with no live drain queues every later
/// frame for that client forever. It also keeps the removal out of the
/// connection-lost hook, which fires from the transport task and could
/// otherwise remove the entry under a drain suspended at an `.await`.
struct ActiveDrainSlot {
    active: ActiveClientRequests,
    client_id: u128,
}

impl Drop for ActiveDrainSlot {
    fn drop(&mut self) {
        self.active.borrow_mut().remove(&self.client_id);
    }
}

/// The state that the drain task of one connection shares with the requests
/// it spawned.
///
/// The drain takes the frames of the connection in arrival order and runs the
/// prologue of each. Login, logout, `BIND_SESSION` and partition admission
/// finish on the drain. Metadata commits run in tasks, but later non-PING
/// frames wait in the queue until the metadata lane releases, before they
/// capture session state or resolve a namespace. This orders dispatch, not local
/// metadata catch-up: partition writes still resolve from this shard's STM
/// without waiting for the connection watermark. PING can pass that queue.
/// Stored-offset reads, Next/auto-commit polls and explicit offset writes share
/// a lane per resolved namespace and consumer until their reply or wait expiry.
/// Expiry releases only this connection's guard, not an admitted mutation, which
/// may still commit after later progress. Independent requests can pass a queued
/// conflict, but cannot pass a queued session or metadata barrier. A PING still waits
/// behind a frame already suspended in admission or binding.
///
/// `capacity` caps running tasks at `message_bus.client_queue_capacity`.
/// Permits end when replies are enqueued, so they do not reserve outbound
/// queue slots. A metadata write, a segment delete, a login and a logout also take
/// the metadata lane, which holds one of them at a time. The metadata client
/// table keeps one pending write per client and refuses a lower request id
/// after a higher one committed, so a second write in flight would fail with
/// `TransientNotAccepted` or `RequestTooOld`. The lane also runs the rewrite of
/// a write after the previous write replied, because the rewrite reads state
/// that the previous write can change.
///
/// At the cap or the lane the drain waits, and the frames behind it queue up
/// to [`MAX_QUEUED_CLIENT_REQUESTS`]. While tasks run, the drain outlives its
/// queue, so the cap and the lane still count them when the next frame
/// arrives.
pub(in crate::dispatch) struct ClientDrain {
    capacity: usize,
    in_flight: Cell<usize>,
    /// Whether a frame holds the metadata lane.
    metadata_write: Cell<bool>,
    progress: RefCell<AHashSet<ConsumerProgress>>,
    /// The drain task, while it waits for a frame or for room to admit one.
    parked: Cell<Option<Waker>>,
}

/// What a frame waits for before [`ClientDrain::admit`] counts it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Admission {
    /// Room under the in-flight cap.
    Cap,
    /// Room under the cap and a free metadata lane.
    MetadataLane,
}

impl ClientDrain {
    pub(in crate::dispatch) fn new(server_config: &ServerConfig) -> Self {
        Self {
            capacity: server_config.message_bus.client_queue_capacity,
            in_flight: Cell::new(0),
            metadata_write: Cell::new(false),
            progress: RefCell::default(),
            parked: Cell::new(None),
        }
    }

    /// Count one more request task, after waiting for what `admission` needs.
    #[allow(clippy::future_not_send)]
    async fn admit(self: &Rc<Self>, admission: Admission) -> InFlight {
        let metadata_write = admission == Admission::MetadataLane;
        poll_fn(|cx| {
            if self.in_flight.get() < self.capacity
                && !(metadata_write && self.metadata_write.get())
            {
                return Poll::Ready(());
            }
            self.parked.set(Some(cx.waker().clone()));
            Poll::Pending
        })
        .await;
        self.in_flight.set(self.in_flight.get() + 1);
        if metadata_write {
            self.metadata_write.set(true);
        }
        InFlight {
            drain: Rc::clone(self),
            metadata_write,
            progress: None,
        }
    }

    fn admit_progress(self: &Rc<Self>, progress: ConsumerProgress) -> InFlight {
        self.progress.borrow_mut().insert(progress);
        self.in_flight.set(self.in_flight.get() + 1);
        InFlight {
            drain: Rc::clone(self),
            metadata_write: false,
            progress: Some(progress),
        }
    }

    /// Wait until a frame arrives or a request task ends.
    #[allow(clippy::future_not_send)]
    async fn park(&self) {
        let mut parked = false;
        poll_fn(|cx| {
            if parked {
                return Poll::Ready(());
            }
            parked = true;
            self.parked.set(Some(cx.waker().clone()));
            Poll::Pending
        })
        .await;
    }

    fn wake(&self) {
        if let Some(waker) = self.parked.take() {
            waker.wake();
        }
    }
}

/// One request task counted by its [`ClientDrain`]. The task owns it, so the
/// count drops when the task ends or the runtime drops it.
pub struct InFlight {
    drain: Rc<ClientDrain>,
    metadata_write: bool,
    progress: Option<ConsumerProgress>,
}

impl Drop for InFlight {
    fn drop(&mut self) {
        self.drain.in_flight.set(self.drain.in_flight.get() - 1);
        if self.metadata_write {
            self.drain.metadata_write.set(false);
        }
        if let Some(progress) = self.progress {
            self.drain.progress.borrow_mut().remove(&progress);
        }
        self.drain.wake();
    }
}

#[allow(clippy::future_not_send)]
async fn drain_client_requests<B, MJ, S, SB>(
    shard: Rc<ShellShard<B, MJ, S, SB>>,
    sessions: Rc<RefCell<SessionManager>>,
    server_config: Arc<ServerConfig>,
    max_tokens_per_user: u32,
    queues: ClientRequestQueues,
    drain: &Rc<ClientDrain>,
    client_id: u128,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    loop {
        let metadata_frontier = shard.plane.metadata().applied_frontier().get();
        let next = pop_next_client_request(&queues, client_id, |queued| {
            if is_ping(&queued.message) {
                return true;
            }
            if drain.metadata_write.get() {
                return false;
            }
            queued
                .progress_key(metadata_frontier, |message| progress_key(&shard, message))
                .is_none_or(|key| {
                    drain.in_flight.get() < drain.capacity
                        && !drain.progress.borrow().contains(&key)
                })
        });
        if let Some(queued) = next {
            let progress = queued
                .progress
                .and_then(|(_, key)| key)
                .map(|key| drain.admit_progress(key));
            handle_client_request(
                &shard,
                &sessions,
                &server_config,
                max_tokens_per_user,
                drain,
                client_id,
                queued.message,
                progress,
            )
            .await;
        } else if drain.in_flight.get() == 0 {
            return;
        } else {
            drain.park().await;
        }
    }
}

fn pop_next_client_request(
    queues: &ClientRequestQueues,
    client_id: u128,
    mut ready: impl FnMut(&mut QueuedClientRequest) -> bool,
) -> Option<QueuedClientRequest> {
    // Freed as soon as it drains empty. Keeping it would cost the connection
    // one `VecDeque` (at its peak depth, since `pop_front` never gives
    // capacity back), and the connection-lost hook cannot be the sole owner:
    // the dispatch task can still deliver a buffered frame after the hook
    // ran, and `client_id` is never reused, so that entry would live until
    // process exit.
    let mut queues = queues.borrow_mut();
    let queue = queues.get_mut(&client_id)?;
    let mut waiting = false;
    let mut barrier = false;
    let index = queue.iter_mut().position(|queued| {
        if is_ping(&queued.message) {
            return true;
        }
        if barrier {
            return false;
        }
        let orders_followers =
            queued
                .message
                .try_as_typed::<RequestHeader>()
                .map_or(true, |request| {
                    let header = request.header();
                    if header.operation == Operation::NonReplicated {
                        header.reserved[..4] == BIND_SESSION_CODE.to_le_bytes()
                    } else {
                        !header.operation.is_partition()
                    }
                });
        if waiting && orders_followers {
            barrier = true;
            return false;
        }
        if ready(queued) {
            return true;
        }
        waiting = true;
        barrier = orders_followers;
        false
    })?;
    let message = queue.remove(index);
    if queue.is_empty() {
        queues.remove(&client_id);
    }
    message
}

fn is_ping(message: &Message<GenericHeader>) -> bool {
    message
        .try_as_typed::<RequestHeader>()
        .is_ok_and(|request| {
            let header = request.header();
            header.operation == Operation::NonReplicated
                && header.reserved[..4] == PING_CODE.to_le_bytes()
        })
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
        if !matches!(
            nr_code,
            PING_CODE | iggy_binary_protocol::codes::BIND_SESSION_CODE
        ) && !bound
        {
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

/// A copy of the header of a client frame whose operation byte this build
/// does not declare, for the deny to patch and echo. A declared byte costs
/// one read and no copy.
fn undeclared_operation_header(message: &Message<GenericHeader>) -> Option<[u8; HEADER_SIZE]> {
    let header = message.as_slice().get(..HEADER_SIZE)?;
    if Operation::is_known_code(header[offset_of!(RequestHeader, operation)]) {
        return None;
    }
    header.try_into().ok()
}

/// Type a client frame as a request. A frame whose operation byte this build
/// does not declare gets the `InvalidCommand` deny, and any other header that
/// will not cast is dropped with a warning: both return `None`. Probing the
/// byte first skips the cast's command check, which is safe because the
/// transport admits only `Request` frames.
#[allow(clippy::future_not_send)]
async fn decode_or_deny_client_request<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    transport_client_id: u128,
    message: Message<GenericHeader>,
) -> Option<Message<RequestHeader>>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    if let Some(request_header) = undeclared_operation_header(&message) {
        send_undeclared_operation_deny(shard, transport_client_id, request_header).await;
        return None;
    }
    match message.try_into_typed::<RequestHeader>() {
        Ok(request) => Some(request),
        Err(error) => {
            warn!(
                transport_client_id,
                error = %error,
                "dropping client request with invalid header"
            );
            None
        }
    }
}

#[allow(
    clippy::future_not_send,
    clippy::too_many_arguments,
    clippy::too_many_lines
)]
async fn handle_client_request<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    sessions: &Rc<RefCell<SessionManager>>,
    server_config: &Arc<ServerConfig>,
    max_tokens_per_user: u32,
    drain: &Rc<ClientDrain>,
    transport_client_id: u128,
    message: Message<iggy_binary_protocol::GenericHeader>,
    progress: Option<InFlight>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let Some(request) = decode_or_deny_client_request(shard, transport_client_id, message).await
    else {
        return;
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
    let include_attachment = request.header().operation == Operation::NonReplicated;
    let mut touched = sessions
        .borrow_mut()
        .touch_connection(transport_client_id, include_attachment);
    if touched.is_none() {
        // A transport's first frame: the peer address and transport kind live
        // on the bus, so only this path pays that lookup.
        ensure_transport_connection(shard, sessions, transport_client_id);
        touched = sessions
            .borrow_mut()
            .touch_connection(transport_client_id, include_attachment);
    }
    let ConnectionContext {
        bound,
        attachment,
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
            let context = ConnectionContext {
                bound,
                attachment,
                user_id,
                address: client_address,
                metadata_watermark,
            };
            // `ClientDrain` says which frames stay on the drain.
            if matches!(non_replicated_code(header), PING_CODE | BIND_SESSION_CODE) {
                handle_non_replicated_request(
                    shard,
                    sessions,
                    server_config,
                    transport_client_id,
                    request,
                    context,
                )
                .await;
                return;
            }
            let in_flight = match progress {
                Some(progress) => progress,
                None => drain.admit(Admission::Cap).await,
            };
            let task_shard = Rc::clone(shard);
            let task_sessions = Rc::clone(sessions);
            let task_config = Arc::clone(server_config);
            shard.bus.spawn(async move {
                let _in_flight = in_flight;
                handle_non_replicated_request(
                    &task_shard,
                    &task_sessions,
                    &task_config,
                    transport_client_id,
                    request,
                    context,
                )
                .await;
            });
        }
        RequestClass::LoginRegister => {
            let _in_flight = drain.admit(Admission::MetadataLane).await;
            handle_login_register_request(shard, sessions, transport_client_id, request).await;
        }
        RequestClass::Logout => {
            let _in_flight = drain.admit(Admission::MetadataLane).await;
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
            let in_flight = drain.admit(Admission::MetadataLane).await;
            let task_shard = Rc::clone(shard);
            shard.bus.spawn(async move {
                let _in_flight = in_flight;
                handle_delete_segments_request(&task_shard, transport_client_id, bound, &request)
                    .await;
            });
        }
        RequestClass::Partition => {
            // `bound` is Some here: `classify` sends unbound transports to
            // `UnboundReplicated`.
            let (vsr_client_id, bound_session) = bound.unwrap_or((0, 0));
            let consumer_session = {
                let attachment = sessions
                    .borrow()
                    .attached_consumer_session(transport_client_id);
                match attachment {
                    Ok(attachment) => attachment,
                    Err(error) => {
                        send_deny_reply(
                            shard,
                            transport_client_id,
                            request.header(),
                            error.as_code(),
                        )
                        .await;
                        return;
                    }
                }
            };
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
                consumer_session,
                None,
                progress,
            )
            .await;
        }
        RequestClass::ReplicatedMetadata => {
            // The one arm that needs the by-value copy: the rewrite below
            // stamps the consensus client / session / group over the header,
            // and a pre-consensus deny still has to echo what the client sent.
            let header = *header;
            let in_flight = drain.admit(Admission::MetadataLane).await;
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
            let request_header = *request.header();
            // Replicated request: run consensus on the metadata owner (shard 0) and
            // bring the committed reply back here. This shard owns the connection,
            // so it writes the reply to the socket via the transport client id --
            // shard 0 can't route by the consensus client id (no home-shard bits).
            // The wait for the commit runs off the drain, and its task holds the
            // metadata lane until the reply is out.
            let task_shard = Rc::clone(shard);
            let task_sessions = Rc::clone(sessions);
            shard.bus.spawn(async move {
                let _in_flight = in_flight;
                let (shard, sessions) = (&task_shard, &task_sessions);
                let Some(reply) = submit_client_request_on_owner(shard, request).await else {
                    // No verdict came back, for example because the inbox of
                    // shard 0 refused the forward. The outcome is unknown, so
                    // only a replay of this request id is safe. A silent drop
                    // leaves the client waiting for its whole read timeout.
                    warn!(
                        transport_client_id,
                        operation = ?header.operation,
                        "replicated request has no metadata verdict; client replays"
                    );
                    send_deny_reply(
                        shard,
                        transport_client_id,
                        &header,
                        IggyError::TransientNotCommitted.as_code(),
                    )
                    .await;
                    return;
                };
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
                        send_deny_reply(shard, transport_client_id, &header, error.as_code()).await;
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
            });
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

#[allow(clippy::future_not_send)]
async fn complete_session_binding<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
    identity: SessionIdentity,
    secret: BindSecret,
) -> Result<(u32, u64), IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    if message_bus::is_server_internal_client(identity.client_id) {
        return Err(IggyError::Unauthenticated);
    }
    let watermark = identity.metadata_watermark.max(identity.session);
    reads::await_metadata_read_frontier(shard, watermark).await?;
    let (user_id, session, attachment) = if shard.id == 0 {
        shard
            .plane
            .metadata()
            .client_table
            .borrow_mut()
            .bind_session(identity.client_id, identity.session, secret.expose_secret())?
    } else {
        let (reply, receiver) = shard::channel(1);
        shard.forward_metadata_submit(shard::MetadataSubmit::BindSession {
            vsr_client_id: identity.client_id,
            session: identity.session,
            secret,
            reply,
        });
        let outcome = pin!(receiver.recv());
        let deadline = pin!(
            shard
                .bus
                .sleep(shard.plane.metadata().applied_frontier().read_budget())
        );
        match select(outcome, deadline).await {
            Either::Left((result, _)) => result.map_err(|_| IggyError::TransientNotAccepted)??,
            Either::Right(_) => return Err(IggyError::TransientNotAccepted),
        }
    };
    let active_user = shard.plane.metadata().mux_stm.users().read(|users| {
        users
            .items
            .get(user_id as usize)
            .is_some_and(|user| user.status == UserStatus::Active)
    });
    if !active_user || !attachment.is_valid() {
        return Err(IggyError::Unauthenticated);
    }
    sessions.borrow_mut().bind_authenticated_connection(
        transport_client_id,
        identity.client_id,
        session,
        user_id,
        attachment,
        watermark,
    )?;
    Ok((user_id, session))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_meta::ClusterRoster;
    use crate::dispatch::host::ServerHost;
    use crate::dispatch::test_support::{
        FIRST_BOOT, SpyBus, TestMux, TestShard, UNDECLARED_OPERATION, create_one_partition,
        create_reader_request, prepare_message, register_reply, request_message, test_shard,
    };
    use bytes::Bytes;
    use consensus::Sequencer;
    use consensus::client_table::bind_verifier;
    use iggy_binary_protocol::Command;
    use iggy_binary_protocol::codes::{
        GET_CLIENT_CODE, GET_CLIENTS_CODE, GET_CLUSTER_METADATA_CODE, GET_CONSUMER_OFFSET_CODE,
        GET_CONSUMER_OFFSET_ROUTING_CODE, GET_ME_CODE, GET_PARTITION_ROUTE_CODE,
        GET_POLL_ROUTING_CODE, GET_STATS_CODE, POLL_MESSAGES_CODE, POLL_MESSAGES_ON_PRIMARY_CODE,
    };
    use iggy_binary_protocol::primitives::partition_assignment::CreatedPartitionAssignment;
    use iggy_binary_protocol::primitives::polling_strategy::WirePollingStrategy;
    use iggy_binary_protocol::requests::consumer_offsets::{
        GetConsumerOffsetRequest, StoreConsumerOffsetRequest,
    };
    use iggy_binary_protocol::requests::messages::{GetPartitionRouteRequest, PollMessagesRequest};
    use iggy_binary_protocol::requests::streams::CreateStreamRequest;
    use iggy_binary_protocol::requests::system::{
        BindSessionRequest, GetClientRequest, GetClientsRequest, GetStatsRequest,
    };
    use iggy_binary_protocol::requests::topics::{
        CreateTopicRequest, CreateTopicWithAssignmentsRequest, DeleteTopicRequest,
    };
    use iggy_binary_protocol::requests::users::login_register::{
        LoginRegisterRequest, RegisterRequest,
    };
    use iggy_binary_protocol::requests::users::login_register_with_pat::LoginRegisterWithPatRequest;
    use iggy_binary_protocol::requests::users::{
        CreateUserRequest, DeleteUserRequest, UpdatePermissionsRequest, UpdateUserRequest,
    };
    use iggy_binary_protocol::{
        AckLevel, ClientVersionInfo, EvictionHeader, IGGY_PROTOCOL_VERSION, ReplyHeader,
        WireConsumer,
    };
    use iggy_binary_protocol::{WireEncode, WireIdentifier, WireName, WireOptions};
    use iggy_common::Identifier;
    use iggy_common::defaults::DEFAULT_ROOT_USER_ID;
    use journal::prepare_journal::PrepareJournal;
    use message_bus::installer::conn_info::ClientTransportKind;
    use metadata::IggyMetadata;
    use metadata::impls::metadata::IggySnapshot;
    use metadata::stm::StateMachine;
    use metadata::stm::lifecycle::{CompleteLifecycleRequest, LifecycleCompletion};
    use metadata::stm::user::CreatePersonalAccessTokenRequest;
    use partitions::{IggyPartitions, PartitionPathLayout, PartitionsConfig};
    use server_common::MESSAGE_ALIGN;
    use server_common::sharding::{PartitionLocation, ShardId};
    use shard::metrics::ShardMetrics;
    use shard::shards_table::{PapayaShardsTable, ShardsTable};
    use shard::{
        ConnectedClientInfo, LifecycleFrame, ListClientsReply, MetadataSubmit, NoopHost,
        PartitionConsensusConfig, PartitionRead, PartitionReadReply, ReplicaTopology, ShardFrame,
        ShardHost, ShardIdentity, shard_channel,
    };
    use std::mem::size_of;
    use std::sync::atomic::AtomicBool;

    #[compio::test]
    #[allow(clippy::too_many_lines)]
    async fn given_a_verified_login_when_register_commits_after_user_deletion_should_refuse_id_reuse()
     {
        const CLIENT: u128 = 10;
        const TRANSPORT: u128 = 20;
        const USER: u32 = 1;
        const CREATED_AT: u64 = 10;
        const REPLACED_AT: u64 = 30;
        const REGISTER_OP: u64 = 40;
        const SECRET: [u8; 32] = [0x5a; 32];
        const PASSWORD: &str = "original-password";
        const TOKEN: &str = "original-personal-access-token";
        let password_hash = server_common::crypto::hash_password(PASSWORD);
        for replace_before_register in [false, true] {
            let bus = SpyBus::default();
            let shard = Rc::new(test_shard(&bus, 0, 1, FIRST_BOOT));
            let metadata = shard.plane.metadata();
            metadata.mux_stm.users().ensure_root_user("root", "hash");
            let apply = |op, operation, client, user_id, body: &[u8]| {
                let prepare = prepare_message(operation, client, 0, body).transmute_header(
                    |old, header: &mut iggy_binary_protocol::PrepareHeader| {
                        *header = old;
                        header.op = op;
                        header.timestamp = op;
                        header.user_id = user_id;
                        header.session = 0;
                    },
                );
                metadata::apply_committed_prepare(
                    &*metadata.mux_stm,
                    &metadata.client_table,
                    true,
                    |_| {},
                    prepare,
                )
            };
            let create = CreateUserRequest {
                username: WireName::new("original").unwrap(),
                password: password_hash.clone(),
                status: UserStatus::Active.as_code(),
                permissions: None,
                options: WireOptions::empty(),
            };
            let created = apply(CREATED_AT, Operation::CreateUser, 0, 0, &create.to_bytes());
            assert_eq!(iggy_binary_protocol::result_code(created.body()), Some(0));
            let token_hash = iggy_common::PersonalAccessToken::hash_token(TOKEN);
            let token = CreatePersonalAccessTokenRequest {
                user_id: USER,
                name: WireName::new("original-token").unwrap(),
                expiry: 0,
                token_hash: token_hash.as_bytes().try_into().unwrap(),
            };
            let created = apply(
                CREATED_AT + 1,
                Operation::CreatePersonalAccessToken,
                0,
                0,
                &token.to_bytes(),
            );
            assert_eq!(iggy_binary_protocol::result_code(created.body()), Some(0));
            let verified =
                session_ops::verify_login_credentials(&shard, "original", PASSWORD).unwrap();
            assert_eq!(
                verified,
                session_ops::verify_pat_credentials(&shard, TOKEN).unwrap()
            );
            assert_eq!(
                verified,
                (USER, iggy_common::IggyTimestamp::from(CREATED_AT))
            );
            let registration = RegisterRequest {
                user_created_at: verified.1.as_micros(),
                bind_verifier: bind_verifier(CLIENT, verified.0, &SECRET),
            };
            let deleted = apply(
                CREATED_AT + 2,
                Operation::DeleteUser,
                0,
                0,
                &DeleteUserRequest {
                    user_id: WireIdentifier::numeric(USER),
                }
                .to_bytes(),
            );
            assert_eq!(iggy_binary_protocol::result_code(deleted.body()), Some(0));
            let replacement = CreateUserRequest {
                username: WireName::new("replacement").unwrap(),
                ..create
            }
            .to_bytes();
            if replace_before_register {
                apply(REPLACED_AT, Operation::CreateUser, 0, 0, &replacement);
            }
            let denied = apply(
                REGISTER_OP,
                Operation::Register,
                CLIENT,
                USER,
                &registration.to_bytes(),
            );
            assert_eq!(
                iggy_binary_protocol::result_code(denied.body()),
                Some(IggyError::Unauthenticated.as_code())
            );
            assert_eq!(metadata.client_table.borrow().get_user_id(CLIENT), None);
            if !replace_before_register {
                apply(REGISTER_OP + 1, Operation::CreateUser, 0, 0, &replacement);
            }
            metadata.applied_frontier().advance(REGISTER_OP + 1);
            let sessions = Rc::new(RefCell::new(SessionManager::new()));
            sessions.borrow_mut().ensure_connection(
                TRANSPORT,
                "127.0.0.1:34567".parse().unwrap(),
                ClientTransportKind::Tcp,
            );
            for session in [0, REGISTER_OP] {
                let result = complete_session_binding(
                    &shard,
                    &sessions,
                    TRANSPORT,
                    SessionIdentity {
                        client_id: CLIENT,
                        session,
                        metadata_watermark: REGISTER_OP,
                    },
                    BindSecret::new(Box::new(SECRET)),
                )
                .await;
                assert!(
                    result.is_err(),
                    "replaced before Register: {replace_before_register}, session: {session}, result: {result:?}"
                );
            }
            assert_eq!(sessions.borrow().get_session(TRANSPORT), None);
        }
    }

    #[compio::test]
    async fn given_reserved_client_id_when_logging_in_should_evict_without_registering() {
        const TRANSPORT: u128 = 94;
        let version_info = ClientVersionInfo {
            protocol_version: IGGY_PROTOCOL_VERSION,
            sdk_name: WireName::new("test-sdk").unwrap(),
            sdk_version: WireName::new("1.0.0").unwrap(),
        };
        let bodies = [
            LoginRegisterRequest {
                version_info: version_info.clone(),
                bind_secret: BindSecret::new(Box::new([0x5a; 32])),
                username: WireName::new("root").unwrap(),
                password: "password".into(),
                client_context: None,
            }
            .to_bytes(),
            LoginRegisterWithPatRequest {
                version_info,
                bind_secret: BindSecret::new(Box::new([0x5a; 32])),
                token: "personal-access-token".into(),
                client_context: None,
            }
            .to_bytes(),
        ];
        for replica in [0, 1] {
            let bus = SpyBus::default();
            let shard = Rc::new(test_shard(&bus, replica, 3, FIRST_BOOT));
            let sessions = Rc::new(RefCell::new(SessionManager::new()));
            for client in [
                message_bus::server_internal_client_id(0),
                message_bus::AUTO_COMMIT_CLIENT_ID,
            ] {
                for body in &bodies {
                    handle_login_register_request(
                        &shard,
                        &sessions,
                        TRANSPORT,
                        request_message(Operation::Register, client, 0, 0, body),
                    )
                    .await;
                    let replies = bus.client_replies.take();
                    assert_eq!(replies.len(), 1);
                    let (transport, frame) = &replies[0];
                    assert_eq!(*transport, TRANSPORT);
                    assert_eq!(frame_command(frame), Command::Eviction as u8);
                    assert_eq!(
                        eviction_reason_byte(frame),
                        EvictionReason::MalformedLogin as u8
                    );
                    assert!(bus.replica_sends.borrow().is_empty());
                    assert_eq!(
                        shard
                            .plane
                            .metadata()
                            .client_table
                            .borrow()
                            .get_epoch(client),
                        None
                    );
                    assert_eq!(sessions.borrow().get_session(TRANSPORT), None);
                }
            }
        }
    }

    #[compio::test]
    async fn bind_session_rechecks_the_client_id_and_active_account() {
        const CLIENT: u128 = 1;
        const TRANSPORT: u128 = 2;
        const SESSION: u64 = 1;
        const SECRET: [u8; 32] = [0x5a; 32];
        for (client_id, status) in [
            (CLIENT, None),
            (CLIENT, Some(UserStatus::Inactive)),
            (CLIENT, Some(UserStatus::Active)),
            (
                message_bus::server_internal_client_id(0),
                Some(UserStatus::Active),
            ),
            (message_bus::AUTO_COMMIT_CLIENT_ID, Some(UserStatus::Active)),
        ] {
            let bus = SpyBus::default();
            let shard = Rc::new(test_shard(&bus, 0, 1, FIRST_BOOT));
            let metadata = shard.plane.metadata();
            if let Some(status) = status {
                metadata.mux_stm.users().ensure_root_user("iggy", "hash");
                metadata
                    .mux_stm
                    .update(prepare_message(
                        Operation::UpdateUser,
                        CLIENT,
                        SESSION,
                        &UpdateUserRequest {
                            user_id: WireIdentifier::numeric(0),
                            username: None,
                            status: Some(status.as_code()),
                            options: WireOptions::empty(),
                        }
                        .to_bytes(),
                    ))
                    .unwrap();
            }
            metadata
                .client_table
                .borrow_mut()
                .commit_register(
                    client_id,
                    0,
                    bind_verifier(client_id, 0, &SECRET),
                    register_reply(client_id, SESSION),
                )
                .unwrap();
            metadata.applied_frontier().advance(SESSION);
            let sessions = Rc::new(RefCell::new(SessionManager::new()));
            sessions.borrow_mut().ensure_connection(
                TRANSPORT,
                "127.0.0.1:34567".parse().unwrap(),
                ClientTransportKind::Tcp,
            );
            let outcome = complete_session_binding(
                &shard,
                &sessions,
                TRANSPORT,
                SessionIdentity {
                    client_id,
                    session: SESSION,
                    metadata_watermark: SESSION,
                },
                BindSecret::new(Box::new(SECRET)),
            )
            .await;
            if status == Some(UserStatus::Active)
                && !message_bus::is_server_internal_client(client_id)
            {
                assert_eq!(outcome.unwrap(), (0, SESSION));
                assert_eq!(
                    sessions.borrow().get_session(TRANSPORT),
                    Some((client_id, SESSION))
                );
            } else {
                assert!(
                    matches!(outcome, Err(IggyError::Unauthenticated)),
                    "client {client_id}, {status:?}: {outcome:?}"
                );
                assert!(sessions.borrow().get_session(TRANSPORT).is_none());
            }
        }
    }

    /// A read carries the metadata op of the client's last write, and the
    /// connection floor can be older. The read must wait for that op, or a
    /// client could miss its own write.
    #[compio::test]
    async fn given_read_above_the_frontier_when_dispatched_should_wait_for_its_metadata_op() {
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, 0, 1, FIRST_BOOT));
        commit_root_session(&shard);
        let sessions = Rc::new(RefCell::new(SessionManager::new()));
        bind_root_session(&shard, &sessions).await;
        let server_config = Arc::new(ServerConfig::default());

        handle_client_request(
            &shard,
            &sessions,
            &server_config,
            1,
            &Rc::new(ClientDrain::new(&server_config)),
            ROOT_TRANSPORT,
            held_get_me(1, UNAPPLIED_OP),
            None,
        )
        .await;
        settle().await;
        assert!(
            bus.client_replies.borrow().is_empty(),
            "the read must wait for op {UNAPPLIED_OP}"
        );
        shard
            .plane
            .metadata()
            .applied_frontier()
            .advance(UNAPPLIED_OP);
        await_replies(&bus, 1).await;
        let replies = bus.client_replies.borrow();
        assert_eq!(replies.len(), 1);
        assert_eq!(reply_status(&replies[0].1), 0);
    }

    const ROOT_CLIENT: u128 = 1;
    const ROOT_TRANSPORT: u128 = 2;
    const ROOT_SESSION: u64 = 1;
    const ROOT_SECRET: [u8; 32] = [0x5a; 32];
    /// The op of a write the root client was told committed. The frontier
    /// stays below it until a test applies it.
    const UNAPPLIED_OP: u64 = ROOT_SESSION + 1;

    /// Commit a session of `ROOT_CLIENT` for the root user straight into the
    /// client table, and apply the frontier up to it.
    fn commit_root_session(shard: &TestShard) {
        let metadata = shard.plane.metadata();
        metadata.mux_stm.users().ensure_root_user("iggy", "hash");
        metadata
            .client_table
            .borrow_mut()
            .commit_register(
                ROOT_CLIENT,
                0,
                bind_verifier(ROOT_CLIENT, 0, &ROOT_SECRET),
                register_reply(ROOT_CLIENT, ROOT_SESSION),
            )
            .unwrap();
        metadata.applied_frontier().advance(ROOT_SESSION);
    }

    /// Record `ROOT_TRANSPORT` as an open connection, as its first frame does
    /// on a bus that knows the peer.
    fn connect_root(sessions: &Rc<RefCell<SessionManager>>) {
        sessions.borrow_mut().ensure_connection(
            ROOT_TRANSPORT,
            "127.0.0.1:34567".parse().unwrap(),
            ClientTransportKind::Tcp,
        );
    }

    /// Bind `ROOT_TRANSPORT` to the session of [`commit_root_session`].
    async fn bind_root_session(shard: &Rc<TestShard>, sessions: &Rc<RefCell<SessionManager>>) {
        connect_root(sessions);
        complete_session_binding(
            shard,
            sessions,
            ROOT_TRANSPORT,
            SessionIdentity {
                client_id: ROOT_CLIENT,
                session: ROOT_SESSION,
                metadata_watermark: ROOT_SESSION,
            },
            BindSecret::new(Box::new(ROOT_SECRET)),
        )
        .await
        .unwrap();
    }

    /// A header-only read of `code` from the root session.
    fn root_read(code: u32, request: u64) -> Message<RequestHeader> {
        let mut read = wire_request(
            Operation::NonReplicated,
            ROOT_CLIENT,
            ROOT_SESSION,
            request,
            &[],
        );
        bytemuck::checked::from_bytes_mut::<RequestHeader>(
            &mut read.as_mut_slice()[..size_of::<RequestHeader>()],
        )
        .reserved[..4]
            .copy_from_slice(&code.to_le_bytes());
        read
    }

    /// A `GET_ME` that waits until the frontier applies `metadata_op`.
    fn held_get_me(request: u64, metadata_op: u64) -> Message<GenericHeader> {
        let mut read = root_read(GET_ME_CODE, request);
        bytemuck::checked::from_bytes_mut::<RequestHeader>(
            &mut read.as_mut_slice()[..size_of::<RequestHeader>()],
        )
        .minimum_metadata_op = metadata_op;
        read.into_generic()
    }

    #[compio::test]
    async fn given_a_read_held_at_the_frontier_when_the_session_ends_should_refuse_stale_client() {
        let (bus, shard, host) = root_host(ServerConfig::default());
        bind_root_session(&shard, host.sessions()).await;

        host.on_client_request(ROOT_TRANSPORT, held_get_me(1, UNAPPLIED_OP));
        settle().await;
        assert!(
            bus.client_replies.borrow().is_empty(),
            "the read must wait for op {UNAPPLIED_OP}"
        );
        let metadata = shard.plane.metadata();
        assert!(metadata.client_table.borrow_mut().end_session(
            ROOT_CLIENT,
            DEFAULT_ROOT_USER_ID,
            ROOT_SESSION,
            UNAPPLIED_OP
        ));
        metadata.applied_frontier().advance(UNAPPLIED_OP);

        await_replies(&bus, 1).await;
        let replies = bus.client_replies.borrow();
        assert_eq!(replies.len(), 1);
        assert_eq!(
            reply_status(&replies[0].1),
            IggyError::StaleClient.as_code()
        );
    }

    #[compio::test]
    async fn given_a_read_waiting_on_a_shard_when_the_session_ends_should_refuse_stale_client() {
        for (code, body) in shard_waiting_reads() {
            assert_read_refused_after_shard_wait(code, &body, ReadRevocation::Session).await;
        }
    }

    #[compio::test]
    async fn given_a_read_waiting_on_a_shard_when_grants_are_revoked_should_refuse_unauthorized() {
        for (code, body) in shard_waiting_reads() {
            assert_read_refused_after_shard_wait(code, &body, ReadRevocation::Grants).await;
        }
    }

    #[compio::test]
    async fn given_a_route_waiting_on_a_shard_when_the_topic_is_recreated_should_refuse_stale_route()
     {
        let (code, body) = shard_waiting_reads()
            .into_iter()
            .find(|(code, _)| *code == GET_PARTITION_ROUTE_CODE)
            .unwrap();
        assert_read_refused_after_shard_wait(code, &body, ReadRevocation::Topic).await;
    }

    /// Every read that asks another shard before it answers: the client reads
    /// ask every shard, the rest ask the owner of the partition.
    fn shard_waiting_reads() -> [(u32, Bytes); 9] {
        let offset = GetConsumerOffsetRequest {
            consumer: WireConsumer::consumer(WireIdentifier::numeric(1)),
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            partition_id: Some(0),
        }
        .to_bytes();
        let poll = PollMessagesRequest {
            consumer: WireConsumer::consumer(WireIdentifier::numeric(1)),
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            partition_id: Some(0),
            strategy: WirePollingStrategy::offset(0),
            count: 1,
            auto_commit: false,
        }
        .to_bytes();
        [
            (GET_CLIENTS_CODE, GetClientsRequest.to_bytes()),
            (
                GET_CLIENT_CODE,
                GetClientRequest { client_id: 1 }.to_bytes(),
            ),
            (GET_STATS_CODE, GetStatsRequest.to_bytes()),
            (
                GET_PARTITION_ROUTE_CODE,
                GetPartitionRouteRequest {
                    stream_id: WireIdentifier::numeric(0),
                    topic_id: WireIdentifier::numeric(0),
                    partition_id: 0,
                }
                .to_bytes(),
            ),
            (GET_POLL_ROUTING_CODE, poll.clone()),
            (POLL_MESSAGES_CODE, poll.clone()),
            (POLL_MESSAGES_ON_PRIMARY_CODE, poll),
            (GET_CONSUMER_OFFSET_ROUTING_CODE, offset.clone()),
            (GET_CONSUMER_OFFSET_CODE, offset),
        ]
    }

    enum ReadRevocation {
        Session,
        Grants,
        Topic,
    }

    #[allow(clippy::too_many_lines)]
    async fn assert_read_refused_after_shard_wait(code: u32, body: &[u8], change: ReadRevocation) {
        let bus = SpyBus::default();
        let mut shard = test_shard(&bus, 0, 1, FIRST_BOOT);
        let (sender, shard_inbox, _shard_replies) = shard_channel(0, 1, 1);
        shard.attach_senders(vec![sender]);
        let shard = Rc::new(shard);
        let namespace = create_one_partition(&shard);
        shard
            .shards_table()
            .insert(namespace, PartitionLocation::new(ShardId::new(0), 0));
        let sessions = bind_reader_session(&shard).await;
        let mut read = wire_request(
            Operation::NonReplicated,
            READER_CLIENT,
            READER_SESSION,
            1,
            body,
        );
        let header = bytemuck::checked::from_bytes_mut::<RequestHeader>(
            &mut read.as_mut_slice()[..size_of::<RequestHeader>()],
        );
        header.reserved[..4].copy_from_slice(&code.to_le_bytes());
        header.partition_incarnation = shard
            .plane
            .metadata()
            .mux_stm
            .streams()
            .created_revision_for_namespace(namespace)
            .unwrap();
        let server_config = Arc::new(ServerConfig::default());

        handle_client_request(
            &shard,
            &sessions,
            &server_config,
            1,
            &Rc::new(ClientDrain::new(&server_config)),
            READER_TRANSPORT,
            read.into_generic(),
            None,
        )
        .await;
        settle().await;
        let Ok(ShardFrame::Lifecycle(frame)) = shard_inbox.try_recv() else {
            panic!("code {code} must wait on a shard");
        };
        assert!(bus.client_replies.borrow().is_empty());
        let metadata = shard.plane.metadata();
        let refusal = match change {
            ReadRevocation::Session => {
                assert!(metadata.client_table.borrow_mut().end_session(
                    READER_CLIENT,
                    READER,
                    READER_SESSION,
                    READER_SESSION + 1
                ));
                IggyError::StaleClient
            }
            ReadRevocation::Grants => {
                let revoke = UpdatePermissionsRequest {
                    user_id: WireIdentifier::numeric(READER),
                    permissions: None,
                };
                let revoked = metadata
                    .mux_stm
                    .update(prepare_message(
                        Operation::UpdatePermissions,
                        0,
                        4,
                        &revoke.to_bytes(),
                    ))
                    .unwrap();
                assert_eq!(revoked.code, 0);
                IggyError::Unauthorized
            }
            ReadRevocation::Topic => {
                let apply = |op, operation, body: Bytes| {
                    metadata
                        .mux_stm
                        .update(prepare_message(operation, 0, op, &body).transmute_header(
                            |old, header: &mut iggy_binary_protocol::PrepareHeader| {
                                *header = old;
                                header.op = op;
                            },
                        ))
                        .unwrap()
                };
                let deleted = apply(
                    4,
                    Operation::DeleteTopic,
                    DeleteTopicRequest {
                        stream_id: WireIdentifier::numeric(
                            namespace.stream_id().try_into().unwrap(),
                        ),
                        topic_id: WireIdentifier::numeric(namespace.topic_id().try_into().unwrap()),
                    }
                    .to_bytes(),
                );
                assert_eq!(deleted.code, 0);
                let completion = CompleteLifecycleRequest::new(
                    4,
                    namespace.stream_id().try_into().unwrap(),
                    vec![LifecycleCompletion {
                        topic_id: namespace.topic_id().try_into().unwrap(),
                        partition_id: namespace.partition_id().try_into().unwrap(),
                        partition_op: 1,
                    }],
                )
                .unwrap();
                let completed = apply(5, Operation::CompleteLifecycle, completion.to_bytes());
                assert_eq!(completed.code, 0);
                let created = apply(
                    6,
                    Operation::CreateTopicWithAssignments,
                    CreateTopicWithAssignmentsRequest {
                        request: CreateTopicRequest {
                            stream_id: WireIdentifier::numeric(
                                namespace.stream_id().try_into().unwrap(),
                            ),
                            partitions_count: 1,
                            name: WireName::new("topic").unwrap(),
                            options: WireOptions::empty(),
                        },
                        derived_options: WireOptions::empty(),
                        partitions: vec![CreatedPartitionAssignment {
                            partition_id: namespace.partition_id().try_into().unwrap(),
                            consensus_group_id: 2,
                        }],
                        created_view: 0,
                    }
                    .to_bytes(),
                );
                assert_eq!(created.code, 0);
                IggyError::TransientNotAccepted
            }
        };
        answer_shard_read(code, frame);

        await_replies(&bus, 1).await;
        let replies = bus.client_replies.borrow();
        assert_eq!(replies.len(), 1, "code {code} must answer once");
        assert_eq!(
            reply_status(&replies[0].1),
            refusal.as_code(),
            "code {code} must refuse a stale read"
        );
    }

    /// The user of [`create_reader_request`], the first one after root, and
    /// its session.
    const READER: u32 = 1;
    const READER_CLIENT: u128 = 10;
    const READER_TRANSPORT: u128 = 20;
    const READER_SESSION: u64 = 1;
    const READER_SECRET: [u8; 32] = [0x5a; 32];

    /// Create the reader, commit its session, and bind `READER_TRANSPORT` to
    /// that session.
    async fn bind_reader_session(shard: &Rc<TestShard>) -> Rc<RefCell<SessionManager>> {
        let metadata = shard.plane.metadata();
        let created = metadata
            .mux_stm
            .update(prepare_message(
                Operation::CreateUser,
                0,
                3,
                &create_reader_request().to_bytes(),
            ))
            .unwrap();
        assert_eq!(created.code, 0);
        metadata
            .client_table
            .borrow_mut()
            .commit_register(
                READER_CLIENT,
                READER,
                bind_verifier(READER_CLIENT, READER, &READER_SECRET),
                register_reply(READER_CLIENT, READER_SESSION),
            )
            .unwrap();
        metadata.applied_frontier().advance(READER_SESSION);
        let sessions = Rc::new(RefCell::new(SessionManager::new()));
        sessions.borrow_mut().ensure_connection(
            READER_TRANSPORT,
            "127.0.0.1:34567".parse().unwrap(),
            ClientTransportKind::Tcp,
        );
        complete_session_binding(
            shard,
            &sessions,
            READER_TRANSPORT,
            SessionIdentity {
                client_id: READER_CLIENT,
                session: READER_SESSION,
                metadata_watermark: READER_SESSION,
            },
            BindSecret::new(Box::new(READER_SECRET)),
        )
        .await
        .unwrap();
        sessions
    }

    /// Answer `frame` of the read `code` the way a shard does when it has no
    /// clients, is the partition primary and holds a stored offset.
    fn answer_shard_read(code: u32, frame: LifecycleFrame) {
        const STORED_OFFSET: u64 = 5;
        match frame {
            LifecycleFrame::ListClients {
                reply: ListClientsReply::Clients(reply),
            } => reply.try_send(Vec::new()).unwrap(),
            LifecycleFrame::ListClients {
                reply: ListClientsReply::Count(reply),
            } => reply.try_send(0).unwrap(),
            LifecycleFrame::PartitionRead {
                read: PartitionRead::Primary,
                reply,
                ..
            } => reply
                .try_send(PartitionReadReply::Primary {
                    view: 0,
                    primary: 0,
                })
                .unwrap(),
            LifecycleFrame::PartitionRead {
                read: PartitionRead::PollOnPrimary { attachment, .. },
                reply,
                ..
            } => reply
                .try_send(PartitionReadReply::Poll {
                    context: attachment.metadata.context(0),
                    fragments: Vec::new().into(),
                    current_offset: STORED_OFFSET,
                })
                .unwrap(),
            LifecycleFrame::PartitionRead {
                read: PartitionRead::ConsumerOffset { .. },
                reply,
                ..
            } => reply
                .try_send(PartitionReadReply::ConsumerOffset {
                    stored: Some(STORED_OFFSET),
                    current_offset: STORED_OFFSET,
                })
                .unwrap(),
            _ => panic!("code {code} sent an unexpected frame"),
        }
    }

    #[compio::test]
    async fn given_deleted_user_when_id_reused_should_reject_retained_session() {
        for primary in [false, true] {
            assert_revoked_user_cannot_resume(Operation::DeleteUser, primary).await;
        }
    }

    #[compio::test]
    async fn given_deactivated_user_when_reactivated_should_reject_retained_session() {
        for primary in [false, true] {
            assert_revoked_user_cannot_resume(Operation::UpdateUser, primary).await;
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn assert_revoked_user_cannot_resume(operation: Operation, primary: bool) {
        const CLIENT: u128 = 10;
        const TRANSPORT: u128 = 20;
        const SESSION: u64 = 2;
        const USER: u32 = 1;
        const ROOT_CLIENT: u128 = CLIENT + 1;
        const SECRET: [u8; 32] = [0x5a; 32];
        let bus = SpyBus::default();
        let directory = tempfile::tempdir().unwrap();
        let mut shard = test_shard(&bus, 0, 1, FIRST_BOOT);
        shard.plane.metadata_mut().journal = Some(
            PrepareJournal::open(&directory.path().join("journal.wal"), 0)
                .await
                .unwrap(),
        );
        let shard = Rc::new(shard);
        let metadata = shard.plane.metadata();
        metadata.mux_stm.users().ensure_root_user("iggy", "hash");
        let create = CreateUserRequest {
            username: WireName::new("original").unwrap(),
            password: "hash".to_owned(),
            status: UserStatus::Active.as_code(),
            permissions: None,
            options: WireOptions::empty(),
        };
        let created = metadata
            .mux_stm
            .update(prepare_message(
                Operation::CreateUser,
                0,
                1,
                &create.to_bytes(),
            ))
            .unwrap();
        assert_eq!(created.code, 0);
        metadata
            .client_table
            .borrow_mut()
            .commit_register(
                CLIENT,
                USER,
                bind_verifier(CLIENT, USER, &SECRET),
                register_reply(CLIENT, SESSION),
            )
            .unwrap();
        metadata.applied_frontier().advance(SESSION);
        let sessions = Rc::new(RefCell::new(SessionManager::new()));
        sessions.borrow_mut().ensure_connection(
            TRANSPORT,
            "127.0.0.1:34567".parse().unwrap(),
            ClientTransportKind::Tcp,
        );
        let identity = SessionIdentity {
            client_id: CLIENT,
            session: SESSION,
            metadata_watermark: SESSION,
        };
        complete_session_binding(
            &shard,
            &sessions,
            TRANSPORT,
            identity,
            BindSecret::new(Box::new(SECRET)),
        )
        .await
        .unwrap();
        let body = if operation == Operation::DeleteUser {
            DeleteUserRequest {
                user_id: WireIdentifier::numeric(USER),
            }
            .to_bytes()
        } else {
            UpdateUserRequest {
                user_id: WireIdentifier::numeric(USER),
                username: None,
                status: Some(UserStatus::Inactive.as_code()),
                options: WireOptions::empty(),
            }
            .to_bytes()
        };
        metadata
            .client_table
            .borrow_mut()
            .commit_register(ROOT_CLIENT, 0, SECRET, register_reply(ROOT_CLIENT, 1))
            .unwrap();
        if primary {
            let consensus = metadata.consensus.as_ref().unwrap();
            consensus.restore_commit_state(SESSION, SESSION);
            consensus.sequencer().set_sequence(SESSION);
            let request = request_message(operation, ROOT_CLIENT, 1, 1, &body).transmute_header(
                |old, header: &mut RoutedRequestHeader| {
                    *header = old;
                    header.metadata_watermark = SESSION;
                },
            );
            metadata.submit_request_in_process(request).await.unwrap();
        } else {
            let revocation = prepare_message(operation, ROOT_CLIENT, 3, &body).transmute_header(
                |old, header: &mut iggy_binary_protocol::PrepareHeader| {
                    *header = old;
                    header.op = 3;
                },
            );
            let reply = metadata::apply_committed_prepare(
                &*metadata.mux_stm,
                &metadata.client_table,
                true,
                |_| {},
                consensus::seal_prepare_checksum(revocation),
            );
            assert_eq!(iggy_binary_protocol::result_code(reply.body()), Some(0));
        }
        let old_binding = sessions.borrow().get_session(TRANSPORT);
        let restore = if operation == Operation::DeleteUser {
            prepare_message(
                Operation::CreateUser,
                0,
                4,
                &CreateUserRequest {
                    username: WireName::new("replacement").unwrap(),
                    ..create
                }
                .to_bytes(),
            )
        } else {
            prepare_message(
                Operation::UpdateUser,
                0,
                4,
                &UpdateUserRequest {
                    user_id: WireIdentifier::numeric(USER),
                    username: None,
                    status: Some(UserStatus::Active.as_code()),
                    options: WireOptions::empty(),
                }
                .to_bytes(),
            )
        };
        assert_eq!(metadata.mux_stm.update(restore).unwrap().code, 0);
        assert!(metadata.mux_stm.users().read(|users| {
            users
                .items
                .get(USER as usize)
                .is_some_and(|user| user.status == UserStatus::Active)
        }));
        let outcome = complete_session_binding(
            &shard,
            &sessions,
            TRANSPORT,
            identity,
            BindSecret::new(Box::new(SECRET)),
        )
        .await;
        assert!(
            matches!(outcome, Err(IggyError::Unauthenticated)),
            "committed {operation:?} must reject the old secret after restoring user id {USER}: {outcome:?}"
        );
        assert!(
            old_binding.is_none(),
            "committed {operation:?} must invalidate an already authenticated connection"
        );
        assert_eq!(metadata.client_table.borrow().ended_sessions().count(), 1);
    }

    /// A test shard wired to its own lanes (the held sender feeds them),
    /// for the reply-lane pump tests below.
    fn reply_lane_test_shard(name: &str) -> (SpyBus, shard::TaggedSender, Rc<TestShard>) {
        // These tests do not dispatch disk polls, so keep that lane minimal.
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
        let (sender, inbox_rx, reply_inbox_rx) = shard_channel(0, 16, 16);
        let lane_sender = sender.clone();
        let shard = TestShard::new(
            ShardIdentity::new(0, name.to_string()),
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

    /// A shard built on [`NoopHost`] routes the host-bound lifecycle frames
    /// like any other: the host drops the carried reply sender, so the
    /// waiting side sees a disconnect instead of hanging on the pump, and
    /// the pump goes on serving frames afterwards.
    #[compio::test]
    async fn noop_host_drops_host_bound_replies_and_keeps_the_pump_alive() {
        const TRANSPORT: u128 = 94;
        const BUDGET: std::time::Duration = std::time::Duration::from_secs(5);
        let (bus, lane_sender, shard) = reply_lane_test_shard("noop-host-test");
        let (list_reply, list_rx) = shard::channel::<Vec<ConnectedClientInfo>>(1);
        let (submit_reply, submit_rx) = shard::channel::<Option<Message<GenericHeader>>>(1);
        lane_sender
            .try_send(ShardFrame::lifecycle(LifecycleFrame::ListClients {
                reply: ListClientsReply::Clients(list_reply),
            }))
            .expect("inbox has capacity");
        lane_sender
            .try_send(ShardFrame::lifecycle(LifecycleFrame::MetadataSubmit(
                MetadataSubmit::ClientRequest {
                    request: Message::<GenericHeader>::new(size_of::<GenericHeader>()),
                    reply: submit_reply,
                },
            )))
            .expect("inbox has capacity");

        let (stop_tx, stop_rx) = shard::channel::<()>(1);
        let pump_shard = Rc::clone(&shard);
        let pump = compio::runtime::spawn(async move {
            pump_shard
                .run_message_pump(stop_rx, Arc::new(AtomicBool::new(false)))
                .await;
        });

        let list = compio::time::timeout(BUDGET, list_rx.recv())
            .await
            .expect("the pump must process the list-clients frame");
        let submit = compio::time::timeout(BUDGET, submit_rx.recv())
            .await
            .expect("the pump must process the metadata-submit frame");
        assert!(
            list.is_err(),
            "a noop host answers list-clients by dropping the reply sender"
        );
        assert!(
            submit.is_err(),
            "a noop host answers metadata-submit by dropping the reply sender"
        );

        // Sent only now, so its delivery proves the pump outlived both
        // host-bound frames rather than being served between them.
        lane_sender
            .reply_sender()
            .try_send(reply_lane_forward(TRANSPORT))
            .expect("reply lane has capacity");
        compio::time::timeout(BUDGET, async {
            while bus.client_replies.borrow().is_empty() {
                compio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the pump must keep serving after the host-bound frames");
        stop_tx.try_send(()).expect("stop channel has capacity");
        let _ = pump.await;

        let replies = bus.client_replies.borrow();
        assert_eq!(replies[0].0, TRANSPORT, "forward must reach its client");
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
        let server_config = Arc::new(ServerConfig::default());

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
                &server_config,
                1,
                &Rc::new(ClientDrain::new(&server_config)),
                TRANSPORT,
                metadata_read(),
                None,
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

    fn reply_request(frame: &[u8]) -> u64 {
        const REQUEST_OFFSET: usize = std::mem::offset_of!(ReplyHeader, request);
        u64::from_le_bytes(
            frame[REQUEST_OFFSET..REQUEST_OFFSET + 8]
                .try_into()
                .unwrap(),
        )
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
        let server_config = Arc::new(ServerConfig::default());

        for code in [LOGIN_USER_CODE, LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE] {
            handle_client_request(
                &shard,
                &sessions,
                &server_config,
                1,
                &Rc::new(ClientDrain::new(&server_config)),
                TRANSPORT,
                non_replicated_request(TRANSPORT, code),
                None,
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
        let server_config = Arc::new(ServerConfig::default());

        handle_client_request(
            &shard,
            &sessions,
            &server_config,
            1,
            &Rc::new(ClientDrain::new(&server_config)),
            TRANSPORT,
            wire_request(Operation::CreateStream, TRANSPORT, 1, 1, &[]).into_generic(),
            None,
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
        let server_config = Arc::new(ServerConfig::default());

        handle_client_request(
            &shard,
            &sessions,
            &server_config,
            1,
            &Rc::new(ClientDrain::new(&server_config)),
            TRANSPORT,
            non_replicated_request(TRANSPORT, PING_CODE),
            None,
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
        let server_config = Arc::new(ServerConfig::default());

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
            &server_config,
            1,
            &Rc::new(ClientDrain::new(&server_config)),
            TRANSPORT,
            message.into_generic(),
            None,
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

    /// Rounds of [`run_spawned_tasks`] in [`settle`], more than any task chain
    /// here needs to reach its next wait.
    const SETTLE_ROUNDS: usize = 10;

    async fn settle() {
        for _ in 0..SETTLE_ROUNDS {
            run_spawned_tasks().await;
        }
    }

    /// Run spawned tasks until `bus` holds `count` replies, or for 500 rounds.
    async fn await_replies(bus: &SpyBus, count: usize) {
        for _ in 0..500 {
            if bus.client_replies.borrow().len() >= count {
                return;
            }
            run_spawned_tasks().await;
        }
    }

    /// A host over a fresh shard with [`commit_root_session`] done. No
    /// connection is bound to the session yet.
    fn root_host(
        server_config: ServerConfig,
    ) -> (
        SpyBus,
        Rc<TestShard>,
        ServerHost<SpyBus, PrepareJournal, IggySnapshot>,
    ) {
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, 0, 1, FIRST_BOOT));
        commit_root_session(&shard);
        let shard_handle = unset_shard_handle();
        *shard_handle.borrow_mut() = Some(Rc::downgrade(&shard));
        let host = ServerHost::new(&bus, &shard_handle, Arc::new(server_config), 1);
        (bus, shard, host)
    }

    /// One host per shard means one connection-lost hook on its bus: a
    /// second install would overwrite the first and orphan the sessions
    /// the first one bound.
    #[test]
    fn server_host_installs_one_connection_lost_hook() {
        let bus = SpyBus::default();
        let _host = ServerHost::new(
            &bus,
            &unset_shard_handle(),
            Arc::new(ServerConfig::default()),
            1,
        );
        assert_eq!(
            bus.connection_lost_hooks.get(),
            1,
            "one host must install exactly one connection-lost hook"
        );
    }

    /// The host is built before the shard it serves. A frame that
    /// arrives while the self-reference is still unset stays queued, and
    /// the enqueue must release the client's active slot: otherwise every
    /// later frame for that client finds the slot taken and nothing is
    /// ever drained, the stranded frame included.
    #[compio::test]
    async fn server_host_drains_after_the_shard_handle_is_set() {
        const TRANSPORT: u128 = 98;
        let bus = SpyBus::default();
        let shard_handle = unset_shard_handle();
        let host = ServerHost::new(&bus, &shard_handle, Arc::new(ServerConfig::default()), 1);

        host.on_client_request(TRANSPORT, non_replicated_request(TRANSPORT, PING_CODE));
        run_spawned_tasks().await;
        assert!(
            bus.client_replies.borrow().is_empty(),
            "nothing can be served before the shard exists"
        );

        let shard = Rc::new(test_shard(&bus, 0, 1, FIRST_BOOT));
        *shard_handle.borrow_mut() = Some(Rc::downgrade(&shard));
        host.on_client_request(TRANSPORT, non_replicated_request(TRANSPORT, PING_CODE));
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

    /// A read held for the metadata frontier runs in its own task, so a `PING`
    /// behind it on the same connection gets its reply first.
    #[compio::test]
    async fn given_a_held_read_when_a_ping_follows_on_one_connection_should_answer_the_ping_first()
    {
        const READ_REQUEST: u64 = 1;
        const PING_REQUEST: u64 = 2;
        let (bus, shard, host) = root_host(ServerConfig::default());
        bind_root_session(&shard, host.sessions()).await;

        host.on_client_request(ROOT_TRANSPORT, held_get_me(READ_REQUEST, UNAPPLIED_OP));
        host.on_client_request(
            ROOT_TRANSPORT,
            root_read(PING_CODE, PING_REQUEST).into_generic(),
        );
        await_replies(&bus, 1).await;
        {
            let replies = bus.client_replies.borrow();
            assert_eq!(replies.len(), 1, "the ping must not wait for the held read");
            assert_eq!(reply_request(&replies[0].1), PING_REQUEST);
            assert_eq!(reply_status(&replies[0].1), 0);
        }

        shard
            .plane
            .metadata()
            .applied_frontier()
            .advance(UNAPPLIED_OP);
        await_replies(&bus, 2).await;
        let replies = bus.client_replies.borrow();
        assert_eq!(replies.len(), 2, "the read must answer once its op applies");
        assert_eq!(reply_request(&replies[1].1), READ_REQUEST);
        assert_eq!(reply_status(&replies[1].1), 0);
    }

    #[compio::test]
    async fn given_a_held_partition_poll_when_ping_follows_should_answer_ping_before_owner_reply() {
        const POLL_REQUEST: u64 = 1;
        const PING_REQUEST: u64 = 2;
        for code in [POLL_MESSAGES_CODE, POLL_MESSAGES_ON_PRIMARY_CODE] {
            let bus = SpyBus::default();
            let mut shard = test_shard(&bus, 0, 1, FIRST_BOOT);
            let (sender, owner_inbox, _owner_replies) = shard_channel(0, 1, 1);
            shard.attach_senders(vec![sender]);
            let shard = Rc::new(shard);
            let namespace = create_one_partition(&shard);
            shard
                .shards_table()
                .insert(namespace, PartitionLocation::new(ShardId::new(0), 0));
            commit_root_session(&shard);
            let shard_handle = Rc::new(RefCell::new(Some(Rc::downgrade(&shard))));
            let host = ServerHost::new(&bus, &shard_handle, Arc::new(ServerConfig::default()), 1);
            bind_root_session(&shard, host.sessions()).await;
            let body = PollMessagesRequest {
                consumer: WireConsumer::consumer(WireIdentifier::numeric(1)),
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(0),
                partition_id: Some(0),
                strategy: WirePollingStrategy::offset(0),
                count: 1,
                auto_commit: false,
            }
            .to_bytes();
            let mut poll = wire_request(
                Operation::NonReplicated,
                ROOT_CLIENT,
                ROOT_SESSION,
                POLL_REQUEST,
                &body,
            );
            let header = bytemuck::checked::from_bytes_mut::<RequestHeader>(
                &mut poll.as_mut_slice()[..size_of::<RequestHeader>()],
            );
            header.reserved[..4].copy_from_slice(&code.to_le_bytes());
            header.partition_incarnation = shard
                .plane
                .metadata()
                .mux_stm
                .streams()
                .created_revision_for_namespace(namespace)
                .unwrap();
            host.on_client_request(ROOT_TRANSPORT, poll.into_generic());
            settle().await;
            let ShardFrame::Lifecycle(LifecycleFrame::PartitionRead {
                read: PartitionRead::PollOnPrimary { .. },
                reply,
                ..
            }) = owner_inbox
                .try_recv()
                .expect("the poll must reach its partition owner")
            else {
                panic!("the owner must receive a primary-gated poll");
            };
            assert!(bus.client_replies.borrow().is_empty());

            host.on_client_request(
                ROOT_TRANSPORT,
                root_read(PING_CODE, PING_REQUEST).into_generic(),
            );
            await_replies(&bus, 1).await;
            {
                let replies = bus.client_replies.borrow();
                assert_eq!(
                    replies.len(),
                    1,
                    "PING must not wait for partition I/O on code {code}"
                );
                assert_eq!(reply_request(&replies[0].1), PING_REQUEST);
            }
            reply
                .try_send(PartitionReadReply::Rejected(
                    IggyError::TransientNotAccepted,
                ))
                .unwrap();
            await_replies(&bus, 2).await;
            let replies = bus.client_replies.borrow();
            assert_eq!(replies.len(), 2);
            assert_eq!(reply_request(&replies[1].1), POLL_REQUEST);
        }
    }

    #[compio::test]
    async fn given_a_held_store_when_next_and_offset_reads_queue_should_wait_for_its_guard() {
        const CONSUMER_ID: u32 = 1;
        const NEXT_REQUEST: u64 = 2;
        const OFFSET_REQUEST: u64 = 3;
        let (bus, shard, host) = root_host(ServerConfig::default());
        let namespace = create_one_partition(&shard);
        bind_root_session(&shard, host.sessions()).await;
        let config = Arc::new(ServerConfig::default());
        let drain = Rc::new(ClientDrain::new(&config));
        let store = StoreConsumerOffsetRequest {
            consumer: WireConsumer::consumer(WireIdentifier::numeric(CONSUMER_ID)),
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            partition_id: Some(0),
            offset: 0,
            ack: AckLevel::Quorum,
        }
        .to_bytes();
        let store = wire_request(
            Operation::StoreConsumerOffset,
            ROOT_CLIENT,
            ROOT_SESSION,
            1,
            &store,
        )
        .into_generic();
        let key = progress_key(&shard, &store).expect("store must take a consumer progress lane");
        assert_eq!(key.0, namespace.inner());
        let held = drain.admit_progress(key);
        let poll = PollMessagesRequest {
            consumer: WireConsumer::consumer(WireIdentifier::numeric(CONSUMER_ID)),
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            partition_id: Some(0),
            strategy: WirePollingStrategy::next(),
            count: 1,
            auto_commit: false,
        }
        .to_bytes();
        let offset = GetConsumerOffsetRequest {
            consumer: WireConsumer::consumer(WireIdentifier::numeric(CONSUMER_ID)),
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            partition_id: Some(0),
        }
        .to_bytes();
        let mut reads = VecDeque::new();
        for (request, code, body) in [
            (NEXT_REQUEST, POLL_MESSAGES_CODE, poll),
            (OFFSET_REQUEST, GET_CONSUMER_OFFSET_CODE, offset),
        ] {
            let mut message = wire_request(
                Operation::NonReplicated,
                ROOT_CLIENT,
                ROOT_SESSION,
                request,
                &body,
            );
            bytemuck::checked::from_bytes_mut::<RequestHeader>(
                &mut message.as_mut_slice()[..size_of::<RequestHeader>()],
            )
            .reserved[..4]
                .copy_from_slice(&code.to_le_bytes());
            reads.push_back(QueuedClientRequest::from(message.into_generic()));
        }
        let queues = Rc::new(RefCell::new(AHashMap::from_iter([(ROOT_TRANSPORT, reads)])));
        let running = drain_client_requests(
            Rc::clone(&shard),
            Rc::clone(host.sessions()),
            config,
            1,
            Rc::clone(&queues),
            &drain,
            ROOT_TRANSPORT,
        );
        futures::pin_mut!(running);
        assert!(futures::poll!(running.as_mut()).is_pending());
        settle().await;
        assert_eq!(
            queues
                .borrow()
                .get(&ROOT_TRANSPORT)
                .map_or(0, VecDeque::len),
            2,
            "Next without auto-commit and GetConsumerOffset must both stay queued"
        );
        assert!(bus.client_replies.borrow().is_empty());

        drop(held);
        assert!(futures::poll!(running.as_mut()).is_pending());
        await_replies(&bus, 1).await;
        while futures::poll!(running.as_mut()).is_pending() {
            settle().await;
        }
        await_replies(&bus, 2).await;
        let replies = bus.client_replies.borrow();
        assert_eq!(
            replies
                .iter()
                .map(|(_, reply)| reply_request(reply))
                .collect::<Vec<_>>(),
            [NEXT_REQUEST, OFFSET_REQUEST]
        );
    }

    #[compio::test]
    async fn given_queued_progress_when_metadata_changes_should_resolve_once_per_frontier() {
        const CREATED_FRONTIER: u64 = ROOT_SESSION + 1;
        let (_, shard, _) = root_host(ServerConfig::default());
        let body = StoreConsumerOffsetRequest {
            consumer: WireConsumer::consumer(WireIdentifier::numeric(1)),
            stream_id: WireIdentifier::named("stream").unwrap(),
            topic_id: WireIdentifier::named("topic").unwrap(),
            partition_id: Some(0),
            offset: 1,
            ack: AckLevel::Quorum,
        }
        .to_bytes();
        let mut queued = QueuedClientRequest::from(
            wire_request(
                Operation::StoreConsumerOffset,
                ROOT_CLIENT,
                ROOT_SESSION,
                1,
                &body,
            )
            .into_generic(),
        );
        assert!(
            queued.progress.is_none(),
            "enqueue must not resolve ahead of a metadata barrier"
        );
        let resolutions = Cell::new(0);
        let resolve = |message: &Message<GenericHeader>| {
            resolutions.set(resolutions.get() + 1);
            progress_key(&shard, message)
        };
        for _ in 0..3 {
            assert_eq!(queued.progress_key(ROOT_SESSION, resolve), None);
        }
        assert_eq!(
            resolutions.get(),
            1,
            "wakes must reuse a resolved missing key too"
        );
        let namespace = create_one_partition(&shard);
        shard
            .plane
            .metadata()
            .applied_frontier()
            .advance(CREATED_FRONTIER);
        for _ in 0..3 {
            assert_eq!(
                queued.progress_key(shard.plane.metadata().applied_frontier().get(), resolve),
                Some((namespace.inner(), iggy_binary_protocol::KIND_CONSUMER, 1))
            );
        }
        assert_eq!(
            resolutions.get(),
            2,
            "new metadata must invalidate the cached namespace"
        );
    }

    #[compio::test]
    async fn given_pending_metadata_when_read_and_ping_queue_should_answer_ping_then_wake_read() {
        const READ_REQUEST: u64 = 1;
        const PING_REQUEST: u64 = 2;
        let config = Arc::new(ServerConfig::default());
        let (bus, shard, host) = root_host((*config).clone());
        bind_root_session(&shard, host.sessions()).await;
        let drain = Rc::new(ClientDrain::new(&config));
        let metadata = drain.admit(Admission::MetadataLane).await;
        let queues = Rc::new(RefCell::new(AHashMap::from_iter([(
            ROOT_TRANSPORT,
            VecDeque::from(
                [
                    held_get_me(READ_REQUEST, 0),
                    root_read(PING_CODE, PING_REQUEST).into_generic(),
                ]
                .map(QueuedClientRequest::from),
            ),
        )])));
        let draining = drain_client_requests(
            shard,
            Rc::clone(host.sessions()),
            config,
            1,
            queues,
            &drain,
            ROOT_TRANSPORT,
        );
        futures::pin_mut!(draining);
        assert!(futures::poll!(draining.as_mut()).is_pending());
        {
            let replies = bus.client_replies.borrow();
            assert_eq!(replies.len(), 1, "the dependent read must remain queued");
            assert_eq!(reply_request(&replies[0].1), PING_REQUEST);
        }
        drop(metadata);
        draining.await;
        let replies = bus.client_replies.borrow();
        assert_eq!(
            replies.len(),
            2,
            "release must wake the read without another frame"
        );
        assert_eq!(reply_request(&replies[1].1), READ_REQUEST);
    }

    #[test]
    fn given_queued_progress_conflict_when_session_barrier_follows_should_only_let_ping_pass() {
        const BLOCKED: u64 = 1;
        const BARRIER: u64 = 2;
        const READ: u64 = 3;
        const PING: u64 = 4;
        for operation in [
            Operation::Logout,
            Operation::DeleteTopic,
            Operation::NonReplicated,
        ] {
            let barrier = if operation == Operation::NonReplicated {
                root_read(BIND_SESSION_CODE, BARRIER).into_generic()
            } else {
                wire_request(operation, ROOT_CLIENT, ROOT_SESSION, BARRIER, &[]).into_generic()
            };
            let queues = Rc::new(RefCell::new(AHashMap::from_iter([(
                ROOT_TRANSPORT,
                VecDeque::from(
                    [
                        wire_request(
                            Operation::StoreConsumerOffset,
                            ROOT_CLIENT,
                            ROOT_SESSION,
                            BLOCKED,
                            &[],
                        )
                        .into_generic(),
                        barrier,
                        held_get_me(READ, 0),
                        root_read(PING_CODE, PING).into_generic(),
                    ]
                    .map(QueuedClientRequest::from),
                ),
            )])));
            let ready = |queued: &mut QueuedClientRequest| {
                queued
                    .message
                    .try_as_typed::<RequestHeader>()
                    .unwrap()
                    .header()
                    .request
                    != BLOCKED
            };
            let ping = pop_next_client_request(&queues, ROOT_TRANSPORT, ready).unwrap();
            assert!(is_ping(&ping.message));
            assert!(pop_next_client_request(&queues, ROOT_TRANSPORT, ready).is_none());
            for expected in [BLOCKED, BARRIER, READ] {
                let message = pop_next_client_request(&queues, ROOT_TRANSPORT, |_| true).unwrap();
                assert_eq!(
                    message
                        .message
                        .try_as_typed::<RequestHeader>()
                        .unwrap()
                        .header()
                        .request,
                    expected
                );
            }
        }
    }

    #[compio::test]
    async fn given_a_full_in_flight_cap_when_another_frame_arrives_should_serve_it_after_a_slot_frees()
     {
        const CAPACITY: usize = 2;
        const HELD_READS: u64 = CAPACITY as u64 + 1;
        const PING_REQUEST: u64 = HELD_READS + 1;
        let mut server_config = ServerConfig::default();
        server_config.message_bus.client_queue_capacity = CAPACITY;
        let (bus, shard, host) = root_host(server_config);
        bind_root_session(&shard, host.sessions()).await;

        for request in 1..=HELD_READS {
            host.on_client_request(ROOT_TRANSPORT, held_get_me(request, UNAPPLIED_OP));
        }
        host.on_client_request(
            ROOT_TRANSPORT,
            root_read(PING_CODE, PING_REQUEST).into_generic(),
        );
        settle().await;
        assert!(
            bus.client_replies.borrow().is_empty(),
            "the read past the cap and the ping behind it must wait"
        );

        shard
            .plane
            .metadata()
            .applied_frontier()
            .advance(UNAPPLIED_OP);
        await_replies(&bus, 4).await;
        let replies = bus.client_replies.borrow();
        let mut requests: Vec<u64> = replies
            .iter()
            .map(|(_, frame)| reply_request(frame))
            .collect();
        requests.sort_unstable();
        assert_eq!(requests, [1, 2, HELD_READS, PING_REQUEST]);
        assert!(replies.iter().all(|(_, frame)| reply_status(frame) == 0));
    }

    /// The bus hook of a lost connection drops its queue but not its drain.
    /// The drain admits the frame it holds, then goes after its last request
    /// task ends.
    #[compio::test]
    async fn given_a_lost_connection_when_requests_are_in_flight_should_remove_the_drain_after_they_end()
     {
        const LATER_OP: u64 = UNAPPLIED_OP + 1;
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, 0, 1, FIRST_BOOT));
        commit_root_session(&shard);
        let shard_handle = unset_shard_handle();
        *shard_handle.borrow_mut() = Some(Rc::downgrade(&shard));
        let sessions = Rc::new(RefCell::new(SessionManager::new()));
        bind_root_session(&shard, &sessions).await;
        let mut server_config = ServerConfig::default();
        server_config.message_bus.client_queue_capacity = 1;
        let server_config = Arc::new(server_config);
        let queues: ClientRequestQueues = Rc::default();
        let active: ActiveClientRequests = Rc::default();
        let enqueue = |message| {
            enqueue_client_request(
                &bus,
                &shard_handle,
                &sessions,
                &server_config,
                1,
                &queues,
                &active,
                ROOT_TRANSPORT,
                message,
            );
        };

        // The first read takes the only place under the cap, the second waits
        // for it on the drain, and the ping waits in the queue.
        enqueue(held_get_me(1, UNAPPLIED_OP));
        enqueue(held_get_me(2, LATER_OP));
        enqueue(root_read(PING_CODE, 3).into_generic());
        settle().await;
        assert!(queues.borrow().contains_key(&ROOT_TRANSPORT));

        // What the `ServerHost` hook does when the bus loses the connection.
        queues.borrow_mut().remove(&ROOT_TRANSPORT);
        sessions.borrow_mut().remove_connection(ROOT_TRANSPORT);
        let frontier = shard.plane.metadata().applied_frontier();
        frontier.advance(UNAPPLIED_OP);
        await_replies(&bus, 1).await;
        settle().await;
        assert!(
            active.borrow().contains_key(&ROOT_TRANSPORT),
            "the drain must stay while its second read runs"
        );

        frontier.advance(LATER_OP);
        for _ in 0..500 {
            if active.borrow().is_empty() {
                break;
            }
            run_spawned_tasks().await;
        }
        assert!(
            active.borrow().is_empty(),
            "the drain must go after its last request task"
        );
        let replies = bus.client_replies.borrow();
        let mut requests: Vec<u64> = replies
            .iter()
            .map(|(_, frame)| reply_request(frame))
            .collect();
        requests.sort_unstable();
        assert_eq!(
            requests,
            [1, 2],
            "both reads run, and the dropped ping does not"
        );
    }

    /// `BIND_SESSION` runs on the drain, so the replicated frame behind it is
    /// served on the session it binds, not evicted for a missing session.
    #[compio::test]
    async fn given_a_bind_session_when_a_replicated_frame_follows_should_serve_it_bound() {
        const MISSING_STREAM: u32 = 404;
        let (bus, _shard, host) = root_host(ServerConfig::default());
        connect_root(host.sessions());
        let bind = BindSessionRequest {
            version_info: ClientVersionInfo {
                protocol_version: IGGY_PROTOCOL_VERSION,
                sdk_name: WireName::new("test-sdk").unwrap(),
                sdk_version: WireName::new("1.0.0").unwrap(),
            },
            identity: SessionIdentity {
                client_id: ROOT_CLIENT,
                session: ROOT_SESSION,
                metadata_watermark: ROOT_SESSION,
            },
            bind_secret: BindSecret::new(Box::new(ROOT_SECRET)),
        }
        .to_bytes();
        let mut bind = wire_request(Operation::NonReplicated, ROOT_CLIENT, 0, 1, &bind);
        bytemuck::checked::from_bytes_mut::<RequestHeader>(
            &mut bind.as_mut_slice()[..size_of::<RequestHeader>()],
        )
        .reserved[..4]
            .copy_from_slice(&BIND_SESSION_CODE.to_le_bytes());
        let store = StoreConsumerOffsetRequest {
            consumer: WireConsumer::consumer(WireIdentifier::Numeric(1)),
            stream_id: WireIdentifier::Numeric(MISSING_STREAM),
            topic_id: WireIdentifier::Numeric(1),
            partition_id: Some(0),
            offset: 0,
            ack: AckLevel::Quorum,
        }
        .to_bytes();
        let store = wire_request(
            Operation::StoreConsumerOffset,
            ROOT_CLIENT,
            ROOT_SESSION,
            2,
            &store,
        );

        host.on_client_request(ROOT_TRANSPORT, bind.into_generic());
        host.on_client_request(ROOT_TRANSPORT, store.into_generic());
        await_replies(&bus, 2).await;

        let replies = bus.client_replies.borrow();
        assert_eq!(replies.len(), 2);
        assert_eq!(reply_status(&replies[0].1), 0, "the bind must succeed");
        assert_eq!(
            frame_command(&replies[1].1),
            Command::Reply as u8,
            "a frame after the bind must not be evicted"
        );
        assert_eq!(
            reply_status(&replies[1].1),
            IggyError::StreamIdNotFound(Identifier::numeric(MISSING_STREAM).unwrap()).as_code()
        );
    }

    /// The metadata lane admits one metadata write of a connection at a time,
    /// and a read passes it under the cap.
    #[compio::test]
    async fn given_a_metadata_write_in_flight_when_another_admits_should_wait_until_the_first_drops()
     {
        let drain = Rc::new(ClientDrain::new(&ServerConfig::default()));
        let first = drain.admit(Admission::MetadataLane).await;
        let read = drain.admit(Admission::Cap);
        futures::pin_mut!(read);
        assert!(
            futures::poll!(read.as_mut()).is_ready(),
            "a read takes no lane"
        );
        let second = drain.admit(Admission::MetadataLane);
        futures::pin_mut!(second);
        assert!(futures::poll!(second.as_mut()).is_pending());
        drop(first);
        assert!(futures::poll!(second.as_mut()).is_ready());
    }

    /// A logout waits for the metadata lane while a write of its connection is
    /// in flight, so it cannot overtake that write in the metadata client
    /// table.
    #[compio::test]
    async fn given_a_metadata_write_in_flight_when_logout_arrives_should_wait_for_the_write() {
        let (bus, shard, host) = root_host(ServerConfig::default());
        bind_root_session(&shard, host.sessions()).await;
        let create = CreateStreamRequest {
            name: WireName::new("stream").unwrap(),
            options: WireOptions::empty(),
        }
        .to_bytes();

        // The test shard has no journal, so the write never commits.
        host.on_client_request(
            ROOT_TRANSPORT,
            wire_request(
                Operation::CreateStream,
                ROOT_CLIENT,
                ROOT_SESSION,
                1,
                &create,
            )
            .into_generic(),
        );
        host.on_client_request(
            ROOT_TRANSPORT,
            wire_request(Operation::Logout, ROOT_CLIENT, ROOT_SESSION, 2, &[]).into_generic(),
        );
        settle().await;

        assert!(
            bus.client_replies.borrow().is_empty(),
            "the logout must wait for the write"
        );
        assert_eq!(
            host.sessions().borrow().get_session(ROOT_TRANSPORT),
            Some((ROOT_CLIENT, ROOT_SESSION)),
            "the logout must not unbind the connection under the write"
        );
    }

    /// The two frames differ only in the operation byte. A declared one gets
    /// the retryable deny, and an undeclared one gets `InvalidCommand`,
    /// because no retry of that frame can succeed.
    #[compio::test]
    async fn overflowing_request_gets_transient_deny_unless_its_operation_is_undeclared() {
        const TRANSPORT: u128 = 99;
        assert!(
            !Operation::is_known_code(UNDECLARED_OPERATION),
            "test needs an operation byte this build does not declare"
        );
        for (operation, status) in [
            (
                Operation::NonReplicated as u8,
                IggyError::TransientNotAccepted.as_code(),
            ),
            (UNDECLARED_OPERATION, IggyError::InvalidCommand.as_code()),
        ] {
            let bus = SpyBus::default();
            let shard = Rc::new(test_shard(&bus, 0, 1, FIRST_BOOT));
            let shard_handle = unset_shard_handle();
            *shard_handle.borrow_mut() = Some(Rc::downgrade(&shard));
            let mut frame = non_replicated_request(TRANSPORT, PING_CODE);
            frame.as_mut_slice()[offset_of!(RequestHeader, operation)] = operation;

            deny_overflowing_client_request(&shard_handle, TRANSPORT, frame);
            for _ in 0..500 {
                if !bus.client_replies.borrow().is_empty() {
                    break;
                }
                run_spawned_tasks().await;
            }

            let replies = bus.client_replies.borrow();
            assert_eq!(
                replies.len(),
                1,
                "operation {operation:#04x}: an over-queue request must be answered"
            );
            let (client, reply) = &replies[0];
            assert_eq!(*client, TRANSPORT);
            assert_eq!(frame_command(reply), Command::Reply as u8);
            assert_eq!(
                reply_status(reply),
                status,
                "operation {operation:#04x}: wrong deny status"
            );
            assert_eq!(
                reply[offset_of!(ReplyHeader, operation)],
                operation,
                "the deny must echo the request's own operation byte"
            );
        }
    }

    const HOME_TRANSPORT: u128 = 95;
    const HOME_CLIENT: u128 = 1;
    const HOME_SESSION: u64 = 1;

    /// Dispatch `request` on shard 1 from `HOME_TRANSPORT`, bound to a session
    /// of `HOME_CLIENT`, and wait for one reply. Shard 1 forwards a metadata
    /// write that passes the pre-consensus chain to `owner`, the inbox of the
    /// metadata owner shard 0.
    async fn dispatch_on_home_shard(
        bus: &SpyBus,
        owner: shard::TaggedSender,
        request: Message<RequestHeader>,
    ) {
        const USER: u32 = 0;
        let mut shard = test_shard(bus, 0, 1, FIRST_BOOT);
        let (home, _, _) = shard_channel(1, 1, 1);
        shard.id = 1;
        shard.attach_senders(vec![owner, home]);
        let attachment = {
            let mut client_table = shard.plane.metadata().client_table.borrow_mut();
            client_table
                .commit_register(
                    HOME_CLIENT,
                    USER,
                    [0x5a; 32],
                    register_reply(HOME_CLIENT, HOME_SESSION),
                )
                .unwrap();
            client_table
                .attach_session(HOME_CLIENT, HOME_SESSION, USER)
                .unwrap()
        };
        let sessions = Rc::new(RefCell::new(SessionManager::new()));
        sessions.borrow_mut().ensure_connection(
            HOME_TRANSPORT,
            "127.0.0.1:34567".parse().unwrap(),
            ClientTransportKind::Tcp,
        );
        sessions
            .borrow_mut()
            .bind_authenticated_connection(
                HOME_TRANSPORT,
                HOME_CLIENT,
                HOME_SESSION,
                USER,
                attachment,
                HOME_SESSION,
            )
            .unwrap();
        let server_config = Arc::new(ServerConfig::default());

        handle_client_request(
            &Rc::new(shard),
            &sessions,
            &server_config,
            1,
            &Rc::new(ClientDrain::new(&server_config)),
            HOME_TRANSPORT,
            request.into_generic(),
            None,
        )
        .await;
        await_replies(bus, 1).await;
    }

    /// A home shard other than 0 forwards a metadata write to shard 0. A full
    /// inbox there drops the forward, so no verdict comes back. A silent drop
    /// left the client waiting for its whole read timeout.
    #[compio::test]
    async fn given_full_owner_inbox_when_metadata_write_is_forwarded_should_deny_transient_not_committed()
     {
        let bus = SpyBus::default();
        let (owner, _owner_inbox, _owner_replies) = shard_channel(0, 1, 1);
        owner
            .try_send(reply_lane_forward(HOME_TRANSPORT))
            .expect("owner inbox has one slot");
        let body = DeleteUserRequest {
            user_id: WireIdentifier::numeric(1),
        }
        .to_bytes();
        let request = wire_request(Operation::DeleteUser, HOME_CLIENT, HOME_SESSION, 1, &body);

        dispatch_on_home_shard(&bus, owner, request).await;

        let replies = bus.client_replies.borrow();
        assert_eq!(replies.len(), 1, "a lost forward must still answer");
        let (client, reply) = &replies[0];
        assert_eq!(*client, HOME_TRANSPORT);
        assert_eq!(frame_command(reply), Command::Reply as u8);
        assert_eq!(
            reply_status(reply),
            IggyError::TransientNotCommitted.as_code(),
            "only a replay of the same request id is safe"
        );
    }

    /// The group rewrite is the last step of the pre-consensus chain. A join
    /// whose body does not decode must be denied there, before the home
    /// shard forwards it to the metadata owner.
    #[compio::test]
    async fn given_malformed_join_when_dispatching_should_deny_invalid_command_before_consensus() {
        // Shorter than any join encoding, so the decoder fails on length.
        const TRUNCATED_BODY: &[u8] = &[0x01];
        let bus = SpyBus::default();
        let (owner, owner_inbox, _owner_replies) = shard_channel(0, 1, 1);
        let request = wire_request(
            Operation::JoinConsumerGroup,
            HOME_CLIENT,
            HOME_SESSION,
            1,
            TRUNCATED_BODY,
        );

        dispatch_on_home_shard(&bus, owner, request).await;

        let replies = bus.client_replies.borrow();
        assert_eq!(replies.len(), 1, "a malformed join must be answered");
        let (client, reply) = &replies[0];
        assert_eq!(*client, HOME_TRANSPORT);
        assert_eq!(frame_command(reply), Command::Reply as u8);
        assert_eq!(reply_status(reply), IggyError::InvalidCommand.as_code());
        assert!(
            owner_inbox.try_recv().is_err(),
            "a denied join must never reach the metadata owner"
        );
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
            .push_back(non_replicated_request(CLIENT, PING_CODE).into());

        assert!(pop_next_client_request(&queues, CLIENT, |_| true).is_some());
        assert!(
            queues.borrow().is_empty(),
            "the entry must be freed as soon as it drains empty"
        );
        assert!(
            pop_next_client_request(&queues, CLIENT, |_| true).is_none(),
            "a drained client has nothing left to pop"
        );
    }
}
