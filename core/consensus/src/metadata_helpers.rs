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

//! Metadata-plane helpers: register preflight, request dedup vs
//! `ClientTable`, eviction frame builders. Partition plane is
//! at-least-once and does not call into here.

use crate::client_table::{ClientTable, RequestStatus};
use crate::{Consensus, Pipeline, PipelineEntry, VsrConsensus};
use iggy_binary_protocol::{
    EvictionHeader, EvictionReason, HEADER_SIZE, IGGY_PROTOCOL_VERSION, IGGY_PROTOCOL_VERSION_MIN,
};
use iggy_common::IggyError;
use message_bus::MessageBus;
use server_common::iobuf::Frozen;
use server_common::{MESSAGE_ALIGN, Message};
use std::cell::RefCell;

/// What [`request_preflight`] decided, without touching the wire itself.
///
/// The preflight runs on shard 0 (the metadata owner), but the client's
/// transport connection lives on its home shard. Sending a resend/eviction
/// from here would route by the VSR consensus `client_id`, whose top bits are
/// random and carry no home-shard routing -- so it (almost) never reaches the
/// client. Returning the decision instead lets the home shard
/// (`submit_request_in_process` -> `handle_client_request`) emit the frame by
/// transport id, exactly like a fresh commit.
pub enum PreflightOutcome {
    /// New (client, request): dispatch a fresh prepare through consensus.
    Dispatch,
    /// Duplicate retry: resend this cached committed reply (wire bytes).
    Replay(Frozen<MESSAGE_ALIGN>),
    /// Session gone (`NoSession`) or rotated past the retry (`SessionTooLow`):
    /// the client must be told with an eviction frame.
    Evict(EvictionReason),
    /// Transient: the request could not be committed *right now* but a replay of
    /// the same `request_id` is expected to succeed (in-flight prepare,
    /// not-caught-up primary). The caller sends a `TransientNotCommitted` reply
    /// so the client replays immediately instead of waiting out its read-timeout.
    NotReady,
    /// Absorbed with nothing to send: stale/gap retry or a client-bug newer
    /// session. Replaying the same `request_id` cannot help, so stay silent.
    Drop,
    /// Terminal: the request will never be executed and no cached reply
    /// exists, so the caller answers with this `IggyError` code. Distinct from
    /// [`Self::Drop`] in that the client learns immediately instead of waiting
    /// out its read timeout, and from [`Self::NotReady`] in that a replay of
    /// the same `request_id` cannot change the answer.
    Reject(u32),
}

/// Request preflight (metadata only): epoch fence, watermark dedup,
/// in-flight check.
///
/// Pure decision -- emits no frames (see [`PreflightOutcome`]). Callers turn
/// the outcome into a reply: the home-shard path resends by transport id, the
/// message-plane paths fall back to [`apply_preflight_consensus_plane`].
///
/// `session` is the wire `session` field, which carries the entry's fence
/// epoch; `request_checksum` is the request's integrity stamp (zero =
/// unstamped, disables the reuse check).
///
/// ## What the catch-up gate below does and does not establish
///
/// It says this replica has APPLIED everything it holds and its log suffix has
/// re-earned quorum, which is what makes the eviction decisions below safe
/// against a stale in-memory table on a freshly promoted primary.
///
/// It says nothing about how the table was BUILT. Every clause of
/// `is_caught_up_primary` is about the log; recovery, meanwhile, replays from
/// this node's snapshot floor, and checkpointing is node-local, so two replicas
/// reconstruct their tables from different starting points. Epochs survive that
/// (they are op-derived), watermarks do not -- see
/// `metadata::impls::recovery`. So a caught-up primary is authoritative for its
/// own table, not for agreement with its peers' tables. Closing that needs the
/// table in the snapshot, not a stronger gate here.
pub fn request_preflight<B, P>(
    consensus: &VsrConsensus<B, P>,
    client_table: &RefCell<ClientTable>,
    client_id: u128,
    session: u64,
    request: u64,
    request_checksum: u128,
) -> PreflightOutcome
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    // In-flight dedup: a live prepare from this client absorbs the retry.
    // Pump delivers the reply at commit.
    if consensus.pipeline_has_message_from_client(client_id) {
        tracing::debug!(
            client_id,
            request,
            "request_preflight: in-flight prepare, not ready"
        );
        return PreflightOutcome::NotReady;
    }

    // Catch-up gate: stale ClientTable on a new primary could return `New`
    // for a (client, request) already committed in inherited WAL but not yet
    // applied. Dispatching a fresh prepare -> two prepares for the same
    // request -> the second applies the operation twice.
    if !is_caught_up_primary(consensus) {
        tracing::debug!(
            client_id,
            request,
            is_primary = consensus.is_primary(),
            is_normal = consensus.is_normal(),
            is_transferring = consensus.is_transferring(),
            commit_min = consensus.commit_min(),
            commit_max = consensus.commit_max(),
            "request_preflight: not caught up, not ready"
        );
        return PreflightOutcome::NotReady;
    }

    let status = client_table
        .borrow()
        .check_request(client_id, session, request, request_checksum);
    match status {
        // Frozen-backed cache -> refcount handoff to the home shard, no copy.
        RequestStatus::Duplicate(cached_reply) => {
            PreflightOutcome::Replay(cached_reply.into_wire_bytes())
        }
        // Session evicted under capacity pressure. The catch-up gate makes this
        // replica authoritative for its own committed session state, which is
        // what an eviction frame reports.
        RequestStatus::NoSession => PreflightOutcome::Evict(EvictionReason::NoSession),
        // Zombie holdover from before a re-register: terminal for that holder.
        // Sound on any caught-up replica because the fence is op-derived, so
        // every replica that applied this register holds the same value.
        RequestStatus::Fenced { current, received } => {
            tracing::debug!(
                client_id,
                current,
                received,
                "request_preflight: fencing stale-epoch request"
            );
            PreflightOutcome::Evict(EvictionReason::SessionTooLow)
        }
        // Catch-up gate rules out network race; an epoch newer than any this
        // table minted = client bug. Error log, no eviction (transient bug
        // must not kill the session), no rate limit (per-event).
        RequestStatus::EpochAhead { current, received } => {
            tracing::error!(
                client_id,
                current,
                received,
                "request_preflight: ignoring future epoch (client bug)"
            );
            PreflightOutcome::Drop
        }
        // Same request id, different request bytes: replaying the cached
        // reply would answer the wrong request, and re-executing would
        // double-apply. Loud drop; the client must fix its numbering.
        RequestStatus::ChecksumMismatch { request } => {
            tracing::error!(
                client_id,
                request,
                "request_preflight: request id reused for a different operation (client bug)"
            );
            PreflightOutcome::Drop
        }
        // Applied once, original reply aged out of the ring: refuse
        // re-execution, and say so. Re-executing would double-apply and no
        // cached reply survives, so the honest answer is a terminal code
        // rather than silence that costs the client a read timeout. A run of
        // these means the reply ring is too small for the client's retry
        // latency.
        RequestStatus::AlreadyApplied { request, watermark } => {
            tracing::warn!(
                client_id,
                request,
                watermark,
                "request_preflight: duplicate whose reply aged out of the ring, refusing"
            );
            PreflightOutcome::Reject(IggyError::RequestAlreadyApplied.as_code())
        }
        RequestStatus::New => PreflightOutcome::Dispatch,
    }
}

/// Message-plane fallback for [`request_preflight`]: best-effort resend by the
/// consensus `client_id`.
///
/// The wire-path ingress (`on_request`) and the queued retry drain have no
/// home-shard transport context to route through, so they apply the outcome
/// here. Returns `true` iff the caller should dispatch a fresh prepare.
///
/// Delivery is best-effort: a VSR consensus id's top bits are random, so
/// `send_to_client` may not reach the client. The in-process client path
/// (`submit_request_in_process`) instead carries the outcome back to the home
/// shard and resends by transport id.
#[allow(clippy::future_not_send)]
pub async fn apply_preflight_consensus_plane<B, P>(
    consensus: &VsrConsensus<B, P>,
    outcome: PreflightOutcome,
    client_id: u128,
) -> bool
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    match outcome {
        PreflightOutcome::Dispatch => true,
        PreflightOutcome::Replay(reply) => {
            let _ = consensus
                .message_bus()
                .send_to_client(client_id, reply)
                .await;
            false
        }
        PreflightOutcome::Evict(reason) => {
            send_eviction_to_client(consensus, client_id, reason).await;
            false
        }
        // The wire-ingress plane has no per-request transport context to build a
        // correlated `TransientNotCommitted` reply (that lives on the in-process
        // home-shard path); stay silent here as before. NotReady and Drop both
        // mean "do not dispatch"; the difference (explicit retry frame) only
        // applies where the request header is in scope.
        // `Reject` needs the request header to build a correlated reply, which
        // only the home-shard path holds; it degrades to silence here for the
        // same reason NotReady does.
        PreflightOutcome::NotReady | PreflightOutcome::Drop | PreflightOutcome::Reject(_) => false,
    }
}

/// Register preflight: dedup for `Register`.
///
/// # Returns
/// `true` -> dispatch. `false` -> absorbed (`AlreadyRegistered` replays cache;
/// in-flight register silently dropped).
#[allow(clippy::future_not_send)]
pub fn register_preflight<B, P>(
    consensus: &VsrConsensus<B, P>,
    client_table: &RefCell<ClientTable>,
    client_id: u128,
    user_id: u32,
) -> bool
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    // In-flight dedup.
    if consensus.pipeline_has_message_from_client(client_id) {
        tracing::debug!(client_id, "register_preflight: in-flight prepare, drop");
        return false;
    }

    // Catch-up gate: new primary may have inherited Register(client, op=N)
    // committed in WAL but not yet applied. Without the gate this dispatches a
    // second register, which commits at a later op -> the entry's fence moves
    // past the one the first register's reply handed the client, fencing a live
    // client for no reason. SDK retry recovers post-catch-up.
    if !is_caught_up_primary(consensus) {
        tracing::debug!(
            client_id,
            is_primary = consensus.is_primary(),
            is_normal = consensus.is_normal(),
            is_transferring = consensus.is_transferring(),
            commit_min = consensus.commit_min(),
            commit_max = consensus.commit_max(),
            "register_preflight: not caught up, drop"
        );
        return false;
    }

    // OWNERSHIP GATE. `commit_register`'s rebind branch overwrites the entry's
    // `user_id`, and `resolve_acting_user_id` resolves authority for every
    // replicated op from that field, so admitting a register for an entry
    // another user owns would hand the caller that user's authority. Refuse by
    // dropping it: this runs on the wire ingress and on the promotion of a
    // queued register, neither of which has a caller to return a typed error
    // to. The in-process submit checks the same condition first and does
    // return one (`ClientIdOwnedByAnotherUser`), so a client that reaches here
    // and is dropped retries into that terminal answer.
    //
    // Correct to decide here because the catch-up gate above already
    // established this replica as authoritative for session truth.
    if let Some(owner) = client_table.borrow().get_user_id(client_id)
        && owner != user_id
    {
        tracing::warn!(
            client_id,
            presented_user = user_id,
            entry_owner = owner,
            "register_preflight: dropping register for an entry owned by another user"
        );
        return false;
    }

    // Past the gates, every Register dispatches -- including one whose client
    // already holds an entry. A bind is a fencing event: `commit_register`'s
    // rebind branch bumps the entry's epoch, which is what fences the previous
    // holder of this session (the design's zombie fencing). Absorbing a
    // re-register here would return the old epoch un-bumped and leave two live
    // holders sharing a fence. The cost is that a stale Register retransmit
    // also commits and fences the live holder, who recovers with one
    // re-register round trip (the rebind branch preserves the watermark, so
    // no dedup history is lost). Stream transports do not duplicate frames
    // within a connection, and the in-flight scan above absorbs concurrent
    // duplicates, so that path is foreign-client-only.
    true
}

/// Stamping context for [`EvictionHeader`]. Filled once from
/// `VsrConsensus`, passed down without growing helper signatures.
#[derive(Debug, Clone, Copy)]
pub struct EvictionContext {
    pub cluster: u128,
    pub view: u32,
    pub replica: u8,
}

impl EvictionContext {
    #[must_use]
    pub const fn from_consensus<B, P>(consensus: &VsrConsensus<B, P>) -> Self
    where
        B: MessageBus,
        P: Pipeline<Entry = PipelineEntry>,
    {
        Self {
            cluster: consensus.cluster(),
            view: consensus.view(),
            replica: consensus.replica(),
        }
    }
}

/// Primary -> client `Eviction` frame. Session-level, no per-request correlation.
/// Typed reason on the wire so SDKs trigger their callback without string parsing.
///
/// # Panics
/// Unreachable: zeroed `HEADER_SIZE` buffer is always a valid `EvictionHeader`.
#[must_use]
#[allow(clippy::cast_possible_truncation)]
pub fn build_eviction_message(
    ctx: EvictionContext,
    client_id: u128,
    reason: EvictionReason,
) -> Message<EvictionHeader> {
    debug_assert!(
        client_id != 0,
        "build_eviction_message: client_id != 0 (header validation rejects 0)"
    );
    debug_assert!(
        reason != EvictionReason::Reserved,
        "build_eviction_message: Reserved is sentinel; pick a real variant"
    );
    build_eviction_from_header(EvictionHeader::new(
        ctx.cluster,
        ctx.view,
        ctx.replica,
        client_id,
        reason,
    ))
}

/// `IncompatibleProtocol` eviction carrying the server's accepted protocol
/// window, see [`EvictionHeader::incompatible_protocol`].
///
/// # Panics
/// Unreachable: zeroed `HEADER_SIZE` buffer is always a valid `EvictionHeader`.
#[must_use]
pub fn build_incompatible_protocol_eviction_message(
    ctx: EvictionContext,
    client_id: u128,
) -> Message<EvictionHeader> {
    debug_assert!(
        client_id != 0,
        "build_incompatible_protocol_eviction_message: client_id != 0"
    );
    build_eviction_from_header(EvictionHeader::incompatible_protocol(
        ctx.cluster,
        ctx.view,
        ctx.replica,
        client_id,
        IGGY_PROTOCOL_VERSION,
        IGGY_PROTOCOL_VERSION_MIN,
    ))
}

/// # Panics
/// Unreachable: zeroed `HEADER_SIZE` buffer is always a valid `EvictionHeader`.
fn build_eviction_from_header(header: EvictionHeader) -> Message<EvictionHeader> {
    let mut msg = Message::<EvictionHeader>::new(HEADER_SIZE);
    let slot = bytemuck::checked::try_from_bytes_mut::<EvictionHeader>(
        &mut msg.as_mut_slice()[..HEADER_SIZE],
    )
    .expect("zeroed bytes are valid");
    *slot = header;
    msg
}

/// True iff primary, Normal, not syncing, and `commit_min == commit_max`.
/// Safe to dispatch new prepares and emit eviction signals.
///
/// # Safety
///
/// - **Eviction**: partitioned-then-promoted primary emitting
///   `NoSession`/`SessionTooLow` against stale table erases live clients.
/// - **Dispatch**: primary with `commit_min < commit_max` may hold an
///   inherited `Register(client, op=N)` in WAL but not yet applied.
///   Admitting a fresh Register commits a second register, bumping the
///   epoch past the one the inherited register's reply handed the client
///   and fencing a live client for no reason.
///
/// `false` -> caller silent-drops; client retry lands on peer or here
/// post-catch-up.
///
/// `is_transferring` is real (state transfer replaces snapshot-shaped state
/// on a cluster-restart rejoin), but safety still rests on
/// `commit_min == commit_max` -- a transferring replica also fails that. Do
/// not weaken commit-equality.
pub fn is_caught_up_primary<B, P>(consensus: &VsrConsensus<B, P>) -> bool
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    consensus.is_primary()
        && !consensus.has_ceded_primaryship()
        && consensus.is_normal()
        && !consensus.is_transferring()
        && consensus.commit_min() == consensus.commit_max()
        // Recovery re-pipelines the WAL's prepared-but-uncommitted suffix;
        // those ops were acked to clients before the restart, so admitting
        // new requests (a login is a Register write) before the suffix
        // re-commits would serve state that rolls back committed history.
        // Held transient until the retransmit path re-earns quorum.
        && consensus.commit_max() >= consensus.recovery_barrier()
}

/// Build + best-effort send `Eviction`. `SendError` dropped: eviction is
/// terminal one-way; gone connection has nothing to recover.
#[allow(clippy::future_not_send)]
pub async fn send_eviction_to_client<B, P>(
    consensus: &VsrConsensus<B, P>,
    client_id: u128,
    reason: EvictionReason,
) where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    let ctx = EvictionContext::from_consensus(consensus);
    let msg = build_eviction_message(ctx, client_id, reason);
    let _ = consensus
        .message_bus()
        .send_to_client(client_id, msg.into_generic().into_frozen())
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_table::REGISTER_REQUEST_ID;
    use crate::{CLIENTS_TABLE_MAX, LocalPipeline};
    use iggy_binary_protocol::{Command2, Operation, ReplyHeader};
    use message_bus::SendError;

    /// Acting user for register fixtures; these tests exercise preflight /
    /// replay, not user resolution, so the exact value is immaterial.
    const ACTING_USER_ID: u32 = 1;

    /// Production-sized `ClientTable`.
    fn fresh_client_table() -> RefCell<ClientTable> {
        RefCell::new(ClientTable::new(CLIENTS_TABLE_MAX))
    }

    /// Records `send_to_client` for assertion.
    struct ClientSpyBus {
        client_sends: std::cell::RefCell<Vec<(u128, Frozen<MESSAGE_ALIGN>)>>,
    }

    impl ClientSpyBus {
        fn new() -> Self {
            Self {
                client_sends: std::cell::RefCell::new(Vec::new()),
            }
        }
    }

    #[allow(clippy::future_not_send)]
    impl MessageBus for ClientSpyBus {
        fn track_background(&self, _handle: message_bus::JoinHandle<()>) {}

        async fn send_to_client(
            &self,
            client_id: u128,
            data: Frozen<MESSAGE_ALIGN>,
        ) -> Result<(), SendError> {
            self.client_sends.borrow_mut().push((client_id, data));
            Ok(())
        }

        async fn send_to_replica(
            &self,
            _replica: u8,
            _data: Frozen<MESSAGE_ALIGN>,
        ) -> Result<(), SendError> {
            Ok(())
        }

        fn set_connection_lost_fn(&self, _f: message_bus::ConnectionLostFn) {}
        fn set_replica_forward_fn(&self, _f: message_bus::ReplicaForwardFn) {}
        fn set_client_forward_fn(&self, _f: message_bus::ClientForwardFn) {}
    }

    // A registered client's fresh Register dispatches instead of being
    // absorbed: a bind is a fencing event and only a committed Register bumps
    // the entry's epoch. Absorbing here would leave two live holders sharing
    // one fence (the inert-fence bug).
    #[test]
    fn register_preflight_dispatches_rebind_for_registered_client() {
        let consensus = VsrConsensus::new(1, 0, 3, 0, ClientSpyBus::new(), LocalPipeline::new());
        consensus.init();
        let client_table = fresh_client_table();

        let client_id: u128 = 0xBEEF;
        let initial_reply = synthesize_register_reply(&consensus, client_id, 17);
        client_table
            .borrow_mut()
            .commit_register(client_id, ACTING_USER_ID, initial_reply);
        // Progress past registration; a rebind must dispatch regardless.
        let app_reply = synthesize_send_messages_reply(&consensus, client_id, 1, 18);
        client_table.borrow_mut().commit_reply(client_id, app_reply);

        assert!(
            register_preflight(&consensus, &client_table, client_id, ACTING_USER_ID),
            "rebind must dispatch so commit_register bumps the fence epoch"
        );
        let sends = consensus.message_bus().client_sends.borrow();
        assert!(sends.is_empty(), "preflight itself sends nothing");
    }

    // No-session: non-Register from unknown client -> Eviction(NoSession).
    #[test]
    fn request_preflight_no_session_evicts_client() {
        let consensus = VsrConsensus::new(1, 0, 3, 0, ClientSpyBus::new(), LocalPipeline::new());
        consensus.init();
        let client_table = fresh_client_table();

        let client_id: u128 = 0xCAFE;

        let result = futures::executor::block_on(apply_preflight_consensus_plane(
            &consensus,
            request_preflight(
                &consensus,
                &client_table,
                client_id,
                10, // epoch (wire session field)
                1,  // request
                0,  // request_checksum (unstamped)
            ),
            client_id,
        ));
        assert!(!result, "NoSession short-circuits");

        let sends = consensus.message_bus().client_sends.borrow();
        assert_eq!(sends.len(), 1, "one Eviction");
        assert_eq!(sends[0].0, client_id);

        let frozen = &sends[0].1;
        let header =
            bytemuck::checked::try_from_bytes::<EvictionHeader>(&frozen.as_slice()[..HEADER_SIZE])
                .expect("valid EvictionHeader");
        assert_eq!(header.command, Command2::Eviction);
        assert_eq!(header.reason, EvictionReason::NoSession);
        assert_eq!(header.client, client_id);
    }

    // Zombie fencing: a request stamped with a pre-rebind epoch gets a
    // terminal SessionTooLow eviction.
    #[test]
    fn request_preflight_stale_epoch_evicts_client() {
        let consensus = VsrConsensus::new(1, 0, 3, 0, ClientSpyBus::new(), LocalPipeline::new());
        consensus.init();
        let client_table = fresh_client_table();

        let client_id: u128 = 0xBEEF;

        // Register, then rebind: entry epoch is now 2.
        let initial_reply = synthesize_register_reply(&consensus, client_id, 17);
        client_table
            .borrow_mut()
            .commit_register(client_id, ACTING_USER_ID, initial_reply);
        let rebind_reply = synthesize_register_reply(&consensus, client_id, 25);
        client_table
            .borrow_mut()
            .commit_register(client_id, ACTING_USER_ID, rebind_reply);

        // Zombie still stamping epoch 1: fenced.
        let result = futures::executor::block_on(apply_preflight_consensus_plane(
            &consensus,
            request_preflight(&consensus, &client_table, client_id, 1, 1, 0),
            client_id,
        ));
        assert!(!result, "Fenced short-circuits");

        let sends = consensus.message_bus().client_sends.borrow();
        assert_eq!(sends.len(), 1, "one Eviction");

        let frozen = &sends[0].1;
        let header =
            bytemuck::checked::try_from_bytes::<EvictionHeader>(&frozen.as_slice()[..HEADER_SIZE])
                .expect("valid EvictionHeader");
        assert_eq!(header.reason, EvictionReason::SessionTooLow);
        assert_eq!(header.client, client_id);
    }

    // Newer-than-minted epoch: epochs are only handed out by register
    // replies; healthy SDK can't reach this. Client bug -> silent drop, no
    // eviction.
    #[test]
    fn request_preflight_future_epoch_is_silent_drop() {
        let consensus = VsrConsensus::new(1, 0, 3, 0, ClientSpyBus::new(), LocalPipeline::new());
        consensus.init();
        let client_table = fresh_client_table();

        let client_id: u128 = 0xBEEF;

        // Entry at epoch 1.
        let initial_reply = synthesize_register_reply(&consensus, client_id, 17);
        client_table
            .borrow_mut()
            .commit_register(client_id, ACTING_USER_ID, initial_reply);

        // Client claims epoch 99 (> 1), client bug.
        let result = futures::executor::block_on(apply_preflight_consensus_plane(
            &consensus,
            request_preflight(&consensus, &client_table, client_id, 99, 1, 0),
            client_id,
        ));
        assert!(!result, "EpochAhead short-circuits");

        let sends = consensus.message_bus().client_sends.borrow();
        assert!(sends.is_empty(), "future epoch must be silent drop");
    }

    // Backups never send NoSession: their ClientTable lags. Without gate,
    // partitioned-then-promoted backup would erase live sessions.
    #[test]
    fn request_preflight_no_session_silently_drops_on_backup() {
        // Replica 1, view 0, 3 replicas: primary=0 -> this is backup.
        let consensus = VsrConsensus::new(1, 1, 3, 0, ClientSpyBus::new(), LocalPipeline::new());
        consensus.init();
        let client_table = fresh_client_table();
        assert!(!consensus.is_primary(), "test setup: backup");

        let client_id: u128 = 0xCAFE;

        let result = futures::executor::block_on(apply_preflight_consensus_plane(
            &consensus,
            request_preflight(
                &consensus,
                &client_table,
                client_id,
                10, // epoch (wire session field)
                1,  // request
                0,  // request_checksum (unstamped)
            ),
            client_id,
        ));
        assert!(!result, "NoSession short-circuits");

        let sends = consensus.message_bus().client_sends.borrow();
        assert!(
            sends.is_empty(),
            "backup must NOT evict, primary may hold live session"
        );
    }

    // Below-watermark retry whose reply is still cached: replayed, not
    // re-executed and not dropped.
    #[test]
    fn request_preflight_below_watermark_replays_ring_hit() {
        let consensus = VsrConsensus::new(1, 0, 3, 0, ClientSpyBus::new(), LocalPipeline::new());
        consensus.init();
        let client_table = fresh_client_table();

        let client_id: u128 = 0xABCD;
        // Epoch = the register's commit op.
        let epoch: u64 = 5;

        let initial_reply = synthesize_register_reply(&consensus, client_id, 5);
        client_table
            .borrow_mut()
            .commit_register(client_id, ACTING_USER_ID, initial_reply);
        for (request, commit) in [(3u64, 98u64), (5, 100)] {
            let reply = synthesize_send_messages_reply(&consensus, client_id, request, commit);
            client_table.borrow_mut().commit_reply(client_id, reply);
        }

        let result = futures::executor::block_on(apply_preflight_consensus_plane(
            &consensus,
            request_preflight(&consensus, &client_table, client_id, epoch, 3, 0),
            client_id,
        ));
        assert!(!result, "duplicate short-circuits");

        let sends = consensus.message_bus().client_sends.borrow();
        assert_eq!(sends.len(), 1, "ring hit replays the original reply");
        let header =
            bytemuck::checked::try_from_bytes::<ReplyHeader>(&sends[0].1.as_slice()[..HEADER_SIZE])
                .expect("valid ReplyHeader");
        assert_eq!(header.request, 3, "original reply for the retried request");
    }

    // A duplicate whose reply aged out of the ring must be refused with a
    // terminal code, not silently dropped: re-executing would double-apply,
    // and silence costs the client its read timeout with nothing learned.
    #[test]
    fn request_preflight_aged_out_duplicate_is_terminally_refused() {
        let consensus = VsrConsensus::new(1, 0, 3, 0, ClientSpyBus::new(), LocalPipeline::new());
        consensus.init();
        let client_table = fresh_client_table();

        let client_id: u128 = 0xABCD;
        let epoch: u64 = 5;
        let initial_reply = synthesize_register_reply(&consensus, client_id, 5);
        client_table
            .borrow_mut()
            .commit_register(client_id, ACTING_USER_ID, initial_reply);
        // Ring capacity is 5, so request 1's reply is displaced once 6 commits.
        for request in 1..=6u64 {
            let reply =
                synthesize_send_messages_reply(&consensus, client_id, request, 100 + request);
            client_table.borrow_mut().commit_reply(client_id, reply);
        }

        let outcome = request_preflight(&consensus, &client_table, client_id, epoch, 1, 0);
        assert!(
            matches!(
                outcome,
                PreflightOutcome::Reject(code) if code == IggyError::RequestAlreadyApplied.as_code()
            ),
            "expected a terminal RequestAlreadyApplied refusal"
        );
    }

    // The promotion path and the wire ingress both land here, and neither has
    // a caller to return a typed error to, so an entry owned by another user is
    // refused by dropping the register. Without this a register queued while
    // the primary was catching up would commit at promotion and
    // `commit_register` would overwrite the entry's `user_id`, handing the
    // presenter that user's authority.
    #[test]
    fn register_preflight_drops_a_register_for_another_users_entry() {
        const OWNER: u32 = ACTING_USER_ID;
        const IMPOSTOR: u32 = ACTING_USER_ID + 1;

        let consensus = VsrConsensus::new(1, 0, 3, 0, ClientSpyBus::new(), LocalPipeline::new());
        consensus.init();
        let client_table = fresh_client_table();
        let client_id: u128 = 0xBEEF;
        let initial_reply = synthesize_register_reply(&consensus, client_id, 17);
        client_table
            .borrow_mut()
            .commit_register(client_id, OWNER, initial_reply);

        assert!(
            !register_preflight(&consensus, &client_table, client_id, IMPOSTOR),
            "a register for another user's entry must not dispatch"
        );
        assert!(
            register_preflight(&consensus, &client_table, client_id, OWNER),
            "the owner's own rebind must still dispatch"
        );
        assert_eq!(
            client_table.borrow().get_user_id(client_id),
            Some(OWNER),
            "the refused register must not have touched the entry"
        );
    }

    // Watermark jump: request numbers above the watermark dispatch even
    // when non-contiguous (there is no RequestGap).
    #[test]
    fn request_preflight_jump_above_watermark_dispatches() {
        let consensus = VsrConsensus::new(1, 0, 3, 0, ClientSpyBus::new(), LocalPipeline::new());
        consensus.init();
        let client_table = fresh_client_table();

        let client_id: u128 = 0xABCD;
        // Epoch = the register's commit op.
        let epoch: u64 = 5;

        let initial_reply = synthesize_register_reply(&consensus, client_id, 5);
        client_table
            .borrow_mut()
            .commit_register(client_id, ACTING_USER_ID, initial_reply);
        let advanced = synthesize_send_messages_reply(&consensus, client_id, 2, 99);
        client_table.borrow_mut().commit_reply(client_id, advanced);

        let outcome = request_preflight(&consensus, &client_table, client_id, epoch, 9, 0);
        assert!(
            matches!(outcome, PreflightOutcome::Dispatch),
            "watermark jump must dispatch"
        );
    }

    // Catch-up gate prevents the WAL-replay race in commit_register: primary
    // with commit_min < commit_max may hold an inherited Register(client) in
    // WAL not yet applied. A fresh Register would commit a second register
    // and bump the epoch past the inherited reply's, fencing a live client.
    #[test]
    fn register_preflight_silently_drops_when_behind_on_commits() {
        let consensus = VsrConsensus::new(1, 0, 3, 0, ClientSpyBus::new(), LocalPipeline::new());
        consensus.init();
        assert!(consensus.is_primary(), "test setup: primary");

        // commit_min=0, commit_max=5: behind on local execution.
        consensus.advance_commit_max(5);
        assert_ne!(consensus.commit_min(), consensus.commit_max());

        let client_table = fresh_client_table();
        let client_id: u128 = 0xC0DE;

        let result = register_preflight(&consensus, &client_table, client_id, ACTING_USER_ID);
        assert!(!result, "register dispatch must short-circuit");

        let sends = consensus.message_bus().client_sends.borrow();
        assert!(sends.is_empty(), "silent drop until catch-up");
    }

    // Direct gate test: primary, normal, commit_min == commit_max, and not
    // mid-state-transfer.
    #[test]
    fn is_caught_up_primary_gate_states() {
        // Primary, normal, equal commits -> true.
        let primary = VsrConsensus::new(1, 0, 3, 0, ClientSpyBus::new(), LocalPipeline::new());
        primary.init();
        assert!(primary.is_primary());
        assert!(primary.is_normal());
        assert!(!primary.is_transferring());
        assert_eq!(primary.commit_min(), primary.commit_max());
        assert!(is_caught_up_primary(&primary));

        // Mid-transfer -> false, even with equal commits.
        primary.begin_state_transfer_await();
        assert!(primary.is_transferring());
        assert!(!is_caught_up_primary(&primary));
        primary.set_state_transfer_stage(crate::StateTransferStage::Idle);
        assert!(is_caught_up_primary(&primary));

        // commit_min < commit_max -> false.
        primary.advance_commit_max(5);
        assert_ne!(primary.commit_min(), primary.commit_max());
        assert!(!is_caught_up_primary(&primary));

        // Backup -> false.
        let backup = VsrConsensus::new(1, 1, 3, 0, ClientSpyBus::new(), LocalPipeline::new());
        backup.init();
        assert!(!backup.is_primary());
        assert!(!is_caught_up_primary(&backup));
    }

    #[test]
    fn register_preflight_new_client_does_not_send_reply() {
        let consensus = VsrConsensus::new(1, 0, 3, 0, ClientSpyBus::new(), LocalPipeline::new());
        consensus.init();

        let client_table = fresh_client_table();
        let client_id: u128 = 0xC0DE;

        let result = register_preflight(&consensus, &client_table, client_id, ACTING_USER_ID);
        assert!(result, "New client proceeds through consensus");

        let sends = consensus.message_bus().client_sends.borrow();
        assert!(sends.is_empty(), "no reply for New client");
    }

    // Fixture: register reply mirroring `commit_register` storage.
    // `register_commit` stamps the reply's op/commit (recency for eviction
    // ordering); the entry's epoch is minted by the table, not read from it.
    #[allow(clippy::cast_possible_truncation)]
    fn synthesize_register_reply<B, P>(
        consensus: &VsrConsensus<B, P>,
        client_id: u128,
        register_commit: u64,
    ) -> Message<ReplyHeader>
    where
        B: MessageBus,
        P: Pipeline<Entry = PipelineEntry>,
    {
        let header_size = std::mem::size_of::<ReplyHeader>();
        let mut msg = Message::<ReplyHeader>::new(header_size);
        let header = bytemuck::checked::try_from_bytes_mut::<ReplyHeader>(
            &mut msg.as_mut_slice()[..header_size],
        )
        .expect("zeroed bytes are valid");
        *header = ReplyHeader {
            cluster: consensus.cluster(),
            size: header_size as u32,
            view: consensus.view(),
            command: Command2::Reply,
            replica: consensus.replica(),
            client: client_id,
            op: register_commit,
            commit: register_commit,
            request: REGISTER_REQUEST_ID,
            operation: Operation::Register,
            ..ReplyHeader::default()
        };
        msg
    }

    // SendMessages reply fixture: advances the cached watermark.
    #[allow(clippy::cast_possible_truncation)]
    fn synthesize_send_messages_reply<B, P>(
        consensus: &VsrConsensus<B, P>,
        client_id: u128,
        request: u64,
        commit: u64,
    ) -> Message<ReplyHeader>
    where
        B: MessageBus,
        P: Pipeline<Entry = PipelineEntry>,
    {
        let header_size = std::mem::size_of::<ReplyHeader>();
        let mut msg = Message::<ReplyHeader>::new(header_size);
        let header = bytemuck::checked::try_from_bytes_mut::<ReplyHeader>(
            &mut msg.as_mut_slice()[..header_size],
        )
        .expect("zeroed bytes are valid");
        *header = ReplyHeader {
            cluster: consensus.cluster(),
            size: header_size as u32,
            view: consensus.view(),
            command: Command2::Reply,
            replica: consensus.replica(),
            client: client_id,
            op: commit,
            commit,
            request,
            operation: Operation::SendMessages,
            ..ReplyHeader::default()
        };
        msg
    }
}
