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

use crate::client_table::{ClientTable, REGISTER_REQUEST_ID, RequestStatus};
use crate::{Consensus, Pipeline, PipelineEntry, VsrConsensus};
use iggy_binary_protocol::{
    EvictionHeader, EvictionReason, HEADER_SIZE, IGGY_PROTOCOL_VERSION, IGGY_PROTOCOL_VERSION_MIN,
};
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
}

/// Request preflight (metadata only): session validation, dedup, in-flight check.
///
/// Pure decision -- emits no frames (see [`PreflightOutcome`]). Callers turn
/// the outcome into a reply: the home-shard path resends by transport id, the
/// message-plane paths fall back to [`apply_preflight_consensus_plane`].
pub fn request_preflight<B, P>(
    consensus: &VsrConsensus<B, P>,
    client_table: &RefCell<ClientTable>,
    client_id: u128,
    session: u64,
    request: u64,
) -> PreflightOutcome
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    // In-flight dedup: a live prepare from this client absorbs the retry.
    // Pump delivers the reply at commit.
    if consensus
        .pipeline()
        .borrow()
        .has_message_from_client(client_id)
    {
        tracing::debug!(
            client_id,
            request,
            "request_preflight: in-flight prepare, not ready"
        );
        return PreflightOutcome::NotReady;
    }

    // Catch-up gate: stale ClientTable on a new primary could return `New`
    // for a (client, request) already committed in inherited WAL but not yet
    // applied. Dispatching a fresh prepare -> two prepares for same op ->
    // commit_reply's regression assert panics.
    if !is_caught_up_primary(consensus) {
        tracing::debug!(
            client_id,
            request,
            is_primary = consensus.is_primary(),
            is_normal = consensus.is_normal(),
            is_syncing = consensus.is_syncing(),
            commit_min = consensus.commit_min(),
            commit_max = consensus.commit_max(),
            "request_preflight: not caught up, not ready"
        );
        return PreflightOutcome::NotReady;
    }

    let status = client_table
        .borrow()
        .check_request(client_id, session, request);
    match status {
        // Frozen-backed cache -> refcount handoff to the home shard, no copy.
        RequestStatus::Duplicate(cached_reply) => {
            PreflightOutcome::Replay(cached_reply.into_wire_bytes())
        }
        // Session evicted under capacity pressure. SAFETY: catch-up gate makes
        // this replica authoritative for session truth.
        RequestStatus::NoSession => PreflightOutcome::Evict(EvictionReason::NoSession),
        RequestStatus::SessionMismatch { expected, received } => {
            // expected > received: stale session (rotated post-eviction) -> terminal eviction.
            // expected < received: client bug; silent drop, log.
            // SAFETY: catch-up gate makes this replica authoritative.
            if expected > received {
                PreflightOutcome::Evict(EvictionReason::SessionTooLow)
            } else {
                // Catch-up gate rules out network race; newer-than-issued
                // session = client bug. Error log, no eviction (transient bug
                // must not kill session), no rate limit (per-event).
                tracing::error!(
                    client_id,
                    expected,
                    received,
                    "request_preflight: ignoring newer session (client bug)"
                );
                PreflightOutcome::Drop
            }
        }
        // Client bug; recovered by client retry. Silent drop.
        RequestStatus::Stale
        | RequestStatus::RequestGap { .. }
        | RequestStatus::AlreadyRegistered { .. } => PreflightOutcome::Drop,
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
        PreflightOutcome::NotReady | PreflightOutcome::Drop => false,
    }
}

/// Register preflight: dedup for `Register`.
///
/// # Returns
/// `true` -> dispatch. `false` -> absorbed (`AlreadyRegistered` replays cache;
/// in-flight register silently dropped).
#[allow(clippy::future_not_send)]
pub async fn register_preflight<B, P>(
    consensus: &VsrConsensus<B, P>,
    client_table: &RefCell<ClientTable>,
    client_id: u128,
) -> bool
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    // In-flight dedup.
    if consensus
        .pipeline()
        .borrow()
        .has_message_from_client(client_id)
    {
        tracing::debug!(client_id, "register_preflight: in-flight prepare, drop");
        return false;
    }

    // Catch-up gate: new primary may have inherited Register(client, op=N)
    // committed in WAL but not yet applied. Without gate, check_register
    // returns New -> fresh register -> two register entries -> commit_register's
    // session-equality assert panics on replay. SDK retry recovers post-catch-up.
    if !is_caught_up_primary(consensus) {
        tracing::debug!(
            client_id,
            is_primary = consensus.is_primary(),
            is_normal = consensus.is_normal(),
            is_syncing = consensus.is_syncing(),
            commit_min = consensus.commit_min(),
            commit_max = consensus.commit_max(),
            "register_preflight: not caught up, drop"
        );
        return false;
    }

    let status = client_table.borrow().check_register(client_id);
    match status {
        RequestStatus::AlreadyRegistered {
            session,
            cached_reply,
        } => {
            // cached.request == REGISTER_REQUEST_ID: replay cached bytes
            // (preserves original primary's checksum/view/replica, no synthesis).
            // cached.request > REGISTER_REQUEST_ID: client committed past register;
            // retry is older than latest committed -> silent drop. Genuinely lost
            // sessions surface as NoSession on next non-register request.
            if cached_reply.header().request == REGISTER_REQUEST_ID {
                // Frozen handoff: refcount only.
                let _ = consensus
                    .message_bus()
                    .send_to_client(client_id, cached_reply.into_wire_bytes())
                    .await;
                tracing::debug!(
                    client_id,
                    session,
                    "register_preflight: replayed cached register reply"
                );
            } else {
                tracing::debug!(
                    client_id,
                    session,
                    cached_request = cached_reply.header().request,
                    "register_preflight: retry past register, drop"
                );
            }
            false
        }
        RequestStatus::New => true,
        // check_register only returns AlreadyRegistered or New (in-flight
        // filtered upstream by pipeline scan).
        other => {
            tracing::warn!(client_id, ?other, "register_preflight: unexpected status");
            false
        }
    }
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
///   Fresh `Register(client, op=M>N)` panics `commit_register`'s
///   session-equality assert on replay.
///
/// `false` -> caller silent-drops; client retry lands on peer or here
/// post-catch-up.
///
/// `is_syncing` is currently hardcoded `false` (inert clause); safety rests
/// on `commit_min == commit_max`, correlated (a syncing replica also has
/// `commit_min < commit_max`). Do not weaken commit-equality.
pub fn is_caught_up_primary<B, P>(consensus: &VsrConsensus<B, P>) -> bool
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    consensus.is_primary()
        && !consensus.has_ceded_primaryship()
        && consensus.is_normal()
        && !consensus.is_syncing()
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

    // Register-retry replay: cached IS the register reply; bytes replayed
    // verbatim (preserves original primary's checksum/view/replica) so SDK
    // can recover from TCP reset / process restart.
    #[test]
    fn register_preflight_already_registered_replays_cached_reply() {
        let consensus = VsrConsensus::new(1, 0, 3, 0, ClientSpyBus::new(), LocalPipeline::new());
        consensus.init();
        let client_table = fresh_client_table();

        let client_id: u128 = 0xBEEF;
        let session: u64 = 17;

        // Cached reply IS the register reply (request == REGISTER_REQUEST_ID).
        let initial_reply = synthesize_register_reply(&consensus, client_id, session);
        let original_checksum = initial_reply.header().checksum;
        client_table
            .borrow_mut()
            .commit_register(client_id, ACTING_USER_ID, initial_reply, |_| false);

        let result =
            futures::executor::block_on(register_preflight(&consensus, &client_table, client_id));
        assert!(!result, "AlreadyRegistered short-circuits");

        let sends = consensus.message_bus().client_sends.borrow();
        assert_eq!(sends.len(), 1, "cached reply replayed");
        assert_eq!(sends[0].0, client_id);

        let frozen = &sends[0].1;
        let header =
            bytemuck::checked::try_from_bytes::<ReplyHeader>(&frozen.as_slice()[..HEADER_SIZE])
                .expect("valid ReplyHeader");
        assert_eq!(
            header.checksum, original_checksum,
            "must be original cached bytes, not fresh synthesis"
        );
        assert_eq!(header.request, REGISTER_REQUEST_ID);
        assert_eq!(header.commit, session);
    }

    // Past-register retry: silent drop, no replay, no eviction. Read-timeout
    // recovers; lost session surfaces as NoSession on next non-register.
    #[test]
    fn register_preflight_silently_drops_retry_after_progress() {
        let consensus = VsrConsensus::new(1, 0, 3, 0, ClientSpyBus::new(), LocalPipeline::new());
        consensus.init();
        let client_table = fresh_client_table();

        let client_id: u128 = 0xBEEF;
        let session: u64 = 17;

        let initial_reply = synthesize_register_reply(&consensus, client_id, session);
        client_table
            .borrow_mut()
            .commit_register(client_id, ACTING_USER_ID, initial_reply, |_| false);

        // SendMessages commits -> cached is no longer the register reply.
        let app_reply = synthesize_send_messages_reply(&consensus, client_id, session, 1, 18);
        client_table
            .borrow_mut()
            .commit_reply(client_id, session, app_reply);

        let result =
            futures::executor::block_on(register_preflight(&consensus, &client_table, client_id));
        assert!(!result, "AlreadyRegistered short-circuits");

        let sends = consensus.message_bus().client_sends.borrow();
        assert!(
            sends.is_empty(),
            "post-progress register retry must be silent drop"
        );
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
                10, // session
                1,  // request
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

    // Stale session (post capacity-evict + re-register): terminal SessionTooLow.
    #[test]
    fn request_preflight_session_too_low_evicts_client() {
        let consensus = VsrConsensus::new(1, 0, 3, 0, ClientSpyBus::new(), LocalPipeline::new());
        consensus.init();
        let client_table = fresh_client_table();

        let client_id: u128 = 0xBEEF;
        let real_session: u64 = 99;

        // Slot at session 99.
        let initial_reply = synthesize_register_reply(&consensus, client_id, real_session);
        client_table
            .borrow_mut()
            .commit_register(client_id, ACTING_USER_ID, initial_reply, |_| false);

        // Older retry (17 < 99): stale-session case.
        let result = futures::executor::block_on(apply_preflight_consensus_plane(
            &consensus,
            request_preflight(&consensus, &client_table, client_id, 17, 1),
            client_id,
        ));
        assert!(!result, "SessionMismatch short-circuits");

        let sends = consensus.message_bus().client_sends.borrow();
        assert_eq!(sends.len(), 1, "one Eviction");

        let frozen = &sends[0].1;
        let header =
            bytemuck::checked::try_from_bytes::<EvictionHeader>(&frozen.as_slice()[..HEADER_SIZE])
                .expect("valid EvictionHeader");
        assert_eq!(header.reason, EvictionReason::SessionTooLow);
        assert_eq!(header.client, client_id);
    }

    // Newer-than-cluster session: sessions monotonic; healthy SDK can't reach
    // this. Client bug -> silent drop, no eviction.
    #[test]
    fn request_preflight_session_too_high_is_silent_drop() {
        let consensus = VsrConsensus::new(1, 0, 3, 0, ClientSpyBus::new(), LocalPipeline::new());
        consensus.init();
        let client_table = fresh_client_table();

        let client_id: u128 = 0xBEEF;
        let real_session: u64 = 17;

        // Slot at session 17.
        let initial_reply = synthesize_register_reply(&consensus, client_id, real_session);
        client_table
            .borrow_mut()
            .commit_register(client_id, ACTING_USER_ID, initial_reply, |_| false);

        // Client claims newer session (99 > 17), client bug.
        let result = futures::executor::block_on(apply_preflight_consensus_plane(
            &consensus,
            request_preflight(&consensus, &client_table, client_id, 99, 1),
            client_id,
        ));
        assert!(!result, "SessionMismatch short-circuits");

        let sends = consensus.message_bus().client_sends.borrow();
        assert!(
            sends.is_empty(),
            "newer-session mismatch must be silent drop"
        );
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
                10, // session
                1,  // request
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

    // Stale + RequestGap: silent drop, no eviction.
    #[test]
    fn request_preflight_stale_is_silent_drop() {
        let consensus = VsrConsensus::new(1, 0, 3, 0, ClientSpyBus::new(), LocalPipeline::new());
        consensus.init();
        let client_table = fresh_client_table();

        let client_id: u128 = 0xABCD;
        let session: u64 = 5;

        let initial_reply = synthesize_register_reply(&consensus, client_id, session);
        client_table
            .borrow_mut()
            .commit_register(client_id, ACTING_USER_ID, initial_reply, |_| false);
        // Cache at request 5 -> request 3 is stale.
        let advanced = synthesize_send_messages_reply(&consensus, client_id, session, 5, 100);
        client_table
            .borrow_mut()
            .commit_reply(client_id, session, advanced);

        let result = futures::executor::block_on(apply_preflight_consensus_plane(
            &consensus,
            request_preflight(&consensus, &client_table, client_id, session, 3), // stale
            client_id,
        ));
        assert!(!result);

        let sends = consensus.message_bus().client_sends.borrow();
        assert!(sends.is_empty(), "stale = silent drop");
    }

    // Catch-up gate prevents WAL-replay race in commit_register: primary
    // with commit_min < commit_max may hold inherited Register(client) in
    // WAL not yet applied. Fresh Register would panic session-equality
    // assert on replay.
    #[test]
    fn register_preflight_silently_drops_when_behind_on_commits() {
        let consensus = VsrConsensus::new(1, 0, 3, 0, ClientSpyBus::new(), LocalPipeline::new());
        consensus.init();
        let client_table = fresh_client_table();
        assert!(consensus.is_primary(), "test setup: primary");

        // commit_min=0, commit_max=5: behind on local execution.
        consensus.advance_commit_max(5);
        assert_ne!(consensus.commit_min(), consensus.commit_max());

        let client_id: u128 = 0xC0DE;

        let result =
            futures::executor::block_on(register_preflight(&consensus, &client_table, client_id));
        assert!(!result, "register dispatch must short-circuit");

        let sends = consensus.message_bus().client_sends.borrow();
        assert!(sends.is_empty(), "silent drop until catch-up");
    }

    // Direct gate test. is_syncing is hardcoded false today; clause is
    // inert. Test exercises primary, normal, commit_min == commit_max.
    // Extends naturally when is_syncing becomes real.
    #[test]
    fn is_caught_up_primary_gate_states() {
        // Primary, normal, equal commits -> true.
        let primary = VsrConsensus::new(1, 0, 3, 0, ClientSpyBus::new(), LocalPipeline::new());
        primary.init();
        assert!(primary.is_primary());
        assert!(primary.is_normal());
        assert!(!primary.is_syncing(), "is_syncing inert; pin");
        assert_eq!(primary.commit_min(), primary.commit_max());
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

        let result =
            futures::executor::block_on(register_preflight(&consensus, &client_table, client_id));
        assert!(result, "New client proceeds through consensus");

        let sends = consensus.message_bus().client_sends.borrow();
        assert!(sends.is_empty(), "no reply for New client");
    }

    // Fixture: register reply mirroring `commit_register` storage. Test-only
    // production replays cached via `AlreadyRegistered { cached_reply }`.
    #[allow(clippy::cast_possible_truncation)]
    fn synthesize_register_reply<B, P>(
        consensus: &VsrConsensus<B, P>,
        client_id: u128,
        session: u64,
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
            op: session,
            commit: session,
            request: REGISTER_REQUEST_ID,
            operation: Operation::Register,
            ..ReplyHeader::default()
        };
        msg
    }

    // SendMessages reply fixture: advances cached request number.
    #[allow(clippy::cast_possible_truncation)]
    fn synthesize_send_messages_reply<B, P>(
        consensus: &VsrConsensus<B, P>,
        client_id: u128,
        session: u64,
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
        let _ = session;
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
