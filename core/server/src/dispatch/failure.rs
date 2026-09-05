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

//! The wire failure channels and the one send exit for host-built frames.
//!
//! Every frame the dispatch host builds, success or rejection, leaves through
//! [`send_host_frame`], so the send-failure log has one shape. Which channel a
//! failure rides is a wire contract with the SDK:
//!
//! | channel | carrier | when |
//! |---|---|---|
//! | [`FrameChannel::TypedDeny`] | Reply, nonzero status + empty body, or a result-framed rejection body | rejections that must unblock the SDK's lockstep request slot: checksum, authz, pre-consensus rewrite, unknown or unsupported non-replicated code, unbound non-PING read, transient replay hints |
//! | [`FrameChannel::Eviction`] | session-terminal Eviction frame with a typed reason | the client must register again: `NoSession`, `MalformedLogin`, heartbeat and login evictions. The reason rides the channel label, since one `context` covers four of them |
//! | [`FrameChannel::ResyncSentinel`] | status-0 poll reply, body carries `RESYNC_REQUIRED_PARTITION_SENTINEL` | a fenced consumer-group poll: the consumer must re-sync its assignment; HTTP mirrors it as `resync_required_polled_messages` in `crate::http::wire` |
//! | [`FrameChannel::EmptyFrame`] | status-0 fail-fast body, the 16-byte empty poll | the partition cannot answer yet; the SDK fails fast (empty poll) and retries. A permanent client error never rides this channel: an undecodable body, an unresolved target, and a request the resolve rejects all deny typed, because there is nothing to retry |
//! | [`FrameChannel::Reply`] | status-0 success frame | host-built success replies: login/register, ping, logout, non-replicated read bodies, committed metadata replies |
//! | silent drop | no frame | one deliberate case, a transient consensus submit failure: the SDK read-timeout replays the same request id, and a synthesized failure could contradict a write that commits moments later. A header `RequestHeader::validate` rejected also drops, but that one is a GAP, not a contract - the fields decode, so a deny could be echoed under the transport id, and the client instead waits out its read timeout |
//! | HTTP status | HTTP status code | the HTTP spine maps the same rejections in `crate::http::error`; it never rides these frames |
//!
//! The last two send nothing, so [`FrameChannel`] has no variant for them.
//!
//! Scope: the table covers `crate::dispatch` only. Two neighbours answer on
//! their own paths by design - the partitions engine builds and sends
//! produce/poll replies, and the shard crate builds client-shaped denies of
//! its own (`IggyShard::deny_partition_request_transient` and
//! `stage_transient_deny`, both `TypedDeny`-shaped, the latter shedding the
//! frame outright when its lifecycle queue is full).

use crate::reply_frame::{build_deny_reply, current_metadata_commit};
use crate::responses::NonReplicatedResponse;
use crate::rewrite::RewriteStage;
use crate::shell::{ShellBus, ShellShard};
use bytes::Bytes;
use consensus::{
    EvictionContext, MetadataHandle, build_eviction_message,
    build_incompatible_protocol_eviction_message, build_result_rejection_reply,
};
use iggy_binary_protocol::{EvictionReason, PrepareHeader, RoutedRequestHeader};
use iggy_common::IggyError;
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use message_bus::BusMessage;
use server_common::Message;
use std::rc::Rc;
use tracing::warn;

/// Labels the channel a host-built frame rides, for the send-failure log.
/// The taxonomy, including the two channels that never construct a frame,
/// is on the module doc.
#[derive(Clone, Copy, Debug)]
pub(in crate::dispatch) enum FrameChannel {
    TypedDeny,
    /// The reason travels with the channel: five call sites share the
    /// `"login_rejection"` context across four distinct reasons, so
    /// `context` alone cannot tell a `MalformedLogin` send failure from an
    /// `InvalidCredentials` one.
    Eviction(EvictionReason),
    ResyncSentinel,
    EmptyFrame,
    Reply,
}

impl std::fmt::Display for FrameChannel {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TypedDeny => formatter.write_str("typed_deny"),
            Self::Eviction(reason) => write!(formatter, "eviction({reason:?})"),
            Self::ResyncSentinel => formatter.write_str("resync_sentinel"),
            Self::EmptyFrame => formatter.write_str("empty_frame"),
            Self::Reply => formatter.write_str("reply"),
        }
    }
}

/// The one send exit for host-built client frames. Best-effort: a failed
/// send means the connection is gone (or its queue is full), and there is
/// nothing left to reply on, so the error is logged and dropped. `frame` is
/// any bus message: a contiguous frozen frame or the vectored poll reply.
#[allow(clippy::future_not_send)]
pub(in crate::dispatch) async fn send_host_frame<B: ShellBus>(
    bus: &B,
    transport_client_id: u128,
    frame: impl Into<BusMessage>,
    channel: FrameChannel,
    context: &'static str,
) {
    if let Err(send_error) = bus.send_to_client(transport_client_id, frame).await {
        warn!(
            transport_client_id,
            error = %send_error,
            channel = %channel,
            context,
            "failed to send host frame to client"
        );
    }
}

/// Reply to a request rejected before it reached its plane with the request's
/// own frame: empty body + nonzero `status`. The nonzero status is the whole
/// point: the SDK peeks it and surfaces the typed error, whereas a status-0
/// frame reads as a committed ack for work that never happened. Silence is no
/// better, the connection decodes replies in lockstep and would wedge on every
/// later request.
#[allow(clippy::future_not_send)]
pub(in crate::dispatch) async fn send_deny_reply<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    transport_client_id: u128,
    request_header: &RoutedRequestHeader,
    status: u32,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let commit = current_metadata_commit(shard);
    let reply = build_deny_reply(request_header, transport_client_id, 0, commit, status);
    send_host_frame(
        &shard.bus,
        transport_client_id,
        reply.into_generic().into_frozen(),
        FrameChannel::TypedDeny,
        "request_denial",
    )
    .await;
}

/// Deny a request from an unbound transport without disclosing the metadata
/// commit frontier. The status is the only field a pre-authenticated caller
/// needs, while the live commit would expose cluster write activity.
#[allow(clippy::future_not_send)]
pub(in crate::dispatch) async fn send_unbound_deny_reply<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    transport_client_id: u128,
    request_header: &RoutedRequestHeader,
    status: u32,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let reply = build_deny_reply(request_header, transport_client_id, 0, 0, status);
    send_host_frame(
        &shard.bus,
        transport_client_id,
        reply.into_generic().into_frozen(),
        FrameChannel::TypedDeny,
        "unbound_request_denial",
    )
    .await;
}

/// Reply to a denied non-replicated read with the request's reply frame: empty
/// body + nonzero `status`. The SDK peeks the status before body decode and
/// surfaces the typed error, so a poll denial never reaches the empty-poll
/// "0 messages" body path.
#[allow(clippy::future_not_send)]
pub(in crate::dispatch) async fn send_non_replicated_deny<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    request: &Message<RoutedRequestHeader>,
    transport_client_id: u128,
    status: u32,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let commit = current_metadata_commit(shard);
    let reply = build_deny_reply(
        request.header(),
        request.header().client,
        request.header().session,
        commit,
        status,
    );
    send_host_frame(
        &shard.bus,
        transport_client_id,
        reply.into_generic().into_frozen(),
        FrameChannel::TypedDeny,
        "non_replicated_denial",
    )
    .await;
}

/// Reject a request before it reaches consensus: warn, then send the typed
/// deny reply. A silent drop would wedge every later request on the
/// connection until the socket read timeout. `stage` names the chain step
/// for both log lines, so the set stays enumerable.
#[allow(clippy::future_not_send)]
pub(in crate::dispatch) async fn send_pre_consensus_deny<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    transport_client_id: u128,
    request_header: &RoutedRequestHeader,
    error: &IggyError,
    stage: RewriteStage,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let context = stage.as_str();
    warn!(
        transport_client_id,
        error = %error,
        operation = ?request_header.operation,
        context,
        "denying request pre-consensus"
    );
    let commit = current_metadata_commit(shard);
    let reply = build_deny_reply(
        request_header,
        transport_client_id,
        0,
        commit,
        error.as_code(),
    );
    send_host_frame(
        &shard.bus,
        transport_client_id,
        reply.into_generic().into_frozen(),
        FrameChannel::TypedDeny,
        context,
    )
    .await;
}

/// Result-framed rejection Reply: status 0, body `[count=1][index][code]`.
/// The SDK decodes the nonzero result code, so a transient code makes it
/// replay the same request at once instead of waiting out its read timeout.
/// Replying empty instead would surface as a hard `InvalidFormat` decode
/// failure and break the replay.
#[allow(clippy::future_not_send)]
pub(in crate::dispatch) async fn send_result_rejection<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    transport_client_id: u128,
    request_header: &RoutedRequestHeader,
    error: &IggyError,
    context: &'static str,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let commit = current_metadata_commit(shard);
    let reply = build_result_rejection_reply(request_header, commit, error.as_code());
    send_host_frame(
        &shard.bus,
        transport_client_id,
        reply.into_generic().into_frozen(),
        FrameChannel::TypedDeny,
        context,
    )
    .await;
}

/// Best-effort session-terminal `Eviction` frame: the client's session is
/// gone (or was never granted), so it must register again. Every frame
/// transport decodes `Command::Eviction` and maps the typed reason
/// (`NoSession` -> `Unauthenticated`, ...), so clients fail fast with the
/// real cause instead of a body-decode failure or a timeout. Consensus
/// context (cluster/view/replica) is stamped on the metadata shard and
/// zeroed elsewhere; the SDK only reads the reason, plus the protocol
/// window on `IncompatibleProtocol`.
#[allow(clippy::future_not_send)]
pub(in crate::dispatch) async fn send_eviction<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    transport_client_id: u128,
    vsr_client_id: u128,
    reason: EvictionReason,
    context: &'static str,
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
    send_host_frame(
        &shard.bus,
        transport_client_id,
        eviction.into_generic().into_frozen(),
        FrameChannel::Eviction(reason),
        context,
    )
    .await;
}

/// Send a non-replicated reply body to a client, stamping the current
/// metadata commit. Shared by the non-replicated read arms; `channel`
/// labels the body shape (a real answer, a fail-fast empty poll, or the
/// re-sync sentinel) for the send-failure log.
#[allow(clippy::future_not_send)]
pub(in crate::dispatch) async fn send_non_replicated_bytes<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    request: &Message<RoutedRequestHeader>,
    transport_client_id: u128,
    bytes: Bytes,
    channel: FrameChannel,
    context: &'static str,
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
    send_host_frame(
        &shard.bus,
        transport_client_id,
        reply.into_generic().into_frozen(),
        channel,
        context,
    )
    .await;
}

// Shape tests for the host frame exits: each pins the contract a client
// decodes (status, body shape, identity, commit stamp), never the raw bytes.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::handle_client_request;
    use crate::dispatch::test_support::{
        FIRST_BOOT, SpyBus, TestShard, request_message, test_shard,
    };
    use crate::session_manager::SessionManager;
    use configs::server::ServerSystemConfig;
    use consensus::{LocalPipeline, VsrConsensus};
    use iggy_binary_protocol::codes::PING_CODE;
    use iggy_binary_protocol::consensus::REJECTION_SECTION_LEN;
    use iggy_binary_protocol::{
        Command, ConsensusHeader, EvictionHeader, IGGY_PROTOCOL_VERSION, IGGY_PROTOCOL_VERSION_MIN,
        Operation, ReplyHeader, result_code,
    };
    use iggy_common::RESYNC_REQUIRED_PARTITION_SENTINEL;
    use std::cell::RefCell;
    use std::mem::size_of;
    use std::sync::Arc;

    const TRANSPORT: u128 = 42;
    const VSR_CLIENT: u128 = 7;
    const SESSION: u64 = 3;
    const REQUEST: u64 = 5;
    /// Nonzero, so a stamped frontier is distinguishable from a withheld one.
    const COMMIT: u64 = 11;
    const MAX_TOKENS_PER_USER: u32 = 1;
    /// `[partition_id:4][current_offset:8][count:4]`.
    const EMPTY_POLL_BODY_LEN: usize = 16;
    /// Send-failure log label only; no exit reads it.
    const CONTEXT: &str = "shape_test";

    /// A shard whose metadata commit frontier sits at `COMMIT`.
    fn shard_at_commit() -> (SpyBus, Rc<TestShard>) {
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, 0, 1, FIRST_BOOT));
        metadata_consensus(&shard).advance_commit_max(COMMIT);
        (bus, shard)
    }

    fn metadata_consensus(shard: &TestShard) -> &VsrConsensus<SpyBus, LocalPipeline> {
        shard
            .plane
            .metadata()
            .consensus
            .as_ref()
            .expect("test shard carries metadata consensus")
    }

    /// The one frame on the bus, decoded as `H` and passed through its own
    /// header validation. Every host frame targets the transport connection
    /// and declares its own length.
    fn sole_frame_to_transport<H: ConsensusHeader>(bus: &SpyBus) -> (H, Vec<u8>) {
        let replies = bus.client_replies.borrow();
        assert_eq!(replies.len(), 1, "expected exactly one client-bound frame");
        let (target, frame) = &replies[0];
        assert_eq!(
            *target, TRANSPORT,
            "the frame must go to the transport connection"
        );
        let header_bytes = frame
            .get(..size_of::<H>())
            .expect("frame holds a full header");
        let header = bytemuck::checked::try_pod_read_unaligned::<H>(header_bytes)
            .expect("client frame decodes into the expected header");
        header
            .validate()
            .expect("host frame passes its own header validation");
        assert_eq!(
            header.size() as usize,
            frame.len(),
            "size must cover the whole frame"
        );
        (header, frame[size_of::<H>()..].to_vec())
    }

    fn poll_request() -> Message<RoutedRequestHeader> {
        request_message(Operation::NonReplicated, VSR_CLIENT, SESSION, REQUEST, &[])
    }

    /// The echo every request-correlated Reply carries.
    fn assert_echoes_request(header: &ReplyHeader, request: &RoutedRequestHeader) {
        assert_eq!(header.command, Command::Reply, "must ride a Reply frame");
        assert_eq!(header.request, request.request, "must echo the request id");
        assert_eq!(
            header.operation, request.operation,
            "must echo the request operation"
        );
    }

    /// The typed deny shape: the request's own Reply, nonzero status, no body.
    fn assert_typed_deny(
        header: &ReplyHeader,
        body: &[u8],
        request: &RoutedRequestHeader,
        status: u32,
    ) {
        assert_echoes_request(header, request);
        assert_eq!(
            header.status, status,
            "the status carries the typed error code"
        );
        assert_ne!(
            header.status, 0,
            "a deny must never read as a committed ack"
        );
        assert!(body.is_empty(), "a deny reply carries no body");
    }

    /// The empty `PolledMessages` body the poll arm sends when a partition
    /// cannot answer; with the re-sync sentinel as `partition_id` it tells a
    /// fenced consumer to re-sync its assignment.
    fn empty_poll_body(partition_id: u32) -> Bytes {
        let mut body = Vec::with_capacity(EMPTY_POLL_BODY_LEN);
        body.extend_from_slice(&partition_id.to_le_bytes());
        body.extend_from_slice(&0u64.to_le_bytes());
        body.extend_from_slice(&0u32.to_le_bytes());
        Bytes::from(body)
    }

    /// Send one eviction and decode it. The bus is cleared first so each
    /// reason is inspected on its own.
    async fn evict(
        bus: &SpyBus,
        shard: &Rc<TestShard>,
        vsr_client: u128,
        reason: EvictionReason,
    ) -> EvictionHeader {
        bus.client_replies.borrow_mut().clear();
        send_eviction(shard, TRANSPORT, vsr_client, reason, CONTEXT).await;
        let (header, _) = sole_frame_to_transport::<EvictionHeader>(bus);
        let consensus = metadata_consensus(shard);
        assert_eq!(
            (header.cluster, header.view, header.replica),
            (consensus.cluster(), consensus.view(), consensus.replica()),
            "the metadata shard stamps its live consensus context"
        );
        header
    }

    #[compio::test]
    async fn typed_deny_must_echo_the_request_with_nonzero_status_and_no_body() {
        let (bus, shard) = shard_at_commit();
        let request = poll_request();
        let status = IggyError::Unauthenticated.as_code();

        send_deny_reply(&shard, TRANSPORT, request.header(), status).await;

        let (header, body) = sole_frame_to_transport::<ReplyHeader>(&bus);
        assert_typed_deny(&header, &body, request.header(), status);
        assert_eq!(
            header.client, TRANSPORT,
            "a pre-plane deny answers under the transport id"
        );
        assert_eq!(header.op, 0, "a pre-plane deny has no session to stamp");
        assert_eq!(
            header.commit, COMMIT,
            "a bound deny stamps the live metadata commit"
        );
    }

    #[compio::test]
    async fn unbound_deny_must_not_disclose_the_commit_frontier() {
        let (bus, shard) = shard_at_commit();
        let request = poll_request();
        let status = IggyError::Unauthenticated.as_code();

        send_unbound_deny_reply(&shard, TRANSPORT, request.header(), status).await;

        let (header, body) = sole_frame_to_transport::<ReplyHeader>(&bus);
        assert_typed_deny(&header, &body, request.header(), status);
        assert_eq!(
            header.client, TRANSPORT,
            "an unbound deny answers under the transport id"
        );
        assert_eq!(header.op, 0, "an unbound deny has no session to stamp");
        assert_eq!(
            header.commit, 0,
            "an unbound transport must not learn the commit frontier"
        );
    }

    #[compio::test]
    async fn non_replicated_deny_must_answer_under_the_request_session() {
        let (bus, shard) = shard_at_commit();
        let request = poll_request();
        let status = IggyError::Unauthenticated.as_code();

        send_non_replicated_deny(&shard, &request, TRANSPORT, status).await;

        let (header, body) = sole_frame_to_transport::<ReplyHeader>(&bus);
        assert_typed_deny(&header, &body, request.header(), status);
        assert_eq!(
            header.client, VSR_CLIENT,
            "a read deny answers under the request's client id"
        );
        assert_eq!(
            header.op, SESSION,
            "a read deny stamps the request's session"
        );
        assert_eq!(
            header.commit, COMMIT,
            "a read deny stamps the live metadata commit"
        );
    }

    #[compio::test]
    async fn pre_consensus_deny_must_carry_the_error_code_as_status() {
        let (bus, shard) = shard_at_commit();
        let request = poll_request();
        let error = IggyError::InvalidCredentials;

        send_pre_consensus_deny(
            &shard,
            TRANSPORT,
            request.header(),
            &error,
            RewriteStage::UserPassword,
        )
        .await;

        let (header, body) = sole_frame_to_transport::<ReplyHeader>(&bus);
        assert_typed_deny(&header, &body, request.header(), error.as_code());
        assert_eq!(
            header.client, TRANSPORT,
            "a pre-consensus deny answers under the transport id"
        );
        assert_eq!(header.op, 0, "a pre-consensus deny has no session to stamp");
        assert_eq!(
            header.commit, COMMIT,
            "a pre-consensus deny stamps the live metadata commit"
        );
    }

    #[compio::test]
    async fn result_rejection_must_ride_status_zero_with_the_code_in_the_result_section() {
        let (bus, shard) = shard_at_commit();
        let request = poll_request();
        let error = IggyError::TransientNotAccepted;

        send_result_rejection(&shard, TRANSPORT, request.header(), &error, CONTEXT).await;

        let (header, body) = sole_frame_to_transport::<ReplyHeader>(&bus);
        assert_echoes_request(&header, request.header());
        assert_eq!(header.status, 0, "a result-framed rejection keeps status 0");
        assert_eq!(
            body.len(),
            REJECTION_SECTION_LEN,
            "the body is exactly one rejection section"
        );
        assert_eq!(
            result_code(&body),
            Some(error.as_code()),
            "the result section carries the code"
        );
        assert_eq!(
            header.client, VSR_CLIENT,
            "a rejection answers under the request's client id"
        );
        assert_eq!(
            header.op, COMMIT,
            "op is position-typed at the commit like every reply"
        );
        assert_eq!(
            header.commit, COMMIT,
            "a rejection stamps the live metadata commit"
        );
    }

    #[compio::test]
    async fn eviction_must_carry_the_typed_reason_and_the_live_consensus_context() {
        let bus = SpyBus::default();
        // Replica 1 of 3, so a stamped replica differs from the zeroed fallback.
        let shard = Rc::new(test_shard(&bus, 1, 3, FIRST_BOOT));

        let no_session = evict(&bus, &shard, TRANSPORT, EvictionReason::NoSession).await;
        assert_eq!(
            no_session.reason,
            EvictionReason::NoSession,
            "the reason rides typed"
        );
        assert_eq!(
            no_session.client, TRANSPORT,
            "a client without a session is addressed by its transport id"
        );

        let malformed_login = evict(&bus, &shard, VSR_CLIENT, EvictionReason::MalformedLogin).await;
        assert_eq!(
            malformed_login.reason,
            EvictionReason::MalformedLogin,
            "the reason rides typed"
        );
        assert_eq!(
            malformed_login.client, VSR_CLIENT,
            "a login eviction is addressed to the request's client id"
        );

        let incompatible = evict(
            &bus,
            &shard,
            VSR_CLIENT,
            EvictionReason::IncompatibleProtocol,
        )
        .await;
        assert_eq!(
            incompatible.reason,
            EvictionReason::IncompatibleProtocol,
            "the reason rides typed"
        );
        assert_eq!(
            incompatible.client, VSR_CLIENT,
            "a login eviction is addressed to the request's client id"
        );
        // On a `.0` release both bounds coincide, so a swapped max/min pair
        // passes here; `validate` still holds `1 <= min <= max`.
        assert_eq!(
            (
                incompatible.server_protocol_version,
                incompatible.server_protocol_version_min
            ),
            (IGGY_PROTOCOL_VERSION, IGGY_PROTOCOL_VERSION_MIN),
            "IncompatibleProtocol carries the accepted protocol window"
        );
    }

    /// The HTTP mirror (`resync_required_polled_messages` in
    /// `crate::http::wire`) is a JSON DTO, not a wire frame, so the shared
    /// contract asserted here is the sentinel constant itself; the DTO's own
    /// test pins its `partition_id` to the same constant.
    #[compio::test]
    async fn resync_sentinel_must_ride_a_status_zero_poll_body_led_by_the_sentinel() {
        let (bus, shard) = shard_at_commit();
        let request = poll_request();

        send_non_replicated_bytes(
            &shard,
            &request,
            TRANSPORT,
            empty_poll_body(RESYNC_REQUIRED_PARTITION_SENTINEL),
            FrameChannel::ResyncSentinel,
            CONTEXT,
        )
        .await;

        let (header, body) = sole_frame_to_transport::<ReplyHeader>(&bus);
        assert_echoes_request(&header, request.header());
        assert_eq!(
            header.status, 0,
            "the sentinel rides a success frame, not a deny"
        );
        assert_eq!(
            body.len(),
            EMPTY_POLL_BODY_LEN,
            "the body is the empty poll the SDK decoder requires"
        );
        assert_eq!(
            body[..4],
            RESYNC_REQUIRED_PARTITION_SENTINEL.to_le_bytes(),
            "the body leads with the re-sync sentinel partition id"
        );
        assert_eq!(
            header.client, VSR_CLIENT,
            "a read reply answers under the request's client id"
        );
        assert_eq!(
            header.op, SESSION,
            "a read reply stamps the request's session"
        );
        assert_eq!(
            header.commit, COMMIT,
            "a read reply stamps the live metadata commit"
        );
    }

    /// The PING reply is the one host-built success Reply the funnel serves
    /// without a consensus round.
    #[compio::test]
    async fn ping_must_reply_status_zero_and_empty_under_the_request_session() {
        let (bus, shard) = shard_at_commit();
        let sessions = Rc::new(RefCell::new(SessionManager::new()));
        let system_config = Arc::new(ServerSystemConfig::default());
        let request = poll_request().transmute_header(|header, ping: &mut RoutedRequestHeader| {
            *ping = header;
            ping.reserved[..4].copy_from_slice(&PING_CODE.to_le_bytes());
        });
        let request_header = *request.header();

        handle_client_request(
            &shard,
            &sessions,
            &system_config,
            MAX_TOKENS_PER_USER,
            TRANSPORT,
            request.into_generic(),
        )
        .await;

        let (header, body) = sole_frame_to_transport::<ReplyHeader>(&bus);
        assert_echoes_request(&header, &request_header);
        assert_eq!(header.status, 0, "a served ping is a success reply");
        assert!(body.is_empty(), "the ping reply carries no body");
        assert_eq!(
            header.client, VSR_CLIENT,
            "the ping answers under the request's client id"
        );
        assert_eq!(header.op, SESSION, "the ping stamps the request's session");
        assert_eq!(
            header.commit, COMMIT,
            "the ping stamps the live metadata commit"
        );
    }
}
