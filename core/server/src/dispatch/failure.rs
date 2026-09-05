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

use crate::responses::{NonReplicatedResponse, build_deny_reply, current_metadata_commit};
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

// Byte snapshots pinning each channel's frame to the pre-refactor inline
// construction. What they hold is that routing a rejection through this
// module changed no byte a client sees: same command, same status, same
// header echo, same body length per channel. They are NOT the wire
// contract - a deliberate protocol change updates them.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::handle_client_request;
    use crate::dispatch::test_support::{
        FIRST_BOOT, SpyBus, TestShard, request_message, test_shard,
    };
    use crate::responses::build_empty_reply;
    use crate::session_manager::SessionManager;
    use configs::server::ServerSystemConfig;
    use iggy_binary_protocol::Operation;
    use iggy_binary_protocol::codes::PING_CODE;
    use iggy_common::RESYNC_REQUIRED_PARTITION_SENTINEL;
    use std::cell::RefCell;
    use std::sync::Arc;

    const TRANSPORT: u128 = 42;
    const VSR_CLIENT: u128 = 7;
    const SESSION: u64 = 3;
    const REQUEST: u64 = 5;

    fn snapshot_shard() -> (SpyBus, Rc<TestShard>) {
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, 0, 1, FIRST_BOOT));
        (bus, shard)
    }

    fn sole_client_frame(bus: &SpyBus) -> (u128, Vec<u8>) {
        let replies = bus.client_replies.borrow();
        assert_eq!(replies.len(), 1, "expected exactly one client-bound frame");
        replies[0].clone()
    }

    fn poll_request() -> Message<RoutedRequestHeader> {
        request_message(Operation::NonReplicated, VSR_CLIENT, SESSION, REQUEST, &[])
    }

    /// The 16-byte empty `PolledMessages` body exactly as the pre-refactor
    /// `partition::empty_polled_messages_body` built it.
    fn old_empty_polled_messages_body(partition_id: u32) -> Bytes {
        let mut body = Vec::with_capacity(16);
        body.extend_from_slice(&partition_id.to_le_bytes());
        body.extend_from_slice(&0u64.to_le_bytes());
        body.extend_from_slice(&0u32.to_le_bytes());
        Bytes::from(body)
    }

    fn old_eviction_context(shard: &Rc<TestShard>) -> EvictionContext {
        shard.plane.metadata().consensus.as_ref().map_or(
            EvictionContext {
                cluster: 0,
                view: 0,
                replica: 0,
            },
            EvictionContext::from_consensus,
        )
    }

    #[compio::test]
    async fn snapshot_typed_deny_commit_stamped_frame_unchanged() {
        let (bus, shard) = snapshot_shard();
        let request = poll_request();
        let status = IggyError::Unauthenticated.as_code();
        let old = build_deny_reply(
            request.header(),
            TRANSPORT,
            0,
            current_metadata_commit(&shard),
            status,
        )
        .into_generic();

        send_deny_reply(&shard, TRANSPORT, request.header(), status).await;

        let (target, frame) = sole_client_frame(&bus);
        assert_eq!(target, TRANSPORT);
        assert_eq!(frame, old.as_slice().to_vec());
    }

    #[compio::test]
    async fn snapshot_typed_deny_unbound_frame_unchanged() {
        let (bus, shard) = snapshot_shard();
        let request = poll_request();
        let status = IggyError::Unauthenticated.as_code();
        let old = build_deny_reply(request.header(), TRANSPORT, 0, 0, status).into_generic();

        send_unbound_deny_reply(&shard, TRANSPORT, request.header(), status).await;

        let (target, frame) = sole_client_frame(&bus);
        assert_eq!(target, TRANSPORT);
        assert_eq!(frame, old.as_slice().to_vec());
    }

    #[compio::test]
    async fn snapshot_typed_deny_non_replicated_frame_unchanged() {
        let (bus, shard) = snapshot_shard();
        let request = poll_request();
        let status = IggyError::Unauthenticated.as_code();
        let old = build_deny_reply(
            request.header(),
            request.header().client,
            request.header().session,
            current_metadata_commit(&shard),
            status,
        )
        .into_generic();

        send_non_replicated_deny(&shard, &request, TRANSPORT, status).await;

        let (target, frame) = sole_client_frame(&bus);
        assert_eq!(target, TRANSPORT);
        assert_eq!(frame, old.as_slice().to_vec());
    }

    #[compio::test]
    async fn snapshot_typed_deny_result_framed_frame_unchanged() {
        let (bus, shard) = snapshot_shard();
        let request = poll_request();
        let code = IggyError::TransientNotAccepted;
        let old = build_result_rejection_reply(
            request.header(),
            current_metadata_commit(&shard),
            code.as_code(),
        )
        .into_generic();

        send_result_rejection(&shard, TRANSPORT, request.header(), &code, "snapshot").await;

        let (target, frame) = sole_client_frame(&bus);
        assert_eq!(target, TRANSPORT);
        assert_eq!(frame, old.as_slice().to_vec());
    }

    #[compio::test]
    async fn snapshot_eviction_frame_unchanged() {
        let (bus, shard) = snapshot_shard();
        let ctx = old_eviction_context(&shard);

        // Old `send_unauthenticated_eviction`: reason NoSession, client id =
        // the transport id.
        let old = build_eviction_message(ctx, TRANSPORT, EvictionReason::NoSession).into_generic();
        send_eviction(
            &shard,
            TRANSPORT,
            TRANSPORT,
            EvictionReason::NoSession,
            "snapshot",
        )
        .await;
        let (target, frame) = sole_client_frame(&bus);
        assert_eq!(target, TRANSPORT);
        assert_eq!(frame, old.as_slice().to_vec());
        bus.client_replies.borrow_mut().clear();

        // Old `send_login_eviction`: reason MalformedLogin, client id = the
        // request's VSR client id.
        let old =
            build_eviction_message(ctx, VSR_CLIENT, EvictionReason::MalformedLogin).into_generic();
        send_eviction(
            &shard,
            TRANSPORT,
            VSR_CLIENT,
            EvictionReason::MalformedLogin,
            "snapshot",
        )
        .await;
        let (target, frame) = sole_client_frame(&bus);
        assert_eq!(target, TRANSPORT);
        assert_eq!(frame, old.as_slice().to_vec());
        bus.client_replies.borrow_mut().clear();

        // Old `send_login_eviction` on `IncompatibleProtocol`: the protocol
        // window rides the frame, client id = the request's VSR client id.
        let old = build_incompatible_protocol_eviction_message(ctx, VSR_CLIENT).into_generic();
        send_eviction(
            &shard,
            TRANSPORT,
            VSR_CLIENT,
            EvictionReason::IncompatibleProtocol,
            "snapshot",
        )
        .await;
        let (target, frame) = sole_client_frame(&bus);
        assert_eq!(target, TRANSPORT);
        assert_eq!(frame, old.as_slice().to_vec());
    }

    /// The HTTP mirror (`resync_required_polled_messages` in
    /// `crate::http::wire`) is a JSON DTO, not a wire frame, so the shared
    /// contract asserted here is the sentinel constant itself; the DTO's own
    /// test pins its `partition_id` to the same constant.
    #[compio::test]
    async fn snapshot_resync_sentinel_frame_unchanged() {
        let (bus, shard) = snapshot_shard();
        let request = poll_request();
        let body = old_empty_polled_messages_body(RESYNC_REQUIRED_PARTITION_SENTINEL);
        assert_eq!(
            body[..4],
            RESYNC_REQUIRED_PARTITION_SENTINEL.to_le_bytes(),
            "sentinel poll body must lead with the re-sync sentinel partition id"
        );
        let old = NonReplicatedResponse::Bytes(body.clone())
            .into_reply(
                request.header(),
                request.header().client,
                request.header().session,
                current_metadata_commit(&shard),
            )
            .into_generic();

        send_non_replicated_bytes(
            &shard,
            &request,
            TRANSPORT,
            body,
            FrameChannel::ResyncSentinel,
            "poll_messages",
        )
        .await;

        let (target, frame) = sole_client_frame(&bus);
        assert_eq!(target, TRANSPORT);
        assert_eq!(frame, old.as_slice().to_vec());
    }

    /// The PING reply is the one host-built success Reply the funnel serves
    /// without a consensus round; the old side is the frame the reads
    /// router's PING arm built inline before the exit existed.
    #[compio::test]
    async fn snapshot_reply_frame_unchanged() {
        let (bus, shard) = snapshot_shard();
        let sessions = Rc::new(RefCell::new(SessionManager::new()));
        let system_config = Arc::new(ServerSystemConfig::default());
        let request = request_message(Operation::NonReplicated, VSR_CLIENT, SESSION, REQUEST, &[])
            .transmute_header(|header, ping: &mut RoutedRequestHeader| {
                *ping = header;
                ping.reserved[..4].copy_from_slice(&PING_CODE.to_le_bytes());
                // The funnel promotes the client wire header with `group`
                // unset, so the old side must build from the same bytes.
                ping.group = 0;
            });
        let old = build_empty_reply(
            request.header(),
            request.header().client,
            request.header().session,
            current_metadata_commit(&shard),
        )
        .into_generic();

        handle_client_request(
            &shard,
            &sessions,
            &system_config,
            1,
            TRANSPORT,
            request.into_generic(),
        )
        .await;

        let (target, frame) = sole_client_frame(&bus);
        assert_eq!(target, TRANSPORT);
        assert_eq!(frame, old.as_slice().to_vec());
    }
}
