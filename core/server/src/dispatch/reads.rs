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

//! Non-replicated read router and its per-code arms.
//!
//! Every `Operation::NonReplicated` request lands in
//! [`handle_non_replicated_request`] after the funnel's pre-auth gate; the
//! poll and consumer-offset arms live in `partition` (they read through the
//! shard mesh), everything else is served here from local shard state. The
//! catch-all arm delegates to the shared `responses` builder, which is
//! byte-shared with the HTTP read path -- authorization happens HERE (and in
//! the HTTP layer), never in the builder.

use crate::cluster_meta::ClusterRoster;
use crate::dispatch::authz::{authorize_default_read, authorize_uid};
use crate::dispatch::failure::{
    FrameChannel, send_host_frame, send_non_replicated_bytes, send_non_replicated_deny,
};
use crate::dispatch::partition::{handle_get_consumer_offset, handle_poll_messages};
use crate::responses::{
    build_empty_reply, build_get_me_response, build_get_personal_access_tokens_response,
    build_non_replicated_response, connected_client_to_response, current_metadata_commit,
};
use crate::session_manager::SessionManager;
use crate::shell::{ShellBus, ShellShard};
use crate::snapshot;
use crate::wire::request_body;
use bytes::Bytes;
use configs::server::{ServerConfig, ServerSystemConfig};
use consensus::MetadataHandle;
use futures::future::{Either, select};
use iggy_binary_protocol::PrepareHeader;
use iggy_binary_protocol::codes::{
    DESCRIBE_OPTIONS_CODE, GET_CLIENT_CODE, GET_CLIENTS_CODE, GET_CLUSTER_METADATA_CODE,
    GET_CONSUMER_OFFSET_CODE, GET_ME_CODE, GET_PERSONAL_ACCESS_TOKENS_CODE, GET_SNAPSHOT_FILE_CODE,
    GET_STATS_CODE, PING_CODE, POLL_MESSAGES_CODE, SYNC_CONSUMER_GROUP_CODE,
};
use iggy_binary_protocol::dispatch::lookup_command;
use iggy_binary_protocol::requests::consumer_groups::SyncConsumerGroupRequest;
use iggy_binary_protocol::requests::system::get_client::GetClientRequest;
use iggy_binary_protocol::requests::system::get_snapshot::GetSnapshotRequest;
use iggy_binary_protocol::responses::clients::client_response::ConsumerGroupInfoResponse;
use iggy_binary_protocol::responses::clients::get_client::ClientDetailsResponse;
use iggy_binary_protocol::responses::clients::get_clients::GetClientsResponse;
use iggy_binary_protocol::responses::consumer_groups::SyncConsumerGroupResponse;
use iggy_binary_protocol::responses::system::get_snapshot::GetSnapshotResponse;
use iggy_binary_protocol::{HEADER_SIZE, RoutedRequestHeader, WireDecode, WireEncode};
use iggy_common::{IggyError, SnapshotCompression, SystemSnapshotType};
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use message_bus::framing::MAX_MESSAGE_SIZE;
use metadata::AppliedFrontier;
use metadata::impls::metadata::StreamsFrontend;
use metadata::permissioner::Permissioner;
use server_common::Message;
use std::cell::RefCell;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::pin::pin;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

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
        FrameChannel::Reply,
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
        FrameChannel::Reply,
        "get_me",
    )
    .await;
}

/// Commit broadcasts a held read waits out before it fails retryable.
///
/// Sized for a node merely behind on its commit walk, NOT for a view change --
/// detecting one costs `heartbeat_timeout` and escalating it another
/// `view_change_status_timeout`, and `recovery_barrier_deadline` budgets at
/// least 15s for the same event, so a read that waits out an election is a read
/// the caller should retry elsewhere.
const READ_FRONTIER_BROADCASTS: u32 = 6;

/// How long a held metadata read may wait for this node's applied frontier,
/// sized from `[cluster] commit_broadcast_interval`: the thing the read is
/// short of is a commit broadcast, so the budget has to move with the
/// configured cadence rather than with the compile-time default that
/// `TimeoutManager::COMMIT_MESSAGE_TICKS` names (the runtime overrides it
/// through `set_commit_message_ticks`).
///
/// Minted once per process into the shared [`AppliedFrontier`], because a peer
/// shard's read gate has neither consensus nor the cluster config.
#[must_use]
pub fn read_frontier_budget(config: &ServerConfig) -> Duration {
    config
        .cluster
        .commit_broadcast_interval
        .get_duration()
        // No config ceiling on the interval, so plain `*` can overflow.
        .saturating_mul(READ_FRONTIER_BROADCASTS)
}

/// Whether `code`'s answer comes from the metadata state machine, and so must
/// not be served below the caller's watermark.
///
/// The decision for both planes, consulted by every read path that CAN hold:
/// the binary spine's gated arms run it through [`authorize_and_hold_read`] and
/// the REST spine through `http::reads::gate_local_read`. The arms that never
/// consult it are exactly the codes named below, so this is the single list of
/// what is not gated:
///
/// - `Ping` is the pre-auth liveness probe and reads nothing.
/// - `DescribeOptions` decodes a static catalog.
/// - `GetClusterMetadata` answers from the configured roster plus the
///   consensus view, and sits on the SDK's leader-discovery path, where the
///   wait would be real.
/// - `PollMessages` and `GetConsumerOffset` are partition-plane reads: their
///   answer comes from a partition group's own log, not the metadata STM, and
///   holding them would put metadata lag on the data path.
/// - `GetSnapshotFile` shells out to system tools off-thread; there is no
///   metadata answer to hold.
/// - A code this build does not know: its only outcome is `InvalidCommand`,
///   and parking a terminal error for the whole budget serves nobody.
///
/// A deny-list otherwise, so a read code added later is gated by default: the
/// failure mode of forgetting to name one here is a wait, while forgetting to
/// add it to an allow-list is a silent stale read.
pub const fn read_needs_metadata_frontier(code: u32) -> bool {
    !matches!(
        code,
        PING_CODE
            | DESCRIBE_OPTIONS_CODE
            | GET_CLUSTER_METADATA_CODE
            | POLL_MESSAGES_CODE
            | GET_CONSUMER_OFFSET_CODE
            | GET_SNAPSHOT_FILE_CODE
    ) && lookup_command(code).is_some()
}

/// Whether a frontier wait actually parked.
///
/// The caller's authorization resolved its scope off the pre-wait state
/// machine, and a wait that parked is one where that state machine moved, so
/// only the parked outcome forces the gate to run again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrontierWait {
    /// The frontier already covered the watermark: no await ran.
    Ready,
    /// The read parked, and the frontier caught up while it waited.
    CaughtUp,
}

/// The frontier never reached the watermark inside the budget. Each plane
/// renders it in its own error currency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrontierUnreached;

/// Hold a read until `frontier` covers `watermark`, or until `budget` expires.
///
/// Event-driven: the wait is woken by the commit that advances the frontier
/// (see [`AppliedFrontier::advance`]), so a read resumes on the commit it was
/// short of rather than on a poll that happens to land after it. The caller
/// supplies the budget as a future because the two read planes measure time
/// differently -- the shard bus timer, which is virtual under the simulator,
/// against `compio::time` on the HTTP listener.
///
/// Shared by both planes because one wait with two copies is one wait with a
/// drift vector, and split from its callers so the fast path, the park, the
/// wake and the expiry are all testable without a live shard.
///
/// A caller with nothing to read back has `watermark == 0`, which the first
/// comparison satisfies: one `Acquire` load, no registration, no await. Expiry
/// is loud and carries both numbers, so a frontier that stopped moving is
/// visible instead of showing up as latency.
pub async fn hold_for_frontier(
    frontier: &AppliedFrontier,
    watermark: u64,
    budget: impl Future<Output = ()>,
) -> Result<FrontierWait, FrontierUnreached> {
    if frontier.get() >= watermark {
        return Ok(FrontierWait::Ready);
    }
    let reached = pin!(frontier.reached(watermark));
    let budget = pin!(budget);
    match select(reached, budget).await {
        Either::Left(((), _)) => Ok(FrontierWait::CaughtUp),
        // Reported by the caller, not here: a durably lagging node refuses
        // every held read of every client for as long as it lags, so this is
        // a counter plus a `debug!`, never a line per refusal.
        Either::Right(((), _)) => Err(FrontierUnreached),
    }
}

/// Hold a local metadata read until this node has applied everything the
/// connection was told committed.
///
/// A committed reply hands the client an op number; answering its next read
/// from a state machine below that op contradicts the frame the client is
/// holding. The lag is real on a node whose commit walk trails the client's
/// epoch -- a backup that forwarded the client's register binds a committed
/// session while its own `commit_journal` is still behind it (see
/// [`crate::dispatch::session_ops`]) -- so the gate is not about peer shards:
/// every shard of a node reads one shared frontier and gates identically.
///
/// Fast path is a single `Acquire` load and no await, which is what keeps an
/// uncontended read shared-nothing. A park costs this connection more than the
/// read itself: the per-connection drain loop serves one frame at a time, so
/// the client's queued `SendMessages`, `PollMessages` and `PING` wait behind
/// the held read, and a client that keeps pipelining through the hold is
/// answered `TransientNotAccepted` once its queue hits
/// [`MAX_QUEUED_CLIENT_REQUESTS`](crate::dispatch::MAX_QUEUED_CLIENT_REQUESTS)
/// rather than growing it unbounded. No OTHER connection waits, and the budget
/// bounds the hold itself. Expiry fails loud and retryable rather than serving
/// state the client already saw replaced.
///
/// The wait ends on the commit that closes the gap, not on a poll: the budget
/// timer is the only timer armed, so a read that resumes costs one wake.
#[allow(clippy::future_not_send)]
async fn await_metadata_read_frontier<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    watermark: u64,
) -> Result<FrontierWait, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let frontier = shard.plane.metadata().applied_frontier();
    let budget = frontier.read_budget();
    hold_for_frontier(frontier, watermark, shard.bus.sleep(budget))
        .await
        // `TransientNotAccepted`, not `NotCommitted`: a read never entered a
        // pipeline, so it is safe to re-issue anywhere, and it is the code that
        // drives the SDK's roster walk rather than a replay against the same
        // durably lagging replica.
        .map_err(|FrontierUnreached| {
            shard.metrics().record_metadata_read_frontier_refusal();
            debug!(
                frontier = frontier.get(),
                watermark,
                ?budget,
                "metadata read frontier unreached inside the budget; failing the read retryable"
            );
            IggyError::TransientNotAccepted
        })
}

/// Authorize a metadata read, then hold it for this node's applied frontier.
///
/// Authorization first: a denial is terminal, and parking the connection for
/// the whole budget before answering one buys nothing. Then again on a wait
/// that parked -- the rule resolves its scope and the caller's grants off the
/// state machine, and a park is exactly the case where both moved under it.
#[allow(clippy::future_not_send)]
async fn authorize_and_hold_read<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    code: u32,
    watermark: u64,
    authorize: impl Fn() -> Result<(), IggyError>,
) -> Result<(), IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    authorize()?;
    if !read_needs_metadata_frontier(code) {
        return Ok(());
    }
    if await_metadata_read_frontier(shard, watermark).await? == FrontierWait::CaughtUp {
        authorize()?;
    }
    Ok(())
}

#[allow(clippy::future_not_send, clippy::too_many_lines)]
pub(in crate::dispatch) async fn handle_non_replicated_request<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    sessions: &Rc<RefCell<SessionManager>>,
    system_config: &Arc<ServerSystemConfig>,
    transport_client_id: u128,
    request: Message<RoutedRequestHeader>,
    // Acting user, peer address and read-your-writes floor for the read gates
    // below, resolved by the funnel in the same connection lookup as the
    // heartbeat. `user_id` is `None` only on the pre-auth path (PING), which
    // serves ungated codes; the gated arms fail closed on it. An unknown
    // connection arrives with a floor of `0`: it was promised nothing, so its
    // reads wait for nothing.
    (user_id, client_address, watermark): (Option<u32>, Option<SocketAddr>, u64),
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    const CODE_RANGE: std::ops::Range<usize> = 0..4;
    let code = u32::from_le_bytes(request.header().reserved[CODE_RANGE].try_into().unwrap());
    match code {
        PING_CODE => {
            // No `record_heartbeat` here: the funnel records one for EVERY
            // frame before classification, so a ping is already covered.
            let commit = current_metadata_commit(shard);
            let reply = build_empty_reply(
                request.header(),
                request.header().client,
                request.header().session,
                commit,
            );
            send_host_frame(
                &shard.bus,
                transport_client_id,
                reply.into_generic().into_frozen(),
                FrameChannel::Reply,
                "ping_reply",
            )
            .await;
        }
        GET_ME_CODE => {
            // Self-scoped, so no permissioner rule -- but the consumer-group
            // list it carries is read off the streams STM, so it is gated like
            // any other metadata read.
            if let Err(error) = authorize_and_hold_read(shard, code, watermark, || Ok(())).await {
                send_non_replicated_deny(shard, &request, transport_client_id, error.as_code())
                    .await;
                return;
            }
            handle_get_me(shard, sessions, transport_client_id, &request).await;
        }
        GET_PERSONAL_ACCESS_TOKENS_CODE => {
            if let Err(error) = authorize_and_hold_read(shard, code, watermark, || Ok(())).await {
                send_non_replicated_deny(shard, &request, transport_client_id, error.as_code())
                    .await;
                return;
            }
            handle_get_personal_access_tokens(shard, sessions, transport_client_id, &request).await;
        }
        GET_CLIENTS_CODE => {
            if let Err(error) = authorize_and_hold_read(shard, code, watermark, || {
                authorize_uid(shard, user_id, Permissioner::get_clients)
            })
            .await
            {
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
                FrameChannel::Reply,
                "get_clients",
            )
            .await;
        }
        GET_CLIENT_CODE => {
            if let Err(error) = authorize_and_hold_read(shard, code, watermark, || {
                authorize_uid(shard, user_id, Permissioner::get_client)
            })
            .await
            {
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
            send_non_replicated_bytes(
                shard,
                &request,
                transport_client_id,
                bytes,
                FrameChannel::Reply,
                "get_client",
            )
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
            // header client id, so it carries no permissioner rule. The
            // assignment itself is metadata-STM state, hence the gate.
            if let Err(error) = authorize_and_hold_read(shard, code, watermark, || Ok(())).await {
                send_non_replicated_deny(shard, &request, transport_client_id, error.as_code())
                    .await;
                return;
            }
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
                watermark,
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
    watermark: u64,
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
    // separately); a denial replies status!=0 with an empty body. The
    // read-your-writes hold sits INSIDE the same call, behind that denial: an
    // unauthorized read must fail now, not after the whole poll budget.
    if let Err(error) = authorize_and_hold_read(shard, code, watermark, || {
        authorize_default_read(shard, code, request_body(request), user_id)
    })
    .await
    {
        // Same line as the builder-`Err` branch below: `send_non_replicated_deny`
        // logs only on send FAILURE, so a refusal that reaches the client would
        // otherwise leave nothing server-side - including the refusal this gate
        // now issues for every armless or unknown non-replicated code.
        // `error` separates an authz denial from the frontier wait's own
        // `TransientNotAccepted`, which reaches here through the same `Err`.
        warn!(
            transport_client_id,
            code,
            error = %error,
            "denying non-replicated VSR request"
        );
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
            send_host_frame(
                &shard.bus,
                transport_client_id,
                reply.into_generic().into_frozen(),
                FrameChannel::Reply,
                "non_replicated_reply",
            )
            .await;
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
                FrameChannel::Reply,
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
        FrameChannel::Reply,
        "sync_consumer_group",
    )
    .await;
}

#[cfg(test)]
mod tests {
    use super::{
        FrontierUnreached, FrontierWait, hold_for_frontier, read_frontier_budget,
        read_needs_metadata_frontier,
    };
    use configs::server::ServerConfig;
    use iggy_binary_protocol::codes::{
        DESCRIBE_OPTIONS_CODE, GET_CLUSTER_METADATA_CODE, GET_CONSUMER_OFFSET_CODE, GET_ME_CODE,
        GET_SNAPSHOT_FILE_CODE, GET_STREAM_CODE, PING_CODE, POLL_MESSAGES_CODE,
        SYNC_CONSUMER_GROUP_CODE,
    };
    use iggy_common::IggyDuration;
    use metadata::AppliedFrontier;
    use std::future::pending;
    use std::sync::Arc;
    use std::time::Duration;

    /// The budget has to move with the CONFIGURED commit cadence, not with the
    /// compile-time default: `[cluster] commit_broadcast_interval` is what
    /// sizes the timer the read is waiting on, and a cluster that widens it to
    /// 2s would otherwise get a budget of one and a half broadcasts and refuse
    /// reads on a backup that is merely a commit behind.
    ///
    /// The default arm also pins the fallback the simulator and the unit
    /// fixtures run on, so the two cannot drift apart silently.
    #[test]
    fn given_a_configured_commit_cadence_when_sizing_the_budget_should_scale_with_it() {
        let mut config = ServerConfig::default();
        assert_eq!(
            read_frontier_budget(&config),
            AppliedFrontier::DEFAULT_READ_BUDGET,
            "the config default must agree with the frontier's built-in fallback"
        );

        config.cluster.commit_broadcast_interval = IggyDuration::from(Duration::from_secs(2));
        assert_eq!(
            read_frontier_budget(&config),
            Duration::from_secs(12),
            "six broadcasts of the configured interval"
        );
    }

    /// A caller with nothing to read back (`watermark == 0`) and one whose
    /// watermark this node has already applied are the whole steady state, and
    /// neither may cost a park: no registration, no await. The budget here is
    /// a future that never completes, so a gate that parked would hang instead
    /// of quietly costing a tick.
    #[compio::test]
    async fn given_a_frontier_at_the_watermark_when_gating_should_serve_without_parking() {
        let frontier = AppliedFrontier::default();
        frontier.advance(9);
        for watermark in [0, 7, 9] {
            assert_eq!(
                hold_for_frontier(&frontier, watermark, pending()).await,
                Ok(FrontierWait::Ready),
                "frontier 9 covers {watermark}, so the read must not park"
            );
        }
        assert_eq!(frontier.waiting(), 0, "a served read registers no wait");
    }

    /// The gate's whole point, and the reason the wait is event-driven: a read
    /// whose caller was told op 9 committed is held while this node is at 4,
    /// and the COMMIT that advances the frontier is what answers it - here a
    /// detached task standing in for the commit path, with no timer in the
    /// budget at all. The parked outcome is what tells the caller to re-run
    /// the authorization it resolved off the pre-wait state machine.
    #[compio::test]
    async fn given_a_frontier_behind_the_watermark_when_it_advances_should_answer_the_held_read() {
        let frontier = Arc::new(AppliedFrontier::default());
        frontier.advance(4);
        let committer = Arc::clone(&frontier);
        compio::runtime::spawn(async move {
            // Yields first, so the read is provably parked before the advance:
            // a gate that answered off the lagging frontier would already have
            // returned by the time this runs.
            compio::runtime::time::sleep(std::time::Duration::ZERO).await;
            committer.advance(9);
        })
        .detach();

        assert_eq!(
            hold_for_frontier(&frontier, 9, pending()).await,
            Ok(FrontierWait::CaughtUp),
            "the commit that closed the gap must answer the held read"
        );
        assert_eq!(frontier.waiting(), 0, "the answered wait deregisters");
    }

    /// A node can legitimately never catch up (a durably lagging replica), so
    /// the wait is bounded - and the exit is a refusal, never the stale answer.
    /// Each plane renders it retryable: `TransientNotAccepted` on the binary
    /// transports, the shared 503 over HTTP.
    #[compio::test]
    async fn given_a_frontier_that_never_catches_up_when_the_budget_expires_should_fail_retryable()
    {
        let frontier = AppliedFrontier::default();
        frontier.advance(4);
        assert_eq!(
            hold_for_frontier(&frontier, 9, std::future::ready(())).await,
            Err(FrontierUnreached),
            "an unreached frontier must refuse the read, not serve it"
        );
        assert_eq!(
            frontier.waiting(),
            0,
            "the expired wait must not leave its waker behind"
        );
    }

    /// The deny-list's whole point is that a read answered from the metadata
    /// STM is gated even when nobody remembered to name it, so the arms that
    /// are NOT gated are the ones worth pinning: the static catalog, the
    /// roster read on the leader-discovery path, and a code this build cannot
    /// serve at all (whose only outcome is `InvalidCommand`, which must not
    /// wait out the budget first).
    #[test]
    fn given_a_read_code_when_classified_should_gate_all_but_the_named_exclusions() {
        for code in [GET_STREAM_CODE, GET_ME_CODE, SYNC_CONSUMER_GROUP_CODE] {
            assert!(
                read_needs_metadata_frontier(code),
                "code {code} answers from the metadata STM and must be gated"
            );
        }
        for code in [
            PING_CODE,
            DESCRIBE_OPTIONS_CODE,
            GET_CLUSTER_METADATA_CODE,
            POLL_MESSAGES_CODE,
            GET_CONSUMER_OFFSET_CODE,
            GET_SNAPSHOT_FILE_CODE,
        ] {
            assert!(
                !read_needs_metadata_frontier(code),
                "code {code} has no metadata-STM answer to hold; the arms that skip \
                 the gate are exactly these"
            );
        }
        assert!(
            !read_needs_metadata_frontier(u32::MAX),
            "an unknown code has no answer to hold, so it must not park"
        );
    }
}
