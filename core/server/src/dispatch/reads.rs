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
use crate::dispatch::authz::{authorize_default_read, authorize_uid, send_non_replicated_deny};
use crate::dispatch::partition::{handle_get_consumer_offset, handle_poll_messages};
use crate::dispatch::send_non_replicated_bytes;
use crate::responses::{
    build_empty_reply, build_get_me_response, build_get_personal_access_tokens_response,
    build_non_replicated_response, connected_client_to_response, current_metadata_commit,
};
use crate::session_manager::SessionManager;
use crate::shell::{ShellBus, ShellShard};
use crate::snapshot;
use crate::wire::request_body;
use bytes::Bytes;
use configs::server::ServerSystemConfig;
use consensus::MetadataHandle;
use iggy_binary_protocol::PrepareHeader;
use iggy_binary_protocol::codes::{
    GET_CLIENT_CODE, GET_CLIENTS_CODE, GET_CONSUMER_OFFSET_CODE, GET_ME_CODE,
    GET_PERSONAL_ACCESS_TOKENS_CODE, GET_SNAPSHOT_FILE_CODE, GET_STATS_CODE, PING_CODE,
    POLL_MESSAGES_CODE, SYNC_CONSUMER_GROUP_CODE,
};
use iggy_binary_protocol::requests::consumer_groups::SyncConsumerGroupRequest;
use iggy_binary_protocol::requests::system::get_client::GetClientRequest;
use iggy_binary_protocol::requests::system::get_snapshot::GetSnapshotRequest;
use iggy_binary_protocol::responses::clients::client_response::ConsumerGroupInfoResponse;
use iggy_binary_protocol::responses::clients::get_client::ClientDetailsResponse;
use iggy_binary_protocol::responses::clients::get_clients::GetClientsResponse;
use iggy_binary_protocol::responses::consumer_groups::SyncConsumerGroupResponse;
use iggy_binary_protocol::responses::system::get_snapshot::GetSnapshotResponse;
use iggy_binary_protocol::{HEADER_SIZE, RoutedRequestHeader, WireDecode, WireEncode};
use iggy_common::{IggyError, Permissions, SnapshotCompression, SystemSnapshotType};
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use message_bus::framing::MAX_MESSAGE_SIZE;
use metadata::impls::metadata::StreamsFrontend;
use metadata::permissioner::Permissioner;
use server_common::Message;
use std::cell::RefCell;
use std::net::IpAddr;
use std::rc::Rc;
use std::sync::Arc;
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

#[allow(clippy::future_not_send, clippy::too_many_lines)]
pub(in crate::dispatch) async fn handle_non_replicated_request<B, MJ, S, SB>(
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
    let session_perms = user_id
        .filter(|&uid| crate::external_auth::is_synthetic_user_id(uid))
        .and_then(|uid| sessions.borrow().session_permissions_for_user(uid).cloned());
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
            if let Err(error) = authorize_uid(
                shard,
                user_id,
                Permissioner::get_clients,
                session_perms.as_ref(),
                |p| p.global.manage_servers || p.global.read_servers,
            ) {
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
            if let Err(error) = authorize_uid(
                shard,
                user_id,
                Permissioner::get_client,
                session_perms.as_ref(),
                |p| p.global.manage_servers || p.global.read_servers,
            ) {
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
            handle_get_snapshot(
                shard,
                system_config,
                transport_client_id,
                &request,
                user_id,
                session_perms.as_ref(),
            )
            .await;
        }
        POLL_MESSAGES_CODE => {
            handle_poll_messages(
                shard,
                transport_client_id,
                &request,
                user_id,
                session_perms.as_ref(),
            )
            .await;
        }
        GET_CONSUMER_OFFSET_CODE => {
            handle_get_consumer_offset(
                shard,
                transport_client_id,
                &request,
                user_id,
                session_perms.as_ref(),
            )
            .await;
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
                session_perms.as_ref(),
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
    session_perms: Option<&Permissions>,
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
    if let Err(error) =
        authorize_default_read(shard, code, request_body(request), user_id, session_perms)
    {
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
    session_perms: Option<&Permissions>,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    if let Err(error) = authorize_uid(
        shard,
        user_id,
        Permissioner::get_snapshot,
        session_perms,
        |p| p.global.manage_servers || p.global.read_servers,
    ) {
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
