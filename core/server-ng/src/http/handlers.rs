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

//! The shard-0 REST route handlers: health, login/logout, the metadata reads,
//! the control-plane writes, and the data-plane produce / poll / consumer-offset
//! routes the router binds.

use std::sync::Arc;

use axum::Json;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use chrono::Local;
use consensus::{MetadataHandle, PartitionsHandle};
use iggy_binary_protocol::codes::{
    GET_CONSUMER_GROUP_CODE, GET_CONSUMER_GROUPS_CODE, GET_PERSONAL_ACCESS_TOKENS_CODE,
    GET_STATS_CODE, GET_STREAM_CODE, GET_STREAMS_CODE, GET_TOPIC_CODE, GET_TOPICS_CODE,
    GET_USER_CODE, GET_USERS_CODE,
};
use iggy_binary_protocol::requests::consumer_groups::{
    CreateConsumerGroupRequest, DeleteConsumerGroupRequest, GetConsumerGroupRequest,
    GetConsumerGroupsRequest,
};
use iggy_binary_protocol::requests::partitions::{
    CreatePartitionsRequest, DeletePartitionsRequest,
};
use iggy_binary_protocol::requests::personal_access_tokens::{
    CreatePersonalAccessTokenRequest, DeletePersonalAccessTokenRequest,
    GetPersonalAccessTokensRequest,
};
use iggy_binary_protocol::requests::segments::DeleteSegmentsRequest;
use iggy_binary_protocol::requests::streams::{
    CreateStreamRequest, DeleteStreamRequest, GetStreamRequest, GetStreamsRequest,
    PurgeStreamRequest, UpdateStreamRequest,
};
use iggy_binary_protocol::requests::system::GetStatsRequest;
use iggy_binary_protocol::requests::topics::{
    CreateTopicRequest, DeleteTopicRequest, GetTopicRequest, GetTopicsRequest, PurgeTopicRequest,
    UpdateTopicRequest,
};
use iggy_binary_protocol::requests::users::{
    ChangePasswordRequest, CreateUserRequest, DeleteUserRequest, GetUserRequest, GetUsersRequest,
    UpdatePermissionsRequest, UpdateUserRequest,
};
use iggy_binary_protocol::responses::clients::client_response::ConsumerGroupInfoResponse;
use iggy_binary_protocol::responses::clients::get_client::ClientDetailsResponse;
use iggy_binary_protocol::responses::clients::get_clients::GetClientsResponse;
use iggy_binary_protocol::responses::consumer_groups::get_consumer_group::ConsumerGroupDetailsResponse;
use iggy_binary_protocol::responses::consumer_groups::get_consumer_groups::GetConsumerGroupsResponse;
use iggy_binary_protocol::responses::personal_access_tokens::GetPersonalAccessTokensResponse;
use iggy_binary_protocol::responses::streams::get_stream::GetStreamResponse;
use iggy_binary_protocol::responses::streams::get_streams::GetStreamsResponse;
use iggy_binary_protocol::responses::system::get_stats::StatsResponse;
use iggy_binary_protocol::responses::topics::get_topic::GetTopicResponse;
use iggy_binary_protocol::responses::topics::get_topics::GetTopicsResponse;
use iggy_binary_protocol::responses::users::get_user::UserDetailsResponse;
use iggy_binary_protocol::responses::users::get_users::GetUsersResponse;
use iggy_binary_protocol::{Operation, WireDecode, WireEncode, WireIdentifier, WireName};
use iggy_common::change_password::ChangePassword;
use iggy_common::create_consumer_group::CreateConsumerGroup;
use iggy_common::create_partitions::CreatePartitions;
use iggy_common::create_personal_access_token::CreatePersonalAccessToken;
use iggy_common::create_stream::CreateStream;
use iggy_common::create_topic::CreateTopic;
use iggy_common::create_user::CreateUser;
use iggy_common::delete_consumer_offset::DeleteConsumerOffset;
use iggy_common::delete_partitions::DeletePartitions;
use iggy_common::delete_segments::DeleteSegments;
use iggy_common::get_consumer_offset::GetConsumerOffset;
use iggy_common::get_snapshot::GetSnapshot;
use iggy_common::login_user::LoginUser;
use iggy_common::login_with_personal_access_token::LoginWithPersonalAccessToken;
use iggy_common::store_consumer_offset::StoreConsumerOffset;
use iggy_common::update_permissions::UpdatePermissions;
use iggy_common::update_stream::UpdateStream;
use iggy_common::update_topic::UpdateTopic;
use iggy_common::update_user::UpdateUser;
use iggy_common::wire_conversions::{
    clients_from_wire, consumer_groups_from_wire, identifier_to_wire, permissions_to_wire,
    personal_access_tokens_from_wire, streams_from_wire, topics_from_wire, users_from_wire,
};
use iggy_common::{
    ClientInfo, ClientInfoDetails, ClusterMetadata, Consumer, ConsumerGroup, ConsumerGroupDetails,
    ConsumerOffsetInfo, Identifier, IdentityInfo, IggyError, PersonalAccessTokenInfo, PollMessages,
    PolledMessages, RawPersonalAccessToken, SendMessages, Stats, Stream, StreamDetails, TokenInfo,
    Topic, TopicDetails, UserInfo, UserInfoDetails, Validatable,
};
use metadata::impls::metadata::StreamsFrontend;
use metadata::permissioner::Permissioner;
use secrecy::ExposeSecret;
use send_wrapper::SendWrapper;
use serde::Deserialize;
use shard::{PartitionRead, PartitionReadReply};

use crate::auth::{verify_login_credentials, verify_pat_credentials};
use crate::dispatch::{resolve_consumer_offset_request, resolve_poll_request};
use crate::http::error::{
    ConsistencyQuery, CustomError, PartitionWriteError, ProduceAck, ProduceQuery, ReadError,
    WriteError,
};
use crate::http::extractor::{Authenticated, Identity};
use crate::http::reads::{
    authorize_data_plane, authorize_read, read_local, resolve_gate_stream, resolve_gate_topic,
    resolve_gate_topic_ids,
};
use crate::http::reply::{
    committed_payload, decode_consumer_group_details, decode_raw_pat_token, decode_stream_details,
    decode_topic_details, decode_user_details, login_error_to_iggy,
};
use crate::http::state::{HttpInner, HttpState};
use crate::http::submit::{
    logout_session, partition_write_replicated, produce_unacked, submit_committed, submit_write,
};
use crate::http::wire::{
    consumer_offset_wire_request, delete_offset_wire_request, encode_send_messages,
    poll_wire_request, resync_required_polled_messages, store_offset_wire_request,
};
use crate::responses::{
    build_polled_messages_body, build_raw_pat_reply, connected_client_to_response,
};
use crate::snapshot;

/// `GET /ping` response body, matching the legacy HTTP server's health probe.
const PONG: &str = "pong";

/// VSR client id stamped on HTTP data-plane reads (poll / consumer-offset).
/// HTTP reads never Register a VSR client, and shard-0 client ids are minted
/// from 1, so 0 can never name a live consumer-group member: a group-kind
/// poll fences closed with `ConsumerGroupPartitionNotOwned` and answers the
/// re-sync sentinel empty poll - the same failure a stale TCP member sees.
/// Legacy HTTP polls with client id 0 for the same reason (no persistent
/// sessions, so no group membership).
const HTTP_READ_CLIENT_ID: u128 = 0;

/// Response header attesting what durability a produce response proves:
/// [`DURABILITY_REPLICATED_MEMORY`] after an awaited quorum commit,
/// [`DURABILITY_NONE`] for a `?ack=none` fire-and-forget.
const DURABILITY_HEADER: HeaderName = HeaderName::from_static("x-iggy-durability");

const DURABILITY_REPLICATED_MEMORY: &str = "replicated-memory";

const DURABILITY_NONE: &str = "none";

/// `{user_id}` segment alias resolving to the caller in `GET /users/{user_id}`.
/// Handled inside the handler rather than as a static `/users/me` route so it
/// cannot shadow `{user_id}` matching (a user literally named "me" stays
/// reachable by numeric id).
const CURRENT_USER_ALIAS: &str = "me";

/// Extracting the state here proves at compile time that the `!Send` state
/// bridges into axum's `Send + Sync` router state on shard 0's compio thread.
/// Ping needs no state, so it is discarded.
pub(in crate::http) async fn ping(State(_state): State<HttpState>) -> &'static str {
    PONG
}

pub(in crate::http) async fn login_user(
    State(state): State<HttpState>,
    Json(command): Json<LoginUser>,
) -> Result<Json<IdentityInfo>, CustomError> {
    // Credential verification is a consensus-free STM read; hold it while a
    // recovered WAL suffix (which may carry the user's create/password ops)
    // re-commits, like every other local read.
    SendWrapper::new(crate::http::reads::await_recovery_barrier(&state.shard)).await;
    let user_id = verify_login_credentials(
        &state.shard,
        &command.username,
        command.password.expose_secret(),
    )
    .map_err(|error| login_error_to_iggy(&error))?;
    issue_identity(&state, user_id)
}

pub(in crate::http) async fn login_with_personal_access_token(
    State(state): State<HttpState>,
    Json(command): Json<LoginWithPersonalAccessToken>,
) -> Result<Json<IdentityInfo>, CustomError> {
    SendWrapper::new(crate::http::reads::await_recovery_barrier(&state.shard)).await;
    let user_id = verify_pat_credentials(&state.shard, command.token.expose_secret())
        .map_err(|error| login_error_to_iggy(&error))?;
    issue_identity(&state, user_id)
}

/// `POST /users/refresh-token` body. The route is unauthenticated, so the
/// caller's current access token travels in the body, not a bearer header.
#[derive(Debug, Deserialize)]
pub(in crate::http) struct RefreshToken {
    token: String,
}

/// `POST /users/refresh-token`: re-issue an access token from a still-valid one,
/// answering the same `IdentityInfo` shape as login.
///
/// Stateless by design: server-ng has no replicated revocation list (the P3
/// roadmap item), so refreshing cannot invalidate the presented token - it stays
/// valid until its own `exp`, the same posture as logout ending a session
/// without revoking its bearer. Per-node revocation would be false security in a
/// cluster, so legacy's one-shot revoke-old-jti behavior is deliberately dropped
/// rather than ported.
pub(in crate::http) async fn refresh_token(
    State(state): State<HttpState>,
    Json(command): Json<RefreshToken>,
) -> Result<Json<IdentityInfo>, CustomError> {
    if command.token.is_empty() {
        return Err(IggyError::Unauthenticated.into());
    }
    let claims = state.jwt.decode(&command.token)?;
    let user_id = claims
        .sub
        .parse::<u32>()
        .map_err(|_| IggyError::Unauthenticated)?;
    issue_identity(&state, user_id)
}

/// `DELETE /users/logout`: end the caller's session and answer 204 - the status
/// the SDK's `logout_user()` awaits before dropping its stored bearer. The
/// teardown itself is [`logout_session`]; it is best-effort, so once the caller
/// authenticates this always answers 204.
pub(in crate::http) async fn logout_user(
    State(state): State<HttpState>,
    identity: Authenticated,
) -> StatusCode {
    // `logout_session` is `!Send`; bridge it like every other write path on this
    // shard-0 listener (see [`delete_user`]).
    SendWrapper::new(logout_session(&state, &identity.session)).await;
    StatusCode::NO_CONTENT
}

/// `GET /streams`: list every stream as the same `Vec<Stream>` JSON the legacy
/// server returns. A consensus-free local STM read via [`read_local`].
pub(in crate::http) async fn get_streams(
    State(state): State<HttpState>,
    identity: Identity,
    Query(query): Query<ConsistencyQuery>,
) -> Result<Json<Vec<Stream>>, ReadError> {
    let body = GetStreamsRequest.to_bytes();
    let bytes = SendWrapper::new(read_local(
        &state,
        &identity,
        query.consistency,
        GET_STREAMS_CODE,
        &body,
        Permissioner::get_streams,
    ))
    .await?;
    let response = GetStreamsResponse::decode_from(&bytes)
        .map_err(|_| ReadError::Rejected(IggyError::InvalidCommand))?;
    Ok(Json(streams_from_wire(response)))
}

/// `GET /streams/{stream_id}`: fetch one stream by numeric id or name as the
/// same `StreamDetails` JSON the legacy server returns; 404 when absent. A
/// consensus-free local STM read via [`read_local`].
pub(in crate::http) async fn get_stream(
    State(state): State<HttpState>,
    identity: Identity,
    Path(stream_id): Path<String>,
    Query(query): Query<ConsistencyQuery>,
) -> Result<Json<StreamDetails>, ReadError> {
    let stream_id = Identifier::from_str_value(&stream_id).map_err(ReadError::Rejected)?;
    let wire_stream_id = identifier_to_wire(&stream_id).map_err(ReadError::Rejected)?;
    // Resolve for the gate; a miss leaves it a pass-through so the read renders
    // the existing 404 rather than a 403.
    let scope = resolve_gate_stream(&state, &wire_stream_id);
    let request = GetStreamRequest {
        stream_id: wire_stream_id,
    };
    let body = request.to_bytes();
    let bytes = SendWrapper::new(read_local(
        &state,
        &identity,
        query.consistency,
        GET_STREAM_CODE,
        &body,
        |permissioner, uid| {
            scope.map_or(Ok(()), |stream_id| permissioner.get_stream(uid, stream_id))
        },
    ))
    .await?;
    let response = GetStreamResponse::decode_from(&bytes)
        .map_err(|_| ReadError::Rejected(IggyError::InvalidCommand))?;
    Ok(Json(
        StreamDetails::try_from(response).map_err(ReadError::Rejected)?,
    ))
}

/// `GET /streams/{stream_id}/topics`: list a stream's topics as the same
/// `Vec<Topic>` JSON the legacy server returns. A consensus-free local STM read
/// via [`read_local`].
pub(in crate::http) async fn get_topics(
    State(state): State<HttpState>,
    identity: Identity,
    Path(stream_id): Path<String>,
    Query(query): Query<ConsistencyQuery>,
) -> Result<Json<Vec<Topic>>, ReadError> {
    let stream_id = Identifier::from_str_value(&stream_id).map_err(ReadError::Rejected)?;
    let wire_stream_id = identifier_to_wire(&stream_id).map_err(ReadError::Rejected)?;
    let scope = resolve_gate_stream(&state, &wire_stream_id);
    let request = GetTopicsRequest {
        stream_id: wire_stream_id,
    };
    let body = request.to_bytes();
    let bytes = SendWrapper::new(read_local(
        &state,
        &identity,
        query.consistency,
        GET_TOPICS_CODE,
        &body,
        |permissioner, uid| {
            scope.map_or(Ok(()), |stream_id| permissioner.get_topics(uid, stream_id))
        },
    ))
    .await?;
    let response = GetTopicsResponse::decode_from(&bytes)
        .map_err(|_| ReadError::Rejected(IggyError::InvalidCommand))?;
    Ok(Json(
        topics_from_wire(response).map_err(ReadError::Rejected)?,
    ))
}

/// `GET /streams/{stream_id}/topics/{topic_id}`: fetch one topic by numeric id
/// or name as the same `TopicDetails` JSON the legacy server returns; 404 when
/// absent. A consensus-free local STM read via [`read_local`].
pub(in crate::http) async fn get_topic(
    State(state): State<HttpState>,
    identity: Identity,
    Path((stream_id, topic_id)): Path<(String, String)>,
    Query(query): Query<ConsistencyQuery>,
) -> Result<Json<TopicDetails>, ReadError> {
    let stream_id = Identifier::from_str_value(&stream_id).map_err(ReadError::Rejected)?;
    let topic_id = Identifier::from_str_value(&topic_id).map_err(ReadError::Rejected)?;
    let wire_stream_id = identifier_to_wire(&stream_id).map_err(ReadError::Rejected)?;
    let wire_topic_id = identifier_to_wire(&topic_id).map_err(ReadError::Rejected)?;
    let scope = resolve_gate_topic(&state, &wire_stream_id, &wire_topic_id);
    let request = GetTopicRequest {
        stream_id: wire_stream_id,
        topic_id: wire_topic_id,
    };
    let body = request.to_bytes();
    let bytes = SendWrapper::new(read_local(
        &state,
        &identity,
        query.consistency,
        GET_TOPIC_CODE,
        &body,
        |permissioner, uid| {
            scope.map_or(Ok(()), |(stream_id, topic_id)| {
                permissioner.get_topic(uid, stream_id, topic_id)
            })
        },
    ))
    .await?;
    let response = GetTopicResponse::decode_from(&bytes)
        .map_err(|_| ReadError::Rejected(IggyError::InvalidCommand))?;
    Ok(Json(
        TopicDetails::try_from(response).map_err(ReadError::Rejected)?,
    ))
}

/// `GET /users`: list every user as the same `Vec<UserInfo>` JSON the legacy
/// server returns. A consensus-free local STM read via [`read_local`].
pub(in crate::http) async fn get_users(
    State(state): State<HttpState>,
    identity: Identity,
    Query(query): Query<ConsistencyQuery>,
) -> Result<Json<Vec<UserInfo>>, ReadError> {
    let body = GetUsersRequest.to_bytes();
    let bytes = SendWrapper::new(read_local(
        &state,
        &identity,
        query.consistency,
        GET_USERS_CODE,
        &body,
        Permissioner::get_users,
    ))
    .await?;
    let response = GetUsersResponse::decode_from(&bytes)
        .map_err(|_| ReadError::Rejected(IggyError::InvalidCommand))?;
    Ok(Json(
        users_from_wire(response).map_err(ReadError::Rejected)?,
    ))
}

/// `GET /users/{user_id}`: fetch one user by numeric id or name as the same
/// `UserInfoDetails` JSON the legacy server returns; 404 when absent. A
/// consensus-free local STM read via [`read_local`]. [`CURRENT_USER_ALIAS`]
/// resolves to the caller and skips the `read_users` gate, mirroring the
/// legacy self-read bypass.
pub(in crate::http) async fn get_user(
    State(state): State<HttpState>,
    identity: Identity,
    Path(user_id): Path<String>,
    Query(query): Query<ConsistencyQuery>,
) -> Result<Json<UserInfoDetails>, ReadError> {
    let is_self = user_id == CURRENT_USER_ALIAS;
    let wire_user_id = if is_self {
        WireIdentifier::numeric(identity.user_id)
    } else {
        let user_id = Identifier::from_str_value(&user_id).map_err(ReadError::Rejected)?;
        identifier_to_wire(&user_id).map_err(ReadError::Rejected)?
    };
    let request = GetUserRequest {
        user_id: wire_user_id,
    };
    let body = request.to_bytes();
    let bytes = SendWrapper::new(read_local(
        &state,
        &identity,
        query.consistency,
        GET_USER_CODE,
        &body,
        |permissioner, uid| {
            if is_self {
                Ok(())
            } else {
                permissioner.get_user(uid)
            }
        },
    ))
    .await?;
    let response = UserDetailsResponse::decode_from(&bytes)
        .map_err(|_| ReadError::Rejected(IggyError::InvalidCommand))?;
    Ok(Json(
        UserInfoDetails::try_from(response).map_err(ReadError::Rejected)?,
    ))
}

/// `GET /streams/{stream_id}/topics/{topic_id}/consumer-groups`: list a topic's
/// consumer groups as the same `Vec<ConsumerGroup>` JSON the legacy server
/// returns. A consensus-free local STM read via [`read_local`].
pub(in crate::http) async fn get_cgs(
    State(state): State<HttpState>,
    identity: Identity,
    Path((stream_id, topic_id)): Path<(String, String)>,
    Query(query): Query<ConsistencyQuery>,
) -> Result<Json<Vec<ConsumerGroup>>, ReadError> {
    let stream_id = Identifier::from_str_value(&stream_id).map_err(ReadError::Rejected)?;
    let topic_id = Identifier::from_str_value(&topic_id).map_err(ReadError::Rejected)?;
    let wire_stream_id = identifier_to_wire(&stream_id).map_err(ReadError::Rejected)?;
    let wire_topic_id = identifier_to_wire(&topic_id).map_err(ReadError::Rejected)?;
    let scope = resolve_gate_topic(&state, &wire_stream_id, &wire_topic_id);
    let request = GetConsumerGroupsRequest {
        stream_id: wire_stream_id,
        topic_id: wire_topic_id,
    };
    let body = request.to_bytes();
    let bytes = SendWrapper::new(read_local(
        &state,
        &identity,
        query.consistency,
        GET_CONSUMER_GROUPS_CODE,
        &body,
        |permissioner, uid| {
            scope.map_or(Ok(()), |(stream_id, topic_id)| {
                permissioner.get_consumer_groups(uid, stream_id, topic_id)
            })
        },
    ))
    .await?;
    let response = GetConsumerGroupsResponse::decode_from(&bytes)
        .map_err(|_| ReadError::Rejected(IggyError::InvalidCommand))?;
    Ok(Json(consumer_groups_from_wire(response)))
}

/// `GET /streams/{stream_id}/topics/{topic_id}/consumer-groups/{group_id}`:
/// fetch one consumer group by numeric id or name as the same
/// `ConsumerGroupDetails` JSON the legacy server returns; 404 when absent. The
/// wire-to-domain conversion is infallible.
pub(in crate::http) async fn get_cg(
    State(state): State<HttpState>,
    identity: Identity,
    Path((stream_id, topic_id, group_id)): Path<(String, String, String)>,
    Query(query): Query<ConsistencyQuery>,
) -> Result<Json<ConsumerGroupDetails>, ReadError> {
    let stream_id = Identifier::from_str_value(&stream_id).map_err(ReadError::Rejected)?;
    let topic_id = Identifier::from_str_value(&topic_id).map_err(ReadError::Rejected)?;
    let group_id = Identifier::from_str_value(&group_id).map_err(ReadError::Rejected)?;
    let wire_stream_id = identifier_to_wire(&stream_id).map_err(ReadError::Rejected)?;
    let wire_topic_id = identifier_to_wire(&topic_id).map_err(ReadError::Rejected)?;
    let scope = resolve_gate_topic(&state, &wire_stream_id, &wire_topic_id);
    let request = GetConsumerGroupRequest {
        stream_id: wire_stream_id,
        topic_id: wire_topic_id,
        group_id: identifier_to_wire(&group_id).map_err(ReadError::Rejected)?,
    };
    let body = request.to_bytes();
    let bytes = SendWrapper::new(read_local(
        &state,
        &identity,
        query.consistency,
        GET_CONSUMER_GROUP_CODE,
        &body,
        |permissioner, uid| {
            scope.map_or(Ok(()), |(stream_id, topic_id)| {
                permissioner.get_consumer_group(uid, stream_id, topic_id)
            })
        },
    ))
    .await?;
    let response = ConsumerGroupDetailsResponse::decode_from(&bytes)
        .map_err(|_| ReadError::Rejected(IggyError::InvalidCommand))?;
    Ok(Json(ConsumerGroupDetails::from(response)))
}

/// `GET /stats`: server + storage counters as the same `Stats` JSON the legacy
/// server returns. `GET_STATS` is served by `build_non_replicated_response` like
/// the entity reads, so it flows through [`read_local`] unchanged rather than a
/// dedicated builder. No entity can be missing, so no 404 branch.
pub(in crate::http) async fn get_stats(
    State(state): State<HttpState>,
    identity: Identity,
    Query(query): Query<ConsistencyQuery>,
) -> Result<Json<Stats>, ReadError> {
    let body = GetStatsRequest.to_bytes();
    let bytes = SendWrapper::new(read_local(
        &state,
        &identity,
        query.consistency,
        GET_STATS_CODE,
        &body,
        Permissioner::get_stats,
    ))
    .await?;
    let response = StatsResponse::decode_from(&bytes)
        .map_err(|_| ReadError::Rejected(IggyError::InvalidCommand))?;
    Ok(Json(Stats::from(response)))
}

/// `POST /snapshot`: collect a diagnostic archive and return it as a ZIP
/// download with the same headers the legacy server sets.
///
/// Gated on the snapshot rule (`read_servers || manage_servers`) via the
/// shared [`authorize_read`] gate. Collection shells out to system tools on a
/// dedicated OS thread (see `snapshot::collect`); this handler only awaits the
/// result handoff, which is `Send`, so no `SendWrapper` bridge is needed.
pub(in crate::http) async fn get_snapshot(
    State(state): State<HttpState>,
    identity: Identity,
    Query(query): Query<ConsistencyQuery>,
    Json(command): Json<GetSnapshot>,
) -> Result<(HeaderMap, Body), ReadError> {
    authorize_read(&state, &identity, query.consistency, |permissioner, uid| {
        permissioner.get_snapshot(uid)
    })?;
    let archive = snapshot::collect(
        Arc::clone(&state.system_config),
        command.compression,
        command.snapshot_types,
    )
    .await
    .map_err(ReadError::Rejected)?;

    let filename = format!("iggy_snapshot_{}.zip", Local::now().format("%Y%m%d_%H%M%S"));
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/zip"),
    );
    // The formatted value is fixed ASCII (digits + underscores), but keep the
    // fallback total: a bare attachment still downloads correctly.
    let disposition = HeaderValue::from_str(&format!("attachment; filename=\"{filename}\""))
        .unwrap_or_else(|_| HeaderValue::from_static("attachment"));
    headers.insert(header::CONTENT_DISPOSITION, disposition);
    Ok((headers, Body::from(archive)))
}

/// `GET /cluster/metadata`: report the live cluster topology as the same
/// `ClusterMetadata` JSON the legacy server returns.
///
/// Auth-only: any valid token serves. Unlike the entity reads it bypasses both
/// the per-op authorization gate and the consistency gate, and serves from the
/// roster captured at listener start plus the sync consensus getters, so it
/// never touches the metadata STM, consensus, or a VSR session and stays fully
/// synchronous.
pub(in crate::http) async fn get_cluster_metadata(
    State(state): State<HttpState>,
    _identity: Identity,
) -> Json<ClusterMetadata> {
    Json(state.build_cluster_metadata())
}

/// `GET /clients`: list every connected client across all shards as the same
/// `Vec<ClientInfo>` JSON the legacy server returns.
///
/// Unlike the entity reads, connections live in each shard's session manager,
/// not the metadata STM, so this scatter-gathers over the shard mesh
/// (`list_all_clients`) instead of going through [`read_local`]. It still runs
/// the identical per-op + consistency gate via [`authorize_read`], so its
/// authorization matches every metadata read. The gather future is `!Send`,
/// bridged onto shard 0's thread by `SendWrapper` exactly as the write path
/// bridges its submit.
pub(in crate::http) async fn get_clients(
    State(state): State<HttpState>,
    identity: Identity,
    Query(query): Query<ConsistencyQuery>,
) -> Result<Json<Vec<ClientInfo>>, ReadError> {
    authorize_read(&state, &identity, query.consistency, |permissioner, uid| {
        permissioner.get_clients(uid)
    })?;
    let infos = SendWrapper::new(state.shard.list_all_clients()).await;
    let response = GetClientsResponse {
        clients: infos
            .iter()
            .map(|info| connected_client_to_response(&state.shard, info))
            .collect(),
    };
    Ok(Json(clients_from_wire(response)))
}

/// `GET /clients/{client_id}`: fetch one connected client as the same
/// `ClientInfoDetails` JSON the legacy server returns; 404 when absent.
///
/// The path id is the `u32` wire client id (the seq tail of the u128 transport
/// id), matching the legacy route's `Path<u32>`. There is no reverse map from
/// that id to a home shard, so this gathers every shard's clients and filters -
/// the same fan-out-and-filter as [`get_clients`] and the TCP `get_client`
/// dispatch. The wire-to-domain conversion is infallible.
pub(in crate::http) async fn get_client(
    State(state): State<HttpState>,
    identity: Identity,
    Path(client_id): Path<u32>,
    Query(query): Query<ConsistencyQuery>,
) -> Result<Json<ClientInfoDetails>, ReadError> {
    authorize_read(&state, &identity, query.consistency, |permissioner, uid| {
        permissioner.get_client(uid)
    })?;
    let infos = SendWrapper::new(state.shard.list_all_clients()).await;
    // The wire client id is the u32 seq tail of the u128 transport id.
    #[allow(clippy::cast_possible_truncation)]
    let info = infos
        .iter()
        .find(|info| info.client_id as u32 == client_id)
        .ok_or(ReadError::NotFound)?;
    let consumer_groups = info.vsr_client_id.map_or_else(Vec::new, |vsr_client_id| {
        state
            .shard
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
    let response = ClientDetailsResponse {
        client: connected_client_to_response(&state.shard, info),
        consumer_groups,
    };
    Ok(Json(ClientInfoDetails::from(response)))
}

/// `POST /streams`: create a stream and render the committed reply as the same
/// `StreamDetails` JSON the legacy server returns.
///
/// The accepted body is name-only (`{"name": ...}`), matching the legacy
/// request; server-ng's wire `CreateStreamRequest` is likewise name-only and
/// auto-assigns the id, so there is no client-supplied stream id to honor.
pub(in crate::http) async fn create_stream(
    State(state): State<HttpState>,
    identity: Authenticated,
    Json(command): Json<CreateStream>,
) -> Result<Json<StreamDetails>, WriteError> {
    let request = CreateStreamRequest {
        name: WireName::new(command.name)
            .map_err(|_| WriteError::Rejected(IggyError::InvalidStreamName))?,
    };
    let body = request.to_bytes();
    let payload = SendWrapper::new(submit_write(
        &state,
        &identity.session,
        Operation::CreateStream,
        &body,
    ))
    .await?;
    Ok(Json(decode_stream_details(&payload)?))
}

/// `PUT /streams/{stream_id}`: rename a stream. A committed write returns 204
/// with no body, matching the legacy server.
pub(in crate::http) async fn update_stream(
    State(state): State<HttpState>,
    identity: Authenticated,
    Path(stream_id): Path<String>,
    Json(command): Json<UpdateStream>,
) -> Result<StatusCode, WriteError> {
    let stream_id = Identifier::from_str_value(&stream_id).map_err(WriteError::Rejected)?;
    let request = UpdateStreamRequest {
        stream_id: identifier_to_wire(&stream_id).map_err(WriteError::Rejected)?,
        name: WireName::new(command.name)
            .map_err(|_| WriteError::Rejected(IggyError::InvalidStreamName))?,
    };
    let body = request.to_bytes();
    SendWrapper::new(submit_write(
        &state,
        &identity.session,
        Operation::UpdateStream,
        &body,
    ))
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `DELETE /streams/{stream_id}`: delete a stream. Returns 204 on commit.
pub(in crate::http) async fn delete_stream(
    State(state): State<HttpState>,
    identity: Authenticated,
    Path(stream_id): Path<String>,
) -> Result<StatusCode, WriteError> {
    let stream_id = Identifier::from_str_value(&stream_id).map_err(WriteError::Rejected)?;
    let request = DeleteStreamRequest {
        stream_id: identifier_to_wire(&stream_id).map_err(WriteError::Rejected)?,
    };
    let body = request.to_bytes();
    SendWrapper::new(submit_write(
        &state,
        &identity.session,
        Operation::DeleteStream,
        &body,
    ))
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `DELETE /streams/{stream_id}/purge`: drop a stream's messages. Returns 204.
pub(in crate::http) async fn purge_stream(
    State(state): State<HttpState>,
    identity: Authenticated,
    Path(stream_id): Path<String>,
) -> Result<StatusCode, WriteError> {
    let stream_id = Identifier::from_str_value(&stream_id).map_err(WriteError::Rejected)?;
    let request = PurgeStreamRequest {
        stream_id: identifier_to_wire(&stream_id).map_err(WriteError::Rejected)?,
    };
    let body = request.to_bytes();
    SendWrapper::new(submit_write(
        &state,
        &identity.session,
        Operation::PurgeStream,
        &body,
    ))
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /streams/{stream_id}/topics`: create a topic under a stream and render
/// the committed reply as the same `TopicDetails` JSON the legacy server returns.
///
/// The stream comes from the path; the JSON body carries the remaining fields.
/// The submitted op is a plain `CreateTopic`; the metadata owner allocates the
/// consensus group ids and rewrites it to `CreateTopicWithAssignments` before
/// replication, so this handler stays a pure submit-and-decode.
pub(in crate::http) async fn create_topic(
    State(state): State<HttpState>,
    identity: Authenticated,
    Path(stream_id): Path<String>,
    Json(command): Json<CreateTopic>,
) -> Result<Json<TopicDetails>, WriteError> {
    let stream_id = Identifier::from_str_value(&stream_id).map_err(WriteError::Rejected)?;
    // Rejects empty/oversized name, partitions_count > MAX, replication_factor == Some(0).
    command.validate().map_err(WriteError::Rejected)?;
    let request = CreateTopicRequest {
        stream_id: identifier_to_wire(&stream_id).map_err(WriteError::Rejected)?,
        partitions_count: command.partitions_count,
        compression_algorithm: command.compression_algorithm.as_code(),
        message_expiry: command.message_expiry.into(),
        max_topic_size: command.max_topic_size.into(),
        replication_factor: command.replication_factor.unwrap_or(0),
        name: WireName::new(command.name)
            .map_err(|_| WriteError::Rejected(IggyError::InvalidTopicName))?,
    };
    let body = request.to_bytes();
    let payload = SendWrapper::new(submit_write(
        &state,
        &identity.session,
        Operation::CreateTopic,
        &body,
    ))
    .await?;
    Ok(Json(decode_topic_details(&payload)?))
}

/// `PUT /streams/{stream_id}/topics/{topic_id}`: update a topic. Returns 204.
pub(in crate::http) async fn update_topic(
    State(state): State<HttpState>,
    identity: Authenticated,
    Path((stream_id, topic_id)): Path<(String, String)>,
    Json(command): Json<UpdateTopic>,
) -> Result<StatusCode, WriteError> {
    let stream_id = Identifier::from_str_value(&stream_id).map_err(WriteError::Rejected)?;
    let topic_id = Identifier::from_str_value(&topic_id).map_err(WriteError::Rejected)?;
    // Also rejects replication_factor == Some(0), which `WireName` cannot see.
    command.validate().map_err(WriteError::Rejected)?;
    let request = UpdateTopicRequest {
        stream_id: identifier_to_wire(&stream_id).map_err(WriteError::Rejected)?,
        topic_id: identifier_to_wire(&topic_id).map_err(WriteError::Rejected)?,
        compression_algorithm: command.compression_algorithm.as_code(),
        message_expiry: command.message_expiry.into(),
        max_topic_size: command.max_topic_size.into(),
        replication_factor: command.replication_factor.unwrap_or(0),
        name: WireName::new(command.name)
            .map_err(|_| WriteError::Rejected(IggyError::InvalidTopicName))?,
    };
    let body = request.to_bytes();
    SendWrapper::new(submit_write(
        &state,
        &identity.session,
        Operation::UpdateTopic,
        &body,
    ))
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `DELETE /streams/{stream_id}/topics/{topic_id}`: delete a topic. Returns 204.
pub(in crate::http) async fn delete_topic(
    State(state): State<HttpState>,
    identity: Authenticated,
    Path((stream_id, topic_id)): Path<(String, String)>,
) -> Result<StatusCode, WriteError> {
    let stream_id = Identifier::from_str_value(&stream_id).map_err(WriteError::Rejected)?;
    let topic_id = Identifier::from_str_value(&topic_id).map_err(WriteError::Rejected)?;
    let request = DeleteTopicRequest {
        stream_id: identifier_to_wire(&stream_id).map_err(WriteError::Rejected)?,
        topic_id: identifier_to_wire(&topic_id).map_err(WriteError::Rejected)?,
    };
    let body = request.to_bytes();
    SendWrapper::new(submit_write(
        &state,
        &identity.session,
        Operation::DeleteTopic,
        &body,
    ))
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `DELETE /streams/{stream_id}/topics/{topic_id}/purge`: drop a topic's
/// messages. Returns 204.
pub(in crate::http) async fn purge_topic(
    State(state): State<HttpState>,
    identity: Authenticated,
    Path((stream_id, topic_id)): Path<(String, String)>,
) -> Result<StatusCode, WriteError> {
    let stream_id = Identifier::from_str_value(&stream_id).map_err(WriteError::Rejected)?;
    let topic_id = Identifier::from_str_value(&topic_id).map_err(WriteError::Rejected)?;
    let request = PurgeTopicRequest {
        stream_id: identifier_to_wire(&stream_id).map_err(WriteError::Rejected)?,
        topic_id: identifier_to_wire(&topic_id).map_err(WriteError::Rejected)?,
    };
    let body = request.to_bytes();
    SendWrapper::new(submit_write(
        &state,
        &identity.session,
        Operation::PurgeTopic,
        &body,
    ))
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /streams/{stream_id}/topics/{topic_id}/partitions`: add partitions to a
/// topic. Returns 200 (this listener answers create routes 200, the
/// legacy-majority parity). `partitions_count` comes from the JSON body; stream
/// and topic from the path. RBAC is enforced in-apply on the metadata STM, like
/// the sibling topic writes.
pub(in crate::http) async fn create_partitions(
    State(state): State<HttpState>,
    identity: Authenticated,
    Path((stream_id, topic_id)): Path<(String, String)>,
    Json(command): Json<CreatePartitions>,
) -> Result<StatusCode, WriteError> {
    let stream_id = Identifier::from_str_value(&stream_id).map_err(WriteError::Rejected)?;
    let topic_id = Identifier::from_str_value(&topic_id).map_err(WriteError::Rejected)?;
    command.validate().map_err(WriteError::Rejected)?;
    let request = CreatePartitionsRequest {
        stream_id: identifier_to_wire(&stream_id).map_err(WriteError::Rejected)?,
        topic_id: identifier_to_wire(&topic_id).map_err(WriteError::Rejected)?,
        partitions_count: command.partitions_count,
    };
    let body = request.to_bytes();
    SendWrapper::new(submit_write(
        &state,
        &identity.session,
        Operation::CreatePartitions,
        &body,
    ))
    .await?;
    Ok(StatusCode::OK)
}

/// `DELETE /streams/{stream_id}/topics/{topic_id}/partitions`: remove partitions
/// from a topic. Returns 204. `partitions_count` comes from the query; stream and
/// topic from the path. RBAC in-apply on the metadata STM.
pub(in crate::http) async fn delete_partitions(
    State(state): State<HttpState>,
    identity: Authenticated,
    Path((stream_id, topic_id)): Path<(String, String)>,
    Query(query): Query<DeletePartitions>,
) -> Result<StatusCode, WriteError> {
    let stream_id = Identifier::from_str_value(&stream_id).map_err(WriteError::Rejected)?;
    let topic_id = Identifier::from_str_value(&topic_id).map_err(WriteError::Rejected)?;
    query.validate().map_err(WriteError::Rejected)?;
    let request = DeletePartitionsRequest {
        stream_id: identifier_to_wire(&stream_id).map_err(WriteError::Rejected)?,
        topic_id: identifier_to_wire(&topic_id).map_err(WriteError::Rejected)?,
        partitions_count: query.partitions_count,
    };
    let body = request.to_bytes();
    SendWrapper::new(submit_write(
        &state,
        &identity.session,
        Operation::DeletePartitions,
        &body,
    ))
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `DELETE /streams/{stream_id}/topics/{topic_id}/partitions/{partition_id}`:
/// delete the oldest `segments_count` sealed segments from a partition. Returns
/// 204. `segments_count` comes from the query; stream, topic, and partition from
/// the path.
///
/// `DeleteSegments` is not itself a consensus op: [`submit_write`] carries it
/// through [`submit_gated`], which resolves it to the `TruncatePartition` that
/// commits the trim. RBAC (`delete_segments`) is enforced in-apply on that
/// truncate, like the sibling topic writes.
pub(in crate::http) async fn delete_segments(
    State(state): State<HttpState>,
    identity: Authenticated,
    Path((stream_id, topic_id, partition_id)): Path<(String, String, u32)>,
    Query(query): Query<DeleteSegments>,
) -> Result<StatusCode, WriteError> {
    let stream_id = Identifier::from_str_value(&stream_id).map_err(WriteError::Rejected)?;
    let topic_id = Identifier::from_str_value(&topic_id).map_err(WriteError::Rejected)?;
    let request = DeleteSegmentsRequest {
        stream_id: identifier_to_wire(&stream_id).map_err(WriteError::Rejected)?,
        topic_id: identifier_to_wire(&topic_id).map_err(WriteError::Rejected)?,
        partition_id,
        segments_count: query.segments_count,
    };
    let body = request.to_bytes();
    SendWrapper::new(submit_write(
        &state,
        &identity.session,
        Operation::DeleteSegments,
        &body,
    ))
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `GET /streams/{stream_id}/topics/{topic_id}/messages`: poll a batch of
/// messages as the same `PolledMessages` JSON the legacy server returns. The
/// query is the same flattened `PollMessages` shape the legacy server accepts
/// (`consumer_id`, `partition_id`, strategy `kind`+`value`, `count`,
/// `auto_commit`); stream and topic come from the path.
///
/// A non-replicated read served in band: the same resolution the TCP dispatch
/// runs ([`resolve_poll_request`]), then a mesh read on the owning shard and a
/// re-encode of the stored batches into the legacy wire body, decoded here by
/// the SDK's own [`PolledMessages::from_bytes`] so the JSON is field-identical
/// to a TCP poll. `auto_commit` rides [`resolve_poll_request`]'s args and is
/// honored by the owning shard's poll plan; no extra HTTP work.
pub(in crate::http) async fn poll_messages(
    State(state): State<HttpState>,
    identity: Identity,
    Path((stream_id, topic_id)): Path<(String, String)>,
    Query(query): Query<PollMessages>,
    Query(consistency): Query<ConsistencyQuery>,
) -> Result<Json<PolledMessages>, ReadError> {
    let stream_id = Identifier::from_str_value(&stream_id).map_err(ReadError::Rejected)?;
    let topic_id = Identifier::from_str_value(&topic_id).map_err(ReadError::Rejected)?;
    let scope = resolve_gate_topic_ids(&state, &stream_id, &topic_id);
    authorize_read(
        &state,
        &identity,
        consistency.consistency,
        |permissioner, uid| {
            scope.map_or(Ok(()), |(stream_id, topic_id)| {
                permissioner.poll_messages(uid, stream_id, topic_id)
            })
        },
    )?;
    let wire = poll_wire_request(&stream_id, &topic_id, &query).map_err(ReadError::Rejected)?;
    let (namespace, partition_id, consumer, args) =
        match resolve_poll_request(&state.shard, &wire, HTTP_READ_CLIENT_ID) {
            Ok(decoded) => decoded,
            // TCP parity: a fenced group poll answers 200 with the re-sync
            // sentinel partition, not an error (see [`HTTP_READ_CLIENT_ID`]).
            Err(IggyError::ConsumerGroupPartitionNotOwned(..)) => {
                return Ok(Json(resync_required_polled_messages()));
            }
            // The remaining resolver failures are STM lookups that came up
            // empty (unknown stream, topic, partition, or consumer group), so
            // they render as the legacy 404 body.
            Err(_) => return Err(ReadError::NotFound),
        };
    let reply = SendWrapper::new(
        state
            .shard
            .partition_read(namespace, PartitionRead::Poll { consumer, args }),
    )
    .await;
    match reply {
        Some(PartitionReadReply::Poll {
            fragments,
            current_offset,
        }) => {
            let body = build_polled_messages_body(
                partition_id,
                current_offset,
                fragments,
                state.shard.plane.partitions().config().encryptor.as_deref(),
            )
            .map_err(ReadError::Rejected)?;
            Ok(Json(
                PolledMessages::from_bytes(body).map_err(ReadError::Rejected)?,
            ))
        }
        Some(PartitionReadReply::NotFound) => Err(ReadError::NotFound),
        Some(_) => Err(ReadError::Rejected(IggyError::InvalidCommand)),
        None => Err(ReadError::Timeout),
    }
}

/// `GET /streams/{stream_id}/topics/{topic_id}/consumer-offsets`: fetch a
/// consumer's stored offset as the same `ConsumerOffsetInfo` JSON the legacy
/// server returns. The query is the same flattened `GetConsumerOffset` shape
/// the legacy server accepts (`consumer_id`, optional `partition_id`).
///
/// A non-replicated read served in band, mirroring [`poll_messages`]. A
/// missing offset (never stored, or the partition unknown to its owner) is
/// the legacy 404: the TCP path replies an empty body the SDK decodes as
/// `None`, and the legacy HTTP server renders that `None` as
/// `CustomError::ResourceNotFound`.
pub(in crate::http) async fn get_consumer_offset(
    State(state): State<HttpState>,
    identity: Identity,
    Path((stream_id, topic_id)): Path<(String, String)>,
    Query(query): Query<GetConsumerOffset>,
    Query(consistency): Query<ConsistencyQuery>,
) -> Result<Json<ConsumerOffsetInfo>, ReadError> {
    let stream_id = Identifier::from_str_value(&stream_id).map_err(ReadError::Rejected)?;
    let topic_id = Identifier::from_str_value(&topic_id).map_err(ReadError::Rejected)?;
    let scope = resolve_gate_topic_ids(&state, &stream_id, &topic_id);
    authorize_read(
        &state,
        &identity,
        consistency.consistency,
        |permissioner, uid| {
            scope.map_or(Ok(()), |(stream_id, topic_id)| {
                permissioner.get_consumer_offset(uid, stream_id, topic_id)
            })
        },
    )?;
    let wire =
        consumer_offset_wire_request(&stream_id, &topic_id, &query).map_err(ReadError::Rejected)?;
    let (namespace, partition_id, consumer) =
        resolve_consumer_offset_request(&state.shard, &wire).map_err(|_| ReadError::NotFound)?;
    let reply = SendWrapper::new(
        state
            .shard
            .partition_read(namespace, PartitionRead::ConsumerOffset { consumer }),
    )
    .await;
    match reply {
        Some(PartitionReadReply::ConsumerOffset {
            stored: Some(stored_offset),
            current_offset,
        }) => Ok(Json(ConsumerOffsetInfo {
            partition_id,
            current_offset,
            stored_offset,
        })),
        Some(
            PartitionReadReply::ConsumerOffset { stored: None, .. } | PartitionReadReply::NotFound,
        ) => Err(ReadError::NotFound),
        Some(_) => Err(ReadError::Rejected(IggyError::InvalidCommand)),
        None => Err(ReadError::Timeout),
    }
}

/// `POST /streams/{stream_id}/topics/{topic_id}/messages`: produce a batch of
/// messages to a topic. The JSON body is the same `SendMessages` shape the
/// legacy server accepts (partitioning + base64 messages); stream and topic
/// come from the path.
///
/// Data plane, not control plane: the batch rides the partition group's own
/// consensus (at-least-once, no dedup, no session gate - concurrent produces
/// on one credential are legal), and the committed reply comes back through
/// the session's in-process reply slot rather than a submit return value.
/// The default answers 201 + `X-Iggy-Durability: replicated-memory` only
/// after the quorum commit; `?ack=none` answers 202 + `X-Iggy-Durability:
/// none` immediately after dispatch.
pub(in crate::http) async fn send_messages(
    State(state): State<HttpState>,
    identity: Authenticated,
    Path((stream_id, topic_id)): Path<(String, String)>,
    Query(query): Query<ProduceQuery>,
    Json(command): Json<SendMessages>,
) -> Result<Response, PartitionWriteError> {
    let stream_id =
        Identifier::from_str_value(&stream_id).map_err(PartitionWriteError::Rejected)?;
    let topic_id = Identifier::from_str_value(&topic_id).map_err(PartitionWriteError::Rejected)?;
    // RBAC: authorize the produce on (stream, topic) before any consensus work.
    // Handler-side so an HTTP denial never enters the partition plane.
    authorize_data_plane(
        &state,
        identity.session.user_id,
        &stream_id,
        &topic_id,
        Permissioner::append_messages,
    )
    .map_err(PartitionWriteError::Rejected)?;
    // Rejects an oversized partitioning key and an empty or oversized batch.
    command.validate().map_err(PartitionWriteError::Rejected)?;
    let body = encode_send_messages(&stream_id, &topic_id, &command)
        .map_err(PartitionWriteError::Rejected)?;
    match query.ack {
        ProduceAck::Replicated => {
            SendWrapper::new(partition_write_replicated(
                &state,
                &identity.session,
                Operation::SendMessages,
                &body,
            ))
            .await?;
            Ok((
                StatusCode::CREATED,
                [(
                    DURABILITY_HEADER,
                    HeaderValue::from_static(DURABILITY_REPLICATED_MEMORY),
                )],
            )
                .into_response())
        }
        ProduceAck::None => {
            SendWrapper::new(produce_unacked(&state, &identity.session, &body)).await?;
            Ok((
                StatusCode::ACCEPTED,
                [(DURABILITY_HEADER, HeaderValue::from_static(DURABILITY_NONE))],
            )
                .into_response())
        }
    }
}

/// `PUT /streams/{stream_id}/topics/{topic_id}/consumer-offsets`: store a
/// consumer's offset. The JSON body is the same `StoreConsumerOffset` shape the
/// legacy server accepts (flattened `consumer_id`, optional `partition_id`,
/// `offset`); stream and topic come from the path. Returns 204 on commit,
/// matching the legacy server.
///
/// Data plane like a produce: the offset write is a replicated op on the
/// partition group's own consensus, awaited through the session's in-process
/// reply slot ([`partition_write_replicated`]). The v2 wire op is pinned to
/// `ack = Quorum` - `?ack=none` is a produce-only surface. The consumer
/// identifier passes through on the wire; the dispatch resolvers hash named
/// consumers and rewrite group ids server-side, identically to TCP.
pub(in crate::http) async fn store_consumer_offset(
    State(state): State<HttpState>,
    identity: Authenticated,
    Path((stream_id, topic_id)): Path<(String, String)>,
    Json(command): Json<StoreConsumerOffset>,
) -> Result<StatusCode, PartitionWriteError> {
    let stream_id =
        Identifier::from_str_value(&stream_id).map_err(PartitionWriteError::Rejected)?;
    let topic_id = Identifier::from_str_value(&topic_id).map_err(PartitionWriteError::Rejected)?;
    // RBAC: authorize the offset write on (stream, topic) handler-side.
    authorize_data_plane(
        &state,
        identity.session.user_id,
        &stream_id,
        &topic_id,
        Permissioner::store_consumer_offset,
    )
    .map_err(PartitionWriteError::Rejected)?;
    let request = store_offset_wire_request(&stream_id, &topic_id, &command)
        .map_err(PartitionWriteError::Rejected)?;
    let body = request.to_bytes();
    SendWrapper::new(partition_write_replicated(
        &state,
        &identity.session,
        Operation::StoreConsumerOffset2,
        &body,
    ))
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `DELETE /streams/{stream_id}/topics/{topic_id}/consumer-offsets/{consumer_id}`:
/// delete a consumer's stored offset. The consumer comes from the path and the
/// optional `partition_id` from the query, the same `DeleteConsumerOffset`
/// shape the legacy server accepts. Returns 204 on commit, matching the legacy
/// server; a delete of a never-stored offset is denied by the partition
/// primary (`ReplyHeader.status`) and renders the legacy typed 404. Same
/// replicated partition write as [`store_consumer_offset`].
pub(in crate::http) async fn delete_consumer_offset(
    State(state): State<HttpState>,
    identity: Authenticated,
    Path((stream_id, topic_id, consumer_id)): Path<(String, String, String)>,
    Query(query): Query<DeleteConsumerOffset>,
) -> Result<StatusCode, PartitionWriteError> {
    let stream_id =
        Identifier::from_str_value(&stream_id).map_err(PartitionWriteError::Rejected)?;
    let topic_id = Identifier::from_str_value(&topic_id).map_err(PartitionWriteError::Rejected)?;
    // RBAC: authorize the offset delete on (stream, topic) handler-side.
    authorize_data_plane(
        &state,
        identity.session.user_id,
        &stream_id,
        &topic_id,
        Permissioner::delete_consumer_offset,
    )
    .map_err(PartitionWriteError::Rejected)?;
    // `Consumer::new` fixes the kind to `Consumer`, exactly as the legacy
    // handler does; HTTP cannot express a group-kind offset op.
    let consumer = Consumer::new(
        Identifier::from_str_value(&consumer_id).map_err(PartitionWriteError::Rejected)?,
    );
    let request = delete_offset_wire_request(&stream_id, &topic_id, &consumer, query.partition_id)
        .map_err(PartitionWriteError::Rejected)?;
    let body = request.to_bytes();
    SendWrapper::new(partition_write_replicated(
        &state,
        &identity.session,
        Operation::DeleteConsumerOffset2,
        &body,
    ))
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /streams/{stream_id}/topics/{topic_id}/consumer-groups`: create a
/// consumer group under a topic and render the committed reply as the same
/// `ConsumerGroupDetails` JSON the legacy server returns.
///
/// The stream and topic come from the path; the JSON body is name-only
/// (`{"name": ...}`), matching the legacy request. The submitted op is a plain
/// `CreateConsumerGroup`; the metadata owner assigns the group id, so this
/// handler never allocates one itself. Answers 200, uniform with every other ng
/// create route; the legacy server answers 201 here (yet 200 for stream / topic
/// / user creates), so this trades legacy status parity for internal consistency.
pub(in crate::http) async fn create_cg(
    State(state): State<HttpState>,
    identity: Authenticated,
    Path((stream_id, topic_id)): Path<(String, String)>,
    Json(command): Json<CreateConsumerGroup>,
) -> Result<Json<ConsumerGroupDetails>, WriteError> {
    let stream_id = Identifier::from_str_value(&stream_id).map_err(WriteError::Rejected)?;
    let topic_id = Identifier::from_str_value(&topic_id).map_err(WriteError::Rejected)?;
    let request = CreateConsumerGroupRequest {
        stream_id: identifier_to_wire(&stream_id).map_err(WriteError::Rejected)?,
        topic_id: identifier_to_wire(&topic_id).map_err(WriteError::Rejected)?,
        name: WireName::new(command.name)
            .map_err(|_| WriteError::Rejected(IggyError::InvalidConsumerGroupName))?,
    };
    let body = request.to_bytes();
    let payload = SendWrapper::new(submit_write(
        &state,
        &identity.session,
        Operation::CreateConsumerGroup,
        &body,
    ))
    .await?;
    Ok(Json(decode_consumer_group_details(&payload)?))
}

/// `DELETE /streams/{stream_id}/topics/{topic_id}/consumer-groups/{group_id}`:
/// delete a consumer group. Returns 204 on commit.
pub(in crate::http) async fn delete_cg(
    State(state): State<HttpState>,
    identity: Authenticated,
    Path((stream_id, topic_id, group_id)): Path<(String, String, String)>,
) -> Result<StatusCode, WriteError> {
    let stream_id = Identifier::from_str_value(&stream_id).map_err(WriteError::Rejected)?;
    let topic_id = Identifier::from_str_value(&topic_id).map_err(WriteError::Rejected)?;
    let group_id = Identifier::from_str_value(&group_id).map_err(WriteError::Rejected)?;
    let request = DeleteConsumerGroupRequest {
        stream_id: identifier_to_wire(&stream_id).map_err(WriteError::Rejected)?,
        topic_id: identifier_to_wire(&topic_id).map_err(WriteError::Rejected)?,
        group_id: identifier_to_wire(&group_id).map_err(WriteError::Rejected)?,
    };
    let body = request.to_bytes();
    SendWrapper::new(submit_write(
        &state,
        &identity.session,
        Operation::DeleteConsumerGroup,
        &body,
    ))
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /users`: create a user and render the committed reply as the same
/// `UserInfoDetails` JSON the legacy server returns.
///
/// The plaintext password rides the JSON body; [`submit_write`] hashes it on
/// shard 0 before the request enters consensus (see
/// [`maybe_rewrite_user_password_request`]), so no plaintext is ever replicated.
pub(in crate::http) async fn create_user(
    State(state): State<HttpState>,
    identity: Authenticated,
    Json(command): Json<CreateUser>,
) -> Result<Json<UserInfoDetails>, WriteError> {
    // Rejects empty/oversized username or password before any consensus work.
    command.validate().map_err(WriteError::Rejected)?;
    let request = CreateUserRequest {
        username: WireName::new(&command.username)
            .map_err(|_| WriteError::Rejected(IggyError::InvalidUsername))?,
        password: command.password.expose_secret().to_string(),
        status: command.status.as_code(),
        permissions: command.permissions.as_ref().map(permissions_to_wire),
    };
    let body = request.to_bytes();
    let payload = SendWrapper::new(submit_write(
        &state,
        &identity.session,
        Operation::CreateUser,
        &body,
    ))
    .await?;
    Ok(Json(decode_user_details(&payload)?))
}

/// `PUT /users/{user_id}`: update a user's username and/or status. Returns 204.
pub(in crate::http) async fn update_user(
    State(state): State<HttpState>,
    identity: Authenticated,
    Path(user_id): Path<String>,
    Json(command): Json<UpdateUser>,
) -> Result<StatusCode, WriteError> {
    let user_id = Identifier::from_str_value(&user_id).map_err(WriteError::Rejected)?;
    // Rejects an oversized replacement username; a no-op when username is absent.
    command.validate().map_err(WriteError::Rejected)?;
    let request = UpdateUserRequest {
        user_id: identifier_to_wire(&user_id).map_err(WriteError::Rejected)?,
        username: command
            .username
            .as_deref()
            .map(WireName::new)
            .transpose()
            .map_err(|_| WriteError::Rejected(IggyError::InvalidUsername))?,
        status: command.status.map(|status| status.as_code()),
    };
    let body = request.to_bytes();
    SendWrapper::new(submit_write(
        &state,
        &identity.session,
        Operation::UpdateUser,
        &body,
    ))
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `DELETE /users/{user_id}`: delete a user. Returns 204.
pub(in crate::http) async fn delete_user(
    State(state): State<HttpState>,
    identity: Authenticated,
    Path(user_id): Path<String>,
) -> Result<StatusCode, WriteError> {
    let user_id = Identifier::from_str_value(&user_id).map_err(WriteError::Rejected)?;
    let request = DeleteUserRequest {
        user_id: identifier_to_wire(&user_id).map_err(WriteError::Rejected)?,
    };
    let body = request.to_bytes();
    SendWrapper::new(submit_write(
        &state,
        &identity.session,
        Operation::DeleteUser,
        &body,
    ))
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `PUT /users/{user_id}/password`: change a user's password. Returns 204.
///
/// Both passwords ride the JSON body in plaintext. On shard 0, before the op
/// enters consensus, [`maybe_rewrite_user_password_request`] hashes the new
/// password and strips the current one (so neither plaintext is ever
/// replicated), and verifies `current_password` against the target's stored
/// hash. A wrong current password is not denied pre-consensus: the op still
/// commits, carrying an empty new-password hash the replicated apply turns into
/// an `InvalidCredentials` no-op (surfaced here as 400), so the caller's request
/// sequence stays contiguous.
pub(in crate::http) async fn change_password(
    State(state): State<HttpState>,
    identity: Authenticated,
    Path(user_id): Path<String>,
    Json(command): Json<ChangePassword>,
) -> Result<StatusCode, WriteError> {
    let user_id = Identifier::from_str_value(&user_id).map_err(WriteError::Rejected)?;
    // Rejects empty/oversized current or new password before any consensus work.
    command.validate().map_err(WriteError::Rejected)?;
    let request = ChangePasswordRequest {
        user_id: identifier_to_wire(&user_id).map_err(WriteError::Rejected)?,
        current_password: command.current_password.expose_secret().to_string(),
        new_password: command.new_password.expose_secret().to_string(),
    };
    let body = request.to_bytes();
    SendWrapper::new(submit_write(
        &state,
        &identity.session,
        Operation::ChangePassword,
        &body,
    ))
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `PUT /users/{user_id}/permissions`: replace a user's permissions. Returns 204.
pub(in crate::http) async fn update_permissions(
    State(state): State<HttpState>,
    identity: Authenticated,
    Path(user_id): Path<String>,
    Json(command): Json<UpdatePermissions>,
) -> Result<StatusCode, WriteError> {
    let user_id = Identifier::from_str_value(&user_id).map_err(WriteError::Rejected)?;
    command.validate().map_err(WriteError::Rejected)?;
    let request = UpdatePermissionsRequest {
        user_id: identifier_to_wire(&user_id).map_err(WriteError::Rejected)?,
        permissions: command.permissions.as_ref().map(permissions_to_wire),
    };
    let body = request.to_bytes();
    SendWrapper::new(submit_write(
        &state,
        &identity.session,
        Operation::UpdatePermissions,
        &body,
    ))
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `GET /personal-access-tokens`: list the caller's own tokens (name +
/// expiry, never the secrets) as the same `Vec<PersonalAccessTokenInfo>` JSON
/// the legacy server returns. Self-scoped, so authentication is the whole
/// rule - no permissioner check, matching legacy parity. A consensus-free
/// local STM read via [`read_local`].
pub(in crate::http) async fn get_pats(
    State(state): State<HttpState>,
    identity: Identity,
    Query(query): Query<ConsistencyQuery>,
) -> Result<Json<Vec<PersonalAccessTokenInfo>>, ReadError> {
    let body = GetPersonalAccessTokensRequest.to_bytes();
    let bytes = SendWrapper::new(read_local(
        &state,
        &identity,
        query.consistency,
        GET_PERSONAL_ACCESS_TOKENS_CODE,
        &body,
        |_, _| Ok(()),
    ))
    .await?;
    let response = GetPersonalAccessTokensResponse::decode_from(&bytes)
        .map_err(|_| ReadError::Rejected(IggyError::InvalidCommand))?;
    Ok(Json(personal_access_tokens_from_wire(response)))
}

/// `POST /personal-access-tokens`: mint a personal access token for the caller
/// and return its one-time raw secret as the same `{"token": ...}` JSON the
/// legacy server returns, with HTTP 200.
///
/// The raw token is non-deterministic and secret, so it must never enter
/// consensus: [`rewrite_pat_request_for_user`] (invoked inside
/// [`submit_committed`]) mints it on shard 0 and replicates only its hash, so a
/// successful committed reply body is empty. [`build_raw_pat_reply`] then splices
/// the raw secret back into that reply locally, using the confirmed commit
/// position. The token is surfaced only after the write commits; a malformed
/// splice fails closed rather than emitting a blank token.
///
/// A committed create can still carry a business rejection (duplicate name,
/// invalid expiry), so the result code is honored via [`committed_payload`] -
/// exactly as the mechanical routes do - BEFORE the secret is spliced. Only a
/// genuine success gets a token; a rejection renders the legacy error instead of
/// a bogus 200 + token.
pub(in crate::http) async fn create_pat(
    State(state): State<HttpState>,
    identity: Authenticated,
    Json(command): Json<CreatePersonalAccessToken>,
) -> Result<Json<RawPersonalAccessToken>, WriteError> {
    // Rejects an empty/oversized token name before any consensus work.
    command.validate().map_err(WriteError::Rejected)?;
    let request = CreatePersonalAccessTokenRequest {
        name: WireName::new(&command.name)
            .map_err(|_| WriteError::Rejected(IggyError::InvalidPersonalAccessTokenName))?,
        expiry: command.expiry.into(),
    };
    let body = request.to_bytes();
    let (request_header, committed, raw_token) = SendWrapper::new(submit_committed(
        &state,
        &identity.session,
        Operation::CreatePersonalAccessToken,
        &body,
    ))
    .await?;
    // Reject a committed business error before splicing the secret; the success
    // payload is empty, so the returned slice is discarded.
    committed_payload(&committed)?;
    let reply =
        build_raw_pat_reply(&request_header, committed, raw_token).map_err(WriteError::Rejected)?;
    Ok(Json(RawPersonalAccessToken {
        token: decode_raw_pat_token(&reply)?,
    }))
}

/// `DELETE /personal-access-tokens/{name}`: delete one of the caller's tokens by
/// name. Returns 204 on commit. Self-scoped: any authenticated user may delete
/// their own tokens (the in-apply gate skips PAT ops), matching legacy parity.
pub(in crate::http) async fn delete_pat(
    State(state): State<HttpState>,
    identity: Authenticated,
    Path(name): Path<String>,
) -> Result<StatusCode, WriteError> {
    let request = DeletePersonalAccessTokenRequest {
        name: WireName::new(&name)
            .map_err(|_| WriteError::Rejected(IggyError::InvalidPersonalAccessTokenName))?,
    };
    let body = request.to_bytes();
    SendWrapper::new(submit_write(
        &state,
        &identity.session,
        Operation::DeletePersonalAccessToken,
        &body,
    ))
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Issue a fresh access token for `user_id` and wrap it in the exact
/// `IdentityInfo` shape the SDKs pin: numeric `user_id` plus an `access_token`
/// carrying the token string and its unix-seconds expiry.
fn issue_identity(inner: &HttpInner, user_id: u32) -> Result<Json<IdentityInfo>, CustomError> {
    let generated = inner.jwt.generate(user_id)?;
    Ok(Json(IdentityInfo {
        user_id: generated.user_id,
        access_token: Some(TokenInfo {
            token: generated.access_token,
            expiry: generated.access_token_expiry,
        }),
    }))
}
