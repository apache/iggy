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

use std::collections::BTreeSet;
use std::str::FromStr;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use bytes::{Bytes, BytesMut};
use chrono::Local;
use consensus::MetadataHandle;
use iggy_binary_protocol::codes::{
    DESCRIBE_OPTIONS_CODE, GET_CLIENT_CODE, GET_CLIENTS_CODE, GET_CONSUMER_GROUP_CODE,
    GET_CONSUMER_GROUPS_CODE, GET_CONSUMER_OFFSET_CODE, GET_PERSONAL_ACCESS_TOKENS_CODE,
    GET_SNAPSHOT_FILE_CODE, GET_STATS_CODE, GET_STREAM_CODE, GET_STREAMS_CODE, GET_TOPIC_CODE,
    GET_TOPICS_CODE, GET_USER_CODE, GET_USERS_CODE, POLL_MESSAGES_CODE,
};
use iggy_binary_protocol::primitives::partition_history::PartitionContext;
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
};
use iggy_binary_protocol::requests::system::{
    DescribeOptionsRequest, GetStatsRequest, ResourceTarget, UpdateResourceRequest,
};
use iggy_binary_protocol::requests::topics::{
    CreateTopicRequest, DeleteTopicRequest, GetTopicRequest, GetTopicsRequest,
};
use iggy_binary_protocol::requests::users::{
    ChangePasswordRequest, CreateUserRequest, DeleteUserRequest, GetUserRequest, GetUsersRequest,
    UpdatePermissionsRequest,
};
use iggy_binary_protocol::responses::clients::client_response::ConsumerGroupInfoResponse;
use iggy_binary_protocol::responses::clients::get_client::ClientDetailsResponse;
use iggy_binary_protocol::responses::clients::get_clients::GetClientsResponse;
use iggy_binary_protocol::responses::consumer_groups::get_consumer_group::ConsumerGroupDetailsResponse;
use iggy_binary_protocol::responses::consumer_groups::get_consumer_groups::GetConsumerGroupsResponse;
use iggy_binary_protocol::responses::personal_access_tokens::GetPersonalAccessTokensResponse;
use iggy_binary_protocol::responses::streams::get_stream::GetStreamResponse;
use iggy_binary_protocol::responses::streams::get_streams::GetStreamsResponse;
use iggy_binary_protocol::responses::system::DescribeOptionsResponse;
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
use iggy_common::wire_conversions::{
    clients_from_wire, consumer_groups_from_wire, identifier_to_wire, option_specs_from_wire,
    permissions_to_wire, personal_access_tokens_from_wire, streams_from_wire, topics_from_wire,
    users_from_wire,
};
use iggy_common::{
    ClientInfo, ClientInfoDetails, ClusterMetadata, Consumer, ConsumerGroup, ConsumerGroupDetails,
    ConsumerOffsetInfo, DEFAULT_MAX_TOPIC_SIZE, Identifier, IdentityInfo, IggyError, IggyTimestamp,
    OptionSpec, OptionStorage, OptionsScope, Partitioning, PersonalAccessTokenInfo, PollMessages,
    PolledMessages, RawPersonalAccessToken, SendMessages, SendMessagesConfirmations, Stats, Stream,
    StreamDetails, TokenInfo, Topic, TopicDetails, UserInfo, UserInfoDetails, Validatable,
    descriptors_for, erase_buffer,
};
use metadata::impls::metadata::StreamsFrontend;
use metadata::permissioner::Permissioner;
use secrecy::ExposeSecret;
use secrecy::zeroize::Zeroizing;
use send_wrapper::SendWrapper;
use serde::Deserialize;
use shard::{PartitionRead, PartitionReadReply};

use crate::dispatch::partition::{
    ConsumerOffsetTarget, fence_poll_context, resolve_consumer_offset_request,
    resolve_poll_request, resolve_primary_consumer_offset_request,
};
use crate::dispatch::session_ops::{verify_login_credentials, verify_pat_credentials};
use crate::http::error::{
    Consistency, ConsistencyQuery, CustomError, PartitionWriteError, ProduceAck, ProduceQuery,
    ReadError, WriteError,
};
use crate::http::extractor::{Authenticated, Credential, Identity, JsonMembers, JsonUpdate};
use crate::http::forward::{
    FirstAttempt, PartitionEffect, PartitionHop, PartitionRequest, require_local_primary,
    serve_partition,
};
use crate::http::metrics::{TopicUsageSample, gauge_value};
use crate::http::reads::{
    authorize_data_plane, authorize_read, await_metadata_floor, await_recovery_barrier,
    check_partition_freshness, check_partition_incarnation, gate_local_read, read_local,
    resolve_gate_stream, resolve_gate_topic, resolve_gate_topic_ids, resolve_gate_user,
    topic_durability,
};
use crate::http::reply::{
    committed_payload, decode_consumer_group_details, decode_raw_pat_token, decode_stream_details,
    decode_topic_details, decode_user_details, login_error_to_iggy, send_confirmations,
};
use crate::http::state::{DURABILITY_HEADER, HttpInner, HttpState};
use crate::http::submit::{
    capture_partition_write, logout_session, partition_write_replicated, produce_unacked,
    submit_committed, submit_write,
};
use crate::http::wire::{
    consumer_offset_wire_request, delete_offset_wire_request, encode_send_messages,
    poll_wire_request, store_offset_wire_request,
};
use crate::reply_frame::{build_polled_messages_body, build_raw_pat_reply};
use crate::responses::connected_client_to_response;
use crate::snapshot;

/// `GET /ping` response body, matching the legacy HTTP server's health probe.
const PONG: &str = "pong";

/// VSR client id stamped on HTTP data-plane requests that hold no session:
/// reads (poll / consumer-offset), and a write's partition capture before the
/// node that serves it Registers one. Shard-0 client ids are minted from 1, so
/// 0 can never collide with a live client. `resolve_poll_request` and
/// `capture_partition_write` take a client id only for their consumer-group
/// fencing branches, and HTTP never reaches them: the wire mappers reject
/// `consumer_kind=consumer_group` up front. Legacy HTTP polls with client id 0
/// for the same reason (no persistent sessions, so no group membership).
pub(in crate::http) const HTTP_UNREGISTERED_CLIENT_ID: u128 = 0;

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
    // re-commits, like every other local read. On barrier expiry, fail with a
    // retryable 503 rather than validating against rolled-back credentials:
    // this route's error currency is `IggyError -> CustomError`, and
    // `TransientNotAccepted` is the variant `CustomError` renders 503.
    SendWrapper::new(crate::http::reads::await_recovery_barrier(&state.shard))
        .await
        .map_err(|_| IggyError::TransientNotAccepted)?;
    let (user_id, created_at) = verify_login_credentials(
        &state.shard,
        &command.username,
        command.password.expose_secret(),
    )
    .map_err(|error| login_error_to_iggy(&error))?;
    issue_identity(&state, user_id, created_at)
}

pub(in crate::http) async fn login_with_personal_access_token(
    State(state): State<HttpState>,
    Json(command): Json<LoginWithPersonalAccessToken>,
) -> Result<Json<IdentityInfo>, CustomError> {
    // Same recovery-barrier wait and retryable-503-on-expiry mapping as
    // `login_user`.
    SendWrapper::new(crate::http::reads::await_recovery_barrier(&state.shard))
        .await
        .map_err(|_| IggyError::TransientNotAccepted)?;
    let (user_id, created_at) = verify_pat_credentials(&state.shard, command.token.expose_secret())
        .map_err(|error| login_error_to_iggy(&error))?;
    issue_identity(&state, user_id, created_at)
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
/// Stateless by design: server has no replicated revocation list (the P3
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
    // Refresh rejects trusted-issuer tokens (they keep their own lifecycle);
    // `decode_for_refresh` is `!Send` (a trusted-issuer token may await a JWKS
    // fetch), so bridge it like every other shard-0 path (see [`logout_user`]).
    let (user_id, user_created_at) =
        SendWrapper::new(state.jwt.decode_for_refresh(&command.token)).await?;
    issue_identity(&state, user_id, user_created_at)
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

/// `GET /options/{scope}`: describe the option catalog for one resource
/// scope (`topic`, `stream`, `user`) as `Vec<OptionSpec>` JSON. A
/// consensus-free local read via [`read_local`], gated on authentication
/// only (the catalog is not resource-scoped).
pub(in crate::http) async fn describe_options(
    State(state): State<HttpState>,
    identity: Identity,
    Path(scope): Path<String>,
) -> Result<Json<Vec<OptionSpec>>, ReadError> {
    let scope = OptionsScope::from_str(&scope).map_err(ReadError::Rejected)?;
    let body = DescribeOptionsRequest {
        scope: scope.as_code(),
    }
    .to_bytes();
    let bytes = SendWrapper::new(read_local(
        &state,
        &identity,
        Consistency::default(),
        DESCRIBE_OPTIONS_CODE,
        &body,
        |_, _| Ok(()),
    ))
    .await?;
    let response = DescribeOptionsResponse::decode_from(&bytes)
        .map_err(|_| ReadError::Rejected(IggyError::InvalidCommand))?;
    Ok(Json(
        option_specs_from_wire(response).map_err(ReadError::Rejected)?,
    ))
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
    Ok(Json(
        streams_from_wire(response).map_err(ReadError::Rejected)?,
    ))
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
        // Resolved when the rule RUNS, not here: `read_local` can park for the
        // read-your-writes frontier, and an entity created during that wait
        // would resolve to nothing on a pre-wait pass, where a miss is a
        // pass-through. A miss still leaves the gate a pass-through so the read
        // renders the existing 404 rather than a 403.
        |permissioner, uid| {
            resolve_gate_stream(&state, &request.stream_id)
                .map_or(Ok(()), |stream_id| permissioner.get_stream(uid, stream_id))
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
            resolve_gate_stream(&state, &request.stream_id)
                .map_or(Ok(()), |stream_id| permissioner.get_topics(uid, stream_id))
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
            resolve_gate_topic(&state, &request.stream_id, &request.topic_id)
                .map_or(Ok(()), |(stream_id, topic_id)| {
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
/// consensus-free local STM read via [`read_local`]. Any target resolving to
/// the caller - [`CURRENT_USER_ALIAS`], own id, own username - skips the
/// `read_users` gate, mirroring the legacy self-read bypass.
pub(in crate::http) async fn get_user(
    State(state): State<HttpState>,
    identity: Identity,
    Path(user_id): Path<String>,
    Query(query): Query<ConsistencyQuery>,
) -> Result<Json<UserInfoDetails>, ReadError> {
    let wire_user_id = if user_id == CURRENT_USER_ALIAS {
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
            #[allow(clippy::cast_possible_truncation)]
            let is_self =
                resolve_gate_user(&state, &request.user_id) == Some(identity.user_id as usize);
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
            resolve_gate_topic(&state, &request.stream_id, &request.topic_id)
                .map_or(Ok(()), |(stream_id, topic_id)| {
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
            resolve_gate_topic(&state, &request.stream_id, &request.topic_id)
                .map_or(Ok(()), |(stream_id, topic_id)| {
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

/// `GET <http.metrics.endpoint>`: the metric set in prometheus text
/// exposition. The `Identity` extractor rejects a missing or invalid bearer
/// with 401, and any authenticated user may scrape the aggregate gauges.
/// The per-topic series name every stream and topic, so they are exported only
/// to a user the `/stats` rule ([`Permissioner::get_stats`]) admits; anyone
/// else gets the aggregates with no per-topic series. Scrapers present a JWT
/// or a raw PAT the same way every read route accepts them.
///
/// The entity gauges sample the same reads `/stats` serves: the metadata STM
/// stream and user maps plus the stats-registry rollups, whose partition-plane
/// increments are relaxed, so scraped values are approximate while writes are
/// in flight. The per-topic series come from the same walk and the same
/// rollups `GetTopic` reads, and encode synchronously on shard 0 in time linear
/// in the topic count. The clients count scatter-gathers the per-shard session
/// managers exactly like `GET /clients` and turns partial when a shard misses
/// the reply deadline.
///
/// Per-topic series are this replica's local view, so a lagging backup may
/// report smaller values than the primary.
///
/// Held behind the post-restart recovery barrier like every other local read:
/// until the recovered WAL suffix re-commits, the users map may still grant a
/// permission a committed op revoked, and the walk may name a topic a
/// committed op deleted. An expired barrier answers the retryable 503 rather
/// than a partial exposition.
pub(in crate::http) async fn get_metrics(
    State(state): State<HttpState>,
    identity: Identity,
) -> Result<String, ReadError> {
    SendWrapper::new(await_recovery_barrier(&state.shard)).await?;
    // Awaited before the permission check and the walk so check, walk, replace
    // and encode run without a yield, and a scrape that finishes later never
    // publishes an older per-topic snapshot.
    let clients_count = SendWrapper::new(state.shard.count_all_clients()).await as u64;
    // Both waits can outlast the account the token was issued for, and the
    // grant below is looked up by numeric ID alone.
    state
        .ensure_active_user(identity.user_id, Some(identity.user_created_at))
        .map_err(ReadError::Rejected)?;
    let exports_topics = state
        .shard
        .plane
        .metadata()
        .mux_stm
        .users()
        .authorize(|permissioner| permissioner.get_stats(identity.user_id))
        .is_ok();
    let (
        (streams_count, topics_count, partitions_count, segments_count, messages_count),
        topic_usage,
    ) = state
        .shard
        .plane
        .metadata()
        .mux_stm
        .streams()
        .read(|streams| {
            let mut topics_count = 0u64;
            let mut partitions_count = 0u64;
            let mut segments_count = 0u64;
            let mut messages_count = 0u64;
            let mut topic_usage = if exports_topics {
                Vec::with_capacity(
                    streams
                        .items
                        .iter()
                        .map(|(_, stream)| stream.topics.len())
                        .sum(),
                )
            } else {
                Vec::new()
            };
            for (_, stream) in &streams.items {
                topics_count = topics_count.saturating_add(stream.topics.len() as u64);
                segments_count = segments_count
                    .saturating_add(u64::from(stream.stats.segments_count_inconsistent()));
                messages_count =
                    messages_count.saturating_add(stream.stats.messages_count_inconsistent());
                for (_, topic) in &stream.topics {
                    partitions_count =
                        partitions_count.saturating_add(topic.partitions.len() as u64);
                    if !exports_topics {
                        continue;
                    }
                    topic_usage.push(TopicUsageSample {
                        stream: Arc::clone(&stream.name),
                        topic: Arc::clone(&topic.name),
                        size_bytes: topic.stats.size_bytes_inconsistent(),
                        messages: topic.stats.messages_count_inconsistent(),
                        max_size_bytes: topic
                            .resolved
                            .max_topic_size
                            .resolve(DEFAULT_MAX_TOPIC_SIZE),
                    });
                }
            }
            (
                (
                    streams.items.len() as u64,
                    topics_count,
                    partitions_count,
                    segments_count,
                    messages_count,
                ),
                topic_usage,
            )
        });
    let users_count = state
        .shard
        .plane
        .metadata()
        .mux_stm
        .users()
        .read(|users| users.items.len() as u64);

    let metrics = &state.metrics;
    metrics.streams.set(gauge_value(streams_count));
    metrics.topics.set(gauge_value(topics_count));
    metrics.partitions.set(gauge_value(partitions_count));
    metrics.segments.set(gauge_value(segments_count));
    metrics.messages.set(gauge_value(messages_count));
    metrics.users.set(gauge_value(users_count));
    metrics.clients.set(gauge_value(clients_count));
    metrics.topic_usage.replace(topic_usage);
    Ok(metrics.formatted_output())
}

/// `POST /snapshot`: collect a diagnostic archive and return it as a ZIP
/// download with the same headers the legacy server sets.
///
/// Gated on the snapshot rule (`manage_servers`) through the
/// shared [`gate_local_read`], which also serves it behind the post-restart
/// barrier; the archive itself carries no metadata answer to hold. Collection
/// shells out to system tools on a dedicated OS thread (see
/// `snapshot::collect`); this handler only awaits the result handoff, which is
/// `Send`, so no `SendWrapper` bridge is needed.
pub(in crate::http) async fn get_snapshot(
    State(state): State<HttpState>,
    identity: Identity,
    Query(query): Query<ConsistencyQuery>,
    Json(command): Json<GetSnapshot>,
) -> Result<(HeaderMap, Body), ReadError> {
    SendWrapper::new(gate_local_read(
        &state,
        &identity,
        query.consistency,
        GET_SNAPSHOT_FILE_CODE,
        Permissioner::get_snapshot,
    ))
    .await?;
    let archive = snapshot::collect(
        Arc::clone(&state.server_config),
        command.compression,
        command.snapshot_types,
    )
    .await
    .map_err(ReadError::Rejected)?;
    authorize_read(
        &state,
        &identity,
        query.consistency,
        Permissioner::get_snapshot,
    )?;

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
/// synchronous. The caller's peer IP picks each node's advertised address from
/// its per-client-network selectors.
pub(in crate::http) async fn get_cluster_metadata(
    State(state): State<HttpState>,
    identity: Identity,
) -> Json<ClusterMetadata> {
    Json(state.build_cluster_metadata(identity.client_ip))
}

/// `GET /clients`: list every connected client across all shards as the same
/// `Vec<ClientInfo>` JSON the legacy server returns.
///
/// Unlike the entity reads, connections live in each shard's session manager,
/// not the metadata STM, so this scatter-gathers over the shard mesh
/// (`list_all_clients`) instead of going through [`read_local`]. It still runs
/// the identical gates via [`gate_local_read`] - the consumer-group counts it
/// reports come off the streams STM, so its binary twin holds it for the read
/// frontier too. The gather future is `!Send`, bridged onto shard 0's thread by
/// `SendWrapper` exactly as the write path bridges its submit.
pub(in crate::http) async fn get_clients(
    State(state): State<HttpState>,
    identity: Identity,
    Query(query): Query<ConsistencyQuery>,
) -> Result<Json<Vec<ClientInfo>>, ReadError> {
    SendWrapper::new(gate_local_read(
        &state,
        &identity,
        query.consistency,
        GET_CLIENTS_CODE,
        Permissioner::get_clients,
    ))
    .await?;
    let infos = SendWrapper::new(state.shard.list_all_clients()).await;
    authorize_read(
        &state,
        &identity,
        query.consistency,
        Permissioner::get_clients,
    )?;
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
    SendWrapper::new(gate_local_read(
        &state,
        &identity,
        query.consistency,
        GET_CLIENT_CODE,
        Permissioner::get_client,
    ))
    .await?;
    let infos = SendWrapper::new(state.shard.list_all_clients()).await;
    authorize_read(
        &state,
        &identity,
        query.consistency,
        Permissioner::get_client,
    )?;
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
/// The body carries the name and an optional `options` object. The server
/// assigns the id, so there is no client-supplied stream id to honor.
pub(in crate::http) async fn create_stream(
    State(state): State<HttpState>,
    identity: Authenticated,
    JsonMembers {
        value: command,
        members,
    }: JsonMembers<CreateStream>,
) -> Result<Json<StreamDetails>, WriteError> {
    refuse_misplaced_options(OptionsScope::Stream, &members)?;
    let request = CreateStreamRequest {
        name: WireName::new(command.name)
            .map_err(|_| WriteError::Rejected(IggyError::InvalidStreamName))?,
        options: command.options.to_wire().map_err(WriteError::Rejected)?,
    };
    let body = erasing_body(&request);
    erase_buffer(request.options.into_bytes());
    let payload = SendWrapper::new(submit_write(
        &state,
        &identity.session,
        Operation::CreateStream,
        body,
    ))
    .await?;
    Ok(Json(decode_stream_details(&payload)?))
}

/// `PUT /streams/{stream_id}`: update the stream's name and options. An absent
/// or `null` key keeps its value, a key in `reset` returns to its default.
/// Returns 204 on commit.
pub(in crate::http) async fn update_stream(
    State(state): State<HttpState>,
    identity: Authenticated,
    Path(stream_id): Path<String>,
    update: Result<JsonUpdate, WriteError>,
) -> Result<StatusCode, WriteError> {
    let stream_id = Identifier::from_str_value(&stream_id).map_err(WriteError::Rejected)?;
    let target = ResourceTarget::Stream {
        stream_id: identifier_to_wire(&stream_id).map_err(WriteError::Rejected)?,
    };
    submit_update(&state, &identity, target, update).await
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
        body,
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
    JsonMembers {
        value: command,
        members,
    }: JsonMembers<CreateTopic>,
) -> Result<Json<TopicDetails>, WriteError> {
    let stream_id = Identifier::from_str_value(&stream_id).map_err(WriteError::Rejected)?;
    refuse_misplaced_options(OptionsScope::Topic, &members)?;
    // Rejects empty/oversized name and partitions_count > MAX. The options are
    // admitted by the submit chain, the same gate the binary transports use.
    command.validate().map_err(WriteError::Rejected)?;
    let wire_options = command.options.to_wire().map_err(WriteError::Rejected)?;
    let request = CreateTopicRequest {
        stream_id: identifier_to_wire(&stream_id).map_err(WriteError::Rejected)?,
        partitions_count: command.partitions_count,
        name: WireName::new(command.name)
            .map_err(|_| WriteError::Rejected(IggyError::InvalidTopicName))?,
        options: wire_options,
    };
    let body = request.to_bytes();
    let payload = SendWrapper::new(submit_write(
        &state,
        &identity.session,
        Operation::CreateTopic,
        body,
    ))
    .await?;
    Ok(Json(decode_topic_details(&payload)?))
}

/// `PUT /streams/{stream_id}/topics/{topic_id}`: update the topic's name and
/// updatable options. An absent or `null` key keeps its value, a key in `reset`
/// returns to its default. Returns 204 on commit.
pub(in crate::http) async fn update_topic(
    State(state): State<HttpState>,
    identity: Authenticated,
    Path((stream_id, topic_id)): Path<(String, String)>,
    update: Result<JsonUpdate, WriteError>,
) -> Result<StatusCode, WriteError> {
    let stream_id = Identifier::from_str_value(&stream_id).map_err(WriteError::Rejected)?;
    let topic_id = Identifier::from_str_value(&topic_id).map_err(WriteError::Rejected)?;
    let target = ResourceTarget::Topic {
        stream_id: identifier_to_wire(&stream_id).map_err(WriteError::Rejected)?,
        topic_id: identifier_to_wire(&topic_id).map_err(WriteError::Rejected)?,
    };
    submit_update(&state, &identity, target, update).await
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
        body,
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
        body,
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
        body,
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
/// through `submit_gated`, which resolves it to the `TruncatePartition` that
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
        body,
    ))
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `GET /streams/{stream_id}/topics/{topic_id}/messages`: poll a batch of
/// messages as the same `PolledMessages` JSON the legacy server returns. The
/// query is the same flattened `PollMessages` shape the legacy server accepts
/// (`consumer_id`, `partition_id`, strategy `kind`+`value`, `count`,
/// `auto_commit`); stream and topic come from the path. `consumer_kind`
/// defaults to `consumer` and `consumer_group` is rejected (see
/// [`poll_wire_request`]) since HTTP has no persistent connection and cannot observe
/// group's partition assignment (that can be subject to change).
///
/// A non-replicated read served by the partition primary: the same resolution
/// the TCP dispatch runs ([`resolve_poll_request`]), then a mesh read on the
/// owning shard and a re-encode of the stored batches into the legacy wire
/// body, decoded here by the SDK's own [`PolledMessages::from_bytes`] so the
/// JSON is field-identical to a TCP poll. `auto_commit` rides
/// [`resolve_poll_request`]'s args and is honored by the owning shard's poll
/// plan. A node whose owner is not the primary relays the poll to the primary
/// ([`serve_partition`]).
pub(in crate::http) async fn poll_messages(
    State(state): State<HttpState>,
    identity: Identity,
    Path((stream_id, topic_id)): Path<(String, String)>,
    Query(query): Query<PollMessages>,
    Query(consistency): Query<ConsistencyQuery>,
    Extension(request): Extension<Arc<PartitionRequest>>,
) -> Result<Response, ReadError> {
    let stream_id = Identifier::from_str_value(&stream_id).map_err(ReadError::Rejected)?;
    let topic_id = Identifier::from_str_value(&topic_id).map_err(ReadError::Rejected)?;
    let (consistency, metadata_floor) = SendWrapper::new(partition_read_consistency(
        &state,
        &request,
        consistency.consistency,
    ))
    .await?;
    let rule = |permissioner: &Permissioner, uid| {
        resolve_gate_topic_ids(&state, &stream_id, &topic_id)
            .map_or(Ok(()), |(stream_id, topic_id)| {
                permissioner.poll_messages(uid, stream_id, topic_id)
            })
    };
    SendWrapper::new(gate_local_read(
        &state,
        &identity,
        consistency,
        POLL_MESSAGES_CODE,
        &rule,
    ))
    .await?;

    let wire = match poll_wire_request(&stream_id, &topic_id, &query) {
        Ok(request) => request,
        Err(error) => return Err(ReadError::Rejected(error)),
    };

    let (namespace, partition_id, consumer, args) =
        match resolve_poll_request(&state.shard, &wire, HTTP_UNREGISTERED_CLIENT_ID) {
            Ok(decoded) => decoded,
            // A partition id the topic does not have, or a consumer kind HTTP
            // can't poll with, is a client addressing error with its own code;
            // collapsing it into the generic 404 body told an SDK "no such
            // stream/topic" for a request whose stream and topic both
            // resolved. TCP parity: the dispatch denies typed here.
            Err(error @ (IggyError::PartitionNotFound(..) | IggyError::FeatureUnavailable)) => {
                return Err(ReadError::Rejected(error));
            }
            // The remaining resolver failures are STM lookups that came up
            // empty (unknown stream, topic, or consumer group), so they render
            // as the legacy 404 body.
            Err(_) => return Err(ReadError::NotFound),
        };
    let rule = |permissioner: &Permissioner, uid| {
        permissioner.poll_messages(uid, namespace.stream_id(), namespace.topic_id())
    };
    // Read per attempt: a later attempt checks the context against the
    // metadata this node applied since.
    let local_metadata = || {
        let metadata = state
            .shard
            .plane
            .metadata()
            .mux_stm
            .streams()
            .poll_metadata(namespace, None, HTTP_UNREGISTERED_CLIENT_ID)
            .ok_or(ReadError::Rejected(IggyError::TransientNotAccepted))?;
        let local = metadata.context(state.shard.plane.metadata().applied_frontier().get());
        Ok::<_, ReadError>((metadata, local))
    };
    let (_, local) = local_metadata()?;
    let requested = request
        .route()
        .map(|route| route.context)
        .or(query.strategy.context)
        .unwrap_or(local);
    let requested = PartitionContext {
        metadata_op: requested.metadata_op.max(metadata_floor),
        ..requested
    };
    let effect = if query.auto_commit {
        PartitionEffect::Write
    } else {
        PartitionEffect::Read
    };
    let serve_locally = async || {
        check_partition_freshness(&state).map_err(ReadError::Rejected)?;
        authorize_read(&state, &identity, consistency, rule)?;
        let (metadata, local) = local_metadata()?;
        // `poll_wire_request` refused a group poll above, so this poll has no
        // member and names client 0, the HTTP read client.
        fence_poll_context(requested, local, HTTP_UNREGISTERED_CLIENT_ID, partition_id)
            .map_err(ReadError::Rejected)?;
        let reply = state
            .shard
            .partition_read(
                namespace,
                PartitionRead::UnboundPollOnPrimary {
                    consumer,
                    args: args.clone(),
                    user_id: identity.user_id,
                    user_created_at: identity.user_created_at.as_micros(),
                    metadata,
                },
            )
            .await;
        // The owner may already have auto-committed. At-most-once polls can
        // lose a batch here, but revoked credentials must never receive it.
        authorize_read(&state, &identity, consistency, rule)?;
        check_partition_incarnation(&state, namespace, local.incarnation)?;
        polled_messages_reply(&state, partition_id, reply)
    };
    SendWrapper::new(serve_partition(
        &state,
        &request,
        namespace,
        requested,
        effect,
        FirstAttempt::Local,
        serve_locally,
    ))
    .await
}

/// The HTTP answer to the owner's reply to a poll.
fn polled_messages_reply(
    state: &HttpInner,
    partition_id: u32,
    reply: Option<PartitionReadReply>,
) -> Result<Response, ReadError> {
    match reply {
        Some(PartitionReadReply::Poll {
            context,
            fragments,
            current_offset,
            encryptor,
        }) => {
            // TCP parity (`build_polled_messages_reply`): the served context
            // carries at least the metadata this node applied, so a later poll
            // on a node behind it is refused as not accepted, not as lost history.
            let context = PartitionContext {
                metadata_op: context
                    .metadata_op
                    .max(state.shard.plane.metadata().applied_frontier().get()),
                ..context
            };
            let body = build_polled_messages_body(
                partition_id,
                current_offset,
                context,
                fragments,
                encryptor.as_deref(),
            )
            .map_err(ReadError::Rejected)?;
            Ok(
                Json(PolledMessages::from_bytes(body).map_err(ReadError::Rejected)?)
                    .into_response(),
            )
        }
        Some(PartitionReadReply::NotFound) => Err(ReadError::NotFound),
        Some(PartitionReadReply::Rejected(error)) => Err(ReadError::Rejected(error)),
        Some(_) => Err(ReadError::Rejected(IggyError::InvalidCommand)),
        None => Err(ReadError::Timeout),
    }
}

/// `GET /streams/{stream_id}/topics/{topic_id}/consumer-offsets`: fetch a
/// consumer's stored offset as the same `ConsumerOffsetInfo` JSON the legacy
/// server returns. The query is the flattened `GetConsumerOffset` shape
/// (`consumer_id`, optional `consumer_kind` defaulting to `consumer`,
/// optional `partition_id`).
///
/// A non-replicated read served by the partition primary, mirroring
/// [`poll_messages`]. A missing offset (never stored, or the partition unknown
/// to its topic) is the legacy 404: the TCP path replies an empty body the SDK
/// decodes as `None`, and the legacy HTTP server renders that `None` as
/// `CustomError::ResourceNotFound`. An owner that does not hold the partition
/// yet answers 503 `TransientNotAccepted`, as TCP does, so it never reads as
/// `None`. Unlike poll/store/delete, `consumer_kind=consumer_group` is allowed,
/// because a group offset read needs no held connection, so it is not
/// member-fenced ([`resolve_consumer_offset_request`]).
pub(in crate::http) async fn get_consumer_offset(
    State(state): State<HttpState>,
    identity: Identity,
    Path((stream_id, topic_id)): Path<(String, String)>,
    Query(query): Query<GetConsumerOffset>,
    Query(consistency): Query<ConsistencyQuery>,
    Extension(request): Extension<Arc<PartitionRequest>>,
) -> Result<Response, ReadError> {
    let stream_id = Identifier::from_str_value(&stream_id).map_err(ReadError::Rejected)?;
    let topic_id = Identifier::from_str_value(&topic_id).map_err(ReadError::Rejected)?;
    // The local context below is read after any floor hold, so it covers the
    // floor already.
    let (consistency, _metadata_floor) = SendWrapper::new(partition_read_consistency(
        &state,
        &request,
        consistency.consistency,
    ))
    .await?;
    let rule = |permissioner: &Permissioner, uid| {
        resolve_gate_topic_ids(&state, &stream_id, &topic_id)
            .map_or(Ok(()), |(stream_id, topic_id)| {
                permissioner.get_consumer_offset(uid, stream_id, topic_id)
            })
    };
    SendWrapper::new(gate_local_read(
        &state,
        &identity,
        consistency,
        GET_CONSUMER_OFFSET_CODE,
        &rule,
    ))
    .await?;
    let wire =
        consumer_offset_wire_request(&stream_id, &topic_id, &query).map_err(ReadError::Rejected)?;
    let namespace = match resolve_consumer_offset_request(&state.shard, &wire) {
        Ok((namespace, _, target)) if !matches!(target, ConsumerOffsetTarget::MissingGroup) => {
            namespace
        }
        _ => return Err(ReadError::NotFound),
    };
    let rule = |permissioner: &Permissioner, uid| {
        permissioner.get_consumer_offset(uid, namespace.stream_id(), namespace.topic_id())
    };
    let context = match request.route() {
        Some(route) => route.context,
        None => state
            .shard
            .plane
            .metadata()
            .mux_stm
            .streams()
            .poll_metadata(namespace, None, HTTP_UNREGISTERED_CLIENT_ID)
            .ok_or(ReadError::Rejected(IggyError::TransientNotAccepted))?
            .context(state.shard.plane.metadata().applied_frontier().get()),
    };
    let serve_locally = async || {
        check_partition_freshness(&state).map_err(ReadError::Rejected)?;
        authorize_read(&state, &identity, consistency, rule)?;
        let (namespace, partition_id, read) =
            resolve_primary_consumer_offset_request(&state.shard, &wire, context)
                .map_err(ReadError::Rejected)?;
        let reply = state.shard.partition_read(namespace, read).await;
        authorize_read(&state, &identity, consistency, rule)?;
        check_partition_incarnation(&state, namespace, context.incarnation)?;
        consumer_offset_reply(partition_id, reply).map(IntoResponse::into_response)
    };
    SendWrapper::new(serve_partition(
        &state,
        &request,
        namespace,
        context,
        PartitionEffect::Read,
        FirstAttempt::Local,
        serve_locally,
    ))
    .await
}

/// The consistency a partition read is gated with on this node, and the
/// metadata op its context must cover.
///
/// A hop target reads after its metadata hold and never answers the 307 of
/// the metadata primary, because the partition primary already serves the
/// read. A linearizable read on a node that is not the metadata primary holds
/// for the metadata primary's commit floor instead of the 307
/// ([`await_metadata_floor`]), then authorizes, resolves and relays on state at
/// least that fresh. Without forwarding there is no floor to read, so such a
/// read keeps the 307.
async fn partition_read_consistency(
    state: &HttpInner,
    request: &PartitionRequest,
    consistency: Consistency,
) -> Result<(Consistency, u64), ReadError> {
    const NO_FLOOR: u64 = 0;
    check_partition_freshness(state).map_err(ReadError::Rejected)?;
    if matches!(request.hop, PartitionHop::Target(_)) {
        return Ok((Consistency::Serializable, NO_FLOOR));
    }
    if consistency == Consistency::Serializable
        || state.is_metadata_primary()
        || !state.forward.active()
    {
        return Ok((consistency, NO_FLOOR));
    }
    let floor = await_metadata_floor(state, request.deadline).await?;
    check_partition_freshness(state).map_err(ReadError::Rejected)?;
    Ok((Consistency::Serializable, floor))
}

/// The HTTP answer to the owner's reply to a consumer offset read.
fn consumer_offset_reply(
    partition_id: u32,
    reply: Option<PartitionReadReply>,
) -> Result<Json<ConsumerOffsetInfo>, ReadError> {
    match reply {
        Some(PartitionReadReply::ConsumerOffset {
            stored: Some(stored_offset),
            current_offset,
        }) => Ok(Json(ConsumerOffsetInfo {
            partition_id,
            current_offset,
            stored_offset,
        })),
        Some(PartitionReadReply::ConsumerOffset { stored: None, .. }) => Err(ReadError::NotFound),
        Some(PartitionReadReply::NotFound) => {
            Err(ReadError::Rejected(IggyError::TransientNotAccepted))
        }
        Some(PartitionReadReply::Rejected(error)) => Err(ReadError::Rejected(error)),
        Some(_) => Err(ReadError::Rejected(IggyError::InvalidCommand)),
        None => Err(ReadError::Timeout),
    }
}

/// `POST /streams/{stream_id}/topics/{topic_id}/messages`: produce a batch of
/// messages to a topic. The JSON body is the same `SendMessages` shape the
/// legacy server accepts (partitioning + base64 messages); stream and topic
/// come from the path.
///
/// An optional `context` (the shape of the offset store's) fences the batch
/// to the partition incarnation the caller saw, so a resend after a delete
/// and recreate is refused with `HistoryUnavailable` instead of appending to
/// the new partition. Without one, the batch is fenced with the context
/// current when the write is captured.
///
/// The batch rides partition consensus under a server-generated request ID.
/// Each caller POST is a new mutation; repeated POSTs do not automatically
/// deduplicate. Awaited writes on one token queue behind the session's data
/// gate, and the committed reply reaches its in-process reply slot.
/// The default answers 201 with the completed message durability only
/// after the quorum commit, with the commit's per-partition confirmations as
/// the body; `?ack=none` answers 202 + `Iggy-Durability: none` as soon as the
/// partition plane takes the batch and can carry no confirmation, having
/// awaited none. A refusal before the partition answers its error instead.
///
/// Every 201 carries a `confirmations` list, empty when the commit reported no
/// offsets. One shape for one meaning: a caller that had to tell "no body"
/// apart from "empty list" would be reading the same outcome two ways.
///
/// This node chooses the partition once. A node whose owner is not the
/// partition primary relays the batch to the primary with that choice
/// ([`serve_partition`]). `?ack=none` answers before the owner's gate decides,
/// so it first checks that this node is the primary ([`require_local_primary`])
/// and answers 202 only there.
pub(in crate::http) async fn send_messages(
    State(state): State<HttpState>,
    credential: Credential,
    Path((stream_id, topic_id)): Path<(String, String)>,
    Query(query): Query<ProduceQuery>,
    Extension(request): Extension<Arc<PartitionRequest>>,
    Json(mut command): Json<SendMessages>,
) -> Result<Response, PartitionWriteError> {
    let stream_id =
        Identifier::from_str_value(&stream_id).map_err(PartitionWriteError::Rejected)?;
    let topic_id = Identifier::from_str_value(&topic_id).map_err(PartitionWriteError::Rejected)?;
    // RBAC: authorize the produce on (stream, topic) before any consensus work.
    // Handler-side so an HTTP denial never enters the partition plane.
    authorize_data_plane(
        &state,
        credential.user_id,
        &stream_id,
        &topic_id,
        Permissioner::append_messages,
    )
    .map_err(PartitionWriteError::Rejected)?;
    // Rejects an oversized partitioning key and an empty or oversized batch.
    command.validate().map_err(PartitionWriteError::Rejected)?;
    let route = request.route();
    if let Some(route) = route {
        command.partitioning = Partitioning::partition_id(route.partition_id);
    }
    let policy = topic_durability(&state, &stream_id, &topic_id);
    // Names can be reused while the session gate is held by another request.
    let (stream_id, topic_id) = policy
        .map(super::reads::TopicDurability::identifiers)
        .transpose()
        .map_err(PartitionWriteError::Rejected)?
        .unwrap_or((stream_id, topic_id));
    let body = encode_send_messages(&stream_id, &topic_id, &command)
        .map_err(PartitionWriteError::Rejected)?;
    let (namespace, context) = capture_partition_write(
        &state,
        HTTP_UNREGISTERED_CLIENT_ID,
        Operation::SendMessages,
        &body,
        route.map(|route| route.context).or(command.context),
    )?;
    let serve_locally = async || {
        let session = credential.session(&state).await?;
        match query.ack {
            ProduceAck::Replicated => {
                let (reply, header) = partition_write_replicated(
                    &state,
                    &session,
                    Operation::SendMessages,
                    &body,
                    namespace,
                    context,
                    request.deadline,
                )
                .await?;
                let policy = policy.map_or(iggy_common::Durability::Replicated, |policy| {
                    policy.confirmed_policy(&state)
                });
                let durability = [(DURABILITY_HEADER, HeaderValue::from_static(policy.into()))];
                // An unreadable confirmation still answers 201: the batch committed,
                // only its offsets did not survive the reply.
                let confirmations = send_confirmations(&reply, &header)
                    .map(SendMessagesConfirmations::from)
                    .unwrap_or_default();
                Ok::<_, PartitionWriteError>(
                    (StatusCode::CREATED, durability, Json(confirmations)).into_response(),
                )
            }
            ProduceAck::None => {
                require_local_primary(&state, namespace)
                    .await
                    .map_err(PartitionWriteError::Rejected)?;
                produce_unacked(
                    &state,
                    &session,
                    body.clone(),
                    namespace,
                    context,
                    request.deadline,
                )
                .await?;
                Ok((
                    StatusCode::ACCEPTED,
                    [(DURABILITY_HEADER, HeaderValue::from_static(DURABILITY_NONE))],
                )
                    .into_response())
            }
        }
    };
    SendWrapper::new(serve_partition(
        &state,
        &request,
        namespace,
        context,
        PartitionEffect::Write,
        write_first_attempt(&state, &credential),
        serve_locally,
    ))
    .await
}

/// `PUT /streams/{stream_id}/topics/{topic_id}/consumer-offsets`: store a
/// consumer's offset. The JSON body is the same `StoreConsumerOffset` shape the
/// legacy server accepts (flattened `consumer_id`, optional `consumer_kind`,
/// `partition_id`, `offset`); stream and topic come from the path. Returns
/// 204 on commit, matching the legacy server. `consumer_kind=consumer_group`
/// is rejected ([`store_offset_wire_request`]) since HTTP has no persistent
/// connection and group memberships can change.
///
/// Data plane like a produce: the offset write is a replicated op on the
/// partition group's own consensus, awaited through the session's in-process
/// reply slot ([`partition_write_replicated`]). The wire op is pinned to
/// `ack = Quorum` - `?ack=none` is a produce-only surface. The consumer
/// identifier passes through on the wire; the dispatch resolvers hash named
/// consumers and rewrite group ids server-side, identically to TCP.
pub(in crate::http) async fn store_consumer_offset(
    State(state): State<HttpState>,
    credential: Credential,
    Path((stream_id, topic_id)): Path<(String, String)>,
    Extension(request): Extension<Arc<PartitionRequest>>,
    Json(command): Json<StoreConsumerOffset>,
) -> Result<Response, PartitionWriteError> {
    let stream_id =
        Identifier::from_str_value(&stream_id).map_err(PartitionWriteError::Rejected)?;
    let topic_id = Identifier::from_str_value(&topic_id).map_err(PartitionWriteError::Rejected)?;
    // RBAC: authorize the offset write on (stream, topic) handler-side.
    authorize_data_plane(
        &state,
        credential.user_id,
        &stream_id,
        &topic_id,
        Permissioner::store_consumer_offset,
    )
    .map_err(PartitionWriteError::Rejected)?;
    let body = store_offset_wire_request(&stream_id, &topic_id, &command)
        .map_err(PartitionWriteError::Rejected)?
        .to_bytes();
    let (namespace, context) = capture_partition_write(
        &state,
        HTTP_UNREGISTERED_CLIENT_ID,
        Operation::StoreConsumerOffset,
        &body,
        request
            .route()
            .map(|route| route.context)
            .or(command.context),
    )?;
    let serve_locally = async || {
        let session = credential.session(&state).await?;
        partition_write_replicated(
            &state,
            &session,
            Operation::StoreConsumerOffset,
            &body,
            namespace,
            context,
            request.deadline,
        )
        .await?;
        Ok::<_, PartitionWriteError>(StatusCode::NO_CONTENT.into_response())
    };
    SendWrapper::new(serve_partition(
        &state,
        &request,
        namespace,
        context,
        PartitionEffect::Write,
        write_first_attempt(&state, &credential),
        serve_locally,
    ))
    .await
}

/// `DELETE /streams/{stream_id}/topics/{topic_id}/consumer-offsets/{consumer_id}`:
/// delete a consumer's stored offset. The consumer id comes from the path and the
/// optional `consumer_kind` (defaulting to `consumer`) and `partition_id` come
/// from the query, the same `DeleteConsumerOffset` shape the legacy server
/// accepts. Returns 204 on commit, matching the legacy server; a delete of a
/// never-stored offset is denied by the partition primary
/// (`ReplyHeader.status`) and renders the legacy typed 404. Same replicated
/// partition write as [`store_consumer_offset`]. `consumer_kind=consumer_group`
/// is rejected because HTTP does not persist a connection and group memberships can change.
pub(in crate::http) async fn delete_consumer_offset(
    State(state): State<HttpState>,
    credential: Credential,
    Path((stream_id, topic_id, consumer_id)): Path<(String, String, String)>,
    Query(query): Query<DeleteConsumerOffset>,
    Extension(request): Extension<Arc<PartitionRequest>>,
) -> Result<Response, PartitionWriteError> {
    let stream_id =
        Identifier::from_str_value(&stream_id).map_err(PartitionWriteError::Rejected)?;
    let topic_id = Identifier::from_str_value(&topic_id).map_err(PartitionWriteError::Rejected)?;
    // RBAC: authorize the offset delete on (stream, topic) handler-side.
    authorize_data_plane(
        &state,
        credential.user_id,
        &stream_id,
        &topic_id,
        Permissioner::delete_consumer_offset,
    )
    .map_err(PartitionWriteError::Rejected)?;
    let consumer = Consumer {
        kind: query.consumer_kind,
        id: Identifier::from_str_value(&consumer_id).map_err(PartitionWriteError::Rejected)?,
    };
    let body = delete_offset_wire_request(&stream_id, &topic_id, &consumer, query.partition_id)
        .map_err(PartitionWriteError::Rejected)?
        .to_bytes();
    let (namespace, context) = capture_partition_write(
        &state,
        HTTP_UNREGISTERED_CLIENT_ID,
        Operation::DeleteConsumerOffset,
        &body,
        request.route().map(|route| route.context),
    )?;
    let serve_locally = async || {
        let session = credential.session(&state).await?;
        partition_write_replicated(
            &state,
            &session,
            Operation::DeleteConsumerOffset,
            &body,
            namespace,
            context,
            request.deadline,
        )
        .await?;
        Ok::<_, PartitionWriteError>(StatusCode::NO_CONTENT.into_response())
    };
    SendWrapper::new(serve_partition(
        &state,
        &request,
        namespace,
        context,
        PartitionEffect::Write,
        write_first_attempt(&state, &credential),
        serve_locally,
    ))
    .await
}

/// A write whose caller holds a session here runs here first. Without one,
/// serving here would first Register it, a metadata commit that a relay to
/// another primary wastes, so the write looks its primary up first.
fn write_first_attempt(state: &HttpInner, credential: &Credential) -> FirstAttempt {
    if credential.has_session(state) {
        FirstAttempt::Local
    } else {
        FirstAttempt::Lookup
    }
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
        body,
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
        body,
    ))
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /users`: create a user and render the committed reply as the same
/// `UserInfoDetails` JSON the legacy server returns.
///
/// The plaintext password rides the JSON body; [`submit_write`] hashes it on
/// shard 0 before the request enters consensus (see
/// [`crate::users::maybe_rewrite_user_password_request`]), so no plaintext is
/// ever replicated.
pub(in crate::http) async fn create_user(
    State(state): State<HttpState>,
    identity: Authenticated,
    JsonMembers {
        value: command,
        members,
    }: JsonMembers<CreateUser>,
) -> Result<Json<UserInfoDetails>, WriteError> {
    refuse_misplaced_options(OptionsScope::User, &members)?;
    // Rejects empty/oversized username or password before any consensus work.
    command.validate().map_err(WriteError::Rejected)?;
    let request = CreateUserRequest {
        username: WireName::new(&command.username)
            .map_err(|_| WriteError::Rejected(IggyError::InvalidUsername))?,
        password: command.password.expose_secret().to_string(),
        status: command.status.as_code(),
        permissions: command.permissions.as_ref().map(permissions_to_wire),
        options: command.options.to_wire().map_err(WriteError::Rejected)?,
    };
    let body = request.to_bytes();
    let payload = SendWrapper::new(submit_write(
        &state,
        &identity.session,
        Operation::CreateUser,
        body,
    ))
    .await?;
    Ok(Json(decode_user_details(&payload)?))
}

/// `PUT /users/{user_id}`: update the user's username, status and options. An
/// absent or `null` key keeps its value, a key in `reset` returns to its
/// default. Returns 204 on commit.
pub(in crate::http) async fn update_user(
    State(state): State<HttpState>,
    identity: Authenticated,
    Path(user_id): Path<String>,
    update: Result<JsonUpdate, WriteError>,
) -> Result<StatusCode, WriteError> {
    let user_id = Identifier::from_str_value(&user_id).map_err(WriteError::Rejected)?;
    let target = ResourceTarget::User {
        user_id: identifier_to_wire(&user_id).map_err(WriteError::Rejected)?,
    };
    submit_update(&state, &identity, target, update).await
}

/// Submit an update body as the generic `UpdateResource` the binary transports
/// send, so admission and apply are shared.
async fn submit_update(
    state: &HttpState,
    identity: &Authenticated,
    target: ResourceTarget,
    update: Result<JsonUpdate, WriteError>,
) -> Result<StatusCode, WriteError> {
    let JsonUpdate(update) = update?;
    let parts = update.to_patch().map_err(WriteError::Rejected)?;
    let request = UpdateResourceRequest {
        target,
        reset: parts.reset,
        options: parts.set,
    };
    let body = erasing_body(&request);
    erase_buffer(request.options.into_bytes());
    SendWrapper::new(submit_write(
        state,
        &identity.session,
        Operation::UpdateResource,
        body,
    ))
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Encode `request` into a body that zeroes itself when its last handle drops,
/// since its options may hold the stream key.
fn erasing_body(request: &impl WireEncode) -> Bytes {
    let mut buffer = BytesMut::with_capacity(request.encoded_size());
    request.encode(&mut buffer);
    Bytes::from_owner(Zeroizing::new(Vec::from(buffer)))
}

/// Refuses a create body member that names a stored option of `scope`, an
/// option the client placed beside `options` instead of inside it, and names
/// that member.
fn refuse_misplaced_options(
    scope: OptionsScope,
    members: &BTreeSet<String>,
) -> Result<(), WriteError> {
    descriptors_for(scope)
        .filter(|descriptor| descriptor.storage != OptionStorage::Column)
        .map(|descriptor| <&str>::from(descriptor.key))
        .find(|key| members.contains(*key))
        .map_or(Ok(()), |key| {
            Err(WriteError::MalformedMember(key.to_owned()))
        })
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
        body,
    ))
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `PUT /users/{user_id}/password`: change a user's password. Returns 204.
///
/// Both passwords ride the JSON body in plaintext. On shard 0, before the op
/// enters consensus, [`crate::users::maybe_rewrite_user_password_request`] hashes
/// the new password and strips the current one (so neither plaintext is ever
/// replicated), and verifies `current_password` against the target's stored
/// hash. A wrong current password is not denied pre-consensus: the op still
/// commits, carrying an empty new-password hash the replicated apply turns into
/// an `InvalidCredentials` no-op (surfaced here as 400), so the failure is
/// recorded against the caller's request id like any other committed outcome.
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
        body,
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
        body,
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
/// consensus: [`crate::pat::rewrite_pat_request_for_user`] (invoked inside
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
        body,
    ))
    .await?;
    // Reject a committed business error before splicing the secret; the success
    // payload is empty, so the returned slice is discarded.
    committed_payload(&committed).map_err(|error| match error {
        // The reply carries only the error code, so name the caller and the cap.
        WriteError::Rejected(IggyError::PersonalAccessTokensLimitReached(..)) => {
            WriteError::Rejected(IggyError::PersonalAccessTokensLimitReached(
                identity.session.user_id,
                state.shard.plane.metadata().max_tokens_per_user(),
            ))
        }
        error => error,
    })?;
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
        body,
    ))
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Mint a token only while the account verified by login or named by the
/// refresh token still occupies its user id.
fn issue_identity(
    inner: &HttpInner,
    user_id: u32,
    expected_created_at: IggyTimestamp,
) -> Result<Json<IdentityInfo>, CustomError> {
    let user_created_at = inner.ensure_active_user(user_id, Some(expected_created_at))?;
    let generated = inner.jwt.generate(user_id, user_created_at)?;
    Ok(Json(IdentityInfo {
        user_id: generated.user_id,
        access_token: Some(TokenInfo {
            token: generated.access_token,
            expiry: generated.access_token_expiry,
        }),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;

    use axum::body::to_bytes;
    use iggy_binary_protocol::responses::messages::{
        SendMessagesConfirmationResponse, SendMessagesResponse,
    };
    use iggy_common::{OptionInputMap, OptionInputValue, OptionKey};
    use serde_json::Value;

    use crate::http::reply::classify_partition_reply;
    use crate::http::wire::build_request_message;
    use crate::reply_frame::build_deny_reply;

    /// A create body can carry the stream key, so it zeroes itself on drop and
    /// leaves its options block for the handler to erase.
    #[test]
    fn given_create_with_stream_key_when_encoding_body_should_zero_on_drop_and_free_its_options() {
        let options = OptionInputMap(BTreeMap::from([(
            OptionKey::EncryptionKey.to_string(),
            OptionInputValue::String(
                "/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE="
                    .to_owned()
                    .into(),
            ),
        )]));
        let request = CreateStreamRequest {
            name: WireName::new("orders").expect("valid name"),
            options: options.to_wire().expect("encodes"),
        };
        let body = erasing_body(&request);
        assert_eq!(body, request.to_bytes(), "the plain encoding");
        assert!(
            body.try_into_mut().is_err(),
            "an owned buffer that zeroes on drop"
        );
        assert!(
            request.options.into_bytes().try_into_mut().is_ok(),
            "the only handle to the options block"
        );
    }

    /// The produce extractor keeps the caller's context, so the write is fenced
    /// with the incarnation the caller saw rather than the current one.
    #[test]
    fn given_send_body_with_context_when_extracting_should_keep_the_callers_context() {
        const CONTEXT: PartitionContext = PartitionContext {
            incarnation: 7,
            owner_generation: 0,
            metadata_op: 11,
        };
        let body = br#"{"partitioning":{"kind":"partition_id","value":"AgAAAA=="},"messages":[{"payload":"aGk="}],"context":{"incarnation":7,"owner_generation":0,"metadata_op":11}}"#;

        let Json(command) =
            Json::<SendMessages>::from_bytes(body).expect("a send body may carry a context");

        assert_eq!(command.context, Some(CONTEXT));
    }

    /// A body without a context leaves the context to the write path, which
    /// captures the current one, as every HTTP send did before the field.
    #[test]
    fn given_send_body_without_context_when_extracting_should_leave_the_capture_to_the_server() {
        let body = br#"{"partitioning":{"kind":"partition_id","value":"AgAAAA=="},"messages":[{"payload":"aGk="}]}"#;

        let Json(command) =
            Json::<SendMessages>::from_bytes(body).expect("a legacy send body stays valid");

        assert_eq!(command.context, None);
    }

    /// The partition primary refuses an incarnation that is not its own with a
    /// header-only `HistoryUnavailable` deny. The SDK refreshes its context
    /// only on this exact answer: a 400 whose `id` is 87.
    #[tokio::test]
    async fn given_stale_incarnation_deny_when_producing_should_answer_history_unavailable() {
        const ERROR_BODY_LIMIT: usize = 1024;
        let request = build_request_message(Operation::SendMessages, 42, 7, 1, &[]);
        let deny = build_deny_reply(
            request.header(),
            42,
            0,
            9,
            IggyError::HistoryUnavailable.as_code(),
        );

        let error = classify_partition_reply(&deny.into_generic().into_frozen())
            .expect_err("a deny is never a commit");
        let response = error.into_response();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), ERROR_BODY_LIMIT)
            .await
            .unwrap();
        let error: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(error["id"], IggyError::HistoryUnavailable.as_code());
    }

    /// A relay that lost a write's verdict answers `TransientNotCommitted`,
    /// which licenses only a replay of the same request id. HTTP mints a new
    /// id for every request, so no resend replays it and the write may still
    /// commit: it must render as the 504 unknown outcome of its write kind,
    /// never as a retryable 503.
    #[tokio::test]
    async fn given_not_committed_deny_when_writing_should_answer_outcome_unknown() {
        const ERROR_BODY_LIMIT: usize = 1024;
        for (operation, code) in [
            (Operation::SendMessages, "produce_timeout"),
            (Operation::StoreConsumerOffset, "offset_write_timeout"),
        ] {
            let request = build_request_message(operation, 42, 7, 1, &[]);
            let deny = build_deny_reply(
                request.header(),
                42,
                0,
                9,
                IggyError::TransientNotCommitted.as_code(),
            );

            let error = classify_partition_reply(&deny.into_generic().into_frozen())
                .expect_err("a deny is never a commit");
            let response = error.into_response();

            assert_eq!(
                response.status(),
                StatusCode::GATEWAY_TIMEOUT,
                "{operation:?}"
            );
            let body = to_bytes(response.into_body(), ERROR_BODY_LIMIT)
                .await
                .unwrap();
            let error: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(error["code"], code);
        }
    }

    /// Pins the produce response contract the SDKs decode: `snake_case` field
    /// names and a numeric `base_offset`.
    #[test]
    fn confirmation_renders_the_pinned_json_shape() {
        let response = SendMessagesResponse {
            confirmations: vec![SendMessagesConfirmationResponse {
                stream_id: 3,
                topic_id: 5,
                partition_id: 7,
                base_offset: 41,
            }],
        };
        let json = serde_json::to_string(&SendMessagesConfirmations::from(response))
            .expect("confirmations serialize");
        assert_eq!(
            json,
            r#"{"confirmations":[{"stream_id":3,"topic_id":5,"partition_id":7,"base_offset":41}]}"#
        );
    }

    /// A commit that reports no offsets renders the envelope with an empty
    /// list. Together with [`send_messages`] answering `Json` on every acked
    /// produce - the unreadable-confirmation path included - that is what makes
    /// `confirmations` present on every 201 body.
    #[test]
    fn empty_confirmations_render_an_empty_list() {
        let response = SendMessagesResponse {
            confirmations: Vec::new(),
        };
        let json = serde_json::to_string(&SendMessagesConfirmations::from(response))
            .expect("confirmations serialize");
        assert_eq!(json, r#"{"confirmations":[]}"#);
    }

    /// The HTTP SDK reads a 404 as "no stored offset", and a consumer that
    /// reads it resets its position. Only the owner's own "none" may send it.
    #[test]
    fn given_owner_without_the_partition_when_reading_offset_should_answer_not_accepted() {
        assert!(matches!(
            consumer_offset_reply(0, Some(PartitionReadReply::NotFound)),
            Err(ReadError::Rejected(IggyError::TransientNotAccepted))
        ));
        assert!(matches!(
            consumer_offset_reply(
                0,
                Some(PartitionReadReply::ConsumerOffset {
                    stored: None,
                    current_offset: 0,
                })
            ),
            Err(ReadError::NotFound)
        ));
    }
}
