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

//! Dispatch-time authorization.
//!
//! The metadata STM enforces RBAC in-apply for replicated control ops. What
//! that gate cannot see -- partition-plane ops and non-replicated reads, both
//! decided on the connection's own shard without a replicated apply -- is gated
//! here against the live permissioner via `Users::authorize`. A denial rides
//! `ReplyHeader.status` (empty body), the request-level error channel the SDK
//! peeks before body decode. Root holds every grant in the permissioner, so the
//! rules pass for it without any user-id short-circuit.

use std::rc::Rc;

use consensus::MetadataHandle;
use iggy_binary_protocol::codes::{
    GET_CONSUMER_GROUP_CODE, GET_CONSUMER_GROUPS_CODE, GET_PERSONAL_ACCESS_TOKENS_CODE,
    GET_STATS_CODE, GET_STREAM_CODE, GET_STREAMS_CODE, GET_TOPIC_CODE, GET_TOPICS_CODE,
    GET_USER_CODE, GET_USERS_CODE,
};
use iggy_binary_protocol::requests::consumer_groups::{
    GetConsumerGroupRequest, GetConsumerGroupsRequest,
};
use iggy_binary_protocol::requests::streams::GetStreamRequest;
use iggy_binary_protocol::requests::topics::{GetTopicRequest, GetTopicsRequest};
use iggy_binary_protocol::{Operation, PrepareHeader, RequestHeader, WireDecode, WireIdentifier};
use iggy_common::IggyError;
use journal::{Journal, JournalHandle};
use metadata::impls::metadata::StreamsFrontend;
use metadata::permissioner::Permissioner;
use server_common::Message;
use tracing::warn;

use crate::bootstrap::{ShellBus, ShellShard};
use crate::responses::{
    build_deny_reply, current_metadata_commit, resolve_stream_id, resolve_topic_id,
};

/// Authorize a partition-plane op on its resolved (stream, topic) for the
/// acting user, returning the deny status code or `None` to proceed. The
/// namespace already resolved, so the entity exists; a `None` user id (which
/// the bound-session gate should preclude) fails closed with `Unauthenticated`
/// rather than allow an unattributed write.
pub(super) fn authorize_partition_op<B, MJ, S>(
    shard: &Rc<ShellShard<B, MJ, S>>,
    operation: Operation,
    user_id: Option<u32>,
    stream_id: usize,
    topic_id: usize,
) -> Option<u32>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
{
    let Some(user_id) = user_id else {
        return Some(IggyError::Unauthenticated.as_code());
    };
    let decision =
        shard
            .plane
            .metadata()
            .mux_stm
            .users()
            .authorize(|permissioner| match operation {
                Operation::SendMessages => {
                    permissioner.append_messages(user_id, stream_id, topic_id)
                }
                Operation::StoreConsumerOffset | Operation::StoreConsumerOffset2 => {
                    permissioner.store_consumer_offset(user_id, stream_id, topic_id)
                }
                Operation::DeleteConsumerOffset | Operation::DeleteConsumerOffset2 => {
                    permissioner.delete_consumer_offset(user_id, stream_id, topic_id)
                }
                // The caller only routes the five partition ops above here. The
                // rest are listed exhaustively (no `_`) so a newly added op
                // forces a gate decision at compile time instead of silently
                // slipping through ungated.
                Operation::Reserved
                | Operation::Register
                | Operation::NonReplicated
                | Operation::Logout
                | Operation::CreateTopicWithAssignments
                | Operation::CreatePartitionsWithAssignments
                | Operation::RemoveConsumerGroupMember
                | Operation::CompleteConsumerGroupRevocation
                | Operation::TruncatePartition
                | Operation::CreateStream
                | Operation::UpdateStream
                | Operation::DeleteStream
                | Operation::PurgeStream
                | Operation::CreateTopic
                | Operation::UpdateTopic
                | Operation::DeleteTopic
                | Operation::PurgeTopic
                | Operation::CreatePartitions
                | Operation::DeletePartitions
                | Operation::DeleteSegments
                | Operation::CreateConsumerGroup
                | Operation::DeleteConsumerGroup
                | Operation::CreateUser
                | Operation::UpdateUser
                | Operation::DeleteUser
                | Operation::ChangePassword
                | Operation::UpdatePermissions
                | Operation::CreatePersonalAccessToken
                | Operation::DeletePersonalAccessToken
                | Operation::JoinConsumerGroup
                | Operation::LeaveConsumerGroup => Ok(()),
            });
    decision.err().map(|error| error.as_code())
}

/// Reply to a denied partition op with the op's frame: empty body + nonzero
/// `status`. Distinct from `send_empty_partition_reply` (status 0, the
/// fail-fast "not routable" ack); here the SDK peeks the status and surfaces
/// the typed authorization error. Same lockstep reasoning: a silent drop would
/// wedge every later request on the connection.
#[allow(clippy::future_not_send)]
pub(super) async fn send_partition_deny_reply<B, MJ, S>(
    shard: &Rc<ShellShard<B, MJ, S>>,
    transport_client_id: u128,
    request_header: &RequestHeader,
    status: u32,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
{
    let commit = current_metadata_commit(shard);
    let reply = build_deny_reply(request_header, transport_client_id, 0, commit, status);
    if let Err(error) = shard
        .bus
        .send_to_client(transport_client_id, reply.into_generic().into_frozen())
        .await
    {
        warn!(
            transport_client_id,
            status,
            error = %error,
            operation = ?request_header.operation,
            "failed to surface partition authz denial"
        );
    }
}

/// Run an unscoped non-replicated-read rule for the acting user. A `None` user
/// id (only the pre-auth path, which serves ungated codes) fails closed.
pub(super) fn authorize_uid<B, MJ, S>(
    shard: &Rc<ShellShard<B, MJ, S>>,
    user_id: Option<u32>,
    rule: impl FnOnce(&Permissioner, u32) -> Result<(), IggyError>,
) -> Result<(), IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
{
    let user_id = user_id.ok_or(IggyError::Unauthenticated)?;
    shard
        .plane
        .metadata()
        .mux_stm
        .users()
        .authorize(|permissioner| rule(permissioner, user_id))
}

/// Authorize a partition-plane non-replicated read (poll / consumer-offset) on
/// (stream, topic). `None` proceeds (allowed, or a resolution miss the caller's
/// own not-found path handles); `Some(status)` denies. A `None` user id fails
/// closed.
pub(super) fn authorize_partition_read<B, MJ, S>(
    shard: &Rc<ShellShard<B, MJ, S>>,
    stream_id: &WireIdentifier,
    topic_id: &WireIdentifier,
    user_id: Option<u32>,
    rule: impl FnOnce(&Permissioner, u32, usize, usize) -> Result<(), IggyError>,
) -> Option<u32>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
{
    let Some(user_id) = user_id else {
        return Some(IggyError::Unauthenticated.as_code());
    };
    let (stream_id, topic_id) = resolve_topic_scope(shard, stream_id, topic_id)?;
    shard
        .plane
        .metadata()
        .mux_stm
        .users()
        .authorize(|permissioner| rule(permissioner, user_id, stream_id, topic_id))
        .err()
        .map(|error| error.as_code())
}

/// Authorize a non-replicated read routed through `build_non_replicated_response`
/// (`handle_default_non_replicated`). `Ok(())` allows -- including a resolution
/// miss, which falls through to the builder's own not-found reply so the legacy
/// notfound-before-permission ordering holds. `Err` denies with that code.
/// Unscoped rules gate directly; identifier-scoped rules resolve (stream[,
/// topic]) against committed state first. The PAT list is self-scoped, so
/// authentication is its whole rule. `GET_CLUSTER_METADATA` is deliberately
/// pre-auth (bootstrap / leader discovery; the dispatch allowlist admits it
/// unauthenticated) and, like every other code the builder serves, is ungated
/// here.
pub(super) fn authorize_default_read<B, MJ, S>(
    shard: &Rc<ShellShard<B, MJ, S>>,
    code: u32,
    body: &[u8],
    user_id: Option<u32>,
) -> Result<(), IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
{
    // A `u32` match cannot be exhaustive: every gated code is named explicitly,
    // and the final arm is the ungated set the builder serves without a rule.
    match code {
        GET_STATS_CODE => authorize_uid(shard, user_id, Permissioner::get_stats),
        GET_USERS_CODE => authorize_uid(shard, user_id, Permissioner::get_users),
        GET_USER_CODE => authorize_uid(shard, user_id, Permissioner::get_user),
        // Self-scoped: lists only the caller's own tokens, so there is no
        // permissioner rule to run (legacy runs none either).
        GET_PERSONAL_ACCESS_TOKENS_CODE => user_id.map(|_| ()).ok_or(IggyError::Unauthenticated),
        GET_STREAMS_CODE => authorize_uid(shard, user_id, Permissioner::get_streams),
        GET_STREAM_CODE => gate_stream_scoped::<GetStreamRequest, _, _, _>(
            shard,
            user_id,
            body,
            |request| &request.stream_id,
            Permissioner::get_stream,
        ),
        GET_TOPICS_CODE => gate_stream_scoped::<GetTopicsRequest, _, _, _>(
            shard,
            user_id,
            body,
            |request| &request.stream_id,
            Permissioner::get_topics,
        ),
        GET_TOPIC_CODE => gate_topic_scoped::<GetTopicRequest, _, _, _>(
            shard,
            user_id,
            body,
            |request| (&request.stream_id, &request.topic_id),
            Permissioner::get_topic,
        ),
        GET_CONSUMER_GROUP_CODE => gate_topic_scoped::<GetConsumerGroupRequest, _, _, _>(
            shard,
            user_id,
            body,
            |request| (&request.stream_id, &request.topic_id),
            Permissioner::get_consumer_group,
        ),
        GET_CONSUMER_GROUPS_CODE => gate_topic_scoped::<GetConsumerGroupsRequest, _, _, _>(
            shard,
            user_id,
            body,
            |request| (&request.stream_id, &request.topic_id),
            Permissioner::get_consumer_groups,
        ),
        _ => Ok(()),
    }
}

/// Reply to a denied non-replicated read with the request's reply frame: empty
/// body + nonzero `status`. The SDK peeks the status before body decode and
/// surfaces the typed error, so a poll denial never reaches the empty-poll
/// "0 messages" body path.
#[allow(clippy::future_not_send)]
pub(super) async fn send_non_replicated_deny<B, MJ, S>(
    shard: &Rc<ShellShard<B, MJ, S>>,
    request: &Message<RequestHeader>,
    transport_client_id: u128,
    status: u32,
) where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
{
    let commit = current_metadata_commit(shard);
    let reply = build_deny_reply(
        request.header(),
        request.header().client,
        request.header().session,
        commit,
        status,
    );
    if let Err(error) = shard
        .bus
        .send_to_client(transport_client_id, reply.into_generic().into_frozen())
        .await
    {
        warn!(
            transport_client_id,
            status,
            error = %error,
            "failed to surface non-replicated authz denial"
        );
    }
}

/// Gate a stream-scoped read: decode the request, project its wire stream id,
/// resolve it to the committed slab id, then run `rule`. A malformed body or a
/// resolution miss returns `Ok(())` so the builder's own error / not-found
/// reply is what the client sees (decode-and-notfound-before-permission).
fn gate_stream_scoped<T: WireDecode, B, MJ, S>(
    shard: &Rc<ShellShard<B, MJ, S>>,
    user_id: Option<u32>,
    body: &[u8],
    stream_id: impl FnOnce(&T) -> &WireIdentifier,
    rule: impl FnOnce(&Permissioner, u32, usize) -> Result<(), IggyError>,
) -> Result<(), IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
{
    let Ok(request) = T::decode_from(body) else {
        return Ok(());
    };
    let Some(stream_id) = resolve_stream_scope(shard, stream_id(&request)) else {
        return Ok(());
    };
    authorize_uid(shard, user_id, |permissioner, uid| {
        rule(permissioner, uid, stream_id)
    })
}

/// Gate a topic-scoped read: decode the request, project its wire (stream,
/// topic) pair, resolve both to committed slab ids, then run `rule`. A malformed
/// body or a resolution miss on either returns `Ok(())` so the builder's own
/// error / not-found reply holds (decode-and-notfound-before-permission).
fn gate_topic_scoped<T: WireDecode, B, MJ, S>(
    shard: &Rc<ShellShard<B, MJ, S>>,
    user_id: Option<u32>,
    body: &[u8],
    ids: impl FnOnce(&T) -> (&WireIdentifier, &WireIdentifier),
    rule: impl FnOnce(&Permissioner, u32, usize, usize) -> Result<(), IggyError>,
) -> Result<(), IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
{
    let Ok(request) = T::decode_from(body) else {
        return Ok(());
    };
    let (stream_id, topic_id) = ids(&request);
    let Some((stream_id, topic_id)) = resolve_topic_scope(shard, stream_id, topic_id) else {
        return Ok(());
    };
    authorize_uid(shard, user_id, |permissioner, uid| {
        rule(permissioner, uid, stream_id, topic_id)
    })
}

/// Resolve a wire stream identifier to its committed slab id, or `None` on a
/// miss (the gate then falls through to the builder's not-found reply).
fn resolve_stream_scope<B, MJ, S>(
    shard: &Rc<ShellShard<B, MJ, S>>,
    stream_id: &WireIdentifier,
) -> Option<usize>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
{
    shard
        .plane
        .metadata()
        .mux_stm
        .streams()
        .read(|inner| resolve_stream_id(inner, stream_id))
}

/// Resolve a wire (stream, topic) pair to committed slab ids, or `None` if
/// either misses.
fn resolve_topic_scope<B, MJ, S>(
    shard: &Rc<ShellShard<B, MJ, S>>,
    stream_id: &WireIdentifier,
    topic_id: &WireIdentifier,
) -> Option<(usize, usize)>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<MJ::Storage, Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
{
    shard.plane.metadata().mux_stm.streams().read(|inner| {
        let stream_id = resolve_stream_id(inner, stream_id)?;
        let topic_id = resolve_topic_id(inner, stream_id, topic_id)?;
        Some((stream_id, topic_id))
    })
}
