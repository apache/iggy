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
    DESCRIBE_OPTIONS_CODE, GET_CLUSTER_METADATA_CODE, GET_CONSUMER_GROUP_CODE,
    GET_CONSUMER_GROUPS_CODE, GET_PERSONAL_ACCESS_TOKENS_CODE, GET_STATS_CODE, GET_STREAM_CODE,
    GET_STREAMS_CODE, GET_TOPIC_CODE, GET_TOPICS_CODE, GET_USER_CODE, GET_USERS_CODE,
};
use iggy_binary_protocol::requests::consumer_groups::{
    GetConsumerGroupRequest, GetConsumerGroupsRequest,
};
use iggy_binary_protocol::requests::streams::GetStreamRequest;
use iggy_binary_protocol::requests::topics::{GetTopicRequest, GetTopicsRequest};
use iggy_binary_protocol::requests::users::GetUserRequest;
use iggy_binary_protocol::{
    Operation, PrepareHeader, RoutedRequestHeader, WireDecode, WireIdentifier,
};
use iggy_common::{IggyError, Permissions};
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use metadata::impls::metadata::StreamsFrontend;
use metadata::permissioner::Permissioner;
use server_common::Message;
use tracing::warn;

use crate::external_auth::is_synthetic_user_id;
use crate::responses::{
    build_deny_reply, current_metadata_commit, resolve_stream_id, resolve_topic_id,
};
use crate::shell::{ShellBus, ShellShard};

/// Check session-scoped permissions for a synthetic user. Returns
/// `Some(Ok(()))` if permitted, `Some(Err(Unauthorized))` if denied, or `None`
/// if the user is not synthetic (caller falls through to the Permissioner).
pub(super) fn check_session_permission(
    user_id: Option<u32>,
    session_perms: Option<&Permissions>,
    check: impl FnOnce(&Permissions) -> bool,
) -> Option<Result<(), IggyError>> {
    let user_id = user_id?;
    if !is_synthetic_user_id(user_id) {
        return None;
    }
    match session_perms {
        Some(perms) if check(perms) => Some(Ok(())),
        _ => Some(Err(IggyError::Unauthorized)),
    }
}

/// Check if inline permissions allow sending messages to (stream, topic),
/// mirroring the `Permissioner::append_messages` inheritance chain.
pub fn can_send_messages(perms: &Permissions, stream_id: usize, topic_id: usize) -> bool {
    let g = &perms.global;
    if g.send_messages || g.manage_streams || g.manage_topics {
        return true;
    }
    if let Some(sp) = perms.streams.as_ref().and_then(|s| s.get(&stream_id)) {
        if sp.send_messages || sp.manage_stream || sp.manage_topics {
            return true;
        }
        if sp
            .topics
            .as_ref()
            .and_then(|t| t.get(&topic_id))
            .is_some_and(|tp| tp.send_messages || tp.manage_topic)
        {
            return true;
        }
    }
    false
}

/// Check if inline permissions allow polling messages from (stream, topic),
/// mirroring the `Permissioner::poll_messages` inheritance chain.
pub fn can_poll_messages(perms: &Permissions, stream_id: usize, topic_id: usize) -> bool {
    let g = &perms.global;
    if g.poll_messages || g.read_topics || g.manage_topics || g.read_streams || g.manage_streams {
        return true;
    }
    if let Some(sp) = perms.streams.as_ref().and_then(|s| s.get(&stream_id)) {
        if sp.poll_messages
            || sp.read_topics
            || sp.manage_topics
            || sp.read_stream
            || sp.manage_stream
        {
            return true;
        }
        if sp
            .topics
            .as_ref()
            .and_then(|t| t.get(&topic_id))
            .is_some_and(|tp| tp.poll_messages || tp.read_topic || tp.manage_topic)
        {
            return true;
        }
    }
    false
}

/// Check if inline permissions allow reading a specific stream.
pub fn can_read_stream(perms: &Permissions, stream_id: usize) -> bool {
    let g = &perms.global;
    if g.manage_streams || g.read_streams {
        return true;
    }
    perms
        .streams
        .as_ref()
        .and_then(|s| s.get(&stream_id))
        .is_some_and(|sp| sp.manage_stream || sp.read_stream)
}

/// Check if inline permissions allow listing topics in a specific stream.
/// Mirrors `Permissioner::get_topics`: stream-level read OR topic-level
/// read/manage grants are sufficient (no per-topic ID needed).
pub fn can_list_topics(perms: &Permissions, stream_id: usize) -> bool {
    let g = &perms.global;
    if g.read_streams || g.manage_streams || g.manage_topics || g.read_topics {
        return true;
    }
    perms
        .streams
        .as_ref()
        .and_then(|s| s.get(&stream_id))
        .is_some_and(|sp| sp.manage_stream || sp.read_stream || sp.manage_topics || sp.read_topics)
}

/// Check if inline permissions allow reading a specific topic.
pub fn can_read_topic(perms: &Permissions, stream_id: usize, topic_id: usize) -> bool {
    let g = &perms.global;
    if g.read_streams || g.manage_streams || g.manage_topics || g.read_topics {
        return true;
    }
    if let Some(sp) = perms.streams.as_ref().and_then(|s| s.get(&stream_id)) {
        if sp.manage_stream || sp.read_stream || sp.manage_topics || sp.read_topics {
            return true;
        }
        if sp
            .topics
            .as_ref()
            .and_then(|t| t.get(&topic_id))
            .is_some_and(|tp| tp.manage_topic || tp.read_topic)
        {
            return true;
        }
    }
    false
}

/// Authorize a partition-plane op on its resolved (stream, topic) for the
/// acting user, returning the deny status code or `None` to proceed. The
/// namespace already resolved, so the entity exists; a `None` user id (which
/// the bound-session gate should preclude) fails closed with `Unauthenticated`
/// rather than allow an unattributed write.
///
/// When `session_perms` is provided and the user is synthetic (external auth
/// inline-grant), checks the inline permissions directly instead of the
/// Permissioner.
pub(in crate::dispatch) fn authorize_partition_op<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    operation: Operation,
    user_id: Option<u32>,
    stream_id: usize,
    topic_id: usize,
    session_perms: Option<&Permissions>,
) -> Option<u32>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let Some(user_id) = user_id else {
        return Some(IggyError::Unauthenticated.as_code());
    };
    if let Some(result) =
        check_session_permission(Some(user_id), session_perms, |perms| match operation {
            Operation::SendMessages => can_send_messages(perms, stream_id, topic_id),
            Operation::StoreConsumerOffset | Operation::DeleteConsumerOffset => {
                can_poll_messages(perms, stream_id, topic_id)
            }
            _ => false,
        })
    {
        return result.err().map(|e| e.as_code());
    }
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
                Operation::StoreConsumerOffset => {
                    permissioner.store_consumer_offset(user_id, stream_id, topic_id)
                }
                Operation::DeleteConsumerOffset => {
                    permissioner.delete_consumer_offset(user_id, stream_id, topic_id)
                }
                // The caller only routes the three partition ops above here. The
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
            "failed to surface request denial"
        );
    }
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
            "failed to surface unbound request denial"
        );
    }
}

/// Run an unscoped non-replicated-read rule for the acting user. A `None` user
/// id (only the pre-auth path, which serves ungated codes) fails closed.
///
/// When `session_perms` is provided and the user is synthetic, runs
/// `session_check` against the inline permissions instead of the Permissioner.
pub(in crate::dispatch) fn authorize_uid<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    user_id: Option<u32>,
    rule: impl FnOnce(&Permissioner, u32) -> Result<(), IggyError>,
    session_perms: Option<&Permissions>,
    session_check: impl FnOnce(&Permissions) -> bool,
) -> Result<(), IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let user_id = user_id.ok_or(IggyError::Unauthenticated)?;
    if let Some(result) = check_session_permission(Some(user_id), session_perms, session_check) {
        return result;
    }
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
///
/// When `session_perms` is provided and the user is synthetic, checks the
/// inline permissions directly (`poll_messages` inheritance chain).
pub(in crate::dispatch) fn authorize_partition_read<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    stream_id: &WireIdentifier,
    topic_id: &WireIdentifier,
    user_id: Option<u32>,
    rule: impl FnOnce(&Permissioner, u32, usize, usize) -> Result<(), IggyError>,
    session_perms: Option<&Permissions>,
) -> Option<u32>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let Some(user_id) = user_id else {
        return Some(IggyError::Unauthenticated.as_code());
    };
    let (stream_id, topic_id) = resolve_topic_scope(shard, stream_id, topic_id)?;
    if let Some(result) = check_session_permission(Some(user_id), session_perms, |perms| {
        can_poll_messages(perms, stream_id, topic_id)
    }) {
        return result.err().map(|e| e.as_code());
    }
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
/// authentication is its whole rule, and `GET_CLUSTER_METADATA` -- which
/// describes the private replica network -- is gated the same way.
/// When `session_perms` is provided and the user is synthetic, checks inline
/// permissions instead of the Permissioner.
pub(in crate::dispatch) fn authorize_default_read<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    code: u32,
    body: &[u8],
    user_id: Option<u32>,
    session_perms: Option<&Permissions>,
) -> Result<(), IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    // A `u32` match cannot be exhaustive: every gated code is named explicitly,
    // and the final arm is the ungated set the builder serves without a rule.
    match code {
        GET_STATS_CODE => authorize_uid(
            shard,
            user_id,
            Permissioner::get_stats,
            session_perms,
            |p| p.global.manage_servers || p.global.read_servers,
        ),
        GET_USERS_CODE => authorize_uid(
            shard,
            user_id,
            Permissioner::get_users,
            session_perms,
            |p| p.global.manage_users || p.global.read_users,
        ),
        GET_USER_CODE => gate_user_scoped(shard, user_id, body, session_perms),
        // Self-scoped: lists only the caller's own tokens, so there is no
        // permissioner rule to run (legacy runs none either).
        GET_PERSONAL_ACCESS_TOKENS_CODE => user_id.map(|_| ()).ok_or(IggyError::Unauthenticated),
        // Static catalog plus node defaults; nothing resource-scoped to gate
        // beyond authentication.
        DESCRIBE_OPTIONS_CODE => user_id.map(|_| ()).ok_or(IggyError::Unauthenticated),
        // Defence in depth: `handle_client_request` already denies an unbound
        // transport with an `Unauthenticated` Reply before it reaches the
        // builder, so this arm only ever fires if that gate is bypassed.
        GET_CLUSTER_METADATA_CODE => user_id.map(|_| ()).ok_or(IggyError::Unauthenticated),
        GET_STREAMS_CODE => authorize_uid(
            shard,
            user_id,
            Permissioner::get_streams,
            session_perms,
            |p| p.global.manage_streams || p.global.read_streams,
        ),
        GET_STREAM_CODE => gate_stream_scoped::<GetStreamRequest, _, _, _, _>(
            shard,
            user_id,
            body,
            |request| &request.stream_id,
            Permissioner::get_stream,
            session_perms,
            can_read_stream,
        ),
        GET_TOPICS_CODE => gate_stream_scoped::<GetTopicsRequest, _, _, _, _>(
            shard,
            user_id,
            body,
            |request| &request.stream_id,
            Permissioner::get_topics,
            session_perms,
            can_list_topics,
        ),
        GET_TOPIC_CODE => gate_topic_scoped::<GetTopicRequest, _, _, _, _>(
            shard,
            user_id,
            body,
            |request| (&request.stream_id, &request.topic_id),
            Permissioner::get_topic,
            session_perms,
        ),
        GET_CONSUMER_GROUP_CODE => gate_topic_scoped::<GetConsumerGroupRequest, _, _, _, _>(
            shard,
            user_id,
            body,
            |request| (&request.stream_id, &request.topic_id),
            Permissioner::get_consumer_group,
            session_perms,
        ),
        GET_CONSUMER_GROUPS_CODE => gate_topic_scoped::<GetConsumerGroupsRequest, _, _, _, _>(
            shard,
            user_id,
            body,
            |request| (&request.stream_id, &request.topic_id),
            Permissioner::get_consumer_groups,
            session_perms,
        ),
        _ => Ok(()),
    }
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

/// Gate `GET_USER`: decode the request and resolve its target against the
/// committed users STM. A target resolving to the caller passes without any
/// permissioner rule, matching the legacy server, which skipped `read_users`
/// when a user fetched its own account. A malformed body or a resolution miss
/// returns `Ok(())` so the builder's own error / not-found reply holds
/// (decode-and-notfound-before-permission); any other target runs
/// [`Permissioner::get_user`].
fn gate_user_scoped<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    user_id: Option<u32>,
    body: &[u8],
    session_perms: Option<&Permissions>,
) -> Result<(), IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let Ok(request) = GetUserRequest::decode_from(body) else {
        return Ok(());
    };
    let Some(target_id) = shard
        .plane
        .metadata()
        .mux_stm
        .users()
        .read(|users| users.resolve_user_id(&request.user_id))
    else {
        return Ok(());
    };
    if user_id.is_some_and(|caller_id| caller_id as usize == target_id) {
        return Ok(());
    }
    authorize_uid(shard, user_id, Permissioner::get_user, session_perms, |p| {
        p.global.manage_users || p.global.read_users
    })
}

/// Gate a stream-scoped read: decode the request, project its wire stream id,
/// resolve it to the committed slab id, then run `rule`. A malformed body or a
/// resolution miss returns `Ok(())` so the builder's own error / not-found
/// reply is what the client sees (decode-and-notfound-before-permission).
fn gate_stream_scoped<T: WireDecode, B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    user_id: Option<u32>,
    body: &[u8],
    stream_id: impl FnOnce(&T) -> &WireIdentifier,
    rule: impl FnOnce(&Permissioner, u32, usize) -> Result<(), IggyError>,
    session_perms: Option<&Permissions>,
    session_rule: impl FnOnce(&Permissions, usize) -> bool,
) -> Result<(), IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let Ok(request) = T::decode_from(body) else {
        return Ok(());
    };
    let Some(stream_id) = resolve_stream_scope(shard, stream_id(&request)) else {
        return Ok(());
    };
    authorize_uid(
        shard,
        user_id,
        |permissioner, uid| rule(permissioner, uid, stream_id),
        session_perms,
        |p| session_rule(p, stream_id),
    )
}

/// Gate a topic-scoped read: decode the request, project its wire (stream,
/// topic) pair, resolve both to committed slab ids, then run `rule`. A malformed
/// body or a resolution miss on either returns `Ok(())` so the builder's own
/// error / not-found reply holds (decode-and-notfound-before-permission).
fn gate_topic_scoped<T: WireDecode, B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    user_id: Option<u32>,
    body: &[u8],
    ids: impl FnOnce(&T) -> (&WireIdentifier, &WireIdentifier),
    rule: impl FnOnce(&Permissioner, u32, usize, usize) -> Result<(), IggyError>,
    session_perms: Option<&Permissions>,
) -> Result<(), IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let Ok(request) = T::decode_from(body) else {
        return Ok(());
    };
    let (stream_id, topic_id) = ids(&request);
    let Some((stream_id, topic_id)) = resolve_topic_scope(shard, stream_id, topic_id) else {
        return Ok(());
    };
    authorize_uid(
        shard,
        user_id,
        |permissioner, uid| rule(permissioner, uid, stream_id, topic_id),
        session_perms,
        |p| can_read_topic(p, stream_id, topic_id),
    )
}

/// Resolve a wire stream identifier to its committed slab id, or `None` on a
/// miss (the gate then falls through to the builder's not-found reply).
fn resolve_stream_scope<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    stream_id: &WireIdentifier,
) -> Option<usize>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
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
fn resolve_topic_scope<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    stream_id: &WireIdentifier,
    topic_id: &WireIdentifier,
) -> Option<(usize, usize)>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    shard.plane.metadata().mux_stm.streams().read(|inner| {
        let stream_id = resolve_stream_id(inner, stream_id)?;
        let topic_id = resolve_topic_id(inner, stream_id, topic_id)?;
        Some((stream_id, topic_id))
    })
}
