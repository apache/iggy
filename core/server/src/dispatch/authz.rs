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
    DESCRIBE_OPTIONS_CODE, FLUSH_UNSAVED_BUFFER_CODE, GET_CLUSTER_METADATA_CODE,
    GET_CONSUMER_GROUP_CODE, GET_CONSUMER_GROUPS_CODE, GET_PERSONAL_ACCESS_TOKENS_CODE,
    GET_STATS_CODE, GET_STREAM_CODE, GET_STREAMS_CODE, GET_TOPIC_CODE, GET_TOPICS_CODE,
    GET_USER_CODE, GET_USERS_CODE,
};
use iggy_binary_protocol::requests::consumer_groups::{
    GetConsumerGroupRequest, GetConsumerGroupsRequest,
};
use iggy_binary_protocol::requests::streams::GetStreamRequest;
use iggy_binary_protocol::requests::topics::{GetTopicRequest, GetTopicsRequest};
use iggy_binary_protocol::requests::users::GetUserRequest;
use iggy_binary_protocol::{Operation, PrepareHeader, WireDecode, WireIdentifier, lookup_command};
use iggy_common::IggyError;
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use metadata::impls::metadata::StreamsFrontend;
use metadata::permissioner::Permissioner;
use server_common::Message;

use crate::responses::{resolve_stream_id, resolve_topic_id};
use crate::shell::{ShellBus, ShellShard};

/// Authorize a partition-plane op on its resolved (stream, topic) for the
/// acting user, returning the deny status code or `None` to proceed. The
/// namespace already resolved, so the entity exists; a `None` user id (which
/// the bound-session gate should preclude) fails closed with `Unauthenticated`
/// rather than allow an unattributed write.
pub(in crate::dispatch) fn authorize_partition_op<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    operation: Operation,
    user_id: Option<u32>,
    stream_id: usize,
    topic_id: usize,
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

/// Run an unscoped non-replicated-read rule for the acting user. A `None` user
/// id (only the pre-auth path, which serves ungated codes) fails closed.
pub(in crate::dispatch) fn authorize_uid<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    user_id: Option<u32>,
    rule: impl FnOnce(&Permissioner, u32) -> Result<(), IggyError>,
) -> Result<(), IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
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
pub(in crate::dispatch) fn authorize_partition_read<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    stream_id: &WireIdentifier,
    topic_id: &WireIdentifier,
    user_id: Option<u32>,
    rule: impl FnOnce(&Permissioner, u32, usize, usize) -> Result<(), IggyError>,
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
///
/// The gate is total over the protocol command table: every code the builder
/// serves has a named arm, and the tail refuses everything else (a replicated
/// code inside a `NonReplicated` header, a table-listed code with no arm, an
/// unknown code) instead of deferring it to the builder's catch-all.
pub(in crate::dispatch) fn authorize_default_read<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    code: u32,
    body: &[u8],
    user_id: Option<u32>,
) -> Result<(), IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    // A `u32` match cannot be exhaustive, so totality is by construction:
    // every code with a decision is named, and the tail refuses the rest.
    match code {
        GET_STATS_CODE => authorize_uid(shard, user_id, Permissioner::get_stats),
        GET_USERS_CODE => authorize_uid(shard, user_id, Permissioner::get_users),
        GET_USER_CODE => gate_user_scoped(shard, user_id, body),
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
        GET_STREAMS_CODE => authorize_uid(shard, user_id, Permissioner::get_streams),
        GET_STREAM_CODE => gate_stream_scoped::<GetStreamRequest, _, _, _, _>(
            shard,
            user_id,
            body,
            |request| &request.stream_id,
            Permissioner::get_stream,
        ),
        GET_TOPICS_CODE => gate_stream_scoped::<GetTopicsRequest, _, _, _, _>(
            shard,
            user_id,
            body,
            |request| &request.stream_id,
            Permissioner::get_topics,
        ),
        GET_TOPIC_CODE => gate_topic_scoped::<GetTopicRequest, _, _, _, _>(
            shard,
            user_id,
            body,
            |request| (&request.stream_id, &request.topic_id),
            Permissioner::get_topic,
        ),
        GET_CONSUMER_GROUP_CODE => gate_topic_scoped::<GetConsumerGroupRequest, _, _, _, _>(
            shard,
            user_id,
            body,
            |request| (&request.stream_id, &request.topic_id),
            Permissioner::get_consumer_group,
        ),
        GET_CONSUMER_GROUPS_CODE => gate_topic_scoped::<GetConsumerGroupsRequest, _, _, _, _>(
            shard,
            user_id,
            body,
            |request| (&request.stream_id, &request.topic_id),
            Permissioner::get_consumer_groups,
        ),
        // No on-demand flush primitive exists, and flush has no HTTP route, so
        // this arm is the only thing answering `FeatureUnavailable` for it.
        FLUSH_UNSAVED_BUFFER_CODE => Err(IggyError::FeatureUnavailable),
        // A replicated code smuggled inside a `NonReplicated` header keeps the
        // builder's `FeatureUnavailable`; a table-listed code with no arm above
        // and an unknown code are both refused as `InvalidCommand`. The builder
        // refuses the same set, so a new caller cannot land on a fail-open.
        _ => match lookup_command(code) {
            Some(meta) if meta.is_replicated() => Err(IggyError::FeatureUnavailable),
            _ => Err(IggyError::InvalidCommand),
        },
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
    authorize_uid(shard, user_id, Permissioner::get_user)
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
    authorize_uid(shard, user_id, |permissioner, uid| {
        rule(permissioner, uid, stream_id)
    })
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
    authorize_uid(shard, user_id, |permissioner, uid| {
        rule(permissioner, uid, stream_id, topic_id)
    })
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::test_support::{FIRST_BOOT, SpyBus, TestShard, test_shard};
    use iggy_binary_protocol::COMMAND_TABLE;
    use iggy_binary_protocol::codes::{
        CREATE_STREAM_CODE, GET_CLIENT_CODE, GET_CLIENTS_CODE, GET_CONSUMER_OFFSET_CODE,
        GET_ME_CODE, GET_SNAPSHOT_FILE_CODE, LOGIN_REGISTER_CODE, LOGIN_REGISTER_WITH_PAT_CODE,
        LOGIN_USER_CODE, LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE, LOGOUT_USER_CODE, PING_CODE,
        POLL_MESSAGES_CODE, SYNC_CONSUMER_GROUP_CODE,
    };
    use iggy_common::defaults::DEFAULT_ROOT_USER_ID;

    /// A code no `COMMAND_TABLE` entry claims.
    const UNKNOWN_CODE: u32 = 9999;

    /// The gate's answer as the wire sees it: status 0 allows, anything else
    /// is the deny code stamped into `ReplyHeader.status`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Verdict {
        Allow,
        Deny(u32),
    }

    /// Shard with root seeded, so the root column below exercises the
    /// permissioner rules rather than a missing-user deny.
    fn gate_shard() -> Rc<TestShard> {
        let bus = SpyBus::default();
        let shard = Rc::new(test_shard(&bus, 0, 1, FIRST_BOOT));
        shard
            .plane
            .metadata()
            .mux_stm
            .users()
            .ensure_root_user("iggy", "hash");
        shard
    }

    /// Gate a header-only frame (`body = &[]`) for `user_id`.
    fn gate(shard: &Rc<TestShard>, code: u32, user_id: Option<u32>) -> Verdict {
        match authorize_default_read(shard, code, &[], user_id) {
            Ok(()) => Verdict::Allow,
            Err(error) => Verdict::Deny(error.as_code()),
        }
    }

    /// Expected verdicts per non-replicated code for a header-only frame, as
    /// `(code, unbound caller, root)`. Header-only means the identifier-scoped
    /// arms never decode a body and defer to the builder's own error for both
    /// callers. Codes the reads router serves on its own arms (`PING`, `GET_ME`,
    /// `GET_CLIENTS`, ...) and the codes the classifier settles (the legacy
    /// logins) never reach the gate, which refuses them like any other armless
    /// code.
    fn expected_header_only_verdicts() -> Vec<(u32, Verdict, Verdict)> {
        let allow = Verdict::Allow;
        let unauthenticated = Verdict::Deny(IggyError::Unauthenticated.as_code());
        let invalid_command = Verdict::Deny(IggyError::InvalidCommand.as_code());
        let feature_unavailable = Verdict::Deny(IggyError::FeatureUnavailable.as_code());
        vec![
            (PING_CODE, invalid_command, invalid_command),
            (GET_STATS_CODE, unauthenticated, allow),
            (GET_SNAPSHOT_FILE_CODE, invalid_command, invalid_command),
            (GET_CLUSTER_METADATA_CODE, unauthenticated, allow),
            (GET_ME_CODE, invalid_command, invalid_command),
            (GET_CLIENT_CODE, invalid_command, invalid_command),
            (GET_CLIENTS_CODE, invalid_command, invalid_command),
            (GET_USER_CODE, allow, allow),
            (GET_USERS_CODE, unauthenticated, allow),
            (LOGIN_USER_CODE, invalid_command, invalid_command),
            (LOGOUT_USER_CODE, invalid_command, invalid_command),
            (LOGIN_REGISTER_CODE, invalid_command, invalid_command),
            (GET_PERSONAL_ACCESS_TOKENS_CODE, unauthenticated, allow),
            (
                LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE,
                invalid_command,
                invalid_command,
            ),
            (POLL_MESSAGES_CODE, invalid_command, invalid_command),
            (
                FLUSH_UNSAVED_BUFFER_CODE,
                feature_unavailable,
                feature_unavailable,
            ),
            (GET_CONSUMER_OFFSET_CODE, invalid_command, invalid_command),
            (GET_STREAM_CODE, allow, allow),
            (GET_STREAMS_CODE, unauthenticated, allow),
            (GET_TOPIC_CODE, allow, allow),
            (GET_TOPICS_CODE, allow, allow),
            (GET_CONSUMER_GROUP_CODE, allow, allow),
            (GET_CONSUMER_GROUPS_CODE, allow, allow),
            (SYNC_CONSUMER_GROUP_CODE, invalid_command, invalid_command),
            (
                LOGIN_REGISTER_WITH_PAT_CODE,
                invalid_command,
                invalid_command,
            ),
            (DESCRIBE_OPTIONS_CODE, unauthenticated, allow),
        ]
    }

    /// Every non-replicated table entry has a named decision above. A new
    /// entry without a row fails by name, so it cannot slip past the gate
    /// unnoticed; a row without a table entry is stale and fails too.
    #[test]
    fn gate_is_total_over_the_command_table() {
        let shard = gate_shard();
        let expected = expected_header_only_verdicts();
        for meta in COMMAND_TABLE.iter().filter(|meta| !meta.is_replicated()) {
            let Some((_, unbound, root)) = expected.iter().find(|row| row.0 == meta.code) else {
                panic!(
                    "non-replicated command {} ({}) has no ratchet row: decide it in \
                     authorize_default_read and add the row",
                    meta.name, meta.code
                );
            };
            assert_eq!(
                gate(&shard, meta.code, None),
                *unbound,
                "{} ({}) from an unbound caller",
                meta.name,
                meta.code
            );
            assert_eq!(
                gate(&shard, meta.code, Some(DEFAULT_ROOT_USER_ID)),
                *root,
                "{} ({}) from root",
                meta.name,
                meta.code
            );
        }
        for (code, _, _) in &expected {
            assert!(
                lookup_command(*code).is_some_and(|meta| !meta.is_replicated()),
                "ratchet row {code} names no non-replicated table entry"
            );
        }
    }

    #[test]
    fn unknown_code_is_refused_as_invalid_command() {
        assert!(
            lookup_command(UNKNOWN_CODE).is_none(),
            "test needs a code absent from COMMAND_TABLE"
        );
        let shard = gate_shard();
        let invalid_command = Verdict::Deny(IggyError::InvalidCommand.as_code());
        assert_eq!(gate(&shard, UNKNOWN_CODE, None), invalid_command);
        assert_eq!(
            gate(&shard, UNKNOWN_CODE, Some(DEFAULT_ROOT_USER_ID)),
            invalid_command
        );
    }

    #[test]
    fn replicated_code_in_a_non_replicated_frame_is_refused_as_feature_unavailable() {
        let shard = gate_shard();
        let feature_unavailable = Verdict::Deny(IggyError::FeatureUnavailable.as_code());
        assert_eq!(gate(&shard, CREATE_STREAM_CODE, None), feature_unavailable);
        assert_eq!(
            gate(&shard, CREATE_STREAM_CODE, Some(DEFAULT_ROOT_USER_ID)),
            feature_unavailable
        );
    }
}
