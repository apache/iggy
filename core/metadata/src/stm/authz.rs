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

//! In-apply control-plane authorization for the metadata state machine.
//!
//! Every committed client op is gated here before its `StateHandler::apply`
//! runs. A denial returns an `Unauthorized` [`ApplyReply`] WITHOUT touching
//! state, so the op commits as a deterministic no-op whose error rides the
//! cached reply and replays on retry, exactly like a business rejection.
//!
//! Replay-determinism invariant: `authorize` is a pure function of the
//! prepare header (`operation` + the acting `user_id`, stamped into the
//! replicated header at submit time) and the committed permission/stream
//! state as of the op immediately before this one. That state is applied in
//! the same order on every replica, so the primary (`on_ack`), each backup
//! (`commit_journal`), and WAL replay (`recover`) all recompute the identical
//! decision. This is why identity must ride the header rather than being
//! resolved from a transport-local session table: a backup has no connection
//! for the acting client. The commit paths also check the replicated session
//! table through `reject_stale_session` before this permission gate.

use crate::MuxStateMachine;
use crate::impls::metadata::StreamsFrontend;
use crate::permissioner::Permissioner;
use crate::stm::consumer_group::{JoinConsumerGroupRequest, LeaveConsumerGroupRequest};
use crate::stm::result::{ApplyReply, CreateUserResult, UpdatePermissionsResult};
use crate::stm::stream::{Streams, TruncatePartitionRequest};
use crate::stm::user::Users;
use crate::stm::{ApplyContext, StateMachine};
use consensus::ClientTable;
use iggy_binary_protocol::requests::consumer_groups::{
    CreateConsumerGroupRequest, DeleteConsumerGroupRequest,
};
use iggy_binary_protocol::requests::partitions::{
    CreatePartitionsWithAssignmentsRequest, DeletePartitionsRequest,
};
use iggy_binary_protocol::requests::streams::{DeleteStreamRequest, UpdateStreamRequest};
use iggy_binary_protocol::requests::topics::{
    CreateTopicWithAssignmentsRequest, DeleteTopicRequest, UpdateTopicRequest,
};
use iggy_binary_protocol::requests::users::{
    ChangePasswordRequest, CreateUserRequest, UpdatePermissionsRequest,
};
use iggy_binary_protocol::{Operation, PrepareHeader, WireDecode, WireIdentifier, WireName};
use iggy_common::{IggyError, IggyTimestamp, variadic};
use server_common::Message;

/// Gate a committed prepare, then apply it. A denial commits as an
/// `Unauthorized` no-op (the gate never mutates state); an allow proceeds to
/// the normal state-machine dispatch. This is the single funnel every
/// production apply path uses, so no committed op can bypass the RBAC check.
///
/// The same funnel drops the grants on a stream or topic that the op deleted.
/// The mux hands each op to one state machine, so `Users` never sees the
/// delete, and a grant left behind would reach the next stream or topic that
/// reuses the id.
///
/// The primary (`on_ack`) and backup (`commit_journal`) paths hold the concrete
/// mux and call this directly; WAL replay (`recover`) is generic over the state
/// machine and reaches it through [`GatedApply`].
///
/// # Errors
/// Propagates the underlying [`StateMachine::update`] error (decode / corruption
/// of a committed body), which is fatal on the commit path.
pub(crate) fn gated_apply<M>(
    mux: &M,
    prepare: Message<PrepareHeader>,
) -> Result<ApplyReply, IggyError>
where
    M: StreamsFrontend
        + StateMachine<Input = Message<PrepareHeader>, Output = ApplyReply, Error = IggyError>,
{
    if let Some(denied) = authorize(&prepare, mux.users(), mux.streams()) {
        return Ok(denied);
    }
    if let Some(rejected) = reject_dangling_grant(&prepare, mux.users(), mux.streams()) {
        return Ok(rejected);
    }
    let header = prepare.header();
    let timestamp = IggyTimestamp::from(header.timestamp);
    let context = ApplyContext::from(header);
    let reply = mux.update(prepare)?;
    if let Some(deleted) = reply.deleted_resource {
        // Publish the pruned grants before a later op can reuse the resource ID.
        // Peer shards may see the deletion first, but cannot see its replacement yet.
        mux.users().remove_grants(deleted, timestamp, context);
    }
    Ok(reply)
}

/// An admitted request can commit after its user was deleted or disabled.
/// Check the replicated session table before resolving the reused user ID.
/// Partition completions use a synthetic session without a client-table entry.
pub(crate) fn reject_stale_session(
    header: &PrepareHeader,
    client_table: &ClientTable,
) -> Option<ApplyReply> {
    if matches!(
        header.operation,
        Operation::CompleteConsumerGroupRevocation | Operation::CompleteLifecycle
    ) {
        return None;
    }
    (header.session != 0 && client_table.get_epoch(header.client) != Some(header.session))
        .then(|| ApplyReply::err(IggyError::StaleClient.as_code()))
}

/// Generic entry point to `gated_apply` for the WAL-replay path.
///
/// The replay path is generic over the state machine and cannot name the
/// concrete accessors, so it dispatches through this trait. Implemented only
/// for the metadata mux (and, in tests, the empty state machine used by the
/// recovery harness).
pub trait GatedApply:
    StateMachine<Input = Message<PrepareHeader>, Output = ApplyReply, Error = IggyError>
{
    /// See `gated_apply`.
    ///
    /// # Errors
    /// Propagates the underlying [`StateMachine::update`] error.
    fn gated_update(&self, prepare: Message<PrepareHeader>) -> Result<ApplyReply, IggyError>;

    /// Partition-set revision used to validate retirement coverage during WAL replay.
    fn namespace_revision(&self) -> u64;

    fn is_active_account(&self, user_id: u32, created_at: u64) -> bool;
}

impl GatedApply for MuxStateMachine<variadic!(Users, Streams)> {
    fn is_active_account(&self, user_id: u32, created_at: u64) -> bool {
        self.users().is_active_account(user_id, created_at)
    }

    fn gated_update(&self, prepare: Message<PrepareHeader>) -> Result<ApplyReply, IggyError> {
        gated_apply(self, prepare)
    }

    fn namespace_revision(&self) -> u64 {
        self.streams().read(|inner| inner.namespace_revision)
    }
}

/// The root user is always the first user created (slab id 0) and holds every
/// grant. Short-circuited below rather than checked against the permissioner:
/// server-originated ops carry `user_id` 0 with no session. Initial root
/// creation is one such operation and precedes permissioner population.
const ROOT_USER_ID: u32 = 0;

/// Decides whether the acting user may run this committed op.
///
/// `Some(reply)` denies with a committed `Unauthorized` no-op; `None` allows
/// the op to reach its `StateHandler::apply`. Only control-plane client ops are
/// checked; internal / server-originated ops (which carry `user_id` 0), the
/// self-scoped personal-access-token ops (no legacy RBAC), the pre-projection
/// wire forms, and partition-plane ops all pass through.
///
/// Object ids are resolved against committed stream state first; a resolution
/// miss passes through so `apply` produces its own `NotFound`, preserving the
/// legacy notfound-before-permission ordering.
#[allow(clippy::too_many_lines)]
pub(crate) fn authorize(
    prepare: &Message<PrepareHeader>,
    users: &Users,
    streams: &Streams,
) -> Option<ApplyReply> {
    let header = prepare.header();
    let user_id = header.user_id;

    if user_id == ROOT_USER_ID {
        return None;
    }
    let body = prepare.body();

    match header.operation {
        // Streams. `create_stream` is unscoped; the rest resolve the stream id.
        Operation::CreateStream => check(users, |perm| perm.create_stream(user_id)),
        Operation::UpdateStream => {
            let Ok(request) = UpdateStreamRequest::decode_from(body) else {
                return None;
            };
            stream_scoped(users, streams, &request.stream_id, |perm, sid| {
                perm.update_stream(user_id, sid)
            })
        }
        Operation::DeleteStream => {
            let Ok(request) = DeleteStreamRequest::decode_from(body) else {
                return None;
            };
            stream_scoped(users, streams, &request.stream_id, |perm, sid| {
                perm.delete_stream(user_id, sid)
            })
        }

        // Topics. The wire `CreateTopic` is projected to
        // `CreateTopicWithAssignments` before it reaches apply, so only the
        // enriched form is gated. `create_topic` is stream-scoped.
        Operation::CreateTopicWithAssignments => {
            let Ok(request) = CreateTopicWithAssignmentsRequest::decode_from(body) else {
                return None;
            };
            stream_scoped(users, streams, &request.request.stream_id, |perm, sid| {
                perm.create_topic(user_id, sid)
            })
        }
        Operation::UpdateTopic => {
            let Ok(request) = UpdateTopicRequest::decode_from(body) else {
                return None;
            };
            topic_scoped(
                users,
                streams,
                &request.stream_id,
                &request.topic_id,
                |perm, sid, tid| perm.update_topic(user_id, sid, tid),
            )
        }
        Operation::DeleteTopic => {
            let Ok(request) = DeleteTopicRequest::decode_from(body) else {
                return None;
            };
            topic_scoped(
                users,
                streams,
                &request.stream_id,
                &request.topic_id,
                |perm, sid, tid| perm.delete_topic(user_id, sid, tid),
            )
        }

        // Partitions. Wire `CreatePartitions` is projected to the enriched
        // form; `DeleteSegments` is resolved server-side to `TruncatePartition`
        // carrying the original client's id.
        Operation::CreatePartitionsWithAssignments => {
            let Ok(request) = CreatePartitionsWithAssignmentsRequest::decode_from(body) else {
                return None;
            };
            topic_scoped(
                users,
                streams,
                &request.request.stream_id,
                &request.request.topic_id,
                |perm, sid, tid| perm.create_partitions(user_id, sid, tid),
            )
        }
        Operation::DeletePartitions => {
            let Ok(request) = DeletePartitionsRequest::decode_from(body) else {
                return None;
            };
            topic_scoped(
                users,
                streams,
                &request.stream_id,
                &request.topic_id,
                |perm, sid, tid| perm.delete_partitions(user_id, sid, tid),
            )
        }
        Operation::TruncatePartition => {
            let Ok(request) = TruncatePartitionRequest::decode_from(body) else {
                return None;
            };
            topic_scoped(
                users,
                streams,
                &request.stream_id,
                &request.topic_id,
                |perm, sid, tid| perm.delete_segments(user_id, sid, tid),
            )
        }

        // Consumer groups. Join/Leave carry the enriched request types that add
        // the VSR client id; Create/Delete use the wire types.
        Operation::CreateConsumerGroup => {
            let Ok(request) = CreateConsumerGroupRequest::decode_from(body) else {
                return None;
            };
            topic_scoped(
                users,
                streams,
                &request.stream_id,
                &request.topic_id,
                |perm, sid, tid| perm.create_consumer_group(user_id, sid, tid),
            )
        }
        Operation::DeleteConsumerGroup => {
            let Ok(request) = DeleteConsumerGroupRequest::decode_from(body) else {
                return None;
            };
            topic_scoped(
                users,
                streams,
                &request.stream_id,
                &request.topic_id,
                |perm, sid, tid| perm.delete_consumer_group(user_id, sid, tid),
            )
        }
        Operation::JoinConsumerGroup => {
            let Ok(request) = JoinConsumerGroupRequest::decode_from(body) else {
                return None;
            };
            topic_scoped(
                users,
                streams,
                &request.stream_id,
                &request.topic_id,
                |perm, sid, tid| perm.join_consumer_group(user_id, sid, tid),
            )
        }
        Operation::LeaveConsumerGroup => {
            let Ok(request) = LeaveConsumerGroupRequest::decode_from(body) else {
                return None;
            };
            topic_scoped(
                users,
                streams,
                &request.stream_id,
                &request.topic_id,
                |perm, sid, tid| perm.leave_consumer_group(user_id, sid, tid),
            )
        }

        // Users. Unscoped `manage_users`, except `ChangePassword` which the
        // legacy execution layer exempts when the target is the acting user.
        Operation::CreateUser => check(users, |perm| perm.create_user(user_id)),
        Operation::DeleteUser => check(users, |perm| perm.delete_user(user_id)),
        Operation::UpdateUser => check(users, |perm| perm.update_user(user_id)),
        Operation::UpdatePermissions => check(users, |perm| perm.update_permissions(user_id)),
        Operation::ChangePassword => {
            let Ok(request) = ChangePasswordRequest::decode_from(body) else {
                return None;
            };
            match users.read(|inner| inner.resolve_user_id(&request.user_id)) {
                // Self-service password change: allowed without `manage_users`.
                Some(target_id) if target_id == user_id as usize => None,
                Some(_) => check(users, |perm| perm.change_password(user_id)),
                // Unknown target passes through to apply's `UserNotFound`.
                None => None,
            }
        }

        // Not gated (listed exhaustively so a new op cannot silently skip the
        // gate): VSR-reserved ops (never reach apply as client ops),
        // server-originated internal ops (user_id 0), self-scoped PAT ops (no
        // legacy RBAC), the pre-projection wire forms, and every partition op.
        Operation::Reserved
        | Operation::Register
        | Operation::NonReplicated
        | Operation::Logout
        | Operation::RemoveConsumerGroupMember
        | Operation::CompleteConsumerGroupRevocation
        | Operation::FinalizeSession
        | Operation::RetireSession
        | Operation::CreateTopic
        | Operation::CreatePartitions
        | Operation::DeleteSegments
        | Operation::CreatePersonalAccessToken
        | Operation::DeletePersonalAccessToken
        | Operation::SendMessages
        | Operation::StoreConsumerOffset
        | Operation::DeleteConsumerOffset
        | Operation::CompleteLifecycle
        | Operation::TransitionPartitionHistory
        | Operation::InstallConsumerGroupOwner
        | Operation::RetireConsumerGroupOwners => None,
    }
}

/// Refuses a `CreateUser` or `UpdatePermissions` that grants on a stream or
/// topic that does not exist.
///
/// Grants name streams and topics by slab id, and a create takes a free id, so
/// a grant on a missing id would reach whatever is created there next. The
/// check runs here because the mux hands the op to `Users`, which cannot see
/// the streams, and at apply time, so it sees a delete committed just before.
/// An op that apply refuses for another reason passes through, so the client
/// gets that code, as with `admits_partitions_create`.
fn reject_dangling_grant(
    prepare: &Message<PrepareHeader>,
    users: &Users,
    streams: &Streams,
) -> Option<ApplyReply> {
    let body = prepare.body();
    let (permissions, stream_not_found, topic_not_found) = match prepare.header().operation {
        Operation::CreateUser => {
            let request = CreateUserRequest::decode_from(body).ok()?;
            users
                .read(|inner| inner.validate_create_user(&request))
                .ok()?;
            (
                request.permissions?,
                u32::from(CreateUserResult::StreamNotFound),
                u32::from(CreateUserResult::TopicNotFound),
            )
        }
        Operation::UpdatePermissions => {
            let request = UpdatePermissionsRequest::decode_from(body).ok()?;
            users
                .read(|inner| inner.permissions_target(&request.user_id))
                .ok()?;
            (
                request.permissions?,
                u32::from(UpdatePermissionsResult::StreamNotFound),
                u32::from(UpdatePermissionsResult::TopicNotFound),
            )
        }
        _ => return None,
    };
    streams.read(|inner| {
        permissions.streams.iter().find_map(|grant| {
            let Some(stream) = inner.items.get(grant.stream_id as usize) else {
                return Some(ApplyReply::err(stream_not_found));
            };
            grant
                .topics
                .iter()
                .any(|topic| !stream.topics.contains(topic.topic_id as usize))
                .then(|| ApplyReply::err(topic_not_found))
        })
    })
}

/// What a create that `[metadata] partitions_max` counts adds partitions to.
pub(crate) enum PartitionsCreate {
    /// A `CreateTopic` of a topic with this name.
    Topic { name: WireName },
    /// A `CreatePartitions` for this existing topic.
    Partitions { topic_id: WireIdentifier },
}

/// Whether a `CreateTopic` or a `CreatePartitions` gets past `authorize` and
/// the first checks of its apply: the ids resolve, a new topic name is free in
/// its stream, and `user_id` holds the grant. The primary asks before it denies
/// a create for `[metadata] partitions_max`, so apply answers `NotFound`,
/// `Unauthorized` or `TopicNameAlreadyExists` first. A user who cannot create
/// learns nothing about the cap, and a client that creates a topic only when it
/// is missing gets the name error it expects.
pub(crate) fn admits_partitions_create(
    users: &Users,
    streams: &Streams,
    user_id: u32,
    stream_id: &WireIdentifier,
    create: &PartitionsCreate,
) -> bool {
    match create {
        PartitionsCreate::Topic { name } => streams
            .read(|inner| {
                let stream_id = inner.resolve_stream_id(stream_id)?;
                let name_taken = inner
                    .items
                    .get(stream_id)?
                    .topic_index
                    .contains_key(name.as_str());
                (!name_taken).then_some(stream_id)
            })
            .is_some_and(|stream_id| {
                user_id == ROOT_USER_ID
                    || users
                        .authorize(|perm| perm.create_topic(user_id, stream_id))
                        .is_ok()
            }),
        PartitionsCreate::Partitions { topic_id } => streams
            .resolve_topic_ids(stream_id, topic_id)
            .is_some_and(|(stream_id, topic_id)| {
                user_id == ROOT_USER_ID
                    || users
                        .authorize(|perm| perm.create_partitions(user_id, stream_id, topic_id))
                        .is_ok()
            }),
    }
}

/// Maps a permissioner rule outcome to a gate decision: any `Err` (always
/// `Unauthorized`) denies with a committed `Unauthorized` reply.
fn check(
    users: &Users,
    rule: impl FnOnce(&Permissioner) -> Result<(), IggyError>,
) -> Option<ApplyReply> {
    match users.read(|inner| rule(&inner.permissioner)) {
        Ok(()) => None,
        Err(error) => Some(ApplyReply::err(error.as_code())),
    }
}

/// Resolves a stream id, then runs a stream-scoped rule. A resolution miss
/// passes through (returns `None`) so apply surfaces its own `NotFound`.
fn stream_scoped(
    users: &Users,
    streams: &Streams,
    stream_id: &WireIdentifier,
    rule: impl FnOnce(&Permissioner, usize) -> Result<(), IggyError>,
) -> Option<ApplyReply> {
    let stream_id = streams.resolve_stream_id(stream_id)?;
    check(users, |perm| rule(perm, stream_id))
}

/// Resolves a (stream, topic) pair, then runs a topic-scoped rule. A miss on
/// either passes through so apply surfaces its own `NotFound`.
fn topic_scoped(
    users: &Users,
    streams: &Streams,
    stream_id: &WireIdentifier,
    topic_id: &WireIdentifier,
    rule: impl FnOnce(&Permissioner, usize, usize) -> Result<(), IggyError>,
) -> Option<ApplyReply> {
    let (stream_id, topic_id) = streams.resolve_topic_ids(stream_id, topic_id)?;
    check(users, |perm| rule(perm, stream_id, topic_id))
}

/// The recovery unit tests exercise WAL-replay mechanics with an empty state
/// list (`MuxStateMachine<()>`), which has no permission state and so needs no
/// gate. Production always uses the `variadic!(Users, Streams)` machine above.
#[cfg(test)]
impl GatedApply for MuxStateMachine<()> {
    fn is_active_account(&self, _user_id: u32, _created_at: u64) -> bool {
        true
    }

    fn gated_update(&self, prepare: Message<PrepareHeader>) -> Result<ApplyReply, IggyError> {
        self.update(prepare)
    }

    fn namespace_revision(&self) -> u64 {
        0
    }
}

#[cfg(test)]
#[allow(clippy::cast_possible_truncation)]
mod tests {
    use super::*;
    use crate::stm::lifecycle::{CompleteLifecycleRequest, LifecycleCompletion};
    use iggy_binary_protocol::primitives::partition_assignment::CreatedPartitionAssignment;
    use iggy_binary_protocol::primitives::permissions::{
        WireGlobalPermissions, WirePermissions, WireStreamPermissions, WireTopicPermissions,
    };
    use iggy_binary_protocol::requests::streams::CreateStreamRequest;
    use iggy_binary_protocol::requests::topics::CreateTopicRequest;
    use iggy_binary_protocol::requests::users::CreateUserRequest;
    use iggy_binary_protocol::{Command, WireEncode, WireName, WireOptions};
    use iggy_common::{StreamPermissions, UserStatus};
    use server_common::iobuf::Owned;
    use std::collections::BTreeMap;

    const HEADER_SIZE: usize = size_of::<PrepareHeader>();
    /// Root is the first user, so it takes slab id 0; the first user created
    /// after it is id 1.
    const ROOT: u32 = 0;
    const ALICE: u32 = 1;

    fn mux() -> MuxStateMachine<variadic!(Users, Streams)> {
        let users = Users::default();
        users.ensure_root_user("iggy", "hash");
        let streams = Streams::default();
        MuxStateMachine::new(variadic!(users, streams))
    }

    fn make_prepare(
        op: u64,
        operation: Operation,
        user_id: u32,
        body: &[u8],
    ) -> Message<PrepareHeader> {
        let total = HEADER_SIZE + body.len();
        let mut buffer = Owned::<4096>::zeroed(total);
        {
            let header = bytemuck::checked::from_bytes_mut::<PrepareHeader>(
                &mut buffer.as_mut_slice()[..HEADER_SIZE],
            );
            header.command = Command::Prepare;
            header.operation = operation;
            header.op = op;
            header.user_id = user_id;
            header.size = u32::try_from(total).unwrap();
        }
        buffer.as_mut_slice()[HEADER_SIZE..].copy_from_slice(body);
        Message::try_from(buffer).unwrap()
    }

    fn no_grants() -> WireGlobalPermissions {
        WireGlobalPermissions {
            manage_servers: false,
            read_servers: false,
            manage_users: false,
            read_users: false,
            manage_streams: false,
            read_streams: false,
            manage_topics: false,
            read_topics: false,
            poll_messages: false,
            send_messages: false,
        }
    }

    fn create_user_body(
        name: &str,
        global: WireGlobalPermissions,
        streams: Vec<WireStreamPermissions>,
    ) -> bytes::Bytes {
        CreateUserRequest {
            username: WireName::new(name).unwrap(),
            password: "hash".to_string(),
            status: UserStatus::Active.as_code(),
            permissions: Some(WirePermissions { global, streams }),
            options: WireOptions::empty(),
        }
        .to_bytes()
    }

    fn create_stream_body(name: &str) -> bytes::Bytes {
        CreateStreamRequest {
            name: WireName::new(name).unwrap(),
            options: WireOptions::empty(),
        }
        .to_bytes()
    }

    fn create_topic_body(stream_id: u32, name: &str) -> bytes::Bytes {
        CreateTopicWithAssignmentsRequest {
            request: CreateTopicRequest {
                stream_id: WireIdentifier::numeric(stream_id),
                partitions_count: 1,
                name: WireName::new(name).unwrap(),
                options: WireOptions::empty(),
            },
            derived_options: WireOptions::empty(),
            partitions: vec![CreatedPartitionAssignment {
                initial_view: 0,
                partition_id: 0,
                consensus_group_id: 1,
            }],
        }
        .to_bytes()
    }

    fn change_password_body(target: &str) -> bytes::Bytes {
        ChangePasswordRequest {
            user_id: WireIdentifier::named(target).unwrap(),
            current_password: String::new(),
            new_password: "new-hash".to_string(),
        }
        .to_bytes()
    }

    fn stream_manage_topics(stream_id: u32) -> WireStreamPermissions {
        WireStreamPermissions {
            stream_id,
            manage_stream: false,
            read_stream: false,
            manage_topics: true,
            read_topics: false,
            poll_messages: false,
            send_messages: false,
            topics: Vec::new(),
        }
    }

    fn stream_read(stream_id: u32) -> WireStreamPermissions {
        WireStreamPermissions {
            stream_id,
            manage_stream: false,
            read_stream: true,
            manage_topics: false,
            read_topics: false,
            poll_messages: false,
            send_messages: false,
            topics: Vec::new(),
        }
    }

    /// No stream-wide bit, so access to each topic comes from its entry alone.
    fn topic_read(stream_id: u32, topic_ids: &[u32]) -> WireStreamPermissions {
        WireStreamPermissions {
            read_stream: false,
            topics: topic_ids
                .iter()
                .map(|&topic_id| WireTopicPermissions {
                    topic_id,
                    manage_topic: false,
                    read_topic: true,
                    poll_messages: false,
                    send_messages: false,
                })
                .collect(),
            ..stream_read(stream_id)
        }
    }

    fn update_permissions_body(target: &str, streams: Vec<WireStreamPermissions>) -> bytes::Bytes {
        UpdatePermissionsRequest {
            user_id: WireIdentifier::named(target).unwrap(),
            permissions: Some(WirePermissions {
                global: no_grants(),
                streams,
            }),
        }
        .to_bytes()
    }

    fn delete_stream_body(stream_id: u32) -> bytes::Bytes {
        DeleteStreamRequest {
            stream_id: WireIdentifier::numeric(stream_id),
        }
        .to_bytes()
    }

    fn delete_topic_body(stream_id: u32, topic_id: u32) -> bytes::Bytes {
        DeleteTopicRequest {
            stream_id: WireIdentifier::numeric(stream_id),
            topic_id: WireIdentifier::numeric(topic_id),
        }
        .to_bytes()
    }

    fn commit_as_root(
        mux: &MuxStateMachine<variadic!(Users, Streams)>,
        op: u64,
        operation: Operation,
        body: &[u8],
    ) {
        let reply = mux
            .gated_update(make_prepare(op, operation, ROOT, body))
            .unwrap();
        assert_eq!(reply.code, 0, "root {operation:?} must commit");
    }

    fn stream_grants_of(
        mux: &MuxStateMachine<variadic!(Users, Streams)>,
        user_id: u32,
    ) -> Option<BTreeMap<usize, StreamPermissions>> {
        mux.users().read(|users| {
            users
                .items
                .get(user_id as usize)?
                .permissions
                .as_deref()?
                .streams
                .clone()
        })
    }

    const UNAUTHORIZED: u32 = 41;

    #[test]
    fn given_ungranted_user_when_create_stream_should_deny_and_leave_state_unchanged() {
        let mux = mux();
        assert_eq!(
            mux.gated_update(make_prepare(
                1,
                Operation::CreateUser,
                ROOT,
                &create_user_body("alice", no_grants(), Vec::new()),
            ))
            .unwrap()
            .code,
            0,
            "root may create a user"
        );

        let reply = mux
            .gated_update(make_prepare(
                2,
                Operation::CreateStream,
                ALICE,
                &create_stream_body("s1"),
            ))
            .unwrap();

        assert_eq!(reply.code, UNAUTHORIZED);
        assert_eq!(
            mux.streams().read(|inner| inner.items.len()),
            0,
            "denied op must not mutate stream state"
        );
    }

    #[test]
    fn given_manage_streams_grant_when_create_stream_should_allow() {
        let mux = mux();
        let grants = WireGlobalPermissions {
            manage_streams: true,
            ..no_grants()
        };
        mux.gated_update(make_prepare(
            1,
            Operation::CreateUser,
            ROOT,
            &create_user_body("alice", grants, Vec::new()),
        ))
        .unwrap();

        let reply = mux
            .gated_update(make_prepare(
                2,
                Operation::CreateStream,
                ALICE,
                &create_stream_body("s1"),
            ))
            .unwrap();

        assert_eq!(reply.code, 0);
        assert_eq!(mux.streams().read(|inner| inner.items.len()), 1);
    }

    #[test]
    fn given_stream_scoped_topics_grant_when_create_topic_should_allow_only_that_stream() {
        let mux = mux();
        // Streams 0 and 1 (StreamsInner slab is separate from the user slab).
        mux.gated_update(make_prepare(
            1,
            Operation::CreateStream,
            ROOT,
            &create_stream_body("s0"),
        ))
        .unwrap();
        mux.gated_update(make_prepare(
            2,
            Operation::CreateStream,
            ROOT,
            &create_stream_body("s1"),
        ))
        .unwrap();
        // Alice may manage topics on stream 0 only.
        mux.gated_update(make_prepare(
            3,
            Operation::CreateUser,
            ROOT,
            &create_user_body("alice", no_grants(), vec![stream_manage_topics(0)]),
        ))
        .unwrap();

        let allowed = mux
            .gated_update(make_prepare(
                4,
                Operation::CreateTopicWithAssignments,
                ALICE,
                &create_topic_body(0, "t"),
            ))
            .unwrap();
        assert_eq!(allowed.code, 0, "grant covers stream 0");

        let denied = mux
            .gated_update(make_prepare(
                5,
                Operation::CreateTopicWithAssignments,
                ALICE,
                &create_topic_body(1, "t"),
            ))
            .unwrap();
        assert_eq!(denied.code, UNAUTHORIZED, "grant does not cover stream 1");
    }

    #[test]
    fn given_change_password_when_target_is_self_should_allow_else_deny() {
        let mux = mux();
        mux.gated_update(make_prepare(
            1,
            Operation::CreateUser,
            ROOT,
            &create_user_body("alice", no_grants(), Vec::new()),
        ))
        .unwrap();
        mux.gated_update(make_prepare(
            2,
            Operation::CreateUser,
            ROOT,
            &create_user_body("bob", no_grants(), Vec::new()),
        ))
        .unwrap();

        let own = mux
            .gated_update(make_prepare(
                3,
                Operation::ChangePassword,
                ALICE,
                &change_password_body("alice"),
            ))
            .unwrap();
        assert_eq!(
            own.code, 0,
            "self password change is exempt from manage_users"
        );

        let other = mux
            .gated_update(make_prepare(
                4,
                Operation::ChangePassword,
                ALICE,
                &change_password_body("bob"),
            ))
            .unwrap();
        assert_eq!(
            other.code, UNAUTHORIZED,
            "changing another user needs manage_users"
        );
    }

    #[test]
    fn given_internal_revocation_op_when_authorized_should_skip_the_gate() {
        let mux = mux();
        let prepare = make_prepare(1, Operation::CompleteConsumerGroupRevocation, ROOT, &[]);
        assert!(
            authorize(&prepare, mux.users(), mux.streams()).is_none(),
            "server-originated revocation must not be gated"
        );
    }

    #[test]
    fn given_reserved_client_id_when_session_is_stale_should_reject_client_operations() {
        let table = ClientTable::new(consensus::CLIENTS_TABLE_MAX);
        for client in [
            message_bus::server_internal_client_id(0),
            message_bus::AUTO_COMMIT_CLIENT_ID,
        ] {
            for operation in [
                Operation::CreatePersonalAccessToken,
                Operation::TruncatePartition,
            ] {
                let header = PrepareHeader {
                    client,
                    session: 1,
                    operation,
                    ..Default::default()
                };
                assert_eq!(
                    reject_stale_session(&header, &table)
                        .expect("a reserved ID must not bypass session fencing")
                        .code,
                    IggyError::StaleClient.as_code(),
                    "{operation:?} must check the caller's session"
                );
            }
        }
    }

    #[test]
    fn given_non_root_user_when_create_pat_should_skip_the_gate() {
        let mux = mux();
        mux.gated_update(make_prepare(
            1,
            Operation::CreateUser,
            ROOT,
            &create_user_body("alice", no_grants(), Vec::new()),
        ))
        .unwrap();

        let prepare = make_prepare(2, Operation::CreatePersonalAccessToken, ALICE, &[]);
        assert!(
            authorize(&prepare, mux.users(), mux.streams()).is_none(),
            "PAT ops are self-scoped (parity: no legacy RBAC)"
        );
    }

    // The linchpin: a sequence containing a denied op must produce byte-identical
    // codes and final state whether applied once (primary on_ack) or replayed
    // fresh (backup commit_journal / recover). Identity rides the header, so no
    // session table is consulted.
    #[test]
    fn given_sequence_with_denial_when_replayed_fresh_should_be_state_identical() {
        fn sequence() -> Vec<Message<PrepareHeader>> {
            vec![
                make_prepare(
                    1,
                    Operation::CreateUser,
                    ROOT,
                    &create_user_body("alice", no_grants(), Vec::new()),
                ),
                make_prepare(
                    2,
                    Operation::CreateStream,
                    ROOT,
                    &create_stream_body("s-root"),
                ),
                // Alice lacks manage_streams: denied on both passes.
                make_prepare(
                    3,
                    Operation::CreateStream,
                    ALICE,
                    &create_stream_body("s-alice"),
                ),
            ]
        }

        let primary = mux();
        let primary_codes: Vec<u32> = sequence()
            .into_iter()
            .map(|prepare| primary.gated_update(prepare).unwrap().code)
            .collect();

        let replay = mux();
        let replay_codes: Vec<u32> = sequence()
            .into_iter()
            .map(|prepare| replay.gated_update(prepare).unwrap().code)
            .collect();

        assert_eq!(primary_codes, replay_codes, "replay must be deterministic");
        assert_eq!(primary_codes[2], UNAUTHORIZED, "alice's create is denied");
        assert_eq!(primary.streams().read(|inner| inner.items.len()), 1);
        assert_eq!(replay.streams().read(|inner| inner.items.len()), 1);
    }

    #[test]
    fn given_stream_grant_when_stream_deleted_should_not_grant_the_stream_that_reuses_its_id() {
        let mux = mux();
        commit_as_root(&mux, 1, Operation::CreateStream, &create_stream_body("old"));
        commit_as_root(
            &mux,
            2,
            Operation::CreateStream,
            &create_stream_body("kept"),
        );
        commit_as_root(
            &mux,
            3,
            Operation::CreateUser,
            &create_user_body("alice", no_grants(), vec![stream_read(0), stream_read(1)]),
        );
        assert!(
            mux.users()
                .authorize(|perm| perm.get_stream(ALICE, 0))
                .is_ok()
        );

        commit_as_root(&mux, 4, Operation::DeleteStream, &delete_stream_body(0));
        commit_as_root(&mux, 5, Operation::CreateStream, &create_stream_body("new"));
        assert_eq!(
            mux.streams()
                .resolve_stream_id(&WireIdentifier::named("new").unwrap()),
            Some(0),
            "the new stream must reuse the freed id"
        );

        assert!(
            mux.users()
                .authorize(|perm| perm.get_stream(ALICE, 0))
                .is_err(),
            "a grant on a deleted stream must not reach the stream that reuses its id"
        );
        assert!(
            mux.users()
                .authorize(|perm| perm.get_stream(ALICE, 1))
                .is_ok(),
            "the grant on the live stream stays"
        );
        assert_eq!(
            stream_grants_of(&mux, ALICE).map(|streams| streams.into_keys().collect::<Vec<_>>()),
            Some(vec![1]),
            "the user record must drop the grant too, or a snapshot restore rebuilds it"
        );
    }

    #[test]
    fn given_topic_grant_when_topic_deleted_should_not_grant_the_topic_that_reuses_its_id() {
        let mux = mux();
        commit_as_root(&mux, 1, Operation::CreateStream, &create_stream_body("s"));
        commit_as_root(
            &mux,
            2,
            Operation::CreateTopicWithAssignments,
            &create_topic_body(0, "old"),
        );
        commit_as_root(
            &mux,
            3,
            Operation::CreateTopicWithAssignments,
            &create_topic_body(0, "kept"),
        );
        commit_as_root(
            &mux,
            4,
            Operation::CreateUser,
            &create_user_body("alice", no_grants(), vec![topic_read(0, &[0, 1])]),
        );
        assert!(
            mux.users()
                .authorize(|perm| perm.get_topic(ALICE, 0, 0))
                .is_ok()
        );

        commit_as_root(&mux, 5, Operation::DeleteTopic, &delete_topic_body(0, 0));
        // A topic with partitions leaves, and frees its id, only when the
        // lifecycle of its delete completes.
        let completion = CompleteLifecycleRequest::new(
            5,
            0,
            vec![LifecycleCompletion {
                topic_id: 0,
                partition_id: 0,
                partition_op: 1,
            }],
        )
        .unwrap();
        commit_as_root(
            &mux,
            6,
            Operation::CompleteLifecycle,
            &completion.to_bytes(),
        );
        commit_as_root(
            &mux,
            7,
            Operation::CreateTopicWithAssignments,
            &create_topic_body(0, "new"),
        );
        assert_eq!(
            mux.streams().resolve_topic_ids(
                &WireIdentifier::numeric(0),
                &WireIdentifier::named("new").unwrap()
            ),
            Some((0, 0)),
            "the new topic must reuse the freed id"
        );

        assert!(
            mux.users()
                .authorize(|perm| perm.get_topic(ALICE, 0, 0))
                .is_err(),
            "a grant on a deleted topic must not reach the topic that reuses its id"
        );
        assert!(
            mux.users()
                .authorize(|perm| perm.get_topic(ALICE, 0, 1))
                .is_ok(),
            "the grant on the live topic stays"
        );
        let topic_ids = stream_grants_of(&mux, ALICE)
            .and_then(|mut streams| streams.remove(&0))
            .and_then(|stream| stream.topics)
            .map(|topics| topics.into_keys().collect::<Vec<_>>());
        assert_eq!(
            topic_ids,
            Some(vec![1]),
            "the user record must drop the topic grant too, or a snapshot restore rebuilds it"
        );
    }

    #[test]
    fn given_grant_on_missing_stream_when_creating_user_should_reject_with_stream_not_found() {
        let mux = mux();
        let reply = mux
            .gated_update(make_prepare(
                1,
                Operation::CreateUser,
                ROOT,
                &create_user_body("alice", no_grants(), vec![stream_read(0)]),
            ))
            .unwrap();

        assert_eq!(reply.code, u32::from(CreateUserResult::StreamNotFound));
        assert!(
            mux.users()
                .read(|users| users.user_by_name("alice").is_none()),
            "a rejected create must not add the user"
        );
    }

    #[test]
    fn given_grant_on_missing_topic_when_updating_permissions_should_reject_with_topic_not_found() {
        let mux = mux();
        commit_as_root(&mux, 1, Operation::CreateStream, &create_stream_body("s"));
        commit_as_root(
            &mux,
            2,
            Operation::CreateUser,
            &create_user_body("alice", no_grants(), vec![stream_read(0)]),
        );

        let reply = mux
            .gated_update(make_prepare(
                3,
                Operation::UpdatePermissions,
                ROOT,
                &update_permissions_body("alice", vec![topic_read(0, &[0])]),
            ))
            .unwrap();

        assert_eq!(
            reply.code,
            u32::from(UpdatePermissionsResult::TopicNotFound)
        );
        assert_eq!(
            stream_grants_of(&mux, ALICE).map(|streams| streams.into_keys().collect::<Vec<_>>()),
            Some(vec![0]),
            "a rejected update must keep the previous grants"
        );
        assert!(
            mux.users()
                .authorize(|perm| perm.get_stream(ALICE, 0))
                .is_ok(),
            "the previous stream grant must still apply"
        );
    }

    #[test]
    fn given_taken_name_and_dangling_grant_when_creating_user_should_answer_user_already_exists() {
        let mux = mux();
        commit_as_root(
            &mux,
            1,
            Operation::CreateUser,
            &create_user_body("alice", no_grants(), Vec::new()),
        );

        let reply = mux
            .gated_update(make_prepare(
                2,
                Operation::CreateUser,
                ROOT,
                &create_user_body("alice", no_grants(), vec![stream_read(0)]),
            ))
            .unwrap();

        assert_eq!(
            reply.code,
            u32::from(CreateUserResult::UserAlreadyExists),
            "the refusal of apply must win over the dangling-grant refusal"
        );
    }

    #[test]
    fn given_root_target_and_dangling_grant_when_updating_permissions_should_answer_cannot_change_permissions()
     {
        let mux = mux();
        let reply = mux
            .gated_update(make_prepare(
                1,
                Operation::UpdatePermissions,
                ROOT,
                &update_permissions_body("iggy", vec![stream_read(0)]),
            ))
            .unwrap();

        assert_eq!(
            reply.code,
            u32::from(UpdatePermissionsResult::CannotChangePermissions),
            "the refusal of apply must win over the dangling-grant refusal"
        );
    }
}
