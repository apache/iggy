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
//! Replay-determinism invariant: [`authorize`] is a pure function of the
//! prepare header (`operation` + the acting `user_id`, stamped into the
//! replicated header at submit time) and the committed permission/stream
//! state as of the op immediately before this one. That state is applied in
//! the same order on every replica, so the primary (`on_ack`), each backup
//! (`commit_journal`), and WAL replay (`recover`) all recompute the identical
//! decision. This is why identity must ride the header rather than being
//! resolved from a local session table: a backup has no session for the
//! acting client.

use crate::MuxStateMachine;
use crate::impls::metadata::StreamsFrontend;
use crate::permissioner::Permissioner;
use crate::stm::StateMachine;
use crate::stm::consumer_group::{JoinConsumerGroupRequest, LeaveConsumerGroupRequest};
use crate::stm::result::ApplyReply;
use crate::stm::stream::{Streams, TruncatePartitionRequest};
use crate::stm::user::Users;
use iggy_binary_protocol::requests::consumer_groups::{
    CreateConsumerGroupRequest, DeleteConsumerGroupRequest,
};
use iggy_binary_protocol::requests::partitions::{
    CreatePartitionsWithAssignmentsRequest, DeletePartitionsRequest,
};
use iggy_binary_protocol::requests::streams::{
    DeleteStreamRequest, PurgeStreamRequest, UpdateStreamRequest,
};
use iggy_binary_protocol::requests::topics::{
    CreateTopicWithAssignmentsRequest, DeleteTopicRequest, PurgeTopicRequest, UpdateTopicRequest,
};
use iggy_binary_protocol::requests::users::ChangePasswordRequest;
use iggy_binary_protocol::{Operation, PrepareHeader, WireDecode, WireIdentifier};
use iggy_common::{IggyError, variadic};
use server_common::Message;
use std::mem::size_of;

/// Gate a committed prepare, then apply it. A denial commits as an
/// `Unauthorized` no-op (the gate never mutates state); an allow proceeds to
/// the normal state-machine dispatch. This is the single funnel every
/// production apply path uses, so no committed op can bypass the RBAC check.
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
    mux.update(prepare)
}

/// Generic entry point to [`gated_apply`] for the WAL-replay path.
///
/// The replay path is generic over the state machine and cannot name the
/// concrete accessors, so it dispatches through this trait. Implemented only
/// for the metadata mux (and, in tests, the empty state machine used by the
/// recovery harness).
pub trait GatedApply:
    StateMachine<Input = Message<PrepareHeader>, Output = ApplyReply, Error = IggyError>
{
    /// See [`gated_apply`].
    ///
    /// # Errors
    /// Propagates the underlying [`StateMachine::update`] error.
    fn gated_update(&self, prepare: Message<PrepareHeader>) -> Result<ApplyReply, IggyError>;
}

impl GatedApply for MuxStateMachine<variadic!(Users, Streams)> {
    fn gated_update(&self, prepare: Message<PrepareHeader>) -> Result<ApplyReply, IggyError> {
        gated_apply(self, prepare)
    }
}

/// The root user is always the first user created (slab id 0) and holds every
/// grant. Short-circuited below rather than checked against the permissioner so
/// the gate is correct even in a cluster that provisions no users (the
/// deterministic simulator boots none, so its permissioner is empty).
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
    let body = &prepare.as_slice()[size_of::<PrepareHeader>()..header.size as usize];

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
        Operation::PurgeStream => {
            let Ok(request) = PurgeStreamRequest::decode_from(body) else {
                return None;
            };
            stream_scoped(users, streams, &request.stream_id, |perm, sid| {
                perm.purge_stream(user_id, sid)
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
        Operation::PurgeTopic => {
            let Ok(request) = PurgeTopicRequest::decode_from(body) else {
                return None;
            };
            topic_scoped(
                users,
                streams,
                &request.stream_id,
                &request.topic_id,
                |perm, sid, tid| perm.purge_topic(user_id, sid, tid),
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
        | Operation::CreateTopic
        | Operation::CreatePartitions
        | Operation::DeleteSegments
        | Operation::CreatePersonalAccessToken
        | Operation::DeletePersonalAccessToken
        | Operation::SendMessages
        | Operation::StoreConsumerOffset
        | Operation::DeleteConsumerOffset
        | Operation::StoreConsumerOffset2
        | Operation::DeleteConsumerOffset2 => None,
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
    let stream_id = streams.read(|inner| inner.resolve_stream_id(stream_id))?;
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
    let (stream_id, topic_id) = streams.read(|inner| {
        let stream_id = inner.resolve_stream_id(stream_id)?;
        let topic_id = inner.resolve_topic_id(stream_id, topic_id)?;
        Some((stream_id, topic_id))
    })?;
    check(users, |perm| rule(perm, stream_id, topic_id))
}

/// The recovery unit tests exercise WAL-replay mechanics with an empty state
/// list (`MuxStateMachine<()>`), which has no permission state and so needs no
/// gate. Production always uses the `variadic!(Users, Streams)` machine above.
#[cfg(test)]
impl GatedApply for MuxStateMachine<()> {
    fn gated_update(&self, prepare: Message<PrepareHeader>) -> Result<ApplyReply, IggyError> {
        self.update(prepare)
    }
}

#[cfg(test)]
#[allow(clippy::cast_possible_truncation)]
mod tests {
    use super::*;
    use iggy_binary_protocol::primitives::partition_assignment::CreatedPartitionAssignment;
    use iggy_binary_protocol::primitives::permissions::{
        WireGlobalPermissions, WirePermissions, WireStreamPermissions,
    };
    use iggy_binary_protocol::requests::streams::CreateStreamRequest;
    use iggy_binary_protocol::requests::topics::CreateTopicRequest;
    use iggy_binary_protocol::requests::users::CreateUserRequest;
    use iggy_binary_protocol::{Command2, WireEncode, WireName};
    use iggy_common::UserStatus;
    use server_common::iobuf::Owned;

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
            header.command = Command2::Prepare;
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
        }
        .to_bytes()
    }

    fn create_stream_body(name: &str) -> bytes::Bytes {
        CreateStreamRequest {
            name: WireName::new(name).unwrap(),
        }
        .to_bytes()
    }

    fn create_topic_body(stream_id: u32, name: &str) -> bytes::Bytes {
        CreateTopicWithAssignmentsRequest {
            request: CreateTopicRequest {
                stream_id: WireIdentifier::numeric(stream_id),
                partitions_count: 1,
                compression_algorithm: 0,
                message_expiry: 0,
                max_topic_size: 0,
                replication_factor: 1,
                name: WireName::new(name).unwrap(),
            },
            partitions: vec![CreatedPartitionAssignment {
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
}
