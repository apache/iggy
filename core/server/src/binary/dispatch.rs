/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::binary::handlers;
use crate::shard::IggyShard;
use crate::streaming::session::Session;
use ahash::AHashMap;
use bytes::BytesMut;
use iggy_binary_protocol::RequestFrame;
use iggy_binary_protocol::codec::WireDecode;
use iggy_binary_protocol::codes::*;
use iggy_binary_protocol::primitives::permissions::{
    WireGlobalPermissions, WirePermissions, WireStreamPermissions, WireTopicPermissions,
};
use iggy_binary_protocol::requests::consumer_groups::*;
use iggy_binary_protocol::requests::consumer_offsets::*;
use iggy_binary_protocol::requests::messages::*;
use iggy_binary_protocol::requests::partitions::*;
use iggy_binary_protocol::requests::personal_access_tokens::*;
use iggy_binary_protocol::requests::segments::*;
use iggy_binary_protocol::requests::streams::*;
use iggy_binary_protocol::requests::system::*;
use iggy_binary_protocol::requests::topics::*;
use iggy_binary_protocol::requests::users::*;
use iggy_common::{
    Consumer, ConsumerKind, GlobalPermissions, Identifier, IggyError, Permissions, PollingKind,
    PollingStrategy, SenderKind, StreamPermissions, TopicPermissions,
};
use std::rc::Rc;
use tracing::{error, warn};

/// Result of handling a command. Most commands return `Finished`.
/// `SendMessages` may migrate the TCP connection to another shard.
pub enum HandlerResult {
    Finished,
    Migrated { to_shard: u16 },
}

/// Read the full payload from the sender into a buffer.
pub async fn read_payload(sender: &mut SenderKind, length: u32) -> Result<BytesMut, IggyError> {
    if length > MAX_CONTROL_FRAME_PAYLOAD {
        return Err(IggyError::InvalidCommand);
    }
    let mut buffer = BytesMut::with_capacity(length as usize);
    // SAFETY: when length > 0, sender.read() fills exactly `length` bytes
    // before returning Ok. On error the buffer is dropped without being read.
    // When length == 0, set_len(0) is a no-op (no uninitialized bytes exposed).
    unsafe {
        buffer.set_len(length as usize);
    }
    if length > 0 {
        let (result, buf) = sender.read(buffer).await;
        result?;
        buffer = buf;
    }
    Ok(buffer)
}

fn decode<T: WireDecode>(payload: &[u8]) -> Result<T, IggyError> {
    use iggy_binary_protocol::error::WireError;

    let (val, consumed) = T::decode(payload).map_err(|e| {
        warn!("wire decode error: {e}");
        match e {
            WireError::PayloadTooLarge { .. } => IggyError::InvalidSizeBytes,
            WireError::Validation(_) => IggyError::InvalidFormat,
            _ => IggyError::InvalidCommand,
        }
    })?;
    if consumed != payload.len() {
        warn!(
            "wire decode: {} trailing bytes (consumed {consumed}, payload {})",
            payload.len() - consumed,
            payload.len()
        );
    }
    Ok(val)
}

/// Convert a `WireIdentifier` to the domain `Identifier`.
pub fn wire_id_to_identifier(
    wire: &iggy_binary_protocol::WireIdentifier,
) -> Result<Identifier, IggyError> {
    match wire {
        iggy_binary_protocol::WireIdentifier::Numeric(id) => Identifier::numeric(*id),
        iggy_binary_protocol::WireIdentifier::String(name) => Identifier::named(name.as_str()),
    }
}

/// Convert a `WireConsumer` to the domain `Consumer`.
pub fn wire_consumer_to_consumer(
    wire: &iggy_binary_protocol::WireConsumer,
) -> Result<Consumer, IggyError> {
    let id = wire_id_to_identifier(&wire.id)?;
    let kind = ConsumerKind::from_code(wire.kind)?;
    Ok(Consumer { kind, id })
}

/// Convert a `WirePollingStrategy` to the domain `PollingStrategy`.
pub fn wire_polling_to_strategy(
    wire: &iggy_binary_protocol::WirePollingStrategy,
) -> Result<PollingStrategy, IggyError> {
    Ok(PollingStrategy {
        kind: PollingKind::from_code(wire.kind)?,
        value: wire.value,
    })
}

fn wire_topic_to_domain(wt: &WireTopicPermissions) -> TopicPermissions {
    TopicPermissions {
        manage_topic: wt.manage_topic,
        read_topic: wt.read_topic,
        poll_messages: wt.poll_messages,
        send_messages: wt.send_messages,
    }
}

fn wire_stream_to_domain(ws: &WireStreamPermissions) -> StreamPermissions {
    let topics = if ws.topics.is_empty() {
        None
    } else {
        let mut map = AHashMap::with_capacity(ws.topics.len());
        for wt in &ws.topics {
            map.insert(wt.topic_id as usize, wire_topic_to_domain(wt));
        }
        Some(map)
    };
    StreamPermissions {
        manage_stream: ws.manage_stream,
        read_stream: ws.read_stream,
        manage_topics: ws.manage_topics,
        read_topics: ws.read_topics,
        poll_messages: ws.poll_messages,
        send_messages: ws.send_messages,
        topics,
    }
}

/// Convert `WirePermissions` to the domain `Permissions`.
pub fn wire_permissions_to_permissions(wp: &WirePermissions) -> Permissions {
    let streams = if wp.streams.is_empty() {
        None
    } else {
        let mut map = AHashMap::with_capacity(wp.streams.len());
        for ws in &wp.streams {
            map.insert(ws.stream_id as usize, wire_stream_to_domain(ws));
        }
        Some(map)
    };
    Permissions {
        global: GlobalPermissions {
            manage_servers: wp.global.manage_servers,
            read_servers: wp.global.read_servers,
            manage_users: wp.global.manage_users,
            read_users: wp.global.read_users,
            manage_streams: wp.global.manage_streams,
            read_streams: wp.global.read_streams,
            manage_topics: wp.global.manage_topics,
            read_topics: wp.global.read_topics,
            poll_messages: wp.global.poll_messages,
            send_messages: wp.global.send_messages,
        },
        streams,
    }
}

fn domain_topic_to_wire(topic_id: usize, tp: &TopicPermissions) -> WireTopicPermissions {
    WireTopicPermissions {
        topic_id: topic_id as u32,
        manage_topic: tp.manage_topic,
        read_topic: tp.read_topic,
        poll_messages: tp.poll_messages,
        send_messages: tp.send_messages,
    }
}

fn domain_stream_to_wire(stream_id: usize, sp: &StreamPermissions) -> WireStreamPermissions {
    let topics: Vec<WireTopicPermissions> = sp
        .topics
        .as_ref()
        .map(|map| {
            map.iter()
                .map(|(&tid, tp)| domain_topic_to_wire(tid, tp))
                .collect()
        })
        .unwrap_or_default();
    WireStreamPermissions {
        stream_id: stream_id as u32,
        manage_stream: sp.manage_stream,
        read_stream: sp.read_stream,
        manage_topics: sp.manage_topics,
        read_topics: sp.read_topics,
        poll_messages: sp.poll_messages,
        send_messages: sp.send_messages,
        topics,
    }
}

/// Convert domain `Permissions` to `WirePermissions`.
pub fn domain_permissions_to_wire(perms: &Permissions) -> WirePermissions {
    let streams: Vec<WireStreamPermissions> = perms
        .streams
        .as_ref()
        .map(|map| {
            map.iter()
                .map(|(&sid, sp)| domain_stream_to_wire(sid, sp))
                .collect()
        })
        .unwrap_or_default();
    WirePermissions {
        global: WireGlobalPermissions {
            manage_servers: perms.global.manage_servers,
            read_servers: perms.global.read_servers,
            manage_users: perms.global.manage_users,
            read_users: perms.global.read_users,
            manage_streams: perms.global.manage_streams,
            read_streams: perms.global.read_streams,
            manage_topics: perms.global.manage_topics,
            read_topics: perms.global.read_topics,
            poll_messages: perms.global.poll_messages,
            send_messages: perms.global.send_messages,
        },
        streams,
    }
}

/// Maximum payload size for control-plane commands (non-SendMessages).
/// Prevents OOM from malicious clients sending `length = u32::MAX`.
/// SendMessages has its own size validation via `total_payload_size` checks.
pub const MAX_CONTROL_FRAME_PAYLOAD: u32 = 10 * 1024 * 1024; // 10 MB

/// Dispatch a SendMessages command with staged socket reads (zero-copy path).
///
/// Called by transport layers when the command code is `SEND_MESSAGES_CODE`.
/// The handler reads metadata, indexes, and messages directly from the socket
/// into separate `PooledBuffer`s for zero-copy partition append.
pub async fn dispatch_send_messages(
    sender: &mut SenderKind,
    payload_length: u32,
    session: &Session,
    shard: &Rc<IggyShard>,
) -> Result<HandlerResult, IggyError> {
    handlers::messages::send_messages_handler::handle_send_messages(
        sender,
        payload_length,
        session,
        shard,
    )
    .await
}

/// Central command dispatch for a decoded request frame.
///
/// Transport layers read the 8-byte header, validate via
/// `RequestFrame::payload_length()`, read the full payload, construct a
/// `RequestFrame::from_parts(code, frame.payload)`, and pass it here.
///
/// SendMessages is handled separately via `dispatch_send_messages()`.
#[allow(clippy::too_many_lines)]
pub async fn dispatch(
    frame: RequestFrame<'_>,
    sender: &mut SenderKind,
    session: &Session,
    shard: &Rc<IggyShard>,
) -> Result<HandlerResult, IggyError> {
    match frame.code {
        // System
        PING_CODE => {
            handlers::system::ping_handler::handle_ping(sender, session, shard).await
        }
        GET_STATS_CODE => {
            handlers::system::get_stats_handler::handle_get_stats(sender, session, shard).await
        }
        GET_ME_CODE => {
            handlers::system::get_me_handler::handle_get_me(sender, session, shard).await
        }
        GET_CLIENT_CODE => {
            let req: GetClientRequest = decode(frame.payload)?;
            handlers::system::get_client_handler::handle_get_client(req, sender, session, shard)
                .await
        }
        GET_CLIENTS_CODE => {
            handlers::system::get_clients_handler::handle_get_clients(sender, session, shard).await
        }
        GET_SNAPSHOT_FILE_CODE => {
            let req: GetSnapshotRequest = decode(frame.payload)?;
            handlers::system::get_snapshot_handler::handle_get_snapshot(
                req, sender, session, shard,
            )
            .await
        }
        GET_CLUSTER_METADATA_CODE => {
            handlers::cluster::get_cluster_metadata_handler::handle_get_cluster_metadata(
                sender, session, shard,
            )
            .await
        }

        // Streams
        GET_STREAM_CODE => {
            let req: GetStreamRequest = decode(frame.payload)?;
            handlers::streams::get_stream_handler::handle_get_stream(req, sender, session, shard)
                .await
        }
        GET_STREAMS_CODE => {
            handlers::streams::get_streams_handler::handle_get_streams(sender, session, shard)
                .await
        }
        CREATE_STREAM_CODE => {
            let req: CreateStreamRequest = decode(frame.payload)?;
            handlers::streams::create_stream_handler::handle_create_stream(
                req, sender, session, shard,
            )
            .await
        }
        DELETE_STREAM_CODE => {
            let req: DeleteStreamRequest = decode(frame.payload)?;
            handlers::streams::delete_stream_handler::handle_delete_stream(
                req, sender, session, shard,
            )
            .await
        }
        UPDATE_STREAM_CODE => {
            let req: UpdateStreamRequest = decode(frame.payload)?;
            handlers::streams::update_stream_handler::handle_update_stream(
                req, sender, session, shard,
            )
            .await
        }
        PURGE_STREAM_CODE => {
            let req: PurgeStreamRequest = decode(frame.payload)?;
            handlers::streams::purge_stream_handler::handle_purge_stream(
                req, sender, session, shard,
            )
            .await
        }

        // Topics
        GET_TOPIC_CODE => {
            let req: GetTopicRequest = decode(frame.payload)?;
            handlers::topics::get_topic_handler::handle_get_topic(req, sender, session, shard)
                .await
        }
        GET_TOPICS_CODE => {
            let req: GetTopicsRequest = decode(frame.payload)?;
            handlers::topics::get_topics_handler::handle_get_topics(req, sender, session, shard)
                .await
        }
        CREATE_TOPIC_CODE => {
            let req: CreateTopicRequest = decode(frame.payload)?;
            handlers::topics::create_topic_handler::handle_create_topic(
                req, sender, session, shard,
            )
            .await
        }
        DELETE_TOPIC_CODE => {
            let req: DeleteTopicRequest = decode(frame.payload)?;
            handlers::topics::delete_topic_handler::handle_delete_topic(
                req, sender, session, shard,
            )
            .await
        }
        UPDATE_TOPIC_CODE => {
            let req: UpdateTopicRequest = decode(frame.payload)?;
            handlers::topics::update_topic_handler::handle_update_topic(
                req, sender, session, shard,
            )
            .await
        }
        PURGE_TOPIC_CODE => {
            let req: PurgeTopicRequest = decode(frame.payload)?;
            handlers::topics::purge_topic_handler::handle_purge_topic(
                req, sender, session, shard,
            )
            .await
        }

        // Partitions
        CREATE_PARTITIONS_CODE => {
            let req: CreatePartitionsRequest = decode(frame.payload)?;
            handlers::partitions::create_partitions_handler::handle_create_partitions(
                req, sender, session, shard,
            )
            .await
        }
        DELETE_PARTITIONS_CODE => {
            let req: DeletePartitionsRequest = decode(frame.payload)?;
            handlers::partitions::delete_partitions_handler::handle_delete_partitions(
                req, sender, session, shard,
            )
            .await
        }

        // Segments
        DELETE_SEGMENTS_CODE => {
            let req: DeleteSegmentsRequest = decode(frame.payload)?;
            handlers::segments::delete_segments_handler::handle_delete_segments(
                req, sender, session, shard,
            )
            .await
        }

        // Messages (PollMessages + FlushUnsavedBuffer; SendMessages handled above)
        POLL_MESSAGES_CODE => {
            let req: PollMessagesRequest = decode(frame.payload)?;
            handlers::messages::poll_messages_handler::handle_poll_messages(
                req, sender, session, shard,
            )
            .await
        }
        FLUSH_UNSAVED_BUFFER_CODE => {
            let req: FlushUnsavedBufferRequest = decode(frame.payload)?;
            handlers::messages::flush_unsaved_buffer_handler::handle_flush_unsaved_buffer(
                req, sender, session, shard,
            )
            .await
        }

        // Consumer Offsets
        GET_CONSUMER_OFFSET_CODE => {
            let req: GetConsumerOffsetRequest = decode(frame.payload)?;
            handlers::consumer_offsets::get_consumer_offset_handler::handle_get_consumer_offset(
                req, sender, session, shard,
            )
            .await
        }
        STORE_CONSUMER_OFFSET_CODE => {
            let req: StoreConsumerOffsetRequest = decode(frame.payload)?;
            handlers::consumer_offsets::store_consumer_offset_handler::handle_store_consumer_offset(
                req, sender, session, shard,
            )
            .await
        }
        DELETE_CONSUMER_OFFSET_CODE => {
            let req: DeleteConsumerOffsetRequest = decode(frame.payload)?;
            handlers::consumer_offsets::delete_consumer_offset_handler::handle_delete_consumer_offset(
                req, sender, session, shard,
            )
            .await
        }

        // Consumer Groups
        GET_CONSUMER_GROUP_CODE => {
            let req: GetConsumerGroupRequest = decode(frame.payload)?;
            handlers::consumer_groups::get_consumer_group_handler::handle_get_consumer_group(
                req, sender, session, shard,
            )
            .await
        }
        GET_CONSUMER_GROUPS_CODE => {
            let req: GetConsumerGroupsRequest = decode(frame.payload)?;
            handlers::consumer_groups::get_consumer_groups_handler::handle_get_consumer_groups(
                req, sender, session, shard,
            )
            .await
        }
        CREATE_CONSUMER_GROUP_CODE => {
            let req: CreateConsumerGroupRequest = decode(frame.payload)?;
            handlers::consumer_groups::create_consumer_group_handler::handle_create_consumer_group(
                req, sender, session, shard,
            )
            .await
        }
        DELETE_CONSUMER_GROUP_CODE => {
            let req: DeleteConsumerGroupRequest = decode(frame.payload)?;
            handlers::consumer_groups::delete_consumer_group_handler::handle_delete_consumer_group(
                req, sender, session, shard,
            )
            .await
        }
        JOIN_CONSUMER_GROUP_CODE => {
            let req: JoinConsumerGroupRequest = decode(frame.payload)?;
            handlers::consumer_groups::join_consumer_group_handler::handle_join_consumer_group(
                req, sender, session, shard,
            )
            .await
        }
        LEAVE_CONSUMER_GROUP_CODE => {
            let req: LeaveConsumerGroupRequest = decode(frame.payload)?;
            handlers::consumer_groups::leave_consumer_group_handler::handle_leave_consumer_group(
                req, sender, session, shard,
            )
            .await
        }

        // Users
        GET_USER_CODE => {
            let req: GetUserRequest = decode(frame.payload)?;
            handlers::users::get_user_handler::handle_get_user(req, sender, session, shard).await
        }
        GET_USERS_CODE => {
            handlers::users::get_users_handler::handle_get_users(sender, session, shard).await
        }
        CREATE_USER_CODE => {
            let req: CreateUserRequest = decode(frame.payload)?;
            handlers::users::create_user_handler::handle_create_user(req, sender, session, shard)
                .await
        }
        DELETE_USER_CODE => {
            let req: DeleteUserRequest = decode(frame.payload)?;
            handlers::users::delete_user_handler::handle_delete_user(req, sender, session, shard)
                .await
        }
        UPDATE_USER_CODE => {
            let req: UpdateUserRequest = decode(frame.payload)?;
            handlers::users::update_user_handler::handle_update_user(req, sender, session, shard)
                .await
        }
        UPDATE_PERMISSIONS_CODE => {
            let req: UpdatePermissionsRequest = decode(frame.payload)?;
            handlers::users::update_permissions_handler::handle_update_permissions(
                req, sender, session, shard,
            )
            .await
        }
        CHANGE_PASSWORD_CODE => {
            let req: ChangePasswordRequest = decode(frame.payload)?;
            handlers::users::change_password_handler::handle_change_password(
                req, sender, session, shard,
            )
            .await
        }
        LOGIN_USER_CODE => {
            let req: LoginUserRequest = decode(frame.payload)?;
            handlers::users::login_user_handler::handle_login_user(req, sender, session, shard)
                .await
        }
        LOGOUT_USER_CODE => {
            handlers::users::logout_user_handler::handle_logout_user(sender, session, shard).await
        }

        // Personal Access Tokens
        GET_PERSONAL_ACCESS_TOKENS_CODE => {
            handlers::personal_access_tokens::get_personal_access_tokens_handler::handle_get_personal_access_tokens(
                sender, session, shard,
            )
            .await
        }
        CREATE_PERSONAL_ACCESS_TOKEN_CODE => {
            let req: CreatePersonalAccessTokenRequest = decode(frame.payload)?;
            handlers::personal_access_tokens::create_personal_access_token_handler::handle_create_personal_access_token(
                req, sender, session, shard,
            )
            .await
        }
        DELETE_PERSONAL_ACCESS_TOKEN_CODE => {
            let req: DeletePersonalAccessTokenRequest = decode(frame.payload)?;
            handlers::personal_access_tokens::delete_personal_access_token_handler::handle_delete_personal_access_token(
                req, sender, session, shard,
            )
            .await
        }
        LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE => {
            let req: LoginWithPersonalAccessTokenRequest = decode(frame.payload)?;
            handlers::personal_access_tokens::login_with_personal_access_token_handler::handle_login_with_personal_access_token(
                req, sender, session, shard,
            )
            .await
        }

        _ => {
            error!("Unknown command code: {}", frame.code);
            Err(IggyError::InvalidCommand)
        }
    }
}
