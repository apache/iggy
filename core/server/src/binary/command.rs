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

use crate::define_server_command_enum;
use crate::shard::IggyShard;
use crate::streaming::session::Session;
use bytes::{BufMut, Bytes, BytesMut};
use enum_dispatch::enum_dispatch;
use iggy_common::SenderKind;
use iggy_common::change_password::ChangePassword;
use iggy_common::create_consumer_group::CreateConsumerGroup;
use iggy_common::create_partitions::CreatePartitions;
use iggy_common::create_personal_access_token::CreatePersonalAccessToken;
use iggy_common::create_stream::CreateStream;
use iggy_common::create_topic::CreateTopic;
use iggy_common::create_user::CreateUser;
use iggy_common::delete_consumer_group::DeleteConsumerGroup;
use iggy_common::delete_consumer_offset::DeleteConsumerOffset;
use iggy_common::delete_partitions::DeletePartitions;
use iggy_common::delete_personal_access_token::DeletePersonalAccessToken;
use iggy_common::delete_segments::DeleteSegments;
use iggy_common::delete_stream::DeleteStream;
use iggy_common::delete_topic::DeleteTopic;
use iggy_common::delete_user::DeleteUser;
use iggy_common::get_client::GetClient;
use iggy_common::get_clients::GetClients;
use iggy_common::get_cluster_metadata::GetClusterMetadata;
use iggy_common::get_consumer_group::GetConsumerGroup;
use iggy_common::get_consumer_groups::GetConsumerGroups;
use iggy_common::get_consumer_offset::GetConsumerOffset;
use iggy_common::get_me::GetMe;
use iggy_common::get_personal_access_tokens::GetPersonalAccessTokens;
use iggy_common::get_snapshot::GetSnapshot;
use iggy_common::get_stats::GetStats;
use iggy_common::get_stream::GetStream;
use iggy_common::get_streams::GetStreams;
use iggy_common::get_topic::GetTopic;
use iggy_common::get_topics::GetTopics;
use iggy_common::get_user::GetUser;
use iggy_common::get_users::GetUsers;
use iggy_common::join_consumer_group::JoinConsumerGroup;
use iggy_common::leave_consumer_group::LeaveConsumerGroup;
use iggy_common::login_user::LoginUser;
use iggy_common::login_with_personal_access_token::LoginWithPersonalAccessToken;
use iggy_common::logout_user::LogoutUser;
use iggy_common::ping::Ping;
use iggy_common::purge_stream::PurgeStream;
use iggy_common::purge_topic::PurgeTopic;
use iggy_common::store_consumer_offset::StoreConsumerOffset;
use iggy_common::update_permissions::UpdatePermissions;
use iggy_common::update_stream::UpdateStream;
use iggy_common::update_topic::UpdateTopic;
use iggy_common::update_user::UpdateUser;
use iggy_common::*;
use std::rc::Rc;
use strum::{EnumIter, EnumString};
use tracing::error;

define_server_command_enum! {
    Ping(Ping), PING_CODE, PING, false;
    GetStats(GetStats), GET_STATS_CODE, GET_STATS, false;
    GetMe(GetMe), GET_ME_CODE, GET_ME, false;
    GetClient(GetClient), GET_CLIENT_CODE, GET_CLIENT, true;
    GetClients(GetClients), GET_CLIENTS_CODE, GET_CLIENTS, false;
    GetSnapshot(GetSnapshot), GET_SNAPSHOT_FILE_CODE, GET_SNAPSHOT_FILE, false;
    GetClusterMetadata(GetClusterMetadata), GET_CLUSTER_METADATA_CODE, GET_CLUSTER_METADATA, false;
    PollMessages(PollMessages), POLL_MESSAGES_CODE, POLL_MESSAGES, true;
    FlushUnsavedBuffer(FlushUnsavedBuffer), FLUSH_UNSAVED_BUFFER_CODE, FLUSH_UNSAVED_BUFFER, true;
    GetUser(GetUser), GET_USER_CODE, GET_USER, true;
    GetUsers(GetUsers), GET_USERS_CODE, GET_USERS, false;
    CreateUser(CreateUser), CREATE_USER_CODE, CREATE_USER, true;
    DeleteUser(DeleteUser), DELETE_USER_CODE, DELETE_USER, true;
    UpdateUser(UpdateUser), UPDATE_USER_CODE, UPDATE_USER, true;
    UpdatePermissions(UpdatePermissions), UPDATE_PERMISSIONS_CODE, UPDATE_PERMISSIONS, true;
    ChangePassword(ChangePassword), CHANGE_PASSWORD_CODE, CHANGE_PASSWORD, true;
    LoginUser(LoginUser), LOGIN_USER_CODE, LOGIN_USER, true;
    LogoutUser(LogoutUser), LOGOUT_USER_CODE, LOGOUT_USER, false;
    GetPersonalAccessTokens(GetPersonalAccessTokens), GET_PERSONAL_ACCESS_TOKENS_CODE, GET_PERSONAL_ACCESS_TOKENS, false;
    CreatePersonalAccessToken(CreatePersonalAccessToken), CREATE_PERSONAL_ACCESS_TOKEN_CODE, CREATE_PERSONAL_ACCESS_TOKEN, true;
    DeletePersonalAccessToken(DeletePersonalAccessToken), DELETE_PERSONAL_ACCESS_TOKEN_CODE, DELETE_PERSONAL_ACCESS_TOKEN, false;
    LoginWithPersonalAccessToken(LoginWithPersonalAccessToken), LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE, LOGIN_WITH_PERSONAL_ACCESS_TOKEN, true;
    SendMessages(SendMessages), SEND_MESSAGES_CODE, SEND_MESSAGES, false;
    GetConsumerOffset(GetConsumerOffset), GET_CONSUMER_OFFSET_CODE, GET_CONSUMER_OFFSET, true;
    StoreConsumerOffset(StoreConsumerOffset), STORE_CONSUMER_OFFSET_CODE, STORE_CONSUMER_OFFSET, true;
    DeleteConsumerOffset(DeleteConsumerOffset), DELETE_CONSUMER_OFFSET_CODE, DELETE_CONSUMER_OFFSET, true;
    GetStream(GetStream), GET_STREAM_CODE, GET_STREAM, true;
    GetStreams(GetStreams), GET_STREAMS_CODE, GET_STREAMS, false;
    CreateStream(CreateStream), CREATE_STREAM_CODE, CREATE_STREAM, true;
    DeleteStream(DeleteStream), DELETE_STREAM_CODE, DELETE_STREAM, true;
    UpdateStream(UpdateStream), UPDATE_STREAM_CODE, UPDATE_STREAM, true;
    PurgeStream(PurgeStream), PURGE_STREAM_CODE, PURGE_STREAM, true;
    GetTopic(GetTopic), GET_TOPIC_CODE, GET_TOPIC, true;
    GetTopics(GetTopics), GET_TOPICS_CODE, GET_TOPICS, false;
    CreateTopic(CreateTopic), CREATE_TOPIC_CODE, CREATE_TOPIC, true;
    DeleteTopic(DeleteTopic), DELETE_TOPIC_CODE, DELETE_TOPIC, true;
    UpdateTopic(UpdateTopic), UPDATE_TOPIC_CODE, UPDATE_TOPIC, true;
    PurgeTopic(PurgeTopic), PURGE_TOPIC_CODE, PURGE_TOPIC, true;
    CreatePartitions(CreatePartitions), CREATE_PARTITIONS_CODE, CREATE_PARTITIONS, true;
    DeletePartitions(DeletePartitions), DELETE_PARTITIONS_CODE, DELETE_PARTITIONS, true;
    DeleteSegments(DeleteSegments), DELETE_SEGMENTS_CODE, DELETE_SEGMENTS, true;
    GetConsumerGroup(GetConsumerGroup), GET_CONSUMER_GROUP_CODE, GET_CONSUMER_GROUP, true;
    GetConsumerGroups(GetConsumerGroups), GET_CONSUMER_GROUPS_CODE, GET_CONSUMER_GROUPS, false;
    CreateConsumerGroup(CreateConsumerGroup), CREATE_CONSUMER_GROUP_CODE, CREATE_CONSUMER_GROUP, true;
    DeleteConsumerGroup(DeleteConsumerGroup), DELETE_CONSUMER_GROUP_CODE, DELETE_CONSUMER_GROUP, true;
    JoinConsumerGroup(JoinConsumerGroup), JOIN_CONSUMER_GROUP_CODE, JOIN_CONSUMER_GROUP, true;
    LeaveConsumerGroup(LeaveConsumerGroup), LEAVE_CONSUMER_GROUP_CODE, LEAVE_CONSUMER_GROUP, true;
}

/// Indicates whether a command handler completed normally or migrated the connection.
pub enum HandlerResult {
    /// Command completed, connection stays on current shard.
    Finished,

    /// Connection was migrated to another shard. Source shard should exit without cleanup.
    Migrated { to_shard: u16 },
}

#[enum_dispatch]
pub trait ServerCommandHandler {
    /// Return the command code
    fn code(&self) -> u32;

    /// Handle the command execution
    #[allow(async_fn_in_trait)]
    async fn handle(
        self,
        sender: &mut SenderKind,
        length: u32,
        session: &Session,
        shard: &Rc<IggyShard>,
    ) -> Result<HandlerResult, IggyError>;
}

pub trait BinaryServerCommand {
    /// Parse command from sender
    #[allow(async_fn_in_trait)]
    async fn from_sender(
        sender: &mut SenderKind,
        code: u32,
        length: u32,
    ) -> Result<Self, IggyError>
    where
        Self: Sized;
}

fn as_bytes<T: Command>(command: &T) -> Bytes {
    let payload = command.to_bytes();
    let mut bytes = BytesMut::with_capacity(4 + payload.len());
    bytes.put_u32_le(command.code());
    bytes.put_slice(&payload);
    bytes.freeze()
}
