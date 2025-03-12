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

use bytes::{BufMut, Bytes, BytesMut};
use error_set::ErrContext;
use iggy::command::*;
use iggy::consumer_groups::create_consumer_group::CreateConsumerGroup;
use iggy::consumer_groups::delete_consumer_group::DeleteConsumerGroup;
use iggy::consumer_groups::get_consumer_group::GetConsumerGroup;
use iggy::consumer_groups::get_consumer_groups::GetConsumerGroups;
use iggy::consumer_groups::join_consumer_group::JoinConsumerGroup;
use iggy::consumer_groups::leave_consumer_group::LeaveConsumerGroup;
use iggy::consumer_offsets::delete_consumer_offset::DeleteConsumerOffset;
use iggy::consumer_offsets::get_consumer_offset::GetConsumerOffset;
use iggy::consumer_offsets::store_consumer_offset::StoreConsumerOffset;
use iggy::error::IggyError;
use iggy::messages::poll_messages::PollMessages;
use iggy::messages::send_messages::SendMessages;
use iggy::partitions::create_partitions::CreatePartitions;
use iggy::partitions::delete_partitions::DeletePartitions;
use iggy::personal_access_tokens::create_personal_access_token::CreatePersonalAccessToken;
use iggy::personal_access_tokens::delete_personal_access_token::DeletePersonalAccessToken;
use iggy::personal_access_tokens::get_personal_access_tokens::GetPersonalAccessTokens;
use iggy::personal_access_tokens::login_with_personal_access_token::LoginWithPersonalAccessToken;
use iggy::streams::create_stream::CreateStream;
use iggy::streams::delete_stream::DeleteStream;
use iggy::streams::get_stream::GetStream;
use iggy::streams::get_streams::GetStreams;
use iggy::streams::purge_stream::PurgeStream;
use iggy::streams::update_stream::UpdateStream;
use iggy::system::get_client::GetClient;
use iggy::system::get_clients::GetClients;
use iggy::system::get_me::GetMe;
use iggy::system::get_snapshot::GetSnapshot;
use iggy::system::get_stats::GetStats;
use iggy::system::ping::Ping;
use iggy::topics::create_topic::CreateTopic;
use iggy::topics::delete_topic::DeleteTopic;
use iggy::topics::get_topic::GetTopic;
use iggy::topics::get_topics::GetTopics;
use iggy::topics::purge_topic::PurgeTopic;
use iggy::topics::update_topic::UpdateTopic;
use iggy::users::change_password::ChangePassword;
use iggy::users::create_user::CreateUser;
use iggy::users::delete_user::DeleteUser;
use iggy::users::get_user::GetUser;
use iggy::users::get_users::GetUsers;
use iggy::users::login_user::LoginUser;
use iggy::users::logout_user::LogoutUser;
use iggy::users::update_permissions::UpdatePermissions;
use iggy::users::update_user::UpdateUser;
use iggy::validatable::Validatable;
use iggy::{
    bytes_serializable::BytesSerializable, messages::flush_unsaved_buffer::FlushUnsavedBuffer,
};
use std::fmt::{Display, Formatter};
use strum::EnumString;
use tracing::error;

#[derive(Debug, PartialEq, EnumString)]
pub enum ServerCommand {
    Ping(Ping),
    GetStats(GetStats),
    GetMe(GetMe),
    GetClient(GetClient),
    GetClients(GetClients),
    GetUser(GetUser),
    GetUsers(GetUsers),
    CreateUser(CreateUser),
    DeleteUser(DeleteUser),
    UpdateUser(UpdateUser),
    UpdatePermissions(UpdatePermissions),
    ChangePassword(ChangePassword),
    LoginUser(LoginUser),
    LogoutUser(LogoutUser),
    GetPersonalAccessTokens(GetPersonalAccessTokens),
    CreatePersonalAccessToken(CreatePersonalAccessToken),
    DeletePersonalAccessToken(DeletePersonalAccessToken),
    LoginWithPersonalAccessToken(LoginWithPersonalAccessToken),
    SendMessages(SendMessages),
    PollMessages(PollMessages),
    FlushUnsavedBuffer(FlushUnsavedBuffer),
    GetConsumerOffset(GetConsumerOffset),
    StoreConsumerOffset(StoreConsumerOffset),
    DeleteConsumerOffset(DeleteConsumerOffset),
    GetStream(GetStream),
    GetStreams(GetStreams),
    CreateStream(CreateStream),
    DeleteStream(DeleteStream),
    UpdateStream(UpdateStream),
    PurgeStream(PurgeStream),
    GetTopic(GetTopic),
    GetTopics(GetTopics),
    CreateTopic(CreateTopic),
    DeleteTopic(DeleteTopic),
    UpdateTopic(UpdateTopic),
    PurgeTopic(PurgeTopic),
    CreatePartitions(CreatePartitions),
    DeletePartitions(DeletePartitions),
    GetConsumerGroup(GetConsumerGroup),
    GetConsumerGroups(GetConsumerGroups),
    CreateConsumerGroup(CreateConsumerGroup),
    DeleteConsumerGroup(DeleteConsumerGroup),
    JoinConsumerGroup(JoinConsumerGroup),
    LeaveConsumerGroup(LeaveConsumerGroup),
    GetSnapshotFile(GetSnapshot),
}

impl BytesSerializable for ServerCommand {
    fn to_bytes(&self) -> Bytes {
        match self {
            ServerCommand::Ping(payload) => as_bytes(payload),
            ServerCommand::GetStats(payload) => as_bytes(payload),
            ServerCommand::GetMe(payload) => as_bytes(payload),
            ServerCommand::GetClient(payload) => as_bytes(payload),
            ServerCommand::GetClients(payload) => as_bytes(payload),
            ServerCommand::GetUser(payload) => as_bytes(payload),
            ServerCommand::GetUsers(payload) => as_bytes(payload),
            ServerCommand::CreateUser(payload) => as_bytes(payload),
            ServerCommand::DeleteUser(payload) => as_bytes(payload),
            ServerCommand::UpdateUser(payload) => as_bytes(payload),
            ServerCommand::UpdatePermissions(payload) => as_bytes(payload),
            ServerCommand::ChangePassword(payload) => as_bytes(payload),
            ServerCommand::LoginUser(payload) => as_bytes(payload),
            ServerCommand::LogoutUser(payload) => as_bytes(payload),
            ServerCommand::GetPersonalAccessTokens(payload) => as_bytes(payload),
            ServerCommand::CreatePersonalAccessToken(payload) => as_bytes(payload),
            ServerCommand::DeletePersonalAccessToken(payload) => as_bytes(payload),
            ServerCommand::LoginWithPersonalAccessToken(payload) => as_bytes(payload),
            ServerCommand::SendMessages(payload) => as_bytes(payload),
            ServerCommand::PollMessages(payload) => as_bytes(payload),
            ServerCommand::StoreConsumerOffset(payload) => as_bytes(payload),
            ServerCommand::DeleteConsumerOffset(payload) => as_bytes(payload),
            ServerCommand::GetConsumerOffset(payload) => as_bytes(payload),
            ServerCommand::GetStream(payload) => as_bytes(payload),
            ServerCommand::GetStreams(payload) => as_bytes(payload),
            ServerCommand::CreateStream(payload) => as_bytes(payload),
            ServerCommand::DeleteStream(payload) => as_bytes(payload),
            ServerCommand::UpdateStream(payload) => as_bytes(payload),
            ServerCommand::PurgeStream(payload) => as_bytes(payload),
            ServerCommand::GetTopic(payload) => as_bytes(payload),
            ServerCommand::GetTopics(payload) => as_bytes(payload),
            ServerCommand::CreateTopic(payload) => as_bytes(payload),
            ServerCommand::DeleteTopic(payload) => as_bytes(payload),
            ServerCommand::UpdateTopic(payload) => as_bytes(payload),
            ServerCommand::PurgeTopic(payload) => as_bytes(payload),
            ServerCommand::CreatePartitions(payload) => as_bytes(payload),
            ServerCommand::DeletePartitions(payload) => as_bytes(payload),
            ServerCommand::GetConsumerGroup(payload) => as_bytes(payload),
            ServerCommand::GetConsumerGroups(payload) => as_bytes(payload),
            ServerCommand::CreateConsumerGroup(payload) => as_bytes(payload),
            ServerCommand::DeleteConsumerGroup(payload) => as_bytes(payload),
            ServerCommand::JoinConsumerGroup(payload) => as_bytes(payload),
            ServerCommand::LeaveConsumerGroup(payload) => as_bytes(payload),
            ServerCommand::FlushUnsavedBuffer(payload) => as_bytes(payload),
            ServerCommand::GetSnapshotFile(payload) => as_bytes(payload),
        }
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError> {
        let code = u32::from_le_bytes(
            bytes[..4]
                .try_into()
                .with_error_context(|error| format!("failed to decode command from bytes. {error}"))
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        let payload = bytes.slice(4..);
        match code {
            PING_CODE => Ok(ServerCommand::Ping(Ping::from_bytes(payload)?)),
            GET_STATS_CODE => Ok(ServerCommand::GetStats(GetStats::from_bytes(payload)?)),
            GET_ME_CODE => Ok(ServerCommand::GetMe(GetMe::from_bytes(payload)?)),
            GET_CLIENT_CODE => Ok(ServerCommand::GetClient(GetClient::from_bytes(payload)?)),
            GET_CLIENTS_CODE => Ok(ServerCommand::GetClients(GetClients::from_bytes(payload)?)),
            GET_USER_CODE => Ok(ServerCommand::GetUser(GetUser::from_bytes(payload)?)),
            GET_USERS_CODE => Ok(ServerCommand::GetUsers(GetUsers::from_bytes(payload)?)),
            CREATE_USER_CODE => Ok(ServerCommand::CreateUser(CreateUser::from_bytes(payload)?)),
            DELETE_USER_CODE => Ok(ServerCommand::DeleteUser(DeleteUser::from_bytes(payload)?)),
            UPDATE_USER_CODE => Ok(ServerCommand::UpdateUser(UpdateUser::from_bytes(payload)?)),
            UPDATE_PERMISSIONS_CODE => Ok(ServerCommand::UpdatePermissions(
                UpdatePermissions::from_bytes(payload)?,
            )),
            CHANGE_PASSWORD_CODE => Ok(ServerCommand::ChangePassword(ChangePassword::from_bytes(
                payload,
            )?)),
            LOGIN_USER_CODE => Ok(ServerCommand::LoginUser(LoginUser::from_bytes(payload)?)),
            LOGOUT_USER_CODE => Ok(ServerCommand::LogoutUser(LogoutUser::from_bytes(payload)?)),
            GET_PERSONAL_ACCESS_TOKENS_CODE => Ok(ServerCommand::GetPersonalAccessTokens(
                GetPersonalAccessTokens::from_bytes(payload)?,
            )),
            CREATE_PERSONAL_ACCESS_TOKEN_CODE => Ok(ServerCommand::CreatePersonalAccessToken(
                CreatePersonalAccessToken::from_bytes(payload)?,
            )),
            DELETE_PERSONAL_ACCESS_TOKEN_CODE => Ok(ServerCommand::DeletePersonalAccessToken(
                DeletePersonalAccessToken::from_bytes(payload)?,
            )),
            LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE => {
                Ok(ServerCommand::LoginWithPersonalAccessToken(
                    LoginWithPersonalAccessToken::from_bytes(payload)?,
                ))
            }
            SEND_MESSAGES_CODE => Ok(ServerCommand::SendMessages(SendMessages::from_bytes(
                payload,
            )?)),
            POLL_MESSAGES_CODE => Ok(ServerCommand::PollMessages(PollMessages::from_bytes(
                payload,
            )?)),
            FLUSH_UNSAVED_BUFFER_CODE => Ok(ServerCommand::FlushUnsavedBuffer(
                FlushUnsavedBuffer::from_bytes(payload)?,
            )),
            STORE_CONSUMER_OFFSET_CODE => Ok(ServerCommand::StoreConsumerOffset(
                StoreConsumerOffset::from_bytes(payload)?,
            )),
            DELETE_CONSUMER_OFFSET_CODE => Ok(ServerCommand::DeleteConsumerOffset(
                DeleteConsumerOffset::from_bytes(payload)?,
            )),
            GET_CONSUMER_OFFSET_CODE => Ok(ServerCommand::GetConsumerOffset(
                GetConsumerOffset::from_bytes(payload)?,
            )),
            GET_STREAM_CODE => Ok(ServerCommand::GetStream(GetStream::from_bytes(payload)?)),
            GET_STREAMS_CODE => Ok(ServerCommand::GetStreams(GetStreams::from_bytes(payload)?)),
            CREATE_STREAM_CODE => Ok(ServerCommand::CreateStream(CreateStream::from_bytes(
                payload,
            )?)),
            DELETE_STREAM_CODE => Ok(ServerCommand::DeleteStream(DeleteStream::from_bytes(
                payload,
            )?)),
            UPDATE_STREAM_CODE => Ok(ServerCommand::UpdateStream(UpdateStream::from_bytes(
                payload,
            )?)),
            PURGE_STREAM_CODE => Ok(ServerCommand::PurgeStream(PurgeStream::from_bytes(
                payload,
            )?)),
            GET_TOPIC_CODE => Ok(ServerCommand::GetTopic(GetTopic::from_bytes(payload)?)),
            GET_TOPICS_CODE => Ok(ServerCommand::GetTopics(GetTopics::from_bytes(payload)?)),
            CREATE_TOPIC_CODE => Ok(ServerCommand::CreateTopic(CreateTopic::from_bytes(
                payload,
            )?)),
            DELETE_TOPIC_CODE => Ok(ServerCommand::DeleteTopic(DeleteTopic::from_bytes(
                payload,
            )?)),
            UPDATE_TOPIC_CODE => Ok(ServerCommand::UpdateTopic(UpdateTopic::from_bytes(
                payload,
            )?)),
            PURGE_TOPIC_CODE => Ok(ServerCommand::PurgeTopic(PurgeTopic::from_bytes(payload)?)),
            CREATE_PARTITIONS_CODE => Ok(ServerCommand::CreatePartitions(
                CreatePartitions::from_bytes(payload)?,
            )),
            DELETE_PARTITIONS_CODE => Ok(ServerCommand::DeletePartitions(
                DeletePartitions::from_bytes(payload)?,
            )),
            GET_CONSUMER_GROUP_CODE => Ok(ServerCommand::GetConsumerGroup(
                GetConsumerGroup::from_bytes(payload)?,
            )),
            GET_CONSUMER_GROUPS_CODE => Ok(ServerCommand::GetConsumerGroups(
                GetConsumerGroups::from_bytes(payload)?,
            )),
            CREATE_CONSUMER_GROUP_CODE => Ok(ServerCommand::CreateConsumerGroup(
                CreateConsumerGroup::from_bytes(payload)?,
            )),
            DELETE_CONSUMER_GROUP_CODE => Ok(ServerCommand::DeleteConsumerGroup(
                DeleteConsumerGroup::from_bytes(payload)?,
            )),
            JOIN_CONSUMER_GROUP_CODE => Ok(ServerCommand::JoinConsumerGroup(
                JoinConsumerGroup::from_bytes(payload)?,
            )),
            LEAVE_CONSUMER_GROUP_CODE => Ok(ServerCommand::LeaveConsumerGroup(
                LeaveConsumerGroup::from_bytes(payload)?,
            )),
            GET_SNAPSHOT_FILE_CODE => Ok(ServerCommand::GetSnapshotFile(GetSnapshot::from_bytes(
                payload,
            )?)),
            _ => {
                error!("Invalid server command: {code}");
                Err(IggyError::InvalidCommand)
            }
        }
    }
}

fn as_bytes<T: Command>(command: &T) -> Bytes {
    let payload = command.to_bytes();
    let mut bytes = BytesMut::with_capacity(4 + payload.len());
    bytes.put_u32_le(command.code());
    bytes.put_slice(&payload);
    bytes.freeze()
}

impl Validatable<IggyError> for ServerCommand {
    fn validate(&self) -> Result<(), IggyError> {
        match self {
            ServerCommand::Ping(command) => command.validate(),
            ServerCommand::GetStats(command) => command.validate(),
            ServerCommand::GetMe(command) => command.validate(),
            ServerCommand::GetClient(command) => command.validate(),
            ServerCommand::GetClients(command) => command.validate(),
            ServerCommand::GetUser(command) => command.validate(),
            ServerCommand::GetUsers(command) => command.validate(),
            ServerCommand::CreateUser(command) => command.validate(),
            ServerCommand::DeleteUser(command) => command.validate(),
            ServerCommand::UpdateUser(command) => command.validate(),
            ServerCommand::UpdatePermissions(command) => command.validate(),
            ServerCommand::ChangePassword(command) => command.validate(),
            ServerCommand::LoginUser(command) => command.validate(),
            ServerCommand::LogoutUser(command) => command.validate(),
            ServerCommand::GetPersonalAccessTokens(command) => command.validate(),
            ServerCommand::CreatePersonalAccessToken(command) => command.validate(),
            ServerCommand::DeletePersonalAccessToken(command) => command.validate(),
            ServerCommand::LoginWithPersonalAccessToken(command) => command.validate(),
            ServerCommand::SendMessages(command) => command.validate(),
            ServerCommand::PollMessages(command) => command.validate(),
            ServerCommand::StoreConsumerOffset(command) => command.validate(),
            ServerCommand::DeleteConsumerOffset(command) => command.validate(),
            ServerCommand::GetConsumerOffset(command) => command.validate(),
            ServerCommand::GetStream(command) => command.validate(),
            ServerCommand::GetStreams(command) => command.validate(),
            ServerCommand::CreateStream(command) => command.validate(),
            ServerCommand::DeleteStream(command) => command.validate(),
            ServerCommand::UpdateStream(command) => command.validate(),
            ServerCommand::PurgeStream(command) => command.validate(),
            ServerCommand::GetTopic(command) => command.validate(),
            ServerCommand::GetTopics(command) => command.validate(),
            ServerCommand::CreateTopic(command) => command.validate(),
            ServerCommand::DeleteTopic(command) => command.validate(),
            ServerCommand::UpdateTopic(command) => command.validate(),
            ServerCommand::PurgeTopic(command) => command.validate(),
            ServerCommand::CreatePartitions(command) => command.validate(),
            ServerCommand::DeletePartitions(command) => command.validate(),
            ServerCommand::GetConsumerGroup(command) => command.validate(),
            ServerCommand::GetConsumerGroups(command) => command.validate(),
            ServerCommand::CreateConsumerGroup(command) => command.validate(),
            ServerCommand::DeleteConsumerGroup(command) => command.validate(),
            ServerCommand::JoinConsumerGroup(command) => command.validate(),
            ServerCommand::LeaveConsumerGroup(command) => command.validate(),
            ServerCommand::FlushUnsavedBuffer(command) => command.validate(),
            ServerCommand::GetSnapshotFile(command) => command.validate(),
        }
    }
}

impl Display for ServerCommand {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerCommand::Ping(_) => write!(formatter, "{PING}"),
            ServerCommand::GetStats(_) => write!(formatter, "{GET_STATS}"),
            ServerCommand::GetMe(_) => write!(formatter, "{GET_ME}"),
            ServerCommand::GetClient(payload) => write!(formatter, "{GET_CLIENT}|{payload}"),
            ServerCommand::GetClients(_) => write!(formatter, "{GET_CLIENTS}"),
            ServerCommand::GetUser(payload) => write!(formatter, "{GET_USER}|{payload}"),
            ServerCommand::GetUsers(_) => write!(formatter, "{GET_USERS}"),
            ServerCommand::CreateUser(payload) => write!(formatter, "{CREATE_USER}|{payload}"),
            ServerCommand::DeleteUser(payload) => write!(formatter, "{DELETE_USER}|{payload}"),
            ServerCommand::UpdateUser(payload) => write!(formatter, "{UPDATE_USER}|{payload}"),
            ServerCommand::UpdatePermissions(payload) => {
                write!(formatter, "{UPDATE_PERMISSIONS}|{payload}")
            }
            ServerCommand::ChangePassword(payload) => {
                write!(formatter, "{CHANGE_PASSWORD}|{payload}")
            }
            ServerCommand::LoginUser(payload) => write!(formatter, "{LOGIN_USER}|{payload}"),
            ServerCommand::LogoutUser(_) => write!(formatter, "{LOGOUT_USER}"),
            ServerCommand::GetPersonalAccessTokens(_) => {
                write!(formatter, "{GET_PERSONAL_ACCESS_TOKENS}")
            }
            ServerCommand::CreatePersonalAccessToken(payload) => {
                write!(formatter, "{CREATE_PERSONAL_ACCESS_TOKEN}|{payload}")
            }
            ServerCommand::DeletePersonalAccessToken(payload) => {
                write!(formatter, "{DELETE_PERSONAL_ACCESS_TOKEN}|{payload}")
            }
            ServerCommand::LoginWithPersonalAccessToken(payload) => {
                write!(formatter, "{LOGIN_WITH_PERSONAL_ACCESS_TOKEN}|{payload}")
            }
            ServerCommand::GetStream(payload) => write!(formatter, "{GET_STREAM}|{payload}"),
            ServerCommand::GetStreams(_) => write!(formatter, "{GET_STREAMS}"),
            ServerCommand::CreateStream(payload) => write!(formatter, "{CREATE_STREAM}|{payload}"),
            ServerCommand::DeleteStream(payload) => write!(formatter, "{DELETE_STREAM}|{payload}"),
            ServerCommand::UpdateStream(payload) => write!(formatter, "{UPDATE_STREAM}|{payload}"),
            ServerCommand::PurgeStream(payload) => write!(formatter, "{PURGE_STREAM}|{payload}"),
            ServerCommand::GetTopic(payload) => write!(formatter, "{GET_TOPIC}|{payload}"),
            ServerCommand::GetTopics(payload) => write!(formatter, "{GET_TOPICS}|{payload}"),
            ServerCommand::CreateTopic(payload) => write!(formatter, "{CREATE_TOPIC}|{payload}"),
            ServerCommand::DeleteTopic(payload) => write!(formatter, "{DELETE_TOPIC}|{payload}"),
            ServerCommand::UpdateTopic(payload) => write!(formatter, "{UPDATE_TOPIC}|{payload}"),
            ServerCommand::PurgeTopic(payload) => write!(formatter, "{PURGE_TOPIC}|{payload}"),
            ServerCommand::CreatePartitions(payload) => {
                write!(formatter, "{CREATE_PARTITIONS}|{payload}")
            }
            ServerCommand::DeletePartitions(payload) => {
                write!(formatter, "{DELETE_PARTITIONS}|{payload}")
            }
            ServerCommand::PollMessages(payload) => write!(formatter, "{POLL_MESSAGES}|{payload}"),
            ServerCommand::SendMessages(payload) => write!(formatter, "{SEND_MESSAGES}|{payload}"),
            ServerCommand::StoreConsumerOffset(payload) => {
                write!(formatter, "{STORE_CONSUMER_OFFSET}|{payload}")
            }
            ServerCommand::DeleteConsumerOffset(payload) => {
                write!(formatter, "{DELETE_CONSUMER_OFFSET}|{payload}")
            }
            ServerCommand::GetConsumerOffset(payload) => {
                write!(formatter, "{GET_CONSUMER_OFFSET}|{payload}")
            }
            ServerCommand::GetConsumerGroup(payload) => {
                write!(formatter, "{GET_CONSUMER_GROUP}|{payload}")
            }
            ServerCommand::GetConsumerGroups(payload) => {
                write!(formatter, "{GET_CONSUMER_GROUPS}|{payload}")
            }
            ServerCommand::CreateConsumerGroup(payload) => {
                write!(formatter, "{CREATE_CONSUMER_GROUP}|{payload}")
            }
            ServerCommand::DeleteConsumerGroup(payload) => {
                write!(formatter, "{DELETE_CONSUMER_GROUP}|{payload}")
            }
            ServerCommand::JoinConsumerGroup(payload) => {
                write!(formatter, "{JOIN_CONSUMER_GROUP}|{payload}")
            }
            ServerCommand::LeaveConsumerGroup(payload) => {
                write!(formatter, "{LEAVE_CONSUMER_GROUP}|{payload}")
            }
            ServerCommand::FlushUnsavedBuffer(payload) => {
                write!(formatter, "{FLUSH_UNSAVED_BUFFER}|{payload}")
            }
            ServerCommand::GetSnapshotFile(payload) => {
                write!(formatter, "{GET_SNAPSHOT_FILE}|{payload}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes_and_deserialized_from_bytes() {
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::Ping(Ping::default()),
            PING_CODE,
            &Ping::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::GetStats(GetStats::default()),
            GET_STATS_CODE,
            &GetStats::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::GetMe(GetMe::default()),
            GET_ME_CODE,
            &GetMe::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::GetClient(GetClient::default()),
            GET_CLIENT_CODE,
            &GetClient::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::GetClients(GetClients::default()),
            GET_CLIENTS_CODE,
            &GetClients::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::GetUser(GetUser::default()),
            GET_USER_CODE,
            &GetUser::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::GetUsers(GetUsers::default()),
            GET_USERS_CODE,
            &GetUsers::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::CreateUser(CreateUser::default()),
            CREATE_USER_CODE,
            &CreateUser::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::DeleteUser(DeleteUser::default()),
            DELETE_USER_CODE,
            &DeleteUser::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::UpdateUser(UpdateUser::default()),
            UPDATE_USER_CODE,
            &UpdateUser::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::UpdatePermissions(UpdatePermissions::default()),
            UPDATE_PERMISSIONS_CODE,
            &UpdatePermissions::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::ChangePassword(ChangePassword::default()),
            CHANGE_PASSWORD_CODE,
            &ChangePassword::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::LoginUser(LoginUser::default()),
            LOGIN_USER_CODE,
            &LoginUser::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::LogoutUser(LogoutUser::default()),
            LOGOUT_USER_CODE,
            &LogoutUser::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::GetPersonalAccessTokens(GetPersonalAccessTokens::default()),
            GET_PERSONAL_ACCESS_TOKENS_CODE,
            &GetPersonalAccessTokens::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::CreatePersonalAccessToken(CreatePersonalAccessToken::default()),
            CREATE_PERSONAL_ACCESS_TOKEN_CODE,
            &CreatePersonalAccessToken::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::DeletePersonalAccessToken(DeletePersonalAccessToken::default()),
            DELETE_PERSONAL_ACCESS_TOKEN_CODE,
            &DeletePersonalAccessToken::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::LoginWithPersonalAccessToken(LoginWithPersonalAccessToken::default()),
            LOGIN_WITH_PERSONAL_ACCESS_TOKEN_CODE,
            &LoginWithPersonalAccessToken::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::SendMessages(SendMessages::default()),
            SEND_MESSAGES_CODE,
            &SendMessages::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::PollMessages(PollMessages::default()),
            POLL_MESSAGES_CODE,
            &PollMessages::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::StoreConsumerOffset(StoreConsumerOffset::default()),
            STORE_CONSUMER_OFFSET_CODE,
            &StoreConsumerOffset::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::GetConsumerOffset(GetConsumerOffset::default()),
            GET_CONSUMER_OFFSET_CODE,
            &GetConsumerOffset::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::GetStream(GetStream::default()),
            GET_STREAM_CODE,
            &GetStream::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::GetStreams(GetStreams::default()),
            GET_STREAMS_CODE,
            &GetStreams::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::CreateStream(CreateStream::default()),
            CREATE_STREAM_CODE,
            &CreateStream::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::DeleteStream(DeleteStream::default()),
            DELETE_STREAM_CODE,
            &DeleteStream::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::UpdateStream(UpdateStream::default()),
            UPDATE_STREAM_CODE,
            &UpdateStream::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::PurgeStream(PurgeStream::default()),
            PURGE_STREAM_CODE,
            &PurgeStream::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::GetTopic(GetTopic::default()),
            GET_TOPIC_CODE,
            &GetTopic::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::GetTopics(GetTopics::default()),
            GET_TOPICS_CODE,
            &GetTopics::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::CreateTopic(CreateTopic::default()),
            CREATE_TOPIC_CODE,
            &CreateTopic::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::DeleteTopic(DeleteTopic::default()),
            DELETE_TOPIC_CODE,
            &DeleteTopic::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::UpdateTopic(UpdateTopic::default()),
            UPDATE_TOPIC_CODE,
            &UpdateTopic::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::PurgeTopic(PurgeTopic::default()),
            PURGE_TOPIC_CODE,
            &PurgeTopic::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::CreatePartitions(CreatePartitions::default()),
            CREATE_PARTITIONS_CODE,
            &CreatePartitions::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::DeletePartitions(DeletePartitions::default()),
            DELETE_PARTITIONS_CODE,
            &DeletePartitions::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::GetConsumerGroup(GetConsumerGroup::default()),
            GET_CONSUMER_GROUP_CODE,
            &GetConsumerGroup::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::GetConsumerGroups(GetConsumerGroups::default()),
            GET_CONSUMER_GROUPS_CODE,
            &GetConsumerGroups::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::CreateConsumerGroup(CreateConsumerGroup::default()),
            CREATE_CONSUMER_GROUP_CODE,
            &CreateConsumerGroup::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::DeleteConsumerGroup(DeleteConsumerGroup::default()),
            DELETE_CONSUMER_GROUP_CODE,
            &DeleteConsumerGroup::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::JoinConsumerGroup(JoinConsumerGroup::default()),
            JOIN_CONSUMER_GROUP_CODE,
            &JoinConsumerGroup::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::LeaveConsumerGroup(LeaveConsumerGroup::default()),
            LEAVE_CONSUMER_GROUP_CODE,
            &LeaveConsumerGroup::default(),
        );
        assert_serialized_as_bytes_and_deserialized_from_bytes(
            &ServerCommand::FlushUnsavedBuffer(FlushUnsavedBuffer::default()),
            FLUSH_UNSAVED_BUFFER_CODE,
            &FlushUnsavedBuffer::default(),
        );
    }

    fn assert_serialized_as_bytes_and_deserialized_from_bytes(
        command: &ServerCommand,
        code: u32,
        payload: &dyn Command,
    ) {
        assert_serialized_as_bytes(command, code, payload);
        assert_deserialized_from_bytes(command, code, payload);
    }

    fn assert_serialized_as_bytes(
        server_command: &ServerCommand,
        code: u32,
        command: &dyn Command,
    ) {
        let payload = command.to_bytes();
        let mut bytes = BytesMut::with_capacity(4 + payload.len());
        bytes.put_u32_le(code);
        bytes.put_slice(&payload);
        assert_eq!(server_command.to_bytes(), bytes);
    }

    fn assert_deserialized_from_bytes(
        command: &ServerCommand,
        command_id: u32,
        payload: &dyn Command,
    ) {
        let payload = payload.to_bytes();
        let mut bytes = BytesMut::with_capacity(4 + payload.len());
        bytes.put_u32_le(command_id);
        bytes.put_slice(&payload);
        let bytes = Bytes::from(bytes);
        assert_eq!(&ServerCommand::from_bytes(bytes).unwrap(), command);
    }
}
