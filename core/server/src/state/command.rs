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

use crate::state::models::{
    CreateConsumerGroupWithId, CreatePersonalAccessTokenWithHash, CreateStreamWithId,
    CreateTopicWithId, CreateUserWithId,
};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use iggy_binary_protocol::codes::{
    CHANGE_PASSWORD_CODE, CREATE_CONSUMER_GROUP_CODE, CREATE_PARTITIONS_CODE,
    CREATE_PERSONAL_ACCESS_TOKEN_CODE, CREATE_STREAM_CODE, CREATE_TOPIC_CODE, CREATE_USER_CODE,
    DELETE_CONSUMER_GROUP_CODE, DELETE_PARTITIONS_CODE, DELETE_PERSONAL_ACCESS_TOKEN_CODE,
    DELETE_SEGMENTS_CODE, DELETE_STREAM_CODE, DELETE_TOPIC_CODE, DELETE_USER_CODE,
    PURGE_STREAM_CODE, PURGE_TOPIC_CODE, UPDATE_PERMISSIONS_CODE, UPDATE_STREAM_CODE,
    UPDATE_TOPIC_CODE, UPDATE_USER_CODE,
};
use iggy_binary_protocol::requests::{
    consumer_groups::DeleteConsumerGroupRequest,
    partitions::{CreatePartitionsRequest, DeletePartitionsRequest},
    personal_access_tokens::DeletePersonalAccessTokenRequest,
    segments::DeleteSegmentsRequest,
    streams::{DeleteStreamRequest, PurgeStreamRequest, UpdateStreamRequest},
    topics::{DeleteTopicRequest, PurgeTopicRequest, UpdateTopicRequest},
    users::{
        ChangePasswordRequest, DeleteUserRequest, UpdatePermissionsRequest, UpdateUserRequest,
    },
};
use iggy_binary_protocol::{WireDecode, WireEncode};
use iggy_common::BytesSerializable;
use iggy_common::IggyError;
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum EntryCommand {
    CreateStream(CreateStreamWithId),
    UpdateStream(UpdateStreamRequest),
    DeleteStream(DeleteStreamRequest),
    PurgeStream(PurgeStreamRequest),
    CreateTopic(CreateTopicWithId),
    UpdateTopic(UpdateTopicRequest),
    DeleteTopic(DeleteTopicRequest),
    PurgeTopic(PurgeTopicRequest),
    CreatePartitions(CreatePartitionsRequest),
    DeletePartitions(DeletePartitionsRequest),
    DeleteSegments(DeleteSegmentsRequest),
    CreateConsumerGroup(CreateConsumerGroupWithId),
    DeleteConsumerGroup(DeleteConsumerGroupRequest),
    CreateUser(CreateUserWithId),
    UpdateUser(UpdateUserRequest),
    DeleteUser(DeleteUserRequest),
    ChangePassword(ChangePasswordRequest),
    UpdatePermissions(UpdatePermissionsRequest),
    CreatePersonalAccessToken(CreatePersonalAccessTokenWithHash),
    DeletePersonalAccessToken(DeletePersonalAccessTokenRequest),
}

fn wire_error_to_iggy(e: iggy_binary_protocol::WireError) -> IggyError {
    tracing::warn!("wire decode error during WAL replay: {e}");
    IggyError::InvalidCommand
}

impl BytesSerializable for EntryCommand {
    fn to_bytes(&self) -> Bytes {
        let (code, command) = match self {
            EntryCommand::CreateStream(cmd) => (CREATE_STREAM_CODE, cmd.to_bytes()),
            EntryCommand::UpdateStream(cmd) => (UPDATE_STREAM_CODE, cmd.to_bytes()),
            EntryCommand::DeleteStream(cmd) => (DELETE_STREAM_CODE, cmd.to_bytes()),
            EntryCommand::PurgeStream(cmd) => (PURGE_STREAM_CODE, cmd.to_bytes()),
            EntryCommand::CreateTopic(cmd) => (CREATE_TOPIC_CODE, cmd.to_bytes()),
            EntryCommand::UpdateTopic(cmd) => (UPDATE_TOPIC_CODE, cmd.to_bytes()),
            EntryCommand::DeleteTopic(cmd) => (DELETE_TOPIC_CODE, cmd.to_bytes()),
            EntryCommand::PurgeTopic(cmd) => (PURGE_TOPIC_CODE, cmd.to_bytes()),
            EntryCommand::CreatePartitions(cmd) => (CREATE_PARTITIONS_CODE, cmd.to_bytes()),
            EntryCommand::DeletePartitions(cmd) => (DELETE_PARTITIONS_CODE, cmd.to_bytes()),
            EntryCommand::DeleteSegments(cmd) => (DELETE_SEGMENTS_CODE, cmd.to_bytes()),
            EntryCommand::CreateConsumerGroup(cmd) => (CREATE_CONSUMER_GROUP_CODE, cmd.to_bytes()),
            EntryCommand::DeleteConsumerGroup(cmd) => (DELETE_CONSUMER_GROUP_CODE, cmd.to_bytes()),
            EntryCommand::CreateUser(cmd) => (CREATE_USER_CODE, cmd.to_bytes()),
            EntryCommand::UpdateUser(cmd) => (UPDATE_USER_CODE, cmd.to_bytes()),
            EntryCommand::DeleteUser(cmd) => (DELETE_USER_CODE, cmd.to_bytes()),
            EntryCommand::ChangePassword(cmd) => (CHANGE_PASSWORD_CODE, cmd.to_bytes()),
            EntryCommand::UpdatePermissions(cmd) => (UPDATE_PERMISSIONS_CODE, cmd.to_bytes()),
            EntryCommand::CreatePersonalAccessToken(cmd) => {
                (CREATE_PERSONAL_ACCESS_TOKEN_CODE, cmd.to_bytes())
            }
            EntryCommand::DeletePersonalAccessToken(cmd) => {
                (DELETE_PERSONAL_ACCESS_TOKEN_CODE, cmd.to_bytes())
            }
        };

        let mut bytes = BytesMut::with_capacity(4 + 4 + command.len());
        bytes.put_u32_le(code);
        bytes.put_u32_le(command.len() as u32);
        bytes.extend(command);
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        let code = bytes.slice(0..4).get_u32_le();
        let length = bytes.slice(4..8).get_u32_le();
        let payload = &bytes[8..8 + length as usize];
        match code {
            CREATE_STREAM_CODE => Ok(EntryCommand::CreateStream(
                CreateStreamWithId::decode_from(payload).map_err(wire_error_to_iggy)?,
            )),
            UPDATE_STREAM_CODE => Ok(EntryCommand::UpdateStream(
                UpdateStreamRequest::decode_from(payload).map_err(wire_error_to_iggy)?,
            )),
            DELETE_STREAM_CODE => Ok(EntryCommand::DeleteStream(
                DeleteStreamRequest::decode_from(payload).map_err(wire_error_to_iggy)?,
            )),
            PURGE_STREAM_CODE => Ok(EntryCommand::PurgeStream(
                PurgeStreamRequest::decode_from(payload).map_err(wire_error_to_iggy)?,
            )),
            CREATE_TOPIC_CODE => Ok(EntryCommand::CreateTopic(
                CreateTopicWithId::decode_from(payload).map_err(wire_error_to_iggy)?,
            )),
            UPDATE_TOPIC_CODE => Ok(EntryCommand::UpdateTopic(
                UpdateTopicRequest::decode_from(payload).map_err(wire_error_to_iggy)?,
            )),
            DELETE_TOPIC_CODE => Ok(EntryCommand::DeleteTopic(
                DeleteTopicRequest::decode_from(payload).map_err(wire_error_to_iggy)?,
            )),
            PURGE_TOPIC_CODE => Ok(EntryCommand::PurgeTopic(
                PurgeTopicRequest::decode_from(payload).map_err(wire_error_to_iggy)?,
            )),
            CREATE_PARTITIONS_CODE => Ok(EntryCommand::CreatePartitions(
                CreatePartitionsRequest::decode_from(payload).map_err(wire_error_to_iggy)?,
            )),
            DELETE_PARTITIONS_CODE => Ok(EntryCommand::DeletePartitions(
                DeletePartitionsRequest::decode_from(payload).map_err(wire_error_to_iggy)?,
            )),
            DELETE_SEGMENTS_CODE => Ok(EntryCommand::DeleteSegments(
                DeleteSegmentsRequest::decode_from(payload).map_err(wire_error_to_iggy)?,
            )),
            CREATE_CONSUMER_GROUP_CODE => Ok(EntryCommand::CreateConsumerGroup(
                CreateConsumerGroupWithId::decode_from(payload).map_err(wire_error_to_iggy)?,
            )),
            DELETE_CONSUMER_GROUP_CODE => Ok(EntryCommand::DeleteConsumerGroup(
                DeleteConsumerGroupRequest::decode_from(payload).map_err(wire_error_to_iggy)?,
            )),
            CREATE_USER_CODE => Ok(EntryCommand::CreateUser(
                CreateUserWithId::decode_from(payload).map_err(wire_error_to_iggy)?,
            )),
            UPDATE_USER_CODE => Ok(EntryCommand::UpdateUser(
                UpdateUserRequest::decode_from(payload).map_err(wire_error_to_iggy)?,
            )),
            DELETE_USER_CODE => Ok(EntryCommand::DeleteUser(
                DeleteUserRequest::decode_from(payload).map_err(wire_error_to_iggy)?,
            )),
            CHANGE_PASSWORD_CODE => Ok(EntryCommand::ChangePassword(
                ChangePasswordRequest::decode_from(payload).map_err(wire_error_to_iggy)?,
            )),
            UPDATE_PERMISSIONS_CODE => Ok(EntryCommand::UpdatePermissions(
                UpdatePermissionsRequest::decode_from(payload).map_err(wire_error_to_iggy)?,
            )),
            CREATE_PERSONAL_ACCESS_TOKEN_CODE => Ok(EntryCommand::CreatePersonalAccessToken(
                CreatePersonalAccessTokenWithHash::decode_from(payload)
                    .map_err(wire_error_to_iggy)?,
            )),
            DELETE_PERSONAL_ACCESS_TOKEN_CODE => Ok(EntryCommand::DeletePersonalAccessToken(
                DeletePersonalAccessTokenRequest::decode_from(payload)
                    .map_err(wire_error_to_iggy)?,
            )),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}

impl Display for EntryCommand {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            EntryCommand::CreateStream(command) => write!(f, "CreateStream({command})"),
            EntryCommand::UpdateStream(command) => write!(f, "UpdateStream({command:?})"),
            EntryCommand::DeleteStream(command) => write!(f, "DeleteStream({command:?})"),
            EntryCommand::PurgeStream(command) => write!(f, "PurgeStream({command:?})"),
            EntryCommand::CreateTopic(command) => write!(f, "CreateTopic({command})"),
            EntryCommand::UpdateTopic(command) => write!(f, "UpdateTopic({command:?})"),
            EntryCommand::DeleteTopic(command) => write!(f, "DeleteTopic({command:?})"),
            EntryCommand::PurgeTopic(command) => write!(f, "PurgeTopic({command:?})"),
            EntryCommand::CreatePartitions(command) => write!(f, "CreatePartitions({command:?})"),
            EntryCommand::DeletePartitions(command) => write!(f, "DeletePartitions({command:?})"),
            EntryCommand::DeleteSegments(command) => write!(f, "DeleteSegments({command:?})"),
            EntryCommand::CreateConsumerGroup(command) => {
                write!(f, "CreateConsumerGroup({command})")
            }
            EntryCommand::DeleteConsumerGroup(command) => {
                write!(f, "DeleteConsumerGroup({command:?})")
            }
            EntryCommand::CreateUser(command) => write!(f, "CreateUser({command})"),
            EntryCommand::UpdateUser(command) => write!(f, "UpdateUser({command:?})"),
            EntryCommand::DeleteUser(command) => write!(f, "DeleteUser({command:?})"),
            EntryCommand::ChangePassword(command) => write!(f, "ChangePassword({command:?})"),
            EntryCommand::UpdatePermissions(command) => {
                write!(f, "UpdatePermissions({command:?})")
            }
            EntryCommand::CreatePersonalAccessToken(command) => {
                write!(f, "CreatePersonalAccessToken({command})")
            }
            EntryCommand::DeletePersonalAccessToken(command) => {
                write!(f, "DeletePersonalAccessToken({command:?})")
            }
        }
    }
}
