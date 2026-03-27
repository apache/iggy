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
use bytes::{BufMut, BytesMut};
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
use iggy_binary_protocol::{WireDecode, WireEncode, WireError};
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

impl WireEncode for EntryCommand {
    fn encoded_size(&self) -> usize {
        let inner_size = match self {
            EntryCommand::CreateStream(cmd) => cmd.encoded_size(),
            EntryCommand::UpdateStream(cmd) => cmd.encoded_size(),
            EntryCommand::DeleteStream(cmd) => cmd.encoded_size(),
            EntryCommand::PurgeStream(cmd) => cmd.encoded_size(),
            EntryCommand::CreateTopic(cmd) => cmd.encoded_size(),
            EntryCommand::UpdateTopic(cmd) => cmd.encoded_size(),
            EntryCommand::DeleteTopic(cmd) => cmd.encoded_size(),
            EntryCommand::PurgeTopic(cmd) => cmd.encoded_size(),
            EntryCommand::CreatePartitions(cmd) => cmd.encoded_size(),
            EntryCommand::DeletePartitions(cmd) => cmd.encoded_size(),
            EntryCommand::DeleteSegments(cmd) => cmd.encoded_size(),
            EntryCommand::CreateConsumerGroup(cmd) => cmd.encoded_size(),
            EntryCommand::DeleteConsumerGroup(cmd) => cmd.encoded_size(),
            EntryCommand::CreateUser(cmd) => cmd.encoded_size(),
            EntryCommand::UpdateUser(cmd) => cmd.encoded_size(),
            EntryCommand::DeleteUser(cmd) => cmd.encoded_size(),
            EntryCommand::ChangePassword(cmd) => cmd.encoded_size(),
            EntryCommand::UpdatePermissions(cmd) => cmd.encoded_size(),
            EntryCommand::CreatePersonalAccessToken(cmd) => cmd.encoded_size(),
            EntryCommand::DeletePersonalAccessToken(cmd) => cmd.encoded_size(),
        };
        4 + 4 + inner_size
    }

    fn encode(&self, buf: &mut BytesMut) {
        let (code, inner_size) = match self {
            EntryCommand::CreateStream(cmd) => (CREATE_STREAM_CODE, cmd.encoded_size()),
            EntryCommand::UpdateStream(cmd) => (UPDATE_STREAM_CODE, cmd.encoded_size()),
            EntryCommand::DeleteStream(cmd) => (DELETE_STREAM_CODE, cmd.encoded_size()),
            EntryCommand::PurgeStream(cmd) => (PURGE_STREAM_CODE, cmd.encoded_size()),
            EntryCommand::CreateTopic(cmd) => (CREATE_TOPIC_CODE, cmd.encoded_size()),
            EntryCommand::UpdateTopic(cmd) => (UPDATE_TOPIC_CODE, cmd.encoded_size()),
            EntryCommand::DeleteTopic(cmd) => (DELETE_TOPIC_CODE, cmd.encoded_size()),
            EntryCommand::PurgeTopic(cmd) => (PURGE_TOPIC_CODE, cmd.encoded_size()),
            EntryCommand::CreatePartitions(cmd) => (CREATE_PARTITIONS_CODE, cmd.encoded_size()),
            EntryCommand::DeletePartitions(cmd) => (DELETE_PARTITIONS_CODE, cmd.encoded_size()),
            EntryCommand::DeleteSegments(cmd) => (DELETE_SEGMENTS_CODE, cmd.encoded_size()),
            EntryCommand::CreateConsumerGroup(cmd) => {
                (CREATE_CONSUMER_GROUP_CODE, cmd.encoded_size())
            }
            EntryCommand::DeleteConsumerGroup(cmd) => {
                (DELETE_CONSUMER_GROUP_CODE, cmd.encoded_size())
            }
            EntryCommand::CreateUser(cmd) => (CREATE_USER_CODE, cmd.encoded_size()),
            EntryCommand::UpdateUser(cmd) => (UPDATE_USER_CODE, cmd.encoded_size()),
            EntryCommand::DeleteUser(cmd) => (DELETE_USER_CODE, cmd.encoded_size()),
            EntryCommand::ChangePassword(cmd) => (CHANGE_PASSWORD_CODE, cmd.encoded_size()),
            EntryCommand::UpdatePermissions(cmd) => (UPDATE_PERMISSIONS_CODE, cmd.encoded_size()),
            EntryCommand::CreatePersonalAccessToken(cmd) => {
                (CREATE_PERSONAL_ACCESS_TOKEN_CODE, cmd.encoded_size())
            }
            EntryCommand::DeletePersonalAccessToken(cmd) => {
                (DELETE_PERSONAL_ACCESS_TOKEN_CODE, cmd.encoded_size())
            }
        };
        buf.put_u32_le(code);
        buf.put_u32_le(inner_size as u32);
        match self {
            EntryCommand::CreateStream(cmd) => cmd.encode(buf),
            EntryCommand::UpdateStream(cmd) => cmd.encode(buf),
            EntryCommand::DeleteStream(cmd) => cmd.encode(buf),
            EntryCommand::PurgeStream(cmd) => cmd.encode(buf),
            EntryCommand::CreateTopic(cmd) => cmd.encode(buf),
            EntryCommand::UpdateTopic(cmd) => cmd.encode(buf),
            EntryCommand::DeleteTopic(cmd) => cmd.encode(buf),
            EntryCommand::PurgeTopic(cmd) => cmd.encode(buf),
            EntryCommand::CreatePartitions(cmd) => cmd.encode(buf),
            EntryCommand::DeletePartitions(cmd) => cmd.encode(buf),
            EntryCommand::DeleteSegments(cmd) => cmd.encode(buf),
            EntryCommand::CreateConsumerGroup(cmd) => cmd.encode(buf),
            EntryCommand::DeleteConsumerGroup(cmd) => cmd.encode(buf),
            EntryCommand::CreateUser(cmd) => cmd.encode(buf),
            EntryCommand::UpdateUser(cmd) => cmd.encode(buf),
            EntryCommand::DeleteUser(cmd) => cmd.encode(buf),
            EntryCommand::ChangePassword(cmd) => cmd.encode(buf),
            EntryCommand::UpdatePermissions(cmd) => cmd.encode(buf),
            EntryCommand::CreatePersonalAccessToken(cmd) => cmd.encode(buf),
            EntryCommand::DeletePersonalAccessToken(cmd) => cmd.encode(buf),
        }
    }
}

impl WireDecode for EntryCommand {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        if buf.len() < 8 {
            return Err(WireError::UnexpectedEof {
                offset: 0,
                need: 8,
                have: buf.len(),
            });
        }
        let code = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        let length = u32::from_le_bytes(buf[4..8].try_into().unwrap()) as usize;
        if buf.len() < 8 + length {
            return Err(WireError::UnexpectedEof {
                offset: 8,
                need: length,
                have: buf.len() - 8,
            });
        }
        let payload = &buf[8..8 + length];
        let consumed = 8 + length;
        let cmd = match code {
            CREATE_STREAM_CODE => {
                EntryCommand::CreateStream(CreateStreamWithId::decode_from(payload)?)
            }
            UPDATE_STREAM_CODE => {
                EntryCommand::UpdateStream(UpdateStreamRequest::decode_from(payload)?)
            }
            DELETE_STREAM_CODE => {
                EntryCommand::DeleteStream(DeleteStreamRequest::decode_from(payload)?)
            }
            PURGE_STREAM_CODE => {
                EntryCommand::PurgeStream(PurgeStreamRequest::decode_from(payload)?)
            }
            CREATE_TOPIC_CODE => {
                EntryCommand::CreateTopic(CreateTopicWithId::decode_from(payload)?)
            }
            UPDATE_TOPIC_CODE => {
                EntryCommand::UpdateTopic(UpdateTopicRequest::decode_from(payload)?)
            }
            DELETE_TOPIC_CODE => {
                EntryCommand::DeleteTopic(DeleteTopicRequest::decode_from(payload)?)
            }
            PURGE_TOPIC_CODE => EntryCommand::PurgeTopic(PurgeTopicRequest::decode_from(payload)?),
            CREATE_PARTITIONS_CODE => {
                EntryCommand::CreatePartitions(CreatePartitionsRequest::decode_from(payload)?)
            }
            DELETE_PARTITIONS_CODE => {
                EntryCommand::DeletePartitions(DeletePartitionsRequest::decode_from(payload)?)
            }
            DELETE_SEGMENTS_CODE => {
                EntryCommand::DeleteSegments(DeleteSegmentsRequest::decode_from(payload)?)
            }
            CREATE_CONSUMER_GROUP_CODE => {
                EntryCommand::CreateConsumerGroup(CreateConsumerGroupWithId::decode_from(payload)?)
            }
            DELETE_CONSUMER_GROUP_CODE => {
                EntryCommand::DeleteConsumerGroup(DeleteConsumerGroupRequest::decode_from(payload)?)
            }
            CREATE_USER_CODE => EntryCommand::CreateUser(CreateUserWithId::decode_from(payload)?),
            UPDATE_USER_CODE => EntryCommand::UpdateUser(UpdateUserRequest::decode_from(payload)?),
            DELETE_USER_CODE => EntryCommand::DeleteUser(DeleteUserRequest::decode_from(payload)?),
            CHANGE_PASSWORD_CODE => {
                EntryCommand::ChangePassword(ChangePasswordRequest::decode_from(payload)?)
            }
            UPDATE_PERMISSIONS_CODE => {
                EntryCommand::UpdatePermissions(UpdatePermissionsRequest::decode_from(payload)?)
            }
            CREATE_PERSONAL_ACCESS_TOKEN_CODE => EntryCommand::CreatePersonalAccessToken(
                CreatePersonalAccessTokenWithHash::decode_from(payload)?,
            ),
            DELETE_PERSONAL_ACCESS_TOKEN_CODE => EntryCommand::DeletePersonalAccessToken(
                DeletePersonalAccessTokenRequest::decode_from(payload)?,
            ),
            _ => return Err(WireError::UnknownCommand(code)),
        };
        Ok((cmd, consumed))
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
