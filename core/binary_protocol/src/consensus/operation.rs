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

use bytemuck::{CheckedBitPattern, NoUninit};

/// Replicated operation discriminant. Identifies the state machine operation
/// carried in a consensus message body.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, NoUninit, CheckedBitPattern)]
#[repr(u8)]
pub enum Operation {
    #[default]
    Reserved = 0,

    // Metadata operations (shard 0)
    CreateStream = 128,
    UpdateStream = 129,
    DeleteStream = 130,
    PurgeStream = 131,
    CreateTopic = 132,
    UpdateTopic = 133,
    DeleteTopic = 134,
    PurgeTopic = 135,
    CreatePartitions = 136,
    DeletePartitions = 137,
    // TODO: DeleteSegments is a partition operation (is_partition() == true) but its
    // discriminant sits in the metadata range (128-147). Should be moved to 162 once
    // iggy_common's Operation enum is removed and wire compat is no longer a concern.
    DeleteSegments = 138,
    CreateConsumerGroup = 139,
    DeleteConsumerGroup = 140,
    CreateUser = 141,
    UpdateUser = 142,
    DeleteUser = 143,
    ChangePassword = 144,
    UpdatePermissions = 145,
    CreatePersonalAccessToken = 146,
    DeletePersonalAccessToken = 147,

    // Partition operations (routed by namespace)
    SendMessages = 160,
    StoreConsumerOffset = 161,
}

impl Operation {
    /// Metadata / control-plane operations handled by shard 0.
    #[must_use]
    #[inline]
    pub const fn is_metadata(&self) -> bool {
        matches!(
            self,
            Self::CreateStream
                | Self::UpdateStream
                | Self::DeleteStream
                | Self::PurgeStream
                | Self::CreateTopic
                | Self::UpdateTopic
                | Self::DeleteTopic
                | Self::PurgeTopic
                | Self::CreatePartitions
                | Self::DeletePartitions
                | Self::CreateConsumerGroup
                | Self::DeleteConsumerGroup
                | Self::CreateUser
                | Self::UpdateUser
                | Self::DeleteUser
                | Self::ChangePassword
                | Self::UpdatePermissions
                | Self::CreatePersonalAccessToken
                | Self::DeletePersonalAccessToken
        )
    }

    /// Data-plane operations routed to the shard owning the partition.
    #[must_use]
    #[inline]
    pub const fn is_partition(&self) -> bool {
        matches!(
            self,
            Self::SendMessages | Self::StoreConsumerOffset | Self::DeleteSegments
        )
    }

    /// Bidirectional mapping: `Operation` -> client command code.
    #[must_use]
    pub const fn to_command_code(&self) -> Option<u32> {
        use crate::codes;
        match self {
            Self::Reserved => None,
            Self::CreateStream => Some(codes::CREATE_STREAM_CODE),
            Self::UpdateStream => Some(codes::UPDATE_STREAM_CODE),
            Self::DeleteStream => Some(codes::DELETE_STREAM_CODE),
            Self::PurgeStream => Some(codes::PURGE_STREAM_CODE),
            Self::CreateTopic => Some(codes::CREATE_TOPIC_CODE),
            Self::UpdateTopic => Some(codes::UPDATE_TOPIC_CODE),
            Self::DeleteTopic => Some(codes::DELETE_TOPIC_CODE),
            Self::PurgeTopic => Some(codes::PURGE_TOPIC_CODE),
            Self::CreatePartitions => Some(codes::CREATE_PARTITIONS_CODE),
            Self::DeletePartitions => Some(codes::DELETE_PARTITIONS_CODE),
            Self::DeleteSegments => Some(codes::DELETE_SEGMENTS_CODE),
            Self::CreateConsumerGroup => Some(codes::CREATE_CONSUMER_GROUP_CODE),
            Self::DeleteConsumerGroup => Some(codes::DELETE_CONSUMER_GROUP_CODE),
            Self::CreateUser => Some(codes::CREATE_USER_CODE),
            Self::UpdateUser => Some(codes::UPDATE_USER_CODE),
            Self::DeleteUser => Some(codes::DELETE_USER_CODE),
            Self::ChangePassword => Some(codes::CHANGE_PASSWORD_CODE),
            Self::UpdatePermissions => Some(codes::UPDATE_PERMISSIONS_CODE),
            Self::CreatePersonalAccessToken => Some(codes::CREATE_PERSONAL_ACCESS_TOKEN_CODE),
            Self::DeletePersonalAccessToken => Some(codes::DELETE_PERSONAL_ACCESS_TOKEN_CODE),
            Self::SendMessages => Some(codes::SEND_MESSAGES_CODE),
            Self::StoreConsumerOffset => Some(codes::STORE_CONSUMER_OFFSET_CODE),
        }
    }

    /// Bidirectional mapping: client command code -> `Operation`.
    #[must_use]
    pub const fn from_command_code(code: u32) -> Option<Self> {
        use crate::codes;
        match code {
            codes::CREATE_STREAM_CODE => Some(Self::CreateStream),
            codes::UPDATE_STREAM_CODE => Some(Self::UpdateStream),
            codes::DELETE_STREAM_CODE => Some(Self::DeleteStream),
            codes::PURGE_STREAM_CODE => Some(Self::PurgeStream),
            codes::CREATE_TOPIC_CODE => Some(Self::CreateTopic),
            codes::UPDATE_TOPIC_CODE => Some(Self::UpdateTopic),
            codes::DELETE_TOPIC_CODE => Some(Self::DeleteTopic),
            codes::PURGE_TOPIC_CODE => Some(Self::PurgeTopic),
            codes::CREATE_PARTITIONS_CODE => Some(Self::CreatePartitions),
            codes::DELETE_PARTITIONS_CODE => Some(Self::DeletePartitions),
            codes::DELETE_SEGMENTS_CODE => Some(Self::DeleteSegments),
            codes::CREATE_CONSUMER_GROUP_CODE => Some(Self::CreateConsumerGroup),
            codes::DELETE_CONSUMER_GROUP_CODE => Some(Self::DeleteConsumerGroup),
            codes::CREATE_USER_CODE => Some(Self::CreateUser),
            codes::UPDATE_USER_CODE => Some(Self::UpdateUser),
            codes::DELETE_USER_CODE => Some(Self::DeleteUser),
            codes::CHANGE_PASSWORD_CODE => Some(Self::ChangePassword),
            codes::UPDATE_PERMISSIONS_CODE => Some(Self::UpdatePermissions),
            codes::CREATE_PERSONAL_ACCESS_TOKEN_CODE => Some(Self::CreatePersonalAccessToken),
            codes::DELETE_PERSONAL_ACCESS_TOKEN_CODE => Some(Self::DeletePersonalAccessToken),
            codes::SEND_MESSAGES_CODE => Some(Self::SendMessages),
            codes::STORE_CONSUMER_OFFSET_CODE => Some(Self::StoreConsumerOffset),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Operation;

    #[test]
    fn command_code_roundtrip() {
        let ops = [
            Operation::CreateStream,
            Operation::UpdateStream,
            Operation::DeleteStream,
            Operation::PurgeStream,
            Operation::CreateTopic,
            Operation::UpdateTopic,
            Operation::DeleteTopic,
            Operation::PurgeTopic,
            Operation::CreatePartitions,
            Operation::DeletePartitions,
            Operation::DeleteSegments,
            Operation::CreateConsumerGroup,
            Operation::DeleteConsumerGroup,
            Operation::CreateUser,
            Operation::UpdateUser,
            Operation::DeleteUser,
            Operation::ChangePassword,
            Operation::UpdatePermissions,
            Operation::CreatePersonalAccessToken,
            Operation::DeletePersonalAccessToken,
            Operation::SendMessages,
            Operation::StoreConsumerOffset,
        ];
        for op in ops {
            let code = op
                .to_command_code()
                .unwrap_or_else(|| panic!("Operation {op:?} has no command code mapping"));
            let back = Operation::from_command_code(code)
                .unwrap_or_else(|| panic!("command code {code} has no Operation mapping"));
            assert_eq!(op, back, "roundtrip failed for {op:?} (code={code})");
        }
    }

    #[test]
    fn reserved_has_no_code() {
        assert_eq!(Operation::Reserved.to_command_code(), None);
    }

    #[test]
    fn read_only_commands_have_no_operation() {
        assert!(Operation::from_command_code(crate::PING_CODE).is_none());
        assert!(Operation::from_command_code(crate::GET_STATS_CODE).is_none());
        assert!(Operation::from_command_code(crate::GET_STREAM_CODE).is_none());
        assert!(Operation::from_command_code(crate::POLL_MESSAGES_CODE).is_none());
    }

    #[test]
    fn metadata_vs_partition() {
        assert!(Operation::CreateStream.is_metadata());
        assert!(!Operation::CreateStream.is_partition());
        assert!(Operation::SendMessages.is_partition());
        assert!(!Operation::SendMessages.is_metadata());
        assert!(Operation::DeleteSegments.is_partition());
    }
}
